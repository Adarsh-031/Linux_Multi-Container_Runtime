/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Based on the complete implementation from the project submission,
 * extended with a user-space Round Robin scheduler for comparison
 * against Linux CFS.
 *
 * Architecture:
 *   - Supervisor daemon: UNIX domain socket for control plane,
 *     per-container pipes for logging.
 *   - One producer thread per container reads the log pipe and pushes
 *     chunks into the shared bounded buffer.
 *   - One global consumer (logging_thread) drains the buffer and writes
 *     to per-container log files under logs/.
 *   - CLI clients connect to /tmp/mini_runtime.sock, send a
 *     control_request_t, and read back a control_response_t.
 *   - CMD_RUN keeps the client socket open; reap_children() replies
 *     and closes it when the container exits.
 *
 * Scheduler modes (new):
 *   cfs  - Linux native CFS via nice values (default)
 *   rr   - User-space Round Robin via SIGSTOP / SIGCONT + quantum timer
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */
#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define MONITOR_DEVICE      "/dev/container_monitor"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)   /* 40 MiB */
#define DEFAULT_HARD_LIMIT  (64UL << 20)   /* 64 MiB */
#define DEFAULT_QUANTUM_MS  500            /* RR time quantum */

/* ------------------------------------------------------------------ */
/* Enums                                                               */
/* ------------------------------------------------------------------ */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_PAUSED,    /* RR mode: suspended via SIGSTOP             */
    CONTAINER_STOPPED,   /* user-requested stop                        */
    CONTAINER_KILLED,    /* hard-limit kill by the kernel monitor      */
    CONTAINER_EXITED
} container_state_t;

typedef enum {
    SCHED_MODE_CFS = 0,
    SCHED_MODE_RR
} scheduler_mode_t;

/* ------------------------------------------------------------------ */
/* container_record_t                                                  */
/*                                                                     */
/* stop_requested  - set before SIGTERM so reap_children can          */
/*                   distinguish a manual stop from a hard-limit kill  */
/* log_pipe_read_fd - supervisor's end of the per-container log pipe  */
/* producer_tid    - thread that drains the log pipe into the buffer  */
/* run_client_fd   - socket to reply on when container exits (-1 if   */
/*                   this container was started with CMD_START)        */
/* ------------------------------------------------------------------ */
typedef struct container_record {
    char               id[CONTAINER_ID_LEN];
    pid_t              host_pid;
    time_t             started_at;
    container_state_t  state;
    unsigned long      soft_limit_bytes;
    unsigned long      hard_limit_bytes;
    int                exit_code;
    int                exit_signal;
    char               log_path[PATH_MAX];
    /* termination classification */
    int                stop_requested;
    /* logging */
    int                log_pipe_read_fd;
    pthread_t          producer_tid;
    int                producer_started;
    /* run-blocking */
    int                run_client_fd;
    struct container_record *next;
} container_record_t;

/* ------------------------------------------------------------------ */
/* Other types                                                         */
/* ------------------------------------------------------------------ */
typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;
} child_config_t;

/* Argument passed to each per-container producer thread. */
typedef struct {
    int              pipe_read_fd;
    char             container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_arg_t;

typedef struct {
    int                server_fd;
    int                monitor_fd;
    int                should_stop;
    /* scheduler */
    scheduler_mode_t   scheduler_mode;
    unsigned int       quantum_ms;
    pthread_t          rr_thread;
    container_record_t *rr_current;   /* container currently holding CPU in RR */
    /* logging */
    pthread_t          logger_thread;
    bounded_buffer_t   log_buffer;
    /* metadata */
    pthread_mutex_t    metadata_lock;
    container_record_t *containers;
    char               base_rootfs[PATH_MAX];
} supervisor_ctx_t;

/* ------------------------------------------------------------------ */
/* Global signal flags                                                 */
/* ------------------------------------------------------------------ */
static volatile sig_atomic_t g_sigchld_pending = 0;
static volatile sig_atomic_t g_stop_pending    = 0;

static void handle_sigchld(int sig) { (void)sig; g_sigchld_pending = 1; }
static void handle_stop(int sig)    { (void)sig; g_stop_pending    = 1; }

/* ------------------------------------------------------------------ */
/* Usage / parsing helpers                                             */
/* ------------------------------------------------------------------ */
static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs> [--scheduler cfs|rr] [--quantum N]\n"
        "  %s start <id> <container-rootfs> <command>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run <id> <container-rootfs> <command>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1],
                               &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1],
                               &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_PAUSED:   return "paused";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ================================================================== */
/* Bounded Buffer                                                      */
/*                                                                     */
/* A mutex is correct here because:                                    */
/*   - push() calls pthread_cond_wait(), which requires sleeping.      */
/*   - Spinlocks cannot be held across a sleep.                        */
/*   - The critical section is short (a struct copy), so contention    */
/*     overhead is negligible.                                         */
/* ================================================================== */
static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push
 * Blocks while full; returns -1 if shutdown begins while waiting.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop
 * Blocks while empty. Returns 0 on success, 1 when shutdown + empty.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0) {
        if (buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return 1;   /* nothing left; caller should exit */
        }
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ================================================================== */
/* logging_thread  (consumer)                                          */
/*                                                                     */
/* Drains log_item_t chunks from the shared bounded buffer and        */
/* appends each chunk to the correct per-container log file.          */
/* Continues draining until shutdown is signalled AND buffer is empty */
/* so no output is lost on abrupt container exit.                     */
/* ================================================================== */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buffer = (bounded_buffer_t *)arg;
    log_item_t        item;

    while (1) {
        int rc = bounded_buffer_pop(buffer, &item);
        if (rc != 0)
            break;   /* shutdown + empty */

        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path),
                 "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            fprintf(stderr, "[logger] Cannot open %s: %s\n",
                    log_path, strerror(errno));
            continue;
        }

        ssize_t written = 0;
        while (written < (ssize_t)item.length) {
            ssize_t n = write(fd, item.data + written,
                              item.length - (size_t)written);
            if (n < 0) break;
            written += n;
        }
        close(fd);
    }

    return NULL;
}

/* ================================================================== */
/* producer_thread  (one per container)                                */
/*                                                                     */
/* Reads raw bytes from the container's stdout/stderr pipe and        */
/* inserts them as log_item_t chunks into the shared bounded buffer.  */
/* Exits naturally when read() returns 0 (child closed write end).    */
/* ================================================================== */
static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t      item;
    ssize_t         n;

    memset(&item, 0, sizeof(item));
    snprintf(item.container_id, sizeof(item.container_id),
             "%s", parg->container_id);

    while (1) {
        n = read(parg->pipe_read_fd, item.data, sizeof(item.data));
        if (n <= 0) break;   /* EOF (container exited) or error */

        item.length = (size_t)n;
        if (bounded_buffer_push(parg->log_buffer, &item) != 0)
            break;   /* buffer is shutting down */
    }

    close(parg->pipe_read_fd);
    free(parg);
    return NULL;
}

/* ================================================================== */
/* Metadata helpers                                                    */
/* ================================================================== */
static container_record_t *find_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *r = ctx->containers;
    while (r) {
        if (strncmp(r->id, id, CONTAINER_ID_LEN) == 0) return r;
        r = r->next;
    }
    return NULL;
}

static container_record_t *find_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *r = ctx->containers;
    while (r) {
        if (r->host_pid == pid) return r;
        r = r->next;
    }
    return NULL;
}

/* ================================================================== */
/* child_fn  -  clone() child entry point                              */
/*                                                                     */
/* Runs inside isolated PID / UTS / mount namespaces.                 */
/*   1. Redirect stdout/stderr to the supervisor log pipe.            */
/*   2. chroot() into the container's rootfs.                         */
/*   3. Mount /proc so tools like `ps` work inside the container.     */
/*   4. Apply the requested nice value.                               */
/*   5. Exec the command via /bin/sh -c.                              */
/* ================================================================== */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr to the log pipe write end. */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    /* Isolate the filesystem view. */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    /*
     * Mount /proc inside the container so utilities that inspect
     * /proc work correctly. The child is in a fresh mount namespace
     * (CLONE_NEWNS) so this mount is invisible to the host.
     */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        fprintf(stderr,
                "[container] Warning: mount /proc failed: %s\n",
                strerror(errno));
    }

    /* Adjust scheduling priority if requested. */
    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
            fprintf(stderr,
                    "[container] Warning: setpriority(%d) failed: %s\n",
                    cfg->nice_value, strerror(errno));
    }

    /* Execute the command. */
    char *argv_exec[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", argv_exec);

    fprintf(stderr, "[container] execv failed: %s\n", strerror(errno));
    return 1;
}

/* ================================================================== */
/* Monitor registration helpers                                        */
/* ================================================================== */
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid              = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ================================================================== */
/* start_container                                                     */
/*                                                                     */
/* Creates the log pipe, clones the child into new namespaces,        */
/* starts a producer thread, registers the child with the kernel      */
/* monitor, and appends the container_record to the supervisor list.  */
/*                                                                     */
/* RR mode: newly spawned containers start SIGSTOP'd; the RR thread   */
/* decides when to give them the CPU.                                  */
/* ================================================================== */
static container_record_t *start_container(supervisor_ctx_t *ctx,
                                           const control_request_t *req)
{
    int             log_pipe[2]  = { -1, -1 };
    char           *child_stack  = NULL;
    child_config_t  cfg;
    pid_t           child_pid;
    container_record_t *record = NULL;
    producer_arg_t     *parg   = NULL;

    /* Reject duplicate IDs. */
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        fprintf(stderr, "[supervisor] Container ID '%s' already in use.\n",
                req->container_id);
        return NULL;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Logging pipe: child writes, supervisor reads. */
    if (pipe(log_pipe) < 0) {
        perror("pipe");
        return NULL;
    }

    /* Stack for the clone()-ed child (grows downward). */
    child_stack = malloc(STACK_SIZE);
    if (!child_stack) {
        perror("malloc (child stack)");
        goto err;
    }

    /* Build child configuration. */
    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.id,      sizeof(cfg.id),      "%s", req->container_id);
    snprintf(cfg.rootfs,  sizeof(cfg.rootfs),  "%s", req->rootfs);
    snprintf(cfg.command, sizeof(cfg.command), "%s", req->command);
    cfg.nice_value   = req->nice_value;
    cfg.log_write_fd = log_pipe[1];

    /*
     * Clone the child with:
     *   CLONE_NEWPID  - container sees itself as PID 1
     *   CLONE_NEWUTS  - container can set its own hostname
     *   CLONE_NEWNS   - container has its own mount namespace
     *   SIGCHLD       - parent receives SIGCHLD when child exits
     */
    child_pid = clone(child_fn,
                      child_stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &cfg);
    if (child_pid < 0) {
        perror("clone");
        goto err;
    }

    close(log_pipe[1]);
    log_pipe[1] = -1;

    free(child_stack);
    child_stack = NULL;

    mkdir(LOG_DIR, 0755);

    /* Allocate and populate the container record. */
    record = calloc(1, sizeof(*record));
    if (!record) goto err;

    snprintf(record->id, sizeof(record->id), "%s", req->container_id);
    record->host_pid         = child_pid;
    record->started_at       = time(NULL);
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code        = -1;
    record->exit_signal      = -1;
    record->stop_requested   = 0;
    record->log_pipe_read_fd = log_pipe[0];
    record->run_client_fd    = -1;
    record->producer_started = 0;
    snprintf(record->log_path, sizeof(record->log_path),
             "%s/%s.log", LOG_DIR, req->container_id);

    /*
     * RR mode: immediately SIGSTOP the new child.
     * The RR thread decides when it actually gets the CPU.
     * CFS mode: let Linux schedule it freely.
     */
    if (ctx->scheduler_mode == SCHED_MODE_RR) {
        kill(child_pid, SIGSTOP);
        record->state = CONTAINER_PAUSED;
        /* If no container is running yet, give CPU to this one immediately. */
        if (!ctx->rr_current) {
            kill(child_pid, SIGCONT);
            record->state   = CONTAINER_RUNNING;
            ctx->rr_current = record;
        }
    } else {
        record->state = CONTAINER_RUNNING;
    }

    /* Start the producer thread for this container's log pipe. */
    parg = malloc(sizeof(*parg));
    if (!parg) goto err;

    parg->pipe_read_fd = log_pipe[0];
    parg->log_buffer   = &ctx->log_buffer;
    snprintf(parg->container_id, sizeof(parg->container_id),
             "%s", req->container_id);

    if (pthread_create(&record->producer_tid, NULL,
                       producer_thread, parg) != 0) {
        perror("pthread_create (producer)");
        free(parg);
        goto err;
    }
    record->producer_started = 1;

    /* Register the container's host PID with the kernel monitor. */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id,
                                  child_pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0) {
            fprintf(stderr,
                    "[supervisor] Warning: monitor registration failed "
                    "for '%s': %s\n",
                    req->container_id, strerror(errno));
        }
    }

    /* Prepend to the container list. */
    pthread_mutex_lock(&ctx->metadata_lock);
    record->next    = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr,
            "[supervisor] Started container '%s' pid=%d command='%s' scheduler=%s\n",
            record->id, record->host_pid, req->command,
            ctx->scheduler_mode == SCHED_MODE_RR ? "RR" : "CFS");
    return record;

err:
    if (log_pipe[0] >= 0) close(log_pipe[0]);
    if (log_pipe[1] >= 0) close(log_pipe[1]);
    free(child_stack);
    free(record);
    return NULL;
}

/* ================================================================== */
/* reap_children                                                       */
/*                                                                     */
/* Called whenever SIGCHLD is detected in the event loop.             */
/* Uses waitpid(-1, WNOHANG) to collect all exited children.          */
/*                                                                     */
/* For each reaped child:                                              */
/*   - Updates container state and exit information.                  */
/*   - Classifies termination (exited / stopped / hard-limit killed). */
/*   - Joins the producer thread so its pipe is fully drained.        */
/*   - Unregisters the PID from the kernel monitor.                   */
/*   - Replies to any blocked CMD_RUN client and closes its socket.   */
/*   - Clears rr_current if the RR container exited.                  */
/* ================================================================== */
static void reap_children(supervisor_ctx_t *ctx)
{
    int   wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = find_by_pid(ctx, pid);
        if (!r) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            continue;
        }

        if (WIFEXITED(wstatus)) {
            r->exit_code   = WEXITSTATUS(wstatus);
            r->exit_signal = -1;
            r->state       = CONTAINER_EXITED;
        } else if (WIFSIGNALED(wstatus)) {
            r->exit_signal = WTERMSIG(wstatus);
            r->exit_code   = -1;
            /*
             * Termination classification:
             *   stop_requested=1 AND signal → STOPPED (manual stop)
             *   stop_requested=0 AND SIGKILL → KILLED (hard-limit)
             *   anything else signaled        → STOPPED
             */
            if (r->stop_requested) {
                r->state = CONTAINER_STOPPED;
            } else if (r->exit_signal == SIGKILL) {
                r->state = CONTAINER_KILLED;
            } else {
                r->state = CONTAINER_STOPPED;
            }
        }

        /* If the RR-current container exited, clear the pointer. */
        if (ctx->rr_current == r)
            ctx->rr_current = NULL;

        int run_fd = r->run_client_fd;
        r->run_client_fd = -1;

        pthread_mutex_unlock(&ctx->metadata_lock);

        /* Unregister from kernel monitor before joining producer. */
        if (ctx->monitor_fd >= 0)
            unregister_from_monitor(ctx->monitor_fd, r->id, pid);

        /*
         * Join the producer thread. Because the child's write end of
         * the pipe is now closed, the producer will read EOF, flush
         * any remaining data into the bounded buffer, and exit.
         * Joining here guarantees all log output is captured before
         * we reply to a CMD_RUN client.
         */
        if (r->producer_started)
            pthread_join(r->producer_tid, NULL);

        /* Unblock a CMD_RUN client waiting for container exit. */
        if (run_fd >= 0) {
            control_response_t resp;
            memset(&resp, 0, sizeof(resp));
            resp.status = (r->exit_code >= 0)
                          ? r->exit_code
                          : (128 + r->exit_signal);
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' exited state=%s",
                     r->id, state_to_string(r->state));
            ssize_t wb = write(run_fd, &resp, sizeof(resp));
            (void)wb;
            close(run_fd);
        }

        fprintf(stderr,
                "[supervisor] Reaped container '%s' pid=%d state=%s\n",
                r->id, pid, state_to_string(r->state));
    }
}

/* ================================================================== */
/* rr_scheduler_thread  (new - Round Robin mode only)                  */
/*                                                                     */
/* Every quantum_ms milliseconds:                                      */
/*   1. SIGSTOP the currently running container.                       */
/*   2. Find the next eligible container in list order (wrap-around).  */
/*   3. SIGCONT it and update rr_current.                              */
/*                                                                     */
/* Design: mutex (not spinlock) because cond_wait is used in the      */
/* bounded buffer and the critical section never sleeps under the      */
/* lock — sleep happens outside (usleep) between ticks.               */
/* ================================================================== */
static void *rr_scheduler_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

    while (!ctx->should_stop) {
        usleep(ctx->quantum_ms * 1000U);

        pthread_mutex_lock(&ctx->metadata_lock);

        /* Search for the next eligible container after rr_current. */
        container_record_t *next = NULL;
        container_record_t *p;

        /* First pass: from rr_current->next to end of list. */
        p = ctx->rr_current ? ctx->rr_current->next : ctx->containers;
        while (p) {
            if (p->state == CONTAINER_RUNNING ||
                p->state == CONTAINER_PAUSED) {
                next = p;
                break;
            }
            p = p->next;
        }

        /* Wrap-around: from head up to (not including) rr_current. */
        if (!next) {
            p = ctx->containers;
            while (p && p != ctx->rr_current) {
                if (p->state == CONTAINER_RUNNING ||
                    p->state == CONTAINER_PAUSED) {
                    next = p;
                    break;
                }
                p = p->next;
            }
        }

        if (next && next != ctx->rr_current) {
            /* Pause whoever currently holds the CPU. */
            if (ctx->rr_current &&
                ctx->rr_current->state == CONTAINER_RUNNING) {
                kill(ctx->rr_current->host_pid, SIGSTOP);
                ctx->rr_current->state = CONTAINER_PAUSED;
            }
            /* Hand the CPU to the next container. */
            if (next->state == CONTAINER_PAUSED) {
                kill(next->host_pid, SIGCONT);
                next->state = CONTAINER_RUNNING;
            }
            ctx->rr_current = next;
        } else if (!next) {
            ctx->rr_current = NULL;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
    return NULL;
}

/* ================================================================== */
/* handle_client                                                       */
/*                                                                     */
/* Reads one control_request_t from the connected client, dispatches  */
/* to the appropriate handler, sends a control_response_t back, and   */
/* closes the socket — except for CMD_RUN, where the socket is kept   */
/* open until reap_children() replies when the container exits.       */
/* ================================================================== */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t  req;
    control_response_t resp;
    ssize_t            n;

    memset(&resp, 0, sizeof(resp));

    n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
    if (n != (ssize_t)sizeof(req)) {
        close(client_fd);
        return;
    }

    switch (req.kind) {

    /* -------------------------------------------------------------- */
    case CMD_START: {
        container_record_t *r = start_container(ctx, &req);
        if (r) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Started container '%s' pid=%d",
                     r->id, r->host_pid);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to start container '%s'", req.container_id);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
        break;
    }

    /* -------------------------------------------------------------- */
    case CMD_RUN: {
        container_record_t *r = start_container(ctx, &req);
        if (!r) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to start container '%s'", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            close(client_fd);
            break;
        }
        /*
         * Store the client fd.  reap_children() will send the response
         * and close it when the container exits.  Do NOT close here.
         */
        pthread_mutex_lock(&ctx->metadata_lock);
        r->run_client_fd = client_fd;
        pthread_mutex_unlock(&ctx->metadata_lock);
        break;
    }

    /* -------------------------------------------------------------- */
    case CMD_PS: {
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "Container list:");
        send(client_fd, &resp, sizeof(resp), 0);

        char line[512];
        int  len;
        len = snprintf(line, sizeof(line),
                       "%-16s  %-8s  %-10s  %-20s  %-10s  %-10s\n",
                       "ID", "PID", "STATE", "STARTED",
                       "SOFT(MiB)", "HARD(MiB)");
        send(client_fd, line, (size_t)len, 0);

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r) {
            char timebuf[32] = "";
            struct tm *tm_info = localtime(&r->started_at);
            if (tm_info)
                strftime(timebuf, sizeof(timebuf),
                         "%Y-%m-%d %H:%M:%S", tm_info);
            len = snprintf(line, sizeof(line),
                           "%-16s  %-8d  %-10s  %-20s  %-10lu  %-10lu\n",
                           r->id,
                           r->host_pid,
                           state_to_string(r->state),
                           timebuf,
                           r->soft_limit_bytes >> 20,
                           r->hard_limit_bytes >> 20);
            send(client_fd, line, (size_t)len, 0);
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        close(client_fd);
        break;
    }

    /* -------------------------------------------------------------- */
    case CMD_LOGS: {
        char log_path[PATH_MAX] = "";
        int  found = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = find_by_id(ctx, req.container_id);
        if (r) {
            snprintf(log_path, sizeof(log_path), "%s", r->log_path);
            found = 1;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!found) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Unknown container '%s'", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            close(client_fd);
            break;
        }

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "Log for container '%s':", req.container_id);
        send(client_fd, &resp, sizeof(resp), 0);

        int logfd = open(log_path, O_RDONLY);
        if (logfd >= 0) {
            char buf[4096];
            ssize_t rb;
            while ((rb = read(logfd, buf, sizeof(buf))) > 0)
                send(client_fd, buf, (size_t)rb, 0);
            close(logfd);
        }
        close(client_fd);
        break;
    }

    /* -------------------------------------------------------------- */
    case CMD_STOP: {
        int found_running = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = find_by_id(ctx, req.container_id);
        if (r && (r->state == CONTAINER_RUNNING ||
                  r->state == CONTAINER_PAUSED)) {
            r->stop_requested = 1;
            /* Un-pause first so SIGTERM can be delivered. */
            if (r->state == CONTAINER_PAUSED)
                kill(r->host_pid, SIGCONT);
            kill(r->host_pid, SIGTERM);
            found_running = 1;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (found_running) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Sent SIGTERM to container '%s'", req.container_id);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     r ? "Container '%s' is not running"
                       : "Unknown container '%s'",
                     req.container_id);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
        break;
    }
}

/* ================================================================== */
/* supervisor_shutdown                                                 */
/*                                                                     */
/* Orderly teardown:                                                   */
/*   1. Stop RR scheduler thread (if running).                        */
/*   2. SIGTERM all running/paused containers.                        */
/*   3. Wait up to 3 s; SIGKILL survivors.                            */
/*   4. Drain remaining children via reap_children().                 */
/*   5. Signal the log pipeline to drain and stop.                    */
/*   6. Join the logger thread.                                        */
/*   7. Free container records.                                        */
/* ================================================================== */
static void supervisor_shutdown(supervisor_ctx_t *ctx)
{
    fprintf(stderr, "[supervisor] Initiating orderly shutdown...\n");

    /* Stop the RR thread before touching container states. */
    if (ctx->scheduler_mode == SCHED_MODE_RR) {
        ctx->should_stop = 1;
        pthread_join(ctx->rr_thread, NULL);
    }

    /* Signal all running/paused containers. */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *r = ctx->containers;
    while (r) {
        if (r->state == CONTAINER_RUNNING || r->state == CONTAINER_PAUSED) {
            r->stop_requested = 1;
            if (r->state == CONTAINER_PAUSED)
                kill(r->host_pid, SIGCONT);
            kill(r->host_pid, SIGTERM);
        }
        r = r->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Wait up to 3 seconds for graceful exit. */
    int waited = 0;
    while (waited < 3) {
        reap_children(ctx);
        int any_running = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        for (r = ctx->containers; r; r = r->next)
            if (r->state == CONTAINER_RUNNING ||
                r->state == CONTAINER_PAUSED) { any_running = 1; break; }
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!any_running) break;
        sleep(1);
        waited++;
    }

    /* Force-kill any survivors. */
    pthread_mutex_lock(&ctx->metadata_lock);
    for (r = ctx->containers; r; r = r->next)
        if (r->state == CONTAINER_RUNNING || r->state == CONTAINER_PAUSED)
            kill(r->host_pid, SIGKILL);
    pthread_mutex_unlock(&ctx->metadata_lock);

    sleep(1);
    reap_children(ctx);

    /* Drain and stop the logging pipeline. */
    bounded_buffer_begin_shutdown(&ctx->log_buffer);
    pthread_join(ctx->logger_thread, NULL);

    /* Free all container records. */
    pthread_mutex_lock(&ctx->metadata_lock);
    r = ctx->containers;
    while (r) {
        container_record_t *next = r->next;
        if (r->run_client_fd >= 0) close(r->run_client_fd);
        free(r);
        r = next;
    }
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr, "[supervisor] Shutdown complete.\n");
}

/* ================================================================== */
/* run_supervisor                                                      */
/*                                                                     */
/* Long-running supervisor daemon.                                     */
/*   1. Open /dev/container_monitor.                                   */
/*   2. Bind the UNIX domain control socket.                          */
/*   3. Install SIGCHLD / SIGINT / SIGTERM handlers.                  */
/*   4. Spawn logging consumer thread.                                 */
/*   5. Spawn RR scheduler thread (if requested).                     */
/*   6. Event loop: select() on socket, check signal flags.           */
/* ================================================================== */
static int run_supervisor(const char *rootfs,
                           scheduler_mode_t mode,
                           unsigned int quantum_ms)
{
    supervisor_ctx_t   ctx;
    struct sockaddr_un addr;
    struct sigaction   sa;
    int                rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd      = -1;
    ctx.monitor_fd     = -1;
    ctx.scheduler_mode = mode;
    ctx.quantum_ms     = quantum_ms ? quantum_ms : DEFAULT_QUANTUM_MS;
    strncpy(ctx.base_rootfs, rootfs, PATH_MAX - 1);

    /* Metadata mutex. */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* Bounded buffer for log pipeline. */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    mkdir(LOG_DIR, 0755);

    /* 1) Kernel monitor. */
    ctx.monitor_fd = open(MONITOR_DEVICE, O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "[supervisor] Warning: cannot open %s: %s "
                "(memory monitoring disabled)\n",
                MONITOR_DEVICE, strerror(errno));

    /* 2) Control socket. */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto fail; }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto fail;
    }
    chmod(CONTROL_PATH, 0666);   /* allow non-root clients */
    if (listen(ctx.server_fd, 8) < 0) { perror("listen"); goto fail; }

    fprintf(stderr, "[supervisor] Control socket: %s\n", CONTROL_PATH);
    fprintf(stderr, "[supervisor] Base rootfs:    %s\n", rootfs);
    fprintf(stderr, "[supervisor] Scheduler:      %s",
            mode == SCHED_MODE_RR ? "Round Robin" : "CFS");
    if (mode == SCHED_MODE_RR)
        fprintf(stderr, " (quantum=%ums)", ctx.quantum_ms);
    fprintf(stderr, "\n");

    /* 3) Signal handlers. */
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sa.sa_handler = handle_sigchld;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_flags   = 0;
    sa.sa_handler = handle_stop;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4) Logging consumer thread. */
    rc = pthread_create(&ctx.logger_thread, NULL,
                        logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("pthread_create (logger)"); goto fail;
    }

    /* 5) RR scheduler thread (only in RR mode). */
    if (mode == SCHED_MODE_RR) {
        rc = pthread_create(&ctx.rr_thread, NULL,
                            rr_scheduler_thread, &ctx);
        if (rc != 0) {
            errno = rc; perror("pthread_create (rr_scheduler)"); goto fail;
        }
    }

    fprintf(stderr,
            "[supervisor] Running. Send SIGINT or SIGTERM to shut down.\n");

    /* 6) Event loop. */
    while (!ctx.should_stop && !g_stop_pending) {
        fd_set         rfds;
        struct timeval tv = { 1, 0 };   /* 1 s timeout to check flags */

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);

        int ret = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);

        if (g_sigchld_pending) {
            g_sigchld_pending = 0;
            reap_children(&ctx);
        }

        if (ret > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0)
                handle_client(&ctx, client_fd);
        }
    }

    supervisor_shutdown(&ctx);

    if (ctx.server_fd  >= 0) { close(ctx.server_fd);  unlink(CONTROL_PATH); }
    if (ctx.monitor_fd >= 0)   close(ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    fprintf(stderr, "[supervisor] Exited cleanly.\n");
    return 0;

fail:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 1;
}

/* ================================================================== */
/* send_control_request  (client side)                                 */
/* ================================================================== */
static int send_control_request(const control_request_t *req)
{
    int                fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t            n;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Read the fixed-size response struct. */
    n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Incomplete response from supervisor\n");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    /* Drain trailing text for commands that stream extra data. */
    if (resp.status == 0 &&
        (req->kind == CMD_PS   ||
         req->kind == CMD_LOGS ||
         req->kind == CMD_RUN)) {
        char buf[4096];
        while ((n = recv(fd, buf, sizeof(buf), 0)) > 0)
            fwrite(buf, 1, (size_t)n, stdout);
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

/* ================================================================== */
/* CLI entry points                                                    */
/* ================================================================== */
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
            "Usage: %s start <id> <container-rootfs> <command> "
            "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind             = CMD_START;
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
            "Usage: %s run <id> <container-rootfs> <command> "
            "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind             = CMD_RUN;
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ================================================================== */
/* main                                                                */
/* ================================================================== */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr,
                    "Usage: %s supervisor <base-rootfs>"
                    " [--scheduler cfs|rr] [--quantum N]\n", argv[0]);
            return 1;
        }

        const char      *rootfs  = argv[2];
        scheduler_mode_t mode    = SCHED_MODE_CFS;
        unsigned int     quantum = DEFAULT_QUANTUM_MS;

        for (int i = 3; i < argc; i += 2) {
            if (i + 1 >= argc) { usage(argv[0]); return 1; }
            if (strcmp(argv[i], "--scheduler") == 0) {
                if (strcmp(argv[i+1], "rr") == 0)
                    mode = SCHED_MODE_RR;
                else if (strcmp(argv[i+1], "cfs") == 0)
                    mode = SCHED_MODE_CFS;
                else {
                    fprintf(stderr, "Unknown scheduler: %s\n", argv[i+1]);
                    return 1;
                }
            } else if (strcmp(argv[i], "--quantum") == 0) {
                char *e;
                quantum = (unsigned int)strtoul(argv[i+1], &e, 10);
            } else {
                fprintf(stderr, "Unknown supervisor flag: %s\n", argv[i]);
                return 1;
            }
        }
        return run_supervisor(rootfs, mode, quantum);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}