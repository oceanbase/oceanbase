#include "io/easy_maccept.h"
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "easy_define.h"
#include "io/easy_log.h"
#include "io/easy_socket.h"

#define ma_log(level, fd, format, ...) easy_ ## level ## _log ("easy dispatch socket: fd=%d remote=%s " format, fd, easy_peername(fd), ## __VA_ARGS__)

#define MAX_GROUP_COUNT 16
#define MAX_THREAD_COUNT 256
typedef struct easy_ma_t {
    easy_addr_t addr;
    int flags;
    int efd;
    int lfd;
    void *th;
    volatile int stop;
    int g_count;
    int g_start[MAX_GROUP_COUNT];
    int g_size[MAX_GROUP_COUNT];
    int chan[MAX_GROUP_COUNT * MAX_THREAD_COUNT];
} easy_ma_t;
easy_ma_t g_ma;

int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
int ob_epoll_wait(int __epfd, struct epoll_event *__events,
		          int __maxevents, int __timeout);
void easy_ma_init(int port)
{
    int i;
    g_ma.addr = easy_inet_str_to_addr((port & 0x10000) ? "[]" : NULL, port & 0xffff);
    g_ma.flags = EASY_FLAGS_DEFERACCEPT | EASY_FLAGS_SREUSEPORT;
    g_ma.stop = 1;
    for (i = 0; i < MAX_GROUP_COUNT; i++) {
        g_ma.g_start[i] = MAX_THREAD_COUNT;
    }
}

static int round_robin(int base, int cnt, int idx)
{
    return base + ((idx - base) % cnt);
}

static int choose_dispatch_chan(int* gstart, int* gsize, int idx)
{
    static int accept_seq = 0;
    int i = 0;
    for (i = 0; i < MAX_GROUP_COUNT; i++) {
        if (gstart[i] > idx)
            break;
    }
    if (i <= 0 || i >= MAX_GROUP_COUNT) {
        return round_robin(gstart[0], gsize[0], accept_seq++);
    } else {
        return round_robin(gstart[i - 1], gsize[i - 1], idx);
    }
}

static char* easy_peername(int fd)
{
    static char buf[256];
    easy_addr_t addr = easy_inet_getpeername(fd);
    return easy_inet_addr_to_str(&addr, buf, sizeof(buf));
}

static void easy_ma_dispatch(int idx, int fd)
{
    if (fd < 0) return;
    int t = choose_dispatch_chan(g_ma.g_start, g_ma.g_size, idx);
    int wb = 0;
    while ((wb = write(g_ma.chan[t], &fd, sizeof(fd))) < 0
           && errno == EINTR);
    if (wb != sizeof(fd)) {
        ma_log(error, fd, "write gid fail: idx=%d", idx);
    } else {
        ma_log(info, fd, "dispatch succ: idx=%d", idx);
    }
}

static struct epoll_event *__make_epoll_event(struct epoll_event *event, uint32_t event_flag, int fd)
{
    event->events = event_flag;
    event->data.fd = fd;
    return event;
}

static int epoll_add(int efd, int fd, int mask)
{
    struct epoll_event event;
    return epoll_ctl(efd, EPOLL_CTL_ADD, fd, __make_epoll_event(&event, mask, fd));
}

static int do_accept(int efd, int lfd)
{
    int nfd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (nfd < 0) return -1;
    if (0 != epoll_add(efd, nfd, EPOLLIN | EPOLLPRI)) {
        ma_log(error, nfd, "epoll add fail: errno=%d", errno);
        close(nfd);
        nfd = -1;
    }
    return nfd;
}

static int handle_listen_fd(int efd, int lfd, uint32_t emask)
{
    int j;

    if (emask & EPOLLERR) {
        return -1;
    }
    for (j = 0; j < 64; j++) {
        if (do_accept(efd, lfd) < 0) break;
    }
    return 0;
}

static int read_sock_pri(int fd)
{
    int rb = 0;
    uint8_t idx = 0;
    while ((rb = recv(fd, &idx, sizeof(idx), MSG_OOB)) < 0
           && EINTR == errno)
        ;
    return rb != 1 ? INT32_MAX : (int32_t)idx;
}

static void handle_sock_fd(int efd, int fd, uint32_t emask)
{
    if (emask & EPOLLERR) {
        ma_log(warn, fd, "epoll trigger error");
        close(fd);
        return;
    }
    int idx = INT32_MAX;
    if (emask & EPOLLPRI) {
        idx = read_sock_pri(fd);
        if (idx > MAX_THREAD_COUNT) {
            ma_log(error, fd, "read_sock_pri fail: errno=%d", errno);
        }
    } else {
        ma_log(warn, fd, "old version client");
    }
    epoll_ctl(efd, EPOLL_CTL_DEL, fd, NULL);
    easy_ma_dispatch(idx, fd);
}

void* easy_ma_thread_func(easy_ma_t* ma)
{
    int i;
    int efd = ma->efd;
    int lfd = ma->lfd;
    
    while (!ma->stop) {
        const int maxevents = 64;
        struct epoll_event events[maxevents];
        int cnt = ob_epoll_wait(efd, events, maxevents, 1000);
        for (i = 0; i < cnt; i++) {
            int cfd = events[i].data.fd;
            int emask = events[i].events;
            if (lfd == cfd) {
                if (0 != handle_listen_fd(efd, lfd, emask))
                    goto error_exit;
            } else {
                handle_sock_fd(efd, cfd, emask);
            }
        }
    }
error_exit:
    easy_warn_log("easy dispatch socket thread exit: lfd=%d errno=%d", lfd, errno);
    if (lfd >= 0) {
        close(lfd);
    }
    if (efd >= 0) {
        close(efd);
    }
    return NULL;
}

int easy_ma_start()
{
    char addr_buf[128];
    int lfd = -1, efd = -1;
    if (g_ma.g_count <= 0) {
        easy_info_log("easy dispatch thread do not need to start");
        return 0;
    }
    easy_inet_addr_to_str(&g_ma.addr, addr_buf, sizeof(addr_buf));
    if ((lfd = easy_socket_listen(0, &g_ma.addr, &g_ma.flags, 10240)) < 0) {
        goto error_exit;
    }
    
    if ((efd = epoll_create1(EPOLL_CLOEXEC)) < 0) {
        goto error_exit;
    }
    
    if (0 != epoll_add(efd, lfd, EPOLLIN)) {
        goto error_exit;
    }
    
    g_ma.efd = efd;
    g_ma.lfd = lfd;
    g_ma.stop = 0;
    if (0 != ob_pthread_create(&g_ma.th, (void*)easy_ma_thread_func, (void*)&g_ma)) {
        goto error_exit;
    }
    
    easy_info_log("easy dispatch socket thread start: lfd=%d addr=%s", lfd, addr_buf);
    return 0;

error_exit:
    easy_error_log("easy dispatch socket thread start fail: gcount=%d addr=%s lfd=%d efd=%d errno=%d epoll_add_succ=%d", g_ma.g_count, addr_buf, lfd, efd, errno, g_ma.stop == 0);
    g_ma.stop = 1;
    return -1;
}

void easy_ma_stop()
{
    if (!g_ma.stop) {
        g_ma.stop = 1;
        ob_pthread_join(g_ma.th);
        g_ma.th = NULL;
    }
}

static int create_pipe(int* wfd)
{
    int fd[2];
    if (0 != pipe2(fd, O_NONBLOCK)) return -1;
    *wfd = fd[1];
    return fd[0];
}

int easy_ma_regist(int gid, int idx)
{
    if (g_ma.g_count <= 0 || g_ma.g_start[g_ma.g_count - 1] != gid) {
        g_ma.g_start[g_ma.g_count++] = gid;
    }
    g_ma.g_size[g_ma.g_count - 1] = idx + 1;
    return create_pipe(g_ma.chan + gid + idx);
}

int easy_ma_handshake(int fd, int idx)
{
    int err = 0;
    uint8_t id = idx;
    int wb = 0;
    int64_t time_limit = get_us() + 100 * 1000;

    if (g_ma.stop) {
        return 0;
    }

    while (0 == err && (wb = send(fd, &id, 1, MSG_OOB)) < 0) {
        if (EINTR == errno) {
            // pass
        } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
            if (get_us() > time_limit) {
                ma_log(error, fd, "handshake EAGAIN: idx=%d", idx);
                break;
            }
        } else {
            err = errno;
        }
    }
    return err;
}
