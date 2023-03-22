typedef int (*dispatch_fd_func_t)(int fd, const void* buf, int sz);
typedef struct listen_t
{
  eloop_t ep;
  listenfd_t listenfd;
  sf_t sf;
  dispatch_fd_func_t dispatch_func;
} listen_t;

extern int send_dispatch_handshake(int fd, const char* b, int sz);
extern int listen_init(listen_t* l, addr_t addr, dispatch_fd_func_t dispatch_func);
