/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_net_keepalive.h"
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_defer.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_transport.h"
#include "io/easy_negotiation.h"

extern "C" {
extern int ob_epoll_wait(int __epfd, struct epoll_event *__events,
		         int __maxevents, int __timeout);
extern int sockaddr_compare_c(struct sockaddr_storage *left, struct sockaddr_storage *right);
extern char *sockaddr_to_str_c(struct sockaddr_storage *sock_addr, char *buf, int len);
};

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::lib;
using namespace oceanbase::rpc::frame;
namespace oceanbase
{
namespace obrpc
{

#define KEEPALIVE_INTERVAL 200 * 1000         // 200ms
#define WINDOW_LENGTH      3000 * 1000        // 3s
#define MAX_CREDIBLE_WINDOW 10 * 1000 * 1000  // 10s
#define SERVER_EXPIRED_TIME 600L * 1000 * 1000 // 10min

constexpr int32_t KP_MAGIC = 0x2c15c364;
struct Header
{
public:
  Header(int32_t data_len = 0)
    : magic_(KP_MAGIC), data_len_(data_len) {}
  int encode(char *buf, const int64_t buf_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(encode_i32(buf, buf_len, pos, magic_))) {
      _LOG_WARN("encode magic failed, ret: %d, pos: %ld", ret, pos);
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, data_len_))) {
      _LOG_WARN("encode data len failed, ret: %d, pos: %ld", ret, pos);
    }
    return ret;
  }
  int decode(const char *buf, const int64_t buf_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(decode_i32(buf, buf_len, pos, &magic_))) {
      _LOG_WARN("decode magic failed, ret: %d, pos: %ld", ret, pos);
    } else if (magic_ != KP_MAGIC) {
      ret = OB_ERR_UNEXPECTED;
      _LOG_WARN("unexpected magic, magic: %d", magic_);
    } else if (OB_FAIL(decode_i32(buf, buf_len, pos, &data_len_))) {
      _LOG_WARN("decode data len failed, ret: %d, pos: %ld", ret, pos);
    }
    return ret;
  }
  int32_t get_encoded_size() const
  {
    return encoded_length_i32(magic_) + encoded_length_i32(data_len_);
  }
  int32_t magic_;
  int32_t data_len_;
};

int ObNetKeepAliveData::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (pos >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
  } else if (FALSE_IT(pos += 1)) { // dummy for compatible
    // not reach
  }
  OB_UNIS_ENCODE(rs_server_status_);
  OB_UNIS_ENCODE(start_service_time_);
  return ret;
}

int ObNetKeepAliveData::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (pos >= data_len) {
    ret  = OB_DESERIALIZE_ERROR;
  } else if (FALSE_IT(pos += 1)) { // dummy for compatible
    // not reach
  }
  OB_UNIS_DECODE(rs_server_status_);
  OB_UNIS_DECODE(start_service_time_);
  return ret;
}

int32_t ObNetKeepAliveData::get_encoded_size() const
{
  int32_t len = 0;
  len += 1; // dummy for compatible
  OB_UNIS_ADD_LEN(rs_server_status_);
  OB_UNIS_ADD_LEN(start_service_time_);
  return len;
}

enum {
  UNCONNECT = 0,
  CONNECTING,
  CONNECT_OK,
};

int64_t get_usec()
{
  return ObClockGenerator::getClock();
}

int set_nonblocking(int fd)
{
  int no_block_flag = 1;
  return ioctl(fd, FIONBIO, &no_block_flag);
}

struct server
{
  easy_addr_t cli_addr_;
  int fd_;
};

typedef ObNetKeepAlive::client client;
typedef ObNetKeepAlive::DestKeepAliveState DestKeepAliveState;

void destroy_client(client *c);
void __attribute__((weak)) keepalive_init_data(ObNetKeepAliveData &ka_data)
{
  // do-nothing
}

void __attribute__((weak)) keepalive_make_data(ObNetKeepAliveData &ka_data)
{
  // do-nothing
}

DestKeepAliveState *client2rs(client *c)
{
  return (DestKeepAliveState*)((char*)c - offsetof(DestKeepAliveState, client_buf_));
}

void update_write_ts(DestKeepAliveState *rs)
{
  int64_t now = get_usec();
  ATOMIC_STORE(&rs->last_write_ts_, now);
}

ObNetKeepAlive::ObNetKeepAlive()
  : pipefd_(-1), regist_dest_count_(0)
{
  bzero(&regist_dests_map_, sizeof regist_dests_map_);
  bzero(&regist_dests_, sizeof regist_dests_);
}

ObNetKeepAlive::~ObNetKeepAlive()
{
}

ObNetKeepAlive &ObNetKeepAlive::get_instance()
{
  static ObNetKeepAlive the_one;
  return the_one;
}

int ObNetKeepAlive::start()
{
  int ret = set_thread_count(2);
  if (OB_FAIL(ret)) {
    _LOG_WARN("set thread count failed, ret: %d", ret);
  } else {
    ret = ThreadPool::start();
  }
  return ret;
}

void ObNetKeepAlive::run1()
{
  const int idx = get_thread_idx();
  if (0 == idx) {
    lib::set_thread_name("KeepAliveServer");
    do_server_loop();
  } else {
    lib::set_thread_name("KeepAliveClient");
    do_client_loop();
  }
}

void ObNetKeepAlive::destroy()
{
  ThreadPool::destroy();
}

int ObNetKeepAlive::set_pipefd_listen(int pipefd)
{
  int ret = OB_SUCCESS;
  if (pipefd_ != -1) {
    ret = OB_INIT_TWICE;
    _LOG_ERROR("pipefd has beed setted: %d", pipefd_);
  } else {
    pipefd_ = pipefd;
    _LOG_INFO("set pipefd: %d", pipefd_);
  }
  return ret;
}

const char *addr_to_string(const easy_addr_t &addr)
{
  ObAddr ob_addr;
  ez2ob_addr(ob_addr, const_cast<easy_addr_t&>(addr));
  return to_cstring(ob_addr);
}

int ObNetKeepAlive::in_black(const easy_addr_t &ez_addr, bool &in_black, ObNetKeepAliveData *ka_data)
{
  int ret = OB_SUCCESS;
  in_black = false;
  if (ka_data != nullptr) {
    keepalive_init_data(*ka_data);
  }

  DestKeepAliveState *rs = regist_dest_if_need(ez_addr);
  if (rs != NULL) {
    int64_t now = get_usec();
    int64_t last_wts = ATOMIC_LOAD(&rs->last_write_ts_);
    ATOMIC_STORE(&rs->last_access_ts_, now);
    if (now - last_wts < MAX_CREDIBLE_WINDOW) {
      in_black = rs->in_black_;
      if (ka_data != nullptr) {
        memcpy(ka_data, &rs->ka_data_, sizeof(rs->ka_data_));
      }
    } else if (0 == last_wts) {
      // net_keepalive client has not started or dst server is expired, treat the server not in black
    } else {
      ret = OB_ERR_UNEXPECTED;
      if (REACH_TIME_INTERVAL(1000000)) {
        _LOG_WARN_RET(OB_ERR_UNEXPECTED, "keepalive thread maybe not work, last_write_ts: %ld", last_wts);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObNetKeepAlive::in_black(const common::ObAddr &addr, bool &in_blacklist, ObNetKeepAliveData *ka_data)
{
  easy_addr_t ez_addr = to_ez_addr(addr);
  return in_black(ez_addr, in_blacklist, ka_data);
}

bool ObNetKeepAlive::in_black(const easy_addr_t &addr)
{
  bool in_blacklist = false;
  if (OB_SUCCESS != in_black(addr, in_blacklist, NULL)) {
    in_blacklist = false;
  }
  return in_blacklist;
}

DestKeepAliveState *ObNetKeepAlive::regist_dest_if_need(const easy_addr_t &addr)
{
  DestKeepAliveState *ret = NULL;
  int32_t n_rs = MAX_RS_COUNT;
  uint64_t h = easy_hash_code(&addr, sizeof(addr), 5);
  for (int64_t i = 0; i < n_rs && !ret; i++) {
      DestKeepAliveState *&rs = regist_dests_map_[(h + i) % n_rs];
      if (!rs) {
        DestKeepAliveState *s = (DestKeepAliveState*)ob_malloc(sizeof(DestKeepAliveState), "DestKAState");
        if (NULL == s) {
          _LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed");
          break;
        }
        bzero(s, sizeof(DestKeepAliveState));
        s->svr_addr_ = addr;
        s->last_read_ts_ = get_usec();
        keepalive_init_data(s->ka_data_);
        DestKeepAliveState *ns = ATOMIC_VCAS(&rs, NULL, s);
        if (ns != NULL) {
          ob_free(s);
          s = NULL;
          if (0 == memcmp(&addr, &ns->svr_addr_, sizeof(addr))) {
            ret = ns;
          }
          continue;
        } else {
          ret = s;
          int index = ATOMIC_FAA(&regist_dest_count_, 1);
          if (index >= MAX_RS_COUNT) {
            LOG_WARN_RET(OB_ERR_UNEXPECTED, "regist dest keepalive state failed", K(index));
            ATOMIC_FAA(&regist_dest_count_, -1);
          } else {
            ATOMIC_STORE(&regist_dests_[index], s);
          }
          _LOG_INFO("add new rs, addr: %s", addr_to_string(addr));
        }
      } else {
        if (0 == memcmp(&addr, &rs->svr_addr_, sizeof(addr))) {
          ret = rs;
        }
      }
  }
  return ret;
}

void destroy_client(client *c)
{
  if (c) {
    client2rs(c)->c_ = NULL;
    if (c->fd_ >= 0) {
      close(c->fd_);
    }
  }
}

client* create_client(DestKeepAliveState *rs)
{
  int ret = OB_SUCCESS;
  client *c  = (client *)rs->client_buf_;
  bzero(c, sizeof(client));
  struct sockaddr_storage addr;
  easy_inet_etoa(&rs->svr_addr_, &addr);
  if ((c->fd_ = socket(addr.ss_family, SOCK_STREAM, 0)) < 0) {
    ret = OB_IO_ERROR;
    _LOG_ERROR("create socket failed: %d", errno);
  } else if (set_nonblocking(c->fd_) < 0) {
    ret = OB_IO_ERROR;
    _LOG_ERROR("set nonblocking failed: %d", errno);
  } else if (connect(c->fd_, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    if (errno != EINPROGRESS) {
      ret = OB_IO_ERROR;
      _LOG_ERROR("connect failed: %d", errno);
    } else {
      _LOG_DEBUG("connecting, addr: %s", addr_to_string(rs->svr_addr_));
    }
  } else {
    _LOG_INFO("connect ok, fd: %d, conn: %s,%s", c->fd_, addr_to_string(easy_inet_getpeername(c->fd_)), addr_to_string(rs->svr_addr_));
  }

  if (OB_SUCC(ret)) {
    c->status_ = CONNECTING;
    update_write_ts(rs);
    struct sockaddr_storage self_addr;
    socklen_t len = sizeof(self_addr);
    if (0 == getsockname(c->fd_, (struct sockaddr *)&self_addr, &len)) {
      char str[128];
      const char *addr_str = sockaddr_to_str_c(&self_addr, str, sizeof(str));
      if (0 == sockaddr_compare_c(&self_addr, &addr)) {
        ret = OB_IO_ERROR;
        _LOG_WARN("connection to %s failed, self connect self", addr_str);
      } else {
        _LOG_DEBUG("connection local_addr: %s, fd: %d", addr_str, c->fd_);
      }
    } else {
      ret = OB_IO_ERROR;
      _LOG_WARN("getsockname failed: fd:%d, errno:%d", c->fd_, errno);
    }
  }

  if (OB_FAIL(ret)) {
    if (c->fd_ >= 0) close(c->fd_);
    c = NULL;
  }
  rs->c_ = c;
  return c;
}

char PROTOCOL_DATA = 'o';

void ObNetKeepAlive::do_server_loop()
{
  int ret = OB_SUCCESS;
  struct epoll_event events[512];
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLOUT;
  ev.data.ptr = NULL;
  int epfd = -1;
  if (pipefd_ < 0) {
    ret = OB_NOT_INIT;
    _LOG_ERROR("pipefd not inited: %d", pipefd_);
  } else if ((epfd = epoll_create(1)) < 0) {
    ret = OB_IO_ERROR;
    _LOG_ERROR("epoll_create failed: %d", errno);
  } else if (epoll_ctl(epfd, EPOLL_CTL_ADD, pipefd_, &ev) < 0) {
    ret = OB_IO_ERROR;
    _LOG_ERROR("epoll add listen fd failed: %d", errno);
  }
  if (OB_FAIL(ret)) {
    ob_abort();
  }
  while (!has_set_stop()) {
    int cnt = ob_epoll_wait(epfd, events, sizeof events/sizeof events[0], 1000);
    ObTimeGuard timeguard_server("net_keepalive_server_loop", 100 * 1000);
    for (int i = 0; i < cnt; i++) {
      struct server *s = (struct server *)events[i].data.ptr;
      int ev_fd = NULL == s? pipefd_ : s->fd_;
      bool need_disconn = false;
      if (NULL == s)  {
        struct server *s = (struct server *)ob_malloc(sizeof(struct server), "KeepAliveServer");
        if (NULL == s) {
          // ignore ret
          _LOG_WARN("alloc memory failed");
        } else {
          bool succ = 0;
          DEFER(
            if (!succ) {
              ob_free(s);
              s = NULL;
            }
            );
          bzero(s, sizeof(struct server));
          int conn_fd = -1;
          ssize_t n = -1;
          while ((n = read(pipefd_, &conn_fd, sizeof conn_fd)) < 0 && errno == EINTR);
          if (conn_fd < 0) {
            _LOG_WARN("read(accept) failed: %d", errno);
          } else {
            net_consume_negotiation_msg(conn_fd, ObNetEasy::NET_KEEPALIVE_MAGIC);
            s->cli_addr_ = easy_inet_getpeername(conn_fd);
            DEFER(
              if (!succ) {
                close(conn_fd);
                conn_fd = -1;
              };
              );
            _LOG_INFO("new connection established, fd: %d, addr: %s", conn_fd, addr_to_string(s->cli_addr_));
            s->fd_ = conn_fd;
            if (set_nonblocking(conn_fd) < 0) {
              _LOG_WARN("set_nonblocking failed: %d", errno);
            } else {
              struct epoll_event ev;
              ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLHUP;
              ev.data.ptr = s;
              if (epoll_ctl(epfd, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
                _LOG_WARN("add conn_fd to epoll failed: %d", errno);
              } else {
                succ = 1;
              }
            }
          }
        }
      } else if (events[i].events & EPOLLIN) {
        for (;;) {
          ObTimeGuard timeguard("net_keepalive_server_response", 100 * 1000);
          ssize_t n = -1;
          char data = PROTOCOL_DATA;
          while ((n = read(ev_fd, &data, sizeof data)) < 0 && errno == EINTR);
          if (n <= 0) {
            if (0 == n || (errno != EAGAIN && errno != EWOULDBLOCK)) {
              LOG_INFO("socket need_disconn", K(n), K(errno));
              need_disconn = true;
            }
            break;
          }
          timeguard.click();
          char buf[128];
          const int64_t buf_len = sizeof buf;
          ObNetKeepAliveData ka_data;
          keepalive_make_data(ka_data);
          Header header(ka_data.get_encoded_size());
          int tmp_ret = OB_SUCCESS;
          int64_t pos = 0;
          if (OB_SUCCESS != (tmp_ret = header.encode(buf, buf_len, pos))) {
            _LOG_WARN("encode header failed, ret: %d, pos: %ld", tmp_ret, pos);
          } else if (OB_SUCCESS != (tmp_ret = ka_data.encode(buf, buf_len, pos))) {
            _LOG_WARN("encode ka_data failed, ret: %d, pos: %ld", tmp_ret, pos);
          } else {
            timeguard.click();
            while ((n = write(ev_fd, buf, pos)) < 0 && errno == EINTR);
            need_disconn = n < pos;
          }
        }
      }
      if (!need_disconn) {
        need_disconn = events[i].events & (EPOLLRDHUP | EPOLLHUP);
      }
      if (need_disconn) {
        _LOG_INFO("server connection closed, fd: %d, addr: %s", ev_fd, NULL == s? "" : addr_to_string(s->cli_addr_));
        epoll_ctl(epfd, EPOLL_CTL_DEL, ev_fd, NULL);
        close(ev_fd);
        if (s != NULL) {
          ob_free(s);
          s = NULL;
        }
        continue;
      }
    }
  }
}
void send_keepalive_data(DestKeepAliveState *rs)
{
  int ret = OB_SUCCESS;
  ssize_t n = -1;
  client *c = rs->c_;
  while ((n = write(c->fd_, &PROTOCOL_DATA, sizeof PROTOCOL_DATA)) < 0 && errno == EINTR);
  if (n > 0) {
    c->wait_resp_ = true;
    _LOG_DEBUG("update write ts, addr: %s, fd: %d, ts: %ld", addr_to_string(rs->svr_addr_), c->fd_, client2rs(c)->last_write_ts_);
  } else if (0 == n || ((n < 0 && errno != EAGAIN && errno != EWOULDBLOCK))) {
    _LOG_WARN("send data to rpc server failed, addr: %s, n: %ld, err: %d", addr_to_string(rs->svr_addr_), n, errno);
    destroy_client(c);
  }
}
void check_connect(DestKeepAliveState *rs)
{
  client *c = rs->c_;
  struct pollfd fds[1];
  fds[0].fd = c->fd_;
  fds[0].events = POLLOUT;
  int tret = poll(fds, 1, 0);
  if (tret) {
    uint32_t conn_has_error = 0;
    int idx = 0;
    if (fds[0].revents & POLLOUT
        && EASY_OK == net_send_negotiate_message(1/*negotiation_enable*/, c->fd_,
                                                  ObNetEasy::NET_KEEPALIVE_MAGIC, idx, &conn_has_error)
        && !conn_has_error) {
      _LOG_INFO("connect ok, fd: %d, conn: %s", c->fd_, addr_to_string(client2rs(c)->svr_addr_));
      // try to send keepalive msg once the socket is connected
      c->status_ = CONNECT_OK;
      send_keepalive_data(rs);
    } else {
      _LOG_DEBUG("connect failed, fd: %d, conn: %s", c->fd_, addr_to_string(client2rs(c)->svr_addr_));
      destroy_client(c);
    }
  }
}
void try_read_response(DestKeepAliveState *rs)
{
  int ret = OB_SUCCESS;
  client *c = rs->c_;
  int fd = c->fd_;
  bool need_disconn = false;
  while (true) {
    ssize_t n = -1;
    char buf[128];
    Header header;
    ObNetKeepAliveData ka_data;
    int32_t read_len = header.get_encoded_size();
    ObTimeGuard timeguard_clent_recv("net_keepalive_client_recv", 100 * 1000);
    while ((n = read(fd, buf, read_len)) < 0 && errno == EINTR);
    if (n <= 0) {
      if (0 == n || (errno != EAGAIN && errno != EWOULDBLOCK)) {
        LOG_INFO("socket need_disconn", K(n), K(fd), K(errno));
        need_disconn = true;
      }
      break;
    }
    timeguard_clent_recv.click();
    int tmp_ret = OB_SUCCESS;
    int64_t pos = 0;
    if (OB_SUCCESS != (tmp_ret = header.decode(buf, read_len, pos))) {
      _LOG_WARN("decode failed, ret: %d, pos: %ld", tmp_ret, pos);
    } else {
      char data[512];
      if (header.data_len_ > sizeof data) {
        tmp_ret = OB_BUF_NOT_ENOUGH;
        _LOG_WARN("data buf not enough: %d", header.data_len_);
      } else {
        while ((n = read(fd, data, header.data_len_)) < 0 && errno == EINTR);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
          LOG_INFO("socket need_disconn", K(n), K(fd), K(errno));
          need_disconn = true;
          break;
        }
        pos = 0;
        if (OB_SUCCESS != (tmp_ret = ka_data.decode(data, header.data_len_, pos))) {
          _LOG_WARN("decode failed, ret: %d, pos: %ld", tmp_ret, pos);
        }
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      memcpy(&rs->ka_data_, &ka_data, sizeof(ka_data));
    }
    // ignore decode error, make keepalive avaliable
    rs->last_read_ts_ = get_usec();
    c->wait_resp_ = false;
    if (rs->in_black_) {
      _LOG_INFO("whitewash, addr: %s, last_write_ts_=%ld, last_read_ts_=%ld", addr_to_string(rs->svr_addr_), rs->last_write_ts_, rs->last_read_ts_);
      rs->in_black_ = 0;
    }
  }
  if (need_disconn) {
    _LOG_INFO("client connection closed, fd: %d, conn: %s", c->fd_, addr_to_string(rs->svr_addr_));
    destroy_client(c);
  }
}

void ObNetKeepAlive::do_client_loop()
{
int ret = OB_SUCCESS;
  int64_t last_check_ts = 0;
  while (!has_set_stop()) {
    int64_t now = get_usec();
    int64_t past = now - last_check_ts;
    if (past < KEEPALIVE_INTERVAL) {
      ob_usleep(KEEPALIVE_INTERVAL - past);
    }
    last_check_ts = get_usec();
    // traverse all registed dest, send keepalive data, try to receive response and check if the dest is available
    bool dump_status = false;
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      dump_status = true;
    }
    int64_t regist_dest_count = ATOMIC_LOAD(&regist_dest_count_);
    for (int i = 0; i < regist_dest_count; i++) {
      ObTimeGuard timeguard_clent_send("net_keepalive_client_send", 100 * 1000);
      struct DestKeepAliveState *rs = ATOMIC_LOAD(&regist_dests_[i]);
      if (now - rs->last_access_ts_ > SERVER_EXPIRED_TIME) {
        if (rs->last_write_ts_ > 0) {
          _LOG_INFO("dest has not been accessed by the upper layer for a long time, addr: %s, last_access_time_=%ld", addr_to_string(rs->svr_addr_), rs->last_access_ts_);
          ATOMIC_STORE(&rs->last_write_ts_, 0);
          rs->in_black_ = 0;
          destroy_client(rs->c_);
        }
        continue;
      } else if (0 == rs->last_write_ts_) {
        rs->last_read_ts_ = get_usec();
        _LOG_INFO("dest added, start to send keepalive data, addr : %s", addr_to_string(rs->svr_addr_));
      }
      if (dump_status) {
        _LOG_INFO("dump dest keepalive data states, addr: %s, last_write_ts_=%ld, last_read_ts_=%ld, in_black_=%d",
          addr_to_string(rs->svr_addr_),rs->last_write_ts_, rs->last_read_ts_, rs->in_black_);
      }
      client *c = rs->c_;
      if (!c) {
        c = create_client(rs);
        if (NULL == c) continue;
      }
      if (CONNECTING == c->status_) {
        check_connect(rs);
      } else if (CONNECT_OK == c->status_ && false == c->wait_resp_) {
        send_keepalive_data(rs);
        update_write_ts(rs);
      }
      c = rs->c_;
      if (c != NULL) {
        try_read_response(rs);
      }
      now = get_usec();
      if (now - rs->last_write_ts_ > WINDOW_LENGTH) {
        _LOG_INFO("failed to get resp and destroy client, addr: %s, last_write_ts_=%ld, last_read_ts_=%ld", addr_to_string(rs->svr_addr_), rs->last_write_ts_, rs->last_read_ts_);
        destroy_client(c);
      }
      if (now - rs->last_read_ts_ > WINDOW_LENGTH) {
        if (!rs->in_black_) {
          _LOG_INFO("mark black, addr: %s, last_write_ts_=%ld, last_read_ts_=%ld", addr_to_string(rs->svr_addr_), rs->last_write_ts_, rs->last_read_ts_);
          rs->in_black_ = 1;
        }
      }
    }
  }
}

int ObNetKeepAlive::get_last_resp_ts(const common::ObAddr &addr, int64_t &last_resp_ts)
{
  int ret = OB_SUCCESS;
  last_resp_ts = OB_INVALID_TIMESTAMP;

  easy_addr_t ez_addr = to_ez_addr(addr);
  DestKeepAliveState *rs = regist_dest_if_need(ez_addr);
  if (rs != NULL) {
    last_resp_ts = ATOMIC_LOAD(&rs->last_read_ts_);
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

}//end of namespace obrpc
}//end of namespace oceanbase

extern "C" {
int is_net_keepalive_connection(ssize_t rbytes, char *buf)
{
  int bret = 0;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t header_magic = 0;
  uint16_t msg_body_len = 0;
  uint64_t group_magic  = 0;
  if (rbytes <= 0 || NULL == buf) {
    LOG_INFO("invalid argument", K(rbytes), KP(buf));
  } else if (OB_FAIL(decode_i64(buf, rbytes, pos, reinterpret_cast<int64_t *>(&header_magic)))) {
    LOG_INFO("decode header magic failed!", K(ret));
  } else if (header_magic != NEGOTIATION_PACKET_HEADER_MAGIC_EASY) {
    LOG_INFO("not negotiation msg! header magic does not match!", K(header_magic));
  } else if (OB_FAIL(decode_i16(buf, rbytes, pos, reinterpret_cast<int16_t *>(&msg_body_len)))) {
    LOG_INFO("decode msg body len failed!", K(ret));
  } else if (OB_FAIL(decode_i64(buf, rbytes, pos, reinterpret_cast<int64_t *>(&group_magic)))) {
    LOG_INFO("decode eio magic failed!", K(ret));
  } else {
    if (group_magic == ObNetEasy::NET_KEEPALIVE_MAGIC) {
      bret = 1;
    }
  }
  return bret;
}
}
