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

#include "ob_lua_api.h"
#include "ob_lua_handler.h"

#include <algorithm>
#include <functional>
#include <thread>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/utility/utility.h"
#include "lib/thread/thread.h"

extern "C" {
  #include <lua.h>
  #include <lauxlib.h>
  #include <lualib.h>
extern int ob_epoll_wait(int __epfd, struct epoll_event *__events,
		                     int __maxevents, int __timeout);
}

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::diagnose;

void ObLuaHandler::memory_update(const int size)
{
  if (0 == size) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "bad param", K(size));
  } else if (size > 0) {
    ++alloc_count_;
    alloc_size_ += size;
  } else {
    ++free_count_;
    free_size_ -= size;
  }
}

void *ObLuaHandler::realloc_functor(void *userdata, void *ptr, size_t osize, size_t nsize)
{
  void *ret = nullptr;
  UNUSED(userdata);
  UNUSED(osize);
  if (0 == nsize) {
    // do nothing
  } else if (OB_NOT_NULL(ret = diagnose::alloc(nsize))) {
    if (OB_NOT_NULL(ptr)) {
      memmove(ret, ptr, std::min(osize, nsize));
    }
  }
  if (OB_NOT_NULL(ptr)) {
    diagnose::free(ptr);
  }
  return ret;
}

int ObLuaHandler::process(const char* lua_code)
{
  bool has_segv = false;
  struct LuaExec {
    LuaExec(const char* lua_code) : code_(lua_code) {}
    void operator()() {
      lua_State* L = lua_newstate(realloc_functor, nullptr);
      if (OB_ISNULL(L)) {
        OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "luastate is NULL");
      } else {
        luaL_openlibs(L);
        APIRegister::get_instance().register_api(L);
        try {
          luaL_dostring(L, code_);
        } catch (std::exception& e) {
          _OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "exception during lua code execution, reason %s", e.what());
        }
        lua_close(L);
      }
    }
    const char* code_;
  };
  OB_LOG(INFO, "Lua code was executed", K(alloc_count_), K(free_count_), K(alloc_size_), K(free_size_));
  LuaExec func(lua_code);
  do_with_crash_restore(func, has_segv);
  if (has_segv) {
    _OB_LOG(INFO, "restore from sigsegv, coredump during lua code execution at %s\n", crash_restore_buffer);
  } else {
    OB_LOG(INFO, "Lua code was executed successfully", K(alloc_count_), K(free_count_), K(alloc_size_), K(free_size_));
  }

  struct LuaGC {
    LuaGC(common::ObVector<Function>& destructors) : destructors_(destructors) {}
    void operator()() {
      for (int64_t i = destructors_.size() - 1; i >= 0 ; --i) {
        destructors_[i]();
      }
      destructors_.clear();
    }
    common::ObVector<Function>& destructors_;
  };
  LuaGC gc(destructors_);
  do_with_crash_restore(gc, has_segv);
  if (has_segv) {
    _OB_LOG(INFO, "restore from sigsegv, coredump during lua gc at %s\n", crash_restore_buffer);
  } else {
    OB_LOG(INFO, "Lua gc successfully", K(alloc_count_), K(free_count_), K(alloc_size_), K(free_size_));
  }
  return OB_SUCCESS;
}

void ObUnixDomainListener::run1()
{
  int ret = OB_SUCCESS;
  if ((listen_fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0)) < 0) {
    OB_LOG(ERROR, "ObUnixDomainListener socket init failed", K(errno));
    ret = OB_ERR_UNEXPECTED;
  } else {
    struct sockaddr_un s;
    struct epoll_event listen_ev;
    int epoll_fd = epoll_create(256);
    s.sun_family = AF_UNIX;
    strncpy(s.sun_path, addr, sizeof(s.sun_path) - 1);
    unlink(addr);
    listen_ev.events = EPOLLIN;    
    listen_ev.data.fd = listen_fd_;
    if (bind(listen_fd_, (struct sockaddr *)&s, sizeof(s)) < 0) {
      OB_LOG(ERROR, "ObUnixDomainListener bind failed", K(listen_fd_), K(errno));
      ret = OB_ERR_UNEXPECTED;
    } else if (listen(listen_fd_, MAX_CONNECTION_QUEUE_LENGTH) < 0) {
      OB_LOG(ERROR, "ObUnixDomainListener listen failed", K(listen_fd_), K(errno));
      ret = OB_ERR_UNEXPECTED;
    } else if (epoll_fd < 0) {
      OB_LOG(ERROR, "ObUnixDomainListener create epoll failed", K(errno));
      ret = OB_ERR_UNEXPECTED;
    } else if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd_, &listen_ev) < 0) {
      OB_LOG(ERROR, "ObUnixDomainListener add listen to epoll failed", K(errno));
      ret = OB_ERR_UNEXPECTED;
    } else {
      lib::set_thread_name("LuaHandler");
      constexpr int64_t EPOLL_EVENT_BUFFER_SIZE = 32;
      constexpr int64_t TIMEOUT = 1000;
      struct epoll_event events[EPOLL_EVENT_BUFFER_SIZE];
      struct epoll_event conn_ev;
      char *code_buffer = (char *)diagnose::alloc(CODE_BUFFER_SIZE);
      while (OB_LIKELY(!has_set_stop())) {
        int conn_fd = -1;
        int ret = OB_SUCCESS;
        int64_t event_cnt = ob_epoll_wait(epoll_fd, events, EPOLL_EVENT_BUFFER_SIZE, TIMEOUT);
        if (event_cnt < 0) {
          if (EINTR == errno) {
            // timeout, ignore
          } else {
            OB_LOG(ERROR, "ObUnixDomainListener epoll wait failed", K(epoll_fd), K(errno));
          }
        }
        for (int64_t i = 0; i < event_cnt; ++i) {
          if (events[i].data.fd == listen_fd_) {
            if ((conn_fd = accept(listen_fd_, NULL, NULL)) < 0) {
              if (EAGAIN != errno) {
                ret = OB_ERR_UNEXPECTED;
                OB_LOG(ERROR, "ObUnixDomainListener accept failed", K(listen_fd_), K(errno));
              }
            } else {
              conn_ev.events = EPOLLIN;
              conn_ev.data.fd = conn_fd;
              if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &conn_ev) < 0) {
                OB_LOG(ERROR, "ObUnixDomainListener add event to epoll failed", K(epoll_fd), K(conn_fd), K(errno));
              }
            }
          } else if (events[i].events & EPOLLIN) {
            int rbytes = 0;
            char *buffer = code_buffer;
            int64_t size = CODE_BUFFER_SIZE;
            memset(code_buffer, 0, CODE_BUFFER_SIZE);
            while ((rbytes = read(events[i].data.fd, buffer, size)) > 0) {
              buffer += rbytes;
              size -= rbytes;
            }
            if (rbytes < 0) {
              OB_LOG(ERROR, "ObUnixDomainListener read from socket failed", K(errno));
            } else if (FALSE_IT(APIRegister::get_instance().set_fd(events[i].data.fd))) {
              // do nothing
            } else if (OB_FAIL(ObLuaHandler::get_instance().process(code_buffer))) {
              OB_LOG(ERROR, "ObUnixDomainListener process failed", K(ret));
            } else if (OB_FAIL(APIRegister::get_instance().flush())) {
              OB_LOG(ERROR, "ObUnixDomainListener flush failed", K(ret));
            } else {
              // do nothing
            }
            if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, events[i].data.fd, nullptr) < 0) {
              OB_LOG(ERROR, "ObUnixDomainListener del failed", K(errno));
            }
            close(events[i].data.fd);
            APIRegister::get_instance().set_fd(-1);
          } else {
            OB_LOG(ERROR, "unexpected type");
          }
        }
      }
      diagnose::free(code_buffer);
      close(listen_fd_);
      listen_fd_ = -1;
      close(epoll_fd);
      OB_LOG(INFO, "ObUnixDomainListener running");
    }
  }
}
