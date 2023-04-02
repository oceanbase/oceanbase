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

#include "lib/signal/ob_signal_worker.h"
#include <poll.h>
#include <sys/prctl.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/signal/ob_signal_processor.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/utility.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase
{
using namespace lib;

namespace common
{

int g_fd = -1;
bool g_inited = false;

int send_request_and_wait(ObSigRequestCode code, int exclude_tid)
{
  int ret = OB_SUCCESS;
#ifdef __x86_64__
  DTraceId trace_id = DTraceId::gen_trace_id();
  DTraceIdGuard trace_guard(trace_id);
  ObSigRequest req;
  if (!g_inited) {
    ret = OB_NOT_INIT;
    DLOG(WARN, "global status not ready, ret=%d", ret);
  } else {
    req.trace_id_ = trace_id;
    req.code_ = code;
    req.exclude_tid_ = exclude_tid;
    req.inc_and_fetch_ref();
    req.inc_and_fetch_ref();
    bool sended = false;
    DEFER(
      req.dec_and_fetch_ref();
      const int64_t interval = 50 * 1000;
      int64_t left = 20 * interval; // 10s
      while ((left-=interval) > 0 &&
             req.fetch_ref() > (sended ? 0 : 1)) {
        safe_sleep_micros(interval);
      }
      if (left > 0) {
        CLOSE(req.fd_[0]);
        CLOSE(req.fd_[1]);
        CLOSE(req.fd2_[0]);
        CLOSE(req.fd2_[1]);
      } else {
        // fd would be leak while timeout, bydesign
      }
    );
    DLOG(DEBUG, "client start");
    if (0 != pipe2(req.fd_, O_CLOEXEC)) {
      ret = OB_ERROR;
      DLOG(WARN, "pipe failed");
    } else if (0 != pipe2(req.fd2_, O_CLOEXEC)) {
      ret = OB_ERROR;
      DLOG(WARN, "pipe failed");
    } else {
      ObSigRequest *p_req = &req;
      write(g_fd, &p_req, sizeof(p_req));
      sended = true;
    }
    if (OB_SUCC(ret)) {
      int ack = 0;
      int64_t timeout = 5 * 1000; // 5s
      if (OB_FAIL(wait_readable(req.fd_[0], timeout))) {
        DLOG(WARN, "wait_readable failed, ret=%d", ret);
      } else {
        size_t n = read(req.fd_[0], &ack, sizeof(ack));
        if (-1 == n) {
          DLOG(WARN, "read failed, errno=%d", errno);
        } else if (n != sizeof(ack)) {
          ret = OB_ERR_UNEXPECTED;
          DLOG(WARN, "unexpected nbytes, n=%ld", n);
        } else {
          DLOG(DEBUG, "peer process done, ret=%d", ack);
        }
      }
    }
  }
#endif
  return ret;
}

template<typename Func, typename ... Args>
void iter_task(Func &&cb, int exclude_tid, Args && ... args)
{
  struct linux_dirent64 {
    ino64_t d_ino;
    off64_t d_off;
    unsigned short d_reclen;
    unsigned char d_type;
    char d_name[];
  };
  int fd = ::open("/proc/self/task/", O_DIRECTORY |  O_RDONLY);
  if (-1 == fd) {
  } else {
    int tgid = getpid();
    int self_tid = syscall(SYS_gettid);
    char buf[1024];
    ssize_t nread = 0;
    do {
      nread = syscall(SYS_getdents64, fd, buf, sizeof(buf));
      if (nread < 0) {
        // error
      } else if (0 == nread) {
        // end
      } else {
        ssize_t offset = 0;
        int64_t tid = -1;
        while (offset < nread) {
          linux_dirent64* dirent = reinterpret_cast<linux_dirent64*>(buf + offset);
          if (strcmp(dirent->d_name, ".") != 0 &&
              strcmp(dirent->d_name, "..") != 0 &&
              (tid = atoi(dirent->d_name)) != self_tid &&
              tid != exclude_tid) {
            cb(tgid, tid, args...);
          }
          offset += dirent->d_reclen;
        }
      }
    } while (nread > 0);
  }
  if (fd >= 0) {
    ::close(fd);
  }
}

ObSignalWorker::ObSignalWorker()
{
}

ObSignalWorker::~ObSignalWorker()
{
}

// ignore ret
#define NEW_PROCESSOR(Processor, args...)                        \
  do {                                                           \
    void *ptr = nullptr;                                         \
    Processor *tmp_processor = nullptr;                          \
    if (OB_ISNULL(ptr = ob_malloc(sizeof(Processor), attr))) {   \
    } else if (FALSE_IT(tmp_processor = new (ptr) Processor(args))) {   \
    } else {                                                     \
      processor = tmp_processor;                                 \
    }                                                            \
  } while (0)

void task_process(int, int, ObSigRequest*, ObSigProcessor*);
void ObSignalWorker::run1()
{
  lib::set_thread_name("SignalWorker");
  int ret = OB_SUCCESS;

  int fd[2] = {-1, -1};
  // init pipe
  if (0 != pipe2(fd, O_CLOEXEC)) {
    ret = OB_ERROR;
    DLOG(WARN, "pipe failed");
  } else {
    g_fd = fd[1];
    g_inited = true;
    ObSigHandler handler;
    g_sig_handler_ctx_.handler_ = &handler;

    static int64_t n_req = 0;
    while (OB_SUCC(ret) && !has_set_stop()) {
      int64_t timeout = 10 * 1000; // 10s
      if (OB_FAIL(wait_readable(fd[0], timeout))) {
        if (OB_TIMEOUT == ret) {
          ret = OB_SUCCESS;
        } else {
          DLOG(WARN, "wait_readable failed, ret=%d", ret);
        }
      } else {
        for (;;) {
          ObSigRequest *req = nullptr;
          size_t n = read(fd[0], &req, sizeof(req));
          if (-1 == n) {
            if (errno != EAGAIN) {
              DLOG(WARN, "read failed, errno=%d", errno);
            }
            break;
          } else if (n != sizeof(req)) {
            ret = OB_ERR_UNEXPECTED;
            DLOG(WARN, "unexpected nbytes, n=%ld", n);
            break;
          } else if (!req->check_magic()) {
            ret = OB_ERR_UNEXPECTED;
            DLOG(WARN, "unexpected magic");
          } else {
            DEFER(
              req->dec_and_fetch_ref();
            );
            n_req++;
            DTraceIdGuard trace_guard(req->trace_id_);
            DLOG(INFO, "receive request, req=%d, accumulated=%ld",
                 req->code_, n_req);
            ObSigProcessor *processor = nullptr;
            DEFER(
              if (processor != nullptr) {
                processor->~ObSigProcessor();
                ob_free(processor);
              }
            );
            ObMemAttr attr;
            attr.label_ = "TraceProcessor";
            attr.prio_ = lib::OB_HIGH_ALLOC;
            switch (req->code_)
            {
              case VERB_LEVEL_1:
              {
                NEW_PROCESSOR(ObSigBTOnlyProcessor);
                break;
              }
              case VERB_LEVEL_2:
              {
                NEW_PROCESSOR(ObSigBTSQLProcessor);
                break;
              }
              default:
              {
                ret = OB_ERR_UNEXPECTED;
                DLOG(WARN, "unexpected req, req=%d", req->code_);
                break;
              }
            } // end switch

            int ack = ret;
            if (OB_SUCC(ret)) {
              if (nullptr == processor) {
                ack = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                processor->start();
                iter_task(task_process, req->exclude_tid_, req, processor);
                processor->end();
              }
            }

            // notify client
            write(req->fd_[1], &ack, sizeof(ack));
          }
        }
      }
    } // end while
  }

  DLOG(INFO, "signal worker exit, ret=%d", ret);
}

int ObSignalWorker::start()
{
  ThreadPool::start();
  return OB_SUCCESS;
}

void ObSignalWorker::stop()
{
  ThreadPool::stop();
}

void ObSignalWorker::wait()
{
  ThreadPool::wait();
}

void task_process(int tgid, int tid, ObSigRequest *req, ObSigProcessor *processor)
{
  int ret = OB_SUCCESS;

  auto *ctx = &g_sig_handler_ctx_;
  ctx->trace_id_ = req->trace_id_;
  ctx->processor_ = processor;
  DEFER(
    CLOSE(ctx->fd_[0]);
    CLOSE(ctx->fd_[1]);
    CLOSE(ctx->fd2_[0]);
    CLOSE(ctx->fd2_[1]);
  );
  static int64_t req_id = 0;
  ctx->atomic_set_req_id(ATOMIC_AAF(&req_id, 1));
  DEFER(ctx->atomic_set_req_id(0));
  // init pipe
  if (0 != pipe2(ctx->fd_, O_CLOEXEC)) {
    ret = OB_ERROR;
    DLOG(WARN, "pipe failed");
  } else if (0 != pipe2(ctx->fd2_, O_CLOEXEC)) {
    ret = OB_ERROR;
    DLOG(WARN, "pipe failed");
  } else {
    siginfo_t si;
    memset(&si, 0, sizeof(si));
    si.si_code = SI_QUEUE;
    si.si_value.sival_ptr = (void*)req_id;
    // notify peer to prepare
    int r = syscall(SYS_rt_tgsigqueueinfo, tgid, tid, MP_SIG, &si);
    if (r < 0) {
      DLOG(WARN, "sigqueue failed, errno=%d", errno);
    } else {
      int ack;
      // wait peer prepare done
      int64_t timeout = 50; // 50ms
      if (OB_FAIL(wait_readable(ctx->fd_[0], timeout))) {
        DLOG(WARN, "wait_readable failed, ret=%d", ret);
      } else {
        size_t n = read(ctx->fd_[0], &ack, sizeof(ack));
        if (-1 == n) {
          ret = OB_ERR_UNEXPECTED;
          DLOG(WARN, "read failed, errno=%d", errno);
        } else if (n != sizeof(ack)) {
          ret = OB_ERR_UNEXPECTED;
          DLOG(WARN, "unexpected");
        } else if (OB_FAIL(ack)) {
          DLOG(WARN, "peer failed, ret=%d", ack);
        } else {
          DLOG(DEBUG, "process...");
          ack = ctx->processor_->process();
          // notify peer to exit
          write(ctx->fd2_[1], &ack, sizeof(ack));
          // wait peer exit
          n = read(ctx->fd_[0], &ack, sizeof(ack));
          if (-1 == n) {
            DLOG(WARN, "read failed, errno=%d", errno);
          } else if (n != sizeof(ack)) {
            ret = OB_ERR_UNEXPECTED;
            DLOG(WARN, "unexpected");
          } else {
            DLOG(DEBUG, "exit, peer ret=%d", ack);
          }
        }
      }
    }
  }
}

void ObSigHandler::handle(ObSigHandlerCtx &ctx)
{
  int ret = OB_SUCCESS;
  LoggerSwitchGuard guard(false/*open*/);
  DTraceIdGuard trace_guard(ctx.trace_id_);

  DLOG(DEBUG, "prepare...");
  ret = ctx.processor_->prepare();

  int ack = ret;
  // notify peer prepare done
  write(ctx.fd_[1], &ack, sizeof(ack));

  // wait peer process done or timeout
  bool go_on = true;
  while (OB_SUCC(ret) && go_on) {
    int64_t timeout = 100; // 100ms
    if (OB_FAIL(wait_readable(ctx.fd2_[0], timeout))) {
      DLOG(WARN, "wait_readable failed, ret=%d", ret);
    } else {
      size_t n = read(ctx.fd2_[0], &ack, sizeof(ack));
      if (-1 == n) {
        DLOG(WARN, "read failed, errno=%d", errno);
      } else if (n != sizeof(ack)) {
        ret = OB_ERR_UNEXPECTED;
        DLOG(WARN, "unexpected nbytes, n=%ld", n);
      } else {
        DLOG(DEBUG, "peer process done, ret=%d", ack);
        go_on = false;
      }
    }
  }

  ack = ret;
  // notify peer exit done
  write(ctx.fd_[1], &ack, sizeof(ack));
}

} // namespace common
} // namespace oceanbase
