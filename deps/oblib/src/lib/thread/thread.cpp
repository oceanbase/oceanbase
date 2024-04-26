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

#define USING_LOG_PREFIX LIB

#include "thread.h"
#include "threads.h"
#include <pthread.h>
#include <sys/syscall.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_running_mode.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/rc/context.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/signal/ob_signal_struct.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
thread_local int64_t Thread::loop_ts_ = 0;
thread_local pthread_t Thread::thread_joined_ = 0;
thread_local int64_t Thread::sleep_us_ = 0;
thread_local int64_t Thread::blocking_ts_ = 0;
thread_local ObAddr Thread::rpc_dest_addr_;
thread_local obrpc::ObRpcPacketCode Thread::pcode_ = obrpc::ObRpcPacketCode::OB_INVALID_RPC_CODE;
thread_local uint8_t Thread::wait_event_ = 0;
thread_local Thread* Thread::current_thread_ = nullptr;
int64_t Thread::total_thread_count_ = 0;

Thread &Thread::current()
{
  assert(current_thread_ != nullptr);
  return *current_thread_;
}

Thread::Thread(Threads *threads, int64_t idx, int64_t stack_size)
    : pth_(0),
      threads_(threads),
      idx_(idx),
#ifndef OB_USE_ASAN
      stack_addr_(nullptr),
#endif
      stack_size_(stack_size),
      stop_(true),
      join_concurrency_(0),
      pid_before_stop_(0),
      tid_before_stop_(0),
      tid_(0),
      thread_list_node_(this),
      cpu_time_(0),
      create_ret_(OB_NOT_RUNNING)
{}

Thread::~Thread()
{
  destroy();
}

int Thread::start()
{
  int ret = OB_SUCCESS;
  const int64_t count = ATOMIC_FAA(&total_thread_count_, 1);
  if (count >= get_max_thread_num() - OB_RESERVED_THREAD_NUM) {
    ATOMIC_FAA(&total_thread_count_, -1);
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("thread count reach limit", K(ret), "current count", count);
  } else if (stack_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid stack_size", K(ret), K(stack_size_));
#ifndef OB_USE_ASAN
  } else if (OB_ISNULL(stack_addr_ = g_stack_allocer.alloc(0 == GET_TENANT_ID() ? OB_SERVER_TENANT_ID : GET_TENANT_ID(), stack_size_ + SIG_STACK_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc stack memory failed", K(stack_size_));
#endif
  } else {
    pthread_attr_t attr;
    bool need_destroy = false;
    int pret = pthread_attr_init(&attr);
    if (pret == 0) {
      need_destroy = true;
#ifndef OB_USE_ASAN
      pret = pthread_attr_setstack(&attr, stack_addr_, stack_size_);
#endif
    }
    if (pret == 0) {
      stop_ = false;
      pret = pthread_create(&pth_, &attr, __th_start, this);
      if (pret != 0) {
        LOG_ERROR("pthread create failed", K(pret), K(errno));
        pth_ = 0;
      } else {
        while (ATOMIC_LOAD(&create_ret_) == OB_NOT_RUNNING) {
          sched_yield();
        }
        if (OB_FAIL(create_ret_)) {
          LOG_ERROR("thread create failed", K(create_ret_));
        }
      }
    }
    if (0 != pret) {
      ATOMIC_FAA(&total_thread_count_, -1);
      ret = OB_ERR_SYS;
      stop_ = true;
    }
    if (need_destroy) {
      pthread_attr_destroy(&attr);
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void Thread::stop()
{
  bool stack_addr_flag = true;
#ifndef OB_USE_ASAN
  stack_addr_flag = (stack_addr_ != NULL);
#endif
#ifdef ERRSIM
  if (!stop_
      && stack_addr_flag
      && 0 != (OB_E(EventTable::EN_THREAD_HANG) 0)) {
    int tid_offset = 720;
    int tid = *(pid_t*)((char*)pth_ + tid_offset);
    LOG_WARN_RET(OB_SUCCESS, "stop was ignored", K(tid));
    return;
  }
#endif
#ifndef OB_USE_ASAN
  if (!stop_ && stack_addr_ != NULL) {
    int tid_offset = 720;
    int pid_offset = 724;
    int len = (char*)stack_addr_ + stack_size_ - (char*)pth_;
    if (len >= (max(tid_offset, pid_offset) + sizeof(pid_t))) {
      tid_before_stop_ = *(pid_t*)((char*)pth_ + tid_offset);
      pid_before_stop_ = *(pid_t*)((char*)pth_ + pid_offset);
    }
  }
#endif
  stop_ = true;
}

uint64_t Thread::get_tenant_id() const
{
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
  IRunWrapper *run_wrapper_ = threads_->get_run_wrapper();
  if (OB_NOT_NULL(run_wrapper_)) {
    tenant_id = run_wrapper_->id();
  }
  return tenant_id;
}

void Thread::run()
{
  IRunWrapper *run_wrapper_ = threads_->get_run_wrapper();
  if (OB_NOT_NULL(run_wrapper_)) {
    run_wrapper_->pre_run();
    threads_->run(idx_);
    run_wrapper_->end_run();
  } else {
    threads_->run(idx_);
  }
}

void Thread::dump_pth() // for debug pthread join faileds
{
#ifndef OB_USE_ASAN
  int ret = OB_SUCCESS;
  int fd = 0;
  int64_t len = 0;
  ssize_t size = 0;
  char path[PATH_SIZE];
  len = (char*)stack_addr_ + stack_size_ - (char*)pth_;
  snprintf(path, PATH_SIZE, "log/dump_pth.%p.%d", (char*)pth_, static_cast<pid_t>(syscall(__NR_gettid)));
  LOG_WARN("dump pth start", K(path), K(pth_), K(len), K(stack_addr_), K(stack_size_));
  if (NULL == (char*)pth_ || len >= stack_size_ || len <= 0) {
    LOG_WARN("invalid member", K(pth_), K(stack_addr_), K(stack_size_));
  } else if ((fd = ::open(path, O_WRONLY | O_CREAT | O_TRUNC,
                          S_IRUSR  | S_IWUSR | S_IRGRP)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to create file", KERRMSG, K(ret));
  } else if (len != (size = write(fd, (char*)(pth_), len))) {
    ret = OB_IO_ERROR;
    LOG_WARN("dump pth fail", K(errno), KERRMSG, K(len), K(size), K(ret));
    if (0 != close(fd)) {
      LOG_WARN("fail to close file fd", K(fd), K(errno), KERRMSG, K(ret));
    }
  } else if (::fsync(fd) != 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("sync pth fail", K(errno), KERRMSG, K(len), K(size), K(ret));
    if (0 != close(fd)) {
      LOG_WARN("fail to close file fd", K(fd), K(errno), KERRMSG, K(ret));
    }
  } else if (0 != close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to close file fd", K(fd), KERRMSG, K(ret));
  } else {
    LOG_WARN("dump pth done", K(path), K(pth_), K(size));
  }
#endif
}

void Thread::wait()
{
  int ret = 0;
  if (pth_ != 0) {
    if (0 != (ret = pthread_join(pth_, nullptr))) {
      LOG_ERROR("pthread_join failed", K(ret), K(errno));
#ifndef OB_USE_ASAN
      dump_pth();
      ob_abort();
#endif
    }
    destroy_stack();
  }
}

int Thread::try_wait()
{
  int ret = OB_SUCCESS;
  if (pth_ != 0) {
    int pret = 0;
    if (0 != (pret = pthread_tryjoin_np(pth_, nullptr))) {
      ret = OB_EAGAIN;
      LOG_WARN("pthread_tryjoin_np failed", K(pret), K(errno), K(ret));
    } else {
      destroy_stack();
    }
  }
  return ret;
}

void Thread::destroy()
{
  if (pth_ != 0) {
    /* NOTE: must wait pthread quit before release user_stack
       because the pthread's tcb was allocated from it */
    wait();
  } else {
    destroy_stack();
  }
}

void Thread::destroy_stack()
{
#ifndef OB_USE_ASAN
  if (stack_addr_ != nullptr) {
    g_stack_allocer.dealloc(stack_addr_);
    stack_addr_ = nullptr;
    pth_ = 0;
  }
#else
  pth_ = 0;
#endif
}

void* Thread::__th_start(void *arg)
{
  Thread * const th = reinterpret_cast<Thread*>(arg);
  ob_set_thread_tenant_id(th->get_tenant_id());
  current_thread_ = th;
  th->tid_ = gettid();
#ifndef OB_USE_ASAN
  ObStackHeader *stack_header = ProtectedStackAllocator::stack_header(th->stack_addr_);
  abort_unless(stack_header->check_magic());

  #ifndef OB_USE_ASAN
  /**
    signal handler stack
   */
  stack_t nss;
  stack_t oss;
  bzero(&nss, sizeof(nss));
  bzero(&oss, sizeof(oss));
  nss.ss_sp = &((char*)th->stack_addr_)[th->stack_size_];
  nss.ss_size = SIG_STACK_SIZE;
  bool restore_sigstack = false;
  if (-1 == sigaltstack(&nss, &oss)) {
    LOG_WARN_RET(OB_ERR_SYS, "sigaltstack failed, ignore it", K(errno));
  } else {
    restore_sigstack = true;
  }
  DEFER(if (restore_sigstack) { sigaltstack(&oss, nullptr); });
  #endif

  stack_header->pth_ = (uint64_t)pthread_self();
#endif

  int ret = OB_SUCCESS;
  if (OB_ISNULL(th)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(th), K(ret));
  } else {
    // pm析构逻辑上会访问其它pthread_key绑定的对象，为了避免析构顺序的影响
    // pm不用TSI而是自己做线程局部(__thread)
    // create page manager
    ObPageManager pm;
    ret = pm.set_tenant_ctx(common::OB_SERVER_TENANT_ID, common::ObCtxIds::GLIBC);
    if (OB_FAIL(ret)) {
      LOG_ERROR("set tenant ctx failed", K(ret));
    } else {
      ObPageManager::set_thread_local_instance(pm);
      MemoryContext *mem_context = GET_TSI0(MemoryContext);
      if (OB_ISNULL(mem_context)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null ptr", K(ret));
      } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(*mem_context,
                         ContextParam().set_properties(RETURN_MALLOC_DEFAULT)
                                       .set_label("ThreadRoot")))) {
        LOG_ERROR("create memory context failed", K(ret));
      } else {
        WITH_CONTEXT(*mem_context) {
          try {
            in_try_stmt = true;
            ATOMIC_STORE(&th->create_ret_, OB_SUCCESS);
            th->run();
            in_try_stmt = false;
          } catch (OB_BASE_EXCEPTION &except) {
            // we don't catch other exception because we don't know how to handle it
            _LOG_ERROR("Exception caught!!! errno = %d, exception info = %s", except.get_errno(), except.what());
            ret = OB_ERR_UNEXPECTED;
            in_try_stmt = false;
            if (1 == th->threads_->get_thread_count() && !th->has_set_stop()) {
              LOG_WARN("thread exit by itself without set_stop", K(ret));
              th->threads_->stop();
            }
          }
        }
      }
      if (mem_context != nullptr && *mem_context != nullptr) {
        DESTROY_CONTEXT(*mem_context);
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_STORE(&th->create_ret_, ret);
  }
  ATOMIC_FAA(&total_thread_count_, -1);
  return nullptr;
}

int Thread::get_cpu_time_inc(int64_t &cpu_time_inc)
{
  int ret = OB_SUCCESS;
  const pid_t pid = getpid();
  const int64_t tid = tid_;
  int64_t cpu_time = 0;
  cpu_time_inc = 0;

  int fd = -1;
  int64_t read_size = -1;
  int32_t PATH_BUFSIZE = 512;
  int32_t MAX_LINE_LENGTH = 1024;
  int32_t VALUE_BUFSIZE = 32;
  char stat_path[PATH_BUFSIZE];
  char stat_content[MAX_LINE_LENGTH];

  if (tid == 0) {
    ret = OB_NOT_INIT;
  } else {
    snprintf(stat_path, PATH_BUFSIZE, "/proc/%d/task/%ld/stat", pid, tid);
    if ((fd = ::open(stat_path, O_RDONLY)) < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("open file error", K((const char *)stat_path), K(errno), KERRMSG, K(ret));
    } else if ((read_size = read(fd, stat_content, MAX_LINE_LENGTH)) < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("read file error",
          K((const char *)stat_path),
          K((const char *)stat_content),
          K(ret),
          K(errno),
          KERRMSG,
          K(ret));
    } else {
      // do nothing
    }
    if (fd >= 0) {
      close(fd);
    }
  }

  if (OB_SUCC(ret)) {
    const int USER_TIME_FIELD_INDEX = 13;
    const int SYSTEM_TIME_FIELD_INDEX = 14;
    int field_index = 0;
    char *save_ptr = nullptr;
    char *field_ptr = strtok_r(stat_content, " ", &save_ptr);
    while (field_ptr != NULL) {
      if (field_index == USER_TIME_FIELD_INDEX) {
        cpu_time += strtoul(field_ptr, NULL, 10) * 1000000 / sysconf(_SC_CLK_TCK);
      }
      if (field_index == SYSTEM_TIME_FIELD_INDEX) {
        cpu_time += strtoul(field_ptr, NULL, 10) * 1000000 / sysconf(_SC_CLK_TCK);
        break;
      }
      field_ptr = strtok_r(NULL, " ", &save_ptr);
      field_index++;
    }
    cpu_time_inc = cpu_time - cpu_time_;
    cpu_time_ = cpu_time;
  }
  return ret;
}

namespace oceanbase
{
namespace lib
{
int __attribute__((weak)) get_max_thread_num()
{
  return 4096;
}
}
}
