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

#ifndef CORO_THREAD_H
#define CORO_THREAD_H

#include <functional>
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_latch.h"
#include "lib/net/ob_addr.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace oceanbase {
namespace lib {
class ObPThread;

class Thread;
class Threads;
class IRunWrapper
{
public:
  virtual ~IRunWrapper() {}
  virtual int pre_run()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int end_run()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual uint64_t id() const = 0;
};

/// \class
/// A wrapper of Linux thread that supports normal thread operations.
class Thread {
public:
  friend class ObPThread;
  static constexpr int PATH_SIZE = 128;
  Thread(Threads *threads, int64_t idx, int64_t stack_size);
  ~Thread();

  int start();
  void stop();
  void run();
  void wait();
  void destroy();
  void dump_pth();
  pthread_t get_pthread() { return pth_; }

  /// \brief Get current thread object.
  ///
  /// \warning It would encounter segment fault if current thread
  /// isn't created with this class.
  static Thread &current();

  bool has_set_stop() const;
  uint64_t get_tenant_id() const;
  using ThreadListNode = common::ObDLinkNode<lib::Thread *>;
  ThreadListNode *get_thread_list_node() { return &thread_list_node_; }
  int get_cpu_time_inc(int64_t &cpu_time_inc);
  int64_t get_tid() { return tid_; }

  OB_INLINE static int64_t update_loop_ts(int64_t t)
  {
    int64_t ret = loop_ts_;
    loop_ts_ = t;
    ObLatch::clear_lock();
    return ret;
  }

  OB_INLINE static int64_t update_loop_ts()
  {
    return update_loop_ts(common::ObTimeUtility::fast_current_time());
  }
public:
  class BaseWaitGuard
  {
  public:
    OB_INLINE explicit BaseWaitGuard() : last_ts_(blocking_ts_)
    {
      if (0 == last_ts_) {
        loop_ts_ = blocking_ts_ = common::ObTimeUtility::fast_current_time();
      }
    }
    ~BaseWaitGuard()
    {
      if (0 == last_ts_) {
        blocking_ts_ = 0;
      }
    }
  private:
    int64_t last_ts_;
  };
  class WaitGuard : public BaseWaitGuard
  {
  public:
    OB_INLINE explicit WaitGuard(uint8_t type) : type_(type)
    {
      wait_event_ |= type;
    }
    ~WaitGuard()
    {
      wait_event_ &= ~type_;
    }
  private:
    uint8_t type_;
  };
  class JoinGuard : public BaseWaitGuard
  {
  public:
    OB_INLINE explicit JoinGuard(pthread_t thread)
    {
      thread_joined_ = thread;
    }
    ~JoinGuard()
    {
      thread_joined_ = 0;
    }
  };
  class RpcGuard : public BaseWaitGuard
  {
  public:
    OB_INLINE explicit RpcGuard(const easy_addr_t& addr, obrpc::ObRpcPacketCode pcode)
    {
      IGNORE_RETURN new (&rpc_dest_addr_) ObAddr(addr);
      pcode_ = pcode;
    }
    OB_INLINE explicit RpcGuard(const ObAddr& addr, obrpc::ObRpcPacketCode pcode)
    {
      IGNORE_RETURN new (&rpc_dest_addr_) ObAddr(addr);
      pcode_ = pcode;
    }
    ~RpcGuard()
    {
      rpc_dest_addr_.reset();
      pcode_ = obrpc::ObRpcPacketCode::OB_INVALID_RPC_CODE;
    }
  };

  static constexpr uint8_t WAIT                 = (1 << 0);
  static constexpr uint8_t WAIT_IN_TENANT_QUEUE = (1 << 1);
  static constexpr uint8_t WAIT_FOR_IO_EVENT    = (1 << 2);
  static constexpr uint8_t WAIT_FOR_LOCAL_RETRY = (1 << 3); //Statistics of local retry waiting time for dynamically increasing threads.
  static constexpr uint8_t WAIT_FOR_PX_MSG      = (1 << 4);
  // for thread diagnose, maybe replace it with union later.
  static thread_local int64_t loop_ts_;
  static thread_local pthread_t thread_joined_;
  static thread_local int64_t sleep_us_;
  static thread_local int64_t blocking_ts_;
  static thread_local ObAddr rpc_dest_addr_;
  static thread_local obrpc::ObRpcPacketCode pcode_;
  static thread_local uint8_t wait_event_;
private:
  static void* __th_start(void *th);
  int try_wait();
  void destroy_stack();
  static thread_local Thread* current_thread_;

private:
  static int64_t total_thread_count_;
private:
  pthread_t pth_;
  Threads *threads_;
  int64_t idx_;
#ifndef OB_USE_ASAN
  void *stack_addr_;
#endif
  int64_t stack_size_;
  bool stop_;
  int64_t join_concurrency_;
  pid_t pid_before_stop_;
  pid_t tid_before_stop_;
  int64_t tid_;
  ThreadListNode thread_list_node_;
  int64_t cpu_time_;
  int create_ret_;
};

OB_INLINE bool Thread::has_set_stop() const
{
  IGNORE_RETURN update_loop_ts();
  return stop_;
}

extern int get_max_thread_num();
}  // lib
}  // oceanbase

#endif /* CORO_THREAD_H */
