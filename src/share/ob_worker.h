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

#ifndef OCEANBASE_SHARE_OB_WORKER_H
#define OCEANBASE_SHARE_OB_WORKER_H

#include "lib/worker.h"
#include "lib/coro/co.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/runtime.h"

namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
}

namespace share {
using common::ObArenaAllocator;
using common::ObFIFOAllocator;
using common::ObIAllocator;

class ObWorker : public lib::Worker {
public:
  enum RunStatus { RS_RUN, RS_WAIT, RS_PAUSED };
  enum Status { WS_WAIT, WS_NOWAIT, WS_FREQUENT, WS_INVALID, WS_OUT_OF_THROTTLE };
  using lib::Worker::CompatMode;

  ObWorker();
  virtual ~ObWorker();

  virtual Status check_wait();

  virtual int check_status() override
  {
    check_wait();
    return common::OB_SUCCESS;
  }

  virtual int check_large_query_quota()
  {
    return common::OB_SUCCESS;
  }

  ////
  // Compatibility Mode
  static void set_compatibility_mode(CompatMode mode)
  {
    lib::set_compat_mode(mode);
  }
  static CompatMode get_compatibility_mode()
  {
    return lib::get_compat_mode();
  }

  bool is_timeout_ts_valid()
  {
    return INT64_MAX != timeout_ts_;
  }
  void set_timeout_ts(int64_t timeout_ts);
  int64_t get_timeout_ts() const;
  int64_t get_timeout_remain() const;
  bool is_timeout() const;

  void set_rpc_tenant(uint64_t tenant_id);
  void reset_rpc_tenant();
  uint64_t get_rpc_tenant() const;

  ObIAllocator& ssstore_allocator();
  ObFIFOAllocator& ssstore_fifo_allocator();
  void set_tidx(int64_t tidx);
  void unset_tidx();
  int64_t get_tidx() const;

  void set_session(oceanbase::sql::ObSQLSessionInfo* session)
  {
    session_ = session;
  }

  // It's called when current query can't been retry, maybe some
  // shared states have been published such as part of result has been
  // sent to client, operation is doing or even has done and can't
  // restart easily. It doesn't mean this query won't retry, but
  // indicates following retry flag setting is forbade,
  // i.e. set_retry_flag() will return false unless retry flag has
  // been set from then on.
  virtual void disable_retry();
  // It's used to check whether query need retry. Whenever worker has
  // observed this query need retry, it should stop processing this
  // query immediately.
  virtual bool need_retry() const;
  // Set retry flag so that scheduler will reprocess this request
  // then. If retry is disabled then return value of retry flag, or
  // set retry flag and return true.
  virtual bool set_retry_flag();
  virtual void reset_retry_flag();

  // Set large token expired timestamp.
  //
  // Worker prefer process large queries than normal queries until
  // token expires, that is the timestamp is larger than now.
  void set_large_token_expired(int64_t timestamp);

  // Get large token expired timestamp.
  int64_t get_large_token_expired() const;

  // check wait is disabled if f is true
  void set_disable_wait_flag(bool f);
  bool get_disable_wait_flag() const;

  common::ObDLinkNode<ObWorker*> worker_node_;
  common::ObDLinkNode<ObWorker*> lq_worker_node_;
  common::ObDLinkNode<ObWorker*> lq_waiting_worker_node_;

  virtual void resume()
  {}

  void set_sql_throttle_current_priority(int64_t st_current_priority)
  {
    st_current_priority_ = st_current_priority;
  }
  void reset_sql_throttle_current_priority()
  {
    set_sql_throttle_current_priority(100);
  }

public:
  // static variables
  static ObWorker& self();

protected:
  volatile RunStatus run_status_;
  int64_t st_current_priority_;
  sql::ObSQLSessionInfo* session_;

private:
  int64_t timeout_ts_;
  uint64_t rpc_tenant_id_;

  // worker index in its tenant
  int64_t tidx_;

  // timestamp when large token expires.
  int64_t large_token_expired_;

  // Used to prevent the thread holding the lock from being suspended by check_wait
  bool disable_wait_;

  DISALLOW_COPY_AND_ASSIGN(ObWorker);
};  // end of class ObWorker

inline ObWorker& ObWorker::self()
{
  return reinterpret_cast<ObWorker&>(Worker::self());
}

inline void ObWorker::set_tidx(int64_t tidx)
{
  tidx_ = tidx;
}

inline void ObWorker::unset_tidx()
{
  tidx_ = -1;
}

inline int64_t ObWorker::get_tidx() const
{
  return tidx_;
}

inline bool ObWorker::need_retry() const
{
  return false;
}

inline void ObWorker::disable_retry()
{}

inline bool ObWorker::set_retry_flag()
{
  return false;
}

inline void ObWorker::reset_retry_flag()
{}

inline void ObWorker::set_large_token_expired(int64_t timestamp)
{
  large_token_expired_ = timestamp;
}

inline int64_t ObWorker::get_large_token_expired() const
{
  return large_token_expired_;
}

inline void ObWorker::set_disable_wait_flag(bool f)
{
  disable_wait_ = f;
}

inline bool ObWorker::get_disable_wait_flag() const
{
  return disable_wait_;
}

#define THIS_WORKER oceanbase::share::ObWorker::self()

class DisableSchedInterGuard {
public:
  DisableSchedInterGuard()
  {
    last_flag_ = THIS_WORKER.get_disable_wait_flag();
    THIS_WORKER.set_disable_wait_flag(true);
  }
  ~DisableSchedInterGuard()
  {
    THIS_WORKER.set_disable_wait_flag(last_flag_);
  }

private:
  bool last_flag_;
};

class CompatModeGuard {
public:
  CompatModeGuard(ObWorker::CompatMode mode)
  {
    last_compat_mode_ = THIS_WORKER.get_compatibility_mode();
    THIS_WORKER.set_compatibility_mode(mode);
  }

  ~CompatModeGuard()
  {
    THIS_WORKER.set_compatibility_mode(last_compat_mode_);
  }

private:
  ObWorker::CompatMode last_compat_mode_;
};

using lib::is_mysql_mode;
using lib::is_oracle_mode;

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_OB_WORKER_H */
