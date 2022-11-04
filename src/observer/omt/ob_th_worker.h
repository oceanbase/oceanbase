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

#ifndef _OCEABASE_OBSERVER_OMT_OB_TH_WORKER_H_
#define _OCEABASE_OBSERVER_OMT_OB_TH_WORKER_H_

#include <pthread.h>
#include "lib/worker.h"
#include "lib/lock/ob_thread_cond.h"
#include "rpc/ob_request.h"
#include "lib/thread/threads.h"
#include "lib/thread/ob_thread_name.h"
#include "observer/omt/ob_worker_processor.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace rpc { namespace frame { class ObReqTranslator; } }
namespace omt
{

// Forward declarations
class ObTenant;
class ObResourceGroup;

static const int64_t WORKER_CHECK_PERIOD = 500L;
static const int64_t REQUEST_WAIT_TIME = 10 * 1000L;

class ObThWorker
    : public lib::Worker, public lib::Threads
{
public:
  enum RequestType { RT_NOTASK, RT_NEW, RT_OLD };

public:
  explicit ObThWorker();
  virtual ~ObThWorker();

  int init();
  void destroy();
  inline void reset();

  inline void set_tenant(ObTenant *tenant)
{
  tenant_ = tenant;
  set_run_wrapper(MTL_CTX());
}

  inline void set_group(ObResourceGroup *group);

  void worker(int64_t &tenant_id, int64_t &req_recv_timestamp, int32_t &worker_level);
  void run(int64_t idx);

  void resume();
  void pause();

  Status check_qtime_throttle();
  Status check_throttle();
  Status check_rate_limiter();
  virtual ObThWorker::Status check_wait();
  virtual int check_status() override;
  virtual int check_large_query_quota();

  // retry relating
  virtual bool can_retry() const;
  virtual void set_need_retry();
  virtual bool need_retry() const;

  // active relating
  void wait_active();
  void activate();
  void set_inactive();
  bool is_active() { return active_; }
  int64_t get_active_inactive_ts() const { return active_inactive_ts_; }
  bool is_waiting_active() { return ATOMIC_LOAD(&waiting_active_); }

  bool large_query() const { return large_query_; }
  void set_large_query(bool v=true) { large_query_ = v; }

  void set_lq_token(bool v=true) { lq_token_ = v; }
  bool has_lq_token() const { return lq_token_; }

  int64_t get_query_start_time() const;
  int64_t get_query_enqueue_time() const;
  ObTenant *get_tenant() { return tenant_; }
  ObResourceGroup *get_group() { return group_; }

private:
  // SQL layer should not call disable_retry directly
  // it can only decide retry, don't need to decide no-retry as it is by default.
  // ref: https://yuque.antfin-inc.com/xiaochu.yh/doc/sgl4x3#vhv1R
  virtual void disable_retry();

  void set_th_worker_thread_name(uint64_t tenant_id);
  void wait_runnable();
  void process_request(rpc::ObRequest &req);

  void th_created();
  void th_destroy();

private:
  ObWorkerProcessor procor_;

  bool is_inited_;

  ObTenant *tenant_;
  ObResourceGroup *group_;
  common::ObThreadCond run_cond_;

  bool pause_flag_;
  bool large_query_;

  int64_t query_start_time_;
  int64_t query_enqueue_time_;
  int64_t last_check_time_;

  // indicate whether upper scheduler support retry mechanism or not.
  bool can_retry_;
  // if upper scheduler support retry, need this request retry?
  bool need_retry_;


  // Set by other thread indicating the worker is going to be active
  // or not. When it is set as false, current worker would be paused
  // and no longer process request afterward.
  bool active_;
  // Flag for whether current worker is paused and waiting to be
  // active. Worker itself maintain this variable, set true before
  // wait and set false after. Others need check this variable and
  // waiting it to be true before resource relating to the worker.
  bool waiting_active_;
  int64_t active_inactive_ts_;
  bool lq_token_;
  bool has_add_to_cgroup_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObThWorker);
}; // end of class ObThWorker

inline void ObThWorker::reset()
{
  OB_ASSERT(!pause_flag_ && !active_);
  OB_ASSERT(!lq_token_);
  tenant_ = nullptr;
  group_ = nullptr;
  pause_flag_ = false;
  large_query_ = false;
  query_start_time_ = 0;
  query_enqueue_time_ = 0;
  can_retry_ = true;
  need_retry_ = false;
  active_ = false;
  has_add_to_cgroup_ = false;
  unset_tidx();
}

inline void ObThWorker::set_group(ObResourceGroup *group)
{
  group_ = group;
}

inline void ObThWorker::disable_retry()
{
  can_retry_ = false;
}

inline bool ObThWorker::can_retry() const
{
  return can_retry_;
}

inline bool ObThWorker::need_retry() const
{
  return need_retry_;
}

// Note: you CAN NOT call set_need_retry when can_retry_ == false
inline void ObThWorker::set_need_retry()
{
  need_retry_ = true;
}

inline void ObThWorker::pause()
{
  pause_flag_ = true;
}

inline void ObThWorker::set_inactive()
{
  active_inactive_ts_ = common::ObTimeUtility::current_time();
  active_ = false;
}

inline int64_t ObThWorker::get_query_start_time() const
{
  return query_start_time_;
}

inline int64_t ObThWorker::get_query_enqueue_time() const
{
  return query_enqueue_time_;
}

#define THIS_THWORKER static_cast<oceanbase::omt::ObThWorker &>(THIS_WORKER)
#define THIS_THWORKER_SAFE dynamic_cast<oceanbase::omt::ObThWorker *>(&THIS_WORKER)

} // end of namespace omt
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_TH_WORKER_H_ */
