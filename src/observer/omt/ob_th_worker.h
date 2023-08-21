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
static const int64_t NESTING_REQUEST_WAIT_TIME = 10 * 1000L;

// Quick Queue Priorities
enum { QQ_HIGH = 0, QQ_NORMAL, QQ_LOW, QQ_MAX_PRIO };
// Request queue priorities
enum { RQ_HIGH = QQ_MAX_PRIO, RQ_NORMAL, RQ_LOW, RQ_MAX_PRIO };

class ObThWorker
    : public lib::Worker, public lib::Threads
{
  friend class ObTenant;
public:
  explicit ObThWorker();
  virtual ~ObThWorker();

  virtual ObThWorker::Status check_wait() override;
  virtual int check_status() override;
  virtual int check_large_query_quota() override;
  // retry relating
  virtual bool can_retry() const override { return can_retry_; }
  // Note: you CAN NOT call set_need_retry when can_retry_ == false
  virtual void set_need_retry() override { need_retry_ = true; }
  // THIS is _only_ used (for easy impl) in query_retry_ctrl decide to retry
  // but following process want to invalid the decision.
  // refer `ObQueryRetryCtrl::on_close_resulet_fail_`
  virtual void unset_need_retry() override { need_retry_ = false; }
  virtual bool need_retry() const override { return need_retry_; }
  virtual void resume() override;

  int init();
  void destroy();
  inline void reset();

  OB_INLINE void set_tenant(ObTenant *tenant)
  {
    tenant_ = tenant;
    set_run_wrapper(MTL_CTX());
  }

  OB_INLINE void set_group(ObResourceGroup *group) { group_ = group; }

  void worker(int64_t &tenant_id, int64_t &req_recv_timestamp, int32_t &worker_level);
  void run(int64_t idx) override;

  OB_INLINE void pause() { pause_flag_ = true; }

  Status check_qtime_throttle();
  Status check_throttle();
  Status check_rate_limiter();

  OB_INLINE bool large_query() const { return large_query_; }
  OB_INLINE void set_large_query(bool v=true) { large_query_ = v; }

  OB_INLINE bool is_group_worker() const { return OB_NOT_NULL(group_); }
  OB_INLINE bool is_level_worker() const { return get_worker_level() > 0; }
  OB_INLINE void set_priority_limit(uint8_t limit) { priority_limit_ = limit; }
  OB_INLINE bool is_high_priority() const { return priority_limit_ == QQ_HIGH; }
  OB_INLINE bool is_normal_priority() const { return priority_limit_ == QQ_NORMAL; }
  OB_INLINE bool is_default_worker() const { return !is_group_worker() &&
                                                    !is_level_worker() &&
                                                    priority_limit_ > QQ_NORMAL; }

  OB_INLINE int64_t get_query_start_time() const { return query_start_time_; }
  OB_INLINE int64_t get_query_enqueue_time() const { return query_enqueue_time_; }
  OB_INLINE ObTenant* get_tenant() { return tenant_; }
  OB_INLINE ObResourceGroup* get_group() { return group_; }
  OB_INLINE bool is_lq_yield() const { return is_lq_yield_; }
  OB_INLINE void set_lq_yield(bool v=true) { is_lq_yield_ = v; }
  OB_INLINE int64_t get_last_wakeup_ts() { return last_wakeup_ts_; }
  OB_INLINE void set_last_wakeup_ts(int64_t last_wakeup_ts) { last_wakeup_ts_ = last_wakeup_ts; }
  OB_INLINE int64_t blocking_ts() const { return OB_NOT_NULL(blocking_ts_) ? (*blocking_ts_) : 0; }

private:
  void set_th_worker_thread_name();
  void update_ru_cputime();
  void process_request(rpc::ObRequest &req);

private:
  ObWorkerProcessor procor_;

  bool is_inited_;

  ObTenant *tenant_;
  ObResourceGroup *group_;
  common::ObThreadCond run_cond_;

  bool pause_flag_;
  bool large_query_;
  uint8_t priority_limit_;
  bool is_lq_yield_;

  int64_t query_start_time_;
  int64_t query_enqueue_time_;
  int64_t last_check_time_;

  // indicate whether upper scheduler support retry mechanism or not.
  bool can_retry_;
  // if upper scheduler support retry, need this request retry?
  bool need_retry_;

  bool has_add_to_cgroup_;

  int64_t last_wakeup_ts_;
  int64_t* blocking_ts_;
  int64_t idle_us_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObThWorker);
}; // end of class ObThWorker

inline void ObThWorker::reset()
{
  OB_ASSERT(!pause_flag_);
  tenant_ = nullptr;
  group_ = nullptr;
  pause_flag_ = false;
  large_query_ = false;
  priority_limit_ = RQ_LOW;
  query_start_time_ = 0;
  query_enqueue_time_ = 0;
  can_retry_ = true;
  need_retry_ = false;
  has_add_to_cgroup_ = false;
  last_wakeup_ts_ = 0;
}

/* create a worker
worker: save the new ObThWorker,
tidx: set worker's tidx_, an index of worker
tenant: set worker's tenant, which the worker belongs to
group_id: set worker's group_id
level: set worker's level, in ObResourceGroup level = INT32_MAX, in ObTenant level = 0,
group: set worker's group, in ObResourceGroup level = this, in ObTenant level = nullptr,
*/
int create_worker(ObThWorker* &worker, ObTenant *tenant, int32_t group_id,
                  int32_t level = INT32_MAX, bool force = false, ObResourceGroup *group = nullptr);
                    // defalut level=INT32_MAX, group=nullptr
int destroy_worker(ObThWorker *worker);

#define THIS_THWORKER static_cast<oceanbase::omt::ObThWorker &>(THIS_WORKER)
#define THIS_THWORKER_SAFE dynamic_cast<oceanbase::omt::ObThWorker *>(&THIS_WORKER)

} // end of namespace omt
} // end of namespace oceanbase


#endif /* _OCEABASE_OBSERVER_OMT_OB_TH_WORKER_H_ */
