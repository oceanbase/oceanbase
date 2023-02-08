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

#ifndef _OCEABASE_OBSERVER_OMT_OB_TENANT_H_
#define _OCEABASE_OBSERVER_OMT_OB_TENANT_H_

#include <stdint.h>
#include <cmath>
#include "lib/time/ob_time_utility.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_priority_queue.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_mutex.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/rc/ob_rc.h"
#include "rpc/ob_request.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "observer/omt/ob_th_worker.h"
#include "observer/omt/ob_worker_pool.h"
#include "observer/omt/ob_multi_level_queue.h"
#include "ob_retry_queue.h"
#include "lib/utility/ob_query_rate_limiter.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/omt/ob_tenant_meta.h"
#include "lib/lock/ob_tc_rwlock.h"      // TCRWLock

struct lua_State;
int select_dump_tenant_info(lua_State*);

namespace oceanbase
{
namespace observer
{
class ObAllVirtualDumpTenantInfo;
}
namespace omt
{

class ObPxPool
    : public share::ObThreadPool
{
  using RunFuncT = std::function<void ()>;
  void run(int64_t idx) final;
  void run1() final;
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;

public:
	class Task;
  ObPxPool() :
      tenant_id_(common::OB_INVALID_ID),
      group_id_(0),
      cgroup_ctrl_(nullptr),
      is_inited_(false),
      concurrency_(0),
      active_threads_(0)
  {}
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_group_id(uint64_t group_id) { group_id_ = group_id; }
  void set_cgroup_ctrl(share::ObCgroupCtrl *cgroup_ctrl) { cgroup_ctrl_ = cgroup_ctrl; }
  int64_t get_pool_size() const { return get_thread_count(); }
  int submit(const RunFuncT &func);
  void set_px_thread_name();
private:
  void handle(common::ObLink *task);
  void try_recycle(int64_t idle_time);
  void disable_recycle()
  {
    recycle_lock_.lock();
  }
  void enable_recycle()
  {
    IGNORE_RETURN recycle_lock_.unlock();
  }
private:
  uint64_t tenant_id_;
  uint64_t group_id_;
  share::ObCgroupCtrl *cgroup_ctrl_;
	common::ObPriorityQueue2<0, 1> queue_;
  bool is_inited_;
  int64_t concurrency_;
  int64_t active_threads_;
  mutable common::ObSpinLock recycle_lock_;
};

class ObPxPool::Task : public common::ObLink
{
public:
  Task(const RunFuncT &func)
      : func_(func)
  {}
public:
  RunFuncT func_;
};


class ObPxPools
{
public:
  class DeletePoolFunc
  {
  public:
    DeletePoolFunc() {}
    virtual ~DeletePoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };

  class ThreadRecyclePoolFunc
  {
  public:
    ThreadRecyclePoolFunc() {}
    virtual ~ThreadRecyclePoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };
public:
  static int mtl_init(ObPxPools *&pools)
  {
    int ret = common::OB_SUCCESS;
    uint64_t tenant_id = MTL_ID();
    pools = OB_NEW(ObPxPools, common::ObModIds::OMT_TENANT);
    if (OB_ISNULL(pools)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(pools->init(tenant_id))) {
    }
    return ret;
  }
  static void mtl_destroy(ObPxPools *&pools)
  {
    common::ob_delete(pools);
    pools = nullptr;
  }
public:
  ObPxPools() : tenant_id_(common::OB_INVALID_ID)
  {}
  ~ObPxPools()
  {
    destroy();
  }
  int init(uint64_t tenant_id);
  int get_or_create(int64_t group_id, ObPxPool *&pool);
  int thread_recycle();
private:
  void destroy();
  int create_pool(int64_t group_id, ObPxPool *&pool);
private:
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  common::hash::ObHashMap<int64_t, ObPxPool *> pool_map_;
};


const int64_t MAX_REQUEST_LEVEL = MULTI_LEVEL_QUEUE_SIZE;

struct ObSqlThrottleMetrics
{
  int64_t priority_;
  double rt_;
  double cpu_;
  int64_t io_;
  double network_;
  int64_t logical_reads_;
  double queue_time_;

  ObSqlThrottleMetrics()
      : priority_(-1),
        rt_(-1),
        cpu_(-1),
        io_(-1),
        network_(-1),
        logical_reads_(-1),
        queue_time_(-1)
  {}

  TO_STRING_KV(
    K_(priority),
    K_(rt),
    K_(cpu),
    K_(io),
    K_(network),
    K_(logical_reads),
    K_(queue_time));
};

// Forward declarations
class ObThWorker;
using lib::Worker;

// Type aliases
typedef common::ObDLinkNode<ObThWorker*> WorkerNode;
typedef common::ObDList<WorkerNode> WorkerList;

class MultiLevelReqCnt {
public:
  MultiLevelReqCnt()
  {
    for (int i = 0; i < MAX_REQUEST_LEVEL; i++) {
      cnt_[i] = 0;
    }
  }
  ~MultiLevelReqCnt() {}
  void atomic_inc(const int32_t level);
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    for(int i = 0; i < MAX_REQUEST_LEVEL; i++) {
      common::databuff_printf(buf, buf_len, pos, "cnt[%d]=%ld ", i, cnt_[i]);
    }
    return pos;
  }
private:
  volatile uint64_t cnt_[MAX_REQUEST_LEVEL];
};

class ObResourceGroupNode : public common::SpHashNode
{
public:
  ObResourceGroupNode(int32_t group_id):
    common::SpHashNode(calc_hash(group_id)),
    group_id_(group_id)
  {}
  ~ObResourceGroupNode() {}
  int64_t calc_hash(int32_t group_id)
  {
    return (common::murmurhash(&group_id, sizeof(group_id), 0)) | 1;
  }
  int compare(ObResourceGroupNode* that) {
    int ret = 0;
    if (this->hash_ > that->hash_) {
      ret = 1;
    } else if (this->hash_ < that->hash_) {
      ret = -1;
    } else if (this->is_dummy()) {
      ret = 0;
    } else if (this->group_id_ > that->group_id_) {
      ret = 1;
    } else if (this->group_id_ < that->group_id_) {
      ret = -1;
    } else {
      ret = 0;
    }
    return ret;
  }
  int32_t get_group_id() const { return group_id_; }
  void set_group_id(const int32_t &group_id) { group_id_ = group_id; }
protected:
  int32_t group_id_;
};

class ObResourceGroup : public ObResourceGroupNode // group container, storing thread pool and queue, each group_id corresponds to one{
{
  friend class ObTenant;
  friend class GroupMap;
public:
  using WListNode = common::ObDLinkNode<lib::Worker*>;
  using WList = common::ObDList<WListNode>;
  enum { CALIBRATE_TOKEN_INTERVAL = 100 * 1000 };
  static constexpr int64_t PRESERVE_INACTIVE_WORKER_TIME = 10 * 1000L * 1000L;

  ObResourceGroup(int32_t group_id, ObTenant *tenant, ObWorkerPool *worker_pool, share::ObCgroupCtrl *cgroup_ctrl):
    ObResourceGroupNode(group_id),
    workers_lock_(common::ObLatchIds::TENANT_WORKER_LOCK),
    inited_(false),
    recv_req_cnt_(0),
    pop_req_cnt_(0),
    token_cnt_(0),
    ass_token_cnt_(0),
    min_token_cnt_(0),
    max_token_cnt_(0),
    last_calibrate_token_ts_(0),
    tenant_(tenant),
    worker_pool_(worker_pool),
    cgroup_ctrl_(cgroup_ctrl),
    has_stop_(false)
  {
  }
  ~ObResourceGroup() {}

  bool is_inited() const { return inited_; }
  void atomic_inc_recv_cnt() { ATOMIC_INC(&recv_req_cnt_); }
  uint64_t get_recv_req_cnt() const { return recv_req_cnt_; }
  void atomic_inc_pop_cnt() { ATOMIC_INC(&pop_req_cnt_); }
  uint64_t get_pop_req_cnt() const { return pop_req_cnt_; }
  int64_t get_token_cnt() const { return token_cnt_; }
  void set_token_cnt(const int64_t token_cnt) { token_cnt_ = token_cnt; };
  int64_t get_ass_token_cnt() const { return ass_token_cnt_; }
  void set_ass_token_cnt(const int64_t ass_token_cnt) { ass_token_cnt_ = ass_token_cnt; }
  int64_t get_min_token_cnt() const { return min_token_cnt_; }
  void set_min_token_cnt(const int64_t min_token_cnt) { min_token_cnt_ = min_token_cnt; }
  int64_t get_max_token_cnt() const { return max_token_cnt_; }
  void set_max_token_cnt(const int64_t max_token_cnt) { max_token_cnt_ = max_token_cnt; }

  ObTenant *get_tenant() { return tenant_; }
  ObWorkerPool *get_worker_pool() { return worker_pool_; }
  share::ObCgroupCtrl *get_cgroup_ctrl() { return cgroup_ctrl_; }

  int init();
  int acquire_more_worker(int64_t num, int64_t &succ_num);
  void calibrate_token_count();
  void check_worker_count();
  void check_worker_count(ObThWorker &w);
  int clear_worker();

  lib::ObMutex workers_lock_;

protected:
  WList workers_;
  common::ObPriorityQueue2<0, 1> req_queue_;

private:
  bool inited_;                              // Mark whether the container has threads and queues allocated

  volatile uint64_t recv_req_cnt_;           // Statistics requested to enqueue
  volatile uint64_t pop_req_cnt_;            // Statistics of Dequeue Requests
  volatile uint64_t last_pop_req_cnt_;       // Statistics of the request to leave the team the last time the token was dynamically adjusted

  int64_t token_cnt_;                        // The current number of target threads
  int64_t ass_token_cnt_;                    // The number of threads actually allocated
  int64_t min_token_cnt_;                    // Initial target number of threads
  int64_t max_token_cnt_;                    // Max target number of threads
  int64_t last_calibrate_token_ts_;          // The timestamp of the last time the token was dynamically adjusted

  ObTenant *tenant_;
  ObWorkerPool *worker_pool_;
  share::ObCgroupCtrl *cgroup_ctrl_;
  bool has_stop_;
};

typedef common::FixedHash2<ObResourceGroupNode> GroupHash;
class GroupMap : public GroupHash // Store all group containers of the current tenant
{
public:
  GroupMap(void* buf, int64_t size):
    GroupHash(buf, size)
  {
  }
  ~GroupMap() {}
	int create_and_insert_group(int32_t group_id, ObTenant *tenant, ObWorkerPool *worker_pool, share::ObCgroupCtrl *cgroup_ctrl, ObResourceGroup *&group);
  void wait_group();
  void destroy_group();
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
	  ObResourceGroupNode* iter = NULL;
    ObResourceGroup* group = nullptr;
    int64_t pos = 0;
    while (NULL != (iter = const_cast<GroupMap*>(this)->GroupHash::quick_next(iter))) {
      group = static_cast<ObResourceGroup*>(iter);
      common::databuff_printf(buf, buf_len, pos,
       "group_id = %d,"
       "queue_size = %ld,"
       "recv_req_cnt = %lu,"
       "pop_req_cnt = %lu,"
       "token_cnt = %ld,"
       "min_token_cnt = %ld,"
       "max_token_cnt = %ld,"
       "ass_token_cnt = %ld ",
       group->group_id_,
       group->req_queue_.size(),
       group->recv_req_cnt_,
       group->pop_req_cnt_,
       group->token_cnt_,
       group->min_token_cnt_,
       group->max_token_cnt_,
       group->ass_token_cnt_);
    }
    return pos;
  }
};

class RpcStatInfo
{
public:
  RpcStatInfo(int64_t tenant_id):
    tenant_id_(tenant_id)
  {}
  ~RpcStatInfo() {}
  int64_t to_string(char *buf, const int64_t len) const;
  mutable rpc::RpcStatService rpc_stat_srv_;
  int64_t tenant_id_;
};


//================================= ObTenant ====================================//
// Except for get_new_request wakeup_paused_worker recv_request, all
// other functions aren't thread safe.
class ObTenant : public share::ObTenantBase
{
  friend class observer::ObAllVirtualDumpTenantInfo;
  friend int ::select_dump_tenant_info(lua_State*);
  using WListNode = common::ObDLinkNode<lib::Worker*>;
  using WList = common::ObDList<WListNode>;

  // How long to preserve inactive worker before free it to worker
  // pool.
  static constexpr int64_t PRESERVE_INACTIVE_WORKER_TIME = 10 * 1000L * 1000L;
  enum { CALIBRATE_WORKER_INTERVAL = 30 * 1000 * 1000 };
  enum { CALIBRATE_TOKEN_INTERVAL = 100 * 1000 };

public:
  // Quick Queue Priorities
  enum { QQ_HIGH = 0, QQ_PRIOR_TO_NORMAL,  QQ_NORMAL, QQ_MAX_PRIO };
  // Request queue priorities
  enum { RQ_HIGH = QQ_MAX_PRIO, RQ_NORMAL, RQ_LOW, RQ_MAX_PRIO };

  enum { MAX_RESOURCE_GROUP = 8 };

  ObTenant(const int64_t id,
           const int64_t times_of_workers,
           share::ObCgroupCtrl &cgroup_ctrl);
  virtual ~ObTenant();

  ObTenant(const ObTenant &) = delete;
  ObTenant &operator=(const ObTenant &) = delete;


  int init_ctx();
  int init(const ObTenantMeta &meta);
  void set_stop(const bool is_stop);
  void stop();
  void wait();
  void destroy();
  bool has_stopped() const;

  ObTenantMeta get_tenant_meta();
  bool is_hidden();
  ObTenantCreateStatus get_create_status();
  void set_create_status(const ObTenantCreateStatus status);

  int create_tenant_module();

  share::ObUnitInfoGetter::ObTenantConfig get_unit();
  uint64_t get_unit_id();
  storage::ObTenantSuperBlock get_super_block();
  void set_tenant_meta(const ObTenantMeta &meta);
  void set_tenant_unit(const share::ObUnitInfoGetter::ObTenantConfig &unit);
  void set_tenant_super_block(const storage::ObTenantSuperBlock &super_block);
  void mark_tenant_is_removed();
  void set_unit_status(const share::ObUnitInfoGetter::ObUnitStatus status);
  share::ObUnitInfoGetter::ObUnitStatus get_unit_status();

  void set_unit_max_cpu(double cpu);
  double unit_max_cpu() const;
  void set_unit_min_cpu(double cpu);
  double unit_min_cpu() const;
  void set_token(const int64_t token);
  void set_sug_token(const int64_t token);
  int64_t token_cnt() const;
  int64_t sug_token_cnt() const;
  lib::Worker::CompatMode get_compat_mode() const;
  share::ObTenantSpace &ctx();

  void add_idle_time(int64_t idle_time);
  void add_worker_time(int64_t req_time);

  int rdlock(common::ObLDHandle &handle);
  int wrlock(common::ObLDHandle &handle);
  int try_rdlock(common::ObLDHandle &handle);
  int try_wrlock(common::ObLDHandle &handle);
  virtual int unlock(common::ObLDHandle &handle) override;

  bool has_task() const;
  virtual int64_t waiting_count() const;
  int64_t get_request_queue_length() const;

  // get request from request queue, waiting at most TIMEOUT us.
  // if IN_HIGH_PRIORITY is set, get request from hp queue.
  int get_new_request(
      ObThWorker &w,
      int64_t timeout,
      rpc::ObRequest *&req);

  // receive request from network
  int recv_request(rpc::ObRequest &req);
  int recv_large_request(rpc::ObRequest &req);
  int push_retry_queue(rpc::ObRequest &req, const uint64_t idx);
  void handle_retry_req();

  void calibrate_token_count();
  void calibrate_group_token_count();
  void calibrate_worker_count();
  int timeup();

  TO_STRING_KV(K_(id),
               K_(tenant_meta),
               K_(unit_min_cpu), K_(unit_max_cpu), K_(slice),
               K_(slice_remain), K_(token_cnt), K_(sug_token_cnt),
               K_(ass_token_cnt),
               K_(lq_tokens),
               K_(used_lq_tokens),
               K_(stopped), K_(idle_us),
               K_(recv_hp_rpc_cnt), K_(recv_np_rpc_cnt),
               K_(recv_lp_rpc_cnt), K_(recv_mysql_cnt),
               K_(recv_task_cnt),
               K_(recv_large_req_cnt),
               K_(tt_large_quries),
               K_(pop_normal_cnt),
               K_(actives),
               "workers", workers_.get_size(),
               "nesting workers", nesting_workers_.get_size(),
               "lq waiting workers", lq_waiting_workers_.get_size(),
               K_(req_queue),
               "large queued", large_req_queue_.size(),
               K_(multi_level_queue),
               K_(recv_level_rpc_cnt),
               K_(group_map),
               K_(rpc_stat_info))
public:
  static bool equal(const ObTenant *t1, const ObTenant *t2)
  {
    return (!OB_ISNULL(t1) && !OB_ISNULL(t2) && t1->id_ == t2->id_);
  }

  // update CPU usage
  void update_token_usage();

  // acquire workers if tenant doesn't have sufficient worker.
  int check_worker_count();
  int check_worker_count(ObThWorker &w);
  int check_group_worker_count();

  // Check if there's paused worker need be woken up.
  //
  // If paused worker has been woken up, current worker would be set
  // to inactive and unlink from its queues.
  void check_paused_worker(ObThWorker &w);

  int link_worker(lib::Worker &w);
  void unlink_worker(lib::Worker &w);
  int link_lq_waiting_worker(lib::Worker &w);
  void unlink_lq_waiting_worker(lib::Worker &w);
  void try_unlink_lq_waiting_worker_with_lock(lib::Worker &w);

  // called each checkpoint for worker of this tenant.
  int lq_check_status(ObThWorker &w);

  void disable_user_sched();
  bool user_sched_enabled() const;
  double get_token_usage() const;
  int64_t get_worker_time() const;
  // sql throttle
  void update_sql_throttle_metrics(const ObSqlThrottleMetrics &metrics)
  { st_metrics_ = metrics; }
  const ObSqlThrottleMetrics &get_sql_throttle_metrics() const
  { return st_metrics_; }

  void update_sql_throughput(const int64_t throughput)
  {
    if (throughput < 0) {
      sql_limiter_.set_rate(-1);
    } else {
      sql_limiter_.set_rate(throughput);
    }
  }
  lib::ObRateLimiter &get_sql_rate_limiter()
  { return sql_limiter_; }

  // Node balance thread would periodically check tenant status by
  // calling this function.
  void periodically_check();

private:
  // alloc NUM worker
  int acquire_level_worker(int64_t num, int64_t &succ_num, int32_t level);
  int acquire_more_worker(int64_t num, int64_t &succ_num);
  bool acquire_lq_token();
  void release_lq_token();

  int64_t worker_count_bound() const;

  inline void pause_it(ObThWorker &w);
  inline void resume_it(ObThWorker &w);

  int pop_req(common::ObLink *&req, int64_t timeout);

  // read tenant variable PARALLEL_SERVERS_TARGET
  void check_parallel_servers_target();
  void check_px_thread_recycle();

  // The update of the resource manager is applied to the cgroup
  void check_resource_manager_plan();
  // clean buffer on time
  void check_dtl();
  void check_das();

  int construct_mtl_init_ctx(const ObTenantMeta &meta, share::ObTenantModuleInitCtx *&ctx);

protected:

  mutable common::TCRWLock meta_lock_;
  ObTenantMeta tenant_meta_;

protected:
  // times of workers of cpu slice this tenant can alloc.
  const int64_t times_of_workers_;
  // max/min cpu read from unit
  double unit_max_cpu_;
  double unit_min_cpu_;

  // tenant slice, it is calculated by quota. The slice is the average
  // number of token a tenant can get in every 10ms.
  double slice_;
  // tenant slice remains, will reset every 10s.
  double slice_remain_;
  // locker for update slice remain
  common::ObSpinLock slice_remain_lock_;
  // if set, reset slice remain before add.
  bool slice_remain_clear_flag_;

  // number of active workers the tenant has owned. Only active
  // workers can make progress.
  int64_t sug_token_cnt_;
  int64_t token_cnt_;
  int64_t ass_token_cnt_;
  int64_t lq_tokens_;
  int64_t used_lq_tokens_;
  int64_t last_calibrate_worker_ts_;
  int64_t last_calibrate_token_ts_;
  int64_t last_pop_normal_cnt_;
  int nesting_worker_has_init_;

  bool stopped_;
  bool wait_mtl_finished_;

  /// tenant task queue,
  // 'hp' for high priority and 'np' for normal priority
  common::ObPriorityQueue2<1, QQ_MAX_PRIO - 1, RQ_MAX_PRIO - QQ_MAX_PRIO> req_queue_;
  common::ObLinkQueue large_req_queue_;

  //Create a request queue for each level of nested requests
  ObMultiLevelQueue *multi_level_queue_;
  MultiLevelReqCnt recv_level_rpc_cnt_;

  //Create a timer queue group for retry requests
  ObRetryQueue retry_queue_;

  volatile uint64_t recv_hp_rpc_cnt_;
  volatile uint64_t recv_np_rpc_cnt_;
  volatile uint64_t recv_lp_rpc_cnt_;
  volatile uint64_t recv_mysql_cnt_;
  volatile uint64_t recv_task_cnt_;
  volatile uint64_t recv_sql_task_cnt_;
  volatile uint64_t recv_large_req_cnt_;
  volatile uint64_t pause_cnt_;
  volatile uint64_t resume_cnt_;
  volatile uint64_t recv_retry_on_lock_rpc_cnt_;
  volatile uint64_t recv_retry_on_lock_mysql_cnt_;
  volatile uint64_t actives_;
  volatile uint64_t tt_large_quries_;
  volatile uint64_t pop_normal_cnt_;

  // free worker pool
  ObWorkerPool worker_pool_;

private:
  GroupMap group_map_;
  // for group_map hash node
  char group_map_buf_[sizeof(common::SpHashNode) * MAX_RESOURCE_GROUP];

public:
  common::ObLDLatch lock_;

  // Variables for V2
  WList workers_;
  WList lq_waiting_workers_;
  WList nesting_workers_;
  RpcStatInfo *rpc_stat_info_;
  share::ObTenantModuleInitCtx *mtl_init_ctx_;

  lib::ObMutex workers_lock_;
  lib::ObMutex lq_waiting_workers_lock_;

  share::ObCgroupCtrl &cgroup_ctrl_;

  bool disable_user_sched_;

  double token_usage_;
  int64_t token_usage_check_ts_;
  bool dynamic_modify_token_;
  bool dynamic_modify_group_token_;

  share::ObTenantSpace *ctx_;

  bool px_pool_is_running_;
  ObSqlThrottleMetrics st_metrics_;
  lib::ObQueryRateLimiter sql_limiter_;
  // idle time between two checkpoints
  int64_t worker_us_ CACHE_ALIGNED;
  int64_t idle_us_ CACHE_ALIGNED;

}; // end of class ObTenant

inline bool ObTenant::has_stopped() const
{
  return ATOMIC_LOAD(&stopped_);
}

inline double ObTenant::unit_max_cpu() const
{
  return unit_max_cpu_;
}

inline double ObTenant::unit_min_cpu() const
{
  return unit_min_cpu_;
}


inline share::ObTenantSpace &ObTenant::ctx()
{
  return *ctx_;
}

inline int64_t ObTenant::token_cnt() const
{
  return token_cnt_;
}

inline int64_t ObTenant::sug_token_cnt() const
{
  return sug_token_cnt_;
}

inline void ObTenant::add_idle_time(int64_t idle_time)
{
  (void)ATOMIC_FAA(reinterpret_cast<uint64_t *>(&idle_us_), idle_time);
}

inline void ObTenant::add_worker_time(int64_t req_time)
{
  (void)ATOMIC_FAA(reinterpret_cast<uint64_t *>(&worker_us_), req_time);
}

inline void ObTenant::pause_it(ObThWorker &w)
{
  pause_cnt_++;
  w.pause();
}

inline void ObTenant::resume_it(ObThWorker &w)
{
  resume_cnt_++;
  w.resume();
}

OB_INLINE int ObTenant::pop_req(common::ObLink *&req, int64_t timeout)
{
  return req_queue_.pop(req, timeout);
}

inline int ObTenant::rdlock(common::ObLDHandle &handle)
{
  return lock_.rdlock(handle, common::ObLatchIds::TENANT_LOCK) == common::OB_SUCCESS
      ? common::OB_SUCCESS
      : common::OB_EAGAIN;
}

inline int ObTenant::wrlock(common::ObLDHandle &handle)
{
  uint32_t puid = static_cast<uint32_t>(GETTID());
  return lock_.wrlock(handle, common::ObLatchIds::TENANT_LOCK, INT64_MAX, &puid) == common::OB_SUCCESS
      ? common::OB_SUCCESS
      : common::OB_EAGAIN;
}

inline int ObTenant::try_rdlock(common::ObLDHandle &handle)
{
  return lock_.try_rdlock(handle, common::ObLatchIds::TENANT_LOCK) == common::OB_SUCCESS
      ? common::OB_SUCCESS
      : common::OB_EAGAIN;
}

inline int ObTenant::try_wrlock(common::ObLDHandle &handle)
{
  uint32_t puid = static_cast<uint32_t>(GETTID());
  return lock_.try_wrlock(handle, common::ObLatchIds::TENANT_LOCK, &puid) == common::OB_SUCCESS
      ? common::OB_SUCCESS
      : common::OB_EAGAIN;
}

inline int ObTenant::unlock(common::ObLDHandle &handle)
{
  uint32_t puid = static_cast<uint32_t>(GETTID());
  return lock_.unlock(handle, &puid) == common::OB_SUCCESS
      ? common::OB_SUCCESS
      : common::OB_EAGAIN;
}

inline void ObTenant::disable_user_sched()
{
  disable_user_sched_ = true;
}

inline bool ObTenant::user_sched_enabled() const
{
  return !disable_user_sched_;
}

inline double ObTenant::get_token_usage() const
{
  return token_usage_;
}

inline int64_t ObTenant::get_worker_time() const
{
  return ATOMIC_LOAD(&worker_us_);
}

inline bool ObTenant::acquire_lq_token()
{
  bool succ = true;
  const auto v = ATOMIC_FAA(&used_lq_tokens_, 1);
  if (v >= lq_tokens_) {
    ATOMIC_DEC(&used_lq_tokens_);
    succ = false;
  }
  return succ;
}

inline void ObTenant::release_lq_token()
{
  ATOMIC_DEC(&used_lq_tokens_);
}

} // end of namespace omt
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_TENANT_H_ */
