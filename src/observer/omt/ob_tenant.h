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
  using RunFuncT = std::function<void (bool)>;
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
  virtual void stop();
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_group_id(uint64_t group_id) { group_id_ = group_id; }
  void set_cgroup_ctrl(share::ObCgroupCtrl *cgroup_ctrl) { cgroup_ctrl_ = cgroup_ctrl; }
  int64_t get_pool_size() const { return get_thread_count(); }
  int submit(const RunFuncT &func);
  void set_px_thread_name();
  int64_t get_queue_size() const { return queue_.size(); }
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
  class StopPoolFunc
  {
  public:
    StopPoolFunc() {}
    virtual ~StopPoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };
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
    if (OB_FAIL(pools->init(tenant_id))) {
    }
    return ret;
  }
  static void mtl_stop(ObPxPools *&pools);
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
  static constexpr int64_t PRESERVE_INACTIVE_WORKER_TIME = 10 * 1000L * 1000L;

  ObResourceGroup(int32_t group_id, ObTenant* tenant, share::ObCgroupCtrl *cgroup_ctrl);
  ~ObResourceGroup() {}

  bool is_inited() const { return inited_; }
  void atomic_inc_recv_cnt() { ATOMIC_INC(&recv_req_cnt_); }
  uint64_t get_recv_req_cnt() const { return recv_req_cnt_; }
  int64_t min_worker_cnt() const;
  int64_t max_worker_cnt() const;
  ObTenant *get_tenant() { return tenant_; }
  share::ObCgroupCtrl *get_cgroup_ctrl() { return cgroup_ctrl_; }

  int init();
  void update_queue_size();
  int acquire_more_worker(int64_t num, int64_t &succ_num, bool force = false);
  int acquire_level_worker(int32_t level);
  void check_worker_count();
  void check_worker_count(ObThWorker &w);
  int clear_worker();
  int get_throttled_time(int64_t &throttled_time);
  TO_STRING_KV("group_id", group_id_,
               "queue_size", req_queue_.size(),
               "recv_req_cnt", recv_req_cnt_,
               "min_worker_cnt", min_worker_cnt(),
               "max_worker_cnt", max_worker_cnt(),
               K(multi_level_queue_),
               "recv_level_rpc_cnt", recv_level_rpc_cnt_,
               "worker_cnt", workers_.get_size(),
               "nesting_worker_cnt", nesting_workers_.get_size(),
               "token_change", token_change_ts_);

private:
  lib::ObMutex& workers_lock_;
  WList workers_;
  WList nesting_workers_;
  common::ObPriorityQueue2<0, 1> req_queue_;
  ObMultiLevelQueue multi_level_queue_;
  bool inited_;                                  // Mark whether the container has threads and queues allocated
  volatile uint64_t recv_req_cnt_ CACHE_ALIGNED; // Statistics requested to enqueue
  volatile bool shrink_ CACHE_ALIGNED;
  int64_t token_change_ts_;
  MultiLevelReqCnt recv_level_rpc_cnt_;
  int nesting_worker_cnt_;
  ObTenant *tenant_;
  share::ObCgroupCtrl *cgroup_ctrl_;
  int64_t throttled_time_us_;
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
	int create_and_insert_group(int32_t group_id, ObTenant *tenant, share::ObCgroupCtrl *cgroup_ctrl, ObResourceGroup *&group);
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
       "%s",
       to_cstring(group));
    }
    return pos;
  }
  static int err_code_map(int err);
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
  friend class ObResourceGroup;
  friend int ::select_dump_tenant_info(lua_State*);
  friend int create_worker(ObThWorker* &worker, ObTenant *tenant, int32_t group_id,
                           int32_t level, bool force, ObResourceGroup *group);
  friend int destroy_worker(ObThWorker *worker);
  using WListNode = common::ObDLinkNode<lib::Worker*>;
  using WList = common::ObDList<WListNode>;

  // How long to preserve inactive worker before free it to worker
  // pool.
  static constexpr int64_t PRESERVE_INACTIVE_WORKER_TIME = 10 * 1000L * 1000L;

public:
  enum { MAX_RESOURCE_GROUP = 8 };

  ObTenant(const int64_t id,
           const int64_t times_of_workers,
           share::ObCgroupCtrl &cgroup_ctrl);
  virtual ~ObTenant();

  ObTenant(const ObTenant &) = delete;
  ObTenant &operator=(const ObTenant &) = delete;


  int init_ctx();
  int init_multi_level_queue();
  int init(const ObTenantMeta &meta);
  void stop() { ATOMIC_STORE(&stopped_, ObTimeUtility::current_time()); }
  void start() { ATOMIC_STORE(&stopped_, 0); }
  int try_wait();
  void destroy();
  bool has_stopped() const { return ATOMIC_LOAD(&stopped_) != 0; }

  ObTenantMeta get_tenant_meta();
  bool is_hidden();
  ObTenantCreateStatus get_create_status();
  void set_create_status(const ObTenantCreateStatus status);

  int create_tenant_module();

  share::ObUnitInfoGetter::ObTenantConfig get_unit();
  uint64_t get_unit_id();
  storage::ObTenantSuperBlock get_super_block();
  void set_tenant_unit(const share::ObUnitInfoGetter::ObTenantConfig &unit);
  void set_tenant_super_block(const storage::ObTenantSuperBlock &super_block);
  void mark_tenant_is_removed();
  void set_unit_status(const share::ObUnitInfoGetter::ObUnitStatus status);
  share::ObUnitInfoGetter::ObUnitStatus get_unit_status();

  void set_unit_max_cpu(double cpu);
  void set_unit_min_cpu(double cpu);
  OB_INLINE int64_t total_worker_cnt() const { return total_worker_cnt_; }
  int64_t cpu_quota_concurrency() const;
  int64_t min_worker_cnt() const;
  int64_t max_worker_cnt() const;
  lib::Worker::CompatMode get_compat_mode() const;
  OB_INLINE share::ObTenantSpace &ctx() { return *ctx_; }
  int rdlock(common::ObLDHandle &handle);
  int wrlock(common::ObLDHandle &handle);
  int try_rdlock(common::ObLDHandle &handle);
  int try_wrlock(common::ObLDHandle &handle);
  virtual int unlock(common::ObLDHandle &handle) override;

  // get request from request queue, waiting at most TIMEOUT us.
  // if IN_HIGH_PRIORITY is set, get request from hp queue.
  int get_new_request(ObThWorker &w, int64_t timeout, rpc::ObRequest *&req);

  // receive request from network
  int recv_request(rpc::ObRequest &req);
  int recv_large_request(rpc::ObRequest &req);
  int push_retry_queue(rpc::ObRequest &req, const uint64_t idx);
  void handle_retry_req(bool need_clear = false);
  void check_worker_count(ObThWorker &w);
  void update_queue_size();

  int timeup();
  void print_throttled_time();

  TO_STRING_KV(K_(id),
               K_(tenant_meta),
               K_(unit_min_cpu), K_(unit_max_cpu), K_(total_worker_cnt),
               "min_worker_cnt", min_worker_cnt(),
               "max_worker_cnt", max_worker_cnt(),
               K_(stopped),
               "worker_us", get_worker_time(),
               K_(recv_hp_rpc_cnt), K_(recv_np_rpc_cnt),
               K_(recv_lp_rpc_cnt), K_(recv_mysql_cnt),
               K_(recv_task_cnt),
               K_(recv_large_req_cnt),
               K_(tt_large_quries),
               K_(pop_normal_cnt),
               "workers", workers_.get_size(),
               "nesting workers", nesting_workers_.get_size(),
               K_(req_queue),
               K_(multi_level_queue),
               K_(recv_level_rpc_cnt),
               K_(group_map),
               K_(rpc_stat_info),
               K_(token_change_ts),
               "tenant_role", get_tenant_role())
public:
  static bool equal(const ObTenant *t1, const ObTenant *t2)
  {
    return (!OB_ISNULL(t1) && !OB_ISNULL(t2) && t1->id_ == t2->id_);
  }

  void lq_end(ObThWorker &w);
  // called each checkpoint for worker of this tenant.
  void lq_wait(ObThWorker &w);
  int lq_yield(ObThWorker &w);

  OB_INLINE void disable_user_sched() { disable_user_sched_ = true; }
  OB_INLINE bool user_sched_enabled() const { return !disable_user_sched_; }
  OB_INLINE double get_token_usage() const { return token_usage_; }
  OB_INLINE int64_t get_worker_time() const { return ATOMIC_LOAD(&worker_us_); }
  OB_INLINE int64_t get_cpu_time() const { return ATOMIC_LOAD(&cpu_time_us_); }
  int64_t get_rusage_time();
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
  int64_t lq_retry_queue_size()
  {
    return 0;
  }
  // OB_INLINE bool has_normal_request() const { return req_queue_.size() != 0; }
  // OB_INLINE bool has_level_request() const { return OB_NOT_NULL(multi_level_queue_) && multi_level_queue_->get_total_size() != 0; }
private:
  static void sleep_and_warn(ObTenant* tenant);
  static void* wait(void* tenant);
  // update CPU usage
  void update_token_usage();
  // acquire workers if tenant doesn't have sufficient worker.
  void check_worker_count();
  void check_group_worker_count();
  // alloc NUM worker
  int acquire_level_worker(int64_t num, int64_t &succ_num, int32_t level);
  int acquire_more_worker(int64_t num, int64_t &succ_num, bool force = false);

  int64_t worker_count() const { return workers_.get_size(); }

  inline void pause_it(ObThWorker &w);
  inline void resume_it(ObThWorker &w);

  OB_INLINE int pop_req(common::ObLink *&req, int64_t timeout) { return req_queue_.pop(req, timeout); }

  // read tenant variable PARALLEL_SERVERS_TARGET
  void check_parallel_servers_target();
  void check_px_thread_recycle();

  // The update of the resource manager is applied to the cgroup
  void check_resource_manager_plan();
  // clean buffer on time
  void check_dtl();
  void check_das();

  int construct_mtl_init_ctx(const ObTenantMeta &meta, share::ObTenantModuleInitCtx *&ctx);

  int recv_group_request(rpc::ObRequest &req, int64_t group_id);
protected:

  mutable common::TCRWLock meta_lock_;
  ObTenantMeta tenant_meta_;

protected:
  // number of active workers the tenant has owned. Only active
  // workers can make progress.
  volatile bool shrink_ CACHE_ALIGNED;
  int64_t total_worker_cnt_;
  void *gc_thread_;
  bool has_created_;
  int64_t stopped_;
  bool wait_mtl_finished_;

  /// tenant task queue,
  // 'hp' for high priority and 'np' for normal priority
  common::ObPriorityQueue2<1, QQ_MAX_PRIO - 1, RQ_MAX_PRIO - QQ_MAX_PRIO> req_queue_;

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
  volatile uint64_t tt_large_quries_;
  volatile uint64_t pop_normal_cnt_;

private:
  GroupMap group_map_;
  // for group_map hash node
  char group_map_buf_[sizeof(common::SpHashNode) * MAX_RESOURCE_GROUP];

public:
  common::ObLDLatch lock_;

  // Variables for V2
  WList workers_;
  WList nesting_workers_;
  RpcStatInfo *rpc_stat_info_;
  share::ObTenantModuleInitCtx *mtl_init_ctx_;

  lib::ObMutex workers_lock_;

  share::ObCgroupCtrl &cgroup_ctrl_;

  bool disable_user_sched_;

  double token_usage_;
  int64_t token_usage_check_ts_;
  int64_t token_change_ts_ CACHE_ALIGNED;

  share::ObTenantSpace *ctx_;

  ObSqlThrottleMetrics st_metrics_;
  lib::ObQueryRateLimiter sql_limiter_;
  // idle time between two checkpoints
  int64_t worker_us_;
  int64_t cpu_time_us_ CACHE_ALIGNED;
}; // end of class ObTenant

OB_INLINE int64_t ObResourceGroup::min_worker_cnt() const
{
  uint64_t worker_concurrency = 0;
  if (is_user_group(group_id_)) {
    worker_concurrency = tenant_->cpu_quota_concurrency();
  } else {
    worker_concurrency = share::ObCgSet::instance().get_worker_concurrency(group_id_);
  }
  int64_t cnt = std::max(worker_concurrency * (int64_t)ceil(tenant_->unit_min_cpu()), 1UL);
  if (share::OBCG_CLOG == group_id_ || share::OBCG_LQ == group_id_) {
    cnt = std::max(cnt, 8L);
  } else if (share::OBCG_WR == group_id_) {
    cnt = 2; // one for take snapshot, one for purge
  } else if (share::OBCG_HB_SERVICE == group_id_) {
    cnt = 1;
  }
  return cnt;
}

OB_INLINE int64_t ObResourceGroup::max_worker_cnt() const
{
  int64_t cnt = 0;
  if (share::OBCG_CLOG == group_id_) {
    const int64_t worker_concurrency = share::ObCgSet::instance().get_worker_concurrency(group_id_);
    cnt = std::max(worker_concurrency * (int64_t)ceil(tenant_->unit_max_cpu()), 8L);
  } else if (OB_UNLIKELY(share::OBCG_WR == group_id_)) {
    cnt = 2;
  } else if (OB_UNLIKELY(share::OBCG_HB_SERVICE == group_id_)) {
    cnt = 1;
  } else {
    cnt = tenant_->max_worker_cnt();
  }
  return cnt;
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

} // end of namespace omt
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_TENANT_H_ */
