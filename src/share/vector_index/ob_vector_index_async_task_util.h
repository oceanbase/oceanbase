/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/queue/ob_link.h"
#include "share/vector_index/ob_vector_index_segment.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace storage
{
  class ObValueRowIterator;
}
namespace share
{
typedef common::ObCurTraceId::TraceId TraceId;
const static int64_t VEC_ASYNC_TASK_DEFAULT_ERR_CODE = -1;
static constexpr const char *VEC_TASK_CANCEL_MSG_DISABLED = "task disabled by system configuration";
static constexpr const char *VEC_TASK_CANCEL_MSG_LEADER_SWITCH = "task cancelled due to leader switch";
static constexpr const char *VEC_TASK_CANCEL_MSG_FOLLOWER_TO_LEADER = "follower task cancelled due to switch to leader";
static constexpr const char *VEC_TASK_CANCEL_MSG_DDL_CONFLICT = "task cancelled due to DDL conflict";
static constexpr const char *VEC_TASK_CANCEL_MSG_LS_DESTROY = "task cancelled due to ls destroy";
static constexpr const char *VEC_TASK_CANCEL_MSG_TABLET_TRUNCATE = "task cancelled due to tablet truncate";
class ObPluginVectorIndexMgr;
class ObVectorVerifyRowIterator;


#define CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_)  \
  if (OB_FAIL(ret)) { \
  } else if (++loop_cnt > 20) { \
    ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *); \
    bool is_cancel = false; \
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(ctx_, is_cancel))) { \
      LOG_WARN("fail to check task is cancel", KPC(ctx_));  \
    } else if (is_cancel || (OB_NOT_NULL(vec_idx_mgr_) && vec_idx_mgr_->get_async_task_opt().is_stop())) { \
      ret = OB_CANCELED;  \
      LOG_INFO("async task is cancel", KPC(ctx_));  \
    } else {  \
      loop_cnt = 0; \
    } \
  }

#define CHECK_TASK_CANCELLED(ret, ctx_) \
  if (OB_FAIL(ret)) { \
  } else { \
    bool is_cancel = false; \
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(ctx_, is_cancel))) { \
      OB_LOG(WARN, "fail to check task is cancel", KPC(ctx_)); \
    } else if (is_cancel) { \
      ret = OB_CANCELED; \
      OB_LOG(INFO, "async task is cancel", KR(ret), KPC(ctx_)); \
    } \
  }

enum ObVecIndexAsyncTaskTriggerType
{
  OB_VEC_TRIGGER_AUTO = 0,
  OB_VEC_TRIGGER_MANUAL = 1,
  OB_VEC_TRIGGER_INVALID
};

enum ObVecIndexAsyncTaskStatus //FARM COMPAT WHITELIST
{
  OB_VECTOR_ASYNC_TASK_PREPARE = 0,
  OB_VECTOR_ASYNC_TASK_RUNNING = 1,
  OB_VECTOR_ASYNC_TASK_PENDING = 2, // reserved
  OB_VECTOR_ASYNC_TASK_FINISH = 3,
  OB_VECTOR_ASYNC_TASK_EXCHANGE = 4,
  OB_VECTOR_ASYNC_TASK_CLEAN = 5,
  OB_VECTOR_ASYNC_TASK_CANCEL = 6,
  OB_VECTOR_ASYNC_TASK_STANDBY = 7,
  OB_VECTOR_ASYNC_TASK_QUEUE = 8,
  OB_VECTOR_ASYNC_TASK_INVALID
};

enum ObVecIndexAsyncTaskType { //FARM COMPAT WHITELIST
  OB_VECTOR_ASYNC_INDEX_BUILT = 0, // not used
  OB_VECTOR_ASYNC_INDEX_OPTINAL = 1, // P4
  OB_VECTOR_ASYNC_INDEX_IVF_LOAD = 2, // P1
  OB_VECTOR_ASYNC_INDEX_IVF_CLEAN = 3, // P2
  OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING = 4, // P3
  OB_VECTOR_ASYNC_INDEX_FREEZE = 5, // P2
  OB_VECTOR_ASYNC_INDEX_MERGE = 6, // P2
  OB_VECTOR_ASYNC_MEM_SYNC_TASK = 7, // P1
  OB_VECTOR_ASYNC_TASK_TYPE_INVALID
};

enum ObVectorAsyncOptStatus
{
  OB_VECTOR_ASYNC_OPT_PREPARE = 0,
  OB_VECTOR_ASYNC_OPT_INSERTING,
  OB_VECTOR_ASYNC_OPT_SERIALIZE,
  OB_VECTOR_ASYNC_OPT_REPLACE,
  OB_VECTOR_ASYNC_OPT_STATUS_MAX,
};

struct ObVecIndexTaskProgressInfo
{
  ObVectorAsyncOptStatus vec_opt_status_;
  int64_t opt_esitimate_row_cnt_;
  int64_t opt_finished_row_cnt_;
  float progress_;
  int64_t start_time_;
  int64_t remain_time_;
  ObVecIndexTaskProgressInfo()
      : vec_opt_status_(OB_VECTOR_ASYNC_OPT_STATUS_MAX),
        opt_esitimate_row_cnt_(0),
        opt_finished_row_cnt_(0),
        progress_(0),
        start_time_(0),
        remain_time_(0) {}
  void start_progress(int64_t esitimate_row_cnt) {
    opt_esitimate_row_cnt_ = esitimate_row_cnt;
    opt_finished_row_cnt_ = 0;
    progress_ = 0;
    start_time_ = ObTimeUtility::fast_current_time();
    remain_time_ = 0;
  }
  void update_progress(int64_t row_cnt) {
    opt_finished_row_cnt_ += row_cnt;
    if (opt_esitimate_row_cnt_ > 0 && opt_finished_row_cnt_ < opt_esitimate_row_cnt_) {
      progress_ = static_cast<float>(opt_finished_row_cnt_) / static_cast<float>(opt_esitimate_row_cnt_);
      remain_time_ = (ObTimeUtility::fast_current_time() - start_time_) / progress_ * (1 - progress_);
    } else {
      progress_ = 1;
      remain_time_ = 0;
    }
  }
  void reset() {
    vec_opt_status_ = OB_VECTOR_ASYNC_OPT_STATUS_MAX;
    opt_esitimate_row_cnt_ = 0;
    opt_finished_row_cnt_ = 0;
    progress_ = 0;
    start_time_ = 0;
    remain_time_ = 0;
  }
  TO_STRING_KV(K_(opt_esitimate_row_cnt), K_(opt_finished_row_cnt), K_(vec_opt_status), K_(progress), K_(start_time), K_(remain_time));
};

// Task info for merge segment task (task_info column JSON)
struct ObVecIndexTaskSegInfo
{
  int64_t scn_;
  common::ObString start_key_;
  common::ObString end_key_;
  common::ObString index_type_;
  int64_t vector_cnt_;
  int64_t mem_used_;
  int64_t mem_hold_;
  int64_t min_vid_;
  int64_t max_vid_;
  ObVecIndexTaskSegInfo()
      : scn_(0),
        start_key_(),
        end_key_(),
        index_type_(),
        vector_cnt_(0),
        mem_used_(0),
        mem_hold_(0),
        min_vid_(0),
        max_vid_(0) {}
  void reset()
  {
    scn_ = 0;
    start_key_.reset();
    end_key_.reset();
    index_type_.reset();
    vector_cnt_ = 0;
    mem_used_ = 0;
    mem_hold_ = 0;
    min_vid_ = 0;
    max_vid_ = 0;
  }
  TO_STRING_KV(K_(scn), K_(start_key), K_(end_key), K_(index_type),
               K_(vector_cnt), K_(mem_used), K_(mem_hold), K_(min_vid), K_(max_vid));
};

struct ObVecIndexTaskInfo
{
  common::ObSEArray<ObVecIndexTaskSegInfo, 4> merge_segs_;
  common::ObSEArray<ObVecIndexTaskSegInfo, 1> res_segs_;
  // memory limit related fields
  bool task_memory_limited_;
  int64_t vector_mem_hold_;         // current tenant vector memory used
  int64_t vector_mem_limit_;      // total vector memory limit
  int64_t task_estimate_memory_;    // estimated memory for current task
  int64_t tasks_total_reserved_memory_;         // reserved memory (estimated - actual for other tasks)
  // mem sync batch fields: a single mem sync task may aggregate multiple tablets
  // of the same ls. batch_tablets_ holds the EXTRA tablets aggregated besides the
  // representative one (task_status_.tablet_id_).
  // batch_ret_codes_ and batch_mem_estimates_ are indexed by the FULL batch order
  // [representative, batch_tablets_...] (i.e. length == 1 + batch_tablets_.count()),
  // NOT by batch_tablets_ alone. batch_ret_codes_ is the per-tablet ret_code (used for
  // retry-skip of already-succeeded tablets within the same process); batch_mem_estimates_
  // is the per-tablet memory estimate (statistics only -- mem sync is not memory-limited,
  // but its task_estimate_memory_ total IS read by other tasks' admission check). Only
  // batch_tablets_ is parsed back from the inner table for orphan checks; ret/mem/cancel
  // arrays remain in-memory only. See ob_vector_mem_sync_executor.cpp.
  common::ObSEArray<uint64_t, 4> batch_tablets_;
  common::ObSEArray<int64_t, 4> batch_ret_codes_;
  common::ObSEArray<int64_t, 4> batch_mem_estimates_;
  // In-memory only: tablets in an aggregated mem sync ctx that should be cancelled
  // individually. Protected by ObVecIndexAsyncTaskCtx::lock_.
  common::ObSEArray<uint64_t, 4> batch_cancel_tablets_;

  ObVecIndexTaskInfo()
      : merge_segs_(),
        res_segs_(),
        task_memory_limited_(false),
        vector_mem_hold_(0),
        vector_mem_limit_(0),
        task_estimate_memory_(0),
        tasks_total_reserved_memory_(0),
        batch_tablets_(),
        batch_ret_codes_(),
        batch_mem_estimates_(),
        batch_cancel_tablets_() {}
  void reset()
  {
    merge_segs_.reset();
    res_segs_.reset();
    task_memory_limited_ = false;
    vector_mem_hold_ = 0;
    vector_mem_limit_ = 0;
    task_estimate_memory_ = 0;
    tasks_total_reserved_memory_ = 0;
    batch_tablets_.reset();
    batch_ret_codes_.reset();
    batch_mem_estimates_.reset();
    batch_cancel_tablets_.reset();
  }
  TO_STRING_KV(K_(merge_segs), K_(res_segs), K_(task_memory_limited),
               K_(vector_mem_hold), K_(vector_mem_limit),
               K_(task_estimate_memory), K_(tasks_total_reserved_memory),
               K_(batch_tablets), K_(batch_ret_codes), K_(batch_mem_estimates),
               K_(batch_cancel_tablets));
};

struct ObVecIndexTaskStatus
{
  int64_t gmt_create_;
  int64_t gmt_modified_;

  uint64_t tenant_id_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
  int64_t task_id_;
  int64_t trigger_type_;
  int64_t task_type_;
  int64_t status_;
  SCN target_scn_;
  int64_t ret_code_;
  int64_t last_error_code_;
  // ObString trace_id_str_;
  TraceId trace_id_;
  ObVecIndexTaskProgressInfo progress_info_;
  bool all_finished_;
  ObVecIndexTaskInfo task_info_;
  common::ObAddr exec_addr_; // server address executing the task (from inner table, data_version >= 4.6.0.0)
  int64_t priority_;
  int64_t start_time_;
  int64_t end_time_;
  ObString err_msg_;

  ObVecIndexTaskStatus() :  gmt_create_(0),
                            gmt_modified_(0),
                            tenant_id_(OB_INVALID_ID),
                            table_id_(OB_INVALID_ID),
                            tablet_id_(OB_INVALID_ID),
                            task_id_(-1),
                            trigger_type_(ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_INVALID),
                            task_type_(ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID),
                            status_(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_INVALID),
                            target_scn_(),
                            ret_code_(VEC_ASYNC_TASK_DEFAULT_ERR_CODE),
                            last_error_code_(VEC_ASYNC_TASK_DEFAULT_ERR_CODE),
                            trace_id_(),
                            progress_info_(),
                            all_finished_(false),
                            task_info_(),
                            exec_addr_(),
                            priority_(0),
                            start_time_(0),
                            end_time_(0),
                            err_msg_() {}

  TO_STRING_KV(K_(gmt_create), K_(gmt_modified), K_(tenant_id), K_(table_id),
                K_(tablet_id), K_(task_type), K_(trigger_type), K_(task_id),
                K_(status), K_(target_scn), K_(trace_id), K_(ret_code),
                K_(progress_info), K_(all_finished), K_(task_info), K_(last_error_code),
                K_(exec_addr), K_(priority), K_(start_time), K_(end_time), K_(err_msg));
};

struct ObVecIndexTaskKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t task_id_;
  explicit ObVecIndexTaskKey(uint64_t tenant_id, uint64_t table_id,
      uint64_t tablet_id, int64_t task_id) :
    tenant_id_(tenant_id),
    table_id_(table_id),
    tablet_id_(tablet_id),
    task_id_(task_id) {}
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(tablet_id), K_(task_id));
};

struct ObVecIndexTaskStatusField
{
  ObString field_name_;
  enum
  {
    INT_TYPE = 0,
    UINT_TYPE,
    STRING_TYPE,
  } type_;
  union data
  {
    int64_t int_;
    uint64_t uint_;
    ObString str_;
    data() : str_() {}
  } data_;
  ObVecIndexTaskStatusField()
      : field_name_(),
        type_(INT_TYPE),
        data_() {}
  TO_STRING_KV(K_(field_name), K_(type));
};

typedef common::ObArray<ObVecIndexTaskStatusField> ObVecIndexFieldArray;
typedef common::ObArray<ObVecIndexTaskStatus> ObVecIndexTaskStatusArray;

// Forward decl: queue wrapper node lives in ob_vec_index_priority_queue_manager.h
struct ObVecIndexQueueNode;

// vector index async task ctx
struct ObVecIndexAsyncTaskCtx
{
public:
  ObVecIndexAsyncTaskCtx()
      : tenant_id_(OB_INVALID_TENANT_ID),
        retry_time_(0),
        ls_handle_(),
        task_status_(),
        sys_task_id_(),
        in_thread_pool_(false),
        in_queue_(false),
        queue_node_(nullptr),
        lock_(common::ObLatchIds::OB_VEC_INDEX_ASYNC_TASK_CTX_LOCK),
        allocator_(ObMemAttr(MTL_ID(), "VecIdxTaskCtx")), // set after init
        extra_data_(),
        is_new_task_(false),
        run_inner_sql_(false),
        inner_sql_snapshot_version_(0),
        inner_sql_exec_addr_(),
        truncate_version_(OB_INVALID_VERSION),
        memory_hold_adaptor_(nullptr),
        cancel_ts_(0),
        in_cancel_(false),
        cancel_post_work_pending_(false)
  {}
  virtual ~ObVecIndexAsyncTaskCtx();
  int set_err_msg(const ObString &err_msg);

  storage::ObLS *get_ls() const { return ls_handle_.get_ls(); }

  TO_STRING_KV(K_(tenant_id), K_(retry_time), K_(ls_handle), K_(task_status), K_(sys_task_id), K_(in_thread_pool),
      K_(in_queue), KP_(queue_node), KP_(extra_data), K_(is_new_task), K_(run_inner_sql),
      K_(inner_sql_snapshot_version), K_(inner_sql_exec_addr), K_(truncate_version), KP_(memory_hold_adaptor),
      K_(cancel_ts), K_(in_cancel), K_(cancel_post_work_pending));

  int cancel_task();
  int cancel_task_for_ls_destroy(bool &can_cleanup);
  // Lightweight cancel for switch_to_leader/follower paths: only flips in-memory
  // status to CANCEL and sets cancel_post_work_pending_; defers DB sync and
  // kill_inner_sql to the next scheduler tick to avoid heavy work in switch.
  int mark_task_cancel_lightweight();
  // Run the deferred DB sync + kill_inner_sql when cancel_post_work_pending_.
  // Idempotent: safe for the scheduler to call on every tick.
  int drain_cancel_post_work_if_pending();
  int set_inner_sql_running(const int64_t snapshot_version, const common::ObAddr &exec_addr);
  void clear_inner_sql_running();
  int kill_inner_sql_if_needed();
  int64_t task_memory_hold();
  bool need_mem_limit() const;
  void set_adaptor(ObPluginVectorIndexAdaptor *memory_hold_adaptor) {
    common::ObSpinLockGuard guard(lock_);
    memory_hold_adaptor_ = memory_hold_adaptor;
  }

  // must reset adaptor to nullptr before releasing adaptor to avoid use-after-free
  void reset_adaptor() {
    common::ObSpinLockGuard guard(lock_);
    memory_hold_adaptor_ = nullptr;
  }

  uint64_t tenant_id_;
  uint64_t retry_time_;
  storage::ObLSHandle ls_handle_;
  ObVecIndexTaskStatus task_status_;
  TraceId sys_task_id_;
  bool in_thread_pool_;
  bool in_queue_;
  ObVecIndexQueueNode *queue_node_;
  common::ObSpinLock lock_; // lock for update task_status_
  ObArenaAllocator allocator_; // for extra_data_
  void *extra_data_;
  bool is_new_task_;
  // for inner sql cancel
  bool run_inner_sql_;
  int64_t inner_sql_snapshot_version_;
  common::ObAddr inner_sql_exec_addr_;
  int64_t truncate_version_;
  ObPluginVectorIndexAdaptor *memory_hold_adaptor_; // for real-time memory usage tracking
  // timestamp when cancel_task() set CANCEL flag, 0 means not cancelled
  int64_t cancel_ts_;
  // true while cancel_task() is executing post-lock work (DB sync, kill inner sql, LOG).
  // Prevents scheduler from destroying ctx while cancel_task() still references it.
  bool in_cancel_;
  // Set by mark_task_cancel_lightweight() when the switch path defers the
  // DB sync + kill_inner_sql work; cleared by drain_cancel_post_work_if_pending()
  // after the scheduler tick completes the deferred work.
  bool cancel_post_work_pending_;
};

struct ObVecIndexAsyncTaskKey
{
  common::ObTabletID tablet_id_;
  uint32_t task_type_;

  ObVecIndexAsyncTaskKey()
      : tablet_id_(OB_INVALID_ID),
        task_type_(ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID) {}

  ObVecIndexAsyncTaskKey(const common::ObTabletID &tablet_id, uint32_t task_type)
      : tablet_id_(tablet_id),
        task_type_(task_type) {}

  bool is_valid() const
  {
    return tablet_id_.is_valid() && task_type_ >= 0 && task_type_ < ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash(hash_val);
    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    hash_val = 0;
    if (OB_FAIL(tablet_id_.hash(hash_val))) {
      // do nothing
    } else {
      hash_val = common::murmurhash(&task_type_, sizeof(task_type_), hash_val);
    }
    return ret;
  }

  bool operator==(const ObVecIndexAsyncTaskKey &other) const
  {
    return tablet_id_ == other.tablet_id_ && task_type_ == other.task_type_;
  }

  TO_STRING_KV(K_(tablet_id), K_(task_type));
};

typedef common::hash::ObHashMap<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx *> VecIndexAsyncTaskMap;
typedef common::ObArray<ObVecIndexAsyncTaskCtx*> ObVecIndexTaskCtxArray;

class ObAsyncTaskMapFunc
{
public:
  ObAsyncTaskMapFunc(ObVecIndexTaskCtxArray &array) :array_(array) {}
  ~ObAsyncTaskMapFunc() {}
  int operator()(const hash::HashMapPair<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx*> &entry);
private:
  ObVecIndexTaskCtxArray &array_;
};

class ObVecIndexAsyncTaskOption;

class ObAsyncTaskQueueInvalidateFunc
{
public:
  ObAsyncTaskQueueInvalidateFunc(ObVecIndexAsyncTaskOption &task_opt) : task_opt_(task_opt) {}
  ~ObAsyncTaskQueueInvalidateFunc() {}
  int operator()(const hash::HashMapPair<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx*> &entry);
private:
  ObVecIndexAsyncTaskOption &task_opt_;
};

class ObAsyncTaskCancelFunc
{
public:
  ObAsyncTaskCancelFunc(const ObString &err_msg = ObString(), const bool defer_post_work = false)
      : err_msg_(err_msg), has_failure_(false), defer_post_work_(defer_post_work) {}
  ~ObAsyncTaskCancelFunc() {}
  int operator()(const hash::HashMapPair<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx*> &entry);
  bool has_failure() const { return has_failure_; }
private:
  ObString err_msg_;
  bool has_failure_;
  // When true, only marks in-memory CANCEL state without DB sync or kill_inner_sql.
  // Used by the leader/follower switch path to keep the switch lightweight; the
  // scheduler tick later picks up cancel_post_work_pending_ tasks and drains them.
  bool defer_post_work_;
};

class ObAsyncTaskCancelByTabletFunc
{
public:
  ObAsyncTaskCancelByTabletFunc(const common::ObTabletID &tablet_id)
      : tablet_id_(tablet_id), first_fail_ret_(OB_SUCCESS) {}
  ~ObAsyncTaskCancelByTabletFunc() {}
  int operator()(const hash::HashMapPair<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx*> &entry);
  int get_first_fail_ret() const { return first_fail_ret_; }
private:
  common::ObTabletID tablet_id_;
  int first_fail_ret_;
};

class ObAsyncTaskCancelByTruncateFunc
{
public:
  ObAsyncTaskCancelByTruncateFunc(const common::ObTabletID &tablet_id, const int64_t truncate_version)
      : tablet_id_(tablet_id), truncate_version_(truncate_version), first_fail_ret_(OB_SUCCESS) {}
  ~ObAsyncTaskCancelByTruncateFunc() {}
  int operator()(const hash::HashMapPair<ObVecIndexAsyncTaskKey, ObVecIndexAsyncTaskCtx*> &entry);
  int get_first_fail_ret() const { return first_fail_ret_; }
private:
  common::ObTabletID tablet_id_;
  int64_t truncate_version_;
  int first_fail_ret_;
};

class ObVecIndexAsyncTaskOption
{
public:
  ObVecIndexAsyncTaskOption(uint64_t tenant_id) :
    mem_attr_(tenant_id, "VecIdxATaskCtx"),
    in_flight_lock_(common::ObLatchIds::OB_VEC_INDEX_ASYNC_TASK_CTX_LOCK),
    allocator_(tenant_id),
    ls_task_cnt_(0),
    ls_queued_task_cnt_(0),
    stop_(false)
  {
    SET_IGNORE_MEM_VERSION(mem_attr_);
  }

  ~ObVecIndexAsyncTaskOption();

  int init(const int64_t capacity, const int64_t tenant_id, ObLSID &ls_id);
  void destroy();
  int add_task_ctx(ObTabletID &tablet_id, ObVecIndexAsyncTaskCtx *task, bool &inc_new_task);
  int del_task_ctx(const common::ObTabletID &tablet_id, uint32_t task_type);
  int is_task_ctx_exist(const common::ObTabletID &tablet_id, uint32_t task_type, bool &is_exist);

  // ---- mem sync in-flight tablet map ----
  // Maps any in-flight mem sync tablet_id -> the representative tablet_id of the batch
  // ctx it belongs to. Serves two purposes:
  //   (1) cross-round dedup: load_task skips tablets already in flight (avoids a tablet
  //       appearing in two overlapping batches, which would break the "one-row-per-batch"
  //       semantic). See R1 in the design.
  //   (2) cancel-by-tablet O(1) locate: given any tablet_id, find the representative
  //       tablet_id, then locate the aggregated ctx in task_ctx_map_ by representative key.
  // Lifecycle is kept in lockstep with task_ctx_map_: entries are added when a batch ctx
  // is registered (add_mem_sync_in_flight), and removed in clear_task_ctx by iterating the
  // ctx's batch_tablets_ (erase_mem_sync_in_flight).
  // Returns true via out param if the tablet is already in flight (caller should skip it).
  int is_mem_sync_tablet_in_flight(const common::ObTabletID &tablet_id, bool &is_in_flight);
  // Register all tablets of a batch (representative + extras), all pointing to representative.
  int add_mem_sync_in_flight(const common::ObTabletID &representative_tablet_id,
                             const common::ObIArray<uint64_t> &batch_tablets);
  // Remove all tablets of a batch from the in-flight map (representative + extras).
  int erase_mem_sync_in_flight(const common::ObTabletID &representative_tablet_id,
                               const common::ObIArray<uint64_t> &batch_tablets);
  // Given any tablet_id, return the representative tablet_id of its in-flight batch.
  int get_mem_sync_representative(const common::ObTabletID &tablet_id,
                                  common::ObTabletID &representative_tablet_id);
  void inc_ls_task_cnt() { ATOMIC_INC(&ls_task_cnt_); }
  void dec_ls_task_cnt() { ATOMIC_DEC(&ls_task_cnt_); }
  int64_t get_ls_processing_task_cnt() const { return ATOMIC_LOAD(&ls_task_cnt_); }
  void inc_ls_queued_task_cnt() { ATOMIC_INC(&ls_queued_task_cnt_); }
  void dec_ls_queued_task_cnt() { ATOMIC_DEC(&ls_queued_task_cnt_); }
  int64_t get_ls_queued_task_cnt() const { return ATOMIC_LOAD(&ls_queued_task_cnt_); }
  void set_stop()
  {
    if (!ATOMIC_LOAD(&stop_)) {
      OB_LOG(INFO, "[VEC_ASYNC_TASK] task_opt set_stop", K(lbt()));
    }
    ATOMIC_STORE(&stop_, true);
  }
  void reset_stop() { ATOMIC_STORE(&stop_, false); }
  bool is_stop() { return ATOMIC_LOAD(&stop_); }
  VecIndexAsyncTaskMap &get_async_task_map() { return task_ctx_map_; }
  ObIAllocator *get_allocator() { return &allocator_; }
  TO_STRING_KV(K(mem_attr_));

private:
  ObMemAttr mem_attr_;
  VecIndexAsyncTaskMap task_ctx_map_;
  // mem sync in-flight tablet map: tablet_id -> representative tablet_id. Protected by
  // in_flight_lock_. Only used by mem sync; other task types never touch it.
  common::hash::ObHashMap<common::ObTabletID, common::ObTabletID> mem_sync_in_flight_map_;
  common::ObSpinLock in_flight_lock_;
  common::ObFIFOAllocator allocator_;
  volatile int64_t ls_task_cnt_;
  volatile int64_t ls_queued_task_cnt_;
  bool stop_;
};

// QUEUE_THREAD
class ObVecIndexAsyncTaskHandler : public lib::TGTaskHandler
{
public:
  ObVecIndexAsyncTaskHandler();
  virtual ~ObVecIndexAsyncTaskHandler();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  int push_task(const uint64_t tenant_id, const ObLSID &ls_id, ObVecIndexAsyncTaskCtx *ctx, ObIAllocator *allocator);
  int get_allocator_by_ls(const ObLSID &ls_id, ObIAllocator *&allocator);
  int get_tg_id() { return tg_id_; }

  void inc_async_task_ref() { ATOMIC_INC(&async_task_ref_cnt_); }
  void dec_async_task_ref() { ATOMIC_DEC(&async_task_ref_cnt_); }
  int64_t get_async_task_ref() const { return ATOMIC_LOAD(&async_task_ref_cnt_); }
  void handle_ls_process_task_cnt(const ObLSID &ls_id, const bool is_inc);
  bool is_stopped() { return ATOMIC_LOAD(&stopped_); }
  void set_stop() { ATOMIC_STORE(&stopped_, true); }

  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

  // Check whether tenant CPU has changed since last call; if so, update the thread pool
  // via TG_SET_ADAPTIVE_THREAD and record the new max. Safe to call from scheduler loop.
  int refresh_thread_count();

  // Absolute lower bound: always keep at least 2 threads so P0/P1 tasks can make progress
  // even on the smallest tenant.
  static const int64_t MIN_THREAD_COUNT = 2;
  // Absolute upper bound keeps large tenants from creating too many background workers.
  static const int64_t MAX_THREAD_COUNT = 64;
  // Pure calculation: given a CPU count, return the clamped max thread count.
  // Exposed as static for unit testing; calc_max_thread_count() delegates to this.
  static int64_t calc_max_thread_count_by_cpu(int64_t cpu);
  common::ObSpinLock lock_; // lock for init / thread-count update (public: accessed externally for init guard)

private:
  // Calculate max thread count based on tenant CPU allocation.
  // Uses MTL_CPU_COUNT() * THREAD_FACTOR, clamped to [MIN_THREAD_COUNT, MAX_THREAD_COUNT].
  // Note: depends on MTL context (caller must be in the correct tenant thread).
  int64_t calc_max_thread_count();

  // Scale vector async workers to the tenant CPU count so P1 tasks can drain faster.
  static constexpr double THREAD_FACTOR = 0.5;
  static const int64_t INVALID_TG_ID = -1;
  bool is_inited_;
  int tg_id_;
  volatile int64_t async_task_ref_cnt_;
  volatile bool stopped_;
  int64_t max_thread_cnt_; // current effective max, updated by refresh_thread_count()
};

class ObVecIndexATaskUpdIterator : public blocksstable::ObDatumRowIterator
{
public:
  ObVecIndexATaskUpdIterator(ObVecIndexAsyncTaskCtx *task_ctx = nullptr)
    : got_old_row_(false),
      is_iter_end_(false),
      loop_cnt_(0),
      task_ctx_(task_ctx)
  {}

  virtual ~ObVecIndexATaskUpdIterator() {
    old_row_.reset();
    new_row_.reset();
  }

  int init();
  int add_row(blocksstable::ObDatumRow &old_datum_row, blocksstable::ObDatumRow &new_datum_row);

  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual void reset() override {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexATaskUpdIterator);

private:
  storage::ObValueRowIterator old_row_;
  storage::ObValueRowIterator new_row_;
  bool got_old_row_;
  bool is_iter_end_;
  int64_t loop_cnt_;
  ObVecIndexAsyncTaskCtx *task_ctx_;
};

class ObPluginVectorIndexAdaptor;
class ObVecIndexIAsyncTask
{
public:
  ObVecIndexIAsyncTask(const ObMemAttr &mem_attr)
      : is_inited_(false),
        task_type_(ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID),
        tenant_id_(OB_INVALID_TENANT_ID),
        vector_key_col_idx_(-1),
        vector_data_col_idx_(-1),
        vector_vid_col_idx_(-1),
        vector_col_idx_(-1),
        vector_visible_col_idx_(-1),
        key_col_id_(-1),
        data_col_id_(-1),
        visible_col_id_(-1),
        key_col_cs_type_(CS_TYPE_INVALID),
        data_col_cs_type_(CS_TYPE_INVALID),
        tenant_schema_version_(-1),
        ls_id_(ObLSID::INVALID_LS_ID),
        ctx_(nullptr),
        vec_idx_mgr_(nullptr),
        old_adapter_(nullptr),
        new_adapter_(nullptr),
        mem_attr_(mem_attr),
        allocator_(mem_attr),
        has_replace_old_adapter_(false),
        all_finished_(false),
        filter_sql_str_()
  {}
  ObVecIndexIAsyncTask(const uint64_t tenant_id, const ObLSID &ls_id, ObPluginVectorIndexAdaptor *adapter) : tenant_id_(tenant_id), ls_id_(ls_id), new_adapter_(adapter) {}
  virtual ~ObVecIndexIAsyncTask() {}
  int init(const uint64_t tenant_id, const ObLSID &ls_id, const int task_type, ObVecIndexAsyncTaskCtx *ctx);
  int get_task_type() { return task_type_; }
  ObLSID &get_ls_id() { return ls_id_; }
  ObVecIndexAsyncTaskCtx *get_task_ctx() { return ctx_; }
  void set_old_adapter(ObPluginVectorIndexAdaptor* adapter) { old_adapter_ = adapter; }
  void set_new_adapter(ObPluginVectorIndexAdaptor* adapter) { new_adapter_ = adapter; }
  bool invalid_snapshot_column_ids() {
    return vector_vid_col_idx_ == -1 || vector_col_idx_ == -1 || vector_key_col_idx_ == -1 || vector_data_col_idx_ == -1 || vector_visible_col_idx_ == -1;
  }
  bool all_finished() { return all_finished_; }
  virtual void check_task_free() {}
  virtual int do_work() = 0;

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(task_type), K_(tenant_id), K_(ls_id), KPC(ctx_));

protected:
  bool is_inited_;
  int task_type_;  // 0. built; 1. opt; 2. ivf load; 3. ivf clean
  uint64_t tenant_id_;
  int64_t vector_key_col_idx_;
  int64_t vector_data_col_idx_;
  int64_t vector_vid_col_idx_;
  int64_t vector_col_idx_;
  int64_t vector_visible_col_idx_;
  int64_t key_col_id_;
  int64_t data_col_id_;
  int64_t visible_col_id_;
  ObCollationType key_col_cs_type_;
  ObCollationType data_col_cs_type_;
  int64_t tenant_schema_version_;
  ObLSID ls_id_;
  ObVecIndexAsyncTaskCtx *ctx_;
  ObPluginVectorIndexMgr *vec_idx_mgr_;
  ObPluginVectorIndexAdaptor* old_adapter_;
  ObPluginVectorIndexAdaptor* new_adapter_;
  ObMemAttr mem_attr_;
  common::ObArenaAllocator allocator_;
  bool has_replace_old_adapter_;
  bool all_finished_;
  ObSqlString filter_sql_str_;
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexIAsyncTask);
};

class ObVecIndexAsyncTask : public ObVecIndexIAsyncTask
{
public:
  ObVecIndexAsyncTask()
      : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "VecIdxASyTask"))
  {
  }
  ObVecIndexAsyncTask(const ObMemAttr &mem_attr)
      : ObVecIndexIAsyncTask(mem_attr)
  {
  }

  ObVecIndexAsyncTask(const uint64_t tenant_id, const ObLSID &ls_id, ObPluginVectorIndexAdaptor *adapter) : ObVecIndexIAsyncTask(tenant_id, ls_id, adapter) {}
  virtual ~ObVecIndexAsyncTask() {}
  int do_work() override;
  int parallel_optimize_vec_index();
  int execute_insert();
  int execute_exchange();
  int execute_clean();
  int get_task_paralellism(int64_t &parallelism);
  int get_partition_name(const ObTableSchema &data_table_schema, const int64_t data_table_id, const int64_t index_table_id, const ObTabletID &tablet_id, common::ObIAllocator &allocator, ObString &partition_names);
  int create_new_adapter(ObPluginVectorIndexService *vector_index_service, ObPluginVectorIndexAdapterGuard &old_adapter_guard, ObPluginVectorIndexAdaptor *&new_adapter);
  // pipeline call
  int execute_write_snap_index(
      transaction::ObTxDesc *tx_desc,
      ObVectorIndexRowIterator &iter,
      const ObTabletID &tablet_id,
      const int64_t key_col_idx,
      const int64_t data_col_idx,
      const int64_t visible_col_idx,
      const uint64_t snapshot_version);

protected:
  static const int BATCH_CNT = 2000; // 8M / 4(sizeof(float)) / 1000(dim)
  int get_current_scn(share::SCN &current_scn);
  int execute_inner_sql(const ObTableSchema &data_schema, const int64_t data_table_id, const int64_t dest_table_id, const int64_t task_id, const int64_t parallelism, ObString &partition_names, share::SCN &current_scn);
  int execute_inner_sql(
    const int64_t schema_version, const bool is_user_hidden_table, const bool need_padding,
    const int64_t data_table_id, const int64_t dest_table_id,
    const int64_t task_id, const int64_t parallelism, const ObString &partition_names, share::SCN &current_scn);
  int build_inc_index(ObPluginVectorIndexAdaptor &adaptor);
  int process_data_for_index(ObPluginVectorIndexAdaptor &adaptor, ObPluginVectorIndexAdaptor &old_adaptor);
  int optimize_vector_index(ObPluginVectorIndexAdaptor &adaptor, ObPluginVectorIndexAdaptor &old_adaptor);
  int refresh_snapshot_index_data(ObPluginVectorIndexAdaptor &adaptor, transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot);
  int get_old_snapshot_data(
      ObPluginVectorIndexAdaptor &adaptor,
      transaction::ObTxDesc *tx_desc,
      const int64_t snapshot_column_count,
      common::ObCollationType cs_type,
      ObSEArray<uint64_t, 4> &extra_column_idxs,
      storage::ObTableScanIterator *table_scan_iter,
      storage::ObValueRowIterator &delete_row_iter,
      transaction::ObTxReadSnapshot &snapshot);
  int delete_tablet_data(
      ObPluginVectorIndexAdaptor &adaptor,
      ObTabletID& tablet_id,
      storage::ObDMLBaseParam &dml_param,
      transaction::ObTxDesc *tx_desc,
      storage::ObTableScanIterator *table_scan_iter,
      ObSEArray<uint64_t, 4> &dml_column_ids,
      bool check_null_chunk = false);
  int delete_incr_table_data(ObPluginVectorIndexAdaptor &adaptor, storage::ObDMLBaseParam &dml_param,
                             transaction::ObTxDesc *tx_desc, ObVecIndexAsyncTaskCtx *ctx = nullptr);
  int delete_inc_index_rows(
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const uint64_t schema_version,
      const uint64_t timeout_us);
  int exchange_snap_index_rows(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &snapshot_table_schema,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const uint64_t timeout_us);
  int clean_snap_index_rows(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &snapshot_table_schema,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const uint64_t timeout_us);
  int get_snap_index_column_info(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &snapshot_table_schema,
      ObIArray<uint64_t> &all_column_ids,
      ObIArray<uint64_t> &dml_column_ids,
      ObIArray<uint64_t> &extra_column_idxs,
      common::ObCollationType &cs_type);
  int prepare_dml_param(
      ObDMLBaseParam &dml_param,
      share::schema::ObTableDMLParam &table_dml_param,
      storage::ObStoreCtxGuard &store_ctx_guard,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const uint64_t schema_version,
      const uint64_t timeout_us);
  int prepare_dml_udp_row_iter(
      ObTableScanIterator *table_scan_iter,
      ObIArray<uint64_t> &extra_column_idxs,
      ObVecIndexATaskUpdIterator &row_iter);
  int prepare_dml_del_row_iter(
      transaction::ObTxDesc *tx_desc,
      common::ObCollationType cs_type,
      ObTableScanIterator *table_scan_iter,
      ObIArray<uint64_t> &extra_column_idxs,
      ObVectorVerifyRowIterator &row_iter,
      transaction::ObTxReadSnapshot &snapshot,
      const bool is_force_delete = false);
  int prepare_schema_and_snapshot(
      const ObTableSchema *&data_schema,
      const ObTableSchema *&snapshot_schema,
      const int64_t data_table_id,
      const int64_t snap_table_id,
      const uint64_t snapshot_version,
      oceanbase::transaction::ObTxReadSnapshot &snapshot);
  int construct_vector_row(
      blocksstable::ObDatumRow *in_datum_row,
      ObIArray<uint64_t> &extra_column_idxs,
      const int64_t in_key_col_idx,
      const int64_t in_data_col_idx,
      const int64_t in_visible_col_idx,
      blocksstable::ObDatumRow &out_row);
  int fetch_dml_write_row(
      ObVectorIndexRowIterator &iter,
      const int64_t key_col_idx,
      const int64_t data_col_idx,
      const int64_t visible_col_idx,
      ObIArray<uint64_t> &extra_column_idxs,
      storage::ObValueRowIterator &dml_row_iter);
  int get_ls_leader_addr(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObAddr &leader_addr);
  int try_reuse_segments_from_old_adapter();
  bool check_snapshot_table_available(ObPluginVectorIndexAdaptor &adaptor);
  bool check_task_satisfied_memory_limited(ObPluginVectorIndexAdaptor &adaptor);
  bool check_new_adapter_exist(const int64_t task_id);
  int check_snapshot_table_has_visible_column(bool &has_visible_row);
  int check_and_refresh_new_adapter(bool &need_do_next);
  int get_read_snapshot_table_scn(share::SCN &target_scn);
  int try_deseriale_snapshot_data(common::ObNewRowIterator *snapshot_idx_iter, const bool need_unvisible);
  int check_finished_exchange_before(share::SCN &current_scn, bool &is_finised);
  int fetch_commit_scn_from_tx_table(const transaction::ObTransID &tx_id, share::SCN &commit_scn);

private:
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexAsyncTask);
};

namespace schema
{
class ObSchemaGetterGuard;
}

class ObVecIndexAsyncTaskUtil final
{
  static const int64_t DEFAULT_VEC_INSERT_BATCH_SIZE = 10;

public:
  static int read_vec_tasks(
      const uint64_t tenant_id,
      const char* tname,
      const bool for_update /*false*/,
      const ObVecIndexFieldArray& filters,
      storage::ObLS *ls, /* null means get all tenant task */
      common::ObISQLClient& proxy,
      ObVecIndexTaskStatusArray& result_arr,
      common::ObIAllocator *allocator /*NULL*/);
  // LS-scoped rows only (tablet_id != -1). Do not use read_vec_tasks(ls=null), which targets tablet_id == -1.
  static int read_ls_scope_vec_tasks_from_inner_table(
      const uint64_t tenant_id,
      const char *tname,
      const bool for_update,
      const ObVecIndexFieldArray &filters,
      common::ObISQLClient &proxy,
      ObVecIndexTaskStatusArray &result_arr);
  // Used by ObVecITaskExecutor::load_triggered_task to install ctx for a manual-trigger row.
  static int try_add_task_ctx_from_inner_row(
      const int64_t tenant_id,
      const ObVecIndexTaskStatus &task_row,
      storage::ObLS *ls,
      storage::ObLSHandle &ls_handle,
      ObVecIndexAsyncTaskOption &async_task_opt,
      const bool verify_tablet_on_ls);
  static int delete_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskKey &key,
      int64_t &affect_rows);
  // Generic update helper for __all_vector_index_task rows.
  // When expected_statuses is provided, appends an IN (...) clause so the UPDATE only
  // runs when the DB-side status matches. If matched is non-null, it is set true when
  // affect_rows > 0 (DB actually updated) and false when the conditional UPDATE
  // was skipped (affect_rows == 0, i.e. another thread already advanced the status).
  // ret stays OB_SUCCESS in the skipped case so callers must inspect matched to
  // decide whether to proceed on a DB-already-updated assumption.
  static int update_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskKey &key,
      ObVecIndexFieldArray &update_fields,
      ObVecIndexTaskProgressInfo &progress_info,
      ObVecIndexTaskInfo &task_info,
      const common::ObAddr &exec_addr,
      int64_t priority,
      int64_t start_time,
      int64_t end_time,
      const ObString &err_msg,
      const common::ObIArray<int64_t> *expected_statuses = nullptr,
      bool *matched = nullptr);

  static int format_merge_task_info_to_json(const ObVecIndexTaskInfo &task_info, common::ObSqlString &output);
  static int format_task_mem_info_to_json(const ObVecIndexTaskInfo &task_info, common::ObSqlString &output);
  // Aggregated mem sync task: emit batch tablet list + per-tablet ret_code as JSON.
  static int format_mem_sync_task_info_to_json(uint64_t representative_tablet_id,
                                               const ObVecIndexTaskInfo &task_info,
                                               common::ObSqlString &output);
  static int parse_task_info_from_json(const common::ObString &json_str, common::ObIAllocator &allocator, ObVecIndexTaskInfo &task_info);
  static int append_seg_info_to_json(const ObVecIndexTaskSegInfo &seg, common::ObSqlString &output);
  static int append_task_info_seg_array_to_json(const common::ObIArray<ObVecIndexTaskSegInfo> &segs, common::ObSqlString &output);
  static int seg_meta_to_task_seg_info(const ObVectorIndexSegmentMeta &seg_meta, common::ObIAllocator &allocator, ObVecIndexTaskSegInfo &out);
  static int insert_vec_tasks(
      uint64_t tenant_id,
      const char *tname,
      const int64_t batch_size,
      common::ObISQLClient &proxy,
      ObVecIndexTaskCtxArray &task);
  static int batch_insert_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskCtxArray &task);
  static int clear_history_expire_task_record(
      const uint64_t tenant_id,
      const int64_t batch_size,
      common::ObMySQLTransaction &proxy,
      int64_t &clear_rows);
  static int move_task_to_history_table(
      const uint64_t tenant_id,
      const int64_t batch_size,
      common::ObMySQLTransaction &proxy,
      int64_t &move_rows);
  // Resume-time R1 sweep: push all non-FINISH rows owned by this observer (exec_addr = self_addr)
  // whose tablet currently lives on ls_handle to FINISH+CANCELED. Does not reconstruct ctx.
  static int sweep_self_residual_for_ls(
      const int64_t tenant_id,
      const char *tname,
      storage::ObLSHandle &ls_handle,
      const common::ObAddr &self_addr,
      common::ObISQLClient &proxy);
  static int get_table_id_from_adapter(
      ObPluginVectorIndexAdaptor *adapter,
      const ObTabletID &tablet_id,
      int64_t &table_id);
  static int construct_task_key(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t tablet_id,
      const int64_t task_id,
      ObVecIndexFieldArray& task_key);
  static int update_status_and_ret_code(
      ObVecIndexAsyncTaskCtx *task_ctx);
  /**
   * @brief Persist task_ctx status/ret_code/target_scn only if DB.status ∈ expected_statuses.
   *
   * Wraps update_vec_task with a WHERE status IN (...) clause. Used by scheduler/handle
   * paths to avoid regressing a concurrently-advanced row (e.g. overwriting RUNNING with
   * QUEUE after the handle thread already started executing).
   *
   * @param task_ctx          non-null ctx; status/ret_code/target_scn read under ctx->lock_ by caller
   * @param expected_statuses non-empty array of ObVecIndexAsyncTaskStatus values
   * @retval OB_SUCCESS       either updated the row, or affect_rows==0 because the DB status
   *                          had already been advanced concurrently (treated as no-op).
   *                          Callers who need to distinguish these two cases should call
   *                          update_vec_task directly with the `matched` out-parameter.
   * @retval !=OB_SUCCESS     transient SQL error; caller should retry on next scheduler tick.
   */
  static int update_status_and_ret_code_if_match(
      ObVecIndexAsyncTaskCtx *task_ctx,
      const common::ObIArray<int64_t> &expected_statuses);
  static void invalidate_task_queue_node(
      ObVecIndexAsyncTaskOption &task_opt,
      ObVecIndexAsyncTaskCtx *task_ctx);
  // Kill stale inner SQL sessions by trace_id on a target server during recovery.
  // Used to clean up orphaned sessions from crashed/switched nodes.
  static int kill_stale_inner_sql_by_trace_id(
      common::ObMySQLProxy &proxy,
      const common::ObCurTraceId::TraceId &trace_id,
      const uint64_t tenant_id,
      const common::ObAddr &exec_addr);
  static int get_insert_task_ctx_array(
      ObVecIndexTaskCtxArray &in_task,
      ObVecIndexTaskCtxArray &out_task,
      common::hash::ObHashSet<ObVecIndexAsyncTaskKey> &duplicate_tablet_task);
  static int get_duplicate_tablet_vec_task(
      uint64_t tenant_id,
      const char* tname,
      common::ObISQLClient& proxy,
      common::hash::ObHashSet<ObVecIndexAsyncTaskKey> &duplicate_tablet_task);

  static void get_row_need_skip_for_compatibility(blocksstable::ObDatumRow &row, const bool is_need_unvisible_row, bool &skip_this_row);
  static int set_inner_sql_adapter(const int64_t task_id, ObPluginVectorIndexAdaptor *adapter);
  static int get_inner_sql_adapter(const int64_t task_id, ObPluginVectorIndexAdaptor *&adapter);
  static int set_inner_sql_slice_info(const int64_t task_id, rootserver::ObDDLSliceInfo &ddl_slice_info);
  static int get_inner_sql_slice_info(const int64_t task_id, ObIAllocator &allocator, rootserver::ObDDLSliceInfo &ddl_slice_info);
  static int set_inner_sql_schema_version(const int64_t task_id, const int64_t schema_version);
  static int set_inner_sql_snapshot_version(const int64_t task_id, const int64_t snapshot_version);
  static int get_inner_sql_schema_version(const int64_t task_id, int64_t &schema_version);
  static int get_inner_sql_snapshot_version(const int64_t task_id, int64_t &snapshot_version);
  static int set_inner_sql_ret_code(const int64_t task_id, int ret_code);
  static int get_inner_sql_ret_code(const int64_t task_id, int &ret_code);
  static int init_tablet_rebuild_new_adapter(ObPluginVectorIndexAdaptor *new_adapter, const ObString &row_key);

  static int64_t get_processing_task_cnt(ObVecIndexAsyncTaskOption &task_opt);
  static int fetch_new_task_id(const uint64_t tenant_id, int64_t &new_task_id);
  static int add_sys_task(ObVecIndexAsyncTaskCtx *task);
  static int remove_sys_task(ObVecIndexAsyncTaskCtx *task);
  static int check_task_result(ObVecIndexAsyncTaskCtx *task_ctx);
  static int clear_task_ctxs(ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array);
  static int clear_task_ctx(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx);
  static int mark_mem_sync_tablet_cancel(ObVecIndexAsyncTaskCtx *task_ctx, const common::ObTabletID &tablet_id);
  static int check_mem_sync_tablet_is_cancel(
      ObVecIndexAsyncTaskCtx *task_ctx,
      const common::ObTabletID &tablet_id,
      bool &is_cancel);

  static const int64_t VEC_INDEX_TASK_MAX_RETRY_TIME = 3;
  static int fetch_new_trace_id(const uint64_t basic_num, ObIAllocator *allocator, TraceId &new_trace_id);
  static int in_active_time(const uint64_t tenant_id, bool& is_active_time);
  static int check_task_is_cancel(ObVecIndexAsyncTaskCtx *task, bool &is_cancel);
  // Short name <-> enum mapping for task type, used by vector_task_thread_limit_percent config.
  static const char *get_vec_task_type_short_name(ObVecIndexAsyncTaskType type);
  static int get_vec_task_type_by_short_name(const char *name, ObVecIndexAsyncTaskType &type);
  static int insert_new_task(uint64_t tenant_id, ObVecIndexTaskCtxArray &task_ctx_array);
  // Create one memsync trigger task record in __all_vector_index_task (PREPARE status).
  // Timer/scheduler will read and execute memdata sync for this record.
  static int create_memsync_trigger_task_record(
    uint64_t tenant_id, uint64_t table_id, uint64_t tablet_id);
  static int get_truncate_version(uint64_t tenant_id, uint64_t table_id, int64_t &truncate_version);
  static int construct_read_task_sql(
      const uint64_t tenant_id,
      const char *tname,
      const bool for_update /* select for update*/,
      const bool is_read_tenant_async_task,
      const ObVecIndexFieldArray &filters,
      common::ObISQLClient &proxy,
      ObSqlString &sql);
  static int extract_one_task_sql_result(
      sqlclient::ObMySQLResult *result,
      ObVecIndexTaskStatus &task);

  // memory limit helpers
  // just support merge task, mem sync task and optimize task now
  static int estimate_task_memory(
      ObVecIndexAsyncTaskCtx *task_ctx,
      int64_t &estimate_mem,
      ObPluginVectorIndexAdaptor *adapter = nullptr);
  // estimate memory for merge task based on actual segments to be merged
  static int get_merge_task_vec_cnt(
      ObPluginVectorIndexAdaptor *adapter,
      int64_t &vec_cnt,
      const uint64_t tenant_id);
  // Resolve table 5 id from task aux table id (3/4) for estimate_vector_memory_used.
  static int get_snapshot_table_id(
      schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const uint64_t aux_table_id,
      uint64_t &snapshot_table_id);
  // check if task can start by memory, considering all LS tasks in tenant
  // all_task_opts: array of task_opt pointers from all LS in the tenant
  // Skips gate when candidate is already in thread pool (in_thread_pool_).
  static int check_task_can_start_by_memory(
      const common::ObIArray<ObVecIndexAsyncTaskOption *> &all_task_opts,
      ObVecIndexAsyncTaskCtx *candidate_ctx,
      bool &can_start);
  static int get_tablet_row_count(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const ObTabletID &index_tablet_id,
      common::ObIAllocator &allocator,
      int64_t &row_count);
  static bool disallow_cancel_task(const ObVecIndexAsyncTaskCtx *task_ctx);

private:
  static int kill_mysql_session_for_tenant(
      common::ObMySQLProxy &proxy,
      const uint64_t tenant_id,
      const uint64_t session_id);
};

}
}

#endif // OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_
