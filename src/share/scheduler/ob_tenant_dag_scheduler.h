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

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_mutex.h"
#include "lib/profile/ob_trace_id.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "share/scheduler/ob_diagnose_config.h"
#include "share/ob_table_range.h"
#include "common/errsim_module/ob_errsim_module_type.h"

namespace oceanbase
{
namespace compaction
{
struct ObTabletCompactionProgress;
struct ObDiagnoseTabletCompProgress;
class ObMergeDagHash;
class ObTabletMergeExecuteDag;
struct ObTabletMergeDagParam;
class ObCompactionDiagnoseMgr;
class ObTabletMergeDag;
struct ObIBasicInfoParam;
class ObCompactionMemoryContext;
struct ObCompactionDagStatus;
}
namespace share
{

struct ObDagWarningInfo;
typedef common::ObCurTraceId::TraceId ObDagId;
class ObIDag;
class ObIDagNet;
class ObTenantDagScheduler;
class ObTenantDagWorker;


struct ObDiagnoseLocation final
{
  ObDiagnoseLocation()
    : filename_(nullptr),
      function_(nullptr),
      line_(INVALID_LINE)
  {}
  ~ObDiagnoseLocation() { reset(); }

  void set(const char* filename, const int line, const char* function);
  void set(const ObDiagnoseLocation &new_location);
  bool is_valid() const;
  OB_INLINE void reset() {
    filename_ = nullptr;
    line_ = INVALID_LINE;
    function_ = nullptr;
  }

  ObDiagnoseLocation & operator = (const ObDiagnoseLocation &other);
  inline int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "%s:%d(%s)", filename_, line_, function_);
    return pos;
  }
  static const int INVALID_LINE = -1;
  const char *filename_;
  const char *function_;
  int line_;
};
#define CTX_SET_DIAGNOSE_LOCATION(ctx) \
  { \
    (ctx).info_collector_.error_location_.set(__FILENAME__, __LINE__, __FUNCTION__); \
  }

#define SET_DIAGNOSE_LOCATION(error_location) \
  { \
    if (OB_NOT_NULL(error_location)) { \
      (error_location)->set(__FILENAME__, __LINE__, __FUNCTION__); \
    } \
  }

#define SET_DAG_LOCATION(dag) \
  { \
    if (OB_NOT_NULL(dag)) { \
      (dag)->set_dag_error_location(__FILENAME__, __LINE__, __FUNCTION__); \
    } \
  }

class ObINodeWithChild
{
public:
  ObINodeWithChild()
      : indegree_(0),
        lock_(common::ObLatchIds::WORK_DAG_LOCK)
  {}
  virtual ~ObINodeWithChild() { reset(); }

  void reset()
  {
    indegree_ = 0;
    parent_.reset();
    children_.reset();
  }
  bool check_with_lock(); // defense check
  int64_t get_indegree() const;
  int add_parent_node(ObINodeWithChild &parent);
  const common::ObIArray<ObINodeWithChild*> &get_child_nodes() const { return children_; }
  int remove_parent_for_children();
  int remove_child_for_parents();
  int deep_copy_children(const common::ObIArray<ObINodeWithChild*> &other);
  void reset_children();
  void reset_node();
  int erase_node(const ObINodeWithChild *node, const bool node_is_parent);
  int check_child_exist(
      const ObINodeWithChild *child,
      bool &is_exist);

  TO_STRING_KV(K_(indegree));

protected:
  virtual int add_child_without_lock(ObINodeWithChild &child);

private:
  void inc_indegree() { ATOMIC_INC(&indegree_); }
  int64_t dec_indegree() { return ATOMIC_SAF(&indegree_,1); }

protected:
  static const int64_t DEFAULT_PARENT_NUM = 8;
  static const int64_t DEFAULT_CHILDREN_NUM = 8;
  int64_t indegree_;
  lib::ObMutex lock_;
  common::ObSEArray<ObINodeWithChild*, DEFAULT_PARENT_NUM> parent_;
  common::ObSEArray<ObINodeWithChild*, DEFAULT_CHILDREN_NUM> children_;
};

class ObITask : public common::ObDLinkBase<ObITask>, public ObINodeWithChild
{
public:
  enum ObITaskType
  {
    TASK_TYPE_UT = 0,
    TASK_TYPE_MACROMERGE = 1,
    TASK_TYPE_INDEX_FINISH = 2,
    TASK_TYPE_MAIN_FINISH = 3,
    TASK_TYPE_MINOR_MERGE = 4,
    TASK_TYPE_INDEX_PERPARE = 5,
    TASK_TYPE_INDEX_LOCAL_SORT = 6,
    TASK_TYPE_INDEX_MERGE = 7,
    TASK_TYPE_NORMAL_MINOR_MERGE = 8,
    TASK_TYPE_BUILD_INDEX_NORMAL_MERGE = 9,
    TASK_TYPE_UNIQUE_INDEX_CHECKING = 10,
    TASK_TYPE_REPORT_INDEX_STATUS = 11,
    TASK_TYPE_MERGE_PREPARE_TASK = 12,
    TASK_TYPE_INDEX_MERGE_TO_LATEST_FINISH = 13,
    TASK_TYPE_COMPACT_TO_LASTEST = 14,
    TASK_TYPE_SSTABLE_MERGE_PREPARE = 15,
    TASK_TYPE_SSTABLE_MERGE_FINISH = 16,
    TASK_TYPE_SPLIT_PREPARE_TASK = 17,
    TASK_TYPE_SPLIT_TASK = 18,
    TASK_TYPE_SPLIT_FINISH_TASK = 19,
    TASK_TYPE_UNIQUE_CHECKING_PREPARE = 20,
    TASK_TYPE_SIMPLE_UNIQUE_CHECKING = 21,
    TASK_TYPE_MIGRATE_PREPARE = 22,
    TASK_TYPE_MIGRATE_COPY_LOGIC = 23,
    TASK_TYPE_MIGRATE_FINISH_LOGIC = 24,
    TASK_TYPE_MIGRATE_COPY_PHYSICAL = 25,
    TASK_TYPE_MIGRATE_FINISH_PHYSICAL = 26,
    TASK_TYPE_MIGRATE_FINISH = 27,
    TASK_TYPE_FAKE = 28,
    TASK_TYPE_MIGRATE_ENABLE_REPLAY = 29,
    TASK_TYPE_MAJOR_MERGE_FINISH = 30,
    TASK_TYPE_GROUP_MIGRATE = 31,
    TASK_TYPE_SQL_BUILD_INDEX = 32, // build index by sql plan.
    TASK_TYPE_SERVER_PREPROCESS = 33,
    TASK_TYPE_FAST_RECOVERY = 34,
    TASK_TYPE_MIGRATE_POST_PREPARE = 35,
    TASK_TYPE_FAST_MIGRATE_ASYNC_TASK = 36,
    TASK_TYPE_VALIDATE_BACKUP = 37,
    TASK_TYPE_VALIDATE_FINISH = 38,
    TASK_TYPE_BUILD_CHANGE_REPLICA = 39,
    TASK_TYPE_RESTORE_TAILORED_PREPARE = 40,
    TASK_TYPE_RESTORE_TAILORED_PROCESS = 41,
    TASK_TYPE_RESTORE_TAILORED_FINISH = 42,
    TASK_TYPE_BACKUP_BACKUPSET = 43,
    TASK_TYPE_BACKUP_ARCHIVELOG = 44,
    TASK_TYPE_COMPLEMENT_PREPARE = 45,
    TASK_TYPE_COMPLEMENT_WRITE = 46,
    TASK_TYPE_COMPLEMENT_MERGE = 47,
    TASK_TYPE_BACKUP_CLEAN = 48,
    TASK_TYPE_DDL_KV_DUMP = 49,
    TASK_TYPE_DDL_KV_MERGE = 50,
    TASK_TYPE_TRANSFER_BACKFILL_TX = 51,
    TASK_TYPE_TRANSFER_REPLACE_TABLE = 52,
    TASK_TYPE_MDS_MINI_MERGE = 53,
    TASK_TYPE_TTL_DELETE = 54,
    TASK_TYPE_TENANT_SNAPSHOT_CREATE = 55,
    TASK_TYPE_TENANT_SNAPSHOT_GC = 56,
    TASK_TYPE_BATCH_FREEZE_TABLETS = 57,
    TASK_TYPE_LOB_BUILD_MAP = 58,
    TASK_TYPE_LOB_MERGE_MAP = 59,
    TASK_TYPE_LOB_WRITE_DATA = 60,
    TASK_TYPE_DDL_SPLIT_PREPARE = 61,
    TASK_TYPE_DDL_SPLIT_WRITE = 62,
    TASK_TYPE_DDL_SPLIT_MERGE = 63,
    TASK_TYPE_TABLE_FINISH_BACKFILL = 64,
    TASK_TYPE_MAX,
  };

  enum ObITaskStatus
  {
    TASK_STATUS_INITING = 0,
    TASK_STATUS_WAITING = 1,
    TASK_STATUS_RUNNING = 2,
    TASK_STATUS_FINISHED = 3,
    TASK_STATUS_RETRY = 4,
    TASK_STATUS_FAILED = 5,
  };

  enum ObITaskColor
  {
    WHITE, // not visited
    GRAY,  // visiting, on the stack
    BLACK, // visited, all paths have walked
  };

  explicit ObITask(const ObITaskType type);
  virtual ~ObITask();
  int do_work();
  OB_INLINE bool is_valid() const;
  int add_child(ObITask &child);
  void set_dag(ObIDag &dag) { dag_ = &dag; }
  ObIDag *get_dag() const { return dag_; }
  void set_status(const ObITaskStatus status) { status_ = status; }
  ObITaskStatus get_status() const { return status_; }
  ObITaskType get_type() const { return type_; }
  void prepare_check_cycle();
  ObITaskColor get_color() { return color_; }
  void set_color(const ObITaskColor color) { color_ = color; }
  int64_t get_last_visit_child() { return last_visit_child_; }
  void set_last_visit_child(const int64_t idx) { last_visit_child_ = idx; }
  int generate_next_task();

  VIRTUAL_TO_STRING_KV(KP(this), K_(type), K_(status), K_(dag));
private:
  virtual int generate_next_task(ObITask *&next_task) { UNUSED(next_task); return common::OB_ITER_END; }
  virtual int process() = 0;
private:
  void reset();
  int copy_children_to(ObITask &next_task) const;

protected:
  ObIDag *dag_;
private:
  ObITaskType type_;
  ObITaskStatus status_;
  int64_t last_visit_child_;
  ObITaskColor color_;
};

class ObFakeTask: public ObITask
{
public:
  ObFakeTask(): ObITask(TASK_TYPE_FAKE) {}
  virtual ~ObFakeTask() {}
  virtual int process() override;
};

enum ObDagListIndex
{
  RANK_DAG_LIST = 0,
  READY_DAG_LIST = 1,
  WAITING_DAG_LIST = 2,
  DAG_LIST_MAX
};

enum ObDagNetListIndex
{
  BLOCKING_DAG_NET_LIST = 0,
  RUNNING_DAG_NET_LIST = 1,
  FINISHED_DAG_NET_LIST = 2,
  DAG_NET_LIST_MAX
};

const char *dag_net_list_to_str(const ObDagNetListIndex &dag_net_list_index);
inline bool is_valid_dag_net_list(const ObDagNetListIndex &dag_net_list_index)
{
  return dag_net_list_index >= 0 && dag_net_list_index < DAG_NET_LIST_MAX;
}

struct ObIDagInitParam
{
  ObIDagInitParam() {}
  virtual ~ObIDagInitParam() {}
  virtual bool is_valid() const = 0;
};

struct ObDagInfo;
class ObIDag : public common::ObDLinkBase<ObIDag>, public ObINodeWithChild
{
public:
  typedef lib::ObLockGuard<ObIDag> ObDagGuard;
  friend ObDagGuard;
  enum ObDagStatus
  {
    DAG_STATUS_INITING = 0,
    DAG_STATUS_READY = 1,                                        // the dag is ready
    DAG_STATUS_NODE_RUNNING = 2,                                 // the dag is running,the ready dag and the running dag are both in ready_list
    DAG_STATUS_FINISH = 3,
    DAG_STATUS_NODE_FAILED = 4,
    DAG_STATUS_ABORT = 5,                                        // not using status 5, 6, 7
    DAG_STATUS_RM = 6,
    DAG_STATUS_HALTED = 7,
    DAG_STATUS_RETRY = 8,
    /* add new item in ObIDagStatusStr*/
    DAG_STATUS_MAX,
  };

  const static char *ObIDagStatusStr[];

  static const int64_t MergeDagPrioCnt = 3;
  static const ObDagPrio::ObDagPrioEnum MergeDagPrio[];
  static const int64_t MergeDagTypeCnt = 7;
  static const ObDagType::ObDagTypeEnum MergeDagType[];

  explicit ObIDag(const ObDagType::ObDagTypeEnum type);
  virtual ~ObIDag(); // DO NOT ACQUIRE LOCK OF DAG_SCHEDULER !!!!
  int basic_init(ObIAllocator &allocator);
  int add_task(ObITask &task);
  template<typename T>
  int alloc_task(T *&task);
  template <typename T, typename... Args>
  int create_task(ObITask *parent, T *&task, Args&&... args);
  bool is_valid();
  bool is_valid_type() const;
  void set_dag_ret(const int ret) { ATOMIC_VCAS(&dag_ret_, common::OB_SUCCESS, ret); }
  int get_dag_ret() const { return dag_ret_; };
  virtual void set_dag_error_location() { /*do nothing*/ }
  void set_dag_error_location(const char* filename,
      const int line, const char* function) { error_location_.set(filename, line, function); }
  void set_dag_status(const ObDagStatus status) { dag_status_ = status; }
  ObDagStatus get_dag_status() const { return dag_status_; }
  bool is_dag_failed() const { return ObIDag::DAG_STATUS_NODE_FAILED == dag_status_; }
  void set_add_time() { add_time_ = ObTimeUtility::fast_current_time(); }
  int64_t get_add_time() const { return add_time_; }
  ObDagPrio::ObDagPrioEnum get_priority() const { return priority_; }
  void set_priority(ObDagPrio::ObDagPrioEnum prio) { priority_ = prio; }
  const ObDagId &get_dag_id() const { return id_; }
  int set_dag_id(const ObDagId &dag_id);
  ObDagType::ObDagTypeEnum get_type() const { return type_; }
  static const char *get_dag_status_str(enum ObDagStatus status);
  static const char *get_dag_type_str(const ObDagType::ObDagTypeEnum type);
  static const char *get_dag_prio_str(const ObDagPrio::ObDagPrioEnum prio);
  static const char *get_dag_module_str(const enum ObDagType::ObDagTypeEnum type);
  static bool is_finish_status(ObDagStatus dag_status)
  {
    return DAG_STATUS_FINISH == dag_status || DAG_STATUS_ABORT == dag_status;
  }
  static ObDiagnoseTabletType get_diagnose_tablet_type(const enum ObDagType::ObDagTypeEnum type)
  {
    ObDiagnoseTabletType diagnose_type = ObDiagnoseTabletType::TYPE_DIAGNOSE_TABLET_MAX;
    if (ObDagType::ObDagTypeEnum::DAG_TYPE_MINI_MERGE == type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_MINI_MERGE;
    } else if (ObDagType::ObDagTypeEnum::DAG_TYPE_MERGE_EXECUTE == type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_MINOR_MERGE;
    } else if (ObDagType::ObDagTypeEnum::DAG_TYPE_MAJOR_MERGE <= type
        && ObDagType::ObDagTypeEnum::DAG_TYPE_CO_MERGE_FINISH >= type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_MEDIUM_MERGE;
    } else if (ObDagType::ObDagTypeEnum::DAG_TYPE_TX_TABLE_MERGE == type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_TX_TABLE_MERGE;
    } else if (ObDagType::ObDagTypeEnum::DAG_TYPE_MDS_MINI_MERGE == type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_MDS_MINI_MERGE;
    } else if (ObDagType::ObDagTypeEnum::DAG_TYPE_BATCH_FREEZE_TABLETS == type) {
      diagnose_type = ObDiagnoseTabletType::TYPE_BATCH_FREEZE;
    }
    return diagnose_type;
  }
  bool has_set_stop() { return ATOMIC_LOAD(&is_stop_); }
  void set_stop() { ATOMIC_SET(&is_stop_, true); }
  ObIDagNet *get_dag_net() const { return dag_net_; }
  void set_dag_net(ObIDagNet &dag_net)
  {
    lib::ObMutexGuard guard(lock_);
    dag_net_ = &dag_net;
  }
  void clear_dag_net()
  {
    lib::ObMutexGuard guard(lock_);
    dag_net_ = nullptr;
  }
  ObDagListIndex get_list_idx() const { return list_idx_; }
  void set_list_idx(ObDagListIndex list_idx) { list_idx_ = list_idx; }

  int64_t get_running_task_count() const { return running_task_cnt_; }
  int64_t get_task_list_count()
  {
    lib::ObMutexGuard guard(lock_);
    return task_list_.get_size();
  }
  virtual int gene_warning_info(ObDagWarningInfo &info, ObIAllocator &allocator);
  virtual bool ignore_warning() { return false; }
  virtual bool check_can_retry();
  void set_max_retry_times(const uint32_t max_retry_times)
  {
    lib::ObMutexGuard guard(lock_);
    max_retry_times_ = max_retry_times;
  }
  virtual int inner_reset_status_for_retry()
  { // The internal state(clear all tasks) of Dag needs to be traced back to after init in this func
    return common::OB_NOT_SUPPORTED;
  }
  // This func will be called when dag running meets error
  // Then the dag will be add into DagScheduler to retry running
  int reset_status_for_retry();
  void reset_task_running_status(ObITask &task, ObITask::ObITaskStatus task_status);

  virtual bool check_can_schedule()
  { // true: waiting_list -> ready_list OR ready_list -> start running
    // false: ready_list -> waiting_list
    return true;
  }
  int add_child(ObIDag &child);
  int update_status_in_dag_net(bool &dag_net_finished);
  int finish(const ObDagStatus status, bool &dag_net_finished);
  void gene_dag_info(ObDagInfo &info, const char *list_info);
  virtual int gene_compaction_info(compaction::ObTabletCompactionProgress &progress)
  {
    UNUSED(progress);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress)
  {
    UNUSED(progress);
    return common::OB_NOT_IMPLEMENT;
  }
  void set_start_time() { start_time_ = ObTimeUtility::fast_current_time(); }
  int64_t get_start_time() const { return start_time_; }
  void set_force_cancel_flag();
  bool get_force_cancel_flag() { return force_cancel_flag_; }
  int add_child_without_inheritance(ObIDag &child);
  int add_child_without_inheritance(const common::ObIArray<ObINodeWithChild*> &child_array);
  int get_next_ready_task(ObITask *&task);
  void free_task(ObITask &task);
  int finish_task(ObITask &task);
  bool has_finished();
  virtual int report_result()
  {
    // This func is only used for no need retry to report result.
    // Caller implements this function to complete certain functions
    return OB_SUCCESS;
  }

  virtual int generate_next_dag(ObIDag *&next_dag) { UNUSED(next_dag); return common::OB_ITER_END; }
  virtual int set_result(const int32_t result) { UNUSED(result); return common::OB_SUCCESS; }
  int fill_comment(char *buf, const int64_t buf_len);

  virtual bool is_ha_dag() const { return false; }

  DECLARE_VIRTUAL_TO_STRING;
  DISABLE_COPY_ASSIGN(ObIDag);
public:
  virtual bool operator == (const ObIDag &other) const = 0;
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const = 0;
  virtual int init_by_param(const ObIDagInitParam *param)
  {
    UNUSED(param);
    return common::OB_SUCCESS;
  }
  virtual int create_first_task() { return common::OB_SUCCESS; }
  virtual int64_t get_data_size() const { return 0; }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const = 0;
  virtual lib::Worker::CompatMode get_compat_mode() const = 0;
  virtual uint64_t get_consumer_group_id() const = 0;
  int remove_task(ObITask &task);
protected:
  void clear_task_list_with_lock();

#ifdef ERRSIM
public:
  virtual common::ObErrsimModuleType::TYPE get_module_type() { return ObErrsimModuleType::ERRSIM_MODULE_NONE; }
#endif

protected:
  int dag_ret_;
  int64_t add_time_;
  int64_t start_time_;
  uint64_t consumer_group_id_;
  share::ObDiagnoseLocation error_location_;

private:
  typedef common::ObDList<ObITask> TaskList;
  static const int64_t DEFAULT_TASK_NUM = 32;
private:
  void reset();
  void clear_task_list();
  void clear_running_info();
  int check_cycle();
  int lock() { return lock_.lock(); }
  int unlock() { return lock_.unlock(); }
  void inc_running_task_cnt() { ++running_task_cnt_; }
  void dec_running_task_cnt() { --running_task_cnt_; }
  int inner_add_child_without_inheritance(ObIDag &child);
private:
  common::ObIAllocator *allocator_;
  bool is_inited_;
  ObDagType::ObDagTypeEnum type_;
  ObDagPrio::ObDagPrioEnum priority_;
  ObDagId id_;
  ObDagStatus dag_status_;
  int64_t running_task_cnt_;
  TaskList task_list_; // should protect by lock
  bool is_stop_;
  bool force_cancel_flag_; // should protect by lock
  uint32_t max_retry_times_;  // should protect by lock
  uint32_t running_times_;
  ObIDagNet *dag_net_; // should protect by lock
  ObDagListIndex list_idx_;
};

/*
 * ObIDagNet
 * */
struct ObDagRecord  : public common::ObDLinkBase<ObDagRecord>
{
public:
  ObDagRecord()
  : dag_ptr_(nullptr),
    dag_type_(ObDagType::DAG_TYPE_MAX),
    dag_status_()
  {}
  TO_STRING_KV(KP_(dag_ptr), "dag_type", ObIDag::get_dag_type_str(dag_type_),
      "dag_status", ObIDag::get_dag_status_str(dag_status_));
  ObIDag *dag_ptr_; // just for print, can't visit
  ObDagType::ObDagTypeEnum dag_type_;
  ObIDag::ObDagStatus dag_status_;
};

class ObIDagNet : public common::ObDLinkBase<ObIDagNet>
{
public:
  static const int64_t DEFAULT_DAG_BUCKET = 1024;
  typedef common::hash::ObHashMap<const ObIDag *,
                          ObDagRecord *,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<const ObIDag *>,
                          common::hash::equal_to<const ObIDag *> > DagRecordMap;

  explicit ObIDagNet(const ObDagNetType::ObDagNetTypeEnum type);
  virtual ~ObIDagNet()
  {
    ObIDagNet::reset();
  }
  void reset();
  int basic_init(ObIAllocator &allocator);
  ObDagNetType::ObDagNetTypeEnum get_type() const { return type_; }
  int add_dag_into_dag_net(ObIDag &dag);
  virtual int schedule_rest_dag()
  {
    return OB_SUCCESS;
  }
  virtual bool inner_check_finished()
  {
    return true;
  }
  OB_INLINE bool inner_check_finished_without_lock() { return (is_cancel_ || inner_check_finished()) && dag_record_map_.empty(); }
  bool check_finished_and_mark_stop();
  int update_dag_status(ObIDag &dag, bool &dag_net_finished);
  int erase_dag_from_dag_net(ObIDag &dag);
  static const char *get_dag_net_type_str(enum ObDagNetType::ObDagNetTypeEnum type);

  virtual int init_by_param(const ObIDagInitParam *param)
  {
    UNUSED(param);
    return common::OB_SUCCESS;
  }
  void gene_dag_info(ObDagInfo &info, const char *list_info);
  void init_dag_id();
  int set_dag_id(const ObDagId &dag_net_id);
  const ObDagId &get_dag_id() const { return dag_net_id_; }
  void set_add_time() { add_time_ = ObTimeUtility::fast_current_time(); }
  int64_t get_add_time() const { return add_time_; }
  void set_start_time() { start_time_ = ObTimeUtility::fast_current_time(); }
  int64_t get_start_time() const { return start_time_; }
  virtual int clear_dag_net_ctx()
  {
    return OB_SUCCESS;
  }
  void set_cancel();
  bool is_cancel();
  void set_last_dag_finished();
  bool is_inited();
  bool is_started();
  virtual int deal_with_cancel()
  {
    return OB_SUCCESS;
  }
  OB_INLINE bool is_co_dag_net() const { return ObDagNetType::DAG_NET_TYPE_CO_MAJOR == type_; }
  virtual bool is_ha_dag_net() const { return false; }
  void diagnose_dag(common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list);
  int add_dag_warning_info(ObIDag *dag);
  int add_dag_warning_info();
public:

  virtual bool is_valid() const = 0;
  virtual int start_running() = 0;
  virtual bool operator == (const ObIDagNet &other) const = 0;
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const = 0;
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;

  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  void remove_dag_record_(ObDagRecord &dag_record);

private:
  bool is_stopped_;
  lib::ObMutex lock_;
  common::ObIAllocator *allocator_; // use to alloc dag in dag_net later
  ObDagNetType::ObDagNetTypeEnum type_;
  int64_t add_time_;
  int64_t start_time_;
  DagRecordMap dag_record_map_;
  ObDagId dag_net_id_;
  ObDagWarningInfo *first_fail_dag_info_;
  bool is_cancel_;
  bool is_finishing_last_dag_; // making dag net freed after last dag freed if dag net can be freed after finish last dag
};

struct ObDagInfo
{
public:
  ObDagInfo();
  ~ObDagInfo() {}
  TO_STRING_KV(K_(tenant_id), K_(dag_type), K_(dag_net_type), K_(dag_key), K_(dag_net_key), K_(dag_id),
      "dag_status", ObIDag::get_dag_status_str(dag_status_),
      K_(running_task_cnt), K_(add_time), K_(start_time), K_(indegree), K_(comment));
  ObDagInfo & operator = (const ObDagInfo &other);
  bool is_valid() const;

public:
  int64_t tenant_id_;
  ObDagType::ObDagTypeEnum dag_type_;
  ObDagNetType::ObDagNetTypeEnum dag_net_type_;
  char dag_key_[common::OB_DAG_KEY_LENGTH];
  char dag_net_key_[common::OB_DAG_KEY_LENGTH];
  share::ObDagId dag_id_;
  ObIDag::ObDagStatus dag_status_;
  int64_t running_task_cnt_;
  int64_t add_time_;
  int64_t start_time_;
  int64_t indegree_;
  char comment_[common::OB_DAG_COMMET_LENGTH];
};

struct ObDagSchedulerInfo
{
public:
  enum ObValueType
  {
    GENERAL = 0,
    UP_LIMIT,
    LOW_LIMIT,
    DAG_COUNT,
    DAG_NET_COUNT,
    RUNNING_TASK_CNT,
    VALUE_TYPE_MAX,
  };
  static const char *ObValueTypeStr[VALUE_TYPE_MAX];
  static const char* get_value_type_str(ObValueType type);

  ObDagSchedulerInfo();
  ~ObDagSchedulerInfo() {}
  TO_STRING_KV(K_(value_type), K_(key), K_(value));
  ObDagSchedulerInfo & operator = (const ObDagSchedulerInfo &other);
public:
  int64_t tenant_id_;
  ObValueType value_type_;
  char key_[common::OB_DAG_KEY_LENGTH];
  int64_t value_;
};

class ObTenantDagWorker : public lib::TGRunnable, public common::ObDLinkBase<ObTenantDagWorker>
{
public:
  typedef common::ObDLinkNode<ObTenantDagWorker *> Node;
  typedef common::ObDList<Node> WorkerNodeList;
  enum DagWorkerStatus
  {
    DWS_FREE,
    DWS_RUNNABLE,
    DWS_WAITING,
    DWS_RUNNING,
    DWS_STOP,
  };
public:
  ObTenantDagWorker();
  ~ObTenantDagWorker();
  int init(const int64_t check_period);
  int start();
  void stop();
  void wait();
  void reset();
  void destroy();
  void resume();
  void run1() override;
  int yield();
  void set_task(ObITask *task);
  void set_function_type(const int64_t function_type) { function_type_ = function_type; }
  int set_dag_resource(const uint64_t group_id);
  bool need_wake_up() const;
  ObITask *get_task() const { return task_; }
  DagWorkerStatus get_status() { return status_; }
  static ObTenantDagWorker *self() { return self_; }
  static void set_reserve_mode() { is_reserve_mode_ = true; }
  static bool is_reserve_mode() { return is_reserve_mode_; }
  static compaction::ObCompactionMemoryContext* get_mem_ctx() { return mem_ctx_; }
  static void set_mem_ctx(compaction::ObCompactionMemoryContext *mem_ctx) { if (nullptr == mem_ctx_) { mem_ctx_ = mem_ctx; } }
  uint64_t get_group_id() { return group_id_; }
  bool get_force_cancel_flag();
  bool hold_by_compaction_dag() const { return hold_by_compaction_dag_; }
private:
  void notify(DagWorkerStatus status);
  void reset_compaction_thread_locals() { is_reserve_mode_ = false; mem_ctx_ = nullptr; hold_by_compaction_dag_ = false; }
private:
  RLOCAL_STATIC(ObTenantDagWorker *, self_);
  RLOCAL_STATIC(bool, is_reserve_mode_);
  RLOCAL_STATIC(compaction::ObCompactionMemoryContext *, mem_ctx_);
  static const uint32_t SLEEP_TIME_MS = 100; // 100ms
private:
  common::ObThreadCond cond_;
  ObITask *task_;
  DagWorkerStatus status_;
  int64_t check_period_;
  int64_t last_check_time_;
  int64_t function_type_;
  uint64_t group_id_;
  int tg_id_;
  bool hold_by_compaction_dag_;
  bool is_inited_;
};

// TODO(@DanLing) parameters in ObTenantDagScheduler
class DagSchedulerConfig
{
public:
  double min_cpu_;
  double max_cpu_;
  int64_t total_mem_limit;
};

class ObDagNetScheduler {
public:
  ObDagNetScheduler()
    : allocator_(nullptr),
      ha_allocator_(nullptr),
      scheduler_(nullptr)
  {}
  ~ObDagNetScheduler() { destroy(); }
  void destroy();
  int init(
      const uint64_t tenant_id,
      const int64_t dag_limit,
      ObIAllocator &allocator,
      ObIAllocator &ha_allocator,
      ObTenantDagScheduler &scheduler);

  bool is_empty(); // only for unittest
  int add_dag_net(ObIDagNet &dag_net);
  void erase_dag_net_or_abort(ObIDagNet &dag_net);
  void erase_dag_net_id_or_abort(ObIDagNet &dag_net);
  void finish_dag_net_without_lock(ObIDagNet &dag_net);
  void erase_dag_net_list_or_abort(const ObDagNetListIndex &dag_net_list_index, ObIDagNet *dag_net);
  void add_dag_net_list_or_abort(const ObDagNetListIndex &dag_net_list_index, ObIDagNet *dag_net);
  void finish_dag_net(ObIDagNet &dag_net);
  void dump_dag_status();
  int64_t get_dag_net_count();
  void get_all_dag_scheduler_info(
      ObDagSchedulerInfo *info_list,
      common::ObIArray<void *> &scheduler_infos,
      int64_t &idx);
  void get_all_dag_info(
      ObDagInfo *info_list,
      common::ObIArray<void *> &dag_infos,
      int64_t &idx, const int64_t total_cnt);
  int diagnose_dag_net(
      ObIDagNet &dag_net,
      common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list,
      ObDagId &dag_net_id,
      int64_t &start_time);
  int64_t get_dag_net_count(const ObDagNetType::ObDagNetTypeEnum type);
  bool is_dag_map_full();
  int loop_running_dag_net_list();
  // do not hold dag_net_map_lock_, otherwise deadlock when clear_dag_net_ctx,  see
  int loop_finished_dag_net_list();
  int loop_blocking_dag_net_list();
  int check_dag_net_exist(
    const ObDagId &dag_id, bool &exist);
  int cancel_dag_net(const ObDagId &dag_id);
  int get_first_dag_net(ObIDagNet *&dag_net);
  int check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist);
private:
  typedef common::ObDList<ObIDagNet> DagNetList;
  typedef common::hash::ObHashMap<const ObIDagNet*,
                          ObIDagNet*,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<const ObIDagNet*>,
                          common::hash::equal_to<const ObIDagNet*> > DagNetMap;
  typedef common::hash::ObHashMap<ObDagId, const ObIDagNet *>DagNetIdMap;
  static const int64_t DEFAULT_MAX_DAG_MAP_CNT = 150000;
  static const int64_t DEFAULT_MAX_RUNNING_DAG_NET_CNT = 30000;
  static const int64_t DEFAULT_MAX_DAG_NET_CNT = 500000;
  static const int64_t STOP_ADD_DAG_PERCENT = 70;
  static const int64_t PRINT_SLOW_DAG_NET_THREASHOLD = 30 * 60 * 1000 * 1000L; // 30m
  static const int64_t SLOW_COMPACTION_DAG_NET_THREASHOLD = 6 * 60 * 60 * 1000 * 1000L; // 6hours
  static const int64_t LOOP_PRINT_LOG_INTERVAL = 30 * 1000 * 1000L; // 30s

private:
  ObIAllocator* allocator_;
  ObIAllocator* ha_allocator_;
  ObTenantDagScheduler *scheduler_;
  lib::ObMutex dag_net_map_lock_;
  DagNetMap dag_net_map_; // lock by dag_net_map_lock_
  /*
   * blocking and running list should always locked by dag_net_map_lock_, but finished not.
   * finished dag net list must without lock when free dag net, otherwise it would deadlock when clearing dag net ctx
   */
  DagNetList dag_net_list_[DAG_NET_LIST_MAX];
  DagNetIdMap dag_net_id_map_; // for HA to search dag_net of specified dag_id  // lock by dag_net_map_lock_
  int64_t dag_net_cnts_[ObDagNetType::DAG_NET_TYPE_MAX];  // lock by dag_net_map_lock_
};

class ObDagPrioScheduler
{
public:
  typedef common::ObDList<ObTenantDagWorker> WorkerList;
  ObDagPrioScheduler()
    : allocator_(nullptr),
      ha_allocator_(nullptr),
      scheduler_(nullptr),
      priority_(ObDagPrio::DAG_PRIO_MAX),
      running_task_cnts_(0),
      limits_(0),
      adaptive_task_limit_(0)
  {}
  ~ObDagPrioScheduler() { destroy();}
  void destroy();
  void destroy_workers();
  int init(
      const uint64_t tenant_id,
      const int64_t dag_limit,
      const int64_t priority,
      ObIAllocator &allocator,
      ObIAllocator &ha_allocator,
      ObTenantDagScheduler &scheduler);
  bool is_empty() const
  {
    bool bret = true;
    bret &= (dag_list_[READY_DAG_LIST].is_empty()
      && dag_list_[WAITING_DAG_LIST].is_empty()
      && dag_list_[RANK_DAG_LIST].is_empty());
    return bret;
  } // only for unittest

  // func with lock inside
  int loop_ready_dag_list(bool &is_found);
  int loop_waiting_dag_list();
  void dump_dag_status();
  int inner_add_dag(
    const bool emergency,
    const bool check_size_overflow,
    ObIDag *&dag);
  void get_all_dag_scheduler_info(
    ObDagSchedulerInfo *info_list,
    common::ObIArray<void *> &scheduler_infos,
    int64_t &idx);
  void get_all_dag_info(
    ObDagInfo *info_list,
    common::ObIArray<void *> &dag_infos,
    int64_t &idx, const int64_t total_cnt);
  int get_minor_exe_dag_info(
    compaction::ObTabletMergeExecuteDag &dag,
    ObIArray<share::ObScnRange> &merge_range_array);
  void add_compaction_info(
    int64_t &idx,
    const int64_t total_cnt,
    const ObDagListIndex list_index,
    compaction::ObTabletCompactionProgress *progress,
    ObIArray<compaction::ObTabletCompactionProgress *> &progress_array);

  // 1. check ls compaction exist
  // 2. cancel ls compaction waiting dag
  int check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist);
  int get_compaction_dag_count(int64_t dag_count);
  int get_max_major_finish_time(const int64_t version, int64_t &estimated_finish_time);
  int diagnose_dag(
    const ObIDag &dag,
    compaction::ObDiagnoseTabletCompProgress &progress);
  int diagnose_minor_exe_dag(
    const compaction::ObMergeDagHash &merge_dag_info,
    compaction::ObDiagnoseTabletCompProgress &progress);
  int diagnose_all_dags();
  int get_complement_data_dag_progress(const ObIDag &dag,
    int64_t &row_scanned,
    int64_t &row_inserted);
  int deal_with_finish_task(ObITask &task, ObTenantDagWorker &worker, int error_code);
  // force_cancel: whether to cancel running dag
  int cancel_dag(const ObIDag &dag, const bool force_cancel = false);
  int check_dag_exist(const ObIDag &dag, bool &exist);
  int64_t get_limit();
  int64_t get_adaptive_limit();
  int64_t get_running_task_cnt();
  int set_thread_score(const int64_t score, int64_t &old_val, int64_t &new_val);
  bool try_switch(ObTenantDagWorker &worker);
private:
  OB_INLINE bool is_waiting_dag_type(ObDagType::ObDagTypeEnum dag_type)
  { // will add into waiting dag list in add_dag() func
    return false;
  }
  OB_INLINE bool is_mini_compaction_dag(ObDagType::ObDagTypeEnum dag_type) const
  {
    return ObDagType::DAG_TYPE_MINI_MERGE == dag_type;
  }
  OB_INLINE bool is_minor_compaction_dag(ObDagType::ObDagTypeEnum dag_type) const
  {
    return ObDagType::DAG_TYPE_MERGE_EXECUTE == dag_type;
  }
  OB_INLINE bool is_rank_dag_type(ObDagType::ObDagTypeEnum dag_type) const
  {
    return is_mini_compaction_dag(dag_type) ||
           is_minor_compaction_dag(dag_type) ||
           ObDagType::DAG_TYPE_CO_MERGE_PREPARE == dag_type; // add co prepare dag to rank list first
  }
  OB_INLINE bool is_rank_dag_prio() const
  {
    return ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority_
        || ObDagPrio::DAG_PRIO_COMPACTION_MID == priority_
        || ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority_;
  }
  int sys_task_start(ObIDag &dag);
  // Please lock prio_lock_ before calling the function with _ suffix
  int move_dag_to_list_(
    ObIDag &dag,
    ObDagListIndex from_list_index,
    ObDagListIndex to_list_index,
    const bool add_last = true);
  int add_dag_into_list_and_map_(
    const ObDagListIndex list_index,
    ObIDag &dag,
    const bool emergency);
  int get_stored_dag_(ObIDag &dag, ObIDag *&stored_dag);
  int inner_add_dag_(
    const bool emergency,
    const bool check_size_overflow,
    ObIDag *&dag);
  void add_schedule_info_(const ObDagType::ObDagTypeEnum dag_type, const int64_t data_size);
  void add_added_info_(const ObDagType::ObDagTypeEnum dag_type);
  int schedule_one_();
  int schedule_dag_(ObIDag &dag, bool &move_dag_to_waiting_list);
  int pop_task_from_ready_list_(ObITask *&task);
  int rank_compaction_dags_();
  void try_update_adaptive_task_limit_(const int64_t batch_size);
  int batch_move_compaction_dags_(const int64_t batch_size);
  bool check_need_compaction_rank_() const;
  int do_rank_compaction_dags_(
    const int64_t batch_size,
    common::ObSEArray<compaction::ObTabletMergeDag *, 32> &rank_dags);
  int generate_next_dag_(ObIDag &dag);
  int add_dag_warning_info_into_dag_net_(ObIDag &dag, bool &need_add);
  int finish_dag_(
    const ObIDag::ObDagStatus status,
    ObIDag &dag,
    const bool try_move_child);
  int try_move_child_to_ready_list_(ObIDag &dag);
  int erase_dag_(ObIDag &dag);
  int deal_with_fail_dag_(ObIDag &dag, bool &retry_flag);
  int finish_task_in_dag_(ObITask &task, ObIDag &dag);
  void pause_worker_(ObTenantDagWorker &worker);
  bool check_need_load_shedding_(const bool for_schedule);

public:
  static const int32_t MAX_SHOW_DAG_CNT = 100;
private:
  typedef common::ObDList<ObIDag> DagList;
  typedef common::hash::ObHashMap<const ObIDag *,
                          ObIDag *,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<const ObIDag *>,
                          common::hash::equal_to<const ObIDag *> > DagMap;
  static const int32_t COMPACTION_DAG_RERANK_FACTOR = 10;
  static const int64_t DUMP_STATUS_INTERVAL = 10 * 1000LL * 1000LL;
  static const int64_t TASK_MAY_HANG_INTERVAL = 90 * 60 * 1000L * 1000L; // 90 min
private:
  DagMap dag_map_;
  DagList dag_list_[DAG_LIST_MAX];
  lib::ObMutex prio_lock_;  // Make sure the lock is outside if there are nested locks
  WorkerList waiting_workers_;  // workers waiting for time slice to run
  WorkerList running_workers_;  // running workers // lock with prio_lock_
  ObIAllocator* allocator_;
  ObIAllocator* ha_allocator_;
  ObTenantDagScheduler *scheduler_;
  int64_t priority_;
  int64_t running_task_cnts_;
  int64_t limits_;           // needs to be equal with thread_score
  int64_t adaptive_task_limit_;
};

#define DEFINE_ATOMIC_ARRAY_FUNC(name, var) \
  int64_t get_##name(const int64_t idx) const { return ATOMIC_LOAD(&(var[idx])); } \
  void add_##name(const int64_t idx) { ATOMIC_INC(&(var[idx])); } \
  void add_##name(const int64_t idx, const int64_t value) { ATOMIC_AAF(&(var[idx]), value); } \
  void sub_##name(const int64_t idx) { ATOMIC_DEC(&(var[idx])); } \
  void clear_##name(const int64_t idx) { ATOMIC_SET(&(var[idx]), 0); } \

#define DEFINE_ATOMIC_VAR_FUNC(name, var) \
  int64_t get_##name() const { return ATOMIC_LOAD(&var); } \
  void add_##name() { ATOMIC_INC(&var); } \
  void sub_##name() { ATOMIC_DEC(&var); } \
  void clear_##name() { ATOMIC_SET(&var, 0); } \

class ObTenantDagScheduler : public lib::TGRunnable
{
public:
  static int mtl_init(ObTenantDagScheduler* &scheduler);
public:
  ObTenantDagScheduler();
  virtual ~ObTenantDagScheduler();
  // int start();
  void stop();
  void wait();
  void reload_config();
  int init(const uint64_t tenant_id,
           const int64_t check_period = DEFAULT_CHECK_PERIOD,
           const int64_t loop_waiting_list_period = LOOP_WAITING_DAG_LIST_INTERVAL,
           const int64_t dag_limit = DEFAULT_MAX_DAG_NUM);
  int add_dag(ObIDag *dag, const bool emergency = false, const bool check_size_overflow = true);
  int add_dag_net(ObIDagNet *dag_net);
  template<typename T>
  int create_dag(
      const ObIDagInitParam *param,
      T *&dag);
  template<typename T>
  int create_and_add_dag(
      const ObIDagInitParam *param,
      const bool emergency = false,
      const bool check_size_overflow = true);
  template<typename T>
  int alloc_dag(T *&dag);
  template<typename T>
  int alloc_dag_with_priority(const ObDagPrio::ObDagPrioEnum &prio, T *&dag);
  template<typename T>
  int create_and_add_dag_net(const ObIDagInitParam *param);
  void free_dag(ObIDag &dag);
  void inner_free_dag(ObIDag &dag);
  template<typename T>
  void free_dag_net(T *&dag_net);
  void run1() final;
  void notify();
  void notify_when_dag_net_finish();
  void reset();
  void destroy();
  int64_t get_work_thread_num()
  {
    ObThreadCondGuard guard(scheduler_sync_);
    return work_thread_num_;
  }
  int64_t get_dag_limit() const { return dag_limit_; }
  bool is_empty()
  {
    bool bret = true;
    for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) {
      bret &= prio_sche_[i].is_empty();
    }
    bret &= dag_net_sche_.is_empty();
    return bret;
  } // only for unittest

  DEFINE_ATOMIC_ARRAY_FUNC(running_dag_cnts, running_dag_cnts_);
  DEFINE_ATOMIC_ARRAY_FUNC(added_dag_cnts, added_dag_cnts_);
  DEFINE_ATOMIC_ARRAY_FUNC(scheduled_dag_cnts, scheduled_dag_cnts_);
  DEFINE_ATOMIC_ARRAY_FUNC(scheduled_task_cnts, scheduled_task_cnts_);
  DEFINE_ATOMIC_ARRAY_FUNC(scheduled_data_size, scheduled_data_size_);
  DEFINE_ATOMIC_ARRAY_FUNC(type_dag_cnt, dag_cnts_);

  DEFINE_ATOMIC_VAR_FUNC(cur_dag_cnt, dag_cnt_);
  DEFINE_ATOMIC_VAR_FUNC(total_running_task_cnt, total_running_task_cnt_);
  DEFINE_ATOMIC_VAR_FUNC(scheduled_task_cnt, scheduled_task_cnt_);
  bool need_fast_schedule_dag_net() { return ATOMIC_LOAD(&fast_schedule_dag_net_); }
  void set_fast_schedule_dag_net() { ATOMIC_SET(&fast_schedule_dag_net_, true); }
  void clear_fast_schedule_dag_net() { ATOMIC_SET(&fast_schedule_dag_net_, false); }
  int generate_dag_id(ObDagId &dag_id);
  bool dag_count_overflow(const ObDagType::ObDagTypeEnum type);
  int64_t allowed_schedule_dag_count(const ObDagType::ObDagTypeEnum type);
  int64_t get_dag_count(const ObDagType::ObDagTypeEnum type);
  int64_t get_running_task_cnt(const ObDagPrio::ObDagPrioEnum priority);
  int get_limit(const int64_t prio, int64_t &limit);
  int get_adaptive_limit(const int64_t prio, int64_t &limit);
  int check_dag_exist(const ObIDag *dag, bool &exist);
  // force_cancel: whether to cancel running dag
  int cancel_dag(const ObIDag *dag, const bool force_cancel = false);
  int get_all_dag_info(
      common::ObIAllocator &allocator,
      common::ObIArray<void *> &dag_infos);
  int get_all_dag_scheduler_info(
      common::ObIAllocator &allocator,
      common::ObIArray<void *> &scheduler_infos);
  int64_t get_dag_net_count(const ObDagNetType::ObDagNetTypeEnum type);
  int get_all_compaction_dag_info(
      ObIAllocator &allocator,
      ObIArray<compaction::ObTabletCompactionProgress *> &progress_array);
  int get_minor_exe_dag_info(
      const compaction::ObTabletMergeDagParam &param,
      ObIArray<share::ObScnRange> &merge_range_array);
  int diagnose_minor_exe_dag(
      const compaction::ObMergeDagHash *merge_dag_info,
      compaction::ObDiagnoseTabletCompProgress &progress);
  int get_max_major_finish_time(const int64_t version, int64_t &estimated_finish_time);
  int diagnose_dag(const ObIDag *dag, compaction::ObDiagnoseTabletCompProgress &input_progress);
  int diagnose_dag_net(
      ObIDagNet *dag_net,
      common::ObIArray<compaction::ObDiagnoseTabletCompProgress> &progress_list,
      ObDagId &dag_net_id,
      int64_t &start_time);
  int diagnose_all_dags();
  int get_compaction_dag_count(int64_t dag_count);
  void get_suggestion_reason(const int64_t priority, int64_t &reason);

  // 1. check ls compaction exist
  // 2. cancel ls compaction waiting dag
  int check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist);
  int check_dag_net_exist(
      const ObDagId &dag_id, bool &exist);
  int cancel_dag_net(const ObDagId &dag_id);
  int get_complement_data_dag_progress(const ObIDag *dag, int64_t &row_scanned, int64_t &row_inserted);
  int deal_with_finish_task(ObITask &task, ObTenantDagWorker &worker, int error_code);
  bool try_switch(ObTenantDagWorker &worker);
  int dispatch_task(ObITask &task, ObTenantDagWorker *&ret_worker, const int64_t priority);
  void finish_dag_net(ObIDagNet *dag_net);
  // for unittest
  int get_first_dag_net(ObIDagNet *&dag_net);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 1000; // 1s
  static const int64_t DAG_SIZE_LIMIT = 10 << 12;
  static const int64_t DEFAULT_MAX_DAG_NUM = 15000;
  static const int64_t DUMP_DAG_STATUS_INTERVAL = 10 * 1000LL * 1000LL; // 10s
  static const int64_t DEFAULT_CHECK_PERIOD = 3 * 1000 * 1000; // 3s
  static const int64_t LOOP_WAITING_DAG_LIST_INTERVAL = 5 * 1000 * 1000L; // 5s
  static const int64_t LOOP_RUNNING_DAG_NET_MAP_INTERVAL = 1 * 60 * 1000 * 1000L; // 1m
  static const int32_t MAX_SHOW_DAG_NET_CNT_PER_PRIO = 500;
  static const int64_t MANY_DAG_COUNT = 2000;
private:
  int schedule();
  void loop_dag_net();
  int loop_ready_dag_lists();
  int create_worker();
  int try_reclaim_threads();
  void destroy_all_workers();
  int set_thread_score(const int64_t priority, const int64_t concurrency);
  void inner_get_suggestion_reason(const ObDagType::ObDagTypeEnum type, int64_t &reason);
  void dump_dag_status(const bool force_dump = false);
  void diagnose_for_suggestion();
  bool is_dag_map_full();
  int gene_basic_info(
      ObDagSchedulerInfo *info_list,
      common::ObIArray<void *> &scheduler_infos,
      int64_t &idx);
  common::ObIAllocator &get_allocator(const bool is_ha);
  int init_allocator(const uint64_t tenant_id, const lib::ObLabel &label, lib::MemoryContext &mem_context);

private:
  bool is_inited_;
  bool fast_schedule_dag_net_;
  int tg_id_;
  int64_t dag_cnt_;              // atomic value
  int64_t dag_limit_;            // only set in init/destroy
  int64_t check_period_;         // only set in init/destroy
  int64_t loop_waiting_dag_list_period_;  // only set in init/destroy
  int64_t total_worker_cnt_; // lock by scheduler_sync_
  int64_t work_thread_num_; // lock by scheduler_sync_
  int64_t default_work_thread_num_; // only set in init/destroy
  int64_t total_running_task_cnt_;  // atomic value
  int64_t scheduled_task_cnt_; // atomic value // interval scheduled task count
  int64_t dag_cnts_[ObDagType::DAG_TYPE_MAX]; // just for showing // atomic value
  int64_t running_dag_cnts_[ObDagType::DAG_TYPE_MAX]; // atomic value
  int64_t added_dag_cnts_[ObDagType::DAG_TYPE_MAX]; // atomic value // interval add dag count
  int64_t scheduled_dag_cnts_[ObDagType::DAG_TYPE_MAX]; // atomic value // interval scheduled dag count
  int64_t scheduled_task_cnts_[ObDagType::DAG_TYPE_MAX]; // atomic value // interval scheduled task count
  int64_t scheduled_data_size_[ObDagType::DAG_TYPE_MAX]; // atomic value // interval scheduled data size
  common::ObThreadCond scheduler_sync_;  // Make sure the lock is inside if there are nested locks
  lib::MemoryContext mem_context_;
  lib::MemoryContext ha_mem_context_;
  ObDagPrioScheduler::WorkerList free_workers_; // free workers who have not been assigned to any task // locked by scheduler_sync_
  ObDagNetScheduler dag_net_sche_;
  ObDagPrioScheduler prio_sche_[ObDagPrio::DAG_PRIO_MAX];
};

// ATTENTION! when alloc task success, the task is already added into task_list_!!!
template <typename T>
int ObIDag::alloc_task(T *&task)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  task = NULL;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (NULL == (buf = allocator_->alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    task = NULL;
    COMMON_LOG(WARN, "failed to alloc task", K(ret));
  } else {
    T *ntask = new (buf) T();
    ntask->set_dag(*this);
    {
      lib::ObMutexGuard guard(lock_);
      if (!task_list_.add_last(ntask)) {
        ret = common::OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Failed to add task", K(task), K_(id));
      }
    }
    if (OB_SUCC(ret)) {
      task = ntask;
    }
  }
  return ret;
}

template <typename T, typename... Args>
int ObIDag::create_task(ObITask *parent, T *&task, Args&&... args)
{
  int ret = common::OB_SUCCESS;
  task = nullptr;
  if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->init(args...))) {
    STORAGE_LOG(WARN, "failed to init task", KR(ret));
  } else if (nullptr != parent && OB_FAIL(parent->add_child(*task))) {
    STORAGE_LOG(WARN, "failed to add child for parent", KR(ret), KPC(parent), KPC(task));
  } else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "fail to add task", KR(ret), KPC(task));
  }
  if (OB_FAIL(ret) && nullptr != task) {
    task->reset_node();
    remove_task(*task);
    task = nullptr;
  }
  return ret;
}

template <typename T>
int ObTenantDagScheduler::alloc_dag(T *&dag)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  dag = NULL;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "scheduler is not init", K(ret));
  } else if (DAG_SIZE_LIMIT < sizeof(T)){
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Dag Object is too large", K(ret), K(sizeof(T)));
  } else {
    T tmp_dag;
    ObIAllocator &allocator = get_allocator(tmp_dag.is_ha_dag());
    if (NULL == (buf = allocator.alloc(sizeof(T)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag", K(ret));
    } else {
      ObIDag *new_dag = new (buf) T();
      if (OB_FAIL(new_dag->basic_init(allocator))) {
        COMMON_LOG(WARN, "failed to init dag", K(ret));

        // failed to init, free dag
        inner_free_dag(*new_dag);
        new_dag = nullptr;
      } else {
        dag = static_cast<T*>(new_dag);
      }
    }
  }
  return ret;
}

template <typename  T>
int ObTenantDagScheduler::alloc_dag_with_priority(
    const ObDagPrio::ObDagPrioEnum &prio, T *&dag)
{
  int ret = OB_SUCCESS;
  dag = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (prio < ObDagPrio::DAG_PRIO_COMPACTION_HIGH
     || prio >= ObDagPrio::DAG_PRIO_MAX) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "get invalid arg", K(ret), K(prio));
  } else if (OB_FAIL(alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag should not be null", K(ret), KP(dag));
  } else {
    dag->set_priority(prio);
  }
  return ret;
}

template<typename T>
void ObTenantDagScheduler::free_dag_net(T *&dag_net)
{
  if (OB_NOT_NULL(dag_net)) {
    ObIAllocator &allocator = get_allocator(dag_net->is_ha_dag_net());
    dag_net->~T();
    allocator.free(dag_net);
    dag_net = nullptr;
  }
}

template <typename T>
int ObTenantDagScheduler::create_and_add_dag_net(const ObIDagInitParam *param)
{
  int ret = common::OB_SUCCESS;
  void *buf = nullptr;
  T *dag_net = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else {
    T tmp_dag_net;
    ObIAllocator &allocator = get_allocator(tmp_dag_net.is_ha_dag_net());
    if (NULL == (buf = allocator.alloc(sizeof(T)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag_net", K(ret));
    } else if (FALSE_IT(dag_net = new (buf) T())) {
    } else if (OB_FAIL(dag_net->init_by_param(param))) {
      COMMON_LOG(WARN, "failed to init dag_net", K(ret), KPC(dag_net));
    } else if (FALSE_IT(dag_net->basic_init(allocator))) {
    } else if (FALSE_IT(dag_net->init_dag_id())) {
    } else if (OB_FAIL(add_dag_net(dag_net))) {
      if (common::OB_HASH_EXIST == ret) {
        ret = common::OB_TASK_EXIST;
        COMMON_LOG(DEBUG, "dag_net is in blocking_dag_net_map now", K(ret), KPC(dag_net));
      } else {
        COMMON_LOG(WARN, "failed to add dag_net", K(ret), KPC(dag_net));
      }
    } else {
      COMMON_LOG(INFO, "success to create and add dag_net", K(ret), KP(dag_net));
      // Donot call notify(), may cause dead lock.
    }
  }
  if (OB_FAIL(ret)) {
    free_dag_net(dag_net); // free dag_net
  }

  return ret;
}

template<typename T>
int ObTenantDagScheduler::create_dag(
    const ObIDagInitParam *param,
    T *&dag)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag", K(ret));
  } else if (OB_FAIL(dag->init_by_param(param))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret), KPC(dag));
  } else if (OB_FAIL(dag->create_first_task())) {
    COMMON_LOG(WARN, "failed to create first task", K(ret), KPC(dag));
  }
  return ret;
}

template<typename T>
int ObTenantDagScheduler::create_and_add_dag(
    const ObIDagInitParam *param,
    const bool emergency/* = false*/,
    const bool check_size_overflow/* = true*/)
{
  int ret = common::OB_SUCCESS;
  T *dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDagScheduler is not inited", K(ret));
  } else if (OB_FAIL(create_dag(param, dag))) {
    COMMON_LOG(WARN, "failed to alloc dag", K(ret));
  } else if (OB_FAIL(add_dag(dag, emergency, check_size_overflow))) {
    if (common::OB_SIZE_OVERFLOW != ret && common::OB_EAGAIN != ret) {
      COMMON_LOG(WARN, "failed to add dag", K(ret), KPC(dag));
    }
  } else {
    ObThreadCondGuard guard(scheduler_sync_);
    scheduler_sync_.signal(); // wake up scheduler
  }
  if (OB_FAIL(ret) && nullptr != dag) {
    free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}

inline bool is_compaction_dag(ObDagType::ObDagTypeEnum dag_type)
{
  return ObDagType::DAG_TYPE_CO_MERGE_PREPARE == dag_type ||
         ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE == dag_type ||
         ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE == dag_type ||
         ObDagType::DAG_TYPE_CO_MERGE_FINISH == dag_type ||
         ObDagType::DAG_TYPE_MAJOR_MERGE == dag_type ||
         ObDagType::DAG_TYPE_MINI_MERGE == dag_type ||
         ObDagType::DAG_TYPE_MERGE_EXECUTE == dag_type ||
         ObDagType::DAG_TYPE_TX_TABLE_MERGE == dag_type ||
         ObDagType::DAG_TYPE_MDS_MINI_MERGE == dag_type ||
         ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS == dag_type;
}

inline bool is_ha_backfill_dag(const ObDagType::ObDagTypeEnum dag_type)
{
  return ObDagType::DAG_TYPE_TABLET_BACKFILL_TX == dag_type;
}

inline int dag_yield()
{
  int ret = OB_SUCCESS;
  ObTenantDagWorker *worker = ObTenantDagWorker::self();
  if (NULL != worker) {
    if (OB_FAIL(worker->yield()) && OB_CANCELED != ret) {
      COMMON_LOG(WARN, "Invalid return value for worker yield", K(ret));
    }
  }
  return ret;
}

inline bool is_reserve_mode()
{
  bool bret = false;
  ObTenantDagWorker *worker = ObTenantDagWorker::self();
  if (NULL != worker) {
    bret = worker->is_reserve_mode();
  }
  return bret;
}

#define SET_RESERVE_MODE()                                               \
  ({                                                                     \
    share::ObTenantDagWorker *worker = share::ObTenantDagWorker::self(); \
    worker->set_reserve_mode();                                          \
  })

#define SET_MEM_CTX(mem_ctx)                                             \
  ({                                                                     \
    share::ObTenantDagWorker *worker = share::ObTenantDagWorker::self(); \
    if (NULL != worker) {                                                \
      if (worker->hold_by_compaction_dag()) {                            \
        worker->set_mem_ctx(&mem_ctx);                                   \
      } else if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000L/*30s*/)) { \
        COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED,                          \
          "only compaction dag can set memctx", K(worker));              \
      }                                                                  \
    }                                                                    \
  })

#define CURRENT_MEM_CTX()                                                \
  ({                                                                     \
    compaction::ObCompactionMemoryContext *mem_ctx = nullptr;            \
    share::ObTenantDagWorker *worker = share::ObTenantDagWorker::self(); \
    if (NULL != worker) {                                                \
      if (worker->hold_by_compaction_dag()) {                            \
        mem_ctx = worker->get_mem_ctx();                                 \
      } else if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000L/*30s*/)) { \
        COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED,                          \
          "memctx only provided for compaction dag", K(worker));         \
      }                                                                  \
    }                                                                    \
    mem_ctx;                                                             \
  })


constexpr double operator "" _percentage(unsigned long long percentage)
{
  return (percentage + 0.0) / 100;
}

#define ADAPTIVE_PERCENT 40_percentage

} // namespace share
} // namespace oceanbase


#endif /* SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_ */
