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
#include "share/ob_table_range.h"
#include "common/errsim_module/ob_errsim_module_type.h"

namespace oceanbase
{
namespace compaction
{
struct ObTabletCompactionProgress;
struct ObDiagnoseTabletCompProgress;
class ObMergeDagHash;
struct ObTabletMergeDagParam;
struct ObIBasicInfoParam;
}
namespace share
{

struct ObDagWarningInfo;
typedef common::ObCurTraceId::TraceId ObDagId;
class ObIDag;
class ObIDagNet;
class ObTenantDagScheduler;
class ObTenantDagWorker;

class ObINodeWithChild
{
public:
  ObINodeWithChild()
      : indegree_(0)
  {}
  virtual ~ObINodeWithChild() { reset(); }

  void reset()
  {
    indegree_ = 0;
    children_.reset();
  }
  int64_t get_indegree() const { return indegree_; }
  void inc_indegree() { ATOMIC_INC(&indegree_); }
  int64_t dec_indegree() { return ATOMIC_SAF(&indegree_,1); }
  const common::ObIArray<ObINodeWithChild*> &get_child_nodes() const { return children_; }
  int dec_indegree_for_children();
  int deep_copy_children(const common::ObIArray<ObINodeWithChild*> &other);
  void reset_children();
  int erase_children(const ObINodeWithChild *child);
  int check_child_exist(
      const ObINodeWithChild *child,
      bool &is_exist);

  TO_STRING_KV(K_(indegree));

protected:
  virtual int add_child_node(ObINodeWithChild &child);

protected:
  static const int64_t DEFAULT_CHILDREN_NUM = 8;
  int64_t indegree_;
  common::ObSEArray<ObINodeWithChild*, DEFAULT_CHILDREN_NUM> children_;
};

class ObITask : public common::ObDLinkBase<ObITask>, public ObINodeWithChild
{
  static const int64_t DEFAULT_CHILDREN_NUM = 8;
  friend class ObIDag;
  friend class ObTenantDagScheduler;
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
    TASK_TYPE_MDS_TABLE_MERGE = 53,
    TASK_TYPE_TTL_DELETE = 54,
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
  ObIDag *get_dag() const { return dag_; }
  ObITaskStatus get_status() const { return status_; }
  ObITaskType get_type() const { return type_; }
  void set_max_retry_times(const uint32_t max_retry_times) { max_retry_times_ = max_retry_times; }
  OB_INLINE bool check_can_retry();
  VIRTUAL_TO_STRING_KV(KP(this), K_(type), K_(status), K_(dag));
private:
  virtual int generate_next_task(ObITask *&next_task) { UNUSED(next_task); return common::OB_ITER_END; }
  virtual int process() = 0;
private:
  void reset();
  int copy_dep_to(ObITask &other_task) const;
  void set_status(const ObITaskStatus status) { status_ = status; }
  void set_dag(ObIDag &dag) { dag_ = &dag; }
  void prepare_check_cycle();
  ObITaskColor get_color() { return color_; }
  void set_color(const ObITaskColor color) { color_ = color; }
  int64_t get_last_visit_child() { return last_visit_child_; }
  void set_last_visit_child(const int64_t idx) { last_visit_child_ = idx; }

  int generate_next_task();
protected:
  ObIDag *dag_;
private:
  ObITaskType type_;
  ObITaskStatus status_;
  int64_t last_visit_child_;
  ObITaskColor color_;
  uint32_t max_retry_times_;
  uint32_t running_times_;
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
  READY_DAG_LIST = 0,
  WAITING_DAG_LIST = 1,
  DAG_LIST_MAX
};

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
  static const int64_t MergeDagTypeCnt = 3;
  static const ObDagType::ObDagTypeEnum MergeDagType[];

  explicit ObIDag(const ObDagType::ObDagTypeEnum type);
  virtual ~ObIDag(); // DO NOT ACQUIRE LOCK OF DAG_SCHEDULER !!!!
  int add_task(ObITask &task);
  template<typename T>
  int alloc_task(T *&task);
  int get_dag_ret() const { return dag_ret_; };
  ObDagStatus get_dag_status() const { return dag_status_; }
  int64_t get_priority() const { return priority_; }
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

  virtual bool check_can_schedule()
  { // true: waiting_list -> ready_list OR ready_list -> start running
    // false: ready_list -> waiting_list
    return true;
  }
  int add_child(ObIDag &child);
  int update_status_in_dag_net();
  int finish(const ObDagStatus status);
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
  int64_t get_start_time() const { return start_time_; }
  int add_child_without_inheritance(ObIDag &child);
  int add_child_without_inheritance(const common::ObIArray<ObINodeWithChild*> &child_array);
  virtual int report_result()
  {
    // This func is only used for no need retry to report result.
    // Caller implements this function to complete certain functions
    return OB_SUCCESS;
  }

  virtual int generate_next_dag(ObIDag *&next_dag) { UNUSED(next_dag); return common::OB_ITER_END; }
  virtual int set_result(const int32_t result) { UNUSED(result); return common::OB_SUCCESS; }
  int fill_comment(char *buf, const int64_t buf_len);

  virtual bool is_ha_dag() const = 0;

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
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const = 0;
  virtual lib::Worker::CompatMode get_compat_mode() const = 0;
  virtual uint64_t get_consumer_group_id() const = 0;
  int remove_task(ObITask &task);

#ifdef ERRSIM
  virtual common::ObErrsimModuleType::TYPE get_module_type() { return ObErrsimModuleType::ERRSIM_MODULE_NONE; }
#endif

protected:
  int dag_ret_;
  int64_t add_time_;
  int64_t start_time_;
  uint64_t consumer_group_id_;

private:
  typedef common::ObDList<ObITask> TaskList;
  typedef lib::ObLockGuard<ObIDag> ObDagGuard;
  static const int64_t DEFAULT_TASK_NUM = 32;
  friend class ObTenantDagScheduler;
  friend class ObITask;
  friend class ObTenantDagWorker;
  friend ObDagGuard;
private:
  void free_task(ObITask &task);
  int finish_task(ObITask &task);
  bool is_valid();
  bool is_valid_type() const;
  int basic_init(ObIAllocator &allocator);
  void reset_task_running_status(ObITask &task, ObITask::ObITaskStatus task_status);
  void reset();
  void clear_task_list();
  void clear_running_info();
  int check_cycle();
  int get_next_ready_task(ObITask *&task);
  bool has_finished();
  void set_dag_ret(const int ret) { ATOMIC_VCAS(&dag_ret_, common::OB_SUCCESS, ret); }
  void set_dag_status(const ObDagStatus status) { dag_status_ = status; }
  bool is_dag_failed() const { return ObIDag::DAG_STATUS_NODE_FAILED == dag_status_; }
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
  lib::ObMutex lock_;
  TaskList task_list_; // should protect by lock
  bool is_stop_;
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
    dag_id_(),
    dag_status_()
  {}
  TO_STRING_KV(KP_(dag_ptr), "dag_type", ObIDag::get_dag_type_str(dag_type_), K_(dag_id),
      "dag_status", ObIDag::get_dag_status_str(dag_status_));
  ObIDag *dag_ptr_; // just for print, can't visit
  ObDagType::ObDagTypeEnum dag_type_;
  ObDagId dag_id_;
  ObIDag::ObDagStatus dag_status_;
};

class ObIDagNet : public common::ObDLinkBase<ObIDagNet>
{
public:
  static const int64_t DEFAULT_DAG_BUCKET = 1024;
  typedef common::ObDList<ObDagRecord> DagRecordList;
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
  bool check_finished_and_mark_stop();
  int update_dag_status(ObIDag &dag);
  int erase_dag_from_dag_net(ObIDag &dag);
  static const char *get_dag_net_type_str(enum ObDagNetType::ObDagNetTypeEnum type);

  virtual int init_by_param(const ObIDagInitParam *param)
  {
    UNUSED(param);
    return common::OB_SUCCESS;
  }
  void gene_dag_info(ObDagInfo &info, const char *list_info);
  int set_dag_id(const ObDagId &dag_id);
  const ObDagId &get_dag_id() { return dag_id_; }
  virtual int clear_dag_net_ctx()
  {
    return OB_SUCCESS;
  }
  void set_cancel();
  bool is_cancel();
  bool is_inited();
  virtual int deal_with_cancel()
  {
    return OB_SUCCESS;
  }
  virtual bool is_ha_dag_net() const = 0;
public:
  friend class ObTenantDagScheduler;

  virtual bool is_valid() const = 0;
  virtual int start_running() = 0;
  virtual bool operator == (const ObIDagNet &other) const = 0;
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const = 0;
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;

  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  void init_dag_id_();
  void remove_dag_record_(ObDagRecord &dag_record);

private:
  bool is_stopped_;
  lib::ObMutex lock_;
  common::ObIAllocator *allocator_; // use to alloc dag in dag_net later
  ObDagNetType::ObDagNetTypeEnum type_;
  int64_t add_time_;
  int64_t start_time_;
  DagRecordMap dag_record_map_;
  ObDagId dag_id_;
  bool is_cancel_;
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

template<typename T, int PRIOS>
class ObPriorityList
{
public:
  enum {PRIO_CNT = PRIOS};

  bool add_first(T *item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].add_first(item);
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool add_last(T *item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].add_last(item);
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool remove(T *item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      if (NULL != dlists_[prio].remove(item)) {
        bret = true;
      }
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  T *get_head(const int64_t prio)
  {
    T *result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].get_header();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T *get_first(const int64_t prio)
  {
    T *result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].get_first();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T *remove_first(const int64_t prio)
  {
    T *result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].remove_first();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T *remove_last(const int64_t prio)
  {
    T *result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].remove_last();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  bool is_empty(const int64_t prio) const
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].is_empty();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool is_empty() const
  {
    bool bret = true;
    for (int64_t i = 0; i < PRIO_CNT; ++i) {
      bret &= dlists_[i].is_empty();
    }
    return bret;
  }

  void reset()
  {
    for (int64_t i = 0; i < PRIO_CNT; ++i) {
      dlists_[i].reset();
    }
  }

  common::ObDList<T> &get_list(const int64_t prio)
  {
    return dlists_[prio];
  }

  int64_t size(const int64_t prio)
  {
    int64_t len = 0;
    if (prio >= 0 && prio < PRIO_CNT) {
      len = dlists_[prio].get_size();
    } else {
      COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return len;
  }
private:
  common::ObDList<T> dlists_[PRIOS];
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
  void yield();
  void set_task(ObITask *task) { task_ = task; }
  void set_function_type(const int64_t function_type) { function_type_ = function_type; }
  int set_dag_resource(const uint64_t group_id);
  bool need_wake_up() const;
  ObITask *get_task() const { return task_; }
  static ObTenantDagWorker *self() { return self_; }
  uint64_t get_group_id() { return group_id_; }
private:
  void notify(DagWorkerStatus status);
private:
  RLOCAL_STATIC(ObTenantDagWorker *, self_);
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

class ObTenantDagScheduler : public lib::TGRunnable
{
  friend class ObTenantDagWorker;
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
  int create_and_add_dag_net(const ObIDagInitParam *param);
  void free_dag(ObIDag &dag, ObIDag *parent_dag = nullptr);
  template<typename T>
  void free_dag_net(T *&dag_net);
  void run1() final;
  void notify();
  void reset();
  void destroy();
  void get_default_config();
  int64_t get_work_thread_num() const { return work_thread_num_; }
  bool is_empty() const
  {
    return dag_list_[READY_DAG_LIST].is_empty() && dag_list_[WAITING_DAG_LIST].is_empty()
        && 0 == dag_net_map_[RUNNING_DAG_NET_MAP].size();
  } // for unittest
  int64_t get_cur_dag_cnt() const { return dag_cnt_; }
  int sys_task_start(ObIDag *dag);
  int64_t get_dag_count(const ObDagType::ObDagTypeEnum type);
  int64_t get_running_task_cnt(const ObDagPrio::ObDagPrioEnum priority);
  int32_t get_up_limit(const int64_t prio, int64_t &up_limit);
  int check_dag_exist(const ObIDag *dag, bool &exist);
  int cancel_dag(const ObIDag *dag, ObIDag *parent_dag = nullptr);
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

  // 1. check ls compaction exist
  // 2. cancel ls compaction waiting dag
  int check_ls_compaction_dag_exist_with_cancel(const ObLSID &ls_id, bool &exist);
  int check_dag_net_exist(
      const ObDagId &dag_id, bool &exist);
  int cancel_dag_net(const ObDagId &dag_id);
  int get_complement_data_dag_progress(const ObIDag *dag, int64_t &row_scanned, int64_t &row_inserted);
  // for unittest
  int get_first_dag_net(ObIDagNet *&dag_net);

private:
  typedef common::ObDList<ObIDag> DagList;
  typedef common::ObDList<ObIDagNet> DagNetList;
  typedef ObPriorityList<ObIDag, ObDagPrio::DAG_PRIO_MAX> PriorityDagList;
  typedef ObPriorityList<ObTenantDagWorker, ObDagPrio::DAG_PRIO_MAX> PriorityWorkerList;
  typedef common::ObDList<ObTenantDagWorker> WorkerList;
  typedef common::hash::ObHashMap<const ObIDag *,
                          ObIDag *,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<const ObIDag *>,
                          common::hash::equal_to<const ObIDag *> > DagMap;
  typedef common::hash::ObHashMap<const ObIDagNet*,
                          ObIDagNet*,
                          common::hash::NoPthreadDefendMode,
                          common::hash::hash_func<const ObIDagNet*>,
                          common::hash::equal_to<const ObIDagNet*> > DagNetMap;
  typedef common::hash::ObHashMap<ObDagId, const ObIDagNet *>DagNetIdMap;

  static const int64_t TMP_WEIGHT = 5;
  static const int64_t SCHEDULER_WAIT_TIME_MS = 1000; // 1s
  static const int64_t DAG_SIZE_LIMIT = 10 << 12;
  static const int64_t DEFAULT_MAX_DAG_NUM = 15000;
  static const int64_t DEFAULT_MAX_DAG_MAP_CNT = 150000;
  static const int64_t DEFAULT_MAX_RUNNING_DAG_NET_CNT = 30000;
  static const int64_t DEFAULT_MAX_DAG_NET_CNT = 500000;
  static const int64_t STOP_ADD_DAG_PERCENT = 70;
  static const int64_t DUMP_DAG_STATUS_INTERVAL = 10 * 1000LL * 1000LL;
  static const int64_t DEFAULT_CHECK_PERIOD = 3 * 1000 * 1000; // 3s
  static const int64_t LOOP_WAITING_DAG_LIST_INTERVAL = 5 * 1000 * 1000L; // 5s
  static const int64_t LOOP_BLOCKING_DAG_NET_LIST_INTERVAL = 5 * 1000 * 1000L; // 5s
  static const int64_t LOOP_RUNNING_DAG_NET_MAP_INTERVAL = 5 * 60 * 1000 * 1000L; // 5m
  static const int64_t PRINT_SLOW_DAG_NET_THREASHOLD = 30 * 60 * 1000 * 1000L; // 30m
  static const int64_t LOOP_PRINT_LOG_INTERVAL = 30 * 1000 * 1000L; // 30s
  static const int32_t MAX_SHOW_DAG_CNT_PER_PRIO = 100;
  static const int32_t MAX_SHOW_DAG_NET_CNT_PER_PRIO = 500;
private:
  enum DagNetMapIndex
  {
    RUNNING_DAG_NET_MAP = 0,
    DAG_NET_MAP_MAX
  };

  int schedule();
  void loop_dag_net();
  int erase_dag_(ObIDag &dag);
  int pop_task_from_ready_list(const int64_t priority, ObITask *&task);
  int loop_ready_dag_lists();
  int loop_waiting_dag_list(const int64_t priority);
  int loop_blocking_dag_net_list();
  int loop_running_dag_net_map();
  int finish_task_in_dag(ObITask &task, ObIDag &dag, ObIDagNet *&erase_dag_net);
  int finish_dag_(const ObIDag::ObDagStatus status, ObIDag &dag, ObIDagNet *&erase_dag_net, const bool try_move_child);
  int finish_dag_net(ObIDagNet *dag_net);
  int deal_with_finish_task(ObITask &task, ObTenantDagWorker &worker, int error_code);
  int deal_with_fail_task(ObITask &task, ObIDag &dag, const int error_code, bool &retry_flag);
  int deal_with_fail_dag(ObIDag &dag, bool &retry_flag);
  int create_worker();
  int try_reclaim_threads();
  int schedule_one(const int64_t priority);
  int dispatch_task(ObITask &task, ObTenantDagWorker *&ret_worker, const int64_t priority);
  void destroy_all_workers();
  int set_thread_score(const int64_t priority, const int64_t concurrency);
  bool try_switch(ObTenantDagWorker &worker);
  int try_switch(ObTenantDagWorker &worker, const int64_t src_prio, const int64_t dest_prio, bool &need_pause);
  void pause_worker(ObTenantDagWorker &worker, const int64_t priority);
  void dump_dag_status(const bool force_dump = false);
  int check_need_load_shedding(const int64_t priority, const bool for_schedule, bool &need_shedding);
  void update_work_thread_num();
  int move_dag_to_list_(
      ObIDag *dag,
      ObDagListIndex from_list_index,
      ObDagListIndex to_list_index);
  bool is_waiting_dag_type(ObDagType::ObDagTypeEnum dag_type)
  { // will add into waiting dag list in add_dag() func
    return false;
  }
  int add_dag_into_list_and_map_(
      const ObDagListIndex list_index,
      ObIDag *dag,
      const bool emergency);
  bool is_dag_map_full();
  int gene_basic_info(
      ObDagSchedulerInfo *info_list,
      common::ObIArray<void *> &scheduler_infos,
      int64_t &idx);
  int inner_add_dag(
      const bool emergency,
      const bool check_size_overflow,
      ObIDag *&dag);
  int generate_next_dag_(ObIDag *dag);
  int try_move_child_to_ready_list(ObIDag &dag);
  void inner_free_dag(ObIDag &dag);
  OB_INLINE int64_t get_dag_limit(const ObDagPrio::ObDagPrioEnum dag_prio);
  common::ObIAllocator &get_allocator(const bool is_ha);
  int init_allocator(const uint64_t tenant_id, const lib::ObLabel &label, lib::MemoryContext &mem_context);

private:
  bool is_inited_;
  DagMap dag_map_;
  DagNetMap dag_net_map_[DAG_NET_MAP_MAX];
  DagNetList blocking_dag_net_list_;
  PriorityDagList dag_list_[DAG_LIST_MAX];
  common::ObThreadCond scheduler_sync_;
  lib::ObMutex dag_net_map_lock_;
  int64_t dag_cnt_;
  int64_t dag_limit_;
  int64_t compaction_dag_limit_;
  int64_t check_period_;
  int64_t loop_waiting_dag_list_period_;
  int64_t total_worker_cnt_;
  int64_t work_thread_num_;
  int64_t default_work_thread_num_;
  int64_t total_running_task_cnt_;
  int64_t scheduled_task_cnt_; // interval scheduled task count
  int64_t running_task_cnts_[ObDagPrio::DAG_PRIO_MAX];
  int64_t low_limits_[ObDagPrio::DAG_PRIO_MAX]; // wait to delete
  int64_t up_limits_[ObDagPrio::DAG_PRIO_MAX]; // wait to delete
  int64_t scheduled_task_cnts_[ObDagType::DAG_TYPE_MAX]; // interval scheduled dag count
  int64_t dag_cnts_[ObDagType::DAG_TYPE_MAX];
  int64_t dag_net_cnts_[ObDagNetType::DAG_NET_TYPE_MAX];
  lib::MemoryContext mem_context_;
  lib::MemoryContext ha_mem_context_;
  PriorityWorkerList waiting_workers_; // workers waiting for time slice to run
  PriorityWorkerList running_workers_; // running workers
  WorkerList free_workers_; // free workers who have not been assigned to any task
  DagNetIdMap dag_net_id_map_; // for HA to search dag_net of specified dag_id
  int tg_id_;
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
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
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
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "scheduler is not init", K(ret));
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
    } else if (FALSE_IT(dag_net->init_dag_id_())) {
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
  if (OB_FAIL(create_dag(param, dag))) {
    COMMON_LOG(WARN, "failed to alloc dag", K(ret));
  } else if (OB_FAIL(add_dag(dag, emergency, check_size_overflow))) {
    if (common::OB_SIZE_OVERFLOW != ret && common::OB_EAGAIN != ret) {
      COMMON_LOG(WARN, "failed to add dag", K(ret), KPC(dag));
    }
  } else {
    COMMON_LOG(INFO, "success to create and add dag", K(ret), KP(dag));
    scheduler_sync_.signal(); // wake up scheduler
  }
  if (OB_FAIL(ret) && nullptr != dag) {
    free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}

inline void dag_yield()
{
  ObTenantDagWorker *worker = ObTenantDagWorker::self();
  if (NULL != worker) {
    worker->yield();
  }
}

} // namespace share
} // namespace oceanbase


#endif /* SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_ */
