/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_PLUGIN_VECTOR_INDEX_SCHEDULER_DEFINE_H_
#define OCEANBASE_OBSERVER_OB_PLUGIN_VECTOR_INDEX_SCHEDULER_DEFINE_H_
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "logservice/ob_append_callback.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"

namespace oceanbase
{
namespace share
{

class ObPluginVectorIndexService;
class ObPluginVectorIndexMgr;
class ObPluginVectorIndexLoadScheduler;

static const int64_t VECTOR_INDEX_TABLET_ID_COUNT = 100;
typedef ObSEArray<ObTabletID, VECTOR_INDEX_TABLET_ID_COUNT> ObVectorIndexTabletIDArray;
typedef ObSEArray<uint64_t, VECTOR_INDEX_TABLET_ID_COUNT> ObVectorIndexTableIDArray;
class ObVectorIndexSyncLog
{
public:
  OB_UNIS_VERSION(1);

public:
  ObVectorIndexSyncLog(ObVectorIndexTabletIDArray &array, ObVectorIndexTableIDArray &table_id_array)
    : flags_(0),
      tablet_id_array_(array),
      table_id_array_(table_id_array)
  {}
  TO_STRING_KV(K_(flags), K_(tablet_id_array), K_(table_id_array));
  ObVectorIndexTabletIDArray &get_tablet_id_array() { return tablet_id_array_; }
  ObVectorIndexTableIDArray &get_table_id_array() { return table_id_array_; }

private:
  uint32_t flags_;
  ObVectorIndexTabletIDArray &tablet_id_array_;
  ObVectorIndexTableIDArray &table_id_array_;
};

class ObVectorIndexSyncLogCb : public logservice::AppendCb
{
public:
  ObVectorIndexSyncLogCb()
    : scheduler_(nullptr),
      log_buffer_(nullptr)
  {
    reset();
  }

  ~ObVectorIndexSyncLogCb() {
    destory();
  }

  void reset()
  {
    ATOMIC_SET(&is_callback_invoked_, false);
    ATOMIC_SET(&is_success_, false);
  }

  void destory()
  {
    if (OB_NOT_NULL(log_buffer_)) {
      ob_free(log_buffer_);
      log_buffer_ = nullptr;
    }
  }

  int on_success();

  int on_failure();

  TO_STRING_KV(K_(is_callback_invoked), K_(is_success), KP_(log_buffer));
  OB_INLINE bool is_invoked() const { return ATOMIC_LOAD(&is_callback_invoked_); }
  OB_INLINE bool is_success() const { return ATOMIC_LOAD(&is_success_); }

public:
  static const uint32 VECTOR_INDEX_SYNC_LOG_MAX_LENGTH = 16 * 1024; // Max 16KB each log
  static const uint32_t VECTOR_INDEX_MAX_SYNC_COUNT = 512;
  ObPluginVectorIndexLoadScheduler *scheduler_;
  char *log_buffer_;

private:
  bool is_callback_invoked_;
  bool is_success_;

};

typedef common::ObTTLTaskStatus ObVectorIndexTaskStatus;
typedef common::ObTTLStatus ObVectorIndexTenantStatus;

// task context of a tenant
class ObPluginVectorIndexTenantTaskCtx
{
public:
  ObPluginVectorIndexTenantTaskCtx()
    : need_check_(false),
      is_dirty_(false),
      state_(common::ObTTLTaskStatus::OB_TTL_TASK_INVALID)
  {}

  virtual ~ObPluginVectorIndexTenantTaskCtx() {}
  void reuse()
  {
    need_check_ = false;
    is_dirty_ = false;
    state_ = common::ObTTLTaskStatus::OB_TTL_TASK_FINISH;
  }

  TO_STRING_KV(K_(need_check), K_(is_dirty), K_(state));

public:
  bool need_check_;
  bool is_dirty_;
  ObVectorIndexTaskStatus state_;
};

// task context of a ls
struct ObPluginVectorIndexLSTaskCtx
{
  void reuse()
  {
    need_check_ = false;
    all_finished_ = false;
    state_ = common::ObTTLTaskStatus::OB_TTL_TASK_FINISH;
  }

  TO_STRING_KV(K_(task_id), K_(need_check), K_(all_finished), K_(state));
  static const uint32 NON_MEMDATA_TASK_CYCLE_MAX = 90; // check force memdata sync every 15 mins
  uint32 non_memdata_task_cycle_;
  bool need_memdata_sync_;
  int64_t task_id_;
  bool need_check_;
  bool all_finished_;
  ObVectorIndexTaskStatus state_;
};

// memdata sync task ctx
struct ObPluginVectorIndexTaskCtx
{
  ObPluginVectorIndexTaskCtx(ObTabletID &index_tablet_id, uint64_t index_table_id)
    : index_tablet_id_(index_tablet_id),
      index_table_id_(index_table_id),
      task_start_time_(0),
      last_modify_time_(0),
      failure_times_(0),
      err_code_(OB_SUCCESS),
      in_queue_(false),
      task_status_(ObVectorIndexTaskStatus::OB_TTL_TASK_PREPARE)
  {}
  TO_STRING_KV(K_(index_table_id), K_(index_tablet_id), K_(task_start_time), K_(last_modify_time),
               K_(failure_times), K_(err_code), K_(in_queue), K_(task_status));
  ObTabletID index_tablet_id_;
  uint64_t index_table_id_;
  int64_t task_start_time_;
  int64_t last_modify_time_;
  int64_t failure_times_;
  int64_t err_code_;
  bool in_queue_; // whether in dag queue or not
  ObVectorIndexTaskStatus task_status_;
  common::ObSpinLock lock_; // lock for update task_status_
};
typedef hash::ObHashMap<ObTabletID, ObVectorIndexSharedTableInfo> ObVecIdxSharedTableInfoMap;

// schedule vector tasks for a ls
class ObPluginVectorIndexLoadScheduler : public common::ObTimerTask,
                                         public logservice::ObIReplaySubHandler,
                                         public logservice::ObICheckpointSubHandler,
                                         public logservice::ObIRoleChangeSubHandler
{
public:
  ObPluginVectorIndexLoadScheduler()
    : is_inited_(false),
      is_leader_(false),
      need_do_for_switch_(false),
      is_stopped_(false),
      is_logging_(false),
      need_refresh_(true),
      tenant_id_(OB_INVALID_TENANT_ID),
      ttl_tablet_timer_tg_id_(0),
      interval_factor_(1),
      basic_period_(VEC_INDEX_SCHEDULAR_BASIC_PERIOD),
      current_memory_config_(0),
      dag_ref_cnt_(0),
      vector_index_service_(nullptr),
      ls_(nullptr),
      local_schema_version_(OB_INVALID_VERSION),
      local_tenant_task_(),
      cb_()
  {}
  virtual ~ObPluginVectorIndexLoadScheduler()
  {
  }

  int init(uint64_t tenant_id, ObLS *ls, int ttl_tablet_timer_tg_id_);
  virtual void runTimerTask() override;
  void run_task();
  bool is_inited() { return is_inited_; }

  ObPluginVectorIndexService *get_vector_index_service() { return vector_index_service_; }

  bool check_can_do_work();
  int check_tenant_memory();
  int check_schema_version();
  void mark_tenant_need_check();
  void mark_tenant_checked();
  int reload_tenant_task();
  int check_and_execute_tasks(); // was check_and_handle_event
  int check_and_execute_adapter_maintenance_task(ObPluginVectorIndexMgr *&mgr);
  int check_and_execute_memdata_sync_task(ObPluginVectorIndexMgr *mgr);
  int sync_all_dirty_task(ObIArray<ObTabletID>& dirty_tasks);
  int generate_batch_tablet_task();

  // core interfaces
  int execute_adapter_maintenance();
  int acquire_adapter_in_maintenance(const int64_t table_id, const ObTableSchema *table_schema);
  int set_shared_table_info_in_maintenance(const int64_t table_id,
                                           const ObTableSchema *table_schema,
                                           ObVecIdxSharedTableInfoMap &shared_table_info_map);
  int check_task_state(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *task_ctx, bool &is_stop);
  int check_is_vector_index_table(const ObTableSchema &table_schema,
                                  bool &is_vector_index_table,
                                  bool &is_shared_index_table);
  void clean_deprecated_adapters();
  int check_index_adpter_exist(ObPluginVectorIndexMgr *mgr);

  int log_tablets_need_memdata_sync(ObPluginVectorIndexMgr *mgr);
  int execute_all_memdata_sync_task(ObPluginVectorIndexMgr *mgr);
  int execute_one_memdata_sync_task(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *ctx);
  int check_ls_task_state(ObPluginVectorIndexMgr *mgr);

  // task generation interfaces
  bool can_schedule_tenant(const ObPluginVectorIndexMgr *mgr);
  bool can_schedule_task(const ObPluginVectorIndexTaskCtx *task_ctx);
  int try_schedule_task(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *task_ctx);
  int try_schedule_remaining_tasks(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *current_ctx);
  int generate_vec_idx_memdata_dag(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *task_ctx);

  // logger interfaces
  int handle_submit_callback(const bool success);
  int handle_replay_result(ObVectorIndexSyncLog &ls_log);
  int replay(const void *buffer, const int64_t buf_size, const palf::LSN &lsn, const share::SCN &log_scn);

  // checkpoint interfaces
  int flush(share::SCN &scn);
  share::SCN get_rec_scn();

  // role change interfaces
  int switch_to_follower_gracefully();
  void switch_to_follower_forcedly();
  int resume_leader() { return OB_SUCCESS; }
  int switch_to_leader();

  // task save destory
  void stop() { is_stopped_= true; };
  bool is_stopped() { return (is_stopped_ == true); };
  void inc_dag_ref() { ATOMIC_INC(&dag_ref_cnt_); }
  void dec_dag_ref() { ATOMIC_DEC(&dag_ref_cnt_); }
  int64_t get_dag_ref() const { return ATOMIC_LOAD(&dag_ref_cnt_); }

  int safe_to_destroy(bool &is_safe);

  TO_STRING_KV(K_(is_inited), K_(is_leader), K_(need_do_for_switch), K_(is_stopped), K_(is_logging),
               K_(need_refresh), K_(tenant_id), K_(ttl_tablet_timer_tg_id), K_(interval_factor),
               K_(basic_period), K_(current_memory_config), K_(dag_ref_cnt),
               KP_(vector_index_service), KP_(ls),
               K_(local_schema_version), K_(local_tenant_task));

private:
  int submit_log_();
  void inner_switch_to_follower_();

private:

  static const int64_t VEC_INDEX_SCHEDULAR_BASIC_PERIOD = 10 * 1000 * 1000; // 10s
  static const int64_t VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD = 30 * 1000 * 1000; // 30s
  static const int64_t DEFAULT_TABLE_ARRAY_SIZE = 200;
  static const int64_t TBALE_GENERATE_BATCH_SIZE = 200;

  // 1. is_leader_: Only leader is allowed to generate memdata sync logs,
  //   but execute of memdata sync task is allowed on leader/follower
  // 2. need_do_for_switch_ is intended to skip some loops currently being executed,
  //   but in the context of vector indexing, only when leader to follwer need processing currently,
  //   which duplicates the function of is_leader_.
  // 3. is_stopped_ is set only when the timer is stopped, stop to schedule memedata sync tasks

  bool is_inited_;
  bool is_leader_;
  bool need_do_for_switch_;
  bool is_stopped_;

  bool is_logging_;
  bool need_refresh_;
  common::ObSpinLock logging_lock_;
  uint64_t tenant_id_;
  int ttl_tablet_timer_tg_id_;
  int interval_factor_;
  int64_t basic_period_;
  int64_t current_memory_config_;
  volatile int64_t dag_ref_cnt_;
  ObPluginVectorIndexService *vector_index_service_;
  ObLS *ls_;
  int64_t local_schema_version_;
  ObPluginVectorIndexTenantTaskCtx local_tenant_task_;
  ObVectorIndexSyncLogCb cb_;
  ObVectorIndexTabletIDArray tablet_id_array_;
  ObVectorIndexTableIDArray table_id_array_;
};

class ObVectorIndexTask : public share::ObITask
{
public:
  ObVectorIndexTask()
    : ObITask(ObITaskType::TASK_TYPE_VECTOR_INDEX_MEMDATA_SYNC),
      is_inited_(false),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      vec_idx_scheduler_(nullptr),
      vec_idx_mgr_(nullptr),
      task_ctx_(nullptr),
      read_snapshot_(),
      allocator_(ObMemAttr(MTL_ID(), "VecIdxTaskCtx"))
  {}
  ~ObVectorIndexTask() {};
  int init(ObPluginVectorIndexLoadScheduler *schedular,
           ObPluginVectorIndexMgr *mgr,
           ObPluginVectorIndexTaskCtx *task_ctx);
  common::ObIAllocator &get_allocator() { return allocator_; }
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(read_snapshot), KPC_(task_ctx));
private:
  int process_one();

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObPluginVectorIndexLoadScheduler *vec_idx_scheduler_;
  ObPluginVectorIndexMgr *vec_idx_mgr_;
  ObPluginVectorIndexTaskCtx *task_ctx_;
  SCN read_snapshot_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexTask);
};

class ObVectorIndexTaskParam final
{
public:
  ObVectorIndexTaskParam()
    : tenant_id_(OB_INVALID_ID),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      table_id_(OB_INVALID_ID),
      tablet_id_(common::OB_INVALID_ID),
      task_ctx_(nullptr)
  {}
  ~ObVectorIndexTaskParam() {}
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_ID
           && ls_id_.is_valid()
           && table_id_ != OB_INVALID_ID
           && tablet_id_.is_valid();
  }
  bool operator==(const ObVectorIndexTaskParam& param) const
  {
    return tenant_id_ == param.tenant_id_
           && ls_id_ == param.ls_id_
           && table_id_ == param.table_id_
           && tablet_id_ == param.tablet_id_;
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), KP_(task_ctx));
public:
  int64_t tenant_id_;
  share::ObLSID ls_id_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  ObPluginVectorIndexTaskCtx *task_ctx_;
};

class ObVectorIndexDag final: public share::ObIDag
{
public:
  ObVectorIndexDag()
    : ObIDag(ObDagType::DAG_TYPE_VECTOR_INDEX), is_inited_(false),
      param_(),
      compat_mode_(lib::Worker::CompatMode::INVALID)
  {}
  virtual ~ObVectorIndexDag() {}
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  int init(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *task_ctx);
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const { return false; }
private:
  bool is_inited_;
  ObVectorIndexTaskParam param_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexDag);
};

typedef common::hash::ObHashMap<common::ObTabletID, ObPluginVectorIndexTaskCtx*> VectorIndexMemSyncMap;
typedef common::hash::ObHashMap<common::ObTabletID, ObPluginVectorIndexAdaptor*> VectorIndexAdaptorMap;
class ObVectorIndexMemSyncInfo
{
public:
  ObVectorIndexMemSyncInfo(uint64_t tenant_id) :
    processing_first_mem_sync_(true),
    first_mem_sync_map_(),
    second_mem_sync_map_(),
    first_task_allocator_(ObMemAttr(tenant_id, "VecIdxTask")),
    second_task_allocator_(ObMemAttr(tenant_id, "VecIdxTask"))
  {}

  ~ObVectorIndexMemSyncInfo(){}

  int init(int64_t hash_capacity, uint64_t tenant_id, ObLSID &ls_id);
  void destroy();

  int add_task_to_waiting_map(ObVectorIndexSyncLog &ls_log);
  int add_task_to_waiting_map(VectorIndexAdaptorMap &adapter_map);
  int count_processing_finished(bool &is_finished,
                                uint32_t &total_count,
                                uint32_t &finished_count);
  void check_and_switch_if_needed(bool &need_sync, bool &all_finished);
  VectorIndexMemSyncMap &get_processing_map() { return processing_first_mem_sync_ ? first_mem_sync_map_ : second_mem_sync_map_; }

private:
  VectorIndexMemSyncMap &get_waiting_map() { return processing_first_mem_sync_ ? second_mem_sync_map_ : first_mem_sync_map_; }
  ObIAllocator &get_processing_allocator() { return processing_first_mem_sync_ ? first_task_allocator_ : second_task_allocator_; }
  ObIAllocator &get_waiting_allocator() { return processing_first_mem_sync_ ? second_task_allocator_ : first_task_allocator_; }
  void switch_processing_map();

  TO_STRING_KV(K_(processing_first_mem_sync), K(first_mem_sync_map_.size()), K(second_mem_sync_map_.size()));

private:
  // pingpong map/allocator for follower receive memdata sync task from log
  bool processing_first_mem_sync_;
  common::ObSpinLock switch_lock_;
  VectorIndexMemSyncMap first_mem_sync_map_;
  VectorIndexMemSyncMap second_mem_sync_map_;
  ObArenaAllocator first_task_allocator_;
  ObArenaAllocator second_task_allocator_;
};


} // namespace share
} // namespace oceanbase
#endif