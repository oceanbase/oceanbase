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

#ifndef OCEANBASE_OBSERVER_TABLE_OB_TENANT_TABLET_TTL_MANAGER_H_
#define OCEANBASE_OBSERVER_TABLE_OB_TENANT_TABLET_TTL_MANAGER_H_

#include "lib/task/ob_timer.h"
#include "share/table/ob_ttl_util.h"
#include "share/table/ob_table_ttl_common.h"
#include "share/tablet/ob_tablet_info.h"
#include "observer/table/ob_htable_rowkey_mgr.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "lib/container/ob_rbtree.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"

namespace oceanbase
{
namespace table
{

struct ObTTLTaskCtx
{
public :
  ObTTLTaskCtx() : rowkey_cp_allcoator_(ObMemAttr(MTL_ID(), "TTLTaskCtx")),
                   task_info_(),
                   task_status_(common::ObTTLTaskStatus::OB_TTL_TASK_INVALID),
                   ttl_para_(),
                   task_start_time_(OB_INVALID_ID),
                   last_modify_time_(OB_INVALID_ID),
                   failure_times_(0),
                   is_dirty_(false),
                   need_refresh_(true),
                   in_queue_(false) {}
  bool is_valid()
  {
    return task_info_.is_valid() && ttl_para_.is_valid();
  }

  int deep_copy_rowkey(const ObString &rowkey);

  TO_STRING_KV(K_(task_info), K_(task_status), K_(ttl_para), K_(task_start_time), K_(last_modify_time),
               K_(failure_times), K_(is_dirty), K_(need_refresh), K_(in_queue));

public:
  common::ObArenaAllocator  rowkey_cp_allcoator_; // for rowkey copy in ObTTLTaskInfo
  ObTTLTaskInfo    task_info_;
  common::ObTTLTaskStatus task_status_;
  table::ObTTLTaskParam   ttl_para_;
  int64_t                 task_start_time_;
  int64_t                 last_modify_time_;
  int64_t                 failure_times_;

  int64_t                 rsp_time_;
  bool                    is_dirty_; // should sync sys table for tasks
  bool                    is_moved_;
  bool                    need_refresh_; // should refresh task from task table
  common::ObSpinLock      lock_; // lock for update
  bool                    in_queue_; // whether in dag queue or not
};

class ObTabletTTLScheduler;
class OBTTLTimerPeriodicTask : public common::ObTimerTask {
public:
  OBTTLTimerPeriodicTask(ObTabletTTLScheduler &tablet_ttl_mgr)
  : tablet_ttl_mgr_(tablet_ttl_mgr)
  {
    disable_timeout_check();
  }
  virtual ~OBTTLTimerPeriodicTask() {}
  virtual void runTimerTask() override;
private:
  static const int64_t TTL_TIME_TASKER_THRESHOLD = 30 * 1000 * 1000; // 30s
  ObTabletTTLScheduler &tablet_ttl_mgr_;
};

class ObTabletTTLScheduler
{
public:
  friend ObTTLTaskCtx;
  ObTabletTTLScheduler()
  : is_inited_(false),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    schema_service_(NULL),
    sql_proxy_(NULL),
    is_timer_start_(false),
    periodic_delay_(TTL_PERIODIC_DELAY),
    periodic_task_(*this),
    ls_(nullptr),
    tg_id_(-1),
    local_schema_version_(OB_INVALID_VERSION),
    has_start_(false),
    is_leader_(true),
    dag_ref_cnt_(0),
    need_do_for_switch_(true)
  {
  }

  virtual ~ObTabletTTLScheduler()
  {
    destroy();
  }

  int init(storage::ObLS *ls);

  int init(const uint64_t tenant_id);

  // switch leader
  int switch_to_leader();
  int resume_leader() { return switch_to_leader(); }

  int switch_to_follower_gracefully();
  void switch_to_follower_forcedly();
  void inner_switch_to_follower();

  int start();
  void wait();
  void stop();
  void destroy();

  int reload_tenant_task();
  void on_schema_changed(uint64_t schema_changed_tenant_id);

  // timer handle function
  virtual int check_and_generate_tablet_tasks();
  int check_and_handle_event();
  int check_tenant_memory();
  int check_inner_stat();
  void inc_dag_ref() { ATOMIC_INC(&dag_ref_cnt_); }
  void dec_dag_ref() { ATOMIC_DEC(&dag_ref_cnt_); }
  int64_t get_dag_ref() const { return ATOMIC_LOAD(&dag_ref_cnt_); }
  int safe_to_destroy(bool &is_safe);
  int sync_all_dirty_task(common::ObIArray<ObTabletID>& dirty_tasks);
  void run_task();

  TO_STRING_KV(K_(tenant_id),
               K_(is_inited),
               K_(is_timer_start),
               K_(periodic_delay),
               K_(tg_id),
               K_(local_schema_version),
               K_(has_start),
               K_(is_leader),
               K_(dag_ref_cnt),
               K_(need_do_for_switch));

public:
  virtual int try_schedule_task(ObTTLTaskCtx* ctx);
  virtual int report_task_status(ObTTLTaskInfo& task_info,
                                 table::ObTTLTaskParam& task_para,
                                 bool& is_stop,
                                 bool need_copy_task = true);
  virtual int64_t get_tenant_task_table_id() { return common::ObTTLUtil::TTL_TENNAT_TASK_TABLE_ID; }
  virtual int64_t get_tenant_task_tablet_id() { return common::ObTTLUtil::TTL_TENNAT_TASK_TABLET_ID; }

  virtual int do_after_leader_switch() { return OB_SUCCESS; }

  virtual int check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table)
  {
    return ObTTLUtil::check_is_ttl_table(table_schema, is_ttl_table);
  }
private:
  typedef common::hash::ObHashMap<ObTabletID, ObTTLTaskCtx*> TabletTaskMap;
  typedef TabletTaskMap::iterator tablet_task_iter;

  struct ObTTLTenantInfo
  {
  public:
    ObTTLTenantInfo() : tablet_task_map_(),
                        allocator_(ObMemAttr(MTL_ID(), "TTLTenantInfo")),
                        tenant_id_(OB_INVALID_ID),
                        task_id_(OB_INVALID_ID),
                        is_usr_trigger_(false),
                        need_check_(false),
                        is_dirty_(false),
                        ttl_continue_(true),
                        cmd_type_(obrpc::ObTTLRequestArg::TTL_INVALID_TYPE),
                        rsp_time_(OB_INVALID_ID),
                        state_(common::ObTTLTaskStatus::OB_TTL_TASK_INVALID),
                        is_reused_(false),
                        task_type_(ObTTLType::NORMAL)
    {}
    ~ObTTLTenantInfo()
    {
      destory();
    }
    void destory()
    {
      for (TabletTaskMap::const_iterator iter = tablet_task_map_.begin(); iter != tablet_task_map_.end();
        ++iter) {
        ObTTLTaskCtx *ctx = iter->second;
        if (OB_NOT_NULL(ctx)) {
          ctx->~ObTTLTaskCtx();
        }
      }
      tablet_task_map_.destroy();
      allocator_.reset();
    }
    void reuse();
    TO_STRING_KV(K_(tenant_id),
                 K_(task_id),
                 K_(is_usr_trigger),
                 K_(need_check),
                 K_(is_dirty),
                 K_(ttl_continue),
                 K_(rsp_time),
                 K_(state),
                 K_(is_reused),
                 K_(task_type));

  public:
      TabletTaskMap                     tablet_task_map_;
      common::ObArenaAllocator          allocator_;
      uint64_t                          tenant_id_;
      int64_t                           task_id_;
      bool                              is_usr_trigger_;
      bool                              need_check_; /*need scan partition & check*/
      bool                              is_dirty_;  /*need check the current ctx task*/
      bool                              ttl_continue_;
      obrpc::ObTTLRequestArg::TTLRequestType         cmd_type_; // deprecated @dazhi
      int64_t                           rsp_time_; // OB_INVALID_ID means no need response
      common::ObTTLTaskStatus           state_;
      bool                              is_reused_; // all delete task is finished (or canceled)
      ObTTLType                         task_type_;
  };

protected:
  int alloc_tenant_info(uint64_t tenant_id);
  ObTTLTaskCtx* get_one_tablet_ctx(const common::ObTabletID& tablet_id);

  // /*inner function*/
  int handle_all_tablet_event(common::ObSArray<common::ObTabletID>& tablets);
  int handle_one_tablet_event(ObTTLTaskCtx* ctx);
  int generate_batch_tablet_task(ObIArray<share::ObTabletTablePair>& tablet_pairs,
                                 hash::ObHashMap<uint64_t, table::ObTTLTaskParam> &param_map);
  int generate_one_tablet_task(table::ObTTLTaskInfo& task_info, const table::ObTTLTaskParam& para);
  int get_ttl_para_from_schema(const share::schema::ObTableSchema *table_schema, table::ObTTLTaskParam& param);
  void mark_tenant_need_check();
  template <typename TTLTaskT, typename TTLDagT, typename TTLParamT>
  int generate_ttl_dag(table::ObTTLTaskInfo &task_info, TTLParamT &para);
  static int construct_task_record_filter(const uint64_t& task_id,
                                          const uint64_t& table_id,
                                          ObTabletID& tablet_id,
                                          ObTTLStatusFieldArray& filter);
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  int sync_sys_table_op(ObTTLTaskCtx* ctx, bool force_update, bool &tenant_state_changed);
  int sync_sys_table(common::ObTabletID& tablet_id, bool &tenant_state_changed);
  int construct_sys_table_record(ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record);
  bool can_schedule_tenant(const ObTTLTenantInfo &tenant_info);
  bool can_schedule_task(const ObTTLTaskCtx &ttl_task);
  int check_cmd_state_valid(const common::ObTTLTaskStatus current_state,
                            const common::ObTTLTaskStatus incoming_state);
  int copy_all_tenant_ctxs(common::ObSArray<ObTTLTaskCtx *>& ctx_array, uint64_t tenant_id);
  int from_ttl_record(ObTabletID& tablet_id, common::ObTTLStatus& record, bool with_status = true, bool with_err_code = true);
  void check_ttl_tenant_state();
  int try_schedule_prepare_task(ObTabletID& tablet_id);
  void mark_tenant_checked();
  int refresh_tablet_task(ObTTLTaskCtx &ttl_task, bool refresh_status, bool refresh_retcode = false);
  int check_schema_version();
  OB_INLINE bool need_skip_run() { return ATOMIC_LOAD(&need_do_for_switch_); }
protected:
  void mark_ttl_ctx_dirty(ObTTLTenantInfo& tenant_info, ObTTLTaskCtx& ctx);
  int deep_copy_task(ObTTLTaskCtx* ctx, table::ObTTLTaskInfo& task_info, const table::ObTTLTaskParam &task_param, bool with_rowkey_copy = true);
  int try_schedule_remaining_tasks(const ObTTLTaskCtx *current_ctx);

protected:
  ObTTLTenantInfo local_tenant_task_;
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObSpinLock lock_;
protected:
  static const int64_t DEFAULT_TTL_BUCKET_NUM = 100;
  static const int64_t TTL_PERIODIC_DELAY = 10*1000*1000; //10s
  static const int64_t TBALE_GENERATE_BATCH_SIZE = 200;
  static const int64_t DEFAULT_TABLE_ARRAY_SIZE = 200;
  static const int64_t DEFAULT_TABLET_PAIR_SIZE = 1024;
  static const int64_t DEFAULT_PARAM_BUCKET_SIZE = 200;
  static const int64_t TTL_NORMAL_TIME_THRESHOLD = 3*1000*1000; // 3s
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_timer_start_;
  int64_t periodic_delay_;
  OBTTLTimerPeriodicTask periodic_task_;
  storage::ObLS *ls_;
  int tg_id_;
  ObArray<share::ObTabletTablePair> tablet_table_pairs_;
  int64_t local_schema_version_;
  bool has_start_;
  bool is_leader_; // current tenant ttl mgr is in leader ls or not
  volatile int64_t dag_ref_cnt_; // ttl dag ref count for current ls
  // after leader switch, need wait and reset status
  bool need_do_for_switch_;
};

typedef HRowkeyQueueNode HRowkeyNode;
class ObTabletHRowkeyTTLScheduler : public ObTabletTTLScheduler
{
public:
  const static int64_t MAX_HROWKEY_NUM_PER_TASK = 100;
  ObTabletHRowkeyTTLScheduler()
  : hrowkey_queue_()
  {}
  ~ObTabletHRowkeyTTLScheduler()
  {}
public:
  int init(storage::ObLS *ls);
  virtual int try_schedule_task(ObTTLTaskCtx* ctx) override;
  virtual int report_task_status(ObTTLTaskInfo& task_info,
                                 table::ObTTLTaskParam& task_para,
                                 bool& is_stop,
                                 bool need_copy_task = true) override;
  virtual int64_t get_tenant_task_table_id() override { return common::ObTTLUtil::TTL_ROWKEY_TASK_TABLE_ID; }
  virtual int64_t get_tenant_task_tablet_id() override { return common::ObTTLUtil::TTL_ROWKEY_TASK_TABLET_ID; }
  virtual int do_after_leader_switch() override;
  virtual int check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table) override;

private:
  int generate_rowkey_ttl_task();

private:
  struct HRowkeyDedupMapKey
  {
    HRowkeyDedupMapKey()
      : table_id_(OB_INVALID_ID),
        tablet_id_(ObTabletID::INVALID_TABLET_ID)
    {}
    HRowkeyDedupMapKey(uint64_t table_id, common::ObTabletID tablet_id)
      : table_id_(table_id),
        tablet_id_(tablet_id)
    {
    }
    uint64_t            table_id_;
    common::ObTabletID  tablet_id_;
    OB_INLINE bool operator==(const HRowkeyDedupMapKey &other) const
    {
      return table_id_ == other.table_id_ && tablet_id_ == other.tablet_id_;
    }
    OB_INLINE HRowkeyDedupMapKey& operator=(const HRowkeyDedupMapKey &other)
    {
      table_id_ = other.table_id_;
      tablet_id_ = other.tablet_id_;
      return *this;
    }
    OB_INLINE int hash(uint64_t &hash_val) const
    {
      hash_val = tablet_id_.hash();
      hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
      return OB_SUCCESS;
    }
    TO_STRING_KV(K_(table_id), K_(tablet_id));
  };
  class HRowkeyDedupMap
  {
  public:
    typedef container::ObRbTree<HRowkeyNode, container::ObDummyCompHelper<HRowkeyNode>> HRowkeySet;
    typedef common::hash::ObHashMap<HRowkeyDedupMapKey, HRowkeySet *> HRowkeyTabletMap;
  public:
    HRowkeyDedupMap(HRowkeyQueue & hrowkey_owner)
      : allocator_(ObMemAttr(MTL_ID(), "HRowkeyDedupAlc")),
        map_(),
        hrowkey_owner_(hrowkey_owner)
    {}
    ~HRowkeyDedupMap() { destroy(); }
    int init();
    int add_queue_node(HRowkeyQueueNode *node);
    void destroy();
  public:
    ObArenaAllocator allocator_; // use to allocator HRowkeySet
    HRowkeyTabletMap map_;
    HRowkeyQueue &hrowkey_owner_;
  };

  struct TraverseArgs
  {
    TraverseArgs(common::ObIArray<ObString> &rowkeys)
      : rowkeys_(rowkeys)
      {}
    ObIArray<ObString> &rowkeys_;
  };
  static HRowkeyNode *traverse_rowkey_set(HRowkeyDedupMap::HRowkeySet *tree,
                                          HRowkeyNode *cur_node,
                                          void *args);
  struct FreeHRowkeyArgs
  {
    FreeHRowkeyArgs(HRowkeyQueue &queue)
      : hrowkey_owner_(queue)
      {}
    HRowkeyQueue &hrowkey_owner_;
  };
  static void free_hrowkey(HRowkeyNode *cur_node, void *args);

private:
  HRowkeyQueue hrowkey_queue_; // expired hbase rowkey queue for quick deletion
};

template <typename TTLTaskT, typename TTLDagT, typename TTLParamT>
int ObTabletTTLScheduler::generate_ttl_dag(ObTTLTaskInfo &task_info, TTLParamT &para)
{
  int ret = OB_SUCCESS;
  TTLDagT *dag = nullptr;
  TTLTaskT *delete_task = nullptr;
  share::ObTenantDagScheduler *dag_scheduler = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(share::ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "fail to alloc dag", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected error, dag is null", KR(ret), KP(dag));
  } else if (OB_FAIL(dag->init(para, task_info))) {
    COMMON_LOG(WARN, "fail to init ttl dag", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(delete_task))) {
    COMMON_LOG(WARN, "fail to alloc ttl delete task", KR(ret));
  } else if (OB_ISNULL(delete_task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected error, prepare task is null", KR(ret), KP(delete_task));
  } else if (OB_FAIL(delete_task->init(this, para, task_info))) {
    COMMON_LOG(WARN, "fail to init ttl delete task", KR(ret));
  } else if (OB_FAIL(dag->add_task(*delete_task))) {
    COMMON_LOG(WARN, "fail to add ttl delete task to dag", KR(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
    if (OB_EAGAIN == ret) {
      COMMON_LOG(DEBUG, "ttl dag already exists, no need to schedule once again", KR(ret));
    } else if (OB_SIZE_OVERFLOW == ret) {
      COMMON_LOG(DEBUG, "dag is full", KR(ret));
    } else {
      COMMON_LOG(WARN, "fail to add dag to queue", KR(ret));
    }
  } else {
    COMMON_LOG(INFO, "build ttl dag success", KR(ret), K(para), K(task_info));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(dag)) {
    dag_scheduler->free_dag(*dag);
  }
  COMMON_LOG(INFO, "finish to build table api ttl dag", KR(ret), K(para), K(task_info));
  return ret;
}

class ObTenantTabletTTLMgr : public logservice::ObIReplaySubHandler,
                             public logservice::ObICheckpointSubHandler,
                             public logservice::ObIRoleChangeSubHandler
{
public:
  ObTenantTabletTTLMgr();
  virtual ~ObTenantTabletTTLMgr() = default;
  int init(storage::ObLS *ls);
  int switch_to_leader() override;
  int resume_leader() override;
  int switch_to_follower_gracefully() override;
  void switch_to_follower_forcedly() override;
  int safe_to_destroy(bool &is_safe_destroy);
  share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  share::ObPluginVectorIndexLoadScheduler &get_vector_idx_scheduler() { return vector_idx_scheduler_; }
  // for replay, do nothing
  int replay(const void *buffer,
             const int64_t buf_size,
             const palf::LSN &lsn,
             const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(buf_size);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

  int flush(share::SCN &rec_scn) override
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }
  void stop();
  void destroy();
  TO_STRING_KV(K_(is_inited),
               K_(tenant_id),
               K_(ls_id),
               K_(tablet_ttl_schedulers),
               K_(vec_tg_id),
               K_(vector_idx_scheduler));
private:
  template <typename T>
  int alloc_and_init_tablet_scheduler(storage::ObLS *ls);
private:
  // 新增加成员 ObALLTabletTTLScheduler
  ObArenaAllocator allocator_; // use to alloc ObTTLTaskScheduler
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int vec_tg_id_;
  ObSEArray<ObTabletTTLScheduler *, 4> tablet_ttl_schedulers_;
  share::ObPluginVectorIndexLoadScheduler vector_idx_scheduler_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_OB_TENANT_TABLET_TTL_MANAGER_H_ */
