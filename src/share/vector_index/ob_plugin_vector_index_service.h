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

#ifndef OCEANBASE_OBSERVER_OB_PLUGIN_VECTOR_INDEX_SERVICE_DEFINE_H_
#define OCEANBASE_OBSERVER_OB_PLUGIN_VECTOR_INDEX_SERVICE_DEFINE_H_
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_ls_id.h"
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/ob_tenant_vector_index_async_task.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "ob_vector_kmeans_ctx.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace share
{
struct ObIvfHelperKey final
{
public:
  ObIvfHelperKey(): tablet_id_(), context_id_(OB_INVALID_ID)
  {}
  ObIvfHelperKey(const ObTabletID &tablet_id, const int64_t context_id)
    : tablet_id_(tablet_id), context_id_(context_id) {}
  ~ObIvfHelperKey() = default;
  uint64_t hash() const {
    return tablet_id_.hash() + murmurhash(&context_id_, sizeof(context_id_), 0);
  }
  int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
  bool is_valid() const { return tablet_id_.is_valid() && context_id_ >= 0; }
  bool operator == (const ObIvfHelperKey &other) const {
        return tablet_id_ == other.tablet_id_ && context_id_ == other.context_id_; }
  TO_STRING_KV(K_(tablet_id), K_(context_id));
public:
  common::ObTabletID tablet_id_;
  int64_t context_id_;
};

class ObVectorIndexAdapterCandiate final
{
public:
  ObVectorIndexAdapterCandiate()
    : is_init_(true),
      is_valid_(true),
      is_hybrid_(false),
      inc_adatper_guard_(),
      bitmp_adatper_guard_(),
      sn_adatper_guard_(),
      embedded_adatper_guard_()
  {}

  ~ObVectorIndexAdapterCandiate() {
    is_init_ = false;
    is_valid_ = false;
    is_hybrid_ = false;
  }

  TO_STRING_KV(K_(is_init), K_(is_valid), K_(inc_adatper_guard), K_(bitmp_adatper_guard), K_(sn_adatper_guard), K_(embedded_adatper_guard));

public:
  bool is_init_;
  bool is_valid_;
  bool is_hybrid_;
  bool is_complete()
  {
    return inc_adatper_guard_.is_valid() && bitmp_adatper_guard_.is_valid() && sn_adatper_guard_.is_valid() && (!is_hybrid_ || embedded_adatper_guard_.is_valid());
  }
  ObPluginVectorIndexAdapterGuard inc_adatper_guard_;
  ObPluginVectorIndexAdapterGuard bitmp_adatper_guard_;
  ObPluginVectorIndexAdapterGuard sn_adatper_guard_;
  ObPluginVectorIndexAdapterGuard embedded_adatper_guard_;
};

struct ObAdapterMapKeyValue
{
public:
  ObAdapterMapKeyValue(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *adapter)
      : tablet_id_(tablet_id),
        adapter_(adapter)
  {}
  ObAdapterMapKeyValue()
      : tablet_id_(),
        adapter_(nullptr)
  {}
  TO_STRING_KV(K_(tablet_id), K_(adapter));

  ObTabletID tablet_id_;
  ObPluginVectorIndexAdaptor *adapter_;
};

class ObAdapterMapFunc
{
public:
  ObAdapterMapFunc(ObIArray<ObAdapterMapKeyValue> &array) :array_(array) {}
  ~ObAdapterMapFunc() {}
  int operator()(const hash::HashMapPair<common::ObTabletID, ObPluginVectorIndexAdaptor*> &entry);
private:
  ObIArray<ObAdapterMapKeyValue> &array_;
};

typedef common::hash::ObHashMap<ObIvfHelperKey, ObIvfBuildHelper*> IvfVectorIndexHelperMap;
typedef common::hash::ObHashMap<common::ObTabletID, ObIvfCacheMgr*> IvfCacheMgrMap;
// Manage all vector index adapter in a ls
class ObPluginVectorIndexMgr
{
public:
  ObPluginVectorIndexMgr(lib::MemoryContext &memory_context, uint64_t tenant_id)
    : is_inited_(false),
      need_check_(false),
      ls_id_(),
      complete_index_adpt_map_(),
      partial_index_adpt_map_(),
      ivf_index_helper_map_(),
      adapter_map_rwlock_(),
      ls_tablet_task_ctx_(),
      tenant_id_(tenant_id),
      interval_factor_(0),
      vector_index_service_(nullptr),
      mem_sync_info_(tenant_id),
      memory_context_(memory_context),
      all_vsag_use_mem_(nullptr),
      async_task_opt_(tenant_id),
      ls_leader_(false)
  {}
  virtual ~ObPluginVectorIndexMgr();

  int init(uint64_t tenant_id, ObLSID ls_id, lib::MemoryContext &memory_context, uint64_t *all_vsag_use_mem);
  ObLSID& get_ls_id() { return ls_id_; }
  uint64_t get_tenant_id() { return tenant_id_; }
  ObPluginVectorIndexLSTaskCtx& get_ls_task_ctx() { return ls_tablet_task_ctx_; }
  VectorIndexAdaptorMap& get_partial_adapter_map() { return partial_index_adpt_map_; }
  VectorIndexAdaptorMap& get_complete_adapter_map() { return complete_index_adpt_map_; }
  IvfCacheMgrMap& get_ivf_cache_mgr_map() { return ivf_cache_mgr_map_; }
  IvfVectorIndexHelperMap& get_ivf_helper_map() { return ivf_index_helper_map_; }
  lib::MemoryContext& get_memory_context() { return memory_context_; }
  uint64_t *get_all_vsag_use_mem() { return all_vsag_use_mem_; }

  // thread save interface
  void destroy();

  void release_all_adapters();
  bool get_ls_leader() { return ls_leader_; }
  void set_ls_leader(const bool ls_leader) { ls_leader_ = ls_leader; }

  int get_adapter_inst_guard(ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adpt_guard);
  int get_adapter_inst_guard_in_lock(ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adpt_guard);
  int get_build_helper_inst_guard(const ObIvfHelperKey &key, ObIvfBuildHelperGuard &helper_guard);
  int create_partial_adapter(ObTabletID idx_tablet_id,
                             ObTabletID data_tablet_id,
                             ObIndexType type,
                             ObIAllocator &allocator,
                             int64_t index_table_id,
                             ObString *vec_index_param = nullptr,
                             int64_t dim = 0);
  int create_ivf_build_helper(const ObIvfHelperKey &key, ObIndexType type, ObString &vec_index_param, ObIAllocator &allocator);
  int replace_with_complete_adapter(ObVectorIndexAdapterCandiate *candidate,
                                    ObVecIdxSharedTableInfoMap &info_map,
                                    ObIAllocator &allocator);
  int replace_old_adapter(ObPluginVectorIndexAdaptor *new_adapter);
  common::RWLock& get_adapter_map_lock() { return adapter_map_rwlock_; }
  int replace_with_full_partial_adapter(ObVectorIndexAcquireCtx &ctx,
                                        ObIAllocator &allocator,
                                        ObPluginVectorIndexAdapterGuard &adapter_guard,
                                        ObString *vec_index_param,
                                        int64_t dim,
                                        ObVectorIndexAdapterCandiate *candidate);

  int get_or_create_partial_adapter(ObTabletID tablet_id,
                                    ObIndexType type,
                                    ObPluginVectorIndexAdapterGuard &adapter_guard,
                                    ObString *vec_index_param,
                                    int64_t dim);

  int get_adapter_inst_by_ctx(ObVectorIndexAcquireCtx &ctx,
                              bool &need_merge,
                              ObIAllocator &allocator,
                              ObPluginVectorIndexAdapterGuard &adapter_guard,
                              ObVectorIndexAdapterCandiate &candidate,
                              ObString *vec_index_param,
                              int64_t dim);

  int get_and_merge_adapter(ObVectorIndexAcquireCtx &ctx,
                            ObIAllocator &allocator,
                            ObPluginVectorIndexAdapterGuard &adapter_guard,
                            ObString *vec_index_param,
                            int64_t dim);
  int check_and_merge_partial_inner(ObVecIdxSharedTableInfoMap &info_map, ObIAllocator &allocator);

  // maintance interface
  int check_need_mem_data_sync_task(bool &need_sync);
  int erase_complete_adapter(ObTabletID tablet_id);
  int erase_partial_adapter(ObTabletID tablet_id);
  int erase_ivf_build_helper(const ObIvfHelperKey &key);
  int release_ivf_cache_mgr(ObIvfCacheMgr* &mgr);
  int set_ivf_cache_mgr(const ObIvfCacheMgrKey& cachr_mgr_key,
                        ObIvfCacheMgr *cache_mgr,
                        int overwrite = 0);
  int get_ivf_cache_mgr(const ObIvfCacheMgrKey& cachr_mgr_key, ObIvfCacheMgr *&cache_mgr);
  int erase_ivf_cache_mgr(const ObIvfCacheMgrKey& cachr_mgr_key);
  int get_ivf_cache_mgr_guard(const ObIvfCacheMgrKey& cachr_mgr_key, ObIvfCacheMgrGuard &cache_mgr_guard);
  int get_or_create_ivf_cache_mgr_guard(ObIAllocator &allocator,
                                        const ObIvfCacheMgrKey &key,
                                        const ObVectorIndexParam &vec_index_param,
                                        int64_t dim,
                                        int64_t table_id,
                                        ObIvfCacheMgrGuard &cache_mgr_guard);
  ObVectorIndexMemSyncInfo &get_mem_sync_info() { return mem_sync_info_; }
  ObVecIndexAsyncTaskOption &get_async_task_opt() { return async_task_opt_; }
  // for debug
  void dump_all_inst();
  // for virtual table
  int get_snapshot_tablet_ids(ObIArray<obrpc::ObLSTabletPair> &complete_tablet_ids,  ObIArray<obrpc::ObLSTabletPair> &partial_tablet_ids);
  int get_cache_tablet_ids(ObLSID &ls_id, ObIArray<ObLSTabletPair> &cache_tablet_ids);

  TO_STRING_KV(K_(is_inited), K_(need_check), K_(ls_id), K_(ls_tablet_task_ctx));

private:
  // non-thread save inner functions
  int get_adapter_inst_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *&index_inst);
  int set_complete_adapter_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *adapter_inst, int overwrite = 0);

  int set_partial_adapter_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *adapter_inst, int overwrite = 0);
  int erase_partial_adapter_(ObTabletID tablet_id);
  int set_ivf_build_helper_(const ObIvfHelperKey &key, ObIvfBuildHelper *helper_inst);
  // thread save inner functions
  int get_or_create_partial_adapter_(ObTabletID tablet_id,
                                     ObIndexType type,
                                     ObPluginVectorIndexAdapterGuard &adapter_guard,
                                     ObString *vec_index_param,
                                     int64_t dim,
                                     ObIAllocator &allocator);

  int get_build_helper_inst_(const ObIvfHelperKey &key, ObIvfBuildHelper *&helper_inst);
  int create_ivf_cache_mgr(ObIAllocator &allocator,
                          const ObIvfCacheMgrKey &key,
                          const ObVectorIndexParam &vec_index_param,
                          int64_t dim,
                          int64_t table_id);
private:
  static const int64_t DEFAULT_ADAPTER_HASH_SIZE = 1000;
  static const int64_t DEFAULT_CANDIDATE_ADAPTER_HASH_SIZE = 1000;

  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;

  bool is_inited_;
  bool need_check_; // schema version change, or ls/tablet not existed
  share::ObLSID ls_id_;
  VectorIndexAdaptorMap complete_index_adpt_map_; // map of complete index adapters with full info
  VectorIndexAdaptorMap partial_index_adpt_map_; // map of passive created index adapters
  IvfVectorIndexHelperMap ivf_index_helper_map_; // map of ivf inder build helper
  IvfCacheMgrMap ivf_cache_mgr_map_; // map of ivf cache managers
  TCRWLock adapter_map_rwlock_; // lock for adapter maps
  ObPluginVectorIndexLSTaskCtx ls_tablet_task_ctx_; // task ctx of ls level
  uint64_t tenant_id_;
  uint32_t interval_factor_; // used to expand real execute interval
  ObPluginVectorIndexService *vector_index_service_;
  int64_t local_schema_version_; // detect schema change
  ObVectorIndexMemSyncInfo mem_sync_info_; // handle follower memdata sync
  lib::MemoryContext &memory_context_;
  uint64_t *all_vsag_use_mem_;
  ObVecIndexAsyncTaskOption async_task_opt_;
  bool ls_leader_;
};

// id to unique identify an vector index adapter
struct ObPluginVectorIndexIdentity
{
  ObPluginVectorIndexIdentity() : data_tablet_id_(), index_identity_() {};
  ObPluginVectorIndexIdentity(ObTabletID data_tablet_id, common::ObString index_identity)
    : data_tablet_id_(data_tablet_id), index_identity_(index_identity)
  {}
  ~ObPluginVectorIndexIdentity()
  {
    data_tablet_id_.reset();
    index_identity_.reset();
  }
  bool is_valid() { return data_tablet_id_.is_valid() && !index_identity_.empty(); }
  uint64_t hash() const
  {
    int64_t hash_value = data_tablet_id_.hash();
    hash_value += index_identity_.hash();
    return hash_value;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObPluginVectorIndexIdentity &other) const
  {
    return data_tablet_id_ == other.data_tablet_id_ && index_identity_ == other.index_identity_;
  }
  TO_STRING_KV(K_(data_tablet_id), K_(index_identity));

  ObTabletID data_tablet_id_;
  ObString index_identity_; // index_name_prefix
};

struct ObVectorIndexTmpInfo final
{
public:
  ObVectorIndexTmpInfo() : snapshot_version_(0), schema_version_(0), ret_code_(OB_NOT_INIT), ddl_slice_info_(), adapter_(nullptr) {}
  ~ObVectorIndexTmpInfo() {}
  TO_STRING_KV(K_(ddl_slice_info), KP_(adapter), K_(snapshot_version), K_(schema_version), K_(ret_code));

public:
  int64_t snapshot_version_;
  int64_t schema_version_;
  int ret_code_;
  rootserver::ObDDLSliceInfo ddl_slice_info_;
  ObPluginVectorIndexAdaptor *adapter_;
  common::ObArenaAllocator allocator_;
};

typedef common::hash::ObHashMap<share::ObLSID, ObPluginVectorIndexMgr*> LSIndexMgrMap;
typedef common::hash::ObHashMap<int64_t, ObVectorIndexTmpInfo*> ObVecIndexTmpInfoMap;
// Manage all vector index adapters of a tenant
class ObPluginVectorIndexService : public logservice::ObIReplaySubHandler,
                                   public logservice::ObICheckpointSubHandler,
                                   public logservice::ObIRoleChangeSubHandler
{
public:
  ObPluginVectorIndexService()
  : is_inited_(false),
    has_start_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_ls_or_tablet_changed_(false),
    schema_service_(NULL),
    ls_service_(NULL),
    sql_proxy_(NULL),
    memory_context_(NULL),
    all_vsag_use_mem_(NULL),
    tenant_vec_async_task_sched_(nullptr),
    is_vec_async_task_started_(false),
    kmeans_tg_id_(OB_INVALID_TG_ID),
    embedding_tg_id_(OB_INVALID_TG_ID)
  {}
  virtual ~ObPluginVectorIndexService();
  int init(const uint64_t tenant_id,
           schema::ObMultiVersionSchemaService *schema_service,
           ObLSService *ls_service);
  bool is_inited() { return is_inited_; }
  // mtl interfaces
  static int mtl_init(ObPluginVectorIndexService *&service);
  int start();
  void stop();
  void wait();
  void destroy();

  // for LS leader operation
  int flush(share::SCN &rec_scn)
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }
  share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  // for replay, do nothing
  int replay(const void *buffer,
             const int64_t buf_size,
             const palf::LSN &lsn,
             const share::SCN &scn)
  {
    UNUSED(buffer);
    UNUSED(buf_size);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  void inner_switch_to_follower();
  void switch_to_follower_forcedly();
  int switch_to_leader();
  int switch_to_follower_gracefully();
  int resume_leader() { return switch_to_leader(); }
  int alloc_tenant_vec_async_task_sched();
  ObFIFOAllocator &get_allocator() { return allocator_; }

  // feature interfaces
  int get_kmeans_tg_id() { return kmeans_tg_id_; }
  int get_embedding_tg_id() { return embedding_tg_id_; }
  ObVecIndexAsyncTaskHandler &get_vec_async_task_handle() { return vec_async_task_handle_; }
  ObKmeansBuildTaskHandler& get_kmeans_build_handler() { return kmeans_build_task_handler_; };
  int get_embedding_task_handler(ObEmbeddingTaskHandler *&handler);
  LSIndexMgrMap &get_ls_index_mgr_map() { return index_ls_mgr_map_; };
  ObVecIndexTmpInfoMap &get_vector_index_tmp_info_map() { return vec_idx_tmp_map_; }
  int release_vector_index_tmp_info(const int64_t task_id);
  int get_adapter_inst_guard(ObLSID ls_id, ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adapter_guard);
  int get_build_helper_inst_guard(ObLSID ls_id, const ObIvfHelperKey &key, ObIvfBuildHelperGuard &helper_guard);
  int create_partial_adapter(ObLSID ls_id,
                             ObTabletID idx_tablet_id,
                             ObTabletID data_tablet_id,
                             ObIndexType type,
                             int64_t index_table_id,
                             ObString *vec_index_param = nullptr,
                             int64_t dim = 0);
  int create_ivf_build_helper(ObLSID ls_id,
                              const ObIvfHelperKey &key,
                              ObIndexType type,
                              ObString &vec_index_param);
  int erase_ivf_build_helper(ObLSID ls_id, const ObIvfHelperKey &key);
  int check_and_merge_adapter(ObLSID ls_id, ObVecIdxSharedTableInfoMap &info_map);
  int acquire_vector_index_mgr(ObLSID ls_id, ObPluginVectorIndexMgr *&mgr);
  int get_vector_index_tmp_info(const int64_t task_id, ObVectorIndexTmpInfo *&tmp_info);

  // user interfaces
  int acquire_adapter_guard(ObLSID ls_id,
                            ObTabletID tablet_id,
                            ObIndexType type,
                            ObPluginVectorIndexAdapterGuard &adapter_guard,
                            ObString *vec_index_param = nullptr,
                            int64_t dim = 0);
  int acquire_adapter_guard(ObLSID ls_id,
                            ObVectorIndexAcquireCtx &ctx,
                            ObPluginVectorIndexAdapterGuard &adapter_guard,
                            ObString *vec_index_param = nullptr,
                            int64_t dim = 0);
  int acquire_ivf_build_helper_guard(ObLSID ls_id,
                                     const ObIvfHelperKey &key,
                                     ObIndexType type,
                                     ObIvfBuildHelperGuard &helper_guard,
                                     ObString &vec_index_param);

  // for debug
  int dump_all_inst();
  // for virtual table
  int get_snapshot_ids(ObIArray<obrpc::ObLSTabletPair> &complete_tablet_ids,  ObIArray<obrpc::ObLSTabletPair> &partial_tablet_ids);
  int get_cache_ids(ObIArray<ObLSTabletPair> &cache_tablet_ids);
  // for ivf
  // ivfflat index needs center ids
  // ivfsq index needs sq metas and center ids
  // ivfpq index needs center ids and pq center ids
  int get_ivf_aux_info(
      const uint64_t table_id,
      const ObTabletID tablet_id,
      ObIAllocator &allocator,
      ObIArray<float*> &aux_info);
  // NOTE(liyao): int callback_func(int64_t dim, float *data);
  //              data should be deep copied if used outside callback_func
  template<class CallbackFunc>
  int process_ivf_aux_info(
      const uint64_t table_id,
      const ObTabletID tablet_id,
      ObIAllocator &allocator,
      CallbackFunc &callback_func);
  int acquire_ivf_cache_mgr_guard(ObLSID ls_id,
                                  const ObIvfCacheMgrKey &key,
                                  const ObVectorIndexParam &vec_index_param,
                                  int64_t dim,
                                  int64_t table_id,
                                  ObIvfCacheMgrGuard &cache_mgr_guard);
  int acquire_ivf_cache_mgr_guard(ObLSID ls_id, const ObIvfCacheMgrKey &key, ObIvfCacheMgrGuard &cache_mgr_guard);
  lib::MemoryContext &get_memory_context() { return memory_context_; }
  uint64_t *get_all_vsag_use_mem() { return all_vsag_use_mem_; }

  int start_kmeans_tg();
  TO_STRING_KV(K_(is_inited), K_(has_start), K_(tenant_id),
               K_(is_ls_or_tablet_changed), KP_(schema_service), KP_(ls_service));
private:
  // for ivf
  int generate_get_aux_info_sql(
      const uint64_t table_id,
      const ObTabletID tablet_id,
      bool &is_hidden_table,
      ObSqlString &sql_string);
private:
  static const int64_t BASIC_TIMER_INTERVAL = 30 * 1000 * 1000; // 30s
  static const int64_t VEC_INDEX_LOAD_TIME_TASKER_THRESHOLD = 30 * 1000 * 1000; // 30s
  static const int64_t DEFAULT_LS_HASH_SIZE = 64;
  bool is_inited_;
  bool has_start_;
  int64_t tenant_id_;
  LSIndexMgrMap index_ls_mgr_map_;
  bool is_ls_or_tablet_changed_;

  share::schema::ObMultiVersionSchemaService *schema_service_;
  storage::ObLSService *ls_service_;
  common::ObMySQLProxy *sql_proxy_;
  ObFIFOAllocator allocator_;
  common::ObArenaAllocator alloc_;
  // do not use this memory context directly
  // use wrapped memory context in ob_tenant_vector_allocator.h and init by this memory context
  lib::MemoryContext memory_context_;
  uint64_t *all_vsag_use_mem_;
  ObTenantVecAsyncTaskScheduler *tenant_vec_async_task_sched_;
  bool is_vec_async_task_started_;
  ObVecIndexAsyncTaskHandler vec_async_task_handle_;
  ObKmeansBuildTaskHandler kmeans_build_task_handler_;
  ObVecIndexTmpInfoMap vec_idx_tmp_map_;
  ObEmbeddingTaskHandler embedding_task_handler_;
  // TODO(haohan): shared_tg_id for kmeans and embedding thread pool
  int kmeans_tg_id_;
  int embedding_tg_id_;

public:
  volatile bool stop_flag_;
};

template<class CallbackFunc>
int ObPluginVectorIndexService::process_ivf_aux_info(
    const uint64_t table_id,
    const ObTabletID tablet_id,
    ObIAllocator &allocator,
    CallbackFunc &callback_func)
{
  int ret = OB_SUCCESS;
  bool is_hidden_table = false;
  ObSqlString sql_string;
  static_assert(std::is_same<typename std::result_of<CallbackFunc(int64_t, float*)>::type, int>::value,
        "process_ivf_aux_info callback format error");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(generate_get_aux_info_sql(table_id, tablet_id, is_hidden_table, sql_string))) {
    OB_LOG(WARN, "failed to generate sql", K(ret), K(table_id));
  } else {
    ObSessionParam session_param;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_dummy_ddl_for_inner_visibility(true);
    session_param.ddl_info_.set_source_table_hidden(is_hidden_table);
    session_param.ddl_info_.set_dest_table_hidden(false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql_string.ptr(), &session_param))) {
        OB_LOG(WARN, "failed to execute sql", K(ret), K(sql_string));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to execute sql", K(ret), K(sql_string));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          const int64_t col_idx = 0;
          ObObj vec_obj;
          ObString blob_data;
          if (OB_FAIL(result->get_obj(col_idx, vec_obj))) {
            OB_LOG(WARN, "failed to get vid", K(ret));
          } else if (FALSE_IT(blob_data = vec_obj.get_string())) {
          } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator,
                                                                        ObLongTextType,
                                                                        CS_TYPE_BINARY,
                                                                        true,
                                                                        blob_data))) {
            OB_LOG(WARN, "fail to get real data.", K(ret), K(blob_data));
          } else {
            int64_t dim = blob_data.length() / sizeof(float);
            if (OB_FAIL(callback_func(dim, reinterpret_cast<float*>(blob_data.ptr())))) {
              OB_LOG(WARN, "fail to do callback func", K(ret), K(dim));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase
#endif
