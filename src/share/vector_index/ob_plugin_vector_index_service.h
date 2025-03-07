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
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "ob_vector_kmeans_ctx.h"

namespace oceanbase
{
namespace share
{

struct ObVectorIndexAcquireCtx
{
  ObTabletID inc_tablet_id_;
  ObTabletID vbitmap_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID data_tablet_id_;

  TO_STRING_KV(K_(inc_tablet_id), K_(vbitmap_tablet_id), K_(snapshot_tablet_id), K_(data_tablet_id));
};

class ObVectorIndexAdapterCandiate final
{
public:
  ObVectorIndexAdapterCandiate()
    : is_init_(true),
      is_valid_(true),
      inc_adatper_guard_(),
      bitmp_adatper_guard_(),
      sn_adatper_guard_()
  {}

  ~ObVectorIndexAdapterCandiate() {
    is_init_ = false;
    is_valid_ = false;
  }

  TO_STRING_KV(K_(is_init), K_(is_valid), K_(inc_adatper_guard), K_(bitmp_adatper_guard), K_(sn_adatper_guard));

public:
  bool is_init_;
  bool is_valid_;
  bool is_complete()
  {
    return inc_adatper_guard_.is_valid() && bitmp_adatper_guard_.is_valid() && sn_adatper_guard_.is_valid();
  }
  ObPluginVectorIndexAdapterGuard inc_adatper_guard_;
  ObPluginVectorIndexAdapterGuard bitmp_adatper_guard_;
  ObPluginVectorIndexAdapterGuard sn_adatper_guard_;
};

typedef common::hash::ObHashMap<common::ObTabletID, ObIvfBuildHelper*> IvfVectorIndexHelperMap;
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
      all_vsag_use_mem_(nullptr)
  {}
  virtual ~ObPluginVectorIndexMgr();

  int init(uint64_t tenant_id, ObLSID ls_id, lib::MemoryContext &memory_context, uint64_t *all_vsag_use_mem);
  ObLSID& get_ls_id() { return ls_id_; }
  uint64_t get_tenant_id() { return tenant_id_; }
  ObPluginVectorIndexLSTaskCtx& get_ls_task_ctx() { return ls_tablet_task_ctx_; }
  VectorIndexAdaptorMap& get_partial_adapter_map() { return partial_index_adpt_map_; }
  VectorIndexAdaptorMap& get_complete_adapter_map() { return complete_index_adpt_map_; }
  IvfVectorIndexHelperMap& get_ivf_helper_map() { return ivf_index_helper_map_; }


  // thread save interface
  void destroy();

  void release_all_adapters();

  int get_adapter_inst_guard(ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adpt_guard);
  int get_build_helper_inst_guard(ObTabletID tablet_id, ObIvfBuildHelperGuard &helper_guard);
  int create_partial_adapter(ObTabletID idx_tablet_id,
                             ObTabletID data_tablet_id,
                             ObIndexType type,
                             ObIAllocator &allocator,
                             int64_t index_table_id,
                             ObString *vec_index_param = nullptr,
                             int64_t dim = 0);
  int create_ivf_build_helper(ObTabletID idx_tablet_id, ObIndexType type, ObString &vec_index_param, ObIAllocator &allocator);
  int replace_with_complete_adapter(ObVectorIndexAdapterCandiate *candidate,
                                    ObVecIdxSharedTableInfoMap &info_map,
                                    ObIAllocator &allocator);
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
  int erase_ivf_build_helper(ObTabletID tablet_id);
  ObVectorIndexMemSyncInfo &get_mem_sync_info() { return mem_sync_info_; }
  // for debug
  void dump_all_inst();
  // for virtual table
  int get_snapshot_tablet_ids(ObIArray<obrpc::ObLSTabletPair> &complete_tablet_ids,  ObIArray<obrpc::ObLSTabletPair> &partial_tablet_ids);

  TO_STRING_KV(K_(is_inited), K_(need_check), K_(ls_id), K_(ls_tablet_task_ctx));

private:
  // non-thread save inner functions
  int get_adapter_inst_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *&index_inst);
  int set_complete_adapter_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *adapter_inst, int overwrite = 0);

  int set_partial_adapter_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *adapter_inst, int overwrite = 0);
  int erase_partial_adapter_(ObTabletID tablet_id);
  int set_ivf_build_helper_(ObTabletID tablet_id, ObIvfBuildHelper *helper_inst);
  // thread save inner functions
  int get_or_create_partial_adapter_(ObTabletID tablet_id,
                                     ObIndexType type,
                                     ObPluginVectorIndexAdapterGuard &adapter_guard,
                                     ObString *vec_index_param,
                                     int64_t dim,
                                     ObIAllocator &allocator);

  int get_build_helper_inst_(ObTabletID tablet_id, ObIvfBuildHelper *&helper_inst);
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
  RWLock adapter_map_rwlock_; // lock for adapter maps
  ObPluginVectorIndexLSTaskCtx ls_tablet_task_ctx_; // task ctx of ls level
  uint64_t tenant_id_;
  uint32_t interval_factor_; // used to expand real execute interval
  ObPluginVectorIndexService *vector_index_service_;
  int64_t local_schema_version_; // detect schema change
  ObVectorIndexMemSyncInfo mem_sync_info_; // handle follower memdata sync
  lib::MemoryContext &memory_context_;
  uint64_t *all_vsag_use_mem_;
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

typedef common::hash::ObHashMap<share::ObLSID, ObPluginVectorIndexMgr*> LSIndexMgrMap;
// Manage all vector index adapters of a tenant
class ObPluginVectorIndexService
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
    all_vsag_use_mem_(0)

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

  // feature interfaces
  LSIndexMgrMap &get_ls_index_mgr_map() { return index_ls_mgr_map_; };
  int get_adapter_inst_guard(ObLSID ls_id, ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adapter_guard);
  int get_build_helper_inst_guard(ObLSID ls_id, ObTabletID tablet_id, ObIvfBuildHelperGuard &helper_guard);
  int create_partial_adapter(ObLSID ls_id,
                             ObTabletID idx_tablet_id,
                             ObTabletID data_tablet_id,
                             ObIndexType type,
                             int64_t index_table_id,
                             ObString *vec_index_param = nullptr,
                             int64_t dim = 0);
  int create_ivf_build_helper(ObLSID ls_id,
                              ObTabletID idx_tablet_id,
                              ObIndexType type,
                              ObString &vec_index_param);
  int erase_ivf_build_helper(ObLSID ls_id, ObTabletID idx_tablet_id);
  int check_and_merge_adapter(ObLSID ls_id, ObVecIdxSharedTableInfoMap &info_map);
  int acquire_vector_index_mgr(ObLSID ls_id, ObPluginVectorIndexMgr *&mgr);

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
                                     ObTabletID tablet_id,
                                     ObIndexType type,
                                     ObIvfBuildHelperGuard &helper_guard,
                                     ObString &vec_index_param);

  // for debug
  int dump_all_inst();
  // for virtual table
  int get_snapshot_ids(ObIArray<obrpc::ObLSTabletPair> &complete_tablet_ids,  ObIArray<obrpc::ObLSTabletPair> &partial_tablet_ids);
  // for ivf
  // ivfflat index needs center ids
  // ivfsq index needs sq metas and center ids
  // ivfpq index needs center ids and pq center ids
  int get_ivf_aux_info(
      const uint64_t table_id,
      const ObTabletID tablet_id,
      ObIAllocator &allocator,
      ObIArray<float*> &aux_info);

  TO_STRING_KV(K_(is_inited), K_(has_start), K_(tenant_id),
               K_(is_ls_or_tablet_changed), KP_(schema_service), KP_(ls_service));
private:
  // for ivf
  int generate_get_aux_info_sql(
      const uint64_t table_id,
      const ObTabletID tablet_id,
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
  lib::MemoryContext memory_context_;
  uint64_t all_vsag_use_mem_;

public:
  volatile bool stop_flag_;
};

} // namespace share
} // namespace oceanbase
#endif