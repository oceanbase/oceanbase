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

#ifndef OB_OCEANBASE_SCHEMA_MULTI_VERSION_SCHEMS_SERVICE_H_
#define OB_OCEANBASE_SCHEMA_MULTI_VERSION_SCHEMS_SERVICE_H_

#include <pthread.h>
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "share/ob_errno.h"
#include "share/schema/ob_server_schema_service.h"
#include "share/schema/ob_schema_cache.h"
#include "share/schema/ob_schema_store.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_ddl_trans_controller.h"
#include "share/schema/ob_ddl_epoch.h"

namespace oceanbase
{
namespace obrpc
{
class ObPurgeRecycleBinArg;
}
class ObSchemaSlot;
namespace common
{
}
namespace tools
{
class ObAgentTaskGenerator;
class ObAgentTaskWorker;
}
namespace share
{
namespace schema
{

static const int64_t MAX_CACHED_VERSION_NUM = 4;

// singleton class
// concurrency control for schema manager constructing tasks.
class ObSchemaConstructTask
{
public:
  virtual ~ObSchemaConstructTask();
  static ObSchemaConstructTask& get_instance();
  // concurrency control for fetching the given schema version
  void cc_before(const int64_t version);
  void cc_after(const int64_t version);
private:
  static const int MAX_PARALLEL_TASK = 1;
  ObSchemaConstructTask();
  void lock();
  void unlock();
  int get_idx(int64_t id);
  bool exist(int64_t id) { return -1 != get_idx(id); }
  void add(int64_t id);
  void remove(int64_t id);
  int64_t count() {return schema_tasks_.count();}
  void wait(const int64_t version);
  void wakeup(const int64_t version);
private:
  common::ObArray<int64_t> schema_tasks_;
  pthread_mutex_t schema_mutex_;
  pthread_cond_t schema_cond_;
};

class ObSchemaVersionUpdater
{
public:
  ObSchemaVersionUpdater(int64_t new_schema_version, bool ignore_error = true)
    : new_schema_version_(new_schema_version), ignore_error_(ignore_error) {};
  virtual ~ObSchemaVersionUpdater() {};
  int operator() (int64_t& version) {
    int ret = common::OB_SUCCESS;
    if (version < new_schema_version_) {
      version = new_schema_version_;
    } else {
      ret = ignore_error_ ? common::OB_SUCCESS : common::OB_OLD_SCHEMA_VERSION;
    }
    return ret;
  }

  int operator() (common::hash::HashMapPair<uint64_t, int64_t> &entry) {
    int ret = common::OB_SUCCESS;
    if (entry.second < new_schema_version_) {
      entry.second = new_schema_version_;
    } else {
      ret = ignore_error_ ? common::OB_SUCCESS : common::OB_OLD_SCHEMA_VERSION;
    }
    return ret;
  }
private:
  int64_t new_schema_version_;
  bool ignore_error_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaVersionUpdater);
};

// singleton class
class ObMultiVersionSchemaService;

class ObSchemaGetterGuard;
class ObMultiVersionSchemaService : public ObServerSchemaService
{
typedef common::ObSortedVector<ObSchemaMgr *> SchemaMgrInfos;
typedef SchemaMgrInfos::iterator SchemaMgrIterator;
public:
  static bool g_skip_resolve_materialized_view_definition_;

  enum RefreshSchemaMode
  {
    NORMAL = 0,
    FORCE_FALLBACK,
    FORCE_LAZY
  };
  const char *print_refresh_schema_mode(const RefreshSchemaMode mode);

public:
  static ObMultiVersionSchemaService &get_instance();

  int init(common::ObMySQLProxy *proxy,
      common::ObDbLinkProxy *dblink_proxy,
      const common::ObCommonConfig *config,
      const int64_t init_version_count,
      const int64_t init_version_count_for_liboblog);

  void dump_schema_statistics();

  /*--------------------- get schema guard --------------------*/

  // FIXME: ATTENTION!!! This interface will be deprecated soon, don't use this interface again.
  virtual int get_cluster_schema_guard(
              ObSchemaGetterGuard &guard,
              const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  // After the schema is split, for user tenants, the system table schema is referenced from the system tenant,
  // so when the guard is taken, the schema_version of the system tenant must be additionally passed in.
  // 1. tenant_schema_version is the schema_version of the corresponding tenant
  // 2. sys_schema_version is the schema_version of the system tenant. For system tenants,
  //  the value will be reset to tenant_schema_version
  virtual int get_tenant_schema_guard(const uint64_t tenant_id,
                                      ObSchemaGetterGuard &guard,
                                      int64_t tenant_schema_version = common::OB_INVALID_VERSION,
                                      int64_t sys_schema_version = common::OB_INVALID_VERSION,
                                      const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  virtual int get_tenant_full_schema_guard(const uint64_t tenant_id,
                                           ObSchemaGetterGuard &guard,
                                           bool check_formal = true);
  //retry get schema guard will retry 10 times, it's interval is 100ms
  int retry_get_schema_guard(
      const uint64_t tenant_id,
      const int64_t schema_version,
      const uint64_t table_id,
      ObSchemaGetterGuard &guard,
      int64_t &save_schema_version);

  /*------------- get/construct schema info ----------------*/

  int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids);

  virtual int get_schema(const ObSchemaMgr *mgr,
                         const ObRefreshSchemaStatus &schema_status,
                         const ObSchemaType schema_type,
                         const uint64_t schema_id,
                         const int64_t schema_version,
                         common::ObKVCacheHandle &handle,
                         const ObSchema *&schema);

  int get_latest_schema(common::ObIAllocator &allocator,
                        const ObSchemaType schema_type,
                        const uint64_t tenant_id,
                        const uint64_t schema_id,
                        const ObSchema *&schema);

  const ObSimpleTenantSchema* get_simple_gts_tenant() const
  {
    return schema_cache_.get_simple_gts_tenant();
  }

  // Get pairs of tablet-table with specific tenant_id/schema_version.
  // If local cache miss, this function will fetch pairs of tablet-table from __all_tablet_to_table_history.
  //
  // @param[in]:
  // - tenant_id: tenant_id != 0.
  // - tablet_ids: tablet_ids.count() > 0. tablet_id in tablet_ids should be unique.
  // - schema_version: schema_version > 0. schema_version should be tenant level to raise cache hit rate
  //
  // @param[out]:
  // - table_ids: There is one-to-one correspondence between tablet_ids and table_ids.
  //              table_id is OB_INVALID_ID means that tablet-table history is recycled
  //              or tablet has been dropped.
  int get_tablet_to_table_history(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletID> &tablet_ids,
      const int64_t schema_version,
      common::ObIArray<uint64_t> &table_ids);

  // link table.
  int fetch_link_table_schema(const ObDbLinkSchema *dblink_schema,
                              const common::ObString &database_name,
                              const common::ObString &table_name,
                              common::ObIAllocator &allocator,
                              ObTableSchema *&table_schema,
                              sql::ObSQLSessionInfo *session_info,
                              const ObString &dblink_name,
                              bool is_reverse_link,
                              uint64_t *current_scn);

  // get the latest schema version
  // if core_schema_version = false, return user schema version
  // if core_schema_version = true, and user schema not inited, return core_schema_version
  virtual int get_tenant_refreshed_schema_version(
              const uint64_t tenant_id,
              int64_t &schema_version,
              const bool core_schema_version = false) const;
  virtual int get_tenant_received_broadcast_version(const uint64_t tenant_id, int64_t &schema_version, const bool core_schema_version = false) const;
  virtual int get_last_refreshed_schema_info(ObRefreshSchemaInfo &schema_info);
  virtual int get_recycle_schema_version(const uint64_t tenant_id, int64_t &schema_version);
  int get_baseline_schema_version(
      const uint64_t tenant_id,
      const bool auto_update,
      int64_t &baseline_schema_version);
  int get_new_schema_version(uint64_t tenant_id, int64_t &schema_version);
  int get_tenant_mem_info(const uint64_t &tenant_id, common::ObIArray<ObSchemaMemory> &tenant_mem_infos);
  int get_tenant_slot_info(common::ObIAllocator &allocator, const uint64_t &tenant_id,
                           common::ObIArray<ObSchemaSlot> &tenant_slot_infos);
  int get_schema_store_tenants(common::ObIArray<uint64_t> &tenant_ids);
  bool check_schema_store_tenant_exist(const uint64_t &tenant_id);

  int get_tenant_broadcast_consensus_version(const uint64_t tenant_id,
                                             int64_t &consensus_version);
  int set_tenant_broadcast_consensus_version(const uint64_t tenant_id,
                                             const int64_t consensus_version);
  virtual int set_tenant_received_broadcast_version(const uint64_t tenant_id, const int64_t version);
  virtual int set_last_refreshed_schema_info(const ObRefreshSchemaInfo &schema_info);
  int update_baseline_schema_version(const uint64_t tenant_id, const int64_t baseline_schema_version);
  int gen_new_schema_version(uint64_t tenant_id, int64_t &schema_version);
  // gen schema versions in [start_version, end_version] with specified schema version cnt.
  // @param[out]:
  //  - schema_version: end_version
  int gen_batch_new_schema_versions(
      const uint64_t tenant_id,
      const int64_t version_cnt,
      int64_t &schema_version);
  int get_dropped_tenant_ids(common::ObIArray<uint64_t> &dropped_tenant_ids);
  /*----------- check schema interface -----------------*/
  bool is_sys_full_schema() const;

  bool is_tenant_full_schema(const uint64_t tenant_id) const;

  bool is_tenant_not_refreshed(const uint64_t tenant_id);
  bool is_tenant_refreshed(const uint64_t tenant_id) const;

  // sql should retry when tenant is normal but never refresh schema successfully.
  bool is_schema_error_need_retry(
       ObSchemaGetterGuard *guard,
       const uint64_t tenant_id);

  int check_table_exist(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const common::ObString &table_name,
                        const bool is_index,
                        const int64_t table_schema_version,
                        bool &exist);
  virtual int check_table_exist(const uint64_t tenant_id,
                                const uint64_t table_id,
                                const int64_t table_schema_version,
                                bool &exist);
  int check_database_exist(const uint64_t tenant_id, const common::ObString &database_name,
                           uint64_t &database_id, bool &exist);
  int check_tablegroup_exist(const uint64_t tenant_id, const common::ObString &tablegroup_name,
                             uint64_t &tablegroup_id, bool &exist);
  int check_if_tenant_has_been_dropped(
      const uint64_t tenant_id,
      bool &is_dropped);
  int check_if_tenant_schema_has_been_refreshed(
    const uint64_t tenant_id,
    bool &is_refreshed);
  int check_is_creating_standby_tenant(
    const uint64_t tenant_id,
    bool &is_creating_standby);
  int check_outline_exist_with_name(const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &outline_name,
      uint64_t &outline_id,
      bool &exist) ;
  int check_outline_exist_with_sql(const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &paramlized_sql,
      bool &exist) ;
  int check_synonym_exist(const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &synonym_sql,
      bool &exist,
      uint64_t &synonym_id) ;
  int check_udf_exist(const uint64_t tenant_id,
      const common::ObString &name,
      bool &exist,
      uint64_t &udf_id);
  int check_sequence_exist(const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &name,
      bool &exist);
  int check_outline_exist_with_sql_id(const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &sql_id,
      bool &exist) ;

  int check_procedure_exist(uint64_t tenant_id, uint64_t database_id,
                            const common::ObString &proc_name, bool &exist);
  int check_label_se_policy_column_name_exist(const uint64_t tenant_id, const common::ObString &column_name, bool &is_exist);
  int check_label_se_component_short_name_exist(const uint64_t tenant_id, const uint64_t policy_id,
                                                const int64_t comp_type, const common::ObString &short_name, bool &is_exist);
  int check_label_se_component_long_name_exist(const uint64_t tenant_id, const uint64_t policy_id,
                                               const int64_t comp_type, const common::ObString &long_name, bool &is_exist);

  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       const common::ObString &host_name,
                       bool &exist);
  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       const common::ObString &host_name,
                       uint64_t &user_id,
                       bool &exist);
  int check_user_exist(const uint64_t tenant_id,
                       const uint64_t user_id,
                       bool &exist);

  int check_tenant_is_restore(ObSchemaGetterGuard *schema_guard,
                              const uint64_t tenant_id,
                              bool &is_restore);
  int check_restore_tenant_exist(const common::ObIArray<uint64_t> &tenant_ids, bool &exist);

  int get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode &name_case_mode);
  /*------------- refresh schema interface -----------------*/
  int broadcast_tenant_schema(
      const uint64_t tenant_id,
      const common::ObIArray<share::schema::ObTableSchema> &table_schemas);

  // new schema refresh interface
  int refresh_and_add_schema(const common::ObIArray<uint64_t> &tenant_ids,
                             bool check_bootstrap = false);
  // Trigger an asynchronous refresh task and wait for the refresh result
  int async_refresh_schema(const uint64_t tenant_id,
                           const int64_t schema_version);
  int add_schema(const uint64_t tenant_id, const bool force_add = false);

  int try_eliminate_schema_mgr();

  /* new interface for liboblog */
  int auto_switch_mode_and_refresh_schema(const uint64_t tenant_id,
      const int64_t expected_schema_version = common::OB_INVALID_VERSION);
  int get_schema_version_by_timestamp(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      int64_t timestamp,
      int64_t &schema_version);
  int get_first_trans_end_schema_version(
      const uint64_t tenant_id,
      int64_t &schema_version);
  int cal_purge_need_timeout(
      const obrpc::ObPurgeRecycleBinArg &purge_recyclebin_arg,
      int64_t &cal_timeout);
  ObDDLTransController &get_ddl_trans_controller() { return ddl_trans_controller_; }
  ObDDLEpochMgr &get_ddl_epoch_mgr() { return ddl_epoch_mgr_; }
//this friend class only for backup
friend class tools::ObAgentTaskGenerator;
friend class tools::ObAgentTaskWorker;
friend class ObDDLEpochMgr;
  virtual void stop();
  virtual void wait();
  virtual int destroy();

protected:
  ObMultiVersionSchemaService();
  virtual ~ObMultiVersionSchemaService();

  virtual int publish_schema(const uint64_t tenant_id) override;
  virtual int init_multi_version_schema_struct(const uint64_t tenant_id) override;
  virtual int update_schema_cache(common::ObIArray<ObTableSchema*> &schema_array) override;
  virtual int update_schema_cache(common::ObIArray<ObTableSchema> &schema_array) override;
  virtual int update_schema_cache(const common::ObIArray<ObTenantSchema> &schema_array) override;
  virtual int update_schema_cache(const share::schema::ObSysVariableSchema &schema) override;

private:
  // check inner stat
  bool check_inner_stat() const;

  int init_original_schema();
  int init_sys_tenant_user_schema();

  int refresh_tenant_schema(const uint64_t tenant_id);

  virtual int add_schema_mgr_info(
              ObSchemaGetterGuard &schema_guard,
              ObSchemaStore* schema_store,
              const ObRefreshSchemaStatus &schema_status,
              const uint64_t tenant_id,
              const int64_t snapshot_version,
              const int64_t latest_local_version,
              const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  // gc dropped tenant schema mgr
  int try_gc_tenant_schema_mgr();
  int try_gc_tenant_schema_mgr(uint64_t tenant_id);
  int try_gc_tenant_schema_mgr_for_refresh(uint64_t tenant_id);
  int try_gc_tenant_schema_mgr_for_fallback(uint64_t tenant_id);
  int try_gc_tenant_schema_mgr(ObSchemaMemMgr *&mem_mgr, ObSchemaMgrCache *&schema_mgr_cache);
  int get_gc_candidates(common::hash::ObHashSet<uint64_t> &candidates);
  // gc existed tenant schema mgr
  int try_gc_existed_tenant_schema_mgr();
  // try release exist tenant's another allocator
  int try_gc_another_allocator(const uint64_t tenant_id,
                               ObSchemaMemMgr *&mem_mgr,
                               ObSchemaMgrCache *&schema_mgr_cache);
  // try release slot's schema mgr which is in current allocator and without reference
  int try_gc_current_allocator(const uint64_t tenant_id,
                               ObSchemaMemMgr *&mem_mgr,
                               ObSchemaMgrCache *&schema_mgr_cache);

  int get_schema_status(
      const common::ObArray<ObRefreshSchemaStatus> &schema_status_array,
      const uint64_t tenant_id,
      ObRefreshSchemaStatus &schema_status);

  int batch_fetch_tablet_to_table_history_(
      const uint64_t tenant_id,
      const common::ObIArray<ObTabletID> &tablet_ids,
      const int64_t schema_version,
      const common::ObIArray<int64_t> &tablet_idxs,
      const int64_t start_idx,
      const int64_t end_idx,
      common::hash::ObHashMap<ObTabletID, uint64_t> &tablet_map);

  virtual int get_schema_version_history(
      const ObRefreshSchemaStatus &fetch_schema_status,
      const uint64_t fetch_tenant_id,
      const int64_t schema_version,
      const VersionHisKey &key,
      VersionHisVal &val,
      bool &not_exist);
  virtual int add_aux_schema_from_mgr(const ObSchemaMgr &mgr,
      ObTableSchema &table_schema,
      const ObTableType table_type) override;
  int build_full_materalized_view_schema(
      ObSchemaGetterGuard &schema_guard,
      common::ObIAllocator &allocator,
      ObTableSchema *&view_schema);

  //for liboblog
  int fallback_schema_mgr_for_liboblog(const ObRefreshSchemaStatus &schema_status,
                                       const int64_t target_version,
                                       const int64_t latest_local_version,
                                       const ObSchemaMgr *&schema_mgr,
                                       ObSchemaMgrHandle &handle);
  int put_fallback_liboblog_schema_to_slot(
      ObSchemaMgrCache &schema_mgr_cache_for_liboblog,
      ObSchemaMemMgr &mem_mgr_for_liboblog,
      ObSchemaMgr *&target_mgr,
      ObSchemaMgrHandle &handle);
  int put_fallback_schema_to_slot(ObSchemaMgr *&new_mgr,
                                  ObSchemaMgrCache &schema_mgr_cache,
                                  ObSchemaMemMgr &schema_mem_mgr,
                                  ObSchemaMgrHandle &handle);
  int alloc_and_put_schema_mgr_(ObSchemaMemMgr &mem_mgr,
                                ObSchemaMgr &latest_schema_mgr,
                                ObSchemaMgrCache &schema_mgr_cache);
  int switch_allocator_(ObSchemaMemMgr &mem_mgr,
                        ObSchemaMgr *&latest_schema_mgr);
  inline static bool compare_schema_mgr_info_(const ObSchemaMgr *lhs,
                                              const ObSchemaMgr *rhs);
  int try_gc_allocator_when_add_schema_(const uint64_t tenant_id,
                                        ObSchemaMemMgr *&mem_mgr,
                                        ObSchemaMgrCache *&schema_mgr_cache);
  int cal_purge_table_timeout_(const uint64_t &tenant_id,
                               const uint64_t &table_id,
                               int64_t &cal_table_timeout,
                               int64_t &total_purge_count);
  int cal_purge_database_timeout_(const uint64_t &tenant_id,
                                  const uint64_t &database_id,
                                  int64_t &cal_database_timeout,
                                  int64_t &total_purge_count);

private:
  static const int64_t MAX_VERSION_COUNT = 64;
  static const int64_t MAX_VERSION_COUNT_FOR_LIBOBLOG = 6;
  static const int32_t MAX_RETRY_TIMES = 10;
  static const int64_t RETRY_INTERVAL_US = 1000 * 1000; //1s
  static const int64_t DEFAULT_TENANT_SET_SIZE = 64;
  static const int64_t RESERVE_SCHEMA_MGR_CNT = 10;

  bool init_;
  mutable lib::ObMutex schema_refresh_mutex_;//assert only one thread can refresh schema
  ObSchemaCache schema_cache_;
  ObSchemaMgrCache schema_mgr_cache_;
  ObSchemaMgrCache schema_mgr_cache_for_liboblog_;
  ObSchemaFetcher schema_fetcher_;
  common::SpinRWLock schema_info_rwlock_;
  ObRefreshSchemaInfo last_refreshed_schema_info_;
  int64_t init_version_cnt_;
  int64_t init_version_cnt_for_liboblog_;
  ObSchemaStoreMap schema_store_map_;
  ObDDLTransController ddl_trans_controller_;
  ObDDLEpochMgr ddl_epoch_mgr_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionSchemaService);
};

#define GSCHEMASERVICE (::oceanbase::share::schema::ObMultiVersionSchemaService::get_instance())
}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_MULTI_VERSION_SCHEMS_SERVICE_H_
