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

namespace oceanbase {
namespace common {}
namespace tools {
class ObAgentTaskGenerator;
class ObAgentTaskWorker;
}  // namespace tools
namespace share {
namespace schema {

static const int64_t MAX_CACHED_VERSION_NUM = 4;
// singleton class
// concurrency control for schema manager constructing tasks.
class ObSchemaConstructTask {
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
  bool exist(int64_t id)
  {
    return -1 != get_idx(id);
  }
  void add(int64_t id);
  void remove(int64_t id);
  int64_t count()
  {
    return schema_tasks_.count();
  }
  void wait(const int64_t version);
  void wakeup(const int64_t version);

private:
  common::ObArray<int64_t> schema_tasks_;
  pthread_mutex_t schema_mutex_;
  pthread_cond_t schema_cond_;
};

class ObSchemaVersionUpdater {
public:
  ObSchemaVersionUpdater(int64_t new_schema_version, bool ignore_error = true)
      : new_schema_version_(new_schema_version), ignore_error_(ignore_error){};
  virtual ~ObSchemaVersionUpdater(){};
  int operator()(int64_t& version)
  {
    int ret = common::OB_SUCCESS;
    if (version < new_schema_version_) {
      version = new_schema_version_;
    } else {
      ret = ignore_error_ ? common::OB_SUCCESS : common::OB_OLD_SCHEMA_VERSION;
    }
    return ret;
  }

  int operator()(common::hash::HashMapPair<uint64_t, int64_t>& entry)
  {
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
class ObMultiVersionSchemaService : public ObServerSchemaService {
public:
  static bool g_skip_resolve_materialized_view_definition_;

  enum RefreshSchemaMode { NORMAL = 0, FORCE_FALLBACK, FORCE_LAZY };
  const char* print_refresh_schema_mode(const RefreshSchemaMode mode);

public:
  static ObMultiVersionSchemaService& get_instance();

  // init the newest system or user table schema
  int init(common::ObMySQLProxy* proxy, common::ObDbLinkProxy* dblink_proxy, const common::ObCommonConfig* config,
      const int64_t init_version_count, const int64_t init_version_count_for_liboblog, const bool with_timestamp);

  int init_sys_schema(const common::ObIArray<ObTableSchema>& table_schemas);

public:
  //--------ddl funcs for RS module ---------//
  // check table exist
  int check_table_exist(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index, const int64_t table_schema_version, bool& exist);
  virtual int check_table_exist(const uint64_t table_id, const int64_t table_schema_version, bool& exist);
  int check_database_exist(
      const uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id, bool& exist);
  int check_tablegroup_exist(
      const uint64_t tenant_id, const common::ObString& tablegroup_name, uint64_t& tablegroup_id, bool& exist);
  int check_partition_exist(
      const uint64_t table_id, const int64_t phy_partition_id, const bool check_dropped_partition, bool& is_exist);
  int check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool& is_dropped);
  int check_outline_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& outline_name, uint64_t& outline_id, bool& exist);
  int check_outline_exist_with_sql(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& paramlized_sql, bool& exist);
  int check_synonym_exist(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& synonym_sql,
      bool& exist, uint64_t& synonym_id);
  int check_udf_exist(const uint64_t tenant_id, const common::ObString& name, bool& exist);
  int check_sequence_exist(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& name, bool& exist);
  int check_outline_exist_with_sql_id(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& sql_id, bool& exist);

  int check_procedure_exist(uint64_t tenant_id, uint64_t database_id, const common::ObString& proc_name, bool& exist);

  // check user exist
  int check_user_exist(
      const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name, bool& exist);
  int check_user_exist(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      uint64_t& user_id, bool& exist);
  int check_user_exist(const uint64_t tenant_id, const uint64_t user_id, bool& exist);
  // get table schema info from local schema manager
  int get_database_schema(const uint64_t database_id, const ObDatabaseSchema*& db_schema);

  int construct_recycle_objects(
      ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id, common::ObIArray<ObRecycleObject>& recycle_objects);

  //-----access schema related funcs---------//
  // note: only for compatible, deprecated after schema split !!!
  int refresh_and_add_schema(int64_t expected_version = -1);
  // new schema refresh interface
  int refresh_and_add_schema(
      const common::ObIArray<uint64_t>& tenant_ids, bool use_new_mode = true, bool check_bootstrap = false);
  // Trigger an asynchronous refresh task and wait for the refresh result
  int async_refresh_schema(const uint64_t tenant_id, const int64_t schema_version);
  int add_schema(const uint64_t tenant_id, const bool force_add = false);
  // partition server that not includes master rs module will invoke this func
  int refresh_new_schema(int64_t& table_count);  // refresh schema incrementally

  // add a new schema, del the oldest-version and not used schema if fullfilled
  int add_schema(const bool force_add = false);

  // note: only for compatible, deprecated after schema split !!!
  virtual void set_response_notify_schema_ts(NewVersionType type);
  // note: only for compatible, deprecated after schema split !!!
  virtual int64_t get_response_notify_schema_ts();
  // int fetch_tables(const int64_t schema_version,
  //                 const uint64_t tenant_id, common::ObIArray<ObSimpleTableSchemaV2> &schema_array,
  //                 const SchemaKey *schema_keys, const int64_t schema_key_size);

  int try_eliminate_schema_mgr();
  virtual bool is_tenant_full_schema(const uint64_t tenant_id) const override;
  int gen_new_schema_version(uint64_t tenant_id, int64_t& schema_version);
  int get_new_schema_version(uint64_t tenant_id, int64_t& schema_version);

  /* new interface for liboblog */
  int auto_switch_mode_and_refresh_schema(
      const uint64_t tenant_id, const int64_t expected_schema_version = common::OB_INVALID_VERSION);
  int get_schema_version_by_timestamp(
      const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, int64_t timestamp, int64_t& schema_version);
  int get_first_trans_end_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  int load_split_schema_version(int64_t& split_schema_version);

  void dump_schema_statistics();

  int check_tenant_is_restore(ObSchemaGetterGuard* schema_guard, const uint64_t tenant_id, bool& is_restore);
  int check_restore_tenant_exist(const common::ObIArray<uint64_t>& tenant_ids, bool& exist);

public:
  static const int64_t MAX_INT64_VALUE = ((((uint64_t)1) << 63) - 1);  // MAX INT64 VALUE

  // get the latest schema version
  // if core_schema_version = false, return user schema version
  // if core_schema_version = true, and user schema not inited, return core_schema_version

  virtual int get_tenant_refreshed_schema_version(const uint64_t tenant_id, int64_t& schema_version,
      const bool core_schema_version = false, const bool use_new_mode = false) const;
  virtual int get_tenant_received_broadcast_version(
      const uint64_t tenant_id, int64_t& schema_version, const bool core_schema_version = false) const;
  virtual int set_tenant_received_broadcast_version(const uint64_t tenant_id, const int64_t version);
  // note: only for compatible, deprecated after schema split !!!
  virtual inline int64_t get_refreshed_schema_version(const bool core_schema_version = false) const;
  // note: only for compatible, deprecated after schema split !!!
  virtual inline int64_t get_received_broadcast_version(const bool core_schema_version = false) const;
  // note: only for compatible, deprecated after schema split !!!
  virtual inline int set_received_broadcast_version(const int64_t version);

  virtual int get_last_refreshed_schema_info(ObRefreshSchemaInfo& schema_info);
  virtual int set_last_refreshed_schema_info(const ObRefreshSchemaInfo& schema_info);

  virtual int get_recycle_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  // this friend class only for backup
  friend class tools::ObAgentTaskGenerator;
  friend class tools::ObAgentTaskWorker;

protected:
  ObMultiVersionSchemaService();
  virtual ~ObMultiVersionSchemaService();
  virtual int destroy() override;

  virtual int publish_schema() override;
  virtual int publish_schema(const uint64_t tenant_id) override;

private:
  // check inner stat
  bool check_inner_stat() const;
  int init_schema_manager();
  int init_original_schema();

  // find a new pos for the new version schema
  int64_t find_replace_pos(const bool for_fallback = false) const;

  //-----------For managing privileges-----------
  template <class T>
  void free(T* p);

  virtual int init_multi_version_schema_struct(const uint64_t tenant_id) override;

  // get old schema_guard
  virtual int get_inner_schema_guard(ObSchemaGetterGuard& guard, int64_t schema_version = common::OB_INVALID_VERSION,
      const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);
  int refresh_tenant_schema(const uint64_t tenant_id);
  virtual int add_schema_mgr_info(ObSchemaGetterGuard& schema_guard, ObSchemaStore* schema_store,
      const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, const int64_t snapshot_version,
      const int64_t latest_local_version, const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  // gc dropped tenant schema mgr
  int try_gc_tenant_schema_mgr();
  int try_gc_tenant_schema_mgr(uint64_t tenant_id);
  int try_gc_tenant_schema_mgr_for_refresh(uint64_t tenant_id);
  int try_gc_tenant_schema_mgr_for_fallback(uint64_t tenant_id, bool use_new_mode = true);
  int try_gc_tenant_schema_mgr(ObSchemaMemMgr*& mem_mgr, ObSchemaMgrCache*& schema_mgr_cache);
  int get_gc_candidates(common::hash::ObHashSet<uint64_t>& candidates);

  // gc existed tenant schema mgr
  int try_gc_existed_tenant_schema_mgr();
  // try release exist tenant's another allocator
  int try_gc_another_allocator(const uint64_t tenant_id, ObSchemaMemMgr*& mem_mgr, ObSchemaMgrCache*& schema_mgr_cache);

  int get_schema_status(const common::ObArray<ObRefreshSchemaStatus>& schema_status_array, const uint64_t tenant_id,
      ObRefreshSchemaStatus& schema_status);

private:
  static const int64_t MAX_VERSION_COUNT = 64;
  static const int64_t MAX_VERSION_COUNT_FOR_LIBOBLOG = 6;
  static const int32_t MAX_RETRY_TIMES = 10;
  static const int64_t RETRY_INTERVAL_US = 1000 * 1000;  // 1s
  static const int64_t DEFAULT_TENANT_SET_SIZE = 64;
  bool init_;
  // If the flashing is successful, it will be updated to the updated version number;
  // if the notification version is consistent with the flashed version, it will be updated to the newly generated
  // version number.
  int64_t response_notify_schema_ts_;
  // Version number brushed
  int64_t refreshed_schema_version_;
  // The version number of the RS notification or heartbeat packet received
  int64_t received_broadcast_version_;
  mutable lib::ObMutex schema_refresh_mutex_;  // assert only one thread can refresh schema

  bool with_timestamp_;

public:
  // schema_cache related
  int init_sys_tenant_user_schema();

  int update_schema_cache(common::ObIArray<ObTableSchema*>& schema_array, const bool is_force = false) override;
  int update_schema_cache(common::ObIArray<ObTableSchema>& schema_array, const bool is_force = false) override;
  int update_schema_cache(const common::ObIArray<ObTenantSchema>& schema_array) override;
  int update_schema_cache(const share::schema::ObSysVariableSchema& schema) override;
  int build_full_materalized_view_schema(
      ObSchemaGetterGuard& schema_guard, common::ObIAllocator& allocator, ObTableSchema*& view_schema);
  virtual int get_schema(const ObSchemaMgr* mgr, const ObRefreshSchemaStatus& schema_status,
      const ObSchemaType schema_type, const uint64_t schema_id, const int64_t schema_version,
      common::ObKVCacheHandle& handle, const ObSchema*& schema);
  virtual int get_schema_version_history(const ObRefreshSchemaStatus& fetch_schema_status,
      const uint64_t fetch_tenant_id, const int64_t schema_version, const VersionHisKey& key, VersionHisVal& val,
      bool& not_exist);
  virtual int add_aux_schema_from_mgr(
      const ObSchemaMgr& mgr, ObTableSchema& table_schema, const ObTableType table_type);
  int make_tenant_space_schema(common::ObIAllocator& allocator, const uint64_t tenant_id,
      const ObSimpleTableSchemaV2& src_table, ObSimpleTableSchemaV2*& dst_table);
  int make_tenant_space_schema(common::ObIAllocator& allocator, const uint64_t tenant_id,
      const ObTableSchema& src_table, ObTableSchema*& dst_table);
  // Only used to get the schema of all tenants that have been refreshed on this machine
  virtual int get_schema_guard(
      ObSchemaGetterGuard& guard, const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  // After the schema is split, for user tenants, the system table schema is referenced from the system tenant,
  // so when the guard is taken, the schema_version of the system tenant must be additionally passed in.
  // 1. tenant_schema_version is the schema_version of the corresponding tenant
  // 2. sys_schema_version is the schema_version of the system tenant. For system tenants,
  //  the value will be reset to tenant_schema_version
  virtual int get_tenant_schema_guard(const uint64_t tenant_id, ObSchemaGetterGuard& guard,
      int64_t tenant_schema_version = common::OB_INVALID_VERSION,
      int64_t sys_schema_version = common::OB_INVALID_VERSION,
      const RefreshSchemaMode refresh_schema_mode = RefreshSchemaMode::NORMAL);

  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);

  virtual int get_tenant_full_schema_guard(
      const uint64_t tenant_id, ObSchemaGetterGuard& guard, bool check_formal = true);
  // retry get schema guard will retry 10 times, it's interval is 100ms
  int retry_get_schema_guard(
      const int64_t schema_version, const uint64_t table_id, ObSchemaGetterGuard& guard, int64_t& save_schema_version);
  // for liboblog
  int fallback_schema_mgr_for_liboblog(const ObRefreshSchemaStatus& schema_status, const int64_t target_version,
      const int64_t latest_local_version, const ObSchemaMgr*& schema_mgr, ObSchemaMgrHandle& handle);
  int put_fallback_liboblog_schema_to_slot(ObSchemaMgrCache& schema_mgr_cache_for_liboblog,
      ObSchemaMemMgr& mem_mgr_for_liboblog, ObSchemaMgr*& target_mgr, ObSchemaMgrHandle& handle);
  int put_fallback_schema_to_slot(ObSchemaMgr*& new_mgr, ObSchemaMgrCache& schema_mgr_cache,
      ObSchemaMemMgr& schema_mem_mgr, ObSchemaMgrHandle& handle);
  int alloc_schema_mgr_for_liboblog(ObSchemaMemMgr& mem_mgr_for_liboblog, ObSchemaMgr*& schema_mgr);
  int free_schema_mgr_for_liboblog(ObSchemaMemMgr& mem_mgr_for_liboblog, ObSchemaMgr* schema_mgr);

  // link table.
  int fetch_link_table_schema(const ObDbLinkSchema& dblink_schema, const common::ObString& database_name,
      const common::ObString& table_name, common::ObIAllocator& allocator, ObTableSchema*& table_schema);

private:
  ObSchemaCache schema_cache_;
  ObSchemaMgrCache schema_mgr_cache_;
  ObSchemaMgrCache schema_mgr_cache_for_liboblog_;
  ObSchemaFetcher schema_fetcher_;

private:
  common::SpinRWLock schema_info_rwlock_;
  ObRefreshSchemaInfo last_refreshed_schema_info_;
  int64_t init_version_cnt_;
  int64_t init_version_cnt_for_liboblog_;

  ObSchemaStoreMap schema_store_map_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionSchemaService);
};

// without user_schemas_rwlock_ protecting, new schema is fetched with protecting
inline int64_t ObMultiVersionSchemaService::get_refreshed_schema_version(const bool core_version) const
{
  int64_t refreshed_schema_version = ATOMIC_LOAD(&refreshed_schema_version_);
  return (!core_version || refreshed_schema_version > 0) ? refreshed_schema_version : OB_CORE_SCHEMA_VERSION;
}

// without user_schemas_rwlock_ protecting, new schema is fetched with protecting
inline int64_t ObMultiVersionSchemaService::get_received_broadcast_version(const bool core_schema_version) const
{
  int64_t received_broadcast_version = ATOMIC_LOAD(&received_broadcast_version_);
  return (!core_schema_version || received_broadcast_version > 0) ? received_broadcast_version : OB_CORE_SCHEMA_VERSION;
}

// update latest db version only when new version is received from ddl server after DDL op.
// without user_schemas_rwlock_ protecting.
inline int ObMultiVersionSchemaService::set_received_broadcast_version(const int64_t version)
{
  int ret = common::OB_SUCCESS;
  if (version != OB_CORE_SCHEMA_VERSION) {
    int64_t received_broadcast_version = -1;
    bool matched = false;
    do {
      received_broadcast_version = ATOMIC_LOAD(&received_broadcast_version_);
      if (received_broadcast_version > version) {
        ret = common::OB_OLD_SCHEMA_VERSION;
        matched = true;
      }
    } while (common::OB_SUCCESS == ret && !matched &&
             !ATOMIC_BCAS(&received_broadcast_version_, received_broadcast_version, version));
  } else {
    ret = common::OB_OLD_SCHEMA_VERSION;
  }

  return ret;
}

template <class T>
void ObMultiVersionSchemaService::free(T* p)
{
  p->~T();
  BACKTRACE(INFO, true, "free schema[%p]", p);
  p = NULL;
}
#define GSCHEMASERVICE (::oceanbase::share::schema::ObMultiVersionSchemaService::get_instance())
}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_MULTI_VERSION_SCHEMS_SERVICE_H_
