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

#ifndef OCEANBASE_LIBOBLOG_TENANT_MGR_H__
#define OCEANBASE_LIBOBLOG_TENANT_MGR_H__

#include "lib/hash/ob_hashset.h"                // ObHashSet
#include "lib/container/ob_se_array.h"          // ObSEArray
#include "lib/string/ob_string.h"               // ObString

#include "ob_log_tenant.h"                      // ObLogTenant, ObLogTenantGuard
#include "ob_log_part_callback.h"               // PartAddCallback, PartRecycleCallback, PartCBArray
#include "ob_log_part_info.h"                   // PartInfoMap
#include "ob_log_table_id_cache.h"              // GIndexCache, TableIDCache

namespace oceanbase
{
namespace common
{
class ObPartitionKey;
}

namespace liboblog
{
class IObLogSchemaGetter;
class IObLogTimeZoneInfoGetter;

class IObLogTenantMgr
{
public:
  IObLogTenantMgr() {}
  virtual ~IObLogTenantMgr() {}

public:
  // add all tenants
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  // @retval OB_TIMEOUT                   timeout
  // @retval other error code             fail
  virtual int add_all_tenants(const int64_t start_tstamp,
      const int64_t sys_schema_version,
      const int64_t timeout) = 0;

  // add tenant
  // @param old_sys_schema_version sys_schema_version before ADD_TENANT DDL operate success
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  // @retval OB_TIMEOUT                   timeout
  // @retval other error code             fail
  virtual int add_tenant(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_serve_tstamp,
      const int64_t sys_schema_version,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout,
      bool &add_tenant_succ) = 0;

  /// Perform a tenant deletion operation, only if the tenant deletion conditions are met
  ///
  /// @param tenant_id          TenantID
  /// @param call_from          caller info
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other error code  Fail
  virtual int drop_tenant(const uint64_t tenant_id, const char *call_from) = 0;

  /// handle DEL_TENANT_START DDL
  virtual int drop_tenant_start(const uint64_t tenant_id, const int64_t drop_tenant_start_tstamp) = 0;

  /// handle DEL_TENANT_END DDL
  virtual int drop_tenant_end(const uint64_t tenant_id, const int64_t drop_tenant_end_tstamp) = 0;

  virtual int alter_tenant_name(const uint64_t tenant_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t timeout,
      const char *&tenant_name,
      bool &tenant_is_chosen) = 0;

  /// Sync only ddl statements from whitelisted tenants, filter ddl statements from other tenants
  ///
  /// @param [in]  tenant_id    TenantID
  /// @param [out] chosen       Returned matching results
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other error code  Fail
  virtual int filter_ddl_stmt(const uint64_t tenant_id, bool &chosen) = 0;

  /// get all Tenant ID
  /// TODO: Removing std::vector dependencies
  ///
  /// @param [out]  tenant_ids  returned tenant ids
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other error code  Fail
  virtual int get_all_tenant_ids(std::vector<uint64_t> &tenant_ids) = 0;
  virtual bool is_inited() = 0;

  /// Get the specified Tenant ID tz_info_wrap, called by ObObj2strHelper, where the tenant does not exist and an error is required
  ///
  /// @param [in]  tenant_id    TenantID
  /// @param [out] tz_info_wrap Timezone info
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other error code  Fail
  virtual int get_tenant_tz_wrap(const uint64_t tenant_id, common::ObTimeZoneInfoWrap *&tz_info_wrap) = 0;

  /// get tz_info_map, ObLogTimeZoneInfoGetter with specified Tenant ID
  ///
  /// @param [in]  tenant_id    TenantID
  /// @param [out] tz_info_wrap Timezone info
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval other error code  Fail
  virtual int get_tenant_tz_map(const uint64_t tenant_id,
      common::ObTZInfoMap *&tz_info_map) = 0;

  /// Handling schema split end DDL
  virtual int handle_schema_split_finish(
      const uint64_t ddl_tenant_id,
      const int64_t new_schema_version,
      const int64_t start_serve_tstamp,
      const int64_t timeout) = 0;

  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST Tenant not exist
  // @retval other error code   Fail
  virtual int get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard) = 0;

  // 基于tenant id获取对应ObLogTenant
  //
  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST Tenant not exist
  // @retval other error code   Fail
  virtual int get_tenant(const uint64_t tenant_id, ObLogTenant *&tenant) = 0;
  // revert tenant
  virtual int revert_tenant(ObLogTenant *tenant) = 0;

  virtual int get_ddl_progress(uint64_t &tenant_id,
      int64_t &ddl_min_progress,
      uint64_t &ddl_min_handle_log_id) = 0;

  virtual void print_stat_info() = 0;

  virtual int register_part_add_callback(PartAddCallback *callback) = 0;
  virtual int register_part_recycle_callback(PartRecycleCallback *callback) = 0;

  // Recycle the partition structure
  // This interface is called when the partition OFFLINE task is processed and the partition OFFLINE task is received, indicating that all data for the partition has been processed.
  virtual int recycle_partition(const common::ObPartitionKey &pkey) = 0;

  // Set the same starting schema version for all tenants
  virtual int set_data_start_schema_version_for_all_tenant(const int64_t version) = 0;

  // set the start schema version for a specific tenant in split mode
  virtual int set_data_start_schema_version_on_split_mode() = 0;

};

typedef common::ObLinkHashMap<TenantID, ObLogTenant> TenantHashMap;
class ObLogTenantMgr : public IObLogTenantMgr
{
  friend ObLogTenant;
public:
  ObLogTenantMgr();
  virtual ~ObLogTenantMgr();

public:
  int init(const bool enable_oracle_mode_match_case_sensitive);
  void destroy();

  int register_part_add_callback(PartAddCallback *callback);
  int register_part_recycle_callback(PartRecycleCallback *callback);

  int add_all_tenants(const int64_t start_tstamp,
      const int64_t sys_schema_version,
      const int64_t timeout);

  // add tenant
  int add_tenant(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_serve_tstamp,
      const int64_t sys_schema_version,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout,
      bool &add_tenant_succ);
  // drop tenant
  int drop_tenant(const uint64_t tenant_id, const char *call_from);
  int drop_tenant_start(const uint64_t tenant_id, const int64_t drop_tenant_start_tstamp);
  int drop_tenant_end(const uint64_t tenant_id, const int64_t drop_tenant_end_tstamp);
  // alter tenant name
  int alter_tenant_name(const uint64_t tenant_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t timeout,
      const char *&tenant_name,
      bool &tenant_is_chosen);

  int recycle_partition(const common::ObPartitionKey &pkey);

  int filter_ddl_stmt(const uint64_t tenant_id, bool &chosen);
  int filter_tenant(const char *tenant_name, bool &chosen);
  int handle_schema_split_finish(
      const uint64_t ddl_tenant_id,
      const int64_t new_schema_version,
      const int64_t start_serve_tstamp,
      const int64_t timeout);
  int get_all_tenant_ids(std::vector<uint64_t> &tenant_ids);

  virtual int get_tenant_tz_wrap(const uint64_t tenant_id,
      common::ObTimeZoneInfoWrap *&tz_info_wrap);
  virtual int get_tenant_tz_map(const uint64_t tenant_id,
      common::ObTZInfoMap *&tz_info_map);

  // Get the corresponding ObLogTenant based on tenant id
  int get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard);
  int get_tenant(const uint64_t tenant_id, ObLogTenant *&tenant);
  int revert_tenant(ObLogTenant *tenant);

  int get_ddl_progress(uint64_t &tenant_id,
      int64_t &ddl_min_progress,
      uint64_t &ddl_min_handle_log_id);
  virtual bool is_inited() { return inited_; }
  void print_stat_info();

  template <typename Func> int for_each_tenant(Func &func)
  {
    return tenant_hash_map_.for_each(func);
  }
  // Set the same starting schema version for all tenants
  int set_data_start_schema_version_for_all_tenant(const int64_t version);

  int set_data_start_schema_version_on_split_mode();
private:
  static const int64_t DATA_OP_TIMEOUT = 1 * _SEC_;
  static const int64_t DEFAULT_TENANT_SET_SIZE = 64;
  static const int64_t CACHED_PART_INFO_COUNT = 1 << 10;
  static const int64_t PART_INFO_BLOCK_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

  typedef common::hash::ObHashSet<uint64_t> TenantIDSet;

  struct TenantUpdateStartSchemaFunc
  {
    TenantUpdateStartSchemaFunc() { };
    bool operator()(const TenantID &tid, ObLogTenant *tenant);
  };

  struct TenantDDLProgessGetter
  {
    TenantDDLProgessGetter();
    bool operator()(const TenantID &tid, ObLogTenant *tenant);

    uint64_t tenant_id_;
    int64_t ddl_min_progress_;
    uint64_t ddl_min_handle_log_id_;
  };

  struct TenantPrinter
  {
    int64_t serving_tenant_count_;
    int64_t offline_tenant_count_;
    std::vector<uint64_t> tenant_ids_;
    std::vector<void *> cf_handles_;

    TenantPrinter() : serving_tenant_count_(0), offline_tenant_count_(0), cf_handles_() {}
    bool operator()(const TenantID &tid, ObLogTenant *tenant);
  };

  struct TenantAddDDLTableFunc
  {
    int err_;
    int64_t succ_tenant_count_;
    int64_t offline_tenant_count_;
    int64_t ddl_table_start_serve_tstamp_;
    int64_t ddl_table_start_schema_version_;

    TenantAddDDLTableFunc(const int64_t start_serve_tstamp, const int64_t start_schema_version) :
        err_(OB_SUCCESS),
        succ_tenant_count_(0),
        offline_tenant_count_(0),
        ddl_table_start_serve_tstamp_(start_serve_tstamp),
        ddl_table_start_schema_version_(start_schema_version)
    {}
    bool operator()(const TenantID &tid, ObLogTenant *tenant);
  };

  struct SetDataStartSchemaVersionFunc
  {
    int64_t data_start_schema_version_;

    explicit SetDataStartSchemaVersionFunc(const int64_t data_start_schema_version) :
        data_start_schema_version_(data_start_schema_version)
    {}
    bool operator()(const TenantID &tid, ObLogTenant *tenant);
  };

private:
  void revert_tenant_(ObLogTenant *tenant);
  int add_served_tenant_for_stat_(const char *tenant_name, const uint64_t tenant_id);
  int add_served_tenant_into_set_(const char *tenant_name, const uint64_t tenant_id);
  int get_tenant_start_schema_version_(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_serve_tstamp,
      const int64_t sys_schema_version,
      int64_t &tenant_schema_version,
      const int64_t timeout);
  int get_first_schema_version_of_tenant_(const uint64_t tenant_id,
      const int64_t sys_schema_version,
      IObLogSchemaGetter &schema_getter,
      int64_t &first_schema_version,
      const int64_t timeout);
  int add_ddl_table_if_needed_(const uint64_t tenant_id,
      ObLogTenant &tenant,
      const int64_t start_serve_tstamp,
      const int64_t tenant_start_schema_version,
      const bool is_new_created_tenant);
  int add_inner_tables_on_backup_mode_(const uint64_t tenant_id,
      ObLogTenant &tenant,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);
  int do_add_tenant_(const uint64_t tenant_id,
      const char *tenant_name,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const bool is_tenant_served,
      const int64_t start_serve_tstamp,
      const int64_t sys_schema_version,
      const int64_t timeout);
  int drop_served_tenant_for_stat_(const uint64_t tenant_id);
  int drop_served_tenant_from_set_(const uint64_t tenant_id);
  int remove_tenant_(const uint64_t tenant_id, ObLogTenant *tenant);
  int start_tenant_service_(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_serve_tstamp,
      const int64_t tenant_start_schema_version,
      const int64_t timeout);
private:
  bool              inited_;

  TenantHashMap     tenant_hash_map_;

  // Structures shared by all tenants
  PartInfoMap       part_info_map_;     // partition info map
  GIndexCache       gindex_cache_;      // cache of global index
  TableIDCache      table_id_cache_;

  // callback
  PartCBArray       part_add_cb_array_;
  PartCBArray       part_rc_cb_array_;

  TenantIDSet       tenant_id_set_;

  bool              enable_oracle_mode_match_case_sensitive_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTenantMgr);
};

}
}
#endif
