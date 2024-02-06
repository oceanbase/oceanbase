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
 *
 * Tenant Manager for OBCDC(ObLogTenantMgr)
 */

#ifndef OCEANBASE_LIBOBCDC_TENANT_MGR_H__
#define OCEANBASE_LIBOBCDC_TENANT_MGR_H__

#include "lib/hash/ob_hashset.h"                // ObHashSet
#include "lib/container/ob_se_array.h"          // ObSEArray
#include "lib/string/ob_string.h"               // ObString

#include "ob_log_tenant.h"                      // ObLogTenant, ObLogTenantGuard
#include "ob_log_ls_callback.h"                 // LSAddCallback, LSRecycleCallback, LSCBArray
#include "ob_log_ls_info.h"                     // LSInfoMap
#include "ob_log_table_id_cache.h"              // GIndexCache, TableIDCache
#include "ob_log_ls_getter.h"                   // ObLogLsGetter
#include "ob_log_meta_data_refresh_mode.h"      // RefreshMode

namespace oceanbase
{
namespace libobcdc
{
class IObLogSchemaGetter;

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
  virtual int add_all_tenants(const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const int64_t timeout) = 0;

  // add tenant when fetch archive log in direct mode
  // @param old_sys_schema_version sys_schema_version before ADD_TENANT DDL operate success
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_TIMEOUT                   timeout
  // @retval other error code             fail
  virtual int add_tenant(
      const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const char *&tenant_name,
      const int64_t timeout,
      bool &add_tenant_succ) = 0;

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
      const int64_t start_tstamp_ns,
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

  virtual int get_sys_ls_progress(uint64_t &tenant_id,
      int64_t &sys_ls_min_progress,
      palf::LSN &sys_ls_min_handle_log_lsn) = 0;

  virtual void print_stat_info() = 0;

  virtual int register_ls_add_callback(LSAddCallback *callback) = 0;
  virtual int register_ls_recycle_callback(LSRecycleCallback *callback) = 0;

  // Recycle the LS structure
  // This interface is called when the LS OFFLINE task is processed and the LS OFFLINE task is received, indicating that all data for the LS has been processed.
  virtual int recycle_ls(const logservice::TenantLSID &tls_id) = 0;

  // Set the same starting schema version for all tenants
  virtual int set_data_start_schema_version_for_all_tenant(const int64_t version) = 0;

  // set the start schema version for a specific tenant in split mode
  virtual int set_data_start_schema_version_on_split_mode() = 0;

  // regist the tenant start serve schema_version and commit_version for new created tenant
  virtual int regist_add_tenant_start_ddl_info(
      const uint64_t tenant_id,
      const int64_t start_schema_version,
      const int64_t start_commit_version) = 0;
  // advance tenant output_checkpoint by global_heartbeat to at least target_checkpoint.
  virtual int update_committer_global_heartbeat(const int64_t global_heartbeat) = 0;
  // get min output_checkpoint among all tenant in obcdc.
  virtual int get_min_output_checkpoint_for_all_tenant(int64_t &min_output_checkpoint) = 0;

};

typedef common::ObLinkHashMap<TenantID, ObLogTenant> TenantHashMap;
class ObLogTenantMgr : public IObLogTenantMgr
{
  friend ObLogTenant;
public:
  ObLogTenantMgr();
  virtual ~ObLogTenantMgr();

public:
  int init(const bool enable_oracle_mode_match_case_sensitive, const RefreshMode &refresh_mode);
  void destroy();

  int register_ls_add_callback(LSAddCallback *callback);
  int register_ls_recycle_callback(LSRecycleCallback *callback);

  int add_all_tenants(const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const int64_t timeout);

  int add_tenant(
      const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const char *&tenant_name,
      const int64_t timeout,
      bool &add_tenant_succ);

  // add tenant
  int add_tenant(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_tstamp_ns,
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

  int recycle_ls(const logservice::TenantLSID &tls_id);

  int filter_ddl_stmt(const uint64_t tenant_id, bool &chosen);
  int filter_tenant(const char *tenant_name, bool &chosen);
  int get_all_tenant_ids(std::vector<uint64_t> &tenant_ids);

  // Get the corresponding ObLogTenant based on tenant id
  int get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard);
  int get_tenant(const uint64_t tenant_id, ObLogTenant *&tenant);
  int revert_tenant(ObLogTenant *tenant);

  int get_sys_ls_progress(uint64_t &tenant_id,
      int64_t &sys_ls_min_progress,
      palf::LSN &sys_ls_min_handle_log_lsn);
  virtual bool is_inited() { return inited_; }
  void print_stat_info();

  template <typename Func> int for_each_tenant(Func &func)
  {
    return tenant_hash_map_.for_each(func);
  }
  // Set the same starting schema version for all tenants
  int set_data_start_schema_version_for_all_tenant(const int64_t version);

  int set_data_start_schema_version_on_split_mode();

  int regist_add_tenant_start_ddl_info(
      const uint64_t tenant_id,
      const int64_t start_schema_version,
      const int64_t start_commit_version) override;
  // advance tenant output_checkpoint by global_heartbeat to at least target_checkpoint.
  int update_committer_global_heartbeat(const int64_t global_heartbeat);
  int get_min_output_checkpoint_for_all_tenant(int64_t &min_output_checkpoint) override;

private:
  static const int64_t DATA_OP_TIMEOUT = 1 * _SEC_;
  static const int64_t DEFAULT_TENANT_SET_SIZE = 64;
  static const int64_t CACHED_LS_INFO_COUNT = 1 << 10;
  static const int64_t LS_INFO_BLOCK_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

  typedef common::hash::ObHashSet<uint64_t> TenantIDSet;

  struct AddTenantStartDDlInfo : common::LinkHashValue<TenantID>
  {
    AddTenantStartDDlInfo()
      :  schema_version_(OB_INVALID_VERSION), commit_version_(OB_INVALID_VERSION) {}
    ~AddTenantStartDDlInfo() {}
    void reset()
    {
      schema_version_ = OB_INVALID_VERSION;
      commit_version_ = OB_INVALID_VERSION;
    }

    void reset(const int64_t start_schema_version, const int64_t start_commit_version)
    {
      schema_version_ = start_schema_version;
      commit_version_ = start_commit_version;
    }

    bool is_valid() const
    {
      return OB_INVALID_VERSION != schema_version_ && OB_INVALID_VERSION != commit_version_;
    }

    TO_STRING_KV(K_(schema_version), K_(commit_version));

    int64_t schema_version_; // add_tenant_start_ddl schema_version
    int64_t commit_version_; // add_tenant_start_ddl commit_version
  };

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
    int64_t sys_ls_min_progress_;
    palf::LSN sys_ls_min_handle_log_lsn_;
  };

  // operator to advance tenant output_checkpoint by global_heartbeat
  struct GlobalHeartbeatUpdateFunc
  {
    GlobalHeartbeatUpdateFunc(const int64_t global_heartbeat)
      : global_heartbeat_(global_heartbeat) {}

    bool operator()(const TenantID &tid, ObLogTenant *tenant);

    int64_t global_heartbeat_;
  };

  // operator to get min_output_checkpoint among all tenant.
  struct TenantOutputCheckpointGetter
  {
    TenantOutputCheckpointGetter()
      : min_output_checkpoint_(OB_INVALID_VERSION),
        min_output_checkpoint_tenant_(OB_INVALID_TENANT_ID),
        max_output_checkpoint_(OB_INVALID_VERSION),
        max_output_checkpoint_tenant_(OB_INVALID_TENANT_ID) {}
    bool operator()(const TenantID &tid, ObLogTenant *tenant);

    int64_t min_output_checkpoint_;
    uint64_t min_output_checkpoint_tenant_;
    int64_t max_output_checkpoint_;
    uint64_t max_output_checkpoint_tenant_;
  };

  struct TenantPrinter
  {
    int64_t serving_tenant_count_;
    int64_t offline_tenant_count_;
    std::vector<uint64_t> tenant_ids_;
    std::vector<void *> cf_handles_;
    std::vector<void *> lob_storage_cf_handles_;

    TenantPrinter() : serving_tenant_count_(0), offline_tenant_count_(0), cf_handles_(), lob_storage_cf_handles_() {}
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
  int get_tenant_ids_(
      const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const int64_t timeout,
      common::ObIArray<uint64_t> &tenant_id_list);
  void revert_tenant_(ObLogTenant *tenant);
  int add_served_tenant_for_stat_(const char *tenant_name, const uint64_t tenant_id);
  int add_served_tenant_into_set_(const char *tenant_name, const uint64_t tenant_id);
  int get_tenant_start_schema_version_(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_tstamp_usec,
      const int64_t sys_schema_version,
      int64_t &tenant_schema_version,
      const int64_t timeout);
  int get_first_schema_version_of_tenant_(const uint64_t tenant_id,
      const int64_t sys_schema_version,
      IObLogSchemaGetter &schema_getter,
      int64_t &first_schema_version,
      const int64_t timeout);
  int get_tenant_start_serve_timestamp_(
      const uint64_t tenant_id,
      const bool is_new_created_tenant,
      int64_t &tenant_start_tstamp_ns,
      bool &use_add_tenant_start_ddl_commit_version);
  int add_sys_ls_if_needed_(const uint64_t tenant_id,
      ObLogTenant &tenant,
      const int64_t start_tstamp_ns,
      const int64_t tenant_start_schema_version,
      const bool is_new_created_tenant);
  int do_add_tenant_(const uint64_t tenant_id,
      const char *tenant_name,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const bool is_tenant_served,
      const int64_t start_tstamp_ns,
      const int64_t sys_schema_version,
      const int64_t timeout);
  int drop_served_tenant_for_stat_(const uint64_t tenant_id);
  int drop_served_tenant_from_set_(const uint64_t tenant_id);
  int remove_tenant_(const uint64_t tenant_id, ObLogTenant *tenant);
  int start_tenant_service_(const uint64_t tenant_id,
      const bool is_new_created_tenant,
      const bool is_new_tenant_by_restore,
      const int64_t start_tstamp_ns,
      const int64_t tenant_start_schema_version,
      const int64_t timeout);
  int get_add_tenant_start_ddl_info_(
      const uint64_t tenant_id,
      int64_t &start_commit_version);
  int get_min_add_tenant_start_ddl_commit_version_(int64_t &commit_version);
  void try_del_tenant_start_ddl_info_(const uint64_t tenant_id);
private:
  bool                inited_;
  RefreshMode         refresh_mode_;

  TenantHashMap       tenant_hash_map_;
  common::ObLinkHashMap<TenantID, AddTenantStartDDlInfo> add_tenant_start_ddl_info_map_;

  // Structures shared by all tenants
  LSInfoMap           ls_info_map_;       // LS Info Map
  GIndexCache         gindex_cache_;      // cache of global index
  TableIDCache        table_id_cache_;

  // LS callback
  LSCBArray           ls_add_cb_array_;
  LSCBArray           ls_rc_cb_array_;

  TenantIDSet         tenant_id_set_;
  ObLogLsGetter       ls_getter_;

  bool                enable_oracle_mode_match_case_sensitive_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTenantMgr);
};

}
}
#endif
