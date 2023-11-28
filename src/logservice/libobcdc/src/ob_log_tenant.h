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
 * Tenant Data Struct in OBCDC
 */

#ifndef OCEANBASE_LIBOBCDC_TENANT_H__
#define OCEANBASE_LIBOBCDC_TENANT_H__

#include "lib/atomic/ob_atomic.h"                   // ATOMIC_LOAD
#include "lib/atomic/atomic128.h"                   // ATOMIC_LOAD

#include "ob_log_all_ddl_operation_schema_info.h"   // ObLogAllDdlOperationSchemaInfo
#include "ob_log_tenant_task_queue.h"               // ObLogTenantTaskQueue
#include "ob_log_part_mgr.h"                        // ObLogPartMgr
#include "ob_log_ls_mgr.h"                          // ObLogLSMgr
#include "ob_log_ref_state.h"                       // RefState
#include "ob_cdc_lob_aux_meta_storager.h"           // ObCDCLobAuxDataCleanTask
#include "ob_log_meta_data_struct.h"                // ObDictTenantInfo
#include <cstdint>

namespace oceanbase
{
namespace libobcdc
{

//////////////////////// TENANT STRUCTURE ////////////////////////

class ObLogTenantMgr;

struct TenantID
{
  uint64_t tenant_id_;

  TenantID() : tenant_id_(OB_INVALID_TENANT_ID) {}

  TenantID(const uint64_t tenant_id) : tenant_id_(tenant_id)
  {}

  int64_t hash() const
  {
    return static_cast<int64_t>(tenant_id_);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  int compare(const TenantID &other) const
  {
    int cmp_ret = 0;

    if (tenant_id_ > other.tenant_id_) {
      cmp_ret = 1;
    } else if (tenant_id_ < other.tenant_id_) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }

    return cmp_ret;
  }

  bool operator== (const TenantID &other) const { return 0 == compare(other); }
  bool operator!=(const TenantID &other) const { return !operator==(other); }
  bool operator<(const TenantID &other) const { return -1 == compare(other); }

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
  }

  TO_STRING_KV(K_(tenant_id));
};

typedef common::LinkHashValue<TenantID> TenantValue;
class ObLogTenant : public TenantValue
{
  static const int64_t DATA_OP_TIMEOUT = 1 * _SEC_;
  static const int64_t PRINT_INTERVAL = 10 * _SEC_;
public:
  ObLogTenant();
  ~ObLogTenant();

public:
  int init(const uint64_t tenant_id,
    const char *tenant_name,
    const int64_t start_tstamp_ns,
    const int64_t start_seq,
    const int64_t start_schema_version,
    void *cf_handle,
    void *lob_storage_cf_handle,
    ObLogTenantMgr &tenant_mgr);
  void reset();
  bool is_inited() const { return inited_; }

  // get/set functions
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  lib::Worker::CompatMode get_compat_mode() const { return compat_mode_; }
  int64_t get_start_schema_version() const { return start_schema_version_; }
  int64_t get_schema_version() const { return part_mgr_.get_schema_version(); }
  int64_t get_sys_ls_progress() const { return ATOMIC_LOAD(&sys_ls_progress_); }
  const palf::LSN &get_handle_log_lsn() const { return ddl_log_lsn_; }
  const ObLogAllDdlOperationSchemaInfo &get_all_ddl_operation_schema_info() const
  { return all_ddl_operation_table_schema_info_; }

  ObLogTenantTaskQueue *get_task_queue() { return task_queue_; }
  int64_t get_committer_output_checkpoint() const
  {
    return std::max(
        ATOMIC_LOAD(&committer_trans_commit_version_),
        ATOMIC_LOAD(&committer_global_heartbeat_));
  }
  int update_committer_trans_commit_version(const int64_t trans_commit_version);
  int update_committer_global_heartbeat(const int64_t global_heartbeat);
  int64_t get_committer_cur_schema_version() const { return committer_cur_schema_version_; }
  void update_committer_cur_schema_version(const int64_t ddl_schema_version)
  {
    committer_cur_schema_version_ = std::max(committer_cur_schema_version_, ddl_schema_version);
  }
  int64_t get_committer_next_trans_schema_version() const { return ATOMIC_LOAD(&committer_next_trans_schema_version_); }
  int update_committer_next_trans_schema_version(int64_t schema_version);
  ObLogLSMgr &get_ls_mgr() { return ls_mgr_; }
  IObLogPartMgr &get_part_mgr() { return part_mgr_; }
  int64_t get_global_schema_version() const { return global_seq_and_schema_version_.hi; }
  int64_t get_global_seq() const { return global_seq_and_schema_version_.lo; }
  void *get_redo_storage_cf_handle() { return redo_cf_handle_; }
  void *get_lob_storage_cf_handle() { return lob_storage_cf_handle_; }
  ObCDCLobAuxDataCleanTask& get_lob_storage_clean_task() { return lob_storage_clean_task_; }

public:
  // Print statistics
  void print_stat_info();

  // Update sys ls-related information
  //
  // @retval OB_SUCCESS         Success
  // @retval other_error_code   Fail
  int update_sys_ls_info(const PartTransTask &task);

  // Assigning global transaction serial numbers and schema versions to DDL tasks
  int alloc_global_trans_seq_and_schema_version_for_ddl(
      const int64_t base_schema_version,
      int64_t &new_seq,
      int64_t &new_schema_version,
      const int64_t timeout);

  int alloc_global_trans_seq_and_schema_version(const int64_t base_schema_version,
      int64_t &new_seq,
      int64_t &new_schema_version,
      volatile bool &stop_flag);

  int alloc_global_trans_schema_version(const bool is_ddl_trans,
      const int64_t base_schema_version,
      int64_t &new_schema_version);

  // Delete a tenant
  // Supports multiple calls, guaranteeing only one will return tenant_can_be_dropped = true
  int drop_tenant(bool &tenant_can_be_dropped, const char *call_from);

  // Increase the number of LS when a tenant is in service
  int inc_ls_count_on_serving(const logservice::TenantLSID &tls_id, bool &is_serving);

  /// Recycle LS
  /// This task is called when processing a LS offline task, if this task is received,
  /// it means that all the LS data has been consumed, there will be no more data and
  /// the tenant can safely subtract the number of LS served
  ///
  /// @param [in]   tls_ld        Target recycled LS
  /// @param [out]  tenant_can_be_dropped tenant can be dropped or not
  ///
  /// @retval OB_SUCCESS         Success
  /// @retval other_error_code   Fail
  int recycle_ls(const logservice::TenantLSID &tls_id, bool &tenant_can_be_dropped);

  /// mark start of drop tenant
  ///
  /// @param drop_tenant_start_tstamp The start timestamp for deleting a tenant means that the tenant can be deleted as soon as progress crosses that timestamp
  ///
  /// @retval OB_SUCCESS         Success
  /// @retval other_error_code   Fail
  int mark_drop_tenant_start(const int64_t drop_tenant_start_tstamp);

  /// Whether the tenant has been marked for deletion
  bool is_drop_tenant_start_marked() const
  {
    return OB_INVALID_TIMESTAMP != drop_tenant_tstamp_;
  }

  void update_global_data_schema_version(const int64_t schema_version);

  int update_data_start_schema_version_on_split_mode();

  /// Adding a SYS LS requires that the tenant must be served
  /// Report an error if the tenant is not in service
  ///
  ///
  /// @retval OB_STATE_NOT_MATCH  tenant not serve
  ///
  /// @retval Other error codes follow PartMgr error codes
  int add_sys_ls(
      const int64_t start_tstamp_ns,
      const int64_t start_schema_version,
      const bool is_create_tenant);

  /// Add all tables and require the tenant to be in service
  ///
  /// @retval OB_STATE_NOT_MATCH  tenant not serve
  ///
  /// @retval Other error codes follow PartMgr error codes
  int add_all_ls(
      const common::ObIArray<share::ObLSID> &ls_id_array,
      const int64_t start_tstamp_ns,
      const int64_t start_schema_version,
      const int64_t timeout);

  int get_table_info_of_tablet(const common::ObTabletID &tablet_id, ObCDCTableInfo &table_info) const
  {
    return part_mgr_.get_table_info_of_tablet_id(tablet_id, table_info);
  }

  int add_all_user_tablets_and_tables_info(const int64_t timeout)
  {
    return part_mgr_.add_all_user_tablets_and_tables_info(timeout);
  }

  int add_all_user_tablets_and_tables_info(
      ObDictTenantInfo *tenant_info,
      const ObIArray<const datadict::ObDictTableMeta *> &table_metas,
      const int64_t timeout)
  {
    return part_mgr_.add_all_user_tablets_and_tables_info(tenant_info, table_metas, timeout);
  }

  // flush memory data in local storage(e.g. memtable for rocksdb)
  void flush_storage();

  // compact data in local storage
  // NOTE: LOB data will compact by resource_collector with lob_aux_meta_storager
  void compact_storage();

public:
  enum
  {
    TENANT_STATE_INVALID = 0,
    TENANT_STATE_NORMAL = 1,
    TENANT_STATE_OFFLINE = 2,
  };

  static const char *print_state(const int64_t state);
  int64_t get_tenant_state() const { return tenant_state_.state_; }
  int64_t get_active_ls_count() const { return tenant_state_.ref_cnt_; }
  bool is_offlined() const { return TENANT_STATE_OFFLINE == get_tenant_state(); }
  bool is_serving() const { return TENANT_STATE_NORMAL == get_tenant_state(); }

private:
  int init_compat_mode_(const uint64_t tenant_id, const int64_t start_schema_version,
      lib::Worker::CompatMode &compat_mode);
  int init_all_ddl_operation_table_schema_info_();
  int update_sys_ls_progress_(
      const int64_t handle_progress,
      const palf::LSN &handle_log_lsn);
  int start_drop_tenant_if_needed_(bool &need_drop_tenant);
  bool need_drop_tenant_() const;
  int drop_sys_ls_();

public:
  TO_STRING_KV(
      K_(tenant_id),
      K_(tenant_name),
      "state", print_state(get_tenant_state()),
      "active_ls_count", get_active_ls_count(),
      "sys_ls_progress", NTS_TO_STR(sys_ls_progress_),
      "drop_tenant_tstamp", TS_TO_STR(drop_tenant_tstamp_),
      K_(ddl_log_lsn),
      "global_seq", get_global_seq(),
      "global_schema_version", get_global_schema_version(),
      K_(start_schema_version),
      K_(committer_trans_commit_version),
      K_(committer_global_heartbeat),
      //"cur_schema_version", part_mgr_.get_schema_version(),
      K_(committer_cur_schema_version),
      K_(committer_next_trans_schema_version),
      KPC_(task_queue));

private:
  bool                    inited_;
  uint64_t                tenant_id_;
  char                    tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH + 1];
  lib::Worker::CompatMode compat_mode_;
  int64_t                 start_schema_version_;
  // task queue
  ObLogTenantTaskQueue    *task_queue_;

  // LS manager
  ObLogLSMgr              ls_mgr_;
  // PartMgr
  ObLogPartMgr            part_mgr_;

  // status of tenant
  // => state variable + ref count
  //
  // The reference count represents the number of active partitions, if the number of active partitions is 0, it means there is no data dependency on the tenant structure
  RefState                tenant_state_  CACHE_ALIGNED;

  // Progress of the last DDL processing
  int64_t                 sys_ls_progress_ CACHE_ALIGNED;
  palf::LSN               ddl_log_lsn_;

  // Tenant __all_ddl_operation TableSchema
  ObLogAllDdlOperationSchemaInfo all_ddl_operation_table_schema_info_;

  // Timestamp for deleting the tenant
  // Indicates that the tenant can be safely deleted as soon as the progress crosses that timestamp, and there will be no further DDL afterwards
  int64_t                 drop_tenant_tstamp_;

  // sequencer
  // Low 64 bits: global serial number; High 64 bits: global Schema version number
  types::uint128_t        global_seq_and_schema_version_ CACHE_ALIGNED;

  // Committer
  // use max value of committer_trans_commit_version_ and committer_global_heartbeat_ as
  // tenant_output_checkpoint, in case of issues caused by advancing output_checkpoint concurrently
  // by committer_trans_commit_version and committer_global_heartbeat
  int64_t                 committer_trans_commit_version_ CACHE_ALIGNED;
  int64_t                 committer_global_heartbeat_ CACHE_ALIGNED;
  int64_t                 committer_cur_schema_version_ CACHE_ALIGNED;      // The current advanced schema version

  // Committer is currently a tenant parallel commit model:
  // Transaction data and DDL data need to be matched for consumption, where the global_schema_version of the current transaction is recorded, which is used by the DDL to determine if it needs to be consumed.
  int64_t                 committer_next_trans_schema_version_ CACHE_ALIGNED;

  void                       *redo_cf_handle_;
  void                       *lob_storage_cf_handle_;
  ObCDCLobAuxDataCleanTask   lob_storage_clean_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTenant);
};

//////////////////////////// ObLogTenantGuard /////////////////////////
class ObLogTenantGuard final
{
public:
  ObLogTenantGuard() : tenant_(NULL) {}
  ~ObLogTenantGuard() { revert_tenant(); }
public:
  ObLogTenant *get_tenant() { return tenant_; }
  void set_tenant(ObLogTenant *tenant) { tenant_ = tenant; }

  TO_STRING_KV(KPC_(tenant));
private:
  void revert_tenant();
private:
  ObLogTenant *tenant_;
  DISALLOW_COPY_AND_ASSIGN(ObLogTenantGuard);
};
}
}
#endif
