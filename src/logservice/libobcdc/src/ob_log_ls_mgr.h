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

#ifndef OCEANBASE_LOG_LS_MGR_H_
#define OCEANBASE_LOG_LS_MGR_H_

#include "lib/lock/ob_thread_cond.h"            // ObThreadCond
#include "ob_log_ls_callback.h"                 // LSCBArray
#include "ob_log_ls_info.h"                     // LSInfoMap

namespace oceanbase
{
namespace libobcdc
{
class ObLogTenant;
class ObLogLSMgr
{
public:
  explicit ObLogLSMgr(ObLogTenant &tenant);
  virtual ~ObLogLSMgr();

  int init(const uint64_t tenant_id,
      const int64_t start_schema_version,
      LSInfoMap &map,
      LSCBArray &ls_add_cb_array,
      LSCBArray &ls_rc_cb_array);
  void reset();

public:
  // Add the SYS LS for this tenant
  // The sys ls must be added successfully, otherwise reported an error
  int add_sys_ls(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const bool is_create_tenant);

  // TODO modify API
  int add_all_ls(
      const common::ObIArray<share::ObLSID> &ls_id_array,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);

  int drop_all_ls();

  int add_ls(
      const logservice::TenantLSID &tls_id,
      const int64_t start_serve_tstamp,
      const bool is_create_ls);

  int offline_ls(const logservice::TenantLSID &tls_id);

  int offline_and_recycle_ls(const logservice::TenantLSID &tls_id);

  void print_ls_info(int64_t &serving_ls_count,
      int64_t &offline_ls_count,
      int64_t &not_served_ls_count);

  /// Check if a ls transaction is being served and if so, increase the number of running transactions on the LS
  /// Supports DDL and DML LS
  ///
  /// @param [out] is_serving           return value, identifies whether the participant transaction is being served
  /// @param [in] tls_id                tenant ls ID
  /// @param [in] commit_log_lsn        commit_log_lsn
  /// @param [in] print_ls_not_serve_info  print not server ls info
  /// @param [in] timeout               timeout time
  ////
  /// @retval OB_SUCCESS                Success
  /// @retval OB_TIMEOUT                timeout
  /// @retval Other return values       Fail
  int inc_ls_trans_count_on_serving(bool &is_serving,
      const logservice::TenantLSID &tls_id,
      const palf::LSN &commit_log_lsn,
      const bool print_ls_not_serve_info,
      const int64_t timeout,
      volatile bool &stop_flag);

  /// Decrement the number of running transactions in a LS
  ///
  /// @param [in] tls_id                tenant ls ID
  ///
  /// @retval OB_SUCCESS                Success
  /// @retval Other return values       Fail
  int dec_ls_trans_count(const logservice::TenantLSID &tls_id);

private:
  static const int64_t PRINT_LOG_INTERVAL = 10 * logfetcher::_SEC_;

  int add_served_ls_(
      const logservice::TenantLSID &tls_id,
      const int64_t start_serve_tstamp,
      const bool is_create_ls,
      bool &add_succ);

  // Check if a LS is served
  // partition by default according to the way of calculating partitions,
  // i.e.: partition tasks by ObLSID
  bool is_ls_served_(const logservice::TenantLSID &tls_id) const;

  // Note: add_ls is not thread-safe
  // The caller has to ensure that adding and deleting LS are executed serially under lock protection
  int add_ls_(
      const logservice::TenantLSID &tls_id,
      const int64_t start_tstamp,
      const bool is_create_ls,
      const bool is_served);

  // Pre-check before adding a LS
  int add_served_ls_pre_check_(const logservice::TenantLSID &tls_id);

  // call callbacks of add-ls
  int call_add_ls_callbacks_(
      const logservice::TenantLSID &tls_id,
      const int64_t start_tstamp,
      const palf::LSN &start_lsn);

  // ensure_recycled_when_offlined: Whether recycling is guaranteed to be successful when LS going offline,
  // i.e. whether the number of transactions left behind is 0
  int offline_ls_(const logservice::TenantLSID &tls_id,
      const bool ensure_recycled_when_offlined = false);
  int recycle_ls_(const logservice::TenantLSID &tls_id,
      ObLogLSInfo *info);
  // Notify the modules that the LS is ready for recycling
  int call_recycle_ls_callbacks_(const logservice::TenantLSID &tls_id);

  /// Check if the partition is serviced, if so, increase the number of partition statements
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  int inc_trans_count_on_serving_(bool &is_serving,
      const logservice::TenantLSID &tls_id,
      const bool print_ls_not_serve_info);

private:
  ObLogTenant       &host_;

  bool              is_inited_;
  uint64_t          tenant_id_;
  LSInfoMap         *map_;

  LSCBArray         *ls_add_cb_array_;
  LSCBArray         *ls_rc_cb_array_;

  // Conditional
  common::ObThreadCond   schema_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogLSMgr);
};


} // namespace libobcdc
} // namespace oceanbase

#endif
