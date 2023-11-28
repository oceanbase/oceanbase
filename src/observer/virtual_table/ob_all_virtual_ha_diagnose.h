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

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_HA_DIAGNOSE_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_HA_DIAGNOSE_H_

#include "common/row/ob_row.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace observer
{
enum IOStatColumn
{
  TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
  LS_ID,
  SVR_IP,
  SVR_PORT,
  ELECTION_ROLE,
  ELECTION_EPOCH,
  PALF_ROLE,
  PALF_STATE,
  PALF_PROPOSAL_ID,
  LOG_HANDLER_ROLE,
  LOG_HANDLER_PROPOSAL_ID,
  LOG_HANDLER_TAKEOVER_STATE,
  LOG_HANDLER_TAKEOVER_LOG_TYPE,
  MAX_APPLIED_SCN,
  MAX_REPALYED_LSN,
  MAX_REPLAYED_SCN,
  REPLAY_DIAGNOSE_INFO,
  GC_STATE,
  GC_START_TS,
  ARCHIVE_SCN,
  CHECKPOINT_SCN,
  MIN_REC_SCN,
  MIN_REC_SCN_LOG_TYPE,
  RESTORE_HANDLER_ROLE,
  RESTORE_HANDLER_PROPOSAL_ID,
  RESTORE_CONTEXT_INFO,
  RESTORE_ERR_CONTEXT_INFO,
  ENABLE_SYNC,
  ENABLE_VOTE,
  ARB_SRV_INFO,
  PARENT,
};

class ObAllVirtualHADiagnose : public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualHADiagnose(omt::ObMultiTenant *omt) : omt_(omt) {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int insert_stat_(storage::DiagnoseInfo &diagnose_info);
private:
  static const int64_t VARCHAR_32 = 32;
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char election_role_str_[VARCHAR_32] = {'\0'};
  char palf_role_str_[VARCHAR_32] = {'\0'};
  char log_handler_role_str_[VARCHAR_32] = {'\0'};
  char log_handler_takeover_state_str_[VARCHAR_32] = {'\0'};
  char log_handler_takeover_log_type_str_[VARCHAR_32] = {'\0'};
  char gc_state_str_[VARCHAR_32] = {'\0'};
  char min_rec_log_scn_log_type_str_[VARCHAR_32] = {'\0'};
  char restore_handler_role_str_[VARCHAR_32] = {'\0'};
  char parent_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  omt::ObMultiTenant *omt_;
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_HA_DIAGNOSE_H_ */