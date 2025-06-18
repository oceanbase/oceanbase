/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_OB_SS_GC_STATUS_H_
#define OB_ALL_VIRTUAL_OB_SS_GC_STATUS_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/garbage_collector/ob_ss_garbage_collector_define.h"
#endif
namespace oceanbase
{
namespace observer
{
class ObAllVirtualSSGCStatus : public common::ObVirtualTableScannerIterator, public omt::ObMultiTenantOperator
{
  static const int64_t ROWKEY_COL_COUNT = 2;

public:
  ObAllVirtualSSGCStatus();
  virtual ~ObAllVirtualSSGCStatus();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    GC_TYPE,
    LAST_SUCC_SCN,
    EXTRA_INFO,
    SVR_IP,
    SVR_PORT
  };

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
#ifdef OB_BUILD_SHARED_STORAGE
  int prepare_start_to_read();
  int get_next_last_succ_scn(LastSuccSCN &last_succ_scn);
#endif
private:
  char gc_type_buf_[OB_SS_GC_TASK_TYPE_LENGTH];
  char extra_info_buf_[MAX_VALUE_LENGTH];
  char ip_buffer_[MAX_IP_ADDR_LENGTH];
  bool is_for_sslog_table_;
#ifdef OB_BUILD_SHARED_STORAGE
  SSGCLastSuccSCNIter last_succ_scn_iter_;
  share::SCN last_succ_scn_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSGCStatus);
};
}  // namespace observer
}  // namespace oceanbase
#endif
