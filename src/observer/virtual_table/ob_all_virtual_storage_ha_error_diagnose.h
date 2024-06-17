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

#ifndef OB_ALL_VIRTUAL_STORAGE_HA_ERROR_DIAGNOSE_H_
#define OB_ALL_VIRTUAL_STORAGE_HA_ERROR_DIAGNOSE_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualStorageHAErrorDiagnose : public common::ObVirtualTableScannerIterator,
                                           public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    MODULE,
    TYPE,
    TASK_ID,
    SVR_IP,
    SVR_PORT,
    RETRY_ID,
    CREATE_TIME,
    RESULT_CODE,
    RESULT_MSG,
    INFO,
  };
  ObAllVirtualStorageHAErrorDiagnose();
  virtual ~ObAllVirtualStorageHAErrorDiagnose();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int get_info_from_type_(ObStorageHADiagInfo *&info, ObTransferErrorDiagInfo &transfer_err_diag);

private:
  // Filter out tenants that need to be processed
  virtual bool is_need_process(uint64_t tenant_id) override;
  // Process tenants for the current iteration
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // Release the resources of the previous tenant
  virtual void release_last_tenant() override;

private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObStorageHADiagInfo *info_;
  ObStorageHADiagIterator iter_;
  char info_str_[common::OB_DIAGNOSE_INFO_LENGTH];
  char task_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualStorageHAErrorDiagnose);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
