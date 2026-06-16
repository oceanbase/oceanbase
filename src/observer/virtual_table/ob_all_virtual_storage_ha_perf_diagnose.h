/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_STORAGE_HA_PERF_DIAGNOSE_H_
#define OB_ALL_VIRTUAL_STORAGE_HA_PERF_DIAGNOSE_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/storage/ob_ha_inflight_diag.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualStorageHAPerfDiagnose : public common::ObVirtualTableScannerIterator,
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
    START_TIMESTAMP,
    END_TIMESTAMP,
    TABLET_ID,
    TABLET_COUNT,
    RESULT_CODE,
    RESULT_MSG,
    INFO,
  };
  ObAllVirtualStorageHAPerfDiagnose();
  virtual ~ObAllVirtualStorageHAPerfDiagnose();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  static constexpr int64_t OB_STORAGE_HA_DIAGNOSE_INFO_LENGTH = (1 << 12);// 4k

private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char info_str_[OB_STORAGE_HA_DIAGNOSE_INFO_LENGTH];
  char task_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  share::ObHAInflightVirtualIter inflight_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualStorageHAPerfDiagnose);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
