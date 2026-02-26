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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_MVIEW_RUNNING_JOB_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_MVIEW_RUNNING_JOB_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
class ObScanner;
}
namespace observer
{
class ObAllVirtualTenantMviewRunningJob : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualTenantMviewRunningJob();
  virtual ~ObAllVirtualTenantMviewRunningJob();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  enum TENANT_MVIEW_RUNNING_JOB_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    JOB_TYPE,
    SESSION_ID,
    READ_SNAPSHOT,
    PARALLEL,
    JOB_START_TIME,
    TARGET_DATA_SYNC_SCN,
  };
  class FillScanner
  {
  public:
    FillScanner():
      ip_buf_(),
      port_(0),
      effective_tenant_id_(OB_INVALID_TENANT_ID),
      scanner_(NULL),
      cur_row_(NULL),
      output_column_ids_() {}
    virtual ~FillScanner() {}
    int operator()(common::hash::HashMapPair<transaction::ObTransID, storage::ObMViewOpArg> &entry);
    int init(uint64_t effective_tenant_id,
             common::ObScanner *scanner,
             common::ObNewRow *cur_row,
             const ObIArray<uint64_t> &column_ids);
    void reset();
  private:
      char ip_buf_[common::OB_IP_STR_BUFF];
      int32_t port_;
      uint64_t effective_tenant_id_;
      common::ObScanner *scanner_;
      common::ObNewRow *cur_row_;
      ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
      DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantMviewRunningJob);
};
}//observer
}//oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_ */
