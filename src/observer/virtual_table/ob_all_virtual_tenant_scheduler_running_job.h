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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_sql_session_mgr.h"
namespace oceanbase
{
namespace common
{
class ObNewRow;
class ObScanner;
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObAllVirtualTenantSchedulerRunningJob : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualTenantSchedulerRunningJob();
  virtual ~ObAllVirtualTenantSchedulerRunningJob();
  inline void set_session_mgr(sql::ObSQLSessionMgr *session_mgr) { session_mgr_ = session_mgr; }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  enum TENANT_SCHEDULER_RUNNING_JOB_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    OWNER,
    JOB_NAME,
    JOB_SUBNAME,
    JOB_STYLE,
    DETACHED,
    SESSION_ID,
    SLAVE_PROCESS_ID,
    SLAVE_OS_PROCESS_ID,
    RESOURCE_CONSUMER_GROUP,
    RUNNING_INSTANCE,
    ELAPSED_TIME,
    CPU_USED,
    DESTINATION_OWNER,
    DESTINATION,
    CREDENTIAL_OWNER,
    CREDENTIAL_NAME
  };
  class FillScanner
  {
  public:
    FillScanner()
        :ip_buf_(),
        port_(0),
        effective_tenant_id_(OB_INVALID_TENANT_ID),
        scanner_(NULL),
        cur_row_(NULL),
        output_column_ids_() {}
    virtual ~FillScanner(){}
    int operator()(common::hash::HashMapPair<uint64_t, sql::ObSQLSessionInfo *> &entry);
    int init(uint64_t effective_tenant_id,
             common::ObScanner *scanner,
             common::ObNewRow *cur_row,
             const ObIArray<uint64_t> &column_ids);
    inline void reset();
  private:
      char ip_buf_[common::OB_IP_STR_BUFF];
      int32_t port_;
      uint64_t effective_tenant_id_;
      common::ObScanner *scanner_;
      common::ObNewRow *cur_row_;
      ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
      DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };
  sql::ObSQLSessionMgr *session_mgr_;
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantSchedulerRunningJob);
};
}//observer
}//oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_ */
