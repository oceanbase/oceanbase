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

#ifndef OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_H_
#define OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/container/ob_array.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualDagWarningHistory : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TASK_ID,
    MODULE,
    TYPE,
    RET,
    STATUS,
    GMT_CREATE,
    GMT_MODIFIED,
    RETRY_CNT,
    WARNING_INFO,
  };
  ObAllVirtualDagWarningHistory();
  virtual ~ObAllVirtualDagWarningHistory();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells(share::ObDagWarningInfo &dag_warning_info);
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override
  {
    dag_warning_info_iter_.reset();
  }
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char task_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  share::ObDagWarningInfo dag_warning_info_;
  compaction::ObIDiagnoseInfoMgr::Iterator dag_warning_info_iter_;
  bool is_inited_;
  char comment_[common::OB_DAG_WARNING_INFO_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDagWarningHistory);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
