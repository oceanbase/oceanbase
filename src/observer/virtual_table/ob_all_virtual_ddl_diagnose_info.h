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

#ifndef OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_
#define OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_ddl_common.h"
#include "storage/ob_i_store.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"
#include "observer/virtual_table/ob_all_virtual_diag_index_scan.h"

namespace oceanbase
{
namespace observer
{
struct ObDDLDiagnoseValue final
{
public:
  ObDDLDiagnoseValue()
    :tenant_id_(0), object_id_(0), ddl_task_id_(0), execution_id_(-1), start_time_(0), finish_time_(0)
  {
    op_name_[0] = '\0';
  }
  ~ObDDLDiagnoseValue() = default;
  TO_STRING_KV(K_(tenant_id), K_(object_id), K_(ddl_task_id), K_(start_time), K_(finish_time), K_(op_name));
  void reset()
  {
    tenant_id_ = 0;
    object_id_ = 0;
    ddl_task_id_ = 0;
    execution_id_ = -1;
    start_time_ = 0;
    finish_time_ = 0;
    op_name_[0] = '\0';
  }
  bool inline is_valid()
  {
    return op_name_ != nullptr && ddl_task_id_ > 0 && tenant_id_ != OB_INVALID_ID;
  }

public:
  uint64_t tenant_id_;
  uint64_t object_id_;
  share::ObDDLType ddl_type_;
  char op_name_[common::MAX_LONG_OPS_NAME_LENGTH];
  uint64_t ddl_task_id_;
  int64_t execution_id_;
  int64_t start_time_;
  int64_t finish_time_;
};

class ObAllVirtualDDLDiagnoseInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDDLDiagnoseInfo()
    : is_inited_(false), sql_proxy_(nullptr), ddl_scan_result_(), ddl_scan_idx_(0), diagnose_info_(),
      sql_monitor_stats_collector_(), diagnose_values_(), idx_(0), value_(), pos_(0)
  {}
  virtual ~ObAllVirtualDDLDiagnoseInfo() = default;
  int init(ObMySQLProxy *sql_proxy);
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

protected:
  int collect_ddl_info(const ObSqlString &scan_sql);
  static const int64_t MAX_SQL_MONITOR_BATCH_SIZE = 300;

private:
  virtual int process();
  int get_next_diagnose_info_row();
  int collect_task_info();
  int fill_cells();
  int collect_task_gmt_create_time();
  enum DDLDiagnoseInfoColumn
  {
    TENANT_ID,
    DDL_TASK_ID,
    OBJECT_ID,
    OP_NAME,
    CREATE_TIME,
    FINISH_TIME,
    DIAGNOSE_INFO,
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLDiagnoseInfo);
  bool is_inited_;
  ObMySQLProxy *sql_proxy_;
  ObArray<observer::ObDDLDiagnoseValue> ddl_scan_result_;
  uint64_t ddl_scan_idx_;
  share::ObDDLDiagnoseInfo diagnose_info_;
  share::ObSqlMonitorStatsCollector sql_monitor_stats_collector_;
  ObArray<observer::ObDDLDiagnoseValue> diagnose_values_;
  int64_t idx_;
  ObDDLDiagnoseValue value_;
  char message_[common::OB_DIAGNOSE_INFO_LENGTH];
  int64_t pos_;
};

class ObAllVirtualDDLDiagnoseInfoI1 : public ObAllVirtualDDLDiagnoseInfo, public ObAllVirtualDiagIndexScan
{
public:
  ObAllVirtualDDLDiagnoseInfoI1() = default;
  virtual ~ObAllVirtualDDLDiagnoseInfoI1() = default;
  virtual int inner_open() override;
protected:
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLDiagnoseInfoI1);
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_
