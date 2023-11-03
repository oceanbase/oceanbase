/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_DDL_SIM_POINT_STAT_H_
#define OB_ALL_VIRTUAL_DDL_SIM_POINT_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_ddl_sim_point.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualDDLSimPoint : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDDLSimPoint() : point_idx_(0) {}
  virtual ~ObAllVirtualDDLSimPoint() {}
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum DDLSimPointColumn
  {
    SIM_POINT_ID = common::OB_APP_MIN_COLUMN_ID,
    SIM_POINT_NAME,
    SIM_POINT_DESC,
    SIM_POINT_ACTION,
  };

private:
  int64_t point_idx_;
  char action_str_[1024];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLSimPoint);
};

class ObAllVirtualDDLSimPointStat : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDDLSimPointStat() : is_inited_(false), idx_(0) {}
  virtual ~ObAllVirtualDDLSimPointStat() {}
  int init(const common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum DDLSimPointStatColumn
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    DDL_TASK_ID,
    SIM_POINT_ID,
    TRIGGER_COUNT,
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLSimPointStat);
  bool is_inited_;
  common::ObAddr addr_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  int64_t idx_;
  ObArray<share::ObDDLSimPointMgr::TaskSimPoint> task_sim_points_;
  ObArray<int64_t> sim_counts_;
};

} // namespace observer
} // namespace oceanbase


#endif
