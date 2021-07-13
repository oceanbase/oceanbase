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

#ifndef _OB_LOG_MV_TABLE_SCAN_H
#define _OB_LOG_MV_TABLE_SCAN_H 1

#include "sql/optimizer/ob_log_table_scan.h"

namespace oceanbase {
namespace sql {
class ObLogMVTableScan : public ObLogTableScan {
public:
  ObLogMVTableScan(ObLogPlan& plan);
  virtual ~ObLogMVTableScan()
  {}
  virtual int init_table_location_info();
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int index_back_check();
  virtual uint64_t hash(uint64_t seed) const;

  void set_depend_table_id(uint64_t depend_tid)
  {
    depend_table_id_ = depend_tid;
  }

private:
  ObTablePartitionInfo right_table_partition_info_;
  uint64_t depend_table_id_;
};
}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_LOG_MV_TABLE_SCAN_H */
