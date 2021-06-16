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

#ifndef OCEANBASE_SQL_TEMP_TABLE_
#define OCEANBASE_SQL_TEMP_TABLE_

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace sql {
struct ObTempTableSqcInfo {
public:
  ObTempTableSqcInfo()
      : sqc_id_(0),
        temp_sqc_addr_(),
        interm_result_ids_(),
        min_task_count_(0),
        max_task_count_(0),
        task_count_(0),
        part_count_(0)
  {}
  virtual ~ObTempTableSqcInfo()
  {}

  TO_STRING_KV(K_(sqc_id), K_(temp_sqc_addr), K_(interm_result_ids), K_(min_task_count), K_(max_task_count),
      K_(task_count), K_(part_count));

  uint64_t sqc_id_;
  ObAddr temp_sqc_addr_;
  ObSEArray<uint64_t, 1> interm_result_ids_;
  uint64_t min_task_count_;
  uint64_t max_task_count_;
  uint64_t task_count_;
  uint64_t part_count_;
};

class ObSqlTempTableCtx {
public:
  ObSqlTempTableCtx() : temp_table_id_(0)
  {}
  virtual ~ObSqlTempTableCtx()
  {}

  TO_STRING_KV(K_(temp_table_id), K_(temp_table_infos));

  // private:
  uint64_t temp_table_id_;
  ObSEArray<ObTempTableSqcInfo, 1, common::ModulePageAllocator, true> temp_table_infos_;
};

class ObSqlTempTableInfo {
public:
  ObSqlTempTableInfo() : ref_table_id_(OB_INVALID_ID), table_name_(), table_query_(NULL), table_plan_(NULL)
  {}
  virtual ~ObSqlTempTableInfo()
  {}

  inline static uint64_t generate_temp_table_id()
  {
    int64_t start_id = (common::ObTimeUtility::current_time() / 1000000) << 20;
    static volatile uint64_t sequence = start_id;
    const uint64_t svr_id = GCTX.server_id_;
    return ((ATOMIC_AAF(&sequence, 1) & 0x0000FFFFFFFFFFFF) | (svr_id << 48));
  }
  TO_STRING_KV(K_(ref_table_id), K_(table_name));

public:
  uint64_t ref_table_id_;
  common::ObString table_name_;
  ObSelectStmt* table_query_;
  ObLogicalOperator* table_plan_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_TEMP_TABLE_
