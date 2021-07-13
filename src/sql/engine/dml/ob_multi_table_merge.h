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

#ifndef OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_H_
#define OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_H_

#include "sql/engine/dml/ob_table_merge.h"

namespace oceanbase {
namespace sql {

class ObMultiTableMergeInput : public ObTableModifyInput {
  friend class ObMultiTableMerge;

public:
  ObMultiTableMergeInput() : ObTableModifyInput()
  {}
  virtual ~ObMultiTableMergeInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_MULTI_TABLE_MERGE;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiTableMergeInput);
};

class ObMultiTableMerge : public ObTableMerge, public ObMultiDMLInfo {
  class ObMultiTableMergeCtx;

public:
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t DML_OP_CNT = 2;

public:
  explicit ObMultiTableMerge(common::ObIAllocator& alloc);
  virtual ~ObMultiTableMerge();
  virtual bool is_multi_dml() const
  {
    return true;
  }

protected:
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;

  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int shuffle_merge_row(ObExecContext& ctx, bool& got_row) const;
  int process_update(share::schema::ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, ObExprCtx& expr_ctx,
      const ObNewRow* child_row, bool& got_row) const;
  int update_row(share::schema::ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, bool need_delete) const;
  int process_insert(share::schema::ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, ObExprCtx& expr_ctx,
      const ObNewRow* child_row, bool& got_row) const;
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_H_ */
