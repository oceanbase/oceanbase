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

#ifndef SQL_ENGINE_DML_OB_MULTI_TABLE_UPDATE_H_
#define SQL_ENGINE_DML_OB_MULTI_TABLE_UPDATE_H_
#include "sql/engine/dml/ob_table_update.h"
namespace oceanbase {
namespace sql {
class ObMultiPartUpdate : public ObTableModify, public ObMultiDMLInfo {
  class ObMultiPartUpdateCtx;

public:
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t UPDATE_OP = 2;
  static const int64_t DML_OP_CNT = 3;  // 3 dml type for multi table update
public:
  explicit ObMultiPartUpdate(common::ObIAllocator& allocator);
  virtual ~ObMultiPartUpdate();

  virtual int create_operator_input(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual bool has_foreign_key() const override
  {
    return subplan_has_foreign_key();
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }

private:
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const;

  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int shuffle_update_row(ObExecContext& ctx, bool& got_row) const;
  int merge_implicit_cursor(
      const common::ObNewRow& full_row, bool is_update, bool client_found_rows, ObPhysicalPlanCtx& plan_ctx) const;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_DML_OB_MULTI_TABLE_UPDATE_H_ */
