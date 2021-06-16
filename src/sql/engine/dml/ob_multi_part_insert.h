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

#ifndef DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_H_
#define DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_H_
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert.h"
namespace oceanbase {
namespace sql {
class ObTableLocation;
class ObMultiPartInsert : public ObTableInsert, public ObMultiDMLInfo {
  class ObMultiPartInsertCtx;

public:
  static const int64_t INSERT_OP = 0;
  static const int64_t DML_OP_CNT = 1;

public:
  explicit ObMultiPartInsert(common::ObIAllocator& alloc);
  virtual ~ObMultiPartInsert();

  virtual int create_operator_input(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual bool has_foreign_key() const override
  {
    return subplan_has_foreign_key();
  }
  void reset();
  void reuse();
  int set_insert_row_exprs();
  virtual bool is_multi_dml() const
  {
    return true;
  }

protected:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  int shuffle_insert_row(ObExecContext& ctx, bool& got_row) const;

protected:
  // exprs for calculating the inserted row
  common::ObDList<ObSqlExpression> insert_row_exprs_;
  // projector for building the inserted row
  int32_t* insert_projector_;
  int64_t insert_projector_size_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_H_ */
