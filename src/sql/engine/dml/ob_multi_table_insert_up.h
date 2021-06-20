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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_H_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/dml/ob_duplicated_key_checker.h"
namespace oceanbase {
namespace sql {
class ObTableLocation;
class ObMultiTableInsertUp : public ObTableInsertUp, public ObMultiDMLInfo {
  class ObMultiTableInsertUpCtx;

public:
  // insert a row if there is no duplicate row with all unique index, otherwise
  // delete and insert a row.
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t UPDATE_OP = 2;
  static const int64_t DML_OP_CNT = 3;

public:
  explicit ObMultiTableInsertUp(common::ObIAllocator& alloc);
  virtual ~ObMultiTableInsertUp();

  virtual int create_operator_input(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual bool has_foreign_key() const override
  {
    return subplan_has_foreign_key();
  }
  ObDuplicatedKeyChecker& get_duplicate_key_checker()
  {
    return duplicate_key_checker_;
  }
  int shuffle_final_delete_row(ObExecContext& ctx, const common::ObNewRow& delete_row) const;
  int shuffle_final_insert_row(ObExecContext& ctx, const common::ObNewRow& insert_row) const;
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
  virtual int inner_close(ObExecContext& ctx) const;
  int load_insert_up_row(ObExecContext& ctx, common::ObRowStore& row_store) const;
  int shuffle_insert_up_row(ObExecContext& ctx, bool& got_row) const;
  int shuffle_insert_row(ObExecContext& ctx, common::ObPartMgr& part_mgr, ObMultiTableInsertUpCtx& replace_ctx,
      const common::ObNewRow& row) const;
  int shuffle_update_row(ObExecContext& ctx, common::ObPartMgr& part_mgr, ObMultiTableInsertUpCtx& insert_up_ctx,
      const common::ObNewRow& insert_row, const common::ObNewRow& duplicate_row) const;

private:
  ObDuplicatedKeyChecker duplicate_key_checker_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_H_ */
