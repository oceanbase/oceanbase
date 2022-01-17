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

#ifndef SQL_ENGINE_DML_OB_MULTI_TABLE_LOCK_H_
#define SQL_ENGINE_DML_OB_MULTI_TABLE_LOCK_H_
#include "sql/engine/dml/ob_table_lock.h"
namespace oceanbase {
namespace sql {
class ObMultiPartLock : public ObTableModify, public ObMultiDMLInfo {
  class ObMultiPartLockCtx;

public:
  static const int64_t LOCK_OP = 0;
  static const int64_t DML_OP_CNT = 1;

public:
  explicit ObMultiPartLock(common::ObIAllocator& allocator);
  virtual ~ObMultiPartLock();
  virtual bool is_multi_dml() const
  {
    return true;
  }
  virtual int create_operator_input(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }

private:
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;

  int shuffle_lock_row(ObExecContext& ctx, const ObNewRow* row) const;
  int process_mini_task(ObExecContext& ctx) const;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_DML_OB_MULTI_TABLE_LOCK_H_ */
