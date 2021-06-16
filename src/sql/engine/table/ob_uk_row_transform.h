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

#ifndef OCEANBASE_TABLE_OB_UK_ROW_TRANSFORM_H_
#define OCEANBASE_TABLE_OB_UK_ROW_TRANSFORM_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/ob_single_child_phy_operator.h"

namespace oceanbase {
namespace sql {

// generate unique index row, set shadow pk values.
class ObUKRowTransform : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObUKRowTransform(common::ObIAllocator& alloc);
  virtual ~ObUKRowTransform()
  {}

  void set_uk_col_cnt(const int64_t uk_col_cnt)
  {
    uk_col_cnt_ = uk_col_cnt;
  }
  void set_shadow_pk_cnt(const int64_t shadow_pk_cnt)
  {
    shadow_pk_cnt_ = shadow_pk_cnt;
  }

  int init_column_array(int64_t size)
  {
    return columns_.init(size);
  }
  common::ObIArray<int64_t>& get_columns()
  {
    return columns_;
  }

  inline bool is_valid() const
  {
    return uk_col_cnt_ > 0 && !columns_.empty() && NULL != child_op_;
  }

  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int rescan(ObExecContext& ctx) const override;

  virtual void reset() override;
  virtual void reuse() override;

  INHERIT_TO_STRING_KV("parent", ObSingleChildPhyOperator, K(uk_col_cnt_), K(shadow_pk_cnt_), K(columns_));

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;

  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;

private:
  class ObUKRowTransformCtx;

private:
  int64_t uk_col_cnt_;
  int64_t shadow_pk_cnt_;
  common::ObFixedArray<int64_t, common::ObIAllocator> columns_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_TABLE_OB_UK_ROW_TRANSFORM_H_
