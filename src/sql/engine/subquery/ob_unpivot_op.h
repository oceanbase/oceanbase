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

#ifndef OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

class ObVectorsResultHolder;

struct ObUnpivotInfo
{
public:
  OB_UNIS_VERSION(1);
public:
  ObUnpivotInfo()
    : is_include_null_(false),
      old_column_count_(0),
      for_column_count_(0),
      unpivot_column_count_(0)
  {}
  ObUnpivotInfo(const bool is_include_null, const int64_t old_column_count,
                const int64_t for_column_count, const int64_t unpivot_column_count)
    : is_include_null_(is_include_null), old_column_count_(old_column_count),
      for_column_count_(for_column_count), unpivot_column_count_(unpivot_column_count)
  {}

  void reset() { new (this) ObUnpivotInfo(); }
  OB_INLINE bool has_unpivot() const
  { return old_column_count_>= 0 && unpivot_column_count_ > 0 && for_column_count_ > 0; };
  OB_INLINE int64_t get_new_column_count() const
  { return unpivot_column_count_ + for_column_count_; }
  OB_INLINE int64_t get_output_column_count() const
  { return old_column_count_ + get_new_column_count(); }
  TO_STRING_KV(K_(is_include_null), K_(old_column_count), K_(unpivot_column_count),
               K_(for_column_count));

  bool is_include_null_;
  int64_t old_column_count_;
  int64_t for_column_count_;
  int64_t unpivot_column_count_;
};

class ObUnpivotSpec : public ObOpSpec
{
  OB_UNIS_VERSION(1);
public:
  ObUnpivotSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(unpivot_info));

  int64_t max_part_count_;
  ObUnpivotInfo unpivot_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotSpec);
};

class ObUnpivotOp : public ObOperator
{
public:
  ObUnpivotOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  void reset();
  int64_t curr_part_idx_;
  int64_t curr_cols_idx_;
  const ObBatchRows *child_brs_;
  ObDatum *multiplex_;
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotOp);
};

class ObUnpivotV2Spec : public ObOpSpec
{
  OB_UNIS_VERSION(1);
public:
  ObUnpivotV2Spec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(origin_exprs), K_(label_exprs),
                       K_(value_exprs), K_(is_include_null));

  ExprFixedArray origin_exprs_;
  ExprFixedArray label_exprs_;
  ExprFixedArray value_exprs_;
  bool is_include_null_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotV2Spec);
};

class ObUnpivotV2Op : public ObOperator
{
public:
  ObUnpivotV2Op(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_get_next_row() override
  {
    return OB_NOT_IMPLEMENT;
  }
  virtual void destroy() override;

private:
  void reset();
  int next_batch(const int64_t max_row_cnt);
  int next_vector(const int64_t max_row_cnt);
  int project_vector();
  int project_vector_with_offset();
  int batch_project_data();
  int batch_copy_data();
  int get_result_buffer();
  int restore_batch_if_need();
  int get_row_in_batch(int64_t rid);
  int get_child_batch_if_need(int64_t batch_size);
  int64_t cur_part_idx_;
  int64_t offset_;
  const ObBatchRows *child_brs_;
  union
  {
    ObDatum *buffer_;
    ObVectorsResultHolder *vec_holder_;
  };
  bool using_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotV2Op);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_ */
