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

#ifndef _OB_SEQUENCE_H
#define _OB_SEQUENCE_H 1
#include "sql/engine/ob_multi_children_phy_operator.h"

namespace oceanbase {
namespace sql {
class ObSqlExpression;
class ObSequence : public ObMultiChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  class ObSequenceCtx;

public:
  explicit ObSequence(common::ObIAllocator& alloc);
  virtual ~ObSequence();

  virtual void reset();
  virtual void reuse();
  int add_uniq_nextval_sequence_id(uint64_t seq_id);

private:
  bool is_valid() const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief overload add_filter to prevent any filter expression add to sequence
   * @param expr[in] any expr
   * @return always return OB_NOT_SUPPORTED
   */
  int add_filter(ObSqlExpression* expr);

  int try_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  TO_STRING_KV(K_(nextval_seq_ids));
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSequence);

private:
  common::ObSEArray<uint64_t, 4> nextval_seq_ids_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_SEQUENCE_H */
