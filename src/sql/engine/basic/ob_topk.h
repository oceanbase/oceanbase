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

#ifndef _OB_TOPK_H
#define _OB_TOPK_H 1
#include "sql/engine/ob_single_child_phy_operator.h"

namespace oceanbase {
namespace sql {
class ObSqlExpression;

class ObTopK : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  class ObTopKCtx;

public:
  explicit ObTopK(common::ObIAllocator& alloc);
  virtual ~ObTopK();

  virtual void reset();
  virtual void reuse();
  int set_topk_params(
      ObSqlExpression* limit, ObSqlExpression* offset, int64_t minimum_row_count, int64_t topk_precision);
  virtual int rescan(ObExecContext& ctx) const;

private:
  bool is_valid() const;
  int get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val, bool& is_null_value) const;
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
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int add_filter(ObSqlExpression* expr);
  int get_topk_final_count(ObExecContext& ctx, int64_t& topk_final_count) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTopK);

private:
  // data members
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  ObSqlExpression* org_limit_;
  ObSqlExpression* org_offset_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_TOPK_H */
