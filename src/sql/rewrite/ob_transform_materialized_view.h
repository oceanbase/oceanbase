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

#ifndef _OB_TRANSFORM_MATERIALIZED_VIEW_H
#define _OB_TRANSFORM_MATERIALIZED_VIEW_H 1

#include "sql/resolver/dml/ob_select_stmt.h"
#include "lib/container/ob_bit_set.h"
#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {
typedef std::pair<uint64_t, uint64_t> JoinTableIdPair;

struct MVDesc {
  uint64_t mv_tid_;
  uint64_t base_tid_;
  uint64_t dep_tid_;
  ObString table_name_;
  ObSelectStmt* mv_stmt_;

  MVDesc() : mv_tid_(OB_INVALID_ID), base_tid_(OB_INVALID_ID), dep_tid_(OB_INVALID_ID), mv_stmt_(NULL)
  {}
  TO_STRING_KV("mv_tid", mv_tid_, "base_tid", base_tid_, "dep_tid_", dep_tid_);
};

class ObTransformMaterializedView : public ObTransformRule {
public:
  explicit ObTransformMaterializedView(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObTransformMaterializedView()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  uint64_t get_real_tid(uint64_t tid, ObSelectStmt& stmt);
  int is_col_in_mv(uint64_t tid, uint64_t cid, MVDesc mv_desc, bool& result);
  int is_all_column_covered(ObSelectStmt& stmt, MVDesc mv_desc, const JoinTableIdPair& idp, bool& result);
  int expr_equal(
      ObSelectStmt* select_stmt, const ObRawExpr* r1, ObSelectStmt* mv_stmt, const ObRawExpr* r2, bool& result);
  int check_where_condition_covered(
      ObSelectStmt* select_stmt, ObSelectStmt* mv_stmt, bool& result, ObBitSet<>& expr_idx);
  int get_column_id(uint64_t mv_tid, uint64_t tid, uint64_t cid, uint64_t& mv_cid);
  int rewrite_column_id(ObSelectStmt* select_stmt, MVDesc mv_desc, const JoinTableIdPair& idp);
  int rewrite_column_id2(ObSelectStmt* select_stmt);
  int check_can_transform(
      ObSelectStmt*& select_stmt, MVDesc mv_desc, bool& result, JoinTableIdPair& idp, ObBitSet<>& expr_idx);
  int reset_table_item(ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t base_tid, const JoinTableIdPair& idp);
  int reset_where_condition(ObSelectStmt* select_stmt, ObBitSet<>& expr_idx);
  int reset_from_item(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp);
  int inner_transform(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array, bool& trans_happened);
  int get_all_related_mv_array(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array);
  int get_view_stmt(const share::schema::ObTableSchema& index_schema, ObSelectStmt*& view_stmt);
  int reset_part_expr(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp);
  int is_rowkey_equal_covered(
      ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t rowkey_tid, uint64_t base_or_dep_tid, bool& is_covered);
  int get_join_tid_cid(
      uint64_t tid, uint64_t cid, uint64_t& out_tid, uint64_t& out_cid, const share::schema::ObTableSchema& mv_schema);

private:
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_TRANSFORM_MATERIALIZED_VIEW_H */
