/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "test_hybrid_search.h"

class TestHybridSearchScalarQuery : public TestHybridSearch
{
public:
  TestHybridSearchScalarQuery()
    : TestHybridSearch("test_query")
  {}
};

static void verify_rowids_by_driver_iter(ObDASSearchDriverIter &iter,
                                         ObEvalCtx &ctx,
                                         ObExpr *id_expr,
                                         const int64_t max_batch_size,
                                         const ObIArray<uint64_t> &expected_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> got_ids;
  int64_t count = 0;
  while (OB_SUCC(ret)) {
    count = 0;
    ret = iter.get_next_rows(count, max_batch_size);
    if (OB_ITER_END == ret) {
      // done
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_GE(count, 0);
      if (count > 0) {
        ObIVector *vec = id_expr->get_vector(ctx);
        ASSERT_NE(nullptr, vec);
        for (int64_t i = 0; i < count; ++i) {
          ASSERT_EQ(OB_SUCCESS, got_ids.push_back(vec->get_uint(i)));
        }
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(expected_ids.count(), got_ids.count());
  for (int64_t i = 0; i < expected_ids.count(); ++i) {
    EXPECT_EQ(expected_ids.at(i), got_ids.at(i));
  }
}

static void run_search_and_verify(TestHybridSearch &test,
                                  ObDASFusionRtDef &fusion_rtdef,
                                  ObDASSearchCtx &search_ctx,
                                  const ObDASSearchOpType expected_op_type,
                                  ObExpr *id_expr,
                                  const int64_t max_batch_size,
                                  const ObIArray<uint64_t> &expected_ids)
{
  TestHybridSearch::ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(expected_op_type, iter.root_op_->get_op_type());
    verify_rowids_by_driver_iter(iter, ctx, id_expr, max_batch_size, expected_ids);
  };
  test.run_search(fusion_rtdef, search_ctx, checker);
}

static void verify_advance(ObIDASSearchOp *op, uint64_t target, uint64_t expected, bool expect_end)
{
  ObDASRowID target_id;
  target_id.reset();
  target_id.set_uint64(target);

  ObDASRowID curr_id;
  double score = 0.0;
  int ret = op->advance_to(target_id, curr_id, score);
  if (expect_end) {
    ASSERT_EQ(OB_ITER_END, ret);
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(expected, curr_id.get_uint64());
  }
}

// scan index table simply
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_index_table)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // create exprs
  // NOTE: column exprs are shared within one statement; use primary_table_id_ for rowid expr
  // even when scanning index table.
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();

  ObExpr *id_col_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_col_expr);

  // root scan op
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;

  // minimal das scan ctdef/rtdef
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_col_expr));

  // scalar scan
  ObDASScalarScanCtDef *scalar_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scalar_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges;
  {
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
    }
    // c1 = 1, __pk_increment in [MIN, MAX]
    start[0].set_int(1);
    end[0].set_int(1);
    for (int64_t i = 1; i < rk_cnt; ++i) {
      start[i].set_min_value();
      end[i].set_max_value();
    }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  }

  ObSEArray<ObExpr*, 1> access_exprs;
  ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_col_expr));
  ObSEArray<uint64_t, 1> scan_out_cols;
  ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
  ObSEArray<ObExpr*, 1> rowkey_exprs;
  ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_col_expr));
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                root_scan_op, scalar_scan_ctdef, scalar_scan_rtdef));
  ASSERT_NE(nullptr, scalar_scan_ctdef);
  ASSERT_NE(nullptr, scalar_scan_rtdef);

  // scalar root
  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(scalar_scan_ctdef, scalar_scan_rtdef,
                                           nullptr, nullptr,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  // fusion
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs,
                                           fusion_ctdef, fusion_rtdef));
  ASSERT_NE(nullptr, fusion_ctdef);
  ASSERT_NE(nullptr, fusion_rtdef);

  // attach fusion ctdef to root scan op and create search context
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
      int ret = OB_SUCCESS;
      ObSEArray<uint64_t, 8> got_ids;
      int64_t count = 0;
      while (OB_SUCC(ret)) {
        count = 0;
        ret = iter.get_next_rows(count, max_batch_size_);
        if (OB_ITER_END == ret) {
        } else {
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_GE(count, 0);
          if (count > 0) {
            ObIVector *vec = id_col_expr->get_vector(ctx);
            ASSERT_NE(nullptr, vec);
            for (int64_t i = 0; i < count; ++i) {
              ASSERT_EQ(OB_SUCCESS, got_ids.push_back(vec->get_uint(i)));
            }
          }
        }
      }
      ASSERT_EQ(OB_ITER_END, ret);

      // expected: only rows with c=1, ordered by id
      EXPECT_EQ(5, got_ids.count());
      EXPECT_EQ(1, got_ids.at(0));
      EXPECT_EQ(2, got_ids.at(1));
      EXPECT_EQ(4, got_ids.at(2));
      EXPECT_EQ(5, got_ids.at(3));
      EXPECT_EQ(7, got_ids.at(4));
  };

  run_search(*fusion_rtdef, search_ctx, checker);
}

// scan primary table simply
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_primary_table)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // create exprs
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);

  const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
  ASSERT_NE(nullptr, c1_col_schema);
  create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

  ObSEArray<ObRawExpr*, 1> pushdown_filters;
  {
    // create c1 = 1 pushdown filter
    ObOpRawExpr *c1_eq = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
    ObColumnRefRawExpr *c1_col = nullptr;

    for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
      ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
      if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (col->get_column_id() == c1_col_schema->get_column_id()) {
          c1_col = col;
          break;
        }
      }
    }
    ASSERT_NE(nullptr, c1_col);

    ObConstRawExpr *val = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
    val->set_value(ObObj(1));
    val->set_data_type(ObInt32Type);
    val->set_collation_type(CS_TYPE_BINARY);
    val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);

    ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
    LinkExecCtxGuard link_guard(session_, exec_ctx_);
    ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));

    ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
  }

  generate_exprs();

  ObExpr *id_col_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_col_expr);
  ObExpr *c1_col_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
  ASSERT_NE(nullptr, c1_col_expr);

  // root scan op
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;

  // minimal scan ctdef/rtdef
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_col_expr));

  // scalar scan
  ObDASScalarScanCtDef *scalar_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scalar_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges;
  {
    ObNewRange range;
    range.table_id_ = primary_table_id_;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  }

  ObSEArray<ObExpr*, 2> access_exprs;
  ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_col_expr));
  ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(c1_col_expr));
  ObSEArray<uint64_t, 1> scan_out_cols;
  ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
  ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
  ObSEArray<ObExpr*, 1> rowkey_exprs;
  ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_col_expr));
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
                                                rowkey_exprs, true /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                root_scan_op, scalar_scan_ctdef, scalar_scan_rtdef, &pushdown_filters));
  ASSERT_NE(nullptr, scalar_scan_ctdef);
  ASSERT_NE(nullptr, scalar_scan_rtdef);

  // scalar root
  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(nullptr, nullptr,
                                           scalar_scan_ctdef, scalar_scan_rtdef,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  // fusion
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs,
                                           fusion_ctdef, fusion_rtdef));
  ASSERT_NE(nullptr, fusion_ctdef);
  ASSERT_NE(nullptr, fusion_rtdef);

  // attach fusion ctdef to root scan op and create search context
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
      int ret = OB_SUCCESS;
      ObSEArray<uint64_t, 8> got_ids;
      int64_t count = 0;
      while (OB_SUCC(ret)) {
        count = 0;
        ret = iter.get_next_rows(count, max_batch_size_);
        if (OB_ITER_END == ret) {
        } else {
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_GE(count, 0);
          if (count > 0) {
            ObIVector *vec = id_col_expr->get_vector(ctx);
            ASSERT_NE(nullptr, vec);
            for (int64_t i = 0; i < count; ++i) {
              ASSERT_EQ(OB_SUCCESS, got_ids.push_back(vec->get_uint(i)));
            }
          }
        }
      }
      ASSERT_EQ(OB_ITER_END, ret);

      EXPECT_EQ(5, got_ids.count());
      EXPECT_EQ(1, got_ids.at(0));
      EXPECT_EQ(2, got_ids.at(1));
      EXPECT_EQ(4, got_ids.at(2));
      EXPECT_EQ(5, got_ids.at(3));
      EXPECT_EQ(7, got_ids.at(4));
  };

  run_search(*fusion_rtdef, search_ctx, checker);
}

// scan index table using index ror op
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_index_ror_op_with_primary_scan)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // exprs (primary id + primary c1)
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
  ASSERT_NE(nullptr, c1_col_schema);
  create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

  // pushdown filter: c1 = 1 (for primary table scan to be semantically equivalent to index range)
  ObSEArray<ObRawExpr*, 1> pushdown_filters;
  {
    ObOpRawExpr *c1_eq = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
    ObColumnRefRawExpr *c1_col = nullptr;
    for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
      ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
      if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (col->get_column_id() == c1_col_schema->get_column_id()) { c1_col = col; break; }
      }
    }
    ASSERT_NE(nullptr, c1_col);
    ObConstRawExpr *val = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
    val->set_value(ObObj(1));
    val->set_data_type(ObInt32Type);
    val->set_collation_type(CS_TYPE_BINARY);
    val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);
    ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
    LinkExecCtxGuard link_guard(session_, exec_ctx_);
    ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));
    ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
  }

  generate_exprs();

  ObExpr *pri_id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObExpr *pri_c1_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
  ASSERT_NE(nullptr, pri_id_expr);
  ASSERT_NE(nullptr, pri_c1_expr);

  // root scan op (for search ctx lead cost + related tablet mapping)
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(2));

  // index scan node: range c1=1
  ObDASScalarScanCtDef *index_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *index_scan_rtdef = nullptr;
  {
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(1);
    end[0].set_int(1);
    for (int64_t i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                  rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, index_scan_ctdef, index_scan_rtdef));
  }

  // primary scan node: whole range (won't be chosen in this case)
  ObDASScalarScanCtDef *primary_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *primary_scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = primary_table_id_;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 2> access_exprs;
    ObSEArray<uint64_t, 2> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(pri_c1_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
                                                  rowkey_exprs, true /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, primary_scan_ctdef, primary_scan_rtdef, &pushdown_filters));
  }

  // scalar root (both children)
  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(index_scan_ctdef, index_scan_rtdef,
                                           primary_scan_ctdef, primary_scan_rtdef,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  // fusion to build search ctx
  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(pri_id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ObSEArray<uint64_t, 8> expected_ids;
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(1));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(2));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(4));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(5));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(7));

  run_search_and_verify(*this, *fusion_rtdef, search_ctx, DAS_SEARCH_OP_SCALAR_INDEX_ROR,
                        pri_id_expr, max_batch_size_, expected_ids);
}

// scan primary table using primary ror op
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_primary_ror_op_without_index_scan)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // exprs
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
  ASSERT_NE(nullptr, c1_col_schema);
  create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

  // pushdown filter: c1=1
  ObSEArray<ObRawExpr*, 1> pushdown_filters;
  {
    ObOpRawExpr *c1_eq = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
    ObColumnRefRawExpr *c1_col = nullptr;
    for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
      ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
      if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (col->get_column_id() == c1_col_schema->get_column_id()) { c1_col = col; break; }
      }
    }
    ASSERT_NE(nullptr, c1_col);
    ObConstRawExpr *val = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
    val->set_value(ObObj(1));
    val->set_data_type(ObInt32Type);
    val->set_collation_type(CS_TYPE_BINARY);
    val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);
    ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
    LinkExecCtxGuard link_guard(session_, exec_ctx_);
    ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));
    ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
  }

  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObExpr *c1_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
  ASSERT_NE(nullptr, id_expr);
  ASSERT_NE(nullptr, c1_expr);

  // root scan op + minimal rtdef/ctdef
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  // scalar scan node (primary ror)
  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = primary_table_id_;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 2> access_exprs;
    ObSEArray<uint64_t, 2> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(c1_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
                                                  rowkey_exprs, true /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef, &pushdown_filters));
  }
  ASSERT_NE(nullptr, scan_ctdef);
  ASSERT_NE(nullptr, scan_rtdef);

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(nullptr, nullptr,
                                           scan_ctdef, scan_rtdef,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  // build fusion + search ctx (rowid expr uses primary id expr)
  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ObSEArray<uint64_t, 8> expected_ids;
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(1));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(2));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(4));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(5));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(7));

  run_search_and_verify(*this, *fusion_rtdef, search_ctx, DAS_SEARCH_OP_SCALAR_PRIMARY_ROR,
                        id_expr, max_batch_size_, expected_ids);
}

// scan index table using index ror op
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_index_ror_op)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // exprs (shared column exprs within one statement)
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *pri_id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, pri_id_expr);

  // root scan op
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  // scalar scan node (index ror): range c1=1
  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(1);
    end[0].set_int(1);
    for (int64_t i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                  rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef));
  }
  ASSERT_NE(nullptr, scan_ctdef);
  ASSERT_NE(nullptr, scan_rtdef);

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(scan_ctdef, scan_rtdef,
                                           nullptr, nullptr,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(pri_id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ObSEArray<uint64_t, 8> expected_ids;
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(1));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(2));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(4));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(5));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(7));

  run_search_and_verify(*this, *fusion_rtdef, search_ctx, DAS_SEARCH_OP_SCALAR_INDEX_ROR,
                        pri_id_expr, max_batch_size_, expected_ids);
}

// scan primary table using scalar scan op
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_op_with_primary_table)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // exprs + filter
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
  ASSERT_NE(nullptr, c1_col_schema);
  create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

  ObSEArray<ObRawExpr*, 1> pushdown_filters;
  {
    ObOpRawExpr *c1_eq = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
    ObColumnRefRawExpr *c1_col = nullptr;
    for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
      ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
      if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (col->get_column_id() == c1_col_schema->get_column_id()) { c1_col = col; break; }
      }
    }
    ASSERT_NE(nullptr, c1_col);
    ObConstRawExpr *val = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
    val->set_value(ObObj(1));
    val->set_data_type(ObInt32Type);
    val->set_collation_type(CS_TYPE_BINARY);
    val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);
    ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
    LinkExecCtxGuard link_guard(session_, exec_ctx_);
    ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));
    ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
  }

  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObExpr *c1_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
  ASSERT_NE(nullptr, id_expr);
  ASSERT_NE(nullptr, c1_expr);

  // root scan op
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = primary_table_id_;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
    ObSEArray<ObExpr*, 2> access_exprs;
    ObSEArray<uint64_t, 2> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(c1_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
                                                  rowkey_exprs, true /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef, &pushdown_filters,
                                                  false /*need_rowkey_order*/, false /*is_rowkey_order_scan*/));
  }

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(nullptr, nullptr,
                                           scan_ctdef, scan_rtdef,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  TestHybridSearch::ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    int64_t count = 0;
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_BITMAP, iter.root_op_->get_op_type());
    EXPECT_EQ(0, iter.get_next_rows(count, max_batch_size_));
  };
  run_search(*fusion_rtdef, search_ctx, checker);
}

// scan primary table using bitmap op
TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_primary_bitmap_op)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (3), (1), (2), (4), (0), (1), (3)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // exprs + filter
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);

  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_expr);

  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      start[i].set_min_value();
      end[i].set_max_value();
    }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                  rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef, nullptr,
                                                  true /*need_rowkey_order*/, false /*is_rowkey_order_scan*/));
  }

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(scan_ctdef, scan_rtdef,
                                           nullptr, nullptr,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  ObSEArray<uint64_t, 8> expected_ids;
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(1));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(2));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(3));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(4));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(5));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(6));
  ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(7));

  run_search_and_verify(*this, *fusion_rtdef, search_ctx, DAS_SEARCH_OP_BITMAP,
                        id_expr, max_batch_size_, expected_ids);
}

// scan primary table using sort op
// DAS_ROWID_TYPE_COMPACT is not supported for now
// TEST_F(TestHybridSearchScalarQuery, test_scalar_scan_primary_sort_op)
// {
//   ASSERT_NE(0, tenant_id_);
//   share::ObTenantSwitchGuard tguard;
//   ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
//   sqlclient::ObISQLConnection *conn = nullptr;
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
//   ASSERT_NE(nullptr, conn);

//   int64_t affected_rows = 0;
//   ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
//   ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int) organization heap", affected_rows));
//   ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

//   prepare_schema_info("t");
//   prepare_test_env();

//   // exprs + filter
//   create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
//   const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
//   ASSERT_NE(nullptr, c1_col_schema);
//   create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

//   ObSEArray<ObRawExpr*, 1> pushdown_filters;
//   {
//     ObOpRawExpr *c1_eq = nullptr;
//     ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
//     ObColumnRefRawExpr *c1_col = nullptr;
//     for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
//       ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
//       if (expr->is_column_ref_expr()) {
//         ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
//         if (col->get_column_id() == c1_col_schema->get_column_id()) { c1_col = col; break; }
//       }
//     }
//     ASSERT_NE(nullptr, c1_col);
//     ObConstRawExpr *val = nullptr;
//     ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
//     val->set_value(ObObj(1));
//     val->set_data_type(ObInt32Type);
//     val->set_collation_type(CS_TYPE_BINARY);
//     val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);
//     ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
//     LinkExecCtxGuard link_guard(session_, exec_ctx_);
//     ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));
//     ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
//     ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
//   }

//   generate_exprs();
//   ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
//   ObExpr *c1_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
//   ASSERT_NE(nullptr, id_expr);
//   ASSERT_NE(nullptr, c1_expr);

//   ObDASScanOp root_scan_op(das_alloc_);
//   root_scan_op.set_ls_id(primary_ls_id_);
//   root_scan_op.trans_desc_ = tx_desc_;
//   root_scan_op.snapshot_ = &snapshot_;
//   root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
//   ObDASScanCtDef root_scan_ctdef(das_alloc_);
//   root_scan_ctdef.ref_table_id_ = primary_table_id_;
//   root_scan_ctdef.schema_version_ = schema_version_;
//   ObDASScanRtDef root_scan_rtdef;
//   root_scan_rtdef.eval_ctx_ = eval_ctx_;
//   root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
//   root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
//   root_scan_op.set_scan_ctdef(&root_scan_ctdef);
//   root_scan_op.set_scan_rtdef(&root_scan_rtdef);
//   ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

//   ObDASScalarScanCtDef *scan_ctdef = nullptr;
//   ObDASScalarScanRtDef *scan_rtdef = nullptr;
//   {
//     ObSEArray<ObNewRange, 1> ranges;
//     ObNewRange range;
//     range.table_id_ = primary_table_id_;
//     range.set_whole_range();
//     ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
//     ObSEArray<ObExpr*, 2> access_exprs;
//     ObSEArray<uint64_t, 2> scan_out_cols;
//     ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
//     ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(c1_expr));
//     ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
//     ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
//     ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
//                                                   true /*is_primary*/, access_exprs, scan_out_cols, ranges,
//                                                   root_scan_op, scan_ctdef, scan_rtdef, &pushdown_filters,
//                                                   true /*need_rowkey_order*/, false /*is_rowkey_order_scan*/));
//   }

//   ExprFixedArray output_exprs(das_alloc_);
//   ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
//   ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
//   ObDASFusionCtDef *fusion_ctdef = nullptr;
//   ObDASFusionRtDef *fusion_rtdef = nullptr;
//   ASSERT_EQ(OB_SUCCESS, create_fusion_node(scan_ctdef, scan_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
//   root_scan_op.attach_ctdef_ = fusion_ctdef;
//   ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
//   ASSERT_NE(nullptr, root_scan_op.search_ctx_);
//   ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
//   search_ctx.rowid_type_ = DAS_ROWID_TYPE_COMPACT;

//   ObSEArray<uint64_t, 8> expected_ids;
//   ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(1));
//   ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(2));
//   ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(4));
//   ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(5));
//   ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(7));

//   run_search_and_verify(*this, *fusion_rtdef, search_ctx, DAS_SEARCH_OP_SORT,
//                         id_expr, max_batch_size_, expected_ids);
// }

// scan primary table using bitmap op and test advance
TEST_F(TestHybridSearchScalarQuery, test_scalar_bitmap_op_advance)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (3), (1), (2), (4), (0), (1), (3)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // exprs + filter
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);

  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_expr);

  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      start[i].set_min_value();
      end[i].set_max_value();
    }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                  rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef, nullptr,
                                                  true /*need_rowkey_order*/, false /*is_rowkey_order_scan*/));
  }

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(scan_ctdef, scan_rtdef,
                                           nullptr, nullptr,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  TestHybridSearch::ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_BITMAP, iter.root_op_->get_op_type());
    ObIDASSearchOp *op = iter.root_op_;

    // Sorted PKs: 1, 2, 3, 4, 5, 6, 7
    verify_advance(op, 2, 2, false);
    verify_advance(op, 4, 4, false);
    verify_advance(op, 6, 6, false);
    verify_advance(op, 8, 0, true);
  };
  run_search(*fusion_rtdef, search_ctx, checker);
}

// scan index table using index ror op and test advance
TEST_F(TestHybridSearchScalarQuery, test_scalar_index_ror_op_advance)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int, key i1(c1)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // index info
  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  // exprs (shared column exprs within one statement)
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *pri_id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, pri_id_expr);

  // root scan op
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  // scalar scan node (index ror): range c1=1
  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    const int64_t rk_cnt = index_schema->get_rowkey_column_num();
    ASSERT_GE(rk_cnt, 2);
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(1);
    end[0].set_int(1);
    for (int64_t i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(pri_id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(index_table_id, index_schema, index_tablet_id,
                                                  rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef));
  }
  ASSERT_NE(nullptr, scan_ctdef);
  ASSERT_NE(nullptr, scan_rtdef);

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(scan_ctdef, scan_rtdef,
                                           nullptr, nullptr,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(pri_id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  TestHybridSearch::ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_SCALAR_INDEX_ROR, iter.root_op_->get_op_type());
    ObIDASSearchOp *op = iter.root_op_;

    // PKs: 1, 2, 4, 5, 7
    verify_advance(op, 1, 1, false);
    verify_advance(op, 3, 4, false);
    verify_advance(op, 6, 7, false);
    verify_advance(op, 8, 0, true);
  };
  run_search(*fusion_rtdef, search_ctx, checker);
}

// scan primary table using primary ror op and test advance
TEST_F(TestHybridSearchScalarQuery, test_scalar_primary_ror_op_advance)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t(c1 int) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t values (1), (1), (2), (1), (1), (3), (1)", affected_rows));

  prepare_schema_info("t");
  prepare_test_env();

  // exprs
  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  const ObColumnSchemaV2 *c1_col_schema = primary_schema_->get_column_schema("c1");
  ASSERT_NE(nullptr, c1_col_schema);
  create_column_expr(primary_table_id_, c1_col_schema->get_column_id(), ObInt32Type);

  // pushdown filter: c1=1
  ObSEArray<ObRawExpr*, 1> pushdown_filters;
  {
    ObOpRawExpr *c1_eq = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_OP_EQ, c1_eq));
    ObColumnRefRawExpr *c1_col = nullptr;
    for (int64_t i = 0; i < raw_exprs_.count(); ++i) {
      ObRawExpr *expr = raw_exprs_.get_expr_array().at(i);
      if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (col->get_column_id() == c1_col_schema->get_column_id()) { c1_col = col; break; }
      }
    }
    ASSERT_NE(nullptr, c1_col);
    ObConstRawExpr *val = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_INT32, val));
    val->set_value(ObObj(1));
    val->set_data_type(ObInt32Type);
    val->set_collation_type(CS_TYPE_BINARY);
    val->set_accuracy(ObAccuracy::MAX_ACCURACY[ObInt32Type]);
    ASSERT_EQ(OB_SUCCESS, c1_eq->set_param_exprs(c1_col, val));
    LinkExecCtxGuard link_guard(session_, exec_ctx_);
    ASSERT_EQ(OB_SUCCESS, c1_eq->formalize(&session_));
    ASSERT_EQ(OB_SUCCESS, pushdown_filters.push_back(c1_eq));
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(pushdown_filters));
  }

  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObExpr *c1_expr = get_rt_expr(primary_table_id_, c1_col_schema->get_column_id());
  ASSERT_NE(nullptr, id_expr);
  ASSERT_NE(nullptr, c1_expr);

  // root scan op + minimal rtdef/ctdef
  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  // scalar scan node (primary ror)
  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    ObNewRange range;
    range.table_id_ = primary_table_id_;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr*, 2> access_exprs;
    ObSEArray<uint64_t, 2> scan_out_cols;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(c1_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(c1_col_schema->get_column_id()));
    ObSEArray<ObExpr*, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(primary_table_id_, primary_schema_, primary_tablet_id_,
                                                  rowkey_exprs, true /*is_primary*/, access_exprs, scan_out_cols, ranges,
                                                  root_scan_op, scan_ctdef, scan_rtdef, &pushdown_filters));
  }
  ASSERT_NE(nullptr, scan_ctdef);
  ASSERT_NE(nullptr, scan_rtdef);

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(nullptr, nullptr,
                                           scan_ctdef, scan_rtdef,
                                           scalar_ctdef, scalar_rtdef));
  ASSERT_NE(nullptr, scalar_ctdef);
  ASSERT_NE(nullptr, scalar_rtdef);

  // build fusion + search ctx (rowid expr uses primary id expr)
  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(scalar_ctdef, scalar_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;

  TestHybridSearch::ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_SCALAR_PRIMARY_ROR, iter.root_op_->get_op_type());
    ObIDASSearchOp *op = iter.root_op_;

    // PKs: 1, 2, 4, 5, 7
    verify_advance(op, 1, 1, false);
    verify_advance(op, 3, 4, false);
    verify_advance(op, 6, 7, false);
    verify_advance(op, 8, 0, true);
  };
  run_search(*fusion_rtdef, search_ctx, checker);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}
