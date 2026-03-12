/**
 * Copyright (c) 2026 OceanBase
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
    : TestHybridSearch("test_scalar_query")
  {}
};

TEST_F(TestHybridSearchScalarQuery, test_boolean_must_scalar)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "drop table if exists t_boolean", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "create table t_boolean(c1 int, c2 int, key i1(c1), key i2(c2)) organization heap", affected_rows));
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(tenant_id_, "insert into t_boolean values (1, 10), (1, 20), (2, 10), (1, 10), (3, 30)", affected_rows));

  prepare_schema_info("t_boolean");
  prepare_test_env();

  // index 1 info (c1)
  int64_t i1_id = 0;
  ObTabletID i1_tablet_id;
  ObLSID i1_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i1_id, "i1"));
  ASSERT_GT(i1_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i1_id, i1_tablet_id, i1_ls_id));
  const ObTableSchema *i1_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i1_id, i1_schema));

  // index 2 info (c2)
  int64_t i2_id = 0;
  ObTabletID i2_tablet_id;
  ObLSID i2_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i2_id, "i2"));
  ASSERT_GT(i2_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i2_id, i2_tablet_id, i2_ls_id));
  const ObTableSchema *i2_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i2_id, i2_schema));

  // create exprs
  create_column_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();

  ObExpr *id_col_expr = get_rt_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
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

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_col_expr));

  // reserve space for multiple indices in root scan op
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(2));

  // scalar scan 1 (c1 = 1)
  ObDASScalarScanCtDef *s1_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s1_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges1;
  {
    const int64_t rk_cnt = i1_schema->get_rowkey_column_num();
    ObNewRange range;
    range.table_id_ = i1_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    for (int64_t i = 0; i < rk_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
    }
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
    ASSERT_EQ(OB_SUCCESS, ranges1.push_back(range));
  }
  ObSEArray<ObExpr*, 1> access_exprs;
  ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_col_expr));
  ObSEArray<uint64_t, 1> scan_out_cols;
  ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
  ObSEArray<ObExpr*, 1> rowkey_exprs;
  ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_col_expr));
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i1_id, i1_schema, i1_tablet_id,
                                                rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges1, root_scan_op,
                                                s1_scan_ctdef, s1_scan_rtdef, nullptr));

  // scalar scan 2 (c2 = 10)
  ObDASScalarScanCtDef *s2_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s2_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges2;
  {
    const int64_t rk_cnt = i2_schema->get_rowkey_column_num();
    ObNewRange range;
    range.table_id_ = i2_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    for (int64_t i = 0; i < rk_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
    }
    start[0].set_int(10);
    end[0].set_int(10);
    for (int64_t i = 1; i < rk_cnt; ++i) {
      start[i].set_min_value();
      end[i].set_max_value();
    }
    range.start_key_.assign(start, rk_cnt);
    range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges2.push_back(range));
  }
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i2_id, i2_schema, i2_tablet_id,
                                                rowkey_exprs, false /*is_primary*/, access_exprs, scan_out_cols, ranges2,
                                                root_scan_op, s2_scan_ctdef, s2_scan_rtdef, nullptr));

  // scalar nodes
  ObDASScalarCtDef *s1_ctdef = nullptr;
  ObDASScalarRtDef *s1_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(s1_scan_ctdef, s1_scan_rtdef, nullptr, nullptr, s1_ctdef, s1_rtdef));
  ObDASScalarCtDef *s2_ctdef = nullptr;
  ObDASScalarRtDef *s2_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(s2_scan_ctdef, s2_scan_rtdef, nullptr, nullptr, s2_ctdef, s2_rtdef));

  // boolean node (must)
  ObDASBooleanQueryCtDef *bool_ctdef = nullptr;
  ObDASBooleanQueryRtDef *bool_rtdef = nullptr;
  ObSEArray<ObIDASSearchCtDef*, 2> must_ctdefs;
  ObSEArray<ObIDASSearchRtDef*, 2> must_rtdefs;
  ObSEArray<ObIDASSearchCtDef*, 1> empty_cts;
  ObSEArray<ObIDASSearchRtDef*, 1> empty_rts;
  ASSERT_EQ(OB_SUCCESS, must_ctdefs.push_back(s1_ctdef));
  ASSERT_EQ(OB_SUCCESS, must_ctdefs.push_back(s2_ctdef));
  ASSERT_EQ(OB_SUCCESS, must_rtdefs.push_back(s1_rtdef));
  ASSERT_EQ(OB_SUCCESS, must_rtdefs.push_back(s2_rtdef));
  ASSERT_EQ(OB_SUCCESS, create_boolean_node(must_ctdefs, must_rtdefs, empty_cts, empty_rts,
                                            empty_cts, empty_rts, empty_cts, empty_rts,
                                            0, bool_ctdef, bool_rtdef));

  // fusion
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(bool_ctdef, bool_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));

  // attach and run
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  root_scan_op.search_ctx_->rowid_type_ = DAS_ROWID_TYPE_UINT64;
  root_scan_op.search_ctx_->rowid_exprs_ = &fusion_ctdef->rowid_exprs_;

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
          if (count > 0) {
            ObIVector *vec = id_col_expr->get_vector(ctx);
            for (int64_t i = 0; i < count; ++i) {
              ASSERT_EQ(OB_SUCCESS, got_ids.push_back(vec->get_uint(i)));
            }
          }
        }
      }
      ASSERT_EQ(OB_ITER_END, ret);

      // expected intersection of c1=1 (1,2,4) and c2=10 (1,3,4) is (1,4)
      EXPECT_EQ(2, got_ids.count());
      EXPECT_EQ(1, got_ids.at(0));
      EXPECT_EQ(4, got_ids.at(1));
  };

  run_search(*fusion_rtdef, *root_scan_op.search_ctx_, checker);
}

TEST_F(TestHybridSearchScalarQuery, test_boolean_should_scalar)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  prepare_schema_info("t_boolean");
  prepare_test_env();

  int64_t i1_id = 0;
  ObTabletID i1_tablet_id;
  ObLSID i1_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i1_id, "i1"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i1_id, i1_tablet_id, i1_ls_id));
  const ObTableSchema *i1_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i1_id, i1_schema));

  int64_t i2_id = 0;
  ObTabletID i2_tablet_id;
  ObLSID i2_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i2_id, "i2"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i2_id, i2_tablet_id, i2_ls_id));
  const ObTableSchema *i2_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i2_id, i2_schema));

  create_column_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *id_col_expr = get_rt_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);

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

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_col_expr));

  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(2));

  // should 1: c1 = 1 (IDs: 1, 2, 4)
  ObDASScalarScanCtDef *s1_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s1_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges1;
  {
    const int64_t rk_cnt = i1_schema->get_rowkey_column_num();
    ObNewRange range;
    range.table_id_ = i1_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    for (int64_t i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(1); end[0].set_int(1);
    for (int64_t i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt); range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start(); range.border_flag_.set_inclusive_end();
    ranges1.push_back(range);
  }
  ObSEArray<ObExpr*, 1> access_exprs; access_exprs.push_back(id_col_expr);
  ObSEArray<uint64_t, 1> scan_out_cols; scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObSEArray<ObExpr*, 1> rowkey_exprs; rowkey_exprs.push_back(id_col_expr);
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i1_id, i1_schema, i1_tablet_id, rowkey_exprs, false, access_exprs, scan_out_cols, ranges1, root_scan_op, s1_scan_ctdef, s1_scan_rtdef, nullptr));

  // should 2: c2 = 30 (ID: 5)
  ObDASScalarScanCtDef *s2_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s2_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> ranges2;
  {
    const int64_t rk_cnt = i2_schema->get_rowkey_column_num();
    ObNewRange range;
    range.table_id_ = i2_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    for (int64_t i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(30); end[0].set_int(30);
    for (int64_t i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt); range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start(); range.border_flag_.set_inclusive_end();
    ranges2.push_back(range);
  }
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i2_id, i2_schema, i2_tablet_id, rowkey_exprs, false, access_exprs, scan_out_cols, ranges2, root_scan_op, s2_scan_ctdef, s2_scan_rtdef, nullptr));

  ObDASScalarCtDef *s1_ctdef = nullptr; ObDASScalarRtDef *s1_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(s1_scan_ctdef, s1_scan_rtdef, nullptr, nullptr, s1_ctdef, s1_rtdef));
  ObDASScalarCtDef *s2_ctdef = nullptr; ObDASScalarRtDef *s2_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_scalar_node(s2_scan_ctdef, s2_scan_rtdef, nullptr, nullptr, s2_ctdef, s2_rtdef));

  ObDASBooleanQueryCtDef *bool_ctdef = nullptr;
  ObDASBooleanQueryRtDef *bool_rtdef = nullptr;
  ObSEArray<ObIDASSearchCtDef*, 2> should_ctdefs;
  ObSEArray<ObIDASSearchRtDef*, 2> should_rtdefs;
  ObSEArray<ObIDASSearchCtDef*, 1> empty_cts;
  ObSEArray<ObIDASSearchRtDef*, 1> empty_rts;
  should_ctdefs.push_back(s1_ctdef); should_ctdefs.push_back(s2_ctdef);
  should_rtdefs.push_back(s1_rtdef); should_rtdefs.push_back(s2_rtdef);
  ASSERT_EQ(OB_SUCCESS, create_boolean_node(empty_cts, empty_rts, empty_cts, empty_rts,
                                            should_ctdefs, should_rtdefs, empty_cts, empty_rts,
                                            1, bool_ctdef, bool_rtdef));

  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS, create_fusion_node(bool_ctdef, bool_rtdef, output_exprs, fusion_ctdef, fusion_rtdef));

  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  root_scan_op.search_ctx_->rowid_type_ = DAS_ROWID_TYPE_UINT64;
  root_scan_op.search_ctx_->rowid_exprs_ = &fusion_ctdef->rowid_exprs_;

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
      int ret = OB_SUCCESS;
      ObSEArray<uint64_t, 8> got_ids;
      int64_t count = 0;
      while (OB_SUCC(ret)) {
        count = 0;
        ret = iter.get_next_rows(count, max_batch_size_);
        if (OB_ITER_END != ret) {
          ASSERT_EQ(OB_SUCCESS, ret);
          ObIVector *vec = id_col_expr->get_vector(ctx);
          for (int64_t i = 0; i < count; ++i) { got_ids.push_back(vec->get_uint(i)); }
        }
      }
      // IDs for c1=1: 1, 2, 4. ID for c2=30: 5. Union: 1, 2, 4, 5.
      EXPECT_EQ(4, got_ids.count());
      EXPECT_EQ(1, got_ids.at(0));
      EXPECT_EQ(2, got_ids.at(1));
      EXPECT_EQ(4, got_ids.at(2));
      EXPECT_EQ(5, got_ids.at(3));
  };

  run_search(*fusion_rtdef, *root_scan_op.search_ctx_, checker);
}

TEST_F(TestHybridSearchScalarQuery, test_boolean_mixed_scalar)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  prepare_schema_info("t_boolean");
  prepare_test_env();

  int64_t i1_id = 0;
  ObTabletID i1_tablet_id;
  ObLSID i1_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i1_id, "i1"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i1_id, i1_tablet_id, i1_ls_id));
  const ObTableSchema *i1_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i1_id, i1_schema));

  int64_t i2_id = 0;
  ObTabletID i2_tablet_id;
  ObLSID i2_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i2_id, "i2"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i2_id, i2_tablet_id, i2_ls_id));
  const ObTableSchema *i2_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i2_id, i2_schema));

  create_column_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *id_col_expr = get_rt_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);

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

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_col_expr));

  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(3));

  // Helper lambda to create range
  auto make_range = [&](int64_t table_id, const ObTableSchema *schema, int val, ObNewRange &range) {
    const int64_t rk_cnt = schema->get_rowkey_column_num();
    range.table_id_ = table_id;
    ObObj *start = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    ObObj *end = static_cast<ObObj *>(das_alloc_.alloc(sizeof(ObObj) * rk_cnt));
    for (int i = 0; i < rk_cnt; ++i) { new (start + i) ObObj(); new (end + i) ObObj(); }
    start[0].set_int(val); end[0].set_int(val);
    for (int i = 1; i < rk_cnt; ++i) { start[i].set_min_value(); end[i].set_max_value(); }
    range.start_key_.assign(start, rk_cnt); range.end_key_.assign(end, rk_cnt);
    range.border_flag_.set_inclusive_start(); range.border_flag_.set_inclusive_end();
  };

  ObSEArray<ObExpr*, 1> access_exprs; access_exprs.push_back(id_col_expr);
  ObSEArray<uint64_t, 1> scan_out_cols; scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ObSEArray<ObExpr*, 1> rowkey_exprs; rowkey_exprs.push_back(id_col_expr);

  // must: c2 = 10 (IDs: 1, 3, 4)
  ObDASScalarScanCtDef *m_scan_ctdef = nullptr; ObDASScalarScanRtDef *m_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> m_ranges; ObNewRange m_range; make_range(i2_id, i2_schema, 10, m_range); m_ranges.push_back(m_range);
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i2_id, i2_schema, i2_tablet_id, rowkey_exprs, false, access_exprs, scan_out_cols, m_ranges, root_scan_op, m_scan_ctdef, m_scan_rtdef, nullptr));

  // should 1: c1 = 1 (IDs: 1, 2, 4)
  ObDASScalarScanCtDef *s1_scan_ctdef = nullptr; ObDASScalarScanRtDef *s1_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> s1_ranges; ObNewRange s1_range; make_range(i1_id, i1_schema, 1, s1_range); s1_ranges.push_back(s1_range);
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i1_id, i1_schema, i1_tablet_id, rowkey_exprs, false, access_exprs, scan_out_cols, s1_ranges, root_scan_op, s1_scan_ctdef, s1_scan_rtdef, nullptr));

  // should 2: c1 = 2 (ID: 3)
  ObDASScalarScanCtDef *s2_scan_ctdef = nullptr; ObDASScalarScanRtDef *s2_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> s2_ranges; ObNewRange s2_range; make_range(i1_id, i1_schema, 2, s2_range); s2_ranges.push_back(s2_range);
  ASSERT_EQ(OB_SUCCESS, create_scalar_scan_node(i1_id, i1_schema, i1_tablet_id, rowkey_exprs, false, access_exprs, scan_out_cols, s2_ranges, root_scan_op, s2_scan_ctdef, s2_scan_rtdef, nullptr));

  ObDASScalarCtDef *m_ctdef, *s1_ctdef, *s2_ctdef; ObDASScalarRtDef *m_rtdef, *s1_rtdef, *s2_rtdef;
  create_scalar_node(m_scan_ctdef, m_scan_rtdef, nullptr, nullptr, m_ctdef, m_rtdef);
  create_scalar_node(s1_scan_ctdef, s1_scan_rtdef, nullptr, nullptr, s1_ctdef, s1_rtdef);
  create_scalar_node(s2_scan_ctdef, s2_scan_rtdef, nullptr, nullptr, s2_ctdef, s2_rtdef);

  ObDASBooleanQueryCtDef *bool_ctdef; ObDASBooleanQueryRtDef *bool_rtdef;
  ObSEArray<ObIDASSearchCtDef*, 1> m_cts; ObSEArray<ObIDASSearchRtDef*, 1> m_rts;
  ObSEArray<ObIDASSearchCtDef*, 2> s_cts; ObSEArray<ObIDASSearchRtDef*, 2> s_rts;
  ObSEArray<ObIDASSearchCtDef*, 1> e_cts; ObSEArray<ObIDASSearchRtDef*, 1> e_rts;
  m_cts.push_back(m_ctdef); m_rts.push_back(m_rtdef);
  s_cts.push_back(s1_ctdef); s_cts.push_back(s2_ctdef);
  s_rts.push_back(s1_rtdef); s_rts.push_back(s2_rtdef);
  ASSERT_EQ(OB_SUCCESS, create_boolean_node(m_cts, m_rts, e_cts, e_rts, s_cts, s_rts, e_cts, e_rts, 1, bool_ctdef, bool_rtdef));

  ObDASFusionCtDef *fusion_ctdef; ObDASFusionRtDef *fusion_rtdef;
  create_fusion_node(bool_ctdef, bool_rtdef, output_exprs, fusion_ctdef, fusion_rtdef);

  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  root_scan_op.search_ctx_->rowid_type_ = DAS_ROWID_TYPE_UINT64;
  root_scan_op.search_ctx_->rowid_exprs_ = &fusion_ctdef->rowid_exprs_;

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
      int ret = OB_SUCCESS; ObSEArray<uint64_t, 8> got_ids; int64_t count = 0;
      while (OB_SUCC(ret)) {
        count = 0; ret = iter.get_next_rows(count, max_batch_size_);
        if (OB_ITER_END != ret) {
          ObIVector *vec = id_col_expr->get_vector(ctx);
          for (int i = 0; i < count; ++i) { got_ids.push_back(vec->get_uint(i)); }
        }
      }
      // Must: 1,3,4. Should (Union): 1,2,3,4. Mixed Intersection: 1, 3, 4.
      EXPECT_EQ(3, got_ids.count());
      EXPECT_EQ(1, got_ids.at(0));
      EXPECT_EQ(3, got_ids.at(1));
      EXPECT_EQ(4, got_ids.at(2));
  };
  run_search(*fusion_rtdef, *root_scan_op.search_ctx_, checker);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}
