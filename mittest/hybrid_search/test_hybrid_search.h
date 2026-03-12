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

#define USING_LOG_PREFIX SQL_DAS
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_scalar_index_ror_op.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/rewrite/ob_query_range.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#undef private
#undef protected

#include "mittest/env/ob_simple_server_helper.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"


using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::unittest;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

class TestHybridSearch : public ObSimpleClusterTestBase {
public:
  TestHybridSearch(const char *test_name)
    : ObSimpleClusterTestBase(test_name),
      tenant_id_(0),
      expr_alloc_(ObModIds::TEST),
      expr_factory_(expr_alloc_),
      expr_frame_info_(expr_alloc_),
      expr_cg_(nullptr),
      session_(0),
      exec_ctx_(expr_alloc_),
      eval_ctx_(nullptr),
      das_alloc_(ObModIds::TEST),
      max_batch_size_(128),
      tx_desc_(nullptr),
      snapshot_(),
      raw_exprs_(false),
      primary_table_id_(0),
      primary_tablet_id_(),
      primary_ls_id_(),
      primary_schema_(nullptr),
      schema_version_(0)
  {}

  void SetUp() override {
    ObSimpleClusterTestBase::SetUp();
    ASSERT_EQ(OB_SUCCESS, init());
  }

  void TearDown() override {
    ASSERT_EQ(OB_SUCCESS, destroy());
    ObSimpleClusterTestBase::TearDown();
  }

  int init() {
    int ret = OB_SUCCESS;
    tenant_id_ = OB_SYS_TENANT_ID;
    return ret;
  }

  int destroy() {
    int ret = OB_SUCCESS;
    if (nullptr != tx_desc_) {
      transaction::ObTransService *txs = MTL(transaction::ObTransService*);
      if (txs) {
        txs->release_tx(*tx_desc_);
      }
    }
    if (nullptr != expr_cg_) {
      delete expr_cg_;
      expr_cg_ = nullptr;
    }
    return ret;
  }

  int get_schema_guard(const int64_t table_id, schema::ObSchemaGetterGuard &schema_guard)
  {
    int ret = OB_SUCCESS;
    ObMultiVersionSchemaService *schema_service = nullptr;
    ObTenantSchemaService* tenant_schema_service = nullptr;
    int64_t schema_version = 0;
    if (OB_ISNULL(tenant_schema_service = MTL(ObTenantSchemaService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get tenant schema service from MTL", K(ret));
    } else if (OB_ISNULL(schema_service = tenant_schema_service->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema service from MTL", K(ret));
    } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id_, schema_version))) {
      LOG_WARN("fail to get tenant local schema version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_service->retry_get_schema_guard(tenant_id_,
                         schema_version,
                         table_id, schema_guard, schema_version))) {
      LOG_WARN("failed to get schema guard", KR(ret));
    }
    return ret;
  }

  int get_table_schema(schema::ObSchemaGetterGuard &schema_guard,
                       const int64_t table_id,
                       const ObTableSchema *&table_schema)
  {
    int ret = OB_SUCCESS;
    table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
      LOG_WARN("Fail to get table schema", K(ret), K(table_id));
    }
    return ret;
  }

  int get_table_id(sqlclient::ObISQLConnection &conn,
                   const char *table_name,
                   int64_t &table_id)
  {
    table_id = 0;
    ObSqlString sql;
    sql.assign_fmt("select table_id as val from oceanbase.__all_table where table_name = '%s' limit 1", table_name);
    return SimpleServerHelper::select_int64(&conn, sql.ptr(), table_id);
  }

  int get_index_table_id(sqlclient::ObISQLConnection &conn,
                         const int64_t primary_table_id,
                         int64_t &index_table_id,
                         const char *index_name = nullptr)
  {
    index_table_id = 0;
    ObSqlString sql;
    if (index_name != nullptr) {
        sql.assign_fmt("select table_id as val from oceanbase.__all_table where data_table_id = %ld and table_name like '%%%s%%' limit 1", primary_table_id, index_name);
    } else {
        sql.assign_fmt("select table_id as val from oceanbase.__all_table where data_table_id = %ld limit 1", primary_table_id);
    }
    return SimpleServerHelper::select_int64(&conn, sql.ptr(), index_table_id);
  }

  int get_tablet_and_ls_id(sqlclient::ObISQLConnection &conn,
                           const int64_t table_id,
                           ObTabletID &tablet_id,
                           ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    int64_t tmp_tablet_id = 0;
    int64_t tmp_ls_id = 0;
    ObSqlString sql;
    sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld limit 1", table_id);
    if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_tablet_id))) {
      sql.reuse();
      sql.assign_fmt("select ls_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld and tablet_id = %ld limit 1",
                     table_id, tmp_tablet_id);
      if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_ls_id))) {
        tablet_id = ObTabletID(tmp_tablet_id);
        ls_id = ObLSID(tmp_ls_id);
      }
    }
    return ret;
  }

  void prepare_test_env()
  {
    // prepare tx desc + read snapshot
    transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    ASSERT_NE(nullptr, txs);
    ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc_));
    ASSERT_NE(nullptr, tx_desc_);
    const transaction::ObTxIsolationLevel iso = transaction::ObTxIsolationLevel::RC;
    const int64_t expire_ts = ObTimeUtility::current_time() + TRANS_TIMEOUT;
    ASSERT_EQ(OB_SUCCESS, txs->get_read_snapshot(*tx_desc_, iso, expire_ts, snapshot_));

    ASSERT_EQ(OB_SUCCESS, session_.test_init(0 /*version*/, 1 /*sessid*/, 1 /*proxy_sessid*/, &expr_alloc_));
    session_.effective_tenant_id_ = tenant_id_;
    session_.tenant_id_ = tenant_id_;
    session_.use_rich_vector_format_ = true;
    session_.force_rich_vector_format_ = ObBasicSessionInfo::ForceRichFormatStatus::FORCE_ON;

    ASSERT_EQ(OB_SUCCESS, exec_ctx_.create_physical_plan_ctx());
    ASSERT_NE(nullptr, exec_ctx_.get_physical_plan_ctx());
  }

  void prepare_schema_info(const char *table_name)
  {
    share::ObTenantSwitchGuard tguard;
    ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    sqlclient::ObISQLConnection *conn = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
    ASSERT_NE(nullptr, conn);

    ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, table_name, primary_table_id_));
    ASSERT_GT(primary_table_id_, 0);

    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, primary_table_id_, primary_tablet_id_, primary_ls_id_));
    ASSERT_TRUE(primary_tablet_id_.is_valid());
    ASSERT_TRUE(primary_ls_id_.is_valid());

    ASSERT_EQ(OB_SUCCESS, get_schema_guard(primary_table_id_, schema_guard_));
    ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, primary_table_id_, primary_schema_));
    ASSERT_NE(nullptr, primary_schema_);
    ASSERT_EQ(OB_SUCCESS, schema_guard_.get_schema_version(tenant_id_, schema_version_));
  }

  void create_column_expr(uint64_t table_id, uint64_t column_id, ObObjType type)
  {
    ObColumnRefRawExpr *col_raw = nullptr;
    ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_REF_COLUMN, col_raw));
    ASSERT_NE(nullptr, col_raw);
    col_raw->add_flag(IS_COLUMN);
    col_raw->set_ref_id(table_id, column_id);
    col_raw->set_data_type(type);
    col_raw->set_collation_type(CS_TYPE_BINARY);
    col_raw->set_accuracy(ObAccuracy::MAX_ACCURACY[type]);
    ASSERT_EQ(OB_SUCCESS, raw_exprs_.append(reinterpret_cast<ObRawExpr *>(col_raw)));
  }

  void generate_exprs() {
    // only generate exprs once
    if (OB_ISNULL(expr_cg_)) {
      expr_cg_ = new ObStaticEngineExprCG(expr_alloc_,
                                          &session_,
                                          &schema_guard_,
                                          0 /*original_param_cnt*/,
                                          0 /*param_cnt*/,
                                          CLUSTER_CURRENT_VERSION);
      ASSERT_NE(nullptr, expr_cg_);
      expr_cg_->batch_size_ = max_batch_size_;
    }

    ASSERT_EQ(OB_SUCCESS, expr_cg_->generate(raw_exprs_, expr_frame_info_));
    ASSERT_EQ(OB_SUCCESS, expr_frame_info_.pre_alloc_exec_memory(exec_ctx_, &expr_alloc_));

    // init after exec_ctx_.frames_ is generated by expr_frame_info_.pre_alloc_exec_memory
    if (OB_ISNULL(eval_ctx_)) {
      eval_ctx_ = new ObEvalCtx(exec_ctx_);
      ASSERT_NE(nullptr, eval_ctx_);
      eval_ctx_->max_batch_size_ = max_batch_size_;
    }
  }

  ObExpr* get_rt_expr(uint64_t table_id, uint64_t column_id)
  {
    ObExpr *rt_expr = nullptr;
    ObSEArray<ObRawExpr*, 4> dummy;
    for (int64_t i = 0; rt_expr == nullptr && i < raw_exprs_.count(); ++i) {
      ObColumnRefRawExpr *col_raw = static_cast<ObColumnRefRawExpr*>(raw_exprs_.get_expr_array().at(i));
      if (col_raw->get_table_id() == table_id && col_raw->get_column_id() == column_id) {
        if (OB_SUCCESS != ObStaticEngineExprCG::generate_rt_expr(*col_raw, dummy, rt_expr)) {
          return nullptr;
        }
      }
    }
    if (rt_expr != nullptr) {
      rt_expr->batch_result_ = true;
    }
    return rt_expr;
  }

  using ResultVerifier = std::function<void(ObDASSearchDriverIter &iter, ObEvalCtx &eval_ctx)>;

  void run_search(ObDASFusionRtDef &root_rtdef, ObDASSearchCtx &search_ctx, ResultVerifier verifier)
  {
    ObDASSearchDriverIter *search_iter = nullptr;
    common::ObLimitParam top_k_limit_param;
    top_k_limit_param.limit_ = -1;
    ASSERT_EQ(OB_SUCCESS, ObDASIterUtils::create_search_driver_iter(das_alloc_, root_rtdef.get_search_rtdef(), &search_ctx, top_k_limit_param, search_iter));
    ASSERT_NE(nullptr, search_iter);
    ASSERT_EQ(OB_SUCCESS, search_iter->do_table_scan());

    verifier(*search_iter, *eval_ctx_);

    ASSERT_EQ(OB_SUCCESS, search_iter->release());
  }

  template <typename T, typename... Args>
  T *alloc(Args &&... args) {
    void *ptr = das_alloc_.alloc(sizeof(T));
    if (ptr != nullptr) {
      new (ptr) T(std::forward<Args>(args)...);
    }
    return static_cast<T *>(ptr);
  }

  int create_scalar_scan_node(uint64_t ref_table_id,
                              const ObTableSchema *table_schema,
                              const ObTabletID &tablet_id,
                              const ObIArray<ObExpr *> &rowkey_exprs,
                              bool is_primary_scan,
                              const ObIArray<ObExpr *> &access_exprs,
                              const ObIArray<uint64_t> &out_cols,
                              const ObIArray<ObNewRange> &ranges,
                              ObDASScanOp &root_scan_op,
                              ObDASScalarScanCtDef *&out_ctdef,
                              ObDASScalarScanRtDef *&out_rtdef,
                              const ObIArray<ObRawExpr *> *pushdown_filters = nullptr,
                              bool need_rowkey_order = true,
                              bool is_rowkey_order_scan = true)
  {
    int ret = OB_SUCCESS;
    out_ctdef = alloc<ObDASScalarScanCtDef>(das_alloc_);
    if (OB_ISNULL(out_ctdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate scalar scan ctdef", K(ret));
    } else if (OB_FAIL(out_ctdef->rowkey_exprs_.init(rowkey_exprs.count()))) {
      LOG_WARN("failed to init rowkey exprs", K(ret));
    } else if (OB_FAIL(out_ctdef->rowkey_exprs_.assign(rowkey_exprs))) {
      LOG_WARN("failed to push back rowkey expr", K(ret));
    } else {
      out_ctdef->ref_table_id_ = ref_table_id;
      out_ctdef->is_get_ = false;
      out_ctdef->is_new_query_range_ = false;
      out_ctdef->need_rowkey_order_ = need_rowkey_order;
      out_ctdef->is_rowkey_order_scan_ = is_rowkey_order_scan;
      out_ctdef->is_primary_table_scan_ = is_primary_scan;
      out_ctdef->schema_version_ = schema_version_;

      if (OB_FAIL(out_ctdef->access_column_ids_.init(out_cols.count()))) {
        LOG_WARN("failed to init access column ids", K(ret));
      } else if (OB_FAIL(out_ctdef->access_column_ids_.assign(out_cols))) {
        LOG_WARN("failed to assign access column ids", K(ret));
      } else if (OB_FAIL(out_ctdef->pd_expr_spec_.access_exprs_.init(access_exprs.count()))) {
        LOG_WARN("failed to init access exprs", K(ret));
      } else if (OB_FAIL(out_ctdef->pd_expr_spec_.access_exprs_.assign(access_exprs))) {
        LOG_WARN("failed to assign access exprs", K(ret));
      } else {
        if (pushdown_filters != nullptr && pushdown_filters->count() > 0) {
          out_ctdef->pd_expr_spec_.pd_storage_flag_.set_flags(true, true, true, false, true, true);
          out_ctdef->pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(true);
          out_ctdef->pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(true);
          out_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(true);
          out_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(true);

          if (OB_FAIL(out_ctdef->pd_expr_spec_.pushdown_filters_.init(pushdown_filters->count()))) {
            LOG_WARN("failed to init pushdown filters", K(ret));
          } else {
            ObSEArray<ObExpr *, 2> rt_exprs;
            ObSEArray<ObRawExpr*, 2> dummy_raw_exprs;
            for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters->count(); ++i) {
              ObRawExpr *raw = pushdown_filters->at(i);
              ObExpr *rt_expr = nullptr;
              if (OB_FAIL(ObStaticEngineExprCG::generate_rt_expr(*raw, dummy_raw_exprs, rt_expr))) {
                LOG_WARN("failed to generate rt expr", K(ret));
              } else if (OB_FAIL(out_ctdef->pd_expr_spec_.pushdown_filters_.push_back(rt_expr))) {
                LOG_WARN("failed to push back rt expr", K(ret));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(out_ctdef->pd_expr_spec_.set_calc_exprs(out_ctdef->pd_expr_spec_.pushdown_filters_, max_batch_size_))) {
            LOG_WARN("failed to set calc exprs", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      out_ctdef->table_param_.set_is_partition_table(table_schema->is_partitioned_table());
      out_ctdef->table_param_.set_is_mlog_table(table_schema->is_mlog_table());
      out_ctdef->table_param_.get_enable_lob_locator_v2() = true;
      out_ctdef->table_param_.set_plan_enable_rich_format(true);
      if (OB_FAIL(out_ctdef->table_param_.convert(*table_schema,
                                                  out_ctdef->access_column_ids_,
                                                  out_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                  &out_cols,
                                                  false, false))) {
        LOG_WARN("failed to convert table param", K(ret));
      } else {
        out_rtdef = alloc<ObDASScalarScanRtDef>();
        if (OB_ISNULL(out_rtdef)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate scalar scan rtdef", K(ret));
        } else {
          out_rtdef->ctdef_ = out_ctdef;
          out_rtdef->eval_ctx_ = eval_ctx_;
          out_rtdef->tenant_schema_version_ = out_ctdef->schema_version_;
          out_rtdef->timeout_ts_ = ObTimeUtility::current_time() + TRANS_TIMEOUT;
          out_rtdef->tx_lock_timeout_ = -1;
          out_rtdef->sql_mode_ = SMO_DEFAULT;
          out_rtdef->scan_flag_.scan_order_ = ObQueryFlag::Forward;
          out_rtdef->scan_flag_.enable_rich_format_ = true;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(out_rtdef->init_pd_op(exec_ctx_, *out_ctdef))) {
      LOG_WARN("failed to init pd op", K(ret));
    } else {
      out_rtdef->stmt_allocator_.set_alloc(&das_alloc_);
      out_rtdef->scan_allocator_.set_alloc(&das_alloc_);

      for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(out_rtdef->key_ranges_.push_back(ranges.at(i)))){
          LOG_WARN("failed to push back key range", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(root_scan_op.set_related_task_info(out_ctdef, out_rtdef, tablet_id))) {
      LOG_WARN("failed to set related task info", K(ret));
    }

    return ret;
  }

  int create_scalar_node(ObDASScalarScanCtDef *index_ctdef,
                         ObDASScalarScanRtDef *index_rtdef,
                         ObDASScalarScanCtDef *primary_ctdef,
                         ObDASScalarScanRtDef *primary_rtdef,
                         ObDASScalarCtDef *&out_ctdef,
                         ObDASScalarRtDef *&out_rtdef)
  {
    int ret = OB_SUCCESS;
    out_ctdef = alloc<ObDASScalarCtDef>(das_alloc_);
    out_rtdef = alloc<ObDASScalarRtDef>();
    if (OB_ISNULL(out_ctdef) || OB_ISNULL(out_rtdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate scalar ctdef or scalar rtdef", K(ret));
    } else {
      out_ctdef->set_has_index_scan(index_ctdef != nullptr);
      out_ctdef->set_has_main_scan(primary_ctdef != nullptr);

      int64_t child_cnt = (index_ctdef ? 1 : 0) + (primary_ctdef ? 1 : 0);
      out_ctdef->children_cnt_ = child_cnt;
      out_ctdef->children_ = static_cast<ObDASBaseCtDef **>(das_alloc_.alloc(sizeof(ObDASBaseCtDef*) * child_cnt));
      out_rtdef->children_cnt_ = child_cnt;
      out_rtdef->children_ = static_cast<ObDASBaseRtDef **>(das_alloc_.alloc(sizeof(ObDASBaseRtDef*) * child_cnt));
      if (OB_ISNULL(out_ctdef->children_) || OB_ISNULL(out_rtdef->children_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate children", K(ret));
      } else {
        int64_t idx = 0;
        if (nullptr != index_ctdef) {
          out_ctdef->children_[idx] = index_ctdef;
          out_rtdef->children_[idx] = index_rtdef;
          ++ idx;
        }
        if (nullptr != primary_ctdef) {
          out_ctdef->children_[idx] = primary_ctdef;
          out_rtdef->children_[idx] = primary_rtdef;
          ++ idx;
        }

        out_rtdef->ctdef_ = out_ctdef;
        out_rtdef->eval_ctx_ = eval_ctx_;
      }
    }
    return ret;
  }

  int create_fusion_node(ObDASBaseCtDef *child_ctdef,
                         ObDASBaseRtDef *child_rtdef,
                         const ExprFixedArray &output_exprs,
                         ObDASFusionCtDef *&out_ctdef,
                         ObDASFusionRtDef *&out_rtdef)
  {
    int ret = OB_SUCCESS;
    ExprFixedArray rowid_exprs(das_alloc_);
    out_ctdef = alloc<ObDASFusionCtDef>(das_alloc_);
    out_rtdef = alloc<ObDASFusionRtDef>();
    if (OB_ISNULL(out_ctdef) || OB_ISNULL(out_rtdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate fusion ctdef or fusion rtdef", K(ret));
    } else if (OB_FAIL(rowid_exprs.init(output_exprs.count()))) {
      LOG_WARN("failed to init rowid exprs", K(ret));
    } else if (OB_FAIL(rowid_exprs.assign(output_exprs))) {
      LOG_WARN("failed to assign rowid exprs", K(ret));
    } else {
      ExprFixedArray empty_score_exprs(das_alloc_);
      ExprFixedArray empty_result_output_exprs(das_alloc_);
      ExprFixedArray empty_weight_exprs(das_alloc_);
      ExprFixedArray empty_path_top_k_limit_exprs(das_alloc_);
      if (OB_FAIL(out_ctdef->init(0,
                                  true,
                                  false,
                                  true,
                                  ObFusionMethod::WEIGHT_SUM,
                                  false,
                                  nullptr,
                                  nullptr,
                                  nullptr,
                                  nullptr,
                                  nullptr,
                                  rowid_exprs,
                                  empty_score_exprs,
                                  empty_result_output_exprs,
                                  empty_weight_exprs,
                                  empty_path_top_k_limit_exprs))) {
        LOG_WARN("failed to init fusion ctdef", K(ret));
      } else if (OB_FAIL(out_ctdef->result_output_.assign(output_exprs))) {
        LOG_WARN("failed to assign result output", K(ret));
      } else {
        out_ctdef->children_cnt_ = 1;
        out_ctdef->children_ = static_cast<ObDASBaseCtDef **>(das_alloc_.alloc(sizeof(ObDASBaseCtDef*)));
        out_rtdef->children_cnt_ = 1;
        out_rtdef->children_ = static_cast<ObDASBaseRtDef **>(das_alloc_.alloc(sizeof(ObDASBaseRtDef*)));
        if (OB_ISNULL(out_ctdef->children_) || OB_ISNULL(out_rtdef->children_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate children", K(ret));
        } else {
          out_ctdef->children_[0] = child_ctdef;
          out_rtdef->children_[0] = child_rtdef;

          out_rtdef->ctdef_ = out_ctdef;
          out_rtdef->eval_ctx_ = eval_ctx_;
        }
      }
    }
    return ret;
  }

  int create_boolean_node(const ObIArray<ObIDASSearchCtDef*> &must_ctdefs,
                         const ObIArray<ObIDASSearchRtDef*> &must_rtdefs,
                         const ObIArray<ObIDASSearchCtDef*> &filter_ctdefs,
                         const ObIArray<ObIDASSearchRtDef*> &filter_rtdefs,
                         const ObIArray<ObIDASSearchCtDef*> &should_ctdefs,
                         const ObIArray<ObIDASSearchRtDef*> &should_rtdefs,
                         const ObIArray<ObIDASSearchCtDef*> &must_not_ctdefs,
                         const ObIArray<ObIDASSearchRtDef*> &must_not_rtdefs,
                         int64_t minimum_should_match,
                         ObDASBooleanQueryCtDef *&out_ctdef,
                         ObDASBooleanQueryRtDef *&out_rtdef)
  {
    int ret = OB_SUCCESS;
    out_ctdef = alloc<ObDASBooleanQueryCtDef>(das_alloc_);
    out_rtdef = alloc<ObDASBooleanQueryRtDef>();
    if (OB_ISNULL(out_ctdef) || OB_ISNULL(out_rtdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate boolean ctdef or boolean rtdef", K(ret));
    } else {
      int64_t total_child_cnt = must_ctdefs.count() + filter_ctdefs.count() +
                               should_ctdefs.count() + must_not_ctdefs.count();
      out_ctdef->children_cnt_ = total_child_cnt;
      out_ctdef->children_ = static_cast<ObDASBaseCtDef **>(das_alloc_.alloc(sizeof(ObDASBaseCtDef*) * total_child_cnt));
      out_rtdef->children_cnt_ = total_child_cnt;
      out_rtdef->children_ = static_cast<ObDASBaseRtDef **>(das_alloc_.alloc(sizeof(ObDASBaseRtDef*) * total_child_cnt));

      if (total_child_cnt > 0 && (OB_ISNULL(out_ctdef->children_) || OB_ISNULL(out_rtdef->children_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate children", K(ret));
      } else {
        int64_t offset = 0;

        // must
        out_ctdef->set_must(offset, must_ctdefs.count());
        for (int64_t i = 0; i < must_ctdefs.count(); ++i) {
          out_ctdef->children_[offset + i] = must_ctdefs.at(i);
          out_rtdef->children_[offset + i] = must_rtdefs.at(i);
        }
        offset += must_ctdefs.count();

        // filter
        out_ctdef->set_filter(offset, filter_ctdefs.count());
        for (int64_t i = 0; i < filter_ctdefs.count(); ++i) {
          out_ctdef->children_[offset + i] = filter_ctdefs.at(i);
          out_rtdef->children_[offset + i] = filter_rtdefs.at(i);
        }
        offset += filter_ctdefs.count();

        // should
        out_ctdef->set_should(offset, should_ctdefs.count());
        for (int64_t i = 0; i < should_ctdefs.count(); ++i) {
          out_ctdef->children_[offset + i] = should_ctdefs.at(i);
          out_rtdef->children_[offset + i] = should_rtdefs.at(i);
        }
        offset += should_ctdefs.count();

        // must_not
        out_ctdef->set_must_not(offset, must_not_ctdefs.count());
        for (int64_t i = 0; i < must_not_ctdefs.count(); ++i) {
          out_ctdef->children_[offset + i] = must_not_ctdefs.at(i);
          out_rtdef->children_[offset + i] = must_not_rtdefs.at(i);
        }
        offset += must_not_ctdefs.count();

        out_ctdef->set_min_should_match(minimum_should_match);
        out_rtdef->ctdef_ = out_ctdef;
        out_rtdef->eval_ctx_ = eval_ctx_;
      }
    }
    return ret;
  }

public:
  uint64_t tenant_id_;
  ObArenaAllocator expr_alloc_;
  ObRawExprFactory expr_factory_;
  ObExprFrameInfo expr_frame_info_;
  ObStaticEngineExprCG *expr_cg_;
  ObSQLSessionInfo session_;
  ObExecContext exec_ctx_;
  ObEvalCtx *eval_ctx_;
  ObArenaAllocator das_alloc_;
  int64_t max_batch_size_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot snapshot_;
  ObRawExprUniqueSet raw_exprs_;

  // schema info
  int64_t primary_table_id_;
  ObTabletID primary_tablet_id_;
  ObLSID primary_ls_id_;
  schema::ObSchemaGetterGuard schema_guard_;
  const ObTableSchema *primary_schema_;
  int64_t schema_version_;
};
