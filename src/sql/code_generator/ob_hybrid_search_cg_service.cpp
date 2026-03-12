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

#define USING_LOG_PREFIX SQL_CG
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
namespace sql
{

typedef int (*ObGenerateHybridSearchCtDefFunc)(ObHybridSearchCgService &,
                                               ObLogTableScan &,
                                               const ObHybridSearchNodeBase *,
                                               ObDASBaseCtDef *&);

template <typename NodeType, typename CtDefType>
int generate_hybrid_search_ctdef_impl(ObHybridSearchCgService &cg_service,
                                      ObLogTableScan &op,
                                      const ObHybridSearchNodeBase *node,
                                      ObDASBaseCtDef *&ctdef)
{
  int ret = OB_SUCCESS;
  CtDefType *new_ctdef = nullptr;
  if (OB_FAIL(cg_service.generate_ctdef(op, static_cast<const NodeType*>(node), new_ctdef))) {
    LOG_WARN("failed to generate ctdef", K(ret));
  } else if (OB_ISNULL(new_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new ctdef is null", K(ret));
  } else {
    ctdef = new_ctdef;
  }
  return ret;
}

static ObGenerateHybridSearchCtDefFunc GENERATE_FUNCS[1 << (sizeof(ObIndexMergeType) * 8)];

struct ObHybridSearchCgInit
{
  ObHybridSearchCgInit()
  {
    #define REGISTER_GEN_FUNC(NODE_TYPE, NODE_CLASS, CTDEF_CLASS) \
      GENERATE_FUNCS[NODE_TYPE] = &generate_hybrid_search_ctdef_impl<NODE_CLASS, CTDEF_CLASS>;

    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_SCALAR_QUERY, ObScalarQueryNode, ObDASScalarCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_BOOLEAN_QUERY, ObBooleanQueryNode, ObDASBooleanQueryCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_FUSION_SEARCH, ObFusionNode, ObDASFusionCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_FTS_QUERY, ObFullTextQueryNode, ObDASBaseCtDef);
    REGISTER_GEN_FUNC(INDEX_MERGE_HYBRID_KNN, ObVecSearchNode, ObDASVecIndexDriverCtDef);
    #undef REGISTER_GEN_FUNC
  }
};

static ObHybridSearchCgInit HYBRID_SEARCH_CG_INIT;

#define GENERATE_HYBRID_SEARCH_CTDEF(cg_service, op, node, ctdef)                        \
  do {                                                                                   \
    if (OB_ISNULL(node)) {                                                               \
      ret = OB_ERR_UNEXPECTED;                                                           \
      LOG_WARN("node is null", K(ret));                                                  \
    } else if (node->node_type_ >= ARRAYSIZEOF(GENERATE_FUNCS) ||                        \
               nullptr == GENERATE_FUNCS[node->node_type_]) {                            \
      ret = OB_ERR_UNEXPECTED;                                                           \
      LOG_WARN("unexpected node type", K(node->node_type_), K(ret));                     \
    } else if (OB_FAIL(GENERATE_FUNCS[node->node_type_](cg_service, op, node, ctdef))) { \
      LOG_WARN("failed to generate ctdef", K(ret));                                      \
    }                                                                                    \
  } while(0)


int ObHybridSearchCgService::generate_hybrid_search_ctdef(ObLogTableScan &op,
                                                          ObTableScanCtDef &tsc_ctdef,
                                                          ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASBaseCtDef *search_ctdef = nullptr;
  const IndexMergePath *path = nullptr;
  if (OB_ISNULL(op.get_access_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_UNLIKELY(!op.use_index_merge())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index merge is not used", K(ret));
  } else {
    path = static_cast<const IndexMergePath*>(op.get_access_path());
    ObHybridSearchNodeBase *root = static_cast<ObHybridSearchNodeBase*>(path->root_);
    if (OB_FAIL(generate_hybrid_search_node_ctdef(op, root, search_ctdef))) {
      LOG_WARN("failed to generate hybrid search ctdef", K(ret));
    } else if (OB_ISNULL(search_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root ctdef is null", K(ret));
    } else if (search_ctdef->op_type_ == DAS_OP_FUSION_QUERY
        && OB_FAIL(try_alloc_topk_collect_ctdef(static_cast<ObDASFusionCtDef*>(search_ctdef)))) {
      LOG_WARN("failed to try alloc topk collect ctdef", K(ret));
    } else if (OB_FAIL(generate_final_lookup_ctdef(op, tsc_ctdef, *search_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate final lookup ctdef", K(ret));
    } else if (OB_FAIL(mock_das_scan_ctdef(op, tsc_ctdef.scan_ctdef_))) {
      LOG_WARN("failed to mock das scan ctdef", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_hybrid_search_node_ctdef(ObLogTableScan &op,
                                                               ObHybridSearchNodeBase *node,
                                                               ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t children_cnt = 0;
  ObDASBaseCtDef *new_root_ctdef = nullptr;
  GENERATE_HYBRID_SEARCH_CTDEF(*this, op, node, new_root_ctdef);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_root_ctdef) || OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(new_root_ctdef), KP(cg_.phy_plan_));
  } else if (FALSE_IT(children_cnt = node->children_.count())) {
  } else if (node->is_leaf_node()) {
    // children cnt of scan node is not necessarily equal to children_ of the search node
    if (node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
      if (OB_UNLIKELY(children_cnt > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scalar query node should not have children", K(ret));
      }
    }
  } else if (children_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected children count for non-leaf node", KR(ret), K(node->node_type_), K(children_cnt));
  } else {
    common::ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    if (OB_ISNULL(new_root_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, children_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate children failed", K(ret));
    }
    new_root_ctdef->children_cnt_ = children_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
      ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(node->children_.at(i));
      ObDASBaseCtDef *child_ctdef = nullptr;
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", K(ret));
      } else if (OB_FAIL(SMART_CALL(generate_hybrid_search_node_ctdef(op, child, child_ctdef)))) {
        LOG_WARN("failed to generate hybrid search node ctdef", K(ret));
      } else if (OB_ISNULL(child_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child ctdef is null", K(ret));
      } else {
        new_root_ctdef->children_[i] = child_ctdef;
      }
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = new_root_ctdef;
  }

  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObScalarQueryNode* scalar_query_node, ObDASScalarCtDef *&scalar_ctdef)
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  bool is_new_query_range = false;
  const ObQueryRange *pre_query_range = nullptr;
  const ObPreRangeGraph *pre_range_graph = nullptr;
  ObDASScalarCtDef *new_scalar_ctdef = nullptr;
  ObDASScalarScanCtDef * scalar_index_scan_ctdef = nullptr;
  ObDASScalarScanCtDef * scalar_main_scan_ctdef = nullptr;
  bool is_search_index_path = false;

  if (OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null", K(ret));
  } else if (OB_ISNULL(scalar_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar query node or access path is null", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_QUERY, cg_.phy_plan_->get_allocator(), new_scalar_ctdef))) {
    LOG_WARN("failed to allocate scalar ctdef", K(ret));
  } else if (OB_ISNULL(new_scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (nullptr != scalar_query_node->ap_) {
    // has index scan access path
    is_search_index_path = scalar_query_node->ap_->is_search_index_path();
    if (scalar_query_node->ap_->is_ordered_by_pk_ || !scalar_query_node->need_rowkey_order_) {
      // index scan is ordered by pk or no need to preserve rowkey order
      // just scan index table
      new_scalar_ctdef->set_has_index_scan(true);
      new_scalar_ctdef->set_has_main_scan(false);
    } else if (!scalar_query_node->ap_->is_ordered_by_pk_ && scalar_query_node->need_rowkey_order_) {
      // index scan is not ordered by pk and need to preserve rowkey order
      // scan index table or primary table, the decision will be made at execution time
      new_scalar_ctdef->set_has_index_scan(true);
      new_scalar_ctdef->set_has_main_scan(nullptr != scalar_query_node->pri_table_ap_);
    } else {
      // never happen, defensive programming
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected case", K(ret));
    }
  } else if (nullptr != scalar_query_node->pri_table_ap_) {
    // has primary table scan access path, but no index scan access path
    // must scan primary table
    new_scalar_ctdef->set_has_index_scan(false);
    new_scalar_ctdef->set_has_main_scan(true);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }

  // index scan
  if (OB_SUCC(ret) && new_scalar_ctdef->has_index_scan()) {
    const AccessPath *ap = scalar_query_node->ap_;
    if (OB_ISNULL(ap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(ap->get_query_range_provider())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query range provider", K(ret), KPC(ap));
    } else if (OB_FAIL(ap->get_query_range_provider()->is_get(is_get))) {
      LOG_WARN("failed to get query range provider is get", K(ret));
    } else {
      is_new_query_range = nullptr != ap->pre_range_graph_;
      pre_range_graph = ap->pre_range_graph_;
      pre_query_range = ap->pre_query_range_;
      ObHybridSearchCgService::DASScalarCGParams cg_params(op.get_ref_table_id(),
                                                           ap->get_real_index_table_id(),
                                                           is_new_query_range,
                                                           ap->is_ordered_by_pk_,
                                                           false,
                                                           op.get_table_type(),
                                                           &scalar_query_node->index_table_query_params_.access_exprs_,
                                                           &scalar_query_node->index_table_query_params_.output_col_ids_,
                                                           &scalar_query_node->index_table_rowkey_exprs_,
                                                           pre_range_graph,
                                                           pre_query_range,
                                                           true, /* has_pre_query_range */
                                                           is_get,
                                                           scalar_query_node->need_rowkey_order_,
                                                           op.use_column_store(),
                                                           nullptr,
                                                           nullptr,
                                                           &scalar_query_node->index_table_query_params_.pushdown_filters_,
                                                           nullptr,
                                                           &scalar_query_node->index_table_query_params_.filter_monotonicity_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, scalar_index_scan_ctdef))) {
        LOG_WARN("failed to generate index scan ctdef", K(ret));
      } else if (OB_ISNULL(scalar_index_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index scan ctdef is null", K(ret));
      } else {
        scalar_index_scan_ctdef->is_primary_table_scan_ = false;
        scalar_index_scan_ctdef->is_search_index_ = is_search_index_path;
      }
    }
  }

  // primary table scan
  if (OB_SUCC(ret) && new_scalar_ctdef->has_main_scan()) {
    const AccessPath *ap = scalar_query_node->pri_table_ap_;
    if (OB_ISNULL(ap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(ap->get_query_range_provider())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query range provider", K(ret), KPC(ap));
    } else if (OB_FAIL(ap->get_query_range_provider()->is_get(is_get))) {
      LOG_WARN("failed to get query range provider is get", K(ret));
    } else if (OB_UNLIKELY(!ap->is_ordered_by_pk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary table is not ordered by pk", K(ret), KPC(ap));
    } else {
      is_new_query_range = nullptr != ap->pre_range_graph_;
      pre_range_graph = ap->pre_range_graph_;
      pre_query_range = ap->pre_query_range_;
      ObHybridSearchCgService::DASScalarCGParams cg_params(op.get_ref_table_id(),
                                                           ap->get_index_table_id(),
                                                           is_new_query_range,
                                                           scalar_query_node->is_extracted_ ? false : ap->is_ordered_by_pk_,
                                                           true,
                                                           op.get_table_type(),
                                                           &scalar_query_node->pri_table_query_params_.access_exprs_,
                                                           &scalar_query_node->pri_table_query_params_.output_col_ids_,
                                                           &op.get_rowkey_exprs(),
                                                           pre_range_graph,
                                                           pre_query_range,
                                                           true, /* has_pre_query_range */
                                                           scalar_query_node->is_extracted_ ? true : is_get,
                                                           scalar_query_node->need_rowkey_order_,
                                                           op.use_column_store(),
                                                           nullptr,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.pushdown_filters_,
                                                           nullptr,
                                                           &scalar_query_node->pri_table_query_params_.filter_monotonicity_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_scalar_scan_ctdef(cg_params, scalar_main_scan_ctdef))) {
        LOG_WARN("failed to generate main scan ctdef", K(ret));
      } else if (OB_ISNULL(scalar_main_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("main scan ctdef is null", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (new_scalar_ctdef->has_index_scan() && new_scalar_ctdef->has_main_scan()) {
    if (OB_ISNULL(new_scalar_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate children failed", K(ret));
    } else if (OB_ISNULL(scalar_index_scan_ctdef) || OB_ISNULL(scalar_main_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan ctdef or main scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 2;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_index_scan_ctdef);
      new_scalar_ctdef->children_[1] = static_cast<ObDASBaseCtDef*>(scalar_main_scan_ctdef);
    }
  } else if (OB_ISNULL(new_scalar_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate children failed", K(ret));
  } else if (new_scalar_ctdef->has_index_scan()) {
    if (OB_ISNULL(scalar_index_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 1;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_index_scan_ctdef);
    }
  } else if (new_scalar_ctdef->has_main_scan()) {
    if (OB_ISNULL(scalar_main_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("main scan ctdef is null", K(ret));
    } else {
      new_scalar_ctdef->children_cnt_ = 1;
      new_scalar_ctdef->children_[0] = static_cast<ObDASBaseCtDef*>(scalar_main_scan_ctdef);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected case", K(ret));
  }

  if (OB_SUCC(ret)) {
    new_scalar_ctdef->set_is_top_level_scoring(scalar_query_node->is_top_level_scoring_);
    new_scalar_ctdef->set_is_scoring(scalar_query_node->is_scoring_);
    scalar_ctdef = new_scalar_ctdef;
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObBooleanQueryNode *boolean_query_node, ObDASBooleanQueryCtDef *&boolean_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASBooleanQueryCtDef *new_boolean_query_ctdef = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(boolean_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(boolean_query_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_BOOLEAN_QUERY, cg_.phy_plan_->get_allocator(), new_boolean_query_ctdef))) {
    LOG_WARN("failed to allocate boolean query ctdef", KR(ret));
  } else if (OB_ISNULL(new_boolean_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("boolean query ctdef is null", KR(ret));
  } else {
    int64_t total_children_cnt = boolean_query_node->must_nodes_.count() +
                                 boolean_query_node->filter_nodes_.count() +
                                 boolean_query_node->should_nodes_.count() +
                                 boolean_query_node->must_not_nodes_.count();
    int64_t min_should_match = static_cast<int64_t>(boolean_query_node->min_should_match_);
    if (min_should_match < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("min should match must be positive", KR(ret), K(min_should_match));
    } else if (boolean_query_node->should_nodes_.count() +
               boolean_query_node->must_nodes_.count() +
               boolean_query_node->filter_nodes_.count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there should be at least one positive clause", KR(ret));
    } else {
      int64_t current_offset = 0;
      if (min_should_match > boolean_query_node->should_nodes_.count()) {
        min_should_match = boolean_query_node->should_nodes_.count();
      }
      new_boolean_query_ctdef->set_min_should_match(min_should_match);
      if (boolean_query_node->must_nodes_.count() > 0) {
        new_boolean_query_ctdef->set_must(current_offset, boolean_query_node->must_nodes_.count());
        current_offset += boolean_query_node->must_nodes_.count();
      }
      if (boolean_query_node->should_nodes_.count() > 0) {
        new_boolean_query_ctdef->set_should(current_offset, boolean_query_node->should_nodes_.count());
        current_offset += boolean_query_node->should_nodes_.count();
      }
      if (boolean_query_node->filter_nodes_.count() > 0) {
        new_boolean_query_ctdef->set_filter(current_offset, boolean_query_node->filter_nodes_.count());
        current_offset += boolean_query_node->filter_nodes_.count();
      }
      if (boolean_query_node->must_not_nodes_.count() > 0) {
        new_boolean_query_ctdef->set_must_not(current_offset, boolean_query_node->must_not_nodes_.count());
        current_offset += boolean_query_node->must_not_nodes_.count();
      }
    }
  }
  if (OB_SUCC(ret)) {
    new_boolean_query_ctdef->set_is_top_level_scoring(boolean_query_node->is_top_level_scoring_);
    new_boolean_query_ctdef->set_is_scoring(boolean_query_node->is_scoring_);
    boolean_query_ctdef = new_boolean_query_ctdef;
  }
  return ret;
}

int ObHybridSearchCgService::generate_scalar_scan_ctdef(const DASScalarCGParams &cg_params,
                                                        ObDASScalarScanCtDef *&scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  ObDASScalarScanCtDef *new_scalar_ctdef = nullptr;
  if (!cg_params.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SCALAR_SCAN_QUERY, cg_.phy_plan_->get_allocator(), new_scalar_ctdef))) {
    LOG_WARN("failed to allocate scalar ctdef", K(ret));
  } else if (OB_ISNULL(new_scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
             OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx or schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(cg_params.get_index_table_id(), new_scalar_ctdef->schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(cg_params.get_index_table_id()));
  } else {
    new_scalar_ctdef->ref_table_id_ = cg_params.get_index_table_id();
    new_scalar_ctdef->is_get_ = cg_params.is_get();
    new_scalar_ctdef->is_new_query_range_ = cg_params.is_new_query_range();
    new_scalar_ctdef->need_rowkey_order_ = cg_params.need_rowkey_order();
    new_scalar_ctdef->is_rowkey_order_scan_ = cg_params.is_rowkey_order_scan();
    new_scalar_ctdef->is_primary_table_scan_ = cg_params.is_primary_table_scan();
  }

  // 1. generate scalar access
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(nullptr == cg_params.get_access_exprs() || cg_params.get_access_exprs()->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access exprs is empty", K(ret));
  } else {
    ObArray<uint64_t> access_col_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_params.get_access_exprs()->count(); ++i) {
      ObRawExpr *expr = cg_params.get_access_exprs()->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_UNLIKELY(T_REF_COLUMN != expr->get_expr_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported expr type", K(ret), K(*expr));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (OB_FAIL(access_col_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("push back column id failed", K(ret));
        }
      }
    } // end for

    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_access_exprs(), new_scalar_ctdef->pd_expr_spec_.access_exprs_))) {
        LOG_WARN("generate rt exprs failed", K(ret));
      } else if (OB_FAIL(new_scalar_ctdef->access_column_ids_.assign(access_col_ids))) {
        LOG_WARN("assign access column ids failed", K(ret));
      } else if (OB_FAIL(cg_.mark_expr_self_produced(*cg_params.get_access_exprs()))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      }
    }
  }

  ObSEArray<uint64_t, 4> aggregate_column_ids;
  ObSEArray<uint64_t, 4> group_by_column_ids;
  ObSEArray<ObAggrParamProperty, 4> aggregate_param_props;
  // generate pushdown aggr columns
  if (OB_FAIL(ret)) {
  } else if (nullptr == cg_params.get_pushdown_aggr_exprs() || cg_params.get_pushdown_aggr_exprs()->empty()) {
    // skip
  } else {
    const ObIArray<ObRawExpr*> &pushdown_aggr_exprs = *cg_params.get_pushdown_aggr_exprs();
    ExprFixedArray &aggregate_output = new_scalar_ctdef->pd_expr_spec_.pd_storage_aggregate_output_;
    new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
    if (OB_FAIL(aggregate_output.reserve(pushdown_aggr_exprs.count()))) {
      LOG_WARN("reserve aggregate output exprs failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_aggr_exprs.count(); ++i) {
      ObAggFunRawExpr *aggr_expr = static_cast<ObAggFunRawExpr *>(pushdown_aggr_exprs.at(i));
      ObRawExpr *param_expr = nullptr;
      ObColumnRefRawExpr* col_expr = nullptr;
      ObExpr *e = nullptr;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_expr(*aggr_expr, e))) {
        LOG_WARN("generate rt expr failed", K(ret));
      } else if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(aggregate_output.push_back(e))) {
        LOG_WARN("push back aggregate output expr failed", K(ret));
      } else if (OB_FAIL(cg_.mark_expr_self_produced(aggr_expr))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      } else if (aggr_expr->get_real_param_exprs().empty()) {
        OZ(aggregate_column_ids.push_back(OB_COUNT_AGG_PD_COLUMN_ID));
        OZ(aggregate_param_props.push_back(ObAggrParamProperty(Monotonicity::NONE_MONO, true)));
      } else if (OB_ISNULL(param_expr = aggr_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param expr", K(ret), KP(param_expr));
      } else {
        ObSEArray<ObRawExpr*, 1> column_exprs;
        // TODO: support aggr param property
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(param_expr, column_exprs))) {
          LOG_WARN("fail to extract column expr", K(ret));
        } else if (OB_UNLIKELY(column_exprs.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpcted column expr count", K(ret), K(column_exprs));
        } else {
          col_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
          OZ(aggregate_column_ids.push_back(col_expr->get_column_id()));
          OZ(aggregate_param_props.push_back(ObAggrParamProperty(Monotonicity::NONE_MONO, true)));
        }
      }
    }
  }

  // 2. generate scalar table param
  if (OB_FAIL(ret)) {
  } else {
    const ObTableID &index_table_id = cg_params.get_index_table_id();
    const ObTableID &data_table_id = cg_params.get_data_table_id();
    int64_t route_policy = 0;
    bool is_cs_replica_query = false;
    ObOptimizerContext *opt_ctx = nullptr;
    const ObBasicSessionInfo *session_info = nullptr;
    const ObTableSchema *table_schema = nullptr;

    if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
        OB_ISNULL(session_info = opt_ctx->get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid id", K(index_table_id), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(index_table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(index_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table schema", K(ret), K(index_table_id));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info->get_effective_tenant_id()));
      if (table_schema->is_fts_index() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_fts_index(true))) {
      } else if (table_schema->is_multivalue_index_aux() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_multivalue_index(true))) {
      } else if (table_schema->is_vec_index() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_vec_index(true))) {
      } else if (table_schema->get_semistruct_encoding_type().is_enable_semistruct_encoding() && FALSE_IT(new_scalar_ctdef->table_param_.set_is_enable_semistruct_encoding(true))) {
      } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
        LOG_WARN("get route policy failed", K(ret));
      } else {
        is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
        new_scalar_ctdef->table_param_.set_is_partition_table(table_schema->is_partitioned_table());
        new_scalar_ctdef->table_param_.set_is_mlog_table(table_schema->is_mlog_table());
        new_scalar_ctdef->table_param_.set_plan_enable_rich_format(opt_ctx->get_enable_rich_vector_format());
        new_scalar_ctdef->table_param_.get_enable_lob_locator_v2() = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cg_params.get_tsc_out_cols())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsc out cols is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->table_param_.convert(*table_schema,
                                                              new_scalar_ctdef->access_column_ids_,
                                                              new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                              cg_params.get_tsc_out_cols(),
                                                              is_oracle_mapping_real_virtual_table(data_table_id),
                                                              is_cs_replica_query))) {
      LOG_WARN("convert table param failed", K(ret), K(data_table_id), K(index_table_id));
    } else if (new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown() &&
        OB_FAIL(new_scalar_ctdef->table_param_.convert_group_by(*table_schema,
                                                                new_scalar_ctdef->access_column_ids_,
                                                                aggregate_column_ids,
                                                                group_by_column_ids,
                                                                aggregate_param_props,
                                                                new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_))) {
      LOG_WARN("convert group by failed", K(ret), KPC(table_schema));
    }
  }

  // 3. generate scalar result output
  if (OB_FAIL(ret)) {
  } else {
    const ExprFixedArray &access_exprs = new_scalar_ctdef->pd_expr_spec_.access_exprs_;
    const ExprFixedArray &agg_exprs = new_scalar_ctdef->pd_expr_spec_.pd_storage_aggregate_output_;
    int64_t access_expr_cnt = access_exprs.count();
    int64_t agg_expr_cnt = agg_exprs.count();
    int64_t access_column_cnt = new_scalar_ctdef->access_column_ids_.count();
    int64_t output_column_cnt = OB_INVALID_ID;
    int64_t domain_id_expr_cnt = OB_INVALID_ID;
    int64_t domain_id_col_ids_cnt = OB_INVALID_ID;
    if (OB_ISNULL(cg_params.get_tsc_out_cols())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsc out cols is null", K(ret));
    } else if (FALSE_IT(output_column_cnt = cg_params.get_tsc_out_cols()->count())) {
    } else if (OB_NOT_NULL(cg_params.get_domain_id_expr()) && FALSE_IT(domain_id_expr_cnt = cg_params.get_domain_id_expr()->count())) {
    } else if (OB_NOT_NULL(cg_params.get_domain_id_col_ids()) && FALSE_IT(domain_id_col_ids_cnt = cg_params.get_domain_id_col_ids()->count())) {
    } else if (OB_UNLIKELY(access_column_cnt != access_expr_cnt ||
                           OB_INVALID_ID == output_column_cnt ||
                           domain_id_expr_cnt != domain_id_col_ids_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt), K(domain_id_expr_cnt), K(domain_id_col_ids_cnt));
    } else if (OB_FAIL(new_scalar_ctdef->result_output_.init(output_column_cnt + agg_expr_cnt))) {
      LOG_WARN("init result output failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_cnt; ++i) {
        int64_t idx = OB_INVALID_INDEX;
        if (OB_NOT_NULL(cg_params.get_domain_id_col_ids()) &&
            OB_NOT_NULL(cg_params.get_domain_id_expr()) &&
            has_exist_in_array(*cg_params.get_domain_id_col_ids(), cg_params.get_tsc_out_cols()->at(i), &idx)) {
          ObExpr *domain_id_expr = nullptr;
          if (OB_ISNULL(cg_params.get_domain_id_expr()->at(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("domain id expr is null", K(ret));
          } else if (OB_FAIL(cg_.generate_rt_expr(*cg_params.get_domain_id_expr()->at(idx), domain_id_expr))) {
            LOG_WARN("generate rt expr failed", K(ret));
          } else if (OB_ISNULL(domain_id_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("domain id expr is null", K(ret));
          } else if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(domain_id_expr))) {
            LOG_WARN("push back domain id expr failed", K(ret), K(cg_params.get_domain_id_col_ids()->at(idx)));
          }
        } else if (!has_exist_in_array(new_scalar_ctdef->access_column_ids_, cg_params.get_tsc_out_cols()->at(i), &idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("output column does not exist in access column ids", K(ret),
                   K(new_scalar_ctdef->access_column_ids_), K(cg_params.get_tsc_out_cols()->at(i)), K(*cg_params.get_tsc_out_cols()),
                   K(cg_params.get_index_table_id()), K(cg_params.get_data_table_id()));
        } else if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(access_exprs.at(idx)))) {
          LOG_WARN("store access expr to result output exprs failed", K(ret));
        }
      }

      if (OB_SUCC(ret) && new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr_cnt; ++i) {
          if (OB_FAIL(new_scalar_ctdef->result_output_.push_back(agg_exprs.at(i)))) {
            LOG_WARN("store agg expr to result output exprs failed", K(ret));
          }
        }
      }
    }
  }

  // 4. generate scalar rowkey exprs
  if (OB_FAIL(ret)) {
  } else {
    if (OB_ISNULL(cg_params.get_rowkey_exprs()) || cg_params.get_rowkey_exprs()->empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey exprs is null", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_rowkey_exprs(), new_scalar_ctdef->rowkey_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret));
    }
  }

  // 5. generate scalar pushdown filters
  if (OB_FAIL(ret)) {
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", K(ret));
    } else if (OB_ISNULL(cg_params.get_access_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access exprs is null", K(ret));
    } else {
      bool pd_blockscan = ObPushdownFilterUtils::is_blockscan_pushdown_enabled(tenant_config->_pushdown_storage_level);
      bool pd_filter = ObPushdownFilterUtils::is_filter_pushdown_enabled(tenant_config->_pushdown_storage_level);
      bool enable_base_skip_index = tenant_config->_enable_skip_index;
      bool enable_inc_skip_index = tenant_config->_enable_skip_index;
      int tmp_ret = OB_E(EventTable::EN_DISABLE_INC_SKIP_INDEX_SCAN) OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        enable_inc_skip_index = false;
      }
      bool enable_prefetch_limit = tenant_config->_enable_prefetch_limiting;
      bool enable_column_store = cg_params.is_use_column_store();
      bool enable_filter_reordering = tenant_config->_enable_filter_reordering;

      new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_flags(pd_blockscan, pd_filter, enable_base_skip_index,
        enable_column_store, enable_prefetch_limit, enable_filter_reordering, enable_inc_skip_index);
      new_scalar_ctdef->table_scan_opt_.io_read_batch_size_ = tenant_config->_io_read_batch_size;
      new_scalar_ctdef->table_scan_opt_.io_read_gap_size_ = tenant_config->_io_read_batch_size * tenant_config->_io_read_redundant_limit_percentage / 100;
      new_scalar_ctdef->table_scan_opt_.storage_rowsets_size_ = tenant_config->storage_rowsets_size;

      if (!pd_blockscan) {
        pd_filter = false;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && (pd_blockscan || pd_filter) && i < cg_params.get_access_exprs()->count(); ++i) {
          ObRawExpr *expr = cg_params.get_access_exprs()->at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret));
          } else if ((enable_column_store && T_ORA_ROWSCN == expr->get_expr_type()) ||
                      T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type() ||
                      T_PSEUDO_OLD_NEW_COL == expr->get_expr_type()) {
            pd_blockscan = false;
            pd_filter = false;
          } else if (expr->is_column_ref_expr()) {
            const ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr *>(expr);
            if (col->is_lob_column() && cg_.get_cur_cluster_version() < CLUSTER_VERSION_4_1_0_0) {
              pd_filter = false;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        enable_base_skip_index = enable_base_skip_index && pd_filter;
        enable_inc_skip_index = enable_inc_skip_index && pd_filter;
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(pd_filter);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(enable_base_skip_index);
        new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(enable_inc_skip_index);
        LOG_TRACE("pushdown filters flag and table scan opt", K(cg_params.get_index_table_id()),
                                                              "pd storage flag", new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                              "table scan opt", new_scalar_ctdef->table_scan_opt_);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(cg_params.get_pushdown_filters()) && !cg_params.get_pushdown_filters()->empty()) {
      // generate pushdown filters
      if (OB_FAIL(cg_.generate_rt_exprs(*cg_params.get_pushdown_filters(), new_scalar_ctdef->pd_expr_spec_.pushdown_filters_))) {
        LOG_WARN("generate pushdown filters failed", K(ret));
      } else if (new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
        ObPushdownFilterConstructor filter_constructor(&cg_.phy_plan_->get_allocator(),
                                                       cg_,
                                                       cg_params.get_filter_monotonicity(),
                                                       new_scalar_ctdef->pd_expr_spec_.pd_storage_flag_.is_use_column_store(),
                                                       cg_params.get_table_type(),
                                                       new_scalar_ctdef->table_param_.is_enable_semistruct_encoding());
        if (OB_FAIL(filter_constructor.apply(*cg_params.get_pushdown_filters(), new_scalar_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to apply filter constructor", K(ret));
        } else if (OB_FAIL(new_scalar_ctdef->table_param_.check_is_safe_filter_with_di(*cg_params.get_pushdown_filters(),
                                                                                       *new_scalar_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to check lob column pushdown", K(ret));
        }
        LOG_TRACE("pushdown filters", K(cg_params.get_index_table_id()), K(*cg_params.get_pushdown_filters()));
      }
    }
  }

  // 6. generate scalar query range
  if (OB_FAIL(ret) || !cg_params.has_pre_query_range()) {
  } else if (cg_params.is_new_query_range()) {
    if (OB_ISNULL(cg_params.get_pre_range_graph())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pre range graph is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->pre_range_graph_.deep_copy(*cg_params.get_pre_range_graph()))) {
      LOG_WARN("deep copy pre range graph failed", K(ret));
    } else {
      new_scalar_ctdef->enable_new_false_range_ = cg_params.get_pre_range_graph()->enable_new_false_range();
    }
  } else {
    if (OB_ISNULL(cg_params.get_pre_query_range())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pre query range is null", K(ret));
    } else if (OB_FAIL(new_scalar_ctdef->pre_query_range_.deep_copy(*cg_params.get_pre_query_range()))) {
      LOG_WARN("deep copy pre query range failed", K(ret));
    } else {
      new_scalar_ctdef->enable_new_false_range_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    scan_ctdef = new_scalar_ctdef;
    LOG_TRACE("generate scalar search ctdef success", KPC(scan_ctdef));
  }
  return ret;
}

// In the hybrid search scenario, tsc_ctdef.scan_ctdef_ is not used to perform scans, but it undertakes
// functions such as partition routing and represents the primary partition corresponding to the DAS SCAN OP.
// Therefore, it is still necessary to fill the required information.
int ObHybridSearchCgService::mock_das_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = nullptr;
  common::ObTableID main_table_id = op.get_ref_table_id();
  if (OB_ISNULL(cg_.opt_ctx_) ||
      OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(main_table_id,
                                                            scan_ctdef.schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(main_table_id));
  } else {
    scan_ctdef.ref_table_id_ = main_table_id;
    if (OB_NOT_NULL(op.get_plan()->get_stmt()) && OB_NOT_NULL(op.get_plan()->get_stmt()->get_query_ctx())) {
      scan_ctdef.hybrid_search_monitor_ = op.get_plan()->get_stmt()->get_query_ctx()->get_global_hint().monitor_;
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_final_lookup_ctdef(ObLogTableScan &op,
                                                         ObTableScanCtDef &tsc_ctdef,
                                                         ObDASBaseCtDef &search_ctdef,
                                                         ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDef *main_scan_ctdef = nullptr;
  ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  ObDASTableLocMeta *loc_meta = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) ||
      OB_ISNULL(cg_.opt_ctx_) ||
      OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.phy_plan_), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(), op.get_ref_table_id(), op.get_stmt(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(op.get_table_id()), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr table schema", K(ret));
  } else {
    ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    if (OB_ISNULL(loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate lookup location meta buffer failed", K(ret));
    } else {
      loc_meta->reset();
      loc_meta->table_loc_id_ = op.get_table_id();
      loc_meta->ref_table_id_ = op.get_real_ref_table_id();
      // force select leader for now
      loc_meta->select_leader_ = 1;
      TableLocRelInfo *rel_info = cg_.opt_ctx_->get_loc_rel_info_by_id(op.get_table_id(), op.get_real_ref_table_id());
      if (OB_ISNULL(rel_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("relation info is nullptr", K(ret), K(op.get_table_id()), K(op.get_real_ref_table_id()));
      } else if (rel_info->related_ids_.count() > 1) {
        loc_meta->related_table_ids_.set_capacity(rel_info->related_ids_.count() - 1);
        for (int64_t i = 0; OB_SUCC(ret) && i < rel_info->related_ids_.count(); ++i) {
          if (rel_info->related_ids_.at(i) != loc_meta->ref_table_id_) {
            if (OB_FAIL(loc_meta->related_table_ids_.push_back(rel_info->related_ids_.at(i)))) {
              LOG_WARN("store related table id failed", K(ret), KPC(rel_info), K(i), K(loc_meta));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
    const bool is_dsl_query = is_dsl_query_path(op.get_access_path());
    if (OB_FAIL(generate_main_scan_ctdef(op, main_scan_ctdef))) {
      LOG_WARN("failed to generate main scan ctdef", K(ret), K(op.get_ref_table_id()));
    } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
        is_dsl_query ? DAS_OP_INDEX_PROJ_LOOKUP : DAS_OP_TABLE_LOOKUP, allocator, lookup_ctdef))) {
      LOG_WARN("alloc lookup ctdef failed", K(ret), K(is_dsl_query));
    } else if (OB_ISNULL(lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      lookup_ctdef->children_cnt_ = 2;
      if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(loc_meta))) {
        LOG_WARN("store scan loc meta failed", K(ret));
      } else {
        lookup_ctdef->children_[0] = &search_ctdef;
        lookup_ctdef->children_[1] = main_scan_ctdef;
      }
    }
  }

  // generate lookup result output exprs
  if (OB_SUCC(ret)) {
    ObArray<ObExpr *> result_outputs;
    if (!ObDASTaskFactory::is_attached(search_ctdef.op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search ctdef is not attached", K(ret), K(search_ctdef.op_type_));
    } else if (OB_FAIL(result_outputs.assign(main_scan_ctdef->result_output_))) {
      LOG_WARN("assign result output failed", K(ret));
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == lookup_ctdef->op_type_) {
      // DSL query need to project the result output of the search ctdef
      ObDASIndexProjLookupCtDef *index_proj_lookup_ctdef = static_cast<ObDASIndexProjLookupCtDef *>(lookup_ctdef);
      if (OB_FAIL(index_proj_lookup_ctdef->index_scan_proj_exprs_.assign(
        static_cast<ObDASAttachCtDef &>(search_ctdef).result_output_))) {
        LOG_WARN("failed to assign index scan project column exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(result_outputs, index_proj_lookup_ctdef->index_scan_proj_exprs_))) {
        LOG_WARN("failed to append final result outputs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      } else {
        root_ctdef = lookup_ctdef;
        LOG_TRACE("generate final lookup ctdef", KPC(root_ctdef));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_main_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef *&main_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(cg_.opt_ctx_) || OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(cg_.phy_plan_), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN,
                                                       cg_.phy_plan_->get_allocator(),
                                                       scan_ctdef))) {
    LOG_WARN("failed to allocate scan ctdef", K(ret));
  } else if (OB_ISNULL(scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan ctdef is null", K(ret));
  } else {
    common::ObTableID ref_table_id = op.get_real_ref_table_id();
    scan_ctdef->ref_table_id_ = ref_table_id;
    ObArray<ObRawExpr *> filters;
    ObArray<ObRawExpr *> lookup_pushdown_filters;
    ObArray<ObRawExpr *> non_pushdown_filters;
    ObArray<ObRawExpr *> access_exprs;
    const bool is_dsl_query = is_dsl_query_path(op.get_access_path());
    if (is_dsl_query) {
      if (!op.get_filter_exprs().empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dsl query should not have additional filters", K(ret), K(ref_table_id));
      }
    } else {
      if (OB_FAIL(filters.assign(op.get_filter_exprs()))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (OB_FAIL(op.extract_nonpushdown_filters(filters, non_pushdown_filters, lookup_pushdown_filters))) {
        LOG_WARN("failed to extract pushdown filters", K(ret));
      }
    }

    // extract access exprs
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> filter_columns;
      if (OB_FAIL(access_exprs.assign(op.get_access_exprs()))) {
        LOG_WARN("failed to assign access exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filters, filter_columns))) {
        LOG_WARN("failed to extract filter columns", K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, filter_columns))) {
        LOG_WARN("failed to append filter columns", K(ret));
      } else if (OB_NOT_NULL(op.get_auto_split_filter())) {
        ObArray<ObRawExpr *> auto_split_filter_columns;
        ObRawExpr *auto_split_expr = const_cast<ObRawExpr *>(op.get_auto_split_filter());
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(auto_split_expr,
                                                         auto_split_filter_columns))) {
          LOG_WARN("extract column exprs failed", K(ret));
        } else if (OB_FAIL(append_array_no_dup(access_exprs, auto_split_filter_columns))) {
          LOG_WARN("append filter column to access exprs failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTscCgService::remove_virtual_generated_access_exprs(op, cg_, access_exprs))) {
          LOG_WARN("failed to remove virtual generated access exprs", K(ret));
        }
      }
      LOG_TRACE("main scan access exprs", K(ret), K(ref_table_id), K(access_exprs));
    }

    // generate access column ids
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> access_column_ids;
      if (OB_FAIL(extract_column_ids(access_exprs, access_column_ids))) {
        LOG_WARN("failed to extract access column ids", K(ret));
      } else if (OB_FAIL(check_column_ids_accessible(ref_table_id, *schema_guard, access_column_ids))) {
        LOG_WARN("column ids are not accessible", K(ret));
      } else {
        if (OB_FAIL(cg_.generate_rt_exprs(access_exprs, scan_ctdef->pd_expr_spec_.access_exprs_))) {
          LOG_WARN("failed to generate access exprs", K(ret));
        } else if (OB_FAIL(cg_.mark_expr_self_produced(access_exprs))) {
          LOG_WARN("makr expr self produced failed", K(ret), K(access_exprs));
        } else if (OB_FAIL(scan_ctdef->access_column_ids_.assign(access_column_ids))) {
          LOG_WARN("assign output column ids failed", K(ret));
        }
        LOG_TRACE("main scan access column ids", K(ret), K(ref_table_id), K(access_column_ids));
      }
    }

    // generate auto split exprs
    if (OB_SUCC(ret)) {
      ObRawExpr *auto_split_expr = const_cast<ObRawExpr*>(op.get_auto_split_filter());
      const uint64_t auto_split_filter_type = op.get_auto_split_filter_type();
      if (OB_NOT_NULL(auto_split_expr) && OB_INVALID_ID != auto_split_filter_type) {
        if (OB_FAIL(cg_.generate_rt_expr(*auto_split_expr,
                                         scan_ctdef->pd_expr_spec_.auto_split_expr_))) {
          LOG_WARN("generate auto split filter expr failed", K(ret));
        } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_auto_split_params(),
                                                 scan_ctdef->pd_expr_spec_.auto_split_params_))) {
          LOG_WARN("generate auto split params failed", K(ret));
        } else {
          scan_ctdef->pd_expr_spec_.auto_split_filter_type_ = auto_split_filter_type;
        }
      }
    }

    // generate table schema version
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard->get_table_schema_version(ref_table_id,
                                                         scan_ctdef->schema_version_))) {
        LOG_WARN("get table schema version failed", K(ret), K(ref_table_id));
      }
    }

    // generate output column ids and table param
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> output_exprs;
      ObArray<ObRawExpr *> output_column_exprs;
      ObArray<uint64_t> output_column_ids;
      if (OB_FAIL(output_exprs.assign(op.get_output_exprs()))) {
        LOG_WARN("failed to assign output exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(output_exprs, non_pushdown_filters))) {
        LOG_WARN("failed to append filter exprs to output exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(output_exprs, output_column_exprs, true))) {
        LOG_WARN("failed to extract output column exprs", K(ret));
      } else if (OB_FAIL(extract_column_ids(output_column_exprs, output_column_ids))) {
        LOG_WARN("failed to extract output column ids", K(ret));
      } else {
        ObBasicSessionInfo *session_info = cg_.opt_ctx_->get_session_info();
        const ObTableSchema *table_schema = nullptr;
        int64_t route_policy = 0;
        if (OB_ISNULL(session_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr session info", K(ret), K(session_info));
        } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                          ref_table_id,
                                                          op.get_stmt(),
                                                          table_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(op.get_table_id()), K(ref_table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr table schema", K(ret), K(op.get_table_id()), K(ref_table_id));
        } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
          LOG_WARN("failed to get route policy", K(ret));
        } else if (FALSE_IT(scan_ctdef->table_param_.get_enable_lob_locator_v2()
                            = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
        } else if (OB_FAIL(scan_ctdef->table_param_.convert(*table_schema,
                                                            scan_ctdef->access_column_ids_,
                                                            scan_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                            &output_column_ids,
                                                            false, /* not oracle mapping real virtual table */
                                                            ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy))) {
          LOG_WARN("convert schema failed", K(ret), K(*table_schema), K(scan_ctdef->access_column_ids_));
        } else if (OB_FAIL(scan_ctdef->result_output_.init(output_column_ids.count()))) {
          LOG_WARN("init result output failed", K(ret), K(ref_table_id), K(output_column_ids));
        } else if (OB_UNLIKELY(scan_ctdef->pd_expr_spec_.access_exprs_.count() != scan_ctdef->access_column_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("access exprs count is not equal to access column ids count", K(ret),
                   K(ref_table_id), K(scan_ctdef->pd_expr_spec_.access_exprs_.count()), K(scan_ctdef->access_column_ids_.count()),
                   K(scan_ctdef->pd_expr_spec_.access_exprs_), K(scan_ctdef->access_column_ids_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids.count(); i++) {
            int64_t idx = OB_INVALID_INDEX;
            if (!has_exist_in_array(scan_ctdef->access_column_ids_, output_column_ids.at(i), &idx)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("output column does not exist in access column ids", K(ret),
                       K(ref_table_id), K(i), K(output_column_ids), K(scan_ctdef->access_column_ids_));
            } else if (OB_FAIL(scan_ctdef->result_output_.push_back(scan_ctdef->pd_expr_spec_.access_exprs_.at(idx)))) {
              LOG_WARN("store access expr to result output exprs failed", K(ret));
            }
          }
        }
      }
      LOG_TRACE("main scan output column ids", K(ret), K(ref_table_id), K(output_column_ids));
    }

    // generate pd storage flag and pushdown filters
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_.generate_tsc_flags(op, *scan_ctdef))) {
        LOG_WARN("failed to generate tsc flags", K(ret));
      } else {
        bool pd_blockscan = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_blockscan_pushdown();
        bool pd_filter = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown();
        bool enable_base_skip_index = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_apply_base_skip_index();
        bool enable_inc_skip_index = scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_apply_inc_skip_index();
        if (!pd_blockscan) {
          pd_filter = false;
        } else {
          FOREACH_CNT_X(e, op.get_access_exprs(), pd_blockscan || pd_filter) {
            if ((op.use_column_store() && T_ORA_ROWSCN == (*e)->get_expr_type())
                || T_PSEUDO_EXTERNAL_FILE_URL == (*e)->get_expr_type()
                || T_PSEUDO_OLD_NEW_COL == (*e)->get_expr_type()) {
              pd_blockscan = false;
              pd_filter = false;
            }
          }
        }
        enable_base_skip_index = enable_base_skip_index && pd_filter;
        enable_inc_skip_index = enable_inc_skip_index && pd_filter;
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_filter_pushdown(pd_filter);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_base_skip_index(enable_base_skip_index);
        scan_ctdef->pd_expr_spec_.pd_storage_flag_.set_enable_inc_skip_index(enable_inc_skip_index);
        LOG_TRACE("main scan pd storage flag", K(ref_table_id), K(pd_blockscan), K(pd_filter), K(enable_base_skip_index), K(enable_inc_skip_index));
        if (!lookup_pushdown_filters.empty()) {
          if (OB_FAIL(cg_.generate_rt_exprs(lookup_pushdown_filters, scan_ctdef->pd_expr_spec_.pushdown_filters_))) {
            LOG_WARN("failed to generate lookup pushdown filters", K(ret));
          } else if (scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
            ObPushdownFilterConstructor filter_constructor(
              &cg_.phy_plan_->get_allocator(), cg_,
              &op.get_filter_monotonicity(),
              scan_ctdef->pd_expr_spec_.pd_storage_flag_.is_use_column_store(),
              op.get_table_type(),
              scan_ctdef->table_param_.is_enable_semistruct_encoding());
            if (OB_FAIL(filter_constructor.apply(
              lookup_pushdown_filters, scan_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
              LOG_WARN("failed to apply filter constructor", K(ret));
            } else if (OB_FAIL(scan_ctdef->table_param_.check_is_safe_filter_with_di(
                        lookup_pushdown_filters,
                        *scan_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
              LOG_WARN("failed to check lob column pushdown", K(ret));
            }
            LOG_TRACE("main scan pushdown filters", K(ref_table_id), K(lookup_pushdown_filters));
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(scan_ctdef)) {
      main_scan_ctdef = scan_ctdef;
      main_scan_ctdef->is_get_ = true;
    }
  }
  return ret;
}

int ObHybridSearchCgService::extract_column_ids(const ObIArray<ObRawExpr *> &column_exprs,
                                                ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    if (OB_ISNULL(column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_ORA_ROWSCN == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("store ora rowscan failed", K(ret));
      }
    } else if (T_PSEUDO_OLD_NEW_COL == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID))) {
        LOG_WARN("store mview old new column id failed", K(ret));
      }
    } else if (T_PSEUDO_GROUP_ID == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_GROUP_IDX_COLUMN_ID))) {
        LOG_WARN("store group column id failed", K(ret));
      }
    } else if (column_exprs.at(i)->is_column_ref_expr()) {
      const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr*>(column_exprs.at(i));
      if (col_expr->is_pseudo_column_ref()) {
        //part_id pseudo columnref exprs not produced in DAS, ignore it
      } else if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store column id failed", K(ret));
      }
    } else {
      //other column exprs not produced in DAS, ignore it
    }
  }
  return ret;
}

int ObHybridSearchCgService::check_column_ids_accessible(
  uint64_t ref_table_id, const ObSqlSchemaGuard &schema_guard, const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema", K(ret), K(ref_table_id));
  } else {
    ObArray<ObColDesc> column_descs;
    if (OB_FAIL(table_schema->get_store_column_ids(column_descs))) {
      LOG_WARN("failed to get store column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
        bool is_found = false;
        for (int64_t j = 0; !is_found && j < column_descs.count(); j++) {
          if (column_ids.at(i) == column_descs.at(j).col_id_) {
            is_found = true;
          }
        }
        if (!is_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column id is not accessible", K(ret), K(i), K(ref_table_id), K(column_ids), K(column_descs));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::try_alloc_topk_collect_ctdef(ObDASFusionCtDef *fusion_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  if (OB_ISNULL(fusion_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fusion_ctdef));
  } else if (fusion_ctdef->need_topk()
      && fusion_ctdef->has_search_subquery()
      && fusion_ctdef->need_search_score()
      && fusion_ctdef->get_search_ctdef()->is_scoring()) {
    ObDASTopKCollectCtDef *topk_collect_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TOPK_COLLECT, allocator, topk_collect_ctdef))) {
      LOG_WARN("failed to allocate topk collect ctdef", K(ret));
    } else if (OB_ISNULL(topk_collect_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null top k collect ctdef", K(ret));
    } else if (OB_ISNULL(topk_collect_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate fusion ctdef children failed", K(ret));
    } else {
      topk_collect_ctdef->limit_ = fusion_ctdef->rank_window_size_expr_;
      const int64_t search_idx = fusion_ctdef->get_search_idx();
      topk_collect_ctdef->children_[0] = fusion_ctdef->children_[search_idx];
      topk_collect_ctdef->children_cnt_ = 1;
      fusion_ctdef->children_[search_idx] = topk_collect_ctdef;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
