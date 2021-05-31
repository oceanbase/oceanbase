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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_pushdown_filter.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "storage/ob_i_store.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {

ObPushdownFilterFactory::PDFilterAllocFunc
    ObPushdownFilterFactory::PD_FILTER_ALLOC[PushdownFilterType::MAX_FILTER_TYPE] = {
        ObPushdownFilterFactory::alloc<ObPushdownBlackFilterNode, BLACK_FILTER>,
        ObPushdownFilterFactory::alloc<ObPushdownWhiteFilterNode, WHITE_FILTER>,
        ObPushdownFilterFactory::alloc<ObPushdownAndFilterNode, AND_FILTER>,
        ObPushdownFilterFactory::alloc<ObPushdownOrFilterNode, OR_FILTER>};

ObPushdownFilterFactory::FilterExecutorAllocFunc
    ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[FilterExecutorType::MAX_EXECUTOR_TYPE] = {
        ObPushdownFilterFactory::alloc<ObBlackFilterExecutor, ObPushdownBlackFilterNode, BLACK_FILTER_EXECUTOR>,
        ObPushdownFilterFactory::alloc<ObWhiteFilterExecutor, ObPushdownWhiteFilterNode, WHITE_FILTER_EXECUTOR>,
        ObPushdownFilterFactory::alloc<ObAndFilterExecutor, ObPushdownAndFilterNode, AND_FILTER_EXECUTOR>,
        ObPushdownFilterFactory::alloc<ObOrFilterExecutor, ObPushdownOrFilterNode, OR_FILTER_EXECUTOR>};

OB_SERIALIZE_MEMBER(ObPushdownFilterNode, type_, n_child_, col_ids_);
OB_SERIALIZE_MEMBER((ObPushdownAndFilterNode, ObPushdownFilterNode));
OB_SERIALIZE_MEMBER((ObPushdownOrFilterNode, ObPushdownFilterNode));
OB_SERIALIZE_MEMBER((ObPushdownBlackFilterNode, ObPushdownFilterNode), column_exprs_, filter_exprs_);
OB_SERIALIZE_MEMBER((ObPushdownWhiteFilterNode, ObPushdownFilterNode));

int ObPushdownBlackFilterNode::merge(ObIArray<ObPushdownFilterNode*>& merged_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filter_exprs_.init(1 + merged_node.count()))) {
    LOG_WARN("failed to init exprs", K(ret));
  } else if (OB_FAIL(filter_exprs_.push_back(tmp_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    for (int64_t i = 0; i < merged_node.count() && OB_SUCC(ret); ++i) {
      ObPushdownBlackFilterNode* black_node = static_cast<ObPushdownBlackFilterNode*>(merged_node.at(i));
      if (OB_ISNULL(black_node->tmp_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: exprs must be only one", K(ret));
      } else {
        if (OB_FAIL(filter_exprs_.push_back(black_node->tmp_expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPushdownBlackFilterNode::postprocess()
{
  int ret = OB_SUCCESS;
  if (0 == filter_exprs_.count()) {
    OZ(filter_exprs_.init(1));
    OZ(filter_exprs_.push_back(tmp_expr_));
  }
  return ret;
}

int ObPushdownFilterConstructor::create_black_filter_node(ObRawExpr* raw_expr, ObPushdownFilterNode*& filter_node)
{
  int ret = OB_SUCCESS;
  ObExpr* expr = nullptr;
  ObArray<ObRawExpr*> column_exprs;
  ObPushdownBlackFilterNode* black_filter_node = nullptr;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null raw expr", K(ret));
  } else if (OB_FAIL(static_cg_.generate_rt_expr(*raw_expr, expr))) {
    LOG_WARN("failed to generate rt expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(raw_expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(factory_.alloc(PushdownFilterType::BLACK_FILTER, 0, filter_node))) {
    LOG_WARN("failed t o alloc pushdown filter", K(ret));
  } else if (OB_ISNULL(filter_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("black filter node is null", K(ret));
  } else if (FALSE_IT(black_filter_node = static_cast<ObPushdownBlackFilterNode*>(filter_node))) {
  } else if (0 < column_exprs.count()) {
    OZ(black_filter_node->col_ids_.init(column_exprs.count()));
    OZ(black_filter_node->column_exprs_.init(column_exprs.count()));
    for (int64_t i = 0; i < column_exprs.count() && OB_SUCC(ret); ++i) {
      ObRawExpr* sub_raw_expr = column_exprs.at(i);
      ObExpr* sub_expr = nullptr;
      ObColumnRefRawExpr* ref_expr = static_cast<ObColumnRefRawExpr*>(sub_raw_expr);
      if (OB_FAIL(static_cg_.generate_rt_expr(*sub_raw_expr, sub_expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(black_filter_node->col_ids_.push_back(ref_expr->get_column_id()))) {
        LOG_WARN("failed to push back column id", K(ret));
      } else if (OB_FAIL(black_filter_node->column_exprs_.push_back(sub_expr))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    black_filter_node->tmp_expr_ = expr;
    LOG_DEBUG("debug black_filter_node", K(*raw_expr), K(*expr), K(black_filter_node->col_ids_));
  }
  return ret;
}

int ObPushdownFilterConstructor::merge_filter_node(
    ObPushdownFilterNode* dst, ObPushdownFilterNode* other, ObIArray<ObPushdownFilterNode*>& merged_node, bool& merged)
{
  int ret = OB_SUCCESS;
  merged = false;
  if (OB_ISNULL(other) || OB_ISNULL(dst) || dst == other) {
  } else if (dst->get_type() == other->get_type()) {
    if (dst->get_type() == PushdownFilterType::BLACK_FILTER) {
      if (is_array_equal(dst->get_col_ids(), other->get_col_ids())) {
        merged = true;
        LOG_DEBUG("merged filter", K(dst->get_col_ids()));
        if (OB_FAIL(merged_node.push_back(other))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        LOG_DEBUG("unmerged filter", K(other->get_col_ids()));
      }
    }
  }
  return ret;
}

int ObPushdownFilterConstructor::deduplicate_filter_node(
    ObIArray<ObPushdownFilterNode*>& filter_nodes, uint32_t& n_node)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < filter_nodes.count() - 1 && OB_SUCC(ret); ++i) {
    ObArray<ObPushdownFilterNode*> merged_node;
    for (int64_t j = i + 1; j < filter_nodes.count() && OB_SUCC(ret); ++j) {
      bool merged = false;
      if (OB_FAIL(merge_filter_node(filter_nodes.at(i), filter_nodes.at(j), merged_node, merged))) {
        LOG_WARN("failed to merge filter node", K(ret));
      } else if (merged) {
        filter_nodes.at(j) = nullptr;
        --n_node;
      }
    }
    if (0 < merged_node.count()) {
      if (OB_ISNULL(filter_nodes.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter node is null", K(ret), K(i));
      } else if (OB_FAIL(filter_nodes.at(i)->merge(merged_node))) {
        LOG_WARN("failed to merge filter node", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilterConstructor::apply(ObRawExpr* raw_expr, ObPushdownFilterNode*& filter_tree)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterNode* filter_node = nullptr;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw expr is null", K(ret));
  } else if (can_split_or(raw_expr)) {
    // flatten or expr and create OR filter
    // if (OB_FAIL(factory_.alloc(PushdownFilterType::OR, filter_node))) {
    //   LOG_WARN("failed t o alloc pushdown filter", K(ret));
    // }
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", K(ret));
  } else if (is_white_mode(raw_expr)) {
    // create white filter
    // if (OB_FAIL(factory_.alloc(PushdownFilterType::WHITE, filter_node))) {
    //   LOG_WARN("failed t o alloc pushdown filter", K(ret));
    // }
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", K(ret));
  } else {
    if (OB_FAIL(create_black_filter_node(raw_expr, filter_node))) {
      LOG_WARN("failed t o alloc pushdown filter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    filter_tree = filter_node;
  }
  return ret;
}

int ObPushdownFilterConstructor::apply(ObIArray<ObRawExpr*>& exprs, ObPushdownFilterNode*& filter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: alloc is null or expr is null", K(ret), KP(alloc_));
  } else if (0 < exprs.count()) {
    if (1 == exprs.count()) {
      ObPushdownFilterNode* filter_node = nullptr;
      if (OB_FAIL(apply(exprs.at(0), filter_node))) {
        LOG_WARN("failed to apply expr", K(ret));
      } else {
        filter_tree = filter_node;
        OZ(filter_node->postprocess());
      }
    } else {
      ObArray<ObPushdownFilterNode*> tmp_filter_nodes;
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
        ObRawExpr* raw_expr = exprs.at(i);
        ObPushdownFilterNode* sub_filter_node = nullptr;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw expr is null", K(ret));
        } else if (OB_FAIL(apply(raw_expr, sub_filter_node))) {
          LOG_WARN("failed to apply raw expr", K(ret));
        } else if (OB_FAIL(tmp_filter_nodes.push_back(sub_filter_node))) {
          LOG_WARN("failed to push back filter node", K(ret));
        }
      }
      ObPushdownFilterNode* filter_node = nullptr;
      uint32_t n_valid_node = (uint32_t)tmp_filter_nodes.count();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(deduplicate_filter_node(tmp_filter_nodes, n_valid_node))) {
        LOG_WARN("failed to deduplicate filter node", K(ret));
      } else if (1 != n_valid_node) {
        if (OB_FAIL(factory_.alloc(PushdownFilterType::AND_FILTER, n_valid_node, filter_node))) {
          LOG_WARN("failed t o alloc pushdown filter", K(ret));
        } else {
          uint32_t n_node = 0;
          for (int64_t i = 0; i < tmp_filter_nodes.count() && OB_SUCC(ret); ++i) {
            if (nullptr != tmp_filter_nodes.at(i)) {
              if (n_node >= n_valid_node) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpeced status: valid node count", K(ret), K(n_node), K(n_valid_node));
              } else {
                OZ(tmp_filter_nodes.at(i)->postprocess());
                filter_node->childs_[n_node] = tmp_filter_nodes.at(i);
                ++n_node;
              }
            }
          }
        }
      } else {
        filter_node = tmp_filter_nodes.at(0);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(filter_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter node is null", K(ret));
      } else {
        filter_tree = filter_node;
      }
    }
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(PushdownFilterType type, uint32_t n_child, ObPushdownFilterNode*& pd_filter)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER <= type && type < MAX_FILTER_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::PD_FILTER_ALLOC[type](*alloc_, n_child, pd_filter))) {
    OB_LOG(WARN, "fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, PushdownFilterType type>
int ObPushdownFilterFactory::alloc(common::ObIAllocator& alloc, uint32_t n_child, ObPushdownFilterNode*& pd_filter)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc pushdown filter", K(ret));
  } else {
    pd_filter = new (buf) ClassT(alloc);
    if (0 < n_child) {
      void* tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterNode*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(ERROR, "fail to alloc pushdown filter", K(ret));
      } else {
        pd_filter->childs_ = reinterpret_cast<ObPushdownFilterNode**>(tmp_buf);
      }
      pd_filter->n_child_ = n_child;
    } else {
      pd_filter->childs_ = nullptr;
    }
    pd_filter->set_type(type);
  }
  return ret;
}

int ObPushdownFilterFactory::alloc(FilterExecutorType type, uint32_t n_child, ObPushdownFilterNode& filter_node,
    ObPushdownFilterExecutor*& filter_executor)
{
  int ret = OB_SUCCESS;
  if (!(BLACK_FILTER_EXECUTOR <= type && type < MAX_EXECUTOR_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid filter type", K(ret), K(type));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "Unexpected null allocator", K(ret));
  } else if (OB_ISNULL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid filter type to alloc", K(ret), K(type));
  } else if (OB_FAIL(ObPushdownFilterFactory::FILTER_EXECUTOR_ALLOC[type](
                 *alloc_, n_child, filter_node, filter_executor))) {
    OB_LOG(WARN, "fail to alloc pushdown filter", K(ret), K(type));
  } else {
  }
  return ret;
}

template <typename ClassT, typename FilterNodeT, FilterExecutorType type>
int ObPushdownFilterFactory::alloc(common::ObIAllocator& alloc, uint32_t n_child, ObPushdownFilterNode& filter_node,
    ObPushdownFilterExecutor*& filter_executor)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc pushdown filter", K(ret), K(sizeof(ClassT)));
  } else {
    filter_executor = new (buf) ClassT(alloc, *static_cast<FilterNodeT*>(&filter_node));
    if (0 < n_child) {
      void* tmp_buf = alloc.alloc(n_child * sizeof(ObPushdownFilterExecutor*));
      if (OB_ISNULL(tmp_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(ERROR, "fail to alloc pushdown filter", K(ret));
      }
      filter_executor->set_childs(n_child, reinterpret_cast<ObPushdownFilterExecutor**>(tmp_buf));
    }
    filter_executor->set_type(type);
  }
  return ret;
}

int ObPushdownFilter::serialize_pushdown_filter(
    char* buf, int64_t buf_len, int64_t& pos, ObPushdownFilterNode* pd_storage_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pd_storage_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pd filter is null", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, pd_storage_filter->type_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, pd_storage_filter->n_child_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(pd_storage_filter->serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    for (int64_t i = 0; i < pd_storage_filter->n_child_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(serialize_pushdown_filter(buf, buf_len, pos, pd_storage_filter->childs_[i]))) {
        LOG_WARN("failed to serialize pushdown storage filter", K(ret));
      }
    }
  }
  return ret;
}

int ObPushdownFilter::deserialize_pushdown_filter(ObPushdownFilterFactory filter_factory, const char* buf,
    int64_t data_len, int64_t& pos, ObPushdownFilterNode*& pd_storage_filter)
{
  int ret = OB_SUCCESS;
  int32_t filter_type;
  uint32_t child_cnt = 0;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &filter_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else {
    if (OB_FAIL(filter_factory.alloc(static_cast<PushdownFilterType>(filter_type), child_cnt, pd_storage_filter))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else if (OB_FAIL(pd_storage_filter->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    } else if (0 < child_cnt) {
      for (uint32_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
        ObPushdownFilterNode* sub_pd_storage_filter = nullptr;
        if (OB_FAIL(deserialize_pushdown_filter(filter_factory, buf, data_len, pos, sub_pd_storage_filter))) {
          LOG_WARN("failed to deserialize child", K(ret));
        } else {
          pd_storage_filter->childs_[i] = sub_pd_storage_filter;
        }
      }
      if (pd_storage_filter->n_child_ != child_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child count is not match", K(ret), K(pd_storage_filter->n_child_), K(child_cnt));
      }
    }
  }
  return ret;
}

int64_t ObPushdownFilter::get_serialize_pushdown_filter_size(ObPushdownFilterNode* pd_filter_node)
{
  int64_t len = 0;
  if (OB_NOT_NULL(pd_filter_node)) {
    len += serialization::encoded_length_vi32(pd_filter_node->type_);
    len += serialization::encoded_length(pd_filter_node->n_child_);
    len += pd_filter_node->get_serialize_size();
    for (int64_t i = 0; i < pd_filter_node->n_child_; ++i) {
      len += get_serialize_pushdown_filter_size(pd_filter_node->childs_[i]);
    }
  } else {
    int ret = OB_SUCCESS;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pushdown filter is null", K(ret));
  }
  return len;
}

OB_DEF_SERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree_)) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, false))) {
      LOG_WARN("fail to encode op type", K(ret));
    }
  } else {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, true))) {
      LOG_WARN("fail to encode op type", K(ret));
    } else if (OB_FAIL(serialize_pushdown_filter(buf, buf_len, pos, filter_tree_))) {
      LOG_WARN("failed to serialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPushdownFilter)
{
  int ret = OB_SUCCESS;
  bool has_filter = false;
  filter_tree_ = nullptr;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, has_filter))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (has_filter) {
    ObPushdownFilterFactory filter_factory(&alloc_);
    if (OB_FAIL(deserialize_pushdown_filter(filter_factory, buf, data_len, pos, filter_tree_))) {
      LOG_WARN("failed to deserialize pushdown filter", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushdownFilter)
{
  int64_t len = 0;
  if (OB_ISNULL(filter_tree_)) {
    len += serialization::encoded_length(false);
  } else {
    len += serialization::encoded_length(true);
    len += get_serialize_pushdown_filter_size(filter_tree_);
  }
  return len;
}

//--------------------- start filter executor ----------------------------
int ObAndFilterExecutor::filter(bool& filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    bool tmp_filtered = false;
    if (OB_ISNULL(childs_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(childs_[i]->filter(tmp_filtered))) {
      LOG_WARN("failed to filter child", K(ret));
    } else if (tmp_filtered) {
      filtered = true;
      break;
    }
  }
  return ret;
}

int ObOrFilterExecutor::filter(bool& filtered)
{
  int ret = OB_SUCCESS;
  filtered = true;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    bool tmp_filtered = false;
    if (OB_ISNULL(childs_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(childs_[i]->filter(tmp_filtered))) {
      LOG_WARN("failed to filter child", K(ret));
    } else if (!tmp_filtered) {
      filtered = false;
      break;
    }
  }
  return ret;
}

int ObBlackFilterExecutor::filter(bool& filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  ObDatum* cmp_res = NULL;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval ctx is null", K(ret));
  } else {
    ObPushdownBlackFilterNode& filter = reinterpret_cast<ObPushdownBlackFilterNode&>(filter_);
    for (uint32_t i = 0; i < filter.filter_exprs_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(filter.filter_exprs_.at(i)->eval(*eval_ctx_, cmp_res))) {
        LOG_WARN("failed to filter child", K(ret));
      } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
        filtered = true;
        break;
      }
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::filter(bool& filtered)
{
  int ret = OB_SUCCESS;
  UNUSED(filtered);
  ret = OB_NOT_SUPPORTED;
  return ret;
}
// end for test filter

int ObPushdownFilterExecutor::find_evaluated_datums(
    ObExpr* expr, const ObIArray<ObExpr*>& calc_exprs, ObIArray<ObExpr*>& eval_exprs)
{
  int ret = OB_SUCCESS;
  if (0 < expr->arg_cnt_ && is_contain(calc_exprs, expr)) {
    if (OB_FAIL(eval_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (uint32_t i = 0; i < expr->arg_cnt_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(find_evaluated_datums(expr->args_[i], calc_exprs, eval_exprs))) {
      LOG_WARN("failed to find evaluated datums", K(ret));
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::find_evaluated_datums(
    ObIArray<ObExpr*>& src_exprs, const ObIArray<ObExpr*>& calc_exprs, ObIArray<ObExpr*>& eval_exprs)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < src_exprs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(find_evaluated_datums(src_exprs.at(i), calc_exprs, eval_exprs))) {
      LOG_WARN("failed to find evaluated datums", K(ret));
    }
  }
  return ret;
}

int ObPushdownFilterExecutor::init_bitmap(const int64_t row_count, common::ObBitmap*& bitmap)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;
  if (OB_NOT_NULL(filter_bitmap_)) {
    if (OB_FAIL(filter_bitmap_->expand_size(row_count))) {
      LOG_WARN("Failed to expand size of filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_->reuse(is_logic_and_node()))) {
    }
  } else {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for filter bitmap", K(ret));
    } else if (FALSE_IT(filter_bitmap_ = new (buf) ObBitmap(allocator_))) {
    } else if (OB_FAIL(filter_bitmap_->init(row_count, is_logic_and_node()))) {
      LOG_WARN("Failed to init result bitmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bitmap = filter_bitmap_;
  }
  return ret;
}

int ObPushdownFilterExecutor::init_filter_param(const share::schema::ColumnMap* col_id_map,
    const common::ObIArray<share::schema::ObColumnParam*>* col_params, const bool need_padding)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;
  const ObIArray<uint64_t>& col_ids = filter_.get_col_ids();
  if (OB_ISNULL(col_id_map) || OB_ISNULL(col_params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid filter executor parameter", K(ret), KP(col_id_map), KP(col_params));
  } else if (is_filter_node()) {
    reset_filter_param();
    if (0 == col_ids.count()) {
    } else if (OB_ISNULL((buf = allocator_.alloc(sizeof(int32_t) * col_ids.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for filter executor col offsets", K(ret), K(col_ids.count()));
    } else if (FALSE_IT(col_offsets_ = new (buf) int32_t[col_ids.count()])) {
    } else if (OB_ISNULL((buf = allocator_.alloc(sizeof(share::schema::ObColumnParam*) * col_ids.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for filter executor col param", K(ret), K(col_ids.count()));
    } else if (FALSE_IT(col_params_ = new (buf) const share::schema::ObColumnParam*[col_ids.count()]())) {
    } else {
      MEMSET(col_offsets_, -1, sizeof(int32_t) * col_ids.count());
      MEMSET(col_params_, 0, sizeof(const share::schema::ObColumnParam*) * col_ids.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); i++) {
        if (OB_FAIL(col_id_map->get(col_ids.at(i), col_offsets_[i]))) {
          LOG_WARN("failed to get column offset from id", K(ret), K(i), K(col_ids), K(col_id_map));
        } else if (!need_padding) {
        } else if (OB_UNLIKELY(col_offsets_[i] >= col_params->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected col offset out of range out_cols", K(ret), K(col_offsets_[i]), K(col_params->count()));
        } else if (col_params->at(col_offsets_[i])->get_meta_type().is_fixed_len_char_type()) {
          col_params_[i] = col_params->at(col_offsets_[i]);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset_filter_param();
  } else {
    n_cols_ = col_ids.count();
  }
  return ret;
}

int ObAndFilterExecutor::init_evaluated_datums(
    ObIAllocator& alloc, const ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums(alloc, calc_exprs, eval_ctx))) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

int ObOrFilterExecutor::init_evaluated_datums(
    ObIAllocator& alloc, const ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < n_child_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(childs_[i]->init_evaluated_datums(alloc, calc_exprs, eval_ctx))) {
      LOG_WARN("failed to filter child", K(ret));
    }
  }
  return ret;
}

int ObWhiteFilterExecutor::init_evaluated_datums(
    ObIAllocator& alloc, const ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UNUSED(calc_exprs);
  UNUSED(eval_ctx);
  ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObBlackFilterExecutor::filter(ObObj* objs, int64_t col_cnt, bool& filtered)
{
  int ret = OB_SUCCESS;
  ObPushdownBlackFilterNode& node = reinterpret_cast<ObPushdownBlackFilterNode&>(filter_);
  if (col_cnt != node.column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: column count not match", K(ret), K(col_cnt), K(filter_.col_ids_));
  } else {
    for (int64_t i = 0; i < node.column_exprs_.count() && OB_SUCC(ret); ++i) {
      ObDatum& expr_datum = node.column_exprs_.at(i)->locate_datum_for_write(*eval_ctx_);
      if (OB_FAIL(expr_datum.from_obj(objs[i]))) {
        LOG_WARN("Failed to convert object from datum", K(ret), K(objs[i]));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filter(filtered))) {
        LOG_WARN("failed to calc filter", K(ret));
      }
      clear_evaluated_datums();
    }
  }
  return ret;
}

int ObBlackFilterExecutor::init_evaluated_datums(
    ObIAllocator& alloc, const ObIArray<ObExpr*>& calc_exprs, ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  eval_ctx_ = eval_ctx;
  ObArray<ObExpr*> eval_exprs;
  if (OB_FAIL(find_evaluated_datums(
          reinterpret_cast<ObPushdownBlackFilterNode&>(filter_).filter_exprs_, calc_exprs, eval_exprs))) {
    LOG_WARN("failed to find evaluated datums", K(ret));
  } else {
    eval_infos_ = reinterpret_cast<ObEvalInfo**>(alloc.alloc(eval_exprs.count() * sizeof(ObEvalInfo*)));
    if (OB_ISNULL(eval_infos_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator memory", K(ret));
    } else {
      for (int64_t i = 0; i < eval_exprs.count() && OB_SUCC(ret); ++i) {
        eval_infos_[i] = &eval_exprs.at(i)->get_eval_info(*eval_ctx_);
      }
      n_eval_infos_ = eval_exprs.count();
    }
  }
  return ret;
}
//--------------------- end filter executor ----------------------------

//--------------------- start filter executor constructor ----------------------------
template <typename CLASST, FilterExecutorType type>
int ObFilterExecutorConstructor::create_filter_executor(
    ObPushdownFilterNode* filter_tree, ObPushdownFilterExecutor*& filter_executor)
{
  int ret = OB_SUCCESS;
  ObPushdownFilterExecutor* tmp_filter_executor = nullptr;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter tree is null", K(ret));
  } else if (OB_FAIL(factory_.alloc(type, filter_tree->n_child_, *filter_tree, tmp_filter_executor))) {
    LOG_WARN("failed to alloc pushdown filter", K(ret), K(filter_tree->n_child_));
  } else {
    filter_executor = static_cast<CLASST*>(tmp_filter_executor);
    for (int64_t i = 0; i < filter_tree->n_child_ && OB_SUCC(ret); ++i) {
      ObPushdownFilterExecutor* sub_filter_executor = nullptr;
      if (OB_FAIL(apply(filter_tree->childs_[i], sub_filter_executor))) {
        LOG_WARN("failed to apply filter node", K(ret));
      } else if (OB_ISNULL(sub_filter_executor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub filter executor is null", K(ret));
      } else if (OB_ISNULL(filter_executor->get_childs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("childs is null", K(ret));
      } else {
        filter_executor->set_child(i, sub_filter_executor);
      }
    }
  }
  return ret;
}

int ObFilterExecutorConstructor::apply(ObPushdownFilterNode* filter_tree, ObPushdownFilterExecutor*& filter_executor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter node is null", K(ret));
  } else {
    switch (filter_tree->get_type()) {
      case BLACK_FILTER: {
        ret = create_filter_executor<ObBlackFilterExecutor, BLACK_FILTER_EXECUTOR>(filter_tree, filter_executor);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case WHITE_FILTER: {
        ret = create_filter_executor<ObWhiteFilterExecutor, WHITE_FILTER_EXECUTOR>(filter_tree, filter_executor);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case AND_FILTER: {
        ret = create_filter_executor<ObAndFilterExecutor, AND_FILTER_EXECUTOR>(filter_tree, filter_executor);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      case OR_FILTER: {
        ret = create_filter_executor<ObOrFilterExecutor, OR_FILTER_EXECUTOR>(filter_tree, filter_executor);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to create filter executor", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected filter type", K(ret));
        break;
    }
  }
  return ret;
}
//--------------------- start filter executor constructor ----------------------------

}  // namespace sql
}  // namespace oceanbase
