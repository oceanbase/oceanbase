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

#define USING_LOG_PREFIX SQL_REWRITE

#include "sql/rewrite/ob_query_range_define.h"
#include "sql/rewrite/ob_expr_range_converter.h"
#include "sql/rewrite/ob_range_graph_generator.h"
#include "sql/rewrite/ob_range_generator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
ERRSIM_POINT_DEF(ERRSIM_FAST_NLJ_RANGE_GENERATOR_CHECK);

void ObRangeNode::reset()
{
  flags_ = 0;
  column_cnt_ = 0;
  start_keys_ = nullptr;
  end_keys_ = nullptr;
  min_offset_ = -1;
  max_offset_ = -1;
  in_param_count_ = 0;
  node_id_ = -1;
  or_next_ = nullptr;
  and_next_ = nullptr;
}

int ObRangeNode::deep_copy(const ObRangeNode &other)
{
  int ret = OB_SUCCESS;
  flags_ = other.flags_;
  column_cnt_= other.column_cnt_;
  min_offset_= other.min_offset_;
  max_offset_= other.max_offset_;
  in_param_count_= other.in_param_count_;
  node_id_= other.node_id_;
  start_keys_ = static_cast<int64_t*>(allocator_.alloc(sizeof(int64_t) * column_cnt_ * 2));
  if (OB_ISNULL(start_keys_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for range node keys");
  } else {
    end_keys_ = start_keys_ + column_cnt_;
    MEMCPY(start_keys_, other.start_keys_, sizeof(int64_t) * column_cnt_);
    MEMCPY(end_keys_, other.end_keys_, sizeof(int64_t) * column_cnt_);
  }
  // do not copy or/and next
  or_next_ = nullptr;
  and_next_ = nullptr;
  return ret;
}

void ObRangeNode::set_always_true()
{
  flags_ = 0;
  always_true_ = true;
  min_offset_ = -1;
  max_offset_ = -1;
  in_param_count_ = 0;
  node_id_ = -1;
  or_next_ = nullptr;
  and_next_ = nullptr;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    start_keys_[i] = OB_RANGE_MIN_VALUE;
    end_keys_[i] = OB_RANGE_MAX_VALUE;
  }
}

void ObRangeNode::set_always_false()
{
  flags_ = 0;
  always_false_ = true;
  min_offset_ = -1;
  max_offset_ = -1;
  in_param_count_ = 0;
  node_id_ = -1;
  or_next_ = nullptr;
  and_next_ = nullptr;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    start_keys_[i] = OB_RANGE_MAX_VALUE;
    end_keys_[i] = OB_RANGE_MIN_VALUE;
  }
}

OB_DEF_SERIALIZE(ObRangeNode)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(column_cnt_);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    OB_UNIS_ENCODE(start_keys_[i]);
    OB_UNIS_ENCODE(end_keys_[i]);
  }
  OB_UNIS_ENCODE(min_offset_);
  OB_UNIS_ENCODE(max_offset_);
  OB_UNIS_ENCODE(in_param_count_);
  OB_UNIS_ENCODE(node_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRangeNode)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(column_cnt_);
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(int64_t) * column_cnt_ * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    start_keys_ = static_cast<int64_t*>(ptr);
    end_keys_ = start_keys_ + column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      OB_UNIS_DECODE(start_keys_[i]);
      OB_UNIS_DECODE(end_keys_[i]);
    }
    OB_UNIS_DECODE(min_offset_);
    OB_UNIS_DECODE(max_offset_);
    OB_UNIS_DECODE(in_param_count_);
    OB_UNIS_DECODE(node_id_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRangeNode)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(column_cnt_);
  // start keys and end keys
  for (int64_t i = 0; i < column_cnt_; ++i) {
    OB_UNIS_ADD_LEN(start_keys_[i]);
    OB_UNIS_ADD_LEN(end_keys_[i]);
  }
  OB_UNIS_ADD_LEN(min_offset_);
  OB_UNIS_ADD_LEN(max_offset_);
  OB_UNIS_ADD_LEN(in_param_count_);
  OB_UNIS_ADD_LEN(node_id_);
  return len;
}

DEF_TO_STRING(ObRangeNode)
{
  int64_t pos = 0;
  J_OBJ_START();
  // print parameters
  J_KV(K_(always_true),
       K_(always_false),
       K_(min_offset),
       K_(max_offset),
       K_(column_cnt),
       K_(include_start),
       K_(include_end),
       K_(contain_in),
       K_(is_domain_node),
       K_(node_id));
  // print range
  J_COMMA();
  BUF_PRINTF("range:");
  J_ARRAY_START();
  for (int64_t i = 0; i < column_cnt_; ++i) {
    BUF_PRINTO(start_keys_[i]);
    if (i == column_cnt_ - 1) {
      BUF_PRINTF("; ");
    } else {
      J_COMMA();
    }
  }
  for (int64_t i = 0; i < column_cnt_; ++i) {
    BUF_PRINTO(end_keys_[i]);
    if (i < column_cnt_ - 1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  // print node connection relation
  J_COMMA();
  J_KV(KP(this),
       KP_(or_next),
       KP_(and_next));
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObRangeMap)
{
  int ret = OB_SUCCESS;
  int64_t cnt = expr_final_infos_.count();
  OB_UNIS_ENCODE(cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    OB_UNIS_ENCODE(expr_final_infos_.at(i).flags_);
    if (expr_final_infos_.at(i).is_param_) {
      OB_UNIS_ENCODE(expr_final_infos_.at(i).param_idx_);
    } else if (expr_final_infos_.at(i).is_const_) {
      OB_UNIS_ENCODE(*expr_final_infos_.at(i).const_val_);
    } else if (expr_final_infos_.at(i).is_expr_) {
      OB_UNIS_ENCODE(*expr_final_infos_.at(i).temp_expr_);
    }
  }
  cnt = in_params_.count();
  OB_UNIS_ENCODE(cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    InParam* param = in_params_.at(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else {
      OB_UNIS_ENCODE(param->count());
      for (int64_t j = 0; OB_SUCC(ret) && j < param->count(); ++j) {
        OB_UNIS_ENCODE(param->at(j));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRangeMap)
{
  int ret = OB_SUCCESS;
  expr_final_infos_.reset();
  int64_t cnt = 0;
  OB_UNIS_DECODE(cnt);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_final_infos_.prepare_allocate(cnt))) {
      LOG_WARN("failed to prepare allocate expr final info", K(cnt));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    OB_UNIS_DECODE(expr_final_infos_.at(i).flags_);
    if (expr_final_infos_.at(i).is_param_) {
      OB_UNIS_DECODE(expr_final_infos_.at(i).param_idx_);
    } else if (expr_final_infos_.at(i).is_const_) {
      ObObj *obj_val = nullptr;
      void *ptr = allocator_.alloc(sizeof(ObObj));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memeory for ObObj");
      } else {
        obj_val = new(ptr)ObObj();
        OB_UNIS_DECODE(*obj_val);
        if (OB_SUCC(ret)) {
          expr_final_infos_.at(i).const_val_ = obj_val;
        }
      }
    } else if (expr_final_infos_.at(i).is_expr_) {
      ObTempExpr *temp_expr = nullptr;
      void *ptr = allocator_.alloc(sizeof(ObTempExpr));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memeory for temp expr");
      } else {
        temp_expr = new(ptr)ObTempExpr(allocator_);
        OB_UNIS_DECODE(*temp_expr);
        if (OB_SUCC(ret)) {
          expr_final_infos_.at(i).temp_expr_ = temp_expr;
        }
      }
    }
  }
  OB_UNIS_DECODE(cnt);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(in_params_.prepare_allocate(cnt))) {
      LOG_WARN("failed to init array", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    InParam* param = nullptr;
    int64_t param_cnt = 0;
    void *ptr = allocator_.alloc(sizeof(InParam));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memeory for in param");
    } else {
      param = new(ptr)InParam(allocator_);
      in_params_.at(i) = param;
      OB_UNIS_DECODE(param_cnt);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(param->prepare_allocate(param_cnt))) {
          LOG_WARN("failed to init array");
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < param_cnt; ++j) {
        OB_UNIS_DECODE(param->at(j));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRangeMap)
{
  int64_t len = 0;
  int64_t cnt = expr_final_infos_.count();
  OB_UNIS_ADD_LEN(cnt);
  for (int64_t i = 0; i < cnt; ++i) {
    OB_UNIS_ADD_LEN(expr_final_infos_.at(i).flags_);
    if (expr_final_infos_.at(i).is_param_) {
      OB_UNIS_ADD_LEN(expr_final_infos_.at(i).param_idx_);
    } else if (expr_final_infos_.at(i).is_const_) {
      OB_UNIS_ADD_LEN(*expr_final_infos_.at(i).const_val_);
    } else if (expr_final_infos_.at(i).is_expr_) {
      OB_UNIS_ADD_LEN(*expr_final_infos_.at(i).temp_expr_);
    }
  }
  cnt = in_params_.count();
  OB_UNIS_ADD_LEN(cnt);
  for (int64_t i = 0; i < cnt; ++i) {
    InParam* param = in_params_.at(i);
    if (OB_NOT_NULL(param)) {
      OB_UNIS_ADD_LEN(param->count());
      for (int64_t j = 0; j < param->count(); ++j) {
        OB_UNIS_ADD_LEN(param->at(j));
      }
    }
  }
  return len;
}

//ObRangeColumnMeta
OB_SERIALIZE_MEMBER(ObRangeColumnMeta, column_type_, column_id_);

//ObFastFinalPos
OB_SERIALIZE_MEMBER(ObFastFinalPos, index_, offset_, flags_);

int ObQueryRangeCtx::init(ObPreRangeGraph *pre_range_graph,
                          const ObIArray<ColumnItem> &range_columns,
                          ExprConstrantArray *expr_constraints,
                          const ParamStore *params,
                          ObRawExprFactory *expr_factory,
                          const bool phy_rowid_for_table_loc,
                          const bool ignore_calc_failure,
                          const int64_t index_prefix,
                          const ColumnIdInfoMap *geo_column_id_map,
                          const bool force_no_link,
                          const ObTableSchema *index_schema,
                          ObRawExprFactory *constraints_expr_factory)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(pre_range_graph) || OB_ISNULL(exec_ctx_) || OB_ISNULL(exec_ctx_->get_my_session()) ||
      OB_ISNULL(query_ctx = exec_ctx_->get_query_ctx()) || OB_UNLIKELY(range_columns.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(pre_range_graph), K(exec_ctx_), K(query_ctx));
  } else if (OB_FAIL(column_metas_.assign(pre_range_graph->get_column_metas()))) {
    LOG_WARN("failed to assign column meta");
  } else if (OB_FAIL(column_flags_.prepare_allocate(pre_range_graph->get_column_metas().count()))) {
    LOG_WARN("failed to prepare allocate");
  } else if (OB_FAIL(exec_ctx_->get_my_session()->
             is_enable_range_extraction_for_not_in(enable_not_in_range_))) {
    LOG_WARN("failed to check not in range enabled", K(ret));
  } else if (OB_FAIL(query_ctx->get_global_hint().opt_params_.get_bool_opt_param(ObOptParamHint::ENABLE_RANGE_EXTRACTION_FOR_NOT_IN, enable_not_in_range_))) {
    LOG_WARN("fail to check opt param not in range enabled", K(ret));
  } else if (OB_FAIL(exec_ctx_->get_my_session()->
             get_optimizer_features_enable_version(optimizer_features_enable_version_))) {
    LOG_WARN("failed to get optimizer features enable version", K(ret));
  } else {
    column_cnt_ = range_columns.count();
    expr_constraints_ = expr_constraints;
    params_ = params;
    phy_rowid_for_table_loc_ = phy_rowid_for_table_loc;
    ignore_calc_failure_ = ignore_calc_failure;
    expr_factory_ = expr_factory;
    session_info_ = exec_ctx_->get_my_session();
    max_mem_size_ = exec_ctx_->get_my_session()->get_range_optimizer_max_mem_size();
    index_prefix_ = index_prefix;
    geo_column_id_map_ = geo_column_id_map;
    is_geo_range_ = geo_column_id_map != NULL;
    force_no_link_ = force_no_link;
    constraints_expr_factory_ = constraints_expr_factory;
    if (OB_NOT_NULL(index_schema) &&
        index_schema->is_unique_index() &&
        index_schema->get_index_column_num() > 0) {
      unique_index_column_num_ = index_schema->get_index_column_num();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count(); ++i) {
    const ColumnItem &col = range_columns.at(i);
    if (OB_UNLIKELY(col.is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid column item", K(col));
    } else if (OB_FAIL(range_column_map_.set_refactored(col.column_id_, i))) {
      LOG_WARN("failed to set column map", K(col), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_flags_.count(); ++i) {
    column_flags_.at(i) = 0;
  }
  return ret;
}

void ObPreRangeGraph::reset()
{
  table_id_ = OB_INVALID_ID;
  column_count_ = 0;
  range_size_ = 0;
  node_count_ = 0;
  node_head_ = nullptr;
  is_standard_range_ = false;
  is_precise_get_ = false;
  is_equal_range_ = false;
  is_get_ = false;
  contain_exec_param_ = false;
  column_metas_.reset();
  range_map_.expr_final_infos_.reset();
  range_map_.in_params_.reset();
  skip_scan_offset_ = -1;
  range_exprs_.reset();
  ss_range_exprs_.reset();
  unprecise_range_exprs_.reset();
  total_range_sizes_.reset();
  flags_ = 0;
}

int ObPreRangeGraph::deep_copy(const ObPreRangeGraph &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  column_count_ = other.column_count_;
  range_size_ = other.range_size_;
  node_count_ = other.node_count_;
  is_standard_range_ = other.is_standard_range_;
  is_precise_get_ = other.is_precise_get_;
  is_equal_range_ = other.is_equal_range_;
  is_get_ = other.is_get_;
  contain_exec_param_ = other.contain_exec_param_;
  skip_scan_offset_ = other.skip_scan_offset_;
  flags_ = other.flags_;
  if (OB_FAIL(range_exprs_.assign(other.range_exprs_))) {
    LOG_WARN("failed to assign range exprs");
  } else if (OB_FAIL(ss_range_exprs_.assign(other.ss_range_exprs_))) {
    LOG_WARN("failed to assign ss range exprs");
  } else if (OB_FAIL(unprecise_range_exprs_.assign(other.unprecise_range_exprs_))) {
    LOG_WARN("failed to assign unprecise range exprs");
  } else if (OB_FAIL(total_range_sizes_.assign(other.total_range_sizes_))) {
    LOG_WARN("failed to assign total range sizes");
  } else if (OB_FAIL(fast_final_pos_arr_.assign(other.fast_final_pos_arr_))) {
    LOG_WARN("failed to assign fast final pos arrary");
  } else if (OB_FAIL(deep_copy_range_graph(other.node_head_))) {
    LOG_WARN("failed to deep copy range graph");
  } else if (OB_FAIL(deep_copy_column_metas(other.column_metas_))) {
    LOG_WARN("failed to deep copy column metas");
  } else if (OB_FAIL(deep_copy_range_map(other.range_map_))) {
    LOG_WARN("failed to deep copy range map");
  }
  return ret;
}

int ObPreRangeGraph::deep_copy_range_graph(ObRangeNode *src_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 16> range_nodes;
  ObSEArray<std::pair<int64_t, int64_t>, 16> ptr_pairs;
  if (src_node == nullptr) {
    // do nothing
  } else if (OB_FAIL(inner_deep_copy_range_graph(src_node, range_nodes, ptr_pairs))) {
    LOG_WARN("failed to inner deep copy range graph");
  } else if (OB_UNLIKELY(range_nodes.count() != node_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range node count", K(node_count_), K(range_nodes.count()));
  } else {
    for (int64_t i = 0; i < node_count_; ++i) {
      std::pair<int64_t, int64_t> &ptr_pair = ptr_pairs.at(i);
      range_nodes.at(i)->and_next_ = (-1 == ptr_pair.first) ? nullptr : range_nodes.at(ptr_pair.first);
      range_nodes.at(i)->or_next_ = (-1 == ptr_pair.second) ? nullptr : range_nodes.at(ptr_pair.second);
    }
    node_head_ = node_count_ > 0 ? range_nodes.at(0) : nullptr;
  }
  return ret;
}

int ObPreRangeGraph::inner_deep_copy_range_graph(ObRangeNode *range_node,
                                                 ObIArray<ObRangeNode*> &range_nodes,
                                                 ObIArray<std::pair<int64_t, int64_t>> &ptr_pairs)
{
  int ret = OB_SUCCESS;
  for (ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    int64_t and_next_id = (nullptr == cur_node->and_next_) ? -1 : cur_node->and_next_->node_id_;
    int64_t or_next_id = (nullptr == cur_node->or_next_) ? -1 : cur_node->or_next_->node_id_;
    ObRangeNode *new_node = static_cast<ObRangeNode *>(allocator_.alloc(sizeof(ObRangeNode)));
    if (OB_ISNULL(new_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for range node");
    } else {
      new_node = new(new_node)ObRangeNode(allocator_);
      if (OB_FAIL(new_node->deep_copy(*cur_node))) {
        LOG_WARN("failed to deep copy range node");
      } else if (OB_FAIL(range_nodes.push_back(new_node))) {
        LOG_WARN("failed to push back range node");
      } else if (OB_FAIL(ptr_pairs.push_back(std::pair<int64_t, int64_t>(and_next_id, or_next_id)))) {
        LOG_WARN("failed to push back and/or next id");
      }
    }
    /**
     * Node id is generated by dfs algorithm. So if node id of and_next smaller than node id
     * of current node, and next node must have been copied.
     *
     *     node(0) -- node(1) -- node(2)
     *       |                 /
     *     node(3) -- node(4) /
    */
    if OB_SUCC(ret) {
      if (cur_node->and_next_ != nullptr && cur_node->and_next_->node_id_ > cur_node->node_id_) {
        if (OB_FAIL(SMART_CALL(inner_deep_copy_range_graph(cur_node->and_next_, range_nodes, ptr_pairs)))) {
          LOG_WARN("failed to inner deep copy range graph");
        }
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::deep_copy_column_metas(const ObIArray<ObRangeColumnMeta*> &src_metas)
{
  int ret = OB_SUCCESS;
  column_metas_.reset();
  int64_t count = src_metas.count();
  if (OB_FAIL(column_metas_.prepare_allocate(count))) {
    LOG_WARN("failed to prepare allocate column meta", K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObRangeColumnMeta *src_meta = src_metas.at(i);
    ObRangeColumnMeta *column_meta = nullptr;
    void *ptr = allocator_.alloc(sizeof(ObRangeColumnMeta));
    if (OB_ISNULL(src_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta");
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memeory for ObRangeColumnMeta");
    } else {
      column_meta = new(ptr)ObRangeColumnMeta();
      column_meta->column_type_ = src_meta->column_type_;
      column_meta->column_id_ = src_meta->column_id_;
      column_metas_.at(i) = column_meta;
    }
  }
  return ret;
}

int ObPreRangeGraph::deep_copy_range_map(const ObRangeMap &src_range_map)
{
  int ret = OB_SUCCESS;
  range_map_.expr_final_infos_.reset();
  int64_t count = src_range_map.expr_final_infos_.count();
  if (OB_FAIL(range_map_.expr_final_infos_.prepare_allocate(count))) {
    LOG_WARN("failed to prepare allocate expr final info", K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    range_map_.expr_final_infos_.at(i).flags_ = src_range_map.expr_final_infos_.at(i).flags_;
    if (range_map_.expr_final_infos_.at(i).is_param_) {
      range_map_.expr_final_infos_.at(i).param_idx_ = src_range_map.expr_final_infos_.at(i).param_idx_;
    } else if (range_map_.expr_final_infos_.at(i).is_const_) {
      ObObj *obj_val = nullptr;
      void *ptr = allocator_.alloc(sizeof(ObObj));
      if (OB_ISNULL(src_range_map.expr_final_infos_.at(i).const_val_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null const val");
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memeory for ObObj");
      } else {
        obj_val = new(ptr)ObObj();
        if (OB_FAIL(ob_write_obj(allocator_, *src_range_map.expr_final_infos_.at(i).const_val_, *obj_val))) {
          LOG_WARN("failed to write obj", KPC(src_range_map.expr_final_infos_.at(i).const_val_));
        } else {
          range_map_.expr_final_infos_.at(i).const_val_ = obj_val;
        }
      }
    } else if (range_map_.expr_final_infos_.at(i).is_expr_) {
      ObTempExpr *temp_expr = nullptr;
      if (OB_ISNULL(src_range_map.expr_final_infos_.at(i).temp_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null temp expr");
      } else if (OB_FAIL(src_range_map.expr_final_infos_.at(i).temp_expr_->deep_copy(allocator_, temp_expr))) {
        LOG_WARN("faield to deep copy temp expr");
      } else {
        range_map_.expr_final_infos_.at(i).temp_expr_ = temp_expr;
      }
    }
  }

  if (OB_SUCC(ret)) {
    count = src_range_map.in_params_.count();
    if (OB_FAIL(range_map_.in_params_.prepare_allocate(count))) {
      LOG_WARN("failed to init array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      InParam* param = nullptr;
      void *ptr = allocator_.alloc(sizeof(InParam));
      if (OB_ISNULL(src_range_map.in_params_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null in param");
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memeory for in param");
      } else {
        param = new(ptr)InParam(allocator_);
        if (OB_FAIL(param->assign(*src_range_map.in_params_.at(i)))) {
          LOG_WARN("failed to assign in param");
        } else {
          range_map_.in_params_.at(i) = param;
        }
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::preliminary_extract_query_range(const ObIArray<ColumnItem> &range_columns,
                                                     const ObIArray<ObRawExpr*> &root_exprs,
                                                     ObExecContext *exec_ctx,
                                                     ExprConstrantArray *expr_constraints,
                                                     const ParamStore *params,
                                                     const bool phy_rowid_for_table_loc,
                                                     const bool ignore_calc_failure,
                                                     const int64_t index_prefix /* =-1*/,
                                                     const ObTableSchema *index_schema /* = NULL*/,
                                                     const ColumnIdInfoMap *geo_column_id_map /* = NULL*/,
                                                     ObRawExprFactory *constraint_expr_factory /* = NULL*/)
{
  int ret = OB_SUCCESS;
  bool force_no_link = false;
  ObQueryRangeCtx ctx(exec_ctx);
  ObRawExprFactory expr_factory(allocator_);
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpeced null", K(ret), K(exec_ctx));
  } else if (OB_FAIL(fill_column_metas(range_columns))) {
    LOG_WARN("failed to fill column metas");
  } else if (OB_FAIL(ctx.init(this, range_columns, expr_constraints,
                              params, &expr_factory,
                              phy_rowid_for_table_loc, ignore_calc_failure, index_prefix,
                              geo_column_id_map, force_no_link, index_schema,
                              constraint_expr_factory))) {
    LOG_WARN("failed to init query range context");
  } else {
    ObExprRangeConverter converter(allocator_, ctx);
    ObRangeGraphGenerator graph_generator(allocator_, ctx, this, range_columns.count());
    if (OB_FAIL(graph_generator.generate_range_graph(root_exprs, converter))) {
      LOG_WARN("failed to generate range graph");
    } else {
      LOG_TRACE("succeed to preliminary extract query range",
                  KPC(this), K(range_columns), K(root_exprs));
    }
  }
  return ret;
}

int ObPreRangeGraph::get_tablet_ranges(common::ObIAllocator &allocator,
                                       ObExecContext &exec_ctx,
                                       ObQueryRangeArray &ranges,
                                       bool &all_single_value_ranges,
                                       const common::ObDataTypeCastParams &dtc_params)  const
{
  int ret = OB_SUCCESS;
  ObMbrFilterArray mbr_filter;
  ObRangeGenerator range_generator(allocator, exec_ctx, this, ranges, all_single_value_ranges, dtc_params, mbr_filter);
  if (OB_FAIL(range_generator.generate_ranges())) {
    LOG_WARN("failed to generate ranges");
  }
  return ret;
}

int ObPreRangeGraph::get_tablet_ranges(ObQueryRangeArray &ranges,
                                       bool &all_single_value_ranges,
                                       const common::ObDataTypeCastParams &dtc_params)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("new qeury range not support this interface");
  return ret;
}

int ObPreRangeGraph::get_tablet_ranges(common::ObIAllocator &allocator,
                                ObExecContext &exec_ctx,
                                ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params,
                                ObIArray<common::ObSpatialMBR> &mbr_filters) const
{
  int ret = OB_SUCCESS;
  ObRangeGenerator range_generator(allocator, exec_ctx, this, ranges, all_single_value_ranges, dtc_params, mbr_filters);
  if (OB_FAIL(range_generator.generate_ranges())) {
    LOG_WARN("failed to generate ranges");
  }
  return ret;
}

int ObPreRangeGraph::get_ss_tablet_ranges(common::ObIAllocator &allocator,
                                          ObExecContext &exec_ctx,
                                          ObQueryRangeArray &ss_ranges,
                                          const common::ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  ObMbrFilterArray mbr_filter;
  ss_ranges.reuse();
  if (!is_ss_range()) {
    // do nothing
  } else {
    bool dummy_all_single_value = false;
    ObRangeGenerator range_generator(allocator, exec_ctx, this, ss_ranges, dummy_all_single_value, dtc_params, mbr_filter);
    if (OB_FAIL(range_generator.generate_ss_ranges())) {
      LOG_WARN("failed to generate ranges");
    } else {
      LOG_DEBUG("get skip range success", K(ss_ranges));
    }
  }
  return ret;
}

int ObPreRangeGraph::get_fast_nlj_tablet_ranges(ObFastFinalNLJRangeCtx &fast_nlj_range_ctx,
                                                common::ObIAllocator &allocator,
                                                ObExecContext &exec_ctx,
                                                const ParamStore &param_store,
                                                int64_t range_buffer_idx,
                                                void *range_buffer,
                                                ObQueryRangeArray &ranges,
                                                const common::ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  if (fast_nlj_range_) {
    if (OB_UNLIKELY(!fast_nlj_range_ctx.has_check_valid_)) {
      if (OB_FAIL(ObRangeGenerator::check_can_final_fast_nlj_range(*this,
                                                                  param_store,
                                                                  fast_nlj_range_ctx.is_valid_))) {
        LOG_WARN("failed to check can final fast nlj range", K(ret));
      }
      fast_nlj_range_ctx.has_check_valid_ = true;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(fast_nlj_range_ctx.is_valid_)) {
      if (OB_FAIL(ObRangeGenerator::generate_fast_nlj_range(*this,
                                                            param_store,
                                                            allocator,
                                                            range_buffer,
                                                            ranges))) {
        LOG_WARN("failed to generate fast nlj range", K(ret));
      }
    } else {
      bool dummy_all_single_value_ranges = false;
      if (OB_FAIL(get_tablet_ranges(allocator,
                                    exec_ctx,
                                    ranges,
                                    dummy_all_single_value_ranges,
                                    dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      }
    }
  } else if (general_nlj_range_) {
    ObArray<ObObj> objs;
    bool can_fast_extract = true;
    bool dummy_all_single_value_ranges = false;
    bool always_false = false;
    bool always_true = false;
    bool need_check_range = false;
    ObQueryRangeArray check_ranges;
    if (OB_UNLIKELY(!fast_nlj_range_ctx.has_check_valid_)) {
      if (OB_FAIL(get_tablet_ranges(allocator,
                                    exec_ctx,
                                    ranges,
                                    dummy_all_single_value_ranges,
                                    dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      } else if (ranges.empty() && enable_new_false_range()) {
        // always false
      } else if (ranges.count() == 1 &&
                 OB_FAIL(ObRangeGenerator::check_range_type(ranges.at(0), always_true, always_false))) {
        LOG_WARN("failed to check false range", K(ret), K(ranges));
      } else if (always_true) {
        fast_nlj_range_ctx.has_check_valid_ = true;
        fast_nlj_range_ctx.is_valid_ = false;
      } else if (always_false) {
        // do nothing
      } else if (OB_FAIL(ObRangeGenerator::check_can_fast_extract_nlj_range(allocator,
                                                                            exec_ctx,
                                                                            dtc_params,
                                                                            *this,
                                                                            objs,
                                                                            always_false,
                                                                            can_fast_extract))) {
        LOG_WARN("failed to check can fast extract nlj range", K(ret));
      } else if (!can_fast_extract) {
        fast_nlj_range_ctx.has_check_valid_ = true;
        fast_nlj_range_ctx.is_valid_ = false;
      } else if (OB_FAIL(fast_nlj_range_ctx.init_first_ranges(column_count_,
                                                              range_buffer_idx,
                                                              ranges))) {
        LOG_WARN("failed to init first ranges", K(ret));
      } else {
        fast_nlj_range_ctx.has_check_valid_ = true;
        fast_nlj_range_ctx.is_valid_ = true;
      }
    } else if (!fast_nlj_range_ctx.is_valid_) {
      if (OB_FAIL(get_tablet_ranges(allocator,
                                    exec_ctx,
                                    ranges,
                                    dummy_all_single_value_ranges,
                                    dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      }
    } else if (OB_FAIL(ObRangeGenerator::check_can_fast_extract_nlj_range(allocator,
                                                                          exec_ctx,
                                                                          dtc_params,
                                                                          *this,
                                                                          objs,
                                                                          always_false,
                                                                          can_fast_extract))) {
      LOG_WARN("failed to check can fast extract nlj range", K(ret));
    } else if (!can_fast_extract) {
      if (OB_FAIL(get_tablet_ranges(allocator,
                                    exec_ctx,
                                    ranges,
                                    dummy_all_single_value_ranges,
                                    dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      } else {
        fast_nlj_range_ctx.is_valid_ = false;
      }
    } else if (always_false) {
      need_check_range = true;
    } else if (OB_FAIL(ObRangeGenerator::fill_general_nlj_range(fast_nlj_range_ctx,
                                                                *this,
                                                                objs,
                                                                range_buffer_idx,
                                                                always_false,
                                                                ranges))) {
      LOG_WARN("failed to fill general nlj range", K(ret));
    } else {
      need_check_range = true;
    }

    if (OB_SUCC(ret) && always_false) {
      ranges.reuse();
      if (!enable_new_false_range()) {
        ObNewRange *range = static_cast<ObNewRange*>(range_buffer);
        ObObj *starts = reinterpret_cast<ObObj*>(static_cast<char*>(range_buffer) + sizeof(ObNewRange));
        ObObj *ends = starts + column_count_;
        for (int i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
          starts[i].set_max_value();
          ends[i].set_min_value();
        }
        range->start_key_.assign(starts, column_count_);
        range->end_key_.assign(ends, column_count_);
        range->table_id_ = get_table_id();
        if (OB_FAIL(ranges.push_back(range))) {
          LOG_WARN("failed to push back ranges", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && ERRSIM_FAST_NLJ_RANGE_GENERATOR_CHECK && need_check_range) {
      if (OB_FAIL(get_tablet_ranges(allocator,
                                    exec_ctx,
                                    check_ranges,
                                    dummy_all_single_value_ranges,
                                    dtc_params))) {
        LOG_WARN("failed to get tablet ranges", K(ret));
      } else if (check_ranges.count() != ranges.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check and find error ranges", K(objs), K(ranges), K(check_ranges));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
          if (!ranges.at(i)->equal(*check_ranges.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("check and find error ranges", K(objs), K(ranges), K(check_ranges));
          }
        }
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::fill_column_metas(const ObIArray<ColumnItem> &range_columns)
{
  int ret = OB_SUCCESS;
  column_count_ = range_columns.count();
  if (OB_FAIL(column_metas_.init(column_count_))) {
    LOG_WARN("failed to init fixed array", K(column_count_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
    const ObColumnRefRawExpr *column_expr = range_columns.at(i).expr_;
    void *ptr = NULL;
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr");
    } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRangeColumnMeta)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memeory for ObRangeColumnMeta");
    } else {
      ObRangeColumnMeta *column_meta =
          new (ptr) ObRangeColumnMeta(column_expr->get_result_type(), column_expr->get_column_id());
      if (column_meta->column_type_.is_lob_locator()) {
        column_meta->column_type_.set_type(ObLongTextType);
      }
      if (OB_FAIL(column_metas_.push_back(column_meta))) {
        LOG_WARN("failed to push back column meta");
      }
    }
    if (OB_SUCC(ret)) {
      if (i == 0) {
        table_id_ = column_expr->get_table_id();
      } else if (OB_UNLIKELY(column_expr->get_table_id() != table_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range columns should belong to same table", K(range_columns));
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::get_prefix_info(int64_t &equal_prefix_count,
                                     int64_t &range_prefix_count,
                                     bool &contain_always_false) const
{
  int ret = OB_SUCCESS;
  equal_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
  range_prefix_count = 0;
  if (OB_ISNULL(node_head_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range node");
  } else if (node_head_->always_true_ || node_head_->always_false_) {
    equal_prefix_count = 0;
    range_prefix_count = 0;
    contain_always_false = node_head_->always_false_;
  } else {
    bool equals[column_count_];
    bool extract_ranges[column_count_];
    MEMSET(equals, 0, sizeof(bool) * column_count_);
    MEMSET(extract_ranges, 0, sizeof(bool) * column_count_);
    if (OB_FAIL(get_prefix_info(node_head_, equals, extract_ranges, equal_prefix_count, range_prefix_count))) {
      LOG_WARN("failed to get prefix info");
    }
  }
  return ret;
}

int ObPreRangeGraph::get_prefix_info(const ObRangeNode *range_node,
                                     bool* equals,
                                     bool* extract_ranges,
                                     int64_t &equal_prefix_count,
                                     int64_t &range_prefix_count) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> new_equal_idx;
  ObSEArray<int64_t, 8> new_range_idx;
  for (const ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != NULL; cur_node = cur_node->or_next_) {
    if (cur_node->min_offset_ >= 0 &&
        OB_FAIL(get_new_equal_idx(cur_node, equals, new_equal_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (cur_node->min_offset_ >= 0 &&
        OB_FAIL(get_new_range_idx(cur_node, extract_ranges, new_range_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (cur_node->and_next_ != nullptr) {
      if (OB_FAIL(SMART_CALL(get_prefix_info(cur_node->and_next_, equals, extract_ranges,
                                             equal_prefix_count, range_prefix_count)))) {
        LOG_WARN("failed to check is strict equal graph");
      }
    } else {
      int64_t cur_pos = 0;
      int64_t tmp_range_prefix_count = 0;
      for (; cur_pos < column_count_; ++cur_pos, ++tmp_range_prefix_count) {
        if (!equals[cur_pos]) {
          if (extract_ranges[cur_pos]) {
            ++tmp_range_prefix_count;
          }
          break;
        }
      }
      equal_prefix_count = std::min(equal_prefix_count, cur_pos);
      range_prefix_count = std::max(range_prefix_count, tmp_range_prefix_count);
    }
    // reset flag in current range node
    for (int64_t j = 0; j < new_equal_idx.count(); ++j) {
      equals[new_equal_idx.at(j)] = false;
    }
    new_equal_idx.reuse();
    for (int64_t j = 0; j < new_range_idx.count(); ++j) {
      extract_ranges[new_range_idx.at(j)] = false;
    }
    new_range_idx.reuse();
  }
  return ret;
}

int ObPreRangeGraph::get_new_equal_idx(const ObRangeNode *range_node,
                                       bool* equals,
                                       ObIArray<int64_t> &new_idx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = range_node->min_offset_; OB_SUCC(ret) && i <= range_node->max_offset_; ++i) {
    if (!equals[i] && range_node->start_keys_[i] == range_node->end_keys_[i] &&
        (range_node->start_keys_[i] < OB_RANGE_EXTEND_VALUE || range_node->start_keys_[i] == OB_RANGE_NULL_VALUE)) {
      equals[i] = true;
      if (OB_FAIL(new_idx.push_back(i))) {
        LOG_WARN("failed to push back idx", K(i));
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::get_new_range_idx(const ObRangeNode *range_node,
                                       bool* extract_ranges,
                                       ObIArray<int64_t> &new_idx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = range_node->min_offset_; OB_SUCC(ret) && i <= range_node->max_offset_; ++i) {
    if (!extract_ranges[i] && (range_node->start_keys_[i] < OB_RANGE_EXTEND_VALUE ||
                               range_node->start_keys_[i] == OB_RANGE_NULL_VALUE  ||
                               range_node->end_keys_[i] < OB_RANGE_EXTEND_VALUE ||
                               range_node->end_keys_[i] == OB_RANGE_NULL_VALUE)) {
      extract_ranges[i] = true;
      if (OB_FAIL(new_idx.push_back(i))) {
        LOG_WARN("failed to push back idx", K(i));
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::serialize_range_graph(ObRangeNode *range_node,
                                           char *buf,
                                           int64_t buf_len,
                                           int64_t &pos) const
{
  int ret = OB_SUCCESS;
  for (ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    int64_t and_next_id = (nullptr == cur_node->and_next_) ? -1 : cur_node->and_next_->node_id_;
    int64_t or_next_id = (nullptr == cur_node->or_next_) ? -1 : cur_node->or_next_->node_id_;
    OB_UNIS_ENCODE(*cur_node);
    OB_UNIS_ENCODE(and_next_id);
    OB_UNIS_ENCODE(or_next_id);
    /**
     * Node id is generated by dfs algorithm. So if node id of and_next smaller than node id
     * of current node, and next node must have been serialized.
     *
     *     node(0) -- node(1) -- node(2)
     *       |                 /
     *     node(3) -- node(4) /
    */
    if (cur_node->and_next_ != nullptr && cur_node->and_next_->node_id_ > cur_node->node_id_) {
      if (OB_FAIL(SMART_CALL(serialize_range_graph(cur_node->and_next_, buf, buf_len, pos)))) {
        LOG_WARN("failed to serialize range graph");
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::deserialize_range_graph(ObRangeNode *&range_node,
                                             const char *buf,
                                             int64_t data_len,
                                             int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRangeNode*, 16> range_nodes;
  ObSEArray<std::pair<int64_t, int64_t>, 16> ptr_pairs;
  for (int64_t i = 0; OB_SUCC(ret) && i < node_count_; ++i) {
    int64_t and_next_id = -1;
    int64_t or_next_id = -1;
    ObRangeNode* node = nullptr;
    void *ptr = allocator_.alloc(sizeof(ObRangeNode));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memeory for ObRangeNode");
    } else {
      node = new(ptr)ObRangeNode(allocator_);
      OB_UNIS_DECODE(*node);
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(node->node_id_ != i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected node id", K(node->node_id_), K(i));
        } else if (OB_FAIL(range_nodes.push_back(node))) {
          LOG_WARN("failed to push back range node");
        }
      }
    }
    OB_UNIS_DECODE(and_next_id);
    OB_UNIS_DECODE(or_next_id);
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY((and_next_id != -1 && and_next_id >= node_count_) ||
                      (or_next_id != -1 && or_next_id >= node_count_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected and/or next id", K(and_next_id), K(or_next_id));
      } else if (OB_FAIL(ptr_pairs.push_back(std::pair<int64_t, int64_t>(and_next_id, or_next_id)))) {
        LOG_WARN("failed to push back and next id and or next id");
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < node_count_; ++i) {
      std::pair<int64_t, int64_t> &ptr_pair = ptr_pairs.at(i);
      range_nodes.at(i)->and_next_ = (-1 == ptr_pair.first) ? nullptr : range_nodes.at(ptr_pair.first);
      range_nodes.at(i)->or_next_ = (-1 == ptr_pair.second) ? nullptr : range_nodes.at(ptr_pair.second);
    }
    range_node = node_count_ > 0 ? range_nodes.at(0) : nullptr;
  }
  return ret;
}

int64_t ObPreRangeGraph::get_serialize_size_range_graph(const ObRangeNode *range_node) const
{
  int64_t len = 0;
  for (const ObRangeNode *cur_node = range_node; cur_node != nullptr; cur_node = cur_node->or_next_) {
    int64_t and_next_id = (nullptr == cur_node->and_next_) ? -1 : cur_node->and_next_->node_id_;
    int64_t or_next_id = (nullptr == cur_node->or_next_) ? -1 : cur_node->or_next_->node_id_;
    OB_UNIS_ADD_LEN(*cur_node);
    OB_UNIS_ADD_LEN(and_next_id);
    OB_UNIS_ADD_LEN(or_next_id);
    /**
     * Node id is generated by dfs algorithm. So if node id of and_next smaller than node id
     * of current node, and next node must have been serialized.
     *
     *     node(0) -- node(1) -- node(2)
     *       |                 /
     *     node(3) -- node(4) /
    */
    if (cur_node->and_next_ != nullptr && cur_node->and_next_->node_id_ > cur_node->node_id_) {
      len += get_serialize_size_range_graph(cur_node->and_next_);
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObPreRangeGraph)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(table_id_);
  OB_UNIS_ENCODE(column_count_);
  OB_UNIS_ENCODE(range_size_);
  OB_UNIS_ENCODE(node_count_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_range_graph(node_head_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize range graph");
    }
  }
  OB_UNIS_ENCODE(is_standard_range_);
  OB_UNIS_ENCODE(is_precise_get_);
  OB_UNIS_ENCODE(is_equal_range_);
  OB_UNIS_ENCODE(is_get_);
  OB_UNIS_ENCODE(contain_exec_param_);
  if (OB_SUCC(ret)) {
    int64_t count = column_metas_.count();
    OB_UNIS_ENCODE(count);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      OB_UNIS_ENCODE(*column_metas_.at(i));
    }
  }
  OB_UNIS_ENCODE(range_map_);
  OB_UNIS_ENCODE(skip_scan_offset_);
  OB_UNIS_ENCODE(flags_);
  if (OB_SUCC(ret)) {
    int64_t count = fast_final_pos_arr_.count();
    OB_UNIS_ENCODE(count);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      OB_UNIS_ENCODE(fast_final_pos_arr_.at(i));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPreRangeGraph)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(table_id_);
  OB_UNIS_DECODE(column_count_);
  OB_UNIS_DECODE(range_size_);
  OB_UNIS_DECODE(node_count_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(deserialize_range_graph(node_head_, buf, data_len, pos))) {
      LOG_WARN("failed to serialize range graph");
    }
  }
  OB_UNIS_DECODE(is_standard_range_);
  OB_UNIS_DECODE(is_precise_get_);
  OB_UNIS_DECODE(is_equal_range_);
  OB_UNIS_DECODE(is_get_);
  OB_UNIS_DECODE(contain_exec_param_);
  if (OB_SUCC(ret)) {
    column_metas_.reset();
    int64_t count = 0;
    OB_UNIS_DECODE(count);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_metas_.prepare_allocate(count))) {
        LOG_WARN("failed to prepare allocate column meta", K(count));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObRangeColumnMeta *column_meta = nullptr;
      void *ptr = allocator_.alloc(sizeof(ObRangeColumnMeta));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memeory for ObRangeColumnMeta");
      } else {
        column_meta = new(ptr)ObRangeColumnMeta();
        OB_UNIS_DECODE(*column_meta);
        if (OB_SUCC(ret)) {
          column_metas_.at(i) = column_meta;
        }
      }
    }
  }
  OB_UNIS_DECODE(range_map_);
  OB_UNIS_DECODE(skip_scan_offset_);
  OB_UNIS_DECODE(flags_);
  if (OB_SUCC(ret)) {
    fast_final_pos_arr_.reset();
    int64_t count = 0;
    OB_UNIS_DECODE(count);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fast_final_pos_arr_.prepare_allocate(count))) {
        LOG_WARN("failed to prepare allocate fast final pos arr", K(count));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      OB_UNIS_DECODE(fast_final_pos_arr_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPreRangeGraph)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(table_id_);
  OB_UNIS_ADD_LEN(column_count_);
  OB_UNIS_ADD_LEN(range_size_);
  OB_UNIS_ADD_LEN(node_count_);
  len += get_serialize_size_range_graph(node_head_);
  OB_UNIS_ADD_LEN(is_standard_range_);
  OB_UNIS_ADD_LEN(is_precise_get_);
  OB_UNIS_ADD_LEN(is_equal_range_);
  OB_UNIS_ADD_LEN(is_get_);
  OB_UNIS_ADD_LEN(contain_exec_param_);
  int64_t count = column_metas_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; ++i) {
    OB_UNIS_ADD_LEN(*column_metas_.at(i));
  }
  OB_UNIS_ADD_LEN(range_map_);
  OB_UNIS_ADD_LEN(skip_scan_offset_);
  OB_UNIS_ADD_LEN(flags_);
  count = fast_final_pos_arr_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; ++i) {
    OB_UNIS_ADD_LEN(fast_final_pos_arr_.at(i));
  }
  return len;
}


DEF_TO_STRING(ObPreRangeGraph)
{
  int64_t pos = 0;

  J_ARRAY_START();
  if (NULL != node_head_) {
    J_OBJ_START();
    J_KV(K_(table_id),
         K_(is_equal_range),
         K_(is_precise_get),
         K_(is_standard_range),
         K_(contain_exec_param),
         K_(flags));
    J_COMMA();
    J_NAME(N_RANGE_GRAPH);
    J_COLON();
    J_OBJ_START();
    pos += range_graph_to_string(buf + pos, buf_len - pos, node_head_);
    J_OBJ_END();
    J_OBJ_END();
  }
  J_ARRAY_END();
  return pos;
}

int64_t ObPreRangeGraph::range_graph_to_string(char *buf,
                                               const int64_t buf_len,
                                               ObRangeNode *range_node) const
{
  int64_t pos = 0;
  bool is_stack_overflow = false;
  int ret = OB_SUCCESS;
  for (ObRangeNode *cur_node = range_node; cur_node != nullptr; cur_node = cur_node->or_next_) {
    J_KV("range_node", *cur_node);
    J_COMMA();
    if (cur_node->and_next_ != nullptr) {
      pos += range_graph_to_string(buf + pos, buf_len - pos, cur_node->and_next_);
    }
  }
  return pos;
}

int64_t ObPreRangeGraph::set_total_range_sizes(uint64_t* total_range_sizes, int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(total_range_sizes_.prepare_allocate(count))) {
    LOG_WARN("failed to prepare allocate");
  } else {
    for (int64_t i = 0; i < count; ++i) {
      total_range_sizes_.at(i) = total_range_sizes[i];
    }
  }
  return ret;
}
int ObPreRangeGraph::get_total_range_sizes(common::ObIArray<uint64_t> &total_range_sizes) const
{
  return total_range_sizes.assign(total_range_sizes_);
}

int ObPreRangeGraph::get_range_exprs(ObRawExprFactory &expr_factory,
                                     const ObIArray<ColumnItem> &range_columns,
                                     const ObIArray<ObRawExpr*> &root_exprs,
                                     ObIArray<ObRawExpr*> &range_exprs,
                                     ObExecContext *exec_ctx,
                                     ObQueryCtx *query_ctx,
                                     const ParamStore *params,
                                     const int64_t index_prefix /* =-1*/)
{
  int ret = OB_SUCCESS;
  ExprConstrantArray *expr_constraints = nullptr;
  const bool phy_rowid_for_table_loc = false;
  const bool ignore_calc_failure = true;
  const ColumnIdInfoMap *geo_column_id_map = nullptr;
  const ObTableSchema *index_schema = nullptr;
  bool force_no_link = true;
  ObQueryRangeCtx ctx(exec_ctx);
  if (OB_FAIL(fill_column_metas(range_columns))) {
    LOG_WARN("failed to fill column metas");
  } else if (OB_FAIL(ctx.init(this, range_columns, expr_constraints,
                              params, &expr_factory,
                              phy_rowid_for_table_loc, ignore_calc_failure, index_prefix,
                              geo_column_id_map, force_no_link, index_schema,
                              NULL))) {
    LOG_WARN("failed to init query range context");
  } else {
    ObExprRangeConverter converter(allocator_, ctx);
    ObRangeGraphGenerator graph_generator(allocator_, ctx, this, range_columns.count());
    if (OB_FAIL(graph_generator.generate_range_graph(root_exprs, converter))) {
      LOG_WARN("failed to generate range graph");
    } else if (OB_FAIL(get_range_exprs_by_graph(ctx, expr_factory, range_columns, range_exprs))) {
      LOG_WARN("Failed to get range exprs by graph");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < range_exprs.count(); i ++) {
      if (OB_ISNULL(range_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(range_exprs));
      } else if (OB_FAIL(range_exprs.at(i)->formalize(exec_ctx->get_my_session()))) {
        LOG_WARN("failed to formalize", K(ret));
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::get_range_exprs_by_graph(ObQueryRangeCtx &ctx,
                                              ObRawExprFactory &expr_factory,
                                              const ObIArray<ColumnItem> &range_columns,
                                              ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> or_exprs;
  hash::ObHashMap<int64_t, ObRangeExprDesc*> expr_map;
  ExprDescList *expr_desc_lists = nullptr;
  bool *equals = nullptr;

  if (OB_ISNULL(node_head_) || OB_UNLIKELY(contain_geo_filters_)) {
    // do nothing
  } else if (node_head_->always_true_) {
    // do nothing
  } else if (node_head_->always_false_) {
    ObRawExpr *const_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(&expr_factory, const_expr, false))) {
      LOG_WARN("failed to build expr", K(ret));
    } else if (OB_FAIL(exprs.push_back(const_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else if (OB_FAIL(expr_map.create(node_count_, "RangeExprMap", "RangeExprMap"))) {
    LOG_WARN("fail to init hashmap", K(node_count_));
  } else if (OB_ISNULL(expr_desc_lists = static_cast<ExprDescList*>(allocator_.alloc(sizeof(ExprDescList) * range_columns.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for tmp expr list");
  } else if (OB_ISNULL(equals = static_cast<bool*>(allocator_.alloc(sizeof(bool) * range_columns.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory to equals array");
  } else {
    for (int64_t i = 0; i < range_columns.count(); ++i) {
      new(expr_desc_lists + i)ExprDescList();
    }
    MEMSET(equals, 0, sizeof(bool) * range_columns.count());
    if (OB_FAIL(recursive_generate_range_node_expr(ctx, expr_factory, expr_map, range_columns, node_head_, expr_desc_lists, equals, or_exprs))) {
      LOG_WARN("failed to recursive generate range node expr");
    } else if (or_exprs.empty()) {
      // do nothing
    } else if (or_exprs.count() == 1) {
      ObRawExpr* top_expr = or_exprs.at(0);
      if (T_OP_AND == top_expr->get_expr_type()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < top_expr->get_param_count(); ++i) {
          if (OB_FAIL(exprs.push_back(top_expr->get_param_expr(i)))) {
            LOG_WARN("failed to push back and exprs");
          }
        }
      } else if (OB_FAIL(exprs.push_back(top_expr))) {
        LOG_WARN("failed to push back and exprs");
      }
    } else {
      ObOpRawExpr *or_expr = nullptr;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_OR, or_expr))) {
        LOG_WARN("failed to create a new expr", K(ret));
      } else if (OB_FAIL(or_expr->set_param_exprs(or_exprs))) {
        LOG_WARN("failed to set param exprs", K(ret));
      }
      if (FAILEDx(exprs.push_back(or_expr))) {
        LOG_WARN("failed to push back and exprs");
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::recursive_generate_range_node_expr(ObQueryRangeCtx &ctx,
                                                        ObRawExprFactory &expr_factory,
                                                        common::hash::ObHashMap<int64_t, ObRangeExprDesc*> &expr_map,
                                                        const ObIArray<ColumnItem> &range_columns,
                                                        ObRangeNode *node,
                                                        ExprDescList *expr_desc_lists,
                                                        bool *equals,
                                                        ObIArray<ObRawExpr*> &or_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> new_idx;
  for (ObRangeNode* cur_node = node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    int64_t cur_node_offset = cur_node->min_offset_ == -1 ? 0 : cur_node->min_offset_;
    ObRawExpr *expr = nullptr;
    bool need_create = false;
    ObRangeExprDesc *expr_desc;

    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
    new_idx.reset();

    if (OB_FAIL(expr_map.get_refactored(cur_node->node_id_, expr_desc))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
        need_create = true;
      } else {
        LOG_WARN("failed to get from expr map", K(cur_node->node_id_));
      }
    }
    if (OB_SUCC(ret) && need_create) {
      if (OB_FAIL(range_node_to_expr(ctx, expr_factory, range_columns, cur_node, expr))) {
        LOG_WARN("failed to convert range node to expr");
      } else if (OB_ISNULL(expr_desc = static_cast<ObRangeExprDesc*>(allocator_.alloc(sizeof(ObRangeExprDesc))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for range expr desc");
      } else {
        expr_desc = new(expr_desc) ObRangeExprDesc(expr, cur_node->min_offset_, cur_node->max_offset_);
        if (OB_FAIL(expr_map.set_refactored(cur_node->node_id_, expr_desc))) {
          LOG_WARN("failed to set expr map");
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cur_node->min_offset_ >= 0 &&
               !cur_node->is_not_in_node_ &&
               !cur_node->is_domain_node_ &&
      OB_FAIL(ObRangeGraphGenerator::get_new_equal_idx(cur_node, equals, new_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (OB_UNLIKELY(!expr_desc_lists[cur_node_offset].add_last(expr_desc))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add last to dlist");
    } else if (cur_node->and_next_ != nullptr &&
               (equals[cur_node_offset] ||
                cur_node->min_offset_ < 0 ||
                cur_node->and_next_->min_offset_ == cur_node->min_offset_)) {
      if (OB_FAIL(SMART_CALL(recursive_generate_range_node_expr(ctx, expr_factory, expr_map, range_columns,
                                                                cur_node->and_next_, expr_desc_lists, equals,
                                                                or_exprs)))) {
        LOG_WARN("failed to recursive generate range node expr");
      }
    } else {
      ObSEArray<ObRawExpr*, 4> and_exprs;
      bool and_next = true;
      int64_t max_offset = -1;
      for (int64_t i = 0; OB_SUCC(ret) && and_next && i < range_columns.count(); ++i) {
        DLIST_FOREACH_X(cur_expr_desc, expr_desc_lists[i], (OB_SUCC(ret) && and_next)) {
          if (cur_expr_desc->min_offset_ > max_offset +1) {
            and_next = false;
          } else if (OB_FAIL(and_exprs.push_back(cur_expr_desc->expr_))) {
            LOG_WARN("failed to push back expr");
          } else if (cur_expr_desc->max_offset_ > max_offset) {
            max_offset = cur_expr_desc->max_offset_;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(and_exprs.empty())) {
        // do nothing
        ObConstRawExpr *int_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, 1, int_expr))) {
          LOG_WARN("failed to build const int expr");
        } else if (OB_FAIL(or_exprs.push_back(int_expr))) {
          LOG_WARN("failed to push back and exprs");
        }
      } else if (and_exprs.count() == 1) {
        if (OB_FAIL(or_exprs.push_back(and_exprs.at(0)))) {
          LOG_WARN("failed to push back and exprs");
        }
      } else {
        ObOpRawExpr *and_expr = nullptr;
        if (OB_FAIL(expr_factory.create_raw_expr(T_OP_AND, and_expr))) {
          LOG_WARN("failed to create a new expr", K(ret));
        } else if (OB_FAIL(and_expr->set_param_exprs(and_exprs))) {
          LOG_WARN("failed to set param exprs", K(ret));
        }
        if (FAILEDx(or_exprs.push_back(and_expr))) {
          LOG_WARN("failed to push back and exprs");
        }
      }
    }

    if (OB_SUCC(ret)) {
      expr_desc_lists[cur_node_offset].remove_last();
    }
  }

  // reset equal flag in current range node
  if (OB_SUCC(ret)) {
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
  }
  return ret;
}

int ObPreRangeGraph::range_node_to_expr(ObQueryRangeCtx &ctx,
                                        ObRawExprFactory &expr_factory,
                                        const ObIArray<ColumnItem> &range_columns,
                                        ObRangeNode *node,
                                        ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *column_expr = nullptr;
  if (OB_ISNULL(node) || node->always_true_ || node->always_false_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range node", KPC(node));
  } else if (node->is_not_in_node_) {
    ObRawExpr *column_expr = nullptr;
    if (OB_FAIL(get_node_column_expr(node, range_columns, column_expr))) {
      LOG_WARN("failed to get node column expr");
    } else {
      ObOpRawExpr *not_in_expr = nullptr;
      int64_t in_list_idx = -(node->start_keys_[node->min_offset_] + 1);
      InParam* in_param = range_map_.in_params_.at(in_list_idx);
      ObOpRawExpr *row_in_expr = nullptr;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, row_in_expr))) {
        LOG_WARN("fail to create equal expr");
      } else if (OB_FAIL(row_in_expr->init_param_exprs(in_param->count()))) {
        LOG_WARN("failed to set param exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < in_param->count(); ++i) {
        int64_t in_val_idx = in_param->at(i);
        ObRawExpr *in_val_expr = range_map_.expr_final_infos_.at(in_val_idx).related_raw_expr_;
        if (OB_FAIL(row_in_expr->add_param_expr(in_val_expr))) {
          LOG_WARN("failed to push back in val expr");
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(create_op_expr(expr_factory, T_OP_NOT_IN, expr, column_expr, row_in_expr))) {
        LOG_WARN("failed to create op expr");
      }
    }
  } else if (node->contain_in_) {
    bool is_rowid = node->is_rowid_node_;
    ObOpRawExpr *in_list_expr = nullptr;
    if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, in_list_expr))) {
      LOG_WARN("fail to create equal expr");
    } else if (is_rowid && OB_FAIL(create_rowid_expr(expr_factory, range_columns, column_expr))) {
      LOG_WARN("faled to create rowid expr");
    } else if (node->min_offset_ == node->max_offset_) {
      if (!is_rowid && OB_FAIL(get_node_column_expr(node, range_columns, column_expr))) {
        LOG_WARN("failed to get node column expr");
      } else {
        int64_t in_list_idx = -(node->start_keys_[node->min_offset_] + 1);
        InParam* in_param = range_map_.in_params_.at(in_list_idx);
        if (OB_FAIL(in_list_expr->init_param_exprs(in_param->count()))) {
          LOG_WARN("failed to set param exprs", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < in_param->count(); ++i) {
          int64_t in_val_idx = in_param->at(i);
          ObRawExpr *in_val_expr = range_map_.expr_final_infos_.at(in_val_idx).related_raw_expr_;
          if (OB_FAIL(in_list_expr->add_param_expr(in_val_expr))) {
            LOG_WARN("failed to push back in val expr");
          }
        }
      }
    } else {
      ObSEArray<InParam*,4> in_params;
      int64_t in_count = 0;
      if (OB_FAIL(!is_rowid && get_node_row_column_expr(node, range_columns, expr_factory, column_expr))) {
        LOG_WARN("failed to get node row column expr");
      }
      for (int64_t i = node->min_offset_; OB_SUCC(ret) && i <= node->max_offset_; ++i) {
        int64_t in_list_idx = -(node->start_keys_[i] + 1);
        InParam* in_param = range_map_.in_params_.at(in_list_idx);
        if (OB_FAIL(in_params.push_back(in_param))) {
          LOG_WARN("failed to push back in param");
        } else {
          in_count = in_param->count();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(in_list_expr->init_param_exprs(in_count))) {
        LOG_WARN("failed to init param exprs", K(ret));
      } else if (is_rowid) {
        InParam* in_param = in_params.at(0);
        for (int64_t i = 0; OB_SUCC(ret) && i < in_count; ++i) {
          int64_t in_val_idx = in_param->at(i);
          ObRawExpr *in_val_expr = range_map_.expr_final_infos_.at(in_val_idx).related_raw_expr_;
          if (OB_FAIL(in_list_expr->add_param_expr(in_val_expr))) {
            LOG_WARN("failed to add param expr");
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < in_count; ++i) {
          ObOpRawExpr *row_expr = nullptr;
          if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, row_expr))) {
            LOG_WARN("fail to create equal expr");
          } else if (OB_FAIL(row_expr->init_param_exprs(in_params.count()))) {
            LOG_WARN("failed to init param exprs", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < in_params.count(); ++j) {
            int64_t in_val_idx = in_params.at(j)->at(i);
            ObRawExpr *in_val_expr = range_map_.expr_final_infos_.at(in_val_idx).related_raw_expr_;
            if (OB_FAIL(row_expr->add_param_expr(in_val_expr))) {
              LOG_WARN("failed to push back in val expr");
            }
          }
          if (FAILEDx(in_list_expr->add_param_expr(row_expr))) {
            LOG_WARN("failed to add param expr");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_op_expr(expr_factory, T_OP_IN, expr, column_expr, in_list_expr))) {
        LOG_WARN("failed to create op expr");
      }
    }
  } else if (node->is_domain_node_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("geo node to expr not support");
  } else if (node->is_like_node_) {
    ObRawExpr *column_expr = nullptr;
    int64_t val_idx = node->start_keys_[node->min_offset_];
    ObRawExpr *decode_like_expr = range_map_.expr_final_infos_.at(val_idx).related_raw_expr_;
    ObRawExpr *pattern_expr = nullptr;
    ObRawExpr *escape_expr = nullptr;
    if (OB_ISNULL(decode_like_expr) ||
        OB_ISNULL(pattern_expr = decode_like_expr->get_param_expr(0)) ||
        OB_ISNULL(escape_expr = decode_like_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", KPC(decode_like_expr));
    } else if (OB_FAIL(get_node_column_expr(node, range_columns, column_expr))) {
      LOG_WARN("failed to get node column expr");
    } else {
      bool create_like = false;
      bool is_start_with = false;
      bool all_is_percent_sign = false;
      if (!pattern_expr->is_static_const_expr() || !escape_expr->is_static_const_expr()) {
        create_like = true;
      } else if (OB_FAIL(ObOptEstUtils::if_expr_start_with_patten_sign(ctx.params_,
                                                                       pattern_expr,
                                                                       escape_expr,
                                                                       ctx.exec_ctx_,
                                                                       allocator_,
                                                                       is_start_with,
                                                                       all_is_percent_sign))) {
        LOG_WARN("failed to check if expr start with percent sign", K(ret));
      } else {
        create_like = !is_start_with && !all_is_percent_sign;
      }
      if (OB_SUCC(ret)) {
        if (create_like) {
          if (OB_FAIL(create_op_expr(expr_factory, T_OP_LIKE, expr,
                                     column_expr, pattern_expr, escape_expr))) {
            LOG_WARN("failed to create op expr");
          }
        } else {
          if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory, column_expr, true, expr))) {
            LOG_WARN("failed to build is null expr");
          }
        }
      }
    }
  } else if (-1 == node->min_offset_) {
    // const expr
    if (node->min_offset_ != node->max_offset_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected range node", KPC(node));
    } else {
      ObRawExpr *const_expr = nullptr;
      int64_t val_idx = node->start_keys_[0];
      ObRawExpr *decode_const_expr = range_map_.expr_final_infos_.at(val_idx).related_raw_expr_;
      if (OB_ISNULL(decode_const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", KPC(decode_const_expr));
      } else {
        expr = decode_const_expr->get_param_expr(0);
      }
    }
  } else if (node->min_offset_ == node->max_offset_) {
    // simple condition
    bool is_rowid = node->is_rowid_node_;
    ObRawExpr *value_expr = nullptr;
    if (!is_rowid && OB_FAIL(get_node_column_expr(node, range_columns, column_expr))) {
      LOG_WARN("failed to get node column expr");
    } else if (is_rowid && OB_FAIL(create_rowid_expr(expr_factory, range_columns, column_expr))) {
      LOG_WARN("faled to create rowid expr");
    } else {
      int64_t start_val_idx = node->start_keys_[node->min_offset_];
      int64_t end_val_idx = node->end_keys_[node->min_offset_];
      ObItemType op_type = T_OP_EQ;
      if (start_val_idx == end_val_idx) {
        if (start_val_idx == OB_RANGE_NULL_VALUE) {
          // col is null
          op_type = T_OP_IS;
          if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory, column_expr, false, expr))) {
            LOG_WARN("failed to build is null expr");
          }
        } else {
          value_expr = range_map_.expr_final_infos_.at(start_val_idx).related_raw_expr_;
        }
      } else if (start_val_idx < OB_RANGE_EXTEND_VALUE && end_val_idx >= OB_RANGE_EXTEND_VALUE) {
        op_type = T_OP_GT;
        value_expr = range_map_.expr_final_infos_.at(start_val_idx).related_raw_expr_;
        if ((node->max_offset_ + 1 == node->column_cnt_ && node->include_start_) ||
            (node->max_offset_ + 1 < node->column_cnt_ &&
             node->start_keys_[node->max_offset_ + 1] == OB_RANGE_MIN_VALUE)) {
          op_type = T_OP_GE;
        }
      } else if (start_val_idx >= OB_RANGE_EXTEND_VALUE && end_val_idx < OB_RANGE_EXTEND_VALUE) {
        op_type = T_OP_LT;
        value_expr = range_map_.expr_final_infos_.at(end_val_idx).related_raw_expr_;
        if ((node->max_offset_ + 1 == node->column_cnt_ && node->include_end_) ||
            (node->max_offset_ + 1 < node->column_cnt_ &&
             node->end_keys_[node->max_offset_ + 1] == OB_RANGE_MAX_VALUE)) {
          op_type = T_OP_LE;
        }
      } else if ((is_mysql_mode() && start_val_idx == OB_RANGE_NULL_VALUE && end_val_idx == OB_RANGE_MAX_VALUE) ||
                 (is_oracle_mode() && start_val_idx == OB_RANGE_MIN_VALUE && end_val_idx == OB_RANGE_NULL_VALUE)) {
        // is not null expr
        // mysql mode: (null, max)
        // oracle mode: (min, null)
        op_type = T_OP_IS;
        if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory, column_expr, true, expr))) {
          LOG_WARN("failed to build is null expr");
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", KPC(node));
      }

      if (OB_SUCC(ret) && T_OP_IS != op_type) {
        if (OB_FAIL(create_op_expr(expr_factory, op_type, expr, column_expr, value_expr))) {
          LOG_WARN("failed to create op expr");
        }
      }
    }
  } else {
    // row condition
    bool is_rowid = node->is_rowid_node_;
    ObOpRawExpr *row_value_expr = nullptr;
    ObRawExpr *rowid_value_expr = nullptr;
    if (!is_rowid && OB_FAIL(get_node_row_column_expr(node, range_columns, expr_factory, column_expr))) {
      LOG_WARN("failed to get node row column expr");
    } else if (is_rowid && OB_FAIL(create_rowid_expr(expr_factory, range_columns, column_expr))) {
      LOG_WARN("faled to create rowid expr");
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, row_value_expr))) {
      LOG_WARN("fail to create equal expr");
    } else {
      ObItemType op_type = T_OP_EQ;
      int64_t start_val_idx = node->start_keys_[node->min_offset_];
      int64_t end_val_idx = node->end_keys_[node->min_offset_];
      if (start_val_idx == end_val_idx) {
        // do nothing
      } else if (start_val_idx < OB_RANGE_EXTEND_VALUE && end_val_idx >= OB_RANGE_EXTEND_VALUE) {
        op_type = T_OP_GT;
        if ((node->max_offset_ + 1 == node->column_cnt_ && node->include_start_) ||
            (node->max_offset_ + 1 < node->column_cnt_ &&
             node->start_keys_[node->max_offset_ + 1] == OB_RANGE_MIN_VALUE)) {
          op_type = T_OP_GE;
        }
      } else if (start_val_idx >= OB_RANGE_EXTEND_VALUE && end_val_idx < OB_RANGE_EXTEND_VALUE) {
        op_type = T_OP_LT;
        if ((node->max_offset_ + 1 == node->column_cnt_ && node->include_end_) ||
            (node->max_offset_ + 1 < node->column_cnt_ &&
             node->end_keys_[node->max_offset_ + 1] == OB_RANGE_MAX_VALUE)) {
          op_type = T_OP_LE;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", KPC(node));
      }
      if (is_rowid) {
        int64_t val_idx = (T_OP_LT == op_type || T_OP_LE == op_type) ? node->end_keys_[node->min_offset_] : node->start_keys_[node->min_offset_];
        ObRawExpr *value_expr = range_map_.expr_final_infos_.at(val_idx).related_raw_expr_;
        rowid_value_expr = value_expr;
      } else {
        if (OB_FAIL(row_value_expr->init_param_exprs(node->max_offset_ - node->min_offset_ + 1))) {
          LOG_WARN("failed to init param exprs", K(ret));
        }
        for (int64_t i = node->min_offset_; OB_SUCC(ret) && i <= node->max_offset_; ++i) {
          int64_t val_idx = (T_OP_LT == op_type || T_OP_LE == op_type) ? node->end_keys_[i] : node->start_keys_[i];
          ObRawExpr *value_expr = range_map_.expr_final_infos_.at(val_idx).related_raw_expr_;
          if (OB_FAIL(row_value_expr->add_param_expr(value_expr))) {
            LOG_WARN("failed to add params for op expr", KPC(value_expr));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (is_rowid) {
          if (OB_FAIL(create_op_expr(expr_factory, op_type, expr, column_expr, rowid_value_expr))) {
            LOG_WARN("failed to create op expr");
          }
        } else if (OB_FAIL(create_op_expr(expr_factory, op_type, expr, column_expr, row_value_expr))) {
          LOG_WARN("failed to create op expr");
        }
      }
    }
  }
  return ret;
}

int ObPreRangeGraph::get_node_column_expr(ObRangeNode *node,
                                          const ObIArray<ColumnItem> &range_columns,
                                          ObRawExpr* &column_expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(node->min_offset_ != node->max_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range node", KPC(node));
  } else {
    int64_t column_idx = node->min_offset_;
    if (OB_UNLIKELY(column_idx < 0 || column_idx >= range_columns.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected column idx", K(column_idx), K(range_columns));
    } else {
      column_expr = range_columns.at(column_idx).expr_;
    }
  }
  return ret;
}

int ObPreRangeGraph::get_node_row_column_expr(ObRangeNode *node,
                                              const ObIArray<ColumnItem> &range_columns,
                                              ObRawExprFactory &expr_factory,
                                              ObRawExpr* &column_expr)
{
  int ret = OB_SUCCESS;
  int64_t min_column_idx = node->min_offset_;
  int64_t max_column_idx = node->max_offset_;
  ObOpRawExpr *row_column_expr = nullptr;
  if (min_column_idx < 0 || min_column_idx >= max_column_idx || max_column_idx >= range_columns.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column idx", K(min_column_idx), K(max_column_idx), K(range_columns));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, row_column_expr))) {
    LOG_WARN("fail to create equal expr");
  } else if (OB_FAIL(row_column_expr->init_param_exprs(max_column_idx - min_column_idx + 1))) {
    LOG_WARN("failed to init param exprs", K(ret));
  }
  for (int64_t i = min_column_idx; OB_SUCC(ret) && i <= max_column_idx; ++i) {
    if (OB_FAIL(row_column_expr->add_param_expr(range_columns.at(i).expr_))) {
      LOG_WARN("failed to add params for op expr", KPC(range_columns.at(i).expr_));
    }
  }
  if (OB_SUCC(ret)) {
    column_expr = row_column_expr;
  }
  return ret;
}

int ObPreRangeGraph::create_rowid_expr(ObRawExprFactory &expr_factory,
                                       const ObIArray<ColumnItem> &range_columns,
                                       ObRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *calc_urowid_expr = nullptr;
  // mocked rowid exprs, only used by selectivity estimation !!!
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CALC_UROWID, calc_urowid_expr))) {
    LOG_WARN("faled to create rowid expr");
  } else if (OB_ISNULL(calc_urowid_expr) || OB_UNLIKELY(range_columns.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(range_columns));
  } else if (OB_FAIL(calc_urowid_expr->set_param_expr(range_columns.at(0).expr_))) {
    LOG_WARN("failed to set param expr for rowid expr", KPC(range_columns.at(0).expr_));
  } else {
    calc_urowid_expr->set_func_name(ObString::make_string(N_CALC_UROWID));
    rowid_expr = calc_urowid_expr;
  }
  return ret;
}

int ObPreRangeGraph::create_op_expr(ObRawExprFactory &expr_factory,
                                    ObItemType op_type,
                                    ObRawExpr *&op_expr,
                                    ObRawExpr *column_expr,
                                    ObRawExpr *value_expr,
                                    ObRawExpr *extra_value_expr /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* tmp_expr = nullptr;
  if (OB_FAIL(expr_factory.create_raw_expr(op_type, tmp_expr))) {
    LOG_WARN("failed to create a new expr", K(op_type));
  } else if (OB_FAIL(tmp_expr->init_param_exprs(extra_value_expr != nullptr ? 3: 2))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(tmp_expr->add_param_expr(column_expr)) ||
             OB_FAIL(tmp_expr->add_param_expr(value_expr))) {
    LOG_WARN("failed to add param for op expr", KPC(column_expr), KPC(value_expr));
  } else if (extra_value_expr != nullptr && OB_FAIL(tmp_expr->add_param_expr(extra_value_expr))) {
    LOG_WARN("failed to add extra for op expr", KPC(extra_value_expr));
  } else {
    op_expr = tmp_expr;
  }
  return ret;
}

int ObPreRangeGraph::set_general_nlj_range_extraction(const ObIArray<ObFastFinalPos> &pos_arr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fast_final_pos_arr_.assign(pos_arr))) {
    LOG_WARN("failed to assign array", K(ret));
  } else {
    general_nlj_range_ = true;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
