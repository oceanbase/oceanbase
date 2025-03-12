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

namespace oceanbase {
using namespace common;
namespace sql {

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
OB_SERIALIZE_MEMBER(ObRangeColumnMeta, column_type_);

int ObQueryRangeCtx::init(ObPreRangeGraph *pre_range_graph,
                          const ObIArray<ColumnItem> &range_columns,
                          ExprConstrantArray *expr_constraints,
                          const ParamStore *params,
                          ObRawExprFactory *expr_factory,
                          const bool phy_rowid_for_table_loc,
                          const bool ignore_calc_failure,
                          const int64_t index_prefix,
                          const ColumnIdInfoMap *geo_column_id_map,
                          const ObTableSchema *index_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pre_range_graph)  || OB_ISNULL(exec_ctx_) || OB_ISNULL(exec_ctx_->get_my_session()) ||
      OB_UNLIKELY(range_columns.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(pre_range_graph), K(exec_ctx_));
  } else if (OB_FAIL(column_metas_.assign(pre_range_graph->get_column_metas()))) {
    LOG_WARN("failed to assign column meta");
  } else if (OB_FAIL(column_flags_.prepare_allocate(pre_range_graph->get_column_metas().count()))) {
    LOG_WARN("failed to prepare allocate");
  } else if (OB_FAIL(exec_ctx_->get_my_session()->
             is_enable_range_extraction_for_not_in(enable_not_in_range_))) {
    LOG_WARN("failed to check not in range enabled", K(ret));
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
                                                     const ColumnIdInfoMap *geo_column_id_map /* = NULL*/)
{
  int ret = OB_SUCCESS;
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
                              geo_column_id_map, index_schema))) {
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
                                                void *range_buffer,
                                                ObQueryRangeArray &ranges,
                                                const common::ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fast_nlj_range_ctx.has_check_valid)) {
    if (OB_FAIL(ObRangeGenerator::check_can_final_fast_nlj_range(*this,
                                                                 param_store,
                                                                 fast_nlj_range_ctx.is_valid))) {
      LOG_WARN("failed to check can final fast nlj range", K(ret));
    }
    fast_nlj_range_ctx.has_check_valid = true;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(fast_nlj_range_ctx.is_valid)) {
    if (OB_FAIL(ObRangeGenerator::generate_fast_nlj_range(*this,
                                                          param_store,
                                                          allocator,
                                                          range_buffer))) {
      LOG_WARN("failed to generate fast nlj range", K(ret));
    } else if (OB_FAIL(ranges.push_back(static_cast<ObNewRange*>(range_buffer)))) {
      LOG_WARN("failed to push back array", K(ret));
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
      ObRangeColumnMeta *column_meta = new(ptr)ObRangeColumnMeta(column_expr->get_result_type());
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
  range_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
  if (OB_ISNULL(node_head_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range node");
  } else if (node_head_->always_true_ || node_head_->always_false_) {
    equal_prefix_count = 0;
    range_prefix_count = 0;
    contain_always_false = node_head_->always_false_;
  } else {
    bool equals[column_count_];
    MEMSET(equals, 0, sizeof(bool) * column_count_);
    if (OB_FAIL(get_prefix_info(node_head_, equals, equal_prefix_count))) {
      LOG_WARN("failed to get prefix info");
    } else {
      range_prefix_count = (equal_prefix_count == column_count_) ? equal_prefix_count : equal_prefix_count + 1;
    }
  }
  return ret;
}

int ObPreRangeGraph::get_prefix_info(const ObRangeNode *range_node,
                                     bool* equals,
                                     int64_t &equal_prefix_count) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> new_idx;
  for (const ObRangeNode *cur_node = range_node; OB_SUCC(ret) && cur_node != NULL; cur_node = cur_node->or_next_) {
    // reset equal flag in previous or range node
    for (int64_t j = 0; j < new_idx.count(); ++j) {
      equals[new_idx.at(j)] = false;
    }
    new_idx.reset();

    if (cur_node->min_offset_ >= 0 &&
        OB_FAIL(get_new_equal_idx(cur_node, equals, new_idx))) {
      LOG_WARN("failed to get new idx");
    } else if (cur_node->and_next_ != nullptr) {
      if (OB_FAIL(SMART_CALL(get_prefix_info(cur_node->and_next_, equals, equal_prefix_count)))) {
        LOG_WARN("failed to check is strict equal graph");
      }
    } else {
      int64_t cur_pos = 0;
      for (; cur_pos < column_count_; ++cur_pos) {
        if (!equals[cur_pos]) {
          break;
        }
      }
      equal_prefix_count = std::min(equal_prefix_count, cur_pos);
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
         K_(contain_exec_param));
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

} // namespace sql
} // namespace oceanbase
