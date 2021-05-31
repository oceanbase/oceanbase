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

#define USING_LOG_PREFIX SQL_OPT
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_i_data_access_service.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "share/part/ob_part_mgr_ad.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/ob_sql_mock_schema_utils.h"

using namespace oceanbase::transaction;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

static int get_part_id_by_mod(const int64_t calc_result, const int64_t part_num, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  if (calc_result < 0 || 0 == part_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments is invalid", K(calc_result), K(part_num), K(ret));
  } else {
    part_id = calc_result % part_num;
  }
  return ret;
}

static bool is_all_ranges_empty(const ObQueryRangeArray& query_array, bool& is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  for (int i = 0; is_empty && OB_SUCCESS == ret && i < query_array.count(); i++) {
    if (NULL == query_array.at(i)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (!query_array.at(i)->empty()) {
      is_empty = false;
    }
  }
  return ret;
}

bool ObListPartMapKey::operator==(const ObListPartMapKey& other) const
{
  return row_ == other.row_;
}

int64_t ObListPartMapKey::hash() const
{
  int64_t hash_value = 0;
  for (int64_t i = 0; i < row_.get_count(); i++) {
    hash_value = row_.get_cell(i).hash(hash_value);
  }
  return hash_value;
}

bool ObHashPartMapKey::operator==(const ObHashPartMapKey& other) const
{
  return part_idx_ == other.part_idx_;
}

int64_t ObHashPartMapKey::hash() const
{
  int64_t hash_value = 0;
  ObObj idx_obj(part_idx_);
  hash_value = idx_obj.hash(hash_value);
  return hash_value;
}

bool TableLocationKey::operator==(const TableLocationKey& other) const
{
  return table_id_ == other.table_id_ && ref_table_id_ == other.ref_table_id_;
}

bool TableLocationKey::operator!=(const TableLocationKey& other) const
{
  return !(*this == other);
}

int ObTableLocation::PartProjector::init_part_projector(
    const ObRawExpr* part_expr, ObPartitionLevel part_level, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ONE == part_level) {
    ret = init_part_projector(part_expr, row_desc);
  } else if (PARTITION_LEVEL_TWO == part_level) {
    ret = init_subpart_projector(part_expr, row_desc);
  } else { /*do nothing*/
  }
  return ret;
}

int ObTableLocation::PartProjector::init_part_projector(const ObRawExpr* part_expr, RowDesc& row_desc)
{
  return init_part_projector(part_expr, row_desc, part_projector_, part_projector_size_);
}

int ObTableLocation::PartProjector::init_subpart_projector(const ObRawExpr* part_expr, RowDesc& row_desc)
{
  return init_part_projector(part_expr, row_desc, subpart_projector_, subpart_projector_size_);
}

int ObTableLocation::PartProjector::init_part_projector(
    const ObRawExpr* part_expr, RowDesc& row_desc, int32_t*& projector, int64_t& projector_size)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> part_columns;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_columns))) {
    LOG_WARN("extract column exprs failed", K(ret));
  } else {
    projector_size = part_columns.count();
    if (OB_ISNULL(projector = static_cast<int32_t*>(allocator_.alloc(projector_size * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(projector_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < projector_size; ++i) {
      int64_t idx = OB_INVALID_INDEX;
      const ObRawExpr* part_column = part_columns.at(i);
      if (OB_ISNULL(part_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part column is null");
      } else if (OB_UNLIKELY(!part_column->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part column isn't column reference", K(ret), K(*part_column));
      } else if (OB_FAIL(row_desc.get_idx(part_column, idx)) && OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get idx failed", K(ret));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        const ObColumnRefRawExpr* col_ref = static_cast<const ObColumnRefRawExpr*>(part_column);
        ObColumnExpression* virtual_col = NULL;
        if (OB_UNLIKELY(!col_ref->is_generated_column())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column reference isn't generated column", K(ret), K(*col_ref));
        } else if (OB_FAIL(ObExprGeneratorImpl::gen_expression_with_row_desc(
                       sql_expr_factory_, expr_op_factory_, row_desc, col_ref->get_dependant_expr(), virtual_col))) {
          LOG_WARN("generate expression with row desc failed", K(ret), K(*col_ref), K(row_desc));
        } else {
          virtual_col->set_result_index(row_desc.get_column_num());
          if (OB_FAIL(row_desc.add_column(const_cast<ObRawExpr*>(part_column)))) {
            LOG_WARN("add part column to row desc failed", K(ret));
          } else if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(virtual_column_exprs_, virtual_col))) {
            LOG_WARN("add virtual column expr failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(row_desc.get_idx(part_column, idx))) {
          LOG_WARN("part_column isn't invalid", K(ret), K(*part_column));
        } else {
          projector[i] = static_cast<int32_t>(idx);
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::PartProjector::calc_part_row(
    const stmt::StmtType& stmt_type, ObExecContext& ctx, const ObNewRow& input_row, ObNewRow*& part_row) const
{
  int ret = OB_SUCCESS;
  ObNewRow& cur_part_row = ctx.get_part_row_manager().get_part_row();
  if (virtual_column_exprs_.get_size() <= 0) {
    cur_part_row.cells_ = input_row.cells_;
    cur_part_row.count_ = column_cnt_;
  } else {
    cur_part_row.count_ = column_cnt_;
    ObExprCtx expr_ctx;
    if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type, ctx, ctx.get_allocator(), expr_ctx))) {
      LOG_WARN("wrap expr ctx failed", K(ret));
    } else if (OB_ISNULL(cur_part_row.cells_)) {
      int64_t row_size = sizeof(ObObj) * column_cnt_;
      cur_part_row.cells_ = static_cast<ObObj*>(ctx.get_allocator().alloc(row_size));
      if (OB_ISNULL(cur_part_row.cells_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for current part row failed", K(ret), K(row_size));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t copy_row_size = (column_cnt_ - virtual_column_exprs_.get_size()) * sizeof(ObObj);
      memcpy(cur_part_row.cells_, input_row.cells_, copy_row_size);
    }
    DLIST_FOREACH(node, virtual_column_exprs_)
    {
      const ObColumnExpression* virtual_col = static_cast<const ObColumnExpression*>(node);
      if (OB_FAIL(virtual_col->calc_and_project(expr_ctx, cur_part_row))) {
        LOG_WARN("calc and project part row failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_row = &cur_part_row;
  }
  return ret;
}

void ObTableLocation::PartProjector::project_part_row(ObPartitionLevel part_level, ObNewRow& part_row) const
{
  int32_t* projector = NULL;
  int64_t projector_size = 0;
  if (part_level == PARTITION_LEVEL_ONE) {
    projector = part_projector_;
    projector_size = part_projector_size_;
  } else if (part_level == PARTITION_LEVEL_TWO) {
    projector = subpart_projector_;
    projector_size = subpart_projector_size_;
  } else { /*do nothing*/
  }
  part_row.projector_ = projector;
  part_row.projector_size_ = projector_size;
}

int ObTableLocation::PartProjector::deep_copy(const PartProjector& other)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  int64_t buf_len = 0;
  part_projector_size_ = other.part_projector_size_;
  subpart_projector_size_ = other.subpart_projector_size_;
  column_cnt_ = other.column_cnt_;
  if (other.part_projector_ != NULL) {
    buf_len = sizeof(int32_t) * other.part_projector_size_;
    if (OB_ISNULL(ptr = allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate projector buffer failed", K(buf_len), K_(part_projector));
    } else {
      part_projector_ = static_cast<int32_t*>(ptr);
      memcpy(part_projector_, other.part_projector_, buf_len);
    }
  }
  if (OB_SUCC(ret) && other.subpart_projector_ != NULL) {
    buf_len = sizeof(int32_t) * other.subpart_projector_size_;
    if (OB_ISNULL(ptr = allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate subpart projector buffer failed", K(buf_len), K_(subpart_projector_size));
    } else {
      subpart_projector_ = static_cast<int32_t*>(ptr);
      memcpy(subpart_projector_, other.subpart_projector_, buf_len);
    }
  }
  DLIST_FOREACH(node, other.virtual_column_exprs_)
  {
    ObColumnExpression* virtual_col = NULL;
    const ObColumnExpression* other_col = static_cast<const ObColumnExpression*>(node);
    if (OB_FAIL(sql_expr_factory_.alloc(virtual_col)) || OB_ISNULL(virtual_col)) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("alloc virtual column failed", K(ret));
    } else if (OB_FAIL(virtual_col->assign(*other_col))) {
      LOG_WARN("assign virtual column failed", K(ret));
    } else if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(virtual_column_exprs_, virtual_col))) {
      LOG_WARN("add expr to list failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableLocation::PartProjector)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(part_projector_, part_projector_size_);
  OB_UNIS_ENCODE_ARRAY(subpart_projector_, subpart_projector_size_);
  OB_UNIS_ENCODE(column_cnt_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(virtual_column_exprs_, buf, buf_len, pos))) {
      LOG_WARN("serialize virtual column expr list failed", K(ret), K(buf_len), K(pos));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLocation::PartProjector)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(part_projector_, part_projector_size_);
  OB_UNIS_ADD_LEN_ARRAY(subpart_projector_, subpart_projector_size_);
  OB_UNIS_ADD_LEN(column_cnt_);
  len += get_dlist_serialize_size(virtual_column_exprs_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableLocation::PartProjector)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  int64_t vir_col_size = 0;
  OB_UNIS_DECODE(part_projector_size_);
  if (OB_SUCC(ret) && part_projector_size_ > 0) {
    int64_t buf_len = sizeof(int32_t) * part_projector_size_;
    if (OB_ISNULL(ptr = allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate projector buffer failed", K(buf_len), K_(part_projector));
    } else {
      part_projector_ = static_cast<int32_t*>(ptr);
      OB_UNIS_DECODE_ARRAY(part_projector_, part_projector_size_);
    }
  }
  OB_UNIS_DECODE(subpart_projector_size_);
  if (OB_SUCC(ret) && subpart_projector_size_ > 0) {
    int64_t buf_len = sizeof(int32_t) * subpart_projector_size_;
    if (OB_ISNULL(ptr = allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate projector buffer failed", K(buf_len), K_(subpart_projector));
    } else {
      subpart_projector_ = static_cast<int32_t*>(ptr);
      OB_UNIS_DECODE_ARRAY(subpart_projector_, subpart_projector_size_);
    }
  }
  OB_UNIS_DECODE(column_cnt_);
  OB_UNIS_DECODE(vir_col_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < vir_col_size; ++i) {
    ObColumnExpression* virtual_column = NULL;
    if (OB_FAIL(sql_expr_factory_.alloc(virtual_column))) {
      LOG_WARN("allocate virtual column failed", K(ret), K(i));
    }
    OB_UNIS_DECODE(*virtual_column);
    if (OB_SUCC(ret) && OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(virtual_column_exprs_, virtual_column))) {
      LOG_WARN("add expr to virtual column expr list failed", K(ret));
    }
  }
  return ret;
}

ObPartLocCalcNode* ObPartLocCalcNode::create_part_calc_node(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode::NodeType type)
{
  void* ptr = NULL;
  ObPartLocCalcNode* ret_node = NULL;
  switch (type) {
    case ObPartLocCalcNode::QUERY_RANGE: {
      ptr = allocator.alloc(sizeof(ObPLQueryRangeNode));
      if (NULL != ptr) {
        ret_node = new (ptr) ObPLQueryRangeNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::FUNC_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLFuncValueNode));
      if (NULL != ptr) {
        ret_node = new (ptr) ObPLFuncValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::COLUMN_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLColumnValueNode));
      if (NULL != ptr) {
        ret_node = new (ptr) ObPLColumnValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_AND: {
      ptr = allocator.alloc(sizeof(ObPLAndNode));
      if (NULL != ptr) {
        ret_node = new (ptr) ObPLAndNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_OR: {
      ptr = allocator.alloc(sizeof(ObPLOrNode));
      if (NULL != ptr) {
        ret_node = new (ptr) ObPLOrNode(allocator);
      }
      break;
    }
    default: {
      LOG_WARN("Invalid ObPartLocCalcNode type", K(type));
      break;
    }
  }
  if (OB_UNLIKELY(NULL == ptr) || OB_UNLIKELY(NULL == ret_node)) {
    LOG_WARN("Failed to allocate ObPartLocCalcNode", K(type));
  } else if (OB_SUCCESS != calc_nodes.push_back(ret_node)) {
    ret_node->~ObPartLocCalcNode();
    allocator.free(ret_node);
    ret_node = NULL;
    LOG_WARN("Store ObPartLocCalcNode failed");
  } else {
  }  // do nothing

  return ret_node;
}

int ObPLAndNode::deep_copy(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode*& other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, CALC_AND))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPartLocCalcNode* left_node = NULL;
    ObPartLocCalcNode* right_node = NULL;
    if (NULL != left_node_ && OB_FAIL(left_node_->deep_copy(allocator, calc_nodes, left_node))) {
      LOG_WARN("Failed to deep copy left node", K(ret));
    } else if (NULL != right_node_ && OB_FAIL(right_node_->deep_copy(allocator, calc_nodes, right_node))) {
      LOG_WARN("Failed todeep copy right node", K(ret));
    } else {
      ObPLAndNode* and_node = static_cast<ObPLAndNode*>(other);
      and_node->left_node_ = left_node;
      and_node->right_node_ = right_node;
    }
  }
  return ret;
}

int ObPLOrNode::deep_copy(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode*& other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, CALC_OR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPartLocCalcNode* left_node = NULL;
    ObPartLocCalcNode* right_node = NULL;
    if (NULL != left_node_ && OB_FAIL(left_node_->deep_copy(allocator, calc_nodes, left_node))) {
      LOG_WARN("Failed to deep copy left node", K(ret));
    } else if (NULL != right_node_ && OB_FAIL(right_node_->deep_copy(allocator, calc_nodes, right_node))) {
      LOG_WARN("Failed todeep copy right node", K(ret));
    } else {
      ObPLOrNode* or_node = static_cast<ObPLOrNode*>(other);
      or_node->left_node_ = left_node;
      or_node->right_node_ = right_node;
    }
  }
  return ret;
}

int ObPLQueryRangeNode::deep_copy(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode*& other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, QUERY_RANGE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLQueryRangeNode* qr_node = static_cast<ObPLQueryRangeNode*>(other);
    if (OB_FAIL(qr_node->pre_query_range_.deep_copy(pre_query_range_))) {
      LOG_WARN("Failed to deep copy pre query range", K(ret));
    }
  }
  return ret;
}

int ObPLFuncValueNode::deep_copy(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode*& other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, FUNC_VALUE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLFuncValueNode* func_node = static_cast<ObPLFuncValueNode*>(other);
    func_node->res_type_ = res_type_;
    func_node->param_idx_ = param_idx_;
    if (OB_FAIL(deep_copy_obj(allocator, value_, func_node->value_))) {
      LOG_WARN("Failed to deep copy value", K(ret));
    } else {
      ObIArray<ParamValuePair>& param_values = func_node->param_value_;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < param_value_.count(); ++idx) {
        ObObj dst;
        const ParamValuePair& value_pair = param_value_.at(idx);
        if (OB_FAIL(deep_copy_obj(allocator, value_pair.obj_value_, dst))) {
          LOG_WARN("Failed to deep copy obj", K(ret));
        } else if (OB_FAIL(param_values.push_back(ParamValuePair(value_pair.param_idx_, dst)))) {
          LOG_WARN("Failed to add ParamValuePair", K(ret));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObPLColumnValueNode::deep_copy(
    ObIAllocator& allocator, ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode*& other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, COLUMN_VALUE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLColumnValueNode* column_node = static_cast<ObPLColumnValueNode*>(other);
    column_node->res_type_ = res_type_;
    column_node->param_idx_ = param_idx_;
    if (OB_FAIL(deep_copy_obj(allocator, value_, column_node->value_))) {
      LOG_WARN("Failed to deep copy obj", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::add_range_part_schema(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  void* ptr1 = NULL;
  void* ptr2 = NULL;
  use_range_part_opt_ = true;
  partition_num_ = table_schema.get_partition_num();
  ObPartition** part_array = table_schema.get_part_array();
  if (partition_num_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition number should not greater than 0", K(ret), K(partition_num_));
  } else {
    if (OB_ISNULL(ptr1 = allocator_.alloc(sizeof(ObObj) * partition_num_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(partition_num_));
    } else if (OB_ISNULL(ptr2 = allocator_.alloc(sizeof(int64_t) * partition_num_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(partition_num_));
    } else {
      range_obj_arr_ = new (ptr1) ObObj[partition_num_]();
      range_part_id_arr_ = static_cast<int64_t*>(ptr2);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
    ObPartition* part = part_array[i];
    if (OB_ISNULL(part)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("get invalid partiton array", K(ret), K(i), K(partition_num_));
    } else if (part->get_high_bound_val().get_obj_cnt() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("high bound val should have one obj", K(ret), K(part->get_high_bound_val().get_obj_cnt()));
    } else if (OB_FAIL(ob_write_obj(allocator_, part->get_high_bound_val().get_obj_ptr()[0], range_obj_arr_[i]))) {
      LOG_WARN("failed to write obj", K(ret), K(i));
    } else {
      range_part_id_arr_[i] = part->get_part_id();
    }
    LOG_TRACE("add range part",
        K(partition_num_),
        K(i),
        K(part->get_high_bound_val()),
        K(range_obj_arr_[i]),
        K(range_part_id_arr_[i]));
  }
  return ret;
}

int ObTableLocation::get_part_id_from_part_idx(int64_t part_idx, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObHashPartMapKey key;
  ObHashPartMapValue* value = NULL;
  key.part_idx_ = part_idx;
  part_id = part_idx;
  if (0 == hash_part_array_.count()) {
    part_id = part_idx;
  } else {
    ret = hash_part_map_.get_refactored(key, value);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get_refactored", K(ret));
    } else {
      part_id = value->part_id_;
    }
  }
  return ret;
}
int ObTableLocation::add_part_idx_to_part_id_map(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObPartition** part_array = table_schema.get_part_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
    if (OB_ISNULL(part_array[i])) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("get invalid partiton array", K(ret), K(i));
    } else {
      ObHashPartMapValue value;
      value.part_id_ = part_array[i]->get_part_id();
      if (-1 == part_array[i]->get_part_idx()) {
        value.key_.part_idx_ = i;
      } else {
        value.key_.part_idx_ = part_array[i]->get_part_idx();
      }
      if (value.part_id_ != value.key_.part_idx_) {
        if (OB_FAIL(hash_part_array_.push_back(value))) {
          LOG_WARN("fail to push back value", K(ret));
        }
        LOG_TRACE("add new part idx->part id item", K(ret), K(value.part_id_), K(value.key_.part_idx_));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hash_part_array_.count(); i++) {
    if (OB_FAIL(hash_part_map_.set_refactored(hash_part_array_.at(i).key_, &hash_part_array_.at(i)))) {
      LOG_WARN("fail to set value", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::add_hash_part_schema(const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  use_hash_part_opt_ = true;
  first_part_num_ = table_schema.get_first_part_num();
  if (OB_FAIL(add_part_idx_to_part_id_map(table_schema))) {
    LOG_WARN("failed to add part idx to part id map", K(ret));
  } else if (UNKNOWN_FAST_CALC == fast_calc_part_opt_) {
    fast_calc_part_opt_ = CAN_FAST_CALC;
    if (OB_FAIL(init_fast_calc_info())) {
      LOG_WARN("fail to init fast calc info, ignore error", K(ret));
      ret = OB_SUCCESS;
      fast_calc_part_opt_ = CANNOT_FAST_CALC;
    }
  }
  return ret;
}

int ObTableLocation::add_list_part_schema(const ObTableSchema& table_schema, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  use_list_part_map_ = true;
  ObPartition** part_array = table_schema.get_part_array();
  LOG_TRACE("begin add_list_part_schema", K(table_schema), K(ret));
  const common::ObPartitionKeyInfo& part_key_info = table_schema.get_partition_key_info();
  const ObRowkeyColumn* column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
    ObPartition* part = part_array[i];
    for (int64_t j = 0; OB_SUCC(ret) && j < part->get_list_row_values().count(); j++) {
      ObListPartMapValue value;
      value.part_id_ = part->get_part_id();
      const common::ObNewRow& tmp_row = part->get_list_row_values().at(j);
      if (lib::is_oracle_mode() && part_key_info.contain_timestamp_ltz_column()) {
        if (OB_FAIL(
                ob_write_row_with_cast_timestamp_ltz(allocator_, tmp_row, value.key_.row_, part_key_info, tz_info))) {
          LOG_WARN("fail to write row", K(ret));
        }
      } else {
        if (OB_FAIL(ob_write_row(allocator_, tmp_row, value.key_.row_))) {
          LOG_WARN("fail to write row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (value.key_.row_.get_count() == 1 && value.key_.row_.get_cell(0).is_max_value()) {
          list_default_part_id_ = value.part_id_;
        } else {
          if (OB_FAIL(list_part_array_.push_back(value))) {
            LOG_WARN("fail to push back value", K(ret));
          } else {
            LOG_TRACE("succ to push_back list_part_array", K(i), K(value), K(ret));
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < list_part_array_.count(); i++) {
    if (OB_FAIL(list_part_map_.set_refactored(list_part_array_.at(i).key_, &list_part_array_.at(i)))) {
      LOG_WARN("fail to set value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 == list_part_array_.count() && OB_FAIL(list_part_map_.init())) {
      LOG_WARN("fail to init list_part_map_", K(ret));
    } else if (UNKNOWN_FAST_CALC == fast_calc_part_opt_) {
      fast_calc_part_opt_ = CAN_FAST_CALC;
      if (OB_FAIL(init_fast_calc_info())) {
        LOG_WARN("fail to init fast calc info, ignore error", K(ret));
        ret = OB_SUCCESS;
        fast_calc_part_opt_ = CANNOT_FAST_CALC;
      }
    }
  }
  return ret;
}

int ObTableLocation::init_fast_calc_info()
{
  int ret = OB_SUCCESS;
  bool insert_or_replace = is_simple_insert_or_replace();
  if (insert_or_replace) {
    if (1 != key_exprs_.count() || OB_ISNULL(key_exprs_.at(0)) || 1 != key_exprs_.at(0)->get_expr_items().count() ||
        1 != key_conv_exprs_.count() || OB_ISNULL(key_conv_exprs_.at(0)) ||
        6 != key_conv_exprs_.at(0)->get_expr_items().count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN(
          "param is not expected as judge can do fast optimizaion", K(key_exprs_.count()), K(key_conv_exprs_.count()));
    } else {
      const ObPostExprItem& key_item = key_exprs_.at(0)->get_expr_items().at(0);
      const ObPostExprItem& key_cvt_item = key_conv_exprs_.at(0)->get_expr_items().at(5);
      if (T_QUESTIONMARK == key_item.get_item_type()) {
        const ObObj* obj = NULL;
        int64_t param_idx = -1;
        if (OB_FAIL(key_item.get_obj().get_unknown(param_idx))) {
          LOG_WARN("fail to get unknown", K(ret), K(key_item.get_obj()));
        } else if (OB_FAIL(part_expr_param_idxs_.push_back(param_idx))) {
          LOG_WARN("fail to init parat expr param idx", K(ret));
        }
      }
    }
  } else {
    const ObPLQueryRangeNode* calc_node2 = static_cast<const ObPLQueryRangeNode*>(calc_node_);
    if (OB_ISNULL(calc_node2) || OB_ISNULL(calc_node2->pre_query_range_.get_table_grapth().key_part_head_) ||
        OB_ISNULL(calc_node2->pre_query_range_.get_table_grapth().key_part_head_->normal_keypart_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret), K(calc_node2));
    } else {
      const ObQueryRange& query_range = calc_node2->pre_query_range_;
      ObObj result = query_range.get_table_grapth().key_part_head_->normal_keypart_->start_;
      if (result.is_unknown()) {
        int64_t param_idx = -1;
        if (OB_FAIL(result.get_unknown(param_idx))) {
          LOG_WARN("fail to get unknown", K(ret), K(result));
        } else if (OB_FAIL(part_expr_param_idxs_.push_back(param_idx))) {
          LOG_WARN("fail to init parat expr param idx", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::ob_write_row_with_cast_timestamp_ltz(common::ObIAllocator& allocator, const ObNewRow& src,
    ObNewRow& dst, const common::ObPartitionKeyInfo& part_key_info, const common::ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  void* ptr1 = NULL;
  void* ptr2 = NULL;
  if (src.count_ <= 0) {
    dst.count_ = src.count_;
    dst.cells_ = NULL;
    dst.projector_size_ = 0;
    dst.projector_ = NULL;
  } else if (OB_UNLIKELY(part_key_info.get_size() != src.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("part_key_info's size is not equal to ObNewRow",
        "size1",
        part_key_info.get_size(),
        "size2",
        src.count_,
        K(part_key_info),
        K(src),
        K(ret));
  } else if (OB_ISNULL(ptr1 = allocator.alloc(sizeof(ObObj) * src.count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("out of memory");
  } else if (NULL != src.projector_ && OB_ISNULL(ptr2 = allocator.alloc(sizeof(int32_t) * src.projector_size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("out of memory");
  } else {
    if (NULL != src.projector_) {
      MEMCPY(ptr2, src.projector_, sizeof(int32_t) * src.projector_size_);
    }
    ObObj* objs = new (ptr1) ObObj[src.count_]();
    const ObRowkeyColumn* tmp_column = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < src.count_; ++i) {
      const ObObj& cell = src.cells_[i];
      if (OB_ISNULL(tmp_column = part_key_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_column failed", K(part_key_info), K(i), K(ret));
      } else if (tmp_column->get_meta_type().is_timestamp_ltz() && cell.is_timestamp_tz()) {
        ObOTimestampData tmp_otd;
        if (OB_FAIL(ObTimeConverter::otimestamp_to_otimestamp(
                ObTimestampTZType, cell.get_otimestamp_value(), tz_info, ObTimestampLTZType, tmp_otd))) {
          LOG_WARN("failed to otimestamp_to_otimestamp", KPC(tz_info), K(cell), K(ret));
        } else {
          objs[i].set_otimestamp_value(ObTimestampLTZType, tmp_otd);
        }
      } else {
        if (OB_FAIL(ob_write_obj(allocator, src.cells_[i], objs[i]))) {
          _OB_LOG(WARN, "copy ObObj error, row=%s, i=%ld, ret=%d", to_cstring(src.cells_[i]), i, ret);
        }
      }
    }
    if (OB_SUCC(ret)) {
      dst.count_ = src.count_;
      dst.cells_ = objs;
      dst.projector_size_ = src.projector_size_;
      dst.projector_ = (NULL != src.projector_) ? static_cast<int32_t*>(ptr2) : NULL;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != ptr1) {
      allocator.free(ptr1);
      ptr1 = NULL;
    }
    if (NULL != ptr2) {
      allocator.free(ptr2);
      ptr2 = NULL;
    }
  }
  return ret;
}

int ObTableLocation::get_location_type(const common::ObAddr& server,
    const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list, ObTableLocationType& location_type)
{
  int ret = OB_SUCCESS;
  location_type = OB_TBL_LOCATION_UNINITIALIZED;
  if (0 == phy_part_loc_info_list.count()) {
    location_type = OB_TBL_LOCATION_LOCAL;
  } else if (1 == phy_part_loc_info_list.count()) {
    share::ObReplicaLocation replica_location;
    if (OB_FAIL(phy_part_loc_info_list.at(0).get_selected_replica(replica_location))) {
      LOG_WARN("fail to get selected replica", K(phy_part_loc_info_list.at(0)));
    } else if (!replica_location.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("replica location is invalid", K(ret), K(replica_location));
    } else {
      location_type = ((server == replica_location.server_) ? OB_TBL_LOCATION_LOCAL : OB_TBL_LOCATION_REMOTE);
    }
  } else {
    location_type = OB_TBL_LOCATION_DISTRIBUTED;
  }
  return ret;
}

int ObTableLocation::get_vt_partition_id(
    ObExecContext& exec_ctx, const uint64_t ref_table_id, ObIArray<int64_t>* partition_ids, int64_t* fake_id)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = exec_ctx.get_task_executor_ctx();
  share::ObIPartitionLocationCache* partition_location_cache = NULL;
  ObSEArray<ObPartitionLocation, 8> part_location_list;
  const int64_t expire_renew_time = 0;
  bool is_cache_hit = false;

  if (NULL == task_exec_ctx) {
    ret = OB_NOT_INIT;
    LOG_WARN("Task exec context should not be NULL", K(ret));
  } else if (!is_virtual_table(ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get virtual table partition, ref_table_id should be virtual table", K(ret));
  } else if (NULL == (partition_location_cache = task_exec_ctx->get_partition_location_cache())) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_location_cache not inited", K(ret));
  } else if (OB_FAIL(
                 partition_location_cache->get(ref_table_id, part_location_list, expire_renew_time, is_cache_hit))) {
    LOG_WARN("fail get virtual_table location", K(ref_table_id));
  } else {
    common::ObAddr self_addr = exec_ctx.get_addr();
    common::ObAddr addr;
    bool find_local = false;
    ObPartitionLocation partition_location;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_location_list.count(); i++) {
      int64_t partition_id = 0;
      if (OB_FAIL(part_location_list.at(i, partition_location))) {
        LOG_WARN("fail to get partition location", K(part_location_list), K(i));
      } else if (partition_location.get_replica_locations().count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("virtual table replication count must be 1", K(partition_location.get_partition_cnt()));
      } else {
        addr = partition_location.get_replica_locations().at(0).server_;
        if (OB_FAIL(task_exec_ctx->calc_virtual_partition_id(ref_table_id, addr, partition_id))) {
          LOG_WARN("Failed to calc virtual partition id", K(addr), K(ret));
        } else if (OB_INVALID_PARTITION_ID == partition_id) {
          // ignore invalid partition id
        } else if (NULL != partition_ids && OB_FAIL(partition_ids->push_back(partition_id))) {
          LOG_WARN("Push partition id to partition ids error", "partition id", partition_id, K(ret));
        } else if (NULL != fake_id && !find_local) {
          *fake_id = partition_id;
          if (addr == self_addr) {
            find_local = true;
            if (NULL == partition_ids) {
              break;
            }
          }
        } else {
          // do nothing
        }
      }
    }  // end of for
  }

  if (OB_SUCCESS == ret && NULL != partition_ids && 0 == partition_ids->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "No server servers the virtual table");
  }
  return ret;
}

ObTableLocation::ObTableLocation(const ObTableLocation& other)
    : inner_allocator_(ObModIds::OB_SQL_TABLE_LOCATION),
      allocator_(inner_allocator_),
      calc_node_(NULL),
      gen_col_node_(NULL),
      subcalc_node_(NULL),
      sub_gen_col_node_(NULL),
      sql_expression_factory_(allocator_),
      expr_op_factory_(allocator_),
      part_expr_(NULL),
      gen_col_expr_(NULL),
      subpart_expr_(NULL),
      sub_gen_col_expr_(NULL),
      first_partition_id_(-1),
      part_projector_(inner_allocator_, sql_expression_factory_, expr_op_factory_),
      range_obj_arr_(NULL),
      range_part_id_arr_(NULL)
{
  *this = other;
}

ObTableLocation& ObTableLocation::operator=(const ObTableLocation& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    inited_ = other.inited_;
    table_id_ = other.table_id_;
    ref_table_id_ = other.ref_table_id_;
    is_partitioned_ = other.is_partitioned_;
    part_level_ = other.part_level_;
    part_type_ = other.part_type_;
    subpart_type_ = other.subpart_type_;
    part_get_all_ = other.part_get_all_;
    subpart_get_all_ = other.subpart_get_all_;
    is_col_part_expr_ = other.is_col_part_expr_;
    is_col_subpart_expr_ = other.is_col_subpart_expr_;
    is_oracle_temp_table_ = other.is_oracle_temp_table_;
    tablet_size_ = other.tablet_size_;
    index_table_id_ = other.index_table_id_;
    part_col_type_ = other.part_col_type_;
    part_collation_type_ = other.part_collation_type_;
    subpart_col_type_ = other.subpart_col_type_;
    subpart_collation_type_ = other.subpart_collation_type_;
    is_in_hit_ = other.is_in_hit_;
    first_partition_id_ = other.first_partition_id_;
    is_global_index_ = other.is_global_index_;
    related_part_expr_idx_ = other.related_part_expr_idx_;
    related_subpart_expr_idx_ = other.related_subpart_expr_idx_;
    use_list_part_map_ = other.use_list_part_map_;
    list_default_part_id_ = other.list_default_part_id_;
    use_hash_part_opt_ = other.use_hash_part_opt_;
    use_range_part_opt_ = other.use_range_part_opt_;
    fast_calc_part_opt_ = other.fast_calc_part_opt_;
    duplicate_type_ = other.duplicate_type_;
    first_part_num_ = other.first_part_num_;
    partition_num_ = other.partition_num_;
    part_num_ = other.part_num_;
    direction_ = other.direction_;
    simple_insert_ = other.simple_insert_;
    stmt_type_ = other.stmt_type_;
    literal_stmt_type_ = other.literal_stmt_type_;
    hint_read_consistency_ = other.hint_read_consistency_;
    is_contain_inner_table_ = other.is_contain_inner_table_;
    is_contain_select_for_update_ = other.is_contain_select_for_update_;
    is_contain_mv_ = other.is_contain_mv_;
    use_calc_part_by_rowid_ = other.use_calc_part_by_rowid_;
    is_valid_range_columns_part_range_ = other.is_valid_range_columns_part_range_;
    is_valid_range_columns_subpart_range_ = other.is_valid_range_columns_subpart_range_;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.list_part_array_.count(); i++) {
      ObListPartMapValue value;
      value.part_id_ = other.list_part_array_.at(i).part_id_;
      if (OB_FAIL(ob_write_row(allocator_, other.list_part_array_.at(i).key_.row_, value.key_.row_))) {
        LOG_WARN("fail to write row", K(ret));
      } else if (OB_FAIL(list_part_array_.push_back(value))) {
        LOG_WARN("fail to push back value", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < list_part_array_.count(); i++) {
      if (OB_FAIL(list_part_map_.set_refactored(list_part_array_.at(i).key_, &list_part_array_.at(i)))) {
        LOG_WARN("fail to set value", K(ret));
      }
    }
    if (OB_SUCC(ret) && use_list_part_map_ && 0 == list_part_array_.count()) {
      if (OB_FAIL(list_part_map_.init())) {
        LOG_WARN("fail to init list_part_map_", K(ret));
      }
      LOG_TRACE("init list_part_map for single default value partition", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.hash_part_array_.count(); i++) {
      ObHashPartMapValue value;
      value.part_id_ = other.hash_part_array_.at(i).part_id_;
      if (FALSE_IT(value.key_.part_idx_ = other.hash_part_array_.at(i).key_.part_idx_)) {
      } else if (OB_FAIL(hash_part_array_.push_back(value))) {
        LOG_WARN("fail to push back value", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < hash_part_array_.count(); i++) {
      if (OB_FAIL(hash_part_map_.set_refactored(hash_part_array_.at(i).key_, &hash_part_array_.at(i)))) {
        LOG_WARN("fail to set value", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      part_expr_param_idxs_ = other.part_expr_param_idxs_;
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.range_obj_arr_)) {
      void* ptr1 = NULL;
      if (OB_ISNULL(ptr1 = allocator_.alloc(sizeof(ObObj) * partition_num_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(partition_num_));
      } else {
        range_obj_arr_ = new (ptr1) ObObj[partition_num_]();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
        if (OB_FAIL(ob_write_obj(allocator_, other.range_obj_arr_[i], range_obj_arr_[i]))) {
          LOG_WARN("failed to write obj", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.range_part_id_arr_)) {
      void* ptr1 = NULL;
      if (OB_ISNULL(ptr1 = allocator_.alloc(sizeof(int64_t) * partition_num_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(partition_num_));
      } else {
        range_part_id_arr_ = static_cast<int64_t*>(ptr1);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num_; i++) {
        range_part_id_arr_[i] = other.range_part_id_arr_[i];
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.calc_node_) {
        if (OB_FAIL(other.calc_node_->deep_copy(allocator_, calc_nodes_, calc_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.part_expr_) {
        if (ObSqlExpressionUtil::copy_sql_expression(sql_expression_factory_, other.part_expr_, part_expr_)) {
          LOG_WARN("Failed to copy part expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.gen_col_node_) {
        if (OB_FAIL(other.gen_col_node_->deep_copy(allocator_, calc_nodes_, gen_col_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.gen_col_expr_) {
        if (ObSqlExpressionUtil::copy_sql_expression(sql_expression_factory_, other.gen_col_expr_, gen_col_expr_)) {
          LOG_WARN("Failed to copy part expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.subpart_expr_) {
        if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expression(
                sql_expression_factory_, other.subpart_expr_, subpart_expr_))) {
          LOG_WARN("Failed to sub part expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.subcalc_node_) {
        if (OB_FAIL(other.subcalc_node_->deep_copy(allocator_, calc_nodes_, subcalc_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.sub_gen_col_node_) {
        if (OB_FAIL(other.sub_gen_col_node_->deep_copy(allocator_, calc_nodes_, sub_gen_col_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.sub_gen_col_expr_) {
        if (ObSqlExpressionUtil::copy_sql_expression(
                sql_expression_factory_, other.sub_gen_col_expr_, sub_gen_col_expr_)) {
          LOG_WARN("Failed to copy part expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && is_partitioned_) {
      if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expressions(sql_expression_factory_, other.key_exprs_, key_exprs_))) {
        LOG_WARN("Failed to copy key exprs", K(ret));
      } else if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expressions(
                     sql_expression_factory_, other.subkey_exprs_, subkey_exprs_))) {
        LOG_WARN("Failed to copy sub key exprs", K(ret));
      } else if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expressions(
                     sql_expression_factory_, other.key_conv_exprs_, key_conv_exprs_))) {
        LOG_WARN("Failed to copy key conv exprs", K(ret));
      } else if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expressions(
                     sql_expression_factory_, other.subkey_conv_exprs_, subkey_conv_exprs_))) {
        LOG_WARN("Failed to copy subkey conv exprs", K(ret));
      } else if (OB_FAIL(part_projector_.deep_copy(other.part_projector_))) {
        LOG_WARN("deep copy part projector failed", K(ret));
      } else {
        num_values_ = other.num_values_;
      }
    }
    if (OB_FAIL(part_hint_ids_.assign(other.part_hint_ids_))) {
      LOG_WARN("Failed to assign part hint ids", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return *this;
}

void ObTableLocation::reset()
{
  inited_ = false;
  table_id_ = OB_INVALID_ID;
  ref_table_id_ = OB_INVALID_ID;
  is_partitioned_ = true;
  part_level_ = PARTITION_LEVEL_ZERO;
  part_type_ = PARTITION_FUNC_TYPE_MAX;
  subpart_type_ = share::schema::PARTITION_FUNC_TYPE_MAX;
  part_get_all_ = false;
  subpart_get_all_ = false;
  is_col_part_expr_ = false;
  is_col_subpart_expr_ = false;
  is_oracle_temp_table_ = false;
  tablet_size_ = common::OB_DEFAULT_TABLET_SIZE;

  calc_node_ = NULL;
  gen_col_node_ = NULL;
  subcalc_node_ = NULL;
  sub_gen_col_node_ = NULL;
  for (int64_t idx = 0; idx < calc_nodes_.count(); ++idx) {
    if (NULL != calc_nodes_.at(idx)) {
      calc_nodes_.at(idx)->~ObPartLocCalcNode();
    }
  }
  calc_nodes_.reset();

  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
  part_expr_ = NULL;
  gen_col_expr_ = NULL;
  subpart_expr_ = NULL;
  sub_gen_col_expr_ = NULL;

  simple_insert_ = false;
  stmt_type_ = stmt::T_NONE;
  literal_stmt_type_ = stmt::T_NONE;
  hint_read_consistency_ = INVALID_CONSISTENCY;
  is_contain_inner_table_ = false;
  is_contain_select_for_update_ = false;
  is_contain_mv_ = false;

  key_exprs_.reset();
  subkey_exprs_.reset();
  num_values_ = 0;
  key_conv_exprs_.reset();
  subkey_conv_exprs_.reset();
  part_hint_ids_.reset();
  part_num_ = 1;
  direction_ = MAX_DIR;

  index_table_id_ = OB_INVALID_ID;
  inner_allocator_.reset();
  part_collation_type_ = CS_TYPE_INVALID;
  subpart_col_type_ = ObNullType;
  subpart_collation_type_ = CS_TYPE_INVALID;
  first_partition_id_ = -1;
  is_in_hit_ = false;
  part_projector_.reset();
  is_global_index_ = false;
  related_part_expr_idx_ = OB_INVALID_INDEX;
  related_subpart_expr_idx_ = OB_INVALID_INDEX;
  use_list_part_map_ = false;
  list_default_part_id_ = OB_INVALID_ID;
  use_hash_part_opt_ = false;
  use_range_part_opt_ = false;
  fast_calc_part_opt_ = NONE_FAST_CALC;
  duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
  first_part_num_ = 1;
  partition_num_ = 1;
  range_obj_arr_ = NULL;
  range_part_id_arr_ = NULL;
  list_part_map_.clear();
  list_part_array_.reset();
  hash_part_map_.clear();
  hash_part_array_.reset();
  part_expr_param_idxs_.reset();
  is_valid_range_columns_part_range_ = false;
  is_valid_range_columns_subpart_range_ = false;
}

int ObTableLocation::init_table_location(ObSqlSchemaGuard& schema_guard, uint64_t table_id, uint64_t ref_table_id,
    ObDMLStmt& stmt, RowDesc& row_desc, const bool is_dml_table, /*whether the ref_table is modified*/
    const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObRawExpr* part_raw_expr = NULL;

  table_id_ = table_id;
  ref_table_id_ = ref_table_id;
  stmt_type_ = stmt.get_stmt_type();
  literal_stmt_type_ = stmt.get_literal_stmt_type();
  hint_read_consistency_ = stmt.get_stmt_hint().get_query_hint().read_consistency_;
  is_partitioned_ = true;
  direction_ = direction;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt query ctx is null");
  } else {
    is_contain_inner_table_ = stmt.get_query_ctx()->is_contain_inner_table_;
    is_contain_select_for_update_ = stmt.get_query_ctx()->is_contain_select_for_update_;
    is_contain_mv_ = stmt.get_query_ctx()->is_contain_mv_;
    if (stmt.is_insert_stmt()) {
      set_simple_insert_or_replace();
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(schema_guard.get_table_schema(ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ref_table_id_));
  } else if (PARTITION_LEVEL_ZERO == (part_level_ = table_schema->get_part_level())) {
    is_partitioned_ = false;  // Non-partitioned table, do not need to calc partition id
  } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition level only support PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO", K(ret), K(part_level_));
  } else if (FALSE_IT(is_oracle_temp_table_ = table_schema->is_oracle_tmp_table())) {
  } else if (OB_UNLIKELY(
                 PARTITION_FUNC_TYPE_MAX <= (part_type_ = table_schema->get_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret), K(ref_table_id_), K(part_type_));
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
                         (subpart_type_ = table_schema->get_sub_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret), K(ref_table_id_), K(subpart_type_));
  } else if (0 >= (part_num_ = table_schema->get_part_option().get_part_num())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("partitioned virtual table's part num should > 0", K(ret), K(part_num_));
  } else {
    if (OB_ISNULL(part_raw_expr = get_related_part_expr(stmt, PARTITION_LEVEL_ONE, table_id, ref_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition expr is null", K(table_id), K(ref_table_id_));
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ &&
               OB_FAIL(can_get_part_by_range_for_range_columns(part_raw_expr, is_valid_range_columns_part_range_))) {
      LOG_WARN("failed ot check can get part by range for range columns", K(ret));
    } else if (FALSE_IT(is_col_part_expr_ = part_raw_expr->is_column_ref_expr())) {
      // never reach
    } else if (OB_FAIL(ObExprGeneratorImpl::gen_expression_with_row_desc(
                   sql_expression_factory_, expr_op_factory_, row_desc, part_raw_expr, part_expr_))) {
      LOG_WARN("gen expression with row desc failed", K(ret));
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type_) {
      if (OB_FAIL(part_projector_.init_part_projector(part_raw_expr, row_desc))) {
        LOG_WARN("init part projector failed", K(ret));
      }
    } else {
      LOG_TRACE("generated part expression", K(*part_raw_expr), K(*part_expr_));
    }
  }
  if (OB_SUCC(ret)) {
    is_global_index_ = table_schema->is_global_index_table();
    if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
      duplicate_type_ = is_dml_table ? ObDuplicateType::DUPLICATE_IN_DML : ObDuplicateType::DUPLICATE;
    } else {
      duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
    }
  }
  if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
    const ObRawExpr* subpart_raw_expr = NULL;
    if (OB_UNLIKELY(
            NULL == (subpart_raw_expr = get_related_part_expr(stmt, PARTITION_LEVEL_TWO, table_id_, ref_table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub partition expr not in stmt", K(ret), K(table_id_), K(ref_table_id_));
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ &&
               OB_FAIL(
                   can_get_part_by_range_for_range_columns(subpart_raw_expr, is_valid_range_columns_subpart_range_))) {
      LOG_WARN("failed to check can get part by range for range columns", K(ret));
    } else if (OB_FAIL(ObExprGeneratorImpl::gen_expression_with_row_desc(
                   sql_expression_factory_, expr_op_factory_, row_desc, subpart_raw_expr, subpart_expr_))) {
      LOG_WARN("gen expression with row desc failed", K(ret));
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ ||
               PARTITION_FUNC_TYPE_LIST_COLUMNS == subpart_type_) {
      if (OB_FAIL(part_projector_.init_subpart_projector(subpart_raw_expr, row_desc))) {
        LOG_WARN("init subpart projector failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_projector_.set_column_cnt(row_desc.get_column_num());
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter iter(*table_schema, check_dropped_schema);
    int64_t partition_id = -1;
    if (OB_FAIL(iter.next_partition_id_v2(partition_id))) {
      // OB_ITER_END is unexpected too
      LOG_WARN("iter failed", K(table_id), K(ret));
    } else if (-1 == partition_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(table_id), K(ret));
    } else {
      first_partition_id_ = partition_id;
      inited_ = true;
    }
  }
  return ret;
}

int ObTableLocation::init_table_location_with_rowkey(ObSqlSchemaGuard& schema_guard, uint64_t table_id,
    ObSQLSessionInfo& session_info, const bool is_dml_table /*= false*/)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_FAIL(schema_checker.get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(table_id));
  } else if (table_schema->get_part_level() != PARTITION_LEVEL_ZERO && table_schema->is_old_no_pk_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("init table location with rowkey by hidden primary key partition table not supported");
  } else {
    uint64_t real_table_id = table_schema->is_index_local_storage() ? table_schema->get_data_table_id() : table_id;
    ObResolverParams resolver_ctx;
    ObRawExprFactory expr_factory(allocator_);
    ObStmtFactory stmt_factory(allocator_);
    TableItem table_item;
    resolver_ctx.allocator_ = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = real_table_id;
    table_item.ref_id_ = real_table_id;
    table_item.type_ = TableItem::BASE_TABLE;
    RowDesc row_desc;
    ObDeleteResolver delete_resolver(resolver_ctx);
    ObDeleteStmt* delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
    const ObTableSchema* real_table_schema = table_schema;
    if (OB_ISNULL(resolver_ctx.query_ctx_) || OB_ISNULL(delete_stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create query_ctx or delete stmt failed", K_(resolver_ctx.query_ctx), K(delete_stmt));
    } else if (OB_FAIL(delete_stmt->get_table_items().push_back(&table_item))) {
      LOG_WARN("store table item failed", K(ret));
    } else if (OB_FAIL(delete_stmt->set_table_bit_index(real_table_id))) {
      LOG_WARN("set table bit index failed", K(ret), K(real_table_id));
    } else if (OB_UNLIKELY(table_schema->is_index_local_storage()) &&
               OB_FAIL(schema_guard.get_table_schema(real_table_id, real_table_schema))) {
      LOG_WARN("get real table schema failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(delete_resolver.resolve_table_partition_expr(table_item, *real_table_schema))) {
      LOG_WARN("resolve table partition expr failed", K(ret));
    } else if (OB_FAIL(generate_rowkey_desc(
                   *delete_stmt, table_schema->get_rowkey_info(), real_table_id, expr_factory, row_desc))) {
      LOG_WARN("generate rowkey desc failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(init_table_location(schema_guard,
                   real_table_id,
                   real_table_id,
                   *delete_stmt,
                   row_desc,
                   is_dml_table,
                   default_asc_direction()))) {
      LOG_WARN("init table location failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(clear_columnlized_in_row_desc(row_desc))) {
      LOG_WARN("Failed to clear columnlized in row desc", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::init(const ObTableSchema* table_schema, ObDMLStmt& stmt, ObSQLSessionInfo* session_info,
    const ObIArray<ObRawExpr*>& filter_exprs, const uint64_t table_id, const uint64_t ref_table_id,
    const ObPartHint* part_hint, const ObDataTypeCastParams& dtc_params,
    const bool is_dml_table, /*whether the ref_table is modified*/
    common::ObIArray<ObRawExpr*>* sort_exprs)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  ref_table_id_ = ref_table_id;
  stmt_type_ = stmt.get_stmt_type();
  literal_stmt_type_ = stmt.get_literal_stmt_type();
  hint_read_consistency_ = stmt.get_stmt_hint().get_query_hint().read_consistency_;
  is_contain_inner_table_ = stmt.get_query_ctx()->is_contain_inner_table_;
  is_contain_select_for_update_ = stmt.get_query_ctx()->is_contain_select_for_update_;
  is_contain_mv_ = stmt.get_query_ctx()->is_contain_mv_;
  is_partitioned_ = true;
  // direction_ = direction;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("table location init twice", K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == ref_table_id || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(table_id), K(ref_table_id), K(ret));
  } else if (PARTITION_LEVEL_ZERO == (part_level_ = table_schema->get_part_level())) {
    is_partitioned_ = false;  // Non-partitioned table, do not need to calc partition id
  } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition level only support PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO", K(ret), K(part_level_));
  } else if (FALSE_IT(is_oracle_temp_table_ = table_schema->is_oracle_tmp_table())) {
  } else if (OB_UNLIKELY(
                 PARTITION_FUNC_TYPE_MAX <= (part_type_ = table_schema->get_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret), K(ref_table_id_), K(part_type_));
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
                         (subpart_type_ = table_schema->get_sub_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret), K(ref_table_id_), K(subpart_type_));
  } else if (0 >= (part_num_ = table_schema->get_part_option().get_part_num())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("partitioned virtual table's part num should > 0", K(ret), K(part_num_));
  } else if (stmt.is_insert_stmt() && !static_cast<ObInsertStmt&>(stmt).value_from_select()) {
    set_simple_insert_or_replace();
    if (OB_FAIL(record_insert_partition_info(stmt, table_schema, session_info))) {
      LOG_WARN("Fail to record insert stmt partition info", K(stmt_type_), K(ret));
    }
  } else if (0 >= (tablet_size_ = table_schema->get_tablet_size())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table's tablet_size should > 0", K(ret), K(tablet_size_));
  } else {
    is_in_hit_ = false;
    if (OB_FAIL(record_in_dml_partition_info(stmt, filter_exprs, is_in_hit_, table_schema))) {  // for in filter
      LOG_WARN("fail to record_in_dml_partition_info", K(ret));
    } else if (!is_in_hit_) {
      if (OB_FAIL(record_not_insert_dml_partition_info(stmt, table_schema, filter_exprs, dtc_params))) {
        LOG_WARN("Fail to record select or update partition info", K(stmt_type_), K(ret));
      } else if (OB_FAIL(get_not_insert_dml_part_sort_expr(stmt, sort_exprs))) {
        LOG_WARN("Failed to get not insert dml sort key with parts", K(ret));
      } else {
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_global_index_ = table_schema->is_global_index_table();
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter iter(*table_schema, check_dropped_schema);
    int64_t partition_id = -1;
    if (OB_FAIL(iter.next_partition_id_v2(partition_id))) {
      // OB_ITER_END is unexpected too
      LOG_WARN("iter failed", K(table_id), K(ref_table_id), K(ret));
    } else if (-1 == partition_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(table_id), K(ref_table_id), K(ret));
    } else {
      first_partition_id_ = partition_id;
    }
  }
  if (OB_SUCC(ret) && PARTITION_LEVEL_ONE == part_level_) {
    if (table_schema->is_list_part()) {
      if (OB_FAIL(add_list_part_schema(*table_schema, dtc_params.tz_info_))) {
        LOG_WARN("fail to add list part schema", K(ret));
      }
    } else if (share::is_oracle_mode() && 1 == table_schema->get_partition_key_column_num() &&
               table_schema->is_user_table()) {
      if (table_schema->is_hash_part()) {
        if (OB_FAIL(add_hash_part_schema(*table_schema))) {
          LOG_WARN("fail to add list part schema", K(ret));
        }
      } else if (table_schema->is_range_part()) {
        if (OB_FAIL(add_range_part_schema(*table_schema))) {
          LOG_WARN("fail to add list part schema", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
      duplicate_type_ = is_dml_table ? ObDuplicateType::DUPLICATE_IN_DML : ObDuplicateType::DUPLICATE;
    } else {
      duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
    }
  }
  if (OB_SUCC(ret) && NULL != part_hint) {
    ret = part_hint_ids_.assign(part_hint->part_ids_);
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ObTableLocation::get_is_weak_read(ObExecContext& exec_ctx, bool& is_weak_read) const
{
  int ret = OB_SUCCESS;
  is_weak_read = false;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  ObTaskExecutorCtx* task_exec_ctx = exec_ctx.get_task_executor_ctx();
  if (OB_ISNULL(session) || OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpeted null", K(ret), K(session), K(task_exec_ctx));
  } else {
    ObConsistencyLevel consistency_level = INVALID_CONSISTENCY;
    int32_t trans_consistency_level = ObTransConsistencyLevel::UNKNOWN;
    int32_t trans_consistency_type = ObTransConsistencyType::UNKNOWN;
    int32_t read_snapshot_type = ObTransReadSnapshotType::UNKNOWN;
    const bool need_consistent_snapshot = true;
    if (stmt::T_SELECT == stmt_type_) {
      if (OB_UNLIKELY(INVALID_CONSISTENCY != hint_read_consistency_)) {
        consistency_level = hint_read_consistency_;
      } else {
        consistency_level = session->get_consistency_level();
      }
    } else {
      consistency_level = STRONG;
    }
    trans_consistency_level = consistency_level;
    if (OB_FAIL(ObSqlTransControl::decide_trans_read_interface_specs("ObTableLocation::get_is_weak_read",
            *session,
            stmt_type_,
            literal_stmt_type_,
            is_contain_select_for_update_,
            is_contain_inner_table_,
            consistency_level,
            need_consistent_snapshot,
            trans_consistency_level,
            trans_consistency_type,
            read_snapshot_type))) {
      LOG_WARN("fail to decide trans read interface specs",
          K(ret),
          K(stmt_type_),
          K(literal_stmt_type_),
          K(is_contain_select_for_update_),
          K(is_contain_inner_table_),
          K(consistency_level),
          KPC(session));
    } else {
      // check read follower
      is_weak_read = (ObTransConsistencyType::BOUNDED_STALENESS_READ == trans_consistency_type);
    }
  }
  return ret;
}

int ObTableLocation::calculate_partition_location_infos(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, ObIPartitionLocationCache& location_cache,
    ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list, const ObDataTypeCastParams& dtc_params,
    bool nonblock /*false*/) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 128> partition_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (OB_FAIL(calculate_partition_ids(exec_ctx, part_mgr, params, partition_ids, dtc_params))) {
    LOG_WARN("Failed to calculate partition ids", K(ret));
  } else if (OB_FAIL(set_partition_locations(
                 exec_ctx, location_cache, ref_table_id_, partition_ids, phy_part_loc_info_list, nonblock))) {
    LOG_WARN("Failed to set partition locations", K(ret), K(partition_ids));
  } else {
  }  // do nothing
  return ret;
}

int ObTableLocation::calculate_partition_ids_by_rowkey(ObSQLSessionInfo& session_info,
    ObSchemaGetterGuard& schema_guard, uint64_t table_id, const ObIArray<ObRowkey>& rowkeys,
    ObIArray<int64_t>& part_ids, ObIArray<RowkeyArray>& rowkey_lists)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObExecContext, exec_ctx)
  {
    ObSqlSchemaGuard sql_schema_guard;
    sql_schema_guard.set_schema_guard(&schema_guard);
    exec_ctx.set_my_session(&session_info);
    if (OB_UNLIKELY(is_virtual_table(table_id))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Calculate virtual table partition id with rowkey");
    } else if (OB_UNLIKELY(rowkeys.count() <= 0)) {
      if (PARTITION_LEVEL_ONE == part_level_) {
        if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, part_ids))) {
          LOG_WARN("Failed to get all part ids", K(ret));
        }
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        ObSEArray<int64_t, 5> tmp_part_ids;
        if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, tmp_part_ids))) {
          LOG_WARN("Failed to get all part ids", K(ret));
        } else if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, part_ids, &tmp_part_ids))) {
          LOG_WARN("Failed to get all subpart ids", K(ret));
        } else {
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unkown part level", K(ret), K(part_level_), K(is_partitioned_));
      }
    } else if (OB_FAIL(init_table_location_with_rowkey(sql_schema_guard, table_id, session_info))) {
      LOG_WARN("implicit init location failed", K(table_id), K(ret));
    } else if (!is_partitioned_) {
      RowkeyArray rowkey_list;
      if (OB_FAIL(part_ids.push_back(0))) {
        LOG_WARN("Failed to push back partition id", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
        if (OB_FAIL(rowkey_list.push_back(i))) {
          LOG_WARN("add rowkey index to rowkey list failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_lists.push_back(rowkey_list))) {
          LOG_WARN("store rowkey list to rowkey_lists failed", K(ret));
        }
      }
    } else if (OB_FAIL(calc_partition_ids_by_rowkey(exec_ctx, &schema_guard, rowkeys, part_ids, rowkey_lists))) {
      LOG_WARN("calc parttion ids by rowkey failed", K(ret));
    }
  }

  return ret;
}

int ObTableLocation::calculate_partition_id_by_row(
    ObExecContext& exec_ctx, ObPartMgr* part_mgr, const ObNewRow& row, int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> part_ids;
  int64_t part_idx = OB_INVALID_INDEX;
  if (OB_FAIL(calculate_partition_ids_by_row(exec_ctx, part_mgr, row, part_ids, part_idx))) {
    LOG_WARN("calculate partition ids by row failed", K(ret));
  } else if (OB_UNLIKELY(part_ids.count() != 1) && OB_UNLIKELY(part_idx != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part ids is invalid", K(part_ids), K(part_idx));
  } else {
    part_id = part_ids.at(0);
  }
  return ret;
}

int ObTableLocation::calculate_partition_ids_by_row(ObExecContext& exec_ctx, ObPartMgr* part_mgr, const ObNewRow& row,
    ObIArray<int64_t>& part_ids, int64_t& part_idx) const
{
  int ret = OB_SUCCESS;
  ObNewRow* part_row = NULL;
  part_idx = OB_INVALID_INDEX;
  if (!is_partitioned_) {
    int64_t part_id = 0;
    if (OB_FAIL(add_var_to_array_no_dup(part_ids, part_id, &part_idx))) {
      LOG_WARN("Failed to push back partition id", K(ret));
    }
  } else if (OB_FAIL(part_projector_.calc_part_row(stmt_type_, exec_ctx, row, part_row))) {
    ret = COVER_SUCC(OB_ERR_UNEXPECTED);
    LOG_WARN("calc part row failed", K(ret), K(part_row));
  } else if (PARTITION_LEVEL_ONE == part_level_) {
    if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, part_ids))) {
      LOG_WARN("calc partition id by row failed", K(ret));
    } else {
      part_idx = exec_ctx.get_part_row_manager().get_part_idx();
    }
  } else {
    ObSEArray<int64_t, 1> tmp_part_ids;
    if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, tmp_part_ids))) {
      LOG_WARN("calc partition id by row failed", K(ret));
    } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, part_ids, &tmp_part_ids))) {
      LOG_WARN("calc sub partition id by row failed", K(ret));
    } else {
      part_idx = exec_ctx.get_part_row_manager().get_part_idx();
    }
  }
  return ret;
}

int ObTableLocation::calculate_partition_ids_fast_for_non_insert(
    ObExecContext& exec_ctx, const ParamStore& param_store, ObObj& result, bool& has_optted) const
{
  int ret = OB_SUCCESS;
  const ObPLQueryRangeNode* calc_node2 = static_cast<const ObPLQueryRangeNode*>(calc_node_);
  has_optted = false;
  if (OB_ISNULL(calc_node2) || OB_ISNULL(calc_node2->pre_query_range_.get_table_grapth().key_part_head_) ||
      OB_ISNULL(calc_node2->pre_query_range_.get_table_grapth().key_part_head_->normal_keypart_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), K(calc_node2));
  } else {
    const ObQueryRange& query_range = calc_node2->pre_query_range_;
    result = query_range.get_table_grapth().key_part_head_->normal_keypart_->start_;
    if (result.is_unknown()) {
      if (OB_FAIL(query_range.get_param_value(result, param_store))) {
        LOG_WARN("get param value failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing ...
    } else {
      const ObObjType& type1 = result.get_type();
      const ObObjType& type2 = query_range.get_table_grapth().key_part_head_->pos_.column_type_.get_type();
      if (result.meta_.get_collation_type() !=
          query_range.get_table_grapth().key_part_head_->pos_.column_type_.get_collation_type()) {
        // no optimize for diffent collation
        LOG_TRACE("collation not the same, won't optimize",
            K(query_range.get_table_grapth().key_part_head_->normal_keypart_->start_.meta_.get_collation_type()),
            K(query_range.get_table_grapth().key_part_head_->pos_.column_type_.get_collation_type()));
      } else {
        if (type1 != type2 && ob_is_string_tc(type1) == ob_is_string_tc(type2)) {
          ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
          allocator.set_label("CalcNoInsert");
          ObExprCtx expr_ctx;
          const ObObj* cast_result = NULL;
          // init expr ctx
          if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
            LOG_WARN("Failed to wrap expr ctx", K(ret));
          } else {
            // EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
            ObCollationType cast_coll_type =
                query_range.get_table_grapth().key_part_head_->pos_.column_type_.get_collation_type();
            ObCastMode cast_mode = (expr_ctx).cast_mode_ | CM_NONE;
            const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);
            ObCastCtx cast_ctx(
                (expr_ctx).calc_buf_, &dtc_params, get_cur_time((expr_ctx).phy_plan_ctx_), cast_mode, cast_coll_type);
            EXPR_CAST_OBJ_V2(type2, result, cast_result);
            LOG_TRACE("cast happened when calc part id fast for non-insert", K(type2), K(result), K(*cast_result));
            if (OB_SUCC(ret)) {
              if (OB_FAIL(deep_copy_obj(exec_ctx.get_allocator(), *cast_result, result))) {
                LOG_WARN("Failed to deep copy obj", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && !result.is_null() && result.is_valid_type()) {
          has_optted = true;
        }
      }
    }
    LOG_TRACE("calc part id fast for non-insert",
        K(ret),
        K(param_store),
        K(result),
        K(has_optted),
        K(use_hash_part_opt_),
        K(use_list_part_map_),
        K(query_range));
  }
  return ret;
}

// for insert, merge
int ObTableLocation::calculate_partition_ids_fast_for_insert(
    ObExecContext& exec_ctx, const ParamStore& param_store, ObObj& result, bool& has_optted) const
{
  int ret = OB_SUCCESS;
  has_optted = false;
  if (1 != key_exprs_.count() || OB_ISNULL(key_exprs_.at(0)) || 1 != key_exprs_.at(0)->get_expr_items().count() ||
      1 != key_conv_exprs_.count() || OB_ISNULL(key_conv_exprs_.at(0)) ||
      6 != key_conv_exprs_.at(0)->get_expr_items().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "param is not expected as judge can do fast optimizaion", K(key_exprs_.count()), K(key_conv_exprs_.count()));
  } else {
    const ObPostExprItem& key_item = key_exprs_.at(0)->get_expr_items().at(0);
    const ObPostExprItem& key_cvt_item = key_conv_exprs_.at(0)->get_expr_items().at(5);
    if (T_QUESTIONMARK == key_item.get_item_type()) {
      const ObObj* obj = NULL;
      int64_t param_idx = -1;
      if (OB_FAIL(key_item.get_obj().get_unknown(param_idx))) {
        LOG_WARN("fail to get unknown", K(ret), K(key_item.get_obj()));
      } else if (OB_UNLIKELY(param_idx < 0 || param_idx >= param_store.count())) {
        ret = common::OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("wrong index of question mark position", K(param_idx), "param_count", param_store.count());
      } else {
        obj = &param_store.at(param_idx);
        if (OB_ISNULL(obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL obj returned", K(ret));
        } else {
          result = *obj;
        }
      }
    } else if (IS_DATATYPE_OP(key_item.get_item_type())) {
      result = key_item.get_obj();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid key item type", K(key_item), K(ret));
    }
    if (OB_SUCC(ret)) {
      // need cast when needed
      if (T_FUN_COLUMN_CONV == key_cvt_item.get_item_type() && OB_NOT_NULL(key_cvt_item.get_expr_operator())) {
        const ObObjType& type1 = result.get_type();
        const ObObjType& type2 = key_cvt_item.get_expr_operator()->get_result_type().get_type();
        if (result.meta_.get_collation_type() !=
            key_cvt_item.get_expr_operator()->get_result_type().get_collation_type()) {
          // no optimize for diffent collation
          LOG_TRACE("collation not the same, won't optimize",
              K(result.meta_.get_collation_type()),
              K(key_cvt_item.get_expr_operator()->get_result_type().get_collation_type()));
        } else {
          if (type1 != type2 && ob_is_string_tc(type1) == ob_is_string_tc(type2)) {
            ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
            allocator.set_label("CalcInsert");
            ObExprCtx expr_ctx;
            const ObObj* cast_result = NULL;
            // init expr ctx
            if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
              LOG_WARN("Failed to wrap expr ctx", K(ret));
            } else {
              ObCollationType cast_coll_type = key_cvt_item.get_expr_operator()->get_result_type().get_collation_type();
              ObCastMode cast_mode = (expr_ctx).cast_mode_ | CM_NONE;
              const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);
              ObCastCtx cast_ctx(
                  (expr_ctx).calc_buf_, &dtc_params, get_cur_time((expr_ctx).phy_plan_ctx_), cast_mode, cast_coll_type);
              EXPR_CAST_OBJ_V2(type2, result, cast_result);
              LOG_TRACE("cast happened when calc part id fast for insert", K(type2), K(result), K(*cast_result));
              if (OB_SUCC(ret)) {
                if (OB_FAIL(deep_copy_obj(exec_ctx.get_allocator(), *cast_result, result))) {
                  LOG_WARN("Failed to deep copy obj", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && result.get_type() == type2) {
            has_optted = true;
          }
        }
      }
    }
    LOG_TRACE("calc part id fast for insert",
        K(ret),
        K(param_store),
        K(result),
        K(has_optted),
        K(use_hash_part_opt_),
        K(use_list_part_map_),
        K(key_item),
        K(key_cvt_item));
  }
  return ret;
}

bool ObTableLocation::calculate_partition_ids_fast(
    ObExecContext& exec_ctx, const ParamStore& param_store, ObIArray<int64_t>& partition_ids) const
{
  int ret = OB_SUCCESS;
  bool has_optted = false;
  int64_t part_idx = OB_INVALID_INDEX;
  ObObj result;
  bool insert_or_replace = is_simple_insert_or_replace();
  if (insert_or_replace) {
    if (OB_FAIL(calculate_partition_ids_fast_for_insert(exec_ctx, param_store, result, has_optted))) {
      LOG_WARN("failed to calc part id fast for ins, error will be ignored", K(ret));
    }
  } else {
    if (OB_FAIL(calculate_partition_ids_fast_for_non_insert(exec_ctx, param_store, result, has_optted))) {
      LOG_WARN("failed to calc part id fast for non-ins, error will be ignored", K(ret));
    }
  }
  if (OB_SUCC(ret) && has_optted) {
    if (use_hash_part_opt_) {
      ObObj hash_result;
      if (PARTITION_FUNC_TYPE_HASH == part_type_ &&
          OB_FAIL(ObExprFuncPartOldHash::calc_value_for_oracle(&result, 1, hash_result))) {
        LOG_WARN("Failed to calc hash value oracle mode", K(ret));
      } else if (PARTITION_FUNC_TYPE_HASH_V2 == part_type_ &&
                 OB_FAIL(ObExprFuncPartHash::calc_value_for_oracle(&result, 1, hash_result))) {
        LOG_WARN("Failed to calc hash value oracle mode", K(ret));
      } else if (OB_FAIL(get_hash_part(hash_result, insert_or_replace, partition_ids, &part_idx))) {
        LOG_WARN("fail to get hash part", K(ret));
      } else {
        exec_ctx.get_part_row_manager().set_part_idx(part_idx);
      }
    } else if (use_list_part_map_) {
      if (OB_FAIL(get_list_part(result, insert_or_replace, partition_ids, &part_idx))) {
        LOG_WARN("fail to get list part", K(ret));
      } else {
        exec_ctx.get_part_row_manager().set_part_idx(part_idx);
      }
    }
  }
  if (OB_FAIL(ret)) {
    has_optted = false;
  }
  LOG_TRACE("calc part id fast opt",
      K(has_optted),
      K(ret),
      K(param_store),
      K(part_idx),
      K(partition_ids),
      K(result),
      K(insert_or_replace));
  return has_optted;
}

int ObTableLocation::calculate_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, ObIArray<int64_t>& partition_ids, const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (use_calc_part_by_rowid_) {
    ObSchemaGetterGuard* schema_guard = dynamic_cast<ObSchemaGetterGuard*>(part_mgr);
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null schema guard", K(ret));
    } else if (OB_FAIL(calculate_partition_ids_with_rowid(exec_ctx, *schema_guard, params, partition_ids))) {
      LOG_WARN("failed to calculate partition ids", K(ret));
    }
  } else {
    if (CAN_FAST_CALC == fast_calc_part_opt_ && calculate_partition_ids_fast(exec_ctx, params, partition_ids)) {
      // do nothing ...
    } else if (is_simple_insert_or_replace()) {
      if (part_get_all_) {
        if (PARTITION_LEVEL_ONE == part_level_) {
          if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids))) {
            LOG_WARN("Failed to get all part ids", K(ret));
          }
        } else if (PARTITION_LEVEL_TWO == part_level_) {
          ObSEArray<int64_t, 4> tmp_part_ids;
          if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, tmp_part_ids))) {
            LOG_WARN("Failed to get all part ids", K(ret));
          } else if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids, &tmp_part_ids))) {
            LOG_WARN("Failed to get all subpart ids", K(ret));
          } else {
          }
        }
      } else if (OB_FAIL(calc_partition_ids_by_stored_expr(exec_ctx, part_mgr, partition_ids, dtc_params))) {
        LOG_WARN("Calc partition ids by stored expr error", K_(stmt_type), K(ret));
      }
    } else if (is_in_hit_) {
      if (OB_FAIL(calc_partition_ids_by_in_expr(exec_ctx, part_mgr, partition_ids, dtc_params))) {
        LOG_WARN("fail to calc_partition_ids_by_in_expr", K(ret));
      }
    } else {
      if (!is_partitioned_) {
        if (OB_FAIL(partition_ids.push_back(0))) {
          LOG_WARN("Failed to push back partition id", K(ret));
        }
      } else if (PARTITION_LEVEL_ONE == part_level_) {
        if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx,
                part_mgr,
                params,
                calc_node_,
                gen_col_node_,
                gen_col_expr_,
                part_get_all_,
                partition_ids,
                dtc_params))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        }
      } else {
        ObSEArray<int64_t, 5> part_ids;
        if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx,
                part_mgr,
                params,
                calc_node_,
                gen_col_node_,
                gen_col_expr_,
                part_get_all_,
                part_ids,
                dtc_params))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx,
                       part_mgr,
                       params,
                       subcalc_node_,
                       sub_gen_col_node_,
                       sub_gen_col_expr_,
                       subpart_get_all_,
                       partition_ids,
                       dtc_params,
                       &part_ids))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        } else {
        }
      }
    }

    NG_TRACE(tl_calc_by_range_end);
    // deal partition hint
    if (OB_SUCCESS == ret && part_hint_ids_.count() > 0) {
      if (OB_FAIL(deal_partition_selection(partition_ids))) {
        LOG_WARN("deal partition select failed", K(ret));
      }
    }

    // As in transaction and engine, partition_ids' count should not be 0,
    // fill fake id.
    if (OB_SUCCESS == ret && 0 == partition_ids.count()) {
      int64_t fake_id = first_partition_id_;
      if (part_hint_ids_.count() > 0) {
        fake_id = part_hint_ids_.at(0);
      }
      if (is_partitioned_ && is_virtual_table(ref_table_id_)) {
        if (OB_FAIL(get_vt_partition_id(exec_ctx, ref_table_id_, NULL, &fake_id))) {
          LOG_WARN("Get virtual table fake id error", K(ret));
        }
      }
      if (OB_FAIL(partition_ids.push_back(fake_id))) {
        LOG_WARN("Add fake partition id error", K(ret));
      }
    }
  }

  NG_TRACE(tl_calc_part_id_end);
  return ret;
}

int ObTableLocation::set_partition_locations(ObExecContext& exec_ctx, ObIPartitionLocationCache& location_cache,
    const uint64_t ref_table_id, const ObIArray<int64_t>& partition_ids,
    ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list, bool nonblock /*false*/)
{
  int ret = OB_SUCCESS;
  int fail_ret = OB_SUCCESS;
  const int64_t expire_renew_time = 0;
  bool is_cache_hit = false;
  if (OB_INVALID_ID == ref_table_id || partition_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ref_table_id), K(partition_ids.empty()));
  } else {
    phy_part_loc_info_list.reset();
    int64_t N = partition_ids.count();
    NG_TRACE(get_location_cache_begin);
    ObSQLSessionInfo* session = exec_ctx.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("some param is NULL", K(ret), K(session));
    } else if (OB_FAIL(phy_part_loc_info_list.prepare_allocate(N))) {
      LOG_WARN("Partitoin location list prepare error", K(ret));
    } else {
      ObPartitionLocation location;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        location.reset();
        ObPhyPartitionLocationInfo& part_loc_info = phy_part_loc_info_list.at(i);
        if (is_link_table_id(ref_table_id)) {
          ret = location_cache.get_link_table_location(ref_table_id, location);
        } else if (nonblock) {
          ret = location_cache.nonblock_get(ref_table_id, partition_ids.at(i), location);
        } else {
          ret = location_cache.get(ref_table_id, partition_ids.at(i), location, expire_renew_time, is_cache_hit);
        }
        if (OB_FAIL(ret)) {
          location.set_table_id(ref_table_id);
          location.set_partition_id(partition_ids.at(i));
          const static int64_t ANY_PARTITION_COUNT = 12345;
          location.set_partition_cnt(ANY_PARTITION_COUNT);
          if (OB_UNLIKELY(OB_SUCCESS != (fail_ret = part_loc_info.set_part_loc_with_only_readable_replica(
                                             location, session->get_retry_info().get_invalid_servers())))) {
            LOG_WARN("fail to set partition location",
                K(ret),
                K(fail_ret),
                K(location),
                K(session->get_retry_info().get_invalid_servers()));
          }
          LOG_WARN("Get partition error, then set partition key for location cache renew later",
              K(ret),
              K(ref_table_id),
              "partition_id",
              partition_ids.at(i),
              K(part_loc_info));
        } else {
          if (GCTX.is_standby_cluster() && !ObMultiClusterUtil::is_cluster_private_table(location.get_table_id())) {
            // do nothing
            // no leader for non private tables of standby cluster
          } else if (OB_FAIL(location.check_strong_leader_exist())) {
            if (OB_LOCATION_LEADER_NOT_EXIST == ret) {
              if (nonblock) {
                ret = location_cache.nonblock_get(ref_table_id, partition_ids.at(i), location);
              } else {
                ret = location_cache.get(
                    ref_table_id, partition_ids.at(i), location, location.get_renew_time(), is_cache_hit);
              }
              if (OB_FAIL(ret)) {
                LOG_WARN("Get partition with force renew error",
                    K(ret),
                    K(ref_table_id),
                    "partition_id",
                    partition_ids.at(i));
              }
            } else {
              LOG_WARN("location check leader exist failed", K(ret), K(location));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(part_loc_info.set_part_loc_with_only_readable_replica(
                                  location, session->get_retry_info().get_invalid_servers()))) {
            LOG_WARN("fail to set partition location with only readable replica",
                K(ret),
                K(location),
                K(session->get_retry_info().get_invalid_servers()));
          }
        }
      }
      NG_TRACE(get_location_cache_end);
    }
  }
  return ret;
}

int ObTableLocation::get_part_col_type(
    const ObRawExpr* expr, ObObjType& col_type, ObCollationType& collation_type, const ObTableSchema* table_schema)
{
  int ret = OB_SUCCESS;
  if (NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
  } else if (T_REF_COLUMN != expr->get_expr_type()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObColumnRefRawExpr* col = static_cast<const ObColumnRefRawExpr*>(expr);
    const ObColumnSchemaV2* col_schema = table_schema->get_column_schema(col->get_column_id());
    if (NULL == col_schema) {
      ret = OB_SCHEMA_ERROR;
    } else {
      col_type = col_schema->get_data_type();
      collation_type = col_schema->get_meta_type().get_collation_type();
    }
  }
  return ret;
}

int ObTableLocation::convert_row_obj_type(const ObNewRow& from, ObNewRow& to, ObObjType col_type,
    ObCollationType collation_type, const ObDataTypeCastParams& dtc_params, bool& is_all, bool& is_none) const
{
  int ret = OB_SUCCESS;
  const ObObj& tmp = from.get_cell(0);
  ObObj& out_obj = to.get_cell(0);
  ObObj tmp_out_obj;
  ObObj tmp_out_obj2;
  ObObjType res_type = ObNullType;
  is_none = false;
  if (ob_obj_type_class(tmp.get_type()) == ob_obj_type_class(col_type) && tmp.get_collation_type() == collation_type) {
    is_all = false;
    out_obj = tmp;
  } else {
    if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(res_type, col_type, tmp.get_type()))) {
      LOG_WARN("fail to get_relational_cmp_type", K(ret));
    } else {
      bool is_cast_monotonic = false;
      if (OB_FAIL(ObObjCaster::is_cast_monotonic(col_type, res_type, is_cast_monotonic))) {
        LOG_WARN("check is cast monotonic failed", K(ret));
      } else if (is_cast_monotonic) {
        if (OB_FAIL(ObObjCaster::is_cast_monotonic(res_type, col_type, is_cast_monotonic))) {
          LOG_WARN("check is cast monotonic failed", K(ret));
        }
      }

      int64_t result = 0;
      if (OB_SUCC(ret)) {
        if (is_cast_monotonic) {
          is_all = false;
          ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_NONE, collation_type);
          if (OB_FAIL(ObObjCaster::to_type(res_type, cast_ctx, tmp, tmp_out_obj))) {
            LOG_WARN("fail to cast", K(ret), K(res_type));
          } else if (OB_FAIL(ObObjCaster::to_type(col_type, cast_ctx, tmp_out_obj, tmp_out_obj2))) {
            LOG_WARN("fail to cast", K(ret), K(col_type));
          } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(
                         result, tmp_out_obj, tmp_out_obj2, cast_ctx, res_type, collation_type))) {
            LOG_WARN("fail to cmp", K(ret));
          }

          if (OB_SUCC(ret)) {
            if (result == 0) {
              out_obj = tmp_out_obj2;
            } else {
              is_none = true;
            }
          } else {
            ret = OB_SUCCESS;
            is_all = true;
          }
        } else {
          is_all = true;
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_in_expr(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    ObIArray<int64_t>& partition_ids, const ObDataTypeCastParams& dtc_params) const
{

  UNUSED(dtc_params);
  int ret = OB_SUCCESS;
  if (key_exprs_.count() != subkey_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (!is_partitioned_) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("Failed to push back partition id", K(ret));
    }
  } else {
    ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
    allocator.set_label("CalcByInExpr");
    ObExprCtx expr_ctx;
    ObNewRow part_row;
    ObNewRow subpart_row;

    // init expr ctx
    if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
      LOG_WARN("Failed to wrap expr ctx", K(ret));
    }

    // init all ObNewRow
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_row(allocator, 1, part_row))) {
      LOG_WARN("Failed to init row", K(ret));
    }
    {}  // do nothing

    if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_row(allocator, 1, subpart_row))) {
        LOG_WARN("Failed to init row", K(ret));
      } else {
      }  // do nothing
    }

    // calc ObNewRow for calc partition id
    for (int64_t value_idx = 0; OB_SUCC(ret) && value_idx < key_exprs_.count(); ++value_idx) {
      // Get part_row for calc conv expr
      if (OB_FAIL(calc_row(expr_ctx, key_exprs_, 1, value_idx, part_row, part_row))) {
        LOG_WARN("Failed to calc part row", K(ret));
      }

      // calc partition id and add it to partition ids no duplicate
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_row(expr_ctx, subkey_exprs_, 1, value_idx, subpart_row, subpart_row))) {
          LOG_WARN("Failed to calc subpart_row", K(ret));
        }
      }

      ObSEArray<int64_t, 5> part_ids;
      bool is_all = false;
      bool is_none = false;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(convert_row_obj_type(
                part_row, part_row, part_col_type_, part_collation_type_, dtc_params, is_all, is_none))) {
          LOG_WARN("fail to convert_row_obj_type", K(ret));
        } else if (!is_none) {
          if (is_all) {
            if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, part_ids))) {
              LOG_WARN("Failed to get all part ids", K(ret));
            }
          } else {
            if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, part_row, part_ids))) {
              LOG_WARN("Calc partition id by row error", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(convert_row_obj_type(
                subpart_row, subpart_row, subpart_col_type_, subpart_collation_type_, dtc_params, is_all, is_none))) {
          LOG_WARN("fail to convert_row_obj_type", K(ret));
        } else if (!is_none) {
          if (is_all) {
            if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids, &part_ids))) {
              LOG_WARN("Failed to get all subpart ids", K(ret));
            }
          } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, subpart_row, partition_ids, &part_ids))) {
            LOG_WARN("Calc partitioin id by row error", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::if_exprs_contain_column(const ObIArray<ObRawExpr*>& filter_exprs, bool& contain_column)
{
  int ret = OB_SUCCESS;
  contain_column = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contain_column && i < filter_exprs.count(); i++) {
    if (NULL == filter_exprs.at(i)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (filter_exprs.at(i)->has_flag(CNT_COLUMN)) {
        contain_column = true;
      }
    }
  }
  return ret;
}

int ObTableLocation::set_location_calc_node(ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& filter_exprs,
    const ObPartitionLevel part_level, const ObDataTypeCastParams& dtc_params, ObSqlExpression*& part_expr,
    ObSqlExpression*& gen_col_expr, bool& is_col_part_expr, ObPartLocCalcNode*& calc_node,
    ObPartLocCalcNode*& gen_col_node, bool& get_all)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 5> part_columns;
  ObSEArray<ColumnItem, 3> gen_cols;
  const ObRawExpr* part_raw_expr = NULL;
  if (OB_FAIL(get_partition_column_info(
          stmt, part_level, part_columns, gen_cols, part_raw_expr, part_expr, gen_col_expr, is_col_part_expr))) {
    LOG_WARN("Failed to get partition column info", K(ret));
  } else if (filter_exprs.empty()) {
    get_all = true;
  } else if (OB_FAIL(
                 get_location_calc_node(part_columns, part_raw_expr, filter_exprs, calc_node, get_all, dtc_params))) {
    LOG_WARN("Failed to get location calc node", K(ret));
  } else if (gen_cols.count() > 0) {
    // analyze information with dependented column of generated column
    bool always_true = false;
    if (OB_FAIL(get_query_range_node(gen_cols, filter_exprs, always_true, gen_col_node, dtc_params))) {
      LOG_WARN("Get query range node error", K(ret));
    } else if (always_true) {
      gen_col_node = NULL;
    } else {
    }  // do nothing
  }
  return ret;
}

/*
T_OP_IN
-- T_OP_ROW
--T_REF_COLUMN
--T_REF_COLUMN
-- T_OP_ROW
--T_OP_ROW
  --T_QUESTIONMARK
  --T_QUESTIONMARK
--T_OP_ROW
  --T_QUESTIONMARK
  --T_QUESTIONMARK
*/ //in expr

int ObTableLocation::record_in_dml_partition_info(
    ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& filter_exprs, bool& hit, const ObTableSchema* table_schema)
{
  int ret = OB_SUCCESS;
  ObRawExpr* filter_expr = NULL;
  ObRawExpr* left_expr = NULL;
  ObRawExpr* right_expr = NULL;

  hit = true;
  if (PARTITION_LEVEL_TWO != part_level_) {
    hit = false;
  }
  if (hit) {
    if (filter_exprs.count() != 1) {
      hit = false;
    } else {
      filter_expr = filter_exprs.at(0);
    }
  }

  const ObRawExpr* part_raw_expr = NULL;
  const ObRawExpr* subpart_raw_expr = NULL;

  if (OB_SUCC(ret) && hit) {
    part_raw_expr = get_related_part_expr(stmt, PARTITION_LEVEL_ONE, table_id_, ref_table_id_);
    if (NULL == part_raw_expr) {
      ret = OB_ERR_UNEXPECTED;
    } else if (T_REF_COLUMN != part_raw_expr->get_expr_type()) {
      hit = false;
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (OB_FAIL(get_part_col_type(part_raw_expr, part_col_type_, part_collation_type_, table_schema))) {
      LOG_WARN("fail to get part col type", K(ret));
    }
  }

  if (OB_SUCC(ret) && hit) {
    subpart_raw_expr = get_related_part_expr(stmt, PARTITION_LEVEL_TWO, table_id_, ref_table_id_);
    if (NULL == subpart_raw_expr) {
      ret = OB_ERR_UNEXPECTED;
    } else if (T_REF_COLUMN != subpart_raw_expr->get_expr_type()) {
      hit = false;
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (OB_FAIL(get_part_col_type(subpart_raw_expr, subpart_col_type_, subpart_collation_type_, table_schema))) {
      LOG_WARN("fail to get part col type", K(ret));
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (T_OP_IN != filter_expr->get_expr_type()) {
      hit = false;
    } else {
      ObOpRawExpr* op_raw_expr = static_cast<ObOpRawExpr*>(filter_expr);
      if (2 != op_raw_expr->get_param_count()) {
        hit = false;
      } else {
        left_expr = op_raw_expr->get_param_expr(0);
        right_expr = op_raw_expr->get_param_expr(1);
      }
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (T_OP_ROW != left_expr->get_expr_type()) {
      hit = false;
    }
  }

  int64_t pos1 = -1;
  int64_t pos2 = -1;
  if (OB_SUCC(ret) && hit) {
    ObOpRawExpr* op_left_expr = static_cast<ObOpRawExpr*>(left_expr);
    if (2 > op_left_expr->get_param_count()) {
      hit = false;
    } else {
      for (int64_t i = 0; i < op_left_expr->get_param_count(); i++) {
        ObRawExpr* tmp = op_left_expr->get_param_expr(i);
        if (tmp->same_as(*part_raw_expr)) {
          pos1 = i;
          break;
        }
      }

      for (int64_t i = 0; i < op_left_expr->get_param_count(); i++) {
        ObRawExpr* tmp = op_left_expr->get_param_expr(i);
        if (tmp->same_as(*subpart_raw_expr)) {
          pos2 = i;
          break;
        }
      }

      if (pos1 != -1 && pos2 != -1 && pos1 != pos2) {
        hit = true;
      } else {
        hit = false;
      }
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (T_OP_ROW != right_expr->get_expr_type()) {
      hit = false;
    }
  }

  if (OB_SUCC(ret) && hit) {
    ObOpRawExpr* op_left_expr = static_cast<ObOpRawExpr*>(left_expr);
    ObOpRawExpr* op_right_expr = static_cast<ObOpRawExpr*>(right_expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_right_expr->get_param_count(); i++) {
      ObRawExpr* tmp = op_right_expr->get_param_expr(i);
      if (T_OP_ROW != tmp->get_expr_type()) {
        hit = false;
        break;
      }

      ObOpRawExpr* value_expr = static_cast<ObOpRawExpr*>(tmp);
      if (op_left_expr->get_param_count() != value_expr->get_param_count()) {
        hit = false;
        break;
      }

      if (OB_FAIL(add_key_expr(key_exprs_, value_expr->get_param_expr(pos1)))) {
        LOG_WARN("fail to add key expr", K(ret));
      } else if (OB_FAIL(add_key_expr(subkey_exprs_, value_expr->get_param_expr(pos2)))) {
        LOG_WARN("fail to add key expr", K(ret));
      }
    }
  }

  ObSEArray<ColumnItem, 5> part_columns;
  ObSEArray<ColumnItem, 3> gen_cols;
  ObSEArray<ColumnItem, 5> subpart_columns;
  ObSEArray<ColumnItem, 3> sub_gen_cols;

  if (OB_SUCC(ret) && hit) {
    if (OB_FAIL(get_partition_column_info(stmt,
            PARTITION_LEVEL_ONE,
            part_columns,
            gen_cols,
            part_raw_expr,
            part_expr_,
            gen_col_expr_,
            is_col_part_expr_))) {
      LOG_WARN("Failed to get partition column info", K(ret));
    } else if (OB_FAIL(get_partition_column_info(stmt,
                   PARTITION_LEVEL_TWO,
                   subpart_columns,
                   sub_gen_cols,
                   subpart_raw_expr,
                   subpart_expr_,
                   sub_gen_col_expr_,
                   is_col_subpart_expr_))) {
      LOG_WARN("Failed to get sub partition column info", K(ret));
    }
  }

  return ret;
}

int ObTableLocation::record_not_insert_dml_partition_info(ObDMLStmt& stmt, const ObTableSchema* table_schema,
    const ObIArray<ObRawExpr*>& filter_exprs, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_location_calc_node(stmt,
          filter_exprs,
          PARTITION_LEVEL_ONE,
          dtc_params,
          part_expr_,
          gen_col_expr_,
          is_col_part_expr_,
          calc_node_,
          gen_col_node_,
          part_get_all_))) {
    LOG_WARN("failed to set location calc node for first-level partition", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level_ && OB_FAIL(set_location_calc_node(stmt,
                                                       filter_exprs,
                                                       part_level_,
                                                       dtc_params,
                                                       subpart_expr_,
                                                       sub_gen_col_expr_,
                                                       is_col_subpart_expr_,
                                                       subcalc_node_,
                                                       sub_gen_col_node_,
                                                       subpart_get_all_))) {
    LOG_WARN("failed to set location calc node for second-level partition", K(ret));
  }

  if (OB_SUCC(ret) && share::is_oracle_mode() && OB_NOT_NULL(table_schema) && table_schema->is_user_table() &&
      1 == table_schema->get_partition_key_column_num() && PARTITION_LEVEL_ONE == part_level_ && is_col_part_expr_ &&
      NONE_FAST_CALC == fast_calc_part_opt_ && OB_NOT_NULL(calc_node_) && calc_node_->is_query_range_node() &&
      OB_ISNULL(gen_col_node_) && OB_ISNULL(gen_col_expr_) && !part_get_all_) {
    const ObPLQueryRangeNode* calc_node2 = static_cast<const ObPLQueryRangeNode*>(calc_node_);
    const ObQueryRange& query_range = calc_node2->pre_query_range_;
    if (!query_range.need_deep_copy() && query_range.is_precise_get() &&
        OB_NOT_NULL(query_range.get_table_grapth().key_part_head_) &&
        query_range.get_table_grapth().key_part_head_->is_equal_condition()) {
      fast_calc_part_opt_ = UNKNOWN_FAST_CALC;
    }
    LOG_TRACE("check can fast opt",
        K(*calc_node2),
        K(fast_calc_part_opt_),
        K(query_range.need_deep_copy()),
        K(query_range.is_precise_get()),
        K(*query_range.get_table_grapth().key_part_head_),
        K(query_range.get_table_grapth().key_part_head_->is_equal_condition()),
        K(query_range));
  }
  return ret;
}

int ObTableLocation::get_not_insert_dml_part_sort_expr(ObDMLStmt& stmt, ObIArray<ObRawExpr*>* sort_exprs) const
{
  int ret = OB_SUCCESS;
  if (NULL != sort_exprs) {
    ObRawExpr* part_expr = NULL;
    if (PARTITION_LEVEL_ONE == part_level_) {
      if ((PARTITION_FUNC_TYPE_RANGE == part_type_ && is_col_part_expr_) ||
          (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_)) {
        part_expr = get_related_part_expr(stmt, PARTITION_LEVEL_ONE, table_id_, ref_table_id_);
      }
    } else if (PARTITION_LEVEL_TWO == part_level_ &&
               ((PARTITION_FUNC_TYPE_RANGE == subpart_type_ && is_col_subpart_expr_) ||
                   PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_)) {
      if (!part_get_all_ && NULL != calc_node_) {
        if (calc_node_->is_column_value_node()) {
          part_expr = get_related_part_expr(stmt, PARTITION_LEVEL_TWO, table_id_, ref_table_id_);
        } else if (calc_node_->is_query_range_node()) {
          if (static_cast<ObPLQueryRangeNode*>(calc_node_)->pre_query_range_.is_precise_get()) {
            part_expr = get_related_part_expr(stmt, PARTITION_LEVEL_TWO, table_id_, ref_table_id_);
          }
        } else {
        }
      }
    } else {
    }

    if (NULL != part_expr) {
      if (part_expr->is_column_ref_expr()) {
        if (OB_FAIL(sort_exprs->push_back(part_expr))) {
          LOG_WARN("Failed to add sort exprs", K(ret));
        }
      } else if (T_OP_ROW == part_expr->get_expr_type()) {
        int64_t param_num = part_expr->get_param_count();
        ObRawExpr* col_expr = NULL;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < param_num; ++idx) {
          if (OB_ISNULL(col_expr = part_expr->get_param_expr(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Col expr should not be NULL", K(ret));
          } else if (!col_expr->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param expr should be column expr", K(ret));
          } else if (OB_FAIL(sort_exprs->push_back(col_expr))) {
            LOG_WARN("Failed to add sort exprs", K(ret));
          } else {
          }  // do nothing
        }
      } else if (related_part_expr_idx_ != OB_INVALID_INDEX) {
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Part expr should be Column or T_OP_ROW of column", K(ret), KPNAME(part_expr));
      }
    }
  }
  return ret;
}

int ObTableLocation::get_location_calc_node(ObIArray<ColumnItem>& partition_columns, const ObRawExpr* partition_expr,
    const ObIArray<ObRawExpr*>& filter_exprs, ObPartLocCalcNode*& res_node, bool& get_all,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  get_all = false;
  bool only_range_node = false;

  if (partition_expr->is_column_ref_expr() || is_virtual_table(ref_table_id_)) {
    // partition expr is signle column, only query range needed
    // In future column list is also only query range needed.
    only_range_node = true;
  } else if (partition_columns.count() == 1) {
    column_id = partition_columns.at(0).column_id_;
  } else { /*do nothing*/
  }

  if (OB_FAIL(ret)) {
  } else if (only_range_node) {
    bool always_true = false;
    ObPartLocCalcNode* calc_node = NULL;
    if (OB_FAIL(get_query_range_node(partition_columns, filter_exprs, always_true, calc_node, dtc_params))) {
      LOG_WARN("Get query range node error", K(ret));
    } else if (always_true) {
      get_all = true;
    } else {
      res_node = calc_node;
    }
  } else {
    ObSEArray<ObRawExpr*, 5> normal_filters;
    bool func_always_true = true;
    ObPartLocCalcNode* func_node = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < filter_exprs.count(); ++idx) {
      bool cnt_func_expr = false;
      bool always_true = false;
      ObPartLocCalcNode* calc_node = NULL;
      if (OB_FAIL(analyze_filter(partition_columns,
              partition_expr,
              column_id,
              filter_exprs.at(idx),
              always_true,
              calc_node,
              cnt_func_expr,
              dtc_params))) {
        LOG_WARN("Failed to analyze filter", K(ret));
      } else {
        if (!always_true && NULL != calc_node) {
          func_always_true = false;
        }
        if (!cnt_func_expr) {
          if (OB_FAIL(normal_filters.push_back(filter_exprs.at(idx)))) {
            LOG_WARN("Failed to add filter", K(ret));
          }
        } else if (OB_FAIL(add_and_node(calc_node, func_node))) {
          LOG_WARN("Failed to add and node", K(ret));
        } else {
        }  // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      bool column_always_true = false;
      ObPartLocCalcNode* column_node = NULL;
      if (normal_filters.count() > 0) {
        if (OB_FAIL(
                get_query_range_node(partition_columns, filter_exprs, column_always_true, column_node, dtc_params))) {
          LOG_WARN("Failed to get query range node", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (func_always_true && column_always_true) {
        get_all = true;
      } else if (func_always_true) {
        res_node = column_node;
      } else if (column_always_true) {
        res_node = func_node;
      } else if (OB_FAIL(add_and_node(func_node, column_node))) {
        LOG_WARN("Failed to add and node", K(ret));
      } else {
        res_node = column_node;
      }
    }
  }
  return ret;
}

int ObTableLocation::get_query_range_node(const ColumnIArray& partition_columns,
    const ObIArray<ObRawExpr*>& filter_exprs, bool& always_true, ObPartLocCalcNode*& calc_node,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (filter_exprs.empty()) {
    always_true = true;
  } else if (OB_ISNULL(calc_node = ObPartLocCalcNode::create_part_calc_node(
                           allocator_, calc_nodes_, ObPartLocCalcNode::QUERY_RANGE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory failed", K(ret));
  } else {
    ObPLQueryRangeNode* node = static_cast<ObPLQueryRangeNode*>(calc_node);
    if (OB_FAIL(node->pre_query_range_.preliminary_extract_query_range(partition_columns, filter_exprs, dtc_params))) {
      LOG_WARN("Failed to pre extract query range", K(ret));
    } else if (node->pre_query_range_.is_precise_whole_range()) {
      // pre query range is whole range, indicate that there are no partition condition in filters,
      // so you need to get all part ids
      always_true = true;
      calc_node = NULL;
    }
  }
  return ret;
}

int ObTableLocation::analyze_filter(const ObIArray<ColumnItem>& partition_columns, const ObRawExpr* partition_expr,
    uint64_t column_id, const ObRawExpr* filter, bool& always_true, ObPartLocCalcNode*& calc_node, bool& cnt_func_expr,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter should not be NULL", K(ret));
  } else if (T_OP_OR == filter->get_expr_type() || T_OP_AND == filter->get_expr_type()) {
    const ObOpRawExpr* op_expr = static_cast<const ObOpRawExpr*>(filter);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < op_expr->get_param_count(); ++idx) {
      ObPartLocCalcNode* cur_node = NULL;
      bool f_always_true = false;
      if (OB_FAIL(analyze_filter(partition_columns,
              partition_expr,
              column_id,
              op_expr->get_param_expr(idx),
              f_always_true,
              cur_node,
              cnt_func_expr,
              dtc_params))) {
        LOG_WARN("Failed to replace sub_expr bool filter", K(ret));
      } else if (T_OP_OR == filter->get_expr_type()) {
        if (f_always_true || NULL == cur_node) {
          always_true = true;
          calc_node = NULL;
        } else if (OB_FAIL(add_or_node(cur_node, calc_node))) {
          LOG_WARN("Failed to add or node", K(ret));
        } else {
        }
      } else {
        if (f_always_true || NULL == cur_node) {
          // do nothing
        } else if (OB_FAIL(add_and_node(cur_node, calc_node))) {
          LOG_WARN("Failed to add and node", K(ret));
        } else {
        }
      }
    }
  } else if (T_OP_IN == filter->get_expr_type()) {
    // todo extract_in_op
  } else if (T_OP_EQ == filter->get_expr_type()) {
    const ObRawExpr* l_expr = filter->get_param_expr(0);
    const ObRawExpr* r_expr = filter->get_param_expr(1);
    if (OB_FAIL(extract_eq_op(l_expr,
            r_expr,
            partition_expr,
            column_id,
            filter->get_result_type(),
            cnt_func_expr,
            always_true,
            calc_node))) {
      LOG_WARN("Failed to extract equal expr", K(ret));
    }
  } else {
  }

  return ret;
}

int ObTableLocation::extract_eq_op(const ObRawExpr* l_expr, const ObRawExpr* r_expr, const ObRawExpr* partition_expr,
    const uint64_t column_id, const ObExprResType& res_type, bool& cnt_func_expr, bool& always_true,
    ObPartLocCalcNode*& calc_node)
{
  int ret = OB_SUCCESS;
  UNUSED(column_id);
  always_true = false;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || OB_ISNULL(partition_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr should not be NULL", K(ret), K(l_expr), K(r_expr), K(partition_expr));
  } else if ((l_expr->has_flag(CNT_COLUMN) && !(l_expr->has_flag(IS_COLUMN)) && r_expr->has_flag(IS_CONST)) ||
             (r_expr->has_flag(CNT_COLUMN) && !(r_expr->has_flag(IS_COLUMN)) && l_expr->has_flag(IS_CONST))) {
    const ObRawExpr* func_expr = l_expr->has_flag(CNT_COLUMN) ? l_expr : r_expr;
    const ObRawExpr* c_expr = l_expr->has_flag(IS_CONST) ? l_expr : r_expr;
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(c_expr);
    bool equal = false;
    ObExprEqualCheckContext equal_ctx;
    bool true_false = true;
    if (!ObQueryRange::can_be_extract_range(T_OP_EQ,
            func_expr->get_result_type(),
            res_type.get_calc_meta(),
            const_expr->get_result_type().get_type(),
            true_false)) {
      always_true = true_false;
    } else if (OB_FAIL(check_expr_equal(partition_expr, func_expr, equal, equal_ctx))) {
      LOG_WARN("Failed to check equal expr", K(ret));
    } else if (equal) {
      if (NULL == (calc_node = ObPartLocCalcNode::create_part_calc_node(
                       allocator_, calc_nodes_, ObPartLocCalcNode::FUNC_VALUE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to create expr calc node", K(ret));
      } else {
        ObPLFuncValueNode* node = static_cast<ObPLFuncValueNode*>(calc_node);
        node->res_type_ = res_type;
        cnt_func_expr = true;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < equal_ctx.param_expr_.count(); ++idx) {
          ObExprEqualCheckContext::ParamExprPair& param_pair = equal_ctx.param_expr_.at(idx);
          if (OB_ISNULL(param_pair.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param expr should not be NULL", K(ret));
          } else if (param_pair.param_idx_ < 0) {
            ret = OB_ERR_ILLEGAL_INDEX;
            LOG_WARN("Wrong index of question mark position", K(ret), "param_idx", param_pair.param_idx_);
          } else if (!param_pair.expr_->is_const_expr() || T_QUESTIONMARK == param_pair.expr_->get_expr_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param pair expr should be const and const in expr_to_find should not be T_QUESTIONMARK", K(ret));
          } else {
            const ObObj& expect_val = static_cast<const ObConstRawExpr*>(param_pair.expr_)->get_value();
            ObObj dst;
            if (OB_FAIL(deep_copy_obj(allocator_, expect_val, dst))) {
              LOG_WARN("Failed to deep copy obj", K(ret));
            } else if (OB_FAIL(node->param_value_.push_back(
                           ObPLFuncValueNode::ParamValuePair(param_pair.param_idx_, dst)))) {
              LOG_WARN("Failed to add param value pair", K(ret));
            } else {
            }  // do nothing
          }
        }
        if (OB_FAIL(ret)) {
        } else if (T_QUESTIONMARK == const_expr->get_expr_type()) {
          node->param_idx_ = const_expr->get_value().get_unknown();
          if (node->param_idx_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param idx should not be less than 0", K(ret));
          }
        } else {
          ObObj value;
          if (OB_FAIL(deep_copy_obj(allocator_, const_expr->get_value(), node->value_))) {
            LOG_WARN("Failed to deep copy const value", K(ret));
          }
        }
      }
    } else {
    }  // do nothing
  } else if ((l_expr->has_flag(IS_COLUMN) && r_expr->has_flag(IS_CONST)) ||
             (r_expr->has_flag(IS_COLUMN) && l_expr->has_flag(IS_CONST))) {
    const ObRawExpr* col_expr = l_expr->has_flag(IS_COLUMN) ? l_expr : r_expr;
    const ObRawExpr* c_expr = l_expr->has_flag(IS_CONST) ? l_expr : r_expr;
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(c_expr);
    const ObColumnRefRawExpr* column_expr = static_cast<const ObColumnRefRawExpr*>(col_expr);
    if (column_id == column_expr->get_column_id()) {
      bool true_false = false;
      if (!ObQueryRange::can_be_extract_range(T_OP_EQ,
              col_expr->get_result_type(),
              res_type.get_calc_meta(),
              const_expr->get_result_type().get_type(),
              true_false)) {
        always_true = true_false;
      } else if (NULL == (calc_node = ObPartLocCalcNode::create_part_calc_node(
                              allocator_, calc_nodes_, ObPartLocCalcNode::COLUMN_VALUE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to create expr calc node", K(ret));
      } else {
        ObPLColumnValueNode* node = static_cast<ObPLColumnValueNode*>(calc_node);
        node->res_type_ = res_type;
        if (T_QUESTIONMARK == const_expr->get_expr_type()) {
          node->param_idx_ = const_expr->get_value().get_unknown();
          if (node->param_idx_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param idx should not be less than 0", K(ret));
          }
        } else {
          ObObj value;
          if (OB_FAIL(deep_copy_obj(allocator_, const_expr->get_value(), node->value_))) {
            LOG_WARN("Failed to deep copy const value", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::check_expr_equal(
    const ObRawExpr* partition_expr, const ObRawExpr* check_expr, bool& equal, ObExprEqualCheckContext& equal_ctx)
{
  int ret = OB_SUCCESS;
  equal = partition_expr->same_as(*check_expr, &equal_ctx);
  if (OB_SUCCESS != equal_ctx.err_code_) {
    ret = equal_ctx.err_code_;
    LOG_WARN("Failed to check whether expr equal", K(ret));
  }
  return ret;
}

// int ObTableLocation::calc_sub_select_partition_ids(ObExecContext &exec_ctx, ObIArray<int64_t> &partition_ids) const
//{
//  int ret = OB_SUCCESS;
//  ObTaskExecutorCtx &task_exec_ctx = exec_ctx.get_task_exec_ctx();
//  const ObPhyTableLocationInfoIArray *tbl_loc_infos = NULL;
//  if (OB_UNLIKELY(OB_INVALID_ID == subselect_table_id_)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("table location is invalid", K(subselect_table_id_));
//  } else if (OB_ISNULL(tbl_loc_infos = task_exec_ctx.get_global_table_location_infos())) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("table location info is null");
//  } else {
//    const ObPhyTableLocationInfo *tbl_location_info = NULL;
//    for (int64_t i = 0; OB_SUCC(ret) && tbl_location_info == NULL && i < tbl_loc_infos->count(); ++i) {
//      if (tbl_loc_infos->at(i).get_table_location_key() == subselect_table_id_) {
//        tbl_location_info = &(tbl_loc_infos->at(i));
//      }
//    }
//    if (OB_SUCC(ret) && OB_ISNULL(tbl_location_info)) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("table partition location not found", K(tbl_location_info));
//    }
//    if (OB_SUCC(ret)) {
//      const ObPhyPartitionLocationInfoIArray &part_list = tbl_location_info->get_phy_part_loc_info_list();
//      for (int64_t i = 0; OB_SUCC(ret) && i < part_list.count(); ++i) {
//        if (OB_FAIL(ObOptimizerUtil::add_index_no_duplicate(partition_ids,
//        part_list.at(i).get_partition_location().get_partition_id()))) {
//          LOG_WARN("add id no duplicated failed", K(part_list.at(i).get_partition_location().get_partition_id()));
//        }
//      }
//    }
//  }
//  return ret;
//}

// int ObTableLocation::calc_insert_select_partition_rule(ObSchemaGetterGuard &schema_guard,
//                                                       const ObInsertStmt &insert_stmt,
//                                                       const ObSelectStmt &sub_select)
//{
//  int ret = OB_SUCCESS;
//  if (sub_select.get_table_size() == 1) {
//    //ignore, only analyzer single table
//    const TableItem *table_item = sub_select.get_table_item(0);
//    const ObTableSchema *sel_tbl = NULL;
//    const ObTableSchema *inst_tbl = NULL;
//    bool is_same_part = false;
//    uint64_t ins_tbl_id = insert_stmt.get_table_id();
//    if (OB_ISNULL(table_item)) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("table item is null");
//    } else if (!table_item->is_basic_table()) {
//      //ignore, only analyzer basic table
//    } else if (OB_FAIL(schema_guard.get_table_schema(table_item->ref_id_, sel_tbl))) {
//      LOG_WARN("get table schema failed", K(ret), K(table_item->ref_id_));
//    } else if (OB_FAIL(schema_guard.get_table_schema(insert_stmt.get_table_id(), inst_tbl))) {
//      LOG_WARN("get table schema failed", K(ret), K(insert_stmt.get_table_id()));
//    } else if (!inst_tbl->is_partitioned_table()) {
//      is_same_part = false;
//    } else if (OB_FAIL(ObOptimizerUtil::has_same_partition(*inst_tbl, get_related_part_expr(insert_stmt,
//    PARTITION_LEVEL_ONE, ins_tbl_id, ins_tbl_id), *sel_tbl,
//                                                           get_related_part_expr(sub_select, PARTITION_LEVEL_ONE,
//                                                           table_item->table_id_, table_item->ref_id_),
//                                                           PARTITION_LEVEL_ONE, is_same_part))) {
//      LOG_WARN("check whether has same partition failed", K(ret));
//    } else if (is_same_part && inst_tbl->get_part_level() == PARTITION_LEVEL_TWO) {
//      if (OB_FAIL(ObOptimizerUtil::has_same_partition(*inst_tbl, get_related_part_expr(insert_stmt,
//      PARTITION_LEVEL_TWO, ins_tbl_id, ins_tbl_id),
//                                                      *sel_tbl, get_related_part_expr(sub_select, PARTITION_LEVEL_TWO,
//                                                      table_item->table_id_, table_item->ref_id_),
//                                                      PARTITION_LEVEL_TWO, is_same_part))) {
//        LOG_WARN("check whether has same partition failed", K(ret));
//      }
//    }
//    if (OB_SUCC(ret) && is_same_part) {
//      //check whether partition key from sub select
//      bool has_same_key = false;
//      const ObPartitionKeyInfo &inst_partkeys = inst_tbl->get_partition_key_info();
//      const ObPartitionKeyInfo &inst_subpart_keys = inst_tbl->get_subpartition_key_info();
//      const ObPartitionKeyInfo &sel_partkeys = sel_tbl->get_partition_key_info();
//      const ObPartitionKeyInfo &sel_subpart_keys = sel_tbl->get_subpartition_key_info();
//      if (OB_FAIL(ObOptimizerUtil::all_same_partition_key(inst_partkeys, sel_partkeys, insert_stmt, sub_select,
//      has_same_key))) {
//        LOG_WARN("check all same partition key failed", K(ret), K(inst_partkeys), K(sel_partkeys));
//      } else if (!has_same_key) {
//        //ignore
//      } else if (OB_FAIL(ObOptimizerUtil::all_same_partition_key(inst_subpart_keys, sel_subpart_keys,
//                                                                 insert_stmt, sub_select, has_same_key))) {
//        LOG_WARN("check all same subpartition key failed", K(ret), K(inst_subpart_keys), K(sel_subpart_keys));
//      } else if (has_same_key) {
//        same_part_with_select_ = true;
//        subselect_table_id_ = table_item->table_id_;
//      }
//    }
//  }
//  return ret;
//}

int ObTableLocation::record_insert_partition_info(
    ObDMLStmt& stmt, const share::schema::ObTableSchema* table_schema, ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  bool partkey_has_subquery = false;
  ObInsertStmt& insert_stmt = static_cast<ObInsertStmt&>(stmt);
  ObSEArray<ColumnItem, 4> partition_columns;
  ObSEArray<ColumnItem, 4> gen_cols;  // actual not used in insert stmt
  const ObRawExpr* partition_raw_expr = NULL;
  if (OB_FAIL(insert_stmt.part_key_has_subquery(partkey_has_subquery))) {
    LOG_WARN("failed to check whether insert stmt has part key", K(ret));
  } else if ((insert_stmt.has_part_key_sequence() || partkey_has_subquery) &&
             PARTITION_LEVEL_ZERO != table_schema->get_part_level()) {
    part_get_all_ = true;
  } else if (OB_FAIL(get_partition_column_info(stmt,
                 PARTITION_LEVEL_ONE,
                 partition_columns,
                 gen_cols,
                 partition_raw_expr,
                 part_expr_,
                 gen_col_expr_,
                 is_col_part_expr_))) {
    LOG_WARN("Failed to get all range column info", K(ret));
  } else if (OB_FAIL(
                 record_insert_part_info(insert_stmt, session_info, partition_columns, key_exprs_, key_conv_exprs_))) {
    LOG_WARN("Failed to record insert partition info", K(ret));
  } else if (OB_FAIL(gen_insert_part_row_projector(
                 partition_raw_expr, partition_columns, PARTITION_LEVEL_ONE, part_type_))) {
    LOG_WARN("generate insert part row projector failed", K(ret), K_(part_type));
  } else if (PARTITION_LEVEL_TWO == part_level_) {
    partition_columns.reuse();
    gen_cols.reuse();
    if (OB_FAIL(get_partition_column_info(stmt,
            PARTITION_LEVEL_TWO,
            partition_columns,
            gen_cols,
            partition_raw_expr,
            subpart_expr_,
            sub_gen_col_expr_,
            is_col_subpart_expr_))) {
      LOG_WARN("Failed to get all range column info", K(ret));
    } else if (OB_FAIL(record_insert_part_info(
                   insert_stmt, session_info, partition_columns, subkey_exprs_, subkey_conv_exprs_))) {
      LOG_WARN("Failed to record insert partition info", K(ret));
    } else if (OB_FAIL(gen_insert_part_row_projector(
                   partition_raw_expr, partition_columns, PARTITION_LEVEL_TWO, subpart_type_))) {
      LOG_WARN("gen insert part row projector failed", K(ret), K_(subpart_type));
    } else {
    }
  } else {
  }  // do nothing

  return ret;
}
int ObTableLocation::generate_rowkey_desc(ObDMLStmt& stmt, const ObRowkeyInfo& rowkey_info, uint64_t data_table_id,
    ObRawExprFactory& expr_factory, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t rowkey_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_id))) {
      LOG_WARN("get rowkey column id failed", K(ret), K(i));
    } else {
      ObColumnRefRawExpr* rowkey_col = stmt.get_column_expr_by_id(data_table_id, rowkey_id);
      if (OB_ISNULL(rowkey_col)) {
        if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, rowkey_col))) {
          LOG_WARN("create mock rowkey column expr failed", K(ret));
        } else if (OB_FAIL(row_desc.add_column(rowkey_col))) {
          LOG_WARN("add rowkey column expr to row desc failed", K(ret));
        }
      } else if (OB_FAIL(row_desc.add_column(rowkey_col))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::get_partition_column_info(ObDMLStmt& stmt, const ObPartitionLevel part_level,
    ObIArray<ColumnItem>& partition_columns, ObIArray<ColumnItem>& gen_cols, const ObRawExpr*& partition_raw_expr,
    ObSqlExpression*& partition_expression, ObSqlExpression*& gen_col_expression, bool& is_col_part_expr)
{
  int ret = OB_SUCCESS;
  partition_raw_expr = NULL;
  ObRawExpr* gen_col_expr = NULL;
  RowDesc row_desc;
  RowDesc gen_row_desc;
  if (PARTITION_LEVEL_ONE != part_level && PARTITION_LEVEL_TWO != part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part level should be One or Two", K(ret));
  } else if (OB_ISNULL(partition_raw_expr = get_related_part_expr(stmt, part_level, table_id_, ref_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition expr not in stmt", K(ret), K(table_id_), K(part_level), K(ref_table_id_));
  } else if (PARTITION_LEVEL_ONE == part_level && PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ &&
             OB_FAIL(can_get_part_by_range_for_range_columns(partition_raw_expr, is_valid_range_columns_part_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level && PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ &&
             OB_FAIL(
                 can_get_part_by_range_for_range_columns(partition_raw_expr, is_valid_range_columns_subpart_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (FALSE_IT(is_col_part_expr = partition_raw_expr->is_column_ref_expr())) {
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else if (OB_FAIL(gen_row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else if (OB_FAIL(add_partition_columns(
                 stmt, partition_raw_expr, partition_columns, gen_cols, gen_col_expr, row_desc, gen_row_desc))) {
    LOG_WARN("Failed to add partitoin column", K(ret));
  } else {
    if (gen_cols.count() > 0) {
      if (partition_columns.count() > 1) {
        gen_cols.reset();
      } else if (OB_FAIL(ObExprGeneratorImpl::gen_expression_with_row_desc(
                     sql_expression_factory_, expr_op_factory_, gen_row_desc, gen_col_expr, gen_col_expression))) {
        LOG_WARN("Failed to gen expression with row desc", K(ret));
      } else {
      }  // do nothing
    }
    // generate partition_expression with partition columns row_desc
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObExprGeneratorImpl::gen_expression_with_row_desc(
              sql_expression_factory_, expr_op_factory_, row_desc, partition_raw_expr, partition_expression))) {
        LOG_WARN("Failed to gen expression with row desc", K(ret));
      }
    }

    // clear IS_COLUMNLIZED flag
    if (OB_SUCC(ret)) {
      if (OB_FAIL(clear_columnlized_in_row_desc(row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      } else if (OB_FAIL(clear_columnlized_in_row_desc(gen_row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      } else {
      }  // do nothing
    }
  }

  return ret;
}

int ObTableLocation::add_partition_columns(ObDMLStmt& stmt, const ObRawExpr* part_expr,
    ObIArray<ColumnItem>& partition_columns, ObIArray<ColumnItem>& gen_cols, ObRawExpr*& gen_col_expr,
    RowDesc& row_desc, RowDesc& gen_row_desc, const bool only_gen_cols)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> cur_vars;
  if (OB_ISNULL(part_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part expr is NULL", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, cur_vars))) {
    LOG_WARN("get column exprs error", K(ret));
  } else {
    ObRawExpr* var = NULL;
    uint64_t column_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    ObColumnRefRawExpr* col_expr = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cur_vars.count(); ++idx) {
      if (OB_ISNULL(var = cur_vars.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Var should not be NULL in column exprs", K(ret));
      } else if (!var->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Var should be column", K(ret));
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(var))) {
      } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = col_expr->get_column_id())) ||
                 OB_UNLIKELY(OB_INVALID_ID == (table_id = col_expr->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column id should not be OB_INVALID_ID", K(ret));
      } else if (only_gen_cols) {  // only deal dependented columns for generated partition column
        if (OB_FAIL(add_partition_column(stmt, table_id, column_id, gen_cols, gen_row_desc))) {
          LOG_WARN("Failed to add partiton column", K(ret));
        }
      } else {
        if (col_expr->is_generated_column()) {
          if (OB_ISNULL(col_expr->get_dependant_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Generated column's dependent expr is NULL", K(ret));
          } else if (is_simple_insert_or_replace()) {
            if (OB_FAIL(add_partition_columns(stmt,
                    col_expr->get_dependant_expr(),
                    partition_columns,
                    gen_cols,
                    gen_col_expr,
                    row_desc,
                    gen_row_desc))) {
              LOG_WARN("Failed to add partition column", K(ret));
            }
          } else if (cur_vars.count() > 1) {
            // do nothing.Only deal case with one partition column.
          } else {
            gen_col_expr = col_expr->get_dependant_expr();
            if (OB_FAIL(add_partition_columns(stmt,
                    col_expr->get_dependant_expr(),
                    partition_columns,
                    gen_cols,
                    gen_col_expr,
                    row_desc,
                    gen_row_desc,
                    true))) {
              LOG_WARN("Failed to add gen columns", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_partition_column(stmt, table_id, column_id, partition_columns, row_desc))) {
            LOG_WARN("Failed to add partiton column", K(ret));
          }
        }
      }  // end of else
    }    // end of for
  }
  return ret;
}

int ObTableLocation::add_partition_column(ObDMLStmt& stmt, const uint64_t table_id, const uint64_t column_id,
    ObIArray<ColumnItem>& partition_columns, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  bool is_different = true;
  for (int64_t idx = 0; OB_SUCC(ret) && is_different && idx < partition_columns.count(); ++idx) {
    if (partition_columns.at(idx).column_id_ == column_id) {
      is_different = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_different) {
      ColumnItem* column_item = NULL;
      if (NULL == (column_item = stmt.get_column_item_by_id(table_id, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get column item by id", K(table_id), K(column_id), K(ret));
      } else if (OB_ISNULL(column_item->get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column item's expr is NULL", K(ret));
      } else if (OB_FAIL(row_desc.add_column(column_item->get_expr()))) {
        LOG_WARN("Failed to add column item to temporary row desc", K(ret));
      } else if (OB_FAIL(partition_columns.push_back(*column_item))) {
        LOG_WARN("Failed to add column item to partition columns", K(ret));
      } else if (OB_FAIL(column_item->get_expr()->add_flag(IS_COLUMNLIZED))) {
        LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
      } else {
      }
    }
  }
  return ret;
}

int ObTableLocation::add_key_expr(IKeyExprs& key_exprs, ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  ObSqlExpression* sql_expr = NULL;
  if (NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Expr should not be NULL", K(expr), K(ret));
  } else if (!expr->is_const_expr() && !expr->has_flag(IS_CALCULABLE_EXPR)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-const or non-calculable expression partition key");
  } else if (OB_FAIL(sql_expression_factory_.alloc(sql_expr))) {
    LOG_WARN("Failed to alloc sql_expr", K(ret));
  } else if (OB_ISNULL(sql_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Alloc partition expr error", K(ret));
  } else {
    RowDesc row_desc;  // empty row desc
    ObExprGeneratorImpl expr_generator(expr_op_factory_, 0, 0, NULL, row_desc);
    if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
      LOG_WARN("Failed to generate sql expression", K(ret));
    } else if (OB_FAIL(key_exprs.push_back(sql_expr))) {
      LOG_WARN("Failed to add sql expr to array", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTableLocation::add_key_expr_with_row_desc(IKeyExprs& key_exprs, RowDesc& row_desc, ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  ObSqlExpression* sql_expr = NULL;
  if (NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Expr should not be NULL", K(expr), K(ret));
  } else if (OB_FAIL(sql_expression_factory_.alloc(sql_expr))) {
    LOG_WARN("fail to alloc sql-expr", K(ret));
  } else if (OB_ISNULL(sql_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Alloc partition expr error", K(ret));
  } else {
    ObExprGeneratorImpl expr_generator(expr_op_factory_, 0, 0, NULL, row_desc);
    if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
      LOG_WARN("Failed to generate sql expression", K(ret));
    } else if (OB_FAIL(key_exprs.push_back(sql_expr))) {
      LOG_WARN("Failed to add sql expr to array", K(ret));
    } else {
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_calc_node(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPartLocCalcNode* calc_node, const ObPartLocCalcNode* gen_col_node,
    const ObSqlExpression* gen_col_expr, const bool part_col_get_all, ObIArray<int64_t>& partition_ids,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_partitioned_) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("Failed to push back partition id", K(ret));
    }
  } else {
    ObSEArray<int64_t, 5> gen_part_ids;
    bool part_col_all_part = false;
    bool gen_col_all_part = false;
    // get partition ids with information of partition columns
    if (!part_col_get_all) {
      // partition condition is not empty, so you need to calc partition id by partition key
      if (OB_FAIL(calc_partition_ids_by_calc_node(
              exec_ctx, part_mgr, params, calc_node, partition_ids, part_col_all_part, dtc_params, part_ids))) {
        LOG_WARN("Failed to calc partitoin ids by calc node", K(ret));
      }
    } else {
      part_col_all_part = true;
    }
    if (OB_SUCC(ret)) {
      if (NULL != gen_col_node && NULL != gen_col_expr) {
        // partition key is generated column and dependent column condition is not empty
        // so you need to calc partition id by dependent column condition
        if (!gen_col_node->is_query_range_node()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column dependented by generated column calc node should be query range node", K(ret));
        } else if (OB_FAIL(calc_query_range_partition_ids(exec_ctx,
                       part_mgr,
                       params,
                       static_cast<const ObPLQueryRangeNode*>(gen_col_node),
                       gen_part_ids,
                       gen_col_all_part,
                       dtc_params,
                       part_ids,
                       gen_col_expr))) {
          LOG_WARN("Failed to calcl partition ids by gen col node", K(ret));
        }
      } else {
        gen_col_all_part = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (!part_col_all_part && !gen_col_all_part) {
        // partition key condition is not empty and generated column dependent condition is not empty
        // so you need to intersect partition ids extracted by these two conditions
        if (OB_FAIL(intersect_partition_ids(gen_part_ids, partition_ids))) {  // intersect ids
          LOG_WARN("Failed to intersect partition ids", K(ret));
        }
      } else if (!part_col_all_part && gen_col_all_part) {
        // generated column condition is empty, but partition key condition is not empty,
        // use the partition ids extracted by partition key condition directly, do nothing
      } else if (part_col_all_part && !gen_col_all_part) {
        // generated column condition is not empty, but partition key condition is empty,
        // use the partiiton ids extracted by generated column condition, so append to partition_ids
        if (OB_FAIL(append(partition_ids, gen_part_ids))) {
          LOG_WARN("append partition ids failed", K(ret));
        }
      } else {
        // part_col_all_part=true && gen_col_all_part=true
        // has no any partition condition, get all partition ids in schema
        if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids, part_ids))) {
          LOG_WARN("Get all part ids error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::intersect_partition_ids(
    const ObIArray<int64_t>& to_inter_ids, ObIArray<int64_t>& partition_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 128> intersect_tmp;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < partition_ids.count(); ++idx) {
    if (has_exist_in_array(to_inter_ids, partition_ids.at(idx), NULL)) {
      if (OB_FAIL(intersect_tmp.push_back(partition_ids.at(idx)))) {
        LOG_WARN("Failed to add partition id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    partition_ids.reset();
    if (OB_FAIL(partition_ids.assign(intersect_tmp))) {
      LOG_WARN("Failed to assign intersect tmp ids", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_calc_node(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPartLocCalcNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  all_part = false;
  if (OB_UNLIKELY(NULL == calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (ObPartLocCalcNode::CALC_AND == calc_node->get_node_type()) {
    if (OB_FAIL(calc_and_partition_ids(exec_ctx,
            part_mgr,
            params,
            static_cast<const ObPLAndNode*>(calc_node),
            partition_ids,
            all_part,
            dtc_params,
            part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::CALC_OR == calc_node->get_node_type()) {
    if (OB_FAIL(calc_or_partition_ids(exec_ctx,
            part_mgr,
            params,
            static_cast<const ObPLOrNode*>(calc_node),
            partition_ids,
            all_part,
            dtc_params,
            part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::QUERY_RANGE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_query_range_partition_ids(exec_ctx,
            part_mgr,
            params,
            static_cast<const ObPLQueryRangeNode*>(calc_node),
            partition_ids,
            all_part,
            dtc_params,
            part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::FUNC_VALUE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_func_value_partition_ids(exec_ctx,
            part_mgr,
            params,
            static_cast<const ObPLFuncValueNode*>(calc_node),
            partition_ids,
            all_part,
            dtc_params,
            part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::COLUMN_VALUE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_column_value_partition_ids(exec_ctx,
            part_mgr,
            params,
            static_cast<const ObPLColumnValueNode*>(calc_node),
            partition_ids,
            all_part,
            dtc_params,
            part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unknown calc node type", K(ret));
  }
  return ret;
}

int ObTableLocation::calc_and_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPLAndNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (OB_ISNULL(calc_node->left_node_) || OB_ISNULL(calc_node->right_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("And node shoul have left and right node", K(ret));
  } else {
    ObSEArray<int64_t, 3> left_part_ids;
    bool left_all = false;
    ObSEArray<int64_t, 3> right_part_ids;
    bool right_all = false;
    if (OB_FAIL(calc_partition_ids_by_calc_node(
            exec_ctx, part_mgr, params, calc_node->left_node_, left_part_ids, left_all, dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx,
                   part_mgr,
                   params,
                   calc_node->right_node_,
                   right_part_ids,
                   right_all,
                   dtc_params,
                   part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (left_all && right_all) {
      all_part = true;
    } else if (left_all) {
      if (OB_FAIL(partition_ids.assign(right_part_ids))) {
        LOG_WARN("Failed to assign part ids", K(ret));
      }
    } else if (right_all) {
      if (OB_FAIL(partition_ids.assign(left_part_ids))) {
        LOG_WARN("Failed to assign part ids", K(ret));
      }
    } else {
      for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < left_part_ids.count(); ++l_idx) {
        for (int64_t r_idx = 0; OB_SUCC(ret) && r_idx < right_part_ids.count(); ++r_idx) {
          if (right_part_ids.at(r_idx) == left_part_ids.at(l_idx)) {
            if (OB_FAIL(partition_ids.push_back(left_part_ids.at(l_idx)))) {
              LOG_WARN("Failed to add part id", K(ret));
            }
          }
        }  // end of r_idx
      }    // end of l_idx
    }
  }

  return ret;
}

int ObTableLocation::calc_or_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPLOrNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (OB_ISNULL(calc_node->left_node_) || OB_ISNULL(calc_node->right_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("And node shoul have left and right node", K(ret));
  } else {
    ObSEArray<int64_t, 3> left_part_ids;
    bool left_all = false;
    ObSEArray<int64_t, 3> right_part_ids;
    bool right_all = false;
    if (OB_FAIL(calc_partition_ids_by_calc_node(
            exec_ctx, part_mgr, params, calc_node->left_node_, left_part_ids, left_all, dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx,
                   part_mgr,
                   params,
                   calc_node->right_node_,
                   right_part_ids,
                   right_all,
                   dtc_params,
                   part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (left_all || right_all) {
      all_part = true;
    } else if (OB_FAIL(append_array_no_dup(partition_ids, left_part_ids))) {
      LOG_WARN("Failed to append array", K(ret));
    } else if (OB_FAIL(append_array_no_dup(partition_ids, right_part_ids))) {
      LOG_WARN("Failed to append array", K(ret));
    } else {
    }
  }

  return ret;
}

int ObTableLocation::calc_query_range_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPLQueryRangeNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids,
    const ObSqlExpression* gen_col_expr) const
{
  UNUSED(part_mgr);
  UNUSED(part_ids);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else {
    all_part = false;
    ObQueryRangeArray query_ranges;
    ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
    allocator.set_label("CalcQRPartIds");
    bool is_all_single_value_ranges = true;
    if (OB_FAIL(calc_node->pre_query_range_.get_tablet_ranges(
            allocator, params, query_ranges, is_all_single_value_ranges, dtc_params))) {
      LOG_WARN("get tablet ranges failed", K(ret));
    } else if (query_ranges.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Query ranges' count should not be 0", "query range count", query_ranges.count(), K(ret));
    } else {
      bool is_empty = true;
      if (OB_FAIL(is_all_ranges_empty(query_ranges, is_empty))) {
        LOG_WARN("fail to check all ranges", K(query_ranges));
      } else if (!is_empty) {
        if (OB_FAIL(calc_partition_ids_by_ranges(exec_ctx,
                part_mgr,
                query_ranges,
                is_all_single_value_ranges,
                partition_ids,
                all_part,
                part_ids,
                gen_col_expr))) {
          LOG_WARN("Failed to get partition ids", K(ret), K(table_id_));
        }
      } else {
      }  // do nothing. partition ids will be empty
    }
  }
  return ret;
}

int ObTableLocation::calc_func_value_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPLFuncValueNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  UNUSED(all_part);
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (calc_node->param_idx_ < 0 && calc_node->value_.is_invalid_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func value node should has param idx or value", K(ret));
  } else {
    bool value_satisfy = true;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < calc_node->param_value_.count(); ++idx) {
      const ObPLFuncValueNode::ParamValuePair& pair = calc_node->param_value_.at(idx);
      if (pair.param_idx_ < 0 || pair.param_idx_ >= params.count()) {
        ret = OB_INVALID_INDEX;
        LOG_WARN("Param idx error", K(ret), "param idx", pair.param_idx_, "count", params.count());
      } else if (!params.at(pair.param_idx_).is_equal(pair.obj_value_, CS_TYPE_BINARY)) {
        value_satisfy = false;
        break;
      } else {
      }  // do nothing
    }
    if (OB_FAIL(ret)) {
    } else if (!value_satisfy) {
      all_part = true;
    } else {
      ObObj result;
      if (calc_node->param_idx_ >= 0) {
        if (calc_node->param_idx_ >= params.count()) {
          ret = OB_INVALID_INDEX;
          LOG_WARN("Param idx error", K(ret), "param_idx", calc_node->param_idx_, "count", params.count());
        } else {
          result = params.at(calc_node->param_idx_);
        }
      } else {
        result = calc_node->value_;
      }
      if (OB_SUCC(ret)) {
        ObObj tmp;
        ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
        allocator.set_label("CalcFVPartIds");
        ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, calc_node->res_type_.get_collation_type());
        ObExprCtx expr_ctx;
        bool is_strict = false;
        int64_t part_idx = OB_INVALID_INDEX;
        if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
          LOG_WARN("Failed to wrap expr ctx", K(ret));
        } else if (OB_ISNULL(expr_ctx.my_session_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error. null session", K(expr_ctx.my_session_), K(ret));
        } else if (FALSE_IT(is_strict = is_strict_mode(expr_ctx.my_session_->get_sql_mode()))) {
          // can not reach here
        } else if (OB_FAIL(ObExprColumnConv::convert_with_null_check(
                       tmp, result, calc_node->res_type_, is_strict, cast_ctx))) {
          LOG_WARN("Cast result type failed", K(ret), K(result), "type", calc_node->res_type_.get_type());
        } else if (OB_FAIL(calc_partition_id_by_func_value(
                       expr_ctx, part_mgr, tmp, true, partition_ids, part_ids, &part_idx))) {
          LOG_WARN("Failed to calc partition id by func value", K(ret));
        } else {
          exec_ctx.get_part_row_manager().set_part_idx(part_idx);
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_column_value_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ParamStore& params, const ObPLColumnValueNode* calc_node, ObIArray<int64_t>& partition_ids, bool& all_part,
    const ObDataTypeCastParams& dtc_params, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  UNUSED(part_mgr);
  UNUSED(all_part);
  UNUSED(part_ids);
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (calc_node->param_idx_ < 0 && calc_node->value_.is_invalid_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func value node should has param idx or value", K(ret));
  } else {
    ObObj value;
    if (calc_node->param_idx_ >= 0) {
      if (calc_node->param_idx_ >= params.count()) {
        ret = OB_ERR_ILLEGAL_INDEX;
        LOG_WARN("Param idx error", K(ret), "param_idx", calc_node->param_idx_, "count", params.count());
      } else {
        value = params.at(calc_node->param_idx_);
      }
    } else {
      value = calc_node->value_;
    }
    if (OB_SUCC(ret)) {
      ObObj tmp;
      ObExprCtx expr_ctx;
      ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
      allocator.set_label("CalcCVPartIds");
      ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, calc_node->res_type_.get_collation_type());
      bool is_strict = false;
      if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
        LOG_WARN("Failed to wrap expr ctx", K(ret));
      } else if (OB_ISNULL(expr_ctx.my_session_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error. null session", K(expr_ctx.my_session_), K(ret));
      } else if (FALSE_IT(is_strict = is_strict_mode(expr_ctx.my_session_->get_sql_mode()))) {
        // can not reach here
      } else if (OB_FAIL(ObExprColumnConv::convert_with_null_check(
                     tmp, value, calc_node->res_type_, is_strict, cast_ctx))) {
        LOG_WARN("Cast result type failed", K(ret), K(value), "type", calc_node->res_type_);
      } else {
        ObNewRow result_row;
        // allocate obj first
        void* ptr = NULL;
        if (NULL == (ptr = allocator.alloc(sizeof(ObObj)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate obj", K(ret));
        } else {
          result_row.cells_ = new (ptr) ObObj();
          result_row.count_ = 1;
          result_row.cells_[0] = tmp;
          if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, result_row, partition_ids, part_ids))) {
            LOG_WARN("Calc partition id by row error", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_stored_expr(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    ObIArray<int64_t>& partition_ids, const ObDataTypeCastParams& dtc_params) const
{
  UNUSED(dtc_params);
  int ret = OB_SUCCESS;
  if (!is_partitioned_) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("Failed to push back partition id", K(ret));
    }
  } else if (num_values_ <= 0 || key_conv_exprs_.count() <= 0 ||
             (PARTITION_LEVEL_TWO == part_level_ && subkey_conv_exprs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Num values, (sub)key conv exprs count should not less than 1",
        K(ret),
        K_(num_values),
        K(key_conv_exprs_.count()),
        K_(part_level),
        K(subkey_conv_exprs_.count()));
  } else {
    ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
    allocator.set_label("CalcStoredExpr");
    ObExprCtx expr_ctx;
    ObNewRow part_row;
    ObNewRow subpart_row;
    ObNewRow part_conv_row;
    ObNewRow subpart_conv_row;
    const int64_t key_column_num = key_exprs_.count() / num_values_;
    const int64_t subkey_column_num = subkey_exprs_.count() / num_values_;
    const int64_t key_conv_count = key_conv_exprs_.count();
    const int64_t subkey_conv_count = subkey_conv_exprs_.count();

    // init expr ctx
    if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
      LOG_WARN("Failed to wrap expr ctx", K(ret));
    }

    // init all ObNewRow
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_row(allocator, key_column_num, part_row))) {
      LOG_WARN("Failed to init row", K(ret));
    }
    {}  // do nothing

    if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_row(allocator, subkey_column_num, subpart_row))) {
        LOG_WARN("Failed to init row", K(ret));
      } else {
      }  // do nothing
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_row(allocator, key_conv_count, part_conv_row))) {
      LOG_WARN("Failed to init part conv row", K(ret));
    } else if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(init_row(allocator, subkey_conv_count, subpart_conv_row))) {
        LOG_WARN("Failed to init subpart conv row", K(ret));
      }
    } else {
    }  // do nothing

    // calc ObNewRow for calc partition id
    exec_ctx.get_part_row_manager().reset();
    for (int64_t value_idx = 0; OB_SUCC(ret) && value_idx < num_values_; ++value_idx) {
      // Get part_row for calc conv expr
      if (OB_FAIL(calc_row(expr_ctx, key_exprs_, key_column_num, value_idx, part_row, part_row))) {
        LOG_WARN("Failed to calc part row", K(ret));
      } else if (OB_FAIL(calc_row(expr_ctx, key_conv_exprs_, key_conv_count, 0, part_row, part_conv_row))) {
        LOG_WARN("Failed to calc part conv row", K(ret));
      } else {
      }  // do nothing

      // calc partition id and add it to partition ids no duplicate
      if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == part_level_) {
        if (OB_FAIL(calc_row(expr_ctx, subkey_exprs_, subkey_column_num, value_idx, subpart_row, subpart_row))) {
          LOG_WARN("Failed to calc subpart_row", K(ret));
        } else if (OB_FAIL(
                       calc_row(expr_ctx, subkey_conv_exprs_, subkey_conv_count, 0, subpart_row, subpart_conv_row))) {
          LOG_WARN("Failed to calc subpart_conv_row", K(ret));
        } else {
        }  // do nothing
      }

      if (OB_SUCC(ret)) {
        if (PARTITION_LEVEL_ONE == part_level_) {
          if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, part_conv_row, partition_ids))) {
            LOG_WARN("Calc partition id by row error", K(ret));
          }
        } else if (PARTITION_LEVEL_TWO == part_level_) {
          ObSEArray<int64_t, 5> part_ids;
          if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, part_conv_row, part_ids))) {
            LOG_WARN("Calc partition id by row error", K(ret));
          } else if (OB_FAIL(
                         calc_partition_id_by_row(exec_ctx, part_mgr, subpart_conv_row, partition_ids, &part_ids))) {
            LOG_WARN("Calc partitioin id by row error", K(ret));
          } else {
          }  // do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected partition level", K(ret), K_(part_level));
        }
      }
      if (OB_SUCC(ret) && is_simple_insert_or_replace() && num_values_ > 1 && !is_virtual_table(ref_table_id_)) {
        int64_t part_idx = exec_ctx.get_part_row_manager().get_part_idx();
        if (OB_FAIL(exec_ctx.get_part_row_manager().add_row_for_part(part_idx, value_idx))) {
          LOG_WARN("add row for part id failed", K(ret), K(part_idx), K(partition_ids));
        }
      }
      // As no allocator in obj, this place did not destruct obj.
    }  // end of for value_idx
  }
  return ret;
}

// int ObTableLocation::calc_partition_ids_by_sub_select(ObExecContext &exec_ctx,
//                                                      common::ObPartMgr *part_mgr,
//                                                      ObIArray<int64_t> &partition_ids) const
//{
//  int ret = OB_SUCCESS;
//  if (!is_partitioned_) {
//    if (OB_FAIL(partition_ids.push_back(0))) {
//      LOG_WARN("Failed to add partition id", K(ret));
//    }
//  } else if (same_part_with_select_) {
//    if (OB_FAIL(calc_sub_select_partition_ids(exec_ctx, partition_ids))) {
//      LOG_WARN("assign subselect_part ids failed", K(ret));
//    }
//  } else if (PARTITION_LEVEL_ONE == part_level_) {
//    if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids))) {
//      LOG_WARN("Failed to get all part ids", K(ret));
//    }
//  } else if (PARTITION_LEVEL_TWO == part_level_) {
//    ObSEArray<int64_t, 5> part_ids;
//    if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, part_ids))) {
//      LOG_WARN("Failed to get all part ids", K(ret));
//    } else if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids, &part_ids))) {
//      LOG_WARN("Failed to get all subpart ids", K(ret));
//    } else { }
//  } else {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("Unkown part level", K(ret), K(part_level_), K(is_partitioned_));
//  }
//  return ret;
//}

int ObTableLocation::calc_partition_ids_by_ranges(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    const ObIArray<ObNewRange*>& ranges, const bool is_all_single_value_ranges, ObIArray<int64_t>& partition_ids,
    bool& all_part, const ObIArray<int64_t>* part_ids, const ObSqlExpression* gen_col_expr) const
{
  int ret = OB_SUCCESS;
  bool get_part_by_range = false;
  all_part = false;
  if (OB_ISNULL(part_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part mgr should not be NULL", K(ret));
  } else if (NULL != gen_col_expr) {
    get_part_by_range = false;
    all_part = !is_all_single_value_ranges;
  } else if (NULL == part_ids) {  // PARTITION_LEVEL_ONE
    if (!is_range_part(part_type_) || (PARTITION_FUNC_TYPE_RANGE == part_type_ && !is_col_part_expr_) ||
        (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ && !is_valid_range_columns_part_range_)) {
      if (!is_all_single_value_ranges) {
        all_part = true;
      }
    } else {
      get_part_by_range = true;
    }
  } else {  // PARTITION_LEVEL_TWO
    if (!is_range_part(subpart_type_) || (PARTITION_FUNC_TYPE_RANGE == subpart_type_ && !is_col_subpart_expr_) ||
        (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ && !is_valid_range_columns_subpart_range_)) {
      if (!is_all_single_value_ranges) {
        all_part = true;
      }
    } else {
      get_part_by_range = true;
    }
  }

  if (OB_SUCC(ret) && !all_part) {
    if (get_part_by_range) {
      if (part_ids != NULL && use_range_part_opt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("use range part opt with partition level two", K(ret));
      } else if (use_range_part_opt_ && 1 == ranges.count() && ranges.at(0)->is_single_rowkey()) {
        bool insert_or_replace = is_simple_insert_or_replace();
        if (OB_FAIL(get_range_part(ranges.at(0), insert_or_replace, partition_ids))) {
          LOG_WARN("fail to get range part", K(ret));
        }
      } else if (OB_FAIL(get_part_ids_by_ranges(part_mgr, ranges, partition_ids, part_ids))) {
        LOG_WARN("Failed to get part ids by ranges", K(ret));
      }
    } else {
      ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
      allocator.set_label("CalcByRanges");
      ObExprCtx expr_ctx;
      if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
        LOG_WARN("Failed to wrap expr ctx", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
        ObNewRange* range = ranges.at(i);
        ObNewRow input_row;
        if (OB_ISNULL(range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid range", K(ret));
        } else {
          input_row.cells_ = const_cast<ObObj*>(range->start_key_.get_obj_ptr());
          input_row.count_ = (range->start_key_.get_obj_cnt());
          if (NULL != gen_col_expr) {
            ObObj gen_col_val;
            if (OB_FAIL(gen_col_expr->calc(expr_ctx, input_row, gen_col_val))) {
              LOG_WARN("Failed to get column exprs", K(ret));
            } else {
              ObNewRow result_row;
              result_row.cells_ = &gen_col_val;
              result_row.count_ = 1;
              if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, result_row, partition_ids, part_ids))) {
                LOG_WARN("Failed to calc partition id by row", K(ret));
              }
            }
          } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, input_row, partition_ids, part_ids))) {
            LOG_WARN("Calc partition id by row error", K(ret));
          } else {
          }  // do nothing
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::get_part_ids_by_ranges(common::ObPartMgr* part_mgr, const ObIArray<ObNewRange*>& ranges,
    ObIArray<int64_t>& partition_ids, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part mgr should not be NULL", K(ret));
  } else if (NULL == part_ids) {
    if (OB_FAIL(ObPartMgrAD::get_part(
            part_mgr, ref_table_id_, PARTITION_LEVEL_ONE, -1, default_asc_direction(), ranges, partition_ids))) {
      LOG_WARN("Failed to get part", K(ret));
    }
  } else {
    ObSEArray<int64_t, 3> subpart_ids;
    for (int64_t p_idx = 0; OB_SUCC(ret) && p_idx < part_ids->count(); ++p_idx) {
      subpart_ids.reuse();
      int64_t part_id = part_ids->at(p_idx);
      if (OB_FAIL(ObPartMgrAD::get_part(
              part_mgr, ref_table_id_, PARTITION_LEVEL_TWO, part_id, default_asc_direction(), ranges, subpart_ids))) {
        LOG_WARN("get partition id from part mgr error", K(ret));
      } else {
        for (int64_t s_idx = 0; OB_SUCC(ret) && s_idx < subpart_ids.count(); ++s_idx) {
          if (OB_FAIL(
                  partition_ids.push_back(generate_phy_part_id(part_id, subpart_ids.at(s_idx), PARTITION_LEVEL_TWO)))) {
            LOG_WARN("Failed to add partition ids", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_id_by_func_value(ObExprCtx& expr_ctx, common::ObPartMgr* part_mgr,
    const ObObj& func_value, const bool calc_oracle_hash, ObIArray<int64_t>& partition_ids,
    const ObIArray<int64_t>* part_ids, int64_t* part_idx) const
{
  int ret = OB_SUCCESS;
  ObObj result = func_value;
  UNUSED(expr_ctx);
  if (OB_SUCC(ret) && !is_inner_table(ref_table_id_)) {
    bool hash_type = false;
    bool hash_type_v2 = false;
    if (NULL == part_ids) {
      hash_type = (PARTITION_FUNC_TYPE_HASH == part_type_);
      hash_type_v2 = (PARTITION_FUNC_TYPE_HASH_V2 == part_type_);
    } else {
      hash_type = (PARTITION_FUNC_TYPE_HASH == subpart_type_);
      hash_type_v2 = (PARTITION_FUNC_TYPE_HASH_V2 == subpart_type_);
    }
    if (hash_type || hash_type_v2) {
      if (hash_type) {
        if (share::is_oracle_mode()) {
          if (calc_oracle_hash && OB_FAIL(ObExprFuncPartOldHash::calc_value_for_oracle(&func_value, 1, result))) {
            LOG_WARN("Failed to calc hash value oracle mode", K(ret));
          }
        } else {
          if (OB_FAIL(ObExprFuncPartOldHash::calc_value_for_mysql(func_value, result))) {
            LOG_WARN("Failed to calc hash value mysql mode", K(ret));
          }
        }
      } else if (hash_type_v2) {
        if (share::is_oracle_mode()) {
          if (calc_oracle_hash && OB_FAIL(ObExprFuncPartHash::calc_value_for_oracle(&func_value, 1, result))) {
            LOG_WARN("Failed to calc hash value oracle mode", K(ret));
          }
        } else {
          if (OB_FAIL(ObExprFuncPartHash::calc_value_for_mysql(func_value, result))) {
            LOG_WARN("Failed to calc hash value mysql mode", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_virtual_table(ref_table_id_) || is_inner_table(ref_table_id_)) {
    // only one level and part expr has part type name, like hash(), key()
    int64_t calc_result = 0;
    int64_t part_id = 0;
    if (OB_FAIL(result.get_int(calc_result))) {
      LOG_WARN("Fail to get int64 from result", K(result), K(ret));
    } else if (OB_INVALID_PARTITION_ID == calc_result && is_virtual_table(ref_table_id_)) {
      // addr is invalid, and partition id calculated by this addr is also invalid, so, do nothing
    } else if (OB_FAIL(get_part_id_by_mod(calc_result, part_num_, part_id))) {
      LOG_WARN("Fail to calculate part id", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(partition_ids, part_id, part_idx))) {
      LOG_WARN("Fail to add partition id into array no duplicate", K(ret));
    } else {
    }
  } else {
    bool insert_or_replace = is_simple_insert_or_replace();
    if (NULL == part_ids) {
      if (OB_FAIL(ObPartMgrAD::get_part(part_mgr,
              ref_table_id_,
              PARTITION_LEVEL_ONE,
              part_type_,
              insert_or_replace,
              -1,
              result,
              partition_ids,
              part_idx))) {
        LOG_WARN("Failed to get part id", K(ret));
      }
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
        if (OB_FAIL(ObPartMgrAD::get_part(part_mgr,
                ref_table_id_,
                PARTITION_LEVEL_TWO,
                subpart_type_,
                insert_or_replace,
                part_ids->at(idx),
                result,
                partition_ids,
                part_idx))) {
          LOG_WARN("Failed to get part id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_rowkey(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const ObIArray<ObRowkey>& rowkeys, ObIArray<int64_t>& part_ids, ObIArray<RowkeyArray>& rowkey_lists) const
{
  int ret = OB_SUCCESS;
  ObNewRow cur_row;
  RowkeyArray rowkey_list;
  ObSEArray<int64_t, 1> tmp_part_ids;
  ObNewRow* part_row = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
    cur_row.cells_ = const_cast<ObObj*>(rowkeys.at(i).get_obj_ptr());
    cur_row.count_ = rowkeys.at(i).get_obj_cnt();
    if (OB_FAIL(part_projector_.calc_part_row(stmt_type_, exec_ctx, cur_row, part_row)) || OB_ISNULL(part_row)) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("calc part row failed", K(ret));
    } else if (PARTITION_LEVEL_ONE == part_level_) {
      if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, part_ids))) {
        LOG_WARN("calc partition id by row failed", K(ret));
      }
    } else {
      tmp_part_ids.reuse();
      if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, tmp_part_ids))) {
        LOG_WARN("calc partition id by row failed", K(ret));
      } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, *part_row, part_ids, &tmp_part_ids))) {
        LOG_WARN("calc sub partition id by row failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t part_idx = exec_ctx.get_part_row_manager().get_part_idx();
      if (OB_UNLIKELY(part_idx < rowkey_lists.count())) {
        if (OB_FAIL(rowkey_lists.at(part_idx).push_back(i))) {
          LOG_WARN("store rowkey to rowkey_lists failed", K(ret));
        }
      } else if (OB_LIKELY(rowkey_lists.count() == part_idx)) {
        rowkey_list.reuse();
        if (OB_FAIL(rowkey_list.push_back(i))) {
          LOG_WARN("store rowkey to rowkey_list failed", K(ret));
        } else if (OB_FAIL(rowkey_lists.push_back(rowkey_list))) {
          LOG_WARN("store rowkey list failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_idx is invalid", K(part_idx), K(rowkey_lists.count()));
      }
    }
  }
  return ret;
}

int ObTableLocation::get_list_part(
    const ObNewRow& row, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids, int64_t* part_idx) const
{
  int ret = OB_SUCCESS;
  ObListPartMapValue* value = NULL;
  ObListPartMapKey key;
  key.row_ = row;
  ret = list_part_map_.get_refactored(key, value);
  if (ret == OB_HASH_NOT_EXIST) {
    LOG_TRACE("get list part not exist", K(key));
    if (list_default_part_id_ == OB_INVALID_ID) {
      if (insert_or_replace) {
        ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(partition_ids, list_default_part_id_, part_idx))) {
        LOG_WARN("Failed to add var to array no dup", K(ret));
      }
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail to get_refactored", K(ret), K(inited_));
  } else {
    if (OB_FAIL(add_var_to_array_no_dup(partition_ids, value->part_id_, part_idx))) {
      LOG_WARN("Failed to add var to array no dup", K(ret));
    }
  }

  return ret;
}

int ObTableLocation::get_list_part(
    ObObj& obj, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids, int64_t* part_idx) const
{
  ObNewRow row;
  row.assign(&obj, 1);
  return get_list_part(row, insert_or_replace, partition_ids, part_idx);
}

int ObTableLocation::get_hash_part(
    const ObObj& func_value, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids, int64_t* part_idx) const
{
  int ret = OB_SUCCESS;
  int64_t partition_idx = 0;
  int64_t val = 0;
  int64_t partition_id = 0;
  UNUSED(insert_or_replace);
  if (OB_FAIL(func_value.get_int(val))) {
    LOG_WARN("failed to get int val", K(ret));
  } else if (val < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Val should not be less than 0", K(ret), K(val));
  } else if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(val, first_part_num_, partition_idx))) {
    LOG_WARN("Failed to get hash part", K(ret));
  } else if (partition_idx < 0 || partition_idx > first_part_num_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_idx), K(first_part_num_));
  } else if (OB_FAIL(get_part_id_from_part_idx(partition_idx, partition_id))) {
    LOG_WARN("failed to get part id from part idx", K(ret), K(partition_idx));
  } else if (OB_FAIL(add_var_to_array_no_dup(partition_ids, partition_id, part_idx))) {
    LOG_WARN("Failed to add var to array no dup", K(ret));
  }
  LOG_TRACE("optimization in get_hash_part",
      K(first_part_num_),
      K(insert_or_replace),
      K(fast_calc_part_opt_),
      K(ret),
      K(func_value),
      K(partition_idx),
      K(*part_idx),
      K(inited_));
  return ret;
}

int ObTableLocation::get_range_part(
    const ObNewRow& row, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids, int64_t* part_idx) const
{
  int ret = OB_SUCCESS;
  int64_t high = partition_num_ - 1;
  int64_t low = 0;
  int64_t mid = 0;
  int64_t res = high;
  const ObObj& cur_obj = row.get_cell(0);
  if (1 != row.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get range part with optimization should have 1 column", K(ret), K(row.get_count()));
  } else if (cur_obj.is_null()) {
    // For Insert or replace stmt, if no partition, report error
    if (!range_obj_arr_[high].is_max_value() && insert_or_replace) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_USER_WARN(OB_NO_PARTITION_FOR_GIVEN_VALUE);
    }
  } else {
    while (low <= high) {
      mid = (low + high) / 2;
      if (ObObjCmpFuncs::compare_oper_nullsafe(
              range_obj_arr_[mid], cur_obj, range_obj_arr_[mid].get_collation_type(), CO_GT)) {
        high = mid - 1;
        res = mid;
      } else {
        low = mid + 1;
      }
    }  // while
    if (res == partition_num_ - 1) {
      if (!range_obj_arr_[res].is_max_value() &&
          !ObObjCmpFuncs::compare_oper_nullsafe(
              range_obj_arr_[res], cur_obj, range_obj_arr_[mid].get_collation_type(), CO_GT)) {
        if (insert_or_replace) {  // For Insert or replace stmt, if no partition, report error
          ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
          LOG_USER_WARN(OB_NO_PARTITION_FOR_GIVEN_VALUE);
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_var_to_array_no_dup(partition_ids, range_part_id_arr_[res], part_idx))) {
    LOG_WARN("Failed to add var to array no dup", K(ret));
  }
  LOG_TRACE("optimization in get_range_part",
      K(partition_num_),
      K(insert_or_replace),
      K(ret),
      K(row),
      K(cur_obj),
      K(res),
      K(range_part_id_arr_[res]),
      K(*part_idx),
      K(partition_ids));
  return ret;
}

int ObTableLocation::get_range_part(
    const ObNewRange* range, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids) const
{
  int ret = OB_SUCCESS;
  ObNewRow input_row;
  int64_t part_idx = 0;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid range", K(ret));
  } else {
    input_row.cells_ = const_cast<ObObj*>(range->start_key_.get_obj_ptr());
    input_row.count_ = (range->start_key_.get_obj_cnt());
    if (OB_FAIL(get_range_part(input_row, insert_or_replace, partition_ids, &part_idx))) {
      LOG_WARN("failed to get range part by ObNewRow", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_id_by_row(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, ObNewRow& row,
    ObIArray<int64_t>& partition_ids, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  ObObj func_result;
  ObTaskExecutorCtx* tctx = NULL;
  ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
  allocator.set_label("CalcByRow");
  ObExprCtx expr_ctx;
  if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else if (is_virtual_table(ref_table_id_) && OB_ISNULL(tctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec ctx is NULL", K(ret));
  } else if (is_virtual_table(ref_table_id_) && OB_FAIL(tctx->init_calc_virtual_part_id_params(ref_table_id_))) {
    LOG_WARN("fail to init_calc_virtual_part_id_params", K(ret), K(ref_table_id_));
    tctx->reset_calc_virtual_part_id_params();
  } else {
    bool insert_or_replace = is_simple_insert_or_replace();
    bool range_columns = false;
    int64_t part_idx = OB_INVALID_INDEX;
    if (NULL == part_ids) {
      if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type_) {
        part_projector_.project_part_row(PARTITION_LEVEL_ONE, row);
        if (related_part_expr_idx_ == OB_INVALID_INDEX) {
          if (use_list_part_map_) {
            if (OB_FAIL(get_list_part(row, insert_or_replace, partition_ids, &part_idx))) {
              LOG_WARN("fail to get list part", K(row), K(ret));
            }
          } else if (use_range_part_opt_) {
            if (OB_FAIL(get_range_part(row, insert_or_replace, partition_ids, &part_idx))) {
              LOG_WARN("fail to get range part", K(row), K(ret));
            }
          } else {
            if (OB_FAIL(ObPartMgrAD::get_part(part_mgr,
                    ref_table_id_,
                    PARTITION_LEVEL_ONE,
                    part_type_,
                    insert_or_replace,
                    -1,
                    row,
                    partition_ids,
                    &part_idx))) {
              LOG_WARN("Failed to get part id", K(ret), K(row));
            }
          }
          if (OB_SUCC(ret)) {
            exec_ctx.get_part_row_manager().set_part_idx(part_idx);
          }
        } else {
          if (OB_ISNULL(part_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part_expr_ should not be NULL", K(ret), K(table_id_), K(ref_table_id_));
          } else if (OB_FAIL(part_expr_->calc(expr_ctx, row, func_result))) {
            LOG_WARN("Failed to calc hash expr", K(ret), K(row), K(ref_table_id_));
          } else {
            if (use_list_part_map_) {
              if (OB_FAIL(get_list_part(func_result, insert_or_replace, partition_ids, &part_idx))) {
                LOG_WARN("fail to get list part", K(ret));
              }
            } else if (use_range_part_opt_) {
              if (OB_FAIL(get_range_part(row, insert_or_replace, partition_ids, &part_idx))) {
                LOG_WARN("fail to get range part", K(ret));
              }
            } else {
              if (OB_FAIL(ObPartMgrAD::get_part(part_mgr,
                      ref_table_id_,
                      PARTITION_LEVEL_ONE,
                      part_type_,
                      insert_or_replace,
                      -1,
                      func_result,
                      partition_ids,
                      &part_idx))) {
                LOG_WARN("Failed to get part id", K(ret), K(func_result));
              }
            }
            if (OB_SUCC(ret)) {
              exec_ctx.get_part_row_manager().set_part_idx(part_idx);
            }
          }
        }
        range_columns = true;
      } else if (OB_ISNULL(part_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_expr_ should not be NULL", K(ret), K(table_id_), K(ref_table_id_));
      } else if (OB_FAIL(part_expr_->calc(expr_ctx, row, func_result))) {
        LOG_WARN("Failed to calc hash expr", K(ret), K(row), K(ref_table_id_));
      } else {
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ ||
               PARTITION_FUNC_TYPE_LIST_COLUMNS == subpart_type_) {
      part_projector_.project_part_row(PARTITION_LEVEL_TWO, row);
      for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
        if (OB_FAIL(ObPartMgrAD::get_part(part_mgr,
                ref_table_id_,
                PARTITION_LEVEL_TWO,
                subpart_type_,
                insert_or_replace,
                part_ids->at(idx),
                row,
                partition_ids,
                &part_idx))) {
          LOG_WARN("Failed to get part id", K(ret));
        }
      }
      range_columns = true;
      if (OB_SUCC(ret)) {
        exec_ctx.get_part_row_manager().set_part_idx(part_idx);
      }
    } else if (OB_ISNULL(subpart_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpart_expr_ should not be NULL", K(ret));
    } else if (OB_FAIL(subpart_expr_->calc(expr_ctx, row, func_result))) {
      LOG_WARN("Failed to calc hash expr", K(ret), K(row), K(ref_table_id_));
    } else {
    }  // do nothing
    if (OB_FAIL(ret) || range_columns) {
    } else {
      if (use_hash_part_opt_) {
        if (OB_FAIL(get_hash_part(func_result, insert_or_replace, partition_ids, &part_idx))) {
          LOG_WARN("fail to get hash part", K(ret));
        }
      } else if (OB_FAIL(calc_partition_id_by_func_value(
                     expr_ctx, part_mgr, func_result, false, partition_ids, part_ids, &part_idx))) {
        LOG_WARN("Failed to calc partiton id by func value", K(ret));
      }

      if (OB_SUCC(ret)) {
        exec_ctx.get_part_row_manager().set_part_idx(part_idx);
      }
    }
    if (is_virtual_table(ref_table_id_)) {
      tctx->reset_calc_virtual_part_id_params();
    }
  }
  return ret;
}

int ObTableLocation::get_all_part_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
    ObIArray<int64_t>& partition_ids, const ObIArray<int64_t>* part_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_partitioned_) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("Failed to add partition id", K(ret));
    }
  } else if (is_partitioned_ && is_virtual_table(ref_table_id_)) {
    if (OB_FAIL(get_vt_partition_id(exec_ctx, ref_table_id_, &partition_ids, NULL))) {
      LOG_WARN("Failed to get virtual table partition ids", K_(ref_table_id), K(ret));
    }
  } else if (is_inner_table(ref_table_id_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num_; ++i) {
      if (OB_FAIL(partition_ids.push_back(i))) {
        LOG_WARN("Push partition id to partition_ids error", "partition id", i, K(ret));
      }
    }
  } else {
    if (NULL == part_ids) {
      if (OB_FAIL(ObPartMgrAD::get_all_part(
              part_mgr, ref_table_id_, PARTITION_LEVEL_ONE, -1, default_asc_direction(), partition_ids))) {
        LOG_WARN("Failed to get all partition ids", K(ret));
      }
    } else {
      ObSEArray<int64_t, 3> sub_part_ids;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
        sub_part_ids.reuse();
        int64_t part_id = part_ids->at(idx);
        if (OB_FAIL(ObPartMgrAD::get_all_part(part_mgr,
                ref_table_id_,
                PARTITION_LEVEL_TWO,
                part_ids->at(idx),
                default_asc_direction(),
                sub_part_ids))) {
          LOG_WARN("Failed to get all part", K(ret));
        } else {
          for (int64_t s_idx = 0; OB_SUCC(ret) && s_idx < sub_part_ids.count(); ++s_idx) {
            int64_t phy_part_id = generate_phy_part_id(part_id, sub_part_ids.at(s_idx), PARTITION_LEVEL_TWO);
            if (OB_FAIL(add_var_to_array_no_dup(partition_ids, phy_part_id))) {
              LOG_WARN("Failed to add part id to partition_ids", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::deal_dml_partition_selection(int64_t part_id) const
{
  int ret = OB_SUCCESS;
  if (!part_hint_ids_.empty()) {
    bool found = false;
    for (int j = 0; !found && j < part_hint_ids_.count(); j++) {
      if (part_id == part_hint_ids_.at(j)) {
        found = true;
      }
    }
    if (!found) {
      ret = OB_PARTITION_NOT_MATCH;
      LOG_DEBUG("Partition not match", K(ret), K(part_id));
    }
  }
  return ret;
}

int ObTableLocation::deal_partition_selection(ObIArray<int64_t>& part_ids) const
{
  int ret = OB_SUCCESS;
  if (is_simple_insert_or_replace()) {
    // if insert partition can be resolved accurately, deal partition selection in the plan generated phase
    if (!part_get_all_) {
      for (int i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
        if (OB_FAIL(deal_dml_partition_selection(part_ids.at(i)))) {
          LOG_WARN("deal insert partition selection failed", K(ret));
        }
      }
    }
  } else {  // other stmt_types
    ObSEArray<int64_t, 16> tmp_ids;
    for (int i = 0; OB_SUCC(ret) && i < part_ids.count(); i++) {
      if (OB_FAIL(tmp_ids.push_back(part_ids.at(i)))) {
        LOG_WARN("Push partition id to part_ids error", "partition id", part_ids.at(i), K(ret));
      }
    }
    part_ids.reset();
    bool found = false;
    for (int tmp_idx = 0; OB_SUCC(ret) && tmp_idx < tmp_ids.count(); tmp_idx++) {
      found = false;
      for (int hint_idx = 0; !found && hint_idx < part_hint_ids_.count(); hint_idx++) {
        if (tmp_ids.at(tmp_idx) == part_hint_ids_.at(hint_idx)) {
          found = true;
        }
      }
      if (found && OB_FAIL(part_ids.push_back(tmp_ids.at(tmp_idx)))) {
        LOG_WARN("Push partition id to part_ids error", "partition id", tmp_ids.at(tmp_idx), K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::add_and_node(ObPartLocCalcNode* l_node, ObPartLocCalcNode*& r_node)
{
  int ret = OB_SUCCESS;
  if (NULL == l_node && NULL == r_node) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("l_node and r_node should not both be NULL", K(ret));
  } else if (NULL == l_node || NULL == r_node) {
    r_node = (NULL == l_node) ? r_node : l_node;
  } else {
    ObPLAndNode* and_node = NULL;
    if (NULL == (and_node = static_cast<ObPLAndNode*>(
                     ObPartLocCalcNode::create_part_calc_node(allocator_, calc_nodes_, ObPartLocCalcNode::CALC_AND)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to allocate part calc node", K(ret));
    } else {
      and_node->left_node_ = l_node;
      and_node->right_node_ = r_node;
      r_node = and_node;
    }
  }
  return ret;
}

int ObTableLocation::add_or_node(ObPartLocCalcNode* l_node, ObPartLocCalcNode*& r_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("l_node should not be NULL", K(ret));
  } else if (NULL == r_node) {
    r_node = l_node;
  } else {
    ObPLOrNode* or_node = NULL;
    if (NULL == (or_node = static_cast<ObPLOrNode*>(
                     ObPartLocCalcNode::create_part_calc_node(allocator_, calc_nodes_, ObPartLocCalcNode::CALC_OR)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to allocate part calc node", K(ret));
    } else {
      or_node->left_node_ = l_node;
      or_node->right_node_ = r_node;
      r_node = or_node;
    }
  }
  return ret;
}

///////private functions only use sql_expression_factory_ and expr_op_factory_.////
///////These functions can moved to nother place.                              ////
int ObTableLocation::record_insert_part_info(ObInsertStmt& insert_stmt, ObSQLSessionInfo* session_info,
    const ObIArray<ColumnItem>& partition_columns, IKeyExprs& key_exprs, IKeyExprs& key_conv_exprs)
{
  int ret = OB_SUCCESS;

  const ObIArray<ObColumnRefRawExpr*>* value_desc = NULL;
  const ObIArray<ObColumnRefRawExpr*>* table_columns = insert_stmt.get_table_columns();
  const ObIArray<ObRawExpr*>* value_vector = NULL;
  const int64_t part_column_count = partition_columns.count();

  if (NULL != table_columns && table_columns->count() > 1) {
    if (0 == table_columns->at(0)->get_table_name().case_compare("sg1")) {
      LOG_DEBUG("shaoge table");
    }
  }

  ObSEArray<ObColumnRefRawExpr*, 4> value_desc_heap_table;
  ObSEArray<ObRawExpr*, 4> value_vector_heap_table;
  if (insert_stmt.get_part_generated_col_dep_cols().count() == 0) {
    value_desc = &insert_stmt.get_values_desc();
    value_vector = &insert_stmt.get_value_vectors();
  } else {
    // generated col as part key in heap table, need get all dep col as value desc
    OZ(insert_stmt.get_values_desc_for_heap_table(value_desc_heap_table));
    OZ(insert_stmt.get_value_vectors_for_heap_table(value_vector_heap_table));
    OX(value_desc = &value_desc_heap_table);
    OX(value_vector = &value_vector_heap_table);
  }
  if (OB_SUCC(ret)) {
    const int64_t value_desc_count = value_desc->count();
    const int64_t value_count = value_vector->count();

    ObSEArray<int64_t, 4> value_need_idx;
    // construct pkey idx
    if (insert_stmt.is_all_const_values()) {
      for (int64_t key_idx = 0; OB_SUCC(ret) && key_idx < part_column_count;
           key_idx++) {  // column num of partition key
        uint64_t column_id = partition_columns.at(key_idx).column_id_;
        if (OB_FAIL(add_key_col_idx_in_val_desc(*value_desc, column_id, value_need_idx))) {
          LOG_WARN("Failed to get key column idx", K(ret));
        }
      }
    } else {
      for (int64_t value_idx = 0; OB_SUCC(ret) && value_idx < value_desc_count; ++value_idx) {
        if (OB_ISNULL(value_desc->at(value_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value desc expr is null");
        } else if (!value_desc->at(value_idx)->is_auto_increment()) {
          if (OB_FAIL(value_need_idx.push_back(value_idx))) {
            LOG_WARN("Failed to add value idx", K(ret));
          }
        }
      }
    }

    const int64_t num_keys = value_need_idx.count();
    // construct value_row_desc with value_need_idx
    RowDesc value_row_desc;
    RowDesc extra_row_desc;
    ObColumnRefRawExpr* expr = NULL;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(value_row_desc.init())) {
        LOG_WARN("Value row desc init error", K(ret));
      } else if (OB_FAIL(extra_row_desc.init())) {
        LOG_WARN("Extra row desc init error", K(ret));
      }
    }
    for (int64_t need_idx = 0; OB_SUCC(ret) && need_idx < num_keys; ++need_idx) {
      if (OB_FAIL(value_desc->at(value_need_idx.at(need_idx), expr))) {
        LOG_WARN("value need idx error", K(ret), K(need_idx), "value desc cnt", value_desc->count());
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (OB_FAIL(value_row_desc.add_column(expr))) {
        LOG_WARN("Failed to add column", K(ret));
      } else if (OB_FAIL(expr->add_flag(IS_COLUMNLIZED))) {
        LOG_WARN("Failed to add IS_COLUMNLIZED flag", K(ret));
      } else {
      }  // do nothing
    }
    ObColumnRefRawExpr* value_column = NULL;
    ObColumnRefRawExpr* table_column = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < value_row_desc.get_columns().count(); i++) {
      OV(value_row_desc.get_column(i)->get_expr_type() == T_REF_COLUMN);
      OV(OB_NOT_NULL(value_column = static_cast<ObColumnRefRawExpr*>(value_row_desc.get_column(i))));
      for (int64_t j = 0; OB_SUCC(ret) && j < table_columns->count(); j++) {
        OV(OB_NOT_NULL(table_column = table_columns->at(j)));
        if (value_column->get_column_id() == table_column->get_column_id()) {
          OZ(extra_row_desc.add_column(table_column));
        }
      }
    }
    OV(value_row_desc.get_columns().count() == extra_row_desc.get_columns().count(),
        OB_ERR_UNEXPECTED,
        value_row_desc.get_columns(),
        extra_row_desc.get_columns());

    // Add value_expr to key_exprs
    if (OB_FAIL(ret)) {
    } else if (0 == value_desc_count || 0 != (value_count % value_desc_count)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_OPT_LOG(WARN, "invalid row desc", K(ret), K(value_count), K(value_desc_count));
    } else {
      num_values_ = value_count / value_desc_count;
      ObRawExpr* value_expr = NULL;
      for (int64_t value_idx = 0; OB_SUCC(ret) && !part_get_all_ && value_idx < num_values_; ++value_idx) {
        for (int64_t key_idx = 0; OB_SUCC(ret) && !part_get_all_ && key_idx < num_keys; ++key_idx) {
          if (OB_FAIL(value_vector->at(value_need_idx.at(key_idx) + value_idx * value_desc_count, value_expr))) {
            LOG_WARN("Failed to get value expr", K(ret));
          } else if (OB_NOT_NULL(value_expr) &&
                     (value_expr->has_flag(CNT_RAND_FUNC) || value_expr->has_flag(CNT_STATE_FUNC))) {
            part_get_all_ = true;
          } else if (OB_FAIL(add_key_expr_with_row_desc(key_exprs, value_row_desc, value_expr))) {
            LOG_WARN("Failed to add key expr", K(ret));
          } else {
          }
        }  // end of for key_idx
      }    // end of for value_idx
    }

    // Get key column conv expression
    for (int64_t key_idx = 0; OB_SUCC(ret) && key_idx < partition_columns.count(); ++key_idx) {
      uint64_t column_id = partition_columns.at(key_idx).column_id_;
      if (OB_FAIL(add_key_conv_expr(
              insert_stmt, session_info, value_row_desc, extra_row_desc, column_id, key_conv_exprs))) {
        LOG_WARN("Add key conv exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && !part_get_all_ && NONE_FAST_CALC == fast_calc_part_opt_ && share::is_oracle_mode()) {
      ObRawExpr* value_expr = NULL;
      ObRawExpr* col_conv_expr = NULL;
      ObColumnRefRawExpr* col_expr = NULL;
      if (1 == num_values_ && 1 == num_keys && 1 == partition_columns.count() && 1 == key_exprs_.count()) {
        if (OB_FAIL(value_vector->at(value_need_idx.at(0), value_expr))) {
          LOG_WARN("Failed to get value expr", K(ret));
        } else {
          const ObIArray<ObRawExpr*>& column_conv_exprs = insert_stmt.get_column_conv_functions();
          const ObIArray<ObColumnRefRawExpr*>* table_column = insert_stmt.get_table_columns();
          bool found = false;
          for (int64_t idx = 0; OB_SUCC(ret) && !found && idx < table_column->count(); ++idx) {
            if (OB_ISNULL(col_expr = table_column->at(idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get invalid column item", K(ret), K(col_expr), K(idx));
            } else if (partition_columns.at(0).column_id_ != col_expr->get_column_id()) {
              // do nothing
            } else if (OB_ISNULL(col_conv_expr = column_conv_exprs.at(idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Column conv expr should not be NULL", K(ret));
            } else {
              found = true;
            }
          }
          if (found && OB_SUCC(ret)) {
            const ObPostExprItem& key_item = key_exprs_.at(0)->get_expr_items().at(0);
            if (IS_DATATYPE_OR_QUESTIONMARK_OP(key_item.get_item_type())) {
              if (value_expr->get_data_type() == col_conv_expr->get_data_type()) {
                fast_calc_part_opt_ = UNKNOWN_FAST_CALC;
              } else if (ob_is_string_tc(value_expr->get_data_type()) &&
                         ob_is_string_tc(col_conv_expr->get_data_type())) {
                fast_calc_part_opt_ = UNKNOWN_FAST_CALC;
              }
            }
          }
        }
      }
      LOG_TRACE("check can fast calc id opt ", K(fast_calc_part_opt_), K(ret));
    }
    if (NONE_FAST_CALC == fast_calc_part_opt_) {
      fast_calc_part_opt_ = CANNOT_FAST_CALC;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(clear_columnlized_in_row_desc(value_row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      }
    }
  }

  return ret;
}

int ObTableLocation::gen_insert_part_row_projector(const ObRawExpr* partition_raw_expr,
    const ObIArray<ColumnItem>& partition_columns, ObPartitionLevel part_level, ObPartitionFuncType part_type)
{
  int ret = OB_SUCCESS;
  if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    RowDesc part_row_desc;
    if (OB_FAIL(part_row_desc.init())) {
      LOG_WARN("init part row desc failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_columns.count(); ++i) {
      uint64_t column_id = partition_columns.at(i).column_id_;
      ObRawExpr* expr = partition_columns.at(i).expr_;
      if (OB_FAIL(part_row_desc.add_column(expr))) {
        LOG_WARN("add column to part row desc failed", K(ret), K(i), K(column_id), KPC(expr));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_projector_.init_part_projector(partition_raw_expr, part_level, part_row_desc))) {
        LOG_WARN("init part row projector failed", K(ret), K(part_row_desc), KPC(partition_raw_expr));
      } else if (OB_FAIL(clear_columnlized_in_row_desc(part_row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::add_key_col_idx_in_val_desc(
    const ObIArray<ObColumnRefRawExpr*>& value_desc, const uint64_t column_id, ObIArray<int64_t>& value_need_idx)
{
  int ret = OB_SUCCESS;
  const int64_t value_desc_count = value_desc.count();
  bool found = false;
  for (int64_t idx = 0; OB_SUCC(ret) && !found && idx < value_desc_count; ++idx) {
    if (OB_ISNULL(value_desc.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row desc");
    } else if (value_desc.at(idx)->get_column_id() != column_id) {
      // do nothing
    } else if (OB_FAIL(value_need_idx.push_back(idx))) {
      LOG_WARN("Failed to add key col idx", K(ret));
    } else {
      found = true;
    }
  }
  return ret;
}

int ObTableLocation::add_key_conv_expr(ObInsertStmt& insert_stmt, ObSQLSessionInfo* session_info,
    RowDesc& value_row_desc, RowDesc& extra_row_desc, uint64_t column_id, IKeyExprs& key_conv_exprs)
{
  int ret = OB_SUCCESS;
  UNUSED(extra_row_desc);
  bool found = false;
  const ObIArray<ObColumnRefRawExpr*>* table_column = insert_stmt.get_table_columns();
  const ObIArray<ObRawExpr*>& column_conv_exprs = insert_stmt.get_column_conv_functions();
  ObRawExpr* col_conv_expr = NULL;
  ObColumnRefRawExpr* col_expr = NULL;
  ObRawExprFactory expr_factory(allocator_);
  CK(OB_NOT_NULL(table_column));
  CK(table_column->count() == column_conv_exprs.count());
  CK(OB_NOT_NULL(session_info));
  for (int64_t idx = 0; OB_SUCC(ret) && !found && idx < table_column->count(); ++idx) {
    RowDesc* row_desc = NULL;
    if (OB_ISNULL(col_expr = table_column->at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid column item", K(ret), K(col_expr), K(idx));
    } else if (column_id != col_expr->get_column_id()) {
      // do nothing
    } else if (OB_ISNULL(col_conv_expr = column_conv_exprs.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Column conv expr should not be NULL", K(ret));
    } else {
      // row_desc = col_conv_expr->is_for_generated_column() ? &extra_row_desc : &value_row_desc;
      row_desc = &value_row_desc;
      if (col_conv_expr->is_for_generated_column() && !session_info->use_static_typing_engine()) {
        ObRawExpr* copy_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, col_conv_expr, copy_expr, COPY_REF_DEFAULT))) {
          LOG_WARN("deep copy expr failed", K(ret));
        } else if (OB_FAIL(recursive_convert_generated_column(*table_column, column_conv_exprs, copy_expr))) {
          LOG_WARN("faield to recursive convert generated column", K(ret));
        } else {
          col_conv_expr = copy_expr;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(add_key_expr_with_row_desc(key_conv_exprs, *row_desc, col_conv_expr))) {
        LOG_WARN("Failed to add key expr", K(ret));
      } else {
        found = true;
      }
    }
  }

  return ret;
}

int ObTableLocation::clear_columnlized_in_row_desc(RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  int64_t row_desc_count = row_desc.get_column_num();
  ObRawExpr* expr = NULL;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < row_desc_count; ++idx) {
    if (OB_ISNULL(expr = row_desc.get_column(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expr in row desc should not be NULL", K(ret));
    } else if (OB_FAIL(expr->clear_flag(IS_COLUMNLIZED))) {
      LOG_WARN("Failed to clear IS_COLUMNLIZED flag", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObTableLocation::init_row(ObIAllocator& allocator, const int64_t column_num, ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (column_num <= 0) {
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObObj) * column_num))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate pkey values", K(ret), K(column_num));
  } else {
    row.cells_ = new (ptr) ObObj[column_num];
    row.count_ = column_num;
  }
  return ret;
}

int ObTableLocation::calc_row(ObExprCtx& expr_ctx, const IKeyExprs& key_exprs, const int64_t key_count,
    const int64_t value_idx, ObNewRow& intput_row, ObNewRow& output_row) const
{
  int ret = OB_SUCCESS;
  if (key_count <= 0) {
    // do  nothing
  } else if (OB_ISNULL(output_row.cells_) || output_row.count_ < key_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output_row row not init correct with key_count", K(ret), K(key_count), K(output_row));
  } else {
    ObSqlExpression* sql_expr = NULL;
    const int64_t start_pos = value_idx * key_count;
    for (int64_t key_idx = 0; OB_SUCC(ret) && key_idx < key_count; ++key_idx) {
      if (OB_FAIL(key_exprs.at(key_idx + start_pos, sql_expr))) {
        LOG_WARN("Failed to get sql expr", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Sql expr should not be NULL", K(ret));
      } else if (OB_FAIL(sql_expr->calc(expr_ctx, intput_row, output_row.cells_[key_idx]))) {
        LOG_WARN("Get const or calc expr value error", K(ret));
      } else {
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_up_key_exprs_partition_id(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const IKeyExprs& key_exprs, const int64_t expect_idx, bool& cross_part, const int64_t part_idx) const
{
  int ret = OB_SUCCESS;
  const int64_t key_count = key_exprs.count();
  cross_part = false;
  ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
  allocator.set_label("CalcUpKey");
  ObNewRow part_row;
  ObExprCtx expr_ctx;
  ObSEArray<int64_t, 1> update_ids;
  if (key_exprs.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Key exprs count should not less than 1", K(ret));
  } else if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else if (OB_FAIL(init_row(allocator, key_count, part_row))) {
    LOG_WARN("Failed to init row", K(ret));
  } else if (OB_FAIL(calc_row(expr_ctx, key_exprs, key_count, 0, part_row, part_row))) {
    LOG_WARN("Failed to calc part row", K(ret));
  } else if (OB_INVALID_INDEX == part_idx) {
    if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, part_row, update_ids, NULL))) {
      LOG_WARN("Calc partition id by row error", K(ret));
    }
  } else {
    ObSEArray<int64_t, 1> part_ids;
    if (OB_FAIL(part_ids.push_back(part_idx))) {
      LOG_WARN("Failed to add part idx", K(ret));
    } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, part_mgr, part_row, update_ids, &part_ids))) {
      LOG_WARN("Calc partition id by row error", K(ret));
    } else {
    }  // do nothing
  }

  // check
  if (OB_FAIL(ret)) {
  } else if (0 == update_ids.count()) {
    ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
  } else if (update_ids.count() != 1) {
    cross_part = true;
  } else if (OB_INVALID_INDEX == part_idx) {
    cross_part = (update_ids.at(0) != expect_idx);
  } else {
    cross_part = (extract_subpart_idx(update_ids.at(0)) != expect_idx);
  }
  return ret;
}

int ObTableLocation::set_log_op_infos(const uint64_t index_table_id, const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  index_table_id_ = index_table_id;
  if (OB_UNLIKELY(MAX_DIR != direction_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("direction_ has been set", K(ret), K(direction_), K(direction));
  } else {
    direction_ = direction;
  }
  return ret;
}

int ObTableLocation::append_phy_table_location(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
    bool is_weak, const ObIArray<int64_t>& part_ids, const ObOrderDirection& order_direction)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocationInfo phy_location_info;
  ObTaskExecutorCtx& executor_ctx = ctx.get_task_exec_ctx();
  ObIPartitionLocationCache* location_cache = executor_ctx.get_partition_location_cache();
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  ObPhyTableLocation* phy_table_loc =
      ObTaskExecutorCtxUtil::get_phy_table_location_for_update(executor_ctx, table_location_key, ref_table_id);
  phy_location_info.set_direction(order_direction);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session_info) || OB_ISNULL(location_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_ctx), K(session_info), K(location_cache));
  } else if (order_direction != UNORDERED) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("append partition location info with order direction not support", K(ret), K(order_direction));
  } else if (phy_table_loc != NULL) {
    // append physical table location
    ObSEArray<int64_t, 8> expected_part_ids;
    if (OB_FAIL(phy_table_loc->find_not_include_part_ids(part_ids, expected_part_ids))) {
      LOG_WARN("find not include part ids failed", K(ret), K(part_ids), KPC(phy_table_loc));
    } else if (expected_part_ids.empty()) {
      // do nothing
    } else if (OB_FAIL(get_phy_table_location_info(ctx,
                   table_location_key,
                   ref_table_id,
                   is_weak,
                   expected_part_ids,
                   *location_cache,
                   phy_location_info))) {
      LOG_WARN("get phy table location info failed", K(ret));
    } else if (OB_FAIL(phy_table_loc->add_partition_locations(phy_location_info))) {
      LOG_WARN("add partition locations failed", K(ret), K(phy_location_info));
    }
  } else if (OB_FAIL(get_phy_table_location_info(
                 ctx, table_location_key, ref_table_id, is_weak, part_ids, *location_cache, phy_location_info))) {
    LOG_WARN("get phy table location info failed", K(ret));
  } else if (OB_FAIL(executor_ctx.append_table_location(phy_location_info))) {
    LOG_WARN("append table location failed", K(ret), K(phy_location_info));
  }

  return ret;
}

int ObTableLocation::get_phy_table_location_info(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
    bool is_weak, const ObIArray<int64_t>& part_ids, ObIPartitionLocationCache& location_cache,
    ObPhyTableLocationInfo& phy_location_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx()) || OB_ISNULL(ctx.get_task_executor_ctx()) ||
      OB_ISNULL(session_info = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null", K(plan_ctx), K(ctx.get_task_executor_ctx()), K(session_info));
  } else if (OB_FAIL(ObTableLocation::set_partition_locations(ctx,
                 location_cache,
                 ref_table_id,
                 part_ids,
                 phy_location_info.get_phy_part_loc_info_list_for_update()))) {
    LOG_WARN("set partition locations failed", K(ret));
  } else {
    phy_location_info.set_table_location_key(table_location_key, ref_table_id);
    ObSEArray<ObPhyTableLocationInfo*, 2> phy_location_info_ptrs;
    share::schema::ObMultiVersionSchemaService* schema_service = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema* table_schema = NULL;
    const uint64_t tenant_id = extract_tenant_id(ref_table_id);
    if (OB_INVALID_ID != ref_table_id && extract_pure_id(ref_table_id) > OB_MIN_USER_TABLE_ID) {
      ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
      if (OB_ISNULL(schema_service = task_exec_ctx.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_service is null", K(ret));
      } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
        LOG_WARN("get table schema failed");
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("schema error, maybe schema version changed, need retry");
      } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
        // phy_location_info.set_duplicate_type(ObDuplicateType::DUPLICATE);
        phy_location_info.set_duplicate_type(ObDuplicateType::DUPLICATE_IN_DML);
        LOG_TRACE(
            "won't set duplicate property since it is being modified", K(table_schema->get_table_name_str()), K(lbt()));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(phy_location_info_ptrs.push_back(&phy_location_info))) {
      LOG_WARN("store physical location info failed", K(ret));
    } else if (OB_FAIL(ObLogPlan::select_replicas(ctx, is_weak, ctx.get_addr(), phy_location_info_ptrs))) {
      LOG_WARN("fail to select replicas", K(ret), K(ctx.get_addr()), K(phy_location_info_ptrs));
    }
  }
  return ret;
}

/*void ObTableLocation::set_index_table_id(const uint64_t index_table_id)
{
  index_table_id_ = index_table_id;
}

int ObTableLocation::set_pre_query_range(const ObQueryRange *pre_query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pre_query_range)) {
  } else if (OB_FAIL(pre_query_range_.deep_copy(*pre_query_range))) {
    LOG_WARN("failed to copy pre query range", K(ret));
  }
  if (OB_ISNULL(pre_query_range)) {
    LOG_TRACE("set pre query range", K(ret), K(pre_query_range));
  } else {
    LOG_TRACE("set pre query range", K(ret), K(*pre_query_range), K(pre_query_range_));
  }
  return ret;
}

int ObTableLocation::set_direction(const ObOrderDirection &direction)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MAX_DIR != direction_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("direction_ has been set", K(ret), K(direction_), K(direction));
  } else {
    direction_ = direction;
  }
  return ret;
}*/

int ObPartIdRowMapManager::add_row_for_part(int64_t part_idx, int64_t row_id)
{
  int ret = OB_SUCCESS;
  // linear search right now. maybe, we can run binary search even interpolation search to get a better perf.
  // if you are free, please do not be shy to improve this.
  if (OB_UNLIKELY(part_idx < 0) || OB_UNLIKELY(part_idx > manager_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part idx is invalid", K(part_idx), K(manager_.count()), K(ret));
  } else if (OB_LIKELY(part_idx < manager_.count())) {
    if (OB_FAIL(manager_.at(part_idx).list_.push_back(row_id))) {
      LOG_WARN("push row id to manager failed", K(ret));
    }
  } else {
    MapEntry entry;
    if (OB_FAIL(entry.list_.push_back(row_id))) {
      LOG_WARN("push row id failed", K(ret), K(part_idx), K(row_id));
    } else if (OB_FAIL(manager_.push_back(entry))) {
      LOG_WARN("push entry failed", K(ret), K(part_idx), K(row_id));
    }
  }
  return ret;
}

const ObPartIdRowMapManager::ObRowIdList* ObPartIdRowMapManager::get_row_id_list(int64_t part_index)
{
  const ObPartIdRowMapManager::ObRowIdList* ret = NULL;
  // linear search right now. maybe, we can run binary search even interpolation search to get a better perf.
  // if you are free, please do not be shy to improve this.
  if (part_index >= 0 && part_index < manager_.count()) {
    ret = &(manager_.at(part_index).list_);
  }
  return ret;
}

int ObPartIdRowMapManager::MapEntry::assign(const ObPartIdRowMapManager::MapEntry& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(list_.assign(other.list_))) {
      LOG_WARN("copy list failed", K(ret));
    }
  }
  return ret;
}

int ObPartIdRowMapManager::assign(const ObPartIdRowMapManager& other)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(other.manager_, i)
  {
    MapEntry map_entry;
    if (OB_FAIL(map_entry.assign(other.manager_.at(i)))) {
      LOG_WARN("assign manager entry failed", K(ret), K(i));
    } else if (OB_FAIL(manager_.push_back(map_entry))) {
      LOG_WARN("store map entry failed", K(ret));
    }
  }
  return ret;
}

int ObPLQueryRangeNode::get_range(ObIAllocator& allocator, const ParamStore& params, ObQueryRangeArray& query_ranges,
    bool& is_all_single_value_ranges, bool& is_empty, const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pre_query_range_.get_tablet_ranges(
          allocator, params, query_ranges, is_all_single_value_ranges, dtc_params))) {
    LOG_WARN("get tablet ranges failed", K(ret));
  } else if (OB_FAIL(is_all_ranges_empty(query_ranges, is_empty))) {
    LOG_WARN("fail to check all ranges", K(query_ranges));
  } else if (query_ranges.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Query ranges' count should not be 0", "query range count", query_ranges.count(), K(ret));
  } else {
  }

  return ret;
}

OB_DEF_SERIALIZE(ObTableLocation)
{
  int ret = OB_SUCCESS;
  bool has_part_expr = (part_expr_ != NULL);
  bool has_subpart_expr = (subpart_expr_ != NULL);
  LST_DO_CODE(OB_UNIS_ENCODE,
      table_id_,
      ref_table_id_,
      stmt_type_,
      literal_stmt_type_,
      hint_read_consistency_,
      is_contain_inner_table_,
      is_contain_select_for_update_,
      is_contain_mv_,
      is_partitioned_,
      direction_,
      part_level_,
      part_type_,
      subpart_type_,
      part_num_,
      is_global_index_,
      has_part_expr,
      has_subpart_expr,
      part_projector_,
      first_partition_id_,
      inited_);
  if (OB_SUCC(ret) && has_part_expr) {
    OB_UNIS_ENCODE(*part_expr_);
  }
  if (OB_SUCC(ret) && has_subpart_expr) {
    OB_UNIS_ENCODE(*subpart_expr_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, is_oracle_temp_table_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, duplicate_type_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLocation)
{
  int64_t len = 0;
  bool has_part_expr = (part_expr_ != NULL);
  bool has_subpart_expr = (subpart_expr_ != NULL);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      table_id_,
      ref_table_id_,
      stmt_type_,
      literal_stmt_type_,
      hint_read_consistency_,
      is_contain_inner_table_,
      is_contain_select_for_update_,
      is_contain_mv_,
      is_partitioned_,
      direction_,
      part_level_,
      part_type_,
      subpart_type_,
      part_num_,
      is_global_index_,
      has_part_expr,
      has_subpart_expr,
      part_projector_,
      first_partition_id_,
      inited_,
      is_oracle_temp_table_,
      duplicate_type_);
  if (has_part_expr) {
    OB_UNIS_ADD_LEN(*part_expr_);
  }
  if (has_subpart_expr) {
    OB_UNIS_ADD_LEN(*subpart_expr_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObTableLocation)
{
  int ret = OB_SUCCESS;
  bool has_part_expr = false;
  bool has_subpart_expr = false;
  LST_DO_CODE(OB_UNIS_DECODE,
      table_id_,
      ref_table_id_,
      stmt_type_,
      literal_stmt_type_,
      hint_read_consistency_,
      is_contain_inner_table_,
      is_contain_select_for_update_,
      is_contain_mv_,
      is_partitioned_,
      direction_,
      part_level_,
      part_type_,
      subpart_type_,
      part_num_,
      is_global_index_,
      has_part_expr,
      has_subpart_expr,
      part_projector_,
      first_partition_id_,
      inited_);
  if (OB_SUCC(ret) && has_part_expr) {
    if (OB_FAIL(sql_expression_factory_.alloc(part_expr_))) {
      LOG_WARN("allocate part expr failed", K(ret));
    } else if (OB_ISNULL(part_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_expr_ is null");
    } else {
      OB_UNIS_DECODE(*part_expr_);
    }
  }
  if (OB_SUCC(ret) && has_subpart_expr) {
    if (OB_FAIL(sql_expression_factory_.alloc(subpart_expr_))) {
      LOG_WARN("allocate part expr failed", K(ret));
    } else if (OB_ISNULL(subpart_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_expr_ is null");
    } else {
      OB_UNIS_DECODE(*subpart_expr_);
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, is_oracle_temp_table_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, duplicate_type_);
  }
  return ret;
}

ObRawExpr* ObTableLocation::get_related_part_expr(
    const ObDMLStmt& stmt, share::schema::ObPartitionLevel part_level, uint64_t table_id, uint64_t index_tid) const
{
  ObRawExpr* ret = NULL;
  if (part_level == PARTITION_LEVEL_ONE) {
    if (related_part_expr_idx_ == OB_INVALID_INDEX) {
      ret = stmt.get_part_expr(table_id, index_tid);
    } else {
      ret = stmt.get_related_part_expr(table_id, index_tid, related_part_expr_idx_);
    }
  } else if (part_level == PARTITION_LEVEL_TWO) {
    if (related_subpart_expr_idx_ == OB_INVALID_INDEX) {
      ret = stmt.get_subpart_expr(table_id, index_tid);
    } else {
      ret = stmt.get_related_subpart_expr(table_id, index_tid, related_subpart_expr_idx_);
    }
  }
  return ret;
}

int ObTableLocation::get_partition_ids_by_range(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const ObNewRange* part_range, const ObNewRange* gen_range, ObIArray<int64_t>& partition_ids,
    const ObIArray<int64_t>* level_one_part_ids) const
{
  int ret = OB_SUCCESS;
  ObPartLocCalcNode* gen_col_node = NULL == level_one_part_ids ? gen_col_node_ : sub_gen_col_node_;
  ObSqlExpression* gen_col_expr = NULL == level_one_part_ids ? gen_col_expr_ : sub_gen_col_expr_;
  ObSEArray<int64_t, 5> gen_part_ids;
  bool all_part_by_part_range = false;
  bool all_part_by_gen_range = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (OB_UNLIKELY(is_simple_insert_or_replace())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert/replace stmt should not get partition ids by range", K(ret));
  } else if (!is_partitioned_) {
    if (OB_FAIL(partition_ids.push_back(0))) {
      LOG_WARN("Failed to push back partition id", K(ret));
    }
  } else {
    if (NULL == part_range) {
      all_part_by_part_range = true;
    } else if (OB_FAIL(calc_partition_ids_by_range(
                   exec_ctx, part_mgr, part_range, partition_ids, all_part_by_part_range, level_one_part_ids))) {
      LOG_WARN("Failed to calc partitoin ids by calc node", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (NULL == gen_range || OB_ISNULL(gen_col_node) || OB_ISNULL(gen_col_expr)) {
        all_part_by_gen_range = true;
      } else if (OB_FAIL(calc_partition_ids_by_range(exec_ctx,
                     part_mgr,
                     gen_range,
                     gen_part_ids,
                     all_part_by_gen_range,
                     level_one_part_ids,
                     gen_col_expr))) {
        LOG_WARN("Failed to calcl partition ids by gen col node", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!all_part_by_part_range && !all_part_by_gen_range) {
        if (OB_FAIL(intersect_partition_ids(gen_part_ids, partition_ids))) {
          LOG_WARN("Failed to intersect partition ids", K(ret));
        }
      } else if (!all_part_by_part_range && all_part_by_gen_range) {
        // do nothing
      } else if (all_part_by_part_range && !all_part_by_gen_range) {
        if (OB_FAIL(append(partition_ids, gen_part_ids))) {
          LOG_WARN("append partition ids failed", K(ret));
        }
      } else if (OB_FAIL(get_all_part_ids(exec_ctx, part_mgr, partition_ids, level_one_part_ids))) {
        LOG_WARN("Get all part ids error", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_range(ObExecContext& exec_ctx, ObPartMgr* part_mgr, const ObNewRange* range,
    ObIArray<int64_t>& partition_ids, bool& all_part, const ObIArray<int64_t>* part_ids,
    const ObSqlExpression* gen_col_expr) const
{
  int ret = OB_SUCCESS;
  all_part = false;
  ObSEArray<ObNewRange*, 1> dummy_query_ranges;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObNewRange", K(ret));
  } else if (range->empty()) {
    // do nothing. partition ids will be empty
  } else if (OB_FAIL(dummy_query_ranges.push_back(const_cast<ObNewRange*>(range)))) {
    LOG_WARN("failed to push back query range", K(ret));
  } else if (OB_FAIL(calc_partition_ids_by_ranges(exec_ctx,
                 part_mgr,
                 dummy_query_ranges,
                 range->is_single_rowkey(),
                 partition_ids,
                 all_part,
                 part_ids,
                 gen_col_expr))) {
    LOG_WARN("Failed to get partition ids", K(ret), K(table_id_));
  }
  return ret;
}

int ObTableLocation::init_table_location_with_row_desc(
    ObSqlSchemaGuard& schema_guard, uint64_t table_id, RowDesc& input_row_desc, ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObTableSchema* table_schema = NULL;
  RowDesc row_desc;
  uint64_t real_table_id = OB_INVALID_ID;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_FAIL(schema_checker.get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema", K(ret), K(table_id));
  } else {
    real_table_id = table_schema->is_index_local_storage() ? table_schema->get_data_table_id() : table_id;
    // reslove partition expr
    ObResolverParams resolver_ctx;
    ObRawExprFactory expr_factory(allocator_);
    ObStmtFactory stmt_factory(allocator_);
    TableItem table_item;
    resolver_ctx.allocator_ = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info;
    resolver_ctx.disable_privilege_check_ = true;
    resolver_ctx.expr_factory_ = &expr_factory;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = real_table_id;
    table_item.ref_id_ = real_table_id;
    table_item.type_ = TableItem::BASE_TABLE;
    ObDeleteResolver delete_resolver(resolver_ctx);
    ObDeleteStmt* delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
    const ObTableSchema* real_table_schema = table_schema;
    if (OB_ISNULL(resolver_ctx.query_ctx_) || OB_ISNULL(delete_stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create query_ctx or delete stmt failed", K_(resolver_ctx.query_ctx), K(delete_stmt));
    } else if (OB_FAIL(delete_stmt->get_table_items().push_back(&table_item))) {
      LOG_WARN("store table item failed", K(ret));
    } else if (OB_FAIL(delete_stmt->set_table_bit_index(real_table_id))) {
      LOG_WARN("set table bit index failed", K(ret), K(real_table_id));
    } else if (OB_UNLIKELY(table_schema->is_index_local_storage()) &&
               OB_FAIL(schema_guard.get_table_schema(real_table_id, real_table_schema))) {
      LOG_WARN("get real table schema failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(delete_resolver.resolve_table_partition_expr(table_item, *real_table_schema))) {
      LOG_WARN("resolve table partition expr failed", K(ret));
    } else if (OB_FAIL(generate_row_desc_from_row_desc(
                   *delete_stmt, real_table_id, expr_factory, input_row_desc, row_desc))) {
      LOG_WARN("generate rowkey desc failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(init_table_location(schema_guard,
                   real_table_id,
                   real_table_id,
                   *delete_stmt,
                   row_desc,
                   false,
                   default_asc_direction()))) {
      LOG_WARN("init table location failed", K(ret), K(real_table_id));
    } else if (OB_FAIL(clear_columnlized_in_row_desc(row_desc))) {
      LOG_WARN("Failed to clear columnlized in row desc", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::init_table_location_with_rowid(ObSqlSchemaGuard& schema_guard,
    const ObIArray<int64_t>& param_index, const uint64_t table_id, const uint64_t ref_table_id,
    ObSQLSessionInfo& session_info, const bool is_dml_table)
{
  int ret = OB_SUCCESS;
  uint64_t real_table_id = ref_table_id;
  if (ObSQLMockSchemaUtils::is_mock_index(real_table_id)) {
    real_table_id = ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(real_table_id);
  }
  if (OB_FAIL(init_table_location_with_rowkey(schema_guard, real_table_id, session_info, is_dml_table))) {
    LOG_WARN("failed to init table location with rowkey", K(ret));
  } else if (OB_FAIL(part_expr_param_idxs_.assign(param_index))) {
    LOG_WARN("failed to assign array", K(ret));
  } else {
    use_calc_part_by_rowid_ = true;
    // init_table_location_with_rowkey will set direction_ to default_acs_direction
    // here we change it to MAX_DIR, it will be set in log_table_scan
    direction_ = MAX_DIR;
    table_id_ = table_id;  // logical table id
  }
  return ret;
}

int ObTableLocation::generate_row_desc_from_row_desc(ObDMLStmt& stmt, const uint64_t data_table_id,
    ObRawExprFactory& expr_factory, const RowDesc& input_row_desc, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_row_desc.get_column_num(); ++i) {
    ObRawExpr* expr = input_row_desc.get_column(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_LIKELY(expr->is_column_ref_expr())) {
      uint64_t column_id = static_cast<ObColumnRefRawExpr*>(expr)->get_column_id();
      col_expr = stmt.get_column_expr_by_id(data_table_id, column_id);
      if (OB_ISNULL(col_expr)) {
        if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
          LOG_WARN("create mock rowkey column expr failed", K(ret));
        } else if (OB_FAIL(row_desc.add_column(col_expr))) {
          LOG_WARN("add rowkey column expr to row desc failed", K(ret));
        }
      } else if (OB_FAIL(row_desc.add_column(col_expr))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    } else {
      if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
        LOG_WARN("create mock rowkey column expr failed", K(ret));
      } else if (OB_FAIL(row_desc.add_column(col_expr))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_location_infos_with_rowid(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard& schema_guard, const ParamStore& params,
    share::ObIPartitionLocationCache& location_cache, ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list,
    bool nonblock) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!use_calc_part_by_rowid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexecpted table location flag", K(ret), K(use_calc_part_by_rowid_));
  }
  ObArray<int64_t> part_ids;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(calculate_partition_ids_with_rowid(exec_ctx, schema_guard, params, part_ids))) {
    LOG_WARN("failed to calculate partition ids", K(ret));
  } else if (OB_FAIL(set_partition_locations(
                 exec_ctx, location_cache, ref_table_id_, part_ids, phy_part_loc_info_list, nonblock))) {
    LOG_WARN("failed to set partition locations", K(ret));
  } else {
  }
  return ret;
}

int ObTableLocation::calculate_partition_ids_with_rowid(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard& schema_guard, const ParamStore& params,
    common::ObIArray<int64_t>& part_ids) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited table location", K(ret));
  } else if (!use_calc_part_by_rowid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error happended", K(ret));
  } else if (!is_partitioned_) {
    ret = part_ids.push_back(0);
  } else {
    ObArray<ObObj> urowid_objs;
    for (int i = 0; OB_SUCC(ret) && i < part_expr_param_idxs_.count(); i++) {
      int64_t idx = part_expr_param_idxs_.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= params.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param index", K(ret), K(idx));
      } else if (OB_FAIL(urowid_objs.push_back(params.at(idx)))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }
    ObSEArray<sql::RowkeyArray, 3> rowkey_list;
    ObArray<ObRowkey> rowkey_arr;
    // construct rowkeys
    ObObj* obj_buf = NULL;
    for (int i = 0; OB_SUCC(ret) && i < urowid_objs.count(); i++) {
      ObArray<ObObj> pk_vals;
      if (OB_FAIL(urowid_objs.at(i).get_urowid().get_pk_vals(pk_vals))) {
        LOG_WARN("failed to get pk vals", K(ret));
      } else if (OB_ISNULL(obj_buf = (ObObj*)exec_ctx.get_allocator().alloc(sizeof(ObObj) * pk_vals.count()))) {
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        for (int k = 0; k < pk_vals.count(); k++) {
          (void)new (obj_buf + k) ObObj();
          obj_buf[k] = pk_vals.at(k);
        }
        ObRowkey rowkey(obj_buf, pk_vals.count());
        if (OB_FAIL(rowkey_arr.push_back(rowkey))) {
          LOG_WARN("failed to push back element", K(ret));
        }
      }
    }  // for end

    const ObTableSchema* base_table_schema = NULL;
    ObArray<ObColDesc> rowkey_descs;
    bool is_all_part_get = false;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(schema_guard.get_table_schema(ref_table_id_, base_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null table schema", K(ret));
    } else if (OB_FAIL(base_table_schema->get_rowkey_column_ids(rowkey_descs))) {
      LOG_WARN("failed to get rowkey column ids", K(ret));
    } else {
      // check wether all rowkey infos are valid, if not, return all part ids
      for (int i = 0; !is_all_part_get && i < rowkey_arr.count(); i++) {
        if (OB_UNLIKELY(rowkey_arr.at(i).get_obj_cnt() != rowkey_descs.count())) {
          is_all_part_get = true;
        } else {
          for (int j = 0; !is_all_part_get && j < rowkey_descs.count(); j++) {
            const ObObj* obj_ptr = rowkey_arr.at(i).get_obj_ptr();
            if (!obj_ptr[j].meta_.is_null() &&
                !ObSQLUtils::is_same_type_for_compare(obj_ptr[j].meta_, rowkey_descs.at(j).col_type_)) {
              is_all_part_get = true;
            }
          }
        }
      }  // for end
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_all_part_get) {
      if (!is_partitioned_) {
        ret = part_ids.push_back(0);
      } else if (PARTITION_LEVEL_ONE == part_level_) {
        if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, part_ids))) {
          LOG_WARN("failed to get all part ids", K(ret));
        }
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        ObArray<int64_t> tmp_part_ids;
        if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, tmp_part_ids))) {
          LOG_WARN("failed to get partition ids", K(ret));
        } else if (OB_FAIL(get_all_part_ids(exec_ctx, &schema_guard, part_ids, &tmp_part_ids))) {
          LOG_WARN("failed to get all partition ids", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("get all part ids", K(part_ids));
      }
    } else if (OB_FAIL(calc_partition_ids_by_rowkey(exec_ctx, &schema_guard, rowkey_arr, part_ids, rowkey_list))) {
      LOG_WARN("failed to calc partition ids", K(ret));
    } else {
      LOG_TRACE("get all partition ids", K(part_ids));
    }
  }
  return ret;
}

int ObTableLocation::can_get_part_by_range_for_range_columns(const ObRawExpr* part_expr, bool& is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (part_expr->is_column_ref_expr()) {
    is_valid = true;
  } else if (T_OP_ROW == part_expr->get_expr_type()) {
    const ObRawExpr* col_expr = NULL;
    is_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < part_expr->get_param_count(); ++i) {
      if (OB_ISNULL(col_expr = part_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!col_expr->is_column_ref_expr()) {
        is_valid = false;
      }
    }
  } else {
    is_valid = false;
  }
  return ret;
}

int ObTableLocation::recursive_convert_generated_column(
    const ObIArray<ObColumnRefRawExpr*>& table_column, const ObIArray<ObRawExpr*>& column_conv_exprs, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_column_ref_expr()) {
    bool found = false;
    const int64_t column_id = static_cast<ObColumnRefRawExpr*>(expr)->get_column_id();
    ObRawExpr* col_conv_expr = NULL;
    ObColumnRefRawExpr* col_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < table_column.count(); ++i) {
      if (OB_ISNULL(col_expr = table_column.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid column item", K(ret), K(col_expr), K(i));
      } else if (column_id != col_expr->get_column_id()) {
        // do nothing
      } else if (OB_ISNULL(col_conv_expr = column_conv_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column conv expr should not be NULL", K(ret));
      } else {
        expr = col_conv_expr;
        found = true;
      }
    }
  } else if (T_FUN_COLUMN_CONV == expr->get_expr_type()) {
    if (OB_FAIL(recursive_convert_generated_column(table_column, column_conv_exprs, expr->get_param_expr(4)))) {
      LOG_WARN("failed to recursive convert generated column", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(recursive_convert_generated_column(table_column, column_conv_exprs, expr->get_param_expr(i)))) {
        LOG_WARN("failed to recursive convert generated column", K(ret));
      }
    }
  }
  return ret;
}
