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
#include "common/ob_smart_call.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/engine/cmd/ob_table_executor.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_context.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/dblink/ob_dblink_utils.h"

using namespace oceanbase::transaction;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

static int get_part_id_by_mod(const int64_t calc_result, const int64_t part_num, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  if (calc_result < 0 || 0 == part_num) {
    ret =  OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments is invalid", K(calc_result), K(part_num), K(ret));
  } else {
    part_id = calc_result % part_num;
  }
  return ret;
}

static int is_all_ranges_empty(const ObQueryRangeArray &query_array, bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  for (int i = 0; is_empty && OB_SUCC(ret) && i < query_array.count(); i++) {
    if (OB_ISNULL(query_array.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!query_array.at(i)->empty()) {
      is_empty = false;
    }
  }
  return ret;
}

bool ObListPartMapKey::operator==(const ObListPartMapKey &other) const {
  return row_ == other.row_;
}

int ObListPartMapKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  for (int64_t i = 0; i < row_.get_count() && OB_SUCC(ret); i ++) {
    if (OB_FAIL(row_.get_cell(i).hash(hash_val, hash_val))) {
      LOG_WARN("hash failed", K(ret), K(row_.get_cell(i)));
    }
  }
  return ret;
}

bool ObHashPartMapKey::operator==(const ObHashPartMapKey &other) const {
  return part_idx_ == other.part_idx_;
}

int64_t ObHashPartMapKey::hash() const
{
  uint64_t hash_value = 0;
  ObObj idx_obj(part_idx_);
  idx_obj.hash(hash_value, hash_value);
  return hash_value;
}

bool TableLocationKey::operator==(const TableLocationKey &other) const {
  return table_id_ == other.table_id_ && ref_table_id_ == other.ref_table_id_;
}

bool TableLocationKey::operator!=(const TableLocationKey &other) const {
  return !(*this == other);
}

int ObTableLocation::PartProjector::init_part_projector(ObExecContext *exec_ctx,
                                                        const ObRawExpr *part_expr,
                                                        ObPartitionLevel part_level,
                                                        RowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ONE == part_level) {
    ret = init_part_projector(exec_ctx, part_expr, row_desc);
  } else if (PARTITION_LEVEL_TWO == part_level) {
    ret = init_subpart_projector(exec_ctx, part_expr, row_desc);
  } else { /*do nothing*/ }
  return ret;
}

int ObTableLocation::PartProjector::init_part_projector(ObExecContext *exec_ctx,
                                                        const ObRawExpr *part_expr,
                                                        RowDesc &row_desc)
{
  return init_part_projector(exec_ctx, part_expr, row_desc,
                             part_projector_, part_projector_size_);
}

int ObTableLocation::PartProjector::init_subpart_projector(ObExecContext *exec_ctx,
                                                           const ObRawExpr *part_expr,
                                                           RowDesc &row_desc)
{
  return init_part_projector(exec_ctx, part_expr, row_desc,
                             subpart_projector_, subpart_projector_size_);
}

int ObTableLocation::PartProjector::init_part_projector(ObExecContext *exec_ctx,
                                                        const ObRawExpr *part_expr,
                                                        RowDesc &row_desc,
                                                        int32_t *&projector,
                                                        int64_t &projector_size)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> part_columns;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_columns))) {
    LOG_WARN("extract column exprs failed", K(ret));
  } else {
    projector_size = part_columns.count();
    if (OB_ISNULL(projector = static_cast<int32_t*>(allocator_.alloc(projector_size * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(projector_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < projector_size; ++i) {
      int64_t idx = OB_INVALID_INDEX;
      const ObRawExpr *part_column = part_columns.at(i);
      if (OB_ISNULL(part_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part column is null");
      } else if (OB_UNLIKELY(!part_column->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part column isn't column reference", K(ret), K(*part_column));
      } else if (OB_FAIL(row_desc.get_idx(part_column, idx)) && OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get idx failed", K(ret));
      } else if (OB_ENTRY_NOT_EXIST == ret ) {
        ret = OB_SUCCESS;
        const ObColumnRefRawExpr *col_ref = static_cast<const ObColumnRefRawExpr *>(part_column);
        if (OB_UNLIKELY(!col_ref->is_generated_column())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column reference isn't generated column", K(ret), K(*col_ref));
        } else {
          ObTempExpr *se_virtual_col = NULL;
          OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(col_ref->get_dependant_expr(),
                                                          row_desc, exec_ctx->get_allocator(),
                                                          exec_ctx->get_my_session(),
                                                          exec_ctx->get_sql_ctx()->schema_guard_,
                                                          se_virtual_col));
          CK(OB_NOT_NULL(se_virtual_col));
          OZ(se_virtual_column_exprs_.push_back(se_virtual_col));
          OZ(virtual_column_result_idx_.push_back(row_desc.get_column_num()));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(row_desc.add_column(const_cast<ObRawExpr*>(part_column)))) {
            LOG_WARN("add part column to row desc failed", K(ret));
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

int ObTableLocation::PartProjector::calc_part_row(ObExecContext &ctx,
                                                  const ObNewRow &input_row,
                                                  ObNewRow *&part_row) const
{
  int ret = OB_SUCCESS;
  ObNewRow &cur_part_row = ctx.get_part_row_manager().get_part_row();
  int64_t virtual_col_size = se_virtual_column_exprs_.count();
  if (virtual_col_size <= 0) {
    cur_part_row.cells_ = input_row.cells_;
    cur_part_row.count_ = column_cnt_;
  } else {
    cur_part_row.count_ = column_cnt_;
    if (OB_ISNULL(cur_part_row.cells_)) {
      //part row的cells未分配，这里先为cells分配空间
      int64_t row_size = sizeof(ObObj) * column_cnt_;
      cur_part_row.cells_ = static_cast<ObObj*>(ctx.get_allocator().alloc(row_size));
      if (OB_ISNULL(cur_part_row.cells_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for current part row failed", K(ret), K(row_size));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t copy_row_size = (column_cnt_ - virtual_col_size) * sizeof(ObObj);
      memcpy(cur_part_row.cells_, input_row.cells_, copy_row_size);
    }
    if (OB_SUCC(ret)) {
      CK(se_virtual_column_exprs_.count() == virtual_column_result_idx_.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < se_virtual_column_exprs_.count(); i++) {
        ObTempExpr *expr = se_virtual_column_exprs_.at(i);
        int64_t result_idx = virtual_column_result_idx_.at(i);
        OZ(expr->eval(ctx, cur_part_row, cur_part_row.cells_[result_idx]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_row = &cur_part_row;
  }
  return ret;
}

void ObTableLocation::PartProjector::project_part_row(ObPartitionLevel part_level, ObNewRow &part_row) const
{
  int32_t *projector = NULL;
  int64_t projector_size = 0;
  if (part_level == PARTITION_LEVEL_ONE) {
    projector = part_projector_;
    projector_size = part_projector_size_;
  } else if (part_level == PARTITION_LEVEL_TWO) {
    projector = subpart_projector_;
    projector_size = subpart_projector_size_;
  } else { /*do nothing*/ }
  part_row.projector_ = projector;
  part_row.projector_size_ = projector_size;
}

int ObTableLocation::PartProjector::deep_copy(const PartProjector &other)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
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
  for (int64_t i = 0; OB_SUCC(ret) && i < other.se_virtual_column_exprs_.count(); i++) {
    ObTempExpr *expr = NULL;
    OZ(other.se_virtual_column_exprs_.at(i)->deep_copy(allocator_, expr));
    OZ(se_virtual_column_exprs_.push_back(expr));
  }
  OZ(virtual_column_result_idx_.assign(other.virtual_column_result_idx_));
  return ret;
}

OB_DEF_SERIALIZE(ObTableLocation::PartProjector)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(part_projector_, part_projector_size_);
  OB_UNIS_ENCODE_ARRAY(subpart_projector_, subpart_projector_size_);
  OB_UNIS_ENCODE(column_cnt_);
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(se_virtual_column_exprs_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < se_virtual_column_exprs_.count(); i++) {
      OB_UNIS_ENCODE(*se_virtual_column_exprs_.at(i));
    }
    OB_UNIS_ENCODE(virtual_column_result_idx_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLocation::PartProjector)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(part_projector_, part_projector_size_);
  OB_UNIS_ADD_LEN_ARRAY(subpart_projector_, subpart_projector_size_);
  OB_UNIS_ADD_LEN(column_cnt_);
  OB_UNIS_ADD_LEN(se_virtual_column_exprs_.count());
  for (int64_t i = 0; i < se_virtual_column_exprs_.count(); i++) {
    OB_UNIS_ADD_LEN(*se_virtual_column_exprs_.at(i));
  }
  OB_UNIS_ADD_LEN(virtual_column_result_idx_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableLocation::PartProjector)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
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
  int64_t virtual_col_count = 0;
  OB_UNIS_DECODE(virtual_col_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < virtual_col_count; i++) {
    ObTempExpr *expr = NULL;
    char *mem = static_cast<char *>(allocator_.alloc(sizeof(ObTempExpr)));
    CK(OB_NOT_NULL(mem));
    OX(expr = new(mem)ObTempExpr(allocator_));
    OB_UNIS_DECODE(*expr);
    OZ(se_virtual_column_exprs_.push_back(expr));
  }
  OB_UNIS_DECODE(virtual_column_result_idx_);

  return ret;
}

ObPartLocCalcNode *ObPartLocCalcNode::create_part_calc_node(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode::NodeType type)
{
  void *ptr = NULL;
  ObPartLocCalcNode *ret_node = NULL;
  switch (type) {
    case ObPartLocCalcNode::QUERY_RANGE: {
      ptr = allocator.alloc(sizeof(ObPLQueryRangeNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLQueryRangeNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::FUNC_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLFuncValueNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLFuncValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::COLUMN_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLColumnValueNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLColumnValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_AND: {
      ptr = allocator.alloc(sizeof(ObPLAndNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLAndNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_OR: {
      ptr = allocator.alloc(sizeof(ObPLOrNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLOrNode(allocator);
      }
      break;
    }
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "Invalid ObPartLocCalcNode type", K(type));
      break;
    }
  }
  if (OB_UNLIKELY(NULL == ptr)
      || OB_UNLIKELY(NULL == ret_node)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "Failed to allocate ObPartLocCalcNode", K(type));
  } else if (OB_SUCCESS != calc_nodes.push_back(ret_node)) {
    ret_node->~ObPartLocCalcNode();
    allocator.free(ret_node);
    ret_node = NULL;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Store ObPartLocCalcNode failed");
  } else { }//do nothing

  return ret_node;
}

int ObPartLocCalcNode::create_part_calc_node(common::ObIAllocator &allocator,
                                             ObPartLocCalcNode::NodeType type,
                                             ObPartLocCalcNode *&calc_node)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObPartLocCalcNode *ret_node = NULL;
  switch (type) {
    case ObPartLocCalcNode::QUERY_RANGE: {
      ptr = allocator.alloc(sizeof(ObPLQueryRangeNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLQueryRangeNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::FUNC_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLFuncValueNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLFuncValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::COLUMN_VALUE: {
      ptr = allocator.alloc(sizeof(ObPLColumnValueNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLColumnValueNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_AND: {
      ptr = allocator.alloc(sizeof(ObPLAndNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLAndNode(allocator);
      }
      break;
    }
    case ObPartLocCalcNode::CALC_OR: {
      ptr = allocator.alloc(sizeof(ObPLOrNode));
      if (NULL != ptr) {
        ret_node = new(ptr) ObPLOrNode(allocator);
      }
      break;
    }
    default: {
      LOG_WARN("Invalid ObPartLocCalcNode type", K(type));
      break;
    }
  }
  if (OB_UNLIKELY(NULL == ptr)
      || OB_UNLIKELY(NULL == ret_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate ObPartLocCalcNode", K(type));
  } else {
    calc_node = ret_node;
  }
  return ret;
}

int ObPLAndNode::deep_copy(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode *&other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, CALC_AND))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPartLocCalcNode *left_node = NULL;
    ObPartLocCalcNode *right_node = NULL;
    if (NULL != left_node_
        && OB_FAIL(left_node_->deep_copy(allocator, calc_nodes, left_node))) {
      LOG_WARN("Failed to deep copy left node", K(ret));
    } else if (NULL != right_node_
               && OB_FAIL(right_node_->deep_copy(allocator, calc_nodes, right_node))) {
      LOG_WARN("Failed todeep copy right node", K(ret));
    } else {
      ObPLAndNode *and_node = static_cast<ObPLAndNode*>(other);
      and_node->left_node_ = left_node;
      and_node->right_node_ = right_node;
    }
  }
  return ret;

}

int ObPLAndNode::add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    if (OB_FAIL(SMART_CALL(left_node_->add_part_calc_node(calc_nodes)))) {
      LOG_WARN("fail to add part calc node", K(ret));
    } else if (OB_FAIL(SMART_CALL(right_node_->add_part_calc_node(calc_nodes)))) {
      LOG_WARN("fail to add part calc node", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_nodes.push_back(this))) {
      LOG_WARN("fail to push back calc nodes", K(ret));
    }
  }
  return ret;
}

int ObPLOrNode::deep_copy(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode *&other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, CALC_OR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPartLocCalcNode *left_node = NULL;
    ObPartLocCalcNode *right_node = NULL;
    if (NULL != left_node_
        && OB_FAIL(left_node_->deep_copy(allocator, calc_nodes, left_node))) {
      LOG_WARN("Failed to deep copy left node", K(ret));
    } else if (NULL != right_node_
               && OB_FAIL(right_node_->deep_copy(allocator, calc_nodes, right_node))) {
      LOG_WARN("Failed todeep copy right node", K(ret));
    } else {
      ObPLOrNode *or_node = static_cast<ObPLOrNode*>(other);
      or_node->left_node_ = left_node;
      or_node->right_node_ = right_node;
    }
  }
  return ret;
}

int ObPLOrNode::add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    if (OB_FAIL(SMART_CALL(left_node_->add_part_calc_node(calc_nodes)))) {
      LOG_WARN("fail to add part calc node", K(ret));
    } else if (OB_FAIL(SMART_CALL(right_node_->add_part_calc_node(calc_nodes)))) {
      LOG_WARN("fail to add part calc node", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_nodes.push_back(this))) {
      LOG_WARN("fail to push back calc nodes", K(ret));
    }
  }
  return ret;
}

int ObPLQueryRangeNode::deep_copy(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode *&other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, QUERY_RANGE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLQueryRangeNode *qr_node = static_cast<ObPLQueryRangeNode*>(other);
    if (OB_FAIL(qr_node->pre_query_range_.deep_copy(pre_query_range_))) {
      LOG_WARN("Failed to deep copy pre query range", K(ret));
    }
  }
  return ret;
}

int ObPLQueryRangeNode::add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes)
{
  return calc_nodes.push_back(this);
}

int ObPLFuncValueNode::deep_copy(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode *&other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, FUNC_VALUE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLFuncValueNode *func_node = static_cast<ObPLFuncValueNode*>(other);
    if (OB_FAIL(vie_.deep_copy(allocator, func_node->vie_))) {
      LOG_WARN("Failed to deep copy value", K(ret));
    }
  }
  return ret;
}

int ObPLFuncValueNode::add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes)
{
  return calc_nodes.push_back(this);
}

int ObPLColumnValueNode::deep_copy(
    ObIAllocator &allocator,
    ObIArray<ObPartLocCalcNode*> &calc_nodes,
    ObPartLocCalcNode *&other) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other = create_part_calc_node(allocator, calc_nodes, COLUMN_VALUE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate calc node failed", K(ret));
  } else {
    ObPLColumnValueNode *column_node = static_cast<ObPLColumnValueNode*>(other);
    if (OB_FAIL(vie_.deep_copy(allocator, column_node->vie_))) {
      LOG_WARN("Failed to deep copy value", K(ret));
    }
  }
  return ret;
}
int ObPLColumnValueNode::add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes)
{
  return calc_nodes.push_back(this);
}


int ObTableLocation::get_location_type(const common::ObAddr &server,
                                       const ObCandiTabletLocIArray &phy_part_loc_info_list,
                                       ObTableLocationType &location_type) const
{
  int ret = OB_SUCCESS;
  location_type = OB_TBL_LOCATION_UNINITIALIZED;
  const TableItem *table_item = NULL;
  if (0 == phy_part_loc_info_list.count()) {
    location_type = OB_TBL_LOCATION_LOCAL;
  } else if (1 == phy_part_loc_info_list.count()) {
    share::ObLSReplicaLocation replica_location;
    if (OB_FAIL(phy_part_loc_info_list.at(0).get_selected_replica(replica_location))) {
      LOG_WARN("fail to get selected replica", K(phy_part_loc_info_list.at(0)));
    } else if (!replica_location.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("replica location is invalid", K(ret), K(replica_location));
    } else {
      location_type = ((server == replica_location.get_server()) ? OB_TBL_LOCATION_LOCAL
                                                                 : OB_TBL_LOCATION_REMOTE);
    }
  } else {
    location_type = OB_TBL_LOCATION_DISTRIBUTED;
  }
  return ret;
}

ObTableLocation::ObTableLocation(const ObTableLocation &other) :
    inner_allocator_(ObModIds::OB_SQL_TABLE_LOCATION),
    allocator_(inner_allocator_),
    loc_meta_(inner_allocator_),
    calc_node_(NULL),
    gen_col_node_(NULL),
    subcalc_node_(NULL),
    sub_gen_col_node_(NULL),
    part_projector_(allocator_),
    related_list_(allocator_)
{
  *this = other;
}

ObTableLocation& ObTableLocation::operator=(const ObTableLocation &other)
{
  IGNORE_RETURN this->assign(other);
  return *this;
}

int ObTableLocation::assign(const ObTableLocation &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    inited_ = other.inited_;
    is_partitioned_ = other.is_partitioned_;
    part_level_ = other.part_level_;
    part_type_ = other.part_type_;
    subpart_type_ = other.subpart_type_;
    part_get_all_ = other.part_get_all_;
    subpart_get_all_ = other.subpart_get_all_;
    is_col_part_expr_ = other.is_col_part_expr_;
    is_col_subpart_expr_ = other.is_col_subpart_expr_;
    is_oracle_temp_table_ = other.is_oracle_temp_table_;
    table_type_ = other.table_type_;
    part_col_type_ = other.part_col_type_;
    part_collation_type_ = other.part_collation_type_;
    subpart_col_type_ = other.subpart_col_type_;
    subpart_collation_type_ = other.subpart_collation_type_;
    is_in_hit_ = other.is_in_hit_;
    stmt_type_ = other.stmt_type_;
    is_valid_range_columns_part_range_ = other.is_valid_range_columns_part_range_;
    is_valid_range_columns_subpart_range_ = other.is_valid_range_columns_subpart_range_;
    has_dynamic_exec_param_ = other.has_dynamic_exec_param_;
    is_valid_temporal_part_range_ = other.is_valid_temporal_part_range_;
    is_valid_temporal_subpart_range_ = other.is_valid_temporal_subpart_range_;
    is_part_range_get_ = other.is_part_range_get_;
    is_subpart_range_get_ = other.is_subpart_range_get_;
    is_non_partition_optimized_ = other.is_non_partition_optimized_;
    tablet_id_ = other.tablet_id_;
    object_id_ = other.object_id_;
    check_no_partition_ = other.check_no_partition_;
    if (OB_FAIL(loc_meta_.assign(other.loc_meta_))) {
      LOG_WARN("assign loc meta failed", K(ret), K(other.loc_meta_));
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.calc_node_) {
        if (OB_FAIL(other.calc_node_->deep_copy(allocator_, calc_nodes_, calc_node_))) {
           LOG_WARN("Failed to deep copy node", K(ret));
         }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != other.gen_col_node_) {
        if (OB_FAIL(other.gen_col_node_->deep_copy(allocator_, calc_nodes_,
                                                   gen_col_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
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
        if (OB_FAIL(other.sub_gen_col_node_->deep_copy(allocator_, calc_nodes_,
                                                       sub_gen_col_node_))) {
          LOG_WARN("Failed to deep copy node", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_partitioned_) {
      // do nothing
    } else if (OB_FAIL(part_projector_.deep_copy(other.part_projector_))) {
      LOG_WARN("deep copy part projector failed", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_hint_ids_.assign(other.part_hint_ids_))) {
      LOG_WARN("Failed to assign part hint ids", K(ret));
    }
    LOG_TRACE("deep copy table location", K(other));
    for (int64_t i = 0; OB_SUCC(ret) && i < other.vies_.count(); i++) {
      ValueItemExpr vie;
      OZ(other.vies_.at(i).deep_copy(allocator_, vie));
      OZ(vies_.push_back(vie));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.sub_vies_.count(); i++) {
      ValueItemExpr vie;
      OZ(other.sub_vies_.at(i).deep_copy(allocator_, vie));
      OZ(sub_vies_.push_back(vie));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.se_part_expr_)) {
      OZ(other.se_part_expr_->deep_copy(allocator_, se_part_expr_));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.se_subpart_expr_)) {
      OZ(other.se_subpart_expr_->deep_copy(allocator_, se_subpart_expr_));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.se_gen_col_expr_)) {
      OZ(other.se_gen_col_expr_->deep_copy(allocator_, se_gen_col_expr_));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(other.se_sub_gen_col_expr_)) {
      OZ(other.se_sub_gen_col_expr_->deep_copy(allocator_, se_sub_gen_col_expr_));
    }
    if (OB_SUCC(ret) && OB_FAIL(related_list_.assign(other.related_list_))) {
      LOG_WARN("assign related list failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return ret;
}

void ObTableLocation::reset()
{
  inited_ = false;
  loc_meta_.reset();
  is_partitioned_ = true;
  part_level_ = PARTITION_LEVEL_ZERO;
  part_type_ = PARTITION_FUNC_TYPE_MAX;
  subpart_type_  = share::schema::PARTITION_FUNC_TYPE_MAX;
  part_get_all_ = false;
  subpart_get_all_ = false;
  is_col_part_expr_ = false;
  is_col_subpart_expr_ = false;
  is_oracle_temp_table_ = false;
  table_type_ = MAX_TABLE_TYPE;

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
  stmt_type_ = stmt::T_NONE;
  vies_.reset();
  sub_vies_.reset();
  se_part_expr_ = NULL;
  se_gen_col_expr_ = NULL;
  se_subpart_expr_ = NULL;
  se_sub_gen_col_expr_ = NULL;
  part_hint_ids_.reset();
  part_col_type_ = ObNullType;
  related_list_.reset();
  inner_allocator_.reset();
  part_collation_type_ = CS_TYPE_INVALID;
  subpart_col_type_ = ObNullType;
  subpart_collation_type_ = CS_TYPE_INVALID;
  is_in_hit_ = false;
  part_projector_.reset();
  is_valid_range_columns_part_range_ = false;
  is_valid_range_columns_subpart_range_ = false;
  has_dynamic_exec_param_ = false;
  is_valid_temporal_part_range_ = false;
  is_valid_temporal_subpart_range_ = false;
  is_part_range_get_ = false;
  is_subpart_range_get_ = false;
  is_non_partition_optimized_ = false;
  tablet_id_.reset();
  object_id_ = OB_INVALID_ID;
  check_no_partition_ = false;
}
int ObTableLocation::init(share::schema::ObSchemaGetterGuard &schema_guard,
    const ObDMLStmt &stmt,
    ObExecContext *exec_ctx,
    const common::ObIArray<ObRawExpr*> &filter_exprs,
    const uint64_t table_id,
    const uint64_t ref_table_id,
    const common::ObIArray<common::ObObjectID> *part_ids,
    const common::ObDataTypeCastParams &dtc_params,
    const bool is_dml_table,
    common::ObIArray<ObRawExpr*> *sort_exprs)
{
  int ret = common::OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret), K(exec_ctx));
  } else if (OB_FAIL(schema_guard.get_table_schema(exec_ctx->get_my_session()->get_effective_tenant_id(),
                                                   ref_table_id, table_schema))) {
    SQL_OPT_LOG(WARN, "failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_FAIL(init(table_schema, stmt, exec_ctx, filter_exprs, table_id,
                          ref_table_id, part_ids, dtc_params, is_dml_table, sort_exprs))) {
    SQL_OPT_LOG(WARN, "failed to init", K(ret), K(ref_table_id));
  }
  return ret;
}
int ObTableLocation::init(ObSqlSchemaGuard &schema_guard,
    const ObDMLStmt &stmt,
   ObExecContext *exec_ctx,
    const common::ObIArray<ObRawExpr*> &filter_exprs,
    const uint64_t table_id,
    const uint64_t ref_table_id,
    const common::ObIArray<common::ObObjectID> *part_ids,
    const common::ObDataTypeCastParams &dtc_params,
    const bool is_dml_table,
    common::ObIArray<ObRawExpr*> *sort_exprs)
{
  int ret = common::OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
    SQL_OPT_LOG(WARN, "failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_FAIL(init(table_schema, stmt, exec_ctx, filter_exprs, table_id,
                          ref_table_id, part_ids, dtc_params, is_dml_table, sort_exprs))) {
    SQL_OPT_LOG(WARN, "failed to init", K(ret), K(ref_table_id));
  }
  return ret;
}
int ObTableLocation::init_location(ObSqlSchemaGuard *schema_guard,
          const ObDMLStmt &stmt,
          ObExecContext *exec_ctx,
          const common::ObIArray<ObRawExpr*> &filter_exprs,
          const uint64_t table_id,
          const uint64_t ref_table_id,
          const ObIArray<ObObjectID> *part_ids,
          const common::ObDataTypeCastParams &dtc_params,
          const bool is_dml_table,
          common::ObIArray<ObRawExpr*> *sort_exprs)
{
  int ret = common::OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    SQL_OPT_LOG(WARN, "failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_FAIL(init(table_schema, stmt, exec_ctx, filter_exprs, table_id,
                          ref_table_id, part_ids, dtc_params, is_dml_table, sort_exprs))) {
    SQL_OPT_LOG(WARN, "failed to init", K(ret), K(ref_table_id));
  }
  return ret;
}
int ObTableLocation::init_table_location(ObExecContext &exec_ctx,
                                         ObSqlSchemaGuard &schema_guard,
                                         uint64_t table_id,
                                         uint64_t ref_table_id,
                                         const ObDMLStmt &stmt,
                                         const RowDesc &row_desc,
                                         const bool is_dml_table, /*whether the ref_table is modified*/
                                         const ObOrderDirection &direction)
{
  int ret = OB_SUCCESS;
  UNUSED(direction);
  const ObTableSchema *table_schema = NULL;
  const ObRawExpr *part_raw_expr = NULL;
  RowDesc loc_row_desc;
  loc_meta_.table_loc_id_ = table_id;
  loc_meta_.ref_table_id_ = ref_table_id;
  stmt_type_ = stmt.get_stmt_type();
  is_partitioned_ = true;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt query ctx is null");
  } else if (OB_FAIL(schema_guard.get_table_schema(loc_meta_.ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(loc_meta_.ref_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(loc_meta_.ref_table_id_));
  } else {
    table_type_ = table_schema->get_table_type();
    loc_meta_.is_external_table_ = table_schema->is_external_table();
    loc_meta_.is_external_files_on_disk_ =
        ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location());
  }

  if (OB_FAIL(ret)) {
  } else if (PARTITION_LEVEL_ZERO == (part_level_ = table_schema->get_part_level())) {
    is_partitioned_ = false; //Non-partitioned table, do not need to calc partition id
  } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition level only support PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO", K(ret), K(part_level_));
  } else if (FALSE_IT(is_oracle_temp_table_ = table_schema->is_oracle_tmp_table())) {
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
      (part_type_ = table_schema->get_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret),  K(loc_meta_.ref_table_id_), K(part_type_));
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
      (subpart_type_ = table_schema->get_sub_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret),  K(loc_meta_.ref_table_id_), K(subpart_type_));
  } else if (OB_FAIL(loc_row_desc.init())) {
    LOG_WARN("init loc row desc failed", K(ret));
  } else if (OB_FAIL(loc_row_desc.assign(row_desc))) {
    LOG_WARN("assign location row desc failed", K(ret));
  } else {
    if (stmt.is_insert_stmt()) {
       ret = OB_INVALID_ARGUMENT;
       LOG_WARN("insert needn't call this function", K(ret));
    } else if (OB_ISNULL(part_raw_expr = stmt.get_part_expr(table_id, loc_meta_.ref_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition expr is null", K(table_id), K(loc_meta_.ref_table_id_));
    } else if ((PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ ||
                PARTITION_FUNC_TYPE_INTERVAL == part_type_) &&
                OB_FAIL(can_get_part_by_range_for_range_columns(part_raw_expr, is_valid_range_columns_part_range_))) {
      LOG_WARN("failed ot check can get part by range for range columns", K(ret));
    } else if (OB_FAIL(can_get_part_by_range_for_temporal_column(part_raw_expr, is_valid_temporal_part_range_))) {
      LOG_WARN("failed to check can get part by range for temporal column", K(ret));
    } else if (FALSE_IT(is_col_part_expr_ = part_raw_expr->is_column_ref_expr())) {
    }
    if (OB_SUCC(ret)) {
      ObTempExpr *se_temp_expr = NULL;
      OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(part_raw_expr, loc_row_desc,
                                                      exec_ctx.get_allocator(),
                                                      exec_ctx.get_my_session(),
                                                      schema_guard.get_schema_guard(),
                                                      se_temp_expr));
      CK(OB_NOT_NULL(se_temp_expr));
      OX(se_part_expr_ = se_temp_expr);
    }
    if (OB_FAIL(ret)) {
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_
        || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type_
        || PARTITION_FUNC_TYPE_INTERVAL == part_type_) {
      if (OB_FAIL(part_projector_.init_part_projector(&exec_ctx,
                                                      part_raw_expr, loc_row_desc))) {
        LOG_WARN("init part projector failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool is_weak_read = false;
    if (OB_FAIL(get_is_weak_read(stmt,
                                 exec_ctx.get_my_session(),
                                 exec_ctx.get_sql_ctx(),
                                 is_weak_read))) {
      LOG_WARN("get is weak read failed", K(ret));
    } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
      loc_meta_.is_dup_table_ = 1;
    }
    if (OB_SUCC(ret)) {
      if (is_dml_table) {
        loc_meta_.select_leader_ = 1;
      } else if (!is_weak_read) {
        loc_meta_.select_leader_ = loc_meta_.is_dup_table_ ? 0 : 1;
      } else {
        loc_meta_.select_leader_ = 0;
        loc_meta_.is_weak_read_ = 1;
      }
    }
  }
  if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
    const ObRawExpr *subpart_raw_expr = NULL;
    if (OB_UNLIKELY(NULL == (subpart_raw_expr = stmt.get_subpart_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub partition expr not in stmt", K(ret), K(loc_meta_));
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ &&
                OB_FAIL(can_get_part_by_range_for_range_columns(subpart_raw_expr, is_valid_range_columns_subpart_range_))) {
      LOG_WARN("failed to check can get part by range for range columns", K(ret));
    } else if (OB_FAIL(can_get_part_by_range_for_temporal_column(subpart_raw_expr, is_valid_temporal_subpart_range_))) {
      LOG_WARN("failed to check can get part by range for temporal column", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObTempExpr *se_temp_expr = NULL;
      OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(subpart_raw_expr, loc_row_desc,
                                                      exec_ctx.get_allocator(),
                                                      exec_ctx.get_my_session(),
                                                      schema_guard.get_schema_guard(),
                                                      se_temp_expr));
      CK(OB_NOT_NULL(se_temp_expr));
      OX(se_subpart_expr_ = se_temp_expr);
    }
    if (OB_FAIL(ret)) {
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_
        || PARTITION_FUNC_TYPE_LIST_COLUMNS == subpart_type_) {
      if (OB_FAIL(
          part_projector_.init_subpart_projector(
              &exec_ctx,
              subpart_raw_expr,
              loc_row_desc))) {
        LOG_WARN("init subpart projector failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_projector_.set_column_cnt(loc_row_desc.get_column_num());
    inited_ = true;
  }
  return ret;
}

int ObTableLocation::init_table_location_with_rowkey(ObSqlSchemaGuard &schema_guard,
                                                     uint64_t table_id,
                                                     ObExecContext &exec_ctx,
                                                     const bool is_dml_table /*= true*/)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObArray<uint64_t> column_ids;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_ISNULL(session_info = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker.get_table_schema(session_info->get_effective_tenant_id(),
                                                     table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(session_info->get_effective_tenant_id()), K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(table_id));
  } else if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column ids", KR(ret));
  } else if (OB_UNLIKELY(column_descs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty column desc", KR(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(column_descs.at(i).col_id_);
    int64_t part_key_pos = column_schema->get_part_key_pos();
    int64_t sub_part_key_pos = column_schema->get_subpart_key_pos();
    if (part_key_pos > 0 || sub_part_key_pos > 0) {
      if(OB_FAIL(column_ids.push_back(column_descs.at(i).col_id_))) {
        LOG_WARN("fail to push back column id", KR(ret), K(column_descs.at(i).col_id_), K(column_ids));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_table_location_with_column_ids(schema_guard,
                                                           table_id,
                                                           column_ids,
                                                           exec_ctx,
                                                           is_dml_table))) {
      LOG_WARN("init table location with column ids failed", K(ret), K(table_id), K(column_ids));
    }
  }
  return ret;
}

int ObTableLocation::init_table_location_with_column_ids(ObSqlSchemaGuard &schema_guard,
                                                         uint64_t table_id,
                                                         const ObIArray<uint64_t> &column_ids,
                                                         ObExecContext &exec_ctx,
                                                         const bool is_dml_table)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_FAIL(schema_checker.init(schema_guard))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else if (OB_ISNULL(session_info = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker.get_table_schema(session_info->get_effective_tenant_id(),
                                                     table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(session_info->get_effective_tenant_id()), K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(table_id));
  } else {
    //找到真实的主表id，并且用主表id去解析partition expr
    //由于局部索引本身没有分区信息，局部索引的分区信息跟主表相同，所以这里需要传主表的table_id
    //而全局索引的分区信息跟主表是独立的，所以全局索引应该传递自己的table_id
    uint64_t real_table_id = (table_schema->is_index_local_storage() || table_schema->is_aux_lob_table())
        ? table_schema->get_data_table_id() : table_id;
    ObResolverParams resolver_ctx;
    ObRawExprFactory expr_factory(allocator_);
    ObStmtFactory stmt_factory(allocator_);
    TableItem table_item;
    resolver_ctx.allocator_  = &allocator_;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = session_info;
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = real_table_id;
    table_item.ref_id_ = real_table_id;
    table_item.type_ = TableItem::BASE_TABLE;
    RowDesc row_desc;
    //这里只是为了使用resolver解析partition expr的接口，任何一个语句的resolver都有这种能力
    //用delete resolver的原因是delete resolver最简单
    SMART_VAR (ObDeleteResolver, delete_resolver, resolver_ctx) {
      ObDeleteStmt *delete_stmt = nullptr;
      const ObTableSchema *real_table_schema = table_schema;
      if (OB_ISNULL(resolver_ctx.query_ctx_) ||
          OB_ISNULL(delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("create query_ctx or delete stmt failed", K_(resolver_ctx.query_ctx), K(delete_stmt));
      } else if (OB_FAIL(delete_stmt->get_table_items().push_back(&table_item))) {
        LOG_WARN("store table item failed", K(ret));
      } else if (OB_FAIL(delete_stmt->set_table_bit_index(real_table_id))) {
        LOG_WARN("set table bit index failed", K(ret), K(real_table_id));
      } else if (OB_UNLIKELY(table_schema->is_index_local_storage() || table_schema->is_aux_lob_table())
          && OB_FAIL(schema_guard.get_table_schema(real_table_id, real_table_schema))) {
        LOG_WARN("get real table schema failed", K(ret), K(real_table_id));
        //由于局部索引没有自己的partition信息，所以如果是计算索引表的partition信息，需要去主表获取分区规则表达式
      } else if (OB_FAIL(delete_resolver.resolve_table_partition_expr(table_item, *real_table_schema))) {
        LOG_WARN("resolve table partition expr failed", K(ret));
      } else if (OB_FAIL(generate_rowkey_desc(*delete_stmt,
                                              column_ids,
                                              real_table_id,
                                              expr_factory,
                                              row_desc))) {
        LOG_WARN("generate rowkey desc failed", K(ret), K(real_table_id));
      } else if (OB_FAIL(init_table_location(exec_ctx,
                                            schema_guard,
                                            real_table_id,
                                            real_table_id,
                                            *delete_stmt,
                                            row_desc,
                                            is_dml_table,
                                            default_asc_direction()))) {
        LOG_WARN("init table location failed", K(ret), K(real_table_id));
      } else if (OB_FAIL(clear_columnlized_in_row_desc(row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      } else {
        loc_meta_.ref_table_id_ = table_id;
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_not_partitioned_table_ids(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> partition_ids;
  ObDASTabletMapper tablet_mapper;
  if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(
                  loc_meta_.ref_table_id_, tablet_mapper, &loc_meta_.related_table_ids_))) {
    LOG_WARN("fail to get das tablet mapper", K(ret));
  } else if (OB_FAIL(tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids))) {
    LOG_WARN("fail to get non partition tablet id", K(ret));
  } else if (!(1 == tablet_ids.count() && 1 == partition_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tablet ids or partition ids size more than 1", K(ret));
  } else {
    tablet_id_ = tablet_ids.at(0);
    object_id_ = partition_ids.at(0);
    DASRelatedTabletMap *related_map = static_cast<DASRelatedTabletMap *>(
          tablet_mapper.get_related_table_info().related_map_);
    if (OB_NOT_NULL(related_map)) {
      related_list_.assign(related_map->get_list());
    }
  }
  return ret;
}

int ObTableLocation::init(
    const ObTableSchema *table_schema,
    const ObDMLStmt &stmt,
    ObExecContext *exec_ctx,
    const ObIArray<ObRawExpr*> &filter_exprs,
    const uint64_t table_id,
    const uint64_t ref_table_id,
    const ObIArray<ObObjectID> *part_ids,
    const ObDataTypeCastParams &dtc_params,
    const bool is_dml_table,  /*whether the ref_table is modified*/
    common::ObIArray<ObRawExpr *> *sort_exprs)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  loc_meta_.table_loc_id_ = table_id;
  loc_meta_.ref_table_id_ = ref_table_id;
  stmt_type_ = stmt.get_stmt_type();
  is_partitioned_ = true;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("table location init twice", K(ret));
  } else if (OB_INVALID_ID == table_id
             || OB_INVALID_ID == ref_table_id
             || OB_ISNULL(table_schema)
             || OB_ISNULL(exec_ctx)
             || OB_ISNULL(session_info = exec_ctx->get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(table_id), K(ref_table_id), K(session_info), K(ret));
  } else {
    table_type_ = table_schema->get_table_type();
    loc_meta_.is_external_table_ = table_schema->is_external_table();
    loc_meta_.is_external_files_on_disk_ =
        ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location());
  }

  if (OB_FAIL(ret)) {
  } else if (PARTITION_LEVEL_ZERO == (part_level_ = table_schema->get_part_level())) {
    is_partitioned_ = false; //Non-partitioned table, do not need to calc partition id
  } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition level only support PARTITION_LEVEL_ONE or PARTITION_LEVEL_TWO", K(ret), K(part_level_));
  } else if (FALSE_IT(is_oracle_temp_table_ = table_schema->is_oracle_tmp_table())) {
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
      (part_type_ = table_schema->get_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret),  K(ref_table_id), K(part_type_));
  } else if (OB_UNLIKELY(PARTITION_FUNC_TYPE_MAX <=
      (subpart_type_ = table_schema->get_sub_part_option().get_part_func_type()))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Part func type error", K(ret),  K(ref_table_id), K(subpart_type_));
  } else if (stmt.is_insert_stmt() && !static_cast<const ObInsertStmt&>(stmt).value_from_select()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert needn't call this function", K(ret));
  } else {
    is_in_hit_ = false;
    if (OB_FAIL(record_in_dml_partition_info(stmt, exec_ctx, filter_exprs, is_in_hit_, table_schema))) { //这是一个特殊路径，针对in filter条件
      LOG_WARN("fail to record_in_dml_partition_info", K(ret));
    } else if (!is_in_hit_) {
      bool is_in_range_optimization_enabled = false;
      if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(stmt.get_query_ctx()->get_global_hint(),
                                                                    session_info,
                                                                    is_in_range_optimization_enabled))) {
        LOG_WARN("failed to check in range optimization enabled", K(ret));
      } else if (OB_FAIL(record_not_insert_dml_partition_info(stmt, exec_ctx, table_schema, filter_exprs, dtc_params,
                                                              is_in_range_optimization_enabled))) {
          LOG_WARN("Fail to record select or update partition info", K(stmt_type_), K(ret));
      } else if (OB_FAIL(get_not_insert_dml_part_sort_expr(stmt, sort_exprs))) {
        LOG_WARN("Failed to get not insert dml sort key with parts", K(ret));
      } else { }
    }
  }
  if (OB_SUCC(ret) && NULL != part_ids && !part_ids->empty()) {
    ret = part_hint_ids_.assign(*part_ids);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(table_id), K(ref_table_id), K(ret));
    } else {
      inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    bool is_weak_read = false;
    if (OB_FAIL(get_is_weak_read(stmt, session_info, exec_ctx->get_sql_ctx(), is_weak_read))) {
      LOG_WARN("get is weak read failed", K(ret));
    } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
      loc_meta_.is_dup_table_ = 1;
    }
    if (is_dml_table) {
      loc_meta_.select_leader_ = 1;
    } else if (!is_weak_read) {
      loc_meta_.select_leader_ = loc_meta_.is_dup_table_ ? 0 : 1;
    } else {
      loc_meta_.select_leader_ = 0;
      loc_meta_.is_weak_read_ = 1;
    }
  }
  return ret;
}

int ObTableLocation::get_is_weak_read(const ObDMLStmt &dml_stmt,
                                      const ObSQLSessionInfo *session,
                                      const ObSqlCtx *sql_ctx,
                                      bool &is_weak_read)
{
  int ret = OB_SUCCESS;
  is_weak_read = false;
  if (OB_ISNULL(session) || OB_ISNULL(sql_ctx) || OB_ISNULL(dml_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null", K(ret), K(session), K(sql_ctx));
  } else if (dml_stmt.get_query_ctx()->has_dml_write_stmt_ ||
             dml_stmt.get_query_ctx()->is_contain_select_for_update_ ||
             dml_stmt.get_query_ctx()->is_contain_inner_table_) {
    is_weak_read = false;
  } else if (share::ObTenantEnv::get_tenant() == nullptr) { //table api can't invoke MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID
    is_weak_read = false;
  } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
    is_weak_read = true;
  } else {
    ObConsistencyLevel consistency_level = INVALID_CONSISTENCY;
    ObTxConsistencyType trans_consistency_type = ObTxConsistencyType::INVALID;
    if (stmt::T_SELECT == dml_stmt.get_stmt_type()) {
      if (sql_ctx->is_protocol_weak_read_) {
        consistency_level = WEAK;
      } else if (OB_UNLIKELY(INVALID_CONSISTENCY
             != dml_stmt.get_query_ctx()->get_global_hint().read_consistency_)) {
        consistency_level = dml_stmt.get_query_ctx()->get_global_hint().read_consistency_;
      } else {
        consistency_level = session->get_consistency_level();
      }
    } else {
      consistency_level = STRONG;
    }
    if (OB_FAIL(ObSqlTransControl::decide_trans_read_interface_specs(
        consistency_level,
        trans_consistency_type))) {
      LOG_WARN("fail to decide trans read interface specs", K(ret),
               K(dml_stmt.get_stmt_type()),
//               K(dml_stmt.get_literal_stmt_type()),
               K(dml_stmt.get_query_ctx()->is_contain_select_for_update_),
               K(dml_stmt.get_query_ctx()->is_contain_inner_table_),
               K(consistency_level),
               KPC(session));
    } else {
      // 判断是否优先读备副本
      is_weak_read = (ObTxConsistencyType::BOUNDED_STALENESS_READ == trans_consistency_type);
    }
  }
  return ret;
}

int ObTableLocation::calculate_candi_tablet_locations(
    ObExecContext &exec_ctx,
    const ParamStore &params,
    ObCandiTabletLocIArray &candi_tablet_locs,
    const ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 8> partition_ids;
  ObSEArray<ObObjectID, 8> first_level_part_ids;
  ObSEArray<ObTabletID, 8> tablet_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (OB_FAIL(calculate_tablet_ids(exec_ctx,
                                          params,
                                          tablet_ids,
                                          partition_ids,
                                          first_level_part_ids,
                                          dtc_params))) {
    LOG_WARN("Failed to calculate partition ids", K(ret));
  } else if (OB_FAIL(get_tablet_locations(exec_ctx.get_das_ctx(),
                                          tablet_ids,
                                          partition_ids,
                                          first_level_part_ids,
                                          candi_tablet_locs))) {
    LOG_WARN("Failed to set partition locations", K(ret), K(partition_ids), K(tablet_ids));
  } else {}//do nothing

  return ret;
}

int ObTableLocation::calculate_single_tablet_partition(ObExecContext &exec_ctx,
                                                       const ParamStore &params,
                                                       const ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  ObDASTabletMapper tablet_mapper;
  ObDASCtx &das_ctx = exec_ctx.get_das_ctx();
  tablet_mapper.set_non_partitioned_table_ids(tablet_id_, object_id_, &related_list_);
  if (OB_FAIL(das_ctx.get_das_tablet_mapper(loc_meta_.ref_table_id_,
                                            tablet_mapper,
                                            &loc_meta_.related_table_ids_))) {
    LOG_WARN("failed to get das tablet mapper", K(ret));
  } else {
    DASRelatedTabletMap *map =
        static_cast<DASRelatedTabletMap*>(tablet_mapper.get_related_table_info().related_map_);
    if (OB_NOT_NULL(map) && !related_list_.empty() && OB_FAIL(map->assign(related_list_))) {
      LOG_WARN("failed to assign related map list", K(ret));
    }
  }
  LOG_DEBUG("calculate single tablet id end", K(loc_meta_), K(object_id_), K(tablet_id_));
  NG_TRACE(tl_calc_part_id_end);

  ObDASTableLoc *table_loc = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObDASTableLocMeta *final_meta = nullptr;
  LOG_DEBUG("das table loc assign begin", K_(loc_meta));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(das_ctx.build_table_loc_meta(loc_meta_, final_meta))) {
    LOG_WARN("build table loc meta failed", K(ret));
  } else if (OB_FAIL(das_ctx.extended_table_loc(*final_meta, table_loc))) {
    LOG_WARN("extended table loc failed", K(ret), K(loc_meta_));
  } else if (OB_FAIL(das_ctx.extended_tablet_loc(*table_loc, tablet_id_, tablet_loc, object_id_))) {
    LOG_WARN("extended tablet loc failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    das_ctx.clear_all_location_info();
  }
  return ret;
}

int ObTableLocation::calculate_final_tablet_locations(ObExecContext &exec_ctx,
                                                      const ParamStore &params,
                                                      const ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (is_non_partition_optimized_) {
    // fast path for single tablet
    if (OB_FAIL(calculate_single_tablet_partition(exec_ctx, params, dtc_params))) {
      LOG_WARN("failed to calculate single final tablet location", K(ret));
    }
  } else {
    ObSEArray<ObObjectID, 8> partition_ids;
    ObSEArray<ObObjectID, 8> first_level_part_ids;
    ObSEArray<ObTabletID, 8> tablet_ids;
    if (OB_FAIL(calculate_tablet_ids(exec_ctx,
                                     params,
                                     tablet_ids,
                                     partition_ids,
                                     first_level_part_ids,
                                     dtc_params))) {
      LOG_WARN("failed to calculate final tablet ids", K(ret));
    } else if (OB_FAIL(add_final_tablet_locations(exec_ctx.get_das_ctx(),
                                                  tablet_ids,
                                                  partition_ids,
                                                  first_level_part_ids))) {
      LOG_WARN("failed to add final tablet locations to das_ctx", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::init_partition_ids_by_rowkey2(ObExecContext &exec_ctx,
                                                   ObSQLSessionInfo &session_info,
                                                   ObSchemaGetterGuard &schema_guard,
                                                   uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard sql_schema_guard;
  sql_schema_guard.set_schema_guard(&schema_guard);
  exec_ctx.set_my_session(&session_info);
  if (OB_UNLIKELY(is_virtual_table(table_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Calculate virtual table partition id with rowkey");
  } else if (OB_FAIL(init_table_location_with_rowkey(sql_schema_guard, table_id, exec_ctx))) {
    LOG_WARN("implicit init location failed", K(table_id), K(ret));
  }
  return ret;
}

//FIXME
int ObTableLocation::calculate_partition_ids_by_rows2(ObSQLSessionInfo &session_info,
                                                       ObSchemaGetterGuard &schema_guard,
                                                       uint64_t table_id,
                                                       ObIArray<ObNewRow> &part_rows,
                                                       ObIArray<ObTabletID> &tablet_ids,
                                                       ObIArray<ObObjectID> &part_ids) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_TABLE_LOCATION);
  SMART_VAR(ObExecContext, exec_ctx, allocator) {
    ObSqlSchemaGuard sql_schema_guard;
    ObSqlCtx sql_ctx;
    sql_ctx.schema_guard_ = &schema_guard;
    sql_schema_guard.set_schema_guard(&schema_guard);
    exec_ctx.set_sql_ctx(&sql_ctx);
    exec_ctx.set_my_session(&session_info);
    ObDASTabletMapper tablet_mapper;
    if (OB_UNLIKELY(is_virtual_table(table_id))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Calculate virtual table partition id with rowkey");
    } else if (!is_partitioned_) {
      ObObjectID object_id = 0;
      ObTabletID tablet_id;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(sql_schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(table_id), KR(ret));
      } else if (OB_FAIL(table_schema->get_tablet_and_object_id(tablet_id, object_id))) {
        LOG_WARN("fail to get tablet and object", KR(ret));
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_rows.count(); i ++) {
          if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
            LOG_WARN("fail to push tablet id", KR(ret));
          } else if (OB_FAIL(part_ids.push_back(object_id))) {
            LOG_WARN("fail to push object id", KR(ret));
          }
        }
      }
    } else if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(table_id,
                                                                    tablet_mapper,
                                                                    &loc_meta_.related_table_ids_))) {
      LOG_WARN("get das tablet mapper failed", KR(ret), K(table_id));
    } else {//TODO: copied from calc_partition_ids_by_rowkey()
      ObSEArray<ObObjectID, 1> tmp_part_ids;
      ObSEArray<ObTabletID, 1> tmp_tablet_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_rows.count(); ++i) {
        tmp_part_ids.reset();
        tmp_tablet_ids.reset(); //must reset
        ObNewRow &part_row = part_rows.at(i);
        if (PARTITION_LEVEL_ONE == part_level_) {
          if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, part_row, tmp_tablet_ids, tmp_part_ids))) {
            LOG_WARN("calc partition id by row failed", K(ret));
          }
        } else {
          ObSEArray<ObObjectID, 1> tmp_part_ids2;
          ObSEArray<ObTabletID, 1> tmp_tablet_ids2;
          if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                              tablet_mapper,
                                              part_row,
                                              tmp_tablet_ids2,
                                              tmp_part_ids2))) {
            LOG_WARN("calc partition id by row failed", K(ret));
          } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                                      tablet_mapper,
                                                      part_row,
                                                      tmp_tablet_ids,
                                                      tmp_part_ids,
                                                      &tmp_part_ids2))) {
            LOG_WARN("calc sub partition id by row failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObObjectID part_id;
          ObTabletID tablet_id;
          if (OB_UNLIKELY(tmp_tablet_ids.empty() && tmp_part_ids.empty())) {
            part_id = OB_INVALID_PARTITION_ID;
            tablet_id = ObTabletID::INVALID_TABLET_ID;
          } else if (OB_UNLIKELY((tmp_tablet_ids.count() != 1) || (tmp_part_ids.count() != 1))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid tablet ids or partition ids", KR(ret), K(tmp_tablet_ids), K(tmp_part_ids));
          } else {
            part_id = tmp_part_ids.at(0);
            tablet_id = tmp_tablet_ids.at(0);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
            LOG_WARN("fail to push tablet id", KR(ret));
          } else if (OB_FAIL(part_ids.push_back(part_id))) {
            LOG_WARN("fail to push object id", KR(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLocation::calculate_partition_ids_by_rowkey(ObSQLSessionInfo &session_info,
                                                       ObSchemaGetterGuard &schema_guard,
                                                       uint64_t table_id,
                                                       const ObIArray<ObRowkey> &rowkeys,
                                                       ObIArray<ObTabletID> &tablet_ids,
                                                       ObIArray<ObObjectID> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_TABLE_LOCATION);
  SMART_VAR(ObExecContext, exec_ctx, allocator) {
    ObSqlSchemaGuard sql_schema_guard;
    ObSqlCtx sql_ctx;
    sql_ctx.schema_guard_ = &schema_guard;
    sql_schema_guard.set_schema_guard(&schema_guard);
    exec_ctx.set_my_session(&session_info);
    exec_ctx.set_sql_ctx(&sql_ctx);
    ObDASTabletMapper tablet_mapper;
    if (is_non_partition_optimized_ && table_id == loc_meta_.ref_table_id_) {
      tablet_mapper.set_non_partitioned_table_ids(tablet_id_, object_id_, &related_list_);
    }
    if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(
          table_id, tablet_mapper, &loc_meta_.related_table_ids_))) {
      LOG_WARN("fail to get das tablet mapper", K(ret));
    } else if (OB_UNLIKELY(is_virtual_table(table_id))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Calculate virtual table partition id with rowkey");
    } else if (OB_UNLIKELY(rowkeys.count() <= 0)) {
      if (PARTITION_LEVEL_ONE == part_level_) {
        if (OB_FAIL(get_all_part_ids(tablet_mapper, tablet_ids, partition_ids))) {
          LOG_WARN("Failed to get all part ids", K(ret));
        }
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        ObSEArray<ObObjectID, 5> tmp_part_ids;
        ObSEArray<ObTabletID, 5> tmp_tablet_ids;
        if (OB_FAIL(get_all_part_ids(tablet_mapper, tmp_tablet_ids, tmp_part_ids))) {
          LOG_WARN("Failed to get all part ids", K(ret));
        } else if (OB_FAIL(get_all_part_ids(tablet_mapper,
                                            tablet_ids,
                                            partition_ids,
                                            &tmp_part_ids))) {
          LOG_WARN("Failed to get all subpart ids", K(ret));
        } else { }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unkown part level", K(ret), K(part_level_), K(is_partitioned_));
      }
    } else if (OB_FAIL(init_table_location_with_rowkey(sql_schema_guard, table_id, exec_ctx))) {
      LOG_WARN("implicit init location failed", K(table_id), K(ret));
    } else if (OB_FAIL(calc_partition_ids_by_rowkey(exec_ctx,
                                                    tablet_mapper,
                                                    rowkeys,
                                                    tablet_ids,
                                                    partition_ids))) {
      LOG_WARN("calc parttion ids by rowkey failed", K(ret));
    }
  }

  return ret;
}

int ObTableLocation::calculate_tablet_id_by_row(ObExecContext &exec_ctx,
                                                uint64_t table_id,
                                                const ObIArray<uint64_t> &column_ids,
                                                ObNewRow &cur_row,
                                                ObIArray<ObTabletID> &tablet_ids,
                                                ObIArray<ObObjectID> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObDASTabletMapper tablet_mapper;
  if (is_non_partition_optimized_ && table_id == loc_meta_.ref_table_id_) {
    tablet_mapper.set_non_partitioned_table_ids(tablet_id_, object_id_, &related_list_);
  }
  if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(table_id, tablet_mapper,
                                                          &loc_meta_.related_table_ids_))) {
    LOG_WARN("fail to get das tablet mapper", K(ret));
  } else if (!is_partitioned_) {
    if (OB_FAIL(tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids))) {
      LOG_WARN("get non partition tablet id failed", K(ret), K(table_id));
    }
  } else if (PARTITION_LEVEL_ONE == part_level_) {
    if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                         tablet_mapper,
                                         cur_row,
                                         tablet_ids,
                                         partition_ids))) {
      LOG_WARN("calc partition id by row failed", K(ret));
    }
  } else {
    ObSEArray<ObTabletID, 1> tmp_tablet_ids;
    ObSEArray<ObObjectID, 1> tmp_partition_ids;
    if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                         tablet_mapper,
                                         cur_row,
                                         tmp_tablet_ids,
                                         tmp_partition_ids))) {
      LOG_WARN("calc the first level partition id by row failed", K(ret), K(cur_row));
    } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                                tablet_mapper,
                                                cur_row,
                                                tablet_ids,
                                                partition_ids,
                                                &tmp_partition_ids))) {
      LOG_WARN("calc the secondary level partition id by row failed", K(ret), K(cur_row));
    }
  }
  return ret;
}

int ObTableLocation::calculate_tablet_ids(ObExecContext &exec_ctx,
                                          const ParamStore &params,
                                          ObIArray<ObTabletID> &tablet_ids,
                                          ObIArray<ObObjectID> &partition_ids,
                                          ObIArray<ObObjectID> &first_level_part_ids,
                                          const ObDataTypeCastParams &dtc_params) const
{
  int ret = OB_SUCCESS;
  ObDASTabletMapper tablet_mapper;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (is_non_partition_optimized_
             && FALSE_IT(tablet_mapper.set_non_partitioned_table_ids(
                             tablet_id_, object_id_, &related_list_))) {
  } else if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(
                      loc_meta_.ref_table_id_, tablet_mapper, &loc_meta_.related_table_ids_))) {
    LOG_WARN("fail to get das tablet mapper", K(ret));
  } else {
    ObPartitionIdMap partition_id_map;
    if (is_in_hit_) { //判断是否是in类型
      if (OB_FAIL(calc_partition_ids_by_in_expr(exec_ctx, tablet_mapper, tablet_ids, partition_ids, dtc_params))) {
        LOG_WARN("fail to calc_partition_ids_by_in_expr", K(ret));
      }
    } else {
      if (!is_partitioned_) {
        OZ(tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids));
      } else if (PARTITION_LEVEL_ONE == part_level_) {
        if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params,
                                                    calc_node_, gen_col_node_,
                                                    se_gen_col_expr_,
                                                    part_get_all_, tablet_ids,
                                                    partition_ids, dtc_params))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        }
      } else {
        ObSEArray<ObTabletID, 1> tmp_tablet_ids;
        ObSEArray<ObObjectID, 1> tmp_part_ids;
        if (OB_FAIL(partition_id_map.create(128, "PartitionIdMap", "PartitionIdMap"))) {
          LOG_WARN("failed to create partition id map");
        } else if (OB_FALSE_IT(tablet_mapper.set_partition_id_map(&partition_id_map))) {
        } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params,
                                                           calc_node_, gen_col_node_,
                                                           se_gen_col_expr_,
                                                           part_get_all_, tmp_tablet_ids,
                                                           tmp_part_ids, dtc_params))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params,
                                                           subcalc_node_, sub_gen_col_node_,
                                                           se_sub_gen_col_expr_, subpart_get_all_,
                                                           tablet_ids, partition_ids,
                                                           dtc_params, &tmp_part_ids))) {
          LOG_WARN("Calc partition ids by calc node error", K_(stmt_type), K(ret));
        } else { }
      }
    }

    if ( OB_SUCC(ret)
        && 0 == partition_ids.count()
        && (stmt::T_INSERT == stmt_type_
            || stmt::T_REPLACE == stmt_type_
            || check_no_partition_)) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_USER_WARN(OB_NO_PARTITION_FOR_GIVEN_VALUE);
    }
    //deal partition hint
    if (OB_SUCCESS == ret && part_hint_ids_.count() > 0) {
      if (OB_FAIL(deal_partition_selection(tablet_ids, partition_ids))) {
        LOG_WARN("deal partition select failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && partition_ids.empty()) {
      ObObjectID default_partition_id = OB_INVALID_ID;
      ObTabletID default_tablet_id;
      if (loc_meta_.is_external_table_) {
        default_partition_id = 0;
        default_tablet_id = default_partition_id;
      } else if (OB_FAIL(tablet_mapper.get_default_tablet_and_object_id(part_level_,
                                                                 part_hint_ids_,
                                                                 default_tablet_id,
                                                                 default_partition_id))) {
        LOG_WARN("get default tablet and object id failed", K(ret), K(part_hint_ids_));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_ids.push_back(default_partition_id))) {
        LOG_WARN("store default partition id failed", K(ret));
      } else if (OB_FAIL(tablet_ids.push_back(default_tablet_id))) {
        LOG_WARN("store default tablet id failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == part_level_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
        ObObjectID first_level_part_id;
        if (OB_FAIL(tablet_mapper.get_partition_id_map(partition_ids.at(i), first_level_part_id))) {
          LOG_WARN("failed to get partition id map", K(i), K(partition_ids.at(i)));
        } else if (OB_FAIL(first_level_part_ids.push_back(first_level_part_id))) {
          LOG_WARN("faield to push back log part id");
        }
      }
    }
  }

  LOG_DEBUG("calculate tablet ids end", K(loc_meta_), K(partition_ids), K(tablet_ids));
  NG_TRACE(tl_calc_part_id_end);
  return ret;
}

int ObTableLocation::get_tablet_locations(ObDASCtx &das_ctx,
                                          const ObIArray<ObTabletID> &tablet_ids,
                                          const ObIArray<ObObjectID> &partition_ids,
                                          const ObIArray<ObObjectID> &first_level_part_ids,
                                          ObCandiTabletLocIArray &candi_tablet_locs) const
{
  return das_ctx.get_location_router().nonblock_get_candi_tablet_locations(loc_meta_,
                                                                           tablet_ids,
                                                                           partition_ids,
                                                                           first_level_part_ids,
                                                                           candi_tablet_locs);
}

int ObTableLocation::add_final_tablet_locations(ObDASCtx &das_ctx,
                                                const ObIArray<ObTabletID> &tablet_ids,
                                                const ObIArray<ObObjectID> &partition_ids,
                                                const ObIArray<ObObjectID> &first_level_part_ids) const
{
  return das_ctx.add_final_table_loc(loc_meta_,
                                     tablet_ids,
                                     partition_ids,
                                     first_level_part_ids);
}

int ObTableLocation::get_part_col_type(const ObRawExpr *expr,
                                       ObObjType &col_type,
                                       ObCollationType &collation_type,
                                       const ObTableSchema *table_schema,
                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid expr", K(ret), KPC(expr));
  } else {
    const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(expr);
    const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col->get_column_id());
    if (NULL == col_schema) {
      /**
       * there are two sence if part column can't find in index schema
       * 1. part column is generated column
       * 2. local index of heap table doesn't store part key
       */
      is_valid = false;
    } else {
      col_type = col_schema->get_data_type();
      collation_type = col_schema->get_meta_type().get_collation_type();
    }
  }
  return ret;
}


int ObTableLocation::convert_row_obj_type(const ObNewRow &from,
                                          ObNewRow &to,
                                          ObObjType col_type,
                                          ObCollationType collation_type,
                                          const ObDataTypeCastParams &dtc_params,
                                          bool &is_all,
                                          bool &is_none) const
{
  int ret = OB_SUCCESS;
  const ObObj &tmp  = from.get_cell(0);
  ObObj &out_obj = to.get_cell(0);
  ObObj tmp_out_obj;
  ObObj tmp_out_obj2;
  ObObjType res_type = ObNullType;
  is_none = false;
  if (ob_obj_type_class(tmp.get_type()) == ob_obj_type_class(col_type) &&
      tmp.get_collation_type() == collation_type) {
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
          } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(result, tmp_out_obj, tmp_out_obj2, cast_ctx, res_type, collation_type))) {
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

////考虑in的类型转换，暂时不考虑不行。判断in的value是否能转换成column列要求的类型，如果不能。直接返回所有的partition。
int ObTableLocation::calc_partition_ids_by_in_expr(ObExecContext &exec_ctx,
                                                   ObDASTabletMapper &tablet_mapper,
                                                   ObIArray<ObTabletID> &tablet_ids,
                                                   ObIArray<ObObjectID> &partition_ids,
                                                   const ObDataTypeCastParams &dtc_params) const
{
  UNUSED(dtc_params);
  int ret = OB_SUCCESS;
  if (vies_.count() != sub_vies_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid key/subkey count", K(vies_.count()), K(sub_vies_.count()));
  }
  if (OB_FAIL(ret)) {
  } else if (!is_partitioned_) {
    OZ(tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids));
  } else {
    ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
    allocator.set_label("CalcByInExpr");
    ObExprCtx expr_ctx;
    ObNewRow part_row;
    ObNewRow subpart_row;

    //init expr ctx
    if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type_, exec_ctx, allocator, expr_ctx))) {
      LOG_WARN("Failed to wrap expr ctx", K(ret));
    }

    //init all ObNewRow
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_row(allocator, 1, part_row))) {
      LOG_WARN("Failed to init row", K(ret));
    } { }//do nothing

    if (PARTITION_LEVEL_TWO == part_level_) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_row(allocator, 1, subpart_row))) {
        LOG_WARN("Failed to init row", K(ret));
      } else { }//do nothing
    }

    //calc ObNewRow for calc partition id
    int64_t key_expr_cnt = vies_.count();
    for (int64_t value_idx = 0; OB_SUCC(ret) && value_idx < key_expr_cnt; ++value_idx) {
      OZ(se_calc_value_item_row(expr_ctx, exec_ctx, vies_, 1, value_idx, part_row, part_row));
      OZ(se_calc_value_item_row(expr_ctx, exec_ctx, sub_vies_, 1, value_idx, subpart_row, subpart_row));

      ObSEArray<ObObjectID, 5> part_ids;
      ObSEArray<ObTabletID, 5> tmp_tablet_ids;
      bool is_all = false;
      bool is_none = false;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(convert_row_obj_type(part_row, part_row, part_col_type_, part_collation_type_,
            dtc_params, is_all, is_none))) {
          LOG_WARN("fail to convert_row_obj_type", K(ret));
        } else if (!is_none) {
          if (is_all) {
            if (OB_FAIL(get_all_part_ids(tablet_mapper, tmp_tablet_ids, part_ids))) {
              LOG_WARN("Failed to get all part ids", K(ret));
            }
          } else {
            if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, part_row, tmp_tablet_ids, part_ids))) {
              LOG_WARN("Calc partition id by row error", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(convert_row_obj_type(subpart_row, subpart_row, subpart_col_type_,
            subpart_collation_type_, dtc_params, is_all, is_none))) {
          LOG_WARN("fail to convert_row_obj_type", K(ret));
        } else if (!is_none) {
          if (is_all) {
            if (OB_FAIL(get_all_part_ids(tablet_mapper, tablet_ids, partition_ids, &part_ids))) {
              LOG_WARN("Failed to get all subpart ids", K(ret));
            }
          } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, subpart_row,
                                                      tablet_ids, partition_ids, &part_ids))) {
            LOG_WARN("Calc partitioin id by row error", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::set_location_calc_node(const ObDMLStmt &stmt,
                                            const ObIArray<ObRawExpr*> &filter_exprs,
                                            const ObPartitionLevel part_level,
                                            const ObDataTypeCastParams &dtc_params,
                                            ObExecContext *exec_ctx,
                                            ObTempExpr *&se_part_expr,
                                            ObTempExpr *&se_gen_col_expr,
                                            bool &is_col_part_expr,
                                            ObPartLocCalcNode *&calc_node,
                                            ObPartLocCalcNode *&gen_col_node,
                                            bool &get_all,
                                            bool &is_range_get,
                                            const bool is_in_range_optimization_enabled)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 5> part_columns;
  ObSEArray<ColumnItem, 3> gen_cols;
  const ObRawExpr *part_raw_expr = NULL;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(exec_ctx), K(ret));
  } else if (OB_FAIL(get_partition_column_info(stmt,
                                               part_level,
                                               part_columns,
                                               gen_cols,
                                               part_raw_expr,
                                               is_col_part_expr,
                                               se_part_expr,
                                               se_gen_col_expr,
                                               exec_ctx))) {
    LOG_WARN("Failed to get partition column info", K(ret));
  } else if (filter_exprs.empty()) {
    get_all = true;
  } else if (OB_FAIL(get_location_calc_node(part_level,
                                            part_columns,
                                            part_raw_expr,
                                            filter_exprs,
                                            calc_node,
                                            get_all,
                                            is_range_get,
                                            dtc_params,
                                            exec_ctx,
                                            is_in_range_optimization_enabled))) {
    LOG_WARN("Failed to get location calc node", K(ret));
  } else if (gen_cols.count() > 0) {
    //analyze information with dependented column of generated column
    bool always_true =false;
    if (OB_FAIL(get_query_range_node(part_level,
                                     gen_cols,
                                     filter_exprs,
                                     always_true,
                                     gen_col_node,
                                     dtc_params,
                                     exec_ctx,
                                     is_in_range_optimization_enabled))) {
      LOG_WARN("Get query range node error", K(ret));
    } else if (always_true) {
      gen_col_node = NULL;
    } else { }//do nothing
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
    */ //in 表达式的结构，下面代码主要按照这个结构在分析

int ObTableLocation::record_in_dml_partition_info(const ObDMLStmt &stmt,
                                                  ObExecContext *exec_ctx,
                                                  const ObIArray<ObRawExpr*> &filter_exprs,
                                                  bool &hit,
                                                  const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObRawExpr *filter_expr = NULL;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;

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

  const ObRawExpr *part_raw_expr = NULL;
  const ObRawExpr *subpart_raw_expr = NULL;

  if (OB_SUCC(ret) && hit) {
    part_raw_expr = stmt.get_part_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_);
    bool is_valid = true;
    if (OB_ISNULL(part_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null part raw expr", K(ret));
    } else if (T_REF_COLUMN != part_raw_expr->get_expr_type()) {
      hit = false;
    } else if (OB_FAIL(get_part_col_type(part_raw_expr,
                                         part_col_type_,
                                         part_collation_type_,
                                         table_schema,
                                         is_valid))) {
      LOG_WARN("fail to get part col type", K(ret));
    } else if (!is_valid) {
      hit = false;
    }
  }

  if (OB_SUCC(ret) && hit) {
    subpart_raw_expr = stmt.get_subpart_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_);
    bool is_valid = true;
    if (OB_ISNULL(subpart_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null subpart raw expr", K(ret));
    } else if (T_REF_COLUMN != subpart_raw_expr->get_expr_type()) {
      hit = false;
    } else if (OB_FAIL(get_part_col_type(subpart_raw_expr,
                                         subpart_col_type_,
                                         subpart_collation_type_,
                                         table_schema,
                                         is_valid))) {
      LOG_WARN("fail to get part col type", K(ret));
    } else if (!is_valid) {
      hit = false;
    }
  }

  if (OB_SUCC(ret) && hit) {
    if (T_OP_IN != filter_expr->get_expr_type()) {
      hit = false;
    } else {
      ObOpRawExpr *op_raw_expr = static_cast<ObOpRawExpr *>(filter_expr);
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
    ObOpRawExpr *op_left_expr = static_cast<ObOpRawExpr *>(left_expr);
    if (2 > op_left_expr->get_param_count()) {
      hit = false;
    } else {
      for (int64_t i = 0; i < op_left_expr->get_param_count(); i ++) {
        ObRawExpr *tmp = op_left_expr->get_param_expr(i);
        if (tmp->same_as(*part_raw_expr)) {
          pos1 = i;
          break;
        }
      }

      for (int64_t i = 0; i < op_left_expr->get_param_count(); i ++) {
        ObRawExpr *tmp = op_left_expr->get_param_expr(i);
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
    ObOpRawExpr *op_left_expr = static_cast<ObOpRawExpr *>(left_expr);
    ObOpRawExpr *op_right_expr = static_cast<ObOpRawExpr *>(right_expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_right_expr->get_param_count(); i ++) {
      ObRawExpr *tmp = op_right_expr->get_param_expr(i);
      if (T_OP_ROW != tmp->get_expr_type()) {
        hit = false;
        break;
      }

      ObOpRawExpr *value_expr = static_cast<ObOpRawExpr *>(tmp);
      if (op_left_expr->get_param_count() != value_expr->get_param_count() ||
          OB_ISNULL(value_expr->get_param_expr(pos1)) ||
          OB_ISNULL(value_expr->get_param_expr(pos2)) ||
          OB_UNLIKELY(!value_expr->get_param_expr(pos1)->is_const_expr() ||
                      !value_expr->get_param_expr(pos2)->is_const_expr())) {
        hit = false;
        break;
      }

      RowDesc value_row_desc;
      OZ(add_se_value_expr(value_expr->get_param_expr(pos1),
                           value_row_desc, 0, exec_ctx, vies_));
      OZ(add_se_value_expr(value_expr->get_param_expr(pos2),
                           value_row_desc, 0, exec_ctx, sub_vies_));
    }
    if (OB_SUCC(ret) && hit) {
      for (int64_t i = 0; OB_SUCC(ret) && i < vies_.count(); i++) {
        vies_.at(i).dst_type_ = op_left_expr->get_param_expr(pos1)->get_result_type().get_type();
        vies_.at(i).dst_cs_type_ = op_left_expr->get_param_expr(pos1)
                                               ->get_result_type().get_collation_type();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sub_vies_.count(); i++) {
        sub_vies_.at(i).dst_type_ = op_left_expr->get_param_expr(pos2)->get_result_type().get_type();
        sub_vies_.at(i).dst_cs_type_ = op_left_expr->get_param_expr(pos2)
                                                   ->get_result_type().get_collation_type();
      }
    }
  }

  ObSEArray<ColumnItem, 5> part_columns;
  ObSEArray<ColumnItem, 3> gen_cols;
  ObSEArray<ColumnItem, 5> subpart_columns;
  ObSEArray<ColumnItem, 3> sub_gen_cols;

  if (OB_SUCC(ret) && hit) {
    if (OB_FAIL(get_partition_column_info(stmt, PARTITION_LEVEL_ONE, part_columns, gen_cols,
                                          part_raw_expr,
                                          is_col_part_expr_, se_part_expr_,
                                          se_gen_col_expr_, exec_ctx))) {
      LOG_WARN("Failed to get partition column info", K(ret));
    } else if (OB_FAIL(get_partition_column_info(stmt, PARTITION_LEVEL_TWO, subpart_columns,
                                                 sub_gen_cols, subpart_raw_expr,
                                                 is_col_subpart_expr_,
                                                 se_subpart_expr_, se_sub_gen_col_expr_, exec_ctx))) {
      LOG_WARN("Failed to get sub partition column info", K(ret));
    }
  }

  return ret;
}

// generate values of insert sql or in expr
int ObTableLocation::add_se_value_expr(const ObRawExpr *value_expr,
                                       RowDesc &value_row_desc,
                                       int64_t expr_idx,
                                       ObExecContext *exec_ctx,
                                       ISeValueItemExprs &vies)
{
  int ret = OB_SUCCESS;
  ValueItemExpr vie;
  CK(OB_NOT_NULL(value_expr));
  CK(OB_NOT_NULL(exec_ctx));
  CK(OB_NOT_NULL(exec_ctx->get_sql_ctx()));
  if (OB_FAIL(ret)) {
  } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(value_expr->get_expr_type())) { // is const
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(value_expr);
    if (T_QUESTIONMARK == const_expr->get_expr_type()) {
      vie.idx_ = const_expr->get_value().get_unknown();
      OX(vie.type_ = QUESTMARK_TYPE);
    } else {
      OZ(deep_copy_obj(allocator_, const_expr->get_value(), vie.obj_));
      OX(vie.type_ = CONST_OBJ_TYPE);
    }
  } else { // not const, generate expr
    OX(vie.type_ = CONST_EXPR_TYPE);
    ObTempExpr *se_temp_expr = NULL;
    OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(value_expr, value_row_desc,
                                                    exec_ctx->get_allocator(),
                                                    exec_ctx->get_my_session(),
                                                    exec_ctx->get_sql_ctx()->schema_guard_,
                                                    se_temp_expr));
    CK(OB_NOT_NULL(se_temp_expr));
    OX(vie.expr_ = se_temp_expr);
  }
  if (OB_SUCC(ret)) {
    if (0 != value_row_desc.get_column_num()) {
      ObRawExpr *desc_expr = NULL;
      OZ(value_row_desc.get_column(expr_idx, desc_expr));
      CK(OB_NOT_NULL(desc_expr));
      OX(vie.dst_type_ = desc_expr->get_result_type().get_type());
      OX(vie.dst_cs_type_ = desc_expr->get_result_type().get_collation_type());
      if (ob_is_enum_or_set_type(vie.dst_type_)) {
        vie.enum_set_values_cnt_ = desc_expr->get_enum_set_values().count();
        vie.enum_set_values_ = desc_expr->get_enum_set_values().get_data();
      }
    } else {
      // for in expr, and set dest_type after call this function
      vie.dst_type_ = value_expr->get_result_type().get_type();
      vie.dst_cs_type_ = value_expr->get_result_type().get_collation_type();
      if (ob_is_enum_or_set_type(vie.dst_type_)) {
        vie.enum_set_values_cnt_ = value_expr->get_enum_set_values().count();
        vie.enum_set_values_ = value_expr->get_enum_set_values().get_data();
      }
    }
    OZ(vies.push_back(vie));
  }

  return ret;
}

int ObTableLocation::record_not_insert_dml_partition_info(
    const ObDMLStmt &stmt,
    ObExecContext *exec_ctx,
    const ObTableSchema *table_schema,
    const ObIArray<ObRawExpr*> &filter_exprs,
    const ObDataTypeCastParams &dtc_params,
    const bool is_in_range_optimization_enabled)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_location_calc_node(stmt,
                                     filter_exprs,
                                     PARTITION_LEVEL_ONE,
                                     dtc_params,
                                     exec_ctx,
                                     se_part_expr_,
                                     se_gen_col_expr_,
                                     is_col_part_expr_,
                                     calc_node_,
                                     gen_col_node_,
                                     part_get_all_,
                                     is_part_range_get_,
                                     is_in_range_optimization_enabled))) {
    LOG_WARN("failed to set location calc node for first-level partition", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level_
             && OB_FAIL(set_location_calc_node(stmt,
                                               filter_exprs,
                                               part_level_,
                                               dtc_params,
                                               exec_ctx,
                                               se_subpart_expr_,
                                               se_sub_gen_col_expr_,
                                               is_col_subpart_expr_,
                                               subcalc_node_,
                                               sub_gen_col_node_,
                                               subpart_get_all_,
                                               is_subpart_range_get_,
                                               is_in_range_optimization_enabled))) {
    LOG_WARN("failed to set location calc node for second-level partition", K(ret));
  }

  return ret;
}

int ObTableLocation::get_not_insert_dml_part_sort_expr(const ObDMLStmt &stmt,
                                                       ObIArray<ObRawExpr*> *sort_exprs) const
{
  int ret = OB_SUCCESS;
  if (NULL != sort_exprs) {
    ObRawExpr *part_expr = NULL;
    //一级column的range分区, range columns.里面的column exprs在partition间是有序的.
    if (PARTITION_LEVEL_ONE == part_level_) {
      if (((PARTITION_FUNC_TYPE_RANGE == part_type_
            || PARTITION_FUNC_TYPE_INTERVAL == part_type_) && is_col_part_expr_)
          || (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_)) {
        part_expr = stmt.get_part_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_);
      }
    } else if (PARTITION_LEVEL_TWO == part_level_
               && ((PARTITION_FUNC_TYPE_RANGE == subpart_type_ && is_col_subpart_expr_)
                   || PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_)) {
      //二级分区,对于subpartition是column的range分区或者range columns。
      //同时一级分区是单值情况,那么各级分区中的column exprs就是在获取的各个partition间有序.
      if (!part_get_all_ && NULL != calc_node_) {
        if (calc_node_->is_column_value_node()) {
          part_expr = stmt.get_subpart_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_);
        } else if (calc_node_->is_query_range_node()) {
          if (static_cast<ObPLQueryRangeNode*>(calc_node_)->pre_query_range_.is_precise_get()) {
            part_expr = stmt.get_subpart_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_);
          }
        } else { }
      }
    } else { }

    if (NULL != part_expr) {
      // When building local index, the partition by column is not included in select,
      // so we replace the part expr with actual generated column expr.
      // But this breaks the assumption that in oracle mode the partition expr must not be a column ref expr or row expr.
      // So we need to disable this optimization in ddl scenario.

      bool is_ddl = false;
      if (stmt.get_table_items().count() > 0) {
        const TableItem *insert_table_item = stmt.get_table_item(0);
        is_ddl = insert_table_item->ddl_table_id_ > 0;
      }
      if (is_ddl) {
      } else if (part_expr->is_column_ref_expr()) {
        if (OB_FAIL(sort_exprs->push_back(part_expr))) {
          LOG_WARN("Failed to add sort exprs", K(ret));
        }
      } else if (T_OP_ROW == part_expr->get_expr_type()) {
        int64_t param_num = part_expr->get_param_count();
        ObRawExpr *col_expr = NULL;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < param_num; ++idx) {
          if (OB_ISNULL(col_expr = part_expr->get_param_expr(idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Col expr should not be NULL", K(ret));
          } else if (!col_expr->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param expr should be column expr", K(ret));
          } else if (OB_FAIL(sort_exprs->push_back(col_expr))) {
            LOG_WARN("Failed to add sort exprs", K(ret));
          } else { }//do nothing
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Part expr should be Column or T_OP_ROW of column", K(ret), KPNAME(part_expr));
      }
    }
  }
  return ret;
}

int ObTableLocation::get_location_calc_node(const ObPartitionLevel part_level,
                                            ObIArray<ColumnItem> &partition_columns,
                                            const ObRawExpr *partition_expr,
                                            const ObIArray<ObRawExpr*> &filter_exprs,
                                            ObPartLocCalcNode *&res_node,
                                            bool &get_all,
                                            bool &is_range_get,
                                            const ObDataTypeCastParams &dtc_params,
                                            ObExecContext *exec_ctx,
                                            const bool is_in_range_optimization_enabled)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  get_all = false;
  is_range_get = false;
  bool only_range_node = false;

  if (partition_expr->is_column_ref_expr() || is_virtual_table(loc_meta_.ref_table_id_)) {
    //partition expr is signle column, only query range needed
    //In future column list is also only query range needed.
    only_range_node = true;
  } else if (partition_columns.count() == 1) {
    column_id = partition_columns.at(0).column_id_;
    //当partition_expr为function，且只有一个column的时候
    //用于处理func(col) = X or col = Y的filter计算
  } else { /*do nothing*/ }

  if (OB_FAIL(ret)) {
  } else if (only_range_node) {
    bool always_true = false;
    ObPartLocCalcNode *calc_node = NULL;
    if (OB_FAIL(get_query_range_node(part_level,
                                     partition_columns,
                                     filter_exprs,
                                     always_true,
                                     calc_node,
                                     dtc_params,
                                     exec_ctx,
                                     is_in_range_optimization_enabled))) {
      LOG_WARN("Get query range node error", K(ret));
    } else if (always_true) {
      get_all = true;
    } else {
      res_node = calc_node;
      if (OB_NOT_NULL(calc_node)) {
        is_range_get = static_cast<ObPLQueryRangeNode*>(calc_node)->pre_query_range_.is_precise_get();
      }
    }
  } else {
    ObSEArray<ObRawExpr *, 5> normal_filters;
    bool func_always_true = true;
    ObPartLocCalcNode *func_node = NULL;
    bool is_func_range_get = false;
    bool is_column_range_get = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < filter_exprs.count(); ++idx) {
      bool cnt_func_expr = false;
      bool always_true = false;
      ObPartLocCalcNode *calc_node = NULL;
      if (OB_FAIL(analyze_filter(partition_columns, partition_expr, column_id, filter_exprs.at(idx),
                                 always_true, calc_node, cnt_func_expr, dtc_params, exec_ctx))) {
        LOG_WARN("Failed to analyze filter", K(ret));
      } else if (!cnt_func_expr) {
        if (OB_FAIL(normal_filters.push_back(filter_exprs.at(idx)))) {
          LOG_WARN("Failed to add filter", K(ret));
        }
      } else if (OB_FAIL(add_and_node(calc_node, func_node))) {
        //这里好像用cnt_func_expr来确保calc_node不为NULL。但是真的有这种保证么
        //如果是这种保证这个变量名就不太合适
        LOG_WARN("Failed to add and node", K(ret));
      } else {
        is_func_range_get = true;
        func_always_true &= always_true || NULL == calc_node;
      }
    }

    if (OB_SUCC(ret)) {
      bool column_always_true = true;
      ObPartLocCalcNode *column_node = NULL;
      if (normal_filters.count() > 0) {
        column_always_true = false;
        if (OB_FAIL(get_query_range_node(part_level, partition_columns, filter_exprs, column_always_true,
                                         column_node, dtc_params, exec_ctx, is_in_range_optimization_enabled))) {
          LOG_WARN("Failed to get query range node", K(ret));
        } else if (OB_NOT_NULL(column_node)) {
          is_column_range_get = static_cast<ObPLQueryRangeNode*>(column_node)->pre_query_range_.is_precise_get();
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
      is_range_get = is_func_range_get || is_column_range_get;
    }
  }
  return ret;
}

int ObTableLocation::get_query_range_node(const ObPartitionLevel part_level,
                                          const ColumnIArray &partition_columns,
                                          const ObIArray<ObRawExpr*> &filter_exprs,
                                          bool &always_true,
                                          ObPartLocCalcNode *&calc_node,
                                          const ObDataTypeCastParams &dtc_params,
                                          ObExecContext *exec_ctx,
                                          const bool is_in_range_optimization_enabled)
{
  int ret = OB_SUCCESS;
  bool phy_rowid_for_table_loc = (part_level == part_level_);
  if (filter_exprs.empty()) {
    always_true = true;
  } else if (OB_ISNULL(calc_node = ObPartLocCalcNode::create_part_calc_node(
      allocator_, calc_nodes_, ObPartLocCalcNode::QUERY_RANGE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory failed", K(ret));
  } else {
    ObPLQueryRangeNode *node = static_cast<ObPLQueryRangeNode*>(calc_node);
    if (OB_FAIL(node->pre_query_range_.preliminary_extract_query_range(partition_columns,
                                                                       filter_exprs,
                                                                       dtc_params,
                                                                       exec_ctx,
                                                                       NULL,
                                                                       NULL,
                                                                       phy_rowid_for_table_loc,
                                                                       false,
                                                                       is_in_range_optimization_enabled))) {
      LOG_WARN("Failed to pre extract query range", K(ret));
    } else if (node->pre_query_range_.is_precise_whole_range()) {
      //pre query range is whole range, indicate that there are no partition condition in filters,
      //so you need to get all part ids
      always_true = true;
      calc_node = NULL;
    }
  }
  return ret;
}

int ObTableLocation::analyze_filter(const ObIArray<ColumnItem> &partition_columns,
                                    const ObRawExpr *partition_expr,
                                    uint64_t column_id,
                                    const ObRawExpr *filter,
                                    bool &always_true,
                                    ObPartLocCalcNode *&calc_node,
                                    bool &cnt_func_expr,
                                    const ObDataTypeCastParams &dtc_params,
                                    ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter should not be NULL", K(ret));
  } else if (T_OP_OR == filter->get_expr_type() || T_OP_AND == filter->get_expr_type()) {
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(filter);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < op_expr->get_param_count(); ++idx) {
      ObPartLocCalcNode *cur_node = NULL;
      bool f_always_true = false;
      if (OB_FAIL(analyze_filter(partition_columns, partition_expr, column_id, op_expr->get_param_expr(idx),
                                 f_always_true, cur_node, cnt_func_expr, dtc_params, exec_ctx))) {
        LOG_WARN("Failed to replace sub_expr bool filter", K(ret));
      } else if (T_OP_OR == filter->get_expr_type()) {
        if (f_always_true || NULL == cur_node) {
          always_true = true;
          calc_node = NULL;
        } else if (OB_FAIL(add_or_node(cur_node, calc_node))) {
          LOG_WARN("Failed to add or node", K(ret));
        } else { }
      } else {
        if (f_always_true || NULL == cur_node) {
          //do nothing
        } else if (OB_FAIL(add_and_node(cur_node, calc_node))) {
          LOG_WARN("Failed to add and node", K(ret));
        } else { }
      }
    }
  } else if (T_OP_IN == filter->get_expr_type()) {
    //todo extract_in_op
  } else if (T_OP_EQ == filter->get_expr_type()) {
    const ObRawExpr *l_expr = filter->get_param_expr(0);
    const ObRawExpr *r_expr = filter->get_param_expr(1);
    if (OB_FAIL(extract_eq_op(exec_ctx, l_expr, r_expr, column_id, partition_expr,
                              filter->get_result_type(), cnt_func_expr,
                              always_true, calc_node))) {
      LOG_WARN("Failed to extract equal expr", K(ret));
    }
  } else { }

  return ret;
}

int ObTableLocation::extract_eq_op(ObExecContext *exec_ctx,
                                   const ObRawExpr *l_expr,
                                   const ObRawExpr *r_expr,
                                   const uint64_t column_id,
                                   const ObRawExpr *partition_expr,
                                   const ObExprResType &res_type,
                                   bool &cnt_func_expr,
                                   bool &always_true,
                                   ObPartLocCalcNode *&calc_node)
{
  int ret = OB_SUCCESS;
  always_true = false;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || OB_ISNULL(partition_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr should not be NULL", K(ret), K(l_expr), K(r_expr), K(partition_expr));
  } else if ((l_expr->has_flag(IS_ROWID) && r_expr->is_const_expr()) ||
             (r_expr->has_flag(IS_ROWID) && l_expr->is_const_expr())) {
    //rowid is meaning to use primary key
    always_true = false;
    cnt_func_expr = false;
  } else if ((l_expr->has_flag(CNT_COLUMN) && !(l_expr->has_flag(IS_COLUMN))
              && r_expr->is_const_expr())
             || (r_expr->has_flag(CNT_COLUMN) && !(r_expr->has_flag(IS_COLUMN))
              && l_expr->is_const_expr())) {
    const ObRawExpr *func_expr = l_expr->has_flag(CNT_COLUMN) ? l_expr : r_expr;
    const ObRawExpr *c_expr = l_expr->is_const_expr() ? l_expr : r_expr;
    // const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(c_expr);
    bool equal = false;
    ObExprEqualCheckContext equal_ctx;
    bool true_false = true;
    if (!ObQueryRange::can_be_extract_range(T_OP_EQ, func_expr->get_result_type(),
        res_type.get_calc_meta(), c_expr->get_result_type().get_type(), true_false)) {
      always_true = true_false;
    } else if (OB_FAIL(check_expr_equal(partition_expr, func_expr, equal, equal_ctx))) {
      LOG_WARN("Failed to check equal expr", K(ret));
    } else if (equal) {
      if (NULL == (calc_node = ObPartLocCalcNode::create_part_calc_node(
                  allocator_, calc_nodes_, ObPartLocCalcNode::FUNC_VALUE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to create expr calc node", K(ret));
      } else {
        ObPLFuncValueNode *node = static_cast<ObPLFuncValueNode*>(calc_node);
        cnt_func_expr = true;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < equal_ctx.param_expr_.count(); ++idx) {
          ObExprEqualCheckContext::ParamExprPair &param_pair = equal_ctx.param_expr_.at(idx);
          if (OB_ISNULL(param_pair.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param expr should not be NULL", K(ret));
          } else if (param_pair.param_idx_ < 0) {
            ret = OB_ERR_ILLEGAL_INDEX;
            LOG_WARN("Wrong index of question mark position", K(ret), "param_idx", param_pair.param_idx_);
          } else if (!param_pair.expr_->is_const_or_param_expr()
                     || T_QUESTIONMARK == param_pair.expr_->get_expr_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Param pair expr should be const and const in expr_to_find should not be T_QUESTIONMARK", K(ret));
          } else {
            const ObObj &expect_val = static_cast<const ObConstRawExpr*>(param_pair.expr_)->get_value();
            ObObj dst;
            if (OB_FAIL(deep_copy_obj(allocator_, expect_val, dst))) {
              LOG_WARN("Failed to deep copy obj", K(ret));
            } else if (OB_FAIL(node->param_value_.push_back(
                ObPLFuncValueNode::ParamValuePair(param_pair.param_idx_, dst)))) {
              LOG_WARN("Failed to add param value pair", K(ret));
            } else { }//do nothing
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(extract_value_item_expr(exec_ctx, c_expr, func_expr, node->vie_))) {
          LOG_WARN("failed to extract value item expr", K(ret));
        }
      }
    } else { }//do nothing
  } else if ((l_expr->has_flag(IS_COLUMN) && r_expr->is_const_expr())
             || (r_expr->has_flag(IS_COLUMN) && l_expr->is_const_expr())) {
    const ObRawExpr *col_expr = !l_expr->is_const_expr() ? l_expr : r_expr;
    const ObRawExpr *c_expr = l_expr->is_const_expr() ? l_expr : r_expr;
    const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr*>(col_expr);
    if (column_id == column_expr->get_column_id()) {
      bool true_false = false;
      if (!ObQueryRange::can_be_extract_range(T_OP_EQ, col_expr->get_result_type(),
          res_type.get_calc_meta(), c_expr->get_result_type().get_type(), true_false)) {
        always_true = true_false;
      } else if (NULL == (calc_node = ObPartLocCalcNode::create_part_calc_node(
                  allocator_, calc_nodes_, ObPartLocCalcNode::COLUMN_VALUE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to create expr calc node", K(ret));
      } else {
        ObPLColumnValueNode *node = static_cast<ObPLColumnValueNode*>(calc_node);
        if (OB_FAIL(extract_value_item_expr(exec_ctx, c_expr, col_expr, node->vie_))) {
          LOG_WARN("failed to extract value item expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::extract_value_item_expr(ObExecContext *exec_ctx,
                                             const ObRawExpr *expr,
                                             const ObRawExpr *dst_expr,
                                             ValueItemExpr &vie)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(dst_expr) || OB_ISNULL(exec_ctx) ||
      OB_ISNULL(exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(expr), K(exec_ctx));
  } else if (expr->is_const_or_param_expr()) {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr);
    if (T_QUESTIONMARK == const_expr->get_expr_type()) {
      vie.idx_ = const_expr->get_value().get_unknown();
      vie.type_ = QUESTMARK_TYPE;
    } else {
      if (OB_FAIL(deep_copy_obj(allocator_, const_expr->get_value(), vie.obj_))) {
        LOG_WARN("Failed to deep copy const value", K(ret));
      } else {
        vie.type_ = CONST_OBJ_TYPE;
      }
    }
  } else {
    vie.type_ = CONST_EXPR_TYPE;
    RowDesc row_desc;
    ObTempExpr *temp_expr = NULL;
    if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(expr,
                                                             row_desc,
                                                             exec_ctx->get_allocator(),
                                                             exec_ctx->get_my_session(),
                                                             exec_ctx->get_sql_ctx()->schema_guard_,
                                                             temp_expr))) {
      LOG_WARN("failed to generate expr with row desc", K(ret));
    } else {
      vie.expr_ = temp_expr;
    }
  }
  if (OB_SUCC(ret)) {
    vie.dst_type_ = dst_expr->get_result_type().get_type();
    vie.dst_cs_type_ = dst_expr->get_result_type().get_collation_type();
    if (ob_is_enum_or_set_type(vie.dst_type_)) {
      vie.enum_set_values_cnt_ = dst_expr->get_enum_set_values().count();
      vie.enum_set_values_ = dst_expr->get_enum_set_values().get_data();
    }
  }
  return ret;
}

int ObTableLocation::check_expr_equal(
    const ObRawExpr *partition_expr,
    const ObRawExpr *check_expr,
    bool &equal,
    ObExprEqualCheckContext &equal_ctx)
{
  int ret = OB_SUCCESS;
  equal = partition_expr->same_as(*check_expr, &equal_ctx);
  if (OB_SUCCESS != equal_ctx.err_code_) {
    ret = equal_ctx.err_code_;
    LOG_WARN("Failed to check whether expr equal", K(ret));
  }
  return ret;
}

//我们要构造的是该索引的rowkey顺序，所以这里的rowkey_info必须要跟被计算的行的rowkey顺序对应
//而这里的分区信息是依赖分区规则的分区信息，对于local索引，其本身没有分区规则信息，所依赖的是主表的分区规则
//所以对于全局索引和主表，传递本身的schema，而对于local索引，rowkey传递local index schema的rowkey
//而partition依赖主表，所以real_table_schema传递主表的schema
int ObTableLocation::generate_rowkey_desc(const ObDMLStmt &stmt,
                                          const ObIArray<uint64_t> &column_ids,
                                          uint64_t data_table_id,
                                          ObRawExprFactory &expr_factory,
                                          RowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    uint64_t rowkey_id = column_ids.at(i);
    //raw expr中的table_id都是主表的table_id，但是我们要构造索引(包括主键索引)的rowkey顺序，
    //所以这里应该用主表table_id去查raw_expr
    ObColumnRefRawExpr *rowkey_col = stmt.get_column_expr_by_id(data_table_id, rowkey_id);
    if (OB_ISNULL(rowkey_col)) {
      //rowkey在stmt中没有被解析，构造一个临时的rowkey column expr
      if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, rowkey_col))) {
        LOG_WARN("create mock rowkey column expr failed", K(ret));
      } else if (OB_FAIL(row_desc.add_column(rowkey_col))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    } else if (OB_FAIL(row_desc.add_column(rowkey_col))) {
      LOG_WARN("add rowkey column expr to row desc failed", K(ret));
    }
  }
  return ret;
}

int ObTableLocation::get_partition_column_info(const ObDMLStmt &stmt,
                                               const ObPartitionLevel part_level,
                                               ObIArray<ColumnItem> &partition_columns,
                                               ObIArray<ColumnItem> &gen_cols,
                                               const ObRawExpr *&partition_raw_expr,
                                               bool &is_col_part_expr,
                                               ObTempExpr *&part_temp_expr,
                                               ObTempExpr *&gen_col_temp_expr,
                                               ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  partition_raw_expr = NULL;
  ObRawExpr *gen_col_expr = NULL;
  RowDesc row_desc;
  RowDesc gen_row_desc;
  if (PARTITION_LEVEL_ONE != part_level && PARTITION_LEVEL_TWO != part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part level should be One or Two", K(ret));
  } else if (PARTITION_LEVEL_ONE == part_level &&
             OB_ISNULL(partition_raw_expr = stmt.get_part_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition expr not in stmt", K(ret), K(loc_meta_));
  } else if (PARTITION_LEVEL_TWO == part_level &&
             OB_ISNULL(partition_raw_expr = stmt.get_subpart_expr(loc_meta_.table_loc_id_, loc_meta_.ref_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Partition expr not in stmt", K(ret), K(loc_meta_));
  } else if (PARTITION_LEVEL_ONE == part_level &&
             (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ ||
             PARTITION_FUNC_TYPE_INTERVAL == part_type_) &&
             OB_FAIL(can_get_part_by_range_for_range_columns(partition_raw_expr,
                                                             is_valid_range_columns_part_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level &&
             PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ &&
             OB_FAIL(can_get_part_by_range_for_range_columns(partition_raw_expr,
                                                             is_valid_range_columns_subpart_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (PARTITION_LEVEL_ONE == part_level &&
             OB_FAIL(can_get_part_by_range_for_temporal_column(partition_raw_expr,
                                                               is_valid_temporal_part_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (PARTITION_LEVEL_TWO == part_level &&
             OB_FAIL(can_get_part_by_range_for_temporal_column(partition_raw_expr,
                                                               is_valid_temporal_subpart_range_))) {
    LOG_WARN("failed ot check can get part by range for range columns", K(ret));
  } else if (FALSE_IT(is_col_part_expr = partition_raw_expr->is_column_ref_expr())) {
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else if (OB_FAIL(gen_row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else if (OB_FAIL(add_partition_columns(stmt, partition_raw_expr, partition_columns,
                                           gen_cols, gen_col_expr, row_desc, gen_row_desc))) {
    LOG_WARN("Failed to add partitoin column", K(ret));
  } else {
    if (gen_cols.count() > 0) {
      if (partition_columns.count() > 1) {
        gen_cols.reset();
      } else if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_sql_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KP(exec_ctx), K(ret));
      } else {
        OZ (ObStaticEngineExprCG::gen_expr_with_row_desc(gen_col_expr,
                                                         gen_row_desc,
                                                         exec_ctx->get_allocator(),
                                                         exec_ctx->get_my_session(),
                                                         exec_ctx->get_sql_ctx()->schema_guard_,
                                                         gen_col_temp_expr));
      }
    }
    //generate partition_expression with partition columns row_desc
    if (OB_SUCC(ret)) {
      ObTempExpr *gen_col_runtime_expr = NULL;
      OZ (ObStaticEngineExprCG::gen_expr_with_row_desc(partition_raw_expr,
                                               row_desc,
                                               exec_ctx->get_allocator(),
                                               exec_ctx->get_my_session(),
                                               exec_ctx->get_sql_ctx()->schema_guard_,
                                               part_temp_expr));
    }

    //clear IS_COLUMNLIZED flag
    if (OB_SUCC(ret)) {
      if (OB_FAIL(clear_columnlized_in_row_desc(row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      } else if (OB_FAIL(clear_columnlized_in_row_desc(gen_row_desc))) {
        LOG_WARN("Failed to clear columnlized in row desc", K(ret));
      } else { }//do nothing
    }
  }

  return ret;
}

int ObTableLocation::add_partition_columns(const ObDMLStmt &stmt,
                                           const ObRawExpr *part_expr,
                                           ObIArray<ColumnItem> &partition_columns,
                                           ObIArray<ColumnItem> &gen_cols,
                                           ObRawExpr *&gen_col_expr,
                                           RowDesc &row_desc,
                                           RowDesc &gen_row_desc,
                                           const bool only_gen_cols)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> cur_vars;
  if (OB_ISNULL(part_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part expr is NULL", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, cur_vars))) {
    LOG_WARN("get column exprs error", K(ret));
  } else {
    ObRawExpr *var = NULL;
    uint64_t column_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    ObColumnRefRawExpr *col_expr = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cur_vars.count(); ++idx) {
      if (OB_ISNULL(var = cur_vars.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Var should not be NULL in column exprs", K(ret));
      } else if (!var->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Var should be column", K(ret));
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(var))) {
      } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = col_expr->get_column_id()))
                  || OB_UNLIKELY(OB_INVALID_ID == (table_id = col_expr->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column id should not be OB_INVALID_ID", K(ret));
      } else if (only_gen_cols) {//only deal dependented columns for generated partition column
        if (OB_FAIL(add_partition_column(stmt, table_id, column_id, gen_cols, gen_row_desc))) {
          LOG_WARN("Failed to add partition column", K(ret));
        }
      } else {
        if (col_expr->is_generated_column()) {
          if (OB_ISNULL(col_expr->get_dependant_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Generated column's dependent expr is NULL", K(ret));
          } else if (cur_vars.count() > 1) {
            //do nothing.Only deal case with one partition column.
          } else {
            gen_col_expr = col_expr->get_dependant_expr();
            bool can_replace = false;
            if (OB_FAIL(check_can_replace(gen_col_expr, col_expr, can_replace))) {
              LOG_WARN("failed to check can replace", K(ret));
            } else if (!can_replace) {
              //do nothing
            } else if (OB_FAIL(add_partition_columns(stmt, col_expr->get_dependant_expr(),
                                              partition_columns, gen_cols, gen_col_expr,
                                              row_desc, gen_row_desc, true))) {
              LOG_WARN("Failed to add gen columns", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_partition_column(stmt, table_id, column_id,
                                          partition_columns, row_desc))) {
            LOG_WARN("Failed to add partition column", K(ret));
          }
        }
      }//end of else
    }//end of for
  }
  return ret;
}

int ObTableLocation::check_can_replace(ObRawExpr *gen_col_expr,
                                       ObRawExpr *col_expr,
                                       bool &can_replace)
{
  int ret = OB_SUCCESS;
  can_replace = false;
  ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
  ObCollationType res_cs_type = CS_TYPE_INVALID;
  ObRawExpr* expr = gen_col_expr;
  if (OB_ISNULL(gen_col_expr) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!ob_is_string_or_lob_type(gen_col_expr->get_result_type().get_type()) ||
             !ob_is_string_or_lob_type(col_expr->get_result_type().get_type())) {
    can_replace = true;
  } else if (gen_col_expr->get_expr_type() != T_FUN_COLUMN_CONV) {
    can_replace = true;
  } else if (((gen_col_expr->get_param_count() != 5) &&
             (gen_col_expr->get_param_count() != 6)) ||
             OB_ISNULL(expr = gen_col_expr->get_param_expr(4))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error",K(gen_col_expr->get_param_count()), K(expr), K(ret));
  } else if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
    while (expr->has_flag(IS_INNER_ADDED_EXPR) &&
           (expr->get_param_count() > 0) &&
           (NULL != expr->get_param_expr(0))) {
        expr = expr->get_param_expr(0);
    }
  }
  if (OB_FAIL(ret) || can_replace) {
    //do nothing
  } else if (expr->get_result_type().get_collation_type() != col_expr->get_result_type().get_collation_type()) {
    can_replace = false;
  } else {
    can_replace = true;
  }
  return ret;
}
int ObTableLocation::add_partition_column(const ObDMLStmt &stmt,
                                          const uint64_t table_id,
                                          const uint64_t column_id,
                                          ObIArray<ColumnItem> &partition_columns,
                                          RowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  //去除partition columns中重复的columns
  bool is_different = true;
  for (int64_t idx = 0; OB_SUCC(ret) && is_different && idx < partition_columns.count(); ++idx) {
    if (partition_columns.at(idx).column_id_ == column_id) {
      is_different = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_different) {
      ColumnItem *column_item = NULL;
      if (OB_ISNULL(column_item = stmt.get_column_item_by_id(table_id, column_id))) {
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
      } else { }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_calc_node(ObExecContext &exec_ctx,
                                                     ObDASTabletMapper &tablet_mapper,
                                                     const ParamStore &params,
                                                     const ObPartLocCalcNode *calc_node,
                                                     const ObPartLocCalcNode *gen_col_node,
                                                     const ObTempExpr *se_gen_col_expr,
                                                     const bool part_col_get_all,
                                                     ObIArray<ObTabletID> &tablet_ids,
                                                     ObIArray<ObObjectID> &partition_ids,
                                                     const ObDataTypeCastParams &dtc_params,
                                                     const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 5> gen_tablet_ids;
  ObSEArray<ObObjectID, 5> gen_part_ids;
  bool part_col_all_part = false;
  bool gen_col_all_part = false;
  //get partition ids with information of partition columns
  if (!part_col_get_all) {
    //partition condition is not empty, so you need to calc partition id by partition key
    if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params, calc_node,
                                                tablet_ids, partition_ids, part_col_all_part,
                                                dtc_params, part_ids))) {
      LOG_WARN("Failed to calc partitoin ids by calc node", K(ret));
    }
  } else {
    part_col_all_part = true;
  }

  if (OB_SUCC(ret)) {
    if (NULL != gen_col_node && NULL != se_gen_col_expr) {
      //partition key is generated column and dependent column condition is not empty
      //so you need to calc partition id by dependent column condition
      if (!gen_col_node->is_query_range_node()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column dependented by generated column calc node should be query range node", K(ret));
      } else if (OB_FAIL(calc_query_range_partition_ids(exec_ctx, tablet_mapper, params,
                                                        static_cast<const ObPLQueryRangeNode*>(gen_col_node),
                                                        gen_tablet_ids, gen_part_ids, gen_col_all_part, dtc_params,
                                                        part_ids, se_gen_col_expr))) {
        LOG_WARN("Failed to calcl partition ids by gen col node", K(ret));
      }
    } else {
      gen_col_all_part = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (!part_col_all_part && !gen_col_all_part) {
      //partition key condition is not empty and generated column dependent condition is not empty
      //so you need to intersect partition ids extracted by these two conditions
      OZ (intersect_partition_ids(gen_tablet_ids, tablet_ids));
      OZ (intersect_partition_ids(gen_part_ids, partition_ids));
    } else if (!part_col_all_part && gen_col_all_part) {
      //generated column condition is empty, but partition key condition is not empty,
      //use the partition ids extracted by partition key condition directly, do nothing
    } else if (part_col_all_part && !gen_col_all_part) {
      //generated column condition is not empty, but partition key condition is empty,
      //use the partiiton ids extracted by generated column condition, so append to partition_ids
      OZ (append(tablet_ids, gen_tablet_ids));
      OZ (append(partition_ids, gen_part_ids));
    } else {
      //part_col_all_part=true && gen_col_all_part=true
      //has no any partition condition, get all partition ids in schema
      if (OB_FAIL(get_all_part_ids(tablet_mapper,tablet_ids,
                                   partition_ids, part_ids))) {
        LOG_WARN("Get all part ids error", K(ret));
      }
    }
  }
  return ret;
}

template <typename T>
int ObTableLocation::intersect_partition_ids(
    const ObIArray<T> &to_inter_ids,
    ObIArray<T> &partition_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<T, 128> intersect_tmp;
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

int ObTableLocation::calc_partition_ids_by_calc_node(ObExecContext &exec_ctx,
                                                     ObDASTabletMapper &tablet_mapper,
                                                     const ParamStore &params,
                                                     const ObPartLocCalcNode *calc_node,
                                                     ObIArray<ObTabletID> &tablet_ids,
                                                     ObIArray<ObObjectID> &partition_ids,
                                                     bool &all_part,
                                                     const ObDataTypeCastParams &dtc_params,
                                                     const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  all_part = false;
  if (OB_UNLIKELY(NULL == calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (ObPartLocCalcNode::CALC_AND == calc_node->get_node_type()) {
    if (OB_FAIL(calc_and_partition_ids(exec_ctx, tablet_mapper, params,
                                       static_cast<const ObPLAndNode*>(calc_node),
                                       tablet_ids, partition_ids, all_part,
                                       dtc_params, part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::CALC_OR == calc_node->get_node_type()) {
    if (OB_FAIL(calc_or_partition_ids(exec_ctx, tablet_mapper, params,
                                      static_cast<const ObPLOrNode *>(calc_node),
                                      tablet_ids, partition_ids, all_part,
                                      dtc_params, part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::QUERY_RANGE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_query_range_partition_ids(exec_ctx, tablet_mapper, params,
                                               static_cast<const ObPLQueryRangeNode*>(calc_node),
                                               tablet_ids, partition_ids, all_part,
                                               dtc_params, part_ids, NULL))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::FUNC_VALUE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_func_value_partition_ids(exec_ctx, tablet_mapper, params,
                                              static_cast<const ObPLFuncValueNode*>(calc_node),
                                              tablet_ids, partition_ids, all_part,
                                              dtc_params, part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else if (ObPartLocCalcNode::COLUMN_VALUE == calc_node->get_node_type()) {
    if (OB_FAIL(calc_column_value_partition_ids(exec_ctx, tablet_mapper, params,
                                                static_cast<const ObPLColumnValueNode*>(calc_node),
                                                tablet_ids, partition_ids,
                                                dtc_params, part_ids))) {
      LOG_WARN("Failed to calc and partition ids", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unknown calc node type", K(ret));
  }
  return ret;
}

int ObTableLocation::calc_and_partition_ids(
    ObExecContext &exec_ctx,
    ObDASTabletMapper &tablet_mapper,
    const ParamStore &params,
    const ObPLAndNode *calc_node,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &partition_ids,
    bool &all_part,
    const ObDataTypeCastParams &dtc_params,
    const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (OB_ISNULL(calc_node->left_node_)
             || OB_ISNULL(calc_node->right_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("And node shoul have left and right node", K(ret));
  } else {
    ObSEArray<ObObjectID, 3> left_part_ids;
    ObSEArray<ObTabletID, 3> left_tablet_ids;
    bool left_all = false;
    ObSEArray<ObObjectID, 3> right_part_ids;
    ObSEArray<ObTabletID, 3> right_tablet_ids;
    bool right_all = false;
    if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params, calc_node->left_node_,
                                                left_tablet_ids, left_part_ids,left_all,
                                                dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params, calc_node->right_node_,
                                                       right_tablet_ids, right_part_ids, right_all,
                                                       dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (left_all && right_all) {
      all_part = true;
    } else if (left_all) {
      if (OB_FAIL(partition_ids.assign(right_part_ids))) {
        LOG_WARN("Failed to assign part ids", K(ret));
      } else if (OB_FAIL(tablet_ids.assign(right_tablet_ids))) {
        LOG_WARN("Failed to assign tablet ids", K(ret));
      }
    } else if (right_all) {
      if (OB_FAIL(partition_ids.assign(left_part_ids))) {
        LOG_WARN("Failed to assign part ids", K(ret));
      } else if (OB_FAIL(tablet_ids.assign(left_tablet_ids))) {
        LOG_WARN("Failed to assign tablet ids", K(ret));
      }
    } else {
      for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < left_part_ids.count(); ++l_idx) {
        for (int64_t r_idx = 0; OB_SUCC(ret) && r_idx < right_part_ids.count(); ++r_idx) {
          if (right_part_ids.at(r_idx) == left_part_ids.at(l_idx)) {
            OZ(partition_ids.push_back(left_part_ids.at(l_idx)));
            OZ(tablet_ids.push_back(left_tablet_ids.at(l_idx)));
          }
        }//end of r_idx
      }//end of l_idx
    }
  }

  return ret;
}

int ObTableLocation::calc_or_partition_ids(
    ObExecContext &exec_ctx,
    ObDASTabletMapper &tablet_mapper,
    const ParamStore &params,
    const ObPLOrNode *calc_node,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &partition_ids,
    bool &all_part,
    const ObDataTypeCastParams &dtc_params,
    const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else if (OB_ISNULL(calc_node->left_node_)
             || OB_ISNULL(calc_node->right_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("And node shoul have left and right node", K(ret));
  } else {
    ObSEArray<ObObjectID, 3> left_part_ids;
    ObSEArray<ObTabletID, 3> left_tablet_ids;
    bool left_all = false;
    ObSEArray<ObObjectID, 3> right_part_ids;
    ObSEArray<ObTabletID, 3> right_tablet_ids;
    bool right_all = false;
    if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params, calc_node->left_node_,
                                                left_tablet_ids, left_part_ids, left_all,
                                                dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (OB_FAIL(calc_partition_ids_by_calc_node(exec_ctx, tablet_mapper, params, calc_node->right_node_,
                                                       right_tablet_ids, right_part_ids, right_all,
                                                       dtc_params, part_ids))) {
      LOG_WARN("Calc node error", K(ret));
    } else if (left_all || right_all) {
      all_part = true;
    } else {
      OZ(append_array_no_dup(partition_ids, left_part_ids));
      OZ(append_array_no_dup(partition_ids, right_part_ids));
      OZ(append_array_no_dup(tablet_ids, left_tablet_ids));
      OZ(append_array_no_dup(tablet_ids, right_tablet_ids));
    }
  }

  return ret;
}

int ObTableLocation::calc_query_range_partition_ids(ObExecContext &exec_ctx,
                                                    ObDASTabletMapper &tablet_mapper,
                                                    const ParamStore &params,
                                                    const ObPLQueryRangeNode *calc_node,
                                                    ObIArray<ObTabletID> &tablet_ids,
                                                    ObIArray<ObObjectID> &partition_ids,
                                                    bool &all_part,
                                                    const ObDataTypeCastParams &dtc_params,
                                                    const ObIArray<ObObjectID> *part_ids,
                                                    const ObTempExpr *se_gen_col_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else {
    all_part = false;
    ObQueryRangeArray query_ranges;
    ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
    allocator.set_label("CalcQRPartIds");
    bool is_all_single_value_ranges = true;
    if (OB_FAIL(calc_node->pre_query_range_.get_tablet_ranges(allocator, exec_ctx, query_ranges,
                                                              is_all_single_value_ranges, dtc_params))) {
      LOG_WARN("get tablet ranges failed", K(ret));
    } else if (OB_UNLIKELY(query_ranges.count() == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Query ranges' count should not be 0",
               "query range count", query_ranges.count(), K(ret));
    } else {
      bool is_empty = true;
      if (OB_FAIL(is_all_ranges_empty(query_ranges, is_empty))) {
        LOG_WARN("fail to check all ranges", K(query_ranges));
      } else if (!is_empty) {
        if (OB_FAIL(calc_partition_ids_by_ranges(exec_ctx,
                                                 tablet_mapper,
                                                 query_ranges,
                                                 is_all_single_value_ranges,
                                                 tablet_ids,
                                                 partition_ids,
                                                 all_part,
                                                 part_ids,
                                                 se_gen_col_expr))) {
          LOG_WARN("Failed to get partition ids", K(ret), K(loc_meta_.table_loc_id_));
        }
      } else { } //do nothing. partition ids will be empty
    }
  }
  return ret;
}

int ObTableLocation::calc_func_value_partition_ids(
    ObExecContext &exec_ctx,
    ObDASTabletMapper &tablet_mapper,
    const ParamStore &params,
    const ObPLFuncValueNode *calc_node,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &partition_ids,
    bool &all_part,
    const ObDataTypeCastParams &dtc_params,
    const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else {
    bool value_satisfy = true;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < calc_node->param_value_.count(); ++idx) {
      const ObPLFuncValueNode::ParamValuePair &pair = calc_node->param_value_.at(idx);
      if (pair.param_idx_ < 0 || pair.param_idx_ >= params.count()) {
        ret = OB_INVALID_INDEX;
        LOG_WARN("Param idx error", K(ret), "param idx", pair.param_idx_, "count", params.count());
      } else if (!params.at(pair.param_idx_).is_equal(pair.obj_value_, CS_TYPE_BINARY)) {
        value_satisfy = false;
        break;
      } else { } //do nothing
    }
    if (OB_FAIL(ret)) {
    } else if (!value_satisfy) {
      all_part = true;
    } else {
      ObObj tmp;
      ObNewRow tmp_row;
      ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
      allocator.set_label("CalcFVPartIds");
      ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, calc_node->vie_.dst_cs_type_);
      if (OB_FAIL(se_calc_value_item(cast_ctx, exec_ctx, params, calc_node->vie_, tmp_row, tmp))) {
        LOG_WARN("failed to calc value item", K(ret));
      } else if (OB_FAIL(calc_partition_id_by_func_value(tablet_mapper,
                                                         tmp,
                                                         true,
                                                         tablet_ids,
                                                         partition_ids,
                                                         part_ids))) {
        LOG_WARN("Failed to calc partition id by func value", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_column_value_partition_ids(
    ObExecContext &exec_ctx,
    ObDASTabletMapper &tablet_mapper,
    const ParamStore &params,
    const ObPLColumnValueNode *calc_node,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &partition_ids,
    const ObDataTypeCastParams &dtc_params,
    const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Calc node should not be NULL", K(ret));
  } else {
    ObObj tmp;
    ObNewRow tmp_row;
    ObArenaAllocator allocator(CURRENT_CONTEXT->get_malloc_allocator());
    allocator.set_label("CalcCVPartIds");
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, calc_node->vie_.dst_cs_type_);
    if (OB_FAIL(se_calc_value_item(cast_ctx, exec_ctx, params, calc_node->vie_, tmp_row, tmp))) {
      LOG_WARN("failed to calc value item", K(ret));
    } else {
      ObNewRow result_row;
      // allocate obj first
      void *ptr = NULL;
      if (NULL == (ptr = allocator.alloc(sizeof(ObObj)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate obj", K(ret));
      } else {
        result_row.cells_ = new (ptr) ObObj();
        result_row.count_ = 1;
        result_row.cells_[0] = tmp;
        if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                              tablet_mapper,
                                              result_row,
                                              tablet_ids,
                                              partition_ids,
                                              part_ids))) {
          LOG_WARN("Calc partition id by row error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_ranges(ObExecContext &exec_ctx,
                                                  ObDASTabletMapper &tablet_mapper,
                                                  const ObIArray<ObNewRange*> &ranges,
                                                  const bool is_all_single_value_ranges,
                                                  ObIArray<ObTabletID> &tablet_ids,
                                                  ObIArray<ObObjectID> &partition_ids,
                                                  bool &all_part,
                                                  const ObIArray<ObObjectID> *part_ids,
                                                  const ObTempExpr *se_gen_col_expr) const
{
  int ret = OB_SUCCESS;
  bool get_part_by_range = false;
  bool need_calc_new_range = false;
  bool need_try_split_range = false;
  all_part = false;
  ObSEArray<ObNewRange *, 1> new_ranges;
  ObSEArray<ObNewRange *, 8> splited_ranges;
  ObArenaAllocator allocator_for_range(CURRENT_CONTEXT->get_malloc_allocator());
  allocator_for_range.set_label("CalcByRanges");
  if (NULL != se_gen_col_expr) {
    get_part_by_range = false;
    need_calc_new_range = false;
    all_part = !is_all_single_value_ranges;
  } else if (NULL == part_ids) {//PARTITION_LEVEL_ONE
    if (!is_range_part(part_type_) ||
        is_include_physical_rowid_range(ranges) ||
        ((PARTITION_FUNC_TYPE_RANGE == part_type_ || PARTITION_FUNC_TYPE_INTERVAL == part_type_) && !is_col_part_expr_) ||
        (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_ && !is_valid_range_columns_part_range_)) {
      if (!is_all_single_value_ranges) {
        if (is_include_physical_rowid_range(ranges)) {
          all_part = true;
        } else if (is_valid_temporal_part_range_) {
          need_calc_new_range = true;
        } else {
          need_try_split_range = true;
        }
      }
    } else {
      get_part_by_range = true;
    }
  } else {//PARTITION_LEVEL_TWO
    if (!is_range_part(subpart_type_) ||
        is_include_physical_rowid_range(ranges) ||
        (PARTITION_FUNC_TYPE_RANGE == subpart_type_ && !is_col_subpart_expr_) ||
        (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ && !is_valid_range_columns_subpart_range_)) {
      if (!is_all_single_value_ranges) {
        if (is_include_physical_rowid_range(ranges)) {
          all_part = true;
        } else if (is_valid_temporal_subpart_range_) {
          need_calc_new_range = true;
        } else {
          need_try_split_range = true;
        }
      }
    } else {
      get_part_by_range = true;
    }
  }
  // if the part_key/subpart_key is in the form like year(datetime), the non-single-value range
  // may be used to prune partition after calc by part_expr
  if (OB_SUCC(ret) && need_calc_new_range) {
    bool is_all_single_value_new_ranges = false;
    if (OB_FAIL(calc_range_by_part_expr(exec_ctx, ranges, part_ids, allocator_for_range,
                                         new_ranges, is_all_single_value_new_ranges))) {
      LOG_WARN("fail to calc range by part expr", K(ret));
    } else if ((NULL == part_ids && PARTITION_FUNC_TYPE_RANGE == part_type_) ||
               (NULL != part_ids && PARTITION_FUNC_TYPE_RANGE == subpart_type_)) {
      get_part_by_range = true;
    } else if (!is_all_single_value_new_ranges) {
      all_part = true;
    }
  }

  if (OB_SUCC(ret) && need_try_split_range) {
    if (OB_FAIL(try_split_integer_range(ranges, allocator_for_range, splited_ranges, all_part))) {
      LOG_WARN("fail to try split integer range", K(ret));
    }
  }

  if (OB_SUCC(ret) && !all_part) {
    if (get_part_by_range) {
      if (need_calc_new_range) {
        OZ (get_part_ids_by_ranges(tablet_mapper, new_ranges,
                                   tablet_ids,  partition_ids, part_ids));
      } else if (OB_FAIL(get_part_ids_by_ranges(tablet_mapper, ranges,
                                                tablet_ids, partition_ids, part_ids))) {
        LOG_WARN("Failed to get part ids by ranges", K(ret));
      }
    } else {
      const ObIArray<ObNewRange*> &single_ranges = need_try_split_range ? splited_ranges : ranges;
      for (int64_t i = 0; OB_SUCC(ret) && !all_part && i < single_ranges.count(); ++i) {
        ObNewRange *range = single_ranges.at(i);
        ObNewRow input_row;
        if (OB_ISNULL(range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid range", K(ret));
        } else if (range->is_physical_rowid_range_) {
          if (OB_FAIL(get_tablet_and_object_id_with_phy_rowid(*range,
                                                              tablet_mapper,
                                                              tablet_ids,
                                                              partition_ids))) {
            LOG_WARN("failed to get tablet and object id with phy rowid", K(ret));
          } else {/*do nothing*/}
        } else {
          input_row.cells_ = const_cast<ObObj*>(range->start_key_.get_obj_ptr());
          input_row.count_ = (range->start_key_.get_obj_cnt());
          if (NULL != se_gen_col_expr) {
            ObObj gen_col_val;
            if (OB_FAIL(se_gen_col_expr->eval(exec_ctx, input_row, gen_col_val))) {
              LOG_WARN("Failed to get column exprs", K(ret));
            } else {
              ObNewRow result_row;
              result_row.cells_ = &gen_col_val;
              result_row.count_ = 1;
              if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, result_row,
                                                   tablet_ids, partition_ids, part_ids))) {
                LOG_WARN("Failed to calc partition id by row", K(ret));
              }
            }
          } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, input_row,
                                                      tablet_ids, partition_ids, part_ids))) {
            LOG_WARN("Calc partition id by row error", K(ret));
          } else { }//do nothing
        }
      }
    }
  }
  return ret;
}

bool ObTableLocation::is_include_physical_rowid_range(const ObIArray<ObNewRange*> &ranges) const
{
  bool is_included = false;
  for (int64_t i = 0; !is_included && i < ranges.count(); ++i) {
    is_included = ranges.at(i) != NULL && ranges.at(i)->is_physical_rowid_range_;
  }
  return is_included;
}

int ObTableLocation::get_part_ids_by_ranges(ObDASTabletMapper &tablet_mapper,
                                            const ObIArray<ObNewRange*> &ranges,
                                            ObIArray<ObTabletID> &tablet_ids,
                                            ObIArray<ObObjectID> &partition_ids,
                                            const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (NULL == part_ids) {
    if (OB_FAIL((tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_ONE, OB_INVALID_ID,
                                   ranges, tablet_ids, partition_ids)))) {
      LOG_WARN("fail to get tablet ids", K(ranges), K(ret));
    }
  } else {
    for (int64_t p_idx = 0; OB_SUCC(ret) && p_idx < part_ids->count(); ++p_idx) {
      ObObjectID part_id = part_ids->at(p_idx);
      if (OB_FAIL((tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_TWO, part_id,
                                     ranges, tablet_ids, partition_ids)))) {
        LOG_WARN("fail to get tablet ids", K(ranges), K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_id_by_func_value(
   ObDASTabletMapper &tablet_mapper,
   const ObObj &func_value,
   const bool calc_oracle_hash,
   ObIArray<ObTabletID> &tablet_ids,
   ObIArray<ObObjectID> &partition_ids,
   const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  ObObj result = func_value;
  if (OB_SUCC(ret) && !is_inner_table(loc_meta_.ref_table_id_)) {
    bool hash_type = false;
    if (NULL == part_ids) {
      hash_type = (PARTITION_FUNC_TYPE_HASH == part_type_);
    } else {
      hash_type = (PARTITION_FUNC_TYPE_HASH == subpart_type_);
    }
    if (hash_type) {
      //内部sql运行时始终作为mysql模式, 导致oracle临时表的借助inner sql执行的判断出问题,
      //所以在这里根据table schema直接调用不同的hash计算函数
      if (lib::is_oracle_mode()) {
        if (calc_oracle_hash && OB_FAIL(ObExprFuncPartHash::calc_value_for_oracle(&func_value, 1, result))) {
          LOG_WARN("Failed to calc hash value oracle mode", K(ret));
        }
      } else {
        if (OB_FAIL(ObExprFuncPartHash::calc_value_for_mysql(func_value, result,
                    func_value.get_type()))) {
          LOG_WARN("Failed to calc hash value mysql mode", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    if (NULL == part_ids) {
      OZ(tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_ONE, OB_INVALID_ID,
                        result, tablet_ids, partition_ids));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
        OZ(tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_TWO, part_ids->at(idx),
                          result, tablet_ids, partition_ids));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_rowkey(ObExecContext &exec_ctx,
                                                  ObDASTabletMapper &tablet_mapper,
                                                  const ObIArray<ObRowkey> &rowkeys,
                                                  ObIArray<ObTabletID> &tablet_ids,
                                                  ObIArray<ObObjectID> &part_ids) const
{
  int ret = OB_SUCCESS;
  ObNewRow cur_row;
  ObSEArray<ObObjectID, 1> tmp_part_ids;
  ObSEArray<ObTabletID, 1> tmp_tablet_ids;
  ObNewRow *part_row = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
    cur_row.cells_ = const_cast<ObObj*>(rowkeys.at(i).get_obj_ptr());
    cur_row.count_ = rowkeys.at(i).get_obj_cnt();
    if (OB_FAIL(part_projector_.calc_part_row(exec_ctx, cur_row, part_row))
        || OB_ISNULL(part_row)) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("calc part row failed", K(ret));
    } else if (PARTITION_LEVEL_ONE == part_level_) {
      if (OB_FAIL(calc_partition_id_by_row(exec_ctx, tablet_mapper, *part_row, tablet_ids, part_ids))) {
        LOG_WARN("calc partition id by row failed", K(ret));
      }
    } else {
      tmp_part_ids.reuse();
      if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                           tablet_mapper,
                                           *part_row,
                                           tmp_tablet_ids,
                                           tmp_part_ids))) {
        LOG_WARN("calc partition id by row failed", K(ret));
      } else if (OB_FAIL(calc_partition_id_by_row(exec_ctx,
                                                  tablet_mapper,
                                                  *part_row,
                                                  tablet_ids,
                                                  part_ids,
                                                  &tmp_part_ids))) {
        LOG_WARN("calc sub partition id by row failed", K(ret));
      }
    }
  }
  return ret;
}

/*
case 1: interval_ym
   diff1 = months_between(const_val - transition_val)
   diff2 = extract(month, interval_val);
case 2: interval_ds
   diff1 = extract(second from (const_val - transition_val))
   diff2 = extract(second from interval);
case 3: other
   diff1 = const_val - transition_val;
   diff2 = interval_val;

   n = trunc(diff1 / diff2) + 1;
   result = transition + n * interval */
int calc_high_bound_obj_new_engine(
  ObIAllocator &allocator,
  ObSQLSessionInfo *session,
  ObObj &const_val,
  uint64_t exist_parts,
  const ObObj &transition_val,
  const ObObj &interval_val,
  ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObNewRow tmp_row;
  ObRawExpr *result_expr = NULL;
  ObRawExpr *n_part_expr = NULL;
  ParamStore dummy_params;
  ObRawExprFactory expr_factory(allocator);
  ObConstRawExpr *max_parts = NULL;
  ObOpRawExpr *less_than_expr = NULL;
  ObObj cmp_res;
  ObObj max_interval;

  CK (OB_NOT_NULL(session));

  OZ (ObRawExprUtils::build_high_bound_raw_expr(expr_factory,
                                                session,
                                                const_val,
                                                transition_val,
                                                interval_val,
                                                result_expr,
                                                n_part_expr));
  CK (OB_NOT_NULL(result_expr));
  OZ (ObSQLUtils::calc_simple_expr_without_row(session,
                                   result_expr, out_val, &dummy_params, allocator));

  OX (max_interval.set_uint64(OB_MAX_INTERVAL_PARTITIONS - exist_parts));
  OZ (ObRawExprUtils::build_const_obj_expr(expr_factory, max_interval, max_parts));
  OZ (ObRawExprUtils::build_less_than_expr(expr_factory, n_part_expr, max_parts, less_than_expr));
  OZ (less_than_expr->formalize(session));
  OZ (ObSQLUtils::calc_simple_expr_without_row(session,
                                   less_than_expr, cmp_res, &dummy_params, allocator));
  if (OB_SUCC(ret) && !cmp_res.get_bool()) {
    ret = OB_ERR_PARTITIONING_KEY_MAPS_TO_A_PARTITION_OUTSIDE_MAXIMUM_PERMITTED_NUMBER_OF_PARTITIONS;
    LOG_WARN("exceed max partition", K(ret), K(const_val), K(exist_parts));
  }
  return ret;
}

// int calc_high_bound_obj(
//   ObExecContext &exec_ctx,
//   ObObj &const_val,
//   const ObObj &transition_val,
//   const ObObj &interval_val,
//   ObObj &out_val)
// {
//   int ret = OB_SUCCESS;
//   ParamStore dummy_params;
//   const stmt::StmtType stmt_type = stmt::T_NONE;
//   ObRawExpr *result_expr = NULL;
//   ObRawExprFactory expr_factory(exec_ctx.get_allocator());

//   OZ (ObRawExprUtils::build_high_bound_raw_expr(expr_factory,
//                                                 exec_ctx.get_my_session(),
//                                                 const_val,
//                                                 transition_val,
//                                                 interval_val,
//                                                 result_expr));

//   CK (OB_NOT_NULL(result_expr));
//   OZ (ObSQLUtils::calc_simple_expr_without_row(stmt_type, exec_ctx.get_my_session(),
//                                    result_expr, out_val, &dummy_params, exec_ctx.get_allocator()));

//   return ret;
// }

/* sync task, need response */
int ObTableLocation::send_add_interval_partition_rpc(
    ObExecContext &exec_ctx,
    share::schema::ObSchemaGetterGuard *schema_guard,
    ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  ObSqlString sql;
  ObMySQLProxy *sql_proxy = nullptr;
  int64_t affected_rows = 0;
  ObTimeZoneInfo tz_info;
  ObObj result_obj;
  uint64_t range_part_num = 0;
  if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(exec_ctx.get_my_session()));
  }

  if (OB_SUCC(ret) && row.get_cell(0).is_null()) {
    ret = OB_ERR_PARTITIONING_KEY_MAPS_TO_A_PARTITION_OUTSIDE_MAXIMUM_PERMITTED_NUMBER_OF_PARTITIONS;
    LOG_USER_WARN(OB_ERR_PARTITIONING_KEY_MAPS_TO_A_PARTITION_OUTSIDE_MAXIMUM_PERMITTED_NUMBER_OF_PARTITIONS);
  }

  CK (OB_NOT_NULL(schema_guard));
  OZ (schema_guard->get_table_schema(exec_ctx.get_my_session()->get_effective_tenant_id(),
                                     loc_meta_.table_loc_id_, table_schema));
  CK (OB_NOT_NULL(table_schema));
  OZ (schema_guard->get_database_schema(table_schema->get_tenant_id(), table_schema->get_database_id(), db_schema));
  CK (OB_NOT_NULL(db_schema));

  CK (1 <= row.get_count());
  CK (table_schema->get_transition_point().is_valid());
  CK (table_schema->get_interval_range().is_valid());
  CK (1 == table_schema->get_transition_point().get_obj_cnt());
  CK (1 == table_schema->get_interval_range().get_obj_cnt());
  OZ (table_schema->get_interval_parted_range_part_num(range_part_num));

  OZ (calc_high_bound_obj_new_engine(exec_ctx.get_allocator(),
                          exec_ctx.get_my_session(),
                          row.get_cell(0),
                          range_part_num,
                          table_schema->get_transition_point().get_obj_ptr()[0],
                          table_schema->get_interval_range().get_obj_ptr()[0],
                          result_obj));

  /* alter table table_name add partition ppp values less than (); */
  if (OB_SUCC(ret)) {
    ObRowkey high_bound_rowkey;
    high_bound_rowkey.assign(&result_obj, 1);


    tz_info.set_offset(0);
    OZ (OTTZ_MGR.get_tenant_tz(table_schema->get_tenant_id(), tz_info.get_tz_map_wrap()));

    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], high_bound_buf) {
      MEMSET(high_bound_buf, 0, OB_MAX_DEFAULT_VALUE_LENGTH);
      int64_t high_bound_buf_pos = 0;

      bool is_oracle_mode = false;
      OZ (ObPartitionUtils::convert_rowkey_to_sql_literal(is_oracle_mode,
                                                      high_bound_rowkey,
                                                      high_bound_buf,
                                                      OB_MAX_DEFAULT_VALUE_LENGTH,
                                                      high_bound_buf_pos,
                                                      false,
                                                      &tz_info));

      OZ (sql.assign_fmt("ALTER TABLE \"%.*s\".\"%.*s\" ADD PARTITION P_SYS_%d "
                        "VALUES LESS THAN (%.*s);",
                              db_schema->get_database_name_str().length(),
                              db_schema->get_database_name_str().ptr(),
                              table_schema->get_table_name_str().length(),
                              table_schema->get_table_name_str().ptr(),
                              1,
                              static_cast<int>(high_bound_buf_pos), high_bound_buf
                              ));
      OX (sql_proxy = GCTX.sql_proxy_);
      CK (OB_NOT_NULL(sql_proxy));
      OZ (sql_proxy->write(table_schema->get_tenant_id(), sql.ptr(), affected_rows, ORACLE_MODE));
    }
  }
  return ret;
}

/* sync task, need response */
int ObTableLocation::send_add_interval_partition_rpc_new_engine(
    ObIAllocator &allocator,
    ObSQLSessionInfo *session,
    ObSchemaGetterGuard *schema_guard,
    const ObTableSchema *table_schema,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *db_schema = NULL;
  ObSqlString sql;
  ObMySQLProxy *sql_proxy = nullptr;
  int64_t affected_rows = 0;
  ObTimeZoneInfo tz_info;
  ObObj result_obj;
  uint64_t range_part_num = 0;

  CK (OB_NOT_NULL(schema_guard));
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(table_schema));
  OZ (schema_guard->get_database_schema(table_schema->get_tenant_id(), table_schema->get_database_id(), db_schema));
  CK (OB_NOT_NULL(db_schema));

  CK (1 <= row.get_count());
  if (OB_SUCC(ret) && row.get_cell(0).is_null()) {
    ret = OB_ERR_PARTITIONING_KEY_MAPS_TO_A_PARTITION_OUTSIDE_MAXIMUM_PERMITTED_NUMBER_OF_PARTITIONS;
    LOG_USER_WARN(OB_ERR_PARTITIONING_KEY_MAPS_TO_A_PARTITION_OUTSIDE_MAXIMUM_PERMITTED_NUMBER_OF_PARTITIONS);
  }
  CK (table_schema->get_transition_point().is_valid());
  CK (table_schema->get_interval_range().is_valid());
  CK (1 == table_schema->get_transition_point().get_obj_cnt());
  CK (1 == table_schema->get_interval_range().get_obj_cnt());
  OZ (table_schema->get_interval_parted_range_part_num(range_part_num));

  OZ (calc_high_bound_obj_new_engine(allocator,
                                     session,
                                     row.get_cell(0),
                                     range_part_num,
                                     table_schema->get_transition_point().get_obj_ptr()[0],
                                     table_schema->get_interval_range().get_obj_ptr()[0],
                                     result_obj));

  /* alter table table_name add partition ppp values less than (); */
  if (OB_SUCC(ret)) {
    ObRowkey high_bound_rowkey;
    high_bound_rowkey.assign(&result_obj, 1);


    tz_info.set_offset(0);
    OZ (OTTZ_MGR.get_tenant_tz(table_schema->get_tenant_id(), tz_info.get_tz_map_wrap()));

    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], high_bound_buf) {
      MEMSET(high_bound_buf, 0, OB_MAX_DEFAULT_VALUE_LENGTH);
      int64_t high_bound_buf_pos = 0;

      bool is_oracle_mode = false;
      int64_t max_part_id = OB_INVALID_ID;
      OZ (ObPartitionUtils::convert_rowkey_to_sql_literal(is_oracle_mode,
                                                          high_bound_rowkey,
                                                          high_bound_buf,
                                                          OB_MAX_DEFAULT_VALUE_LENGTH,
                                                          high_bound_buf_pos,
                                                          false,
                                                          &tz_info));

      OZ (table_schema->get_max_part_id(max_part_id));
      OZ (sql.assign_fmt("ALTER TABLE \"%.*s\".\"%.*s\" ADD PARTITION P_SYS_%d "
                        "VALUES LESS THAN (%.*s);",
                              db_schema->get_database_name_str().length(),
                              db_schema->get_database_name_str().ptr(),
                              table_schema->get_table_name_str().length(),
                              table_schema->get_table_name_str().ptr(),
                              static_cast<int>(max_part_id + 1),
                              static_cast<int>(high_bound_buf_pos), high_bound_buf
                              ));
      OX (sql_proxy = GCTX.sql_proxy_);
      CK (OB_NOT_NULL(sql_proxy));
      OZ (sql_proxy->write(table_schema->get_tenant_id(), sql.ptr(), affected_rows, ORACLE_MODE));
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_id_by_row(ObExecContext &exec_ctx,
                                              ObDASTabletMapper &tablet_mapper,
                                              ObNewRow &row,
                                              ObIArray<ObTabletID> &tablet_ids,
                                              ObIArray<ObObjectID> &partition_ids,
                                              const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  ObObj func_result;
  share::schema::ObPartitionLevel calc_range_part_level = part_level_;
  bool range_columns = false;
  if (NULL == part_ids) {
    if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type_
       || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type_
       || PARTITION_FUNC_TYPE_INTERVAL == part_type_
       || (PARTITION_FUNC_TYPE_LIST == part_type_ && lib::is_oracle_mode())) {
      part_projector_.project_part_row(PARTITION_LEVEL_ONE, row);
      ObObjectID partition_id = OB_INVALID_ID;
      ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
      OZ(tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_ONE, OB_INVALID_ID,
                          row, tablet_id, partition_id));
      if (OB_INVALID_ID != partition_id) {
        OZ(add_var_to_array_no_dup(partition_ids, partition_id));
        OZ(add_var_to_array_no_dup(tablet_ids, tablet_id));
      }
      range_columns = true;
    } else {
      CK(OB_NOT_NULL(se_part_expr_));
      OZ(se_part_expr_->eval(exec_ctx, row, func_result));
      LOG_DEBUG("part expr func result", K(func_result), K(part_type_));
    }
  } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == subpart_type_ || PARTITION_FUNC_TYPE_LIST_COLUMNS == subpart_type_) {
    calc_range_part_level = PARTITION_LEVEL_TWO;
    part_projector_.project_part_row(calc_range_part_level, row);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
      ObObjectID partition_id = OB_INVALID_ID;
      ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
      OZ(tablet_mapper.get_tablet_and_object_id(PARTITION_LEVEL_TWO, part_ids->at(idx),
                 row, tablet_id, partition_id));
      if (OB_INVALID_ID != partition_id) {
        OZ(add_var_to_array_no_dup(partition_ids, partition_id));
        OZ(add_var_to_array_no_dup(tablet_ids, tablet_id));
      }
    }
    range_columns = true;
  } else {
    CK(OB_NOT_NULL(se_subpart_expr_));
    OZ(se_subpart_expr_->eval(exec_ctx, row, func_result));
  }
  if (OB_FAIL(ret) || range_columns) {
  } else if (OB_FAIL(calc_partition_id_by_func_value(tablet_mapper, func_result, false,
                                                      tablet_ids, partition_ids, part_ids))) {
    LOG_WARN("Failed to calc partition id by func value", K(ret));
  }
  if (0 == partition_ids.count() && PARTITION_LEVEL_ONE == calc_range_part_level) {
    bool is_interval = false;
    if (OB_ISNULL(tablet_mapper.get_table_schema())) {
      // virtual table, do nothing
    } else if (tablet_mapper.get_table_schema()->is_interval_part()) {
      if (OB_FAIL(send_add_interval_partition_rpc(exec_ctx,
                                                  exec_ctx.get_sql_ctx()->schema_guard_,
                                                  row))) {
        /* to do: already added, do same thing as add success */
        if (is_need_retry_interval_part_error(ret)) {
          set_interval_partition_insert_error(ret);
        }
        LOG_WARN("failed to send add interval partition rpc", K(ret));
      } else {
        /* change error */
        set_interval_partition_insert_error(ret);
      }
    }
  }
  return ret;
}

int ObTableLocation::get_all_part_ids(
    ObDASTabletMapper &tablet_mapper,
    ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObObjectID> &partition_ids,
    const ObIArray<ObObjectID> *part_ids) const
{
  int ret = OB_SUCCESS;
  if (!is_partitioned_) {
    OZ (tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids));
  } else {
    if (NULL == part_ids) {
      if (OB_FAIL(tablet_mapper.get_all_tablet_and_object_id(
                                             PARTITION_LEVEL_ONE, OB_INVALID_ID,
                                             tablet_ids, partition_ids))) {
        LOG_WARN("fail to get tablet ids", K(ret));
      }
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < part_ids->count(); ++idx) {
        ObObjectID part_id = part_ids->at(idx);
        if (OB_FAIL(tablet_mapper.get_all_tablet_and_object_id(
                                               PARTITION_LEVEL_TWO, part_id,
                                               tablet_ids, partition_ids))) {
          LOG_WARN("fail to get tablet ids", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableLocation::deal_dml_partition_selection(ObObjectID part_id) const
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

int ObTableLocation::deal_partition_selection(ObIArray<ObTabletID> &tablet_ids,
                                              ObIArray<ObObjectID> &part_ids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 16> tmp_part_ids;
  ObSEArray<ObTabletID, 16> tmp_tablet_ids;
  for (int i = 0; OB_SUCC(ret) && i < part_ids.count(); i++) {
    OZ(tmp_part_ids.push_back(part_ids.at(i)));
    OZ(tmp_tablet_ids.push_back(tablet_ids.at(i)));
  }
  part_ids.reset();
  tablet_ids.reset();
  bool found = false;
  for (int tmp_idx = 0; OB_SUCC(ret) && tmp_idx < tmp_part_ids.count(); tmp_idx++) {
    found = false;
    for (int hint_idx = 0; !found && hint_idx < part_hint_ids_.count(); hint_idx++) {
      if (tmp_part_ids.at(tmp_idx) == part_hint_ids_.at(hint_idx)) {
        found = true;
      }
    }
    if (found ) {
      OZ(part_ids.push_back(tmp_part_ids.at(tmp_idx)));
      OZ(tablet_ids.push_back(tmp_tablet_ids.at(tmp_idx)));
    }
  }
  return ret;
}

int ObTableLocation::add_and_node(
    ObPartLocCalcNode *l_node,
    ObPartLocCalcNode *&r_node)
{
  int ret = OB_SUCCESS;
  if (NULL == l_node
      && NULL == r_node) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("l_node and r_node should not both be NULL", K(ret));
  } else if (NULL == l_node
             || NULL == r_node) {
   r_node = (NULL == l_node) ? r_node : l_node;
  } else {
    ObPLAndNode *and_node = NULL;
    if (NULL == (and_node = static_cast<ObPLAndNode*>(ObPartLocCalcNode::create_part_calc_node(
                    allocator_, calc_nodes_, ObPartLocCalcNode::CALC_AND)))) {
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

int ObTableLocation::add_or_node(
    ObPartLocCalcNode *l_node,
    ObPartLocCalcNode *&r_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("l_node should not be NULL", K(ret));
  } else if (NULL == r_node) {
    r_node = l_node;
  } else {
    ObPLOrNode *or_node = NULL;
    if (NULL == (or_node = static_cast<ObPLOrNode*>(ObPartLocCalcNode::create_part_calc_node(
                 allocator_, calc_nodes_, ObPartLocCalcNode::CALC_OR)))) {
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

int ObTableLocation::clear_columnlized_in_row_desc(RowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  int64_t row_desc_count = row_desc.get_column_num();
  ObRawExpr *expr = NULL;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < row_desc_count; ++idx) {
    if (OB_ISNULL(expr = row_desc.get_column(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expr in row desc should not be NULL", K(ret));
    } else if (OB_FAIL(expr->clear_flag(IS_COLUMNLIZED))) {
      LOG_WARN("Failed to clear IS_COLUMNLIZED flag", K(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObTableLocation::init_row(
    ObIAllocator &allocator,
    const int64_t column_num,
    ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
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


int ObTableLocation::se_calc_value_item(ObCastCtx cast_ctx,
                                        ObExecContext &exec_ctx,
                                        const ParamStore &params,
                                        const ValueItemExpr &vie,
                                        ObNewRow &input_row,
                                        ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (QUESTMARK_TYPE == vie.type_) {
    if (vie.idx_ < 0 || vie.idx_ >= params.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param idx", K(vie.idx_), K(ret));
    } else {
      value = params.at(vie.idx_);
    }
  } else if (CONST_OBJ_TYPE == vie.type_) {
    value = vie.obj_;
  } else if (CONST_EXPR_TYPE == vie.type_) {
    if (OB_ISNULL(vie.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(vie.expr_->eval(exec_ctx, input_row, value))) {
      LOG_WARN("failed to eval expr", K(ret));
    }
  }
  // implicit cast values to value desc column type
  if (OB_SUCC(ret)) {
    if (vie.dst_type_ != value.get_type() || vie.dst_cs_type_ != value.get_collation_type()) {
      if (ob_is_enum_or_set_type(vie.dst_type_)) {
        ObExpectType expect_type;
        expect_type.set_type(vie.dst_type_);
        expect_type.set_collation_type(vie.dst_cs_type_);
        ObSEArray<ObString, 8> type_infos;
        for (int64_t i = 0; OB_SUCC(ret) && i < vie.enum_set_values_cnt_; ++i) {
          OZ(type_infos.push_back(vie.enum_set_values_[i]));
        }
        expect_type.set_type_infos(&type_infos);
        OZ(ObObjCaster::to_type(expect_type, cast_ctx, value, value));
      } else {
        OZ(ObObjCaster::to_type(vie.dst_type_, vie.dst_cs_type_, cast_ctx, value, value));
      }
    }
  }
  return ret;
}

//calc row use new static type engine expr
int ObTableLocation::se_calc_value_item_row(common::ObExprCtx &expr_ctx,
                                            ObExecContext &exec_ctx,
                                            const ISeValueItemExprs &vies,
                                            const int64_t key_count,
                                            const int64_t value_idx,
                                            ObNewRow &input_row,
                                            ObNewRow &output_row) const
{
  int ret = OB_SUCCESS;
  if (key_count <= 0) {
    //do  nothing
  } else if (OB_ISNULL(output_row.cells_) || output_row.count_ < key_count
             || OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output_row row not init correct with key_count",
             K(ret), K(key_count), K(output_row), K(exec_ctx.get_physical_plan_ctx()));
  } else {
    const int64_t start_pos = value_idx * key_count;
    const auto &param_store = exec_ctx.get_physical_plan_ctx()->get_param_store();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    for (int64_t key_idx = 0; OB_SUCC(ret) && key_idx < key_count; ++key_idx) {
      ValueItemExpr vie;
      OZ(vies.at(key_idx + start_pos, vie));
      if (QUESTMARK_TYPE == vie.type_) {
        output_row.cells_[key_idx] = param_store.at(vie.idx_);
      } else if (CONST_OBJ_TYPE == vie.type_) {
        output_row.cells_[key_idx] = vie.obj_;
      } else if (CONST_EXPR_TYPE == vie.type_) {
        CK(OB_NOT_NULL(vie.expr_));
        OZ(vie.expr_->eval(exec_ctx, input_row, output_row.cells_[key_idx]));
      }
      // implicit cast values to value desc column type
      if (OB_SUCC(ret)) {
        if (vie.dst_type_ != output_row.cells_[key_idx].get_type()
            || vie.dst_cs_type_ != output_row.cells_[key_idx].get_collation_type()) {
          if (ob_is_enum_or_set_type(vie.dst_type_)) {
            ObExpectType expect_type;
            expect_type.set_type(vie.dst_type_);
            expect_type.set_collation_type(vie.dst_cs_type_);
            ObSEArray<ObString, 8> type_infos;
            for (int64_t i = 0; OB_SUCC(ret) && i < vie.enum_set_values_cnt_; ++i) {
              OZ(type_infos.push_back(vie.enum_set_values_[i]));
            }
            expect_type.set_type_infos(&type_infos);
            OZ(ObObjCaster::to_type(expect_type, cast_ctx,
                                    output_row.cells_[key_idx], output_row.cells_[key_idx]));
          } else {
            OZ(ObObjCaster::to_type(vie.dst_type_, vie.dst_cs_type_, cast_ctx,
                                    output_row.cells_[key_idx], output_row.cells_[key_idx]));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLocation::replace_ref_table_id(const uint64_t ref_table_id, ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("set log op infos", K(ref_table_id), K(loc_meta_));
  //replace the partition hint ids with local index object id
  ObDASTabletMapper tablet_mapper;
  if (is_non_partition_optimized_) {
    tablet_mapper.set_non_partitioned_table_ids(tablet_id_, object_id_, &related_list_);
  }
  if (OB_FAIL(DAS_CTX(exec_ctx).get_das_tablet_mapper(loc_meta_.ref_table_id_, tablet_mapper))) {
    LOG_WARN("get das tablet mapper failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_hint_ids_.count(); ++i) {
    ObObjectID dst_object_id;
    if (OB_FAIL(tablet_mapper.get_related_partition_id(loc_meta_.ref_table_id_,
                                                       part_hint_ids_.at(i),
                                                       ref_table_id,
                                                       dst_object_id))) {
      LOG_WARN("get related partition id failed", K(ret), K(loc_meta_),
               K(ref_table_id), K(part_hint_ids_.at(i)), K(i));
    } else {
      part_hint_ids_.at(i) = dst_object_id;
    }
  }
  if (OB_SUCC(ret)) {
    //replace the ref table id finally
    loc_meta_.ref_table_id_ = ref_table_id;
  }
  return ret;
}

int ObPartIdRowMapManager::add_row_for_part(int64_t part_idx, int64_t row_id)
{
  int ret = OB_SUCCESS;
  //linear search right now. maybe, we can run binary search even interpolation search to get a better perf.
  //if you are free, please do not be shy to improve this.
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
  //linear search right now. maybe, we can run binary search even interpolation search to get a better perf.
  //if you are free, please do not be shy to improve this.
  if (part_index >= 0 && part_index < manager_.count()) {
    ret = &(manager_.at(part_index).list_);
  }
  return ret;
}

int ObPartIdRowMapManager::MapEntry::assign(const ObPartIdRowMapManager::MapEntry &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(list_.assign(other.list_))) {
      LOG_WARN("copy list failed", K(ret));
    }
  }
  return ret;
}

int ObPartIdRowMapManager::assign(const ObPartIdRowMapManager &other)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(other.manager_, i) {
    MapEntry map_entry;
    if (OB_FAIL(map_entry.assign(other.manager_.at(i)))) {
      LOG_WARN("assign manager entry failed", K(ret), K(i));
    } else if (OB_FAIL(manager_.push_back(map_entry))) {
      LOG_WARN("store map entry failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPLAndNode)
{
  int ret = OB_SUCCESS;
  bool has_child = false;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    has_child = true;
  } else if (OB_ISNULL(left_node_) && OB_ISNULL(right_node_)) {
    has_child = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node count is unexpected", K(ret));
  }
  BASE_SER(ObPartLocCalcNode);
  OB_UNIS_ENCODE(has_child);
  if (OB_SUCC(ret) && has_child) {
    OB_UNIS_ENCODE(left_node_->node_type_);
    OB_UNIS_ENCODE(right_node_->node_type_);
    if (OB_FAIL(left_node_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize left node", K(ret));
    } else if (OB_FAIL(right_node_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize right node", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPLAndNode)
{
  int64_t len = 0;
  bool has_child = false;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    has_child = true;
  } else if (OB_ISNULL(left_node_) && OB_ISNULL(right_node_)) {
    has_child = false;
  }
  BASE_ADD_LEN(ObPartLocCalcNode);
  OB_UNIS_ADD_LEN(has_child);
  if (has_child) {
    OB_UNIS_ADD_LEN(left_node_->node_type_);
    OB_UNIS_ADD_LEN(right_node_->node_type_);
    len += left_node_->get_serialize_size();
    len += right_node_->get_serialize_size();
  }
  return len;
}

OB_DEF_DESERIALIZE(ObPLAndNode)
{
  int ret = OB_SUCCESS;
  bool has_child = false;
  BASE_DESER(ObPartLocCalcNode);
  OB_UNIS_DECODE(has_child);
  if (has_child) {
    NodeType left_child_type = INVALID;
    NodeType right_child_type = INVALID;
    OB_UNIS_DECODE(left_child_type);
    OB_UNIS_DECODE(right_child_type);
    ObPartLocCalcNode *left_child = NULL;
    ObPartLocCalcNode *right_child = NULL;
    if (OB_FAIL(create_part_calc_node(allocator_, left_child_type, left_child))) {
      LOG_WARN("fail to create part calc node", K(ret));
    } else if (OB_FAIL(create_part_calc_node(allocator_, right_child_type, right_child))) {
      LOG_WARN("fail to create part calc node", K(ret));
    } else if (OB_FAIL(left_child->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to do deserialize", K(ret));
    } else if (OB_FAIL(right_child->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to do deserialize", K(ret));
    } else {
      left_node_ = left_child;
      right_node_ = right_child;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPLOrNode)
{
  int ret = OB_SUCCESS;
  bool has_child = false;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    has_child = true;
  } else if (OB_ISNULL(left_node_) && OB_ISNULL(right_node_)) {
    has_child = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child node count is unexpected", K(ret));
  }
  BASE_SER(ObPartLocCalcNode);
  OB_UNIS_ENCODE(has_child);
  if (OB_SUCC(ret) && has_child) {
    OB_UNIS_ENCODE(left_node_->node_type_);
    OB_UNIS_ENCODE(right_node_->node_type_);
    if (OB_FAIL(left_node_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize left node", K(ret));
    } else if (OB_FAIL(right_node_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize right node", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPLOrNode)
{
  int64_t len = 0;
  bool has_child = false;
  if (OB_NOT_NULL(left_node_) && OB_NOT_NULL(right_node_)) {
    has_child = true;
  } else if (OB_ISNULL(left_node_) && OB_ISNULL(right_node_)) {
    has_child = false;
  }
  BASE_ADD_LEN(ObPartLocCalcNode);
  OB_UNIS_ADD_LEN(has_child);
  if (has_child) {
    OB_UNIS_ADD_LEN(left_node_->node_type_);
    OB_UNIS_ADD_LEN(right_node_->node_type_);
    len += left_node_->get_serialize_size();
    len += right_node_->get_serialize_size();
  }
  return len;
}

OB_DEF_DESERIALIZE(ObPLOrNode)
{
  int ret = OB_SUCCESS;
  bool has_child = false;
  BASE_DESER(ObPartLocCalcNode);
  OB_UNIS_DECODE(has_child);
  if (has_child) {
    NodeType left_child_type = INVALID;
    NodeType right_child_type = INVALID;
    OB_UNIS_DECODE(left_child_type);
    OB_UNIS_DECODE(right_child_type);
    ObPartLocCalcNode *left_child = NULL;
    ObPartLocCalcNode *right_child = NULL;
    if (OB_FAIL(create_part_calc_node(allocator_, left_child_type, left_child))) {
      LOG_WARN("fail to create part calc node", K(ret));
    } else if (OB_FAIL(create_part_calc_node(allocator_, right_child_type, right_child))) {
      LOG_WARN("fail to create part calc node", K(ret));
    } else if (OB_FAIL(left_child->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to do deserialize", K(ret));
    } else if (OB_FAIL(right_child->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to do deserialize", K(ret));
    } else {
      left_node_ = left_child;
      right_node_ = right_child;
    }
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObPLFuncValueNode::ParamValuePair, param_idx_, obj_value_);
OB_SERIALIZE_MEMBER(ObPartLocCalcNode, node_type_);
OB_SERIALIZE_MEMBER((ObPLQueryRangeNode, ObPartLocCalcNode), pre_query_range_);

OB_DEF_SERIALIZE(ObPLFuncValueNode)
{
  int ret = OB_SUCCESS;
  BASE_SER(ObPartLocCalcNode);
  LST_DO_CODE(OB_UNIS_ENCODE,
              vie_,
              param_value_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPLFuncValueNode)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObPartLocCalcNode);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              vie_,
              param_value_);
  return len;
}

OB_DEF_DESERIALIZE(ObPLFuncValueNode)
{
  int ret = OB_SUCCESS;
  BASE_DESER(ObPartLocCalcNode);
  OZ (vie_.deserialize(allocator_, buf, data_len, pos));
  OB_UNIS_DECODE(param_value_);
  return ret;
}

OB_DEF_SERIALIZE(ObPLColumnValueNode)
{
  int ret = OB_SUCCESS;
  BASE_SER(ObPartLocCalcNode);
  OB_UNIS_ENCODE(vie_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPLColumnValueNode)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObPartLocCalcNode);
  OB_UNIS_ADD_LEN(vie_);
  return len;
}

OB_DEF_DESERIALIZE(ObPLColumnValueNode)
{
  int ret = OB_SUCCESS;
  BASE_DESER(ObPartLocCalcNode);
  OZ (vie_.deserialize(allocator_, buf, data_len, pos));
  return ret;
}

int ValueItemExpr::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  if (CONST_OBJ_TYPE == type_) {
    OB_UNIS_ENCODE(obj_);
  } else if (CONST_EXPR_TYPE == type_) {
    OB_UNIS_ENCODE(*expr_);
  } else if (QUESTMARK_TYPE == type_) {
    OB_UNIS_ENCODE(idx_);
  }
  OB_UNIS_ENCODE(dst_type_);
  if (ob_is_enum_or_set_type(dst_type_)) {
    CK(OB_NOT_NULL(enum_set_values_));
    OB_UNIS_ENCODE_ARRAY(enum_set_values_, enum_set_values_cnt_);
  }

  return ret;

}

int64_t ValueItemExpr::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  if (CONST_OBJ_TYPE == type_) {
    OB_UNIS_ADD_LEN(obj_);
  } else if (CONST_EXPR_TYPE == type_) {
    OB_UNIS_ADD_LEN(*expr_);
  } else if (QUESTMARK_TYPE == type_) {
    OB_UNIS_ADD_LEN(idx_);
  }
  OB_UNIS_ADD_LEN(dst_type_);
  if (ob_is_enum_or_set_type(dst_type_)) {
    OB_UNIS_ADD_LEN_ARRAY(enum_set_values_, enum_set_values_cnt_);
  }

  return len;
}

int ValueItemExpr::deserialize(common::ObIAllocator &allocator, const char *buf,
                               const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  if (CONST_OBJ_TYPE == type_) {
    OB_UNIS_DECODE(obj_);
  } else if (CONST_EXPR_TYPE == type_) {
    char *mem = static_cast<char *>(allocator.alloc(sizeof(ObTempExpr)));
    CK(OB_NOT_NULL(mem));
    OX(expr_ = new(mem)ObTempExpr(allocator));
    OB_UNIS_DECODE(*expr_);
  } else if (QUESTMARK_TYPE == type_) {
    OB_UNIS_DECODE(idx_);
  }
  OB_UNIS_DECODE(dst_type_);
  if (ob_is_enum_or_set_type(dst_type_)) {
    OB_UNIS_DECODE(enum_set_values_cnt_);
    if (enum_set_values_cnt_ > 0) {
      enum_set_values_ =
        static_cast<ObString *>(allocator.alloc(sizeof(ObString) * enum_set_values_cnt_));
      CK(OB_NOT_NULL(enum_set_values_));
      OB_UNIS_DECODE_ARRAY(enum_set_values_, enum_set_values_cnt_)
    }
  }

  return ret;
}

int ValueItemExpr::deep_copy(ObIAllocator &allocator, ValueItemExpr &dst) const
{
  int ret = OB_SUCCESS;
  dst = *this;
  if (CONST_OBJ_TYPE == type_) {
    OZ(deep_copy_obj(allocator, obj_, dst.obj_));
  } else if (CONST_EXPR_TYPE == type_) {
    OZ(expr_->deep_copy(allocator, dst.expr_));
  }
  dst.dst_type_ = dst_type_;
  if (ob_is_enum_or_set_type(dst_type_)) {
    dst.enum_set_values_cnt_ = enum_set_values_cnt_;
    dst.enum_set_values_ =
      static_cast<ObString *>(allocator.alloc(sizeof(ObString) * enum_set_values_cnt_));
    CK(OB_NOT_NULL(dst.enum_set_values_));
    for (int64_t i = 0; OB_SUCC(ret) && i < enum_set_values_cnt_; ++i) {
      OZ(ob_write_string(allocator, enum_set_values_[i], dst.enum_set_values_[i]));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObTableLocation)
{
  int ret = OB_SUCCESS;
  bool has_part_expr = (se_part_expr_ != NULL);
  bool has_subpart_expr = (se_subpart_expr_ != NULL);
  bool has_calc_node = (calc_node_ != NULL);
  bool has_gen_col_node = (gen_col_node_ != NULL);
  bool has_subcalc_node = (subcalc_node_ != NULL);
  bool has_sub_gen_col_node = (sub_gen_col_node_ != NULL);
  bool has_gen_col_expr = (NULL != gen_col_node_ || NULL != se_gen_col_expr_);
  bool has_sub_gen_col_expr = (NULL != sub_gen_col_node_ || NULL != se_sub_gen_col_expr_);
  LST_DO_CODE(OB_UNIS_ENCODE,
              loc_meta_,
              stmt_type_,
              is_partitioned_,
              part_level_,
              part_type_,
              subpart_type_,
              has_part_expr,
              has_subpart_expr,
              part_projector_,
              inited_);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, is_oracle_temp_table_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                part_get_all_,
                subpart_get_all_,
                is_valid_range_columns_part_range_,
                is_valid_range_columns_subpart_range_,
                has_dynamic_exec_param_,
                has_calc_node,
                has_gen_col_node,
                has_subcalc_node,
                has_sub_gen_col_node);
  }
  if (OB_SUCC(ret)) {
    if (has_calc_node) {
      OB_UNIS_ENCODE(calc_node_->node_type_);
      if (OB_FAIL(calc_node_->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_gen_col_node) {
      OB_UNIS_ENCODE(gen_col_node_->node_type_);
      if (OB_FAIL(gen_col_node_->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_subcalc_node) {
      OB_UNIS_ENCODE(subcalc_node_->node_type_);
      if (OB_FAIL(subcalc_node_->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_sub_gen_col_node) {
      OB_UNIS_ENCODE(sub_gen_col_node_->node_type_);
      if (OB_FAIL(sub_gen_col_node_->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize calc node", K(ret));
      }
    }
  }
  OB_UNIS_ENCODE(has_gen_col_expr);
  OB_UNIS_ENCODE(has_sub_gen_col_expr);
  if (has_part_expr) { OB_UNIS_ENCODE(*se_part_expr_);}
  if (has_subpart_expr) { OB_UNIS_ENCODE(*se_subpart_expr_); }
  if (has_gen_col_expr) { OB_UNIS_ENCODE(*se_gen_col_expr_)}
  if (has_sub_gen_col_expr) { OB_UNIS_ENCODE(*se_sub_gen_col_expr_)}
  OB_UNIS_ENCODE(vies_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < vies_.count(); i++) {
    OZ(vies_.at(i).serialize(buf, buf_len, pos));
  }
  OB_UNIS_ENCODE(sub_vies_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < vies_.count(); i++) {
    OZ(sub_vies_.at(i).serialize(buf, buf_len, pos));
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(part_hint_ids_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < part_hint_ids_.count(); i++) {
      OB_UNIS_ENCODE(part_hint_ids_.at(i));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                is_part_range_get_,
                is_subpart_range_get_);
  }
  OB_UNIS_ENCODE(is_non_partition_optimized_);
  OB_UNIS_ENCODE(tablet_id_);
  OB_UNIS_ENCODE(object_id_);
  OB_UNIS_ENCODE(related_list_);
  OB_UNIS_ENCODE(table_type_);
  OB_UNIS_ENCODE(check_no_partition_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLocation)
{
  int64_t len = 0;
  bool has_part_expr = (se_part_expr_ != NULL);
  bool has_subpart_expr = (se_subpart_expr_ != NULL);
  bool has_calc_node = (calc_node_ != NULL);
  bool has_gen_col_node = (gen_col_node_ != NULL);
  bool has_subcalc_node = (subcalc_node_ != NULL);
  bool has_sub_gen_col_node = (sub_gen_col_node_ != NULL);
  bool has_gen_col_expr = (NULL != gen_col_node_ || NULL != se_gen_col_expr_);
  bool has_sub_gen_col_expr = (NULL != sub_gen_col_node_ || NULL != se_sub_gen_col_expr_);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              loc_meta_,
              stmt_type_,
              is_partitioned_,
              part_level_,
              part_type_,
              subpart_type_,
              has_part_expr,
              has_subpart_expr,
              part_projector_,
              inited_,
              is_oracle_temp_table_,
              part_get_all_,
              subpart_get_all_,
              is_valid_range_columns_part_range_,
              is_valid_range_columns_subpart_range_,
              has_dynamic_exec_param_,
              has_calc_node,
              has_gen_col_node,
              has_subcalc_node,
              has_sub_gen_col_node);
  if (has_calc_node) {
    OB_UNIS_ADD_LEN(calc_node_->node_type_);
    len += calc_node_->get_serialize_size();
  }
  if (has_gen_col_node) {
    OB_UNIS_ADD_LEN(gen_col_node_->node_type_);
    len += gen_col_node_->get_serialize_size();
  }
  if (has_subcalc_node) {
    OB_UNIS_ADD_LEN(subcalc_node_->node_type_);
    len += subcalc_node_->get_serialize_size();
  }
  if (has_sub_gen_col_node) {
    OB_UNIS_ADD_LEN(sub_gen_col_node_->node_type_);
    len += sub_gen_col_node_->get_serialize_size();
  }
  OB_UNIS_ADD_LEN(has_gen_col_expr);
  OB_UNIS_ADD_LEN(has_sub_gen_col_expr);
  if (has_part_expr) { OB_UNIS_ADD_LEN(*se_part_expr_); }
  if (has_subpart_expr) { OB_UNIS_ADD_LEN(*se_subpart_expr_); }
  if (has_gen_col_expr) { OB_UNIS_ADD_LEN(*se_gen_col_expr_); }
  if (has_sub_gen_col_expr) { OB_UNIS_ADD_LEN(*se_sub_gen_col_expr_); }
  OB_UNIS_ADD_LEN(vies_.count());
  for (int64_t i = 0; i < vies_.count(); i++) {
    len += vies_.at(i).get_serialize_size();
  }
  OB_UNIS_ADD_LEN(sub_vies_.count());
  for (int64_t i = 0; i < vies_.count(); i++) {
    len += sub_vies_.at(i).get_serialize_size();
  }
  OB_UNIS_ADD_LEN(part_hint_ids_.count());
  for (int64_t i = 0; i < part_hint_ids_.count(); i++) {
    OB_UNIS_ADD_LEN(part_hint_ids_.at(i));
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              is_part_range_get_,
              is_subpart_range_get_);
  OB_UNIS_ADD_LEN(is_non_partition_optimized_);
  OB_UNIS_ADD_LEN(tablet_id_);
  OB_UNIS_ADD_LEN(object_id_);
  OB_UNIS_ADD_LEN(related_list_);
  OB_UNIS_ADD_LEN(table_type_);
  OB_UNIS_ADD_LEN(check_no_partition_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableLocation)
{
  int ret = OB_SUCCESS;
  bool has_part_expr = false;
  bool has_subpart_expr = false;
  bool has_gen_col_expr = false;
  bool has_sub_gen_col_expr = false;
  bool has_calc_node = (calc_node_ != NULL);
  bool has_gen_col_node = (gen_col_node_ != NULL);
  bool has_subcalc_node = (subcalc_node_ != NULL);
  bool has_sub_gen_col_node = (sub_gen_col_node_ != NULL);
  LST_DO_CODE(OB_UNIS_DECODE,
              loc_meta_,
              stmt_type_,
              is_partitioned_,
              part_level_,
              part_type_,
              subpart_type_,
              has_part_expr,
              has_subpart_expr,
              part_projector_,
              inited_);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, is_oracle_temp_table_);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                part_get_all_,
                subpart_get_all_,
                is_valid_range_columns_part_range_,
                is_valid_range_columns_subpart_range_,
                has_dynamic_exec_param_,
                has_calc_node,
                has_gen_col_node,
                has_subcalc_node,
                has_sub_gen_col_node);
  }
  if (OB_SUCC(ret)) {
    calc_nodes_.reset();
    if (has_calc_node) {
      ObPartLocCalcNode::NodeType calc_node_type = ObPartLocCalcNode::INVALID;
      OB_UNIS_DECODE(calc_node_type);
      if (OB_FAIL(ObPartLocCalcNode::create_part_calc_node(allocator_, calc_node_type, calc_node_))) {
        LOG_WARN("fail to create part calc node", K(ret));
      } else if (OB_FAIL(calc_node_->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize calc node", K(ret));
      } else if (OB_FAIL(calc_node_->add_part_calc_node(calc_nodes_))) {
        LOG_WARN("fail to add calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_gen_col_node) {
      ObPartLocCalcNode::NodeType gen_col_node_type = ObPartLocCalcNode::INVALID;
      OB_UNIS_DECODE(gen_col_node_type);
      if (OB_FAIL(ObPartLocCalcNode::create_part_calc_node(allocator_, gen_col_node_type, gen_col_node_))) {
        LOG_WARN("fail to create part calc node", K(ret));
      } else if (OB_FAIL(gen_col_node_->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize calc node", K(ret));
      } else if (OB_FAIL(gen_col_node_->add_part_calc_node(calc_nodes_))) {
        LOG_WARN("fail to add calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_subcalc_node) {
      ObPartLocCalcNode::NodeType subcalc_node_type = ObPartLocCalcNode::INVALID;
      OB_UNIS_DECODE(subcalc_node_type);
      if (OB_FAIL(ObPartLocCalcNode::create_part_calc_node(allocator_, subcalc_node_type, subcalc_node_))) {
        LOG_WARN("fail to create part calc node", K(ret));
      } else if (OB_FAIL(subcalc_node_->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize calc node", K(ret));
      } else if (OB_FAIL(subcalc_node_->add_part_calc_node(calc_nodes_))) {
        LOG_WARN("fail to add calc node", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (has_sub_gen_col_node) {
      ObPartLocCalcNode::NodeType sub_gen_node_type = ObPartLocCalcNode::INVALID;
      OB_UNIS_DECODE(sub_gen_node_type);
      if (OB_FAIL(ObPartLocCalcNode::create_part_calc_node(allocator_, sub_gen_node_type, sub_gen_col_node_))) {
        LOG_WARN("fail to create part calc node", K(ret));
      } else if (OB_FAIL(sub_gen_col_node_->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize calc node", K(ret));
      } else if (OB_FAIL(sub_gen_col_node_->add_part_calc_node(calc_nodes_))) {
        LOG_WARN("fail to add calc node", K(ret));
      }
    }
  }
  OB_UNIS_DECODE(has_gen_col_expr);
  OB_UNIS_DECODE(has_sub_gen_col_expr);
  if (OB_SUCC(ret)) {
    if (has_part_expr) {
      char *mem = static_cast<char *>(allocator_.alloc(sizeof(ObTempExpr)));
      CK(OB_NOT_NULL(mem));
      OX(se_part_expr_ = new(mem)ObTempExpr(allocator_));
      OB_UNIS_DECODE(*se_part_expr_);
    }
    if (OB_SUCC(ret) && has_subpart_expr) {
      char *mem = static_cast<char *>(allocator_.alloc(sizeof(ObTempExpr)));
      CK(OB_NOT_NULL(mem));
      OX(se_subpart_expr_ = new(mem)ObTempExpr(allocator_));
      OB_UNIS_DECODE(*se_subpart_expr_);
    }
    if (OB_SUCC(ret) && has_gen_col_expr) {
      char *mem = static_cast<char *>(allocator_.alloc(sizeof(ObTempExpr)));
      CK(OB_NOT_NULL(mem));
      OX(se_gen_col_expr_ = new(mem)ObTempExpr(allocator_));
      OB_UNIS_DECODE(*se_gen_col_expr_);
    }
    if (OB_SUCC(ret) && has_sub_gen_col_expr) {
      char *mem = static_cast<char *>(allocator_.alloc(sizeof(ObTempExpr)));
      CK(OB_NOT_NULL(mem));
      OX(se_sub_gen_col_expr_ = new(mem)ObTempExpr(allocator_));
      OB_UNIS_DECODE(*se_sub_gen_col_expr_);
    }
    int64_t vie_count = 0;
    OB_UNIS_DECODE(vie_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < vie_count; i++) {
      ValueItemExpr vie;
      OZ(vie.deserialize(allocator_, buf, data_len, pos));
      OZ(vies_.push_back(vie));
    }
    int64_t sub_vie_count = 0;
    OB_UNIS_DECODE(sub_vie_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_vie_count; i++) {
      ValueItemExpr vie;
      OZ(vie.deserialize(allocator_, buf, data_len, pos));
      OZ(sub_vies_.push_back(vie));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t part_hint_ids_count = 0;
    OB_UNIS_DECODE(part_hint_ids_count);
    OZ(part_hint_ids_.init(part_hint_ids_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < part_hint_ids_count; i++) {
      ObObjectID part_hint_id;
      OB_UNIS_DECODE(part_hint_id);
      OZ(part_hint_ids_.push_back(part_hint_id));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE,
                is_part_range_get_,
                is_subpart_range_get_);
  }
  OB_UNIS_DECODE(is_non_partition_optimized_);
  OB_UNIS_DECODE(tablet_id_);
  OB_UNIS_DECODE(object_id_);
  OB_UNIS_DECODE(related_list_);
  OB_UNIS_DECODE(table_type_);
  OB_UNIS_DECODE(check_no_partition_);
  return ret;
}

int ObTableLocation::get_partition_ids_by_range(ObExecContext &exec_ctx,
                                                const ObNewRange *part_range,
                                                const ObNewRange *gen_range,
                                                ObIArray<ObTabletID> &tablet_ids,
                                                ObIArray<ObObjectID> &partition_ids,
                                                const ObIArray<ObObjectID> *level_one_part_ids) const
{
  int ret = OB_SUCCESS;
  ObPartLocCalcNode *gen_col_node = NULL == level_one_part_ids ? gen_col_node_ : sub_gen_col_node_;
  ObTempExpr *se_gen_col_expr = NULL;
  se_gen_col_expr = NULL == level_one_part_ids ? se_gen_col_expr_: se_sub_gen_col_expr_;
  ObSEArray<ObObjectID, 5> gen_part_ids;
  ObSEArray<ObTabletID, 5> gen_tablet_ids;
  bool all_part_by_part_range = false;  // 通过part range计算出的partition ids是表的全部partition
  bool all_part_by_gen_range = false;   // 通过gen range计算出的partition ids是表的全部partition
  ObDASTabletMapper tablet_mapper;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLocation not inited", K(ret));
  } else if (is_non_partition_optimized_
             && FALSE_IT(tablet_mapper.set_non_partitioned_table_ids(
                             tablet_id_, object_id_, &related_list_))) {
  } else if (OB_FAIL(exec_ctx.get_das_ctx().get_das_tablet_mapper(
                      loc_meta_.ref_table_id_, tablet_mapper, &loc_meta_.related_table_ids_))) {
    LOG_WARN("fail to get das tablet mapper", K(ret));
  } else if (!is_partitioned_) {
    OZ (tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids));
  } else {
    // 计算part range涉及到的partition ids
    if (NULL == part_range) {
      all_part_by_part_range = true;
    } else if (OB_FAIL(calc_partition_ids_by_range(exec_ctx, tablet_mapper, part_range,
                                                   tablet_ids, partition_ids,
                                                   all_part_by_part_range, level_one_part_ids,
                                                   NULL))) {
      LOG_WARN("Failed to calc partitoin ids by calc node", K(ret));
    }
    // 计算gen range涉及到的partition ids
    if (OB_SUCC(ret)) {
      if (NULL == gen_range || OB_ISNULL(gen_col_node)
          || OB_ISNULL(se_gen_col_expr)) {
        all_part_by_gen_range = true;
      } else if (OB_FAIL(calc_partition_ids_by_range(exec_ctx, tablet_mapper, gen_range,
                                                     gen_tablet_ids, gen_part_ids,
                                                     all_part_by_gen_range, level_one_part_ids,
                                                     se_gen_col_expr))) {
        LOG_WARN("Failed to calcl partition ids by gen col node", K(ret));
      }
    }

    // 通过part range和gen range分别计算出了两组partition ids, 取两者的交集
    if (OB_SUCC(ret)) {
      if (!all_part_by_part_range && !all_part_by_gen_range) {
        OZ (intersect_partition_ids(gen_part_ids, partition_ids));
        OZ (intersect_partition_ids(gen_tablet_ids, tablet_ids));
      } else if (!all_part_by_part_range && all_part_by_gen_range) {
        // do nothing
      } else if (all_part_by_part_range && !all_part_by_gen_range) {
        OZ (append(partition_ids, gen_part_ids));
        OZ (append(tablet_ids, gen_tablet_ids));
      } else if (OB_FAIL(get_all_part_ids(tablet_mapper, tablet_ids,
                                          partition_ids, level_one_part_ids))) {
        LOG_WARN("Get all part ids error", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_partition_ids_by_range(ObExecContext &exec_ctx,
                                                 ObDASTabletMapper &tablet_mapper,
                                                 const ObNewRange *range,
                                                 ObIArray<ObTabletID> &tablet_ids,
                                                 ObIArray<ObObjectID> &partition_ids,
                                                 bool &all_part,
                                                 const ObIArray<ObObjectID> *part_ids,
                                                 const ObTempExpr *se_gen_col_expr) const
{
  int ret = OB_SUCCESS;
  all_part = false;
  ObSEArray<ObNewRange *, 1> dummy_query_ranges;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObNewRange", K(ret));
  } else if (range->empty()) {
    //do nothing. partition ids will be empty
  } else if (OB_FAIL(dummy_query_ranges.push_back(const_cast<ObNewRange *>(range)))) {
    LOG_WARN("failed to push back query range", K(ret));
  } else if (OB_FAIL(calc_partition_ids_by_ranges(exec_ctx,
                                                  tablet_mapper,
                                                  dummy_query_ranges,
                                                  range->is_single_rowkey(),
                                                  tablet_ids,
                                                  partition_ids,
                                                  all_part,
                                                  part_ids,
                                                  se_gen_col_expr))) {
    LOG_WARN("Failed to get partition ids", K(ret), K(loc_meta_));
  }
  return ret;
}


int ObTableLocation::generate_row_desc_from_row_desc(const ObDMLStmt &stmt,
                                                     const uint64_t data_table_id,
                                                     ObRawExprFactory &expr_factory,
                                                     const RowDesc &input_row_desc,
                                                     RowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *col_expr = NULL;
  if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_row_desc.get_column_num(); ++i) {
    ObRawExpr *expr = input_row_desc.get_column(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_LIKELY(expr->is_column_ref_expr())) {
      uint64_t column_id = static_cast<ObColumnRefRawExpr *>(expr)->get_column_id();
      col_expr = stmt.get_column_expr_by_id(data_table_id, column_id);
      if (OB_ISNULL(col_expr)) {
        // 没有用到的column, 计算part expr时一定用不到, mock一个column expr占位
        if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
          LOG_WARN("create mock rowkey column expr failed", K(ret));
        } else if (OB_FAIL(row_desc.add_column(col_expr))) {
          LOG_WARN("add rowkey column expr to row desc failed", K(ret));
        }
      } else if (OB_FAIL(row_desc.add_column(col_expr))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    } else {
      // 不是column expr, 计算part expr时一定用不到, mock一个column expr占位
      if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
        LOG_WARN("create mock rowkey column expr failed", K(ret));
      } else if (OB_FAIL(row_desc.add_column(col_expr))) {
        LOG_WARN("add rowkey column expr to row desc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLocation::can_get_part_by_range_for_range_columns(const ObRawExpr *part_expr,
                                                             bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (part_expr->is_column_ref_expr()) {
    is_valid = true;
  } else if (T_OP_ROW == part_expr->get_expr_type()) {
    const ObRawExpr *col_expr = NULL;
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


int ObTableLocation::can_get_part_by_range_for_temporal_column(const ObRawExpr *part_expr,
                                                               bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (lib::is_oracle_mode()) {
    // do nothing
  } else {
    switch (part_expr->get_expr_type()) {
      case T_FUN_SYS_TO_DAYS:
      case T_FUN_SYS_TO_SECONDS:
      case T_FUN_SYS_YEAR: {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
          const ObRawExpr *sub_expr = part_expr->get_param_expr(i);
          const ObRawExpr *real_sub_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(sub_expr, real_sub_expr))) {
            LOG_WARN("fail to get real expr", K(ret));
          } else if (real_sub_expr->is_column_ref_expr() &&
                     (ObDateType == real_sub_expr->get_result_type().get_type() ||
                     ObDateTimeType == real_sub_expr->get_result_type().get_type())) {
            is_valid = true;
          }
        }
        break;
      }
      case T_FUN_SYS_UNIX_TIMESTAMP: {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
          const ObRawExpr *sub_expr = part_expr->get_param_expr(i);
          const ObRawExpr *real_sub_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(sub_expr, real_sub_expr))) {
            LOG_WARN("fail to get real expr", K(ret));
          } else if (real_sub_expr->is_column_ref_expr() &&
                     ObTimestampType == real_sub_expr->get_result_type().get_type()) {
            is_valid = true;
          }
        }
        break;
      }
      default: {
        is_valid = false;
      }
    }
  }
  return ret;
}

int ObTableLocation::calc_range_by_part_expr(ObExecContext &exec_ctx,
                                             const ObIArray<ObNewRange*> &ranges,
                                             const ObIArray<ObObjectID> *part_ids,
                                             ObIAllocator &allocator,
                                             ObIArray<ObNewRange*> &new_ranges,
                                             bool &is_all_single_value_ranges) const
{
  int ret = OB_SUCCESS;
  ObTempExpr *se_expr = NULL;
  se_expr = NULL == part_ids ? se_part_expr_ : se_subpart_expr_;
  is_all_single_value_ranges = true;
  for (int64 i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    ObNewRange *range = ranges.at(i);
    ObNewRange *new_range = NULL;
    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid range", K(ret));
    } else if (OB_ISNULL(new_range = static_cast<ObNewRange *>(allocator.alloc(sizeof(ObNewRange))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      new(new_range) ObNewRange();
      new_range->table_id_ = range->table_id_;
      // the range after calc is always close even though the original range is open
      new_range->border_flag_.set_inclusive_start();
      new_range->border_flag_.set_inclusive_end();
      if (range->start_key_.is_min_row()) {
        new_range->start_key_.set_min_row();
      } else {
        ObNewRow input_row;
        ObObj *func_result = NULL;
        input_row.cells_ = const_cast<ObObj *>(range->start_key_.get_obj_ptr());
        input_row.count_ = (range->start_key_.get_obj_cnt());
        OV (OB_NOT_NULL(func_result = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj)))),
            OB_ALLOCATE_MEMORY_FAILED);
        OZ(se_expr->eval(exec_ctx, input_row, *func_result), input_row, loc_meta_);
        OX(new_range->start_key_.assign(func_result, 1));
      }
      if (OB_FAIL(ret)) {
      } else if (range->end_key_.is_max_row()) {
        new_range->end_key_.set_max_row();
      } else {
        ObNewRow input_row;
        ObObj *func_result = NULL;
        input_row.cells_ = const_cast<ObObj *>(range->end_key_.get_obj_ptr());
        input_row.count_ = (range->end_key_.get_obj_cnt());
        OV (OB_NOT_NULL(func_result = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj)))),
            OB_ALLOCATE_MEMORY_FAILED);
        OZ(se_expr->eval(exec_ctx, input_row, *func_result), input_row, loc_meta_);
        OX(new_range->end_key_.assign(func_result, 1));
      }
      OZ(new_ranges.push_back(new_range));
      if (OB_SUCC(ret) && is_all_single_value_ranges) {
        is_all_single_value_ranges = is_all_single_value_ranges && new_range->is_single_rowkey();
      }
    }
  }
  return ret;
}

int ObTableLocation::pruning_single_partition(int64_t partition_id,
                                              ObExecContext &exec_ctx,
                                              bool &pruning,
                                              common::ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  UNUSED(partition_id);
  UNUSED(exec_ctx);
  UNUSED(partition_ids);
  pruning = false;
 // ObTaskExecutorCtx &task_exec_ctx = exec_ctx.get_task_exec_ctx();
 // ObMultiVersionSchemaService *schema_service = NULL;
 // ObPhysicalPlanCtx *plan_ctx = NULL;
 // ObSchemaGetterGuard schema_guard;
 // partition_ids.reset();
 // if (OB_ISNULL(exec_ctx.get_my_session())) {
 //   ret = OB_ERR_UNEXPECTED;
 //   LOG_WARN("session is null", K(ret));
 // } else if (OB_ISNULL(schema_service = task_exec_ctx.schema_service_)) {
 //   ret = OB_ERR_UNEXPECTED;
 //   LOG_WARN("schema_service is null", K(ret));
 // } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
 //             exec_ctx.get_my_session()->get_effective_tenant_id(),
 //             schema_guard))) {
 //   LOG_WARN("failed to get schema guard", K(ret));
 // } else if (OB_ISNULL(plan_ctx = exec_ctx.get_physical_plan_ctx())) {
 //   ret = OB_ERR_UNEXPECTED;
 //   LOG_WARN("plan_ctx is null", K(ret));
 // } else {
 //   ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(exec_ctx.get_my_session());
 //   if (OB_FAIL(calculate_partition_ids(exec_ctx, &schema_guard, plan_ctx->get_param_store(),
 //                                       partition_ids, dtc_params))) {
 //     LOG_WARN("fail to calculate_partition_ids", K(ret));
 //   } else {
 //     pruning = true;
 //     for (int i = 0; i < partition_ids.count() && OB_SUCC(ret); ++i) {
 //       if (partition_ids.at(i) == partition_id) {
 //         pruning = false;
 //         break;
 //       }
 //     }
 //   }
 // }
  return ret;
}

int ObTableLocation::try_split_integer_range(const common::ObIArray<common::ObNewRange*> &ranges,
                                             common::ObIAllocator &allocator,
                                             common::ObIArray<common::ObNewRange*> &new_ranges,
                                             bool &all_part) const
{
  int ret = OB_SUCCESS;
  const static int64_t MAX_INTEGER_RANGE_SPLITE_COUNT = 32;
  uint64_t table_id = OB_INVALID_ID;
  ObObjType obj_type = ObMaxType;
  ObSEArray<std::pair<int64_t, int64_t>, 2> int_ranges;
  for (int64_t i = 0; OB_SUCC(ret) && !all_part && i < ranges.count(); ++i) {
    ObNewRange *range = ranges.at(i);
    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid range", K(ret));
    } else if (OB_UNLIKELY(!range->start_key_.is_valid())
               || OB_UNLIKELY(!range->end_key_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey");
    } else if (range->start_key_.is_min_row() || range->end_key_.is_max_row()) {
      all_part = true;
    } else if (range->start_key_.get_obj_cnt() != 1 || range->end_key_.get_obj_cnt() != 1) {
      all_part = true;
    } else {
      ObObj &start_obj = range->start_key_.get_obj_ptr()[0];
      ObObj &end_obj = range->end_key_.get_obj_ptr()[0];
      table_id = range->table_id_;
      if (!start_obj.is_integer_type() || !end_obj.is_integer_type()) {
        all_part = true;
      } else {
        int64_t start_val = start_obj.get_int();
        int64_t end_val = end_obj.get_int();
        obj_type = start_obj.get_type();
        if (!range->border_flag_.inclusive_start()) {
          start_val++;
        }
        if (!range->border_flag_.inclusive_end()) {
          end_val--;
        }
        if (end_val - start_val >= MAX_INTEGER_RANGE_SPLITE_COUNT || end_val - start_val < 0) {
          all_part = true;
        } else if (OB_FAIL(int_ranges.push_back(std::pair<int64_t, int64_t>(start_val, end_val)))) {
          LOG_WARN("fail to push back integer range", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !all_part && i < int_ranges.count(); ++i) {
    int64_t start_val = int_ranges.at(i).first;
    int64_t end_val = int_ranges.at(i).second;
    for (int64_t obj_val = start_val; OB_SUCC(ret) && obj_val <= end_val; ++obj_val) {
      ObNewRange *new_range = static_cast<ObNewRange *>(allocator.alloc(sizeof(ObNewRange)));
      ObObj *obj = NULL;
      if (OB_ISNULL(new_range)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      } else if (OB_ISNULL(obj = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      } else {
        new(new_range) ObNewRange();
        obj->set_type(obj_type);
        obj->set_int_value(obj_val);
        new_range->table_id_ = table_id;
        new_range->border_flag_.set_inclusive_start();
        new_range->border_flag_.set_inclusive_end();
        new_range->start_key_.assign(obj, 1);
        new_range->end_key_.assign(obj, 1);
        OZ(new_ranges.push_back(new_range));
      }
    }
  }
  return ret;
}

int ObTableLocation::get_full_leader_table_loc(ObDASLocationRouter &loc_router,
                                               ObIAllocator &allocator,
                                               uint64_t tenant_id,
                                               uint64_t table_id,
                                               uint64_t ref_table_id,
                                               ObDASTableLoc *&table_loc)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<ObObjectID, 4> partition_ids;
  ObSEArray<ObObjectID, 4> first_level_part_ids;
  ObSchemaGetterGuard schema_guard;
  OZ(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  OZ(schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema));
  if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null", K(ret), K(table_id), K(tenant_id), K(ref_table_id));
  } else {
    OZ(table_schema->get_all_tablet_and_object_ids(tablet_ids, partition_ids, &first_level_part_ids));
    CK(table_schema->has_tablet());
    CK(tablet_ids.count() == partition_ids.count() && tablet_ids.count() == first_level_part_ids.count());
  }
  if (OB_SUCC(ret)) {
    ObDASTableLocMeta *loc_meta = NULL;
    char *table_buf = static_cast<char*>(allocator.alloc(sizeof(ObDASTableLoc)
                                         + sizeof(ObDASTableLocMeta)));
    if (OB_ISNULL(table_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    }
    if (OB_SUCC(ret)) {
      table_loc = new(table_buf) ObDASTableLoc(allocator);
      loc_meta = new(table_buf+sizeof(ObDASTableLoc)) ObDASTableLocMeta(allocator);
      loc_meta->table_loc_id_ = table_id;
      loc_meta->ref_table_id_ = ref_table_id;
      loc_meta->select_leader_ = true;
      table_loc->loc_meta_ = loc_meta;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      ObDASTabletLoc *tablet_loc = NULL;
      void *tablet_buf = allocator.alloc(sizeof(ObDASTabletLoc));
      if (OB_ISNULL(tablet_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      }
      OX(tablet_loc = new(tablet_buf) ObDASTabletLoc());
      OX(tablet_loc->loc_meta_ = loc_meta);
      OX(tablet_loc->partition_id_ = partition_ids.at(i));
      OX(tablet_loc->first_level_part_id_ = first_level_part_ids.at(i));
      OZ(loc_router.nonblock_get_leader(tenant_id, tablet_ids.at(i), *tablet_loc));
      OZ(table_loc->add_tablet_loc(tablet_loc));
    }
  }
  return ret;
}

int ObTableLocation::get_tablet_and_object_id_with_phy_rowid(ObNewRange &range,
                                                             ObDASTabletMapper &tablet_mapper,
                                                             ObIArray<ObTabletID> &tablet_ids,
                                                             ObIArray<ObObjectID> &partition_ids) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(table_schema = tablet_mapper.get_table_schema()) ||
      OB_UNLIKELY(!range.is_physical_rowid_range_ || range.start_key_.get_obj_cnt() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(range), K(range.start_key_.get_obj_cnt()),
                                     K(range.is_physical_rowid_range_), K(table_schema),
                                     K(range.start_key_.get_obj_ptr()));
  } else if (range.start_key_.get_obj_ptr()[0].is_null()) {
    //do nothing
  } else if (OB_UNLIKELY(!range.start_key_.get_obj_ptr()[0].get_urowid().is_physical_rowid())) {
    ret = OB_INVALID_ROWID;
    LOG_WARN("get invalid rowid", K(range.start_key_.get_obj_ptr()[0].get_urowid()), K(ret));
  } else {
    ObTabletID tablet_id;
    ObSEArray<ObTabletID, 4> all_tablet_ids;
    ObSEArray<ObTabletID, 4> all_index_tablet_ids;
    ObSEArray<ObObjectID, 4> all_partition_ids;
    ObSEArray<ObObjectID, 4> all_first_level_part_ids;
    ObSEArray<ObObjectID, 4> all_index_partition_ids;
    ObSEArray<ObObjectID, 4> all_index_first_level_part_ids;
    bool is_index_table = table_schema->is_index_table();
    ObURowIDData urowid_data = range.start_key_.get_obj_ptr()[0].get_urowid();
    if (OB_FAIL(urowid_data.get_tablet_id_for_heap_organized_table(tablet_id))) {
      LOG_WARN("failed to get tablet id from rowid", K(ret));
    } else if (is_index_table) {
      const ObTableSchema *data_table_schema = NULL;
      ObSchemaGetterGuard *guard = tablet_mapper.get_related_table_info().guard_;
      if (OB_ISNULL(guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(guard), K(ret));
      } else if (OB_FAIL(guard->get_table_schema(table_schema->get_tenant_id(),
                                                 table_schema->get_data_table_id(),
                                                 data_table_schema))) {
        LOG_WARN("fail to get simple table schema", KR(ret), K(table_schema->get_data_table_id()));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(table_schema->get_data_table_id()));
      } else if (OB_FAIL(data_table_schema->get_all_tablet_and_object_ids(all_tablet_ids,
                                                                          all_partition_ids,
                                                                          &all_first_level_part_ids))) {
        LOG_WARN("failed to get all tablet and object ids", K(ret));
      } else if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(all_index_tablet_ids,
                                                                     all_index_partition_ids,
                                                                     &all_index_first_level_part_ids))) {
        LOG_WARN("failed to get all tablet and object ids", K(ret));
      } else {/*do nothing*/}
    } else if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(all_tablet_ids,
                                                                   all_partition_ids,
                                                                   &all_first_level_part_ids))) {
      LOG_WARN("failed to get all tablet and object ids", K(ret));
    } else {/*do nothing*/}
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(all_tablet_ids.count() != all_partition_ids.count() ||
                           all_tablet_ids.count() != all_first_level_part_ids.count() ||
                           all_index_tablet_ids.count() != all_index_partition_ids.count() ||
                           all_index_tablet_ids.count() != all_index_first_level_part_ids.count() ||
                           (is_index_table && all_tablet_ids.count() != all_index_tablet_ids.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(all_tablet_ids), K(all_partition_ids),
                            K(is_index_table), K(all_index_tablet_ids), K(all_index_partition_ids));
    } else {
      bool find_it = false;
      for (int64_t i = 0; OB_SUCC(ret) && !find_it && i < all_tablet_ids.count(); ++i) {
        if (tablet_id == all_tablet_ids.at(i)) {
          ObObjectID partition_id = !is_index_table ? all_partition_ids.at(i) : all_index_partition_ids.at(i);
          ObObjectID first_level_part_id = !is_index_table ? all_first_level_part_ids.at(i) : all_index_first_level_part_ids.at(i);
          if (OB_FAIL(tablet_ids.push_back(!is_index_table ? all_tablet_ids.at(i) : all_index_tablet_ids.at(i)))) {
            LOG_WARN("failed to add tablet id", K(ret), K(all_tablet_ids.at(i)));
          } else if (OB_FAIL(partition_ids.push_back(partition_id))) {
            LOG_WARN("failed to push back", K(ret), K(all_partition_ids.at(i)));
          } else if (PARTITION_LEVEL_TWO == part_level_ &&
                     OB_FAIL(tablet_mapper.set_partition_id_map(first_level_part_id, partition_id))) {
            LOG_WARN("failed to set partition id map", K(first_level_part_id), K(partition_id));
          } else if (OB_FAIL(fill_related_tablet_and_object_ids(tablet_mapper,
                                                                table_schema->get_tenant_id(),
                                                                !is_index_table ? all_tablet_ids.at(i) : all_index_tablet_ids.at(i),
                                                                i))) {
            LOG_WARN("failed to fill related tablet and object ids", K(ret));
          } else {
            find_it = true;
          }
        }
      }
    }
    LOG_TRACE("get tablet and object id with phy rowid", K(range), K(tablet_id), K(all_tablet_ids),
       K(all_partition_ids), K(all_index_tablet_ids), K(all_index_partition_ids), K(is_index_table),
       K(tablet_ids), K(partition_ids));
  }
  return ret;
}

int ObTableLocation::fill_related_tablet_and_object_ids(ObDASTabletMapper &tablet_mapper,
                                                        const uint64_t tenant_id,
                                                        ObTabletID src_tablet_id,
                                                        const int64_t idx) const
{
  int ret = OB_SUCCESS;
  share::schema::RelatedTableInfo &related_table = tablet_mapper.get_related_table_info();
  if (related_table.related_tids_ != NULL && !related_table.related_tids_->empty()) {
    ObSchemaGetterGuard *guard = related_table.guard_;
    if (OB_ISNULL(guard) || OB_ISNULL(related_table.related_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(guard), K(related_table.related_map_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < related_table.related_tids_->count(); i++) {
        const uint64_t related_table_id = related_table.related_tids_->at(i);
        ObSEArray<ObTabletID, 4> all_related_tablet_ids;
        ObSEArray<ObObjectID, 4> all_partition_ids;
        ObSEArray<ObObjectID, 4> all_first_level_part_ids;
        const ObTableSchema *related_schema = NULL;
        if (OB_FAIL(guard->get_table_schema(tenant_id, related_table_id, related_schema))) {
          LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id), K(related_table_id));
        } else if (OB_ISNULL(related_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(tenant_id), K(related_table_id));
        } else if (OB_FAIL(related_schema->get_all_tablet_and_object_ids(all_related_tablet_ids,
                                                                         all_partition_ids,
                                                                         &all_first_level_part_ids))) {
          LOG_WARN("failed to get all tablet and object ids", K(ret));
        } else if (OB_UNLIKELY(idx < 0 || idx >= all_related_tablet_ids.count() ||
                               all_related_tablet_ids.count() != all_partition_ids.count() ||
                               all_related_tablet_ids.count() != all_first_level_part_ids.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(idx), K(all_related_tablet_ids),
                                           K(all_partition_ids), K(all_first_level_part_ids));
        } else if (OB_FAIL(related_table.related_map_->add_related_tablet_id(src_tablet_id,
                                                                             related_table_id,
                                                                             all_related_tablet_ids.at(idx),
                                                                             all_partition_ids.at(idx),
                                                                             all_first_level_part_ids.at(idx)))) {
          LOG_WARN("fail to add related tablet info", K(ret), K(src_tablet_id), K(related_table_id),
                              K(all_related_tablet_ids.at(idx)), K(all_partition_ids.at(idx)), K(all_first_level_part_ids.at(idx)));
        } else {
          LOG_TRACE("succ to add related tablet info", K(ret), K(src_tablet_id), K(related_table_id),
                              K(all_related_tablet_ids.at(idx)), K(all_partition_ids.at(idx)), K(all_first_level_part_ids.at(idx)));
        }
      }
    }
  }
  return ret;
}
