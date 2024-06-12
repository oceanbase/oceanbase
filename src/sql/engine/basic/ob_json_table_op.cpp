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
 * This file contains implementation support for the json table abstraction.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_json_table_op.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_json_access_utils.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_json_value.h"
#include "sql/engine/expr/ob_expr_json_query.h"
#include "sql/engine/expr/ob_expr_json_exists.h"
#include "lib/xml/ob_binary_aggregate.h"
#include "lib/xml/ob_xpath.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

/* json value  mismatch type { MISSING : 0, EXTRA : 1, TYPE : 2, EMPTY : 3} */
const static int32_t JSN_VALUE_TYPE_MISSING_DATA    = 0;
const static int32_t JSN_VALUE_TYPE_EXTRA_DATA      = 1;
const static int32_t JSN_VALUE_TYPE_TYPE_ERROR      = 2;
const static int32_t JSN_VALUE_TYPE_IMPLICIT        = 3;

/* json exists */
const static int32_t JSN_EXIST_FALSE = 0;
const static int32_t JSN_EXIST_TRUE  = 1;
const static int32_t JSN_EXIST_ERROR = 2;
const static int32_t JSN_EXIST_DEFAULT = 3;

#define SET_COVER_ERROR(jt_ctx_ptr, error_code) \
{\
  if (!(jt_ctx_ptr)->is_cover_error_) { \
    (jt_ctx_ptr)->is_cover_error_ = true; \
    (jt_ctx_ptr)->error_code_ = error_code; \
  } \
}

#define EVAL_COVER_CODE(jt_ctx_ptr, error_code) \
{\
  if ((jt_ctx_ptr)->is_cover_error_) { \
    error_code = (jt_ctx_ptr)->error_code_; \
  } \
}

#define RESET_COVER_CODE(jt_ctx_ptr) \
{\
  if ((jt_ctx_ptr)->is_cover_error_) { \
    (jt_ctx_ptr)->is_cover_error_ = false; \
    (jt_ctx_ptr)->error_code_ = 0; \
  } \
}

ObJtColInfo::ObJtColInfo()
  : col_type_(0),
    truncate_(0),
    format_json_(0),
    wrapper_(0),
    allow_scalar_(0),
    output_column_idx_(-1),
    empty_expr_id_(-1),
    error_expr_id_(-1),
    col_name_(),
    path_(),
    on_empty_(3),
    on_error_(3),
    on_mismatch_(3),
    on_mismatch_type_(3),
    data_type_(),
    parent_id_(-1),
    id_(-1) {}

ObJtColInfo::ObJtColInfo(const ObJtColInfo& info)
  : col_type_(info.col_type_),
    truncate_(info.truncate_),
    format_json_(info.format_json_),
    wrapper_(info.wrapper_),
    allow_scalar_(info.allow_scalar_),
    output_column_idx_(info.output_column_idx_),
    empty_expr_id_(info.empty_expr_id_),
    error_expr_id_(info.error_expr_id_),
    col_name_(info.col_name_),
    path_(info.path_),
    on_empty_(info.on_empty_),
    on_error_(info.on_error_),
    on_mismatch_(info.on_mismatch_),
    on_mismatch_type_(info.on_mismatch_type_),
    data_type_(info.data_type_),
    parent_id_(info.parent_id_),
    id_(info.id_) {}


int ObJtColInfo::deep_copy(const ObJtColInfo& src, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (src.col_name_.length() > 0) {
    void *name_buf = allocator->alloc(src.col_name_.length());
    if (OB_ISNULL(name_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(name_buf, src.col_name_.ptr(), src.col_name_.length());
      col_name_.assign(static_cast<char*>(name_buf), src.col_name_.length());
    }
  }

  if (OB_SUCC(ret) && src.path_.length() > 0) {
    void *path_buf = allocator->alloc(src.path_.length());
    if (OB_ISNULL(path_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(path_buf, src.path_.ptr(), src.path_.length());
      path_.assign(static_cast<char*>(path_buf), src.path_.length());
    }
  }

  if (OB_SUCC(ret)) {
    col_type_ = src.col_type_;
    truncate_ = src.truncate_;
    format_json_ = src.format_json_;
    wrapper_ = src.wrapper_;
    allow_scalar_ = src.allow_scalar_;
    output_column_idx_ = src.output_column_idx_;
    empty_expr_id_ = src.empty_expr_id_;
    error_expr_id_ = src.error_expr_id_;
    on_empty_ = src.on_empty_;
    on_error_ = src.on_error_;
    on_mismatch_ = src.on_mismatch_;
    on_mismatch_type_ = src.on_mismatch_type_;
    data_type_ = src.data_type_;
    parent_id_ = src.parent_id_;
    id_ = src.id_;
  }
  return ret;
}

int ObJtColInfo::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  INIT_SUCC(ret);
  OB_UNIS_ENCODE(col_type_);
  OB_UNIS_ENCODE(truncate_);
  OB_UNIS_ENCODE(format_json_);
  OB_UNIS_ENCODE(wrapper_);
  OB_UNIS_ENCODE(allow_scalar_);
  OB_UNIS_ENCODE(output_column_idx_);
  OB_UNIS_ENCODE(empty_expr_id_);
  OB_UNIS_ENCODE(error_expr_id_);
  OB_UNIS_ENCODE(col_name_);
  OB_UNIS_ENCODE(path_);
  OB_UNIS_ENCODE(on_empty_);
  OB_UNIS_ENCODE(on_error_);
  OB_UNIS_ENCODE(on_mismatch_);
  OB_UNIS_ENCODE(on_mismatch_type_);
  OB_UNIS_ENCODE(data_type_);
  OB_UNIS_ENCODE(parent_id_);
  OB_UNIS_ENCODE(id_);

  return ret;
}

int ObJtColInfo::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  INIT_SUCC(ret);
  OB_UNIS_DECODE(col_type_);
  OB_UNIS_DECODE(truncate_);
  OB_UNIS_DECODE(format_json_);
  OB_UNIS_DECODE(wrapper_);
  OB_UNIS_DECODE(allow_scalar_);
  OB_UNIS_DECODE(output_column_idx_);
  OB_UNIS_DECODE(empty_expr_id_);
  OB_UNIS_DECODE(error_expr_id_);
  OB_UNIS_DECODE(col_name_);
  OB_UNIS_DECODE(path_);
  OB_UNIS_DECODE(on_empty_);
  OB_UNIS_DECODE(on_error_);
  OB_UNIS_DECODE(on_mismatch_);
  OB_UNIS_DECODE(on_mismatch_type_);
  OB_UNIS_DECODE(data_type_);
  OB_UNIS_DECODE(parent_id_);
  OB_UNIS_DECODE(id_);
  return ret;
}

int64_t ObJtColInfo::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(col_type_);
  OB_UNIS_ADD_LEN(truncate_);
  OB_UNIS_ADD_LEN(format_json_);
  OB_UNIS_ADD_LEN(wrapper_);
  OB_UNIS_ADD_LEN(allow_scalar_);
  OB_UNIS_ADD_LEN(output_column_idx_);
  OB_UNIS_ADD_LEN(empty_expr_id_);
  OB_UNIS_ADD_LEN(error_expr_id_);
  OB_UNIS_ADD_LEN(col_name_);
  OB_UNIS_ADD_LEN(path_);
  OB_UNIS_ADD_LEN(on_empty_);
  OB_UNIS_ADD_LEN(on_error_);
  OB_UNIS_ADD_LEN(on_mismatch_);
  OB_UNIS_ADD_LEN(on_mismatch_type_);
  OB_UNIS_ADD_LEN(data_type_);
  OB_UNIS_ADD_LEN(parent_id_);
  OB_UNIS_ADD_LEN(id_);

  return len;
}

int ObJtColInfo::from_JtColBaseInfo(const ObJtColBaseInfo& info)
{
  INIT_SUCC(ret);
  col_type_ = info.col_type_;
  truncate_ = info.truncate_;
  format_json_ = info.format_json_;
  wrapper_ = info.wrapper_;
  allow_scalar_ = info.allow_scalar_;
  output_column_idx_ = info.output_column_idx_;
  empty_expr_id_ = info.empty_expr_id_;
  error_expr_id_ = info.error_expr_id_;
  col_name_ = info.col_name_;
  path_ = info.path_;
  on_empty_ = info.on_empty_;
  on_error_ = info.on_error_;
  on_mismatch_ = info.on_mismatch_;
  on_mismatch_type_ = info.on_mismatch_type_;
  data_type_ = info.data_type_;
  parent_id_ = info.parent_id_;
  id_ = info.id_;

  return ret;
}

static int construct_table_func_join_node(ObIAllocator* allocator,
                                          const ObJtColInfo& col_info,
                                          JoinNode*& jt_node)
{
  INIT_SUCC(ret);
  void* node_buf = static_cast<void*>(jt_node);
  if (OB_ISNULL(node_buf)) {
    node_buf = allocator->alloc(sizeof(JoinNode));
    if (OB_ISNULL(node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc col node buffer", K(ret));
    }
    jt_node = static_cast<JoinNode*>(new(node_buf)JoinNode());
  } else {
    jt_node = static_cast<JoinNode*>(new(node_buf)JoinNode());
  }
  return ret;
}

static int construct_table_func_reg_node(ObIAllocator* allocator,
                                        const ObJtColInfo& col_info,
                                        ObRegCol*& jt_node)
{
  INIT_SUCC(ret);
  void* node_buf = allocator->alloc(sizeof(ObRegCol));
  if (OB_ISNULL(node_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc col node buffer", K(ret));
  } else {
    jt_node = static_cast<ObRegCol*>(new(node_buf)ObRegCol(col_info));
  }
  return ret;
}

static int construct_table_func_union_node(ObIAllocator* allocator,
                                          const ObJtColInfo& col_info,
                                          UnionNode*& jt_node)
{
  INIT_SUCC(ret);
  void* node_buf = allocator->alloc(sizeof(UnionNode));
  if (OB_ISNULL(node_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc col node buffer", K(ret));
  } else {
    jt_node = static_cast<UnionNode*>(new(node_buf)UnionNode());
  }
  return ret;
}

static int construct_table_func_scan_node(ObIAllocator* allocator,
                                          const ObJtColInfo& col_info,
                                          ScanNode*& jt_node)
{
  INIT_SUCC(ret);
  void* node_buf = allocator->alloc(sizeof(ScanNode));
  if (OB_ISNULL(node_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc col node buffer", K(ret));
  } else {
    jt_node = static_cast<ScanNode*>(new(node_buf)ScanNode(col_info));
  }
  return ret;
}

void JtColTreeNode::destroy()
{
  regular_cols_.reset();
  for (size_t i = 0; i < nested_cols_.count(); ++i) {
    JtColTreeNode* tmp_node = nested_cols_.at(i);
    tmp_node->destroy();
  }
  nested_cols_.reset();
}

OB_DEF_SERIALIZE(ObJsonTableSpec)
{
  INIT_SUCC(ret);
  BASE_SER((ObJsonTableSpec, ObOpSpec));
  OB_UNIS_ENCODE(value_expr_);
  OB_UNIS_ENCODE(column_exprs_);
  OB_UNIS_ENCODE(emp_default_exprs_);
  OB_UNIS_ENCODE(err_default_exprs_);
  OB_UNIS_ENCODE(has_correlated_expr_);
  int32_t column_count = cols_def_.count();
  OB_UNIS_ENCODE(column_count);
  for (size_t i = 0; OB_SUCC(ret) && i < cols_def_.count(); ++i) {
    const ObJtColInfo& info = *cols_def_.at(i);
    OB_UNIS_ENCODE(info);
  }
  if (table_type_ == MulModeTableType::OB_ORA_XML_TABLE_TYPE) {
    OB_UNIS_ENCODE(table_type_);
    OB_UNIS_ENCODE(namespace_def_);
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObJsonTableSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObJsonTableSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(value_expr_);
  OB_UNIS_ADD_LEN(column_exprs_);
  OB_UNIS_ADD_LEN(emp_default_exprs_);
  OB_UNIS_ADD_LEN(err_default_exprs_);
  OB_UNIS_ADD_LEN(has_correlated_expr_);

  int32_t column_count = cols_def_.count();
  OB_UNIS_ADD_LEN(column_count);
  for (size_t i = 0; i < cols_def_.count(); ++i) {
    const ObJtColInfo& info = *cols_def_.at(i);
    OB_UNIS_ADD_LEN(info);
  }
  if (table_type_ == MulModeTableType::OB_ORA_XML_TABLE_TYPE) {
    OB_UNIS_ADD_LEN(table_type_);
    OB_UNIS_ADD_LEN(namespace_def_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObJsonTableSpec)
{
  INIT_SUCC(ret);
  BASE_DESER((ObJsonTableSpec, ObOpSpec));
  OB_UNIS_DECODE(value_expr_);
  OB_UNIS_DECODE(column_exprs_);
  OB_UNIS_DECODE(emp_default_exprs_);
  OB_UNIS_DECODE(err_default_exprs_);
  OB_UNIS_DECODE(has_correlated_expr_);

  int32_t column_count = 0;
  int8_t table_type_flag = OB_JSON_TABLE;
  OB_UNIS_DECODE(column_count);

  if (OB_SUCC(ret) && OB_FAIL(cols_def_.init(column_count))) {
    LOG_WARN("fail to init cols def array.", K(ret), K(column_count));
  }

  for (size_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    void* col_info_buf = alloc_->alloc(sizeof(ObJtColInfo));
    if (OB_ISNULL(col_info_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate col node buffer.", K(ret));
    } else {
      ObJtColInfo* col_info = static_cast<ObJtColInfo*>(new (col_info_buf) ObJtColInfo());
      ObJtColInfo& tmp_col_info = *col_info;
      OB_UNIS_DECODE(tmp_col_info);
      *col_info = tmp_col_info;
      if (OB_FAIL(cols_def_.push_back(col_info))) {
        LOG_WARN("fail to store col info.", K(ret), K(cols_def_.count()));
      } else if (col_info->col_type_ >= COL_TYPE_VAL_EXTRACT_XML) {
        table_type_flag = OB_XML_TABLE;
      }
    }
  }
  if (table_type_flag == OB_XML_TABLE) {
    OB_UNIS_DECODE(table_type_);
    OB_UNIS_DECODE(namespace_def_);
  }
  return ret;
}

int ObJsonTableOp::generate_table_exec_tree(ObIAllocator* allocator,
                                            const JtColTreeNode& orig_col,
                                            JoinNode*& join_col)
{
  INIT_SUCC(ret);

  int reg_count = orig_col.regular_cols_.count();
  int nest_count = orig_col.nested_cols_.count();
  ScanNode* scan_col = nullptr;

  if (OB_FAIL(construct_table_func_join_node(allocator, orig_col.col_base_info_, join_col))) {
    LOG_WARN("fail to construct join col node", K(ret));
  } else if (OB_FAIL(construct_table_func_scan_node(allocator, orig_col.col_base_info_, scan_col))) {
    LOG_WARN("fail to construct scan col node", K(ret));
  } else {
    join_col->set_left(scan_col);
  }

  for (int i = 0; OB_SUCC(ret) && i < reg_count; ++i) {
    ObRegCol* reg_node = nullptr;
    if (OB_FAIL(construct_table_func_reg_node(allocator, orig_col.regular_cols_.at(i)->col_base_info_, reg_node))) {
      LOG_WARN("fail to construct reg col node", K(ret), K(reg_count), K(i));
    } else {
      if (OB_FAIL(scan_col->add_reg_column_node(reg_node))) {
        LOG_WARN("fail to store col node", K(ret), K(reg_count), K(i));
      }
    }
  }

  if (OB_SUCC(ret) && nest_count > 0) {
    common::ObArray<UnionNode*> ji_nodes;
    for (size_t i = 0; OB_SUCC(ret) && i < nest_count; ++i) {
      UnionNode* ji_node = nullptr;
      if (OB_FAIL(construct_table_func_union_node(allocator, orig_col.nested_cols_.at(i)->col_base_info_, ji_node))) {
        LOG_WARN("fail to construct join col node", K(ret));
      } else if (OB_FAIL(ji_nodes.push_back(ji_node))) {
        LOG_WARN("fail to store ji nodes in tmp array", K(ret), K(nest_count), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      int j = 0;
      UnionNode* last_node = nullptr;
      join_col->set_right(ji_nodes.at(j));
      last_node = ji_nodes.at(j);
      ++j;

      while (j < nest_count) { // nested col/union node in right child
        UnionNode* cur_node = ji_nodes.at(j);
        last_node->set_right(cur_node);
        last_node = cur_node;
        ++j;
      };

      for (int i = 0; OB_SUCC(ret) && i < nest_count; ++i) {
        JoinNode* col_node = nullptr;
        if (OB_FAIL(generate_table_exec_tree(allocator, *orig_col.nested_cols_.at(i), col_node))) {
          LOG_WARN("fail to generate sub col node", K(ret), K(i));
        } else {
          ji_nodes.at(i)->set_left(col_node);
        }
      }
    }
  }

  return ret;
}

int ObJsonTableOp::generate_table_exec_tree()
{
  INIT_SUCC(ret);
  if (OB_FAIL(generate_column_trees(def_root_))) {
    LOG_WARN("fail to generate column tree", K(ret));
  } else if (OB_FAIL(generate_table_exec_tree(allocator_, *def_root_, root_))) {
    LOG_WARN("fail to generate sub col node", K(ret));
  }
  return ret;
}

int ObJsonTableSpec::dup_origin_column_defs(ObIArray<ObJtColBaseInfo*>& columns)
{
  INIT_SUCC(ret);

  for (size_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    void* col_info_buf = alloc_->alloc(sizeof(ObJtColInfo));
    if (OB_ISNULL(col_info_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate col node buffer.", K(ret));
    } else {
      ObJtColInfo col_info;
      if (OB_FAIL(col_info.from_JtColBaseInfo(*columns.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to transform to jtcolinfo", K(ret));
      } else {
        ObJtColInfo* col = static_cast<ObJtColInfo*>(new (col_info_buf) ObJtColInfo());
        if (OB_FAIL(col->deep_copy(col_info, alloc_))) {
          LOG_WARN("fail to deep copy col node", K(ret));
        } else if (OB_FAIL(cols_def_.push_back(col))) {
          LOG_WARN("fail to store col node", K(cols_def_.count()), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObJsonTableOp::find_column(int32_t id, JtColTreeNode* root, JtColTreeNode*& col)
{
  INIT_SUCC(ret);
  common::ObArray<JtColTreeNode*> col_stack;
  if (OB_FAIL(col_stack.push_back(root))) {
    LOG_WARN("fail to store col node tmp", K(ret));
  }

  bool exists = false;

  while (OB_SUCC(ret) && !exists && col_stack.count() > 0) {
    JtColTreeNode* cur_col = col_stack.at(col_stack.count() - 1);
    if (cur_col->col_base_info_.id_ == id) {
      exists = true;
      col = cur_col;
    } else if (cur_col->col_base_info_.parent_id_ < 0
               || cur_col->col_base_info_.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)) {
      col_stack.remove(col_stack.count() - 1);
      for (size_t i = 0; !exists && i < cur_col->nested_cols_.count(); ++i) {
        JtColTreeNode* nest_col = cur_col->nested_cols_.at(i);
        if (nest_col->col_base_info_.id_ == id) {
          exists = true;
          col = nest_col;
        } else if (nest_col->col_base_info_.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)
                  && OB_FAIL(col_stack.push_back(nest_col))) {
          LOG_WARN("fail to store col node tmp", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !exists) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find col node", K(ret));
  }
  return ret;
}

int ObJsonTableOp::generate_column_trees(JtColTreeNode*& root)
{
  INIT_SUCC(ret);

  const ObJsonTableSpec* spec_ptr = reinterpret_cast<const ObJsonTableSpec*>(&spec_);
  const ObIArray<ObJtColInfo*>& plain_def = spec_ptr->cols_def_;

  for (size_t i = 0; OB_SUCC(ret) && i < plain_def.count(); ++i) {
    const ObJtColInfo& info = *plain_def.at(i);
    JtColTreeNode* col_def = static_cast<JtColTreeNode*>(allocator_->alloc(sizeof(JtColTreeNode)));
    if (OB_ISNULL(col_def)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate col node", K(ret));
    } else {
      col_def = new (col_def) JtColTreeNode(info);
    }

    if (OB_FAIL(ret)) {
    } else {
      JtColTreeNode* parent = nullptr;
      if (info.parent_id_ < 0) {
        root = col_def;
      } else if (OB_FAIL(find_column(info.parent_id_, root, parent))) {
        LOG_WARN("fail to find col node parent", K(ret), K(info.parent_id_));
      } else if (info.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)) {
        if (OB_FAIL(parent->nested_cols_.push_back(col_def))) {
          LOG_WARN("fail to store col node", K(ret), K(parent->nested_cols_.count()));
        }
      } else if (OB_FAIL(parent->regular_cols_.push_back(col_def))) {
        LOG_WARN("fail to store col node", K(ret), K(parent->nested_cols_.count()));
      }
    }
  }

  return ret;
}

int ObJsonTableOp::inner_open()
{
  INIT_SUCC(ret);
  if (OB_FAIL(init())) {
    LOG_WARN("failed to init.", K(ret));
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open iter, value expr is null.", K(ret));
  } else if (OB_FAIL(root_->open(&jt_ctx_))) {
    LOG_WARN("failed to open table func xml column node.", K(ret));
  } else {
    is_evaled_ = false;
  }

  return ret;
}

int ObJsonTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to inner rescan", K(ret));
  } else {
    jt_ctx_.row_alloc_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reset_variable())) {
    LOG_WARN("failed to inner open", K(ret));
  }
  return ret;
}

int ObJsonTableOp::reset_variable()
{
  INIT_SUCC(ret);
  jt_ctx_.is_cover_error_ = false;
  jt_ctx_.error_code_ = 0;
  jt_ctx_.is_need_end_ = 0;

  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&jt_ctx_.row_alloc_, jt_ctx_.mem_ctx_))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open iter, value expr is null.", K(ret));
  } else if (OB_FAIL(root_->reset(&jt_ctx_))) {
    LOG_WARN("failed to open table func xml column node.", K(ret));
  } else {
    is_evaled_ = false;
  }

  return ret;
}

int ObJsonTableOp::switch_iterator()
{
  INIT_SUCC(ret);
  return OB_ITER_END;
}

int ObJsonTableOp::init()
{
  INIT_SUCC(ret);
  if (!is_inited_) {
    const ObJsonTableSpec* spec_ptr = reinterpret_cast<const ObJsonTableSpec*>(&spec_);
    jt_ctx_.spec_ptr_ = const_cast<ObJsonTableSpec*>(spec_ptr);
    if (OB_FAIL(generate_table_exec_tree())) {
      LOG_WARN("fail to init json table op, as generate exec tree occur error.", K(ret));
    } else {
      const sql::ObSQLSessionInfo *session = get_exec_ctx().get_my_session();
      uint64_t tenant_id = -1;
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        tenant_id = session->get_effective_tenant_id();
        is_inited_ = true;
        jt_ctx_.spec_ptr_ = const_cast<ObJsonTableSpec*>(spec_ptr);
        jt_ctx_.eval_ctx_ = &eval_ctx_;
        jt_ctx_.exec_ctx_ = &get_exec_ctx();
        jt_ctx_.row_alloc_.set_tenant_id(tenant_id);
        jt_ctx_.op_exec_alloc_ = allocator_;
        jt_ctx_.is_evaled_ = false;
        jt_ctx_.is_charset_converted_ = false;
        jt_ctx_.res_obj_ = nullptr;
        jt_ctx_.jt_op_ = this;
        jt_ctx_.is_const_input_ = !MY_SPEC.has_correlated_expr_;
      }
    }
    void* table_func_buf = NULL;
    if (OB_FAIL(ret)) {
    } else if (jt_ctx_.is_json_table_func()) {
      table_func_buf = jt_ctx_.op_exec_alloc_->alloc(sizeof(JsonTableFunc));
      if (OB_ISNULL(table_func_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate table func buf", K(ret));
      } else if (OB_ISNULL(jt_ctx_.table_func_ = new (table_func_buf) JsonTableFunc())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new json array node", K(ret));
      }
    } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(jt_ctx_.op_exec_alloc_, jt_ctx_.xpath_ctx_))) {
      LOG_WARN("fail to create xpath memory context", K(ret));
    } else if (jt_ctx_.is_xml_table_func()) {
      table_func_buf = jt_ctx_.op_exec_alloc_->alloc(sizeof(XmlTableFunc));
      if (OB_ISNULL(table_func_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate table func buf", K(ret));
      } else if (OB_ISNULL(jt_ctx_.table_func_ = new (table_func_buf) XmlTableFunc())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new json array node", K(ret));
      }
    }
  }
  jt_ctx_.is_cover_error_ = false;
  jt_ctx_.error_code_ = 0;
  jt_ctx_.is_need_end_ = 0;
  if (OB_SUCC(ret) && OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&jt_ctx_.row_alloc_, jt_ctx_.mem_ctx_))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  }
  return ret;
}

int ObJsonTableOp::inner_close()
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(root_)) {
    root_->destroy();
  }
  if (OB_NOT_NULL(def_root_)) {
    def_root_->destroy();
  }
  jt_ctx_.row_alloc_.clear();
  if (OB_NOT_NULL(jt_ctx_.table_func_) && OB_NOT_NULL(jt_ctx_.op_exec_alloc_)) {
    jt_ctx_.op_exec_alloc_->free(jt_ctx_.table_func_);
  }
  if (OB_NOT_NULL(jt_ctx_.xpath_ctx_) && OB_NOT_NULL(jt_ctx_.op_exec_alloc_)) {
    jt_ctx_.op_exec_alloc_->free(jt_ctx_.xpath_ctx_);
  }
  return ret;
}

void ObJsonTableOp::reset_columns()
{
  for (size_t i = 0; i < col_count_; ++i) {
    ObExpr* col_expr = jt_ctx_.spec_ptr_->column_exprs_.at(i);
    col_expr->locate_datum_for_write(*jt_ctx_.eval_ctx_).reset();
    col_expr->locate_datum_for_write(*jt_ctx_.eval_ctx_).set_null();
    col_expr->get_eval_info(*jt_ctx_.eval_ctx_).evaluated_ = true;
  }
}

void ScanNode::reset_reg_columns(JtScanCtx* ctx)
{
  for (size_t i = 0; i < reg_column_count(); ++i) {
    ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(reg_col_node(i)->col_info_.output_column_idx_);
    col_expr->locate_datum_for_write(*ctx->eval_ctx_).reset();
    col_expr->locate_datum_for_write(*ctx->eval_ctx_).set_null();
    col_expr->get_eval_info(*ctx->eval_ctx_).evaluated_ = true;
  }
}

void ObJsonTableOp::destroy()
{
  ObOperator::destroy();
}

int RegularCol::check_item_method_json(ObRegCol &col_node, JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  ObExpr* expr = ctx->spec_ptr_->column_exprs_.at(col_node.col_info_.output_column_idx_);
  if (col_node.type() == COL_TYPE_QUERY) {
    if (col_node.expr_param_.dst_type_ != ObVarcharType
         && col_node.expr_param_.dst_type_ != ObLongTextType
         && col_node.expr_param_.dst_type_ != ObJsonType) {
      ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
      LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
    } else if (OB_ISNULL(col_node.expr_param_.json_path_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get expr param json path is null", K(ret));
    } else if (col_node.expr_param_.json_path_->is_last_func()
                && OB_FAIL( ObJsonExprHelper::check_item_func_with_return(col_node.expr_param_.json_path_->get_last_node_type(),
                              col_node.expr_param_.dst_type_, expr->datum_meta_.cs_type_, 1))) {
      if (ret == OB_ERR_INVALID_DATA_TYPE_RETURNING) {
        ret = OB_ERR_INVALID_DATA_TYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE, "JSON_TABLE");
      }
      LOG_WARN("check item func with return type fail", K(ret));
    } else if (OB_FAIL(ObExprJsonQuery::check_item_method_valid_with_wrapper(col_node.expr_param_.json_path_,
                                                                             col_node.expr_param_.wrapper_))) {
      LOG_WARN("fail to check item method with wrapper", K(ret));
    }
  } else if (col_node.type() == COL_TYPE_VALUE) {
    if (OB_ISNULL(col_node.expr_param_.json_path_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get expr param json path is null", K(ret));
    } else if (col_node.expr_param_.json_path_->is_last_func()
        && OB_FAIL( ObJsonExprHelper::check_item_func_with_return(col_node.expr_param_.json_path_->get_last_node_type(),
                                                                  col_node.expr_param_.dst_type_, expr->datum_meta_.cs_type_, 0))) {
      if (ret == OB_ERR_INVALID_DATA_TYPE_RETURNING) {
        ret = OB_ERR_INVALID_DATA_TYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE, "JSON_TABLE");
      }
      LOG_WARN("check item func with return type fail", K(ret));
    }
  }
  return ret;
}

int RegularCol::eval_query_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null)
{
  INIT_SUCC(ret);
  ObJsonArray* in = static_cast<ObJsonArray*>(col_node.curr_);
  is_null = false;
  int8_t use_wrapper = 0;
  if (in->element_count() == 0) {
    is_null = true;
  } else if (in->element_count() == 1
              && OB_FAIL(ObExprJsonQuery::get_single_obj_wrapper(col_node.col_info_.wrapper_,
                                          use_wrapper, in[0][0]->json_type(), col_node.col_info_.allow_scalar_))) {
    SET_COVER_ERROR(ctx, ret);
    LOG_WARN("result can't be returned without array wrapper", K(ret));
  } else if (in->element_count() > 1
              && OB_FAIL(ObExprJsonQuery::get_multi_scalars_wrapper_type(col_node.col_info_.wrapper_, use_wrapper))) {
    SET_COVER_ERROR(ctx, ret);
    LOG_WARN("result can't be returned without array wrapper", K(ret));
  } else if (!use_wrapper) {
    col_node.curr_ = in[0][0];
  }
  return ret;
}

int RegularCol::eval_value_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null)
{
  INIT_SUCC(ret);
  is_null = false;
  uint8_t is_type_mismatch = 0;
  ObIJsonBase* in = static_cast<ObIJsonBase*>(col_node.curr_);

  if (ob_is_json(col_expr->datum_meta_.type_)) {
  } else if (in->json_type() == ObJsonNodeType::J_OBJECT
             || in->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_JSON_VALUE_NO_SCALAR;
    LOG_WARN("result can not be object", K(ret));
    SET_COVER_ERROR(ctx, ret);
  } else if (lib::is_oracle_mode()
      && OB_FAIL(ObExprJsonValue::deal_item_method_in_seek(in, is_null, col_node.expr_param_.json_path_,
                                                                &ctx->row_alloc_, is_type_mismatch))) {
    SET_COVER_ERROR(ctx, ret);
    LOG_WARN("fail to check res valid" , K(ret));
  } else if (lib::is_oracle_mode()
             && in->json_type() == ObJsonNodeType::J_BOOLEAN
             && ob_is_number_tc(col_node.col_info_.data_type_.get_obj_type())) {
    col_node.curr_ = nullptr;
    is_null = true;
    ret = OB_ERR_BOOL_CAST_NUMBER;
    LOG_WARN("boolean cast number cast not support");
    SET_COVER_ERROR(ctx, ret);
  } else if ((in->json_type() == ObJsonNodeType::J_INT
              || in->json_type() == ObJsonNodeType::J_INT)
            && (ob_is_datetime_tc(col_node.col_info_.data_type_.get_obj_type()))) {
    char* res_ptr = ctx->buf;
    int len = snprintf(ctx->buf, sizeof(ctx->buf), "%ld", in->get_int());
    if (len > 0) {
      ObJsonString* j_string = nullptr;
      if (OB_ISNULL(j_string = static_cast<ObJsonString*>(ctx->row_alloc_.alloc(sizeof(ObJsonString))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RESET_COVER_CODE(ctx);
        LOG_WARN("fail to allocate json string node", K(ret));
      } else {
        col_node.curr_ = new(j_string) ObJsonString(ctx->buf, len);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      RESET_COVER_CODE(ctx);
      LOG_WARN("fail to print int value", K(ret));
    }
  }
  return ret;
}
int RegularCol::eval_exist_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null)
{
  INIT_SUCC(ret);
  is_null = true;
  if (ob_is_string_type(col_node.col_info_.data_type_.get_obj_type())) {
    ObString value("true");
    if (lib::is_mysql_mode()) {
      value.assign_ptr("1", 1);
    }
    void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonString));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      col_node.curr_ = static_cast<ObJsonString*>(new(buf)ObJsonString(value.ptr(), value.length()));
      is_null = false;
    }
  } else {
    void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonInt));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("buf allocate failed", K(ret));
    } else {
      col_node.curr_ = static_cast<ObJsonInt*>(new(buf)ObJsonInt(1));
      is_null = false;
    }
  }
  return ret;
}

int RegularCol::eval_xml_scalar_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr)
{
  INIT_SUCC(ret);
  ObXmlBin *hit = static_cast<ObXmlBin*>(col_node.curr_);
  if (hit->size() > 1 && hit->type() != M_DOCUMENT) {
    ret = OB_ERR_XQUERY_MULTI_VALUE;
    SET_COVER_ERROR(ctx, ret);
  }
  return ret;
}

int RegularCol::eval_xml_type_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr)
{
  INIT_SUCC(ret);
  return ret;
}

// xmltable expr function
int XmlTableFunc::container_at(void* in, void *&out, int32_t pos)
{
  INIT_SUCC(ret);
  ObXmlBin *t_in = static_cast<ObXmlBin*>(in);
  if (pos < 0 || pos >= t_in->size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos out of range", K(ret), K(pos));
  } else {
    out = t_in->at(pos);
  }
  return ret;
}

int XmlTableFunc::eval_input(ObJsonTableOp &jt, JtScanCtx &ctx, ObEvalCtx &eval_ctx)
{
  INIT_SUCC(ret);
  common::ObObjMeta& doc_obj_datum = ctx.spec_ptr_->value_expr_->obj_meta_;
  ObDatumMeta& doc_datum = ctx.spec_ptr_->value_expr_->datum_meta_;
  ObObjType doc_type = doc_datum.type_;
  ObCollationType doc_cs_type = doc_datum.cs_type_;
  ObString j_str;
  bool is_null = false;
  ObIMulModeBase *input_node = NULL;

  if (doc_type == ObNullType) {
    ret = OB_ITER_END;
  } else if (ctx.is_xml_table_func()) {
    if (!doc_obj_datum.is_xml_sql_type() && !ob_is_string_type(doc_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(doc_type)));
    } else {
      jt.reset_columns();
      // get input_node
      if (OB_FAIL(ObXMLExprHelper::get_xml_base_from_expr(ctx.spec_ptr_->value_expr_, ctx.mem_ctx_, eval_ctx, input_node))) {
        LOG_WARN("get real data failed", K(ret));
      } else {
        jt.input_ = input_node;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table func", K(ret));
  }
  return ret;
}

int XmlTableFunc::reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx)
{
  INIT_SUCC(ret);
  bool need_eval = false;
  if (scan_node.node_type() == REG_TYPE) {
    bool is_datum_data = false;
    if (scan_node.type() == COL_TYPE_XMLTYPE_XML) {
      if (scan_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
        need_eval = true;
      }
    } else if (scan_node.type() == COL_TYPE_VAL_EXTRACT_XML) {
      if (scan_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
        need_eval = true;
        is_datum_data = true;
      }
    }
    if (need_eval) {
      ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(scan_node.col_info_.empty_expr_id_);
      if (OB_FAIL(eval_default_value(ctx, default_expr, scan_node.emp_val_, is_datum_data))) {
        LOG_WARN("fail to eval default value", K(ret));
      } else {
        scan_node.is_emp_evaled_ = true;
        scan_node.res_flag_ = ResultType::NOT_DATUM;
        ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(scan_node.col_info_.output_column_idx_);
      }
    }
  }
  return ret;
}

int XmlTableFunc::init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx)
{
  INIT_SUCC(ret);
  bool need_eval = false;    // flag of eval default value
  // init path
  scan_node.tab_type_ = MulModeTableType::OB_ORA_XML_TABLE_TYPE;
  if (!scan_node.is_path_evaled_ && OB_ISNULL(scan_node.path_)
      && (scan_node.node_type() == REG_TYPE || scan_node.node_type() == SCAN_TYPE)
      && !scan_node.col_info_.path_.empty()) {
    ObPathExprIter *t_iter = NULL;
    ObIMulModeBase *doc = nullptr;
    scan_node.path_ = NULL;
    void* path_buf = ctx->op_exec_alloc_->alloc(sizeof(ObPathExprIter));
    if (OB_ISNULL(doc)) {
      if (OB_ISNULL(doc = OB_NEWx(ObXmlDocument, ctx->mem_ctx_->allocator_, ObMulModeNodeType::M_CONTENT, ctx->mem_ctx_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create document", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(ctx->spec_ptr_->namespace_def_,
                          ctx->default_ns,
                          ctx->context, *ctx->op_exec_alloc_))) {
        LOG_WARN("fail to get namespace", K(ret));
    } else if (OB_ISNULL(path_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate json path buffer", K(ret));
    } else {
      ObPathVarObject *t_ns = static_cast<ObPathVarObject*>(ctx->context);
      t_iter = new (path_buf) ObPathExprIter(ctx->op_exec_alloc_, ctx->mem_ctx_->allocator_);
      if (OB_FAIL(t_iter->init(ctx->xpath_ctx_, scan_node.col_info_.path_, ctx->default_ns, doc, t_ns))) {
        LOG_WARN("fail to init xpath iterator", K(scan_node.col_info_.path_), K(ctx->default_ns), K(ret));
      } else if (OB_FAIL(t_iter->open())) {
        ret = OB_ERR_PARSE_XQUERY_EXPR;
        LOG_USER_ERROR(OB_ERR_PARSE_XQUERY_EXPR, t_iter->get_path_str().length(), t_iter->get_path_str().ptr());
        LOG_WARN("fail to open xpath iterator", K(ret));
        // ObXMLExprHelper::replace_xpath_ret_code(ret);
      } else if (OB_FAIL(ObXMLExprHelper::check_xpath_valid(*t_iter, scan_node.node_type() == SCAN_TYPE))) {
        LOG_WARN("check xpath valid failed", K(ret));
      } else {
        scan_node.path_ = t_iter;
        scan_node.is_path_evaled_ = true;
      }
    }
  }
  // default value init
  if (OB_FAIL(ret)) {
  } else if (scan_node.node_type() == REG_TYPE) {
    if (!scan_node.is_emp_evaled_) {
      need_eval = false;
      bool is_datum_data = false;
      if (scan_node.type() == COL_TYPE_XMLTYPE_XML) {
        if (scan_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
          need_eval = true;
        }
      } else if (scan_node.type() == COL_TYPE_VAL_EXTRACT_XML) {
        if (scan_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
          need_eval = true;
          is_datum_data = true;
        }
      }
      if (need_eval) {
        ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(scan_node.col_info_.empty_expr_id_);
        if (OB_FAIL(eval_default_value(ctx, default_expr, scan_node.emp_val_, is_datum_data))) {
          LOG_WARN("fail to eval default value", K(ret));
        } else {
          scan_node.is_emp_evaled_ = true;
          scan_node.res_flag_ = ResultType::NOT_DATUM;
          ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(scan_node.col_info_.output_column_idx_);
          if (OB_FAIL(check_default_value(ctx, scan_node, col_expr))) {
            LOG_WARN("check default value failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int XmlTableFunc::eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum)
{
  INIT_SUCC(ret);
  ObDatum* emp_datum = nullptr;
  if (OB_FAIL(default_expr->eval(*ctx->eval_ctx_, emp_datum))) {
    LOG_WARN("failed do cast to returning type.", K(ret));
  } else if (!ob_is_xml_sql_type(default_expr->datum_meta_.type_, default_expr->obj_meta_.get_subschema_id())) {
    res = emp_datum;
  } else {
    ObIMulModeBase* xml_base = NULL;
    if (OB_FAIL(ObXMLExprHelper::get_xml_base(ctx->xpath_ctx_, emp_datum, *ctx->eval_ctx_, xml_base, ObGetXmlBaseType::OB_SHOULD_CHECK))) {
      LOG_WARN("failed do cast to returning type.", K(ret));
    } else if (OB_NOT_NULL(xml_base)) {
      if (need_datum) {
        res = emp_datum;
      } else {
        res = xml_base;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null value", K(ret));
    }
  }
  return ret;
}

// init_flag
int XmlTableFunc::reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value)
{
  INIT_SUCC(ret);
  ObPathExprIter *t_iter = NULL;
  ObIMulModeBase *doc = static_cast<ObIMulModeBase*>(in);
  if (init_flag == COL_NODE_TYPE) {
    if (OB_ISNULL(doc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc can not be null", K(ret));
    } else if (scan_node.col_info_.path_.length() == 0) {
      ret = OB_ERR_PARSE_XQUERY_EXPR;
      LOG_WARN("path can not be null", K(ret));
    } else if (scan_node.col_info_.path_[0] != '/' && doc->size() == 1) {
      char* extend_start = nullptr;
      int64_t extend_len = 0;
      ObXmlBin* bin_doc = nullptr;
      if (doc->check_extend()) {
        bin_doc = static_cast<ObXmlBin*>(doc);
        if (OB_FAIL(bin_doc->get_extend(extend_start, extend_len))) {
          LOG_WARN("fail to get extend", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        doc = doc->at(0);
        if (OB_NOT_NULL(doc) && OB_NOT_NULL(bin_doc = static_cast<ObXmlBin*>(doc))
          && OB_NOT_NULL(extend_start) && extend_len > 0
          && OB_FAIL(bin_doc->append_extend(extend_start, extend_len))) {
          LOG_WARN("fail to append extend", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(doc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("doc can not be null", K(ret));
  } else if (scan_node.is_path_evaled_ && OB_NOT_NULL(scan_node.path_)) {
    t_iter = static_cast<ObPathExprIter*>(scan_node.path_);
    if (OB_FAIL(t_iter->reset(doc, ctx->mem_ctx_->allocator_))) {
      LOG_WARN("fail to reset t_iter", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path_ should not be null", K(ret));
  }
  // scan node get first node
  if (OB_SUCC(ret) && scan_node.node_type() == SCAN_TYPE) {
    bool is_null_res = false;
    if (OB_FAIL(get_iter_value(scan_node, ctx, is_null_res))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get first node", K(ret));
      }
    } else {
      is_null_value = is_null_res;
    }
  }
  return ret;
}

int XmlTableFunc::get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value)
{
  INIT_SUCC(ret);
  is_null_value = false;
  ObPathExprIter *xpath_iter = static_cast<ObPathExprIter*>(col_node.path_);
  ObIMulModeBase *node = NULL;
  if (OB_ISNULL(xpath_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xpath iter can not be null", K(ret));
  } else if (OB_FAIL(xpath_iter->get_next_node(node))) {
    if (ret != OB_ITER_END) {
      ret = OB_ERR_PARSE_XQUERY_EXPR;
      LOG_USER_ERROR(OB_ERR_PARSE_XQUERY_EXPR, xpath_iter->get_path_str().length(), xpath_iter->get_path_str().ptr());
      LOG_WARN("fail to get next xml node", K(ret));
    }
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xpath result node is null", K(ret));
  } else if (node->type() == ObMulModeNodeType::M_DOCUMENT
            || node->type() == ObMulModeNodeType::M_CONTENT) {
    col_node.iter_ = node;
    col_node.curr_ = node;
    col_node.cur_pos_ ++;
  } else if (node->is_tree() && OB_FAIL(ObMulModeFactory::transform(ctx->mem_ctx_, node, BINARY_TYPE, node))) {
    LOG_WARN("fail to transform to tree", K(ret));
  } else {
    ObBinAggSerializer bin_agg(ctx->mem_ctx_->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));

    ObXmlBin *bin = nullptr;
    char* extend_start = nullptr;
    int64_t extend_len = 0;
    if (OB_ISNULL(bin = static_cast<ObXmlBin*>(node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get bin failed", K(ret));
    } else if (bin->check_extend()) {
      // must be one ans, append extend after final result
      if (OB_FAIL(bin->get_extend(extend_start, extend_len))) {
        LOG_WARN("fail to get extend", K(ret));
      } else if (OB_FAIL(bin->remove_extend())) {
        LOG_WARN("fail to remove extend", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bin_agg.append_key_and_value(bin))) {
      LOG_WARN("fail to add node in doc", K(ret));
    } else {
      /* seek result can not be next input xml,should add doc node in front */
      ObMulModeNodeType type = bin->type();
      ObMulModeNodeType node_type = M_CONTENT;
      if (type == ObMulModeNodeType::M_ELEMENT) {
        node_type = M_DOCUMENT;
      }
      bin_agg.set_header_type(node_type);
      if (OB_FAIL(bin_agg.serialize())) {
        LOG_WARN("failed to serialize binary.", K(ret));
      } else if (OB_FAIL(ObMulModeFactory::get_xml_base(ctx->mem_ctx_, bin_agg.get_buffer()->string(),
                                            ObNodeMemType::BINARY_TYPE,
                                            ObNodeMemType::BINARY_TYPE,
                                            node))) {
        LOG_WARN("fail to transform to tree", K(ret));
      } else if (OB_NOT_NULL(node) && OB_NOT_NULL(bin = static_cast<ObXmlBin*>(node)) && OB_NOT_NULL(extend_start)
        && OB_FAIL(bin->append_extend(extend_start, extend_len))) {
        LOG_WARN("fail to append extend", K(ret), K(node));
      } else {
        col_node.iter_ = node;
        col_node.curr_ = node;
        col_node.cur_pos_ ++;
      }
    }
  }
  return ret;
}

int XmlTableFunc::eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res)
{
  INIT_SUCC(ret);
  ObIMulModeBase *xml_res = NULL;
  ObPathExprIter *xml_iter = static_cast<ObPathExprIter*>(col_node.path_);
  if (OB_ISNULL(xml_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input containter or xpath  can not be null", K(ret));
  } else if (OB_ISNULL(in)) {
    is_null_value = true;
    col_node.curr_ = nullptr;
  } else if (OB_FAIL(ObXMLExprHelper::get_xpath_result(*xml_iter, xml_res, ctx->mem_ctx_, col_node.type() == COL_TYPE_XMLTYPE_XML))) {
    LOG_WARN("xml seek failed", K(col_node.col_info_.path_), K(ret));
    SET_COVER_ERROR(ctx, ret);
  } else if (OB_ISNULL(xml_res) || xml_res->size() == 0) {
    is_null_value = true;
    col_node.curr_ = nullptr;
  } else {
    is_null_value = false;
    col_node.curr_ = xml_res;
  }
  return ret;
}

int XmlTableFunc::col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  ObObjType obj_type = col_node.col_info_.data_type_.get_obj_type();
  JtColType col_type = col_node.type();
  if (col_type == COL_TYPE_XMLTYPE_XML) {
  }
  return ret;
}

// default value cast type check
bool RegularCol::check_cast_allowed(const ObObjType orig_type,
                                    const ObCollationType orig_cs_type,
                                    const ObObjType expect_type,
                                    const ObCollationType expect_cs_type,
                                    const bool is_explicit_cast)
{
  UNUSED(expect_cs_type);
  bool res = true;
  ObObjTypeClass ori_tc = ob_obj_type_class(orig_type);
  ObObjTypeClass expect_tc = ob_obj_type_class(expect_type);
  bool is_expect_lob_tc = (ObLobTC == expect_tc || ObTextTC == expect_tc);
  bool is_ori_lob_tc = (ObLobTC == ori_tc || ObTextTC == ori_tc);
  if (is_oracle_mode()) {
    if (is_explicit_cast) {
      // can't cast lob to other type except char/varchar/nchar/nvarchar2/raw. clob to raw not allowed too.
      if (is_ori_lob_tc) {
        if (expect_tc == ObJsonTC) {
          /* oracle mode, json text use lob store */
        } else if (ObStringTC == expect_tc) {
          // do nothing
        } else if (ObRawTC == expect_tc) {
          res = CS_TYPE_BINARY == orig_cs_type;
        } else {
          res = false;
        }
      }
      // any type to lob type not allowed.
      if (is_expect_lob_tc) {
        res = false;
      }
    } else {
      // BINARY FLOAT/DOUBLE not allow cast lob whether explicit
      if (is_ori_lob_tc) {
        if (expect_tc == ObFloatTC || expect_tc == ObDoubleTC) {
          res = false;
        }
      }
    }
  }
  return res;
}

int XmlTableFunc::check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr)
{
  INIT_SUCC(ret);
  ObString in_str;

  if (col_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
    ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(col_node.col_info_.empty_expr_id_);

    if (static_cast<JtColType>(col_node.col_info_.col_type_) == COL_TYPE_XMLTYPE_XML) {
      // 检查默认值类型
      if (!default_expr->obj_meta_.is_xml_sql_type()) {
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "XMLTYPE", ob_obj_type_str(default_expr->datum_meta_.type_));
      }
    } else if (static_cast<JtColType>(col_node.col_info_.col_type_) == COL_TYPE_VAL_EXTRACT_XML) {
      if (OB_ISNULL(default_expr) || OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dat can not be null", K(ret), KP(default_expr), KP(expr));
      }
      if (OB_FAIL(ret)) {
      } else if (!RegularCol::check_cast_allowed(default_expr->datum_meta_.type_,
                                                  default_expr->datum_meta_.cs_type_,
                                                  expr->datum_meta_.type_,
                                                  expr->datum_meta_.cs_type_,
                                                  true)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("explicit cast to lob type not allowed", K(ret), K(expr->datum_meta_.type_));
      }
    }
  }
  return ret;
}

int XmlTableFunc::set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null)
{
  INIT_SUCC(ret);
  JtColType col_type = col_node.type();
  if (col_type == COL_TYPE_XMLTYPE_XML) {
    switch (col_node.col_info_.on_empty_) {
      case JSN_VALUE_IMPLICIT:
      case JSN_VALUE_NULL: {
        col_node.curr_ = nullptr;
        is_null = true;
        ret = OB_SUCCESS;
        break;
      }
      case JSN_VALUE_DEFAULT: {
        if (col_node.is_emp_evaled_ && OB_NOT_NULL(col_node.emp_val_)) {
          col_node.curr_ = col_node.emp_val_;
          col_node.res_flag_ = ResultType::EMPTY_DATUM;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null value", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ctx->table_func_->cast_to_result(col_node, ctx, false))) {
            LOG_WARN("fail to cast to res", K(ret));
          } else {
            need_cast = false;
            is_null = false;
          }
        }
        break;
      }
      default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
        break;
    }
  } else if (col_type == COL_TYPE_VAL_EXTRACT_XML) {
    switch (col_node.col_info_.on_empty_) {
      case JSN_VALUE_IMPLICIT:
      case JSN_VALUE_NULL: {
        col_node.curr_ = nullptr;
        is_null = true;
        ret = OB_SUCCESS;
        break;
      }
      case JSN_VALUE_DEFAULT: {
        is_null = false;
        if (col_node.is_emp_evaled_ && OB_NOT_NULL(col_node.emp_val_)) {
          col_node.curr_ = col_node.emp_val_;
          col_node.res_flag_ = ResultType::EMPTY_DATUM;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null value", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ctx->table_func_->cast_to_result(col_node, ctx, true))) {
            LOG_WARN("fail to cast to res", K(ret));
          } else {
            need_cast = false;
          }
        }
        break;
      }
      default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
        break;
    }
  }
  return ret;
}

int XmlTableFunc::set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret)
{
  INIT_SUCC(tmp_ret);
  if (ret == OB_SUCCESS) {
  } else {
    const ObJtColInfo& info = col_node.col_info_;
    if (info.on_error_ == JSN_VALUE_ERROR || info.on_error_ == JSN_VALUE_IMPLICIT) {
      EVAL_COVER_CODE(ctx, ret) ;
      if (OB_SUCC(ret) && ctx->is_need_end_) {
        ret = OB_ITER_END;
      }
    }
  }
  return tmp_ret;
}

int XmlTableFunc::cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error, bool is_pack_result)
{
  INIT_SUCC(ret);
  UNUSED(enable_error);
  UNUSED(is_pack_result);
  JtColType col_type = col_node.type();

  ObJtColInfo& col_info = col_node.get_column_def();
  bool is_truncate = static_cast<bool>(col_info.truncate_);

  ObExpr* expr = ctx->spec_ptr_->column_exprs_.at(col_info.output_column_idx_);
  ObDatum& res = expr->locate_datum_for_write(*ctx->eval_ctx_);
  ctx->res_obj_ = &res;
  ObXmlBin *doc = static_cast<ObXmlBin*>(col_node.curr_);

  ObObjType dst_type = expr->datum_meta_.type_;
  ObCollationType coll_type = expr->datum_meta_.cs_type_;
  ObAccuracy accuracy = col_info.data_type_.get_accuracy();
  ObCollationType dst_coll_type = col_info.data_type_.get_collation_type();
  ObCollationType in_coll_type = ctx->is_charset_converted_
                                 ? CS_TYPE_UTF8MB4_BIN
                                 : ctx->spec_ptr_->value_expr_->datum_meta_.cs_type_;
  ObCollationLevel dst_coll_level = col_info.data_type_.get_collation_level();
  ObString xml_str;

  // 这里是不是不应该根据列定义划分，而是根据类型划分，，xml列就使用extract的逻辑，其他列就调用cast逻辑。
  switch(col_type) {
    case COL_TYPE_XMLTYPE_XML : {
      ObString blob_locator;
      if (OB_FAIL(doc->get_raw_binary(xml_str, &ctx->row_alloc_))) {
        LOG_WARN("failed to get bin", K(ret));
      } else if (OB_FAIL(ObXMLExprHelper::pack_binary_res(*expr, *ctx->eval_ctx_, xml_str, blob_locator))) {
        LOG_WARN("pack binary res failed", K(ret));
      } else {
        res.set_string(blob_locator.ptr(), blob_locator.length());
      }
      break;
    }
    case COL_TYPE_VAL_EXTRACT_XML : {
      if (col_node.res_flag_ == ResultType::NOT_DATUM) { // xmltype to unxmltype
        if (OB_FAIL(ObXMLExprHelper::extract_xml_text_node(ctx->mem_ctx_, doc, xml_str))) {
          LOG_WARN("fail to extract xml text node", K(ret), K(xml_str));
        } else if (OB_FAIL(ObXMLExprHelper::cast_to_res(ctx->row_alloc_, xml_str, *expr, *ctx->eval_ctx_, res))) {
          LOG_WARN("fail to cast to res", K(ret), K(xml_str));
        }
      } else { // use datum cast non xmltype, current only use for default value
        ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(col_node.col_info_.empty_expr_id_);
        ObDatum *src = static_cast<ObDatum*>(col_node.curr_);
        col_node.res_flag_ = NOT_DATUM; // reset flag;
        ObObj src_obj;
        bool need_check_acc = false;
        if (ob_is_xml_sql_type(default_expr->datum_meta_.type_, default_expr->obj_meta_.get_subschema_id())) {
          need_check_acc = true;
        }
        if (src->is_null()) {
          res.set_null();
        } else if (OB_FAIL(src->to_obj(src_obj, default_expr->obj_meta_, default_expr->obj_datum_map_))) {
          LOG_WARN("fail cast datum to obj", K(ret));
        } else if (OB_FAIL(ObXMLExprHelper::cast_to_res(ctx->row_alloc_, src_obj, *expr, *ctx->eval_ctx_, res, need_check_acc))) {
          LOG_WARN("fail to cast to res", K(ret), K(xml_str));
        }
      }

      break;
    }
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column type", K(ret), K(col_type));
      break;
    }
  }
  return ret;
}

// scan node function implement
int ScanNode::assign(const ScanNode& other)
{
  INIT_SUCC(ret);

  if (OB_FAIL(reg_col_defs_.assign(other.reg_col_defs_))) {
    LOG_WARN("fail to assign col defs.", K(ret), K(other.reg_col_defs_.count()));
  } else if (OB_FAIL(child_idx_.assign(other.child_idx_))) {
    LOG_WARN("fail to assign child idx defs.", K(ret), K(other.child_idx_.count()));
  } else {
    seek_node_ = other.seek_node_;
  }
  return ret;
}

int JoinNode::assign(const JoinNode& other)
{
  INIT_SUCC(ret);
  right_ = other.right_;
  left_ = other.left_;
  return ret;
}

int ScanNode::add_reg_column_node(ObRegCol* node, bool add_idx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(reg_col_defs_.push_back(node))) {
    LOG_WARN("fail to store node ptr", K(ret), K(reg_col_defs_.count()));
  }
  return ret;
}

// basic function
int ObRegCol::open(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ctx->table_func_->init_ctx(*this, ctx))) {
    LOG_WARN("fail to init variable" , K(ret));
  } else {
    ord_val_ = -1;
    cur_pos_ = -1;
  }

  return ret;
}

int ObRegCol::reset(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ctx->table_func_->reset_ctx(*this, ctx))) {
    LOG_WARN("fail to init variable" , K(ret));
  } else {
    ord_val_ = -1;
    cur_pos_ = -1;
  }
  return ret;
}

int UnionNode::open(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObMultiModeTableNode::open(ctx))) {
    LOG_WARN("fail to open column node.", K(ret));
  } else if (left_ && OB_FAIL(left_->open(ctx))) {
    LOG_WARN("fail to open left node.", K(ret));
  } else if (right_ && OB_FAIL(right_->open(ctx))) {
    LOG_WARN("fail to open right node.", K(ret));
  }
  return ret;
}

int UnionNode::reset(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObMultiModeTableNode::reset(ctx))) {
    LOG_WARN("fail to reset base node", K(ret));
  } else if (left_ && OB_FAIL(left_->reset(ctx))) {
    LOG_WARN("fail to reset left child", K(ret));
  } else if (right_ && OB_FAIL(right_->reset(ctx))) {
    LOG_WARN("fail to reset right child", K(ret));
  } else {
    is_left_iter_end_ = false;
    is_right_iter_end_ = true;
  }
  return ret;
}

int ScanNode::open(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObMultiModeTableNode::open(ctx))) {
    LOG_WARN("fail to open column node.", K(ret));
  } else if (OB_FAIL(seek_node_.open(ctx))) {
    LOG_WARN("fail to open seek node.", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < reg_col_defs_.count(); ++i) {
      ObRegCol* node = reg_col_defs_.at(i);
      if (OB_FAIL(node->open(ctx))) {
        LOG_WARN("fail to open reg node.", K(ret));
      }
    }
  }
  return ret;
}

int ScanNode::reset(JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObMultiModeTableNode::reset(ctx))) {
    LOG_WARN("fail to reset base node", K(ret));
  } else if (OB_FAIL(seek_node_.reset(ctx))) {
    LOG_WARN("fail to reset seek node", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < reg_col_defs_.count(); ++i) {
    ObRegCol* node = reg_col_defs_.at(i);
    ret = node->reset(ctx);
  }
  return ret;
}

void ObMultiModeTableNode::destroy()
{
  // do nothing
}

void ObRegCol::destroy()
{
  // path destory
  if (is_path_evaled_ && OB_NOT_NULL(path_)) {
    is_path_evaled_ = false;
    if (tab_type_ == OB_ORA_XML_TABLE_TYPE) {
      ObPathExprIter* tmp_iter = static_cast<ObPathExprIter*>(path_);
      tmp_iter->~ObPathExprIter();
    } else if (tab_type_ == OB_ORA_JSON_TABLE_TYPE) { // do nothing current
      ObJsonPath* json_path = static_cast<ObJsonPath*>(path_);
      json_path->~ObJsonPath();
    }
  }
}

void UnionNode::destroy()
{
  if (OB_NOT_NULL(left_)) {
    left_->destroy();
  }

  if (OB_NOT_NULL(right_)) {
    right_->destroy();
  }
}

void ScanNode::destroy()
{
  seek_node_.destroy();
  for (size_t i = 0; i < reg_col_defs_.count(); ++i) {
    reg_col_defs_.at(i)->destroy();
  }

  reg_col_defs_.reset();
  child_idx_.reset();
}

// common logical : iter
int ObRegCol::eval_regular_col(void *in, JtScanCtx* ctx, bool& is_null_value)
{
  INIT_SUCC(ret);
  JtColType col_type = type();
  is_null_value = false;
  bool is_null_res = false;
  ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(col_info_.output_column_idx_);
  ctx->res_obj_ = &col_expr->locate_datum_for_write(*ctx->eval_ctx_);
  bool need_cast_res = true;
  bool enable_error = true;

  if (lib::is_mysql_mode() && OB_ISNULL(in)) {
    is_null_value = true;
    need_cast_res = false;
    curr_ = nullptr;
    col_expr->locate_datum_for_write(*ctx->eval_ctx_).set_null();
  } else if (col_type == COL_TYPE_ORDINALITY
            || col_type == COL_TYPE_ORDINALITY_XML) {
    if (OB_ISNULL(in)) {
      col_expr->locate_datum_for_write(*ctx->eval_ctx_).set_null();
    } else {
      col_expr->locate_datum_for_write(*ctx->eval_ctx_).set_int(ctx->ord_val_);
    }
    col_expr->get_eval_info(*ctx->eval_ctx_).evaluated_ = true;
  } else {
    if (OB_FAIL(ctx->table_func_->col_res_type_check(*this, ctx))) {
      LOG_WARN("check column res type failed", K(ret), K(col_info_.data_type_), K(col_info_.col_type_));
    } else if (OB_ISNULL(in)) {
      is_null_value = true;
      curr_ = nullptr;
      EVAL_COVER_CODE(ctx, ret);
    } else if (OB_FAIL(ctx->table_func_->reset_path_iter(*this, in, ctx, ScanType::COL_NODE_TYPE, is_null_res))) {
      RESET_COVER_CODE(ctx);
      LOG_WARN("fail to init func path", K(ret));
    } else if (is_null_res) {
      is_null_value = true;
    } else if (OB_FAIL(ctx->table_func_->eval_seek_col(*this, in, ctx, is_null_res, need_cast_res))) {
      SET_COVER_ERROR(ctx, ret);
      LOG_WARN("json seek failed", K(col_info_.path_), K(ret));
    } else if (curr_ == nullptr || is_null_res) {
      is_null_value = true;
    } else {
      is_null_value = false;
      if (col_type == COL_TYPE_QUERY) {
        if (OB_FAIL(RegularCol::eval_query_col(*this, ctx, col_expr, is_null_value))) {
          LOG_WARN("fail to eval json query value", K(ret), K(col_type));
        }
      } else if (col_type == COL_TYPE_VALUE) {
        if (OB_FAIL(RegularCol::eval_value_col(*this, ctx, col_expr, is_null_value))) {
          LOG_WARN("fail to eval json value value", K(ret), K(col_type));
        }
      } else if (col_type == COL_TYPE_EXISTS) {
        if (OB_FAIL(RegularCol::eval_exist_col(*this, ctx, col_expr, is_null_value))) {
          LOG_WARN("fail to eval json exist value", K(ret), K(col_type));
        }
      } else if (col_type == COL_TYPE_XMLTYPE_XML) {
        if (OB_FAIL(RegularCol::eval_xml_type_col(*this, ctx, col_expr))) {
          LOG_WARN("fail to eval xml type value", K(ret), K(col_type));
        }
      } else if (col_type == COL_TYPE_VAL_EXTRACT_XML) {
        if (OB_FAIL(RegularCol::eval_xml_scalar_col(*this, ctx, col_expr))) {
          LOG_WARN("fail to eval xml scalar value", K(ret), K(col_type));
        }
      }
    }
  }

  if (OB_FAIL(ret)) { // deal empty value
  } else if (col_type == COL_TYPE_EXISTS
              || col_type == COL_TYPE_QUERY
              || col_type == COL_TYPE_VALUE
              || col_type == COL_TYPE_XMLTYPE_XML
              || col_type == COL_TYPE_VAL_EXTRACT_XML) {
    if (is_null_value) {
      if (OB_FAIL(ctx->table_func_->set_on_empty(*this, ctx, need_cast_res, is_null_value))) {
        LOG_WARN("fail to process on empty", K(ret));
      }
    } else if (ctx->is_json_table_func()
              && curr_ && NOT_DATUM == res_flag_
              && static_cast<ObIJsonBase*>(curr_)->json_type() == ObJsonNodeType::J_NULL
              && (!static_cast<ObIJsonBase*>(curr_)->is_real_json_null(static_cast<ObIJsonBase*>(curr_))
                  || lib::is_mysql_mode())) {
      curr_ = nullptr;
    }
  }
  // deal error value
  if (OB_FAIL(ret)) {
    if (ctx->is_cover_error_) {
      enable_error = false;
      int tmp_ret = ctx->table_func_->set_on_error(*this, ctx, ret);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("failed to set error val.", K(tmp_ret));
      }
    }
  }
  // cast_to_res
  if (OB_FAIL(ret)) {
  } else if (col_type == COL_TYPE_EXISTS
              || col_type == COL_TYPE_QUERY
              || col_type == COL_TYPE_VALUE
              || col_type == COL_TYPE_XMLTYPE_XML
              || col_type == COL_TYPE_VAL_EXTRACT_XML) {
    if (OB_ISNULL(curr_)) {
      is_null_value = true;
      col_expr->locate_datum_for_write(*ctx->eval_ctx_).set_null();
    } else if (need_cast_res && OB_FAIL(ctx->table_func_->cast_to_result(*this, ctx, enable_error))) {
      LOG_WARN("failed to do cast to res type", K(ret));
    } else if (OB_ISNULL(curr_)) {
      is_null_value = true;
    }
  }
  if (OB_SUCC(ret)) {
    res_flag_ = NOT_DATUM;
    col_expr->get_eval_info(*ctx->eval_ctx_).evaluated_ = true;
  }

  return ret;
}

int RegularCol::check_default_value_inner_oracle(JtScanCtx* ctx,
                                                 ObJtColInfo &col_info,
                                                 ObExpr* col_expr,
                                                 ObExpr* default_expr)
{
  INIT_SUCC(ret);
  ObString in_str;
  ObDatum *emp_datum = nullptr;

  if (OB_FAIL(default_expr->eval(*ctx->eval_ctx_, emp_datum))) {
    LOG_WARN("failed do cast to returning type.", K(ret));
  } else {
    in_str.assign_ptr(emp_datum->ptr_, emp_datum->len_);
  }
  if (OB_FAIL(ret)) {
  } else if (default_expr->datum_meta_.type_ == ObNullType && ob_is_string_type(col_info.data_type_.get_obj_type())) {
    ret = OB_ERR_DEFAULT_VALUE_NOT_LITERAL;
    LOG_WARN("default value not match returing type", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::pre_default_value_check(col_expr->datum_meta_.type_, in_str, default_expr->datum_meta_.type_, col_info.data_type_.get_accuracy().get_length()))) {
    LOG_WARN("default value pre check fail", K(ret), K(in_str));
  } else {
    if (ob_obj_type_class(col_expr->datum_meta_.type_) == ob_obj_type_class(default_expr->datum_meta_.type_)
             && OB_FAIL(ObExprJsonValue::check_default_val_accuracy<ObDatum>(col_info.data_type_.get_accuracy(), default_expr->datum_meta_.type_, emp_datum))) {
      LOG_WARN("fail to check accuracy", K(ret));
    }
  }
  return ret;
}

int ScanNode::get_next_iter(void* in, JtScanCtx* ctx, bool& is_null_value)
{
  INIT_SUCC(ret);
  is_null_value = false;
  bool is_null_iter = false;
  if (!is_evaled_ || in_ != in) {
    in_ = in;
    is_null_result_ = false;
    if (OB_ISNULL(in_)) {
      is_null_value = is_null_result_ = true;
      seek_node_.curr_ = seek_node_.iter_ = nullptr;
      seek_node_.cur_pos_ = 0;
      seek_node_.total_ = 0;
    } else if (OB_FAIL(ctx->table_func_->reset_path_iter(seek_node_, in_, ctx, ScanType::SCAN_NODE_TYPE, is_null_iter))) {   // reset path & get first result
      RESET_COVER_CODE(ctx);
      LOG_WARN("fail to init path", K(ret), K(ctx->spec_ptr_->table_type_), K(ctx->table_func_));
    } else if (is_null_iter) {
      is_null_value = is_null_result_ = true;
      seek_node_.curr_ = seek_node_.iter_ = nullptr;
      seek_node_.total_ = 1;
      seek_node_.cur_pos_ = 0;
      // 1. if root node seek result is NULL, but input(in) not null,then return end.
      if (seek_node_.col_info_.parent_id_ == common::OB_INVALID_ID
          || (ctx->jt_op_->get_root_param() == in  // 2. if path == '$' && root scan node not have regular column,
              && ctx->jt_op_->get_root_entry()->get_scan_node()->reg_column_count() == 0
              && ctx->jt_op_->get_root_entry()->get_scan_node() == this)) {
        ret = OB_ITER_END;
      }
    }
    if (OB_SUCC(ret)) {
      is_evaled_= true;
      seek_node_.cur_pos_ = 0;
    }
  } else {
    is_null_iter = false;
    if (OB_FAIL(ctx->table_func_->get_iter_value(seek_node_, ctx, is_null_iter))) {
      if (ret == OB_ITER_END) {
        seek_node_.curr_ = seek_node_.iter_ = nullptr;
      } else {
        LOG_WARN("fail to get seek value", K(ret));
      }
    } else if (is_null_iter) {
      is_null_value = is_null_result_ = true;
      seek_node_.curr_ = seek_node_.iter_ = nullptr;
    }
  }
  return ret;
}

int ScanNode::get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value)
{
  INIT_SUCC(ret);
  bool is_empty_node = false;
  is_null_value = false;
  if (OB_FAIL(get_next_iter(in, ctx, is_empty_node))) {
    LOG_WARN("fail to get current node", K(ret));
  }
  is_null_value = is_empty_node;
  // need reset column
  reset_reg_columns(ctx);

  // eval regular column
  if (OB_SUCC(ret)) {
    uint32_t reg_count = reg_col_defs_.count();
    bool tmp_is_null = false;
    for (uint32_t i = 0; OB_SUCC(ret) && i < reg_count && is_evaled_; ++i) {
      ObRegCol* cur_node = reg_col_defs_.at(i);
      if (cur_node->type() == COL_TYPE_ORDINALITY || cur_node->type() == COL_TYPE_ORDINALITY_XML) {
        ctx->ord_val_ = seek_node_.cur_pos_ + 1;
      }
      if (OB_FAIL(cur_node->eval_regular_col(seek_node_.iter_, ctx, tmp_is_null))) {
        LOG_WARN("fail to get regular column value", K(ret));
      } else {
        is_null_value &= tmp_is_null;
      }
    } // eval scan node in join node
  }

  return ret;
}

int JoinNode::get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value)
{
  INIT_SUCC(ret);
  is_null_value = false;

  ObMultiModeTableNode* left_node = left();
  ObMultiModeTableNode* right_node = right();

  bool is_left_null = false;
  bool is_right_null = false;
  if (!is_right_iter_end_) {  // right node can expand more value,
  } else if (OB_NOT_NULL(left_)) {
    is_right_iter_end_ = false;
    ret = left_node->get_next_row(in, ctx, is_left_null);
    if (OB_FAIL(ret) && ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_SUCC(ret)) {
      is_null_value = is_left_null;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get iter node", K(ret));
  }

  bool is_sub_result_null = false; // nested child result is null then
  if (OB_SUCC(ret)) {
    // child : join ->right child_ = union node
    if (right_node) {
      if (OB_FAIL(right_node->get_next_row(get_curr_iter_value(), ctx, is_sub_result_null))) {
        if (OB_FAIL(ret) && ret != OB_ITER_END) {
          LOG_WARN("fail to get column value", K(ret));
        } else if (ret == OB_ITER_END) {
          is_right_iter_end_ = true;
        }
      } else if (OB_SUCC(ret)) {
        is_null_value &= is_sub_result_null;
        if (is_sub_result_null) {
          is_right_iter_end_ = true;
        }
      }
    } else {
      is_right_iter_end_ = true;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(get_curr_iter_value())
      && ctx->jt_op_->get_root_entry() == this) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ret) && is_right_iter_end_) { // nested column evaled finis should get next iter
    if (!is_evaled_) {
      ret = OB_SUCCESS; // ignore only one null result
    } else if (OB_FAIL(get_next_row(in, ctx, is_null_value))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get value", K(ret));
      }
    }
  } else if (OB_FAIL(ret)) { // if return fail, need reset flag
    is_right_iter_end_ = true;
  }
  is_evaled_ = true;

  return ret;
}

int UnionNode::get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value)
{
  INIT_SUCC(ret);

  ObMultiModeTableNode* left_node = left();
  ObMultiModeTableNode* right_node = right();
  is_null_value = false;
  bool is_left_null = false;
  bool is_right_null = false;
  if (in != in_) {
    is_left_iter_end_ = false;
  }
  if (OB_NOT_NULL(left_node) && !is_left_iter_end_) {
    ret = left_node->get_next_row(in, ctx, is_left_null);
    if (OB_FAIL(ret)) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        is_left_iter_end_ = true;
      }
    } else {
      is_null_value = is_left_null;
      if (is_left_null) {
        is_left_iter_end_ = true;
      }
    }
  } else {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    if (is_left_null && OB_NOT_NULL(right_node)) {
      ret = right_node->get_next_row(in, ctx, is_right_null);
      if (OB_FAIL(ret) &&  ret != OB_ITER_END) {
        LOG_WARN("fail to get next row", K(ret));
      } else if (OB_SUCC(ret)) {
        if (!is_right_null) {
          is_null_value = false;
        }
      }
    }
  } else if (OB_NOT_NULL(right_node) && (ret == OB_ITER_END)) {
    ret = right_node->get_next_row(in, ctx, is_right_null);
    if (OB_FAIL(ret) &&  ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_SUCC(ret) && is_right_null) {
      if (in_ == in) {
        ret = OB_ITER_END;
      }
    } else if (OB_SUCC(ret) && !is_right_null) {
      is_null_value = false;
    }
  }

  in_ = in;
  return ret;
}

// json/xml table function inner
int ObJsonTableOp::inner_get_next_row()
{
  INIT_SUCC(ret);
  bool is_root_null = false;
  if (!(jt_ctx_.is_xml_table_func()
        || jt_ctx_.is_json_table_func())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupport table function", K(ret));
  } else if (is_evaled_) {
    clear_evaluated_flag();
    if (OB_FAIL(root_->get_next_row(input_, &jt_ctx_, is_root_null))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to open get next row.", K(ret));
      }
    }
  } else {
    clear_evaluated_flag();
    if (OB_FAIL(jt_ctx_.table_func_->eval_input(*this, jt_ctx_, *jt_ctx_.eval_ctx_))) {  // get input value
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get input val", K(ret));
      }
    } else if (OB_FAIL(root_->get_next_row(input_, &jt_ctx_, is_root_null))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      is_evaled_ = true;
    }
  }

  return ret;
}

int JsonTableFunc::col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  ObObjType obj_type = col_node.col_info_.data_type_.get_obj_type();
  JtColType col_type = col_node.type();
  if (lib::is_mysql_mode()) {
  } else if (col_type == COL_TYPE_EXISTS) {
    if (ob_is_string_type(obj_type)
        || ob_is_numeric_type(obj_type)
        || ob_is_integer_type(obj_type)) {
      // do nothing
    } else if (ob_is_json_tc(obj_type)) {
      ret = OB_ERR_USAGE_KEYWORD;
      LOG_WARN("invalid usage of keyword EXISTS", K(ret));
    } else {
      ret = OB_ERR_NON_NUMERIC_CHARACTER_VALUE;
      SET_COVER_ERROR(ctx, ret);
    }
  } else if (col_type == COL_TYPE_QUERY ) {
    // do nothing
  }
  return ret;
}

int JsonTableFunc::check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr)
{
  INIT_SUCC(ret);
  if (lib::is_mysql_mode()) {
    // in mysql mode, should check default value with parse json
    if (OB_FAIL(RegularCol::check_default_value_mysql(col_node, ctx, expr))) {
      LOG_WARN("fail to check default value in mysql", K(ret));
    }
  } else { // oracle mode can use datum as result
    if (OB_FAIL(RegularCol::check_default_value_oracle(ctx, col_node.col_info_, expr))) {
      LOG_WARN("fail to check default value in oracle", K(ret));
    }
  }
  return ret;
}

int RegularCol::parse_default_value_2json(ObExpr* default_expr,
                                          JtScanCtx* ctx,
                                          ObDatum*& tmp_datum,
                                          ObIJsonBase *&res)
{
  INIT_SUCC(ret);
  ObObjType val_type = default_expr->datum_meta_.type_;
  ObCollationType cs_type = default_expr->datum_meta_.cs_type_;
  ObDatum converted_datum;
  converted_datum.set_datum(*tmp_datum);
  ObString origin_str = converted_datum.get_string();
  // convert string charset if needed
  if (ob_is_string_type(val_type) && ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4) {
    ObString converted_str;
    if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type, converted_str,
                                                      CS_TYPE_UTF8MB4_BIN, ctx->row_alloc_))) {
      LOG_WARN("convert string collation failed", K(ret), K(cs_type), K(origin_str.length()));
    } else {
      converted_datum.set_string(converted_str);
      cs_type = CS_TYPE_UTF8MB4_BIN;
    }
  }
  origin_str = converted_datum.get_string();
  if (OB_SUCC(ret)
      && OB_FAIL(ObJsonExprHelper::get_json_val(converted_datum, *ctx->exec_ctx_, default_expr,
                                                ctx->op_exec_alloc_, val_type, cs_type, res))) {
    LOG_WARN("fail to parse default value", K(ret));
  }
  return ret;
}

int RegularCol::check_default_value_mysql(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* expr)
{
  INIT_SUCC(ret);
  ctx->is_cover_error_ = false;
  ObIJsonBase* j_res = NULL;

  if (col_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
    j_res = static_cast<ObIJsonBase*>(col_node.emp_val_);
    ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(col_node.col_info_.empty_expr_id_);
    if (OB_FAIL(RegularCol::check_default_value_inner_mysql(ctx, col_node, default_expr, expr, j_res))) {
      LOG_WARN("fail to check empty default value", K(ret));
    }
  }

  if (OB_SUCC(ret) && col_node.col_info_.on_error_ == JSN_VALUE_DEFAULT) {
    j_res = static_cast<ObIJsonBase*>(col_node.err_val_);
    ObExpr* default_expr = ctx->spec_ptr_->err_default_exprs_.at(col_node.col_info_.error_expr_id_);
    if (OB_FAIL(RegularCol::check_default_value_inner_mysql(ctx, col_node, default_expr, expr, j_res))) {
      LOG_WARN("fail to check error default value", K(ret));
    }
  }
  return ret;
}

int RegularCol::check_default_value_inner_mysql(JtScanCtx* ctx,
                                                ObRegCol &col_node,
                                                ObExpr* default_expr,
                                                ObExpr* expr,
                                                ObIJsonBase* j_base)
{
  INIT_SUCC(ret);
  ObDatum res;
  char datum_buff[OBJ_DATUM_STRING_RES_SIZE] = {0};
  res.ptr_ = datum_buff;

  ObDatum* tmp_datum = nullptr;
  uint8_t is_type_mismatch = 0;
  ObAccuracy accuracy = col_node.col_info_.data_type_.get_accuracy();
  char col_str[col_node.col_info_.col_name_.length() + 1];
  ObObjType dst_type = expr->datum_meta_.type_;
  ObJsonCastParam cast_param(dst_type, default_expr->datum_meta_.cs_type_, expr->datum_meta_.cs_type_, false);
  cast_param.is_only_check_ = true;
  cast_param.relaxed_time_convert_ = true;
  cast_param.rt_expr_ = expr;

  if (OB_ISNULL(j_base)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json data can not be null", K(ret));
  } else if ((dst_type != ObJsonType && !j_base->is_json_scalar(j_base->json_type()))) {
    ret = OB_INVALID_DEFAULT;
    LOG_USER_ERROR(OB_INVALID_DEFAULT, col_node.col_info_.col_name_.length(), col_node.col_info_.col_name_.ptr());
  } else if (OB_FAIL(ObJsonUtil::cast_to_res(&ctx->row_alloc_, *ctx->eval_ctx_,
                            j_base, accuracy, cast_param, res, is_type_mismatch))) {
    ret = OB_OPERATE_OVERFLOW;
    databuff_printf(col_str, col_node.col_info_.col_name_.length() + 1, "%s", col_node.col_info_.col_name_.ptr());
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "JSON_TABLE", col_str);
  }
  return ret;
}

int RegularCol::check_default_value_oracle(JtScanCtx* ctx, ObJtColInfo &col_info, ObExpr* expr)
{
  INIT_SUCC(ret);
  if (static_cast<JtColType>(col_info.col_type_) == COL_TYPE_VALUE) {
    if (col_info.on_empty_ == JSN_VALUE_DEFAULT) {
      ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(col_info.empty_expr_id_);
      if (OB_FAIL(RegularCol::check_default_value_inner_oracle(ctx, col_info, expr, default_expr))) {
        LOG_WARN("fail to check empty default value in oracle", K(ret));
      }
    }
    if (OB_SUCC(ret) && col_info.on_error_ == JSN_VALUE_DEFAULT) {
      ObExpr* default_expr = ctx->spec_ptr_->err_default_exprs_.at(col_info.error_expr_id_);
      if (OB_FAIL(RegularCol::check_default_value_inner_oracle(ctx, col_info, expr, default_expr))) {
        LOG_WARN("fail to check error default value in oracle", K(ret));
      }
    }
  }
  return ret;
}

int JsonTableFunc::set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx)
{
  INIT_SUCC(ret);
  ObExpr* expr = ctx->spec_ptr_->column_exprs_.at(col_node.col_info_.output_column_idx_);

  ObObjType dst_type = expr->datum_meta_.type_;
  ObAccuracy accuracy = col_node.col_info_.data_type_.get_accuracy();
  col_node.expr_param_.truncate_ = col_node.col_info_.truncate_;
  col_node.expr_param_.format_json_ = col_node.col_info_.format_json_;
  col_node.expr_param_.wrapper_ = col_node.col_info_.wrapper_;
  col_node.expr_param_.empty_type_ = col_node.col_info_.on_empty_;
  col_node.expr_param_.error_type_ = col_node.col_info_.on_error_;
  col_node.expr_param_.accuracy_ = accuracy;
  col_node.expr_param_.dst_type_ = dst_type;
  col_node.expr_param_.pretty_type_ = 0;
  col_node.expr_param_.ascii_type_ = 0;
  col_node.expr_param_.scalars_type_ = col_node.col_info_.allow_scalar_;
  if (OB_FAIL(col_node.expr_param_.on_mismatch_.push_back(col_node.col_info_.on_mismatch_))) {
    LOG_WARN("fail to push mismatch value into array", K(ret));
  } else if (OB_FAIL(col_node.expr_param_.on_mismatch_type_.push_back(col_node.col_info_.on_mismatch_type_))) {
    LOG_WARN("fail to push mismatch type into array", K(ret));
  }
  return ret;
}

int JsonTableFunc::set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null)
{
  INIT_SUCC(ret);
  JtColType col_type = col_node.type();
  bool is_cover_by_error = true;
  if (col_type == COL_TYPE_QUERY) {
    is_null = false;
    bool is_json_arr = false;
    bool is_json_obj = false;
    if (OB_FAIL(ObExprJsonQuery::get_empty_option(is_cover_by_error,
                          col_node.col_info_.on_empty_, is_null, is_json_arr,
                          is_json_obj))) {
      if (is_cover_by_error) {
        SET_COVER_ERROR(ctx, ret);
      }
      LOG_WARN("empty cluase report error res", K(ret));
    } else if (is_null) {
      col_node.curr_ = nullptr;
      ret = OB_SUCCESS;
    } else if (is_json_arr) {
      col_node.curr_ = ctx->jt_op_->get_js_array();
    } else if (is_json_obj) {
      col_node.curr_ = ctx->jt_op_->get_js_object();
    }
  } else if (col_type == COL_TYPE_VALUE) {
    is_null = false;
    ObDatum *t_res = NULL;
    if (OB_FAIL(ObExprJsonValue::get_empty_option(t_res, is_cover_by_error, col_node.col_info_.on_empty_,
                                                static_cast<ObDatum*>(col_node.err_val_), is_null))) {
      if (is_cover_by_error) {
        SET_COVER_ERROR(ctx, ret);
      }
      LOG_WARN("empty clause report error opt", K(ret));
    } else if (is_null) {
      col_node.curr_ = nullptr;
    } else {
      if (OB_ISNULL(col_node.emp_val_) || !col_node.is_emp_evaled_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get err_val", K(ret), K(col_node.is_emp_evaled_));
      } else if (lib::is_oracle_mode()) {
        col_node.curr_ = col_node.emp_val_;
        col_node.res_flag_ = ResultType::EMPTY_DATUM;
      } else { // mysql mode
        col_node.curr_ = col_node.emp_val_;
        col_node.res_flag_ = ResultType::NOT_DATUM;
      }
    }
  } else if (col_type == COL_TYPE_EXISTS) {
    bool res_val = false;
    if (OB_FAIL(ObExprJsonExists::get_empty_option(col_node.col_info_.on_empty_, res_val))) {
      LOG_WARN("empty clause report error opt", K(ret));
    } else if (!res_val) { // result will return false currently, not return true
      if (ob_is_string_type(col_node.col_info_.data_type_.get_obj_type())) {
        ObString value = lib::is_oracle_mode() ? "false" : "0";
        void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          col_node.curr_ = static_cast<ObJsonString*>(new(buf)ObJsonString(value.ptr(), value.length()));
          is_null = false;
        }
      } else {
        void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonInt));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret));
        } else {
          col_node.curr_ = static_cast<ObJsonInt*>(new(buf)ObJsonInt(0));
          is_null = false;
        }
      }
    }
  }
  return ret;
}

int JsonTableFunc::set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret)
{
  INIT_SUCC(tmp_ret);
  if (ret == OB_SUCCESS) {
  } else {
    bool is_null = false;
    const ObJtColInfo& info = col_node.col_info_;
    JtColType col_type = col_node.type();
    ObIJsonBase* t_val = nullptr;
    bool has_default_val = false;
    ObExpr* expr = ctx->spec_ptr_->column_exprs_.at(col_node.col_info_.output_column_idx_);
    if (col_type == COL_TYPE_VALUE) {
      ObExprJsonValue::get_error_option(info.on_error_, is_null, has_default_val);
      if (is_null) {
        col_node.curr_ = nullptr;
        ret = ctx->is_need_end_ ? OB_ITER_END : OB_SUCCESS;
      } else if (has_default_val) {
        if (OB_ISNULL(col_node.err_val_) || !col_node.is_err_evaled_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get err_val", K(ret));
        } else if (lib::is_oracle_mode()) {
          col_node.curr_ = col_node.err_val_;
          col_node.res_flag_ = ResultType::ERROR_DATUM;
          ret = OB_SUCCESS;
        } else { // mysql mode
          col_node.curr_ = col_node.err_val_;
          col_node.res_flag_ = ResultType::NOT_DATUM;
          ret = OB_SUCCESS;
        }
      } else if (ret != OB_SUCCESS) {
        EVAL_COVER_CODE(ctx, ret) ;
        if (ctx->is_need_end_ && OB_SUCC(ret)) {
          ret = OB_ITER_END;
        }
      }
    } else if (col_type == COL_TYPE_QUERY) {
      t_val = NULL;
      if (col_node.expr_param_.error_type_ == JSN_QUERY_ERROR) {
      } else if (OB_FAIL(ObExprJsonQuery::get_error_option(col_node.expr_param_.error_type_, t_val,
                  ctx->jt_op_->get_js_array(), ctx->jt_op_->get_js_object(), is_null))) {
        LOG_WARN("error option report error", K(ret));
      } else if (is_null) {
        if (OB_FAIL(ObExprJsonQuery::get_mismatch_option(col_node.expr_param_.on_mismatch_[0], ctx->error_code_))) {
          LOG_WARN("mismatch clause will report error", K(ret));
        } else {
          is_null = true;
          col_node.curr_ = nullptr;
          ret = ctx->is_need_end_ ? OB_ITER_END : OB_SUCCESS;
        }
      } else {
        col_node.curr_ = t_val;
        ret = ctx->is_need_end_ ? OB_ITER_END : OB_SUCCESS;
      }
    } else if (col_type == COL_TYPE_EXISTS) {
      int is_true = 0;
      if (info.on_error_ == JSN_EXIST_ERROR) {
        ret = ctx->error_code_;
        if (OB_SUCC(ret) && ctx->is_need_end_) {
          ret = OB_ITER_END;
        }
      } else if (info.on_error_ == JSN_EXIST_DEFAULT || info.on_error_ == JSN_EXIST_FALSE) {
        is_null = false;
        ret = ctx->is_need_end_ ? OB_ITER_END : OB_SUCCESS;
      } else if (info.on_error_ == JSN_EXIST_TRUE) {
        is_true = 0;
        is_null = false;
        ret = ctx->is_need_end_ ? OB_ITER_END : OB_SUCCESS;
      }

      if (OB_FAIL(ret)) {
      } else if (ob_is_string_type(info.data_type_.get_obj_type())) {
        ObString value = is_true ? ObString("true") : ObString("false");
        if (lib::is_mysql_mode()) {
          value = is_true ? ObString("1") : ObString("0");
        }
        void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret));
        } else {
          col_node.curr_ = static_cast<ObJsonString*>(new(buf)ObJsonString(value.ptr(), value.length()));
          is_null = false;
        }
      } else if (ob_is_number_tc(info.data_type_.get_obj_type()) || lib::is_mysql_mode()) {
        void* buf = ctx->row_alloc_.alloc(sizeof(ObJsonInt));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret));
        } else {
          col_node.curr_ = static_cast<ObJsonInt*>(new(buf)ObJsonInt(is_true));
          is_null = false;
        }
      } else {
        if (col_node.col_info_.on_error_ != JSN_EXIST_ERROR) {
          col_node.curr_ = nullptr;
          is_null = true;
        } else {
          ret = OB_ERR_NON_NUMERIC_CHARACTER_VALUE;
        }
      }
    }

    if (OB_SUCC(ret) && is_null) {
      expr->locate_datum_for_write(*ctx->eval_ctx_).set_null();
    }
  }
  return ret;
}

int JsonTableFunc::cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error, bool is_pack_result)
{
  INIT_SUCC(ret);
  ObIJsonBase *js_val = static_cast<ObIJsonBase*>(col_node.curr_);
  ObJtColInfo& col_info = col_node.get_column_def();
  bool is_truncate = static_cast<bool>(col_info.truncate_);
  JtColType col_type = col_node.type();

  ObExpr* expr = ctx->spec_ptr_->column_exprs_.at(col_info.output_column_idx_);
  ObDatum& res = expr->locate_datum_for_write(*ctx->eval_ctx_);
  ctx->res_obj_ = &res;
  uint8_t is_type_mismatch = false;
  uint8_t ascii_type = false;

  ObObjType dst_type = expr->datum_meta_.type_;
  ObCollationType coll_type = expr->datum_meta_.cs_type_;
  ObAccuracy accuracy = col_info.data_type_.get_accuracy();
  ObCollationType dst_coll_type = col_info.data_type_.get_collation_type();
  ObCollationType in_coll_type = ctx->is_charset_converted_
                                 ? CS_TYPE_UTF8MB4_BIN
                                 : ctx->spec_ptr_->value_expr_->datum_meta_.cs_type_;
  ObCollationLevel dst_coll_level = col_info.data_type_.get_collation_level();
  bool is_quote = (col_info.col_type_ == COL_TYPE_QUERY && js_val->json_type() == ObJsonNodeType::J_STRING);
  ObJsonCastParam cast_param(dst_type, in_coll_type, dst_coll_type, ascii_type);
  cast_param.is_const_ = ctx->is_const_input_;
  cast_param.is_trunc_ = is_truncate;
  cast_param.relaxed_time_convert_ = true;
  cast_param.rt_expr_ = expr;
  switch (col_type) {
    case JtColType::COL_TYPE_VALUE: {
      if (col_node.res_flag_ != ResultType::NOT_DATUM) {
        ObDatum *js_val = static_cast<ObDatum*>(col_node.curr_);
        ObExpr* default_expr;
        if (col_node.res_flag_ == ResultType::EMPTY_DATUM) {
          default_expr = ctx->spec_ptr_->emp_default_exprs_.at(col_node.col_info_.empty_expr_id_);
        } else if (col_node.res_flag_ == ResultType::ERROR_DATUM) {
          default_expr = ctx->spec_ptr_->err_default_exprs_.at(col_node.col_info_.error_expr_id_);
        }

        if (OB_ISNULL(js_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("empty/error value can not be null", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::cast_to_res(ctx->row_alloc_, *js_val, *expr, *default_expr, *ctx->eval_ctx_, res, true))) {
          enable_error = false;
          LOG_WARN("fail to cast to res", K(ret));
        }
      } else {
        ret = ObJsonUtil::cast_to_res(&ctx->row_alloc_, *ctx->eval_ctx_,
                  js_val, accuracy, cast_param, res, is_type_mismatch);
      }
      break;
    }
    case JtColType::COL_TYPE_QUERY: {
      cast_param.is_quote_ = true;
      ret = ObJsonUtil::cast_to_res(&ctx->row_alloc_, *ctx->eval_ctx_,
                  js_val, accuracy, cast_param, res, is_type_mismatch);
      break;
    }
    case JtColType::COL_TYPE_EXISTS: {
      ret = ObJsonUtil::cast_to_res(&ctx->row_alloc_, *ctx->eval_ctx_,
                js_val, accuracy, cast_param, res, is_type_mismatch);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect type input", K(ret));
      break;
    }
  }
  if (OB_FAIL(ret) && enable_error) {
    int tmp_ret = set_on_error(col_node, ctx, ret);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("failed to set error val.", K(tmp_ret));
    } else if (OB_SUCC(tmp_ret) && OB_NOT_NULL(col_node.curr_)
               && OB_FAIL(cast_to_result(col_node, ctx, false, false))) { // due of without type calc, so use cast transform default value to res.
      LOG_WARN("fail to cast default value to res", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_pack_result && is_lob_storage(dst_type) && (col_node.res_flag_ == ResultType::NOT_DATUM) && !res.is_null()) {
    ObString val = res.get_string();
    if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(*expr, *ctx->eval_ctx_, res, val, &ctx->row_alloc_))) {
      LOG_WARN("fail to pack res result.", K(ret));
    }
  }
  return ret;
}

int JsonTableFunc::eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res)
{
  INIT_SUCC(ret);
  ObJsonSeekResult hit;
  ObIJsonBase* in_val = static_cast<ObIJsonBase*>(in);
  ObJsonPath* json_path = static_cast<ObJsonPath*>(col_node.path_);
  ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(col_node.col_info_.output_column_idx_);
  in_val->set_allocator(&ctx->row_alloc_);
  if (OB_FAIL(in_val->seek(*json_path, json_path->path_node_cnt(), true, false, hit))) {
    SET_COVER_ERROR(ctx, ret);
    LOG_WARN("json seek failed", K(col_node.col_info_.path_), K(ret));
  } else if (hit.size() == 0) {
    col_node.curr_ = nullptr;
    is_null_value = true;
  } else if (col_node.type() == COL_TYPE_EXISTS && hit.size() > 0) {
    is_null_value = false;
    col_node.curr_ = hit[0];
  } else if (col_node.type() != COL_TYPE_QUERY && hit.size() == 1) {
    is_null_value = false;
    col_node.curr_ = hit[0];
  } else if (col_node.type() == COL_TYPE_VALUE
             && !(lib::is_mysql_mode()
                   && ob_is_json(col_expr->datum_meta_.type_))) {
    ret = OB_ERR_JSON_VALUE_NO_SCALAR;
    SET_COVER_ERROR(ctx, ret);
  } else if (col_node.type() == COL_TYPE_QUERY ||
             (col_node.type() == COL_TYPE_VALUE
              && lib::is_mysql_mode()
              && ob_is_json(col_expr->datum_meta_.type_))) {
    void* js_arr_buf = ctx->row_alloc_.alloc(sizeof(ObJsonArray));
    ObIJsonBase* js_arr_ptr = nullptr;
    if (OB_ISNULL(js_arr_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate json array buf", K(ret));
    } else if (OB_ISNULL(js_arr_ptr = new (js_arr_buf) ObJsonArray(&ctx->row_alloc_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new json array node", K(ret));
    } else if (OB_FAIL(ObExprJsonQuery::append_node_into_res(js_arr_ptr, json_path, hit, &ctx->row_alloc_))) {
      LOG_WARN("fail to tree apeend node", K(ret));
    }

    if (OB_SUCC(ret)) {
      is_null_value = false;
      col_node.curr_ = js_arr_ptr;
    }
  }
  return ret;
}

int JsonTableFunc::init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx)
{
  INIT_SUCC(ret);
  ObJsonPath* js_path = NULL;
  scan_node.tab_type_ = MulModeTableType::OB_ORA_JSON_TABLE_TYPE;
  bool need_eval = false;    // flag of eval default value
  bool need_datum = lib::is_oracle_mode();
  if (!scan_node.is_path_evaled_ && OB_ISNULL(scan_node.path_)
      && (scan_node.node_type() == REG_TYPE
          || scan_node.node_type() == SCAN_TYPE)
      && !scan_node.col_info_.path_.empty()) {
    void* path_buf = ctx->op_exec_alloc_->alloc(sizeof(ObJsonPath));
    if (OB_ISNULL(path_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate json path buffer", K(ret));
    } else {
      js_path = new (path_buf) ObJsonPath(scan_node.col_info_.path_, ctx->op_exec_alloc_);
      if (OB_FAIL(js_path->parse_path())) {
        ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
        LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, scan_node.col_info_.path_.length(), scan_node.col_info_.path_.ptr());
      } else {
        scan_node.expr_param_.json_path_ = js_path;
        scan_node.path_ = js_path;
        scan_node.is_path_evaled_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (scan_node.node_type() == REG_TYPE) {
    if (!scan_node.is_emp_evaled_) {
      need_eval = false;
      if (scan_node.type() == COL_TYPE_VALUE) {
        if (scan_node.col_info_.on_empty_ == JSN_VALUE_IMPLICIT
            && scan_node.col_info_.on_error_ == JSN_VALUE_DEFAULT) {
          ObExpr* default_expr = ctx->spec_ptr_->err_default_exprs_.at(scan_node.col_info_.error_expr_id_);
          if (OB_FAIL(eval_default_value(ctx, default_expr, scan_node.err_val_, need_datum))) {
            ret = OB_INVALID_DEFAULT;
            LOG_USER_ERROR(OB_INVALID_DEFAULT, scan_node.col_info_.col_name_.length(), scan_node.col_info_.col_name_.ptr());
          } else {
            scan_node.is_err_evaled_ = true;
          }
        } else if (scan_node.col_info_.on_empty_ == JSN_VALUE_DEFAULT) {
          ObExpr* default_expr = ctx->spec_ptr_->emp_default_exprs_.at(scan_node.col_info_.empty_expr_id_);
          if (OB_FAIL(eval_default_value(ctx, default_expr, scan_node.emp_val_, need_datum))) {
            ret = OB_INVALID_DEFAULT;
            LOG_USER_ERROR(OB_INVALID_DEFAULT, scan_node.col_info_.col_name_.length(), scan_node.col_info_.col_name_.ptr());
          } else {
            scan_node.is_emp_evaled_ = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !scan_node.is_err_evaled_) {
      need_eval = false;
      if (scan_node.type() == COL_TYPE_VALUE) {
        if (scan_node.col_info_.on_error_ == JSN_VALUE_DEFAULT) {
          need_eval = true;
        }
        if (need_eval) {
          ObExpr* default_expr = ctx->spec_ptr_->err_default_exprs_.at(scan_node.col_info_.error_expr_id_);
          if (OB_FAIL(eval_default_value(ctx, default_expr, scan_node.err_val_, need_datum))) {
            ret = OB_INVALID_DEFAULT;
            LOG_USER_ERROR(OB_INVALID_DEFAULT, scan_node.col_info_.col_name_.length(), scan_node.col_info_.col_name_.ptr());
          } else {
            scan_node.is_err_evaled_ = true;
          }
        }
      }
    }
    scan_node.res_flag_ = ResultType::NOT_DATUM;
    ObExpr* col_expr = ctx->spec_ptr_->column_exprs_.at(scan_node.col_info_.output_column_idx_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_expr_exec_param(scan_node, ctx))) {
      LOG_WARN("fail to init expr param", K(ret));
    } else if (OB_FAIL(RegularCol::check_item_method_json(scan_node, ctx))) {
      LOG_WARN("fail to check expr param", K(ret));
    } else if (OB_FAIL(check_default_value(ctx, scan_node, col_expr))) {
      // json value empty need check default value first
      LOG_WARN("default value check fail", K(ret));
    }
  }
  return ret;
}

int JsonTableFunc::eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum)
{
  INIT_SUCC(ret);
  ObDatum* emp_datum = nullptr;
  ObIJsonBase* emp_json = nullptr;
  if (OB_FAIL(default_expr->eval(*ctx->eval_ctx_, emp_datum))) {
    LOG_WARN("failed do cast to returning type.", K(ret));
  } else if (need_datum) {
    res = emp_datum;
  } else if (OB_FAIL(RegularCol::parse_default_value_2json(default_expr, ctx, emp_datum, emp_json))) {
    LOG_WARN("fail to process empty default value", K(ret));
  } else {
    res = emp_json;
  }
  return ret;
}

int JsonTableFunc::reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value)
{
  INIT_SUCC(ret);
  if (init_flag == SCAN_NODE_TYPE) {
    ObJsonSeekResult hit;
    ObIJsonBase* in_val = static_cast<ObIJsonBase*>(in);
    ObJsonPath* js_path = static_cast<ObJsonPath*>(scan_node.path_);
    in_val->set_allocator(&ctx->row_alloc_);
    if (OB_ISNULL(in_val) || OB_ISNULL(js_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect input", K(ret));
    } else if (OB_FAIL(in_val->seek(*js_path, js_path->path_node_cnt(), true, false, hit))) {
      LOG_WARN("json seek failed", K(js_path), K(ret));
      SET_COVER_ERROR(ctx, ret);
    } else if (hit.size() == 0) {
      scan_node.cur_pos_ = 0;
      scan_node.total_ = 1;
      is_null_value = true;
      scan_node.curr_ = scan_node.iter_ = nullptr;
    } else if (hit.size() == 1) {
      scan_node.iter_ = scan_node.curr_ = hit[0];
      is_null_value = false;
      scan_node.cur_pos_ = 0;
      scan_node.total_ = 1;
    } else {
      is_null_value = false;
      void* js_arr_buf = ctx->row_alloc_.alloc(sizeof(ObJsonArray));
      ObJsonArray* js_arr_ptr = nullptr;
      if (OB_ISNULL(js_arr_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate json array buf", K(ret));
      } else if (OB_ISNULL(js_arr_ptr = new (js_arr_buf) ObJsonArray(&ctx->row_alloc_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new json array node", K(ret));
      } else {
        ObJsonNode *j_node = NULL;
        ObIJsonBase *jb_node = NULL;
        for (int32_t i = 0; OB_SUCC(ret) && i < hit.size(); i++) {
          if (ObJsonBaseFactory::transform(&ctx->row_alloc_, hit[i], ObJsonInType::JSON_TREE, jb_node)) { // to tree
            LOG_WARN("fail to transform to tree", K(ret), K(i), K(*(hit[i])));
          } else {
            j_node = static_cast<ObJsonNode *>(jb_node);
            if (OB_FAIL(js_arr_ptr->array_append(j_node->clone(&ctx->row_alloc_)))) {
              LOG_WARN("failed to array append", K(ret), K(i), K(*j_node));
            }
          }
        }

        if (OB_SUCC(ret)) {
          scan_node.curr_ = js_arr_ptr;
          scan_node.total_ = hit.size();
          ObIJsonBase* iter = NULL;
          if (OB_FAIL(js_arr_ptr->get_array_element(0, iter))) {
            LOG_WARN("failed to get array selement 0.", K(ret));
          } else {
            scan_node.iter_ = iter;
          }
        }
      }
    }
  } else {
    // do nothing in col node
  }
  return ret;
}

int JsonTableFunc::get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value)
{
  INIT_SUCC(ret);

  if (col_node.cur_pos_ + 1 < col_node.total_) {
    col_node.cur_pos_++;
    if (OB_FAIL(container_at(col_node.curr_, col_node.iter_, col_node.cur_pos_))) {
      LOG_WARN("fail to get container element.", K(ret), K(col_node.cur_pos_));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int JsonTableFunc::eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx)
{
  INIT_SUCC(ret);
  common::ObObjMeta& doc_obj_datum = ctx.spec_ptr_->value_expr_->obj_meta_;
  ObDatumMeta& doc_datum = ctx.spec_ptr_->value_expr_->datum_meta_;
  ObObjType doc_type = doc_datum.type_;
  ObCollationType doc_cs_type = doc_datum.cs_type_;
  ObString j_str;
  bool is_null = false;
  ObIJsonBase* in = NULL;

  if (doc_type == ObNullType) {
    ret = OB_ITER_END;
  } else if (doc_type == ObNCharType ||
              !(doc_type == ObJsonType
                || doc_type == ObRawType
                || ob_is_string_type(doc_type))) {
    ret = OB_ERR_INPUT_JSON_TABLE;
    LOG_WARN("fail to get json base", K(ret), K(doc_type));
  } else {
    jt.reset_columns();
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(ctx.spec_ptr_->value_expr_, eval_ctx,
                                                        ctx.row_alloc_, j_str, is_null))) {
      ret = OB_ERR_INPUT_JSON_TABLE;
      LOG_WARN("get real data failed", K(ret));
    } else if (is_null) {
      ret = OB_ITER_END;
    } else if ((ob_is_string_type(doc_type) || doc_type == ObLobType)
                && (doc_cs_type != CS_TYPE_BINARY)
                && (ObCharset::charset_type_by_coll(doc_cs_type) != CHARSET_UTF8MB4)) {
      // need convert to utf8 first, we are using GenericInsituStringStream<UTF8<> >
      char *buf = nullptr;
      const int64_t factor = 2;
      int64_t buf_len = j_str.length() * factor;
      uint32_t result_len = 0;

      if (OB_ISNULL(buf = static_cast<char*>(ctx.row_alloc_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(ObCharset::charset_convert(doc_cs_type, j_str.ptr(),
                                                    j_str.length(), CS_TYPE_UTF8MB4_BIN, buf,
                                                    buf_len, result_len))) {
        LOG_WARN("charset convert failed", K(ret));
      } else {
        ctx.is_charset_converted_ = true;
        j_str.assign_ptr(buf, result_len);
      }
    }
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(doc_type);
    ObJsonInType expect_type = ObJsonInType::JSON_TREE;
    uint32_t parse_flag = lib::is_oracle_mode() ? ObJsonParser::JSN_RELAXED_FLAG : ObJsonParser::JSN_DEFAULT_FLAG;

    // json type input, or has is json check
    bool is_ensure_json = lib::is_oracle_mode() && (doc_type != ObJsonType);


    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&ctx.row_alloc_, j_str, j_in_type, expect_type, in, parse_flag))
                || (in->json_type() != ObJsonNodeType::J_ARRAY && in->json_type() != ObJsonNodeType::J_OBJECT)) {
      if (OB_FAIL(ret) || (is_ensure_json)) {
        in= nullptr;
        ret = OB_ERR_JSON_SYNTAX_ERROR;
        SET_COVER_ERROR(&ctx, ret);
        ctx.is_need_end_ = 1;
        if (lib::is_oracle_mode() && jt.get_root_entry()->get_scan_node()->seek_node_.col_info_.on_error_ != JSN_QUERY_ERROR) {
          ret = OB_SUCCESS;
        }
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      jt.input_ = in;
    }
  }
  return ret;
}

int JsonTableFunc::container_at(void* in, void *&out, int32_t pos) {
  INIT_SUCC(ret);
  ObIJsonBase* in_ = static_cast<ObIJsonBase*>(in);
  ObIJsonBase* res = nullptr;

  if (in_->json_type() == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(in_->get_array_element(pos, res))) {
      LOG_WARN("fail to get array element", K(ret), K(pos));
    }
  } else if (in_->json_type() == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(in_->get_object_value(pos, res))) {
      LOG_WARN("fail to get object element", K(ret), K(pos));
    }
  }
  if (OB_SUCC(ret)) {
    out = res;
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase