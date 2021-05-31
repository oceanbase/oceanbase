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

#define USING_LOG_PREFIX SQL_PC
#include "ob_ps_sql_utils.h"
#include "lib/string/ob_string.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
namespace oceanbase {
using namespace common;
namespace sql {

int ObPsSqlUtils::deep_copy_str(common::ObIAllocator& allocator, const common::ObString& src, common::ObString& dst)
{
  int ret = common::OB_SUCCESS;
  int32_t size = src.length() + 1;
  char* buf = static_cast<char*>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(size), K(src));
  } else {
    MEMCPY(buf, src.ptr(), src.length());
    buf[size - 1] = '\0';
    dst.assign_ptr(buf, src.length());
  }
  return ret;
}

int ObPsSqlParamHelper::find_special_paramalize(const ParseNode& parse_node, int64_t& question_mark_count,
    ObIArray<int64_t>& no_check_type_offsets, ObIArray<int64_t>& not_param_offsets)
{
  int ret = OB_SUCCESS;
  TraverseContext ctx;
  ctx.node_ = &parse_node;
  question_mark_count = 0;

  SQL_PC_LOG(DEBUG, "traverse syntax tree for ps mode", "result_tree", SJ(ObParserResultPrintWrapper(parse_node)));
  if (OB_FAIL(traverse(ctx, no_check_type_offsets, not_param_offsets))) {
    LOG_WARN("traverse node failed", K(ret));
  } else {
    question_mark_count = ctx.question_marks_.num_members();
  }
  return ret;
}

// bool ObPsSqlParamHelper::need_visit_children(const ParseNode &node)
//{
//  bool bret = true;
//  return bret;
//}

int ObPsSqlParamHelper::traverse(
    TraverseContext& ctx, ObIArray<int64_t>& no_check_type_offsets, ObIArray<int64_t>& not_param_offsets)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.node_)) {
    // ignore
  } else {
    bool is_set = false;
    const ParseNode& parent = *ctx.node_;
    if (ObSqlParameterization::is_tree_not_param(&parent)) {
      is_set = ctx.is_child_not_param_ ? false : true;
      ctx.is_child_not_param_ = true;
    }
    if (T_QUESTIONMARK == parent.type_) {
      if (ctx.is_child_not_param_) {
        if (OB_FAIL(not_param_offsets.push_back(parent.value_))) {
          LOG_WARN("pushback offset failed", K(ctx));
        } else {
          LOG_INFO("pushback offset success", K(ctx));
        }
      }
      if (OB_SUCC(ret) && INSERT_VALUE_VECTOR_CHILD_LEVEL == ctx.insert_vector_level_) {
        // insert values (?, ?), ? do not need check type
        if (OB_FAIL(no_check_type_offsets.push_back(parent.value_))) {
          LOG_WARN("failed to push back element", K(ret));
        } else {
          LOG_DEBUG("pushback offset success", K(ret), K(parent.value_));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ctx.question_marks_.add_member(parent.value_))) {
          LOG_WARN("add member for question mark failed",
              K(parent.value_),
              K(ctx.question_marks_),
              K(ctx.question_mark_count_),
              K(ctx));
        } else {
          ctx.question_mark_count_++;
        }
      }
    }
    int32_t num_of_child = parent.num_child_;
    if (OB_SUCC(ret)) {
      if (T_VALUE_VECTOR == parent.type_) {
        ctx.insert_vector_level_ = INSERT_VALUE_VECTOR_CHILD_LEVEL;
      } else if (ctx.insert_vector_level_ >= INSERT_VALUE_VECTOR_CHILD_LEVEL) {
        ctx.insert_vector_level_++;
      }
      if (num_of_child > 0 && OB_ISNULL(parent.children_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null", K(num_of_child));
      }
      for (int i = 0; OB_SUCC(ret) && i < num_of_child; ++i) {
        if (T_OP_LIKE == parent.type_ && i > 0) {
          is_set = ctx.is_child_not_param_ ? false : true;
          ctx.is_child_not_param_ = true;
        }
        ctx.node_ = parent.children_[i];
        if (OB_FAIL(traverse(ctx, no_check_type_offsets, not_param_offsets))) {
          LOG_WARN("visit child node failed", K(i));
        }
      }
      if (ctx.insert_vector_level_ >= INSERT_VALUE_VECTOR_CHILD_LEVEL) {
        ctx.insert_vector_level_--;  // unset
      }
    }
    if (is_set) {
      ctx.is_child_not_param_ = false;  // unset
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
