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
#include "lib/timezone/ob_time_convert.h"
#include "lib/container/ob_array_serialization.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_like.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer_util.h"

// if cnd is true get full range key part which is always true
// else, get empty key part which is always false

#define GET_ALWAYS_TRUE_OR_FALSE(cnd, out_key_part)               \
  do {                                                            \
    if (OB_SUCC(ret)) {                                           \
      query_range_ctx_->cur_expr_is_precise_ = false;             \
      if (OB_ISNULL(table_graph_.key_part_head_)) {               \
        ret = OB_ERR_NULL_VALUE;                                  \
        LOG_WARN("Can not find key_part");                        \
      } else if (cnd) {                                           \
        if (OB_FAIL(alloc_full_key_part(out_key_part))) {         \
          LOG_WARN("alloc_full_key_part failed", K(ret));         \
        } else {                                                  \
          out_key_part->id_ = table_graph_.key_part_head_->id_;   \
          out_key_part->pos_ = table_graph_.key_part_head_->pos_; \
        }                                                         \
      } else {                                                    \
        if (OB_FAIL(alloc_empty_key_part(out_key_part))) {        \
          LOG_WARN("alloc_empty_key_part failed", K(ret));        \
        } else if (OB_ISNULL(out_key_part)) {                     \
          ret = OB_ALLOCATE_MEMORY_FAILED;                        \
          LOG_ERROR("out_key_part is null.", K(ret));             \
        } else {                                                  \
          out_key_part->id_ = table_graph_.key_part_head_->id_;   \
          out_key_part->pos_ = table_graph_.key_part_head_->pos_; \
        }                                                         \
      }                                                           \
    }                                                             \
  } while (0)

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
ObQueryRange::ObQueryRange()
    : state_(NEED_INIT),
      column_count_(0),
      contain_row_(false),
      inner_allocator_(ObModIds::OB_SQL_QUERY_RANGE),
      allocator_(inner_allocator_),
      query_range_ctx_(NULL),
      key_part_store_(allocator_),
      range_exprs_(allocator_),
      param_indexs_(allocator_)
{}

ObQueryRange::ObQueryRange(ObIAllocator& alloc)
    : state_(NEED_INIT),
      column_count_(0),
      contain_row_(false),
      inner_allocator_(ObModIds::OB_SQL_QUERY_RANGE),
      allocator_(alloc),
      query_range_ctx_(NULL),
      key_part_store_(allocator_),
      range_exprs_(allocator_),
      param_indexs_(allocator_)
{}

ObQueryRange::~ObQueryRange()
{
  reset();
}

ObQueryRange& ObQueryRange::operator=(const ObQueryRange& other)
{
  if (this != &other) {
    reset();
    deep_copy(other);
  }
  return *this;
}

void ObQueryRange::reset()
{
  DLIST_FOREACH_NORET(node, key_part_store_.get_obj_list())
  {
    if (node != NULL && node->get_obj() != NULL) {
      node->get_obj()->~ObKeyPart();
    }
  }
  key_part_store_.destory();
  query_range_ctx_ = NULL;
  state_ = NEED_INIT;
  column_count_ = 0;
  contain_row_ = false;
  table_graph_.reset();
  range_exprs_.reset();
  inner_allocator_.reset();
  param_indexs_.reset();
}

int ObQueryRange::init_query_range_ctx(
    ObIAllocator& allocator, const ColumnIArray& range_columns, const ParamsIArray* params)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  uint64_t table_id = OB_INVALID_ID;

  if (range_columns.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("range column array is empty");
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObQueryRangeCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc query range context failed");
  } else {
    query_range_ctx_ = new (ptr) ObQueryRangeCtx(params);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count(); ++i) {
    const ColumnItem& col = range_columns.at(i);
    if (OB_UNLIKELY(col.is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column item is invalid", K_(col.expr));
    } else {
      ObKeyPartId key_part_id(col.table_id_, col.column_id_);
      const ObExprResType* expr_res_type = col.get_column_type();
      if (OB_ISNULL(expr_res_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr result type is null", K(ret));
      } else {
        ObExprResType tmp_expr_type = *expr_res_type;
        if (tmp_expr_type.is_lob_locator()) {
          tmp_expr_type.set_type(ObLongTextType);
        }
        ObKeyPartPos key_part_pos(i, tmp_expr_type);
        table_id = (i > 0 ? table_id : col.table_id_);

        if (OB_UNLIKELY(table_id != col.table_id_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("range columns must have the same table id", K(table_id), K_(col.table_id));
        } else if (OB_FAIL(key_part_pos.set_enum_set_values(allocator_, col.expr_->get_enum_set_values()))) {
          LOG_WARN("fail to set values", K(key_part_pos), K(ret));
        } else if (OB_FAIL(query_range_ctx_->key_part_map_.set_refactored(key_part_id, key_part_pos))) {
          LOG_WARN("set key part map failed", K(ret), K(key_part_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // Add the default range of the index and remember the count of the rowkeys.
    // Just to handle the full range case
    // E.g.
    //       select * from t where true;
    ObKeyPart* full_key_part = NULL;
    if (OB_FAIL(alloc_full_key_part(full_key_part))) {
      LOG_WARN("alloc_full_key_part failed", K(ret));
    } else if (OB_ISNULL(full_key_part)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("full_key_part is null.", K(ret));
    } else {
      full_key_part->id_ = ObKeyPartId(table_id, OB_INVALID_ID);
      full_key_part->pos_ = ObKeyPartPos(allocator_, -1);
      table_graph_.key_part_head_ = full_key_part;
      column_count_ = range_columns.count();
    }
  }
  if (OB_SUCCESS != ret && NULL != query_range_ctx_) {
    destroy_query_range_ctx(allocator);
  }
  return ret;
}

void ObQueryRange::destroy_query_range_ctx(ObIAllocator& ctx_allocator)
{
  if (NULL != query_range_ctx_) {
    query_range_ctx_->~ObQueryRangeCtx();
    ctx_allocator.free(query_range_ctx_);
    query_range_ctx_ = NULL;
  }
}

int ObQueryRange::preliminary_extract_query_range(const ColumnIArray& range_columns, const ObRawExpr* expr_root,
    const ObDataTypeCastParams& dtc_params, const ParamsIArray* params /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator ctx_allocator(ObModIds::OB_QUERY_RANGE_CTX);
  if (OB_FAIL(init_query_range_ctx(ctx_allocator, range_columns, params))) {
    LOG_WARN("init query range context failed", K(ret));
  } else if (OB_ISNULL(query_range_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_range_ctx_ is not inited.", K(ret));
  } else {
    query_range_ctx_->need_final_extact_ = false;
    ObKeyPart* root = NULL;
    if (OB_UNLIKELY(NULL == expr_root)) {
      //(MIN, MAX), whole range
      GET_ALWAYS_TRUE_OR_FALSE(true, root);
    } else {
      if (OB_FAIL(preliminary_extract(expr_root, root, dtc_params, T_OP_IN == expr_root->get_expr_type()))) {
        LOG_WARN("gen table range failed", K(ret));
      } else if (query_range_ctx_->cur_expr_is_precise_ && root != NULL) {
        // for simple in_expr
        int64_t max_pos = -1;
        bool is_strict_equal = true;
        if (OB_FAIL(is_strict_equal_graph(root, 0, max_pos, is_strict_equal))) {
          LOG_WARN("is strict equal graph failed", K(ret));
        } else if (is_strict_equal) {
          ObRangeExprItem expr_item;
          expr_item.cur_expr_ = expr_root;
          for (const ObKeyPart* cur_and = root; OB_SUCC(ret) && cur_and != NULL; cur_and = cur_and->and_next_) {
            if (OB_FAIL(expr_item.cur_pos_.push_back(cur_and->pos_.offset_))) {
              LOG_WARN("push back pos offset failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(query_range_ctx_->precise_range_exprs_.push_back(expr_item))) {
            LOG_WARN("store precise range exprs failed", K(ret));
          }
        } else if (NULL == root->and_next_ && is_general_graph(*root)) {
          // Because the optimizer can only remove the top filter,
          // and the goal of remove filter is to extract conjunctive paradigm
          // the standard conjunctive normal form such as (c1>1 or c1<0) and c2=1 expressions
          // must be split into multiple expressions in the resolver
          // if there is no split, such expressions cannot be removed,
          // because the removed expression must be a complete expression every time
          ObRangeExprItem expr_item;
          expr_item.cur_expr_ = expr_root;
          if (OB_FAIL(expr_item.cur_pos_.push_back(root->pos_.offset_))) {
            LOG_WARN("push back pos offset failed", K(ret));
          } else if (OB_FAIL(query_range_ctx_->precise_range_exprs_.push_back(expr_item))) {
            LOG_WARN("store precise range exprs failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(normalize_range_graph(root))) {
        LOG_WARN("normalize range graph failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && root != NULL) {
      SQL_REWRITE_LOG(DEBUG, "root key part", K(*root));
      int64_t max_pos = -1;
      table_graph_.key_part_head_ = root;
      table_graph_.is_standard_range_ = is_standard_graph(root);
      OZ(is_strict_equal_graph(root, 0, max_pos, table_graph_.is_equal_range_));
      OZ(check_graph_type());
    }
  }
  if (OB_SUCC(ret)) {
    if (query_range_ctx_->need_final_extact_) {
      state_ = NEED_PREPARE_PARAMS;
    } else {
      state_ = CAN_READ;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(param_indexs_.assign(query_range_ctx_->param_indexs_))) {
      LOG_WARN("assign param indexs failed", K(ret), K(query_range_ctx_->param_indexs_));
    }
  }
  destroy_query_range_ctx(ctx_allocator);
  return ret;
}

int ObQueryRange::preliminary_extract_query_range(const ColumnIArray& range_columns, const ExprIArray& root_exprs,
    const ObDataTypeCastParams& dtc_params, const ParamsIArray* params /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObKeyPartList and_ranges;
  ObKeyPart* temp_result = NULL;

  SQL_REWRITE_LOG(DEBUG, "preliminary extract", K(range_columns), K(root_exprs));
  ObArenaAllocator ctx_allocator(ObModIds::OB_QUERY_RANGE_CTX);
  if (OB_FAIL(init_query_range_ctx(ctx_allocator, range_columns, params))) {
    LOG_WARN("init query range context failed", K(ret));
  } else if (OB_ISNULL(query_range_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_range_ctx_ is not inited.", K(ret));
  } else {
    if (OB_LIKELY(root_exprs.count() > 0)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < root_exprs.count(); ++j) {
        if (NULL == root_exprs.at(j)) {
          // continue
        } else if (OB_FAIL(preliminary_extract(
                       root_exprs.at(j), temp_result, dtc_params, T_OP_IN == root_exprs.at(j)->get_expr_type()))) {
          LOG_WARN("Generate table range failed", K(ret));
        } else if (NULL == temp_result) {
          // ignore the condition from which we can not extract key part range
        } else if (!and_ranges.add_last(temp_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add key part range failed", K(ret));
        } else if (query_range_ctx_->cur_expr_is_precise_ && temp_result != NULL) {
          if (is_strict_in_graph(temp_result)) {
            ObRangeExprItem expr_item;
            expr_item.cur_expr_ = root_exprs.at(j);
            for (const ObKeyPart* cur_and = temp_result; OB_SUCC(ret) && cur_and != NULL;
                 cur_and = cur_and->and_next_) {
              if (OB_FAIL(expr_item.cur_pos_.push_back(cur_and->pos_.offset_))) {
                LOG_WARN("push back pos offset failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(query_range_ctx_->precise_range_exprs_.push_back(expr_item))) {
              LOG_WARN("store precise range exprs failed", K(ret));
            }
          } else if (NULL == temp_result->and_next_ && is_general_graph(*temp_result)) {
            ObRangeExprItem expr_item;
            expr_item.cur_expr_ = root_exprs.at(j);
            if (OB_FAIL(expr_item.cur_pos_.push_back(temp_result->pos_.offset_))) {
              LOG_WARN("push back pos offset failed", K(ret));
            } else if (OB_FAIL(query_range_ctx_->precise_range_exprs_.push_back(expr_item))) {
              LOG_WARN("store precise range exprs failed", K(ret));
            }
          }
        }
      }  // for each where condition
    } else {
      GET_ALWAYS_TRUE_OR_FALSE(true, temp_result);
      if (OB_SUCC(ret)) {
        if (!and_ranges.add_last(temp_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add key part range failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(and_range_graph(and_ranges, temp_result))) {
      LOG_WARN("And query range failed", K(ret));
    } else if (OB_FAIL(normalize_range_graph(temp_result))) {
      LOG_WARN("normalize range graph failed", K(ret));
    } else if (NULL == temp_result) {
      // no range left
    } else {
      int64_t max_pos = -1;
      table_graph_.key_part_head_ = temp_result;
      table_graph_.is_standard_range_ = is_standard_graph(temp_result);
      if (OB_FAIL(is_strict_equal_graph(temp_result, 0, max_pos, table_graph_.is_equal_range_))) {
        LOG_WARN("is strict equal graph failed", K(ret));
      } else if (OB_FAIL(check_graph_type())) {
        LOG_WARN("check graph type failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (query_range_ctx_->need_final_extact_) {
      state_ = NEED_PREPARE_PARAMS;
    } else {
      state_ = CAN_READ;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(param_indexs_.assign(query_range_ctx_->param_indexs_))) {
      LOG_WARN("assign param indexs failed", K(ret), K(query_range_ctx_->param_indexs_));
    }
  }
  destroy_query_range_ctx(ctx_allocator);
  return ret;
}

int ObQueryRange::is_get(bool& is_range_get) const
{
  return is_get(column_count_, is_range_get);
}

int ObQueryRange::is_get(int64_t column_count, bool& is_range_get) const
{
  int ret = OB_SUCCESS;
  is_range_get = true;
  if (table_graph_.is_precise_get_) {
    // return true
  } else if (NULL == table_graph_.key_part_head_) {
    is_range_get = false;
  } else if (OB_FAIL(check_is_get(*table_graph_.key_part_head_, 0, column_count, is_range_get))) {
    LOG_WARN("failed to check is get", K(ret));
  }
  return ret;
}

int ObQueryRange::check_is_get(ObKeyPart& key_part, const int64_t depth, const int64_t column_count, bool& bret) const
{
  int ret = OB_SUCCESS;
  if (key_part.pos_.offset_ != depth || !key_part.is_equal_condition()) {
    bret = false;
  } else {
    if (NULL != key_part.and_next_) {
      ret = SMART_CALL(check_is_get(*key_part.and_next_, depth + 1, column_count, bret));
    } else if (depth < column_count - 1) {
      bret = false;
    }
    if (OB_SUCC(ret) && bret && NULL != key_part.or_next_) {
      ret = SMART_CALL(check_is_get(*key_part.or_next_, depth, column_count, bret));
    }
  }
  return ret;
}

int ObQueryRange::check_graph_type()
{
  int ret = OB_SUCCESS;
  table_graph_.is_precise_get_ = true;
  if (OB_ISNULL(query_range_ctx_) || OB_ISNULL(table_graph_.key_part_head_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("query isn't init", K_(query_range_ctx), K_(table_graph_.key_part_head));
  }
  if (OB_SUCC(ret)) {
    int64_t depth = -1;
    int64_t column_count = column_count_;
    bool is_terminated = false;
    for (ObKeyPart* cur = table_graph_.key_part_head_; !is_terminated && NULL != cur; cur = cur->and_next_) {
      if (cur->pos_.offset_ != (++depth)) {
        table_graph_.is_precise_get_ = false;
        // there is a missing key, the missing key and the following keys are invalid
        if (OB_FAIL(remove_precise_range_expr(depth))) {
          LOG_WARN("remove precise range expr failed", K(ret));
        }
        is_terminated = true;
      } else if (NULL != cur->or_next_ || NULL != cur->item_next_) {
        table_graph_.is_precise_get_ = false;
      } else if (cur->is_like_key()) {
        table_graph_.is_precise_get_ = false;
      } else if (!cur->is_equal_condition()) {
        table_graph_.is_precise_get_ = false;
      } else {
        // do nothing
      }
      if (OB_SUCC(ret) && !is_terminated) {
        if (is_strict_in_graph(cur)) {
          // do nothing
        } else if (!is_general_graph(*cur)) {
          if (OB_FAIL(remove_precise_range_expr(cur->pos_.offset_ + 1))) {
            LOG_WARN("remove precise range expr failed", K(ret));
          }
          is_terminated = true;
        } else if (has_scan_key(*cur)) {
          if (OB_FAIL(remove_precise_range_expr(cur->pos_.offset_ + 1))) {
            LOG_WARN("remove precise range expr failed", K(ret));
          }
          is_terminated = true;
        }
      }
    }
    if (OB_SUCC(ret) && table_graph_.is_precise_get_ && depth != column_count - 1) {
      table_graph_.is_precise_get_ = false;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(range_exprs_.init(query_range_ctx_->precise_range_exprs_.count()))) {
    LOG_WARN("init range exprs failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_range_ctx_->precise_range_exprs_.count(); ++i) {
    const ObRawExpr* cur_expr = query_range_ctx_->precise_range_exprs_.at(i).cur_expr_;
    if (NULL != cur_expr) {
      if (OB_FAIL(range_exprs_.push_back(const_cast<ObRawExpr*>(cur_expr)))) {
        LOG_WARN("push back precise range expr failed", K(ret));
      }
    }
  }
  return ret;
}

// this function is used to determine
// whether the column type of the expression to be extracted is compatible with the comparison type of the expression,
// then judge whether the expression should be extracted accurately or enlarged to (min, max)
bool ObQueryRange::can_be_extract_range(ObItemType cmp_type, const ObExprResType& col_type,
    const ObExprCalcType& calc_type, ObObjType data_type, bool& always_true)
{
  bool bret = true;
  always_true = true;
  /**
   * the prerequisite for determining whether an expression can use our extraction rules is
   * whether the range of the collection after extraction is smaller than the value range expressed by the expression.
   * for an expression(col compare const), the comparison type(calc_type) contain that a set A
   * (the element data type is calc_type),
   * the query range needs to find a set B (the element type is column_type) is included by A,
   * in this way, the column range extracted by the query range can satisfy this expression.
   * Set B is determined by query range through calc_type and column_type and expression extraction rules.
   * the extraction rules must be one-to-one, whether it is type conversion or character set conversion.
   * For example, int->varchar, 123 is converted to '123', not '0123',
   * character set UTF8MB4_GENERAL_CI'A'->UTF8MB4_BIN'A' instead of UTF8MB4_BIN'a'.
   * So to satisfy the expression relationship(col compare const),
   * if column_type and calc_type are not compatible, need to convert the data type.
   * This expression relationship can finally be summarized as: f1(col, calc_type) compare f2(const, calc_type)
   * f1 means that the value of col is mapped from column_type to calc_type,
   * so to discuss the relationship between set A and set B is to discuss the mapping relationship of f1.
   * The factors that affect the range of the collection may be the type and character set,
   * and the character set is meaningful only in the string type
   * The first case:
   * if the type of column_type is same with calc_type and the character set is also the same (if it is a string type),
   * and then f1 is a one-to-one mapping relationship, that is set A = set B
   * The second case:
   * if the character set in column_type is a case-sensitive character set,
   * and the character set in calc_type is a case-insensitive character set,
   * then f1 is a one-to-many relationship.
   * !f1 is a many-to-one relationship, such as general_ci'A'-> bin'a' or bin'A',
   * then Set B extracted by the query range rule is included in Set A, which does not satisfy the assumption.
   * So in this case, the range is (min, max).
   * The third case:
   * if column_type is a string type, and calc_type is a non-string type,
   * because their collation and conversion rules are different, f1 is a many-to-one relationship,
   * such as '0123'->123, '123'->123, then !f1 is a one-to-many relationship, which does not satisfy the assumption,
   * so in this case the range is (min, max).
   * The fourth case:
   * if column_type is a numeric type, and calc_type is a string type,
   * we can see from the third case that the mapping f1 from numeric type to string type is one-to-many relationship,
   * so !f1 is a many-to-one relationship, and the extraction rule of query range is a one-to-one mapping relationship,
   * any element a belonging to set A can uniquely determine a value b to make the expression true in set B,
   * so the fourth case can also be extracted by extraction rules
   * f1 in other cases is also a one-to-one mapping relationship, set A = set B,
   * so the extraction rules can also be used
   **/
  if (T_OP_NSEQ == cmp_type && ObNullType == data_type) {
    bret = true;
  }
  if (bret && T_OP_NSEQ != cmp_type && ObNullType == data_type) {
    // pk cmp null
    bret = false;
    always_true = false;
  }
  if (bret && T_OP_LIKE == cmp_type) {
    if ((!col_type.is_string_or_lob_locator_type()) || (!calc_type.is_string_or_lob_locator_type())) {
      bret = false;
      always_true = true;
    }
  }
  if (bret) {
    bool is_cast_monotonic = false;
    int ret = OB_SUCCESS;
    // because cast has special processing for certain value ranges of certain time types,
    // resulting in A cast B, which is not necessarily reversible.
    // an expression can be extracted, and it needs to be cast monotonous in both directions
    if (OB_FAIL(ObObjCaster::is_cast_monotonic(col_type.get_type(), calc_type.get_type(), is_cast_monotonic))) {
      LOG_WARN("check is cast monotonic failed", K(ret));
    } else if (!is_cast_monotonic) {
      bret = false;
      always_true = true;
    } else if (OB_FAIL(ObObjCaster::is_cast_monotonic(calc_type.get_type(), col_type.get_type(), is_cast_monotonic))) {
      LOG_WARN("check is cast monotonic failed", K(ret));
    } else if (!is_cast_monotonic) {
      bret = false;
      always_true = true;
    } else if (calc_type.is_string_or_lob_locator_type() && col_type.is_string_or_lob_locator_type()) {
      if (col_type.get_collation_type() != calc_type.get_collation_type()) {
        bret = false;
        always_true = true;
      }
    }
  }
  return bret;
}
int ObQueryRange::get_const_key_part(const ObRawExpr* l_expr, const ObRawExpr* r_expr, const ObRawExpr* escape_expr,
    ObItemType cmp_type, const ObExprResType& result_type, ObKeyPart*& out_key_part,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || (OB_ISNULL(escape_expr) && T_OP_LIKE == cmp_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(l_expr), KP(r_expr));
  } else {
    const ObConstRawExpr* l_const = static_cast<const ObConstRawExpr*>(l_expr);
    const ObConstRawExpr* r_const = static_cast<const ObConstRawExpr*>(r_expr);
    const ObExprCalcType& calc_type = result_type.get_calc_meta();
    ObCollationType cmp_cs_type = calc_type.get_collation_type();
    // '?' is const too, if " '?' cmp const ", we seem it as true now
    if (l_expr->has_flag(IS_PARAM) || r_expr->has_flag(IS_PARAM)) {
      GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
    } else if (l_const->get_value().is_null() || r_const->get_value().is_null()) {
      if (l_const->get_value().is_null() && r_const->get_value().is_null() && T_OP_NSEQ == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
      } else {
        GET_ALWAYS_TRUE_OR_FALSE(false, out_key_part);
      }
    } else if (cmp_type >= T_OP_EQ && cmp_type <= T_OP_NE) {
      ObObjType compare_type = ObMaxType;
      int64_t eq_cmp = 0;
      ObCastMode cast_mode = CM_WARN_ON_FAIL;
      ObCastCtx cast_ctx(&allocator_, &dtc_params, cast_mode, cmp_cs_type);
      if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(
              compare_type, l_const->get_value().get_type(), r_const->get_value().get_type()))) {
        LOG_WARN("get compare type failed", K(ret));
      } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(
                     eq_cmp, l_const->get_value(), r_const->get_value(), cast_ctx, compare_type, cmp_cs_type))) {
        LOG_WARN("compare obj failed", K(ret));
      } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(0 == eq_cmp, out_key_part);
      } else if (T_OP_LE == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(eq_cmp <= 0, out_key_part);
      } else if (T_OP_LT == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(eq_cmp < 0, out_key_part);
      } else if (T_OP_GE == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(eq_cmp >= 0, out_key_part);
      } else if (T_OP_GT == cmp_type) {
        GET_ALWAYS_TRUE_OR_FALSE(eq_cmp > 0, out_key_part);
      } else {
        GET_ALWAYS_TRUE_OR_FALSE(0 != eq_cmp, out_key_part);
      }
    } else if (T_OP_LIKE == cmp_type) {
      if (!escape_expr->is_const_expr()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("escape_expr must be const expr", K(ret));
      } else {
        const ObConstRawExpr* const_escape = static_cast<const ObConstRawExpr*>(escape_expr);
        if (OB_FAIL(get_like_const_range(l_const, r_const, const_escape, cmp_cs_type, out_key_part, dtc_params))) {
          LOG_WARN("get like const range failed", K(ret));
        }
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObQueryRange::get_column_key_part(const ObRawExpr* l_expr, const ObRawExpr* r_expr, const ObRawExpr* escape_expr,
    ObItemType cmp_type, const ObExprResType& result_type, ObKeyPart*& out_key_part,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || (OB_ISNULL(escape_expr) && T_OP_LIKE == cmp_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(l_expr), KP(r_expr), KP(cmp_type));
  } else {
    const ObColumnRefRawExpr* column_item = NULL;
    const ObConstRawExpr* const_item = NULL;
    const ObRawExpr* const_expr = NULL;
    const ObExprCalcType& calc_type = result_type.get_calc_meta();
    ObItemType c_type = cmp_type;
    if (OB_UNLIKELY(r_expr->has_flag(IS_COLUMN))) {
      column_item = static_cast<const ObColumnRefRawExpr*>(r_expr);
      const_item = static_cast<const ObConstRawExpr*>(l_expr);
      const_expr = l_expr;
      c_type = (T_OP_LE == cmp_type
                    ? T_OP_GE
                    : (T_OP_GE == cmp_type
                              ? T_OP_LE
                              : (T_OP_LT == cmp_type ? T_OP_GT : (T_OP_GT == cmp_type ? T_OP_LT : cmp_type))));
    } else if (l_expr->has_flag(IS_COLUMN)) {
      column_item = static_cast<const ObColumnRefRawExpr*>(l_expr);
      const_item = static_cast<const ObConstRawExpr*>(r_expr);
      const_expr = r_expr;
      c_type = cmp_type;
    }
    if (const_expr->has_flag(IS_PARAM)) {
      if (T_OP_LIKE != c_type || NULL == query_range_ctx_->params_) {
        // 1. non like condition
        // 2. like condition and params_ is NULL
        query_range_ctx_->need_final_extact_ = true;
      } else {
        // like condition and params_ NOT NULL
      }
    }
    ObKeyPartId key_part_id(column_item->get_table_id(), column_item->get_column_id());
    ObKeyPartPos key_part_pos(allocator_);
    bool b_is_key_part = false;
    bool always_true = true;
    if (OB_FAIL(is_key_part(key_part_id, key_part_pos, b_is_key_part))) {
      LOG_WARN("is_key_part failed", K(ret));
    } else if (!b_is_key_part) {
      GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
    } else if (!can_be_extract_range(cmp_type,
                   key_part_pos.column_type_,
                   calc_type,
                   const_expr->get_result_type().get_type(),
                   always_true)) {
      GET_ALWAYS_TRUE_OR_FALSE(always_true, out_key_part);
    } else if (OB_ISNULL((out_key_part = create_new_key_part()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      ObObj val;
      out_key_part->id_ = key_part_id;
      out_key_part->pos_ = key_part_pos;
      out_key_part->null_safe_ = (T_OP_NSEQ == c_type);
      if (NULL != const_item) {
        // none-ObConstRawExpr does not have value
        val = const_item->get_value();
        if (val.is_unknown()) {
          if (T_OP_LIKE != c_type || NULL == query_range_ctx_->params_) {
            // push down? no need to process it
            if (!const_expr->has_flag(IS_EXEC_PARAM) &&
                OB_FAIL(add_var_to_array_no_dup(query_range_ctx_->param_indexs_, val.get_unknown()))) {
              LOG_WARN("store param index in query range context failed", K(ret), K(val));
            }
          } else if (OB_FAIL(get_param_value(val, *query_range_ctx_->params_))) {
            LOG_WARN("failed to get param value", K(ret));
          }
        }
      }
      // if current expr can be extracted to range, just store the expr
      if (OB_SUCC(ret)) {
        if (c_type != T_OP_LIKE) {
          if (OB_FAIL(get_normal_cmp_keypart(c_type, val, *out_key_part))) {
            LOG_WARN("get normal cmp keypart failed", K(ret));
          }
        } else {
          const ObConstRawExpr* const_escape = static_cast<const ObConstRawExpr*>(escape_expr);
          if ((const_expr->has_flag(IS_PARAM) || escape_expr->has_flag(IS_PARAM)) &&
              NULL == query_range_ctx_->params_) {
            if (OB_FAIL(out_key_part->create_like_key())) {
              LOG_WARN("create like key part failed", K(ret));
            } else if (OB_FAIL(
                           ob_write_obj(allocator_, const_item->get_value(), out_key_part->like_keypart_->pattern_))) {
              LOG_WARN("deep copy pattern obj failed", K(ret));
            } else if (OB_FAIL(
                           ob_write_obj(allocator_, const_escape->get_value(), out_key_part->like_keypart_->escape_))) {
              LOG_WARN("deep copy escape obj failed", K(ret));
            }
          } else {
            ObObj escape_val = const_escape->get_value();
            if (escape_val.is_unknown() && OB_ISNULL(query_range_ctx_->params_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (escape_val.is_unknown() && OB_FAIL(get_param_value(val, *query_range_ctx_->params_))) {
              LOG_WARN("failed to get param value", K(ret));
            } else if (OB_FAIL(get_like_range(val, escape_val, *out_key_part, dtc_params))) {
              LOG_WARN("get like range failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && out_key_part->is_normal_key() && !out_key_part->is_question_mark()) {
          if (OB_FAIL(out_key_part->cast_value_type(dtc_params, contain_row_))) {
            LOG_WARN("cast keypart value type failed", K(ret));
          } else {
            // do nothing
          }
        }
        if (OB_SUCC(ret) && key_part_pos.column_type_.is_string_type() && calc_type.is_string_type()) {
          if (CS_TYPE_UTF8MB4_GENERAL_CI == key_part_pos.column_type_.get_collation_type() &&
              CS_TYPE_UTF8MB4_GENERAL_CI != calc_type.get_collation_type()) {
            // in this case, unify the character set into the character set of the target column,
            // use general ci to compare, because the character set of calc type is general bin
            // Because the general bin is converted to general ci, there is many-to-one,
            // so the obtained range may be enlarged, not an accurate range
            query_range_ctx_->cur_expr_is_precise_ = false;
          }
        }
        if (OB_SUCC(ret) && is_oracle_mode() && out_key_part->is_normal_key() && NULL != const_expr) {
          // c1 char(5), c2 varchar(5) for the value'abc', c1 ='abc', c2 ='abc'
          //'abc' in oracle mode !='abc', but c1 = cast('abc' as varchar2(5))
          // The range (abc; abc) will be extracted,
          // because the storage layer stores strings without padding spaces,
          // so this range will cause'abc'
          // was selected, so this range is not an accurate range
          ObObjType column_type = key_part_pos.column_type_.get_type();
          ObObjType const_type = const_expr->get_result_type().get_type();
          if ((ObCharType == column_type && ObVarcharType == const_type) ||
              (ObNCharType == column_type && ObNVarchar2Type == const_type)) {
            query_range_ctx_->cur_expr_is_precise_ = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::get_normal_cmp_keypart(ObItemType cmp_type, const ObObj& val, ObKeyPart& out_keypart) const
{
  int ret = OB_SUCCESS;
  // precise range expr
  if (OB_ISNULL(query_range_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("query range context is null");
  } else if (OB_FAIL(out_keypart.create_normal_key())) {
    LOG_WARN("create normal key failed", K(ret));
  } else if (OB_ISNULL(out_keypart.normal_keypart_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("normal keypart is null");
  } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
    out_keypart.normal_keypart_->include_start_ = true;
    out_keypart.normal_keypart_->include_end_ = true;
    if (share::is_mysql_mode() &&
        (out_keypart.pos_.column_type_.is_datetime() || out_keypart.pos_.column_type_.is_date()) &&
        out_keypart.pos_.column_type_.is_not_null() && T_OP_NSEQ == cmp_type && val.is_null()) {
      ObObj tmp_val;
      tmp_val.set_meta_type(out_keypart.pos_.column_type_);
      if (out_keypart.pos_.column_type_.is_datetime()) {
        tmp_val.set_datetime(ObTimeConverter::ZERO_DATETIME);
      } else if (out_keypart.pos_.column_type_.is_date()) {
        tmp_val.set_date(ObTimeConverter::ZERO_DATE);
      }
      out_keypart.normal_keypart_->start_ = tmp_val;
      out_keypart.normal_keypart_->end_ = tmp_val;
    } else {
      // normal action
      out_keypart.normal_keypart_->start_ = val;
      out_keypart.normal_keypart_->end_ = val;
    }
  } else if (T_OP_LE == cmp_type || T_OP_LT == cmp_type) {
    // index order in storage is, Null is greater than min, less than the value of any meaningful
    // c1 < val doesn't contain Null -> (NULL, val)
    if (share::is_oracle_mode()) {
      out_keypart.normal_keypart_->start_.set_min_value();
    } else {
      out_keypart.normal_keypart_->start_.set_null();
    }
    out_keypart.normal_keypart_->end_ = val;
    out_keypart.normal_keypart_->include_start_ = false;
    out_keypart.normal_keypart_->include_end_ = (T_OP_LE == cmp_type);
  } else if (T_OP_GE == cmp_type || T_OP_GT == cmp_type) {
    out_keypart.normal_keypart_->start_ = val;
    if (share::is_oracle_mode()) {
      out_keypart.normal_keypart_->end_.set_null();
    } else {
      out_keypart.normal_keypart_->end_.set_max_value();
    }
    out_keypart.normal_keypart_->include_start_ = (T_OP_GE == cmp_type);
    out_keypart.normal_keypart_->include_end_ = false;
  }
  if (OB_SUCC(ret)) {
    query_range_ctx_->cur_expr_is_precise_ = true;
    out_keypart.normal_keypart_->always_false_ = false;
    out_keypart.normal_keypart_->always_true_ = false;
  }
  return ret;
}

int ObQueryRange::get_row_key_part(const ObRawExpr* l_expr, const ObRawExpr* r_expr, ObItemType cmp_type,
    const ObExprResType& result_type, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", KP(l_expr), KP(r_expr), K_(query_range_ctx));
  } else {
    bool row_is_precise = true;
    if (T_OP_EQ != cmp_type) {
      contain_row_ = true;
      row_is_precise = false;
    }
    ObKeyPartList key_part_list;
    ObKeyPart* row_tail = out_key_part;
    // resolver makes sure the syntax right, so we don't concern whether the numbers of row are equal
    const ObOpRawExpr* l_row = static_cast<const ObOpRawExpr*>(l_expr);
    const ObOpRawExpr* r_row = static_cast<const ObOpRawExpr*>(r_expr);
    int64_t num = 0;
    num = l_row->get_param_count() <= r_row->get_param_count() ? l_row->get_param_count() : r_row->get_param_count();

    ObItemType c_type = T_INVALID;
    switch (cmp_type) {
      case T_OP_LT:
      case T_OP_LE:
        c_type = T_OP_LE;
        break;
      case T_OP_GT:
      case T_OP_GE:
        c_type = T_OP_GE;
        break;
      default:
        // Other compare types are passed on without changes.
        // In the vector, the processing logic of T_OP_EQ and T_OP_NSEQ is the same as that of ordinary conditions.
        // T_OP_NE extracting range is meaningless,
        // and the compare type cannot be changed,
        // so that the lower layer can judge and ignore such row compare
        // The compare type of the subquery also passes the original compare type,
        // so that the lower-level judgment interface can ignore the compare expression of the subquery
        c_type = cmp_type;
        break;
    }
    bool b_flag = false;
    ObArenaAllocator alloc;
    ObExprResType res_type(alloc);
    ObKeyPart* tmp_key_part = NULL;
    for (int i = 0; OB_SUCC(ret) && !b_flag && i < num; ++i) {
      res_type.set_calc_meta(result_type.get_row_calc_cmp_types().at(i));
      tmp_key_part = NULL;
      if (OB_FAIL(get_basic_query_range(l_row->get_param_expr(i),
              r_row->get_param_expr(i),
              NULL,
              i < num - 1 ? c_type : cmp_type,
              res_type,
              tmp_key_part,
              dtc_params))) {
        LOG_WARN("Get basic query key part failed", K(ret), K(*l_row), K(*r_row), K(c_type));
      } else if (T_OP_ROW == l_row->get_param_expr(i)->get_expr_type() ||
                 T_OP_ROW == r_row->get_param_expr(i)->get_expr_type()) {
        // ((a,b),(c,d)) = (((1,2),(2,3)),((1,2),(2,3)))
        row_is_precise = false;
      } else if (T_OP_EQ == cmp_type || T_OP_NSEQ == cmp_type) {
        row_is_precise = (row_is_precise && query_range_ctx_->cur_expr_is_precise_);
        if (OB_FAIL(add_and_item(key_part_list, tmp_key_part))) {
          LOG_WARN("Add basic query key part failed", K(ret));
        } else if (num - 1 == i) {
          if (OB_FAIL(and_range_graph(key_part_list, out_key_part))) {
            LOG_WARN("and basic query key part failed", K(ret));
          } else {
            b_flag = true;
          }
        }
      } else if (OB_FAIL(add_row_item(row_tail, tmp_key_part))) {
        LOG_WARN("Add basic query key part failed", K(ret));
      } else {
        if (NULL == out_key_part) {
          out_key_part = tmp_key_part;
        }
        if (NULL == tmp_key_part || tmp_key_part->is_always_false()) {
          // when find false key part, then no need to do next
          // E.g. (10, c1) > (5, 0)
          if (NULL == out_key_part) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("out_key_part is null", K(ret));
          }
          b_flag = true;  // break
        } else {
          row_tail = tmp_key_part;
        }
      }
    }
    if (OB_SUCC(ret)) {
      query_range_ctx_->cur_expr_is_precise_ = row_is_precise;
    }
  }
  return ret;
}

// Get range from basic compare expression, like 'col >= 30', 'row(c1, c2) > row(1, 2)'
//  if this compare expression is not kinds of that we can use,
//  return alway true key part, because it may be in OR expression
//  E.g.
//  case 1:
//         key1 > 0 and (key2 < 5 or not_key3 >0)
//         currently, we get range from 'not_key3 >0', if we do not generate  always true key part for it,
//         the result of (key2 < 5 or not_key3 >0) will be 'key2 belongs (min, 5)'
//  case 2:
//         key1 > 0 and (key2 < 5 or key1+key2 >0)
//  case 3:
//         key1 > 0 and (key2 < 5 or func(key1) >0)

int ObQueryRange::get_basic_query_range(const ObRawExpr* l_expr, const ObRawExpr* r_expr, const ObRawExpr* escape_expr,
    ObItemType cmp_type, const ObExprResType& result_type, ObKeyPart*& out_key_part,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  out_key_part = NULL;
  if (OB_ISNULL(query_range_ctx_) || OB_ISNULL(l_expr) || OB_ISNULL(r_expr) ||
      (OB_ISNULL(escape_expr) && T_OP_LIKE == cmp_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Wrong input params to get basic query range",
        K(ret),
        KP(query_range_ctx_),
        KP(l_expr),
        KP(r_expr),
        KP(escape_expr),
        K(cmp_type));
  } else if ((T_OP_ROW == l_expr->get_expr_type() && T_OP_ROW != r_expr->get_expr_type()) ||
             (T_OP_ROW != l_expr->get_expr_type() && T_OP_ROW == r_expr->get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Row must compare to row", K(ret));
  } else {
    query_range_ctx_->cur_expr_is_precise_ = false;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (IS_BASIC_CMP_OP(cmp_type)) {
    if (T_OP_ROW != l_expr->get_expr_type()) {  // 1. unary compare
      bool l_is_lossless = false;
      bool r_is_lossless = false;
      if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(l_expr, l_is_lossless))) {
        LOG_WARN("failed to check l_expr is lossless column cast", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(r_expr, r_is_lossless))) {
        LOG_WARN("failed to check l_expr is lossless column cast", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (l_is_lossless) {
          l_expr = l_expr->get_param_expr(0);
        }
        if (r_is_lossless) {
          r_expr = r_expr->get_param_expr(0);
        }
        if (l_expr->is_const_expr() && r_expr->is_const_expr()) {  // const
          if (OB_FAIL(
                  get_const_key_part(l_expr, r_expr, escape_expr, cmp_type, result_type, out_key_part, dtc_params))) {
            LOG_WARN("get const key part failed.", K(ret));
          }
        } else if ((l_expr->has_flag(IS_COLUMN) && (r_expr->is_const_expr() || r_expr->has_flag(IS_PARAM))) ||
                   ((l_expr->is_const_expr() || l_expr->has_flag(IS_PARAM)) && r_expr->has_flag(IS_COLUMN) &&
                       T_OP_LIKE != cmp_type)) {
          if (OB_FAIL(get_column_key_part(
                  l_expr, r_expr, escape_expr, cmp_type, result_type, out_key_part, dtc_params))) {  // column
            LOG_WARN("get column key part failed.", K(ret));
          }
        } else if (l_expr->has_flag(IS_COLUMN) && r_expr->has_flag(IS_COLUMN)) {
          GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
        } else {
          GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
        }
      }
    } else if (OB_FAIL(get_row_key_part(
                   l_expr, r_expr, cmp_type, result_type, out_key_part, dtc_params))) {  // 2. row compare
      LOG_WARN("get row key part failed.", K(ret));
    }
  } else {
    // we can not extract range from this type, return all
    GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
  }

  return ret;
}

int ObQueryRange::get_like_const_range(const ObConstRawExpr* text_expr, const ObConstRawExpr* pattern_expr,
    const ObConstRawExpr* escape_expr, ObCollationType cmp_cs_type, ObKeyPart*& out_key_part,
    const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(text_expr) || OB_ISNULL(pattern_expr) || OB_ISNULL(escape_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(text_expr), K(pattern_expr), K(escape_expr));
  } else {
    const ObObj& text = text_expr->get_value();
    const ObObj& pattern = pattern_expr->get_value();
    const ObObj& escape = escape_expr->get_value();
    ObString escape_str;
    ObString text_str;
    ObString pattern_str;
    if (escape.is_null()) {
      escape_str.assign_ptr("\\", 1);
    } else if (ObVarcharType != escape.get_type()) {
      ObObj tmp_obj = escape;
      tmp_obj.set_scale(escape_expr->get_result_type().get_scale());
      ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, cmp_cs_type);
      EXPR_GET_VARCHAR_V2(escape, escape_str);
    } else {
      escape_str = escape.get_varchar();
    }
    if (OB_SUCC(ret)) {
      if (escape_str.empty()) {  // escape ''
        escape_str.assign_ptr("\\", 1);
      }
      if (ObVarcharType != text.get_type()) {
        ObObj tmp_obj = text;
        tmp_obj.set_scale(text_expr->get_result_type().get_scale());  // 1.0 like 1
        ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, cmp_cs_type);
        EXPR_GET_VARCHAR_V2(tmp_obj, text_str);
      } else {
        text_str = text.get_varchar();
      }
    }
    if (OB_SUCC(ret)) {
      if (ObVarcharType != pattern.get_type()) {
        ObObj tmp_obj = pattern;
        tmp_obj.set_scale(pattern_expr->get_result_type().get_scale());  // 1 like 1.0
        ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, cmp_cs_type);
        EXPR_GET_VARCHAR_V2(tmp_obj, pattern_str);
      } else {
        pattern_str = pattern.get_varchar();
      }
    }
    if (OB_SUCC(ret)) {
      ObObj result;
      bool is_true = false;
      if (OB_FAIL(ObExprLike::calc_with_non_instr_mode(result,
              cmp_cs_type,
              escape.get_collation_type(),
              text_str,
              pattern_str,
              escape_str))) {  // no optimization.
        LOG_WARN("calc like func failed", K(ret));
      } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
        LOG_WARN("failed to call is_true", K(ret));
      } else if (is_true) {
        GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
      } else {
        GET_ALWAYS_TRUE_OR_FALSE(false, out_key_part);
      }
    }
  }
  return ret;
}

//  Add row item to the end of the row list
//  1. if item is always true, do not do next
//      there are two kinds of cases:
//      1) real true, row(k1, 2, k2)>row(1, 1, 1), "2>1" is true.
//          Under this kind of case, in fact we can ignore the second item and do next
//      2) not real true, row(k1, k2, k2)>=(1, k1, 4), "k2>=k1" is run-time defined,
//          we can not know during parser, so true is returned.
//          under this case, we can not ignore it. (k1=1 and k2=3) satisfied this condtion,
//          but not satisfied row(k1, k2)>=(1, 4)
//      we can not distinguish them, so do not do next.
//  2. if item is always false, no need to do next
//  3. if key part pos is not larger than exists', ignore it. because the range already be (min, max)
//      E.g.
//          a. rowkey(c1, c2, c3), condition: row(c1, c3, c2) > row(1, 2, 3).
//              while compare, row need item in order,  when considering c3, key part c2 must have range,
//              so (min, max) already used for c2.
//          b. (c1, c1, c2) > (const1, const2, const3) ==> (c1, c2) > (const1, const3)
//
//  NB: 1 and 2 are ensured by caller

int ObQueryRange::add_row_item(ObKeyPart*& row_tail, ObKeyPart* key_part)
{
  int ret = OB_SUCCESS;
  if (NULL != key_part) {
    if (NULL == row_tail) {
      row_tail = key_part;
    } else if (key_part->is_always_true()) {
      // ignore
    } else if (key_part->is_always_false()) {
      row_tail->and_next_ = key_part;
      key_part->and_next_ = NULL;
    } else {
      if (NULL == row_tail->and_next_ && row_tail->pos_.offset_ < key_part->pos_.offset_) {
        row_tail->and_next_ = key_part;
        key_part->and_next_ = NULL;
      } else {
        // find key part id no less than it
        // ignore
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

// Add and item to array

int ObQueryRange::add_and_item(ObKeyPartList& and_storage, ObKeyPart* key_part)
{
  int ret = OB_SUCCESS;
  if (NULL != key_part) {
    if (key_part->is_always_true()) {
      // everything and true is itself
      // if other item exists, ignore this key_part
      if (and_storage.get_size() <= 0) {
        if (!and_storage.add_last(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add and key part graph failed", K(ret));
        }
      }
    } else if (key_part->is_always_false()) {
      // everything and false is false
      if (1 == and_storage.get_size() && NULL != and_storage.get_first() &&
          and_storage.get_first()->is_always_false()) {
        // already false, ignore add action
      } else {
        and_storage.clear();
        if (!and_storage.add_last(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add and key part graph failed", K(ret));
        }
      }
    } else {  // normal case
      if (1 == and_storage.get_size() && NULL != and_storage.get_first() &&
          and_storage.get_first()->is_always_false()) {
        // already false, ignore add action
      } else {
        if (1 == and_storage.get_size() && NULL != and_storage.get_first() &&
            and_storage.get_first()->is_always_true()) {
          and_storage.clear();
        }
        if (!and_storage.increasing_add(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add and key part graph failed", K(ret));
        }
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

// Add or item to array

int ObQueryRange::add_or_item(ObKeyPartList& or_storage, ObKeyPart* key_part)
{
  int ret = OB_SUCCESS;
  if (NULL != key_part) {
    if (key_part->is_always_false()) {
      // everything or false is itself
      // if other item exists, ignore this key_part
      if (or_storage.get_size() <= 0) {
        if (!or_storage.add_last(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add or key part graph failed", K(ret));
        }
      }
    } else if (key_part->is_always_true()) {
      // everything or true is true
      if (1 == or_storage.get_size() && NULL != or_storage.get_first() && or_storage.get_first()->is_always_true()) {
        // already true, ignore add action
      } else {
        or_storage.clear();
        if (!or_storage.add_last(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add or key part graph failed", K(ret));
        }
      }
    } else {  // normal case
      if (1 == or_storage.get_size() && NULL != or_storage.get_first() && or_storage.get_first()->is_always_true()) {
        // already true, ignore add action
      } else {
        if (1 == or_storage.get_size() && NULL != or_storage.get_first() && or_storage.get_first()->is_always_false()) {
          or_storage.clear();
        }
        if (!or_storage.add_last(key_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add or key part graph failed", K(ret));
        }
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObQueryRange::pre_extract_basic_cmp(
    const ObRawExpr* node, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is null.", K(node));
  } else {
    const ObRawExpr* escape_expr = NULL;
    const ObOpRawExpr* multi_expr = static_cast<const ObOpRawExpr*>(node);
    if (T_OP_LIKE != node->get_expr_type()) {
      if (2 != multi_expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi_expr must has 2 arguments", K(ret));
      }
    } else {
      if (3 != multi_expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multi_expr must has 3 arguments", K(ret));
      } else {
        escape_expr = multi_expr->get_param_expr(2);
      }
    }
    if (OB_SUCC(ret)) {
      const ObRawExpr* right_expr = multi_expr->get_param_expr(1);
      if (lib::is_oracle_mode() && T_OP_ROW == multi_expr->get_param_expr(0)->get_expr_type()) {
        if (T_OP_ROW == multi_expr->get_param_expr(1)->get_expr_type() &&
            1 == multi_expr->get_param_expr(1)->get_param_count() &&
            T_OP_ROW == multi_expr->get_param_expr(1)->get_param_expr(0)->get_expr_type()) {
          right_expr = multi_expr->get_param_expr(1)->get_param_expr(0);
        }
      }
      if (OB_FAIL(get_basic_query_range(multi_expr->get_param_expr(0),
              right_expr,
              escape_expr,
              node->get_expr_type(),
              node->get_result_type(),
              out_key_part,
              dtc_params))) {
        LOG_WARN("Get basic query key part failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_ne_op(
    const ObOpRawExpr* t_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(t_expr), K_(query_range_ctx));
  } else if (2 != t_expr->get_param_count()) {  // trip op expr
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t_expr must has 2 arguments", K(ret));
  } else {
    bool is_precise = true;
    const ObRawExpr* l_expr = t_expr->get_param_expr(0);
    const ObRawExpr* r_expr = t_expr->get_param_expr(1);
    ObKeyPartList key_part_list;
    if (share::is_oracle_mode() && T_OP_ROW == l_expr->get_expr_type() && T_OP_ROW == r_expr->get_expr_type()) {
      if (1 == r_expr->get_param_count() && T_OP_ROW == r_expr->get_param_expr(0)->get_expr_type()) {
        r_expr = r_expr->get_param_expr(0);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
      query_range_ctx_->cur_expr_is_precise_ = false;
      ObKeyPart* tmp = NULL;
      if (OB_FAIL(get_basic_query_range(
              l_expr, r_expr, NULL, i == 0 ? T_OP_LT : T_OP_GT, t_expr->get_result_type(), tmp, dtc_params))) {
        LOG_WARN("Get basic query range failed", K(ret));
      } else if (OB_FAIL(add_or_item(key_part_list, tmp))) {
        LOG_WARN("push back failed", K(ret));
      } else {
        // A != B expression is split into two expressions, A<B OR A>B
        // To ensure that each expression is accurate, the entire expression is accurate
        is_precise = (is_precise && query_range_ctx_->cur_expr_is_precise_);
      }
    }
    if (OB_SUCC(ret)) {
      query_range_ctx_->cur_expr_is_precise_ = is_precise;
      // not need params when preliminary extract
      if (OB_FAIL(or_range_graph(key_part_list, NULL, out_key_part, dtc_params))) {
        LOG_WARN("or range graph failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_is_op(
    const ObOpRawExpr* b_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(b_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(b_expr), K_(query_range_ctx));
  } else if (ObNullType == b_expr->get_param_expr(1)->get_result_type().get_type()) {
    // pk is null will be extracted
    if (3 != b_expr->get_param_count()) {  // binary op expr
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("b_expr must has 3 arguments", K(ret));
    } else if (OB_FAIL(get_basic_query_range(b_expr->get_param_expr(0),
                   b_expr->get_param_expr(1),
                   NULL,
                   T_OP_NSEQ,
                   b_expr->get_result_type(),
                   out_key_part,
                   dtc_params))) {
      LOG_WARN("Get basic query key part failed", K(ret));
    }
  } else {
    query_range_ctx_->cur_expr_is_precise_ = false;
    GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
  }
  return ret;
}

int ObQueryRange::pre_extract_btw_op(
    const ObOpRawExpr* t_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(t_expr), K_(query_range_ctx));
  } else if (3 != t_expr->get_param_count()) {  // trip op expr
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t_expr must has 3 arguments", K(ret));
  } else {
    bool btw_op_is_precise = true;
    const ObRawExpr* l_expr = t_expr->get_param_expr(0);
    ObKeyPartList key_part_list;
    for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
      const ObRawExpr* r_expr = t_expr->get_param_expr(i + 1);
      ObKeyPart* tmp = NULL;
      if (OB_FAIL(get_basic_query_range(
              l_expr, r_expr, NULL, i == 0 ? T_OP_GE : T_OP_LE, t_expr->get_result_type(), tmp, dtc_params))) {
        LOG_WARN("Get basic query range failed", K(ret));
      } else if (OB_FAIL(add_and_item(key_part_list, tmp))) {
        LOG_WARN("push back failed", K(ret));
      } else {
        // BETWEEN...AND... expression is split into two expressions
        // To ensure that each expression is accurate, the entire expression is accurate
        btw_op_is_precise = (btw_op_is_precise && query_range_ctx_->cur_expr_is_precise_);
      }
    }
    if (OB_SUCC(ret)) {
      query_range_ctx_->cur_expr_is_precise_ = btw_op_is_precise;
      if (OB_FAIL(and_range_graph(key_part_list, out_key_part))) {
        LOG_WARN("and range graph failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_not_btw_op(
    const ObOpRawExpr* t_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(t_expr), K_(query_range_ctx));
  } else if (3 != t_expr->get_param_count()) {  // trip op expr
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t_expr must has 3 arguments", K(ret));
  } else {
    bool not_btw_op_is_precise = true;
    const ObRawExpr* l_expr = t_expr->get_param_expr(0);
    ObKeyPartList key_part_list;
    for (int i = 0; OB_SUCC(ret) && i < 2; ++i) {
      query_range_ctx_->cur_expr_is_precise_ = false;
      const ObRawExpr* r_expr = t_expr->get_param_expr(i + 1);
      ObKeyPart* tmp = NULL;
      if (OB_FAIL(get_basic_query_range(
              l_expr, r_expr, NULL, i == 0 ? T_OP_LT : T_OP_GT, t_expr->get_result_type(), tmp, dtc_params))) {
        LOG_WARN("Get basic query range failed", K(ret));
      } else if (OB_FAIL(add_or_item(key_part_list, tmp))) {
        LOG_WARN("push back failed", K(ret));
      } else {
        // NOT BETWEEN...AND... expression is split into two expressions
        // To ensure that each expression is accurate, the entire expression is accurate
        not_btw_op_is_precise = (not_btw_op_is_precise && query_range_ctx_->cur_expr_is_precise_);
      }
    }
    if (OB_SUCC(ret)) {
      query_range_ctx_->cur_expr_is_precise_ = not_btw_op_is_precise;
      // not need params when preliminary extract
      if (OB_FAIL(or_range_graph(key_part_list, NULL, out_key_part, dtc_params))) {
        LOG_WARN("or range graph failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_single_in_op(
    const ObOpRawExpr* b_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(b_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr or query_range_ctx is null. ", K(b_expr), K_(query_range_ctx));
  } else if (2 != b_expr->get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t_expr must be 3 argument", K(ret));
  } else {
    const ObOpRawExpr* r_expr = static_cast<const ObOpRawExpr*>(b_expr->get_param_expr(1));
    if (NULL == r_expr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("r_expr is null.", K(ret));
    } else {
      ObArenaAllocator alloc;
      bool cur_in_is_precise = true;
      ObKeyPart* tmp_tail = NULL;
      ObKeyPart* find_false = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < r_expr->get_param_count(); i++) {
        ObKeyPart* tmp = NULL;
        ObExprResType res_type(alloc);
        if (OB_FAIL(get_in_expr_res_type(b_expr, i, res_type))) {
          LOG_WARN("get in expr element result type failed", K(ret), K(i));
        } else if (OB_FAIL(get_basic_query_range(b_expr->get_param_expr(0),
                       r_expr->get_param_expr(i),
                       NULL,
                       T_OP_EQ,
                       res_type,
                       tmp,
                       dtc_params))) {
          LOG_WARN("Get basic query range failed", K(ret));
        } else if (OB_ISNULL(tmp) || NULL != tmp->or_next_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tmp is null or tmp->or_next is not null", K(ret), K(tmp));
        } else if (tmp->is_always_true()) {  // find true , out_key_part -> true, ignore other
          out_key_part = tmp;
          cur_in_is_precise = (cur_in_is_precise && query_range_ctx_->cur_expr_is_precise_);
          break;
        } else if (tmp->is_always_false()) {  // find false
          find_false = tmp;
        } else if (NULL == tmp_tail) {
          tmp_tail = tmp;
          out_key_part = tmp;
        } else {
          tmp_tail->or_next_ = tmp;
          tmp_tail = tmp;
        }
        if (OB_SUCC(ret)) {
          cur_in_is_precise = (cur_in_is_precise && query_range_ctx_->cur_expr_is_precise_);
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL != find_false && NULL == out_key_part) {
          out_key_part = find_false;
        }
        query_range_ctx_->cur_expr_is_precise_ = cur_in_is_precise;
        int64_t max_pos = -1;
        bool is_strict_equal = true;
        if (OB_FAIL(is_strict_equal_graph(out_key_part, out_key_part->pos_.offset_, max_pos, is_strict_equal))) {
          LOG_WARN("is trict equal graph failed", K(ret));
        } else if (!is_strict_equal) {
          ObKeyPartList key_part_list;
          if (OB_FAIL(split_or(out_key_part, key_part_list))) {
            LOG_WARN("split temp_result to or_list failed", K(ret));
          } else if (OB_FAIL(or_range_graph(key_part_list, NULL, out_key_part, dtc_params))) {
            LOG_WARN("or range graph failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_in_op(
    const ObOpRawExpr* b_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  // treat IN operation as 'left_param = right_item_1 or ... or left_param = right_item_n'
  if (OB_ISNULL(b_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(b_expr), K_(query_range_ctx));
  } else if (2 != b_expr->get_param_count()) {  // binary op expr
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t_expr must has 3 arguments", K(ret));
  } else {
    const ObOpRawExpr* r_expr = static_cast<const ObOpRawExpr*>(b_expr->get_param_expr(1));
    if (NULL == r_expr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("r_expr is null.", K(ret));
    } else {
      ObKeyPartList key_part_list;
      ObArenaAllocator alloc;
      bool cur_in_is_precise = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < r_expr->get_param_count(); i++) {
        ObKeyPart* tmp = NULL;
        ObExprResType res_type(alloc);
        if (OB_FAIL(get_in_expr_res_type(b_expr, i, res_type))) {
          LOG_WARN("get in expr element result type failed", K(ret), K(i));
        } else if (OB_FAIL(get_basic_query_range(b_expr->get_param_expr(0),
                       r_expr->get_param_expr(i),
                       NULL,
                       T_OP_EQ,
                       res_type,
                       tmp,
                       dtc_params))) {
          LOG_WARN("Get basic query range failed", K(ret));
        } else if (OB_FAIL(add_or_item(key_part_list, tmp))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          cur_in_is_precise = (cur_in_is_precise && query_range_ctx_->cur_expr_is_precise_);
        }
      }
      if (OB_SUCC(ret)) {
        query_range_ctx_->cur_expr_is_precise_ = cur_in_is_precise;
        if (OB_FAIL(or_range_graph(key_part_list, NULL, out_key_part, dtc_params))) {
          LOG_WARN("or range graph failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_and_or_op(
    const ObOpRawExpr* m_expr, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(m_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(m_expr), K_(query_range_ctx));
  } else {
    bool cur_expr_is_precise = true;
    ObKeyPartList key_part_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < m_expr->get_param_count(); ++i) {
      ObKeyPart* tmp = NULL;
      query_range_ctx_->cur_expr_is_precise_ = false;
      if (OB_FAIL(preliminary_extract(m_expr->get_param_expr(i), tmp, dtc_params))) {
        LOG_WARN("preliminary_extract failed", K(ret));
      } else if (T_OP_AND == m_expr->get_expr_type()) {
        if (OB_FAIL(add_and_item(key_part_list, tmp))) {
          LOG_WARN("push back failed", K(ret));
        }
      } else {  // T_OP_OR
        if (OB_FAIL(add_or_item(key_part_list, tmp))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        cur_expr_is_precise = (cur_expr_is_precise && query_range_ctx_->cur_expr_is_precise_);
      }
    }
    // for
    if (OB_SUCC(ret)) {
      query_range_ctx_->cur_expr_is_precise_ = cur_expr_is_precise;
      if (T_OP_AND == m_expr->get_expr_type()) {
        if (OB_FAIL(and_range_graph(key_part_list, out_key_part))) {
          LOG_WARN("and range graph failed", K(ret));
        }
      } else {
        if (OB_FAIL(or_range_graph(key_part_list, NULL, out_key_part, dtc_params))) {
          LOG_WARN("or range graph failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::pre_extract_const_op(const ObConstRawExpr* c_expr, ObKeyPart*& out_key_part)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c_expr) || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(c_expr), K_(query_range_ctx));
  } else {
    bool b_val = false;
    query_range_ctx_->cur_expr_is_precise_ = false;
    if (c_expr->has_flag(IS_PARAM)) {
      GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
    } else if (OB_FAIL(ObObjEvaluator::is_true(c_expr->get_value(), b_val))) {
      LOG_WARN("get bool value failed.", K(ret));
    } else {
      GET_ALWAYS_TRUE_OR_FALSE(b_val, out_key_part);
    }
  }
  return ret;
}

//  For each index, preliminary extract query range,
//  the result may contain prepared '?' expression.
//  If prepared '?' expression exists, final extract action is needed
int ObQueryRange::preliminary_extract(
    const ObRawExpr* node, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params, const bool is_single_in)
{
  int ret = OB_SUCCESS;
  out_key_part = NULL;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_ISNULL(query_range_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("argument is not inited", K(ret), KP(node), KP(query_range_ctx_));
  } else if (NULL == node) {
    // do nothing
  } else if (node->is_const_expr()) {
    const ObConstRawExpr* c_expr = static_cast<const ObConstRawExpr*>(node);
    if (OB_FAIL(pre_extract_const_op(c_expr, out_key_part))) {
      LOG_WARN("extract is_op failed", K(ret));
    }
  } else {
    const ObOpRawExpr* b_expr = static_cast<const ObOpRawExpr*>(node);
    if (IS_BASIC_CMP_OP(node->get_expr_type())) {
      if (OB_FAIL(pre_extract_basic_cmp(node, out_key_part, dtc_params))) {
        LOG_WARN("extract basic cmp failed", K(ret));
      }
    } else if (T_OP_NE == node->get_expr_type()) {
      if (OB_FAIL(pre_extract_ne_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract ne_op failed", K(ret));
      }
    } else if (T_OP_IS == node->get_expr_type()) {
      if (OB_FAIL(pre_extract_is_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract is_op failed", K(ret));
      }
    } else if (T_OP_BTW == node->get_expr_type()) {
      if (OB_FAIL(pre_extract_btw_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract btw_op failed", K(ret));
      }
    } else if (T_OP_NOT_BTW == node->get_expr_type()) {
      if (OB_FAIL(pre_extract_not_btw_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract not_btw failed", K(ret));
      }
    } else if (T_OP_IN == node->get_expr_type()) {
      if (is_single_in) {
        if (OB_FAIL(pre_extract_single_in_op(b_expr, out_key_part, dtc_params))) {
          LOG_WARN("extract single in_op failed", K(ret));
        }
      } else if (OB_FAIL(pre_extract_in_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract in_op failed", K(ret));
      }
    } else if (T_OP_AND == node->get_expr_type() || T_OP_OR == node->get_expr_type()) {
      if (OB_FAIL(pre_extract_and_or_op(b_expr, out_key_part, dtc_params))) {
        LOG_WARN("extract and_or failed", K(ret));
      }
    } else {
      query_range_ctx_->cur_expr_is_precise_ = false;
      GET_ALWAYS_TRUE_OR_FALSE(true, out_key_part);
    }
  }
  return ret;
}

#undef GET_ALWAYS_TRUE_OR_FALSE

int ObQueryRange::get_in_expr_res_type(const ObRawExpr* in_expr, int64_t val_idx, ObExprResType& res_type) const
{
  int ret = OB_SUCCESS;
  if (NULL == in_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("in_expr is null.", K(ret));
  } else {
    const ObRawExpr* l_expr = in_expr->get_param_expr(0);
    if (NULL == l_expr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("l_expr is null.", K(ret));
    } else {
      int64_t row_dimension = (T_OP_ROW == l_expr->get_expr_type()) ? l_expr->get_param_count() : 1;
      const ObIArray<ObExprCalcType>& calc_types = in_expr->get_result_type().get_row_calc_cmp_types();
      if (T_OP_ROW != l_expr->get_expr_type()) {
        res_type.set_calc_meta(calc_types.at(val_idx));
      } else if (OB_FAIL(res_type.init_row_dimension(row_dimension))) {
        LOG_WARN("fail to init row dimension", K(ret), K(row_dimension));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_dimension; ++i) {
          ret = res_type.get_row_calc_cmp_types().push_back(calc_types.at(val_idx * row_dimension + i));
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::is_key_part(const ObKeyPartId& id, ObKeyPartPos& pos, bool& is_key_part)
{
  int ret = OB_SUCCESS;
  if (NULL == query_range_ctx_) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_range_ctx_ is not inited.", K(ret));
  } else {
    is_key_part = false;
    int map_ret = query_range_ctx_->key_part_map_.get_refactored(id, pos);
    if (OB_SUCCESS == map_ret) {
      is_key_part = true;
      SQL_REWRITE_LOG(DEBUG, "id pair is  key part", K_(id.table_id), K_(id.column_id));
    } else {
      if (OB_HASH_NOT_EXIST != map_ret) {
        ret = map_ret;
        LOG_WARN("get kay_part_id from hash map failed", K(ret), K_(id.table_id), K_(id.column_id));
      } else {
        is_key_part = false;
        SQL_REWRITE_LOG(DEBUG, "id pair is not key part", K_(id.table_id), K_(id.column_id));
      }
    }
  }
  return ret;
}

// split the head key part to general term or-array.
// each gt has its own and_next_, so no need to deep copy it.

int ObQueryRange::split_general_or(ObKeyPart* graph, ObKeyPartList& or_storage)
{
  int ret = OB_SUCCESS;
  or_storage.clear();
  if (NULL != graph) {
    ObKeyPart* cur_gt = graph;
    while (NULL != cur_gt && OB_SUCC(ret)) {
      ObKeyPart* or_next_gt = cur_gt->cut_general_or_next();
      if (!or_storage.add_last(cur_gt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Split graph to or array failed", K(ret));
      } else {
        cur_gt = or_next_gt;
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

// split the head key part to or-list
// several or key parts may share and_next_, so deep copy is needed.

int ObQueryRange::split_or(ObKeyPart* graph, ObKeyPartList& or_list)
{
  int ret = OB_SUCCESS;
  if (NULL != graph) {
    ObKeyPart* cur = (graph);
    ObKeyPart* prev_and_next = NULL;
    while (OB_SUCC(ret) && NULL != cur) {
      ObKeyPart* or_next = cur->or_next_;
      if (cur->and_next_ != prev_and_next) {
        prev_and_next = cur->and_next_;
      } else {
        if (NULL != prev_and_next) {
          ObKeyPart* new_and_next = NULL;
          if (OB_FAIL(deep_copy_range_graph(cur->and_next_, new_and_next))) {
            LOG_WARN("Copy range graph failed", K(ret));
          }
          cur->and_next_ = new_and_next;
        }
      }
      if (OB_SUCC(ret)) {
        cur->or_next_ = NULL;
        if (!(or_list.add_last(cur))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Split graph to or array failed", K(ret));
        } else {
          cur = or_next;
        }
      }
    }
  }
  return ret;
}

// int ObQueryRange::deal_not_align_keypart(ObKeyPart *l_cur,
//                                         ObKeyPart *r_cur,
//                                         const int64_t &s_offset,
//                                         const int64_t &e_offset,
//                                         ObKeyPart *&rest1)
//{
//  int ret = OB_SUCCESS;
//  ObKeyPart *tail = NULL;
//  while (OB_SUCC(ret)
//         && ) {
//    ObKeyPart *new_key_part = NULL;
//    if (OB_FAIL(alloc_full_key_part(new_key_part))) {
//      //warn
//    } else if (NULL != l_cur && NULL != r_cur) {
//      if (l_cur->pos_.offset_ < r_cur->pos_.offset_) {
//        if (s_need_continue) {
//          new_key_part->set_normal_start(l_cur);
//        }
//        if (e_need_continue) {
//          new_key_part->set_normal_end(l_cur);
//        }
//        if (OB_FAIL(link_item(new_key_part, l_cur))) {
//          //warn
//        } else if (NULL != l_cur->or_next_) {
//          l_cur = NULL;
//        } else {
//          l_cur = l_cur->and_next_;
//        }
//      } else if (l_cur->pos_.offset_ > r_cur->pos_.offset_) {
//        if (s_need_continue) {
//          new_key_part->set_normal_start(r_cur);
//        }
//        if (e_need_continue) {
//          new_key_part->set_normal_end(r_cur);
//        }
//        if (OB_FAIL(link_item(new_key_part, r_cur))) {
//          //warn
//        } else if (NULL != r_cur->or_next_) {
//          r_cur = NULL;
//        } else {
//          r_cur = r_cur->and_next_;
//        }
//      }
//    } else {
//      if (NULL == l_cur) {
//        new_key_part->set_normal_start(r_cur);
//        new_key_part->set_normal_end(r_cur);
//        if (OB_FAIL(link_item(new_key_part, r_cur))) {
//          //warn
//        } else if (NULL != r_cur->or_next_) {
//          r_cur = NULL;
//        } else {
//          r_cur = r_cur->and_next_;
//        }
//      } else if (NULL == r_cur) {
//        new_key_part->set_normal_start(l_cur);
//        new_key_part->set_normal_end(l_cur);
//        if (OB_FAIL(link_item(new_key_part, l_cur))) {
//          //warn
//        } else if (NULL != l_cur->or_next_) {
//          l_cur = NULL;
//        } else {
//          l_cur = l_cur->and_next_;
//        }
//      }
//    }
//    if (OB_SUCC(ret) && NULL != new_key_part) {
//      if (NULL == tail) {
//        tail = new_key_part;
//        rest1 = tail;
//      } else {
//        tail->and_next_ = new_key_part;
//        tail = new_key_part;
//      }
//    }
//  }
//  return ret;
//}
// int ObQueryRange::link_item(ObKeyPart *new_key_part, ObKeyPart *cur)
//{
//  int ret = OB_SUCCESS;
//  ObKeyPart *item = cur ? cur->item_next_ : NULL;
//  while (OB_SUCC(ret) && NULL != item) {
//    ObKeyPart *new_item = NULL;
//    if (OB_ISNULL(new_item = deep_copy_key_part(item))) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("Cope item key part failed", K(ret));
//    } else {
//      new_item->item_next_ = new_key_part->item_next_;
//      new_key_part->item_next_ = new_item;
//      item = item->item_next_;
//    }
//  }
//  return ret;
//}
//
// For row action, we need to know where the new start_ and the new end_
// come from, start_border_type/end_border_type will return the new
//  edges source, then we can treat row as integrity
//
//  E.g.
//       row(k1, k2) > (3, 2) and row(k1, k2) > (1, 4)
//       when find the intersection of k1, we need to know the result3 is comes
//       from the first const row(3,2), then 2 is the new start of k2.
//       ==> row(k1, k2) > (3, 2),  not row(k1, k2) > (3, 4)

// When the current range is equal, take the next one to judge the boundary,
// for example (a, b)>(1, 2) and (a, b)> (1, 3),
// Judge by b.
// If there is a misalignment, such as (a, c)> (1, 3) and (a, b, c)> (1, 2, 3)
// Select the side with value as the boundary, here select the right side
// Use two bool variables to distinguish whether the starting boundary needs to be judged by the next column value.
// Set to constant false for the condition of equivalence, but the actual value is not equal.
// For example (a,b)=(1, 1) and (a, b)=(1, 2)
// For the non-first column range without intersection, set it to constant false.
// Such as (a, b)> (1, 2) and (a, b) <(2, 3) and (a, b)> (1, 4) and (a, b) <(2, 5)

int ObQueryRange::intersect_border_from(const ObKeyPart* l_key_part, const ObKeyPart* r_key_part,
    ObRowBorderType& start_border_type, ObRowBorderType& end_border_type, bool& is_always_false)
{
  int ret = OB_SUCCESS;
  bool start_identified = true;
  bool end_identified = true;
  bool s_need_continue = false;
  bool e_need_continue = false;
  start_border_type = OB_FROM_NONE;
  end_border_type = OB_FROM_NONE;
  bool left_is_equal = false;
  if (OB_ISNULL(l_key_part) || OB_ISNULL(r_key_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(l_key_part), K(r_key_part));
  } else if (OB_UNLIKELY(!l_key_part->is_normal_key()) || OB_UNLIKELY(!r_key_part->is_normal_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("keypart isn't normal key", K(*l_key_part), K(*r_key_part));
  } else if (l_key_part->pos_.offset_ < r_key_part->pos_.offset_) {
    start_border_type = OB_FROM_LEFT;
    end_border_type = OB_FROM_LEFT;
  } else if (l_key_part->pos_.offset_ > r_key_part->pos_.offset_) {
    start_border_type = OB_FROM_RIGHT;
    end_border_type = OB_FROM_RIGHT;
  } else if (true == (left_is_equal = l_key_part->is_equal_condition()) || r_key_part->is_equal_condition()) {
    if (l_key_part->is_question_mark() || r_key_part->is_question_mark()) {
      s_need_continue = true;
      e_need_continue = true;
    } else if (l_key_part->has_intersect(r_key_part)) {
      // incase here is last
      if (NULL == l_key_part->and_next_) {
        start_border_type = left_is_equal ? OB_FROM_LEFT : OB_FROM_RIGHT;
        end_border_type = start_border_type;
      } else {
        s_need_continue = true;
        e_need_continue = true;
      }
    } else {
      is_always_false = true;
    }
  } else {
    ObObj* s1 = &l_key_part->normal_keypart_->start_;
    ObObj* e1 = &l_key_part->normal_keypart_->end_;
    ObObj* s2 = &r_key_part->normal_keypart_->start_;
    ObObj* e2 = &r_key_part->normal_keypart_->end_;
    if (OB_ISNULL(s1) || OB_ISNULL(e1) || OB_ISNULL(s2) || OB_ISNULL(e2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("s1,e1,s2,e2 can not be null", K(ret), KP(s1), KP(e1), KP(s2), KP(e2));
    } else if (l_key_part->is_question_mark() || r_key_part->is_question_mark()) {
      if (!is_min_range_value(*s1) && !is_min_range_value(*s2)) {
        // both have none-min start value
        start_border_type = OB_FROM_NONE;
      } else if (is_min_range_value(*s1) && !is_min_range_value(*s2)) {
        // only r_key_part has start value
        start_border_type = OB_FROM_RIGHT;
      } else if (!is_min_range_value(*s1) && is_min_range_value(*s2)) {
        // only l_key_part has start value
        start_border_type = OB_FROM_LEFT;
      } else {  // both have min start value
        start_identified = false;
      }
      if (!is_max_range_value(*e1) && !is_max_range_value(*e2)) {
        end_border_type = OB_FROM_NONE;
      } else if (is_max_range_value(*e1) && !is_max_range_value(*e2)) {
        end_border_type = OB_FROM_RIGHT;
      } else if (!is_max_range_value(*e1) && is_max_range_value(*e2)) {
        end_border_type = OB_FROM_LEFT;
      } else {
        end_identified = false;
      }
    } else {
      // has changeless value
      if (l_key_part->id_ != r_key_part->id_ || l_key_part->pos_ != r_key_part->pos_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("l_id is not equal to r_id", K(ret));
      } else if (l_key_part->has_intersect(r_key_part)) {
        if (is_min_range_value(*s1) && is_min_range_value(*s2)) {
          start_identified = false;
        } else {
          int cmp = s1->compare(*s2);
          if (cmp > 0) {
            start_border_type = OB_FROM_LEFT;
          } else if (cmp < 0) {
            start_border_type = OB_FROM_RIGHT;
          } else {  // equal
            if (NULL == l_key_part->and_next_) {
              start_border_type = OB_FROM_LEFT;  // lucky left
            }
            s_need_continue = true;
          }
        }
        if (is_max_range_value(*e1) && is_max_range_value(*e2)) {
          end_identified = false;
        } else {
          int cmp = e1->compare(*e2);
          if (cmp > 0) {
            end_border_type = OB_FROM_RIGHT;
          } else if (cmp < 0) {
            end_border_type = OB_FROM_LEFT;
          } else {  // euqal
            if (NULL == l_key_part->and_next_) {
              end_border_type = OB_FROM_LEFT;  // lucky left
            }
            e_need_continue = true;
          }
        }
      } else {
        is_always_false = true;
      }
    }
    if (!start_identified || !end_identified) {
      if (!start_identified && !end_identified) {
        start_border_type = OB_FROM_LEFT;  // lucky left
        end_border_type = OB_FROM_LEFT;    // lucky left
      } else if (start_identified) {
        end_border_type = start_border_type;
        e_need_continue = s_need_continue;
      } else if (end_identified) {
        start_border_type = end_border_type;
        s_need_continue = e_need_continue;
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret) && !is_always_false && NULL != l_key_part->and_next_ && NULL != r_key_part->and_next_ &&
      (s_need_continue || e_need_continue)) {
    ObRowBorderType tmp_start_border = OB_FROM_NONE;
    ObRowBorderType tmp_end_border = OB_FROM_NONE;
    if (OB_FAIL(SMART_CALL(intersect_border_from(
            l_key_part->and_next_, r_key_part->and_next_, tmp_start_border, tmp_end_border, is_always_false)))) {
      LOG_WARN("invalid argument.", K(ret), K(l_key_part), K(r_key_part));
    } else if (s_need_continue) {
      start_border_type = tmp_start_border;
      if (e_need_continue) {
        end_border_type = tmp_end_border;
      }
    } else {
    }
  }
  return ret;
}

bool ObQueryRange::is_max_range_value(const ObObj& obj) const
{
  return lib::is_oracle_mode() ? (obj.is_max_value() || obj.is_null()) : obj.is_max_value();
}

bool ObQueryRange::is_min_range_value(const ObObj& obj) const
{
  return lib::is_oracle_mode() ? (obj.is_min_value()) : (obj.is_min_value() || obj.is_null());
}

// After wen known where the edges of the row come from,
// set the new edges to the result.
// if or_next_ list is not NULL, cut it.

int ObQueryRange::set_partial_row_border(ObKeyPart* l_gt, ObKeyPart* r_gt, ObRowBorderType start_border_type,
    ObRowBorderType end_border_type, ObKeyPart*& result)
{
  int ret = OB_SUCCESS;
  ObKeyPart* l_cur = (NULL != l_gt && NULL == l_gt->or_next_) ? l_gt : NULL;
  ObKeyPart* r_cur = (NULL != r_gt && NULL == r_gt->or_next_) ? r_gt : NULL;
  ObKeyPart* prev_key_part = NULL;
  result = NULL;
  if (ObQueryRange::OB_FROM_NONE != start_border_type || ObQueryRange::OB_FROM_NONE != end_border_type) {
    bool b_flag = false;
    while (OB_SUCC(ret) && !b_flag && (NULL != l_cur || NULL != r_cur)) {
      ObKeyPart* new_key_part = NULL;
      if (start_border_type != end_border_type && NULL != l_cur && NULL != r_cur &&
          l_cur->is_question_mark() != r_cur->is_question_mark()) {
        // we can express such case: start is unknown value, but end is known value
        b_flag = true;
      } else if (((ObQueryRange::OB_FROM_LEFT == start_border_type || ObQueryRange::OB_FROM_LEFT == end_border_type) &&
                     OB_ISNULL(l_cur)) ||
                 ((ObQueryRange::OB_FROM_RIGHT == start_border_type ||
                      ObQueryRange::OB_FROM_RIGHT == end_border_type) &&
                     OB_ISNULL(r_cur))) {
        b_flag = true;
      } else if (OB_FAIL(alloc_full_key_part(new_key_part))) {
        LOG_WARN("Get key part failed", K(ret));
        b_flag = true;
      } else if (OB_ISNULL(new_key_part) || OB_UNLIKELY(!new_key_part->is_normal_key())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new_key_part is null.");
      } else {
        new_key_part->normal_keypart_->always_true_ = false;
        if (ObQueryRange::OB_FROM_LEFT == start_border_type && l_cur) {
          new_key_part->set_normal_start(l_cur);
        } else if (ObQueryRange::OB_FROM_RIGHT == start_border_type && NULL != r_cur) {
          new_key_part->set_normal_start(r_cur);
        } else {
          // do nothing
        }
        if (ObQueryRange::OB_FROM_LEFT == end_border_type && l_cur) {
          new_key_part->set_normal_end(l_cur);
        } else if (ObQueryRange::OB_FROM_RIGHT == end_border_type && NULL != r_cur) {
          new_key_part->set_normal_end(r_cur);
        } else {
          // do nothing
        }
        ObKeyPart* item = NULL != l_cur ? l_cur->item_next_ : NULL;
        while (OB_SUCC(ret) && NULL != item) {
          ObKeyPart* new_item = NULL;
          if (OB_ISNULL(new_item = deep_copy_key_part(item))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Cope item key part failed", K(ret));
          } else {
            new_item->item_next_ = new_key_part->item_next_;
            new_key_part->item_next_ = new_item;
            item = item->item_next_;
          }
        }
        item = NULL != r_cur ? r_cur->item_next_ : NULL;
        while (OB_SUCC(ret) && NULL != item) {
          ObKeyPart* new_item = NULL;
          if (OB_ISNULL(new_item = deep_copy_key_part(item))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Cope item key part failed", K(ret));
          } else {
            new_item->item_next_ = new_key_part->item_next_;
            new_key_part->item_next_ = new_item;
            item = item->item_next_;
          }
        }
      }
      if (OB_SUCC(ret) && !b_flag) {
        if (NULL != prev_key_part) {
          prev_key_part->and_next_ = new_key_part;
        } else {
          result = new_key_part;
        }
        prev_key_part = new_key_part;
        // if and_next_ does not have or-item, then do next
        if (NULL != l_cur && NULL != l_cur->and_next_ && NULL == l_cur->and_next_->or_next_) {
          l_cur = l_cur->and_next_;
        } else {
          l_cur = NULL;
        }
        if (NULL != r_cur && NULL != r_cur->and_next_ && NULL == r_cur->and_next_->or_next_) {
          r_cur = r_cur->and_next_;
        } else {
          r_cur = NULL;
        }
      }
    }
  }
  return ret;
}

//  do and of general term (gt) when row exists.
//  if you need, find the meaning of gt from comments of do_gt_and().
//
//  ROW(k1(s1, e1), k2(s2, e2)) and ROW(k1(s3, e4), k2(s3, e4))
//         k1(s1, e1): means the first key part in row between s1 and e1.
//  because the and operands is row, we can not AND each key part separtely.
//  so the result is:
//  ROW(k1(ns1, ne1), k2(ns2, ne2)), if two row values has intersection.
//         if s1>s3, ns1 = s1 and ns2 = s2 else ns1 = s3 and ns2 = s4;
//         if e1<e2, ne1 = e1 and ne2 = e2 else ne1 = e3 and ne2 = e4;
//         if start/end value can not compare, k1 is returned only
//             e.g. row(k1, k2) > (?1, ?2) and row(k1, k2) > (?3, ?4)

int ObQueryRange::do_row_gt_and(ObKeyPart* l_gt, ObKeyPart* r_gt, ObKeyPart*& res_gt)
{
  int ret = OB_SUCCESS;
  if (NULL == l_gt && NULL == r_gt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Wrong argument", K(ret));
  } else if (NULL == l_gt) {
    res_gt = r_gt;
  } else if (NULL == r_gt) {
    res_gt = l_gt;
  } else if (l_gt->pos_.offset_ < r_gt->pos_.offset_) {
    res_gt = l_gt;
  } else if (r_gt->pos_.offset_ < l_gt->pos_.offset_) {
    res_gt = r_gt;
  } else {
    ObKeyPart* l_gt_next = l_gt->and_next_;
    ObKeyPart* r_gt_next = r_gt->and_next_;
    res_gt = NULL;
    bool always_true = false;
    ObKeyPart* find_false = NULL;
    ObKeyPart* tail = NULL;
    ObKeyPart* l_cur = NULL;
    ObKeyPart* r_cur = NULL;
    for (l_cur = l_gt; OB_SUCC(ret) && !always_true && NULL != l_cur; l_cur = l_cur->or_next_) {
      for (r_cur = r_gt; OB_SUCC(ret) && !always_true && NULL != r_cur; r_cur = r_cur->or_next_) {
        ObKeyPart* result = NULL;
        ObKeyPart* rest = NULL;
        if (l_cur->is_like_key() && r_cur->is_like_key()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("l_cur and r_cur are both like key", K(ret), K(*l_cur), K(*r_cur));
        } else if (l_cur->is_like_key()) {
          result = r_cur;
        } else if (r_cur->is_like_key()) {
          result = l_cur;
        } else if (!l_cur->is_normal_key() || !r_cur->is_normal_key() || l_cur->is_always_true() ||
                   l_cur->is_always_false() || r_cur->is_always_true() || r_cur->is_always_false()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("l_cur and r_cur are not always true or false.", K(*l_cur), K(*r_cur));
        } else {
          ObKeyPart* new_l_cur = NULL;
          ObKeyPart* new_r_cur = NULL;
          if (OB_FAIL(deep_copy_key_part_and_items(l_cur, new_l_cur))) {
            LOG_WARN("Light copy key part and items failed", K(ret));
          } else if (OB_FAIL(deep_copy_key_part_and_items(r_cur, new_r_cur))) {
            LOG_WARN("Right copy key part and items failed", K(ret));
          } else if (OB_FAIL(
                         do_key_part_node_and(new_l_cur, new_r_cur, result))) {  // do AND of each key part node only
            LOG_WARN("Do key part node intersection failed", K(ret));
          } else if (NULL == result) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null.", K(ret));
          } else if (result->is_always_true()) {
            always_true = true;
            res_gt = result;
          } else if (result->is_always_false()) {
            // ignore
            find_false = result;
          } else {
            // set other value of row
            ObRowBorderType s_border = OB_FROM_NONE;
            ObRowBorderType e_border = OB_FROM_NONE;
            bool is_always_false = false;
            if (OB_FAIL(intersect_border_from(l_cur, r_cur, s_border, e_border, is_always_false))) {
              LOG_WARN("Find row border failed", K(ret));
            } else if (is_always_false) {
              result->normal_keypart_->always_false_ = true;
              result->normal_keypart_->always_true_ = true;
              result->normal_keypart_->start_.set_max_value();
              result->normal_keypart_->end_.set_min_value();
              find_false = result;
            } else if (OB_FAIL(set_partial_row_border(l_gt_next, r_gt_next, s_border, e_border, rest))) {
              LOG_WARN("Set row border failed", K(ret));
            } else if (OB_ISNULL(rest)) {
              if (OB_FAIL(remove_precise_range_expr(result->pos_.offset_ + 1))) {
                LOG_WARN("remove precise range expr failed", K(ret));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          result->link_gt(rest);
          // link to the or_next_ list
          if (NULL != tail) {
            tail->or_next_ = result;
          } else {
            res_gt = result;
          }
          tail = result;
        }
      }
    }
    if (OB_SUCC(ret) && NULL == res_gt) {
      if (NULL == find_false) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find_false is null.", K(ret));
      } else {
        res_gt = find_false;
      }
    }
  }
  return ret;
}

//  AND single key part (itself) and items in item_next_ list
//  Each item in item_next_ list must be unkown item untill physical plan open
//
//  E.g.
//       (A1 and ?1 and ?2 and ... and ?m) and (A2 and ?I and ?II and ... and ?n)
//       ==>
//       (A12 and ?1 and ?2 and ... and ?m and ?I and ?II and ... and ?n)
//       A12 is the result of (A1 intersect A2)

int ObQueryRange::do_key_part_node_and(ObKeyPart* l_key_part, ObKeyPart* r_key_part, ObKeyPart*& res_key_part)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_key_part) || OB_ISNULL(r_key_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Wrong argument", K(ret));
  } else {
    if (l_key_part->is_always_true() || r_key_part->is_always_true()) {
      res_key_part = r_key_part;
    } else if (l_key_part->is_always_false() || r_key_part->is_always_false()) {
      res_key_part = l_key_part;
    } else {
      res_key_part = NULL;
      l_key_part->and_next_ = NULL;
      l_key_part->or_next_ = NULL;
      r_key_part->and_next_ = NULL;
      r_key_part->or_next_ = NULL;
      ObKeyPart* l_items = l_key_part;
      ObKeyPart* r_items = r_key_part;
      if (!l_key_part->is_question_mark() && !r_key_part->is_question_mark()) {
        l_items = l_key_part->item_next_;
        r_items = r_key_part->item_next_;
        if (OB_FAIL(l_key_part->intersect(r_key_part, contain_row_))) {
          LOG_WARN("Do key part node intersection failed", K(ret));
        } else {
          res_key_part = l_key_part;
        }
      } else if (!l_key_part->is_question_mark()) {
        res_key_part = l_key_part;
        l_items = l_key_part->item_next_;
      } else if (!r_key_part->is_question_mark()) {
        res_key_part = r_key_part;
        r_items = r_key_part->item_next_;
      }

      // link all unkown-value items
      if (OB_SUCC(ret)) {
        if (NULL != l_items) {
          if (NULL != res_key_part) {
            res_key_part->item_next_ = l_items;
          } else {
            res_key_part = l_items;
          }
        }
        if (NULL != r_items) {
          if (OB_ISNULL(res_key_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("res_key_part is null.", K(ret));
          } else {
            ObKeyPart* tail = res_key_part;
            // find the item_next_ list tail
            while (NULL != tail->item_next_) {
              tail = tail->item_next_;
            }
            tail->item_next_ = r_items;
          }
        }
      }
    }
  }
  return ret;
}

//  Just get the key part node itself and items in its item_next_ list

int ObQueryRange::deep_copy_key_part_and_items(const ObKeyPart* src_key_part, ObKeyPart*& dest_key_part)
{
  int ret = OB_SUCCESS;
  const ObKeyPart* tmp_key_part = src_key_part;
  ObKeyPart* prev_key_part = NULL;
  while (OB_SUCC(ret) && NULL != tmp_key_part) {
    ObKeyPart* new_key_part = NULL;
    if (OB_ISNULL(new_key_part = create_new_key_part())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc ObKeyPart failed", K(ret));
    } else if (OB_FAIL(new_key_part->deep_node_copy(*tmp_key_part))) {
      LOG_WARN("Copy key part node failed", K(ret));
    } else {
      if (NULL != prev_key_part) {
        prev_key_part->item_next_ = new_key_part;
      } else {
        dest_key_part = new_key_part;
      }
      prev_key_part = new_key_part;
      tmp_key_part = tmp_key_part->item_next_;
    }
  }
  return ret;
}

//  Just and the general item
//       each key part in the general item or_next_ list has same and_next_.
//       each key part in the general item or_next_ list may have item_next_,
//       any key part linked by item_next_ is run-time value,
//       which can not know at this time
//
//  l_gt/r_gt is a group
//  l_gt/r_gt must satisfy following features:
//       1. the key parts must have same key offset
//       2. all key part in same or_next_ list have same and_next_.
//
//  E.g.
//       (A1 or A2 or ... or Am) and (AI or AII or ... or An)
//       ==>
//       (A1I or A1II or ... or A1n or A2I or A2II or ... or A2n or ...... or AmI or AmII or ... or A1mn)
//       Aij = (Ai from (A1 or A2 or ... or Am) intersect Aj from (AI or AII or ... or An))

int ObQueryRange::do_gt_and(ObKeyPart* l_gt, ObKeyPart* r_gt, ObKeyPart*& res_gt)
{
  int ret = OB_SUCCESS;
  if (NULL == l_gt && NULL == r_gt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Wrong argument", K(ret));
  } else if (NULL == l_gt) {
    res_gt = r_gt;
  } else if (NULL == r_gt) {
    res_gt = l_gt;
  } else {
    res_gt = NULL;
    if (OB_UNLIKELY(l_gt->pos_.offset_ != r_gt->pos_.offset_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Wrong argument", K(ret), K(l_gt->pos_.offset_), K(r_gt->pos_.offset_));
    } else {
      ObKeyPart* find_false = NULL;
      ObKeyPart* tail = NULL;
      ObKeyPart* l_cur = NULL;
      ObKeyPart* r_cur = NULL;
      for (l_cur = l_gt; OB_SUCC(ret) && NULL != l_cur; l_cur = l_cur->or_next_) {
        bool find_true = false;
        for (r_cur = r_gt; OB_SUCC(ret) && !find_true && NULL != r_cur; r_cur = r_cur->or_next_) {
          ObKeyPart* result = NULL;
          ObKeyPart* new_l_cur = NULL;
          ObKeyPart* new_r_cur = NULL;
          if (OB_FAIL(deep_copy_key_part_and_items(l_cur, new_l_cur))) {
            LOG_WARN("Light copy key part and items failed", K(ret));
          } else if (OB_FAIL(deep_copy_key_part_and_items(r_cur, new_r_cur))) {
            LOG_WARN("right copy key part and items failed", K(ret));
          } else if (OB_FAIL(
                         do_key_part_node_and(new_l_cur, new_r_cur, result))) {  // do AND of each key part node only
            LOG_WARN("Do key part node intersection failed", K(ret));
          } else if (NULL == result) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null.", K(ret));
          } else if (result->is_always_true()) {
            res_gt = result;
            find_true = true;
          } else if (result->is_always_false()) {
            // ignore
            find_false = result;
          } else {
            if (NULL == res_gt) {
              res_gt = result;
            }
            // link to the or_next_ list
            if (NULL != tail) {
              tail->or_next_ = result;
            }
            tail = result;
          }
        }
      }
      if (OB_SUCC(ret)) {
        // all false
        if (NULL == res_gt) {
          if (NULL == find_false) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("find_false is NULL.", K(ret));
          } else {
            res_gt = find_false;
          }
        }
      }
    }
  }
  return ret;
}

//  do and operation of each graph in l_array and r_array by two path method,
//  then treat the results as or relation
//
//  we do the and operation recursively:
//  1. if left id < right id, result = left-current-key and RECUSIVE_AND(left-rest-keys, right-keys) ;
//  2. if left id > right id, result = right-current-key and RECUSIVE_AND(left-keys, right-rest-keys) ;
//  3. if left id == right id, result = INTERSECT(left-current-key, right-current-key) and RECUSIVE_AND(left-rest-keys,
//  right-rest-keys) ;

int ObQueryRange::and_single_gt_head_graphs(ObKeyPartList& l_array, ObKeyPartList& r_array, ObKeyPartList& res_array)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (l_array.get_size() <= 0 || r_array.get_size() <= 0 || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("And operand can not be empty", K(ret), K(l_array.get_size()), K(r_array.get_size()));
  } else {
    res_array.clear();
    ObKeyPart* find_false = NULL;
    bool always_true = false;
    for (ObKeyPart* l = l_array.get_first(); OB_SUCC(ret) && !always_true && l != l_array.get_header() && NULL != l;
         l = l->get_next()) {
      ObKeyPart* tmp_result = NULL;
      for (ObKeyPart* r = r_array.get_first(); OB_SUCC(ret) && r != r_array.get_header() && NULL != r;
           r = r->get_next()) {
        ObKeyPart* l_cur_gt = NULL;
        ObKeyPart* r_cur_gt = NULL;
        if (OB_FAIL(deep_copy_range_graph(l, l_cur_gt))) {
          LOG_WARN("Left deep copy range graph failed", K(ret));
        } else if (OB_FAIL(deep_copy_range_graph(r, r_cur_gt))) {
          LOG_WARN("Right deep copy range graph failed", K(ret));
        } else if (NULL == l_cur_gt || NULL == r_cur_gt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key_part is null.", K(ret));
        } else {
          ObKeyPart* l_and_next = l_cur_gt->and_next_;
          ObKeyPart* r_and_next = r_cur_gt->and_next_;
          ObKeyPart* rest_result = NULL;
          // false and everything is false
          if (l_cur_gt->is_always_false() || r_cur_gt->is_always_false()) {
            tmp_result = l_cur_gt->is_always_false() ? l_cur_gt : r_cur_gt;
            tmp_result->and_next_ = NULL;
          } else if (r_cur_gt->is_always_true()) {
            tmp_result = l_cur_gt;
            // tmp_result->and_next_ = NULL;
          } else if (l_cur_gt->is_always_true()) {
            tmp_result = r_cur_gt;
            // tmp_result->and_next_ = NULL;
          } else if (contain_row_) {
            if (OB_FAIL(do_row_gt_and(l_cur_gt, r_cur_gt, tmp_result))) {
              LOG_WARN("AND row failed", K(ret));
            } else if (query_range_ctx_ != NULL) {
              query_range_ctx_->precise_range_exprs_.reset();
            }
          } else {  // normal case
            // 1. do and of the first general item
            if (l_cur_gt->pos_.offset_ < r_cur_gt->pos_.offset_) {
              tmp_result = l_cur_gt;
              r_and_next = r_cur_gt;
            } else if (r_cur_gt->pos_.offset_ < l_cur_gt->pos_.offset_) {
              tmp_result = r_cur_gt;
              l_and_next = l_cur_gt;
            } else {  // r_cur_gt->id_ == l_cur_gt->id_
              if (OB_FAIL(do_gt_and(l_cur_gt, r_cur_gt, tmp_result))) {
                LOG_WARN("Do AND of gerneral term failed", K(ret));
              }
            }
            if (OB_SUCCESS != ret) {
              // do nothing
            } else if (OB_ISNULL(tmp_result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tmp_result is null.", K(ret));
            } else {
              tmp_result->and_next_ = NULL;

              // 2. recursive do rest part keys
              if (tmp_result->is_always_false() || tmp_result->is_always_true()) {
                // no need to do rest part
              } else {
                ObKeyPartList and_storage;
                if (NULL == l_and_next) {
                  rest_result = r_and_next;
                } else if (NULL == r_and_next) {
                  rest_result = l_and_next;
                } else if (!and_storage.add_last(l_and_next) || !and_storage.add_last(r_and_next)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("Add ObKeyPart to list failed", K(ret));
                } else if (OB_FAIL(and_range_graph(and_storage, rest_result))) {
                  LOG_WARN("And range graph failed", K(ret));
                } else {
                  // do nothing
                }

                // 3. AND head and rest
                if (OB_SUCC(ret)) {
                  // not contain row, if rest result is false, then whole result is false
                  if (NULL != rest_result && rest_result->is_always_false()) {
                    if (!contain_row_) {
                      tmp_result = rest_result;
                    }
                  } else if (NULL != rest_result && rest_result->is_always_true()) {
                    // no need to link rest part
                  } else {
                    tmp_result->link_gt(rest_result);
                  }
                }
              }
            }
          }
        }

        // 4. add current result to result array
        if (OB_SUCC(ret)) {
          // and the result to result array
          if (OB_ISNULL(tmp_result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tmp_result is null.", K(ret));
          } else {
            if (tmp_result->is_always_false()) {
              // ignore false
              if (!find_false) {
                find_false = tmp_result;
              }
            } else if (tmp_result->is_always_true()) {
              // no need to do any more
              always_true = true;
              res_array.clear();
              if (!res_array.add_last(tmp_result)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("res_array added tmp_result failed.", K(ret));
              }
            } else {
              if (!res_array.add_last(tmp_result)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("res_array added tmp_result failed.", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && res_array.get_size() <= 0) {
      // all false ranges
      if (OB_ISNULL(find_false)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find_false is null.", K(ret));
      } else if (!res_array.add_last(find_false)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_array added find_false failed.", K(ret));
      }
    }
  }
  return ret;
}

//  And all query range graph
//  the initial ranges must come from add_and_item (), that ensure only 1 real-true graph in array
//  and no part false in graph

int ObQueryRange::and_range_graph(ObKeyPartList& ranges, ObKeyPart*& out_key_part)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (ranges.get_size() <= 0 || OB_ISNULL(query_range_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("AND array can not be empty", K(ret), KP(query_range_ctx_), K(ranges.get_size()));
  } else if (1 == ranges.get_size()) {
    out_key_part = ranges.get_first();
    ranges.remove(out_key_part);
  } else {

    // each graph may have many or_next_
    //    we split them to a single and list, then and each of two,
    //    at last do or of all the result
    // E.g.
    //    l_graph: ((A1 and B1) or (A2 and B2)) and other_cnd1
    //                 AND
    //    r_graph: ((A3 and B3) or (A4 and B4)) and other_cnd2
    //    Equal to:
    //                 ((A1 and B1) and (A3 and B3)) and (other_cnd1 and other_cnd2)
    //                 OR
    //                 ((A1 and B1) and (A4 and B4)) and (other_cnd1 and other_cnd2)
    //                 OR
    //                 ((A2 and B2) and (A3 and B3)) and (other_cnd1 and other_cnd2)
    //                 OR
    //                 ((A2 and B2) and (A4 and B4)) and (other_cnd1 and other_cnd2)

    ObKeyPartList res_storage1;
    ObKeyPartList res_storage2;
    ObKeyPart* cur = ranges.get_first();
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur is null.", K(ret));
    } else {
      ObKeyPart* cur_next = cur->get_next();
      ranges.remove_first();
      if (OB_ISNULL(cur_next)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_next is null.", K(ret));
      } else if (cur->is_always_true() || cur->is_always_false()) {
        cur->and_next_ = NULL;
        cur->or_next_ = NULL;
        if (!res_storage1.add_last(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("res_storage1 added cur failed.", K(ret));
        }
      } else if (OB_FAIL(split_general_or(cur, res_storage1))) {
        LOG_WARN("split general or key part failed", K(ret));
      } else {
        // do nothing
      }
    }
    ObKeyPartList* l_array = &res_storage1;
    ObKeyPartList* res_array = &res_storage2;
    int i = 0;
    while (OB_SUCC(ret) && ranges.get_size() > 0) {
      ObKeyPart* other = ranges.get_first();
      ranges.remove_first();
      ++i;
      if (i % 2) {
        l_array = &res_storage1;
        res_array = &res_storage2;
      } else {
        l_array = &res_storage2;
        res_array = &res_storage1;
      }
      ObKeyPartList r_array;
      if (OB_ISNULL(other) || OB_ISNULL(l_array) || OB_ISNULL(l_array->get_first())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("other is null.", K(ret));
      } else if (other->is_always_true() || other->is_always_false()) {
        if (!r_array.add_last(other)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("r_array added other failed", K(ret));
        }
      } else if (OB_FAIL(split_general_or(other, r_array))) {
        LOG_WARN("split general or key part failed", K(ret));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("Split graph failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(and_single_gt_head_graphs(*l_array, r_array, *res_array)))) {
        LOG_WARN("And single general term head graphs failed", K(ret));
      } else if (OB_ISNULL(res_array) || OB_ISNULL(ranges.get_first())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_array or ranges.get_first() is null.", K(ret));
      } else {
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(link_or_graphs(*res_array, out_key_part))) {
        // LOG_WARN("And single general term head graphs failed res_array=%s arr_size=%ld",
        //          ret, to_cstring(*res_array), res_array->count());
        LOG_WARN("And single general term head graphs failed", K(ret), K(res_array->get_size()));
      }
    }
  }
  return ret;
}

// Link all graphs of OR relation

int ObQueryRange::link_or_graphs(ObKeyPartList& storage, ObKeyPart*& out_key_part)
{
  int ret = OB_SUCCESS;
  ObKeyPart* last_gt_tail = NULL;
  if (OB_UNLIKELY(storage.get_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Or storage is empty", K(ret), "query range", to_cstring(*this));
  } else {
    ObKeyPart* first_key_part = storage.get_first();
    bool b_flag = false;
    while (OB_SUCC(ret) && !b_flag && storage.get_size() > 0) {
      ObKeyPart* cur = storage.get_first();
      storage.remove_first();
      if (NULL != cur) {
        if (cur == first_key_part) {
          out_key_part = first_key_part;
          last_gt_tail = first_key_part;
        } else if (cur->is_always_true()) {
          // return ture
          out_key_part = cur;
          b_flag = true;
        } else if (cur->is_always_false()) {
          // ignore false
          // if the first is always_false_, out_key_part has already pointed to it
        } else {
          // we have record the first always_false_,
          // replace it to the first following none-always_false_ key part
          if (NULL == out_key_part) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("out_key_part is null.", K(ret));
          } else if (out_key_part->is_always_false()) {
            out_key_part = cur;
          } else {
            if (NULL == last_gt_tail) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("last_gt_tail is null.", K(ret));
            } else {
              // link the key part to previous or_next_
              last_gt_tail->or_next_ = cur;
            }
          }
        }
        if (OB_SUCC(ret)) {
          // find the last item in gt
          for (last_gt_tail = cur; NULL != last_gt_tail && last_gt_tail->or_next_ != NULL;
               last_gt_tail = last_gt_tail->or_next_)
            ;
        }
      }
    }
  }
  return ret;
}

// Replace unknown value in item_next_ list,
// and intersect them.

int ObQueryRange::definite_key_part(
    ObKeyPart*& key_part, const ParamsIArray& params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (NULL != key_part) {
    for (ObKeyPart* cur = key_part; OB_SUCC(ret) && NULL != cur; cur = cur->item_next_) {
      if (OB_FAIL(replace_questionmark(cur, params, dtc_params))) {
        LOG_WARN("Replace unknown value failed", K(ret));
      } else if (cur->is_always_false()) {  // set key_part false
        key_part->normal_keypart_ = cur->normal_keypart_;
        key_part->key_type_ = T_NORMAL_KEY;
        break;
      } else if (key_part != cur) {
        if (OB_FAIL(key_part->intersect(cur, contain_row_))) {
          LOG_WARN("Intersect key part failed", K(ret));
        } else if (key_part->is_always_false()) {
          break;
        }
      } else {
        // do nothing
      }
    }
    key_part->item_next_ = NULL;
  }
  return ret;
}

int ObQueryRange::union_single_equal_cond(ObKeyPartList& or_list, const ParamsIArray* params,
    const ObDataTypeCastParams& dtc_params, ObKeyPart* cur1, ObKeyPart* cur2)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur1) || OB_ISNULL(cur2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur1 or cur2 is null", K(ret), K(cur1), K(cur2));
  } else if (cur1->and_next_ && cur2->and_next_) {
    ObKeyPart* next_key_part = NULL;
    ObKeyPartList next_or_list;
    if (!next_or_list.add_last(cur1->and_next_) || !next_or_list.add_last(cur2->and_next_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do merge Or graph failed", K(ret));
    } else if (OB_FAIL(or_range_graph(next_or_list, params, next_key_part, dtc_params))) {
      LOG_WARN("Do merge Or graph failed", K(ret));
    } else {
      cur1->and_next_ = next_key_part;
    }
  } else {
    cur1->and_next_ = NULL;
    if (query_range_ctx_ != NULL && query_range_ctx_->cur_expr_is_precise_) {
      query_range_ctx_->cur_expr_is_precise_ = (NULL == cur1->and_next_ && NULL == cur2->and_next_);
    }
    if (OB_FAIL(remove_precise_range_expr(cur1->pos_.offset_ + 1))) {
      LOG_WARN("remove precise range expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    or_list.remove(cur2);
  }
  return ret;
}

// do two path and operation
int ObQueryRange::or_single_head_graphs(
    ObKeyPartList& or_list, const ParamsIArray* params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (or_list.get_size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("And array can not be empty", K(ret));
  } else {
    // 1. replace unknown value, and refresh the graph
    if (NEED_PREPARE_PARAMS == state_) {
      ObKeyPart* find_true = NULL;
      ObKeyPart* cur = or_list.get_first();
      if (OB_ISNULL(params)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("params is needed to extract question mark");
      } else if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur is null.", K(ret));
      } else {
      }
      bool part_is_true = false;
      while (!part_is_true && OB_SUCC(ret) && cur != or_list.get_header()) {
        ObKeyPart* old_tmp = cur;
        ObKeyPart* new_tmp = cur;
        ObKeyPart* and_next = cur->and_next_;
        cur = cur->get_next();
        // replace undefinited value
        if (OB_FAIL(definite_key_part(new_tmp, *params, dtc_params))) {
          LOG_WARN("Fill unknown value failed", K(ret));
        } else if (new_tmp != old_tmp) {
          old_tmp->replace_by(new_tmp);
        } else {
          // do nothing
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_ISNULL(new_tmp) || OB_UNLIKELY(!new_tmp->is_normal_key())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new_tmp is null.", K(new_tmp));
        } else {
          // check result keypart
          if (new_tmp->is_always_true()) {
            if (!find_true) {
              find_true = new_tmp;
              new_tmp->and_next_ = NULL;
            }
            or_list.remove(new_tmp);
            part_is_true = true;
          } else if (new_tmp->is_always_false()) {
            new_tmp->and_next_ = NULL;
            if (or_list.get_size() > 1) {
              or_list.remove(new_tmp);
            }
          } else {
            // handle the rest of the graph recursively
            if (NULL != and_next) {
              // recursively process following and key part
              ObKeyPartList sub_or_list;
              if (OB_FAIL(split_or(and_next, sub_or_list))) {
                LOG_WARN("Split OR failed", K(ret));
              } else if (OB_FAIL(or_range_graph(sub_or_list, params, new_tmp->and_next_, dtc_params))) {
                LOG_WARN("Do OR of range graphs failed", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && NULL != find_true) {
        or_list.clear();
        if (!or_list.add_last(find_true)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Add true keypart graph failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && or_list.get_size() > 1) {  // 2. do OR of the heads of range graphs
      ObKeyPart* cur1 = or_list.get_first();
      while (OB_SUCC(ret) && cur1 != or_list.get_last() && cur1 != or_list.get_header()) {
        bool has_union = false;
        // Union key part who have intersection from next
        ObKeyPart* cur2 = NULL;
        if (NULL == cur1 || NULL == cur1->get_next()) {  // yeti2
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("keypart is null.", K(ret));
        } else {
          cur2 = cur1->get_next();
        }
        while (OB_SUCC(ret) && cur2 != or_list.get_header()) {
          ObKeyPart* cur2_next = cur2->get_next();
          if (cur1->normal_key_is_equal(cur2)) {
            if (OB_FAIL(union_single_equal_cond(or_list, params, dtc_params, cur1, cur2))) {
              LOG_WARN("union single equal cond failed", K(ret));
            }
          } else if (cur1->can_union(cur2)) {
            has_union = true;
            int cmp = cur2->normal_keypart_->start_.compare(cur1->normal_keypart_->start_);
            if (cmp < 0) {
              cur1->normal_keypart_->start_ = cur2->normal_keypart_->start_;
              cur1->normal_keypart_->include_start_ = cur2->normal_keypart_->include_start_;
            } else if (0 == cmp) {
              cur1->normal_keypart_->include_start_ =
                  (cur1->normal_keypart_->include_start_ || cur2->normal_keypart_->include_start_);
            }
            cmp = cur2->normal_keypart_->end_.compare(cur1->normal_keypart_->end_);
            if (cmp > 0) {
              cur1->normal_keypart_->end_ = cur2->normal_keypart_->end_;
              cur1->normal_keypart_->include_end_ = cur2->normal_keypart_->include_end_;
            } else if (0 == cmp) {
              cur1->normal_keypart_->include_end_ =
                  (cur1->normal_keypart_->include_end_ || cur2->normal_keypart_->include_end_);
            }
            if (cur1->and_next_ && cur1->and_next_->equal_to(cur2->and_next_)) {
              // keep and_next_
            } else {
              cur1->and_next_ = NULL;
              if (query_range_ctx_ != NULL && query_range_ctx_->cur_expr_is_precise_) {
                query_range_ctx_->cur_expr_is_precise_ = (NULL == cur1->and_next_ && NULL == cur2->and_next_);
              }
              if (OB_FAIL(remove_precise_range_expr(cur1->pos_.offset_ + 1))) {
                LOG_WARN("remove precise range expr failed", K(ret));
              }
            }
            or_list.remove(cur2);
          }
          if (OB_SUCC(ret)) {
            cur2 = cur2_next;
          }
        }
        if (OB_SUCC(ret)) {
          if (!has_union) {
            cur1 = cur1->get_next();
          }
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::definite_in_range_graph(
    const ParamsIArray& params, ObKeyPart*& root, bool& has_scan_key, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root key is null", K(root));
  } else if (OB_FAIL(definite_key_part(root, params, dtc_params))) {
    LOG_WARN("definite key part failed", K(ret));
  } else if (OB_UNLIKELY(!root->is_normal_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root key is invalid", K(root));
  } else {
    if (!root->is_equal_condition()) {
      has_scan_key = true;
    }
    if (NULL != root->and_next_) {
      if (OB_FAIL(SMART_CALL(definite_in_range_graph(params, root->and_next_, has_scan_key, dtc_params)))) {
        LOG_WARN("definite and_next_ key part failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != root->or_next_) {
      if (OB_FAIL(SMART_CALL(definite_in_range_graph(params, root->or_next_, has_scan_key, dtc_params)))) {
        LOG_WARN("definit or_next_ key part failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::or_range_graph(
    ObKeyPartList& ranges, const ParamsIArray* params, ObKeyPart*& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (ranges.get_size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("OR array can not be empty", K(ranges.get_size()), K_(query_range_ctx));
  } else {

    bool need_do_or = true;
    ObKeyPart* find_false = NULL;
    ObKeyPart* find_true = NULL;
    ObKeyPartList or_list;
    ObKeyPart* head_key_part = ranges.get_first();
    if (OB_ISNULL(head_key_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("head_key_part is null.", K(ret));
    } else {
      int64_t offset = head_key_part->pos_.offset_;
      ObKeyPart* cur = NULL;
      while (OB_SUCC(ret) && ranges.get_size() > 0) {
        cur = ranges.get_first();
        ranges.remove_first();
        if (NULL == cur) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur is null.", K(ret));
        } else if (cur->is_always_false() && NULL == cur->or_next_) {
          if (!find_false) {
            cur->and_next_ = NULL;
            find_false = cur;
          }
        } else if (cur->is_always_true() && NULL == cur->and_next_) {
          if (!find_true) {
            cur->or_next_ = NULL;
            find_true = cur;
          }
          need_do_or = false;
          break;
        } else if (offset != cur->pos_.offset_) {
          // E.g. (k1>0 and k2>0) or (k2<5) ==> (min, max)
          if (NULL == find_true) {
            if (OB_FAIL(alloc_full_key_part(find_true))) {
              LOG_WARN("Get full key part failed", K(ret));
            } else if (OB_ISNULL(find_true)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("find_true is null.", K(ret));
            } else {
              // just to remember the index id and column number
              find_true->id_ = offset < cur->pos_.offset_ ? head_key_part->id_ : cur->id_;
              find_true->pos_ = offset < cur->pos_.offset_ ? head_key_part->pos_ : cur->pos_;
              if (query_range_ctx_ != NULL) {
                query_range_ctx_->cur_expr_is_precise_ = false;
                if (OB_FAIL(remove_precise_range_expr(find_true->pos_.offset_))) {
                  LOG_WARN("remove precise range expr failed", K(ret));
                }
              }
            }
          }
          need_do_or = false;
          break;
        } else if (OB_FAIL(split_or(cur, or_list))) {
          LOG_WARN("Split OR graph failed", K(ret));
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (find_true) {
        out_key_part = find_true;
      } else if (or_list.get_size() <= 0) {
        // all false
        if (OB_ISNULL(find_false)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("find_false is null.", K(ret));
        } else {
          out_key_part = find_false;
        }
      } else if (OB_FAIL(SMART_CALL(or_single_head_graphs(or_list, params, dtc_params)))) {
        LOG_WARN("Or single head graphs failed", K(ret));
      } else if (OB_FAIL(link_or_graphs(or_list, out_key_part))) {
        LOG_WARN("Or single head graphs failed", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

inline int ObQueryRange::gen_simple_get_range(const ObKeyPart& root, ObIAllocator& allocator,
    const ParamsIArray& params, ObQueryRangeArray& ranges, ObGetMethodArray& get_methods,
    const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  ObObj* start = NULL;
  ObObj* end = NULL;
  bool always_false = false;
  bool always_true = false;
  if (OB_ISNULL(start = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for end_obj failed", K(ret));
  } else {
    // do nothing
  }
  const ObKeyPart* cur = &root;
  bool b_flag = false;
  for (int64_t i = 0; OB_SUCC(ret) && !b_flag && i < column_count_; ++i) {
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur is null.", K(ret));
    } else if (OB_UNLIKELY(cur->normal_keypart_->always_false_)) {
      always_false = true;
    } else {
      new (start + i) ObObj(cur->normal_keypart_->start_);
      if (OB_FAIL(get_param_value(*(start + i), params))) {
        LOG_WARN("get end param value failed", K(ret));
      } else if ((start + i)->is_unknown()) {
        // push down?
        always_true = true;
      } else if (ObSQLUtils::is_same_type_for_compare((start + i)->get_meta(), cur->pos_.column_type_.get_obj_meta())) {
        (start + i)->set_collation_type(cur->pos_.column_type_.get_collation_type());
        // copy end
        new (end + i) ObObj(*(start + i));
      } else {
        const ObObj* dest_val = NULL;
        if (OB_UNLIKELY((start + i)->is_null()) && !cur->null_safe_) {
          always_false = true;
        } else if (!(start + i)->is_min_value() && !(start + i)->is_max_value()) {
          ObCastCtx cast_ctx(&allocator, &dtc_params, CM_WARN_ON_FAIL, cur->pos_.column_type_.get_collation_type());
          ObObj& tmp_start = *(start + i);
          ObExpectType expect_type;
          expect_type.set_type(cur->pos_.column_type_.get_type());
          expect_type.set_collation_type(cur->pos_.column_type_.get_collation_type());
          expect_type.set_type_infos(&cur->pos_.get_enum_set_values());
          EXPR_CAST_OBJ_V2(expect_type, tmp_start, dest_val);
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_ISNULL(dest_val)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cast failed.", K(ret));
          } else {
            int64_t cmp = 0;
            ObObjType cmp_type = ObMaxType;
            if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(
                    cmp_type, tmp_start.get_type(), dest_val->get_type()))) {
              LOG_WARN("get compare type failed", K(ret));
            } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(cmp,
                           tmp_start,
                           *dest_val,
                           cast_ctx,
                           cmp_type,
                           cur->pos_.column_type_.get_collation_type()))) {
              LOG_WARN("compare obj value failed", K(ret));
            } else if (0 == cmp) {
              *(start + i) = *dest_val;
              (start + i)->set_collation_type(cur->pos_.column_type_.get_collation_type());
              new (end + i) ObObj(*(start + i));
            } else {
              // always false
              always_false = true;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && always_false) {
      // set whole range max to min
      for (int64_t j = 0; j < column_count_; ++j) {
        (start + j)->set_max_value();
        (end + j)->set_min_value();
      }
      b_flag = true;
    }
    if (OB_SUCC(ret) && always_true) {
      // set whole range max to min
      for (int64_t j = 0; j < column_count_; ++j) {
        (start + j)->set_min_value();
        (end + j)->set_max_value();
      }
      b_flag = true;
    }

    if (OB_SUCC(ret) && !b_flag) {
      cur = cur->and_next_;
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRange* range = NULL;
    if (OB_ISNULL(range = static_cast<ObNewRange*>(allocator.alloc(sizeof(ObNewRange))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for new range failed");
    } else {
      new (range) ObNewRange();
      range->table_id_ = root.id_.table_id_;
      range->start_key_.assign(start, column_count_);
      range->end_key_.assign(end, column_count_);
      if (!always_false && !always_true) {
        range->border_flag_.set_inclusive_start();
        range->border_flag_.set_inclusive_end();
      } else {
        range->border_flag_.unset_inclusive_start();
        range->border_flag_.unset_inclusive_end();
      }
      if (OB_FAIL(ranges.push_back(range))) {
        LOG_WARN("push back range to array failed", K(ret));
      } else if (OB_FAIL(get_methods.push_back(!always_false))) {
        LOG_WARN("push back get method failed", K(ret));
      }
    }
  }
  return ret;
}

// generate always true range or always false range.
int ObQueryRange::generate_true_or_false_range(const ObKeyPart* cur, ObIAllocator& allocator, ObNewRange*& range) const
{
  int ret = OB_SUCCESS;
  ObObj* start = NULL;
  ObObj* end = NULL;
  if (OB_ISNULL(cur)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur is null", K(ret));
  } else if (OB_ISNULL(start = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for end_obj failed", K(ret));
  } else {
    new (start) ObObj();
    new (end) ObObj();
    if (cur->is_always_false()) {
      start[0].set_max_value();
      end[0].set_min_value();
    } else {  //  always true or whole range
      start[0].set_min_value();
      end[0].set_max_value();
    }
    for (int i = 1; i < column_count_; i++) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      start[i] = start[0];
      end[i] = end[0];
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(range = static_cast<ObNewRange*>(allocator.alloc(sizeof(ObNewRange))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      new (range) ObNewRange();
      range->table_id_ = cur->id_.table_id_;
      range->border_flag_.unset_inclusive_start();
      range->border_flag_.unset_inclusive_end();
      range->start_key_.assign(start, column_count_);
      range->end_key_.assign(end, column_count_);
    }
  }
  return ret;
}

// copy existing key parts to range, and fill in the missing key part
int ObQueryRange::generate_single_range(
    ObSearchState& search_state, int64_t column_num, uint64_t table_id, ObNewRange*& range, bool& is_get_range) const
{
  int ret = OB_SUCCESS;
  ObObj* start = NULL;
  ObObj* end = NULL;
  if (column_num <= 0 || search_state.max_exist_index_ < 0 || !search_state.start_ || !search_state.end_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Wrong argument", K(ret));
  } else if (OB_ISNULL(start = static_cast<ObObj*>(search_state.allocator_.alloc(sizeof(ObObj) * column_num)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end = static_cast<ObObj*>(search_state.allocator_.alloc(sizeof(ObObj) * column_num)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for end_obj failed", K(ret));
  } else {
    int64_t max_pred_index = search_state.max_exist_index_;
    for (int i = 0; OB_SUCC(ret) && i < column_num; i++) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      if (i < max_pred_index) {
        // exist key part, deep copy it
        if (OB_FAIL(ob_write_obj(search_state.allocator_, *(search_state.start_ + i), *(start + i)))) {
          LOG_WARN("deep copy start obj failed", K(ret), K(i));
        } else if (OB_FAIL(ob_write_obj(search_state.allocator_, *(search_state.end_ + i), *(end + i)))) {
          LOG_WARN("deep copy end obj failed", K(ret), K(i));
        }
      } else {
        // fill in the missing key part as (min, max)

        // If the start key is included in the range, then we should set
        // min to the rest of the index keys; otherwise, we should set
        // max to the rest of the index keys in order to skip any values
        // in (.. start_key, min:max, min:max ).
        //
        // For example:
        // sql> create table t1(c1 int, c2 int, c3 int, primary key(c1, c2, c3));
        // sql> select * from t1 where c1 > 1 and c1 < 7;
        //
        // The range we get should be (1, max, max) to (7, min, min)
        // and if the condition become c1 >= 1 and c1<= 7, we should get
        // (1, min, min) to (7, max, max) instead.
        //
        // This should be done always with the exception of start_key being
        // min or max, in which case, we should set min to the rest all the
        // time.
        // Same logic applies to the end key as well.
        if (max_pred_index > 0 && !(search_state.start_[max_pred_index - 1]).is_min_value() &&
            search_state.last_include_start_ == false) {
          start[i].set_max_value();
        } else {
          start[i].set_min_value();
        }

        // See above
        if (max_pred_index > 0 && !(search_state.end_[max_pred_index - 1]).is_max_value() &&
            search_state.last_include_end_ == false) {
          end[i].set_min_value();
        } else {
          end[i].set_max_value();
        }
      }
    }
    if (OB_SUCC(ret)) {
      range = static_cast<ObNewRange*>(search_state.allocator_.alloc(sizeof(ObNewRange)));
      if (NULL == range) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      } else {
        new (range) ObNewRange();
        range->table_id_ = table_id;
      }
    }
    if (OB_SUCC(ret)) {
      if (search_state.max_exist_index_ == column_num && search_state.last_include_start_) {
        range->border_flag_.set_inclusive_start();
      } else {
        range->border_flag_.unset_inclusive_start();
      }
      if (search_state.max_exist_index_ == column_num && search_state.last_include_end_) {
        range->border_flag_.set_inclusive_end();
      } else {
        range->border_flag_.unset_inclusive_end();
      }
      range->start_key_.assign(start, column_num);
      range->end_key_.assign(end, column_num);
      is_get_range = (range->start_key_ == range->end_key_) && range->border_flag_.inclusive_start() &&
                     range->border_flag_.inclusive_end();
    }
  }
  return ret;
}

int ObQueryRange::store_range(ObNewRange* range, bool is_get_range, ObSearchState& search_state,
    ObQueryRangeArray& ranges, ObGetMethodArray& get_methods)
{
  int ret = OB_SUCCESS;
  bool is_duplicate = false;
  if (search_state.is_equal_range_) {
    ObRangeWrapper range_wrapper;
    range_wrapper.range_ = range;
    if (OB_HASH_NOT_EXIST == (ret = search_state.range_set_.exist_refactored(range_wrapper))) {
      is_duplicate = false;
      if (OB_FAIL(search_state.range_set_.set_refactored(range_wrapper))) {
        LOG_WARN("set range to range set failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_HASH_EXIST == ret) {
      is_duplicate = true;
      ret = OB_SUCCESS;
    }
  } else {
    is_duplicate = false;
  }
  if (OB_SUCC(ret) && !is_duplicate) {
    if (OB_FAIL(ranges.push_back(range))) {
      LOG_WARN("push back range failed", K(ret));
    } else if (OB_FAIL(get_methods.push_back(is_get_range))) {
      LOG_WARN("push back get_method failed", K(ret));
    }
  }
  return ret;
}

int ObQueryRange::and_first_search(ObSearchState& search_state, ObKeyPart* cur, ObQueryRangeArray& ranges,
    ObGetMethodArray& get_methods, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_ISNULL(search_state.start_) || OB_ISNULL(search_state.end_) || OB_ISNULL(search_state.include_start_) ||
             OB_ISNULL(search_state.include_end_) || OB_ISNULL(cur) || OB_UNLIKELY(!cur->is_normal_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(search_state.start), K_(search_state.end), K(cur));
  } else {
    int copy_depth = search_state.depth_;
    bool copy_produce_range = search_state.produce_range_;

    // 1. generate current key part range, fill missing part as (min, max)
    int i = search_state.depth_;
    if (!cur->is_always_true() && !cur->is_always_false()) {
      for (; cur->pos_.offset_ != -1 /* default */ && i < cur->pos_.offset_; i++) {
        if (share::is_oracle_mode()) {
          search_state.start_[i].set_min_value();
          search_state.end_[i].set_null();
        } else {
          search_state.start_[i].set_null();
          search_state.end_[i].set_max_value();
        }
        search_state.include_start_[i] = false;
        search_state.include_end_[i] = false;
      }
    }
    if (search_state.max_exist_index_ >= search_state.depth_ + 1) {
      // get the larger scope
      if (search_state.start_[i] > cur->normal_keypart_->start_) {
        search_state.start_[i] = cur->normal_keypart_->start_;
        search_state.include_start_[i] = cur->normal_keypart_->include_start_;
      }
      if (search_state.end_[i] < cur->normal_keypart_->end_) {
        search_state.end_[i] = cur->normal_keypart_->end_;
        search_state.include_end_[i] = cur->normal_keypart_->include_end_;
      }
    } else {
      search_state.start_[i] = cur->normal_keypart_->start_;
      search_state.end_[i] = cur->normal_keypart_->end_;
      search_state.max_exist_index_ = search_state.depth_ + 1;
      search_state.include_start_[i] = cur->normal_keypart_->include_start_;
      search_state.include_end_[i] = cur->normal_keypart_->include_end_;
    }

    // 2. process next and key part
    if (NULL != cur->and_next_ && cur->pos_.offset_ >= 0 && cur->pos_.offset_ + 1 == cur->and_next_->pos_.offset_) {
      // current key part is not equal value
      // include_start_/include_end_ is ignored
      if (cur->normal_keypart_->start_ != cur->normal_keypart_->end_) {
        search_state.produce_range_ = false;
      }
      search_state.depth_++;
      if (OB_FAIL(SMART_CALL(and_first_search(search_state, cur->and_next_, ranges, get_methods, dtc_params)))) {
      } else {
        search_state.depth_ = copy_depth;
      }
    }

    // 3. to check if need to  output
    // The role of copy_produec_range is to control whether the range can be output,
    // not all recursion can be output at the end,
    // for example: a>1 and a<=2 and ((b>1 and b <2) or (b> 4, and b <5)).
    // This example cannot be drawn into two ranges, only one range,
    // because if you draw into two ranges->(1, max;2, 2) or (1, max;2, 5) these two ranges overlap
    // if the prefix is a range, the range of the suffix can only be used to determine the boundary value,
    // so only the total start boundary and end boundary of all intervals of the suffix should be recorded
    // this range should be (1, max; 2, 5)
    if (OB_SUCC(ret)) {
      // several case need to output:
      // that previous key parts are all equal value is necessary,
      // 1. current key part is not equal value;
      // 2. current key part is equal value and and_next_ is NULL,
      // 3. current key part is equal value and and_next_ is not NULL, but consequent key does not exist.
      if (copy_produce_range && (NULL == cur->and_next_ || cur->normal_keypart_->start_ != cur->normal_keypart_->end_ ||
                                    cur->pos_.offset_ + 1 != cur->and_next_->pos_.offset_)) {
        ObNewRange* range = NULL;
        bool is_get_range = false;
        search_state.last_include_start_ = true;
        search_state.last_include_end_ = true;
        if (OB_FAIL(search_state.tailor_final_range(column_count_))) {
          LOG_WARN("tailor final range failed", K(ret));
        } else if (OB_FAIL(
                       generate_single_range(search_state, column_count_, cur->id_.table_id_, range, is_get_range))) {
          LOG_WARN("Get single range failed", K(ret));
        } else if (OB_FAIL(store_range(range, is_get_range, search_state, ranges, get_methods))) {
          LOG_WARN("store range failed", K(ret));
        } else {
          // reset search_state
          search_state.depth_ = copy_depth;
          search_state.max_exist_index_ = 0;
          search_state.produce_range_ = copy_produce_range;
        }
      }
    }

    // 4. has or item
    if (OB_SUCC(ret) && NULL != cur->or_next_) {
      cur = cur->or_next_;
      if (OB_FAIL(SMART_CALL(and_first_search(search_state, cur, ranges, get_methods, dtc_params)))) {
        LOG_WARN("and_first_search failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::get_tablet_ranges(common::ObIAllocator& allocator, const ParamsIArray& params,
    ObQueryRangeArray& ranges, bool& all_single_value_ranges, const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  ObGetMethodArray get_methods;
  if (OB_LIKELY(!need_deep_copy())) {
    if (OB_FAIL(get_tablet_ranges(allocator, params, ranges, get_methods, dtc_params))) {
      LOG_WARN("get tablet ranges without deep copy failed", K(ret));
    }
  } else {
    // need to deep copy
    ObQueryRange tmp_query_range(allocator);
    if (OB_FAIL(tmp_query_range.deep_copy(*this))) {
      LOG_WARN("deep copy query range failed", K(ret));
    } else if (OB_FAIL(tmp_query_range.final_extract_query_range(params, dtc_params))) {
      LOG_WARN("final extract query range failed", K(ret));
    } else if (OB_FAIL(tmp_query_range.get_tablet_ranges(ranges, get_methods, dtc_params))) {
      LOG_WARN("get tablet range with deep copy failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t N = get_methods.count();
    all_single_value_ranges = true;
    for (int64_t i = 0; all_single_value_ranges && i < N; ++i) {
      if (!get_methods.at(i)) {
        all_single_value_ranges = false;
      }
    }
  }
  return ret;
}

int ObQueryRange::ObSearchState::tailor_final_range(int64_t column_count)
{
  int ret = OB_SUCCESS;
  bool skip_start = false;
  bool skip_end = false;
  if (OB_ISNULL(include_start_) || OB_ISNULL(include_end_) || OB_ISNULL(start_) || OB_ISNULL(end_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("search state is not init", K_(include_start), K_(include_end), K_(start), K_(end));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < max_exist_index_ && !is_empty_range_; ++i) {
    if (!skip_start) {
      last_include_start_ = (last_include_start_ && include_start_[i]);
      if (!start_[i].is_min_value() && !include_start_[i]) {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < column_count; ++j) {
          start_[j].set_max_value();
        }
        last_include_start_ = false;
        skip_start = true;
      }
    }
    if (!skip_end) {
      last_include_end_ = (last_include_end_ && include_end_[i]);
      if (!end_[i].is_max_value() && !include_end_[i]) {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < column_count; ++j) {
          end_[j].set_min_value();
        }
        last_include_end_ = false;
        skip_end = true;
      }
    }
    if (start_[i].is_min_value() && end_[i].is_max_value()) {
      last_include_start_ = false;
      last_include_end_ = false;
      max_exist_index_ = i + 1;
      break;
    }
  }
  return ret;
}

int ObQueryRange::get_tablet_ranges(ObIAllocator& allocator, const ParamsIArray& params, ObQueryRangeArray& ranges,
    ObGetMethodArray& get_methods, const ObDataTypeCastParams& dtc_params, ObIArray<int64_t>* range_pos) const
{
  int ret = OB_SUCCESS;
  ranges.reset();
  get_methods.reset();
  ObSEArray<ArrayParamInfo, 1> array_params;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_indexs_.count(); ++i) {
    if (param_indexs_.at(i) >= params.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param index is invalid", K(ret), K(param_indexs_.at(i)), K(params));
    } else if (params.at(param_indexs_.at(i)).is_ext()) {
      ArrayParamInfo array_param;
      array_param.param_index_ = param_indexs_.at(i);
      if (OB_FAIL(array_params.push_back(array_param))) {
        LOG_WARN("store array param failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (array_params.empty()) {
      if (OB_FAIL(inner_get_tablet_ranges(allocator, params, ranges, get_methods, dtc_params))) {
        LOG_WARN("inner get tablet ranges failed", K(ret));
      }
    } else {
      ArrayParamInfo& first_array_param = array_params.at(0);
      ParamsIArray& params_for_update = const_cast<ParamsIArray&>(params);
      int64_t array_param_count = 1;
      for (int64_t i = 0; OB_SUCC(ret) && i < array_param_count; ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < array_params.count(); ++j) {
          ArrayParamInfo& array_param = array_params.at(j);
          int64_t param_index = array_param.param_index_;
        }
        if (range_pos != NULL) {
          if (OB_FAIL(range_pos->push_back(ranges.count()))) {
            LOG_WARN("store range pos failed", K(ret), K(ranges));
          }
          LOG_DEBUG("print range array position", K(ret), K(ranges), KPC(range_pos));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(inner_get_tablet_ranges(allocator, params_for_update, ranges, get_methods, dtc_params))) {
            LOG_WARN("inner get tablet ranges failed", K(ret));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < array_params.count(); ++i) {
        ArrayParamInfo& array_param = array_params.at(i);
        params_for_update.at(array_param.param_index_).set_param_meta();
      }
    }
  }
  return ret;
}

// @notice must call need_deep_copy() before calling this interface to determine
// whether it is possible to perform final extract without copying
OB_INLINE int ObQueryRange::inner_get_tablet_ranges(ObIAllocator& allocator, const ParamsIArray& params,
    ObQueryRangeArray& ranges, ObGetMethodArray& get_methods, const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_graph_.key_part_head_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table_graph_.key_part_head_ is not inited.", K(ret));
  } else if (table_graph_.key_part_head_->pos_.offset_ < 0) {
    ObNewRange* range = NULL;
    bool is_get_range = false;
    if (OB_FAIL(generate_true_or_false_range(table_graph_.key_part_head_, allocator, range))) {
      LOG_WARN("get true_or_false range failed", K(ret));
    } else if (OB_FAIL(ranges.push_back(range))) {
      LOG_WARN("push back range failed", K(ret));
    } else if (OB_FAIL(get_methods.push_back(is_get_range))) {
      LOG_WARN("push back get_method failed", K(ret));
    } else {
    }
  } else if (table_graph_.is_precise_get_) {
    if (OB_FAIL(
            gen_simple_get_range(*table_graph_.key_part_head_, allocator, params, ranges, get_methods, dtc_params))) {
      LOG_WARN("gen simple get range failed", K(ret));
    }
  } else {
    ObSearchState search_state(allocator);
    void* start_ptr = NULL;
    void* end_ptr = NULL;

    if (OB_ISNULL(start_ptr = search_state.allocator_.alloc(sizeof(ObObj) * column_count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory for start_ptr failed", K(ret));
    } else if (OB_ISNULL(end_ptr = search_state.allocator_.alloc(sizeof(ObObj) * column_count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory for end_ptr failed", K(ret));
    } else if (OB_ISNULL(search_state.include_start_ =
                             static_cast<bool*>(search_state.allocator_.alloc(sizeof(bool) * column_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory for search state start failed", K(ret));
    } else if (OB_ISNULL(search_state.include_end_ =
                             static_cast<bool*>(search_state.allocator_.alloc(sizeof(bool) * column_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory for search state end failed", K(ret));
    } else {
      search_state.start_ = new (start_ptr) ObObj[column_count_];
      search_state.end_ = new (end_ptr) ObObj[column_count_];
      search_state.max_exist_index_ = column_count_;
      search_state.last_include_start_ = true;
      search_state.last_include_end_ = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
      search_state.start_[i].set_min_value();
      search_state.end_[i].set_max_value();
      search_state.include_start_[i] = false;
      search_state.include_end_[i] = false;
    }
    for (ObKeyPart* cur = table_graph_.key_part_head_; OB_SUCC(ret) && NULL != cur && !search_state.is_empty_range_;
         cur = cur->and_next_) {
      if (OB_FAIL(get_single_key_value(cur, params, search_state, dtc_params))) {
        LOG_WARN("get single key value failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(search_state.tailor_final_range(column_count_))) {
      LOG_WARN("tailor final range failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObNewRange* range = NULL;
      bool is_get_range = false;
      if (OB_FAIL(generate_single_range(
              search_state, column_count_, table_graph_.key_part_head_->id_.table_id_, range, is_get_range))) {
        LOG_WARN("generate single range failed", K(ret));
      } else if (OB_FAIL(ranges.push_back(range))) {
        LOG_WARN("push back range to array failed", K(ret));
      } else if (OB_FAIL(get_methods.push_back(is_get_range))) {
        LOG_WARN("push back get method to array failed", K(ret));
      }
    }
  }
  return ret;
}

#define CAST_VALUE_TYPE(expect_type, column_type, start, include_start, end, include_end)                           \
  if (OB_SUCC(ret)) {                                                                                               \
    ObObj cast_obj;                                                                                                 \
    const ObObj* dest_val = NULL;                                                                                   \
    if (!start.is_min_value() && !start.is_max_value() && !start.is_unknown() &&                                    \
        !ObSQLUtils::is_same_type_for_compare(start.get_meta(), column_type.get_obj_meta())) {                      \
      ObCastCtx cast_ctx(&allocator, &dtc_params, CM_WARN_ON_FAIL, expect_type.get_collation_type());               \
      ObObj& tmp_start = start;                                                                                     \
      EXPR_CAST_OBJ_V2(expect_type, tmp_start, dest_val);                                                           \
      if (OB_FAIL(ret)) {                                                                                           \
        LOG_WARN("cast obj to dest type failed", K(ret), K(start), K(expect_type));                                 \
      } else if (OB_ISNULL(dest_val)) {                                                                             \
        ret = OB_ERR_UNEXPECTED;                                                                                    \
        LOG_WARN("dest_val is null.", K(ret));                                                                      \
      } else {                                                                                                      \
        int64_t cmp = 0;                                                                                            \
        ObObjType cmp_type = ObMaxType;                                                                             \
        if (OB_FAIL(                                                                                                \
                ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, start.get_type(), dest_val->get_type()))) { \
          LOG_WARN("get compare type failed", K(ret));                                                              \
        } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(                                              \
                       cmp, start, *dest_val, cast_ctx, cmp_type, column_type.get_collation_type()))) {             \
          LOG_WARN("compare obj value failed", K(ret));                                                             \
        } else if (cmp < 0) {                                                                                       \
          include_start = true;                                                                                     \
        } else if (cmp > 0) {                                                                                       \
          include_start = false;                                                                                    \
        }                                                                                                           \
        start = *dest_val;                                                                                          \
      }                                                                                                             \
    }                                                                                                               \
    if (OB_SUCC(ret)) {                                                                                             \
      if (!end.is_min_value() && !end.is_max_value() && !end.is_unknown() &&                                        \
          !ObSQLUtils::is_same_type_for_compare(end.get_meta(), column_type.get_obj_meta())) {                      \
        ObCastCtx cast_ctx(&allocator, &dtc_params, CM_WARN_ON_FAIL, expect_type.get_collation_type());             \
        ObObj& tmp_end = end;                                                                                       \
        EXPR_CAST_OBJ_V2(expect_type, tmp_end, dest_val);                                                           \
        if (OB_FAIL(ret)) {                                                                                         \
          LOG_WARN("cast obj to dest type failed", K(ret), K(end), K(expect_type));                                 \
        } else {                                                                                                    \
          int64_t cmp = 0;                                                                                          \
          ObObjType cmp_type = ObMaxType;                                                                           \
          if (OB_FAIL(                                                                                              \
                  ObExprResultTypeUtil::get_relational_cmp_type(cmp_type, end.get_type(), dest_val->get_type()))) { \
            LOG_WARN("get compare type failed", K(ret));                                                            \
          } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(                                            \
                         cmp, end, *dest_val, cast_ctx, cmp_type, column_type.get_collation_type()))) {             \
            LOG_WARN("compare obj value failed", K(ret));                                                           \
          } else if (cmp > 0) {                                                                                     \
            include_end = true;                                                                                     \
          } else if (cmp < 0) {                                                                                     \
            include_end = false;                                                                                    \
          }                                                                                                         \
          end = *dest_val;                                                                                          \
        }                                                                                                           \
      }                                                                                                             \
    }                                                                                                               \
    if (OB_SUCC(ret)) {                                                                                             \
      start.set_collation_type(expect_type.get_collation_type());                                                   \
      end.set_collation_type(expect_type.get_collation_type());                                                     \
    }                                                                                                               \
  }

inline int ObQueryRange::get_single_key_value(const ObKeyPart* key, const ParamsIArray& params,
    ObSearchState& search_state, const ObDataTypeCastParams& dtc_params) const
{
  int ret = OB_SUCCESS;
  for (const ObKeyPart* cur = key; OB_SUCC(ret) && NULL != cur && cur->is_normal_key() && !search_state.is_empty_range_;
       cur = cur->item_next_) {
    ObObj start = cur->normal_keypart_->start_;
    ObObj end = cur->normal_keypart_->end_;
    bool include_start = cur->normal_keypart_->include_start_;
    bool include_end = cur->normal_keypart_->include_end_;
    ObExpectType expect_type;
    expect_type.set_type(cur->pos_.column_type_.get_type());
    expect_type.set_collation_type(cur->pos_.column_type_.get_collation_type());
    expect_type.set_type_infos(&cur->pos_.get_enum_set_values());
    ObIAllocator& allocator = search_state.allocator_;
    if (cur->normal_keypart_->always_false_) {
      start.set_max_value();
      end.set_min_value();
      include_start = false;
      include_end = false;
    } else if (cur->normal_keypart_->always_true_) {
      start.set_min_value();
      end.set_max_value();
      include_start = false;
      include_end = false;
    } else {
      if (start.is_unknown()) {
        if (OB_FAIL(get_param_value(start, params))) {
          LOG_WARN("get param value failed", K(ret));
        } else if (!cur->null_safe_ && start.is_null()) {
          start.set_max_value();
          end.set_min_value();
          include_start = false;
          include_end = false;
        } else if (start.is_unknown()) {
          start.set_min_value();
          include_start = false;
          end.set_max_value();
          include_end = false;
        }
      }
      if (OB_SUCC(ret) && end.is_unknown()) {
        if (OB_FAIL(get_param_value(end, params))) {
          LOG_WARN("get param value failed", K(ret));
        } else if (!cur->null_safe_ && end.is_null()) {
          start.set_max_value();
          end.set_min_value();
          include_start = false;
          include_end = false;
        } else if (end.is_unknown()) {
          start.set_min_value();
          include_start = false;
          end.set_max_value();
          include_end = false;
        }
      }
    }
    CAST_VALUE_TYPE(expect_type, cur->pos_.column_type_, start, include_start, end, include_end);
    if (OB_SUCC(ret)) {
      search_state.depth_ = static_cast<int>(cur->pos_.offset_);
      if (OB_FAIL(search_state.intersect(start, include_start, end, include_end))) {
        LOG_WARN("intersect current key part failed", K(ret));
      }
    }
  }
  return ret;
}
#undef CAST_VALUE_TYPE

int ObQueryRange::get_tablet_ranges(
    ObQueryRangeArray& ranges, ObGetMethodArray& get_methods, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  ObSearchState search_state(allocator_);

  ranges.reset();
  get_methods.reset();
  if (CAN_READ != state_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Can not get query range before final extraction", K(ret), K_(state));
  } else if (OB_ISNULL(table_graph_.key_part_head_) || OB_UNLIKELY(!table_graph_.key_part_head_->is_normal_key())) {
    ret = OB_NOT_INIT;
    LOG_WARN("table_graph_.key_part_head_ is not inited.", K(table_graph_.key_part_head_));
  } else if (table_graph_.is_equal_range_ && OB_FAIL(search_state.range_set_.create(RANGE_BUCKET_SIZE))) {
    LOG_WARN("create range set bucket failed", K(ret));
  } else if (table_graph_.key_part_head_->pos_.offset_ > 0) {
    SQL_REWRITE_LOG(DEBUG, "get table range from index failed, whole range will returned", K(ret));
    ObNewRange* range = NULL;
    bool is_get_range = false;
    if (OB_FAIL(generate_true_or_false_range(table_graph_.key_part_head_, allocator_, range))) {
      LOG_WARN("generate true_or_false range failed", K(ret));
    } else if (OB_FAIL(ranges.push_back(range))) {
      LOG_WARN("push back range failed", K(ret));
    } else if (OB_FAIL(get_methods.push_back(is_get_range))) {
      LOG_WARN("push back get_method failed", K(ret));
    } else {
    }
  } else {
    ret = OB_SUCCESS;
    search_state.depth_ = 0;
    search_state.max_exist_index_ = 0;
    search_state.last_include_start_ = false;
    search_state.last_include_end_ = false;
    search_state.produce_range_ = true;
    search_state.is_equal_range_ = table_graph_.is_equal_range_;
    search_state.start_ = static_cast<ObObj*>(search_state.allocator_.alloc(sizeof(ObObj) * column_count_));
    search_state.end_ = static_cast<ObObj*>(search_state.allocator_.alloc(sizeof(ObObj) * column_count_));
    search_state.include_start_ = static_cast<bool*>(search_state.allocator_.alloc(sizeof(bool) * column_count_));
    search_state.include_end_ = static_cast<bool*>(search_state.allocator_.alloc(sizeof(bool) * column_count_));
    if (OB_ISNULL(search_state.start_) || OB_ISNULL(search_state.end_) || OB_ISNULL(search_state.include_start_) ||
        OB_ISNULL(search_state.include_end_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed",
          K(search_state.start_),
          K(search_state.end_),
          K_(search_state.include_start),
          K_(search_state.include_end),
          K_(column_count));
    } else {
      for (int i = 0; i < column_count_; ++i) {
        new (search_state.start_ + i) ObObj();
        new (search_state.end_ + i) ObObj();
        (search_state.start_ + i)->set_max_value();
        (search_state.end_ + i)->set_min_value();
        search_state.include_start_[i] = false;
        search_state.include_end_[i] = false;
      }
      if (OB_FAIL(and_first_search(search_state, table_graph_.key_part_head_, ranges, get_methods, dtc_params))) {
        LOG_WARN("and_first_search failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SQL_REWRITE_LOG(DEBUG, "get range success", K(ranges));
    if (table_graph_.is_equal_range_) {
      search_state.range_set_.destroy();
    }
  }
  return ret;
}

int ObQueryRange::alloc_empty_key_part(ObKeyPart*& out_key_part)
{
  int ret = OB_SUCCESS;
  out_key_part = NULL;
  if (OB_ISNULL(out_key_part = create_new_key_part())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc ObKeyPart failed", K(ret));
  } else if (OB_FAIL(out_key_part->create_normal_key())) {
    LOG_WARN("create normal key failed", K(ret));
  } else {
    out_key_part->normal_keypart_->start_.set_max_value();
    out_key_part->normal_keypart_->end_.set_min_value();
    out_key_part->normal_keypart_->always_false_ = true;
    out_key_part->normal_keypart_->include_start_ = false;
    out_key_part->normal_keypart_->include_end_ = false;
  }
  return ret;
}

int ObQueryRange::alloc_full_key_part(ObKeyPart*& out_key_part)
{
  int ret = OB_SUCCESS;
  out_key_part = NULL;
  if (OB_ISNULL(out_key_part = create_new_key_part())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc ObKeyPart failed", K(ret));
  } else if (OB_FAIL(out_key_part->create_normal_key())) {
    LOG_WARN("create normal key failed", K(ret));
  } else {
    out_key_part->normal_keypart_->start_.set_min_value();
    out_key_part->normal_keypart_->end_.set_max_value();
    out_key_part->normal_keypart_->always_true_ = true;
    out_key_part->normal_keypart_->include_start_ = false;
    out_key_part->normal_keypart_->include_end_ = false;
  }
  return ret;
}

#define FINAL_EXTRACT(graph)                                                    \
  if (OB_SUCC(ret)) {                                                           \
    or_array.clear();                                                           \
    if (!or_array.add_last(graph)) {                                            \
      ret = OB_ERR_UNEXPECTED;                                                  \
      LOG_WARN("Add query graph to list failed", K(ret));                       \
    } else if (OB_FAIL(or_range_graph(or_array, &params, graph, dtc_params))) { \
      LOG_WARN("Do OR of range graph failed", K(ret));                          \
    }                                                                           \
  }
int ObQueryRange::final_extract_query_range(const ParamsIArray& params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  SQL_REWRITE_LOG(DEBUG, "final extract query range", K(params));
  if (state_ == NEED_PREPARE_PARAMS && NULL != table_graph_.key_part_head_) {
    ObKeyPartList or_array;
    // find all key part path and do OR option
    bool has_scan_key = false;
    if (table_graph_.is_equal_range_) {
      if (OB_FAIL(definite_in_range_graph(params, table_graph_.key_part_head_, has_scan_key, dtc_params))) {
        LOG_WARN("definite in range graph failed", K(ret));
      } else if (has_scan_key) {
        table_graph_.is_equal_range_ = false;
        FINAL_EXTRACT(table_graph_.key_part_head_);
      }
    } else {
      FINAL_EXTRACT(table_graph_.key_part_head_);
    }
    if (OB_SUCC(ret)) {
      state_ = CAN_READ;
    }
  }
  return ret;
}
#undef FINAL_EXTRACT

inline int ObQueryRange::get_param_value(ObObj& val, const ParamsIArray& params) const
{
  int ret = OB_SUCCESS;
  int64_t param_idx = OB_INVALID_ID;

  if (val.is_unknown()) {
    if (OB_FAIL(val.get_unknown(param_idx))) {
      LOG_WARN("get question mark value failed", K(ret), K(val));
    } else if (param_idx < 0) {
      ret = OB_ERR_ILLEGAL_INDEX;
      LOG_WARN("Wrong index of question mark position", K(ret), K(param_idx), K(params));
    } else if (param_idx >= params.count()) {
    } else {
      val = params.at(param_idx);
      if (val.is_nop_value()) {
        ret = OB_ERR_NOP_VALUE;
      }
    }
  }
  return ret;
}

int ObQueryRange::replace_questionmark(
    ObKeyPart* root, const ParamsIArray& params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root=null ", K(ret));
  } else if (!root->is_like_key()) {
    if (root->normal_keypart_->start_.is_unknown()) {
      if (OB_FAIL(get_param_value(root->normal_keypart_->start_, params))) {
        LOG_WARN("get param value failed", K(ret));
      } else if (!root->null_safe_ && root->normal_keypart_->start_.is_null()) {
        root->normal_keypart_->always_false_ = true;
      } else if (root->normal_keypart_->start_.is_unknown()) {
        root->normal_keypart_->start_.set_min_value();
        root->normal_keypart_->include_start_ = false;
      }
    }
    if (OB_SUCC(ret) && root->normal_keypart_->end_.is_unknown()) {
      if (OB_FAIL(get_param_value(root->normal_keypart_->end_, params))) {
        LOG_WARN("get param value failed", K(ret));
      } else if (!root->null_safe_ && root->normal_keypart_->end_.is_null()) {
        root->normal_keypart_->always_false_ = true;
      } else if (root->normal_keypart_->end_.is_unknown()) {
        root->normal_keypart_->end_.set_max_value();
        root->normal_keypart_->include_end_ = false;
      }
    }
    if (OB_SUCC(ret) && root->normal_keypart_->always_false_) {
      root->normal_keypart_->start_.set_max_value();
      root->normal_keypart_->end_.set_min_value();
      root->normal_keypart_->include_start_ = false;
      root->normal_keypart_->include_end_ = false;
    }
  } else {
    if (OB_FAIL(get_param_value(root->like_keypart_->pattern_, params))) {
      LOG_WARN("get param value failed", K(ret));
    } else if (OB_FAIL(get_param_value(root->like_keypart_->escape_, params))) {
      LOG_WARN("get param value failed", K(ret));
    } else if (OB_FAIL(
                   get_like_range(root->like_keypart_->pattern_, root->like_keypart_->escape_, *root, dtc_params))) {
      LOG_WARN("get like range failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(root->cast_value_type(dtc_params, contain_row_))) {
    LOG_WARN("cast value type failed", K(ret));
  }
  return ret;
}

int ObQueryRange::get_like_range(
    const ObObj& pattern, const ObObj& escape, ObKeyPart& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  ObString pattern_str;
  ObString escape_str;
  ObObj start;
  ObObj end;
  void* min_str_buf = NULL;
  void* max_str_buf = NULL;
  int32_t col_len = out_key_part.pos_.column_type_.get_accuracy().get_length();
  ObCollationType cs_type = out_key_part.pos_.column_type_.get_collation_type();
  size_t min_str_len = 0;
  size_t max_str_len = 0;
  ObObj pattern_buf_obj;
  ObObj escape_buf_obj;
  const ObObj* pattern_val = NULL;
  // const ObObj *escape_val = NULL;
  if (OB_FAIL(out_key_part.create_normal_key())) {
    LOG_WARN("create normal key failed", K(ret));
  } else if (pattern.is_null()) {
    // a like null return empty range
    out_key_part.normal_keypart_->start_.set_max_value();
    out_key_part.normal_keypart_->end_.set_min_value();
    out_key_part.normal_keypart_->include_start_ = false;
    out_key_part.normal_keypart_->include_end_ = false;
    out_key_part.normal_keypart_->always_false_ = true;
    out_key_part.normal_keypart_->always_true_ = false;
  } else if (!pattern.is_string_type() || (!escape.is_string_type() && !escape.is_null()) || col_len <= 0) {
    // 1 like 1 return whole range
    out_key_part.normal_keypart_->start_.set_min_value();
    out_key_part.normal_keypart_->end_.set_max_value();
    out_key_part.normal_keypart_->include_start_ = false;
    out_key_part.normal_keypart_->include_end_ = false;
    out_key_part.normal_keypart_->always_false_ = false;
    out_key_part.normal_keypart_->always_true_ = true;
  } else if (OB_FAIL(cast_like_obj_if_needed(pattern, pattern_buf_obj, pattern_val, out_key_part, dtc_params))) {
    LOG_WARN("failed to cast like obj if needed", K(ret));
  } else if (OB_FAIL(pattern_val->get_string(pattern_str))) {
    LOG_WARN("get varchar failed", K(ret), K(pattern));
  } else {
    int64_t mbmaxlen = 1;
    ObString escape_val;
    if (escape.is_null()) {
      escape_str.assign_ptr("\\", 1);
    } else if (ObCharset::is_cs_nonascii(escape.get_collation_type())) {
      if (OB_FAIL(escape.get_string(escape_val))) {
        LOG_WARN("failed to get escape string", K(escape), K(ret));
      } else if (OB_FAIL(ObCharset::charset_convert(allocator_,
                     escape_val,
                     escape.get_collation_type(),
                     CS_TYPE_UTF8MB4_GENERAL_CI,
                     escape_str,
                     true))) {
        LOG_WARN("failed to do charset convert", K(ret), K(escape_val));
      }
    } else if (OB_FAIL(escape.get_string(escape_str))) {
      LOG_WARN("failed to get escape string", K(escape), K(ret));
    } else { /* do nothing */
    }
    if (OB_FAIL(ret)) {
      // do nothing;
    } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
      LOG_WARN("fail to get mbmaxlen", K(ret), K(cs_type), K(pattern), K(escape));
    } else if (OB_ISNULL(escape_str.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Escape str should not be NULL", K(ret));
    } else if (OB_UNLIKELY((lib::is_oracle_mode() && 1 != escape_str.length()) ||
                           (!lib::is_oracle_mode() && 1 > escape_str.length()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to check escape length", K(escape_str), K(escape_str.length()));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ESCAPE");
    } else {
    }

    if (OB_SUCC(ret)) {
      // convert character counts to len in bytes
      col_len = static_cast<int32_t>(col_len * mbmaxlen);
      min_str_len = col_len;
      max_str_len = col_len;
      if (OB_ISNULL(min_str_buf = allocator_.alloc(min_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(min_str_len));
      } else if (OB_ISNULL(max_str_buf = allocator_.alloc(max_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(max_str_len));
      } else if (escape_str.length() > 1 || OB_FAIL(ObCharset::like_range(cs_type,
                                                pattern_str,
                                                *(escape_str.ptr()),
                                                static_cast<char*>(min_str_buf),
                                                &min_str_len,
                                                static_cast<char*>(max_str_buf),
                                                &max_str_len))) {
        // set whole range
        out_key_part.normal_keypart_->start_.set_min_value();
        out_key_part.normal_keypart_->end_.set_max_value();
        out_key_part.normal_keypart_->include_start_ = false;
        out_key_part.normal_keypart_->include_end_ = false;
        out_key_part.normal_keypart_->always_false_ = false;
        out_key_part.normal_keypart_->always_true_ = true;
        ret = OB_SUCCESS;
      } else {
        ObObj& start = out_key_part.normal_keypart_->start_;
        ObObj& end = out_key_part.normal_keypart_->end_;
        start.set_collation_type(out_key_part.pos_.column_type_.get_collation_type());
        start.set_string(out_key_part.pos_.column_type_.get_type(),
            static_cast<char*>(min_str_buf),
            static_cast<int32_t>(min_str_len));
        end.set_collation_type(out_key_part.pos_.column_type_.get_collation_type());
        end.set_string(out_key_part.pos_.column_type_.get_type(),
            static_cast<char*>(max_str_buf),
            static_cast<int32_t>(max_str_len));
        out_key_part.normal_keypart_->include_start_ = true;
        out_key_part.normal_keypart_->include_end_ = true;
        out_key_part.normal_keypart_->always_false_ = false;
        out_key_part.normal_keypart_->always_true_ = false;

        /// check if is precise
        check_like_range_precise(pattern_str, static_cast<char*>(max_str_buf), max_str_len, *(escape_str.ptr()));
      }
      if (NULL != min_str_buf) {
        allocator_.free(min_str_buf);
        min_str_buf = NULL;
      }
      if (NULL != max_str_buf) {
        allocator_.free(max_str_buf);
        max_str_buf = NULL;
      }
    }
  }
  return ret;
}

// Generaal term: if one or more same key parts with OR realtion have same and_next_,
//                        we call them general term(GT)
// E.g.
//       A and B, A is GT
//       A and (B1 or B2) and C, (B1 or B2) is GT.
//       A and (((B1 or B2) and C1) or (B3 and c2)) and D,
//       (((B1 or B2) and C1) or (B3 and c2)) is not GT.
//
// Our query range graph must abide by following rules:
// 1. on key part is pointed to by more than one key parts, except they are GT.
// 2. a key part can only point to the first member of a GT.
//
// So, we do not generate following graph:
//       A--B1--C1--D
//             |
//             B2(points to D too)
// but generate
//       A--B1--C1--D1
//             |
//             B2--D2
//       (key part D1 is equal to D2, but has different storage)
// '--', means and_next_;
// '|', means or_next_;

// Out of usage, ObKeyPart is free by key_part_store_

// maybe use later

// void ObQueryRange::free_range_graph(ObKeyPart *&graph)
//{
//  ObKeyPart *next_gt = NULL;
//  for (ObKeyPart *cur_gt = graph; cur_gt != NULL; cur_gt = next_gt) {
//    next_gt = cur_gt->general_or_next();
//    free_range_graph(cur_gt->and_next_);
//    ObKeyPart *next_or = NULL;
//    for (ObKeyPart *cur_or = cur_gt;
//         cur_or != NULL && cur_or->and_next_ == cur_gt->and_next_;
//         cur_or = next_or) {
//      next_or = cur_or->or_next_;
//      ObKeyPart *next_item = NULL;
//      for (ObKeyPart *item = cur_or->item_next_; item != NULL; item = next_item) {
//        next_item = item->item_next_;
//        ObKeyPart::free(item);
//      }
//      ObKeyPart::free(cur_or);
//    }
//  }
//  graph = NULL;
//}

ObKeyPart* ObQueryRange::create_new_key_part()
{
  void* ptr = NULL;
  ObKeyPart* key_part = NULL;
  if (NULL != (ptr = allocator_.alloc(sizeof(ObKeyPart)))) {
    key_part = new (ptr) ObKeyPart(allocator_);
    if (OB_SUCCESS != key_part_store_.store_obj(key_part)) {
      key_part->~ObKeyPart();
      key_part = NULL;
      LOG_WARN("Store ObKeyPart failed");
    }
  }
  return key_part;
}

// Deep copy this key part node only, not include any item in XXX_next_ list

ObKeyPart* ObQueryRange::deep_copy_key_part(ObKeyPart* key_part)
{
  int ret = OB_SUCCESS;
  ObKeyPart* new_key_part = NULL;
  if (key_part) {
    if (OB_ISNULL(new_key_part = create_new_key_part())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Get new key part failed", K(ret));
    } else if ((ret = new_key_part->deep_node_copy(*key_part)) != OB_SUCCESS) {
      LOG_WARN("Copy key part node failed", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ret)) {
    new_key_part = NULL;
  }
  return new_key_part;
}

int ObQueryRange::serialize_range_graph(
    const ObKeyPart* cur, const ObKeyPart* pre_and_next, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_ISNULL(cur)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cur is null.", K(ret));
  } else {
    bool has_and_next = cur->and_next_ ? true : false;
    bool has_or_next = cur->or_next_ ? true : false;
    bool encode_and_next = false;

    OB_UNIS_ENCODE(has_and_next);
    OB_UNIS_ENCODE(has_or_next);
    if (OB_SUCC(ret) && has_and_next) {
      encode_and_next = cur->and_next_ == pre_and_next ? false : true;
      OB_UNIS_ENCODE(encode_and_next);
      if (OB_SUCC(ret) && encode_and_next) {
        if (OB_FAIL(SMART_CALL(serialize_range_graph(cur->and_next_, NULL, buf, buf_len, pos)))) {
          LOG_WARN("serialize and_next_ child graph failed", K(ret));
        }
      }
    }
    if (OB_SUCC((ret)) && OB_FAIL(serialize_cur_keypart(*cur, buf, buf_len, pos))) {
      LOG_WARN("serialize current key part failed", K(ret));
    }
    if (OB_SUCC(ret) && has_or_next) {
      if (OB_FAIL(SMART_CALL(serialize_range_graph(cur->or_next_, cur->and_next_, buf, buf_len, pos)))) {
        LOG_WARN("serialize or_next_ child graph failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::deserialize_range_graph(
    ObKeyPart* pre_key, ObKeyPart*& cur, const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  bool has_and_next = false;
  bool has_or_next = false;
  bool encode_and_next = false;
  ObKeyPart* and_next = NULL;

  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    OB_UNIS_DECODE(has_and_next);
    OB_UNIS_DECODE(has_or_next);
    if (OB_SUCC(ret) && has_and_next) {
      OB_UNIS_DECODE(encode_and_next);
      if (OB_SUCC(ret) && encode_and_next) {
        if (OB_FAIL(SMART_CALL(deserialize_range_graph(NULL, and_next, buf, data_len, pos)))) {
          LOG_WARN("deserialize and_next_ child graph failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(deserialize_cur_keypart(cur, buf, data_len, pos))) {
      LOG_WARN("deserialize current key part failed", K(ret));
    }
    // build and_next_ child grap and pre_key with current key part
    if (OB_SUCC(ret)) {
      if (encode_and_next) {
        cur->and_next_ = and_next;
      } else if (has_and_next) {
        if (OB_ISNULL(pre_key)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("pre_key is null.", K(ret));
        } else {
          cur->and_next_ = pre_key->and_next_;
        }
      } else {
        // do nothing
      }
      if (pre_key) {
        pre_key->or_next_ = cur;
      }
    }
    if (OB_SUCC(ret) && has_or_next) {
      if (OB_FAIL(SMART_CALL(deserialize_range_graph(cur, cur->or_next_, buf, data_len, pos)))) {
        LOG_WARN("deserialize or_next_ child graph failed", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryRange::serialize_cur_keypart(const ObKeyPart& cur, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool has_item_next = (cur.item_next_ != NULL);
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    OB_UNIS_ENCODE(cur);
    OB_UNIS_ENCODE(has_item_next);
    if (OB_SUCC(ret) && has_item_next) {
      if (OB_FAIL(SMART_CALL(serialize_cur_keypart(*cur.item_next_, buf, buf_len, pos)))) {
        LOG_WARN("serialize cur keypart failed", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObQueryRange::get_cur_keypart_serialize_size(const ObKeyPart& cur) const
{
  int64_t len = 0;
  bool has_item_next = (cur.item_next_ != NULL);
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    OB_UNIS_ADD_LEN(cur);
    OB_UNIS_ADD_LEN(has_item_next);
    if (has_item_next) {
      len += get_cur_keypart_serialize_size(*cur.item_next_);
    }
  }
  return len;
}

int ObQueryRange::deserialize_cur_keypart(ObKeyPart*& cur, const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  bool has_item_next = false;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    if (OB_ISNULL(cur = create_new_key_part())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create new key part failed", K(ret));
    }
    OB_UNIS_DECODE(*cur);
    OB_UNIS_DECODE(has_item_next);
    if (OB_SUCC(ret) && has_item_next) {
      if (OB_FAIL(SMART_CALL(deserialize_cur_keypart(cur->item_next_, buf, data_len, pos)))) {
        LOG_WARN("deserialize item next failed", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObQueryRange::get_range_graph_serialize_size(const ObKeyPart* cur, const ObKeyPart* pre_and_next) const
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_ISNULL(cur)) {
    LOG_WARN("cur is null.");
  } else {
    bool has_and_next = cur->and_next_ ? true : false;
    bool has_or_next = cur->or_next_ ? true : false;
    bool encode_and_next = false;

    OB_UNIS_ADD_LEN(has_and_next);
    OB_UNIS_ADD_LEN(has_or_next);
    if (has_and_next) {
      encode_and_next = cur->and_next_ == pre_and_next ? false : true;
      OB_UNIS_ADD_LEN(encode_and_next);
      if (encode_and_next) {
        len += get_range_graph_serialize_size(cur->and_next_, NULL);
      }
    }
    len += get_cur_keypart_serialize_size(*cur);
    if (has_or_next) {
      len += get_range_graph_serialize_size(cur->or_next_, cur->and_next_);
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObQueryRange)
{
  int ret = OB_SUCCESS;
  int64_t graph_count = (NULL != table_graph_.key_part_head_ ? 1 : 0);

  OB_UNIS_ENCODE(static_cast<int64_t>(state_));
  OB_UNIS_ENCODE(column_count_);
  OB_UNIS_ENCODE(graph_count);
  if (1 == graph_count) {
    if (OB_FAIL(serialize_range_graph(table_graph_.key_part_head_, NULL, buf, buf_len, pos))) {
      LOG_WARN("serialize range graph failed", K(ret));
    }
    OB_UNIS_ENCODE(table_graph_.is_precise_get_);
    OB_UNIS_ENCODE(table_graph_.is_equal_range_);
    OB_UNIS_ENCODE(table_graph_.is_standard_range_);
  }
  OB_UNIS_ENCODE(contain_row_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObQueryRange)
{
  int64_t len = 0;
  int64_t graph_count = (NULL != table_graph_.key_part_head_ ? 1 : 0);

  OB_UNIS_ADD_LEN(static_cast<int64_t>(state_));
  OB_UNIS_ADD_LEN(column_count_);
  OB_UNIS_ADD_LEN(graph_count);
  if (1 == graph_count) {
    len += get_range_graph_serialize_size(table_graph_.key_part_head_, NULL);
    OB_UNIS_ADD_LEN(table_graph_.is_precise_get_);
    OB_UNIS_ADD_LEN(table_graph_.is_equal_range_);
    OB_UNIS_ADD_LEN(table_graph_.is_standard_range_);
  }
  OB_UNIS_ADD_LEN(contain_row_);
  return len;
}

OB_DEF_DESERIALIZE(ObQueryRange)
{
  int ret = OB_SUCCESS;
  int64_t state = 0;
  int64_t graph_count = 0;

  OB_UNIS_DECODE(state);
  OB_UNIS_DECODE(column_count_);
  OB_UNIS_DECODE(graph_count);
  if (1 == graph_count) {
    if (OB_FAIL(deserialize_range_graph(NULL, table_graph_.key_part_head_, buf, data_len, pos))) {
      LOG_WARN("deserialize range graph failed", K(ret));
    }
    OB_UNIS_DECODE(table_graph_.is_precise_get_);
    OB_UNIS_DECODE(table_graph_.is_equal_range_);
    OB_UNIS_DECODE(table_graph_.is_standard_range_);
  }
  OB_UNIS_DECODE(contain_row_);
  if (OB_SUCC(ret)) {
    state_ = static_cast<ObQueryRangeState>(state);
  }
  return ret;
}

// Deep copy range graph of one index
int ObQueryRange::deep_copy_range_graph(ObKeyPart* src, ObKeyPart*& dest)
{
  int ret = OB_SUCCESS;
  ObKeyPart* prev_gt = NULL;
  for (ObKeyPart* cur_gt = src; OB_SUCC(ret) && NULL != cur_gt; cur_gt = cur_gt->general_or_next()) {
    ObKeyPart* and_next = NULL;
    ObKeyPart* new_key_part = NULL;
    ObKeyPart* prev_key_part = NULL;
    ObKeyPart* new_cur_gt_head = NULL;
    if (OB_FAIL(SMART_CALL(deep_copy_range_graph(cur_gt->and_next_, and_next)))) {
      LOG_WARN("Deep copy range graph failed", K(ret));
    } else {
      for (ObKeyPart* cur_or = cur_gt; OB_SUCC(ret) && NULL != cur_or && cur_or->and_next_ == cur_gt->and_next_;
           cur_or = cur_or->or_next_) {
        if (OB_FAIL(deep_copy_key_part_and_items(cur_or, new_key_part))) {
          LOG_WARN("Deep copy key part and items failed");
        } else if (cur_or == cur_gt) {
          new_cur_gt_head = new_key_part;
        } else {
          if (OB_ISNULL(prev_key_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("prev_key_part is null.", K(ret));
          } else {
            prev_key_part->or_next_ = new_key_part;
          }
        }
        if (OB_SUCC(ret)) {
          prev_key_part = new_key_part;
          if (OB_ISNULL(new_key_part)) {  // yeti2
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new_key_part is null.", K(ret));
          } else {
            new_key_part->and_next_ = and_next;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != prev_gt) {
        prev_gt->or_next_ = new_cur_gt_head;
      } else {
        dest = new_cur_gt_head;
      }
      prev_gt = new_key_part;
    }
  }
  if (OB_FAIL(ret)) {
    dest = NULL;
  }
  return ret;
}

// Deep copy whole query range

int ObQueryRange::deep_copy(const ObQueryRange& other)
{
  int ret = OB_SUCCESS;
  const ObRangeGraph& other_graph = other.table_graph_;
  state_ = other.state_;
  contain_row_ = other.contain_row_;
  column_count_ = other.column_count_;
  if (OB_FAIL(range_exprs_.assign(other.range_exprs_))) {
    LOG_WARN("assign range exprs failed", K(ret));
  } else if (OB_FAIL(param_indexs_.assign(other.param_indexs_))) {
    LOG_WARN("assign param indexs failed", K(ret), K_(other.param_indexs));
  } else if (OB_FAIL(table_graph_.assign(other_graph))) {
    LOG_WARN("Deep copy range columns failed", K(ret));
  } else {
    if (OB_FAIL(deep_copy_range_graph(other_graph.key_part_head_, table_graph_.key_part_head_))) {
      LOG_WARN("Deep copy key part graph failed", K(ret));
    }
  }
  return ret;
}

int ObQueryRange::all_single_value_ranges(bool& all_single_values, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  all_single_values = false;
  ObQueryRangeArray ranges;
  ObGetMethodArray dummy;
  if (OB_FAIL(get_tablet_ranges(ranges, dummy, dtc_params))) {
    LOG_WARN("fail to get tablet ranges", K(ret));
  } else {
    if (ranges.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ranges.", K(ret));
    } else {
      all_single_values = true;
      for (int64_t i = 0; OB_SUCC(ret) && all_single_values && i < ranges.count(); i++) {
        if (OB_ISNULL(ranges.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the memver of ranges is null.", K(ret));
        } else if (!ranges.at(i)->border_flag_.inclusive_start() || !ranges.at(i)->border_flag_.inclusive_end() ||
                   ranges.at(i)->start_key_ != ranges.at(i)->end_key_) {
          all_single_values = false;
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObQueryRange::is_min_to_max_range(bool& is_min_to_max_range, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  is_min_to_max_range = false;
  ObQueryRangeArray ranges;
  ObGetMethodArray dummy;
  if (OB_FAIL(get_tablet_ranges(ranges, dummy, dtc_params))) {
    LOG_WARN("fail to get tablet ranges", K(ret));
  } else if (OB_ISNULL(ranges.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the memver of ranges is null.", K(ret));
  } else {
    if (1 == ranges.count() && ranges.at(0)->start_key_.is_min_row() && ranges.at(0)->end_key_.is_max_row()) {
      is_min_to_max_range = true;
    }
  }
  return ret;
}

DEF_TO_STRING(ObQueryRange)
{
  int64_t pos = 0;

  J_ARRAY_START();
  if (NULL != table_graph_.key_part_head_) {
    J_OBJ_START();
    J_KV(N_IN,
        table_graph_.is_equal_range_,
        N_IS_GET,
        table_graph_.is_precise_get_,
        N_IS_STANDARD,
        table_graph_.is_standard_range_);
    J_COMMA();
    J_NAME(N_RANGE_GRAPH);
    J_COLON();
    pos += range_graph_to_string(buf + pos, buf_len - pos, table_graph_.key_part_head_);
    J_OBJ_END();
  }
  J_ARRAY_END();
  return pos;
}

int64_t ObQueryRange::range_graph_to_string(char* buf, const int64_t buf_len, ObKeyPart* key_part) const
{
  int64_t pos = 0;
  bool is_stack_overflow = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    J_OBJ_START();
    if (NULL != key_part) {
      J_KV(N_KEY_PART_VAL, *key_part);
      if (key_part->item_next_) {
        J_COMMA();
        J_NAME(N_ITEM_KEY_PART);
        J_COLON();
        pos += range_graph_to_string(buf + pos, buf_len - pos, key_part->item_next_);
      }
      if (key_part->and_next_) {
        J_COMMA();
        J_NAME(N_AND_KEY_PART);
        J_COLON();
        pos += range_graph_to_string(buf + pos, buf_len - pos, key_part->and_next_);
      }
      if (key_part->or_next_) {
        J_COMMA();
        J_NAME(N_OR_KEY_PART);
        J_COLON();
        pos += range_graph_to_string(buf + pos, buf_len - pos, key_part->or_next_);
      }
    }
    J_OBJ_END();
  }
  return pos;
}

// UNUSED NOW
int ObQueryRange::extract_query_range(const ColumnIArray& range_columns, ObRawExpr* expr_root,
    const ParamsIArray& params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(preliminary_extract_query_range(range_columns, expr_root, dtc_params))) {
    LOG_WARN("failed to preliminary extract query range", K(ret));
  } else if (OB_FAIL(final_extract_query_range(params, dtc_params))) {
    LOG_WARN("failed to final extract query range", K(ret));
  } else {
  }
  return ret;
}

int ObQueryRange::extract_query_range(const ColumnIArray& range_columns, const ExprIArray& root_exprs,
    const ParamsIArray& params, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(preliminary_extract_query_range(range_columns, root_exprs, dtc_params))) {
    LOG_WARN("failed to preliminary extract query range", K(ret));
  } else if (OB_FAIL(final_extract_query_range(params, dtc_params))) {
    LOG_WARN("failed to final extract query range", K(ret));
  } else {
  }
  return ret;
}

bool ObQueryRange::need_deep_copy() const
{
  return !table_graph_.is_standard_range_;
}

inline bool ObQueryRange::is_standard_graph(const ObKeyPart* root) const
{
  bool bret = true;
  if (contain_row_) {
    bret = false;
  } else {
    for (const ObKeyPart* cur = root; bret && NULL != cur; cur = cur->and_next_) {
      if (NULL != cur->or_next_ || cur->is_like_key()) {
        bret = false;
      } else {
        for (const ObKeyPart* item_next = cur->item_next_; bret && NULL != item_next;
             item_next = item_next->item_next_) {
          if (item_next->is_like_key()) {
            bret = false;
          }
        }
      }
    }
  }
  return bret;
}

// Determine whether it can be exempted or
// Strictly equivalent node graph
// Conditions are met: KEY is continuous, aligned, and equivalent
int ObQueryRange::is_strict_equal_graph(
    const ObKeyPart* node, const int64_t cur_pos, int64_t& max_pos, bool& is_strict_equal) const
{
  is_strict_equal = true;
  bool is_stack_overflow = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (NULL == node || node->pos_.offset_ != cur_pos) {
    is_strict_equal = false;
  } else if (!node->is_equal_condition()) {
    is_strict_equal = false;
  } else {
    // and direction
    if (NULL != node->and_next_) {
      OZ(SMART_CALL(is_strict_equal_graph(node->and_next_, cur_pos + 1, max_pos, is_strict_equal)));
    } else {  // check alignment
      if (-1 == max_pos) {
        max_pos = cur_pos;
      } else if (cur_pos != max_pos) {
        is_strict_equal = false;
      }
    }
    // or direction
    if (is_strict_equal && OB_SUCC(ret) && NULL != node->or_next_) {
      OZ(SMART_CALL(is_strict_equal_graph(node->or_next_, cur_pos, max_pos, is_strict_equal)));
    }
  }
  return ret;
}

// The condition that graph is a strict range is that the extracted range can only contain one in expression
// You can set the start key position start_pos, the default is 0.

// When pos is not 0,
// it is used to judge whether it is a neat graph (or operation is required to judge when hard parsing),
// the primary keys c1, c2, c3
// (c2,c3) in ((1,2),(2,3)) -> is a neat graph, pos starts from 1

// Normal situation such as: the primary key is c1, c2, c3
// (c1, c2, c3) in ((1, 2, 3), (4, 5, 6)) TRUE
// (c1, c3) in ((1, 3), (4, 6)) The primary key is not continuous, FALSE
// (c2,c3) in ((1,1),(2,2)) is not a primary key prefix, FALSE
// (c1, c2, c3) in ((1, 2, 3), (4, (select 5), 6)) FALSE,
// the value of a position in the primary key is a subquery, (min, max) covers the back
bool ObQueryRange::is_strict_in_graph(const ObKeyPart* root, const int64_t start_pos) const
{
  bool bret = true;
  if (NULL == root) {
    bret = false;
  } else if (column_count_ < 1) {
    bret = false;
  }
  int64_t first_len = -1;
  for (const ObKeyPart* cur_or = root; bret && NULL != cur_or; cur_or = cur_or->or_next_) {
    const ObKeyPart* cur_and = cur_or;
    int64_t j = start_pos;
    for (j = start_pos; bret && j < column_count_ && NULL != cur_and; ++j) {
      if (cur_and->pos_.offset_ != j) {
        bret = false;
      } else if (!cur_and->is_equal_condition()) {
        bret = false;
      } else if (start_pos != j && NULL != cur_and->or_next_) {
        // except the first item, others can't has or_next
        bret = false;
      } else {
        cur_and = cur_and->and_next_;
      }
    }
    if (bret) {
      if (first_len < 0) {
        first_len = j;
      } else if (j != first_len || NULL != cur_and) {
        bret = false;
      }
    }
  }
  return bret;
}

bool ObQueryRange::is_regular_in_graph(const ObKeyPart* root) const
{
  bool bret = true;
  if (NULL == root) {
    bret = false;
  } else {
    bret = is_strict_in_graph(root, root->pos_.offset_);
  }
  return bret;
}

int ObQueryRange::ObSearchState::intersect(const ObObj& start, bool include_start, const ObObj& end, bool include_end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start_) || OB_ISNULL(end_) || OB_ISNULL(include_start_) || OB_ISNULL(include_end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(start), K_(end), K_(include_start), K_(include_end), K_(depth));
  } else if (depth_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid depth", K(ret), K_(depth));
    // depth_ is allowed to be less than 0, but depth_ is less than 0,
    // the value of the node may only be constant true or constant false or whole key_part.
    // These 3 conditions have been processed above
    // Should not come here
  } else {
    ObObj& s1 = start_[depth_];
    ObObj& e1 = end_[depth_];
    int cmp = start.compare(end);
    if (cmp > 0 || (0 == cmp && (!include_start || !include_end)) ||
        !has_intersect(start, include_start, end, include_end)) {
      for (int64_t i = 0; i < max_exist_index_; ++i) {
        start_[i].set_max_value();
        end_[i].set_min_value();
      }
      last_include_start_ = false;
      last_include_end_ = false;
      is_empty_range_ = true;
    } else if (start.is_min_value() && end.is_max_value()) {
      // always true, true and A => the result's A, so ignore true
    } else {
      cmp = s1.compare(start);
      if (cmp > 0) {
        // do nothing
      } else if (cmp < 0) {
        s1 = start;
        include_start_[depth_] = include_start;
      } else {
        include_start_[depth_] = (include_start_[depth_] && include_start);
      }

      cmp = e1.compare(end);
      if (cmp > 0) {
        e1 = end;
        include_end_[depth_] = include_end;
      } else if (cmp < 0) {
        // do nothing
      } else {
        include_end_[depth_] = (include_end_[depth_] && include_end);
      }
    }
  }
  return ret;
}

int ObQueryRange::remove_precise_range_expr(int64_t offset)
{
  int ret = OB_SUCCESS;
  if (query_range_ctx_ != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_range_ctx_->precise_range_exprs_.count(); ++i) {
      ObRangeExprItem& expr_item = query_range_ctx_->precise_range_exprs_.at(i);
      for (int64_t j = 0; j < expr_item.cur_pos_.count(); ++j) {
        if (expr_item.cur_pos_.at(j) >= offset) {
          expr_item.cur_expr_ = NULL;
          break;
        }
      }
    }
  }
  return ret;
}

bool ObQueryRange::is_general_graph(const ObKeyPart& keypart) const
{
  bool bret = true;
  const ObKeyPart* cur_key = &keypart;
  const ObKeyPart* cur_and_next = cur_key->and_next_;
  for (const ObKeyPart* or_next = cur_key->or_next_; bret && or_next != NULL; or_next = or_next->or_next_) {
    if (or_next->and_next_ != cur_and_next) {
      bret = false;
    }
  }
  return bret;
}

bool ObQueryRange::has_scan_key(const ObKeyPart& keypart) const
{
  bool bret = false;
  for (const ObKeyPart* or_next = &keypart; !bret && or_next != NULL; or_next = or_next->or_next_) {
    if (!or_next->is_equal_condition()) {
      bret = true;
    }
  }
  return bret;
}

int ObQueryRange::normalize_range_graph(ObKeyPart*& keypart)
{
  int ret = OB_SUCCESS;
  ObKeyPart* full_key = NULL;
  if (keypart != NULL && keypart->pos_.offset_ > 0) {
    if (OB_FAIL(alloc_full_key_part(full_key))) {
      LOG_WARN("alloc full key part failed", K(ret));
    } else if (OB_ISNULL(full_key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keypart is null");
    } else {
      full_key->id_ = keypart->id_;
    }
  }
  return ret;
}

void ObQueryRange::check_like_range_precise(
    const ObString& pattern_str, const char* max_str_buf, const size_t max_str_len, const char escape)
{
  if (NULL != query_range_ctx_) {
    const char* pattern_str_buf = pattern_str.ptr();
    bool end_with_percent = false;
    bool find_first_percent = false;
    int64_t i = 0;
    int64_t j = 0;
    int64_t last_equal_idx = -1;
    while (i < pattern_str.length() && j < max_str_len) {
      // handle escape
      if (pattern_str_buf[i] == escape) {
        if (i == pattern_str.length() - 1) {
          // if escape is last character in pattern, then we will use its origin meaning
          // e.g. c1 like 'aa%' escape '%', % in pattern means match any
        } else {
          ++i;
        }
      }

      if (pattern_str_buf[i] == max_str_buf[j]) {
        last_equal_idx = i;
        ++i;
        ++j;
      } else if (pattern_str_buf[i] == '%') {
        if (find_first_percent) {
          if (pattern_str_buf[i - 1] == '%') {
            end_with_percent = (pattern_str.length() == i + 1);
          } else {
            end_with_percent = false;
            break;
          }
        } else {
          find_first_percent = true;
          end_with_percent = (pattern_str.length() == i + 1);
        }
        ++i;
      } else {
        // The wildcard character'_' has different processing methods for different character sets,
        // so the case of'_' is not dealt with here.
        // If it encounters'_', it is directly regarded as not an exact match
        break;
      }
    }
    bool match_without_wildcard = (i == pattern_str.length() && j == max_str_len);
    // End with'%' or there is no wildcard in the pattern,
    // and the last equal character is not a space (that is,
    // the pattern does not contain trailing spaces)
    if ((end_with_percent || match_without_wildcard) &&
        (-1 == last_equal_idx || pattern_str_buf[last_equal_idx] != ' ')) {
      if (!is_oracle_mode() && match_without_wildcard) {
        // in mysql, all operater will ignore trailing spaces except like.
        // for example, 'abc  ' = 'abc' is true, but 'abc  ' like 'abc' is false
      } else {
        query_range_ctx_->cur_expr_is_precise_ = true;
      }
    }
  }
}

int ObQueryRange::cast_like_obj_if_needed(const ObObj& string_obj, ObObj& buf_obj, const ObObj*& obj_ptr,
    ObKeyPart& out_key_part, const ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  obj_ptr = &string_obj;
  ObExprResType& col_res_type = out_key_part.pos_.column_type_;
  if (!ObSQLUtils::is_same_type_for_compare(string_obj.get_meta(), col_res_type.get_obj_meta())) {
    ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, col_res_type.get_collation_type());
    ObExpectType expect_type;
    expect_type.set_type(col_res_type.get_type());
    expect_type.set_collation_type(col_res_type.get_collation_type());
    expect_type.set_type_infos(&out_key_part.pos_.get_enum_set_values());
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, string_obj, buf_obj, obj_ptr))) {
      LOG_WARN("cast obj to dest type failed", K(ret), K(string_obj), K(col_res_type));
    }
  }
  return ret;
}

DEF_TO_STRING(ObQueryRange::ObRangeExprItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cur_expr), K_(cur_pos));
  J_OBJ_END();
  return pos;
}

}  // namespace sql
}  // namespace oceanbase
