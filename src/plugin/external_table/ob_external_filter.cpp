/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_filter.h"
#include "plugin/external_table/ob_external_arrow_object.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
using namespace sql;

namespace plugin {
namespace external {
int union_record_batch_list(RecordBatchVector &record_batch_list, shared_ptr<RecordBatch> &record_batch_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  SchemaBuilder schema_builder;
  ArrayVector array_vector;
  for (vector<shared_ptr<RecordBatch>>::iterator iter = record_batch_list.begin(), itend = record_batch_list.end();
       OB_SUCC(ret) && iter != itend; ++iter) {
    shared_ptr<RecordBatch> &record_batch = *iter;
    const ArrayVector &this_array_vector = record_batch->columns();
    if (OBARROW_FAIL(schema_builder.AddSchema(record_batch->schema()))) {
      LOG_WARN("add fields failed", K(obstatus));
    } else {
      array_vector.insert(array_vector.end(), this_array_vector.begin(), this_array_vector.end());
    }
  }
  if (OB_SUCC(ret)) {
    shared_ptr<Schema> schema;
    if (OBARROW_FAIL(schema_builder.Finish().Value(&schema))) {
      LOG_WARN("failed to build scheam", K(obstatus));
    } else if (FALSE_IT(record_batch_ret = RecordBatch::Make(schema, 1, array_vector))) {
    } else if (!record_batch_ret) {
      ret = OB_ERROR;
      LOG_WARN("failed to build filter record batch", K(ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObArrowFilterConstants
const char *ObArrowFilterConstants::item_type_name(ObItemType item_type)
{
#define CMP_PREFIX "cmp."
#define CONCAT_PREFIX "concat."

  switch (item_type) {
    case T_OP_EQ:        return CMP_PREFIX "eq";
    case T_OP_LE:        return CMP_PREFIX "le";
    case T_OP_LT:        return CMP_PREFIX "lt";
    case T_OP_GE:        return CMP_PREFIX "ge";
    case T_OP_GT:        return CMP_PREFIX "gt";
    case T_OP_NE:        return CMP_PREFIX "ne";
    case T_OP_IS:        return CMP_PREFIX "is";
    case T_OP_IS_NOT:    return CMP_PREFIX "is_not";
    case T_OP_BTW:       return CMP_PREFIX "between";
    case T_OP_NOT_BTW:   return CMP_PREFIX "not_between";
    case T_OP_LIKE:      return CMP_PREFIX "like";
    case T_OP_NOT_LIKE:  return CMP_PREFIX "not_like";
    case T_OP_IN:        return CMP_PREFIX "in";
    case T_OP_NOT_IN:    return CMP_PREFIX "not_in";
    case T_OP_AND:       return CONCAT_PREFIX "and";
    case T_OP_OR:        return CONCAT_PREFIX "or";
    case T_OP_NOT:       return CONCAT_PREFIX "not";
    default:             return nullptr;
  }
#undef CMP_PREFIX
#undef CONCAT_PREFIX
}
////////////////////////////////////////////////////////////////////////////////
// ObArrowFilterListBuilder

int ObArrowFilterListBuilder::add_filter(shared_ptr<RecordBatch> filter)
{
  int ret = OB_SUCCESS;
  ObArrowExprBuilder expr_builder;
  shared_ptr<RecordBatch> filter_id_expr;
  OB_TRY_BEGIN;
  if (OB_FAIL(expr_builder.build_filter_id_expr(filter_id_expr))) {
    LOG_WARN("failed to build filter id expr", K(ret));
  } else if (FALSE_IT(expr_list_.emplace_back(filter))) {
  } else if (FALSE_IT(expr_list_.emplace_back(filter_id_expr))) {
  }
  OB_TRY_END;
  return ret;
}

int ObArrowFilterListBuilder::build(shared_ptr<RecordBatch> &filter_list_ret)
{
  int ret = OB_SUCCESS;
  OB_TRY_BEGIN;
  ret = union_record_batch_list(expr_list_, filter_list_ret);
  OB_TRY_END;
  if (filter_list_ret) {
    LOG_TRACE("filter list is",
              KCSTRING(filter_list_ret->ToString().c_str()),
              KCSTRING(filter_list_ret->schema()->ToString().c_str()));
  }
  LOG_TRACE("union_record_batch_list done", K(ret));
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObArrowFilterBuilder
void ObArrowFilterBuilder::reuse()
{
  not_supported_ = false;
  expr_list_.clear();
}

int ObArrowFilterBuilder::build_filter(const ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_filter_ret)
{
  int ret = OB_SUCCESS;
  arrow_filter_ret.reset();
  /// this is the highest level
  /// don't use try/catch in sub routines
  shared_ptr<RecordBatch> arrow_filter_expr;
  OB_TRY_BEGIN;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(raw_expr));
  } else if (OB_FAIL(build_filter_expr(raw_expr, arrow_filter_expr))) { // a filter is a filter expr
    LOG_WARN("failed to build an arrow filter expr", K(ret));
  } else if (!arrow_filter_expr || not_supported_) {
    LOG_TRACE("filter is not supported");
  } else if (OB_FAIL(union_record_batch_list(expr_list_, arrow_filter_ret))) {
    LOG_WARN("failed to union filter exprs into a filter", K(ret));
  }
  OB_TRY_END;
  return ret;
}

int ObArrowFilterBuilder::build_filter_expr(const ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(raw_expr));
  } else {
    const ObItemType item_type = raw_expr->get_expr_type();
    switch (item_type) {
      case T_OP_EQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:
      case T_OP_IS:
      case T_OP_IS_NOT:
      case T_OP_BTW:
      case T_OP_NOT_BTW:
      case T_OP_LIKE:
      case T_OP_NOT_LIKE:
      case T_OP_AND:
      case T_OP_OR:
      case T_OP_NOT: {
        vector<int32_t> children_indexes;
        for (int64_t i = 0; OB_SUCC(ret) && !not_supported_ && i < raw_expr->get_param_count(); i++) {
          shared_ptr<RecordBatch> arrow_child_expr;
          if (OB_FAIL(build_expr(raw_expr->get_param_expr(i), arrow_child_expr))) {
            LOG_WARN("failed to build expr", K(ret));
          } else if (!arrow_child_expr) {
            not_supported_ = true;
            LOG_TRACE("not supported: got null child expr");
          } else if (!not_supported_) {
            expr_list_.emplace_back(arrow_child_expr);
            children_indexes.push_back((int32_t)expr_list_.size() - 1);
          }
        }

        if (OB_SUCC(ret) && !not_supported_) {
          ObArrowExprBuilder expr_builder;
          if (OB_FAIL(expr_builder.build_compound_expr(item_type, children_indexes, arrow_expr_ret))) {
            LOG_WARN("failed to set expr type", K(ret));
          } else if (expr_builder.not_supported() || !arrow_expr_ret) {
            not_supported_ = true;
            LOG_TRACE("not supported: build compound expr return null");
          } else {
            expr_list_.emplace_back(arrow_expr_ret);
          }
        }
      } break;
      case T_OP_IN:
      case T_OP_NOT_IN: {
        vector<int32_t> children_indexes;
        shared_ptr<RecordBatch> arrow_child_expr;
        // The first child expected to be a column_ref expr
        if (OB_FAIL(build_expr(raw_expr->get_param_expr(0), arrow_child_expr))) {
          LOG_WARN("failed to build child expr", K(ret));
        } else if (!arrow_child_expr) {
          not_supported_ = true;
          LOG_TRACE("got not supported child expr");
        } else if (!not_supported_) {
          expr_list_.emplace_back(arrow_child_expr);
          children_indexes.push_back((int32_t)expr_list_.size() - 1);
        }
        LOG_TRACE("in expr children count", K(raw_expr->get_param_count()));

        // refer to ObPushdownFilterConstructor::is_white_mode to get the information about IN expr
        const ObRawExpr *param_exprs = raw_expr->get_param_expr(1);
        for (int64_t i = 0; OB_SUCC(ret) && !not_supported_ && i < param_exprs->get_param_count(); i++) {
          if (OB_FAIL(build_expr(param_exprs->get_param_expr(i), arrow_child_expr))) {
            LOG_WARN("failed to build expr", K(ret));
          } else if (!arrow_child_expr) {
            not_supported_ = true;
            LOG_TRACE("not supported: got null child expr");
          } else if (!not_supported_) {
            expr_list_.emplace_back(arrow_child_expr);
            children_indexes.push_back((int32_t)expr_list_.size() - 1);
          }
        }

        if (OB_SUCC(ret) && !not_supported_) {
          ObArrowExprBuilder expr_builder;
          if (OB_FAIL(expr_builder.build_compound_expr(item_type, children_indexes, arrow_expr_ret))) {
            LOG_WARN("failed to set expr type", K(ret));
          } else if (expr_builder.not_supported() || !arrow_expr_ret) {
            not_supported_ = true;
            LOG_TRACE("not supported: build compound expr return null");
          } else {
            expr_list_.emplace_back(arrow_expr_ret);
          }
        }
      } break;
      default: {
        // not supported
        not_supported_ = true;
        LOG_TRACE("cannot pushdown", K(item_type));
      } break;
    }
  }
  return ret;
}

int ObArrowFilterBuilder::build_expr(const ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(raw_expr));
  } else {
    ObArrowExprBuilder expr_builder;
    const ObItemType item_type = raw_expr->get_expr_type();
    if (raw_expr->is_column_ref_expr()) {
      const ObColumnRefRawExpr *col_ref = static_cast<const ObColumnRefRawExpr*>(raw_expr);
      if (OB_FAIL(expr_builder.build_column_ref_expr(*col_ref, arrow_expr_ret))) {
        LOG_WARN("failed to build column ref expr", K(ret));
      }
    } else if (raw_expr->is_const_expr() && raw_expr->get_result_meta().is_null()) {
      if (OB_FAIL(expr_builder.build_null_expr(arrow_expr_ret))) {
        LOG_WARN("failed to build null expr", K(ret));
      }
    } else if (raw_expr->is_immutable_const_expr()) { // this is a const value
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(raw_expr);
      if (OB_FAIL(expr_builder.build_const_value_expr(const_expr->get_value(), arrow_expr_ret))) {
        LOG_WARN("failed to build const value expr", K(ret));
      }
    } else if (T_QUESTIONMARK == item_type && raw_expr->is_const_raw_expr()) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(raw_expr);
      ObObj value;
      bool need_check = false;
      int64_t placeholder_index = -1;
      if (!const_expr->get_value().is_unknown()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid object type, should be unknown", K(const_expr->get_value()));
      } else if (OB_FAIL(const_expr->get_value().get_unknown(placeholder_index))) {
        LOG_WARN("failed to get question mark param index", K(ret));
      } else if (OB_FAIL(ObSQLUtils::calc_const_expr(raw_expr, &param_store_, value, need_check))) {
        LOG_WARN("failed to get param raw expr's value", K(ret));
      } else if (OB_FAIL(expr_builder.build_question_mark_expr(value, placeholder_index, arrow_expr_ret))) {
        LOG_WARN("faield to build const value expr", K(ret), K(value));
      }
    } else {
      if (OB_FAIL(build_filter_expr(raw_expr, arrow_expr_ret))) {
        LOG_WARN("failed to build a filter expr", K(ret));
      }
    }
    not_supported_ = expr_builder.not_supported();
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObArrowExprBuilder

ObArrowExprBuilder::ObArrowExprBuilder()
{}

ObArrowExprBuilder::~ObArrowExprBuilder()
{}

int ObArrowExprBuilder::build_column_ref_expr(const ObColumnRefRawExpr &column_expr,
                                              shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();
  const ObString &column_name = column_expr.get_column_name();
  field_ = arrow::field(ObArrowFilterConstants::COLUMN_REF_FIELD_NAME, arrow::utf8(), false/*nullable*/);
  value_ = make_shared<StringScalar>(StringScalar(string(column_name.ptr(), column_name.length())));

  if (OB_FAIL(build(arrow_expr_ret))) {
    LOG_WARN("failed to build column ref expr", K(ret));
  }

  return ret;
}

int ObArrowExprBuilder::build_const_value_expr(const ObObj &value, shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();
  if (OB_FAIL(get_const_value_scalar(value, value_))) {
    LOG_WARN("failed to build const value", K(ret), K(value));
  } else if (not_supported_) {
  } else {
    field_ = arrow::field(ObArrowFilterConstants::CONST_VALUE_FIELD_NAME, value_->type, false/*nullable*/);
    ret = build(arrow_expr_ret);
  }
  return ret;
}

/**
 * @brief build question mark expression
 * @details A question mark expression is an expression that can be evaluated at running time.
 * In the SQL `SELECT a, b, c FROM t_external WHERE a=100`, the constant `100` will be marked '?'
 * which is a `question mark expression` in the logical/physical plan. The physical plan will be
 * cached in the `plan cache` so we should pushdown the `question mark` expression into the storage,
 * or external plugin data source.
 *
 * A question mark expression contains 2 fields:
 * - data type;
 * - a placeholder index which can help us retrieve value from `ParamStore` which is an array storing
 *   data values.
 *
 * Here we use a StructScalar to describe the question mark expression. The struct contains 2 fields too.
 * One is `value` and the other is `placeholder`.
 * We use `value` instead of `value type` because arrow cannot put a `DataType` into Scalar.
 */
int ObArrowExprBuilder::build_question_mark_expr(const ObObj &value,
                                                 int64_t placeholder_index,
                                                 shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  arrow_expr_ret.reset();
  shared_ptr<Scalar> value_scalar;
  StructScalar::ValueType question_mark_scalar_value;
  shared_ptr<Scalar> placeholder_scalar;
  if (OB_FAIL(get_const_value_scalar(value, value_scalar))) {
    LOG_WARN("failed to build const value", K(ret), K(value));
  } else if (not_supported_) {
  } else if (OBARROW_FAIL(MakeScalar(arrow::int64(), placeholder_index).Value(&placeholder_scalar))) {
    LOG_WARN("failed to create placeholder scalar", K(obstatus));
  } else {
    question_mark_scalar_value.emplace_back(value_scalar);
    question_mark_scalar_value.emplace_back(placeholder_scalar);

    vector<string> question_mark_field_names{
      ObArrowFilterConstants::QUESTION_MARK_VALUE_FIELD_NAME,
      ObArrowFilterConstants::QUESTION_MARK_PLACEHOLDER_FIELD_NAME
    };

    if (OBARROW_FAIL(StructScalar::Make(question_mark_scalar_value, question_mark_field_names).Value(&value_))) {
      LOG_WARN("failed to create question mark scalar", K(obstatus));
    } else {
      field_ = arrow::field(ObArrowFilterConstants::QUESTION_MARK_FIELD_NAME, value_->type, false/*nullable*/);
      ret = build(arrow_expr_ret);
    }
  }
  return ret;
}

int ObArrowExprBuilder::get_const_value_scalar(const ObObj &value, shared_ptr<Scalar> &value_scalar_ret)
{
  int ret = OB_SUCCESS;
  value_scalar_ret.reset();

  if (OB_FAIL(ObArrowObject::get_scalar_value(value, value_scalar_ret))) {
    LOG_WARN("failed to convert ob object to arrow scalar", K(ret), K(value));
  } else if (!value_scalar_ret) {
    not_supported_ = true;
  }
  return ret;
}

int ObArrowExprBuilder::build_null_expr(shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();

  value_ = MakeNullScalar(arrow::int64());
  field_ = arrow::field(ObArrowFilterConstants::NULL_FIELD_NAME, value_->type, true/*nullable*/);

  if (OB_FAIL(build(arrow_expr_ret))) {
    LOG_WARN("failed to build null expr", K(ret));
  }
  return ret;
}

int ObArrowExprBuilder::build_compound_expr(ObItemType item_type,
                                            const vector<int32_t> &children,
                                            shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  Int32Builder children_array_builder;
  shared_ptr<Array> data_array;
  const char *item_type_name = nullptr;
  if (OB_ISNULL(item_type_name = ObArrowFilterConstants::item_type_name(item_type))) {
    not_supported_ = true;
    LOG_TRACE("not supported item type", K(item_type));
  } else if (OBARROW_FAIL(children_array_builder.AppendValues(children))) {
    LOG_WARN("failed to append array values", K(obstatus));
  } else if (OBARROW_FAIL(children_array_builder.Finish(&data_array))) {
    LOG_WARN("failed to build array", K(obstatus));
  } else {
    value_ = make_shared<ListScalar>(data_array);
    field_ = arrow::field(item_type_name, value_->type);
    ret = build(arrow_expr_ret);
  }
  return ret;
}

int ObArrowExprBuilder::build_filter_id_expr(shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  arrow_expr_ret.reset();
  value_ = make_shared<Int64Scalar>(0); // value is no need
  field_ = arrow::field(ObArrowFilterConstants::FILTER_ID_FIELD_NAME, value_->type, false/*nullable*/);
  ret = build(arrow_expr_ret);
  return ret;
}

int ObArrowExprBuilder::build(shared_ptr<RecordBatch> &arrow_expr_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  shared_ptr<Schema> schema;
  SchemaBuilder schema_builder;
  vector<shared_ptr<Array>> value_array_vector;
  value_array_vector.resize(1);
  if (!field_ || !value_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema is null", KP(field_.get()), KP(value_.get()));
  } else if (OBARROW_FAIL(schema_builder.AddField(field_))) {
    LOG_WARN("failed to add field", K(obstatus));
  } else if (OBARROW_FAIL(schema_builder.Finish().Value(&schema))) {
    LOG_WARN("failed to build schema", K(obstatus));
  } else if (OB_FAIL(build_array(*value_, *value_array_vector.data()))) {
    LOG_WARN("failed to build column datas", K(ret));
  } else {
    arrow_expr_ret = RecordBatch::Make(schema, 1, value_array_vector);
  }
  return ret;
}

int ObArrowExprBuilder::build_array(const Scalar &scalar_value, shared_ptr<Array> &array)
{
  int ret = OB_SUCCESS;
  array.reset();
  ObArrowStatus obstatus(ret);
  unique_ptr<ArrayBuilder> array_builder;
  if (OBARROW_FAIL(arrow::MakeBuilder(scalar_value.type).Value(&array_builder))) {
    LOG_WARN("failed to create array builder", K(obstatus));
  } else if (OBARROW_FAIL(array_builder->AppendScalar(scalar_value))) {
    LOG_WARN("failed to append scalar", K(obstatus));
  } else if (OBARROW_FAIL(array_builder->Finish().Value(&array))) {
    LOG_WARN("failed to build scalar", K(obstatus));
  }
  return ret;
}

} // namespace external
} // namespace plugin
} // namespace oceanbase
