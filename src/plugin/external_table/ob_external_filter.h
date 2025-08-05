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

#pragma once

#include "plugin/external_table/ob_external_arrow_status.h"
#include "objit/include/objit/common/ob_item_type.h"
#include "common/object/ob_object.h"

/**
 * @file ob_external_filter.h
 * @brief Transform oceanbase filter (ObRawExpr) to arrow RecordBatch.
 * @details Transform oceanbase filter to arrow::RecordBatch and then transfer it to the Java
 * plugin. Java plugin internal util decode RecordBatch into Java FilterExpr, so Java plugin
 * can use filter expressions.
 * The RecordBatch of filters is a data like a struct which has some fields (can be duplicated)
 * and each field has only one value (arrow::array with 1 value). A filter is such an expression
 * that nested with children expressions. We flatten filter to struct to store it in RecordBatch.
 * Filters are some filters can be compound with `AND` and transfered in an array. We use a
 * special expression/field `fileter id` to seperate them.
 * @see ObJavaExternalDataEngine::pushdown_filters to see how `pushdown_filters` works.
 */

namespace oceanbase {

namespace sql {
class ObRawExpr;
class ObColumnRefRawExpr;
}

namespace plugin {
namespace external {

struct ObArrowFilterConstants final
{
  static const char *item_type_name(ObItemType item_type);

  static constexpr const char *COLUMN_REF_FIELD_NAME                = "column_ref";
  static constexpr const char *CONST_VALUE_FIELD_NAME               = "const_value";
  static constexpr const char *NULL_FIELD_NAME                      = "null";
  static constexpr const char *QUESTION_MARK_FIELD_NAME             = "question_mark";
  static constexpr const char *QUESTION_MARK_VALUE_FIELD_NAME       = "value";
  static constexpr const char *QUESTION_MARK_PLACEHOLDER_FIELD_NAME = "placeholder";
  static constexpr const char *FILTER_ID_FIELD_NAME                 = "filter";
};

class ObArrowFilterListBuilder final
{
public:
  ObArrowFilterListBuilder() = default;
  ~ObArrowFilterListBuilder() = default;

  int add_filter(shared_ptr<RecordBatch> filter);
  int build(shared_ptr<RecordBatch> &filter_list_ret);

private:
  vector<shared_ptr<RecordBatch>> expr_list_;
};

class ObArrowFilterBuilder final
{
public:
  ObArrowFilterBuilder(const common::ParamStore &param_store)
      : param_store_(param_store)
  {}
  ~ObArrowFilterBuilder() = default;

  void reuse();

  int build_filter(const sql::ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_filter_ret);

private:
  int build_expr(const sql::ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_expr_ret);

  /// filter is a special expression
  int build_filter_expr(const sql::ObRawExpr *raw_expr, shared_ptr<RecordBatch> &arrow_expr_ret);
  /// not supported yet
  /// int build_compound_expr(const ObRawExpr *raw_expr, shared_ptr<StructScalar> &arrow_expr_ret);

  int build();
private:
  const common::ParamStore &param_store_;

  bool              not_supported_ = false;
  RecordBatchVector expr_list_;
};

class ObArrowExprBuilder final
{
public:
  ObArrowExprBuilder();
  ~ObArrowExprBuilder();

  int build_column_ref_expr(const sql::ObColumnRefRawExpr &column_ref_expr, shared_ptr<RecordBatch> &arrow_expr);
  int build_null_expr(shared_ptr<RecordBatch> &arrow_expr);
  int build_const_value_expr(const ObObj &value, shared_ptr<RecordBatch> &arrow_expr);
  int build_question_mark_expr(const ObObj &value, int64_t placeholder, shared_ptr<RecordBatch> &arrow_expr);
  int build_compound_expr(ObItemType item_type, const vector<int32_t> &children, shared_ptr<RecordBatch> &arrow_expr);
  int build_filter_id_expr(shared_ptr<RecordBatch> &arrow_expr);

  bool not_supported() const { return not_supported_; }

private:
  int get_const_value_scalar(const ObObj &value, shared_ptr<Scalar> &scalar_value);
  int build(shared_ptr<RecordBatch> &arrow_expr);

private:
  int build_array(const Scalar &scalar_value, shared_ptr<Array> &array);

private:
  shared_ptr<Scalar> value_; // we can use arrow::Array instead of Scalar
  shared_ptr<Field>  field_;
  bool  not_supported_ = false;
};

} // namespace external
} // namespace plugin
} // namespace oceanbase
