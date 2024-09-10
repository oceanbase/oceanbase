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
 * This file contains implementation for ob_array_expr_utils.
 */

#ifndef OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_
#define OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr.h" // for ObExpr
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{

struct ObVectorCastInfo
{
  ObVectorCastInfo()
    : is_vector_(false),
      need_cast_(false),
      subschema_id_(UINT16_MAX),
      dim_cnt_(0)
  {}
  bool is_vector_;
  bool need_cast_;
  uint16_t subschema_id_;
  uint16_t dim_cnt_;
};

class ObArrayExprUtils
{
public:
  ObArrayExprUtils();
  virtual ~ObArrayExprUtils() = default;
  static int set_array_res(ObIArrayType *arr_obj, const int32_t data_len, const ObExpr &expr, ObEvalCtx &ctx, common::ObString &res,
                           const char *data = nullptr);
  static int set_array_res(ObIArrayType *arr_obj, const int32_t data_len, ObIAllocator &allocator, common::ObString &res,
                           const char *data = nullptr);
  static int set_array_obj_res(ObIArrayType *arr_obj, ObObjCastParams *params, ObObj *obj);
  template <typename ResVec>
  static int set_array_res(ObIArrayType *arr_obj, const ObExpr &expr, ObEvalCtx &ctx,
                           ResVec *res_vec, int64_t batch_idx);
  static int deduce_array_element_type(ObExecContext *exec_ctx, ObExprResType* types_stack, int64_t param_num, ObDataType &elem_type);
  static int deduce_nested_array_subschema_id(ObExecContext *exec_ctx,  ObDataType &elem_type, uint16_t &subschema_id);
  static int check_array_type_compatibility(ObExecContext *exec_ctx, uint16_t l_subid, uint16_t r_subid, bool &is_compatiable);
  static int get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObObjType &obj_type, uint32_t &depth, bool &is_vec);
  static int get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObDataType &elem_type, uint32_t &depth, bool &is_vec);
  static int dispatch_array_attrs(ObEvalCtx &ctx, ObExpr &expr, ObString &array_data, const int64_t row_idx);
  static int dispatch_array_attrs(ObEvalCtx &ctx, ObIArrayType *arr_obj, ObExpr **attrs, uint32_t attr_count, const int64_t row_idx);
  static int batch_dispatch_array_attrs(ObEvalCtx &ctx, ObExpr &expr, int64_t begin, int64_t batch_size, const uint16_t *selector = NULL);
  static int transform_array_to_uniform(ObEvalCtx &ctx, const ObExpr &expr, const int64_t batch_size, const ObBitVector *skip);
  static int construct_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, ObIArrayType *&res, bool read_only = true);
  static int calc_nested_expr_data_size(const ObExpr &expr, ObEvalCtx &ctx, const int64_t batch_idx, int64_t &size);
  static int get_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, const ObString &raw_data, ObIArrayType *&res);
  static int dispatch_array_attrs_rows(ObEvalCtx &ctx, ObIArrayType *arr_obj, const int64_t row_idx,
                                       ObExpr **attrs, uint32_t attr_count, bool is_shallow = true);
  static int nested_expr_from_rows(const ObExpr &expr, ObEvalCtx &ctx, const sql::RowMeta &row_meta, const sql::ObCompactRow **stored_rows,
                                   const int64_t size, const int64_t col_idx, const int64_t *selector = NULL);
  static int nested_expr_to_rows(const ObExpr &expr, ObEvalCtx &ctx, const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                                 const uint16_t selector[], const int64_t size, const int64_t col_idx);
  static int nested_expr_to_row(const ObExpr &expr, ObEvalCtx &ctx, char *row_buf,
                                const int64_t col_offset, const uint64_t row_idx, int64_t &cell_len, const int64_t *remain_size = nullptr);
  static int assemble_array_attrs(ObEvalCtx &ctx, const ObExpr &expr, int64_t row_idx, ObIArrayType *arr_obj);
  static void set_expr_attrs_null(const ObExpr &expr, ObEvalCtx &ctx, const int64_t idx);

  // for vector
  static int get_type_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObIAllocator &allocator,
                             ObIArrayType *&result,
                             bool &is_null);
  static int get_type_vector(const ObExpr &expr,
                             const ObDatum &datum,
                             ObEvalCtx &ctx,
                             ObIAllocator &allocator,
                             ObIArrayType *&result);
  static int calc_cast_type(ObExprResType &type, common::ObExprTypeCtx &type_ctx, const bool only_vector = false);
  static int calc_cast_type2(ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx, uint16_t &res_subschema_id,
                             const bool only_vector = false);
  static int collect_vector_cast_info(ObExprResType &type, ObExecContext &exec_ctx, ObVectorCastInfo &info);

  // update inplace
  static int vector_datum_add(ObDatum &res, const ObDatum &data, ObIAllocator &allocator, bool negative = false);
private:
  static const char* DEFAULT_CAST_TYPE_NAME;
  static const ObString DEFAULT_CAST_TYPE_STR;
};

struct ObVectorArithFunc
{
  enum ArithType
  {
    ADD = 0,
    MINUS,
    MUL,
    DIV,
  };
};

struct ObVectorVectorArithFunc : public ObVectorArithFunc
{

  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const;
};

struct ObVectorFloatArithFunc : public ObVectorArithFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const;
};

class ObNestedVectorFunc
{
public:
  static int construct_attr_param(ObIAllocator &alloc, ObEvalCtx &ctx, ObExpr &param_expr,
                                const uint16_t meta_id, int64_t row_idx, ObIArrayType *&param_obj);
  static int construct_param(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id,
                              ObString &str_data, ObIArrayType *&param_obj);
  static int construct_res_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id, ObIArrayType *&res_obj);

  static int construct_params(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t left_meta_id,
                                const uint16_t right_meta_id, const uint16_t res_meta_id, ObString &left, ObString right,
                                ObIArrayType *&left_obj, ObIArrayType *&right_obj, ObIArrayType *&res_obj);
};


} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_