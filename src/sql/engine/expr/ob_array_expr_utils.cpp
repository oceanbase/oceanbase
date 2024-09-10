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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

const char* ObArrayExprUtils::DEFAULT_CAST_TYPE_NAME = "ARRAY(FLOAT)";
const ObString ObArrayExprUtils::DEFAULT_CAST_TYPE_STR = ObString::make_string(DEFAULT_CAST_TYPE_NAME);

int ObArrayExprUtils::get_type_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObIAllocator &allocator,
    ObIArrayType *&result,
    bool &is_null)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  if (OB_FAIL(expr.eval(ctx, datum))) {
    LOG_WARN("eval failed", K(ret));
  } else if (OB_UNLIKELY(datum->is_null())) {
    is_null = true;
  } else if (OB_FAIL(get_type_vector(expr, *datum, ctx, allocator, result))) {
    LOG_WARN("failed to get vector", K(ret));
  }
  return ret;
}

// get vector or array(float)
int ObArrayExprUtils::get_type_vector(
    const ObExpr &expr,
    const ObDatum &datum,
    ObEvalCtx &ctx,
    ObIAllocator &allocator,
    ObIArrayType *&result)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  if (!expr.obj_meta_.is_collection_sql_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret), K(expr.obj_meta_));
  } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid subschema type", K(ret), K(value));
  } else {
    ObString blob_data = datum.get_string();
    const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                          ObLongTextType,
                                                          CS_TYPE_BINARY,
                                                          true,
                                                          blob_data))) {
      LOG_WARN("fail to get real data.", K(ret), K(blob_data));
    } else if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *arr_type, result, true))) {
      LOG_WARN("construct array obj failed", K(ret), K(*coll_info));
    } else if (OB_FAIL(result->init(blob_data))) {
      LOG_WARN("failed to init array", K(ret));
    }
  }
  return ret;
}

int ObArrayExprUtils::vector_datum_add(ObDatum &res, const ObDatum &data, ObIAllocator &allocator, bool negative)
{
  int ret = OB_SUCCESS;
  ObString blob_res = res.get_string();
  ObString blob_data = data.get_string();
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                        ObLongTextType,
                                                        CS_TYPE_BINARY,
                                                        true,
                                                        blob_data))) {
    LOG_WARN("fail to get real data.", K(ret), K(blob_data));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                        ObLongTextType,
                                                        CS_TYPE_BINARY,
                                                        true,
                                                        blob_res))) {
    LOG_WARN("fail to get real data.", K(ret), K(blob_data));
  } else {
    int64_t length = blob_data.length() / sizeof(float);
    float *float_data = reinterpret_cast<float *>(blob_data.ptr());
    float *float_res = reinterpret_cast<float *>(blob_res.ptr());
    for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
      negative ? float_res[i] -= float_data[i] : float_res[i] += float_data[i];
      if (isinff(float_res[i]) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        SQL_LOG(WARN, "value overflow", K(ret), K(i), K(float_data[i]), K(float_res[i]));
      }
    }
  }
  return ret;
}

// cast any array and varchar to array(float)
int ObArrayExprUtils::calc_cast_type(
    ObExprResType &type,
    common::ObExprTypeCtx &type_ctx,
    const bool only_vector)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t dst_subschema_id = 0;
  bool need_cast = false;
  if (!type.is_collection_sql_type() && !type.is_string_type() && !type.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (type.is_collection_sql_type()) {
    ObSubSchemaValue value;
    uint16_t src_subschema_id = type.get_subschema_id();
    if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(src_subschema_id, value))) {
      LOG_WARN("failed to get subschema ctx", K(ret));
    } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid subschema type", K(ret), K(value));
    } else {
      const ObSqlCollectionInfo *coll_info = NULL;
      coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
      if (coll_info->collection_meta_->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
        ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
        if (only_vector) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("only support vector type", K(ret));
        } else if (arr_type->element_type_->type_id_ != ObNestedType::OB_BASIC_TYPE) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested array is not support", K(ret));
        } else {
          ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
          if (ObFloatType != elem_type->basic_meta_.get_obj_type()) {
            need_cast = true;
          }
        }
      }
      // vector and array(float) don't need to cast
      if (OB_SUCC(ret) && !need_cast) {
        type.set_calc_type(ObCollectionSQLType);
        type.set_calc_subschema_id(src_subschema_id); // avoid cast by set the same subschema_id
      }
    }
  } else if (type.is_string_type()) {
    need_cast = true;
  }
  if (OB_FAIL(ret)) {
  } else if (need_cast) {
    if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(DEFAULT_CAST_TYPE_STR, dst_subschema_id))) {
      LOG_WARN("failed to get subschema id by type string", K(ret), K(DEFAULT_CAST_TYPE_STR));
    } else {
      type.set_calc_type(ObCollectionSQLType);
      type.set_calc_subschema_id(dst_subschema_id);
    }
  }

  return ret;
}

int ObArrayExprUtils::collect_vector_cast_info(ObExprResType &type, ObExecContext &exec_ctx, ObVectorCastInfo &info)
{
  int ret = OB_SUCCESS;
  if (type.is_collection_sql_type()) {
    ObSubSchemaValue value;
    info.subschema_id_ = type.get_subschema_id();
    if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(info.subschema_id_, value))) {
      LOG_WARN("failed to get subschema ctx", K(ret));
    } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid subschema type", K(ret), K(value));
    } else {
      const ObSqlCollectionInfo *coll_info = NULL;
      coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
      if (coll_info->collection_meta_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
        ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
        info.is_vector_ = true;
        info.dim_cnt_ = arr_type->dim_cnt_;
      } else if (coll_info->collection_meta_->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
        ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
        ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
        if (ObFloatType != elem_type->basic_meta_.get_obj_type()) {
          info.need_cast_ = true;
        }
      }
    }
  } else if (type.is_string_type()) {
    info.need_cast_ = true;
  } else if (!type.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  }
  return ret;
}

int ObArrayExprUtils::calc_cast_type2(
    ObExprResType &type1,
    ObExprResType &type2,
    common::ObExprTypeCtx &type_ctx,
    uint16_t &res_subschema_id,
    const bool only_vector)
{
  int ret = OB_SUCCESS;
  res_subschema_id = UINT16_MAX;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObString default_dst_type("ARRAY(FLOAT)");
  uint16_t default_dst_subschema_id = UINT16_MAX;

  ObVectorCastInfo info1;
  ObVectorCastInfo info2;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (OB_FAIL(collect_vector_cast_info(type1, *exec_ctx, info1))) {
    LOG_WARN("failed to collect vector cast info", K(ret));
  } else if (OB_FAIL(collect_vector_cast_info(type2, *exec_ctx, info2))) {
    LOG_WARN("failed to collect vector cast info", K(ret));
  } else if (info1.is_vector_ && info2.is_vector_) {
    if (info1.dim_cnt_ != info2.dim_cnt_) {
      ret = OB_ERR_INVALID_VECTOR_DIM;
      LOG_WARN("check array validty failed", K(ret), K(info1.dim_cnt_), K(info2.dim_cnt_));
    }
  } else if (info1.is_vector_) {
    if (!type2.is_null()) {
      type2.set_calc_type(ObCollectionSQLType);
      type2.set_calc_subschema_id(info1.subschema_id_);
      info2.need_cast_ = true;
    }
    res_subschema_id = info1.subschema_id_;
  } else if (info2.is_vector_) {
    if (!type1.is_null()) {
      type1.set_calc_type(ObCollectionSQLType);
      type1.set_calc_subschema_id(info2.subschema_id_);
      info1.need_cast_ = true;
    }
    res_subschema_id = info2.subschema_id_;
  } else if (only_vector) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("no vector in the expr", K(ret));
  } else if (info1.need_cast_ || info2.need_cast_) {
    if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(default_dst_type, default_dst_subschema_id))) {
      LOG_WARN("failed to get subschema id by type string", K(ret), K(default_dst_type));
    } else {
      if (info1.need_cast_) {
        type1.set_calc_type(ObCollectionSQLType);
        type1.set_calc_subschema_id(default_dst_subschema_id);
      }
      if (info2.need_cast_) {
        type2.set_calc_type(ObCollectionSQLType);
        type2.set_calc_subschema_id(default_dst_subschema_id);
      }
      res_subschema_id = default_dst_subschema_id;
    }
  }
  if (OB_SUCC(ret)) {
    if (type1.is_collection_sql_type() && !info1.need_cast_) {
      type1.set_calc_type(ObCollectionSQLType);
      type1.set_calc_subschema_id(type1.get_subschema_id()); // avoid cast by set the same subschema_id
      res_subschema_id = type1.get_subschema_id();
    }
    if (type2.is_collection_sql_type() && !info2.need_cast_) {
      type2.set_calc_type(ObCollectionSQLType);
      type2.set_calc_subschema_id(type2.get_subschema_id()); // avoid cast by set the same subschema_id
      res_subschema_id = type2.get_subschema_id();
    }
  }
  return ret;
}

int ObArrayExprUtils::set_array_res(ObIArrayType *arr_obj, const int32_t res_size, const ObExpr &expr, ObEvalCtx &ctx, ObString &res, const char *data)
{
  int ret = OB_SUCCESS;
  char *res_buf = nullptr;
  int64_t res_buf_len = 0;
  ObDatum tmp_res;
  ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &tmp_res);
  if (OB_FAIL(str_result.init(res_size, nullptr))) {
    LOG_WARN("fail to init result", K(ret), K(res_size));
  } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
    LOG_WARN("fail to get reserver buffer", K(ret));
  } else if (res_buf_len < res_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
  } else if (nullptr != data) {
    MEMCPY(res_buf, data, res_size);
  } else if (nullptr != arr_obj && OB_FAIL(arr_obj->get_raw_binary(res_buf, res_buf_len))) {
    LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
  }
  if (FAILEDx(str_result.lseek(res_size, 0))) {
    LOG_WARN("failed to lseek res.", K(ret), K(str_result), K(res_size));
  } else {
    str_result.get_result_buffer(res);
  }
  return ret;
}

int ObArrayExprUtils::set_array_res(ObIArrayType *arr_obj, const int32_t res_size, ObIAllocator &allocator, ObString &res, const char *data)
{
  int ret = OB_SUCCESS;
  bool has_lob_header = !IS_CLUSTER_VERSION_BEFORE_4_1_0_0;
  char *res_buf = nullptr;
  int64_t res_buf_len = 0;
  ObDatum tmp_res;
  ObTextStringDatumResult str_result(ObCollectionSQLType, has_lob_header, &tmp_res);
  if (OB_FAIL(str_result.init(res_size, &allocator))) {
    LOG_WARN("fail to init result", K(ret), K(res_size));
  } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
    LOG_WARN("fail to get reserver buffer", K(ret));
  } else if (res_buf_len < res_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
  } else if (nullptr != data) {
    MEMCPY(res_buf, data, res_size);
  } else if (nullptr != arr_obj && OB_FAIL(arr_obj->get_raw_binary(res_buf, res_buf_len))) {
    LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
  }
  if (FAILEDx(str_result.lseek(res_size, 0))) {
    LOG_WARN("failed to lseek res.", K(ret), K(str_result), K(res_size));
  } else {
    str_result.get_result_buffer(res);
  }
  return ret;
}

template <typename ResVec>
int ObArrayExprUtils::set_array_res(ObIArrayType *arr_obj, const ObExpr &expr, ObEvalCtx &ctx,
                                    ResVec *res_vec, int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  int32_t res_size = arr_obj->get_raw_binary_len();
  char *res_buf = nullptr;
  int64_t res_buf_len = 0;
  ObTextStringVectorResult<ResVec> str_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, batch_idx);
  if (OB_FAIL(str_result.init_with_batch_idx(res_size, batch_idx))) {
    LOG_WARN("fail to init result", K(ret), K(res_size));
  } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
    LOG_WARN("fail to get reserver buffer", K(ret));
  } else if (res_buf_len < res_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
  } else if (OB_FAIL(arr_obj->get_raw_binary(res_buf, res_buf_len))) {
    LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
  } else if (OB_FAIL(str_result.lseek(res_size, 0))) {
    LOG_WARN("failed to lseek res.", K(ret), K(str_result), K(res_size));
  } else {
    str_result.set_result();
  }
  return ret;
}

template int ObArrayExprUtils::set_array_res<ObUniformFormat<true>>(
    ObIArrayType* arr_obj, const ObExpr& expr, ObEvalCtx& ctx,
    ObUniformFormat<true>* res_vec, int64_t batch_idx);

int ObArrayExprUtils::set_array_obj_res(ObIArrayType *arr_obj, ObObjCastParams *params, ObObj *obj)
{
  int ret = OB_SUCCESS;
  bool has_lob_header = !IS_CLUSTER_VERSION_BEFORE_4_1_0_0;
  int32_t res_size = arr_obj->get_raw_binary_len();
  char *res_buf = nullptr;
  int64_t res_buf_len = 0;
  sql::ObTextStringObObjResult text_result(ObCollectionSQLType, params, obj, has_lob_header);
  if (OB_FAIL(text_result.init(res_size, params->allocator_v2_))) {
    LOG_WARN("init lob result failed");
  } else if (OB_FAIL(text_result.get_reserved_buffer(res_buf, res_buf_len))) {
    LOG_WARN("fail to get reserver buffer", K(ret));
  } else if (res_buf_len < res_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
  } else if (OB_FAIL(arr_obj->get_raw_binary(res_buf, res_buf_len))) {
    LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
  } else if (OB_FAIL(text_result.lseek(res_size, 0))) {
    LOG_WARN("failed to lseek res.", K(ret), K(text_result), K(res_size));
  } else {
    text_result.set_result();
  }
  return ret;
}

int ObArrayExprUtils::check_array_type_compatibility(ObExecContext *exec_ctx, uint16_t l_subid, uint16_t r_subid, bool &is_compatiable)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue l_meta;
  ObSubSchemaValue r_meta;
  if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(l_subid, l_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(l_subid));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(r_subid, r_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(l_subid));
  } else if (l_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE
             || r_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(l_meta.type_), K(r_meta.type_));
  } else if (OB_ISNULL(l_meta.value_) || OB_ISNULL(r_meta.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type info is null", K(ret), K(l_meta.value_), K(r_meta.value_));
  } else {
    is_compatiable =
      reinterpret_cast<const ObSqlCollectionInfo *>(l_meta.value_)->has_same_super_type(*reinterpret_cast<const ObSqlCollectionInfo *>(r_meta.value_));
  }
  return ret;
}

int ObArrayExprUtils::get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObDataType &elem_type,
                                             uint32_t &depth, bool &is_vec)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue meta;
  if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subid, meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(subid));
  } else if (meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(meta.type_));
  } else if (OB_ISNULL(meta.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type info is null", K(ret));
  } else {
    const ObSqlCollectionInfo * coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(meta.value_);
    elem_type = coll_info->get_basic_meta(depth);
    is_vec = coll_info->collection_meta_->type_id_ == ObNestedType::OB_VECTOR_TYPE;
  }
  return ret;
}

int ObArrayExprUtils::get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObObjType &obj_type,
                                             uint32_t &depth, bool &is_vec)
{
  int ret = OB_SUCCESS;
  ObDataType elem_type;
  if (OB_FAIL(get_array_element_type(exec_ctx, subid, elem_type, depth, is_vec))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(subid));
  } else {
    obj_type = elem_type.get_obj_type();
  }
  return ret;
}

int ObArrayExprUtils::deduce_array_element_type(ObExecContext *exec_ctx, ObExprResType* types_stack, int64_t param_num, ObDataType &elem_type)
{
  int ret = OB_SUCCESS;
  uint16_t last_subschema_id = ObInvalidSqlType;
  ObExprResType coll_calc_type;
  ObLength str_len = 0;
  bool is_decimal_exist = false;
  bool is_bigint_signed_exsit = false;
  bool is_all_num_tc = true;
  bool is_all_same_type = true;
  ObObjType last_type = ObNullType;
  int64_t elem_idx = 0;
  ObDecimalIntWideType dec_type = DECIMAL_INT_32;
  for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    if (!types_stack[i].is_null()) {
      elem_idx = i;
    }
    if (types_stack[i].get_type() == ObDecimalIntType) {
      is_decimal_exist = true;
      dec_type = MAX(dec_type, get_decimalint_type(types_stack[i].get_precision()));
    }
    if (!ob_is_numeric_tc(types_stack[i].get_type_class()) && !types_stack[i].is_null()) {
      is_all_num_tc = false;
    }
    if (types_stack[i].get_type() == ObIntType) {
      is_bigint_signed_exsit = true;
    }
    if (ob_is_collection_sql_type(types_stack[i].get_type())) {
      // check subschmea id
      if (last_subschema_id == ObInvalidSqlType) {
        coll_calc_type = types_stack[i];
        last_subschema_id = types_stack[i].get_subschema_id();
        elem_type.meta_.set_collection(last_subschema_id);
      } else if (last_subschema_id != types_stack[i].get_subschema_id()) {
        ObExprResType tmp_calc_type;
        if (OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, coll_calc_type, types_stack[i], tmp_calc_type))) {
          LOG_WARN("failed to check array compatibilty", K(ret));
        } else {
          last_subschema_id = tmp_calc_type.get_subschema_id();
          coll_calc_type = tmp_calc_type;
          elem_type.meta_.set_collection(last_subschema_id);
        }
      }
    } else if (i == 0) {
      // do nothing
    } else if (types_stack[i - 1].get_type() != types_stack[i].get_type()
               && !types_stack[i].is_null() && !types_stack[i - 1].is_null()) { // null is legal input type
      is_all_same_type = false;
      if (!is_all_num_tc) {
        ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
        LOG_USER_ERROR(OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION);
      }
    }
    if (OB_SUCC(ret)) {
      if (ob_is_string_tc(types_stack[i].get_type())) {
        str_len = MAX(str_len, types_stack[i].get_length());
      }
    }
  }
  if (OB_SUCC(ret) && last_subschema_id == ObInvalidSqlType) {
    if (is_all_same_type && !is_decimal_exist) {
      if (param_num == 0) {
        // default type
        elem_type.meta_.set_tinyint();
      } else {
        elem_type.set_meta_type(types_stack[elem_idx].get_obj_meta());
        elem_type.set_accuracy(types_stack[elem_idx].get_accuracy());
        if (ob_is_string_tc(types_stack[elem_idx].get_type())) {
          elem_type.set_length(str_len);
          for (int64_t i = 0; i < param_num; i++) {
            if (!types_stack[i].is_null()) {
              if (types_stack[i].get_length() != str_len) {
                types_stack[i].set_calc_length(str_len);
              }
            }
          }
        }
      }
    } else if (is_decimal_exist) {
      elem_type.meta_.set_double();
      for (int64_t i = 0; i < param_num; i++) {
        if (!types_stack[i].is_null()) {
          if (types_stack[i].get_type() != ObDoubleType) {
            types_stack[i].set_calc_type(ObDoubleType);
          }
        }
      }
    } else if (is_all_num_tc) {
      ObObjType calc_type = ObUInt64Type;
      if (is_bigint_signed_exsit) {
        elem_type.meta_.set_int();
        calc_type = ObIntType;
      } else {
        elem_type.meta_.set_uint64();
      }
      for (int64_t i = 0; i < param_num; i++) {
        if (!types_stack[i].is_null()) {
          if (types_stack[i].get_type() != calc_type) {
            types_stack[i].set_calc_type(calc_type);
          }
        }
      }
    }
  }

  return ret;
}

int ObArrayExprUtils::deduce_nested_array_subschema_id(ObExecContext *exec_ctx,  ObDataType &elem_type, uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  uint16_t elem_subid = elem_type.meta_.get_subschema_id();
  ObSubSchemaValue elem_meta;
  if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(elem_subid, elem_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(elem_subid));
  } else if (elem_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(elem_meta.type_));
  } else {
    const int MAX_LEN = 256;
    int64_t pos = 0;
    char tmp[MAX_LEN] = {0};
    ObString type_info;
    const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(elem_meta.value_);
    if (OB_FAIL(databuff_printf(tmp, MAX_LEN, pos, "ARRAY("))) {
      LOG_WARN("failed to convert len to string", K(ret));
    } else if (FALSE_IT(STRNCPY(tmp + pos, coll_info->name_def_, coll_info->name_len_))) {
    } else if (FALSE_IT(pos += coll_info->name_len_)) {
    } else if (OB_FAIL(databuff_printf(tmp, MAX_LEN, pos, ")"))) {
      LOG_WARN("failed to add ) to string", K(ret));
    } else if (FALSE_IT(type_info.assign_ptr(tmp, static_cast<int32_t>(pos)))) {
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(type_info, subschema_id))) {
      LOG_WARN("failed get subschema id", K(ret), K(type_info));
    }
  }
  return ret;
}

int ObVectorVectorArithFunc::operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const
{
  int ret = OB_SUCCESS;
  const ObExpr &left_expr = *expr.args_[0];
  const ObExpr &right_expr = *expr.args_[1];
  ObIArrayType *arr_l = NULL;
  ObIArrayType *arr_r = NULL;
  ObIArrayType *arr_res = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionArrayType *arr_type = NULL;
  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(left_expr, l, ctx, tmp_allocator, arr_l))) {
    LOG_WARN("failed to get vector", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(right_expr, r, ctx, tmp_allocator, arr_r))) {
    LOG_WARN("failed to get vector", K(ret));
  } else if (OB_ISNULL(arr_l) || OB_ISNULL(arr_r)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr_l), K(arr_r));
  } else if (OB_UNLIKELY(arr_l->size() != arr_r->size())) {
    ret = OB_ERR_INVALID_VECTOR_DIM;
    LOG_WARN("check array validty failed", K(ret), K(arr_l->size()), K(arr_r->size()));
  } else if (arr_l->contain_null() || arr_r->contain_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("array with null can't add", K(ret));
  } else if (FALSE_IT(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
  } else if (OB_ISNULL(coll_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("collect info is null", K(ret), K(subschema_id));
  } else if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_NULL_VALUE;
     LOG_WARN("array type is null", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *arr_type, arr_res))) {
    LOG_WARN("construct array obj failed", K(ret), K(subschema_id), K(coll_info));
  } else {
    const float *data_l = reinterpret_cast<const float*>(arr_l->get_data());
    const float *data_r = reinterpret_cast<const float*>(arr_r->get_data());
    const uint32_t size = arr_l->size();
    ObArrayFixedSize<float> *float_array = static_cast<ObArrayFixedSize<float> *>(arr_res);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      const float float_res = type == ADD ? data_l[i] + data_r[i] :
                              type == MUL ? data_l[i] * data_r[i] :
                              data_l[i] - data_r[i];
      if (isinff(float_res) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("value overflow", K(ret), K(i), K(data_l[i]), K(data_r[i]));
      } else if (OB_FAIL(float_array->push_back(float_res))) {
        LOG_WARN("failed to push back value", K(ret), K(float_res));
      }
    }
    ObString res_str;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res,
                                                       arr_res->get_raw_binary_len(),
                                                       ctx.get_expr_res_alloc(),
                                                       res_str))) {
      LOG_WARN("get array binary string failed", K(ret), K(*coll_info));
    //   FIXME huhaosheng.hhs: maybe set batch_idx_ before in order to use frame res_buf
    // } else if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res, expr, ctx, res_str))) {

    //   LOG_WARN("get array binary string failed", K(ret), K(*coll_info));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObArrayExprUtils::dispatch_array_attrs(ObEvalCtx &ctx, ObIArrayType *arr_obj, ObExpr **attrs, uint32_t attr_count, const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  ObArrayAttr arr_attrs[attr_count];
  uint32_t attr_idx = 0;
  if (OB_FAIL(arr_obj->flatten(arr_attrs, attr_count, attr_idx))) {
    LOG_WARN("array flatten failed", K(ret));
  } else {
    for (uint32_t i = 0; i < attr_count; i++) {
      ObIVector *vec = attrs[i]->get_vector(ctx);
      if (i == 0) {
        vec->set_int(row_idx, arr_obj->size());
      } else if (arr_attrs[i - 1].ptr_ == NULL && arr_attrs[i - 1].length_ == 0) {
        vec->set_payload_shallow(row_idx, NULL, 0); // get ride of random values
        vec->set_null(row_idx);
      } else {
        const char *payload = arr_attrs[i - 1].ptr_;
        uint32_t len = arr_attrs[i - 1].length_;
        vec->set_payload_shallow(row_idx, payload, len);
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::dispatch_array_attrs(ObEvalCtx &ctx, ObExpr &expr, ObString &array_data, const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  // to fix : outrow lob can't use tmp allocator
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  const ObSqlCollectionInfo *coll_info = NULL;
  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else {
    ObLobCommon *lob_comm = (ObLobCommon*)(array_data.ptr());
    if (lob_comm->is_valid()
        && OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                            ObLongTextType,
                                                            CS_TYPE_BINARY,
                                                            true,
                                                            array_data))) {
      LOG_WARN("fail to get real data.", K(ret), K(array_data));
    } else {
      ObIArrayType *arr_obj = NULL;
      coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
      ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
      if (OB_ISNULL(coll_info)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("collect info is null", K(ret), K(subschema_id));
      } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *arr_type, arr_obj, true))) {
        LOG_WARN("construct array obj failed", K(ret), K(subschema_id), K(coll_info));
      } else if (OB_FAIL(arr_obj->init(array_data))) {
        LOG_WARN("init array obj failed", K(ret), K(subschema_id), K(coll_info));
      } else if (OB_FAIL(dispatch_array_attrs(ctx, arr_obj, expr.attrs_, expr.attrs_cnt_, row_idx))) {
        LOG_WARN("dispatch array attributes failed", K(ret), K(subschema_id), K(coll_info));
      }
    }
  }
  return ret;
}

int ObVectorFloatArithFunc::operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const
{
  UNUSED(type);
  int ret = OB_SUCCESS;
  const ObExpr &left_expr = *expr.args_[0];
  const ObExpr &right_expr = *expr.args_[1];
  ObIArrayType *arr_l = NULL;
  float data_r = r.get_float();
  ObIArrayType *arr_res = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionArrayType *arr_type = NULL;
  if (0 == data_r) {
    res.set_null();
  } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(left_expr, l, ctx, tmp_allocator, arr_l))) {
    LOG_WARN("failed to get vector", K(ret));
  } else if (OB_ISNULL(arr_l)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr_l));
  } else if (arr_l->contain_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("array with null can't add", K(ret));
  } else if (FALSE_IT(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
  } else if (OB_ISNULL(coll_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("collect info is null", K(ret), K(subschema_id));
  } else if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_NULL_VALUE;
     LOG_WARN("array type is null", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *arr_type, arr_res))) {
    LOG_WARN("construct array obj failed", K(ret), K(subschema_id), K(coll_info));
  } else {
    const float *data_l = reinterpret_cast<const float*>(arr_l->get_data());
    const uint32_t size = arr_l->size();
    ObVectorData *float_array = static_cast<ObVectorData *>(arr_res);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      const float float_res = data_l[i] / data_r; // only support div now
      if (isinff(float_res) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("value overflow", K(ret), K(i), K(data_l[i]), K(data_r));
      } else if (OB_FAIL(float_array->push_back(float_res))) {
        LOG_WARN("failed to push back value", K(ret), K(float_res));
      }
    }
    ObString res_str;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res,
                                                       arr_res->get_raw_binary_len(),
                                                       ctx.get_expr_res_alloc(),
                                                       res_str))) {
      LOG_WARN("get array binary string failed", K(ret), K(*coll_info));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObArrayExprUtils::batch_dispatch_array_attrs(ObEvalCtx &ctx, ObExpr &expr, int64_t begin, int64_t batch_size, const uint16_t *selector)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = expr.get_vector(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObIArrayType *arr_obj = NULL;
  // to fix : outrow lob can't use tmp allocator
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(construct_array_obj(tmp_allocator, ctx, expr.obj_meta_.get_subschema_id(), arr_obj))) {
    LOG_WARN("fail to construct array obj.", K(ret));
  } else {
    for (int64_t row_idx = begin; row_idx < begin + batch_size && OB_SUCC(ret); row_idx++) {
      int64_t idx = selector != NULL ? selector[row_idx] : row_idx;
      ObString raw_data = vec->get_string(idx);
      uint32_t attr_idx = 0;
      if (vec->is_null(idx)) {
        for (uint32_t i = 0; i < expr.attrs_cnt_; i++) {
          ObIVector *attr_vec = expr.attrs_[i]->get_vector(ctx);
          attr_vec->set_null(row_idx);
        }
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                            ObLongTextType,
                                                            CS_TYPE_BINARY,
                                                            true,
                                                            raw_data))) {
        LOG_WARN("fail to get real data.", K(ret), K(raw_data));
      } else if (OB_FAIL(arr_obj->init(raw_data))) {
        LOG_WARN("init array obj failed", K(ret));
      } else if (OB_FAIL(dispatch_array_attrs_rows(ctx, arr_obj, idx, expr.attrs_, expr.attrs_cnt_))) {
        LOG_WARN("failed to dispatch array attrs rows", K(ret));
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::assemble_array_attrs(ObEvalCtx &ctx, const ObExpr &expr, int64_t row_idx, ObIArrayType *arr_obj)
{
  int ret = OB_SUCCESS;
  ObDatum attr_val[expr.attrs_cnt_];
  for (uint32_t i = 0; i < expr.attrs_cnt_; ++i) {
    ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
    const char *payload = NULL;
    ObLength payload_len = 0;
    vec->get_payload(row_idx, payload, payload_len);
    attr_val[i].ptr_ = payload;
    attr_val[i].pack_ = payload_len;
  }
  if (OB_FAIL(arr_obj->init(attr_val, expr.attrs_cnt_))) {
    LOG_WARN("init array attrs failed", K(ret));
  }
  return ret;
}

int ObArrayExprUtils::transform_array_to_uniform(ObEvalCtx &ctx, const ObExpr &expr, const int64_t batch_size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  const ObBitVector *nulls = NULL;
  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema", K(ret));
  } else if (expr.get_vector(ctx)->get_format() == VEC_DISCRETE
             && FALSE_IT(nulls = static_cast<ObDiscreteBase *>(expr.get_vector(ctx))->get_nulls()) ) {
  } else if (expr.get_vector(ctx)->get_format() == VEC_CONTINUOUS
             && FALSE_IT(nulls = static_cast<ObContinuousBase *>(expr.get_vector(ctx))->get_nulls()) ) {
  } else {
    ObIArrayType *arr_obj = NULL;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &alloc = tmp_alloc_g.get_allocator();
    const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
    if (OB_ISNULL(coll_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("collect info is null", K(ret), K(subschema_id));
    } else if (OB_ISNULL(nulls)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("failed to get nulls", K(ret), K(expr.get_vector(ctx)->get_format()));
    } else if (OB_FAIL(ObArrayTypeObjFactory::construct(alloc, *arr_type, arr_obj, true))) {
      LOG_WARN("construct array obj failed", K(ret), K(subschema_id), K(coll_info));
    } else {
      ret = expr.init_vector(ctx, VEC_UNIFORM, batch_size);
      UniformFormat *root_vec = static_cast<UniformFormat *>(expr.get_vector(ctx));
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
        if (expr.attrs_cnt_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected attrs cnt", K(ret), K(expr.attrs_cnt_));
        } else if (skip != nullptr && skip->at(row_idx)) {
          // skip row
        } else if (nulls->at(row_idx) || expr.attrs_[0]->get_vector(ctx)->is_null(row_idx)) {
          root_vec->set_null(row_idx);
        } else if (OB_FAIL(assemble_array_attrs(ctx, expr, row_idx, arr_obj))) {
          LOG_WARN("assemble array attrs failed", K(ret));
        } else if (OB_FAIL(set_array_res(arr_obj, expr, ctx, root_vec, row_idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::calc_nested_expr_data_size(const ObExpr &expr, ObEvalCtx &ctx, const int64_t batch_idx, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  for (uint32_t i = 0; i < expr.attrs_cnt_; ++i) {
    ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
    VectorFormat format = vec->get_format();
    if (VEC_DISCRETE == format) {
      ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
      if (!disc_vec->is_null(batch_idx)) {
        ObLength *lens = disc_vec->get_lens();
        size += lens[batch_idx];
      }
    } else if (VEC_CONTINUOUS == format) {
      ObContinuousBase *cont_vec = static_cast<ObContinuousBase*>(vec);
      uint32_t *offsets = cont_vec->get_offsets();
      size += (offsets[batch_idx + 1] - offsets[batch_idx]);
    } else if (is_uniform_format(format)) {
      ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
      ObDatum *datums = uni_vec->get_datums();
      const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
      size += datums[batch_idx & idx_mask].len_;
    } else if (VEC_FIXED == format) {
      // array len
      size += sizeof(uint32_t);
    }
  }
  ObTextStringResult blob_res(ObLongTextType, true, nullptr);
  if (OB_FAIL(blob_res.calc_buffer_len(size))) {
    LOG_WARN("calculate data size failed", K(ret));
  } else {
    size = blob_res.get_buff_len();
  }
  return ret;
}

int ObArrayExprUtils::construct_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, ObIArrayType *&res, bool read_only)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue meta;
  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, meta))) {
    LOG_WARN("failed to get subschema meta", K(ret), K(subschema_id));
  } else {
    const ObSqlCollectionInfo *src_coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(meta.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(src_coll_info->collection_meta_);
    if (OB_FAIL(ObArrayTypeObjFactory::construct(alloc, *arr_type, res, read_only))) {
      LOG_WARN("construct array obj failed", K(ret), K(src_coll_info));
    }
  }
  return ret;
}

int ObArrayExprUtils::get_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, const ObString &raw_data, ObIArrayType *&res)
{
  int ret = OB_SUCCESS;
  ObString data_str = raw_data;
  if (res == NULL && OB_FAIL(construct_array_obj(alloc, ctx, subschema_id, res))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&alloc,
                                                              ObLongTextType,
                                                              CS_TYPE_BINARY,
                                                              true,
                                                              data_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(data_str));
  } else if (OB_FAIL(res->init(data_str))) {
    LOG_WARN("failed to init array", K(ret));
  }
  return ret;
}

int ObArrayExprUtils::dispatch_array_attrs_rows(ObEvalCtx &ctx, ObIArrayType *arr_obj, const int64_t row_idx,
                                                ObExpr **attrs, uint32_t attr_count, bool is_shallow)
{
  int ret = OB_SUCCESS;
  ObArrayAttr arr_attrs[attr_count];
  uint32_t attr_idx = 0;
  if (OB_FAIL(arr_obj->flatten(arr_attrs, attr_count, attr_idx))) {
    LOG_WARN("array flatten failed", K(ret));
  } else {
    for (uint32_t i = 0; i < attr_count; i++) {
      ObIVector *vec = attrs[i]->get_vector(ctx);
      if (i == 0) {
        vec->set_int(row_idx, arr_obj->size());
      } else if (arr_attrs[i - 1].ptr_ == NULL && arr_attrs[i - 1].length_ == 0) {
        vec->set_payload_shallow(row_idx, NULL, 0); // get ride of random values
        vec->set_null(row_idx);
      } else {
        const char *payload = arr_attrs[i - 1].ptr_;
        uint32_t len = arr_attrs[i - 1].length_;
        (is_shallow || payload == NULL) ? vec->set_payload_shallow(row_idx, payload, len) : vec->set_payload(row_idx, payload, len);
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::nested_expr_from_rows(const ObExpr &expr, ObEvalCtx &ctx, const sql::RowMeta &row_meta, const sql::ObCompactRow **stored_rows,
                                            const int64_t size, const int64_t col_idx, const int64_t *selector)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = expr.get_vector(ctx);
  VectorFormat format = vec->get_format();
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  // to fix : outrow lob can't use tmp allocator
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObIArrayType *arr_obj = NULL;
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();

  if (OB_FAIL(construct_array_obj(tmp_allocator, ctx, subschema_id, arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  }
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t row_idx = i;
    if (selector != nullptr) {
      row_idx = selector[i];
    }
    if (stored_rows[i]->is_null(col_idx)) {
      vec->set_null(row_idx);
      set_expr_attrs_null(expr, ctx, row_idx);
    } else {
      const char *payload = NULL;
      ObLength len = 0;
      stored_rows[i]->get_cell_payload(row_meta, col_idx, payload, len);
      ObLobCommon *lob_comm = (ObLobCommon*)(payload);
      ObString array_data(len, payload);
      if (lob_comm->is_valid()) {
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                              ObLongTextType,
                                                              CS_TYPE_BINARY,
                                                              true,
                                                              array_data))) {
          LOG_WARN("fail to get real data.", K(ret), K(array_data));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(arr_obj->init(array_data))) {
        LOG_WARN("failed to init array", K(ret));
      } else if (OB_FAIL(dispatch_array_attrs_rows(ctx, arr_obj, row_idx, expr.attrs_, expr.attrs_cnt_))) {
        LOG_WARN("failed to dispatch array attrs rows", K(ret));
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::nested_expr_to_rows(const ObExpr &expr, ObEvalCtx &ctx, const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                                          const uint16_t selector[], const int64_t size, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = expr.get_vector(ctx);
  VectorFormat format = vec->get_format();
  for (int64_t i = 0; i < size; i++) {
    int64_t row_idx = selector[i];
    if (vec->is_null(row_idx)) {
      stored_rows[i]->set_null(row_meta, col_idx);
    } else {
      int64_t pos = 0;
      for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
        const char *payload = NULL;
        ObLength payload_len = 0;
        ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
        VectorFormat format = vec->get_format();
        vec->get_payload(row_idx, payload, payload_len);
        stored_rows[i]->append_cell_payload(row_meta, col_idx, payload, payload_len, pos);
      }
    }
  }
  return ret;
}

int ObArrayExprUtils::nested_expr_to_row(const ObExpr &expr, ObEvalCtx &ctx, char *row_buf,
                                         const int64_t col_offset, const uint64_t row_idx, int64_t &cell_len,
                                         const int64_t *remain_size)
{
  int ret = OB_SUCCESS;
  uint32_t data_len = 0;
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObSubSchemaValue meta;
  bool is_vector = false;
  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, meta))) {
    LOG_WARN("failed to get subschema meta", K(ret), K(subschema_id));
  } else {
    const ObSqlCollectionInfo *src_coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(meta.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(src_coll_info->collection_meta_);
    is_vector = arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE;
    if (is_vector) {
      ObIVector *vec = expr.attrs_[2]->get_vector(ctx);
      data_len += vec->get_length(row_idx);
    } else {
      for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
        if (i == 0) {
          data_len += sizeof(uint32_t);
        } else {
          ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
          data_len += vec->get_length(row_idx);
        }
      }
    }
    ObString res_buf;
    ObTextStringResult blob_res(ObLongTextType, true, nullptr);
    if (OB_FAIL(blob_res.calc_buffer_len(data_len))) {
      LOG_WARN("calc buffer len failed", K(ret), K(data_len));
    } else if (remain_size != NULL && blob_res.get_buff_len() > *remain_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("row memory isn't enough", K(ret), K(blob_res.get_buff_len()), K(*remain_size));
    } else if (FALSE_IT(res_buf.assign_ptr(row_buf + col_offset, blob_res.get_buff_len()))) {
    } else if (OB_FAIL(blob_res.init(data_len, res_buf))) {
      LOG_WARN("text string init failed", K(ret), K(data_len));
    } else if (is_vector) {
      const char *payload = NULL;
      ObLength payload_len = 0;
      ObIVector *vec = expr.attrs_[2]->get_vector(ctx);
      vec->get_payload(row_idx, payload, payload_len);
      if (OB_FAIL(blob_res.append(payload, payload_len))) {
        LOG_WARN("failed to append realdata", K(ret), K(payload_len));
      }
    } else {
      for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
        const char *payload = NULL;
        ObLength payload_len = 0;
        ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
        uint32_t len = 0;
        if (i == 0) {
          len = vec->get_uint32(row_idx);
          payload = reinterpret_cast<const char *>(&len);
          payload_len = sizeof(uint32_t);
        } else {
          vec->get_payload(row_idx, payload, payload_len);
        }
        if (OB_FAIL(blob_res.append(payload, payload_len))) {
          LOG_WARN("failed to append realdata", K(ret), K(payload_len));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cell_len = blob_res.get_buff_len();
    }
  }
  return ret;
}

void ObArrayExprUtils::set_expr_attrs_null(const ObExpr &expr, ObEvalCtx &ctx, const int64_t idx)
{
  ObIVector *vec = expr.get_vector(ctx);
  VectorFormat format = vec->get_format();
  if (is_uniform_format(format)) {
    // do nothing
  } else {
    for (uint32_t i = 0; i < expr.attrs_cnt_; ++i) {
      ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
      vec->set_null(idx);
    }
  }
}

int ObNestedVectorFunc::construct_attr_param(ObIAllocator &alloc, ObEvalCtx &ctx, ObExpr &param_expr,
    const uint16_t meta_id, int64_t row_idx, ObIArrayType *&param_obj)
{
  int ret = OB_SUCCESS;
  if (param_obj == NULL && OB_FAIL(ObArrayExprUtils::construct_array_obj(alloc, ctx, meta_id, param_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::assemble_array_attrs(ctx, param_expr, row_idx, param_obj))) {
    LOG_WARN("assemble array attrs failed", K(ret));
  }
  return ret;
}

int ObNestedVectorFunc::construct_param(
    ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id, ObString &str_data, ObIArrayType *&param_obj)
{
  return ObArrayExprUtils::get_array_obj(alloc, ctx, meta_id, str_data, param_obj);
}

int ObNestedVectorFunc::construct_res_obj(
    ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id, ObIArrayType *&res_obj)
{
  return ObArrayExprUtils::construct_array_obj(alloc, ctx, meta_id, res_obj, false);
}

int ObNestedVectorFunc::construct_params(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t left_meta_id,
    const uint16_t right_meta_id, const uint16_t res_meta_id, ObString &left, ObString right, ObIArrayType *&left_obj,
    ObIArrayType *&right_obj, ObIArrayType *&res_obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc, ctx, left_meta_id, left, left_obj))) {
    SQL_ENG_LOG(WARN, "get array failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc, ctx, right_meta_id, right, right_obj))) {
    SQL_ENG_LOG(WARN, "get array failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(alloc, ctx, res_meta_id, res_obj, false))) {
    SQL_ENG_LOG(WARN, "construct res array failed", K(ret));
  }
  return ret;
}

} // sql
} // oceanbase
