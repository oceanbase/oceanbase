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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_odps_table_row_iter.h"
#include "sql/engine/expr/ob_expr_calc_odps_size.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{

ObExprCalcOdpsSize::ObExprCalcOdpsSize(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CALC_ODPS_SIZE, N_CALC_ODPS_SIZE, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprCalcOdpsSize::calc_result_type2(ObExprResType &type,
  ObExprResType &type1,
  ObExprResType &type2,
  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  UNUSED(type2);
  if (NOT_ROW_DIMENSION == row_dimension_) {
    if (ObMaxType == type1.get_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    } else {
      type.set_int();
      type1.set_calc_type(ObVarcharType);
      type2.set_calc_type(ObIntType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_WARN_ON_FAIL);
    }
    ObExprOperator::calc_result_flag2(type, type1, type2);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;  // arithmetic not support row
  }
  return ret;
}

int ObExprCalcOdpsSize::calc_result_type3(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 ObExprResType &type3,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  UNUSED(type2);
  if (NOT_ROW_DIMENSION == row_dimension_) {
    if (ObMaxType == type1.get_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    } else {
      type.set_varchar();
      type.set_calc_type(ObVarcharType);
      type.set_length(1024);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_collation_type(CS_TYPE_BINARY);
      type1.set_calc_type(ObVarcharType);
      type2.set_calc_type(ObIntType);
      type3.set_calc_type(ObVarcharType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_WARN_ON_FAIL);
    }
    ObExprOperator::calc_result_flag3(type, type1, type2, type3);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP; // arithmetic not support row
  }
  return ret;
}

int ObExprCalcOdpsSize::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_array,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (2 == param_num) {
    ret = calc_result_type2(type, types_array[0], types_array[1], type_ctx);
  } else if (3 == param_num) {
    ret = calc_result_type3(type, types_array[0], types_array[1], types_array[2], type_ctx);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObExprCalcOdpsSize::calc_odps_size(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  ObString session_id = ObString::make_empty_string();
  ObDatum *file_url_datum = NULL;
  int ret_file_url = expr.args_[0]->eval(ctx, file_url_datum);
  ObString property;
  int64_t rowcount_0_size_1_option = -1; //  0 get row count, 1 get size, -1 is not set

  ObString file_url;
  int64_t row_count = -1;
  if (OB_SUCCESS != ret_file_url) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ret_file_url));
  } else {
    if (file_url_datum->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", K(ret), K(file_url_datum->is_null()));
    } else {
      file_url = file_url_datum->get_string();
    }
  }

  if (OB_FAIL(ret)) {

  } else if (expr.arg_cnt_ == 3) {
    ObDatum *rowcount_0_size_1_option_datum = NULL;
    ObDatum *property_name_datum = NULL;
    int ret_rowcount_0_size_1_option = expr.args_[1]->eval(ctx, rowcount_0_size_1_option_datum);
    int ret_property_name = expr.args_[2]->eval(ctx, property_name_datum);
    if (OB_SUCCESS != ret_rowcount_0_size_1_option || OB_SUCCESS != ret_property_name) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ret_rowcount_0_size_1_option), K(ret_property_name));
    } else {
      if (property_name_datum->is_null() || rowcount_0_size_1_option_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), K(property_name_datum->is_null()));
      } else {
        property = property_name_datum->get_string();
        rowcount_0_size_1_option = rowcount_0_size_1_option_datum->get_int();
      }
      if (OB_FAIL(ret)) {
      } else if (rowcount_0_size_1_option != 1 && rowcount_0_size_1_option != 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected get size or row count", K(ret), K(rowcount_0_size_1_option));
      }
    }
  } else if (expr.arg_cnt_ == 2) {
    ObDatum *table_id_datum = NULL;
    int ret_table_id  = expr.args_[1]->eval(ctx, table_id_datum);
    int64_t table_id = -1;
    if (OB_SUCCESS != ret_table_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ret_table_id));
    } else {
      if (table_id_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), K(table_id_datum->is_null()));
      } else {
        table_id = table_id_datum->get_int();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (table_id == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table id", K(ret), K(table_id));
    } else {
      const uint64_t tenant_id = ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
      ObSchemaGetterGuard *schema_guard = ctx.exec_ctx_.get_sql_ctx()->schema_guard_;
      const ObTableSchema *table_schema = NULL;

      int64_t row_count = -1;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else {
        property = table_schema->get_external_properties();
        rowcount_0_size_1_option = 1; // 统计size
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument count", K(ret), K(expr.arg_cnt_));
  }

  ObOdpsJniConnector::OdpsFetchType fetch_type = ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT;
  if (OB_SUCC(ret)) {
    if (rowcount_0_size_1_option == 0) {
      fetch_type = ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT;
    } else if (rowcount_0_size_1_option == 1) {
      fetch_type = ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE;
    }
  }

  if (OB_SUCC(ret)) {
    if (!GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS)
      ret = fetch_size_or_row_count_wrapper<sql::ObODPSTableRowIterator,
                                            sql::ObOdpsPartitionDownloaderMgr>(
          file_url, property, fetch_type, row_count, allocator, session_id);
      if (ret == OB_ODPS_ERROR) {
        ret = OB_SUCCESS;
        row_count = -1;
      } else {
        LOG_TRACE("odps success to fetch row count", K(ret), K(file_url), K(row_count));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support odps cpp connector", K(ret), K(GCONF._use_odps_jni_connector));
#endif
    } else {
#if defined(OB_BUILD_JNI_ODPS)
      bool is_odps_external_table = false;
      ObODPSGeneralFormat::ApiMode odps_api_mode;
      if (OB_FAIL(ObSQLUtils::get_odps_api_mode(property, is_odps_external_table, odps_api_mode))) {
        LOG_WARN("failed to check is odps external table or not", K(ret), K(property));
      } else {
        if (odps_api_mode == ObODPSGeneralFormat::TUNNEL_API) {
          // do nothing
          ret = fetch_size_or_row_count_wrapper<
              sql::ObODPSJNITableRowIterator,
              sql::ObOdpsPartitionJNIDownloaderMgr>(
              file_url, property, fetch_type, row_count, allocator, session_id);
          if (ret == OB_JNI_JAVA_EXCEPTION_ERROR) {
            ret = OB_SUCCESS;
            row_count = -1;
          } else if (OB_FAIL(ret)) {
            LOG_INFO("failed to fetch row count", K(ret), K(file_url), K(property));
          }
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support odps jni connector", K(ret), K(GCONF._use_odps_jni_connector));
#endif
    }
  }
  if (OB_SUCC(ret)) {
    if (expr.arg_cnt_ == 2) {
      res_datum.set_int(row_count);
    } else if (expr.arg_cnt_ == 3) {
      // 拼接session_id和row_count
      // row_count to string
      ObFastFormatInt ffi(row_count);
      ObString answer = ObString::make_empty_string();
      ObString temp_str = ObString::make_string(ffi.str());
      ObString row_count_str = ObString::make_empty_string();
      if (OB_FAIL(ob_write_string(allocator, temp_str, row_count_str))) {
        LOG_WARN("failed to write row count", K(ret));
      } else {
        if (!session_id.empty()) {
          ObString str[3] = {row_count_str, ObString::make_string("|"), session_id};
          if (OB_FAIL(ob_concat_string(allocator, answer, 3, str))) {
            LOG_WARN("failed to concat string", K(ret));
          }
        } else {
          if (OB_FAIL(ob_write_string(allocator, row_count_str, answer))) {
            LOG_WARN("failed to write row count", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        char *buf = expr.get_str_res_mem(ctx, answer.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(answer.length()));
        } else {
          MEMCPY(buf, answer.ptr(), answer.length());
          answer.assign_ptr(buf, answer.length());
        }
        OX(res_datum.set_string(answer));
      }
    }
  }

  return ret;
}

int ObExprCalcOdpsSize::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK (2 == raw_expr.get_param_count() || 3 == raw_expr.get_param_count());
  OX (rt_expr.eval_func_ = calc_odps_size);
  return ret;
}

} // namespace sql
} // namespace oceanbase
