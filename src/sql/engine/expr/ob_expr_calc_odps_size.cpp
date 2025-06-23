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

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{

ObExprCalcOdpsSize::ObExprCalcOdpsSize(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CALC_ODPS_SIZE, N_CALC_ODPS_SIZE, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
    ret = OB_ERR_INVALID_TYPE_FOR_OP; // arithmetic not support row
  }
  return ret;
}

int ObExprCalcOdpsSize::calc_odps_size(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *file_url_datum = NULL;
  ObDatum *table_id_datum = NULL;
  int ret_file_url = expr.args_[0]->eval(ctx, file_url_datum);
  int ret_table_id  = expr.args_[1]->eval(ctx, table_id_datum);
  if (OB_SUCCESS == ret_file_url && OB_SUCCESS == ret_table_id) {
    if (file_url_datum->is_null() || table_id_datum->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", K(file_url_datum->is_null()), K(table_id_datum->is_null()), K(ret));
    } else {
      const ObString file_url = file_url_datum->get_string();
      const int64_t table_id = table_id_datum->get_int();
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
      }

      if (OB_SUCC(ret)) {
        if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
          if (OB_FAIL(ObOdpsPartitionDownloaderMgr::fetch_row_count(
                  file_url, table_schema->get_external_properties(),
                  row_count))) {
            if (ret == OB_ODPS_ERROR) {
              ret = OB_SUCCESS;
              row_count = -1;
            } else {
              LOG_WARN("failed to fetch row count", K(ret), K(file_url),
                       K(table_schema->get_external_properties()));
            }
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support odps cpp connector", K(ret), K(GCONF._use_odps_jni_connector));
#endif
        } else {
#if defined(OB_BUILD_JNI_ODPS)
          bool is_odps_external_table = false;
          ObODPSGeneralFormat::ApiMode odps_api_mode;
          if (OB_FAIL(ObSQLUtils::get_odps_api_mode(
                  table_schema->get_external_properties(), is_odps_external_table, odps_api_mode))) {
            LOG_WARN("failed to check is odps external table or not", K(ret), K(table_schema->get_external_properties()));
          } else {
            if (odps_api_mode != ObODPSGeneralFormat::TUNNEL_API) {
              // do nothing
              if (odps_api_mode == ObODPSGeneralFormat::BYTE) {
                // do nothing
              } else {
                if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_storage_row_count(
                        ctx.exec_ctx_, file_url, table_schema->get_external_properties(), row_count))) {
                  LOG_WARN("failed to fetch row count", K(ret), K(file_url), K(table_schema->get_external_properties()));
                } else {
                  // -1 means the partition spec not exists
                  LOG_TRACE("fetch row count with partition spec",
                      K(ret),
                      K(row_count),
                      K(file_url),
                      K(table_schema->get_external_properties()));
                }
              }
            } else {
              if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_row_count(
                      ctx.exec_ctx_, file_url, table_schema->get_external_properties(), row_count))) {
                LOG_WARN("failed to fetch row count", K(ret), K(file_url), K(table_schema->get_external_properties()));
              } else {
                // -1 means the partition spec not exists
                LOG_TRACE("fetch row count with partition spec",
                    K(ret),
                    K(row_count),
                    K(file_url),
                    K(table_schema->get_external_properties()));
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
        res_datum.set_int(row_count);
      }
    }
  } else {
    ret = (OB_SUCCESS == ret_file_url) ? ret_table_id : ret_file_url;
  }
  return ret;
}

int ObExprCalcOdpsSize::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK (2 == raw_expr.get_param_count());
  OX (rt_expr.eval_func_ = calc_odps_size);
  return ret;
}

} // namespace sql
} // namespace oceanbase
