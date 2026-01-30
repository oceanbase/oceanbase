/**
 * Copyright (c) 2025 OceanBase
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
#include "ob_expr_load_file.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using oceanbase::common::ObAIFuncJsonUtils;

namespace oceanbase
{
namespace sql
{

ObExprLoadFile::ObExprLoadFile(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOAD_FILE, N_LOAD_FILE, 2,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLoadFile::~ObExprLoadFile()
{
}

int ObExprLoadFile::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  if (type1.is_null() || type2.is_null()) {
    is_null_res = true;
  } else {
    if (!ob_is_string_type(type1.get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid template type", K(ret), K(type1.get_type()));
    } else {
      type1.set_calc_type(ObVarcharType);
      type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }

    if (OB_FAIL(ret)) {
    } else if (!ob_is_string_type(type2.get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid template type", K(ret), K(type2.get_type()));
    } else {
      type2.set_calc_type(ObVarcharType);
      type2.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else {
    type.set_blob();
    type.set_length(OB_MAX_LONGTEXT_LENGTH);
  }
  return ret;
}

int ObExprLoadFile::eval_load_file(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *location_name_datum = nullptr;
  ObDatum *filename_datum = nullptr;
  bool is_null_res = false;
  ObString file_data;

  if (ob_is_null(expr.obj_meta_.get_type())) {
    is_null_res = true;
  } else if (OB_FAIL(expr.eval_param_value(ctx, location_name_datum, filename_datum))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (location_name_datum->is_null() || filename_datum->is_null()) {
    is_null_res = true;
  } else {
    const ObString location_name = location_name_datum->get_string();
    const ObString filename = filename_datum->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_LOAD_FILE));
    if (OB_FAIL(read_file_from_location(location_name, filename, tenant_id, ctx.exec_ctx_, temp_allocator, file_data))) {
      LOG_WARN("fail to read file from location", K(ret), K(location_name), K(filename));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    expr_datum.set_null();
  } else {
    ObTextStringDatumResult blob_res(ObLongTextType, &expr, &ctx, &expr_datum);
    if (OB_FAIL(blob_res.init(file_data.length()))) {
      LOG_WARN("fail to init blob result", K(ret), K(file_data.length()));
    } else if (OB_FAIL(blob_res.append(file_data))) {
      LOG_WARN("fail to append file data", K(ret), K(file_data.length()));
    } else {
      blob_res.set_result();
    }
  }
  return ret;
}

int ObExprLoadFile::eval_load_file_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  // eval input parameters
  if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("eval parameters failed", K(ret));
  } else {
    // get vector data
    ObIVector *location_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *filename_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    // get temp allocator and tenant id
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_LOAD_FILE));

    // iterate each index and process
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      eval_flags.set(idx);

      ObString location_name = location_vec->get_string(idx);
      ObString filename = filename_vec->get_string(idx);
      ObString file_data;

      // read location_name and filename
      if (ob_is_null(expr.obj_meta_.get_type())) {
        is_null_res = true;
      } else if (location_vec->is_null(idx) || filename_vec->is_null(idx)) {
        is_null_res = true;
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  temp_allocator,
                  location_vec,
                  expr.args_[0]->datum_meta_,
                  expr.args_[0]->obj_meta_.has_lob_header(),
                  location_name,
                  idx))) {
        LOG_WARN("fail to read location_name from vector", K(ret), K(idx));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  temp_allocator,
                  filename_vec,
                  expr.args_[1]->datum_meta_,
                  expr.args_[1]->obj_meta_.has_lob_header(),
                  filename,
                  idx))) {
        LOG_WARN("fail to read filename from vector", K(ret), K(idx));
      } else if (OB_FAIL(read_file_from_location(location_name, filename, tenant_id, ctx.exec_ctx_, temp_allocator, file_data))) {
        LOG_WARN("fail to read file from location", K(ret), K(location_name), K(filename), K(idx));
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
      } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, file_data, idx))) {
        LOG_WARN("fail to save file data to vector as LOB", K(ret), K(idx), K(file_data.length()));
      }
    }
  }
  return ret;
}


/*

*/
int ObExprLoadFile::read_file_from_location(const ObString &location_name,
                                            const ObString &filename,
                                            const uint64_t tenant_id,
                                            ObExecContext &exec_ctx,
                                            ObIAllocator &alloc,
                                            ObString &file_data)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObLocationSchema *location_schema = nullptr;
  ObString file_url;
  ObString access_info;
  const ObSQLSessionInfo *session_info = exec_ctx.get_my_session();
  share::schema::ObSessionPrivInfo session_priv;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t document_ai_file_max_size = tenant_config.is_valid() ?
                                      tenant_config->document_ai_file_max_size
                                      : ObExprLoadFile::DEFAULT_DOCUMENT_AI_FILE_MAX_SIZE;
  if (location_name.empty() || filename.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location name or filename is empty", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_location_schema_by_name(tenant_id, location_name, location_schema))) {
    LOG_WARN("fail to get location schema by name", K(ret), K(location_name), K(tenant_id));
  } else if (OB_ISNULL(location_schema)) {
    ret = OB_LOCATION_OBJ_NOT_EXIST;
    LOG_WARN("location object does't exist", K(ret), K(tenant_id), K(location_name));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (OB_FAIL(session_info->get_session_priv_info(session_priv))) {
    LOG_WARN("fail to get session priv info", K(ret));
  } else if (OB_FAIL(schema_guard.check_location_access(session_priv,
                                                        session_info->get_enable_role_array(),
                                                        location_name,
                                                        false /* is_need_write_priv: read only */))) {
    LOG_WARN("location access denied", K(ret), K(location_name));
  } else {
    const ObString &location_url = location_schema->get_location_url_str();
    access_info = location_schema->get_location_access_info_str();
    if (OB_FAIL(build_file_path(location_url, filename, alloc, file_url))) {
      LOG_WARN("fail to build full file path", K(ret), K(location_url), K(filename));
    } else {
      ObExternalDataAccessDriver driver;
      int64_t file_size = 0;
      char *buffer = nullptr;
      ObArenaAllocator tmp_alloc;
      ObString file_url_cstring;
      // initialize driver (need to pass location_url and access_info)
      if (OB_FAIL(driver.init(location_url, access_info))) {
        LOG_WARN("fail to init external data access driver", K(ret), K(location_url));
      } else if (OB_FAIL(ob_write_string(tmp_alloc, file_url, file_url_cstring, true))) {
        LOG_WARN("fail to write file url string", K(ret), K(file_url));
      } else if (OB_FAIL(driver.get_file_size(file_url_cstring, file_size))) {
        LOG_WARN("fail to get file size", K(ret), K(file_url_cstring));
      } else if (file_size < 0) {
        ret = OB_OBJECT_NAME_NOT_EXIST;
        LOG_WARN("file does not exist", K(ret), K(file_url_cstring));
      } else if (file_size == 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid file size (empty file)", K(ret), K(file_size));
        FORWARD_USER_ERROR(ret, "invalid file size (empty file)");
      } else if (file_size > document_ai_file_max_size) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "file size exceeds document ai file max size is");
      } else if (OB_ISNULL(buffer = static_cast<char *>(alloc.alloc(file_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(file_size));
      } else {
        if (OB_FAIL(driver.open(file_url_cstring.ptr()))) {
          LOG_WARN("fail to open file", K(ret), K(file_url_cstring));
        } else {
          // read file content ,one pread must small than INT32_MAX = 2,147,483,647（2GB）
          int64_t read_bytes = 0;
          if (OB_FAIL(driver.pread(buffer, file_size, 0, read_bytes))) {
            LOG_WARN("fail to read file", K(ret), K(file_size));
          } else if (read_bytes != file_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("read bytes mismatch", K(ret), K(read_bytes), K(file_size));
          } else {
            file_data.assign_ptr(buffer, static_cast<int32_t>(read_bytes));
          }
          driver.close();
        }
      }
    }
  }
  return ret;
}

int ObExprLoadFile::build_file_path(const ObString &location_url, const ObString &filename, ObIAllocator &alloc, ObString &full_path)
{
  int ret = OB_SUCCESS;
  ObStringBuffer path_builder(&alloc);
  if (location_url.empty() || filename.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location url or filename is empty", K(ret));
  } else if (OB_FAIL(path_builder.reserve(location_url.length() + filename.length() + 2))) {
    LOG_WARN("fail to reserve memory", K(ret), K(location_url.length()), K(filename.length()));
  } else if (OB_FAIL(path_builder.append(location_url))) {
    LOG_WARN("fail to append location url", K(ret), K(location_url));
  } else {
    const char *url_ptr = location_url.ptr();
    int64_t url_len = location_url.length();
    bool need_separator = true;
    ObString path_str;
    char *buf = nullptr;
    if ((url_len > 0 && url_ptr[url_len - 1] == '/') || (filename.length() > 0 && filename.ptr()[0] == '/')) {
      need_separator = false;
    }
    if (need_separator && OB_FAIL(path_builder.append("/"))) {
      LOG_WARN("fail to append separator", K(ret));
    } else if (OB_FAIL(path_builder.append(filename))) {
      LOG_WARN("fail to append filename", K(ret), K(filename));
    } else if (OB_FALSE_IT(path_str = path_builder.string())) {
    } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(path_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(path_str.length()));
    } else {
      MEMCPY(buf, path_str.ptr(), path_str.length());
      full_path.assign_ptr(buf, path_str.length());
    }
  }
  return ret;
}

int ObExprLoadFile::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);

  rt_expr.eval_func_ = eval_load_file;
  rt_expr.eval_vector_func_ = eval_load_file_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
