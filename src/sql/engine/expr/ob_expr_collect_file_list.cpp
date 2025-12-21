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
#include "sql/engine/expr/ob_expr_collect_file_list.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{

OB_SERIALIZE_MEMBER(ObExprCollectFileListRes, file_urls_, sizes_, last_modify_times_);

ObExprCollectFileList::ObExprCollectFileList(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_COLLECT_FILE_LIST,
                         N_COLLECT_FILE_LIST,
                         MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

int ObExprCollectFileList::calc_result_typeN(ObExprResType &type,
                                             ObExprResType *types_array,
                                             int64_t param_num,
                                             common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num == 3) {
    type.set_varchar();
    type.set_default_collation_type();
    types_array[0].set_calc_type(ObVarcharType);
    types_array[0].set_default_collation_type();
    types_array[1].set_calc_type(ObVarcharType);
    types_array[1].set_default_collation_type();
    types_array[2].set_calc_type(ObVarcharType);
    types_array[2].set_default_collation_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObExprCollectFileList::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (rt_expr.arg_cnt_ != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collect_file_list expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])
             || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of collect_file_list expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = collect_file_list;
  }
  return ret;
}

int ObExprCollectFileList::collect_file_list(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *datum_path = NULL;
  ObDatum *datum_patten = NULL;
  ObDatum *datum_access_info = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObExprRegexpSessionVariables regexp_vars;
  ObSEArray<ObString, 4> tmp_file_urls;
  ObSEArray<int64_t, 4> tmp_file_sizes;
  ObSEArray<int64_t, 4> tmp_last_modify_times;
  ObSEArray<ObString, 4> tmp_content_digests;
  ObArenaAllocator tmp_allocator;
  ObFixedArray<ObExprCollectFileListRes, ObIAllocator> res(tmp_allocator);
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(session->get_regexp_session_vars(regexp_vars))) {
    LOG_WARN("failed to get regexp_vars.", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, datum_path, datum_patten, datum_access_info))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(datum_path->is_null())) {
    res_datum.set_null();
  } else if (OB_FAIL(ObExternalTableFileManager::get_external_file_list_on_device(
                 datum_path->get_string(),
                 datum_patten->get_string(),
                 regexp_vars,
                 tmp_file_urls,
                 tmp_file_sizes,
                 tmp_last_modify_times,
                 tmp_content_digests,
                 datum_access_info->get_string(),
                 tmp_allocator))) {
    LOG_WARN("fail to get from external file list", K(ret), K(datum_path->get_string()));
  } else if (tmp_file_urls.count() != tmp_file_sizes.count()
             || tmp_file_urls.count() != tmp_last_modify_times.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not expected count.",
             K(ret),
             K(datum_path->get_string()),
             K(tmp_file_urls.count()),
             K(tmp_file_sizes.count()),
             K(tmp_last_modify_times.count()));
  } else if (OB_FAIL(prepare_serialize(tmp_allocator,
                                       tmp_file_urls,
                                       tmp_file_sizes,
                                       tmp_last_modify_times,
                                       res))) {
    LOG_WARN("failed to prepare serialize", K(ret));
  } else if (OB_FALSE_IT(buf_len = res.get_serialize_size())) {
  } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(buf_len));
  } else if (OB_FAIL(res.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    res_datum.set_string(buf, buf_len);
  }

  return ret;
}

int ObExprCollectFileList::prepare_serialize(
    ObIAllocator &allocator,
    const ObIArray<ObString> &files,
    const ObIArray<int64_t> &sizes,
    const ObIArray<int64_t> &last_modify_times,
    ObFixedArray<ObExprCollectFileListRes, ObIAllocator> &res)
{
  int ret = OB_SUCCESS;

  if (files.empty()) {
    // do nothing
  } else if (OB_FAIL(res.prepare_allocate(files.count()))) {
    LOG_WARN("failed to init array", K(ret), K(files.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); i++) {
    OZ(ob_write_string(allocator, files.at(i), res.at(i).file_urls_));
    OX(res.at(i).sizes_ = sizes.at(i));
    OX(res.at(i).last_modify_times_ = last_modify_times.at(i));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
