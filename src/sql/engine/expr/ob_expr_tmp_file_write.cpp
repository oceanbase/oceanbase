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

#include "sql/engine/expr/ob_expr_tmp_file_write.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
namespace sql
{
ObExprTmpFileWrite::ObExprTmpFileWrite(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_TMP_FILE_WRITE, N_TMP_FILE_WRITE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprTmpFileWrite::~ObExprTmpFileWrite() {}

int ObExprTmpFileWrite::calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  type1.set_calc_type(ObIntType); // fd
  type2.set_calc_type(ObVarcharType); // data

  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

  return ret;
}

int ObExprTmpFileWrite::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_tmp_file_write;
  return ret;
}

int ObExprTmpFileWrite::eval_tmp_file_write(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *fd_datum = NULL;
  ObDatum *data_datum = NULL;
  int64_t fd = -1;
  ObString data_str;

  if (OB_FAIL(expr.args_[0]->eval(ctx, fd_datum))) {
    LOG_WARN("eval arg 0 failed", KR(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, data_datum))) {
    LOG_WARN("eval arg 1 failed", KR(ret));
  } else if (fd_datum->is_null() || data_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(validate_input_params(*fd_datum, *data_datum, fd, data_str))) {
    LOG_WARN("validate input params failed", KR(ret));
  } else if (data_str.length() > 0) {
    int64_t written_size = 0;
    if (OB_FAIL(write_temp_file(fd, data_str, written_size))) {
      LOG_WARN("write temp file failed", KR(ret));
    } else {
      res_datum.set_int(written_size); // return data size
    }
  }

  return ret;
}

int ObExprTmpFileWrite::validate_input_params(const ObDatum &fd_datum, const ObDatum &data_datum,
                                              int64_t &fd, ObString &data_str)
{
  int ret = OB_SUCCESS;

  if (fd_datum.is_null() || data_datum.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file descriptor cannot be null", KR(ret));
  } else {
    fd = fd_datum.get_int();
    data_str = data_datum.get_string();
    if (fd < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid file descriptor", KR(ret), K(fd));
    }
    if (data_str.empty()) {
      LOG_INFO("data string is empty", K(fd));
    }
  }
  return ret;
}

int ObExprTmpFileWrite::write_temp_file(int64_t fd, const ObString &data_str, int64_t &written_size)
{
  int ret = OB_SUCCESS;
  written_size = 0;

  tmp_file::ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.size_ = data_str.length();
  io_info.buf_ = const_cast<char *>(data_str.ptr());
  io_info.io_desc_.set_wait_event(ObWaitEventIds::TMP_FILE_WRITE);
  io_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());

  if(!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io info", KR(ret), K(fd), K(io_info));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(MTL_ID(), io_info))) {
    LOG_WARN("write to temp file failed", KR(ret), K(fd), K(data_str.length()));
  } else {
    written_size = data_str.length();
    LOG_DEBUG("write to temp file success", K(fd), K(written_size));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase