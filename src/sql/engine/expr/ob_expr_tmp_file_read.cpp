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

#include "sql/engine/expr/ob_expr_tmp_file_read.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
namespace sql
{

ObExprTmpFileRead::ObExprTmpFileRead(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_TMP_FILE_READ, N_TMP_FILE_READ, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprTmpFileRead::~ObExprTmpFileRead() {}

int ObExprTmpFileRead::calc_result_type3(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, ObExprResType &type3,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  type1.set_calc_type(ObIntType); // fd
  type2.set_calc_type(ObIntType); // offset
  type3.set_calc_type(ObIntType); // size

  type.set_varchar();
  type.set_length(OB_MAX_VARCHAR_LENGTH);
  type.set_collation_type(CS_TYPE_BINARY);

  return ret;
}

int ObExprTmpFileRead::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_tmp_file_read;
  return ret;
}

int ObExprTmpFileRead::eval_tmp_file_read(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *fd_datum = NULL;
  ObDatum *offset_datum = NULL;
  ObDatum *size_datum = NULL;
  int64_t fd = -1;
  int64_t offset = 0;
  int64_t size = 0;

  if (OB_FAIL(expr.args_[0]->eval(ctx, fd_datum))) {
    LOG_WARN("eval arg 0 failed", KR(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, offset_datum))) {
    LOG_WARN("eval arg 1 failed", KR(ret));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, size_datum))) {
    LOG_WARN("eval arg 2 failed", KR(ret));
  } else if (fd_datum->is_null() || offset_datum->is_null() || size_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(validate_input_params(*fd_datum, *offset_datum, *size_datum, fd, offset, size))) {
    LOG_WARN("validate input params failed", KR(ret));
  } else {
    char *buffer = expr.get_str_res_mem(ctx, size);
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", KR(ret), K(size));
    } else if (OB_FAIL(read_temp_file(fd, offset, size, buffer))) {
      LOG_WARN("read temp file failed", KR(ret));
    } else {
      res_datum.set_string(buffer, size);
    }
  }

  return ret;
}

int ObExprTmpFileRead::validate_input_params(const ObDatum &fd_datum, const ObDatum &offset_datum, const ObDatum &size_datum,
                                              int64_t &fd, int64_t &offset, int64_t &size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(fd_datum.is_null() || offset_datum.is_null() || size_datum.is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input params cannot be null", KR(ret));
  } else {
    fd = fd_datum.get_int();
    offset = offset_datum.get_int();
    size = size_datum.get_int();
    if (fd < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid file descriptor", KR(ret), K(fd));
    }
    if (offset < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid offset", KR(ret), K(offset));
    }
    if (size <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid size", KR(ret), K(size));
    }
  }
  return ret;
}

int ObExprTmpFileRead::read_temp_file(int64_t fd, int64_t offset, int64_t size, char *buffer)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer cannot be null", KR(ret));
  } else {
    tmp_file::ObTmpFileIOHandle io_handle;
    tmp_file::ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.buf_ = buffer;
    io_info.size_ = size;
    io_info.io_desc_.set_wait_event(ObWaitEventIds::TMP_FILE_READ);
    io_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());

    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, offset, io_handle))) {
      LOG_WARN("read from temp file failed", KR(ret), K(fd), K(offset), K(size));
    } else {
      LOG_DEBUG("read from temp file success", K(fd), K(offset), K(size));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase