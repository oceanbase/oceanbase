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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_load_file.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>
#include "lib/oblog/ob_log.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprLoadFile::ObExprLoadFile(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_LOAD_FILE, N_LOAD_FILE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLoadFile::~ObExprLoadFile()
{
}

int ObExprLoadFile::calc_result_type1(ObExprResType &type,
                                        ObExprResType &str,
                                        ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  str.set_calc_type(ObVarcharType);

  const ObSQLSessionInfo *cur_session = type_ctx.get_session();
  ObCharsetType charset_type = CHARSET_INVALID;
  if(OB_FAIL(cur_session->get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (!ObCharset::is_valid_charset(charset_type)) {
    charset_type = CHARSET_UTF8MB4;
  }
  ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
  type.set_collation_type(collation_type);
  type.set_type(ObLongTextType);
  return ret;
}

int ObExprLoadFile::eval_load_file(const ObExpr &expr, ObEvalCtx &ctx,
                                    ObDatum &expr_datum)
{
	int ret = OB_SUCCESS;
	ObDatum *file_path_datum = NULL;
	if (expr.arg_cnt_ != 1 || 
			OB_ISNULL(expr.args_) ||
			OB_ISNULL(expr.args_[0])) {
		ret = OB_ERR_UNEXPECTED;
		LOG_WARN("invalid expr args", K(expr.arg_cnt_), K(expr));
	} else if(OB_FAIL(expr.args_[0]->eval(ctx, file_path_datum)) ||
							OB_ISNULL(file_path_datum)){
		ret = OB_ERR_UNEXPECTED;
		LOG_WARN("eval arg failed", K(ret), K(expr.args_[0]));
	} else if (file_path_datum->get_string().empty()){
    expr_datum.set_null();
		LOG_WARN("file path is empty");
  } else{
    ObString file_path = file_path_datum->get_string();
    char *full_path_buf = nullptr;
    char *actual_path = nullptr;
    ObString secure_file_priv;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    if (OB_ISNULL(full_path_buf = static_cast<char *>(tmp_allocator.alloc(MAX_PATH_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (OB_ISNULL(actual_path = realpath(file_path.ptr(), full_path_buf))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file not exist", K(ret), K(file_path));
    } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_secure_file_priv(secure_file_priv))) {
      LOG_WARN("failed to get secure file priv", K(ret));
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, actual_path))) {
      LOG_WARN("failed to check secure path", K(ret), K(secure_file_priv), K(actual_path));
    } else {
      common::ObFileReader file_reader;
      int64_t file_size = -1;
      int64_t max_allowed_packet = 0;
      
      if (OB_FAIL(file_reader.open(file_path.ptr(), false))) {
        LOG_WARN("fail to open file", KR(ret), K(file_path));
      } else if((file_size = ::get_file_size(file_path.ptr())) == -1){
        LOG_WARN("fail to get io device file size", KR(ret), K(file_size));
      } else if(OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_allowed_packet))){
        if (OB_ENTRY_NOT_EXIST == ret) { 
          ret = OB_SUCCESS;
          max_allowed_packet = OB_MAX_LONGTEXT_LENGTH;
        } else {
          expr_datum.set_null();
          LOG_WARN("Failed to get max allow packet size", K(ret));
        }
      } 
      if(OB_SUCC(ret)) {
        const int64_t data_buf_size = file_size + 1;
        void *lob_buf = nullptr;
        int64_t read_size = 0;
        if(file_size > OB_MAX_LONGTEXT_LENGTH) {
          expr_datum.set_null();
          LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast", static_cast<int>(OB_MAX_LONGTEXT_LENGTH));
        } else if(file_size > max_allowed_packet){
          expr_datum.set_null();
          LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast", static_cast<int>(max_allowed_packet));
        } else if (OB_ISNULL(lob_buf = tmp_allocator.alloc(data_buf_size + sizeof(ObLobCommon)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(data_buf_size));
        } else {
          ObLobCommon *lob_comm = new(lob_buf)ObLobCommon();
          bool eof = false;
          while (!eof && read_size < file_size) {
            int64_t this_read_size = 0;
            int64_t count = file_size - read_size;
            int64_t offset = read_size;
            if(OB_FAIL(file_reader.pread(lob_comm->buffer_ + read_size, count, offset, this_read_size))){
              LOG_WARN("fail to pread file buf", KR(ret), K(count), K(offset), K(this_read_size));
            } else if (0 == this_read_size) {
              eof = true;
            } else {
              read_size += this_read_size;
            }
          }
          expr_datum.set_lob_data(*lob_comm, data_buf_size + sizeof(ObLobCommon));
        } 
      }
    }
  }
	return ret;
}

int ObExprLoadFile::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprLoadFile::eval_load_file;
  return ret;
}

} /* sql */
} /* oceanbase */