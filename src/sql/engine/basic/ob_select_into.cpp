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
#include "ob_select_into.h"
#include "lib/file/ob_file.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"

namespace oceanbase {
using namespace common;
namespace sql {

class ObSelectInto::ObSelectIntoCtx : public ObPhyOperatorCtx {
  friend class ObSelectInto;

public:
  explicit ObSelectIntoCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx), top_limit_cnt_(INT64_MAX), file_appender_(), is_first_(true)
  {}
  virtual ~ObSelectIntoCtx()
  {}

  virtual void destroy()
  {
    file_appender_.~ObFileAppender();
    ObPhyOperatorCtx::destroy_base();
  }

  void reset()
  {
    is_first_ = true;
    file_appender_.close();
  }

private:
  int64_t top_limit_cnt_;
  ObFileAppender file_appender_;
  bool is_first_;
  ObObj filed_str_;
  ObObj line_str_;
  ObObj file_name_;
};

ObSelectInto::ObSelectInto(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      into_type_(T_INTO_OUTFILE),
      user_vars_(),
      outfile_name_(),
      filed_str_(),
      line_str_(),
      closed_cht_(0),
      is_optional_(false)
{}

ObSelectInto::~ObSelectInto()
{
  reset();
}

void ObSelectInto::reset()
{
  ObSingleChildPhyOperator::reset();
  user_vars_.reset();
  into_type_ = T_INTO_OUTFILE;
}

void ObSelectInto::reuse()
{
  ObSingleChildPhyOperator::reset();
  user_vars_.reuse();
  into_type_ = T_INTO_OUTFILE;
}

int ObSelectInto::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObSelectInto::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObSelectIntoCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create LimitCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObSelectInto::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSelectIntoCtx* into_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  ObExprCtx expr_ctx;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(into_ctx = GET_PHY_OPERATOR_CTX(ObSelectIntoCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt_ctx is null", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  } else if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else {
    into_ctx->file_name_ = outfile_name_;
    into_ctx->filed_str_ = filed_str_;
    into_ctx->line_str_ = line_str_;
  }

  int64_t row_count = 0;
  if (OB_SUCC(ret)) {
    bool need_check = false;
    const ObNewRow* row = NULL;
    if (OB_FAIL(ObSQLUtils::get_param_value(
            outfile_name_, phy_plan_ctx->get_param_store(), into_ctx->file_name_, need_check))) {
      LOG_WARN("get param value failed", K(ret));
    } else if (OB_FAIL(ObSQLUtils::get_param_value(
                   filed_str_, phy_plan_ctx->get_param_store(), into_ctx->filed_str_, need_check))) {
      LOG_WARN("get param value failed", K(ret));
    } else if (OB_FAIL(ObSQLUtils::get_param_value(
                   line_str_, phy_plan_ctx->get_param_store(), into_ctx->line_str_, need_check))) {
      LOG_WARN("get param value failed", K(ret));
    } else if (OB_FAIL(session->get_sql_select_limit(into_ctx->top_limit_cnt_))) {
      LOG_WARN("fail tp get sql select limit", K(ret));
    } else if (OB_FAIL(wrap_expr_ctx(ctx, into_ctx->expr_ctx_))) {
      LOG_WARN("wrap expr ctx failed", K(ret));
    } else if (OB_FAIL(init_cur_row(*into_ctx, true))) {
      LOG_WARN("fail to init cur row", K(ret));
    }

    while (OB_SUCC(ret) && row_count < into_ctx->top_limit_cnt_) {
      if (OB_FAIL(get_next_row(ctx, row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else {
        LOG_DEBUG("copy cur row", K(*row));
      }
      if (OB_SUCC(ret)) {
        if (T_INTO_VARIABLES == into_type_) {
          if (OB_FAIL(into_varlist(*row, into_ctx->expr_ctx_))) {
            LOG_WARN("into varlist failed", K(ret));
          }
        } else if (T_INTO_OUTFILE == into_type_) {
          if (OB_FAIL(into_outfile(*into_ctx, *row))) {
            LOG_WARN("into outfile failed", K(ret));
          }
        } else {
          if (OB_FAIL(into_dumpfile(*into_ctx, *row))) {
            LOG_WARN("into dumpfile failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {  // if into user variables or into dumpfile, must be one row
        ++row_count;
        if ((T_INTO_VARIABLES == into_type_ || T_INTO_DUMPFILE == into_type_) && row_count > 1) {
          ret = OB_ERR_TOO_MANY_ROWS;
          LOG_WARN("more than one row for into variables or into dumpfile", K(ret), K(row_count));
        }
      }
    }  // end while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {  // set affected rows
    phy_plan_ctx->set_affected_rows(row_count);
  }
  return ret;
}

int ObSelectInto::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObSelectIntoCtx* into_ctx = NULL;
  if (OB_ISNULL(into_ctx = GET_PHY_OPERATOR_CTX(ObSelectIntoCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get into ctx failed", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", "op_type", ob_phy_operator_type_str(child_op_->get_type()), K(ret));
    } else {
      // OB_ITER_END
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret));
  } else if (OB_FAIL(copy_cur_row(*into_ctx, row))) {
    LOG_WARN("copy current row failed", K(ret));
  } else {
    LOG_DEBUG("succ inner get next row", K(*row));
  }
  return ret;
}
/*
int ObSelectInto::get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
{
  int ret = OB_SUCCESS;
  ObSelectIntoCtx *into_ctx = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  int64_t row_count = 0;
  if (OB_ISNULL(into_ctx = GET_PHY_OPERATOR_CTX(ObSelectIntoCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get into ctx failed", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get plan ctx failed", K(ret));
  } else {
    // do nothing
  }

  while (OB_SUCC(ret) && row_count < into_ctx->top_limit_cnt_) {
    if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", "op_type", ob_phy_operator_type_str(child_op_->get_type()), K(ret));
      } else {
        // OB_ITER_END
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null", K(ret));
    } else if (T_INTO_VARIABLES == into_type_) {
      if (OB_FAIL(into_varlist(*row, into_ctx->expr_ctx_))) {
         LOG_WARN("into varlist failed", K(ret));
      }
    } else if (OB_FAIL(into_outfile(*into_ctx, *row))) {
      LOG_WARN("into outfile failed", K(ret));
    }
    if (OB_SUCC(ret)) { // if into user variables or into dumpfile, must be one row
      ++row_count;
      if ((T_INTO_VARIABLES == into_type_ || T_INTO_DUMPFILE == into_type_) && row_count > 1) {
        ret = OB_ERR_TOO_MANY_ROWS;
        LOG_WARN("more than one row for into variables or into dumpfile", K(ret), K(row_count));
      }
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) { // set affected rows
    phy_plan_ctx->set_affected_rows(row_count);
  }
  return ret;
}
*/
int ObSelectInto::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObSelectInto::get_row_str(const ObNewRow& row, const int64_t buf_len, bool is_first_row, char* buf, int64_t& pos,
    ObSelectIntoCtx& into_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj& filed_str = into_ctx.filed_str_;
  if (!is_first_row && into_ctx.line_str_.is_varying_len_char_type()) {  // lines terminated by "a"
    ret = databuff_printf(
        buf, buf_len, pos, "%.*s", into_ctx.line_str_.get_varchar().length(), into_ctx.line_str_.get_varchar().ptr());
  }
  for (int i = 0; OB_SUCC(ret) && i < row.get_count(); i++) {
    const ObObj& cell = row.get_cell(i);
    if (0 != closed_cht_ && (!is_optional_ || cell.is_string_type())) {
      // closed by "a" (for all cell) or optionally by "a" (for string cell)
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c", closed_cht_))) {
        LOG_WARN("print closed character failed", K(ret), K(closed_cht_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cell.print_plain_str_literal(buf, buf_len, pos))) {  // cell value
        LOG_WARN("print sql failed", K(ret), K(cell));
      } else if (0 != closed_cht_ && (!is_optional_ || cell.is_string_type())) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c", closed_cht_))) {
          LOG_WARN("print closed character failed", K(ret), K(closed_cht_));
        }
      }
      // filed terminated by "a"
      if (OB_SUCC(ret) && i != row.get_count() - 1 && filed_str.is_varying_len_char_type()) {
        if (OB_FAIL(databuff_printf(
                buf, buf_len, pos, "%.*s", filed_str.get_varchar().length(), filed_str.get_varchar().ptr()))) {
          LOG_WARN("print filed str failed", K(ret), K(filed_str));
        }
      }
    }
  }

  return ret;
}

int ObSelectInto::into_outfile(ObSelectIntoCtx& into_ctx, const ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  if (row.get_count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row in select into outfile", K(row.get_count()), K(ret));
  } else if (into_ctx.is_first_) {  // create file
    if (OB_FAIL(into_ctx.file_appender_.create(into_ctx.file_name_.get_varchar(), true))) {
      LOG_WARN("create dumpfile failed", K(ret), K(into_ctx.file_name_));
    } else {
      into_ctx.is_first_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    const ObObj& cell = row.get_cell(0);
    if (OB_FAIL(into_ctx.file_appender_.append(cell.get_string_ptr(), cell.get_string_len(), false))) {
      LOG_WARN("failed to append file");
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObSelectInto::into_dumpfile(ObSelectIntoCtx& into_ctx, const ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  char buf[MAX_VALUE_LENGTH];
  int64_t buf_len = MAX_VALUE_LENGTH;
  int64_t pos = 0;
  if (OB_FAIL(get_row_str(row, buf_len, into_ctx.is_first_, buf, pos, into_ctx))) {
    LOG_WARN("get row str failed", K(ret));
  } else if (into_ctx.is_first_) {  // create file
    if (OB_FAIL(into_ctx.file_appender_.create(into_ctx.file_name_.get_varchar(), true))) {
      LOG_WARN("create dumpfile failed", K(ret), K(into_ctx.file_name_));
    } else {
      into_ctx.is_first_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(into_ctx.file_appender_.append(buf, pos, false))) {
      LOG_WARN("failed to append file");
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObSelectInto::into_varlist(const ObNewRow& row, const ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (row.get_count() != user_vars_.count()) {
    ret = OB_ERR_COLUMN_SIZE;
    LOG_WARN("user vars count should be equal to cell count", K(row.get_count()), K(user_vars_.count()));
  } else {
    for (int i = 0; i < user_vars_.count(); ++i) {
      const ObString& var_name = user_vars_.at(i);
      if (OB_FAIL(ObVariableSetExecutor::set_user_variable(row.get_cell(i), var_name, expr_ctx))) {
        LOG_WARN("set user variable failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectInto::set_user_vars(const ObIArray<common::ObString>& user_vars, ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  ObString var;
  for (int64_t i = 0; OB_SUCC(ret) && i < user_vars.count(); ++i) {
    var.reset();
    if (OB_FAIL(ob_write_string(phy_alloc, user_vars.at(i), var))) {
      LOG_WARN("fail to deep copy string", K(user_vars.at(i)), K(ret));
    } else if (OB_FAIL(user_vars_.push_back(var))) {
      LOG_WARN("fail to push back var", K(var), K(ret));
    }
  }
  return ret;
}

int ObSelectInto::set_outfile_name(const common::ObObj& outfile_name, common::ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_obj(phy_alloc, outfile_name, outfile_name_))) {
    LOG_WARN("fail to deep copy obj", K(ret));
  }
  return ret;
}

int ObSelectInto::set_filed_str(const common::ObObj& filed_str, common::ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_obj(phy_alloc, filed_str, filed_str_))) {
    LOG_WARN("fail to deep copy obj", K(ret));
  }
  return ret;
}

int ObSelectInto::set_line_str(const common::ObObj& line_str, common::ObIAllocator& phy_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_obj(phy_alloc, line_str, line_str_))) {
    LOG_WARN("fail to deep copy obj", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObSelectInto, ObSingleChildPhyOperator), into_type_, closed_cht_, is_optional_, outfile_name_,
    filed_str_, line_str_, user_vars_);

}  // namespace sql
}  // namespace oceanbase
