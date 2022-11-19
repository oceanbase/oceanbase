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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_link.h"
#include "sql/optimizer/ob_log_plan.h"
//#include "sql/ob_sql_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

ObLogLink::ObLogLink(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    allocator_(plan.get_allocator()),
    stmt_fmt_buf_(NULL),
    stmt_fmt_len_(0),
    param_infos_()
{}

int ObLogLink::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    card_ = child->get_card();
    op_cost_ = 0;
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogLink::print_link_stmt(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (stmt_fmt_len_ > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("print link stmt failed", K(ret), K(stmt_fmt_len_), K(buf_len));
  } else {
    MEMCPY(buf, stmt_fmt_buf_, stmt_fmt_len_);
    char *ch = buf;
    char *stmt_end = buf + stmt_fmt_len_ - 3;
    while (ch < stmt_end) {
      if (0 == ch[0] && 0 == ch[1]) {
        uint16_t param_idx = *(uint16_t *)(ch + 2);
        ch[0] = '$';
        if (param_idx > 999) {
          ch[1] = 'M';
          ch[2] = 'A';
          ch[3] = 'X';
        } else {
          ch[3] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[2] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[1] = static_cast<char>('0' + param_idx % 10);
        }
        ch += 4;
      } else {
        ch++;
      }
    }
  }
  return ret;
}

int ObLogLink::print_my_plan_annotation(char *buf, int64_t &buf_len, int64_t &pos, ExplainType type)
{
  UNUSED(type);
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (type != EXPLAIN_BASIC && OB_FAIL(BUF_PRINTF(", dblink_id=%lu,", get_dblink_id()))) {
    LOG_WARN("BUF_PRINTF failed", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      link_stmt="))) {
    LOG_WARN("BUF_PRINTF failed", K(ret));
  } else if (OB_FAIL(print_link_stmt(buf + pos, buf_len - pos))) {
    LOG_WARN("failed to print link stmt", K(ret));
  } else {
    pos += stmt_fmt_len_;
  }
  return ret;
}

int ObLogLink::generate_link_sql_post(GenLinkStmtPostContext &link_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(link_ctx.spell_link(static_cast<const ObSelectStmt *>(get_stmt()),
                                  &stmt_fmt_buf_,
                                  stmt_fmt_len_,
                                  get_op_ordering(),
                                  output_exprs_,
                                  startup_exprs_,
                                  filter_exprs_))) {
    LOG_WARN("dblink fail to reverse spell link", K(dblink_id_), K(ret));
  } else {
    LOG_WARN("dblink stmt", K(ObString(stmt_fmt_len_, stmt_fmt_buf_)));
    link_ctx.reset();
  }
  return ret;
}

// see comment in ObConstRawExpr::get_name_internal().
int ObLogLink::gen_link_stmt_param_infos()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_fmt_buf_) || stmt_fmt_len_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt fmt is not inited", KP(stmt_fmt_buf_), K(stmt_fmt_len_));
  } else {
    param_infos_.reset();
  }
  int64_t param_pos = 0;
  int64_t param_idx = 0;
  const int64_t param_len = ObLinkStmtParam::get_param_len();
  while (OB_SUCC(ret) && param_idx >= 0) {
    if (OB_FAIL(ObLinkStmtParam::read_next(stmt_fmt_buf_, stmt_fmt_len_, param_pos, param_idx))) {
      LOG_WARN("failed to read next param", K(ret));
    } else if (param_idx < 0) {
      // skip.
    } else if (OB_FAIL(param_infos_.push_back(ObParamPosIdx(static_cast<int32_t>(param_pos),
                                                            static_cast<int32_t>(param_idx))))) {
      LOG_WARN("failed to push back param pos idx", K(ret), K(param_pos), K(param_idx));
    } else {
      param_pos += param_len;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
