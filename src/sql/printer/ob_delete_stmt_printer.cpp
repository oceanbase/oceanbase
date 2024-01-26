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

#define USING_LOG_PREFIX SQL
#include "sql/printer/ob_delete_stmt_printer.h"
#include "sql/ob_sql_context.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

void ObDeleteStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObDeleteStmt *stmt)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
}

int ObDeleteStmtPrinter::do_print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be NULL", K(ret));
  } else {
    expr_printer_.init(buf_,
                       buf_len_,
                       pos_,
                       schema_guard_,
                       print_params_,
                       param_store_);
    if (OB_FAIL(print())) {
      LOG_WARN("fail to print stmt", K(ret));
    }
  }

  return ret;
}


int ObDeleteStmtPrinter::print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_FAIL(print_basic_stmt())) {
    LOG_WARN("fail to print basic stmt", K(ret), K(*stmt_));
  } else { /*do nothing*/ }

  return ret;
}

int ObDeleteStmtPrinter::print_basic_stmt()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_FAIL(print_with())) {
    LOG_WARN("failed to print with", K(ret));
  } else if (OB_FAIL(print_temp_table_as_cte())) {
    LOG_WARN("failed to print cte", K(ret));
  } else if (OB_FAIL(print_delete())) {
    LOG_WARN("fail to print select", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_from())) {
    LOG_WARN("fail to print from", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_where())) {
    LOG_WARN("fail to print where", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_order_by())) {
    LOG_WARN("fail to print order by", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_limit())) {
    LOG_WARN("fail to print limit", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_returning())) {
    LOG_WARN("fail to print_returning", K(ret), K(*stmt_));
  } else {
    // do-nothing
  }

  return ret;
}

int ObDeleteStmtPrinter::print_delete()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_delete_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid delete stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    DATA_PRINTF("delete ");
    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_hint())) { // hint
        LOG_WARN("fail to print hint", K(ret), K(*stmt_));
      }
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase



