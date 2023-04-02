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
#include "lib/json/ob_json.h"
#include "sql/optimizer/ob_help_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/resolver/ob_stmt.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;

int ObHelpLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *top = NULL;
  ObLogValues *values = nullptr;
  set_max_op_id(1);
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_help_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else if (OB_FAIL(allocate_values_as_top(top))) {
    LOG_WARN("failed to allocate expr values as top", K(ret));
  } else if (OB_ISNULL(values = static_cast<ObLogValues *>(top)) 
             || OB_UNLIKELY(LOG_VALUES != top->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_FAIL(values->set_row_store(
                     static_cast<const ObHelpStmt*>(get_stmt())->get_row_store()))) {
    LOG_WARN("failed to set row store", K(ret));
  } else {
    top->mark_is_plan_root();
    set_plan_root(top);
    get_optimizer_context().get_all_exprs().reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < values->get_col_count(); ++i) {
      if (OB_FAIL(allocate_output_expr_for_values_op(*values))) {
        LOG_WARN("failed to allocate output expr", K(ret));
      }
    }
  }
  return ret;
}
