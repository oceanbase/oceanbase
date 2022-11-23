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
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/code_generator/ob_code_generator.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;

/**
 *  This function does two things:
 *
 *  1. generate a logical plan for the 'real' query that's being 'explained'
 *  2. generate the plan text from the logical plan and put the text in the buffer
 *     and remember the logical plan as well.
 */
int ObExplainLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_explain_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    ObLogPlan *child_plan = NULL;
    const ObDMLStmt *child_stmt = NULL;
    ObLogicalOperator *top = NULL;
    ObLogValues *values_op = NULL;
    int64_t batch_size = 0;
    const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
    if (OB_ISNULL(child_stmt = explain_stmt->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(child_plan = optimizer_context_.get_log_plan_factory().
                         create(optimizer_context_, *child_stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create log plan for explain stmt");
    } else if (OB_FAIL(child_plan->init_plan_info())) {
      LOG_WARN("failed to init equal_sets");
    } else if (OB_FAIL(child_plan->generate_plan())) {
      LOG_WARN("failed to generate plan tree for explain", K(ret));
    } else if (OB_FAIL(ObCodeGenerator::detect_batch_size(*child_plan, batch_size))) {
      LOG_WARN("detect batch size failed", K(ret));
    } else if (OB_FAIL(allocate_values_as_top(top))) {
      LOG_WARN("failed to allocate expr values_op as top", K(ret));
    } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_VALUES != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(top), K(ret));
    } else {
      set_plan_root(top);
      top->mark_is_plan_root();
      child_plan->get_optimizer_context().set_batch_size(batch_size);
      char *buf = NULL;
      values_op = static_cast<ObLogValues*>(top);
      if (OB_ISNULL(
          buf = static_cast<char*>(get_allocator().alloc(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to allocate buffer", "buffer size",
            static_cast<int64_t>(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE), K(ret));
      } else {
        int64_t pos = 0;
        values_op->set_explain_plan(child_plan);

        if (EXPLAIN_FORMAT_JSON == explain_stmt->get_explain_type()) {
          char *pre_buf = NULL;
          if (NULL
              == (pre_buf = static_cast<char*>(get_allocator().alloc(
                  ObLogValues::MAX_EXPLAIN_BUFFER_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("Failed to allocate buffer", "buffer size",
                static_cast<int64_t>(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE), K(ret));
          } else if (NULL == child_plan->get_plan_root()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid argument", K(ret));
          } else {
            Value *child_value = NULL;
            if (OB_FAIL(
                child_plan->get_plan_root()->to_json(pre_buf, ObLogValues::MAX_EXPLAIN_BUFFER_SIZE,
                    pos, child_value))) {
              LOG_WARN("to_json fails", K(ret), K(child_value));
            } else {
              Tidy tidy(child_value);
              pos = tidy.to_string(buf, ObLogValues::MAX_EXPLAIN_BUFFER_SIZE);
              if (pos < ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 2) {
                buf[pos + 1] = '\0';
              } else {
                buf[ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 1] = '\0';
              }
            }
          }
          if (NULL != pre_buf) {
            get_allocator().free(pre_buf);
            pre_buf = NULL;
          }
        } else {
          pos = child_plan->to_string(buf, ObLogValues::MAX_EXPLAIN_BUFFER_SIZE,
              explain_stmt->get_explain_type(),
              explain_stmt->get_display_opt());
          if (pos < ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 2) {
            buf[pos + 1] = '\0';
          } else {
            buf[ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 1] = '\0';
          }
        }

        if (OB_SUCC(ret)) {
          //For explain stmt, we can do pack at the stage of expr alloc,
          //But we need to use the FALSE flag to tell the driver 
          //to use normal encoding instead of memcopy
          optimizer_context_.set_packed(false);
          ObObj obj;
          obj.set_varchar(ObString::make_string(buf));
          ObNewRow row;
          row.cells_ = &obj;
          row.count_ = 1;
          if (OB_FAIL(values_op->add_row(row))) {
            LOG_WARN("failed to add row", K(ret));
          } else {
            // set values_op operator id && set max operator id for LogPlan
            values_op->set_op_id(0);
            set_max_op_id(1);
          }
        }
        if (NULL != buf) {
          get_allocator().free(buf);
          buf = NULL;
        }
      }
    }
    get_optimizer_context().get_all_exprs().reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(allocate_output_expr_for_values_op(*values_op))) {
      LOG_WARN("failed ot allocate output expr", K(ret));
    } else {
      get_optimizer_context().set_plan_type(ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            false);
    }
  }
  return ret;
}
