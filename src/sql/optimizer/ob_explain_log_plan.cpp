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
  ObLogPlan* child_plan = NULL;
  ObDMLStmt* child_stmt = NULL;

  ObLogValues* values = NULL;
  if (NULL == get_stmt() || !get_stmt()->is_explain_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt for explain", "stmt", get_stmt());
  } else {
    const ObExplainStmt* explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
    if (OB_ISNULL(child_stmt = explain_stmt->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no child stmt for explain");
    } else if (OB_ISNULL(
                   child_plan = optimizer_context_.get_log_plan_factory().create(optimizer_context_, *child_stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create log plan for explain stmt");
    } else if (OB_FAIL(child_plan->init_plan_info())) {
      LOG_WARN("failed to init equal_sets");
    } else if (OB_FAIL(child_plan->generate_plan())) {
      LOG_WARN("failed to generate plan tree for explain", K(ret));
    } else {
      char* buf = NULL;
      values = static_cast<ObLogValues*>(log_op_factory_.allocate(*this, LOG_VALUES));
      if (NULL == values) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate UPDATE operator");
      } else if (NULL == (buf = static_cast<char*>(get_allocator().alloc(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Failed to allocate buffer",
            "buffer size",
            static_cast<int64_t>(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE),
            K(ret));
      } else {
        // NO MORE FAILURE FROM NOW ON!!!
        int64_t pos = 0;
        values->set_explain_plan(child_plan);

        if (EXPLAIN_JSON == explain_stmt->get_explain_type()) {
          char* pre_buf = NULL;
          if (NULL == (pre_buf = static_cast<char*>(get_allocator().alloc(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("Failed to allocate buffer",
                "buffer size",
                static_cast<int64_t>(ObLogValues::MAX_EXPLAIN_BUFFER_SIZE),
                K(ret));
          } else if (NULL == child_plan->get_plan_root()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid argument", K(ret));
          } else {
            Value* child_value = NULL;
            if (OB_FAIL(child_plan->get_plan_root()->to_json(
                    pre_buf, ObLogValues::MAX_EXPLAIN_BUFFER_SIZE, pos, child_value))) {
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
          pos = child_plan->to_string(buf, ObLogValues::MAX_EXPLAIN_BUFFER_SIZE, explain_stmt->get_explain_type());
          if (pos < ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 2) {
            buf[pos + 1] = '\0';
          } else {
            buf[ObLogValues::MAX_EXPLAIN_BUFFER_SIZE - 1] = '\0';
          }
        }
        ObObj obj;
        obj.set_varchar(ObString::make_string(buf));
        ObNewRow row;
        row.cells_ = &obj;
        row.count_ = 1;
        values->add_row(row);
        set_phy_plan_type(OB_PHY_PLAN_LOCAL);
        set_plan_root(values);
        values->mark_is_plan_root();
        // set values operator id && set max operator id for LogPlan
        values->set_op_id(0);
        set_max_op_id(1);
        if (NULL != buf) {
          get_allocator().free(buf);
          buf = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(optimizer_context_.get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (optimizer_context_.get_session_info()->use_static_typing_engine()) {
      // Static typing engine need expr to output rows. We generate a const output expr
      // for values operator.
      ObConstRawExpr* output = NULL;
      if (OB_FAIL(optimizer_context_.get_expr_factory().create_raw_expr(T_VARCHAR, output))) {
        LOG_WARN("create const expr failed", K(ret));
      } else {
        ObObj v;
        v.set_varchar(" ");
        v.set_collation_type(ObCharset::get_system_collation());
        output->set_param(v);
        output->set_value(v);
        if (OB_FAIL(output->formalize(optimizer_context_.get_session_info()))) {
          LOG_WARN("const expr formalize failed", K(ret));
        } else if (OB_FAIL(values->add_expr_to_output(output))) {
          LOG_WARN("add output expr failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(plan_traverse_loop(OPERATOR_NUMBERING, ALLOC_EXPR))) {
          LOG_WARN("failed to do plan traverse", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObExplainLogPlan::generate_plan()
{
  return generate_raw_plan();
}
