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
#include "sql/engine/dml/ob_err_log_op.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
OB_SERIALIZE_MEMBER((ObErrLogSpec, ObOpSpec), err_log_ct_def_, type_)

int ObErrLogOp::inner_open()
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObErrLogOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObString err_msg;
  ObString err_op_type;
  bool need_get_next_row = false;
  do {
    need_get_next_row = false;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get_next_row from child ", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
        ObExpr *expr = MY_SPEC.output_[i];
        ObDatum *datum = NULL;
        if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
          if(should_catch_err(ret) && OB_NOT_NULL(datum)) {
            err_log_rt_def_.first_err_ret_ = ret;
            datum->set_null();
            expr->set_evaluated_projected(eval_ctx_);
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to eval expr", K(ret), KPC(expr));
          }
        }
      }

      if (OB_SUCCESS != err_log_rt_def_.first_err_ret_ && OB_SUCC(ret)) {
        if (OB_FAIL(record_err_log())) {
          LOG_WARN("fail to record_err_log", K(ret));
        } else {
          err_log_rt_def_.curr_err_log_record_num_++;
          err_log_rt_def_.reset();
          need_get_next_row = true;
        }
      }
    }
  } while (OB_SUCC(ret) && need_get_next_row);
  return ret;
}

int ObErrLogOp::record_err_log()
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null");
  } else if (OB_FAIL(err_log_service_.insert_err_log_record(session, 
                                                            MY_SPEC.err_log_ct_def_,
                                                            err_log_rt_def_,
                                                            MY_SPEC.type_))) {
    LOG_WARN("fail to insert error logging table", K(ret));
  }

  return ret;
}

int ObErrLogOp::inner_close()
{
  NG_TRACE(insert_close);
  return ObOperator::inner_close();
}
}  // namespace sql
}  // namespace oceanbase
