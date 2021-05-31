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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_interm_task_spliter.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_transmit.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObIntermTaskSpliter::ObIntermTaskSpliter()
    : prepare_done_flag_(false), next_task_idx_(0), total_task_count_(0), store_()
{}

ObIntermTaskSpliter::~ObIntermTaskSpliter()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObTaskInfo* t = store_.at(i);
    if (OB_LIKELY(NULL != t)) {
      t->~ObTaskInfo();
    }
  }
}

int ObIntermTaskSpliter::prepare()
{
  int ret = OB_SUCCESS;
  ObPhyOperator* phy_op = NULL;
  ObTransmit* transmit_op = NULL;
  prepare_done_flag_ = false;
  if (OB_I(t1)(OB_ISNULL(plan_ctx_) || OB_ISNULL(allocator_) || OB_ISNULL(job_) || OB_ISNULL(job_conf_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("param not init", K_(plan_ctx), K_(allocator), K_(job), K_(job_conf));
  } else if (OB_I(t2)(OB_UNLIKELY(NULL == (phy_op = job_->get_root_op())) || (!IS_TRANSMIT(phy_op->get_type())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is null or not transmit", K(phy_op));
  } else {
    transmit_op = static_cast<ObTransmit*>(phy_op);
    next_task_idx_ = 0;
    total_task_count_ = transmit_op->get_split_task_count();
    // mark as done
    prepare_done_flag_ = true;
  }
  return ret;
}

int ObIntermTaskSpliter::get_next_task(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_UNLIKELY(false == prepare_done_flag_)) {
    ret = prepare();
  }
  // after success prepare
  if (OB_SUCC(ret)) {
    if (next_task_idx_ >= total_task_count_) {
      ret = OB_ITER_END;
    } else {
      if (OB_I(t1)(OB_UNLIKELY(NULL == (ptr = allocator_->alloc(sizeof(ObTaskInfo)))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail allocate task");
      } else {
        ObTaskInfo* t = new (ptr) ObTaskInfo(*allocator_);
        if (OB_FAIL(store_.push_back(t))) {
          LOG_WARN("fail to push taskinfo into store", K(ret));
        } else {
          ObTaskID ob_task_id;
          ObTaskLocation task_loc;
          ob_task_id.set_ob_job_id(job_->get_ob_job_id());
          ob_task_id.set_task_id(next_task_idx_);
          task_loc.set_ob_task_id(ob_task_id);
          task_loc.set_server(server_);  // ObTaskControl will rewrite server later.
          t->set_task_split_type(get_type());
          t->set_pull_slice_id(next_task_idx_);
          t->set_task_location(task_loc);
          t->set_root_op(job_->get_root_op());
          t->set_state(OB_TASK_STATE_NOT_INIT);
          task = t;
          // move to next info
          next_task_idx_++;
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
