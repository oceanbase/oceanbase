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

#include "sql/executor/ob_remote_identity_task_spliter.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_job.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

ObRemoteIdentityTaskSpliter::ObRemoteIdentityTaskSpliter()
  : ObTaskSpliter(),
    task_(NULL)
{
}

ObRemoteIdentityTaskSpliter::~ObRemoteIdentityTaskSpliter()
{
  if (OB_LIKELY(NULL != task_)) {
    task_->~ObTaskInfo();
    task_ = NULL;
  }
}

int ObRemoteIdentityTaskSpliter::get_next_task(ObTaskInfo *&task)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_I(t1) (OB_ISNULL(plan_ctx_) ||
             OB_ISNULL(exec_ctx_) ||
             OB_ISNULL(allocator_) ||
             OB_ISNULL(job_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx or executor ctx or allocator or job or job conf is NULL", K(ret),
             K(plan_ctx_), K(exec_ctx_), K(allocator_), K(job_));
  } else if (NULL != task_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(job_->get_root_spec())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret));
  } else {
    ObDASTableLoc *first_table_loc = DAS_CTX(*exec_ctx_).get_table_loc_list().get_first();
    // t1 union t1这种情况， t1(p0) union t2(p0)这种情况，等等，
    // 都是remote模式，但table_loc_list的count可能大于1
    // 优化器必须保证：remote模式下，所有表的location都是一致的，并且都是单分区。
    ObDASTabletLoc *first_tablet_loc = first_table_loc->get_first_tablet_loc();
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObTaskInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc ObTaskInfo", K(ret));
    } else {
      task_ = new(ptr) ObTaskInfo(*allocator_);
      ObTaskID ob_task_id;
      ObTaskLocation task_loc;
      ob_task_id.set_ob_job_id(job_->get_ob_job_id());
      ob_task_id.set_task_id(0);
      task_loc.set_ob_task_id(ob_task_id);
      task_loc.set_server(first_tablet_loc->server_);
      task_->set_task_split_type(get_type());
      task_->set_pull_slice_id(0);
      task_->set_location_idx(0);
      task_->set_task_location(task_loc);
      task_->set_state(OB_TASK_STATE_NOT_INIT);
      task_->set_root_spec(job_->get_root_spec()); // for static engine
      if (OB_FAIL(task_->init_location_idx_array(1))) {
        LOG_WARN("init location idx array failed", K(ret));
      } else if (OB_FAIL(task_->add_location_idx(0))) {
        LOG_WARN("add location index to task failed", K(ret));
      } else {
        // 将task_作为类成员的目的是为了保证第二次调用get_next_task能返回OB_ITER_END
        task = task_;
      }
    }
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */




