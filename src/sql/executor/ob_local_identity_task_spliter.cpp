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

#include "sql/executor/ob_local_identity_task_spliter.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_job.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObLocalIdentityTaskSpliter::ObLocalIdentityTaskSpliter()
  : task_(NULL)
{
}

ObLocalIdentityTaskSpliter::~ObLocalIdentityTaskSpliter()
{
  if (NULL != task_) {
    task_->~ObTaskInfo();
    task_ = NULL;
  }
}

int ObLocalIdentityTaskSpliter::get_next_task(ObTaskInfo *&task)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_UNLIKELY(OB_ISNULL(allocator_) || OB_ISNULL(job_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(allocator_), K(job_));
  } else if (NULL != task_) {
    ret = OB_ITER_END;
  } else {
    void *ptr = allocator_->alloc(sizeof(ObTaskInfo));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail allocate ObTaskInfo", K(ret));
    } else {
      task_ = new(ptr) ObTaskInfo(*allocator_);
      ObTaskID ob_task_id;
      ObTaskLocation task_loc;
      ob_task_id.set_ob_job_id(job_->get_ob_job_id());
      ob_task_id.set_task_id(0);
      task_loc.set_ob_task_id(ob_task_id);
      task_loc.set_server(server_);
      task_->set_task_split_type(get_type());
      task_->set_location_idx(0);
      task_->set_pull_slice_id(0);
      task_->set_task_location(task_loc);
      task_->set_state(OB_TASK_STATE_NOT_INIT);
      task_->set_root_spec(job_->get_root_spec());
      // 将task_作为类成员的目的是为了保证第二次调用get_next_task能返回OB_ITER_END
      task = task_;
    }
  }
  return ret;
}


