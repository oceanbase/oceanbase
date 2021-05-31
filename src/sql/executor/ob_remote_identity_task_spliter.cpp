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
#include "sql/executor/ob_receive.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/table/ob_table_scan.h"
#include "lib/container/ob_array.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

ObRemoteIdentityTaskSpliter::ObRemoteIdentityTaskSpliter() : ObTaskSpliter(), task_(NULL)
{}

ObRemoteIdentityTaskSpliter::~ObRemoteIdentityTaskSpliter()
{
  if (OB_LIKELY(NULL != task_)) {
    task_->~ObTaskInfo();
    task_ = NULL;
  }
}

int ObRemoteIdentityTaskSpliter::get_next_task(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  void* ptr = NULL;
  if (OB_I(t1)(OB_ISNULL(plan_ctx_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(allocator_) || OB_ISNULL(job_) ||
               OB_ISNULL(job_conf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx or executor ctx or allocator or job or job conf is NULL",
        K(ret),
        K(plan_ctx_),
        K(exec_ctx_),
        K(allocator_),
        K(job_),
        K(job_conf_));
  } else if (NULL != task_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(root_op = job_->get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret));
  } else {
    ObPhyTableLocationIArray& table_locations = exec_ctx_->get_task_exec_ctx().get_table_locations();

    if (OB_UNLIKELY(table_locations.count() < 1)) {  // t1 join t2 may have multiple locations
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get phy table location", K(ret), "expect", 1, "acutal", table_locations.count());
    } else {
      const ObPartitionReplicaLocationIArray& partition_loc_list = table_locations.at(0).get_partition_location_list();
      // the case of t1 union t1, the case of t1(p0) union t2(p0), etc.,
      // Both are in remote mode, but the count of table_loc_list may be greater than 1
      // The optimizer must ensure that in remote mode, the locations of all tables are the same,
      // and they are all single partitions.
      if (OB_UNLIKELY(1 > partition_loc_list.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("there must be at least one table location", K(partition_loc_list.count()));
      } else {
        const ObReplicaLocation& replica_loc = partition_loc_list.at(0).get_replica_location();
        if (!replica_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replica location is invalid", K(ret), K(partition_loc_list.at(0)));
        } else if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObTaskInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc ObTaskInfo", K(ret));
        } else {
          task_ = new (ptr) ObTaskInfo(*allocator_);
          ObTaskID ob_task_id;
          ObTaskLocation task_loc;
          ob_task_id.set_ob_job_id(job_->get_ob_job_id());
          ob_task_id.set_task_id(0);
          task_loc.set_ob_task_id(ob_task_id);
          task_loc.set_server(replica_loc.server_);
          task_->set_task_split_type(get_type());
          task_->set_pull_slice_id(0);
          task_->set_location_idx(0);
          task_->set_task_location(task_loc);
          task_->set_root_op(job_->get_root_op());
          task_->set_state(OB_TASK_STATE_NOT_INIT);
          if (OB_FAIL(task_->init_location_idx_array(1))) {
            LOG_WARN("init location idx array failed", K(ret));
          } else if (OB_FAIL(task_->add_location_idx(0))) {
            LOG_WARN("add location index to task failed", K(ret));
          } else {
            // The purpose of task_ as a class member is
            // to ensure that the second call to get_next_task can return OB_ITER_END
            task = task_;
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
