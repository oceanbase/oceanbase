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
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_parallel_handler.h"
#include "sql/das/ob_data_access_service.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "lib/profile/ob_trace_id.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::lib;
using namespace oceanbase::share;
int64_t ObDASParallelTaskFactory::alloc_count_;
int64_t ObDASParallelTaskFactory::free_count_;

void __attribute__((weak)) request_finish_callback();

int ObDASParallelHandler::init(observer::ObSrvTask *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(task));
  } else {
    task_ = task;
  }
  return ret;
}

int ObDASParallelHandler::deep_copy_all_das_tasks(ObDASTaskFactory &das_factory,
                                                  ObIAllocator &alloc,
                                                  ObIArray<ObIDASTaskOp*> &src_task_list,
                                                  ObIArray<ObIDASTaskOp*> &new_task_list,
                                                  ObDASRemoteInfo &remote_info,
                                                  ObDasAggregatedTask &das_task_wrapper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObDataAccessService *)->collect_das_task_info(src_task_list, remote_info))) {
    LOG_WARN("fail to collect das task info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_task_list.count(); i++) {
      ObIDASTaskOp *das_op = nullptr;
      if (OB_FAIL(deep_copy_das_task(das_factory, src_task_list.at(i), das_op, alloc))) {
        LOG_WARN("fail to deep copy das_op",K(ret), K(src_task_list.at(i)));
      } else if (OB_ISNULL(das_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(new_task_list.push_back(das_op))) {
        LOG_WARN("fail to push back task list", K(ret));
      } else if (OB_FAIL(das_task_wrapper.push_back_task(das_op))) {
        LOG_WARN("fail to push back das_op", K(ret));
      }
    }
  }
  return ret;
}


int ObDASParallelHandler::deep_copy_das_task(ObDASTaskFactory &das_factory,
                                             ObIDASTaskOp *src_op,
                                             ObIDASTaskOp *&dst_op,
                                             ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *das_op = nullptr;
  if (OB_ISNULL(src_op->get_agg_task()) || OB_ISNULL(src_op->get_cur_agg_list())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null agg_task", K(ret), K(src_op->get_agg_task()), K(src_op->get_cur_agg_list()));
  } else if (OB_FAIL(das_factory.create_das_task_op(src_op->get_type(), das_op))) {
    LOG_WARN("fail to create das_op", K(ret));
  } else if (OB_FAIL(das_op->init_task_info(ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE))) {
    LOG_WARN("fail to init das_op info", K(ret));
  } else {
    int64_t ser_pos = 0;
    int64_t des_pos = 0;
    void *ser_ptr = NULL;
    das_op->trans_desc_ = src_op->trans_desc_;
    das_op->snapshot_ = src_op->snapshot_;
    int64_t ser_arg_len = src_op->get_serialize_size();
    if (OB_ISNULL(ser_ptr = alloc.alloc(ser_arg_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail alloc memory", K(ser_arg_len), KP(ser_ptr), K(ret));
    } else if (OB_FAIL(src_op->serialize(static_cast<char *>(ser_ptr), ser_arg_len, ser_pos))) {
      LOG_WARN("fail serialzie init task arg", KP(ser_ptr), K(ser_arg_len), K(ser_pos), K(ret));
    } else if (OB_FAIL(das_op->deserialize(static_cast<const char *>(ser_ptr), ser_pos, des_pos))) {
      LOG_WARN("fail des task arg", KP(ser_ptr), K(ser_pos), K(des_pos), K(ret));
    } else if (ser_pos != des_pos) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("data_len and pos mismatch", K(ser_arg_len), K(ser_pos), K(des_pos), K(ret));
    } else {
      das_op->set_tablet_loc(src_op->get_tablet_loc());
      dst_op = das_op;
    }
  }
  return ret;
}
int ObDASParallelHandler::record_status_and_op_result(ObIDASTaskOp *src_op, ObIDASTaskOp *dst_op)
{
  int ret = OB_SUCCESS;
  // record all affected_row and other info
  if (dst_op->get_task_status() == ObDasTaskStatus::UNSTART) {
    src_op->set_task_status(ObDasTaskStatus::UNSTART);
  } else if (dst_op->get_task_status() == ObDasTaskStatus::FINISHED) {
    src_op->set_task_status(ObDasTaskStatus::FINISHED);
    src_op->errcode_ = OB_SUCCESS;
    if (OB_FAIL(src_op->state_advance())) {
      LOG_WARN("failed to advance das task state.",K(ret));
    } else if (OB_FAIL(src_op->assign_task_result(dst_op))) {
      LOG_WARN("failed to assign das task result.",K(ret));
    }
  } else if (dst_op->get_task_status() == ObDasTaskStatus::FAILED) {
    src_op->set_task_status(ObDasTaskStatus::FAILED);
    src_op->errcode_ = dst_op->errcode_;
    if (OB_FAIL(src_op->state_advance())) {
      LOG_WARN("failed to advance das task state.",K(ret));
    }
  }
  return ret;
}

int ObDASParallelHandler::run()
{
  int ret = OB_SUCCESS;
  // execute all das_tasks
  ObDASParallelTask *task = static_cast<ObDASParallelTask *>(task_);
  common::ObSEArray<ObIDASTaskOp*, 4> new_task_list;
  common::ObSEArray<ObIDASTaskOp*, 4> src_task_list;
  lib::MemoryContext mem_context = nullptr;
  common::ObCurTraceId::set(task->get_trace_id());
  CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, MTL_ID()) {
    int interrupted_code = task->get_das_ref_count_ctx().get_interrupted_err_code();
    if (interrupted_code != OB_SUCCESS) {
      task->get_agg_task()->set_save_ret(interrupted_code);
      LOG_WARN("this task is interrupted,ret_code is", K(interrupted_code));
    } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context,
        lib::ContextParam().set_mem_attr(MTL_ID(), "DASParallelTask")))) {
      LOG_WARN("create memory entity failed", K(ret));
    } else {
      WITH_CONTEXT(mem_context) {
        ObDASRemoteInfo remote_info;
        ObArenaAllocator tmp_alloc;
        ObDASTaskFactory das_factory(mem_context->get_arena_allocator());
        ObDasAggregatedTask das_task_wrapper(tmp_alloc);
        ObDASRemoteInfo::get_remote_info() = &remote_info;
        if (OB_FAIL(task->get_agg_task()->get_aggregated_tasks(src_task_list))) {
          LOG_WARN("fail to get all das tasks", K(ret), KPC(task));
        } else if (OB_FAIL(deep_copy_all_das_tasks(das_factory,
                                            mem_context->get_arena_allocator(),
                                            src_task_list,
                                            new_task_list,
                                            remote_info,
                                            das_task_wrapper))) {
          LOG_WARN("fail to deep copy all das tasks", K(ret));
        } else if (OB_FAIL(MTL(ObDataAccessService *)->parallel_execute_das_task(new_task_list))) {
          LOG_WARN("fail to parallel execute das task", K(ret), KPC(task));
        } else {
          // do nothing
        }

        // close new_task_list and copy all task execute result
        int last_ret = OB_SUCCESS;
        for (int64_t i = 0; i < new_task_list.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = record_status_and_op_result(src_task_list.at(i), new_task_list.at(i)))) {
            LOG_WARN("fail to record task status and result", K(tmp_ret));
          }
          last_ret = OB_SUCCESS == last_ret ? tmp_ret : last_ret;
          if (OB_SUCCESS != (tmp_ret = new_task_list.at(i)->end_das_task())) {
            LOG_WARN("end das task failed", K(ret), K(tmp_ret), KPC(new_task_list.at(i)));
          }
          last_ret = OB_SUCCESS == last_ret ? tmp_ret : last_ret;
        }
        ret = OB_SUCCESS == ret ? last_ret : ret;
      } // end for mem_context
    }
  }
  if (nullptr != mem_context) {
    DESTROY_CONTEXT(mem_context);
    mem_context = NULL;
  }

  task->get_agg_task()->set_save_ret(ret);
  if (ret != OB_SUCCESS) {
    task->get_das_ref_count_ctx().interrupt_other_workers(ret);
  }
  // whether success or failure,we must dec the reference_count
  task->get_das_ref_count_ctx().inc_concurrency_limit_with_signal();
  request_finish_callback();
  // cover the error code
  ret = OB_SUCCESS;
  return ret;
}
int ObDASParallelTask::init(ObDasAggregatedTask *agg_task, int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (NULL == agg_task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null, unexpected error", K(ret));
  } else if (OB_FAIL(handler_.init(this))) {
    LOG_WARN("init handler failed", K(ret));
  } else {
    set_group_id(group_id);
    agg_task_ = agg_task;
    trace_id_.set(*ObCurTraceId::get_trace_id());
    set_type(ObRequest::OB_DAS_PARALLEL_TASK);
  }
  return ret;
}
ObDASParallelTask *ObDASParallelTaskFactory::alloc(DASRefCountContext &ref_count_ctx)
{
  ObDASParallelTask *task = NULL;
  if (NULL != (task = op_alloc_args(ObDASParallelTask, ref_count_ctx))) {
    (void)ATOMIC_FAA(&alloc_count_, 1);
    alloc_count_++;
    if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
      LOG_INFO("ts response task statistics", K_(alloc_count), K_(free_count));
    }
  }
  return task;
}
void ObDASParallelTaskFactory::free(ObDASParallelTask *task)
{
  if (NULL != task) {
    op_reclaim_free(task);
    task = NULL;
    (void)ATOMIC_FAA(&free_count_, 1);
  }
}
