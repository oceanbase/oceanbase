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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetcher_dispatcher.h"      // ObLogFetcherDispatcher

#include "lib/oblog/ob_log_module.h"        // LOG_ERROR
#include "lib/atomic/ob_atomic.h"           // ATOMIC_FAA
#include "lib/utility/ob_macro_utils.h"     // RETRY_FUNC

#include "ob_log_dml_parser.h"              // IObLogDmlParser
#include "ob_log_ddl_handler.h"             // IObLogDDLHandler
#include "ob_log_sequencer1.h"              // IObLogSequencer
#include "ob_log_committer.h"               // IObLogCommitter
#include "ob_log_part_trans_task.h"         // PartTransTask
#include "ob_log_instance.h"                // TCTX

using namespace oceanbase::common;
namespace oceanbase
{
namespace liboblog
{

ObLogFetcherDispatcher::ObLogFetcherDispatcher() :
    inited_(false),
    ddl_handler_(NULL),
    committer_(NULL),
    checkpoint_seq_(0)
{
}

ObLogFetcherDispatcher::~ObLogFetcherDispatcher()
{
  destroy();
}

int ObLogFetcherDispatcher::init(IObLogDDLHandler *ddl_handler,
    IObLogCommitter *committer,
    const int64_t start_seq)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ddl_handler_ = ddl_handler)
      || OB_ISNULL(committer_ = committer)
      || OB_UNLIKELY(start_seq < 0)) {
    LOG_ERROR("invalid argument", K(ddl_handler), K(committer), K(start_seq));
    ret = OB_INVALID_ARGUMENT;
  } else {
    checkpoint_seq_ = start_seq;
    inited_ = true;
  }

  return ret;
}

void ObLogFetcherDispatcher::destroy()
{
  inited_ = false;
  ddl_handler_ = NULL;
  committer_ = NULL;
  checkpoint_seq_ = 0;
}

int ObLogFetcherDispatcher::dispatch(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    // All tasks are uniformly assigned checkpoint seq
    task.set_checkpoint_seq(ATOMIC_FAA(&checkpoint_seq_, 1));

    LOG_DEBUG("[STAT] [PART_TRANS] [FETCHER_DISPATCHER]", K(task), "checkpoint_seq", task.get_checkpoint_seq());

    switch (task.get_type()) {
      case PartTransTask::TASK_TYPE_DML_TRANS:
        ret = dispatch_dml_trans_task_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_DDL_TRANS:
        ret = dispatch_ddl_trans_task_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_GLOBAL_HEARTBEAT:
        ret = dispatch_global_part_heartbeat_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_PART_HEARTBEAT:
        ret = dispatch_part_heartbeat_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_OFFLINE_PARTITION:
        ret = dispatch_offline_partition_task_(task, stop_flag);
        break;

      default:
        LOG_ERROR("invalid task, unkown type", K(task));
        ret = OB_NOT_SUPPORTED;
        break;
    }

    if (OB_SUCCESS != ret) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("dispatch task fail", KR(ret), K(task));
      }
    }
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_dml_trans_task_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogSequencer *sequencer = TCTX.sequencer_;

  if (OB_ISNULL(sequencer)) {
    LOG_ERROR("sequencer is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sequencer->push(&task, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("sequencer push fail", KR(ret), K(task));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_ddl_trans_task_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ddl_handler_)) {
    LOG_ERROR("invalid ddl handler", K(ddl_handler_));
    ret = OB_INVALID_ERROR;
  } else {
    // DDL transaction push into DDLHandler
    RETRY_FUNC(stop_flag, *ddl_handler_, push, &task, DATA_OP_TIMEOUT);
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_to_committer_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(committer_)) {
    LOG_ERROR("invalid committer", K(committer_));
    ret = OB_INVALID_ERROR;
  } else {
    const int64_t task_count = 1;
    // Push into committer
    RETRY_FUNC(stop_flag, *committer_, push, &task, task_count, DATA_OP_TIMEOUT);
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_part_heartbeat_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  // Heartbeat of the DDL partition is distributed to the DDL processor
  if (task.is_ddl_part_heartbeat()) {
    if (OB_ISNULL(ddl_handler_)) {
      LOG_ERROR("invalid ddl handler", K(ddl_handler_));
      ret = OB_INVALID_ERROR;
    } else {
      // Push into DDL Handler
      RETRY_FUNC(stop_flag, *ddl_handler_, push, &task, DATA_OP_TIMEOUT);
    }
  } else {
    ret = dispatch_to_committer_(task, stop_flag);
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_offline_partition_task_(PartTransTask &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  // DDL partition's offline tasks are distributed to DDL processors
  if (task.is_ddl_offline_task()) {
    if (OB_ISNULL(ddl_handler_)) {
      LOG_ERROR("invalid ddl handler", K(ddl_handler_));
      ret = OB_INVALID_ERROR;
    } else {
      // Push into DDL Handler
      RETRY_FUNC(stop_flag, *ddl_handler_, push, &task, DATA_OP_TIMEOUT);
    }
  } else {
    ret = dispatch_to_committer_(task, stop_flag);
  }

  return ret;
}

int ObLogFetcherDispatcher::dispatch_global_part_heartbeat_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogSequencer *sequencer = TCTX.sequencer_;

  if (OB_ISNULL(sequencer)) {
    LOG_ERROR("sequencer is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t thread_num = sequencer->get_thread_num();
    // 1. Set the reference count to the number of worker threads as a natural barrier, pushing to all worker threads in Sequencer each time
    // 2. Decrement the reference count when each worker thread handle the global heartbeat, and update the Sequencer local safety point when it becomes 0
    task.set_ref_cnt(thread_num);

    // Note: The current rotation strategy and push are single-threaded operations, so this is the correct implementation
    for (int64_t idx = 0; OB_SUCC(ret) && idx < thread_num; ++idx) {
      if (OB_FAIL(sequencer->push(&task, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("sequencer push fail", KR(ret), K(task));
        }
      }
    } // for
  }

  return ret;
}

}
}
