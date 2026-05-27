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
 *
 * Binlog Record Sorter Module. Sort binlog record for user
 * Module Input: TransCtx
 */

#define USING_LOG_PREFIX OBLOG_SORTER


#include "ob_log_trans_msg_sorter.h"
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX, TCONF
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_cdc_update_split_merge_cache.h"

#define RETRY_FUNC_ON_ERROR_WITH_USLEEP(err_no, var, func, args...) \
  do {\
    if (OB_SUCC(ret)) \
    { \
      ret = (err_no); \
      uint64_t retry_cnt = 0; \
      while ((err_no) == ret && ! (common::ObSimpleThreadPool::has_set_stop())) \
      { \
        ret = OB_SUCCESS; \
        ret = (var).func(args); \
        if (err_no == ret) { \
          retry_cnt ++; \
          if (0 == retry_cnt % 12000) { \
            LOG_WARN(#func " retry for too many times", K(retry_cnt), KP(&var), K(var)); \
          } \
          /* sleep 5 ms*/ \
          ob_usleep(5 * 1000); \
        }\
      } \
      if ((common::ObSimpleThreadPool::has_set_stop())) \
      { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

namespace oceanbase
{
namespace libobcdc
{

ObLogTransMsgSorter::ObLogTransMsgSorter() :
  is_inited_(false),
  thread_num_(0),
  task_limit_(0),
  total_task_count_(0),
  trans_stat_mgr_(NULL),
  enable_sort_by_seq_no_(false),
  err_handler_(NULL)
{
  br_sort_func_ = NULL;
}

ObLogTransMsgSorter::~ObLogTransMsgSorter()
{
  destroy();
}

int ObLogTransMsgSorter::init(
    const bool enable_sort_by_seq_no,
    const int64_t thread_num,
    const int64_t task_limit,
    IObLogTransStatMgr &trans_stat_mgr,
    IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    LOG_ERROR("br sorter has been initialized", K_(is_inited));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
          || OB_UNLIKELY(task_limit <= 0)
          || OB_ISNULL(err_handler)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(task_limit), KP(err_handler));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_stat_mgr_ = &trans_stat_mgr;
    enable_sort_by_seq_no_ = enable_sort_by_seq_no;
    thread_num_ = thread_num;
    task_limit_ = task_limit;
    err_handler_ = err_handler;
    if (enable_sort_by_seq_no_) {
      br_sort_func_ = &ObLogTransMsgSorter::sort_br_by_seq_no_;
    } else {
      br_sort_func_ = &ObLogTransMsgSorter::sort_br_by_part_order_;
    }
    is_inited_ = true;
  }

  LOG_INFO("start sorter succ", K(enable_sort_by_seq_no), K(thread_num), K(task_limit));

  return ret;
}

void ObLogTransMsgSorter::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    TransMsgSorterThread::wait();
    TransMsgSorterThread::destroy();
    thread_num_ = 0;
    task_limit_ = 0;
    total_task_count_ = 0;
    trans_stat_mgr_ = NULL;
    err_handler_ = NULL;
    br_sort_func_ = NULL;
    LOG_INFO("TransMsgSorter destroy succ");
  }
}

int ObLogTransMsgSorter::start()
{
  int ret = OB_SUCCESS;

  LOG_INFO("begin start TransMsgSorter");

  if (OB_FAIL(TransMsgSorterThread::init(thread_num_, task_limit_, "obcdc-br-sorter"))) {
    LOG_ERROR("failed to init sorter thread pool", KR(ret), K_(thread_num), K_(task_limit));
  } else {
    LOG_INFO("start TransMsgSorter succ", K_(enable_sort_by_seq_no), K_(thread_num), K_(task_limit));
  }

  return ret;
}

void ObLogTransMsgSorter::stop()
{
  mark_stop_flag();
}

int ObLogTransMsgSorter::submit(TransCtx *trans)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TransMsgSorter not init", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("TransMsgSorter is stopped!", KR(ret), K_(is_inited));
  } else if (OB_FAIL(TransMsgSorterThread::push(trans))) {
    if (OB_EAGAIN == ret) {
      ret = OB_NEED_RETRY;
    }
  } else {
    ATOMIC_INC(&total_task_count_);
  }

  return ret;
}

int ObLogTransMsgSorter::get_task_count(int64_t &task_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    task_count = ATOMIC_LOAD(&total_task_count_);
  }

  return ret;
}

void ObLogTransMsgSorter::mark_stop_flag()
{
  if (! has_set_stop()) {
    TransMsgSorterThread::stop();
    LOG_INFO("mark TransMsgSorter stop succ");
  }
}

void ObLogTransMsgSorter::handle(void *data)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  TransCtx *trans = NULL;

  if (OB_UNLIKELY(IS_NOT_INIT)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TransMsgSorter not init", KR(ret));
  } else if (OB_UNLIKELY(has_set_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("TransMsgSorter is stopped!", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(trans = static_cast<TransCtx*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task to be handled should not null!", KR(ret));
  } else {
    trans->set_trans_sorting();
    if (OB_FAIL(((*this).*br_sort_func_)(*trans))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("failed to sort trans br", KR(ret), K_(enable_sort_by_seq_no), KPC(trans));
      }
    } else {
      LOG_DEBUG("br sorter handle trans done", K_(enable_sort_by_seq_no), KPC(trans));
      ATOMIC_DEC(&total_task_count_);
      trans_stat_mgr_->do_sort_trans_stat();
      /* trans status change to TRANS_BR_SORTED */
      trans->set_trans_sorted();
      // no more access to trans cause it may be freed by anytime.
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "Sorter thread exits, err=%d", ret);
  }
}

int ObLogTransMsgSorter::sort_br_by_part_order_(TransCtx &trans)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("br sorter handle trans begin", K(trans));
  PartTransTask *part_trans_task = trans.get_participant_objs();

  if (OB_ISNULL(part_trans_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("part_trans_task of trans_ctx is null!", KR(ret), K(trans));
  } else {
    while (OB_SUCC(ret) && ! has_set_stop() && OB_NOT_NULL(part_trans_task)) {
      if (OB_FAIL(handle_partition_br_(trans, *part_trans_task))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("failed to output br of part trans", KR(ret), KPC(part_trans_task), K(trans));
        }
      } else {
        part_trans_task = part_trans_task->next_task();
      }
    }

    if (has_set_stop()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int ObLogTransMsgSorter::handle_partition_br_with_merge_(
    TransCtx &trans,
    PartTransTask &part_trans_task,
    ObCDCUpdateSplitMergeCache &merge_cache,
    const bool enable_merge)
{
  int ret = OB_SUCCESS;
  DmlStmtTask *dml_stmt_task = NULL;

  while (OB_SUCC(ret) && ! has_set_stop()) {
    if (OB_FAIL(next_stmt_contains_valid_br_(part_trans_task, dml_stmt_task))) {
      if (OB_ITER_END != ret && OB_IN_STOP_STATE != ret) {
        LOG_ERROR("failed to get next valid br of part trans", KR(ret), K(part_trans_task), K(trans));
      }
    } else {
      ObLogBR *output_br = nullptr;
      LOG_TRACE("[MERGE] sorter candidate stmt",
          "trans_id", part_trans_task.get_trans_id(),
          "commit_version", part_trans_task.get_trans_commit_version(),
          "table_id", dml_stmt_task->get_table_id(),
          "seq_no", dml_stmt_task->get_row_seq_no(),
          "dml_flag", dml_stmt_task->get_dml_flag(),
          "trace_id", dml_stmt_task->get_update_split_trace_id(),
          "has_trace_id", dml_stmt_task->has_update_split_trace_id(),
          "enable_merge", enable_merge,
          KP(dml_stmt_task));

      if (!enable_merge || !dml_stmt_task->has_update_split_trace_id()) {
        output_br = dml_stmt_task->get_binlog_record();
      } else if (OB_FAIL(try_merge_update_split_(merge_cache, dml_stmt_task, output_br))) {
        LOG_ERROR("try_merge_update_split_ failed", KR(ret), KPC(dml_stmt_task));
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(output_br)) {
        if (OB_FAIL(trans.append_sorted_br(output_br))) {
          LOG_ERROR("failed to append trans br", KR(ret), KPC(dml_stmt_task), K(trans));
        } else {
          feedback_br_output_info_(part_trans_task);
        }
      }
    }

    dml_stmt_task = NULL;
  }

  if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  LOG_DEBUG("handle_partition_br_with_merge_ end", KR(ret), K(trans), K(part_trans_task));

  return ret;
}

int ObLogTransMsgSorter::sort_br_by_seq_no_(TransCtx &trans)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("br sorter handle trans begin", K(trans));
  DmlStmtTask *dml_stmt_task = NULL;
  // 1. build a min-top heap
  std::priority_queue<DmlStmtTask*, std::vector<DmlStmtTask*>, StmtSequerenceCompFunc> heap;
  PartTransTask *part_trans_task = trans.get_participant_objs();
  // 2. get a dml_stmt contains a valid br from each part_trans and put into the heap

  while (OB_SUCC(ret) && ! has_set_stop() && OB_NOT_NULL(part_trans_task)) {
    if (OB_FAIL(next_stmt_contains_valid_br_(*part_trans_task, dml_stmt_task))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("failed to get next valid br of part trans", KR(ret), KPC(part_trans_task), K(trans));
      }
    } else {
      heap.push(dml_stmt_task);
    }
    if (OB_SUCC(ret)) {
      part_trans_task = part_trans_task->next_task();
    }
    dml_stmt_task = NULL;
  }

  // 2.5. init merge cache if enabled
  const bool enable_merge = (1 == TCTX.enable_update_split_merge_);
  ObCDCUpdateSplitMergeCache merge_cache;
  if (OB_SUCC(ret) && enable_merge) {
    if (OB_FAIL(merge_cache.init(
        trans.get_trans_id(),
        TCTX.update_split_merge_storager_,
        *TCTX.resource_collector_,
        TCTX.trans_stat_mgr_->get_update_split_merge_stat()))) {
      LOG_ERROR("merge_cache init failed", KR(ret));
    }
  }

  // 3. pop br from heap and push a new br into the heap
  while(OB_SUCC(ret) && ! has_set_stop() && !heap.empty()) {
    // 3.1. get the min br from heap top and then pop the br
    dml_stmt_task = heap.top();
    PartTransTask &cur_part_trans = dml_stmt_task->get_host();
    DmlStmtTask *next_dml_stmt = NULL;
    heap.pop();

    // 3.2. determine output br: merge or direct output
    ObLogBR *output_br = nullptr;
    LOG_TRACE("[MERGE] sorter candidate stmt",
        "trans_id", cur_part_trans.get_trans_id(),
        "commit_version", cur_part_trans.get_trans_commit_version(),
        "table_id", dml_stmt_task->get_table_id(),
        "seq_no", dml_stmt_task->get_row_seq_no(),
        "dml_flag", dml_stmt_task->get_dml_flag(),
        "trace_id", dml_stmt_task->get_update_split_trace_id(),
        "has_trace_id", dml_stmt_task->has_update_split_trace_id(),
        "enable_merge", enable_merge,
        KP(dml_stmt_task));
    if (!enable_merge || !dml_stmt_task->has_update_split_trace_id()) {
      output_br = dml_stmt_task->get_binlog_record();
    } else if (OB_FAIL(try_merge_update_split_(merge_cache, dml_stmt_task, output_br))) {
      LOG_ERROR("try_merge_update_split_ failed", KR(ret), KPC(dml_stmt_task));
    }

    // 3.3. output br to TransCtx if we have one
    if (OB_SUCC(ret) && OB_NOT_NULL(output_br)) {
      if (OB_FAIL(trans.append_sorted_br(output_br))) {
        LOG_ERROR("failed to append trans br", KR(ret), K(cur_part_trans), K_(enable_sort_by_seq_no), K(trans));
      } else {
        feedback_br_output_info_(cur_part_trans);
      }
    }

    // 3.4. push next stmt from the same partition into heap
    if (OB_SUCC(ret)) {
      if (heap.empty()) {
        // 3.4.1. if heap is empty, continue handling remaining stmt of the current
        // partition with the same merge logic as heap path. Keep part-order path unchanged.
        if (OB_FAIL(handle_partition_br_with_merge_(trans, cur_part_trans, merge_cache, enable_merge))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("failed to handle the last partition br", KR(ret), K_(enable_sort_by_seq_no), K(cur_part_trans), K(trans));
          }
        }
      } else if (OB_FAIL(next_stmt_contains_valid_br_(cur_part_trans, next_dml_stmt))) {
        // 3.4.2 if heap not empty, get next valid br in current part_trans_task and push into heap
          if (OB_ITER_END != ret && OB_IN_STOP_STATE != ret) {
            LOG_ERROR("failed to get next stmt with valid br", KR(ret), K(cur_part_trans));
          } else {
            // 3.4.3. if all br in current partition has put into heap, skip current partition and go on with sort
            LOG_DEBUG("current partition all br has output, go on with other partitions", K(cur_part_trans));
            ret = OB_SUCCESS;
          }
      } else {
        heap.push(next_dml_stmt);
      }
    }
  }

  // 4. handle remaining unmatched split stmts
  if (OB_SUCC(ret) && enable_merge) {
    if (OB_FAIL(merge_cache.handle_unmatched())) {
      LOG_ERROR("handle_unmatched failed", KR(ret));
    }
    merge_cache.destroy();
  }

  if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTransMsgSorter::handle_partition_br_(TransCtx &trans, PartTransTask &part_trans_task)
{
  int ret = OB_SUCCESS;
  DmlStmtTask *dml_stmt_task = NULL;

  while (OB_SUCC(ret) && ! has_set_stop()) {
    if (OB_FAIL(next_stmt_contains_valid_br_(part_trans_task, dml_stmt_task))) {
      if (OB_ITER_END != ret && OB_IN_STOP_STATE != ret) {
        LOG_ERROR("failed to get next valid br of part trans", KR(ret), K(part_trans_task), K(trans));
      }
    } else if (OB_FAIL(trans.append_sorted_br(dml_stmt_task->get_binlog_record()))) {
      // append_sorted_br should be implement by TransCtx
      LOG_ERROR("failed to append trans br", KR(ret), KPC(dml_stmt_task), K(trans));
    } else {
      /* succ handle partition br */
      feedback_br_output_info_(part_trans_task);
    }

    dml_stmt_task = NULL;
  }

  if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  LOG_DEBUG("handle_partition_br_ end", KR(ret), K(trans), K(part_trans_task));

  return ret;
}

int ObLogTransMsgSorter::next_stmt_contains_valid_br_(PartTransTask &part_trans_task, DmlStmtTask *&dml_stmt_task)
{
  int ret = OB_SUCCESS;

  RETRY_FUNC_ON_ERROR_WITH_USLEEP(OB_NEED_RETRY, part_trans_task, next_dml_stmt, dml_stmt_task);

  if (OB_ITER_END != ret) {
    if (has_set_stop()) {
      ret = OB_IN_STOP_STATE;
    } else if (OB_ISNULL(dml_stmt_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("dml_stmt_task for sorter should not be null", KR(ret));
    } else {
      ObLogBR *br = dml_stmt_task->get_binlog_record();
      if (OB_ISNULL(br)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("br for sorter should not be null",KR(ret), KPC(dml_stmt_task));
      } else if (!br->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sorter expect valid br", KR(ret), KPC(br));
      } else {
        LOG_DEBUG("sorter push br", KPC(br), KP(br), KP(br->get_host()), KP(br->get_data()));
        /* succ to get stmt with valid br */
      }
    }
  }

  LOG_DEBUG("next_stmt_contains_valid_br_ end", KR(ret), K(part_trans_task), KPC(dml_stmt_task));

  return ret;
}

void ObLogTransMsgSorter::feedback_br_output_info_(PartTransTask &part_trans_task)
{
  part_trans_task.inc_sorted_br();
  trans_stat_mgr_->do_sort_br_stat();
}

int ObLogTransMsgSorter::try_merge_update_split_(
    ObCDCUpdateSplitMergeCache &merge_cache,
    DmlStmtTask *dml_stmt,
    ObLogBR *&output_br)
{
  int ret = OB_SUCCESS;
  output_br = nullptr;
  const int64_t trace_id = dml_stmt->get_update_split_trace_id();

  if (dml_stmt->is_delete()) {
    if (OB_FAIL(merge_cache.put(trace_id, dml_stmt))) {
      LOG_ERROR("merge_cache put failed", KR(ret), K(trace_id));
    }
  } else if (dml_stmt->is_insert()) {
    if (OB_FAIL(merge_cache.get_and_merge(trace_id, dml_stmt, output_br))) {
      LOG_ERROR("merge_cache get_and_merge failed", KR(ret), K(trace_id));
    }
  } else {
    output_br = dml_stmt->get_binlog_record();
  }

  return ret;
}

}
}
