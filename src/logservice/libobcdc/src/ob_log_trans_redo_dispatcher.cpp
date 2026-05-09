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
 * Redo Read Task Dispatch Module. Dispatch read_redo_task into RedoReader module if redo data is in storage
 * Module Input: TransCtx
 * A Plugin Called by Sequencer
 */

#define USING_LOG_PREFIX OBLOG_DISPATCHER

#include "ob_log_instance.h"
#include "ob_log_trans_redo_dispatcher.h"
#include "ob_cdc_auto_config_mgr.h"
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard

namespace oceanbase
{
namespace libobcdc
{
ObLogTransRedoDispatcher::ObLogTransRedoDispatcher() :
  redo_memory_limit_(0),
  cur_dispatched_redo_memory_(0),
  task_count_(0),
  trans_stat_mgr_(NULL),
  err_handler_(NULL),
  enable_sort_by_seq_no_(false),
  task_queue_(),
  inited_(false)
{
  dispatch_func_ = NULL;
}

ObLogTransRedoDispatcher::~ObLogTransRedoDispatcher()
{
  destroy();
}

int ObLogTransRedoDispatcher::init(const int64_t redo_dispatcher_memory_limit,
                                   const bool enable_sort_by_seq_no,
                                   const int64_t thread_num,
                                   const int64_t queue_size,
                                   IObLogTransStatMgr &trans_stat_mgr,
                                   IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(redo_dispatcher_memory_limit <= 0)
      || OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    LOG_ERROR("invalid arguments", KR(ret), K(redo_dispatcher_memory_limit), K(thread_num), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogTransRedoDispatcher has been initialized");
    ret = OB_INIT_TWICE;
  } else {
    trans_stat_mgr_ = &trans_stat_mgr;
    err_handler_ = &err_handler;
    ATOMIC_SET(&redo_memory_limit_, redo_dispatcher_memory_limit);
    ATOMIC_SET(&cur_dispatched_redo_memory_, 0);
    enable_sort_by_seq_no_ = enable_sort_by_seq_no;

    if (enable_sort_by_seq_no_) {
      dispatch_func_ = &ObLogTransRedoDispatcher::dispatch_by_turn_;
    } else {
      dispatch_func_ = &ObLogTransRedoDispatcher::dispatch_by_partition_order_;
    }

    if (OB_FAIL(task_queue_.init(queue_size))) {
      LOG_ERROR("init task_queue failed", KR(ret), K(queue_size));
    } else if (OB_FAIL(lib::ThreadPool::set_thread_count(thread_num))) {
      LOG_ERROR("set thread count failed", KR(ret), K(thread_num));
    } else if (OB_FAIL(lib::ThreadPool::init())) {
      LOG_ERROR("ThreadPool init failed", KR(ret));
    } else {
      inited_ = true;
      LOG_INFO("init redo dispatcher succ", K(thread_num), K(queue_size), K(redo_dispatcher_memory_limit));
    }
  }

  return ret;
}

int ObLogTransRedoDispatcher::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObLogTransRedoDispatcher has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("start redo dispatcher thread pool failed", KR(ret), "thread_num", get_thread_count());
  } else {
    LOG_INFO("start redo dispatcher threads succ", "thread_num", get_thread_count());
  }

  return ret;
}

void ObLogTransRedoDispatcher::stop()
{
  if (inited_) {
    mark_stop_flag();
    lib::ThreadPool::wait();
    LOG_INFO("stop redo dispatcher threads succ", "thread_num", get_thread_count());
  }
}

void ObLogTransRedoDispatcher::destroy()
{
  if (inited_) {
    lib::ThreadPool::stop();
    lib::ThreadPool::wait();
    lib::ThreadPool::destroy();

    // Drain remaining tasks
    TransCtx *trans_ctx = NULL;
    while (OB_SUCCESS == task_queue_.pop(trans_ctx)) {
      // TransCtx is managed by TransCtxMgr, we don't free it here
    }

    task_queue_.destroy();
    redo_memory_limit_ = 0;
    cur_dispatched_redo_memory_ = 0;
    task_count_ = 0;
    trans_stat_mgr_ = NULL;
    err_handler_ = NULL;
    dispatch_func_ = NULL;
    inited_ = false;
  }
}

void ObLogTransRedoDispatcher::configure(const ObLogConfig &config)
{
  const int64_t redo_mem_limit = CDC_CFG_MGR.get_redo_dispatcher_memory_limit();
  ATOMIC_SET(&redo_memory_limit_, redo_mem_limit);
  LOG_INFO("[CONFIG][REDO_DISPATCHER]", "redo_dispatcher_memory_limit", redo_mem_limit, "to_size", SIZE_TO_STR(redo_mem_limit));

  // Dynamic thread count adjustment
  const int64_t new_thread_num = config.redo_dispatcher_thread_num.get();
  const int64_t cur_thread_num = get_thread_count();
  if (new_thread_num > 0 && new_thread_num != cur_thread_num) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(lib::ThreadPool::set_thread_count(new_thread_num))) {
      LOG_WARN("failed to set thread count dynamically", KR(ret), K(cur_thread_num), K(new_thread_num));
    } else {
      LOG_INFO("[CONFIG][REDO_DISPATCHER] thread_num adjusted", "old_thread_num", cur_thread_num, "new_thread_num", new_thread_num);
    }
  }
}

int ObLogTransRedoDispatcher::dispatch_trans_redo(TransCtx &trans)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObLogTransRedoDispatcher has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(has_set_stop())) {
    ret = OB_IN_STOP_STATE;
  } else {
    // Push TransCtx* to queue for async processing
    if (OB_FAIL(task_queue_.push(&trans))) {
      if (OB_SIZE_OVERFLOW == ret) {
        if (REACH_TIME_INTERVAL(10 * 1000)) {
          LOG_DEBUG("redo dispatcher queue is full, need retry", K(trans), "queue_size", task_queue_.get_curr_total());
        }
        ret = OB_NEED_RETRY;
      } else {
        LOG_ERROR("push trans_ctx to queue failed", KR(ret), K(trans));
      }
    } else {
      // Successfully queued, increment task count atomically
      ATOMIC_INC(&task_count_);
      LOG_DEBUG("dispatch_trans_redo queued", K(trans));
    }
  }

  return ret;
}

int ObLogTransRedoDispatcher::dec_dispatched_redo_memory(const int64_t &log_size)
{
  int ret = OB_SUCCESS;
  int64_t remain_redo_mem_ = ATOMIC_SAF(&cur_dispatched_redo_memory_, log_size);

  if (remain_redo_mem_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to recycle memory in redo_dispatcher", KR(ret));
  } else { /* succ */ }

  return ret;
}

// dispatch redo_read_task by partition order if enable_output_trans_order_by_sql_operation = 0;
int ObLogTransRedoDispatcher::dispatch_by_partition_order_(TransCtx &trans)
{
  int ret = OB_SUCCESS;
  bool enable_monitor = false;
  ObLogTimeMonitor monitor("ObLogRedoDispatcher::dispatch_by_partition_order_", enable_monitor);
  PartTransTask *part_trans_task = trans.get_participant_objs();

  while(OB_SUCC(ret) && !lib::ThreadPool::has_set_stop() && OB_NOT_NULL(part_trans_task)) {
    PartTransTask *next_part_trans_task = part_trans_task->next_task();
    bool is_part_dispatch_finish = false;

    while(OB_SUCC(ret) && !lib::ThreadPool::has_set_stop()) {
      bool has_memory_to_dispatch_redo = false;

      if (OB_FAIL(try_get_and_dispatch_single_redo_(*part_trans_task, has_memory_to_dispatch_redo, is_part_dispatch_finish))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("try_get_and_dispatch_single_redo_ fail", KR(ret), K(has_memory_to_dispatch_redo),
              K(is_part_dispatch_finish), KP(part_trans_task), KPC(part_trans_task));
        }
      } else if (is_part_dispatch_finish) {
        break;
      } else if (!has_memory_to_dispatch_redo) {
        // sleep 5 ms and retry current PartTransTask
        ob_usleep(5 * _MSEC_);
      } else {
        /* dispatch one redo of PartTransTask success */
        monitor.mark_and_get_cost("get_and_dispatch_redo_done", true);
      }
    }

    if (is_part_dispatch_finish) {
      part_trans_task = next_part_trans_task;
      monitor.mark_and_get_cost("part_done", true);
      LOG_DEBUG("dispatch part_trans_task done, try next_part", K(trans));
    }
  }

  if (lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

// dispatch redo_read_task by turn if enable_output_trans_order_by_sql_operation = 1;
int ObLogTransRedoDispatcher::dispatch_by_turn_(TransCtx &trans)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;

  // Create TransDispatchCtx on stack
  TransDispatchCtx dispatch_ctx;
  if (OB_FAIL(dispatch_ctx.init(trans))) {
    LOG_ERROR("failed to set dispatch context for trans", KR(ret), K(trans), K(dispatch_ctx));
  }

  while (OB_SUCC(ret) && !lib::ThreadPool::has_set_stop() && !dispatch_ctx.is_trans_dispatched()) {
    if (OB_FAIL(reblance_part_redo_memory_to_alloc_(trans, dispatch_ctx))) {
      LOG_ERROR("failed to reblance redo dispatcher memory budget", KR(ret), K(trans));
    } else if (OB_FAIL(dispatch_trans_batch_redo_(trans, dispatch_ctx))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("failed to batch dispatch redo", KR(ret), K_(enable_sort_by_seq_no), K(trans));
      }
    } else if (!dispatch_ctx.is_trans_dispatched()) {
      ob_usleep(200); // sleep 200 us
      if (OB_UNLIKELY(++retry_cnt % 50000 == 0)) {
        // print each 5 sec
        // TODO: simply log content
        LOG_WARN("trans dispatch_by_turn for too many times", KR(ret), K(retry_cnt), K(trans), K(dispatch_ctx));
      }
    } else {
    }
  }

  if (lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTransRedoDispatcher::reblance_part_redo_memory_to_alloc_(TransCtx &trans, TransDispatchCtx &dispatch_ctx)
{
  int ret = OB_SUCCESS;

  // only used for sort_br_by_stmt_seq_no
  const int64_t redo_memory_limit = ATOMIC_LOAD(&redo_memory_limit_);
  const int64_t cur_used_mem = ATOMIC_LOAD(&cur_dispatched_redo_memory_);
  const int64_t total_budget = redo_memory_limit - cur_used_mem;

  if (OB_FAIL(dispatch_ctx.reblance_budget(redo_memory_limit, cur_used_mem, trans))) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("failed to get redo dispatch budget", KR(ret), K(redo_memory_limit), K(cur_used_mem), K(total_budget), K(dispatch_ctx));
    }
  }

  return ret;
}

int ObLogTransRedoDispatcher::dispatch_trans_batch_redo_(TransCtx &trans, TransDispatchCtx &dispatch_ctx)
{
  int ret = OB_SUCCESS;
  PartBudgetArray &normal_priority_part_task_arr = dispatch_ctx.get_normal_priority_part_budgets();
  PartBudgetArray &high_priority_part_task_arr = dispatch_ctx.get_high_priority_part_budgets();

  if (OB_FAIL(dispatch_part_redo_with_budget_(trans, dispatch_ctx, normal_priority_part_task_arr))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("failed to handle batch dispatch redo task", KR(ret), K_(enable_sort_by_seq_no), K(trans),
          K(normal_priority_part_task_arr));
    }
  } else if (OB_FAIL(dispatch_part_redo_with_budget_(trans, dispatch_ctx, high_priority_part_task_arr))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("failed to handle batch dispatch redo task", KR(ret), K_(enable_sort_by_seq_no), K(trans),
          K(high_priority_part_task_arr));
    }
  } else { /* disaptch batch succ, wait for next batch */ }

  if (lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTransRedoDispatcher::dispatch_part_redo_with_budget_(
    TransCtx &trans,
    TransDispatchCtx &dispatch_ctx,
    PartBudgetArray &part_task_budget_array)
{
  int ret = OB_SUCCESS;
  bool is_cur_batch_dispatch_finish = (0 == part_task_budget_array.count());
  ObArray<int64_t> dispatched_part_idx_arr;
  dispatched_part_idx_arr.reset();
  LOG_DEBUG("dispatch part redo with budget", K(part_task_budget_array), K(trans));

  while(OB_SUCC(ret) && !lib::ThreadPool::has_set_stop() && !is_cur_batch_dispatch_finish) {
    int budget_usedup_part_cnt = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < part_task_budget_array.count(); i++) {
      bool is_part_dispatch_finish = false; // set true if part budget used up or part redo dispatched finished
      bool has_memory_to_dispatch_redo = false;
      PartTransDispatchBudget &part_trans_dispatch_budget = part_task_budget_array[i];
      PartTransTask *part_trans_task = part_trans_dispatch_budget.part_trans_task_;

      if (OB_ISNULL(part_trans_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected PartTaskBudget", KR(ret), K(part_trans_dispatch_budget));
      } else if (OB_FAIL(try_get_and_dispatch_single_redo_(*part_trans_task, has_memory_to_dispatch_redo, is_part_dispatch_finish,
                            &part_trans_dispatch_budget))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("try_get_and_dispatch_single_redo_ fail", KR(ret), K(has_memory_to_dispatch_redo),
              K(is_part_dispatch_finish), K(part_trans_dispatch_budget));
        }
      } else if (is_part_dispatch_finish) {
        dispatch_ctx.inc_dispatched_part();
        if (OB_FAIL(dispatched_part_idx_arr.push_back(i))) {
          LOG_ERROR("failed to push_back dispatch finished part idx to dispatched_part_idx_arr", KR(ret), K(part_trans_dispatch_budget));
        }
      } else if (!has_memory_to_dispatch_redo) {
        budget_usedup_part_cnt++;
      }
    }

    // clear part that dispatch finished
    for (int64_t i = dispatched_part_idx_arr.count() - 1; OB_SUCC(ret) && i >= 0 ; i--) {
      int64_t &dispatch_finish_idx = dispatched_part_idx_arr[i];
      if (OB_FAIL(part_task_budget_array.remove(dispatch_finish_idx))) {
        LOG_ERROR("remove dispatch finished part_task from part_task_budget_array failed", KR(ret), K(dispatch_ctx), K(trans));
      }
    }

    // all budget used up if budget_used_up_part count equals to part_task_budget_array remain size(kickout part_handle_done budget)
    if (budget_usedup_part_cnt == part_task_budget_array.count()) {
      is_cur_batch_dispatch_finish = true;
    }

    dispatched_part_idx_arr.reset();

  }

  if (lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTransRedoDispatcher::try_get_and_dispatch_single_redo_(
    PartTransTask &part_trans_task,
    bool &has_memory_to_dispatch_redo,
    bool &is_part_dispatch_finish,
    PartTransDispatchBudget *part_budget)
{
  int ret = OB_SUCCESS;

  // 1. check_redo_can_dispatch_: expected OB_SUCCESS, will get can_dispatch=false if dispatcher redo budget used up,
  //     need wait GC release memory and retry. In this case, won't get redo from PartTransTask and won't miss redo
  // 2. get_next_dispatch_redo: will get expected error code OB_EMPTY_RESULT if all redo of current PartTranstask
  //     has been dispatched. In this case, try with another PartTransTask
  // 3. dispatch_redo_: expected OB_SUCCESS, all kind of other error code are unexpected and libobcdc should stop
  if (part_trans_task.is_part_dispatch_finish()) {
    // if trans has no redo log, will mark dispatch finish before check redo_dispatcher_memory_limit
    is_part_dispatch_finish = true;
  } else if (OB_FAIL(check_redo_can_dispatch_(has_memory_to_dispatch_redo, part_budget))) {
    LOG_ERROR("failed to check_redo_can_dispatch", KR(ret), K_(cur_dispatched_redo_memory), K_(redo_memory_limit),
        K(part_trans_task), K(has_memory_to_dispatch_redo), KP(part_budget), KPC(part_budget));
  } else if (has_memory_to_dispatch_redo) {
    DmlRedoLogNode *redo_node = NULL;

    if (OB_FAIL(part_trans_task.next_redo_to_dispatch(redo_node, is_part_dispatch_finish))) {
      if (OB_EMPTY_RESULT != ret) {
        LOG_ERROR("failed to get next dispatch redo from PartTransTask", KR(ret), K(part_trans_task));
      } else {
        is_part_dispatch_finish = true;
        ret = OB_SUCCESS;
      }
    } else {
      const int64_t redo_node_len = redo_node->get_data_len();

      // redo is not null, whcih is checked by PartTransTask::next_redo_to_dispatch
      // NOTICE: NO MORE ACCESS TO redo_node after dispatch_redo_
      if (OB_FAIL(dispatch_redo_(part_trans_task, *redo_node))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("failed to dispatch redo", KR(ret), KPC(redo_node));
        }
      } else {
        // dispatch one redo success
        if (enable_sort_by_seq_no_) {
          if (OB_ISNULL(part_budget)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("part_budget should not be null after try_get_and_dispatch redo_node succ", KR(ret), K_(enable_sort_by_seq_no), K(part_trans_task), KP(part_budget));
          } else {
            part_budget->inc_dispatched_size(redo_node_len);
          }
        }
      }
    }
  }

  if (lib::ThreadPool::has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTransRedoDispatcher::check_redo_can_dispatch_(bool &can_dispatch, PartTransDispatchBudget *part_budget)
{
  int ret = OB_SUCCESS;
  can_dispatch = false;

  // 1. check can dispatch right now: currently check dispatcher module dispatch limit, will check partition redo dispatch limit
  // 2. if can dispatch, dispatch 1 redo
  if (enable_sort_by_seq_no_) {
    if (OB_ISNULL(part_budget) || !part_budget->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("part_task_budget should not be NULL or invalid if enable_sort_by_seq", KR(ret), K_(enable_sort_by_seq_no), KPC(part_budget));
    } else if (part_budget->can_dispatch()) {
      can_dispatch = true;
    }
  } else {
    can_dispatch = ATOMIC_LOAD(&cur_dispatched_redo_memory_) < ATOMIC_LOAD(&redo_memory_limit_);
    if (!can_dispatch) {
      LOG_DEBUG("dispatcher redo size touch limit, need pause and retry",
          K_(redo_memory_limit), K_(cur_dispatched_redo_memory));
    }
  }

  return ret;
}

// check memory and dispatch
int ObLogTransRedoDispatcher::dispatch_redo_(PartTransTask &part_trans, DmlRedoLogNode &redo_node)
{
  int ret = OB_SUCCESS;
  const bool is_redo_in_storage = redo_node.is_stored();
  ObLogEntryTask *log_entry_task = NULL;

  if (OB_FAIL(alloc_task_for_redo_(part_trans, redo_node, log_entry_task))) {
    LOG_ERROR("fail to generate log_entry_task for redo_node", KR(ret), K(part_trans), K(redo_node));
  } else if (is_redo_in_storage) {
    if (OB_FAIL(push_task_to_reader_(*log_entry_task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("fail to push log_entry_task to reader", KR(ret), K(is_redo_in_storage), K(log_entry_task));
      }
    }
  } else if (OB_FAIL(push_task_to_parser_(*log_entry_task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("fail to push log_entry_task to parser", KR(ret), K(is_redo_in_storage), K(log_entry_task));
    }
  }

  if (OB_SUCC(ret)) {
    trans_stat_mgr_->do_dispatch_redo_stat();
  }

  return ret;
}

// alloc an ObLogEntryTask as redo_read_task
int ObLogTransRedoDispatcher::alloc_task_for_redo_(PartTransTask &part_task,
    DmlRedoLogNode &redo_node,
    ObLogEntryTask *&log_entry_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(TCTX.log_entry_task_pool_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_entry_task_pool is null!", KR(ret));
  } else if (OB_FAIL(TCTX.log_entry_task_pool_->alloc(redo_node.is_direct_load_inc_log(), log_entry_task, part_task))) {
    LOG_ERROR("log_entry_task_pool_ alloc fail", KR(ret), KPC(log_entry_task), K(part_task));
  } else if (OB_ISNULL(log_entry_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_entry_task is NULL", KR(ret), K(part_task));
  } else if (OB_FAIL(log_entry_task->init(part_task.get_tls_id(), part_task.get_tenant_ls_str(),
          part_task.get_trans_id(), &redo_node))) {
    LOG_ERROR("failed to init log_entry_task", KR(ret), K(part_task), KP(&part_task), K(redo_node), KP(&redo_node));
  } else {
    /* init log_entry_task success*/
    const int64_t redo_node_len = redo_node.get_data_len();
    (void)ATOMIC_AAF(&cur_dispatched_redo_memory_, redo_node_len);
  }

  return ret;
}

// push redo_read_task(ObLogEntryTask) to ObLogRedoReader if redo is in storage
int ObLogTransRedoDispatcher::push_task_to_reader_(ObLogEntryTask &log_entry_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(TCTX.reader_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("redo reader is null!", KR(ret));
  } else {
    RETRY_FUNC(lib::ThreadPool::has_set_stop(), (*TCTX.reader_), push, log_entry_task, DISPATCH_OP_TIMEOUT);
  }

  return ret;
}

// push ObLogEntryTask to dml_parser if redo is in memory
int ObLogTransRedoDispatcher::push_task_to_parser_(ObLogEntryTask &log_entry_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(TCTX.dml_parser_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dml parser is null!", KR(ret));
  } else {
    RETRY_FUNC(lib::ThreadPool::has_set_stop(), (*TCTX.dml_parser_), push, log_entry_task, DISPATCH_OP_TIMEOUT);
  }

  return ret;
}

void ObLogTransRedoDispatcher::run(int64_t idx)
{
  int ret = OB_SUCCESS;
  set_cdc_thread_name("CDC-REDO-DISPATCHER", idx);
  TransCtx *trans_ctx = NULL;

  while (!lib::ThreadPool::has_set_stop() && OB_SUCC(ret)) {
    ObLogTraceIdGuard trace_guard;
    if (OB_FAIL(task_queue_.pop(trans_ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Queue is empty, wait a bit
        ob_usleep(100);
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("pop trans_ctx from queue failed", KR(ret));
        break;
      }
    } else if (OB_ISNULL(trans_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("trans_ctx is null", KR(ret));
      ATOMIC_DEC(&task_count_);
    } else if (OB_FAIL(process_dispatch_task_(*trans_ctx))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("process_dispatch_task_ failed", KR(ret), KP(trans_ctx));
      }
      ATOMIC_DEC(&task_count_);
    } else {
      ATOMIC_DEC(&task_count_);
    }
  }

  // Failure to exit
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "Redo dispatcher thread exits, thread_index=%ld, err=%d", idx, ret);
  }
}

int ObLogTransRedoDispatcher::process_dispatch_task_(TransCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  bool enable_monitor = false;
  ObLogTimeMonitor monitor("RedoDispatcher::process_dispatch_task", enable_monitor);

  // Use dispatch_func_ to call the appropriate dispatch function
  if (OB_FAIL(((*this).*dispatch_func_)(trans_ctx))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("failed to dispatch redo of trans", KR(ret), K(trans_ctx));
    }
  } else {
    trans_ctx.set_trans_redo_dispatched();
    trans_stat_mgr_->do_dispatch_trans_stat();
  }

  LOG_DEBUG("process_dispatch_task_ finish", KR(ret), K(trans_ctx));

  return ret;
}

void ObLogTransRedoDispatcher::get_task_stat(int64_t &task_count, int64_t &queue_size) const
{
  task_count = ATOMIC_LOAD(&task_count_);
  queue_size = task_queue_.get_curr_total();
}

} // end namespace libobcdc
} // end namespace oceanbase
