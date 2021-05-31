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

#include "ob_ilog_fetcher.h"
#include "lib/time/ob_time_utility.h"
#include "clog/ob_log_define.h"  // ObGetCursorResult
#include "ob_ilog_fetch_task_mgr.h"
#include "clog/ob_log_engine.h"            // ObILogEngine
#include "ob_archive_clog_split_engine.h"  // ObArCLogSplitEngine
#include "ob_archive_allocator.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::clog;

ObArchiveIlogFetcher::ObArchiveIlogFetcher()
    : inited_(false),
      start_flag_(false),
      log_archive_round_(-1),
      incarnation_(-1),
      cur_consume_ilog_file_id_(0),
      cur_handle_ilog_file_id_(0),
      log_engine_(NULL),
      log_wrapper_(NULL),
      pg_mgr_(NULL),
      clog_split_engine_(NULL),
      ilog_fetch_mgr_(NULL),
      allocator_()
{}

ObArchiveIlogFetcher::~ObArchiveIlogFetcher()
{
  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher destroy");
  destroy();
}

int ObArchiveIlogFetcher::init(ObArchiveAllocator* allocator, ObILogEngine* log_engine,
    ObArchiveLogWrapper* log_wrapper, ObArchivePGMgr* pg_mgr, ObArCLogSplitEngine* clog_split_engine,
    ObArchiveIlogFetchTaskMgr* ilog_fetch_mgr)
{
  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher begin init");
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(ERROR, "ObArchiveIlogFetcher has been initialized", KR(ret));
  } else if (OB_ISNULL(allocator_ = allocator) || OB_ISNULL(log_engine_ = log_engine) ||
             OB_ISNULL(log_wrapper_ = log_wrapper) || OB_ISNULL(pg_mgr_ = pg_mgr) ||
             OB_ISNULL(clog_split_engine_ = clog_split_engine) || OB_ISNULL(ilog_fetch_mgr_ = ilog_fetch_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(allocator),
        K(log_engine),
        K(log_wrapper),
        K(pg_mgr),
        K(clog_split_engine),
        K(ilog_fetch_mgr));
  } else {
    start_flag_ = false;
    inited_ = true;
  }

  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher init succ");

  return ret;
}

void ObArchiveIlogFetcher::destroy()
{
  inited_ = false;
  stop();
  wait();
  start_flag_ = false;
  log_archive_round_ = -1;
  incarnation_ = -1;
  cur_consume_ilog_file_id_ = 0;
  cur_handle_ilog_file_id_ = 0;
  log_engine_ = NULL;
  log_wrapper_ = NULL;
  pg_mgr_ = NULL;
  clog_split_engine_ = NULL;
  ilog_fetch_mgr_ = NULL;
  allocator_ = NULL;
}

int ObArchiveIlogFetcher::start()
{
  int ret = OB_SUCCESS;
  const int64_t thread_num = ARCHIVE_ILOG_FETCHER_NUM;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveIlogFetcher not init");
    ret = OB_NOT_INIT;
  } else {
    set_thread_count(thread_num);

    if (OB_FAIL(ObThreadPool::start())) {
      ARCHIVE_LOG(WARN, "ObArchiveIlogFetcher start fail", KR(ret));
    } else {
      ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher start succ");
    }
  }

  return ret;
}

void ObArchiveIlogFetcher::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher stop");
  start_flag_ = false;
  ObThreadPool::stop();
}

void ObArchiveIlogFetcher::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher wait");
  ObThreadPool::wait();
}

void ObArchiveIlogFetcher::notify_start_archive_round()
{
  start_flag_ = true;
  ARCHIVE_LOG(INFO, "notify ObArchiveIlogFetcher start work", K(start_flag_));
}

void ObArchiveIlogFetcher::notify_stop()
{
  start_flag_ = false;
  ARCHIVE_LOG(INFO, "notify ObArchiveIlogFetcher stop work", K(start_flag_));
}

int ObArchiveIlogFetcher::generate_and_submit_checkpoint_task(
    const ObPGKey& pg_key, const uint64_t max_log_id, const int64_t max_log_submit_ts, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObPGArchiveCLogTask* task = NULL;
  ObPGArchiveTaskGuard guard(pg_mgr_);
  ObPGArchiveTask* pg_archive_task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveIlogFetcher not init");
  } else if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == max_log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == max_log_submit_ts) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == checkpoint_ts) || OB_ISNULL(pg_mgr_) ||
             OB_ISNULL(clog_split_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(pg_key),
        K(max_log_id),
        K(max_log_submit_ts),
        K(checkpoint_ts),
        K(pg_mgr_),
        K(clog_split_engine_));
  } else if (OB_FAIL(pg_mgr_->get_pg_archive_task_guard_with_status(pg_key, guard)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN,
        "get_pg_archive_task_guard_with_status fail",
        KR(ret),
        K(pg_key),
        K(max_log_id),
        K(max_log_submit_ts),
        K(checkpoint_ts));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      ARCHIVE_LOG(WARN, "pg not exist", KR(ret), K(pg_key), K(max_log_id), K(max_log_submit_ts), K(checkpoint_ts));
    }
  } else if (OB_ISNULL(pg_archive_task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(
        ERROR, "pg_archive_task is NULL", KR(ret), K(pg_key), K(max_log_id), K(max_log_submit_ts), K(checkpoint_ts));
  } else if (OB_ISNULL(task = alloc_clog_split_task())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(
        WARN, "alloc_clog_split_task fail", KR(ret), K(pg_key), K(max_log_id), K(max_log_submit_ts), K(checkpoint_ts));
  } else if (OB_FAIL(task->init_checkpoint_task(pg_key,
                 pg_archive_task->get_pg_incarnation(),
                 pg_archive_task->get_pg_archive_round(),
                 pg_archive_task->get_pg_leader_epoch(),
                 max_log_id,
                 checkpoint_ts,
                 max_log_submit_ts,
                 common::ObCompressorType::NONE_COMPRESSOR))) {
    ARCHIVE_LOG(WARN,
        "ObPGArchiveCLogTask init fail",
        KR(ret),
        K(pg_key),
        K(max_log_id),
        K(max_log_submit_ts),
        K(checkpoint_ts));
  } else if (OB_FAIL(consume_clog_task_(task))) {
    ARCHIVE_LOG(WARN,
        "consume_clog_task_ fail",
        KR(ret),
        K(task),
        K(pg_key),
        K(max_log_id),
        K(max_log_submit_ts),
        K(checkpoint_ts));
  }

  if (NULL != task) {
    free_clog_split_task(task);
    task = NULL;
  }

  return ret;
}

int ObArchiveIlogFetcher::generate_and_submit_pg_first_log(StartArchiveHelper& helper)
{
  int ret = OB_SUCCESS;
  ObPGArchiveCLogTask* task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveIlogFetcher not init");
  } else if (OB_UNLIKELY(!helper.is_valid()) || OB_ISNULL(clog_split_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(helper), K(clog_split_engine_));
  } else if (OB_ISNULL(task = alloc_clog_split_task())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc_clog_split_task fail", KR(ret), K(helper));
  } else if (OB_FAIL(task->init_kickoff_task(helper.pg_key_,
                 helper.incarnation_,
                 helper.archive_round_,
                 helper.epoch_,
                 helper.round_start_info_,
                 helper.max_archived_log_info_.max_checkpoint_ts_archived_,
                 common::ObCompressorType::NONE_COMPRESSOR))) {
    ARCHIVE_LOG(WARN, "ObPGArchiveCLogTask init fail", KR(ret), K(helper));
  } else if (OB_FAIL(consume_clog_task_(task))) {
    ARCHIVE_LOG(WARN, "consume_clog_task_ fail", KR(ret), K(helper), KPC(task));
  }

  if (NULL != task) {
    free_clog_split_task(task);
    task = NULL;
  }

  return ret;
}

ObPGArchiveCLogTask* ObArchiveIlogFetcher::alloc_clog_split_task()
{
  ObPGArchiveCLogTask* task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveIlogFetcher not init");
  } else if (OB_ISNULL(allocator_)) {
    ARCHIVE_LOG(ERROR, "allocator_ is NULL", K(allocator_));
  } else {
    task = allocator_->alloc_clog_split_task();
  }

  return task;
}

void ObArchiveIlogFetcher::free_clog_split_task(ObPGArchiveCLogTask* task)
{
  if (NULL != task && NULL != allocator_) {
    allocator_->free_clog_split_task(task);
  }
}

int ObArchiveIlogFetcher::set_archive_round_info(const int64_t round, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(round <= 0) || OB_UNLIKELY(incarnation <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(round), K(incarnation), KR(ret));
  } else {
    log_archive_round_ = round;
    incarnation_ = incarnation;
  }

  return ret;
}

void ObArchiveIlogFetcher::clear_archive_info()
{
  log_archive_round_ = -1;
  incarnation_ = -1;
}

// ilog fetcher start work only after all partitions already archive kickoff log
void ObArchiveIlogFetcher::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveIlogFetcher thread run");

  lib::set_thread_name("ArchiveIlogFetcher");

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveIlogFetcher not init");
  } else {
    // wait until start_flag_ becomes true
    while (!has_set_stop()) {
      if (start_flag_) {
        do_thread_task_();
      } else {
        usleep(1000 * 1000L);
      }
    }
  }
}

// partition's task self drive, push a task back regardless of new ilog exists or not
void ObArchiveIlogFetcher::do_thread_task_()
{
  int ret = OB_SUCCESS;
  const int64_t thread_index = get_thread_idx();
  PGFetchTask fetch_task;
  PGFetchTask next_task;
  uint64_t end_log_id = OB_INVALID_ID;
  bool task_exist = false;
  bool empty_task = false;

  ObPGArchiveTaskGuard guard(pg_mgr_);
  ObPGArchiveTask* pg_archive_task = NULL;
  bool need_block_log_archive = false;
#ifdef ERRSIM
  need_block_log_archive = GCONF.enable_block_log_archive;
#endif

  if (need_block_log_archive) {
    // do nothing
  } else if (OB_ISNULL(pg_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg_mgr_ is NULL");
  } else if (OB_FAIL(get_pg_ilog_fetch_task_(fetch_task, task_exist))) {
    ARCHIVE_LOG(WARN, "get_pg_ilog_fetch_task_ fail", KR(ret), K(thread_index));
  } else if (!task_exist) {
    usleep(100 * 1000L);
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "no ARCHIVE task on this observer");
    }
  } else if (OB_FAIL(pg_mgr_->get_pg_archive_task_guard_with_status(fetch_task.pg_key_, guard)) &&
             OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(fetch_task));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ARCHIVE_LOG(WARN, "pg archive task not exist, skip it", K(fetch_task));
  } else if (OB_ISNULL(pg_archive_task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg_archive_task is NULL", KR(ret), K(pg_archive_task), K(fetch_task));
  } else if (OB_FAIL(affirm_pg_log_consume_progress_(fetch_task, *pg_archive_task))) {
    ARCHIVE_LOG(WARN, "affirm_pg_log_consume_progress_ fail", KR(ret), K(fetch_task), K(pg_archive_task));
  } else if (OB_FAIL(handle_ilog_fetch_task_(fetch_task, *pg_archive_task, empty_task, end_log_id))) {
    ARCHIVE_LOG(WARN, "handle_ilog_fetch_task_ fail", KR(ret), K(thread_index), K(fetch_task));
  } else if (OB_FAIL(update_pg_split_progress_(end_log_id, empty_task, *pg_archive_task, fetch_task))) {
    ARCHIVE_LOG(WARN, "update_pg_split_progress_ fail", KR(ret), K(fetch_task), K(fetch_task));
  } else if (OB_FAIL(construct_pg_next_ilog_fetch_task_(fetch_task, *pg_archive_task, next_task))) {
    ARCHIVE_LOG(WARN, "construct_pg_next_ilog_fetch_task_ fail", KR(ret), K(fetch_task));
  } else if (OB_FAIL(check_and_consume_clog_task_(fetch_task, next_task))) {
    ARCHIVE_LOG(WARN, "check_and_consume_clog_task_ fail", KR(ret), K(fetch_task), K(next_task));
  }

  if (OB_SUCC(ret) && task_exist) {
    if (OB_FAIL(push_back_pg_ilog_fetch_task_(next_task))) {
      ARCHIVE_LOG(WARN, "push_back_pg_ilog_fetch_task_ fail", KR(ret), K(fetch_task), K(next_task));
    }
  } else if (!task_exist) {
    // skip it
  } else if (OB_ENTRY_NOT_EXIST == ret || OB_LOG_ARCHIVE_LEADER_CHANGED == ret || OB_IN_STOP_STATE == ret) {
    free_clog_split_task(fetch_task.clog_task_);
    fetch_task.clog_task_ = NULL;
  } else {
    mark_fatal_error_(fetch_task.pg_key_, fetch_task.epoch_, fetch_task.incarnation_, fetch_task.archive_round_, ret);
  }
}

int ObArchiveIlogFetcher::get_pg_ilog_fetch_task_(PGFetchTask& task, bool& task_exist)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ilog_fetch_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ilog_fetch_mgr_ is NULL", KR(ret));
  } else if (OB_FAIL(ilog_fetch_mgr_->pop_ilog_fetch_task(task, task_exist))) {
    ARCHIVE_LOG(WARN, "pop_ilog_fetch_task fail", KR(ret));
  } else if (task_exist && task.ilog_file_id_ != cur_handle_ilog_file_id_) {
    ARCHIVE_LOG(INFO, "switch ilog file succ", K(cur_handle_ilog_file_id_), K(task));
    cur_handle_ilog_file_id_ = task.ilog_file_id_;
  }

  return ret;
}

// 1. search max log of partition
// 2. locate log in which ilog file
// 3. construct new ilog fetch task
int ObArchiveIlogFetcher::construct_pg_next_ilog_fetch_task_(
    PGFetchTask& fetch_task, ObPGArchiveTask& pg_archive_task, PGFetchTask& task)
{
  int ret = OB_SUCCESS;
  uint64_t max_split_log_id = OB_INVALID_ID;
  uint64_t start_log_id = OB_INVALID_ID;
  uint64_t end_log_id = OB_INVALID_ID;
  bool ilog_file_exist = false;
  file_id_t ilog_file_id = OB_INVALID_FILE_ID;
  const ObPGKey& pg_key = fetch_task.pg_key_;

  if (OB_ISNULL(log_wrapper_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log_wrapper_ is NULL", KR(ret), K(log_wrapper_));
  } else if (OB_FAIL(pg_archive_task.get_fetcher_max_split_log_id(
                 fetch_task.epoch_, fetch_task.incarnation_, fetch_task.archive_round_, max_split_log_id))) {
    ARCHIVE_LOG(WARN, "get_fetcher_max_split_log_id fail", KR(ret), K(fetch_task), K(pg_archive_task));
  } else if (OB_UNLIKELY(OB_INVALID_ID == max_split_log_id || OB_INVALID_ID == (start_log_id = max_split_log_id + 1))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), K(max_split_log_id));
  } else if (OB_FAIL(log_wrapper_->locate_ilog_by_log_id(
                 pg_key, start_log_id, end_log_id, ilog_file_exist, ilog_file_id))) {
    ARCHIVE_LOG(WARN, "locate_ilog_by_log_id fail", KR(ret), K(pg_key), K(start_log_id));
  } else {
    task.pg_key_ = pg_key;
    task.start_log_id_ = start_log_id;
    task.ilog_file_id_ = ilog_file_id;
    task.incarnation_ = incarnation_;
    task.archive_round_ = log_archive_round_;
    task.epoch_ = fetch_task.epoch_;
  }

  return ret;
}

int ObArchiveIlogFetcher::push_back_pg_ilog_fetch_task_(PGFetchTask& task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ilog_fetch_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ilog_fetch_mgr_ is NULL", KR(ret));
  } else {
    do {
      ret = OB_SUCCESS;
      if (OB_FAIL(ilog_fetch_mgr_->add_ilog_fetch_task(task))) {
        ARCHIVE_LOG(WARN, "add_ilog_fetch_task fail", KR(ret), K(task));
        usleep(100);
      }
    } while (OB_FAIL(ret));
  }

  return ret;
}

int ObArchiveIlogFetcher::update_pg_split_progress_(
    const uint64_t end_log_id, bool empty_task, ObPGArchiveTask& pg_archive_task, PGFetchTask& task)
{
  int ret = OB_SUCCESS;

  if (empty_task) {
    // do nothing
  } else if (OB_FAIL(pg_archive_task.update_max_split_log_id(
                 task.epoch_, task.incarnation_, task.archive_round_, end_log_id, cur_handle_ilog_file_id_))) {
    ARCHIVE_LOG(WARN, "update_max_split_log_id fail", KR(ret), K(end_log_id), K(task), K(pg_archive_task));
  }

  return ret;
}

int ObArchiveIlogFetcher::handle_ilog_fetch_task_(
    PGFetchTask& task, ObPGArchiveTask& pg_archive_task, bool& empty_task, uint64_t& end_log_id)
{
  int ret = OB_SUCCESS;
  const uint64_t start_log_id = task.start_log_id_;
  bool need_do = false;
  bool new_task = false;
  empty_task = false;

  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task is not valid", KR(ret), K(task));
  } else if (OB_FAIL(update_ilog_fetch_info_(task, pg_archive_task, end_log_id, need_do))) {
    ARCHIVE_LOG(WARN, "update_ilog_fetch_info_ fail", KR(ret), K(task));
  } else if (!need_do) {
    // skip it
  } else if (NULL == task.clog_task_) {
    if (OB_ISNULL(task.clog_task_ = alloc_clog_split_task())) {
      // skip this task if alloc memory fail
      need_do = false;
      ARCHIVE_LOG(WARN, "alloc_clog_split_task fail", KR(ret), K(task));
    } else {
      new_task = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (!need_do) {
      // reset end_log_id with start log id
      end_log_id = start_log_id - 1;
      empty_task = true;
    } else if (OB_FAIL(build_clog_cursor_(task, start_log_id, end_log_id, *task.clog_task_))) {
      ARCHIVE_LOG(WARN, "build_clog_cursor_ fail", KR(ret), K(task));
    } else if (!new_task) {
      // do nothing
    } else if (OB_FAIL(init_clog_split_task_meta_info_(
                   task.pg_key_, task.start_log_id_, pg_archive_task, *task.clog_task_))) {
      ARCHIVE_LOG(WARN, "init_clog_split_task_meta_info_ fail", KR(ret), K(task));
    } else {
    }
  }

  return ret;
}

int ObArchiveIlogFetcher::affirm_pg_log_consume_progress_(PGFetchTask& task, ObPGArchiveTask& pg_archive_task)
{
  int ret = OB_SUCCESS;
  uint64_t max_consume_log_id = OB_INVALID_ID;
  const uint64_t start_log_id = task.start_log_id_;

  if (OB_FAIL(pg_archive_task.get_fetcher_max_split_log_id(
          task.epoch_, task.incarnation_, task.archive_round_, max_consume_log_id))) {
    ARCHIVE_LOG(WARN, "get_fetcher_max_split_log_id fail", KR(ret), K(task), K(pg_archive_task));
  } else if (OB_UNLIKELY(OB_INVALID_ID == max_consume_log_id) || OB_UNLIKELY(start_log_id != max_consume_log_id + 1)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "pg log consume turn wrong", KR(ret), K(task), K(pg_archive_task));
  }

  return ret;
}

int ObArchiveIlogFetcher::update_ilog_fetch_info_(
    PGFetchTask& task, ObPGArchiveTask& pg_archive_task, uint64_t& end_log_id, bool& need_do)
{
  int ret = OB_SUCCESS;
  const uint64_t start_log_id = task.start_log_id_;
  const ObPGKey& pg_key = pg_archive_task.get_pg_key();
  uint64_t max_log_id = OB_INVALID_ID;
  bool ilog_file_exist = false;
  file_id_t ilog_file_id = OB_INVALID_FILE_ID;
  need_do = true;

  if (OB_ISNULL(log_wrapper_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log_wrapper_ is NULL", KR(ret), K(pg_archive_task));
  } else if (OB_FAIL(log_wrapper_->get_pg_max_log_id(pg_key, max_log_id))) {
    ARCHIVE_LOG(WARN, "get_pg_max_log_id fail", KR(ret), K(pg_archive_task));
    if (OB_LOG_ARCHIVE_LEADER_CHANGED == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = pg_mgr_->inner_delete_pg_archive_task(pg_key))) {
        ARCHIVE_LOG(WARN, "inner_delete_pg_archive_task fail", KR(ret), K(pg_key));
      }
    }
  } else if (max_log_id < start_log_id) {
    need_do = false;
  } else if (OB_FAIL(
                 pg_archive_task.update_max_log_id(task.epoch_, task.incarnation_, task.archive_round_, max_log_id))) {
    ARCHIVE_LOG(WARN, "update_max_log_id fail", KR(ret), K(pg_archive_task), K(max_log_id));
  } else if (OB_FAIL(log_wrapper_->locate_ilog_by_log_id(
                 pg_key, start_log_id, end_log_id, ilog_file_exist, ilog_file_id))) {
    ARCHIVE_LOG(ERROR, "locate_ilog_by_log_id fail", KR(ret), K(pg_key), K(start_log_id));
  } else if (!ilog_file_exist) {
    end_log_id = max_log_id;
  }

  return ret;
}

int ObArchiveIlogFetcher::build_clog_cursor_(
    PGFetchTask& fetch_task, const uint64_t min_log_id, const uint64_t max_log_id, ObPGArchiveCLogTask& task)
{
  int ret = OB_SUCCESS;
  const ObPGKey pg_key = fetch_task.pg_key_;

  if (OB_ISNULL(log_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log_engine_ is NULL", KR(ret));
  } else {
    int64_t log_index = 0;
    while (OB_SUCC(ret) && (min_log_id + log_index) <= max_log_id) {
      const uint64_t start_log_id = min_log_id + log_index;
      int64_t arr_len = max_log_id - start_log_id >= 1024 ? 1024 : (max_log_id - start_log_id + 1);
      ObGetCursorResult cursor_result;
      if (OB_FAIL(init_log_cursor_result_(pg_key, arr_len, cursor_result))) {
        ARCHIVE_LOG(WARN, "init_log_cursor_result_ fail", KR(ret), K(pg_key));
      } else if (OB_FAIL(log_engine_->get_cursor_batch(pg_key, start_log_id, cursor_result))) {
        // max_log_id is queried before, error should not be returned here
        ARCHIVE_LOG(ERROR, "get_cursor_batch fail", KR(ret), K(pg_key), K(start_log_id));
      } else if (OB_FAIL(build_archive_clog_task(start_log_id, cursor_result, task, fetch_task))) {
        ARCHIVE_LOG(WARN, "build_archive_clog_task fail", KR(ret), K(start_log_id), K(cursor_result), K(task));
      } else {
        log_index += cursor_result.ret_len_;
      }

      if (NULL != cursor_result.csr_arr_) {
        ob_archive_free(cursor_result.csr_arr_);
      }
    }
  }

  return ret;
}

int ObArchiveIlogFetcher::init_log_cursor_result_(
    const ObPGKey& pg_key, const int64_t arr_len, ObGetCursorResult& cursor_result)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt* cursor_array = NULL;
  int64_t alloc_size = arr_len * sizeof(ObLogCursorExt);

  do {
    if (OB_ISNULL(cursor_array = static_cast<ObLogCursorExt*>(ob_archive_malloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        ARCHIVE_LOG(WARN, "ob_archive_malloc fail", KR(ret), K(pg_key));
      }
      usleep(DEFAULT_ARCHIVE_WAIT_TIME_AFTER_EAGAIN);
    }
  } while (OB_ALLOCATE_MEMORY_FAILED == ret && start_flag_);

  if (OB_SUCC(ret)) {
    for (int64_t index = 0; index < arr_len; index++) {
      new (cursor_array + index) ObLogCursorExt();
      ObLogCursorExt& cursor_ext = cursor_array[index];
      cursor_ext.reset();
    }

    cursor_result.csr_arr_ = cursor_array;
    cursor_result.arr_len_ = arr_len;
    cursor_result.ret_len_ = 0;
  }

  return ret;
}

int ObArchiveIlogFetcher::init_clog_split_task_meta_info_(
    const ObPGKey& pg_key, const uint64_t start_log_id, ObPGArchiveTask& pg_archive_task, ObPGArchiveCLogTask& task)
{
  int ret = OB_SUCCESS;
  const int64_t incarnation = pg_archive_task.get_pg_incarnation();
  const int64_t archive_round = pg_archive_task.get_pg_archive_round();
  const int64_t epoch = pg_archive_task.get_pg_leader_epoch();

  if (OB_FAIL(task.init_clog_split_task(pg_key,
          incarnation,
          archive_round,
          epoch,
          start_log_id,
          ObTimeUtility::current_time(),
          ObServerConfig::get_instance().backup_log_archive_option.get_compressor_type()))) {
    ARCHIVE_LOG(WARN, "init_pg_archive_clog_task fail", KR(ret), K(pg_key), K(start_log_id));
  }

  return ret;
}

// function caller FREE task itself
int ObArchiveIlogFetcher::consume_clog_task_(ObPGArchiveCLogTask*& task, file_id_t ilog_file_id)
{
  int ret = OB_SUCCESS;
  int64_t task_num = 0;
  const int64_t cur_ilog_file_id = ATOMIC_LOAD(&cur_consume_ilog_file_id_);
  const bool new_ilog_flag = -1 != ilog_file_id && cur_ilog_file_id != ilog_file_id;

  if (OB_ISNULL(clog_split_engine_) || OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR,
        "clog_split_engine_ or task is NULL",
        KR(ret),
        K(clog_split_engine_),
        K(task),
        K(cur_ilog_file_id),
        K(ilog_file_id));
  }
  // to increase clog cache hit, submit task util clog_split queue is empty when switch ilog file
  // only single thread mode work
  else if (new_ilog_flag) {
    do {
      task_num = clog_split_engine_->get_total_task_num();
      if (task_num > 0) {
        usleep(WAIT_TIME_AFTER_EAGAIN);
      }
    } while (task_num > 0 && !has_set_stop());
    ATOMIC_STORE(&cur_consume_ilog_file_id_, ilog_file_id);
    ARCHIVE_LOG(INFO, "switch consume ilog file", K(cur_ilog_file_id), K(ilog_file_id));
  }

  if (OB_SUCC(ret)) {
    if (task->is_archive_checkpoint_task()) {
      if (OB_FAIL(clog_split_engine_->submit_split_task(task))) {
        ARCHIVE_LOG(WARN, "submit checkpoint task fail", KR(ret), KPC(task));
      } else {
        task = NULL;
      }
    } else {
      bool need_retry = false;
      do {
        need_retry = false;
        ret = OB_SUCCESS;
        task_num = clog_split_engine_->get_total_task_num();
        if (task_num >= 0.7 * MAX_SPLIT_TASK_COUNT) {
          need_retry = true;
        } else if (OB_FAIL(clog_split_engine_->submit_split_task(task)) && OB_EAGAIN != ret) {
          ARCHIVE_LOG(WARN, "push task fail", KR(ret), KPC(task));
        } else if (OB_EAGAIN == ret) {
          need_retry = true;
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
            ARCHIVE_LOG(WARN, "push task fail", KR(ret), KPC(task));
          }
        } else {
          need_retry = false;
          task = NULL;
        }

        if (need_retry) {
          usleep(WAIT_TIME_AFTER_EAGAIN);
        }
      } while (need_retry && !has_set_stop());
    }
  }

  return ret;
}

int ObArchiveIlogFetcher::build_archive_clog_task(
    const uint64_t start_log_id, ObGetCursorResult& cursor_result, ObPGArchiveCLogTask& task, PGFetchTask& fetch_task)
{
  int ret = OB_SUCCESS;
  const int64_t count = cursor_result.ret_len_;

  if (count > DEFAULT_CLOG_CURSOR_NUM) {
    do {
      if (OB_FAIL(task.clog_pos_list_.reserve(count))) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            ARCHIVE_LOG(WARN, "clog_pos_list_ reserve fail, not enough memory", KR(ret), K(cursor_result));
          }
          usleep(DEFAULT_ARCHIVE_WAIT_TIME_AFTER_EAGAIN);
        } else {
          ARCHIVE_LOG(WARN, "clog_pos_list_ reserve fail", KR(ret), K(cursor_result));
        }
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret && start_flag_);
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObArchiveLogCursor archive_log_cursor;
      archive_log_cursor.set(cursor_result.csr_arr_[i].get_file_id(),
          cursor_result.csr_arr_[i].get_offset(),
          cursor_result.csr_arr_[i].get_size(),
          cursor_result.csr_arr_[i].is_batch_committed(),
          start_log_id + i,
          cursor_result.csr_arr_[i].get_submit_timestamp(),
          cursor_result.csr_arr_[i].get_accum_checksum());
      if (OB_FAIL(task.clog_pos_list_.push_back(archive_log_cursor))) {
        ARCHIVE_LOG(ERROR,
            "task.clog_pos_list_.push_back fail",
            KR(ret),
            K(i),
            K(count),
            K(cursor_result),
            K(archive_log_cursor));
      } else {
        fetch_task.clog_count_++;
        fetch_task.clog_size_ += cursor_result.csr_arr_[i].get_size();
        fetch_task.first_log_gen_tstamp_ = OB_INVALID_TIMESTAMP == fetch_task.first_log_gen_tstamp_
                                               ? ObTimeUtility::current_time()
                                               : fetch_task.first_log_gen_tstamp_;
      }
    }
  }

  return ret;
}

// next_task inherit next_task if clog_task not consumed
//
// clog_task can be consumed only if any condition is satisfied
// 1. switch ilog_file, fetch_task.ilog_file_id_ != next_task.ilog_file_id_
// 2. clog_task exists more than 10s
// 3. clog_task size > THRESHOLD
// 4. clog_task count > THRESHOLD
int ObArchiveIlogFetcher::check_and_consume_clog_task_(PGFetchTask& fetch_task, PGFetchTask& next_task)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = ObTimeUtility::current_time();
  const int64_t archive_checkpoint_interval = GCONF.log_archive_checkpoint_interval;
  const int64_t max_delay_time = std::min(MAX_ILOG_FETCHER_CONSUME_DELAY, archive_checkpoint_interval);

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP != fetch_task.first_log_gen_tstamp_ && NULL == fetch_task.clog_task_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid fetch_task", KR(ret), K(fetch_task));
  } else if (NULL == fetch_task.clog_task_) {
    // no task, skip it
  } else if (fetch_task.ilog_file_id_ != next_task.ilog_file_id_ ||
             cur_ts - fetch_task.first_log_gen_tstamp_ > max_delay_time ||
             fetch_task.clog_size_ > ILOG_FETCHER_CONSUME_CLOG_SIZE_THRESHOLD ||
             fetch_task.clog_count_ > ILOG_FETCHER_CONSUME_CLOG_COUNT_THRESHOLD) {
    if (OB_FAIL(consume_clog_task_(fetch_task.clog_task_, fetch_task.ilog_file_id_))) {
      ARCHIVE_LOG(WARN, "consume_clog_task_ fail", KR(ret), K(fetch_task));
    }
    // consume fail, ilog_fetcher free buffer
    if (NULL != fetch_task.clog_task_) {
      free_clog_split_task(fetch_task.clog_task_);
      fetch_task.clog_task_ = NULL;
    }
  } else {
    next_task.clog_task_ = fetch_task.clog_task_;
    next_task.clog_size_ = fetch_task.clog_size_;
    next_task.clog_count_ = fetch_task.clog_count_;
    next_task.first_log_gen_tstamp_ = fetch_task.first_log_gen_tstamp_;
  }

  return ret;
}

void ObArchiveIlogFetcher::mark_fatal_error_(
    const ObPGKey& pg_key, const int64_t epoch, const int64_t incarnation, const int64_t round, const int ret_code)
{
  int ret = OB_SUCCESS;

  if (OB_LOG_ARCHIVE_LEADER_CHANGED == ret_code) {
    // skip it
    ARCHIVE_LOG(INFO, "obsolete task, skip it", KR(ret_code), K(pg_key), K(epoch), K(incarnation), K(round));
  } else if (OB_FAIL(pg_mgr_->mark_fatal_error(pg_key, epoch, incarnation, round))) {
    ARCHIVE_LOG(ERROR, "mark_fatal_error fail", KR(ret), K(pg_key), K(epoch), K(incarnation), K(round), KR(ret_code));
  } else {
    ARCHIVE_LOG(ERROR, "mark_fatal_error_ succ", KR(ret_code), K(pg_key), K(epoch), K(incarnation), K(round));
  }
}

}  // namespace archive
}  // namespace oceanbase
