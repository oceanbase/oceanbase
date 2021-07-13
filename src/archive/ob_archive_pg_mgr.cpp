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

#include "ob_archive_round_mgr.h"
#include "ob_archive_pg_mgr.h"
#include <pthread.h>
#include "ob_archive_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "ob_log_archive_struct.h"
#include "ob_archive_util.h"
#include "ob_start_archive_helper.h"
#include "ob_ilog_fetcher.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "share/ob_debug_sync.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace archive {
class ObArchivePGMgr::CheckArchiveRoundStartFunctor {
public:
  CheckArchiveRoundStartFunctor(const int64_t archive_round, const int64_t incarnation)
      : start_flag_(true), archive_round_(archive_round), incarnation_(incarnation)
  {}

  bool operator()(const ObPGKey& pg_key, ObPGArchiveTask* task)
  {
    int ret = OB_SUCCESS;
    bool bret = true;

    if (OB_UNLIKELY(!pg_key.is_valid()) || OB_ISNULL(task)) {
      ret = OB_INVALID_ARGUMENT;
      ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), KPC(task));
      bret = false;
    } else if (task->check_upload_first_log_succ()) {
      // skip
    } else {
      ARCHIVE_LOG(INFO, "kickoff log not upload", KPC(task));
      start_flag_ = false;
    }

    return bret;
  }

  bool return_start_flag()
  {
    return start_flag_;
  }

private:
  bool start_flag_;
  int64_t archive_round_;
  int64_t incarnation_;
};

class ObArchivePGMgr::CheckDeletePGFunctor {
public:
  CheckDeletePGFunctor(ObArchivePGMgr* pg_mgr) : pg_mgr_(pg_mgr)
  {}

  bool operator()(const ObPGKey& pg_key, ObPGArchiveTask* task)
  {
    int ret = OB_SUCCESS;
    bool is_leader = true;
    ObRole role;
    ObIPartitionGroupGuard guard;
    UNUSED(task);

    if (OB_ISNULL(pg_mgr_) || OB_UNLIKELY(!pg_key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key), K(pg_mgr_));
    } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ARCHIVE_LOG(TRACE, "partition not exist", KR(ret), K(pg_key));
      } else {
        ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key));
      }
      is_leader = false;
    } else if (OB_FAIL(guard.get_partition_group()->get_log_service()->get_role(role))) {
      ARCHIVE_LOG(TRACE, "get_role fail", KR(ret), K(pg_key));
      is_leader = false;
    } else if (!is_strong_leader(role)) {
      is_leader = false;
    }

    if (!is_leader && OB_NOT_NULL(pg_mgr_)) {
      if (OB_FAIL(pg_mgr_->inner_delete_pg_archive_task(pg_key))) {
        ARCHIVE_LOG(WARN, "inner_delete_pg_archive_task fail", KR(ret), K(pg_key));
      }
    }

    return true;
  }

private:
  ObArchivePGMgr* pg_mgr_;
};

//============================== start of ObArchivePGMgr =============================//
//============================= start of public functions ============================//
ObArchivePGMgr::ObArchivePGMgr()
    : inited_(false),
      thread_counter_(0),
      log_archive_round_(-1),
      incarnation_(-1),
      start_archive_ts_(OB_INVALID_TIMESTAMP),
      last_reconfirm_pg_tstamp_(OB_INVALID_TIMESTAMP),
      pg_map_(),
      thread_num_(0),
      queue_num_(0),
      allocator_(NULL),
      log_wrapper_(NULL),
      archive_round_mgr_(NULL),
      archive_mgr_(NULL),
      partition_service_(NULL)
{}

ObArchivePGMgr::~ObArchivePGMgr()
{
  destroy();

  ARCHIVE_LOG(INFO, "ObArchivePGMgr destroy");
}

int ObArchivePGMgr::init(ObArchiveAllocator* allocator, ObArchiveLogWrapper* log_wrapper,
    ObPartitionService* partition_service, ObArchiveRoundMgr* archive_round_mgr, ObArchiveMgr* archive_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr has been initialized", KR(ret));
  } else if (OB_ISNULL(allocator_ = allocator) || OB_ISNULL(log_wrapper_ = log_wrapper) ||
             OB_ISNULL(archive_mgr_ = archive_mgr) || OB_ISNULL(partition_service_ = partition_service) ||
             OB_ISNULL(archive_round_mgr_ = archive_round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(allocator),
        K(log_wrapper),
        K(archive_mgr),
        K(partition_service),
        K(archive_round_mgr));
  } else if (OB_FAIL(pg_map_.init(ObModIds::OB_ARCHIVE_PG_MAP))) {
    ARCHIVE_LOG(WARN, "pg_map_ init fail", KR(ret));
  } else {
    // succ
    thread_num_ = lib::is_mini_mode() ? 2L : PG_MGR_THREAD_COUNT;
    queue_num_ = thread_num_ - 1;
    inited_ = true;
    ARCHIVE_LOG(INFO, "ObArchivePGMgr init succ");
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }

  return ret;
}

void ObArchivePGMgr::destroy()
{
  inited_ = false;
  stop();
  wait();
  (void)reset_tasks();
  pg_map_.destroy();

  inited_ = false;
  thread_counter_ = 0;
  log_archive_round_ = -1;
  incarnation_ = -1;
  start_archive_ts_ = OB_INVALID_TIMESTAMP;
  last_reconfirm_pg_tstamp_ = OB_INVALID_TIMESTAMP;
  thread_num_ = 0;
  queue_num_ = 0;
  log_wrapper_ = NULL;
  archive_round_mgr_ = NULL;
  archive_mgr_ = NULL;
  partition_service_ = NULL;
}

int ObArchivePGMgr::reset_tasks()
{
  int ret = OB_SUCCESS;

  // reset queue_
  for (int64_t i = 0; i < PG_MGR_QUEUE_SIZE; i++) {
    while (!pre_pg_queue_[i].is_empty()) {
      ObLink* link = nullptr;
      if (OB_FAIL(pre_pg_queue_[i].pop(link))) {
        ARCHIVE_LOG(ERROR, "pre_pg_queue_ pop fail", KR(ret), K(i));
      } else if (OB_ISNULL(link)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(WARN, "link is NULL", KR(ret), K(link));
      } else {
        ob_archive_free((void*)link);
      }
    }
  }
  // reset map
  pg_map_.reset();

  ARCHIVE_LOG(INFO, "after reset pg_map_", "count", pg_map_.count());

  return ret;
}

void ObArchivePGMgr::clear_archive_info()
{
  log_archive_round_ = -1;
  incarnation_ = -1;
  start_archive_ts_ = OB_INVALID_TIMESTAMP;
  last_reconfirm_pg_tstamp_ = OB_INVALID_TIMESTAMP;
}

int ObArchivePGMgr::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init", KR(ret));
  } else {
    set_thread_count(thread_num_);
    if (OB_FAIL(ObThreadPool::start())) {
      ARCHIVE_LOG(WARN, "ObArchivePGMgr start fail", KR(ret), K(thread_num_));
    } else {
      ARCHIVE_LOG(INFO, "ObArchivePGMgr start succ", K(thread_num_));
    }
  }

  return ret;
}

void ObArchivePGMgr::stop()
{
  ARCHIVE_LOG(INFO, "ObArchivePGMgr stop");
  ObThreadPool::stop();
}

void ObArchivePGMgr::wait()
{
  ARCHIVE_LOG(INFO, "ObArchivePGMgr wait");
  ObThreadPool::wait();
}

int ObArchivePGMgr::add_pg_archive_task(storage::ObIPartitionGroup* partition, bool& is_added)
{
  int ret = OB_SUCCESS;
  const bool is_add = true;
  int64_t create_ts = OB_INVALID_TIMESTAMP;
  int64_t epoch = OB_INVALID_TIMESTAMP;
  ObRole role = ObRole::INVALID_ROLE;
  int64_t takeover_ts = OB_INVALID_TIMESTAMP;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupGuard guard;
  is_added = false;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(partition)) {
    ARCHIVE_LOG(WARN, "partition is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObPGKey& pg_key = partition->get_partition_key();
    if (OB_SYS_TENANT_ID == pg_key.get_tenant_id()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "sys tenant do not need to do log archiving", KR(ret), K(pg_key));
    } else if (OB_ISNULL(pls = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "ObIPartitionLogService is NULL", KR(ret), K(pg_key));
    } else if (!pls->is_leader_active()) {
      // skip it
    } else if (OB_FAIL(pls->get_role_and_leader_epoch(role, epoch, takeover_ts))) {
      ARCHIVE_LOG(WARN, "get_role_and_leader_epoch fail", KR(ret), K(pg_key));
    } else if (!is_strong_leader(role)) {
      // not leader, skip
      ARCHIVE_LOG(DEBUG, "not leader, skip", K(pg_key));
    } else if (OB_FAIL(partition->get_create_ts(create_ts))) {
      ARCHIVE_LOG(WARN, "get_create_ts fail", KR(ret), KPC(partition));
    } else if (OB_FAIL(put_pg_archive_task_(pg_key, epoch, takeover_ts, create_ts, is_add))) {
      ARCHIVE_LOG(WARN, "put_pg_archive_task_ fail", KR(ret), K(pg_key), K(is_add));
    } else {
      is_added = true;
    }
  }

  return ret;
}

int ObArchivePGMgr::add_all_pg_on_start_archive_task(const int64_t incarnation, const int64_t archive_round)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(partition_service_) || OB_ISNULL(archive_round_mgr_) ||
             (OB_UNLIKELY(incarnation != incarnation_)) || (OB_UNLIKELY(archive_round != log_archive_round_))) {
    ARCHIVE_LOG(WARN,
        "invalid argument",
        K(partition_service_),
        K(archive_round_mgr_),
        K(archive_round),
        K(log_archive_round_),
        K(incarnation),
        K(incarnation_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObIPartitionGroupIterator* iter = NULL;
    if (OB_ISNULL(iter = partition_service_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc_pg_iter fail", KR(ret));
    } else {
      while (OB_SUCCESS == ret && !has_set_stop()) {
        ObIPartitionGroup* partition = NULL;
        bool is_added = false;
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END == ret) {
            // done
          } else {
            ARCHIVE_LOG(WARN, "iterate next partition fail", KR(ret));
          }
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          ARCHIVE_LOG(ERROR, "iterate partition fail", KR(ret), K(partition));
        } else {
          const bool is_normal_pg = !partition->get_pg_storage().is_restore();
          const ObPGKey& pg_key = partition->get_partition_key();
          // sys and restoring tenant not do archive
          if ((OB_SYS_TENANT_ID != pg_key.get_tenant_id()) && is_normal_pg) {
            if (OB_FAIL(add_pg_archive_task(partition, is_added))) {
              ARCHIVE_LOG(WARN, "add_pg_archive_task fail", KR(ret), K(pg_key));
            } else if (is_added) {
              archive_round_mgr_->inc_total_pg_count();
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        archive_round_mgr_->add_pg_finish_ = true;
        ARCHIVE_LOG(INFO, "archive_round_mgr_ total_pg_count", K(archive_round_mgr_->total_pg_count_));
      }
    }

    if (NULL != iter) {
      partition_service_->revert_pg_iter(iter);
      iter = NULL;
    }
  }

  return ret;
}

int ObArchivePGMgr::delete_pg_archive_task(storage::ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  const bool is_add = false;
  const int64_t unused_epoch = OB_INVALID_TIMESTAMP;
  const int64_t unused_takeover_ts = OB_INVALID_TIMESTAMP;
  const int64_t unused_create_ts = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init", KR(ret), K(partition));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "partition is NULL", KR(ret), K(partition));
  } else {
    const ObPGKey& pg_key = partition->get_partition_key();
    if (OB_FAIL(put_pg_archive_task_(pg_key, unused_epoch, unused_takeover_ts, unused_create_ts, is_add))) {
      ARCHIVE_LOG(WARN, "put_pg_archive_task_ fail", KR(ret), K(pg_key));
    } else {
      ARCHIVE_LOG(INFO, "delete_pg_archive_task succ", K(pg_key));
    }
  }

  return ret;
}

int ObArchivePGMgr::inner_delete_pg_archive_task(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  const bool is_add = false;
  const int64_t unused_epoch = OB_INVALID_TIMESTAMP;
  const int64_t unused_takeover_ts = OB_INVALID_TIMESTAMP;
  const int64_t unused_create_ts = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init", KR(ret), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(put_pg_archive_task_(pg_key, unused_epoch, unused_takeover_ts, unused_create_ts, is_add))) {
    ARCHIVE_LOG(WARN, "put_pg_archive_task_ fail", KR(ret), K(pg_key));
  } else {
    ARCHIVE_LOG(INFO, "inner_delete_pg_archive_task succ", K(pg_key));
  }

  return ret;
}

int ObArchivePGMgr::revert_pg_archive_task(ObPGArchiveTask* pg_archive_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (NULL != pg_archive_task) {
    pg_map_.revert(pg_archive_task);
  }

  return ret;
}

bool ObArchivePGMgr::is_prepare_pg_empty()
{
  bool bret = true;

  for (int64_t i = 0; bret && i < PG_MGR_THREAD_COUNT; i++) {
    bret = pre_pg_queue_[i].is_empty();
  }

  return bret;
}

int ObArchivePGMgr::get_pg_archive_task_guard(const ObPGKey& key, ObPGArchiveTaskGuard& guard)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTask* pg_archive_task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pg_map_.get(key, pg_archive_task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "pg_map_ get fail", KR(ret), K(key));
    }
  } else if (OB_ISNULL(pg_archive_task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "pg_archive_task is NULL", K(key), KR(ret));
  } else {
    guard.set_pg_archive_task(pg_archive_task);
  }

  return ret;
}

// get pg_archive_task guard, return OB_ENTRY_NOT_EXIST if pg_archive_task mark delete
int ObArchivePGMgr::get_pg_archive_task_guard_with_status(const ObPGKey& key, ObPGArchiveTaskGuard& guard)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTask* pg_archive_task = NULL;
  bool mark_delete = false;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pg_map_.get(key, pg_archive_task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "pg_map_ get fail", KR(ret), K(key));
    }
  } else if (OB_ISNULL(pg_archive_task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "pg_archive_task is NULL", K(key), KR(ret));
  } else if (OB_UNLIKELY(mark_delete = pg_archive_task->is_pg_mark_delete())) {
    ret = OB_ENTRY_NOT_EXIST;
    if (REACH_TIME_INTERVAL(1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "pg_archive_task mark delete, return ret OB_ENTRY_NOT_EXIST", K(key));
    }
  } else {
    guard.set_pg_archive_task(pg_archive_task);
  }

  return ret;
}

int ObArchivePGMgr::update_clog_split_progress(ObPGArchiveCLogTask* clog_task)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;
  if (OB_ISNULL(clog_task)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid clog task is NULL", KR(ret));
  } else {
    const uint64_t last_split_log_id = clog_task->get_last_log_id();
    const int64_t last_split_log_submit_ts = clog_task->get_last_log_submit_ts();
    const int64_t last_split_checkpoint_ts = clog_task->get_last_checkpoint_ts();
    const bool need_update_log_ts = clog_task->need_update_log_ts();
    const int64_t epoch_id = clog_task->epoch_id_;
    const int64_t incarnation = clog_task->incarnation_;
    const int64_t log_archive_round = clog_task->log_archive_round_;

    ObPGKey& pg_key = clog_task->pg_key_;
    if (OB_FAIL(get_pg_archive_task_guard_with_status(pg_key, guard))) {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(pg_key));
    } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "get_pg_archive_task fail", K(pg_key), KR(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == last_split_log_id || OB_INVALID_TIMESTAMP == last_split_log_submit_ts)) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(
          WARN, "invalid last split log info", K(pg_key), K(last_split_log_id), K(last_split_log_submit_ts), KR(ret));
    } else if (OB_FAIL(task->update_last_split_log_info(need_update_log_ts,
                   epoch_id,
                   incarnation,
                   log_archive_round,
                   last_split_log_id,
                   last_split_log_submit_ts,
                   last_split_checkpoint_ts))) {
      ARCHIVE_LOG(WARN, "failed to update_last_split_log_info", K(pg_key), "clog_task", *clog_task, KR(ret));
    } else { /*do nothing*/
    }
  }

  return ret;
}

int ObArchivePGMgr::get_clog_split_info(const ObPGKey& key, const int64_t epoch, const int64_t incarnation,
    const int64_t round, uint64_t& last_split_log_id, int64_t& last_split_log_ts, int64_t& last_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;

  if (OB_FAIL(get_pg_archive_task_guard_with_status(key, guard))) {
    ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(key));
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ARCHIVE_LOG(ERROR, "get_pg_archive_task fail", K(key));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(task->get_last_split_log_info(
                 epoch, incarnation, round, last_split_log_id, last_split_log_ts, last_checkpoint_ts))) {
    ARCHIVE_LOG(WARN, "get_last_split_log_info fail", KR(ret), K(key), K(epoch), K(incarnation), K(round), KPC(task));
  }

  return ret;
}

int ObArchivePGMgr::set_archive_round_info(const int64_t round, const int64_t incarnation)
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

int ObArchivePGMgr::set_server_start_archive_ts(const int64_t start_archive_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_archive_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(start_archive_ts));
  } else {
    start_archive_ts_ = start_archive_ts;
  }

  return ret;
}

int ObArchivePGMgr::mark_fatal_error(
    const ObPGKey& pg_key, const int64_t epoch_id, const int64_t incarnation, const int64_t log_archive_round)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;

  archive_mgr_->mark_encounter_fatal_err(pg_key, incarnation, log_archive_round);
  ARCHIVE_LOG(ERROR, "mark fatal error", K(pg_key), K(epoch_id), K(incarnation), K(log_archive_round));
  if (OB_FAIL(get_pg_archive_task_guard_with_status(pg_key, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard_with_status fail", KR(ret), K(pg_key));
    } else {
      ret = OB_SUCCESS;
      ARCHIVE_LOG(WARN, "pg leader may be revoked", KR(ret), K(pg_key));
    }
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(
                 task->get_pg_incarnation() != incarnation || task->get_pg_archive_round() != log_archive_round)) {
    // all tasks should be removed when previous archive round stop
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "get_pg_archive_task fail", K(pg_key));
  } else if (OB_FAIL(task->set_encount_fatal_error(epoch_id, incarnation, log_archive_round))) {
    ARCHIVE_LOG(
        WARN, "set_encount_fatal_error fail", KR(ret), K(pg_key), K(epoch_id), K(incarnation), K(log_archive_round));
  }

  return ret;
}

int ObArchivePGMgr::check_if_task_expired(
    const ObPGKey& pg_key, const int64_t incarnation, const int64_t log_archive_round, bool& is_expired)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(archive_mgr_)) {
    ARCHIVE_LOG(WARN, "ObArchiveMgr is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (!archive_mgr_->is_in_archive_status()) {
    is_expired = true;
  } else {
    ObPGArchiveTaskGuard guard(this);
    ObPGArchiveTask* task = NULL;
    if (OB_FAIL(get_pg_archive_task_guard_with_status(pg_key, guard))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        ARCHIVE_LOG(WARN, "get_pg_archive_task_guard fail", KR(ret), K(pg_key));
      } else {
        is_expired = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid task is NULL", KR(ret), K(pg_key));
    } else if (OB_UNLIKELY(
                   task->get_pg_incarnation() != incarnation || task->get_pg_archive_round() != log_archive_round)) {
      // all tasks should be removed when previous archive round stop
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "invalid task", K(pg_key), "task", *task);
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObArchivePGMgr::get_archive_pg_map(PGArchiveMap*& map)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init", KR(ret));
  } else {
    map = &pg_map_;
  }

  return ret;
}
//=============================== end of public functions ========================//

//============================== start of private functions ========================//

// put add/gc task into queue_
int ObArchivePGMgr::put_pg_archive_task_(const ObPGKey& pg_key, const int64_t epoch, const int64_t takeover_ts,
    const int64_t create_timestamp, const bool is_add)
{
  int ret = OB_SUCCESS;
  PreArchiveLinkedPGKey* link = NULL;
  void* data = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(archive_mgr_)) {
    ARCHIVE_LOG(WARN, "ObArchiveMgr is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(NULL == (data = ob_archive_malloc(sizeof(PreArchiveLinkedPGKey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc PreArchiveLinkedPGKey fail", KR(ret), K(pg_key));
  } else {
    link = new (data) PreArchiveLinkedPGKey(pg_key, epoch, takeover_ts, create_timestamp, is_add);
    if (OB_FAIL(push_pre_task_(pg_key, link))) {
      ARCHIVE_LOG(WARN, "push_pre_task_ fail", KR(ret), K(pg_key));
    } else {
      ARCHIVE_LOG(
          INFO, "success to put_pg_archive_task", K(pg_key), K(epoch), K(takeover_ts), K(create_timestamp), K(is_add));
    }
  }

  if (OB_SUCCESS != ret && data != NULL) {
    ob_archive_free(data);
    link = NULL;
    data = NULL;
  }

  return ret;
}

int ObArchivePGMgr::check_pg_task_exist_(const ObPGKey& pg_key, bool& pg_exist)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;
  pg_exist = false;

  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key));
  } else if (OB_FAIL(get_pg_archive_task_guard(pg_key, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard fail", KR(ret), K(pg_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "get_pg_archive_task is NULL", KR(ret), K(pg_key), K(task));
  } else {
    pg_exist = true;
  }

  return ret;
}

void ObArchivePGMgr::run1()
{
  ARCHIVE_LOG(INFO, "ObArchivePGMgr thread run");

  lib::set_thread_name("ArchivePGMgr");

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
  } else {
    while (!has_set_stop()) {
      do_thread_task_();
    }
  }
}

int64_t ObArchivePGMgr::thread_index_()
{
  // thread_counter_ is used to get index for every thread
  static RLOCAL(int64_t, index);
  if (index == 0) {
    index = ATOMIC_AAF(&thread_counter_, 1);
  }
  return index - 1;
}

// 1. add pg task if needed
// 2. handle add/delete pg task
// 3. check if archive round start
// 4. delete pg task if needed
void ObArchivePGMgr::do_thread_task_()
{
  int64_t begin_tstamp = ObTimeUtility::current_time();
  bool need_confirm = need_confirm_pg_();
  if (need_confirm) {
    (void)reconfirm_pg_add_();
  }

  if (need_dispatch_pg_()) {
    do_dispatch_pg_();
  }

  if (need_check_start_archive_()) {
    handle_check_start_archive_round_();
  }

  if (need_confirm) {
    (void)reconfirm_pg_delete_();
  }

  int64_t end_tstamp = ObTimeUtility::current_time();
  int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
  if (wait_interval > 0) {
    usleep(wait_interval);
  }
}

bool ObArchivePGMgr::need_dispatch_pg_()
{
  bool bret = false;
  const int64_t thread_index = thread_index_();

  if (OB_ISNULL(archive_mgr_)) {
    ARCHIVE_LOG(ERROR, "archive_mgr_ is NULL");
  } else if (0 != thread_index && archive_mgr_->is_in_archive_status()) {
    bret = true;
  }

  return bret;
}

bool ObArchivePGMgr::need_confirm_pg_()
{
  int64_t tstamp = ObTimeUtility::current_time();
  const int64_t thread_index = thread_index_();

  return (archive_mgr_->is_in_archive_doing_status() && tstamp - last_reconfirm_pg_tstamp_ > RECONFIRM_PG_INTERVAL &&
          0 == thread_index);
}

bool ObArchivePGMgr::need_check_start_archive_()
{
  const int64_t thread_index = thread_index_();
  return 0 == thread_index && archive_mgr_->is_in_archive_beginning_status();
}

void ObArchivePGMgr::handle_check_start_archive_round_()
{
  int ret = OB_SUCCESS;
  if (!archive_mgr_->is_in_archive_status()) {
    ARCHIVE_LOG(WARN, "not in archive mode, skip");
  } else {
    const bool add_pg_finish = archive_round_mgr_->get_add_pg_finish_flag();
    if (add_pg_finish && is_prepare_pg_empty()) {
      CheckArchiveRoundStartFunctor functor(log_archive_round_, incarnation_);
      if (OB_FAIL(pg_map_.for_each(functor))) {
        ARCHIVE_LOG(WARN, "for_each fail", KR(ret), K(log_archive_round_), K(incarnation_));
      } else if (!functor.return_start_flag()) {
        ARCHIVE_LOG(INFO, "return_start_flag is false, do dothing");
      } else {
        notify_start_archive_round_succ_();
        ARCHIVE_LOG(INFO, "notify_start_archive_round_succ_");
      }
    } else {
      ARCHIVE_LOG(
          DEBUG, "can not entry into doing period so far", K(add_pg_finish), "is_quque_empty", is_prepare_pg_empty());
    }
  }
}

void ObArchivePGMgr::notify_start_archive_round_succ_()
{
  DEBUG_SYNC(NOTIFY_START_ARCHIVE_SUCC);
  archive_mgr_->notify_all_archive_round_started();
  last_reconfirm_pg_tstamp_ = ObTimeUtility::current_time();
}

void ObArchivePGMgr::do_dispatch_pg_()
{
  int ret = OB_SUCCESS;

  if ((OB_ISNULL(archive_mgr_))) {
    ARCHIVE_LOG(ERROR, "archive_mgr_ is NULL");
  } else {
    while (OB_SUCC(ret) && !has_set_stop() && archive_mgr_->is_in_archive_status() && !is_pre_task_empty_()) {
      ObLink* link = nullptr;
      PreArchiveLinkedPGKey* link_pg_key = nullptr;

      if (OB_FAIL(pop_pre_task_(link))) {
        ARCHIVE_LOG(WARN, "pop_pre_task_ fail", KR(ret));
      } else if (OB_ISNULL(link)) {
        ARCHIVE_LOG(WARN, "link is NULL");
        ret = OB_ERR_UNEXPECTED;
      } else {
        link_pg_key = static_cast<PreArchiveLinkedPGKey*>(link);
        const ObPGKey pg_key = link_pg_key->pg_key_;
        const bool is_add_task = link_pg_key->type_;
        const int64_t timestamp = link_pg_key->create_timestamp_;
        const int64_t epoch = link_pg_key->epoch_;
        const int64_t takeover_ts = link_pg_key->takeover_ts_;
        if (is_add_task) {
          if (OB_FAIL(handle_add_task_(pg_key, timestamp, epoch, takeover_ts))) {
            ARCHIVE_LOG(WARN, "handle_add_task_ fail", KR(ret), K(pg_key), K(timestamp), K(epoch));
          }
        } else {
          if (OB_FAIL(handle_gc_task_(pg_key))) {
            ARCHIVE_LOG(WARN, "handle_gc_task_ fail", KR(ret), K(pg_key));
          }
        }

        // push back all failed pg task, pg not satisfied can be removed in next retry
        if (OB_SUCC(ret)) {
          link_pg_key->~PreArchiveLinkedPGKey();
          ob_archive_free((void*)link_pg_key);
          link_pg_key = NULL;
        } else {
          (void)push_pre_task_(pg_key, link_pg_key);
          usleep(100 * 1000L);
          // overwrite ret
          ret = OB_SUCCESS;
        }
      }
    }  // while
  }    // else
}

// handle add pg archive task
//
// Include these steps:
//   0) check pg can do archive task
//   1)locate inital log id and ilog file id
//   2)query next archived index file and data file id
//   3) write kickoff log
//   4) push pg to ilog_fetch queue
int ObArchivePGMgr::handle_add_task_(
    const ObPGKey& pg_key, const int64_t create_timestamp, const int64_t leader_epoch, const int64_t leader_takeover_ts)
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  ObPGArchiveTask* task = NULL;
  bool pg_exist = false;
  bool compatible = false;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == create_timestamp) ||
      OB_UNLIKELY(OB_INVALID_TIMESTAMP == leader_epoch) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == leader_takeover_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(pg_key), K(create_timestamp), K(leader_epoch));
  } else if (OB_FAIL(check_is_leader(pg_key, leader_epoch, is_leader))) {
    ARCHIVE_LOG(WARN, "check_is_leader fail", KR(ret), K(pg_key));
  } else if (!is_leader) {
    // skip
    ARCHIVE_LOG(DEBUG, "add_archive_task with different epoch", K(pg_key), K(leader_epoch));
  } else if (OB_FAIL(check_active_pg_archive_task_exist_(pg_key, leader_epoch, pg_exist))) {
    ARCHIVE_LOG(WARN, "check_active_pg_archive_task_exist_ fail", KR(ret), K(pg_key), K(leader_epoch));
  } else if (pg_exist) {
    // skip
    ARCHIVE_LOG(INFO, "pg archive task already exist, skip it", K(pg_key));
  } else if (OB_UNLIKELY(!get_and_check_compatible_(compatible))) {
    ARCHIVE_LOG(INFO, "get and check compatible not pass, skip it", K(pg_key), K(leader_epoch));
  } else {
    ObString uri(archive_round_mgr_->root_path_);
    ObString storage_info(archive_round_mgr_->storage_info_);
    StartArchiveHelper start_archive_helper(pg_key,
        create_timestamp,
        incarnation_,
        log_archive_round_,
        leader_epoch,
        leader_takeover_ts,
        compatible,
        start_archive_ts_,
        uri,
        storage_info,
        *log_wrapper_,
        *archive_mgr_);
    if (OB_FAIL(start_archive_helper.handle())) {
      ARCHIVE_LOG(WARN, "start_archive_helper handle fail", KR(ret), K(start_archive_helper));
    } else if (OB_FAIL(insert_or_update_pg_(start_archive_helper, task))) {
      ARCHIVE_LOG(WARN, "insert_or_update_pg_ fail", KR(ret), K(pg_key), K(start_archive_helper));
    } else if (OB_FAIL(record_for_residual_data_file_(start_archive_helper, task))) {
      ARCHIVE_LOG(WARN, "record_for_residual_data_file_ fail", KR(ret), K(start_archive_helper));
    } else if (OB_FAIL(generate_and_submit_first_log_(start_archive_helper))) {
      ARCHIVE_LOG(WARN, "generate_and_submit_first_log_ fail", KR(ret), K(start_archive_helper));
    } else if (OB_FAIL(add_pg_to_ilog_fetch_queue_(start_archive_helper))) {
      ARCHIVE_LOG(WARN, "add_pg_to_ilog_fetch_queue_ fail", KR(ret), K(start_archive_helper));
    } else {
      archive_round_mgr_->inc_started_pg();
      ARCHIVE_LOG(INFO, "add_pg_task succ", K(start_archive_helper), KPC(task));
    }
  }

  return ret;
}

int ObArchivePGMgr::check_active_pg_archive_task_exist_(const ObPGKey& pg_key, const int64_t epoch, bool& exist)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;
  int64_t task_epoch = OB_INVALID_TIMESTAMP;
  exist = false;

  if (OB_FAIL(get_pg_archive_task_guard(pg_key, guard)) && OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_pg_archive_task_guard fail", KR(ret), K(pg_key), K(epoch));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    exist = false;
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(pg_key), K(epoch));
  } else if (epoch == (task_epoch = task->get_pg_leader_epoch())) {
    exist = true;
  } else if (epoch < task_epoch) {
    exist = true;
    ARCHIVE_LOG(INFO, "stale add_archive_task", K(epoch), KPC(task));
  } else {
    exist = false;
    ARCHIVE_LOG(DEBUG, "already exist pg archive task, skip it", K(epoch), KPC(task));
  }

  return ret;
}

bool ObArchivePGMgr::get_and_check_compatible_(bool& compatible)
{
  bool bret = true;
  int64_t incarnation = -1;
  int64_t round = -1;

  archive_round_mgr_->get_archive_round_compatible(incarnation, round, compatible);
  if (OB_UNLIKELY(incarnation != incarnation_ || log_archive_round_ != round)) {
    bret = false;
    ARCHIVE_LOG(WARN,
        "diff archive incarnation or round, skip it",
        K(incarnation),
        K(round),
        K(incarnation_),
        K(log_archive_round_));
  }

  return bret;
}

int ObArchivePGMgr::record_for_residual_data_file_(StartArchiveHelper& helper, ObPGArchiveTask* task)
{
  int ret = OB_SUCCESS;
  ObArchiveSender* sender = NULL;

  if (OB_ISNULL(archive_mgr_) || OB_ISNULL(sender = archive_mgr_->get_sender()) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(archive_mgr_), K(sender), K(task));
  } else if (!helper.data_file_exist_unrecorded_) {
    // skip it
  } else if (OB_FAIL(sender->record_index_info(*task, helper.epoch_, helper.incarnation_, helper.archive_round_))) {
    ARCHIVE_LOG(WARN, "record_index_info fail", KR(ret), K(helper), KPC(task));
  } else if (OB_FAIL(task->switch_archive_file(
                 helper.epoch_, helper.incarnation_, helper.archive_round_, LOG_ARCHIVE_FILE_TYPE_DATA))) {
    ARCHIVE_LOG(WARN, "switch data file fail", KR(ret), K(helper), KPC(task));
  }

  if (OB_SUCC(ret) && !helper.need_kickoff_log_) {
    task->mark_pg_first_record_finish(helper.epoch_, incarnation_, log_archive_round_);
  }

  return ret;
}

int ObArchivePGMgr::add_pg_to_ilog_fetch_queue_(StartArchiveHelper& helper)
{
  int ret = OB_SUCCESS;
  const ObPGKey& pg_key = helper.pg_key_;
  const uint64_t start_log_id = helper.start_log_id_;
  const file_id_t ilog_file_id = helper.start_ilog_file_id_;
  PGFetchTask task;
  task.pg_key_ = helper.pg_key_;
  task.incarnation_ = helper.incarnation_;
  task.archive_round_ = helper.archive_round_;
  task.epoch_ = helper.epoch_;
  task.start_log_id_ = helper.start_log_id_;
  task.ilog_file_id_ = helper.start_ilog_file_id_;
  ObArchiveIlogFetchTaskMgr* ilog_fetch_task_mgr = NULL;

  if (OB_ISNULL(archive_mgr_) || OB_ISNULL(ilog_fetch_task_mgr = &archive_mgr_->ilog_fetch_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ilog_fetch_task_mgr get fail", KR(ret), K(archive_mgr_), K(ilog_fetch_task_mgr));
  } else if (OB_FAIL(ilog_fetch_task_mgr->add_ilog_fetch_task(task))) {
    ARCHIVE_LOG(WARN, "add_ilog_fetch_task fail", KR(ret), K(pg_key), K(start_log_id), K(ilog_file_id));
  }

  return ret;
}

int ObArchivePGMgr::generate_and_submit_first_log_(StartArchiveHelper& helper)
{
  int ret = OB_SUCCESS;
  ObArchiveIlogFetcher* ilog_fetcher = NULL;

  if (OB_ISNULL(archive_mgr_) || OB_ISNULL(ilog_fetcher = &archive_mgr_->ilog_fetcher_) ||
      OB_UNLIKELY(!helper.is_valid())) {
    ARCHIVE_LOG(WARN, "invalid argument", K(archive_mgr_), K(ilog_fetcher), K(helper));
    ret = OB_INVALID_ARGUMENT;
  } else if (!helper.need_kickoff_log_) {
    // skip it
  } else if (OB_FAIL(ilog_fetcher->generate_and_submit_pg_first_log(helper))) {
    ARCHIVE_LOG(WARN, "generate_and_submit_pg_first_log fail", KR(ret), K(helper));
  }

  return OB_SUCCESS;
}

int ObArchivePGMgr::handle_gc_task_(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObPGArchiveTaskGuard guard(this);
  ObPGArchiveTask* task = NULL;
  bool is_leader = false;

  if (OB_ISNULL(archive_round_mgr_)) {
    ARCHIVE_LOG(ERROR, "archive_round_mgr_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_pg_archive_task_guard(pg_key, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ARCHIVE_LOG(DEBUG, "pg archive task already delete", KR(ret), K(pg_key));
      ret = OB_SUCCESS;
    } else {
      ARCHIVE_LOG(WARN, "get pg archive task fail", KR(ret), K(pg_key));
    }
  } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
    ARCHIVE_LOG(WARN, "task is NULL", K(pg_key), K(guard), K(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(check_is_leader(pg_key, task->get_pg_leader_epoch(), is_leader))) {
    ARCHIVE_LOG(WARN, "check_is_leader fail", KR(ret), KPC(task));
  } else if (!is_leader) {
    if (OB_FAIL(remove_pg_(pg_key))) {
      ARCHIVE_LOG(WARN, "remove_pg_ fail", KR(ret), K(pg_key));
    } else {
      ARCHIVE_LOG(INFO, "gc pg_archive_task succ", K(pg_key));
    }
  } else {
    ARCHIVE_LOG(DEBUG, "pg is still leader, skip it", KPC(task));
  }

  return ret;
}

int ObArchivePGMgr::insert_or_update_pg_(StartArchiveHelper& helper, ObPGArchiveTask*& pg_task)
{
  int ret = OB_SUCCESS;
  const ObPGKey pg_key = helper.pg_key_;

  if (OB_UNLIKELY(!helper.is_valid())) {
    ARCHIVE_LOG(WARN, "helper is not valid", K(helper));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ENTRY_EXIST == (ret = pg_map_.contains_key(pg_key))) {
    // update pg task info
    ret = OB_SUCCESS;
    ObPGArchiveTaskGuard guard(this);
    ObPGArchiveTask* task = NULL;
    if (OB_FAIL(get_pg_archive_task_guard(pg_key, guard))) {
      ARCHIVE_LOG(WARN, "get_pg_archive_task_guard fail", KR(ret), K(pg_key));
    } else if (OB_ISNULL(task = guard.get_pg_archive_task())) {
      ARCHIVE_LOG(ERROR, "get_pg_archive_task is NULL", K(pg_key));
      ret = OB_ERR_UNEXPECTED;
    } else {
      task->update_pg_archive_task_on_new_start(helper);
      pg_task = task;
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObPGArchiveTask* pg_archive_task = NULL;
    // const bool mandatory = true;
    if (OB_FAIL(pg_map_.alloc_value(pg_archive_task))) {
      ARCHIVE_LOG(WARN, "alloc_value fail", K(pg_key));
    } else if (OB_ISNULL(pg_archive_task)) {
      ARCHIVE_LOG(WARN, "pg_archive_task is NULL", K(pg_key));
      ret = OB_ERR_UNEXPECTED;
    } else {
      pg_task = pg_archive_task;
      if (OB_FAIL(pg_archive_task->init(helper, allocator_))) {
        ARCHIVE_LOG(WARN, "pg_archive_task init fail", KR(ret), K(helper));
      } else if (OB_FAIL(pg_map_.insert_and_get(pg_key, pg_archive_task))) {
        ARCHIVE_LOG(WARN, "pg_map_ insert_and_get fail", KR(ret), K(pg_key), KPC(pg_archive_task));
      } else {
        // revert pg
        (void)revert_pg_archive_task(pg_archive_task);
      }
    }

    if (OB_FAIL(ret) && NULL != pg_archive_task) {
      (void)pg_map_.del(pg_key);
      pg_map_.free_value(pg_archive_task);
      pg_archive_task->~ObPGArchiveTask();
      pg_archive_task = NULL;
    }
  } else {
    ARCHIVE_LOG(WARN, "pg_map_ contains_key fail", KR(ret), K(pg_key));
  }

  return ret;
}

int ObArchivePGMgr::remove_pg_(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchivePGMgr not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pg_map_.del(pg_key))) {
    ARCHIVE_LOG(WARN, "pg_map_ del fail", KR(ret), K(pg_key));
  } else {
    ARCHIVE_LOG(DEBUG, "remove pg succ", K(pg_key));
  }

  return ret;
}

int ObArchivePGMgr::reconfirm_pg_add_()
{
  int ret = OB_SUCCESS;
  int64_t pg_count = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init", KR(ret));
  } else if (OB_ISNULL(archive_mgr_) || OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(archive_mgr_), K(partition_service_));
  } else {
    ObIPartitionGroupIterator* iter = NULL;
    if (OB_ISNULL(iter = partition_service_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc_pg_iter fail", KR(ret));
    } else {
      while (OB_SUCCESS == ret && !has_set_stop()) {
        ObIPartitionGroup* partition = NULL;
        bool pg_exist = false;
        bool is_added = false;
        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END == ret) {
            // done
          } else {
            ARCHIVE_LOG(WARN, "iterate next partition fail", KR(ret));
          }
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          ARCHIVE_LOG(ERROR, "iterate partition fail", KR(ret), K(partition));
        } else {
          const ObPGKey& pg_key = partition->get_partition_key();
          const bool is_normal_pg = !partition->get_pg_storage().is_restore();
          // sys tenant and restoring tenant can not start archive
          if ((OB_SYS_TENANT_ID != pg_key.get_tenant_id()) && is_normal_pg) {
            if (OB_FAIL(check_pg_task_exist_(partition->get_partition_key(), pg_exist))) {
              ARCHIVE_LOG(WARN, "check_pg_task_exist_ fail", KR(ret), KPC(partition));
            } else if (pg_exist) {
              // pg task exist, skip
            } else if (OB_FAIL(add_pg_archive_task(partition, is_added))) {
              ARCHIVE_LOG(WARN, "add_pg_archive_task fail", KR(ret), K(pg_key));
            } else if (is_added) {
              pg_count++;
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (NULL != iter) {
      partition_service_->revert_pg_iter(iter);
      iter = NULL;
    }
  }

  ARCHIVE_LOG(INFO, "reconfirm_pg_add_ pg_count", K(pg_count));

  return ret;
}

int ObArchivePGMgr::reconfirm_pg_delete_()
{
  int ret = OB_SUCCESS;
  CheckDeletePGFunctor functor(this);

  if (OB_FAIL(pg_map_.for_each(functor))) {
    ARCHIVE_LOG(WARN, "pg_map for_each fail", KR(ret));
  }

  last_reconfirm_pg_tstamp_ = ObTimeUtility::current_time();

  return ret;
}

bool ObArchivePGMgr::is_pre_task_empty_()
{
  const int64_t thread_index = thread_index_();
  return pre_pg_queue_[thread_index - 1].is_empty();
}

int ObArchivePGMgr::pop_pre_task_(ObLink*& link)
{
  const int64_t thread_index = thread_index_();
  return pre_pg_queue_[thread_index - 1].pop(link);
}

int ObArchivePGMgr::push_pre_task_(const ObPGKey& pg_key, ObLink* link)
{
  int ret = OB_SUCCESS;
  if (0 == queue_num_) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchivePGMgr not init", KR(ret));
  } else {
    const uint64_t queue_index = pg_key.hash() % queue_num_;
    ret = pre_pg_queue_[queue_index].push(link);
  }
  return ret;
}

//==================== end of private_functions ==================//
//====================== end of ObArchivePGMgr =====================//

//===================== PreArchiveLinkedPGKey =====================//
PreArchiveLinkedPGKey::PreArchiveLinkedPGKey(const ObPGKey& pg_key, const int64_t epoch, const int64_t takeover_ts,
    const int64_t create_timestamp, const bool is_add)
{
  pg_key_ = pg_key;
  type_ = is_add;
  epoch_ = epoch;
  takeover_ts_ = takeover_ts;
  create_timestamp_ = create_timestamp;
  retry_times_ = 0;
}

void PreArchiveLinkedPGKey::reset()
{
  pg_key_.reset();
  type_ = false;
  epoch_ = 0;
  takeover_ts_ = OB_INVALID_TIMESTAMP;
  create_timestamp_ = OB_INVALID_TIMESTAMP;
  retry_times_ = 0;
}

}  // namespace archive
}  // namespace oceanbase
