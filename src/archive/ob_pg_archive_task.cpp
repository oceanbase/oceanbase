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

#include "ob_pg_archive_task.h"
#include "ob_archive_pg_mgr.h"
#include "ob_archive_mgr.h"
#include "clog/ob_log_define.h"
#include "ob_archive_thread_pool.h"
#include "observer/virtual_table/ob_all_virtual_pg_log_archive_stat.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;

ObPGArchiveTask::ObPGArchiveTask()
    : pg_been_deleted_(false),
      is_first_record_finish_(false),
      has_encount_error_(false),
      incarnation_(-1),
      archive_round_(-1),
      epoch_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      current_ilog_id_(OB_INVALID_FILE_ID),
      max_log_id_(OB_INVALID_ID),
      round_start_info_(),
      start_log_id_(OB_INVALID_ID),
      fetcher_max_split_log_id_(OB_INVALID_ID),
      last_split_log_id_(OB_INVALID_ID),
      last_split_log_submit_ts_(OB_INVALID_TIMESTAMP),
      last_split_checkpoint_ts_(OB_INVALID_TIMESTAMP),
      archived_log_id_(OB_INVALID_ID),
      archived_log_timestamp_(OB_INVALID_TIMESTAMP),
      archived_checkpoint_ts_(OB_INVALID_TIMESTAMP),
      archived_clog_epoch_id_(OB_INVALID_TIMESTAMP),
      archived_accum_checksum_(0),
      mandatory_(false),
      archive_destination_(),
      pg_key_(),
      send_task_queue_(NULL),
      clog_task_queue_(NULL),
      allocator_(NULL),
      rwlock_()
{}

ObPGArchiveTask::~ObPGArchiveTask()
{
  ARCHIVE_LOG(DEBUG, "ObPGArchiveTask delete", K(pg_key_));
  destroy();
}

int ObPGArchiveTask::init(StartArchiveHelper& helper, ObArchiveAllocator* allocator)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!helper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(helper));
  } else {
    update_unlock_(helper);
    allocator_ = allocator;
  }

  return ret;
}

void ObPGArchiveTask::destroy()
{
  WLockGuard guard(rwlock_);

  pg_been_deleted_ = false;
  is_first_record_finish_ = false;
  archive_round_ = 0;
  epoch_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  current_ilog_id_ = OB_INVALID_FILE_ID;
  max_log_id_ = OB_INVALID_ID;

  round_start_info_.reset();

  start_log_id_ = OB_INVALID_ID;
  fetcher_max_split_log_id_ = OB_INVALID_ID;
  last_split_log_id_ = OB_INVALID_ID;
  last_split_log_submit_ts_ = OB_INVALID_TIMESTAMP;
  last_split_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  archived_log_id_ = OB_INVALID_ID;
  archived_log_timestamp_ = OB_INVALID_TIMESTAMP;
  archived_checkpoint_ts_ = OB_INVALID_TIMESTAMP;
  archived_clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  archived_accum_checksum_ = 0;

  free_task_status_();
  allocator_ = NULL;
  mandatory_ = false;
  pg_key_.reset();
}

void ObPGArchiveTask::get_pg_log_archive_status(clog::ObPGLogArchiveStatus& status, int64_t& epoch)
{
  RLockGuard guard(rwlock_);

  if (has_encount_error_) {
    status.status_ = share::ObLogArchiveStatus::INTERRUPTED;
  } else {
    status.status_ = share::ObLogArchiveStatus::DOING;
  }

  status.round_start_ts_ = round_start_info_.start_ts_;
  status.round_start_log_id_ = round_start_info_.start_log_id_;
  status.round_snapshot_version_ = round_start_info_.snapshot_version_;
  status.round_log_submit_ts_ = round_start_info_.log_submit_ts_;
  status.round_clog_epoch_id_ = round_start_info_.clog_epoch_id_;
  status.round_accum_checksum_ = round_start_info_.accum_checksum_;

  status.archive_incarnation_ = incarnation_;
  status.log_archive_round_ = archive_round_;
  status.last_archived_log_id_ = archived_log_id_;
  status.last_archived_log_submit_ts_ = archived_log_timestamp_;
  status.last_archived_checkpoint_ts_ = archived_checkpoint_ts_;

  status.clog_epoch_id_ = archived_clog_epoch_id_;
  status.accum_checksum_ = archived_accum_checksum_;

  epoch = epoch_;
}

int ObPGArchiveTask::get_last_split_log_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
    uint64_t& last_split_log_id, int64_t& last_split_log_submit_ts, int64_t& last_split_checkpoint_ts)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else {
    last_split_log_id = last_split_log_id_;
    last_split_log_submit_ts = last_split_log_submit_ts_;
    last_split_checkpoint_ts = last_split_checkpoint_ts_;
  }

  return ret;
}

int ObPGArchiveTask::get_max_archived_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
    uint64_t& max_log_id, int64_t& max_log_ts, int64_t& checkpoint_ts, int64_t& clog_epoch_id, int64_t& accum_checksum)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else {
    max_log_id = archived_log_id_;
    max_log_ts = archived_log_timestamp_;
    checkpoint_ts = archived_checkpoint_ts_;
    clog_epoch_id = archived_clog_epoch_id_;
    accum_checksum = archived_accum_checksum_;
  }

  return ret;
}

int ObPGArchiveTask::get_fetcher_max_split_log_id(
    const int64_t epoch, const int64_t incarnation, const int64_t round, uint64_t& fetcher_max_split_id)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else {
    fetcher_max_split_id = fetcher_max_split_log_id_;
  }

  return ret;
}

int ObPGArchiveTask::build_data_file_index_record(
    const int64_t epoch, const uint64_t incarnation, const uint64_t round, ObArchiveIndexFileInfo& info)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (!archive_destination_.is_data_file_valid_) {
    if (OB_FAIL(info.build_invalid_record(archive_destination_.cur_data_file_id_,
            archived_log_id_,
            archived_checkpoint_ts_,
            archived_log_timestamp_,
            archived_clog_epoch_id_,
            archived_accum_checksum_,
            round_start_info_))) {
      ARCHIVE_LOG(WARN, "build_invalid_record fail", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(info.build_valid_record(archive_destination_.cur_data_file_id_,
                 archive_destination_.data_file_min_log_id_,
                 archive_destination_.data_file_min_log_ts_,
                 archived_log_id_,
                 archived_checkpoint_ts_,
                 archived_log_timestamp_,
                 archived_clog_epoch_id_,
                 archived_accum_checksum_,
                 round_start_info_))) {
    ARCHIVE_LOG(WARN, "build_valid_record fail", KR(ret), KPC(this));
  }

  return ret;
}

int ObPGArchiveTask::get_current_file_info(const int64_t epoch, const int64_t incarnation, const int64_t round,
    const LogArchiveFileType type, bool& compatible, int64_t& offset, const int64_t path_len, char* file_path)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.get_file_info(
                 pg_key_, type, incarnation_, archive_round_, compatible, offset, path_len, file_path))) {
    ARCHIVE_LOG(WARN, "get_file_info fail", KR(ret), KPC(this));
  }

  return ret;
}

int ObPGArchiveTask::get_current_file_offset(const int64_t epoch, const int64_t incarnation, const int64_t round,
    const LogArchiveFileType type, int64_t& offset, bool& force_switch_flag)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.get_file_offset(type, offset, force_switch_flag))) {
    ARCHIVE_LOG(WARN, "get_file_offset fail", KR(ret), KPC(this));
  }

  return ret;
}

int ObPGArchiveTask::get_current_data_file_min_log_info(const int64_t epoch, const int64_t incarnation,
    const int64_t round, uint64_t& min_log_id, int64_t& min_log_ts, bool& is_valid)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.get_data_file_min_log_info(min_log_id, min_log_ts, is_valid))) {
    ARCHIVE_LOG(WARN, "get_data_file_min_log_info fail", KR(ret), KPC(this));
  }

  return ret;
}

int ObPGArchiveTask::update_pg_archive_progress(const bool need_update_log_ts, const int64_t epoch,
    const int64_t incarnation, const int64_t archive_round, const uint64_t archived_log_id, const int64_t tstamp,
    const int64_t checkpoint_ts, const int64_t clog_epoch_id, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  // checkpoint_ts may be 0 when no checkpoint log in clogs

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch <= 0 || incarnation <= 0 || archive_round <= 0 || OB_INVALID_ID == archived_log_id ||
                  clog_epoch_id < 0 || tstamp <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(pg_key_),
        K(need_update_log_ts),
        K(epoch),
        K(incarnation),
        K(archive_round),
        K(archived_log_id),
        K(tstamp),
        K(checkpoint_ts),
        K(clog_epoch_id),
        K(accum_checksum),
        K(archived_log_id_),
        K(archived_log_timestamp_),
        K(checkpoint_ts));
  } else if (pg_been_deleted_ || epoch < epoch_) {
    // do nothing
  } else if (epoch != epoch_ || incarnation != incarnation_ || archive_round != archive_round_) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        KPC(this),
        K(need_update_log_ts),
        K(epoch),
        K(incarnation),
        K(archive_round),
        K(archived_log_id),
        K(tstamp),
        K(checkpoint_ts));
  } else if (OB_UNLIKELY(archived_log_id < archived_log_id_ ||
                         (need_update_log_ts && tstamp <= archived_log_timestamp_) ||
                         clog_epoch_id < archived_clog_epoch_id_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR,
        "invalid archive progress info",
        KR(ret),
        KPC(this),
        K(need_update_log_ts),
        K(epoch),
        K(incarnation),
        K(archive_round),
        K(archived_log_id),
        K(clog_epoch_id),
        K(tstamp),
        K(checkpoint_ts));
  } else {
    archived_log_id_ = archived_log_id;
    if (need_update_log_ts) {
      archived_log_timestamp_ = tstamp;
    }

    if (checkpoint_ts > archived_checkpoint_ts_) {
      archived_checkpoint_ts_ = checkpoint_ts;
    }

    archived_clog_epoch_id_ = clog_epoch_id;
    archived_accum_checksum_ = accum_checksum;
  }

  return ret;
}

int ObPGArchiveTask::update_last_split_log_info(const bool need_update_log_ts, const int64_t epoch_id,
    const int64_t incarnation, const int64_t log_archive_round, const uint64_t log_id, const int64_t log_ts,
    const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch_id <= 0 || incarnation <= 0 || log_archive_round <= 0 || OB_INVALID_ID == log_id ||
                  OB_INVALID_TIMESTAMP == log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR,
        "invalid arguments",
        KR(ret),
        K(epoch_id),
        K(incarnation),
        K(log_archive_round),
        K(log_id),
        K(log_ts),
        K(pg_key_));
    ret = OB_ERR_UNEXPECTED;
  } else if (pg_been_deleted_ || epoch_id < epoch_) {
    // do nothing
  } else if (epoch_ != epoch_id || incarnation_ != incarnation || archive_round_ != log_archive_round ||
             log_id < last_split_log_id_ || (need_update_log_ts && (log_ts < last_split_log_submit_ts_))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR,
        "invalid new split_log_info",
        KR(ret),
        K(epoch_id),
        K(incarnation),
        K(log_archive_round),
        K(log_id),
        K(log_ts),
        K(pg_key_),
        K(need_update_log_ts),
        K(checkpoint_ts),
        KPC(this));
  } else {
    last_split_log_id_ = log_id;
    last_split_log_submit_ts_ = log_ts;
    if (checkpoint_ts > last_split_checkpoint_ts_) {
      last_split_checkpoint_ts_ = checkpoint_ts;
    }
  }

  return ret;
}

int ObPGArchiveTask::set_encount_fatal_error(const int64_t epoch, const int64_t incarnation, const int64_t round)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation != incarnation_ || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else {
    has_encount_error_ = true;
  }

  return ret;
}

int ObPGArchiveTask::update_pg_archive_checkpoint_ts(const int64_t epoch, const int64_t incarnation,
    const int64_t archive_round, const int64_t tstamp, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch <= 0 || incarnation <= 0 || archive_round <= 0 || OB_INVALID_TIMESTAMP == tstamp ||
                  OB_INVALID_TIMESTAMP == checkpoint_ts)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(tstamp),
        K(pg_key_),
        K(epoch),
        K(incarnation),
        K(archive_round),
        K(checkpoint_ts));
  } else if (epoch < epoch_ || pg_been_deleted_) {
    // skip
  } else if (epoch != epoch_ || incarnation != incarnation_ || archive_round != archive_round_) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN,
        "invalid new archive ts",
        KR(ret),
        K(epoch),
        K(incarnation),
        K(archive_round),
        K(tstamp),
        K(checkpoint_ts),
        KPC(this));
  } else if (OB_UNLIKELY(tstamp < archived_log_timestamp_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid new archive ts", KR(ret), K(tstamp), KPC(this));
  } else {
    archived_log_timestamp_ = tstamp;
    if (checkpoint_ts > archived_checkpoint_ts_) {
      archived_checkpoint_ts_ = checkpoint_ts;
    }
  }

  return ret;
}

int ObPGArchiveTask::mark_pg_first_record_finish(
    const int64_t epoch, const int64_t incarnation, const int64_t archive_round)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch <= 0 || incarnation <= 0 || archive_round <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(pg_key_), K(epoch), K(incarnation), K(archive_round));
  } else if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || archive_round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(archive_round), KPC(this));
  } else if (pg_been_deleted_) {
    // skip
  } else {
    is_first_record_finish_ = true;
  }

  return ret;
}

int ObPGArchiveTask::update_max_split_log_id(
    const int64_t epoch, const int64_t incarnation, const int64_t round, const uint64_t log_id, const file_id_t ilog_id)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_INVALID_ID != fetcher_max_split_log_id_ && fetcher_max_split_log_id_ >= log_id) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(fetcher_max_split_log_id_), K(log_id), K(pg_key_));
  } else {
    fetcher_max_split_log_id_ = log_id;
    current_ilog_id_ = ilog_id;
  }

  return ret;
}

int ObPGArchiveTask::update_max_log_id(
    const int64_t epoch, const int64_t incarnation, const int64_t round, const uint64_t max_log_id)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == max_log_id || (OB_INVALID_ID != max_log_id_ && max_log_id_ > max_log_id))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(max_log_id), KPC(this));
  } else {
    max_log_id_ = max_log_id;
  }

  return ret;
}

void ObPGArchiveTask::update_pg_archive_task_on_new_start(StartArchiveHelper& helper)
{
  WLockGuard guard(rwlock_);

  if (incarnation_ != helper.incarnation_ || archive_round_ != helper.archive_round_ || epoch_ < helper.epoch_) {
    update_unlock_(helper);
  } else {
    ARCHIVE_LOG(INFO, "obsolete add_pg_archive_task, skip it", K(helper), KPC(this));
  }
}

void ObPGArchiveTask::update_unlock_(StartArchiveHelper& helper)
{
  int64_t index_file_offset = 0;
  int64_t data_file_offset = 0;

  pg_been_deleted_ = false;
  pg_key_ = helper.pg_key_;
  tenant_id_ = extract_tenant_id(helper.pg_key_.get_table_id());
  round_start_info_ = helper.round_start_info_;

  start_log_id_ = helper.start_log_id_;
  fetcher_max_split_log_id_ = start_log_id_ - 1;

  archived_log_id_ = start_log_id_ - 1;
  archived_checkpoint_ts_ = helper.max_archived_log_info_.max_checkpoint_ts_archived_;
  archived_log_timestamp_ = helper.max_archived_log_info_.max_log_submit_ts_archived_;
  archived_clog_epoch_id_ = helper.max_archived_log_info_.clog_epoch_id_;
  archived_accum_checksum_ = helper.max_archived_log_info_.accum_checksum_;
  last_split_log_id_ = start_log_id_ - 1;
  last_split_log_submit_ts_ = helper.max_archived_log_info_.max_log_submit_ts_archived_;
  last_split_checkpoint_ts_ = helper.max_archived_log_info_.max_checkpoint_ts_archived_;

  current_ilog_id_ = helper.start_ilog_file_id_;

  incarnation_ = helper.incarnation_;
  archive_round_ = helper.archive_round_;
  epoch_ = helper.epoch_;
  mandatory_ = helper.is_mandatory_;

  if (!helper.data_file_exist_unrecorded_) {
    archive_destination_.init(pg_key_,
        helper.incarnation_,
        helper.archive_round_,
        helper.compatible_,
        helper.next_index_file_id_,
        index_file_offset,
        helper.next_data_file_id_,
        data_file_offset);
  } else if (!helper.unrecorded_data_file_valid_) {
    archive_destination_.init(pg_key_,
        helper.incarnation_,
        helper.archive_round_,
        helper.compatible_,
        helper.next_index_file_id_,
        index_file_offset,
        helper.max_data_file_id_,
        data_file_offset);
  } else {
    archive_destination_.init_with_valid_residual_data_file(pg_key_,
        helper.incarnation_,
        helper.archive_round_,
        helper.compatible_,
        helper.next_index_file_id_,
        index_file_offset,
        helper.max_data_file_id_,
        data_file_offset,
        helper.min_log_id_unrecorded_,
        helper.min_log_ts_unrecorded_);
  }

  free_task_status_();
}

int ObPGArchiveTask::update_pg_archive_file_offset(const int64_t epoch, const int64_t incarnation, const int64_t round,
    const int64_t buf_size, const LogArchiveFileType type)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_UNLIKELY(0 >= buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(buf_size), KPC(this));
  } else if (OB_FAIL(archive_destination_.update_file_offset(buf_size, type))) {
    ARCHIVE_LOG(
        WARN, "update_file_offset fail", KR(ret), K(epoch), K(incarnation), K(round), KPC(this), K(buf_size), K(type));
  }

  return ret;
}

void ObPGArchiveTask::mark_pg_archive_task_del(
    const int64_t epoch, const int64_t incarnation, const int64_t archive_round)
{
  WLockGuard guard(rwlock_);

  if (incarnation == incarnation_ && archive_round == archive_round_ && epoch == epoch_) {
    pg_been_deleted_ = true;
  }
}

int ObPGArchiveTask::switch_archive_file(
    const int64_t epoch, const int64_t incarnation, const int64_t round, const LogArchiveFileType type)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.switch_file(pg_key_, type, incarnation, round))) {
    ARCHIVE_LOG(WARN, "dest switch file fail", K(ret), K(pg_key_), K(archive_destination_));
  }

  return ret;
}

int ObPGArchiveTask::set_file_force_switch(
    const int64_t epoch, const int64_t incarnation, const int64_t round, const LogArchiveFileType type)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.set_file_force_switch(type))) {
    ARCHIVE_LOG(WARN, "set_file_force_switch fail", KR(ret), K(pg_key_), K(type));
  }

  return ret;
}

int ObPGArchiveTask::set_pg_data_file_record_min_log_info(const int64_t epoch, const uint64_t incarnation,
    const uint64_t round, const uint64_t min_log_id, const int64_t min_log_ts)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(epoch != epoch_ || incarnation_ != incarnation || round != archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(epoch), K(incarnation), K(round), KPC(this));
  } else if (OB_FAIL(archive_destination_.set_data_file_record_min_log_info(min_log_id, min_log_ts))) {
    ARCHIVE_LOG(WARN, "set_data_file_record_min_log_info fail", KR(ret), K(min_log_id), K(min_log_ts), K(pg_key_));
  }

  return ret;
}

int ObPGArchiveTask::push_send_task(ObArchiveSendTask& task, ObArchiveThreadPool& worker)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "allocator_ is NULL", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "send_task is invalid", KR(ret), K(task));
  } else if (OB_UNLIKELY(epoch_ != task.epoch_id_ || incarnation_ != task.incarnation_ ||
                         archive_round_ != task.log_archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", K(task), KPC(this));
  } else if (NULL == send_task_queue_) {
    if (OB_ISNULL(send_task_queue_ = allocator_->alloc_send_task_status(pg_key_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc ObArchiveSendTaskStatus fail", KR(ret), K(task));
    } else {
      send_task_queue_->inc_ref();
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(send_task_queue_)) {
    if (OB_FAIL(send_task_queue_->push(task, worker))) {
      ARCHIVE_LOG(WARN, "send_task_queue_ push fail", KR(ret), K(task));
    }
  }

  return ret;
}

int ObPGArchiveTask::push_split_task(ObPGArchiveCLogTask& task, ObArchiveThreadPool& worker)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "allocator_ is NULL", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "split_task is invalid", KR(ret), K(task));
  } else if (OB_UNLIKELY(epoch_ != task.epoch_id_ || incarnation_ != task.incarnation_ ||
                         archive_round_ != task.log_archive_round_)) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "invalid arguments", K(task), KPC(this));
  } else if (NULL == clog_task_queue_) {
    if (OB_ISNULL(clog_task_queue_ = allocator_->alloc_clog_task_status(pg_key_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc ObArchiveCLogTaskStatus fail", KR(ret), K(task));
    } else {
      clog_task_queue_->inc_ref();
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(clog_task_queue_)) {
    if (OB_FAIL(clog_task_queue_->push(task, worker))) {
      ARCHIVE_LOG(WARN, "clog_task_queue_ push fail", KR(ret), K(task));
    }
  }

  return ret;
}

void ObPGArchiveTask::free_task_status_()
{
  if (NULL != send_task_queue_) {
    bool is_discarded = false;
    send_task_queue_->free(is_discarded);
    if (is_discarded && NULL != allocator_) {
      allocator_->free_send_task_status(send_task_queue_);
      send_task_queue_ = NULL;
    }
  }

  if (NULL != clog_task_queue_) {
    bool is_discarded = false;
    clog_task_queue_->free(is_discarded);
    if (is_discarded && NULL != allocator_) {
      allocator_->free_clog_task_status(clog_task_queue_);
      clog_task_queue_ = NULL;
    }
  }
}

int ObPGArchiveTask::get_log_archive_stat(observer::PGLogArchiveStat& stat)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  stat.pg_key_ = pg_key_;
  stat.incarnation_ = incarnation_;
  stat.round_ = archive_round_;
  stat.epoch_ = epoch_;
  stat.been_deleted_ = pg_been_deleted_;
  stat.is_first_record_finish_ = is_first_record_finish_;
  stat.has_encount_error_ = has_encount_error_;
  stat.current_ilog_id_ = current_ilog_id_;
  stat.max_log_id_ = max_log_id_;
  stat.round_start_log_id_ = round_start_info_.start_log_id_;
  stat.round_start_log_ts_ = round_start_info_.start_ts_;
  stat.round_snapshot_version_ = round_start_info_.snapshot_version_;
  stat.cur_start_log_id_ = start_log_id_;
  stat.fetcher_max_split_log_id_ = fetcher_max_split_log_id_;
  stat.clog_max_split_log_id_ = last_split_log_id_;
  stat.clog_max_split_log_ts_ = last_split_log_submit_ts_;
  stat.clog_split_checkpoint_ts_ = last_split_checkpoint_ts_;
  stat.max_archived_log_id_ = archived_log_id_;
  stat.max_archived_log_ts_ = archived_log_timestamp_;
  stat.max_archived_checkpoint_ts_ = archived_checkpoint_ts_;
  stat.archived_clog_epoch_ = archived_clog_epoch_id_;
  stat.archived_accum_checksum_ = archived_accum_checksum_;
  stat.cur_index_file_id_ = archive_destination_.cur_index_file_id_;
  stat.index_file_offset_ = archive_destination_.index_file_offset_;
  stat.cur_data_file_id_ = archive_destination_.cur_data_file_id_;
  stat.data_file_offset_ = archive_destination_.data_file_offset_;

  return ret;
}

int ObPGArchiveTask::need_record_archive_key(const int64_t incarnation, const int64_t round, bool& need_record)
{
  int ret = OB_SUCCESS;
  need_record = false;

  RLockGuard guard(rwlock_);

  if (incarnation != incarnation_ || round != archive_round_) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "diff incarnation or round", KR(ret), K(incarnation), K(round), K(pg_key_));
  } else {
    need_record = !is_first_record_finish_;
  }

  return ret;
}

void ObPGArchiveTask::mock_init(const ObPGKey& pg_key, ObArchiveAllocator* allocator)
{
  if (OB_ISNULL(allocator)) {
    ARCHIVE_LOG(ERROR, "allocator is NULL");
  }
  pg_key_ = pg_key;
  start_log_id_ = 1;
  incarnation_ = 1;
  archive_round_ = 1;
  epoch_ = 1;
  allocator_ = allocator;
}

int ObPGArchiveTask::mock_push_task(ObArchiveSendTask& task, ObSpLinkQueue& queue)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "allocator_ is NULL", KR(ret));
  } else if (NULL == send_task_queue_) {
    if (OB_ISNULL(send_task_queue_ = allocator_->alloc_send_task_status(pg_key_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ARCHIVE_LOG(WARN, "alloc ObArchiveSendTaskStatus fail", KR(ret), K(task));
    } else {
      send_task_queue_->inc_ref();
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(send_task_queue_)) {
    if (OB_FAIL(send_task_queue_->mock_push(task, queue))) {
      ARCHIVE_LOG(WARN, "send_task_queue_ push fail", KR(ret), K(task));
    }
  }

  return ret;
}

void ObPGArchiveTask::mock_free_task_status()
{
  WLockGuard guard(rwlock_);

  free_task_status_();
}

ObPGArchiveTaskGuard::ObPGArchiveTaskGuard(ObArchivePGMgr* pg_mgr) : pg_archive_task_(NULL), pg_mgr_(pg_mgr)
{}

ObPGArchiveTaskGuard::~ObPGArchiveTaskGuard()
{
  revert_pg_archive_task_();
  pg_mgr_ = NULL;
}

void ObPGArchiveTaskGuard::set_pg_archive_task(ObPGArchiveTask* pg_archive_task)
{
  pg_archive_task_ = pg_archive_task;
}

ObPGArchiveTask* ObPGArchiveTaskGuard::get_pg_archive_task()
{
  return pg_archive_task_;
}

void ObPGArchiveTaskGuard::revert_pg_archive_task_()
{
  if (OB_ISNULL(pg_mgr_) || OB_ISNULL(pg_archive_task_)) {
    // nothing
  } else {
    pg_mgr_->revert_pg_archive_task(pg_archive_task_);
    pg_archive_task_ = NULL;
  }
}

}  // namespace archive
}  // namespace oceanbase
