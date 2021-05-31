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

#include "ob_start_archive_helper.h"
#include "share/backup/ob_backup_info_mgr.h"  // ObBackupInfo
#include "ob_archive_io.h"                    // ObArchiveIO
#include "ob_archive_log_wrapper.h"           // ObArchiveLogWrapper
#include "ob_archive_destination_mgr.h"       // ObArchiveDestination
#include "ob_archive_path.h"                  // ObArchivePathUtil
#include "ob_archive_entry_iterator.h"        // ObArchiveEntryIterator
#include "ob_archive_log_file_store.h"        // ObArchiveLogFileStore
#include "observer/ob_server_struct.h"        // GCTX
#include "ob_archive_mgr.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "ob_archive_file_utils.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::share;

void ObMaxArchivedLogInfo::reset()
{
  max_log_id_archived_ = 0;
  max_checkpoint_ts_archived_ = OB_INVALID_TIMESTAMP;
  max_log_submit_ts_archived_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
}

StartArchiveHelper::StartArchiveHelper(const ObPGKey& pg_key, const int64_t timestamp, const int64_t incarnation,
    const int64_t archive_round, const int64_t epoch, const int64_t takeover_ts, const bool compatible,
    const int64_t start_archive_ts, ObString& uri, ObString& storage_info, ObArchiveLogWrapper& log_wrapper,
    ObArchiveMgr& archive_mgr)
    : success_(false),
      pg_key_(pg_key),
      need_kickoff_log_(false),
      is_mandatory_(false),
      incarnation_(incarnation),
      archive_round_(archive_round),
      epoch_(epoch),
      takeover_ts_(takeover_ts),
      create_timestamp_(timestamp),
      round_start_info_(),
      start_log_id_(OB_INVALID_ID),
      start_ilog_file_id_(OB_INVALID_FILE_ID),
      max_archived_log_info_(),
      min_index_file_id_(0),
      max_index_file_id_(0),
      index_file_exist_(false),
      index_record_exist_(false),
      max_archived_index_info_(),
      min_data_file_id_(0),
      max_data_file_id_(0),
      data_file_exist_unrecorded_(false),
      unrecorded_data_file_valid_(false),
      min_log_id_unrecorded_(0),
      min_log_ts_unrecorded_(-1),
      max_log_id_unrecorded_(0),
      max_checkpoint_ts_unrecorded_(OB_INVALID_TIMESTAMP),
      max_log_submit_ts_unrecorded_(-1),
      next_data_file_id_(0),
      next_index_file_id_(0),
      exist_log_(false),
      compatible_(compatible),
      server_start_ts_(start_archive_ts),
      tenant_archive_checkpoint_ts_(OB_INVALID_TIMESTAMP),
      archive_status_(share::ObLogArchiveStatus::STATUS::INVALID),
      rs_start_ts_(OB_INVALID_TIMESTAMP),
      uri_(uri),
      storage_info_(storage_info),
      log_wrapper_(log_wrapper),
      archive_mgr_(archive_mgr)
{}

StartArchiveHelper::~StartArchiveHelper()
{}

bool StartArchiveHelper::is_valid()
{
  bool bret = true;

  if (OB_UNLIKELY(!success_) || OB_UNLIKELY(OB_INVALID_ID == start_log_id_) || OB_UNLIKELY(0 == start_ilog_file_id_) ||
      OB_UNLIKELY(0 == next_index_file_id_) || OB_UNLIKELY(0 == next_data_file_id_) ||
      OB_UNLIKELY(!round_start_info_.is_valid())) {
    bret = false;
    ARCHIVE_LOG(ERROR, "StartArchiveHelper is invalid", KPC(this));
  }

  return bret;
}

int StartArchiveHelper::handle()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!pg_key_.is_valid())) {
    ARCHIVE_LOG(WARN, "invalid pg_key", K(pg_key_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(handle_pg_start_archive_())) {
    ARCHIVE_LOG(WARN, "handle_pg_start_archive_ fail", KPC(this));
  } else {
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    ARCHIVE_LOG(INFO,
        "handle_pg_start_archive_ succ",
        K(cost_ts),
        KPC(this),
        K(need_kickoff_log_),
        K(next_data_file_id_),
        K(next_index_file_id_),
        K(unrecorded_data_file_valid_),
        K(success_),
        K(is_mandatory_),
        K(incarnation_),
        K(archive_round_),
        K(epoch_),
        K(server_start_ts_),
        K(archive_status_),
        K(rs_start_ts_),
        K(uri_));
  }

  return ret;
}

// start archive progress with reading archive files:
//
// 1. query max archived index file
// 2. get max archived index info
// 3. calculate max archived data file
// 4. read max arhived data file, get max archived log info
// 5. locate start ilog file id
// 6. decide need write kickoff log or not
// 7. make pg archive directory
int StartArchiveHelper::handle_pg_start_archive_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_max_file_remote_())) {
    ARCHIVE_LOG(WARN, "query_max_file_remote_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(decide_pg_next_start_file_id_())) {
    ARCHIVE_LOG(WARN, "decide_pg_next_start_file_id_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(decide_pg_start_archive_log_id_())) {
    ARCHIVE_LOG(WARN, "decide_pg_start_archive_log_id_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(locate_pg_start_ilog_id_())) {
    ARCHIVE_LOG(WARN, "locate_pg_start_ilog_id_ fail", KR(ret), K(pg_key_));
  } else if (OB_FAIL(decide_need_archive_first_record_())) {
    ARCHIVE_LOG(WARN, "decide_need_archive_first_record_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(check_and_make_pg_archive_directory_())) {
    ARCHIVE_LOG(WARN, "check_and_make_pg_archive_directory_ fail", KR(ret), KPC(this));
  } else {
    success_ = true;
  }

  return ret;
}

int StartArchiveHelper::decide_pg_start_archive_log_id_locally_()
{
  int ret = OB_SUCCESS;

  if (is_archive_status_doing_()) {
    if (OB_FAIL(decide_start_log_id_on_doing_())) {
      ARCHIVE_LOG(WARN, "decide_start_log_id_on_doing_ fail", KR(ret), KPC(this));
    }
  } else if (is_archive_status_beginning_()) {
    if (OB_FAIL(decide_start_log_id_on_beginning_())) {
      ARCHIVE_LOG(WARN, "decide_start_log_id_on_beginning_ fail", KR(ret), KPC(this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid archive status", KR(ret), KPC(this));
  }

  return ret;
}

int StartArchiveHelper::get_cluster_archive_status_()
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo info;

  if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
    ARCHIVE_LOG(WARN, "get_log_archive_backup_info fail", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObLogArchiveBackupInfo is not valid", KR(ret), K(info));
  } else if (share::ObLogArchiveStatus::STATUS::BEGINNING != info.status_.status_ &&
             share::ObLogArchiveStatus::STATUS::DOING != info.status_.status_) {
    ret = OB_EAGAIN;
    ARCHIVE_LOG(WARN, "invalid archive status", KR(ret), K(info));
  } else {
    archive_status_ = info.status_.status_;
    rs_start_ts_ = info.status_.start_ts_;
  }

  return ret;
}

int StartArchiveHelper::get_tenant_archive_status_()
{
  int ret = OB_SUCCESS;
  ObLogArchiveInfoMgr::ObLogArchiveSimpleInfo info;

  // get archive progress with pg_key, takeover_ts_
  if (OB_FAIL(
          ObLogArchiveInfoMgr::get_instance().get_log_archive_status(pg_key_.get_tenant_id(), takeover_ts_, info)) &&
      OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "get_log_archive_status fail", KR(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ARCHIVE_LOG(WARN, "tenant log archive info not exist yet", KR(ret), KPC(this));
    ret = OB_SUCCESS;
    tenant_archive_checkpoint_ts_ = 0;
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObLogArchiveSimpleInfo is not valid", KR(ret), K(info));
  } else if (share::ObLogArchiveStatus::STATUS::BEGINNING != info.status_ &&
             share::ObLogArchiveStatus::STATUS::DOING != info.status_) {
    ret = OB_EAGAIN;
    ARCHIVE_LOG(WARN, "no doing archive status, retry later", KR(ret), K(info));
  } else {
    tenant_archive_checkpoint_ts_ = info.checkpoint_ts_;
  }

  return ret;
}

bool StartArchiveHelper::is_archive_status_beginning_()
{
  return share::ObLogArchiveStatus::STATUS::BEGINNING == archive_status_;
}

bool StartArchiveHelper::is_archive_status_doing_()
{
  return share::ObLogArchiveStatus::STATUS::DOING == archive_status_;
}

int StartArchiveHelper::decide_start_log_id_on_beginning_()
{
  int ret = OB_SUCCESS;

  bool is_from_restore = false;
  if (OB_FAIL(log_wrapper_.check_is_from_restore(pg_key_, is_from_restore))) {
    ARCHIVE_LOG(WARN, "failed to check_is_from_restore", KR(ret), K(pg_key_));
  } else if (server_start_ts_ <= create_timestamp_ && (!is_from_restore)) {
    // fake a safe progress for pg whose start log id is 0
    int64_t first_log_submit_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(log_wrapper_.get_pg_first_log_submit_ts(pg_key_, first_log_submit_ts))) {
      ARCHIVE_LOG(ERROR, "get_pg_first_log_submit_ts fail", KR(ret), K(pg_key_));
      archive_mgr_.mark_encounter_fatal_err(pg_key_, incarnation_, archive_round_);
    } else {
      max_archived_log_info_.max_log_id_archived_ = 0;
      start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
      // set round_start_log_ts 1, to avoid push up the global start_ts in doing
      round_start_info_.start_ts_ = 1;
      round_start_info_.start_log_id_ = start_log_id_;
      round_start_info_.log_submit_ts_ = first_log_submit_ts - 1;
      round_start_info_.snapshot_version_ = 0;
      round_start_info_.clog_epoch_id_ = 1;
      round_start_info_.accum_checksum_ = 0;

      max_archived_log_info_.max_checkpoint_ts_archived_ = first_log_submit_ts - 1;
      max_archived_log_info_.max_log_submit_ts_archived_ = first_log_submit_ts - 1;
      max_archived_log_info_.clog_epoch_id_ = 1;  // fake_clog_epoch_id
      max_archived_log_info_.accum_checksum_ = 0;
    }
  } else {
    ObSavedStorageInfoV2 info;
    if (OB_FAIL(log_wrapper_.get_all_saved_info(pg_key_, info))) {
      ARCHIVE_LOG(WARN, "failed to get_saved_clog_info", KR(ret), K(pg_key_));
    } else {
      const ObBaseStorageInfo& clog_info = info.get_clog_info();
      if (OB_UNLIKELY(OB_INVALID_ID == clog_info.get_last_replay_log_id() ||
                      OB_INVALID_TIMESTAMP == clog_info.get_submit_timestamp() || 0 > clog_info.get_epoch_id())) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(WARN, "invalid info", KR(ret), K(pg_key_), K(info));
      } else {
        max_archived_log_info_.max_log_id_archived_ = clog_info.get_last_replay_log_id();
        start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;

        round_start_info_.start_ts_ = clog_info.get_submit_timestamp();
        round_start_info_.start_log_id_ = start_log_id_;
        round_start_info_.log_submit_ts_ = clog_info.get_submit_timestamp();
        round_start_info_.snapshot_version_ = info.get_data_info().get_publish_version();
        round_start_info_.clog_epoch_id_ = clog_info.get_epoch_id();
        round_start_info_.accum_checksum_ = clog_info.get_accumulate_checksum();

        max_archived_log_info_.max_checkpoint_ts_archived_ = 0;
        max_archived_log_info_.max_log_submit_ts_archived_ = clog_info.get_submit_timestamp();
        max_archived_log_info_.clog_epoch_id_ = clog_info.get_epoch_id();
        max_archived_log_info_.accum_checksum_ = clog_info.get_accumulate_checksum();
      }
    }
  }

  return ret;
}

int StartArchiveHelper::decide_start_log_id_on_doing_()
{
  int ret = OB_SUCCESS;

  bool is_from_restore = false;
  if (OB_FAIL(log_wrapper_.check_is_from_restore(pg_key_, is_from_restore))) {
    ARCHIVE_LOG(WARN, "failed to check_is_from_restore", KR(ret), K(pg_key_));
  } else if (server_start_ts_ < create_timestamp_ && (!is_from_restore)) {
    // fake a safe progress for pg whose start log id is 0
    int64_t first_log_submit_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(log_wrapper_.get_pg_first_log_submit_ts(pg_key_, first_log_submit_ts))) {
      ARCHIVE_LOG(ERROR, "get_pg_first_log_submit_ts fail", KR(ret), K(pg_key_));
      archive_mgr_.mark_encounter_fatal_err(pg_key_, incarnation_, archive_round_);
    } else {
      max_archived_log_info_.max_log_id_archived_ = 0;
      start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
      // set round_start_log_ts 1, to avoid push up the global start_ts in doing
      round_start_info_.start_ts_ = 1;
      round_start_info_.start_log_id_ = start_log_id_;
      round_start_info_.log_submit_ts_ = first_log_submit_ts - 1;
      round_start_info_.snapshot_version_ = 0;
      round_start_info_.clog_epoch_id_ = 1;
      round_start_info_.accum_checksum_ = 0;

      max_archived_log_info_.max_checkpoint_ts_archived_ = first_log_submit_ts - 1;
      max_archived_log_info_.max_log_submit_ts_archived_ = first_log_submit_ts - 1;
      max_archived_log_info_.clog_epoch_id_ = 1;  // fake_clog_epoch_id
      max_archived_log_info_.accum_checksum_ = 0;
    }
  } else {
    // restore tenant come into this case
    ObSavedStorageInfoV2 info;
    if (OB_FAIL(log_wrapper_.get_all_saved_info(pg_key_, info))) {
      ARCHIVE_LOG(WARN, "failed to get_save_clog_info", KR(ret), K(pg_key_));
    } else {
      const ObBaseStorageInfo& clog_info = info.get_clog_info();
      max_archived_log_info_.max_log_id_archived_ = clog_info.get_last_replay_log_id();
      start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;

      round_start_info_.start_ts_ = clog_info.get_submit_timestamp();
      round_start_info_.start_log_id_ = start_log_id_;
      round_start_info_.log_submit_ts_ = clog_info.get_submit_timestamp();
      round_start_info_.snapshot_version_ = info.get_data_info().get_publish_version();
      round_start_info_.clog_epoch_id_ = clog_info.get_epoch_id();
      round_start_info_.accum_checksum_ = clog_info.get_accumulate_checksum();

      max_archived_log_info_.max_checkpoint_ts_archived_ = 0;
      max_archived_log_info_.max_log_submit_ts_archived_ = clog_info.get_submit_timestamp();
      max_archived_log_info_.clog_epoch_id_ = clog_info.get_epoch_id();
      max_archived_log_info_.accum_checksum_ = clog_info.get_accumulate_checksum();
    }
  }

  return ret;
}

int StartArchiveHelper::locate_pg_start_ilog_id_()
{
  int ret = OB_SUCCESS;
  file_id_t ilog_id = OB_INVALID_FILE_ID;
  uint64_t max_log_id_in_ilog_file = OB_INVALID_ID;
  bool ilog_file_exist = false;

  if (OB_FAIL(log_wrapper_.locate_ilog_by_log_id(
          pg_key_, start_log_id_, max_log_id_in_ilog_file, ilog_file_exist, ilog_id))) {
    ARCHIVE_LOG(WARN, "locate_ilog_by_log_id fail", KR(ret));
  } else {
    start_ilog_file_id_ = ilog_id;
  }

  return ret;
}

int StartArchiveHelper::check_and_make_pg_archive_directory_()
{
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io(storage_info_);
  char index_file_prefix_path[MAX_FILE_PATH];
  char data_file_prefix_path[MAX_FILE_PATH];
  ObArchivePathUtil util;

  if (uri_.prefix_match(OB_OSS_PREFIX)) {
    // skip it
  } else if (OB_FAIL(util.build_archive_file_prefix(pg_key_,
                 LOG_ARCHIVE_FILE_TYPE_INDEX,
                 incarnation_,
                 archive_round_,
                 MAX_FILE_PATH,
                 index_file_prefix_path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_prefix fail", KR(ret), K(pg_key_));
  } else if (OB_FAIL(util.build_archive_file_prefix(pg_key_,
                 LOG_ARCHIVE_FILE_TYPE_DATA,
                 incarnation_,
                 archive_round_,
                 MAX_FILE_PATH,
                 data_file_prefix_path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_prefix fail", KR(ret), K(pg_key_));
  } else {
    ObString index_uri(index_file_prefix_path);
    ObString data_uri(data_file_prefix_path);
    if (OB_FAIL(archive_io.check_and_make_dir(pg_key_, index_uri))) {
      ARCHIVE_LOG(WARN, "failed to generate index_dir", KR(ret), K(pg_key_), K(index_uri));
    } else if (OB_FAIL(archive_io.check_and_make_dir(pg_key_, data_uri))) {
      ARCHIVE_LOG(WARN, "failed to generate data_dir", KR(ret), K(pg_key_), K(data_uri));
    } else {
    }
  }

  return ret;
}

int StartArchiveHelper::query_max_file_remote_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_max_exist_index_file_id_())) {
    ARCHIVE_LOG(WARN, "get_max_exist_index_file_id_ fail", K(ret), K(pg_key_));
  } else if (OB_FAIL(get_max_archived_index_info_())) {
    ARCHIVE_LOG(WARN, "get_max_archived_index_info_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(get_max_exist_data_file_id_())) {
    ARCHIVE_LOG(WARN, "get_max_exist_data_file_id_ fail", K(ret), K(pg_key_));
  } else {
    // succ
  }

  return ret;
}

// get max index file
int StartArchiveHelper::get_max_exist_index_file_id_()
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = 0;
  uint64_t max_index_file_id = 0;
  ObArchiveIO archive_io(storage_info_);

  if (OB_FAIL(archive_io.get_index_file_range(
          pg_key_, incarnation_, archive_round_, min_index_file_id, max_index_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_index_file_range fail", KR(ret), K(pg_key_), K(uri_));
    } else {
      ARCHIVE_LOG(DEBUG, "no index file exist", K(pg_key_));
      ret = OB_SUCCESS;
    }
  } else {
    min_index_file_id_ = min_index_file_id;
    max_index_file_id_ = max_index_file_id;
    index_file_exist_ = true;
    ARCHIVE_LOG(DEBUG,
        "get_max_exist_index_file_id_ succ",
        K(pg_key_),
        K(min_index_file_id_),
        K(max_index_file_id_),
        K(index_file_exist_));
  }

  return ret;
}

// get max data file
int StartArchiveHelper::get_max_exist_data_file_id_()
{
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io(storage_info_);
  uint64_t base_data_file_id = 0;

  if (index_record_exist_ && max_archived_index_info_.data_file_collect_) {
    base_data_file_id = max_archived_index_info_.max_record_data_file_id_;
    max_data_file_id_ = base_data_file_id;
    if (OB_FAIL(check_if_exist_data_file_not_record_(base_data_file_id + 1))) {
      ARCHIVE_LOG(WARN, "check_if_exist_data_file_not_record_ fail", KR(ret), K(base_data_file_id));
    } else if (data_file_exist_unrecorded_ && OB_FAIL(reconfirm_unrecord_data_file_())) {
      ARCHIVE_LOG(WARN, "reconfirm_unrecord_data_file_ fail", KR(ret), K(pg_key_));
    }
  } else if (OB_FAIL(list_and_check_data_file_with_no_index_file_())) {
    ARCHIVE_LOG(WARN, "list_and_check_data_file_with_no_index_file_ fail", KR(ret), KPC(this));
  }

  return ret;
}

// get max index info from index files
int StartArchiveHelper::get_max_archived_index_info_()
{
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io(storage_info_);

  if (!index_file_exist_) {
    // skip
  } else if (OB_FAIL(archive_io.get_max_archived_index_info(pg_key_,
                 incarnation_,
                 archive_round_,
                 min_index_file_id_,
                 max_index_file_id_,
                 max_archived_index_info_))) {
    ARCHIVE_LOG(WARN, "get_max_archived_index_info fail", KR(ret), KPC(this));
  } else if (max_archived_index_info_.data_file_collect_ || max_archived_index_info_.archived_log_collect_) {
    index_record_exist_ = true;
  }

  return ret;
}

int StartArchiveHelper::list_and_check_data_file_with_no_index_file_()
{
  int ret = OB_SUCCESS;
  uint64_t min_data_file_id = 0;
  uint64_t max_data_file_id = 0;
  ObArchiveIO archive_io(storage_info_);

  if (OB_FAIL(
          archive_io.get_data_file_range(pg_key_, incarnation_, archive_round_, min_data_file_id, max_data_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_max_data_file_id fail", KR(ret), K(pg_key_));
    } else {
      ARCHIVE_LOG(DEBUG, "no data file exist", K(pg_key_));
      ret = OB_SUCCESS;
    }
  } else if (min_data_file_id != max_data_file_id) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR,
        "data file more than one, and no data file recorded in index file",
        KR(ret),
        K(min_data_file_id),
        K(max_data_file_id),
        KPC(this));
  } else {
    data_file_exist_unrecorded_ = true;
    min_data_file_id_ = min_data_file_id;
    max_data_file_id_ = max_data_file_id;
    ARCHIVE_LOG(DEBUG,
        "list_and_check_data_file_with_no_index_file_ succ",
        K(min_data_file_id_),
        K(max_data_file_id_),
        K(pg_key_));
  }

  return ret;
}

// max_data_file_id_ + 1 not exist
int StartArchiveHelper::reconfirm_unrecord_data_file_()
{
  int ret = OB_SUCCESS;
  char data_file_path[MAX_FILE_PATH];
  bool file_exist = false;
  uint64_t file_id = max_data_file_id_ + 1;
  ObArchiveIO archive_io(storage_info_);
  ObArchivePathUtil util;

  if (OB_FAIL(util.build_archive_file_path(
          pg_key_, LOG_ARCHIVE_FILE_TYPE_DATA, file_id, incarnation_, archive_round_, MAX_FILE_PATH, data_file_path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_path fail", KR(ret), K(pg_key_));
  } else if (OB_FAIL(archive_io.check_file_exist(pg_key_, data_file_path, file_exist))) {
    ARCHIVE_LOG(WARN, "check_file_exist fail", KR(ret), KPC(this));
  } else if (file_exist) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "exist too many data file not recorded", K(file_id), K(pg_key_));
  }

  return ret;
}

int StartArchiveHelper::check_if_exist_data_file_not_record_(const uint64_t file_id)
{
  int ret = OB_SUCCESS;
  char data_file_path[MAX_FILE_PATH];
  bool file_exist = false;
  ObArchiveIO archive_io(storage_info_);
  ObArchivePathUtil util;

  if (OB_FAIL(util.build_archive_file_path(
          pg_key_, LOG_ARCHIVE_FILE_TYPE_DATA, file_id, incarnation_, archive_round_, MAX_FILE_PATH, data_file_path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_path fail", KR(ret), K(pg_key_));
  } else if (OB_FAIL(archive_io.check_file_exist(pg_key_, data_file_path, file_exist))) {
    ARCHIVE_LOG(WARN, "check_file_exist fail", KR(ret), K(file_id), KPC(this));
  } else if (!file_exist) {
    ARCHIVE_LOG(DEBUG, "NO data file exist not record in index file", K(file_id), K(data_file_path), K(pg_key_));
  } else {
    data_file_exist_unrecorded_ = true;
    max_data_file_id_ = file_id;
    ARCHIVE_LOG(DEBUG, "check_if_exist_data_file_not_record_ succ", K(pg_key_), K(max_data_file_id_));
  }

  return ret;
}

// 1. index/data file not exist
// 2. index not exist, data file exist
// 3. index/data both exist
int StartArchiveHelper::decide_pg_next_start_file_id_()
{
  int ret = OB_SUCCESS;

  next_index_file_id_ = max_index_file_id_ + 1;
  next_data_file_id_ = max_data_file_id_ + 1;

  return ret;
}

int StartArchiveHelper::decide_pg_start_archive_log_id_remote_()
{
  int ret = OB_SUCCESS;

  // get start log info and max recorded log info from index info
  if (index_record_exist_ && max_archived_index_info_.archived_log_collect_) {
    get_log_info_from_index_info_();
  }

  // get max log info from unrecorded data file
  // if no index info exist, start archive info also included in the unrecorded data file
  if (data_file_exist_unrecorded_) {
    if (OB_FAIL(extract_archive_log_from_data_file_())) {
      ARCHIVE_LOG(WARN, "extract_archive_log_from_data_file_ fail", KR(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret) && exist_log_) {
    start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
  }

  return ret;
}

int StartArchiveHelper::extract_archive_log_from_data_file_()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(quick_extract_archive_log_from_data_file_())) {
    ARCHIVE_LOG(INFO, "quick_extract_archive_log_from_data_file_ succ", KPC(this));
  } else if (OB_SUCC(iterate_archive_log_from_data_file_())) {
    ARCHIVE_LOG(INFO, "iterate_archive_log_from_data_file_ succ", KPC(this));
  } else {
    ARCHIVE_LOG(WARN, "extract_archive_log_from_data_file_ fail", KPC(this));
  }

  return ret;
}

int StartArchiveHelper::quick_extract_archive_log_from_data_file_()
{
  int ret = OB_SUCCESS;
  char path[MAX_FILE_PATH] = {0};
  ObArchivePathUtil path_util;
  ObArchiveBlockMeta block_meta;
  clog::ObLogEntry log_entry;
  ObLogArchiveInnerLog inner_log;
  bool is_kickoff = false;
  ObArchiveLogFileStore file_store;
  const uint64_t tenant_id = pg_key_.get_tenant_id();
  const char* cluster_name = GCTX.config_->cluster;
  const int64_t cluster_id = GCTX.config_->cluster_id;

  if (OB_UNLIKELY(!data_file_exist_unrecorded_ || 0 == max_data_file_id_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid data file id", KR(ret), K(data_file_exist_unrecorded_), K(max_data_file_id_));
  } else if (OB_FAIL(file_store.init(
                 uri_.ptr(), storage_info_.ptr(), cluster_name, cluster_id, tenant_id, incarnation_, archive_round_))) {
    ARCHIVE_LOG(WARN, "init file_store fail", KR(ret), KPC(this), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(path_util.build_archive_file_path(pg_key_,
                 LOG_ARCHIVE_FILE_TYPE_DATA,
                 max_data_file_id_,
                 incarnation_,
                 archive_round_,
                 MAX_FILE_PATH,
                 path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_path fail", KR(ret), K(pg_key_), K(max_data_file_id_));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.extract_last_log_in_data_file(
            pg_key_, max_data_file_id_, &file_store, uri, storage_info_, block_meta, log_entry, inner_log))) {
      ARCHIVE_LOG(WARN, "extract_last_log_in_data_file fail", KR(ret), K(pg_key_), K(uri));
    } else {
      exist_log_ = true;
      rebuild_archive_progress_from_block_meta_(block_meta);
      const ObLogEntryHeader& log_header = log_entry.get_header();
      is_kickoff = is_archive_kickoff_log(log_header.get_log_type());
      if (is_kickoff) {
        if (OB_FAIL(rebuild_start_archive_info_from_kickoff_log_(block_meta, log_header, inner_log))) {
          ARCHIVE_LOG(WARN, "rebuild_start_archive_info_from_kickoff_log_", K(pg_key_));
        }
      }
      unrecorded_data_file_valid_ = true;
    }

    // if start archive info still not be found,
    // kickoff log must be the first block in the file
    if (OB_SUCC(ret) && !round_start_info_.is_valid() && !is_kickoff) {
      ObArchiveBlockMeta first_block_meta;
      clog::ObLogEntry first_log_entry;
      ObLogArchiveInnerLog first_inner_log;
      if (OB_FAIL(utils.extract_first_log_in_data_file(
              pg_key_, max_data_file_id_, &file_store, first_block_meta, first_log_entry, first_inner_log))) {
        ARCHIVE_LOG(WARN, "extract_first_log_in_data_file fail", KR(ret), K(pg_key_));
      } else if (OB_FAIL(rebuild_start_archive_info_from_kickoff_log_(
                     first_block_meta, first_log_entry.get_header(), first_inner_log))) {
        ARCHIVE_LOG(WARN,
            "rebuild_start_archive_info_from_kickoff_log_ fail",
            KR(ret),
            K(pg_key_),
            K(first_block_meta),
            K(first_log_entry),
            K(first_inner_log),
            KPC(this));
      }
    }
  }

  return ret;
}

int StartArchiveHelper::iterate_archive_log_from_data_file_()
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT = 60 * 1000 * 1000L;
  const uint64_t tenant_id = pg_key_.get_tenant_id();
  const char* cluster_name = GCTX.config_->cluster;
  const int64_t cluster_id = GCTX.config_->cluster_id;

  ObArchiveEntryIterator iter;
  ObArchiveLogFileStore file_store;

  const bool need_limit_bandwidth = false;
  if (OB_UNLIKELY(!data_file_exist_unrecorded_ || 0 == max_data_file_id_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid data file id", KR(ret), K(data_file_exist_unrecorded_), K(max_data_file_id_));
  } else if (OB_FAIL(file_store.init(
                 uri_.ptr(), storage_info_.ptr(), cluster_name, cluster_id, tenant_id, incarnation_, archive_round_))) {
    ARCHIVE_LOG(WARN, "init file_store fail", KR(ret), KPC(this), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(iter.init(&file_store, pg_key_, max_data_file_id_, 0, TIMEOUT, need_limit_bandwidth))) {
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator init fail", KR(ret), K(pg_key_));
  } else {
    clog::ObLogEntry log_entry;
    bool has_valid_block = false;
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = iter.next_entry(log_entry))) {
      exist_log_ = true;
      has_valid_block = true;
      const ObLogEntryHeader& log_header = log_entry.get_header();
      const uint64_t log_id = log_header.get_log_id();
      clog::ObLogType log_type = log_header.get_log_type();
      const int64_t data_len = log_entry.get_header().get_data_len();
      if (is_archive_kickoff_log(log_type)) {
        ObLogArchiveInnerLog inner_log;
        int64_t pos = 0;
        if (OB_FAIL(inner_log.deserialize(log_entry.get_buf(), data_len, pos))) {
          ARCHIVE_LOG(WARN, "failed to deserialize inner_log", KR(ret), K(log_entry), K(pg_key_));
        } else {
          const ObArchiveBlockMeta& block_meta = iter.get_last_block_meta();

          if (OB_UNLIKELY(block_meta.max_log_id_ != log_id - 1)) {
            ret = OB_ERR_UNEXPECTED;
            ARCHIVE_LOG(ERROR,
                "block meta is not consistent with log data",
                KR(ret),
                K(pg_key_),
                K(block_meta),
                K(log_entry),
                K(inner_log),
                KPC(this));
          } else if (OB_FAIL(rebuild_start_archive_info_from_kickoff_log_(block_meta, log_header, inner_log))) {
            ARCHIVE_LOG(
                WARN, "rebuild_start_archive_info_from_kickoff_log_ fail", KPC(this), K(block_meta), K(log_header));
          }
        }
      }
      unrecorded_data_file_valid_ = true;
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && has_valid_block) {
      const ObArchiveBlockMeta& block_meta = iter.get_last_block_meta();
      const uint64_t log_entry_log_id = log_entry.get_header().get_log_id();
      // last archived log id = log_id of kickoff - 1
      const uint64_t last_archived_log_id =
          is_archive_kickoff_log(log_entry.get_header().get_log_type()) ? (log_entry_log_id - 1) : log_entry_log_id;
      if (OB_UNLIKELY(block_meta.max_log_id_ != last_archived_log_id)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR,
            "block meta is not consistent with log data",
            KR(ret),
            K(pg_key_),
            K(block_meta),
            K(log_entry),
            KPC(this));
      } else {
        rebuild_archive_progress_from_block_meta_(block_meta);
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int StartArchiveHelper::rebuild_start_archive_info_from_kickoff_log_(
    const ObArchiveBlockMeta& block_meta, const ObLogEntryHeader& log_header, ObLogArchiveInnerLog& inner_log)
{
  int ret = OB_SUCCESS;
  clog::ObLogType log_type = log_header.get_log_type();

  if (OB_UNLIKELY(!is_archive_kickoff_log(log_type))) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "not kickoff log", KR(ret), K(log_header), K(block_meta));
  } else {
    round_start_info_.start_log_id_ = log_header.get_log_id();
    ;
    round_start_info_.log_submit_ts_ = log_header.get_submit_timestamp();
    round_start_info_.start_ts_ = inner_log.get_round_start_ts();
    round_start_info_.snapshot_version_ = inner_log.get_round_snapshot_version();
    round_start_info_.clog_epoch_id_ = block_meta.clog_epoch_id_;
    round_start_info_.accum_checksum_ = block_meta.accum_checksum_;
  }

  return ret;
}

void StartArchiveHelper::rebuild_archive_progress_from_block_meta_(const ObArchiveBlockMeta& block_meta)
{
  min_log_id_unrecorded_ = block_meta.min_log_id_in_file_;
  min_log_ts_unrecorded_ = block_meta.min_log_ts_in_file_;
  max_log_id_unrecorded_ = block_meta.max_log_id_;
  max_checkpoint_ts_unrecorded_ = block_meta.max_checkpoint_ts_;
  max_log_submit_ts_unrecorded_ = block_meta.max_log_submit_ts_;

  max_archived_log_info_.max_log_id_archived_ = block_meta.max_log_id_;
  max_archived_log_info_.max_checkpoint_ts_archived_ = block_meta.max_checkpoint_ts_;
  max_archived_log_info_.max_log_submit_ts_archived_ = block_meta.max_log_submit_ts_;
  max_archived_log_info_.clog_epoch_id_ = block_meta.clog_epoch_id_;
  max_archived_log_info_.accum_checksum_ = block_meta.accum_checksum_;
}

void StartArchiveHelper::get_log_info_from_index_info_()
{
  round_start_info_ = max_archived_index_info_.round_start_info_;

  max_archived_log_info_.max_log_id_archived_ = max_archived_index_info_.max_record_log_id_;
  max_archived_log_info_.max_checkpoint_ts_archived_ = max_archived_index_info_.max_record_checkpoint_ts_;
  max_archived_log_info_.max_log_submit_ts_archived_ = max_archived_index_info_.max_record_log_submit_ts_;

  max_archived_log_info_.clog_epoch_id_ = max_archived_index_info_.clog_epoch_id_;
  max_archived_log_info_.accum_checksum_ = max_archived_index_info_.accum_checksum_;
  exist_log_ = true;
}

// at least on file exist to guarantee archive progress can be obtained
int StartArchiveHelper::decide_pg_start_archive_log_id_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(decide_pg_start_archive_log_id_remote_())) {
    ARCHIVE_LOG(WARN, "decide_pg_start_archive_log_id_remote_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(get_cluster_archive_status_())) {
    ARCHIVE_LOG(WARN, "get_cluster_archive_status_ fail", KR(ret));
  } else if (OB_FAIL(get_tenant_archive_status_())) {
    ARCHIVE_LOG(WARN, "get_tenant_archive_status_ fail", KR(ret));
  }

  if (OB_SUCC(ret)) {
    // If no data exist in backup destiantion, the start archive info will be decided in local.
    if (!exist_log_) {
      if (OB_FAIL(decide_pg_start_archive_log_id_locally_())) {
        ARCHIVE_LOG(WARN, "decide_pg_start_archive_log_id_locally_ fail", KR(ret), KPC(this));
      }
    }
    // If log exist in backup dest, the archive progress will be decided by backup dest/rs/local cache chekcpoint ts.
    else {
      int64_t cur_checkpoint_ts = 0;
      if (OB_FAIL(get_cur_pg_log_archive_checkpoint_ts_(cur_checkpoint_ts))) {
        ARCHIVE_LOG(WARN, "get_cur_pg_log_archive_checkpoint_ts_ fail", KR(ret), K(pg_key_));
      } else {
        max_archived_log_info_.max_checkpoint_ts_archived_ = std::max(cur_checkpoint_ts,
            std::max(max_archived_log_info_.max_checkpoint_ts_archived_, tenant_archive_checkpoint_ts_));
      }
    }
  }

  return ret;
}

int StartArchiveHelper::get_cur_pg_log_archive_checkpoint_ts_(int64_t& cur_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObPGLogArchiveStatus status;
  cur_checkpoint_ts = 0;

  if (OB_FAIL(log_wrapper_.get_pg_log_archive_status(pg_key_, status))) {
    ARCHIVE_LOG(WARN, "get_pg_log_archive_status fail", KR(ret), K(pg_key_));
  } else if (status.archive_incarnation_ != incarnation_ || status.log_archive_round_ != archive_round_) {
    // skip it
  } else {
    ARCHIVE_LOG(INFO, "get_log_archive_status succ", K(status), K(pg_key_));
    cur_checkpoint_ts = status.last_archived_checkpoint_ts_;
  }

  return ret;
}

int StartArchiveHelper::decide_need_archive_first_record_()
{
  int ret = OB_SUCCESS;

  if (exist_log_) {
    need_kickoff_log_ = false;
  } else {
    need_kickoff_log_ = true;
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
