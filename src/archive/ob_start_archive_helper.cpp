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
#include "ob_archive_util.h"  //is_valid_piece_info()
#include "ob_archive_file_utils.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "ob_archive_file_utils.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::share;

void ObMaxArchivedLogInfo::reset()
{
  max_log_id_archived_ = OB_INVALID_ID;
  max_checkpoint_ts_archived_ = OB_INVALID_TIMESTAMP;
  max_log_submit_ts_archived_ = OB_INVALID_TIMESTAMP;
  clog_epoch_id_ = OB_INVALID_TIMESTAMP;
  accum_checksum_ = 0;
}

bool ObMaxArchivedLogInfo::is_valid() const
{
  return ((OB_INVALID_ID != max_log_id_archived_) && (OB_INVALID_TIMESTAMP != max_checkpoint_ts_archived_) &&
          (OB_INVALID_TIMESTAMP != max_log_submit_ts_archived_) && (OB_INVALID_TIMESTAMP != clog_epoch_id_));
}
StartArchiveHelper::StartArchiveHelper(const ObPGKey& pg_key, const int64_t create_timestamp, const int64_t epoch,
    const int64_t incarnation, const int64_t archive_round, const int64_t takeover_ts, const bool compatible,
    const int64_t start_archive_ts, ObString& uri, ObString& storage_info, ObArchiveLogWrapper& log_wrapper,
    ObArchiveMgr& archive_mgr)
    : success_(false),
      need_check_and_mkdir_(true),
      compatible_(compatible),
      need_kickoff_(true),
      need_switch_piece_on_beginning_(false),
      start_ilog_file_id_(OB_INVALID_FILE_ID),
      max_index_file_id_(OB_INVALID_ARCHIVE_FILE_ID),
      max_data_file_id_(OB_INVALID_ARCHIVE_FILE_ID),
      epoch_(epoch),
      incarnation_(incarnation),
      archive_round_(archive_round),
      cur_piece_id_(OB_BACKUP_INVALID_PIECE_ID),
      cur_piece_create_date_(OB_INVALID_TIMESTAMP),
      takeover_ts_(takeover_ts),
      create_timestamp_(create_timestamp),
      server_start_ts_(start_archive_ts),
      tenant_archive_checkpoint_ts_(OB_INVALID_TIMESTAMP),
      archive_status_(share::ObLogArchiveStatus::STATUS::INVALID),
      pg_key_(pg_key),
      round_start_info_(),
      max_archived_index_info_(),
      data_file_meta_(),
      max_archived_log_info_(),
      uri_(uri),
      storage_info_(storage_info),
      log_wrapper_(log_wrapper),
      archive_mgr_(archive_mgr)
{}

StartArchiveHelper::~StartArchiveHelper()
{}

bool StartArchiveHelper::is_valid() const
{
  bool bret = true;

  if (OB_UNLIKELY(!success_) || OB_UNLIKELY(!max_archived_log_info_.is_valid()) ||
      OB_UNLIKELY(0 == start_ilog_file_id_) || OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id_) ||
      OB_UNLIKELY(OB_INVALID_ARCHIVE_FILE_ID == max_data_file_id_) || OB_UNLIKELY(!round_start_info_.is_valid())) {
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
    ARCHIVE_LOG(INFO, "handle_pg_start_archive_ succ", K(cost_ts), KPC(this), K(incarnation_), K(archive_round_));
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

  if (OB_FAIL(query_archive_meta_remote_())) {
    ARCHIVE_LOG(WARN, "query_max_file_remote_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(decide_pg_start_archive_log_info_())) {
    ARCHIVE_LOG(WARN, "decide_pg_start_archive_log_id_ fail", KR(ret), KPC(this));
  } else if (OB_FAIL(locate_pg_start_ilog_file_id_())) {
    ARCHIVE_LOG(WARN, "locate_pg_start_ilog_id_ fail", KR(ret), K(pg_key_));
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
  }

  return ret;
}

int StartArchiveHelper::get_tenant_archive_status_(ObLogArchiveSimpleInfo& info)
{
  int ret = OB_SUCCESS;
  // get archive progress with pg_key, takeover_ts_
  if (OB_FAIL(
          ObLogArchiveInfoMgr::get_instance().get_log_archive_status(pg_key_.get_tenant_id(), takeover_ts_, info))) {
    ARCHIVE_LOG(WARN, "get_log_archive_status fail", KR(ret));
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
      round_start_info_.start_ts_ = 1;
      round_start_info_.start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
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

        round_start_info_.start_ts_ = clog_info.get_submit_timestamp();
        round_start_info_.start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
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
  } else if (!is_from_restore) {
    // fake a safe progress for pg whose start log id is 0
    int64_t first_log_submit_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(log_wrapper_.get_pg_first_log_submit_ts(pg_key_, first_log_submit_ts))) {
      ARCHIVE_LOG(ERROR, "get_pg_first_log_submit_ts fail", KR(ret), K(pg_key_));
      archive_mgr_.mark_encounter_fatal_err(pg_key_, incarnation_, archive_round_);
    } else {
      max_archived_log_info_.max_log_id_archived_ = 0;
      round_start_info_.start_ts_ = 1;
      round_start_info_.start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
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

      round_start_info_.start_ts_ = clog_info.get_submit_timestamp();
      round_start_info_.start_log_id_ = max_archived_log_info_.max_log_id_archived_ + 1;
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

int StartArchiveHelper::locate_pg_start_ilog_file_id_()
{
  int ret = OB_SUCCESS;
  file_id_t ilog_file_id = OB_INVALID_FILE_ID;
  uint64_t max_log_id_in_ilog_file = OB_INVALID_ID;
  bool ilog_file_exist = false;

  if (OB_FAIL(log_wrapper_.locate_ilog_by_log_id(
          pg_key_, get_start_log_id(), max_log_id_in_ilog_file, ilog_file_exist, ilog_file_id))) {
    if (OB_NEED_RETRY == ret) {
      ARCHIVE_LOG(WARN, "locate_ilog_by_log_id fail", KR(ret), K(pg_key_));
    } else {
      ARCHIVE_LOG(ERROR, "locate_ilog_by_log_id fail", KR(ret), K(pg_key_));
      archive_mgr_.mark_encounter_fatal_err(pg_key_, incarnation_, archive_round_);
    }
  } else {
    start_ilog_file_id_ = ilog_file_id;
  }

  return ret;
}

int StartArchiveHelper::check_and_make_pg_archive_directory_()
{
  int ret = OB_SUCCESS;
  if (!need_check_and_mkdir_ || uri_.prefix_match(OB_OSS_PREFIX)) {
    // do nothing
  } else {
    ObArchiveIO archive_io(storage_info_);
    if (OB_FAIL(archive_io.check_and_make_dir_for_archive(
            pg_key_, incarnation_, archive_round_, cur_piece_id_, cur_piece_create_date_))) {
      ARCHIVE_LOG(
          WARN, "failed to check_and_make_dir_for_archive", K(ret), KPC(this), K(incarnation_), K(archive_round_));
    }
  }
  return ret;
}

bool StartArchiveHelper::is_archive_meta_complete_() const
{
  return round_start_info_.is_valid() && max_archived_log_info_.is_valid() &&
         OB_INVALID_ARCHIVE_FILE_ID != max_index_file_id_ && OB_INVALID_ARCHIVE_FILE_ID != max_data_file_id_;
}

bool StartArchiveHelper::archive_data_exists_() const
{
  // index file exists or data file exists or archive_key exist
  return ((OB_INVALID_ARCHIVE_FILE_ID != max_index_file_id_ && 0 < max_index_file_id_) ||
          (OB_INVALID_ARCHIVE_FILE_ID != max_data_file_id_ && 0 < max_data_file_id_) || round_start_info_.is_valid());
}

int StartArchiveHelper::query_archive_meta_remote_()
{
  int ret = OB_SUCCESS;
  ObLogArchiveSimpleInfo archive_info;
  if (OB_FAIL(get_tenant_archive_status_(archive_info))) {
    ARCHIVE_LOG(WARN, "get_tenant_archive_status_ fail", K(ret), KPC(this));
  } else if (OB_FAIL(get_piece_archive_meta_(
                 archive_info.cur_piece_id_, archive_info.cur_piece_create_date_, true /*is_cur_piece*/))) {
    ARCHIVE_LOG(WARN, "failed to get_piece_archive_meta_ of cur_piece", K(ret), KPC(this));
  } else {
    if (archive_data_exists_()) {
      // cur_piece has data, no need to mkdir
      need_check_and_mkdir_ = false;
    }

    if (OB_SUCC(ret)) {
      // check prev piece
      if (!is_archive_meta_complete_() && (archive_info.is_piece_freezing())) {
        if (archive_data_exists_()) {
          ret = OB_ERR_UNEXPECTED;
          ARCHIVE_LOG(ERROR, "cur piece data is not valid", K(ret), KPC(this), K(archive_info));
        } else if (OB_FAIL(get_piece_archive_meta_(
                       archive_info.prev_piece_id_, archive_info.prev_piece_create_date_, false /*is_cur_piece*/))) {
          ARCHIVE_LOG(WARN, "failed to get_piece_archive_meta_ of pre_piece", K(ret), KPC(this), K(archive_info));
        } else {
          need_switch_piece_on_beginning_ = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      // set piece_info
      cur_piece_id_ = archive_info.cur_piece_id_;
      cur_piece_create_date_ = archive_info.cur_piece_create_date_;

      // in case of switch piece scene, has no data
      if (!round_start_info_.is_valid()) {
        need_kickoff_ = true;
        // has neither effective index data or log data
        (void)generate_max_data_file_id_();
        if (OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id_) {
          max_index_file_id_ = 0;
        }
      }

      if (need_kickoff_) {
        // need_kickoff(new_borm) pg do not need to switch_piece on beginning
        need_switch_piece_on_beginning_ = false;
      }
    }
  }
  return ret;
}

// get max index file
void StartArchiveHelper::generate_max_data_file_id_()
{
  if (OB_INVALID_ARCHIVE_FILE_ID == max_data_file_id_) {
    if (data_file_meta_.need_record_index_) {
      max_data_file_id_ = data_file_meta_.data_file_id_;
    } else if (max_archived_index_info_.is_data_file_collected()) {
      // max_record_data_file_id may be value of previous piece when index file not exists
      max_data_file_id_ = max_archived_index_info_.max_record_data_file_id_;
    } else {
      max_data_file_id_ = 0;
    }
  }
}

int StartArchiveHelper::get_piece_archive_meta_(
    const int64_t piece_id, const int64_t piece_create_date, const bool is_cur_piece)
{
  int ret = OB_SUCCESS;
  bool is_first_piece = false;
  bool is_archive_key_compelete = false;
  if (OB_UNLIKELY(!is_valid_piece_info(piece_id, piece_create_date))) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid piece_info",
        K(ret),
        K(pg_key_),
        K(incarnation_),
        K(archive_round_),
        K(piece_id),
        K(piece_create_date),
        K(is_cur_piece));
  } else if (OB_UNLIKELY(max_archived_index_info_.is_index_record_collected())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN,
        "max_archived_index_info has already collected index record",
        K(ret),
        K(pg_key_),
        K(incarnation_),
        K(archive_round_),
        K(piece_id),
        K(piece_create_date),
        K(is_cur_piece),
        K(max_archived_index_info_));
  } else if (OB_FAIL(get_archived_info_from_index_(piece_id, piece_create_date))) {
    ARCHIVE_LOG(WARN, "get_max_archived_index_info_ fail", KR(ret), KPC(this));
  } else if (!max_archived_index_info_.is_index_record_collected() && OB_BACKUP_NO_SWITCH_PIECE_ID != piece_id &&
             OB_FAIL(get_archived_info_from_archive_key_(
                 piece_id, piece_create_date, is_cur_piece, is_first_piece, is_archive_key_compelete))) {
    ARCHIVE_LOG(
        WARN, "failed to get_archived_info_from_archive_key_", KR(ret), KPC(this), K(piece_id), K(piece_create_date));
  } else if (OB_BACKUP_NO_SWITCH_PIECE_ID != piece_id && !max_archived_index_info_.is_data_file_collected() &&
             !is_archive_key_compelete) {
    // in switching_piece mode, no need to check next archive_data_file  when archive_key is not
    // valid and index record not collected
  } else {
    if (!round_start_info_.is_valid() && max_archived_index_info_.is_round_start_info_collected()) {
      // init round_start_info_
      round_start_info_ = max_archived_index_info_.round_start_info_;
    }

    ObArchiveDataFileMeta data_file_meta;
    if (OB_FAIL(check_and_extract_data_file_archive_info_(piece_id, piece_create_date))) {
      ARCHIVE_LOG(WARN,
          "failed to get_max_exist_data_file_id_",
          K(ret),
          K(pg_key_),
          K(incarnation_),
          K(archive_round_),
          K(piece_id),
          K(piece_create_date));
    } else {
      // 1: in case of no switch piece scene，should generate here
      // 2: in case of switch piece scene, come here means index file has effective data or archive pg
      // key has effective data, should generate here
      (void)generate_max_data_file_id_();
      // generate index file id
      if (OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id_) {
        max_index_file_id_ = 0;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (max_archived_index_info_.archived_log_collect_ && !max_archived_log_info_.is_valid()) {
      // 1.no switch piece mode: has index file, and no effective data file which is not indexed
      // 2.switch piece mode:
      //   2.1 first piece, has index file, and no effective data file which is not indexed
      //   2.2 not first piece, has index file, and no effective data file which is not indexed, or
      //   only has archive_pg_key file
      max_archived_log_info_.max_log_id_archived_ = max_archived_index_info_.max_record_log_id_;
      max_archived_log_info_.max_checkpoint_ts_archived_ = max_archived_index_info_.max_record_checkpoint_ts_;
      max_archived_log_info_.max_log_submit_ts_archived_ = max_archived_index_info_.max_record_log_submit_ts_;
      max_archived_log_info_.clog_epoch_id_ = max_archived_index_info_.clog_epoch_id_;
      max_archived_log_info_.accum_checksum_ = max_archived_index_info_.accum_checksum_;
    }

    if (is_archive_meta_complete_()) {
      // do nothing
      need_kickoff_ = false;
    } else {
      if (OB_BACKUP_NO_SWITCH_PIECE_ID == piece_id) {
        if (OB_INVALID_ARCHIVE_FILE_ID != max_index_file_id_ && !max_archived_log_info_.is_valid() &&
            round_start_info_.is_valid()) {
          // https://work.aone.alibaba-inc.com/issue/34123450: if index file and data file exist, but
          // archive log not exist, just fake max_archive_log_info_
          max_archived_log_info_.max_log_id_archived_ = round_start_info_.start_log_id_ - 1;
          max_archived_log_info_.max_checkpoint_ts_archived_ = 0;
          max_archived_log_info_.max_log_submit_ts_archived_ = round_start_info_.log_submit_ts_;
          max_archived_log_info_.clog_epoch_id_ = round_start_info_.clog_epoch_id_;
          max_archived_log_info_.accum_checksum_ = round_start_info_.accum_checksum_;
          need_kickoff_ = true;
        }
      }
    }
  }

  return ret;
}

// get max data file
int StartArchiveHelper::check_and_extract_data_file_archive_info_(
    const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io(storage_info_);

  if (max_archived_index_info_.data_file_collect_) {
    const int64_t file_id = max_archived_index_info_.max_record_data_file_id_ + 1;
    if (OB_FAIL(check_if_exist_data_file_not_record_(piece_id, piece_create_date, file_id))) {
      ARCHIVE_LOG(WARN, "check_if_exist_data_file_not_record_ fail", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(list_and_check_data_file_with_no_index_file_(piece_id, piece_create_date))) {
    ARCHIVE_LOG(WARN, "list_and_check_data_file_with_no_index_file_ fail", KR(ret), KPC(this));
  }

  if (OB_SUCC(ret) && data_file_meta_.need_record_index_) {
    if (OB_FAIL(extract_archive_log_from_data_file_(piece_id, piece_create_date))) {
      ARCHIVE_LOG(WARN, "extract_archive_log_from_data_file_ fail", KR(ret), KPC(this));
    }
  }

  return ret;
}

// get max index info from index files
int StartArchiveHelper::get_archived_info_from_index_(const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  uint64_t min_index_file_id = OB_INVALID_ARCHIVE_FILE_ID;
  uint64_t max_index_file_id = OB_INVALID_ARCHIVE_FILE_ID;
  ObArchiveIO archive_io(storage_info_);
  if (OB_FAIL(archive_io.get_index_file_range(
          pg_key_, incarnation_, archive_round_, piece_id, piece_create_date, min_index_file_id, max_index_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_index_file_range fail", KR(ret), K(pg_key_), K(uri_));
    } else {
      ret = OB_SUCCESS;
      ARCHIVE_LOG(INFO,
          "no index file exist in current piece ",
          K(pg_key_),
          K(incarnation_),
          K(archive_round_),
          K(piece_id),
          K(piece_create_date));
    }
  } else if (OB_FAIL(archive_io.get_max_archived_index_info(pg_key_,
                 incarnation_,
                 archive_round_,
                 piece_id,
                 piece_create_date,
                 min_index_file_id,
                 max_index_file_id,
                 max_archived_index_info_))) {
    ARCHIVE_LOG(WARN, "get_max_archived_index_info fail", KR(ret), KPC(this));
  } else {
    // update next_data_file_id_
    if (OB_UNLIKELY(
            OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id || (OB_INVALID_ARCHIVE_FILE_ID != max_index_file_id_))) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN,
          "max_index_file_id is unexpected",
          KR(ret),
          K(max_index_file_id_),
          K(max_index_file_id),
          K(min_index_file_id),
          K(piece_id),
          K(piece_create_date),
          KPC(this));
    } else {
      // set max_index_file_id_
      max_index_file_id_ = max_index_file_id;
    }
  }
  return ret;
}

int StartArchiveHelper::get_archived_info_from_archive_key_(const int64_t piece_id, const int64_t piece_create_date,
    const bool is_cur_piece, bool& is_first_piece, bool& is_archive_key_complete)
{
  // only non_first_piece will assign to max_archived_index_info_
  int ret = OB_SUCCESS;
  ObArchiveIO archive_io(storage_info_);
  ObArchiveKeyContent archive_key_content;
  is_first_piece = false;
  is_archive_key_complete = false;
  if (OB_FAIL(archive_io.get_archived_info_from_archive_key(
          pg_key_, incarnation_, archive_round_, piece_id, piece_create_date, archive_key_content)) &&
      OB_ENTRY_NOT_EXIST != ret) {
    ARCHIVE_LOG(WARN, "failed to get_archived_info_from_archive_key", KR(ret), KPC(this));
  } else if (OB_ENTRY_NOT_EXIST == ret || (!archive_key_content.check_integrity())) {
    // file not exist or content is not integrity
    if (is_index_file_exists_()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN,
          "archive_key_file is corrupted",
          KR(ret),
          KPC(this),
          K(piece_id),
          K(piece_create_date),
          K(is_cur_piece),
          K(archive_key_content));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    is_archive_key_complete = true;
    if (archive_key_content.is_first_piece_) {
      is_first_piece = true;
      if (OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id_) {
        max_index_file_id_ = 0;
      }
    } else if (!archive_key_content.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(WARN, "invalid archive_key_content", KR(ret), KPC(this), K(archive_key_content));
    } else {
      if (OB_FAIL(max_archived_index_info_.set_round_start_info(archive_key_content.index_info_))) {
        ARCHIVE_LOG(ERROR, "failed to set_round_start_info", K(archive_key_content), KR(ret));
      } else if (OB_FAIL(max_archived_index_info_.set_log_info(archive_key_content.index_info_.max_log_id_,
                     archive_key_content.index_info_.max_checkpoint_ts_,
                     archive_key_content.index_info_.max_log_submit_ts_,
                     archive_key_content.index_info_.clog_epoch_id_,
                     archive_key_content.index_info_.accum_checksum_))) {
        ARCHIVE_LOG(ERROR, "failed to set_log_info", KR(ret), K(archive_key_content), KPC(this));
      } else if (OB_INVALID_ARCHIVE_FILE_ID == max_index_file_id_) {
        max_index_file_id_ = 0;
      }
      ARCHIVE_LOG(INFO, "finish getting archived_info from archive_key", K(archive_key_content), KPC(this), KR(ret));
    }
  }
  return ret;
}

int StartArchiveHelper::list_and_check_data_file_with_no_index_file_(
    const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  uint64_t min_data_file_id = OB_INVALID_ARCHIVE_FILE_ID;
  uint64_t max_data_file_id = OB_INVALID_ARCHIVE_FILE_ID;
  ObArchiveIO archive_io(storage_info_);

  if (OB_FAIL(archive_io.get_data_file_range(
          pg_key_, incarnation_, archive_round_, piece_id, piece_create_date, min_data_file_id, max_data_file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "failed to get_data_file_range", KR(ret), KPC(this));
    } else {
      ARCHIVE_LOG(INFO, "no data file exist", KR(ret), KPC(this));
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
    data_file_meta_.need_record_index_ = true;
    data_file_meta_.data_file_id_ = min_data_file_id;
    max_data_file_id_ = min_data_file_id;
    ARCHIVE_LOG(INFO,
        "list_and_check_data_file_with_no_index_file_",
        K(min_data_file_id),
        K(max_data_file_id),
        K(pg_key_),
        K(piece_id),
        K(piece_create_date));
  }

  return ret;
}

int StartArchiveHelper::check_if_exist_data_file_not_record_(
    const int64_t piece_id, const int64_t piece_create_date, const uint64_t file_id)
{
  int ret = OB_SUCCESS;
  char data_file_path[MAX_FILE_PATH];
  bool file_exist = false;
  ObArchiveIO archive_io(storage_info_);
  ObArchivePathUtil util;

  if (OB_FAIL(util.build_archive_file_path(pg_key_,
          LOG_ARCHIVE_FILE_TYPE_DATA,
          file_id,
          incarnation_,
          archive_round_,
          piece_id,
          piece_create_date,
          MAX_FILE_PATH,
          data_file_path))) {
    ARCHIVE_LOG(WARN, "build_archive_file_path fail", KR(ret), K(pg_key_));
  } else if (OB_FAIL(archive_io.check_file_exist(pg_key_, data_file_path, file_exist))) {
    ARCHIVE_LOG(WARN, "check_file_exist fail", KR(ret), K(file_id), KPC(this));
  } else if (!file_exist) {
    ARCHIVE_LOG(DEBUG, "NO data file exist not record in index file", K(file_id), K(data_file_path), K(pg_key_));
  } else {
    data_file_meta_.need_record_index_ = true;
    data_file_meta_.data_file_id_ = file_id;
    max_data_file_id_ = file_id;
    ARCHIVE_LOG(INFO,
        "check_if_exist_data_file_not_record_",
        K(pg_key_),
        K(piece_id),
        K(piece_create_date),
        K(max_data_file_id_));
  }

  return ret;
}

int StartArchiveHelper::extract_archive_log_from_data_file_(const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(quick_extract_archive_log_from_data_file_(piece_id, piece_create_date))) {
    ARCHIVE_LOG(INFO, "quick_extract_archive_log_from_data_file_ succ", KPC(this));
  } else if (OB_SUCC(iterate_archive_log_from_data_file_(piece_id, piece_create_date))) {
    ARCHIVE_LOG(INFO, "iterate_archive_log_from_data_file_ succ", KPC(this));
  } else {
    ARCHIVE_LOG(WARN, "extract_archive_log_from_data_file_ fail", KPC(this));
  }

  return ret;
}

int StartArchiveHelper::quick_extract_archive_log_from_data_file_(
    const int64_t piece_id, const int64_t piece_create_date)
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

  const uint64_t file_id = data_file_meta_.data_file_id_;
  if (OB_UNLIKELY(0 == file_id || OB_INVALID_ARCHIVE_FILE_ID == file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid data file id", KR(ret), K(data_file_meta_), K(file_id));
  } else if (OB_FAIL(file_store.init(uri_.ptr(),
                 storage_info_.ptr(),
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation_,
                 archive_round_,
                 piece_id,
                 piece_create_date))) {
    ARCHIVE_LOG(WARN, "init file_store fail", KR(ret), KPC(this), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(path_util.build_archive_file_path(pg_key_,
                 LOG_ARCHIVE_FILE_TYPE_DATA,
                 file_id,
                 incarnation_,
                 archive_round_,
                 piece_id,
                 piece_create_date,
                 MAX_FILE_PATH,
                 path))) {
    ARCHIVE_LOG(
        WARN, "build_archive_file_path fail", KR(ret), K(pg_key_), K(file_id), K(piece_id), K(piece_create_date));
  } else {
    ObString uri(path);
    ObArchiveFileUtils utils;
    if (OB_FAIL(utils.extract_last_log_in_data_file(
            pg_key_, file_id, &file_store, uri, storage_info_, block_meta, log_entry, inner_log))) {
      ARCHIVE_LOG(WARN, "extract_last_log_in_data_file fail", KR(ret), K(pg_key_), K(uri));
    } else {
      rebuild_archive_progress_from_block_meta_(block_meta);
      const ObLogEntryHeader& log_header = log_entry.get_header();
      is_kickoff = is_archive_kickoff_log(log_header.get_log_type());
      if (is_kickoff) {
        if (OB_FAIL(rebuild_start_archive_info_from_kickoff_log_(block_meta, log_header, inner_log))) {
          ARCHIVE_LOG(WARN, "rebuild_start_archive_info_from_kickoff_log_", K(pg_key_));
        }
      }
    }

    // if start archive info still not be found,
    // kickoff log must be the first block in the file
    if (OB_SUCC(ret) && !round_start_info_.is_valid() && !is_kickoff) {
      ObArchiveBlockMeta first_block_meta;
      clog::ObLogEntry first_log_entry;
      ObLogArchiveInnerLog first_inner_log;
      if (OB_FAIL(utils.extract_first_log_in_data_file(
              pg_key_, file_id, &file_store, first_block_meta, first_log_entry, first_inner_log))) {
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

int StartArchiveHelper::iterate_archive_log_from_data_file_(const int64_t piece_id, const int64_t piece_create_date)
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT = 60 * 1000 * 1000L;
  const uint64_t tenant_id = pg_key_.get_tenant_id();
  const char* cluster_name = GCTX.config_->cluster;
  const int64_t cluster_id = GCTX.config_->cluster_id;

  ObArchiveEntryIterator iter;
  ObArchiveLogFileStore file_store;
  const bool need_limit_bandwidth = false;
  const uint64_t real_tenant_id = pg_key_.get_tenant_id();
  const uint64_t file_id = data_file_meta_.data_file_id_;
  if (OB_UNLIKELY(0 == file_id || OB_INVALID_ARCHIVE_FILE_ID == file_id)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid data file id", KR(ret), K(file_id), KPC(this));
  } else if (OB_FAIL(file_store.init(uri_.ptr(),
                 storage_info_.ptr(),
                 cluster_name,
                 cluster_id,
                 tenant_id,
                 incarnation_,
                 archive_round_,
                 piece_id,
                 piece_create_date))) {
    ARCHIVE_LOG(WARN, "init file_store fail", KR(ret), KPC(this), K(cluster_name), K(cluster_id));
  } else if (OB_FAIL(iter.init(&file_store, pg_key_, file_id, 0, TIMEOUT, need_limit_bandwidth, real_tenant_id))) {
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator init fail", KR(ret), K(pg_key_));
  } else {
    clog::ObLogEntry log_entry;
    bool has_valid_block = false;
    bool unused_flag = false;
    int64_t unused_accum_checksum = 0;
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = iter.next_entry(log_entry, unused_flag, unused_accum_checksum))) {
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
    ARCHIVE_LOG(ERROR, "not kickoff log", KR(ret), K(log_header), K(block_meta), K(round_start_info_));
  } else if (round_start_info_.is_valid()) {
    // just skip, in old version code, kickoff may be be file 2, when index record for file 1 has
    // already include round_start_info
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
  data_file_meta_.min_log_id_ = block_meta.min_log_id_in_file_;
  data_file_meta_.min_log_ts_ = block_meta.min_log_ts_in_file_;
  data_file_meta_.max_log_id_ = block_meta.max_log_id_;
  data_file_meta_.max_log_ts_ = block_meta.max_log_submit_ts_;
  data_file_meta_.max_checkpoint_ts_ = block_meta.max_checkpoint_ts_;
  data_file_meta_.has_effective_data_ = true;

  max_archived_log_info_.max_log_id_archived_ = block_meta.max_log_id_;
  max_archived_log_info_.max_checkpoint_ts_archived_ = block_meta.max_checkpoint_ts_;
  max_archived_log_info_.max_log_submit_ts_archived_ = block_meta.max_log_submit_ts_;
  max_archived_log_info_.clog_epoch_id_ = block_meta.clog_epoch_id_;
  max_archived_log_info_.accum_checksum_ = block_meta.accum_checksum_;
}

int StartArchiveHelper::decide_pg_start_archive_log_info_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_cluster_archive_status_())) {
    ARCHIVE_LOG(WARN, "get_cluster_archive_status_ fail", KR(ret));
  } else {
    if (!has_remote_data()) {
      if (OB_FAIL(decide_pg_start_archive_log_id_locally_())) {
        ARCHIVE_LOG(WARN, "decide_pg_start_archive_log_id_locally_ fail", KR(ret), KPC(this));
      }
    } else {
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

}  // namespace archive
}  // namespace oceanbase
