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

#ifndef OCEANBASE_ARCHIVE_OB_START_ARCHIVE_HELPER_H_
#define OCEANBASE_ARCHIVE_OB_START_ARCHIVE_HELPER_H_

#include "common/ob_partition_key.h"        // ObPGKey
#include "lib/string/ob_string.h"           // ObString
#include "clog/ob_log_define.h"             // file_id_t
#include "ob_log_archive_struct.h"          // MaxArchivedIndexInfo
#include "share/backup/ob_backup_struct.h"  // ObLogArchiveStatus
#include "clog/ob_log_entry.h"
#include "clog/ob_log_type.h"

namespace oceanbase {
using clog::ObLogArchiveInnerLog;
namespace archive {
class ObArchiveLogWrapper;
class ObArchiveMgr;
struct ObMaxArchivedLogInfo {
public:
  ObMaxArchivedLogInfo()
  {
    reset();
  }
  ~ObMaxArchivedLogInfo()
  {
    reset();
  }
  void reset();
  TO_STRING_KV(K_(max_log_id_archived), K_(max_checkpoint_ts_archived), K_(max_log_submit_ts_archived),
      K_(clog_epoch_id), K_(accum_checksum));

public:
  uint64_t max_log_id_archived_;        // max archived log id
  int64_t max_checkpoint_ts_archived_;  // max archived checkpoint ts
  int64_t max_log_submit_ts_archived_;  // max archived log ts
  int64_t clog_epoch_id_;               // epoch_id of the max archived log
  int64_t accum_checksum_;              // accumulate_checksum of the max archived log
};

class StartArchiveHelper {
  static const int64_t MAX_FILE_PATH = OB_MAX_ARCHIVE_PATH_LENGTH;

public:
  StartArchiveHelper(const common::ObPGKey& pg_key, const int64_t timestamp, const int64_t incarnation,
      const int64_t archive_round, const int64_t epoch, const int64_t takeover_ts, const bool compatible,
      const int64_t start_archive_ts, common::ObString& uri, common::ObString& storage_info,
      ObArchiveLogWrapper& log_wrapper, ObArchiveMgr& archive_mgr);
  ~StartArchiveHelper();

public:
  bool is_valid();
  int handle();
  bool check_query_info_ready();
  TO_STRING_KV(K(pg_key_), K(takeover_ts_), K(create_timestamp_), K(round_start_info_), K(start_log_id_),
      K(max_archived_log_info_), K(min_index_file_id_), K(max_index_file_id_), K(index_record_exist_),
      K(max_archived_index_info_), K(min_data_file_id_), K(max_data_file_id_), K(data_file_exist_unrecorded_),
      K(min_log_id_unrecorded_), K(min_log_ts_unrecorded_), K(max_log_id_unrecorded_), K(max_checkpoint_ts_unrecorded_),
      K(max_log_submit_ts_unrecorded_), K(exist_log_));

private:
  int handle_pg_start_archive_();
  int confirm_start_log_exist_();
  int locate_pg_start_ilog_id_();
  int check_and_make_pg_archive_directory_();
  int query_max_file_remote_();
  int get_max_exist_index_file_id_();
  int get_max_exist_data_file_id_();
  int get_max_archived_index_info_();
  int list_and_check_data_file_with_no_index_file_();
  int reconfirm_unrecord_data_file_();
  int check_if_exist_data_file_not_record_(const uint64_t file_id);
  int extract_archive_log_from_data_file_();
  int quick_extract_archive_log_from_data_file_();
  int iterate_archive_log_from_data_file_();
  int rebuild_start_archive_info_from_kickoff_log_(
      const ObArchiveBlockMeta& block_meta, const clog::ObLogEntryHeader& log_entry, ObLogArchiveInnerLog& inner_log);
  void rebuild_archive_progress_from_block_meta_(const ObArchiveBlockMeta& block_meta);
  void get_log_info_from_index_info_();
  int decide_pg_next_start_file_id_();
  int decide_pg_start_archive_log_id_locally_();
  int decide_pg_start_archive_log_id_remote_();
  int decide_pg_start_archive_log_id_();
  int get_cur_pg_log_archive_checkpoint_ts_(int64_t& cur_checkpoint_ts);
  int decide_need_archive_first_record_();
  int get_cluster_archive_status_();
  int get_tenant_archive_status_();
  bool is_archive_status_beginning_();
  bool is_archive_status_doing_();
  int decide_start_log_id_on_beginning_();
  int decide_start_log_id_on_doing_();

public:
  bool success_;
  common::ObPGKey pg_key_;
  bool need_kickoff_log_;
  bool is_mandatory_;
  int64_t incarnation_;
  int64_t archive_round_;
  int64_t epoch_;
  int64_t takeover_ts_;
  int64_t create_timestamp_;
  ObArchiveRoundStartInfo round_start_info_;

  // the first log id should archive
  uint64_t start_log_id_;
  // the ilog file id corresponding to the start log id
  clog::file_id_t start_ilog_file_id_;

  ObMaxArchivedLogInfo max_archived_log_info_;

  // index file range
  uint64_t min_index_file_id_;
  uint64_t max_index_file_id_;
  // index file exists in backup dest or not
  bool index_file_exist_;
  // index info exists or not
  // (Maybe index file is exist while it is an empty file, then index info is not exist.)
  bool index_record_exist_;
  MaxArchivedIndexInfo max_archived_index_info_;

  // data file range
  uint64_t min_data_file_id_;
  uint64_t max_data_file_id_;
  bool data_file_exist_unrecorded_;
  // all data files are recorded in index files
  bool unrecorded_data_file_valid_;
  uint64_t min_log_id_unrecorded_;
  int64_t min_log_ts_unrecorded_;
  uint64_t max_log_id_unrecorded_;
  int64_t max_checkpoint_ts_unrecorded_;
  int64_t max_log_submit_ts_unrecorded_;

  uint64_t next_data_file_id_;
  uint64_t next_index_file_id_;

  // log exist in backup dest, start log id will be set max archived log id + 1
  bool exist_log_;
  bool compatible_;
  int64_t server_start_ts_;
  int64_t tenant_archive_checkpoint_ts_;
  share::ObLogArchiveStatus::STATUS archive_status_;
  int64_t rs_start_ts_;
  common::ObString uri_;
  common::ObString storage_info_;

  ObArchiveLogWrapper& log_wrapper_;
  ObArchiveMgr& archive_mgr_;
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_START_ARCHIVE_HELPER_H_ */
