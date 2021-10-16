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
namespace share {
struct ObLogArchiveSimpleInfo;
};
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
  bool is_valid() const;
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
  StartArchiveHelper(const common::ObPGKey& pg_key, const int64_t create_timestamp, const int64_t epoch,
      const int64_t incarnation, const int64_t archive_round, const int64_t takeover_ts, const bool compatible,
      const int64_t start_archive_ts, ObString& uri, ObString& storage_info, ObArchiveLogWrapper& log_wrapper,
      ObArchiveMgr& archive_mgr);
  ~StartArchiveHelper();

public:
  bool is_valid() const;
  int handle();
  // can recovery archive meta from backup dest
  bool has_remote_data() const
  {
    return (round_start_info_.is_valid() && max_archived_log_info_.is_valid());
  }
  uint64_t get_start_log_id() const
  {
    return max_archived_log_info_.max_log_id_archived_ + 1;
  }
  bool need_kickoff() const
  {
    return need_kickoff_;
  }
  TO_STRING_KV(K(pg_key_), K(success_), K(need_check_and_mkdir_), K(compatible_), K(need_kickoff_),
      K(need_switch_piece_on_beginning_), K(start_ilog_file_id_), K(max_index_file_id_), K(max_data_file_id_),
      K(epoch_), K(cur_piece_id_), K(cur_piece_create_date_), K(takeover_ts_), K(create_timestamp_),
      K(server_start_ts_), K(round_start_info_), K(max_archived_index_info_), K(data_file_meta_),
      K(max_archived_log_info_), "start_log_id", get_start_log_id());

private:
  int handle_pg_start_archive_();
  int locate_pg_start_ilog_file_id_();
  int check_and_make_pg_archive_directory_();
  bool has_effective_data_remote_() const
  {
    return max_archived_log_info_.is_valid();
  }
  bool is_archive_meta_complete_() const;
  int query_archive_meta_remote_();
  int get_piece_archive_meta_(const int64_t piece_id, const int64_t piece_create_date, const bool is_cur_piece);
  int get_max_exist_index_file_id_(const int64_t piece_id, const int64_t piece_create_date);
  int get_max_exist_data_file_id_(
      const int64_t piece_id, const int64_t piece_create_date, MaxArchivedIndexInfo& max_archived_index_info);
  int get_archived_info_from_index_(const int64_t piece_id, const int64_t piece_create_date);
  // invoke this in switch _piece
  int get_archived_info_from_archive_key_(const int64_t piece_id, const int64_t piece_create_date,
      const bool is_cur_piece, bool& is_first_piece, bool& is_archive_key_complete);
  int list_and_check_data_file_with_no_index_file_(const int64_t piece_id, const int64_t piece_create_date);
  int check_if_exist_data_file_not_record_(
      const int64_t piece_id, const int64_t piece_create_date, const uint64_t file_id);
  int extract_archive_log_from_data_file_(const int64_t piece_id, const int64_t piece_create_date);
  int quick_extract_archive_log_from_data_file_(const int64_t piece_id, const int64_t piece_create_date);
  int iterate_archive_log_from_data_file_(const int64_t piece_id, const int64_t piece_create_date);
  int rebuild_start_archive_info_from_kickoff_log_(
      const ObArchiveBlockMeta& block_meta, const clog::ObLogEntryHeader& log_entry, ObLogArchiveInnerLog& inner_log);
  void rebuild_archive_progress_from_block_meta_(const ObArchiveBlockMeta& block_meta);
  int decide_pg_start_archive_log_id_locally_();
  int decide_pg_start_archive_log_id_remote_();
  int decide_pg_start_archive_log_info_();
  int get_cur_pg_log_archive_checkpoint_ts_(int64_t& cur_checkpoint_ts);
  int get_cluster_archive_status_();
  int get_tenant_archive_status_(share::ObLogArchiveSimpleInfo& archive_info);
  bool is_archive_status_beginning_();
  bool is_archive_status_doing_();
  int decide_start_log_id_on_beginning_();
  int decide_start_log_id_on_doing_();
  bool archive_data_exists_() const;

  int check_and_extract_data_file_archive_info_(const int64_t piece_id, const int64_t piece_create_date);
  bool is_index_file_exists_() const
  {
    return 0 < max_index_file_id_ && OB_INVALID_ARCHIVE_FILE_ID != max_index_file_id_;
  }
  void generate_max_data_file_id_();

public:
  bool success_;
  bool need_check_and_mkdir_;
  bool compatible_;
  bool need_kickoff_;
  bool need_switch_piece_on_beginning_;

  clog::file_id_t start_ilog_file_id_;
  uint64_t max_index_file_id_;
  uint64_t max_data_file_id_;

  int64_t epoch_;
  int64_t incarnation_;
  int64_t archive_round_;
  int64_t cur_piece_id_;
  int64_t cur_piece_create_date_;

  int64_t takeover_ts_;
  int64_t create_timestamp_;
  int64_t server_start_ts_;
  int64_t tenant_archive_checkpoint_ts_;
  share::ObLogArchiveStatus::STATUS archive_status_;

  common::ObPGKey pg_key_;
  ObArchiveRoundStartInfo round_start_info_;
  MaxArchivedIndexInfo max_archived_index_info_;
  ObArchiveDataFileMeta data_file_meta_;
  ObMaxArchivedLogInfo max_archived_log_info_;

  common::ObString uri_;
  common::ObString storage_info_;

  ObArchiveLogWrapper& log_wrapper_;
  ObArchiveMgr& archive_mgr_;
};

}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_START_ARCHIVE_HELPER_H_ */
