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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_ROUND_MGR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_ROUND_MGR_H_

#include "stdint.h"
#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "ob_log_archive_struct.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace archive {
class ObArchiveRoundMgr {
public:
  ObArchiveRoundMgr();
  ~ObArchiveRoundMgr();

  enum LogArchiveStatus {
    LOG_ARCHIVE_INVALID_STATUS = 0,
    LOG_ARCHIVE_BEGINNING,
    LOG_ARCHIVE_DOING,
    LOG_ARCHIVE_IN_STOPPING,
    LOG_ARCHIVE_STOPPED,
    LOG_ARCHIVE_MAX
  };

public:
  int init();
  void destroy();
  int64_t get_current_archive_round() const
  {
    return current_archive_round_;
  }
  int64_t get_current_archive_incarnation() const
  {
    return incarnation_;
  }
  int64_t get_cur_piece_id() const
  {
    return cur_piece_id_;
  }
  int64_t get_cur_piece_create_date() const
  {
    return cur_piece_create_date_;
  }
  bool is_compatible() const
  {
    return compatible_;
  }
  bool is_oss() const
  {
    return is_oss_;
  }
  int64_t get_total_pg_count() const
  {
    return total_pg_count_;
  }
  bool get_add_pg_finish_flag() const
  {
    return add_pg_finish_;
  }
  void set_add_pg_finish_flag();
  void inc_total_pg_count();
  void dec_total_pg_count();
  void inc_started_pg();
  const char* get_storage_info() const
  {
    return storage_info_;
  }
  // return OB_EAGAIN wheren incarnation or archive_round is not the same
  int mark_fatal_error(const common::ObPartitionKey& pg_key, const int64_t incarnation, const int64_t archive_round);
  // used for observer report archive_status to rs
  bool has_encounter_fatal_error(const int64_t incarnation, const int64_t archive_round);
  int set_archive_start(const int64_t incarnation, const int64_t archive_round, const int64_t piece_id,
      const int64_t piece_create_date, const bool is_oss, const share::ObTenantLogArchiveStatus::COMPATIBLE compatible);
  void set_archive_force_stop(const int64_t incarnation, const int64_t archive_round);
  int update_cur_piece_info(const int64_t incarnation, const int64_t archive_round, const int64_t new_piece_id,
      const int64_t new_piece_create_date);
  void get_archive_round_info(int64_t& incarnation, int64_t& archive_round, int64_t& cur_piece_id,
      int64_t& cur_piece_create_date, bool& is_oss, LogArchiveStatus& log_archive_status,
      bool& has_encount_error) const;
  void get_archive_round_compatible(int64_t& incarnation, int64_t& archive_round, bool& compatible);
  bool need_handle_error();

  bool is_in_archive_invalid_status() const;
  bool is_in_archive_status() const;
  bool is_in_archive_beginning_status() const;
  bool is_in_archive_doing_status() const;
  bool is_in_archive_stopping_status() const;
  bool is_in_archive_stopped_status() const;
  bool is_server_archive_stop(const int64_t incarnation, const int64_t round);
  void update_log_archive_status(const LogArchiveStatus status);
  LogArchiveStatus get_log_archive_status();

  void set_has_handle_error(bool has_handle);
  TO_STRING_KV(K(add_pg_finish_), K(total_pg_count_), K(started_pg_count_), K(incarnation_), K(current_archive_round_),
      K(cur_piece_id_), K(cur_piece_create_date_), K(compatible_), K(is_oss_), K(root_path_), K(storage_info_));
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

public:
  // true afterall partitions archive kickoff log, then archive data
  bool add_pg_finish_;

  int64_t total_pg_count_;
  int64_t started_pg_count_;

  int64_t incarnation_;
  int64_t current_archive_round_;
  int64_t cur_piece_id_;
  int64_t cur_piece_create_date_;
  bool compatible_;

  bool is_oss_;  // no need to create dir before writing a file
  char root_path_[OB_MAX_ARCHIVE_PATH_LENGTH];
  char storage_info_[OB_MAX_ARCHIVE_STORAGE_INFO_LENGTH];

  // archive_round status
  bool has_handle_error_;
  bool has_encount_error_;
  LogArchiveStatus log_archive_status_;
  RWLock rwlock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveRoundMgr);
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_MGR_H_ */
