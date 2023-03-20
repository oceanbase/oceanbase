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

#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_struct.h"   // ObArchiveRoundState
#include "ob_archive_define.h"                // ObArchiveInterruptReason
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLSID;
};
namespace archive
{
using oceanbase::share::ObArchiveRoundState;
class ObArchiveRoundMgr
{
public:
  ObArchiveRoundMgr();
  ~ObArchiveRoundMgr();

public:
  int init();
  void destroy();
   void get_round(ArchiveKey &key) const { key = key_; }
  bool is_compatible() const {return compatible_;}
  int set_archive_start(const ArchiveKey &key,
      const share::SCN &round_start_scn,
      const int64_t piece_switch_interval,
      const share::SCN &genesis_scn,
      const int64_t base_piece_id,
      const share::ObTenantLogArchiveStatus::COMPATIBLE compatible,
      const share::ObBackupDest &dest);
  void set_archive_force_stop(const ArchiveKey &key);
  void set_archive_interrupt(const ArchiveKey &key);
  void set_archive_suspend(const ArchiveKey &key);
  int get_backup_dest(const ArchiveKey &key,
      share::ObBackupDest &dest);
  int get_piece_info(const ArchiveKey &key,
      int64_t &piece_switch_interval,
      share::SCN &genesis_scn,
      int64_t &base_piece_id);
  void get_archive_round_info(ArchiveKey &key, ObArchiveRoundState &state) const;
  int get_archive_start_scn(const ArchiveKey &key, share::SCN &scn);
  void get_archive_round_compatible(ArchiveKey &key, bool &compatible);
  bool is_in_archive_status(const ArchiveKey &key) const;
  bool is_in_suspend_status(const ArchiveKey &key) const;
  bool is_in_archive_stopping_status(const ArchiveKey &key) const;
  bool is_in_archive_stop_status(const ArchiveKey &key) const;
  void update_log_archive_status(const ObArchiveRoundState::Status status);
  int mark_fatal_error(const share::ObLSID &id, const ArchiveKey &key, const ObArchiveInterruptReason &reason);

  void set_has_handle_error(bool has_handle);
  TO_STRING_KV(K_(key),
               K_(round_start_scn),
               K_(compatible),
               K_(log_archive_state),
               K_(backup_dest));

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

private:
  ArchiveKey            key_;
  share::SCN            round_start_scn_;
  int64_t               piece_switch_interval_;
  int64_t               base_piece_id_;
  share::SCN            genesis_scn_;
  bool                  compatible_;            // 该轮次兼容性处理
  ObArchiveRoundState   log_archive_state_;
  share::ObBackupDest   backup_dest_;
  RWLock                rwlock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveRoundMgr);
};
} // namespace archive
} // namespace oceanbase


#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_MGR_H_ */
