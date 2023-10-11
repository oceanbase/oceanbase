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

#include "share/backup/ob_archive_piece.h"
#include "share/ob_ls_id.h"           // ObLSID
#include "share/backup/ob_backup_struct.h"    // ObBackupPathString
#include "logservice/palf/lsn.h"      // LSN
#include "share/scn.h"      // SCN
#include "ob_archive_define.h"        // ArchiveWorkStation
#include "ob_archive_persist_mgr.h"   // ObArchivePersistMgr ObLSArchivePersistInfo

namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace logservice
{
class ObLogService;
}

namespace palf
{
struct LSN;
}
namespace archive
{
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;

class StartArchiveHelper
{
public:
  explicit StartArchiveHelper(const ObLSID &id,
      const uint64_t tenant_id,
      const ArchiveWorkStation &station,
      const share::SCN &min_scn,
      const int64_t piece_interval,
      const share::SCN &genesis_scn,
      const int64_t base_piece_id,
      ObArchivePersistMgr *persist_mgr);

  ~StartArchiveHelper();

public:
  bool is_valid() const;
  int handle();
  const ObLSID &get_ls_id() const { return id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ArchiveWorkStation &get_station() const { return station_; }
  const LSN &get_piece_min_lsn() const { return piece_min_lsn_; }
  const LSN &get_max_no_limit_lsn() const { return max_offset_; }
  const LSN &get_offset() const { return start_offset_; }
  int64_t get_file_id() const { return archive_file_id_; }
  int64_t get_file_offset() const { return archive_file_offset_; }
  const share::SCN &get_round_start_scn() const { return min_scn_; }
  const share::SCN &get_max_archived_scn() const { return max_archived_scn_; }
  const share::ObArchivePiece &get_piece() const { return piece_; }
  bool is_log_gap_exist() const { return log_gap_exist_; }
  TO_STRING_KV(K_(id),
               K_(station),
               K_(log_gap_exist),
               K_(min_scn),
               K_(piece_interval),
               K_(genesis_scn),
               K_(base_piece_id),
               K_(piece_min_lsn),
               K_(max_offset),
               K_(start_offset),
               K_(archive_file_id),
               K_(archive_file_offset),
               K_(max_archived_scn),
               K_(piece));

private:
  int load_inner_log_archive_status_(ObLSArchivePersistInfo &info);
  int fetch_exist_archive_progress_(bool &record_exist);
  int locate_round_start_archive_point_();
  int cal_archive_file_id_offset_(const LSN &lsn, const int64_t archive_file_id, const int64_t archive_file_offset);
  int get_local_base_lsn_(palf::LSN &lsn, bool &log_gap);
  int get_local_start_scn_(share::SCN &scn);

private:
  ObLSID id_;
  uint64_t tenant_id_;
  ArchiveWorkStation station_;
  bool log_gap_exist_;
  share::SCN min_scn_;
  int64_t piece_interval_;
  share::SCN genesis_scn_;
  int64_t base_piece_id_;
  LSN piece_min_lsn_;
  LSN max_offset_;    // archive_lag_target with noneffective smaller than this lsn
  LSN start_offset_;
  int64_t archive_file_id_;
  int64_t archive_file_offset_;
  share::SCN max_archived_scn_;
  share::ObArchivePiece piece_;

  ObArchivePersistMgr *persist_mgr_;
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_START_ARCHIVE_HELPER_H_ */
