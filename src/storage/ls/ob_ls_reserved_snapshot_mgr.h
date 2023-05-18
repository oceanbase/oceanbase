//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OB_STORAGE_STORAGE_LS_RESERVED_SNAPSHOT_MGR_H_
#define OB_STORAGE_STORAGE_LS_RESERVED_SNAPSHOT_MGR_H_

#include "logservice/ob_append_callback.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "storage/ob_storage_clog_recorder.h"
#include "logservice/ob_log_base_header.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
} // namespace palf

namespace storage
{
class ObLS;

class ObLSReservedSnapshotMgr : public ObIStorageClogRecorder
{
public:
  ObLSReservedSnapshotMgr();
  ~ObLSReservedSnapshotMgr();

  int init(const int64_t tenant_id, storage::ObLS *ls, logservice::ObLogHandler *log_handler);
  virtual void destroy() override;

  // for leader
  int try_sync_reserved_snapshot(const int64_t new_reserved_snapshot, const bool update_flag);
  // follower
  int replay_reserved_snapshot_log(const share::SCN &scn, const char *buf, const int64_t size, int64_t &pos);
  // operate with write_lock
  int add_dependent_medium_tablet(const ObTabletID tablet_id);
  int del_dependent_medium_tablet(const ObTabletID tablet_id);

  int64_t get_min_reserved_snapshot();

private:
  int update_min_reserved_snapshot_for_leader(const int64_t new_reserved_snapshot);
  int inner_update_reserved_snapshot(const int64_t reserved_snapshot);

  virtual int inner_replay_clog(
      const int64_t update_version,
      const share::SCN &scn,
      const char *buf,
      const int64_t size,
      int64_t &pos) override;
  virtual int sync_clog_succ_for_leader(const int64_t update_version) override;
  virtual void sync_clog_failed_for_leader() override
  {
    // do nothing
  }
  virtual int submit_log(
      const int64_t update_version,
      const char *clog_buf,
      const int64_t clog_len) override;
  virtual int prepare_struct_in_lock(
      int64_t &update_version,
      ObIAllocator *allocator,
      char *&clog_buf,
      int64_t &clog_len) override;
  virtual void free_struct_in_lock() override;

private:
  static const int64_t CLOG_BUF_LEN = sizeof(logservice::ObLogBaseHeader) + sizeof(int64_t);
  static const int64_t HASH_BUCKET = 64;
  static const int64_t PRINT_LOG_INTERVAL = 20 * 1000 * 1000L;
  OB_INLINE bool need_print_log()
  {
    bool bret = false;
    if (last_print_log_ts_ + PRINT_LOG_INTERVAL <= ObTimeUtility::fast_current_time()) {
      last_print_log_ts_ = ObTimeUtility::fast_current_time();
      bret = true;
    }
    return bret;
  }
  int sync_clog(const int64_t new_reserved_snapshot);

  bool is_inited_;
  common::ObArenaAllocator allocator_;
  int64_t min_reserved_snapshot_;
  int64_t next_reserved_snapshot_;
  mutable common::TCRWLock snapshot_lock_;
  lib::ObMutex sync_clog_lock_;
  storage::ObLS *ls_;
  ObLSHandle ls_handle_;
  common::hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> dependent_tablet_set_; // tablet_id
  ObStorageCLogCb clog_cb_;
  int64_t last_print_log_ts_;
  // clog part
  char clog_buf_[CLOG_BUF_LEN];
};

} // namespace storage
} // namespace oceanbase

#endif
