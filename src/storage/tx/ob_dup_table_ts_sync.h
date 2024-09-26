// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_TS_SYNC_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_TS_SYNC_H

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_dup_table_stat.h"

namespace oceanbase
{

namespace transaction
{

class ObDupTableTsSyncRequest;
class ObDupTableTsSyncResponse;
class ObDupTableLSHandler;

struct DupTableTsInfo
{
  // common::ObAddr addr_;
  share::SCN max_replayed_scn_;
  share::SCN max_read_version_;
  share::SCN max_commit_version_;
  // TODO last receive time

  DupTableTsInfo() { reset(); }
  void reset()
  {
    max_replayed_scn_.reset();
    max_read_version_.reset();
    max_commit_version_.reset();
  }
  void update(const DupTableTsInfo &ts_info);
  bool is_valid()
  {
    return max_replayed_scn_.is_valid() && max_read_version_.is_valid()
           && max_commit_version_.is_valid();
  }

  TO_STRING_KV(K(max_replayed_scn_), K(max_read_version_), K(max_commit_version_));
};

typedef common::hash::ObHashMap<common::ObAddr, DupTableTsInfo, common::hash::NoPthreadDefendMode>
    DupTableTsInfoMap;

class ObDupTableLSTsSyncMgr
{
public:
  const int64_t TS_INFO_CACHE_REFRESH_INTERVAL = 1 * 1000 * 1000; // 1s
public:
  ObDupTableLSTsSyncMgr() : ts_sync_diag_info_log_buf_(nullptr) {}
  ~ObDupTableLSTsSyncMgr() { destroy(); }
  int init(ObDupTableLSHandler *dup_ls_handle);
  int offline();

  void reset()
  {
    is_stopped_ = false;
    ls_id_.reset();
    is_master_ = false;

    last_refresh_ts_info_cache_ts_ = 0;
    dup_ls_handle_ptr_ = nullptr;
    ts_info_cache_.destroy();
    if (OB_NOT_NULL(ts_sync_diag_info_log_buf_)) {
      ob_free(ts_sync_diag_info_log_buf_);
    }
    ts_sync_diag_info_log_buf_ = nullptr;
  }

  void destroy() { reset(); }

  bool is_master() { return ATOMIC_LOAD(&is_master_); }

  // redo sync
  int validate_replay_ts(const common::ObAddr &dst,
                         const share::SCN &target_replay_scn,
                         const ObTransID &tx_id,
                         bool &replay_all_redo,
                         share::SCN &max_read_version);
  // pre_commit sync
  int validate_commit_version(const common::ObAddr &dst, const share::SCN target_commit_scn);

  // update all ts info cache
  int update_all_ts_info_cache();

  // try to sync ts info
  int request_ts_info(const common::ObAddr &dst);

  int leader_takeover();
  int leader_revoke();

  int handle_ts_sync_request(const ObDupTableTsSyncRequest &ts_sync_req);
  int handle_ts_sync_response(const ObDupTableTsSyncResponse &ts_sync_reps);

  int get_local_ts_info(DupTableTsInfo &ts_info);
  int get_cache_ts_info(const common::ObAddr &addr, DupTableTsInfo &ts_info);

  void print_ts_sync_diag_info_log(const bool is_master);

  int get_lease_mgr_stat(ObDupLSLeaseMgrStatIterator &collect_iter,
                         FollowerLeaseMgrStatArr &arr);

  TO_STRING_KV(K(ls_id_), K(ts_info_cache_.size()));
private:
  int clean_ts_info_cache_();
  int request_ts_info_by_rpc_(const common::ObAddr &addr, const share::SCN &leader_commit_scn);
  int update_ts_info_(const common::ObAddr &addr, const DupTableTsInfo &ts_info);
  int get_ts_info_cache_(const common::ObAddr &addr, DupTableTsInfo &ts_info);

  class DiagInfoGenerator
  {
  public:
    DiagInfoGenerator(char *info_buf, int64_t info_buf_len)
        : info_buf_(info_buf), info_buf_len_(info_buf_len), info_buf_pos_(0)
    {}

    int64_t get_buf_pos() { return info_buf_pos_; }

    int operator()(const common::hash::HashMapPair<common::ObAddr, DupTableTsInfo> &hash_pair);

  private:
    char *info_buf_;
    int64_t info_buf_len_;
    int64_t info_buf_pos_;
  };

private:
  SpinRWLock ts_sync_lock_;

  bool is_stopped_;
  share::ObLSID ls_id_;
  bool is_master_;

  ObDupTableLSHandler *dup_ls_handle_ptr_;
  DupTableTsInfoMap ts_info_cache_;

  int64_t last_refresh_ts_info_cache_ts_;

  char *ts_sync_diag_info_log_buf_;

  // dup table cb list order by ts
  // DupTableCbList replay_ts_list_;
  // DupTableCbList commit_ts_list_;
};
} // namespace transaction
} // namespace oceanbase

#endif
