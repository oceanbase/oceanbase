// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "logservice/ob_log_service.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx/ob_dup_table_ts_sync.h"
#include "storage/tx/ob_dup_table_util.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

namespace transaction
{

void DupTableTsInfo::update(const DupTableTsInfo &ts_info)
{
  max_replayed_scn_ = share::SCN::max(max_replayed_scn_, ts_info.max_replayed_scn_);
  max_read_version_ = share::SCN::max(max_read_version_, ts_info.max_read_version_);
  max_commit_version_ = share::SCN::max(max_commit_version_, ts_info.max_commit_version_);
}

int ObDupTableLSTsSyncMgr::init(ObDupTableLSHandler *dup_ls_handle)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ts_info_cache_.create(32, "DUP_TABLE"))) {
    DUP_TABLE_LOG(WARN, "create ts info map failed", K(ret));
  } else {
    ls_id_ = dup_ls_handle->get_ls_id();
    is_stopped_ = false;
    is_master_ = false;
    dup_ls_handle_ptr_ = dup_ls_handle;
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::offline()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(ts_sync_lock_);
  if (OB_FAIL(clean_ts_info_cache_())) {
    DUP_TABLE_LOG(WARN, "clean ts info cache", K(ret), KPC(this));
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::validate_replay_ts(const common::ObAddr &dst,
                                              const share::SCN &target_replay_scn,
                                              const ObTransID &tx_id,
                                              bool &replay_all_redo,
                                              share::SCN &max_read_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  DupTableTsInfo tmp_ts_info;
  max_read_version.reset();
  replay_all_redo = false;

  SpinRLockGuard r_guard(ts_sync_lock_);

  if (OB_FAIL(get_ts_info_cache_(dst, tmp_ts_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "get ts info cache failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && tmp_ts_info.max_replayed_scn_ >= target_replay_scn) {
    replay_all_redo = true;
    max_read_version = tmp_ts_info.max_read_version_;
  } else if (OB_HASH_NOT_EXIST == ret
             || (OB_SUCC(ret) && tmp_ts_info.max_replayed_scn_ < target_replay_scn)) {
    replay_all_redo = false;
    max_read_version.reset();
    if (OB_TMP_FAIL(request_ts_info_by_rpc_(dst, share::SCN::min_scn()))) {
      DUP_TABLE_LOG(WARN, "request ts info by rpc failed", K(ret), K(tmp_ret), K(ls_id_));
    }
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (!replay_all_redo) {
    DUP_TABLE_LOG(INFO, "the dst follower has not replay all redo", K(ls_id_), K(tx_id), K(dst),
                  K(tmp_ts_info), K(target_replay_scn), K(replay_all_redo), K(max_read_version));
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::validate_commit_version(const common::ObAddr &dst,
                                                   share::SCN target_commit_version)
{
  int ret = OB_SUCCESS;

  DupTableTsInfo tmp_ts_info;
  SpinRLockGuard r_guard(ts_sync_lock_);

  if (OB_FAIL(get_ts_info_cache_(dst, tmp_ts_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "get ts info cache failed", K(ret));
    }
  }

  if (OB_HASH_NOT_EXIST == ret
      || (OB_SUCC(ret) && tmp_ts_info.max_commit_version_ < target_commit_version)) {
    if (OB_FAIL(request_ts_info_by_rpc_(dst, target_commit_version))) {
      DUP_TABLE_LOG(WARN, "request ts info by rpc failed", K(ret));
    }
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::update_all_ts_info_cache()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t err_cnt = 0;
  int64_t total_cnt = 0;

  DupTableTsInfo local_ts_info;
  if (OB_FAIL(get_local_ts_info(local_ts_info))) {
    DUP_TABLE_LOG(WARN, "get local ts info failed", K(ret));
  }

  SpinRLockGuard r_guard(ts_sync_lock_);

  const int64_t current_ts = ObTimeUtility::current_time();
  if (current_ts - last_refresh_ts_info_cache_ts_ > TS_INFO_CACHE_REFRESH_INTERVAL) {
    DupTableTsInfoMap::iterator cache_iter = ts_info_cache_.begin();
    while (OB_SUCC(ret) && cache_iter != ts_info_cache_.end()) {
      if (OB_TMP_FAIL(
              request_ts_info_by_rpc_(cache_iter->first, local_ts_info.max_commit_version_))) {
        err_cnt++;
        DUP_TABLE_LOG(WARN, "request ts info by rpc failed", K(tmp_ret));
      }
      cache_iter++;
      total_cnt++;
    }
    last_refresh_ts_info_cache_ts_ = current_ts;
  }
  // if (err_cnt > 0) {
  //   DUP_TABLE_LOG(WARN, "update some ts info cache failed", K(err_cnt), K(ret), K(tmp_ret));
  // }

  if (total_cnt > 0) {
    DUP_TABLE_LOG(DEBUG, "update ts info cache", K(ret), K(err_cnt), K(total_cnt));
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::request_ts_info(const common::ObAddr &dst)
{
  // only post msg , not need ts_sync_lock_
  return request_ts_info_by_rpc_(dst, share::SCN::min_scn());
}

int ObDupTableLSTsSyncMgr::leader_takeover()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard w_guard(ts_sync_lock_);

  ret = clean_ts_info_cache_();

  ATOMIC_STORE(&is_master_, true);

  return ret;
}

int ObDupTableLSTsSyncMgr::leader_revoke()
{
  int ret = OB_SUCCESS;

  // SpinWLockGuard w_guard(ts_sync_lock_);
  ATOMIC_STORE(&is_master_, false);
  return ret;
}

int ObDupTableLSTsSyncMgr::clean_ts_info_cache_()
{
  int ret = OB_SUCCESS;

  if (!ts_info_cache_.empty()) {
    ts_info_cache_.clear();
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::handle_ts_sync_request(const ObDupTableTsSyncRequest &ts_sync_req)
{
  int ret = OB_SUCCESS;
  DupTableTsInfo local_ts_info;

  SpinRLockGuard r_guard(ts_sync_lock_);

  if (!ts_sync_req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ts_sync_req));
  } else {
    // MTL(ObTransService *)
    //     ->get_tx_version_mgr()
    //     .update_max_commit_ts(ts_sync_req.get_max_commit_scn(), false);
    if (OB_FAIL(get_local_ts_info(local_ts_info))) {
      DUP_TABLE_LOG(WARN, "get local ts info failed", K(ret));
    } else {
      ObDupTableTsSyncResponse ts_sync_reps(local_ts_info.max_replayed_scn_,
                                            local_ts_info.max_commit_version_,
                                            local_ts_info.max_read_version_);

      const ObLSID &cur_ls_id = ls_id_;
      ObAddr leader_addr = ts_sync_req.get_src();
      ObILocationAdapter *location_adapter = MTL(ObTransService *)->get_location_adapter();

      if (OB_FAIL(location_adapter->nonblock_get_leader(GCONF.cluster_id, MTL_ID(), cur_ls_id,
                                                        leader_addr))) {
        DUP_TABLE_LOG(WARN, "get ls leader failed", K(ret), K(leader_addr), K(cur_ls_id),
                      K(MTL_ID()));
        (void)location_adapter->nonblock_renew(GCONF.cluster_id, MTL_ID(), cur_ls_id);
      } else if (leader_addr != ts_sync_req.get_src()) {
        DUP_TABLE_LOG(INFO, "The leader addr is not the src", K(leader_addr), K(ts_sync_req));
      }

      ts_sync_reps.set_header(ts_sync_req.get_dst(), leader_addr, ts_sync_req.get_dst(), cur_ls_id);

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(MTL(ObTransService *)
                             ->get_dup_table_rpc_impl()
                             .post_msg(leader_addr, ts_sync_reps))) {
        DUP_TABLE_LOG(WARN, "post ts sync response failed", K(ret));
      } else {
        DUP_TABLE_LOG(DEBUG, "post ts sync response success", K(ret), K(ts_sync_reps));
      }
    }
  }
  return ret;
}

int ObDupTableLSTsSyncMgr::handle_ts_sync_response(const ObDupTableTsSyncResponse &ts_sync_reps)
{
  int ret = OB_SUCCESS;

  DupTableTsInfo tmp_ts_info;
  tmp_ts_info.max_replayed_scn_ = ts_sync_reps.get_max_replayed_scn();
  tmp_ts_info.max_commit_version_ = ts_sync_reps.get_max_commit_scn();
  tmp_ts_info.max_read_version_ = ts_sync_reps.get_max_read_scn();

  SpinWLockGuard w_guard(ts_sync_lock_);

  if (!ts_sync_reps.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ts_sync_reps));
  } else if (OB_FAIL(update_ts_info_(ts_sync_reps.get_src(), tmp_ts_info))) {
    DUP_TABLE_LOG(WARN, "update ts info failed", K(ret), K(ts_sync_reps));
  } else {
    DUP_TABLE_LOG(DEBUG, "handle ts sync response success", K(ret), K(ts_sync_reps),
                  K(tmp_ts_info));
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::request_ts_info_by_rpc_(const common::ObAddr &addr,
                                                   const share::SCN &leader_commit_scn)
{
  int ret = OB_SUCCESS;

  const common::ObAddr self_addr = MTL(ObTransService *)->get_server();
  ObDupTableTsSyncRequest ts_sync_req(leader_commit_scn);
  ts_sync_req.set_header(self_addr, addr, self_addr, ls_id_);

  if (OB_FAIL(MTL(ObTransService *)->get_dup_table_rpc_impl().post_msg(addr, ts_sync_req))) {
    DUP_TABLE_LOG(WARN, "post ts sync request failed", K(ret));
  }
  return ret;
}

int ObDupTableLSTsSyncMgr::update_ts_info_(const common::ObAddr &addr,
                                           const DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;
  DupTableTsInfo tmp_ts_info;

  if (OB_FAIL(ts_info_cache_.get_refactored(addr, tmp_ts_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "get ts info cache failed", K(ret), K(addr), K(ts_info));
    } else {
      DUP_TABLE_LOG(INFO, "it is a new ts info which has not cached", K(ret), K(addr));
      ret = OB_SUCCESS;
    }
  }

  tmp_ts_info.update(ts_info);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ts_info_cache_.set_refactored(addr, tmp_ts_info, 1))) {
    DUP_TABLE_LOG(WARN, "set ts info failed", K(ret));
  }
  DUP_TABLE_LOG(DEBUG, "update ts info", K(ret), K(addr), K(tmp_ts_info));
  return ret;
}

int ObDupTableLSTsSyncMgr::get_ts_info_cache_(const common::ObAddr &addr, DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ts_info_cache_.get_refactored(addr, ts_info))) {
    DUP_TABLE_LOG(WARN, "get ts info cache failed", K(addr), K(ts_info));
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::get_local_ts_info(DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;

  DupTableTsInfo tmp_ts_info;

  // We need get max_replayed_scn before max_read_version and max_commit_version.
  // Because max_read_version must be acquired after replaying before_prepare
  if (OB_FAIL(dup_ls_handle_ptr_->get_log_handler()->get_max_decided_scn(
          tmp_ts_info.max_replayed_scn_))) {
    DUP_TABLE_LOG(WARN, "get max replayed ts failed", K(ret));
  } else if (OB_FAIL(dup_ls_handle_ptr_->check_and_update_max_replayed_scn(
                 tmp_ts_info.max_replayed_scn_))) {
    DUP_TABLE_LOG(WARN, "invalid max replayed scn", K(ret), K(ls_id_),
                  K(tmp_ts_info.max_replayed_scn_));
  } else {
    tmp_ts_info.max_commit_version_ =
        MTL(ObTransService *)->get_tx_version_mgr().get_max_commit_ts(false);
    tmp_ts_info.max_read_version_ = MTL(ObTransService *)->get_tx_version_mgr().get_max_read_ts();
    ts_info.update(tmp_ts_info);
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::get_cache_ts_info(const common::ObAddr &addr, DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  {
    SpinRLockGuard guard(ts_sync_lock_);

    if (OB_FAIL(get_ts_info_cache_(addr, ts_info))) {
      DUP_TABLE_LOG(WARN, "get ts info cache failed", K(ret));
    }

    DUP_TABLE_LOG(DEBUG, "get ts info cache", K(ret), K(ret), K(ts_info));
  }

  if (OB_HASH_NOT_EXIST == ret) {
    if (OB_TMP_FAIL(request_ts_info(addr))) {
      DUP_TABLE_LOG(WARN, "request ts info failed", K(tmp_ret), K(ts_info));
    }
  }
  return ret;
}

int ObDupTableLSTsSyncMgr::get_lease_mgr_stat(ObDupLSLeaseMgrStatIterator &collect_iter,
                                              FollowerLeaseMgrStatArr &collect_arr)
{
  int ret = OB_SUCCESS;
  DupTableTsInfo tmp_info;
  SpinRLockGuard r_lock(ts_sync_lock_);

  for (int i = 0; i < collect_arr.count() && OB_SUCC(ret); i++) {
    ObDupTableLSLeaseMgrStat &tmp_stat = collect_arr.at(i);
    const common::ObAddr follower_addr = tmp_stat.get_follower_addr();

    if (OB_SUCC(ts_info_cache_.get_refactored(follower_addr, tmp_info))) {
      // if exist, update tmp_stat
      tmp_stat.set_max_replayed_scn(tmp_info.max_replayed_scn_);
      tmp_stat.set_max_commit_version(
          tmp_info.max_commit_version_.convert_to_ts(true /*ignore invalid*/));
      tmp_stat.set_max_read_version(
          tmp_info.max_read_version_.convert_to_ts(true /*ignore invalid*/));
    } else if (OB_HASH_NOT_EXIST == ret) {
      // rewrite retcode
      ret = OB_SUCCESS;
    } else {
      DUP_TABLE_LOG(WARN, "get ts info failed", K(ret));
    }
    // push into iter
    if (OB_SUCC(ret)) {
      if (OB_FAIL(collect_iter.push(tmp_stat))) {
        DUP_TABLE_LOG(WARN, "push into virtual iter failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDupTableLSTsSyncMgr::DiagInfoGenerator::operator()(
    const common::hash::HashMapPair<common::ObAddr, DupTableTsInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  const char *addr_str = to_cstring(hash_pair.first);

  ret = ::oceanbase::common::databuff_printf(
      info_buf_, info_buf_len_, info_buf_pos_,
      "%s[%sCached Ts Info] owner=%s, max_commit_version=%s, max_read_version=%s, "
      "max_replayed_scn=%s\n",
      DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, addr_str,
      to_cstring(hash_pair.second.max_commit_version_),
      to_cstring(hash_pair.second.max_read_version_),
      to_cstring(hash_pair.second.max_replayed_scn_));

  return ret;
}

void ObDupTableLSTsSyncMgr::print_ts_sync_diag_info_log(const bool is_master)
{
  int ret = OB_SUCCESS;

  SpinRLockGuard guard(ts_sync_lock_);

  const uint64_t TS_SYNC_PRINT_BUF_LEN =
      DupTableDiagStd::DUP_DIAG_INFO_LOG_BUF_LEN[DupTableDiagStd::TypeIndex::TS_SYNC_INDEX];
  // if (OB_NOT_NULL(dup_ls_handle_ptr_)) {
  const int64_t tenant_id = MTL_ID();
  const ObLSID ls_id = ls_id_;

  if (OB_ISNULL(ts_sync_diag_info_log_buf_)) {
    if (OB_ISNULL(ts_sync_diag_info_log_buf_ =
                      static_cast<char *>(ob_malloc(TS_SYNC_PRINT_BUF_LEN, "DupTableDiag")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _DUP_TABLE_LOG(WARN, "%salloc ts sync diag info buf failed, ret=%d, ls_id=%lu",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
    }
  }

  if (OB_SUCC(ret) && is_master) {
    DiagInfoGenerator diag_info_gen(ts_sync_diag_info_log_buf_, TS_SYNC_PRINT_BUF_LEN);
    if (OB_FAIL(hash_for_each_update(ts_info_cache_, diag_info_gen))) {
      _DUP_TABLE_LOG(WARN, "%sprint ts info cache failed, ret=%d, ls_id=%lu",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
    }

    ts_sync_diag_info_log_buf_[MIN(diag_info_gen.get_buf_pos(), TS_SYNC_PRINT_BUF_LEN - 1)] = '\0';

    _DUP_TABLE_LOG(INFO, "[%sTs Sync Cache] tenant: %lu, ls: %lu , ts_info_count: %lu\n%s",
                   DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, tenant_id, ls_id.id(),
                   ts_info_cache_.size(), ts_sync_diag_info_log_buf_);
  }
  // }
}

} // namespace transaction

} // namespace oceanbase
