// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

// #include "lib/utility/ob_print_utils.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx/ob_dup_table_lease.h"
#include "storage/tx/ob_dup_table_ts_sync.h"
#include "storage/tx/ob_dup_table_util.h"
#include "storage/tx/ob_location_adapter.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
namespace transaction
{

const int64_t ObDupTableLSLeaseMgr::LEASE_UNIT = ObDupTableLoopWorker::LOOP_INTERVAL;
const int64_t ObDupTableLSLeaseMgr::DEFAULT_LEASE_INTERVAL = ObDupTableLSLeaseMgr::LEASE_UNIT * 60;
const int64_t ObDupTableLSLeaseMgr::MIN_LEASE_INTERVAL = ObDupTableLSLeaseMgr::LEASE_UNIT * 60;

int ObDupTableLSLeaseMgr::init(ObDupTableLSHandler *dup_ls_handle)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_ls_handle)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(leader_lease_map_.create(32, "DUP_TABLE"))) {
    DUP_TABLE_LOG(WARN, "create leader_lease_map_ failed", K(ret));
  } else {
    follower_lease_info_.reset();
    dup_ls_handle_ptr_ = dup_ls_handle;
    is_master_ = false;
    is_stopped_ = false;
    ls_id_ = dup_ls_handle->get_ls_id();
  }

  return ret;
}

int ObDupTableLSLeaseMgr::offline()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lease_lock_);
  follower_lease_info_.reset();
  leader_lease_map_.clear();
  return ret;
}

void ObDupTableLSLeaseMgr::reset()
{
  ls_id_.reset();
  is_master_ = false;
  is_stopped_ = true;
  dup_ls_handle_ptr_ = nullptr;
  leader_lease_map_.destroy();
  follower_lease_info_.reset();
  last_lease_req_post_time_ = 0;
  last_lease_req_cache_handle_time_ = 0;
  if (OB_NOT_NULL(lease_diag_info_log_buf_)) {
    ob_free(lease_diag_info_log_buf_);
  }
  lease_diag_info_log_buf_ = nullptr;
}

int ObDupTableLSLeaseMgr::recive_lease_request(const ObDupTableLeaseRequest &lease_req)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lease_lock_);

  DupTableLeaderLeaseInfo tmp_lease_info;

  if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_NOT_INIT;
  } else if (!is_master()) {
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(leader_lease_map_.get_refactored(lease_req.get_src(), tmp_lease_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "get from lease_req_cache_ failed", K(ret), K(lease_req));
    } else {
      DUP_TABLE_LOG(INFO, "first lease request from the new dup_table follower", K(ret), K(lease_req));
    }
  } else if (tmp_lease_info.cache_lease_req_.is_ready()) {
    DUP_TABLE_LOG(INFO, "leader lease info is logging which can not recive new lease request",
                  K(lease_req.get_src()));
  } else if (tmp_lease_info.cache_lease_req_.request_ts_ < lease_req.get_request_ts()) {
    // renew request ts before submit lease log
    ret = OB_HASH_NOT_EXIST;
  }

  if (OB_HASH_NOT_EXIST == ret) {
    tmp_lease_info.cache_lease_req_.renew_lease_req(lease_req.get_request_ts(),
                                                    lease_req.get_lease_interval_us());

    if (OB_FAIL(leader_lease_map_.set_refactored(lease_req.get_src(), tmp_lease_info, 1))) {
      DUP_TABLE_LOG(WARN, "insert into lease_req_cache_ failed", K(ret), K(lease_req));
    }
  }

  DUP_TABLE_LOG(DEBUG, "cache lease request", K(ret), K(leader_lease_map_.size()), K(lease_req),
                K(tmp_lease_info));
  return ret;
}

int ObDupTableLSLeaseMgr::prepare_serialize(int64_t &max_ser_size,
                                            DupTableLeaseItemArray &lease_header_array)
{
  int ret = OB_SUCCESS;
  max_ser_size = 0;
  lease_header_array.reuse();
  int64_t loop_start_time = ObTimeUtility::current_time();
  common::ObAddr tmp_addr;
  DupTableTsInfo local_ts_info;

  SpinWLockGuard guard(lease_lock_);

  if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "the tablets mgr is not running", K(ret), K(is_stopped_), K(ls_id_));
  } else {
    if (need_retry_lease_operation_(loop_start_time, last_lease_req_cache_handle_time_)) {
      if (OB_FAIL(dup_ls_handle_ptr_->get_local_ts_info(local_ts_info))) {
        DUP_TABLE_LOG(WARN, "get local ts info failed", K(ret), K(ls_id_), K(local_ts_info));
      } else {
        LeaseReqCacheHandler req_handler(this, loop_start_time, local_ts_info.max_replayed_scn_,
                                         lease_header_array);
        if (OB_FAIL(hash_for_each_remove(tmp_addr, leader_lease_map_, req_handler))) {
          DUP_TABLE_LOG(WARN, "handle lease requests failed", K(ret));
        }

        if (OB_SUCC(ret) && req_handler.get_renew_lease_count() > 0) {
          DUP_TABLE_LOG(DEBUG, "renew lease list in the log", K(ret), K(req_handler),
                        K(lease_header_array), K(max_ser_size), K(loop_start_time),
                        K(last_lease_req_cache_handle_time_));
        } else if (OB_SUCC(ret) && req_handler.get_renew_lease_count() == 0) {
          req_handler.clear_ser_content();
        }

        if (req_handler.get_error_ret() != OB_SUCCESS) {
          req_handler.clear_ser_content();
          ret = req_handler.get_error_ret();
        } else {
          max_ser_size += req_handler.get_max_ser_size();
        }

        if (OB_SUCC(ret) && req_handler.get_max_ser_size() > 0) {
          last_lease_req_cache_handle_time_ = loop_start_time;
        }
      }
    }
  }

  DUP_TABLE_LOG(DEBUG, "prepare serialize lease log", K(ret), K(max_ser_size),
                K(lease_header_array), K(loop_start_time), K(last_lease_req_cache_handle_time_));
  return ret;
}

int ObDupTableLSLeaseMgr::serialize_lease_log(const DupTableLeaseItemArray &lease_header_array,
                                              char *buf,
                                              const int64_t buf_len,
                                              int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;
  // SpinRLockGuard guard(lease_lock_);

  for (int i = 0; i < lease_header_array.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(lease_header_array[i].log_header_.serialize(buf, buf_len, tmp_pos))) {
      DUP_TABLE_LOG(WARN, "serialize lease log header failed", K(ret), K(lease_header_array[i]));
    } else if (lease_header_array[i].log_header_.is_durable_lease_log()) {
      DupTableDurableLease tmp_durable_lease = lease_header_array[i].durable_lease_;
      DupTableDurableLeaseLogBody durable_log_body(tmp_durable_lease);
      if (OB_FAIL(durable_log_body.serialize(buf, buf_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "serialize durable lease log body failed", K(ret), K(durable_log_body));
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  return ret;
}

int ObDupTableLSLeaseMgr::deserialize_lease_log(DupTableLeaseItemArray &lease_header_array,
                                                const char *buf,
                                                const int64_t data_len,
                                                int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;

  lease_header_array.reuse();

  SpinWLockGuard guard(lease_lock_);

  if (OB_ISNULL(buf) || data_len <= 0 || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    DupTableLeaseLogHeader lease_log_header;
    DupTableLeaderLeaseInfo leader_lease_info;
    // DupTableDurableLeaseLogBody durable_lease_log_body;
    while (OB_SUCC(ret) && tmp_pos < data_len) {
      lease_log_header.reset();
      leader_lease_info.reset();
      DupTableDurableLeaseLogBody durable_lease_log_body(leader_lease_info.confirmed_lease_info_);
      if (OB_FAIL(lease_log_header.deserialize(buf, data_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "deserialize lease log header failed", K(ret), K(lease_log_header),
                      K(tmp_pos), K(data_len));
      } else if (lease_log_header.is_durable_lease_log()) {
        if (OB_FAIL(durable_lease_log_body.deserialize(buf, data_len, tmp_pos))) {
          DUP_TABLE_LOG(WARN, "deserialize leader lease info failed", K(ret));
        } else if (OB_FAIL(lease_header_array.push_back(DupTableLeaseItem(
                       lease_log_header, leader_lease_info.confirmed_lease_info_)))) {
          DUP_TABLE_LOG(WARN, "push back leader_lease_info failed", K(ret), K(leader_lease_info));
        } else if (OB_FALSE_IT(leader_lease_info.lease_expired_ts_ =
                                   leader_lease_info.confirmed_lease_info_.request_ts_
                                   + leader_lease_info.confirmed_lease_info_.lease_interval_us_)) {
          // do nothing
        } else if (OB_FAIL(leader_lease_map_.set_refactored(lease_log_header.get_lease_owner(),
                                                            leader_lease_info, 1))) {
          DUP_TABLE_LOG(WARN, "insert into leader_lease_map_ for replay failed", K(ret),
                        K(lease_log_header), K(leader_lease_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      pos = tmp_pos;
    }
  }
  return ret;
}

int ObDupTableLSLeaseMgr::lease_log_submitted(const bool submit_result,
                                              const share::SCN &lease_log_scn,
                                              const bool for_replay,
                                              const DupTableLeaseItemArray &lease_header_array)
{
  int ret = OB_SUCCESS;

  UNUSED(submit_result);
  UNUSED(lease_log_scn);
  UNUSED(for_replay);
  UNUSED(lease_header_array);

  return ret;
}

int ObDupTableLSLeaseMgr::lease_log_synced(const bool sync_result,
                                           const share::SCN &lease_log_scn,
                                           const bool for_replay,
                                           const DupTableLeaseItemArray &lease_header_array)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && !for_replay) {
    for (int i = 0; i < lease_header_array.count() && OB_SUCC(ret); i++) {
      DupTableLeaderLeaseInfo *leader_lease_ptr = nullptr;
      common::ObAddr lease_owner = lease_header_array[i].log_header_.get_lease_owner();
      if (OB_ISNULL(leader_lease_ptr = leader_lease_map_.get(lease_owner))) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "null leader lease info ptr", K(ret), K(lease_header_array[i]));
      } else {
        if (sync_result) {
          // if cache in invalid or preapre state, do nothing
          if (leader_lease_ptr->cache_lease_req_.is_ready()) {
            if (leader_lease_ptr->cache_lease_req_.request_ts_
                != lease_header_array[i].durable_lease_.request_ts_) {
              DUP_TABLE_LOG(WARN, "lease req cache not equal to durable lease",
                            K(lease_header_array[i].durable_lease_),
                            K(leader_lease_ptr->cache_lease_req_));
              // rewrite req cache
              leader_lease_ptr->cache_lease_req_.request_ts_ =
                  lease_header_array[i].durable_lease_.request_ts_;
              leader_lease_ptr->cache_lease_req_.lease_acquire_ts_ = ObTimeUtility::current_time();
              // update leader expired lease
              leader_lease_ptr->lease_expired_ts_ =
                  leader_lease_ptr->cache_lease_req_.lease_acquire_ts_ + DEFAULT_LEASE_INTERVAL;
              // reset req cache to invalid state
              leader_lease_ptr->cache_lease_req_.set_invalid();
            } else {
              leader_lease_ptr->confirmed_lease_info_ = lease_header_array[i].durable_lease_;
              leader_lease_ptr->lease_expired_ts_ =
                  leader_lease_ptr->cache_lease_req_.lease_acquire_ts_
                  + leader_lease_ptr->cache_lease_req_.lease_interval_us_;
              // reset req cache to invalid state
              leader_lease_ptr->cache_lease_req_.set_invalid();
            }
            if (!for_replay) {
              DUP_TABLE_LOG(
                  INFO, DUP_LEASE_LIFE_PREFIX "update lease expired ts in the leader_lease_map",
                  K(ret), K(lease_log_scn), K(for_replay), K(lease_owner), KPC(leader_lease_ptr));
            }
          }
        } else {
          // if log sync failed, allow renew lease req
          leader_lease_ptr->cache_lease_req_.set_invalid();
        }
      }
    }
  }

  if (OB_SUCC(ret) && sync_result && for_replay) {
    if (OB_FAIL(follower_try_acquire_lease(lease_log_scn))) {
      DUP_TABLE_LOG(WARN, "acquire lease from lease log error", K(ret), K(lease_log_scn));
    }
  }

  if (lease_header_array.count() > 0) {
    DUP_TABLE_LOG(DEBUG, "lease log sync", K(ret), K(sync_result), K(for_replay), K(lease_log_scn),
                  K(lease_header_array), K(is_master()));
  }
  return ret;
}

bool ObDupTableLSLeaseMgr::can_grant_lease_(const common::ObAddr &addr,
                                            const share::SCN &local_max_applyed_scn,
                                            const DupTableLeaderLeaseInfo &lease_info)
{
  bool lease_success = true;
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_id_;

  DupTableTsInfo cache_ts_info;
  if (OB_FAIL(dup_ls_handle_ptr_->get_cache_ts_info(addr, cache_ts_info))) {
    lease_success = false;
    DUP_TABLE_LOG(
        WARN, DUP_LEASE_LIFE_PREFIX "Not allowed to acquire lease - get cache ts info failed",
        K(ret), K(ls_id), K(addr), K(cache_ts_info), K(local_max_applyed_scn), K(lease_info));
  } else if (!cache_ts_info.max_replayed_scn_.is_valid() || !local_max_applyed_scn.is_valid()) {
    lease_success = false;
    DUP_TABLE_LOG(WARN, DUP_LEASE_LIFE_PREFIX "Not allowed to acquire lease - invalid applyed scn",
                  K(ret), K(ls_id), K(addr), K(cache_ts_info), K(local_max_applyed_scn),
                  K(lease_info));
  } else {

    uint64_t local_max_applied_ts = local_max_applyed_scn.get_val_for_gts();
    uint64_t follower_replayed_ts = cache_ts_info.max_replayed_scn_.get_val_for_gts();
    const uint64_t MAX_APPLIED_SCN_INTERVAL = DEFAULT_LEASE_INTERVAL * 1000; /*us -> ns*/

    if (local_max_applied_ts > follower_replayed_ts
        && local_max_applied_ts - follower_replayed_ts >= MAX_APPLIED_SCN_INTERVAL) {
      lease_success = false;
      DUP_TABLE_LOG(WARN, DUP_LEASE_LIFE_PREFIX "Not allowed to acquire lease - slow replay",
                    K(ret), K(ls_id), K(addr), K(local_max_applied_ts), K(follower_replayed_ts),
                    K(local_max_applied_ts - follower_replayed_ts), K(MAX_APPLIED_SCN_INTERVAL),
                    K(cache_ts_info), K(lease_info));
    }
  }

  if (lease_success) {
    DUP_TABLE_LOG(INFO, DUP_LEASE_LIFE_PREFIX "Lease will be granted to the follower", K(ret),
                  K(addr), K(local_max_applyed_scn), K(lease_info), K(cache_ts_info));
  }

  return lease_success;
}

int ObDupTableLSLeaseMgr::handle_lease_req_cache_(int64_t loop_start_time,
                                                  const share::SCN &local_max_applyed_scn,
                                                  const common::ObAddr &addr,
                                                  DupTableLeaderLeaseInfo &single_lease_info)
{
  int ret = OB_SUCCESS;
  if (!single_lease_info.cache_lease_req_.is_prepare()) {
    // do nothing
    if (single_lease_info.cache_lease_req_.is_invalid()) {
      DUP_TABLE_LOG(DEBUG, "No lease request from this follower", K(addr), K(single_lease_info));
    } else if (single_lease_info.cache_lease_req_.is_ready()) {
      DUP_TABLE_LOG(DEBUG, "the lease request is still logging", K(ret), K(addr),
                    K(single_lease_info));
    } else {
      // error state
      DUP_TABLE_LOG(WARN, "unexpected lease request cache for this follower", K(addr),
                    K(single_lease_info));
    }
  } else if (can_grant_lease_(addr, local_max_applyed_scn, single_lease_info)) {
    // grant lease
    single_lease_info.cache_lease_req_.grant_lease_success(loop_start_time);
  } else {
    single_lease_info.cache_lease_req_.grant_lease_failed();
  }

  DUP_TABLE_LOG(DEBUG, "handle lease request cache", K(ret), K(addr), K(loop_start_time),
                K(single_lease_info));

  return ret;
}

int ObDupTableLSLeaseMgr::follower_handle()
{
  int ret = OB_SUCCESS;
  DupTableTsInfo local_ts_info;
  if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "dup table lease mgr is not inited", K(ret));
  } else if (OB_FAIL(dup_ls_handle_ptr_->get_local_ts_info(local_ts_info))) {
    DUP_TABLE_LOG(WARN, "get local ts info failed", K(ret));
  } else {
    SpinWLockGuard guard(lease_lock_);

    ObILocationAdapter *location_adapter = MTL(ObTransService *)->get_location_adapter();
    const share::ObLSID cur_ls_id = ls_id_;
    const common::ObAddr self_addr = MTL(ObTransService *)->get_server();
    int64_t loop_start_time = ObTimeUtility::current_time();

    if (OB_ISNULL(location_adapter) || !cur_ls_id.is_valid() || loop_start_time <= 0) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), KP(location_adapter), K(cur_ls_id),
                    K(loop_start_time));
    } else if (need_post_lease_request_(loop_start_time)) {
      update_request_ts_(loop_start_time);
      ObDupTableLeaseRequest req(local_ts_info.max_replayed_scn_, local_ts_info.max_commit_version_,
                                 local_ts_info.max_read_version_,
                                 follower_lease_info_.durable_lease_.request_ts_,
                                 DEFAULT_LEASE_INTERVAL);
      common::ObAddr leader_addr;
      if (OB_FAIL(location_adapter->nonblock_get_leader(GCONF.cluster_id, MTL_ID(), cur_ls_id,
                                                        leader_addr))) {
        DUP_TABLE_LOG(WARN, "get ls leader failed", K(ret));
        (void)location_adapter->nonblock_renew(GCONF.cluster_id, MTL_ID(), cur_ls_id);
      } else if (FALSE_IT(req.set_header(self_addr, leader_addr, self_addr, cur_ls_id))) {
        DUP_TABLE_LOG(WARN, "set msg header failed", K(ret));
      } else if (OB_FAIL(
                     MTL(ObTransService *)->get_dup_table_rpc_impl().post_msg(leader_addr, req))) {
        DUP_TABLE_LOG(WARN, "post lease request failed", K(ret), K(leader_addr));
      } else {
        last_lease_req_post_time_ = loop_start_time;
        DUP_TABLE_LOG(DEBUG, "post lease request success", K(ret), K(leader_addr), K(req),
                      K(cur_ls_id));
      }
    }
  }
  return ret;
}

int ObDupTableLSLeaseMgr::follower_try_acquire_lease(const share::SCN &lease_log_scn)
{
  int ret = OB_SUCCESS;

  // SpinRLockGuard guard(lease_lock_);

  const common::ObAddr self_addr = MTL(ObTransService *)->get_server();
  DupTableLeaderLeaseInfo tmp_leader_lease_info;

  if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "dup_table lease_mgr not init", K(ret));
  } else if (!lease_log_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), K(lease_log_scn));
  } else if (OB_FAIL(leader_lease_map_.get_refactored(self_addr, tmp_leader_lease_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      DUP_TABLE_LOG(DEBUG, "It can not get lease from this lease log", K(lease_log_scn),
                    K(self_addr));
    } else {
      DUP_TABLE_LOG(WARN, "get lease info from leader_lease_map_ failed", K(ret), K(self_addr));
    }

  } else if (tmp_leader_lease_info.confirmed_lease_info_.request_ts_
             == follower_lease_info_.durable_lease_.request_ts_) {

    const share::ObLSID ls_id = ls_id_;
    if (follower_lease_info_.lease_expired_ts_
        < tmp_leader_lease_info.confirmed_lease_info_.lease_interval_us_
              + tmp_leader_lease_info.confirmed_lease_info_.request_ts_) {
      follower_lease_info_.last_lease_scn_ = lease_log_scn;
      const bool acquire_new_lease = tmp_leader_lease_info.confirmed_lease_info_.request_ts_
                                     > follower_lease_info_.lease_expired_ts_;
      if (acquire_new_lease) {
        follower_lease_info_.lease_acquire_scn_ = lease_log_scn;
      }
      follower_lease_info_.durable_lease_.lease_interval_us_ =
          tmp_leader_lease_info.confirmed_lease_info_.lease_interval_us_;
      follower_lease_info_.lease_expired_ts_ =
          follower_lease_info_.durable_lease_.request_ts_
          + follower_lease_info_.durable_lease_.lease_interval_us_;

      DUP_TABLE_LOG(
          INFO, DUP_LEASE_LIFE_PREFIX "The follower can get new lease from this lease log", K(ret),
          K(ls_id), K(lease_log_scn), K(self_addr), K(acquire_new_lease), K(follower_lease_info_));
    } else {
      DUP_TABLE_LOG(DEBUG, "No new lease in this lease log", K(ret), K(ls_id), K(lease_log_scn),
                    K(self_addr), K(follower_lease_info_));
    }
  } else {
    DUP_TABLE_LOG(
        INFO, "request_ts_ in lease log is not match, wait for new lease",
        "request_ts_in_lease_log", tmp_leader_lease_info.confirmed_lease_info_.request_ts_,
        "request_ts_in_memory", follower_lease_info_.durable_lease_.request_ts_,
        K(follower_lease_info_.lease_expired_ts_), K(follower_lease_info_.last_lease_scn_),
        K(follower_lease_info_.lease_acquire_scn_));
  }

  return ret;
}

int ObDupTableLSLeaseMgr::get_lease_valid_array(LeaseAddrArray &lease_array)
{
  int ret = OB_SUCCESS;

  SpinRLockGuard guard(lease_lock_);
  GetLeaseValidAddrFunctor functor(lease_array);

  if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "dup_table lease_mgr not init", K(ret));
  } else if (OB_FAIL(hash_for_each_update(leader_lease_map_, functor))) {
    DUP_TABLE_LOG(WARN, "get lease valid array from leader_lease_map failed", K(ret));
  }

  return ret;
}

int ObDupTableLSLeaseMgr::leader_takeover(bool is_resume)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lease_lock_);

  // clear follower lease info
  follower_lease_info_.reset();

  if (!is_resume) {
    // If it is new leader, we will continue to use the last lease list from the last leader.
    // Each follower acquire lease at this time in leader's view.
    // The lease length of leader will be larger than follower.
    LeaderActiveLeaseFunctor functor;
    if (OB_FAIL(hash_for_each_update(leader_lease_map_, functor))) {
      DUP_TABLE_LOG(WARN, "update lease_expired_ts_ when leader active failed", K(ret));
    }
  }

  ATOMIC_STORE(&is_master_, true);

  return ret;
}

int ObDupTableLSLeaseMgr::leader_revoke()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lease_lock_);

  // only can reset follower lease,
  // can not reset leader lease list
  follower_lease_info_.reset();

  ATOMIC_STORE(&is_master_, false);
  return ret;
}

bool ObDupTableLSLeaseMgr::is_follower_lease_valid()
{
  bool is_follower_lease = false;

  SpinRLockGuard guard(lease_lock_);
  is_follower_lease = follower_lease_info_.lease_expired_ts_ > ObTimeUtility::current_time();

  return is_follower_lease;
}

bool ObDupTableLSLeaseMgr::check_follower_lease_serving(const bool is_election_leader,
                                                        const share::SCN &max_replayed_scn)
{
  SpinRLockGuard guard(lease_lock_);
  bool follower_lease_serving = false;
  if (is_election_leader) {
    follower_lease_serving = true;
    DUP_TABLE_LOG(INFO, "no need to check follower serving on a leader", K(is_election_leader),
                  K(max_replayed_scn));
  } else if (follower_lease_info_.lease_expired_ts_ <= ObTimeUtility::current_time()) {
    follower_lease_serving = false;
    DUP_TABLE_LOG(INFO, "dup table lease has been expired", K(follower_lease_serving),
                  K(is_election_leader), K(max_replayed_scn), K(follower_lease_info_));
  } else if (follower_lease_info_.lease_acquire_scn_.is_valid()
             || follower_lease_info_.lease_acquire_scn_ <= max_replayed_scn) {
    follower_lease_serving = true;
  }
  return follower_lease_serving;
}

void ObDupTableLSLeaseMgr::print_lease_diag_info_log(const bool is_master)
{
  SpinRLockGuard guard(lease_lock_);
  int ret = OB_SUCCESS;

  const uint64_t LEASE_PRINT_BUF_LEN =
      DupTableDiagStd::DUP_DIAG_INFO_LOG_BUF_LEN[DupTableDiagStd::TypeIndex::LEASE_INDEX];
  const int64_t tenant_id = MTL_ID();
  const ObLSID ls_id = ls_id_;
  const int64_t cur_time = ObTimeUtility::current_time();

  if (OB_ISNULL(lease_diag_info_log_buf_)) {
    if (OB_ISNULL(lease_diag_info_log_buf_ =
                      static_cast<char *>(ob_malloc(LEASE_PRINT_BUF_LEN, "DupTableDiag")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _DUP_TABLE_LOG(WARN, "%salloc lease diag info buf failed, ret=%d, ls_id=%lu, cur_time=%s",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id(),
                     common::ObTime2Str::ob_timestamp_str(cur_time));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_master) {

      DiagInfoGenerator diag_info_gen(true, lease_diag_info_log_buf_, LEASE_PRINT_BUF_LEN,
                                      cur_time);
      if (OB_FAIL(hash_for_each_update(leader_lease_map_, diag_info_gen))) {
        _DUP_TABLE_LOG(WARN, "%sprint leader lease list failed, ret=%d, ls_id=%lu, cur_time=%s",
                       DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id(),
                       common::ObTime2Str::ob_timestamp_str(cur_time));
      }

      lease_diag_info_log_buf_[MIN(diag_info_gen.get_buf_pos(), LEASE_PRINT_BUF_LEN - 1)] = '\0';

      _DUP_TABLE_LOG(INFO, "[%sLeader Lease List] tenant: %lu, ls: %lu , current_time = %s\n%s",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, tenant_id, ls_id.id(),
                     common::ObTime2Str::ob_timestamp_str(cur_time), lease_diag_info_log_buf_);
    } else {
      _DUP_TABLE_LOG(
          INFO,
          "[%sFollower Lease Info] tenant: %lu, ls: %lu , current_time = %s\n"
          "%s[%sFollower Lease] is_expired=%s, request_ts=%lu, request_ts(date)=%s, "
          "lease_expired_time=%s, "
          "lease_interval_us=%lu, last_lease_scn=%s, lease_acquire_scn=%s",
          DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, tenant_id, ls_id.id(),
          common::ObTime2Str::ob_timestamp_str(cur_time), DupTableDiagStd::DUP_DIAG_INDENT_SPACE,
          DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
          to_cstring(cur_time >= follower_lease_info_.lease_expired_ts_),
          follower_lease_info_.durable_lease_.request_ts_,
          common::ObTime2Str::ob_timestamp_str(follower_lease_info_.durable_lease_.request_ts_),
          common::ObTime2Str::ob_timestamp_str(follower_lease_info_.lease_expired_ts_),
          follower_lease_info_.durable_lease_.lease_interval_us_,
          to_cstring(follower_lease_info_.last_lease_scn_),
          to_cstring(follower_lease_info_.lease_acquire_scn_));
    }
  }
}

int ObDupTableLSLeaseMgr::get_lease_mgr_stat(FollowerLeaseMgrStatArr &collect_arr)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  const ObAddr leader_addr = GCTX.self_addr();
  const int64_t cur_time = ObTimeUtility::current_time();
  SpinRLockGuard r_lock(lease_lock_);

  if (OB_FAIL(collect_arr.prepare_allocate(leader_lease_map_.size()))) {
    DUP_TABLE_LOG(WARN, "pre allocate failed", K(ret));
  } else {
    LeaderLeaseMgrStatFunctor collect_handler(collect_arr, tenant_id, cur_time, leader_addr,
                                              dup_ls_handle_ptr_->get_ls_id());

    if (OB_FAIL(hash_for_each_update(leader_lease_map_, collect_handler))) {
      DUP_TABLE_LOG(WARN, "colloect leader mgr info failed", K(ret));
    }
  }
  DUP_TABLE_LOG(DEBUG, "colloect leader mgr info", K(ret), K(collect_arr));
  return ret;
}

int ObDupTableLSLeaseMgr::recover_lease_from_ckpt(
    const ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lease_lock_);

  if (!dup_ls_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(dup_ls_meta));
  } else {
    DupTableLeaderLeaseInfo leader_lease_info;
    for (int i = 0; i < dup_ls_meta.lease_item_array_.count() && OB_SUCC(ret); i++) {
      leader_lease_info.reset();
      leader_lease_info.confirmed_lease_info_ = dup_ls_meta.lease_item_array_[i].durable_lease_;
      // lease expired ts will be updated in leader_takeover
      leader_lease_info.lease_expired_ts_ =
          leader_lease_info.confirmed_lease_info_.lease_interval_us_
          + leader_lease_info.confirmed_lease_info_.request_ts_;
      if (OB_FAIL(leader_lease_map_.set_refactored(
              dup_ls_meta.lease_item_array_[i].log_header_.get_lease_owner(), leader_lease_info,
              1))) {
        DUP_TABLE_LOG(WARN, "insert into leader lease map failed ", K(ret), K(i),
                      K(dup_ls_meta.lease_item_array_[i]), K(leader_lease_info));
      }
    }
  }

  return ret;
}

void ObDupTableLSLeaseMgr::update_request_ts_(int64_t loop_start_time)
{
  // set self_request_ts_ = 0 when replay lease log success
  int64_t cur_lease_interval = follower_lease_info_.durable_lease_.lease_interval_us_ > 0
                                   ? follower_lease_info_.durable_lease_.lease_interval_us_
                                   : DEFAULT_LEASE_INTERVAL;

  if (loop_start_time - follower_lease_info_.durable_lease_.request_ts_ >= cur_lease_interval / 2) {
    follower_lease_info_.durable_lease_.request_ts_ = loop_start_time;
  }
}

bool ObDupTableLSLeaseMgr::need_post_lease_request_(int64_t loop_start_time)
{
  bool need_post = false;

  if (need_retry_lease_operation_(loop_start_time, last_lease_req_post_time_)
      && (loop_start_time >= follower_lease_info_.lease_expired_ts_
          || follower_lease_info_.lease_expired_ts_ - loop_start_time
                 <= follower_lease_info_.durable_lease_.lease_interval_us_ / 10 * 4)) {
    need_post = true;
  }

  return need_post;
}

bool ObDupTableLSLeaseMgr::need_retry_lease_operation_(const int64_t cur_time,
                                                       const int64_t last_time)
{
  return cur_time - last_time >= MIN_LEASE_INTERVAL / 10 * 2;
}

bool ObDupTableLSLeaseMgr::LeaseReqCacheHandler::operator()(
    common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair)
{
  bool will_remove = false;
  int ret = OB_SUCCESS;

  DupTableLeaseItem item;
  if (OB_FAIL(lease_mgr_ptr_->handle_lease_req_cache_(loop_start_time_, local_max_applyed_scn_,
                                                      hash_pair.first, hash_pair.second))) {
    error_ret = ret;
    DUP_TABLE_LOG(WARN, "handle lease request failed", K(ret));
  } else if (hash_pair.second.cache_lease_req_.is_ready()) {
    renew_lease_count_++;
    item.log_header_.set_lease_owner(hash_pair.first);
    item.durable_lease_.request_ts_ = hash_pair.second.cache_lease_req_.request_ts_;
    item.durable_lease_.lease_interval_us_ = hash_pair.second.cache_lease_req_.lease_interval_us_;
  } else if (loop_start_time_ >= hash_pair.second.lease_expired_ts_) {
    DUP_TABLE_LOG(INFO, "remove expired lease follower from map", K(ret), K(will_remove),
                  K(hash_pair.first), K(hash_pair.second));
    will_remove = true;
  } else {
    item.log_header_.set_lease_owner(hash_pair.first);
    item.durable_lease_.request_ts_ = hash_pair.second.confirmed_lease_info_.request_ts_;
    item.durable_lease_.lease_interval_us_ =
        hash_pair.second.confirmed_lease_info_.lease_interval_us_;
  }

  if (!will_remove) {
    DupTableDurableLeaseLogBody durable_log_body(item.durable_lease_);
    max_ser_size_ += item.log_header_.get_serialize_size() + durable_log_body.get_serialize_size();
    if (OB_FAIL(lease_item_array_.push_back(item))) {
      error_ret = ret;
      DUP_TABLE_LOG(WARN, "push back into lease_item_array_ failed", K(ret), K(item));
    }
  }

  return will_remove;
}

int ObDupTableLSLeaseMgr::GetLeaseValidAddrFunctor::operator()(
    common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  if (INT64_MAX == cur_time_) {
    cur_time_ = ObTimeUtility::current_time();
  }

  /*
  +----------------------------------------------------+
  |                  submit lease log                  |
  +----------------------------------------------------+
    |
    | lease A ready
    v
  +----------------------------------------------------+
  |                 submit commit info                 |
  +----------------------------------------------------+
    |
    |
    v
  +----------------------------------------------------+
  |              commit_info::on_success               |
  +----------------------------------------------------+
    |
    |
    v
  +----------------------------------------------------+
  |             redo sync finish without A             |
  +----------------------------------------------------+
    |
    |
    v
  +----------------------------------------------------+
  |               lease_log::on_success                |
  +----------------------------------------------------+
    |
    | lease A is valid
    v
  +----------------------------------------------------+
  |             read without the trx on A              |
  | replay ts between (lease_log_scn, commit_info_scn) |
  +----------------------------------------------------+
  */

  if (hash_pair.second.lease_expired_ts_ > cur_time_
      // include a granted logging lease
      || hash_pair.second.cache_lease_req_.is_ready()) {
    if (OB_FAIL(addr_arr_.push_back(hash_pair.first))) {
      DUP_TABLE_LOG(WARN, "push back lease valid array failed", K(ret));
    }
  }

  return ret;
}

int ObDupTableLSLeaseMgr::DiagInfoGenerator::operator()(
    const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  const char *addr_str = to_cstring(hash_pair.first);

  ret = ::oceanbase::common::databuff_printf(
      info_buf_, info_buf_len_, info_buf_pos_,
      "%s[%sConfirmed Lease] owner=%s, is_expired=%s, request_ts=%lu, request_ts(date)=%s, "
      "lease_expired_time=%s, lease_interval_us=%lu\n",
      DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, addr_str,
      to_cstring(cur_time_ >= hash_pair.second.lease_expired_ts_),
      hash_pair.second.confirmed_lease_info_.request_ts_,
      common::ObTime2Str::ob_timestamp_str(hash_pair.second.confirmed_lease_info_.request_ts_),
      common::ObTime2Str::ob_timestamp_str(hash_pair.second.lease_expired_ts_),
      hash_pair.second.confirmed_lease_info_.lease_interval_us_);

  if (OB_SUCC(ret) && need_cache_) {
    if (hash_pair.second.cache_lease_req_.is_invalid()) {

      ret = ::oceanbase::common::databuff_printf(
          info_buf_, info_buf_len_, info_buf_pos_,
          "%s[%sCached Lease] owner=%s, No Lease Request Cache\n",
          DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
          addr_str);
    } else {

      ret = ::oceanbase::common::databuff_printf(
          info_buf_, info_buf_len_, info_buf_pos_,
          "%s[%sCached Lease] owner=%s, request_ts=%lu, request_ts(date)=%s, "
          "handle_request_time=%lu, handle_request_time(date)=%s, request_lease_interval_us "
          "=%lu\n",
          DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, addr_str,
          hash_pair.second.cache_lease_req_.request_ts_,
          common::ObTime2Str::ob_timestamp_str(hash_pair.second.cache_lease_req_.request_ts_),
          hash_pair.second.cache_lease_req_.lease_acquire_ts_,
          common::ObTime2Str::ob_timestamp_str(hash_pair.second.cache_lease_req_.lease_acquire_ts_),
          hash_pair.second.cache_lease_req_.lease_interval_us_);
    }
  }

  return ret;
}

int ObDupTableLSLeaseMgr::LeaderActiveLeaseFunctor::operator()(
    common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  if (INT64_MAX == cur_time_) {
    cur_time_ = ObTimeUtility::current_time();
  }

  hash_pair.second.lease_expired_ts_ =
      cur_time_ + hash_pair.second.confirmed_lease_info_.lease_interval_us_;

  return ret;
}

int ObDupTableLSLeaseMgr::LeaderLeaseMgrStatFunctor::operator()(
    const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  if (cnt_ > collect_arr_.count()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(ERROR, "unexpect err", K(ret), K(cnt_), K(collect_arr_.count()));
  } else {
    // first get emptry stat
    ObDupTableLSLeaseMgrStat &tmp_stat = collect_arr_.at(cnt_);
    // second fill content
    // tmp_stat.set_addr(leader_addr_);
    tmp_stat.set_tenant_id(tenant_id_);
    tmp_stat.set_ls_id(ls_id_);
    tmp_stat.set_follower_addr(hash_pair.first);
    tmp_stat.set_expired_ts(hash_pair.second.lease_expired_ts_);
    tmp_stat.set_cached_req_ts(hash_pair.second.cache_lease_req_.request_ts_);
    tmp_stat.set_lease_interval(hash_pair.second.confirmed_lease_info_.lease_interval_us_);
    tmp_stat.set_grant_req_ts(hash_pair.second.confirmed_lease_info_.request_ts_);
    tmp_stat.set_grant_ts(hash_pair.second.lease_expired_ts_ - tmp_stat.get_lease_interval());
    // set remain us
    if (collect_ts_ > tmp_stat.get_grant_ts() && collect_ts_ < tmp_stat.get_expired_ts()) {
      tmp_stat.set_remain_us(tmp_stat.get_expired_ts() - collect_ts_);
    } else {
      tmp_stat.set_remain_us(0);
    }
    // update cnt_ for next read
    cnt_++;
    // for debug
    DUP_TABLE_LOG(INFO, "insert one row in svr list", K(ret), K(tmp_stat), K(cnt_),
                  K(hash_pair.second), K(collect_arr_));
  }

  return ret;
}

} // namespace transaction
} // namespace oceanbase
