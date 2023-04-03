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

#include "storage/tx/ob_dup_table.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_dup_table_rpc.h"

namespace oceanbase
{
using namespace storage;

namespace transaction
{
typedef ObRedoLogSyncResponseMsg::ObRedoLogSyncResponseStatus ObRedoLogSyncResponseStatus;
typedef ObDupTableLeaseResponseMsg::ObDupTableLeaseStatus ObDupTableLeaseStatus;

void ObDupTableLeaseInfo::reset()
{
  lease_expired_ts_ = INT64_MAX;
  cur_log_id_ = 0;
  gts_ = -1;
}

void ObDupTableLeaseInfo::destroy()
{
  reset();
}

bool ObDupTableLeaseInfo::is_lease_expired() const
{
  return ObTimeUtility::current_time() >= ATOMIC_LOAD(&lease_expired_ts_);
}

int ObDupTableLeaseInfo::update_lease_expired_ts(const int64_t lease_interval_us)
{
  int ret = OB_SUCCESS;

  // indicates whether lease is successfully updated
  bool update_lease_expired_ts_succ = false;

  // when lease_expired_ts <= current timestamp, lease is invalid, and update should fail
  // otherwise update succeed
  int64_t tmp_lease_expired_ts = ATOMIC_LOAD(&lease_expired_ts_);
  while (tmp_lease_expired_ts > ObTimeUtility::current_time()
         && !(update_lease_expired_ts_succ = ATOMIC_BCAS(&lease_expired_ts_,
                                                         tmp_lease_expired_ts,
                                                         ObTimeUtility::current_time() + lease_interval_us))) {
    tmp_lease_expired_ts = ATOMIC_LOAD(&lease_expired_ts_);
  }

  return ret;
}

int ObDupTableLeaseInfo::update_cur_log_id(const uint64_t cur_log_id)
{
  int ret = OB_SUCCESS;

  atomic_update<uint64_t>(&cur_log_id_, cur_log_id);

  return ret;
}

int ObDupTableLeaseInfo::update_gts(const int64_t gts)
{
  int ret = OB_SUCCESS;

  atomic_update<int64_t>(&gts_, gts);

  return ret;
}

void ObDupTableLeaseInfoStat::reset()
{
  addr_.reset();
  lease_expired_ts_ = 0;
  cur_log_id_ = 0;
  gts_ = -1;
}

// void ObDupTableLeaseRequestStatistics::statistics(const common::ObPartitionKey &pkey)
// {
//   // print statistics info every 10 seconds
//   if (ObTimeUtility::current_time() - last_print_ts_ > 10 * 1000 * 1000) {
//     int64_t response_count = resp_lease_expired_count_ + resp_log_too_old_count_ + resp_succ_count_;
//     int64_t avg_rt = response_count > 0 ? (total_rt_ / response_count) : 0;
//     TRANS_LOG(INFO, "[DUP TABLE] lease request statistics", K(pkey), K(total_rt_), K(avg_rt),
//               K(request_count_), K(response_count), K(resp_succ_count_),
//               K(resp_log_too_old_count_), K(resp_lease_expired_count_), K(resp_not_master_count_));
//     last_print_ts_ = ObTimeUtility::current_time();
//   }
// }

void ObDupTablePartitionInfo::reset()
{
  ls_tx_ctx_mgr_ = NULL;
  lease_expired_ts_ = OB_INVALID_TIMESTAMP;
  leader_log_id_ = 0;
  replay_log_id_ = 0;
  need_refresh_location_now_ = false;
  lease_request_statistics_.reset();
}

void ObDupTablePartitionInfo::destroy()
{
  reset();
}

int ObDupTablePartitionInfo::init(ObLSTxCtxMgr *ls_tx_ctx_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_tx_ctx_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ls_tx_ctx_mgr));
  } else {
    ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
  }

  return ret;
}

int ObDupTablePartitionInfo::handle_lease_response(const ObDupTableLeaseResponseMsg &msg,
                                                   const uint64_t tenant_id,
                                                   ObTransService *txs)
{
  UNUSED(msg);
  UNUSED(tenant_id);
  UNUSED(txs);
  int ret = OB_NOT_SUPPORTED;

  // const int64_t request_ts = msg.get_request_ts();
  // const uint64_t leader_log_id = msg.get_cur_log_id();
  // const int64_t lease_interval_us = msg.get_lease_interval_us();
  // // msg may be from old version server, in which case gts would be 0
  // const int64_t gts = msg.get_gts();
  // // statistics rt
  // lease_request_statistics_.add_total_rt(ObTimeUtility::current_time() - request_ts);
  // if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_SUCC == msg.get_status()) {
  //   // update gts cache
  //   txs->get_tx_version_mgr().update_max_commit_ts(gts, false);
  //   // update lease
  //   int64_t new_lease_expired_ts = request_ts + lease_interval_us;
  //   // when checking whether the service can be provided,
  //   // lease_expired_ts is first checked and then leader_log_id
  //   // so, first update leader_log_id and then lease_expired_ts here
  //   if (is_lease_expired()) {
  //     // When you successfully apply for a lease for the first time,
  //     // you need to record the leader log id, and only when next_replay_log_id
  //     // is greater than leader log id can service be available to the outside.
  //     atomic_update<uint64_t>(&leader_log_id_, leader_log_id);
  //     TRANS_LOG(INFO, "[DUP TABLE]duplicate partition update leader log id",
  //               K_(leader_log_id), K(leader_log_id), K(msg.get_partition()));
  //   }
  //   atomic_update<int64_t>(&lease_expired_ts_, new_lease_expired_ts);
  //   TRANS_LOG(INFO, "[DUP TABLE]update dup table lease success",
  //             K(request_ts), K_(leader_log_id), K_(lease_expired_ts));
  //   lease_request_statistics_.inc_resp_succ_count();
  // } else {
  //   TRANS_LOG(WARN, "[DUP TABLE]request dup table lease fail", K(msg));
  //   if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_EXPIRED == msg.get_status()) {
  //     lease_request_statistics_.inc_resp_lease_expired_count();
  //   } else if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_LOG_TOO_OLD == msg.get_status()) {
  //     lease_request_statistics_.inc_resp_log_too_old_count();
  //   } else if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_NOT_MASTER == msg.get_status()) {
  //     lease_request_statistics_.inc_resp_not_master_count();
  //     ATOMIC_STORE(&need_refresh_location_now_, true);
  //   } else {
  //     // do nothing
  //   }
  // }

  return ret;
}

int ObDupTablePartitionInfo::handle_redo_log_sync_request(const ObRedoLogSyncRequestMsg &msg,
                                                          ObTransService *txs)
{
  UNUSED(msg);
  UNUSED(txs);
  return OB_NOT_SUPPORTED;
  /*
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDupTableRpc *dup_table_rpc = NULL;
  ObITsMgr *ts_mgr = NULL;
  const uint64_t log_id = msg.get_log_id();
  const int64_t log_ts = msg.get_log_ts();
  const ObPartitionKey &pkey = msg.get_partition();
  const ObTransID &trans_id = msg.get_trans_id();
  const int64_t log_type = msg.get_log_type();
  bool need_response = false;
  ObRedoLogSyncResponseStatus status = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_UNKNOWN;

  if (OB_ISNULL(dup_table_rpc = txs->get_dup_table_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "dup table rpc is NULL", KR(ret), K(msg));
  } else if (OB_ISNULL(ts_mgr = txs->get_ts_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ts mgr is NULL", KR(ret), K(msg));
  } else if (is_lease_expired()) {
    // lease is expired
    TRANS_LOG(INFO, "[DUP TABLE]dup table lease expired", K(msg));
    // respond with rpc
    need_response = true;
    status = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_LEASE_EXPIRED;
  } else if (ObStorageLogTypeChecker::is_trans_commit_log(log_type)) {
    // for commit log, it is only required to check whether gts cache is greater than commit version
    if (OB_SUCCESS != (tmp_ret = ts_mgr->wait_gts_elapse(pkey.get_tenant_id(), log_ts))) {
      if (OB_EAGAIN != tmp_ret) {
        TRANS_LOG(WARN, "wait get elapse error", K(tmp_ret), K(msg));
      }
    } else {
      need_response = true;
      status = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_SUCC;
    }
  } else if (ATOMIC_LOAD(&replay_log_id_) >= log_id) {
    // 1. when next_replay_log_id is greater than log id, log sync can be viewed as success
    need_response = true;
    status = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_SUCC;
    // if (OB_FAIL(before_prepare_(trans_id))) {
    //   TRANS_LOG(WARN, "before prepare failed", KR(ret), K(trans_id));
    // }
  } else if (check_trans_log_id_replayed_(trans_id, log_id)) {
    // 2. when next_log_id is less than or equal to log id,
    //    need to check whether the log is actually replayed
    need_response = true;
    status = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_SUCC;
    // if (OB_FAIL(before_prepare_(trans_id))) {
    //   TRANS_LOG(WARN, "before prepare failed", KR(ret), K(trans_id));
    // }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret) && need_response) {
    ObRedoLogSyncResponseMsg resp_msg;
    int64_t gts = 0;
    //if gts gts failed, leader will retry
    if (OB_FAIL(ts_mgr->get_local_trans_version(pkey.get_tenant_id(), NULL, gts))
        || OB_UNLIKELY(gts <= 0)) {
      TRANS_LOG(WARN, "get gts failed", KR(ret), K(trans_id));
    } else if (OB_FAIL(resp_msg.init(msg.get_partition(),
                                     msg.get_log_id(),
                                     msg.get_trans_id(),
                                     txs->get_server(),
                                     status))) {
      TRANS_LOG(WARN, "ObRedoLogSyncResponseMsg init error", KR(ret), K(msg), K(resp_msg));
    } else if (FALSE_IT(resp_msg.set_gts(gts))) {
    } else if (OB_FAIL(resp_msg.set_header(msg.get_dst(), msg.get_dst(), msg.get_src()))) {
      TRANS_LOG(WARN, "ObRedoLogSyncResponseMsg set header error", KR(ret), K(resp_msg), K(msg));
    } else if (OB_FAIL(dup_table_rpc->post_redo_log_sync_response(pkey.get_tenant_id(),
                                                                  msg.get_src(),
                                                                  resp_msg))) {
      TRANS_LOG(WARN, "[DUP TABLE]post redo log sync response error", KR(ret), K(msg), K(resp_msg));
    } else {
      TRANS_LOG(DEBUG, "[DUP TABLE]post redo log sync response success", K(msg), K(resp_msg));
    }
  } else {
    TRANS_LOG(DEBUG, "not post redo log sync response", KR(ret), K(msg));
  }

  return ret;
  */
}

bool ObDupTablePartitionInfo::need_renew_lease() const
{
  return ATOMIC_LOAD(&lease_expired_ts_) - ObTimeUtility::current_time()
    <= ObTransService::DUP_TABLE_LEASE_START_RENEW_INTERVAL_US;
}

bool ObDupTablePartitionInfo::is_serving()
{
  int bool_ret = false;

  if (is_lease_expired()) {
    bool_ret = false;
    // service is unavailable due to lease is invalid, need to refresh location
    ATOMIC_STORE(&need_refresh_location_now_, true);
  } else if (ATOMIC_LOAD(&replay_log_id_) < ATOMIC_LOAD(&leader_log_id_)) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }

  if (!bool_ret) {
    TRANS_LOG(INFO, "duplicate partition not serving", K_(replay_log_id), K_(leader_log_id));
  }

  return bool_ret;
}

int ObDupTablePartitionInfo::update_replay_log_id(const uint64_t cur_log_id)
{
  int ret = OB_SUCCESS;

  if (cur_log_id < UINT64_MAX - 1) {
    // when partition is migrating or rebuilding, the log id
    // returned by get_last_slide_log_id interface
    // could be invalid values like UINT64_MAX or UINT64_MAX - 1
    atomic_update<uint64_t>(&replay_log_id_, cur_log_id);
  }

  return ret;
}

bool ObDupTablePartitionInfo::need_refresh_location()
{
  bool bool_ret = ATOMIC_LOAD(&need_refresh_location_now_);
  if (bool_ret) {
    ATOMIC_STORE(&need_refresh_location_now_, false);
  }
  return bool_ret;
}

bool ObDupTablePartitionInfo::check_trans_log_id_replayed_(const ObTransID &trans_id, const uint64_t log_id)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  ObPartTransCtx *part_ctx = NULL;

  if (OB_ISNULL(ls_tx_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition mgr is null", KR(ret), K(trans_id));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_tx_ctx(trans_id,
                                                for_replay,
                                                part_ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), K(trans_id));
    }
  } else {
    //bool_ret = part_ctx->is_redo_log_replayed(log_id);
    (void)ls_tx_ctx_mgr_->revert_tx_ctx(part_ctx);
  }

  return bool_ret;
}

/*int ObDupTablePartitionInfo::before_prepare_(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  ObTransCtx *ctx = NULL;
  ObPartTransCtx *part_ctx = NULL;

  if (OB_ISNULL(ls_tx_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition mgr is null", KR(ret), K(trans_id));
  } else if (OB_FAIL(ls_tx_ctx_mgr_->get_trans_ctx(trans_id,
                                                   for_replay,
                                                   is_readonly,
                                                   is_bounded_staleness_read,
                                                   need_completed_dirty_txn,
                                                   alloc,
                                                   ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), K(trans_id));
    }
  } else {
    part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (OB_FAIL(part_ctx->before_prepare())) {
      TRANS_LOG(WARN, "part ctx before prepare failed", KR(ret), K(trans_id));
    }
    (void)ls_tx_ctx_mgr_->revert_tx_ctx(ctx);
  }

  return ret;
}*/

void PrintDupTableLeaseHashMapFunctor::reset()
{
  lease_list_.reset();
}

// void ObDupTableLeaseStatistics::statistics(const common::ObPartitionKey &pkey)
// {
//   // print statistics info every 10 seconds
//   if (ObTimeUtility::current_time() - last_print_ts_ > 10 * 1000 * 1000) {
//     TRANS_LOG(INFO, "[DUP TABLE] lease mgr statistics:",
//               K(pkey),
//               K_(not_master_count),
//               K_(get_lease_info_err_count),
//               K_(insert_lease_info_err_count),
//               K_(rpc_err_count),
//               K_(not_dup_table_count));
//     last_print_ts_ = ObTimeUtility::current_time();
//   }
// }

void ObDupTablePartitionMgr::reset()
{
  txs_ = NULL;
  ls_tx_ctx_mgr_ = NULL;
  dup_table_lease_infos_.reset();
  cur_log_id_ = 0;
  start_serving_ts_ = -1;
  is_master_ = false;
  is_dup_table_ = false;
  lease_statistics_.reset();
}

int ObDupTablePartitionMgr::leader_revoke()
{
  int ret = OB_SUCCESS;

  is_master_ = false;
  dup_table_lease_infos_.reset();
  //cur_log_id_ = 0;
  start_serving_ts_ = 0;
  TRANS_LOG(INFO, "ObDupTablePartitionMgr revoke");

  return ret;
}

int ObDupTablePartitionMgr::leader_active(const uint64_t cur_log_id, const bool election_by_changing_leader)
{
  int ret = OB_SUCCESS;

  // In the case of changing leader, you need to wait until
  // the old leader lease expires to provide write services.
  // when changing leader, the lease of the old owner can be brought to
  // the new leader without waiting for the lease to expire
  if (election_by_changing_leader) {
    start_serving_ts_ = ObTimeUtility::current_time() + ObTransService::DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US;
  } else {
    start_serving_ts_ = ObTimeUtility::current_time();
  }
  update_cur_log_id(cur_log_id);
  is_master_ = true;
  TRANS_LOG(INFO, "ObDupTablePartitionMgr active", K_(start_serving_ts));

  return ret;
}

void ObDupTablePartitionMgr::destroy()
{
  reset();
  dup_table_lease_infos_.destroy();
}

// int ObDupTablePartitionMgr::init(ObTransService *txs,
//                                  const common::ObPartitionKey &partition,
//                                  ObLSTxCtxMgr *ls_tx_ctx_mgr,
//                                  const bool is_master)
// {
//   int ret = OB_SUCCESS;
//
//   if (OB_UNLIKELY(!partition.is_valid())
//       || OB_ISNULL(ls_tx_ctx_mgr)
//       || OB_ISNULL(txs)) {
//     ret = OB_INVALID_ARGUMENT;
//     TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), KP(ls_tx_ctx_mgr), KP(txs));
//   } else {
//     txs_ = txs;
//     partition_ = partition;
//     ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
//     is_master_ = is_master;
//   }
//
//   return ret;
// }

void ObDupTablePartitionMgr::check_is_dup_table_()
{
}

int ObDupTablePartitionMgr::handle_lease_request(const ObDupTableLeaseRequestMsg &request)
{
  UNUSED(request);
  return OB_NOT_SUPPORTED;
  /*
  int ret = OB_SUCCESS;
  ObDupTableLeaseStatus lease_status = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_UNKNOWN;
  check_is_dup_table_();

  if (!is_dup_table_) {
    TRANS_LOG(WARN, "[DUP TABLE]not duplicate partition receive lease request", K_(is_dup_table));
    lease_statistics_.inc_not_dup_table_count();
  } else {
    if (OB_FAIL(decide_lease_status_for_response_(lease_status, request))) {
      TRANS_LOG(WARN,"[DUP TABLE]decide lease status error", KR(ret), K_(partition), K(request), K(lease_status));
    } else if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_UNKNOWN != lease_status) {
      //post lease response
      ObDupTableLeaseResponseMsg resp_msg;
      int64_t gts = 0;
      if (OB_FAIL(OB_TS_MGR.get_local_trans_version(request.get_partition().get_tenant_id(), NULL, gts))) {
        TRANS_LOG(WARN, "gts gts cache error", KR(ret), K(request));
      } else if (OB_FAIL(resp_msg.init(request.get_request_ts(),
                                       request.get_partition(),
                                       ATOMIC_LOAD(&cur_log_id_),
                                       lease_status,
                                       request.get_request_lease_interval_us(),
                                       gts))) {
        TRANS_LOG(WARN, "ObDupTableLeaseResponseMsg init error", KR(ret), K(request), K_(partition));
      } else if (OB_FAIL(resp_msg.set_header(request.get_dst(), request.get_dst(), request.get_src()))) {
        TRANS_LOG(WARN, "ObDupTableLeaseResponseMsg set header error", KR(ret), K(request), K_(partition));
      } else if (OB_FAIL(txs_
                         ->get_dup_table_rpc()
                         ->post_dup_table_lease_response(request.get_partition().get_tenant_id(),
                                                         request.get_src(),
                                                         resp_msg))) {
        TRANS_LOG(WARN, "post dup table lease response error", KR(ret), K(request), K(resp_msg));
      } else {
        TRANS_LOG(DEBUG, "post dup table lease response success", K(request), K(resp_msg));
      }

      if (OB_FAIL(ret)) {
        lease_statistics_.inc_rpc_err_count();
      }
    }
  }

  //print statistics
  statistics();

  return ret;
  */
}

int ObDupTablePartitionMgr::decide_lease_status_for_response_(ObDupTableLeaseStatus &lease_status,
                                                              const ObDupTableLeaseRequestMsg & request)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDupTableLeaseInfo *lease_info = NULL;
  uint64_t tmp_log_id = ATOMIC_LOAD(&cur_log_id_);

  if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_UNKNOWN != lease_status) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[DUP TABLE]invalid argument when deciding lease_status", K(lease_status));
  } else if (!is_master_) {
    // TRANS_LOG(WARN, "[DUP TABLE]duplicate partition follower receive lease request", K_(is_master), K_(partition), K(request.get_addr()));
    lease_status = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_NOT_MASTER;
    lease_statistics_.inc_not_master_count();
  } else if (tmp_log_id < request.get_last_log_id()
             || tmp_log_id - request.get_last_log_id() <= MAX_ALLOWED_LOG_MISSING_COUNT) {
    //may be expired in the future
    lease_status = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_SUCC;
  } else {
    lease_status = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_LOG_TOO_OLD;
    // TRANS_LOG(WARN,"[DUP TABLE]the log_id of lease request is too old", K(tmp_log_id), K(request.get_last_log_id()), K_(partition), K(request.get_addr()));
  }

  //get lease_info
  if (ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_SUCC != lease_status) {
    lease_info = NULL;
  } else if (OB_SUCCESS != (tmp_ret = dup_table_lease_infos_.get(request.get_addr(),lease_info))) {
    if(OB_ENTRY_NOT_EXIST == tmp_ret) {
      if (OB_ISNULL(lease_info = ObDupTableLeaseInfoFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "[DUP TABLE]alloc ObDupTableLeaseInfo fail", KR(ret), K(request));
        lease_statistics_.inc_insert_lease_info_err_count();
      } else if (OB_FAIL(dup_table_lease_infos_.insert_and_get(request.get_addr(), lease_info))) {
        (void)ObDupTableLeaseInfoFactory::release(lease_info);
        lease_info = NULL;
        if (OB_ENTRY_EXIST == ret) {
          // the lease of the replica already exists, and there is race condition,
          // for the sake of simplicity, do not deal with this situation
          ret = OB_SUCCESS;
        } else {
          // TRANS_LOG(WARN, "[DUP TABLE]insert dup table lease info error", KR(ret), K(request), K_(partition));
          lease_statistics_.inc_insert_lease_info_err_count();
        }
      }
    } else {
      ret = tmp_ret;
      // TRANS_LOG(WARN, "[DUP TABLE]get dup table lease info error", KR(ret), K(request), K_(partition));
      lease_statistics_.inc_get_lease_info_err_count();
    }
  }

  if (NULL != lease_info) {
    if (OB_FAIL(update_lease_info_(lease_info, request))) {
      // TRANS_LOG(WARN,"[DUP TABLE]update lease_info error", KR(ret), K_(partition), K(request));
    }
    //lease expired , reject lease request
    if (lease_info->is_lease_expired()) {
      if (OB_FAIL(dup_table_lease_infos_.del(request.get_addr()))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          // TRANS_LOG(WARN, "[DUP TABLE]del lease info error", KR(ret), K_(partition), K(request));
        } else {
          // overwrite return code
          ret = OB_SUCCESS;
        }
      }
      lease_status = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_EXPIRED;
    }

    (void)dup_table_lease_infos_.revert(lease_info);
  }

  return ret;
}

int ObDupTablePartitionMgr::update_lease_info_(ObDupTableLeaseInfo *lease_info, const ObDupTableLeaseRequestMsg & request)
{
  int ret = OB_SUCCESS;
  //request may be from old version server, in which case gts would be -1
  const int64_t gts = request.get_gts();

  if (OB_ISNULL(lease_info)) {
    TRANS_LOG(WARN, "[DUP TABLE]invalid argument when update lease_info", K(lease_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(lease_info->update_cur_log_id(request.get_last_log_id()))) {
      TRANS_LOG(WARN, "update cur log id error", KR(ret), K(request));
    } else if (OB_FAIL(lease_info->update_lease_expired_ts(request.get_request_lease_interval_us()))) {
      TRANS_LOG(WARN, "update lease expired ts error", KR(ret), K(request));
    } else if (gts > 0 && OB_FAIL(lease_info->update_gts(gts))) {
      TRANS_LOG(WARN, "update gts error", KR(ret), K(request));
    }
  }

  return ret;
}

int ObDupTablePartitionMgr::handle_redo_log_sync_response(const ObRedoLogSyncResponseMsg &msg)
{
  UNUSED(msg);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObDupTablePartitionMgr::handle_dup_pre_commit_response(const ObPreCommitResponseMsg &msg)
{
  UNUSED(msg);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObDupTablePartitionMgr::generate_redo_log_sync_set(common::ObMaskSet2<ObAddrLogId> &msg_mask_set,
                                                       ObAddrLogIdArray &dup_table_lease_addrs,
                                                       const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  msg_mask_set.reset();
  dup_table_lease_addrs.reset();
  GenPlaFromDupTableLeaseHashMapFunctor functor(dup_table_lease_addrs, log_id);

  if (OB_FAIL(dup_table_lease_infos_.remove_if(functor))) {
    // TRANS_LOG(WARN, "iterator lease info error", KR(ret), K_(partition));
  } else if (OB_FAIL(functor.return_err())) {
    // TRANS_LOG(WARN, "iterator lease info return error", KR(ret), K_(partition));
  } else if (OB_FAIL(msg_mask_set.init(&dup_table_lease_addrs))) {
    // TRANS_LOG(WARN, "msg_mask_set init error", KR(ret), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObDupTablePartitionMgr::generate_pre_commit_set(common::ObMaskSet2<common::ObAddr> &mask_set,
                                                    ObAddrArray &addr_array,
                                                    const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  mask_set.reset();
  addr_array.reset();
  GenPreCommitSetHashMapFunctor functor(addr_array, commit_version);

  if (OB_FAIL(dup_table_lease_infos_.remove_if(functor))) {
    // TRANS_LOG(WARN, "iterator lease info error", KR(ret), K_(partition));
  } else if (OB_FAIL(functor.return_err())) {
    // TRANS_LOG(WARN, "iterator lease info return error", KR(ret), K_(partition));
  } else if (OB_FAIL(mask_set.init(&addr_array))) {
    // TRANS_LOG(WARN, "msg_mask_set init error", KR(ret), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObDupTablePartitionMgr::update_cur_log_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  atomic_update<uint64_t>(&cur_log_id_, log_id);

  return ret;
}

void ObDupTablePartitionMgr::print_lease_info()
{
  // PrintDupTableLeaseHashMapFunctor functor(partition_);
  // (void)dup_table_lease_infos_.for_each(functor);
  // TRANS_LOG(DEBUG, "[duplicate table stat]", K_(partition));
}

bool ObDupTablePartitionMgr::is_serving() const
{
  bool bool_ret = false;
  // For the leader, it must wait until the current time
  // has passed start_serving_ts to serve externally.
  // That is, you must wait for the old leader’s lease to expire
  bool_ret = (ObTimeUtility::current_time() >= start_serving_ts_);
  if (!bool_ret) {
    // TRANS_LOG(WARN, "ObDupTablePartitionMgr not serving",
    //           K(partition_), K(start_serving_ts_), K(is_dup_table_));
  }
  return bool_ret;
}

// int ObDupTableLeaseTask::init(const common::ObPartitionKey pkey, ObTransService *txs)
// {
//   int ret = OB_SUCCESS;
//
//   if (is_inited_) {
//     ret = OB_INIT_TWICE;
//     TRANS_LOG(WARN, "ObDupTableLeaseTask init twice");
//   } else if (!pkey.is_valid() || OB_ISNULL(txs)) {
//     ret = OB_INVALID_ARGUMENT;
//     TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), KP(txs));
//   } else {
//     pkey_ = pkey;
//     txs_ = txs;
//     is_inited_ = true;
//   }
//
//   return ret;
// }

void ObDupTableLeaseTask::reset()
{
  is_inited_ = false;
  txs_ = NULL;
}

void ObDupTableLeaseTask::runTimerTask()
{
}

void ObDupTableRedoSyncTask::reset()
{
  ObTransTask::reset();
  trans_id_.reset();
  log_id_ = 0;
  log_type_ = 0;
  timestamp_ = 0;
  last_generate_mask_set_ts_ = 0;
  is_mask_set_ready_ = 0;
  create_ts_ = 0;
}

bool ObDupTableRedoSyncTask::is_valid() const
{
  return
    trans_id_.is_valid() &&
    log_id_ > 0 &&
    log_type_ > 0 &&
    timestamp_ > 0;
}

int ObDupTableRedoSyncTask::make(const int64_t task_type,
                                 const ObTransID &trans_id,
                                 const uint64_t log_id,
                                 const int64_t log_type,
                                 const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(!trans_id.is_valid() ||
  //                 !partition.is_valid() ||
  //                 log_id <= 0 ||
  //                 log_type <= 0 ||
  //                 timestamp <= 0)) {
  //   TRANS_LOG(WARN, "invalid argument", K(trans_id), K(partition), K(log_id),
  //             K(log_type), K(timestamp));
  //   ret = OB_INVALID_ARGUMENT;
  // } else if (OB_FAIL(ObTransTask::make(task_type))) {
  //   TRANS_LOG(WARN, "ObTransTask make error", KR(ret), K(task_type), K(partition), K(trans_id), K(log_id));
  // } else {
  //   trans_id_ = trans_id;
  //   partition_ = partition;
  //   log_id_ = log_id;
  //   log_type_ = log_type;
  //   timestamp_ = timestamp;
  //   create_ts_ = ObTimeUtility::current_time();
  // }

  return ret;
}

bool ObPreCommitTask::need_generate_mask_set() const
{
  return INT64_MAX == last_generate_mask_set_ts_ ||
           ObTimeUtility::current_time() - last_generate_mask_set_ts_ >=
              ObTransService::DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US;
}

void ObDupTablePartCtxInfo::reset()
{
  msg_mask_set_.reset();
  lease_addrs_.reset();
  redo_sync_task_ = NULL;
  pre_commit_task_ = NULL;
  is_prepare_ = false;
  syncing_log_id_ = UINT64_MAX;
  syncing_log_ts_ = INT64_MAX;
}

void ObDupTablePartCtxInfo::destroy()
{
  if (NULL != redo_sync_task_) {
    ObDupTableRedoSyncTaskFactory::release(redo_sync_task_);
    redo_sync_task_ = NULL;
  }
  if (NULL != pre_commit_task_) {
    pre_commit_task_->~ObPreCommitTask();
    ob_free(pre_commit_task_);
    pre_commit_task_ = NULL;
  }
  reset();
}

int ObDupTablePartCtxInfo::alloc_redo_log_sync_task()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redo_sync_task_ = ObDupTableRedoSyncTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "[dup table] alloc redo_sync_task failed");
  }

  return ret;
}

int ObDupTablePartCtxInfo::alloc_pre_commit_task()
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = NULL;

  if (OB_ISNULL(tmp_ptr = ob_malloc(sizeof(ObPreCommitTask), "PreCommitTask"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "[dup table] alloc pre_commit_task failed");
  } else {
    pre_commit_task_ = new (tmp_ptr) ObPreCommitTask();
  }

  return ret;
}
}//transaction
}//oceanbase
