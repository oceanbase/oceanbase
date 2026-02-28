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

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_sby_read_define.h"

namespace oceanbase
{
namespace transaction
{

int ObPartTransCtx::infer_standby_trx_state_v2(const share::SCN snapshot,
                                               const int64_t collect_state_timeout_us,
                                               const bool filter_unreadable_prepare_trx,
                                               ObTxCommitData::TxDataState &tx_data_state,
                                               share::SCN &commit_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  tx_data_state = ObTxCommitData::TxDataState::UNKOWN;
  commit_version.set_invalid();

  ObTxCommitData::TxDataState infer_trx_state = ObTxCommitData::TxDataState::UNKOWN;
  share::SCN infer_trx_commit_version;
  infer_trx_commit_version.set_invalid();
  share::SCN ls_replica_readable_scn;
  ls_replica_readable_scn.set_invalid();

  const int64_t start_time = ObTimeUtility::fast_current_time();
  int64_t print_log_time = start_time;

  //# 1. Check invalid arguments
  if (!snapshot.is_valid_and_not_min() || collect_state_timeout_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(snapshot), K(collect_state_timeout_us),
              K(get_trans_id()), K(get_ls_id()), KP(this));
  }

  //# 2. Check the local replica trx state with the lock of this tx_ctx.
  //     If the local replica trx state is not determined, try to collect state info from all
  //     participants until timeout.
  do {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {

    } else if (!get_ls_id().is_valid() || !is_inited()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "the tx_ctx is not inited, need retry without lock", K(ret), K(snapshot),
                K(get_trans_id()), K(get_ls_id()), KP(this));
    } else {
      //## 2.1 Ensure the ls_replica_readable_scn exceeds the snapshot before accessing tx_ctx
      // state.
      //     If we check ls_replica_readable_scn after accessed tx_ctx state:
      //      +----------------------------------------------------------+
      //      |                snapshot > ls_replayed_scn                |
      //      +----------------------------------------------------------+
      //        |
      //        |
      //        v
      //      +----------------------------------------------------------+
      //      |                 exec_info.state == init                  |
      //      +----------------------------------------------------------+
      //        |
      //        |
      //        v
      //      +----------------------------------------------------------+
      //      |      Prepare Version > ls_replayed_scn > snapshot?       |
      //      +----------------------------------------------------------+
      //        |
      //        | replay prepare log and get ls_replayed_scn
      //        v
      //      +----------------------------------------------------------+
      //      | ls_replayed_scn > snapshot && prepare_version < snapshot |
      //      +----------------------------------------------------------+
      if (!ls_replica_readable_scn.is_valid()) {
        if (OB_FAIL(get_ls_replica_readable_scn_(get_ls_id(), ls_replica_readable_scn))) {
          TRANS_LOG(WARN, "get ls replica readable scn failed, retry until timeout", K(ret),
                    K(get_ls_id()), K(get_trans_id()), KP(this), K(ls_replica_readable_scn));
          ls_replica_readable_scn.set_invalid();
          ret = OB_EAGAIN;
        }
      }

      if (OB_SUCC(ret)) {
        if (!ls_replica_readable_scn.is_valid_and_not_min()) {
          ret = OB_EAGAIN;
          TRANS_LOG(WARN, "invalid ls replica readable scn", K(ret), K(get_ls_id()),
                    K(get_trans_id()), KP(this), K(ls_replica_readable_scn));
        } else if (ls_replica_readable_scn < snapshot) {
          ret = OB_REPLICA_NOT_READABLE;
          TRANS_LOG(WARN, "the standby read cannot execute on a replica that lags behind in replay",
                    K(ret), K(ret), K(get_ls_id()), K(get_trans_id()), KP(this),
                    K(ls_replica_readable_scn), K(snapshot));
        }
      }

      //## 2.2 Check if the tx_ctx state is 'Prepare', if so, it is necessary to obtain the
      // collection
      // results;
      //     otherwise, the trx state and the trx version can be directly determined.
      if (OB_SUCC(ret)) {
        CtxLockGuard guard;
        if (!lock_.is_locked_by_self()) {
          get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
        }
        ObTxSbyStateInfo ctx_sby_state_info;
        if (OB_SUCC(cache_sby_state_info_.get_confirmed_state(infer_trx_state,
                                                              infer_trx_commit_version))) {
          TRANS_LOG(DEBUG, "get confirmed state from the cache", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(cache_sby_state_info_), KPC(this));
        } else if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get confirmed state from the cache failed", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(cache_sby_state_info_), KPC(this));
        } else if (OB_SUCC(cache_sby_state_info_.infer_by_userful_info(
                       snapshot, filter_unreadable_prepare_trx, infer_trx_state,
                       infer_trx_commit_version))) {
          TRANS_LOG(DEBUG, "infer the trx state from the cache", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(cache_sby_state_info_), KPC(this));
        } else if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "infer the trx state from the cache failed", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(cache_sby_state_info_), KPC(this));
        } else if (OB_FAIL(build_ctx_sby_state_info_(snapshot, ls_replica_readable_scn, INVALID_LS,
                                                     -1, -1, ctx_sby_state_info))) {
          TRANS_LOG(WARN, "build local tx ctx state info failed", K(ret), K(snapshot),
                    K(ls_replica_readable_scn), K(ctx_sby_state_info), KPC(this));
        } else if (OB_SUCC(ctx_sby_state_info.get_confirmed_state(infer_trx_state,
                                                                  infer_trx_commit_version))) {
          TRANS_LOG(DEBUG, "get confirmed state from the ctx", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(ctx_sby_state_info), KPC(this));
          cache_sby_state_info_.try_to_update(ctx_sby_state_info, false);

        } else if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get confirmed state from the ctx failed", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(ctx_sby_state_info), KPC(this));

        } else if (OB_SUCC(ctx_sby_state_info.infer_by_userful_info(
                       snapshot, filter_unreadable_prepare_trx, infer_trx_state,
                       infer_trx_commit_version))) {
          cache_sby_state_info_.try_to_update(ctx_sby_state_info, false);
          TRANS_LOG(DEBUG, "infer the trx state from the ctx", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(ctx_sby_state_info), K(cache_sby_state_info_),
                    KPC(this));

        } else if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "infer the trx state from the ctx failed", K(ret), K(infer_trx_state),
                    K(infer_trx_commit_version), K(ctx_sby_state_info), K(cache_sby_state_info_),
                    KPC(this));
        }

//         TRANS_LOG(INFO, "try to infer a standby trx state from the local replica", K(ret),
//                   K(trans_id_), K(ls_id_), K(snapshot), K(ls_replica_readable_scn),
//                   K(infer_trx_state), K(infer_trx_commit_version), K(cache_sby_state_info_),
//                   K(ctx_sby_state_info));
      }

      //# 3. If we can not confirm the trx_state or commit_version by the local tx_ctx,it must be
      // infered by the state_info which collected from all participants.
      if (ret == OB_EAGAIN
          && is_undecied_standby_trx_state_info(infer_trx_state, infer_trx_commit_version)) {

        TRANS_LOG(INFO, "undecided standby trx state info, need ask to participants", K(ret),
                  K(trans_id_), K(ls_id_), K(infer_trx_state), K(infer_trx_commit_version),
                  K(cache_sby_state_info_), K(snapshot), K(ls_replica_readable_scn));

        if (OB_TMP_FAIL(ask_sby_participants_(snapshot, ls_replica_readable_scn))) {
          TRANS_LOG(WARN, "ask standby participants", K(ret), K(tmp_ret), K(trans_id_), K(ls_id_),
                    K(infer_trx_state), K(infer_trx_commit_version));
        }
      }
    }

    const int64_t cur_time = ObTimeUtility::fast_current_time();

    if (cur_time - print_log_time > 1 * 1000 * 1000) {
      TRANS_LOG(INFO, "collect standby participants state info cost too much time", K(ret),
                K(get_ls_id()), K(get_trans_id()), KP(this), K(ls_replica_readable_scn),
                K(snapshot), K(start_time), K(print_log_time), K(cur_time));
      print_log_time = cur_time;
    }

    if (ret == OB_EAGAIN) {
      if (cur_time - start_time > collect_state_timeout_us) {
        ret = OB_TRANS_RPC_TIMEOUT;
        TRANS_LOG(WARN, "collect standby participants state info timeout", K(ret), K(get_ls_id()),
                  K(get_trans_id()), KP(this), K(ls_replica_readable_scn), K(snapshot),
                  K(start_time), K(print_log_time), K(cur_time));
      } else {
        usleep(100);
      }
    }
  } while (ret == OB_EAGAIN);

  if (OB_SUCC(ret)) {
    if (!is_undecied_standby_trx_state_info(infer_trx_state, infer_trx_commit_version)) {
      tx_data_state = infer_trx_state;
      commit_version = infer_trx_commit_version;

    } else {
      ret = OB_REPLICA_NOT_READABLE;
      TRANS_LOG(WARN, "we cannot inferred a decided standy trx state info", K(ret),
                K(infer_trx_state), K(infer_trx_commit_version), K(get_trans_id()), K(get_ls_id()),
                K(snapshot), K(filter_unreadable_prepare_trx), K(ls_replica_readable_scn),
                K(cache_sby_state_info_));
    }
  }

  return ret;
}

int ObPartTransCtx::build_ctx_sby_state_info_(const share::SCN read_snapshot,
                                              const share::SCN ls_replica_readable_scn,
                                              const ObLSID sender_ls_id,
                                              const int64_t epoch,
                                              const int64_t transfer_epoch,
                                              ObTxSbyStateInfo &state_info)
{
  int ret = OB_SUCCESS;
  state_info.reset();
  state_info.init(ls_id_);

  CtxLockGuard guard;
  if (!lock_.is_locked_by_self()) {
    get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  if (!is_follower_()) {
    TRANS_LOG(INFO, "Please attention! We are infering the standby trx state on a tx_ctx leader",
              K(ret), KPC(this));
  }

  // get state without prepare_version
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!read_snapshot.is_valid_and_not_min()
             || !ls_replica_readable_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(read_snapshot), K(ls_replica_readable_scn),
              K(trans_id_), K(ls_id_));
  } else if (get_durable_state_() == ObTxState::UNKNOWN) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "Unkown durable state in tx_ctx", K(ret), KPC(this));
    print_trace_log_();
  } else if (exec_info_.max_applied_log_ts_ < exec_info_.max_applying_log_ts_
             && ls_replica_readable_scn >= exec_info_.max_applying_log_ts_) {
    /*check if the replayed scn is valid*/
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
              "The ls replayed scn must be smaller than the max_applying_log_ts during the "
              "retry of the replay task",
              K(ret), KPC(this));
  } else if (ls_replica_readable_scn < read_snapshot) {
    ret = OB_REPLICA_NOT_READABLE;
    TRANS_LOG(WARN, "unreadable replica with the smaller ls_replica_readable_scn", K(ret),
              K(trans_id_), K(ls_id_), K(read_snapshot), K(ls_replica_readable_scn));
  } else if (replay_completeness_.is_incomplete() || get_durable_state_() == ObTxState::CLEAR) {
    TRANS_LOG(INFO, "There is a incompleted replay tx ctx. Only tx data state is valid", K(ret),
              K(read_snapshot), KPC(this));
    if (ctx_tx_data_.get_state() == ObTxCommitData::TxDataState::COMMIT) {
      state_info.fill(ls_id_, ObTxState::COMMIT, ctx_tx_data_.get_commit_version(),
                      ls_replica_readable_scn, read_snapshot);
    } else if (ctx_tx_data_.get_state() == ObTxCommitData::ABORT) {
      state_info.fill(ls_id_, ObTxState::ABORT, share::SCN::invalid_scn(), ls_replica_readable_scn,
                      read_snapshot);
    } else if (ctx_tx_data_.get_state() == ObTxCommitData::TxDataState::RUNNING) {
      if (ObTxState::CLEAR == get_durable_state_()) {
        ret = OB_TRANS_CTX_NOT_EXIST;
        TRANS_LOG(INFO, "A cleared ctx without tx data cannot be inferred", K(ret), KPC(this));
      } else {
        state_info.fill(ls_id_, get_durable_state_(), share::SCN::invalid_scn(),
                        ls_replica_readable_scn, read_snapshot);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unexpected tx data state", K(ret), KPC(this));
      print_trace_log_();
    }
  } else if (get_durable_state_() < ObTxState::PREPARE
             || (get_durable_state_() == ObTxState::PREPARE
                 && exec_info_.max_applied_log_ts_ > exec_info_.max_applying_log_ts_)) {
    /*This tx_ctx has not replayed a prepare log --> snapshot <= replayed scn <
     * prepare_log_ts(prepare version)*/
    state_info.fill(ls_id_, ObTxState::INIT, share::SCN::invalid_scn(), ls_replica_readable_scn,
                    read_snapshot);
  } else if (get_durable_state_() == ObTxState::PREPARE) {
    /*This tx_ctx's state is prepared. If the filter_unreadable_prepare_trx is true, we set
     * tx_data_state as RUNNING to skip this version for read*/
    state_info.fill(ls_id_, ObTxState::PREPARE, share::SCN::invalid_scn(), ls_replica_readable_scn,
                    read_snapshot);
  } else if (get_durable_state_() == ObTxState::PRE_COMMIT
             || get_durable_state_() == ObTxState::COMMIT) {
    /*This tx_ctx's state is determined. If we can not get a valid commit_version for a
     * committed trx, it will be inferred by state_info from all participants*/
    if (ctx_tx_data_.get_commit_version().is_valid_and_not_min()) {
      state_info.fill(ls_id_, ObTxState::COMMIT, ctx_tx_data_.get_commit_version(),
                      ls_replica_readable_scn, read_snapshot);
    } else {
      state_info.fill(ls_id_, ObTxState::PREPARE, share::SCN::invalid_scn(),
                      ls_replica_readable_scn, read_snapshot);
      TRANS_LOG(INFO, "a committed trx without the commit_version", K(ret), K(state_info),
                KPC(this));
    }
  } else if (get_durable_state_() == ObTxState::ABORT) {
    state_info.fill(ls_id_, ObTxState::ABORT, share::SCN::invalid_scn(), ls_replica_readable_scn,
                    read_snapshot);
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "Unexpected tx state", K(ret), KPC(this));
    print_trace_log_();
  }

  // handle prepare_version
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (state_info.tx_state_ == ObTxState::PREPARE) {
    if (exec_info_.prepare_version_.is_valid_and_not_min()) {
      if (sender_ls_id.is_valid()) {
        if (!is_exec_complete_without_lock(sender_ls_id, epoch, transfer_epoch)) {
          state_info.fill(ls_id_, ObTxState::ABORT, share::SCN::invalid_scn(),
                          ls_replica_readable_scn, read_snapshot, true /*is_transfer_prepare*/);
          TRANS_LOG(INFO, "the transfer_epoch is not complete", K(ret), K(state_info),
                    K(sender_ls_id), K(epoch), K(transfer_epoch), KPC(this));
        }
      }
      if (state_info.tx_state_ == ObTxState::PREPARE) {
        // if the snapshot is larger than the prepare_version,
        // we can not infer it by the local state;
        state_info.trans_version_ = exec_info_.prepare_version_;
      }
    } else if (ObTxState::COMMIT == get_durable_state_()) {
      // reply for a commit log and retrying
      ret = OB_REPLICA_NOT_READABLE;
      state_info.reset();
      TRANS_LOG(INFO, "invalid prepare version with a replaying commit log", K(ret), KPC(this));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid prepare_version with the prepare state", K(ret), KPC(this));
      print_trace_log_();
    }
  }

  if (OB_SUCC(ret)) {
    state_info.src_addr_ = GCONF.self_addr_;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(INFO, "build ctx info in local ctx", K(ret), KP(this), K(trans_id_), K(ls_id_),
                K(state_info), K(get_durable_state_()), K(exec_info_.prepare_version_),
                K(ctx_tx_data_), K(read_snapshot), K(sender_ls_id), K(epoch), K(transfer_epoch),
                K(ls_replica_readable_scn));
    }
  }

  return ret;
}

int ObPartTransCtx::sby_read_ask_upstream_(const share::SCN read_snapshot,
                                           const share::SCN ls_replica_readable_scn,
                                           const share::ObLSID ori_ls_id,
                                           const ObAddr &ori_addr)
{
  int ret = OB_SUCCESS;

  if (!read_snapshot.is_valid_and_not_min() || !ori_ls_id.is_valid() || !ori_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(read_snapshot), K(ori_ls_id), K(ori_addr),
              KPC(this));
  } else if (is_root()) {
    ObTxSbyAskDownstreamReq ask_self_req;
    ask_self_req.fill_common_header(tenant_id_, cluster_version_, trans_id_, read_snapshot, ls_id_,
                                    GCONF.self_addr_, ls_id_, GCONF.self_addr_);
    ask_self_req.root_ls_id_ = ls_id_;
    ask_self_req.root_addr_ = GCONF.self_addr_;
    ask_self_req.exec_epoch_ = -1;
    ask_self_req.transfer_epoch_ = -1;
    // record ori_addr && collect the state of all parts
    if (OB_FAIL(init_parts_sby_info_array_(read_snapshot, ori_ls_id, ori_addr))) {
      TRANS_LOG(WARN, "init parts_sby_info_array failed", K(ret), K(read_snapshot), K(ori_ls_id),
                K(ori_addr), KPC(this));
    } else if (OB_FAIL(sby_read_ask_downsteam_(ask_self_req, ls_replica_readable_scn))) {
      TRANS_LOG(WARN, "ask downstream for standy failed", K(ret), K(read_snapshot), K(ls_id_),
                K(trans_id_), K(is_root()), K(ori_ls_id), K(ori_addr), K(ask_self_req));
    }

  } else {
    ObTxSbyAskUpstreamReq req;
    ObAddr upstream_addr;
    if (OB_FAIL(MTL(ObTransService *)
                    ->get_location_adapter()
                    ->nonblock_get_leader(GCONF.cluster_id, tenant_id_, exec_info_.upstream_,
                                          upstream_addr))) {
      // OB_LS_IS_DELETED
      TRANS_LOG(WARN, "get upstream leader addr failed", K(ret), K(read_snapshot), K(ori_ls_id),
                K(ori_addr), K(upstream_addr), KPC(this));
    } else {
      req.fill_common_header(tenant_id_, cluster_version_, trans_id_, read_snapshot, ls_id_,
                             GCONF.self_addr_, exec_info_.upstream_, upstream_addr);
      req.ori_ls_id_ = ori_ls_id;
      req.ori_addr_ = ori_addr;
      if (OB_FAIL(MTL(ObTransService *)->get_sby_rpc_impl().post_msg(req.dst_addr_, req))) {
        TRANS_LOG(WARN, "post standy ask_upstream request failed", K(ret), K(req));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::sby_read_ask_downsteam_(const ObTxSbyAskDownstreamReq &req,
                                            const share::SCN &ls_replica_readable_scn)
{
  int ret = OB_SUCCESS;

  if (!req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(req), KPC(this));

  } else {
    bool need_ask_downstream = false;

    // update cache and reply
    if (cache_sby_state_info_.is_confirmed()) {
      need_ask_downstream = false;
      TRANS_LOG(INFO, "no need to ask downstream with a confirmed state_info", K(ret), K(req),
                K(cache_sby_state_info_), K(trans_id_), K(ls_id_));
    } else if (is_root()) {
      need_ask_downstream = true;
    } else if (!exec_info_.commit_parts_.empty() && exec_info_.upstream_ == req.src_ls_id_) {
      need_ask_downstream = true;
    } else if (!exec_info_.commit_parts_.empty()) {
      TRANS_LOG(INFO, "no need to ask downstream for standy read", K(ret), K(req), KPC(this));
    }

    if (OB_SUCC(ret) && need_ask_downstream) {
      int tmp_ret = OB_SUCCESS;
      ObTxSbyAskDownstreamReq next_req;
      next_req.fill_common_header(tenant_id_, cluster_version_, trans_id_, req.msg_snapshot_,
                                  ls_id_, GCONF.self_addr_, ls_id_, GCONF.self_addr_);
      next_req.root_ls_id_ = req.root_ls_id_;
      next_req.root_addr_ = req.root_addr_;

      for (int i = 0; i < exec_info_.commit_parts_.count(); i++) {
        next_req.dst_ls_id_ = exec_info_.commit_parts_[i].ls_id_;
        next_req.exec_epoch_ = exec_info_.commit_parts_[i].exec_epoch_;
        next_req.transfer_epoch_ = exec_info_.commit_parts_[i].transfer_epoch_;
        if (OB_TMP_FAIL(MTL(ObTransService *)
                            ->get_location_adapter()
                            ->nonblock_get_leader(GCONF.cluster_id, tenant_id_, next_req.dst_ls_id_,
                                                  next_req.dst_addr_))) {
          TRANS_LOG(WARN, "get downstream location leader failed", K(ret), K(tmp_ret), K(req),
                    K(next_req), KPC(this));
        } else if (OB_TMP_FAIL(MTL(ObTransService *)
                                   ->get_sby_rpc_impl()
                                   .post_msg(next_req.dst_addr_, next_req))) {
          TRANS_LOG(WARN, "post standby ask_downstream msg failed", K(ret), K(tmp_ret), K(req),
                    K(next_req), KPC(this));
        } else {
          TRANS_LOG(DEBUG, "post downstream request successfully", K(ret), K(i),
                    K(exec_info_.commit_parts_[i]), K(next_req), K(req));
        }
      }

      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(INFO, "post downstream request", K(ret), K(exec_info_.commit_parts_), K(req));
      }
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObTxSbyStateInfo ctx_sby_state_info;
      if (!cache_sby_state_info_.is_confirmed()) {
        if (OB_TMP_FAIL(build_ctx_sby_state_info_(req.msg_snapshot_, ls_replica_readable_scn,
                                                  req.src_ls_id_, req.exec_epoch_,
                                                  req.transfer_epoch_, ctx_sby_state_info))) {
          TRANS_LOG(WARN, "build ctx standby state_info failed", K(ret), K(tmp_ret), K(req),
                    K(ls_replica_readable_scn), KPC(this));
          if (tmp_ret == OB_REPLICA_NOT_READABLE) {
            if (OB_TMP_FAIL(MTL(ObTransService *)->ask_sby_state_info_from_replicas(&req))) {
              TRANS_LOG(WARN, "ask other replicas failed", K(ret), K(tmp_ret), K(req),
                        K(ls_replica_readable_scn));
            }
          }
        }
      } else {
        TRANS_LOG(INFO, "reply a confirmed cache_sby_state_info", K(ret), K(tmp_ret), K(req),
                  K(ls_replica_readable_scn), K(ctx_sby_state_info), K(cache_sby_state_info_));
        ctx_sby_state_info.try_to_update(cache_sby_state_info_, true /*allow_update_by_other_ls*/);
      }

      if (!ctx_sby_state_info.is_useful() || OB_TMP_FAIL(tmp_ret)) {
        TRANS_LOG(INFO, "unused cache sby state info", K(ret), K(tmp_ret), K(ctx_sby_state_info),
                  K(cache_sby_state_info_), K(req));
      } else if (OB_TMP_FAIL(sby_read_post_state_result_(ctx_sby_state_info, req.root_ls_id_,
                                                         req.root_addr_))) {
        TRANS_LOG(WARN, "post standby state result failed", K(ret), K(tmp_ret), K(req),
                  K(ctx_sby_state_info), K(cache_sby_state_info_), KPC(this));
      }
    }
  }
  return ret;
}

int ObPartTransCtx::sby_read_post_state_result_(const ObTxSbyStateInfo &state_info,
                                                const share::ObLSID dst_ls_id,
                                                const ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;

  if (!state_info.is_valid() || !dst_ls_id.is_valid() || !dst_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(state_info), K(dst_ls_id), K(dst_addr));
  } else {
    ObTxSbyStateResultMsg msg;
    msg.fill_common_header(tenant_id_, cluster_version_, trans_id_,
                           state_info.max_applied_snapshot_, ls_id_, GCONF.self_addr_, dst_ls_id,
                           dst_addr);
    msg.from_root_ = is_root();
    msg.src_state_info_ = state_info;
    for (int i = 0; OB_SUCC(ret) && i < exec_info_.commit_parts_.count(); i++) {
      if (OB_FAIL(msg.downstream_parts_.push_back(exec_info_.commit_parts_[i].ls_id_))) {
        TRANS_LOG(WARN, "push back downstream failed", K(ret), K(exec_info_.commit_parts_[i]), K(i),
                  K(trans_id_), K(ls_id_));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (dst_addr == GCONF.self_addr_ && dst_ls_id == ls_id_
               && OB_FAIL(handle_sby_state_result(msg))) {
      TRANS_LOG(WARN, "handle state result for self failed", K(ret), K(msg), K(trans_id_),
                K(ls_id_));
    } else if (OB_FAIL(MTL(ObTransService *)
                           ->get_location_adapter()
                           ->nonblock_get_leader(GCONF.cluster_id, tenant_id_, msg.dst_ls_id_,
                                                 msg.dst_addr_))) {
      TRANS_LOG(WARN, "get downstream location leader failed", K(ret), K(msg), KPC(this));
    } else if (OB_FAIL(MTL(ObTransService *)->get_sby_rpc_impl().post_msg(msg.dst_addr_, msg))) {
      TRANS_LOG(WARN, "post standby state_result msg failed", K(ret), K(msg), KPC(this));
    }
    TRANS_LOG(INFO, "post standby state_result msg", K(ret), K(msg), KPC(this));
  }

  return ret;
}

int ObPartTransCtx::init_parts_sby_info_array_(const share::SCN read_snapshot,
                                               const share::ObLSID ori_ls_id,
                                               const ObAddr &ori_addr)
{
  int ret = OB_SUCCESS;

  if (!read_snapshot.is_valid_and_not_min() || !ori_ls_id.is_valid() || !ori_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(read_snapshot), K(ori_addr), K(ori_ls_id),
              K(ls_id_), K(trans_id_));

  } else {
    bool contain_self = false;
    if (OB_SUCC(ret) && parts_sby_info_list_.empty()) {
      for (int i = 0; OB_SUCC(ret) && i < exec_info_.commit_parts_.count(); i++) {
        ObTxSbyStateInfo state_info;
        state_info.init(exec_info_.commit_parts_[i].ls_id_);
        state_info.fill(exec_info_.commit_parts_[i].ls_id_, ObTxState::UNKNOWN,
                        share::SCN::invalid_scn(), share::SCN::invalid_scn(),
                        share::SCN::invalid_scn());

        if (!state_info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "try to insert a invalid state info", K(ret), K(read_snapshot),
                    K(ori_ls_id), K(ori_addr), K(state_info), K(trans_id_), K(ls_id_));
        } else if (OB_FAIL(parts_sby_info_list_.push_back(state_info))) {
          TRANS_LOG(WARN, "push back state_info failed", K(ret), K(i),
                    K(exec_info_.commit_parts_[i]), K(read_snapshot), K(ori_ls_id), K(ori_addr),
                    K(trans_id_), K(ls_id_));
        } else if (state_info.part_id_ == ls_id_) {
          contain_self = true;
        }
      }

      if (OB_SUCC(ret) && !contain_self) {

        ObTxSbyStateInfo state_info;
        state_info.init(ls_id_);
        state_info.fill(ls_id_, ObTxState::UNKNOWN, share::SCN::invalid_scn(),
                        share::SCN::invalid_scn(), share::SCN::invalid_scn());

        if (!state_info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "try to insert a invalid state info", K(ret), K(read_snapshot),
                    K(ori_ls_id), K(ori_addr), K(state_info), K(trans_id_), K(ls_id_));
        } else if (OB_FAIL(parts_sby_info_list_.push_back(state_info))) {
          TRANS_LOG(WARN, "push back state_info failed", K(ret), K(ls_id_), K(read_snapshot),
                    K(ori_ls_id), K(ori_addr), K(trans_id_), K(ls_id_));
        }
      }

      if (OB_FAIL(ret)) {
        parts_sby_info_list_.reuse();
      }
    }

    if (OB_SUCC(ret)
        && ObTxSbyAskOrigin::MAX_WAIT_ASK_ORIGIN_COUNT >= sby_origin_list_.get_size()) {
      ObTxSbyAskOrigin *prev_origin = nullptr;

      DLIST_FOREACH_BACKWARD_X(cur, sby_origin_list_, OB_SUCC(ret))
      {
        if (cur->read_snapshot_ <= read_snapshot) {
          prev_origin = cur;
          break;

        } else {
          // do nothing
        }
      }
      if (OB_SUCC(ret) && prev_origin == nullptr) {
        prev_origin = sby_origin_list_.get_first();
      }

      if (OB_SUCC(ret) && prev_origin != nullptr) {
        int tmp_ret = OB_SUCCESS;
        ObTxSbyAskOrigin *tmp_origin =
            static_cast<ObTxSbyAskOrigin *>(mtl_malloc(sizeof(ObTxSbyAskOrigin), "SbyAskOrigin"));
        if (OB_ISNULL(tmp_origin)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "alloc memory failed", K(ret), KP(tmp_origin), K(trans_id_), K(ls_id_));
        } else {
          new (tmp_origin) ObTxSbyAskOrigin(read_snapshot, ori_ls_id, ori_addr);
          if (prev_origin == sby_origin_list_.get_header()
              || prev_origin->read_snapshot_ != tmp_origin->read_snapshot_
              || prev_origin->ori_ls_id_ != tmp_origin->ori_ls_id_
              || prev_origin->ori_addr_ != tmp_origin->ori_addr_) {
            if (!sby_origin_list_.add_before(prev_origin, tmp_origin)) {
              tmp_ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "insert into sby_origin list failed", K(ret), K(trans_id_), K(ls_id_),
                        KP(prev_origin), KPC(tmp_origin));
            }
          } else {
            tmp_ret = OB_EAGAIN;
          }

          if (OB_TMP_FAIL(tmp_ret)) {
            mtl_free(tmp_origin);
          } else {
            TRANS_LOG(INFO, "insert into sby_origin_list_", K(ret), K(tmp_ret), K(trans_id_),
                      K(ls_id_), KPC(tmp_origin), KP(prev_origin), KP(tmp_origin));
          }
        }
      }
    }
  }

  return ret;
}

int ObPartTransCtx::ask_sby_participants_(const share::SCN read_snapshot,
                                          const share::SCN ls_readable_scn)
{
  int ret = OB_SUCCESS;
  bool need_ask_upstream = false;

  {
    CtxLockGuard guard;
    if (!lock_.is_locked_by_self()) {
      get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
    }
    if (ask_state_info_interval_.reach()) {
      need_ask_upstream = true;
    }
  }

  if (!read_snapshot.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid  arguments", K(ret), K(read_snapshot), K(trans_id_), K(ls_id_));
  } else if (!need_ask_upstream) {
    // do nothing
  } else {
    ObTxSbyAskUpstreamReq req;
    req.fill_common_header(tenant_id_, cluster_version_, trans_id_, read_snapshot, ls_id_,
                           GCONF.self_addr_, ls_id_, GCONF.self_addr_);

    req.ori_ls_id_ = ls_id_;
    req.ori_addr_ = GCONF.self_addr_;
    if (OB_FAIL(handle_sby_ask_upstream(req, ls_readable_scn))) {
      TRANS_LOG(WARN, "handle standby ask_upstream request failed", K(ret), K(read_snapshot),
                K(req));
    }
  }

  return ret;
}

int ObPartTransCtx::handle_sby_ask_upstream(const ObTxSbyAskUpstreamReq &req,
                                            const share::SCN &ls_readable_scn)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard;
  if (!lock_.is_locked_by_self()) {
    get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  if (!req.is_valid()) {
    ret = OB_INVALID_SIZE;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_id_), K(ls_id_), K(req));
  } else if (OB_FAIL(sby_read_ask_upstream_(req.msg_snapshot_, ls_readable_scn, req.ori_ls_id_,
                                            req.ori_addr_))) {
    TRANS_LOG(WARN, "ask standy upstream failed", K(ret), K(req), K(ls_id_), K(trans_id_));
  }

  return ret;
}

int ObPartTransCtx::handle_sby_ask_downstream(const ObTxSbyAskDownstreamReq &req,
                                              const share::SCN &ls_readable_scn)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard;
  if (!lock_.is_locked_by_self()) {
    get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  if (!req.is_valid()) {
    ret = OB_INVALID_SIZE;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_id_), K(ls_id_), K(req));
  } else if (OB_FAIL(sby_read_ask_downsteam_(req, ls_readable_scn))) {
    TRANS_LOG(WARN, "ask downstream for standby failed", K(ret), K(trans_id_), K(ls_id_), K(req));
  }

  return ret;
}

int ObPartTransCtx::handle_sby_state_result(const ObTxSbyStateResultMsg &msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  CtxLockGuard guard;
  if (!lock_.is_locked_by_self()) {
    get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_id_), K(ls_id_), K(msg));
  } else if (cache_sby_state_info_.is_valid()
             && (msg.src_state_info_.tx_state_ == ObTxState::UNKNOWN
                 || cache_sby_state_info_.tx_state_ == ObTxState::UNKNOWN)) {

    if (OB_FAIL(handle_sby_unknown_state_info(msg.src_state_info_, cache_sby_state_info_))) {
      TRANS_LOG(WARN, "handle sby unknown state info failed", K(ret), K(msg),
                K(cache_sby_state_info_));
    } else {
      TRANS_LOG(INFO, "receive a unknown sby state info", K(ret), K(cache_sby_state_info_), K(msg),
                KPC(this));
    }
  } else if (ls_id_ == msg.src_state_info_.part_id_
             || msg.src_state_info_.flag_.flag_bit_.infer_by_root_
             || msg.src_state_info_.is_confirmed()) {
    cache_sby_state_info_.init(ls_id_);
    cache_sby_state_info_.try_to_update(msg.src_state_info_,
                                        msg.src_state_info_.flag_.flag_bit_.infer_by_root_
                                            || msg.src_state_info_.is_confirmed());
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_root() && !msg.src_state_info_.flag_.flag_bit_.infer_by_root_
             && !cache_sby_state_info_.is_confirmed()) {
    bool all_prepared = false;
    bool *contain_parts_array = nullptr;
    bool contain_src = false;
    share::SCN min_snapshot = share::SCN::max_scn();
    share::SCN max_readable_scn = share::SCN::min_scn();

    if (msg.downstream_parts_.count() > 0) {
      contain_parts_array = static_cast<bool *>(
          mtl_malloc(sizeof(bool) * msg.downstream_parts_.count(), "SbyPartsContain"));
      if (contain_parts_array == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc parts contain array failed", K(ret), K(msg), K(trans_id_),
                  K(ls_id_));
      }
      memset(contain_parts_array, 0, sizeof(bool) * msg.downstream_parts_.count());
    }

    for (int i = 0; i < parts_sby_info_list_.count() && OB_SUCC(ret); i++) {
      if (msg.src_state_info_.part_id_ == parts_sby_info_list_[i].part_id_) {
        parts_sby_info_list_[i].try_to_update(msg.src_state_info_, false);
        if (parts_sby_info_list_[i].max_applied_snapshot_.is_valid_and_not_min()) {
          min_snapshot = SCN::min(min_snapshot, parts_sby_info_list_[i].max_applied_snapshot_);
        }
        if (parts_sby_info_list_[i].ls_readable_scn_.is_valid()
            && !parts_sby_info_list_[i].ls_readable_scn_.is_max()) {
          max_readable_scn = SCN::max(max_readable_scn, parts_sby_info_list_[i].ls_readable_scn_);
        }
        contain_src = true;
      }

      for (int j = 0; j < msg.downstream_parts_.count() && OB_SUCC(ret); j++) {
        if (msg.downstream_parts_[j] == parts_sby_info_list_[i].part_id_) {
          contain_parts_array[j] = true;
        }
      }
    }

    for (int i = 0; i < msg.downstream_parts_.count() && OB_SUCC(ret); i++) {
      if (!contain_parts_array[i]) {
        ObTxSbyStateInfo state_info;
        state_info.init(msg.downstream_parts_[i]);
        state_info.fill(msg.downstream_parts_[i], ObTxState::UNKNOWN, share::SCN::invalid_scn(),
                        share::SCN::invalid_scn(), share::SCN::invalid_scn());
        if (OB_FAIL(parts_sby_info_list_.push_back(state_info))) {

          TRANS_LOG(WARN, "push back state_info failed", K(ret), K(i), K(msg.downstream_parts_[i]),
                    K(msg), K(trans_id_), K(ls_id_));
        } else {
          if (state_info.max_applied_snapshot_.is_valid()) {
            min_snapshot = SCN::min(min_snapshot, state_info.max_applied_snapshot_);
          }
          if (state_info.ls_readable_scn_.is_valid() && !state_info.ls_readable_scn_.is_max()) {
            max_readable_scn = SCN::max(max_readable_scn, state_info.ls_readable_scn_);
          }
        }
      }
    }

    mtl_free(contain_parts_array);

    if (OB_SUCC(ret)) {
      if (!contain_src) {
        if (OB_FAIL(parts_sby_info_list_.push_back(msg.src_state_info_))) {
          TRANS_LOG(WARN, "push back src state_info failed", K(ret), K(msg), K(trans_id_),
                    K(ls_id_));
        } else {

          if (msg.src_state_info_.max_applied_snapshot_.is_valid()) {
            min_snapshot = SCN::min(min_snapshot, msg.src_state_info_.max_applied_snapshot_);
          }
          if (msg.src_state_info_.ls_readable_scn_.is_valid()
              && !msg.src_state_info_.ls_readable_scn_.is_max()) {
            max_readable_scn = SCN::max(max_readable_scn, msg.src_state_info_.ls_readable_scn_);
          }
        }
      }
    }

    // infer by the parts
    if (OB_SUCC(ret)) {
      if (!cache_sby_state_info_.is_useful()
          || min_snapshot > cache_sby_state_info_.max_applied_snapshot_
          || (min_snapshot == cache_sby_state_info_.max_applied_snapshot_
              && !cache_sby_state_info_.flag_.flag_bit_.infer_by_root_)
          || max_readable_scn > cache_sby_state_info_.ls_readable_scn_) {
        if (OB_FAIL(infer_standby_trx_state_by_participants_())) {
          TRANS_LOG(WARN, "infer the standby trx state failed", K(ret), K(cache_sby_state_info_),
                    K(trans_id_), K(ls_id_), K(parts_sby_info_list_));
        }
      } else {
        TRANS_LOG(INFO, "no need to infer", K(ret), K(trans_id_), K(ls_id_), K(min_snapshot),
                  K(cache_sby_state_info_), K(parts_sby_info_list_));
      }
    }
  }

  if (OB_SUCC(ret) && cache_sby_state_info_.is_useful() && is_root()) {
    tmp_ret = OB_SUCCESS;
    DLIST_FOREACH_REMOVESAFE_NORET(cur, sby_origin_list_)
    {
      if (cache_sby_state_info_.is_confirmed()
          || cur->read_snapshot_ <= cache_sby_state_info_.max_applied_snapshot_) {
        // TRANS_LOG(INFO, "before try to post", K(ret), K(tmp_ret), KPC(cur), KP(cur),
        //           K(cache_sby_state_info_), K(trans_id_), K(ls_id_));
        if (cur->ori_ls_id_ == ls_id_ && cur->ori_addr_ == GCONF.self_addr_) {
          // do nothing
        } else if (OB_TMP_FAIL(sby_read_post_state_result_(cache_sby_state_info_, cur->ori_ls_id_,
                                                           cur->ori_addr_))) {
          TRANS_LOG(WARN, "post state result to ori failed", K(ret), K(tmp_ret), KPC(cur),
                    K(cache_sby_state_info_), K(trans_id_), K(ls_id_));
        }

        if (tmp_ret == OB_SUCCESS) {
          TRANS_LOG(INFO, "post state result to ori", K(ret), K(tmp_ret), KPC(cur), KP(cur),
                    K(cache_sby_state_info_), K(trans_id_), K(ls_id_));
          sby_origin_list_.remove(cur);
          mtl_free(cur);
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObPartTransCtx::infer_standby_trx_state_by_participants_()
{
  int ret = OB_SUCCESS;

  ObTxSbyStateInfo infer_state_info;
  infer_state_info.reset();
  infer_state_info.init(ls_id_);

  if (parts_sby_info_list_.count() < exec_info_.commit_parts_.count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(trans_id_), K(ls_id_), K(parts_sby_info_list_),
              K(exec_info_.commit_parts_));
  } else {
    int64_t prepared_participant_cnt = 0;
    int64_t unknown_participant_cnt = 0;
    share::SCN max_prepare_version;
    max_prepare_version.set_min();
    infer_state_info.tx_state_ = ObTxState::UNKNOWN;
    infer_state_info.max_applied_snapshot_.set_max();

    for (int i = 0;
         i < parts_sby_info_list_.count() && OB_SUCC(ret) && !infer_state_info.is_confirmed();
         i++) {
      infer_state_info.max_applied_snapshot_ = share::SCN::min(
          infer_state_info.max_applied_snapshot_, parts_sby_info_list_[i].max_applied_snapshot_);
      if (parts_sby_info_list_[i].ls_readable_scn_ < infer_state_info.max_applied_snapshot_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "Unexpected participant sby state_info", K(ret), K(i),
                  K(parts_sby_info_list_), K(infer_state_info), KPC(this));
      } else if (parts_sby_info_list_[i].tx_state_ == ObTxState::PRE_COMMIT
                 || parts_sby_info_list_[i].tx_state_ == ObTxState::COMMIT) {
        infer_state_info = parts_sby_info_list_[i];
        infer_state_info.tx_state_ = ObTxState::COMMIT;
        break;
      } else if (parts_sby_info_list_[i].tx_state_ == ObTxState::ABORT) {
        infer_state_info = parts_sby_info_list_[i];
        break;
      } else if (parts_sby_info_list_[i].tx_state_ == ObTxState::CLEAR) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "Unexpected tx state in parts_sby_info_array_", K(ret), K(trans_id_),
                  K(ls_id_), K(i), K(parts_sby_info_list_[i]), K(cache_sby_state_info_));
      } else if (parts_sby_info_list_[i].tx_state_ == ObTxState::PREPARE) {
        if (i == 0) {
          if (infer_state_info.tx_state_ != ObTxState::UNKNOWN
              || infer_state_info.trans_version_.is_valid_and_not_min()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "Unexpected first state", K(ret), K(trans_id_), K(ls_id_),
                      K(infer_state_info), K(parts_sby_info_list_[i]), K(i));
          } else {
            infer_state_info.tx_state_ = ObTxState::PREPARE;
            infer_state_info.trans_version_ = parts_sby_info_list_[i].trans_version_;
            infer_state_info.ls_readable_scn_ = parts_sby_info_list_[i].ls_readable_scn_;
            prepared_participant_cnt++;
          }
        } else if (infer_state_info.tx_state_ == ObTxState::PREPARE) {
          infer_state_info.trans_version_ = share::SCN::max(infer_state_info.trans_version_,
                                                            parts_sby_info_list_[i].trans_version_);
          prepared_participant_cnt++;

        } else if (infer_state_info.tx_state_ < ObTxState::PREPARE
                   && infer_state_info.tx_state_ >= ObTxState::INIT) {
          infer_state_info.tx_state_ = ObTxState::INIT;
          infer_state_info.ls_readable_scn_ = share::SCN::max(
              infer_state_info.ls_readable_scn_, parts_sby_info_list_[i].trans_version_);
        } else if (infer_state_info.tx_state_ == ObTxState::UNKNOWN) {
          if (OB_FAIL(handle_sby_unknown_state_info(parts_sby_info_list_[i], infer_state_info))) {
            TRANS_LOG(WARN, "handle sby unknown state info failed", K(ret), K(i),
                      K(parts_sby_info_list_), K(infer_state_info));
          }
        }

      } else if (parts_sby_info_list_[i].tx_state_ == ObTxState::UNKNOWN) {
        if (OB_FAIL(handle_sby_unknown_state_info(parts_sby_info_list_[i], infer_state_info))) {
          TRANS_LOG(WARN, "handle sby unknown state info failed", K(ret), K(i),
                    K(parts_sby_info_list_), K(infer_state_info));
        }

      } else if (parts_sby_info_list_[i].tx_state_ < ObTxState::PREPARE) {

        if (i == 0) {
          if (infer_state_info.tx_state_ != ObTxState::UNKNOWN
              || infer_state_info.trans_version_.is_valid_and_not_min()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "Unexpected first state", K(ret), K(trans_id_), K(ls_id_),
                      K(infer_state_info), K(parts_sby_info_list_[i]), K(i));
          } else {
            infer_state_info.tx_state_ = ObTxState::INIT;
            infer_state_info.trans_version_ = parts_sby_info_list_[i].trans_version_;
            infer_state_info.ls_readable_scn_ = parts_sby_info_list_[i].ls_readable_scn_;
          }
        } else if (infer_state_info.tx_state_ == ObTxState::PREPARE) {
          infer_state_info.tx_state_ = ObTxState::INIT;
          infer_state_info.ls_readable_scn_ = share::SCN::max(
              infer_state_info.trans_version_, parts_sby_info_list_[i].ls_readable_scn_);
          infer_state_info.trans_version_ = share::SCN::invalid_scn();
        } else if (infer_state_info.tx_state_ < ObTxState::PREPARE
                   && infer_state_info.tx_state_ >= ObTxState::INIT) {
          infer_state_info.ls_readable_scn_ = share::SCN::max(
              infer_state_info.ls_readable_scn_, parts_sby_info_list_[i].ls_readable_scn_);
        } else if (infer_state_info.tx_state_ == ObTxState::UNKNOWN) {
          if (OB_FAIL(handle_sby_unknown_state_info(parts_sby_info_list_[i], infer_state_info))) {
            TRANS_LOG(WARN, "handle sby unknown state info failed", K(ret), K(i),
                      K(parts_sby_info_list_), K(infer_state_info));
          }
        }
      }
    }

    if (OB_SUCC(ret) && prepared_participant_cnt >= parts_sby_info_list_.count()
        && infer_state_info.tx_state_ == ObTxState::PREPARE
        && infer_state_info.trans_version_.is_valid_and_not_min()) {
      TRANS_LOG(INFO, "all participants is prepared, set as the commit state", K(ret), K(trans_id_),
                K(ls_id_), K(prepared_participant_cnt), K(parts_sby_info_list_.count()),
                K(infer_state_info), K(parts_sby_info_list_));
      infer_state_info.tx_state_ = ObTxState::COMMIT;
    }
  }

  if (OB_SUCC(ret)) {
    infer_state_info.src_addr_ = GCONF.self_addr_;
    infer_state_info.flag_.flag_bit_.infer_by_root_ = true;
    cache_sby_state_info_.init(ls_id_);
    cache_sby_state_info_.try_to_update(infer_state_info, true);

    if (REACH_TIME_INTERVAL(100 * 1000)) {
      TRANS_LOG(INFO, "infer by standby participants' state_info", K(ret), K(trans_id_), K(ls_id_),
                K(infer_state_info), K(cache_sby_state_info_), K(parts_sby_info_list_));
    }
  }

  return ret;
}

// Clear log ts > Commit log ts >= L(readable_scn) >= P(readable_scn) >= Snapshot
//     => P(prepare_version) > snapshot
//
//     // L(prepare_version) > Snapshot
#define HANDLE_UNKNOWN_SBY_TX_STATE(UNKOWN_STATE_INFO, PREPARE_STATE_INFO, RES_STATE_INFO)        \
  if (PREPARE_STATE_INFO.ls_readable_scn_ >= UNKOWN_STATE_INFO.ls_readable_scn_) {                \
    RES_STATE_INFO.tx_state_ = ObTxState::INIT;                                                   \
    RES_STATE_INFO.trans_version_.set_invalid();                                                  \
    RES_STATE_INFO.ls_readable_scn_ = PREPARE_STATE_INFO.ls_readable_scn_;                        \
    RES_STATE_INFO.max_applied_snapshot_ = min_snapshot;                                          \
  } else if (PREPARE_STATE_INFO.tx_state_ < ObTxState::PREPARE                                    \
             || (PREPARE_STATE_INFO.tx_state_ == ObTxState::PREPARE                               \
                 && min_snapshot <= PREPARE_STATE_INFO.trans_version_)) {                          \
    RES_STATE_INFO.tx_state_ = ObTxState::INIT;                                                   \
    RES_STATE_INFO.trans_version_.set_invalid();                                                  \
    RES_STATE_INFO.ls_readable_scn_ = PREPARE_STATE_INFO.ls_readable_scn_;                        \
    RES_STATE_INFO.max_applied_snapshot_ = min_snapshot;                                          \
  } else if (PREPARE_STATE_INFO.tx_state_ == ObTxState::PREPARE                                   \
             && min_snapshot > PREPARE_STATE_INFO.trans_version_) {                               \
    share::SCN max_readable_scn =                                                                 \
        share::SCN::max(PREPARE_STATE_INFO.ls_readable_scn_, UNKOWN_STATE_INFO.ls_readable_scn_); \
    max_readable_scn = share::SCN::max(max_readable_scn, PREPARE_STATE_INFO.trans_version_);      \
    RES_STATE_INFO.tx_state_ = ObTxState::UNKNOWN;                                                \
    RES_STATE_INFO.trans_version_.set_invalid();                                                  \
    RES_STATE_INFO.ls_readable_scn_ = max_readable_scn;                                           \
    RES_STATE_INFO.max_applied_snapshot_ = min_snapshot;                                          \
  } else {                                                                                        \
    ret = OB_ERR_UNEXPECTED;                                                                      \
    TRANS_LOG(ERROR, "Unexpected local sby state info", K(ret), K(local_sby_info),                \
              K(parts_sby_info));                                                                 \
  }

int ObPartTransCtx::handle_sby_unknown_state_info(const ObTxSbyStateInfo &parts_sby_info,
                                                  ObTxSbyStateInfo &local_sby_info)
{
  int ret = OB_SUCCESS;

  ObTxSbyStateInfo unknown_sby_info;
  share::SCN min_snapshot =
      share::SCN::min(parts_sby_info.max_applied_snapshot_, local_sby_info.max_applied_snapshot_);

  if (!parts_sby_info.is_valid() || !local_sby_info.is_valid()
      || (parts_sby_info.tx_state_ != ObTxState::UNKNOWN
          && local_sby_info.tx_state_ != ObTxState::UNKNOWN)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(parts_sby_info), K(local_sby_info));
  } else if (local_sby_info.max_applied_snapshot_ > parts_sby_info.ls_readable_scn_
             || local_sby_info.max_applied_snapshot_ > local_sby_info.ls_readable_scn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected replica with the smaller ls_readable_scn", K(ret),
              K(local_sby_info), K(parts_sby_info));
  } else if (local_sby_info.is_confirmed()) {
    // do nothing
  } else {
    if (parts_sby_info.tx_state_ == ObTxState::UNKNOWN
        && local_sby_info.tx_state_ == ObTxState::UNKNOWN) {
      local_sby_info.ls_readable_scn_ =
          share::SCN::max(parts_sby_info.ls_readable_scn_, local_sby_info.ls_readable_scn_);
      local_sby_info.max_applied_snapshot_ = min_snapshot;
    } else if (parts_sby_info.tx_state_ == ObTxState::UNKNOWN) {
      HANDLE_UNKNOWN_SBY_TX_STATE(parts_sby_info, local_sby_info, local_sby_info);
    } else if (local_sby_info.tx_state_ == ObTxState::UNKNOWN) {
      if (parts_sby_info.is_confirmed()) {
        local_sby_info.try_to_update(parts_sby_info, true);
        local_sby_info.max_applied_snapshot_ = min_snapshot;
      } else {
        HANDLE_UNKNOWN_SBY_TX_STATE(local_sby_info, parts_sby_info, local_sby_info);
      }
    }

    local_sby_info.flag_.flag_bit_.infer_by_root_ = true;
  }

  return ret;
}

} // namespace transaction
} // namespace oceanbase
