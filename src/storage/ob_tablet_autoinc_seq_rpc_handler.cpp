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

#define USING_LOG_PREFIX STORAGE

#include "ob_tablet_autoinc_seq_rpc_handler.h"
#include "logservice/ob_log_service.h"
#include "storage/multi_data_source/mds_ctx.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

// ObSyncTabletSeqReplayExecutor
ObSyncTabletSeqReplayExecutor::ObSyncTabletSeqReplayExecutor()
  : ObTabletReplayExecutor(), seq_(0), is_tablet_creating_(false), scn_()
{
}

int ObSyncTabletSeqReplayExecutor::init(
    const uint64_t autoinc_seq,
    const bool is_tablet_creating,
    const SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(autoinc_seq == 0)
          || OB_UNLIKELY(!replay_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(autoinc_seq), K(replay_scn), K(ret));
  } else {
    seq_ = autoinc_seq;
    is_tablet_creating_ = is_tablet_creating;
    scn_ = replay_scn;
    is_inited_ = true;
  }

  return ret;
}

int ObSyncTabletSeqReplayExecutor::do_replay_(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = handle.get_obj();
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(handle));
  } else {
    // replay to mds table
    ObArenaAllocator allocator;
    ObTabletAutoincSeq curr_autoinc_seq;
    uint64_t curr_autoinc_seq_value;
    bool need_replay = true;
    if (CLUSTER_CURRENT_VERSION >= CLUSTER_VERSION_4_3_2_0) {
      // just replay for multi-vesion mds
    } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_autoinc_seq(allocator, share::SCN::max_scn(), curr_autoinc_seq))) {
      LOG_WARN("fail to get latest autoinc seq", K(ret), KPC(tablet));
    } else if (OB_FAIL(curr_autoinc_seq.get_autoinc_seq_value(curr_autoinc_seq_value))) {
      LOG_WARN("failed to get autoinc seq value", K(ret), KPC(tablet), K(curr_autoinc_seq));
    } else if (seq_ <= curr_autoinc_seq_value) {
      need_replay = false;
    }

    if (OB_SUCC(ret) && need_replay) {
      if (OB_FAIL(curr_autoinc_seq.set_autoinc_seq_value(allocator, seq_))) {
        LOG_WARN("failed to set autoinc seq value", K(ret), K(seq_), K(curr_autoinc_seq));
      } else {
        mds::MdsWriter mds_writer(mds::WriterType::AUTO_INC_SEQ, static_cast<int64_t>(seq_));
        mds::MdsCtx mds_ctx(mds_writer);
        if (OB_FAIL(replay_to_mds_table_(handle, curr_autoinc_seq, mds_ctx, scn_))) {
          LOG_WARN("failed to save autoinc seq", K(ret), K(curr_autoinc_seq));
        } else {
          mds_ctx.single_log_commit(scn_, scn_);
        }
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqReplayExecutor::init(mds::BufferCtx &user_ctx, const share::SCN &scn, const ObTabletAutoincSeq &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "tablet autoinc replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    data_ = &data;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAutoincSeqReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);
  if (OB_FAIL(replay_to_mds_table_(tablet_handle, *data_, user_ctx, scn_))) {
    TRANS_LOG(WARN, "failed to replay to tablet", K(ret));
  }
  return ret;
}

// ObTabletAutoincSeqRpcHandler
ObTabletAutoincSeqRpcHandler::ObTabletAutoincSeqRpcHandler()
  : is_inited_(false), bucket_lock_()
{
}

ObTabletAutoincSeqRpcHandler::~ObTabletAutoincSeqRpcHandler()
{
}

ObTabletAutoincSeqRpcHandler &ObTabletAutoincSeqRpcHandler::get_instance()
{
  static ObTabletAutoincSeqRpcHandler autoinc_rpc_handler;
  return autoinc_rpc_handler;
}

int ObTabletAutoincSeqRpcHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(bucket_lock_.init(BUCKET_LOCK_BUCKET_CNT))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::fetch_tablet_autoinc_seq_cache(
    const ObFetchTabletSeqArg &arg,
    ObFetchTabletSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      ObTabletHandle tablet_handle;
      ObTabletAutoincInterval autoinc_interval;
      const ObTabletID &tablet_id = arg.tablet_id_;
      int64_t proposal_id = -1;
      ObTabletCreateDeleteMdsUserData user_data;
      mds::MdsWriter writer;
      mds::TwoPhaseCommitState trans_stat;
      share::SCN trans_version;
      bool is_committed = false;
      ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follower received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle, THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : obrpc::ObRpcProxy::MAX_RPC_TIMEOUT))) {
        LOG_WARN("failed to get tablet", KR(ret), K(arg));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
        LOG_WARN("fail to get latest tablet status", K(ret), K(arg));
      } else if (OB_UNLIKELY(trans_stat != mds::TwoPhaseCommitState::ON_COMMIT)) {
        ret = OB_EAGAIN;
        LOG_WARN("tablet status not committed, maybe transfer or split start trans", K(ret), K(user_data), K(trans_stat), K(writer));
      // TODO(lihongqin.lhq): fetch from split dst to avoid retry
      } else if (OB_FAIL(tablet_handle.get_obj()->fetch_tablet_autoinc_seq_cache(
          arg.cache_size_, autoinc_interval))) {
        LOG_WARN("failed to fetch tablet autoinc seq on tablet", K(ret), K(tablet_id));
      } else {
        res.cache_interval_ = autoinc_interval;
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::batch_get_tablet_autoinc_seq(
    const obrpc::ObBatchGetTabletAutoincSeqArg &arg,
    obrpc::ObBatchGetTabletAutoincSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      int64_t proposal_id = -1;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follower received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(res.autoinc_params_.reserve(arg.src_tablet_ids_.count()))) {
        LOG_WARN("failed to reserve autoinc param", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.src_tablet_ids_.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          const ObTabletID &src_tablet_id = arg.src_tablet_ids_.at(i);
          ObTabletHandle tablet_handle;
          share::ObMigrateTabletAutoincSeqParam autoinc_param;
          ObArenaAllocator allocator("BatchGetSeq");
          autoinc_param.src_tablet_id_ = src_tablet_id;
          autoinc_param.dest_tablet_id_ = arg.dest_tablet_ids_.at(i);
          ObBucketHashRLockGuard lock_guard(bucket_lock_, src_tablet_id.hash());
          if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(src_tablet_id, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(tmp_ret), K(src_tablet_id));
          } else {
            ObTabletAutoincSeq autoinc_seq;
            if (OB_TMP_FAIL(tablet_handle.get_obj()->get_autoinc_seq(autoinc_seq, allocator))) {
              LOG_WARN("fail to get latest autoinc seq", K(ret));
            } else if (OB_TMP_FAIL(autoinc_seq.get_autoinc_seq_value(autoinc_param.autoinc_seq_))) {
              LOG_WARN("failed to get autoinc seq value", K(tmp_ret));
            }
          }
          autoinc_param.ret_code_ = tmp_ret;
          if (OB_FAIL(res.autoinc_params_.push_back(autoinc_param))) {
            LOG_WARN("failed to push autoinc param", K(ret), K(autoinc_param));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::batch_set_tablet_autoinc_seq(
    const obrpc::ObBatchSetTabletAutoincSeqArg &arg,
    obrpc::ObBatchSetTabletAutoincSeqRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(res.autoinc_params_.assign(arg.autoinc_params_))) {
    LOG_WARN("failed to assign autoinc params", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      share::ObLSID ls_id = arg.ls_id_;
      ObRole role = common::INVALID_ROLE;
      int64_t proposal_id = -1;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follower received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls not leader", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < res.autoinc_params_.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          ObTabletHandle tablet_handle;
          share::ObMigrateTabletAutoincSeqParam &autoinc_param = res.autoinc_params_.at(i);
          ObBucketHashWLockGuard lock_guard(bucket_lock_, autoinc_param.dest_tablet_id_.hash());
          if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(autoinc_param.dest_tablet_id_, tablet_handle,
              ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
            LOG_WARN("failed to get tablet", K(tmp_ret), K(autoinc_param));
          } else if (OB_TMP_FAIL(tablet_handle.get_obj()->update_tablet_autoinc_seq(autoinc_param.autoinc_seq_, arg.is_tablet_creating_))) {
            LOG_WARN("failed to update tablet autoinc seq", K(tmp_ret), K(autoinc_param));
          }
          autoinc_param.ret_code_ = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::replay_update_tablet_autoinc_seq(
    const ObLS *ls,
    const ObTabletID &tablet_id,
    const uint64_t autoinc_seq,
    const bool is_tablet_creating,
    const share::SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls == nullptr || !tablet_id.is_valid() || autoinc_seq == 0 || !replay_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(autoinc_seq), K(replay_scn));
  } else {
    ObTabletHandle tablet_handle;
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());
    ObSyncTabletSeqReplayExecutor replay_executor;
    if (OB_FAIL(replay_executor.init(autoinc_seq, is_tablet_creating, replay_scn))) {
      LOG_WARN("failed to init tablet auto inc sequence replay executor", K(ret), K(autoinc_seq), K(is_tablet_creating), K(replay_scn));
    } else if (OB_FAIL(replay_executor.execute(replay_scn, ls->get_ls_id(), tablet_id))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("tablet may be deleted, skip this log", K(ret), K(tablet_id), K(replay_scn));
        ret = OB_SUCCESS;
      } else if (OB_NO_NEED_UPDATE == ret) {
        LOG_INFO("no need replay, skip this log", K(ret), K(tablet_id), K(replay_scn));
        ret = OB_SUCCESS;
      } else if (OB_EAGAIN == ret) {
        // retry replay again
      } else {
        LOG_WARN("fail to replay get tablet, retry again", K(ret), K(tablet_id), K(replay_scn));
        ret = OB_EAGAIN;
      }
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::batch_set_tablet_autoinc_seq_in_trans(
    ObLS &ls,
    const obrpc::ObBatchSetTabletAutoincSeqArg &arg,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = arg.ls_id_;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SetAutoSeq"));
  if (OB_UNLIKELY(ls_id != ls.get_ls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls", K(ret), K(ls_id), K(ls.get_ls_id()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.autoinc_params_.count(); i++) {
    allocator.reuse();
    const ObTabletID &tablet_id = arg.autoinc_params_.at(i).dest_tablet_id_;
    const uint64_t autoinc_seq = arg.autoinc_params_.at(i).autoinc_seq_;
    ObTabletAutoincSeq data;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(data.set_autoinc_seq_value(allocator, autoinc_seq))) {
      LOG_WARN("failed to set autoinc seq value", K(ret), K(ls_id), K(tablet_id), K(autoinc_seq));
    } else if (OB_FAIL(set_tablet_autoinc_seq_in_trans(ls, tablet_id, data, replay_scn, ctx))) {
      LOG_WARN("failed to set mds", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObTabletAutoincSeqRpcHandler::set_tablet_autoinc_seq_in_trans(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const ObTabletAutoincSeq &data,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls.get_ls_id();
  if (!replay_scn.is_valid()) {
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret));
    } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::set(data, user_ctx, 0/*lock_timeout_us*/))) {
      LOG_WARN("failed to set mds data", K(ret));
    }
  } else {
    ObTabletAutoincSeqReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, replay_scn, data))) {
      LOG_WARN("failed to init replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(replay_scn, ls_id, tablet_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay mds", K(ret));
      }
    }
  }
  return ret;
}

}
}
