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
#include "rootserver/ob_root_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_handle.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

// ObSyncTabletSeqReplayExecutor
ObSyncTabletSeqReplayExecutor::ObSyncTabletSeqReplayExecutor()
  : ObTabletReplayExecutor(), seq_(0), scn_()
{
}

int ObSyncTabletSeqReplayExecutor::init(
    const uint64_t autoinc_seq,
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
    if (OB_FAIL(tablet->get_autoinc_seq(allocator, share::SCN::max_scn(), curr_autoinc_seq))) {
      LOG_WARN("fail to get latest autoinc seq", K(ret), KPC(tablet));
    } else if (OB_FAIL(curr_autoinc_seq.get_autoinc_seq_value(curr_autoinc_seq_value))) {
      LOG_WARN("failed to get autoinc seq value", K(ret), KPC(tablet), K(curr_autoinc_seq));
    } else if (seq_ > curr_autoinc_seq_value) {
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
      ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls_id, role, proposal_id))) {
        LOG_WARN("get palf role failed", K(ret));
      } else if (!is_strong_leader(role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
        LOG_WARN("failed to get tablet", KR(ret), K(arg));
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
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
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
            if (OB_TMP_FAIL(tablet_handle.get_obj()->get_autoinc_seq(allocator, share::SCN::max_scn(), autoinc_seq))) {
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
        LOG_WARN("follwer received FetchTabletsSeq rpc", K(ret), K(ls_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(ls_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < res.autoinc_params_.count(); i++) {
          int tmp_ret = OB_SUCCESS;
          ObTabletHandle tablet_handle;
          share::ObMigrateTabletAutoincSeqParam &autoinc_param = res.autoinc_params_.at(i);
          ObBucketHashWLockGuard lock_guard(bucket_lock_, autoinc_param.dest_tablet_id_.hash());
          if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(autoinc_param.dest_tablet_id_, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(tmp_ret), K(autoinc_param));
          } else if (OB_TMP_FAIL(tablet_handle.get_obj()->update_tablet_autoinc_seq(autoinc_param.autoinc_seq_))) {
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
    if (OB_FAIL(replay_executor.init(autoinc_seq, replay_scn))) {
      LOG_WARN("failed to init tablet auto inc sequence replay executor", K(ret), K(autoinc_seq), K(replay_scn));
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

}
}
