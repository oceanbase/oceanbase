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

#include "storage/tablet/ob_tablet_create_delete_helper.h"

#include "lib/ob_abort.h"
#include "lib/worker.h"
#include "lib/utility/utility.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_id_set.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"
#include "observer/omt/ob_tenant_config_mgr.h"

#define USING_LOG_PREFIX STORAGE

#define PRINT_CREATE_ARG(arg) (ObSimpleBatchCreateTabletArg(arg))

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{
ObTabletCreateInfo::ObTabletCreateInfo()
  : create_data_tablet_(false),
    data_tablet_id_(),
    index_tablet_id_array_(),
    lob_meta_tablet_id_(),
    lob_piece_tablet_id_()
{
}

ObTabletCreateInfo::ObTabletCreateInfo(const ObTabletCreateInfo &other)
  : create_data_tablet_(other.create_data_tablet_),
    data_tablet_id_(other.data_tablet_id_),
    index_tablet_id_array_(other.index_tablet_id_array_),
    lob_meta_tablet_id_(other.lob_meta_tablet_id_),
    lob_piece_tablet_id_(other.lob_piece_tablet_id_)
{
}

ObTabletCreateInfo &ObTabletCreateInfo::operator=(const ObTabletCreateInfo &other)
{
  if (this != &other) {
    create_data_tablet_ = other.create_data_tablet_;
    data_tablet_id_ = other.data_tablet_id_;
    index_tablet_id_array_ = other.index_tablet_id_array_;
    lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
  }
  return *this;
}


ObTabletCreateDeleteHelper::ObTabletCreateDeleteHelper(
    ObLS &ls,
    ObTabletIDSet &tablet_id_set)
  : ls_(ls),
    tablet_id_set_(tablet_id_set)
{
}

int ObTabletCreateDeleteHelper::prepare_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  const ObTransID &tx_id = trans_flags.tx_id_;
  const SCN scn = trans_flags.scn_;
  const bool is_clog_replaying = trans_flags.for_replay_;
  const int64_t create_begin_ts = ObTimeUtility::current_monotonic_time();
  ObTabletBindingPrepareCtx binding_ctx;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(PRINT_CREATE_ARG(arg)), K(ls_id));
  } else if (OB_FAIL(ObTabletBindingHelper::lock_tablet_binding_for_create(arg, ls_, trans_flags, binding_ctx))) {
    LOG_WARN("failed to lock tablet binding", K(ret));
  } else if (is_clog_replaying) {
    if (OB_FAIL(replay_prepare_create_tablets(arg, trans_flags))) {
      LOG_WARN("failed to replay prepare create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    }
  } else {
    // NOT in clog replaying procedure
    if (OB_FAIL(check_create_new_tablets(arg))) {
      LOG_WARN("failed to check crate new tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    } else if (OB_FAIL(do_prepare_create_tablets(arg, trans_flags))) {
      LOG_WARN("failed to do prepare create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    } else {
      LOG_INFO("succeeded to prepare create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    }
  }

  if (OB_FAIL(ret) && !is_clog_replaying) {
    ObTabletBindingHelper::rollback_lock_tablet_binding_for_create(arg, ls_, trans_flags, binding_ctx);
  }

  if (OB_SUCC(ret)) {
    const int64_t create_cost_time = ObTimeUtility::current_monotonic_time() - create_begin_ts;
    LOG_INFO("prepare create cost time", K(ret), K(trans_flags), K(create_begin_ts), K(create_cost_time));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::replay_prepare_create_tablets(
      const ObBatchCreateTabletArg &arg,
      const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  SCN scn = trans_flags.scn_;
  const ObBatchCreateTabletArg *final_arg = &arg;
  ObBatchCreateTabletArg new_arg;
  ObSArray<ObTabletID> existed_tablet_id_array;
  NonLockedHashSet existed_tablet_id_set;
  bool need_create = true;

  if (OB_UNLIKELY(!arg.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(PRINT_CREATE_ARG(arg)), K(scn));
  } else if (OB_FAIL(existed_tablet_id_set.create(arg.get_tablet_count()))) {
    LOG_WARN("failed to init existed tablet id set", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_FAIL(get_all_existed_tablets(arg, scn, existed_tablet_id_array, existed_tablet_id_set))) {
    LOG_WARN("failed to get all existed tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(scn));
  } else if (!existed_tablet_id_set.empty()) {
    if (OB_FAIL(build_batch_create_tablet_arg(arg, existed_tablet_id_set, new_arg))) {
      LOG_WARN("failed to build new batch create tablet arg", K(ret),
          K(PRINT_CREATE_ARG(arg)), K(existed_tablet_id_set));
    } else if (new_arg.get_tablet_count() == 0) {
      // all tablet ids exist, no need to create, do nothing
      need_create = false;
    } else if (OB_UNLIKELY(!new_arg.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new arg is invalid", K(ret), K(PRINT_CREATE_ARG(new_arg)));
    } else {
      final_arg = &new_arg;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (trans_flags.for_replay_
      && OB_FAIL(handle_special_tablets_for_replay(existed_tablet_id_array, trans_flags))) {
    LOG_WARN("failed to handle special tablets for replay", K(ret), K(existed_tablet_id_array), K(trans_flags));
  } else if (!need_create) {
    LOG_INFO("all tablets exist, no need to create", K(ret), K(PRINT_CREATE_ARG(arg)), KPC(final_arg));
    // MUST check data tablet existence because skipping early replaying auxiliary tablets creation
    // might omit updating attributes on data tablet
    if (OB_FAIL(ensure_skip_create_all_tablets_safe(*final_arg, scn))) {
      LOG_WARN("failed to replay check tablet", K(ret), K(scn), K(PRINT_CREATE_ARG(*final_arg)));
    }
  } else if (OB_FAIL(do_prepare_create_tablets(*final_arg, trans_flags))) {
    LOG_WARN("failed to do prepare create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(*final_arg)));
  } else {
    LOG_INFO("succeeded to replay prepare create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(*final_arg)));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_prepare_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletCreateInfo> tablet_create_info_array;

  if (OB_FAIL(verify_tablets_absence(arg, tablet_create_info_array))) {
    LOG_WARN("failed to verify tablets", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else {
    if (OB_FAIL(batch_create_tablets(arg, trans_flags))) {
      LOG_WARN("failed to do batch create tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(trans_flags));
    } else if (OB_FAIL(record_tablet_id(tablet_create_info_array))) {
      LOG_WARN("failed to record tablet id", K(ret));
    }

    // roll back operation
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(roll_back_remove_tablets(arg, trans_flags))) {
        LOG_WARN("failed to roll back remove tablets", K(tmp_ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)), K(lbt()));
        ob_usleep(1000 * 1000);
        ob_abort(); // roll back operation should NOT fail
      }
    }
  }

  if (OB_FAIL(ret) && OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret = OB_EAGAIN;
    LOG_WARN("failed due to 4013, has rolled back operations", K(ret), K(trans_flags), K(arg));
  }

  if (OB_SUCC(ret)) {
    print_multi_data_for_create_tablet(tablet_create_info_array);
  }

  return ret;
}

int ObTabletCreateDeleteHelper::record_tablet_id(
    const ObIArray<ObTabletCreateInfo> &tablet_create_info_array)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < tablet_create_info_array.count(); ++i) {
    const ObTabletCreateInfo &info = tablet_create_info_array.at(i);
    if (info.create_data_tablet_) {
      if (OB_FAIL(tablet_id_set_.set(info.data_tablet_id_))) {
        LOG_WARN("fail to set tablet id set", K(ret), "tablet_id", info.data_tablet_id_);
      }
    }

    for (int64_t j = 0; j < info.index_tablet_id_array_.count(); ++j) {
      const ObTabletID &tablet_id = info.index_tablet_id_array_.at(j);
      if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
        LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
      }
    }

    if (info.lob_meta_tablet_id_.is_valid()) {
      const ObTabletID &tablet_id = info.lob_meta_tablet_id_;
      if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
        LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
      }
    }

    if (info.lob_piece_tablet_id_.is_valid()) {
      const ObTabletID &tablet_id = info.lob_piece_tablet_id_;
      if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
        LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::handle_special_tablets_for_replay(
    const ObIArray<ObTabletID> &existed_tablet_id_array,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;
  ObTabletTxMultiSourceDataUnit tx_data;
  const ObLSID &ls_id = ls_.get_ls_id();
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  if (OB_UNLIKELY(!trans_flags.for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only handle special tablets for replay", K(ret), K(trans_flags));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < existed_tablet_id_array.count(); ++i) {
      const ObTabletID &tablet_id = existed_tablet_id_array.at(i);
      key.tablet_id_ = tablet_id;
      tx_data.reset();
      bool tx_data_is_valid = false;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      } else if (OB_FAIL(tablet_handle.get_obj()->check_tx_data(tx_data_is_valid))) {
        LOG_WARN("failed to check tx data", K(ret));
      } else if (OB_UNLIKELY(tx_data_is_valid)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tx data should not be valid", K(ret), K(key));
      } else {
        int tmp_ret = OB_SUCCESS;
        tx_data.tx_id_ = trans_flags.tx_id_;
        tx_data.tx_scn_ = trans_flags.scn_;
        tx_data.tablet_status_ = ObTabletStatus::CREATING;
        const MemtableRefOp ref_op = MemtableRefOp::NONE;

        if (OB_FAIL(tablet_handle.get_obj()->set_tx_data(tx_data, trans_flags.for_replay_, ref_op))) {
          LOG_WARN("failed to set tx data", K(ret), K(tx_data), K(trans_flags), K(ref_op));
        } else if (OB_FAIL(t3m->insert_pinned_tablet(key))) {
          LOG_WARN("failed to insert in tx tablet", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::set_scn(
    const ObIArray<ObTabletCreateInfo> &tablet_create_info_array,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const ObLSID &ls_id = ls_.get_ls_id();
  ObTabletMapKey key;
  key.ls_id_ = ls_id;
  SCN scn = trans_flags.scn_;
  const int64_t tx_id = trans_flags.tx_id_;
  const bool for_replay = false;

  for (int64_t i = 0; i < tablet_create_info_array.count(); ++i) {
    const ObTabletCreateInfo &info = tablet_create_info_array.at(i);
    if (info.create_data_tablet_) {
      key.tablet_id_ = info.data_tablet_id_;
      if (OB_TMP_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(key));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_TMP_FAIL(tablet->set_tx_scn(tx_id, scn, for_replay))) {
        LOG_WARN("failed to set tx log ts", K(tmp_ret), K(key), K(scn));
      } else if (OB_TMP_FAIL(tablet->tablet_meta_.update_create_scn(scn))) {
        LOG_WARN("failed to update create scn", K(tmp_ret), K(scn));
      }
    }
    if (OB_SUCC(ret) && OB_TMP_FAIL(tmp_ret)) {
      ret = tmp_ret;
    }

    if (info.lob_meta_tablet_id_.is_valid()) {
      key.tablet_id_ = info.lob_meta_tablet_id_;
      if (OB_TMP_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(key));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_TMP_FAIL(tablet->set_tx_scn(tx_id, scn, for_replay))) {
        LOG_WARN("failed to set tx log ts", K(tmp_ret), K(key), K(scn));
      } else if (OB_TMP_FAIL(tablet->tablet_meta_.update_create_scn(scn))) {
        LOG_WARN("failed to update create scn", K(tmp_ret), K(scn));
      }
    }
    if (OB_SUCC(ret) && OB_TMP_FAIL(tmp_ret)) {
      ret = tmp_ret;
    }

    if (info.lob_piece_tablet_id_.is_valid()) {
      key.tablet_id_ = info.lob_piece_tablet_id_;
      if (OB_TMP_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(key));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_TMP_FAIL(tablet->set_tx_scn(tx_id, scn, for_replay))) {
        LOG_WARN("failed to set tx log ts", K(tmp_ret), K(key), K(scn));
      } else if (OB_TMP_FAIL(tablet->tablet_meta_.update_create_scn(scn))) {
        LOG_WARN("failed to update create scn", K(tmp_ret), K(scn));
      }
    }
    if (OB_SUCC(ret) && OB_TMP_FAIL(tmp_ret)) {
      ret = tmp_ret;
    }

    for (int64_t j = 0; j < info.index_tablet_id_array_.count(); ++j) {
      const ObTabletID &index_tablet_id = info.index_tablet_id_array_.at(j);
      key.tablet_id_ = index_tablet_id;
      if (OB_TMP_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(key));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_TMP_FAIL(tablet->set_tx_scn(tx_id, scn, for_replay))) {
        LOG_WARN("failed to set tx log ts", K(tmp_ret), K(key), K(scn));
      } else if (OB_TMP_FAIL(tablet->tablet_meta_.update_create_scn(scn))) {
        LOG_WARN("failed to update create scn", K(tmp_ret), K(scn));
      }
      if (OB_SUCC(ret) && OB_TMP_FAIL(tmp_ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::print_multi_data_for_create_tablet(
    const ObIArray<ObTabletCreateInfo> &tablet_create_info_array)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  const ObLSID &ls_id = ls_.get_ls_id();
  ObTabletMapKey key;
  key.ls_id_ = ls_id;
  ObTabletTxMultiSourceDataUnit tx_data;

  for (int64_t i = 0; i < tablet_create_info_array.count(); ++i) {
    const ObTabletCreateInfo &info = tablet_create_info_array.at(i);
    if (info.create_data_tablet_) {
      key.tablet_id_ = info.data_tablet_id_;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet does not exist", K(key));
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get_tx_data", K(ret), K(key));
      } else {
        LOG_INFO("create tablet tx_data", K(tx_data), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
      }
    }

    for (int64_t j = 0; j < info.index_tablet_id_array_.count(); ++j) {
      const ObTabletID &index_tablet_id = info.index_tablet_id_array_.at(j);
      key.tablet_id_ = index_tablet_id;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet does not exist", K(key));
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get_tx_data", K(ret), K(key));
      } else {
        LOG_INFO("create index tablet tx_data", K(tx_data), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
      }
    }

    if (info.lob_meta_tablet_id_.is_valid()) {
      key.tablet_id_ = info.lob_meta_tablet_id_;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet does not exist", K(key));
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get_tx_data", K(ret), K(key));
      } else {
        LOG_INFO("create lob meta tablet tx_data", K(tx_data), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
      }
    }

    if (info.lob_piece_tablet_id_.is_valid()) {
      key.tablet_id_ = info.lob_piece_tablet_id_;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet does not exist", K(key));
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get_tx_data", K(ret), K(key));
      } else {
        LOG_INFO("create lob piece tablet tx_data", K(tx_data), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::print_multi_data_for_remove_tablet(
    const ObBatchRemoveTabletArg &arg)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = arg.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tablet does not exist", "ls_id", arg.id_, K(tablet_id));
      } else {
        LOG_WARN("failed to get tablet", K(ret), "ls_id", arg.id_, K(tablet_id));
      }
    } else {
      ObTabletTxMultiSourceDataUnit tx_data;
      if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get tx data", K(ret), K(tablet_handle));
      } else {
        LOG_INFO("remove tablet tx_data", K(tx_data), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::redo_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  SCN scn = trans_flags.scn_;
  const bool is_clog_replaying = trans_flags.for_replay_;

  if (OB_UNLIKELY(!arg.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected arg", K(ret), K(PRINT_CREATE_ARG(arg)), K(ls_id));
  } else if (is_clog_replaying) {
    LOG_INFO("in clog replaying procedure, do nothing while redo log callback", K(ret));
  } else {
    bool is_valid = true;
    ObSArray<ObTabletCreateInfo> tablet_create_info_array;
    if (OB_FAIL(check_tablet_existence(arg, is_valid))) {
      LOG_ERROR("failed to check tablet existence", K(ret), K(PRINT_CREATE_ARG(arg)));
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error, tablet does not exist", K(ret));
    } else if (OB_FAIL(build_tablet_create_info(arg, tablet_create_info_array))) {
      LOG_ERROR("failed to build tablet create info", K(ret));
    } else if (OB_FAIL(set_scn(tablet_create_info_array, trans_flags))) {
      LOG_ERROR("failed to set log ts", K(ret), K(tablet_create_info_array), K(scn));
    } else if (FALSE_IT(print_multi_data_for_create_tablet(tablet_create_info_array))) {
    } else if (OB_FAIL(ObTabletBindingHelper::set_scn_for_create(arg, ls_, trans_flags))) {
      LOG_WARN("failed to set log ts for create", K(ret));
    } else {
      LOG_INFO("succeeded to redo create tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::commit_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags,
    ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(PRINT_CREATE_ARG(arg)), K(ls_id));
  } else {
    bool is_valid = true;
    ObSArray<ObTabletCreateInfo> tablet_create_info_array;
    if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_for_create(arg, ls_, trans_flags))) {
      LOG_ERROR("failed to modify tablet binding", K(ret), K(trans_flags));
    } else if (OB_FAIL(build_tablet_create_info(arg, tablet_create_info_array))) {
      LOG_WARN("failed to build tablet create info", K(ret), K(PRINT_CREATE_ARG(arg)));
    } else if (OB_FAIL(do_commit_create_tablets(arg, trans_flags, tablet_id_array))) {
      LOG_WARN("failed to commit create tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(trans_flags));
    } else if (FALSE_IT(print_multi_data_for_create_tablet(tablet_create_info_array))) {
    } else if (OB_FAIL(ObTabletBindingHelper::unlock_tablet_binding_for_create(arg, ls_, trans_flags))) {
      LOG_ERROR("failed to unlock tablet binding", K(ret), K(trans_flags));
    } else {
      LOG_INFO("succeeded to commit create tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(trans_flags));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_commit_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags,
    ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
      const ObTabletID &tablet_id = info.tablet_ids_.at(j);
      key.tablet_id_ = tablet_id;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          if (trans_flags.for_replay_) {
            ret = OB_SUCCESS;
            LOG_INFO("tablet does not exist, maybe already deleted, skip commit", K(ret), K(key));
          } else {
            LOG_WARN("unexepected error, tablet does not exist", K(ret), K(key));
          }
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(do_commit_create_tablet(tablet_id, trans_flags))) {
        LOG_WARN("failed to do commit create tablet", K(ret), K(tablet_id), K(trans_flags));
      } else if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_commit_create_tablet(
    const ObTabletID &tablet_id,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletMapKey key(ls_.get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;
  ObTabletTxMultiSourceDataUnit tx_data;

  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), K(tablet_handle));
  } else if (trans_flags.for_replay_ && trans_flags.scn_ <= tx_data.tx_scn_) {
    // replaying procedure, clog ts is smaller than tx log ts, just skip
    LOG_INFO("skip commit create tablet", K(ret), K(key), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(!trans_flags.for_replay_ && trans_flags.scn_ <= tx_data.tx_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts is smaller than tx log ts", K(ret), K(key), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(trans_flags.tx_id_ != tx_data.tx_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx id does not equal", K(ret), K(key), K(trans_flags), K(tx_data));
    print_memtables_for_table(tablet_handle);
  } else if (OB_UNLIKELY(ObTabletStatus::CREATING != tx_data.tablet_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is not CREATING", K(ret), K(key), K(trans_flags), K(tx_data));
  } else {
    if (OB_FAIL(set_tablet_final_status(tablet_handle, ObTabletStatus::NORMAL,
        trans_flags.scn_, trans_flags.scn_, trans_flags.for_replay_))) {
      LOG_WARN("failed to set tablet status to NORMAL", K(ret), K(tablet_handle),
          K(trans_flags.scn_), K(trans_flags));
    } else if (OB_FAIL(t3m->erase_pinned_tablet(key))) {
      LOG_ERROR("failed to erase tablet handle", K(ret), K(key));
      ob_usleep(1000 * 1000);
      ob_abort();
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::abort_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (OB_FAIL(ObTabletBindingHelper::fix_binding_info_for_create_tablets(arg, ls_, trans_flags))) {
    LOG_WARN("failed to fix_binding_info_for_create_tablets", K(ret), K(arg), K(trans_flags));
  } else if (OB_FAIL(do_abort_create_tablets(arg, trans_flags))) {
    LOG_WARN("failed to do abort create tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(trans_flags));
  } else if (OB_FAIL(ObTabletBindingHelper::unlock_tablet_binding_for_create(arg, ls_, trans_flags))) {
    LOG_WARN("failed to unlock tablet binding", K(ret), K(trans_flags));
  } else {
    LOG_INFO("succeeded to abort create tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(trans_flags));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_abort_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = ls_.get_ls_id();

  if (!trans_flags.is_redo_synced()) {
    // on redo cb has not been called
    // just remove tablets directly
    if (OB_FAIL(roll_back_remove_tablets(arg, trans_flags))) {
      LOG_WARN("failed to remove tablets", K(ret), K(trans_flags), K(PRINT_CREATE_ARG(arg)));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
      const ObCreateTabletInfo &info = arg.tablets_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
        const ObTabletID &tablet_id = info.tablet_ids_.at(j);
        key.tablet_id_ = tablet_id;
        if (OB_FAIL(get_tablet(key, tablet_handle))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("tablet does not exist, maybe already deleted", K(ret), K(key));
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(key));
          }
        } else if (OB_FAIL(do_abort_create_tablet(tablet_handle, trans_flags))) {
          LOG_WARN("failed to do abort create tablet", K(ret), K(tablet_handle), K(trans_flags));
        }
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_tx_end_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  // modify tablet status_info
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
      bool skip = false;
      const ObTabletID &tablet_id = info.tablet_ids_.at(j);
      if (trans_flags.for_replay_
          && OB_FAIL(ObTabletBindingHelper::check_skip_tx_end(tablet_id, ls_, trans_flags, skip))) {
        LOG_WARN("fail to check_skip_tx_end_create", K(ret), K(arg));
      } else if (skip) {
        LOG_INFO("skip tx_end for replay_create", K(tablet_id), K(trans_flags));
      } else if (OB_FAIL(prepare_data_for_tablet_status(tablet_id, ls_, trans_flags))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet does not exist, maybe fail to create", K(ret), K(tablet_id));
        } else {
          LOG_WARN("fail to prepare_data", K(ret), K(tablet_id));
        }
      }
    }
  }

  // modify data_tablet status_info and binding_info
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    bool need_modify = false;
    if (is_pure_hidden_tablets(info)) {
      need_modify = true;
    } else if (is_pure_aux_tablets(info) || is_mixed_tablets(info)) {
      if (ObTabletBindingHelper::has_lob_tablets(arg, info)) {
        need_modify = true;
      }
    }

    if (OB_SUCC(ret) && need_modify) {
      bool skip = false;
      if (trans_flags.for_replay_
          && OB_FAIL(ObTabletBindingHelper::check_skip_tx_end(info.data_tablet_id_, ls_, trans_flags, skip))) {
        LOG_WARN("fail to check_skip_tx_end_create", K(ret), K(arg));
      } else if (skip) {
        LOG_INFO("skip tx_end for replay_create", K(info.data_tablet_id_), K(trans_flags));
      } else if (OB_FAIL(prepare_data_for_tablet_status(info.data_tablet_id_, ls_, trans_flags))) {
        LOG_WARN("failed to prepare_data_for_tablet_status", K(ret));
      } else if (OB_FAIL(prepare_data_for_binding_info(info.data_tablet_id_, ls_, trans_flags))) {
        LOG_WARN("failed to prepare_data_for_binding_info", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::tx_end_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &ls_id = ls_.get_ls_id();

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else {
    bool is_valid = true;
    ObSArray<ObTabletCreateInfo> tablet_create_info_array;
    if (OB_FAIL(do_tx_end_create_tablets(arg, trans_flags))) {
      LOG_WARN("failed to prepare data", K(ret));
    } else if (OB_FAIL(build_tablet_create_info(arg, tablet_create_info_array))) {
      LOG_WARN("failed to build tablet create info", K(ret), K(arg));
    } else {
      print_multi_data_for_create_tablet(tablet_create_info_array);
      LOG_INFO("succeeded to tx_end_create_tablets", K(ret), K(arg), K(trans_flags));
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::roll_back_remove_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObLSID &ls_id = ls_.get_ls_id();

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
      const ObTabletID &tablet_id = info.tablet_ids_.at(j);
      if (OB_FAIL(roll_back_remove_tablet(ls_id, tablet_id, trans_flags))) {
        LOG_WARN("failed to remove tablet for abort create", K(ret), K(ls_id), K(tablet_id), K(trans_flags));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::roll_back_remove_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle tablet_handle;

  // Try figure out if tablet needs to dec memtable ref or not
  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret) {
      // tablet does not exist or tablet creation failed on half way, do nothing
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
    }
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletMemtableMgr *memtable_mgr = static_cast<ObTabletMemtableMgr*>(tablet->get_memtable_mgr());
    ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handle_array;
    bool need_dec = false;
    if (OB_FAIL(memtable_mgr->get_all_memtables(memtable_handle_array))) {
      LOG_WARN("failed to get all memtables", K(ret), K(ls_id), K(tablet_id));
    } else if (memtable_handle_array.empty()) {
      // tablet does not have memtable, do nothing
    } else {
      const int64_t cnt = memtable_handle_array.count();
      ObTableHandleV2 &last_table_handle = memtable_handle_array.at(cnt - 1);
      ObMemtable *last_memtable = nullptr;
      ObTabletTxMultiSourceDataUnit tx_data;

      if (OB_FAIL(last_table_handle.get_data_memtable(last_memtable))) {
        LOG_WARN("failed to get memtable", K(ret), K(ls_id), K(tablet_id), K(cnt));
      } else if (OB_ISNULL(last_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last memtable is null", K(ret), K(ls_id), K(tablet_id), K(cnt));
      } else if (!last_memtable->has_multi_source_data_unit(MultiSourceDataUnitType::TABLET_TX_DATA)) {
        LOG_INFO("last memtable does not have msd, do nothing", K(ret), K(ls_id), K(tablet_id), K(cnt));
      } else if (OB_FAIL(tablet->get_tx_data(tx_data))) {
        LOG_WARN("failed to get tx data", K(ret), K(ls_id), K(tablet_id), K(cnt));
      } else if (OB_FAIL(ObTabletBindingHelper::check_need_dec_cnt_for_abort(tx_data, need_dec))) {
        LOG_WARN("failed to save tx data", K(ret), K(tx_data), K(trans_flags));
      } else if (need_dec && OB_FAIL(tablet->set_multi_data_for_commit(tx_data, ObScnRange::MAX_SCN,
          trans_flags.for_replay_, MemtableRefOp::DEC_REF))) {
        LOG_WARN("failed to save msd", K(ret), K(ls_id), K(tablet_id), K(cnt));
      } else {
        LOG_INFO("succeeded to dec ref for memtable in roll back operation", K(ret), K(ls_id), K(tablet_id), K(cnt));
      }
    }
  }

  // Whatever, delete tablet object from map and id set
  // del_tablet and erase should swallow any not exist error
  if (OB_SUCC(ret)) {
    if (OB_FAIL(t3m->del_tablet(key))) {
      LOG_WARN("failed to delete tablet from t3m", K(ret), K(key));
    } else if (OB_FAIL(tablet_id_set_.erase(tablet_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // tablet id does not exist in hash set
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to erase tablet id from hash set", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::do_abort_create_tablet(
    ObTabletHandle &tablet_handle,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
  const ObTabletMapKey key(ls_.get_ls_id(), tablet_id);
  ObTabletTxMultiSourceDataUnit tx_data;

  if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), K(tablet_handle));
  } else if (trans_flags.for_replay_ && trans_flags.scn_ <= tx_data.tx_scn_) {
    // replaying procedure, clog ts is smaller than tx log ts, just skip
    LOG_INFO("skip abort create tablet", K(ret), K(tablet_id), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(!trans_flags.for_replay_
                         && trans_flags.scn_.is_valid()
                         && tx_data.tx_scn_ != SCN::max_scn()
                         && trans_flags.scn_ <= tx_data.tx_scn_)) {
    // If tx log ts equals SCN::max_scn(), it means redo callback has not been called.
    // Thus, we should handle this situation
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts is no bigger than tx log ts", K(ret), K(tablet_id), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(trans_flags.tx_id_ != tx_data.tx_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx id does not equal", K(ret), K(tablet_id), K(trans_flags), K(tx_data));
    print_memtables_for_table(tablet_handle);
  } else if (OB_UNLIKELY(ObTabletStatus::CREATING != tx_data.tablet_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is not CREATING", K(ret), K(tablet_id), K(trans_flags), K(tx_data));
  } else {
    share::SCN tx_scn;
    share::SCN memtable_scn;
    if (trans_flags.scn_.is_valid()) {
      tx_scn = trans_flags.scn_;
      memtable_scn = trans_flags.scn_;
    } else {
      // trans flags scn is invalid, maybe tx is force killed
      tx_scn = tx_data.tx_scn_;
      memtable_scn = tx_data.tx_scn_;
    }

    if (OB_FAIL(set_tablet_final_status(tablet_handle, ObTabletStatus::DELETED,
        tx_scn, memtable_scn, trans_flags.for_replay_))) {
      LOG_WARN("failed to set tablet status to DELETED", K(ret), K(key), K(tablet_handle), K(trans_flags), K(tx_scn), K(memtable_scn));
    } else if (OB_FAIL(t3m->erase_pinned_tablet(key))) {
      LOG_ERROR("failed to erase tablet handle", K(ret), K(key));
      ob_usleep(1000 * 1000);
      ob_abort();
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::prepare_remove_tablets(
    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  bool normal = true;
  ObSArray<ObTabletID> tablet_id_array;
  const int64_t tablet_cnt = arg.tablet_ids_.count();

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg), K(ls_id));
  } else if (!trans_flags.for_replay_
      && OB_FAIL(check_tablet_status(arg, normal))) {
    LOG_WARN("failed to check tablet status", K(ret), K(arg));
  } else if (OB_UNLIKELY(!normal)) {
    ret = OB_EAGAIN;
    LOG_WARN("some tablet is not normal", K(ret), K(arg));
  } else if (trans_flags.for_replay_) {
    if (OB_FAIL(tablet_id_array.reserve(tablet_cnt))) {
      LOG_WARN("fail to allocate memory for tablet_id_array", K(ret), K(tablet_cnt));
    } else if (OB_FAIL(replay_verify_tablets(arg, trans_flags.scn_, tablet_id_array))) {
      LOG_WARN("failed to replay verify tablets", K(ret), K(trans_flags));
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObTabletID> *tablet_ids = trans_flags.for_replay_ ? &tablet_id_array : &arg.tablet_ids_;
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit cur_tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id;

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids->count(); ++i) {
      key.tablet_id_ = tablet_ids->at(i);
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(cur_tx_data))) {
        LOG_WARN("failed to get tx data", K(ret), K(key));
      } else {
        SCN flag_scn = trans_flags.scn_;
        ObTabletTxMultiSourceDataUnit tx_data;
        tx_data.tx_id_ = trans_flags.tx_id_;
        tx_data.tx_scn_ = trans_flags.for_replay_ ? flag_scn: cur_tx_data.tx_scn_;
        tx_data.tablet_status_ = ObTabletStatus::DELETING;

        if (OB_FAIL(ObTabletBindingHelper::lock_and_set_tx_data(tablet_handle, tx_data, trans_flags.for_replay_))) {
          LOG_WARN("failed to lock tablet binding", K(ret), K(key), K(tx_data));
        }
      }
    }
  }

  if (OB_FAIL(ret) && !trans_flags.for_replay_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(abort_remove_tablets(arg, trans_flags))) {
      LOG_WARN("failed to rollback prepare_remove", K(tmp_ret), K(trans_flags));
    } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_EAGAIN;
      LOG_WARN("failed due to 4013, has rolled back operations", K(ret), K(trans_flags), K(arg));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeeded to prepare remove tablets", K(ret), K(arg), K(trans_flags));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::replay_verify_tablets(
    const ObBatchRemoveTabletArg &arg,
    const SCN &scn,
    ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = arg.tablet_ids_[i];
    if (OB_FAIL(ls_.replay_get_tablet(tablet_id, scn, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("tablet already deleted", K(ret), K(tablet_id), K(scn));
        ret = OB_SUCCESS; // tablet already deleted, won't regard it as error
      } else if (OB_EAGAIN == ret) {
        // retry again
      } else {
        LOG_WARN("failed to replay get tablet", K(ret), K(tablet_id), K(scn));
      }
    } else {
      ObTabletTxMultiSourceDataUnit tx_data;
      if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get tx data", K(ret), K(tablet_handle));
      } else if (scn <= tx_data.tx_scn_) {
        FLOG_INFO("clog ts is no bigger than tx log ts in tx data, should skip this clog",
            K(tablet_id), K(scn), K(tx_data));
      } else if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("failed to push back into tablet id array", K(ret), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::redo_remove_tablets(
    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  const bool is_clog_replaying = trans_flags.for_replay_;
  const SCN scn = trans_flags.scn_;

  if (OB_UNLIKELY(!arg.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg), K(ls_id));
  } else if (trans_flags.for_replay_) {
    LOG_INFO("in clog replaying procedure, do nothing while redo log callback", K(ret), K(trans_flags));
  } else {
    ObTabletHandle tablet_handle;
    ObTabletTxMultiSourceDataUnit tx_data;
    ObTabletMapKey key;
    key.ls_id_ = ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
      key.tablet_id_ = arg.tablet_ids_.at(i);
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
        LOG_WARN("failed to get tx data", K(ret), K(tablet_handle));
      } else if (OB_FAIL(tablet_handle.get_obj()->set_tx_scn(tx_data.tx_id_, scn, false/*for_replay*/))) {
        LOG_WARN("failed to set tx data", K(ret), K(tablet_handle), K(tx_data), K(scn));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeeded to redo remove tablets", K(ret), K(arg), K(trans_flags));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::commit_remove_tablets(
    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  ObSArray<ObTabletID> tablet_id_array;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg), K(ls_id));
  } else if (trans_flags.for_replay_) {
    if (OB_FAIL(tablet_id_array.reserve(arg.tablet_ids_.count()))) {
    } else if (OB_FAIL(replay_verify_tablets(arg, trans_flags.scn_, tablet_id_array))) {
      LOG_WARN("failed to replay verify tablets", K(ret), K(trans_flags));
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObTabletID> *tablet_ids = trans_flags.for_replay_ ? &tablet_id_array : &arg.tablet_ids_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids->count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids->at(i);
      if (OB_FAIL(do_commit_remove_tablet(tablet_id, trans_flags))) {
        LOG_WARN("failed to do commit remove tablet", K(ret), K(tablet_id), K(trans_flags));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeeded to commit remove tablets", K(ret), K(arg), K(trans_flags));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_commit_remove_tablet(
    const ObTabletID &tablet_id,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletMapKey key(ls_.get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;
  ObTabletTxMultiSourceDataUnit tx_data;

  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), K(key));
  } else if (OB_UNLIKELY(!trans_flags.for_replay_ && trans_flags.scn_ <= tx_data.tx_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts is smaller than tx log ts", K(ret), K(key), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(trans_flags.tx_id_ != tx_data.tx_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx id does not equal", K(ret), K(key), K(trans_flags), K(tx_data));
    print_memtables_for_table(tablet_handle);
  } else if (OB_UNLIKELY(ObTabletStatus::DELETING != tx_data.tablet_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is not DELETING", K(ret), K(key), K(trans_flags), K(tx_data));
  } else {
    if (OB_FAIL(set_tablet_final_status(tablet_handle, ObTabletStatus::DELETED,
        trans_flags.scn_, trans_flags.scn_, trans_flags.for_replay_))) {
      LOG_WARN("failed to set tablet status to DELETED", K(ret), K(tablet_handle),
          K(trans_flags.scn_), K(trans_flags));
    } else if (OB_FAIL(t3m->erase_pinned_tablet(key))) {
      LOG_ERROR("failed to erase tablet handle", K(ret), K(key));
      ob_usleep(1000 * 1000);
      ob_abort();
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::abort_remove_tablets(
    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  ObSArray<ObTabletID> tablet_id_array;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg), K(ls_id));
  } else if (trans_flags.for_replay_) {
    if (OB_FAIL(tablet_id_array.reserve(arg.tablet_ids_.count()))) {
    } else if (OB_FAIL(replay_verify_tablets(arg, trans_flags.scn_, tablet_id_array))) {
      LOG_WARN("failed to replay verify tablets", K(ret), K(trans_flags));
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObTabletID> *tablet_ids = trans_flags.for_replay_ ? &tablet_id_array : &arg.tablet_ids_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids->count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids->at(i);
      if (OB_FAIL(do_abort_remove_tablet(tablet_id, trans_flags))) {
        LOG_WARN("failed to do abort remove tablet", K(ret), K(tablet_id), K(trans_flags));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeeded to abort remove tablets", K(ret), K(trans_flags));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_abort_remove_tablet(
    const ObTabletID &tablet_id,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletMapKey key(ls_.get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;
  ObTabletTxMultiSourceDataUnit tx_data;
  MemtableRefOp ref_op = MemtableRefOp::NONE;
  bool is_valid = true;
  SCN tx_scn;

  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    // leader handle 4013 error(caused by memory not enough in load and dump procedure)
    if (OB_ALLOCATE_MEMORY_FAILED == ret && !trans_flags.for_replay_) {
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      ObTabletTxMultiSourceDataUnit tx_data;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(t3m->get_tablet_pointer_tx_data(key, tx_data))) {
        LOG_WARN("failed to get tablet status", K(tmp_ret), K(key));
      } else if (ObTabletStatus::NORMAL == tx_data.tablet_status_) {
        ret = OB_SUCCESS;
        is_valid = false;
        LOG_INFO("tablet status is NORMAL, no need to handle", K(ret), K(key));
      } else {
        LOG_WARN("tablet status is NOT NORMAL", K(ret), K(key), K(tx_data));
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), K(key));
  } else if (OB_UNLIKELY(!trans_flags.for_replay_
                         && trans_flags.scn_ != SCN::invalid_scn()
                         && trans_flags.scn_ <= tx_data.tx_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts is no bigger than tx log ts", K(ret), K(key), K(trans_flags), K(tx_data));
  } else if (OB_UNLIKELY(trans_flags.tx_id_ != tx_data.tx_id_)) {
    is_valid = false;
    LOG_INFO("tx id does not equal", K(ret), K(key), K(trans_flags), K(tx_data));
    print_memtables_for_table(tablet_handle);
  } else if (OB_UNLIKELY(ObTabletStatus::DELETING != tx_data.tablet_status_)) {
    is_valid = false;
    LOG_INFO("tablet status is not DELETING", K(ret), K(key), K(trans_flags), K(tx_data));
  } else {
    bool need_dec = false;
    if (OB_FAIL(ObTabletBindingHelper::check_need_dec_cnt_for_abort(tx_data, need_dec))) {
      LOG_WARN("failed to save tx data", K(ret), K(tx_data), K(trans_flags));
    } else if (need_dec) {
      ref_op = MemtableRefOp::DEC_REF;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_valid) {
    if (trans_flags.for_replay_) {
      // if tablet is NORMAL, and meets abort remove procedure when replaying, we should not consider it illegal
      if (ObTabletStatus::NORMAL == tx_data.tablet_status_ && ObTabletCommon::FINAL_TX_ID == tx_data.tx_id_) {
        LOG_INFO("tablet is in NORMAL status, do nothing", K(ret), K(key), K(trans_flags));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is not valid, and not in NORMAL status", K(ret), K(key), K(trans_flags));
      }
    } else {
      // For leader, we may encounter such disorder:
      // 1) redo create
      // 2) prepare remove
      // 3) commit create
      // 4) abort remove
      // This is because upper layer may do 'prepare action' before lower layer actually do 'commit create' callback,
      // thus 'commit create' callback may be delayed.
      // So we should NOT consider such scene as error if tx id or tablet status is not what we expected.
      LOG_INFO("tablet is no valid but do nothing", K(ret), K(key), K(trans_flags));
    }
  } else {
    if (trans_flags.for_replay_) {
      tx_scn = trans_flags.scn_;
    } else if (trans_flags.is_redo_synced()) {
      tx_scn = trans_flags.scn_;
    } else {
      tx_scn = tx_data.tx_scn_;
    }

    const SCN &memtable_scn = (SCN::invalid_scn() == trans_flags.scn_) ? SCN::max_scn() : trans_flags.scn_;

    if (OB_FAIL(set_tablet_final_status(tablet_handle, ObTabletStatus::NORMAL,
        tx_scn, memtable_scn, trans_flags.for_replay_, ref_op))) {
      LOG_WARN("failed to set tablet status to NORMAL", K(ret), K(tablet_handle),
          K(tx_scn), K(memtable_scn), K(trans_flags), K(ref_op));
    } else if (OB_FAIL(t3m->erase_pinned_tablet(key))) {
      LOG_ERROR("failed to erase tablet handle", K(ret), K(key));
      ob_usleep(1000 * 1000);
      ob_abort();
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_tx_end_remove_tablets(

    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
    bool skip = false;
    if (trans_flags.for_replay_
        && OB_FAIL(ObTabletBindingHelper::check_skip_tx_end(tablet_id, ls_, trans_flags, skip))) {
      LOG_WARN("fail to check_skip_tx_end_remove", K(ret), K(arg));
    } else if (skip) {
      LOG_INFO("skip tx_end for replay_remove", K(tablet_id), K(trans_flags));
    }
    // modify tablet status_info
    else if (OB_FAIL(prepare_data_for_tablet_status(tablet_id, ls_, trans_flags))) {
      LOG_WARN("failed to prepare_data_for_tablet_status", KR(ret), K(tablet_id));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::tx_end_remove_tablets(
    const ObBatchRemoveTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &ls_id = ls_.get_ls_id();

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else if (OB_UNLIKELY(arg.id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg), K(ls_id), K(tenant_id));
  } else if (OB_FAIL(do_tx_end_remove_tablets(arg, trans_flags))) {
    LOG_WARN("failed to do_tx_end", K(ret));
  } else {
    print_multi_data_for_remove_tablet(arg);
  }

  return ret;
}

int ObTabletCreateDeleteHelper::get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TIME_US = 10;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;
  const int64_t begin_time = ObClockGenerator::getClock();
  int64_t current_time = 0;

  while (OB_SUCC(ret)) {
    ret = t3m->get_tablet(WashTabletPriority::WTP_HIGH, key, tablet_handle);
    if (OB_SUCC(ret)) {
      break;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_DEBUG("tablet does not exist", K(ret), K(key));
    } else if (OB_ITEM_NOT_SETTED == ret) {
      current_time = ObClockGenerator::getClock();
      if (current_time - begin_time > timeout_us) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("continuously meet item not set error", K(ret), K(begin_time), K(current_time), K(timeout_us));
      } else {
        ret = OB_SUCCESS;
        ob_usleep(SLEEP_TIME_US);
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key));
    }
  }

  if (OB_SUCC(ret)) {
    handle = tablet_handle;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_and_get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletStatus::Status tablet_status = ObTabletStatus::MAX;

  // TODO(bowen.gbw): optimize this logic, refactor ObTabletStatusChecker
  if (OB_FAIL(get_tablet(key, tablet_handle, timeout_us))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_DEBUG("tablet does not exist", K(ret), K(key), K(timeout_us));
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key), K(timeout_us));
    }
  } else if (tablet_handle.get_obj()->is_ls_inner_tablet()) {
    // no need to check ls inner tablet, do nothing
  } else if (ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US == timeout_us) {
    // no checking
  } else if (OB_FAIL(tablet_handle.get_obj()->get_tablet_status(tablet_status))) {
    LOG_WARN("failed to get tablet status", K(ret));
  } else if (ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US == timeout_us) {
    if (ObTabletStatus::NORMAL != tablet_status) {
      ret = OB_TABLET_NOT_EXIST;
    }
  } else {
    if (ObTabletStatus::NORMAL == tablet_status) {
    } else if (ObTabletStatus::DELETED == tablet_status) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      // check status
      ObTabletStatusChecker checker(*tablet_handle.get_obj());
      if (OB_FAIL(checker.check(timeout_us))) {
        LOG_WARN("failed to check tablet status", K(ret), K(timeout_us), K(tablet_handle));
      }
    }
  }

  if (OB_SUCC(ret)) {
    handle = tablet_handle;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::acquire_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const bool only_acquire)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("failed to get ls", K(ret), "ls_id", key.ls_id_);
  } else if (OB_FAIL(t3m->acquire_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, tablet_handle, only_acquire))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(key));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(tablet_handle));
  } else {
    handle = tablet_handle;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::create_sstable(
    const ObTabletCreateSSTableParam &param,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else {
    ObIAllocator &allocator = t3m->get_tenant_allocator();
    ObTableHandleV2 handle;
    ObSSTable *sstable = nullptr;

    if (OB_FAIL(t3m->acquire_sstable(handle))) {
      LOG_WARN("failed to acquire sstable", K(ret));
    } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(handle.get_table()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table", K(ret), K(handle));
    } else if (OB_FAIL(sstable->init(param, &allocator))) {
      LOG_WARN("failed to init sstable", K(ret), K(param));
    } else {
      table_handle = handle;
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_create_new_tablets(const obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObUnitInfoGetter::ObTenantConfig unit;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  int64_t tablet_cnt_per_gb = 20000; // default value
  bool skip_check = !arg.need_check_tablet_cnt_;

  // skip hidden tablet creation or truncate tablet creation
  for (int64_t i = 0; OB_SUCC(ret) && !skip_check && i < arg.table_schemas_.count(); ++i) {
    if (arg.table_schemas_[i].is_user_hidden_table()
      || OB_INVALID_VERSION != arg.table_schemas_[i].get_truncate_version()) {
      skip_check = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (skip_check) {
  } else {
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid tenant config", K(ret));
      } else {
        tablet_cnt_per_gb = tenant_config->_max_tablet_cnt_per_gb;
      }
    }

    if (FAILEDx(GCTX.omt_->get_tenant_unit(tenant_id, unit))) {
      if (OB_TENANT_NOT_IN_SERVER != ret) {
        LOG_WARN("failed to get tenant unit", K(ret), K(tenant_id));
      } else {
        // during restart, tenant unit not ready, skip check
        ret = OB_SUCCESS;
      }
    } else {
      const double memory_limit = unit.config_.memory_size();
      const int64_t max_tablet_cnt = memory_limit / (1 << 30) * tablet_cnt_per_gb;
      const int64_t cur_tablet_cnt = t3m->get_total_tablet_cnt();
      const int64_t inc_tablet_cnt = arg.get_tablet_count();

      if (OB_UNLIKELY(cur_tablet_cnt + inc_tablet_cnt >= max_tablet_cnt)) {
        ret = OB_TOO_MANY_PARTITIONS_ERROR;
        LOG_WARN("too many partitions of tenant", K(ret), K(tenant_id), K(memory_limit), K(tablet_cnt_per_gb),
        K(max_tablet_cnt), K(cur_tablet_cnt), K(inc_tablet_cnt));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::verify_tablets_absence(
    const ObBatchCreateTabletArg &arg,
    ObIArray<ObTabletCreateInfo> &tablet_create_info_array)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;

  if (OB_FAIL(check_tablet_absence(arg, is_valid))) {
    LOG_WARN("failed to check tablet absence", K(ret), K(PRINT_CREATE_ARG(arg)));
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, some tablet exists", K(ret));
  } else if (OB_FAIL(build_tablet_create_info(arg, tablet_create_info_array))) {
    LOG_WARN("failed to build tablet create info", K(ret));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_tablet_existence(
    const ObBatchCreateTabletArg &arg,
    bool &is_valid)
{
  // expect all tablets exist
  int ret = OB_SUCCESS;
  bool exist = true;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && exist && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && exist && j < info.tablet_ids_.count(); ++j) {
      key.tablet_id_ = info.tablet_ids_.at(j);
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else {
        exist = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_valid = exist;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_tablet_absence(
    const ObBatchCreateTabletArg &arg,
    bool &is_valid)
{
  // expect all tablets do not exist, except data tablet when creating pure index/hidden tablets
  int ret = OB_SUCCESS;
  is_valid = true;
  const ObLSID &ls_id = arg.id_;
  ObSArray<int64_t> skip_idx;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    if (is_contain(skip_idx, i)) {
      // do nothing
    } else if (is_pure_data_tablets(info) || is_mixed_tablets(info)) {
      if (OB_FAIL(check_pure_data_or_mixed_tablets_info(ls_id, info, is_valid))) {
        LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
      }
    } else if (is_pure_aux_tablets(info)) {
      if (OB_FAIL(check_pure_index_or_hidden_tablets_info(ls_id, info, is_valid))) {
        LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
      }
    } else if (is_pure_hidden_tablets(info)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
        int64_t aux_idx = -1;
        if (find_related_aux_info(arg, info.tablet_ids_.at(j), aux_idx)) {
          const ObCreateTabletInfo &aux_info = arg.tablets_.at(aux_idx);
          if (OB_FAIL(check_pure_data_or_mixed_tablets_info(ls_id, aux_info, is_valid))) {
            LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(aux_info));
          } else if (OB_FAIL(skip_idx.push_back(aux_idx))) {
            LOG_WARN("failed to push back skip idx", K(ret), K(aux_idx));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(check_pure_index_or_hidden_tablets_info(ls_id, info, is_valid))) {
        LOG_WARN("failed to check create tablet info", K(ret), K(ls_id), K(info));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected create tablet info", K(ret), K(info));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_pure_data_or_mixed_tablets_info(
    const ObLSID &ls_id,
    const ObCreateTabletInfo &info,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool not_exist = true;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  for (int64_t i = 0; OB_SUCC(ret) && not_exist && i < info.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = info.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        not_exist = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      }
    } else {
      not_exist = false;
      LOG_INFO("tablet exists", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    is_valid = not_exist;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_pure_index_or_hidden_tablets_info(
    const ObLSID &ls_id,
    const ObCreateTabletInfo &info,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = ls_id;

  for (int64_t i = 0; OB_SUCC(ret) && valid && i < info.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = info.tablet_ids_[i];
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        valid = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      }
    } else {
      valid = false;
      LOG_INFO("tablet exists", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    is_valid = valid;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::build_tablet_create_info(
    const ObBatchCreateTabletArg &arg,
    ObIArray<ObTabletCreateInfo> &tablet_create_info_array)
{
  int ret = OB_SUCCESS;
  ObSArray<int64_t> skip_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    ObTabletCreateInfo tablet_create_info;
    bool need_push = false;
    if (is_contain(skip_idx, i)) {
      // do nothing
    } else if (is_pure_data_tablets(info)) {
      tablet_create_info.create_data_tablet_ = true;
      tablet_create_info.data_tablet_id_ = info.data_tablet_id_;
      need_push = true;
    } else if (is_mixed_tablets(info)) {
      tablet_create_info.create_data_tablet_ = true;
      tablet_create_info.data_tablet_id_ = info.data_tablet_id_;
      need_push = true;
      if (OB_FAIL(fill_aux_infos(arg, info, tablet_create_info))) {
        LOG_WARN("failed to fill aux info", K(ret));
      }
    } else if (is_pure_aux_tablets(info)) {
      tablet_create_info.create_data_tablet_ = false;
      tablet_create_info.data_tablet_id_ = info.data_tablet_id_;
      need_push = true;
      if (OB_FAIL(fill_aux_infos(arg, info, tablet_create_info))) {
        LOG_WARN("failed to fill aux info", K(ret));
      }
    } else if (is_pure_hidden_tablets(info)) {
      tablet_create_info.create_data_tablet_ = true;
      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); j++) {
        tablet_create_info.data_tablet_id_ = info.tablet_ids_[j];
        // find hidden lob info
        int64_t aux_idx = -1;
        if (find_related_aux_info(arg, tablet_create_info.data_tablet_id_, aux_idx)) {
          const ObCreateTabletInfo &aux_info = arg.tablets_.at(aux_idx);
          if (OB_FAIL(fill_aux_infos(arg, aux_info, tablet_create_info))) {
            LOG_WARN("failed to fill aux info", K(ret), K(PRINT_CREATE_ARG(arg)), K(aux_idx));
          } else if (OB_FAIL(skip_idx.push_back(aux_idx))) {
            LOG_WARN("failed to push skip idx", K(ret), K(aux_idx));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(tablet_create_info_array.push_back(tablet_create_info))) {
          LOG_WARN("failed to push back tablet create info", K(ret), K(tablet_create_info));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet create info", K(ret), K(info));
    }

    if (OB_FAIL(ret)) {
    } else if (need_push) {
      if (!tablet_create_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet create info", K(ret), K(tablet_create_info));
      } else if (OB_FAIL(tablet_create_info_array.push_back(tablet_create_info))) {
        LOG_WARN("failed to push back tablet create info", K(ret), K(tablet_create_info));
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::ensure_skip_create_all_tablets_safe(
    const ObBatchCreateTabletArg &arg,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObSArray<int64_t> skip_idx;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    const ObTabletID &data_tablet_id = info.data_tablet_id_;
    if (is_contain(skip_idx, i)) {
      // do nothing
      LOG_INFO("pure aux tablet info should be skipped", K(ret), K(info));
    } else if (is_mixed_tablets(info) || is_pure_data_tablets(info)) {
      // data tablet already checked, can ignore
    } else if (is_pure_aux_tablets(info) || is_pure_hidden_tablets(info)) {
      if (is_pure_hidden_tablets(info)) {
        // If hidden tablets and related lob tablets are created simultaneously,
        // upper layer ensures that hidden tablet info occurs before lob tablets(as aux tablet info).
        // So here we only need to find aux info which is related to hidden tablets,
        // and the related aux tablet info will be skipped later.
        for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
          int64_t aux_idx = -1;
          if (find_related_aux_info(arg, info.tablet_ids_.at(j), aux_idx)) {
            if (OB_FAIL(skip_idx.push_back(aux_idx))) {
              LOG_WARN("failed to push back skip idx", K(ret), K(aux_idx));
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls_.replay_get_tablet(data_tablet_id, scn, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret && OB_EAGAIN != ret) {
          LOG_WARN("failed to replay get tablet", K(ret), K(data_tablet_id), K(scn));
        } else if (OB_TABLET_NOT_EXIST == ret) {
          // data tablet is deleted after log_ts of this transaction, safe to skip creation
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::fill_aux_infos(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    ObTabletCreateInfo &tablet_create_info)
{
  int ret = OB_SUCCESS;
  for (int j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
    const ObTabletID &tablet_id = info.tablet_ids_.at(j);
    if (tablet_id == info.data_tablet_id_) {
      // do nothing
    } else if (arg.table_schemas_.at(info.table_schema_index_.at(j)).is_aux_lob_meta_table()) {
      tablet_create_info.lob_meta_tablet_id_ = tablet_id;
    } else if (arg.table_schemas_.at(info.table_schema_index_.at(j)).is_aux_lob_piece_table()) {
      tablet_create_info.lob_piece_tablet_id_ = tablet_id;
    } else if (OB_FAIL(tablet_create_info.index_tablet_id_array_.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
    }
  }
  return ret;
}

bool ObTabletCreateDeleteHelper::find_related_aux_info(
    const ObBatchCreateTabletArg &arg,
    const ObTabletID &data_tablet_id,
    int64_t &idx)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    if (is_pure_aux_tablets(info) && info.data_tablet_id_ == data_tablet_id) {
      idx = i;
      bret = true;
    }
  }
  return bret;
}

bool ObTabletCreateDeleteHelper::is_pure_data_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() == 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_mixed_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() > 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_pure_aux_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateDeleteHelper::is_pure_hidden_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && info.is_create_bind_hidden_tablets_;
}

int ObTabletCreateDeleteHelper::get_all_existed_tablets(
    const ObBatchCreateTabletArg &arg,
    const SCN &scn,
    ObIArray<ObTabletID> &existed_tablet_id_array,
    NonLockedHashSet &existed_tablet_id_set)
{
  int ret = OB_SUCCESS;
  bool b_exist = false;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
      const ObTabletID &tablet_id = info.tablet_ids_[j];
      key.tablet_id_ = tablet_id;
      b_exist = true;
      if (OB_FAIL(get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          b_exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), "ls_id", arg.id_, K(tablet_id));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (b_exist) {
        FLOG_INFO("tablet already exists while clog replaying", K(tablet_handle));
        bool tx_data_is_valid = false;
        // check tx data
        if (OB_FAIL(tablet_handle.get_obj()->check_tx_data(tx_data_is_valid))) {
          LOG_WARN("failed to check tx data in new tablet", K(ret));
        } else if (!tx_data_is_valid) {
          // tx data is invalid, because we may write slog before tablet gc service does minor freezing
          // in this situation, tx data is NOT valid
          if (OB_FAIL(existed_tablet_id_array.push_back(tablet_id))) {
            LOG_WARN("failed to push back into array", K(ret), K(tablet_id));
          } else {
            LOG_INFO("tablet exists and tx data is invalid", K(ret), K(key));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(existed_tablet_id_set.set_refactored(tablet_id))) {
          LOG_WARN("failed to insert into hash set", K(ret), K(tablet_id));
        }
      }
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::build_batch_create_tablet_arg(
    const ObBatchCreateTabletArg &old_arg,
    const NonLockedHashSet &existed_tablet_id_set,
    ObBatchCreateTabletArg &new_arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObSArray<ObCreateTabletInfo> &old_info_array = old_arg.tablets_;

  for (int64_t i = 0; OB_SUCC(ret) && i < old_info_array.count(); ++i) {
    const ObCreateTabletInfo &old_info = old_info_array[i];
    const ObSArray<ObTabletID> &old_tablet_id_array = old_info.tablet_ids_;
    ObCreateTabletInfo new_info;
    new_info.data_tablet_id_ = old_info.data_tablet_id_;
    new_info.compat_mode_ = old_info.compat_mode_;
    new_info.is_create_bind_hidden_tablets_ = old_info.is_create_bind_hidden_tablets_;

    for (int64_t j = 0; OB_SUCC(ret) && j < old_tablet_id_array.count(); ++j) {
      const ObTabletID &old_tablet_id = old_tablet_id_array[j];
      tmp_ret = existed_tablet_id_set.exist_refactored(old_tablet_id);
      if (OB_HASH_EXIST == tmp_ret) {
        LOG_INFO("tablet id exists, should skip", K(old_tablet_id));
      } else if (OB_HASH_NOT_EXIST == tmp_ret) {
        const int64_t old_table_schema_index = old_info.table_schema_index_[j];
        if (OB_FAIL(new_info.tablet_ids_.push_back(old_tablet_id))) {
          LOG_WARN("failed to push back old tablet id", K(ret), K(old_tablet_id));
        } else if (OB_FAIL(new_info.table_schema_index_.push_back(old_table_schema_index))) {
          LOG_WARN("failed to push back table schema index", K(ret), K(old_tablet_id));
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("unexpected error when checking old tablet id", K(ret), K(old_tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (new_info.get_tablet_count() > 0) {
      if (OB_FAIL(new_arg.tablets_.push_back(new_info))) {
        LOG_WARN("failed to push back tablet info", K(ret), K(new_info));
      }
    } else {
      LOG_INFO("skip create tablet info", K(ret), K(old_info));
    }
  }

  // copy other members in arg
  if (OB_FAIL(ret)) {
  } else {
    new_arg.id_ = old_arg.id_;
    new_arg.major_frozen_scn_ = old_arg.major_frozen_scn_;

    if (new_arg.get_tablet_count() > 0 && OB_FAIL(new_arg.table_schemas_.assign(old_arg.table_schemas_))) {
      LOG_WARN("failed to assign table schemas", K(ret), K(old_arg));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::batch_create_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = arg.tablets_.at(i);
    if (OB_FAIL(create_tablet(arg, info, trans_flags))) {
      LOG_WARN("failed to create tablet", K(ret), K(info), K(i), K(trans_flags));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::create_tablet(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid create tablet info", K(ret), K(info));
  } else if (is_pure_data_tablets(info)) {
    // pure data tablet.
    if (OB_FAIL(build_pure_data_tablet(arg, info, trans_flags))) {
      LOG_WARN("failed to build single data tablet", K(ret), K(PRINT_CREATE_ARG(arg)), K(info), K(trans_flags));
    }
  } else if (is_mixed_tablets(info)) {
    // data tablet + index tablets.
    if (OB_FAIL(build_mixed_tablets(arg, info, trans_flags))) {
      LOG_WARN("failed to build index tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(info), K(trans_flags));
    }
  } else if (is_pure_aux_tablets(info)) {
    // pure index tablet.
    if (OB_FAIL(build_pure_aux_tablets(arg, info, trans_flags))) {
      LOG_WARN("failed to build index tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(info), K(trans_flags));
    }
  } else if (is_pure_hidden_tablets(info)) {
    // pure hidden tablet.
    if (OB_FAIL(build_pure_hidden_tablets(arg, info, trans_flags))) {
      LOG_WARN("failed to build hidden tablets", K(ret), K(PRINT_CREATE_ARG(arg)), K(info), K(trans_flags));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info is invalid", K(ret), K(info));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::build_pure_data_tablet(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObTableSchema> &table_schemas = arg.table_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  ObSArray<ObTabletID> empty_array;
  ObTabletID empty_lob_tablet_id;
  ObTabletHandle tablet_handle;
  int64_t index = -1;
  ObTabletMapKey key(ls_id, data_tablet_id);

  if (OB_FAIL(get_tablet_schema_index(data_tablet_id, tablet_ids, index))) {
    LOG_WARN("failed to get tablet schema index in array", K(ret), K(data_tablet_id));
  } else if (OB_UNLIKELY(index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema index is invalid", K(ret), K(key), K(index));
  } else if (OB_FAIL(do_create_tablet(data_tablet_id, data_tablet_id, empty_lob_tablet_id,
      empty_lob_tablet_id, empty_array, arg, trans_flags,
      table_schemas[info.table_schema_index_[index]], compat_mode, tablet_handle))) {
    LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(data_tablet_id), K(PRINT_CREATE_ARG(arg)));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::build_mixed_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObTableSchema> &table_schemas = arg.table_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  ObSArray<ObTabletID> empty_array;
  ObSArray<ObTabletID> index_tablet_array;
  ObTabletHandle data_tablet_handle;
  int64_t data_tablet_index = -1;
  int64_t lob_meta_tablet_index = -1;
  int64_t lob_piece_tablet_index = -1;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  ObTabletID empty_lob_tablet_id;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids[i];
    if (tablet_id == data_tablet_id) {
      data_tablet_index = i;
    } else if (table_schemas.at(info.table_schema_index_.at(i)).is_aux_lob_meta_table()) {
      lob_meta_tablet_index = i;
    } else if (table_schemas.at(info.table_schema_index_.at(i)).is_aux_lob_piece_table()) {
      lob_piece_tablet_index = i;
    } else {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(index_tablet_array.push_back(tablet_id))) {
        LOG_WARN("failed to insert tablet id into index tablet array", K(ret), K(tablet_id));
      } else if (OB_FAIL(do_create_tablet(tablet_id, data_tablet_id, empty_lob_tablet_id,
          empty_lob_tablet_id, empty_array, arg, trans_flags,
          table_schemas[info.table_schema_index_[i]], compat_mode, tablet_handle))) {
        LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id), K(PRINT_CREATE_ARG(arg)));
      }
    }
  }

  // process lob tablets
  ObTabletHandle lob_meta_tablet_handle;
  ObTabletHandle lob_piece_tablet_handle;
  if (OB_FAIL(ret)) { // do nothing
  } else if (lob_meta_tablet_index != -1) {
    lob_meta_tablet_id = tablet_ids[lob_meta_tablet_index];
    if (OB_FAIL(do_create_tablet(lob_meta_tablet_id, data_tablet_id, lob_meta_tablet_id,
        empty_lob_tablet_id, empty_array, arg, trans_flags,
        table_schemas[info.table_schema_index_[lob_meta_tablet_index]], compat_mode, lob_meta_tablet_handle))) {
      LOG_WARN("failed to do create lob meta tablet", K(ret), K(ls_id), K(lob_meta_tablet_id), K(data_tablet_id), K(PRINT_CREATE_ARG(arg)));
    }
  }
  if (OB_FAIL(ret)) { // do nothing
  } else if (lob_piece_tablet_index != -1) {
    lob_piece_tablet_id = tablet_ids[lob_piece_tablet_index];
    if (OB_FAIL(do_create_tablet(lob_piece_tablet_id, data_tablet_id, empty_lob_tablet_id,
        lob_piece_tablet_id, empty_array, arg, trans_flags,
        table_schemas[info.table_schema_index_[lob_piece_tablet_index]], compat_mode, lob_piece_tablet_handle))) {
      LOG_WARN("failed to do create lob piece tablet", K(ret), K(ls_id), K(lob_piece_tablet_id), K(data_tablet_id), K(PRINT_CREATE_ARG(arg)));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(data_tablet_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data tablet index is invalid", K(ret), K(data_tablet_index));
  } else if (OB_FAIL(do_create_tablet(data_tablet_id, data_tablet_id, lob_meta_tablet_id,
      lob_piece_tablet_id, index_tablet_array, arg, trans_flags,
      table_schemas[info.table_schema_index_[data_tablet_index]], compat_mode, data_tablet_handle))) {
    LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(data_tablet_id), K(index_tablet_array), K(PRINT_CREATE_ARG(arg)));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::build_pure_aux_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  const ObSArray<ObTableSchema> &table_schemas = arg.table_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  ObSArray<ObTabletID> empty_array;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  ObTabletID empty_lob_tablet_id;
  ObTabletHandle aux_tablet_handle;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &aux_tablet_id = tablet_ids[i];
    if (table_schemas.at(info.table_schema_index_.at(i)).is_aux_lob_meta_table()) {
      lob_meta_tablet_id = aux_tablet_id;
    } else if (table_schemas.at(info.table_schema_index_.at(i)).is_aux_lob_piece_table()) {
      lob_piece_tablet_id = aux_tablet_id;
    } else {
      lob_meta_tablet_id.reset();
      lob_piece_tablet_id.reset();
    }
    if (OB_FAIL(do_create_tablet(aux_tablet_id, data_tablet_id, lob_meta_tablet_id,
        lob_piece_tablet_id, empty_array, arg, trans_flags,
        table_schemas[info.table_schema_index_[i]], compat_mode, aux_tablet_handle))) {
      LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(aux_tablet_id), K(data_tablet_id), K(PRINT_CREATE_ARG(arg)));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::build_pure_hidden_tablets(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  const ObSArray<ObTabletID> &hidden_tablet_ids = info.tablet_ids_;
  const ObSArray<ObTableSchema> &table_schemas = arg.table_schemas_;
  const lib::Worker::CompatMode &compat_mode = info.compat_mode_;
  ObSArray<ObTabletID> empty_array;
  ObTabletHandle hidden_tablet_handle;
  ObMetaDiskAddr mem_addr;
  if (OB_FAIL(mem_addr.set_mem_addr(0, sizeof(ObTablet)))) {
    LOG_WARN("fail to set memory address", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hidden_tablet_ids.count(); ++i) {
    const ObTabletID &hidden_tablet_id = hidden_tablet_ids[i];
    int64_t aux_idx = -1;
    ObTabletID lob_meta_tablet_id;
    ObTabletID lob_piece_tablet_id;
    if (find_related_aux_info(arg, hidden_tablet_id, aux_idx)) {
      const ObCreateTabletInfo &aux_info = arg.tablets_.at(aux_idx);
      for (int64_t j = 0; j < aux_info.tablet_ids_.count(); ++j) {
        if (arg.table_schemas_.at(aux_info.table_schema_index_.at(j)).is_aux_lob_meta_table()) {
          lob_meta_tablet_id = aux_info.tablet_ids_.at(j);
        } else if (arg.table_schemas_.at(aux_info.table_schema_index_.at(j)).is_aux_lob_piece_table()) {
          lob_piece_tablet_id = aux_info.tablet_ids_.at(j);
        }
      }
    }
    if (OB_FAIL(do_create_tablet(hidden_tablet_id, hidden_tablet_id, lob_meta_tablet_id,
        lob_piece_tablet_id, empty_array, arg, trans_flags,
        table_schemas[info.table_schema_index_[i]], compat_mode, hidden_tablet_handle))) {
      LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(hidden_tablet_id), K(PRINT_CREATE_ARG(arg)));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::get_tablet_schema_index(
    const ObTabletID &tablet_id,
    const ObIArray<ObTabletID> &table_ids,
    int64_t &index)
{
  int ret = OB_SUCCESS;
  bool match = false;

  for (int64_t i = 0; !match && i < table_ids.count(); ++i) {
    if (table_ids.at(i) == tablet_id) {
      index = i;
      match = true;
    }
  }

  if (OB_UNLIKELY(!match)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot find target tablet id in array", K(ret), K(tablet_id));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::do_create_tablet(
    const ObTabletID &tablet_id,
    const ObTabletID &data_tablet_id,
    const ObTabletID &lob_meta_tablet_id,
    const ObTabletID &lob_piece_tablet_id,
    const ObIArray<ObTabletID> &index_tablet_array,
    const ObBatchCreateTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags,
    const ObTableSchema &table_schema,
    const lib::Worker::CompatMode &compat_mode,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObLSID &ls_id = arg.id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;
  ObTableHandleV2 table_handle;
  ObTabletCreateSSTableParam param;
  ObFreezer *freezer = ls_.get_freezer();
  bool need_create_empty_major_sstable = false;
  const ObTransID &tx_id = trans_flags.tx_id_;
  const bool for_replay = trans_flags.for_replay_;
  MemtableRefOp ref_op = for_replay ? MemtableRefOp::NONE : MemtableRefOp::INC_REF;
  SCN scn = trans_flags.for_replay_ ? trans_flags.scn_ : SCN::max_scn();
  SCN create_scn = trans_flags.scn_;
  ObMetaDiskAddr mem_addr;
  ObTabletTableStoreFlag table_store_flag;
  table_store_flag.set_with_major_sstable();
  if (OB_FAIL(mem_addr.set_mem_addr(0, sizeof(ObTablet)))) {
    LOG_WARN("fail to set memory address", K(ret));
  } else if (OB_FAIL(acquire_tablet(key, tablet_handle, false/*only acquire*/))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(key));
  } else if (OB_FAIL(check_need_create_empty_major_sstable(table_schema, need_create_empty_major_sstable))) {
    LOG_WARN("failed to check need create sstable", K(ret));
  } else if (!need_create_empty_major_sstable) {
    table_store_flag.set_without_major_sstable();
    LOG_INFO("no need to create sstable", K(ls_id), K(tablet_id), K(table_schema));
  } else if (OB_FAIL(build_create_sstable_param(
      table_schema, tablet_id, arg.major_frozen_scn_.get_val_for_tx(), param))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(tablet_id),
        K(table_schema), K(arg), K(param));
  } else if (OB_FAIL(create_sstable(param, table_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(param));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tablet is NULL", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->init(ls_id, tablet_id, data_tablet_id, lob_meta_tablet_id, lob_piece_tablet_id,
      create_scn, arg.major_frozen_scn_.get_val_for_tx(), table_schema, compat_mode, table_store_flag, table_handle, freezer))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(lob_meta_tablet_id), K(lob_piece_tablet_id), K(index_tablet_array),
        K(arg), K(create_scn), K(table_schema), K(compat_mode), K(table_store_flag));
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, mem_addr, tablet_handle, tablet_handle))) {
    LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(mem_addr), K(tablet_handle));
  } else {
    LOG_INFO("prepare to set tx data", K(ret), K(ls_id), K(tablet_id));

    ObTabletTxMultiSourceDataUnit tx_data;
    tx_data.tx_id_ = tx_id;
    tx_data.tx_scn_ = scn;
    tx_data.tablet_status_ = ObTabletStatus::CREATING;

    if (OB_FAIL(tablet->set_tx_data(tx_data, trans_flags.for_replay_, ref_op))) {
      LOG_WARN("failed to set tx data", K(ret), K(tx_data), K(trans_flags), K(ref_op));
    } else if (OB_FAIL(t3m->insert_pinned_tablet(key))) {
      LOG_WARN("failed to insert in tx tablet", K(ret), K(key));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_need_create_empty_major_sstable(
    const ObTableSchema &table_schema,
    bool &need_create_sstable)
{
  int ret = OB_SUCCESS;
  need_create_sstable = false;
  if (OB_UNLIKELY(!table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_schema));
  } else {
    need_create_sstable = !(table_schema.is_user_hidden_table() || (table_schema.is_index_table() && !table_schema.can_read_index()));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::build_create_sstable_param(
    const ObTableSchema &table_schema,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!table_schema.is_valid()
      || !tablet_id.is_valid()
      || OB_INVALID_VERSION == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_schema), K(snapshot_version));
  } else if (OB_FAIL(table_schema.get_encryption_id(param.encrypt_id_))) {
    LOG_WARN("fail to get_encryption_id", K(ret), K(table_schema));
  } else {
    param.master_key_id_ = table_schema.get_master_key_id();
    MEMCPY(param.encrypt_key_, table_schema.get_encrypt_key_str(), table_schema.get_encrypt_key_len());

    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    param.table_key_.tablet_id_ = tablet_id;
    param.table_key_.version_range_.snapshot_version_ = snapshot_version;
    param.max_merged_trans_version_ = snapshot_version;

    param.schema_version_ = table_schema.get_schema_version();
    param.create_snapshot_version_ = 0;
    param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
    param.progressive_merge_step_ = 0;

    param.table_mode_ = table_schema.get_table_mode_struct();
    param.index_type_ = table_schema.get_index_type();
    param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = (ObRowStoreType::ENCODING_ROW_STORE == table_schema.get_row_store_type()
        ? ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE : table_schema.get_row_store_type());
    param.latest_row_store_type_ = table_schema.get_row_store_type();
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.data_checksum_ = 0;
    param.occupy_size_ = 0;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_.set_min();
    param.original_size_ = 0;
    param.ddl_scn_.set_min();
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    if (OB_FAIL(table_schema.get_store_column_count(param.column_cnt_, true/*is_full*/))) {
      LOG_WARN("fail to get stored col cnt of table schema", K(ret), K(table_schema));
    } else if (FALSE_IT(param.column_cnt_ += multi_version_col_cnt)) {
    } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_,
        param.column_checksums_))) {
      LOG_WARN("fail to fill column checksum for empty major", K(ret), K(param));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::set_tablet_final_status(
    ObTabletHandle &tablet_handle,
    const ObTabletStatus::Status status,
    const SCN &tx_scn,
    const SCN &memtable_scn,
    const bool for_replay,
    const MemtableRefOp ref_op)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  const ObLSID &ls_id = tablet->get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
  ObTabletTxMultiSourceDataUnit tx_data;
  ObTabletStatusChecker checker(*tablet);

  if (ObTabletStatus::NORMAL != status && ObTabletStatus::DELETED != status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(status));
  } else if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!ObTabletStatus::is_valid_status(tx_data.tablet_status_, status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid status", K(ret), "current_status", tx_data.tablet_status_, "target_status", status);
  } else {
    tx_data.tx_id_ = ObTabletCommon::FINAL_TX_ID;
    tx_data.tablet_status_ = status;
    tx_data.tx_scn_ = tx_scn;

    if (OB_FAIL(checker.wake_up(tx_data, memtable_scn, for_replay, ref_op))) {
      LOG_WARN("failed to wake up", K(ret), KPC(tablet), K(status),
          K(tx_scn), K(memtable_scn), K(for_replay), K(ref_op));
    } else {
      LOG_INFO("succeeded to set tablet final status", K(ret), K(ls_id), K(tablet_id), K(status),
          K(tx_scn), K(memtable_scn), K(for_replay), K(ref_op));
    }
  }

  return ret;
}

bool ObTabletCreateDeleteHelper::check_tablet_status(
    const ObTabletHandle &tablet_handle,
    const ObTabletStatus::Status expected_status)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTxMultiSourceDataUnit tx_data;

  if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret));
  } else if (expected_status != tx_data.tablet_status_) {
    b_ret = false;
  }

  if (OB_FAIL(ret)) {
    b_ret = false;
  }

  return b_ret;
}

int ObTabletCreateDeleteHelper::check_tablet_status(
    const ObBatchRemoveTabletArg &arg,
    bool &normal)
{
  int ret = OB_SUCCESS;
  normal = true;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && normal && i < arg.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
    key.tablet_id_ = tablet_id;
    if (OB_FAIL(check_and_get_tablet(key, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_TIMEOUT_US))) {
      if (OB_TIMEOUT == ret) {
        normal = false;
        ret = OB_SUCCESS;
        LOG_INFO("tablet is not in NORMAL status", K(ret), K(key));
      } else if (OB_TABLET_NOT_EXIST == ret) {
        normal = false;
        ret = OB_SUCCESS;
        LOG_INFO("tablet does not exist or may be deleted", K(ret), K(key));
      } else {
        LOG_WARN("failed to check and get tablet", K(ret), K(key));
      }
    }
  }

  return ret;
}

ObSimpleBatchCreateTabletArg::ObSimpleBatchCreateTabletArg(const ObBatchCreateTabletArg &arg)
  : arg_(arg)
{
}

int64_t ObSimpleBatchCreateTabletArg::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObSimpleBatchCreateTabletArg");
    J_COLON();
    J_KV("id", arg_.id_,
         "major_frozen_scn", arg_.major_frozen_scn_,
         "total_tablet_cnt", arg_.get_tablet_count());
    J_COMMA();

    BUF_PRINTF("tablets");
    J_COLON();
    J_OBJ_START();
    for (int64_t i = 0; i < arg_.tablets_.count(); ++i) {
      const ObCreateTabletInfo &info = arg_.tablets_.at(i);
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      BUF_PRINTF("[%ld] [", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF("] ");
      J_KV("data_tablet_id", info.data_tablet_id_,
           "tablet_ids", info.tablet_ids_,
           "compat_mode", info.compat_mode_,
           "is_create_bind_hidden_tablets", info.is_create_bind_hidden_tablets_);
    }
    J_NEWLINE();
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

int ObTabletCreateDeleteHelper::prepare_data_for_tablet_status(const ObTabletID &tablet_id, const ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls.get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletTxMultiSourceDataUnit tx_data;

  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(key));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", KR(ret));
  } else if (OB_FAIL(tablet->prepare_data(tx_data, trans_flags))) {
    LOG_WARN("failed to prepare tx data", KR(ret), K(tx_data));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::prepare_data_for_binding_info(const ObTabletID &tablet_id, const ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls.get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletBindingInfo binding_info;

  if (OB_FAIL(get_tablet(key, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(key));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_ddl_data(binding_info))) {
    LOG_WARN("failed to get tx data", KR(ret));
  } else if (OB_FAIL(tablet->prepare_data(binding_info, trans_flags))) {
    LOG_WARN("failed to prepare binding info", KR(ret), K(binding_info));
  }

  return ret;
}

void ObTabletCreateDeleteHelper::print_memtables_for_table(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  common::ObSArray<storage::ObITable *> memtables;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_handle is not valid", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_memtables(memtables, true))) {
    LOG_WARN("failed to get_memtables", K(ret), K(tablet_handle));
  } else {
    LOG_INFO("memtables print", K(memtables), K(tablet_handle));
  }
}

} // namespace storage
} // namespace oceanbase
