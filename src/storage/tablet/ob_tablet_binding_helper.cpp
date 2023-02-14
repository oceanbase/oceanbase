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

#include "storage/tablet/ob_tablet_binding_helper.h"

#include "lib/ob_abort.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_rpc_struct.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{

ObTabletBindingInfo::ObTabletBindingInfo()
  : redefined_(false),
    snapshot_version_(INT64_MAX),
    schema_version_(INT64_MAX),
    data_tablet_id_(),
    hidden_tablet_ids_(),
    lob_meta_tablet_id_(),
    lob_piece_tablet_id_()
{
}

void ObTabletBindingInfo::reset()
{
  redefined_ = false;
  snapshot_version_ = INT64_MAX;
  schema_version_ = INT64_MAX;
  data_tablet_id_.reset();
  hidden_tablet_ids_.reset();
  lob_meta_tablet_id_.reset();
  lob_piece_tablet_id_.reset();
}

bool ObTabletBindingInfo::is_valid() const
{
  bool valid = true;

  if (INT64_MAX == snapshot_version_) {
    valid = false;
  } else if (INT64_MAX == schema_version_) {
    valid = false;
  }

  return valid;
}

int ObTabletBindingInfo::assign(const ObTabletBindingInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hidden_tablet_ids_.assign(other.hidden_tablet_ids_))) {
    LOG_WARN("failed to assign hidden tablet ids", K(ret));
  } else {
    redefined_ = other.redefined_;
    snapshot_version_ = other.snapshot_version_;
    schema_version_ = other.schema_version_;
    data_tablet_id_ = other.data_tablet_id_;
    lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
  }
  return ret;
}

int ObTabletBindingInfo::deep_copy(
    const memtable::ObIMultiSourceDataUnit *src,
    ObIAllocator *allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src info", K(ret));
  } else if (OB_UNLIKELY(src->type() != type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret));
  } else if (OB_FAIL(assign(*static_cast<const ObTabletBindingInfo *>(src)))) {
    LOG_WARN("failed to copy tablet binding info", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletBindingInfo, redefined_, snapshot_version_, schema_version_, data_tablet_id_, hidden_tablet_ids_, lob_meta_tablet_id_, lob_piece_tablet_id_);

ObBatchUnbindTabletArg::ObBatchUnbindTabletArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    schema_version_(OB_INVALID_VERSION),
    orig_tablet_ids_(),
    hidden_tablet_ids_()
{
}

int ObBatchUnbindTabletArg::assign(const ObBatchUnbindTabletArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(orig_tablet_ids_.assign(other.orig_tablet_ids_))) {
    LOG_WARN("failed to assign orig tablet ids", K(ret));
  } else if (OB_FAIL(hidden_tablet_ids_.assign(other.hidden_tablet_ids_))) {
    LOG_WARN("failed to assign hidden tablet ids", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    schema_version_ = other.schema_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchUnbindTabletArg, tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_);

// lock non-creating orig tablet and data tablets
int ObTabletBindingHelper::lock_tablet_binding_for_create(
    const ObBatchCreateTabletArg &arg,
    ObLS &ls,
    const ObMulSourceDataNotifyArg &trans_flags,
    ObTabletBindingPrepareCtx &ctx)
{
  int ret = OB_SUCCESS;
  ctx.skip_idx_.reset();
  ctx.last_idx_ = -1;
  ObTabletBindingHelper helper(ls, trans_flags);
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    // for mixed tablets, no need to lock data tablet because it will be locked on creating
    if (is_contain(ctx.skip_idx_, i)) {
      // do nothing
    } else if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
        int64_t aux_idx = -1;
        if (ObTabletCreateDeleteHelper::find_related_aux_info(arg, info.tablet_ids_.at(j), aux_idx)
            && OB_FAIL(ctx.skip_idx_.push_back(aux_idx))) {
          LOG_WARN("failed to push related aux idx", K(ret), K(aux_idx));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(helper.lock_tablet_binding(info.data_tablet_id_))) {
          LOG_WARN("failed to lock orig tablet binding", K(ret));
        } else {
          ctx.last_idx_ = i;
        }
      }
    } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info)) {
      if (has_lob_tablets(arg, info)) {
        if (OB_FAIL(helper.lock_tablet_binding(info.data_tablet_id_))) {
          LOG_WARN("failed to lock tablet binding", K(ret));
        } else {
          ctx.last_idx_ = i;
        }
      }
    }
  }
  return ret;
}

void ObTabletBindingHelper::rollback_lock_tablet_binding_for_create(
    const ObBatchCreateTabletArg &arg,
    ObLS &ls,
    const ObMulSourceDataNotifyArg &prepare_trans_flags,
    const ObTabletBindingPrepareCtx &ctx)
{
  if (arg.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    ObMulSourceDataNotifyArg trans_flags = prepare_trans_flags;
    trans_flags.notify_type_ = NotifyType::ON_ABORT;
    ObTabletBindingHelper helper(ls, trans_flags);
    for (int64_t i = 0; i <= ctx.last_idx_ && i < arg.tablets_.count(); i++) {
      const ObCreateTabletInfo &info = arg.tablets_[i];
      if (is_contain(ctx.skip_idx_, i)) {
        // do nothing
      } else if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
        if (OB_TMP_FAIL(helper.unlock_tablet_binding(info.data_tablet_id_))) {
          LOG_ERROR_RET(tmp_ret, "failed to lock orig tablet binding", K(tmp_ret));
        }
      } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info)) {
        if (has_lob_tablets(arg, info) && OB_TMP_FAIL(helper.unlock_tablet_binding(info.data_tablet_id_))) {
          LOG_ERROR_RET(tmp_ret, "failed to lock tablet binding", K(tmp_ret));
        }
      }
    }
  }
  return;
}

// set log scn for non-creating orig tablets and data tablets
int ObTabletBindingHelper::set_scn_for_create(const ObBatchCreateTabletArg &arg, ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObSArray<int64_t> skip_idx;
  ObTabletBindingHelper helper(ls, trans_flags);
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    if (is_contain(skip_idx, i)) {
      // do nothing
    } else if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
      if (OB_FAIL(helper.set_scn(info.data_tablet_id_))) {
        LOG_WARN("failed to set log ts for orig tablet", K(ret));
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
        int64_t aux_idx = -1;
        if (ObTabletCreateDeleteHelper::find_related_aux_info(arg, info.tablet_ids_.at(j), aux_idx)
            && OB_FAIL(skip_idx.push_back(aux_idx))) {
          LOG_WARN("failed to push related aux idx", K(ret), K(aux_idx));
        }
      }
    } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info)) {
      if (has_lob_tablets(arg, info) && OB_FAIL(helper.set_scn(info.data_tablet_id_))) {
        LOG_WARN("failed to lock tablet binding", K(ret));
      }
    }
  }
  return ret;
}

// unlock non-creating orig tablets and data tablets
int ObTabletBindingHelper::unlock_tablet_binding_for_create(const ObBatchCreateTabletArg &arg, ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObSArray<int64_t> skip_idx;
  ObTabletBindingHelper helper(ls, trans_flags);
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    if (is_contain(skip_idx, i)) {
      // do nothing
    } else if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
      if (OB_FAIL(helper.unlock_tablet_binding(info.data_tablet_id_))) {
        LOG_WARN("failed to lock tablet binding", K(ret));
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < info.tablet_ids_.count(); ++j) {
        int64_t aux_idx = -1;
        if (ObTabletCreateDeleteHelper::find_related_aux_info(arg, info.tablet_ids_.at(j), aux_idx)
            && OB_FAIL(skip_idx.push_back(aux_idx))) {
          LOG_WARN("failed to push related aux idx", K(ret), K(aux_idx));
        }
      }
    } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info)) {
      if (has_lob_tablets(arg, info) && OB_FAIL(helper.unlock_tablet_binding(info.data_tablet_id_))) {
        LOG_WARN("failed to unlock tablet binding", K(ret));
      }
    }
  }
  return ret;
}

// bind aux and hidden tablets to creating and non-creating data tablet
int ObTabletBindingHelper::modify_tablet_binding_for_create(
    const ObBatchCreateTabletArg &arg,
    ObLS &ls,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> empty_array;
  ObTabletBindingHelper helper(ls, trans_flags);
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    bool need_modify = false;
    bool tablet_ids_as_aux_tablets = false;
    if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
      need_modify = true;
    } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info) || ObTabletCreateDeleteHelper::is_mixed_tablets(info)) {
      if (has_lob_tablets(arg, info)) {
        need_modify = true;
        tablet_ids_as_aux_tablets = true;
      }
    }
    if (OB_SUCC(ret) && need_modify) {
      ObTabletHandle handle;
      if (OB_FAIL(helper.get_tablet(info.data_tablet_id_, handle))) {
        if (OB_NO_NEED_UPDATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret));
        }
      } else if (tablet_ids_as_aux_tablets) {
        if (OB_FAIL(add_tablet_binding(arg, info, handle, info.tablet_ids_, empty_array, trans_flags))) {
          LOG_WARN("failed to modify tablet binding", K(ret), K(info));
        }
      } else {
        if (OB_FAIL(add_tablet_binding(arg, info, handle, empty_array, info.tablet_ids_, trans_flags))) {
          LOG_WARN("failed to modify tablet binding", K(ret), K(info));
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::add_tablet_binding(
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &tinfo,
    ObTabletHandle &orig_tablet_handle,
    const ObIArray<ObTabletID> &aux_tablet_ids,
    const ObIArray<ObTabletID> &hidden_tablet_ids,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;
  ObTablet *tablet = orig_tablet_handle.get_obj();
  const ObTabletID &orig_tablet_id = tablet->get_tablet_meta().tablet_id_;
  bool is_locked = false;
  if (OB_FAIL(check_is_locked(orig_tablet_handle, trans_flags.tx_id_, is_locked))) {
    LOG_WARN("failed to check is locked", K(ret));
  } else if (is_locked) {
    ObTabletBindingInfo info;
    if (OB_FAIL(tablet->get_ddl_data(info))) {
      LOG_WARN("failed to get ddl data", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < aux_tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = aux_tablet_ids.at(i);
      if (tablet_id != orig_tablet_id) {
        if (arg.table_schemas_.at(tinfo.table_schema_index_.at(i)).is_aux_lob_meta_table()) {
          info.lob_meta_tablet_id_ = tablet_id;
        } else if (arg.table_schemas_.at(tinfo.table_schema_index_.at(i)).is_aux_lob_piece_table()) {
          info.lob_piece_tablet_id_ = tablet_id;
        } else {
          // do not maintain index tablet ids
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < hidden_tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = hidden_tablet_ids.at(i);
      if (tablet_id != orig_tablet_id && !is_contain(info.hidden_tablet_ids_, tablet_id)
          && OB_FAIL(info.hidden_tablet_ids_.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablet->set_multi_data_for_commit(info, trans_flags.scn_, trans_flags.for_replay_, MemtableRefOp::NONE))) {
        LOG_WARN("failed to save multi source data", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::lock_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &batch_arg, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(batch_arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObTabletBindingHelper helper(*ls_handle.get_ls(), trans_flags);
    int64_t last_orig_idx = -1;
    int64_t last_hidden_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.orig_tablet_ids_.count(); i++) {
      if (OB_FAIL(helper.lock_tablet_binding(batch_arg.orig_tablet_ids_[i]))) {
        LOG_WARN("failed to lock tablet binding", K(ret));
      } else {
        last_orig_idx = i;
      }
    }
    if (batch_arg.is_redefined()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.hidden_tablet_ids_.count(); i++) {
        if (OB_FAIL(helper.lock_tablet_binding(batch_arg.hidden_tablet_ids_[i]))) {
          LOG_WARN("failed to lock tablet binding", K(ret));
        } else {
          last_hidden_idx = i;
        }
      }
    }

    if (OB_FAIL(ret) && !trans_flags.for_replay_) {
      int tmp_ret = OB_SUCCESS;
      ObMulSourceDataNotifyArg rollback_trans_flags = trans_flags;
      rollback_trans_flags.notify_type_ = NotifyType::ON_ABORT;
      ObTabletBindingHelper rollback_helper(*ls_handle.get_ls(), rollback_trans_flags);
      for (int64_t i = 0; i <= last_orig_idx; i++) {
        if (OB_TMP_FAIL(rollback_helper.unlock_tablet_binding(batch_arg.orig_tablet_ids_[i]))) {
          LOG_ERROR("failed to unlock tablet binding", K(tmp_ret));
        }
      }
      for (int64_t i = 0; i <= last_hidden_idx; i++) {
        if (OB_TMP_FAIL(rollback_helper.unlock_tablet_binding(batch_arg.hidden_tablet_ids_[i]))) {
          LOG_ERROR("failed to unlock tablet binding", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::set_scn_for_unbind(const ObBatchUnbindTabletArg &batch_arg, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(batch_arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObTabletBindingHelper helper(*ls_handle.get_ls(), trans_flags);
    if (OB_FAIL(helper.set_scn(batch_arg.orig_tablet_ids_))) {
      LOG_WARN("failed to lock tablet binding", K(ret));
    } else if (batch_arg.is_redefined()) {
      if (OB_FAIL(helper.set_scn(batch_arg.hidden_tablet_ids_))) {
        LOG_WARN("failed to lock tablet binding", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::prepare_data_for_tablet(const ObTabletID &tablet_id, const ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  bool skip = false;
  if (trans_flags.for_replay_
      && OB_FAIL(check_skip_tx_end(tablet_id, ls, trans_flags, skip))) {
    LOG_WARN("fail to check_skip_tx_end", K(ret), K(tablet_id), K(trans_flags));
  } else if (skip) {
    LOG_INFO("skip tx_end_unbind for replay", K(tablet_id), K(trans_flags));
  }
  // prepare tablet status_info of orig_tablet
  else if (OB_FAIL(ObTabletCreateDeleteHelper::prepare_data_for_tablet_status(tablet_id, ls, trans_flags))) {
    LOG_WARN("failed to prepare_data_for_tablet_status", KR(ret), K(tablet_id), K(ls));
  }
  // prepare tablet binding_info of orig_tablet
  else if (OB_FAIL(ObTabletCreateDeleteHelper::prepare_data_for_binding_info(tablet_id, ls, trans_flags))) {
    LOG_WARN("failed to prepare_data_for_binding_info", KR(ret), K(tablet_id), K(ls));
  }

  return ret;
}

int ObTabletBindingHelper::check_skip_tx_end(const ObTabletID &tablet_id, const ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags, bool &skip)
{
  int ret = OB_SUCCESS;
  skip = false;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletTxMultiSourceDataUnit tx_data;
  ObTabletBindingHelper helper(ls, trans_flags);

  if (!trans_flags.scn_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(trans_flags));
  } else if (OB_FAIL(helper.get_tablet(tablet_id, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      skip = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", KR(ret));
  } else if (tx_data.tx_scn_ >= trans_flags.scn_) {
    skip = true;
  }
  return ret;
}

int ObTabletBindingHelper::on_tx_end_for_modify_tablet_binding(const ObBatchUnbindTabletArg &batch_arg, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(batch_arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObLS &ls = *ls_handle.get_ls();
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.orig_tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = batch_arg.orig_tablet_ids_.at(i);
      if (OB_FAIL(prepare_data_for_tablet(tablet_id, ls, trans_flags))) {
        LOG_WARN("failed to prepare_data_for_tablet", K(tablet_id), K(ret), K(batch_arg));
      }
    }

    if (batch_arg.is_redefined()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.hidden_tablet_ids_.count(); i++) {
        const ObTabletID &tablet_id = batch_arg.hidden_tablet_ids_.at(i);
        if (OB_FAIL(prepare_data_for_tablet(tablet_id, ls, trans_flags))) {
          LOG_WARN("failed to prepare_data_for_tablet", K(tablet_id), K(ret), K(batch_arg));
        }
      }
    }
  }

  return ret;
}

int ObTabletBindingHelper::unlock_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &batch_arg, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(batch_arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObTabletBindingHelper helper(*ls_handle.get_ls(), trans_flags);
    if (OB_FAIL(helper.unlock_tablet_binding(batch_arg.orig_tablet_ids_))) {
      LOG_WARN("failed to lock tablet binding", K(ret));
    } else if (batch_arg.is_redefined()) {
      if (OB_FAIL(helper.unlock_tablet_binding(batch_arg.hidden_tablet_ids_))) {
        LOG_WARN("failed to lock tablet binding", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::fix_unsynced_cnt_for_binding_info(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletBindingInfo binding_info;
  const SCN scn = SCN::max_scn();

  if (OB_FAIL(get_tablet(tablet_id, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    }
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_ddl_data(binding_info))) {
    LOG_WARN("failed to get ddl data", KR(ret));
  } else if (OB_FAIL(tablet->clear_unsynced_cnt_for_tx_end_if_need(binding_info, scn, trans_flags_.for_replay_))) {
    LOG_WARN("failed to prepare binding info", KR(ret), K(binding_info));
  }

  return ret;
}

int ObTabletBindingHelper::fix_binding_info_for_create_tablets(const ObBatchCreateTabletArg &arg, const ObLS &ls, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  // fix data_tablet binding_info for pure_aux_table
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
    const ObCreateTabletInfo &info = arg.tablets_[i];
    bool need_modify = false;
    if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
      need_modify = true;
    } else if (ObTabletCreateDeleteHelper::is_pure_aux_tablets(info) || ObTabletCreateDeleteHelper::is_mixed_tablets(info)) {
      if (has_lob_tablets(arg, info)) {
        need_modify = true;
      }
    }
    if (OB_SUCC(ret) && need_modify) {
      ObTabletBindingHelper helper(ls, trans_flags);
      if (OB_FAIL(helper.fix_unsynced_cnt_for_binding_info(info.data_tablet_id_))) {
        LOG_WARN("failed to fix_unsynced_cnt_for_binding_info", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::fix_binding_info_for_modify_tablet_binding(
    const ObBatchUnbindTabletArg &batch_arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(get_ls(batch_arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObTabletBindingHelper helper(*ls_handle.get_ls(), trans_flags);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.orig_tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = batch_arg.orig_tablet_ids_.at(i);
      if (OB_FAIL(helper.fix_unsynced_cnt_for_binding_info(tablet_id))) {
        LOG_WARN("failed to fix_unsynced_cnt_for_binding_info", K(tablet_id), K(ret), K(batch_arg));
      }
    }

    if (batch_arg.is_redefined()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_arg.hidden_tablet_ids_.count(); i++) {
        const ObTabletID &tablet_id = batch_arg.hidden_tablet_ids_.at(i);
        if (OB_FAIL(helper.fix_unsynced_cnt_for_binding_info(tablet_id))) {
          LOG_WARN("failed to fix_unsynced_cnt_for_binding_info", K(tablet_id), K(ret), K(batch_arg));
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::modify_tablet_binding_for_unbind(
    const ObBatchUnbindTabletArg &arg,
    const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  const ObTransID &tx_id = trans_flags.tx_id_;
  const SCN scn = trans_flags.scn_;
  const bool for_replay = trans_flags.for_replay_;
  const SCN commit_version = trans_flags.trans_version_;
  if (OB_FAIL(get_ls(arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObTabletBindingHelper helper(*ls_handle.get_ls(), trans_flags);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    bool is_locked = false;
    ObTabletBindingInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.orig_tablet_ids_.count(); i++) {
      if (OB_FAIL(helper.get_tablet(arg.orig_tablet_ids_.at(i), tablet_handle))) {
        if (OB_NO_NEED_UPDATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret));
        }
      } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_FAIL(check_is_locked(tablet_handle, tx_id, is_locked))) {
        LOG_WARN("failed to check is locked", K(ret));
      } else if (!is_locked) {
        LOG_WARN("already commit", K(ret), K(tx_id));
      } else if (OB_FAIL(tablet->get_ddl_data(info))) {
        LOG_WARN("failed to get ddl data", K(ret));
      } else {
        info.hidden_tablet_ids_.reset();
        if (arg.is_redefined()) {
          info.redefined_ = true;
          info.snapshot_version_ = commit_version.get_val_for_tx();
        }
        if (OB_FAIL(tablet->set_multi_data_for_commit(info, scn, for_replay, MemtableRefOp::NONE))) {
          LOG_WARN("failed to save tablet binding info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && arg.is_redefined()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.hidden_tablet_ids_.count(); i++) {
        const ObTabletID &tablet_id = arg.hidden_tablet_ids_.at(i);
        ObTabletBindingInfo info;
        if (OB_FAIL(helper.get_tablet(tablet_id, tablet_handle))) {
          if (OB_NO_NEED_UPDATE == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get tablet", K(ret));
          }
        } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
        } else if (OB_FAIL(check_is_locked(tablet_handle, tx_id, is_locked))) {
          LOG_WARN("failed to check is locked", K(ret));
        } else if (!is_locked) {
          LOG_WARN("already commit", K(ret), K(tx_id));
        } else if (OB_FAIL(tablet->get_ddl_data(info))) {
          LOG_WARN("failed to get ddl data", K(ret));
        } else {
          info.redefined_ = false;
          info.snapshot_version_ = commit_version.get_val_for_tx();
          info.schema_version_ = arg.schema_version_;
          if (OB_FAIL(tablet->set_multi_data_for_commit(info, scn, for_replay, MemtableRefOp::NONE))) {
            LOG_WARN("failed to save tablet binding info", K(ret));
          } else if (OB_FAIL(tablet->set_redefined_schema_version_in_tablet_pointer(info.schema_version_))) {
            LOG_WARN("failed to set redefined schema version in tablet pointer", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::check_schema_version(ObIArray<ObTabletHandle> &handles, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < handles.count(); i++) {
    if (OB_FAIL(check_schema_version(handles.at(i), schema_version))) {
      LOG_WARN("failed to check schema version", K(ret));
    }
  }
  return ret;
}

int ObTabletBindingHelper::check_schema_version(ObTabletHandle &handle, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = handle.get_obj();
  int64_t redefined_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(tablet->get_redefined_schema_version_in_tablet_pointer(redefined_schema_version))) {
    LOG_WARN("failed to get tablet binding info", K(ret));
  } else if (OB_UNLIKELY(schema_version < redefined_schema_version)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("use stale schema before ddl", K(ret), K(tablet->get_tablet_meta().tablet_id_), K(redefined_schema_version), K(schema_version));
  }
  return ret;
}

int ObTabletBindingHelper::check_snapshot_readable(ObTabletHandle &handle, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = handle.get_obj();
  TCRWLock &lock = tablet->get_rw_lock();
  TCRLockGuard guard(lock);
  ObTabletBindingInfo info;
  if (OB_FAIL(tablet->get_ddl_data(info))) {
    LOG_WARN("failed to get tablet binding info", K(ret));
  } else if (OB_UNLIKELY(info.redefined_ && snapshot_version >= info.snapshot_version_)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("read data after ddl, need to retry on new tablet", K(ret), K(tablet->get_tablet_meta().tablet_id_), K(snapshot_version), K(info));
  } else if (OB_UNLIKELY(!info.redefined_ && snapshot_version < info.snapshot_version_)) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read data before ddl", K(ret), K(tablet->get_tablet_meta().tablet_id_), K(snapshot_version), K(info));
  }
  return ret;
}

int ObTabletBindingHelper::get_ls(const ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService* ls_srv = nullptr;
  ObLS *ls = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls", KR(ret));
  }
  return ret;
}

int ObTabletBindingHelper::get_tablet(const ObTabletID &tablet_id, ObTabletHandle &handle) const
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  if (trans_flags_.for_replay_) {
    ret = replay_get_tablet(key, handle);
  } else {
    ret = ObTabletCreateDeleteHelper::get_tablet(key, handle);
  }
  if (OB_TABLET_NOT_EXIST == ret) {
    ret = OB_NO_NEED_UPDATE;
    LOG_INFO("tablet removed", K(ret), K(key), K(trans_flags_));
  } else if (OB_EAGAIN == ret) {
    // do nothing
  } else if (OB_SUCCESS == ret) {
    ObTabletTxMultiSourceDataUnit tx_data;
    if (OB_FAIL(handle.get_obj()->get_tx_data(tx_data))) {
      LOG_WARN("failed to get tx data", K(ret), K(key));
    } else if (SCN::invalid_scn() != trans_flags_.scn_ && trans_flags_.scn_ <= tx_data.tx_scn_) {
      ret = OB_NO_NEED_UPDATE;
      LOG_INFO("tablet frozen", K(ret), K(key), K(trans_flags_), K(tx_data));
    }
  } else {
    LOG_WARN("failed to get tablet", K(ret), K(key), K(trans_flags_));
  }
  return ret;
}

int ObTabletBindingHelper::replay_get_tablet(const ObTabletMapKey &key, ObTabletHandle &handle) const
{
  // NOTICE: temporarily used, will be removed later!
  int ret = OB_SUCCESS;
  const SCN tablet_change_checkpoint_scn = ls_.get_tablet_change_checkpoint_scn();
  ObTabletHandle tablet_handle;

  if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet", KR(ret), K(key));
    } else if (trans_flags_.scn_ <= tablet_change_checkpoint_scn) {
      LOG_WARN("tablet already gc", KR(ret), K(key), K(trans_flags_), K(tablet_change_checkpoint_scn));
    } else {
      LOG_INFO("tablet already gc, trans_flags_.scn_ is not less than tablet_change_checkpoint_scn",
               KR(ret), K(key), K(trans_flags_), K(tablet_change_checkpoint_scn));
    }
  } else {
    ObTabletTxMultiSourceDataUnit tx_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data))) {
      LOG_WARN("failed to get tablet tx data", KR(ret), K(tablet_handle));
    } else if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_INFO("tablet is already deleted", KR(ret), K(key), K(tx_data));
    }
  }

  if (OB_SUCC(ret)) {
    handle = tablet_handle;
  }

  return ret;
}

int ObTabletBindingHelper::check_is_locked(ObTabletHandle &handle, const ObTransID &tx_id, bool &is_locked)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = handle.get_obj();
  TCRWLock &lock = tablet->get_rw_lock();
  TCRLockGuard guard(lock);
  ObTabletTxMultiSourceDataUnit tx_data;
  if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret));
  } else {
    is_locked = tx_id == tx_data.tx_id_;
  }
  return ret;
}

bool ObTabletBindingHelper::has_lob_tablets(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info)
{
  bool has_lob = false;
  for (int64_t i = 0; !has_lob && i < info.tablet_ids_.count(); i++) {
    const ObTableSchema &table_schema = arg.table_schemas_.at(info.table_schema_index_.at(i));
    if (table_schema.is_aux_lob_meta_table() || table_schema.is_aux_lob_piece_table()) {
      has_lob = true;
    }
  }
  return has_lob;
}

// for prepare
int ObTabletBindingHelper::lock_and_set_tx_data(ObTabletHandle &handle, ObTabletTxMultiSourceDataUnit &tx_data, const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTablet *tablet = handle.get_obj();
  const ObLSID &ls_id = tablet->get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);

  const SCN scn = for_replay ? tx_data.tx_scn_ : SCN::max_scn();
  const MemtableRefOp ref_op = for_replay ? MemtableRefOp::NONE : MemtableRefOp::INC_REF;
  ObTabletTxMultiSourceDataUnit old_tx_data;
  if (OB_FAIL(tablet->get_tx_data(old_tx_data))) {
    LOG_WARN("failed to get tx data", K(ret));
  } else {
    const ObTransID &old_tx_id = old_tx_data.tx_id_;
    bool need_update = true;
    if (!old_tx_id.is_valid()) {
      // do nothing
    } else if (old_tx_id == tx_data.tx_id_) {
      need_update = false;
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("tablet binding locked by others", K(ret), K(tablet_id), K(tx_data), K(old_tx_data));
    }
    if (OB_FAIL(ret)) {
    } else if (need_update && OB_FAIL(tablet->set_tx_data(tx_data, scn, for_replay,
        ref_op, false/*is_callback*/))) {
      LOG_WARN("failed to save msd", K(ret), K(tx_data), K(scn), K(for_replay), K(ref_op));
    } else if (OB_FAIL(t3m->insert_pinned_tablet(key))) {
      LOG_WARN("failed to insert in tx tablet", K(ret), K(key));
    }
  }
  return ret;
}

// for prepare, reentrant
int ObTabletBindingHelper::lock_tablet_binding(ObTabletHandle &handle, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTransID &tx_id = trans_flags.tx_id_;
  const SCN scn = trans_flags.scn_;
  const bool for_replay = trans_flags.for_replay_;
  ObTablet *tablet = handle.get_obj();
  const ObTabletMapKey key(tablet->tablet_meta_.ls_id_, tablet->tablet_meta_.tablet_id_);
  ObTabletTxMultiSourceDataUnit tx_data;
  if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret));
  } else {
    const ObTransID old_tx_id = tx_data.tx_id_;
    const SCN old_scn = tx_data.tx_scn_;
    bool need_update = true;
    const SCN memtable_scn = for_replay ? scn : SCN::max_scn();;
    const MemtableRefOp ref_op = for_replay ? MemtableRefOp::NONE : MemtableRefOp::INC_REF;
    if (!old_tx_id.is_valid()) {
      tx_data.tx_id_ = tx_id;
      tx_data.tx_scn_ = for_replay ? scn : old_scn;
    } else if (old_tx_id == tx_id) {
      need_update = false; // already same
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("tablet binding locked by others", K(ret), K(tx_id), K(scn), K(tablet->get_tablet_meta().tablet_id_), K(tx_data));
    }
    if (OB_FAIL(ret)) {
    } else if (need_update && OB_FAIL(tablet->set_tx_data(tx_data, memtable_scn, for_replay,
        ref_op, false/*is_callback*/))) {
      LOG_WARN("failed to save tx data", K(ret), K(tx_data), K(scn), K(for_replay), K(ref_op));
      if (!for_replay && OB_BLOCK_FROZEN == ret) {
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(t3m->insert_pinned_tablet(key))) {
      LOG_WARN("failed to insert in tx tablet", K(ret), K(key));
    }
  }
  return ret;
}

/// reentrant, lock by tx_id_
int ObTabletBindingHelper::lock_tablet_binding(const ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  if (OB_FAIL(get_tablet(tablet_id, handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FAIL(lock_tablet_binding(handle, trans_flags_))) {
    LOG_WARN("failed to lock tablet binding", K(ret));
  }
  return ret;
}

/// reentrant, lock by tx_id_
int ObTabletBindingHelper::lock_tablet_binding(const ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    if (OB_FAIL(lock_tablet_binding(tablet_ids.at(i)))) {
      LOG_WARN("failed to lock tablet binding", K(ret));
    }
  }
  return ret;
}

// for redo and not replay, fill log ts, not reentrant
int ObTabletBindingHelper::set_scn(ObTabletHandle &handle, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = trans_flags.tx_id_;
  const SCN scn = trans_flags.scn_;
  const bool for_replay = trans_flags.for_replay_;
  ObTablet *tablet = handle.get_obj();
  ObTabletTxMultiSourceDataUnit data;
  if (OB_FAIL(tablet->get_tx_data(data))) {
    LOG_WARN("failed to get data", K(ret));
  } else if (OB_UNLIKELY(data.tx_id_ != tx_id)) {
    ret = OB_ERR_UNEXPECTED;
    ObTabletCreateDeleteHelper::print_memtables_for_table(handle);
    LOG_WARN("cannot set log ts for unlocked tablet", K(ret), K(tx_id), K(data), "tablet_id", tablet->get_tablet_meta().tablet_id_);
  } else if (OB_UNLIKELY(!data.tx_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid log scn", K(ret), K(tx_id), K(scn), K(data));
  } else if (data.tx_scn_ == scn) {
    LOG_WARN("log ts already set, may be bug or retry", K(ret), K(tx_id), K(scn), K(data));
  } else {
    data.tx_scn_ = scn;
    if (OB_FAIL(tablet->set_tx_data(data, scn, for_replay, memtable::MemtableRefOp::DEC_REF, true/*is_callback*/))) {
      LOG_WARN("failed to save msd", K(ret), K(data), K(scn), K(for_replay));
    }
  }
  return ret;
}

int ObTabletBindingHelper::set_scn(const ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  if (OB_FAIL(get_tablet(tablet_id, handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FAIL(set_scn(handle, trans_flags_))) {
    LOG_WARN("failed to set log ts", K(ret));
  }
  return ret;
}

int ObTabletBindingHelper::set_scn(const ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    if (OB_FAIL(set_scn(tablet_ids.at(i)))) {
      LOG_WARN("failed to set log ts", K(ret));
    }
  }
  return ret;
}

int ObTabletBindingHelper::check_need_dec_cnt_for_abort(const ObTabletTxMultiSourceDataUnit &tx_data, bool &need_dec)
{
  int ret = OB_SUCCESS;
  const int cnt = tx_data.get_unsync_cnt_for_multi_data();
  need_dec = false;
  if ((tx_data.is_tx_end() && cnt == 2) || (!tx_data.is_tx_end() && cnt == 1)) {
    need_dec = true;
  } else if ((tx_data.is_tx_end() && cnt == 1) || (!tx_data.is_tx_end() && cnt == 0)) {
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cnt", K(ret), K(tx_data));
  }
  return ret;
}

/// for commit or abort, reentrant for replay
int ObTabletBindingHelper::unlock_tablet_binding(ObTabletHandle &handle, const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTransID &tx_id = trans_flags.tx_id_;
  const SCN scn = trans_flags.scn_;
  const bool for_replay = trans_flags.for_replay_;
  const bool for_commit = trans_flags.notify_type_ == NotifyType::ON_COMMIT;
  ObTablet *tablet = handle.get_obj();
  const ObTabletMapKey key(tablet->tablet_meta_.ls_id_, tablet->tablet_meta_.tablet_id_);
  ObTabletTxMultiSourceDataUnit tx_data;
  if (OB_FAIL(tablet->get_tx_data(tx_data))) {
    LOG_WARN("failed to get tx data", K(ret));
  } else {
    const SCN old_scn = tx_data.tx_scn_;
    if (tx_data.tx_id_ == tx_id) {
      if (for_replay && scn <= old_scn) {
        // replaying procedure, clog ts is smaller than tx log ts, just skip
        LOG_INFO("skip abort create tablet", K(ret), K(trans_flags), K(tx_data));
      } else if (for_commit && OB_UNLIKELY(!scn.is_valid() || !old_scn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid log scn", K(ret), K(tx_id), K(scn), K(old_scn), K(tx_data));
      } else {
        const bool abort_without_redo = !for_commit && !for_replay && !trans_flags.is_redo_synced();
        tx_data.tx_id_ = ObTabletCommon::FINAL_TX_ID;
        tx_data.tx_scn_ = abort_without_redo ? old_scn : scn;
        const SCN memtable_scn = (!scn.is_valid()) ? SCN::max_scn(): scn;
        bool need_dec = false;
        MemtableRefOp ref_op = MemtableRefOp::NONE;
        if (OB_FAIL(check_need_dec_cnt_for_abort(tx_data, need_dec))) {
          LOG_WARN("failed to save tx data", K(ret), K(tx_data), K(scn), K(for_replay));
        } else if (FALSE_IT(ref_op = (need_dec ? MemtableRefOp::DEC_REF : MemtableRefOp::NONE))) {
        } else if (OB_FAIL(tablet->set_tablet_final_status(tx_data, memtable_scn, for_replay, ref_op))) {
          LOG_WARN("failed to save tx data", K(ret), K(tx_data), K(scn), K(for_replay), K(ref_op));
        } else if (OB_FAIL(t3m->erase_pinned_tablet(key))) {
          LOG_WARN("failed to erase in tx tablet", K(ret), K(key));
        }
      }
    } else {
      const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
      LOG_WARN("already unlocked or bug", K(ret), K(tablet_meta), K(scn), K(trans_flags), K(tx_data));
      ObTabletCreateDeleteHelper::print_memtables_for_table(handle);
    }
  }
  return ret;
}

/// reentrant, unlock by tx_id_
int ObTabletBindingHelper::unlock_tablet_binding(const ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  if (OB_FAIL(get_tablet(tablet_id, handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FAIL(unlock_tablet_binding(handle, trans_flags_))) {
    LOG_ERROR("failed to unlock tablet binding", K(ret));
  }
  return ret;
}

/// reentrant, unlock by tx_id_
int ObTabletBindingHelper::unlock_tablet_binding(const ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < tablet_ids.count(); i++) {
    if (OB_TMP_FAIL(unlock_tablet_binding(tablet_ids.at(i)))) {
      LOG_WARN("failed to unlock tablet binding", K(ret), K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
