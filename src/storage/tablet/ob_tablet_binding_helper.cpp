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
#include "storage/tablet/ob_tablet_binding_replay_executor.h"
#include "lib/ob_abort.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_rpc_struct.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/multi_data_source/mds_ctx.h"
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

ObBatchUnbindTabletArg::ObBatchUnbindTabletArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    schema_version_(OB_INVALID_VERSION),
    orig_tablet_ids_(),
    hidden_tablet_ids_(),
    is_old_mds_(false)
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
    is_old_mds_ = other.is_old_mds_;
  }
  return ret;
}

int ObBatchUnbindTabletArg::skip_array_len(const char *buf,
    int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    TRANS_LOG(WARN, "failed to decode array count", K(ret), KP(buf), K(data_len));
  } else if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len), K(pos), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObTabletID tablet_id;
      OB_UNIS_DECODE(tablet_id);
    }
  }
  return ret;
}

int ObBatchUnbindTabletArg::is_old_mds(const char *buf,
    int64_t data_len,
    bool &is_old_mds)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  is_old_mds = false;
  int64_t version = 0;
  int64_t len = 0;
  uint64_t tenant_id;
  share::ObLSID id;
  int64_t schema_version;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, version, len, tenant_id, id, schema_version);
    if (OB_FAIL(ret)) {
    }
    // orig tablets array
    else if (OB_FAIL(skip_array_len(buf, data_len, pos))) {
      TRANS_LOG(WARN, "failed to skip_unis_array_len", K(ret), KP(buf), K(data_len), K(pos));
    }
    // hidden tablets array
    else if (OB_FAIL(skip_array_len(buf, data_len, pos))) {
      TRANS_LOG(WARN, "failed to skip_unis_array_len", K(ret), KP(buf), K(data_len), K(pos));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, is_old_mds);
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObBatchUnbindTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_, is_old_mds_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBatchUnbindTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_, is_old_mds_);
  return len;
}

OB_DEF_DESERIALIZE(ObBatchUnbindTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,  tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_);
  if (OB_SUCC(ret)) {
    if (pos == data_len) {
      is_old_mds_ = true;
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, is_old_mds_);
    }
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

int ObTabletBindingHelper::get_tablet_for_new_mds(const ObLS &ls, const ObTabletID &tablet_id, const share::SCN &replay_scn, ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls.get_ls_id(), tablet_id);
  const bool for_replay = replay_scn.is_valid();
  if (for_replay) {
    const bool replay_allow_tablet_not_exist = true;
    if (OB_FAIL(ls.replay_get_tablet_no_check(tablet_id, replay_scn, replay_allow_tablet_not_exist, handle))) {
      if (OB_OBSOLETE_CLOG_NEED_SKIP == ret) {
        ret = OB_NO_NEED_UPDATE;
        LOG_WARN("clog is obsolete, should skip replay", K(ret));
      } else {
        LOG_WARN("failed to get tablet", K(ret));
      }
    } else if (OB_UNLIKELY(handle.get_obj()->is_empty_shell())) {
      ret = OB_NO_NEED_UPDATE;
      LOG_WARN("tablet is already deleted, need skip", K(ret), K(key));
    }
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  }

  return ret;
}

int ObTabletBindingHelper::has_lob_tablets(const obrpc::ObBatchCreateTabletArg &arg, const obrpc::ObCreateTabletInfo &info, bool &has_lob)
{
  int ret = OB_SUCCESS;
  has_lob = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_lob && i < info.tablet_ids_.count(); i++) {
    const ObCreateTabletSchema *create_tablet_schema = arg.create_tablet_schemas_.at(info.table_schema_index_.at(i));
    if (OB_ISNULL(create_tablet_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("storage is NULL", KR(ret), K(arg));
    } else if (create_tablet_schema->is_aux_lob_meta_table() || create_tablet_schema->is_aux_lob_piece_table()) {
      has_lob = true;
    }
  }
  return ret;
}

int ObTabletBindingHelper::modify_tablet_binding_for_new_mds_create(const ObBatchCreateTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(ObTabletBindingHelper::get_ls(arg.id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    const ObArray<ObTabletID> empty_array;
    ObLS &ls = *ls_handle.get_ls();
    bool has_lob = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
      const ObCreateTabletInfo &info = arg.tablets_[i];
      if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
        if (CLICK_FAIL(bind_hidden_tablet_to_orig_tablet(ls, info, replay_scn, ctx, arg.is_old_mds_))) {
          LOG_WARN("failed to add hidden tablet", K(ret));
        }
      } else if (OB_FAIL(ObTabletBindingHelper::has_lob_tablets(arg, info, has_lob))) {
        LOG_WARN("failed to has_lob_tablets", KR(ret));
      } else if (has_lob) {
        if (CLICK_FAIL(bind_lob_tablet_to_data_tablet(ls, arg, info, replay_scn, ctx))) {
          LOG_WARN("failed to add lob tablet", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBindHiddenTabletToOrigTabletOp::assign(const ObBindHiddenTabletToOrigTabletOp &other)
{
  int ret = OB_SUCCESS;
  info_ = other.info_;
  return ret;
}

int ObBindHiddenTabletToOrigTabletOp::operator()(ObTabletBindingMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else {
    const ObTabletID &orig_tablet_id = info_->data_tablet_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_->tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = info_->tablet_ids_.at(i);
      if (tablet_id != orig_tablet_id && tablet_id != data.hidden_tablet_id_) {
        data.hidden_tablet_id_ = tablet_id;
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::bind_hidden_tablet_to_orig_tablet(
    ObLS &ls,
    const ObCreateTabletInfo &info,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx,
    const bool for_old_mds)
{
  ObBindHiddenTabletToOrigTabletOp op(info);
  return modify_tablet_binding_new_mds(ls, info.data_tablet_id_, replay_scn, ctx, for_old_mds, op);
}

int ObBindLobTabletToDataTabletOp::assign(const ObBindLobTabletToDataTabletOp &other)
{
  int ret = OB_SUCCESS;
  arg_ = other.arg_;
  info_ = other.info_;
  return ret;
}

int ObBindLobTabletToDataTabletOp::operator()(ObTabletBindingMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg_) || OB_ISNULL(info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(arg_), KPC(info_));
  } else {
    const ObTabletID &data_tablet_id = info_->data_tablet_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_->tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = info_->tablet_ids_.at(i);
      if (tablet_id != data_tablet_id) {
        const ObCreateTabletSchema *create_tablet_schema = arg_->create_tablet_schemas_.at(info_->table_schema_index_.at(i));
        if (OB_ISNULL(create_tablet_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("storage is NULL", KR(ret), K(arg_));
        } else if (create_tablet_schema->is_aux_lob_meta_table()) {
          data.lob_meta_tablet_id_ = tablet_id;
        } else if (create_tablet_schema->is_aux_lob_piece_table()) {
          data.lob_piece_tablet_id_ = tablet_id;
        } else {
          // do not maintain index tablet ids
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingHelper::bind_lob_tablet_to_data_tablet(
    ObLS &ls,
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  ObBindLobTabletToDataTabletOp op(arg, info);
  return modify_tablet_binding_new_mds(ls, info.data_tablet_id_, replay_scn, ctx, arg.is_old_mds_, op);
}

// TODO (lihongqin.lhq) Separate the code of replay
template<typename F>
int ObTabletBindingHelper::modify_tablet_binding_new_mds(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx,
    bool for_old_mds,
    F &&op)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletBindingMdsUserData data;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
  if (CLICK_FAIL(get_tablet_for_new_mds(ls, tablet_id, replay_scn, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret));
    }
  } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), data))) {
    if (OB_ERR_SHARED_LOCK_CONFLICT == ret && !replay_scn.is_valid()) {
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("failed to get ddl data", K(ret));
    }
  } else if (CLICK_FAIL(op(data))) {
    LOG_WARN("failed to operate on mds", K(ret));
  } else {
    if (replay_scn.is_valid()) {
      ObTabletBindingReplayExecutor replay_executor;
      if (CLICK_FAIL(replay_executor.init(ctx, data, replay_scn, for_old_mds))) {
        LOG_WARN("failed to init replay executor", K(ret));
      } else if (CLICK_FAIL(replay_executor.execute(replay_scn, ls.get_ls_id(), tablet_id))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to replay tablet binding log", K(ret));
        }
      }
    } else {
      if (CLICK_FAIL(ls.get_tablet_svr()->set_ddl_info(tablet_id, std::move(data), user_ctx, 0/*lock_timeout_us*/))) {
        if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
          ret = OB_EAGAIN;
        } else {
          LOG_WARN("failed to save tablet binding info", K(ret));
        }
      }
    }
  }
  if (replay_scn.is_valid() && OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletUnbindMdsHelper::register_process(
    ObBatchUnbindTabletArg &arg,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(modify_tablet_binding_for_unbind(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to modify tablet binding", K(ret));
  } else {
    LOG_INFO("modify_tablet_binding_for_unbind register", KR(ret), K(arg));
  }

  return ret;
}

int ObTabletUnbindMdsHelper::on_commit_for_old_mds(
    const char* buf,
    const int64_t len,
    const transaction::ObMulSourceDataNotifyArg &notify_arg)
{
  mds::TLOCAL_MDS_INFO.reset();// disable runtime check
  return ObTabletCreateDeleteHelper::process_for_old_mds<ObBatchUnbindTabletArg, ObTabletUnbindMdsHelper>(buf, len, notify_arg);
}

int ObTabletUnbindMdsHelper::on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBatchUnbindTabletArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (arg.is_old_mds_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, arg is old mds", K(ret), K(arg));
  } else if (OB_FAIL(register_process(arg, ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletUnbindMdsHelper::replay_process(
    ObBatchUnbindTabletArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(modify_tablet_binding_for_unbind(arg, scn, ctx))) {
    LOG_WARN("failed to modify tablet binding", K(ret));
  } else {
    LOG_INFO("modify_tablet_binding_for_unbind redo", KR(ret), K(scn), K(arg));
  }

  return ret;
}

int ObTabletUnbindMdsHelper::on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBatchUnbindTabletArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (arg.is_old_mds_) {
    LOG_INFO("skip unbind mds tablet for old mds", K(arg), K(scn));
  } else if (OB_FAIL(replay_process(arg, scn, ctx))) {
    LOG_WARN("failed to replay_process", K(ret));
  }
  return ret;
}

int ObUnbindHiddenTabletFromOrigTabletOp::operator()(ObTabletBindingMdsUserData &data)
{
  int ret = OB_SUCCESS;
  data.hidden_tablet_id_.reset();
  if (OB_INVALID_VERSION != schema_version_) {
    data.redefined_ = true;
    data.snapshot_version_ = OB_INVALID_VERSION; // will be fill back on commit
    data.schema_version_ = schema_version_;
  }
  return ret;
};

int ObTabletUnbindMdsHelper::unbind_hidden_tablets_from_orig_tablets(
    ObLS &ls,
    const ObBatchUnbindTabletArg &arg,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.orig_tablet_ids_.count(); i++) {
    const ObTabletID &orig_tablet = arg.orig_tablet_ids_.at(i);
    ObUnbindHiddenTabletFromOrigTabletOp op(arg.schema_version_);
    if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_new_mds(ls, orig_tablet, replay_scn, ctx, arg.is_old_mds_, op))) {
      LOG_WARN("failed to modify tablet binding", K(ret));
    }
  }
  return ret;
}

int ObSetRwDefensiveOp::operator()(ObTabletBindingMdsUserData &data)
{
  int ret = OB_SUCCESS;
  data.redefined_ = false;
  data.snapshot_version_ = OB_INVALID_VERSION; // will be fill back on commit
  data.schema_version_ = schema_version_;
  return ret;
};

int ObTabletUnbindMdsHelper::set_redefined_versions_for_hidden_tablets(
    ObLS &ls,
    const ObBatchUnbindTabletArg &arg,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.hidden_tablet_ids_.count(); i++) {
    const ObTabletID &hidden_tablet = arg.hidden_tablet_ids_.at(i);
    ObSetRwDefensiveOp op(arg.schema_version_);
    if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_new_mds(ls, hidden_tablet, replay_scn, ctx, arg.is_old_mds_, op))) {
      LOG_WARN("failed to modify tablet binding", K(ret));
    }
  }
  return ret;
}

int ObTabletUnbindMdsHelper::modify_tablet_binding_for_unbind(const ObBatchUnbindTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(ObTabletBindingHelper::get_ls(arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObLS &ls = *ls_handle.get_ls();
    if (OB_FAIL(unbind_hidden_tablets_from_orig_tablets(ls, arg, replay_scn, ctx))) {
      LOG_WARN("failed to unbind", K(ret));
    } else if (arg.is_redefined()) {
      if (OB_FAIL(set_redefined_versions_for_hidden_tablets(ls, arg, replay_scn, ctx))) {
        LOG_WARN("failed to set redefined versions", K(ret));
      }
    }
  }
  return ret;
}

ObTabletBindingMdsArg::ObTabletBindingMdsArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), tablet_ids_(), binding_datas_()
{
  reset();
}

bool ObTabletBindingMdsArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid() && tablet_ids_.count() == binding_datas_.count();
}

void ObTabletBindingMdsArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  tablet_ids_.reset();
  binding_datas_.reset();
}

int ObTabletBindingMdsArg::assign(const ObTabletBindingMdsArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(binding_datas_.assign(other.binding_datas_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletBindingMdsArg, tenant_id_, ls_id_, tablet_ids_, binding_datas_);

int ObTabletBindingMdsHelper::get_sorted_ls_tablets(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObArray<std::pair<ObLSID, ObTabletID>> &ls_tablets,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ls_tablets.reset();
  ObArray<ObLSID> ls_ids;
  if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
    LOG_WARN("failed to batch get ls", K(ret));
  } else if (OB_UNLIKELY(tablet_ids.count() != ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet ids ls ids", K(ret), K(tablet_ids), K(ls_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    if (OB_FAIL(ls_tablets.push_back(std::make_pair(ls_ids[i], tablet_ids.at(i))))) {
      LOG_WARN("failed to push back tablet id and ls id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(ls_tablets.begin(), ls_tablets.end(), LSTabletCmp());
  }
  return ret;
}

int ObTabletBindingMdsHelper::batch_get_tablet_binding(
    const int64_t abs_timeout_us,
    const ObBatchGetTabletBindingArg &arg,
    ObBatchGetTabletBindingRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSService *ls_service = MTL(ObLSService *);
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObRole role = INVALID_ROLE;
      if (OB_ISNULL(ls_service) || OB_ISNULL(log_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls_service or log_service", K(ret));
      } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls", K(ret), K(arg.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls not leader", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); i++) {
          const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
          ObTabletHandle tablet_handle;
          ObTabletBindingMdsUserData data;
          if (OB_FAIL(ObTabletBindingHelper::get_tablet_for_new_mds(*ls, tablet_id, share::SCN::invalid_scn(), tablet_handle))) {
            LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(abs_timeout_us));
          } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_data(share::SCN::max_scn(), data, abs_timeout_us - ObTimeUtility::current_time()))) {
            LOG_WARN("failed to update tablet autoinc seq", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(res.binding_datas_.push_back(data))) {
            LOG_WARN("failed to push back", K(ret));
          }

          // currently not support to read uncommitted mds set by this transaction, so check and avoid such usage
          if (OB_SUCC(ret) && arg.check_committed_) {
            ObTabletBindingMdsUserData tmp_data;
            bool is_committed = true;
            if (OB_FAIL(tablet_handle.get_obj()->get_latest_ddl_data(tmp_data, is_committed))) {
              if (OB_EMPTY_RESULT == ret) {
                is_committed = true;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get latest ddl data", K(ret));
              }
            } else if (OB_UNLIKELY(!is_committed)) {
              ret = OB_EAGAIN;
              LOG_WARN("check committed failed", K(ret), K(tenant_id), K(arg.ls_id_), K(tablet_id), K(tmp_data));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletBindingMdsHelper::get_tablet_binding_mds_by_rpc(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletBindingMdsUserData> &datas)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  share::ObLocationService *location_service = nullptr;
  ObAddr leader_addr;
  obrpc::ObBatchGetTabletBindingArg arg;
  obrpc::ObBatchGetTabletBindingRes res;
  if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", K(ret), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id, tablet_ids, true/*check_committed*/))) {
    LOG_WARN("failed to init arg", K(ret), K(tenant_id), K(ls_id), K(ls_id));
  } else {
    bool force_renew = false;
    bool finish = false;
    for (int64_t retry_times = 0; OB_SUCC(ret) && !finish; retry_times++) {
      if (OB_FAIL(location_service->get_leader(cluster_id, tenant_id, ls_id, force_renew, leader_addr))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(abs_timeout_us - ObTimeUtility::current_time()).batch_get_tablet_binding(arg, res))) {
        LOG_WARN("fail to batch get tablet binding", K(ret), K(retry_times), K(abs_timeout_us));
      } else {
        finish = true;
      }
      if (OB_FAIL(ret)) {
        force_renew = true;
        if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_GET_LOCATION_TIME_OUT == ret || OB_NOT_MASTER == ret
            || OB_ERR_SHARED_LOCK_CONFLICT == ret || OB_LS_OFFLINE == ret
            || OB_NOT_INIT == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret || OB_LS_LOCATION_NOT_EXIST == ret) {
          // overwrite ret
          if (OB_UNLIKELY(ObTimeUtility::current_time() > abs_timeout_us)) {
            ret = OB_TIMEOUT;
            LOG_WARN("timeout", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret), K(abs_timeout_us));
          } else if (retry_times >= 3) {
            ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(100 * 1000L); // 100ms
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(datas.assign(res.binding_datas_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObTabletBindingMdsHelper::modify_tablet_binding_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &arg,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = arg.id_;

  if (OB_SUCC(ret)) {
    ObArray<ObTabletID> batch_tablet_ids;
    ObArray<ObBindHiddenTabletToOrigTabletOp> batch_ops;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
      const ObCreateTabletInfo &info = arg.tablets_[i];
      const bool is_last = i == arg.tablets_.count() - 1;
      if (ObTabletCreateDeleteHelper::is_pure_hidden_tablets(info)) {
        ObBindHiddenTabletToOrigTabletOp op(info);
        if (OB_FAIL(batch_tablet_ids.push_back(info.data_tablet_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(batch_ops.push_back(op))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_last || batch_tablet_ids.count() >= ObTabletBindingMdsArg::BATCH_TABLET_CNT) {
        if (OB_FAIL(modify_tablet_binding_(tenant_id, ls_id, batch_tablet_ids, abs_timeout_us,
                ModifyBindingByOps<ObBindHiddenTabletToOrigTabletOp>(batch_ops), trans))) {
          LOG_WARN("failed to modify tablet binding", K(ret));
        } else {
          batch_tablet_ids.reuse();
          batch_ops.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!batch_tablet_ids.empty() || !batch_ops.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch not consumed out", K(ret), K(batch_tablet_ids), K(batch_ops));
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<ObTabletID> batch_tablet_ids;
    ObArray<ObBindLobTabletToDataTabletOp> batch_ops;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); i++) {
      const ObCreateTabletInfo &info = arg.tablets_[i];
      const bool is_last = i == arg.tablets_.count() - 1;
      bool has_lob = false;
      if (OB_FAIL(ObTabletBindingHelper::has_lob_tablets(arg, info, has_lob))) {
        LOG_WARN("failed to has_lob_tablets", KR(ret));
      } else if (has_lob) {
        ObBindLobTabletToDataTabletOp op(arg, info);
        if (OB_FAIL(batch_tablet_ids.push_back(info.data_tablet_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(batch_ops.push_back(op))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_last || batch_tablet_ids.count() >= ObTabletBindingMdsArg::BATCH_TABLET_CNT) {
        if (OB_FAIL(modify_tablet_binding_(tenant_id, ls_id, batch_tablet_ids, abs_timeout_us,
                ModifyBindingByOps<ObBindLobTabletToDataTabletOp>(batch_ops), trans))) {
          LOG_WARN("failed to modify tablet binding", K(ret));
        } else {
          batch_tablet_ids.reuse();
          batch_ops.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!batch_tablet_ids.empty() || !batch_ops.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch not consumed out", K(ret), K(batch_tablet_ids), K(batch_ops));
    }
  }
  return ret;
}

// redefined_schema_version is not OB_INVALID_VERSION iff for ddl succ
int ObTabletBindingMdsHelper::modify_tablet_binding_for_unbind(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &orig_tablet_ids,
    const ObIArray<ObTabletID> &hidden_tablet_ids,
    const int64_t redefined_schema_version,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ObUnbindHiddenTabletFromOrigTabletOp op(redefined_schema_version);
    if (OB_FAIL(modify_tablet_binding_(tenant_id, orig_tablet_ids, abs_timeout_us, op, trans))) {
      LOG_WARN("failed to modify tablet binding", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_VERSION != redefined_schema_version) {
    ObSetRwDefensiveOp op(redefined_schema_version);
    if (OB_FAIL(modify_tablet_binding_(tenant_id, hidden_tablet_ids, abs_timeout_us, op, trans))) {
      LOG_WARN("failed to modify tablet binding", K(ret));
    }
  }
  return ret;
}

int ObTabletBindingMdsHelper::modify_tablet_binding_for_rw_defensive(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t schema_version,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSetRwDefensiveOp op(schema_version);
  if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(schema_version), K(tablet_ids));
  } else if (OB_FAIL(modify_tablet_binding_(tenant_id, tablet_ids, abs_timeout_us, op, trans))) {
    LOG_WARN("failed to modify tablet binding", K(ret));
  }
  return ret;
}

template<typename F>
int ObTabletBindingMdsHelper::modify_tablet_binding_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!tablet_ids.empty()) {
    ObArray<ObTabletBindingMdsUserData> old_datas;
    if (OB_FAIL(get_tablet_binding_mds_by_rpc(tenant_id, ls_id, tablet_ids, abs_timeout_us, old_datas))) {
      LOG_WARN("failed to get tablet binding mds", K(ret));
    } else {
      ObTabletBindingMdsArg arg;
      arg.tenant_id_ = tenant_id;
      arg.ls_id_ = ls_id;
      if (OB_FAIL(arg.tablet_ids_.assign(tablet_ids))) {
        LOG_WARN("failed to assign", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < old_datas.count(); i++) {
        if (OB_FAIL(arg.binding_datas_.push_back(old_datas.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          ObTabletBindingMdsUserData &data = arg.binding_datas_.at(arg.binding_datas_.count() - 1);
          ret = op(i, data);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!arg.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(ret), K(arg));
      } else if (OB_FAIL(register_mds_(arg, trans))) {
        LOG_WARN("failed to register mds", K(ret));
      }
    }
  }
  return ret;
}

template<typename F>
int ObTabletBindingMdsHelper::modify_tablet_binding_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!tablet_ids.empty()) {
    ObArray<std::pair<ObLSID, ObTabletID>> ls_tablets;
    ObArray<ObTabletID> this_batch_tablet_ids;
    if (OB_FAIL(get_sorted_ls_tablets(tenant_id, tablet_ids, ls_tablets, trans))) {
      LOG_WARN("failed to get sorted ls tablets", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); i++) {
      const ObLSID &ls_id = ls_tablets.at(i).first;
      const ObTabletID &tablet_id = ls_tablets.at(i).second;
      const bool is_last_or_next_ls_id_changed = i == ls_tablets.count() - 1 || ls_id != ls_tablets.at(i+1).first;
      if (OB_FAIL(this_batch_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (is_last_or_next_ls_id_changed || this_batch_tablet_ids.count() >= ObTabletBindingMdsArg::BATCH_TABLET_CNT) {
        if (OB_FAIL(modify_tablet_binding_(tenant_id, ls_id, this_batch_tablet_ids, abs_timeout_us,
                ModifyBindingByOp<F>(op), trans))) {
          LOG_WARN("failed to modify tablet binding", K(ret));
        } else {
          this_batch_tablet_ids.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!this_batch_tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch not consumed out", K(ret), K(this_batch_tablet_ids));
    }
  }
  return ret;
}

int ObTabletBindingMdsHelper::register_mds_(
    const ObTabletBindingMdsArg &arg,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  sqlclient::ObISQLConnection *isql_conn = nullptr;
  if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection", K(ret));
  } else {
    const int64_t size = arg.get_serialize_size();
    ObArenaAllocator allocator("TblBind");
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize arg", K(ret));
    } else if (OB_FAIL(static_cast<observer::ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, ObTxDataSourceType::TABLET_BINDING, buf, pos))) {
      LOG_WARN("failed to register mds", K(ret));
    }
  }
  return ret;
}

int ObTabletBindingMdsHelper::on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletBindingMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify_(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletBindingMdsHelper::on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletBindingMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify_(arg, scn, ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletBindingMdsHelper::modify_(
    const ObTabletBindingMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = arg.ls_id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(arg), KP(ls));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); i++) {
    const ObTabletID &tablet_id = arg.tablet_ids_[i];
    if (OB_FAIL(set_tablet_binding_mds_(*ls, tablet_id, scn, arg.binding_datas_[i], ctx))) {
      LOG_WARN("failed to set tablet binding mds", K(ret), K(ls_id), K(tablet_id), K(scn));
    }
  }
  LOG_INFO("modify tablet binding data", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  return ret;
}

int ObTabletBindingMdsHelper::set_tablet_binding_mds_(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    const ObTabletBindingMdsUserData &data,
    mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  if (!replay_scn.is_valid()) {
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    ObLSHandle ls_handle;
    if (CLICK_FAIL(ls.get_tablet_svr()->set_ddl_info(tablet_id, data, user_ctx, 0/*lock_timeout_us*/))) {
      if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("failed to save tablet binding info", K(ret));
      }
    }
  } else {
    ObTabletBindingReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, data, replay_scn, false/*for_old_mds*/))) {
      LOG_WARN("failed to init replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(replay_scn, ls.get_ls_id(), tablet_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay tablet binding log", K(ret));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
