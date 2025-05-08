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
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "rootserver/ob_ddl_service.h"

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
    is_old_mds_(false),
    is_write_defensive_(false)
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
    is_write_defensive_ = other.is_write_defensive_;
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
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_, is_old_mds_, is_write_defensive_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBatchUnbindTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, ls_id_, schema_version_, orig_tablet_ids_, hidden_tablet_ids_, is_old_mds_, is_write_defensive_);
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
  LST_DO_CODE(OB_UNIS_DECODE, is_write_defensive_);
  return ret;
}

ObBatchUnbindLobTabletArg::ObBatchUnbindLobTabletArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    data_tablet_ids_()
{
}

int ObBatchUnbindLobTabletArg::assign(const ObBatchUnbindLobTabletArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_tablet_ids_.assign(other.data_tablet_ids_))) {
    LOG_WARN("failed to assign data tablet ids", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchUnbindLobTabletArg, tenant_id_, ls_id_, data_tablet_ids_);

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

int ObTabletBindingHelper::bind_hidden_tablet_to_orig_tablet(
    ObLS &ls,
    const ObCreateTabletInfo &info,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx,
    const bool for_old_mds)
{
  return modify_tablet_binding_new_mds(ls, info.data_tablet_id_, replay_scn, ctx, for_old_mds, [&info](ObTabletBindingMdsUserData &data) -> int {
    int ret = OB_SUCCESS;
    const ObTabletID &orig_tablet_id = info.data_tablet_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < info.tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = info.tablet_ids_.at(i);
      if (tablet_id != orig_tablet_id && tablet_id != data.hidden_tablet_id_) {
        data.hidden_tablet_id_ = tablet_id;
      }
    }
    return ret;
  });
}

int ObTabletBindingHelper::bind_lob_tablet_to_data_tablet(
    ObLS &ls,
    const ObBatchCreateTabletArg &arg,
    const ObCreateTabletInfo &info,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  return modify_tablet_binding_new_mds(ls, info.data_tablet_id_, replay_scn, ctx, arg.is_old_mds_, [&arg, &info](ObTabletBindingMdsUserData &data) -> int {
    int ret = OB_SUCCESS;
    const ObTabletID &data_tablet_id = info.data_tablet_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < info.tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = info.tablet_ids_.at(i);
      if (tablet_id != data_tablet_id) {
        const ObCreateTabletSchema *create_tablet_schema = arg.create_tablet_schemas_.at(info.table_schema_index_.at(i));
        if (OB_ISNULL(create_tablet_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("storage is NULL", KR(ret), K(arg));
        } else if (create_tablet_schema->is_aux_lob_meta_table()) {
          data.lob_meta_tablet_id_ = tablet_id;
        } else if (create_tablet_schema->is_aux_lob_piece_table()) {
          data.lob_piece_tablet_id_ = tablet_id;
        } else {
          // do not maintain index tablet ids
        }
      }
    }
    return ret;
  });
}

int ObTabletBindingHelper::build_single_table_write_defensive(
    rootserver::ObDDLService &ddl_service,
    const ObTableSchema &table_schema,
    const int64_t schema_version,
    rootserver::ObDDLSQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.is_valid() || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(table_schema), K(schema_version));
  }
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObBatchUnbindTabletArg> args;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("invalid args", KR(ret), K(table_schema), K(tablet_ids));
    } else if (OB_FAIL(ddl_service.build_modify_tablet_binding_args(
        tenant_id, tablet_ids, true/*is_hidden_tablets*/, schema_version, args, trans))) {
      LOG_WARN("failed to modify tablet binding args", KR(ret), K(tenant_id), K(tablet_ids), K(schema_version), K(args));
    }
    ObArenaAllocator allocator("DDLWriteDefens");
    for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); i++) {
      args[i].is_write_defensive_ = true;
      int64_t pos = 0;
      int64_t size = args[i].get_serialize_size();
      char *buf = nullptr;
      allocator.reuse();
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate", KR(ret), K(size), K(allocator.total()), K(allocator.used()));
      } else if (OB_FAIL(args[i].serialize(buf, size, pos))) {
        LOG_WARN("failed to serialize arg", KR(ret), K(args[i]), K(size));
      } else if (OB_FAIL(trans.register_tx_data(args[i].tenant_id_, args[i].ls_id_, transaction::ObTxDataSourceType::UNBIND_TABLET_NEW_MDS, buf, pos))) {
        LOG_WARN("failed to register tx data", KR(ret), K(args[i].tenant_id_), K(args[i].ls_id_));
      }
    }
  }
  return ret;
}

// TODO (lihongqin.lhq) Separate the code of replay
template<typename F>
int ObTabletBindingHelper::modify_tablet_binding_new_mds(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx,
    bool for_old_mds,
    F op)
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
  } else if (CLICK_FAIL(tablet->get_ddl_data(data))) {
    LOG_WARN("failed to get ddl data", K(ret));
    ret = OB_EAGAIN;
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
  mds::TLOCAL_MDS_TRANS_NOTIFY_TYPE = NotifyType::UNKNOWN;// disable runtime check
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

int ObTabletUnbindMdsHelper::unbind_hidden_tablets_from_orig_tablets(
    ObLS &ls,
    const ObBatchUnbindTabletArg &arg,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.orig_tablet_ids_.count(); i++) {
    const ObTabletID &orig_tablet = arg.orig_tablet_ids_.at(i);
    if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_new_mds(ls, orig_tablet, replay_scn, ctx, arg.is_old_mds_, [&arg](ObTabletBindingMdsUserData &data) -> int {
          data.hidden_tablet_id_.reset();
          if (arg.is_redefined()) {
            data.redefined_ = true;
            data.schema_version_ = arg.schema_version_;
            if (!arg.is_write_defensive_) {
              data.snapshot_version_ = OB_INVALID_VERSION; // will be fill back on commit
            }
          }
          return OB_SUCCESS;
        }))) {
      LOG_WARN("failed to modify tablet binding", K(ret));
    }
  }
  return ret;
}

int ObTabletUnbindMdsHelper::set_redefined_versions_for_hidden_tablets(
    ObLS &ls,
    const ObBatchUnbindTabletArg &arg,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.hidden_tablet_ids_.count(); i++) {
    const ObTabletID &hidden_tablet = arg.hidden_tablet_ids_.at(i);
    if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_new_mds(ls, hidden_tablet, replay_scn, ctx, arg.is_old_mds_, [&arg](ObTabletBindingMdsUserData &data) -> int {
          data.redefined_ = false;
          data.schema_version_ = arg.schema_version_;
          if (!arg.is_write_defensive_) {
            data.snapshot_version_ = OB_INVALID_VERSION; // will be fill back on commit
          }
          return OB_SUCCESS;
        }))) {
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

int ObTabletUnbindLobMdsHelper::modify_tablet_binding_for_unbind_lob_(const ObBatchUnbindLobTabletArg &arg, const share::SCN &replay_scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(ObTabletBindingHelper::get_ls(arg.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    ObLS &ls = *ls_handle.get_ls();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.data_tablet_ids_.count(); i++) {
      const ObTabletID &data_tablet_id = arg.data_tablet_ids_.at(i);
      if (OB_FAIL(ObTabletBindingHelper::modify_tablet_binding_new_mds(ls, data_tablet_id,
              replay_scn, ctx, false, ClearLobTabletId()))) {
        LOG_WARN("failed to modify tablet binding", K(ret), K(data_tablet_id));
      }
    }
  }
  return ret;
}

int ObTabletUnbindLobMdsHelper::on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBatchUnbindLobTabletArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify_tablet_binding_for_unbind_lob_(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to modify_tablet_binding_for_unbind_lob", K(ret));
  } else {
    LOG_INFO("register unbind lob success", K(arg), K(ctx));
  }
  return ret;
}

int ObTabletUnbindLobMdsHelper::on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBatchUnbindLobTabletArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify_tablet_binding_for_unbind_lob_(arg, scn, ctx))) {
    LOG_WARN("failed to modify_tablet_binding_for_unbind_lob", K(ret));
  } else {
    LOG_INFO("replay unbind lob success", K(arg), K(ctx));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
