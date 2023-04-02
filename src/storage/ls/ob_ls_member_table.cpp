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
#include "storage/ls/ob_ls.h"
#include "share/ob_ls_id.h"
#include "storage/ls/ob_ls_member_table.h"
#include "share/schema/ob_table_schema.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace memtable;
using namespace share;
using namespace share::schema;

int ObLSMemberTable::handle_create_tablet_notify(const NotifyType type,
                                                 const char *buf,
                                                 const int64_t len,
                                                 const ObMulSourceDataNotifyArg &trans_flags)
{
  int ret = OB_SUCCESS;
  obrpc::ObBatchCreateTabletArg create_arg;
  int64_t new_pos = 0;

  TRANS_LOG(INFO, "do_handle_create_tablet_notify ", K(type), K(len), K(trans_flags));

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(create_arg.deserialize(buf, len, new_pos))) {
    TRANS_LOG(WARN, "deserialize failed for ls_member trans",
                    KR(ret), K(type), K(trans_flags.for_replay_));
  } else {
    switch (type) {
    case NotifyType::REGISTER_SUCC: {
        ret = prepare_create_tablets(create_arg, trans_flags);
        break;
      }
    case NotifyType::ON_REDO: {
        ret = on_redo_create_tablets(create_arg, trans_flags);
        break;
      }
    case NotifyType::ON_COMMIT:
    case NotifyType::ON_ABORT: {
        bool commit = (type == NotifyType::ON_COMMIT) ? true: false;
        ret = on_commit_create_tablets(&create_arg, trans_flags, commit);
        break;
      }
    case NotifyType::TX_END: {
        ret = on_tx_end_create_tablets(create_arg, trans_flags);
        break;
      }
    default: {
        break;
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to create tablet", KR(ret), K(type), K(create_arg));
    }
  }
  return ret;
}

int ObLSMemberTable::prepare_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                            const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("prepare_create_tablets ", K(arg), K(trans_flags));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_
             && trans_flags.scn_ <= ls->get_tablet_change_checkpoint_scn()) {
    LOG_INFO("replay skip for create tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_prepare_create_tablets(arg, trans_flags))) {
    LOG_WARN("fail to create tablet", KR(ret), K(arg), K(trans_flags));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member redo create failed, which is not allowed",
               KR(ret), K(arg), K(trans_flags), K(arg.id_));
  }
  return ret;
}

int ObLSMemberTable::on_redo_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                            const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("on_redo_create_tablets ", K(arg), K(trans_flags));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_ ) {
    LOG_INFO("replay skip for create tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_redo_create_tablets(arg, trans_flags))) {
    LOG_WARN("fail to on_redo_create_tablets", KR(ret), K(arg), K(trans_flags));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member redo create failed, which is not allowed",
               KR(ret), K(arg), K(trans_flags), K(arg.id_));
  }
  return ret;
}

int ObLSMemberTable::on_commit_create_tablets(const obrpc::ObBatchCreateTabletArg *arg,
                                              const ObMulSourceDataNotifyArg &trans_flags,
                                              bool commit)
{
  LOG_INFO("on_commit_create_tablets ", KPC(arg), K(trans_flags),
                                        K(commit));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg->id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_
             && trans_flags.scn_ <= ls->get_tablet_change_checkpoint_scn()) {
    LOG_INFO("replay skip for create tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (commit) {
    if (OB_FAIL(tablet_svr->on_commit_create_tablets(*arg, trans_flags))) {
      LOG_WARN("fail to on_commit_create_tablets", KR(ret), KPC(arg), K(trans_flags));
    } else {
      ls->get_tablet_gc_handler()->set_tablet_persist_trigger();
    }
  } else if (!commit) {
    if (OB_FAIL(tablet_svr->on_abort_create_tablets(*arg, trans_flags))) {
      LOG_WARN("fail to on_abort_create_tablets", KR(ret), KPC(arg), K(trans_flags));
    } else {
      ls->get_tablet_gc_handler()->set_tablet_gc_trigger();
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member commit create failed, which is not allowed",
               KR(ret), KPC(arg), K(trans_flags));
  }
  return ret;
}

int ObLSMemberTable::on_tx_end_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                           const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("tx_end_create_tablets ", K(arg), K(trans_flags));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (!trans_flags.is_redo_submitted()) {
    LOG_INFO("replay skip for create tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_tx_end_create_tablets(arg, trans_flags))) {
    LOG_WARN("fail to on_tx_end_create_tablets", KR(ret), K(arg), K(trans_flags));
  }
  return ret;
}

int ObLSMemberTable::handle_remove_tablet_notify(const NotifyType type,
                                                 const char *buf,
                                                 const int64_t len,
                                                 const ObMulSourceDataNotifyArg & trans_flags)
{
  int ret = OB_SUCCESS;
  obrpc::ObBatchRemoveTabletArg remove_arg;
  int64_t new_pos = 0;

  TRANS_LOG(INFO, "do_handle_remove_tablet_notify ", K(type), K(len), K(trans_flags));

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(remove_arg.deserialize(buf, len, new_pos))) {
    TRANS_LOG(WARN, "deserialize failed for ls_member trans",
                    KR(ret), K(type), K(trans_flags));
  } else {
    switch (type) {
    case NotifyType::REGISTER_SUCC: {
        ret = prepare_remove_tablets(remove_arg, trans_flags);
        break;
      }
    case NotifyType::ON_REDO: {
        ret = on_redo_remove_tablets(remove_arg, trans_flags);
        break;
      }
    case NotifyType::ON_COMMIT:
    case NotifyType::ON_ABORT: {
        bool commit = (type == NotifyType::ON_COMMIT) ? true: false;
        ret = on_commit_remove_tablets(&remove_arg, trans_flags, commit);
        break;
      }
    case NotifyType::TX_END: {
        ret = on_tx_end_remove_tablets(remove_arg, trans_flags);
        break;
      }
    default: {
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to remove tablet", KR(ret), K(type), K(remove_arg));
    }
  }
  return ret;
}

int ObLSMemberTable::prepare_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                            const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("prepare_remove_tablets ", K(arg), K(trans_flags));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_    // dropping tablet triggers dumpping memtable
             && trans_flags.scn_ <= ls->get_tablet_change_checkpoint_scn()) {
    LOG_INFO("replay skip for remove tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_prepare_remove_tablets(arg, trans_flags))) {
    LOG_WARN("fail to remove tablet", KR(ret), K(arg), K(trans_flags));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member prepare remove failed, which is not allowed",
               KR(ret), K(arg), K(trans_flags), K(arg.id_));
  }
  return ret;
}

int ObLSMemberTable::on_redo_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                            const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("on_redo_remove_tablets ", K(arg), K(trans_flags));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_) {
    LOG_INFO("replay skip for remove tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_redo_remove_tablets(arg, trans_flags))) {
    LOG_WARN("fail to on_redo_remove_tablets", KR(ret), K(arg), K(trans_flags));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member redo remove failed, which is not allowed",
               KR(ret), K(arg), K(trans_flags), K(arg.id_));
  }
  return ret;
}

int ObLSMemberTable::on_commit_remove_tablets(const obrpc::ObBatchRemoveTabletArg *arg,
                                              const ObMulSourceDataNotifyArg &trans_flags,
                                              bool commit)
{
  LOG_INFO("on_commit_remove_tablets ", K(trans_flags), K(commit), KPC(arg));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg->id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (trans_flags.for_replay_
             && trans_flags.scn_ <= ls->get_tablet_change_checkpoint_scn()) {
    LOG_INFO("replay skip for remove tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (commit) {
    if (OB_FAIL(tablet_svr->on_commit_remove_tablets(*arg, trans_flags))) {
      LOG_WARN("fail to on_commit_remove_tablets", KR(ret), KPC(arg), K(trans_flags));
    } else {
      ls->get_tablet_gc_handler()->set_tablet_gc_trigger();
    }
  } else if (OB_FAIL(tablet_svr->on_abort_remove_tablets(*arg, trans_flags))) {
    LOG_WARN("fail to on_abort_remove_tablets", KR(ret), KPC(arg), K(trans_flags));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ls_member commit remove failed, which is not allowed",
               KR(ret), KPC(arg), K(trans_flags));
  }
  return ret;
}

int ObLSMemberTable::on_tx_end_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                           const ObMulSourceDataNotifyArg &trans_flags)
{
  LOG_INFO("tx_end_remove_tablets ", K(trans_flags), K(arg));
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSTabletService *tablet_svr;
  ObLSService* ls_srv = nullptr;
  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", KR(ret));
  } else if (OB_FAIL(ls_srv->get_ls(arg.id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
  } else if (!trans_flags.is_redo_submitted()) {
    LOG_INFO("replay skip for remove tablet", KR(ret), K(trans_flags), K(arg), K(ls->get_ls_meta()));
  } else if (OB_ISNULL(tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr is NULL", KR(ret), K(ls));
  } else if (OB_FAIL(tablet_svr->on_tx_end_remove_tablets(arg, trans_flags))) {
    LOG_WARN("fail to on_tx_end_remove_tablets", KR(ret), K(arg), K(trans_flags));
  }
  return ret;
}

} // storage
} // oceanbase

