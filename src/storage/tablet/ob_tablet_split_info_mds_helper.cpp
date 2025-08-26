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

#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_split_info_mds_helper.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tablet/ob_tablet_split_info_replay_executor.h"
#define USING_LOG_PREFIX MDS

using oceanbase::sqlclient::ObISQLConnection;

namespace oceanbase
{
namespace storage
{

void ObTabletSplitInfoMdsArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  split_info_datas_.reset();
}
bool ObTabletSplitInfoMdsArg::is_valid() const
{
  bool is_valid = (OB_INVALID_TENANT_ID != tenant_id_) && ls_id_.is_valid();
  for (int64_t i = 0; is_valid && i < split_info_datas_.count(); ++i) {
    is_valid &= split_info_datas_.at(i).is_valid();
  }
  return is_valid;
}

int ObTabletSplitInfoMdsArg::assign(const ObTabletSplitInfoMdsArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    if (OB_FAIL(split_info_datas_.assign(other.split_info_datas_))) {
      LOG_WARN("failed to assign split info datas", K(ret), K(other.split_info_datas_));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletSplitInfoMdsArg, tenant_id_, ls_id_, split_info_datas_);

int ObTabletSplitInfoMdsHelper::modify(
    const ObTabletSplitInfoMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = arg.ls_id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool need_empty_shell_trigger = false;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(arg), KP(ls));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.split_info_datas_.count(); i++) {
    const ObTabletID &tablet_id = arg.split_info_datas_[i].source_tablet_id_;
    const ObTabletSplitInfoMdsUserData &data = arg.split_info_datas_[i]; // index range is checked by is_valid()
    if (OB_FAIL(set_tablet_split_mds(ls_id, tablet_id, scn, data, ctx))) {
      LOG_WARN("failed to modify", K(ret), K(ls_id), K(tablet_id), K(scn));
    }

  }
  LOG_INFO("modify tablet split data", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  return ret;
}

int ObTabletSplitInfoMdsHelper::set_tablet_split_mds(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    const ObTabletSplitInfoMdsUserData &data,
    mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
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
    ObTabletSplitInfoReplayExecutor replay_executor;
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

int ObTabletSplitInfoMdsHelper::on_register(const char* buf, const int64_t len,  mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletSplitInfoMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletSplitInfoMdsHelper::on_replay(const char* buf, const int64_t len, share::SCN scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletSplitInfoMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, scn, ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}


int ObTabletSplitInfoMdsHelper::register_mds(const ObTabletSplitInfoMdsArg &arg, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *isql_conn = nullptr;
  if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incalid argument", K(ret), K(arg));
  } else {
    const int64_t size = arg.get_serialize_size();
    ObArenaAllocator allocator(ObMemAttr(common::OB_SERVER_TENANT_ID, "DldUpTabMeta"));
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize arg", K(ret));
    } else if (OB_FAIL(static_cast<observer::ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, transaction::ObTxDataSourceType::TABLET_SPLIT_INFO, buf, pos))) {
      LOG_WARN("failed to register mds", K(ret));
    }
  }
  return ret;
}
}
}