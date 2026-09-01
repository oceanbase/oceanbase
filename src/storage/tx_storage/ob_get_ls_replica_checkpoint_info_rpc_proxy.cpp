/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tx_storage/ob_get_ls_replica_checkpoint_info_rpc_proxy.h"

#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace obrpc
{

int ObRpcGetLSReplicaCheckpointInfoP::process()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!GCTX.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("global context is not init", KR(ret));
  } else if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K_(arg));
  } else {
    MTL_SWITCH(arg_.get_tenant_id()) {
      SCN checkpoint_scn = SCN::min_scn();
      ObLSService *ls_svr = nullptr;
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", KR(ret), K_(arg));
      } else if (OB_FAIL(ls_svr->get_ls(
          arg_.get_ls_id(), ls_handle, ObLSGetMod::TXSTORAGE_MOD))) {
        LOG_WARN("get log stream failed", KR(ret), K_(arg));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log stream is null", KR(ret), K_(arg), K(ls_handle));
      } else if (OB_FAIL(ls->get_majority_min_replica_checkpoint_scn(checkpoint_scn))) {
        LOG_WARN("failed to get majority min replica checkpoint scn", KR(ret), K_(arg));
      } else if (OB_UNLIKELY(!checkpoint_scn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("checkpoint scn is invalid", KR(ret), K_(arg), K(checkpoint_scn));
      } else if (OB_FAIL(result_.init(
          arg_.get_tenant_id(), arg_.get_ls_id(), checkpoint_scn))) {
        LOG_WARN("failed to init result", KR(ret), K_(arg), K(checkpoint_scn));
      } else {
        LOG_INFO("finish get ls replica checkpoint info", KR(ret), K_(arg),
            K_(result), K(checkpoint_scn));
      }
    }
  }
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
