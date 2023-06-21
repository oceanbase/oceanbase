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

#include "storage/tx_storage/ob_tenant_freezer_rpc.h"

#include "observer/ob_server.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_tenant_freezer_common.h"
#include "storage/multi_data_source/mds_table_mgr.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace storage::mds;
using namespace rootserver;
namespace obrpc
{

int ObTenantFreezerRpcCb::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTenantFreezer::rpc_callback())) {
    LOG_WARN("rpc callback failed", K(ret));
  }
  return ret;
}

void ObTenantFreezerRpcCb::on_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("Tenant major freeze request timeout");
  if (OB_FAIL(ObTenantFreezer::rpc_callback())) {
    LOG_WARN("rpc callback failed", K(ret));
  }
}

int ObTenantFreezerP::process()
{
  int ret = OB_SUCCESS;
  if (storage::MINOR_FREEZE == arg_.freeze_type_) {
    LOG_ERROR("should not be here");
  } else if (storage::TX_DATA_TABLE_FREEZE == arg_.freeze_type_) {
    if (OB_FAIL(do_tx_data_table_freeze_())) {
      LOG_WARN("do tx data table freeze failed.", KR(ret), K(arg_));
    }
  } else if (storage::MAJOR_FREEZE == arg_.freeze_type_) {
    if (OB_FAIL(do_major_freeze_())) {
      LOG_WARN("do major freeze failed", K(ret));
    }
  } else if (storage::MDS_TABLE_FREEZE == arg_.freeze_type_) {
    if (OB_FAIL(do_mds_table_freeze_())) {
      LOG_WARN("do mds table freeze failed.", KR(ret), K(arg_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown freeze type", K(arg_), K(ret));
  }
  return ret;
}

int ObTenantFreezerP::do_tx_data_table_freeze_()
{
  int ret = OB_SUCCESS;

  LOG_INFO("start tx data table self freeze task in rpc handle thread", K(arg_));

  common::ObSharedGuard<ObLSIterator> iter_guard;
  ObTenantTxDataFreezeGuard tenant_freeze_guard;
  ObLSService *ls_srv = MTL(ObLSService *);
  ObTenantFreezer *freezer = MTL(ObTenantFreezer *);

  if (OB_FAIL(tenant_freeze_guard.init(freezer))) {
    LOG_WARN("[TenantFreezer] fail to get log stream iterator", K(ret));
  } else if (!tenant_freeze_guard.can_freeze()) {
    // skip tx data self freeze due to another freeze task is running
  } else if (OB_FAIL(ls_srv->get_ls_iter(iter_guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("[TenantFreezer] fail to get log stream iterator", K(ret));
  } else {
    int ls_cnt = 0;
    while (OB_SUCC(ret))
    {
      ObTxTableGuard tx_table_guard;
      ObLS *ls = nullptr;
      if (OB_FAIL(iter_guard->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next ls failed.", KR(ret), K(arg_));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is unexpected nullptr", KR(ret), K(arg_));
      } else if (OB_FAIL(ls->get_tx_table_guard(tx_table_guard))) {
        LOG_WARN("get tx table guard failed.", KR(ret), K(arg_));
      } else if (OB_FAIL(tx_table_guard.self_freeze_task())) {
        LOG_WARN("freeze tx data table failed.", KR(ret), K(arg_));
      }
      ++ls_cnt;
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (0 == ls_cnt) {
        LOG_WARN("[TenantFreezer] no logstream", K(ret), K(ls_cnt));
      }
    }
  }

  LOG_INFO("finish self freeze task in rpc handle thread", KR(ret), K(arg_));
  return ret;
}

int ObTenantFreezerP::do_major_freeze_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  SCN frozen_scn;

  if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(tenant_id, frozen_scn))) {
    LOG_WARN("get_frozen_scn failed", KR(ret));
  } else {
    int64_t frozen_scn_val = frozen_scn.get_val_for_tx();
    bool need_major = true;
    ObTenantFreezer *freezer = MTL(ObTenantFreezer *);
    ObRetryMajorInfo retry_major_info = freezer->get_retry_major_info();
    retry_major_info.tenant_id_ = tenant_id;
    retry_major_info.frozen_scn_ = arg_.try_frozen_scn_;
    if (arg_.try_frozen_scn_ > 0) {
      if (arg_.try_frozen_scn_ < frozen_scn_val) {
        need_major = false;
      } else {
        need_major = true;
      }
    } else if (!freezer->tenant_need_major_freeze()) {
      need_major = false;
    }
    if (!need_major) {
      retry_major_info.reset();
    } else {
      retry_major_info.frozen_scn_ = frozen_scn_val;

      ObMajorFreezeParam param;
      param.transport_ = GCTX.net_frame_->get_req_transport();
      if (OB_FAIL(param.add_freeze_info(tenant_id))) {
        LOG_WARN("push back failed", KR(ret), K(tenant_id));
      } else {
        LOG_INFO("do major freeze", K(param));
        if (OB_FAIL(ObMajorFreezeHelper::major_freeze(param))) {
          LOG_WARN("major freeze failed", K(param), KR(ret));
        } else {
          retry_major_info.reset();
        }
      }
    }
    freezer->set_retry_major_info(retry_major_info);
  }

  LOG_INFO("finish tenant major freeze", KR(ret), K(tenant_id), K(frozen_scn));
  return ret;
}

int ObTenantFreezerP::do_mds_table_freeze_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start mds table self freeze task in rpc handle thread", K(arg_));

  common::ObSharedGuard<ObLSIterator> iter_guard;
  ObLSService *ls_srv = MTL(ObLSService *);

  if (OB_FAIL(ls_srv->get_ls_iter(iter_guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("[TenantFreezer] fail to get log stream iterator", K(ret));
  } else {
    int ls_cnt = 0;
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      MdsTableMgrHandle mgr_handle;
      ObMdsTableMgr *mds_table_mgr = nullptr;

      if (OB_FAIL(iter_guard->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next ls failed.", KR(ret), K(arg_));
        }
      } else if (OB_ISNULL(ls) || OB_ISNULL(ls->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is unexpected nullptr", KR(ret), K(arg_));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ls->flush_mds_table(INT64_MAX))) {
          LOG_WARN("flush mds table failed", KR(tmp_ret), KPC(ls));
        }
      }
      ++ls_cnt;
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (0 == ls_cnt) {
        LOG_WARN("[TenantFreezer] no logstream", K(ret), K(ls_cnt));
      }
    }
  }

  LOG_INFO("finish mds table self freeze task in rpc handle thread", KR(ret), K(arg_));
  return ret;
}

} // obrpc
} // oceanbase
