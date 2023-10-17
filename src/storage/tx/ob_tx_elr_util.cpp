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

#include "ob_tx_elr_util.h"
#include "common/ob_clock_generator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "ob_trans_event.h"

namespace oceanbase
{
namespace transaction
{

int ObTxELRUtil::check_and_update_tx_elr_info(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID != MTL_ID() && MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    if (can_tenant_elr_) {  // tenant config enable elr
      tx.set_can_elr(true);
      TX_STAT_ELR_ENABLE_TRANS_INC(MTL_ID());
    }
    refresh_elr_tenant_config_();
  }
  return ret;
}

void ObTxELRUtil::refresh_elr_tenant_config_()
{
  bool need_refresh = ObClockGenerator::getClock() - last_refresh_ts_ > REFRESH_INTERVAL;

  if (OB_UNLIKELY(need_refresh)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_LIKELY(tenant_config.is_valid())) {
      can_tenant_elr_ = tenant_config->enable_early_lock_release;
      last_refresh_ts_ = ObClockGenerator::getClock();
    }
    if (REACH_TIME_INTERVAL(10000000 /* 10s */)) {
      TRANS_LOG(INFO, "refresh tenant config success", "tenant_id", MTL_ID(), K(*this));
    }
  }
}

} //transaction

} //oceanbase
