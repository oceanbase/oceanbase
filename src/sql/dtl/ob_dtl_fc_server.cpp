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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_fc_server.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_errno.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/dtl/ob_dtl.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::lib;
using namespace oceanbase::share;

// ObTenantDfc
ObTenantDfc::ObTenantDfc(uint64_t tenant_id)
: tenant_dfc_(), tenant_id_(tenant_id), blocked_dfc_cnt_(0), channel_total_cnt_(0), max_parallel_cnt_(0),
  max_blocked_buffer_size_(0), max_buffer_size_(0), tenant_mem_mgr_(tenant_id)
{}

ObTenantDfc::~ObTenantDfc()
{}

int ObTenantDfc::mtl_new(ObTenantDfc *&tenant_dfc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  tenant_dfc = static_cast<ObTenantDfc *> (ob_malloc(sizeof(ObTenantDfc), ObMemAttr(tenant_id, "SqlDtlDfc")));
  if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tenant dfc", K(ret));
  } else if (FALSE_IT(new (tenant_dfc) ObTenantDfc(tenant_id))) {
  }
  return ret;
}


int ObTenantDfc::mtl_init(ObTenantDfc *&tenant_dfc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  if (OB_SUCC(ret)) {
    tenant_dfc->channel_total_cnt_ = 0;
    tenant_dfc->blocked_dfc_cnt_ = 0;
    tenant_dfc->max_parallel_cnt_ = 0;
    tenant_dfc->max_blocked_buffer_size_ = 0;
    tenant_dfc->max_buffer_size_ = 0;
    tenant_dfc->tenant_id_ = tenant_id;
    if (OB_FAIL(tenant_dfc->tenant_mem_mgr_.init())) {
      LOG_WARN("failed to init tenant memory manager", K(ret));
    }
    // tenant_dfc->calc_max_buffer(10);
    LOG_INFO("init tenant dfc", K(ret), K(tenant_dfc->tenant_id_));
  }
  return ret;
}

void ObTenantDfc::mtl_destroy(ObTenantDfc *&tenant_dfc)
{
  if (nullptr != tenant_dfc) {
    LOG_INFO("trace tenant dfc destroy", K(tenant_dfc->tenant_id_));
    tenant_dfc->tenant_mem_mgr_.destroy();
    common::ob_delete(tenant_dfc);
    tenant_dfc = nullptr;
  }
}

void ObTenantDfc::check_dtl(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (tenant_id != get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: tenant_id is not match",
      K(tenant_id), K(get_tenant_id()), K(ret));
  } else {
    check_dtl_buffer_size();
    clean_on_timeout();
  }
}
void ObTenantDfc::check_dtl_buffer_size()
{
  uint64_t tenant_id = get_tenant_id();
  int ret = OB_SUCCESS;
  double min_cpu = 0;
  double max_cpu = 0;
  if (OB_ISNULL(GCTX.omt_)) {
  } else if (OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
    LOG_WARN("fail to get tenant cpu", K(ret));
  } else {
    calc_max_buffer(lround(max_cpu) * DFC_CPU_RATIO);
  }
}

int ObTenantDfc::clean_on_timeout()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = get_tenant_id();
  if (OB_FAIL(tenant_mem_mgr_.auto_free_on_time())) {
    LOG_WARN("failed to auto free memory manager", K(ret));
  }
  LOG_INFO("tenant dfc status", K(ret), K(get_tenant_id()),
    K(get_channel_cnt()),
    K(get_current_buffer_used()),
    K(get_current_blocked_cnt()),
    K(get_current_buffer_cnt()),
    K(get_max_parallel()),
    K(get_max_blocked_buffer_size()),
    K(get_max_buffer_size()),
    K(get_accumulated_blocked_cnt()),
    K(get_max_size_per_channel()));
  return ret;
}

void ObTenantDfc::calc_max_buffer(int64_t max_parallel_cnt)
{
  if (0 == max_parallel_cnt) {
    max_parallel_cnt = 1;
  }
  max_parallel_cnt_ = max_parallel_cnt;
  // MAX_BUFFER_CNT表示一个算子最大buffer的数据，+2表示transmit端最大2个,MAX_BUFFER_FACTOR表示浮动比例，/2表示最大并行度为max_parallel_cnt的1/2
  // 假设max_parallel_cnt_=1，则 1 * (4 + 2) * 64 * 1024 * 2 / 2，则最大6个buffer页
  //    max_parallel_cnt_=10，则 10 * (4 + 2) * 64 * 1024 * 2 / 2，则最大60个buffer页，假设最大的channel数为5*5*2=50，
  //       则每个channel有1.2个buffer页,如果一个算子有5个chanel，则1.2*5=6个buffer页
  //    max_parallel_cnt_=600，则 600 * (4 + 2) * 64 * 1024 * 2 / 2，则最大3600个buffer页
  //      假设都是1:1，则300个并发sql，最大的channel数为600，每个dfc 约6个buffer
  //      假设2个query，每个分别为150*2，则channel数约150*150*2，每个dfc约12个buffer
  max_blocked_buffer_size_ = max_parallel_cnt_ * (MAX_BUFFER_CNT + 2) * GCONF.dtl_buffer_size * MAX_BUFFER_FACTOR / 2;
  max_buffer_size_ = max_blocked_buffer_size_ * MAX_BUFFER_FACTOR;
  int64_t factor = 1;
  int ret = OB_SUCCESS;
  ret = OB_E(EventTable::EN_DFC_FACTOR) ret;
  if (OB_FAIL(ret)) {
    factor = -ret;
    max_buffer_size_ *= factor;
    max_blocked_buffer_size_ *= factor;
    ret = OB_SUCCESS;
  }
  LOG_INFO("trace tenant dfc parameters", K(max_parallel_cnt_), K(max_blocked_buffer_size_), K(max_buffer_size_));
}

int ObTenantDfc::register_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfc.register_channel(ch))) {
    LOG_WARN("failed to regiester channel", KP(ch->get_id()), K(ret));
  } else {
    increase_channel_cnt(1);
  }
  return ret;
}

int ObTenantDfc::unregister_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = dfc.unregister_channel(ch))) {
    ret = tmp_ret;
    LOG_WARN("failed to regiester channel", KP(ch->get_id()), K(ret));
  }
  if (OB_ENTRY_NOT_EXIST != ret) {
    decrease_channel_cnt(1);
  }
  return ret;
}

int ObTenantDfc::deregister_dfc(ObDtlFlowControl &dfc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t n_ch = dfc.get_channel_count();
  if (dfc.is_receive()) {
    ObDtlChannel* ch = nullptr;
    for (int i = 0; i < n_ch; ++i) {
      if (OB_SUCCESS != (tmp_ret = dfc.get_channel(i, ch))) {
        ret = tmp_ret;
        LOG_WARN("failed to free channel or no channel", K(i), K(dfc.get_channel_count()), K(n_ch), K(ret));
      } else if (nullptr == ch) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to free channel or no channel", K(i), K(dfc.get_channel_count()), K(n_ch), K(ret));
      }
    }
  }
  if (OB_SUCCESS != (tmp_ret = dfc.unregister_all_channel())) {
    ret = tmp_ret;
    LOG_ERROR("fail unregister all channel from dfc", KR(tmp_ret));
  }
  decrease_channel_cnt(n_ch);
  return ret;
}

int ObTenantDfc::enforce_block(ObDtlFlowControl *dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  if (!dfc->is_block(ch_idx)) {
    increase_blocked_channel_cnt();
    dfc->set_block(ch_idx);
    LOG_TRACE("receive set channel block trace", K(dfc), K(ret), K(ch_idx));
  }
  return ret;
}

int ObTenantDfc::try_unblock_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  if (dfc->is_block()) {
    int64_t unblock_cnt = 0;
    if (can_unblock(dfc)) {
      if (OB_FAIL(dfc->notify_all_blocked_channels_unblocking(unblock_cnt))) {
        LOG_WARN("failed to unblock all blocked channel", K(dfc), K(ch_idx), K(ret));
      }
      if (0 < unblock_cnt) {
        decrease_blocked_channel_cnt(unblock_cnt);
      }
      LOG_TRACE("unblock channel on decrease size", K(dfc), K(ret), K(unblock_cnt), K(ch_idx));
    } else if (dfc->is_block(ch_idx)) {
      ObDtlChannel *dtl_ch = nullptr;
      if (OB_FAIL(dfc->get_channel(ch_idx, dtl_ch))) {
        LOG_WARN("failed to get dtl channel", K(dfc), K(ch_idx), K(ret));
      } else {
        ObDtlBasicChannel *ch = reinterpret_cast<ObDtlBasicChannel*>(dtl_ch);
        int64_t unblock_cnt = 0;
        if (dfc->is_qc_coord() && ch->has_less_buffer_cnt()) {
          // 对于merge sort coord的channel，保证每一个channel的recv_list都不为空，即扩展unblock条件
          // 否则merge sort receive可能死等，即被blocked的channel没法发送unblocking msg
          LOG_TRACE("unblock channel on decrease size by self", K(dfc), K(ret), KP(ch->get_id()), K(ch->get_peer()), K(ch_idx),
            K(ch->get_processed_buffer_cnt()));
          if (OB_FAIL(dfc->notify_channel_unblocking(ch, unblock_cnt))) {
            LOG_WARN("failed to unblock channel",
              K(dfc), K(ret), KP(ch->get_id()), K(ch->get_peer()), K(ch->belong_to_receive_data()),
              K(ch->belong_to_transmit_data()), K(ch->get_processed_buffer_cnt()));
          }
          decrease_blocked_channel_cnt(unblock_cnt);
        }
      }
    }
    LOG_TRACE("unblock channel on decrease size", K(dfc), K(ret), K(dfc->is_block()));
  }
  return ret;
}

int ObTenantDfc::unblock_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  dfc->decrease(size);
  decrease(size);
  if (OB_FAIL(try_unblock_tenant_dfc(dfc, ch_idx))) {
    LOG_WARN("failed to try unblock tenant dfc", K(ret));
  }
  return ret;
}

int ObTenantDfc::unblock_channel(ObDtlFlowControl *dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_unblock_tenant_dfc(dfc, ch_idx))) {
    LOG_WARN("failed to unblock all blocked channel", K(dfc), K(ret));
  }
  return ret;
}

int ObTenantDfc::unblock_channels(ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  if (dfc->is_block()) {
    int64_t unblock_cnt = 0;
    if (OB_FAIL(dfc->notify_all_blocked_channels_unblocking(unblock_cnt))) {
      LOG_WARN("failed to unblock all blocked channel", K(dfc), K(ret));
    }
    if (0 < unblock_cnt) {
      decrease_blocked_channel_cnt(unblock_cnt);
    }
    LOG_TRACE("unblock channel on decrease size", K(dfc), K(ret), K(unblock_cnt));
  }
  return ret;
}

int ObTenantDfc::block_tenant_dfc(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  dfc->increase(size);
  increase(size);
  //LOG_TRACE("tenant dfc size", K(dfc->get_used()), K(dfc->get_total_buffer_cnt()), K(tenant_dfc_.get_used()), K(tenant_dfc_.get_total_buffer_cnt()), K(need_block(dfc)));
  if (need_block(dfc)) {
    if (OB_FAIL(enforce_block(dfc, ch_idx))) {
      LOG_WARN("failed to block channel", K(size), K(dfc), K(ret), K(ch_idx));
    }
  }
  return ret;
}
// dfc server
int ObDfcServer::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObDfcServer::destroy()
{
}

int ObDfcServer::get_current_tenant_dfc(uint64_t tenant_id, ObTenantDfc *&tenant_dfc)
{
  int ret = OB_SUCCESS;
  tenant_dfc = nullptr;
  tenant_dfc = MTL(ObTenantDfc*);
  if (nullptr == tenant_dfc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create tenant dfc", K(ret), K(tenant_id));
  } else if (tenant_id != tenant_dfc->get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected tenant mtl", K(tenant_id), K(tenant_dfc->get_tenant_id()));
    // if (OB_SYS_TENANT_ID == tenant_dfc->get_tenant_id()) {
    //   // 这里是为了解决 sys 租户 change tenant 到其它租户后，要求能使用 dtl 服务
    //   // 否则有 bug：
    //   //
    //   // 进入这个分支的场景是：init_sqc 这个 rpc 在这个机器上没有找到租户资源，
    //   // 于是 fallback 到 sys 租户，于是 MTL 中取出的 dfc tenant id 为 sys 租户 id
    //   //
    //   // 此时：返回 sys 租户的 dfc 资源给调用者
    // } else {
    //   ret = OB_ERR_UNEXPECTED;
    //   LOG_WARN("the tenant id of tenant dfc is not match with tenant id hinted",
    //     K(ret), K(tenant_id), K(tenant_dfc->get_tenant_id()));
    // }
  }
  return ret;
}

ObDtlTenantMemManager *ObDfcServer::get_tenant_mem_manager(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDtlTenantMemManager *tenant_mem_manager = nullptr;
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else {
    tenant_mem_manager = tenant_dfc->get_tenant_mem_manager();
  }
  return tenant_mem_manager;
}

int ObDfcServer::block_on_increase_size(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->block_tenant_dfc(dfc, ch_idx, size))) {
    LOG_WARN("failed to block tenant dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::unblock_on_decrease_size(ObDtlFlowControl *dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unblock_tenant_dfc(dfc, ch_idx, size))) {
    LOG_WARN("failed to unblock tenant dfc", K(tenant_id), K(ch_idx), K(ret));
  }
  return ret;
}

int ObDfcServer::unblock_channel(ObDtlFlowControl *dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unblock_channel(dfc, ch_idx))) {
    LOG_WARN("failed to unblock tenant dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::unblock_channels(ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unblock_channels(dfc))) {
    LOG_WARN("failed to unblock tenant dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::register_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc.get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->register_dfc_channel(dfc, ch))) {
    LOG_WARN("failed to register dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::unregister_dfc_channel(ObDtlFlowControl &dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc.get_tenant_id();
  ObTenantDfc *tenant_dfc = nullptr;
  if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_dfc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unregister_dfc_channel(dfc, ch))) {
    LOG_WARN("failed to register dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::register_dfc(ObDtlFlowControl &dfc)
{
  UNUSED(dfc);
  return OB_SUCCESS;
}

int ObDfcServer::deregister_dfc(ObDtlFlowControl &dfc)
{
  int ret = OB_SUCCESS;
  if (dfc.is_init()) {
    uint64_t tenant_id = dfc.get_tenant_id();
    ObTenantDfc *tenant_dfc = nullptr;
    if (OB_FAIL(get_current_tenant_dfc(tenant_id, tenant_dfc))) {
      LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
    } else if (OB_ISNULL(tenant_dfc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant dfc is null", K(tenant_id), K(ret));
    } else if (OB_FAIL(tenant_dfc->deregister_dfc(dfc))) {
      LOG_WARN("failed to deregister dfc", K(tenant_id), K(ret));
    }
  }
  return ret;
}
