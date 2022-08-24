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

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::lib;
using namespace oceanbase::share;

// ObTenantDfc
ObTenantDfc::ObTenantDfc(uint64_t tenant_id)
    : tenant_dfc_(),
      tenant_id_(tenant_id),
      blocked_dfc_cnt_(0),
      channel_total_cnt_(0),
      max_parallel_cnt_(0),
      max_blocked_buffer_size_(0),
      max_buffer_size_(0),
      tenant_mem_mgr_(tenant_id),
      first_buffer_mgr_(tenant_id, &tenant_mem_mgr_)
{}

ObTenantDfc::~ObTenantDfc()
{}

int ObTenantDfc::mtl_init(uint64_t tenant_id, ObTenantDfc*& tenant_dfc)
{
  int ret = OB_SUCCESS;
  tenant_dfc = OB_NEW(ObTenantDfc, common::ObModIds::OB_SQL_DTL, tenant_id);
  if (nullptr == tenant_dfc) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tenant dfc", K(ret));
  } else {
    tenant_dfc->channel_total_cnt_ = 0;
    tenant_dfc->blocked_dfc_cnt_ = 0;
    tenant_dfc->max_parallel_cnt_ = 0;
    tenant_dfc->max_blocked_buffer_size_ = 0;
    tenant_dfc->max_buffer_size_ = 0;
    tenant_dfc->tenant_id_ = tenant_id;
    if (OB_FAIL(tenant_dfc->tenant_mem_mgr_.init())) {
      LOG_WARN("failed to init tenant memory manager", K(ret));
    } else if (OB_FAIL(tenant_dfc->first_buffer_mgr_.init())) {
      LOG_WARN("failed to init newe first buffer manager", K(ret));
    }
    // tenant_dfc->calc_max_buffer(10);
    LOG_INFO("init tenant dfc", K(ret), K(tenant_dfc->tenant_id_));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != tenant_dfc) {
      tenant_dfc->first_buffer_mgr_.destroy();
      tenant_dfc->tenant_mem_mgr_.destroy();
      common::ob_delete(tenant_dfc);
      tenant_dfc = nullptr;
    }
    LOG_WARN("failed to mtl init", K(ret));
  }
  return ret;
}

void ObTenantDfc::mtl_destroy(ObTenantDfc*& tenant_dfc)
{
  if (nullptr != tenant_dfc) {
    tenant_dfc->first_buffer_mgr_.destroy();
    tenant_dfc->tenant_mem_mgr_.destroy();
    common::ob_delete(tenant_dfc);
    tenant_dfc = nullptr;
  }
}

void ObTenantDfc::check_dtl_buffer_size()
{
  uint64_t tenant_id = get_tenant_id();
  if (OB_SYS_TENANT_ID != tenant_id && OB_MAX_RESERVED_TENANT_ID >= tenant_id) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else {
    int ret = OB_SUCCESS;
    int64_t parallel_max_servers = 0;
    if (OB_SUCC(share::schema::ObSchemaUtils::get_tenant_int_variable(
            tenant_id, SYS_VAR_PARALLEL_MAX_SERVERS, parallel_max_servers))) {
      calc_max_buffer(parallel_max_servers);
    }
  }
}

void ObTenantDfc::clean_buffer_on_time()
{
  uint64_t tenant_id = get_tenant_id();
  if (OB_SYS_TENANT_ID != tenant_id && OB_MAX_RESERVED_TENANT_ID >= tenant_id) {
    // Except for system rentals, internal tenants do not allocate px threads
  } else {
    int ret = OB_SUCCESS;
    if (OB_FAIL(clean_on_timeout())) {
      LOG_WARN("failed to clean buffer on time", K(ret));
    }
    LOG_INFO("tenant dfc status",
        K(ret),
        K(get_tenant_id()),
        K(get_channel_cnt()),
        K(get_current_buffer_used()),
        K(get_current_blocked_cnt()),
        K(get_current_buffer_cnt()),
        K(get_max_parallel()),
        K(get_max_blocked_buffer_size()),
        K(get_max_buffer_size()),
        K(get_accumulated_blocked_cnt()),
        K(get_max_size_per_channel()));
  }
}

void ObTenantDfc::calc_max_buffer(int64_t max_parallel_cnt)
{
  if (0 == max_parallel_cnt) {
    max_parallel_cnt = 1;
  }
  max_parallel_cnt_ = max_parallel_cnt;
  max_blocked_buffer_size_ = max_parallel_cnt_ * (MAX_BUFFER_CNT + 2) * GCONF.dtl_buffer_size * MAX_BUFFER_FACTOR / 2;
  max_buffer_size_ = max_blocked_buffer_size_ * MAX_BUFFER_FACTOR;
  LOG_INFO("trace tenant dfc parameters", K(max_parallel_cnt_), K(max_blocked_buffer_size_), K(max_buffer_size_));
}

int ObTenantDfc::register_dfc_channel(ObDtlFlowControl& dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfc.register_channel(ch))) {
    LOG_WARN("failed to regiester channel", KP(ch->get_id()), K(ret));
  } else {
    increase_channel_cnt(1);
  }
  return ret;
}

int ObTenantDfc::unregister_dfc_channel(ObDtlFlowControl& dfc, ObDtlChannel* ch)
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

int ObTenantDfc::deregister_dfc(ObDtlFlowControl& dfc)
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

int ObTenantDfc::clean_on_timeout()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = get_tenant_id();
  bool with_tenant_res = false;
  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    with_tenant_res = true;
  }
  if (OB_FAIL(tenant_mem_mgr_.auto_free_on_time(with_tenant_res))) {
    LOG_WARN("failed to auto free memory manager", K(ret));
  }
  return ret;
}

int ObTenantDfc::cache_buffer(int64_t chid, ObDtlLinkedBuffer*& data_buffer, bool attach)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(data_buffer->has_dfo_key())) {
    if (OB_FAIL(first_buffer_mgr_.cache_buffer(chid, data_buffer, attach))) {
      LOG_WARN("failed to cache buffer", K(ret), KP(chid), K(attach));
    }
  } else {
    // some compatibility handling
    ret = OB_NOT_SUPPORTED;
    LOG_TRACE("old data not cache", K(ret), KP(chid), K(attach));
  }
  return ret;
}

int ObTenantDfc::get_buffer_cache(ObDtlDfoKey& key, ObDtlLocalFirstBufferCache*& buf_cache)
{
  int ret = OB_SUCCESS;
  buf_cache = nullptr;
  if (OB_FAIL(first_buffer_mgr_.get_buffer_cache(key, buf_cache))) {
    LOG_WARN("failed to get cache buffer", "dfc_tenant", get_tenant_id(), K(ret), K(key));
  } else if (nullptr == buf_cache) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: buffer cache is null", K(ret));
  } else {
    buf_cache->release();
  }
  return ret;
}

int ObTenantDfc::try_process_first_buffer_by_qc(ObDtlFlowControl* dfc, ObDtlChannel* ch, int64_t ch_idx, bool& got)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer* data_buffer = nullptr;
  int64_t chid = ch->get_id();
  got = false;
  ObDtlLocalFirstBufferCache* buf_cache = dfc->get_first_buffer_cache();
  if (nullptr == buf_cache) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: first buffer cache is null", K(ret));
  } else if (OB_FAIL(first_buffer_mgr_.get_cached_buffer(buf_cache, chid, data_buffer))) {
    LOG_WARN("failed to get cache buffer", K(ch->get_id()));
  } else if (nullptr != data_buffer) {
    bool is_eof = data_buffer->is_eof();
    if (!is_eof && OB_FAIL(enforce_block(dfc, ch_idx))) {
      LOG_WARN("failed to enforce block", K(ret), KP(chid), K(ch_idx));
    } else if (OB_FAIL(ch->attach(data_buffer, true))) {
      LOG_WARN("failed to feedup data buffer", K(ret), KP(chid));
    } else {
      got = true;
      LOG_TRACE("process first msg", KP(chid), K(ch->get_peer()), K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (nullptr != data_buffer && OB_SUCCESS != (tmp_ret = tenant_mem_mgr_.free(data_buffer))) {
      LOG_WARN("failed to free data buffer", KP(chid), K(ret), K(tmp_ret), K(lbt()));
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObTenantDfc::try_process_first_buffer(ObDtlFlowControl* dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  ObDtlChannel* ch = nullptr;
  if (OB_FAIL(dfc->get_channel(ch_idx, ch))) {
    LOG_WARN("failed to get dtl channel", K(dfc), K(ch_idx), K(ret));
  } else {
    bool got = false;
    if (dfc->has_dfo_key()) {
      if (OB_FAIL(try_process_first_buffer_by_qc(dfc, ch, ch_idx, got))) {
        LOG_WARN("failed to process first buffer", K(ch->get_id()), K(ch_idx), K(dfc->get_dfo_key()));
      }
    }
  }
  return ret;
}

int ObTenantDfc::enforce_block(ObDtlFlowControl* dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  if (!dfc->is_block(ch_idx)) {
    increase_blocked_channel_cnt();
    dfc->set_block(ch_idx);
    LOG_TRACE("receive set channel block trace", K(dfc), K(ret), K(ch_idx));
  }
  return ret;
}

int ObTenantDfc::unblock_tenant_dfc(ObDtlFlowControl* dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  dfc->decrease(size);
  decrease(size);
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
      ObDtlChannel* dtl_ch = nullptr;
      if (OB_FAIL(dfc->get_channel(ch_idx, dtl_ch))) {
        LOG_WARN("failed to get dtl channel", K(dfc), K(ch_idx), K(ret));
      } else {
        ObDtlBasicChannel* ch = reinterpret_cast<ObDtlBasicChannel*>(dtl_ch);
        int64_t unblock_cnt = 0;
        if (dfc->is_qc_coord() && ch->has_less_buffer_cnt()) {
          LOG_TRACE("unblock channel on decrease size by self",
              K(dfc),
              K(ret),
              KP(ch->get_id()),
              K(ch->get_peer()),
              K(ch_idx),
              K(ch->get_processed_buffer_cnt()));
          if (OB_FAIL(dfc->notify_channel_unblocking(ch, unblock_cnt))) {
            LOG_WARN("failed to unblock channel",
                K(dfc),
                K(ret),
                KP(ch->get_id()),
                K(ch->get_peer()),
                K(ch->belong_to_receive_data()),
                K(ch->belong_to_transmit_data()),
                K(ch->get_processed_buffer_cnt()));
          }
          decrease_blocked_channel_cnt(unblock_cnt);
        }
      }
    }
    LOG_TRACE("unblock channel on decrease size", K(dfc), K(ret));
  }
  return ret;
}

int ObTenantDfc::unblock_channels(ObDtlFlowControl* dfc)
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

int ObTenantDfc::block_tenant_dfc(ObDtlFlowControl* dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  dfc->increase(size);
  increase(size);
  // LOG_TRACE("tenant dfc size", K(dfc->get_used()), K(dfc->get_total_buffer_cnt()), K(tenant_dfc_.get_used()),
  // K(tenant_dfc_.get_total_buffer_cnt()), K(need_block(dfc)));
  if (need_block(dfc)) {
    if (OB_FAIL(enforce_block(dfc, ch_idx))) {
      LOG_WARN("failed to block channel", K(size), K(dfc), K(ret), K(ch_idx));
    }
  }
  return ret;
}
int ObTenantDfc::register_first_buffer_cache(ObDtlLocalFirstBufferCache* buf_cache)
{
  return first_buffer_mgr_.register_first_buffer_cache(buf_cache);
}

int ObTenantDfc::unregister_first_buffer_cache(ObDtlDfoKey& key, ObDtlLocalFirstBufferCache* org_buf_cache)
{
  return first_buffer_mgr_.unregister_first_buffer_cache(key, org_buf_cache);
}

int ObDfcServer::init()
{
  int ret = OB_SUCCESS;
  // refresh dfc info every 10 seconds
  const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L;
  if (OB_FAIL(
          tenant_dfc_map_.create(common::OB_MAX_SERVER_TENANT_CNT, "DtlDfcMap", "DtlDfcNode", OB_SERVER_TENANT_ID))) {
    LOG_WARN("fail create tenant dfc map", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::DtlDfc))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::DtlDfc, *this, REFRESH_INTERVAL, true /* repeat */))) {
    LOG_WARN("fail define timer schedule", K(ret));
  } else {
    LOG_INFO("DfcServer init OK");
  }
  return ret;
}

void ObDfcServer::stop()
{
  TG_STOP(lib::TGDefIDs::DtlDfc);
  LOG_INFO("DtlDfc timer stopped");
}

void ObDfcServer::destroy()
{
  TG_DESTROY(lib::TGDefIDs::DtlDfc);
  LOG_INFO("DtlDfc timer destroy");
}

void ObDfcServer::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantDfc*> dfc_list;
  (void)dfc_list.reserve(tenant_dfc_map_.size());
  {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    for (DfcHashMapIterator iter = tenant_dfc_map_.begin(); iter != tenant_dfc_map_.end(); ++iter) {
      ObTenantDfc *dfc = iter->second;
      if (OB_ISNULL(dfc)) {
        LOG_WARN("null ptr, unexpected. ignore this tenant and continue", K(ret));
      } else if (OB_FAIL(dfc_list.push_back(dfc))) {
        LOG_WARN("fail push back dfc. ignore tenant and continue", K(ret), K(dfc->get_tenant_id()));
      }
    }
  }
  FOREACH(it, dfc_list)
  {
    (*it)->check_dtl_buffer_size();
    (*it)->clean_buffer_on_time();
  }
}

int ObDfcServer::get_tenant_dfc_by_id(uint64_t tenant_id, ObTenantDfc*& tenant_dfc)
{
  int ret = OB_SUCCESS;
  tenant_dfc = nullptr;
  // get tenant_dfc from cache, if not exist, create one. never release until server restart
  if (OB_FAIL(tenant_dfc_map_.get_refactored(tenant_id, tenant_dfc))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail get tenant dfc", K(tenant_id), K(ret));
    } else {
      ObLockGuard<ObSpinLock> lock_guard(lock_);
      if (OB_SUCC(tenant_dfc_map_.get_refactored(tenant_id, tenant_dfc))) {
        // some one has already created it, bypass
      } else if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail get dfc", K(tenant_id), K(ret));
      } else if (OB_FAIL(ObTenantDfc::mtl_init(tenant_id, tenant_dfc))) {
        LOG_WARN("fail create tenant dfc for tenant", K(tenant_id), K(ret));
      } else if (OB_FAIL(tenant_dfc_map_.set_refactored(tenant_id, tenant_dfc))) {
        LOG_WARN("fail set tenant dfc to map", K(tenant_id), KP(tenant_dfc), K(ret));
      }
    }
  }
  return ret;
}

ObDtlTenantMemManager* ObDfcServer::get_tenant_mem_manager(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObDtlTenantMemManager* tenant_mem_manager = nullptr;
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else {
    tenant_mem_manager = tenant_dfc->get_tenant_mem_manager();
  }
  return tenant_mem_manager;
}

int ObDfcServer::block_on_increase_size(ObDtlFlowControl* dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->block_tenant_dfc(dfc, ch_idx, size))) {
    LOG_WARN("failed to block tenant dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::unblock_on_decrease_size(ObDtlFlowControl* dfc, int64_t ch_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unblock_tenant_dfc(dfc, ch_idx, size))) {
    LOG_WARN("failed to unblock tenant dfc", K(tenant_id), K(ch_idx), K(ret));
  }
  return ret;
}

int ObDfcServer::unblock_channels(ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unblock_channels(dfc))) {
    LOG_WARN("failed to unblock tenant dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::cache(uint64_t tenant_id, int64_t chid, ObDtlLinkedBuffer*& data_buffer, bool attach)
{
  int ret = OB_SUCCESS;
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->cache_buffer(chid, data_buffer, attach))) {
    LOG_WARN("failed to cache data buffer", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::cache(int64_t chid, ObDtlLinkedBuffer*& data_buffer, bool attach)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = data_buffer->tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->cache_buffer(chid, data_buffer, attach))) {
    LOG_WARN("failed to cache data buffer", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::try_process_first_buffer(ObDtlFlowControl* dfc, int64_t ch_idx)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc->get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->try_process_first_buffer(dfc, ch_idx))) {
    LOG_WARN("failed to process first cache buffer", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::register_dfc_channel(ObDtlFlowControl& dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc.get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->register_dfc_channel(dfc, ch))) {
    LOG_WARN("failed to register dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::unregister_dfc_channel(ObDtlFlowControl& dfc, ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = dfc.get_tenant_id();
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unregister_dfc_channel(dfc, ch))) {
    LOG_WARN("failed to register dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::register_dfc(ObDtlFlowControl& dfc)
{
  UNUSED(dfc);
  return OB_SUCCESS;
}

int ObDfcServer::deregister_dfc(ObDtlFlowControl& dfc)
{
  int ret = OB_SUCCESS;
  if (dfc.is_init()) {
    uint64_t tenant_id = dfc.get_tenant_id();
    ObTenantDfc* tenant_dfc = nullptr;
    if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
      LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
    } else if (OB_FAIL(tenant_dfc->deregister_dfc(dfc))) {
      LOG_WARN("failed to deregister dfc", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObDfcServer::get_buffer_cache(uint64_t tenant_id, ObDtlDfoKey& key, ObDtlLocalFirstBufferCache*& buf_cache)
{
  int ret = OB_SUCCESS;
  buf_cache = nullptr;
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (key.is_valid() && OB_FAIL(tenant_dfc->get_buffer_cache(key, buf_cache))) {
    LOG_WARN("failed to deregister dfc", K(tenant_id), K(ret));
  }
  return ret;
}

int ObDfcServer::register_first_buffer_cache(uint64_t tenant_id, ObDtlLocalFirstBufferCache* buf_cache)
{
  int ret = OB_SUCCESS;
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (buf_cache->get_key().is_valid() && OB_FAIL(tenant_dfc->register_first_buffer_cache(buf_cache))) {
    LOG_WARN("failed to deregister dfc", K(ret));
  }
  return ret;
}

int ObDfcServer::unregister_first_buffer_cache(
    uint64_t tenant_id, ObDtlDfoKey& key, ObDtlLocalFirstBufferCache* org_buf_cache)
{
  int ret = OB_SUCCESS;
  ObTenantDfc* tenant_dfc = nullptr;
  if (OB_FAIL(get_tenant_dfc_by_id(tenant_id, tenant_dfc))) {
    LOG_WARN("failed to get tenant dfc", K(tenant_id), K(ret));
  } else if (OB_FAIL(tenant_dfc->unregister_first_buffer_cache(key, org_buf_cache))) {
    LOG_WARN("failed to deregister dfc", K(ret));
  }
  return ret;
}
