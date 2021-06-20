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

#include "ob_ha_gts_source.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_srv_rpc_proxy.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "ob_ha_gts.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace transaction;
namespace gts {
int ObHaGtsSource::ObHaGtsSourceTask::init(ObHaGtsSource* gts_source)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gts_source)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    gts_source_ = gts_source;
  }
  return ret;
}

void ObHaGtsSource::ObHaGtsSourceTask::runTimerTask()
{
  if (OB_ISNULL(gts_source_)) {
    STORAGE_LOG(ERROR, "ObHaGtsSourceTask is not inited");
  } else {
    gts_source_->runTimerTask();
  }
}

class ObHaGtsSource::GetTenantArrayFunctor {
public:
  GetTenantArrayFunctor(const transaction::MonotonicTs srr) : srr_(srr)
  {
    tenant_array_.reset();
  }
  ~GetTenantArrayFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObGtsMeta& gts_meta)
  {
    UNUSED(gts_id);
    int tmp_ret = OB_SUCCESS;
    const ObTenantLeaseArray& tenant_lease_array = gts_meta.get_tenant_lease_array();
    for (int64_t i = 0; i < tenant_lease_array.count(); i++) {
      const uint64_t tenant_id = tenant_lease_array[i].tenant_id_;
      const transaction::MonotonicTs lease_begin = tenant_lease_array[i].lease_begin_;
      const transaction::MonotonicTs lease_end = tenant_lease_array[i].lease_end_;
      if (srr_ >= lease_begin && srr_ < lease_end) {
        if (OB_SUCCESS != (tmp_ret = tenant_array_.push_back(tenant_id))) {
          STORAGE_LOG(WARN, "tenant_array_ push_back failed", K(tmp_ret));
        }
      }
    }
    return true;
  }
  const ObTenantArray& get_tenant_array() const
  {
    return tenant_array_;
  }

private:
  transaction::MonotonicTs srr_;
  ObTenantArray tenant_array_;
};

class ObHaGtsSource::ModifyTenantCacheFunctor {
public:
  ModifyTenantCacheFunctor(const transaction::MonotonicTs srr, const int64_t gts) : srr_(srr), gts_(gts)
  {}
  ~ModifyTenantCacheFunctor()
  {}

public:
  bool operator()(const ObTenantID& tenant_id, ObTenantCache& tenant_cache)
  {
    UNUSED(tenant_id);
    if (tenant_cache.get_srr() < srr_) {
      tenant_cache.set_srr_and_gts(srr_, gts_);
    }
    return true;
  }

private:
  transaction::MonotonicTs srr_;
  int64_t gts_;
};

class ObHaGtsSource::RefreshGtsFunctor {
public:
  RefreshGtsFunctor(ObHaGtsSource* source) : host_(source)
  {}
  ~RefreshGtsFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObGtsMeta& gts_meta)
  {
    const uint64_t gts = gts_id.get_value();
    const common::ObMemberList& member_list = gts_meta.get_member_list();
    if (member_list.get_member_number() >= 1) {
      const common::ObAddr gts_server = choose_gts_server_(member_list);
      host_->refresh_gts(gts, gts_server);
    } else {
      STORAGE_LOG(ERROR, "no gts server", K(gts));
    }
    STORAGE_LOG(TRACE, "RefreshGtsFunctor", K(gts_id), K(gts_meta));
    return true;
  }

private:
  const common::ObAddr choose_gts_server_(const common::ObMemberList& member_list) const
  {
    int tmp_ret = OB_SUCCESS;
    const int64_t rand_num = ObRandom::rand(0, member_list.get_member_number() - 1);
    common::ObAddr addr;
    if (OB_SUCCESS != (tmp_ret = member_list.get_server_by_index(rand_num, addr))) {
      STORAGE_LOG(ERROR, "get_server_by_index failed", K(tmp_ret));
    }
    return addr;
  }
  ObHaGtsSource* host_;
};

class ObHaGtsSource::ModifyGtsMetaFunctor {
public:
  ModifyGtsMetaFunctor(const ObGtsIDArray& gts_id_array, const ObGtsMetaArray& gts_meta_array)
      : gts_id_array_(gts_id_array), gts_meta_array_(gts_meta_array)
  {}
  ~ModifyGtsMetaFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObGtsMeta& gts_meta)
  {
    bool bool_ret = false;
    const int64_t gts = gts_id.get_value();
    int64_t index = 0;
    if (is_gts_exist_(gts, index)) {
      const ObGtsMeta& new_gts_meta = gts_meta_array_[index];
      handle_gts_meta_(new_gts_meta, gts_meta);
    } else {
      // gts already deleted
      bool_ret = true;
    }
    return bool_ret;
  }

private:
  bool is_gts_exist_(const uint64_t gts_id, int64_t& index) const
  {
    bool bool_ret = false;
    for (int64_t i = 0; (!bool_ret) && i < gts_id_array_.count(); i++) {
      if (gts_id == gts_id_array_[i]) {
        bool_ret = true;
        index = i;
      }
    }
    return bool_ret;
  }
  bool is_tenant_lease_exist_(
      const uint64_t tenant_id, const ObTenantLeaseArray& curr_tenant_lease_array, int64_t& index) const
  {
    bool bool_ret = false;
    for (int64_t i = 0; (!bool_ret) && i < curr_tenant_lease_array.count(); i++) {
      if (tenant_id == curr_tenant_lease_array[i].tenant_id_) {
        bool_ret = true;
        index = i;
      }
    }
    return bool_ret;
  }
  void handle_gts_meta_(const ObGtsMeta& new_gts_meta, ObGtsMeta& curr_gts_meta) const
  {
    int ret = OB_SUCCESS;
    ObTenantLeaseArray tmp_tenant_lease_array;
    const ObTenantLeaseArray& new_tenant_lease_array = new_gts_meta.get_tenant_lease_array();
    const ObTenantLeaseArray& curr_tenant_lease_array = curr_gts_meta.get_tenant_lease_array();
    for (int64_t i = 0; (OB_SUCCESS == ret) && i < new_tenant_lease_array.count(); i++) {
      const uint64_t tenant_id = new_tenant_lease_array[i].tenant_id_;
      int64_t index = 0;
      if (is_tenant_lease_exist_(tenant_id, curr_tenant_lease_array, index)) {
        const ObTenantLease& curr_tenant_lease = curr_tenant_lease_array[index];
        ObTenantLease tmp_tenant_lease;
        tmp_tenant_lease.tenant_id_ = tenant_id;
        tmp_tenant_lease.lease_begin_ =
            std::min(new_tenant_lease_array[i].lease_begin_, curr_tenant_lease.lease_begin_);
        tmp_tenant_lease.lease_end_ = std::max(new_tenant_lease_array[i].lease_end_, curr_tenant_lease.lease_end_);
        if (OB_FAIL(tmp_tenant_lease_array.push_back(tmp_tenant_lease))) {
          STORAGE_LOG(WARN, "tmp_tenant_lease_array push_back failed", K(ret), K(tenant_id));
        }
      } else if (OB_FAIL(tmp_tenant_lease_array.push_back(new_tenant_lease_array[i]))) {
        STORAGE_LOG(WARN, "tmp_tenant_lease_array push_back failed", K(ret), K(tenant_id));
      }
    }
    if (OB_SUCCESS == ret) {
      curr_gts_meta.set_member_list(new_gts_meta.get_member_list());
      curr_gts_meta.set_tenant_lease_array(tmp_tenant_lease_array);
    }
  }

private:
  const ObGtsIDArray& gts_id_array_;
  const ObGtsMetaArray& gts_meta_array_;
};

ObHaGtsSource::ObHaGtsSource()
{
  reset();
}

ObHaGtsSource::~ObHaGtsSource()
{
  destroy();
}

int ObHaGtsSource::init(
    obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "ObHaGtsSource is inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy) || OB_ISNULL(sql_proxy) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), KP(rpc_proxy), KP(sql_proxy), K(self_addr));
  } else if (OB_FAIL(gts_table_operator_.init(sql_proxy))) {
    STORAGE_LOG(WARN, "gts_table_operator_ init failed", K(ret));
  } else if (OB_FAIL(gts_meta_map_.init(ObModIds::OB_HA_GTS_GTS_META_MAP))) {
    STORAGE_LOG(WARN, "gts_meta_map_ init failed", K(ret));
  } else if (OB_FAIL(tenant_cache_map_.init(ObModIds::OB_HA_GTS_TENANT_CACHE_MAP))) {
    STORAGE_LOG(WARN, "tenant_cache_map_ init failed", K(ret));
  } else if (OB_FAIL(task_.init(this))) {
    STORAGE_LOG(ERROR, "task_ init failed", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    STORAGE_LOG(ERROR, "timer_ init failed", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_addr_ = self_addr;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  STORAGE_LOG(INFO, "ObHaGtsSource is inited", K(ret));
  return ret;
}

void ObHaGtsSource::reset()
{
  is_inited_ = false;
  gts_meta_map_.reset();
  tenant_cache_map_.reset();
  // task_/timer_ no need reset
  rpc_proxy_ = NULL;
  self_addr_.reset();
  latest_refresh_ts_ = 0;
  latest_trigger_ts_ = 0;
  STORAGE_LOG(INFO, "ObHaGtsSource reset");
}

void ObHaGtsSource::destroy()
{
  is_inited_ = false;
  gts_meta_map_.destroy();
  tenant_cache_map_.destroy();
  // task_ no need destroy
  timer_.destroy();
  rpc_proxy_ = NULL;
  self_addr_.reset();
  STORAGE_LOG(INFO, "ObHaGtsSource destroy");
}

int ObHaGtsSource::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(timer_.schedule(task_, TIMER_TASK_INTERVAL, repeat))) {
    STORAGE_LOG(ERROR, "fail to schedule task", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    STORAGE_LOG(ERROR, "share::ObThreadPool::start failed", K(ret));
  } else {
    // do nothing
  }
  STORAGE_LOG(INFO, "ObHaGtsSource start finished", K(ret));
  return ret;
}

void ObHaGtsSource::stop()
{
  timer_.stop();
  share::ObThreadPool::stop();
  STORAGE_LOG(INFO, "ObHaGtsSource stop finished");
}

void ObHaGtsSource::wait()
{
  timer_.wait();
  share::ObThreadPool::wait();
  STORAGE_LOG(INFO, "ObHaGtsSource wait finished");
}

int ObHaGtsSource::get_gts(const uint64_t tenant_id, const MonotonicTs stc, int64_t& gts) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(tenant_id);
  UNUSED(stc);
  UNUSED(gts);
  return ret;
  //  int ret = OB_SUCCESS;
  //  ObTenantCache tenant_cache;
  //  ObTenantID tenant_id_wrapper(tenant_id);
  //  if (IS_NOT_INIT) {
  //    ret = OB_NOT_INIT;
  //    STORAGE_LOG(WARN, "ObHaGtsSource is not inited", K(ret));
  //  } else if (!is_valid_no_sys_tenant_id(tenant_id) || stc <= 0) {
  //    ret = OB_INVALID_ARGUMENT;
  //    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(stc));
  //  } else if (OB_FAIL(tenant_cache_map_.get(tenant_id_wrapper, tenant_cache))) {
  //    if (OB_ENTRY_NOT_EXIST == ret) {
  //      ret = OB_NOT_SUPPORTED;
  //    } else {
  //      STORAGE_LOG(WARN, "tenant_cache_map_ get failed", K(ret), K(tenant_id));
  //    }
  //  } else if (stc <= tenant_cache.get_srr()) {
  //    gts = tenant_cache.get_gts();
  //  } else {
  //    ret = OB_EAGAIN;
  //  }
  //  STORAGE_LOG(TRACE, "get_gts finished", K(ret), K(tenant_id), K(stc), K(gts));
  //  return ret;
}

int ObHaGtsSource::trigger_gts()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsSource is not inited", K(ret));
  } else {
    const int64_t latest_refresh_ts = ATOMIC_LOAD(&latest_refresh_ts_);
    const bool is_refreshing = is_refreshing_(latest_refresh_ts);
    const int64_t saved_refresh_ts = get_refresh_ts_(latest_refresh_ts);
    const int64_t curr_time = ObClockGenerator::getCurrentTime();
    const int64_t new_refresh_ts = set_refreshing_(curr_time);
    if (!is_refreshing && curr_time - saved_refresh_ts >= ObServerConfig::get_instance().gts_refresh_interval &&
        ATOMIC_BCAS(&latest_refresh_ts_, latest_refresh_ts, new_refresh_ts)) {
      // The following operations will only be called by a single thread
      refresh_gts_();
      const int64_t new_refresh_ts2 = reset_refreshing_(new_refresh_ts);
      ATOMIC_STORE(&latest_refresh_ts_, new_refresh_ts2);
    }

    ATOMIC_STORE(&latest_trigger_ts_, curr_time);
    STORAGE_LOG(TRACE, "trigger_gts", K(saved_refresh_ts), K(curr_time));
  }
  return ret;
}

int ObHaGtsSource::handle_get_response(const obrpc::ObHaGtsGetResponse& response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t gts_id = response.get_gts_id();
  const transaction::MonotonicTs srr = response.get_srr();
  const int64_t gts = response.get_gts();
  const ObGtsID gts_id_wrapper(gts_id);
  GetTenantArrayFunctor gts_meta_functor(srr);
  // ModifyTenantCacheFunctor tenant_cache_functor(srr, gts);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsSource is not inited", K(ret));
  } else if (!response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(response));
  } else if (OB_FAIL(gts_meta_map_.operate(gts_id_wrapper, gts_meta_functor))) {
    STORAGE_LOG(WARN, "gts_meta_map_ operate failed", K(ret), K(gts_id));
  } else {
    const ObTenantArray& tenant_array = gts_meta_functor.get_tenant_array();
    for (int64_t i = 0; i < tenant_array.count(); i++) {
      const uint64_t tenant_id = tenant_array[i];
      const ObTenantID tenant_id_wrapper(tenant_id);
      // Due to the conflict between replacing the MONOTONIC clock source and some logics of ha_gts, support in the next
      // stage
      if (OB_SUCCESS != (tmp_ret = OB_TS_MGR.handle_ha_gts_response(tenant_id, srr, gts))) {
        STORAGE_LOG(WARN, "handle_ha_gts_response failed", K(ret), K(gts_id), K(tenant_id));
      }
      // if (OB_SUCCESS != (tmp_ret = tenant_cache_map_.operate(tenant_id_wrapper,
      //                                                       tenant_cache_functor))) {
      //  STORAGE_LOG(WARN, "tenant_info_map_ operate failed", K(tmp_ret), K(gts_id), K(tenant_id));
      // }
    }
  }
  STORAGE_LOG(TRACE, "handle_get_response finished", K(ret), K(response));
  return ret;
}

void ObHaGtsSource::run1()
{
  lib::set_thread_name("HAGtsSource");
  while (!has_set_stop()) {
    const int64_t begin_time = ObTimeUtility::current_time();
    // get trigger_ts, refresh_ts in order
    const int64_t latest_trigger_ts = ATOMIC_LOAD(&latest_trigger_ts_);
    const int64_t latest_refresh_ts = get_refresh_ts_(ATOMIC_LOAD(&latest_refresh_ts_));
    if (begin_time - latest_refresh_ts >= RETRY_TRIGGER_INTERVAL || latest_trigger_ts > latest_refresh_ts) {
      trigger_gts();
    }
    int32_t sleep_interval = static_cast<int32_t>(
        ObServerConfig::get_instance().gts_refresh_interval - (ObTimeUtility::current_time() - begin_time));
    if (sleep_interval < 0) {
      sleep_interval = 0;
    }
    usleep(sleep_interval);
  }
}

void ObHaGtsSource::runTimerTask()
{
  load_gts_meta_map_();
  return;
}

void ObHaGtsSource::refresh_gts(const uint64_t gts_id, const common::ObAddr& gts_server)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGtsSource is not inited", K(ret));
  } else if (!is_valid_gts_id(gts_id) || !gts_server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(gts_id), K(gts_server));
  } else {
    obrpc::ObHaGtsGetRequest request;
    const transaction::MonotonicTs srr = transaction::MonotonicTs::current_time();
    request.set(gts_id, self_addr_, 0, srr);
    if (OB_SUCCESS != (ret = rpc_proxy_->to(gts_server)
                                 .by(OB_SERVER_TENANT_ID)
                                 .timeout(ObHaGts::HA_GTS_RPC_TIMEOUT)
                                 .ha_gts_get_request(request, NULL))) {
      STORAGE_LOG(WARN, "ha_gts_get_request failed", K(ret));
    }
  }
  STORAGE_LOG(TRACE, "refresh_gts finished", K(ret), K(gts_id), K(gts_server));
}

void ObHaGtsSource::refresh_gts_()
{
  RefreshGtsFunctor functor(this);
  gts_meta_map_.for_each(functor);
}

void ObHaGtsSource::load_gts_meta_map_()
{
  int ret = OB_SUCCESS;
  ObGtsIDArray gts_id_array;
  ObGtsMetaArray gts_meta_array;
  if (OB_FAIL(get_gts_meta_array_(gts_id_array, gts_meta_array))) {
    STORAGE_LOG(WARN, "get_gts_meta_array_ failed", K(ret));
  } else if (OB_FAIL(handle_gts_meta_array_(gts_id_array, gts_meta_array))) {
    STORAGE_LOG(WARN, "handle_gts_meta_array_ failed", K(ret));
  }
}

int ObHaGtsSource::get_gts_meta_array_(ObGtsIDArray& gts_id_array, ObGtsMetaArray& gts_meta_array)
{
  int ret = OB_SUCCESS;
  ObGtsTenantInfoArray gts_tenant_info_array;
  const MonotonicTs lease_begin = transaction::MonotonicTs::current_time();
  const MonotonicTs lease_end = lease_begin + MonotonicTs(HA_GTS_SOURCE_LEASE);
  if (OB_FAIL(gts_table_operator_.get_gts_tenant_infos(gts_tenant_info_array))) {
    STORAGE_LOG(WARN, "get_gts_tenant_infos failed", K(ret));
  } else {
    uint64_t gts_id = 0;
    ObGtsMeta gts_meta;
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_tenant_info_array.count(); i++) {
      const ObGtsTenantInfo& gts_tenant_info = gts_tenant_info_array[i];
      if (!gts_tenant_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "gts_tenant_info is invalid", K(ret), K(gts_tenant_info));
      } else {
        if (gts_id != gts_tenant_info.gts_id_) {
          if (i != 0) {
            if (OB_FAIL(gts_id_array.push_back(gts_id))) {
              STORAGE_LOG(WARN, "gts_id_array push_back failed", K(ret), K(i), K(gts_id));
            } else if (OB_FAIL(gts_meta_array.push_back(gts_meta))) {
              STORAGE_LOG(WARN, "gts_meta_array push_back failed", K(ret), K(i), K(gts_id));
            }
          }

          if (OB_SUCC(ret)) {
            gts_id = gts_tenant_info.gts_id_;
            gts_meta.reset();
            gts_meta.set_member_list(gts_tenant_info.member_list_);
          }
        }

        if (OB_SUCC(ret)) {
          ObTenantLease tenant_lease;
          tenant_lease.tenant_id_ = gts_tenant_info.tenant_id_;
          tenant_lease.lease_begin_ = lease_begin;
          tenant_lease.lease_end_ = lease_end;

          if (OB_FAIL(gts_meta.get_tenant_lease_array().push_back(tenant_lease))) {
            STORAGE_LOG(WARN, "tenant_lease_array push_back failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && gts_tenant_info_array.count() != 0) {
      if (OB_FAIL(gts_id_array.push_back(gts_id))) {
        STORAGE_LOG(WARN, "gts_id_array push_back failed", K(ret), K(gts_id));
      } else if (OB_FAIL(gts_meta_array.push_back(gts_meta))) {
        STORAGE_LOG(WARN, "gts_meta_array push_back failed", K(ret), K(gts_id));
      }
    }
  }
  STORAGE_LOG(INFO, "get_gts_meta_array_", K(ret), K(gts_id_array), K(gts_meta_array));

  return ret;
}

int ObHaGtsSource::handle_gts_meta_array_(const ObGtsIDArray& gts_id_array, const ObGtsMetaArray& gts_meta_array)
{
  int ret = OB_SUCCESS;
  ModifyGtsMetaFunctor functor(gts_id_array, gts_meta_array);
  if (OB_FAIL(gts_meta_map_.remove_if(functor))) {
    STORAGE_LOG(WARN, "gts_meta_map_ remove_if failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < gts_id_array.count(); i++) {
    const uint64_t gts_id = gts_id_array[i];
    const ObGtsMeta& gts_meta = gts_meta_array[i];
    const ObGtsID gts_id_wrapper(gts_id);
    ObGtsMeta tmp_gts_meta;

    for (int64_t j = 0; OB_SUCC(ret) && j < gts_meta.get_tenant_lease_array().count(); j++) {
      const uint64_t tenant_id = gts_meta.get_tenant_lease_array()[j].tenant_id_;
      const ObTenantID tenant_id_wrapper(tenant_id);
      ObTenantCache tenant_cache;
      if (OB_FAIL(tenant_cache_map_.get(tenant_id_wrapper, tenant_cache)) && OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "tenant_cache_map_ get failed", K(ret), K(tenant_id));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        tenant_cache.reset();
        if (OB_FAIL(tenant_cache_map_.insert(tenant_id_wrapper, tenant_cache))) {
          STORAGE_LOG(WARN, "tenant_cache_map_ insert failed", K(ret), K(tenant_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(gts_meta_map_.get(gts_id_wrapper, tmp_gts_meta)) && OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(ERROR, "gts_meta_map_ get failed", K(ret), K(gts_id));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(gts_meta_map_.insert(gts_id_wrapper, gts_meta))) {
          STORAGE_LOG(ERROR, "gts_meta_map_ insert failed", K(ret), K(gts_id));
        }
      }
    }
  }
  return ret;
}

int64_t ObHaGtsSource::get_refresh_ts_(const int64_t refresh_ts)
{
  return refresh_ts & (~REFRESHING_FLAG);
}

bool ObHaGtsSource::is_refreshing_(const int64_t refresh_ts)
{
  return refresh_ts & REFRESHING_FLAG;
}

int64_t ObHaGtsSource::set_refreshing_(const int64_t refresh_ts)
{
  return refresh_ts | REFRESHING_FLAG;
}

int64_t ObHaGtsSource::reset_refreshing_(const int64_t refresh_ts)
{
  return refresh_ts & (~REFRESHING_FLAG);
}
}  // namespace gts
}  // namespace oceanbase
