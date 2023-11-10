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

#define USING_LOG_PREFIX SERVER
#include "ob_server_struct.h"
#include "observer/ob_server.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/schema/ob_schema_service.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_lease_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
namespace oceanbase
{
namespace observer
{

void ObGlobalContext::init()
{
  server_status_ = share::OBSERVER_INVALID_STATUS;
}

ObGlobalContext &global_context()
{
  return OBSERVER.get_gctx();
}

bool ObGlobalContext::is_observer() const { return !share::schema::ObSchemaService::g_liboblog_mode_; }

bool ObGlobalContext::is_primary_cluster() const
{
  return true;;
}

bool ObGlobalContext::is_standby_cluster() const
{
  return false;
}

common::ObClusterRole ObGlobalContext::get_cluster_role() const
{
  return PRIMARY_CLUSTER;
}

share::ServerServiceStatus ObGlobalContext::get_server_service_status() const
{
  int64_t server_status = ATOMIC_LOAD(&server_status_);
  return static_cast<share::ServerServiceStatus>(server_status);
}

DEF_TO_STRING(ObGlobalContext)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(self_addr_seq),
       KP_(root_service),
       KP_(ob_service),
       KP_(schema_service),
       KP_(config),
       KP_(config_mgr),
       KP_(lst_operator),
       KP_(tablet_operator),
       KP_(srv_rpc_proxy),
       KP_(storage_rpc_proxy),
       KP_(rs_rpc_proxy),
       KP_(load_data_proxy),
       KP_(executor_rpc),
       KP_(sql_proxy),
       KP_(rs_mgr),
       KP_(bandwidth_throttle),
       KP_(vt_par_ser),
       KP_(session_mgr),
       KP_(sql_engine),
       KP_(omt),
       KP_(vt_iter_creator),
       KP_(batch_rpc),
       KP_(server_tracer),
       K_(start_time),
       KP_(warm_up_start_time));
  J_COMMA();
  J_KV(K_(server_id),
       K_(status),
       K_(start_service_time),
       KP_(diag),
       KP_(scramble_rand),
       KP_(weak_read_service),
       KP_(schema_status_proxy),
       K_(ssl_key_expired_time),
       K_(inited));
  J_OBJ_END();
  return pos;
}

ObGlobalContext &ObGlobalContext::operator=(const ObGlobalContext &other)
{
  if (this != &other) {
    self_addr_seq_ = other.self_addr_seq_;
    root_service_ = other.root_service_;
    ob_service_ = other.ob_service_;
    schema_service_ = other.schema_service_;
    config_ = other.config_;
    config_mgr_ = other.config_mgr_;
    lst_operator_ = other.lst_operator_;
    tablet_operator_ = other.tablet_operator_;
    srv_rpc_proxy_ = other.srv_rpc_proxy_;
    storage_rpc_proxy_ = other.storage_rpc_proxy_;
    rs_rpc_proxy_ = other.rs_rpc_proxy_;
    load_data_proxy_ = other.load_data_proxy_;
    inner_sql_rpc_proxy_ = other.inner_sql_rpc_proxy_;
    executor_rpc_ = other.executor_rpc_;
    sql_proxy_ = other.sql_proxy_;
    rs_mgr_ = other.rs_mgr_;
    bandwidth_throttle_ = other.bandwidth_throttle_;
    vt_par_ser_ = other.vt_par_ser_;
    session_mgr_ = other.session_mgr_;
    sql_engine_ = other.sql_engine_;
    pl_engine_ = other.pl_engine_;
    omt_ = other.omt_;
    vt_iter_creator_ = other.vt_iter_creator_;
    start_time_ = other.start_time_;
    warm_up_start_time_ = other.warm_up_start_time_;
    server_id_ = other.server_id_;
    status_ = other.status_;
    rs_server_status_ = other.rs_server_status_;
    start_service_time_ = other.start_service_time_;
    ssl_key_expired_time_ = other.ssl_key_expired_time_;
    diag_ = other.diag_;
    scramble_rand_ = other.scramble_rand_;
    table_service_ = other.table_service_;
    cgroup_ctrl_ = other.cgroup_ctrl_;
    inited_ = other.inited_;
    weak_read_service_ = other.weak_read_service_;
    schema_status_proxy_ = other.schema_status_proxy_;
    net_frame_ = other.net_frame_;
    rl_mgr_ = other.rl_mgr_;
    batch_rpc_ = other.batch_rpc_;
    server_tracer_ = other.server_tracer_;
  }
  return *this;
}

ObUseWeakGuard::ObUseWeakGuard()
{
  auto *tsi_value = GET_TSI(TSIUseWeak);
  if (NULL != tsi_value) {
    tsi_value->inited_ = true;
    tsi_value->did_use_weak_ = GCTX.is_standby_cluster_and_started();;
  } else {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "tsi value is NULL");
  }
}

ObUseWeakGuard::~ObUseWeakGuard()
{
  auto *tsi_value = GET_TSI(TSIUseWeak);
  if (NULL != tsi_value) {
    tsi_value->inited_ = false;
  }
}

bool ObUseWeakGuard::did_use_weak()
{
  bool use_weak = false;
  auto *tsi_value = GET_TSI(TSIUseWeak);
  if (NULL == tsi_value) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "tsi value is NULL");
    use_weak = GCTX.is_standby_cluster_and_started();
  } else if (tsi_value->inited_) {
    use_weak = tsi_value->did_use_weak_;
  } else {
    use_weak = GCTX.is_standby_cluster_and_started();
  }
  return use_weak;
}

} // end of namespace observer
} // end of namespace oceanbase
