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
#include "storage/ob_partition_service.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/schema/ob_schema_service.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_lease_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
namespace oceanbase {
namespace observer {

void ObGlobalContext::init()
{
  SpinWLockGuard guard(cluster_info_rwlock_);
  cluster_info_.reset();
  server_status_ = share::OBSERVER_INVALID_STATUS;
  cluster_idx_ = OB_INVALID_INDEX;
  sync_standby_cluster_id_ = OB_INVALID_ID;
  sync_standby_redo_options_.reset();
  sync_timeout_partition_cnt_ = 0;
}

ObGlobalContext& global_context()
{
  return OBSERVER.get_gctx();
}

bool ObGlobalContext::is_observer() const
{
  return !share::schema::ObSchemaService::g_liboblog_mode_;
}

bool ObGlobalContext::is_primary_cluster() const
{
  return true;
}

bool ObGlobalContext::is_standby_cluster() const
{
  return false;
}

common::ObClusterType ObGlobalContext::get_cluster_type() const
{
  return common::PRIMARY_CLUSTER;
}

share::ServerServiceStatus ObGlobalContext::get_server_service_status() const
{
  return share::OBSERVER_ACTIVE;
}

bool ObGlobalContext::can_be_parent_cluster() const
{
  return true;
}

bool ObGlobalContext::can_do_leader_takeover() const
{
  return true;
}

int64_t ObGlobalContext::get_switch_epoch2() const
{
  return 0;
}

int64_t ObGlobalContext::get_pure_switch_epoch() const
{
  return 0;
}

int ObGlobalContext::get_sync_standby_cluster_id(int64_t& sync_cluster_id)
{
  int ret = OB_SUCCESS;
  sync_cluster_id = ATOMIC_LOAD(&sync_standby_cluster_id_);
  return ret;
}

int ObGlobalContext::get_sync_standby_cluster_list(common::ObIArray<int64_t>& sync_standby)
{
  int ret = OB_SUCCESS;
  sync_standby.reset();
  const int64_t cluster_id = ATOMIC_LOAD(&sync_standby_cluster_id_);
  if (OB_FAIL(sync_standby.push_back(cluster_id))) {
    LOG_WARN("failed to push back cluster id", KR(ret), K(cluster_id));
  }
  return ret;
}

void ObGlobalContext::get_cluster_type_and_status(
    common::ObClusterType& cluster_type, share::ServerServiceStatus& server_status) const
{
  cluster_type = common::PRIMARY_CLUSTER;
  server_status = share::OBSERVER_ACTIVE;
}

bool ObGlobalContext::is_in_standby_switching_state() const
{
  return false;
}

bool ObGlobalContext::is_in_phy_fb_mode() const
{
  return false;
}

bool ObGlobalContext::is_in_phy_fb_verify_mode() const
{
  return false;
}

bool ObGlobalContext::is_in_flashback_state() const
{
  return false;
}

bool ObGlobalContext::is_in_cleanup_state() const
{
  return false;
}

bool ObGlobalContext::is_in_standby_active_state() const
{
  return false;
}

bool ObGlobalContext::is_in_invalid_state() const
{
  return false;
}

void ObGlobalContext::set_split_schema_version(int64_t new_split_schema_version)
{
  int64_t old_split_schema_version = split_schema_version_;
  if (new_split_schema_version != old_split_schema_version) {
    LOG_INFO("split_schema_version changed", K(old_split_schema_version), K(new_split_schema_version));
  }
  split_schema_version_ = new_split_schema_version;
}

void ObGlobalContext::set_split_schema_version_v2(int64_t new_split_schema_version_v2)
{
  int64_t old_split_schema_version_v2 = split_schema_version_v2_;
  if (new_split_schema_version_v2 != old_split_schema_version_v2) {
    LOG_INFO("split_schema_version_v2 changed", K(old_split_schema_version_v2), K(new_split_schema_version_v2));
  }
  split_schema_version_v2_ = new_split_schema_version_v2;
}

int64_t ObGlobalContext::get_cluster_idx() const
{
  return 0;
}

void ObGlobalContext::set_cluster_idx(const int64_t cluster_idx)
{
  UNUSED(cluster_idx);
}

bool ObGlobalContext::is_in_disabled_state() const
{
  return false;
}

DEF_TO_STRING(ObGlobalContext)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(self_addr),
      KP_(root_service),
      KP_(ob_service),
      KP_(schema_service),
      KP_(part_mgr),
      KP_(config),
      KP_(config_mgr),
      KP_(pt_operator),
      KP_(srv_rpc_proxy),
      KP_(rs_rpc_proxy),
      KP_(load_data_proxy),
      KP_(executor_rpc),
      KP_(sql_proxy),
      KP_(rs_mgr),
      KP_(par_ser),
      KP_(gts),
      KP_(bandwidth_throttle),
      KP_(vt_par_ser),
      KP_(session_mgr),
      KP_(sql_engine),
      KP_(omt),
      KP_(vt_iter_creator),
      KP_(location_cache),
      K_(start_time),
      KP_(merged_version),
      KP_(global_last_merged_version),
      KP_(warm_up_start_time),
      K_(sync_standby_cluster_id));
  J_COMMA();
  J_KV(K_(server_id),
      K_(status),
      K_(start_service_time),
      KP_(sort_dir),
      KP_(diag),
      KP_(scramble_rand),
      K_(split_schema_version),
      K_(split_schema_version_v2),
      KP_(weak_read_service),
      KP_(schema_status_proxy),
      K_(ssl_key_expired_time),
      K_(inited));
  J_OBJ_END();
  return pos;
}

ObGlobalContext& ObGlobalContext::operator=(const ObGlobalContext& other)
{
  if (this != &other) {
    SpinWLockGuard guard(cluster_info_rwlock_);
    self_addr_ = other.self_addr_;
    root_service_ = other.root_service_;
    ob_service_ = other.ob_service_;
    schema_service_ = other.schema_service_;
    part_mgr_ = other.part_mgr_;
    config_ = other.config_;
    config_mgr_ = other.config_mgr_;
    pt_operator_ = other.pt_operator_;
    remote_pt_operator_ = other.remote_pt_operator_;
    srv_rpc_proxy_ = other.srv_rpc_proxy_;
    rs_rpc_proxy_ = other.rs_rpc_proxy_;
    load_data_proxy_ = other.load_data_proxy_;
    executor_rpc_ = other.executor_rpc_;
    sql_proxy_ = other.sql_proxy_;
    remote_sql_proxy_ = other.remote_sql_proxy_;
    rs_mgr_ = other.rs_mgr_;
    par_ser_ = other.par_ser_;
    gts_ = other.gts_;
    bandwidth_throttle_ = other.bandwidth_throttle_;
    vt_par_ser_ = other.vt_par_ser_;
    session_mgr_ = other.session_mgr_;
    sql_engine_ = other.sql_engine_;
    omt_ = other.omt_;
    vt_iter_creator_ = other.vt_iter_creator_;
    location_cache_ = other.location_cache_;
    start_time_ = other.start_time_;
    merged_version_ = other.merged_version_;
    global_last_merged_version_ = other.global_last_merged_version_;
    warm_up_start_time_ = other.warm_up_start_time_;
    server_id_ = other.server_id_;
    status_ = other.status_;
    rs_server_status_ = other.rs_server_status_;
    start_service_time_ = other.start_service_time_;
    ssl_key_expired_time_ = other.ssl_key_expired_time_;
    sort_dir_ = other.sort_dir_;
    diag_ = other.diag_;
    scramble_rand_ = other.scramble_rand_;
    cgroup_ctrl_ = other.cgroup_ctrl_;
    inited_ = other.inited_;
    split_schema_version_ = other.split_schema_version_;
    split_schema_version_v2_ = other.split_schema_version_v2_;
    cluster_info_ = other.cluster_info_;
    weak_read_service_ = other.weak_read_service_;
    schema_status_proxy_ = other.schema_status_proxy_;
    sync_standby_cluster_id_ = other.sync_standby_cluster_id_;
  }
  return *this;
}

ObUseWeakGuard::ObUseWeakGuard()
{
  auto* tsi_value = GET_TSI(TSIUseWeak);
  if (NULL != tsi_value) {
    tsi_value->inited_ = true;
    tsi_value->did_use_weak_ = GCTX.is_standby_cluster_and_started();
    ;
  } else {
    LOG_ERROR("tsi value is NULL");
  }
}

ObUseWeakGuard::~ObUseWeakGuard()
{
  auto* tsi_value = GET_TSI(TSIUseWeak);
  if (NULL != tsi_value) {
    tsi_value->inited_ = false;
  }
}

bool ObUseWeakGuard::did_use_weak()
{
  bool use_weak = false;
  auto* tsi_value = GET_TSI(TSIUseWeak);
  if (NULL == tsi_value) {
    LOG_ERROR("tsi value is NULL");
    use_weak = GCTX.is_standby_cluster_and_started();
  } else if (tsi_value->inited_) {
    use_weak = tsi_value->did_use_weak_;
  } else {
    use_weak = GCTX.is_standby_cluster_and_started();
  }
  return use_weak;
}

void __attribute__((constructor(MALLOC_INIT_PRIORITY))) disable_coro()
{
  ::oceanbase::common::USE_CO_LATCH = false;
  ::oceanbase::lib::CO_FORCE_SYSCALL_FUTEX();
}

}  // end of namespace observer
}  // end of namespace oceanbase
