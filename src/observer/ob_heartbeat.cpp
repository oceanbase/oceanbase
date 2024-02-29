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

#include "observer/ob_heartbeat.h"

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_lease_struct.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_config_manager.h"
#include "share/ob_version.h"
#include "share/ob_zone_table_operation.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/ob_file_system_router.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "observer/ob_server_schema_updater.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "common/ob_timeout_ctx.h"
#include "storage/slog/ob_storage_logger_manager.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
namespace observer
{
using namespace storage;
using namespace blocksstable;
using namespace common;
using namespace share;
ObHeartBeatProcess::ObHeartBeatProcess(const ObGlobalContext &gctx,
                                       ObServerSchemaUpdater &schema_updater,
                                       ObLeaseStateMgr &lease_state_mgr)
  : inited_(false),
    update_task_(*this),
    zone_lease_info_(),
    newest_lease_info_version_(0),
    gctx_(gctx),
    schema_updater_(schema_updater),
    lease_state_mgr_(lease_state_mgr),
    server_id_persist_task_()
{
}

ObHeartBeatProcess::~ObHeartBeatProcess()
{}

int ObHeartBeatProcess::init()
{
  int ret = OB_SUCCESS;
  ObZone zone;
  const ObZone empty_zone = "";
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!gctx_.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ObHeartbeat))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (empty_zone == (zone = gctx_.config_->zone.str())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("zone must not be empty", K(zone), KR(ret));
  } else {
    zone_lease_info_.zone_ = zone;
    inited_ = true;
  }
  return ret;
}

void ObHeartBeatProcess::stop()
{
  TG_STOP(lib::TGDefIDs::ObHeartbeat);
}

void ObHeartBeatProcess::wait()
{
  TG_WAIT(lib::TGDefIDs::ObHeartbeat);
}

void ObHeartBeatProcess::destroy()
{
  TG_DESTROY(lib::TGDefIDs::ObHeartbeat);
}

#ifdef OB_BUILD_TDE_SECURITY
int ObHeartBeatProcess::set_lease_request_max_stored_versions(
    share::ObLeaseRequest &lease_request,
    const common::ObIArray<std::pair<uint64_t, uint64_t> > &max_stored_versions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < max_stored_versions.count(); ++i) {
    const std::pair<uint64_t, uint64_t> &src = max_stored_versions.at(i);
    std::pair<uint64_t, ObLeaseRequest::TLRqKeyVersion> dest;
    dest.first = src.first;
    dest.second.max_flushed_key_version_ = src.second;
    if (OB_FAIL(lease_request.tenant_max_flushed_key_version_.push_back(dest))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}
#endif

int ObHeartBeatProcess::init_lease_request(ObLeaseRequest &lease_request)
{
  int ret = OB_SUCCESS;
  common::ObArray<std::pair<uint64_t, uint64_t> > max_stored_versions;
  lease_request.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.ob_service_ is null", KR(ret), KP(GCTX.ob_service_));
  } else if (OB_FAIL((GCTX.ob_service_->get_server_resource_info(lease_request.resource_info_)))) {
    LOG_WARN("fail to get server resource info", KR(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_max_stored_versions(max_stored_versions))) {
    LOG_WARN("fail to get max stored versions", KR(ret));
  } else if (OB_FAIL(set_lease_request_max_stored_versions(lease_request, max_stored_versions))) {
    LOG_WARN("fail to set lease request max stored key versions",
             KR(ret), K(lease_request), K(max_stored_versions));
#endif
  } else if (OB_FAIL(get_package_and_svn(lease_request.build_version_, sizeof(lease_request.build_version_)))) {
    LOG_WARN("fail to get build_version", KR(ret));
  } else {
    lease_request.request_lease_time_ = 0; // this is not a valid member
    lease_request.version_ = ObLeaseRequest::LEASE_VERSION;
    lease_request.zone_ = gctx_.config_->zone.str();
    lease_request.server_ = gctx_.self_addr();
    lease_request.sql_port_ = gctx_.config_->mysql_port;
    OTC_MGR.get_lease_request(lease_request);
    lease_request.start_service_time_ = gctx_.start_service_time_;
    lease_request.ssl_key_expired_time_ = gctx_.ssl_key_expired_time_;
#ifdef ERRSIM
    common::ObZone err_zone("z3");
    const bool enable_disk_error_test = GCONF.enable_disk_error_test;
    lease_request.server_status_
      |= (err_zone == lease_request.zone_ && enable_disk_error_test) ? LEASE_REQUEST_DATA_DISK_ERROR : 0;
#else
    int tmp_ret = OB_SUCCESS;
    // TODO: add the func to check disk status
    const bool is_slog_disk_warning = false;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t abnormal_time = 0;
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().get_device_health_status(dhs, abnormal_time))) {
      CLOG_LOG(WARN, "get device health status failed", K(tmp_ret));
    } else if (OB_UNLIKELY(DEVICE_HEALTH_ERROR == dhs) || OB_UNLIKELY(is_slog_disk_warning)) {
      const int64_t PRINT_LOG_INTERVAL_IN_US = 60 * 1000 * 1000; // 1min
      if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_IN_US)) {
        LOG_WARN("error occurs on data disk or slog disk",
            "data_disk_health_status", device_health_status_to_str(dhs), K(abnormal_time), K(is_slog_disk_warning));
      }
      if (OB_FILE_SYSTEM_ROUTER.is_single_zone_deployment_on()) {
        dhs = DEVICE_HEALTH_NORMAL; // ignore this error in scs single zone.
      }
    }
    const bool is_data_disk_error = (DEVICE_HEALTH_ERROR == dhs);
    lease_request.server_status_ |= (is_data_disk_error || is_slog_disk_warning) ? LEASE_REQUEST_DATA_DISK_ERROR : 0;
#endif

  }
  return ret;
}

void ObHeartBeatProcess::check_and_update_server_id_(const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!is_valid_server_id(server_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_id", KR(ret), K(server_id));
  } else {
    // once server_id is confirmed, it cannnot be changed
    // in 4.1, server_id persistance is not supported, observer can only get its server_id via heartbeat
    // in 4.2, server_id is persisted when the server is added into the cluster
    // in upgrade period 4.1 -> 4.2, we need to persist the server_id via heartbeat
    const int64_t delay = 0;
    const bool repeat = false;
    if (0 == GCTX.server_id_) {
      GCTX.server_id_ = server_id;
      LOG_INFO("receive new server id in GCTX", K(server_id));
    } else if (server_id != GCTX.server_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("GCTX.server_id_ is not the same as server_id in RS", KR(ret),
          K(GCTX.server_id_), K(server_id));
    }
    if (OB_FAIL(ret)) {
    } else if (0 == GCONF.observer_id) {
      GCONF.observer_id = server_id;
      LOG_INFO("receive new server id in GCONF", K(server_id));
      if (OB_SUCCESS != (tmp_ret = TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, server_id_persist_task_, delay, repeat))) {
        server_id_persist_task_.enable_need_retry_flag();
        LOG_WARN("schedule server_id persist task failed", K(tmp_ret));
      } else {
        server_id_persist_task_.disable_need_retry_flag();
      }
    } else if (server_id != GCONF.observer_id) {
      ret = OB_ERR_UNEXPECTED;
      uint64_t server_id_in_GCONF = GCONF.observer_id;
      LOG_ERROR("GCONF.server_id is not the same as server_id in RS", KR(ret),
          K(server_id_in_GCONF), K(server_id));
    }
    if (server_id_persist_task_.is_need_retry()) {
      if (OB_SUCCESS != (tmp_ret = TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, server_id_persist_task_, delay, repeat))) {
        LOG_WARN("schedule server_id persist task failed", K(tmp_ret));
      } else {
        server_id_persist_task_.disable_need_retry_flag();
      }
    }
  }
}

//pay attention to concurrency control
int ObHeartBeatProcess::do_heartbeat_event(const ObLeaseResponse &lease_response)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!lease_response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_response", K(lease_response), KR(ret));
  } else if (ObLeaseResponse::LEASE_VERSION != lease_response.version_) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN("version mismatching", "version", lease_response.version_,
        LITERAL_K(ObLeaseResponse::LEASE_VERSION), KR(ret));
  } else {
    LOG_DEBUG("get lease_response", K(lease_response));
    int tmp_ret = OB_SUCCESS;
    (void) check_and_update_server_id_(lease_response.server_id_);
    if (!ObHeartbeatHandler::is_rs_epoch_id_valid()) {
      if (RSS_INVALID != lease_response.rs_server_status_) {
        if (GCTX.rs_server_status_ != lease_response.rs_server_status_) {
          LOG_INFO("receive new server status recorded in rs",
                  "old_status", GCTX.rs_server_status_,
                  "new_status", lease_response.rs_server_status_);
          GCTX.rs_server_status_ = lease_response.rs_server_status_;
        }
      }
    }
    // even try reload schema failed, we should continue do following things
    const bool set_received_schema_version = false;
    int schema_ret = schema_updater_.try_reload_schema(lease_response.refresh_schema_info_,
                                                       set_received_schema_version);

    if (OB_SUCCESS != schema_ret) {
      LOG_WARN("try reload schema failed", "schema_version", lease_response.schema_version_,
              "refresh_schema_info", lease_response.refresh_schema_info_, K(schema_ret));
    } else {
      LOG_INFO("try reload schema success", "schema_version", lease_response.schema_version_,
              "refresh_schema_info", lease_response.refresh_schema_info_, K(schema_ret));
    }

    // while rootservice startup, lease_info_version may be set to 0.
    if (lease_response.lease_info_version_ > 0) {
      newest_lease_info_version_ = lease_response.lease_info_version_;
    }
    bool is_exist = false;
    const int64_t delay = 0;
    const bool repeat = false;
    if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::ObHeartbeat, update_task_, is_exist))) {
      LOG_WARN("check exist failed", KR(ret));
    } else if (is_exist) {
      LOG_DEBUG("update task in scheduled, no need to schedule again");
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ObHeartbeat, update_task_, delay, repeat))) {
      LOG_WARN("schedule update zone lease info task failed", K(delay), K(repeat), KR(ret));
    }
    // generate the task for refreshing the Tenant-level configuration
    if (OB_SUCCESS != (tmp_ret = OTC_MGR.got_versions(lease_response.tenant_config_version_))) {
      LOG_WARN("tenant got versions failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObHeartBeatProcess::update_lease_info()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (newest_lease_info_version_ == zone_lease_info_.lease_info_version_) {
    LOG_DEBUG("newest version lease info already got, no need to update",
        K_(newest_lease_info_version));
  } else if (newest_lease_info_version_ < zone_lease_info_.lease_info_version_) {
    ret = OB_ERR_SYS;
    LOG_WARN("newest_lease_info_version_ is smaller than old lease_info_version",
        K_(newest_lease_info_version),
        "lease_info_version", zone_lease_info_.lease_info_version_, KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_zone_lease_info(
      *GCTX.sql_proxy_, zone_lease_info_))) {
    LOG_WARN("get zone lease info failed", KR(ret));
  } else {
    LOG_INFO("succeed to update cluster_lease_info", K_(zone_lease_info));
  }
  return ret;
}

int ObHeartBeatProcess::try_update_infos()
{
  int ret = OB_SUCCESS;
  const int64_t config_version = zone_lease_info_.config_version_;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(try_reload_config(config_version))) {
    LOG_WARN("try_reload_config failed", KR(ret), K(config_version));
  }

  return ret;
}

int ObHeartBeatProcess::try_reload_config(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (config_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_version", K(config_version), KR(ret));
  } else {
    ObConfigManager &config_mgr = *gctx_.config_mgr_;
    if (OB_FAIL(config_mgr.got_version(config_version, true))) {
      LOG_WARN("got_version failed", K(config_version), KR(ret));
    }
  }
  return ret;
}

ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::ObZoneLeaseInfoUpdateTask(
    ObHeartBeatProcess &hb_process)
  : hb_process_(hb_process)
{
}

ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::~ObZoneLeaseInfoUpdateTask()
{
}

void ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hb_process_.update_lease_info())) {
    LOG_WARN("update_lease_info failed", KR(ret));
  } else {
    // while rootservice startup, lease_info_version may be set to 0.
    if (OB_LIKELY(hb_process_.zone_lease_info_.lease_info_version_ > 0)) {
      if (OB_FAIL(hb_process_.try_update_infos())) {
        LOG_WARN("try_update_infos failed", KR(ret));
      }
    }
  }
}

void ObHeartBeatProcess::ObServerIdPersistTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  if (OB_NOT_NULL(GCTX.config_mgr_)) {
    if (OB_FAIL(GCTX.config_mgr_->dump2file())) {
      need_retry = true;
      LOG_WARN("dump server id to file failed", K(ret));
    }
  } else {
    need_retry = true;
    LOG_WARN("GCTX.config_mgr_ is NULL, observer may not init");
  }
  if (need_retry) {
    // retry server id persistence task in 1s later
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, *this, 1000 * 1000L, false))) {
      LOG_WARN("Reschedule server id persistence task failed", K(ret));
    }
  }
}

}//end namespace observer
}//end namespace oceanbase
