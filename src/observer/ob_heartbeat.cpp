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
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_build_index_scheduler.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "observer/ob_server_schema_updater.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase {
namespace observer {
using namespace storage;
using namespace blocksstable;
using namespace common;
using namespace share;
ObHeartBeatProcess::ObHeartBeatProcess(
    const ObGlobalContext& gctx, ObServerSchemaUpdater& schema_updater, ObLeaseStateMgr& lease_state_mgr)
    : inited_(false),
      update_task_(*this),
      zone_lease_info_(),
      newest_lease_info_version_(0),
      gctx_(gctx),
      schema_updater_(schema_updater),
      lease_state_mgr_(lease_state_mgr)
{}

ObHeartBeatProcess::~ObHeartBeatProcess()
{
  TG_DESTROY(lib::TGDefIDs::ObHeartbeat);
}

int ObHeartBeatProcess::init()
{
  int ret = OB_SUCCESS;
  ObZone zone;
  const ObZone empty_zone = "";
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!gctx_.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ObHeartbeat))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (empty_zone == (zone = gctx_.config_->zone.str())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("zone must not be empty", K(zone), K(ret));
  } else {
    zone_lease_info_.zone_ = zone;
    inited_ = true;
  }
  return ret;
}

int ObHeartBeatProcess::init_lease_request(ObLeaseRequest& lease_request)
{
  int ret = OB_SUCCESS;
  int64_t partition_cnt = 0;
  omt::ObTenantNodeBalancer::ServerResource cpu_mem_assigned;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(OBSERVER.get_partition_service().get_partition_count(partition_cnt))) {
    LOG_WARN("fail get partition count for lease request", K(ret));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().get_server_allocated_resource(cpu_mem_assigned))) {
    LOG_WARN("fail to get server allocated resource", K(ret));
  } else {
    lease_request.request_lease_time_ = 0;  // this is not a valid member
    lease_request.version_ = ObLeaseRequest::LEASE_VERSION;
    lease_request.zone_ = gctx_.config_->zone.str();
    lease_request.server_ = gctx_.self_addr_;
    lease_request.inner_port_ = gctx_.config_->mysql_port;
    lease_request.resource_info_.cpu_ = gctx_.omt_->get_node_quota();
    lease_request.resource_info_.report_cpu_assigned_ = cpu_mem_assigned.min_cpu_;
    lease_request.resource_info_.report_cpu_max_assigned_ = cpu_mem_assigned.max_cpu_;
    lease_request.resource_info_.report_mem_assigned_ = cpu_mem_assigned.min_memory_;
    lease_request.resource_info_.report_mem_max_assigned_ = cpu_mem_assigned.max_memory_;
    lease_request.resource_info_.mem_in_use_ = 0;
    lease_request.resource_info_.mem_total_ = GCONF.get_server_memory_avail();
    lease_request.resource_info_.disk_total_ =
        OB_FILE_SYSTEM.get_total_macro_block_count() * OB_FILE_SYSTEM.get_macro_block_size();
    lease_request.resource_info_.disk_in_use_ =
        OB_FILE_SYSTEM.get_used_macro_block_count() * OB_FILE_SYSTEM.get_macro_block_size();
    lease_request.resource_info_.partition_cnt_ = partition_cnt;
    get_package_and_svn(lease_request.build_version_, sizeof(lease_request.build_version_));
    OTC_MGR.get_lease_request(lease_request);
    lease_request.start_service_time_ = gctx_.start_service_time_;
    lease_request.ssl_key_expired_time_ = gctx_.ssl_key_expired_time_;
    lease_request.timeout_partition_ = gctx_.get_sync_timeout_partition_cnt();
#ifdef ERRSIM
    common::ObZone err_zone("z3");
    const bool enable_disk_error_test = GCONF.enable_disk_error_test;
    lease_request.server_status_ |=
        (err_zone == lease_request.zone_ && enable_disk_error_test) ? LEASE_REQUEST_DATA_DISK_ERROR : 0;
#else
    int tmp_ret = OB_SUCCESS;
    bool is_disk_error = false;
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().is_disk_error_definite(is_disk_error))) {
      CLOG_LOG(WARN, "is_disk_error_definite failed", K(tmp_ret));
    }
    bool is_slog_ok = true;
    if (OB_SUCCESS != (tmp_ret = SLOGGER.is_logger_ok(is_slog_ok))) {
      CLOG_LOG(WARN, "is_logger_ok failed", K(tmp_ret));
    }
    lease_request.server_status_ |= (is_disk_error || !is_slog_ok) ? LEASE_REQUEST_DATA_DISK_ERROR : 0;
#endif
  }
  return ret;
}

// pay attention to concurrency control
int ObHeartBeatProcess::do_heartbeat_event(const ObLeaseResponse& lease_response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_response", K(lease_response), K(ret));
  } else if (ObLeaseResponse::LEASE_VERSION != lease_response.version_) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN(
        "version mismatching", "version", lease_response.version_, LITERAL_K(ObLeaseResponse::LEASE_VERSION), K(ret));
  } else {
    LOG_DEBUG("get lease_response", K(lease_response));

    if (OB_INVALID_ID != lease_response.server_id_) {
      if (GCTX.server_id_ != lease_response.server_id_) {
        LOG_INFO("receive new server id", "old_id", GCTX.server_id_, "new_id", lease_response.server_id_);
        GCTX.server_id_ = lease_response.server_id_;
      }
    }

    // even try reload schema failed, we should continue do following things
    int schema_ret = OB_SUCCESS;
    if (lease_response.refresh_schema_info_.is_valid()) {
      schema_ret = schema_updater_.try_reload_schema(lease_response.refresh_schema_info_);
    } else {
      schema_ret = schema_updater_.try_reload_schema(lease_response.schema_version_);
    }

    if (OB_SUCCESS != schema_ret) {
      LOG_WARN("try reload schema failed",
          "schema_version",
          lease_response.schema_version_,
          "refresh_schema_info",
          lease_response.refresh_schema_info_,
          K(schema_ret));
    } else {
      LOG_INFO("try reload schema success",
          "schema_version",
          lease_response.schema_version_,
          "refresh_schema_info",
          lease_response.refresh_schema_info_,
          K(schema_ret));
    }

    const int64_t delay = 0;
    const bool repeat = false;
    // while rootservice startup, lease_info_version may be set to 0.
    if (lease_response.lease_info_version_ > 0) {
      newest_lease_info_version_ = lease_response.lease_info_version_;
    }
    bool is_exist = false;
    if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::ObHeartbeat, update_task_, is_exist))) {
      LOG_WARN("check exist failed", K(ret));
    } else if (is_exist) {
      LOG_DEBUG("update task in scheduled, no need to schedule again");
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ObHeartbeat, update_task_, delay, repeat))) {
      LOG_WARN("schedule update zone lease info task failed", K(delay), K(repeat), K(ret));
    }
    // generate the task for refreshing the Tenant-level configuration
    common::ObSEArray<std::pair<uint64_t, int64_t>, 10> versions = lease_response.tenant_config_version_;
    if (OB_SUCCESS != (tmp_ret = OTC_MGR.got_versions(versions))) {
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
    LOG_WARN("not init", K(ret));
  } else if (newest_lease_info_version_ == zone_lease_info_.lease_info_version_) {
    LOG_DEBUG("newest version lease info already got, no need to update", K_(newest_lease_info_version));
  } else if (newest_lease_info_version_ < zone_lease_info_.lease_info_version_) {
    ret = OB_ERR_SYS;
    LOG_WARN("newest_lease_info_version_ is smaller than old lease_info_version",
        K_(newest_lease_info_version),
        "lease_info_version",
        zone_lease_info_.lease_info_version_,
        K(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_zone_lease_info(*GCTX.sql_proxy_, zone_lease_info_))) {
    LOG_WARN("get zone lease info failed", K(ret));
  } else {
    LOG_INFO("succeed to update cluster_lease_info", K_(zone_lease_info));
  }
  return ret;
}

int ObHeartBeatProcess::try_update_infos()
{
  int ret = OB_SUCCESS;
  int temp_ret = OB_SUCCESS;
  const int64_t config_version = zone_lease_info_.config_version_;
  const int64_t broadcast_version = zone_lease_info_.broadcast_version_;
  const int64_t proposal_frozen_version = zone_lease_info_.proposal_frozen_version_;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // for test only
    // it will be deleted after has the processing of invalid index
    {
      ObPartitionScheduler& scheduler = ObPartitionScheduler::get_instance();
      if (proposal_frozen_version > scheduler.get_frozen_version()) {
        LOG_WARN("XXXX", K(broadcast_version), K(proposal_frozen_version), K(scheduler.get_frozen_version()));
      }
    }
    if (zone_lease_info_.suspend_merging_) {
      ObPartitionScheduler::get_instance().stop_merge();
    } else {
      ObPartitionScheduler::get_instance().resume_merge();
    }

    if (OB_SUCCESS != (temp_ret = try_start_merge(broadcast_version))) {
      LOG_WARN("try_start_merge failed", K(broadcast_version), K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }

    if (OB_SUCCESS != (temp_ret = try_reload_config(config_version))) {
      LOG_WARN("try_reload_config failed", K(config_version), K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }

    if (zone_lease_info_.last_merged_version_ > *(gctx_.merged_version_)) {
      *(gctx_.merged_version_) = zone_lease_info_.last_merged_version_;
    }

    if (zone_lease_info_.global_last_merged_version_ > *(gctx_.global_last_merged_version_)) {
      *(gctx_.global_last_merged_version_) = zone_lease_info_.global_last_merged_version_;
    }

    if (zone_lease_info_.warm_up_start_time_ != *(gctx_.warm_up_start_time_)) {
      *(gctx_.warm_up_start_time_) = zone_lease_info_.warm_up_start_time_;
    }
  }

  return ret;
}

int ObHeartBeatProcess::try_start_merge(const int64_t broadcast_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (broadcast_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid broadcast_version", K(broadcast_version), K(ret));
  } else {
    const int64_t frozen_version = broadcast_version;
    ObPartitionScheduler& scheduler = ObPartitionScheduler::get_instance();

    if (frozen_version > scheduler.get_frozen_version()) {
      if (OB_FAIL(scheduler.schedule_merge(frozen_version))) {
        LOG_WARN("Fail to schedule merge", K(frozen_version), K(ret));
      }
    }
  }

  return ret;
}

int ObHeartBeatProcess::try_reload_config(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (config_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_version", K(config_version), K(ret));
  } else {
    ObConfigManager& config_mgr = *gctx_.config_mgr_;
    if (OB_FAIL(config_mgr.got_version(config_version, true))) {
      LOG_WARN("got_version failed", K(config_version), K(ret));
    }
  }
  return ret;
}

ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::ObZoneLeaseInfoUpdateTask(ObHeartBeatProcess& hb_process)
    : hb_process_(hb_process)
{}

ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::~ObZoneLeaseInfoUpdateTask()
{}

void ObHeartBeatProcess::ObZoneLeaseInfoUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hb_process_.update_lease_info())) {
    LOG_WARN("update_lease_info failed", K(ret));
  } else {
    // while rootservice startup, lease_info_version may be set to 0.
    if (OB_LIKELY(hb_process_.zone_lease_info_.lease_info_version_ > 0)) {
      if (OB_FAIL(hb_process_.try_update_infos())) {
        LOG_WARN("try_update_infos failed", K(ret));
      }
    }
  }
}

}  // end namespace observer
}  // end namespace oceanbase
