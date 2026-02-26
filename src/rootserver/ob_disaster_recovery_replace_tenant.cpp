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

#ifdef OB_BUILD_SHARED_LOG_SERVICE
#ifdef OB_BUILD_SHARED_STORAGE
#ifdef OB_BUILD_TDE_SECURITY
#include "ob_disaster_recovery_replace_tenant.h"
#include "observer/ob_service.h"
#include "common/ob_member.h"
#include "common/ob_member_list.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_disaster_recovery_task_utils.h"
#include "rootserver/ob_server_zone_op_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/ob_file_op.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_ss_cluster_info.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "share/ob_version.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_share_util.h"
#include "share/ob_replica_info.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_master_key_getter.h"
#include "share/ob_server_table_operator.h"
#include "share/ob_unit_table_operator.h"
#include "share/unit/ob_resource_pool.h"
#include "share/ob_zone_table_operation.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/object_storage/ob_device_connectivity.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_cluster_event_history_table_operator.h"
#include "share/unit/ob_unit_config.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "logservice/palf/log_meta_info.h"
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_env_ffi_instance.h"
#include "close_modules/shared_log_service/logservice/libpalf/libpalf_common_define.h"

namespace oceanbase
{
namespace rootserver
{
#define DRRT_LOG_INFO(fmt, args...) FLOG_INFO("[DR_REPLACE_TENANT] " fmt, ##args)
#define DRRT_LOG_WARN(fmt, args...) FLOG_WARN("[DR_REPLACE_TENANT] " fmt, ##args)

int ObDRReplaceTenant::init(const sql::ObReplaceTenantStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx_, REPLACE_TENANT_TIMEOUT))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else if (OB_UNLIKELY(!stmt.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(stmt_.assign(stmt))) {
    LOG_WARN("fail to assign stmt", KR(ret));
  } else {
    inited_ = true;
  }
  DRRT_LOG_INFO("init replace tenant", KR(ret), "timeout", ctx_.get_timeout());
  return ret;
}

int ObDRReplaceTenant::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRService is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!stmt_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (ctx_.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("check inner stat timeout", KR(ret), K_(ctx));
  }
  DRRT_LOG_INFO("check inner stat", KR(ret), "timeout", ctx_.get_timeout());
  return ret;
}

int ObDRReplaceTenant::do_replace_tenant()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_BEGIN, DBA_STEP_INC_INFO(replace_sys),
                  "[DR_REPLACE_TENANT] start to replace sys tenant");
  DRRT_LOG_INFO("start do replace tenant");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(basic_check_for_replace_tenant_())) {
    LOG_WARN("fail to check for replace tenant", KR(ret));
  } else if (OB_FAIL(set_in_replace_sys_())) {
    LOG_WARN("fail to set in replace sys", KR(ret));
  } else if (OB_FAIL(check_and_init_for_replace_tenant_())) {
    LOG_WARN("fail to check for replace tenant", KR(ret));
  } else if (OB_FAIL(create_unit_())) {
    LOG_WARN("fail to check for replace tenant", KR(ret));
  } else if (OB_FAIL(get_and_init_server_id_())) {
    LOG_WARN("fail to init server id", KR(ret));
  } else if (OB_FAIL(replace_sys_tenant_())) {
    LOG_WARN("fail to replace sys tenant ls", KR(ret));
  } else if (OB_FAIL(wait_sys_tenant_ready_())) {
    LOG_WARN("fail to wait sys tenant full schema", KR(ret));
  } else if (OB_FAIL(correct_inner_table_())) {
    LOG_WARN("fail to replace sys tenant ls", KR(ret));
  } else if (OB_FAIL(set_finish_replace_sys_())) {
    LOG_WARN("fail to set finish replace sys", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(clean_up_on_failure_())) {
      LOG_WARN("fail to clean up on failure", KR(tmp_ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys success");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys fail");
  }
  DRRT_LOG_INFO("finish replace tenant", KR(ret));
  return ret;
}

int ObDRReplaceTenant::set_in_replace_sys_()
{
  int ret = OB_SUCCESS;
  if (GCTX.atomic_set_replace_sys(false, true)) {
    DRRT_LOG_INFO("successfully set in replace sys");
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Server is replacing sys now, concurrent replace sys is");
    LOG_WARN("server is replacing sys", KR(ret));
  }
  return ret;
}

int ObDRReplaceTenant::set_finish_replace_sys_()
{
  int ret = OB_SUCCESS;
  if (GCTX.atomic_set_replace_sys(true, false)) {
    DRRT_LOG_INFO("successfully set in replace sys false");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set replace sys finish", KR(ret));
  }
  return ret;
}

int ObDRReplaceTenant::check_and_init_for_replace_tenant_()
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version; // not used
  common::ObMemberList member_list; // not used
  DRRT_LOG_INFO("start check and init for replace tenant");
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_INIT_BEGIN, DBA_STEP_INC_INFO(replace_sys),
    "[DR_REPLACE_TENANT] replace start do check and init");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(init_and_check_logservice_access_point_())) {
  // 1. init and check logservice access point
    LOG_WARN("set logservice_access_point to LibPalfEnvFFIInstance failed", KR(ret));
  } else if (OB_FAIL(check_new_zone_())) {
  // 2. check new zone
    LOG_WARN("fail to check new zone", KR(ret));
  } else if (OB_FAIL(load_and_check_data_version_())) {
  // 3. load data version and check sys tenant data match version 4.4.1.0
    LOG_WARN("fail to load data version", KR(ret));
  } else if (OB_FAIL(share::ObMasterKeyUtil::init_root_key_for_replace_tenant())) {
  // 4. load root key from palf kv
    LOG_WARN("fail to get sys root key from palf kv", KR(ret));
  } else if (OB_FAIL(check_can_replace_sys_ls_(SSLOG_LS, config_version, member_list))) {
  // 5. check sys tenant ls alive
    LOG_WARN("fail to check can replace sys ls", KR(ret));
  } else if (OB_FAIL(check_and_init_ss_info_())) {
  // 6. check and init ss info
    LOG_WARN("fail to check cluster region", KR(ret));
  } else if (OB_FAIL(check_ss_cluster_info_())) {
  // 7. check ss cluster info
    LOG_WARN("fail to check logservice cluster id", KR(ret));
  } else if (OB_FAIL(check_cluster_id_())) {
  // 8. check same cluster_id
    LOG_WARN("fail to get cluster id", KR(ret));
  } else if (OB_FAIL(share::ObMasterKeyGetter::instance().init_master_key_for_replace_tenant())) {
  // 9. get master key from palf kv
    LOG_WARN("fail to get master key", KR(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_INIT_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys success to do check and init");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_INIT_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys fail to do check and init");
  }
  DRRT_LOG_INFO("finish check and init for replace tenant", KR(ret));
  return ret;
}

int ObDRReplaceTenant::basic_check_for_replace_tenant_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool server_empty = false;
  ObZone current_zone;
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_BASIC_CHECK_BEGIN, DBA_STEP_INC_INFO(replace_sys),
                  "[DR_REPLACE_TENANT] replace start do basic check");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service_ is nullptr",  KR(ret), KP(GCTX.ob_service_));
  } else if (stmt_.get_tenant_name().string().case_compare(OB_SYS_TENANT_NAME) != 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Replace non-sys tenant is");
    LOG_WARN("only support replace sys tenant", KR(ret));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Current start up mode is not ss, replace tenant is");
    LOG_WARN("not in ss mode", KR(ret));
  } else if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Current logservice is not enabled, replace tenant is");
    LOG_WARN("logservice is not enabled", KR(ret));
  } else if (OB_FAIL(GCTX.ob_service_->check_server_empty(server_empty))) {
    LOG_WARN("fail to check server empty", KR(ret));
  } else if (!server_empty) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Server is not empty, replace tenant is");
    LOG_WARN("server is not empty, replace tenant is not allow", KR(ret));
  } else if (GCONF.self_addr_ != stmt_.get_server()) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    char input_addr[OB_MAX_SERVER_ADDR_SIZE] = {0};
    char current_addr[OB_MAX_SERVER_ADDR_SIZE] = {0};
    if (OB_TMP_FAIL(stmt_.get_server().ip_port_to_string(input_addr, OB_MAX_SERVER_ADDR_SIZE))) {
      LOG_WARN("fail to get server addr string", KR(ret), K(stmt_.get_server()));
    } else if (OB_TMP_FAIL(GCONF.self_addr_.ip_port_to_string(current_addr, OB_MAX_SERVER_ADDR_SIZE))) {
      LOG_WARN("fail to get server addr string", KR(ret), K(GCONF.self_addr_));
    }
    snprintf(err_msg, sizeof(err_msg),
      "Input server(%s) is not same as current execute server(%s) which is", input_addr, current_addr);
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("input server is not same as current execute server", KR(ret), K(GCONF.self_addr_), K(stmt_.get_server()));
  } else if (OB_FAIL(current_zone.assign(GCONF.zone.str()))) {
    LOG_WARN("fail to assign zone", KR(ret), K(GCONF.zone.str()));
  } else if (current_zone != stmt_.get_zone()) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    snprintf(err_msg, sizeof(err_msg),
    "Input zone(%s) is not same as current execute zone(%s) which is", stmt_.get_zone().ptr(), current_zone.ptr());
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("input server is not same as current execute server", KR(ret), K(current_zone), K(stmt_.get_zone()));
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_BASIC_CHECK_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys success to do basic check");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_BASIC_CHECK_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys fail to do basic check");
  }
  return ret;
}

int ObDRReplaceTenant::init_and_check_logservice_access_point_()
{
  int ret = OB_SUCCESS;
  const libpalf::LibPalfEnvFFI *ffi = nullptr;
  if (OB_FAIL(libpalf::LibPalfEnvFFIInstance::get_instance().set_logservice_access_point(
                      stmt_.get_logservice_access_point().ptr()))) {
    LOG_WARN("set logservice_access_point to LibPalfEnvFFIInstance failed", KR(ret));
  } else if (OB_FAIL(libpalf::LibPalfEnvFFIInstance::get_instance().get_ffi(ffi))) {
    // only check object storage path valid
    LOG_WARN("fail to get ffi", KR(ret));
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "logservice access point which is not accessible");
    LOG_WARN("invalid logservice access point", KR(ret));
  }
  DRRT_LOG_INFO("check logservice cluster info over", KR(ret));
  return ret;
}

int ObDRReplaceTenant::check_and_init_ss_info_()
{
  int ret = OB_SUCCESS;
  ObSSFormat ss_format;
  share::ObDeviceConnectivityCheckManager device_conn_check_mgr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(stmt_.get_shared_storage_info().string(), shared_storage_info_, storage_dest_))) {
    LOG_WARN("failed to parse shared_storage_info", KR(ret));
  } else if (OB_UNLIKELY(!shared_storage_info_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared_storage_info is invalid", KR(ret));
  } else if (OB_FAIL(device_conn_check_mgr.check_device_connectivity(storage_dest_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ss info which unable to connect");
    LOG_WARN("fail to check device connectivity", KR(ret), K_(storage_dest));
  } else if (OB_FAIL(ObSSFormatUtil::read_ss_format(storage_dest_, ss_format))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      // input object storage path not exist, maybe the object storage path is not correct
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "object storage path which not exist");
      LOG_WARN("ss path not exist", KR(ret), K_(storage_dest), K(ss_format));
    } else {
      LOG_WARN("fail to read ss_format file", KR(ret), K_(storage_dest), K(ss_format));
    }
  } else if (OB_FAIL(share::ObDeviceConfigMgr::get_instance().set_storage_dest(shared_storage_info_.use_for_, storage_dest_))) {
    LOG_WARN("fail to set storage dest", KR(ret), K_(storage_dest));
  }
  return ret;
}

int ObDRReplaceTenant::check_ss_cluster_info_()
{
  int ret = OB_SUCCESS;
  uint64_t logservice_cluster_id = OB_INVALID_ID;
  ObSSClusterInfo ss_cluster_info;
  // 1. check logservice cluster id.
  if (OB_FAIL(get_logservice_cluster_id(logservice_cluster_id))) {
    LOG_WARN("fail to get logservice cluster id", KR(ret));
  } else if (OB_FAIL(ObSSClusterInfoUtil::read_ss_cluster_info(storage_dest_, ss_cluster_info))) {
    LOG_WARN("fail to read ss cluster info", KR(ret), K(storage_dest_));
  } else if (logservice_cluster_id != ss_cluster_info.get_body().logservice_cluster_id_) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    snprintf(err_msg, sizeof(err_msg), "Input logservice cluster(%lu) is not match with ss_info's logservice cluster(%lu) which is",
              logservice_cluster_id, ss_cluster_info.get_body().logservice_cluster_id_);
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("logservice cluster id not match", KR(ret), K(logservice_cluster_id), K(ss_cluster_info.get_body().logservice_cluster_id_));
  }
  // 2. check region.
  if (OB_FAIL(ret)) {
  } else if (ss_cluster_info.get_body().cluster_region_ != stmt_.get_region()) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    snprintf(err_msg, sizeof(err_msg), "Input region(%s) is not same as original cluster region(%s) which is",
                                      stmt_.get_region().ptr(), ss_cluster_info.get_body().cluster_region_.ptr());
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("cluster region is not same as input cluster region", KR(ret),
              K(ss_cluster_info.get_body().cluster_region_), K(stmt_.get_region()));
  }
  DRRT_LOG_INFO("check ss cluster info over", KR(ret), K(ss_cluster_info), K(logservice_cluster_id));
  return ret;
}

int ObDRReplaceTenant::check_new_zone_()
{
  int ret = OB_SUCCESS;
  bool new_zone = true;
  if (OB_FAIL(ObServerZoneOpService::check_new_zone_in_palf_kv(stmt_.get_zone(), new_zone))) {
    LOG_WARN("fail to check new zone", KR(ret), K(stmt_.get_zone()));
  } else if (!new_zone) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    snprintf(err_msg, sizeof(err_msg),
      "Zone already exists in original cluster(%s), replace tenant in old zone is", stmt_.get_zone().ptr());
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    DRRT_LOG_WARN("zone has exist", KR(ret), "zone", stmt_.get_zone());
  }
  return ret;
}

int ObDRReplaceTenant::check_cluster_id_()
{
  int ret = OB_SUCCESS;
  const int64_t new_cluster_id = GCONF.cluster_id;
  uint64_t orgi_cluster_id = OB_INVALID_ID;
  char *object_storage_root_dir = nullptr;
  ObBackupIoAdapter adapter;
  ObSharedClusterDirListOp shared_cluster_id_op;
  ObArray<uint64_t> cluster_ids;
  if (OB_UNLIKELY(new_cluster_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_cluster_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_ISNULL(object_storage_root_dir)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object storage root dir is nullptr", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(adapter.list_directories(object_storage_root_dir, storage_dest_.get_storage_info(), shared_cluster_id_op))) {
    // get cluster_id by list in the root directory
    LOG_WARN("fail to list files", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(shared_cluster_id_op.get_cluster_ids(cluster_ids))) {
    LOG_WARN("fail to get file list", KR(ret));
  } else if (1 != cluster_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster ids count is not 1", KR(ret), K(cluster_ids));
  } else {
    orgi_cluster_id = cluster_ids.at(0);
  }
  if (OB_FAIL(ret)) {
  } else if (orgi_cluster_id != new_cluster_id) {
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    snprintf(err_msg, sizeof(err_msg), "Cluster id(%lu) is not same as original cluster id(%lu) which is", new_cluster_id, orgi_cluster_id);
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("orgi_cluster_id is not same as new_cluster_id", KR(ret), K(new_cluster_id), K(orgi_cluster_id));
  }
  DRRT_LOG_INFO("check cluster id", KR(ret), K(cluster_ids), K(orgi_cluster_id), K(new_cluster_id), K(object_storage_root_dir));
  return ret;
}

int ObDRReplaceTenant::load_and_check_data_version_()
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start load data version");
  uint64_t sys_data_version = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(ObServerZoneOpService::get_data_version_in_palf_kv(OB_SYS_TENANT_ID, sys_data_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Current data version less than 4.4.1.0, replace tenant is");
      DRRT_LOG_WARN("no data version in palf kv", KR(ret), KDV(sys_data_version));
    } else {
      LOG_WARN("fail to get sys data version in palf kv", KR(ret));
    }
  } else if (sys_data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Current data version less than 4.4.1.0, replace tenant is");
    DRRT_LOG_WARN("data version not match", KR(ret), KDV(sys_data_version));
  } else if (GET_MIN_CLUSTER_VERSION() != sys_data_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Current cluster version less than sys tenant data version, replace tenant is");
    DRRT_LOG_WARN("cluster version and data version not match", KR(ret), KDV(GET_MIN_CLUSTER_VERSION()), KDV(sys_data_version));
  } else if (OB_FAIL(ODV_MGR.set(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("fail to update data_version", KR(ret), "tenant_id", OB_SYS_TENANT_ID, KDV(sys_data_version));
  }
  DRRT_LOG_INFO("finish load data version", KR(ret), KDV(sys_data_version));
  return ret;
}

int ObDRReplaceTenant::get_and_init_server_id_()
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start init server id");
  if (OB_FAIL(ObServerZoneOpService::generate_new_server_id_from_palf_kv(new_server_id_))) {
    LOG_WARN("fail to get server zone op service", KR(ret));
  } else {
    (void) GCTX.set_server_id(new_server_id_);
    GCONF.observer_id = new_server_id_;
  }
  DRRT_LOG_INFO("finish init server id", KR(ret), K_(new_server_id));
  return ret;
}

int ObDRReplaceTenant::create_unit_()
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start create unit");
  const bool is_hidden_sys = false;
  obrpc::ObRootKeyResult root_key_result;
  obrpc::TenantServerUnitConfig tenant_unit_server_config;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(unit_config_.gen_sys_tenant_unit_config(is_hidden_sys))) {
    // attention: here unit_config_id is 1, unit_config_name is "sys_pool",
    // later correct inner table use different unit_config_id and different unit_config_name
    // and OmtNodeBalancer will correct local unit information to be the same as the inner table.
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else if (OB_FAIL(ObServerZoneOpService::generate_new_unit_id_from_palf_kv(new_unit_id_))) {
    LOG_WARN("fail to gen unit id", KR(ret));
  } else if (OB_FAIL(share::ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID,
                                                                       root_key_result.key_type_,
                                                                       root_key_result.root_key_))) {
    LOG_WARN("failed to get sys tenant root key", KR(ret));
  } else if (OB_FAIL(tenant_unit_server_config.init(
                                OB_SYS_TENANT_ID,
                                new_unit_id_,
                                lib::Worker::CompatMode::MYSQL,
                                unit_config_,
                                ObReplicaType::REPLICA_TYPE_FULL,
                                false/*if not grant*/,
                                false/*is_delete*/,
                                root_key_result))) {
    LOG_WARN("fail to init tenant unit server config", KR(ret));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().notify_create_tenant(tenant_unit_server_config))) {
    LOG_WARN("fail to handle_notify_unit_resource", KR(ret), K(tenant_unit_server_config));
  }
  DRRT_LOG_INFO("finish create unit", KR(ret), K(tenant_unit_server_config));
  return ret;
}

int ObDRReplaceTenant::replace_sys_tenant_()
{
  int ret = OB_SUCCESS;
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_REPLACE_SSLOG_LS_BEGIN, DBA_STEP_INC_INFO(replace_sys),
    "[DR_REPLACE_TENANT] start to replace sslog LS");
  DRRT_LOG_INFO("start replace sys tenant");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(replace_sys_tenant_ls_(share::SSLOG_LS))) {
    LOG_WARN("fail to replace sys tenant ls", KR(ret));
  } else if (OB_FAIL(wait_ls_has_leader_(share::SSLOG_LS))) {
    LOG_WARN("fail to wait sslog ls has leader", KR(ret));
  } else if (OB_FAIL(wait_sslog_ls_ready_())) {
    LOG_WARN("fail to wait sslog ls ready", KR(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_REPLACE_SSLOG_LS_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] success to replace sslog LS");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_REPLACE_SSLOG_LS_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] failed to replace sslog LS");
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_REPLACE_SYS_LS_BEGIN, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] start to replace sys LS");
    if (OB_FAIL(replace_sys_tenant_ls_(share::SYS_LS))) {
      LOG_WARN("fail to replace sys tenant sys ls", KR(ret));
    } else if (OB_FAIL(wait_ls_has_leader_(share::SYS_LS))) {
      LOG_WARN("fail to wait sslog ls has leader", KR(ret));
    }
    if (OB_SUCC(ret)) {
      LOG_DBA_INFO_V2(OB_REPLACE_SYS_REPLACE_SYS_LS_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
        "[DR_REPLACE_TENANT] success to replace sys LS");
    } else {
      LOG_DBA_ERROR_V2(OB_REPLACE_SYS_REPLACE_SYS_LS_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
        "[DR_REPLACE_TENANT] failed to replace sys LS");
    }
  }
  DRRT_LOG_INFO("finish replace sys tenant", KR(ret));
  return ret;
}

int ObDRReplaceTenant::replace_sys_tenant_ls_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start replace sys tenant ls", K(ls_id), "addr", GCONF.self_addr_);
  obrpc::ObLSReplaceReplicaArg replace_arg;
  palf::LogConfigVersion config_version;
  common::ObMemberList member_list;
  share::ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  ObReplicaMember dst_member(GCONF.self_addr_, ObTimeUtility::current_time(), REPLICA_TYPE_FULL);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_sys_ls() && !ls_id.is_sslog_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(check_can_replace_sys_ls_(ls_id, config_version, member_list))) {
    LOG_WARN("fail to check can replace sys ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(replace_arg.init(task_id, OB_SYS_TENANT_ID, ls_id, dst_member, config_version))) {
    LOG_WARN("fail to init replace_arg", KR(ret), K(ls_id), K(dst_member), K(config_version));
  } else if (OB_FAIL(observer::ObService::do_replace_ls_replica(replace_arg))) {
    LOG_WARN("fail to do replace ls replica", KR(ret), K(replace_arg));
  }
  DRRT_LOG_INFO("finish replace sys tenant ls", KR(ret), K(ls_id), K(replace_arg), K(GCONF.self_addr_));
  return ret;
}

int ObDRReplaceTenant::wait_ls_has_leader_(const share::ObLSID &ls_id)
{
  DRRT_LOG_INFO("start wait ls has leader", K(ls_id));
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(OB_SYS_TENANT_ID))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location_service_ nullptr", KR(ret), KP(GCTX.location_service_));
  } else {
    while (OB_SUCC(ret)) {
      ObAddr leader_addr;
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait ls leader timeout", KR(ret), K(ls_id), K(ctx_));
      } else if (OB_TMP_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, OB_SYS_TENANT_ID, ls_id, true/*force_renew*/, leader_addr))) {
        // ret may be OB_LS_LOCATION_LEADER_NOT_EXIST.
        LOG_WARN("failed to get ls leader", KR(tmp_ret), K(ls_id));
      } else if (leader_addr.is_valid() && leader_addr == GCONF.self_addr_) {
        DRRT_LOG_INFO("ls has leader", K(ls_id), K(leader_addr));
        break;
      }
      if (OB_SUCC(ret)) {
        ob_usleep(RETRY_INTERVAL_MS);
      }
      DRRT_LOG_INFO("check ls has leader one round", KR(ret), K(ls_id), K(leader_addr));
    } // end while
  } // end else
  DRRT_LOG_INFO("finish wait ls has leader", KR(ret), K(ls_id));
  return ret;
}

int ObDRReplaceTenant::wait_sslog_ls_ready_()
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start wait sslog ls ready");
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    ObLSService *ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (ctx_.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("replace sys ls timeout", KR(ret), K_(ctx));
        } else if (OB_FAIL(ls_service->check_sslog_ls_exist())) {
          if (OB_SSLOG_LS_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            DRRT_LOG_INFO("sslog ls not ready, wait to retry");
          } else {
            LOG_WARN("failed to check sslog ls", KR(ret));
          }
        } else {
          DRRT_LOG_INFO("replace sys ls success");
          break;
        }
        if (OB_SUCC(ret)) {
          ob_usleep(RETRY_INTERVAL_MS);
          DRRT_LOG_INFO("wait to replace sys ls");
        }
      } // end while
    } // end else
  }
  DRRT_LOG_INFO("finish wait sslog ls ready", KR(ret));
  return ret;
}

int ObDRReplaceTenant::wait_sys_tenant_ready_()
{
  int ret = OB_SUCCESS;
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_WAIT_SYS_TENANT_READY_BEGIN, DBA_STEP_INC_INFO(replace_sys),
    "[DR_REPLACE_TENANT] replace start wait sys tenant ready");
  DRRT_LOG_INFO("start wait sys tenant ready");
  bool sys_ready = false;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret), KP(GCTX.schema_service_), KP(GCTX.root_service_));
  } else {
    while (OB_SUCC(ret) && !sys_ready) {
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait sys tenant ready timeout", KR(ret), K(ctx_));
      } else {
        sys_ready = GCTX.schema_service_->is_sys_full_schema() && GCTX.root_service_->is_full_service();
        if (!sys_ready) {
          ob_usleep(RETRY_INTERVAL_MS);
        }
      }
      DRRT_LOG_INFO("wait sys tenant ready one round", KR(ret), K(sys_ready));
    } // end while
  } // end else
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_WAIT_SYS_TENANT_READY_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace success to wait sys tenant ready");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_WAIT_SYS_TENANT_READY_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace fail to wait sys tenant ready");
  }
  DRRT_LOG_INFO("finish wait sys tenant ready", KR(ret), K(sys_ready));
  return ret;
}

int ObDRReplaceTenant::check_can_replace_sys_ls_(
    const share::ObLSID &ls_id,
    palf::LogConfigVersion &config_version,
    common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  GlobalLearnerList learner_list; // not used
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(OB_SYS_TENANT_ID))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(DisasterRecoveryUtils::get_member_info_from_log_service(OB_SYS_TENANT_ID,
                                                                             ls_id,
                                                                             config_version,
                                                                             member_list,
                                                                             learner_list))) {
    LOG_WARN("failed to get member info", KR(ret), K(ls_id));
  } else if (OB_FAIL(check_member_list_can_replace_(member_list, ls_id))) {
    LOG_WARN("fail to check member list can replace", KR(ret), K(member_list), K(ls_id));
  }
  return ret;
}

int ObDRReplaceTenant::check_member_list_can_replace_(
    const common::ObMemberList &member_list,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  DRRT_LOG_INFO("check all server alive", K(member_list), K(ls_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < member_list.get_member_number(); ++index) {
      common::ObAddr server;
      bool alive = false;
      if (OB_FAIL(member_list.get_server_by_index(index, server))) {
        LOG_WARN("fail to get member", KR(ret), K(index));
      } else if (server == GCONF.self_addr_) {
        // retry in same server this may happen, retry in other server need kill orignal server.
        DRRT_LOG_INFO("has alive member in log service, which is current server", K(server), K(member_list));
      } else if (OB_FAIL(check_server_alive_(server, alive))) {
        DRRT_LOG_WARN("failed to check server in blacklist", KR(ret), K(server));
      } else if (alive) {
        char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
        char current_addr[OB_MAX_SERVER_ADDR_SIZE] = {0};
        if (OB_TMP_FAIL(server.ip_port_to_string(current_addr, OB_MAX_SERVER_ADDR_SIZE))) {
          LOG_WARN("fail to get server addr string", KR(ret), K(server));
        }
        snprintf(err_msg, sizeof(err_msg),
          "Not all member list(%s) of LS (%ld) is offline and the current operation is", current_addr, ls_id.id());
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        DRRT_LOG_WARN("has server in member list alive", KR(ret), K(server), K(member_list));
      } else {
        DRRT_LOG_INFO("server is offline", K(server), K(member_list));
      }
    }
  }
  DRRT_LOG_INFO("finish check all server alive", KR(ret), K(member_list), K(ls_id));
  return ret;
}

int ObDRReplaceTenant::check_server_alive_(
    const common::ObAddr &server,
    bool &alive)
{
  DRRT_LOG_INFO("start check server alive");
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  alive = false;
  obrpc::ObCheckServerAliveArg arg;
  int64_t rpc_timeout = GCONF.rpc_timeout;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ is nullptr", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(arg.init(server))) {
    LOG_WARN("fail to init arg", KR(ret), K(server));
  } else if (OB_TMP_FAIL(GCTX.srv_rpc_proxy_->to(server)
                                             .timeout(rpc_timeout)
                                             .group_id(share::OBCG_HB_SERVICE) // use high priority rpc
                                             .check_server_alive(arg))) {
    DRRT_LOG_WARN("fail to execute rpc", KR(tmp_ret), K(server), K(rpc_timeout));
  } else {
    alive = true;
  }
  DRRT_LOG_INFO("finsh check server alive", KR(ret), K(server), K(alive), K(rpc_timeout));
  return ret;
}

int ObDRReplaceTenant::correct_inner_table_()
{
  LOG_DBA_INFO_V2(OB_REPLACE_SYS_CORRECT_TABLE_BEGIN, DBA_STEP_INC_INFO(replace_sys),
    "[DR_REPLACE_TENANT] replace start to correct table");
  DRRT_LOG_INFO("start correct inner table");
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.root_service_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else if (OB_FAIL(correct_all_zone_(trans))) {
    LOG_WARN("fail to correct all zone", KR(ret));
  } else if (OB_FAIL(correct_zone_info_in_ss_(trans))) {
    LOG_WARN("fail to correct all unit", KR(ret));
  } else if (OB_FAIL(correct_all_server_(trans))) {
    LOG_WARN("fail to correct all server", KR(ret));
  } else if (OB_FAIL(correct_unit_related_table_(trans))) {
    LOG_WARN("fail to correct all unit", KR(ret));
  } else if (OB_FAIL(correct_max_resource_id_(trans))) {
    LOG_WARN("fail to correct max resource id", KR(ret));
  } else if (OB_FAIL(record_history_table_(trans))) {
    LOG_WARN("fail to record histore table", KR(ret));
  }
  int tmp_ret = trans.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", KR(tmp_ret), KR(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  if (OB_SUCC(ret)) {
    if (OB_TMP_FAIL(GCTX.root_service_->load_server_manager())) {
      LOG_WARN("fail to load server manager", KR(tmp_ret));
    } else if (OB_TMP_FAIL(GCTX.root_service_->get_zone_mgr().reload())) {
      LOG_WARN("fail to load server manager", KR(tmp_ret));
    } else if (OB_TMP_FAIL(GCTX.root_service_->submit_reload_unit_manager_task())) {
      LOG_WARN("fail to load server manager", KR(tmp_ret));
    } else if (OB_TMP_FAIL(ObServerZoneOpService::insert_zone_in_palf_kv(stmt_.get_zone()))) {
      LOG_WARN("fail to insert zone into palf kv", KR(tmp_ret), "zone", stmt_.get_zone());
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DBA_INFO_V2(OB_REPLACE_SYS_CORRECT_TABLE_SUCCESS, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys success to correct table");
  } else {
    LOG_DBA_ERROR_V2(OB_REPLACE_SYS_CORRECT_TABLE_FAIL, ret, DBA_STEP_INC_INFO(replace_sys),
      "[DR_REPLACE_TENANT] replace sys fail to correct table");
  }
  DRRT_LOG_INFO("finish correct inner table", KR(ret));
  return ret;
}

int ObDRReplaceTenant::correct_all_zone_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    bool exist = false;
    SMART_VAR(share::ObZoneInfo, zone_in_table) {
      if (OB_FAIL(share::ObZoneTableOperation::get_zone_info(stmt_.get_zone(), trans, zone_in_table))) {
        if (OB_ZONE_INFO_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed get zone, zone not exist", KR(ret), "zone",stmt_.get_zone());
        }
      } else {
        exist = true;
      }
      if (OB_FAIL(ret)) {
      } else if (exist) {
        char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
        snprintf(err_msg, sizeof(err_msg),
          "Zone already exists in original cluster(%s), replace tenant in old zone is", stmt_.get_zone().ptr());
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        DRRT_LOG_WARN("zone has exist", KR(ret), K(zone_in_table), "zone", stmt_.get_zone());
      } else {
        DRRT_LOG_INFO("new zone, insert zone info", "zone", stmt_.get_zone());
        SMART_VAR(share::ObZoneInfo, new_zone_info) {
          new_zone_info.reset();
          const share::ObZoneInfo::StorageType storage_type = share::ObZoneInfo::STORAGE_TYPE_SHARED_STORAGE;
          const ObZoneStatus::Status zone_status = ObZoneStatus::ACTIVE;
          if (OB_FAIL(new_zone_info.init(stmt_.get_zone(), stmt_.get_region(), zone_status, storage_type))) {
            LOG_WARN("zone init failed", K(stmt_.get_zone()), K(stmt_.get_region()), K(zone_status), K(storage_type));
          } else if (OB_FAIL(share::ObZoneTableOperation::insert_zone_info(trans, new_zone_info))) {
            LOG_WARN("insert zone info failed", KR(ret), K(new_zone_info));
          }
        } // end new_zone SMART_VAR
      }
    } // end zone_in_table SMART_VAR
  }
  return ret;
}

int ObDRReplaceTenant::correct_zone_info_in_ss_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  obrpc::ObAdminStorageArg full_args;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is NULL", KR(ret), KP(GCTX.root_service_));
  } else if (OB_UNLIKELY(!shared_storage_info_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_args is invalid", KR(ret), K_(shared_storage_info));
  } else if (OB_FAIL(full_args.assign(shared_storage_info_))) {
    LOG_WARN("failed to assign full_args", KR(ret), K_(shared_storage_info));
  } else {
    full_args.zone_ = stmt_.get_zone();
    full_args.region_ = stmt_.get_region();
    // here lock service epoch
    if (OB_FAIL(GCTX.root_service_->add_storage_in_trans(full_args, trans))) {
      // has checked for new zone in correct_all_zone.
      LOG_WARN("failed to add storage", KR(ret), K(full_args));
    }
  }
  DRRT_LOG_INFO("add zone ss storage", KR(ret));
  return ret;
}

int ObDRReplaceTenant::correct_all_server_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObZone zone;
  share::ObServerInfoInTable new_server_info;
  share::ObServerInfoInTable server_info_in_table;
  const common::ObAddr &server = stmt_.get_server();
  const int64_t sql_port = GCONF.mysql_port;
  const uint64_t server_id = GCONF.observer_id;
  ObArray<uint64_t> server_id_in_cluster;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(zone.assign(GCONF.zone.str()))) {
    LOG_WARN("fail to assign zone", KR(ret), K(GCONF.zone.str()));
  } else if (OB_FAIL(observer::ObService::get_build_version(build_version_))) {
    LOG_WARN("fail to get build version", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::get(trans, server, server_info_in_table))) {
    if (OB_SERVER_NOT_IN_WHITE_LIST == ret) {
      ret = OB_SUCCESS;
      DRRT_LOG_INFO("no this server, need insert", K(server));
    } else {
      LOG_WARN("fail to get server_info in table", KR(ret), K(server));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (server_info_in_table.is_valid()) {
    // server_ip must not same with origina cluster server.
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    char current_addr[OB_MAX_SERVER_ADDR_SIZE] = {0};
    if (OB_TMP_FAIL(server.ip_port_to_string(current_addr, OB_MAX_SERVER_ADDR_SIZE))) {
      LOG_WARN("fail to get server addr string", KR(ret), K(server));
    }
    snprintf(err_msg, sizeof(err_msg),
        "Server already exists in original cluster(%s), replace tenant in old server is", current_addr);
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    LOG_WARN("server has exist", KR(ret), K(server), K(server_info_in_table));
  } else if (OB_FAIL(ObServerTableOperator::get_clusters_server_id(trans, server_id_in_cluster))) {
    LOG_WARN("fail to get servers' id in the cluster", KR(ret));
  } else if (!ObServerZoneOpService::check_server_index(server_id, server_id_in_cluster)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("server index is outdated due to concurrent operations", KR(ret), K(server_id), K(server_id_in_cluster));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Server index is outdated due to concurrent operations, replace sys is");
  } else if (OB_FAIL(new_server_info.init(server,
                                          server_id,
                                          zone,
                                          sql_port,
                                          false, /* with_rootserver */ // auto update later
                                          ObServerStatus::OB_SERVER_ACTIVE,
                                          build_version_,
                                          0, /* stop_time */
                                          0, /* start_service_time */
                                          0 /* last_offline_time */))) {
    LOG_WARN("fail to init server info in table", KR(ret), K(server), K(zone), K(sql_port), K_(build_version));
  } else if (OB_FAIL(ObServerTableOperator::insert(trans, new_server_info))) {
    LOG_WARN("fail to insert server info into __all_server table", KR(ret), K(new_server_info));
  }
  return ret;
}

int ObDRReplaceTenant::correct_unit_related_table_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableOperator unit_operator;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObUnitTableTransaction::lock_service_epoch(trans))) {
    LOG_WARN("fail to lock unit service epoch", KR(ret));
  } else if (OB_FAIL(create_new_unit_config_(trans, unit_operator))) {
    LOG_WARN("fail to create new unit config", KR(ret));
  } else if (OB_FAIL(create_new_resource_pool_(trans, unit_operator))) {
    LOG_WARN("fail to create new resource pool", KR(ret));
  } else if (OB_FAIL(create_new_unit_(trans, unit_operator))) {
    LOG_WARN("fail to create new unit", KR(ret));
  }
  return ret;
}

int ObDRReplaceTenant::create_new_unit_config_(
    common::ObMySQLTransaction &trans,
    share::ObUnitTableOperator &unit_operator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is null", KR(ret), KP(GCTX.root_service_));
  } else if (OB_FAIL(GCTX.root_service_->get_unit_mgr().fetch_new_unit_config_id(new_unit_config_id_))) {
    LOG_WARN("fail to get new unit_config_id", KR(ret));
  } else {
    int64_t row_len = 0;
    char unit_config_buf_name[MAX_UNIT_CONFIG_LENGTH] = {'\0'};
    if (OB_FAIL(databuff_printf(unit_config_buf_name,
                                MAX_UNIT_CONFIG_LENGTH,
                                row_len,
                                "%s_%lu",
                                ObUnitConfig::SYS_UNIT_CONFIG_NAME,
                                new_unit_config_id_))) {
      LOG_WARN("failed to print unit config", KR(ret), K(row_len), K(new_unit_config_id_));
    } else {
      const ObFixedLengthString<MAX_UNIT_CONFIG_LENGTH> unit_config_name_str(unit_config_buf_name);
      if (OB_FAIL(unit_config_.set(new_unit_config_id_, unit_config_name_str))) {
        LOG_WARN("fail to set unit config", KR(ret), K_(new_unit_config_id), K(unit_config_name_str));
      } else if (OB_FAIL(unit_operator.update_unit_config(trans, unit_config_))) {
        LOG_WARN("fail to update unit_config", KR(ret), K_(unit_config));
      }
    }
    DRRT_LOG_INFO("create new unit config", KR(ret), K_(unit_config));
  }
  return ret;
}

int ObDRReplaceTenant::create_new_resource_pool_(
    common::ObMySQLTransaction &trans,
    share::ObUnitTableOperator &unit_operator)
{
  int ret = OB_SUCCESS;
  share::ObResourcePool pool;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is null", KR(ret), KP(GCTX.root_service_));
  } else if (OB_FAIL(GCTX.root_service_->get_unit_mgr().fetch_new_resource_pool_id(new_resource_pool_id_))) {
    LOG_WARN("fail to get new unit_config_id", KR(ret));
  } else {
    int64_t row_len = 0;
    char resource_pool_buf_name[MAX_RESOURCE_POOL_LENGTH] = {'\0'};
    if (OB_FAIL(databuff_printf(resource_pool_buf_name,
                                MAX_RESOURCE_POOL_LENGTH,
                                row_len,
                                "%s_%lu",
                                share::ObResourcePool::SYS_RESOURCE_POOL_NAME, // "sys_pool"
                                new_resource_pool_id_))) {
      LOG_WARN("failed to print resource pool name", KR(ret), K(row_len), K(new_resource_pool_id_));
    } else {
      const ObFixedLengthString<MAX_RESOURCE_POOL_LENGTH> resource_pool_name_str(resource_pool_buf_name);
      common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> zone_list;
      if (OB_FAIL(zone_list.push_back(stmt_.get_zone()))) {
        LOG_WARN("fail to push back", KR(ret), K(stmt_.get_zone()));
      } else if (OB_FAIL(pool.init(new_resource_pool_id_,
                                   resource_pool_name_str,
                                   1/*unit_count*/,
                                   new_unit_config_id_,
                                   zone_list,
                                   OB_SYS_TENANT_ID,
                                   ObReplicaType::REPLICA_TYPE_FULL))) {
        LOG_WARN("fail to init resource pool", KR(ret), K_(new_resource_pool_id), K(resource_pool_name_str), K_(new_unit_config_id));
      } else if (OB_FAIL(unit_operator.update_resource_pool(trans, pool, false/*need_check_conflict_with_clone*/))) {
        LOG_WARN("update_resource_pool failed", KR(ret), K(pool));
      }
    }
  }
  DRRT_LOG_INFO("create new resource pool", KR(ret));
  return ret;
}

int ObDRReplaceTenant::create_new_unit_(
    common::ObMySQLTransaction &trans,
    share::ObUnitTableOperator &unit_operator)
{
  int ret = OB_SUCCESS;
  ObUnit new_unit;
  if (OB_FAIL(new_unit.init(new_unit_id_,
                            new_resource_pool_id_,
                            OB_SYS_UNIT_GROUP_ID, // 1
                            stmt_.get_zone(),
                            stmt_.get_server(),
                            ObAddr(), // migrate_from_server
                            false/*is_manual_migrate*/,
                            ObUnit::UNIT_STATUS_ACTIVE,
                            common::ObReplicaType::REPLICA_TYPE_FULL,
                            OB_INVALID_TIMESTAMP))) {
    LOG_WARN("fail to init unit", KR(ret), K_(new_unit_id), K_(new_resource_pool_id), K(stmt_.get_zone()), K(stmt_.get_server()));
  } else if (OB_FAIL(unit_operator.insert_unit(trans, new_unit))) {
    LOG_WARN("failed to insert unit", KR(ret), K(new_unit));
  }
  DRRT_LOG_INFO("create new unit", KR(ret), K(new_unit));
  return ret;
}

int ObDRReplaceTenant::correct_max_resource_id_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    share::ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    uint64_t fetched_max_server_id = OB_INVALID_ID;
    uint64_t fetched_max_unit_id = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_max_id(trans, OB_SYS_TENANT_ID, OB_MAX_USED_SERVER_ID_TYPE, fetched_max_server_id))) {
      // select for update
      LOG_WARN("failed to get max id", KR(ret));
    } else if (fetched_max_server_id < new_server_id_) {
      if (OB_FAIL(id_fetcher.update_max_id(trans, OB_SYS_TENANT_ID, OB_MAX_USED_SERVER_ID_TYPE, new_server_id_))) {
        LOG_WARN("failed to update max id", KR(ret), K_(new_server_id));
      }
    }
    if (FAILEDx(id_fetcher.fetch_max_id(trans, OB_SYS_TENANT_ID, OB_MAX_USED_UNIT_ID_TYPE, fetched_max_unit_id))) {
      // select for update
      LOG_WARN("failed to get max id", KR(ret));
    } else if (fetched_max_unit_id < new_unit_id_) {
      if (OB_FAIL(id_fetcher.update_max_id(trans, OB_SYS_TENANT_ID, OB_MAX_USED_UNIT_ID_TYPE, new_unit_id_))) {
        LOG_WARN("failed to update max id", KR(ret), K_(new_unit_id));
      }
    }
  }
  return ret;
}

int ObDRReplaceTenant::record_history_table_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  DRRT_LOG_INFO("start record history table");
  char ori_min_server_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
  uint64_t ori_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(ori_min_server_version, OB_SERVER_VERSION_LENGTH, ori_cluster_version)) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("fail to print version str", KR(ret), K(ori_cluster_version));
  } else {
    CLUSTER_EVENT_SYNC_ADD_WITH_PROXY(
      trans,
      "REPLACE_SYS", // module
      "REPLACE_SYS_SUCCESS", // event
      "server_id", new_server_id_,
      "unit_id", new_unit_id_,
      "unit_config_id", new_unit_config_id_,
      "resource_pool_id", new_resource_pool_id_,
      "cluster_version", ori_min_server_version,
      "build_version", build_version_.ptr());
  }
  DRRT_LOG_INFO("finish record history table", KR(ret));
  return ret;
}

int ObDRReplaceTenant::clean_up_on_failure_()
{
  DRRT_LOG_INFO("start cleanup on failure");
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    if (OB_INVALID_ID != new_server_id_) {
      DRRT_LOG_INFO("cleanup server_id in palf kv");
      if (OB_FAIL(ObServerZoneOpService::delete_server_id_in_palf_kv(new_server_id_))) {
        LOG_WARN("fail to delete server id in palf kv", KR(ret), K_(new_server_id));
      }
    }
  }
  DRRT_LOG_INFO("finish cleanup on failure", KR(ret), K_(new_server_id));
  return ret;
}

int ObDRReplaceTenant::get_logservice_cluster_id(uint64_t &logservice_cluster_id)
{
  // only check object storage path valid, and get cluster id from object storage path.
  // if the log service process hangs, but the ss path is valid, it will return OB_SUCCESS
  // and the corresponding logservice cluster_id.
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  logservice_cluster_id = OB_INVALID_ID;
  char cluster_version[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char lm_rpc_addr[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char lm_http_addr[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  ObTimeoutCtx ctx;
  const libpalf::LibPalfEnvFFI *ffi = nullptr;
  const int64_t default_timeout = 15L * 1000L * 1000L; // 15s
  if (OB_FAIL(ctx.set_timeout(default_timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(libpalf::LibPalfEnvFFIInstance::get_instance().get_ffi(ffi))) {
    LOG_WARN("fail to get ffi", KR(ret));
  } else if (OB_ISNULL(ffi)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ffi is nullptr", KR(ret));
  } else {
    bool check_success = false;
    while (OB_SUCC(ret) && !check_success) {
      MEMSET(cluster_version, 0, sizeof(cluster_version)); // reset buffer
      MEMSET(lm_rpc_addr, 0, sizeof(lm_rpc_addr));
      MEMSET(lm_http_addr, 0, sizeof(lm_http_addr));
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("check log service cluster info timeout", KR(ret), K(ctx));
      } else if (OB_TMP_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_get_lm_info(ffi,
                                                                             common::OB_IP_PORT_STR_BUFF,
                                                                             &logservice_cluster_id,
                                                                             cluster_version,
                                                                             lm_rpc_addr,
                                                                             lm_http_addr)))) {
        ob_usleep(RETRY_INTERVAL_MS); // 100ms
        LOG_WARN("fail to get logservice cluster info", KR(tmp_ret));
      } else {
        check_success = true;
      }
      DRRT_LOG_INFO("check logservice one round", KR(ret), KR(tmp_ret), "timeout", ctx.get_timeout());
    } // end while
    if (OB_FAIL(ret)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Invalid logservice access point, which is");
      LOG_ERROR("invalid logservice access point", KR(ret));
    } else if (OB_INVALID_ID == logservice_cluster_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("logservice cluster id is invalid", KR(ret));
    }
    DRRT_LOG_INFO("get logservice cluster id", KR(ret), K(check_success), K(logservice_cluster_id));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase

#endif
#endif
#endif