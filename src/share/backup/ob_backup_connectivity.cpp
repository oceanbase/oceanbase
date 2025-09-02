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

#define USING_LOG_PREFIX SHARE
#include "rootserver/ob_root_service.h"
#include "ob_backup_connectivity.h"
#include "lib/restore/ob_object_device.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_zone_table_operation.h"
#include "storage/ob_locality_manager.h"
#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_archive_persist_helper.h"

namespace oceanbase
{
namespace share
{
ObBackupConnectivityCheckManager::ObBackupConnectivityCheckManager()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    rpc_proxy_(NULL),
    sql_proxy_(NULL)
{
}

ObBackupConnectivityCheckManager::~ObBackupConnectivityCheckManager()
{
}

int ObBackupConnectivityCheckManager::init(
    const uint64_t tenant_id,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT; 
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupConnectivityCheckManager::set_last_check_time_(const share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  int64_t last_check_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("connectivity check manager not init", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::update_last_check_time(
      *sql_proxy_, tenant_id_, backup_dest, last_check_time))) {
    LOG_WARN("failed to update last check time", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObBackupConnectivityCheckManager::check_io_permission_(const share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupCheckFile check_file;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("connectivity check manager not init", K(ret));
  } else if (ObStorageType::OB_STORAGE_FILE == backup_dest.get_storage_info()->device_type_) {
    // do nothing
  } else if (OB_FAIL(check_file.init(tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init check file", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_file.delete_permission_check_file(backup_dest))) {
    LOG_WARN("failed to delete permission check file", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_file.check_io_permission(backup_dest))) {
    LOG_WARN("failed to check io permission", K(ret), K_(tenant_id), K(backup_dest));
  } else {
    FLOG_INFO("[BACKUP_DEST_CHECK] succeed to finish oss/cos interface permission check",
      K_(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupConnectivityCheckManager::prepare_connectivity_check_file_(const share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupCheckFile check_file;
  bool is_new_create = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("connectivity check manager not init", K(ret));
  } else if (OB_FAIL(check_file.init(tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init check file", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_file.create_connectivity_check_file(backup_dest, is_new_create))) {
    LOG_WARN("failed to create check file", K(ret), K_(tenant_id));
  } else if (is_new_create) {
    if (OB_FAIL(ObBackupStorageInfoOperator::insert_backup_storage_info(
        *sql_proxy_, tenant_id_, backup_dest, check_file.get_connectivity_file_name()))) {
      LOG_WARN("failed to insert storage info", K(ret), K_(tenant_id), K(backup_dest));
    }
  } 
  return ret;
}

int ObBackupConnectivityCheckManager::set_connectivity_check_path_(
    const share::ObBackupDest &backup_dest,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char check_file_name[OB_MAX_BACKUP_PATH_LENGTH];
  ObBackupCheckFile check_file;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("connectivity check manager not init", K(ret));
  } else if (OB_FAIL(check_file.init(tenant_id_, *sql_proxy_))) {
    LOG_WARN("failed to init check file", K(ret)); 
  } else if (OB_FAIL(check_file.get_check_file_path(backup_dest, path))) {
    LOG_WARN("failed to get check file path", K(ret), K_(tenant_id));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_check_file_name(
      *sql_proxy_, tenant_id_, backup_dest, check_file_name))) {
    LOG_WARN("failed to get check file name", K(ret), K_(tenant_id));
  } else if (OB_FAIL(path.join(check_file_name, ObBackupFileSuffix::NONE))) { // check_file_name already include suffix
    LOG_WARN("failed to join check file name", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObBackupConnectivityCheckManager::check_backup_dest_connectivity(
    const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check manager do not init", KR(ret));
  } else if (!backup_dest.is_valid() || !backup_dest.get_storage_info()->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(prepare_connectivity_check_file_(backup_dest))) {
    LOG_WARN("failed to prepare check file", K(ret), K_(tenant_id), K(backup_dest));
  } else if (OB_FAIL(check_io_permission_(backup_dest))) {
    LOG_WARN("failed to check oss/cos io permission", K(ret), K_(tenant_id), K(backup_dest)); 
  } else if (OB_FAIL(set_connectivity_check_path_(backup_dest, path))) {
    LOG_WARN("failed to get check file", K(ret), K_(tenant_id), K(backup_dest));
  } else if (OB_FAIL(schedule_check_read_write_consistency_(backup_dest))) {
    LOG_WARN("fail to check nfs connectivity", K(ret), K_(tenant_id), K(backup_dest));
  } else if (OB_FAIL(set_last_check_time_(backup_dest))) {
    LOG_WARN("failed to set last check time", K(ret), K_(tenant_id), K(backup_dest));
  } else {
    FLOG_INFO("[BACKUP_DEST_CHECK] succeed to finish backup_dest connectivity check", K_(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupConnectivityCheckManager::schedule_check_read_write_consistency_(const share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupConsistencyCheckFile check_file;
  ObBackupPathString check_file_path;
  common::ObArray<ObAddr> server_list;
  int64_t renew_time = 0;
  bool is_self_tenant_server = false;
  int64_t file_len = 0;
  uint64_t data_checksum = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check manager do not init", KR(ret));
  } else if (!backup_dest.is_valid() || !backup_dest.get_storage_info()->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id));
  } else if (OB_FAIL(ObBackupUtils::get_tenant_backup_servers(backup_dest.get_storage_info()->extension_,
                                                                  tenant_id_, server_list, is_self_tenant_server))) {
    LOG_WARN("fail to get tenant alive servers", K(ret), K_(tenant_id), K(backup_dest));
  } else if (!is_self_tenant_server) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this server is not in tenant's server list", KR(ret), "curr_server", GCONF.self_addr_, K(server_list));
  } else if (OB_FAIL(check_file.init(tenant_id_, backup_dest))) {
    LOG_WARN("fail to init ObBackupCheckFile", K(ret), K_(tenant_id), K(backup_dest));
  } else if (OB_FAIL(check_file.write_check_file(file_len, data_checksum))) {
    LOG_WARN("fail to create nfs connectivity check file", K(ret), K(backup_dest));
  } else if (OB_FAIL(check_file.get_file_dest_str(check_file_path))) {
    LOG_WARN("fail to get check file path", K(ret), K(check_file));
  } else if (OB_FAIL(check_server_rw_consistency_(server_list,
                                                  check_file_path.str(),
                                                  file_len,
                                                  data_checksum))) { // check PUT
    LOG_WARN("[RW CONSISTENCY CHECK] fail to check RW consistency after PUT", K(ret), K(server_list), K(check_file_path));
  } else if (backup_dest.is_enable_worm()) {
    // no need to check overwrite and append
  } else if (OB_FAIL(check_file.overwrite_check_file(file_len, data_checksum))) {
    LOG_WARN("fail to create check file", K(ret), K(check_file));
  } else if (OB_FAIL(check_server_rw_consistency_(server_list,
                                                  check_file_path.str(),
                                                  file_len,
                                                  data_checksum))) { // check OVERWRITE
    LOG_WARN("fail to check RW consistency after OVERWRITE", K(ret), K(server_list), K(check_file_path));
  } else if (!backup_dest.is_storage_type_file()) {
    // normal object is not appendablw
  } else if (OB_FAIL(check_file.append_check_file(file_len, data_checksum))) {
    LOG_WARN("fail to append check file", K(ret), K(check_file));
  } else if (OB_FAIL(check_server_rw_consistency_(server_list,
                                                  check_file_path.str(),
                                                  file_len,
                                                  data_checksum))) { // check APPEND
    LOG_WARN("fail to check RW consistency after APPEND", K(ret), K(server_list), K(check_file_path));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to schedule check RW consistency", K_(tenant_id));
#ifdef ERRSIM
    SERVER_EVENT_ADD("storage_ha", "send backup_device_rw_consistency_check",
                     "tenant_id", tenant_id_,
                     "backup_dest", backup_dest.get_root_path().ptr(),
                     "sender", GCONF.self_addr_);
#endif
  }
  return ret;
}

int ObBackupConnectivityCheckManager::check_server_rw_consistency_(
    const common::ObArray<ObAddr> &server_list,
    const ObString &file_path,
    const int64_t file_len,
    const uint64_t data_checksum)
{
  int ret = OB_SUCCESS;
  obrpc::ObCheckBackupDestRWConsistencyArg arg;
  if (server_list.empty() || file_path.empty() || file_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_list), K(file_path), K(file_len));
  } else if (OB_FAIL(arg.init(tenant_id_, file_path, data_checksum, file_len))) {
  } else {
    rootserver::ObCheckBackupDestRWConsistencyProxy proxy(
    *rpc_proxy_, &obrpc::ObSrvRpcProxy::check_backup_dest_rw_consistency);
    //send
    ARRAY_FOREACH_X(server_list, i, cnt, OB_SUCC(ret)) {
      const ObAddr &dest = server_list.at(i);
      if (OB_FAIL(proxy.call(dest, GCONF.rpc_timeout, arg))) {
        LOG_WARN("fail to send rpc", K(ret), K(dest), K(arg));
      }
    }
    //wait
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("fail to wait all", KR(tmp_ret), KR(ret));
      ret = COVER_SUCC(tmp_ret);
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else {
      //check result
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
        ret = return_code_array.at(i);
        const ObAddr &server = proxy.get_dests().at(i);
        char receiver_addr_str[OB_MAX_SERVER_ADDR_SIZE] = { 0 };
        char sender_addr_str[OB_MAX_SERVER_ADDR_SIZE] = { 0 };
        if (OB_TMP_FAIL(server.ip_port_to_string(receiver_addr_str, sizeof(receiver_addr_str)))) {
          LOG_WARN("fail to convert ip_port to string", K(tmp_ret), K(ret), K(server));
        } else if (OB_TMP_FAIL(GCONF.self_addr_.ip_port_to_string(sender_addr_str, sizeof(sender_addr_str)))) {
          LOG_WARN("fail to convert ip_port to string", K(tmp_ret), K(ret));
        }
        if (OB_BACKUP_DEVICE_NOT_MOUNTED == ret) {
          LOG_USER_ERROR(OB_BACKUP_DEVICE_NOT_MOUNTED, receiver_addr_str, sender_addr_str);
          LOG_WARN("backup device may be not mounted", KR(ret), K(server));
        } else if (OB_BACKUP_DEVICE_NOT_STRONG_RW_CONSISTENT == ret) {
          LOG_USER_ERROR(OB_BACKUP_DEVICE_NOT_STRONG_RW_CONSISTENT, receiver_addr_str, sender_addr_str);
          LOG_WARN("backup device is not read & write strong consistent", KR(ret), K(server));
        } else {
          LOG_WARN("fail to check server read & write consistency", K(ret), K(server), K(arg));
        }
      }
    }
  }
  return ret;
}

//******************************ObBackupCheckFile**********************
ObBackupCheckFile::ObBackupCheckFile()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(NULL)
{
  connectivity_file_name_[0] = '\0';
  permission_file_name_[0] = '\0';
}

ObBackupCheckFile::~ObBackupCheckFile()
{
}

int ObBackupCheckFile::init(
    const uint64_t tenant_id,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT; 
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupCheckFile::get_check_file_path(
    const ObBackupDest &backup_dest,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(path.init(backup_dest.get_root_path()))) {
    LOG_WARN("failed to init path", K(ret));
  } else if (OB_FAIL(path.join(OB_STR_BACKUP_CHECK_FILE, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join check_file", K(ret));
  }
  return ret;
}

int ObBackupCheckFile::set_connectivity_check_name_()
{
  int ret = OB_SUCCESS;
  int64_t check_time_s = ObTimeUtility::current_time() / 1000 / 1000;
  char buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  int64_t pos = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(backup_time_to_strftime(check_time_s, buff, sizeof(buff), pos, 'T'/* concat */))) {
    LOG_WARN("failed to convert time", K(ret));
  } else if (OB_FAIL(databuff_printf(connectivity_file_name_, sizeof(connectivity_file_name_),
      "%lu_%s_%s_%s%s", tenant_id_, "connect", "file", buff, OB_BACKUP_SUFFIX))) {
    LOG_WARN("failed to set connectivity file name", K(ret));
  }
  return ret;
}

int ObBackupCheckFile::create_check_file_dir_(const ObBackupDest &backup_dest, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObBackupIoAdapter util;
  path.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(get_check_file_path(backup_dest, path))) {
    LOG_WARN("failed to get_check_file_path", K(ret), K(backup_dest));
  } else if (OB_FAIL(util.is_exist(path.get_obstr(), backup_dest.get_storage_info(), is_exist))) {
    LOG_WARN("failed to check is exist", K(ret), K(path), K(backup_dest));
  } else if (!is_exist) {
    if (OB_FAIL(util.mkdir(path.get_obstr(), backup_dest.get_storage_info()))) {
      LOG_WARN("failed to check is exist", K(ret), K(path), K(backup_dest));
    }
  }
  return ret;
}

int ObBackupCheckFile::compare_check_file_name_(
    const ObBackupDest &backup_dest,
    const ObBackupPath &path,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  ObArray<ObIODirentEntry> d_entrys;
  char check_file_prefix[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };;
  char check_file_name[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };
  ObBackupIoAdapter util;
  is_match = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(databuff_printf(check_file_prefix, sizeof(check_file_prefix), "%lu_%s_%s",
      tenant_id_, "connect", "file"))) {
    LOG_WARN("failed to get check file prefix", K(ret), K_(tenant_id));
  } else {
    ObDirPrefixEntryNameFilter prefix_op(d_entrys);
    if (OB_FAIL(prefix_op.init(check_file_prefix, static_cast<int32_t>(strlen(check_file_prefix))))) {
      LOG_WARN("failed to init dir prefix", K(ret), K(check_file_prefix), K_(tenant_id));
    } else if (OB_FAIL(util.adaptively_list_files(path.get_obstr(), backup_dest.get_storage_info(), prefix_op))) {
      LOG_WARN("failed to list files", K(ret), K_(tenant_id));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_check_file_name(
        *sql_proxy_, tenant_id_, backup_dest, check_file_name))) {
      LOG_WARN("failed to get check file name", K(ret), K_(tenant_id), K(backup_dest));
    } else {
      char del_file_path[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
      ObIODirentEntry tmp_entry; 
      for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
        tmp_entry = d_entrys.at(i);
        if (OB_ISNULL(tmp_entry.name_)) {
          ret = OB_ERR_UNEXPECTED; 
          LOG_WARN("file name is null", K(ret));
        } else if (0 == STRCMP(check_file_name, tmp_entry.name_)) {
          is_match = true;
        } else if (OB_FAIL(databuff_printf(del_file_path, sizeof(del_file_path),
            "%s/%s", path.get_ptr(), tmp_entry.name_))) {
          LOG_WARN("failed to set check file path", K(ret), K(path), K_(tmp_entry.name));
        } else {
          common::ObString uri(del_file_path);
          if (OB_FAIL(util.adaptively_del_file(uri, backup_dest.get_storage_info()))) {
            if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == ret && backup_dest.is_enable_worm()) {
              //if object locked by worm, don't need to return error
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to delete check file", K(ret), K_(tenant_id));
            }
          }

        }
      }
    }
  }
  
  return ret;
}

int ObBackupCheckFile::generate_format_desc_(const share::ObBackupDest &dest, share::ObBackupCheckDesc &format_desc)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTenantSchema *tenant_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
             OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant schema", K(ret), K_(tenant_id));
  } else if (OB_FAIL(format_desc.cluster_name_.assign(GCONF.cluster.str()))) {
    LOG_WARN("failed to assign cluster name", K(ret), K_(tenant_id));
  } else if (OB_FAIL(format_desc.tenant_name_.assign(tenant_schema->get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", K(ret), K_(tenant_id));
  } else if (OB_FAIL(format_desc.path_.assign(dest.get_root_path()))) {
    LOG_WARN("failed to assign path", K(ret), K(dest));
  } else {
    format_desc.tenant_id_ = tenant_id_;
    format_desc.incarnation_ = OB_START_INCARNATION;
    format_desc.cluster_id_ = GCONF.cluster_id;
  }

  return ret;
}

int ObBackupCheckFile::create_connectivity_check_file(
    const ObBackupDest &backup_dest,
    bool &is_new_create)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  bool is_match = false;
  is_new_create = false;
  share::ObBackupCheckDesc check_desc;
  ObBackupStore store;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(create_check_file_dir_(backup_dest, path))) {
    LOG_WARN("failed to get check file path", K(ret), K(backup_dest)); 
  } else if (OB_FAIL(compare_check_file_name_(backup_dest, path, is_match))) {
    LOG_WARN("failed to compare check file name", K(ret), K_(tenant_id));
  } else if (false == is_match) {
    if (OB_FAIL(set_connectivity_check_name_())) {
      LOG_WARN("failed to set check file name", K(ret), K_(tenant_id));
    } else if (OB_FAIL(path.join(connectivity_file_name_, ObBackupFileSuffix::NONE))) { // connectivity_file_name_ already include suffix
      LOG_WARN("failed to join connectivity file name", K(ret), K_(tenant_id));
    } else if (OB_FAIL(generate_format_desc_(backup_dest, check_desc))) {
      LOG_WARN("failed to set buffer", K(ret), K_(tenant_id));
    } else if (OB_FAIL(store.init(backup_dest))) {
      LOG_WARN("failed to set buffer", K(ret), K_(tenant_id));
    } else if (OB_FAIL(store.write_check_file(path.get_ptr(), check_desc))) {
      if (OB_CHECKSUM_TYPE_NOT_SUPPORTED == ret) {
        LOG_USER_ERROR(OB_CHECKSUM_TYPE_NOT_SUPPORTED, backup_dest.get_storage_info()->get_checksum_type_str());
      }
      LOG_WARN("failed to write check file", K(ret), K(path), K(check_desc));
    } else {
      is_new_create = true;
      FLOG_INFO("[BACKUP_DEST_CHECK] succeed to create new check file", K(path), K(is_match));
    }
  }

  return ret;
}

int ObBackupCheckFile::delete_permission_check_file(const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObArray<ObIODirentEntry> d_entrys;
  char check_file_prefix[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };
  char check_file_name[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };
  ObBackupIoAdapter util;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(databuff_printf(check_file_prefix, sizeof(check_file_prefix), "%lu_%s",
      tenant_id_, "permission"))) {
    LOG_WARN("failed to get check file prefix", K(ret));
  } else if (OB_FAIL(get_check_file_path(backup_dest, path))) {
    LOG_WARN("failed to get check file path", K(ret), K(backup_dest));
  } else {
    ObDirPrefixEntryNameFilter prefix_op(d_entrys);
    if (OB_FAIL(prefix_op.init(check_file_prefix, static_cast<int32_t>(strlen(check_file_prefix))))) {
      LOG_WARN("failed to init dir prefix", K(ret), K(check_file_prefix), K_(tenant_id));
    } else if (OB_FAIL(util.adaptively_list_files(path.get_obstr(), backup_dest.get_storage_info(), prefix_op))) {
      LOG_WARN("failed to list files", K(ret), K_(tenant_id));
    } else {
      char del_file_path[OB_MAX_BACKUP_PATH_LENGTH];
      ObIODirentEntry tmp_entry; 
      for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
        tmp_entry = d_entrys.at(i);
        if (OB_ISNULL(tmp_entry.name_)) {
          ret = OB_ERR_UNEXPECTED; 
          LOG_WARN("file name is null", K(ret));
        } else if (OB_FAIL(databuff_printf(del_file_path, sizeof(del_file_path),
            "%s/%s", path.get_ptr(), tmp_entry.name_))) {
          LOG_WARN("failed to set delete file path", K(ret), K(path), K_(tmp_entry.name));
        } else {
          common::ObString uri(del_file_path);
          if (OB_FAIL(util.adaptively_del_file(uri, backup_dest.get_storage_info()))) {
            if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == ret && backup_dest.is_enable_worm()) {
              //if object locked by worm, don't need to return error
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to delete permission check file", K(ret), K_(tenant_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupCheckFile::get_permission_check_file_path_(
    const ObBackupDest &backup_dest,
    const ObStorageAccessType access_type,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  int64_t check_time_s = ObTimeUtility::current_time() / 1000/ 1000;
  char buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  const char *prefix = nullptr;
  int64_t pos = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE <= access_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid access type", K(ret), K(access_type));
  } else if (OB_FALSE_IT(prefix = get_storage_access_type_str(access_type))) {
  } else if (OB_FAIL(get_check_file_path(backup_dest, path))) {
    LOG_WARN("failed to get check file path", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_time_to_strftime(check_time_s, buff, sizeof(buff), pos, 'T'/* concat */))) {
    LOG_WARN("failed to convert time", K(ret), K(backup_dest));
  } else if (OB_FAIL(databuff_printf(permission_file_name_, sizeof(permission_file_name_),
      "%lu_%s_%s_%s_%s%s", tenant_id_, prefix, "permission", "file", buff, OB_BACKUP_SUFFIX))) {
    LOG_WARN("failed to set permission file name", K(ret), K(buff));
  }  else if (OB_FAIL(path.join(permission_file_name_, ObBackupFileSuffix::NONE))) { // permission_file_name_ already include suffix
    LOG_WARN("failed to join permission file name", K(ret), K_(permission_file_name)); 
  }
  return ret;
}

int ObBackupCheckFile::check_appender_permission_(const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t write_size = 0;
  ObIODevice *device_handle = NULL;
  ObIODOpt iod_opt_array[DEFAULT_OPT_ARG_NUM]; 
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;
  iod_opts.opt_cnt_ = 0;
  bool lock_mode = true;
  bool is_data_file = true;
  bool new_file;
  int64_t epoch = -1;
  ObIOFd fd;
  const static int64_t BUF_LENGTH = 64;
  char data[BUF_LENGTH];
  ObBackupPath path;
  // here mtl_id is 0, use sys tenant's memory instead of 500 tenant
  ObObjectStorageTenantGuard object_storage_tenant_guard(
  OB_SYS_TENANT_ID, OB_IO_MANAGER.get_object_storage_io_timeout_ms(OB_SYS_TENANT_ID) * 1000LL);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(get_permission_check_file_path_(backup_dest, ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER, path))) {
      LOG_WARN("failed to get permission check file path", K(ret), K_(tenant_id), K(backup_dest)); 
  } else if (OB_FAIL(util.set_access_type(&iod_opts, true/*is_appender*/, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set access type");
  } else if (OB_FAIL(util.set_append_strategy(&iod_opts, is_data_file, epoch, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set append strategy");
  } else if (OB_FAIL(util.get_and_init_device(device_handle, backup_dest.get_storage_info(), path.get_obstr(),
                                              ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN( "fail to get device", K(ret));
  } else if (OB_FAIL(device_handle->open(path.get_ptr(), -1/* flag */, 0/* mode */, fd, &iod_opts))) { // flag=-1 and mode=0 are invalid, because oss/cos unused flag and mode;
    LOG_WARN("fail to open file", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(databuff_printf(data, sizeof(data), "tenant(%lu) appender writer at %ld", tenant_id_, ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set data", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(device_handle->pwrite(fd, 0, strlen(data), data, write_size))) {
    LOG_WARN("fail to write file", K(ret), K(path.get_ptr()), K(data));
  } else if (!backup_dest.is_enable_worm()
                && OB_FAIL(util.adaptively_del_file(path.get_obstr(), backup_dest.get_storage_info()))) {
    LOG_WARN("failed to del file", K(ret), K(path));
  }

  if (OB_SUCCESS != (tmp_ret = util.close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    LOG_WARN("failed to close file and release device!", K(tmp_ret));
  }

  return ret;
}

int ObBackupCheckFile::check_multipart_upload_permission_(const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  int64_t write_size = 0;
  int64_t offset = 0;
  ObIODevice *device_handle = NULL;
  ObIOFd fd;
  const static int64_t BUF_LENGTH = 64;
  char data[BUF_LENGTH];
  ObBackupPath path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(get_permission_check_file_path_(backup_dest, access_type, path))) {
    LOG_WARN("failed to get permission check file path", K(ret), K_(tenant_id), K(backup_dest));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, backup_dest.get_storage_info(),
             path.get_obstr(), access_type, ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("fail to open device or fd", K(ret), K(backup_dest), K(path));
  } else if (OB_FAIL(databuff_printf(data, sizeof(data), "tenant(%lu) multipart writer at %ld", tenant_id_, ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set data", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(device_handle->pwrite(fd, offset, strlen(data), data, write_size))) {
    LOG_WARN("fail to write file", K(ret), K(path.get_ptr()), K(data));
  } else if (OB_FAIL(device_handle->complete(fd))) {
    STORAGE_LOG(WARN, "fail to complete multipart upload", K(ret), K(device_handle), K(fd));
  } else if (!backup_dest.is_enable_worm()
                && OB_FAIL(util.del_file(path.get_obstr(), backup_dest.get_storage_info()))) {
    LOG_WARN("failed to del file", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(device_handle) && OB_TMP_FAIL(device_handle->abort(fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG(WARN, "fail to abort multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd));
    }
  }
  if (OB_TMP_FAIL(util.close_device_and_fd(device_handle, fd))) {
    ret = COVER_SUCC(tmp_ret);
    STORAGE_LOG(WARN, "fail to close device and fd", K(ret), K(tmp_ret), K(device_handle), K(fd));
  }
  return ret;
}

bool ObBackupCheckFile::is_permission_error_(const int32_t result) 
{ 
  int ret = OB_SUCCESS;
  bool is_permission = false;
  if (OB_IO_ERROR == result
      || OB_OBJECT_STORAGE_IO_ERROR == result) {
    is_permission = true; 
  }
  return is_permission;
}

int ObBackupCheckFile::check_io_permission(const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  char *buf = nullptr;
  int64_t pos = 0;
  int64_t file_len = 0;
  int64_t read_size = 0;
  ObBackupPath path;
  bool write_ok = false;
  share::ObBackupCheckDesc check_desc;
  ObArenaAllocator allocator;
  ObBackupStore store;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is valid", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(get_permission_check_file_path_(backup_dest, ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER, path))) {
      LOG_WARN("failed to get permission check file path", K(ret), K_(tenant_id));
  } else if (OB_FAIL(generate_format_desc_(backup_dest, check_desc))) {
    LOG_WARN("failed to set buffer", K(ret), K_(tenant_id));
  } else if (OB_FAIL(store.init(backup_dest))) {
    LOG_WARN("failed to set buffer", K(ret), K_(tenant_id));
  } else if (OB_FAIL(store.write_check_file(path.get_ptr(), check_desc))) {
    if (is_permission_error_(ret)) {
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check", 
          "tenant_id", tenant_id_, "error_code", ret, "comment", "write single file");
    }
    LOG_WARN("failed to write single file", K(ret), K_(tenant_id), K(backup_dest));
  } else if (FALSE_IT(write_ok = true)
      || OB_FAIL(util.adaptively_get_file_length(path.get_obstr(), backup_dest.get_storage_info(), file_len))) {
    if (is_permission_error_(ret)) { 
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check", 
          "tenant_id", tenant_id_, "error_code", ret, "comment", "get file length");
    }
    LOG_WARN("failed to get file length", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_len));
  } else if (OB_FAIL(util.adaptively_read_single_file(path.get_obstr(), backup_dest.get_storage_info(),
                     buf, file_len, read_size, ObStorageIdMod::get_default_id_mod()))) {
    if (is_permission_error_(ret)) {
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check", 
          "tenant_id", tenant_id_, "error_code", ret, "comment", "read single file");
    }
    LOG_WARN("failed to read single file", K(ret));
  }
  if (write_ok && !backup_dest.is_enable_worm()
          && OB_TMP_FAIL(util.adaptively_del_file(path.get_obstr(), backup_dest.get_storage_info()))) {
    if (is_permission_error_(tmp_ret)) {
      tmp_ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check", 
          "tenant_id", tenant_id_, "error_code", tmp_ret, "comment", "delete file");
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    LOG_WARN("failed to del file", K(tmp_ret), K(ret), K(path), K(backup_dest));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_appender_permission_(backup_dest))){
    if (is_permission_error_(ret)) {
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check",
          "tenant_id", tenant_id_, "error_code", ret, "comment", "appender write");
    }
    LOG_WARN("failed to appender permission", K(ret));
  } else if (OB_FAIL(check_multipart_upload_permission_(backup_dest))) {
    if (is_permission_error_(ret)) {
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check",
          "tenant_id", tenant_id_, "error_code", ret, "comment", "multipart upload write");
    }
    LOG_WARN("failed to check multipart permission", K(ret), K(backup_dest));
  }

  return ret;
}

//***********************ObBackupConsistencyCheckFile*********************

ObBackupConsistencyCheckFile::ObBackupConsistencyCheckFile()
  : is_inited_(false),
    backup_dest_(),
    path_(),
    check_desc_(),
    allocator_()
{
}

ObBackupConsistencyCheckFile::~ObBackupConsistencyCheckFile()
{
}

int ObBackupConsistencyCheckFile::init(const uint64_t tenant_id, const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  const int64_t curr_time_us = ObTimeUtility::current_time(); //used as part of the file name
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)
             || !backup_dest.is_valid()
             || !backup_dest.get_storage_info()->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("fail to deep copy backup dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(check_desc_.init(tenant_id))) {
    LOG_WARN("fail to init check desc", K(ret), K(tenant_id));
  } else if (OB_FAIL(init_file_path_(curr_time_us))) {
    LOG_WARN("fail to get file path", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupConsistencyCheckFile::write_check_file(int64_t &file_len, uint64_t &data_checksum)
{
  int ret = OB_SUCCESS;
  ObBackupStore store;
  ObBackupIoAdapter io_util;
  file_len = 0;
  data_checksum = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("check file not init", K(ret));
  } else if (OB_FAIL(store.init(backup_dest_))) {
    LOG_WARN("fail to init ObBackupStore", K(ret), K(backup_dest_));
  } else if (OB_FAIL(store.write_rw_consistency_check_file(path_.get_obstr(), check_desc_))) {
    LOG_WARN("fail to write check file", K(ret), K_(path), K_(check_desc));
  } else if (OB_FAIL(io_util.get_file_length(path_.get_obstr(), backup_dest_.get_storage_info(), file_len))) {
    LOG_WARN("fail to get file length", K(ret), K_(path));
  } else if (OB_FAIL(get_data_checksum_(data_checksum))) {
    LOG_WARN("fail to get data data checksum", K(ret), KPC(this));
  }
  return ret;
}

int ObBackupConsistencyCheckFile::overwrite_check_file(int64_t &file_len, uint64_t &data_checksum)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = check_desc_.get_tenant_id();
  check_desc_.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("check file not init", K(ret));
  } else if (OB_FAIL(check_desc_.init(tenant_id))) { //random content will re-generate
    LOG_WARN("fail to init check desc", K(ret), K(tenant_id));
  } else if (OB_FAIL(write_check_file(file_len, data_checksum))) {
    LOG_WARN("fail to write check file", K(ret), K_(check_desc));
  }
  return ret;
}

int ObBackupConsistencyCheckFile::append_check_file(int64_t &file_len, uint64_t &data_checksum)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  char *append_data = nullptr;
  int64_t append_data_len = 0;
  file_len = 0;
  data_checksum = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("check file not init",  K(ret));
  } else {
    const int64_t write_offset =  check_desc_.get_serialize_size();
    int64_t write_size = 0;
    if (OB_FAIL(generate_append_data_(append_data, append_data_len))) {
      LOG_WARN("fail to generate append data", K(ret));
    } else if (OB_FAIL(io_util.pwrite(path_.get_obstr(),
                                      backup_dest_.get_storage_info(),
                                      append_data,
                                      write_offset,
                                      append_data_len,
                                      common::ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                      write_size,
                                      false /*is_can_seal*/,
                                      ObStorageIdMod::get_default_backup_id_mod()))) {
      LOG_WARN("fail to append check file", K(ret), K_(path), K_(check_desc));
    } else if (OB_FAIL(io_util.get_file_length(path_.get_obstr(), backup_dest_.get_storage_info(), file_len))) {
      LOG_WARN("fail to get file length", K(ret), K_(path), K_(backup_dest));
    } else if (OB_FAIL(get_data_checksum_(data_checksum))) {
      LOG_WARN("fail to get data checksum", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObBackupConsistencyCheckFile::generate_append_data_(char *&append_data, int64_t &append_len)
{
  int ret = OB_SUCCESS;
  ObBackupConsistencyCheckDesc check_desc;
  if (OB_FAIL(check_desc.init(check_desc_.get_tenant_id()))) {
    LOG_WARN("fail to init check desc", K(ret));
  } else {
    const int64_t ser_len = check_desc.get_serialize_size();
    int64_t ser_pos = 0;
    if (OB_ISNULL(append_data = static_cast<char*>(allocator_.alloc(ser_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem",  K(ret), K(ser_len));
    } else if (OB_FAIL(check_desc.serialize(append_data, ser_len, ser_pos))) {
      LOG_WARN("fail to serialize check desc");
    } else {
      append_len = ser_len;
    }
  }
  return ret;
}

int ObBackupConsistencyCheckFile::get_data_checksum_(uint64_t &data_checksum)
{
  int ret = OB_SUCCESS;
  data_checksum = 0;
  common::ObBackupIoAdapter io_util;
  int64_t file_len = 0;
  char *buf = nullptr;
  int64_t read_size = 0;

  if (OB_FAIL(io_util.get_file_length(path_.get_obstr(),
                                      backup_dest_.get_storage_info(),
                                      file_len))) {
    LOG_WARN("fail to get file len", K(ret), KPC(this));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret), K(file_len));
  } else if (OB_FAIL(io_util.read_single_file(path_.get_obstr(),
                                              backup_dest_.get_storage_info(),
                                              buf,
                                              file_len,
                                              read_size,
                                              ObStorageIdMod::get_default_backup_id_mod()))) {
    LOG_WARN("fail to read single file", K(ret), KPC(this));
  } else if (file_len != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_len and read_size are not equal", K(ret), K(file_len), K(read_size));
  } else {
    data_checksum = common::ob_crc64(buf, file_len);
  }
  return ret;
}

int ObBackupConsistencyCheckFile::init_file_path_(const int64_t curr_time_us)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char file_name[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };
  path_.reset();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_FAIL(path_.init(backup_dest_.get_root_path()))) {
    LOG_WARN("failed to init path", K(ret));
  } else if (OB_FAIL(path_.join(OB_STR_BACKUP_CHECK_FILE, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join check_file", K(ret));
  }else if (OB_FAIL(databuff_printf(file_name,
                                    OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH,
                                    pos,
                                    "%lu_%s_%ld",
                                    check_desc_.get_tenant_id(),
                                    "nfs_connectivity_check",
                                    curr_time_us))) {
    LOG_WARN("fail to databuff printf", K(ret));
  } else if (OB_FAIL(path_.join(file_name, ObBackupFileSuffix::BACKUP) )) {
    LOG_WARN("fail to join check file path", K(ret));
  }
  return ret;
}

int ObBackupConsistencyCheckFile::get_file_dest_str(ObBackupPathString &file_dest_str)
{
  int ret = OB_SUCCESS;
  file_dest_str.reset();
  ObBackupDest tmp_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("check file not init", K(ret));
  } else if (OB_FAIL(tmp_dest.set(path_.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("fail to set tmp dest", K(ret), K_(path), K_(backup_dest));
  } else if (OB_FAIL(tmp_dest.get_backup_dest_str(file_dest_str.ptr(), file_dest_str.capacity()))) {
    LOG_WARN("fail to get backup dest str", K(ret), K(tmp_dest));
  }
  return ret;
}

//*************************ObBackupDestCheck*********************
int ObBackupDestCheck::check_backup_dest_connectivity(
    const uint64_t tenant_id,
    const char *backup_path,
    const ObBackupPath &check_path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupDest backup_dest;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  bool is_exist = false;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, backup_path, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(backup_path), K(tenant_id));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    LOG_INFO("io prohibited, don't check connectivity");
  } else if (OB_FAIL(check_check_file_exist_(backup_dest, check_path, is_exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(check_path), K(backup_dest));
  } else if (!is_exist) {
    ret = OB_BACKUP_DEST_NOT_CONNECT;
    LOG_WARN("check backup check file is not exist", K(ret), K(check_path), K(backup_dest));
  }

  return ret;
}

int ObBackupDestCheck::check_check_file_exist_(
    const ObBackupDest &backup_dest,
    const share::ObBackupPath &path,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t retry_times = 3;
  bool need_retry = true;
  is_exist = false;
  while (retry_times--) {
    if (OB_FAIL(util.adaptively_is_exist(path.get_obstr(), backup_dest.get_storage_info(), is_exist))) {
      LOG_WARN("failed to check is_exist", K(ret), K(path), K(backup_dest), K(retry_times));
      ob_usleep(1 * 1000 * 1000L); // 1s 
      continue;
    }
    break;
  }
  return ret;
}

//*********************ObBackupStorageInfoOperator*****************
int ObBackupStorageInfoOperator::insert_backup_storage_info(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    const char *check_file_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  char *root_path = NULL;
  int64_t dest_id = 0;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = { 0 };
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid() || OB_ISNULL(check_file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_storage_info()->get_authorization_info(authorization, sizeof(authorization)))) {
    LOG_WARN("fail to set authorization", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_AUTHORIZATION, authorization))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_EXTENSION, backup_dest.get_storage_info()->extension_))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_CHECK_FILE_NAME, check_file_name))) {
    LOG_WARN("fail to fill backup dest info", K(ret));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows && 2 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("success insert/update backup storage info", K(sql), K(tenant_id), K(backup_dest));
  }

  return ret;
}

int ObBackupStorageInfoOperator::insert_backup_storage_info(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    const ObBackupDestType::TYPE &dest_type,
    const int64_t dest_id,
    const int64_t max_iops,
    const int64_t max_bandwidth)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  char *root_path = NULL;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = { 0 };
  uint64_t compat_version = 0;
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), compat_version))) {
    LOG_WARN("failed to get min data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in compat, can't read/write inner table", K(ret), K(compat_version));
  } else if (OB_FAIL(backup_dest.get_storage_info()->get_authorization_info(authorization, sizeof(authorization)))) {
    LOG_WARN("fail to set authorization", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_DEST_ID, dest_id))
      || OB_FAIL(dml.add_column(OB_STR_DEST_TYPE, ObBackupDestType::get_str(dest_type))) 
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_AUTHORIZATION, authorization))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_EXTENSION, backup_dest.get_storage_info()->extension_))
      || OB_FAIL(dml.add_column(OB_STR_MAX_IOPS, max_iops))
      || OB_FAIL(dml.add_column(OB_STR_MAX_BANDWIDTH, max_bandwidth))) {
    LOG_WARN("fail to fill backup dest info", K(ret));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (0 != affected_rows && 1 != affected_rows && 2 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("succ insert/update backup storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::remove_backup_storage_info(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu "
      "AND path = '%s' AND endpoint = '%s'", OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, backup_dest.get_root_path().ptr(), backup_dest.get_storage_info()->endpoint_))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else {
    LOG_INFO("succ delete backup storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::remove_backup_storage_info(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDestType::TYPE &backup_dest_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || !ObBackupDestType::is_valid(backup_dest_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest_type));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu "
      "AND dest_type = '%s'", OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, ObBackupDestType::get_str(backup_dest_type)))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret));
  } else {
    LOG_INFO("succ delete backup storage info", K(sql), K(tenant_id), K(affected_rows), K(backup_dest_type));
  }
  return ret;
}
int ObBackupStorageInfoOperator::update_backup_dest_extension(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObBackupDest &backup_dest,
    const char *extension)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(backup_dest), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_EXTENSION, extension))) {
        LOG_WARN("failed to fill on item", K(ret), K(backup_dest));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected_rows", K(ret), K(affected_rows), K(sql), K(extension));
  } else {
    LOG_INFO("succ update backup storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::update_backup_authorization(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = { 0 };
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_storage_info()->get_authorization_info(authorization, sizeof(authorization)))) {
    LOG_WARN("fail to set authorization", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_AUTHORIZATION, authorization))) {
    LOG_WARN("failed to fill on item", K(ret), K(backup_dest));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(backup_dest));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (0 != affected_rows && 1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(sql), K(tenant_id), K(affected_rows));
  } else {
    LOG_INFO("update backup authorization in storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::update_backup_dest_attribute(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObBackupDest &backup_dest,
    const int64_t max_iops,
    const int64_t max_bandwidth)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  uint64_t compat_version = 0;
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(backup_dest));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), compat_version))) {
    LOG_WARN("failed to get min data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in compat, can't read/write inner table", K(ret), K(compat_version));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, gen_user_tenant_id(tenant_id)))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_MAX_IOPS, max_iops))
      || OB_FAIL(dml.add_column(OB_STR_MAX_BANDWIDTH, max_bandwidth))) {
    LOG_WARN("failed to fill on item", K(ret), K(backup_dest));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(backup_dest));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (0 != affected_rows && 1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("update backup attribute in storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::update_last_check_time(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    const int64_t last_check_time)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(backup_dest));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_LAST_CHECK_TIME, last_check_time))) {
    LOG_WARN("failed to add column", K(ret), K(backup_dest));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_STORAGE_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("update backup last check time in storage info", K(sql), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupStorageInfoOperator::get_check_file_name(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    char *check_file_name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id && !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s "
      "WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_BACKUP_CHECK_FILE_NAME, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, backup_dest.get_root_path().ptr(), backup_dest.get_storage_info()->endpoint_))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        int64_t tmp_real_str_len = 0;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_CHECK_FILE_NAME,
          check_file_name, OB_MAX_BACKUP_PATH_LENGTH, tmp_real_str_len);
        UNUSED(tmp_real_str_len);
      } else if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }

  return ret;
}

int ObBackupStorageInfoOperator::get_dest_id(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  dest_id = 0;

  if (OB_INVALID_ID == tenant_id && !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_DEST_ID, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, backup_dest.get_root_path().ptr(), backup_dest.get_storage_info()->endpoint_))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_DEST_ID, dest_id, int64_t);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", K(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupStorageInfoOperator::get_restore_dest_id(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDestType::TYPE &backup_dest_type,
    int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  dest_id = 0;

  if (OB_INVALID_ID == tenant_id || !ObBackupDestType::is_valid(backup_dest_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(backup_dest_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s WHERE tenant_id = %lu AND dest_type = '%s'",
      OB_STR_DEST_ID, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, ObBackupDestType::get_str(backup_dest_type)))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_DEST_ID, dest_id, int64_t);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", K(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupStorageInfoOperator::get_restore_shared_storage_limit(const common::ObTrafficControl::ObStorageKey &storage_key, int64_t &max_iops, int64_t &max_bandwidth)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient &proxy = GCTX.root_service_->get_sql_proxy();
  const uint64_t dest_id = storage_key.get_storage_id();
  const uint64_t tenant_id = storage_key.get_tenant_id();
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s WHERE tenant_id = %lu AND dest_id = '%lu'",
      OB_STR_MAX_IOPS_AND_MAX_BANDWIDTH, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, dest_id))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_MAX_IOPS, max_iops, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_MAX_BANDWIDTH, max_bandwidth, int64_t);
      } else if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", K(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupStorageInfoOperator::get_dest_type(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    ObBackupDestType::TYPE &dest_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char dest_type_str[OB_DEFAULT_STATUS_LENTH] = { 0 };
  if (OB_INVALID_ID == tenant_id && !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_DEST_TYPE, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, backup_dest.get_root_path().ptr(), backup_dest.get_storage_info()->endpoint_))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        int64_t tmp_real_str_len = 0;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_DEST_TYPE,
          dest_type_str, OB_DEFAULT_STATUS_LENTH, tmp_real_str_len);
        UNUSED(tmp_real_str_len);
        dest_type = ObBackupDestType::get_type(dest_type_str);
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupStorageInfoOperator::parse_backup_path(
    const char *backup_path,
    char *path,
    int64_t path_len,
    char *endpoint,
    int64_t endpoint_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  common::ObStorageType type;
  ObString bakup_path_str(backup_path);

  if (OB_ISNULL(backup_path) || OB_ISNULL(path) || OB_ISNULL(endpoint)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(backup_path));
  } else if (OB_FAIL(get_storage_type_from_path(bakup_path_str, type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else {
    while (backup_path[pos] != '\0') {
      if (backup_path[pos] == '?') {
        break;
      }
      ++pos;
    }
    int64_t left_count = strlen(backup_path) - pos;
    if (pos >= path_len || left_count >= endpoint_len) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_ERROR("backup dest is too long, cannot work",
          K(ret), K(pos), K(backup_path), K(left_count));
    } else {
      MEMCPY(path, backup_path, pos);
      path[pos] = '\0';
      ++pos;
      if (0 != left_count) {
        MEMCPY(endpoint, backup_path + pos, left_count);
      }
    }
  }
  return ret;
}

int ObBackupStorageInfoOperator::get_backup_dest_extensions(
    const uint64_t tenant_id,
    const ObIArray<int64_t> &dest_ids,
    common::ObIAllocator &allocator,
    ObIArray<std::pair<int64_t, ObString>> &extensions)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  int64_t dest_id_count = dest_ids.count();
  if (OB_INVALID_ID == tenant_id || dest_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dest_id_count));
  } else if (OB_ISNULL(sql_proxy) || dest_id_count > OB_MAX_BACKUP_DEST_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is NULL or too many dest ids", K(ret), K(dest_id_count));
  } else {
    if (OB_FAIL(sql.assign_fmt("select %s,%s FROM %s WHERE tenant_id = %lu AND %s in (",
                  OB_STR_BACKUP_DEST_ID,
                  OB_STR_BACKUP_DEST_EXTENSION,
                  OB_ALL_BACKUP_STORAGE_INFO_TNAME,
                  tenant_id, OB_STR_BACKUP_DEST_ID))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_id_count; ++i) {
      if (i != 0) {
        if (FAILEDx(sql.append_fmt(", "))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
      if (FAILEDx(sql.append_fmt("%ld", dest_ids.at(i)))) {
        LOG_WARN("fail to append fmt", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append fmt", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else {
        while (OB_SUCC(ret) ) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
          } else {
           int64_t dest_id = OB_INVALID_DEST_ID;
           common::ObString tmp_extension;
           int64_t tmp_real_str_len = 0;
           EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_ID, dest_id, int64_t);
           EXTRACT_VARCHAR_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_EXTENSION, tmp_extension);
           if (OB_SUCC(ret)) {
            common::ObString deep_copy_extension;
            if (OB_FAIL(deep_copy_ob_string(allocator, tmp_extension, deep_copy_extension))) {
              LOG_WARN("fail to deep copy extension", K(ret), K(tmp_extension));
            } else if (OB_FAIL(extensions.push_back(std::make_pair(dest_id, deep_copy_extension)))) {
              LOG_WARN("fail to push back extension", K(ret), K(deep_copy_extension), K(dest_id));
            } else {
              LOG_DEBUG("succeed to push back extension", K(ret), K(deep_copy_extension), K(dest_id));
            }
           }
          }
        }
      }
    }
  }

  return ret;
}

int ObBackupStorageInfoOperator::get_backup_dest_extension(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObBackupDest &backup_dest,
    char *extension,
    const int64_t buffer_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s FROM %s WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_BACKUP_DEST_EXTENSION, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, backup_dest.get_root_path().ptr(), backup_dest.get_storage_info()->endpoint_))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        int64_t tmp_real_str_len = 0;

        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_EXTENSION,
          extension, buffer_len, tmp_real_str_len);
        UNUSED(tmp_real_str_len);
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }

  return ret;
}

int ObBackupStorageInfoOperator::get_backup_dest(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupPathString &backup_path,
    ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *result = NULL;
  backup_dest.reset();
  char path[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = { 0 };
  char encrypt_authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = { 0 };
  char extension[OB_MAX_BACKUP_EXTENSION_LENGTH] = { 0 }; 
  int64_t dest_id = OB_INVALID_DEST_ID;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(parse_backup_path(backup_path.ptr(), path, sizeof(path), endpoint, sizeof(endpoint)))) {
    LOG_WARN("failed to parse backup path", K(ret), K(tenant_id), K(backup_path)); 
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s,%s,%s FROM %s WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_BACKUP_DEST_ID, OB_STR_BACKUP_DEST_AUTHORIZATION,
      OB_STR_BACKUP_DEST_EXTENSION, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
      tenant_id, path, endpoint))) {
    LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(path), K(endpoint));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_SUCC(result->next())) {
        int64_t tmp_real_str_len = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_ID, dest_id, int64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_AUTHORIZATION, encrypt_authorization,
          OB_MAX_BACKUP_AUTHORIZATION_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, OB_STR_BACKUP_DEST_EXTENSION, extension,
          OB_MAX_BACKUP_EXTENSION_LENGTH, tmp_real_str_len);
        UNUSED(tmp_real_str_len);
      } else if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no exist row", K(ret), K(sql));
      } else {
        LOG_WARN("fail to get next row", K(ret), K(sql));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(backup_dest.set(path, endpoint, encrypt_authorization, extension, dest_id))) {
    LOG_WARN("fail to set backup dest", K(ret), K(tenant_id)); 
  } else {
    LOG_INFO("success get backup dest", K(sql), K(tenant_id), K(backup_dest)); 
  }
  return ret;
}

int ObBackupDestIOPermissionMgr::ObRefreshIOPermissionTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObRefreshIOPermissionTask init twice", KR(ret), K(this));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_= true;
  }

  return ret;
}

void ObBackupDestIOPermissionMgr::ObRefreshIOPermissionTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mgr_.refresh_io_permission())) {
    OB_LOG(WARN, "failed to refresh io permission", K(ret));
  }
}

ObBackupDestIOPermissionMgr::ObBackupDestIOPermissionMgr()
  : is_inited_(false),
    lock_(),
    tenant_id_(OB_INVALID_ID),
    dest_io_permission_map_(),
    last_refresh_time_(0),
    zone_(),
    region_(),
    idc_(),
    refresh_io_permission_task_(*this)
{
}

ObBackupDestIOPermissionMgr::~ObBackupDestIOPermissionMgr()
{
  destroy();
}

int ObBackupDestIOPermissionMgr::mtl_init(ObBackupDestIOPermissionMgr* &backup_dest_io_permission_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(backup_dest_io_permission_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "backup_dest_io_permission_mgr is null", K(ret));
  } else if (OB_FAIL(backup_dest_io_permission_mgr->init(MTL_ID()))) {
    OB_LOG(WARN, "failed to init backup dest io permission mgr", K(ret));
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObBackupDestIOPermissionMgr has been inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(dest_io_permission_map_.create(OB_BACKUP_IO_PERMISSION_MAP_BUCKET_NUM,
                                                      lib::ObMemAttr(tenant_id, "IOPermissionMap")))) {
    OB_LOG(WARN, "fail to create dest io permission map", K(ret));
  } else if (OB_FAIL(refresh_io_permission_task_.init(tenant_id))) {
    OB_LOG(WARN, "failed to init refresh io permission task", K(ret));
  } else {
    is_inited_= true;
    tenant_id_ = tenant_id;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::start()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                  refresh_io_permission_task_,
                                  PERMISSION_UPDATE_INTERVAL,
                                  true /* repeat */))){
    OB_LOG(WARN, "failed to schedule BackupDestIOPermissionMgr task", K(ret));
  }

  OB_LOG(INFO, "start BackupDestIOPermissionMgr task", K(ret));
  return ret;
}

void ObBackupDestIOPermissionMgr::stop()
{
  if (OB_LIKELY(refresh_io_permission_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_io_permission_task_);
  }
}

void ObBackupDestIOPermissionMgr::wait()
{
  if (OB_LIKELY(refresh_io_permission_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_io_permission_task_);
  }
}

void ObBackupDestIOPermissionMgr::destroy()
{
  if (refresh_io_permission_task_.is_inited_) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_io_permission_task_);
  }
  dest_io_permission_map_.destroy();
  is_inited_ = false;
}

int ObBackupDestIOPermissionMgr::refresh_and_get_dest_ids_in_map_(ObIArray<int64_t> &dest_ids)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> inactive_dest_ids;
  const int64_t curr_time_us = ObTimeUtility::current_time();
  dest_ids.reset();
  int64_t archive_dest_id = OB_INVALID_DEST_ID;
  int64_t backup_dest_id = OB_INVALID_DEST_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDestIOPermissionMgr not init", K(ret));
  } else if (OB_FAIL(get_backup_and_archive_path_dest_id_(backup_dest_id, archive_dest_id))) {
    LOG_WARN("failed to get backup and archive path dest id", K(ret));
  } else if (OB_INVALID_DEST_ID != archive_dest_id && OB_FAIL(dest_ids.push_back(archive_dest_id))) {
    LOG_WARN("failed to push back archive dest id", K(ret), K(archive_dest_id));
  } else if (OB_INVALID_DEST_ID != backup_dest_id && OB_FAIL(dest_ids.push_back(backup_dest_id))) {
    LOG_WARN("failed to push back backup dest id", K(ret), K(backup_dest_id));
  } else {
    if (OB_SUCC(ret)) {
      TCRLockGuard guard(lock_);
      ObDestIOPermissionMap::iterator iter = dest_io_permission_map_.begin();
      for (; OB_SUCC(ret) && iter != dest_io_permission_map_.end(); ++iter) {
        const int64_t dest_id = iter->first;
        const int64_t last_access_time = iter->second.last_access_time_;
        if (dest_id != archive_dest_id && dest_id != backup_dest_id) {
          if (curr_time_us - last_access_time > PERMISSION_EXPIRED_TIME) {
            if (OB_FAIL(inactive_dest_ids.push_back(dest_id))) {
              LOG_WARN("failed to push back inactive dest id", K(ret), K(dest_id));
            }
          } else {
            if (OB_FAIL(dest_ids.push_back(dest_id))) {
              LOG_WARN("failed to push active back dest id", K(ret), K(dest_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !inactive_dest_ids.empty()) {
      TCWLockGuard guard(lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < inactive_dest_ids.count(); ++i) {
        const int64_t &inactive_dest_id = inactive_dest_ids[i];
        if (OB_FAIL(dest_io_permission_map_.erase_refactored(inactive_dest_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to erase inactive dest id", K(ret), K(inactive_dest_id));
          }
        }
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::get_server_locality_info_(
    const ObAddr &addr,
    common::ObRegion &region,
    common::ObIDC &idc,
    common::ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObLocalityManager * locality_manager = GCTX.locality_manager_;
  zone.reset();
  idc.reset();
  region.reset();

  if (OB_ISNULL(locality_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local manager is null", K(ret));
  } else if (OB_FAIL(locality_manager->get_server_region(addr, region))) {
    LOG_WARN("failed to get self region", K(ret), K(addr));
  } else if (OB_FAIL(locality_manager->get_server_idc(addr, idc))) {
    LOG_WARN("failed to get self idc", K(ret), K(addr));
  } else if (OB_FAIL(locality_manager->get_server_zone(addr, zone))) {
    LOG_WARN("failed to get self zone", K(ret), K(addr));
  }
  if (OB_FAIL(ret)) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("not set locality info, default same.", K(ret), K(addr), K(region), K(idc), K(zone));
      ret = OB_SUCCESS;
    }
  } else {
    LOG_INFO("succeed to get region and idc", K(addr), K(region), K(idc), K(zone));
  }
  return ret;
}


// extension_ may has contain multiple pieces of information, such as 'appid=xxx&zone=z1,z2;z3&s3_region=xxx'
// src info can have various types
// need get src info and src type from extension, src info = 'z1,z2;z3', src_type = ObBackupSrcType::ZONE
int ObBackupDestIOPermissionMgr::get_src_info_from_extension(
    const ObString &extension,
    char *src_locality,
    const int64_t src_locality_length,
    share::ObBackupSrcType &src_type)
{
  int ret = OB_SUCCESS;
  const char *buf = extension.ptr();
  int64_t extension_len = extension.length();
  src_type = ObBackupSrcType::EMPTY;
  if (extension.empty()) {
    //do nothing
  } else if (OB_ISNULL(src_locality) || OB_MAX_BACKUP_STORAGE_INFO_LENGTH <= extension_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(src_locality), K(extension_len));
  } else {
    char tmp[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
    char *token = NULL;
    char *saved_ptr = NULL;
    int64_t pos = 0;
    if (sizeof(tmp) < OB_MAX_BACKUP_STORAGE_INFO_LENGTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("malloc tmp buffer failed", K(ret), K(sizeof(tmp)), K(OB_MAX_BACKUP_STORAGE_INFO_LENGTH));
    } else {
      MEMCPY(tmp, buf, extension_len);
      tmp[extension_len] = '\0';
      token = tmp;
      for (char *str = token; OB_SUCC(ret); str = NULL) {
        token = ::strtok_r(str, "&", &saved_ptr);
        if (NULL == token) {
          break;
        } else if (0 == strncmp(BACKUP_ZONE, token, strlen(BACKUP_ZONE))) {
          src_type = ObBackupSrcType::ZONE;
          if (OB_FAIL(databuff_printf(src_locality, src_locality_length, pos, "%s", token+strlen(BACKUP_ZONE)))) {
            LOG_WARN("failed to set src info", K(ret));
          }
        } else if (0 == strncmp(BACKUP_REGION, token, strlen(BACKUP_REGION))) {
          src_type = ObBackupSrcType::REGION;
          if (OB_FAIL(databuff_printf(src_locality, src_locality_length, pos, "%s", token+strlen(BACKUP_REGION)))) {
            LOG_WARN("failed to set src info", K(ret));
          }
        } else if (0 == strncmp(BACKUP_IDC, token, strlen(BACKUP_IDC))) {
          src_type = ObBackupSrcType::IDC;
          if (OB_FAIL(databuff_printf(src_locality, src_locality_length, pos, "%s", token+strlen(BACKUP_IDC)))) {
            LOG_WARN("failed to set src info", K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("get src info from extension", K(ret), K(src_type), K(src_locality));
  return ret;
}

int ObBackupDestIOPermissionMgr::check_zone_in_src_info_(
    const char *src_info,
    const ObZone &zone,
    bool &io_prohibited) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupZone> backup_zone_array;
  io_prohibited = false;
  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_zone_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else {
    io_prohibited = true;
    ARRAY_FOREACH_X(backup_zone_array, i, cnt, OB_SUCC(ret)) {
      if (backup_zone_array.at(i).zone_ == zone) {
        io_prohibited = false;
        break;
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::check_idc_in_src_info_(
    const char *src_info,
    const ObIDC &idc,
    bool &io_prohibited) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupIdc> backup_idc_array;
  io_prohibited = false;

  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_idc_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else {
    io_prohibited = true;
    ARRAY_FOREACH_X(backup_idc_array, i, cnt, OB_SUCC(ret)) {
      if (backup_idc_array.at(i).idc_ == idc) {
        io_prohibited = false;
        break;
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::check_region_in_src_info_(
    const char *src_info,
    const ObRegion &region,
    bool &io_prohibited) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupRegion> backup_region_array;
  io_prohibited = false;

  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_region_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else {
    io_prohibited = true;
    ARRAY_FOREACH_X(backup_region_array, i, cnt, OB_SUCC(ret)) {
      if (backup_region_array.at(i).region_ == region) {
        io_prohibited = false;
        break;
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::refresh_io_permission()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<std::pair<int64_t, ObString>> dest_id_and_extensions;
  ObArray<int64_t> dest_ids;
  const int64_t now_time = ObTimeUtility::current_time();
  ObDestIOProhibitedInfo prohibited_info;
  share::ObBackupSrcType src_type;
  const ObAddr &self_addr = GCTX.self_addr();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDestIOPermissionMgr not init", K(ret));
  } else if (OB_FAIL(get_server_locality_info_(self_addr, region_, idc_, zone_))) {
    LOG_WARN("failed to get server geography info", K(ret));
  } else if (OB_FAIL(refresh_and_get_dest_ids_in_map_(dest_ids))) {
    LOG_WARN("failed to statistic dest ids need refresh", K(ret));
  } else if (0 != dest_ids.count() && OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_extensions(
      gen_user_tenant_id(tenant_id_), dest_ids, allocator, dest_id_and_extensions))) {
    LOG_WARN("failed to get backup dest extensions", K(ret));
  } else {
    ARRAY_FOREACH_X(dest_id_and_extensions, i, cnt, OB_SUCC(ret)) {
      const int64_t &dest_id = dest_id_and_extensions.at(i).first;
      const ObString &extension = dest_id_and_extensions.at(i).second;
      char src_info[OB_MAX_BACKUP_SRC_INFO_LENGTH] = {0};
      bool io_prohibited = false;
      src_type = ObBackupSrcType::EMPTY;
      if (OB_SUCC(ret)) {
        TCRLockGuard guard(lock_);
        if (OB_FAIL(dest_io_permission_map_.get_refactored(dest_id, prohibited_info))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to get dest io permissiom info from map", K(ret), K(dest_id));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(get_src_info_from_extension(extension, src_info, sizeof(src_info), src_type))) {
          LOG_WARN("failed to get src info from extension", K(ret), K(extension));
        } else if (ObBackupSrcType::ZONE == src_type && OB_FAIL(check_zone_in_src_info_(src_info,
                                                                    zone_, io_prohibited))) {
          LOG_WARN("failed to check zone in src info", K(ret), K(src_info), K(zone_));
        } else if (ObBackupSrcType::IDC == src_type && OB_FAIL(check_idc_in_src_info_(src_info,
                                                                  idc_, io_prohibited))) {
          LOG_WARN("failed to check idc in src info", K(ret), K(src_info), K(idc_));
        } else if (ObBackupSrcType::REGION == src_type && OB_FAIL(check_region_in_src_info_(src_info,
                                                                  region_, io_prohibited))) {
          LOG_WARN("failed to check region in src info", K(ret), K(src_info), K(region_));
        }
      }
      if (OB_SUCC(ret) && io_prohibited != prohibited_info.io_prohibited_) {
        TCWLockGuard guard(lock_);
        if (OB_FAIL(dest_io_permission_map_.get_refactored(dest_id, prohibited_info))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to get dest io permissiom info from map", K(ret), K(dest_id));
          } else {
            ret = OB_SUCCESS;
            LOG_WARN("dest not exist in map", K(ret), K(dest_id));
          }
        } else {
          prohibited_info.io_prohibited_ = io_prohibited;
          if (FAILEDx(dest_io_permission_map_.set_refactored(dest_id, prohibited_info, true /*conver exists key*/))) {
            LOG_WARN("failed to update dest io permission map", K(ret), K(dest_id));
          } else {
            LOG_INFO("success update dest io permission map", K(ret), K(dest_id), K(io_prohibited));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    TCWLockGuard guard(lock_);
    last_refresh_time_ = now_time;
    LOG_DEBUG("success refresh io permission", K(now_time));
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::get_backup_and_archive_path_dest_id_(int64_t &backup_dest_id, int64_t &archive_dest_id) const
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper archive_helper;
  share::ObBackupHelper backup_helper;
  ObBackupPathString backup_dest_str;
  ObBackupDest dest;
  // Only one dest is supported.
  const int64_t dest_no = 0;
  const bool need_lock = true;
  uint64_t tenant_id = gen_user_tenant_id(tenant_id_);

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP(GCTX.sql_proxy_));
  } else {
    ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
    if (OB_FAIL(archive_helper.init(tenant_id))) {
      LOG_WARN("fail to init archive helper", K(ret), K(tenant_id));
    } else if (OB_FAIL(archive_helper.get_dest_id(sql_proxy, need_lock, dest_no, archive_dest_id))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get archive dest id", K(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (FAILEDx(backup_helper.init(tenant_id, sql_proxy))) {
      LOG_WARN("fail to init backup helper", K(ret), K(tenant_id));
    } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get backup dest id", K(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (backup_dest_str.is_empty()) {
      //do nothing
    } else if (OB_FAIL(dest.set(backup_dest_str.ptr()))) {
      LOG_WARN("fail to set backup dest", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(sql_proxy, tenant_id, dest, backup_dest_id))) {
      LOG_WARN("failed to get dest id", K(ret), K(dest));
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::is_io_prohibited(const common::ObObjectStorageInfo *storage_info, bool &is_io_prohibited)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = OB_INVALID_DEST_ID;
  char extension[OB_MAX_BACKUP_EXTENSION_LENGTH] = {0};
  int64_t pos = 0;
  ObDestIOProhibitedInfo io_prohibited_info;
  is_io_prohibited = false;
  int64_t now_time = ObTimeUtility::current_time();
  int tmp_ret = OB_SUCCESS;
  bool has_find_in_map = false;
  if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(storage_info));
  } else if (storage_info->is_backup_storage_info()) {
    dest_id = static_cast<const share::ObBackupStorageInfo *>(storage_info)->get_dest_id();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_DEST_ID == dest_id) {
    //do nothing
  } else {
    TCRLockGuard guard(lock_);
    tmp_ret = dest_io_permission_map_.get_refactored(dest_id, io_prohibited_info);
    if (OB_SUCC(tmp_ret) && REFRESH_TIMEOUT > now_time - last_refresh_time_) {
      is_io_prohibited = io_prohibited_info.io_prohibited_;
      has_find_in_map = true;
    } else if (OB_HASH_NOT_EXIST != tmp_ret && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to get from dest io permission map", KR(ret), K(dest_id));
    } else {
      char src_info[OB_MAX_BACKUP_SRC_INFO_LENGTH] = {0};
      share::ObBackupSrcType src_type;
      if (OB_FAIL(databuff_printf(extension, OB_MAX_BACKUP_EXTENSION_LENGTH,
                    pos, "%s", storage_info->get_extension()))) {
        LOG_WARN("failed to get extension from storage info", K(ret), K(storage_info));
      } else if (OB_FAIL(get_src_info_from_extension(ObString(extension),
                            src_info, sizeof(src_info), src_type))) {
        LOG_WARN("failed to get src info from extension", K(ret));
      } else if (ObBackupSrcType::ZONE == src_type && OB_FAIL(check_zone_in_src_info_(src_info,
                                                                  zone_, is_io_prohibited))) {
        LOG_WARN("failed to check zone in src info", K(ret), K(src_info), K(zone_));
      } else if (ObBackupSrcType::IDC == src_type && OB_FAIL(check_idc_in_src_info_(src_info,
                                                                  idc_, is_io_prohibited))) {
        LOG_WARN("failed to check idc in src info", K(ret), K(src_info), K(idc_));
      } else if (ObBackupSrcType::REGION == src_type && OB_FAIL(check_region_in_src_info_(src_info,
                                                                    region_, is_io_prohibited))) {
        LOG_WARN("failed to check region in src info", K(ret), K(src_info), K(region_));
      }
    }
  }

  if (OB_START_DEST_ID <= dest_id && OB_SUCC(ret) &&
          now_time - io_prohibited_info.last_access_time_ > PERMISSION_UPDATE_INTERVAL) {
    if (OB_FAIL(update_last_access_time_(dest_id, is_io_prohibited))) {
      LOG_WARN("failed to add after remove oldest dest id", K(ret), K(dest_id), K(is_io_prohibited));
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::update_last_access_time_(
    const int64_t dest_id,
    const bool is_io_prohibited)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDestIOProhibitedInfo io_prohibited_info;
  TCWLockGuard guard(lock_);
  tmp_ret = dest_io_permission_map_.get_refactored(dest_id, io_prohibited_info);
  if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
    ret = tmp_ret;
    LOG_WARN("failed to get from dest io permission map", KR(ret), K(dest_id));
  } else if (OB_SUCCESS == tmp_ret) {
    io_prohibited_info.last_access_time_= ObTimeUtility::current_time();
    if (OB_FAIL(dest_io_permission_map_.set_refactored(dest_id,
                                                io_prohibited_info, true /*conver exists key*/))) {
      LOG_WARN("failed to update dest io permission map", K(ret), K(dest_id));
    }
  } else {
    if (dest_io_permission_map_.size() >= OB_MAX_BACKUP_DEST_COUNT) {
      io_prohibited_info.last_access_time_= ObTimeUtility::current_time();
      io_prohibited_info.io_prohibited_ = is_io_prohibited;
      int64_t oldest_dest_id = OB_INVALID_DEST_ID;
      int64_t oldest_insert_timestamp = OB_INVALID_TIMESTAMP;
      ObDestIOPermissionMap::iterator iter = dest_io_permission_map_.begin();
      for (; OB_SUCC(ret) && iter != dest_io_permission_map_.end(); ++iter) {
        if (iter->second.last_access_time_ < oldest_insert_timestamp || OB_INVALID_TIMESTAMP == oldest_insert_timestamp) {
          oldest_dest_id = iter->first;
          oldest_insert_timestamp = iter->second.last_access_time_;
        }
      }
      if (OB_SUCC(ret) && OB_INVALID_DEST_ID != oldest_dest_id) {
        if (OB_FAIL(dest_io_permission_map_.erase_refactored(oldest_dest_id))) {
          LOG_WARN("failed to erase dest io permission map", K(ret), K(oldest_dest_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dest_io_permission_map_.set_refactored(dest_id,
                    io_prohibited_info, true /*conver exists key*/))) {
        LOG_WARN("failed to update dest io permission map", K(ret), K(dest_id));
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::check_backup_src_info_valid(
        const char *backup_src_info,
        const ObBackupSrcType &backup_src_type)
{
  int ret = OB_SUCCESS;
  uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();

  if ((CLUSTER_VERSION_4_3_0_0 <= min_cluster_version && min_cluster_version < CLUSTER_VERSION_4_4_1_0)
          || (CLUSTER_VERSION_4_2_2_0 < min_cluster_version && min_cluster_version < MOCK_CLUSTER_VERSION_4_2_5_2)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup zone is not supported for current cluster version", K(ret), K(min_cluster_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "setting backup zone is");
  } else if (OB_ISNULL(backup_src_info) || backup_src_info[0] == '\0'
          || ObBackupSrcType::EMPTY > backup_src_type || ObBackupSrcType::MAX <= backup_src_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_src_info), K(backup_src_type));
  } else if (ObBackupSrcType::ZONE == backup_src_type && OB_FAIL(check_zone_valid(backup_src_info))) {
    LOG_WARN("failed to check zone valid", K(ret), K(backup_src_info));
  } else if (ObBackupSrcType::IDC == backup_src_type && OB_FAIL(check_idc_valid(backup_src_info))) {
    LOG_WARN("failed to check idc valid", K(ret), K(backup_src_info));
  } else if (ObBackupSrcType::REGION == backup_src_type && OB_FAIL(check_region_valid(backup_src_info))) {
    LOG_WARN("failed to check region valid", K(ret), K(backup_src_info));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("success check backup src info valid", K(backup_src_info), K(backup_src_type));
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::get_backup_path_src_info(
    char *src_locality,
    const int64_t src_locality_length,
    share::ObBackupSrcType &src_type) const
{
  int ret = OB_SUCCESS;
  share::ObBackupHelper backup_helper;
  ObBackupPathString backup_dest_str;
  char extension[OB_MAX_BACKUP_EXTENSION_LENGTH] = {0};
  char backup_path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  ObBackupDest dest;
  uint64_t tenant_id = gen_user_tenant_id(tenant_id_);

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP(GCTX.sql_proxy_));
  } else {
    ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
    if (OB_FAIL(backup_helper.init(tenant_id, sql_proxy))) {
      LOG_WARN("fail to init backup help", K(ret));
    } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
      LOG_WARN("fail to get backup dest", K(ret), K(tenant_id));
    } else if (backup_dest_str.is_empty()) {
      ret = OB_BACKUP_CAN_NOT_START;
      LOG_WARN("empty backup dest is not allowed, backup can't start", K(ret));
    } else if (OB_FAIL(dest.set(backup_dest_str.ptr()))) {
      LOG_WARN("fail to set backup dest", K(ret), K(tenant_id));
    } else if (OB_FAIL(dest.get_backup_path_str(backup_path, sizeof(backup_path)))) {
      LOG_WARN("fail to get backup path str", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_extension(sql_proxy,
                                                      gen_user_tenant_id(tenant_id_),
                                                      dest, extension, sizeof(extension)))) {
      LOG_WARN("failed to get backup dest extension", K(ret), K(tenant_id));
    } else if (OB_FAIL(get_src_info_from_extension(ObString(extension), src_locality, src_locality_length, src_type))) {
      LOG_WARN("failed to get src info from extension", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("success get backup path src info", K(src_locality), K(src_type), K(tenant_id));
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::check_zone_valid(const char *src_info)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupZone> backup_zone_array;
  ObArray<ObZone> zone_array;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_zone_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObZoneTableOperation::get_zone_list(*GCTX.sql_proxy_, zone_array))) {
    LOG_WARN("failed to get region list", K(ret));
  } else {
    ARRAY_FOREACH_X(backup_zone_array, i, cnt, OB_SUCC(ret)) {
      const ObZone &tmp_zone = backup_zone_array.at(i).zone_;
      bool found = false;
      for (int64_t j = 0; !found && j < zone_array.count(); ++j) {
        const ObZone &zone = zone_array.at(j);
        if (tmp_zone == zone) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_BACKUP_ZONE_IDC_REGION_INVALID;
        LOG_WARN("src info input is not exist in zone array", K(ret), K(src_info), K(zone_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
            pos, "zone do not exist in zone list. can not set zone : %s.", src_info))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_BACKUP_ZONE_IDC_REGION_INVALID, error_msg);
        }
      }
    }
  }
  return ret;
}

int ObBackupDestIOPermissionMgr::check_region_valid(const char *src_info)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupRegion> backup_region_array;
  ObArray<ObRegion> region_array;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_region_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObZoneTableOperation::get_region_list(*GCTX.sql_proxy_, region_array))) {
    LOG_WARN("failed to get region list", K(ret));
  } else {
    ARRAY_FOREACH_X(backup_region_array, i, cnt, OB_SUCC(ret)) {
      const ObRegion &tmp_region = backup_region_array.at(i).region_;
      bool found = false;
      for (int64_t j = 0; !found && j < region_array.count(); ++j) {
        const ObRegion &region = region_array.at(j);
        if (tmp_region == region) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_BACKUP_ZONE_IDC_REGION_INVALID;
        LOG_WARN("src info input is not exist in region array", K(ret), K(src_info), K(region_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
            pos, "region do not exist in region list. can not set region : %s.", src_info))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_BACKUP_ZONE_IDC_REGION_INVALID, error_msg);
        }
      }
    }
  }
  return ret;
}

int ObBackupDestIOPermissionMgr::check_idc_valid(const char *src_info)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupIdc> backup_idc_array;
  ObArray<ObIDC> idc_array;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(src_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(share::ObBackupUtils::parse_backup_format_input(ObString(src_info), backup_idc_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(src_info));
  } else if (OB_FAIL(share::ObZoneTableOperation::get_idc_list(*GCTX.sql_proxy_, idc_array))) {
    LOG_WARN("failed to get idc list", K(ret));
  } else {
    ARRAY_FOREACH_X(backup_idc_array, i, cnt, OB_SUCC(ret)) {
      const ObIDC &tmp_idc = backup_idc_array.at(i).idc_;
      bool found = false;
      for (int64_t j = 0; !found && j < idc_array.count(); ++j) {
        const ObIDC &idc = idc_array.at(j);
        if (tmp_idc == idc) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_BACKUP_ZONE_IDC_REGION_INVALID;
        LOG_WARN("idc is not exist in idc list", K(ret), K(src_info), K(idc_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
            pos, "idc do not exist in idc list. can not set idc : %s.", src_info))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_BACKUP_ZONE_IDC_REGION_INVALID, error_msg);
        }
      }
    }
  }
  return ret;
}

int ObBackupChangeExternalStorageDestUtil::change_src_info(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObBackupDestAttribute &option,
    const ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  bool do_not_need_update = false;

  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid() || OB_ISNULL(option.src_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest), KP(option.src_info_));
  } else {
    char extension[OB_MAX_BACKUP_EXTENSION_LENGTH] = { 0 };
    const char *src_info = option.src_info_;
    if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_extension(
              proxy, tenant_id, backup_dest, extension, sizeof(extension)))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("path is not exist, please check the path", K(ret), K(tenant_id));
      } else {
        LOG_WARN("failed to get backup dest extension", K(ret), K(tenant_id), K(backup_dest));
      }
    } else if (OB_FAIL(process_src_info_in_extension_before_update(src_info,
                          extension, sizeof(extension), do_not_need_update))) {
      LOG_WARN("failed to update src info in extension", K(ret), K(src_info), KP(extension));
    } else if (!do_not_need_update) {
      if (OB_FAIL(ObBackupStorageInfoOperator::update_backup_dest_extension(proxy,
                                                    gen_user_tenant_id(tenant_id), backup_dest, extension))) {
          LOG_WARN("failed to update backup dest extension", K(ret), K(tenant_id), K(backup_dest));
      } else {
        LOG_INFO("success change src info", K(src_info));
      }
    }
  }

  return ret;
}

//find locality info in extension
//update extension with new locality info
//compare new locality info with old locality info
//if new locality info is same with old locality info, set do_not_need_update to true

int ObBackupChangeExternalStorageDestUtil::process_src_info_in_extension_before_update(
    const char *src_info,
    char *extension,
    const int64_t extension_length,
    bool &do_not_need_update)
{
  int ret = OB_SUCCESS;
  do_not_need_update = false;
  if (OB_ISNULL(src_info) || OB_ISNULL(extension)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_info), KP(extension));
  } else {
    int64_t pos =0;
    if ('\0' == extension[0]) {
      //do nothing
    } else {
      //delete old src info from extension
      char tmp[OB_MAX_BACKUP_EXTENSION_LENGTH] = { 0 };
      char *token = NULL;
      char *saved_ptr = NULL;
      bool has_found_src_info = false;

      if (OB_FAIL(databuff_printf(tmp, sizeof(tmp), "%s", extension))) {
        LOG_WARN("failed to copy extension", K(ret), K(extension));
      } else {
        extension[0] = '\0';
        token = tmp;
        for (char *str = token; !has_found_src_info && OB_SUCC(ret); str = NULL) {
          token = ::strtok_r(str, "&", &saved_ptr);

          if (NULL == token) {
            break;
          } else if (0 == strncmp(BACKUP_ZONE, token, strlen(BACKUP_ZONE))
                      || 0 == strncmp(BACKUP_IDC, token, strlen(BACKUP_IDC))
                      || 0 == strncmp(BACKUP_REGION, token, strlen(BACKUP_REGION))) {
            has_found_src_info = true;
            if (0 == strcmp(token, src_info)) {
              do_not_need_update = true;
              LOG_INFO("src info is same with old src info", K(src_info), K(extension));
            }
          } else {
            if (pos > 0 && OB_FAIL(databuff_printf(extension, extension_length, pos, "&"))) {
              LOG_WARN("failed to add delimiter to extension", K(ret), K(pos), K(extension_length), KP(extension));
            } else if (OB_FAIL(databuff_printf(extension, extension_length, pos, "%s", token))) {
              LOG_WARN("failed to add token to extension", K(ret), K(pos),
                  K(extension_length), KP(token), KP(extension));
            }
          }
        }

        if (OB_SUCC(ret) && !OB_ISNULL(saved_ptr) && 0 != strlen(saved_ptr)) {
          if (pos > 0 && OB_FAIL(databuff_printf(extension, extension_length, pos, "&"))) {
            LOG_WARN("failed to add delimiter to extension", K(ret), K(pos), K(extension_length), KP(extension));
          } else if (OB_FAIL(databuff_printf(extension, extension_length, pos, "%s", saved_ptr))) {
            LOG_WARN("failed to add rest info to extension",
              K(ret), K(pos), K(extension_length), KP(saved_ptr), KP(extension));
          }
        }
      }
    }

    //add src info
    if (OB_SUCC(ret)) {
      if ( '\0' == src_info[0]) {
        //do nothing
      } else if (pos > 0 && OB_FAIL(databuff_printf(extension, extension_length, pos, "&"))) {
        LOG_WARN("failed to add delimiter to extension", K(ret), K(pos), K(extension_length), KP(extension));
      } else if (OB_FAIL(databuff_printf(extension, extension_length, pos, "%s", src_info))) {
        LOG_WARN("failed to add src info to extension", K(ret), K(pos), K(extension_length),
            KP(src_info), KP(extension));
      } else {
        LOG_INFO("success update src info in extension", K(src_info), K(extension));
      }
    }
  }

  return ret;
}

int ObBackupDestIOPermissionMgr::delete_locality_info_in_backup_dest_str(char *backup_dest_str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char locality_info[OB_MAX_BACKUP_SRC_INFO_LENGTH] = { 0 };
  if (OB_ISNULL(backup_dest_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    while (backup_dest_str[pos] != '\0') {
      if ('?' == backup_dest_str[pos]) {
        ++pos;
        break;
      }
      ++pos;
    }
    if (backup_dest_str[pos] != '\0' && OB_FAIL(separate_locality_info_from_dest_string(
                                                    backup_dest_str + pos, strlen(backup_dest_str + pos) + 1,
                                                    locality_info, OB_MAX_BACKUP_SRC_INFO_LENGTH))) {
      LOG_WARN("fail to delete locality info in path info", K(ret), K(backup_dest_str + pos));
    } else if(pos > 0 && '\0' == backup_dest_str[pos--] && '?' == backup_dest_str[pos]) {
      backup_dest_str[pos] = '\0';
    }
  }
  return ret;
}

//input:dest_attribute="host=xxxx&access_key=adsd&access_id=dsfdf&zone=z1,z2&checksum_type=md5"
//output:dest_attribute="host=xxxx&access_key=adsd&access_id=dsfdf&checksum_type=md5" locality_info="zone=z1,z2"
int ObBackupDestIOPermissionMgr::separate_locality_info_from_dest_string(
    char *dest_string,
    const int64_t dest_string_length,
    char *locality_info,
    const int64_t locality_info_max_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_string) || OB_ISNULL(locality_info) || 0 == dest_string_length || 0 == locality_info_max_length) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(dest_string), KP(locality_info),
                K(dest_string_length), K(locality_info_max_length));
  } else {
    char tmp[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    char *token = NULL;
    char *saved_ptr = NULL;
    if (OB_FAIL(databuff_printf(tmp, sizeof(tmp), "%s", dest_string))) {
        LOG_WARN("failed to copy storage info", K(ret), K(dest_string));
    } else {
      dest_string[0] = '\0';
      locality_info[0] = '\0';
      int64_t pos = 0;
      token = tmp;
      for (char *str = token; OB_SUCC(ret); str = NULL) {
        token = ::strtok_r(str, "&", &saved_ptr);
        if (NULL == token) {
          break;
        } else if (0 == strncmp(BACKUP_ZONE, token, strlen(BACKUP_ZONE))
                      || 0 == strncmp(BACKUP_IDC, token, strlen(BACKUP_IDC))
                      || 0 == strncmp(BACKUP_REGION, token, strlen(BACKUP_REGION))) {
          if (OB_FAIL(databuff_printf(locality_info, locality_info_max_length, "%s", token))) {
            LOG_WARN("failed to add token to locality info buf", K(ret), K(locality_info_max_length), KP(locality_info));
          }
        } else {
          if (pos > 0 && OB_FAIL(databuff_printf(dest_string, dest_string_length, pos, "&"))) {
            LOG_WARN("failed to add delimiter to buff", K(ret), K(pos), K(dest_string_length), KP(dest_string));
          } else if (OB_FAIL(databuff_printf(dest_string, dest_string_length, pos, "%s", token))) {
            LOG_WARN("failed to add token to buff", K(ret), K(pos),
                K(dest_string_length), K(token), KP(dest_string));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDestIOPermissionMgr::filter_server_list_by_src_info(
  const common::ObArray<ObAddr> &server_list,
  const char *extension,
  common::ObArray<ObAddr> &filtered_server_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extension)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(extension));
  } else {
    ObBackupSrcInfo src_info;
    common::ObRegion region;
    common::ObIDC idc;
    common::ObZone zone;
    bool locality_valid = false;
    if (OB_FAIL(get_src_info_from_extension(extension, src_info))) {
      LOG_WARN("failed to get src info from extension", K(ret), K(extension));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const ObAddr &server = server_list.at(i);
      zone.reset();
      idc.reset();
      region.reset();
      locality_valid = false;
      if (OB_FAIL(get_server_locality_info_(server, region, idc, zone))) {
        LOG_WARN("failed to get server locality info", K(ret), K(server));
      } else if (OB_FAIL(src_info.check_locality_info_valid(region, idc, zone, locality_valid))) {
        LOG_WARN("failed to check locality info valid", K(ret), K(server), K(region), K(idc), K(zone));
      } else if (locality_valid) {
        if (OB_FAIL(filtered_server_list.push_back(server))) {
          LOG_WARN("failed to push back server", K(ret), K(server));
        }
      }
    }
  }
  return ret;
}

int ObBackupDestIOPermissionMgr::get_src_info_from_extension(const char *extension, ObBackupSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extension)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(extension));
  } else {
    src_info.reset();
    char locality_info_str[OB_MAX_BACKUP_SRC_INFO_LENGTH] = {0};
    if (OB_FAIL(get_src_info_from_extension(ObString(extension), locality_info_str,
                                              sizeof(locality_info_str), src_info.src_type_))) {
      LOG_WARN("failed to get src info from extension", K(ret), K(extension));
    } else if ('\0' !=  locality_info_str[0]) {
      if (OB_FAIL(ObBackupUtils::parse_backup_format_input(ObString(locality_info_str), src_info.locality_list_))) {
        LOG_WARN("failed to parse backup format input", K(ret), K(locality_info_str));
      }
    }
  }
  return ret;
}

}//share
}//oceanbase
