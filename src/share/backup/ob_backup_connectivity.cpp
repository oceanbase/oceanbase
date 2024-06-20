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
#include "ob_backup_connectivity.h"
#include "ob_backup_struct.h"
#include "ob_backup_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_encryption_util.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/restore/ob_object_device.h"

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

int ObBackupConnectivityCheckManager::schedule_connectivity_check_(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  // TODO(wenjinyu.wjy) in 4.3, this code logic needs to be rewritten. Since server_mgr needs to be removed, first comment the code
  // obrpc::ObCheckBackupConnectivityArg args;
  // args.tenant_id_ = tenant_id_;
  // common::ObArray<ObAddr> server_list;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("connectivity check manager not init", K(ret));
  // } else if (OB_FAIL(backup_dest.get_backup_path_str(args.backup_path_, sizeof(args.backup_path_)))) {
  //   LOG_WARN("failed to set args.backup_dest_", K(ret), K_(tenant_id));
  // } else if (OB_FAIL(databuff_printf(args.check_path_, sizeof(args.check_path_), "%s", path.get_ptr()))) {
  //   LOG_WARN("failed to set args.check_path_", K(ret), K_(tenant_id), K(path));
  // } else if (OB_FAIL(server_mgr_->get_all_server_list(server_list))) {
  //   LOG_WARN("failed to get all server list", K(ret), K_(tenant_id));
  // } else if (OB_UNLIKELY(server_list.empty())) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("no server exist", K(ret), K_(tenant_id));
  // } else {
  //   for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
  //     const common::ObAddr &dest = server_list.at(i);
  //     bool is_active = false;
  //     if (OB_FAIL(server_mgr_->check_server_active(dest, is_active))) {
  //       LOG_WARN("failed to check server active", K(ret), K_(tenant_id), K(dest));
  //     } else if (!is_active) {
  //       LOG_WARN("server is not active", K(OB_SERVER_NOT_ACTIVE), K(dest));
  //       continue;
  //     } else if (OB_FAIL(rpc_proxy_->to(dest).check_backup_dest_connectivity(args))) {
  //       if (OB_BACKUP_DEST_NOT_CONNECT == ret) {
  //         char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  //         int tmp_ret = OB_SUCCESS;
  //         if (OB_SUCCESS != (tmp_ret = dest.ip_port_to_string(ip, sizeof(ip)))) {
  //           LOG_WARN("fail to convert ip to string", K(tmp_ret), K(dest));
  //         } else {
  //           ROOTSERVICE_EVENT_ADD("connectivity_check", "backup_dest_not_connectivity", "ip:port", ip,
  //               "tenant_id", tenant_id_, "error_code", ret, "comment", "backup_dest is disconnect");
  //         }
  //       }
  //       LOG_WARN("failed to check backup_dest connectivity", KR(ret), K_(tenant_id), K(dest)); 
  //     }
  //   }
  // }
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
  // TODO(wenjinyu.wjy) in 4.3, support check connectivity
  //} else if (OB_FAIL(schedule_connectivity_check_(backup_dest, path))) {
  //  LOG_WARN("failed to schedule connectivity check", K(ret), K_(tenant_id));
  } else if (OB_FAIL(set_last_check_time_(backup_dest))) {
    LOG_WARN("failed to set last check time", K(ret), K_(tenant_id), K(backup_dest));
  } else {
    FLOG_INFO("[BACKUP_DEST_CHECK] succeed to finish backup_dest connectivity check", K_(tenant_id), K(backup_dest));
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
          if(OB_FAIL(util.adaptively_del_file(uri, backup_dest.get_storage_info()))) {
            LOG_WARN("failed to delete check file", K(ret), K_(tenant_id));
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
          if(OB_FAIL(util.adaptively_del_file(uri, backup_dest.get_storage_info()))) {
            LOG_WARN("failed to delete permission check file", K(ret), K_(tenant_id));
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
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup check file not init", K(ret));
  } else if (OB_FAIL(get_permission_check_file_path_(backup_dest, ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER, path))) {
      LOG_WARN("failed to get permission check file path", K(ret), K_(tenant_id), K(backup_dest)); 
  } else if (OB_FAIL(util.set_access_type(&iod_opts, true/*is_appender*/, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set access type");
  } else if (OB_FAIL(util.set_append_strategy(&iod_opts, is_data_file, epoch, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set append strategy");
  } else if (OB_FAIL(util.get_and_init_device(device_handle, backup_dest.get_storage_info(), path.get_obstr()))) {
    LOG_WARN( "fail to get device", K(ret));
  } else if (OB_FAIL(device_handle->open(path.get_ptr(), -1/* flag */, 0/* mode */, fd, &iod_opts))) { // flag=-1 and mode=0 are invalid, because oss/cos unused flag and mode;
    LOG_WARN("fail to open file", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(databuff_printf(data, sizeof(data), "tenant(%lu) appender writer at %ld", tenant_id_, ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set data", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(device_handle->write(fd, data, strlen(data),  write_size))) {
    LOG_WARN("fail to write file", K(ret), K(path.get_ptr()), K(data));
  } else if (OB_FAIL(util.adaptively_del_file(path.get_obstr(), backup_dest.get_storage_info()))) {
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
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, backup_dest.get_storage_info(), path.get_obstr(), access_type))) {
    LOG_WARN("fail to open device or fd", K(ret), K(backup_dest), K(path));
  } else if (OB_FAIL(databuff_printf(data, sizeof(data), "tenant(%lu) multipart writer at %ld", tenant_id_, ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set data", K(ret), K(path.get_ptr()));
  } else if (OB_FAIL(device_handle->pwrite(fd, offset, strlen(data), data, write_size))) {
    LOG_WARN("fail to write file", K(ret), K(path.get_ptr()), K(data));
  } else if (OB_FAIL(device_handle->complete(fd))) {
    STORAGE_LOG(WARN, "fail to complete multipart upload", K(ret), K(device_handle), K(fd));
  } else if (OB_FAIL(util.del_file(path.get_obstr(), backup_dest.get_storage_info()))) {
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
      || OB_OSS_ERROR == result
      || OB_COS_ERROR == result
      || OB_S3_ERROR == result) {
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
  } else if (OB_FAIL(util.adaptively_read_single_file(path.get_obstr(), backup_dest.get_storage_info(), buf, file_len, read_size))) {
    if (is_permission_error_(ret)) {
      ret = OB_BACKUP_PERMISSION_DENIED;
      ROOTSERVICE_EVENT_ADD("connectivity_check", "permission check", 
          "tenant_id", tenant_id_, "error_code", ret, "comment", "read single file");
    }
    LOG_WARN("failed to read single file", K(ret));
  }
  if (write_ok && (OB_SUCCESS != (tmp_ret = util.adaptively_del_file(path.get_obstr(), backup_dest.get_storage_info())))) {
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
    const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  char *root_path = NULL;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = { 0 };
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_storage_info()->get_authorization_info(authorization, sizeof(authorization)))) {
    LOG_WARN("fail to set authorization", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
      || OB_FAIL(dml.add_pk_column(OB_STR_PATH, backup_dest.get_root_path().ptr()))
      || OB_FAIL(dml.add_pk_column(OB_STR_BACKUP_DEST_ENDPOINT, backup_dest.get_storage_info()->endpoint_))
      || OB_FAIL(dml.add_column(OB_STR_DEST_ID, dest_id))
      || OB_FAIL(dml.add_column(OB_STR_DEST_TYPE, ObBackupDestType::get_str(dest_type))) 
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_AUTHORIZATION, authorization))
      || OB_FAIL(dml.add_column(OB_STR_BACKUP_DEST_EXTENSION, backup_dest.get_storage_info()->extension_))) {
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
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("update backup authorization in storage info", K(sql), K(tenant_id), K(backup_dest));
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
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(parse_backup_path(backup_path.ptr(), path, sizeof(path), endpoint, sizeof(endpoint)))) {
    LOG_WARN("failed to parse backup path", K(ret), K(tenant_id), K(backup_path)); 
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT %s,%s FROM %s WHERE tenant_id = %lu AND path = '%s' AND endpoint = '%s'",
      OB_STR_BACKUP_DEST_AUTHORIZATION, OB_STR_BACKUP_DEST_EXTENSION, OB_ALL_BACKUP_STORAGE_INFO_TNAME,
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
  } else if (OB_FAIL(backup_dest.set(path, endpoint, encrypt_authorization, extension))) {
    LOG_WARN("fail to set backup dest", K(ret), K(tenant_id)); 
  } else {
    LOG_INFO("success get backup dest", K(sql), K(tenant_id), K(backup_dest)); 
  }
  return ret;
}
}//share
}//oceanbase