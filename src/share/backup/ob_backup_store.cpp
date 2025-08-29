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
#include "ob_backup_store.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_path.h"
#include "lib/random/ob_mysql_random.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase;
using namespace common;
using namespace share;

/**
 * ------------------------------ObBackupFormatDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObBackupFormatDesc, cluster_name_, tenant_name_, path_,
    cluster_id_, tenant_id_, incarnation_, dest_id_, dest_type_, ts_);

ObBackupFormatDesc::ObBackupFormatDesc()
{
  cluster_id_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  incarnation_ = OB_START_INCARNATION;
  dest_id_ = 0;
  dest_type_ = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  ts_ = ObTimeUtility::current_time();
}

bool ObBackupFormatDesc::is_valid() const
{
  return !cluster_name_.is_empty()
         && !tenant_name_.is_empty()
         && !path_.is_empty()
         && OB_INVALID_TENANT_ID != tenant_id_
         && OB_START_INCARNATION <= incarnation_
         && 0 < dest_id_
         && 0 <= ts_;
}

uint16_t ObBackupFormatDesc::get_data_type() const
{
  return ObBackupFileType::BACKUP_FORMAT_FILE;
}

uint16_t ObBackupFormatDesc::get_data_version() const
{
  return FILE_VERSION;
}

bool ObBackupFormatDesc::is_format_equal(const ObBackupFormatDesc &desc) const
{
  return path_ == desc.path_
      && cluster_id_ == desc.cluster_id_
      && tenant_id_ == desc.tenant_id_
      && incarnation_ == desc.incarnation_
      && dest_id_ == desc.dest_id_
      && dest_type_ == desc.dest_type_;
}

// ------------------------------ObBackupCheckDesc---------------------
OB_SERIALIZE_MEMBER(ObBackupCheckDesc, cluster_name_, tenant_name_, path_,
    cluster_id_, tenant_id_, incarnation_, ts_);

ObBackupCheckDesc::ObBackupCheckDesc()
{
  cluster_id_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  incarnation_ = OB_START_INCARNATION;
  ts_ = ObTimeUtility::current_time();
}

bool ObBackupCheckDesc::is_valid() const
{
  return !cluster_name_.is_empty()
         && !tenant_name_.is_empty()
         && !path_.is_empty()
         && OB_INVALID_TENANT_ID != tenant_id_
         && OB_START_INCARNATION <= incarnation_
         && 0 <= ts_;
}

uint16_t ObBackupCheckDesc::get_data_type() const
{
  return ObBackupFileType::BACKUP_CHECK_FILE;
}

uint16_t ObBackupCheckDesc::get_data_version() const
{
  return FILE_VERSION;
}

// --------------------------ObBackupConsistencyCheckDesc------------------
OB_SERIALIZE_MEMBER(ObBackupConsistencyCheckDesc, tenant_id_, random_content_);
ObBackupConsistencyCheckDesc::ObBackupConsistencyCheckDesc()
  : tenant_id_(OB_INVALID_TENANT_ID)
{
  MEMSET(random_content_, 0, sizeof(random_content_));
}

bool ObBackupConsistencyCheckDesc::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
         && strlen(random_content_) > 0;
}

uint16_t ObBackupConsistencyCheckDesc::get_data_type() const
{
  return ObBackupFileType::BACKUP_CHECK_FILE;
}

uint16_t ObBackupConsistencyCheckDesc::get_data_version() const
{
  return FILE_VERSION;
}

int ObBackupConsistencyCheckDesc::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(generate_random_content_())) {
    LOG_WARN("fail to generate random content", K(ret));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

void ObBackupConsistencyCheckDesc::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  MEMSET(random_content_, 0, sizeof(random_content_));
}

int ObBackupConsistencyCheckDesc::generate_random_content_()
{
  int ret = OB_SUCCESS;
  common::ObMysqlRandom rand;
  int64_t len = sizeof(random_content_);
  uint64_t seed1 = reinterpret_cast<uint64_t>(this);
  uint64_t seed2 = static_cast<uint64_t>(ObTimeUtility::current_time());
  rand.init(seed1, seed2);
  if (OB_FAIL(rand.create_random_string(random_content_, len))) {
    LOG_WARN("fail to create random string", K(ret), K(len));
  }
  return ret;
}

/**
 * ------------------------------ObBackupStore---------------------
 */
ObBackupStore::ObBackupStore() 
  : is_inited_(false) 
{}


int ObBackupStore::init(const char *backup_dest)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupStore init twice.", K(ret));
  } else if (OB_ISNULL(backup_dest)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid archive dest.", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.set(backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(backup_dest));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObBackupStore::init(const share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupStore init twice.", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid archive dest.", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(backup_dest));
  } else {
    is_inited_ = true;
  }

  return ret;
}

bool ObBackupStore::is_init() const
{
  return IS_INIT;
}

void ObBackupStore::reset()
{
  is_inited_ = false;
  backup_dest_.reset();
}

const ObBackupDest &ObBackupStore::get_backup_dest() const
{
  return backup_dest_;
}

const ObBackupStorageInfo *ObBackupStore::get_storage_info() const
{
  return backup_dest_.get_storage_info();
}

// oss://archive/format
int ObBackupStore::get_format_file_path(ObBackupPathString &path) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBackupPath format_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(format_path.init(backup_dest_.get_root_path()))) {
    LOG_WARN("failed to get format path", K(ret));
  } else if (OB_FAIL(format_path.join(OB_STR_FORMAT_FILE_NAME, ObBackupFileSuffix::BACKUP))) {
    LOG_WARN("failed to assign format path", K(ret), K(format_path));
  } else if (OB_FAIL(databuff_printf(path.ptr(), path.capacity(), pos, "%s", format_path.get_ptr()))) {
    LOG_WARN("failed to assign format file name", K(ret), K(path));
  }
  return ret;
}

int ObBackupStore::is_format_file_exist(bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPathString full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(get_format_file_path(full_path))) {
    LOG_WARN("failed to get format file path", K(ret));
  } else if (OB_FAIL(util.is_exist(full_path.ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check format file exist.", K(ret), K(full_path));
  } 

  return ret;
}

int ObBackupStore::dest_is_empty_directory(bool &is_empty) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(util.is_empty_directory(backup_dest_.get_root_path(), get_storage_info(), is_empty))) {
    LOG_WARN("fail to init store", K(ret), K_(backup_dest));
  }
  return ret;
}

int ObBackupStore::read_format_file(ObBackupFormatDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(get_format_file_path(full_path))) {
    LOG_WARN("failed to get format file path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupStore::write_format_file(const ObBackupFormatDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(get_format_file_path(full_path))) {
    LOG_WARN("failed to get format file path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupStore::write_check_file(const ObBackupPathString &full_path, const ObBackupCheckDesc &desc) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret; 
}

int ObBackupStore::read_check_file(const ObBackupPathString &full_path, ObBackupCheckDesc &desc) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret; 
}

int ObBackupStore::write_rw_consistency_check_file(const ObBackupPathString &full_path, const ObBackupConsistencyCheckDesc &desc) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupStore::read_rw_consistency_check_file(const ObBackupPathString &full_path, ObBackupConsistencyCheckDesc &desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupStore not init", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupStore::write_single_file(const ObBackupPathString &full_path, const ObIBackupSerializeProvider &serializer) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  // wrapper with common header.
  ObBackupSerializeHeaderWrapper serializer_wrapper(&(const_cast<ObIBackupSerializeProvider &>(serializer)));
  const ObBackupStorageInfo *storage_info = get_storage_info();
  buf_size = serializer_wrapper.get_serialize_size();
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size), K(full_path), K(serializer));
  } else if (OB_FAIL(serializer_wrapper.serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize file.", K(ret), K(buf_size), K(full_path), K(serializer));
  } else if (pos != buf_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized size not match.", K(ret), K(pos), K(buf_size), K(full_path), K(serializer));
  } else if (OB_FAIL(util.mk_parent_dir(full_path.str(), storage_info))) {
    LOG_WARN("failed to mk dir.", K(ret), K(full_path), K(serializer));
  // TODO(yangyi.yyy & zhaoyongheng.zyh) use valid dest_id for QoS, including ObBackupDataStore & ObArchiveStore
  } else if (OB_FAIL(util.write_single_file(full_path.str(), storage_info, buf, buf_size,
                                            ObStorageIdMod::get_default_backup_id_mod()))) {
    LOG_WARN("failed to write single file.", K(ret), K(full_path), K(serializer));
  } else {
    FLOG_INFO("succeed to write single file.", K(full_path), K(serializer));
  }

  return ret;
}

int ObBackupStore::read_single_file(const ObBackupPathString &full_path, ObIBackupSerializeProvider &serializer) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  // wrapper with common header.
  ObBackupSerializeHeaderWrapper serializer_wrapper(&serializer);
  const ObBackupStorageInfo *storage_info = get_storage_info();

  if (OB_FAIL(util.get_file_length(full_path.ptr(), storage_info, file_length))) {
    if (OB_OBJECT_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length.", K(ret), K(full_path));
    } else {
      LOG_INFO("file not exist.", K(ret), K(full_path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("file is empty.", K(ret), K(full_path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(full_path), K(file_length));
  // TODO(yangyi.yyy & zhaoyongheng.zyh) use valid dest_id for QoS, including ObBackupDataStore & ObArchiveStore
  } else if (OB_FAIL(util.read_single_file(full_path.str(), storage_info, buf, file_length, read_size,
                                           ObStorageIdMod::get_default_backup_id_mod()))) {
    LOG_WARN("failed to read file.", K(ret), K(full_path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match.", K(ret), K(full_path), K(file_length), K(read_size));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {
    LOG_WARN("failed to deserialize.", K(ret), K(full_path), K(file_length));
  } else {
    FLOG_INFO("succeed to read single file.", K(full_path), K(serializer));
  }

  return ret;
}

ObBackupDestMgr::ObBackupDestMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    max_iops_(),
    max_bandwidth_(),
    dest_type_(ObBackupDestType::TYPE::DEST_TYPE_MAX),
    backup_dest_(),
    sql_proxy_(NULL),
    is_remote_execute_(false)
{
} 

int ObBackupDestMgr::init(
    const uint64_t tenant_id,
    const ObBackupDestType::TYPE &dest_type,
    const share::ObBackupPathString &backup_dest_str,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupStore init twice.", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id && backup_dest_str.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest", K(ret), K(dest_type));
  } else if (OB_FAIL(backup_dest_.set(backup_dest_str))) {
    LOG_WARN("invalid backup dest", K(ret), K(dest_type));
  } else {
    tenant_id_ = tenant_id;
    max_iops_ = backup_dest_.get_storage_info()->max_iops_;
    max_bandwidth_ = backup_dest_.get_storage_info()->max_bandwidth_;
    dest_type_ = dest_type;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupDestMgr::init_for_rpc(
    const uint64_t tenant_id,
    const ObBackupDestType::TYPE &dest_type,
    const share::ObBackupPathString &backup_dest_str,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(tenant_id, dest_type, backup_dest_str, sql_proxy))) {
    LOG_WARN("fail to init", K(ret), K(tenant_id), K(dest_type));
  } else {
    is_remote_execute_ = true;
  }
  return ret;
}

int ObBackupDestMgr::check_dest_connectivity(obrpc::ObSrvRpcProxy &rpc_proxy)
{
  int ret = OB_SUCCESS;
  ObBackupConnectivityCheckManager check_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDestMgr not init", K(ret));
  } else if (OB_FAIL(check_mgr.init(tenant_id_, rpc_proxy, *sql_proxy_))) {
    LOG_WARN("fail to init connectivity check mgr", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_mgr.check_backup_dest_connectivity(backup_dest_))) {
    LOG_WARN("fail to check backup dest connectivity", K(ret), K_(tenant_id), K_(backup_dest));
  }
  return ret;
}

int ObBackupDestMgr::check_dest_validity(obrpc::ObSrvRpcProxy &rpc_proxy, const bool need_format_file)
{
  int ret = OB_SUCCESS;
  share::ObBackupStore store;
  int64_t dest_id = 0;
  bool is_empty = true;
  bool is_exist = false;
  bool need_remote_execute = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDestMgr not init", K(ret));
  } else if (OB_FAIL(remote_execute_if_need_(rpc_proxy,
                                             need_format_file,
                                             RemoteExecuteType::CHECK_DEST_VALIDITY,
                                             need_remote_execute))) {
    LOG_WARN("fail to remote execute if need", K(ret));
  } else if (need_remote_execute) { //do nothing
  } else if (OB_FAIL(store.init(backup_dest_))) {
    LOG_WARN("fail to init store", K(ret), K_(backup_dest));
  } else if (OB_FAIL(store.dest_is_empty_directory(is_empty))) {
    LOG_WARN("fail to check dest is empty dirctory", K(ret), K_(backup_dest));
  } else if (OB_FAIL(check_dest_connectivity(rpc_proxy))) {
    LOG_WARN("fail to check dest connectivity", K(ret), K_(backup_dest));
  } else if (!is_empty) {
    if (OB_FAIL(store.is_format_file_exist(is_exist))) {
      LOG_WARN("fail to check format file exist", K(ret), K_(backup_dest));
    } else if (!is_exist) {
      ret = OB_BACKUP_FORMAT_FILE_NOT_EXIST;
      LOG_WARN("format file does not exist", K(ret), K_(backup_dest));
      LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_EXIST, ", try to set a new directory.");
    } else {
      share::ObBackupFormatDesc format_desc;
      share::ObBackupFormatDesc dest_format;
      ObBackupDestType::TYPE dest_type;
      if (OB_FAIL(store.read_format_file(dest_format))) {
        LOG_WARN("fail to read format file", K(ret), K_(tenant_id), K_(backup_dest)); 
      } else if (dest_format.dest_type_ != dest_type_) {
        ret = OB_BACKUP_FORMAT_FILE_NOT_MATCH;
        LOG_WARN("dest_type not match", K(ret), K(dest_format), K(dest_type_));
        LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_MATCH, ", try to set a new directory.");
      } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id_, backup_dest_, dest_id))) {
        LOG_WARN("fail to get dest id", K(ret), K_(tenant_id), K(backup_dest_));
      } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_type(*sql_proxy_, tenant_id_, backup_dest_, dest_type))) {
        LOG_WARN("fail to get dest type", K(ret), K_(tenant_id), K(backup_dest_));
      } else if (dest_type != dest_type_) {
        ret = OB_BACKUP_FORMAT_FILE_NOT_MATCH;
        LOG_WARN("dest type is not match", K(ret), K(dest_type), K(dest_type_));
        LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_MATCH, ",try to set a new directory.");
      } else if (OB_FAIL(generate_format_desc_(dest_id, dest_type, format_desc))) {
        LOG_WARN("fail to generate format desc", K(ret), K(backup_dest_), K(dest_id));
      } else if (format_desc.dest_type_ != dest_type_) {
        ret = OB_BACKUP_FORMAT_FILE_NOT_MATCH;
        LOG_WARN("dest_type not match", K(ret), K(dest_format), K(dest_type_));
        LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_MATCH, ", try to set a new directory.");
      } else if (!(format_desc.is_format_equal(dest_format))) {
        ret = OB_BACKUP_FORMAT_FILE_NOT_MATCH;
        LOG_WARN("format file is not match", K(ret), K(format_desc), K(dest_format));
        LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_MATCH, ", try to set a new directory.");
      } 
    }
  } else {
    if (!need_format_file) {
      LOG_INFO("succ check dest validity", K_(backup_dest), K(is_empty)); 
    } else {
      ret = OB_BACKUP_FORMAT_FILE_NOT_EXIST;
      LOG_WARN("format file does not exist", K(ret), K_(backup_dest), K(is_empty));
      LOG_USER_ERROR(OB_BACKUP_FORMAT_FILE_NOT_EXIST, ", try to set a new directory.");
    }
  }
  return ret;
}

int ObBackupDestMgr::remote_execute_if_need_(obrpc::ObSrvRpcProxy &rpc_proxy,
                                             const bool need_format_file,
                                             const RemoteExecuteType type,
                                             bool &need_remote_execute)
{
  int ret = OB_SUCCESS;
  bool is_self_tenant_server = true;
  common::ObArray<ObAddr> server_list;
  if (!is_valid_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid remote execute type", K(ret), K(type));
  } else if (OB_FAIL(ObBackupUtils::get_tenant_backup_servers(tenant_id_,
                                                             server_list,
                                                             is_self_tenant_server))) {
    LOG_WARN("fail to get tenant alive servers", K(ret), K_(tenant_id));
  } else if (OB_FALSE_IT(need_remote_execute = !is_self_tenant_server)) {
  } else if (need_remote_execute) {  // then forward request to a tenant server
    if (is_remote_execute_) { // but reciever can not foward this request again
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("self not in tenant alive servers is unexpected, should not remote execute again",
               KR(ret),
               K(is_self_tenant_server),
               K(server_list));
    } else {
      LOG_INFO("self not in tenant alive servers, remote execute on meta_tenant's leader", K(type));
      obrpc::ObRemoteCheckBackupDestValidityArg args;
      char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
      common::ObAddr dest;
      if (OB_FAIL(backup_dest_.get_backup_dest_str(backup_dest_str, sizeof(backup_dest_str)))) {
        LOG_WARN("fail to get backup dest str", K(ret), K_(backup_dest));
      } else if (OB_FAIL(args.init(tenant_id_, dest_type_, ObString(backup_dest_str), need_format_file))) {
        LOG_WARN("fail to init ObRemoteCheckBackupDestValidityArg", K(ret));
      } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id, gen_meta_tenant_id(tenant_id_), ObLSID(ObLSID::SYS_LS_ID), dest))) {
        LOG_WARN("fail to get meta tenant leader addr", K(ret), K(tenant_id_));
      } else if (CHECK_DEST_VALIDITY == type) {
        if (OB_FAIL(rpc_proxy.to(dest).check_backup_dest_validity(args))) {
          LOG_WARN("fail to check dest validity on server", K(ret), K(dest), K(args));
        }
      } else if (WRITE_FORMAT_FILE == type) {
        if (OB_FAIL(rpc_proxy.to(dest).write_backup_dest_format_file(args))) {
          LOG_WARN("fail to send rpc to write format file", K(ret), K(dest), K(args));
        }
      }
    }
  }

  return ret;
}

int ObBackupDestMgr::generate_format_desc_(
    const int64_t dest_id,
    const ObBackupDestType::TYPE &dest_type,
    share::ObBackupFormatDesc &format_desc)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  share::ObBackupPathString root_path;
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
  } else if (OB_FAIL(backup_dest_.get_backup_path_str(format_desc.path_.ptr(), format_desc.path_.capacity()))) {
    LOG_WARN("failed to get backup path", K(ret), K(backup_dest_));
  } else {
    format_desc.tenant_id_ = tenant_id_;
    format_desc.incarnation_ = OB_START_INCARNATION;
    format_desc.dest_id_ = dest_id;
    format_desc.dest_type_ = dest_type;
    format_desc.cluster_id_ = GCONF.cluster_id;
  }

  return ret;
}

int ObBackupDestMgr::updata_backup_file_status_()
{
  int ret = OB_SUCCESS;
  int64_t old_dest_id = 0;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id_, backup_dest_, old_dest_id))) {
    LOG_WARN("fail to get old dest id", K(ret), K_(backup_dest));
  } else if (0 == old_dest_id) {
    // do nothing
  } else if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == dest_type_) {
    ObSArray<ObBackupSetFileDesc> backup_set_infos;
    if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_files_specified_dest(*sql_proxy_, tenant_id_, old_dest_id, backup_set_infos))) {
      LOG_WARN("fail to get backup set files", K(ret), K_(backup_dest), K(old_dest_id), K(tenant_id_));
    } else if (0 == backup_set_infos.count()) {
      // do nothing
    } else {
      for (int i = 0; OB_SUCC(ret) && i < backup_set_infos.count(); i++) {
        share::ObBackupSetFileDesc backup_set_info = backup_set_infos.at(i);
        backup_set_info.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETED;
        if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(*sql_proxy_, backup_set_info))) {
          LOG_WARN("fail to update backup set file", K(ret), K_(backup_dest));
        }
      }
    }
  } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == dest_type_) {
    ObArchivePersistHelper archive_table_op;
    ObSArray<ObTenantArchivePieceAttr> backup_piece_infos;
    if (OB_FAIL(archive_table_op.init(tenant_id_))) {
      LOG_WARN("failed to init archive table operator", K(ret));
    } else if (OB_FAIL(archive_table_op.get_pieces(*sql_proxy_, old_dest_id, backup_piece_infos))) {
      LOG_WARN("failed to get pieces", K(ret), K(old_dest_id));
    } else if (0 == backup_piece_infos.count()) {
      // do nothing
    } else {
      for (int i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); i++) {
        share::ObTenantArchivePieceAttr backup_piece_info = backup_piece_infos.at(i);
        ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETED;
        if (OB_FAIL(archive_table_op.mark_new_piece_file_status(*sql_proxy_, backup_piece_info.key_.dest_id_,
            backup_piece_info.key_.round_id_, backup_piece_info.key_.piece_id_, file_status))) {
          LOG_WARN("fail to updata backup piece file status", K(ret), K_(backup_dest));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dest", K(ret), K_(backup_dest), K(tenant_id_), K(dest_type_)); 
  }

  return ret;
}

int ObBackupDestMgr::write_format_file()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore store;
  share::ObBackupFormatDesc format_desc;
  bool is_exist = false;
  int64_t dest_id = 0;
  common::ObArray<ObAddr> server_list;
  bool need_remote_execute = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDestMgr not init", K(ret));
  } else if (OB_FAIL(remote_execute_if_need_(*GCTX.srv_rpc_proxy_,
                                              false /*unused*/,
                                              RemoteExecuteType::WRITE_FORMAT_FILE,
                                              need_remote_execute))) {
    LOG_WARN("fail to remote execute if need", K(ret));
  } else if (need_remote_execute) { // do nothing
  } else if (OB_FAIL(store.init(backup_dest_))) {
    LOG_WARN("fail to init store", K(ret), K_(backup_dest));
  } else if (OB_FAIL(store.is_format_file_exist(is_exist))) {
    LOG_WARN("fail to check format file exist", K(ret), K_(backup_dest));
  } else if (is_exist) {
    // do not recreate the format file
  } else if (OB_FAIL(updata_backup_file_status_())) {
    LOG_WARN("fail to update backup file status", K(ret), K_(tenant_id), K(backup_dest_)); 
  } else if (OB_FAIL(ObLSBackupInfoOperator::get_next_dest_id(*sql_proxy_, tenant_id_, dest_id))) {
    LOG_WARN("fail to get dest id", K(ret), K_(tenant_id), K(backup_dest_));
  } else if (OB_FAIL(generate_format_desc_(dest_id, dest_type_, format_desc))) {
    LOG_WARN("fail to generate format desc", K(ret), K(dest_id));
  } else if (OB_FAIL(store.write_format_file(format_desc))) {
    LOG_WARN("fail to write format file", K(ret), K(format_desc));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::insert_backup_storage_info(
      *sql_proxy_, tenant_id_, backup_dest_, dest_type_, dest_id, max_iops_, max_bandwidth_))) {
    LOG_WARN("fail to insert backup storage info", K(ret), K(backup_dest_)); 
  }
  return ret;
}

void ObBackupDestMgr::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_type_ = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  backup_dest_.reset();
  sql_proxy_ = NULL;
}