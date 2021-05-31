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
#include "ob_extern_backup_info_mgr.h"
#include "common/ob_record_header.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase;
using namespace common;
using namespace share;

OB_SERIALIZE_MEMBER(ObExternBackupInfos, extern_backup_info_array_);
ObExternBackupInfos::ObExternBackupInfos() : extern_backup_info_array_(), is_modified_(false)
{}

ObExternBackupInfos::~ObExternBackupInfos()
{}

void ObExternBackupInfos::reset()
{
  extern_backup_info_array_.reset();
  is_modified_ = false;
}

bool ObExternBackupInfos::is_valid() const
{
  bool bool_ret = true;
  const int64_t count = extern_backup_info_array_.count();
  for (int64_t i = 0; bool_ret && i < count; ++i) {
    bool_ret = extern_backup_info_array_.at(i).is_valid();
  }
  return bool_ret;
}

int64_t ObExternBackupInfos::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternBackupInfos::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_INFO;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObExternBackupInfos::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check backup info", K(ret), K(*common_header));
    } else if (ObBackupFileType::BACKUP_INFO != common_header->data_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup info data type is unexpected", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize backup info", K(ret), K(*common_header));
    }
  }
  return ret;
}

// in order to reuse, can not refer to last extern backup info' status is DOING
int ObExternBackupInfos::update(const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfo last_backup_info;
  if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(extern_backup_info));
  } else if (OB_FAIL(get_last(last_backup_info))) {
    LOG_WARN("failed to get last extern backup info", K(ret), K(extern_backup_info_array_));
  } else if (extern_backup_info.is_equal(last_backup_info)) {
    // do nothing
  } else if (!extern_backup_info.is_equal_without_status(last_backup_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup info is invalid", K(ret), K(last_backup_info), K(extern_backup_info));
  } else {
    extern_backup_info_array_.at(extern_backup_info_array_.count() - 1) = extern_backup_info;
    is_modified_ = true;
  }
  return ret;
}

int ObExternBackupInfos::get_last(ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  extern_backup_info.reset();

  if (extern_backup_info_array_.empty()) {
    ret = OB_BACKUP_INFO_NOT_EXIST;
  } else {
    extern_backup_info = extern_backup_info_array_.at(extern_backup_info_array_.count() - 1);
  }
  return ret;
}

// TODO() change error code
int ObExternBackupInfos::get_last_succeed_info(ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  extern_backup_info.reset();
  if (extern_backup_info_array_.empty()) {
    ret = OB_BACKUP_INFO_NOT_EXIST;
  } else {
    bool found = false;
    for (int64_t i = extern_backup_info_array_.count() - 1; !found && i >= 0; --i) {
      const ObExternBackupInfo& info = extern_backup_info_array_.at(i);
      if (ObExternBackupInfo::SUCCESS == info.status_) {
        extern_backup_info = info;
        found = true;
      }
    }

    if (!found) {
      ret = OB_BACKUP_INFO_NOT_EXIST;
    }
  }
  return ret;
}

int ObExternBackupInfos::get_extern_backup_infos(ObIArray<ObExternBackupInfo>& extern_backup_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extern_backup_infos.assign(extern_backup_info_array_))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(extern_backup_info_array_));
  }
  return ret;
}

int ObExternBackupInfos::get_extern_full_backup_infos(ObIArray<ObExternBackupInfo>& extern_backup_infos)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_info_array_.count(); ++i) {
    const ObExternBackupInfo& extern_backup_info = extern_backup_info_array_.at(i);
    if (extern_backup_info.full_backup_set_id_ == extern_backup_info.inc_backup_set_id_) {
      if (OB_FAIL(extern_backup_infos.push_back(extern_backup_info))) {
        LOG_WARN("failed to push extern backup info into array", K(ret), K(extern_backup_info));
      }
    }
  }
  return ret;
}

int ObExternBackupInfos::add(const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfo last_backup_info;

  if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(extern_backup_info));
  } else if (OB_FAIL(get_last(last_backup_info))) {
    if (OB_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get last backup info", K(ret), K(extern_backup_info));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (extern_backup_info.is_equal_without_status(last_backup_info)) {
    LOG_INFO("extern backup info has already in extern backup infos, no need add",
        K(extern_backup_info),
        K(last_backup_info));
  } else if (OB_FAIL(extern_backup_info_array_.push_back(extern_backup_info))) {
    LOG_WARN("failed to push extern backup info into array", K(ret), K(extern_backup_info));
  } else {
    is_modified_ = true;
  }
  return ret;
}

int ObExternBackupInfos::find_backup_info(
    const int64_t restore_snapshot_version, const char* passwd_array, ObExternBackupInfo& backup_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;

  if (OB_ISNULL(passwd_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(passwd_array));
  }
  for (int64_t i = extern_backup_info_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObExternBackupInfo& tmp_backup_info = extern_backup_info_array_.at(i);
    if (ObExternBackupInfo::SUCCESS != tmp_backup_info.status_) {
      // do nothing
    } else if (tmp_backup_info.backup_snapshot_version_ <= restore_snapshot_version) {
      backup_info = tmp_backup_info;
      idx = i;
      break;
    }
  }

  if (OB_SUCC(ret) && idx < 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to find backup info", K(ret), K(extern_backup_info_array_), K(restore_snapshot_version));
  }

  for (int64_t i = idx; OB_SUCC(ret) && i >= 0; --i) {
    const ObExternBackupInfo& tmp_backup_info = extern_backup_info_array_.at(i);
    if (ObExternBackupInfo::SUCCESS != tmp_backup_info.status_) {
      // do nothing
    } else if (OB_FAIL(check_passwd(passwd_array, tmp_backup_info.passwd_.ptr()))) {
      LOG_WARN("failed to check passwd", K(ret), K(passwd_array), K(tmp_backup_info));
    } else if (ObBackupType::is_full_backup(tmp_backup_info.backup_type_)) {
      break;
    }
  }
  return ret;
}

int ObExternBackupInfos::check_passwd(const char* passwd_array, const char* passwd)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(passwd_array) || OB_ISNULL(passwd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(passwd_array), KP(passwd));
  } else if (STRLEN(passwd) == 0) {
    // empty password no need check
  } else if (OB_ISNULL(STRSTR(passwd_array, passwd))) {
    ret = OB_BACKUP_INVALID_PASSWORD;
    LOG_WARN("no valid passwd found", K(ret), K(passwd_array), K(passwd));
  }
  return ret;
}

int ObExternBackupInfos::mark_backup_info_deleted(const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_info_array_.count(); ++i) {
    ObExternBackupInfo& extern_backup_info = extern_backup_info_array_.at(i);
    if (backup_set_id == extern_backup_info.full_backup_set_id_) {
      extern_backup_info.is_mark_deleted_ = true;
      is_modified_ = true;
    }
  }
  return ret;
}

int ObExternBackupInfos::delete_marked_backup_info()
{
  int ret = OB_SUCCESS;
  for (int64_t i = extern_backup_info_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObExternBackupInfo& extern_backup_info = extern_backup_info_array_.at(i);
    if (extern_backup_info.is_mark_deleted_) {
      if (OB_FAIL(extern_backup_info_array_.remove(i))) {
        LOG_WARN("failed to remove extern backup info", K(ret), K(extern_backup_info));
      } else {
        is_modified_ = true;
        LOG_INFO("extern backup info is deleted", K(extern_backup_info), K(extern_backup_info_array_));
      }
    }
  }
  return ret;
}

int ObExternBackupInfos::get_extern_full_backup_info(
    const int64_t full_backup_set_id, ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_info_array_.count() && !found; ++i) {
    const ObExternBackupInfo& tmp_extern_backup_info = extern_backup_info_array_.at(i);
    if (full_backup_set_id == tmp_extern_backup_info.full_backup_set_id_ &&
        full_backup_set_id == tmp_extern_backup_info.inc_backup_set_id_) {
      extern_backup_info = tmp_extern_backup_info;
      found = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (!found) {
      ret = OB_BACKUP_INFO_NOT_EXIST;
      LOG_WARN("extern backup info not exist", K(ret), K(full_backup_set_id), K(extern_backup_info_array_));
    }
  }
  return ret;
}

int ObExternBackupInfos::try_finish_extern_backup_info(const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try finish extern backup info get invalid argument", K(ret), K(backup_set_id));
  } else {
    for (int64_t i = extern_backup_info_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObExternBackupInfo& extern_info = extern_backup_info_array_.at(i);
      if (backup_set_id > extern_info.inc_backup_set_id_) {
        break;
      } else if (backup_set_id == extern_info.inc_backup_set_id_) {
        if (ObExternBackupInfo::DOING == extern_info.status_) {
          extern_info.status_ = ObExternBackupInfo::FAILED;
          LOG_INFO("extern backup info has set failed", K(extern_info));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

ObExternBackupInfoMgr::ObExternBackupInfoMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      backup_dest_(),
      extern_backup_infos_(),
      last_succeed_info_(),
      backup_lease_service_(NULL)
{}

ObExternBackupInfoMgr::~ObExternBackupInfoMgr()
{}

int ObExternBackupInfoMgr::init(const uint64_t tenant_id, const ObClusterBackupDest& backup_dest,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup info mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern backup info mgr get invalid argument", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(get_backup_path(tenant_id, backup_dest, path))) {
    LOG_WARN("failed to get backup path", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(lock_.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock_.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(get_extern_backup_infos(tenant_id, backup_dest))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(get_last_succeed_info())) {
    LOG_WARN("failed to get last succeed info", K(ret), K(tenant_id), K(backup_dest));
  } else {
    tenant_id_ = tenant_id;
    backup_dest_ = backup_dest;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternBackupInfoMgr::get_extern_backup_infos(const uint64_t tenant_id, const ObClusterBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(get_backup_path(tenant_id, backup_dest, path))) {
    LOG_WARN("failed to get backup path", K(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern backup info not exist", K(ret), K(tenant_id), K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern backup info is empty", K(ret), K(tenant_id), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_backup_infos_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_backup_info", K(path), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::get_last_succeed_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extern_backup_infos_.get_last_succeed_info(last_succeed_info_))) {
    if (OB_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get last succeed info", K(ret), K(extern_backup_infos_));
    }
  }
  if (OB_BACKUP_INFO_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExternBackupInfoMgr::get_last_info(ObExternBackupInfo& last_backup_info)
{
  int ret = OB_SUCCESS;
  last_backup_info.reset();
  if (OB_FAIL(extern_backup_infos_.get_last(last_backup_info))) {
    if (OB_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get last info", K(ret), K(extern_backup_infos_));
    }
  }
  if (OB_BACKUP_INFO_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExternBackupInfoMgr::check_can_backup(const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfo last_backup_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check has already backup get invalid argument", K(ret));
  } else if (OB_FAIL(get_last_info(last_backup_info))) {
    LOG_WARN("failed to get last info", K(ret));
  } else {
    if (ObExternBackupInfo::DOING != extern_backup_info.status_) {
      if (!last_backup_info.is_valid()) {
        ret = OB_BACKUP_INFO_NOT_EXIST;
        LOG_WARN(
            "backup is finished, but last extern info not exist", K(ret), K(extern_backup_info), K(last_backup_info));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!last_backup_info.is_valid()) {
      // do nothing
    } else if (last_backup_info.inc_backup_set_id_ == extern_backup_info.inc_backup_set_id_) {
      if (last_backup_info.is_equal(extern_backup_info)) {
        // do nothing
      } else if (ObExternBackupInfo::DOING != last_backup_info.status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last info inc backup set id is equal to extern backup info but status is not doing",
            K(ret),
            K(last_backup_info),
            K(extern_backup_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (!last_succeed_info_.is_valid()) {
        // do nothing
      } else if (last_succeed_info_.inc_backup_set_id_ == extern_backup_info.inc_backup_set_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last succeed info inc backup set id is equal to extern backup info",
            K(ret),
            K(last_succeed_info_),
            K(extern_backup_info));
      }
    }
  }

  return ret;
}

int ObExternBackupInfoMgr::upload_backup_info(const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (!extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check has already backup get invalid argument", K(ret));
  } else if (ObExternBackupInfo::DOING == extern_backup_info.status_ &&
             OB_FAIL(extern_backup_infos_.add(extern_backup_info))) {  // first time add backup infos
    LOG_WARN("failed to add extern backup info", K(ret), K(extern_backup_info));
  } else if (ObExternBackupInfo::DOING != extern_backup_info.status_ &&
             OB_FAIL(extern_backup_infos_.update(extern_backup_info))) {  // not first must update backup infos
    LOG_WARN("failed to add extern backup info", K(ret), K(extern_backup_info));
  } else if (OB_FAIL(upload_backup_info())) {
    LOG_WARN("failed to upload backup info", K(ret), K(extern_backup_info));
  }

  return ret;
}

int ObExternBackupInfoMgr::get_backup_path(
    const uint64_t tenant_id, const ObClusterBackupDest& backup_dest, ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(ObBackupPathUtil::get_cluster_data_backup_info_path(backup_dest, path))) {
      LOG_WARN("failed to get cluster data backup info path", K(ret), K(backup_dest));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_data_backup_info_path(backup_dest, tenant_id, path))) {
      LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
    }
  }
  return ret;
}

int ObExternBackupInfoMgr::get_extern_backup_info(const ObBaseBackupInfoStruct& info,
    rootserver::ObFreezeInfoManager& freeze_info_mgr, ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  extern_backup_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (!info.is_valid() ||
             (!info.backup_status_.is_scheduler_status() && !info.backup_status_.is_cleanup_status() &&
                 !info.backup_status_.is_doing_status() && !info.backup_status_.is_cancel_status())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get extern backup info get invalid argument", K(ret), K(info));
  } else {
    if (info.backup_status_.is_cleanup_status() || info.backup_status_.is_doing_status()) {
      ObExternBackupInfo last_backup_info;
      if (OB_FAIL(extern_backup_infos_.get_last(last_backup_info))) {
        LOG_WARN("failed to get last backup info", K(ret), K(info));
      } else {
        extern_backup_info = last_backup_info;
      }
    } else {
      // info.status_.is_scheduler_status
      ObArray<TenantIdAndSchemaVersion> schema_versions;
      share::ObSimpleFrozenStatus frozen_status;
      const int64_t prev_backup_set_id = info.backup_set_id_ - 1;
      if (prev_backup_set_id > 0 && OB_FAIL(try_finish_extern_backup_info(prev_backup_set_id))) {
        LOG_WARN("failed to try finish extern backup info", K(ret), K(prev_backup_set_id));
      } else if (OB_FAIL(freeze_info_mgr.get_freeze_info(info.backup_data_version_, frozen_status))) {
        LOG_WARN("failed to get freeze info", K(ret), K(info), K(frozen_status));
      } else if (OB_FAIL(freeze_info_mgr.get_freeze_schema_versions(
                     info.tenant_id_, info.backup_data_version_, schema_versions))) {
        LOG_WARN("failed to get freeze schema versions", K(ret), K(info));
      } else if (schema_versions.empty()) {
        TenantIdAndSchemaVersion tenant_id_schema;
        tenant_id_schema.tenant_id_ = info.tenant_id_;
        tenant_id_schema.schema_version_ = 0;
        if (OB_FAIL(schema_versions.push_back(tenant_id_schema))) {
          LOG_WARN("failed to push backup tenant id schema", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (1 != schema_versions.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get schema versions", K(ret), K(info), K(schema_versions));
      } else {
        extern_backup_info.inc_backup_set_id_ = info.backup_set_id_;
        extern_backup_info.backup_data_version_ = info.backup_data_version_;
        extern_backup_info.backup_snapshot_version_ = info.backup_snapshot_version_;
        extern_backup_info.backup_schema_version_ = info.backup_schema_version_;
        extern_backup_info.frozen_snapshot_version_ = frozen_status.frozen_timestamp_;
        extern_backup_info.frozen_schema_version_ = schema_versions.at(0).schema_version_;
        extern_backup_info.status_ = ObExternBackupInfo::DOING;
        extern_backup_info.compatible_ = OB_BACKUP_COMPATIBLE_VERSION;
        extern_backup_info.cluster_version_ = ObClusterVersion::get_instance().get_cluster_version();
        extern_backup_info.encryption_mode_ = info.encryption_mode_;
        extern_backup_info.passwd_ = info.passwd_;
        if (!last_succeed_info_.is_valid()) {
          extern_backup_info.full_backup_set_id_ = info.backup_set_id_;
          extern_backup_info.prev_full_backup_set_id_ = 0;
          extern_backup_info.prev_inc_backup_set_id_ = 0;
          extern_backup_info.prev_backup_data_version_ = 0;
          extern_backup_info.backup_type_ = ObBackupType::FULL_BACKUP;
        } else {
          const int64_t full_backup_set_id =
              info.backup_type_.is_full_backup() ? info.backup_set_id_ : last_succeed_info_.full_backup_set_id_;
          extern_backup_info.full_backup_set_id_ = full_backup_set_id;
          extern_backup_info.prev_full_backup_set_id_ =
              info.backup_type_.is_full_backup() ? 0 : last_succeed_info_.full_backup_set_id_;
          extern_backup_info.prev_inc_backup_set_id_ =
              info.backup_type_.is_full_backup() ? 0 : last_succeed_info_.inc_backup_set_id_;
          extern_backup_info.prev_backup_data_version_ =
              info.backup_type_.is_full_backup() ? 0 : last_succeed_info_.backup_data_version_;
          extern_backup_info.backup_type_ = info.backup_type_.type_;
        }
      }
    }
  }
  return ret;
}

int ObExternBackupInfoMgr::find_backup_info(
    const int64_t restore_snapshot_version, const char* passwd_array, ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (restore_snapshot_version <= 0 || OB_ISNULL(passwd_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("find backup info get invalid argument", K(ret), K(restore_snapshot_version));
  } else if (OB_FAIL(
                 extern_backup_infos_.find_backup_info(restore_snapshot_version, passwd_array, extern_backup_info))) {
    LOG_WARN("failed to find backup info", K(ret), K(restore_snapshot_version));
  }
  return ret;
}

int ObExternBackupInfoMgr::get_extern_backup_infos(ObIArray<ObExternBackupInfo>& extern_backup_infos)
{
  int ret = OB_SUCCESS;
  extern_backup_infos.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_infos_.get_extern_backup_infos(extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::get_extern_full_backup_infos(ObIArray<ObExternBackupInfo>& extern_backup_infos)
{
  int ret = OB_SUCCESS;
  extern_backup_infos.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_infos_.get_extern_full_backup_infos(extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::upload_backup_info()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need_retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(get_backup_path(tenant_id_, backup_dest_, path))) {
    LOG_WARN("failed to get backup path", K(ret), K(tenant_id_), K(backup_dest_));
  } else if (FALSE_IT(buf_size = extern_backup_infos_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_backup_infos_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(extern_backup_infos_));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(extern_backup_infos_));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_backup_info", K(path), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::mark_backup_info_deleted(const ObIArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      const int64_t backup_set_id = backup_set_ids.at(i);
      if (OB_FAIL(extern_backup_infos_.mark_backup_info_deleted(backup_set_id))) {
        LOG_WARN("failed to mark backup info deleted", K(ret), K(backup_set_id));
      }
    }
  }
  return ret;
}

int ObExternBackupInfoMgr::delete_marked_backup_info()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_infos_.delete_marked_backup_info())) {
    LOG_WARN("failed to delete marked backup info", K(ret), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::get_extern_full_backup_info(
    const int64_t full_backup_set_id, ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  extern_backup_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_infos_.get_extern_full_backup_info(full_backup_set_id, extern_backup_info))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::try_finish_extern_backup_info(const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try finish extern backup info get invalid argument", K(ret), K(backup_set_id));
  } else if (OB_FAIL(extern_backup_infos_.try_finish_extern_backup_info(backup_set_id))) {
    LOG_WARN("failed to try finish extern backup info", K(ret), K(backup_set_id), K(extern_backup_infos_));
  }
  return ret;
}

int ObExternBackupInfoMgr::get_lastest_incremental_backup_count(int32_t& incremental_backup_count)
{
  int ret = OB_SUCCESS;
  incremental_backup_count = 0;
  ObArray<ObExternBackupInfo> extern_backup_infos;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_infos_.get_extern_backup_infos(extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", K(ret));
  } else {
    for (int64_t i = extern_backup_infos.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObExternBackupInfo& extern_backup_info = extern_backup_infos.at(i);
      if (ObBackupType::is_full_backup(extern_backup_info.backup_type_)) {
        break;
      } else {
        ++incremental_backup_count;
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExternBackupSetInfos, extern_backup_set_info_array_);
ObExternBackupSetInfos::ObExternBackupSetInfos() : extern_backup_set_info_array_()
{}

ObExternBackupSetInfos::~ObExternBackupSetInfos()
{}

void ObExternBackupSetInfos::reset()
{
  extern_backup_set_info_array_.reset();
}

bool ObExternBackupSetInfos::is_valid() const
{
  bool bool_ret = true;
  const int64_t count = extern_backup_set_info_array_.count();
  for (int64_t i = 0; bool_ret && i < count; ++i) {
    bool_ret = extern_backup_set_info_array_.at(i).is_valid();
  }
  return bool_ret;
}

int64_t ObExternBackupSetInfos::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternBackupSetInfos::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_SET_INFO;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObExternBackupSetInfos::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check backup set info", K(ret), K(*common_header));
    } else if (ObBackupFileType::BACKUP_SET_INFO != common_header->data_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup set info data type is unexpected", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize backup set info", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternBackupSetInfos::add(const ObExternBackupSetInfo& extern_backup_set_info)
{
  int ret = OB_SUCCESS;
  if (!extern_backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(extern_backup_set_info));
  } else if (OB_FAIL(extern_backup_set_info_array_.push_back(extern_backup_set_info))) {
    LOG_WARN("failed to push extern backup set info into array", K(ret), K(extern_backup_set_info));
  }
  return ret;
}

int ObExternBackupSetInfos::get_extern_backup_set_infos(ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extern_backup_set_infos.assign(extern_backup_set_info_array_))) {
    LOG_WARN("failed to get extern backup set infos", K(ret), K(extern_backup_set_info_array_));
  }
  return ret;
}

ObExternBackupSetInfoMgr::ObExternBackupSetInfoMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      full_backup_set_id_(0),
      backup_dest_(),
      extern_backup_set_infos_(),
      backup_lease_service_(NULL)
{}

ObExternBackupSetInfoMgr::~ObExternBackupSetInfoMgr()
{}

int ObExternBackupSetInfoMgr::init(const uint64_t tenant_id, const int64_t full_backup_set_id,
    const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup set info mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid() || full_backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern backup set info mgr get invalid argument",
        K(ret),
        K(tenant_id),
        K(backup_dest),
        K(full_backup_set_id));
  } else if (OB_FAIL(
                 ObBackupPathUtil::get_tenant_backup_set_info_path(backup_dest, tenant_id, full_backup_set_id, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
  } else if (OB_FAIL(lock_.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock_.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(get_extern_backup_set_infos(tenant_id, full_backup_set_id, backup_dest))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(tenant_id), K(backup_dest));
  } else {
    tenant_id_ = tenant_id;
    backup_dest_ = backup_dest;
    full_backup_set_id_ = full_backup_set_id;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternBackupSetInfoMgr::get_extern_backup_set_infos(
    const uint64_t tenant_id, const int64_t full_backup_set_id, const ObClusterBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_info_path(backup_dest, tenant_id, full_backup_set_id, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern backup set info not exist", K(ret), K(tenant_id), K(full_backup_set_id), K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern backup info is empty", K(ret), K(tenant_id), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_backup_set_infos_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_backup_set_info", K(path), K(extern_backup_set_infos_));
  }
  return ret;
}

int ObExternBackupSetInfoMgr::upload_backup_set_info(const ObExternBackupSetInfo& extern_backup_set_info)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (!extern_backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check has already backup get invalid argument", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_info_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, path))) {
    LOG_WARN(
        "failed to get tenant data backup info path", K(ret), K(backup_dest_), K(tenant_id_), K(full_backup_set_id_));
  } else if (OB_FAIL(extern_backup_set_infos_.add(extern_backup_set_info))) {
    LOG_WARN("failed to push extern backup set info into array", K(ret), K(extern_backup_set_info));
  } else if (FALSE_IT(buf_size = extern_backup_set_infos_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_backup_set_infos_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(extern_backup_set_info));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(extern_backup_set_info));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_backup_set_info", K(path), K(extern_backup_set_infos_));
  }
  return ret;
}

int ObExternBackupSetInfoMgr::get_extern_backup_set_infos(ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup set info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_backup_set_infos_.get_extern_backup_set_infos(extern_backup_set_infos))) {
    LOG_WARN("failed to get extern backup set infos", K(ret), K(extern_backup_set_infos_));
  }
  return ret;
}

int ObExternBackupSetInfoMgr::touch_extern_backup_set_info()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObStorageUtil util(false /*need retry*/);
  bool is_exist = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup set info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_info_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, path))) {
    LOG_WARN(
        "failed to get tenant data backup info path", K(ret), K(backup_dest_), K(tenant_id_), K(full_backup_set_id_));
  } else if (OB_FAIL(util.is_exist(path.get_ptr(), backup_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("failed to check backup file is exist", K(ret), K(path), K(backup_dest_));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.update_file_modify_time(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to update file modify time", K(ret), K(path), K(backup_dest_));
  }
  return ret;
}

int ObExternBackupSetInfoMgr::delete_extern_backup_set_info()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObStorageUtil util(false /*need retry*/);
  bool is_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup set info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_info_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, path))) {
    LOG_WARN(
        "failed to get tenant data backup info path", K(ret), K(backup_dest_), K(tenant_id_), K(full_backup_set_id_));
  } else if (OB_FAIL(util.is_exist(path.get_ptr(), backup_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("failed to check backup file is exist", K(ret), K(path), K(backup_dest_));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.del_file(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to update file modify time", K(ret), K(path), K(backup_dest_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExternPGList, pkeys_);
ObExternPGList::ObExternPGList() : pkeys_()
{}

void ObExternPGList::reset()
{
  pkeys_.reset();
}

bool ObExternPGList::is_valid() const
{
  bool b_ret = true;
  for (int64_t i = 0; b_ret && i < pkeys_.count(); ++i) {
    b_ret = pkeys_.at(i).is_valid();
  }
  return b_ret;
}

int64_t ObExternPGList::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternPGList::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_PG_LIST;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObExternPGList::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check backup info", K(ret), K(*common_header));
    } else if (ObBackupFileType::BACKUP_PG_LIST != common_header->data_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup info data type is unexpected", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize backup info", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternPGList::add(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg key is invalid", K(ret), K(pg_key));
  } else if (OB_FAIL(pkeys_.push_back(pg_key))) {
    LOG_WARN("failed to push pg key into array", K(ret), K(pg_key));
  }
  return ret;
}

int ObExternPGList::get(ObIArray<ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_keys.assign(pkeys_))) {
    LOG_WARN("failed to assign pg keys", K(ret));
  }
  return ret;
}

ObExternPGListMgr::ObExternPGListMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      backup_dest_(),
      extern_sys_pg_list_(),
      extern_normal_pg_list_(),
      backup_lease_service_(NULL)
{}

ObExternPGListMgr::~ObExternPGListMgr()
{}

int ObExternPGListMgr::init(const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern pg list mgr init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid() || full_backup_set_id <= 0 ||
             inc_backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern pg list mgr get invalid argument",
        K(ret),
        K(tenant_id),
        K(backup_dest),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else {
    tenant_id_ = tenant_id;
    backup_dest_ = backup_dest;
    full_backup_set_id_ = full_backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternPGListMgr::add_pg_key(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern pg list mgr do not init ", K(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg key is invalid", K(ret), K(pg_key));
  } else {
    if (is_sys_table(pg_key.get_table_id())) {
      if (OB_FAIL(extern_sys_pg_list_.add(pg_key))) {
        LOG_WARN("failed to push pg key into sys pg list", K(ret), K(pg_key));
      }
    } else {
      if (OB_FAIL(extern_normal_pg_list_.add(pg_key))) {
        LOG_WARN("failed to push pg key into normal pg list", K(ret), K(pg_key));
      }
    }
  }
  return ret;
}

int ObExternPGListMgr::upload_pg_list()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extern pg list mgr do not init", K(ret));
  } else if (OB_FAIL(upload_sys_pg_list())) {
    LOG_WARN("failed to upload sys pg list", K(ret));
  } else if (OB_FAIL(upload_normal_pg_list())) {
    LOG_WARN("failed to upload normal pg list", K(ret));
  }
  return ret;
}

int ObExternPGListMgr::upload_sys_pg_list()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_sys_pg_list_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path",
        K(ret),
        K(backup_dest_),
        K(tenant_id_),
        K(full_backup_set_id_),
        K(inc_backup_set_id_));
  } else if (FALSE_IT(buf_size = extern_sys_pg_list_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_sys_pg_list_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_sys_pg_list", K(path));
  }
  return ret;
}

int ObExternPGListMgr::upload_normal_pg_list()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_normal_pg_list_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path",
        K(ret),
        K(backup_dest_),
        K(tenant_id_),
        K(full_backup_set_id_),
        K(inc_backup_set_id_));
  } else if (FALSE_IT(buf_size = extern_normal_pg_list_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_normal_pg_list_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_normal_pg_list", K(path));
  }
  return ret;
}

int ObExternPGListMgr::get_sys_pg_list(common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern pg list mgr do not init", K(ret));
  } else if (OB_FAIL(get_extern_sys_pg_list())) {
    LOG_WARN("failed to get extern sys pg list", K(ret));
  } else if (OB_FAIL(extern_sys_pg_list_.get(pg_keys))) {
    LOG_WARN("failed to get extern sys pg list", K(ret));
  }
  return ret;
}

int ObExternPGListMgr::get_normal_pg_list(common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern pg list mgr do not init", K(ret));
  } else if (OB_FAIL(get_extern_normal_pg_list())) {
    LOG_WARN("failed to get extern sys pg list", K(ret));
  } else if (OB_FAIL(extern_normal_pg_list_.get(pg_keys))) {
    LOG_WARN("failed to get extern normal pg list", K(ret));
  }
  return ret;
}

int ObExternPGListMgr::get_extern_sys_pg_list()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(ObBackupPathUtil::get_tenant_sys_pg_list_path(
          backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path",
        K(ret),
        K(backup_dest_),
        K(tenant_id_),
        K(full_backup_set_id_),
        K(inc_backup_set_id_));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest_.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest_));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern sys pg list not exist",
          K(ret),
          K(tenant_id_),
          K(full_backup_set_id_),
          K(inc_backup_set_id_),
          K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern sys pg list is empty", K(ret), K(tenant_id_), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_sys_pg_list_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_sys_pg_list", K(path));
  }
  return ret;
}

int ObExternPGListMgr::get_extern_normal_pg_list()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(ObBackupPathUtil::get_tenant_normal_pg_list_path(
          backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path",
        K(ret),
        K(backup_dest_),
        K(tenant_id_),
        K(full_backup_set_id_),
        K(inc_backup_set_id_));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest_.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest_));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern normal pg list not exist",
          K(ret),
          K(tenant_id_),
          K(full_backup_set_id_),
          K(inc_backup_set_id_),
          K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern sys pg list is empty", K(ret), K(tenant_id_), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_normal_pg_list_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_normal_pg_list", K(path));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExternTenantInfos, extern_tenant_info_array_);
ObExternTenantInfos::ObExternTenantInfos() : extern_tenant_info_array_(), is_modified_(false)
{}

ObExternTenantInfos::~ObExternTenantInfos()
{}

void ObExternTenantInfos::reset()
{
  extern_tenant_info_array_.reset();
  is_modified_ = false;
}

bool ObExternTenantInfos::is_valid() const
{
  bool bool_ret = true;
  const int64_t count = extern_tenant_info_array_.count();
  for (int64_t i = 0; bool_ret && i < count; ++i) {
    bool_ret = extern_tenant_info_array_.at(i).is_valid();
  }
  return bool_ret;
}

int64_t ObExternTenantInfos::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternTenantInfos::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_TENANT_INFO;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObExternTenantInfos::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check backup set info", K(ret), K(*common_header));
    } else if (ObBackupFileType::BACKUP_TENANT_INFO != common_header->data_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup tenant info data type is unexpected", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize backup tenant info data", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternTenantInfos::get_extern_tenant_infos(ObIArray<ObExternTenantInfo>& extern_tenant_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extern_tenant_infos.assign(extern_tenant_info_array_))) {
    LOG_WARN("failed to get extern tenant infos", K(ret), K(extern_tenant_info_array_));
  }
  return ret;
}

int ObExternTenantInfos::add(const ObExternTenantInfo& extern_tenant_info)
{
  int ret = OB_SUCCESS;
  if (!extern_tenant_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup info is invalid", K(ret), K(extern_tenant_info));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < extern_tenant_info_array_.count(); ++i) {
      ObExternTenantInfo& info = extern_tenant_info_array_.at(i);
      if (info.tenant_id_ == extern_tenant_info.tenant_id_) {
        found = true;
        if (info.tenant_name_ != extern_tenant_info.tenant_name_) {
          info.tenant_name_ = extern_tenant_info.tenant_name_;
        }
      }
    }
    if (!found) {
      if (OB_FAIL(extern_tenant_info_array_.push_back(extern_tenant_info))) {
        LOG_WARN("failed to push tenant info into array", K(ret), K(extern_tenant_info));
      } else {
        is_modified_ = true;
      }
    }
  }
  return ret;
}

int ObExternTenantInfos::find_tenant_info(const uint64_t tenant_id, ObExternTenantInfo& tenant_info)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (OB_INVALID_ID == tenant_id || 0 == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  }

  for (int64_t i = 0; !found && i < extern_tenant_info_array_.count(); ++i) {
    const ObExternTenantInfo& tmp_tenant_info = extern_tenant_info_array_.at(i);
    if (tmp_tenant_info.tenant_id_ == tenant_id) {
      tenant_info = tmp_tenant_info;
      found = true;
    }
  }

  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObExternTenantInfos::delete_tenant_info(const uint64_t tenant_id)
{
  // skip tenant id not exist
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || 0 == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else {
    for (int64_t i = extern_tenant_info_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObExternTenantInfo& tmp_tenant_info = extern_tenant_info_array_.at(i);
      if (tmp_tenant_info.tenant_id_ == tenant_id) {
        if (OB_FAIL(extern_tenant_info_array_.remove(i))) {
          LOG_WARN("failed to remove exter tenant info", K(ret), K(tmp_tenant_info), K(tenant_id));
        } else {
          is_modified_ = true;
          break;
        }
      }
    }
  }
  return ret;
}

ObExternTenantInfoMgr::ObExternTenantInfoMgr()
    : is_inited_(false), backup_dest_(), extern_tenant_infos_(), backup_lease_service_(NULL)
{}

ObExternTenantInfoMgr::~ObExternTenantInfoMgr()
{}

int ObExternTenantInfoMgr::init(
    const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern tenant info mgr init twice", K(ret));
  } else if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern tenant info mgr get invalid argument", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_info_path(backup_dest, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
  } else if (OB_FAIL(lock_.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock_.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(get_extern_tenant_infos(backup_dest))) {
    LOG_WARN("failed to get extern backup infos", K(ret), K(backup_dest));
  } else {
    backup_dest_ = backup_dest;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternTenantInfoMgr::get_extern_tenant_infos(const ObClusterBackupDest& backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (OB_FAIL(ObBackupPathUtil::get_tenant_info_path(backup_dest, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern backup info not exist", K(ret), K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern backup info is empty", K(ret), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_tenant_infos_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_tenant_info", K(path), K(extern_tenant_infos_));
  }
  return ret;
}

int ObExternTenantInfoMgr::add_tenant_info(const ObExternTenantInfo& tenant_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extern tenant info mgr do not init", K(ret));
  } else if (!tenant_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add tenant info get invalid argument", K(ret), K(tenant_info));
  } else if (OB_FAIL(extern_tenant_infos_.add(tenant_info))) {
    LOG_WARN("failed to add extern tenant info", K(ret), K(tenant_info));
  }
  return ret;
}

int ObExternTenantInfoMgr::upload_tenant_infos()
{
  int ret = OB_SUCCESS;

  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_info_path(backup_dest_, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest_));
  } else if (FALSE_IT(buf_size = extern_tenant_infos_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_tenant_infos_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(extern_tenant_infos_));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(extern_tenant_infos_));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_tenant_info", K(path), K(extern_tenant_infos_));
  }

  return ret;
}

int ObExternTenantInfoMgr::get_extern_tenant_infos(ObIArray<ObExternTenantInfo>& tenant_infos)
{
  int ret = OB_SUCCESS;
  tenant_infos.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern backup info mgr do not init", K(ret));
  } else if (OB_FAIL(extern_tenant_infos_.get_extern_tenant_infos(tenant_infos))) {
    LOG_WARN("failed to get extern tenant infos", K(ret), K(extern_tenant_infos_));
  }
  return ret;
}

int ObExternTenantInfoMgr::find_tenant_info(const uint64_t tenant_id, ObExternTenantInfo& tenant_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant info mgr do not init", K(ret));
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("find tenant info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(extern_tenant_infos_.find_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("failed to find tenant info", K(ret), K(tenant_id));
  }
  return ret;
}

int ObExternTenantInfoMgr::delete_tenant_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant info mgr do not init", K(ret));
  } else if (0 == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete tenant info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(extern_tenant_infos_.delete_tenant_info(tenant_id))) {
    LOG_WARN("failed to delete tenant info", K(ret), K(tenant_id));
  }
  return ret;
}

bool ObExternTenantInfoMgr::is_empty() const
{
  return extern_tenant_infos_.is_empty();
}

OB_SERIALIZE_MEMBER(ObExternTenantLocality, extern_tenant_locality_info_);
ObExternTenantLocality::ObExternTenantLocality() : extern_tenant_locality_info_()
{}

ObExternTenantLocality::~ObExternTenantLocality()
{}

void ObExternTenantLocality::reset()
{
  extern_tenant_locality_info_.reset();
}

bool ObExternTenantLocality::is_valid() const
{
  return extern_tenant_locality_info_.is_valid();
}

int64_t ObExternTenantLocality::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternTenantLocality::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_TENANT_LOCALITY_INFO;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObExternTenantLocality::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check backup set info", K(ret), K(*common_header));
    } else if (ObBackupFileType::BACKUP_TENANT_LOCALITY_INFO != common_header->data_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup tenant info data type is unexpected", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize backup tenant info data", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternTenantLocality::set_tenant_locality_info(const ObExternTenantLocalityInfo& tenant_locality_info)
{
  int ret = OB_SUCCESS;
  if (!tenant_locality_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tenant locality info get invalid argument", K(ret), K(tenant_locality_info));
  } else {
    extern_tenant_locality_info_ = tenant_locality_info;
  }
  return ret;
}

ObExternTenantLocalityInfoMgr::ObExternTenantLocalityInfoMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      backup_dest_(),
      extern_tenant_locality_(),
      backup_lease_service_(NULL)
{}

ObExternTenantLocalityInfoMgr::~ObExternTenantLocalityInfoMgr()
{}

int ObExternTenantLocalityInfoMgr::init(const uint64_t tenant_id, const int64_t full_backup_set_id,
    const int64_t inc_backup_set_id, const ObClusterBackupDest& backup_dest,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern tenant locality info mgr init twice", K(ret));
  } else if (!backup_dest.is_valid() || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 ||
             inc_backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern tenant locality info mgr get invalid argument",
        K(ret),
        K(backup_dest),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else {
    tenant_id_ = tenant_id;
    full_backup_set_id_ = full_backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    backup_dest_ = backup_dest;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternTenantLocalityInfoMgr::upload_tenant_locality_info(const ObExternTenantLocalityInfo& tenant_locality_info)
{
  int ret = OB_SUCCESS;

  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant locality info mgr do not init", K(ret));
  } else if (!tenant_locality_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add tenant info get invalid argument", K(ret), K(tenant_locality_info));
  } else if (OB_FAIL(extern_tenant_locality_.set_tenant_locality_info(tenant_locality_info))) {
    LOG_WARN("failed to set tenant locality info", K(ret), K(tenant_locality_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_locality_info_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest_));
  } else if (FALSE_IT(buf_size = extern_tenant_locality_.get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(extern_tenant_locality_.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(extern_tenant_locality_));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(extern_tenant_locality_));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_tenant_locality_info", K(path), K(extern_tenant_locality_));
  }

  return ret;
}

int ObExternTenantLocalityInfoMgr::get_extern_tenant_locality_info(ObExternTenantLocalityInfo& tenant_locality_info)
{
  int ret = OB_SUCCESS;
  tenant_locality_info.reset();

  ObBackupPath path;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant locality info mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_locality_info_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant locality info path", K(ret), K(backup_dest_));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), backup_dest_.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(backup_dest_));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern tenant locality info not exist", K(ret), K(path));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern tenant locality info is empty", K(ret), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(
                 util.read_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(extern_tenant_locality_.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to read_extern_tenant_locality_info", K(path), K(extern_tenant_locality_));
    tenant_locality_info = extern_tenant_locality_.get_tenant_locality_info();
  }
  return ret;
}

ObExternTenantBackupDiagnoseMgr::ObExternTenantBackupDiagnoseMgr()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      backup_dest_(),
      backup_lease_service_(NULL)
{}

ObExternTenantBackupDiagnoseMgr::~ObExternTenantBackupDiagnoseMgr()
{}

int64_t ObExternTenantBackupDiagnoseMgr::get_write_buf_size() const
{
  const int64_t EXTRA_SIZE = 64;
  int64_t size = sizeof(ObBackupCommonHeader);
  size += sizeof(ObExternBackupDiagnoseInfo);
  size += EXTRA_SIZE;  // for '\n'
  return size;
}

int ObExternTenantBackupDiagnoseMgr::write_buf(
    const ObExternBackupDiagnoseInfo& diagnose_info, char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;
  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_TENANT_DIAGNOSE_INFO;
    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n"))) {
      LOG_WARN("failed to set tenant id", K(ret), K(diagnose_info));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s\n", to_cstring(diagnose_info.tenant_id_)))) {
      LOG_WARN("failed to set tenant id", K(ret), K(diagnose_info));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s\n", to_cstring(diagnose_info.extern_backup_info_)))) {
      LOG_WARN("failed to set extern backup info path", K(ret), K(diagnose_info));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s\n", to_cstring(diagnose_info.backup_set_info_)))) {
      LOG_WARN("failed to set backup set info", K(ret), K(diagnose_info));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s\n", to_cstring(diagnose_info.tenant_locality_info_)))) {
      LOG_WARN("failed to set backup tenant info", K(ret), K(diagnose_info));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObExternTenantBackupDiagnoseMgr::init(const uint64_t tenant_id, const int64_t full_backup_set_id,
    const int64_t inc_backup_set_id, const ObClusterBackupDest& backup_dest,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern tenant backup diagnose mgr init twice", K(ret));
  } else if (!backup_dest.is_valid() || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 ||
             inc_backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init extern tenant diagnose mgr get invalid argument",
        K(ret),
        K(backup_dest),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else {
    tenant_id_ = tenant_id;
    full_backup_set_id_ = full_backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    backup_dest_ = backup_dest;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObExternTenantBackupDiagnoseMgr::upload_tenant_backup_diagnose_info(const ObExternBackupDiagnoseInfo& diagnose_info)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("extern tenant diagnose mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_diagnose_path(
                 backup_dest_, tenant_id_, full_backup_set_id_, inc_backup_set_id_, path))) {
    LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest_));
  } else if (FALSE_IT(buf_size = get_write_buf_size())) {
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(write_buf(diagnose_info, buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(diagnose_info));
  } else if (pos > buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(diagnose_info));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service_->check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), backup_dest_.get_storage_info(), buf, pos))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    // TODO(): modify log level later
    FLOG_INFO("succeed to write_extern_tenant_diagnose_info", K(path), K(diagnose_info));
  }

  return ret;
}
