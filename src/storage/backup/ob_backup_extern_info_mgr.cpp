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

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "common/ob_smart_var.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_data_store.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObBackupLSMetaInfo */

OB_SERIALIZE_MEMBER(ObBackupLSMetaInfo, ls_meta_package_);

ObBackupLSMetaInfo::ObBackupLSMetaInfo() : ls_meta_package_()
{}

ObBackupLSMetaInfo::~ObBackupLSMetaInfo()
{}

bool ObBackupLSMetaInfo::is_valid() const
{
  return ls_meta_package_.is_valid();
}

int64_t ObBackupLSMetaInfo::get_total_serialize_buf_size() const
{
  return sizeof(ObBackupCommonHeader) + get_serialize_size();
}

int ObBackupLSMetaInfo::serialize_to(char *buf, int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t required_size = get_total_serialize_buf_size();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret), KP(buf));
  } else if (buf_size - pos < required_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not enough", K(ret), K(buf_size), K(pos), K(required_size));
  } else {
    // serialize common header.
    ObBackupCommonHeader *common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = 0;
    common_header->data_type_ = 0;
    common_header->data_version_ = 0;
    common_header->header_length_ = sizeof(ObBackupCommonHeader);
    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    // serialize self.
    if (OB_FAIL(serialize(buf, buf_size, pos))) {
      LOG_WARN("failed to serialize", K(ret), K(*this));
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

int ObBackupLSMetaInfo::deserialize_from(char *buf, int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size is too small", K(ret), K(buf_size), K(sizeof(ObBackupCommonHeader)));
  } else {
    ObBackupCommonHeader *common_header = reinterpret_cast<ObBackupCommonHeader *>(buf);
    int64_t pos = common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_size - pos) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf size is too small", K(ret), K(buf_size), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_WARN("failed to check round start desc checksum", K(ret), K(*common_header));
    } else if (OB_FAIL(deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize round start desc", K(ret), K(*common_header));
    }
  }
  return ret;
}

/* ObExternLSMetaMgr */

ObExternLSMetaMgr::ObExternLSMetaMgr()
    : is_inited_(false),
      backup_dest_(),
      backup_set_desc_(),
      ls_id_(),
      turn_id_(),
      retry_id_()
{}

ObExternLSMetaMgr::~ObExternLSMetaMgr()
{}

int ObExternLSMetaMgr::init(const ObBackupDest &backup_dest, const ObBackupSetDesc &backup_set_desc,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() ||
             !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(ls_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    is_inited_ = true;
  }
  return ret;
}

int ObExternLSMetaMgr::write_ls_meta_info(const ObBackupLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_size = ls_meta.get_total_serialize_buf_size();
  ObArenaAllocator allocator;
  ObBackupPath path;
  ObBackupIoAdapter util;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size), K(ls_meta));
  } else if (OB_FAIL(ls_meta.serialize_to(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize ls meta info.", K(ret), K(buf_size), K(ls_meta));
  } else if (pos != buf_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized size not match.", K(ret), K(pos), K(buf_size), K(ls_meta));
  } else if (OB_FAIL(get_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get ls meta backup path", K(ret), K(ls_meta));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(path));
  } else if (OB_FAIL(util.write_single_file(path.get_obstr(), backup_dest_.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write tenant tablet logstream info.", K(ret), K(path), K(ls_meta));
  } else {
    LOG_INFO("succeed to write ls meta info", K(path), K(ls_meta));
  }
  return ret;
}

int ObExternLSMetaMgr::read_ls_meta_info(ObBackupLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObBackupPath path;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup extern mgr not init.", K(ret));
  } else if (OB_FAIL(get_ls_meta_backup_path_(path))) {
    LOG_WARN("failed to get backup ls meta path", K(ret));
  } else if (OB_FAIL(util.get_file_length(path.get_obstr(), backup_dest_.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get ls meta backup path file length.", K(ret), K(path));
    } else {
      LOG_INFO("tablet to ls info file not exist.", K(ret), K(path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("tablet to ls info file is empty.", K(ret), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(path), K(file_length));
  } else if (OB_FAIL(util.read_single_file(path.get_obstr(), backup_dest_.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read tablet to ls info file.", K(ret), K(path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match.", K(ret), K(path), K(file_length), K(read_size));
  } else if (OB_FAIL(ls_meta.deserialize_from(buf, file_length))) {
    LOG_WARN("failed to deserialize tablet to ls info.", K(ret), K(path), K(file_length));
  } else {
    LOG_INFO("succeed to read ls meta info", K(path), K(ls_meta));
  }
  return ret;
}

int ObExternLSMetaMgr::get_ls_meta_backup_path_(ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::get_ls_meta_info_backup_path(backup_dest_, backup_set_desc_, ls_id_, turn_id_, 
      retry_id_, path))) {
    LOG_WARN("failed to get ls meta info backup path", K(ret), K_(backup_dest), K_(backup_set_desc), K_(ls_id), 
        K_(turn_id), K_(retry_id));
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
