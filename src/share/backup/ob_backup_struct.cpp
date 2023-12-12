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
#include "ob_backup_struct.h"
#include "common/ob_record_header.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "storage/transaction/ob_i_ts_source.h"

using namespace oceanbase;
using namespace lib;
using namespace common;
using namespace share;

int share::get_backup_backup_start_copy_id(const ObBackupBackupCopyIdLevel& copy_id_level, int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level) {
    copy_id = OB_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_USER_DEST == copy_id_level) {
    copy_id = OB_CLUSTER_USER_DEST_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_TENANT_GCONF_DEST == copy_id_level) {
    copy_id = OB_TENANT_GCONF_DEST_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_TENANT_USER_DEST == copy_id_level) {
    copy_id = OB_TENANT_USER_DEST_START_COPY_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int share::get_backup_copy_id_range_by_copy_level(
    const ObBackupBackupCopyIdLevel& copy_id_level, int64_t& left_copy_id, int64_t& right_copy_id)
{
  int ret = OB_SUCCESS;
  if (OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST == copy_id_level) {
    left_copy_id = OB_START_COPY_ID;
    right_copy_id = OB_CLUSTER_USER_DEST_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_CLUSTER_USER_DEST == copy_id_level) {
    left_copy_id = OB_CLUSTER_USER_DEST_START_COPY_ID;
    right_copy_id = OB_TENANT_GCONF_DEST_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_TENANT_GCONF_DEST == copy_id_level) {
    left_copy_id = OB_TENANT_GCONF_DEST_START_COPY_ID;
    right_copy_id = OB_TENANT_USER_DEST_START_COPY_ID;
  } else if (OB_BB_COPY_ID_LEVEL_TENANT_USER_DEST == copy_id_level) {
    left_copy_id = OB_TENANT_USER_DEST_START_COPY_ID;
    right_copy_id = OB_MAX_COPY_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

bool share::has_independ_inc_backup_set(const int64_t version)
{
  return version >= OB_BACKUP_COMPATIBLE_VERSION_V2 && version < OB_BACKUP_COMPATIBLE_VERSION_MAX;
}
bool share::is_valid_backup_inner_table_version(const ObBackupInnerTableVersion& version)
{
  return version > 0 && version < OB_BACKUP_INNER_TABLE_VMAX;
}

OB_SERIALIZE_MEMBER(ObSimpleBackupSetPath, backup_set_id_, copy_id_, backup_dest_, file_status_);
OB_SERIALIZE_MEMBER(ObSimpleBackupPiecePath, backup_piece_id_, copy_id_, backup_dest_, file_status_, round_id_);

ObSimpleBackupSetPath::ObSimpleBackupSetPath() : backup_set_id_(-1), copy_id_(0), backup_dest_(), file_status_()
{}

ObSimpleBackupSetPath::~ObSimpleBackupSetPath()
{}

void ObSimpleBackupSetPath::reset()
{
  backup_set_id_ = -1;
  copy_id_ = 0;
  backup_dest_.reset();
}

int ObSimpleBackupSetPath::set(const share::ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  ObBackupDest backup_dest;
  char tmp_buf[OB_MAX_BACKUP_DEST_LENGTH] = "";

  if (!path_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(path_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(path_info, backup_path))) {
    COMMON_LOG(WARN, "failed to get tenant data full backup set path", K(ret), K(path_info));
  } else if (OB_FAIL(backup_dest.set(backup_path.get_ptr(), path_info.dest_.get_storage_info()))) {
    COMMON_LOG(WARN, "failed to set backup dest", K(ret), K(backup_path));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(tmp_buf, OB_MAX_BACKUP_DEST_LENGTH))) {
    COMMON_LOG(WARN, "failed to get backup dest str", K(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.assign(tmp_buf))) {
    COMMON_LOG(WARN, "failed to assign str", K(ret), K(tmp_buf));
  } else {
    backup_set_id_ = path_info.inc_backup_set_id_;
  }
  return ret;
}

int ObSimpleBackupSetPath::set(const common::ObString& uri)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.assign(uri))) {
    COMMON_LOG(WARN, "failed to assign str", K(ret), K(uri));
  } else {
    backup_set_id_ = 1;  // fake
  }
  return ret;
}

int ObSimpleBackupSetPath::set(const common::ObString& path, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  char backup_path_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_FAIL(backup_dest.set(path.ptr(), storage_info.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(path), K(storage_info));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_path_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.assign(backup_path_str))) {
    LOG_WARN("failed to assign str", KR(ret));
  } else {
    backup_set_id_ = 1;  // fake
  }
  return ret;
}

bool ObSimpleBackupSetPath::is_valid() const
{
  return backup_set_id_ > 0 && copy_id_ >= 0 && !backup_dest_.is_empty();
}

common::ObString ObSimpleBackupSetPath::get_simple_path() const
{
  int64_t pos = 0;
  const char* str = backup_dest_.ptr();
  char* ptr = const_cast<char*>(str);
  while (ptr[pos] != '\0') {
    if (ptr[pos] == '?') {
      break;
    }
    ++pos;
  }
  return ObString(pos, str);
}

common::ObString ObSimpleBackupSetPath::get_storage_info() const
{
  ObString res;
  int64_t pos = 0;
  const char* str = backup_dest_.ptr();
  char* ptr = const_cast<char*>(str);
  while (ptr[pos] != '\0') {
    if (ptr[pos] == '?') {
      break;
    }
    ++pos;
  }
  if (ptr[pos] == '\0') {
    res = "";
  } else {
    ++ptr;
    res = ObString(strlen(ptr) - pos, ptr + pos);
  }
  return res;
}

ObSimpleBackupPiecePath::ObSimpleBackupPiecePath()
    : round_id_(-1), backup_piece_id_(-1), copy_id_(0), backup_dest_(), file_status_()
{}

ObSimpleBackupPiecePath::~ObSimpleBackupPiecePath()
{}

void ObSimpleBackupPiecePath::reset()
{
  round_id_ = -1;
  backup_piece_id_ = -1;
  copy_id_ = 0;
  backup_dest_.reset();
}

int ObSimpleBackupPiecePath::set(const common::ObString& uri)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_dest_.assign(uri))) {
    COMMON_LOG(WARN, "failed to assign str", K(ret), K(uri));
  } else {
    backup_piece_id_ = 1;  // fake
  }
  return ret;
}

int ObSimpleBackupPiecePath::set(const common::ObString& path, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  char backup_path_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_FAIL(backup_dest.set(path.ptr(), storage_info.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(path), K(storage_info));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_path_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(backup_dest_.assign(backup_path_str))) {
    LOG_WARN("failed to assign str", KR(ret));
  } else {
    backup_piece_id_ = 1;  // fake
  }
  return ret;
}

bool ObSimpleBackupPiecePath::is_valid() const
{
  return backup_piece_id_ >= 0 && copy_id_ >= 0 && !backup_dest_.is_empty();
}

common::ObString ObSimpleBackupPiecePath::get_simple_path() const
{
  int64_t pos = 0;
  const char* str = backup_dest_.ptr();
  char* ptr = const_cast<char*>(str);
  while (ptr[pos] != '\0') {
    if (ptr[pos] == '?') {
      break;
    }
    ++pos;
  }
  return ObString(pos, str);
}

common::ObString ObSimpleBackupPiecePath::get_storage_info() const
{
  ObString res;
  int64_t pos = 0;
  const char* str = backup_dest_.ptr();
  char* ptr = const_cast<char*>(str);
  while (ptr[pos] != '\0') {
    if (ptr[pos] == '?') {
      break;
    }
    ++pos;
  }
  if (ptr[pos] == '\0') {
    res = "";
  } else {
    ++ptr;
    res = ObString(strlen(ptr) - pos, ptr + pos);
  }
  return res;
}

bool ObCompareSimpleBackupSetPath::operator()(
    const share::ObSimpleBackupSetPath& lhs, const share::ObSimpleBackupSetPath& rhs)
{
  bool bret = false;
  if (lhs.backup_set_id_ != rhs.backup_set_id_) {
    bret = lhs.backup_set_id_ < rhs.backup_set_id_;
  } else {
    bret = lhs.copy_id_ < rhs.copy_id_;
  }
  return bret;
}

bool ObCompareSimpleBackupPiecePath::operator()(
    const share::ObSimpleBackupPiecePath& lhs, const share::ObSimpleBackupPiecePath& rhs)
{
  bool bret = false;
  if (lhs.backup_piece_id_ != rhs.backup_piece_id_) {
    bret = lhs.backup_piece_id_ < rhs.backup_piece_id_;
  } else {
    bret = lhs.copy_id_ < rhs.copy_id_;
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObPhysicalRestoreBackupDestList, backup_set_path_list_, backup_piece_path_list_);

ObPhysicalRestoreBackupDestList::ObPhysicalRestoreBackupDestList()
    : allocator_(), backup_set_path_list_(), backup_piece_path_list_()
{}

ObPhysicalRestoreBackupDestList::~ObPhysicalRestoreBackupDestList()
{
  reset();
}

void ObPhysicalRestoreBackupDestList::reset()
{
  backup_set_path_list_.reset();
  backup_piece_path_list_.reset();
  allocator_.reset();
}

int ObPhysicalRestoreBackupDestList::assign(const ObPhysicalRestoreBackupDestList& list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_set_path_list_.assign(list.backup_set_path_list_))) {
    LOG_WARN("failed to assign backup set path list", KR(ret));
  } else if (OB_FAIL(backup_piece_path_list_.assign(list.backup_piece_path_list_))) {
    LOG_WARN("failed to assign backup piece path list", KR(ret));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::set(const common::ObArray<share::ObSimpleBackupSetPath>& backup_set_list,
    const common::ObArray<share::ObSimpleBackupPiecePath>& backup_piece_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_set_path_list_.assign(backup_set_list))) {
    LOG_WARN("failed to assign", KR(ret));
  } else if (OB_FAIL(backup_piece_path_list_.assign(backup_piece_list))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::get_backup_set_list_format_str(
    common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* str_buf = NULL;
  int64_t str_buf_len = get_backup_set_list_format_str_len() + 1;
  if (str_buf_len > OB_MAX_LONGTEXT_LENGTH + 1) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format str is too long", KR(ret), K(str_buf_len));
  } else if (OB_ISNULL(str_buf = static_cast<char*>(allocator.alloc(str_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(str_buf_len));
  } else if (backup_set_path_list_.count() <= 0) {
    MEMSET(str_buf, '\0', str_buf_len);
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_path_list_.count(); i++) {
      const share::ObSimpleBackupSetPath& item = backup_set_path_list_.at(i);
      if (OB_FAIL(databuff_printf(str_buf,
              str_buf_len,
              pos,
              "%s%.*s",
              0 == i ? "" : ",",
              static_cast<int>(item.backup_dest_.size()),
              item.backup_dest_.ptr()))) {
        LOG_WARN("fail to append str", KR(ret), K(item), K(pos), K(str_buf));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(str_buf) || str_buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format str", KR(ret), K(str_buf), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len - 1);
    LOG_DEBUG("get format backup set path list str", KR(ret), K(str));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::get_backup_piece_list_format_str(
    common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* str_buf = NULL;
  int64_t str_buf_len = get_backup_piece_list_format_str_len() + 1;
  if (str_buf_len > OB_MAX_LONGTEXT_LENGTH + 1) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format str is too long", KR(ret), K(str_buf_len));
  } else if (OB_ISNULL(str_buf = static_cast<char*>(allocator.alloc(str_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(str_buf_len));
  } else if (backup_piece_path_list_.count() <= 0) {
    MEMSET(str_buf, '\0', str_buf_len);
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_path_list_.count(); i++) {
      const share::ObSimpleBackupPiecePath& item = backup_piece_path_list_.at(i);
      if (OB_FAIL(databuff_printf(str_buf,
              str_buf_len,
              pos,
              "%s%.*s",
              0 == i ? "" : ",",
              static_cast<int>(item.backup_dest_.size()),
              item.backup_dest_.ptr()))) {
        LOG_WARN("fail to append str", KR(ret), K(item), K(pos), K(str_buf));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(str_buf) || str_buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format str", KR(ret), K(str_buf), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len - 1);
    LOG_DEBUG("get format backup piece path list str", KR(ret), K(str));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::backup_set_list_assign_with_hex_str(const common::ObString& str)
{
  int ret = OB_SUCCESS;
  int64_t str_size = str.length();
  char* deserialize_buf = NULL;
  int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  if (str_size <= 0) {
    // skip
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator_.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", KR(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(backup_set_path_list_.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize backup set path list",
        KR(ret),
        K(str),
        "deserialize_buf",
        ObString(deserialize_size, deserialize_buf));
  } else if (deserialize_pos > deserialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
  } else {
    LOG_DEBUG("assign with hex str", KR(ret), K(str));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::backup_piece_list_assign_with_hex_str(const common::ObString& str)
{
  int ret = OB_SUCCESS;
  int64_t str_size = str.length();
  char* deserialize_buf = NULL;
  int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  if (str_size <= 0) {
    // skip
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator_.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", KR(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(backup_piece_path_list_.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize backup piece path list",
        KR(ret),
        K(str),
        "deserialize_buf",
        ObString(deserialize_size, deserialize_buf));
  } else if (deserialize_pos > deserialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
  } else {
    LOG_DEBUG("assign with hex str", KR(ret), K(str));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::get_backup_set_list_hex_str(
    common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* serialize_buf = NULL;
  int64_t serialize_size = backup_set_path_list_.get_serialize_size();
  int64_t serialize_pos = 0;
  char* hex_buf = NULL;
  int64_t hex_size = 2 * serialize_size;
  int64_t hex_pos = 0;
  if (serialize_size > OB_MAX_LONGTEXT_LENGTH / 2) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize_size is too long", KR(ret), K(serialize_size));
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(backup_set_path_list_.serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("fail to serialize table_items", KR(ret), K_(backup_set_path_list));
  } else if (serialize_pos > serialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    LOG_WARN("fail to print hex", KR(ret), K(serialize_pos), K(hex_size));
  } else if (hex_pos > hex_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    str.assign_ptr(hex_buf, hex_size);
    LOG_DEBUG("get hex backup set list str", KR(ret), K(str));
  }
  return ret;
}

int ObPhysicalRestoreBackupDestList::get_backup_piece_list_hex_str(
    common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* serialize_buf = NULL;
  int64_t serialize_size = backup_piece_path_list_.get_serialize_size();
  int64_t serialize_pos = 0;
  char* hex_buf = NULL;
  int64_t hex_size = 2 * serialize_size;
  int64_t hex_pos = 0;
  if (serialize_size > OB_MAX_LONGTEXT_LENGTH / 2) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize_size is too long", KR(ret), K(serialize_size));
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(backup_piece_path_list_.serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("fail to serialize table_items", KR(ret), K_(backup_piece_path_list));
  } else if (serialize_pos > serialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    LOG_WARN("fail to print hex", KR(ret), K(serialize_pos), K(hex_size));
  } else if (hex_pos > hex_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    str.assign_ptr(hex_buf, hex_size);
    LOG_DEBUG("get hex backup piece list str", KR(ret), K(str));
  }
  return ret;
}

int64_t ObPhysicalRestoreBackupDestList::get_backup_set_list_format_str_len() const
{
  int64_t length = 0;
  for (int64_t i = 0; i < backup_set_path_list_.count(); ++i) {
    const ObSimpleBackupSetPath& path = backup_set_path_list_.at(i);
    length += path.backup_dest_.size() + (0 == i ? 0 : 1);
  }
  return length;
}

int64_t ObPhysicalRestoreBackupDestList::get_backup_piece_list_format_str_len() const
{
  int64_t length = 0;
  for (int64_t i = 0; i < backup_piece_path_list_.count(); ++i) {
    const ObSimpleBackupPiecePath& path = backup_piece_path_list_.at(i);
    length += path.backup_dest_.size() + (0 == i ? 0 : 1);
  }
  return length;
}

/******************ObBackupCommonHeader*******************/
ObBackupCommonHeader::ObBackupCommonHeader()
{
  reset();
}

void ObBackupCommonHeader::reset()
{
  memset(this, 0, sizeof(ObBackupCommonHeader));
  header_version_ = COMMON_HEADER_VERSION;
  compressor_type_ = static_cast<uint8_t>(common::ObCompressorType::INVALID_COMPRESSOR);
  data_type_ = static_cast<uint16_t>(ObBackupFileType::BACKUP_TYPE_MAX);
  header_length_ = static_cast<uint16_t>(sizeof(ObBackupCommonHeader));
}

void ObBackupCommonHeader::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupCommonHeader::set_checksum(const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_zlength_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_zlength));
  } else {
    data_checksum_ = ob_crc64(buf, len);
    header_checksum_ = calc_header_checksum();
  }
  return ret;
}

int ObBackupCommonHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupCommonHeader::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(compressor_type_));
  checksum = checksum ^ data_type_;
  checksum = checksum ^ header_length_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  format_i64(align_length_, checksum);
  return checksum;
}

int ObBackupCommonHeader::check_valid() const
{
  int ret = OB_SUCCESS;

  if (COMMON_HEADER_VERSION != header_version_ || ObCompressorType::INVALID_COMPRESSOR == compressor_type_ ||
      ((ObCompressorType::NONE_COMPRESSOR == compressor_type_) && (data_length_ != data_zlength_)) ||
      data_type_ >= ObBackupFileType::BACKUP_TYPE_MAX || header_length_ != sizeof(ObBackupCommonHeader) ||
      align_length_ < 0) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(ret), K(*this), "ObBackupCommonHeader size", sizeof(ObBackupCommonHeader));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

bool ObBackupCommonHeader::is_compresssed_data() const
{
  return (data_length_ != data_zlength_) || (compressor_type_ > ObCompressorType::NONE_COMPRESSOR);
}

int ObBackupCommonHeader::check_data_checksum(const char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(len));
  } else if ((0 == len) && (0 != data_length_ || 0 != data_zlength_ || 0 != data_checksum_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(len), K_(data_length), K_(data_zlength), K_(data_checksum));
  } else if (data_zlength_ != len) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(len), K_(data_zlength));
  } else {
    int64_t crc_check_sum = ob_crc64(buf, len);
    if (crc_check_sum != data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "checksum error", K(crc_check_sum), K_(data_checksum), K(ret));
    }
  }
  return ret;
}

/******************ObBackupMetaHeader*******************/
ObBackupMetaHeader::ObBackupMetaHeader()
{
  reset();
}

void ObBackupMetaHeader::reset()
{
  memset(this, 0, sizeof(ObBackupMetaHeader));
  header_version_ = META_HEADER_VERSION;
  meta_type_ = static_cast<uint8_t>(ObBackupMetaType::META_TYPE_MAX);
}

void ObBackupMetaHeader::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupMetaHeader::set_checksum(const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_length_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_length));
  } else {
    data_checksum_ = ob_crc64(buf, len);
    header_checksum_ = calc_header_checksum();
  }
  return ret;
}

int ObBackupMetaHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupMetaHeader::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(meta_type_));
  format_i32(data_length_, checksum);
  format_i64(data_checksum_, checksum);

  return checksum;
}

int ObBackupMetaHeader::check_data_checksum(const char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_length_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_length));
  } else {
    int64_t checksum = ob_crc64(buf, len);
    if (OB_UNLIKELY(data_checksum_ != checksum)) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "check data checksum failed.", K(*this), K(checksum), K(ret));
    }
  }
  return ret;
}

int ObBackupMetaHeader::check_valid() const
{
  int ret = OB_SUCCESS;

  if (META_HEADER_VERSION != header_version_ || meta_type_ >= ObBackupFileType::BACKUP_TYPE_MAX) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

/******************ObBackupMetaIndex*******************/
OB_SERIALIZE_MEMBER(ObBackupMetaIndex, meta_type_, table_id_, partition_id_, offset_, data_length_, task_id_);
ObBackupMetaIndex::ObBackupMetaIndex()
{
  reset();
}

void ObBackupMetaIndex::reset()
{
  memset(this, 0, sizeof(ObBackupMetaIndex));
  meta_type_ = static_cast<uint8_t>(ObBackupMetaType::META_TYPE_MAX);
}

int ObBackupMetaIndex::check_valid() const
{
  int ret = OB_SUCCESS;

  if (meta_type_ >= ObBackupFileType::BACKUP_TYPE_MAX) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (partition_id_ < 0 || offset_ < 0 || data_length_ < 0 || task_id_ < 0) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid  index", K(ret), K(*this));
  }

  return ret;
}

bool ObBackupMetaIndex::operator==(const ObBackupMetaIndex& other) const
{
  return table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && meta_type_ == other.meta_type_ &&
         offset_ == other.offset_ && data_length_ == other.data_length_ && task_id_ == other.task_id_;
}

/******************ObBackupMacroIndex*******************/
ObBackupMacroIndex::ObBackupMacroIndex()
{
  reset();
}

void ObBackupMacroIndex::reset()
{
  memset(this, 0, sizeof(ObBackupMacroIndex));
}

OB_SERIALIZE_MEMBER(ObBackupMacroIndex, table_id_, partition_id_, index_table_id_, sstable_macro_index_, data_version_,
    data_seq_, backup_set_id_, sub_task_id_, offset_, data_length_);

int ObBackupMacroIndex::check_valid() const
{
  int ret = OB_SUCCESS;

  if (!(table_id_ != OB_INVALID_ID && partition_id_ >= 0 && index_table_id_ != OB_INVALID_ID &&
          sstable_macro_index_ >= 0 && data_version_ >= 0 && data_seq_ >= 0 && backup_set_id_ >= 0 &&
          sub_task_id_ >= 0 && offset_ >= 0 && data_length_ >= 0)) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid index", K(*this), K(ret));
  }

  return ret;
}

/******************ObBackupSStableMacroIndex*******************/
ObBackupSStableMacroIndex::ObBackupSStableMacroIndex()
{
  reset();
}

void ObBackupSStableMacroIndex::reset()
{
  memset(this, 0, sizeof(ObBackupSStableMacroIndex));
  header_version_ = SSTABLE_MACRO_INDEX_VERSION;
  data_type_ = static_cast<uint8_t>(ObBackupFileType::BACKUP_SSTABLE_MACRO_INDEX);
  header_length_ = static_cast<uint16_t>(sizeof(ObBackupSStableMacroIndex));
}

void ObBackupSStableMacroIndex::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupSStableMacroIndex::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupSStableMacroIndex::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(data_type_));
  checksum = checksum ^ header_length_;
  checksum = checksum ^ reserved_;
  format_i64(table_id_, checksum);
  format_i64(partition_id_, checksum);
  format_i64(index_table_id_, checksum);
  format_i64(offset_, checksum);
  format_i64(count_, checksum);

  return checksum;
}

int ObBackupSStableMacroIndex::is_valid() const
{
  int ret = OB_SUCCESS;

  if (SSTABLE_MACRO_INDEX_VERSION != header_version_ || ObBackupFileType::BACKUP_SSTABLE_MACRO_INDEX != data_type_ ||
      header_length_ != sizeof(ObBackupSStableMacroIndex)) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

ObMetaIndexKey::ObMetaIndexKey()
{
  reset();
}

ObMetaIndexKey::ObMetaIndexKey(const int64_t table_id, const int64_t partition_id, const uint8_t meta_type)
{
  table_id_ = table_id;
  partition_id_ = partition_id;
  meta_type_ = meta_type;
}

void ObMetaIndexKey::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  meta_type_ = ObBackupMetaType::META_TYPE_MAX;
}

bool ObMetaIndexKey::operator==(const ObMetaIndexKey& other) const
{
  return table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && meta_type_ == other.meta_type_;
}

bool ObMetaIndexKey::operator!=(const ObMetaIndexKey& other) const
{
  return !(*this == other);
}

uint64_t ObMetaIndexKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  hash_val = murmurhash(&meta_type_, sizeof(meta_type_), hash_val);
  return hash_val;
}

bool ObMetaIndexKey::is_valid() const
{
  return OB_INVALID_ID != table_id_ && partition_id_ >= 0 && meta_type_ < ObBackupMetaType::META_TYPE_MAX;
}

static const char* status_strs[] = {
    "INVALID",
    "STOP",
    "BEGINNING",
    "DOING",
    "STOPPING",
    "INTERRUPTED",
    "MIXED",
    "PAUSED",
};

const char* ObLogArchiveStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= MAX) {
    str = "UNKNOWN";
  } else {
    str = status_strs[status];
  }
  return str;
}

ObLogArchiveStatus::STATUS ObLogArchiveStatus::get_status(const char* status_str)
{
  ObLogArchiveStatus::STATUS status = ObLogArchiveStatus::MAX;

  const int64_t count = ARRAYSIZEOF(status_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObLogArchiveStatus::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, status_strs[i])) {
      status = static_cast<ObLogArchiveStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

OB_SERIALIZE_MEMBER(ObGetTenantLogArchiveStatusArg, incarnation_, round_, backup_piece_id_);

bool ObGetTenantLogArchiveStatusArg::is_valid() const
{
  return incarnation_ >= 1 && round_ >= 1;
}

ObTenantLogArchiveStatus::ObTenantLogArchiveStatus()
    : tenant_id_(OB_INVALID_ID),
      copy_id_(0),
      start_ts_(0),
      checkpoint_ts_(0),
      incarnation_(0),
      round_(0),
      status_(ObLogArchiveStatus::MAX),
      is_mark_deleted_(false),
      is_mount_file_created_(false),
      compatible_(COMPATIBLE::NONE),
      backup_piece_id_(0),
      start_piece_id_(0)
{}

void ObTenantLogArchiveStatus::reset()
{
  tenant_id_ = OB_INVALID_ID;
  copy_id_ = 0;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  incarnation_ = 0;
  round_ = 0;
  status_ = ObLogArchiveStatus::MAX;
  is_mark_deleted_ = false;
  is_mount_file_created_ = false;
  compatible_ = COMPATIBLE::NONE;
  backup_piece_id_ = 0;
  start_piece_id_ = 0;
}

bool ObTenantLogArchiveStatus::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && copy_id_ >= 0 && ObLogArchiveStatus::is_valid(status_) && incarnation_ >= 0 &&
         round_ >= 0 && start_ts_ >= -1 && checkpoint_ts_ >= -1 && compatible_ >= COMPATIBLE::NONE &&
         backup_piece_id_ >= 0 && start_piece_id_ >= 0;
}

bool ObTenantLogArchiveStatus::is_compatible_valid(COMPATIBLE compatible)
{
  return compatible >= COMPATIBLE::NONE && compatible < COMPATIBLE::MAX;
}

bool ObTenantLogArchiveStatus::is_piece_open() const
{
  return 0 < start_piece_id_;
}

// archive log state machine
// STOP->STOP
// BEGINNING -> BEGINNING\DOING\STOPPING\INERRUPTED
// DOING -> DOING\STOPPING\INERRUPTED
// STOPPING -> STOPPIONG\STOP
// INTERRUPTED -> INERRUPTED\STOPPING
// backup archive log state machine
// DOING->PAUSED\STOP
// PAUSED->DOING
int ObTenantLogArchiveStatus::update(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not valid status", K(ret), K(*this));
  } else if (!new_status.is_valid() || new_status.incarnation_ != incarnation_ || new_status.round_ != round_ ||
             new_status.tenant_id_ != tenant_id_ || new_status.compatible_ != compatible_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "is_new_status_valid", new_status.is_valid(), K(new_status), K(*this));
  } else if (is_mount_file_created_ && !new_status.is_mount_file_created_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_mount_file_created_ cannot change", K(ret), K(*this), K(new_status));
  } else {
    is_mount_file_created_ = new_status.is_mount_file_created_;
    switch (status_) {
      case ObLogArchiveStatus::STOP: {
        if (OB_FAIL(update_stop_(new_status))) {
          LOG_WARN("failed to update stop", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::BEGINNING: {
        if (OB_FAIL(update_beginning_(new_status))) {
          LOG_WARN("failed to update beginning", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::DOING: {
        if (OB_FAIL(update_doing_(new_status))) {
          LOG_WARN("failed to update doing", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::STOPPING: {
        if (OB_FAIL(update_stopping_(new_status))) {
          LOG_WARN("failed to update stopping", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::INTERRUPTED: {
        if (OB_FAIL(update_interrupted_(new_status))) {
          LOG_WARN("failed to update interrupted", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::PAUSED: {
        if (OB_FAIL(update_paused_(new_status))) {
          LOG_WARN("failed to update paused", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid status", K(ret), K(*this), K(new_status));
      }
    }  // end switch
  }

  return ret;
}

int ObTenantLogArchiveStatus::get_piece_key(ObBackupPieceInfoKey& key) const
{
  int ret = OB_SUCCESS;
  key.reset();

  key.incarnation_ = incarnation_;
  key.tenant_id_ = tenant_id_;
  key.round_id_ = round_;
  if (start_piece_id_ > 0) {
    key.backup_piece_id_ = backup_piece_id_;
  } else {
    key.backup_piece_id_ = 0;  // 0 means not switch piece
  }
  key.copy_id_ = copy_id_;
  return ret;
}

bool ObTenantLogArchiveStatus::need_switch_piece() const
{
  return start_piece_id_ > 0;
}

int ObTenantLogArchiveStatus::update_stop_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::STOP != new_status.status_) {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stop status can only update with stop", K(ret), K(*this), K(new_status));
  } else {
    LOG_INFO("no need update stop status", K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_beginning_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::DOING == new_status.status_) {
    if (0 >= new_status.start_ts_ || 0 >= new_status.checkpoint_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_WARN("[LOG_ARCHIVE] checkpoint_ts or start_ts must not less than 0", K(ret), K(*this), K(new_status));
    } else {
      LOG_INFO("[LOG_ARCHIVE] update beginning to doing", K(*this), K(new_status));
      status_ = new_status.status_;
      start_ts_ = new_status.start_ts_;
      checkpoint_ts_ = new_status.checkpoint_ts_;
    }
  } else if (ObLogArchiveStatus::BEGINNING == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_ ||
             ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    LOG_INFO("[LOG_ARCHIVE] update beginning to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_doing_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::DOING == new_status.status_) {
    if (start_ts_ < new_status.start_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_ERROR("new start_ts must not larger than the prev one", K(ret), K(*this), K(new_status));
    } else if (checkpoint_ts_ > new_status.checkpoint_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_WARN("new checkpoint_ts must not less than the prev one", K(ret), K(*this), K(new_status));
    } else {
      LOG_INFO("update doing stat", K(*this), K(new_status));
      checkpoint_ts_ = new_status.checkpoint_ts_;
    }
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_ ||
             ObLogArchiveStatus::INTERRUPTED == new_status.status_ ||
             ObLogArchiveStatus::PAUSED == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update doing to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_stopping_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::STOPPING == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOP == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update stopping to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_interrupted_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update interrupted to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_paused_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::DOING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update paused to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTenantLogArchiveStatus, tenant_id_, start_ts_, checkpoint_ts_, incarnation_, round_, status_,
    is_mark_deleted_, is_mount_file_created_, compatible_, backup_piece_id_, start_piece_id_);

OB_SERIALIZE_MEMBER(ObServerTenantLogArchiveStatus, tenant_id_, incarnation_, status_, round_, min_backup_piece_id_,
    start_ts_, checkpoint_ts_, max_log_ts_);

ObServerTenantLogArchiveStatus::ObServerTenantLogArchiveStatus()
{
  reset();
}

bool ObServerTenantLogArchiveStatus::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && incarnation_ >= 0 && ObLogArchiveStatus::is_valid(status_) && round_ >= 0 &&
         min_backup_piece_id_ >= 0 && start_ts_ >= -1 && checkpoint_ts_ >= -1 && max_log_ts_ >= 0;
}

void ObServerTenantLogArchiveStatus::reset()
{
  tenant_id_ = 0;
  incarnation_ = 0;
  status_ = ObLogArchiveStatus::MAX;
  round_ = 0;
  min_backup_piece_id_ = 0;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  max_log_ts_ = 0;
}

int ObServerTenantLogArchiveStatus::get_compat_status(ObTenantLogArchiveStatus& status) const
{
  int ret = OB_SUCCESS;
  status.reset();

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this status is not valid", K(ret), K(*this));
  } else if (min_backup_piece_id_ != 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only compat mode need trans server tenant status to tenant status", K(ret), K(*this));
  } else {
    status.tenant_id_ = tenant_id_;
    status.incarnation_ = incarnation_;
    status.status_ = status_;
    status.round_ = round_;
    status.backup_piece_id_ = min_backup_piece_id_;  // must be 0
    status.start_ts_ = start_ts_;
    status.checkpoint_ts_ = checkpoint_ts_;
    // ObTenantLogArchiveStatus has not max_log_ts
  }
  return ret;
}

int ObServerTenantLogArchiveStatus::set_status(const ObTenantLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;

  if (!status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(status));
  } else {
    tenant_id_ = status.tenant_id_;
    incarnation_ = status.incarnation_;
    status_ = status.status_;
    round_ = status.round_;
    min_backup_piece_id_ = status.backup_piece_id_;
    start_ts_ = status.start_ts_;
    checkpoint_ts_ = status.checkpoint_ts_;
    max_log_ts_ = 0;  // // ObTenantLogArchiveStatus has not max_log_ts
  }
  return ret;
}

ObServerTenantLogArchiveStatusWrapper::ObServerTenantLogArchiveStatusWrapper()
    : result_code_(OB_SUCCESS), status_array_()
{}

OB_SERIALIZE_MEMBER(ObServerTenantLogArchiveStatusWrapper, result_code_, status_array_);

ObTenantLogArchiveStatusWrapper::ObTenantLogArchiveStatusWrapper() : result_code_(OB_SUCCESS), status_array_()
{}

OB_SERIALIZE_MEMBER(ObTenantLogArchiveStatusWrapper, result_code_, status_array_);

ObLogArchiveBackupInfo::ObLogArchiveBackupInfo() : status_()
{
  backup_dest_[0] = '\0';
}

void ObLogArchiveBackupInfo::reset()
{
  status_.reset();
  backup_dest_[0] = '\0';
}

bool ObLogArchiveBackupInfo::is_valid() const
{
  return status_.is_valid();
}

// TODO(yaoying.yyy): S3 is alse oss?
bool ObLogArchiveBackupInfo::is_oss() const
{
  ObString dest(backup_dest_);
  return dest.prefix_match(OB_OSS_PREFIX);
}

bool ObLogArchiveBackupInfo::is_same(const ObLogArchiveBackupInfo& other) const
{
  return 0 == strncmp(backup_dest_, other.backup_dest_, sizeof(backup_dest_)) &&
         status_.tenant_id_ == other.status_.tenant_id_ && status_.start_ts_ == other.status_.start_ts_ &&
         status_.checkpoint_ts_ == other.status_.checkpoint_ts_ && status_.incarnation_ == other.status_.incarnation_ &&
         status_.round_ == other.status_.round_ && status_.status_ == other.status_.status_ &&
         status_.is_mark_deleted_ == other.status_.is_mark_deleted_;
}

int ObLogArchiveBackupInfo::get_piece_key(ObBackupPieceInfoKey& key) const
{
  return status_.get_piece_key(key);
}

int ObLogArchiveBackupInfo::get_backup_dest(ObBackupDest& backup_dest) const
{
  int ret = OB_SUCCESS;
  backup_dest.reset();
  if (0 == strlen(backup_dest_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_));
  }
  return ret;
}

ObBackupDest::ObBackupDest() : device_type_(OB_STORAGE_MAX_TYPE)
{
  root_path_[0] = '\0';
  storage_info_[0] = '\0';
}

bool ObBackupDest::is_valid() const
{
  return device_type_ >= 0 && device_type_ < OB_STORAGE_MAX_TYPE && strlen(root_path_) > 0;
}

void ObBackupDest::reset()
{
  device_type_ = OB_STORAGE_MAX_TYPE;
  root_path_[0] = '\0';
  storage_info_[0] = '\0';
}

int ObBackupDest::set(const common::ObString& backup_dest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char* buf = NULL;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(backup_dest.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", KR(ret));
  } else {
    MEMCPY(buf, backup_dest.ptr(), backup_dest.length());
    buf[backup_dest.length()] = '\0';
    if (OB_FAIL(set(buf))) {
      LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest));
    }
  }
  return ret;
}

int ObBackupDest::set(const char* backup_dest)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  common::ObStorageType type;
  ObString bakup_dest_str(backup_dest);

  if (device_type_ != OB_STORAGE_MAX_TYPE) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (OB_ISNULL(backup_dest)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(backup_dest));
  } else if (OB_FAIL(get_storage_type_from_path(bakup_dest_str, type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else {
    // file:///root_backup_dir"
    while (backup_dest[pos] != '\0') {
      if (backup_dest[pos] == '?') {
        break;
      }
      ++pos;
    }
    int64_t left_count = strlen(backup_dest) - pos;

    if (pos >= sizeof(root_path_) || left_count >= sizeof(storage_info_)) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_ERROR("backup dest is too long, cannot work", K(ret), K(pos), K(backup_dest), K(left_count));
    } else {
      MEMCPY(root_path_, backup_dest, pos);
      root_path_[pos] = '\0';
      ++pos;
      if (left_count > 0) {
        STRNCPY(storage_info_, backup_dest + pos, left_count);
        storage_info_[left_count] = '\0';
      }

      if (ObStorageType::OB_STORAGE_FILE != type && 0 == strlen(storage_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup device is not nfs, do not allow storage info be empty", K(ret), K(backup_dest));
      } else {
        device_type_ = type;
        LOG_TRACE("succeed to set backup dest", K(ret), K(backup_dest), K(*this));
      }
    }
  }

  return ret;
}

int ObBackupDest::set(const char* root_path, const char* storage_info)
{
  int ret = OB_SUCCESS;
  common::ObStorageType type;
  ObString root_path_str(root_path);

  if (device_type_ != OB_STORAGE_MAX_TYPE) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (OB_ISNULL(root_path) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(root_path), KP(storage_info));
  } else if (OB_FAIL(get_storage_type_from_path(root_path_str, type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else if (OB_FAIL(databuff_printf(root_path_, OB_MAX_BACKUP_PATH_LENGTH, "%s", root_path))) {
    LOG_WARN("failed to set root path", K(ret), K(root_path), K(strlen(root_path)));
  } else if (OB_FAIL(databuff_printf(storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", storage_info))) {
    LOG_WARN("failed to set storage info", K(ret), K(storage_info), K(strlen(storage_info)));
  } else if (ObStorageType::OB_STORAGE_FILE != type && 0 == strlen(storage_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup device is not nfs, do not allow storage info empty", K(ret), K(root_path), K(storage_info));
  } else {
    device_type_ = type;
  }
  return ret;
}

const char* ObBackupDest::get_type_str() const
{
  return get_storage_type_str(device_type_);
}

bool ObBackupDest::is_nfs_storage() const
{
  return OB_STORAGE_FILE == device_type_;
}

bool ObBackupDest::is_oss_storage() const
{
  return OB_STORAGE_OSS == device_type_;
}

bool ObBackupDest::is_cos_storage() const
{
  return OB_STORAGE_COS == device_type_;
}

bool ObBackupDest::is_root_path_equal(const ObBackupDest& backup_dest) const
{
  return device_type_ == backup_dest.device_type_ &&
         (0 == STRNCMP(root_path_, backup_dest.root_path_, OB_MAX_BACKUP_PATH_LENGTH));
}

int ObBackupDest::get_backup_dest_str(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup dest is not init", K(ret), K(*this));
  } else if (OB_ISNULL(buf) || buf_size < share::OB_MAX_BACKUP_DEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup dest str get invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%s", root_path_))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(root_path_), K(sizeof(root_path_)));
  } else if (ObStorageType::OB_STORAGE_OSS == device_type_) {
    const int64_t str_len = strlen(buf);
    if (OB_FAIL(databuff_printf(buf + str_len, buf_size - str_len, "?%s", storage_info_))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(storage_info_), K(str_len), K(sizeof(storage_info_)));
    }
  } else if (ObStorageType::OB_STORAGE_COS == device_type_) {
    const int64_t str_len = strlen(buf);
    if (OB_FAIL(databuff_printf(buf + str_len, buf_size - str_len, "?%s", storage_info_))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(storage_info_), K(str_len), K(sizeof(storage_info_)));
    }
  }
  return ret;
}

bool ObBackupDest::operator==(const ObBackupDest& backup_dest) const
{
  return device_type_ == backup_dest.device_type_ && (0 == STRCMP(root_path_, backup_dest.root_path_)) &&
         (0 == STRCMP(storage_info_, backup_dest.storage_info_));
}

bool ObBackupDest::operator!=(const ObBackupDest& backup_dest) const
{
  return device_type_ != backup_dest.device_type_ || (0 != STRCMP(root_path_, backup_dest.root_path_)) ||
         (0 != STRCMP(storage_info_, backup_dest.storage_info_));
}

uint64_t ObBackupDest::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&device_type_, sizeof(device_type_), hash_val);
  hash_val = murmurhash(root_path_, static_cast<int32_t>(strlen(root_path_)), hash_val);
  hash_val = murmurhash(storage_info_, static_cast<int32_t>(strlen(storage_info_)), hash_val);
  return hash_val;
}

ObBackupBaseDataPathInfo::ObBackupBaseDataPathInfo()
    : dest_(), tenant_id_(OB_INVALID_ID), full_backup_set_id_(0), inc_backup_set_id_(0), backup_date_(0), compatible_(0)
{}

int ObBackupBaseDataPathInfo::set(const ObClusterBackupDest& dest, const uint64_t tenant_id,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_date,
    const int64_t compatible)
{
  int ret = OB_SUCCESS;
  if (!dest.is_valid() || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 || inc_backup_set_id <= 0 ||
      backup_date < 0 || compatible < OB_BACKUP_COMPATIBLE_VERSION_V1 ||
      compatible > OB_BACKUP_COMPATIBLE_VERSION_MAX) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid argument",
        K(ret),
        K(dest),
        K(tenant_id),
        K(full_backup_set_id),
        K(inc_backup_set_id),
        K(backup_date),
        K(compatible));
  } else {
    dest_ = dest;
    tenant_id_ = tenant_id;
    full_backup_set_id_ = full_backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    backup_date_ = backup_date;
    compatible_ = compatible;
  }
  return ret;
}

void ObBackupBaseDataPathInfo::reset()
{
  dest_.reset();
  tenant_id_ = OB_INVALID_ID;
  full_backup_set_id_ = 0;
  inc_backup_set_id_ = 0;
  backup_date_ = 0;
  compatible_ = 0;
}

bool ObBackupBaseDataPathInfo::is_valid() const
{
  return dest_.is_valid() && OB_INVALID_ID != tenant_id_ && inc_backup_set_id_ >= full_backup_set_id_ &&
         full_backup_set_id_ > 0 && backup_date_ >= 0 && compatible_ < OB_BACKUP_COMPATIBLE_VERSION_MAX &&
         compatible_ >= OB_BACKUP_COMPATIBLE_VERSION_V1;
}

const char* ObBackupInfoStatus::get_status_str(const BackupStatus& status)
{
  const char* str = "UNKNOWN";
  const char* info_backup_status_strs[] = {
      "STOP",
      "PREPARE",
      "SCHEDULE",
      "DOING",
      "CANCEL",
      "CLEANUP",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(info_backup_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = info_backup_status_strs[status];
  }
  return str;
}

const char* ObBackupInfoStatus::get_info_backup_status_str() const
{
  return get_status_str(status_);
}

int ObBackupInfoStatus::set_info_backup_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set info backup status get invalid argument", K(ret), KP(buf));
  } else {
    const char* info_backup_status_strs[] = {
        "STOP",
        "PREPARE",
        "SCHEDULE",
        "DOING",
        "CANCEL",
        "CLEANUP",
    };
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(info_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(info_backup_status_strs); i++) {
      if (0 == STRCMP(info_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup info status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObBackupType::get_backup_type_str() const
{
  const char* str = "UNKNOWN";
  const char* backup_func_type_strs[] = {
      "",
      "D",
      "I",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_func_type_strs), "types count mismatch");
  if (type_ < 0 || type_ >= MAX) {
    LOG_ERROR("invalid backup type", K(type_));
  } else {
    str = backup_func_type_strs[type_];
  }
  return str;
}

int ObBackupType::set_backup_type(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup type get invalid argument", K(ret), KP(buf));
  } else {
    const char* backup_func_type_strs[] = {
        "",
        "D",
        "I",
    };
    BackupType tmp_type = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_func_type_strs), "types count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_func_type_strs); i++) {
      if (0 == STRCMP(backup_func_type_strs[i], buf)) {
        tmp_type = static_cast<BackupType>(i);
      }
    }

    if (MAX == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup type str not found", K(ret), K(buf));
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

const char* ObBackupDeviceType::get_backup_device_str() const
{
  const char* str = "UNKNOWN";
  const char* backup_device_type_strs[] = {
      "file://",
      "oss://",
      "ofs://",
      "cos://",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_device_type_strs), "types count mismatch");
  if (type_ < 0 || type_ >= MAX) {
    LOG_ERROR("invalid backup device type", K(type_));
  } else {
    str = backup_device_type_strs[type_];
  }
  return str;
}

int ObBackupDeviceType::set_backup_device_type(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup device get invalid argument", K(ret), KP(buf));
  } else {
    const char* backup_device_type_strs[] = {
        "file://",
        "oss://",
        "ofs://",
        "cos://",
    };
    BackupDeviceType tmp_type = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_device_type_strs), "types count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_device_type_strs); i++) {
      if (0 == STRCMP(backup_device_type_strs[i], buf)) {
        tmp_type = static_cast<BackupDeviceType>(i);
      }
    }

    if (MAX == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup device type str not found", K(ret), K(buf));
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

ObBaseBackupInfoStruct::ObBaseBackupInfoStruct()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      backup_dest_(),
      backup_backup_dest_(),
      backup_snapshot_version_(0),
      backup_schema_version_(0),
      backup_data_version_(0),
      detected_backup_region_(),
      backup_type_(),
      backup_status_(),
      backup_task_id_(0),
      copy_id_(0),
      encryption_mode_(share::ObBackupEncryptionMode::MAX_MODE),
      passwd_()
{}

void ObBaseBackupInfoStruct::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  incarnation_ = 0;
  backup_dest_.reset();
  backup_backup_dest_.reset();
  backup_snapshot_version_ = 0;
  backup_schema_version_ = 0;
  backup_data_version_ = 0;
  detected_backup_region_.reset();
  backup_type_.reset();
  backup_status_.reset();
  backup_task_id_ = 0;
  copy_id_ = 0;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
}

bool ObBaseBackupInfoStruct::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && tenant_id_ > 0 && backup_status_.is_valid() &&
         share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

bool ObBaseBackupInfoStruct::has_cleaned() const
{
  return is_valid() && backup_status_.is_stop_status() && 0 == backup_snapshot_version_ &&
         0 == backup_schema_version_ && 0 == backup_data_version_ && ObBackupType::EMPTY == backup_type_.type_;
}

ObBaseBackupInfoStruct& ObBaseBackupInfoStruct::operator=(const ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(info))) {
    LOG_ERROR("failed to assign backup info", K(ret), K(info));
  }
  return *this;
}

int ObBaseBackupInfoStruct::assign(const ObBaseBackupInfoStruct& backup_info_struct)
{
  int ret = OB_SUCCESS;
  if (!backup_info_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(backup_info_struct));
  } else {
    backup_dest_ = backup_info_struct.backup_dest_;
    backup_backup_dest_ = backup_info_struct.backup_backup_dest_;
    detected_backup_region_ = backup_info_struct.detected_backup_region_;
    tenant_id_ = backup_info_struct.tenant_id_;
    backup_set_id_ = backup_info_struct.backup_set_id_;
    incarnation_ = backup_info_struct.incarnation_;
    backup_snapshot_version_ = backup_info_struct.backup_snapshot_version_;
    backup_schema_version_ = backup_info_struct.backup_schema_version_;
    backup_data_version_ = backup_info_struct.backup_data_version_;
    backup_type_ = backup_info_struct.backup_type_;
    backup_status_ = backup_info_struct.backup_status_;
    backup_task_id_ = backup_info_struct.backup_task_id_;
    copy_id_ = backup_info_struct.copy_id_;
    encryption_mode_ = backup_info_struct.encryption_mode_;
    passwd_ = backup_info_struct.passwd_;
  }
  return ret;
}

int ObBaseBackupInfoStruct::check_backup_info_match(const ObBaseBackupInfoStruct& backup_info_struct) const
{
  int ret = OB_SUCCESS;
  if (!backup_info_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(backup_info_struct));
  } else if (backup_dest_ != backup_info_struct.backup_dest_ || tenant_id_ != backup_info_struct.tenant_id_ ||
             backup_set_id_ != backup_info_struct.backup_set_id_ || incarnation_ != backup_info_struct.incarnation_ ||
             backup_snapshot_version_ != backup_info_struct.backup_snapshot_version_ ||
             backup_schema_version_ != backup_info_struct.backup_schema_version_ ||
             backup_data_version_ != backup_info_struct.backup_data_version_ ||
             backup_type_.type_ != backup_info_struct.backup_type_.type_ ||
             backup_status_.status_ != backup_info_struct.backup_status_.status_ ||
             encryption_mode_ != backup_info_struct.encryption_mode_ || passwd_ != backup_info_struct.passwd_) {
    ret = OB_BACKUP_INFO_NOT_MATCH;
    LOG_WARN("backup info is not match", K(ret), K(*this), K(backup_info_struct));
  }
  return ret;
}

ObClusterBackupDest::ObClusterBackupDest() : dest_(), cluster_id_(0), incarnation_(0)
{
  cluster_name_[0] = '\0';
}

bool ObClusterBackupDest::is_valid() const
{
  return dest_.is_valid() && strlen(cluster_name_) > 0 && cluster_id_ > 0 && incarnation_ == OB_START_INCARNATION;
}

bool ObClusterBackupDest::is_same(const ObClusterBackupDest& other) const
{
  return dest_ == other.dest_ && 0 == STRCMP(cluster_name_, other.cluster_name_) && cluster_id_ == other.cluster_id_ &&
         incarnation_ == other.incarnation_;
}

void ObClusterBackupDest::reset()
{
  dest_.reset();
  cluster_id_ = 0;
  incarnation_ = 0;
  cluster_name_[0] = '\0';
}

int ObClusterBackupDest::set(const char* backup_dest, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  const char* cluster_name = GCONF.cluster;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(set(backup_dest, cluster_name, cluster_id, incarnation))) {
    LOG_WARN("failed to set", K(ret), K(backup_dest), K(cluster_name), K(cluster_id), K(incarnation));
  }
  return ret;
}

int ObClusterBackupDest::set(
    const char* backup_dest, const char* cluster_name, const int64_t cluster_id, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(backup_dest) || OB_ISNULL(cluster_name) || cluster_id <= 0 || incarnation != OB_START_INCARNATION) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(backup_dest), KP(cluster_name), K(cluster_id), K(incarnation));
  } else if (strlen(cluster_name) >= sizeof(cluster_name_)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cluster is too long, cannot work", K(ret), K(cluster_name));
  } else if (OB_FAIL(dest_.set(backup_dest))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_dest));
  } else {
    const int64_t len = strlen(cluster_name);
    STRNCPY(cluster_name_, cluster_name, len);
    cluster_name_[len] = '\0';
    cluster_id_ = cluster_id;
    incarnation_ = incarnation;
    LOG_TRACE("succeed to set cluster backup dest", K(ret), K(*this), K(backup_dest));
  }

  return ret;
}

int ObClusterBackupDest::set(const ObBackupDest& backup_dest, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  const char* cluster_name = GCONF.cluster;
  if (incarnation <= 0 || !backup_dest.is_valid()) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), K(backup_dest), K(incarnation));
  } else if (strlen(cluster_name) >= sizeof(cluster_name_)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cluster is too long, cannot work", K(ret), K(cluster_name));
  } else {
    const int64_t len = strlen(cluster_name);
    STRNCPY(cluster_name_, cluster_name, len);
    cluster_name_[len] = '\0';
    cluster_id_ = GCONF.cluster_id;
    incarnation_ = incarnation;
    dest_ = backup_dest;
    LOG_INFO("succeed to set cluster backup dest", K(ret), K(*this), K(dest_));
  }
  return ret;
}

uint64_t ObClusterBackupDest::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(cluster_name_, static_cast<int32_t>(strlen(cluster_name_)), hash_val);
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  hash_val = murmurhash(&incarnation_, sizeof(incarnation_), hash_val);
  hash_val += dest_.hash();
  return hash_val;
}

bool ObClusterBackupDest::operator==(const ObClusterBackupDest& other) const
{
  return is_same(other);
}

ObTenantBackupTaskItem::ObTenantBackupTaskItem()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      snapshot_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_backup_data_version_(0),
      pg_count_(0),
      macro_block_count_(0),
      finish_pg_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      start_time_(0),
      end_time_(0),
      compatible_(0),
      cluster_version_(0),
      backup_type_(),
      status_(BackupStatus::MAX),
      device_type_(ObStorageType::OB_STORAGE_MAX_TYPE),
      result_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID),
      backup_dest_(),
      backup_data_version_(0),
      backup_schema_version_(0),
      partition_count_(0),
      finish_partition_count_(0),
      encryption_mode_(ObBackupEncryptionMode::MAX_MODE),
      passwd_(),
      is_mark_deleted_(false),
      start_replay_log_ts_(0),
      date_(0),
      copy_id_(0)
{}

void ObTenantBackupTaskItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  incarnation_ = 0;
  snapshot_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_backup_data_version_ = 0;
  pg_count_ = 0;
  macro_block_count_ = 0;
  finish_pg_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  backup_type_.reset();
  status_ = BackupStatus::MAX;
  device_type_ = ObStorageType::OB_STORAGE_MAX_TYPE;
  result_ = 0;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  backup_dest_.reset();
  backup_data_version_ = 0;
  backup_schema_version_ = 0;
  partition_count_ = 0;
  finish_partition_count_ = 0;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
  is_mark_deleted_ = false;
  start_replay_log_ts_ = 0;
  date_ = 0;
  copy_id_ = 0;
}

bool ObTenantBackupTaskItem::is_key_valid() const
{
  return tenant_id_ != OB_INVALID_ID && backup_set_id_ > 0 && incarnation_ > 0;
}

bool ObTenantBackupTaskItem::is_valid() const
{
  return is_key_valid() && BackupStatus::MAX != status_ && share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

bool ObTenantBackupTaskItem::is_same_task(const ObTenantBackupTaskItem& other) const
{
  return tenant_id_ == other.tenant_id_ && backup_set_id_ == other.backup_set_id_ &&
      incarnation_ == other.incarnation_ && copy_id_ == other.copy_id_;
}

bool ObTenantBackupTaskItem::is_result_succeed() const
{
  return OB_SUCCESS == result_;
}

const char* ObTenantBackupTaskItem::get_backup_task_status_str() const
{
  const char* str = "UNKNOWN";
  const char* tenant_task_backup_status_strs[] = {"GENERATE", "DOING", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_task_backup_status_strs), "status count mismatch");
  if (status_ < 0 || status_ >= MAX) {
    LOG_WARN("invalid backup task status", K(status_));
  } else {
    str = tenant_task_backup_status_strs[status_];
  }
  return str;
}

int ObTenantBackupTaskItem::set_backup_task_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup task status get invalid argument", K(ret), KP(buf));
  } else {
    const char* tenant_task_backup_status_strs[] = {"GENERATE", "DOING", "FINISH", "CANCEL"};
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_task_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_task_backup_status_strs); i++) {
      if (0 == STRCMP(tenant_task_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

uint64_t ObTenantBackupTaskItem::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&incarnation_, sizeof(incarnation_), hash_val);
  hash_val = murmurhash(&backup_set_id_, sizeof(backup_set_id_), hash_val);
  hash_val = murmurhash(&copy_id_, sizeof(copy_id_), hash_val);
  return hash_val;
}

bool ObTenantBackupTaskItem::operator==(const ObTenantBackupTaskItem &other) const
{
  return is_same_task(other);
}

ObPGBackupTaskItem::ObPGBackupTaskItem()
    : tenant_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      partition_id_(-1),
      incarnation_(0),
      backup_set_id_(0),
      backup_type_(),
      snapshot_version_(0),
      partition_count_(0),
      macro_block_count_(0),
      finish_partition_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      start_time_(0),
      end_time_(0),
      retry_count_(0),
      role_(INVALID_ROLE),
      replica_type_(REPLICA_TYPE_MAX),
      server_(),
      status_(BackupStatus::MAX),
      result_(0),
      task_id_(0),
      trace_id_()
{}

void ObPGBackupTaskItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  incarnation_ = 0;
  backup_set_id_ = 0;
  backup_type_.reset();
  snapshot_version_ = 0;
  partition_count_ = 0;
  macro_block_count_ = 0;
  finish_partition_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  retry_count_ = 0;
  role_ = INVALID_ROLE;
  replica_type_ = REPLICA_TYPE_MAX;
  server_.reset();
  status_ = BackupStatus::MAX;
  result_ = 0;
  task_id_ = 0;
  trace_id_.reset();
}

bool ObPGBackupTaskItem::is_key_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != table_id_ && partition_id_ >= 0 && incarnation_ > 0 &&
         backup_set_id_ > 0;
}

bool ObPGBackupTaskItem::is_same_task(const ObPGBackupTaskItem& item) const
{
  return tenant_id_ == item.tenant_id_ && table_id_ == item.table_id_ && partition_id_ == item.partition_id_ &&
         incarnation_ == item.incarnation_ && backup_set_id_ == item.backup_set_id_;
}

bool ObPGBackupTaskItem::is_valid() const
{
  return is_key_valid() && BackupStatus::MAX != status_;
}

const char* ObPGBackupTaskItem::get_status_str(const BackupStatus& status)
{
  const char* str = "UNKNOWN";
  const char* pg_task_backup_status_strs[] = {"PENDING", "DOING", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_task_backup_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_WARN("invalid backup task status", K(status));
  } else {
    str = pg_task_backup_status_strs[status];
  }
  return str;
}

const char* ObPGBackupTaskItem::get_backup_task_status_str() const
{
  return get_status_str(status_);
}

int ObPGBackupTaskItem::set_backup_task_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup task status get invalid argument", K(ret), KP(buf));
  } else {
    const char* pg_task_backup_status_strs[] = {"PENDING", "DOING", "FINISH", "CANCEL"};
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_task_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(pg_task_backup_status_strs); i++) {
      if (0 == STRCMP(pg_task_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

int ObPGBackupTaskItem::set_trace_id(const char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set trace id get invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (0 == strlen(buf)) {
    // do nothing
  } else if (OB_FAIL(trace_id_.set(buf))) {
    LOG_WARN("failed to set trace id", K(ret), K(buf), K(buf_size));
  }
  return ret;
}

int ObPGBackupTaskItem::get_trace_id(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_trace_id(trace_id_, buf_size, buf))) {
    LOG_WARN("failed to get trace id", K(ret), K(trace_id_), KP(buf), K(buf_size));
  }
  return ret;
}

int ObPGBackupTaskItem::get_trace_id(const ObTaskId& trace_id, const int64_t buf_size, char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    LOG_WARN("get trace id get invalid argument", K(ret), K(trace_id), K(buf_size));
  } else if (trace_id.is_invalid()) {
    // do noting
  } else {
    const int64_t pos = trace_id.to_string(buf, buf_size);
    if (pos > buf_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to change trace id to string", K(ret), K(buf), K(buf_size));
    }
  }
  return ret;
}

static const char* backup_validate_status_strs[] = {
    "SCHEDULE",
    "DOING",
    "FINISHED",
    "CANCEL",
};

ObBackupValidateTenant::ObBackupValidateTenant() : tenant_id_(OB_INVALID_ID), is_dropped_(false)
{}

ObBackupValidateTenant::~ObBackupValidateTenant()
{
  reset();
}

void ObBackupValidateTenant::reset()
{
  tenant_id_ = OB_INVALID_ID;
  is_dropped_ = false;
}

bool ObBackupValidateTenant::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

ObBackupValidateTaskItem::ObBackupValidateTaskItem()
    : job_id_(-1),
      tenant_id_(OB_INVALID_ID),
      tenant_name_(""),
      incarnation_(-1),
      backup_set_id_(-1),
      progress_percent_(0),
      status_(ObBackupValidateTaskItem::MAX)
{}

ObBackupValidateTaskItem::~ObBackupValidateTaskItem()
{
  reset();
}

void ObBackupValidateTaskItem::reset()
{
  job_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  tenant_name_ = "";
  incarnation_ = -1;
  backup_set_id_ = -1;
  progress_percent_ = 0;
  status_ = ObBackupValidateTaskItem::MAX;
}

bool ObBackupValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && OB_INVALID_ID != tenant_id_ && incarnation_ > 0 && backup_set_id_ >= 0 &&
         status_ < ValidateStatus::MAX;
}

bool ObBackupValidateTaskItem::is_same_task(const ObBackupValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_ && incarnation_ == other.incarnation_ &&
         backup_set_id_ == other.backup_set_id_;
}

const char* ObBackupValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = backup_validate_status_strs[status];
  }
  return str;
}

int ObBackupValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_validate_status_strs); ++i) {
      if (0 == STRCMP(backup_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

static const char* tenant_validate_status_strs[] = {
    "SCHEDULE",
    "DOING",
    "FINISHED",
    "CANCEL",
};

ObTenantValidateTaskItem::ObTenantValidateTaskItem()
    : job_id_(-1),
      task_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      backup_set_id_(-1),
      status_(ObTenantValidateTaskItem::MAX),
      backup_dest_(""),
      start_time_(-1),
      end_time_(-1),
      total_pg_count_(-1),
      finish_pg_count_(0),
      total_partition_count_(-1),
      finish_partition_count_(0),
      total_macro_block_count_(-1),
      finish_macro_block_count_(0),
      log_size_(-1),
      result_(-1),
      comment_()
{}

ObTenantValidateTaskItem::~ObTenantValidateTaskItem()
{
  reset();
}

void ObTenantValidateTaskItem::reset()
{
  job_id_ = -1;
  task_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  incarnation_ = -1;
  backup_set_id_ = -1;
  status_ = ObTenantValidateTaskItem::MAX;
  backup_dest_[0] = '\0';
  start_time_ = -1;
  end_time_ = -1;
  total_pg_count_ = -1;
  finish_pg_count_ = -1;
  total_partition_count_ = -1;
  finish_partition_count_ = -1;
  total_macro_block_count_ = -1;
  finish_macro_block_count_ = -1;
  log_size_ = -1;
}

bool ObTenantValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && OB_INVALID_ID != tenant_id_ && incarnation_ > 0 && backup_set_id_ >= 0 &&
         status_ < ObTenantValidateTaskItem::MAX;
}

bool ObTenantValidateTaskItem::is_same_task(const ObTenantValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && task_id_ == other.task_id_ && tenant_id_ == other.tenant_id_ &&
         incarnation_ == other.incarnation_ && backup_set_id_ == other.backup_set_id_;
}

const char* ObTenantValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = tenant_validate_status_strs[status];
  }
  return str;
}

int ObTenantValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_validate_status_strs); ++i) {
      if (0 == STRCMP(tenant_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

static const char* pg_validate_status_strs[] = {
    "PENDING",
    "DOING",
    "FINISHED",
};

ObPGValidateTaskItem::ObPGValidateTaskItem()
    : job_id_(-1),
      task_id_(-1),
      tenant_id_(OB_INVALID_ID),
      table_id_(-1),
      partition_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      archive_round_(-1),
      total_partition_count_(-1),
      finish_partition_count_(-1),
      total_macro_block_count_(-1),
      finish_macro_block_count_(-1),
      log_info_(""),
      log_size_(-1),
      result_(-1),
      trace_id_(),
      status_(ObPGValidateTaskItem::MAX)
{}

ObPGValidateTaskItem::~ObPGValidateTaskItem()
{
  reset();
}

void ObPGValidateTaskItem::reset()
{
  job_id_ = -1;
  task_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  table_id_ = -1;
  partition_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  archive_round_ = -1;
  total_partition_count_ = -1;
  finish_partition_count_ = -1;
  total_macro_block_count_ = -1;
  finish_macro_block_count_ = -1;
  log_info_[0] = '\0';
  log_size_ = -1;
  result_ = -1;
  trace_id_.reset();
  status_ = ObPGValidateTaskItem::MAX;
}

bool ObPGValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && OB_INVALID_ID != tenant_id_ && backup_set_id_ >= 0 &&
         archive_round_ >= 0 /*&& status_ < ObPGValidateTaskItem::MAX*/;
}

bool ObPGValidateTaskItem::is_same_task(const ObPGValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && task_id_ == other.task_id_ && tenant_id_ == other.tenant_id_ &&
         table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && incarnation_ == other.incarnation_ &&
         backup_set_id_ == other.backup_set_id_ && archive_round_ == other.archive_round_;
}

const char* ObPGValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = pg_validate_status_strs[status];
  }
  return str;
}

int ObPGValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(pg_validate_status_strs); ++i) {
      if (0 == STRCMP(pg_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

ObPGValidateTaskRowKey::ObPGValidateTaskRowKey()
    : tenant_id_(0), job_id_(0), task_id_(0), incarnation_(0), backup_set_id_(0), table_id_(0), partition_id_(0)
{}

ObPGValidateTaskRowKey::~ObPGValidateTaskRowKey()
{}

int ObPGValidateTaskRowKey::set(const ObPGValidateTaskInfo* pg_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pg_task)) {
    // use the default value
  } else {
    tenant_id_ = pg_task->tenant_id_;
    job_id_ = pg_task->job_id_;
    task_id_ = pg_task->task_id_;
    incarnation_ = pg_task->incarnation_;
    backup_set_id_ = pg_task->backup_set_id_;
    table_id_ = pg_task->table_id_;
    partition_id_ = pg_task->partition_id_;
  }
  return ret;
}

/* ObBackupBackupsetType */
const char* ObBackupBackupsetType::get_type_str() const
{
  const char* str = "UNKNOWN";
  const char* backup_backupset_type_str[] = {
      "A",
      "S",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_type_str), "type count mismatch");
  if (type_ < 0 || type_ >= MAX) {
    LOG_ERROR("invalid backup type", K(type_));
  } else {
    str = backup_backupset_type_str[type_];
  }
  return str;
}

int ObBackupBackupsetType::set_type(const char* buf)
{
  int ret = OB_SUCCESS;
  BackupBackupsetType tmp_type = MAX;
  const char* backup_backupset_type_str[] = {
      "A",
      "S",
  };
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set type get invalid argument", KR(ret), KP(buf));
  } else {
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_type_str), "type count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_backupset_type_str); ++i) {
      if (0 == STRCMP(backup_backupset_type_str[i], buf)) {
        tmp_type = static_cast<BackupBackupsetType>(i);
      }
    }
    if (MAX == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup backupset type str not found", KR(ret), K(buf));
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

/* ObBackupBackupsetJobItem */
static const char* backup_backupset_job_status_strs[] = {"SCHEDULE", "BACKUP", "CLEAN", "FINISH", "CANCEL"};

ObBackupBackupsetJobItem::ObBackupBackupsetJobItem()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      type_(),
      tenant_name_(),
      job_status_(JobStatus::MAX),
      backup_dest_(),
      max_backup_times_(-1),
      result_(OB_SUCCESS),
      comment_()
{}

ObBackupBackupsetJobItem::~ObBackupBackupsetJobItem()
{}

void ObBackupBackupsetJobItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  job_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  job_status_ = JobStatus::MAX;
  backup_dest_.reset();
  max_backup_times_ = -1;
  result_ = OB_SUCCESS;
  comment_.reset();
}

bool ObBackupBackupsetJobItem::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && backup_set_id_ >= 0 && type_.is_valid() &&
         job_status_ >= ObBackupBackupsetJobItem::SCHEDULE && job_status_ < ObBackupBackupsetJobItem::MAX;
}

int ObBackupBackupsetJobItem::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup backupset job status get invalid argument", KR(ret), KP(buf));
  } else {
    JobStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_job_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_backupset_job_status_strs); i++) {
      if (0 == STRCMP(backup_backupset_job_status_strs[i], buf)) {
        tmp_status = static_cast<JobStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", KR(ret), K(buf));
    } else {
      job_status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObBackupBackupsetJobItem::get_status_str(const JobStatus& status)
{
  const char* str = "UNKNOWN";
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_job_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup backupset job info status", K(status));
  } else {
    str = backup_backupset_job_status_strs[status];
  }
  return str;
}

/* ObTenantBackupBackupsetTaskItem */
ObTenantBackupBackupsetTaskItem::ObTenantBackupBackupsetTaskItem()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      backup_type_(),
      snapshot_version_(-1),
      prev_full_backup_set_id_(-1),
      prev_inc_backup_set_id_(-1),
      prev_backup_data_version_(-1),
      input_bytes_(0),
      output_bytes_(0),
      start_ts_(-1),
      end_ts_(-1),
      compatible_(false),
      cluster_id_(-1),
      cluster_version_(-1),
      task_status_(TaskStatus::MAX),
      src_backup_dest_(),
      dst_backup_dest_(),
      backup_data_version_(-1),
      backup_schema_version_(-1),
      total_pg_count_(-1),
      finish_pg_count_(-1),
      total_partition_count_(-1),
      finish_partition_count_(-1),
      total_macro_block_count_(-1),
      finish_macro_block_count_(-1),
      result_(-1),
      encryption_mode_(),
      passwd_(),
      is_mark_deleted_(false),
      start_replay_log_ts_(0),
      date_(0)
{}

ObTenantBackupBackupsetTaskItem::~ObTenantBackupBackupsetTaskItem()
{}

void ObTenantBackupBackupsetTaskItem::reset()
{}

bool ObTenantBackupBackupsetTaskItem::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && backup_set_id_ >= 0 && copy_id_ >= 0 &&
         backup_type_.is_valid() && task_status_ >= ObTenantBackupBackupsetTaskItem::GENERATE &&
         task_status_ < ObTenantBackupBackupsetTaskItem::MAX;
}

int ObTenantBackupBackupsetTaskItem::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tenant backup backupset task status get invalid argument", KR(ret), KP(buf));
  } else {
    const char* tenant_backup_backupset_task_status_strs[] = {"GENERATE", "BACKUP", "FINISH", "CANCEL"};
    TaskStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_backup_backupset_task_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_backup_backupset_task_status_strs); i++) {
      if (0 == STRCMP(tenant_backup_backupset_task_status_strs[i], buf)) {
        tmp_status = static_cast<TaskStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", KR(ret), K(buf));
    } else {
      task_status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObTenantBackupBackupsetTaskItem::get_status_str(const TaskStatus& status)
{
  const char* str = "UNKNOWN";
  const char* backup_backupset_task_status_strs[] = {"GENERATE", "BACKUP", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_task_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup backupset task info status", K(status));
  } else {
    str = backup_backupset_task_status_strs[status];
  }
  return str;
}

int ObTenantBackupBackupsetTaskItem::convert_to_backup_task_info(ObTenantBackupTaskInfo& info) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("self should be valid", KR(ret), K(this));
  } else {
    info.tenant_id_ = tenant_id_;
    info.backup_set_id_ = backup_set_id_;
    info.incarnation_ = incarnation_;
    info.snapshot_version_ = snapshot_version_;
    info.prev_full_backup_set_id_ = prev_full_backup_set_id_;
    info.prev_inc_backup_set_id_ = prev_inc_backup_set_id_;
    info.prev_backup_data_version_ = prev_backup_data_version_;
    info.pg_count_ = total_pg_count_;
    info.macro_block_count_ = total_macro_block_count_;
    info.finish_pg_count_ = finish_pg_count_;
    info.finish_macro_block_count_ = finish_macro_block_count_;
    info.input_bytes_ = input_bytes_;
    info.output_bytes_ = output_bytes_;
    info.start_time_ = start_ts_;
    info.end_time_ = end_ts_;
    info.compatible_ = compatible_;
    info.cluster_version_ = cluster_version_;
    info.backup_type_ = backup_type_;
    if (TaskStatus::GENERATE == task_status_) {
      info.status_ = ObTenantBackupTaskInfo::GENERATE;
    } else if (TaskStatus::BACKUP == task_status_) {
      info.status_ = ObTenantBackupTaskInfo::DOING;
    } else if (TaskStatus::FINISH == task_status_) {
      info.status_ = ObTenantBackupTaskInfo::FINISH;
    } else if (TaskStatus::CANCEL == task_status_) {
      info.status_ = ObTenantBackupTaskInfo::CANCEL;
    }

    info.device_type_ = dst_backup_dest_.device_type_;
    info.result_ = result_;
    info.cluster_id_ = cluster_id_;
    info.backup_dest_ = dst_backup_dest_;
    info.backup_data_version_ = backup_data_version_;
    info.backup_schema_version_ = backup_schema_version_;
    info.partition_count_ = total_partition_count_;
    info.finish_partition_count_ = total_partition_count_;
    info.encryption_mode_ = encryption_mode_;
    info.passwd_ = passwd_;
    info.is_mark_deleted_ = is_mark_deleted_;
    info.start_replay_log_ts_ = start_replay_log_ts_;
    info.date_ = date_;
    info.copy_id_ = copy_id_;
  }
  return ret;
}

/* ObPGBackupBackupsetTaskRowKey */
ObPGBackupBackupsetTaskRowKey::ObPGBackupBackupsetTaskRowKey()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      table_id_(-1),
      partition_id_(-1)
{}

ObPGBackupBackupsetTaskRowKey::~ObPGBackupBackupsetTaskRowKey()
{}

int ObPGBackupBackupsetTaskRowKey::set(const uint64_t tenant_id, const int64_t job_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t copy_id, const int64_t table_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  job_id_ = job_id;
  incarnation_ = incarnation;
  backup_set_id_ = backup_set_id;
  copy_id_ = copy_id;
  table_id_ = table_id;
  partition_id_ = partition_id;
  return ret;
}

void ObPGBackupBackupsetTaskRowKey::reset()
{
  tenant_id_ = OB_INVALID_ID;
  job_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  copy_id_ = -1;
  table_id_ = -1;
  partition_id_ = -1;
}

void ObPGBackupBackupsetTaskRowKey::reuse()
{
  tenant_id_ = 0;
  job_id_ = 0;
  incarnation_ = 0;
  backup_set_id_ = 0;
  copy_id_ = 0;
  table_id_ = 0;
  partition_id_ = 0;
}

bool ObPGBackupBackupsetTaskRowKey::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && backup_set_id_ > 0 && copy_id_ > 0;
}

ObPGBackupBackupsetTaskStat::ObPGBackupBackupsetTaskStat()
    : total_partition_count_(0), total_macro_block_count_(0), finish_partition_count_(0), finish_macro_block_count_(0)
{}

ObPGBackupBackupsetTaskStat::~ObPGBackupBackupsetTaskStat()
{}

int ObPGBackupBackupsetTaskStat::set(const int64_t total_partition_cnt, const int64_t total_macro_block_cnt,
    const int64_t finish_partition_cnt, const int64_t finish_macro_block_cnt)
{
  int ret = OB_SUCCESS;
  total_partition_count_ = total_partition_cnt;
  total_macro_block_count_ = total_macro_block_cnt;
  finish_partition_count_ = finish_partition_cnt;
  finish_macro_block_count_ = finish_macro_block_cnt;
  return ret;
}

void ObPGBackupBackupsetTaskStat::reset()
{
  total_partition_count_ = 0;
  total_macro_block_count_ = 0;
  finish_partition_count_ = 0;
  finish_macro_block_count_ = 0;
}

bool ObPGBackupBackupsetTaskStat::is_valid() const
{
  return finish_partition_count_ >= 0 && finish_macro_block_count_ >= 0;
}

/* ObPGBackupBackupsetTaskItem */
ObPGBackupBackupsetTaskItem::ObPGBackupBackupsetTaskItem()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      table_id_(-1),
      partition_id_(-1),
      status_(ObPGBackupBackupsetTaskItem::MAX),
      trace_id_(),
      server_(),
      total_partition_count_(0),
      finish_partition_count_(0),
      total_macro_block_count_(0),
      finish_macro_block_count_(0),
      result_(0)
{}

ObPGBackupBackupsetTaskItem::~ObPGBackupBackupsetTaskItem()
{}

void ObPGBackupBackupsetTaskItem::reset()
{}

bool ObPGBackupBackupsetTaskItem::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && backup_set_id_ > 0 && copy_id_ > 0 &&
         status_ >= ObPGBackupBackupsetTaskItem::PENDING && status_ < ObPGBackupBackupsetTaskItem::MAX;
}

int ObPGBackupBackupsetTaskItem::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set pg backup backupset task status get invalid argument", KR(ret), KP(buf));
  } else {
    const char* pg_backup_backupset_task_status_strs[] = {"PENDING", "DOING", "FINISH"};
    TaskStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_backup_backupset_task_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(pg_backup_backupset_task_status_strs); i++) {
      if (0 == STRCMP(pg_backup_backupset_task_status_strs[i], buf)) {
        tmp_status = static_cast<TaskStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", KR(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObPGBackupBackupsetTaskItem::get_status_str(const TaskStatus& status)
{
  const char* str = "UNKNOWN";
  const char* pg_backup_backupset_task_status_strs[] = {"PENDING", "DOING", "FINISH"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_backup_backupset_task_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup backupset task info status", K(status));
  } else {
    str = pg_backup_backupset_task_status_strs[status];
  }
  return str;
}

int ObPGBackupBackupsetTaskItem::set_trace_id(const char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set trace id get invalid argument", KR(ret), KP(buf), K(buf_size));
  } else if (0 == strlen(buf)) {
    // do nothing
  } else if (OB_FAIL(trace_id_.set(buf))) {
    LOG_WARN("failed to set trace id", K(ret), K(buf), K(buf_size));
  }
  return ret;
}

int ObPGBackupBackupsetTaskItem::get_trace_id(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_trace_id(trace_id_, buf_size, buf))) {
    LOG_WARN("failed to get trace id", KR(ret), K(trace_id_), KP(buf), K(buf_size));
  }
  return ret;
}

int ObPGBackupBackupsetTaskItem::get_trace_id(const ObTaskId& trace_id, const int64_t buf_size, char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    LOG_WARN("get trace id get invalid argument", KR(ret), K(trace_id), K(buf_size));
  } else if (trace_id.is_invalid()) {
    // do noting
  } else {
    const int64_t pos = trace_id.to_string(buf, buf_size);
    if (pos > buf_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to change trace id to string", K(ret), K(buf), K(buf_size));
    }
  }
  return ret;
}

/* ObBackupBackupPieceJobInfo */
ObBackupBackupPieceJobInfo::ObBackupBackupPieceJobInfo()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      piece_id_(-1),
      status_(ObBackupBackupPieceJobInfo::MAX),
      max_backup_times_(-1),
      result_(OB_SUCCESS),
      comment_(),
      type_()
{}

void ObBackupBackupPieceJobInfo::reset()
{
  tenant_id_ = OB_SYS_TENANT_ID;
  job_id_ = -1;
  incarnation_ = -1;
  piece_id_ = -1;
  status_ = ObBackupBackupPieceJobInfo::MAX;
  max_backup_times_ = -1;
  result_ = OB_SUCCESS;
  comment_.reset();
  type_ = JOB_TYPE_MAX;
}

bool ObBackupBackupPieceJobInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 &&
         piece_id_ >= 0;  // equals 0 mean backup backuppiece all
}

const char* backup_backuppiece_job_status_strs[] = {
    "SCHEDULE",
    "DOING",
    "CANCEL",
    "FINISH",
};

int ObBackupBackupPieceJobInfo::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set job status get invalid argument", K(ret), KP(buf));
  } else {
    JobStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_backuppiece_job_status_strs); i++) {
      if (0 == STRCMP(backup_backuppiece_job_status_strs[i], buf)) {
        tmp_status = static_cast<JobStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("job status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObBackupBackupPieceJobInfo::get_status_str() const
{
  const char* str = "UNKNOWN";
  if (status_ < 0 || status_ >= MAX) {
    LOG_ERROR("invalid backup info status", K(status_));
  } else {
    str = backup_backuppiece_job_status_strs[status_];
  }
  return str;
}

bool ObBackupBackupPieceJobInfo::with_active_piece() const
{
  return type_ == WITH_ACTIVE_PIECE;
}

/* ObBackupBackupPieceTaskInfo */
ObBackupBackupPieceTaskInfo::ObBackupBackupPieceTaskInfo()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      round_id_(-1),
      piece_id_(-1),
      copy_id_(-1),
      task_status_(ObBackupBackupPieceTaskInfo::MAX),
      start_ts_(-1),
      end_ts_(-1),
      result_(0)
{}

void ObBackupBackupPieceTaskInfo::reset()
{
  tenant_id_ = OB_SYS_TENANT_ID;
  job_id_ = -1;
  incarnation_ = -1;
  round_id_ = -1;
  piece_id_ = -1;
  copy_id_ = -1;
  task_status_ = ObBackupBackupPieceTaskInfo::MAX;
  start_ts_ = -1;
  end_ts_ = -1;
  result_ = 0;
}

bool ObBackupBackupPieceTaskInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && round_id_ > 0 && piece_id_ > 0 &&
         copy_id_ > 0;
}
static const char* backup_backuppiece_task_status_strs[] = {
    "DOING",
    "FINISH",
};

int ObBackupBackupPieceTaskInfo::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set job status get invalid argument", K(ret), KP(buf));
  } else {

    TaskStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_backuppiece_task_status_strs); i++) {
      if (0 == STRCMP(backup_backuppiece_task_status_strs[i], buf)) {
        tmp_status = static_cast<TaskStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("task status str not found", K(ret), K(buf));
    } else {
      task_status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObBackupBackupPieceTaskInfo::get_status_str() const
{
  const char* str = "UNKNOWN";
  if (task_status_ < 0 || task_status_ >= MAX) {
    LOG_ERROR("invalid backup info status", K(task_status_));
  } else {
    str = backup_backuppiece_task_status_strs[task_status_];
  }
  return str;
}

int ObBackupBackupPieceTaskInfo::get_backup_piece_key(share::ObBackupPieceInfoKey& key) const
{
  int ret = OB_SUCCESS;
  key.incarnation_ = incarnation_;
  key.tenant_id_ = tenant_id_;
  key.round_id_ = round_id_;
  key.backup_piece_id_ = piece_id_;
  key.copy_id_ = copy_id_;
  return ret;
}

int ObBackupUtils::get_backup_info_default_timeout_ctx(ObTimeoutCtx& ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 2 * 1000 * 1000;  // 2s
  int64_t abs_timeout_us = ctx.get_abs_timeout();
  int64_t worker_timeout_us = THIS_WORKER.get_timeout_ts();

  if (abs_timeout_us < 0) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  }

  if (INT64_MAX == worker_timeout_us) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  } else if (worker_timeout_us > 0 && worker_timeout_us < abs_timeout_us) {
    abs_timeout_us = worker_timeout_us;
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }
  return ret;
}

bool ObBackupUtils::is_need_retry_error(const int err)
{
  bool bret = true;
  switch (err) {
    case OB_NOT_INIT:
    case OB_INVALID_ARGUMENT:
    case OB_ERR_UNEXPECTED:
    case OB_ERR_SYS:
    case OB_INIT_TWICE:
    case OB_SRC_DO_NOT_ALLOWED_MIGRATE:
    case OB_CANCELED:
    case OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT:
    case OB_LOG_ARCHIVE_STAT_NOT_MATCH:
    case OB_NOT_SUPPORTED:
    case OB_TENANT_HAS_BEEN_DROPPED:
    case OB_CS_OUTOF_DISK_SPACE:
    case OB_HASH_NOT_EXIST:
    case OB_ARCHIVE_LOG_NOT_CONTINUES_WITH_DATA:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}

bool ObBackupUtils::is_extern_device_error(const int err)
{
  bool bret = false;
  switch (err) {
    case OB_IO_ERROR:
    case OB_OSS_ERROR:
    case OB_BACKUP_IO_PROHIBITED:
      bret = true;
      break;
    default:
      break;
  }
  return bret;
}

int ObBackupUtils::retry_get_tenant_schema_guard(const uint64_t tenant_id,
    schema::ObMultiVersionSchemaService& schema_service, const int64_t tenant_schema_version,
    schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_mode =
      schema::ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_FALLBACK;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || tenant_schema_version < OB_INVALID_VERSION)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tenant_schema_version));
  } else {
    int retry_times = 0;
    const int64_t sys_schema_version = OB_INVALID_VERSION;  // sys_schema_version use latest
    while (retry_times < MAX_RETRY_TIMES) {
      if (OB_FAIL(schema_service.get_tenant_schema_guard(
              tenant_id, schema_guard, tenant_schema_version, sys_schema_version, refresh_mode))) {
        STORAGE_LOG(WARN,
            "fail to get schema, sleep 1s and retry",
            K(ret),
            K(tenant_id),
            K(tenant_schema_version),
            K(sys_schema_version));
        usleep(RETRY_INTERVAL);
      } else {
        break;
      }
      ++retry_times;
    }
  }
  return ret;
}

int ObBackupUtils::get_snapshot_to_time_date(const int64_t snapshot_version, int64_t& date)
{
  int ret = OB_SUCCESS;
  date = 0;
  static const int64_t MAX_BUF_LENGTH = 64;
  char buf[MAX_BUF_LENGTH] = {0};
  int64_t tmp_date = 0;

  if (snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get snapshot to time str get invalid argument", K(ret), K(snapshot_version));
  } else {
    const time_t rawtime = snapshot_version / (1000 * 1000);  // second
    struct tm time_info;
    struct tm* time_info_ptr = NULL;

    if (NULL == (time_info_ptr = (localtime_r(&rawtime, &time_info)))) {
      ret = OB_ERR_SYS;
      LOG_WARN("get localtime failed", K(ret));
    } else if (0 == strftime(buf, MAX_BUF_LENGTH, "%Y%m%d", time_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to strftime", K(ret), K(buf), K(snapshot_version));
    } else if (OB_FAIL(ob_atoll(buf, tmp_date))) {
      STORAGE_LOG(WARN, "failed to atoll", K(ret), K(buf));
    } else {
      date = tmp_date;
    }
  }
  return ret;
}

int ObBackupUtils::check_user_tenant_gts(
    schema::ObMultiVersionSchemaService& schema_service, const ObIArray<uint64_t>& tenant_ids, bool& is_gts)
{
  int ret = OB_SUCCESS;
  is_gts = true;

  for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret) && is_gts; ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    if (!is_valid_no_sys_tenant_id(tenant_id)) {
      // sys tenants has gts
    } else if (OB_FAIL(check_gts(schema_service, tenant_id, is_gts))) {
      LOG_WARN("failed to check gts", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObBackupUtils::check_gts(
    schema::ObMultiVersionSchemaService& schema_service, const uint64_t tenant_id, bool& is_gts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t gts_type = oceanbase::transaction::TS_SOURCE_LTS;
  bool is_dropped = false;
  schema::ObSchemaGetterGuard schema_guard;

  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard to determine tenant gts type", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_timestamp_service_type(tenant_id, gts_type))) {
    LOG_WARN("fail to get tenant gts switch", KR(ret), K(tenant_id));
  } else if (!oceanbase::transaction::is_ts_type_external_consistent(gts_type)) {
    is_gts = false;
    LOG_INFO("tenant is not gts", K(tenant_id));
  }
  DEBUG_SYNC(BEFORE_BACKUP_DROP_TENANT);
  if (OB_FAIL(ret)) {
    // ignore restoring and dropped tenant.
    const schema::ObSimpleTenantSchema *tenant_schema = NULL;
    if (OB_SUCCESS != (tmp_ret = schema_service.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
      LOG_WARN("fail to check if tenant has been dropped", K(tmp_ret), K(tenant_id));
    } else if (is_dropped) {
      ret = OB_SUCCESS;
      LOG_INFO("tenant has been dropped, no need to check gts", K(tenant_id));
    } else if (OB_SUCCESS != (tmp_ret = (schema_guard.get_tenant_info(tenant_id, tenant_schema)))) {
      LOG_WARN("failed to get tenant info", K(tmp_ret), K(tenant_id));
    } else if (tenant_schema->is_restore()) {
      ret = OB_SUCCESS;
      LOG_INFO("tenant is restoring, no need to check gts", K(tenant_id));
    }
  }
  return ret;
}

bool ObBackupUtils::can_backup_pieces_be_deleted(const ObBackupPieceStatus::STATUS& status)
{
  return ObBackupPieceStatus::BACKUP_PIECE_INACTIVE == status || ObBackupPieceStatus::BACKUP_PIECE_FROZEN == status;
}

int ObBackupUtils::check_passwd(const char* passwd_array, const char* passwd)
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

int ObBackupUtils::check_is_tmp_file(const common::ObString& file_name, bool& is_tmp_file)
{
  int ret = OB_SUCCESS;
  is_tmp_file = false;
  char* buf = NULL;
  ObArenaAllocator allocator;
  if (file_name.empty()) {
    // no need check
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_name.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(file_name));
  } else {
    STRNCPY(buf, file_name.ptr(), file_name.length());
    buf[file_name.length()] = '\0';
    char* ptr = STRSTR(buf, OB_STR_TMP_FILE_MARK);
    if (OB_ISNULL(ptr)) {
      is_tmp_file = false;
    } else {
      ptr = ptr + STRLEN(OB_STR_TMP_FILE_MARK);
      bool is_valid_tmp_file = false;
      while (OB_NOT_NULL(ptr)) {
        if ('\0' == *ptr) {
          break;  // reach str end
        } else if (!isdigit(*ptr)) {
          is_valid_tmp_file = false;
          break;
        } else {
          is_valid_tmp_file = true;
        }
        ++ptr;
      }
      if (is_valid_tmp_file) {
        is_tmp_file = true;
      } else {
        is_tmp_file = false;
      }
    }
  }
  return ret;
}

ObPhysicalRestoreInfo::ObPhysicalRestoreInfo()
    : cluster_id_(0),
      incarnation_(0),
      tenant_id_(0),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      log_archive_round_(0),
      restore_snapshot_version_(OB_INVALID_TIMESTAMP),
      restore_start_ts_(0),
      compatible_(0),
      cluster_version_(0),
      multi_restore_path_list_(),
      backup_date_(0),
      restore_schema_version_(OB_INVALID_VERSION)
{
  backup_dest_[0] = '\0';
  cluster_name_[0] = '\0';
}

void ObPhysicalRestoreInfo::reset()
{
  backup_dest_[0] = '\0';
  cluster_name_[0] = '\0';
  cluster_id_ = 0;
  incarnation_ = 0;
  tenant_id_ = 0;
  full_backup_set_id_ = 0;
  inc_backup_set_id_ = 0;
  log_archive_round_ = 0;
  restore_snapshot_version_ = OB_INVALID_TIMESTAMP;
  restore_start_ts_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  multi_restore_path_list_.reset();
  backup_date_ = 0;
  restore_schema_version_ = OB_INVALID_VERSION;
}

bool ObPhysicalRestoreInfo::is_valid() const
{
  return !(0 == strlen(backup_dest_) && 0 == multi_restore_path_list_.get_backup_set_path_list().count()) &&
         strlen(cluster_name_) > 0 && cluster_id_ > 0 && OB_START_INCARNATION == incarnation_ && tenant_id_ > 0 &&
         full_backup_set_id_ > 0 && inc_backup_set_id_ > 0 && log_archive_round_ > 0 && restore_snapshot_version_ > 0 &&
         restore_start_ts_ > 0 && compatible_ > 0 && cluster_version_ > 0 && backup_date_ >= 0;
}

void ObPhysicalRestoreInfo::set_array_label(const char* lable)
{
  UNUSED(lable);
  // backup_piece_list_.set_label(lable);
  // backup_set_list_.set_label(lable);
}

bool ObPhysicalRestoreInfo::is_switch_piece_mode() const
{
  bool b_ret = false;
  if (multi_restore_path_list_.get_backup_piece_path_list().count() > 0) {
    b_ret =
        (OB_BACKUP_NO_SWITCH_PIECE_ID != multi_restore_path_list_.get_backup_piece_path_list().at(0).backup_piece_id_);
  }
  return b_ret;
}

int ObPhysicalRestoreInfo::assign(const ObPhysicalRestoreInfo& other)
{
  int ret = OB_SUCCESS;
  STRNCPY(backup_dest_, other.backup_dest_, share::OB_MAX_BACKUP_DEST_LENGTH);
  STRNCPY(cluster_name_, other.cluster_name_, common::OB_MAX_CLUSTER_NAME_LENGTH);
  cluster_id_ = other.cluster_id_;
  incarnation_ = other.incarnation_;
  tenant_id_ = other.tenant_id_;
  full_backup_set_id_ = other.full_backup_set_id_;
  inc_backup_set_id_ = other.inc_backup_set_id_;
  log_archive_round_ = other.log_archive_round_;
  restore_snapshot_version_ = other.restore_snapshot_version_;
  restore_start_ts_ = other.restore_start_ts_;
  compatible_ = other.compatible_;
  cluster_version_ = other.cluster_version_;
  backup_date_ = other.backup_date_;
  restore_schema_version_ = other.restore_schema_version_;
  if (OB_FAIL(multi_restore_path_list_.assign(other.multi_restore_path_list_))) {
    LOG_WARN("failed to assign multi restore path list", KR(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalRestoreInfo, backup_dest_, cluster_name_, cluster_id_, incarnation_, tenant_id_,
    full_backup_set_id_, inc_backup_set_id_, log_archive_round_, restore_snapshot_version_, restore_start_ts_,
    compatible_, cluster_version_, multi_restore_path_list_, backup_date_, restore_schema_version_);

ObRestoreBackupInfo::ObRestoreBackupInfo()
    : compat_mode_(Worker::CompatMode::INVALID),
      snapshot_version_(0),
      schema_version_(0),
      frozen_data_version_(0),
      frozen_snapshot_version_(0),
      frozen_schema_version_(0),
      physical_restore_info_(),
      sys_pg_key_list_()
{
  locality_[0] = '\0';
  primary_zone_[0] = '\0';
}

bool ObRestoreBackupInfo::is_valid() const
{
  return Worker::CompatMode::INVALID != compat_mode_ && snapshot_version_ > 0 && schema_version_ > 0 &&
         frozen_data_version_ > 0 && frozen_snapshot_version_ > 0 && frozen_schema_version_ > 0 &&
         physical_restore_info_.is_valid();
}

ObPhysicalRestoreArg::ObPhysicalRestoreArg() : restore_info_(), pg_key_(), restore_data_version_(0)
{}

ObPhysicalRestoreArg::ObPhysicalRestoreArg(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_ERROR("failed to assign arg", K(ret), K(other));
  }
}

ObPhysicalRestoreArg& ObPhysicalRestoreArg::operator=(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_ERROR("failed to assign arg", K(ret), K(other));
  }
  return *this;
}

bool ObPhysicalRestoreArg::is_valid() const
{
  return restore_info_.is_valid() && pg_key_.is_valid() && restore_data_version_ > 0;
}

int ObPhysicalRestoreArg::assign(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_.assign(other.restore_info_))) {
    LOG_WARN("fail to assign restore info", K(ret), K(other));
  } else {
    pg_key_ = other.pg_key_;
    restore_data_version_ = other.restore_data_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalRestoreArg, restore_info_, pg_key_, restore_data_version_);

ObPhysicalValidateArg::ObPhysicalValidateArg()
    : backup_dest_(""),
      cluster_name_(""),
      uri_header_(""),
      storage_info_(""),
      job_id_(-1),
      task_id_(-1),
      trace_id_(),
      server_(),
      cluster_id_(-1),
      pg_key_(),
      table_id_(-1),
      partition_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      archive_round_(-1),
      backup_set_id_(-1),
      total_partition_count_(-1),
      total_macro_block_count_(-1),
      clog_end_timestamp_(-1),
      start_log_id_(-1),
      end_log_id_(-1),
      log_size_(-1),
      is_dropped_tenant_(false),
      need_validate_clog_(true),
      full_backup_set_id_(-1),
      inc_backup_set_id_(-1),
      cluster_version_(0),
      backup_date_(0),
      compatible_(0)
{}

ObPhysicalValidateArg& ObPhysicalValidateArg::operator=(const ObPhysicalValidateArg& other)
{
  if (this != &other) {
    STRNCPY(backup_dest_, other.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH);
    STRNCPY(cluster_name_, other.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH);
    STRNCPY(uri_header_, other.uri_header_, OB_MAX_URI_HEADER_LENGTH);
    STRNCPY(storage_info_, other.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    job_id_ = other.job_id_;
    task_id_ = other.task_id_;
    trace_id_.set(other.trace_id_);
    // server_
    cluster_id_ = other.cluster_id_;
    pg_key_ = other.pg_key_;
    table_id_ = other.table_id_;
    partition_id_ = other.partition_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_ = other.incarnation_;
    archive_round_ = other.archive_round_;
    backup_set_id_ = other.backup_set_id_;
    total_partition_count_ = other.total_partition_count_;
    total_macro_block_count_ = other.total_macro_block_count_;
    clog_end_timestamp_ = other.clog_end_timestamp_;
    start_log_id_ = other.start_log_id_;
    end_log_id_ = other.end_log_id_;
    log_size_ = other.log_size_;
    is_dropped_tenant_ = other.is_dropped_tenant_;
    need_validate_clog_ = other.need_validate_clog_;
    full_backup_set_id_ = other.full_backup_set_id_;
    inc_backup_set_id_ = other.inc_backup_set_id_;
    cluster_version_ = other.cluster_version_;
    backup_date_ = other.backup_date_;
    compatible_ = other.compatible_;
  }
  return *this;
}

int ObPhysicalValidateArg::assign(const ObPhysicalValidateArg& other)
{
  int ret = OB_SUCCESS;
  *this = other;
  return ret;
}

bool ObPhysicalValidateArg::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && cluster_id_ >= 0 && table_id_ >= 0 && OB_INVALID_ID != tenant_id_ &&
         incarnation_ >= 0 && archive_round_ >= 0 && backup_set_id_ >= 0 && total_partition_count_ >= 0 &&
         total_macro_block_count_ >= 0 && clog_end_timestamp_ >= 0 && start_log_id_ >= 0 && end_log_id_ >= 0 &&
         log_size_ >= 0 && cluster_version_ > 0 && backup_date_ >= 0 &&
         compatible_ >= OB_BACKUP_COMPATIBLE_VERSION_V1 && compatible_ < OB_BACKUP_COMPATIBLE_VERSION_MAX;
}

int ObPhysicalValidateArg::get_validate_pgkey(common::ObPartitionKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_key.init(pg_key_.get_table_id(), pg_key_.get_partition_id(), 0))) {
    STORAGE_LOG(WARN, "init pg key failed", K(ret), K(pg_key_));
  }
  return ret;
}

int ObPhysicalValidateArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalValidateArg", K(ret), K(*this));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest_, cluster_name_, cluster_id_, incarnation_))) {
    STORAGE_LOG(WARN, "failed to set backup dest", K(ret), K(*this));
  } else {
    path_info.tenant_id_ = tenant_id_;
    path_info.full_backup_set_id_ = full_backup_set_id_;
    path_info.inc_backup_set_id_ = inc_backup_set_id_;
    path_info.compatible_ = compatible_;
    path_info.backup_date_ = backup_date_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalValidateArg, backup_dest_, cluster_name_, uri_header_, storage_info_, job_id_, task_id_,
    trace_id_, server_, cluster_id_, pg_key_, table_id_, partition_id_, tenant_id_, incarnation_, archive_round_,
    backup_set_id_, total_partition_count_, total_macro_block_count_, clog_end_timestamp_, start_log_id_, end_log_id_,
    log_size_, is_dropped_tenant_, need_validate_clog_, full_backup_set_id_, inc_backup_set_id_, cluster_version_,
    backup_date_, compatible_);

ObPhysicalBackupArg::ObPhysicalBackupArg()
    : uri_header_(),
      storage_info_(),
      incarnation_(0),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      backup_data_version_(0),
      backup_schema_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_data_version_(0),
      task_id_(0),
      backup_type_(ObBackupType::MAX),
      backup_snapshot_version_(0),
      backup_date_(0),
      prev_backup_date_(0),
      compatible_(0)
{}

void ObPhysicalBackupArg::reset()
{
  MEMSET(uri_header_, 0, sizeof(uri_header_));
  MEMSET(storage_info_, 0, sizeof(storage_info_));
  incarnation_ = 0;
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  backup_data_version_ = 0;
  backup_schema_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_data_version_ = 0;
  task_id_ = 0;
  backup_type_ = ObBackupType::MAX;
  backup_snapshot_version_ = 0;
  backup_date_ = 0;
  prev_backup_date_ = 0;
  compatible_ = 0;
}

bool ObPhysicalBackupArg::is_valid() const
{
  bool ret = true;
  if (STRLEN(uri_header_) == 0 || incarnation_ < 0 || tenant_id_ == OB_INVALID_ID || backup_set_id_ <= 0 ||
      backup_data_version_ <= 0 || backup_schema_version_ <= 0 || prev_full_backup_set_id_ < 0 ||
      prev_inc_backup_set_id_ < 0 || task_id_ < 0 ||
      (backup_type_ >= ObBackupType::MAX && backup_type_ < ObBackupType::FULL_BACKUP) ||
      backup_snapshot_version_ <= 0 || backup_date_ <= 0 ||
      (ObBackupType::INCREMENTAL_BACKUP == backup_type_ && prev_backup_date_ <= 0) ||
      compatible_ < OB_BACKUP_COMPATIBLE_VERSION_V1 || compatible_ > OB_BACKUP_COMPATIBLE_VERSION_MAX) {
    ret = false;
  }
  return ret;
}

bool ObPhysicalBackupArg::is_incremental_backup() const
{
  return ObBackupType::INCREMENTAL_BACKUP == backup_type_;
}

int ObPhysicalBackupArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(uri_header_, storage_info_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(*this));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
  } else {
    path_info.tenant_id_ = tenant_id_;
    int64_t full_backup_set_id = 0;
    int64_t inc_backup_set_id = 0;
    if (ObBackupType::FULL_BACKUP == backup_type_) {
      full_backup_set_id = backup_set_id_;
      inc_backup_set_id = backup_set_id_;
    } else if (ObBackupType::INCREMENTAL_BACKUP == backup_type_) {
      full_backup_set_id = prev_full_backup_set_id_;
      inc_backup_set_id = backup_set_id_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected ObBackupType", K(ret), K(*this));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(path_info.set(cluster_backup_dest,
                   tenant_id_,
                   full_backup_set_id,
                   inc_backup_set_id,
                   backup_date_,
                   compatible_))) {
      LOG_WARN("failed to set backup path info", K(ret), K(cluster_backup_dest));
    }
  }
  return ret;
}

int ObPhysicalBackupArg::get_prev_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_UNLIKELY(ObBackupType::INCREMENTAL_BACKUP != backup_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "only incremental backup has prev backup data", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(uri_header_, storage_info_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(*this));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(path_info.set(cluster_backup_dest,
                 tenant_id_,
                 prev_full_backup_set_id_,
                 prev_inc_backup_set_id_,
                 prev_backup_date_,
                 compatible_))) {
    LOG_WARN("failed to set backup path info", K(ret), K(cluster_backup_dest));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalBackupArg, storage_info_, uri_header_, incarnation_, tenant_id_, backup_set_id_,
    backup_data_version_, backup_schema_version_, prev_full_backup_set_id_, prev_inc_backup_set_id_, prev_data_version_,
    task_id_, backup_type_, backup_date_, prev_backup_date_, compatible_, backup_snapshot_version_);

OB_SERIALIZE_MEMBER(ObBackupBackupsetArg, src_uri_header_, src_storage_info_, dst_uri_header_, dst_storage_info_,
    cluster_name_, cluster_id_, job_id_, tenant_id_, incarnation_, backup_set_id_, copy_id_, backup_data_version_,
    backup_schema_version_, prev_full_backup_set_id_, prev_inc_backup_set_id_, pg_key_, server_, backup_type_,
    delete_input_, cluster_version_, tenant_dropped_, backup_date_, compatible_);

ObBackupBackupsetArg::ObBackupBackupsetArg()
    : src_uri_header_(),
      src_storage_info_(),
      dst_uri_header_(),
      dst_storage_info_(),
      cluster_name_(),
      cluster_id_(-1),
      job_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      backup_data_version_(-1),
      backup_schema_version_(-1),
      prev_full_backup_set_id_(-1),
      prev_inc_backup_set_id_(-1),
      prev_data_version_(-1),
      pg_key_(),
      server_(),
      backup_type_(ObBackupType::MAX),
      delete_input_(false),
      cluster_version_(0),
      tenant_dropped_(false),
      backup_date_(0),
      compatible_(0)
{}

void ObBackupBackupsetArg::reset()
{
  MEMSET(src_uri_header_, 0, sizeof(src_uri_header_));
  MEMSET(src_storage_info_, 0, sizeof(src_storage_info_));
  MEMSET(dst_uri_header_, 0, sizeof(dst_uri_header_));
  MEMSET(dst_storage_info_, 0, sizeof(dst_storage_info_));
  MEMSET(cluster_name_, 0, sizeof(cluster_name_));
  job_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  cluster_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  copy_id_ = -1;
  prev_full_backup_set_id_ = -1;
  prev_inc_backup_set_id_ = -1;
  prev_data_version_ = -1;
  pg_key_.reset();
  server_.reset();
  backup_type_ = ObBackupType::MAX;
  delete_input_ = false;
  cluster_version_ = 0;
  tenant_dropped_ = false;
  backup_date_ = 0;
  compatible_ = 0;
}

bool ObBackupBackupsetArg::is_valid() const
{
  return job_id_ > 0 && OB_INVALID_ID != tenant_id_ && cluster_id_ > 0 && incarnation_ > 0 && backup_set_id_ > 0 &&
         copy_id_ > 0 && pg_key_.is_valid() && server_.is_valid() && backup_type_ >= ObBackupType::FULL_BACKUP &&
         backup_type_ < ObBackupType::MAX && cluster_version_ > 0 && backup_date_ >= 0 &&
         compatible_ >= OB_BACKUP_COMPATIBLE_VERSION_V1 && compatible_ < OB_BACKUP_COMPATIBLE_VERSION_MAX;
}

int ObBackupBackupsetArg::get_src_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObBackupBackupsetArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(src_uri_header_, src_storage_info_))) {
    STORAGE_LOG(WARN, "failed to set src backup dest", KR(ret), K(src_uri_header_), K(src_storage_info_));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation_))) {
    STORAGE_LOG(WARN, "failed to set cluster backup dest", K(ret), K(backup_dest));
  } else {
    const uint64_t tenant_id = pg_key_.get_tenant_id();
    int64_t full_backup_set_id = 0;
    const int64_t inc_backup_set_id = backup_set_id_;
    if (0 == prev_full_backup_set_id_ && 0 == prev_inc_backup_set_id_) {
      full_backup_set_id = backup_set_id_;
    } else {
      full_backup_set_id = prev_full_backup_set_id_;
    }

    if (OB_FAIL(path_info.set(
            cluster_backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id, backup_date_, compatible_))) {
      STORAGE_LOG(WARN, "failed to set backup path info", K(ret), K(cluster_backup_dest));
    }
  }
  return ret;
}

int ObBackupBackupsetArg::get_dst_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObClusterBackupDest cluster_backup_dest;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObBackupBackupsetArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(dst_uri_header_, dst_storage_info_))) {
    STORAGE_LOG(WARN, "failed to set src backup dest", KR(ret), K(dst_uri_header_), K(dst_storage_info_));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation_))) {
    STORAGE_LOG(WARN, "failed to set cluster backup dest", K(ret), K(backup_dest));
  } else {
    const uint64_t tenant_id = pg_key_.get_tenant_id();
    int64_t full_backup_set_id = 0;
    int64_t inc_backup_set_id = 0;
    if (0 == prev_full_backup_set_id_ && 0 == prev_inc_backup_set_id_) {
      full_backup_set_id = backup_set_id_;
      inc_backup_set_id = backup_set_id_;
    } else {
      full_backup_set_id = prev_full_backup_set_id_;
      inc_backup_set_id = backup_set_id_;
    }
    if (OB_FAIL(path_info.set(
            cluster_backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id, backup_date_, compatible_))) {
      STORAGE_LOG(WARN, "failed to set backup path info", K(ret), K(cluster_backup_dest));
    }
  }
  return ret;
}

ObBackupArchiveLogArg::ObBackupArchiveLogArg()
    : pg_key_(),
      tenant_id_(OB_INVALID_ID),
      log_archive_round_(-1),
      piece_id_(-1),
      create_date_(-1),
      job_id_(-1),
      rs_checkpoint_ts_(-1),
      src_backup_dest_(),
      src_storage_info_(),
      dst_backup_dest_(),
      dst_storage_info_()
{}

void ObBackupArchiveLogArg::reset()
{
  memset(this, 0, sizeof(ObBackupArchiveLogArg));
  log_archive_round_ = -1;
  rs_checkpoint_ts_ = -1;
}

bool ObBackupArchiveLogArg::is_valid() const
{
  return pg_key_.is_valid() && log_archive_round_ > 0 && rs_checkpoint_ts_ > 0;
}

int ObPhysicalRestoreArg::trans_schema_id(
    const uint64_t schema_id, const uint64_t trans_tenant_id_, uint64_t& trans_schema_id) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_id(schema_id) || !is_valid_tenant_id(trans_tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid id", K(ret), K(schema_id), K(trans_tenant_id_));
  } else {
    trans_schema_id = combine_id(trans_tenant_id_, schema_id);
  }
  return ret;
}

int ObPhysicalRestoreArg::get_backup_pgkey(common::ObPartitionKey& pg_key) const
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  if (OB_FAIL(trans_schema_id(pg_key_.get_tablegroup_id(), restore_info_.tenant_id_, table_id))) {
    pg_key.reset();
    STORAGE_LOG(WARN, "get backup pgkey fail", K(ret), K(pg_key_), K(restore_info_));
  } else if (OB_FAIL(pg_key.init(table_id, pg_key_.get_partition_id(), pg_key_.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(pg_key_));
  }
  return ret;
}

int ObPhysicalRestoreArg::change_dst_pkey_to_src_pkey(
    const common::ObPartitionKey& dst_pkey, common::ObPartitionKey& src_pkey) const
{
  int ret = OB_SUCCESS;
  uint64_t src_table_id = 0;
  if (!dst_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "dst pkey is invalid", K(ret), K(dst_pkey));
  } else if (OB_FAIL(trans_schema_id(dst_pkey.get_table_id(), restore_info_.tenant_id_, src_table_id))) {
    STORAGE_LOG(WARN, "failed to change dst table id to src table id", K(dst_pkey), K(restore_info_));
  } else if (OB_FAIL(src_pkey.init(src_table_id, dst_pkey.get_partition_id(), dst_pkey.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(dst_pkey));
  }
  return ret;
}

int ObPhysicalRestoreArg::change_src_pkey_to_dst_pkey(
    const common::ObPartitionKey& src_pkey, common::ObPartitionKey& dst_pkey) const
{
  int ret = OB_SUCCESS;
  uint64_t dst_table_id = 0;
  dst_pkey.reset();
  if (!src_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "src pkey is invalid", K(ret), K(src_pkey));
  } else if (OB_FAIL(trans_schema_id(src_pkey.get_table_id(), pg_key_.get_tenant_id(), dst_table_id))) {
    STORAGE_LOG(WARN, "failed to change src table id to dst table id", K(src_pkey), K(restore_info_));
  } else if (OB_FAIL(dst_pkey.init(dst_table_id, src_pkey.get_partition_id(), src_pkey.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(src_pkey));
  }
  return ret;
}

int ObPhysicalRestoreArg::trans_to_backup_schema_id(const uint64_t schema_id, uint64_t& backup_schema_id) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(trans_schema_id(schema_id, restore_info_.tenant_id_, backup_schema_id))) {
    STORAGE_LOG(WARN, "tran schema is fail", K(ret), K(*this), K(schema_id), K(backup_schema_id));
  }
  return ret;
}

int ObPhysicalRestoreArg::trans_from_backup_schema_id(const uint64_t backup_schema_id, uint64_t& schema_id) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(trans_schema_id(backup_schema_id, pg_key_.get_tenant_id(), schema_id))) {
    STORAGE_LOG(WARN, "tran schema is fail", K(ret), K(*this), K(backup_schema_id), K(schema_id));
  }
  return ret;
}

int ObPhysicalRestoreArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_FAIL(cluster_backup_dest.set(restore_info_.backup_dest_,
                 restore_info_.cluster_name_,
                 restore_info_.cluster_id_,
                 restore_info_.incarnation_))) {
    STORAGE_LOG(WARN, "failed to set cluster backup dest", K(ret), K(restore_info_));
  } else if (OB_FAIL(path_info.set(cluster_backup_dest,
                 restore_info_.tenant_id_,
                 restore_info_.full_backup_set_id_,
                 restore_info_.inc_backup_set_id_,
                 restore_info_.backup_date_,
                 restore_info_.compatible_))) {
    STORAGE_LOG(WARN, "failed to set backup path info", K(ret), K(restore_info_));
  }
  return ret;
}

int ObPhysicalRestoreArg::get_largest_backup_set_path(share::ObSimpleBackupSetPath& simple_path) const
{
  int ret = OB_SUCCESS;
  simple_path.reset();
  int64_t idx = -1;
  int64_t largest_backup_set_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < restore_info_.get_backup_set_path_list().count(); ++i) {
    const share::ObSimpleBackupSetPath& path = restore_info_.get_backup_set_path_list().at(i);
    if (path.backup_set_id_ > largest_backup_set_id) {
      largest_backup_set_id = path.backup_set_id_;
      idx = i;
    }
  }
  if (OB_SUCC(ret)) {
    if (idx >= 0) {
      simple_path = restore_info_.get_backup_set_path_list().at(idx);
      STORAGE_LOG(INFO, "largest backup set path", K(simple_path));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "backup set path not found", K(ret));
    }
  }
  return ret;
}

int ObPhysicalRestoreArg::get_restore_set_list(common::ObIArray<share::ObSimpleBackupSetPath>& path_list) const
{
  int ret = OB_SUCCESS;
  path_list.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalRestoreArg", K(ret), K(*this));
  } else if (OB_FAIL(path_list.assign(restore_info_.get_backup_set_path_list()))) {
    STORAGE_LOG(WARN, "failed to assign backup set list", K(ret), K(*this));
  }
  return ret;
}

ObExternBackupInfo::ObExternBackupInfo()
    : full_backup_set_id_(0),
      inc_backup_set_id_(0),
      backup_data_version_(0),
      backup_snapshot_version_(0),
      backup_schema_version_(0),
      frozen_snapshot_version_(0),
      frozen_schema_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_backup_data_version_(0),
      compatible_(0),
      cluster_version_(0),
      backup_type_(ObBackupType::MAX),
      status_(ObExternBackupInfo::DOING),
      encryption_mode_(share::ObBackupEncryptionMode::MAX_MODE),
      passwd_(),
      is_mark_deleted_(false),
      date_(0)
{}

void ObExternBackupInfo::reset()
{
  full_backup_set_id_ = 0;
  inc_backup_set_id_ = 0;
  backup_data_version_ = 0;
  backup_snapshot_version_ = 0;
  backup_schema_version_ = 0;
  frozen_snapshot_version_ = 0;
  frozen_schema_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_backup_data_version_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  backup_type_ = ObBackupType::MAX;
  status_ = ObExternBackupInfo::DOING;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
  is_mark_deleted_ = false;
  date_ = 0;
}

bool ObExternBackupInfo::is_valid() const
{
  return full_backup_set_id_ > 0 && inc_backup_set_id_ > 0 && backup_data_version_ > 0 &&
         backup_snapshot_version_ > 0 && backup_schema_version_ > 0 && frozen_snapshot_version_ > 0 &&
         frozen_schema_version_ >= 0 && backup_type_ >= ObBackupType::FULL_BACKUP && backup_type_ < ObBackupType::MAX &&
         status_ >= SUCCESS && status_ <= FAILED && share::ObBackupEncryptionMode::is_valid(encryption_mode_) &&
         date_ >= 0;
}

bool ObExternBackupInfo::is_equal_without_status(const ObExternBackupInfo& extern_backup_info) const
{
  return full_backup_set_id_ == extern_backup_info.full_backup_set_id_ &&
         inc_backup_set_id_ == extern_backup_info.inc_backup_set_id_ &&
         backup_data_version_ == extern_backup_info.backup_data_version_ &&
         backup_snapshot_version_ == extern_backup_info.backup_snapshot_version_ &&
         backup_schema_version_ == extern_backup_info.backup_schema_version_ &&
         frozen_snapshot_version_ == extern_backup_info.frozen_snapshot_version_ &&
         frozen_schema_version_ == extern_backup_info.frozen_schema_version_ &&
         prev_full_backup_set_id_ == extern_backup_info.prev_full_backup_set_id_ &&
         prev_inc_backup_set_id_ == extern_backup_info.prev_inc_backup_set_id_ &&
         prev_backup_data_version_ == extern_backup_info.prev_backup_data_version_ &&
         compatible_ == extern_backup_info.compatible_ && cluster_version_ == extern_backup_info.cluster_version_ &&
         backup_type_ == extern_backup_info.backup_type_ && encryption_mode_ == extern_backup_info.encryption_mode_ &&
         passwd_ == extern_backup_info.passwd_ && date_ == extern_backup_info.date_;
}

bool ObExternBackupInfo::is_equal(const ObExternBackupInfo& extern_backup_info) const
{
  return is_equal_without_status(extern_backup_info) && status_ == extern_backup_info.status_ &&
         is_mark_deleted_ == extern_backup_info.is_mark_deleted_;
}

OB_SERIALIZE_MEMBER(ObExternBackupInfo, full_backup_set_id_, inc_backup_set_id_, backup_data_version_,
    backup_snapshot_version_, backup_schema_version_, frozen_snapshot_version_, frozen_schema_version_,
    prev_full_backup_set_id_, prev_inc_backup_set_id_, prev_backup_data_version_, compatible_, cluster_version_,
    backup_type_, status_, encryption_mode_, passwd_, is_mark_deleted_, date_);

ObExternBackupSetInfo::ObExternBackupSetInfo()
    : backup_set_id_(0),
      backup_snapshot_version_(0),
      compatible_(0),
      pg_count_(0),
      macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      cluster_version_(0),
      compress_type_(ObCompressorType::INVALID_COMPRESSOR)
{}

void ObExternBackupSetInfo::reset()
{
  backup_set_id_ = 0;
  backup_snapshot_version_ = 0;
  compatible_ = 0;
  pg_count_ = 0;
  macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  cluster_version_ = 0;
  compress_type_ = ObCompressorType::INVALID_COMPRESSOR;
}

bool ObExternBackupSetInfo::is_valid() const
{
  return backup_set_id_ >= 0 && backup_snapshot_version_ >= 0 && pg_count_ >= 0 && macro_block_count_ >= 0 &&
         input_bytes_ >= 0 && output_bytes_ >= 0 && cluster_version_ > 0 &&
         compress_type_ > ObCompressorType::INVALID_COMPRESSOR;
}

OB_SERIALIZE_MEMBER(ObExternBackupSetInfo, backup_set_id_, backup_snapshot_version_, compatible_, pg_count_,
    macro_block_count_, input_bytes_, output_bytes_, cluster_version_, compress_type_);

ObExternTenantInfo::ObExternTenantInfo()
    : tenant_id_(OB_INVALID_ID),
      create_timestamp_(0),
      delete_timestamp_(0),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      tenant_name_(),
      backup_snapshot_version_(0)
{}

void ObExternTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  create_timestamp_ = 0;
  delete_timestamp_ = 0;
  tenant_name_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  backup_snapshot_version_ = 0;
}

bool ObExternTenantInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && create_timestamp_ >= 0 && !tenant_name_.is_empty() &&
         compat_mode_ != lib::Worker::CompatMode::INVALID && backup_snapshot_version_ > 0;
}

OB_SERIALIZE_MEMBER(ObExternTenantInfo, tenant_id_, create_timestamp_, delete_timestamp_, compat_mode_, tenant_name_,
    backup_snapshot_version_);

ObExternTenantLocalityInfo::ObExternTenantLocalityInfo()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      backup_snapshot_version_(),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      tenant_name_(),
      locality_(),
      primary_zone_()
{}

void ObExternTenantLocalityInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  backup_snapshot_version_ = 0;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  tenant_name_.reset();
  locality_.reset();
  primary_zone_.reset();
}

bool ObExternTenantLocalityInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && backup_set_id_ > 0 && backup_snapshot_version_ > 0 &&
         compat_mode_ != lib::Worker::CompatMode::INVALID && !tenant_name_.is_empty() && !locality_.is_empty() &&
         !primary_zone_.is_empty();
}

bool ObExternTenantLocalityInfo::is_equal(const ObExternTenantLocalityInfo& tenant_locality_info) const
{
  return tenant_id_ == tenant_locality_info.tenant_id_ && backup_set_id_ == tenant_locality_info.backup_set_id_ &&
         backup_snapshot_version_ == tenant_locality_info.backup_snapshot_version_ &&
         compat_mode_ == tenant_locality_info.compat_mode_ && tenant_name_ == tenant_locality_info.tenant_name_ &&
         locality_ == tenant_locality_info.locality_ && primary_zone_ == tenant_locality_info.primary_zone_;
}

OB_SERIALIZE_MEMBER(ObExternTenantLocalityInfo, tenant_id_, backup_set_id_, backup_snapshot_version_, compat_mode_,
    tenant_name_, locality_, primary_zone_);

ObExternBackupDiagnoseInfo::ObExternBackupDiagnoseInfo()
    : tenant_id_(OB_INVALID_ID), tenant_locality_info_(), backup_set_info_(), extern_backup_info_()
{}

void ObExternBackupDiagnoseInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  tenant_locality_info_.reset();
  backup_set_info_.reset();
  extern_backup_info_.reset();
}

static const char* backup_clean_status_strs[] = {
    "STOP",
    "PREPARE",
    "DOING",
    "CANCEL",
};

const char* ObBackupCleanInfoStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= MAX) {
    str = "UNKOWN";
  } else {
    str = backup_clean_status_strs[status];
  }
  return str;
}

ObBackupCleanInfoStatus::STATUS ObBackupCleanInfoStatus::get_status(const char* status_str)
{
  ObBackupCleanInfoStatus::STATUS status = ObBackupCleanInfoStatus::MAX;

  const int64_t count = ARRAYSIZEOF(backup_clean_status_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupCleanInfoStatus::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, backup_clean_status_strs[i])) {
      status = static_cast<ObBackupCleanInfoStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

static const char* backup_clean_type_str[] = {
    "",
    "DELETE OBSOLETE BACKUP",
    "DELETE BACKUP SET",
    "DELETE BACKUP PIECE",
    "DELETE BACKUP ALL",
    "DELETE OBSOLETE BACKUP BACKUP",
    "DELETE BACKUP ROUND",
};

const char* ObBackupCleanType::get_str(const TYPE& type)
{
  const char* str = nullptr;

  if (type < 0 || type >= MAX) {
    str = "UNKOWN";
  } else {
    str = backup_clean_type_str[type];
  }
  return str;
}

ObBackupCleanType::TYPE ObBackupCleanType::get_type(const char* type_str)
{
  ObBackupCleanType::TYPE type = ObBackupCleanType::MAX;

  const int64_t count = ARRAYSIZEOF(backup_clean_type_str);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupCleanType::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, backup_clean_type_str[i])) {
      type = static_cast<ObBackupCleanType::TYPE>(i);
      break;
    }
  }
  return type;
}

ObBackupCleanInfo::ObBackupCleanInfo()
    : tenant_id_(OB_INVALID_ID),
      job_id_(0),
      start_time_(0),
      end_time_(0),
      incarnation_(0),
      copy_id_(0),
      type_(ObBackupCleanType::MAX),
      status_(ObBackupCleanInfoStatus::MAX),
      expired_time_(0),
      backup_set_id_(0),
      error_msg_(),
      comment_(),
      clog_gc_snapshot_(0),
      result_(OB_SUCCESS),
      backup_piece_id_(0),
      backup_round_id_(0)
{}

void ObBackupCleanInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  job_id_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  incarnation_ = 0;
  copy_id_ = 0;
  type_ = ObBackupCleanType::MAX;
  status_ = ObBackupCleanInfoStatus::MAX;
  expired_time_ = 0;
  backup_set_id_ = 0;
  error_msg_.reset();
  comment_.reset();
  clog_gc_snapshot_ = 0;
  result_ = OB_SUCCESS;
  backup_piece_id_ = 0;
  backup_round_id_ = 0;
}

bool ObBackupCleanInfo::is_valid() const
{
  bool is_valid = true;
  is_valid = tenant_id_ != OB_INVALID_ID && ObBackupCleanInfoStatus::is_valid(status_);
  if (is_valid) {
    if (ObBackupCleanInfoStatus::STOP != status_) {
      is_valid = ObBackupCleanType::is_valid(type_) && job_id_ >= 0 && incarnation_ > 0 &&
                 ((is_delete_obsolete() && expired_time_ > 0) || (is_backup_set_clean() && backup_set_id_ > 0) ||
                     (is_delete_backup_piece() && backup_piece_id_ >= 0) ||
                     (is_delete_backup_round() && backup_round_id_ > 0));
    }
  }
  return is_valid;
}

int ObBackupCleanInfo::get_clean_parameter(int64_t& parameter) const
{
  int ret = OB_SUCCESS;
  parameter = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup clean info is invalid", K(*this));
  } else if (is_delete_obsolete()) {
    parameter = expired_time_;
  } else if (is_backup_set_clean()) {
    parameter = backup_set_id_;
  } else if (is_delete_backup_piece()) {
    parameter = backup_piece_id_;
  } else if (is_delete_backup_round()) {
    parameter = backup_round_id_;
  } else if (is_empty_clean_type()) {
    // do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get clean paramater", K(ret), K(*this));
  }
  return ret;
}

int ObBackupCleanInfo::set_clean_parameter(const int64_t parameter)
{
  int ret = OB_SUCCESS;
  if (parameter < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set clean parameter get invalid argument", K(ret), K(parameter));
  } else if (is_empty_clean_type() && ObBackupCleanInfoStatus::STOP == status_) {
    // do nothing
  } else if (is_delete_obsolete()) {
    expired_time_ = parameter;
  } else if (is_backup_set_clean()) {
    backup_set_id_ = parameter;
  } else if (is_delete_backup_piece()) {
    backup_piece_id_ = parameter;
  } else if (is_delete_backup_round()) {
    backup_round_id_ = parameter;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean info is invalid, can not set parameter", K(ret), K(*this), K(parameter));
  }
  return ret;
}

int ObBackupCleanInfo::set_copy_id(const int64_t copy_id)
{
  int ret = OB_SUCCESS;
  if (copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set copy id get invalid argument", KR(ret), K(copy_id));
  } else {
    copy_id_ = copy_id;
  }
  return ret;
}

int ObBackupCleanInfo::check_backup_clean_info_match(const ObBackupCleanInfo& clean_info) const
{
  int ret = OB_SUCCESS;
  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(clean_info));
  } else if (tenant_id_ != clean_info.tenant_id_ || job_id_ != clean_info.job_id_ ||
             incarnation_ != clean_info.incarnation_ || type_ != clean_info.type_ || status_ != clean_info.status_) {
    ret = OB_BACKUP_CLEAN_INFO_NOT_MATCH;
    LOG_WARN("backup clean info is not match", K(ret), K(*this), K(clean_info));
  }
  return ret;
}

ObBackupDestOpt::ObBackupDestOpt()
    : log_archive_checkpoint_interval_(0),
      recovery_window_(0),
      piece_switch_interval_(0),
      backup_copies_(0),
      auto_delete_obsolete_backup_(false),
      auto_touch_reserved_backup_(false),
      is_valid_(false)
{}

void ObBackupDestOpt::reset()
{
  log_archive_checkpoint_interval_ = 0;
  recovery_window_ = 0;
  piece_switch_interval_ = 0;
  backup_copies_ = 0;
  auto_delete_obsolete_backup_ = false;
  auto_touch_reserved_backup_ = false;
  is_valid_ = false;
}

int ObBackupDestOpt::init(const bool is_backup_backup)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = OB_MAX_CONFIG_VALUE_LEN;
  char opt_buf[buf_len] = "";
  const bool global_auto_delete_obsolete_backup = GCONF.auto_delete_expired_backup;
  const int64_t global_recovery_window = GCONF.backup_recovery_window;
  int64_t global_checkpoint_interval = is_backup_backup ? 0 : GCONF.log_archive_checkpoint_interval;
  const bool auto_touch = GCONF._auto_update_reserved_backup_timestamp;

  if (is_backup_backup) {
    if (OB_FAIL(GCONF.backup_backup_dest_option.copy(opt_buf, buf_len))) {
      LOG_WARN("failed to copy backup dest opt", K(ret));
    }
  } else {
    if (OB_FAIL(GCONF.backup_dest_option.copy(opt_buf, buf_len))) {
      LOG_WARN("failed to copy backup dest opt", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init(is_backup_backup,
                 opt_buf,
                 global_auto_delete_obsolete_backup,
                 global_recovery_window,
                 global_checkpoint_interval,
                 auto_touch))) {
    LOG_WARN("failed to init",
        K(ret),
        K(global_auto_delete_obsolete_backup),
        K(global_recovery_window),
        K(global_checkpoint_interval));
  }

  return ret;
}

int ObBackupDestOpt::init(const bool is_backup_backup, const char* opt_str,
    const bool global_auto_delete_obsolete_backup, const int64_t global_recovery_window,
    const int64_t global_checkpoint_interval, const bool global_auto_touch_reserved_backup)
{
  int ret = OB_SUCCESS;
  const bool* delete_obsolete_ptr = &global_auto_delete_obsolete_backup;
  const int64_t* recovery_window_ptr = &global_recovery_window;
  const int64_t* checkpoint_interval_ptr = &global_checkpoint_interval;
  const bool* auto_touch_ptr = &global_auto_touch_reserved_backup;
  reset();

  if (OB_ISNULL(opt_str) || global_recovery_window < 0 || global_checkpoint_interval < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(opt_str));
  } else if (OB_FAIL(parse_opt_(
                 opt_str, delete_obsolete_ptr, recovery_window_ptr, checkpoint_interval_ptr, auto_touch_ptr))) {
    LOG_WARN("failed to parse opt", K(ret), K(opt_str));
  } else if (OB_FAIL(
                 fill_global_opt_(delete_obsolete_ptr, recovery_window_ptr, checkpoint_interval_ptr, auto_touch_ptr))) {
    LOG_WARN("failed to fill global opt", K(ret), K(opt_str));
  } else if (OB_FAIL(check_valid_())) {
    LOG_WARN("failed to check valid", K(ret), K(*this));
  } else if (is_backup_backup) {
    if (piece_switch_interval_ != 0 || backup_copies_ != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup backup dest cannot support these param",
          K(ret),
          K(piece_switch_interval_),
          K(backup_copies_),
          K(*opt_str));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
    is_valid_ = false;
    LOG_WARN("failed to init BackupDestOpt",
        K(opt_str),
        K(global_auto_delete_obsolete_backup),
        K(global_recovery_window),
        K(global_checkpoint_interval),
        K(global_auto_touch_reserved_backup),
        K(is_backup_backup));
  } else {
    is_valid_ = true;
    LOG_INFO("succeed to init BackupDestOpt",
        K(*this),
        K(opt_str),
        K(global_auto_delete_obsolete_backup),
        K(global_recovery_window),
        K(global_checkpoint_interval),
        K(global_auto_touch_reserved_backup),
        K(is_backup_backup));
  }
  return ret;
}

int ObBackupDestOpt::check_valid_()
{
  int ret = OB_SUCCESS;
  ;

  if (log_archive_checkpoint_interval_ != 0 &&
      log_archive_checkpoint_interval_ < OB_MIN_LOG_ARCHIVE_CHECKPOINT_INTERVAL) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("log_archive_checkpoint_interval_ cannot less than 5s", K(log_archive_checkpoint_interval_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "log_archive_checkpoint_interval_ less than 5s is");
  } else if (recovery_window_ < 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("recovery_window_ must not less than 0", K(recovery_window_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "recovery_window_ less than 0 is");
  } else if (piece_switch_interval_ != 0 && piece_switch_interval_ < OB_MIN_LOG_ARCHIVE_PIECE_SWITH_INTERVAL) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("piece_switch_interval_ less than 10s is not support", K(piece_switch_interval_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "piece_switch_interval_ less than 10s is ");
  } else if (backup_copies_ < 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup copies must not less than 0", K(backup_copies_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup copies less than 0 is");
  } else if (auto_delete_obsolete_backup_ && auto_touch_reserved_backup_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot do auto tounch and auto delete both");
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto delete and touch backup at same time is");
  }

  return ret;
}

int ObBackupDestOpt::parse_opt_(const char* opt_str, const bool*& delete_obsolete_ptr,
    const int64_t*& recovery_window_ptr, const int64_t*& checkount_interval_ptr, const bool*& auto_touch)
{
  int ret = OB_SUCCESS;
  const char* LOG_ARCHIVE_CHECKPOINT_INTERVAL = "log_archive_checkpoint_interval=";
  const char* RECOVERY_WINDOW = "recovery_window=";
  const char* LOG_ARCHIVE_PIECE_SWITCH_INTERVAL = "log_archive_piece_switch_interval=";
  const char* BACKUP_COPIES = "backup_copies=";
  const char* AUTO_DELETE_OBSOLETE_BACKUP = "auto_delete_obsolete_backup=";
  const char* AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP = "auto_update_reserved_backup_timestamp=";
  char buf[OB_MAX_CONFIG_VALUE_LEN] = "";
  char* token = nullptr;
  char* saved_ptr = nullptr;

  if (OB_FAIL(databuff_printf(buf, sizeof(buf), "%s", opt_str))) {
    LOG_WARN("failed to copy opt str", K(ret), K(opt_str));
  } else if (NULL != memchr(opt_str, ' ', strlen(opt_str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest opt should not contain backspace", K(ret), K(opt_str));
  }

  for (char* str = buf; OB_SUCC(ret); str = nullptr) {
    token = ::strtok_r(str, "&", &saved_ptr);
    if (NULL == token) {
      break;
    } else if (0 == strncmp(token, LOG_ARCHIVE_CHECKPOINT_INTERVAL, strlen(LOG_ARCHIVE_CHECKPOINT_INTERVAL))) {
      if (OB_FAIL(parse_time_interval_(
              token + strlen(LOG_ARCHIVE_CHECKPOINT_INTERVAL), log_archive_checkpoint_interval_))) {
        LOG_WARN("failed to parse log_archive_checkpoint_interval", K(ret), K(token));
      }
      checkount_interval_ptr = nullptr;
    } else if (0 == strncmp(token, RECOVERY_WINDOW, strlen(RECOVERY_WINDOW))) {
      if (OB_FAIL(parse_time_interval_(token + strlen(RECOVERY_WINDOW), recovery_window_))) {
        LOG_WARN("failed to parse recover window", K(ret), K(*token));
      }
      recovery_window_ptr = nullptr;
    } else if (0 == strncmp(token, LOG_ARCHIVE_PIECE_SWITCH_INTERVAL, strlen(LOG_ARCHIVE_PIECE_SWITCH_INTERVAL))) {
      if (OB_FAIL(parse_time_interval_(token + strlen(LOG_ARCHIVE_PIECE_SWITCH_INTERVAL), piece_switch_interval_))) {
        LOG_WARN("failed to parse log archive piece switch interval", K(ret), K(token));
      }
    } else if (0 == strncmp(token, BACKUP_COPIES, strlen(BACKUP_COPIES))) {
      if (OB_FAIL(parse_int_(token + strlen(BACKUP_COPIES), backup_copies_))) {
        LOG_WARN("failed to parse backup copies", K(ret), K(*token));
      }
    } else if (0 == strncmp(token, AUTO_DELETE_OBSOLETE_BACKUP, strlen(AUTO_DELETE_OBSOLETE_BACKUP))) {
      if (OB_FAIL(parse_bool_(token + strlen(AUTO_DELETE_OBSOLETE_BACKUP), auto_delete_obsolete_backup_))) {
        LOG_WARN("failed to parse auto delete obsolete backup", K(ret), K(token));
      }
      delete_obsolete_ptr = nullptr;
    } else if (0 ==
               strncmp(token, AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP, strlen(AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP))) {
      if (OB_FAIL(parse_bool_(token + strlen(AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP), auto_touch_reserved_backup_))) {
        LOG_WARN("failed to parse auto touch reserved backup", K(ret), K(token));
      }
      auto_touch = nullptr;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("unknown backup dest opt", K(ret), K(token));
    }
  }

  return ret;
}

int ObBackupDestOpt::parse_time_interval_(const char* str, int64_t& val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    val = ObConfigTimeParser::get(str, is_valid);
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid time interval str", K(ret), K(str));
    }
  }
  return ret;
}

int ObBackupDestOpt::parse_int_(const char* str, int64_t& val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    val = ObConfigIntParser::get(str, is_valid);
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid int str", K(ret), K(str));
    }
  }

  return ret;
}

int ObBackupDestOpt::parse_bool_(const char* str, bool& val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = false;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    val = ObConfigBoolParser::get(str, is_valid);
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid bool str", K(ret), K(str));
    }
  }

  return ret;
}

int ObBackupDestOpt::fill_global_opt_(const bool* delete_obsolete_ptr, const int64_t* recovery_window_ptr,
    const int64_t* checkpoint_interval_ptr, const bool* auto_touch_ptr)
{
  int ret = OB_SUCCESS;

  if (nullptr != delete_obsolete_ptr) {
    auto_delete_obsolete_backup_ = *delete_obsolete_ptr;
  }

  if (nullptr != recovery_window_ptr) {
    recovery_window_ = *recovery_window_ptr;
  }

  if (nullptr != checkpoint_interval_ptr) {
    log_archive_checkpoint_interval_ = *checkpoint_interval_ptr;
  }

  if (nullptr != auto_touch_ptr) {
    auto_touch_reserved_backup_ = *auto_touch_ptr;
  }
  return ret;
}

ObBackupPieceInfoKey::ObBackupPieceInfoKey()
    : incarnation_(0), tenant_id_(0), round_id_(0), backup_piece_id_(0), copy_id_(0)
{}

bool ObBackupPieceInfoKey::is_valid() const
{
  return OB_START_INCARNATION == incarnation_ && tenant_id_ > 0 && round_id_ > 0 && backup_piece_id_ >= 0 &&
         copy_id_ >= 0;
}

void ObBackupPieceInfoKey::reset()
{
  incarnation_ = 0;
  tenant_id_ = 0;
  round_id_ = 0;
  backup_piece_id_ = 0;
  copy_id_ = 0;
}

bool ObBackupPieceInfoKey::operator==(const ObBackupPieceInfoKey& o) const
{
  return incarnation_ == o.incarnation_ && tenant_id_ == o.tenant_id_ && round_id_ == o.round_id_ &&
         backup_piece_id_ == o.backup_piece_id_ && copy_id_ == o.copy_id_;
}

bool ObBackupPieceInfoKey::operator<(const ObBackupPieceInfoKey& o) const
{
  bool result = false;

  if (incarnation_ < o.incarnation_) {
    result = true;
  } else if (incarnation_ > o.incarnation_) {
    result = false;
  } else if (tenant_id_ < o.tenant_id_) {
    result = true;
  } else if (tenant_id_ > o.tenant_id_) {
    result = false;
  } else if (round_id_ < o.round_id_) {
    result = true;
  } else if (round_id_ > o.round_id_) {
    result = false;
  } else if (backup_piece_id_ < o.backup_piece_id_) {
    result = true;
  } else if (backup_piece_id_ > o.backup_piece_id_) {
    result = false;
  } else if (copy_id_ < o.copy_id_) {
    result = true;
  } else {
    result = false;
  }
  return result;
}

static const char* PIECE_STATUS_STRS[] = {
    "ACTIVE",
    "FREEZING",
    "FROZEN",
    "INACTIVE",
};

const char* ObBackupPieceStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= BACKUP_PIECE_MAX) {
    str = "UNKNOWN";
  } else {
    str = PIECE_STATUS_STRS[status];
  }
  return str;
}

ObBackupPieceStatus::STATUS ObBackupPieceStatus::get_status(const char* status_str)
{
  ObBackupPieceStatus::STATUS status = ObBackupPieceStatus::BACKUP_PIECE_MAX;

  const int64_t count = ARRAYSIZEOF(PIECE_STATUS_STRS);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupPieceStatus::BACKUP_PIECE_MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, PIECE_STATUS_STRS[i])) {
      status = static_cast<ObBackupPieceStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

static const char* PIECE_FILE_STATUS_STRS[] = {
    "AVAILABLE",
    "COPYING",
    "INCOMPLETE",
    "DELETING",
    "EXPIRED",
    "BROKEN",
    "DELETED",
};

const char* ObBackupFileStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= BACKUP_FILE_MAX) {
    str = "UNKNOWN";
  } else {
    str = PIECE_FILE_STATUS_STRS[status];
  }
  return str;
}

ObBackupFileStatus::STATUS ObBackupFileStatus::get_status(const char* status_str)
{
  ObBackupFileStatus::STATUS status = ObBackupFileStatus::BACKUP_FILE_MAX;

  const int64_t count = ARRAYSIZEOF(PIECE_FILE_STATUS_STRS);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupFileStatus::BACKUP_FILE_MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, PIECE_FILE_STATUS_STRS[i])) {
      status = static_cast<ObBackupFileStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

int ObBackupFileStatus::check_can_change_status(
    const ObBackupFileStatus::STATUS& src_file_status, const ObBackupFileStatus::STATUS& dest_file_status)
{
  int ret = OB_SUCCESS;
  if (ObBackupFileStatus::BACKUP_FILE_MAX == src_file_status ||
      ObBackupFileStatus::BACKUP_FILE_MAX == dest_file_status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(src_file_status), K(dest_file_status));
  } else {
    switch (src_file_status) {
      case ObBackupFileStatus::BACKUP_FILE_AVAILABLE: {
        if (ObBackupFileStatus::BACKUP_FILE_BROKEN != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_EXPIRED != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_DELETING != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_AVAILABLE != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_INCOMPLETE != dest_file_status /*for sys backup backup*/) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_INCOMPLETE: {
        if (ObBackupFileStatus::BACKUP_FILE_COPYING != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_INCOMPLETE != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_DELETING != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_COPYING: {
        if (ObBackupFileStatus::BACKUP_FILE_COPYING != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_AVAILABLE != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_INCOMPLETE != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_BROKEN: {
        if (ObBackupFileStatus::BACKUP_FILE_BROKEN != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_DELETING != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_EXPIRED: {
        if (ObBackupFileStatus::BACKUP_FILE_EXPIRED != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_AVAILABLE != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_DELETING != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_DELETING: {
        if (ObBackupFileStatus::BACKUP_FILE_DELETING != dest_file_status &&
            ObBackupFileStatus::BACKUP_FILE_DELETED != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not change backup file status", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_DELETED: {
        if (ObBackupFileStatus::BACKUP_FILE_DELETED != dest_file_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup set file info", K(ret), K(src_file_status), K(dest_file_status));
        }
        break;
      }
      case ObBackupFileStatus::BACKUP_FILE_MAX: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup task", K(ret), K(src_file_status), K(dest_file_status));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown file status", K(ret), K(src_file_status), K(dest_file_status));
        break;
      }
    }
  }
  return ret;
}

bool ObBackupFileStatus::can_show_in_preview(const ObBackupFileStatus::STATUS& status)
{
  return ObBackupFileStatus::BACKUP_FILE_AVAILABLE == status || ObBackupFileStatus::BACKUP_FILE_DELETING == status ||
         ObBackupFileStatus::BACKUP_FILE_DELETED == status;
}

ObBackupPieceInfo::ObBackupPieceInfo()
    : key_(),
      create_date_(0),
      start_ts_(0),
      checkpoint_ts_(0),
      max_ts_(0),
      status_(ObBackupPieceStatus::BACKUP_PIECE_MAX),
      file_status_(ObBackupFileStatus::BACKUP_FILE_AVAILABLE),
      backup_dest_(),
      compatible_(ObTenantLogArchiveStatus::MAX),
      start_piece_id_(0)
{}

OB_SERIALIZE_MEMBER(ObBackupPieceInfo, key_.incarnation_, key_.tenant_id_, key_.round_id_, key_.backup_piece_id_,
    key_.copy_id_, create_date_, start_ts_, checkpoint_ts_, max_ts_, status_, file_status_, backup_dest_, compatible_,
    start_piece_id_);

bool ObBackupPieceInfo::is_valid() const
{
  return key_.is_valid() && create_date_ >= 0 && status_ >= 0 && status_ < ObBackupPieceStatus::BACKUP_PIECE_MAX &&
         file_status_ >= 0 && file_status_ < ObBackupFileStatus::BACKUP_FILE_MAX && start_ts_ >= 0 &&
         checkpoint_ts_ >= 0 && max_ts_ >= 0 && backup_dest_.size() > 0 &&
         (compatible_ >= ObTenantLogArchiveStatus::NONE && compatible_ < ObTenantLogArchiveStatus::MAX) &&
         start_piece_id_ >= 0;
}

void ObBackupPieceInfo::reset()
{
  key_.reset();
  create_date_ = 0;
  status_ = ObBackupPieceStatus::BACKUP_PIECE_MAX;
  file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  max_ts_ = 0;
  backup_dest_.reset();
  compatible_ = ObTenantLogArchiveStatus::MAX;
  start_piece_id_ = 0;
}

bool ObBackupPieceInfo::operator==(const ObBackupPieceInfo& o) const
{
  return key_ == o.key_ && create_date_ == o.create_date_ && start_ts_ == o.start_ts_ &&
         checkpoint_ts_ == o.checkpoint_ts_ && max_ts_ == o.max_ts_ && status_ == o.status_ &&
         file_status_ == o.file_status_ && backup_dest_ == o.backup_dest_ && compatible_ == o.compatible_ &&
         start_piece_id_ == o.start_piece_id_;
}

int ObBackupPieceInfo::init_piece_info(const ObBackupPieceInfo& sys_piece, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  reset();

  if (!sys_piece.is_valid() || OB_INVALID_ID == tenant_id || OB_SYS_TENANT_ID != sys_piece.key_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sys_piece), K(tenant_id));
  } else if (OB_FAIL(backup_dest_.assign(sys_piece.backup_dest_))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(sys_piece));
  } else {
    // key
    key_ = sys_piece.key_;
    key_.tenant_id_ = tenant_id;
    create_date_ = sys_piece.create_date_;
    // value
    start_ts_ = sys_piece.start_ts_;
    checkpoint_ts_ = sys_piece.checkpoint_ts_;
    max_ts_ = sys_piece.max_ts_;
    status_ = sys_piece.status_;
    file_status_ = sys_piece.file_status_;
    compatible_ = sys_piece.compatible_;
    start_piece_id_ = sys_piece.start_piece_id_;
  }
  return ret;
}

int ObBackupPieceInfo::get_backup_dest(ObBackupDest& backup_dest) const
{
  int ret = OB_SUCCESS;
  backup_dest.reset();
  if (backup_dest_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_));
  }
  return ret;
}

ObNonFrozenBackupPieceInfo::ObNonFrozenBackupPieceInfo()
    : has_prev_piece_info_(false), prev_piece_info_(), cur_piece_info_()
{}

void ObNonFrozenBackupPieceInfo::reset()
{
  has_prev_piece_info_ = false;
  prev_piece_info_.reset();
  cur_piece_info_.reset();
}

bool ObNonFrozenBackupPieceInfo::is_valid() const
{
  bool is_valid = true;

  if (!cur_piece_info_.is_valid()) {
    is_valid = false;
    LOG_WARN("cur piece info is invalid", K(*this));
  }

  if (has_prev_piece_info_ && is_valid) {
    if (!prev_piece_info_.is_valid()) {
      is_valid = false;
      LOG_WARN("prev piece info is invalid", K(*this));
    } else if (ObBackupPieceStatus::BACKUP_PIECE_ACTIVE == prev_piece_info_.status_) {
      is_valid = false;
      LOG_WARN("previous piece info is active", K(*this));
    }
  }

  return is_valid;
}

int ObNonFrozenBackupPieceInfo::get_backup_piece_id(int64_t& active_piece_id) const
{
  int ret = OB_SUCCESS;
  active_piece_id = -1;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid piece info", K(ret), K(*this));
  } else {
    active_piece_id = cur_piece_info_.key_.backup_piece_id_;
  }

  return ret;
}

int ObNonFrozenBackupPieceInfo::get_backup_piece_info(int64_t& active_piece_id, int64_t& active_piece_create_date) const
{
  int ret = OB_SUCCESS;
  active_piece_id = -1;
  active_piece_create_date = OB_INVALID_TIMESTAMP;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid piece info", K(ret), K(*this));
  } else {
    active_piece_id = cur_piece_info_.key_.backup_piece_id_;
    active_piece_create_date = cur_piece_info_.create_date_;
  }

  return ret;
}

int64_t ObNonFrozenBackupPieceInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(has_prev_piece_info), K_(cur_piece_info));
    if (has_prev_piece_info_) {
      J_COMMA();
      J_KV(K_(prev_piece_info));
    }

    J_OBJ_END();
  }
  return pos;
}

const char* backup_set_file_info_status_strs[] = {
    "DOING",
    "SUCCESS",
    "FAILED",
};

ObBackupSetFileInfo::ObBackupSetFileInfo()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      copy_id_(-1),
      snapshot_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_backup_data_version_(0),
      pg_count_(0),
      macro_block_count_(0),
      finish_pg_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      start_time_(0),
      end_time_(0),
      compatible_(0),
      cluster_version_(0),
      backup_type_(),
      status_(BackupSetStatus::MAX),
      result_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID),
      backup_dest_(),
      backup_data_version_(0),
      backup_schema_version_(0),
      partition_count_(0),
      finish_partition_count_(0),
      encryption_mode_(ObBackupEncryptionMode::MAX_MODE),
      passwd_(),
      file_status_(ObBackupFileStatus::BACKUP_FILE_MAX),
      start_replay_log_ts_(0),
      date_(0)
{}

void ObBackupSetFileInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  incarnation_ = 0;
  copy_id_ = -1;
  snapshot_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_backup_data_version_ = 0;
  pg_count_ = 0;
  macro_block_count_ = 0;
  finish_pg_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  backup_type_.reset();
  status_ = BackupSetStatus::MAX;
  result_ = 0;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  backup_dest_.reset();
  backup_data_version_ = 0;
  backup_schema_version_ = 0;
  partition_count_ = 0;
  finish_partition_count_ = 0;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
  file_status_ = ObBackupFileStatus::BACKUP_FILE_MAX;
  start_replay_log_ts_ = 0;
  date_ = 0;
}

bool ObBackupSetFileInfo::is_key_valid() const
{
  return tenant_id_ != OB_INVALID_ID && backup_set_id_ > 0 && incarnation_ > 0 && copy_id_ >= 0;
}

bool ObBackupSetFileInfo::is_valid() const
{
  return is_key_valid() && BackupSetStatus::MAX != status_ &&
         share::ObBackupEncryptionMode::is_valid(encryption_mode_) && ObBackupFileStatus::is_valid(file_status_);
}

bool ObBackupSetFileInfo::is_same_task(const ObBackupSetFileInfo& other) const
{
  return tenant_id_ == other.tenant_id_ && backup_set_id_ == other.backup_set_id_ &&
         incarnation_ == other.incarnation_ && copy_id_ == other.copy_id_;
}

const char* ObBackupSetFileInfo::get_backup_set_status_str() const
{
  const char* str = "UNKNOWN";

  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_set_file_info_status_strs), "status count mismatch");
  if (status_ < 0 || status_ >= MAX) {
    LOG_WARN("invalid backup set status", K(status_));
  } else {
    str = backup_set_file_info_status_strs[status_];
  }
  return str;
}

int ObBackupSetFileInfo::set_backup_set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup task status get invalid argument", K(ret), KP(buf));
  } else {
    BackupSetStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_set_file_info_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_set_file_info_status_strs); i++) {
      if (0 == STRCMP(backup_set_file_info_status_strs[i], buf)) {
        tmp_status = static_cast<BackupSetStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

int ObBackupSetFileInfo::extract_from_backup_task_info(const ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract from backup task info get invalid argument", K(ret), K(task_info));
  } else {
    char backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
    tenant_id_ = task_info.tenant_id_;
    backup_set_id_ = task_info.backup_set_id_;
    incarnation_ = task_info.incarnation_;
    copy_id_ = 0;  // first backup image copy id is 0
    snapshot_version_ = task_info.snapshot_version_;
    prev_full_backup_set_id_ = task_info.prev_full_backup_set_id_;
    prev_inc_backup_set_id_ = task_info.prev_inc_backup_set_id_;
    prev_backup_data_version_ = task_info.prev_backup_data_version_;
    pg_count_ = task_info.pg_count_;
    macro_block_count_ = task_info.macro_block_count_;
    finish_pg_count_ = task_info.finish_pg_count_;
    finish_macro_block_count_ = task_info.finish_macro_block_count_;
    input_bytes_ = task_info.input_bytes_;
    output_bytes_ = task_info.output_bytes_;
    start_time_ = task_info.start_time_;
    end_time_ = task_info.end_time_;
    compatible_ = task_info.compatible_;
    cluster_version_ = task_info.cluster_version_;
    backup_type_.type_ = task_info.backup_type_.type_;
    if (ObTenantBackupTaskItem::FINISH == task_info.status_) {
      status_ = OB_SUCCESS == task_info.result_ ? ObBackupSetFileInfo::SUCCESS : ObBackupSetFileInfo::FAILED;
    } else {
      status_ = ObBackupSetFileInfo::DOING;
    }
    result_ = task_info.result_;
    cluster_id_ = task_info.cluster_id_;
    backup_data_version_ = task_info.backup_data_version_;
    backup_schema_version_ = task_info.backup_schema_version_;
    partition_count_ = task_info.partition_count_;
    finish_partition_count_ = task_info.finish_partition_count_;
    encryption_mode_ = task_info.encryption_mode_;
    passwd_ = task_info.passwd_;
    start_replay_log_ts_ = task_info.start_replay_log_ts_;
    date_ = task_info.date_;
    if (ObBackupSetFileInfo::DOING == status_) {
      file_status_ = ObBackupFileStatus::BACKUP_FILE_COPYING;
    } else if (ObBackupSetFileInfo::SUCCESS == status_) {
      file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    } else if (ObBackupSetFileInfo::FAILED == status_) {
      file_status_ = ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
    }
    if (task_info.is_mark_deleted_) {
      file_status_ = ObBackupFileStatus::BACKUP_FILE_DELETING;
    }
    if (OB_FAIL(task_info.backup_dest_.get_backup_dest_str(backup_dest, OB_MAX_URI_LENGTH))) {
      LOG_WARN("failed to get backup dest str", K(ret), K(task_info));
    } else if (OB_FAIL(backup_dest_.assign(backup_dest))) {
      LOG_WARN("failed to assign backup dest str", K(ret), K(backup_dest));
    } else if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extract from backup task info", K(ret), K(*this), K(task_info));
    }
  }
  return ret;
}

int ObBackupSetFileInfo::extract_from_backup_info(
    const ObBaseBackupInfoStruct& info, const ObExternBackupInfo& extern_backup_info)
{
  int ret = OB_SUCCESS;
  if (!extern_backup_info.is_valid() || !info.is_valid() || info.backup_status_.is_stop_status()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract from extern backup info get invalid argument", K(ret), K(extern_backup_info), K(info));
  } else {
    tenant_id_ = info.tenant_id_;
    backup_set_id_ = info.backup_set_id_;
    incarnation_ = info.incarnation_;
    copy_id_ = 0;  // first backup image copy id is 0
    snapshot_version_ = info.backup_snapshot_version_;
    prev_full_backup_set_id_ = extern_backup_info.prev_full_backup_set_id_;
    prev_inc_backup_set_id_ = extern_backup_info.prev_inc_backup_set_id_;
    prev_backup_data_version_ = extern_backup_info.prev_backup_data_version_;
    pg_count_ = 0;
    macro_block_count_ = 0;
    finish_pg_count_ = 0;
    finish_macro_block_count_ = 0;
    input_bytes_ = 0;
    output_bytes_ = 0;
    start_time_ = 0;
    end_time_ = 0;
    compatible_ = extern_backup_info.compatible_;
    cluster_version_ = extern_backup_info.cluster_version_;
    backup_type_.type_ = extern_backup_info.backup_type_;
    status_ = ObBackupSetFileInfo::DOING;
    result_ = OB_SUCCESS;
    cluster_id_ = GCONF.cluster_id;
    backup_data_version_ = info.backup_data_version_;
    backup_schema_version_ = info.backup_schema_version_;
    partition_count_ = 0;
    finish_partition_count_ = 0;
    encryption_mode_ = info.encryption_mode_;
    passwd_ = info.passwd_;
    backup_dest_ = info.backup_dest_;
    file_status_ = ObBackupFileStatus::BACKUP_FILE_COPYING;
    start_replay_log_ts_ = 0;
    date_ = extern_backup_info.date_;
    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extract backup set file info from backup info", K(ret), K(info), K(extern_backup_info));
    }
  }
  return ret;
}

int ObBackupSetFileInfo::check_passwd(const char* passwd_array, const char* passwd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupUtils::check_passwd(passwd_array, passwd))) {
    LOG_WARN("failed to check passwd", KR(ret), K(passwd_array), K(passwd));
  }
  return ret;
}

int ObBackupSetFileInfo::convert_to_backup_backup_task_info(const int64_t job_id, const int64_t copy_id,
    const int64_t result, const share::ObBackupDest& dst_backup_dest, ObTenantBackupBackupsetTaskInfo& bb_task)
{
  int ret = OB_SUCCESS;
  if (job_id < 0 || copy_id <= 0 || !dst_backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_id), K(copy_id));
  } else if (OB_FAIL(bb_task.src_backup_dest_.set(backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_));
  } else {
    bb_task.tenant_id_ = tenant_id_;
    bb_task.job_id_ = job_id;
    bb_task.incarnation_ = incarnation_;
    bb_task.backup_set_id_ = backup_set_id_;
    bb_task.copy_id_ = copy_id;
    bb_task.backup_type_ = backup_type_;
    bb_task.snapshot_version_ = snapshot_version_;
    bb_task.prev_full_backup_set_id_ = prev_full_backup_set_id_;
    bb_task.prev_inc_backup_set_id_ = prev_inc_backup_set_id_;
    bb_task.prev_backup_data_version_ = prev_backup_data_version_;
    bb_task.input_bytes_ = input_bytes_;
    bb_task.output_bytes_ = output_bytes_;
    bb_task.start_ts_ = start_time_;
    bb_task.end_ts_ = end_time_;
    bb_task.compatible_ = compatible_;
    bb_task.cluster_id_ = cluster_id_;
    bb_task.cluster_version_ = cluster_version_;
    bb_task.task_status_ = ObTenantBackupBackupsetTaskItem::FINISH;
    bb_task.dst_backup_dest_ = dst_backup_dest;
    bb_task.backup_data_version_ = backup_data_version_;
    bb_task.backup_schema_version_ = backup_schema_version_;
    bb_task.total_pg_count_ = pg_count_;
    bb_task.finish_pg_count_ = finish_pg_count_;
    bb_task.total_partition_count_ = partition_count_;
    bb_task.finish_partition_count_ = finish_partition_count_;
    bb_task.total_macro_block_count_ = macro_block_count_;
    bb_task.finish_macro_block_count_ = finish_macro_block_count_;
    bb_task.result_ = result;
    bb_task.encryption_mode_ = encryption_mode_;
    bb_task.passwd_ = passwd_;
    bb_task.is_mark_deleted_ = false;
    bb_task.start_replay_log_ts_ = start_replay_log_ts_;
    bb_task.date_ = date_;
  }
  return ret;
}

int ObBackupSetFileInfo::get_backup_dest(share::ObBackupDest& backup_dest) const
{
  int ret = OB_SUCCESS;
  backup_dest.reset();
  if (backup_dest_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupSetFileInfo, tenant_id_, backup_set_id_, incarnation_, copy_id_, snapshot_version_,
    prev_full_backup_set_id_, prev_inc_backup_set_id_, prev_backup_data_version_, pg_count_, macro_block_count_,
    finish_pg_count_, finish_macro_block_count_, input_bytes_, output_bytes_, start_time_, end_time_, compatible_,
    cluster_version_, backup_type_.type_, status_, result_, cluster_id_, backup_dest_, backup_data_version_,
    backup_schema_version_, partition_count_, finish_partition_count_, encryption_mode_, passwd_, file_status_,
    start_replay_log_ts_, date_);

ObBackupRegion::ObBackupRegion() : region_(), priority_(-1)
{}

ObBackupRegion::~ObBackupRegion()
{}

void ObBackupRegion::reset()
{
  region_.reset();
  priority_ = -1;
}

int ObBackupRegion::set(const ObString& region, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (0 == region.length() || priority < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "set backup region get invalid argument", K(ret), K(region), K(priority));
  } else if (OB_FAIL(region_.assign(region))) {
    OB_LOG(WARN, "failed to assign region", K(ret), K(region));
  } else {
    priority_ = priority;
  }
  return ret;
}

ObBackupZone::ObBackupZone() : zone_(), priority_(-1)
{}

ObBackupZone::~ObBackupZone()
{}

void ObBackupZone::reset()
{
  zone_.reset();
  priority_ = -1;
}

int ObBackupZone::set(const ObString& zone, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (0 == zone.length() || priority < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "set backup zone get invalid argument", K(ret), K(zone), K(priority));
  } else if (OB_FAIL(zone_.assign(zone))) {
    OB_LOG(WARN, "failed to assign zone", K(ret), K(zone));
  } else {
    priority_ = priority;
  }
  return ret;
}

ObBackupSetIdPair::ObBackupSetIdPair() : backup_set_id_(0), copy_id_(0)
{}

void ObBackupSetIdPair::reset()
{
  backup_set_id_ = 0;
  copy_id_ = 0;
}

bool ObBackupSetIdPair::is_valid() const
{
  return backup_set_id_ > 0 && copy_id_ >= 0;
}

ObBackupPieceIdPair::ObBackupPieceIdPair() : backup_piece_id_(0), copy_id_(0)
{}

void ObBackupPieceIdPair::reset()
{
  backup_piece_id_ = 0;
  copy_id_ = 0;
}

bool ObBackupPieceIdPair::is_valid() const
{
  return backup_piece_id_ > 0 && copy_id_ >= 0;
}

ObBackupStatistics::ObBackupStatistics()
    : pg_count_(0),
      finish_pg_count_(0),
      partition_count_(0),
      finish_partition_count_(0),
      macro_block_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0)
{}

void ObBackupStatistics::reset()
{
  pg_count_ = 0;
  finish_pg_count_ = 0;
  partition_count_ = 0;
  finish_partition_count_ = 0;
  macro_block_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
}

bool ObBackupStatistics::is_valid() const
{
  return pg_count_ >= 0 && finish_pg_count_ >= 0 && partition_count_ >= 0 && finish_partition_count_ >= 0 &&
         macro_block_count_ >= 0 && finish_macro_block_count_ >= 0 && input_bytes_ >= 0 && output_bytes_ >= 0;
}
