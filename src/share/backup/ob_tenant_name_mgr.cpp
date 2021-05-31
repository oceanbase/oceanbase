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
#include "ob_tenant_name_mgr.h"
#include "ob_backup_path.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_operator.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

OB_SERIALIZE_MEMBER(ObTenantNameSimpleMgr::ObTenantIdItem, tenant_id_, timestamp_);
ObTenantNameSimpleMgr::ObTenantIdItem::ObTenantIdItem() : tenant_id_(0), timestamp_(0)
{}

OB_SERIALIZE_MEMBER(ObTenantNameSimpleMgr::ObTenantNameInfo, tenant_name_, is_complete_, id_list_);
ObTenantNameSimpleMgr::ObTenantNameInfo::ObTenantNameInfo()
    : tenant_name_(), is_complete_(false), id_list_("tenant_name_id", DEFAULT_TENANT_ID_ITEM_COUNT)
{}

ObTenantNameSimpleMgr::ObTenantNameInfo::~ObTenantNameInfo()
{}

int ObTenantNameSimpleMgr::ObTenantNameInfo::complete()
{
  int ret = OB_SUCCESS;

  if (id_list_.empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("id list must not empty", K(ret), K(*this));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < id_list_.count(); ++i) {
      ObTenantIdItem& prev = id_list_.at(i - 1);
      ObTenantIdItem& curr = id_list_.at(i);
      if (prev.timestamp_ >= curr.timestamp_) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid timetamp sequence", K(ret), K(i), K(prev), K(curr), K(*this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_complete_ = true;
    LOG_INFO("complete info", K(*this));
  }
  return ret;
}

int ObTenantNameSimpleMgr::ObTenantNameInfo::add(const ObTenantIdItem& item)
{
  int ret = OB_SUCCESS;
  ObTenantIdCompare comp;
  is_complete_ = false;

  common::ObSEArray<ObTenantIdItem, DEFAULT_TENANT_ID_ITEM_COUNT>::iterator iter =
      std::lower_bound(id_list_.begin(), id_list_.end(), item, comp);

  if (iter == id_list_.end()) {
    if (OB_FAIL(id_list_.push_back(item))) {
      LOG_WARN("failed to add add item", K(ret));
    }
  } else if (iter->timestamp_ == item.timestamp_) {
    if (iter->tenant_id_ == item.tenant_id_) {
      // skip same
    } else if (iter->tenant_id_ != item.tenant_id_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("should not have two tenant with same timestamp", K(*iter), K(item), K(*this));
    }
  } else {
    if (OB_FAIL(id_list_.push_back(item))) {
      LOG_WARN("failed to add add item", K(ret));
    } else {
      std::sort(id_list_.begin(), id_list_.end(), comp);
    }
  }

  return ret;
}

bool ObTenantNameSimpleMgr::ObTenantIdCompare::operator()(const ObTenantIdItem& left, const ObTenantIdItem& right)
{
  return left.timestamp_ < right.timestamp_;
}

OB_SERIALIZE_MEMBER(ObTenantNameSimpleMgr::ObTenantNameMeta, schema_version_, tenant_count_);
ObTenantNameSimpleMgr::ObTenantNameMeta::ObTenantNameMeta() : schema_version_(0), tenant_count_(0)
{}

void ObTenantNameSimpleMgr::ObTenantNameMeta::reset()
{
  schema_version_ = 0;
  tenant_count_ = 0;
}

ObTenantNameSimpleMgr::ObTenantNameSimpleMgr()
    : is_inited_(false), meta_(), tenant_name_infos_(), allocator_("tenant_name_id")
{}

ObTenantNameSimpleMgr::~ObTenantNameSimpleMgr()
{
  reset();
}

void ObTenantNameSimpleMgr::reset()
{
  is_inited_ = false;
  meta_.reset();

  if (tenant_name_infos_.created()) {
    for (TenantNameHashMap::const_iterator iter = tenant_name_infos_.begin(); iter != tenant_name_infos_.end();
         ++iter) {
      iter->second->~ObTenantNameInfo();
    }
  }
  tenant_name_infos_.destroy();
  allocator_.reset();
}

int ObTenantNameSimpleMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(tenant_name_infos_.create(OB_MAX_SERVER_TENANT_CNT, "tenant_name_map"))) {
    LOG_WARN("failed to creaet map", K(ret));
  } else {
    meta_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObTenantNameSimpleMgr::assign(const ObTenantNameSimpleMgr& other)
{
  int ret = OB_SUCCESS;
  const int64_t count = other.tenant_name_infos_.size();

  reset();

  if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  }

  for (TenantNameHashMap::const_iterator iter = other.tenant_name_infos_.begin();
       OB_SUCC(ret) && iter != other.tenant_name_infos_.end();
       ++iter) {
    void* buf = nullptr;
    ObTenantNameInfo* new_info = nullptr;
    ObTenantNameInfo* other_info = iter->second;

    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTenantNameInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret));
    } else if (OB_ISNULL(new_info = new (buf) ObTenantNameInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new info", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, other_info->tenant_name_, new_info->tenant_name_))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(new_info->id_list_.assign(other_info->id_list_))) {
      LOG_WARN("failed to copy id list", K(ret));
    } else if (OB_FAIL(tenant_name_infos_.set_refactored(new_info->tenant_name_, new_info))) {
      LOG_WARN("failed to add new info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    meta_ = other.meta_;
  }
  return ret;
}

int64_t ObTenantNameSimpleMgr::get_schema_version() const
{
  return meta_.schema_version_;
}

int ObTenantNameSimpleMgr::complete(const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (schema_version < meta_.schema_version_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema version should not rollback", K(ret), K(schema_version), K(meta_));
  }

  for (TenantNameHashMap::const_iterator iter = tenant_name_infos_.begin();
       OB_SUCC(ret) && iter != tenant_name_infos_.end();
       ++iter) {
    if (OB_FAIL(iter->second->complete())) {
      LOG_WARN("failed to complete info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    meta_.tenant_count_ = tenant_name_infos_.size();
    meta_.schema_version_ = schema_version;
    LOG_INFO("finish complete", K(meta_), "map_size", tenant_name_infos_.size());
  }
  return ret;
}

int ObTenantNameSimpleMgr::add(const common::ObString& tenant_name, const int64_t timestamp, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObTenantNameInfo* info = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_SUCCESS != (hash_ret = tenant_name_infos_.get_refactored(tenant_name, info))) {
    if (hash_ret != OB_HASH_NOT_EXIST) {
      ret = hash_ret;
      LOG_WARN("failed to get info", K(ret), K(tenant_name));
    } else if (OB_FAIL(alloc_tenant_name_info_(tenant_name, info))) {
      LOG_WARN("failed to alloc tenant name info", K(ret), K(tenant_name));
    }
  }

  if (OB_SUCC(ret)) {
    ObTenantIdItem item;
    item.tenant_id_ = tenant_id;
    item.timestamp_ = timestamp;
    if (OB_ISNULL(info)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("info must not null", K(ret));
    } else if (OB_FAIL(info->add(item))) {
      LOG_WARN("failed to add item", K(ret));
    } else {
      meta_.reset();
      LOG_INFO("succeed to add new item", K(tenant_name), K(item), K(*info));
    }
  }

  return ret;
}

int ObTenantNameSimpleMgr::get_tenant_id(
    const common::ObString& tenant_name, const int64_t timestamp, uint64_t& tenant_id) const
{
  int ret = OB_SUCCESS;
  ObTenantNameInfo* info = nullptr;
  tenant_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (meta_.schema_version_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid schema version, tenant name mgr is not complete", K(ret), K(meta_.schema_version_));
  } else if (timestamp <= 0 || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(timestamp), K(tenant_name));
  } else if (OB_FAIL(tenant_name_infos_.get_refactored(tenant_name, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant name not exist", K(ret), K(tenant_name));
    } else {
      LOG_WARN("failed to get info", K(ret));
    }
  } else {
    for (int64_t i = info->id_list_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObTenantIdItem& item = info->id_list_.at(i);
      if (item.timestamp_ <= timestamp) {
        tenant_id = item.tenant_id_;
        break;
      }
    }

    if (OB_SUCC(ret) && tenant_id <= 0) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("cannot find tenant at this timestamp", K(ret), K(tenant_name), K(timestamp), K(*info));
    }
  }
  return ret;
}

int ObTenantNameSimpleMgr::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObTenantNameInfo info;
  ObTenantNameInfo* new_info = nullptr;
  const ObBackupCommonHeader* common_header = nullptr;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not intied", K(ret));
  } else if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else if (meta_.schema_version_ != 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("this simple mgr is not empty, cannot read buf", K(ret), K(meta_));
  } else {
    common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (common_header->data_zlength_ != common_header->data_length_ ||
               common_header->data_type_ != ObBackupFileType::BACKUP_TENANT_NAME_INFO ||
               common_header->data_version_ != TENANT_NAME_SIMPLE_MGR_VERSION) {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid common header", K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check archive backup info", K(ret), K(*common_header));
    } else if (OB_FAIL(meta_.deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize archive backup ifno", K(ret), K(*common_header));
    } else {
      LOG_INFO("succeed to read meta", K(meta_), K(pos), "length", common_header->data_zlength_);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.tenant_count_; ++i) {
    if (OB_FAIL(info.deserialize(buf, buf_len, pos))) {
      LOG_WARN("failed to deserialize", K(ret), K(buf_len), K(pos));
    } else if (!info.is_complete_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("info is not complete", K(ret), K(info));
    } else if (OB_FAIL(alloc_tenant_name_info_(info.tenant_name_, new_info))) {
      LOG_WARN("failed to alloc tenant name info", K(ret));
    } else if (OB_FAIL(new_info->id_list_.assign(info.id_list_))) {
      LOG_WARN("failed assign id list", K(ret), K(info));
    } else {
      LOG_INFO("succeed to read info from buf", K(i), K(*new_info), K(pos), "length", common_header->data_zlength_);
    }
  }
  return ret;
}

int ObTenantNameSimpleMgr::alloc_tenant_name_info_(const common::ObString& tenant_name, ObTenantNameInfo*& info)
{
  int ret = OB_SUCCESS;
  ObTenantNameInfo* new_info = nullptr;
  void* buf = allocator_.alloc(sizeof(ObTenantNameInfo));
  info = nullptr;

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (OB_ISNULL(new_info = new (buf) ObTenantNameInfo())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new info", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, tenant_name, new_info->tenant_name_))) {
    LOG_WARN("failed to copy tenant name", K(ret));
  } else if (OB_FAIL(tenant_name_infos_.set_refactored(new_info->tenant_name_, new_info))) {
    LOG_WARN("failed to set new info", K(ret));
  } else {
    info = new_info;
    new_info = nullptr;
    LOG_INFO("succeed to alloc_tenant_name_info_", K(tenant_name));
  }

  if (nullptr != new_info) {
    new_info->~ObTenantNameInfo();
    allocator_.free(new_info);
  }

  return ret;
}

int ObTenantNameSimpleMgr::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    // LOG_ARCHIVE_BACKUP_INFO_CONTENT_VERSION
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_TENANT_NAME_INFO;
    common_header->data_version_ = TENANT_NAME_SIMPLE_MGR_VERSION;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(write_buf_(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret));
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

int ObTenantNameSimpleMgr::write_buf_(char* buf, const int64_t buf_size, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> tenant_names;
  ObTenantNameInfo* info = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(pos), K(buf_size));
  } else if (meta_.tenant_count_ != tenant_name_infos_.size()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tenant count not match", K(ret), K(meta_), "map_count", tenant_name_infos_.size());
  } else if (OB_FAIL(meta_.serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize meta", K(ret));
  } else {
    LOG_INFO("succceed to write meta to buf", K(meta_));
  }
  for (TenantNameHashMap::const_iterator iter = tenant_name_infos_.begin();
       OB_SUCC(ret) && iter != tenant_name_infos_.end();
       ++iter) {
    ObString tenant_name = iter->first;
    if (OB_FAIL(tenant_names.push_back(tenant_name))) {
      LOG_WARN("failed to add tenant_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    std::sort(tenant_names.begin(), tenant_names.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_names.count(); ++i) {
      if (OB_FAIL(tenant_name_infos_.get_refactored(tenant_names.at(i), info))) {
        LOG_WARN("failed to get info", K(ret), K(i), "name", tenant_names.at(i));
      } else if (!info->is_complete_) {
        ret = OB_ERR_SYS;
        LOG_ERROR("info is not complete", K(ret), K(*info));
      } else if (OB_FAIL(info->serialize(buf, buf_size, pos))) {
        LOG_WARN("failed to serialize info", K(ret), K(*info));
      } else {
        LOG_INFO("succeed to write info to buf", K(*info), K(pos), K(buf_size));
      }
    }
  }

  return ret;
}

int64_t ObTenantNameSimpleMgr::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += meta_.get_serialize_size();

  for (TenantNameHashMap::const_iterator iter = tenant_name_infos_.begin(); iter != tenant_name_infos_.end(); ++iter) {
    size += iter->second->get_serialize_size();
  }

  return size;
}

int ObTenantNameSimpleMgr::read_backup_file(const ObClusterBackupDest& cluster_backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t read_size = 0;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (meta_.schema_version_ != 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot read backup simple mgr twice", K(ret), K(meta_));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_name_info_path(cluster_backup_dest, path))) {
    LOG_WARN("failed to get tenant name path", K(ret));
  } else if (OB_FAIL(util.get_file_length(path.get_obstr(), cluster_backup_dest.get_storage_info(), file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret));
    } else {
      LOG_INFO("backup tenant name info not exist", K(ret), K(path));
    }
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length));
  } else if (OB_FAIL(util.read_single_file(
                 path.get_obstr(), cluster_backup_dest.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(cluster_backup_dest));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(read_buf(buf, read_size))) {
    LOG_WARN("failed to read next mgr", K(ret));
  }
  return ret;
}

int ObTenantNameSimpleMgr::write_backup_file(const ObClusterBackupDest& cluster_backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = get_write_buf_size();
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_name_info_path(cluster_backup_dest, path))) {
    LOG_WARN("failed to get tenant name path", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_obstr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed to mkdir", K(ret), K(path));
  } else if (OB_FAIL(util.write_single_file(path.get_obstr(), cluster_backup_dest.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write simple mgr file", K(ret));
  }

  return ret;
}

int ObTenantNameSimpleMgr::get_infos(ObTenantNameMeta& meta, common::ObIArray<const ObTenantNameInfo*>& infos) const
{
  int ret = OB_SUCCESS;

  meta = meta_;
  for (ObTenantNameSimpleMgr::TenantNameHashMap::const_iterator it = tenant_name_infos_.begin();
       it != tenant_name_infos_.end();
       ++it) {
    const ObTenantNameInfo* info = it->second;
    if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("failed to add info", K(ret));
    }
  }
  return ret;
}

int ObTenantNameSimpleMgr::get_tenant_ids(hash::ObHashSet<uint64_t>& tenant_id_set)
{
  int ret = OB_SUCCESS;
  ObTenantNameInfo* info = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (meta_.schema_version_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid schema version, tenant name mgr is not complete", K(ret), K(meta_.schema_version_));
  } else {
    for (TenantNameHashMap::iterator iter = tenant_name_infos_.begin();
         OB_SUCC(ret) && iter != tenant_name_infos_.end();
         ++iter) {
      info = iter->second;
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant info should not be NULL", K(ret), KP(info));
      } else {
        const ObTenantIdItem& item = info->id_list_.at(0);
        int hash_ret = tenant_id_set.set_refactored(item.tenant_id_);
        if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to set tenant id into set", K(ret), K(*info));
        }
      }
    }
  }
  return ret;
}

ObTenantNameMgr::ObTenantNameMgr()
    : is_inited_(false),
      sql_proxy_(nullptr),
      schema_service_(nullptr),
      tenant_ids_(),
      cluster_backup_dest_(),
      last_update_timestamp_(0)
{
  cur_mgr_ = &simple_mgr_[0];
}

ObTenantNameMgr::~ObTenantNameMgr()
{}

int ObTenantNameMgr::init(common::ObMySQLProxy& sql_proxy, share::schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantNameMgr::cleanup()
{
  simple_mgr_[0].reset();
  simple_mgr_[1].reset();
  cur_mgr_ = &simple_mgr_[0];
  tenant_ids_.reset();
  cluster_backup_dest_.reset();
  last_update_timestamp_ = 0;
}

int ObTenantNameMgr::reload_backup_dest(const char* backup_dest, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest new_backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(backup_dest) || incarnation <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(backup_dest), K(incarnation));
  } else if (OB_FAIL(new_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest), K(incarnation));
  } else if (!cluster_backup_dest_.is_same(new_backup_dest)) {
    FLOG_INFO("change backup dest", K(cluster_backup_dest_), K(new_backup_dest));
    cluster_backup_dest_ = new_backup_dest;
    cur_mgr_->reset();
    last_update_timestamp_ = 0;
    tenant_ids_.reset();
  }
  return ret;
}

int ObTenantNameMgr::do_update(const bool is_force)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  int64_t schema_version = 0;
  ObTenantNameSimpleMgr* next_mgr = get_next_mgr_();
  last_update_timestamp_ = ObTimeUtil::current_time();
  next_mgr->reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!cluster_backup_dest_.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid backup dest", K(ret), K(cluster_backup_dest_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(get_backup_tenant_ids_from_schema_(schema_guard))) {
    LOG_WARN("get_backup_tenant_ids_from_schema_ failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  } else if (schema_version < cur_mgr_->get_schema_version()) {
    ret = OB_SCHEMA_NOT_UPTODATE;
    LOG_ERROR("schema version is not up to date",
        K(ret),
        K(schema_version),
        "mgr_schema_version",
        cur_mgr_->get_schema_version());
  } else if (!is_force && schema_version == cur_mgr_->get_schema_version()) {
    // do nothing
  } else {
    FLOG_INFO("schema version is newer, need update tenant name info",
        K(schema_version),
        "cur_mgr_schema_version",
        cur_mgr_->get_schema_version(),
        K(is_force));
    if (OB_FAIL(update_backup_simple_mgr_(schema_version))) {
      LOG_WARN("failed to write_backup_simple_mgr", K(ret));
    } else if (OB_FAIL(ObTenantBackupInfoOperation::update_tenant_name_backup_schema_version(
                   *sql_proxy_, schema_version))) {
      LOG_WARN("failed to update_tenant_name_backup_schema_version", K(ret), K(schema_version));
    }
  }

  if (OB_FAIL(ret)) {
    last_update_timestamp_ = 0;
  }

  return ret;
}

int ObTenantNameMgr::get_backup_tenant_ids_from_schema_(ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  tenant_ids_.reuse();

  if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    const ObTenantSchema* tenant_info = nullptr;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
      LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
    } else if (tenant_info->is_restore()) {
      // skip backup tenant is restore
      LOG_INFO("skip restoring tenant", K(tenant_id));
    } else if (OB_FAIL(tenant_ids_.push_back(tenant_id))) {
      LOG_WARN("failed to add tenant id", K(ret));
    }
  }
  return ret;
}

int ObTenantNameMgr::get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids, int64_t& last_update_ts)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  last_update_ts = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (last_update_timestamp_ <= 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("last update timestamp is invalid", K(ret), K(last_update_timestamp_));
  } else if (OB_FAIL(tenant_ids.assign(tenant_ids_))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else {
    last_update_ts = last_update_timestamp_;
  }

  return ret;
}

int ObTenantNameMgr::update_backup_simple_mgr_(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObTenantNameSimpleMgr* next_mgr = get_next_mgr_();
  next_mgr->reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (schema_version < cur_mgr_->get_schema_version()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "schema version is too small", K(ret), K(schema_version), "cur_schema_version", cur_mgr_->get_schema_version());
  } else if (OB_FAIL(next_mgr->init())) {
    LOG_WARN("failed to init next mgr", K(ret));
  } else if (OB_FAIL(next_mgr->read_backup_file(cluster_backup_dest_))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to load simple mgr", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (FAILEDx(add_new_tenant_name_(schema_version, *next_mgr))) {
    LOG_WARN("failed to add new tenant name", K(ret));
  } else if (OB_FAIL(next_mgr->write_backup_file(cluster_backup_dest_))) {
    LOG_WARN("failed to write backup simple mgr", K(ret));
  } else {
    cur_mgr_ = next_mgr;
  }

  return ret;
}

int ObTenantNameMgr::add_new_tenant_name_(const int64_t schema_version, ObTenantNameSimpleMgr& new_mgr)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    const char* FETCH_TENANT_NAME_HISTORY_SQL =
        "select tenant_id, tenant_name, schema_version from %s where tenant_name is not NULL and schema_version <= %ld";
    ObSqlString sql;
    uint64_t tenant_id = 0;
    ObString tenant_name;
    int64_t is_deleted = 0;
    int64_t timestamp = 0;
    int64_t real_length = 0;
    ObArenaAllocator allocator;
    uint64_t last_tenant_id = 0;
    ObString last_tenant_name;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (OB_FAIL(sql.append_fmt(FETCH_TENANT_NAME_HISTORY_SQL, OB_ALL_TENANT_HISTORY_TNAME, schema_version))) {
      LOG_WARN("failed to generate sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("failed to exec sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", K(ret));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tenant_name", tenant_name);
      EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", timestamp, int64_t);

      if (OB_SUCC(ret)) {
        if (tenant_id == last_tenant_id && 0 == tenant_name.compare(last_tenant_name)) {
          // same tenant name and id, do nothing
        } else if (OB_FAIL(new_mgr.add(tenant_name, timestamp, tenant_id))) {
          LOG_WARN("failed to add tenant name", K(ret));
        } else {
          last_tenant_name.reset();
          allocator.reset();
          if (OB_FAIL(ob_write_string(allocator, tenant_name, last_tenant_name))) {
            LOG_WARN("failed to copy tenant name", K(ret));
          } else {
            last_tenant_id = tenant_id;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_mgr.complete(schema_version))) {
        LOG_WARN("failed to complete new mgr", K(ret));
      }
    }
  }
  return ret;
}

ObTenantNameSimpleMgr* ObTenantNameMgr::get_next_mgr_()
{
  ObTenantNameSimpleMgr* next_mgr = &simple_mgr_[0];
  if (next_mgr == cur_mgr_) {
    next_mgr = &simple_mgr_[1];
  }
  return next_mgr;
}
