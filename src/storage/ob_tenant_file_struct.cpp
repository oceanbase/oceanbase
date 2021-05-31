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

#include "ob_tenant_file_struct.h"
#include "storage/ob_file_system_util.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

OB_SERIALIZE_MEMBER(ObTenantFileKey, tenant_id_, file_id_);

void ObTenantFileKey::reset()
{
  tenant_id_ = OB_INVALID_ID;
  file_id_ = 0;
}

uint64_t ObTenantFileKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
  hash_val = common::murmurhash(&file_id_, sizeof(int64_t), hash_val);
  return hash_val;
}

bool ObTenantFileKey::operator==(const ObTenantFileKey& other) const
{
  return tenant_id_ == other.tenant_id_ && file_id_ == other.file_id_;
}

bool ObTenantFileKey::operator!=(const ObTenantFileKey& other) const
{
  return !operator==(other);
}

ObTenantFileSuperBlock::ObTenantFileSuperBlock()
    : macro_meta_entry_(), pg_meta_entry_(), status_(TENANT_FILE_INVALID), is_sys_table_file_(false)
{}

void ObTenantFileSuperBlock::reset()
{
  macro_meta_entry_.reset();
  pg_meta_entry_.reset();
  status_ = TENANT_FILE_INVALID;
}

bool ObTenantFileSuperBlock::is_valid() const
{
  return macro_meta_entry_.is_valid() && pg_meta_entry_.is_valid() && status_ > TENANT_FILE_INVALID &&
         status_ < TENANT_FILE_MAX;
}

OB_SERIALIZE_MEMBER(ObTenantFileSuperBlock, macro_meta_entry_, pg_meta_entry_, status_, is_sys_table_file_);

int ObTenantFileInfo::serialize_pg_map(char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, pg_map_.size()))) {
    LOG_WARN("fail to encode pg map count", K(ret));
  } else {
    PG_MAP& pg_map = const_cast<PG_MAP&>(pg_map_);
    for (PG_MAP::iterator iter = pg_map.begin(); OB_SUCC(ret) && iter != pg_map.end(); ++iter) {
      if (OB_FAIL(iter->first.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize pg key", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantFileInfo::deserialize_pg_map(const char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  lib::ObLabel label("file_info_map");
  int64_t pg_cnt = 0;
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &pg_cnt))) {
    LOG_WARN("fail to decode pg map count", K(ret));
  } else if (pg_cnt > 0) {
    if (OB_FAIL(pg_map_.create(ObTenantFileValue::MAX_REF_CNT_PER_FILE, label))) {
      LOG_WARN("fail to create pg map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pg_cnt; ++i) {
        ObPGKey pg_key;
        if (OB_FAIL(pg_key.deserialize(buf, buf_len, pos))) {
          LOG_WARN("fail to deserialize pg key", K(ret));
        } else if (OB_FAIL(pg_map_.set_refactored(pg_key, true))) {
          LOG_WARN("fail to set pg key", K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObTenantFileInfo::get_pg_map_serialize_size() const
{
  int64_t serialize_size = 0;
  const int64_t pg_cnt = pg_map_.size();
  serialize_size += serialization::encoded_length_i64(pg_cnt);
  PG_MAP& pg_map = const_cast<PG_MAP&>(pg_map_);
  for (PG_MAP::iterator iter = pg_map.begin(); iter != pg_map.end(); ++iter) {
    serialize_size += iter->first.get_serialize_size();
  }
  return serialize_size;
}

DEFINE_SERIALIZE(ObTenantFileInfo)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(tenant_key_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tenant file key", K(ret));
  } else if (OB_FAIL(tenant_file_super_block_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tenant file super block", K(ret));
  } else if (OB_FAIL(serialize_pg_map(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize pg map", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTenantFileInfo)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(tenant_key_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize tenant filek key", K(ret));
  } else if (OB_FAIL(tenant_file_super_block_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize tenant file super block", K(ret));
  } else if (OB_FAIL(deserialize_pg_map(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize pg map", K(ret));
  }
  return ret;
}

int64_t ObTenantFileInfo::get_serialize_size() const
{
  return tenant_key_.get_serialize_size() + tenant_file_super_block_.get_serialize_size() + get_pg_map_serialize_size();
}

int ObTenantFileInfo::add_pg(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key));
  } else if (!pg_map_.created()) {
    lib::ObLabel label("file_info_map");
    if (OB_FAIL(pg_map_.create(ObTenantFileValue::MAX_REF_CNT_PER_FILE, label))) {
      LOG_WARN("fail to create pg map", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pg_map_.set_refactored(pg_key, true))) {
      LOG_WARN("fail to add pg to pg map", K(ret), K(pg_key));
    }
  }
  return ret;
}

int ObTenantFileInfo::remove_pg(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key));
  } else if (!pg_map_.created()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(pg_map_.erase_refactored(pg_key))) {
    LOG_WARN("fail to erase pg from map", K(ret), K(pg_key));
  }
  return ret;
}

int ObTenantFileInfo::exist_pg(const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pg_key));
  } else if (!pg_map_.created()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(pg_map_.get_refactored(pg_key, exist))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get from map", K(ret), K(pg_key));
    }
  }
  return ret;
}

int ObTenantFileInfo::deep_copy(ObTenantFileInfo& dst_info)
{
  int ret = OB_SUCCESS;
  lib::ObLabel label("file_info_map");
  dst_info.reset();
  if (OB_FAIL(dst_info.pg_map_.create(ObTenantFileValue::MAX_REF_CNT_PER_FILE, label))) {
    LOG_WARN("fail to create pg map", K(ret));
  }
  for (PG_MAP::iterator iter = pg_map_.begin(); OB_SUCC(ret) && iter != pg_map_.end(); ++iter) {
    if (OB_FAIL(dst_info.pg_map_.set_refactored(iter->first, iter->second))) {
      LOG_WARN("fail to set to map", K(ret), K(*iter));
    }
  }
  if (OB_SUCC(ret)) {
    dst_info.tenant_key_ = tenant_key_;
    dst_info.tenant_file_super_block_ = tenant_file_super_block_;
  }
  return ret;
}

ObMetaBlockListHandle::ObMetaBlockListHandle() : meta_handles_(), cur_handle_pos_(0)
{
  meta_handles_[0].reset();
  meta_handles_[1].reset();
}

ObMetaBlockListHandle::~ObMetaBlockListHandle()
{
  reset();
}

int ObMetaBlockListHandle::add_macro_blocks(
    const ObIArray<blocksstable::MacroBlockId>& block_list, const bool need_switch_handle)
{
  int ret = OB_SUCCESS;
  ObMacroBlocksHandle& new_handle = meta_handles_[1 - cur_handle_pos_];
  ObMacroBlocksHandle& old_handle = meta_handles_[cur_handle_pos_];
  for (int64_t i = 0; OB_SUCC(ret) && i < block_list.count(); ++i) {
    if (OB_FAIL(new_handle.add(block_list.at(i)))) {
      LOG_WARN("fail to add macro block handle", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (need_switch_handle) {
      switch_handle();
    }
  } else {
    reset_new_handle();
  }
  return ret;
}

void ObMetaBlockListHandle::set_storage_file(blocksstable::ObStorageFile* file)
{
  meta_handles_[0].set_storage_file(file);
  meta_handles_[1].set_storage_file(file);
}

void ObMetaBlockListHandle::reset()
{
  cur_handle_pos_ = 0;
  meta_handles_[0].reset();
  meta_handles_[1].reset();
}

int ObMetaBlockListHandle::reserve(const int64_t block_count)
{
  int ret = OB_SUCCESS;
  if (block_count > 0) {
    if (OB_FAIL(meta_handles_[1 - cur_handle_pos_].reserve(block_count))) {
      LOG_WARN("fail to reserve meta handle", K(ret));
    }
  }
  return ret;
}

const ObIArray<MacroBlockId>& ObMetaBlockListHandle::get_meta_block_list() const
{
  return meta_handles_[cur_handle_pos_].get_macro_id_list();
}

void ObMetaBlockListHandle::switch_handle()
{
  meta_handles_[cur_handle_pos_].reset();
  cur_handle_pos_ = 1 - cur_handle_pos_;
}

void ObMetaBlockListHandle::reset_new_handle()
{
  meta_handles_[1 - cur_handle_pos_].reset();
}

void ObTenantFileValue::reset()
{
  meta_block_handle_.reset();
  if (nullptr != info_file_) {
    ObFileSystemUtil::close_file(info_file_);
    info_file_ = nullptr;
  }
  if (nullptr != storage_file_.file_) {
    ObFileSystemUtil::close_file(storage_file_.file_);
    storage_file_.file_ = nullptr;
  }
  from_svr_ckpt_ = false;
}

ObTenantFileFilter::ObTenantFileFilter() : tenant_file_key_()
{}

void ObTenantFileFilter::set_filter_tenant_file_key(const ObTenantFileKey& tenant_file_key)
{
  tenant_file_key_ = tenant_file_key;
}

void ObTenantFileFilter::reset()
{
  tenant_file_key_.reset();
}

int ObTenantFileFilter::is_filtered(const ObTenantFileKey& tenant_file_key, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (tenant_file_key_.is_valid()) {
    is_filtered = tenant_file_key_ != tenant_file_key;
  }
  return ret;
}

ObTenantFileCheckpointEntry::ObTenantFileCheckpointEntry() : tenant_file_key_(), super_block_(), meta_block_handle_()
{}

ObTenantFileCheckpointEntry::~ObTenantFileCheckpointEntry()
{
  reset();
}

void ObTenantFileCheckpointEntry::reset()
{
  tenant_file_key_.reset();
  super_block_.reset();
  meta_block_handle_.reset();
}

int ObTenantFileCheckpointEntry::assign(const ObTenantFileCheckpointEntry& other)
{
  int ret = OB_SUCCESS;
  tenant_file_key_ = other.tenant_file_key_;
  super_block_ = other.super_block_;
  meta_block_handle_.set_storage_file(
      const_cast<ObTenantFileCheckpointEntry&>(other).meta_block_handle_.get_storage_file());
  if (OB_FAIL(meta_block_handle_.add_macro_blocks(
          other.meta_block_handle_.get_meta_block_list(), true /*switch handle*/))) {
    LOG_WARN("fail to assign meta block handle", K(ret));
  }
  return ret;
}
