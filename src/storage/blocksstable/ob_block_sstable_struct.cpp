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
#include "ob_block_sstable_struct.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/utility.h"
#include "common/cell/ob_cell_writer.h"
#include "common/row/ob_row.h"
#include "common/log/ob_log_entry.h"
#include "share/config/ob_server_config.h"
#include "ob_data_buffer.h"
#include "ob_row_writer.h"
#include "ob_row_reader.h"
#include "ob_column_map.h"
#include "ob_store_file.h"
#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_tenant_file_struct.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace storage;
namespace blocksstable {

const char* BLOCK_SSTBALE_DIR_NAME = "sstable";
const char* BLOCK_SSTBALE_FILE_NAME = "block_file";

//================================ObStorageEnv======================================
bool ObStorageEnv::is_valid() const
{
  return NULL != data_dir_ && default_block_size_ > 0 && log_spec_.is_valid() && NULL != clog_dir_ &&
         NULL != ilog_dir_ && NULL != clog_shm_path_ && NULL != ilog_shm_path_ && index_cache_priority_ > 0 &&
         user_block_cache_priority_ > 0 && user_row_cache_priority_ > 0 && fuse_row_cache_priority_ > 0 &&
         bf_cache_priority_ > 0 && clog_cache_priority_ > 0 && index_clog_cache_priority_ > 0 && ethernet_speed_ > 0;
}

//================================SuperBlock======================================

ObSuperBlockHeader::ObSuperBlockHeader()
{
  reset();
}

bool ObSuperBlockHeader::is_valid() const
{
  return super_block_size_ > 0 && version_ > 0 && version_ < OB_SUPER_BLOCK_VERSION_MAX && 0 == attr_;
}

void ObSuperBlockHeader::reset()
{
  super_block_size_ = 0;
  version_ = 0;
  magic_ = 0;
  attr_ = 0;
}

DEFINE_SERIALIZE(ObSuperBlockHeader)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < sizeof(ObSuperBlockHeader)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN(
        "serialize superblock failed.", K(ret), KP(buf), K(buf_len), K(pos), "header_size", sizeof(ObSuperBlockHeader));
  } else {
    MEMCPY(buf + pos, this, sizeof(ObSuperBlockHeader));
    pos += sizeof(ObSuperBlockHeader);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSuperBlockHeader)
{
  int ret = OB_SUCCESS;
  // read size first;
  if (NULL == buf || data_len - pos < sizeof(ObSuperBlockHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(data_len), K(pos), "header_size", sizeof(ObSuperBlockHeader));
  } else {
    const int32_t header_size = *(reinterpret_cast<const int32_t*>(buf + pos));
    if (data_len - pos < header_size || sizeof(ObSuperBlockHeader) > header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("data_len not enough for header size.",
          K(ret),
          K(data_len),
          K(pos),
          K(header_size),
          "header_size",
          sizeof(ObSuperBlockHeader));
    } else {
      MEMCPY(this, buf + pos, sizeof(ObSuperBlockHeader));
      pos += sizeof(ObSuperBlockHeader);
    }
  }

  if (OB_SUCC(ret)) {
    if (version_ <= 0 || version_ >= OB_SUPER_BLOCK_VERSION_MAX) {
      ret = OB_ERR_SYS;
      LOG_ERROR("version not match", K(ret), K(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSuperBlockHeader)
{
  return sizeof(ObSuperBlockHeader);
}

ObSuperBlockHeaderV2::ObSuperBlockHeaderV2()
{
  reset();
}

bool ObSuperBlockHeaderV2::is_valid() const
{
  return version_ > 0 && version_ < OB_SUPER_BLOCK_VERSION_MAX;
}

void ObSuperBlockHeaderV2::reset()
{
  version_ = HEADER_VERSION;
  magic_ = SERVER_SUPER_BLOCK_MAGIC;
}

DEFINE_SERIALIZE(ObSuperBlockHeaderV2)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < sizeof(ObSuperBlockHeaderV2)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("serialize superblock failed.",
        K(ret),
        KP(buf),
        K(buf_len),
        K(pos),
        "header_size",
        sizeof(ObSuperBlockHeaderV2));
  } else {
    MEMCPY(buf + pos, this, sizeof(ObSuperBlockHeaderV2));
    pos += sizeof(ObSuperBlockHeaderV2);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSuperBlockHeaderV2)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len - pos < sizeof(ObSuperBlockHeaderV2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(data_len), K(pos), "header_size", sizeof(ObSuperBlockHeaderV2));
  } else {
    MEMCPY(this, buf + pos, sizeof(ObSuperBlockHeaderV2));
    pos += sizeof(ObSuperBlockHeaderV2);
  }

  if (OB_SUCC(ret)) {
    if (version_ <= 0 || version_ >= OB_SUPER_BLOCK_VERSION_MAX) {
      ret = OB_ERR_SYS;
      LOG_ERROR("version not match", K(ret), K(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSuperBlockHeaderV2)
{
  return sizeof(ObSuperBlockHeaderV2);
}

ObSuperBlockV1::ObSuperBlockV1()
    : header_(),
      create_timestamp_(0),
      modify_timestamp_(0),
      macro_block_size_(0),
      total_macro_block_count_(0),
      reserved_block_count_(0),
      free_macro_block_count_(0),
      first_macro_block_(0),
      first_free_block_index_(0),
      total_file_size_(0),
      backup_meta_count_(0),
      macro_block_meta_entry_block_index_(0),
      partition_meta_entry_block_index_(0),
      table_mgr_meta_entry_block_index_(0),
      partition_meta_log_seq_(0),
      table_mgr_meta_log_seq_(0)

{
  memset(backup_meta_blocks_, 0, sizeof(backup_meta_blocks_));
  memset(reserved_, 0, sizeof(reserved_));
}

bool ObSuperBlockV1::is_valid() const
{
  return header_.is_valid() && create_timestamp_ > 0 && modify_timestamp_ >= create_timestamp_ &&
         macro_block_size_ > 0 && total_macro_block_count_ > 0 && reserved_block_count_ >= 0 &&
         free_macro_block_count_ >= 0 && first_macro_block_ >= 0 && first_free_block_index_ > 0 &&
         total_file_size_ >= macro_block_size_ && backup_meta_count_ > 0 && macro_block_meta_entry_block_index_ >= -1 &&
         partition_meta_entry_block_index_ >= -1 && replay_start_point_.is_valid() &&
         table_mgr_meta_entry_block_index_ >= -1 && partition_meta_log_seq_ >= 0 && table_mgr_meta_log_seq_ >= 0;
}

DEFINE_SERIALIZE(ObSuperBlockV1)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < header_.super_block_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("serialize superblock failed.", KP(buf), K(buf_len), K(pos), K(header_), K(ret));
  } else if (sizeof(ObSuperBlockV1) != header_.super_block_size_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("super block v1 size not match", K(ret), K(header_), "super_block_size", sizeof(ObSuperBlockV1));
  } else {
    MEMCPY(buf + pos, this, sizeof(ObSuperBlockV1));
    pos += header_.super_block_size_;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSuperBlockV1)
{
  int ret = OB_SUCCESS;
  // read size first;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(int32_t))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(header_), K(ret));
  } else {
    int32_t header_size = *(reinterpret_cast<const int32_t*>(buf + pos));
    if (data_len - pos < header_size || data_len - pos < sizeof(ObSuperBlockV1)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("data_len not enough for header size.",
          K(data_len),
          K(pos),
          K(header_size),
          "super block v1 size",
          sizeof(ObSuperBlockV1),
          K(ret));
    } else {
      MEMCPY(this, buf + pos, sizeof(ObSuperBlockV1));
      pos += header_size;
      LOG_INFO("deserialize super block v1", K(pos), K(header_size), K(*this));
    }
  }

  if (OB_SUCC(ret)) {
    if (sizeof(ObSuperBlockV1) != header_.super_block_size_ || OB_SUPER_BLOCK_V1 != header_.version_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("super block v1 size not match", K(ret), K(header_), "super_block_size", sizeof(ObSuperBlockV1));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSuperBlockV1)
{
  return sizeof(ObSuperBlockV1);
}

int ObSuperBlockV1::read_super_block_buf(char* buf, const int64_t buf_size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t payload_start_pos = pos + serialization::encoded_length_i64(0);
  int64_t checksum = 0;
  int64_t cal_checksum = 0;

  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot read super block twice", K(ret), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_size, pos, &checksum))) {
    LOG_WARN("deserialize checksum error.", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (pos != payload_start_pos) {
    ret = OB_ERR_SYS;
    LOG_ERROR("pos not match payload_start_pos", K(ret), K(pos), K(payload_start_pos));
  } else if (OB_FAIL(deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (checksum !=
             (cal_checksum = static_cast<int64_t>(ob_crc64(buf + payload_start_pos, pos - payload_start_pos)))) {
    ret = OB_CHECKSUM_ERROR;
    LOG_ERROR(
        "super_block checksum error.", K(ret), K(checksum), K(cal_checksum), K(pos), K(payload_start_pos), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("Invalid data, ", K(ret), K(*this));
  } else {
    LOG_INFO("load superblock ok.", K(buf_size), K(pos), K(*this));
  }
  return ret;
}

int ObSuperBlockV1::write_super_block_buf(char* buf, const int64_t buf_size, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (!is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot write invalid super block", K(ret), K(*this));
  } else if (get_serialize_size() > ObSuperBlockV1::MAX_SUPER_BLOCK_SIZE ||
             get_serialize_size() != header_.super_block_size_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("super block size not valid", K(ret), "size", get_serialize_size(), K(*this));
  } else {
    MEMSET(buf + pos, 0, buf_size - pos);
    if (OB_FAIL(serialization::encode_i64(buf, buf_size, pos, ob_crc64(this, header_.super_block_size_)))) {
      LOG_ERROR("serialize checksum error", K(ret), K(pos));
    } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
      LOG_ERROR("serialize super block error", K(ret), K(pos), K(*this));
    }
  }

  return ret;
}

int ObSuperBlockV1::set_super_block(const ObSuperBlockV2& other)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(other));
  } else {
    MEMSET(this, 0, sizeof(ObSuperBlockV1));
    header_.super_block_size_ = sizeof(ObSuperBlockV1);
    header_.magic_ = SUPER_BLOCK_MAGIC;
    header_.version_ = OB_SUPER_BLOCK_V1;
    header_.attr_ = 0;

    backup_meta_count_ = ObSuperBlockV1::MAX_BACKUP_META_COUNT;
    backup_meta_blocks_[0] = 0;
    backup_meta_blocks_[1] = 1;

    create_timestamp_ = other.content_.create_timestamp_;
    modify_timestamp_ = other.content_.modify_timestamp_;
    macro_block_size_ = other.content_.macro_block_size_;
    total_macro_block_count_ = other.content_.total_macro_block_count_;
    reserved_block_count_ = backup_meta_count_;
    free_macro_block_count_ = other.content_.free_macro_block_count_;
    first_macro_block_ = backup_meta_blocks_[1] + 1;
    first_free_block_index_ = first_macro_block_;
    total_file_size_ = other.content_.total_file_size_;

    macro_block_meta_entry_block_index_ = other.content_.macro_block_meta_.block_index_;
    partition_meta_entry_block_index_ = other.content_.partition_meta_.block_index_;
    table_mgr_meta_entry_block_index_ = other.content_.table_mgr_meta_.block_index_;
    partition_meta_log_seq_ = other.content_.partition_meta_.log_seq_;
    table_mgr_meta_log_seq_ = other.content_.table_mgr_meta_.log_seq_;
    replay_start_point_.file_id_ = other.content_.replay_start_point_.file_id_;
    replay_start_point_.log_id_ = other.content_.replay_start_point_.log_id_;
    replay_start_point_.offset_ = other.content_.replay_start_point_.offset_;

    LOG_INFO("succeed to set super block with new version", K(*this), K(other));
  }

  return ret;
}

ObSuperBlockV2::MetaEntry::MetaEntry()
{
  reset();
}

bool ObSuperBlockV2::MetaEntry::is_valid() const
{
  return block_index_ >= -1 && log_seq_ >= 0 && file_id_ >= 0 && file_size_ >= 0;
}

void ObSuperBlockV2::MetaEntry::reset()
{
  block_index_ = -1;
  log_seq_ = 0;
  file_id_ = 0;
  file_size_ = 0;
}

OB_SERIALIZE_MEMBER(ObSuperBlockV2::MetaEntry, block_index_, log_seq_, file_id_, file_size_);

ObSuperBlockV2::SuperBlockContent::SuperBlockContent()
{
  reset();
}

bool ObSuperBlockV2::SuperBlockContent::is_valid() const
{
  return create_timestamp_ > 0 && modify_timestamp_ >= create_timestamp_ && macro_block_size_ > 0 &&
         total_macro_block_count_ > 0 && free_macro_block_count_ >= 0 && total_file_size_ >= macro_block_size_ &&
         replay_start_point_.is_valid() && macro_block_meta_.is_valid() && partition_meta_.is_valid() &&
         table_mgr_meta_.is_valid() && tenant_config_meta_.is_valid();
}

void ObSuperBlockV2::SuperBlockContent::reset()
{
  create_timestamp_ = 0;
  modify_timestamp_ = 0;
  macro_block_size_ = 0;
  total_macro_block_count_ = 0;
  free_macro_block_count_ = 0;
  total_file_size_ = 0;
  replay_start_point_.reset();
  macro_block_meta_.reset();
  partition_meta_.reset();
  table_mgr_meta_.reset();
  tenant_config_meta_.reset();
}

OB_SERIALIZE_MEMBER(ObSuperBlockV2::SuperBlockContent, create_timestamp_, modify_timestamp_, macro_block_size_,
    total_macro_block_count_, free_macro_block_count_, total_file_size_, replay_start_point_, macro_block_meta_,
    partition_meta_, table_mgr_meta_, tenant_config_meta_);

ObSuperBlockV2::ObSuperBlockV2() : header_(), content_()
{}

bool ObSuperBlockV2::is_valid() const
{
  return header_.is_valid() && content_.is_valid() && SUPER_BLOCK_MAGIC_V2 == header_.magic_;
}

int ObSuperBlockV2::serialize(char* buf, const int64_t buf_size, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t max_super_block_size = ObSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (!is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("cannot write invalid super block", K(ret), K(*this));
  } else if (get_serialize_size() > max_super_block_size || header_.super_block_size_ != get_serialize_size()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("super block size not valid",
        K(ret),
        "header_size",
        header_.get_serialize_size(),
        "content_size",
        content_.get_serialize_size(),
        K(max_super_block_size),
        K(*this));
  } else {
    MEMSET(buf + pos, 0, buf_size - pos);
    if (OB_FAIL(header_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block header", K(ret), K(buf_size), K(new_pos), K(*this));
    } else if (OB_FAIL(content_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block content", K(ret), K(buf_size), K(new_pos), K(*this));
    } else if (header_.super_block_size_ != new_pos - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("super block serialize block size not match", K(ret), K(new_pos), K(pos), K_(header));
    } else {
      pos = new_pos;
      LOG_INFO("succeed to write super block buf", K(buf_size), K(pos), K(*this));
    }
  }
  return ret;
}

int ObSuperBlockV2::deserialize(const char* buf, const int64_t buf_size, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot read super block twice", K(ret), K(*this));
  } else if (OB_FAIL(header_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_FAIL(content_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode content", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data, ", K(ret), K(*this));
  } else {
    LOG_INFO("load superblock ok.", K(buf_size), K(pos), K(*this));
  }
  return ret;
}

int ObSuperBlockV2::set_super_block(const ObSuperBlockV1& other)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(other));
  } else {
    header_.magic_ = SUPER_BLOCK_MAGIC_V2;
    header_.version_ = OB_SUPER_BLOCK_V2;
    header_.attr_ = 0;

    content_.create_timestamp_ = other.create_timestamp_;
    content_.modify_timestamp_ = other.modify_timestamp_;
    content_.macro_block_size_ = other.macro_block_size_;
    content_.total_macro_block_count_ = other.total_macro_block_count_;
    content_.free_macro_block_count_ = other.free_macro_block_count_;
    content_.total_file_size_ = other.total_file_size_;
    content_.replay_start_point_ = other.replay_start_point_;

    content_.macro_block_meta_.block_index_ = other.macro_block_meta_entry_block_index_;
    content_.macro_block_meta_.log_seq_ = 0;  // macro block meta has no log seq
    content_.macro_block_meta_.file_id_ = 0;
    content_.macro_block_meta_.file_size_ = 0;

    content_.partition_meta_.block_index_ = other.partition_meta_entry_block_index_;
    content_.partition_meta_.log_seq_ = other.partition_meta_log_seq_;
    content_.partition_meta_.file_id_ = 0;
    content_.partition_meta_.file_size_ = 0;

    content_.table_mgr_meta_.block_index_ = other.table_mgr_meta_entry_block_index_;
    content_.table_mgr_meta_.log_seq_ = other.table_mgr_meta_log_seq_;
    content_.table_mgr_meta_.file_id_ = 0;
    content_.table_mgr_meta_.file_size_ = 0;

    content_.tenant_config_meta_.block_index_ = 0;  // v1 doesn't have this
    content_.tenant_config_meta_.log_seq_ = 0;      // macro block meta has no log seq
    content_.tenant_config_meta_.file_id_ = 0;
    content_.tenant_config_meta_.file_size_ = 0;

    if (OB_FAIL(fill_super_block_size())) {
      LOG_WARN("failed to fill super block size", K(ret));
    } else {
      LOG_INFO("succeed to set super block with old version", K(*this), K(other));
    }
  }
  return ret;
}

void ObSuperBlockV2::reset()
{
  header_.reset();
  content_.reset();
}

int ObSuperBlockV2::fill_super_block_size()
{
  int ret = OB_SUCCESS;

  if (get_serialize_size() > ObSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE) {
    ret = OB_ERR_SYS;
    LOG_ERROR("super block size is too large, not support", K(ret), K(*this));
  } else {
    header_.super_block_size_ = static_cast<int32_t>(get_serialize_size());
    if (!is_valid() || header_.super_block_size_ != get_serialize_size()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid super block", K(ret), K(*this));
    }
  }
  return ret;
}

//======================ObSSTableMicroBlockHeader===============================
bool ObMicroBlockHeader::is_valid() const
{
  return header_size_ > 0 && version_ >= MICRO_BLOCK_HEADER_VERSION && MICRO_BLOCK_HEADER_MAGIC == magic_ &&
         attr_ >= 0 && column_count_ > 0 && row_index_offset_ > 0 && row_count_ > 0;
}

ObMicroBlockHeaderV2::ObMicroBlockHeaderV2()
    : header_size_(sizeof(*this)),
      version_(MICRO_BLOCK_HEADERV2_VERSION),
      row_count_(0),
      var_column_count_(0),
      row_data_offset_(0),
      opt_(0),
      reserved_(0)
{}

//=====================ObLinkedMacroBlockHeader========================
bool ObLinkedMacroBlockHeader::is_valid() const
{
  bool ret = header_size_ > 0 && version_ >= LINKED_MACRO_BLOCK_HEADER_VERSION && attr_ >= 0 &&
             meta_data_offset_ >= 0 && meta_data_count_ >= 0 && previous_block_index_ >= -1 &&
             total_previous_count_ >= 0;
  if (ret) {
    if (ObMacroBlockCommonHeader::PartitionMeta == attr_ && PARTITION_META_HEADER_MAGIC != magic_) {
      ret = false;
    }
    if (ObMacroBlockCommonHeader::MacroMeta == attr_ && MACRO_META_HEADER_MAGIC != magic_) {
      ret = false;
    }
    if (ObMacroBlockCommonHeader::SSTableData == attr_ && SSTABLE_DATA_HEADER_MAGIC != magic_) {
      ret = false;
    }
    if ((ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_) &&
        LOB_MACRO_BLOCK_HEADER_MAGIC != magic_) {
      ret = false;
    }
  }
  return ret;
}

//=====================ObLinkedMacroBlockHeaderV2========================
bool ObLinkedMacroBlockHeaderV2::is_valid() const
{
  bool b_ret = sizeof(ObLinkedMacroBlockHeaderV2) == header_size_ && LINKED_MACRO_BLOCK_HEADER_VERSION == version_ &&
               LINKED_MACRO_BLOCK_HEADER_MAGIC == magic_;
  return b_ret;
}
const MacroBlockId ObLinkedMacroBlockHeaderV2::get_previous_block_id() const
{
  MacroBlockId macro_id;
  macro_id.set_first_id(previous_block_first_id_);
  macro_id.set_second_id(previous_block_second_id_);
  macro_id.set_third_id(previous_block_third_id_);
  macro_id.set_fourth_id(previous_block_fourth_id_);
  return macro_id;
}

void ObLinkedMacroBlockHeaderV2::set_previous_block_id(const MacroBlockId& block_id)
{
  previous_block_first_id_ = block_id.first_id();
  previous_block_second_id_ = block_id.second_id();
  previous_block_third_id_ = block_id.third_id();
  previous_block_fourth_id_ = block_id.fourth_id();
}

int ObLinkedMacroBlockHeaderV2::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(buf), K(buf_len));
  } else if (pos + get_serialize_size() > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("linked header is invalid", K(ret), K(*this));
  } else {
    ObLinkedMacroBlockHeaderV2* linked_header = reinterpret_cast<ObLinkedMacroBlockHeaderV2*>(buf + pos);
    linked_header->header_size_ = header_size_;
    linked_header->version_ = version_;
    linked_header->magic_ = magic_;
    linked_header->attr_ = attr_;
    linked_header->item_count_ = item_count_;
    linked_header->fragment_offset_ = fragment_offset_;
    linked_header->previous_block_first_id_ = previous_block_first_id_;
    linked_header->previous_block_second_id_ = previous_block_second_id_;
    linked_header->previous_block_third_id_ = previous_block_third_id_;
    linked_header->previous_block_fourth_id_ = previous_block_fourth_id_;
    pos += linked_header->get_serialize_size();
  }
  return ret;
}

int ObLinkedMacroBlockHeaderV2::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(data_len - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer not enough", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObLinkedMacroBlockHeaderV2* ptr = reinterpret_cast<const ObLinkedMacroBlockHeaderV2*>(buf + pos);
    header_size_ = ptr->header_size_;
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    attr_ = ptr->attr_;
    item_count_ = ptr->item_count_;
    fragment_offset_ = ptr->fragment_offset_;
    previous_block_first_id_ = ptr->previous_block_first_id_;
    previous_block_second_id_ = ptr->previous_block_second_id_;
    previous_block_third_id_ = ptr->previous_block_third_id_;
    previous_block_fourth_id_ = ptr->previous_block_fourth_id_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      STORAGE_LOG(ERROR, "deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

int64_t ObLinkedMacroBlockHeaderV2::get_serialize_size(void) const
{
  return sizeof(ObLinkedMacroBlockHeaderV2);
}

//=====================ObSSTableMacroBlockHeader========================
bool ObSSTableMacroBlockHeader::is_valid() const
{
  return header_size_ > 0 && version_ >= ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION &&
         SSTABLE_DATA_HEADER_MAGIC == magic_ && attr_ >= 0 && OB_INVALID_ID != table_id_ && data_version_ >= 0 &&
         column_count_ >= rowkey_column_count_ && rowkey_column_count_ > 0 && column_index_scale_ >= 0 &&
         row_store_type_ >= 0 && row_count_ > 0 && occupy_size_ > 0 && micro_block_count_ > 0 &&
         micro_block_size_ > 0 && micro_block_data_offset_ > 0 && micro_block_data_size_ > 0 &&
         micro_block_index_offset_ >= micro_block_data_offset_ && micro_block_index_size_ > 0 &&
         micro_block_endkey_offset_ >= micro_block_index_offset_ && micro_block_endkey_size_ > 0 &&
         data_checksum_ >= 0 && partition_id_ >= -1 && encrypt_id_ >= 0 && master_key_id_ >= -1;
}

ObMacroBlockSchemaInfo::ObMacroBlockSchemaInfo()
    : column_number_(0),
      rowkey_column_number_(0),
      schema_version_(0),
      schema_rowkey_col_cnt_(0),
      compressor_(nullptr),
      column_id_array_(nullptr),
      column_type_array_(nullptr),
      column_order_array_(nullptr)
{}

bool ObMacroBlockSchemaInfo::operator==(const ObMacroBlockSchemaInfo& other) const
{
  bool bret = false;
  bret = column_number_ == other.column_number_ && rowkey_column_number_ == other.rowkey_column_number_ &&
         schema_version_ == other.schema_version_ && schema_rowkey_col_cnt_ == other.schema_rowkey_col_cnt_;

  if (nullptr == compressor_ && nullptr == other.compressor_) {
  } else if (nullptr != compressor_ && nullptr != other.compressor_) {
    bret = bret && (0 == STRCMP(compressor_, other.compressor_));
  } else {
    bret = false;
  }

  if (nullptr == column_id_array_ && nullptr == other.column_id_array_) {
  } else if (nullptr != column_id_array_ && nullptr != other.column_id_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_id_array_[i] == other.column_id_array_[i];
    }
  } else {
    bret = false;
  }

  if (nullptr == column_type_array_ && nullptr == other.column_type_array_) {
  } else if (nullptr != column_type_array_ && nullptr != other.column_type_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_type_array_[i] == other.column_type_array_[i];
    }
  } else {
    bret = false;
  }

  if (nullptr == column_order_array_ && nullptr == other.column_order_array_) {
  } else if (nullptr != column_order_array_ && nullptr != other.column_order_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_order_array_[i] == other.column_order_array_[i];
    }
  } else {
    bret = false;
  }

  return bret;
}

int ObMacroBlockSchemaInfo::serialize(char* buf, int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObBufferWriter buffer_writer(buf, data_len, pos);
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = MACRO_BLOCK_SCHEMA_INFO_HEADER_VERSION;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_writer.write(header_size))) {
    LOG_WARN("serialization header_size error.",
        K(header_size),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(header_version))) {
    LOG_WARN("serialization header_version error.",
        K(header_version),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(column_number_))) {
    LOG_WARN("fail to write column number", K(ret));
  } else if (OB_FAIL(buffer_writer.write(rowkey_column_number_))) {
    LOG_WARN("fail to write rowkey column number", K(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_version_))) {
    LOG_WARN("fail to write schema version", K(ret));
  } else if (OB_FAIL(buffer_writer.write(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to write schema rowkey column cnt", K(ret));
  } else if (OB_FAIL(buffer_writer.append_fmt("%s", compressor_))) {
    LOG_WARN("fail to append fmt", K(ret));
  } else if (column_number_ > 0) {
    for (int32_t i = 0; i < column_number_ && nullptr != column_id_array_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(buffer_writer.write(column_id_array_[i]))) {
        LOG_WARN("serialization column_id_array_ error.",
            K(column_id_array_[i]),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      }
    }
    for (int32_t i = 0; i < column_number_ && nullptr != column_type_array_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(buffer_writer.write(column_type_array_[i]))) {
        LOG_WARN("serialization column_type_array_ error.",
            K(column_type_array_[i]),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      }
    }

    for (int32_t i = 0; i < column_number_ && nullptr != column_order_array_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(buffer_writer.write(column_order_array_[i]))) {
        LOG_WARN("serialization column_order_array_ error.",
            K(column_order_array_[i]),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int32_t size = static_cast<int32_t>(buffer_writer.pos() - start_pos);
    char* size_ptr = buffer_writer.data() + start_pos;
    *reinterpret_cast<int32_t*>(size_ptr) = size;
    pos = buffer_writer.pos();
  }

  return ret;
}

int ObMacroBlockSchemaInfo::deserialize(const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("fail to read header size", K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("fail to read header version", K(ret));
  } else if (OB_FAIL(buffer_reader.read(column_number_))) {
    LOG_WARN("fail to read column number", K(ret));
  } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
    LOG_WARN("fail to read rowkey column number", K(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_version_))) {
    LOG_WARN("fail to read schema version", K(ret));
  } else if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
    LOG_WARN("fail to read schema rowkey column count", K(ret));
  } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
    LOG_WARN(
        "fail to deserialize compressor", K(ret), K_(compressor), K(buffer_reader.capacity()), K(buffer_reader.pos()));
  } else if (column_number_ > 0) {
    char* column_array = buffer_reader.current();
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    column_id_array_ = reinterpret_cast<uint16_t*>(column_array);
    column_array += column_id_size;
    column_type_array_ = reinterpret_cast<ObObjMeta*>(column_array);
    column_array += column_type_size;
    column_order_array_ = reinterpret_cast<ObOrderType*>(column_array);
    if (OB_FAIL(buffer_reader.advance(total_array_size))) {
      LOG_WARN("fail to advance buffer reader", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos = buffer_reader.pos();
  }
  return ret;
}

int64_t ObMacroBlockSchemaInfo::get_serialize_size() const
{
  int64_t serialize_size = 0;
  int32_t header_size = 0;
  int32_t header_version = 0;
  serialize_size += sizeof(header_size);
  serialize_size += sizeof(header_version);
  serialize_size += sizeof(column_number_);
  serialize_size += sizeof(rowkey_column_number_);
  serialize_size += sizeof(schema_version_);
  serialize_size += sizeof(schema_rowkey_col_cnt_);
  if (nullptr != compressor_) {
    serialize_size += strlen(compressor_) + 1;
  }
  if (column_number_ > 0) {
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    serialize_size += total_array_size;
  }
  return serialize_size;
}

int64_t ObMacroBlockSchemaInfo::get_deep_copy_size() const
{
  int64_t deep_copy_size = 0;
  deep_copy_size = sizeof(ObMacroBlockSchemaInfo);
  if (nullptr != compressor_) {
    deep_copy_size += strlen(compressor_) + 1;
  }
  if (column_number_ > 0) {
    const int64_t column_id_size = column_number_ * sizeof(uint16_t);
    const int64_t column_type_size = column_number_ * sizeof(ObObjMeta);
    const int64_t column_order_size = column_number_ * sizeof(ObOrderType);
    const int64_t total_array_size = column_id_size + column_type_size + column_order_size;
    deep_copy_size += total_array_size;
  }
  return deep_copy_size;
}

int ObMacroBlockSchemaInfo::deep_copy(ObMacroBlockSchemaInfo*& new_schema_info, ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t size = get_deep_copy_size();
  new_schema_info = nullptr;
  char* buf = nullptr;
  int64_t pos = 0;
  int64_t item_size = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size), K(*this));
  } else {
    new_schema_info = new (buf) ObMacroBlockSchemaInfo();
    new_schema_info->column_number_ = column_number_;
    new_schema_info->rowkey_column_number_ = rowkey_column_number_;
    new_schema_info->schema_version_ = schema_version_;
    new_schema_info->schema_rowkey_col_cnt_ = schema_rowkey_col_cnt_;
    pos += sizeof(ObMacroBlockSchemaInfo);
    if (OB_SUCC(ret) && nullptr != compressor_) {
      item_size = strlen(compressor_) + 1;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->compressor_ = buf + pos;
        MEMCPY(new_schema_info->compressor_, compressor_, item_size - 1);
        new_schema_info->compressor_[item_size - 1] = '\0';
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && NULL != column_id_array_) {
      item_size = sizeof(uint16_t) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_id_array_ = reinterpret_cast<uint16_t*>(buf + pos);
        MEMCPY(new_schema_info->column_id_array_, column_id_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && nullptr != column_type_array_) {
      item_size = sizeof(ObObjMeta) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_type_array_ = reinterpret_cast<ObObjMeta*>(buf + pos);
        MEMCPY(new_schema_info->column_type_array_, column_type_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCC(ret) && NULL != column_order_array_) {
      item_size = sizeof(ObOrderType) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        new_schema_info->column_order_array_ = reinterpret_cast<ObOrderType*>(buf + pos);
        MEMCPY(new_schema_info->column_order_array_, column_order_array_, item_size);
        pos += item_size;
      }
    }
  }
  return ret;
  ;
}

int64_t ObMacroBlockSchemaInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(column_number), K_(rowkey_column_number), K_(schema_version), K_(schema_rowkey_col_cnt), K_(compressor));
  J_COMMA();
  if (NULL != column_id_array_ && column_number_ > 0) {
    J_KV("column_id_array", ObArrayWrap<uint16_t>(column_id_array_, column_number_));
  }
  if (NULL != column_type_array_ && column_number_ > 0) {
    J_KV("column_type_array", ObArrayWrap<ObObjMeta>(column_type_array_, column_number_));
  }
  if (NULL != column_order_array_ && column_number_ > 0) {
    J_KV("column_order_array", ObArrayWrap<ObOrderType>(column_order_array_, column_number_));
  }
  return pos;
}

// ------ ObMacroBlockMeta-------------------
ObMacroBlockMeta::ObMacroBlockMeta()
    : attr_(0),
      data_version_(0),
      column_number_(0),
      rowkey_column_number_(0),
      column_index_scale_(0),
      row_store_type_(0),
      row_count_(0),
      occupy_size_(0),
      data_checksum_(0),
      micro_block_count_(0),
      micro_block_data_offset_(0),
      micro_block_index_offset_(0),
      micro_block_endkey_offset_(0),
      compressor_(NULL),
      column_id_array_(NULL),
      column_type_array_(NULL),
      column_checksum_(NULL),
      endkey_(NULL),
      table_id_(0),
      data_seq_(-1),
      schema_version_(0),
      snapshot_version_(0),
      schema_rowkey_col_cnt_(0),
      column_order_array_(NULL),
      row_count_delta_(0),
      micro_block_mark_deletion_offset_(0),
      macro_block_deletion_flag_(false),
      micro_block_delta_offset_(0),
      partition_id_(-1),
      column_checksum_method_(CCM_UNKOWN),
      progressive_merge_round_(0),
      write_seq_(0),
      bf_flag_(0),
      create_timestamp_(0),
      retire_timestamp_(0),
      collation_free_endkey_(NULL),
      encrypt_id_(0),
      master_key_id_(0)
{
  encrypt_key_[0] = '\0';
}

bool ObMacroBlockMeta::operator==(const ObMacroBlockMeta& other) const
{
  bool bret = false;
  bret = attr_ == other.attr_ && data_version_ == other.data_version_ && column_number_ == other.column_number_ &&
         rowkey_column_number_ == other.rowkey_column_number_ && column_index_scale_ == other.column_index_scale_ &&
         row_store_type_ == other.row_store_type_ && row_count_ == other.row_count_ &&
         occupy_size_ == other.occupy_size_ && data_checksum_ == other.data_checksum_ &&
         micro_block_count_ == other.micro_block_count_ && micro_block_data_offset_ == other.micro_block_data_offset_ &&
         micro_block_index_offset_ == other.micro_block_index_offset_ &&
         micro_block_endkey_offset_ == other.micro_block_endkey_offset_ && table_id_ == other.table_id_ &&
         data_seq_ == other.data_seq_ && schema_version_ == other.schema_version_ &&
         snapshot_version_ == other.snapshot_version_ && schema_rowkey_col_cnt_ == other.schema_rowkey_col_cnt_ &&
         row_count_delta_ == other.row_count_delta_ &&
         micro_block_mark_deletion_offset_ == other.micro_block_mark_deletion_offset_ &&
         macro_block_deletion_flag_ == other.macro_block_deletion_flag_ &&
         micro_block_delta_offset_ == other.micro_block_delta_offset_ && partition_id_ == other.partition_id_ &&
         column_checksum_method_ == other.column_checksum_method_ &&
         progressive_merge_round_ == other.progressive_merge_round_ && encrypt_id_ == other.encrypt_id_ &&
         master_key_id_ == other.master_key_id_;

  if (NULL == compressor_ && NULL == other.compressor_) {
    ;
  } else if (NULL != compressor_ && NULL != other.compressor_) {
    bret = bret && (0 == STRCMP(compressor_, other.compressor_));
  } else {
    bret = false;
  }

  if (NULL == column_id_array_ && NULL == other.column_id_array_) {
    ;
  } else if (NULL != column_id_array_ && NULL != other.column_id_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_id_array_[i] == other.column_id_array_[i];
    }
  } else {
    bret = false;
  }

  if (NULL == column_type_array_ && NULL == other.column_type_array_) {
    ;
  } else if (NULL != column_type_array_ && NULL != other.column_type_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_type_array_[i] == other.column_type_array_[i];
    }
  } else {
    bret = false;
  }

  if (NULL == column_order_array_ && NULL == other.column_order_array_) {
    ;
  } else if (NULL != column_order_array_ && NULL != other.column_order_array_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_order_array_[i] == other.column_order_array_[i];
    }
  } else {
    bret = false;
  }

  if (NULL == column_checksum_ && NULL == other.column_checksum_) {
    ;
  } else if (NULL != column_checksum_ && NULL != other.column_checksum_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_checksum_[i] == other.column_checksum_[i];
    }
  } else {
    bret = false;
  }

  if (NULL == endkey_ && NULL == other.endkey_) {
    ;
  } else if (NULL != endkey_ && NULL != other.endkey_) {
    for (int64_t i = 0; bret && i < rowkey_column_number_; ++i) {
      bret = endkey_[i] == other.endkey_[i];
    }
  } else {
    bret = false;
  }

  bret = bret && (0 == STRNCMP(encrypt_key_, other.encrypt_key_, sizeof(encrypt_key_)));
  return bret;
}

bool ObMacroBlockMeta::operator!=(const ObMacroBlockMeta& other) const
{
  return !(*this == other);
}

int ObMacroBlockMeta::serialize(char* buf, int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = ObMacroBlockMetaV2::MACRO_BLOCK_META_VERSION_V1;
  ObBufferWriter buffer_writer(buf, data_len, pos);

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta is not valid data.", K(*this), K(ret));
  } else if (OB_FAIL(buffer_writer.write(header_size))) {
    LOG_WARN("serialization header_size error.",
        K(header_size),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(header_version))) {
    LOG_WARN("serialization header_version error.",
        K(header_version),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(attr_))) {
    LOG_WARN("serialization attr_ error.", K_(attr), K(buffer_writer.capacity()), K(buffer_writer.pos()), K(ret));
  } else if (OB_FAIL(buffer_writer.write(data_version_))) {
    LOG_WARN("serialization data_version_ error.",
        K_(data_version),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()),
        K(ret));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_writer.write(column_number_))) {
      LOG_WARN("serialization column_number_ error.",
          K_(column_number),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(rowkey_column_number_))) {
      LOG_WARN("serialization rowkey_column_number_ error.",
          K_(rowkey_column_number),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(column_index_scale_))) {
      LOG_WARN("serialization column_index_scale_ error.",
          K_(column_index_scale),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(row_store_type_))) {
      LOG_WARN("serialization flag_ error.",
          K_(row_store_type),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(row_count_))) {
      LOG_WARN("serialization row_count_ error.",
          K_(row_count),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(occupy_size_))) {
      LOG_WARN("serialization occupy_size_ error.",
          K_(occupy_size),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(data_checksum_))) {
      LOG_WARN("serialization data_checksum_ error.",
          K_(data_checksum),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_count_))) {
      LOG_WARN("serialization micro_block_count_ error.",
          K_(micro_block_count),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_data_offset_))) {
      LOG_WARN("serialization micro_block_data_offset_ error.",
          K_(micro_block_data_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_index_offset_))) {
      LOG_WARN("serialization micro_block_index_offset_ error.",
          K_(micro_block_index_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_endkey_offset_))) {
      LOG_WARN("serialization micro_block_endkey_offset_ error.",
          K_(micro_block_endkey_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.append_fmt("%s", compressor_))) {
      LOG_WARN("serialization compressor_ error.",
          K_(compressor),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {
      // serialization column_ids_ & column_checksum_ array
      for (int32_t i = 0; i < column_number_ && NULL != column_id_array_ && OB_SUCCESS == ret; ++i) {
        if (OB_FAIL(buffer_writer.write(column_id_array_[i]))) {
          LOG_WARN("serialization column_id_array_ error.",
              K(column_id_array_[i]),
              K(buffer_writer.capacity()),
              K(buffer_writer.pos()),
              K(ret));
        }
      }

      for (int32_t i = 0; i < column_number_ && NULL != column_type_array_ && OB_SUCCESS == ret; ++i) {
        if (OB_FAIL(buffer_writer.write(column_type_array_[i]))) {
          LOG_WARN("serialization column_type_array_ error.",
              K(column_type_array_[i]),
              K(buffer_writer.capacity()),
              K(buffer_writer.pos()),
              K(ret));
        }
      }

      for (int32_t i = 0; i < column_number_ && NULL != column_checksum_ && OB_SUCCESS == ret; ++i) {
        if (OB_FAIL(buffer_writer.write(column_checksum_[i]))) {
          LOG_WARN("serialization column_checksum_ error.",
              K(column_checksum_[i]),
              K(buffer_writer.capacity()),
              K(buffer_writer.pos()),
              K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      if (OB_FAIL(write_compact_rowkey(
              buffer_writer, endkey_, rowkey_column_number_, static_cast<ObRowStoreType>(row_store_type_)))) {
        LOG_WARN(" write_compact_rowkey failed.",
            KP_(endkey),
            K_(rowkey_column_number),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K_(attr),
            K(ret));
      }
    }

    // serialization rowkey;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_writer.write(table_id_))) {
        LOG_WARN("serialization table_id_ error.",
            K_(table_id),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(data_seq_))) {
        LOG_WARN("serialization data_seq_ error.",
            K_(data_seq),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(schema_version_))) {
        LOG_WARN("serialization schema_version_ error.",
            K_(schema_version),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(snapshot_version_))) {
        LOG_WARN("serialization snapshot_version_ error.",
            K_(snapshot_version),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(schema_rowkey_col_cnt_))) {
        LOG_WARN("serialization schema_rowkey_col_cnt_ error.",
            K_(schema_rowkey_col_cnt),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      }
    }

    // serialize column_orders_ array
    if (OB_SUCCESS == ret && column_number_ > 0) {
      for (int32_t i = 0; i < column_number_ && NULL != column_order_array_ && OB_SUCCESS == ret; ++i) {
        if (OB_FAIL(buffer_writer.write(column_order_array_[i]))) {
          LOG_WARN("serialization column_order_array_ error.",
              K(column_order_array_[i]),
              K(buffer_writer.capacity()),
              K(buffer_writer.pos()),
              K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_writer.write(row_count_delta_))) {
        LOG_WARN("serialization row_count_delta error",
            K_(row_count_delta),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(micro_block_mark_deletion_offset_))) {
        LOG_WARN("serialization micro_block_mark_deletion_offset_ error.",
            K_(micro_block_mark_deletion_offset),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(macro_block_deletion_flag_))) {
        LOG_WARN("serialization macro_block_deletion_flag_ error.",
            K_(macro_block_deletion_flag),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(micro_block_delta_offset_))) {
        LOG_WARN("serialize micro_block_delta_offset error",
            K_(micro_block_delta_offset),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(partition_id_))) {
        LOG_WARN("serialize partition id error", K(ret), K(partition_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(column_checksum_method_))) {
        LOG_WARN("serialize partition id error", K(ret), K(column_checksum_method_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(progressive_merge_round_))) {
        LOG_WARN("fail to write progressive merge round", K(ret));
      } else if (OB_FAIL(buffer_writer.write(encrypt_id_))) {
        LOG_WARN("serialize encrypt id error", K(ret), K(encrypt_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(master_key_id_))) {
        LOG_WARN("serialize encrypt id error", K(ret), K(master_key_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(encrypt_key_, sizeof(encrypt_key_)))) {
        LOG_WARN("serialization encrypt_key error.", K(encrypt_key_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int32_t size = static_cast<int32_t>(buffer_writer.pos() - start_pos);
    char* size_ptr = buffer_writer.data() + start_pos;
    *reinterpret_cast<int32_t*>(size_ptr) = size;
    pos = buffer_writer.pos();
  }

  return ret;
}

int ObMacroBlockMeta::deserialize(const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);

  if (NULL == endkey_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The endkey is NULL, can not deserialize, ", K(ret));
  } else if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.",
        K(header_size),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.",
        K(header_version),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (OB_FAIL(buffer_reader.read(attr_))) {
    LOG_WARN(
        "deserialization attr_ error.", K_(attr), K(ret), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (OB_FAIL(buffer_reader.read(data_version_))) {
    LOG_WARN("deserialization data_version_ error.",
        K_(data_version),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_reader.read(column_number_))) {
      LOG_WARN("deserialization column_number_ error.",
          K_(column_number),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
      LOG_WARN("deserialization rowkey_column_number_ error.",
          K_(rowkey_column_number),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(column_index_scale_))) {
      LOG_WARN("deserialization column_index_scale_ error.",
          K_(column_index_scale),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_store_type_))) {
      LOG_WARN("deserialization flag_ error.",
          K_(row_store_type),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_count_))) {
      LOG_WARN("deserialization row_count_ error.",
          K_(row_count),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(occupy_size_))) {
      LOG_WARN("deserialization occupy_size_ error.",
          K_(occupy_size),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
      LOG_WARN("deserialization data_checksum_ error.",
          K_(data_checksum),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_count_))) {
      LOG_WARN("deserialization micro_block_count_ error.",
          K_(micro_block_count),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_data_offset_))) {
      LOG_WARN("deserialization micro_block_data_offset_ error.",
          K_(micro_block_data_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_index_offset_))) {
      LOG_WARN("deserialization micro_block_index_offset_ error.",
          K_(micro_block_index_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_endkey_offset_))) {
      LOG_WARN("deserialization micro_block_endkey_offset_ error.",
          K_(micro_block_endkey_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read_cstr(compressor_))) {
      LOG_WARN("deserialization compressor_ error.",
          K_(compressor),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {
      int64_t cip = 0;
      int64_t ctp = cip + column_number_ * sizeof(uint16_t);   // column type array start position
      int64_t ckp = ctp + column_number_ * sizeof(ObObjMeta);  // column checksum array start position
      int64_t ekp = ckp + column_number_ * sizeof(int64_t);    // endkey start position
      char* column_array_start = buffer_reader.current();

      if (OB_ISNULL(column_array_start)) {
        ret = OB_ERR_SYS;
        LOG_WARN("column array is NULL.", K(ret));
      } else if (OB_FAIL(buffer_reader.advance(ekp))) {
        LOG_WARN("remain buffer length not enough for column array.", K(ekp), K(buffer_reader.remain()), K(ret));
      } else {
        column_id_array_ = reinterpret_cast<uint16_t*>(column_array_start + cip);
        column_type_array_ = reinterpret_cast<ObObjMeta*>(column_array_start + ctp);
        column_checksum_ = reinterpret_cast<int64_t*>(column_array_start + ckp);
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      // deserialize rowkey;
      if (OB_FAIL(read_compact_rowkey(buffer_reader,
              column_type_array_,
              endkey_,
              rowkey_column_number_,
              static_cast<ObRowStoreType>(row_store_type_)))) {
        LOG_WARN("read compact rowkey failed.", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(table_id_))) {
          LOG_WARN("deserialization table_id_ error.",
              K_(table_id),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(data_seq_))) {
          LOG_WARN("deserialization data_seq_ error.",
              K_(data_seq),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as -1;
        data_seq_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_version_))) {
          LOG_WARN("deserialization schema_version_ error.",
              K_(schema_version),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        schema_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(snapshot_version_))) {
          LOG_WARN("deserialization snapshot_version_ error.",
              K_(snapshot_version),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as -1;
        snapshot_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
          LOG_WARN("deserialization schema_rowkey_col_cnt error.",
              K_(schema_rowkey_col_cnt),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        schema_rowkey_col_cnt_ = 0;
      }
    }

    if (OB_SUCC(ret) && column_number_ > 0) {
      if (buffer_reader.pos() - start_pos < header_size) {
        int64_t order_array_size = column_number_ * sizeof(ObOrderType);
        char* column_order_array_start = buffer_reader.current();

        if (OB_ISNULL(column_order_array_start)) {
          ret = OB_ERR_SYS;
          LOG_WARN("column order array is NULL.", K(ret));
        } else if (OB_FAIL(buffer_reader.advance(order_array_size))) {
          LOG_WARN("remain buffer length not enough for column order array.",
              K(order_array_size),
              K(buffer_reader.remain()),
              K(ret));
        } else {
          column_order_array_ = reinterpret_cast<ObOrderType*>(column_order_array_start);
        }
      } else {
        // older version, with no order_array info in the header
        column_order_array_ = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(row_count_delta_))) {
          LOG_WARN("deserialization row_count_delta error.",
              K_(row_count_delta),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        row_count_delta_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_mark_deletion_offset_))) {
          LOG_WARN("deserialization micro_block_mark_deletion_offset_ error.",
              K_(micro_block_mark_deletion_offset),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        micro_block_mark_deletion_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(macro_block_deletion_flag_))) {
          LOG_WARN("deserialization macro_block_deletion_flag_ error.",
              K_(macro_block_deletion_flag),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as false;
        macro_block_deletion_flag_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_delta_offset_))) {
          LOG_WARN("deserialization micro_block_delta_offset error.",
              K_(micro_block_delta_offset),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        micro_block_delta_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(partition_id_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        partition_id_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(column_checksum_method_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        column_checksum_method_ = CCM_UNKOWN;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(progressive_merge_round_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        progressive_merge_round_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(master_key_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        master_key_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_key_, sizeof(encrypt_key_)))) {
          LOG_WARN("deserialization encrypt_key_ error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_key_[0] = '\0';
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos > header_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN(
            "elapsed buffer size larger than object.", K(header_size), K(buffer_reader.pos()), K(start_pos), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_reader.set_pos(start_pos + header_size))) {
      LOG_WARN("set pos on buffer reader failed.", K(ret));
    } else {
      pos = buffer_reader.pos();
    }
  }
  return ret;
}

int64_t ObMacroBlockMeta::get_serialize_size() const
{
  return get_meta_content_serialize_size() + sizeof(int32_t) * 2;
}

bool ObMacroBlockMeta::is_valid() const
{
  bool ret = true;
  if (!(attr_ >= ObMacroBlockCommonHeader::Free && attr_ < ObMacroBlockCommonHeader::MaxMacroType)) {
    ret = false;
  } else if (is_normal_data_block()) {
    if (rowkey_column_number_ <= 0 || column_number_ < rowkey_column_number_ || schema_rowkey_col_cnt_ < 0) {
      ret = false;
    } else if (column_index_scale_ < 0 || row_store_type_ < 0 || row_count_ <= 0 || occupy_size_ <= 0 ||
               data_checksum_ < 0 || micro_block_count_ <= 0 || micro_block_data_offset_ < 0 ||
               micro_block_index_offset_ < micro_block_data_offset_ ||
               micro_block_endkey_offset_ < micro_block_index_offset_ || micro_block_mark_deletion_offset_ < 0 ||
               micro_block_delta_offset_ < 0) {
      ret = false;
    } else if (0 != column_number_) {
      if (NULL == column_id_array_ || NULL == column_type_array_ || NULL == column_checksum_ || NULL == endkey_) {
        ret = false;
      }
    } else if ((0 == micro_block_mark_deletion_offset_ && 0 < micro_block_delta_offset_) ||
               (0 == micro_block_delta_offset_ && 0 < micro_block_mark_deletion_offset_)) {
      ret = false;
    } else if (partition_id_ < -1) {
      ret = false;
    } else if (column_checksum_method_ < CCM_UNKOWN || column_checksum_method_ >= CCM_MAX) {
      ret = false;
    } else if (progressive_merge_round_ < 0) {
      ret = false;
    }
  }

  return ret;
}

int64_t ObMacroBlockMeta::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(attr),
      K_(data_version),
      K_(column_number),
      K_(rowkey_column_number),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(data_checksum),
      K_(table_id),
      K_(data_seq),
      K_(schema_version),
      K_(snapshot_version),
      K_(schema_rowkey_col_cnt),
      K_(row_count_delta));
  J_COMMA();
  J_KV(K_(micro_block_count),
      K_(micro_block_data_offset),
      K_(micro_block_index_offset),
      K_(micro_block_endkey_offset),
      K_(compressor),
      K_(write_seq),
      K_(bf_flag),
      K_(create_timestamp),
      K_(retire_timestamp),
      K_(micro_block_mark_deletion_offset),
      K_(macro_block_deletion_flag),
      K_(micro_block_delta_offset),
      K_(partition_id),
      K_(column_checksum_method),
      K_(encrypt_id),
      K_(master_key_id),
      K_(encrypt_key));
  J_COMMA();
  if (is_data_block()) {
    if (NULL != column_id_array_ && column_number_ > 0) {
      J_KV("column_id_array", ObArrayWrap<uint16_t>(column_id_array_, column_number_));
    }
    if (NULL != column_type_array_ && column_number_ > 0) {
      J_KV("column_type_array", ObArrayWrap<ObObjMeta>(column_type_array_, column_number_));
    }
    if (NULL != column_order_array_ && column_number_ > 0) {
      J_KV("column_order_array", ObArrayWrap<ObOrderType>(column_order_array_, column_number_));
    }
    if (NULL != column_checksum_ && column_number_ > 0) {
      J_KV("column_checksum", ObArrayWrap<int64_t>(column_checksum_, column_number_));
    }
    J_COMMA();
    if (NULL != endkey_ && rowkey_column_number_ > 0) {
      ObStoreRowkey rowkey(endkey_, rowkey_column_number_);
      J_KV(K(rowkey));
    }
    J_COMMA();
    if (NULL != collation_free_endkey_ && rowkey_column_number_ > 0) {
      ObStoreRowkey collation_free_endkey(collation_free_endkey_, rowkey_column_number_);
      J_KV(K(collation_free_endkey));
    }
  }
  return pos;
}

int64_t ObMacroBlockMeta::get_meta_content_serialize_size() const
{
  int64_t size = sizeof(ObMacroBlockMeta);  // struct self
  if (is_data_block()) {
    if (NULL != compressor_) {
      size += strlen(compressor_) + 1;  // compressor_ string length.
    }
    if (NULL != column_id_array_) {
      size += sizeof(int16_t) * column_number_;  // column ids array
    }
    if (NULL != column_type_array_) {
      size += sizeof(ObObjMeta) * column_number_;  // column type array
    }
    if (NULL != column_checksum_) {
      size += sizeof(int64_t) * column_number_;  // column_checksum_ array
    }
    if (NULL != endkey_) {
      // FIXME-: Not sure if we can simply use ObRowkey deep_copy_size
      // or if we need to add the type_ size
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      size += rowkey.get_deep_copy_size();  // rowkey object array
    }
    // regardless of whether column_order_array is NULL
    // because we may need to fill the column_order_array with default value (for compatibility)
    size += sizeof(ObOrderType) * column_number_;
  }
  return size;
}

// Here ObRowkey is used only as an object array holder.
int ObMacroBlockMeta::get_deep_copy_size(
    ObRowkey& collation_free_endkey, common::ObIAllocator& allocator, int64_t& size) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  size = get_meta_content_serialize_size();
  if (is_normal_data_block()) {
    size += sizeof(int64_t) * rowkey_column_number_;  // empty_read_cnt_ array
    if (NULL != endkey_) {                            // collation_free_endkey_
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
        LOG_WARN("fail to check if need to transform to collation free", K(ret));
      } else if (need_transform) {
        if (OB_FAIL(rowkey.to_collation_free_rowkey(collation_free_endkey, allocator))) {
          LOG_WARN("fail to get collation free rowkey", K(ret));
        } else if (0 != collation_free_endkey.get_obj_cnt()) {
          size += collation_free_endkey.get_deep_copy_size();
        }
      }
    }
  }

  return ret;
}

int ObMacroBlockMeta::check_collation_free_valid(bool& is_collation_free_valid) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  is_collation_free_valid = true;
  if (!is_normal_data_block()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block meta attr", K(ret), K(attr_));
  } else if (NULL != endkey_) {
    ObStoreRowkey rowkey(endkey_, rowkey_column_number_);
    if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
      LOG_WARN("fail to check need transform to collation free", K(ret));
    } else if (need_transform && NULL == collation_free_endkey_) {
      is_collation_free_valid = false;
    }
  }
  return ret;
}

int ObMacroBlockMeta::deep_copy(ObMacroBlockMeta*& meta_ptr, common::ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t item_size = 0;
  ObRowkey collation_free_endkey;
  common::ObArenaAllocator tmp_allocator(ObModIds::OB_MACRO_BLOCK_META_TEMP);
  int64_t size = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta is not valid data.", K(*this), K(ret));
  } else if (OB_FAIL(get_deep_copy_size(collation_free_endkey, tmp_allocator, size))) {
    LOG_WARN("fail to get deep copy size", K(ret));
  } else if (NULL == (meta_ptr = reinterpret_cast<ObMacroBlockMeta*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate meta memory failed.", K(size), K(ret));
  } else {
    // copy structure;
    MEMCPY(meta_ptr, this, sizeof(ObMacroBlockMeta));
    pos += sizeof(ObMacroBlockMeta);
  }

  // todo  deep copy lob macroblock will skip empty_read_cnt_ and collation free check
  if (OB_SUCCESS == ret && is_data_block()) {
    if (OB_SUCCESS == ret && NULL != compressor_) {
      item_size = strlen(compressor_) + 1;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        // copy checksum;
        meta_ptr->compressor_ = (char*)meta_ptr + pos;
        strcpy(meta_ptr->compressor_, compressor_);
        pos += item_size;
      }
    }

    if (OB_SUCCESS == ret && NULL != column_id_array_) {
      item_size = sizeof(uint16_t) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        // copy checksum;
        meta_ptr->column_id_array_ = reinterpret_cast<uint16_t*>((char*)meta_ptr + pos);
        MEMCPY(meta_ptr->column_id_array_, column_id_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCCESS == ret && NULL != column_type_array_) {
      item_size = sizeof(ObObjMeta) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        // copy checksum;
        meta_ptr->column_type_array_ = reinterpret_cast<ObObjMeta*>((char*)meta_ptr + pos);
        MEMCPY(meta_ptr->column_type_array_, column_type_array_, item_size);
        pos += item_size;
      }
    }

    if (OB_SUCCESS == ret && NULL != column_checksum_) {
      item_size = sizeof(int64_t) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        // copy checksum;
        meta_ptr->column_checksum_ = reinterpret_cast<int64_t*>((char*)meta_ptr + pos);
        MEMCPY(meta_ptr->column_checksum_, column_checksum_, item_size);
        pos += item_size;
      }
    }
  }

  if (OB_SUCC(ret) && is_normal_data_block()) {
    if (NULL == endkey_ || rowkey_column_number_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("endkey is empty.", KP_(endkey), K_(rowkey_column_number), K(ret));
    } else {
      // copy endkey
      // ObRowkey used only as an ObObj array holder, not involved in any comparison
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      bool need_transform = false;
      meta_ptr->endkey_ = reinterpret_cast<ObObj*>((char*)meta_ptr + pos);
      if (OB_FAIL(rowkey.deep_copy((char*)meta_ptr->endkey_, size - pos))) {
        LOG_WARN("copy rowkey error.", K(to_cstring(rowkey)), K(ret), K(size - pos));
      } else {
        pos += rowkey.get_deep_copy_size();
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
        LOG_WARN("fail to check if need to transform to collation free", K(ret));
      } else if (need_transform && (0 != collation_free_endkey.get_obj_cnt())) {
        int64_t endkey_size = collation_free_endkey.get_deep_copy_size();
        if (pos + endkey_size > size) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("deep copy meta buffer overflow.",
              K(ret),
              K(pos),
              K(size),
              "collation free key size",
              collation_free_endkey.get_deep_copy_size());
        } else {
          meta_ptr->collation_free_endkey_ = reinterpret_cast<ObObj*>(reinterpret_cast<char*>(meta_ptr) + pos);
          if (OB_FAIL(collation_free_endkey.deep_copy(
                  reinterpret_cast<char*>(meta_ptr->collation_free_endkey_), size - pos))) {
            LOG_WARN("deep copy failed", K(ret), K(collation_free_endkey), K(size - pos));
          } else {
            pos += endkey_size;
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret && is_data_block()) {
    item_size = sizeof(ObOrderType) * column_number_;
    if (pos + item_size > size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
    } else {
      meta_ptr->column_order_array_ = reinterpret_cast<ObOrderType*>((char*)meta_ptr + pos);
      if (NULL != column_order_array_) {
        // copy order_array;
        MEMCPY(meta_ptr->column_order_array_, column_order_array_, item_size);
      } else {  // column_order_array_ may be NULL if the source meta is deserialized from an older version of data
        for (int16_t i = 0; i < column_number_; i++) {
          meta_ptr->column_order_array_[i] = ObOrderType::ASC;
        }
      }
      pos += item_size;
    }
  }

  return ret;
}

int ObMacroBlockMeta::extract_columns(common::ObIArray<share::schema::ObColDesc>& columns) const
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  columns.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_number_; ++i) {
    col_desc.col_id_ = column_id_array_[i];
    col_desc.col_type_ = column_type_array_[i];
    col_desc.col_order_ = column_order_array_[i];
    if (OB_FAIL(columns.push_back(col_desc))) {
      LOG_WARN("Fail to push col_desc to columns, ", K(ret));
    }
  }
  return ret;
}

ObSSTableColumnMeta::ObSSTableColumnMeta() : column_id_(0), column_default_checksum_(0), column_checksum_(0)
{}

ObSSTableColumnMeta::~ObSSTableColumnMeta()
{}

bool ObSSTableColumnMeta::operator==(const ObSSTableColumnMeta& other) const
{
  return column_id_ == other.column_id_ && column_default_checksum_ == other.column_default_checksum_ &&
         column_checksum_ == other.column_checksum_;
}

bool ObSSTableColumnMeta::operator!=(const ObSSTableColumnMeta& other) const
{
  return !(*this == other);
}

void ObSSTableColumnMeta::reset()
{
  column_id_ = 0;
  column_default_checksum_ = 0;
  column_checksum_ = 0;
}

OB_SERIALIZE_MEMBER(ObSSTableColumnMeta, column_id_, column_default_checksum_, column_checksum_);

ObSSTableBaseMeta::ObSSTableBaseMeta()
    : inner_alloc_("SSTableBaseMeta"), column_metas_(&inner_alloc_), new_column_metas_(&inner_alloc_)
{
  reset();
}

ObSSTableBaseMeta::ObSSTableBaseMeta(common::ObIAllocator& alloc) : column_metas_(&alloc), new_column_metas_(&alloc)
{
  reset();
}

bool ObSSTableBaseMeta::operator==(const ObSSTableBaseMeta& other) const
{
  bool bret = false;
  bret = index_id_ == other.index_id_ && row_count_ == other.row_count_ && occupy_size_ == other.occupy_size_ &&
         data_checksum_ == other.data_checksum_ && row_checksum_ == other.row_checksum_ &&
         rowkey_column_count_ == other.rowkey_column_count_ && table_type_ == other.table_type_ &&
         index_type_ == other.index_type_ && available_version_ == other.available_version_ &&
         macro_block_count_ == other.macro_block_count_ &&
         use_old_macro_block_count_ == other.use_old_macro_block_count_ && column_cnt_ == other.column_cnt_ &&
         macro_block_second_index_ == other.macro_block_second_index_ &&
         total_sstable_count_ == other.total_sstable_count_ && max_logic_block_index_ == other.max_logic_block_index_ &&
         lob_macro_block_count_ == other.lob_macro_block_count_ &&
         lob_use_old_macro_block_count_ == other.lob_use_old_macro_block_count_ &&
         checksum_method_ == other.checksum_method_ && logical_data_version_ == other.logical_data_version_ &&
         has_compact_row_ == other.has_compact_row_ && contain_uncommitted_row_ == other.contain_uncommitted_row_ &&
         upper_trans_version_ == other.upper_trans_version_ &&
         max_merged_trans_version_ == other.max_merged_trans_version_ && end_log_id_ == other.end_log_id_;

  if (bret) {
    for (int64_t i = 0; bret && i < column_cnt_; ++i) {
      bret = (column_metas_.at(i) == other.column_metas_.at(i));
    }
  }

  if (bret) {
    for (int64_t i = 0; bret && i < column_cnt_; ++i) {
      bret = (new_column_metas_.at(i) == other.new_column_metas_.at(i));
    }
  }
  return bret;
}

bool ObSSTableBaseMeta::operator!=(const ObSSTableBaseMeta& other) const
{
  return !(*this == other);
}

bool ObSSTableBaseMeta::is_valid() const
{
  bool ret = (index_id_ >= 0 && row_count_ >= 0 && occupy_size_ >= 0 && data_checksum_ >= 0 && row_checksum_ >= 0 &&
              data_version_ >= 0 && rowkey_column_count_ >= 0 && table_type_ >= 0 && index_type_ >= 0 &&
              available_version_ >= 0 && macro_block_count_ >= 0 && use_old_macro_block_count_ >= 0 &&
              column_cnt_ >= rowkey_column_count_ && column_cnt_ == column_metas_.count() &&
              macro_block_second_index_ >= 0 && max_logic_block_index_ >= -1 && create_snapshot_version_ >= 0 &&
              lob_use_old_macro_block_count_ >= 0 && lob_macro_block_count_ >= 0 && table_mode_.is_valid() &&
              (sstable_format_version_ < SSTABLE_FORMAT_VERSION_5 ||
                  (sstable_format_version_ == SSTABLE_FORMAT_VERSION_5 && logical_data_version_ >= data_version_) ||
                  (logical_data_version_ > 0)) &&
              upper_trans_version_ >= 0 && max_merged_trans_version_ >= 0);
  return ret;
}

void ObSSTableBaseMeta::reset()
{
  index_id_ = 0;
  row_count_ = 0;
  occupy_size_ = 0;
  data_checksum_ = 0;
  row_checksum_ = 0;
  data_version_ = 0;
  rowkey_column_count_ = 0;
  table_type_ = USER_TABLE;
  index_type_ = INDEX_TYPE_IS_NOT;
  available_version_ = 0;
  macro_block_count_ = 0;
  use_old_macro_block_count_ = 0;
  column_cnt_ = 0;
  column_metas_.reset();

  sstable_format_version_ = SSTABLE_FORMAT_VERSION_1;
  macro_block_second_index_ = 0;
  total_sstable_count_ = 0;
  max_logic_block_index_ = -1;
  build_on_snapshot_ = false;
  create_index_base_version_ = 0;
  schema_version_ = 0;
  progressive_merge_start_version_ = 0;
  progressive_merge_end_version_ = 0;
  create_snapshot_version_ = 0;
  lob_macro_block_count_ = 0;
  lob_use_old_macro_block_count_ = 0;
  checksum_method_ = CCM_TYPE_AND_VALUE;
  new_column_metas_.reset();
  progressive_merge_round_ = 0;
  progressive_merge_step_ = 0;
  table_mode_.reset();
  pg_key_.reset();
  logical_data_version_ = 0;
  multi_version_rowkey_type_ = ObMultiVersionRowkeyHelpper::MVRC_VERSION_MAX;
  contain_uncommitted_row_ = false;
  upper_trans_version_ = 0;
  max_merged_trans_version_ = 0;
  end_log_id_ = 0;
  has_compact_row_ = true;
}

int ObSSTableBaseMeta::assign(const ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;
  index_id_ = meta.index_id_;
  row_count_ = meta.row_count_;
  occupy_size_ = meta.occupy_size_;
  data_checksum_ = meta.data_checksum_;
  row_checksum_ = meta.row_checksum_;
  data_version_ = meta.data_version_;
  rowkey_column_count_ = meta.rowkey_column_count_;
  table_type_ = meta.table_type_;
  index_type_ = meta.index_type_;
  available_version_ = meta.available_version_;
  macro_block_count_ = meta.macro_block_count_;
  use_old_macro_block_count_ = meta.use_old_macro_block_count_;
  column_cnt_ = meta.column_cnt_;

  sstable_format_version_ = meta.sstable_format_version_;
  macro_block_second_index_ = meta.macro_block_second_index_;
  total_sstable_count_ = meta.total_sstable_count_;
  max_logic_block_index_ = meta.max_logic_block_index_;
  build_on_snapshot_ = meta.build_on_snapshot_;
  create_index_base_version_ = meta.create_index_base_version_;
  schema_version_ = meta.schema_version_;
  progressive_merge_start_version_ = meta.progressive_merge_start_version_;
  progressive_merge_end_version_ = meta.progressive_merge_end_version_;
  create_snapshot_version_ = meta.create_snapshot_version_;
  lob_macro_block_count_ = meta.lob_macro_block_count_;
  lob_use_old_macro_block_count_ = meta.lob_use_old_macro_block_count_;
  checksum_method_ = meta.checksum_method_;
  progressive_merge_round_ = meta.progressive_merge_round_;
  progressive_merge_step_ = meta.progressive_merge_step_;
  table_mode_ = meta.table_mode_;
  pg_key_ = meta.pg_key_;
  logical_data_version_ = meta.logical_data_version_;
  multi_version_rowkey_type_ = meta.multi_version_rowkey_type_;
  upper_trans_version_ = meta.upper_trans_version_;
  max_merged_trans_version_ = meta.max_merged_trans_version_;
  contain_uncommitted_row_ = meta.contain_uncommitted_row_;
  end_log_id_ = meta.end_log_id_;
  has_compact_row_ = meta.has_compact_row_;

  if (OB_FAIL(column_metas_.assign(meta.column_metas_))) {
    LOG_WARN("fail to assign column metas", K(ret));
  } else if (OB_FAIL(new_column_metas_.assign(meta.new_column_metas_))) {
    LOG_WARN("fail to assign new column metas", K(ret));
  }
  return ret;
}

int ObSSTableBaseMeta::check_data(const ObSSTableBaseMeta& other_meta)
{
  int ret = OB_SUCCESS;
  if (index_id_ != other_meta.index_id_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "index_id_ not match", K(ret));
  } else if (row_count_ != other_meta.row_count_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "row_count_ not match", K(ret));
  } else if (occupy_size_ != other_meta.occupy_size_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "occupy_size_ not match", K(ret));
  } else if (data_checksum_ != other_meta.data_checksum_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "data checksum not match", K(ret));
  } else if (row_checksum_ != other_meta.row_checksum_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "row_checksum_ not match", K(ret));
  } else if (data_version_ != other_meta.data_version_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "data_version_ not match", K(ret));
  } else if (rowkey_column_count_ != other_meta.rowkey_column_count_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "rowkey_column_count_ not match", K(ret));
  } else if (table_type_ != other_meta.table_type_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "table_type_ not match", K(ret));
  } else if (index_type_ != other_meta.index_type_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "index_type_ not match", K(ret));
  } else if (macro_block_count_ != other_meta.macro_block_count_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "macro_block_count_ not match", K(ret));
  } else if (lob_macro_block_count_ != other_meta.lob_macro_block_count_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "lob_macro_block_count_ not match", K(ret));
  } else if (column_cnt_ != other_meta.column_cnt_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "column_cnt_ not match", K(ret));
  } else if (total_sstable_count_ != other_meta.total_sstable_count_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "total_sstable_count_ not match", K(ret));
  } else if (max_logic_block_index_ != other_meta.max_logic_block_index_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "max_logic_block_index_ not match", K(ret));
  } else if (contain_uncommitted_row_ != other_meta.contain_uncommitted_row_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "contain_uncommitted_row not match", K(ret));
  } else if (end_log_id_ != other_meta.end_log_id_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "end_log_id not match", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const ObSSTableColumnMeta& src_col = column_metas_.at(i);
      const ObSSTableColumnMeta& other_col = other_meta.column_metas_.at(i);
      if (src_col.column_id_ != other_col.column_id_ ||
          src_col.column_default_checksum_ != other_col.column_default_checksum_ ||
          src_col.column_checksum_ != other_col.column_checksum_) {
        ret = OB_INVALID_DATA;
        STORAGE_LOG(WARN, "column_metas_ not match", K(ret), K(i), "src_meta", *this, K(other_meta));
      }
    }
  }
  return ret;
}

void ObSSTableBaseMeta::set_allocator(common::ObIAllocator& allocator)
{
  column_metas_.set_allocator(&allocator);
  new_column_metas_.set_allocator(&allocator);
}

int ObSSTableBaseMeta::set_upper_trans_version(const int64_t upper_trans_version)
{
  int ret = OB_SUCCESS;
  const int64_t old_val = ATOMIC_LOAD(&upper_trans_version_);
  // only set once
  if (INT64_MAX == old_val && INT64_MAX != upper_trans_version) {
    const int64_t new_val = std::max(upper_trans_version, max_merged_trans_version_);
    ATOMIC_CAS(&upper_trans_version_, old_val, new_val);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSTableBaseMeta, index_id_, row_count_, occupy_size_, data_checksum_, row_checksum_,
    data_version_, rowkey_column_count_, table_type_, available_version_, macro_block_count_,
    use_old_macro_block_count_, column_cnt_, column_metas_, macro_block_second_index_, total_sstable_count_,
    index_type_, lob_macro_block_count_, lob_use_old_macro_block_count_,
    sstable_format_version_,  // added since 2.0
    max_logic_block_index_, build_on_snapshot_, create_index_base_version_, schema_version_,
    progressive_merge_start_version_, progressive_merge_end_version_, create_snapshot_version_, checksum_method_,
    new_column_metas_, progressive_merge_round_, progressive_merge_step_, table_mode_, logical_data_version_,
    has_compact_row_, pg_key_, multi_version_rowkey_type_, contain_uncommitted_row_, upper_trans_version_,
    max_merged_trans_version_, end_log_id_);

ObSSTableMeta::ObSSTableMeta(ObIAllocator& alloc)
    : ObSSTableBaseMeta(alloc),
      macro_block_array_(DEFAULT_MACRO_BLOCK_ARRAY_PAGE_SIZE),
      lob_macro_block_array_(DEFAULT_MACRO_BLOCK_ARRAY_PAGE_SIZE),
      macro_block_idx_array_(DEFAULT_MACRO_BLOCK_ARRAY_IDX_PAGE_SIZE),
      lob_macro_block_idx_array_(DEFAULT_MACRO_BLOCK_ARRAY_IDX_PAGE_SIZE),
      file_ctx_(alloc),
      lob_file_ctx_(alloc),
      bloom_filter_block_id_(),
      bloom_filter_block_id_in_files_(0),
      bloom_filter_file_ctx_(alloc)
{}

bool ObSSTableMeta::is_valid() const
{
  bool bool_ret = true;

  if (!ObSSTableBaseMeta::is_valid()) {
    bool_ret = false;
    LOG_WARN("sstable base meta not valid");
  } else if (sstable_format_version_ >= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_3) {
    if (!file_ctx_.is_valid() || !lob_file_ctx_.is_valid()) {
      bool_ret = false;
      LOG_WARN("file ctx or lob file ctx not valid", K_(sstable_format_version));
    } else if (file_ctx_.need_file_id_list() && macro_block_array_.count() != macro_block_idx_array_.count()) {
      bool_ret = false;
      LOG_WARN("macro block array and idx array not match",
          "macro_count",
          macro_block_array_.count(),
          "idx_count",
          macro_block_idx_array_.count());
    } else if (lob_file_ctx_.need_file_id_list() &&
               lob_macro_block_array_.count() != lob_macro_block_idx_array_.count()) {
      bool_ret = false;
      LOG_WARN("lob block array and idx array not match",
          "lob macro count",
          lob_macro_block_array_.count(),
          "lob idx count",
          lob_macro_block_idx_array_.count());
    }
  }
  return bool_ret;
}

bool ObSSTableMeta::operator==(const ObSSTableMeta& other) const
{
  bool bret = true;
  if (ObSSTableBaseMeta::operator!=(other)) {
    bret = false;
  } else if (!is_array_equal(macro_block_array_, other.macro_block_array_)) {
    bret = false;
  } else if (!is_array_equal(lob_macro_block_array_, other.lob_macro_block_array_)) {
    bret = false;
  } else if (!is_array_equal(macro_block_idx_array_, other.macro_block_idx_array_)) {
    bret = false;
  } else if (!is_array_equal(lob_macro_block_idx_array_, other.lob_macro_block_idx_array_)) {
    bret = false;
  } else if (!file_ctx_.equals(other.file_ctx_)) {
    bret = false;
  } else if (!lob_file_ctx_.equals(other.lob_file_ctx_)) {
    bret = false;
  } else if (bloom_filter_block_id_ != other.bloom_filter_block_id_) {
    bret = false;
  } else if (bloom_filter_block_id_in_files_ != other.bloom_filter_block_id_in_files_) {
    bret = false;
  } else if (!bloom_filter_file_ctx_.equals(other.bloom_filter_file_ctx_)) {
    bret = false;
  }
  return bret;
}

bool ObSSTableMeta::operator!=(const ObSSTableMeta& other) const
{
  return !(*this == other);
}

void ObSSTableMeta::reset()
{
  ObSSTableBaseMeta::reset();
  macro_block_array_.reset();
  lob_macro_block_array_.reset();
  macro_block_idx_array_.reset();
  lob_macro_block_idx_array_.reset();
  file_ctx_.reset();
  lob_file_ctx_.reset();
  bloom_filter_block_id_.reset();
  bloom_filter_block_id_in_files_ = 0;
  bloom_filter_file_ctx_.reset();
}

OB_SERIALIZE_MEMBER((ObSSTableMeta, ObSSTableBaseMeta), macro_block_array_, lob_macro_block_array_,
    macro_block_idx_array_, lob_macro_block_idx_array_, file_ctx_, lob_file_ctx_, bloom_filter_block_id_,
    bloom_filter_block_id_in_files_, bloom_filter_file_ctx_);

//=======================ObPartitionMeta=====================================
ObPartitionMeta::ObPartitionMeta()
    : table_id_(0),
      partition_id_(0),
      partition_cnt_(0),
      data_version_(0),
      table_type_(0),
      migrate_status_(0),
      replica_status_(0),
      migrate_timestamp_(0),
      step_merge_start_version_(0),
      step_merge_end_version_(0),
      index_table_count_(0),
      log_info_(),
      store_type_(MAJOR_SSSTORE),
      is_restore_(0),
      partition_checksum_(0)
{}

ObPartitionMeta::~ObPartitionMeta()
{}

bool ObPartitionMeta::is_valid() const
{
  return table_id_ >= 0 && partition_id_ >= 0 && data_version_ > 0 && table_type_ >= 0 && migrate_status_ >= 0 &&
         migrate_timestamp_ >= 0 && step_merge_start_version_ >= 0 && step_merge_end_version_ >= 0 &&
         index_table_count_ >= 0 && (is_ssstore(store_type_)) && partition_checksum_ >= 0;
}

void ObPartitionMeta::reset()
{
  table_id_ = 0;
  partition_id_ = 0;
  partition_cnt_ = 0;
  data_version_ = 0;
  table_type_ = 0;
  migrate_status_ = 0;
  replica_status_ = 0;
  migrate_timestamp_ = 0;
  step_merge_start_version_ = 0;
  step_merge_end_version_ = 0;
  index_table_count_ = 0;
  log_info_.reset();
  store_type_ = INVALID_STORE_TYPE;
  is_restore_ = 0;
  partition_checksum_ = 0;
}

ObPartitionMeta& ObPartitionMeta::operator=(const ObPartitionMeta& meta)
{
  if (this != &meta) {
    table_id_ = meta.table_id_;
    partition_id_ = meta.partition_id_;
    partition_cnt_ = meta.partition_cnt_;
    data_version_ = meta.data_version_;
    table_type_ = meta.table_type_;
    migrate_status_ = meta.migrate_status_;
    replica_status_ = meta.replica_status_;
    migrate_timestamp_ = meta.migrate_timestamp_;
    step_merge_start_version_ = meta.step_merge_start_version_;
    step_merge_end_version_ = meta.step_merge_end_version_;
    index_table_count_ = meta.index_table_count_;
    log_info_ = meta.log_info_;
    store_type_ = meta.store_type_;
    is_restore_ = meta.is_restore_;
    partition_checksum_ = meta.partition_checksum_;
  }
  return *this;
}

bool ObPartitionMeta::operator==(const ObPartitionMeta& meta) const
{
  return table_id_ == meta.table_id_ && partition_id_ == meta.partition_id_ && partition_cnt_ == meta.partition_cnt_ &&
         data_version_ == meta.data_version_ && table_type_ == meta.table_type_ &&
         migrate_status_ == meta.migrate_status_ && replica_status_ == meta.replica_status_ &&
         migrate_timestamp_ == meta.migrate_timestamp_ && step_merge_start_version_ == meta.step_merge_start_version_ &&
         step_merge_end_version_ == meta.step_merge_end_version_ && index_table_count_ == meta.index_table_count_ &&
         log_info_ == meta.log_info_ && store_type_ == meta.store_type_ && is_restore_ == meta.is_restore_ &&
         partition_checksum_ == meta.partition_checksum_;
}

bool ObPartitionMeta::operator!=(const ObPartitionMeta& meta) const
{
  return !(*this == meta);
}

int ObPartitionMeta::deserialize(const char* buf, const int64_t buf_len, int64_t& pos, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char* self_log_info = NULL;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize partition meta failed.", K(ret), K(buf_len), K(pos));
  } else if (0 == log_info_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition meta log info is NULL", K(log_info_.length()), K(ret));
  } else if (NULL == (self_log_info = static_cast<char*>(allocator.alloc(log_info_.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot allocate memory for log info.", K(log_info_.length()), K(ret));
  } else {
    MEMCPY(self_log_info, log_info_.ptr(), log_info_.length());
    log_info_.assign(self_log_info, log_info_.length());
  }

  if (OB_SUCCESS != ret) {
    if (NULL != log_info_.ptr()) {
      log_info_.assign(NULL, 0);
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPartitionMeta, table_id_, partition_id_, partition_cnt_, data_version_, table_type_,
    migrate_status_, replica_status_, migrate_timestamp_, step_merge_start_version_, step_merge_end_version_,
    index_table_count_, log_info_, store_type_, is_restore_, partition_checksum_);

int write_compact_rowkey(ObBufferWriter& buffer_writer, const common::ObObj* endkey, const int64_t count,
    const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  common::ObNewRow row;
  ObRowWriter writer;
  int64_t pos = buffer_writer.pos();
  row.cells_ = const_cast<ObObj*>(endkey);
  row.count_ = count;
  if (NULL == endkey || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(endkey), K(count), K(ret));
  } else if (OB_FAIL(writer.write(row, buffer_writer.data(), buffer_writer.capacity(), row_store_type, pos))) {
    LOG_WARN(" write_compact_rowkey failed.", K(ret), K(buffer_writer.capacity()), K(pos));
  } else if (OB_FAIL(buffer_writer.set_pos(pos))) {
    LOG_WARN("set pos on buffer_writer failed.", K(ret));
  }

  return ret;
}

int read_compact_rowkey(ObBufferReader& buffer_reader, const common::ObObjMeta* column_type_array,
    common::ObObj* endkey, const int64_t count, const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  ObSparseRowReader sparse_row_reader;
  ObFlatRowReader flat_row_reader;
  ObIRowReader* reader = nullptr;
  common::ObNewRow row;
  row.cells_ = endkey;
  row.count_ = count;
  int64_t pos = buffer_reader.pos();
  if (NULL == buffer_reader.data() || buffer_reader.length() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buffer_reader.data()), K(buffer_reader.length()), K(ret));
  } else if (NULL == column_type_array || NULL == endkey || count <= 0 || row_store_type >= MAX_ROW_STORE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(column_type_array), KP(endkey), K(count), K(ret));
  } else {
    if (SPARSE_ROW_STORE == row_store_type) {
      reader = &sparse_row_reader;
    } else {
      reader = &flat_row_reader;
    }
    if (OB_NOT_NULL(reader) &&
        OB_FAIL(reader->read_compact_rowkey(
            column_type_array, count, buffer_reader.data(), buffer_reader.capacity(), pos, row))) {
      LOG_WARN("read compact rowkey failed.", K(ret));
    } else if (OB_FAIL(buffer_reader.set_pos(pos))) {
      LOG_WARN("set pos on buffer reader failed.", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSTablePair, data_version_, data_seq_);

OB_SERIALIZE_MEMBER(ObLogicBlockIndex, logic_block_index_);
ObSimpleMacroBlockMetaInfo::ObSimpleMacroBlockMetaInfo()
{
  reuse();
}

void ObSimpleMacroBlockMetaInfo::reuse()
{
  block_index_ = 0;
  attr_ = 0;
  table_id_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
  write_seq_ = 0;
  create_timestamp_ = 0;
}

void ObSimpleMacroBlockMetaInfo::reset()
{
  reuse();
}

ObMacroBlockMarkerStatus::ObMacroBlockMarkerStatus()
{
  reset();
}

void ObMacroBlockMarkerStatus::reset()
{
  total_block_count_ = 0;
  reuse();
}

void ObMacroBlockMarkerStatus::fill_comment(char* buf, const int32_t buf_len) const
{
  const int64_t now = ObTimeUtility::current_time();
  if (NULL != buf && buf_len > 0) {
    if (hold_info_.create_timestamp_ <= 0) {
      buf[0] = '\0';
    } else {
      snprintf(buf,
          buf_len,
          "oldest hold macro block info: block_index=%ld, attr=%d,"
          " table_id=%lu, data_version=%lu, data_seq=%ld,"
          " write_seq=%d, create_ts=%ld,"
          " hold_time=%lds",
          hold_info_.block_index_,
          hold_info_.attr_,
          hold_info_.table_id_,
          hold_info_.data_version_,
          hold_info_.data_seq_,
          hold_info_.write_seq_,
          hold_info_.create_timestamp_,
          (now - hold_info_.create_timestamp_) / 1000 / 1000);  // ignore ret
    }
  }
}

void ObMacroBlockMarkerStatus::reuse()
{
  reserved_block_count_ = 0;
  macro_meta_block_count_ = 0;
  partition_meta_block_count_ = 0;
  data_block_count_ = 0;
  lob_data_block_count_ = 0;
  second_index_count_ = 0;
  lob_second_index_count_ = 0;
  bloomfilter_count_ = 0;
  hold_count_ = 0;
  pending_free_count_ = 0;
  free_count_ = 0;
  mark_cost_time_ = 0;
  sweep_cost_time_ = 0;
  hold_alert_time_ = HOLD_ALERT_TIME;
#ifdef ERRSIM
  hold_alert_time_ = GCONF.macro_block_hold_alert_time;
#endif
  hold_info_.reuse();
}

ObMacroBlockMarkerBitMap::ObMacroBlockMarkerBitMap()
    : is_inited_(false), block_bitmap_(NULL), byte_size_(0), total_macro_block_count_(0)

{}

ObMacroBlockMarkerBitMap::~ObMacroBlockMarkerBitMap()
{
  reset();
}

int ObMacroBlockMarkerBitMap::init(const int64_t macro_block_count)
{
  int ret = OB_SUCCESS;
  const int64_t byte_size = upper_align(macro_block_count, BYTES_PER_BLOCK) / 8;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (byte_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(byte_size));
  } else if (NULL == (block_bitmap_ = (char*)ob_malloc(byte_size, ObModIds::OB_SSTABLE_BLOCK_BIT_MAP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc block_bitmap_", K(ret), K(byte_size));
  } else {
    byte_size_ = byte_size;
    total_macro_block_count_ = macro_block_count;
    is_inited_ = true;
    reuse();
  }
  return ret;
}

void ObMacroBlockMarkerBitMap::reuse()
{
  if (is_inited_) {
    MEMSET(block_bitmap_, 0, byte_size_);
  }
}

void ObMacroBlockMarkerBitMap::reset()
{
  is_inited_ = false;
  if (NULL != block_bitmap_) {
    ob_free(block_bitmap_);
  }
  block_bitmap_ = NULL;
  byte_size_ = 0;
  total_macro_block_count_ = 0;
}

int ObMacroBlockMarkerBitMap::set_block(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroBlockMarker has not been inited, ", K(ret));
  } else if (block_index < 0 && block_index >= total_macro_block_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block index.", K(block_index), K(total_macro_block_count_), K(ret));
  } else {
    size_type* bit_ptr = (size_type*)(block_bitmap_);
    bit_ptr[mem_index(block_index)] |= bit_mask(block_index);
  }
  return ret;
}

int ObMacroBlockMarkerBitMap::test_block(const int64_t block_index, bool& bool_ret) const
{
  int ret = OB_SUCCESS;
  bool_ret = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroBlockMarker has not been inited, ", K(ret));
  } else if (block_index < 0 || block_index >= total_macro_block_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block index.", K(block_index), K(total_macro_block_count_), K(ret));
  } else {
    size_type* bit_ptr = (size_type*)(block_bitmap_);
    bool_ret = (bit_ptr[mem_index(block_index)] & bit_mask(block_index)) != 0;
  }

  return ret;
}

ObMacroBlockMarkerHelper::ObMacroBlockMarkerHelper() : is_inited_(false), status_(), bit_map_()
{}

ObMacroBlockMarkerHelper::~ObMacroBlockMarkerHelper()
{
  is_inited_ = false;
}

int ObMacroBlockMarkerHelper::init(const int64_t macro_block_count)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(bit_map_.init(macro_block_count))) {
    LOG_WARN("failed to init bit map", K(ret));
  } else {
    status_.total_block_count_ = macro_block_count;
    is_inited_ = true;
  }

  return ret;
}

void ObMacroBlockMarkerHelper::reuse()
{
  if (is_inited_) {
    status_.reuse();
    bit_map_.reuse();
  }
}

void ObMacroBlockMarkerHelper::reset()
{
  if (is_inited_) {
    status_.reset();
    bit_map_.reset();
  }
  is_inited_ = false;
}

int ObMacroBlockMarkerHelper::set_block(const ObMacroBlockCommonHeader::MacroBlockType& type, const int64_t block_index)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(bit_map_.test_block(block_index, is_exist))) {
    LOG_WARN("failed to test block", K(ret), K(block_index));
  } else if (OB_FAIL(bit_map_.set_block(block_index))) {
    LOG_WARN("failed to set block", K(ret), K(block_index));
  } else if (!is_exist) {

    switch (type) {
      case ObMacroBlockCommonHeader::SSTableData:
        status_.data_block_count_++;
        break;
      case ObMacroBlockCommonHeader::PartitionMeta:
        status_.partition_meta_block_count_++;
        break;
      case ObMacroBlockCommonHeader::MacroMeta:
        status_.macro_meta_block_count_++;
        break;
      case ObMacroBlockCommonHeader::Reserved:
        status_.reserved_block_count_++;
        break;
      case ObMacroBlockCommonHeader::MacroBlockSecondIndex:
        status_.second_index_count_++;
        break;
      case ObMacroBlockCommonHeader::LobData:
      case ObMacroBlockCommonHeader::LobIndex:
        status_.lob_data_block_count_++;
        break;
      default:
        LOG_ERROR("unknown macro block type", K(block_index), K(type));
        break;
    }
  }
  return ret;
}

int ObMacroBlockMarkerHelper::set_block(
    const ObMacroBlockCommonHeader::MacroBlockType& type, const common::ObIArray<int64_t>& block_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < block_array.count(); ++i) {
      if (OB_FAIL(set_block(type, block_array.at(i)))) {
        LOG_WARN("failed to set block", K(ret), K(type), K(i), K(block_array.at(i)));
      }
    }
  }
  return ret;
}

int ObMacroBlockMarkerHelper::set_block(
    const ObMacroBlockCommonHeader::MacroBlockType& type, const common::ObIArray<MacroBlockId>& block_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < block_array.count(); ++i) {
      if (OB_FAIL(set_block(type, block_array.at(i).block_index()))) {
        LOG_WARN("failed to set block", K(ret), K(type), K(i), K(block_array.at(i)));
      }
    }
  }
  return ret;
}

int ObMacroBlockMarkerHelper::test_block(const int64_t block_index, bool& bool_ret) const
{
  int ret = OB_SUCCESS;
  bool_ret = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(bit_map_.test_block(block_index, bool_ret))) {
    LOG_WARN("failed to test block", K(ret), K(block_index), K(bool_ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObStoreFileInfo, file_id_, file_size_);

bool ObStoreFileInfo::operator==(const ObStoreFileInfo& other) const
{
  return file_id_ == other.file_id_ && file_size_ == other.file_size_;
}

OB_SERIALIZE_MEMBER(ObStoreFileCtx, file_system_type_, file_type_, block_count_per_file_, file_list_);

ObStoreFileCtx::ObStoreFileCtx(common::ObIAllocator& allocator)
    : file_system_type_(STORE_FILE_SYSTEM_MAX),
      file_type_(STORE_FILE_TYPE_MAX),
      block_count_per_file_(0),
      file_list_(&allocator)
{}

ObStoreFileCtx::~ObStoreFileCtx()
{}

int ObStoreFileCtx::init(
    const ObStoreFileSystemType& file_system_type, const ObStoreFileType& file_type, const int64_t block_count_per_file)
{
  int ret = OB_SUCCESS;

  if (STORE_FILE_SYSTEM_MAX != file_system_type_ || file_list_.count() > 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (file_system_type < 0 || file_system_type >= STORE_FILE_SYSTEM_MAX || file_type < 0 ||
             file_type >= STORE_FILE_TYPE_MAX || block_count_per_file <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(file_system_type), K(file_type), K(block_count_per_file));
  } else {
    file_system_type_ = file_system_type;
    file_type_ = file_type;
    block_count_per_file_ = block_count_per_file;
  }
  return ret;
}

bool ObStoreFileCtx::is_valid() const
{
  return true;
}

void ObStoreFileCtx::reset()
{
  file_system_type_ = STORE_FILE_SYSTEM_MAX;
  block_count_per_file_ = 0;
  file_list_.reset();
}

int ObStoreFileCtx::assign(const ObStoreFileCtx& src)
{
  int ret = OB_SUCCESS;

  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(src));
  } else {
    file_system_type_ = src.file_system_type_;
    file_type_ = src.file_type_;
    block_count_per_file_ = src.block_count_per_file_;
    if (OB_FAIL(file_list_.assign(src.file_list_))) {
      LOG_WARN("failed to assign file list", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

bool ObStoreFileCtx::equals(const ObStoreFileCtx& other) const
{
  bool is_same = true;

  if (file_system_type_ != other.file_system_type_ || file_type_ != other.file_type_ ||
      block_count_per_file_ != other.block_count_per_file_) {
    is_same = false;
  } else if (!is_array_equal(file_list_, other.file_list_)) {
    is_same = false;
  }
  return is_same;
}

ObRecordHeaderV3::ObRecordHeaderV3()
    : magic_(0),
      header_length_(0),
      version_(0),
      header_checksum_(0),
      reserved16_(0),
      data_length_(0),
      data_zlength_(0),
      data_checksum_(0),
      data_encoding_length_(0),
      row_count_(0),
      column_cnt_(0),
      column_checksums_()
{}

void ObRecordHeaderV3::set_header_checksum()
{
  int16_t checksum = 0;
  header_checksum_ = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    format_i64(data_encoding_length_, checksum);
    format_i64(row_count_, checksum);
    format_i64(column_cnt_, checksum);
    for (int64_t i = 0; i < column_cnt_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }
  header_checksum_ = checksum;
}

int ObRecordHeaderV3::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    format_i64(data_encoding_length_, checksum);
    format_i64(row_count_, checksum);
    format_i64(column_cnt_, checksum);
    for (int64_t i = 0; i < column_cnt_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }

  if (0 != checksum) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_ERROR("record check checksum failed", K(ret), K(*this));
  }
  return ret;
}

int ObRecordHeaderV3::check_payload_checksum(const char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len < 0 || data_zlength_ != len ||
      (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len), K(data_zlength_), K(data_length_), K(data_checksum_));
  } else {
    const int64_t data_checksum = ob_crc64_sse42(buf, len);
    if (data_checksum != data_checksum_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      LOG_ERROR("checksum error", K(ret), K(data_checksum_), K(data_checksum));
    }
  }
  return ret;
}

int ObRecordHeaderV3::deserialize_and_check_record(
    const char* ptr, const int64_t size, const int16_t magic, const char*& payload_ptr, int64_t& payload_size)
{
  int ret = OB_SUCCESS;
  ObRecordHeaderV3 header;
  int64_t pos = 0;
  if (NULL == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(header.deserialize(ptr, size, pos))) {
    LOG_WARN("fail to deserialize header", K(ret));
  } else if (OB_FAIL(header.check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check and get record", K(ret));
  }

  return ret;
}

int ObRecordHeaderV3::check_and_get_record(
    const char* ptr, const int64_t size, const int16_t magic, const char*& payload_ptr, int64_t& payload_size) const
{
  int ret = OB_SUCCESS;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr));
  } else if (magic != magic_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("record header magic is not match", K(ret), K(magic), K(magic_));
  } else if (OB_FAIL(check_header_checksum())) {
    LOG_WARN("fail to check header checksum", K(ret));
  } else {
    const int64_t header_size = get_serialize_size();
    if (size < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer not enough", K(ret), K(size), K(header_size));
    } else {
      payload_ptr = ptr + header_size;
      payload_size = size - header_size;
      if (OB_FAIL(check_payload_checksum(payload_ptr, payload_size))) {
        LOG_WARN("fail to check payload checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObRecordHeaderV3::check_record(const char* ptr, const int64_t size, const int16_t magic) const
{
  int ret = OB_SUCCESS;
  const char* payload_ptr = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check record", K(ret), KP(ptr), K(size), K(magic));
  }
  return ret;
}

int ObRecordHeaderV3::deserialize_and_check_record(const char* ptr, const int64_t size, const int16_t magic)
{
  int ret = OB_SUCCESS;
  const char* payload_buf = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(magic));
  } else if (OB_FAIL(deserialize_and_check_record(ptr, size, magic, payload_buf, payload_size))) {
    LOG_WARN("fail to check record", K(ret));
  }
  return ret;
}

int64_t ObRecordHeaderV3::get_serialize_size() const
{
  int64_t size = 0;
  size += sizeof(ObRecordCommonHeader);
  if (RECORD_HEADER_VERSION_V3 == version_) {
    size += sizeof(data_encoding_length_);
    size += sizeof(row_count_);
    size += sizeof(column_cnt_);
    size += column_cnt_ * sizeof(int64_t);
  }
  return size;
}

int ObRecordHeaderV3::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    const int64_t serialize_size = get_serialize_size();
    int64_t pos_orig = pos;
    buf += pos_orig;
    pos = 0;
    if (serialize_size + pos_orig > buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer not enough", K(ret), K(serialize_size), K(buf_len), K(pos_orig));
    } else {
      ObRecordCommonHeader* common_header = reinterpret_cast<ObRecordCommonHeader*>(buf + pos);
      common_header->magic_ = magic_;
      common_header->header_length_ = header_length_;
      common_header->version_ = version_;
      common_header->header_checksum_ = header_checksum_;
      common_header->reserved16_ = reserved16_;
      common_header->data_length_ = data_length_;
      common_header->data_zlength_ = data_zlength_;
      common_header->data_checksum_ = data_checksum_;
      pos += sizeof(ObRecordCommonHeader);
      if (RECORD_HEADER_VERSION_V3 == version_) {
        MEMCPY(buf + pos, &data_encoding_length_, sizeof(data_encoding_length_));
        pos += sizeof(data_encoding_length_);
        MEMCPY(buf + pos, &row_count_, sizeof(row_count_));
        pos += sizeof(row_count_);
        MEMCPY(buf + pos, &column_cnt_, sizeof(column_cnt_));
        pos += sizeof(column_cnt_);
        MEMCPY(buf + pos, column_checksums_, column_cnt_ * sizeof(int64_t));
        pos += column_cnt_ * sizeof(int64_t);
      }
    }
    pos += pos_orig;
  }
  return ret;
}

int ObRecordHeaderV3::deserialize(const char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < 8) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos_orig = pos;
    const ObRecordCommonHeader* common_header = reinterpret_cast<const ObRecordCommonHeader*>(buf);
    pos = 0;
    buf += pos_orig;
    magic_ = common_header->magic_;
    header_length_ = common_header->header_length_;
    version_ = common_header->version_;
    header_checksum_ = common_header->header_checksum_;
    reserved16_ = common_header->reserved16_;
    data_length_ = common_header->data_length_;
    data_zlength_ = common_header->data_zlength_;
    data_checksum_ = common_header->data_checksum_;
    pos += sizeof(ObRecordCommonHeader);
    if (RECORD_HEADER_VERSION_V3 == version_) {
      const int64_t* column_checksums = nullptr;
      MEMCPY(&data_encoding_length_, buf + pos, sizeof(data_encoding_length_));
      pos += sizeof(data_encoding_length_);
      MEMCPY(&row_count_, buf + pos, sizeof(row_count_));
      pos += sizeof(row_count_);
      MEMCPY(&column_cnt_, buf + pos, sizeof(column_cnt_));
      pos += sizeof(column_cnt_);
      column_checksums = reinterpret_cast<const int64_t*>(buf + pos);
      column_checksums_ = const_cast<int64_t*>(column_checksums);
      pos += column_cnt_ * sizeof(int64_t);
    }
    pos += pos_orig;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSuperBlockMetaEntry, macro_block_id_);

ObServerSuperBlock::ServerSuperBlockContent::ServerSuperBlockContent()
{
  reset();
}

bool ObServerSuperBlock::ServerSuperBlockContent::is_valid() const
{
  return create_timestamp_ > 0 && modify_timestamp_ >= create_timestamp_ && macro_block_size_ > 0 &&
         total_macro_block_count_ > 0 && total_file_size_ >= macro_block_size_ && replay_start_point_.is_valid() &&
         super_block_meta_.is_valid() && tenant_config_meta_.is_valid();
}

void ObServerSuperBlock::ServerSuperBlockContent::reset()
{
  create_timestamp_ = 0;
  modify_timestamp_ = 0;
  macro_block_size_ = 0;
  total_macro_block_count_ = 0;
  total_file_size_ = 0;
  replay_start_point_.reset();
  super_block_meta_.reset();
  tenant_config_meta_.reset();
}

OB_SERIALIZE_MEMBER(ObServerSuperBlock::ServerSuperBlockContent, create_timestamp_, modify_timestamp_,
    macro_block_size_, total_macro_block_count_, total_file_size_, replay_start_point_, super_block_meta_,
    tenant_config_meta_);

ObServerSuperBlock::ObServerSuperBlock() : header_(), content_()
{}

bool ObServerSuperBlock::is_valid() const
{
  return header_.is_valid() && content_.is_valid() && SERVER_SUPER_BLOCK_MAGIC == header_.magic_;
}

int64_t ObServerSuperBlock::get_serialize_size() const
{
  return header_.get_serialize_size() + content_.get_serialize_size();
}

int ObServerSuperBlock::serialize(char* buf, const int64_t buf_size, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (!is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("cannot write invalid super block", K(ret), K(*this));
  } else {
    MEMSET(buf + pos, 0, buf_size - pos);
    if (OB_FAIL(header_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block header", K(ret), K(buf_size), K(new_pos), K(*this));
    } else if (OB_FAIL(content_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block content", K(ret), K(buf_size), K(new_pos), K(*this));
    } else {
      pos = new_pos;
      LOG_INFO("succeed to serialize super block buf", K(buf_size), K(pos), K(*this));
    }
  }
  return ret;
}

int ObServerSuperBlock::deserialize(const char* buf, const int64_t buf_size, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot read super block twice", K(ret), K(*this));
  } else if (OB_FAIL(header_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_FAIL(content_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode content", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data, ", K(ret), K(*this));
  } else {
    LOG_INFO("load superblock ok.", K(buf_size), K(pos), K(*this));
  }
  return ret;
}

void ObServerSuperBlock::reset()
{
  header_.reset();
  content_.reset();
}

int ObServerSuperBlock::format_startup_super_block(const int64_t macro_block_size, const int64_t data_file_size)
{
  int ret = OB_SUCCESS;

  if (macro_block_size <= 0 || data_file_size <= 0 || data_file_size < macro_block_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block_size), K(data_file_size));
  } else {
    reset();
    header_.version_ = OB_SUPER_BLOCK_V3;
    header_.magic_ = SERVER_SUPER_BLOCK_MAGIC;

    content_.create_timestamp_ = ObTimeUtility::current_time();
    content_.modify_timestamp_ = content_.create_timestamp_;
    content_.macro_block_size_ = macro_block_size;
    content_.total_macro_block_count_ = data_file_size / macro_block_size;
    content_.total_file_size_ = lower_align(data_file_size, macro_block_size);

    content_.replay_start_point_.file_id_ = 1;
    content_.replay_start_point_.log_id_ = 0;
    content_.replay_start_point_.offset_ = 0;
    LOG_INFO("success to format super block", K(*this));
  }

  return ret;
}
ObMacroBlockMetaV2::ObMacroBlockMetaV2()
    : attr_(0),
      data_version_(0),
      column_number_(0),
      rowkey_column_number_(0),
      column_index_scale_(0),
      row_store_type_(0),
      row_count_(0),
      occupy_size_(0),
      data_checksum_(0),
      micro_block_count_(0),
      micro_block_data_offset_(0),
      micro_block_index_offset_(0),
      micro_block_endkey_offset_(0),
      column_checksum_(NULL),
      endkey_(NULL),
      table_id_(0),
      data_seq_(-1),
      schema_version_(0),
      snapshot_version_(0),
      schema_rowkey_col_cnt_(0),
      row_count_delta_(0),
      micro_block_mark_deletion_offset_(0),
      macro_block_deletion_flag_(false),
      micro_block_delta_offset_(0),
      partition_id_(-1),
      column_checksum_method_(CCM_UNKOWN),
      progressive_merge_round_(0),
      write_seq_(0),
      bf_flag_(0),
      create_timestamp_(0),
      retire_timestamp_(0),
      collation_free_endkey_(NULL),
      encrypt_id_(0),
      master_key_id_(0),
      contain_uncommitted_row_(false),
      max_merged_trans_version_(0)
{
  encrypt_key_[0] = '\0';
}

bool ObMacroBlockMetaV2::operator==(const ObMacroBlockMetaV2& other) const
{
  bool bret = false;
  bret = attr_ == other.attr_ && data_version_ == other.data_version_ && column_number_ == other.column_number_ &&
         rowkey_column_number_ == other.rowkey_column_number_ && column_index_scale_ == other.column_index_scale_ &&
         row_store_type_ == other.row_store_type_ && row_count_ == other.row_count_ &&
         occupy_size_ == other.occupy_size_ && data_checksum_ == other.data_checksum_ &&
         micro_block_count_ == other.micro_block_count_ && micro_block_data_offset_ == other.micro_block_data_offset_ &&
         micro_block_index_offset_ == other.micro_block_index_offset_ &&
         micro_block_endkey_offset_ == other.micro_block_endkey_offset_ && table_id_ == other.table_id_ &&
         data_seq_ == other.data_seq_ && schema_version_ == other.schema_version_ &&
         snapshot_version_ == other.snapshot_version_ && schema_rowkey_col_cnt_ == other.schema_rowkey_col_cnt_ &&
         row_count_delta_ == other.row_count_delta_ &&
         micro_block_mark_deletion_offset_ == other.micro_block_mark_deletion_offset_ &&
         macro_block_deletion_flag_ == other.macro_block_deletion_flag_ &&
         micro_block_delta_offset_ == other.micro_block_delta_offset_ && partition_id_ == other.partition_id_ &&
         column_checksum_method_ == other.column_checksum_method_ &&
         progressive_merge_round_ == other.progressive_merge_round_ && encrypt_id_ == other.encrypt_id_ &&
         master_key_id_ == other.master_key_id_ && max_merged_trans_version_ == other.max_merged_trans_version_ &&
         contain_uncommitted_row_ == other.contain_uncommitted_row_;

  if (NULL == column_checksum_ && NULL == other.column_checksum_) {
    ;
  } else if (NULL != column_checksum_ && NULL != other.column_checksum_) {
    for (int64_t i = 0; bret && i < column_number_; ++i) {
      bret = column_checksum_[i] == other.column_checksum_[i];
    }
  } else {
    bret = false;
  }

  if (NULL == endkey_ && NULL == other.endkey_) {
    ;
  } else if (NULL != endkey_ && NULL != other.endkey_) {
    for (int64_t i = 0; bret && i < rowkey_column_number_; ++i) {
      bret = endkey_[i] == other.endkey_[i];
    }
  } else {
    bret = false;
  }

  bret = bret && (0 == STRNCMP(encrypt_key_, other.encrypt_key_, sizeof(encrypt_key_)));
  return bret;
}

bool ObMacroBlockMetaV2::operator!=(const ObMacroBlockMetaV2& other) const
{
  return !(*this == other);
}

int ObMacroBlockMetaV2::serialize(char* buf, int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = MACRO_BLOCK_META_VERSION_V2;
  ObBufferWriter buffer_writer(buf, data_len, pos);

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta is not valid data.", K(*this), K(ret));
  } else if (OB_FAIL(buffer_writer.write(header_size))) {
    LOG_WARN("serialization header_size error.",
        K(header_size),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(header_version))) {
    LOG_WARN("serialization header_version error.",
        K(header_version),
        K(ret),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()));
  } else if (OB_FAIL(buffer_writer.write(attr_))) {
    LOG_WARN("serialization attr_ error.", K_(attr), K(buffer_writer.capacity()), K(buffer_writer.pos()), K(ret));
  } else if (OB_FAIL(buffer_writer.write(data_version_))) {
    LOG_WARN("serialization data_version_ error.",
        K_(data_version),
        K(buffer_writer.capacity()),
        K(buffer_writer.pos()),
        K(ret));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_writer.write(column_number_))) {
      LOG_WARN("serialization column_number_ error.",
          K_(column_number),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(rowkey_column_number_))) {
      LOG_WARN("serialization rowkey_column_number_ error.",
          K_(rowkey_column_number),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(column_index_scale_))) {
      LOG_WARN("serialization column_index_scale_ error.",
          K_(column_index_scale),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(row_store_type_))) {
      LOG_WARN("serialization flag_ error.",
          K_(row_store_type),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(row_count_))) {
      LOG_WARN("serialization row_count_ error.",
          K_(row_count),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(occupy_size_))) {
      LOG_WARN("serialization occupy_size_ error.",
          K_(occupy_size),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(data_checksum_))) {
      LOG_WARN("serialization data_checksum_ error.",
          K_(data_checksum),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_count_))) {
      LOG_WARN("serialization micro_block_count_ error.",
          K_(micro_block_count),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_data_offset_))) {
      LOG_WARN("serialization micro_block_data_offset_ error.",
          K_(micro_block_data_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_index_offset_))) {
      LOG_WARN("serialization micro_block_index_offset_ error.",
          K_(micro_block_index_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_writer.write(micro_block_endkey_offset_))) {
      LOG_WARN("serialization micro_block_endkey_offset_ error.",
          K_(micro_block_endkey_offset),
          K(buffer_writer.capacity()),
          K(buffer_writer.pos()),
          K(ret));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {

      for (int32_t i = 0; i < column_number_ && NULL != column_checksum_ && OB_SUCCESS == ret; ++i) {
        if (OB_FAIL(buffer_writer.write(column_checksum_[i]))) {
          LOG_WARN("serialization column_checksum_ error.",
              K(column_checksum_[i]),
              K(buffer_writer.capacity()),
              K(buffer_writer.pos()),
              K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      if (OB_FAIL(buffer_writer.write(rowkey))) {
        LOG_WARN(" write_compact_rowkey failed.",
            KP_(endkey),
            K_(rowkey_column_number),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K_(attr),
            K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_writer.write(table_id_))) {
        LOG_WARN("serialization table_id_ error.",
            K_(table_id),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(data_seq_))) {
        LOG_WARN("serialization data_seq_ error.",
            K_(data_seq),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(schema_version_))) {
        LOG_WARN("serialization schema_version_ error.",
            K_(schema_version),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(snapshot_version_))) {
        LOG_WARN("serialization snapshot_version_ error.",
            K_(snapshot_version),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(schema_rowkey_col_cnt_))) {
        LOG_WARN("serialization schema_rowkey_col_cnt_ error.",
            K_(schema_rowkey_col_cnt),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_writer.write(row_count_delta_))) {
        LOG_WARN("serialization row_count_delta error",
            K_(row_count_delta),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(micro_block_mark_deletion_offset_))) {
        LOG_WARN("serialization micro_block_mark_deletion_offset_ error.",
            K_(micro_block_mark_deletion_offset),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(macro_block_deletion_flag_))) {
        LOG_WARN("serialization macro_block_deletion_flag_ error.",
            K_(macro_block_deletion_flag),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(micro_block_delta_offset_))) {
        LOG_WARN("serialize micro_block_delta_offset error",
            K_(micro_block_delta_offset),
            K(buffer_writer.capacity()),
            K(buffer_writer.pos()),
            K(ret));
      } else if (OB_FAIL(buffer_writer.write(partition_id_))) {
        LOG_WARN("serialize partition id error", K(ret), K(partition_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(column_checksum_method_))) {
        LOG_WARN("serialize partition id error", K(ret), K(column_checksum_method_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(progressive_merge_round_))) {
        LOG_WARN("fail to write progressive merge round", K(ret));
      } else if (OB_FAIL(buffer_writer.write(encrypt_id_))) {
        LOG_WARN("serialize encrypt id error", K(ret), K(encrypt_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(master_key_id_))) {
        LOG_WARN("serialize encrypt id error", K(ret), K(master_key_id_), K(buffer_writer));
      } else if (OB_FAIL(buffer_writer.write(encrypt_key_, sizeof(encrypt_key_)))) {
        LOG_WARN("serialization encrypt_key error.", K(encrypt_key_), K(ret));
      } else if (OB_FAIL(buffer_writer.write(contain_uncommitted_row_))) {
        LOG_WARN("failed to serialize contain_uncommitted_row", K(ret), K_(contain_uncommitted_row));
      } else if (OB_FAIL(buffer_writer.write(max_merged_trans_version_))) {
        LOG_WARN("failed to serialize max_merged_trans_version", K(ret), K_(max_merged_trans_version));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int32_t size = static_cast<int32_t>(buffer_writer.pos() - start_pos);
    char* size_ptr = buffer_writer.data() + start_pos;
    *reinterpret_cast<int32_t*>(size_ptr) = size;
    pos = buffer_writer.pos();
  }

  return ret;
}

int ObMacroBlockMetaV2::deserialize(const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, data_len, pos);

  if (NULL == endkey_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The endkey is NULL, can not deserialize, ", K(ret));
  } else if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.",
        K(header_size),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.",
        K(header_version),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (OB_FAIL(buffer_reader.read(attr_))) {
    LOG_WARN(
        "deserialization attr_ error.", K_(attr), K(ret), K(buffer_reader.capacity()), K(buffer_reader.pos()), K(ret));
  } else if (OB_FAIL(buffer_reader.read(data_version_))) {
    LOG_WARN("deserialization data_version_ error.",
        K_(data_version),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (is_data_block()) {
    if (OB_FAIL(buffer_reader.read(column_number_))) {
      LOG_WARN("deserialization column_number_ error.",
          K_(column_number),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(rowkey_column_number_))) {
      LOG_WARN("deserialization rowkey_column_number_ error.",
          K_(rowkey_column_number),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(column_index_scale_))) {
      LOG_WARN("deserialization column_index_scale_ error.",
          K_(column_index_scale),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_store_type_))) {
      LOG_WARN("deserialization flag_ error.",
          K_(row_store_type),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(row_count_))) {
      LOG_WARN("deserialization row_count_ error.",
          K_(row_count),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(occupy_size_))) {
      LOG_WARN("deserialization occupy_size_ error.",
          K_(occupy_size),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(data_checksum_))) {
      LOG_WARN("deserialization data_checksum_ error.",
          K_(data_checksum),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_count_))) {
      LOG_WARN("deserialization micro_block_count_ error.",
          K_(micro_block_count),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_data_offset_))) {
      LOG_WARN("deserialization micro_block_data_offset_ error.",
          K_(micro_block_data_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_index_offset_))) {
      LOG_WARN("deserialization micro_block_index_offset_ error.",
          K_(micro_block_index_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    } else if (OB_FAIL(buffer_reader.read(micro_block_endkey_offset_))) {
      LOG_WARN("deserialization micro_block_endkey_offset_ error.",
          K_(micro_block_endkey_offset),
          K(buffer_reader.capacity()),
          K(buffer_reader.pos()),
          K(ret));
    }

    if (OB_SUCCESS == ret && column_number_ > 0) {
      int64_t ekp = column_number_ * sizeof(int64_t);  // endkey start position
      char* column_array_start = buffer_reader.current();

      if (OB_ISNULL(column_array_start)) {
        ret = OB_ERR_SYS;
        LOG_WARN("column array is NULL.", K(ret));
      } else if (OB_FAIL(buffer_reader.advance(ekp))) {
        LOG_WARN("remain buffer length not enough for column array.", K(ekp), K(buffer_reader.remain()), K(ret));
      } else {
        column_checksum_ = reinterpret_cast<int64_t*>(column_array_start);
      }
    }

    if (OB_SUCC(ret) && is_normal_data_block()) {
      // deserialize rowkey;
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      if (OB_FAIL(buffer_reader.read(rowkey))) {
        LOG_WARN("fail to read rowkey", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(table_id_))) {
          LOG_WARN("deserialization table_id_ error.",
              K_(table_id),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(data_seq_))) {
          LOG_WARN("deserialization data_seq_ error.",
              K_(data_seq),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as -1;
        data_seq_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_version_))) {
          LOG_WARN("deserialization schema_version_ error.",
              K_(schema_version),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        schema_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(snapshot_version_))) {
          LOG_WARN("deserialization snapshot_version_ error.",
              K_(snapshot_version),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as -1;
        snapshot_version_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(schema_rowkey_col_cnt_))) {
          LOG_WARN("deserialization schema_rowkey_col_cnt error.",
              K_(schema_rowkey_col_cnt),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        schema_rowkey_col_cnt_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(row_count_delta_))) {
          LOG_WARN("deserialization row_count_delta error.",
              K_(row_count_delta),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        row_count_delta_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_mark_deletion_offset_))) {
          LOG_WARN("deserialization micro_block_mark_deletion_offset_ error.",
              K_(micro_block_mark_deletion_offset),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as 0;
        micro_block_mark_deletion_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(macro_block_deletion_flag_))) {
          LOG_WARN("deserialization macro_block_deletion_flag_ error.",
              K_(macro_block_deletion_flag),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        // set default value as false;
        macro_block_deletion_flag_ = false;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(micro_block_delta_offset_))) {
          LOG_WARN("deserialization micro_block_delta_offset error.",
              K_(micro_block_delta_offset),
              K(buffer_reader.capacity()),
              K(buffer_reader.pos()),
              K(ret));
        }
      } else {
        micro_block_delta_offset_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(partition_id_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        partition_id_ = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(column_checksum_method_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        column_checksum_method_ = CCM_UNKOWN;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(progressive_merge_round_))) {
          LOG_WARN("deserialization partition_id error", K(ret), K(buffer_reader));
        }
      } else {
        progressive_merge_round_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(master_key_id_))) {
          LOG_WARN("deserialization encrypt_id error", K(ret), K(buffer_reader));
        }
      } else {
        master_key_id_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(encrypt_key_, sizeof(encrypt_key_)))) {
          LOG_WARN("deserialization encrypt_key_ error", K(ret), K(buffer_reader));
        }
      } else {
        encrypt_key_[0] = '\0';
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(contain_uncommitted_row_))) {
          LOG_WARN("failed to deserialize contain_uncommitted_row", K(ret), K(buffer_reader));
        }
      } else {
        contain_uncommitted_row_ = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos < header_size) {
        if (OB_FAIL(buffer_reader.read(max_merged_trans_version_))) {
          LOG_WARN("failed to deserialize max_merged_trans_version", K(ret), K(buffer_reader));
        }
      } else {
        max_merged_trans_version_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (buffer_reader.pos() - start_pos > header_size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN(
            "elapsed buffer size larger than object.", K(header_size), K(buffer_reader.pos()), K(start_pos), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_reader.set_pos(start_pos + header_size))) {
      LOG_WARN("set pos on buffer reader failed.", K(ret));
    } else {
      pos = buffer_reader.pos();
    }
  }
  return ret;
}

int64_t ObMacroBlockMetaV2::get_serialize_size() const
{
  int64_t serialize_size = 2 * sizeof(int32_t);  // header_size and header_version
  serialize_size += sizeof(attr_);
  serialize_size += sizeof(data_version_);
  if (is_data_block()) {
    serialize_size += sizeof(column_number_);
    serialize_size += sizeof(rowkey_column_number_);
    serialize_size += sizeof(column_index_scale_);
    serialize_size += sizeof(row_store_type_);
    serialize_size += sizeof(row_count_);
    serialize_size += sizeof(occupy_size_);
    serialize_size += sizeof(data_checksum_);
    serialize_size += sizeof(micro_block_count_);
    serialize_size += sizeof(micro_block_data_offset_);
    serialize_size += sizeof(micro_block_index_offset_);
    serialize_size += sizeof(micro_block_endkey_offset_);
  }
  serialize_size += column_number_ * sizeof(int64_t);
  if (is_normal_data_block()) {
    ObRowkey rowkey(endkey_, rowkey_column_number_);
    serialize_size += rowkey.get_serialize_size();
  }
  serialize_size += sizeof(table_id_);
  serialize_size += sizeof(data_seq_);
  serialize_size += sizeof(schema_version_);
  serialize_size += sizeof(snapshot_version_);
  serialize_size += sizeof(schema_rowkey_col_cnt_);
  serialize_size += sizeof(row_count_delta_);
  serialize_size += sizeof(micro_block_mark_deletion_offset_);
  serialize_size += sizeof(macro_block_deletion_flag_);
  serialize_size += sizeof(micro_block_delta_offset_);
  serialize_size += sizeof(partition_id_);
  serialize_size += sizeof(column_checksum_method_);
  serialize_size += sizeof(progressive_merge_round_);
  serialize_size += sizeof(encrypt_id_);
  serialize_size += sizeof(master_key_id_);
  serialize_size += sizeof(encrypt_key_);
  serialize_size += sizeof(contain_uncommitted_row_);
  serialize_size += sizeof(max_merged_trans_version_);
  return serialize_size;
}

bool ObMacroBlockMetaV2::is_valid() const
{
  bool ret = true;
  if (!(attr_ >= ObMacroBlockCommonHeader::Free && attr_ < ObMacroBlockCommonHeader::MaxMacroType)) {
    ret = false;
  } else if (is_normal_data_block()) {
    if (rowkey_column_number_ <= 0 || column_number_ < rowkey_column_number_ || schema_rowkey_col_cnt_ < 0) {
      ret = false;
    } else if (column_index_scale_ < 0 || row_store_type_ < 0 || row_count_ <= 0 || occupy_size_ <= 0 ||
               data_checksum_ < 0 || micro_block_count_ <= 0 || micro_block_data_offset_ < 0 ||
               micro_block_index_offset_ < micro_block_data_offset_ ||
               micro_block_endkey_offset_ < micro_block_index_offset_ || micro_block_mark_deletion_offset_ < 0 ||
               micro_block_delta_offset_ < 0) {
      ret = false;
    } else if (0 != column_number_) {
      if (NULL == column_checksum_ || NULL == endkey_) {
        ret = false;
      }
    } else if ((0 == micro_block_mark_deletion_offset_ && 0 < micro_block_delta_offset_) ||
               (0 == micro_block_delta_offset_ && 0 < micro_block_mark_deletion_offset_)) {
      ret = false;
    } else if (partition_id_ < -1) {
      ret = false;
    } else if (column_checksum_method_ < CCM_UNKOWN || column_checksum_method_ >= CCM_MAX) {
      ret = false;
    } else if (progressive_merge_round_ < 0) {
      ret = false;
    } else if (max_merged_trans_version_ < 0 || (!contain_uncommitted_row_ && 0 == max_merged_trans_version_)) {
      ret = false;
    }
  }

  return ret;
}

int64_t ObMacroBlockMetaV2::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(attr),
      K_(data_version),
      K_(column_number),
      K_(rowkey_column_number),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(data_checksum),
      K_(table_id),
      K_(data_seq),
      K_(schema_version),
      K_(snapshot_version),
      K_(schema_rowkey_col_cnt),
      K_(row_count_delta));
  J_COMMA();
  J_KV(K_(micro_block_count),
      K_(micro_block_data_offset),
      K_(micro_block_index_offset),
      K_(micro_block_endkey_offset),
      K_(write_seq),
      K_(bf_flag),
      K_(create_timestamp),
      K_(retire_timestamp),
      K_(micro_block_mark_deletion_offset),
      K_(macro_block_deletion_flag),
      K_(micro_block_delta_offset),
      K_(partition_id),
      K_(column_checksum_method),
      K_(encrypt_id),
      K_(master_key_id),
      K_(encrypt_key),
      K_(max_merged_trans_version),
      K_(contain_uncommitted_row));
  J_COMMA();
  if (is_data_block()) {
    if (NULL != column_checksum_ && column_number_ > 0) {
      J_KV("column_checksum", ObArrayWrap<int64_t>(column_checksum_, column_number_));
    }
    J_COMMA();
    if (NULL != endkey_ && rowkey_column_number_ > 0) {
      ObStoreRowkey rowkey(endkey_, rowkey_column_number_);
      J_KV(K(rowkey));
    }
    J_COMMA();
    if (NULL != collation_free_endkey_ && rowkey_column_number_ > 0) {
      ObStoreRowkey collation_free_endkey(collation_free_endkey_, rowkey_column_number_);
      J_KV(K(collation_free_endkey));
    }
  }
  return pos;
}

int64_t ObMacroBlockMetaV2::get_meta_content_serialize_size() const
{
  int64_t size = sizeof(ObMacroBlockMetaV2);  // struct self
  if (is_data_block()) {
    if (NULL != column_checksum_) {
      size += sizeof(int64_t) * column_number_;  // column_checksum_ array
    }
    if (NULL != endkey_) {
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      size += rowkey.get_deep_copy_size();  // rowkey object array
    }
  }
  return size;
}

// Here ObRowkey is used only as an object array holder.
int ObMacroBlockMetaV2::get_deep_copy_size(
    ObRowkey& collation_free_endkey, common::ObIAllocator& allocator, int64_t& size) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  size = get_meta_content_serialize_size();
  if (is_normal_data_block()) {
    size += sizeof(int64_t) * rowkey_column_number_;  // empty_read_cnt_ array
    if (NULL != endkey_) {                            // collation_free_endkey_
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
        LOG_WARN("fail to check if need to transform to collation free", K(ret));
      } else if (need_transform) {
        if (OB_FAIL(rowkey.to_collation_free_rowkey(collation_free_endkey, allocator))) {
          LOG_WARN("fail to get collation free rowkey", K(ret));
        } else if (0 != collation_free_endkey.get_obj_cnt()) {
          size += collation_free_endkey.get_deep_copy_size();
        }
      }
    }
  }

  return ret;
}

int ObMacroBlockMetaV2::check_collation_free_valid(bool& is_collation_free_valid) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  is_collation_free_valid = true;
  if (!is_normal_data_block()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block meta attr", K(ret), K(attr_));
  } else if (NULL != endkey_) {
    ObStoreRowkey rowkey(endkey_, rowkey_column_number_);
    if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
      LOG_WARN("fail to check need transform to collation free", K(ret));
    } else if (need_transform && NULL == collation_free_endkey_) {
      is_collation_free_valid = false;
    }
  }
  return ret;
}

int ObMacroBlockMetaV2::deep_copy(ObMacroBlockMetaV2*& meta_ptr, common::ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t item_size = 0;
  ObRowkey collation_free_endkey;
  common::ObArenaAllocator tmp_allocator(ObModIds::OB_MACRO_BLOCK_META_TEMP);
  int64_t size = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta is not valid data.", K(*this), K(ret));
  } else if (OB_FAIL(get_deep_copy_size(collation_free_endkey, tmp_allocator, size))) {
    LOG_WARN("fail to get deep copy size", K(ret));
  } else if (NULL == (meta_ptr = reinterpret_cast<ObMacroBlockMetaV2*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate meta memory failed.", K(size), K(ret));
  } else {
    // copy structure;
    MEMCPY(meta_ptr, this, sizeof(ObMacroBlockMetaV2));
    pos += sizeof(ObMacroBlockMetaV2);
  }

  // todo  deep copy lob macroblock will skip empty_read_cnt_ and collation free check
  if (OB_SUCCESS == ret && is_data_block()) {
    if (OB_SUCCESS == ret && NULL != column_checksum_) {
      item_size = sizeof(int64_t) * column_number_;
      if (pos + item_size > size) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deep copy meta buffer overflow.", K(pos), K(item_size), K(size), K(ret));
      } else {
        // copy checksum;
        meta_ptr->column_checksum_ = reinterpret_cast<int64_t*>((char*)meta_ptr + pos);
        MEMCPY(meta_ptr->column_checksum_, column_checksum_, item_size);
        pos += item_size;
      }
    }
  }

  if (OB_SUCC(ret) && is_normal_data_block()) {
    if (NULL == endkey_ || rowkey_column_number_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("endkey is empty.", KP_(endkey), K_(rowkey_column_number), K(ret));
    } else {
      // copy endkey
      // ObRowkey used only as an ObObj array holder, not involved in any comparison
      ObRowkey rowkey(endkey_, rowkey_column_number_);
      bool need_transform = false;
      meta_ptr->endkey_ = reinterpret_cast<ObObj*>((char*)meta_ptr + pos);
      if (OB_FAIL(rowkey.deep_copy((char*)meta_ptr->endkey_, size - pos))) {
        LOG_WARN("copy rowkey error.", K(to_cstring(rowkey)), K(ret), K(size - pos));
      } else {
        pos += rowkey.get_deep_copy_size();
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rowkey.need_transform_to_collation_free(need_transform))) {
        LOG_WARN("fail to check if need to transform to collation free", K(ret));
      } else if (need_transform && (0 != collation_free_endkey.get_obj_cnt())) {
        int64_t endkey_size = collation_free_endkey.get_deep_copy_size();
        if (pos + endkey_size > size) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("deep copy meta buffer overflow.",
              K(ret),
              K(pos),
              K(size),
              "collation free key size",
              collation_free_endkey.get_deep_copy_size());
        } else {
          meta_ptr->collation_free_endkey_ = reinterpret_cast<ObObj*>(reinterpret_cast<char*>(meta_ptr) + pos);
          if (OB_FAIL(collation_free_endkey.deep_copy(
                  reinterpret_cast<char*>(meta_ptr->collation_free_endkey_), size - pos))) {
            LOG_WARN("deep copy failed", K(ret), K(collation_free_endkey), K(size - pos));
          } else {
            pos += endkey_size;
          }
        }
      }
    }
  }

  return ret;
}

ObFullMacroBlockMeta::ObFullMacroBlockMeta() : schema_(nullptr), meta_(nullptr)
{}

ObFullMacroBlockMeta::ObFullMacroBlockMeta(const ObMacroBlockSchemaInfo* schema, const ObMacroBlockMetaV2* meta)
    : schema_(schema), meta_(meta)
{}

int ObFullMacroBlockMeta::assign(const ObFullMacroBlockMeta& other)
{
  int ret = OB_SUCCESS;
  schema_ = other.schema_;
  meta_ = other.meta_;
  return ret;
}

void ObFullMacroBlockMeta::reset()
{
  schema_ = nullptr;
  meta_ = nullptr;
}

int ObFullMacroBlockMeta::deep_copy(ObFullMacroBlockMeta& dst, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObMacroBlockSchemaInfo* schema = nullptr;
  ObMacroBlockMetaV2* meta = nullptr;
  dst.reset();
  if (nullptr != schema_) {
    if (OB_FAIL(schema_->deep_copy(schema, allocator))) {
      LOG_WARN("fail to deep copy schema info", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr != meta_) {
    if (OB_FAIL(meta_->deep_copy(meta, allocator))) {
      LOG_WARN("fail to deep copy meta", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != schema) {
      schema->~ObMacroBlockSchemaInfo();
      schema = nullptr;
    }
    if (nullptr != dst.meta_) {
      meta->~ObMacroBlockMetaV2();
      meta = nullptr;
    }
  } else {
    dst.schema_ = schema;
    dst.meta_ = meta;
  }
  return ret;
}

int ObFullMacroBlockMeta::convert_macro_meta(const ObMacroBlockMeta& src_meta, ObMacroBlockMetaV2& dest_meta,
    ObMacroBlockSchemaInfo& dest_schema, common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  const ObMacroBlockMeta* old_meta = nullptr;
  if (OB_UNLIKELY(!src_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_meta));
  } else if (nullptr != allocator) {
    ObMacroBlockMeta* deep_copy_meta = nullptr;
    if (OB_FAIL(src_meta.deep_copy(deep_copy_meta, *allocator))) {
      LOG_WARN("fail to deep copy meta", K(ret));
    } else {
      old_meta = deep_copy_meta;
    }
  } else {
    old_meta = &src_meta;
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(old_meta)) {
    ObMacroBlockMetaV2& mbi = dest_meta;
    ObMacroBlockSchemaInfo& schema = dest_schema;
    const ObMacroBlockMeta& meta = *old_meta;
    mbi.endkey_ = meta.endkey_;
    mbi.snapshot_version_ = meta.snapshot_version_;
    mbi.attr_ = meta.attr_;
    mbi.create_timestamp_ = meta.create_timestamp_;
    mbi.data_version_ = meta.data_version_;
    mbi.column_number_ = meta.column_number_;
    mbi.rowkey_column_number_ = meta.rowkey_column_number_;
    mbi.column_index_scale_ = meta.column_index_scale_;
    mbi.row_store_type_ = meta.row_store_type_;
    mbi.row_count_ = meta.row_count_;
    mbi.occupy_size_ = meta.occupy_size_;
    mbi.column_checksum_ = meta.column_checksum_;

    mbi.data_checksum_ = meta.data_checksum_;
    mbi.micro_block_count_ = meta.micro_block_count_;
    mbi.micro_block_data_offset_ = meta.micro_block_data_offset_;
    mbi.micro_block_index_offset_ = meta.micro_block_index_offset_;
    mbi.micro_block_endkey_offset_ = meta.micro_block_endkey_offset_;
    mbi.table_id_ = meta.table_id_;
    mbi.data_seq_ = meta.data_seq_;
    mbi.schema_version_ = meta.schema_version_;
    mbi.schema_rowkey_col_cnt_ = meta.schema_rowkey_col_cnt_;
    mbi.row_count_delta_ = meta.row_count_delta_;
    mbi.micro_block_mark_deletion_offset_ = meta.micro_block_mark_deletion_offset_;
    mbi.macro_block_deletion_flag_ = meta.macro_block_deletion_flag_;
    mbi.micro_block_delta_offset_ = meta.micro_block_delta_offset_;
    mbi.write_seq_ = meta.write_seq_;
    mbi.partition_id_ = meta.partition_id_;
    mbi.column_checksum_method_ = meta.column_checksum_method_;
    mbi.progressive_merge_round_ = meta.progressive_merge_round_;
    mbi.encrypt_id_ = meta.encrypt_id_;
    mbi.master_key_id_ = meta.master_key_id_;
    MEMCPY(mbi.encrypt_key_, meta.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

    schema.column_number_ = meta.column_number_;
    schema.rowkey_column_number_ = meta.rowkey_column_number_;
    schema.schema_rowkey_col_cnt_ = meta.schema_rowkey_col_cnt_;
    schema.schema_version_ = meta.schema_version_;
    schema.compressor_ = meta.compressor_;
    schema.column_id_array_ = meta.column_id_array_;
    schema.column_type_array_ = meta.column_type_array_;
    schema.column_order_array_ = meta.column_order_array_;

    if (!mbi.is_valid() || !schema.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "build incorrect meta", K(mbi.is_valid()), K(schema.is_valid()), K(mbi), K(schema));
    }
  }
  return ret;
}

int ObFullMacroBlockMeta::convert_from_old_macro_meta(const ObMacroBlockMeta& src_meta, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;
  if (OB_UNLIKELY(!src_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(src_meta));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMacroBlockMetaV2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for macro block meta", K(ret));
  } else if (FALSE_IT(meta_ = new (buf) ObMacroBlockMetaV2())) {
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMacroBlockSchemaInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for macro block schema", K(ret));
  } else if (FALSE_IT(schema_ = new (buf) ObMacroBlockSchemaInfo())) {
  } else if (OB_FAIL(convert_macro_meta(src_meta,
                 const_cast<ObMacroBlockMetaV2&>(*meta_),
                 const_cast<ObMacroBlockSchemaInfo&>(*schema_),
                 &allocator))) {
    LOG_WARN("fail to covert macro meta", K(ret), K(src_meta));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != meta_) {
      meta_->~ObMacroBlockMetaV2();
      meta_ = nullptr;
    }
    if (nullptr != schema_) {
      schema_->~ObMacroBlockSchemaInfo();
      schema_ = nullptr;
    }
  }
  return ret;
}

ObFullMacroBlockMetaEntry::ObFullMacroBlockMetaEntry(ObMacroBlockMetaV2& meta, ObMacroBlockSchemaInfo& schema)
    : meta_(meta), schema_(schema)
{}

int ObFullMacroBlockMetaEntry::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize macro block meta v2", K(ret));
  } else if (OB_FAIL(schema_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize macro block schema info", K(ret));
  }
  return ret;
}

int ObFullMacroBlockMetaEntry::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = pos;
  int32_t header_size = 0;
  int32_t header_version = 0;
  ObBufferReader buffer_reader(buf, buf_len, pos);
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_size))) {
    LOG_WARN("deserialization header_size error.",
        K(header_size),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (OB_FAIL(buffer_reader.read(header_version))) {
    LOG_WARN("deserialization header_version error.",
        K(header_version),
        K(buffer_reader.capacity()),
        K(buffer_reader.pos()),
        K(ret));
  } else if (ObMacroBlockMetaV2::MACRO_BLOCK_META_VERSION_V1 == header_version) {
    ObMacroBlockMeta old_meta;
    old_meta.endkey_ = meta_.endkey_;
    if (OB_FAIL(old_meta.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserlize old version macro meta", K(ret), K(header_version));
    } else if (OB_FAIL(ObFullMacroBlockMeta::convert_macro_meta(old_meta, meta_, schema_))) {
      LOG_WARN("fail to covert old macro meta", K(ret), K(old_meta));
    }
  } else if (ObMacroBlockMetaV2::MACRO_BLOCK_META_VERSION_V2 == header_version) {
    if (OB_FAIL(meta_.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize macro block meta v2", K(ret));
    } else if (OB_FAIL(schema_.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize macro block schema info", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported header version", K(ret), K(header_size));
  }
  return ret;
}

int64_t ObFullMacroBlockMetaEntry::get_serialize_size() const
{
  return meta_.get_serialize_size() + schema_.get_serialize_size();
}

}  // namespace blocksstable
}  // namespace oceanbase
