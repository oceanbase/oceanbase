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

#include <gtest/gtest.h>
#include "storage/blocksstable/ob_data_buffer.h"
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "./ob_macro_meta_generate.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {

bool check_meta_entry(ObSuperBlockV2::MetaEntry& meta_entry, const int64_t block_idx, const int64_t log_seq,
    const int64_t file_id, const int64_t file_size)
{
  return meta_entry.block_index_ == block_idx && meta_entry.log_seq_ == log_seq && meta_entry.file_id_ == file_id &&
         meta_entry.file_size_ == file_size;
}

struct SuperBlock20 {
  SuperBlock20();
  static const int64_t MAX_BACKUP_META_COUNT = 2;
  static const int64_t SUPER_BLOCK_RESERVED_COUNT = 7;
  static const int64_t MAX_SUPER_BLOCK_SIZE = 1 << 12;

  int32_t header_size_;
  int32_t version_;
  int32_t magic_;  // magic number
  int32_t attr_;   // reserved, set 0

  int64_t create_timestamp_;  // create timestamp
  int64_t modify_timestamp_;  // last modified timestamp
  int64_t macro_block_size_;
  int64_t total_macro_block_count_;
  int64_t reserved_block_count_;
  int64_t free_macro_block_count_;
  int64_t first_macro_block_;
  int64_t first_free_block_index_;
  int64_t total_file_size_;

  int64_t backup_meta_count_;
  int64_t backup_meta_blocks_[MAX_BACKUP_META_COUNT];

  int64_t macro_block_meta_entry_block_index_;  // entry of macro block meta blocks.
  int64_t partition_meta_entry_block_index_;    // entry of partition meta blocks.
  common::ObLogCursor replay_start_point_;
  int64_t table_mgr_meta_entry_block_index_;  // entry of table mgr macro block meta blocks.
  int64_t partition_meta_log_seq_;            // log seq of partition meta in checkpoint
  int64_t table_mgr_meta_log_seq_;            // log seq of table mgr meta in checkpoint
  int64_t reserved_[SUPER_BLOCK_RESERVED_COUNT];

  bool is_valid() const;
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(create_timestamp), K_(modify_timestamp),
      K_(macro_block_size), K_(total_macro_block_count), K_(reserved_block_count), K_(free_macro_block_count),
      K_(first_macro_block), K_(first_free_block_index), K_(total_file_size), K_(backup_meta_count),
      "backup_meta_blocks_", common::ObArrayWrap<int64_t>(backup_meta_blocks_, backup_meta_count_),
      K_(macro_block_meta_entry_block_index), K_(partition_meta_entry_block_index),
      K_(table_mgr_meta_entry_block_index), K_(partition_meta_log_seq), K_(table_mgr_meta_log_seq),
      K_(replay_start_point));
  NEED_SERIALIZE_AND_DESERIALIZE;
};
//================================SuperBlock======================================
SuperBlock20::SuperBlock20()
    : header_size_(0),
      version_(0),
      magic_(0),
      attr_(0),
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

bool SuperBlock20::is_valid() const
{
  return header_size_ > 0 && version_ >= 0 && SUPER_BLOCK_MAGIC == magic_ && attr_ >= 0 && create_timestamp_ > 0 &&
         modify_timestamp_ >= create_timestamp_ && macro_block_size_ > 0 && total_macro_block_count_ > 0 &&
         reserved_block_count_ >= 0 && free_macro_block_count_ >= 0 && first_macro_block_ >= 0 &&
         first_free_block_index_ > 0 && total_file_size_ >= macro_block_size_ && backup_meta_count_ > 0 &&
         macro_block_meta_entry_block_index_ >= -1 && partition_meta_entry_block_index_ >= -1 &&
         replay_start_point_.is_valid() && table_mgr_meta_entry_block_index_ >= -1 && partition_meta_log_seq_ >= 0 &&
         table_mgr_meta_log_seq_ >= 0;
}

DEFINE_SERIALIZE(SuperBlock20)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < header_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "serialize superblock failed.", KP(buf), K(buf_len), K(pos), K(header_size_), K(ret));
  } else {
    MEMCPY(buf + pos, this, sizeof(SuperBlock20));
    pos += header_size_;
  }
  return ret;
}

DEFINE_DESERIALIZE(SuperBlock20)
{
  int ret = OB_SUCCESS;
  // read size first;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(int32_t))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(pos), K(header_size_), K(ret));
  } else {
    int32_t header_size = *(reinterpret_cast<const int32_t*>(buf));
    if (data_len - pos < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(ERROR, "data_len not enough for header size.", K(data_len), K(pos), K(header_size), K(ret));
    } else {
      MEMCPY(this, buf + pos, sizeof(SuperBlock20));
      pos += header_size;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(SuperBlock20)
{
  return sizeof(SuperBlock20);
}

struct SuperBlock14 {
  SuperBlock14();
  static const int64_t MAX_BACKUP_META_COUNT = 2;
  static const int64_t SUPER_BLOCK_RESERVED_COUNT = 10;
  static const int64_t MAX_SUPER_BLOCK_SIZE = 1 << 12;

  int32_t header_size_;
  int32_t version_;
  int32_t magic_;  // magic number
  int32_t attr_;   // reserved, set 0

  int64_t create_timestamp_;  // create timestamp
  int64_t modify_timestamp_;  // last modified timestamp
  int64_t macro_block_size_;
  int64_t total_macro_block_count_;
  int64_t reserved_block_count_;
  int64_t free_macro_block_count_;
  int64_t first_macro_block_;
  int64_t first_free_block_index_;
  int64_t total_file_size_;

  int64_t backup_meta_count_;
  int64_t backup_meta_blocks_[MAX_BACKUP_META_COUNT];

  int64_t macro_block_meta_entry_block_index_;  // entry of macro block meta blocks.
  int64_t partition_meta_entry_block_index_;    // entry of partition meta blocks.
  common::ObLogCursor replay_start_point_;
  int64_t reserved_[SUPER_BLOCK_RESERVED_COUNT];

  bool is_valid() const;
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(create_timestamp), K_(modify_timestamp),
      K_(macro_block_size), K_(total_macro_block_count), K_(reserved_block_count), K_(free_macro_block_count),
      K_(first_macro_block), K_(first_free_block_index), K_(total_file_size), K_(backup_meta_count),
      "backup_meta_blocks_", common::ObArrayWrap<int64_t>(backup_meta_blocks_, backup_meta_count_),
      K_(macro_block_meta_entry_block_index), K_(partition_meta_entry_block_index), K_(replay_start_point));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

//================================SuperBlock======================================
SuperBlock14::SuperBlock14()
    : header_size_(0),
      version_(0),
      magic_(0),
      attr_(0),
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
      partition_meta_entry_block_index_(0)

{
  memset(backup_meta_blocks_, 0, sizeof(backup_meta_blocks_));
  memset(reserved_, 0, sizeof(reserved_));
}

bool SuperBlock14::is_valid() const
{
  return header_size_ > 0 && version_ >= 0 && SUPER_BLOCK_MAGIC == magic_ && attr_ >= 0 && create_timestamp_ > 0 &&
         modify_timestamp_ >= create_timestamp_ && macro_block_size_ > 0 && total_macro_block_count_ > 0 &&
         reserved_block_count_ >= 0 && free_macro_block_count_ >= 0 && first_macro_block_ >= 0 &&
         first_free_block_index_ > 0 && total_file_size_ >= macro_block_size_ && backup_meta_count_ > 0 &&
         macro_block_meta_entry_block_index_ >= -1 && partition_meta_entry_block_index_ >= -1 &&
         replay_start_point_.is_valid();
}

DEFINE_SERIALIZE(SuperBlock14)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < header_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "serialize superblock failed.", KP(buf), K(buf_len), K(pos), K(header_size_), K(ret));
  } else {
    MEMCPY(buf + pos, this, sizeof(SuperBlock14));
    pos += header_size_;
  }
  return ret;
}

DEFINE_DESERIALIZE(SuperBlock14)
{
  int ret = OB_SUCCESS;
  // read size first;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(int32_t))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(pos), K(header_size_), K(ret));
  } else {
    int32_t header_size = *(reinterpret_cast<const int32_t*>(buf));
    if (data_len - pos < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(ERROR, "data_len not enough for header size.", K(data_len), K(pos), K(header_size), K(ret));
    } else {
      MEMCPY(this, buf + pos, sizeof(SuperBlock14));
      pos += header_size;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(SuperBlock14)
{
  return sizeof(SuperBlock14);
}

TEST(ObCommitLogSpec, normal)
{
  // is_valid() test
  ObCommitLogSpec log_spec;
  log_spec.log_dir_ = "./";
  log_spec.max_log_size_ = 2L * 1024L;
  log_spec.log_sync_type_ = 0;
  ASSERT_TRUE(log_spec.is_valid());
  log_spec.log_dir_ = NULL;
  ASSERT_FALSE(log_spec.is_valid());
  // to_string() test
  const char* out = to_cstring(log_spec);
  ASSERT_STRNE(NULL, out);
}

TEST(ObStorageEnv, normal)
{
  // to_string() test
  ObStorageEnv env;
  const char* out = to_cstring(env);
  ASSERT_STRNE(NULL, out);
}

TEST(SuperBlock, normal)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t buf_len = ObSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE;
  char* buf = static_cast<char*>(allocator.alloc(buf_len));
  ASSERT_TRUE(NULL != buf);
  int64_t pos = 0;

  ObSuperBlockV2 sb1;
  ObSuperBlockV2 sb2;

  sb1.reset();
  sb1.header_.version_ = OB_SUPER_BLOCK_V2;
  sb1.header_.magic_ = SUPER_BLOCK_MAGIC_V2;
  sb1.header_.attr_ = 0;

  sb1.content_.create_timestamp_ = ObTimeUtility::current_time();
  sb1.content_.modify_timestamp_ = sb1.content_.create_timestamp_;
  sb1.content_.macro_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  sb1.content_.total_macro_block_count_ = 10;
  sb1.content_.free_macro_block_count_ = 0;
  sb1.content_.total_file_size_ = 10 * common::OB_DEFAULT_MACRO_BLOCK_SIZE;

  sb1.content_.macro_block_meta_.reset();
  sb1.content_.partition_meta_.reset();
  sb1.content_.table_mgr_meta_.reset();
  sb1.content_.replay_start_point_.file_id_ = 1;
  sb1.content_.replay_start_point_.log_id_ = 0;
  sb1.content_.replay_start_point_.offset_ = 0;
  sb1.header_.super_block_size_ = static_cast<int32_t>(sb1.get_serialize_size());

  // serialize and deserizlize success test
  pos = 0;
  ret = sb1.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  pos = 0;
  ret = sb2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp(&sb1, &sb2, sizeof(sb1)));

  // serialize boundary test
  pos = 0;
  ret = sb1.serialize(buf, 10, pos);
  ASSERT_NE(OB_SUCCESS, ret);
  // deserialize boundary test
  pos = 0;
  ret = sb2.deserialize(buf, 10, pos);
  ASSERT_NE(OB_SUCCESS, ret);
  // to_string() test
  const char* out = to_cstring(sb1);
  ASSERT_STRNE(NULL, out);
}

TEST(SuperBlock, upgrade_from_20)
{
  const int64_t buf_len = SuperBlock20::MAX_SUPER_BLOCK_SIZE;
  char buf[buf_len];
  SuperBlock20 super_block;
  ObSuperBlockV1 super_block_v1;
  ObSuperBlockV2 super_block_v2;
  const int64_t data_file_size = 100 * 1024 * 1024;
  int64_t pos = 0;

  ASSERT_GT(buf_len, sizeof(super_block_v1));
  ASSERT_EQ(sizeof(super_block_v1), sizeof(super_block));

  MEMSET(&super_block, 0, sizeof(SuperBlock20));
  super_block.header_size_ = sizeof(SuperBlock20);
  super_block.version_ = ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION;
  super_block.magic_ = SUPER_BLOCK_MAGIC;
  super_block.attr_ = 0;

  // first two blocks are superblock.
  super_block.backup_meta_count_ = SuperBlock20::MAX_BACKUP_META_COUNT;
  super_block.backup_meta_blocks_[0] = 0;
  super_block.backup_meta_blocks_[1] = 1;

  super_block.create_timestamp_ = ObTimeUtility::current_time();
  super_block.modify_timestamp_ = super_block.create_timestamp_;
  super_block.macro_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  super_block.total_macro_block_count_ = data_file_size / common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  super_block.reserved_block_count_ = super_block.backup_meta_count_;
  super_block.free_macro_block_count_ = 0;
  super_block.first_macro_block_ = super_block.backup_meta_blocks_[1] + 1;
  super_block.first_free_block_index_ = super_block.first_macro_block_;
  super_block.total_file_size_ = lower_align(data_file_size, common::OB_DEFAULT_MACRO_BLOCK_SIZE);

  super_block.macro_block_meta_entry_block_index_ = 10;
  super_block.partition_meta_entry_block_index_ = 20;
  super_block.table_mgr_meta_entry_block_index_ = 30;
  super_block.partition_meta_log_seq_ = 40;
  super_block.table_mgr_meta_log_seq_ = 50;
  super_block.replay_start_point_.file_id_ = 60;
  super_block.replay_start_point_.log_id_ = 70;
  super_block.replay_start_point_.offset_ = 80;

  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, buf_len, pos, ob_crc64(&super_block, super_block.header_size_)));
  ASSERT_EQ(OB_SUCCESS, super_block.serialize(buf, buf_len, pos));
  ASSERT_EQ(sizeof(super_block) + serialization::encoded_length_i64(0), pos);
  STORAGE_LOG(INFO, "succeed to serialize super block", K(buf_len), K(pos), K(super_block));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_block_v1.read_super_block_buf(buf, buf_len, pos));
  STORAGE_LOG(INFO, "read super block v1", K(super_block), K(super_block_v1));
  ASSERT_EQ(0, memcmp(&super_block_v1, &super_block, sizeof(super_block)));
  ASSERT_EQ(OB_SUCCESS, super_block_v2.set_super_block(super_block_v1));
  STORAGE_LOG(INFO, "convert from v1", K(super_block_v1), K(super_block_v2));

  const int64_t super_block_size = super_block_v2.get_serialize_size();
  ASSERT_EQ(super_block_size, super_block_v2.header_.super_block_size_);
  ASSERT_EQ(OB_SUPER_BLOCK_V2, super_block_v2.header_.version_);
  ASSERT_EQ(SUPER_BLOCK_MAGIC_V2, super_block_v2.header_.magic_);
  ASSERT_EQ(0, super_block_v2.header_.attr_);
  ASSERT_EQ(super_block.create_timestamp_, super_block_v2.content_.create_timestamp_);
  ASSERT_EQ(super_block.modify_timestamp_, super_block_v2.content_.modify_timestamp_);
  ASSERT_EQ(super_block.macro_block_size_, super_block_v2.content_.macro_block_size_);
  ASSERT_EQ(super_block.total_macro_block_count_, super_block_v2.content_.total_macro_block_count_);
  ASSERT_EQ(super_block.free_macro_block_count_, super_block_v2.content_.free_macro_block_count_);
  ASSERT_EQ(super_block.total_file_size_, super_block_v2.content_.total_file_size_);
  ASSERT_EQ(super_block.replay_start_point_.file_id_, super_block_v2.content_.replay_start_point_.file_id_);
  ASSERT_EQ(super_block.replay_start_point_.log_id_, super_block_v2.content_.replay_start_point_.log_id_);
  ASSERT_EQ(super_block.replay_start_point_.offset_, super_block_v2.content_.replay_start_point_.offset_);
  ASSERT_TRUE(check_meta_entry(
      super_block_v2.content_.macro_block_meta_, super_block.macro_block_meta_entry_block_index_, 0, 0, 0));
  ASSERT_TRUE(check_meta_entry(super_block_v2.content_.partition_meta_,
      super_block.partition_meta_entry_block_index_,
      super_block.partition_meta_log_seq_,
      0,
      0));
  ASSERT_TRUE(check_meta_entry(super_block_v2.content_.table_mgr_meta_,
      super_block.table_mgr_meta_entry_block_index_,
      super_block.table_mgr_meta_log_seq_,
      0,
      0));
}

TEST(SuperBlock, upgrade_from_14)
{
  const int64_t buf_len = SuperBlock14::MAX_SUPER_BLOCK_SIZE;
  char buf[buf_len];
  SuperBlock14 super_block;
  ObSuperBlockV1 super_block_v1;
  ObSuperBlockV2 super_block_v2;
  const int64_t data_file_size = 100 * 1024 * 1024;
  int64_t pos = 0;

  ASSERT_GT(buf_len, sizeof(super_block_v1));
  ASSERT_EQ(sizeof(super_block_v1), sizeof(super_block));

  MEMSET(&super_block, 0, sizeof(SuperBlock14));
  super_block.header_size_ = sizeof(SuperBlock14);
  super_block.version_ = ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION;
  super_block.magic_ = SUPER_BLOCK_MAGIC;
  super_block.attr_ = 0;

  // first two blocks are superblock.
  super_block.backup_meta_count_ = SuperBlock14::MAX_BACKUP_META_COUNT;
  super_block.backup_meta_blocks_[0] = 0;
  super_block.backup_meta_blocks_[1] = 1;

  super_block.create_timestamp_ = ObTimeUtility::current_time();
  super_block.modify_timestamp_ = super_block.create_timestamp_;
  super_block.macro_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  super_block.total_macro_block_count_ = data_file_size / common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  super_block.reserved_block_count_ = super_block.backup_meta_count_;
  super_block.free_macro_block_count_ = 0;
  super_block.first_macro_block_ = super_block.backup_meta_blocks_[1] + 1;
  super_block.first_free_block_index_ = super_block.first_macro_block_;
  super_block.total_file_size_ = lower_align(data_file_size, common::OB_DEFAULT_MACRO_BLOCK_SIZE);

  super_block.macro_block_meta_entry_block_index_ = 10;
  super_block.partition_meta_entry_block_index_ = 20;
  super_block.replay_start_point_.file_id_ = 60;
  super_block.replay_start_point_.log_id_ = 70;
  super_block.replay_start_point_.offset_ = 80;

  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, buf_len, pos, ob_crc64(&super_block, super_block.header_size_)));
  ASSERT_EQ(OB_SUCCESS, super_block.serialize(buf, buf_len, pos));
  ASSERT_EQ(sizeof(super_block) + serialization::encoded_length_i64(0), pos);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_block_v1.read_super_block_buf(buf, buf_len, pos));
  STORAGE_LOG(INFO, "read super block v1", K(super_block), K(super_block_v1));
  ASSERT_EQ(0, memcmp(&super_block_v1, &super_block, sizeof(super_block)));
  ASSERT_EQ(OB_SUCCESS, super_block_v2.set_super_block(super_block_v1));
  STORAGE_LOG(INFO, "convert from v1", K(super_block_v1), K(super_block_v2));

  ASSERT_EQ(super_block_v2.get_serialize_size(), super_block_v2.header_.super_block_size_);
  ASSERT_EQ(OB_SUPER_BLOCK_V2, super_block_v2.header_.version_);
  ASSERT_EQ(SUPER_BLOCK_MAGIC_V2, super_block_v2.header_.magic_);
  ASSERT_EQ(0, super_block_v2.header_.attr_);
  ASSERT_EQ(super_block.create_timestamp_, super_block_v2.content_.create_timestamp_);
  ASSERT_EQ(super_block.modify_timestamp_, super_block_v2.content_.modify_timestamp_);
  ASSERT_EQ(super_block.macro_block_size_, super_block_v2.content_.macro_block_size_);
  ASSERT_EQ(super_block.total_macro_block_count_, super_block_v2.content_.total_macro_block_count_);
  ASSERT_EQ(super_block.free_macro_block_count_, super_block_v2.content_.free_macro_block_count_);
  ASSERT_EQ(super_block.total_file_size_, super_block_v2.content_.total_file_size_);
  ASSERT_EQ(super_block.replay_start_point_.file_id_, super_block_v2.content_.replay_start_point_.file_id_);
  ASSERT_EQ(super_block.replay_start_point_.log_id_, super_block_v2.content_.replay_start_point_.log_id_);
  ASSERT_EQ(super_block.replay_start_point_.offset_, super_block_v2.content_.replay_start_point_.offset_);
  ASSERT_TRUE(check_meta_entry(
      super_block_v2.content_.macro_block_meta_, super_block.macro_block_meta_entry_block_index_, 0, 0, 0));
  ASSERT_TRUE(check_meta_entry(
      super_block_v2.content_.partition_meta_, super_block.partition_meta_entry_block_index_, 0, 0, 0));
  ASSERT_TRUE(check_meta_entry(super_block_v2.content_.table_mgr_meta_, 0, 0, 0, 0));
}

TEST(SuperBlock, set_super_block)
{
  int ret = OB_SUCCESS;
  const int64_t data_file_size = 100 * 1024 * 1024;
  ObSuperBlockV1 src_v1;
  ObSuperBlockV1 dst_v1;
  ObSuperBlockV2 sb_v2;
  MEMSET(&src_v1, 0, sizeof(src_v1));
  MEMSET(&dst_v1, 0, sizeof(dst_v1));
  MEMSET(&sb_v2, 0, sizeof(sb_v2));

  src_v1.header_.super_block_size_ = sizeof(src_v1);
  src_v1.header_.version_ = ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION;
  src_v1.header_.magic_ = SUPER_BLOCK_MAGIC;
  src_v1.header_.attr_ = 0;

  src_v1.backup_meta_count_ = ObSuperBlockV1::MAX_BACKUP_META_COUNT;
  src_v1.backup_meta_blocks_[0] = 0;
  src_v1.backup_meta_blocks_[1] = 1;

  src_v1.create_timestamp_ = ObTimeUtility::current_time();
  src_v1.modify_timestamp_ = ObTimeUtility::current_time();
  src_v1.macro_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  src_v1.total_macro_block_count_ = data_file_size / common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  src_v1.reserved_block_count_ = src_v1.backup_meta_count_;
  src_v1.free_macro_block_count_ = 100;
  src_v1.first_macro_block_ = src_v1.backup_meta_blocks_[1] + 1;
  src_v1.first_free_block_index_ = src_v1.first_macro_block_;
  src_v1.total_file_size_ = lower_align(data_file_size, common::OB_DEFAULT_MACRO_BLOCK_SIZE);

  src_v1.macro_block_meta_entry_block_index_ = 10;
  src_v1.partition_meta_entry_block_index_ = 20;
  src_v1.table_mgr_meta_entry_block_index_ = 30;
  src_v1.partition_meta_log_seq_ = 40;
  src_v1.table_mgr_meta_log_seq_ = 50;
  src_v1.replay_start_point_.file_id_ = 60;
  src_v1.replay_start_point_.log_id_ = 70;
  src_v1.replay_start_point_.offset_ = 80;

  ret = sb_v2.set_super_block(src_v1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = dst_v1.set_super_block(sb_v2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(src_v1.header_.super_block_size_, dst_v1.header_.super_block_size_);
  ASSERT_EQ(src_v1.header_.version_, dst_v1.header_.version_);
  ASSERT_EQ(src_v1.header_.magic_, dst_v1.header_.magic_);
  ASSERT_EQ(src_v1.header_.attr_, dst_v1.header_.attr_);

  ASSERT_EQ(src_v1.backup_meta_count_, dst_v1.backup_meta_count_);
  ASSERT_EQ(src_v1.backup_meta_blocks_[0], dst_v1.backup_meta_blocks_[0]);
  ASSERT_EQ(src_v1.backup_meta_blocks_[1], dst_v1.backup_meta_blocks_[1]);

  ASSERT_EQ(src_v1.create_timestamp_, dst_v1.create_timestamp_);
  ASSERT_EQ(src_v1.modify_timestamp_, dst_v1.modify_timestamp_);
  ASSERT_EQ(src_v1.macro_block_size_, dst_v1.macro_block_size_);
  ASSERT_EQ(src_v1.total_macro_block_count_, dst_v1.total_macro_block_count_);
  ASSERT_EQ(src_v1.reserved_block_count_, dst_v1.reserved_block_count_);
  ASSERT_EQ(src_v1.free_macro_block_count_, dst_v1.free_macro_block_count_);
  ASSERT_EQ(src_v1.first_macro_block_, dst_v1.first_macro_block_);
  ASSERT_EQ(src_v1.first_free_block_index_, dst_v1.first_free_block_index_);
  ASSERT_EQ(src_v1.total_file_size_, dst_v1.total_file_size_);

  ASSERT_EQ(src_v1.macro_block_meta_entry_block_index_, dst_v1.macro_block_meta_entry_block_index_);
  ASSERT_EQ(src_v1.partition_meta_entry_block_index_, dst_v1.partition_meta_entry_block_index_);
  ASSERT_EQ(src_v1.table_mgr_meta_entry_block_index_, dst_v1.table_mgr_meta_entry_block_index_);
  ASSERT_EQ(src_v1.partition_meta_log_seq_, dst_v1.partition_meta_log_seq_);
  ASSERT_EQ(src_v1.table_mgr_meta_log_seq_, dst_v1.table_mgr_meta_log_seq_);
  ASSERT_EQ(src_v1.replay_start_point_.file_id_, dst_v1.replay_start_point_.file_id_);
  ASSERT_EQ(src_v1.replay_start_point_.log_id_, dst_v1.replay_start_point_.log_id_);
  ASSERT_EQ(src_v1.replay_start_point_.offset_, dst_v1.replay_start_point_.offset_);
}

TEST(ObLinkedMacroBlockHeader, normal)
{
  // check() test
  ObLinkedMacroBlockHeader link_header;
  link_header.version_ = 1;
  link_header.magic_ = 2;
  link_header.attr_ = 1;
  ASSERT_FALSE(link_header.is_valid());
  link_header.header_size_ = sizeof(link_header);
  link_header.version_ = LINKED_MACRO_BLOCK_HEADER_VERSION;
  link_header.magic_ = MACRO_META_HEADER_MAGIC;
  link_header.attr_ = ObMacroBlockCommonHeader::MacroMeta;
  ASSERT_TRUE(link_header.is_valid());
  // to_string() test
  const char* out = to_cstring(link_header);
  ASSERT_STRNE(NULL, out);
}

TEST(ObMicroBlockHeader, normal)
{
  // to_string() test
  ObMicroBlockHeader micro_header;
  const char* out = to_cstring(micro_header);
  ASSERT_STRNE(NULL, out);
}

TEST(ObMacroBlockCommonHeader, normal)
{
  // check() test
  ObMacroBlockCommonHeader common_header;
  common_header.set_attr(ObMacroBlockCommonHeader::PartitionMeta);
  ASSERT_TRUE(common_header.is_valid());
  common_header.set_previous_block_index(-2);
  ASSERT_FALSE(common_header.is_valid());
  // to_string() test
  const char* out = to_cstring(common_header);
  ASSERT_STRNE(NULL, out);
  // serialization length test
  ASSERT_EQ(common_header.header_size_, common_header.get_serialize_size());
}

TEST(ObSSTableMacroBlockHeader, normal)
{
  // to_string() test
  ObSSTableMacroBlockHeader sstable_header;
  const char* out = to_cstring(sstable_header);
  ASSERT_STRNE(NULL, out);
}

TEST(ObMacroBlockMeta, serialize)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t buf_len = 0;
  int64_t pos = 0;
  char* buf = NULL;

  ObMacroBlockMeta meta1;
  ObMacroBlockMeta meta2;

  // serialize and deserialize success test
  ObMacroMetaGenerator::gen_meta(meta1, allocator);
  buf_len = meta1.get_serialize_size();
  buf = static_cast<char*>(allocator.alloc(buf_len));
  ASSERT_STRNE(NULL, buf);
  pos = 0;
  ret = meta1.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObj out_endkey[128];
  meta2.endkey_ = out_endkey;
  pos = 0;
  ret = meta2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(meta1 == meta2);

  // serialize boundary
  for (int64_t i = buf_len - 50; i < buf_len; ++i) {
    pos = i;
    ret = meta1.serialize(buf, buf_len, pos);
    ASSERT_NE(OB_SUCCESS, ret);
  }
}

TEST(ObMacroBlockMeta, deep_copy)
{
  int ret = OB_SUCCESS;
  int64_t column_num = 7;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMacroBlockMeta meta1;
  ObMacroBlockMeta* meta2 = NULL;
  ObMacroMetaGenerator::gen_meta(meta1, allocator);
  ObRowkey rowkey(meta1.endkey_, column_num);
  ObRowkey collation_free_rowkey;
  int64_t deep_copy_size = sizeof(ObMacroBlockMeta) + 5        // compressor_
                           + sizeof(uint16_t) * column_num     // column_id_array_
                           + sizeof(ObObjMeta) * column_num    // column_type_array
                           + sizeof(ObOrderType) * column_num  // column_order_array_
                           + sizeof(int64_t) * column_num      // column_checksum_
                           + sizeof(int64_t) * column_num      // empty_read_cnt_
                           + rowkey.get_deep_copy_size();      // endkey_
  int64_t get_deep_copy_size = 0;
  ASSERT_EQ(OB_SUCCESS, meta1.get_deep_copy_size(collation_free_rowkey, allocator, get_deep_copy_size));
  deep_copy_size += collation_free_rowkey.get_deep_copy_size();
  ASSERT_EQ(deep_copy_size, get_deep_copy_size);
  // copy success test
  ret = meta1.deep_copy(meta2, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(NULL != meta2);
  ASSERT_TRUE(meta1 == *meta2);

  // empty_read_cnt_ = NULL;
  ObMacroBlockMeta* meta3 = NULL;
  ASSERT_EQ(OB_SUCCESS, meta1.get_deep_copy_size(collation_free_rowkey, allocator, get_deep_copy_size));
  ASSERT_EQ(deep_copy_size, get_deep_copy_size);
  ret = meta1.deep_copy(meta3, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != meta3);
  ASSERT_TRUE(meta1 == *meta3);
}

TEST(ObMacroBlockMeta, misc)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObMacroBlockMeta meta;
  ObMacroMetaGenerator::gen_meta(meta, allocator);
  // to_string() test
  const char* out = to_cstring(meta);
  ASSERT_STRNE(NULL, out);
}

TEST(ObSSTableMeta, normal)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t buf_len = 0;
  int64_t pos = 0;
  char* buf = NULL;

  ObSSTableMeta smeta1(allocator);
  ObSSTableMeta smeta2(allocator);
  smeta1.index_id_ = 3001;
  smeta1.row_count_ = 2;
  smeta1.occupy_size_ = 2 * 1024 * 1024;
  smeta1.data_checksum_ = 12345;
  smeta1.row_checksum_ = 12345;
  smeta1.data_version_ = 1;
  smeta1.rowkey_column_count_ = 2;
  smeta1.table_type_ = 0;
  smeta1.index_type_ = 0;
  smeta1.available_version_ = 0;
  smeta1.macro_block_count_ = 1;
  smeta1.column_cnt_ = 10;
  smeta1.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  ObSSTableColumnMeta col_meta;
  ASSERT_EQ(OB_SUCCESS, smeta1.column_metas_.reserve(smeta1.column_cnt_));
  ASSERT_EQ(OB_SUCCESS, smeta1.new_column_metas_.reserve(smeta1.column_cnt_));
  for (int64_t i = 0; i < smeta1.column_cnt_; ++i) {
    col_meta.column_id_ = i + 1;
    col_meta.column_default_checksum_ = i;
    col_meta.column_checksum_ = i * 10;
    smeta1.column_metas_.push_back(col_meta);
  }
  for (int64_t i = 0; i < smeta1.column_cnt_; ++i) {
    col_meta.column_id_ = i + 1;
    col_meta.column_default_checksum_ = i;
    col_meta.column_checksum_ = i * 10;
    smeta1.new_column_metas_.push_back(col_meta);
  }
  MacroBlockId block_id(0, 0, 0, 2);
  ret = smeta1.macro_block_array_.push_back(block_id);
  ASSERT_EQ(ret, OB_SUCCESS);

  buf_len = smeta1.get_serialize_size();
  buf = static_cast<char*>(allocator.alloc(buf_len));
  ASSERT_STRNE(NULL, buf);

  // serizlize and deserialize success
  pos = 0;
  ret = smeta1.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(smeta1.get_serialize_size(), pos);

  pos = 0;
  ret = smeta2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(smeta1 == smeta2);

  // serialize boundary test
  for (int64_t i = 1; i < buf_len; ++i) {
    pos = i;
    ret = smeta1.serialize(buf, buf_len, pos);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // deserialize boundary test
  for (int64_t i = 1; i < buf_len; ++i) {
    pos = i;
    ret = smeta2.deserialize(buf, buf_len, pos);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // to_string
  const char* out = to_cstring(smeta1);
  ASSERT_STRNE(NULL, out);
}

TEST(ObPartitionMeta, normal)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t buf_len = 0;
  int64_t pos = 0;
  char* buf = NULL;

  ObPartitionMeta pmeta1;
  ObPartitionMeta pmeta2;
  memset(&pmeta1, 223, sizeof(pmeta1));
  pmeta1.log_info_.assign_ptr("hello", static_cast<ObString::obstr_size_t>(strlen("hello")));

  // serialize and deserialize boundary test
  buf_len = pmeta1.get_serialize_size();
  buf = static_cast<char*>(allocator.alloc(buf_len));
  ASSERT_STRNE(NULL, buf);
  for (int64_t i = 1; i < buf_len; ++i) {
    pos = i;
    ret = pmeta1.serialize(buf, buf_len, pos);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // serialize and deserialize success test
  pos = 0;
  ret = pmeta1.serialize(buf, buf_len, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, pmeta1.get_serialize_size());

  pos = 0;
  ret = pmeta2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(pmeta1 == pmeta2);

  pmeta2.reset();
  pos = 0;
  ret = pmeta2.deserialize(buf, buf_len, pos, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(pmeta1 == pmeta2);

  // to_string
  const char* out = to_cstring(pmeta1);
  ASSERT_STRNE(NULL, out);
}

TEST(ObSSTableMeta, to_string)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSTableMeta sstable_meta(allocator);
  MacroBlockId block_id(1);
  const int64_t buf_len = 100;
  char buf[buf_len];
  int64_t pos = 0;

  for (int64_t i = 0; i < 10000; ++i) {
    sstable_meta.macro_block_count_++;
    ret = sstable_meta.macro_block_array_.push_back(block_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  pos = sstable_meta.to_string(buf, buf_len);
  ASSERT_TRUE(pos <= buf_len);
}

TEST(ObPartitionMeta, deserialize_error)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObLfFIFOAllocator fifo_allocator;
  int64_t buf_len = 0;
  int64_t pos = 0;
  char* buf = NULL;

  ObPartitionMeta pmeta1;
  ObPartitionMeta pmeta2;
  memset(&pmeta1, 223, sizeof(pmeta1));
  pmeta1.log_info_.assign_ptr("hello", static_cast<ObString::obstr_size_t>(strlen("hello")));

  ASSERT_EQ(
      OB_SUCCESS, fifo_allocator.init(8 * 1024 * 1024, ObModIds::TEST, OB_SERVER_TENANT_ID, 1, 8 * 1024 * 1024 + 4096));
  void* used = fifo_allocator.alloc(8 * 1024 * 1024 - 17 /*sizeof(SliceHeader)*/ - 1);
  ASSERT_TRUE(NULL != used);
  buf_len = pmeta1.get_serialize_size();
  buf = static_cast<char*>(allocator.alloc(buf_len));
  ASSERT_TRUE(NULL != buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, pmeta1.serialize(buf, buf_len, pos));

  pos = 0;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, pmeta2.deserialize(buf, buf_len, pos, fifo_allocator));
  if (NULL != pmeta2.log_info_.ptr()) {
    fifo_allocator.free(pmeta2.log_info_.ptr());
  }
  fifo_allocator.free(used);
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}
