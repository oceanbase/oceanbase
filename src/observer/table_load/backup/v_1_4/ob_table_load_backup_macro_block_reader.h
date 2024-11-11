/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_micro_block_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{

class ObTableLoadBackupMicroBlockIndex
{
public:
  ObTableLoadBackupMicroBlockIndex()
    : data_offset_(0), endkey_offset_(0) {}
  ~ObTableLoadBackupMicroBlockIndex() {}
  bool operator ==(const ObTableLoadBackupMicroBlockIndex &other) const
  {
    return data_offset_ == other.data_offset_;
  }
  bool operator !=(const ObTableLoadBackupMicroBlockIndex &other) const
  {
    return data_offset_ != other.data_offset_;
  }
  bool operator <(const ObTableLoadBackupMicroBlockIndex &other) const
  {
    return data_offset_ < other.data_offset_;
  }
  inline bool is_valid() const { return data_offset_ >= 0 && endkey_offset_ >= 0; }
  TO_STRING_KV(K(data_offset_), K(endkey_offset_));
public:
  int32_t data_offset_;
  int32_t endkey_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupMicroBlockIndex);
};

class ObTableLoadBackupMacroBlockReader
{
public:
  ObTableLoadBackupMacroBlockReader()
    : allocator_("TLD_BMBR_V_1_4"),
      micro_index_(nullptr),
      compressor_(nullptr),
      buf_(nullptr),
      buf_size_(0),
      decomp_buf_(nullptr),
      decomp_buf_size_(0),
      uncomp_buf_(nullptr),
      pos_(0),
      micro_offset_(0),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadBackupMacroBlockReader() {}
  const char* get_uncomp_buf() { return uncomp_buf_; }
  const ObTableLoadBackupColumnMap* get_column_map() { return &column_map_; }
  const ObTableLoadBackupMacroBlockMeta* get_macro_block_meta() { return &meta_; }
  const ObTableLoadBackupMicroBlockIndex* get_micro_index() { return micro_index_; }
  int init(const char *buf, int64_t buf_size);
  void reset();
  int decompress_data(const int32_t micro_block_idx);
private:
  int inner_init();
  int alloc_buf(const int64_t size);
private:
  ObArenaAllocator allocator_;
  ObTableLoadBackupMacroBlockMeta meta_;
  ObTableLoadBackupColumnMap column_map_;
  const ObTableLoadBackupMicroBlockIndex *micro_index_;
  common::ObCompressor *compressor_;
  const char *buf_;
  int64_t buf_size_;
  char *decomp_buf_;
  int64_t decomp_buf_size_;
  const char *uncomp_buf_;
  int64_t pos_;
  int64_t micro_offset_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupMacroBlockReader);
};

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
