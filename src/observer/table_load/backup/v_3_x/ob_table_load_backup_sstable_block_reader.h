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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_micro_block_scanner.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/blocksstable/ob_micro_block_encryption.h"
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

class ObMicroBlockIndex
{
public:
  ObMicroBlockIndex()
    : data_offset_(0), endkey_offset_(0)
  {}
  inline bool is_valid() const { return data_offset_ >= 0 && endkey_offset_ >= 0; }
  TO_STRING_KV(K_(data_offset), K_(endkey_offset));
public:
  int32_t data_offset_;
  int32_t endkey_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndex);
};

class ObLobMicroBlockIndex
{
public:
  ObLobMicroBlockIndex()
    : data_offset_(0)
  {}
  inline bool is_valid() const { return data_offset_ >= 0; }
  TO_STRING_KV(K_(data_offset));
public:
  int32_t data_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobMicroBlockIndex);
};

class ObTableLoadBackupSSTableBlockReader
{
public:
  ObTableLoadBackupSSTableBlockReader();
  ~ObTableLoadBackupSSTableBlockReader();
  int init(const char *buf, int64_t buf_size, const ObSchemaInfo &schema_info);
  void reset();
  int get_next_micro_block(const ObMicroBlockData *&micro_block_data);
  const ObMacroBlockCommonHeader& get_macro_block_common_header()
  {
    return macro_block_common_header_;
  }
  const ObColumnMap* get_column_map()
  {
    return &col_map_;
  }
  TO_STRING_KV(
      KP_(buf),
      K_(buf_size),
      K_(pos),
      KP_(macro_block_buf),
      K_(macro_block_buf_size),
      K_(macro_block_buf_pos),
      KP_(decomp_buf),
      K_(decomp_buf_size),
      KP_(backup_common_header),
      K_(macro_block_common_header),
      KP_(sstable_macro_block_header),
      KP_(micro_indexes),
      K(common::ObArrayWrap<uint16_t>(column_ids_, sstable_macro_block_header_->column_count_)),
      K(common::ObArrayWrap<common::ObObjMeta>(column_types_, sstable_macro_block_header_->column_count_)),
      KP_(column_orders),
      K(common::ObArrayWrap<int64_t>(column_checksum_, sstable_macro_block_header_->column_count_)),
      K_(columns),
      K_(col_map),
      KP_(encryption),
      KP_(compressor));
private:
  static const uint64_t OB_TENANT_ID_SHIFT = 40;
  int inner_init(const ObSchemaInfo &schema_info);
  int init_backup_common_header();
  int init_full_macro_block_meta_entry();
  int inner_init_data_block(const ObSchemaInfo &schema_info);
  int inner_init_lob_block();
  int decrypt_data_buf(
      const char *buf,
      const int64_t size,
      const char *&decrypt_buf,
      int64_t &decrypt_size);
  int init_encrypter_if_needed();
  int alloc_buf(const int64_t size);
  OB_INLINE uint64_t extract_tenant_id(uint64_t id) { return id >> OB_TENANT_ID_SHIFT; }
private:
  ObArenaAllocator allocator_;
  ObArenaAllocator buf_allocator_;
  const char *buf_;
  int64_t buf_size_;
  int64_t pos_;
  const char *macro_block_buf_;
  int64_t macro_block_buf_size_;
  int64_t macro_block_buf_pos_;
  char *decomp_buf_;
  int64_t decomp_buf_size_;
  const share::ObBackupCommonHeader *backup_common_header_;
  ObMacroBlockCommonHeader macro_block_common_header_;
  const ObSSTableMacroBlockHeader *sstable_macro_block_header_;
  const ObMicroBlockIndex *micro_indexes_; //address of the micro block index
  const ObLobMicroBlockIndex *lob_micro_indexes_; //address of the lob micro block index
  const uint16_t *column_ids_;
  const common::ObObjMeta *column_types_;
  const common::ObOrderType *column_orders_;
  const int64_t *column_checksum_;
  common::ObArray<share::schema::ObColDesc> columns_;
  ObColumnMap col_map_;
  blocksstable::ObMicroBlockEncryption *encryption_;
  common::ObCompressor *compressor_;
  int64_t cur_block_idx_;
  ObMicroBlockData micro_block_data_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupSSTableBlockReader);
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
