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
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "observer/table_load/backup/ob_table_load_backup_icolumn_map.h"
#include "observer/table_load/backup/ob_table_load_backup_imacro_block_reader.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadBackupSSTableBlockReader : public ObTableLoadBackupIMacroBlockReader
{
public:
  ObTableLoadBackupSSTableBlockReader();
  virtual ~ObTableLoadBackupSSTableBlockReader();
  int init(const char *buf, int64_t buf_size, const ObTableLoadBackupVersion &backup_version) override;
  void reset() override;
  int get_next_micro_block(const ObMicroBlockData *&micro_block_data) override;
  const ObMacroBlockMetaV2* get_macro_block_meta() { return &macro_block_meta_v2_; }
  const common::ObArray<share::schema::ObColDesc>& get_columns() { return columns_; }
  const ObIColumnMap* get_column_map() override { return col_map_; }
  bool is_lob_block() override { return macro_block_common_header_.is_lob_data_block(); }
  TO_STRING_KV(KP_(buf),
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
               KP_(col_map),
               KP_(compressor),
               K_(backup_version),
               K_(macro_block_meta_v2),
               K_(macro_block_meta_v3));
private:
  static const uint64_t OB_TENANT_ID_SHIFT = 40;
  int inner_init();
  int init_backup_common_header();
  int init_macro_block_meta();
  int init_macro_block_common_header();
  int inner_init_data_block();
  int inner_init_lob_block();
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
  ObIColumnMap *col_map_;
  common::ObCompressor *compressor_;
  int64_t cur_block_idx_;
  ObMicroBlockData micro_block_data_;
  ObTableLoadBackupVersion backup_version_;
  ObMacroBlockMetaV2 macro_block_meta_v2_;
  ObMacroBlockMetaV3 macro_block_meta_v3_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupSSTableBlockReader);
};

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
