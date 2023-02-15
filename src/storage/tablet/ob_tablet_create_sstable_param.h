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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM

#include "lib/utility/ob_print_utils.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/scn.h"
#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
namespace storage
{
struct ObTabletCreateSSTableParam final
{
public:
  ObTabletCreateSSTableParam();
  ~ObTabletCreateSSTableParam() = default;
  ObTabletCreateSSTableParam(const ObTabletCreateSSTableParam &other) = delete;
  ObTabletCreateSSTableParam &operator=(ObTabletCreateSSTableParam &other) = delete;
public:
  bool is_valid() const;
  bool is_block_meta_valid(const ObMetaDiskAddr &addr,
                           const blocksstable::ObMicroBlockData &data) const;

  TO_STRING_KV(K_(table_key),
      K_(sstable_logic_seq),
      K_(schema_version),
      K_(create_snapshot_version),
      K_(progressive_merge_round),
      K_(progressive_merge_step),
      K_(is_ready_for_read),
      K_(table_mode),
      K_(index_type),
      K_(root_block_addr),
      K_(root_block_data),
      K_(root_row_store_type),
      K_(latest_row_store_type),
      K_(data_index_tree_height),
      K_(data_block_macro_meta_addr),
      K_(data_block_macro_meta),
      K_(index_blocks_cnt),
      K_(data_blocks_cnt),
      K_(micro_block_cnt),
      K_(use_old_macro_block_count),
      K_(row_count),
      K_(rowkey_column_cnt),
      K_(column_cnt),
      K_(column_checksums),
      K_(data_checksum),
      K_(occupy_size),
      K_(original_size),
      K_(max_merged_trans_version),
      K_(ddl_scn),
      K_(contain_uncommitted_row),
      K_(is_meta_root),
      K_(compressor_type),
      K_(encrypt_id),
      K_(master_key_id),
      K_(recycle_version),
      K_(nested_offset),
      K_(nested_size),
      KPHEX_(encrypt_key, sizeof(encrypt_key_)));
private:
  static const int64_t DEFAULT_MACRO_BLOCK_CNT = 64;
public:
  ObITable::TableKey table_key_;
  int16_t sstable_logic_seq_;
  int64_t schema_version_;
  int64_t create_snapshot_version_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  bool is_ready_for_read_;
  share::schema::ObTableMode table_mode_;
  share::schema::ObIndexType index_type_;
  ObMetaDiskAddr root_block_addr_;
  blocksstable::ObMicroBlockData root_block_data_;
  common::ObRowStoreType root_row_store_type_;
  common::ObRowStoreType latest_row_store_type_;
  int16_t data_index_tree_height_;
  ObMetaDiskAddr data_block_macro_meta_addr_;
  blocksstable::ObMicroBlockData data_block_macro_meta_;
  int64_t index_blocks_cnt_;
  int64_t data_blocks_cnt_;
  int64_t micro_block_cnt_;
  int64_t use_old_macro_block_count_;
  int64_t row_count_;
  int64_t rowkey_column_cnt_;
  int64_t column_cnt_;
  common::ObSEArray<int64_t, common::OB_ROW_DEFAULT_COLUMNS_COUNT> column_checksums_;
  int64_t data_checksum_;
  int64_t occupy_size_;
  int64_t original_size_;
  int64_t max_merged_trans_version_;
  share::SCN ddl_scn_; // saved into sstable meta
  share::SCN filled_tx_scn_;
  bool contain_uncommitted_row_;
  bool is_meta_root_;
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  int64_t recycle_version_;
  int64_t nested_offset_;
  int64_t nested_size_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> data_block_ids_;
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> other_block_ids_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM
