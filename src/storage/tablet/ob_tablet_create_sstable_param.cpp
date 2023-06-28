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

#include "storage/tablet/ob_tablet_create_sstable_param.h"

#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
// if you add membership for this param, plz check all paths that use it, including
// but not limited to unittest, merge, ddl, shared_macro_block
ObTabletCreateSSTableParam::ObTabletCreateSSTableParam()
  : table_key_(),
    sstable_logic_seq_(0),
    schema_version_(-1),
    create_snapshot_version_(0),
    progressive_merge_round_(0),
    progressive_merge_step_(0),
    is_ready_for_read_(true),
    table_mode_(),
    index_type_(share::schema::ObIndexType::INDEX_TYPE_MAX),
    root_block_addr_(),
    root_block_data_(),
    root_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    latest_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    data_index_tree_height_(0),
    data_block_macro_meta_addr_(),
    data_block_macro_meta_(),
    index_blocks_cnt_(0),
    data_blocks_cnt_(0),
    micro_block_cnt_(0),
    use_old_macro_block_count_(0),
    row_count_(0),
    rowkey_column_cnt_(0),
    column_cnt_(0),
    data_checksum_(0),
    occupy_size_(0),
    original_size_(0),
    max_merged_trans_version_(0),
    ddl_scn_(SCN::min_scn()),
    filled_tx_scn_(SCN::min_scn()),
    contain_uncommitted_row_(false),
    is_meta_root_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    encrypt_id_(0),
    master_key_id_(0),
    recycle_version_(0),
    nested_offset_(0),
    nested_size_(0),
    data_block_ids_(),
    other_block_ids_()
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

bool ObTabletCreateSSTableParam::is_valid() const
{
  bool ret = true;
  if (OB_UNLIKELY(!table_key_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table key", K(table_key_));
  } else if (OB_UNLIKELY(!table_mode_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table mode", K(table_mode_));
  } else if (!(schema_version_ >= 0
               && sstable_logic_seq_ >= 0
               && create_snapshot_version_ >= 0
               && index_type_ < share::schema::ObIndexType::INDEX_TYPE_MAX
               && root_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
               && (latest_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
                  || ObRowStoreType::DUMMY_ROW_STORE == latest_row_store_type_)
               && data_index_tree_height_ >= 0
               && index_blocks_cnt_ >= 0
               && data_blocks_cnt_ >= 0
               && micro_block_cnt_ >= 0
               && use_old_macro_block_count_ >= 0
               && row_count_ >= 0
               && rowkey_column_cnt_ >= 0
               && column_cnt_ >= 0
               && occupy_size_ >= 0
               && ddl_scn_.is_valid()
               && filled_tx_scn_.is_valid()
               && original_size_ >= 0
               && recycle_version_ >= 0)) {
    ret = false;
    LOG_WARN("invalid basic params", K(schema_version_), K_(sstable_logic_seq), K(create_snapshot_version_), K(index_type_),
             K(root_row_store_type_), K_(latest_row_store_type), K(data_index_tree_height_), K(index_blocks_cnt_),
             K(data_blocks_cnt_), K(micro_block_cnt_), K(use_old_macro_block_count_),
             K(row_count_), K(rowkey_column_cnt_), K(column_cnt_), K(occupy_size_),
             K(ddl_scn_), K(filled_tx_scn_), K(original_size_), K_(recycle_version));
  } else if (ObITable::is_ddl_sstable(table_key_.table_type_)) {
    // ddl sstable can have invalid meta addr, so skip following ifs
    if (!ddl_scn_.is_valid_and_not_min()) {
      ret = false;
      LOG_WARN("ddl log ts is invalid", K(ddl_scn_), K(table_key_));
    }
  } else if (!is_block_meta_valid(root_block_addr_, root_block_data_)) {
    ret = false;
    LOG_WARN("invalid root meta", K(root_block_addr_), K(root_block_data_));
  } else if (!is_block_meta_valid(data_block_macro_meta_addr_, data_block_macro_meta_)) {
    ret = false;
    LOG_WARN("invalid data meta", K(data_block_macro_meta_addr_), K(data_block_macro_meta_));
  }
  return ret;
}

bool ObTabletCreateSSTableParam::is_block_meta_valid(const storage::ObMetaDiskAddr &addr,
                                                     const blocksstable::ObMicroBlockData &data) const
{
  return addr.is_valid() && (!addr.is_memory() || (data.is_valid() && data.size_ == addr.size()));
}

} // namespace storage
} // namespace oceanbase
