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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_ROW_STRUCT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_ROW_STRUCT_H_

#include "storage/ob_i_store.h"
#include "ob_block_sstable_struct.h"
#include "ob_data_buffer.h"
#include "ob_macro_block.h"
#include "ob_datum_row.h"

namespace oceanbase
{
namespace common
{
class ObObj;
}
namespace storage
{
class ObStoreRow;
}
namespace blocksstable
{

struct ObIndexBlockRowDesc
{
  ObIndexBlockRowDesc();
  ObIndexBlockRowDesc(ObDataStoreDesc &data_store_desc);

  OB_INLINE bool is_valid() const
  {
    bool ret = true;
    if (OB_UNLIKELY((is_macro_node_ && macro_block_count_ != 1) /*data macro row*/
        || (is_data_block_ && micro_block_count_ != 1) /*any leaf row*/
        || (is_secondary_meta_ && macro_block_count_ != 0) /*sec meta leaf row*/
        || (nullptr == data_store_desc_))) {
      ret = false;
    }
    return ret;
  }

  // const ObSSTablePreAggreator *aggregator_;
  const ObDataStoreDesc *data_store_desc_;
  ObDatumRowkey row_key_;
  MacroBlockId macro_id_;
  int64_t block_offset_;
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  int64_t block_size_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_data_block_;
  bool is_secondary_meta_;
  bool is_macro_node_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool is_last_row_last_flag_;

  TO_STRING_KV(KP_(data_store_desc), K_(row_key), K_(macro_id),
      K_(block_offset), K_(row_count), K_(row_count_delta),
      K_(max_merged_trans_version), K_(block_size),
      K_(macro_block_count), K_(micro_block_count),
      K_(is_deleted), K_(contain_uncommitted_row), K_(is_data_block),
      K_(is_secondary_meta), K_(is_macro_node), K_(has_string_out_row), K_(has_lob_out_row),
      K_(is_last_row_last_flag));
};

struct ObIndexBlockRowHeader
{
  static const int64_t INDEX_BLOCK_HEADER_V1 = 1;
  static const int64_t DEFAULT_IDX_ROW_MACRO_IDX  = MacroBlockId::AUTONOMIC_BLOCK_INDEX;
  static MacroBlockId DEFAULT_IDX_ROW_MACRO_ID;

  ObIndexBlockRowHeader();

  void reset();
  OB_INLINE bool is_valid() const
  {
    bool aggregation_valid = (is_pre_aggregated() && is_major_node()) || !is_pre_aggregated();
    bool version_valid = INDEX_BLOCK_HEADER_V1 == version_;
    bool macro_id_valid =
        (macro_id_ == DEFAULT_IDX_ROW_MACRO_ID)
        || !is_data_block()
        || !is_data_index();
    return aggregation_valid && version_valid && macro_id_valid;
  }

  OB_INLINE uint64_t get_version() const { return version_; }
  OB_INLINE uint64_t get_block_offset() const { return block_offset_; }
  OB_INLINE uint64_t get_block_size() const { return block_size_; }
  OB_INLINE ObRowStoreType get_row_store_type() const
  {
    return static_cast<ObRowStoreType>(row_store_type_);
  }
  OB_INLINE ObCompressorType get_compressor_type() const
  {
    return static_cast<ObCompressorType>(compressor_type_);
  }
  OB_INLINE int64_t get_encrypt_id() const { return encrypt_id_; }
  OB_INLINE int64_t get_master_key_id() const { return master_key_id_; }
  OB_INLINE const char *get_encrypt_key() const { return encrypt_key_; }
  OB_INLINE uint64_t get_row_count() const { return row_count_; }
  OB_INLINE uint64_t get_schema_version() const { return schema_version_; }
  OB_INLINE const MacroBlockId &get_macro_id() const { return macro_id_; }
  OB_INLINE void fill_deserialize_meta(ObMicroBlockDesMeta &des_meta) const
  {
    des_meta.compressor_type_ = static_cast<common::ObCompressorType>(compressor_type_);
    des_meta.encrypt_id_ = encrypt_id_;
    des_meta.master_key_id_ = master_key_id_;
    des_meta.encrypt_key_ = encrypt_key_;
  }
  OB_INLINE bool is_data_block() const { return 1 == is_data_block_; }
  OB_INLINE bool is_leaf_block() const { return 1 == is_leaf_block_; }
  OB_INLINE bool is_major_node() const { return 1 == is_major_node_; }
  OB_INLINE bool is_pre_aggregated() const { return 1 == is_pre_aggregated_; }
  OB_INLINE bool contain_uncommitted_row() const { return 1 == contain_uncommitted_row_; }
  OB_INLINE bool is_deleted() const { return 1 == is_deleted_; }
  OB_INLINE bool is_macro_node() const { return 1 == is_macro_node_; }
  OB_INLINE bool is_data_index() const { return 1 == is_data_index_; }
  OB_INLINE bool has_string_out_row() const { return 1 == has_string_out_row_; }
  OB_INLINE bool has_lob_out_row() const { return 0 == all_lob_in_row_; }

  OB_INLINE void set_data_block() { is_data_block_ = 1; }
  OB_INLINE void set_leaf_block() { is_leaf_block_ = 1; }
  OB_INLINE void set_major_node() { is_major_node_ = 1; }
  OB_INLINE void set_pre_aggregated() { is_pre_aggregated_ = 1; }
  OB_INLINE void set_contain_uncommitted_row() { contain_uncommitted_row_ = 1; }
  OB_INLINE void set_deleted() { is_deleted_ = 1; }
  OB_INLINE void set_macro_node() { is_macro_node_ = 1; }

  int fill_micro_des_meta(const bool need_deep_copy_key, ObMicroBlockDesMeta &des_meta) const;

  union
  {
    uint64_t pack_;
    struct
    {
      uint64_t version_:8;                  // Version number of index block row header
      uint64_t row_store_type_:8;           // Row store type of next level micro block
      uint64_t compressor_type_:8;          // Compressor type for next micro block
      uint64_t is_data_index_:1;       // Whether this tree is built for data index
      uint64_t is_data_block_:1;            // Whether current row point to a data block directly
      uint64_t is_leaf_block_:1;             // Whether current row point to a leaf index block
      uint64_t is_major_node_:1;            // Whether this tree is located in a major sstable
      uint64_t is_pre_aggregated_:1;        // Whether data in children of this row were pre-aggregated
      uint64_t is_deleted_:1;               // Whether the microblock was pointed was deleted
      uint64_t contain_uncommitted_row_:1;  // Whether children of this row contains uncommitted row
      uint64_t is_macro_node_:1;            // Whether this row represent for macro block level meta
      uint64_t has_string_out_row_ : 1;     // Whether sub-tree of this node has string column out row as lob
      uint64_t all_lob_in_row_ : 1;         // Whether sub-tree of this node has out row lob column
      uint64_t reserved_:30;
    };
  };
  MacroBlockId macro_id_;                   // Physical macro block id, set to default in leaf node
  int32_t block_offset_;                    // Offset of micro block in macro block
  int32_t block_size_;                      // Length of micro block data
  int64_t master_key_id_;                   // Master key id for encryption
  int64_t encrypt_id_;                      // Encryption id
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH]; // Encrypt key 16 bytes
  uint64_t row_count_;                      // Row count of the blocks this row points to
  uint64_t schema_version_;                 // Schema version of the data block
  // TODO: fill block count correctly
  uint64_t macro_block_count_;              // Macro block count this index row covered
  uint64_t micro_block_count_;              // Micro block count this index row covered

  TO_STRING_KV(
      K_(version), K_(row_store_type), K_(compressor_type),
      K_(is_data_index), K_(is_data_block),K_(is_leaf_block),
      K_(is_major_node), K_(is_pre_aggregated),K_(is_deleted), K_(contain_uncommitted_row),
      K_(is_macro_node), K_(has_string_out_row), K_(all_lob_in_row), K_(macro_id), K_(block_offset), K_(block_size),
      K_(master_key_id), K_(encrypt_id), KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(row_count), K_(schema_version), K_(macro_block_count), K_(micro_block_count));
};

struct ObIndexBlockRowMinorMetaInfo
{
  void reset() { MEMSET(this, 0, sizeof(*this)); }
  int64_t snapshot_version_;               // Snapshow version for minor sstable
  int64_t max_merged_trans_version_;       // Max transaction version in blocks
  int64_t row_count_delta_;                // Delta row count to data baseline
  TO_STRING_KV(K_(snapshot_version), K_(max_merged_trans_version), K_(row_count_delta));
};

struct ObMicroIndexInfo
{
public:
  ObMicroIndexInfo()
    : row_header_(nullptr),
      minor_meta_info_(nullptr),
      endkey_(nullptr),
      query_range_(nullptr),
      flag_(0),
      range_idx_(-1),
      parent_macro_id_(),
      nested_offset_(0)
  {
  }
  OB_INLINE void reset()
  {
    row_header_ = nullptr;
    minor_meta_info_ = nullptr;
    endkey_ = nullptr;
    query_range_ = nullptr;
    flag_ = 0;
    range_idx_ = -1;
    parent_macro_id_.reset();
    nested_offset_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    bool bret = false;
    const bool row_header_valid = nullptr != row_header_ && row_header_->is_valid();
    if (row_header_valid) {
      const bool minor_meta_info_valid =
          !row_header_->is_data_index()
          || row_header_->is_major_node()
          || nullptr != minor_meta_info_;
      const bool parent_macro_id_valid = !row_header_->is_data_block() || parent_macro_id_.is_valid();
      bret = minor_meta_info_valid && parent_macro_id_valid && nullptr != endkey_;
    }
    return bret;
  }
  OB_INLINE bool is_macro_node() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_macro_node();
  }
  OB_INLINE bool is_leaf_block() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_leaf_block();
  }
  OB_INLINE bool is_data_block() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_data_block();
  }
  OB_INLINE const MacroBlockId &get_macro_id()
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_data_block() ? parent_macro_id_ : row_header_->get_macro_id();
  }
  OB_INLINE uint64_t get_block_offset() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->get_block_offset() + nested_offset_;
  }
  OB_INLINE uint64_t get_block_size() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->get_block_size();
  }
  OB_INLINE ObRowStoreType get_row_store_type() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->get_row_store_type();
  }
  OB_INLINE uint64_t get_row_count() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->get_row_count();
  }
  OB_INLINE uint64_t get_row_count_delta() const
  {
    return OB_NOT_NULL(minor_meta_info_) ? minor_meta_info_->row_count_delta_ : 0;
  }
  OB_INLINE int64_t get_max_merged_trans_version() const
  {
    return OB_NOT_NULL(minor_meta_info_) ? minor_meta_info_->max_merged_trans_version_ : 0;
  }
  OB_INLINE int64_t get_snapshot_version() const
  {
    return OB_NOT_NULL(minor_meta_info_) ? minor_meta_info_->snapshot_version_ : 0;
  }
  OB_INLINE bool contain_uncommitted_row() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->contain_uncommitted_row();
  }
  OB_INLINE bool is_deleted() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_deleted();
  }
  OB_INLINE bool has_string_out_row() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->has_string_out_row();
  }
  OB_INLINE bool has_lob_out_row() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->has_lob_out_row();
  }
  OB_INLINE bool is_left_border() const
  {
    return is_left_border_;
  }
  OB_INLINE bool is_right_border() const
  {
    return is_right_border_;
  }
  OB_INLINE bool is_get() const
  {
    return 0 != is_get_;
  }
  OB_INLINE int64_t range_idx() const
  {
    return range_idx_;
  }
  OB_INLINE const ObDatumRowkey &get_query_key() const
  {
    OB_ASSERT(nullptr != rowkey_);
    return *rowkey_;
  }
  OB_INLINE const ObDatumRange &get_query_range() const
  {
    OB_ASSERT(nullptr != range_);
    return *range_;
  }
  OB_INLINE const MacroBlockId &get_macro_id() const
  {
    OB_ASSERT(nullptr != row_header_);
    return row_header_->is_data_block() ? parent_macro_id_ : row_header_->get_macro_id();
  }
  OB_INLINE bool can_blockscan(const bool has_lob_out) const
  {
    return can_blockscan_ && !has_string_out_row() && (!has_lob_out || !has_lob_out_row());
  }
  OB_INLINE void set_blockscan()
  {
    can_blockscan_ = true;
  }
  OB_INLINE bool is_filter_applied() const
  {
    return is_filter_applied_;
  }
  OB_INLINE void set_filter_applied()
  {
    is_filter_applied_ = true;
  }
  OB_INLINE bool can_be_aggregated()
  {
    return is_filter_applied_ && !is_left_border_ && !is_right_border_;
  }

  TO_STRING_KV(KP_(query_range), KPC_(row_header), KPC_(minor_meta_info), KPC_(endkey),
      K_(flag), K_(range_idx), K_(parent_macro_id), K_(nested_offset));

public:
  const ObIndexBlockRowHeader *row_header_;
  const ObIndexBlockRowMinorMetaInfo *minor_meta_info_;
  const ObDatumRowkey *endkey_;
  // ObPreAggDataStore *pre_agg_data_;
  union {
    const ObDatumRowkey *rowkey_;
    const ObDatumRange *range_;
    const void *query_range_;
  };
  union {
    uint16_t flag_;
    struct {
      uint16_t is_get_ : 1;
      uint16_t is_left_border_ : 1;
      uint16_t is_right_border_ : 1;
      uint16_t can_blockscan_ : 1;
      uint16_t is_filter_applied_ : 1;
      uint16_t reserved_ : 11;
    };
  };
  int64_t range_idx_;
  MacroBlockId parent_macro_id_;
  int64_t nested_offset_;
};


class ObIndexBlockRowBuilder
{
public:
  ObIndexBlockRowBuilder();
  virtual ~ObIndexBlockRowBuilder();
  // Don't need to manually free the memory since we use ObArenaAllocator
  void reuse();
  void reset();

  int init(const ObDataStoreDesc &desc);
  int build_row(const ObIndexBlockRowDesc &desc, const ObDatumRow *&row);
  int build_row(const ObMicroIndexInfo &micro_idx_info, const ObDatumRow *&row);
private:
  int set_rowkey(const ObIndexBlockRowDesc &desc);
  int set_rowkey(const ObDatumRowkey &rowkey);
  int append_header_and_meta(const ObIndexBlockRowDesc &desc);
  int append_aggregate_data(const ObIndexBlockRowDesc &desc);
  static int calc_data_size(const ObIndexBlockRowDesc &desc, int64_t &size);
  int calc_data_size(const ObIndexBlockRowHeader &idx_row_header, int64_t &size);

private:
  // Memory of row.cells_ and rowkey_column_types_ should be allocated from an Arena allocator
  // whose life-cycle is longer than this class.
  // Allocate memory for row data
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator index_data_allocator_;
  ObDatumRow row_;
  int64_t rowkey_column_count_;
  ObObjMeta *rowkey_column_types_;
  char *data_buf_;
  int64_t write_pos_;
  ObIndexBlockRowHeader *header_;
  bool is_inited_;
};

class ObIndexBlockRowParser
{
public:
  ObIndexBlockRowParser();
  virtual ~ObIndexBlockRowParser() {}

  // Double init is available
  int init(const int64_t rowkey_column_count, const ObDatumRow &index_row);
  int init(const char *data_buf);
  int get_header(const ObIndexBlockRowHeader *&header) const;
  int get_minor_meta(const ObIndexBlockRowMinorMetaInfo *&meta) const;
  int is_macro_node(bool &is_macro_node) const;
  int64_t get_snapshot_version() const;
  int64_t get_max_merged_trans_version() const;
  int64_t get_row_count_delta() const;
  TO_STRING_KV(K_(is_inited), KPC(header_));

private:
  const ObIndexBlockRowHeader *header_;
  const ObIndexBlockRowMinorMetaInfo *minor_meta_info_;
  // Aggregate data read struct
  bool is_inited_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_BLOCK_ROW_STRUCT_H_
