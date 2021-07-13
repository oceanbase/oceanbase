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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_V1_H_
#define OCEANBASE_STORAGE_OB_SSTABLE_V1_H_

#include "lib/container/ob_vector.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_dchash.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/object/ob_obj_compare.h"
#include "common/ob_zone.h"
#include "common/ob_range.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "blocksstable/ob_macro_block_reader.h"
#include "blocksstable/ob_macro_block_writer.h"
#include "blocksstable/ob_macro_block_meta_mgr.h"
#include "blocksstable/ob_store_file_system.h"
#include "ob_i_store.h"
#include "ob_i_table.h"
#include "ob_macro_block_iterator.h"
#include "ob_sstable_rowkey_helper.h"
#include "ob_sstable_struct.h"

namespace oceanbase {
namespace common {
class ObStoreRowkey;
class ObStoreRange;
}  // namespace common
namespace blocksstable {
class ObMacroBlockWriter;
class ObRowHeader;
}  // namespace blocksstable
namespace storage {
struct ObStoreRow;
class ObISSTableRowIterator;
class ObStoreRowSingleGetter;
class ObStoreRowMultiGetter;
class ObStoreRowMultiGetEstimator;
class ObStoreRowSingleScanner;
class ObStoreRowSingleScanEstimator;
class ObStoreRowMultiScanner;
class ObStoreRowMultiScanEstimator;
class ObStoreRowSingleExister;
class ObStoreRowScanner;
class ObIWarmUpRequest;
class ObSSTableScanEstimator;
class ObSSTableSplitCtx;

class ObOldSSTable : public ObITable {
public:
  static const int64_t DEFAULT_MACRO_BLOCK_NUM = 4;
  static const int64_t DEFAULT_ALLOCATOR_BLOCK_SIZE = 1024;
  static const int64_t RESERVE_META_SIZE = 4 * 1024;
  static const int64_t RESERVE_MULTI_VERSION_COUNT = 4;
  typedef common::ObIArray<common::ObExtStoreRowkey> ExtStoreRowkeyArray;
  typedef common::ObIArray<share::schema::ObColDesc> ColumnArray;

  typedef common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_NUM> MacroBlockArray;
  typedef common::hash::ObCuckooHashMap<common::ObLogicMacroBlockId, blocksstable::MacroBlockId> LobBlockIdMap;

public:
  class ObSSTableGroupMacroBlocks {
  public:
    ObSSTableGroupMacroBlocks(const MacroBlockArray& data_macro_blocks, const MacroBlockArray& lob_macro_blocks);
    ~ObSSTableGroupMacroBlocks();
    OB_INLINE int64_t data_macro_block_count() const
    {
      return data_macro_blocks_.count();
    }
    OB_INLINE int64_t lob_macro_block_count() const
    {
      return lob_macro_blocks_.count();
    }
    OB_INLINE int64_t count() const
    {
      return data_macro_block_count() + lob_macro_block_count();
    }
    const blocksstable::MacroBlockId& at(const int64_t idx) const;
    int at(const int64_t idx, blocksstable::MacroBlockId& block_id) const;
    virtual int64_t to_string(char* buf, int64_t buf_len) const;

  private:
    ObSSTableGroupMacroBlocks();
    DISALLOW_COPY_AND_ASSIGN(ObSSTableGroupMacroBlocks);

  private:
    const MacroBlockArray& data_macro_blocks_;
    const MacroBlockArray& lob_macro_blocks_;
  };

public:
  ObOldSSTable();
  virtual ~ObOldSSTable();

  virtual int init(const ObITable::TableKey& table_key);
  virtual int replay_set_table_key(const ObITable::TableKey& table_key);
  virtual int open(const ObCreateSSTableParam& param);
  virtual int open(const blocksstable::ObSSTableBaseMeta& meta);
  virtual int close();
  virtual void destroy();

  // if set success, src ObMacroBlocksWriteCtx will be clear
  // if set failed, dest sstable macro blocks will be clear
  virtual int append_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int append_lob_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int append_bf_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int set_sstable(ObOldSSTable& src_sstable);
  void clear();

  int get_macro_block_ctx(const blocksstable::MacroBlockId& macro_id, blocksstable::ObMacroBlockCtx& ctx);
  int get_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);
  int get_lob_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);
  int get_bf_macro_block_ctx(blocksstable::ObMacroBlockCtx& ctx);
  int get_combine_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);
  int get_macro_block_meta(const int64_t idx, const blocksstable::ObMacroBlockMeta*& macro_meta);

  bool is_valid() const;
  int dump2text(const share::schema::ObTableSchema& schema, const char* fname);

  virtual int scan_macro_block(ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan = false);
  virtual int scan_macro_block_totally(ObMacroBlockIterator& macro_block_iter);
  virtual int scan_macro_block(
      const common::ObExtStoreRange& range, ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan = false);

  virtual int scan(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObExtStoreRange& key_range, ObStoreRowIterator*& row_iter) override;

  virtual int get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRowkey& rowkey, ObStoreRowIterator*& row_iter) override;

  virtual int multi_get(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObStoreRowIterator*& row_iter) override;

  virtual int multi_scan(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRange>& ranges, ObStoreRowIterator*& row_iter) override;

  virtual int estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObPartitionEst& part_est) override;

  virtual int estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRange& key_range, ObPartitionEst& part_est) override;
  virtual int estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRange>& ranges, ObPartitionEst& part_est) override;

  virtual int set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& columns, ObStoreRowIterator& row_iter) override;

  inline void set_available_version(const int64_t version)
  {
    meta_.available_version_ = version;
  }
  inline int64_t get_available_version() const
  {
    return meta_.available_version_;
  }
  inline bool is_build_on_snapshot() const
  {
    return meta_.build_on_snapshot_;
  }
  inline void set_build_on_snapshot(const bool build_on_snapshot)
  {
    meta_.build_on_snapshot_ = build_on_snapshot;
  }
  inline int64_t get_create_index_base_version() const
  {
    return meta_.create_index_base_version_;
  }
  inline void set_create_index_base_version(const int64_t version)
  {
    meta_.create_index_base_version_ = version;
  }
  inline bool has_bloom_filter_macro_block() const
  {
    return meta_.bloom_filter_block_id_.is_valid();
  }
  inline const blocksstable::MacroBlockId& get_bloom_filter_block_id() const
  {
    return meta_.bloom_filter_block_id_;
  }
  inline virtual const blocksstable::ObSSTableMeta& get_meta() const
  {
    return meta_;
  }
  inline virtual const common::ObIArray<blocksstable::MacroBlockId>& get_meta_macro_blocks() const
  {
    return macro_block_second_indexes_;
  }
  inline virtual const common::ObIArray<blocksstable::MacroBlockId>& get_lob_meta_macro_blocks() const
  {
    return lob_macro_block_second_indexes_;
  }
  inline virtual const ObSSTableGroupMacroBlocks& get_total_meta_macro_blocks() const
  {
    return total_meta_macro_blocks_;
  }
  inline virtual const ObSSTableGroupMacroBlocks& get_total_macro_blocks() const
  {
    return total_macro_blocks_;
  }

  inline virtual const common::ObIArray<blocksstable::MacroBlockId>& get_macro_block_ids() const
  {
    return meta_.macro_block_array_;
  }
  inline virtual const common::ObIArray<blocksstable::MacroBlockId>& get_lob_macro_block_ids() const
  {
    return meta_.lob_macro_block_array_;
  }
  inline ObSSTableMergeInfo& get_sstable_merge_info()
  {
    return sstable_merge_info_;
  }
  inline const ObSSTableMergeInfo& get_sstable_merge_info() const
  {
    return sstable_merge_info_;
  }
  int set_column_checksum(const int64_t column_cnt, const int64_t* column_checksum);
  int set_column_checksum(const common::hash::ObHashMap<int64_t, int64_t>& column_checksum_map);

  int add_sstable_merge_info(const ObSSTableMergeInfo& sstable_merge_info);

  int find_macro(const common::ObExtStoreRowkey& rowkey, common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks);
  int find_macros(const ExtStoreRowkeyArray& rowkey, common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks);
  int find_macros(const common::ObExtStoreRange& range, common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks);

  int find_macros(const common::ObIArray<common::ObExtStoreRange>& ranges,
      common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks,
      common::ObIArray<int64_t>& end_block_idx_of_ranges);
  // lob macro blocks
  int find_lob_macros(const ObExtStoreRange& range, ObIArray<blocksstable::MacroBlockId>& lob_macro_blocks);
  int get_block_meta(const blocksstable::MacroBlockId& macro_id, const blocksstable::ObMacroBlockMeta*& macro_meta);
  int build_block_item_map();
  int build_lob_block_map();
  int check_logical_data_version();
  OB_INLINE const LobBlockIdMap& get_lob_block_map() const
  {
    return lob_block_id_map_;
  }
  OB_INLINE bool has_lob_macro_blocks() const
  {
    return meta_.lob_macro_block_count_ > 0;
  }
  int check_collation_free_valid();
  int get_last_rowkey(common::ObStoreRowkey& rowkey, common::ObArenaAllocator& allocator);
  int get_range(
      const int64_t idx, const int64_t concurrent_cnt, common::ObIAllocator& allocator, common::ObExtStoreRange& range);

  int get_concurrent_cnt(int64_t tablet_size, int64_t& concurrent_cnt);
  int fill_split_handles(ObSSTableSplitCtx& split_ctx, const int64_t merge_round);
  int query_range_to_macros(common::ObIAllocator& allocator, const common::ObIArray<common::ObStoreRange>& ranges,
      const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
      common::ObIArray<common::ObStoreRange>* splitted_ranges, common::ObIArray<int64_t>* split_index);
  int exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found);
  virtual int prefix_exist(storage::ObRowsInfo& rows_info, bool& may_exist);
  int exist(ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found);
  int64_t get_occupy_size() const
  {
    return meta_.occupy_size_;
  }
  int64_t get_total_row_count() const
  {
    return meta_.row_count_;
  }
  int64_t get_macro_block_count() const
  {
    return meta_.macro_block_count_;
  }
  inline uint64_t get_table_id() const
  {
    return meta_.index_id_;
  }
  inline int64_t get_rowkey_column_count() const
  {
    return meta_.rowkey_column_count_;
  }
  OB_INLINE int64_t get_logical_data_version() const
  {
    return meta_.logical_data_version_;
  }
  int get_table_stat(common::ObTableStat& stat);
  virtual int get_frozen_schema_version(int64_t& schema_version) const override;
  int fill_old_meta_info(
      const int64_t schema_version, const int64_t step_merge_start_version, const int64_t step_merge_end_version);
  int64_t get_micro_count_count() const
  {
    return micro_block_count_;
  }
  virtual int bf_may_contain_rowkey(const common::ObStoreRowkey& rowkey, bool& contain);
  // rowkey_helper
  OB_INLINE bool is_rowkey_helper_valid() const
  {
    return rowkey_helper_.is_valid();
  }
  OB_INLINE ObSSTableRowkeyHelper& get_rowkey_helper()
  {
    return rowkey_helper_;
  }
  int get_row_max_trans_version(const ObStoreRowkey& rowkey, int64_t& version);
  int get_macro_max_row_count(int64_t& count);
  virtual int get_multi_version_rowkey_type() const
  {
    return is_multi_version_minor_sstable() ? (int)(meta_.multi_version_rowkey_type_)
                                            : (int)ObMultiVersionRowkeyHelpper::MVRC_NONE;
  }
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K(status_), K(meta_));

private:
  int sort_ranges(
      const common::ObIArray<common::ObStoreRange>& ranges, common::ObIArray<common::ObStoreRange>& ordered_ranges);
  int remove_duplicate_ordered_block_id(const common::ObIArray<blocksstable::ObMacroBlockCtx>& origin_blocks,
      common::ObIArray<blocksstable::ObMacroBlockCtx>& blocks);
  int locate_lob_macro(const ObExtStoreRowkey& ext_rowkey, const bool upper_bound, MacroBlockArray::iterator& iter);
  int find_lob_split_point(const int64_t macro_split_point, int64_t& lob_split_point);
  int fill_split_handle(const ObCreateSSTableParam& split_table_param, const int64_t macro_split_point,
      const int64_t lob_split_point, ObTableHandle& split_table_handle);
  int get_macro_block_ctx(const int64_t idx, const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids,
      const common::ObIArray<int64_t>& block_id_in_files, const blocksstable::ObStoreFileCtx& file_ctx,
      blocksstable::ObMacroBlockCtx& ctx);
  int append_macro_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx,
      common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIArray<int64_t>& block_id_in_files,
      blocksstable::ObStoreFileCtx& file_ctx);
  int add_macro_block_meta(const blocksstable::MacroBlockId& macro_block_id);
  int add_macro_block_meta(const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids);
  int clean_lob_column_checksum();
  int accumulate_macro_column_checksum(const blocksstable::ObMacroBlockMeta& macro_meta);
  int accumulate_macro_column_checksum_impl(const blocksstable::ObMacroBlockMeta& macro_meta,
      common::ObIArray<blocksstable::ObSSTableColumnMeta>& column_metas);
  int build_exist_iterator(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
      const ObExtStoreRowkey& ext_rowkey, ObStoreRowIterator*& iter);
  int build_multi_exist_iterator(ObRowsInfo& rows_info, ObStoreRowIterator*& iter);

private:
  void set_multi_version_rowkey_type(const ObMultiVersionRowkeyHelpper::MultiVersionRowkeyType rowkey_type)
  {
    if (is_multi_version_table()) {
      meta_.multi_version_rowkey_type_ = rowkey_type;
    } else {
      meta_.multi_version_rowkey_type_ = ObMultiVersionRowkeyHelpper::MVRC_NONE;
    }
  }

private:
  friend class ObMacroBlockIterator;
  typedef common::ObFixedArray<blocksstable::ObMacroBlockMetaHandle*, common::ObIAllocator> MacroMetaArray;
  common::ObArenaAllocator allocator_;
  blocksstable::ObSSTableMeta meta_;
  MacroBlockArray macro_block_second_indexes_;
  MacroMetaArray macro_block_metas_;
  MacroBlockArray lob_macro_block_second_indexes_;
  MacroMetaArray lob_macro_block_metas_;
  ObSSTableRowkeyHelper rowkey_helper_;
  ObSSTableMergeInfo sstable_merge_info_;
  ObSSTableStatus status_;
  common::hash::ObCuckooHashMap<blocksstable::MacroBlockId, int64_t> block_id_map_;
  LobBlockIdMap lob_block_id_map_;
  ObSSTableGroupMacroBlocks total_macro_blocks_;
  ObSSTableGroupMacroBlocks total_meta_macro_blocks_;
  bool exist_invalid_collation_free_meta_;
  int64_t micro_block_count_;
  DISALLOW_COPY_AND_ASSIGN(ObOldSSTable);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif
