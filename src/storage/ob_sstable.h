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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_H_
#define OCEANBASE_STORAGE_OB_SSTABLE_H_

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
#include "ob_tenant_file_struct.h"
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
struct ObStoreRowLockState;
struct ObSSTableSplitCtx;
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
class ObIPartitionComponentFactory;
class ObSSTableScanEstimator;
class ObPartitionMetaRedoModule;
class ObSSTableV1;

class ObSSTable : public ObITable {
public:
  static const int64_t DEFAULT_MACRO_BLOCK_NUM = 4;
  static const int64_t DEFAULT_ALLOCATOR_BLOCK_SIZE = 512;
  static const int64_t DEFAULT_MACRO_BLOCK_ARRAY_PAGE_SIZE =
      DEFAULT_MACRO_BLOCK_NUM * sizeof(blocksstable::MacroBlockId);
  static const int64_t RESERVE_META_SIZE = 4 * 1024;
  static const int64_t RESERVE_MULTI_VERSION_COUNT = 4;
  typedef common::ObIArray<common::ObExtStoreRowkey> ExtStoreRowkeyArray;
  typedef common::ObIArray<share::schema::ObColDesc> ColumnArray;

  typedef common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_NUM> MacroBlockArray;
  typedef common::hash::ObCuckooHashMap<common::ObLogicMacroBlockId, blocksstable::MacroBlockId> LogicBlockIdMap;

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
  ObSSTable();
  virtual ~ObSSTable();

  virtual int init(const ObITable::TableKey& table_key);
  virtual int replay_set_table_key(const ObITable::TableKey& table_key);
  virtual int open(const ObCreateSSTableParamWithTable& param);
  virtual int open(const ObCreateSSTableParamWithPartition& param);
  virtual int open(const blocksstable::ObSSTableBaseMeta& meta);
  virtual int close();
  virtual void destroy() override;

  // if set success, src ObMacroBlocksWriteCtx will be clear
  // if set failed, dest sstable macro blocks will be clear
  virtual int append_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int append_lob_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int append_bf_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx);
  virtual int set_sstable(ObSSTable& src_sstable);
  void clear();

  OB_INLINE int set_storage_file_handle(const blocksstable::ObStorageFileHandle& file_handle)
  {
    return file_handle_.assign(file_handle);
  }
  virtual OB_INLINE int64_t get_timestamp() const override
  {
    return dump_memtable_timestamp_;
  }

  OB_INLINE const blocksstable::ObStorageFileHandle& get_storage_file_handle() const
  {
    return file_handle_;
  }
  OB_INLINE blocksstable::ObStorageFileHandle& get_storage_file_handle()
  {
    return file_handle_;
  }
  OB_INLINE void set_replay_module(storage::ObPartitionMetaRedoModule* pt_replay_module)
  {
    pt_replay_module_ = pt_replay_module;
  }
  int get_tenant_file_key(ObTenantFileKey& file_key);
  int get_macro_block_ctx(const blocksstable::MacroBlockId& macro_id, blocksstable::ObMacroBlockCtx& ctx);
  int get_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);
  int get_lob_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);
  int get_bf_macro_block_ctx(blocksstable::ObMacroBlockCtx& ctx);
  int get_combine_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx);

  bool is_valid() const;
  int dump2text(const char* dir_name, const share::schema::ObTableSchema& schema, const char* fname);

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
  int find_lob_macros(const ObExtStoreRange& range, ObIArray<blocksstable::ObMacroBlockInfoPair>& lob_macro_blocks);
  OB_INLINE const LogicBlockIdMap& get_logic_block_map() const
  {
    return logic_block_id_map_;
  }
  OB_INLINE bool has_lob_macro_blocks() const
  {
    return meta_.lob_macro_block_count_ > 0;
  }
  int check_collation_free_valid();
  int get_last_rowkey(common::ObStoreRowkey& rowkey, common::ObArenaAllocator& allocator);
  int get_all_macro_block_endkey(ObIArray<common::ObRowkey>& rowkey_array, common::ObIAllocator& allocator);
  int get_range(
      const int64_t idx, const int64_t concurrent_cnt, common::ObIAllocator& allocator, common::ObExtStoreRange& range);

  int get_concurrent_cnt(int64_t tablet_size, int64_t& concurrent_cnt);
  int query_range_to_macros(common::ObIAllocator& allocator, const common::ObIArray<common::ObStoreRange>& ranges,
      const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
      common::ObIArray<common::ObStoreRange>* splitted_ranges, common::ObIArray<int64_t>* split_index);
  int exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found) override;
  virtual int prefix_exist(storage::ObRowsInfo& rows_info, bool& may_exist) override;
  int exist(ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found) override;
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
  int get_macro_max_row_count(int64_t& count);
  virtual int get_multi_version_rowkey_type() const
  {
    return is_multi_version_minor_sstable() ? (int)(meta_.multi_version_rowkey_type_)
                                            : (int)ObMultiVersionRowkeyHelpper::MVRC_NONE;
  }
  virtual int64_t get_upper_trans_version() const override
  {
    return is_multi_version_minor_sstable() ? meta_.get_upper_trans_version() : get_snapshot_version();
  }
  virtual int64_t get_max_merged_trans_version() const override
  {
    return is_multi_version_minor_sstable() ? meta_.get_max_merged_trans_version() : get_snapshot_version();
  }
  int set_upper_trans_version(const int64_t upper_trans_version);
  bool contain_uncommitted_row() const
  {
    return meta_.contain_uncommitted_row_;
  }
  bool has_uncommitted_trans() const
  {
    return meta_.upper_trans_version_ == INT64_MAX;
  }
  int check_row_locked(const ObStoreCtx& ctx, const common::ObStoreRowkey& rowkey,
      const ObIArray<share::schema::ObColDesc>& columns, ObStoreRowLockState& lock_state);
  int get_meta(const blocksstable::MacroBlockId& block_id, blocksstable::ObFullMacroBlockMeta& macro_meta) const;
  int replay_macro_metas();
  int get_all_macro_info(common::ObIArray<blocksstable::ObMacroBlockInfoPair>& macro_infos);
  int convert_from_old_sstable(ObOldSSTable& src_sstable);
  bool has_compact_row() const
  {
    return meta_.has_compact_row_;
  }
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  INHERIT_TO_STRING_KV(
      "ObITable", ObITable, KP(this), K_(status), K_(meta), K_(file_handle), K_(dump_memtable_timestamp));

private:
  int read_second_index(const MacroBlockArray& indexes, MacroBlockArray& blocks, const int64_t block_cnt);
  int sort_ranges(
      const common::ObIArray<common::ObStoreRange>& ranges, common::ObIArray<common::ObStoreRange>& ordered_ranges);
  int remove_duplicate_ordered_block_id(const common::ObIArray<blocksstable::ObMacroBlockCtx>& origin_blocks,
      common::ObIArray<blocksstable::ObMacroBlockCtx>& blocks);
  int locate_lob_macro(const ObExtStoreRowkey& ext_rowkey, const bool upper_bound, MacroBlockArray::iterator& iter);
  int get_macro_block_ctx(const blocksstable::MacroBlockId& block_id, const blocksstable::ObStoreFileCtx& file_ctx,
      blocksstable::ObMacroBlockCtx& ctx);
  int append_macro_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx,
      common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIArray<int64_t>& block_id_in_files,
      blocksstable::ObStoreFileCtx& file_ctx);
  int add_macro_block_meta(
      const blocksstable::MacroBlockId& macro_block_id, const blocksstable::ObFullMacroBlockMeta& meta);
  int add_macro_block_meta(const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids,
      const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& macro_block_metas);
  int clean_lob_column_checksum();
  int accumulate_macro_column_checksum(const blocksstable::ObFullMacroBlockMeta& macro_meta);
  int accumulate_macro_column_checksum_impl(const blocksstable::ObFullMacroBlockMeta& macro_meta,
      common::ObIArray<blocksstable::ObSSTableColumnMeta>& column_metas);
  int build_exist_iterator(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
      const ObExtStoreRowkey& ext_rowkey, ObStoreRowIterator*& iter);
  int build_multi_exist_iterator(ObRowsInfo& rows_info, ObStoreRowIterator*& iter);
  int check_logical_data_version(const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas);
  int build_block_meta_map();
  int build_logic_block_id_map();
  int convert_array_to_meta_map(const common::ObIArray<blocksstable::MacroBlockId>& block_ids,
      const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas);
  int convert_array_to_id_map(const common::ObIArray<blocksstable::MacroBlockId>& block_ids,
      const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas);
  int replay_add_macro_block_meta(const blocksstable::MacroBlockId& macro_block_id, ObSSTable& other);
  int replay_add_macro_block_meta(
      const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, ObSSTable& other);
  int get_file_handle_from_replay_module(const common::ObPGKey& pg_key);
  int convert_add_macro_block_meta(const blocksstable::MacroBlockId& macro_block_id, common::ObIAllocator& allocator);
  int convert_add_macro_block_meta(
      const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIAllocator& allocator);
  int serialize_schema_map(char* buf, int64_t data_len, int64_t& pos) const;
  int deserialize_schema_map(const char* buf, int64_t data_len, int64_t& pos);
  int64_t get_schema_map_serialize_size() const;
  int add_macro_ref();

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
  static const int64_t DDL_VERSION_COUNT = 16;
  friend class ObMacroBlockIterator;
  common::ObArenaAllocator allocator_;
  common::ObLfFIFOAllocator map_allocator_;
  blocksstable::ObSSTableMeta meta_;
  ObSSTableRowkeyHelper rowkey_helper_;
  ObSSTableMergeInfo sstable_merge_info_;
  ObSSTableStatus status_;
  LogicBlockIdMap logic_block_id_map_;
  common::hash::ObCuckooHashMap<blocksstable::MacroBlockId, const blocksstable::ObMacroBlockMetaV2*> block_meta_map_;
  common::hash::ObCuckooHashMap<int64_t, const blocksstable::ObMacroBlockSchemaInfo*> schema_map_;
  ObSSTableGroupMacroBlocks total_macro_blocks_;
  bool exist_invalid_collation_free_meta_;
  ObArray<blocksstable::ObFullMacroBlockMeta> macro_meta_array_;
  ObArray<blocksstable::ObFullMacroBlockMeta> lob_macro_meta_array_;
  ObArray<blocksstable::ObFullMacroBlockMeta> bloomfilter_macro_meta_array_;
  blocksstable::ObStorageFileHandle file_handle_;
  storage::ObPartitionMetaRedoModule* pt_replay_module_;
  int64_t dump_memtable_timestamp_;  // timestamp of the lastest memtable which generate the sstable
  DISALLOW_COPY_AND_ASSIGN(ObSSTable);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif
