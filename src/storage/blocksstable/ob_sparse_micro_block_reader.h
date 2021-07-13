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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_SPARSE_MICRO_BLOCK_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_SPARSE_MICRO_BLOCK_READER_H_
#include "ob_block_sstable_struct.h"
#include "ob_micro_block_reader.h"
#include "storage/ob_i_store.h"
#include "ob_row_reader.h"
#include "ob_column_map.h"
#include "lib/hash/ob_array_index_hash_set.h"

namespace oceanbase {
namespace common {
class ObStoreRowkey;
}
namespace blocksstable {

class ObSparseMicroBlockGetReader : public ObMicroBlockGetReader {
public:
  ObSparseMicroBlockGetReader();
  virtual ~ObSparseMicroBlockGetReader();
  virtual int get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data, const common::ObStoreRowkey& rowkey,
      const ObColumnMap& column_map, const ObFullMacroBlockMeta& macro_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRow& row) override;
  virtual int get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data, const common::ObStoreRowkey& rowkey,
      const ObFullMacroBlockMeta& macro_meta, const storage::ObSSTableRowkeyHelper* rowkey_helper,
      storage::ObStoreRow& row) override;
  virtual int exist_row(const uint64_t tenant_id, const ObMicroBlockData& block_data,
      const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, bool& exist, bool& found) override;
  virtual int check_row_locked(memtable::ObIMvccCtx& ctx, const transaction::ObTransStateTableGuard& trans_table_guard,
      const transaction::ObTransID& read_trans_id, const ObMicroBlockData& block_data,
      const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRowLockState& lock_state) override;

protected:
  virtual int locate_row(const common::ObStoreRowkey& rowkey, const storage::ObSSTableRowkeyHelper* rowkey_helper,
      const common::ObObjMeta* cols_type, const char*& row_buf, int64_t& row_len) override;
};

class ObSparseMicroBlockReader : public ObIMicroBlockReader {
public:
  ObSparseMicroBlockReader();
  virtual ~ObSparseMicroBlockReader();
  virtual int init(const ObMicroBlockData& block_data, const ObColumnMap* column_map,
      const common::ObRowStoreType out_type = common::FLAT_ROW_STORE) override;
  virtual void reset() override;
  virtual int get_row(const int64_t index, storage::ObStoreRow& row) override;
  virtual int get_rows(const int64_t begin_index, const int64_t end_index, const int64_t row_capacity,
      storage::ObStoreRow* rows, int64_t& row_count) override;
  virtual int get_row_count(int64_t& row_count) override;
  virtual int get_row_header(const int64_t row_idx, const ObRowHeader*& row_header) override;
  virtual int get_multi_version_info(const int64_t row_idx, const int64_t version_column_idx,
      const int64_t sql_sequence_idx, storage::ObMultiVersionRowFlag& flag, transaction::ObTransID& trans_id,
      int64_t& version, int64_t& sql_sequence) override;

protected:
  int base_init(const ObMicroBlockData& block_data);
  virtual int find_bound(const common::ObStoreRowkey& key, const bool lower_bound, const int64_t begin_idx,
      const int64_t end_idx, int64_t& row_idx, bool& equal) override;

private:
  int get_row_impl(const int64_t index, storage::ObStoreRow& row);

protected:
  const ObMicroBlockHeader* header_;
  const char* data_begin_;  // start position of micro block data
  const char* data_end_;    // end position of micro block data
  const int32_t* index_data_;
  common::ObArenaAllocator allocator_;
  ObSparseRowReader sparse_row_reader_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
