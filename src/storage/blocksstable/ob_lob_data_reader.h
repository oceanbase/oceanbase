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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_READER_H_

#include "lib/hash/ob_cuckoo_hashmap.h"
#include "lib/io/ob_io_manager.h"
#include "ob_lob_struct.h"
#include "ob_macro_block_reader.h"
#include "ob_lob_micro_block_index_reader.h"
#include "ob_macro_block_meta_mgr.h"
#include "ob_store_file.h"
#include "storage/ob_pg_mgr.h"

namespace oceanbase {
namespace storage {
class ObSSTable;
}
namespace blocksstable {
class ObLobMicroBlockReader {
public:
  ObLobMicroBlockReader();
  virtual ~ObLobMicroBlockReader() = default;
  int read(const char* buf, const int64_t buf_len, const char*& out_buf, int64_t& out_buf_len);

private:
  int check_micro_header(const ObLobMicroBlockHeader* header);
};

class ObLobMacroBlockReader {
public:
  ObLobMacroBlockReader();
  virtual ~ObLobMacroBlockReader() = default;
  int init();
  int open(const common::ObStoreRowkey& rowkey, const uint16_t column_id, const MacroBlockId& block_id,
      const bool is_sys_read, const ObFullMacroBlockMeta& meta, const common::ObPartitionKey& pkey,
      ObStorageFileHandle& file_handle);
  int read_next_micro_block(const char*& out_buf, int64_t& out_buf_len);
  void reset();

private:
  int open(const MacroBlockId& block_id, const bool is_sys_read, const ObFullMacroBlockMeta& meta,
      const common::ObPartitionKey& pkey, ObStorageFileHandle& file_handle);
  int check_macro_meta(const common::ObStoreRowkey& rowkey, const uint16_t column_id);

private:
  bool is_inited_;
  MacroBlockId curr_block_id_;
  ObLobMicroBlockIndexReader micro_index_reader_;
  ObLobMicroBlockReader micro_block_reader_;
  ObMacroBlockReader macro_reader_;
  ObMacroBlockHandle macro_handle_;
  const char* data_buf_;  // macro block data buffer
  ObFullMacroBlockMeta full_meta_;
};

class ObLobDataReader {
public:
  ObLobDataReader();
  virtual ~ObLobDataReader();
  int init(const bool is_sys_read, const storage::ObSSTable& sstable);
  // for temporary lob
  int read_lob_data(const common::ObObj& src_obj, common::ObObj& dst_obj);
  int read_lob_data(const common::ObStoreRowkey& rowkey, const uint16_t column_id, const common::ObObj& src_obj,
      common::ObObj& dst_obj);
  void reuse();
  void reset();
  OB_INLINE bool is_sys_read()
  {
    return is_sys_read_;
  }

private:
  void inner_reuse();
  int read_lob_data_impl(const common::ObStoreRowkey& rowkey, const uint16_t column_id, const common::ObObj& src_obj,
      common::ObObj& dst_obj);
  int read_data_macro(const common::ObLobIndex& lob_index);
  int read_index_macro(const MacroBlockId& block_id, common::ObIArray<common::ObLobIndex>& lob_indexes);
  int parse_lob_index(const char* buf, const int64_t buf_len, common::ObIArray<common::ObLobIndex>& lob_indexes);
  int read_all_direct_index(const common::ObLobIndex& index, common::ObIArray<common::ObLobIndex>& lob_indexes);

private:
  bool is_inited_;
  ObLobMacroBlockReader reader_;
  const common::hash::ObCuckooHashMap<common::ObLogicMacroBlockId, MacroBlockId>* logic2phy_map_;
  const storage::ObSSTable* sstable_;
  common::ObArenaAllocator allocator_;
  char* buf_;
  int64_t pos_;
  int64_t byte_size_;
  bool is_sys_read_;
  common::ObStoreRowkey rowkey_;
  uint16_t column_id_;
  ObStorageFileHandle file_handle_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_READER_H_
