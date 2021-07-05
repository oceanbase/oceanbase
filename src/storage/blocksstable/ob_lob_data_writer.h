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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_WRITER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "ob_lob_struct.h"
#include "ob_macro_block.h"

namespace oceanbase {
namespace blocksstable {
class ObLobDataWriter;
class ObLobMicroBlockWriter {
public:
  ObLobMicroBlockWriter();
  virtual ~ObLobMicroBlockWriter() = default;
  int init(const int64_t macro_block_size);
  int write(const char* buf, const int64_t buf_len, const char*& out_buf, int64_t& out_buf_len);
  int64_t get_block_size() const
  {
    return data_buffer_.length();
  }
  void reset();
  void reuse();

private:
  int reserve_header();

private:
  bool is_inited_;
  ObLobMicroBlockHeader* header_;
  ObSelfBufferWriter data_buffer_;
};

struct ObLobMicroBlockDesc {
  TO_STRING_KV(K_(buf), K_(byte_size), K_(char_size), K_(old_micro_block), K_(column_id), K_(column_checksum));
  const char* buf_;
  int64_t buf_size_;
  int64_t byte_size_;
  int64_t char_size_;
  bool old_micro_block_;
  uint16_t column_id_;
  int64_t column_checksum_;
};

class ObLobMacroBlockWriter {
public:
  ObLobMacroBlockWriter();
  virtual ~ObLobMacroBlockWriter() = default;
  int init(const ObDataStoreDesc& store_desc, const int64_t start_seq, ObLobDataWriter* writer);
  int open(const int16_t lob_type, const common::ObStoreRowkey& rowkey, const common::ObIArray<uint16_t>& column_ids,
      const common::ObIArray<common::ObObjMeta>& column_types);
  int write_micro_block(ObLobMicroBlockDesc& block_desc);
  int close();
  void reset();

private:
  void reuse();
  int flush();
  int init_header(
      const common::ObIArray<uint16_t>& column_ids, const common::ObIArray<common::ObObjMeta>& column_types);
  int finish_header(const int64_t data_len, const int64_t index_len, const int64_t size_array_len);
  int check_need_switch(const int64_t size, bool& need_switch);
  int add_column_checksum(const uint16_t column_id, const int64_t column_checksum);
  int64_t get_remain_size() const
  {
    return data_.remain() - index_.get_block_size();
  }
  int64_t get_data_size() const
  {
    return data_.length() + index_.get_block_size();
  }
  int build_macro_meta(ObFullMacroBlockMeta& meta);

private:
  bool is_inited_;
  ObSelfBufferWriter data_;
  ObLobMicroBlockIndexWriter index_;
  ObLobMacroBlockHeader* header_;
  ObMacroBlockCommonHeader common_header_;
  ObLobDataWriter* writer_;
  uint16_t* column_ids_;
  common::ObObjMeta* column_types_;
  common::ObOrderType* column_orders_;
  int64_t* column_checksum_;
  int64_t data_base_offset_;
  bool is_dirty_;
  ObLobMicroBlockWriter micro_block_writer_;
  ObMicroBlockCompressor compressor_;
  int64_t byte_size_;
  int64_t char_size_;
  ObDataStoreDesc desc_;
  common::ObStoreRowkey rowkey_;
  int64_t current_seq_;
  int64_t use_old_micro_block_cnt_;
  common::ObArray<uint16_t> column_id_array_;
  common::ObArray<common::ObObjMeta> column_type_array_;
  int16_t lob_type_;
  int64_t lob_data_version_;
  ObMacroBlocksWriteCtx block_write_ctx_;
};

class ObLobDataWriter {
public:
  ObLobDataWriter();
  virtual ~ObLobDataWriter();
  int init(const ObDataStoreDesc& desc, const int64_t start_seq);
  int write_lob_data(const common::ObStoreRowkey& rowkey, const uint16_t column_id, const common::ObObj& src,
      common::ObObj& dst, common::ObIArray<ObMacroBlockInfoPair>& macro_blocks);
  int add_macro_block(const common::ObLobIndex& index, const ObMacroBlockInfoPair& block_info);
  void reset();
  void reuse();

private:
  void clear_macro_block_ref();
  int compress_micro_block(const char*& buf, int64_t& size);
  int write_lob_index(common::ObLobData& lob_data);
  int write_lob_index_impl(const int64_t start_idx, const int64_t end_idx, int64_t& add_count);
  int get_physical_macro_blocks(common::ObIArray<blocksstable::ObMacroBlockInfoPair>& macro_blocks);
  void inner_reuse();
  int fill_micro_block_desc(const ObSelfBufferWriter& data, const int64_t byte_size, const int64_t char_size,
      ObLobMicroBlockDesc& block_desc);
  int check_rowkey(const common::ObStoreRowkey& rowkey, bool& check_ret) const;

private:
  static const int64_t DEFAULT_RESERVE_PERCENT = 90;
  bool is_inited_;
  common::ObArray<ObMacroBlockInfoPair> macro_blocks_;
  common::ObArray<common::ObLobIndex> lob_indexes_;
  ObLobMacroBlockWriter writer_;
  ObSelfBufferWriter lob_index_buffer_;
  int64_t rowkey_column_cnt_;
  int64_t micro_block_size_;
  uint16_t column_id_;
  common::ObArenaAllocator allocator_;
  common::ObArray<uint16_t> column_ids_;
  common::ObArray<common::ObObjMeta> column_types_;
  common::ObStoreRowkey rowkey_;
  ObStorageFileHandle file_handle_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_DATA_WRITER_H_
