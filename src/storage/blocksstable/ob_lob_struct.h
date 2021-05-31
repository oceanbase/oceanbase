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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_STRUCT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array_wrap.h"
#include "ob_block_sstable_struct.h"

namespace oceanbase {
namespace blocksstable {
struct ObLobMacroBlockHeader : public ObSSTableMacroBlockHeader {
  int32_t micro_block_size_array_offset_;
  int32_t micro_block_size_array_size_;
  ObLobMacroBlockHeader()
      : ObSSTableMacroBlockHeader(), micro_block_size_array_offset_(0), micro_block_size_array_size_()
  {
    micro_block_size_array_offset_ = 0;
    micro_block_size_array_size_ = 0;
  }
  bool is_valid() const
  {
    return header_size_ > 0 && version_ >= LOB_MACRO_BLOCK_HEADER_VERSION_V1 &&
           LOB_MACRO_BLOCK_HEADER_MAGIC == magic_ && common::OB_INVALID_ID != table_id_ && data_version_ >= 0 &&
           column_count_ >= rowkey_column_count_ && rowkey_column_count_ > 0 && column_index_scale_ >= 0 &&
           row_store_type_ >= 0 && row_count_ > 0 && occupy_size_ > 0 && micro_block_count_ > 0 &&
           micro_block_size_ > 0 && micro_block_data_offset_ > 0 && micro_block_data_size_ > 0 &&
           micro_block_index_offset_ >= micro_block_data_offset_ && micro_block_index_size_ > 0 &&
           data_checksum_ >= 0 && partition_id_ >= -1 &&
           (ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_ ||
               0 == attr_ /*temporarily for 147 bug*/);
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(attr), K_(table_id), K_(data_version), K_(column_count),
      K_(rowkey_column_count), K_(column_index_scale), K_(row_store_type), K_(row_count), K_(occupy_size),
      K_(micro_block_count), K_(micro_block_size), K_(micro_block_data_offset), K_(micro_block_data_size),
      K_(micro_block_index_offset), K_(micro_block_index_size), K_(micro_block_endkey_offset),
      K_(micro_block_endkey_size), K_(data_checksum), K_(compressor_name), K_(data_seq),
      K_(partition_id),  // in 2.0 partition_id replace reserved_[1] in 14x
      K_(micro_block_size_array_offset), K_(micro_block_size_array_size));
};

struct ObLobMicroBlockHeader {
  TO_STRING_KV(K_(header_size), K_(version), K_(magic));
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOB_STRUCT_H_
