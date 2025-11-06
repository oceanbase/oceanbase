/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SIMPLIFIED_SSTABLE_MACRO_BLOCK_HEADER_H_
#define OB_SIMPLIFIED_SSTABLE_MACRO_BLOCK_HEADER_H_

#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "common/ob_store_format.h"

namespace oceanbase
{
namespace blocksstable
{
// simplified macro block store struct
//  |- ObSimplifiedSSTableMacroBlockHeader
//  |- MicroBlock 1
//  |- MicroBlock 2
//  |- MicroBlock N
//  |- IndexMicroBlock

class ObSimplifiedSSTableMacroBlockHeader final
{
public:
  ObSimplifiedSSTableMacroBlockHeader();
  ~ObSimplifiedSSTableMacroBlockHeader();
  void reset();
  int init(const ObSSTableMacroBlockHeader &macro_header, const int64_t macro_header_pos);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  OB_INLINE int64_t get_serialize_size() const { return sizeof(ObSimplifiedSSTableMacroBlockHeader); }
  bool is_valid() const;
  TO_STRING_KV(K_(idx_block_offset), K_(idx_block_size), K_(first_data_micro_block_offset),
        K_(rowkey_column_count), K_(micro_block_count), K_(row_store_type),
        K_(compressor_type), K_(encrypt_id), K_(master_key_id),
        KPHEX_(encrypt_key, sizeof(encrypt_key_)));
public:
  static int simplify_macro_block(char *macro_block_buf,
                                  const int64_t macro_block_buf_size,
                                  const char *&simplified_buffer,
                                  int64_t &simplified_buffer_size);
public:
  int32_t idx_block_offset_;
  int32_t idx_block_size_;
  int32_t first_data_micro_block_offset_;
  int32_t rowkey_column_count_;
  int32_t micro_block_count_;
  common::ObRowStoreType row_store_type_;
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  bool is_inited_;
};

}//end namespace blocksstable
}//end namespace oceanbase

#endif /* OB_SIMPLIFIED_SSTABLE_MACRO_BLOCK_HEADER_H_ */