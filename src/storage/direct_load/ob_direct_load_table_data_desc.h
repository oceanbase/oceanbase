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
#pragma once

#include "lib/compress/ob_compressor.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadRowFlag
{
public:
  ObDirectLoadRowFlag() : flag_(0) {}
  void reset() { flag_ = 0; }
  OB_INLINE int64_t get_column_count(const int64_t column_count) const
  {
    return uncontain_hidden_pk_ ? column_count + 1 : column_count;
  }
  TO_STRING_KV(K_(uncontain_hidden_pk), K_(has_delete_row), K_(lob_id_only));
public:
  union
  {
    struct
    {
      bool uncontain_hidden_pk_ : 1; // 无主键表且数据不包含隐藏主键
      bool has_delete_row_ : 1; // TODO: 后续维护这个flag
      bool lob_id_only_; // 数据中只有lob_id
      int64_t reserved_ : 61;
    };
    int64_t flag_;
  };
};

struct ObDirectLoadTableDataDesc
{
  static const int64_t DEFAULT_EXTRA_BUF_SIZE = (2LL << 20); // 2M
public:
  ObDirectLoadTableDataDesc();
  ~ObDirectLoadTableDataDesc();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(rowkey_column_num), K_(column_count), K_(external_data_block_size),
               K_(sstable_index_block_size), K_(sstable_data_block_size), K_(extra_buf_size),
               K_(compressor_type), K_(is_shared_storage), K_(row_flag));
public:
  int64_t rowkey_column_num_;
  int64_t column_count_;
  int64_t external_data_block_size_;
  int64_t sstable_index_block_size_;
  int64_t sstable_data_block_size_;
  int64_t extra_buf_size_;
  common::ObCompressorType compressor_type_;
  bool is_shared_storage_; // 共享存储模式, multiple sstable会额外写一份rowkey数据
  ObDirectLoadRowFlag row_flag_;
};

} // namespace storage
} // namespace oceanbase
