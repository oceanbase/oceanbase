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
               K_(compressor_type), K_(is_heap_table), K_(mem_chunk_size), K_(max_mem_chunk_count),
               K_(merge_count_per_round), K_(heap_table_mem_chunk_size), K_(session_count), K_(exe_mode));
public:
  int64_t rowkey_column_num_;
  int64_t column_count_;
  int64_t external_data_block_size_;
  int64_t sstable_index_block_size_;
  int64_t sstable_data_block_size_;
  int64_t extra_buf_size_;
  common::ObCompressorType compressor_type_;
  bool is_heap_table_;
  // sort param
  int64_t mem_chunk_size_;
  int64_t max_mem_chunk_count_;
  int64_t merge_count_per_round_;

  //heap sort param
  int64_t heap_table_mem_chunk_size_;
  int32_t session_count_;
  observer::ObTableLoadExeMode exe_mode_;
};

} // namespace storage
} // namespace oceanbase
