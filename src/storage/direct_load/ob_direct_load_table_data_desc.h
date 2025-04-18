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

struct ObDirectLoadSampleMode
{
#define OB_DIRECT_LOAD_SAMPLE_MODE_DEF(DEF) \
  DEF(NO_SAMPLE, = 0)                       \
  DEF(DATA_BLOCK_SAMPLE, = 1)               \
  DEF(ROW_SAMPLE, = 2)                      \
  DEF(MAX_SAMPLE_MODE, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_SAMPLE_MODE_DEF, static);

  static bool is_type_valid(const Type type) { return type >= NO_SAMPLE && type < MAX_SAMPLE_MODE; }
  static bool is_sample_enabled(const Type type)
  {
    return type > NO_SAMPLE && type < MAX_SAMPLE_MODE;
  }
  static bool is_data_block_sample(const Type type) { return DATA_BLOCK_SAMPLE == type; }
  static bool is_row_sample(const Type type) { return ROW_SAMPLE == type; }
};

struct ObDirectLoadTableDataDesc
{
  static const int64_t DEFAULT_EXTRA_BUF_SIZE = (2LL << 20); // 2M
  static const int64_t DEFAULT_ROWS_PER_SAMPLE = 10000; // 1w行采样1个
public:
  ObDirectLoadTableDataDesc();
  ~ObDirectLoadTableDataDesc();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(rowkey_column_num), K_(column_count), K_(external_data_block_size),
               K_(sstable_index_block_size), K_(sstable_data_block_size), K_(extra_buf_size),
               K_(compressor_type), K_(row_flag),
               "sample_mode", ObDirectLoadSampleMode::get_type_string(sample_mode_), K_(num_per_sample));
public:
  int64_t rowkey_column_num_;
  int64_t column_count_;
  int64_t external_data_block_size_;
  int64_t sstable_index_block_size_;
  int64_t sstable_data_block_size_;
  int64_t extra_buf_size_;
  common::ObCompressorType compressor_type_;
  ObDirectLoadRowFlag row_flag_;
  ObDirectLoadSampleMode::Type sample_mode_; // 采样模式
  int64_t num_per_sample_; // 采样间隔
};

} // namespace storage
} // namespace oceanbase
