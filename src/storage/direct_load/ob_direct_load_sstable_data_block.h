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

#include "storage/direct_load/ob_direct_load_data_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadSSTableDataBlock
{
public:
  static const int64_t DEFAULT_DATA_BLOCK_SIZE = 16 * 1024; // 16K
  struct Header : public ObDirectLoadDataBlock::Header
  {
    OB_UNIS_VERSION(1);
  public:
    Header();
    ~Header();
    void reset();
    TO_STRING_KV(K_(last_row_pos));
  public:
    int32_t last_row_pos_;
    int32_t reserved_;
  };
public:
  static int64_t get_header_size();
};

struct ObDirectLoadSSTableDataBlockDesc
{
public:
  ObDirectLoadSSTableDataBlockDesc();
  ~ObDirectLoadSSTableDataBlockDesc();
  void reset();
  TO_STRING_KV(K_(fragment_idx), K_(offset), K_(size), K_(block_count), K_(is_left_border),
               K_(is_right_border));
public:
  int64_t fragment_idx_;
  int64_t offset_;
  int64_t size_;
  int64_t block_count_;
  bool is_left_border_;
  bool is_right_border_;
};

} // namespace storage
} // namespace oceanbase
