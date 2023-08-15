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

class ObDirectLoadSSTableIndexBlock
{
public:
  static const int64_t DEFAULT_INDEX_BLOCK_SIZE = 4 * 1024; // 4K
  struct Header : public ObDirectLoadDataBlock::Header
  {
    OB_UNIS_VERSION(1);
  public:
    Header();
    ~Header();
    void reset();
    TO_STRING_KV(K_(offset), K_(count), K_(last_entry_pos));
  public:
    int64_t offset_; // start offset of index block data
    int32_t count_;
    int32_t last_entry_pos_;
  };
  struct Entry
  {
    OB_UNIS_VERSION(1);
  public:
    Entry();
    ~Entry();
    void reuse();
    void reset();
    TO_STRING_KV(K_(offset));
  public:
    int64_t offset_;
  };
public:
  static int64_t get_header_size();
  static int64_t get_entry_size();
  static int64_t get_entries_per_block(int64_t block_size);
};

struct ObDirectLoadSSTableIndexEntry
{
public:
  ObDirectLoadSSTableIndexEntry();
  ~ObDirectLoadSSTableIndexEntry();
  TO_STRING_KV(K_(offset), K_(size));
public:
  int64_t offset_;
  int64_t size_;
};

} // namespace storage
} // namespace oceanbase
