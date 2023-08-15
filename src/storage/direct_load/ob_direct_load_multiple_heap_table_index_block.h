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

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_data_block.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMultipleHeapTableIndexBlock
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
    TO_STRING_KV(K_(count), K_(last_entry_pos));
  public:
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
    TO_STRING_KV(K_(tablet_id), K_(row_count), K_(offset));
  public:
    uint64_t tablet_id_;
    int64_t row_count_;
    union
    {
      struct
      {
        int64_t fragment_idx_ : 16;
        int64_t offset_ : 48;
      };
      int64_t offset_val_;
    };
  };
public:
  static int64_t get_header_size();
  static int64_t get_entry_size();
  static int64_t get_entries_per_block(int64_t block_size);
};

struct ObDirectLoadMultipleHeapTableTabletIndex
{
public:
  ObDirectLoadMultipleHeapTableTabletIndex();
  ~ObDirectLoadMultipleHeapTableTabletIndex();
  TO_STRING_KV(K_(tablet_id), K_(row_count), K_(fragment_idx), K_(offset));
public:
  common::ObTabletID tablet_id_;
  int64_t row_count_;
  int64_t fragment_idx_;
  int64_t offset_;
};

} // namespace storage
} // namespace oceanbase
