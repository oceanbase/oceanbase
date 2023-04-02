// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
