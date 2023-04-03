// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadDataBlock
{
public:
  static const int64_t DEFAULT_DATA_BLOCK_SIZE = 128 * 1024; // 128K
  struct Header
  {
    OB_UNIS_VERSION(1);
  public:
    Header();
    ~Header();
    void reset();
    TO_STRING_KV(K_(occupy_size), K_(data_size), K_(checksum));
  public:
    int32_t occupy_size_; // occupy size of data block, include header
    int32_t data_size_; // size of raw data, include header
    int64_t checksum_; // checksum of valid data
  };
public:
  static int64_t get_header_size();
};

} // namespace storage
} // namespace oceanbase
