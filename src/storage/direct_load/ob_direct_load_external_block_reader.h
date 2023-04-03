// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_data_block.h"
#include "storage/direct_load/ob_direct_load_data_block_reader.h"

namespace oceanbase
{
namespace storage
{

template <typename T>
using ObDirectLoadExternalBlockReader =
  ObDirectLoadDataBlockReader<ObDirectLoadDataBlock::Header, T>;

} // namespace storage
} // namespace oceanbase
