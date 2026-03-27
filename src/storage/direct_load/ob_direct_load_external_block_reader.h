/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
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
