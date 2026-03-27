/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_mem_chunk.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadExternalMultiPartitionRowRange
{
public:
  ObDirectLoadExternalMultiPartitionRowRange() : start_(nullptr), end_(nullptr) {}
  ObDirectLoadExternalMultiPartitionRowRange(ObDirectLoadConstExternalMultiPartitionRow *start,
                                             ObDirectLoadConstExternalMultiPartitionRow *end)
    : start_(start), end_(end)
  {
  }
  TO_STRING_KV(KP_(start), KP_(end));

public:
  ObDirectLoadConstExternalMultiPartitionRow *start_;
  ObDirectLoadConstExternalMultiPartitionRow *end_;
};

typedef ObDirectLoadMemChunk<ObDirectLoadConstExternalMultiPartitionRow,
                             ObDirectLoadExternalMultiPartitionRowCompare>
  ObDirectLoadExternalMultiPartitionRowChunk;

} // namespace storage
} // namespace oceanbase
