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
