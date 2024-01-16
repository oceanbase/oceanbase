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

#include "storage/direct_load/ob_direct_load_sstable_builder.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include <memory>
#include "storage/direct_load/ob_direct_load_mem_dump.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMemSample
{
  static const constexpr int64_t DEFAULT_SAMPLE_TIMES = 50000;
  typedef ObDirectLoadConstExternalMultiPartitionRow RowType;
  typedef ObDirectLoadExternalMultiPartitionRowChunk ChunkType;
  typedef ObDirectLoadExternalMultiPartitionRowRange RangeType;
  typedef ObDirectLoadExternalMultiPartitionRowCompare CompareType;
public:
  ObDirectLoadMemSample(observer::ObTableLoadTableCtx *ctx, ObDirectLoadMemContext *mem_ctx);
  virtual ~ObDirectLoadMemSample() {}

  int do_sample();

private:
  int do_work();
  int add_dump(int64_t idx,
               common::ObArray<ChunkType *> &mem_chunk_array,
               const RangeType &range,
               table::ObTableLoadHandle<ObDirectLoadMemDump::Context> sample_ptr);
  int gen_ranges(common::ObIArray<ChunkType *> &chunks,
                 common::ObIArray<RangeType> &ranges);

private:
  // data members
  observer::ObTableLoadTableCtx *ctx_;
  ObDirectLoadMemContext *mem_ctx_;
  int64_t range_count_;
};



} // namespace storage
} // namespace oceanbase
