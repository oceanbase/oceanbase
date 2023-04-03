// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_sstable_builder.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include <memory>
#include "storage/direct_load/ob_direct_load_mem_dump.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"

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
  ObDirectLoadMemSample(ObDirectLoadMemContext *mem_ctx);
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
  ObDirectLoadMemContext *mem_ctx_;
  int64_t range_count_;
};



} // namespace storage
} // namespace oceanbase
