// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_mem_worker.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMemLoader : public ObDirectLoadMemWorker
{
  typedef ObDirectLoadConstExternalMultiPartitionRow RowType;
  typedef ObDirectLoadExternalMultiPartitionRowChunk ChunkType;
  typedef ObDirectLoadExternalMultiPartitionRowCompare CompareType;
public:
  ObDirectLoadMemLoader(ObDirectLoadMemContext *mem_ctx);
  virtual ~ObDirectLoadMemLoader();
  int add_table(ObIDirectLoadPartitionTable *table) override;
  int work() override;
  VIRTUAL_TO_STRING_KV(KP(mem_ctx_), K_(fragments));
private:
  int close_chunk(ChunkType *&chunk);
private:
  ObDirectLoadMemContext *mem_ctx_;
  ObDirectLoadExternalFragmentArray fragments_;
};

} // namespace storage
} // namespace oceanbase
