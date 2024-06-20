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

#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_mem_worker.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

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
  ObDirectLoadMemLoader(observer::ObTableLoadTableCtx *ctx, ObDirectLoadMemContext *mem_ctx);
  virtual ~ObDirectLoadMemLoader();
  int add_table(ObIDirectLoadPartitionTable *table) override;
  int work() override;
  VIRTUAL_TO_STRING_KV(KP(mem_ctx_), K_(fragments));
private:
  int close_chunk(ChunkType *&chunk);
private:
  observer::ObTableLoadTableCtx *ctx_;
  ObDirectLoadMemContext *mem_ctx_;
  ObDirectLoadExternalFragmentArray fragments_;
};

} // namespace storage
} // namespace oceanbase
