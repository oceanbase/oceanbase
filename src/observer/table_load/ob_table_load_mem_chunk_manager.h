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

#include "storage/direct_load/ob_direct_load_mem_context.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;

struct ObTableLoadChunkNode
{
public:
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  ObTableLoadChunkNode();
  ~ObTableLoadChunkNode();
  bool is_used() const { return ATOMIC_LOAD(&is_used_); }
  bool set_used() { return (false == ATOMIC_VCAS(&is_used_, false, true)); }
  bool set_unused() { return (true == ATOMIC_VCAS(&is_used_, true, false)); }
  TO_STRING_KV(KP_(chunk), K_(is_used));
public:
  ChunkType *chunk_;
  bool is_used_;
};

class ObTableLoadMemChunkManager
{
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  using CompareType = storage::ObDirectLoadExternalMultiPartitionRowCompare;
public:
  ObTableLoadMemChunkManager();
  ~ObTableLoadMemChunkManager();
  int init(ObTableLoadStoreCtx *store_ctx, storage::ObDirectLoadMemContext *mem_ctx);
  int get_chunk(int64_t &chunk_node_id, ChunkType *&chunk);
  // for close all
  int get_unclosed_chunks(ObIArray<int64_t> &chunk_node_ids);
  int push_chunk(int64_t chunk_node_id);
  int close_chunk(int64_t chunk_node_id);
private:
  int acquire_chunk(ChunkType *&chunk);
private:
  ObTableLoadStoreCtx *store_ctx_;
  storage::ObDirectLoadMemContext *mem_ctx_;
  ObArray<ObTableLoadChunkNode> chunk_nodes_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTableLoadMemChunkManager);
};

} // namespace observer
} // namespace oceanbase
