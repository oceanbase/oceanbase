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
#ifndef _OB_TABLE_LOAD_MEM_CHUNK_MANAGER_H_
#define _OB_TABLE_LOAD_MEM_CHUNK_MANAGER_H_

#include "storage/direct_load/ob_direct_load_external_block_writer.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "deps/oblib/src/lib/lock/ob_thread_cond.h"


namespace oceanbase
{
namespace storage
{
  class ObDirectLoadMemContext;
}
namespace observer
{
struct ObTableLoadChunkNode {
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  ObTableLoadChunkNode();
  ~ObTableLoadChunkNode();
  ChunkType *chunk_;
  lib::ObMutex chunk_node_mutex_;
  bool is_used_;
  TO_STRING_KV(K_(is_used));
};
class ObTableLoadMemChunkManager {
  using ChunkType = storage::ObDirectLoadExternalMultiPartitionRowChunk;
  using CompareType = storage::ObDirectLoadExternalMultiPartitionRowCompare;
public:
  ObTableLoadMemChunkManager();
  ~ObTableLoadMemChunkManager();
  int init(ObTableLoadStoreCtx *store_ctx, storage::ObDirectLoadMemContext *mem_ctx);
  int alloc_chunk(ChunkType *&chunk);
  int get_chunk_node_id(int64_t &chunk_node_id);
  int get_chunk(int64_t &chunk_node_id, ChunkType *&chunk);
  int push_chunk(int64_t chunk_node_id);
  int close_all_chunk();
  int close_chunk(int64_t chunk_node_id);
  int handle_close_task_finished();
private:
  class CloseChunkTaskProcessor;
  class CloseChunkTaskCallback;
public:
  observer::ObTableLoadStoreCtx *store_ctx_;
  storage::ObDirectLoadMemContext *mem_ctx_;
  int64_t chunks_count_;
  int64_t max_chunks_count_;
  int64_t mem_chunk_size_;
  observer::ObTableLoadExeMode exe_mode_;
private:
  ObArray<ObTableLoadChunkNode *> chunk_nodes_;
  int64_t close_task_count_;
  int64_t finished_close_task_count_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadMemChunkManager);
};

} // namespace observer
} // namespace oceanbase
#endif /*_OB_TABLE_LOAD_MEM_CHUNK_MANAGER_H_*/
