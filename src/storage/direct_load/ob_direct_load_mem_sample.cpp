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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_mem_sample.h"
#include "lib/random/ob_random.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace observer;
using namespace table;

ObDirectLoadMemSample::ObDirectLoadMemSample(observer::ObTableLoadTableCtx *ctx, ObDirectLoadMemContext *mem_ctx)
  : ctx_(ctx), mem_ctx_(mem_ctx), range_count_(mem_ctx_->dump_thread_cnt_)
{
}


int ObDirectLoadMemSample::gen_ranges(ObIArray<ChunkType *> &chunks, ObIArray<RangeType> &ranges)
{
  int ret = OB_SUCCESS;
  ObArray<RowType *> sample_rows;
  sample_rows.set_tenant_id(MTL_ID());
  int64_t row_count = 0;
  for (int64_t i = 0; i < chunks.count(); ++i) {
    row_count += chunks.at(i)->get_size();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MIN(row_count, DEFAULT_SAMPLE_TIMES); i ++) {
    int idx = ObRandom::rand(0, chunks.count() - 1);
    ChunkType *chunk = chunks.at(idx);
    int idx2 = ObRandom::rand(0, chunk->get_size() - 1);
    RowType *row = chunk->get_item(idx2);
    if (OB_FAIL(sample_rows.push_back(row))) {
      LOG_WARN("fail to push row", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    CompareType compare;
    if (OB_FAIL(compare.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else {
      lib::ob_sort(sample_rows.begin(), sample_rows.end(), compare);
    }
  }

  int64_t step = sample_rows.count() / range_count_;

  RowType *last_row = nullptr;
  for (int64_t i = 1; OB_SUCC(ret) && i <= range_count_; i ++) {
    if (i != range_count_) {
      if (OB_FAIL(ranges.push_back(RangeType(last_row, sample_rows[i * step])))) {
        LOG_WARN("fail to push range", KR(ret));
      } else {
        last_row = sample_rows[i * step];
      }
    } else {
      if (OB_FAIL(ranges.push_back(RangeType(last_row, nullptr)))) {
        LOG_WARN("fail to push range", KR(ret));
      } else {
        last_row = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMemSample::do_work()
{
  int ret = OB_SUCCESS;
  ObArray<ChunkType *> chunks;
  ObArray<RangeType> ranges;
  ObTableLoadHandle<ObDirectLoadMemDump::Context> context_ptr;

  chunks.set_tenant_id(MTL_ID());
  ranges.set_tenant_id(MTL_ID());
  mem_ctx_->mem_chunk_queue_.pop_all(chunks);
  if (OB_FAIL(ObTableLoadHandle<ObDirectLoadMemDump::Context>::make_handle(context_ptr))) {
    LOG_WARN("fail to make handle", KR(ret));
  } else if (FALSE_IT(context_ptr->sub_dump_count_ = range_count_)) {
  } else if (OB_FAIL(context_ptr->init())) {
    LOG_WARN("fail to init context", KR(ret));
  } else if (OB_FAIL(context_ptr->mem_chunk_array_.assign(chunks))) {
    LOG_WARN("fail to assgin chunks", KR(ret));

    //出错以后释放chunks
    context_ptr->mem_chunk_array_.reset();
    for (int64_t i = 0; i < chunks.count(); i ++) {
      ChunkType *chunk = chunks.at(i);
      if (chunk != nullptr) {
        chunk->~ChunkType();
        ob_free(chunk);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(gen_ranges(chunks, ranges))) {
      LOG_WARN("fail to gen ranges", KR(ret));
    } else {
      ATOMIC_AAF(&(mem_ctx_->running_dump_task_cnt_), range_count_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_count_; i ++) {
    if (OB_FAIL(add_dump(i, chunks, ranges[i], context_ptr))) {
      LOG_WARN("fail to start dump", KR(ret));
    }
  }

  return ret;
}

int ObDirectLoadMemSample::add_dump(int64_t idx,
                                    common::ObArray<ChunkType *> &mem_chunk_array,
                                    const RangeType &range,
                                    ObTableLoadHandle<ObDirectLoadMemDump::Context> context_ptr)
{
  int ret = OB_SUCCESS;
  storage::ObDirectLoadMemDump *mem_dump = OB_NEW(
    ObDirectLoadMemDump, ObMemAttr(MTL_ID(), "TLD_mem_dump"), ctx_, mem_ctx_, range, context_ptr, idx);
  if (mem_dump == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mem dump", KR(ret));
  } else if (OB_FAIL(mem_ctx_->mem_dump_queue_.push(mem_dump))) {
    LOG_WARN("fail to push mem dump", KR(ret));
  }
  return ret;
}

int ObDirectLoadMemSample::do_sample()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_)) {
    if (mem_ctx_->finish_load_thread_cnt_ >= mem_ctx_->load_thread_cnt_) {
      if (mem_ctx_->mem_chunk_queue_.size() > 0) {
        if (OB_FAIL(do_work())) {
          LOG_WARN("fail to do work", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(mem_ctx_->mem_dump_queue_.push(nullptr))) {
          LOG_WARN("fail to push queue", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        while (mem_ctx_->running_dump_task_cnt_ > 0 && OB_LIKELY(!mem_ctx_->has_error_)) { //等待所有的merge做完
          usleep(100000);
        }
      }
      break;
    }
    int64_t mem_chunk_dump_count = mem_ctx_->max_mem_chunk_count_ / 2;
    if (mem_chunk_dump_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(mem_ctx_->max_mem_chunk_count_));
    }
    if (OB_SUCC(ret)) {
      if (mem_ctx_->mem_chunk_queue_.size() < mem_chunk_dump_count) {
        usleep(100000);
        continue;
      }
      if (OB_FAIL(do_work())) {
        LOG_WARN("fail to do work", KR(ret));
      }
    }
  }
  if (OB_UNLIKELY(ret != OB_SUCCESS || mem_ctx_->has_error_)) {
    mem_ctx_->mem_dump_queue_.push(nullptr); //出错了，让dump结束，避免卡死
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
