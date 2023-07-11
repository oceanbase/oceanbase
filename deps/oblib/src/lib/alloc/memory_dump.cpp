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

#define USING_LOG_PREFIX COMMON

#include "lib/alloc/memory_dump.h"
#include <setjmp.h>
#include "lib/alloc/ob_free_log_printer.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/rc/context.h"
#include "lib/utility/utility.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"
#include "lib/utility/ob_print_utils.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
using namespace lib;

namespace common
{

void get_tenant_ids(uint64_t *ids, int cap, int &cnt)
{
  auto *instance = ObMallocAllocator::get_instance();
  cnt = 0;
  for (uint64_t tenant_id = 1; tenant_id <= instance->get_max_used_tenant_id() && cnt < cap; ++tenant_id) {
    if (nullptr != instance->get_tenant_ctx_allocator(tenant_id,
                                                      ObCtxIds::DEFAULT_CTX_ID) ||
        nullptr != instance->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                 ObCtxIds::DEFAULT_CTX_ID)) {
      ids[cnt++] = tenant_id;
    }
  }
}

RLOCAL(sigjmp_buf, jmp);

static void dump_handler(int sig, siginfo_t *s, void *p)
{
  if (SIGSEGV == sig || SIGABRT == sig) {
    siglongjmp(jmp, 1);
  } else {
    ob_signal_handler(sig, s, p);
  }
}

template<typename Function>
void do_with_segv_catch(Function &&func, bool &has_segv, decltype(func()) &ret)
{
  has_segv = false;

  signal_handler_t handler_bak = get_signal_handler();
  int js = sigsetjmp(jmp, 1);
  if (0 == js) {
    get_signal_handler() = dump_handler;
    ret = func();
  } else if (1 == js) {
    has_segv = true;
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error!!!", K(js));
    ob_abort();
  }
  get_signal_handler() = handler_bak;
}


ObMemoryDump::ObMemoryDump()
  : task_mutex_(ObLatchIds::ALLOC_MEM_DUMP_TASK_LOCK),
    avaliable_task_set_((1 << TASK_NUM) - 1),
    print_buf_(nullptr),
    dump_context_(nullptr),
    iter_lock_(),
    r_stat_(nullptr),
    w_stat_(nullptr),
    huge_segv_cnt_(0),
    is_inited_(false)
{
  STATIC_ASSERT(TASK_NUM <= 64, "task num too large");
}

ObMemoryDump::~ObMemoryDump()
{
  if (is_inited_) {
    destroy();
  }
}

ObMemoryDump &ObMemoryDump::get_instance()
{
  static ObMemoryDump the_one;
  if (OB_UNLIKELY(!the_one.is_inited()) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    LOG_WARN_RET(OB_NOT_INIT, "memory dump not init");
  }
  return the_one;
}

int ObMemoryDump::init()
{
  int ret = OB_SUCCESS;
#ifndef OB_USE_ASAN
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(queue_.init(TASK_NUM, "memdumpqueue"))) {
    LOG_WARN("task queue init failed", K(ret));
  } else {
    MemoryContext context;// = nullptr;
    int ret = ROOT_CONTEXT->CREATE_CONTEXT(context, ContextParam().set_label("MemDumpContext"));
    PreAllocMemory *pre_mem = nullptr;
    if (OB_FAIL(ret)) {
      LOG_WARN("create context failed", K(ret));
    } else if (OB_ISNULL(pre_mem = (PreAllocMemory*)context->allocp(sizeof(PreAllocMemory)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else {
      LOG_INFO("pre memory size", K(sizeof(PreAllocMemory)));
      print_buf_ = pre_mem->print_buf_;
      array_ = pre_mem->array_buf_;
      tenant_ids_ = (uint64_t*)pre_mem->tenant_ids_buf_;
      log_buf_ = pre_mem->log_buf_;
      if (OB_FAIL(lmap_.create(1000, "MemDumpMap"))) {
        LOG_WARN("create map failed", K(ret));
      } else {
        r_stat_ = new (pre_mem->stats_buf_) Stat();
        w_stat_ = new (r_stat_ + 1) Stat();
        dump_context_ = context;
        is_inited_ = true;
        if (OB_FAIL(r_stat_->malloc_sample_map_.create(1000, "MallocInfoMap",
                                                       "MallocInfoMap"))) {
          LOG_WARN("create memory info map for reading failed", K(ret));
        } else if (OB_FAIL(w_stat_->malloc_sample_map_.create(1000, "MallocInfoMap",
                                                              "MallocInfoMap"))) {
          LOG_WARN("create memory info map for writing failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(TG_SET_RUNNABLE_AND_START(TGDefIDs::MEMORY_DUMP, *this))) {
          LOG_WARN("start thread pool fail", K(ret));
        }
      }
    }
    if (OB_FAIL(ret) && context != nullptr) {
      DESTROY_CONTEXT(context);
      context = nullptr;
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
#else
// do nothing
#endif
  return ret;
}

void ObMemoryDump::stop()
{
  if (is_inited_) {
    TG_STOP(TGDefIDs::MEMORY_DUMP);
  }
}

void ObMemoryDump::wait()
{
  if (is_inited_) {
    TG_WAIT(TGDefIDs::MEMORY_DUMP);
  }
}

void ObMemoryDump::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    queue_.destroy();
    is_inited_ = false;
  }
}

int ObMemoryDump::push(void *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = queue_.push(task);
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObMemoryDump::load_malloc_sample_map(ObMallocSampleMap &malloc_sample_map)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ObLatchRGuard guard(iter_lock_, ObLatchIds::MEM_DUMP_ITER_LOCK);
    auto &map = r_stat_->malloc_sample_map_;
    for (auto it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      ret = malloc_sample_map.set_refactored(it->first, it->second);
    }
  }
  return ret;
}

void ObMemoryDump::run1()
{
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  int ret = OB_SUCCESS;
  lib::set_thread_name("MemoryDump");
  static int64_t last_dump_ts = common::ObClockGenerator::getClock();
  while (!has_set_stop()) {
    void *task = NULL;
    if (OB_SUCC(queue_.pop(task, 100 * 1000))) {
      handle(task);
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      int64_t current_ts = common::ObClockGenerator::getClock();
      if (current_ts - last_dump_ts > STAT_LABEL_INTERVAL) {
        auto *task = alloc_task();
        if (OB_ISNULL(task)) {
          LOG_WARN("alloc task failed");
        } else {
          task->type_ = STAT_LABEL;
          if (OB_FAIL(push(task))){
            LOG_WARN("push task failed", K(ret));
            free_task(task);
          }
        }
        last_dump_ts = current_ts;
      } else {
        ob_usleep(current_ts - last_dump_ts);
      }
    }
  }
}

AChunk *ObMemoryDump::find_chunk(void *ptr)
{
  AChunk *ret = nullptr;
  auto func = [ptr] () {
      AChunk *ret = nullptr;
      const static uint32_t magic = ABLOCK_MAGIC_CODE + 1;
      int offset = 16;
      char *start = (char*)ptr - offset;
      char *end = (char*)ptr + offset;
      while (true) {
        void *loc = std::search(start,
                                end,
                                (char*)&magic,
                                (char*)&magic + sizeof(magic));
        if (loc != nullptr) {
          ABlock *block = (ABlock*)loc;
          AChunk *chunk = block->chunk();
          if (chunk != nullptr && chunk->is_valid()) {
            ret = chunk;
            break;
          }
        }
        start -= offset;
        end -= offset;
      }
      return ret;
  };
  bool has_segv = false;
  do_with_segv_catch(func, has_segv, ret);
  if (has_segv) {
    LOG_INFO("restore from sigsegv, let's goon~");
  }
  return ret;
}

template<typename BlockFunc, typename ObjectFunc>
int parse_block_meta(AChunk *chunk, ABlock *block, BlockFunc b_func, ObjectFunc o_func)
{
  int ret = OB_SUCCESS;
  ret = b_func(chunk, block);
  if (block->in_use_) {
    char *block_end = !block->is_large_ ?
      (char*)block->data() + block->hold() :
      (char*)block->chunk() + block->chunk()->hold();
    int loop_cnt = 0;
    AObject *object = (AObject*)(block->data());
    while (OB_SUCC(ret)) {
      if ((char*)object + AOBJECT_HEADER_SIZE > block_end ||
          !object->is_valid() || loop_cnt++ >= AllocHelper::cells_per_block(block->ablock_size_)) {
        break;
      }
      ret = o_func(chunk, block, object);
      // is_large shows that there is only one ojbect in block, so we can break directly.
      if (object->is_large_ || object->is_last(AllocHelper::cells_per_block(block->ablock_size_))) {
        break;
      }
      object = object->phy_next(object->nobjs_);
      if (nullptr == object) {
        break;
      }
    }
  }
  return ret;
}

template<typename ChunkFunc, typename BlockFunc, typename ObjectFunc>
int parse_chunk_meta(AChunk *chunk, ChunkFunc c_func, BlockFunc b_func,
                     ObjectFunc o_func)
{
  int ret = OB_SUCCESS;
  ret = c_func(chunk);
  char *block_meta_end = (char*)chunk + ACHUNK_HEADER_SIZE;
  int loop_cnt = 0;
  int offset = 0;
  do {
    ABlock *block = chunk->offset2blk(offset);
    if ((char*)block + ABLOCK_HEADER_SIZE > block_meta_end ||
        !block->is_valid() ||
        offset >= BLOCKS_PER_CHUNK ||
        loop_cnt++ >= BLOCKS_PER_CHUNK) {
      break;
    }
    ret = parse_block_meta(chunk, block, b_func, o_func);
    int next_offset = -1;
    bool is_last = chunk->is_last_blk_offset(offset, &next_offset);
    if (is_last) break;
    offset = next_offset;
  } while (OB_SUCC(ret));
  return ret;
}

int print_chunk_meta(AChunk *chunk, char *buf, int64_t buf_len, int64_t &pos)
{
  return databuff_printf(buf, buf_len, pos,
                         "chunk: %p, alloc_bytes: %ld, all_size: %ld, hold_size: %ld, washed_size: %ld, washed_blks: %ld, "
                         "ablock_size: %d, block_set: %p\n",
                         chunk, chunk->alloc_bytes_, chunk->aligned(), chunk->hold(),
                         chunk->washed_size_, chunk->washed_blks_, ABLOCK_SIZE, chunk->block_set_);
}

int print_block_meta(AChunk *chunk, ABlock *block, char *buf, int64_t buf_len, int64_t &pos,
                     int fd)
{
  int ret = OB_SUCCESS;
  ret = databuff_printf(buf, buf_len, pos,
                        "    block: %p, offset: %03d, in_use: %d, is_large: %d, is_washed: %d, nblocks: %03d," \
                        " alloc_bytes: %lu, aobject_size: %d, obj_set: %p, context: %p\n",
                        chunk->blk_data(block), chunk->blk_offset(block), block->in_use_, block->is_large_,
                        block->is_washed_, chunk->blk_nblocks(block),
                        block->alloc_bytes_, AOBJECT_CELL_BYTES, block->obj_set_, (int64_t*)block->mem_context_);
  if (OB_SUCC(ret)) {
    if (pos > buf_len / 2) {
      ::write(fd, buf, pos);
      pos = 0;
    }
  }
  return ret;
}

int print_object_meta(AChunk *chunk, ABlock *block, AObject *object, char *buf,
                      int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int offset = ((char*)object - chunk->blk_data(block))/AOBJECT_CELL_BYTES;
  void *label = &object->label_[0];
  void *end = memchr(label, '\0', sizeof(object->label_));
  int len = end ? (char*)end - (char*)label : sizeof(object->label_);
  ret = databuff_printf(buf, buf_len, pos,
                        "        object: %p, offset: %04d, in_use: %d, is_large: %d, nobjs: %04d," \
                        " label: \'%.*s\', alloc_bytes: %u\n",
                        object, offset, object->in_use_, object->is_large_, object->nobjs_,
                        len, (char*)label, object->alloc_bytes_);
  return ret;
}

int label_stat(AChunk *chunk, ABlock *block, AObject *object,
               LabelMap &lmap, LabelItem *items, int64_t item_cap,
               int64_t &item_used)
{
  int ret = OB_SUCCESS;
  if (object->in_use_) {
    int64_t hold = 0;
    if (!object->is_large_) {
      hold = object->nobjs_ * AOBJECT_CELL_BYTES;
    } else if (!block->is_large_) {
      hold = chunk->blk_nblocks(block) * ABLOCK_SIZE;
    } else {
      hold = align_up2(chunk->alloc_bytes_ + ACHUNK_HEADER_SIZE, get_page_size());
    }
    LabelItem *litem = nullptr;
    auto key = std::make_pair(*(uint64_t*)object->label_, *((uint64_t*)object->label_ + 1));
    LabelInfoItem *linfoitem = lmap.get(key);
    int64_t bt_size = object->on_malloc_sample_ ? AOBJECT_BACKTRACE_SIZE : 0;
    if (NULL != linfoitem) {
      // exist
      litem = linfoitem->litem_;
      litem->hold_ += hold;
      litem->used_ += (object->alloc_bytes_ - bt_size);
      litem->count_++;
      if (chunk != linfoitem->chunk_) {
        litem->chunk_cnt_ += 1;
        linfoitem->chunk_ = chunk;
      }
      if (block != linfoitem->block_) {
        litem->block_cnt_ += 1;
        linfoitem->block_ = block;
      }
    } else {
      if (item_used >= item_cap) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("label cnt too large", K(ret), K(item_cap), K(item_used));
      } else {
        litem = &items[item_used++];
        STRNCPY(litem->str_, object->label_, sizeof(litem->str_));
        litem->str_[sizeof(litem->str_) - 1] = '\0';
        litem->str_len_ = strlen(litem->str_);
        litem->hold_ = hold;
        litem->used_ = (object->alloc_bytes_ - bt_size);
        litem->count_ = 1;
        litem->block_cnt_ = 1;
        litem->chunk_cnt_ = 1;
        ret = lmap.set_refactored(key, LabelInfoItem(litem, chunk, block));
      }
    }
  }
  return ret;
}

int malloc_sample_stat(uint64_t tenant_id, uint64_t ctx_id,
                       AObject *object, ObMallocSampleMap &malloc_sample_map)
{
  int ret = OB_SUCCESS;
  if (object->in_use_ && object->on_malloc_sample_) {
    int64_t offset = object->alloc_bytes_ - AOBJECT_BACKTRACE_SIZE;
    ObMallocSampleKey key;
    key.tenant_id_ = tenant_id;
    key.ctx_id_ = ctx_id;
    MEMCPY((char*)key.bt_, &object->data_[offset], AOBJECT_BACKTRACE_SIZE);
    STRNCPY(key.label_, object->label_, sizeof(key.label_));
    key.label_[sizeof(key.label_) - 1] = '\0';
    ObMallocSampleValue *item = malloc_sample_map.get(key);
    if (NULL != item) {
      item->alloc_count_ += 1;
      item->alloc_bytes_ += offset;
    } else {
      ret = malloc_sample_map.set_refactored(key, ObMallocSampleValue(1, offset));
    }
  }
  return ret;
}

void ObMemoryDump::handle(void *task)
{
  int ret = OB_SUCCESS;
  bool segv_cnt_over = false;
  ObMemoryDumpTask *m_task = static_cast<ObMemoryDumpTask*>(task);
  LOG_INFO("handle dump task", "task", *m_task);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (STAT_LABEL == m_task->type_) {
    int tenant_cnt = 0;
    get_tenant_ids(tenant_ids_, MAX_TENANT_CNT, tenant_cnt);
    std::sort(tenant_ids_, tenant_ids_ + tenant_cnt);
    w_stat_->tcr_cnt_ = 0;
    w_stat_->malloc_sample_map_.clear();
    int64_t item_used = 0;
    int64_t log_pos = 0;
    IGNORE_RETURN databuff_printf(log_buf_, LOG_BUF_LEN, log_pos,
                                  "\ntenant_cnt: %d, max_chunk_cnt: %d\n" \
                                  "%-15s%-15s%-15s%-15s%-15s\n",
                                  tenant_cnt, MAX_CHUNK_CNT,
                                  "tenant_id", "ctx_id", "chunk_cnt", "label_cnt",
                                  "segv_cnt");
    const int64_t start_ts = ObTimeUtility::current_time();
    for (int tenant_idx = 0; tenant_idx < tenant_cnt; tenant_idx++) {
      uint64_t tenant_id = tenant_ids_[tenant_idx];
      for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        auto ta =
          ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
        if (nullptr == ta) {
          ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                      ctx_id);
        }
        if (nullptr == ta) {
          continue;
        }
        int segv_cnt = 0;
        const int64_t orig_item_used = item_used;
        int chunk_cnt = 0;
        ret = OB_SUCCESS;
        ta->get_chunks(chunks_, MAX_CHUNK_CNT, chunk_cnt);
        auto &w_stat = w_stat_;
        auto &lmap = lmap_;
        lmap.clear();
        for (int i = 0; OB_SUCC(ret) && i < chunk_cnt; i++) {
          AChunk *chunk = chunks_[i];
          auto func = [&, chunk] {
              int ret = parse_chunk_meta(chunk,
                  [] (AChunk *chunk) {
                    UNUSEDx(chunk);
                    return OB_SUCCESS;
                  },
                  [] (AChunk *chunk, ABlock *block) {
                    UNUSEDx(chunk, block);
                    return OB_SUCCESS;
                  },
                  [tenant_id, ctx_id, &lmap, w_stat, &item_used]
                  (AChunk *chunk, ABlock *block, AObject *object) {
                    int ret = OB_SUCCESS;
                    if (object->in_use_) {
                      bool expect = AOBJECT_TAIL_MAGIC_CODE ==
                        reinterpret_cast<uint64_t&>(object->data_[object->alloc_bytes_]);
                      if (!expect && object->is_valid() && object->in_use_
                          && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                        ObLabel label = object->label();
                        char *ptr = object->data_;
                        int32_t length = object->alloc_bytes_;
                        LOG_INFO("tail magic maybe broken!!!", K(tenant_id), KP(ptr),
                                  K(length), K(label));
                      }
                    }
                    ret = label_stat(chunk, block, object, lmap,
                                      w_stat->up2date_items_, ARRAYSIZEOF(w_stat->up2date_items_),
                                      item_used);
                    if (OB_SUCC(ret)) {
                      ret = malloc_sample_stat(tenant_id, ctx_id,
                                               object, w_stat->malloc_sample_map_);
                    }
                    return ret;
                  });
              if (OB_FAIL(ret)) {
                LOG_WARN("parse_chunk_meta failed", K(ret), KP(chunk));
              }
              return ret;
          };
          bool has_segv = false;
          do_with_segv_catch(func, has_segv, ret);
          if (has_segv) {
            LOG_INFO("restore from sigsegv, let's goon~");
            segv_cnt++;
            continue;
          }
        } // iter chunk end
        if (OB_SUCC(ret)) {
          auto &tcr = w_stat_->tcrs_[w_stat_->tcr_cnt_++];
          tcr.tenant_id_ = tenant_id;
          tcr.ctx_id_ = ctx_id;
          tcr.start_ = orig_item_used;
          tcr.end_ = item_used;
        }
        if (segv_cnt > 128) {
          LOG_WARN("too many sigsegv, maybe there is a low-level bug", K(segv_cnt));
          segv_cnt_over = true;
        }
        if (OB_SUCC(ret) && (chunk_cnt != 0 || segv_cnt != 0)) {
          IGNORE_RETURN databuff_printf(log_buf_, LOG_BUF_LEN, log_pos,
                                        "%-15lu%-15d%-15d%-15ld%-15d\n",
                                        tenant_id, ctx_id, chunk_cnt,
                                        item_used - orig_item_used, segv_cnt);
        }
      } // iter ctx end
    } // iter tenant end
    if (segv_cnt_over) {
      ++huge_segv_cnt_;
    } else {
      huge_segv_cnt_ = 0;
    }
    if (huge_segv_cnt_ > 8) {
      LOG_ERROR("too many sigsegv has happened many times continuously, maybe there is a low-level bug");
    }
    if (OB_SUCC(ret)) {
      IGNORE_RETURN databuff_printf(log_buf_, LOG_BUF_LEN, log_pos, "cost_time: %ld",
                                    ObTimeUtility::current_time() - start_ts);
    }
    if (log_pos > 0) {
      _OB_LOG(INFO, "statistics: %.*s", static_cast<int32_t>(log_pos), log_buf_);
    }
    // switch stat as long as one tenant-ctx is generated, ignore the error code.
    if (w_stat_->tcr_cnt_ > 0) {
      ObLatchWGuard guard(iter_lock_, common::ObLatchIds::MEM_DUMP_ITER_LOCK);
      std::swap(r_stat_, w_stat_);
    }
    ObFreeLogPrinter::get_instance().disable_free_log();
  } else {
    int fd = -1;
    if (-1 == (fd = ::open(LOG_FILE,
                           O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create new file failed", KCSTRING(strerror(errno)));
    }
    if (OB_SUCC(ret)) {
      int64_t print_pos = 0;
      struct timeval tv;
      gettimeofday(&tv, NULL);
      struct tm tm;
      ::localtime_r((const time_t *) &tv.tv_sec, &tm);
      ret = databuff_printf(print_buf_, PRINT_BUF_LEN, print_pos,
          "\n###################%04d-%02d-%02d %02d:%02d:%02d.%06ld###################\n",
          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
          tm.tm_sec, tv.tv_usec);
      print_pos += m_task->to_string(print_buf_ + print_pos, PRINT_BUF_LEN - print_pos);
      ret = databuff_printf(print_buf_, PRINT_BUF_LEN, print_pos, "\n");
      if (DUMP_CONTEXT == m_task->type_) {
        __MemoryContext__ *context = reinterpret_cast<__MemoryContext__*>(m_task->p_context_);
        auto func = [&] {
            const char *str = to_cstring(*context);
            ::write(fd, str, strlen(str));
            ::write(fd, "\n", 1);
            return OB_SUCCESS;
        };
        bool has_segv = false;
        do_with_segv_catch(func, has_segv, ret);
        if (has_segv) {
          LOG_INFO("restore from sigsegv, let's goon~");
        }
      } else {
        // chunk
        int cnt = 0;
        if (m_task->dump_all_) {
          int tenant_cnt = 0;
          get_tenant_ids(tenant_ids_, MAX_TENANT_CNT, tenant_cnt);
          std::sort(tenant_ids_, tenant_ids_ + tenant_cnt);
          for (int tenant_idx = 0; tenant_idx < tenant_cnt; tenant_idx++) {
            uint64_t tenant_id = tenant_ids_[tenant_idx];
            for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
              auto ta =
                ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
              if (nullptr == ta) {
                ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                            ctx_id);
              }
              if (nullptr != ta) {
                ta->get_chunks(chunks_, MAX_CHUNK_CNT, cnt);
              }
            }
          }
        } else if (m_task->dump_tenant_ctx_) {
          auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(m_task->tenant_id_,
                                                                                m_task->ctx_id_);
          if (nullptr == ta) {
            ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(m_task->tenant_id_,
                                                                                        m_task->ctx_id_);
          }
          if (nullptr != ta) {
            ta->get_chunks(chunks_, MAX_CHUNK_CNT, cnt);
          }
        } else {
          AChunk *chunk = find_chunk(m_task->p_chunk_);
          if (chunk != nullptr) {
            chunks_[cnt++] = chunk;
          }
        }
        LOG_INFO("chunk cnt", K(cnt));
        // sort chunk
        std::sort(chunks_, chunks_ + cnt);
        // iter chunk
        for (int i = 0; OB_SUCC(ret) && i < cnt; i++) {
          AChunk *chunk = chunks_[i];
          char *print_buf = print_buf_; // for lambda capture
          auto func = [&, chunk] {
              int ret = parse_chunk_meta(chunk,
                  [print_buf, &print_pos] (AChunk *chunk) {
                    return print_chunk_meta(chunk, print_buf, PRINT_BUF_LEN, print_pos);
                  },
                  [print_buf, &print_pos, fd] (AChunk *chunk, ABlock *block) {
                    UNUSEDx(chunk);
                    return print_block_meta(chunk, block, print_buf, PRINT_BUF_LEN, print_pos, fd);
                  },
                  [print_buf, &print_pos] (AChunk *chunk, ABlock *block, AObject *object) {
                    UNUSEDx(chunk, block);
                    return print_object_meta(chunk, block, object, print_buf, PRINT_BUF_LEN, print_pos);
                  });
              if (OB_FAIL(ret)) {
                LOG_WARN("parse_chunk_meta failed", K(ret), KP(chunk));
              }
              return OB_SUCCESS;
          };
          bool has_segv = false;
          do_with_segv_catch(func, has_segv, ret);
          if (has_segv) {
            LOG_INFO("restore from sigsegv, let's goon~");
            continue;
          }
        } // iter chunk end
        if (OB_SUCC(ret) && print_pos > 0) {
          ::write(fd, print_buf_, print_pos);
        }
      }
    }
    if (fd > 0) {
      ::close(fd);
    }
  }
  if (task != nullptr) {
    free_task(task);
  }
}

} // namespace common
} // namespace oceanbase
