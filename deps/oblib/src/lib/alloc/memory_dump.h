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

#ifndef OCEANBASE_DUMP_MEMORY_H_
#define OCEANBASE_DUMP_MEMORY_H_

#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/rc/context.h"
#include "lib/thread/thread_mgr_interface.h"

// This file will be placed under lib for a short period of time to facilitate unit testing. After the function is stable, move to ob
// The corresponding MySimpleThreadPool will also be deleted

namespace oceanbase
{
namespace observer
{
class ObAllVirtualMemoryInfo;
class ObMallocSampleInfo;
}
namespace lib
{
struct AChunk;
struct ABlock;
struct AObject;
class ObTenantCtxAllocator;
}
namespace common
{
enum DumpType
{
  DUMP_CONTEXT,
  DUMP_CHUNK,
  STAT_LABEL
};

class ObMemoryDumpTask
{
public:
  TO_STRING_KV(K(type_), K(dump_all_), KP(p_context_), K(slot_idx_),
               K(dump_tenant_ctx_), K(tenant_id_), K(ctx_id_), KP(p_chunk_));
  DumpType type_;
  bool dump_all_;
  union
  {
    struct {
      void *p_context_;
      int slot_idx_;
    };
    struct {
      bool dump_tenant_ctx_;
      union {
        struct {
          int64_t tenant_id_;
          int64_t ctx_id_;
        };
        void *p_chunk_;
      };
    };
  };
};

struct LabelItem
{
  LabelItem()
  {
    MEMSET(this, 0 , sizeof(*this));
  }
  char str_[lib::AOBJECT_LABEL_SIZE + 1];
  int str_len_;
  int64_t hold_;
  int64_t used_;
  int64_t count_;
  int64_t block_cnt_;
  int64_t chunk_cnt_;
  LabelItem &operator +=(const LabelItem &item)
  {
    hold_ += item.hold_;
    used_ += item.used_;
    count_ += item.count_;
    return *this;
  }
};
struct LabelInfoItem
{
  LabelInfoItem()
  {}
  LabelInfoItem(LabelItem* litem, void *chunk, void *block)
    : litem_(litem), chunk_(chunk), block_(block)
  {}
  LabelItem* litem_;
  void *chunk_;
  void *block_;
};

typedef common::hash::ObHashMap<std::pair<uint64_t, uint64_t>, LabelInfoItem, hash::NoPthreadDefendMode> LabelMap;

using lib::AChunk;
using lib::ABlock;
using lib::AObject;
class ObMemoryDump : public lib::TGRunnable
{
public:
  static constexpr const char *LOG_FILE = "log/memory_meta";
private:
friend class observer::ObAllVirtualMemoryInfo;
friend class lib::ObTenantCtxAllocator;
friend class lib::ObMallocAllocator;

static const int64_t TASK_NUM = 8;
static const int PRINT_BUF_LEN = 1L << 20;
static const int64_t MAX_MEMORY = 1L << 40; // 1T
static const int MAX_CHUNK_CNT = MAX_MEMORY / (2L << 20);
static const int MAX_TENANT_CNT = OB_MAX_SERVER_TENANT_CNT;
static const int MAX_LABEL_ITEM_CNT = 16L << 10;
static const int64_t STAT_LABEL_INTERVAL = 10L * 1000L * 1000L;
static const int64_t LOG_BUF_LEN = 64L << 10;

struct TenantCtxRange
{
  static bool compare(const TenantCtxRange &tcr,
                      const std::pair<uint64_t, uint64_t> &cmp_val)
  {
    return tcr.tenant_id_ < cmp_val.first ||
      (tcr.tenant_id_ == cmp_val.first && tcr.ctx_id_ < cmp_val.second);
  }
  uint64_t tenant_id_;
  uint64_t ctx_id_;
  // [start_, end_)
  int start_;
  int end_;
};

struct Stat {
  LabelItem up2date_items_[MAX_LABEL_ITEM_CNT];
  TenantCtxRange tcrs_[MAX_TENANT_CNT * ObCtxIds::MAX_CTX_ID];
  lib::ObMallocSampleMap  malloc_sample_map_;
  int tcr_cnt_ = 0;
};

struct PreAllocMemory
{
  char print_buf_[PRINT_BUF_LEN];
  char array_buf_[MAX_CHUNK_CNT * sizeof(void*)];
  char tenant_ids_buf_[MAX_TENANT_CNT * sizeof(uint64_t)];
  char stats_buf_[sizeof(Stat) * 2];
  char log_buf_[LOG_BUF_LEN];
};

public:
  ObMemoryDump();
  ~ObMemoryDump();
  static ObMemoryDump &get_instance();
  int init();
  void stop();
  void wait();
  void destroy();
  bool is_inited() const { return is_inited_; }
  int push(void *task);
  ObMemoryDumpTask *alloc_task()
  {
    ObMemoryDumpTask *task = nullptr;
    lib::ObMutexGuard guard(task_mutex_);
    int pos = -1;
    if ((pos = ffsl(avaliable_task_set_))) {
      pos--;
      abort_unless(pos >= 0 && pos < TASK_NUM);
      task = &tasks_[pos];
      avaliable_task_set_ &= ~(1 << pos);
    }
    return task;
  }
  void free_task(void *task)
  {
    int pos = (ObMemoryDumpTask *)task - &tasks_[0];
    abort_unless(pos >= 0 && pos < TASK_NUM);
    lib::ObMutexGuard guard(task_mutex_);
    avaliable_task_set_ |= (1 << pos);
  }
  int load_malloc_sample_map(lib::ObMallocSampleMap& malloc_sample_map);
private:
  void run1() override;
  void handle(void *task);
private:
  AChunk *find_chunk(void *ptr);
private:
  ObLightyQueue queue_;
  lib::ObMutex task_mutex_;
  ObMemoryDumpTask tasks_[TASK_NUM];
  int64_t avaliable_task_set_;
  char *print_buf_;
  union {
    void *array_;
    AChunk **chunks_;
  };
  uint64_t *tenant_ids_;
  lib::MemoryContext dump_context_;
  LabelMap lmap_;
  common::ObLatch iter_lock_;
  Stat *r_stat_;
  Stat *w_stat_;
  char *log_buf_;
  int huge_segv_cnt_;
  bool is_inited_;
};

extern void get_tenant_ids(uint64_t *ids, int cap, int &cnt);
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_DUMP_MEMORY_H_
