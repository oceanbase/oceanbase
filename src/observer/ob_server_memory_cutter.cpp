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

#define USING_LOG_PREFIX SERVER
#include "observer/ob_server_memory_cutter.h"
#include "lib/resource/achunk_mgr.h"
#include "lib/alloc/alloc_struct.h"
#include "share/cache/ob_kvcache_store.h"
#include "share/cache/ob_kv_storecache.h"
#include "lib/signal/ob_signal_utils.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {
ObServerMemoryCutter g_server_mem_cutter;

void ObServerMemoryCutter::free_cache(int64_t& total_size)
{
  auto& kv_store = ObKVGlobalCache::get_instance().store_;
  for (int i = 0; i < kv_store.cur_mb_num_; i++) {
    bool has_crash = false;
    do_with_crash_restore(
        [&]() {
          auto& mb_handle = kv_store.mb_handles_[i];
          if (mb_handle.mem_block_ != nullptr) {
            lib::AChunk* chunk = (AChunk*)((char*)mb_handle.mem_block_ - offsetof(AChunk, data_));
            if (chunk->check_magic_code()) {
              uint64_t all_size = ObMemoryCutter::chunk_size(chunk);
              ObMemoryCutter::free_chunk(chunk, all_size);
              total_size += all_size;
            } else {
              DLOG(WARN, "invalid chunk magic");
            }
          }
        },
        has_crash);
    if (has_crash) {
      DLOG(WARN, "restore from crash, let's go on~");
    }
  }
}

void ObServerMemoryCutter::cut(int64_t& total_size)
{
  lib::g_lib_mem_cutter.cut(total_size);
  bool has_crash = false;

  int64_t total_size_bak = total_size;
  do_with_crash_restore([&]() { return free_cache(total_size); }, has_crash);
  if (has_crash) {
    DLOG(WARN, "restore from crash, let's go on~");
  } else {
    DLOG(INFO, "free_cache, freed: %ld", total_size - total_size_bak);
  }
}

}  // end of namespace observer
}  // end of namespace oceanbase
