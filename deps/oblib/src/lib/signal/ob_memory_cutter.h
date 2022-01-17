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

#ifndef OCEANBASE_MEMORY_CUTTER_H_
#define OCEANBASE_MEMORY_CUTTER_H_

#include <stdint.h>
namespace oceanbase {
namespace lib {
class AChunk;
class ObIMemoryCutter {
public:
  virtual void cut(int64_t& total_size) = 0;
};

class ObMemoryCutter : public ObIMemoryCutter {
public:
  virtual void cut(int64_t& total_size) override;
  static void free_chunk(const void* ptr, const uint64_t size);
  static uint64_t chunk_size(AChunk* chunk);

private:
  void free_chunk(int64_t& total_size);
  void free_stack(int64_t& total_size);
  void free_memstore(int64_t& total_size);
};

extern ObMemoryCutter g_lib_mem_cutter;
extern ObIMemoryCutter* g_mem_cutter;

}  // namespace lib
}  // namespace oceanbase

#endif  // OCEANBASE_MEMORY_CUTTER_H_
