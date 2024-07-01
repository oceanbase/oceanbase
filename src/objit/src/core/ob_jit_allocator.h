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

#ifndef OB_JIT_ALLOCATOR_H
#define OB_JIT_ALLOCATOR_H

#include <sys/mman.h>
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace jit {
namespace core {

enum JitMemType{
  JMT_RO,
  JMT_RW,
  JMT_RWE
};


class ObJitMemoryBlock;
class ObJitMemoryGroup
{
public:
  ObJitMemoryGroup()
      : header_(nullptr),
        tailer_(nullptr),
        block_cnt_(0),
        used_(0),
        total_(0)
  {
  }
  ~ObJitMemoryGroup() { free(); }

  //分配时遍历block_list, 如果有可用大小的block, 则直接从block中获取内存
  void *alloc_align(int64_t sz, int64_t align, int64_t p_flags = PROT_READ | PROT_WRITE);
  int finalize(int64_t p_flags);
  //free all
  void free();
  void reset();
  void reserve(int64_t sz, int64_t align, int64_t p_flags);

  DECLARE_TO_STRING;
private:
  ObJitMemoryBlock *alloc_new_block(int64_t sz, int64_t p_flags);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJitMemoryGroup);

private:
  ObJitMemoryBlock *header_;
  ObJitMemoryBlock *tailer_;
  int64_t block_cnt_;       // number of block allocated
  int64_t used_;        // total number of bytes allocated by users
  int64_t total_;       // total number of bytes occupied by pages
};

class ObJitAllocator
{
public:
  ObJitAllocator()
      : code_mem_(),
        rw_data_mem_(),
        ro_data_mem_()
  {}

  void *alloc(const JitMemType mem_type, int64_t sz, int64_t align);
  void free();
  bool finalize();
  void reserve(const JitMemType mem_type, int64_t sz, int64_t align);

private:
  ObJitMemoryGroup code_mem_;
  ObJitMemoryGroup rw_data_mem_;
  ObJitMemoryGroup ro_data_mem_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObJitAllocator);
};

}  // core
}  // jit
}  // oceanbase

#endif /* OB_JIT_ALLOCATOR_H */
