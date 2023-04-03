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

#include <sys/mman.h>

#if 0
struct MProtect
{
  int protect(int flag) {
    uint64_t page_size = 4096;
    return mprotect((void*)(~(page_size - 1) & ((int64_t)buf_)), page_size, flag);
  }
  char buf_[0];
} __attribute__((aligned(4096)));
struct MProtectGuard
{
  explicit MProtectGuard(MProtect& host): host_(host) {
    host.protect(PROT_READ | PROT_WRITE);
  }
  ~MProtectGuard(){
    host_.protect(PROT_READ);
  }
  MProtect& host_;
};
#else
struct MProtect
{};

struct MProtectGuard
{
  explicit MProtectGuard(MProtect& host) { (void)host; }
  ~MProtectGuard(){}
};
#endif
