/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
