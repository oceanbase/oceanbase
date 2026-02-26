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
#ifndef TC_INFO
#define TC_INFO(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#endif

#ifndef ALLOCATE_QDTABLE
#define ALLOCATE_QDTABLE(size, name) static_cast<void**>(new (std::nothrow) void*[size])
#endif

#ifndef CACHE_ALIGNED
#define CACHE_ALIGNED __attribute__((aligned(64)))
#endif

#ifndef structof
#define structof(p, T, m) (T*)((char*)p - __builtin_offsetof(T, m))
#endif

#ifndef MAX_CPU_NUM
#define MAX_CPU_NUM 128
#endif

#define MAX_N_CHAN 16
#ifndef tc_itid
int next_itid_ = 0;
__thread int itid_ = -1;
int tc_itid() {
  if (itid_ >= 0) return itid_;
  itid_ = ATOMIC_FAA(&next_itid_, 1);
  return itid_;
}
#define tc_itid tc_itid
#endif
#define arrlen(x) (sizeof(x)/sizeof(x[0]))
