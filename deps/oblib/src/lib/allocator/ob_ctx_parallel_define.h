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

#ifndef OCEANBASE_COMMON_CTX_PARALLEL_DEFINE_H_
#define OCEANBASE_COMMON_CTX_PARALLEL_DEFINE_H_

#include "lib/allocator/ob_mod_define.h"

namespace oceanbase {
namespace common {
struct ObCtxParallel {
public:
  ObCtxParallel()
  {
    for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
      parallel_[i] = DEFAULT_CTX_PARALLEL;
    }
#define CTX_PARALLEL_DEF(name, parallel) parallel_[ObCtxIds::name] = parallel;
    CTX_PARALLEL_DEF(DEFAULT_CTX_ID, 32)
    CTX_PARALLEL_DEF(LIBEASY, 8)
    CTX_PARALLEL_DEF(PLAN_CACHE_CTX_ID, 4)
    CTX_PARALLEL_DEF(LOGGER_CTX_ID, 1)
#undef CTX_PARALLEL_DEF
  }
  static ObCtxParallel& instance();
  int parallel_of_ctx(int64_t ctx_id) const
  {
    int p = 0;
    if (ctx_id >= 0 && ctx_id < ObCtxIds::MAX_CTX_ID) {
      p = parallel_[ctx_id];
    }
    return p;
  }

private:
  const static int DEFAULT_CTX_PARALLEL = 8;
  int parallel_[ObCtxIds::MAX_CTX_ID];
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_CTX_PARALLEL_DEFINE_H_
