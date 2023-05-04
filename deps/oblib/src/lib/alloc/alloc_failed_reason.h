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

#ifndef _OCEABASE_LIB_ALLOC_FAILED_REASON_H_
#define _OCEABASE_LIB_ALLOC_FAILED_REASON_H_

#include <stdint.h>
#include "lib/coro/co_var.h"

namespace oceanbase
{
namespace lib
{
enum AllocFailedReason
{
  UNKNOWN = 0,
  SINGLE_ALLOC_SIZE_OVERFLOW,
  CTX_HOLD_REACH_LIMIT,
  TENANT_HOLD_REACH_LIMIT,
  SERVER_HOLD_REACH_LIMIT,
  PHYSICAL_MEMORY_EXHAUST
};

struct AllocFailedCtx
{
public:
  int reason_;
  int64_t alloc_size_;
  union
  {
    int errno_;
    struct {
      int64_t ctx_id_;
      int64_t ctx_hold_;
      int64_t ctx_limit_;
    };
    struct {
      uint64_t tenant_id_;
      int64_t tenant_hold_;
      int64_t tenant_limit_;
    };
    struct {
      int64_t server_hold_;
      int64_t server_limit_;
    };
  };
  bool need_wash() const
  {
    return reason_ == lib::CTX_HOLD_REACH_LIMIT ||
            reason_ == lib::TENANT_HOLD_REACH_LIMIT ||
            reason_ == lib::SERVER_HOLD_REACH_LIMIT;
  }
};

char *alloc_failed_msg();

AllocFailedCtx &g_alloc_failed_ctx();

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_FAILED_REASON_H_ */
