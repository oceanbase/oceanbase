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

#ifndef CO_CONTEXT_H
#define CO_CONTEXT_H

#include <stdint.h>
#include <cstdlib>
#include "lib/coro/co_config.h"
#include "lib/coro/context/fcontext.hpp"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace lib {

using FContext = boost::context::detail::fcontext_t;
class CoCtx {
private:
  static constexpr uint64_t MAX_CO_LOCAL_STORE_SIZE = 512;

public:
  // Create a new co-routine context
  //
  // @param ss_size stack size of created context
  // @parem cc the new context
  // @return OB_SUCCESS if it executes successfully
  //
  static int create(const uint64_t tenant_id, size_t ss_size, CoCtx* cc);

  // Delete a context and free its memory
  //
  // @param cc the context
  //
  static void destroy(CoCtx* cc);

public:
  int init(const uint64_t tenant_id, const size_t ss_size);
  FContext& get_ctx();
  const FContext& get_ctx() const;
  char* get_crls_buffer();
  void get_stack(void*& stackaddr, size_t& stacksize);
  char* get_local_store()
  {
    return local_store_;
  }

  CoCtx() : ctx_(), ss_sp_(nullptr), ss_size_(0)
  {}

private:
  FContext ctx_;
  char* ss_sp_;
  size_t ss_size_;
  char local_store_[MAX_CO_LOCAL_STORE_SIZE];
};

OB_INLINE FContext& CoCtx::get_ctx()
{
  return ctx_;
}

OB_INLINE const FContext& CoCtx::get_ctx() const
{
  return ctx_;
}

OB_INLINE char* CoCtx::get_crls_buffer()
{
  return &ss_sp_[ss_size_];
}

OB_INLINE void CoCtx::get_stack(void*& stackaddr, size_t& stacksize)
{
  stackaddr = ss_sp_;
  stacksize = ss_size_;
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_CONTEXT_H */
