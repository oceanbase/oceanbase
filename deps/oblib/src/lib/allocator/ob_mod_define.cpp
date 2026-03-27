/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "deps/oblib/src/lib/allocator/ob_slice_alloc.h"

namespace oceanbase
{
namespace common
{

#define LABEL_ITEM_DEF(name, ...) constexpr const char ObModIds::name[];
#include "lib/allocator/ob_mod_define.h"
#undef LABEL_ITEM_DEF

bool ObCtxInfo::is_valid_ctx_name(const char *ctx_name, uint64_t& ctx_id) const
{
  bool ret = false;
  for (int i = 0; i < ObCtxIds::MAX_CTX_ID && !ret; ++i) {
    if (0 == strcmp(ctx_names_[i], ctx_name)) {
      ret = true;
      ctx_id = i;
    }
  }
  return ret;
}

}; // end namespace common
}; // end namespace oceanbase
