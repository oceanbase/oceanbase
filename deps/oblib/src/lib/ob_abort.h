/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_ABORT_H_
#define OCEANBASE_COMMON_OB_ABORT_H_
#include <stdlib.h>
#include "lib/utility/ob_macro_utils.h"

extern void ob_abort (void) __THROW;

// we use int instead of bool to compatible with c code.
#ifdef __cplusplus
static inline void abort_unless(bool result)
#else
static inline void abort_unless(int result)
#endif
{
  if (OB_UNLIKELY(!result)) {
    ob_abort();
  }
}
#ifdef ENABLE_DEBUG_ASSERT
#define DEBUG_ASSERT(result) abort_unless(result)
#else
#define DEBUG_ASSERT(result)
#endif

#endif // OCEANBASE_COMMON_OB_ABORT_H_
