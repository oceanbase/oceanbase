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

#endif // OCEANBASE_COMMON_OB_ABORT_H_
