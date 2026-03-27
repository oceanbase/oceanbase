/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBUNWIND_BT_H_
#define OCEANBASE_LIBUNWIND_BT_H_

#include <inttypes.h>
#include "lib/utility/ob_macro_utils.h"

EXTERN_C_BEGIN
extern int safe_backtrace(char *buf, int64_t len, int64_t *pos);
EXTERN_C_END

#endif
