/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_abort.h"
#include "lib/ob_define.h"

void ob_abort (void) __THROW
{
  fprintf(stderr, "OB_ABORT, tid: %ld, lbt: %s\n", GETTID(), oceanbase::common::lbt());
  abort();
}
