/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_LIBEASY_MEM_POOL_H
#define OB_LIBEASY_MEM_POOL_H

#include <cstdlib>

namespace oceanbase
{
namespace common
{
void *ob_easy_realloc(void *ptr, size_t size);
}
}

#endif
