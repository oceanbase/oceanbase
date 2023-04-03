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

#define USING_LOG_PREFIX SQL_PARSER

#include "parser_proxy_func.h"

#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_allocator.h"


using namespace oceanbase::common;

void *parser_alloc_buffer(void *malloc_pool, const int64_t alloc_size)
{
  int ret = OB_SUCCESS;
  void *alloced_buf = NULL;
  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid malloc pool", K(ret));
  } else {
    ObIAllocator *allocator = static_cast<ObIAllocator *>(malloc_pool);
    if (OB_ISNULL(alloced_buf = allocator->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(alloc_size));
    } else {
      // do nothing
    }
  }
  return alloced_buf;
}

void parser_free_buffer(void *malloc_pool, void *buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null malloc pool", K(ret));
  } else {
    ObIAllocator *allocator = static_cast<ObIAllocator *>(malloc_pool);
    allocator->free(buffer);
  }
}