/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PARSER


#include "parser_proxy_func.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_allocator.h"


using namespace oceanbase::common;

static __thread int errcode_for_allocator = 0;

int get_errcode_for_allocator()
{
  return  errcode_for_allocator;
}

void reset_errcode_for_allocator()
{
  errcode_for_allocator = 0;
}

void set_errcode_for_allocator(int errcode)
{
  errcode_for_allocator = errcode;
}

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
      errcode_for_allocator = ret;
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