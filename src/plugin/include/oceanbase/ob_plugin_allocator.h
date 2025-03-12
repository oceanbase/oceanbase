/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "oceanbase/ob_plugin_base.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @addtogroup ObPlugin
 * @{
 */

/**< The allocator paramter */
typedef ObPluginDatum ObPluginAllocatorPtr;

/**< malloc interface without label */
OBP_PUBLIC_API void *obp_malloc(int64_t size);

/**< malloc interface with alignment */
OBP_PUBLIC_API void *obp_malloc_align(int64_t alignment, int64_t size);

/**< free memory */
OBP_PUBLIC_API void obp_free(void *ptr);

/**
 * allocate memory with the specific allocator
 * @details you can get allocator from specific plugin interface, such as FTParser
 * @note A valid allocator always has a memory label
 */
OBP_PUBLIC_API void *obp_allocate(ObPluginAllocatorPtr allocator, int64_t size);

/**< allocate memory with alignment parameter */
OBP_PUBLIC_API void *obp_allocate_align(ObPluginAllocatorPtr allocator, int64_t alignment, int64_t size);

/**< free memory */
OBP_PUBLIC_API void obp_deallocate(ObPluginAllocatorPtr allocator, void *ptr);

/** @} */

#ifdef __cplusplus
} // extern "C"
#endif
