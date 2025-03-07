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

/**
 * @addtogroup ObPlugin
 * @{
 */

/**
 * this is the adaptor errno of oceanbase errno
 */

#ifdef __cplusplus
extern "C" {
#endif

const int OBP_SUCCESS = 0;
const int OBP_INVALID_ARGUMENT = -4002;
const int OBP_INIT_TWICE = -4005;
const int OBP_NOT_INIT = -4006;
const int OBP_NOT_SUPPORTED = -4007;
const int OBP_ITER_END = -4008;
const int OBP_ALLOCATE_MEMORY_FAILED = -4013;
const int OBP_PLUGIN_ERROR = -11078;

#ifdef __cplusplus
} // extern "C"
#endif

/** @} */
