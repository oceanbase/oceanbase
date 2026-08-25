/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
const int OBP_ERR_UNEXPECTED = -4016;
const int OBP_ENTRY_EXIST = -4017;
const int OBP_ENTRY_NOT_EXIST = -4018;
const int OBP_SIZE_OVERFLOW = -4019;
const int OBP_PLUGIN_ERROR = -11078;

#ifdef __cplusplus
} // extern "C"
#endif

/** @} */
