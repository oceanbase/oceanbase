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

#ifndef ZSTD_ERRORS_H_398273423
#define ZSTD_ERRORS_H_398273423

#if defined (__cplusplus)
extern "C" {
#endif

/*===== dependency =====*/
#include <stddef.h>   /* size_t */


/*-****************************************
*  error codes list
******************************************/
typedef enum {
  ZSTD_error_no_error,
  ZSTD_error_GENERIC,
  ZSTD_error_prefix_unknown,
  ZSTD_error_version_unsupported,
  ZSTD_error_parameter_unknown,
  ZSTD_error_frameParameter_unsupported,
  ZSTD_error_frameParameter_unsupportedBy32bits,
  ZSTD_error_frameParameter_windowTooLarge,
  ZSTD_error_compressionParameter_unsupported,
  ZSTD_error_init_missing,
  ZSTD_error_memory_allocation,
  ZSTD_error_stage_wrong,
  ZSTD_error_dstSize_tooSmall,
  ZSTD_error_srcSize_wrong,
  ZSTD_error_corruption_detected,
  ZSTD_error_checksum_wrong,
  ZSTD_error_tableLog_tooLarge,
  ZSTD_error_maxSymbolValue_tooLarge,
  ZSTD_error_maxSymbolValue_tooSmall,
  ZSTD_error_dictionary_corrupted,
  ZSTD_error_dictionary_wrong,
  ZSTD_error_maxCode
} ZSTD_ErrorCode;

/*! ZSTD_getErrorCode() :
    convert a `size_t` function result into a `ZSTD_ErrorCode` enum type,
    which can be used to compare directly with enum list published into "error_public.h" */
ZSTD_ErrorCode ZSTD_getErrorCode(size_t functionResult);
const char* ZSTD_getErrorString(ZSTD_ErrorCode code);


#if defined (__cplusplus)
}
#endif

#endif /* ZSTD_ERRORS_H_398273423 */
