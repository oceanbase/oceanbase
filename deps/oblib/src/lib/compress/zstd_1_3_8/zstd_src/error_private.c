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

#include "error_private.h"

const char* ERR_getErrorString(ERR_enum code)
{
#ifdef ZSTD_STRIP_ERROR_STRINGS
    (void)code;
    return "Error strings stripped";
#else
    static const char* const notErrorCode = "Unspecified error code";
    switch( code )
    {
    case PREFIX(no_error): return "No error detected";
    case PREFIX(GENERIC):  return "Error (generic)";
    case PREFIX(prefix_unknown): return "Unknown frame descriptor";
    case PREFIX(version_unsupported): return "Version not supported";
    case PREFIX(frameParameter_unsupported): return "Unsupported frame parameter";
    case PREFIX(frameParameter_windowTooLarge): return "Frame requires too much memory for decoding";
    case PREFIX(corruption_detected): return "Corrupted block detected";
    case PREFIX(checksum_wrong): return "Restored data doesn't match checksum";
    case PREFIX(parameter_unsupported): return "Unsupported parameter";
    case PREFIX(parameter_outOfBound): return "Parameter is out of bound";
    case PREFIX(init_missing): return "Context should be init first";
    case PREFIX(memory_allocation): return "Allocation error : not enough memory";
    case PREFIX(workSpace_tooSmall): return "workSpace buffer is not large enough";
    case PREFIX(stage_wrong): return "Operation not authorized at current processing stage";
    case PREFIX(tableLog_tooLarge): return "tableLog requires too much memory : unsupported";
    case PREFIX(maxSymbolValue_tooLarge): return "Unsupported max Symbol Value : too large";
    case PREFIX(maxSymbolValue_tooSmall): return "Specified maxSymbolValue is too small";
    case PREFIX(dictionary_corrupted): return "Dictionary is corrupted";
    case PREFIX(dictionary_wrong): return "Dictionary mismatch";
    case PREFIX(dictionaryCreation_failed): return "Cannot create Dictionary from provided samples";
    case PREFIX(dstSize_tooSmall): return "Destination buffer is too small";
    case PREFIX(srcSize_wrong): return "Src size is incorrect";
    case PREFIX(dstBuffer_null): return "Operation on NULL destination buffer";
        /* following error codes are not stable and may be removed or changed in a future version */
    case PREFIX(frameIndex_tooLarge): return "Frame index is too large";
    case PREFIX(seekableIO): return "An I/O error occurred when reading/seeking";
    case PREFIX(maxCode):
    default: return notErrorCode;
    }
#endif
}
