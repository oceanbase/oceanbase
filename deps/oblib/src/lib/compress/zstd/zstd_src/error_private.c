/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/* The purpose of this file is to have a single list of error strings embedded in binary */

#include "error_private.h"

// Use macro definition to force string segment concatenation
#define ERROR_STRING(name, str) __attribute__((used)) static const char name[] = str;

// Define all error strings
ERROR_STRING(STR_NO_ERROR, "No error detected");
ERROR_STRING(STR_GENERIC, "Error (generic)");
ERROR_STRING(STR_PREFIX_UNKNOWN, "Unknown frame descriptor");
ERROR_STRING(STR_VERSION_UNSUPPORTED, "Version not supported");
ERROR_STRING(STR_PARAMETER_UNKNOWN, "Unknown parameter type");
ERROR_STRING(STR_FRAME_PARAM_UNSUPPORTED, "Unsupported frame parameter");
ERROR_STRING(STR_FRAME_PARAM_32BIT, "Frame parameter unsupported in 32-bits mode");
ERROR_STRING(STR_FRAME_WINDOW_TOO_LARGE, "Frame requires too much memory for decoding");
ERROR_STRING(STR_COMPRESSION_PARAM, "Compression parameter is out of bound");
ERROR_STRING(STR_INIT_MISSING, "Context should be init first");
ERROR_STRING(STR_MEMORY_ALLOCATION, "Allocation error : not enough memory");
ERROR_STRING(STR_STAGE_WRONG, "Operation not authorized at current processing stage");
ERROR_STRING(STR_DST_TOO_SMALL, "Destination buffer is too small");
ERROR_STRING(STR_SRC_SIZE_WRONG, "Src size incorrect");
ERROR_STRING(STR_CORRUPTION_DETECTED, "Corrupted block detected");
ERROR_STRING(STR_CHECKSUM_WRONG, "Restored data doesn't match checksum");
ERROR_STRING(STR_TABLELOG_TOO_LARGE, "tableLog requires too much memory : unsupported");
ERROR_STRING(STR_MAX_SYMBOL_TOO_LARGE, "Unsupported max Symbol Value : too large");
ERROR_STRING(STR_MAX_SYMBOL_TOO_SMALL, "Specified maxSymbolValue is too small");
ERROR_STRING(STR_DICT_CORRUPTED, "Dictionary is corrupted");
ERROR_STRING(STR_DICT_WRONG, "Dictionary mismatch");
ERROR_STRING(STR_NOT_ERROR_CODE, "Unspecified error code");

const char* ERR_getErrorString(ERR_enum code) {
    static const char* const notErrorCode = STR_NOT_ERROR_CODE;
    switch(code) {
        case PREFIX(no_error): return STR_NO_ERROR;
        case PREFIX(GENERIC): return STR_GENERIC;
        case PREFIX(prefix_unknown): return STR_PREFIX_UNKNOWN;
        case PREFIX(version_unsupported): return STR_VERSION_UNSUPPORTED;
        case PREFIX(parameter_unknown): return STR_PARAMETER_UNKNOWN;
        case PREFIX(frameParameter_unsupported): return STR_FRAME_PARAM_UNSUPPORTED;
        case PREFIX(frameParameter_unsupportedBy32bits): return STR_FRAME_PARAM_32BIT;
        case PREFIX(frameParameter_windowTooLarge): return STR_FRAME_WINDOW_TOO_LARGE;
        case PREFIX(compressionParameter_unsupported): return STR_COMPRESSION_PARAM;
        case PREFIX(init_missing): return STR_INIT_MISSING;
        case PREFIX(memory_allocation): return STR_MEMORY_ALLOCATION;
        case PREFIX(stage_wrong): return STR_STAGE_WRONG;
        case PREFIX(dstSize_tooSmall): return STR_DST_TOO_SMALL;
        case PREFIX(srcSize_wrong): return STR_SRC_SIZE_WRONG;
        case PREFIX(corruption_detected): return STR_CORRUPTION_DETECTED;
        case PREFIX(checksum_wrong): return STR_CHECKSUM_WRONG;
        case PREFIX(tableLog_tooLarge): return STR_TABLELOG_TOO_LARGE;
        case PREFIX(maxSymbolValue_tooLarge): return STR_MAX_SYMBOL_TOO_LARGE;
        case PREFIX(maxSymbolValue_tooSmall): return STR_MAX_SYMBOL_TOO_SMALL;
        case PREFIX(dictionary_corrupted): return STR_DICT_CORRUPTED;
        case PREFIX(dictionary_wrong): return STR_DICT_WRONG;
        case PREFIX(maxCode):
        default: return notErrorCode;
    }
}
