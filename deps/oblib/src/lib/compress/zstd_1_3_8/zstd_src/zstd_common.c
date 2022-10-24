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

#include <stdlib.h>      /* malloc, calloc, free */
#include <string.h>      /* memset */
#include "error_private.h"
#include "zstd_internal.h"


/*-****************************************
*  Version
******************************************/
unsigned ZSTD_versionNumber(void) { return ZSTD_VERSION_NUMBER; }

const char* ZSTD_versionString(void) { return ZSTD_VERSION_STRING; }


/*-****************************************
*  ZSTD Error Management
******************************************/
#undef ZSTD_isError   /* defined within zstd_internal.h */
/*! ZSTD_isError() :
 *  tells if a return value is an error code
 *  symbol is required for external callers */
unsigned ZSTD_isError(size_t code) { return ERR_isError(code); }

/*! ZSTD_getErrorName() :
 *  provides error code string from function result (useful for debugging) */
const char* ZSTD_getErrorName(size_t code) { return ERR_getErrorName(code); }

/*! ZSTD_getError() :
 *  convert a `size_t` function result into a proper ZSTD_errorCode enum */
ZSTD_ErrorCode ZSTD_getErrorCode(size_t code) { return ERR_getErrorCode(code); }

/*! ZSTD_getErrorString() :
 *  provides error code string from enum */
const char* ZSTD_getErrorString(ZSTD_ErrorCode code) { return ERR_getErrorString(code); }



/*=**************************************************************
*  Custom allocator
****************************************************************/
void* ZSTD_malloc(size_t size, ZSTD_customMem customMem)
{
    if (customMem.customAlloc)
        return customMem.customAlloc(customMem.opaque, size);
    return malloc(size);
}

void* ZSTD_calloc(size_t size, ZSTD_customMem customMem)
{
    if (customMem.customAlloc) {
        /* calloc implemented as malloc+memset;
         * not as efficient as calloc, but next best guess for custom malloc */
        void* const ptr = customMem.customAlloc(customMem.opaque, size);
        memset(ptr, 0, size);
        return ptr;
    }
    return calloc(1, size);
}

void ZSTD_free(void* ptr, ZSTD_customMem customMem)
{
    if (ptr!=NULL) {
        if (customMem.customFree)
            customMem.customFree(customMem.opaque, ptr);
        else
            free(ptr);
    }
}
