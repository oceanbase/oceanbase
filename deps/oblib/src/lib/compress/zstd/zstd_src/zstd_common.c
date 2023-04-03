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

#include <stdlib.h>         /* malloc */
#include "error_private.h"
#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"           /* declaration of ZSTD_isError, ZSTD_getErrorName, ZSTD_getErrorCode, ZSTD_getErrorString, ZSTD_versionNumber */
#include "zbuff.h"          /* declaration of ZBUFF_isError, ZBUFF_getErrorName */


/*-****************************************
*  Version
******************************************/
unsigned ZSTD_versionNumber (void) { return ZSTD_VERSION_NUMBER; }


/*-****************************************
*  ZSTD Error Management
******************************************/
/*! ZSTD_isError() :
*   tells if a return value is an error code */
unsigned ZSTD_isError(size_t code) { return ERR_isError(code); }

/*! ZSTD_getErrorName() :
*   provides error code string from function result (useful for debugging) */
const char* ZSTD_getErrorName(size_t code) { return ERR_getErrorName(code); }

/*! ZSTD_getError() :
*   convert a `size_t` function result into a proper ZSTD_errorCode enum */
ZSTD_ErrorCode ZSTD_getErrorCode(size_t code) { return ERR_getErrorCode(code); }

/*! ZSTD_getErrorString() :
*   provides error code string from enum */
const char* ZSTD_getErrorString(ZSTD_ErrorCode code) { return ERR_getErrorName(code); }


/* **************************************************************
*  ZBUFF Error Management
****************************************************************/
unsigned ZBUFF_isError(size_t errorCode) { return ERR_isError(errorCode); }

const char* ZBUFF_getErrorName(size_t errorCode) { return ERR_getErrorName(errorCode); }



/*=**************************************************************
*  Custom allocator
****************************************************************/
/* default uses stdlib */
void* ZSTD_defaultAllocFunction(void* opaque, size_t size)
{
    void* address = malloc(size);
    (void)opaque;
    return address;
}

void ZSTD_defaultFreeFunction(void* opaque, void* address)
{
    (void)opaque;
    free(address);
}

void* ZSTD_malloc(size_t size, ZSTD_customMem customMem)
{
    return customMem.customAlloc(customMem.opaque, size);
}

void ZSTD_free(void* ptr, ZSTD_customMem customMem)
{
    if (ptr!=NULL)
        customMem.customFree(customMem.opaque, ptr);
}
