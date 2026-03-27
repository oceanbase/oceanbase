/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBLIB_CO_VAR_H
#define OBLIB_CO_VAR_H

#include <stdint.h>
#include <cstdlib>

// RLOCAL_EXTERN -- declaration of extern CoVar in header file
// RLOCAL_STATIC -- declaration of static class member CoVar
// _RLOCAL       -- defination of static and extern CoVar
// RLOCAL        -- defination of local CoVar
// RLOCAL_INLINE -- defination of local CoVar in INLINE/template/static function

// TYPE must not be array, pls use Wrapper.

template<int N> using ByteBuf = char[N];

#define RLOCAL_EXTERN(TYPE, VAR) extern thread_local TYPE VAR
#define RLOCAL_STATIC(TYPE, VAR) static thread_local TYPE VAR
#define _RLOCAL(TYPE, VAR) thread_local TYPE VAR
#define RLOCAL(TYPE, VAR) thread_local TYPE VAR
#define RLOCAL_INLINE(TYPE, VAR) thread_local TYPE VAR
#define RLOCAL_INIT(TYPE, VAR, INIT) thread_local TYPE VAR = INIT

#endif /* OBLIB_CO_VAR_H */
