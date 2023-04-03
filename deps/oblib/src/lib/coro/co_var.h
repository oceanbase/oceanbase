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
