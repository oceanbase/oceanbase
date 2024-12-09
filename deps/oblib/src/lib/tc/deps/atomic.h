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
#ifndef ATOMIC_LOAD
#define ATOMIC_LOAD(x) __atomic_load_n((x), __ATOMIC_ACQUIRE)
#define ATOMIC_STORE(x, v) __atomic_store_n((x), (v), __ATOMIC_RELEASE)
#define ATOMIC_FAA(val, addv) __sync_fetch_and_add((val), (addv))
#define ATOMIC_AAF(val, addv) __sync_add_and_fetch((val), (addv))
#define ATOMIC_TAS(val, newv) __sync_lock_test_and_set((val), (newv))
#define ATOMIC_VCAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#define ATOMIC_BCAS(val, cmpv, newv) __sync_bool_compare_and_swap((val), (cmpv), (newv))
#define PAUSE() __asm__("pause\n")
#endif
