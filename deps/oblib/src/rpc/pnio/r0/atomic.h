/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#define BARRIER() asm volatile(""::: "memory")
#define MBARRIER() __sync_synchronize()

#if defined(__x86_64__)
#define SPIN_PAUSE() asm("pause\n")
#elif defined(__aarch64__)
#define SPIN_PAUSE() asm("yield\n")
#endif

#define LOAD(x) __atomic_load_n((x), __ATOMIC_ACQUIRE)
#define STORE(x, v) __atomic_store_n((x), (v), __ATOMIC_RELEASE)

#define FAA(val, addv) __sync_fetch_and_add((val), (addv))
#define AAF(val, addv) __sync_add_and_fetch((val), (addv))
#define TAS(val, newv) __sync_lock_test_and_set((val), (newv))
#define VCAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#define BCAS(val, cmpv, newv) __sync_bool_compare_and_swap((val), (cmpv), (newv))
#define TAS(val, newv) __sync_lock_test_and_set((val), (newv))
