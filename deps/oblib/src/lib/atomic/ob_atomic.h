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

#ifndef _OB_ATOMIC_H_
#define _OB_ATOMIC_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/profile/ob_atomic_event.h"
namespace oceanbase
{
namespace common
{
#define OB_GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)

#define __COMPILER_BARRIER() asm volatile("" ::: "memory")
#define MEM_BARRIER() __sync_synchronize()
#if defined(__x86_64__)
#define WEAK_BARRIER() __COMPILER_BARRIER()
#define PAUSE() ({OB_ATOMIC_EVENT(atomic_pause); asm("pause\n");})
#elif defined(__aarch64__)
#define WEAK_BARRIER() __sync_synchronize()
#define PAUSE() ({OB_ATOMIC_EVENT(atomic_pause); asm("yield\n");})  // for ARM
#else
#error arch unsupported
#endif

#define ATOMIC_LOAD(x) __atomic_load_n((x), __ATOMIC_SEQ_CST)
#define ATOMIC_LOAD_ACQ(x) __atomic_load_n((x), __ATOMIC_ACQUIRE)
#define ATOMIC_STORE(x, v) ({ __atomic_store_n((x), (v), __ATOMIC_SEQ_CST);})
#define ATOMIC_STORE_REL(x, v) ({ __atomic_store_n((x), (v), __ATOMIC_RELEASE);})
#define ATOMIC_LOAD64(addr) ({int64_t x = __atomic_load_n((int64_t*)addr, __ATOMIC_SEQ_CST); *(typeof(addr))&x; })
#define ATOMIC_STORE64(addr, v) ({ typeof(v) v1 = v; __atomic_store_n((int64_t*)addr, *(int64_t*)&v1, __ATOMIC_SEQ_CST); })

#define ATOMIC_FAAx(val, addv, id)                              \
    ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_faa); __sync_fetch_and_add((val), (addv)); })
#define ATOMIC_AAFx(val, addv, id)                              \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_aaf); __sync_add_and_fetch((val), (addv)); })
#define ATOMIC_FASx(val, subv, id)                              \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_fas); __sync_fetch_and_sub((val), (subv)); })
#define ATOMIC_SAFx(val, subv, id)                              \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_saf); __sync_sub_and_fetch((val), (subv)); })
#define ATOMIC_TASx(val, newv, id)                              \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_tas); __atomic_exchange_n((val), (newv), __ATOMIC_SEQ_CST); })
#define ATOMIC_SETx(val, newv, id)                              \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_set); __atomic_exchange_n((val), (newv), __ATOMIC_SEQ_CST); })
#define ATOMIC_VCASx(val, cmpv, newv, id)                       \
  ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_vcas);                  \
    __sync_val_compare_and_swap((val), (cmpv), (newv)); })
#define ATOMIC_BCASx(val, cmpv, newv, id)                       \
    ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_bcas);                \
    __sync_bool_compare_and_swap((val), (cmpv), (newv)); })
#define ATOMIC_ANDFx(val, andv, id)                              \
    ({ UNUSED(id); OB_ATOMIC_EVENT(atomic_Aaf); __sync_and_and_fetch((val), (andv)); })

template<typename T>
    struct remove_volatile { using type=T; };
template<typename T>
    struct remove_volatile<volatile T> { using type=T; };
#define LA_ATOMIC_ID 0
#define ATOMIC_FAA(val, addv) ATOMIC_FAAx(val, addv, LA_ATOMIC_ID)
#define ATOMIC_FAA(val, addv) ATOMIC_FAAx(val, addv, LA_ATOMIC_ID)
#define ATOMIC_FAA_AR(val, addv) __atomic_fetch_add(val, addv, __ATOMIC_ACQ_REL)
#define ATOMIC_AAF(val, addv) ATOMIC_AAFx(val, addv, LA_ATOMIC_ID)
#define ATOMIC_AAF_AR(val, addv) __atomic_add_fetch(val, addv, __ATOMIC_ACQ_REL)
#define ATOMIC_FAS(val, subv) ATOMIC_FASx(val, subv, LA_ATOMIC_ID)
#define ATOMIC_SAF(val, subv) ATOMIC_SAFx(val, subv, LA_ATOMIC_ID)
#define ATOMIC_TAS(val, newv) ATOMIC_TASx(val, newv, LA_ATOMIC_ID)
#define ATOMIC_SET(val, newv) ATOMIC_SETx(val, newv, LA_ATOMIC_ID)
#define ATOMIC_VCAS(val, cmpv, newv) ATOMIC_VCASx(val, cmpv, newv, LA_ATOMIC_ID)
#define ATOMIC_VCAS_AR(val, cmpv, newv) ({ remove_volatile<typeof(*(val))>::type cmp = cmpv; __atomic_compare_exchange_n(val, &cmp, newv, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE); cmp;})
#define ATOMIC_BCAS(val, cmpv, newv) ATOMIC_BCASx(val, cmpv, newv, LA_ATOMIC_ID)
#define ATOMIC_BCAS_AR(val, cmpv, newv) ({ remove_volatile<typeof(*(val))>::type cmp = cmpv; __atomic_compare_exchange_n(val, &cmp, newv, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE); })
#define ATOMIC_ANDF(val, andv) ATOMIC_ANDFx(val, andv, LA_ATOMIC_ID)

template <typename T>
inline int64_t inc_update(T* v_, T x)
{
  T ov = 0;
  T nv = ATOMIC_LOAD(v_);
  while ((ov = nv) < x) {
    if ((nv = ATOMIC_VCAS(v_, ov, x)) == ov) {
      nv = x;
    }
  }
  return nv;
}

template <typename T>
inline int64_t dec_update(T* v_, T x)
{
  T ov = 0;
  T nv = ATOMIC_LOAD(v_);
  while ((ov = nv) > x) {
    if ((nv = ATOMIC_VCAS(v_, ov, x)) == ov) {
      nv = x;
    }
  }
  return nv;
}

#define ATOMIC_CAS(val, cmpv, newv) ATOMIC_VCAS((val), (cmpv), (newv))
#define ATOMIC_INC(val) do { IGNORE_RETURN ATOMIC_AAF((val), 1); } while (0)
#define ATOMIC_DEC(val) do { IGNORE_RETURN ATOMIC_SAF((val), 1); } while (0)

}
}

#endif /* _OB_ATOMIC_H_ */
