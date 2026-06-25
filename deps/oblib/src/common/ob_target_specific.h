/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_TARGET_SPECIFIC_H_
#define OCEANBASE_COMMON_OB_TARGET_SPECIFIC_H_

#include <stdint.h>
#include "lib/cpu/ob_cpu_topology.h"

namespace oceanbase
{
namespace common
{
enum ObTargetArch : uint32_t
{
  Default  = 0,
  SSE42    = (1 << 0),
  AVX      = (1 << 1),
  AVX2     = (1 << 2),
  AVX512    = (1 << 3),
  NEON    = (1 << 4)
};

bool is_arch_supported(ObTargetArch arch);

#if defined(__GNUC__) && defined(__x86_64__)

#define OB_USE_MULTITARGET_CODE 1

#if defined(__clang__)

#define OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,fma")))
#define OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,fma")))
#define OB_AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx")))
#define OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt")))
#define OB_DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define OB_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,fma\"))),apply_to=function)")
#   define OB_BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,fma\"))),apply_to=function)")
#   define OB_BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx\"))),apply_to=function)")
#   define OB_BEGIN_SSE42_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt\"))),apply_to=function)")
#   define OB_END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")

#   define OB_DUMMY_FUNCTION_DEFINITION [[maybe_unused]] void _dummy_function_definition();
#else

#define OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,fma,tune=native")))
#define OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,fma,tune=native")))
#define OB_AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,tune=native")))
#define OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,tune=native")))
#define OB_DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define OB_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,fma,tune=native\")")
#   define OB_BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,fma,tune=native\")")
#   define OB_BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,tune=native\")")
#   define OB_BEGIN_SSE42_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,tune=native\")")
#   define OB_END_TARGET_SPECIFIC_CODE \
        _Pragma("GCC pop_options")

#   define OB_DUMMY_FUNCTION_DEFINITION
#endif

#define OB_DECLARE_SSE42_SPECIFIC_CODE(...) \
OB_BEGIN_SSE42_SPECIFIC_CODE \
namespace specific { \
namespace sse42 { \
  OB_DUMMY_FUNCTION_DEFINITION \
  using namespace oceanbase::common::specific::sse42; \
  __VA_ARGS__ \
} \
} \
OB_END_TARGET_SPECIFIC_CODE

#define OB_DECLARE_AVX_SPECIFIC_CODE(...) \
OB_BEGIN_AVX_SPECIFIC_CODE \
namespace specific { \
namespace avx { \
  OB_DUMMY_FUNCTION_DEFINITION \
  using namespace oceanbase::common::specific::avx; \
  __VA_ARGS__ \
} \
} \
OB_END_TARGET_SPECIFIC_CODE

#define OB_DECLARE_AVX2_SPECIFIC_CODE(...) \
OB_BEGIN_AVX2_SPECIFIC_CODE \
namespace specific { \
namespace avx2 { \
  OB_DUMMY_FUNCTION_DEFINITION \
  using namespace oceanbase::common::specific::avx2; \
  __VA_ARGS__ \
} \
} \
OB_END_TARGET_SPECIFIC_CODE

#define OB_DECLARE_AVX512_SPECIFIC_CODE(...) \
OB_BEGIN_AVX512_SPECIFIC_CODE \
namespace specific { \
namespace avx512 { \
  OB_DUMMY_FUNCTION_DEFINITION \
  using namespace oceanbase::common::specific::avx512; \
  __VA_ARGS__ \
} \
} \
OB_END_TARGET_SPECIFIC_CODE

/* NEON is not available on x86_64, define empty macro */
#define OB_DECLARE_NEON_SPECIFIC_CODE(...)
#define OB_ARM_USE_MULTITARGET_CODE 0

#elif defined(__GNUC__) && defined(__aarch64__)

#if defined(__ARM_NEON)

#define OB_ARM_USE_MULTITARGET_CODE 1

#define OB_DECLARE_NEON_SPECIFIC_CODE(...) \
namespace specific { \
namespace neon { \
  using namespace oceanbase::common::specific::neon; \
  __VA_ARGS__ \
} \
} \

#else

#define OB_DECLARE_NEON_SPECIFIC_CODE(...)

#endif

/* x86 instructions are not available on aarch64, define empty macros */
#define OB_DECLARE_SSE42_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX2_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX512_SPECIFIC_CODE(...)
#define OB_USE_MULTITARGET_CODE 0

#else

/* Multitarget code is disabled, just delete target-specific code.
 */
#define OB_DECLARE_SSE42_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX2_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX512_SPECIFIC_CODE(...)
#define OB_DECLARE_NEON_SPECIFIC_CODE(...)
#define OB_USE_MULTITARGET_CODE 0
#define OB_ARM_USE_MULTITARGET_CODE 0

#endif

#define OB_DECLARE_DEFAULT_CODE(...) \
namespace specific { \
namespace normal { \
  using namespace oceanbase::common::specific::normal; \
  __VA_ARGS__ \
} \
}

#define OB_DECLARE_MULTITARGET_CODE(...) \
OB_DECLARE_DEFAULT_CODE         (__VA_ARGS__) \
OB_DECLARE_SSE42_SPECIFIC_CODE  (__VA_ARGS__) \
OB_DECLARE_AVX_SPECIFIC_CODE    (__VA_ARGS__) \
OB_DECLARE_AVX2_SPECIFIC_CODE   (__VA_ARGS__) \
OB_DECLARE_AVX512_SPECIFIC_CODE (__VA_ARGS__) \

#define OB_DECLARE_DEFAULT_AND_AVX2_CODE(...) \
OB_DECLARE_DEFAULT_CODE       (__VA_ARGS__) \
OB_DECLARE_AVX2_SPECIFIC_CODE (__VA_ARGS__)

#define OB_DECLARE_DEFAULT_AND_AVX512_CODE(...) \
OB_DECLARE_DEFAULT_CODE         (__VA_ARGS__) \
OB_DECLARE_AVX512_SPECIFIC_CODE (__VA_ARGS__)

// OB_DISPATCH_MULTITARGET_CODE
// AVX512 is not dispatched here to avoid turbo downclock on older Intel Xeon.
#if OB_USE_MULTITARGET_CODE
#define OB_DISPATCH_MULTITARGET_CODE(func, ...)                                   \
  (oceanbase::common::is_arch_supported(oceanbase::common::ObTargetArch::AVX2)    \
     ? oceanbase::common::specific::avx2::func(__VA_ARGS__)                       \
   : oceanbase::common::is_arch_supported(oceanbase::common::ObTargetArch::SSE42) \
     ? oceanbase::common::specific::sse42::func(__VA_ARGS__)                      \
     : oceanbase::common::specific::normal::func(__VA_ARGS__))
#elif OB_ARM_USE_MULTITARGET_CODE
#define OB_DISPATCH_MULTITARGET_CODE(func, ...)                                \
  (oceanbase::common::is_arch_supported(oceanbase::common::ObTargetArch::NEON) \
     ? oceanbase::common::specific::neon::func(__VA_ARGS__)                    \
     : oceanbase::common::specific::normal::func(__VA_ARGS__))
#else
#define OB_DISPATCH_MULTITARGET_CODE(func, ...) \
  (oceanbase::common::specific::normal::func(__VA_ARGS__))
#endif

// Hook used by OB_UNITTEST_MULTITARGET_CODE to record the current target arch in unittest traces.
// Override before including this header.
// e.g.(gtest):
//   #define OB_UNITTEST_MULTITARGET_CODE_SCOPED_TRACE(msg) SCOPED_TRACE(msg)
#ifndef OB_UNITTEST_MULTITARGET_CODE_SCOPED_TRACE
#define OB_UNITTEST_MULTITARGET_CODE_SCOPED_TRACE(msg) ((void)0)
#endif

// OB_UNITTEST_MULTITARGET_CODE
// Unittest helper: expand statements for each compiled and CPU-supported target.
// Use NS:: in body to call the per-target implementation in oceanbase::common::specific::*.
// e.g.(gtest):
//   OB_UNITTEST_MULTITARGET_CODE(
//     EXPECT_EQ(ref, NS::popcnt_bytes(data, sz));
//   )
#define OB_UNITTEST_MULTITARGET_CODE_IMPL_(arch, ns, ...)                              \
  do {                                                                                 \
    if (oceanbase::common::is_arch_supported(oceanbase::common::ObTargetArch::arch)) { \
      namespace NS = oceanbase::common::specific::ns;                                  \
      OB_UNITTEST_MULTITARGET_CODE_SCOPED_TRACE("target=" #ns);                        \
      __VA_ARGS__                                                                      \
    }                                                                                  \
  } while (0);

#if OB_USE_MULTITARGET_CODE
#define OB_UNITTEST_MULTITARGET_CODE(...)                          \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(Default, normal, __VA_ARGS__) \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(SSE42, sse42, __VA_ARGS__)    \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(AVX2, avx2, __VA_ARGS__)      \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(AVX512, avx512, __VA_ARGS__)
#elif OB_ARM_USE_MULTITARGET_CODE
#define OB_UNITTEST_MULTITARGET_CODE(...)                          \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(Default, normal, __VA_ARGS__) \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(NEON, neon, __VA_ARGS__)
#else
#define OB_UNITTEST_MULTITARGET_CODE(...) \
  OB_UNITTEST_MULTITARGET_CODE_IMPL_(Default, normal, __VA_ARGS__)
#endif

OB_DECLARE_DEFAULT_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::Default;
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::SSE42;
)

OB_DECLARE_AVX_SPECIFIC_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::AVX;
)

OB_DECLARE_AVX2_SPECIFIC_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::AVX2;
)

OB_DECLARE_AVX512_SPECIFIC_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::AVX512;
)

OB_DECLARE_NEON_SPECIFIC_CODE(
  constexpr ObTargetArch BuildArch = ObTargetArch::NEON;
)

#define OB_MULTITARGET_FUNCTION_HEADER(...) __VA_ARGS__

#define OB_MULTITARGET_FUNCTION_BODY(...) __VA_ARGS__

#if defined(__GNUC__) && defined(__x86_64__)

#define OB_MULTITARGET_FUNCTION_AVX2_SSE42(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##_avx2 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE \
    name##_sse42 \
    FUNCTION_BODY \
    \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#else

#define OB_MULTITARGET_FUNCTION_AVX2_SSE42(FUNCTION_HEADER, name, FUNCTION_BODY) \
    FUNCTION_HEADER \
    \
    name \
    FUNCTION_BODY \

#endif

void init_arches();
extern uint32_t arches;

OB_INLINE bool is_arch_supported(ObTargetArch arch)
{
  return arch == ObTargetArch::Default || (arches & static_cast<uint32_t>(arch));
}

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_COMMON_OB_TARGET_SPECIFIC_H_
