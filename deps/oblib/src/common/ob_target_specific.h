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
};

bool is_arch_supported(ObTargetArch arch);

#if defined(__GNUC__) && defined(__x86_64__)

#define OB_USE_MULTITARGET_CODE 1

#if defined(__clang__)

#define OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl")))
#define OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2")))
#define OB_AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx")))
#define OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt")))
#define OB_DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define OB_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl\"))),apply_to=function)")
#   define OB_BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2\"))),apply_to=function)")
#   define OB_BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx\"))),apply_to=function)")
#   define OB_BEGIN_SSE42_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt\"))),apply_to=function)")
#   define OB_END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")

#   define OB_DUMMY_FUNCTION_DEFINITION [[maybe_unused]] void _dummy_function_definition();
#else

#define OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,tune=native")))
#define OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native")))
#define OB_AVX_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,tune=native")))
#define OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt",tune=native)))
#define OB_DEFAULT_FUNCTION_SPECIFIC_ATTRIBUTE

#   define OB_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,tune=native\")")
#   define OB_BEGIN_AVX2_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native\")")
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

#else

#define OB_USE_MULTITARGET_CODE 0

/* Multitarget code is disabled, just delete target-specific code.
 */
#define OB_DECLARE_SSE42_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX2_SPECIFIC_CODE(...)
#define OB_DECLARE_AVX512_SPECIFIC_CODE(...)

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

OB_DECLARE_DEFAULT_CODE(
  constexpr auto BuildArch = ObTargetArch::Default;
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
  constexpr auto BuildArch = ObTargetArch::SSE42;
)

OB_DECLARE_AVX_SPECIFIC_CODE(
  constexpr auto BuildArch = ObTargetArch::AVX;
)

OB_DECLARE_AVX2_SPECIFIC_CODE(
  constexpr auto BuildArch = ObTargetArch::AVX2;
)

OB_DECLARE_AVX512_SPECIFIC_CODE(
  constexpr auto BuildArch = ObTargetArch::AVX512;
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

OB_INLINE uint32_t get_supported_archs()
{
  uint32_t result = 0;
  if (ObCpuFlagsCache::support_sse42()) {
    result |= static_cast<uint32_t>(ObTargetArch::SSE42);
  }
  if (ObCpuFlagsCache::support_avx()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX);
  }
  if (ObCpuFlagsCache::support_avx2()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX2);
  }
  if (ObCpuFlagsCache::support_avx512()) {
    result |= static_cast<uint32_t>(ObTargetArch::AVX512);
  }
  return result;
}

OB_INLINE bool is_arch_supported(ObTargetArch arch)
{
  static uint32_t arches = get_supported_archs();
  return arch == ObTargetArch::Default || (arches & static_cast<uint32_t>(arch));
}

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_COMMON_OB_TARGET_SPECIFIC_H_
