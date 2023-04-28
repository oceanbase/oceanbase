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

#ifndef LIB_UTILITY_OB_TEMPLATE_UTILS_
#define LIB_UTILITY_OB_TEMPLATE_UTILS_
#include <functional>
namespace oceanbase
{
namespace common
{
template <bool c>
struct BoolType
{
  static const bool value = c;
};
typedef BoolType<false> FalseType;
typedef BoolType<true> TrueType;

template <typename T>
struct IsPointer : public FalseType
{ };

template <typename T>
struct IsPointer<T*>: public TrueType
{ };


// std::conditional needed c++1x
template <bool t, typename T, typename F>
struct Conditional;
template <typename T, typename F>
struct Conditional<true, T, F>
{
  typedef T type;
};
template <typename T, typename F>
struct Conditional<false, T, F>
{
  typedef F type;
};

} // end namespace common
} // end namespace oceanbase

#define DEFINE_HAS_MEMBER(member) \
  namespace oceanbase \
  { \
  namespace common \
  { \
  template <typename T, typename F = void> \
  struct __has_##member##__ \
  { \
    typedef char yes[1]; \
    typedef char no [2]; \
    \
    template <typename _1> \
    static yes &chk(typename std::enable_if<std::is_void<F>::value, __typeof__(&_1::member) >::type); \
    \
    template <typename _1> \
    static yes &chk(decltype(std::mem_fn<F>(&_1::member))*); \
    \
    template <typename> \
    static no  &chk(...); \
    \
    static bool const value = sizeof(chk<T>(0)) == sizeof(yes); \
  }; \
  } \
  }

#define HAS_MEMBER(type, member, ...) \
  oceanbase::common::__has_##member##__<type, ##__VA_ARGS__>::value

#define HAS_HASH(type) \
  HAS_MEMBER(type, hash) ||\
  HAS_MEMBER(type, hash, uint64_t() const) ||\
  HAS_MEMBER(type, hash, int(uint64_t&) const) ||\
  HAS_MEMBER(type, hash, int(uint64_t&, uint64_t) const)

DEFINE_HAS_MEMBER(MAX_PRINTABLE_SIZE)
DEFINE_HAS_MEMBER(to_cstring)
DEFINE_HAS_MEMBER(reset)
DEFINE_HAS_MEMBER(get_serialize_size)
DEFINE_HAS_MEMBER(to_string)
DEFINE_HAS_MEMBER(hash)
DEFINE_HAS_MEMBER(Response)
DEFINE_HAS_MEMBER(serialize)
DEFINE_HAS_MEMBER(deserialize)
DEFINE_HAS_MEMBER(get_copy_assign_ret)
DEFINE_HAS_MEMBER(set_allocator)

// CompileAssert is an implementation detail of COMPILE_ASSERT
template <bool>
struct CompileAssert
{
};

// STATIC_ASSERT: A poor man's static_assert.  This doesn't handle
// condition expressions that contain unparenthesized top-level commas;
// write STATIC_ASSERT((expr), "comment") when needed.
#define PRIVATE_CAT_IMMEDIATE(a, b) a ## b
#define PRIVATE_CAT(a, b) PRIVATE_CAT_IMMEDIATE(a, b)
#define OLD_STATIC_ASSERT(expr, ignored) \
  typedef CompileAssert<(static_cast<bool>(expr))> \
  PRIVATE_CAT(static_assert_failed_at_line, __LINE__)[bool(expr) ? 1 : -1] __attribute__((unused))

#define STATIC_ASSERT(expr, message) static_assert(expr, message)

#endif /* LIB_UTILITY_OB_TEMPLATE_UTILS_ */
