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

#ifndef DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_TYPE_TRAIT_H
#define DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_TYPE_TRAIT_H
#include <type_traits>
#include <utility>
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
namespace mds
{
class BufferCtx;
}
}
namespace transaction
{
  class ObMulSourceDataNotifyArg;
}
namespace common
{
namespace meta
{

#define __CONNECT_CLASS_AND_METHOD__(class, method) class.method

#define REGISTER_FUNCTION_TRAIT(function_name) \
template<typename, typename>\
struct has_##function_name {};\
template<typename C, typename Ret, typename... Args>\
struct has_##function_name<C, Ret(Args...)> {\
private:\
  template<typename T>\
  static constexpr auto check(T*)\
  -> typename\
    std::is_same<\
      decltype( \
        __CONNECT_CLASS_AND_METHOD__(std::declval<T>(), function_name)\
        ( std::declval<Args>()... )),\
      Ret\
    >::type;\
  template<typename>\
  static constexpr std::false_type check(...);\
  typedef decltype(check<C>(0)) type;\
public:\
  static constexpr bool value = type::value;\
};

template<typename, typename>
struct is_function_like {};
template<typename C, typename Ret, typename... Args>
struct is_function_like<C, Ret(Args...)> {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype(
        std::declval<T>().operator()( std::declval<Args>()... )),
      Ret
    >::type;
  template<typename>
  static constexpr std::false_type check(...);
  typedef decltype(check<C>(0)) type;
public:
  static constexpr bool value = type::value;
};

template<typename C>
struct has_less_operator {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype(
        std::declval<T>().operator<()(std::declval<T>())),
      bool
    >::type;
  template<typename>
  static constexpr std::false_type check(...);
  typedef decltype(check<C>(0)) type;
public:
  static constexpr bool value = type::value;
};

template<typename C>
struct has_equal_operator {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype(
        std::declval<T>().operator==()(std::declval<T>())),
      bool
    >::type;
  template<typename>
  static constexpr std::false_type check(...);
  typedef decltype(check<C>(0)) type;
public:
  static constexpr bool value = type::value;
};

REGISTER_FUNCTION_TRAIT(init)
REGISTER_FUNCTION_TRAIT(destroy)
REGISTER_FUNCTION_TRAIT(start)
REGISTER_FUNCTION_TRAIT(stop)
REGISTER_FUNCTION_TRAIT(wait)
REGISTER_FUNCTION_TRAIT(assign)
REGISTER_FUNCTION_TRAIT(to_string)
REGISTER_FUNCTION_TRAIT(serialize)
REGISTER_FUNCTION_TRAIT(deserialize)
REGISTER_FUNCTION_TRAIT(get_serialize_size)
REGISTER_FUNCTION_TRAIT(compare)
REGISTER_FUNCTION_TRAIT(check_can_replay_commit)
REGISTER_FUNCTION_TRAIT(on_commit_for_old_mds)

REGISTER_FUNCTION_TRAIT(on_set)
REGISTER_FUNCTION_TRAIT(on_redo)
REGISTER_FUNCTION_TRAIT(on_prepare)
REGISTER_FUNCTION_TRAIT(on_commit)
REGISTER_FUNCTION_TRAIT(on_abort)


#define DECAY(CLASS) typename std::decay<CLASS>::type

// define thread like trait and enable_if macro
#define OB_TRAIT_IS_THREAD_LIKE(CLASS) \
::oceanbase::common::meta::has_start<DECAY(CLASS), int()>::value &&\
::oceanbase::common::meta::has_stop<DECAY(CLASS), void()>::value &&\
::oceanbase::common::meta::has_wait<DECAY(CLASS), void()>::value
#define ENABLE_IF_LIKE_THREAD(CLASS) \
typename std::enable_if<OB_TRAIT_IS_THREAD_LIKE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_LIKE_THREAD(CLASS) \
typename std::enable_if<!OB_TRAIT_IS_THREAD_LIKE(CLASS), bool>::type = true

// define to_string trait and enable_if macro
#define OB_TRAIT_HAS_TO_STRING(CLASS) \
::oceanbase::common::meta::has_to_string<DECAY(CLASS), int(char*, const int64_t)>::value
#define ENABLE_IF_HAS_TO_STRING(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_TO_STRING(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_TO_STRING(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_TO_STRING(CLASS), bool>::type = true

// define serialize trait and enable_if macro
#define OB_TRAIT_SERIALIZEABLE(CLASS) \
(::oceanbase::common::meta::has_serialize<DECAY(CLASS),\
                                         int(char*, const int64_t, int64_t &)>::value &&\
::oceanbase::common::meta::has_deserialize<DECAY(CLASS),\
                                           int(const char*, const int64_t, int64_t &)>::value &&\
::oceanbase::common::meta::has_get_serialize_size<DECAY(CLASS), int64_t()>::value)
#define ENABLE_IF_SERIALIZEABLE(CLASS) \
typename std::enable_if<OB_TRAIT_SERIALIZEABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_SERIALIZEABLE(CLASS) \
typename std::enable_if<!OB_TRAIT_SERIALIZEABLE(CLASS), bool>::type = true

// define serialize trait and enable_if macro
#define OB_TRAIT_DEEP_SERIALIZEABLE(CLASS) \
(::oceanbase::common::meta::has_serialize<DECAY(CLASS),\
                                         int(char*, const int64_t, int64_t &)>::value &&\
::oceanbase::common::meta::has_deserialize<DECAY(CLASS),\
                                           int(ObIAllocator &, const char*, const int64_t, int64_t &)>::value &&\
::oceanbase::common::meta::has_get_serialize_size<DECAY(CLASS), int64_t()>::value)
#define ENABLE_IF_DEEP_SERIALIZEABLE(CLASS) \
typename std::enable_if<OB_TRAIT_DEEP_SERIALIZEABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_DEEP_SERIALIZEABLE(CLASS) \
typename std::enable_if<!OB_TRAIT_DEEP_SERIALIZEABLE(CLASS), bool>::type = true

// define assign trait and enable_if macro
#define OB_TRAIT_HAS_ASSIGN(CLASS) \
::oceanbase::common::meta::has_assign<DECAY(CLASS), int(const CLASS &)>::value
#define ENABLE_IF_HAS_ASSIGN(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ASSIGN(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ASSIGN(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ASSIGN(CLASS), bool>::type = true

// define assign trait and enable_if macro
#define OB_TRAIT_HAS_DEEP_ASSIGN(CLASS) \
::oceanbase::common::meta::has_assign<DECAY(CLASS), int(ObIAllocator &, const CLASS &)>::value
#define ENABLE_IF_HAS_DEEP_ASSIGN(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_DEEP_ASSIGN(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_DEEP_ASSIGN(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_DEEP_ASSIGN(CLASS), bool>::type = true

// define funtion like trait and enable_if macro
#define OB_TRAIT_IS_FUNCTION_LIKE(CLASS, DECLEARATION) \
::oceanbase::common::meta::is_function_like<DECAY(CLASS), DECLEARATION>::value
#define ENABLE_IF_LIKE_FUNCTION(CLASS, DECLEARATION) \
typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(CLASS, DECLEARATION), bool>::type = true
#define ENABLE_IF_NOT_LIKE_FUNCTION(CLASS, DECLEARATION) \
typename std::enable_if<!OB_TRAIT_IS_FUNCTION_LIKE(CLASS, DECLEARATION), bool>::type = true

// define common enable_if macro
#define ENABLE_IF_HAS(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<::oceanbase::common::meta::has_##FUNCTION<DECAY(CLASS),\
                                                                  DECLEARATION>::value,\
                        bool>::type = true
#define ENABLE_IF_NOT_HAS(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<!::oceanbase::common::meta::has_##FUNCTION<DECAY(CLASS),\
                                                                   DECLEARATION>::value,\
                        bool>::type = true

// define compare trait and enable_if macro
#define OB_TRAIT_HAS_LESS_OPERATOR(CLASS) \
::oceanbase::common::meta::has_less_operator<DECAY(CLASS)>::value
#define OB_TRAIT_HAS_EQUAL_OPERATOR(CLASS) \
::oceanbase::common::meta::has_equal_operator<DECAY(CLASS)>::value
#define OB_TRAIT_IS_ORIGIN_COMPAREABLE(CLASS) \
OB_TRAIT_HAS_LESS_OPERATOR(CLASS) && OB_TRAIT_HAS_EQUAL_OPERATOR(CLASS)
#define OB_TRAIT_IS_METHOD_COMPAREABLE(CLASS) \
::oceanbase::common::meta::has_compare<DECAY(CLASS), int(const CLASS &)>::value
#define OB_TRAIT_IS_COMPAREABLE(CLASS) \
OB_TRAIT_IS_ORIGIN_COMPAREABLE(CLASS) || OB_TRAIT_IS_METHOD_COMPAREABLE(CLASS)
#define ENABLE_IF_COMPAREABLE(CLASS) \
typename std::enable_if<OB_TRAIT_IS_COMPAREABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_COMPAREABLE(CLASS) \
typename std::enable_if<!OB_TRAIT_IS_COMPAREABLE(CLASS), bool>::type = true

// define copy and move trait and enable_if macro
#define OB_TRAIT_IS_MOVEABLE(CLASS) \
std::is_move_assignable<DECAY(T)>::value || std::is_move_constructible<DECAY(T)>::value
#define OB_TRAIT_IS_COPIABLE(CLASS) \
OB_TRAIT_HAS_ASSIGN(T) ||\
std::is_copy_assignable<DECAY(T)>::value ||\
std::is_copy_constructible<DECAY(T)>::value
#define OB_TRAIT_IS_MOVE_OR_COPIABLE(CLASS) \
OB_TRAIT_IS_MOVEABLE(CLASS) || OB_TRAIT_IS_COPIABLE(CLASS)
#define ENABLE_IF_MOVEABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<OB_TRAIT_IS_MOVEABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_MOVEABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<!OB_TRAIT_IS_MOVEABLE(CLASS), bool>::type = true
#define ENABLE_IF_COPIABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<OB_TRAIT_IS_COPIABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_COPIABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<!OB_TRAIT_IS_COPIABLE(CLASS), bool>::type = true
#define ENABLE_IF_MOVE_OR_COPIABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<OB_TRAIT_IS_MOVE_OR_COPIABLE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_MOVE_AND_COPIABLE(CLASS, FUNCTION, DECLEARATION) \
typename std::enable_if<!OB_TRAIT_IS_MOVE_OR_COPIABLE(CLASS), bool>::type = true

// define two-phase-commit trait and enable_if macro
#define OB_TRAIT_HAS_ON_SET(CLASS) \
::oceanbase::common::meta::has_on_set<DECAY(CLASS), void()>::value
#define OB_TRAIT_HAS_ON_REDO(CLASS) \
::oceanbase::common::meta::has_on_redo<DECAY(CLASS), void(share::SCN &)>::value
#define OB_TRAIT_HAS_ON_PREPARE(CLASS) \
::oceanbase::common::meta::has_on_prepare<DECAY(CLASS), void(share::SCN &)>::value
#define OB_TRAIT_HAS_ON_COMMIT(CLASS) \
::oceanbase::common::meta::has_on_commit<DECAY(CLASS), void(share::SCN &, share::SCN &)>::value
#define OB_TRAIT_HAS_ON_ABORT(CLASS) \
::oceanbase::common::meta::has_on_abort<DECAY(CLASS), void(share::SCN &)>::value
#define ENABLE_IF_HAS_ON_SET(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ON_SET(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ON_SET(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ON_SET(CLASS), bool>::type = true
#define ENABLE_IF_HAS_ON_REDO(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ON_REDO(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ON_REDO(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ON_REDO(CLASS), bool>::type = true
#define ENABLE_IF_HAS_ON_PREPARE(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ON_PREPARE(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ON_PREPARE(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ON_PREPARE(CLASS), bool>::type = true
#define ENABLE_IF_HAS_ON_COMMIT(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ON_COMMIT(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ON_COMMIT(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ON_COMMIT(CLASS), bool>::type = true
#define ENABLE_IF_HAS_ON_ABORT(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_ON_ABORT(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_ON_ABORT(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_ON_ABORT(CLASS), bool>::type = true

#define OB_TRAIT_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS) \
::oceanbase::common::meta::has_check_can_replay_commit<DECAY(CLASS), bool(const char *, const int64_t, const share::SCN &, storage::mds::BufferCtx &)>::value
#define ENABLE_IF_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS) \
typename std::enable_if<OB_TRAIT_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS), bool>::type = true
#define ENABLE_IF_NOT_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS) \
typename std::enable_if<!OB_TRAIT_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS), bool>::type = true

#define OB_TRAIT_MDS_COMMIT_FOR_OLD_MDS(CLASS) \
::oceanbase::common::meta::has_on_commit_for_old_mds<DECAY(CLASS), int(const char *, const int64_t, const transaction::ObMulSourceDataNotifyArg &)>::value
#define ENABLE_IF_MDS_COMMIT_FOR_OLD_MDS(CLASS) \
typename std::enable_if<OB_TRAIT_MDS_COMMIT_FOR_OLD_MDS(CLASS), bool>::type = true
#define ENABLE_IF_NOT_MDS_COMMIT_FOR_OLD_MDS(CLASS) \
typename std::enable_if<!OB_TRAIT_MDS_COMMIT_FOR_OLD_MDS(CLASS), bool>::type = true

template <typename T>
struct MdsCheckCanReplayWrapper {
  template <typename CLASS = T, ENABLE_IF_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS)>
  static bool check_can_replay_commit(const char *buf, const int64_t buf_len, const share::SCN &scn, storage::mds::BufferCtx &ctx) {
    return T::check_can_replay_commit(buf, buf_len, scn, ctx);
  }
  template <typename CLASS = T, ENABLE_IF_NOT_HAS_CHECK_CAN_REPLAY_COMMIT(CLASS)>
  static bool check_can_replay_commit(const char *, const int64_t, const share::SCN &, storage::mds::BufferCtx &) {
    return true;
  }
};

template <typename T>
struct MdsCommitForOldMdsWrapper {
  template <typename CLASS = T, ENABLE_IF_MDS_COMMIT_FOR_OLD_MDS(CLASS)>
  static int on_commit_for_old_mds(const char *buf, const int64_t buf_len, const transaction::ObMulSourceDataNotifyArg &arg) {
    return T::on_commit_for_old_mds(buf, buf_len, arg);
  }
  template <typename CLASS = T, ENABLE_IF_NOT_MDS_COMMIT_FOR_OLD_MDS(CLASS)>
  static int on_commit_for_old_mds(const char *, const int64_t, const transaction::ObMulSourceDataNotifyArg &) {
    return OB_SUCCESS;
  }
};

}
}
}

#endif
