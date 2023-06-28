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

#ifndef LOGSERVICE_COORDINATOR_OB_TUPLR_H
#define LOGSERVICE_COORDINATOR_OB_TUPLR_H

#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include <tuple>
#include <type_traits>
#include <utility>
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
namespace obtuple
{
template <size_t I>
struct get_impl
{
  template <typename T, typename ...Ts>
  static int get(std::tuple<Ts...> &tup, size_t idx, T &p_v)
  {
    if (idx == I - 1) {
      p_v = std::get<I - 1>(tup);
      return OB_SUCCESS;
    } else {
      return get_impl<I - 1>::get(tup, idx, p_v);
    }
  }
};

template <>
struct get_impl<0>
{
  template <typename T, typename ...Ts>
  static int get(std::tuple<Ts...> &tup, size_t idx, T &p_value) { return common::OB_INDEX_OUT_OF_RANGE; }
};

template <typename T, typename... Ts>
int get(std::tuple<Ts...>& tup, size_t idx, T &p_value)
{
  return get_impl<sizeof...(Ts)>::get(tup, idx, p_value);
}

template <int N, typename ...T>
struct get_type {
  using type = typename get_type<N - 1, T...>::type;
};

template <typename T, typename ...Rest>
struct get_type<0, T, Rest...> {
  using type = T;
};

#include <type_traits>

// Primary template with a static assertion
// for a meaningful error message
// if it ever gets instantiated.
// We could leave it undefined if we didn't care.

template<typename, typename T>
struct has_assign {
  static_assert(std::integral_constant<T, false>::value,
    "Second template parameter needs to be of function type.");
};

// specialization that does the checking

template<typename C, typename Ret, typename... Args>
struct has_assign<C, Ret(Args...)> {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype( std::declval<T>().assign( std::declval<Args>()... ) ),
      Ret
    >::type;  // attempt to call it and see if the return type is correct

  template<typename>
  static constexpr std::false_type check(...);

  typedef decltype(check<C>(0)) type;

public:
  static constexpr bool value = type::value;
};

template <typename T, typename std::enable_if<has_assign<T, int(const T&)>::value, bool>::type = true>
int copy_assign_element(T &lhs, const T &rhs) {
  return lhs.assign(rhs);
}
template <typename T, typename std::enable_if<!has_assign<T, int(const T&)>::value, bool>::type = false>
int copy_assign_element(T &lhs, const T &rhs) {
  lhs = rhs;
  return 0;
}

template<typename, typename T>
struct has_serialize {
  static_assert(std::integral_constant<T, false>::value,
    "Second template parameter needs to be of function type.");
};

template<typename C, typename Ret, typename... Args>
struct has_serialize<C, Ret(Args...)> {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype( std::declval<T>().serialize( std::declval<Args>()... ) ),
      Ret
    >::type;  // attempt to call it and see if the return type is correct

  template<typename>
  static constexpr std::false_type check(...);

  typedef decltype(check<C>(0)) type;

public:
  static constexpr bool value = type::value;
};

template <typename ...Rest>
struct has_serialize_for_all_type;

template <typename T, typename ...Rest>
struct has_serialize_for_all_type<T, Rest...>
{
  static constexpr bool value = has_serialize_for_all_type<T>::value && has_serialize_for_all_type<Rest...>::value;
};

template <typename T>
struct has_serialize_for_all_type<T>
{
  static constexpr bool value = std::is_class<T>::value ? has_serialize<T, int(char *, const int64_t, int64_t &)>::value : true;
};

template<typename, typename T>
struct has_deserialize {
  static_assert(std::integral_constant<T, false>::value,
    "Second template parameter needs to be of function type.");
};

template<typename C, typename Ret, typename... Args>
struct has_deserialize<C, Ret(Args...)> {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype( std::declval<T>().deserialize( std::declval<Args>()... ) ),
      Ret
    >::type;  // attempt to call it and see if the return type is correct

  template<typename>
  static constexpr std::false_type check(...);

  typedef decltype(check<C>(0)) type;

public:
  static constexpr bool value = type::value;
};

template <typename ...Rest>
struct has_deserialize_for_all_type;

template <typename T, typename ...Rest>
struct has_deserialize_for_all_type<T, Rest...>
{
  static constexpr bool value = has_deserialize_for_all_type<T>::value && has_deserialize_for_all_type<Rest...>::value;
};

template <typename T>
struct has_deserialize_for_all_type<T>
{
  static constexpr bool value = std::is_class<T>::value ? has_deserialize<T, int(const char *, const int64_t, int64_t &)>::value : true;
};

template<typename, typename T>
struct has_get_serialize_size {
  static_assert(std::integral_constant<T, false>::value,
    "Second template parameter needs to be of function type.");
};

template<typename C, typename Ret, typename... Args>
struct has_get_serialize_size<C, Ret(Args...)> {
private:
  template<typename T>
  static constexpr auto check(T*)
  -> typename
    std::is_same<
      decltype( std::declval<T>().get_serialize_size( std::declval<Args>()... ) ),
      Ret
    >::type;  // attempt to call it and see if the return type is correct

  template<typename>
  static constexpr std::false_type check(...);

  typedef decltype(check<C>(0)) type;

public:
  static constexpr bool value = type::value;
};

template <typename ...Rest>
struct has_get_serialize_size_for_all_type;

template <typename T, typename ...Rest>
struct has_get_serialize_size_for_all_type<T, Rest...>
{
  static constexpr bool value = has_get_serialize_size_for_all_type<T>::value && has_get_serialize_size_for_all_type<Rest...>::value;
};

template <typename T>
struct has_get_serialize_size_for_all_type<T>
{
  static constexpr bool value = std::is_class<T>::value ? has_get_serialize_size<T, int64_t()>::value : true;
};

template <typename ELEMENT_TYPE, typename HEAD, typename ...OTHERS>
struct ConvertElementTypeToIndex
{
  static constexpr int index = ConvertElementTypeToIndex<ELEMENT_TYPE, OTHERS...>::index;
};

template <typename ELEMENT_TYPE, typename ...OTHERS>
struct ConvertElementTypeToIndex<ELEMENT_TYPE, ELEMENT_TYPE, OTHERS...>
{
  static constexpr int index = sizeof...(OTHERS);
};

template <typename ...T>
class ObTupleBaseBase
{
public:
  static constexpr int get_element_size() { return sizeof...(T); }
  template <typename ELEMENT_TYPE>
  static constexpr int get_element_index() { return sizeof...(T) - 1 - ConvertElementTypeToIndex<ELEMENT_TYPE, T...>::index; }
protected:
  std::tuple<T...> tuple_;
  template <int N, typename FUNC>
  struct ForEachHelper
  {
    static int iterate(std::tuple<T...> &tuple, FUNC &functor)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(functor(std::get<N>(tuple)))) {
        // OB_LOG(WARN, "apply FUNC on element failed", K(ret), K(N), K(std::get<N>(tuple)));
      } else if (OB_SUCCESS != (ret = (ForEachHelper<N+1, FUNC>::iterate(tuple, functor)))) {
      }
      return ret;
    }
    static int iterate(const std::tuple<T...> &tuple, FUNC &functor)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(functor(std::get<N>(tuple)))) {
        // OB_LOG(WARN, "apply FUNC on element failed", K(ret), K(N), K(std::get<N>(tuple)));
      } else if (OB_SUCCESS != (ret = (ForEachHelper<N+1, FUNC>::iterate(tuple, functor)))) {
      }
      return ret;
    }
  };
  template <typename FUNC>
  struct ForEachHelper<sizeof...(T), FUNC>
  {
    static int iterate(std::tuple<T...> &tuple, FUNC &functor)
    {
      return OB_SUCCESS;
    }
    static int iterate(const std::tuple<T...> &tuple, FUNC &functor)
    {
      return OB_SUCCESS;
    }
  };
public:
  template <typename FUNC>
  int for_each(FUNC &&functor)
  {
    return ForEachHelper<0, FUNC>::iterate(tuple_, functor);
  }
  template <typename FUNC>
  int for_each(FUNC &&functor) const
  {
    return ForEachHelper<0, FUNC>::iterate(tuple_, functor);
  }
public:
  template <typename ...Args>
  ObTupleBaseBase(Args &&...args) : tuple_(std::forward<Args>(args)...) {}
  template <int INDEX>
  auto element() -> decltype(std::get<INDEX>(tuple_)) { return std::get<INDEX>(tuple_); }
  template <int INDEX>
  auto element() const -> const decltype(std::get<INDEX>(tuple_)) { return std::get<INDEX>(tuple_); }
  template <typename ELEMENT_TYPE>
  auto element() -> ELEMENT_TYPE& { return std::get<get_element_index<ELEMENT_TYPE>()>(tuple_); }
  template <typename ELEMENT_TYPE>
  auto element() const -> ELEMENT_TYPE& { return std::get<get_element_index<ELEMENT_TYPE>()>(tuple_); }
  template <typename V>
  int get_element(size_t idx, V &value)
  {
    return obtuple::get(tuple_, idx, value);
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    print_<0>(buf, buf_len, pos);
    return pos;
  }
  std::tuple<T...> &tuple() { return tuple_; }
protected:
  template <int N>
  int64_t print_(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    if (N == 0) {
      databuff_printf(buf, buf_len, pos, "{");
    }
    databuff_printf(buf, buf_len, pos, "%s,", to_cstring(std::get<N>(tuple_)));
    print_<N+1>(buf, buf_len, pos);
    return pos;
  }
  template <>
  int64_t print_<sizeof...(T) - 1>(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    databuff_printf(buf, buf_len, pos, "%s}", to_cstring(std::get<sizeof...(T) - 1>(tuple_)));
    return pos;
  }

  template <int N>
  int assign_(const std::tuple<T...> &rhs)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(copy_assign_element(std::get<N>(tuple_), std::get<N>(rhs)))) {
      OB_LOG(WARN, "assign element failed", K(ret), K(std::get<N>(tuple_)));
    } else if (OB_FAIL(assign_<N + 1>(rhs))) {
    }
    return ret;
  }

  template <>
  int assign_<sizeof...(T) - 1>(const std::tuple<T...> &rhs)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(copy_assign_element(std::get<sizeof...(T) - 1>(tuple_), std::get<sizeof...(T) - 1>(rhs)))) {
      OB_LOG(WARN, "assign element failed", K(ret), K(std::get<sizeof...(T) - 1>(tuple_)));
    }
    return ret;
  }
};

template <typename EnableSerialMethod, typename ...T>
class ObTupleBase;

template <typename ...T>
class ObTupleBase <typename std::enable_if<obtuple::has_get_serialize_size_for_all_type<T...>::value &&
                                           obtuple::has_serialize_for_all_type<T...>::value &&
                                           obtuple::has_deserialize_for_all_type<T...>::value>::type, T...> : public ObTupleBaseBase<T...>
{
protected:
  struct GetAllTypeSerializeSizeFunctor
  {
    GetAllTypeSerializeSizeFunctor(int64_t &total_size) : total_size_(total_size) {}
    template <typename Type>
    int operator()(const Type &element) {
      total_size_ += serialization::encoded_length(element);
      return OB_SUCCESS;
    }
    int64_t &total_size_;
  };
  struct ExecuteAllTypeSerializeMethodFunctor
  {
    ExecuteAllTypeSerializeMethodFunctor(char *buf, const int64_t buf_len, int64_t &pos) :
    buf_(buf), buf_len_(buf_len), pos_(pos) {}
    template <typename Type>
    int operator()(const Type &element) {
      return serialization::encode(buf_, buf_len_, pos_, element);
    }
    char *buf_;
    const int64_t buf_len_;
    int64_t &pos_;
  };
  struct ExecuteAllTypeDeSerializeMethodFunctor
  {
    ExecuteAllTypeDeSerializeMethodFunctor(const char *buf, const int64_t data_len, int64_t &pos) :
    buf_(buf), data_len_(data_len), pos_(pos) {}
    template <typename Type>
    int operator()(Type &element) {
      return serialization::decode(buf_, data_len_, pos_, element);
    }
    const char *buf_;
    const int64_t data_len_;
    int64_t &pos_;
  };
public:
  public:
  template <typename ...Args>
  ObTupleBase(Args &&...args) : ObTupleBaseBase<T...>(std::forward<Args>(args)...) {}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    ExecuteAllTypeSerializeMethodFunctor functor(buf, buf_len, pos);
    return ObTupleBaseBase<T...>::template ForEachHelper<0, ExecuteAllTypeSerializeMethodFunctor>::iterate(ObTupleBaseBase<T...>::tuple_, functor);
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    ExecuteAllTypeDeSerializeMethodFunctor functor(buf, data_len, pos);
    return ObTupleBaseBase<T...>::template ForEachHelper<0, ExecuteAllTypeDeSerializeMethodFunctor>::iterate(ObTupleBaseBase<T...>::tuple_, functor);
  }
  int64_t get_serialize_size() const
  {
    int64_t size = 0;
    GetAllTypeSerializeSizeFunctor functor(size);
    ObTupleBaseBase<T...>::template ForEachHelper<0, GetAllTypeSerializeSizeFunctor>::iterate(ObTupleBaseBase<T...>::tuple_, functor);
    return size;
  }
};

template <typename ...T>
class ObTupleBase <typename std::enable_if<!(obtuple::has_get_serialize_size_for_all_type<T...>::value &&
                                             obtuple::has_serialize_for_all_type<T...>::value &&
                                             obtuple::has_deserialize_for_all_type<T...>::value)>::type, T...> : public ObTupleBaseBase<T...> {
public:
  template <typename ...Args>
  ObTupleBase(Args &&...args) : ObTupleBaseBase<T...>(std::forward<Args>(args)...) {}
};
}

template <typename ...T>
class ObTuple : public obtuple::ObTupleBase<void, T...> {
public:
  template <typename ...Args>
  ObTuple(Args &&...args) : obtuple::ObTupleBase<void, T...>(std::forward<Args>(args)...) {}
  int assign(const ObTuple<T...> &rhs) {
    return obtuple::ObTupleBaseBase<T...>::template assign_<0>(rhs.tuple_);
  }
};

}
}
#endif