/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_DELEGATE_
#define OCEANBASE_SHARE_OB_DELEGATE_
#include <type_traits>

// this one need put the define of delegate_obj before delegate of function.
#define DELEGATE(delegate_obj, func_name)                       \
  template <typename ...Args>                                   \
  auto func_name(Args &&...args)                                \
  ->decltype(delegate_obj.func_name(std::forward<Args>(args)...)) { \
    return delegate_obj.func_name(std::forward<Args>(args)...); \
  }

#define CONST_DELEGATE(delegate_obj, func_name)                       \
  template <typename ...Args>                                   \
  auto func_name(Args &&...args) const                              \
  ->decltype(delegate_obj.func_name(std::forward<Args>(args)...)) { \
    return delegate_obj.func_name(std::forward<Args>(args)...); \
  }

#define DELEGATE_WITH_RET(delegate_obj, func_name, ret)         \
  template <typename ...Args>                                   \
    ret func_name(Args &&...args) {                             \
    return delegate_obj.func_name(std::forward<Args>(args)...); \
  }

#define DELEGATE_PTR_WITH_RET(delegate_obj, func_name, ret)     \
  template <typename ...Args>                                   \
    ret func_name(Args &&...args) {                             \
    return delegate_obj->func_name(std::forward<Args>(args)...); \
  }

#define DELEGATE_PTR_WITH_CHECK(delegate_obj, func_name)          \
  template <typename ...Args>                                     \
  int func_name(Args &&...args) {                                 \
    int ret = OB_SUCCESS;                                         \
    if (nullptr == delegate_obj) {                                \
      ret = OB_NOT_INIT;                                          \
    } else {                                                      \
      ret = delegate_obj->func_name(std::forward<Args>(args)...); \
    }                                                             \
    return ret;                                                   \
  }

#define CONST_DELEGATE_WITH_RET(delegate_obj, func_name, ret) \
  template <typename ...Args>                                   \
  ret func_name(Args &&...args) const {                               \
    return delegate_obj.func_name(std::forward<Args>(args)...); \
  }

#define CONST_DELEGATE_PTR_WITH_RET(delegate_obj, func_name, ret) \
  template <typename ...Args>                                     \
  ret func_name(Args &&...args) const {                           \
    return delegate_obj->func_name(std::forward<Args>(args)...);  \
  }

#define DELEGATE_WITH_EXTRA_ARG(delegate_obj, func_name, extra_arg)                \
  template <typename ...Args>                                                      \
    int func_name_(Args &&...args) {                                               \
    return delegate_obj.func_name(std::forward<Args>(args)..., extra_arg);         \
  }                                                                                \
  template <typename ...Args>                                                      \
    int func_name(Args &&...args) {                                                \
    return func_name_(extra_arg, std::forward<Args>(args)...);                     \
  }

#endif
