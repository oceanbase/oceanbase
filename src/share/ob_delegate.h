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
  auto func_name(Args &&...args)                                \
  ->decltype(delegate_obj.func_name(std::forward<Args>(args)...)) const { \
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

#endif
