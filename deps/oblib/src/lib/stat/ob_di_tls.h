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

#ifndef OB_DI_TLS_H_
#define OB_DI_TLS_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"

#include <cxxabi.h>

namespace oceanbase
{
namespace common
{
template<class T, int N>
struct ObDITlsPlaceHolder
{
  char buf_[sizeof(T[N])];
};

extern thread_local bool is_thread_in_exit;

template <class T, size_t tag>
class ObDITls
{
  // avoid reconstruct during construction.
  static constexpr uint64_t PLACE_HOLDER = 0x1;
  static constexpr uint64_t MAX_TNAME_LENGTH = 128;
public:
  static T* get_instance();
  OB_INLINE bool is_valid() { return OB_NOT_NULL(instance_) && PLACE_HOLDER != (uint64_t)instance_; }
private:
  ObDITls() : instance_(nullptr) {}
  ~ObDITls();
  static const char* get_label();
private:
  T* instance_;
};

template <class T, size_t tag>
const char* ObDITls<T, tag>::get_label() {
  const char* cxxname = typeid(T).name();
  const char* ret = "DITls";
  static char buf[MAX_TNAME_LENGTH];
  if (nullptr == cxxname || strlen(cxxname) > MAX_TNAME_LENGTH - 5) {
    // do nothing, avoid realloc in __cxa_demangle
  } else {
    int status = 0;
    int length = MAX_TNAME_LENGTH - 3;
    ret = abi::__cxa_demangle(cxxname, buf + 3, (size_t*)&length, &status);
    if (0 != status) {
      ret = "DITls";
    } else {
      // remove namespace
      length = MAX_TNAME_LENGTH - 1;
      while (length >= 3 && buf[length] != ':') {
        --length;
      }
      length -= 2;
      buf[length] = '[';
      buf[length + 1] = 'T';
      buf[length + 2] = ']';
      ret = buf + length;
    }
  }
  return ret;
}

template <class T, size_t tag>
ObDITls<T, tag>::~ObDITls()
{
  is_thread_in_exit = true;
  if (is_valid()) {
    lib::ObDisableDiagnoseGuard disable_diagnose_guard;
    ob_delete(instance_);
    instance_ = nullptr;
  }
}

template <class T, size_t tag>
T* ObDITls<T, tag>::get_instance()
{
  // for static check
  static ObDITlsPlaceHolder<T, 1> placeholder __attribute__((used));
  static thread_local ObDITls<T, tag> di_tls;
  if (OB_LIKELY(!di_tls.is_valid() && !is_thread_in_exit)) {
    static const char* label = get_label();
    di_tls.instance_ = (T*)PLACE_HOLDER;
    // add tenant
    ObMemAttr attr(ob_thread_tenant_id(), label);
    SET_USE_500(attr);
    di_tls.instance_ = OB_NEW(T, attr);
  }
  return di_tls.instance_;
}
template <class T, int N, size_t tag>
class ObDITls<T[N], tag>
{
  // avoid reconstruct during construction.
  static constexpr uint64_t PLACE_HOLDER = 0x1;
  static constexpr uint64_t MAX_TNAME_LENGTH = 128;
public:
  static T* get_instance();
  OB_INLINE bool is_valid() { return OB_NOT_NULL(instance_) && PLACE_HOLDER != (uint64_t)instance_; }
private:
  ObDITls() : instance_(nullptr) {}
  ~ObDITls();
  static const char* get_label();
private:
  T* instance_;
};

template <class T, int N, size_t tag>
const char* ObDITls<T[N], tag>::get_label() {
  const char* cxxname = typeid(T).name();
  const char* ret = "DITls";
  static char buf[MAX_TNAME_LENGTH];
  if (nullptr == cxxname || strlen(cxxname) > MAX_TNAME_LENGTH - 5) {
    // do nothing, avoid realloc in __cxa_demangle
  } else {
    int status = 0;
    int length = MAX_TNAME_LENGTH - 3;
    ret = abi::__cxa_demangle(cxxname, buf + 3, (size_t*)&length, &status);
    if (0 != status) {
      ret = "DITls";
    } else {
      // remove namespace
      length = MAX_TNAME_LENGTH - 1;
      while (length >= 3 && buf[length] != ':') {
        --length;
      }
      length -= 2;
      buf[length] = '[';
      buf[length + 1] = 'T';
      buf[length + 2] = ']';
      ret = buf + length;
    }
  }
  return ret;
}

template <class T, int N, size_t tag>
ObDITls<T[N], tag>::~ObDITls()
{
  is_thread_in_exit = true;
  if (is_valid()) {
    for (auto i = 0; i < N; ++i) {
      instance_[i].~T();
    }
    ob_free(instance_);
  }
}

template <class T, int N, size_t tag>
T* ObDITls<T[N], tag>::get_instance()
{
  // for static check
  static ObDITlsPlaceHolder<T, N> placeholder __attribute__((used));
  static thread_local ObDITls<T[N], tag> di_tls;
  if (OB_LIKELY(!di_tls.is_valid() && !is_thread_in_exit)) {
    static const char* label = get_label();
    di_tls.instance_ = (T*)PLACE_HOLDER;
    ObMemAttr attr(ob_thread_tenant_id(), label);
    SET_USE_500(attr);
    // add tenant
    if (OB_NOT_NULL(di_tls.instance_ = (T*)ob_malloc(sizeof(T) * N, attr))) {
      for (auto i = 0; i < N; ++i) {
        new (di_tls.instance_ + i) T;
      }
    }
  }
  return di_tls.instance_;
}

}
}
#endif
