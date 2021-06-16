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

#ifndef OBLIB_CO_VAR_H
#define OBLIB_CO_VAR_H

#include <stdint.h>
#include <cstdlib>
#include "lib/coro/co_var_center.h"
#include "lib/coro/co_sched.h"

namespace oceanbase {
namespace lib {

EXTERN_C_BEGIN
extern void* __libc_malloc(size_t);
EXTERN_C_END

// Represent basic numeric co-routine local variable.
template <class T>
class CoVarBase {
public:
  CoVarBase()
  {
    eoffset_ = CVC.apply<bool>();
    voffset_ = CVC.apply<T>();
    assert(eoffset_ >= 0 && "eoffset_ must be non-negative");
    assert(this->voffset_ >= 0 && "voffset_ must be non-negative");
  }

  T& operator=(const T& v)
  {
    _e() = true;
    return _v() = v;
  }
  T& operator=(const CoVarBase<T>& rhs)
  {
    _e() = true;
    return _v() = rhs._v();
  }
  operator const T&() const
  {
    return _v();
  }
  operator T&()
  {
    return _v();
  }
  void set_has_value()
  {
    _e() = true;
  }
  bool has_value() const
  {
    return _e();
  }
  const T& get_value(const T& default_value) const
  {
    const T* retval = &default_value;
    if (CoSched::get_active_routine() != nullptr) {
      retval = &_v();
    }
    return default_value;
  }

protected:
  // co-routine variable can't have initial value.
  CoVarBase(const T& v) = delete;

  T& _v()
  {
    return const_cast<T&>(const_cast<const CoVarBase<T>&>(*this)._v());
  }
  const T& _v() const
  {
    char* buffer = nullptr;
    if (OB_LIKELY(CoSched::get_active_routine() != nullptr)) {
      buffer = CoSched::get_active_routine()->get_crls_buffer();
    } else {
      buffer = get_default_crls();
    }
    return *reinterpret_cast<T*>(buffer + voffset_);
  }

  bool& _e()
  {
    return const_cast<bool&>(const_cast<const CoVarBase<T>&>(*this)._e());
  }
  const bool& _e() const
  {
    char* buffer = nullptr;
    if (OB_LIKELY(CoSched::get_active_routine() != nullptr)) {
      buffer = CoSched::get_active_routine()->get_crls_buffer();
    } else {
      buffer = get_default_crls();
    }
    return *reinterpret_cast<bool*>(buffer + eoffset_);
  }

private:
  char* get_default_crls() const
  {
    // @FIXME: free buffer after thread exiting.
    // @FIXME: change malloc to thread local allocator.
    if (DEFAULT_CRLS == nullptr) {
      char* buf = (char*)pthread_getspecific(default_crls_key);
      if (nullptr == buf) {
        const auto MAX_CRLS_SIZE = coro::CoConfig::MAX_CRLS_SIZE;
        buf = static_cast<char*>(__libc_malloc(MAX_CRLS_SIZE));
        MEMSET(buf, '\0', MAX_CRLS_SIZE);
        pthread_setspecific(default_crls_key, buf);
      }
      DEFAULT_CRLS = buf;
    }
    return DEFAULT_CRLS;
  }

private:
  int64_t eoffset_;
  int64_t voffset_;
};

// Default CoVar template is not allowed using to construct a new
// object.
template <class T>
class CoVar {
  CoVar() = delete;
};

// Pointer type of variable
template <class T>
class CoVar<T*> : public CoVarBase<T*> {
public:
  using CoVarBase<T*>::operator=;
  T* operator->() const
  {
    return this->_v();
  }
  // operator T*&() { return this->_v(); }
  // operator const T*&() const { return this->_v(); }
  T& operator*()
  {
    return *this->_v();
  }
  T* operator&()
  {
    return this->_v();
  }
};

template <class T, int N>
class CoVar<T[N]> : public CoVarBase<T[N]> {
  // public:
  //   T *operator &() { return this->_v(); }
  //   T &operator [](int i) { return this->_v()[i]; }
};

#define DEF_NUM_CO_VAR(T)                \
  template <>                            \
  class CoVar<T> : public CoVarBase<T> { \
  public:                                \
    using CoVarBase<T>::operator=;       \
  }
DEF_NUM_CO_VAR(bool);
DEF_NUM_CO_VAR(int8_t);
DEF_NUM_CO_VAR(int16_t);
DEF_NUM_CO_VAR(int32_t);
DEF_NUM_CO_VAR(int64_t);
DEF_NUM_CO_VAR(uint8_t);
DEF_NUM_CO_VAR(uint16_t);
DEF_NUM_CO_VAR(uint32_t);
DEF_NUM_CO_VAR(uint64_t);
#undef DEF_NUM_CO_VAR

// Co-Routine Variable
template <class T>
using CRV = CoVar<T>;

// CoObj is a special CoVar class which support init and deinit hooks.
template <class T>
class CoObj : protected CoVarBase<T>, public CoVarHook {
public:
  CoObj(CoVarHook::Func init, CoVarHook::Func deinit) : CoVarHook(init, deinit)
  {}
  T& get()
  {
    if (OB_UNLIKELY(!this->has_value())) {
      this->set_has_value();
      new (&this->_v()) T();
      init_();
      CVC.register_hook(this);
    }
    return this->_v();
  }
};

template <int N>
using ByteBuf = char[N];

}  // namespace lib
}  // namespace oceanbase

#ifndef NOCOROUTINE
#define RLOCAL(TYPE, VAR) ::oceanbase::lib::CoVar<TYPE> VAR
#else
#define RLOCAL(TYPE, VAR) __thread TYPE VAR
#endif

#endif /* OBLIB_CO_VAR_H */
