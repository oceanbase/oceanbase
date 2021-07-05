/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software
 * according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *
 * http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A
 * PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DI_TLS_H_
#define OB_DI_TLS_H_

#include "lib/ob_define.h"

namespace oceanbase {
namespace common {

template <class T>
class ObDITls {
public:
  static ObDITls& get_di_tls();
  void destroy();
  T* new_instance();
  static T* get_instance();

private:
  ObDITls() : key_(INT32_MAX)
  {
    if (0 != pthread_key_create(&key_, destroy_thread_data_)) {}
  }
  ~ObDITls()
  {
    destroy();
  }
  static void destroy_thread_data_(void* ptr);

private:
  pthread_key_t key_;
  static __thread T* instance_;
};
// NOTE: thread local diagnose information
// TODO: check if multi-query execute within one thread.
template <class T>
__thread T* ObDITls<T>::instance_ = NULL;

template <class T>
void ObDITls<T>::destroy_thread_data_(void* ptr)
{
  if (NULL != ptr) {
    T* tls = (T*)ptr;
    delete tls;
    instance_ = NULL;
  }
}

template <class T>
ObDITls<T>& ObDITls<T>::get_di_tls()
{
  static ObDITls<T> di_tls;
  return di_tls;
}

template <class T>
void ObDITls<T>::destroy()
{
  if (INT32_MAX != key_) {
    void* ptr = pthread_getspecific(key_);
    destroy_thread_data_(ptr);
    if (0 != pthread_key_delete(key_)) {
    } else {
      key_ = INT32_MAX;
    }
  }
}

template <class T>
T* ObDITls<T>::new_instance()
{
  T* instance = NULL;
  if (INT32_MAX != key_) {
    T* tls = (T*)pthread_getspecific(key_);
    if (NULL == tls) {
      tls = new (std::nothrow) T();
      if (NULL != tls && 0 != pthread_setspecific(key_, tls)) {
        delete tls;
        tls = NULL;
      }
    }
    if (NULL != tls) {
      instance = tls;
    }
  }
  return instance;
}

template <class T>
T* ObDITls<T>::get_instance()
{
  if (OB_UNLIKELY(NULL == instance_)) {
    instance_ = get_di_tls().new_instance();
  }
  return instance_;
}

}  // namespace common
}  // namespace oceanbase
#endif
