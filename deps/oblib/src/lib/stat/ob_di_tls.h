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

namespace oceanbase
{
namespace common
{

template <class T>
class ObDITls
{
public:
  static T* get_instance();
private:
  ~ObDITls();
private:
  T* instance_;
  bool disable_;
};

template <class T>
ObDITls<T>::~ObDITls()
{
  if (OB_NOT_NULL(instance_)) {
    T* tls = instance_;
    instance_ = nullptr;
    disable_ = true;
    //delete tls;
    ob_delete(tls);
  }
}

template <class T>
T* ObDITls<T>::get_instance()
{
  static thread_local ObDITls<T> di_tls;
  if (OB_ISNULL(di_tls.instance_)) {
    if (OB_LIKELY(!di_tls.disable_)) {
      const char* label = "DITls";
      //if (OB_NOT_NULL(ob_get_origin_thread_name())) {
      //  label = ob_get_origin_thread_name();
      //}
      di_tls.disable_ = true;
      // add tenant
      di_tls.instance_ = OB_NEW(T, label);
      //instance_ = new T();
      di_tls.disable_ = false;
    }
  }
  return di_tls.instance_;
}

}
}
#endif
