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

#ifndef CO_LOCAL_STORAGE_H
#define CO_LOCAL_STORAGE_H

#include "lib/coro/co_var.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator_v2.h"

namespace oceanbase {
namespace lib {

class CoLocalStorage {
  // The type of memory allocator used to allocate memory for each
  // object instance.
  using Allocator = common::ObArenaAllocator;

  // INode structure is used to create/destroy object instance. Since
  // object array and normal object should process in different way
  // and, moreover, C++ template function can't handle specialization
  // very well, so we add the auxiliary template structure, INode, to
  // deliver the different instance type. INode has two interface
  // functions, the first called `create' is used to create a
  // co-routine local object with given type. And the second function
  // called `destroy' is used to destroy the object created using
  // function `create'.
  //
  // The user interface is function `get' which would return suitable
  // object pointer just as standard `new' function, simple object
  // would return pointer to the object and object array would return
  // pointer to the first object in the array.
  template <class T>
  class INode;
  template <class T, int N>
  class INode<T[N]>;

public:
  // Allocate `bytes' bytes memory space from co-routine local
  // storage. There's no free interface because memory allocated by
  // `alloc' interface would been freed whenever relating routine
  // exiting.
  static void* alloc(int64_t bytes);

  // Get co-routine local instance. The instance wound call
  // constructor when being created and destructor when existing.
  template <class T, int idx = 0>
  static typename INode<T>::RETT* get_instance();

private:
  // Memory of the allocator object.
  static CoVar<char[sizeof(Allocator)]> buf_;
  // Pointer of allocator
  static CoObj<Allocator*> allocator_;
};

inline void* CoLocalStorage::alloc(int64_t bytes)
{
  return allocator_.get()->alloc(bytes);
}

template <class T>
class CoLocalStorage::INode {
public:
  using RETT = T;
  INode() : obj_([this] { this->create(); }, [this] { this->destroy(); })
  {}
  RETT* get()
  {
    return obj_.get();
  }

private:
  void create()
  {
    void* ptr = CoLocalStorage::alloc(sizeof(T));
    if (ptr != nullptr) {
      obj_.get() = new (ptr) T();
    } else {
      obj_.get() = nullptr;
    }
  }
  void destroy()
  {
    if (obj_.get() != nullptr) {
      obj_.get()->~T();
      obj_.get() = nullptr;
    }
  }

private:
  CoObj<T*> obj_;
};

template <class T, int N>
class CoLocalStorage::INode<T[N]> {
public:
  using RETT = T;
  INode() : obj_([this] { this->create(); }, [this] { this->destroy(); })
  {}
  T* get()
  {
    return obj_.get();
  }

private:
  void create()
  {
    void* ptr = CoLocalStorage::alloc(sizeof(T[N]));
    if (ptr != nullptr) {
      obj_.get() = new (ptr) T[N];
    } else {
      obj_.get() = nullptr;
    }
  }
  void destroy()
  {
    if (obj_.get() != nullptr) {
      for (int i = 0; i < N; i++) {
        obj_.get()[i].~T();
      }
    }
  }

private:
  CoObj<T*> obj_;
};

template <class T, int idx>
typename CoLocalStorage::INode<T>::RETT* CoLocalStorage::get_instance()
{
  static INode<T> node;
  return node.get();
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_LOCAL_STORAGE_H */
