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

#ifndef OCEANBASE_LIB_GUARD_OB_SCOPE_GUARD_H
#define OCEANBASE_LIB_GUARD_OB_SCOPE_GUARD_H
#include <type_traits>
#include <utility>

namespace oceanbase
{
namespace common
{
// ScopeGuard是一个模版类，其中包含了被保护的资源类型，以及析构该资源的可调用函数类型
template <typename Resource, typename Deleter>
class ScopeGuard
{
public:
  template <typename T>
  ScopeGuard(Resource &p, T &&deleter) : p_(&p), deleter_(std::forward<Deleter>(deleter)) {}// ScopeGuard会在栈上开辟一个指针，指向被保护的资源
  ScopeGuard(const ScopeGuard &g) = delete;// ScopeGuard是禁止拷贝的
  ScopeGuard(ScopeGuard &&g) : p_(g.p_), deleter_(std::move(g.deleter_)) { g.p_ = nullptr; }// 但是允许移动
  ~ScopeGuard() { if (p_) { deleter_(*p_); p_ = nullptr; } }// 当ScopeGuard析构时，若其指向的资源仍然是有效的，则会调用析构操作
  Resource &resource() { return *p_; }// 通过resource()方法访问被保护的资源
  Resource &fetch_resource() {// 通过fetch_resource()取出被保护的资源
    Resource *p = p_;
    p_ = nullptr;
    return *p;
  }
  // 以下两个函数使用了SFINAE技法，只有当保护的资源是指针类型时编译器才会生成下面的两个函数
  template <typename T = Resource, typename std::enable_if<std::is_pointer<T>::value, bool>::type = true>
  typename std::add_lvalue_reference<typename std::remove_pointer<T>::type>::type operator*() { return **p_; }
  template <typename T = Resource, typename std::enable_if<std::is_pointer<T>::value, bool>::type = true>
  Resource operator->() { return *p_; }
private:
  Resource *p_;
  Deleter deleter_;
};
// MAKE_SCOPE宏用于去掉使用ScopeGuard时的语法噪音
#define MAKE_SCOPE(resource, lambda) \
({\
auto deleter = lambda;\
auto my_guard = ScopeGuard<typename std::remove_reference<decltype(resource)>::type, typename std::remove_reference<decltype(deleter)>::type>(resource, deleter); \
std::move(my_guard);\
})

}// namespace common
}// namespace oceanbase

#endif