#ifndef DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_COPY_H
#define DEPS_OBLIB_SRC_COMMON_META_PROGRAMMING_OB_META_COPY_H
#include "ob_meta_define.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "ob_type_traits.h"

namespace oceanbase
{
namespace common
{
namespace meta
{

// 3.1 try user defined deep assign method with allocator
template <typename T, typename std::enable_if<OB_TRAIT_HAS_DEEP_ASSIGN(T), bool>::type = true>
inline int copy_or_assign(const T &src,
                          T &dst,
                          ObIAllocator &alloc = DummyAllocator::get_instance())
{
  return dst.assign(alloc, src);
}

// 3.2 try user defined assign method without allocator
template <typename T, typename std::enable_if<!OB_TRAIT_HAS_DEEP_ASSIGN(T) &&
                                              OB_TRAIT_HAS_ASSIGN(T), bool>::type = true>
inline int copy_or_assign(const T &src,
                          T &dst,
                          ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(alloc);
  return dst.assign(src);
}

// 3.3 try standard assignment
template <typename T,
          typename std::enable_if<!OB_TRAIT_HAS_DEEP_ASSIGN(T) &&
                                  !OB_TRAIT_HAS_ASSIGN(T) &&
                                  std::is_copy_assignable<T>::value, bool>::type = true>
inline int copy_or_assign(const T &src,
                          T &dst,
                          ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(alloc);
  dst = src;
  return common::OB_SUCCESS;
}

// 3.4. try copy construction
template <typename T,
          typename std::enable_if<!OB_TRAIT_HAS_DEEP_ASSIGN(T) &&
                                  !OB_TRAIT_HAS_ASSIGN(T) &&
                                  !std::is_copy_assignable<T>::value &&
                                  std::is_copy_constructible<T>::value, bool>::type = true>
inline int copy_or_assign(const T &src,
                          T &dst,
                          ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(alloc);
  new (&dst) T (src);
  return common::OB_SUCCESS;
}

// 3.5. compile error message
template <typename T,
          typename std::enable_if<!OB_TRAIT_HAS_DEEP_ASSIGN(T) &&
                                  !OB_TRAIT_HAS_ASSIGN(T) &&
                                  !std::is_copy_assignable<T>::value &&
                                  !std::is_copy_constructible<T>::value, bool>::type = true>
inline int copy_or_assign(const T &src,
                          T &dst,
                          ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(src);
  UNUSED(dst);
  UNUSED(alloc);
  static_assert(!(!OB_TRAIT_HAS_DEEP_ASSIGN(T) &&
                !OB_TRAIT_HAS_ASSIGN(T) &&
                !std::is_copy_assignable<T>::value &&
                !std::is_copy_constructible<T>::value),
                "your type is not deep assignable, not normal assignable, not copy assignable, "
                "not copy constructible, there is no way to copy it");
  return OB_NOT_SUPPORTED;
}

// user will benefit from move sematic if dst is an rvalue and support move sematic
// 1.1 try standard move assignment
template <typename T,
          typename std::enable_if<std::is_rvalue_reference<T>::value &&
                                  std::is_move_assignable<T>::value, bool>::type = true>
inline int move_or_copy_or_assign(T &&src,
                                  T &dst,
                                  ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(alloc);
  dst = std::move(src);
  return common::OB_SUCCESS;
}

// 1.2 try move construction
template <typename T,
          typename std::enable_if<std::is_rvalue_reference<T>::value &&
                                  !std::is_move_assignable<T>::value &&
                                  std::is_move_constructible<T>::value, bool>::type = true>
inline int move_or_copy_or_assign(T &&src,
                                  T &dst,
                                  ObIAllocator &alloc = DummyAllocator::get_instance())
{
  UNUSED(alloc);
  new (&dst) T (std::move(src));
  return common::OB_SUCCESS;
}

// if type T is not moveable or src is a lvalue, try copy path
// 2.0 deep copy with allocator
template <typename T>
inline int move_or_copy_or_assign(const T &src,
                                  T &dst,
                                  ObIAllocator &alloc = DummyAllocator::get_instance())
{
  return copy_or_assign(src, dst, alloc);
}

}
}
}
#endif