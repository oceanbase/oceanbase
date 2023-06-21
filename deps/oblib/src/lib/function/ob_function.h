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

/**
 * ObFunction
 *
 * ObFunction is a basic abstruct tool, just behave like std::function.
 * ObFunction is a template class, it can only be used with template specialization.
 * User need specify Function input args' types and return type when specialization.
 * All callable object, no matter function pointer, functor class, or lambda, as long as they have
 * same input args' types and return type, they could be described as the same type using
 * ObFunction<Ret(Args...)>.
 * ObFunction has value semantics.
 *
 *
 * When to use:
 *   - you want to describe a callable object.
 *
 * When not to use:
 *   - memory alloc is not allowed.
 *   - the callable object' copy contruct action is very heavy, which can not be accepted.
 *
 * Memory usage:
 *   - ObFunction<Ret(Args...)> object has constant size: 64 Bytes in common.
 *   - if the stored callable object's size not more than SMALL_OBJ_MAX_SIZE(40 Bytes in common) By-
 *     tes, it will be stored in ObFunction's inner buffer, no extra memory will be allocated.
 *   - if the stored callable object's size more than SMALL_OBJ_MAX_SIZE Bytes, it will be stored in
 *     heap memory.
 *   - the memory allocator can be specified, if not, the default memory allocation strategy just s-
 *     imply using ob_malloc.
 *   - assign between big ObFunction with same allocator has been optimized for rvalue.
 *
 * Manual:
 *   - Interface
 *     1. construction
 *     + default construction:
 *         explicit ObFunction(ObIAllocator &)
 *     + copy construction:
 *         ObFunction(const ObFunction<Ret(Args...)> &, ObIAllocator &)
 *     + move construction:
 *         ObFunction(ObFunction<Ret(Args...)> &&, ObIAllocator &)
 *     + general construction:
 *         template <typename Fn>
 *         ObFunction(Fn &&, ObIAllocator &)
 *     2. destruction
 *     + reset:
 *         void reset()
 *         idempotent interface
 *     + default construction:
 *         ~ObFunction()
 *         just simply call reset()
 *     3. check
 *     + valid:
 *         bool is_valid()
 *     4. using
 *     + operator:
 *         Ret Operator(Args &&...args);
 *         call stored callable object, make sure ObFunction is valid before call this.
 *
 *  - CAUTION:
 *      + DO check is_valid() after ObFuntion contructed, cause store big callable object may alloc
 *        memory, set ObFunction to a invalid state if alloc memory failed.
 *      + MAKE SURE ObFunction is valid before call it, or will CRASH.
 *
 *  - Contact  for help.
 */

#ifndef OCEANBASE_LIB_FUNCTION_OB_FUNTION_H
#define OCEANBASE_LIB_FUNCTION_OB_FUNTION_H

#include <type_traits>
#include <utility>
#include <assert.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{

namespace function
{
#ifdef UNITTEST_DEBUG
struct DebugRecorder {
  int function_default_construct_time = 0;
  int function_copy_construct_time = 0;
  int function_move_construct_time = 0;
  int function_general_construct_time = 0;
  int function_copy_equal_time = 0;
  int function_move_equal_time = 0;
  int function_general_equal_time = 0;
  int function_copy_assign_time = 0;
  int function_move_assign_time = 0;
  int function_general_assign_time = 0;
  int function_base_assign_time = 0;
  int derived_construct_time = 0;

  void reset () {
    memset(this, 0, sizeof(*this));
  }
  static DebugRecorder &get_instance() {
    static DebugRecorder recorder;
    return recorder;
  }
};
#endif

struct DefaultFunctionAllocator : public ObIAllocator {
  void *alloc(const int64_t size) override {
#ifdef UNITTEST_DEBUG
    total_alive_num++;
#endif
    static lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "ObFunction");
    SET_USE_500(attr);
    return ob_malloc(size, attr);
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override {
#ifdef UNITTEST_DEBUG
    total_alive_num--;
#endif
    ob_free(ptr);
  }
#ifdef UNITTEST_DEBUG
  int total_alive_num = 0;
#endif
  static DefaultFunctionAllocator &get_default_allocator() {
    static DefaultFunctionAllocator default_allocator;
    return default_allocator;
  }
};

// sizeof(ObFunction<*>) will be 64 bytes, and inner buffer will be 48 bytes.
// cause there is a vtable ptr, so when sizeof(Fn) <= 40 bytes, no extra memory will be allocated.
static const int64_t SMALL_OBJ_MAX_SIZE = 64 - sizeof(void*) - sizeof(ObIAllocator &);
}// namespace function

#define RECORDER function::DebugRecorder::get_instance()
#define DEFAULT_ALLOCATOR function::DefaultFunctionAllocator::get_default_allocator()

// general declaration
template <typename T>
class ObFunction;

// ObFunction specialization
template <typename Ret, typename ...Args>
class ObFunction<Ret(Args...)> {
  class Abstract {
  public:
    virtual Ret invoke(Args ...args) const = 0;
    virtual Abstract *copy(ObIAllocator &allocator, void *local_buffer) const = 0;
    virtual ~Abstract() {}
  };

  template <typename Fn>
  class Derived : public Abstract {
  static_assert(!std::is_const<Fn>::value, "Fn should not be const");
  public:
    Derived() = delete;
    Derived(const Derived<Fn> &fn) = delete;
    Derived(const Derived<Fn> &&fn) = delete;
    template <typename Arg>
    Derived(Arg &&fn) : func_(std::forward<Arg>(fn)) {
#ifdef UNITTEST_DEBUG
      RECORDER.derived_construct_time++;
#endif
    }
    Ret invoke(Args ...args) const override {
      return (const_cast<Derived *>(this))->func_(std::forward<Args>(args)...);
    }
    Abstract *copy(ObIAllocator &allocator, void *local_buffer) const override {
      Derived<Fn> *ptr = nullptr;
      if (sizeof(Derived<Fn>) <= function::SMALL_OBJ_MAX_SIZE) {
        ptr = (Derived<Fn>*)local_buffer;// small obj use local buffer
      } else {
        ptr = (Derived<Fn>*)allocator.alloc(sizeof(Derived<Fn>));// big obj need malloc heap memory
      }
      if (OB_LIKELY(nullptr != ptr)) {
        ptr = new (ptr)Derived<Fn>(func_);
      } else {
        const char* class_name = typeid(Fn).name();
        int class_size = sizeof(Derived<Fn>);
        OCCAM_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "ptr is nullptr",
                      K(class_name), K(class_size),
                      K(function::SMALL_OBJ_MAX_SIZE), KP(local_buffer));
      }
      return ptr;
    }
    ~Derived() override {}
  private:
    Fn func_;
  };

public:
  // default constructor
  explicit ObFunction(ObIAllocator &allocator = DEFAULT_ALLOCATOR) :
  base_(nullptr), allocator_(allocator) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_default_construct_time++;
#endif
  }
  // copy constructor
  ObFunction(const ObFunction<Ret(Args...)> &rhs,
             ObIAllocator &allocator = DEFAULT_ALLOCATOR) :
  base_(nullptr), allocator_(allocator) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_copy_construct_time++;
#endif
    (void)assign(rhs);
  }
  // move constructor
  ObFunction(ObFunction<Ret(Args...)> &&rhs,
             ObIAllocator &allocator = DEFAULT_ALLOCATOR) :
  base_(nullptr), allocator_(allocator) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_move_construct_time++;
#endif
    (void)assign(std::forward<ObFunction<Ret(Args...)>>(rhs));
  }
  // normal constructor
  // ObFunction is a callable class also, so enable_if(SFINAE) condition is needed here,
  // to making ObFunction copy and move constructor avaliable
  template <typename Fn,
  typename std::enable_if<
  !std::is_same<typename std::decay<Fn>::type, ObFunction<Ret(Args...)>>::value &&
  !std::is_base_of<ObIAllocator, typename std::decay<Fn>::type>::value, bool>::type = true>
  ObFunction(Fn &&rhs,
             ObIAllocator &allocator = DEFAULT_ALLOCATOR) :
  base_(nullptr), allocator_(allocator) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_general_construct_time++;
#endif
    (void)assign(std::forward<Fn>(rhs));
  }
#ifdef UNITTEST_DEBUG
  Abstract* get_func_ptr() { return base_; }
#endif

  // copy assign operator
  ObFunction &operator=(const ObFunction<Ret(Args...)> &rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_copy_equal_time++;
#endif
    (void)assign(rhs);
    return *this;
  }
  // move assign operator
  ObFunction &operator=(ObFunction<Ret(Args...)> &&rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_move_equal_time++;
#endif
    (void)assign(std::forward<ObFunction<Ret(Args...)>>(rhs));
    return *this;
  }
  // general assign operator
  template <typename Fn,
  typename std::enable_if<
  !std::is_same<typename std::decay<Fn>::type, ObFunction<Ret(Args...)>>::value, bool>::type = true>
  ObFunction &operator=(Fn &&rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_general_equal_time++;
#endif
    (void)assign(std::forward<Fn>(rhs));
    return *this;
  }
  // destructor
  ~ObFunction() { reset(); }
  // function operator
  Ret operator()(Args ...args) const {
    assert(nullptr != base_);
    return base_->invoke(std::forward<Args>(args)...);
  }

/***************************[OB style interface]********************************/
  void reset() {
    if (OB_LIKELY(base_ != nullptr)) {
      base_->~Abstract();
      if (!is_local_obj_()) {
        allocator_.free(base_);
      }
      base_ = nullptr;
    }
  }
  bool is_valid() const { return nullptr != base_; }
  // copy assign
  int assign(const ObFunction<Ret(Args...)> &rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_copy_assign_time++;
#endif
    return base_assign_(rhs.base_);
  }
  // move assign
  int assign(ObFunction<Ret(Args...)> &&rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_move_assign_time++;
#endif
    int ret = OB_SUCCESS;
    if ((&allocator_ == &rhs.allocator_) && !is_local_obj_() && !rhs.is_local_obj_()) {
      // same allocator, both self and rhs are big obj
      // just swap pointer, rhs will destory and destruct this->base_
      Abstract *temp = base_;
      base_ = rhs.base_;
      rhs.base_ = temp;
    } else {// if allocator not same, or no heap obj, should not move
      ret = base_assign_(rhs.base_);
      if (OB_SUCC(ret)) {
        rhs.reset();
      }
    }
    return ret;
  }
  // general assign
  template <typename Fn,
  typename std::enable_if<
    !std::is_same<typename std::decay<Fn>::type, ObFunction<Ret(Args...)>>::value, bool>::type = true>
  int assign(Fn &&rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_general_assign_time++;
#endif
    int ret = OB_SUCCESS;
    reset();
    if (sizeof(Derived<typename std::decay<Fn>::type>) <= function::SMALL_OBJ_MAX_SIZE) {
      base_ = (Abstract*)local_buffer_;
    } else {
      base_ = (Abstract*)allocator_.alloc(sizeof(Derived<typename std::decay<Fn>::type>));
    }
    if (OB_LIKELY(nullptr != base_)) {
      base_ = new (base_)Derived<typename std::decay<Fn>::type>(std::forward<Fn>(rhs));
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(WARN, "no memory, function is invalid", K(ret), KP_(base), KP(&allocator_),
                   KP(&DEFAULT_ALLOCATOR), K(lbt()));
    }
    return ret;
  }
  TO_STRING_KV(KP(this), KP_(base), KP(&allocator_),
               KP(&DEFAULT_ALLOCATOR));
private:
  int base_assign_(Abstract *rhs) {
#ifdef UNITTEST_DEBUG
    RECORDER.function_base_assign_time++;
#endif
    int ret = OB_SUCCESS;
    reset();
    if (rhs != nullptr) {
      base_ = rhs->copy(allocator_, local_buffer_);
      if (nullptr == base_) {// allocate failed
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OCCAM_LOG(WARN, "no memory, function is invalid", K(ret), KP_(base), KP(&allocator_),
               KP(&DEFAULT_ALLOCATOR), K(lbt()));
      }
    }
    return ret;
  }
  bool is_local_obj_() {
    return  ((void *)base_) == ((void*)local_buffer_);
  }
  Abstract *base_;
  ObIAllocator &allocator_;
  unsigned char local_buffer_[function::SMALL_OBJ_MAX_SIZE];
};
#undef RECORDER
#undef DEFAULT_ALLOCATOR

}// namespace common
}// namespace oceanbase
#endif
