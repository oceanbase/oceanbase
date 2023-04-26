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

#ifndef CONTEXT_H_
#define CONTEXT_H_

#include <functional>
#include <type_traits>
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/lds/ob_lds_define.h"
#include "lib/lds/ob_lds_assist.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/utility/utility.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator_v2.h"
#include "lib/allocator/ob_page_manager.h"
#include "lib/allocator/page_arena.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_clock_generator.h"

#ifdef OB_USE_ASAN
#include "lib/allocator/ob_asan_allocator.h"
#endif
namespace oceanbase
{
namespace lib
{
#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1):__FILE__)

#define CURRENT_CONTEXT lib::Flow::current_ctx()
#define ROOT_CONTEXT lib::MemoryContext::root()
#define CREATE_CONTEXT(args...) \
  CREATE_CONTEXT_(CONCAT(static_info, __COUNTER__), args)
#define DESTROY_CONTEXT(context) lib::__MemoryContext__::destory_context(context)
#define WITH_CONTEXT(context) WITH_CONTEXT_P(true, context)
#define WITH_CONTEXT_P(condition, context) \
  CONTEXT_P(condition, lib::ContextSource::WITH, context)
#define CREATE_WITH_TEMP_CONTEXT(...) CREATE_WITH_TEMP_CONTEXT_P(true, __VA_ARGS__)
#define CREATE_WITH_TEMP_CONTEXT_P(condition, args...) \
  CREATE_WITH_TEMP_CONTEXT_P_(CONCAT(static_info, __COUNTER__), condition, args)
// The following are auxiliary macros
#define CREATE_CONTEXT_(static_info, context, args...)                                                   \
  create_context(context, lib::DynamicInfo(), args,                                                      \
                 ({                                                                                      \
                    static lib::StaticInfo static_info{__FILENAME__, __LINE__, __FUNCTION__};            \
                    &static_info;                                                                        \
                 }))
#define CONTEXT_P(condition, context_source, ...)                                                \
  for (lib::_S<context_source> _s{condition, __VA_ARGS__}; OB_SUCC(ret) && _s.i_-- > 0; _s.i_--) \
    if (OB_SUCC(_s.get_ret()))
#define CREATE_WITH_TEMP_CONTEXT_P_(static_info, condition, ...)                                \
  static lib::StaticInfo static_info{__FILENAME__, __LINE__, __FUNCTION__};                     \
  CONTEXT_P(condition, lib::ContextSource::CREATE,                                              \
            lib::DynamicInfo(), __VA_ARGS__, &static_info)


using std::nullptr_t;
using lib::ObMemAttr;
class Flow;
class __MemoryContext__;
enum class ContextSource
{
  WITH,      // The parameter is already the target Context, switch directly
  CREATE     // Created by parameters
};

class ContextTLOptGuard
{
  friend class __MemoryContext__;
  RLOCAL_STATIC(bool, enable_tl_opt);
public:
  ContextTLOptGuard(const bool enable_flag)
    : enable_flag_bak_(enable_tl_opt)
  {
    enable_tl_opt = enable_flag;
  }
  ~ContextTLOptGuard()
  {
    enable_tl_opt = enable_flag_bak_;
  }
private:
  const bool enable_flag_bak_;
};

static const uint64_t DEFAULT_PROPERTIES = 0;
enum ContextPropertyEnum
{
  ADD_CHILD_THREAD_SAFE     = 1 << 1,
  ALLOC_THREAD_SAFE         = 1 << 2,
  USE_TL_PAGE_OPTIONAL      = 1 << 3, // By default, TL(Thread Local) optimization is not used, OPTINAL will read the local state of the thread to determine
  // Determine whether the allocator returned by the context get_allocator interface is malloc or arena
  RETURN_MALLOC_DEFAULT     = 1 << 4
};

class TreeNode
{
public:
  TreeNode(TreeNode *parent,
          const bool with_lock)
    : parent_(parent),
      child_(nullptr),
      prev_(nullptr),
      next_(nullptr),
      with_lock_(with_lock),
      lock_(0)
  {}

  void init()
  {
    if (OB_LIKELY(parent_ != nullptr)) {
      auto *parent = parent_;
      parent_ = nullptr;
      parent->add_child(*this);
    }
  }

  void deinit()
  {
    if (OB_LIKELY(parent_ != nullptr)) {
      parent_->del_child(*this);
    }
  }

  void add_child(TreeNode &node)
  {
    if (OB_UNLIKELY(with_lock_)) {
      while (ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    abort_unless(nullptr == node.parent_ &&
        nullptr == node.prev_ && nullptr == node.next_);
    node.parent_ = this;
    if (nullptr == child_) {
      child_ = &node;
    } else {
      node.next_ = child_->next_;
      if (child_->next_ != nullptr) {
        child_->next_->prev_ = &node;
      }
      child_->next_ = &node;
      node.prev_ = child_;
    }
    if (OB_UNLIKELY(with_lock_)) {
      ATOMIC_STORE(&lock_, 0);
    }
  }

  void del_child(TreeNode &node)
  {
    if (OB_UNLIKELY(with_lock_)) {
      while (ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    abort_unless(node.parent_ == this);
    node.parent_ = nullptr;
    TreeNode *prev = node.prev_;
    TreeNode *next = node.next_;
    if (prev != nullptr) {
      prev->next_ = next;
    }
    if (next != nullptr) {
      next->prev_ = prev;
    }
    node.prev_ = node.next_ = nullptr;
    if (this->child_ == &node) {
      abort_unless(prev == nullptr);
      this->child_ = next;
    }
    if (OB_UNLIKELY(with_lock_)) {
      ATOMIC_STORE(&lock_, 0);
    }
  }

  TO_STRING_KV(KP(parent_));

  TreeNode *parent_;
  TreeNode *child_;
  TreeNode *prev_;
  TreeNode *next_;
  const bool with_lock_;
  int64_t lock_;
};

class ContextParam
{
  friend class __MemoryContext__;
public:
  ContextParam()
    : properties_(DEFAULT_PROPERTIES),
      attr_(),
      page_size_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
      ablock_size_(lib::INTACT_NORMAL_AOBJECT_SIZE),
      parallel_(1)
  {}
  ContextParam &set_properties(int64_t properties)
  {
    properties_ = properties;
    return *this;
  }
  template<typename ... Args>
  ContextParam &set_mem_attr(Args && ... args)
  {
    attr_ = ObMemAttr(args...);
    return *this;
  }
  ContextParam &set_label(const ObLabel &label)
  {
    attr_.label_ = label;
    return *this;
  }
  ContextParam &set_page_size(int64_t page_size)
  {
    page_size_ = page_size;
    return *this;
  }
  ContextParam &set_ablock_size(uint32_t ablock_size)
  {
    ablock_size_ = ablock_size;
    return *this;
  }
  ContextParam &set_parallel(int parallel)
  {
    parallel_ = parallel;
    return *this;
  }
private:
  int64_t properties_;
  ObMemAttr attr_;
  int64_t page_size_;
  uint32_t ablock_size_;
  int parallel_;
};

struct StaticInfo
{
  const char *filename_;
  int line_;
  const char *function_;
  TO_STRING_KV(KCSTRING(filename_), K(line_), KCSTRING(function_));
};

struct DynamicInfo
{
  DynamicInfo()
    : tid_(GETTID()), cid_(0lu),
      create_time_(common::ObClockGenerator::getClock())
  {}
  TO_STRING_KV(K(tid_), K(cid_), K(create_time_));
  int64_t tid_;
  int64_t cid_;
  int64_t create_time_;
};

class MemoryContext
{
  friend class __MemoryContext__;
  constexpr static int64_t MAGIC_CODE = 0xedde13244231dede;
public:
  MemoryContext(__MemoryContext__ *ref_context)
  {
    operator=(ref_context);
  }
  MemoryContext()
  {
    operator=(nullptr);
  }
  MemoryContext &operator=(__MemoryContext__ *ref_context);
  __MemoryContext__* operator->() const
  { return ref_context_; }
  __MemoryContext__ *ref_context() const
  { return ref_context_; }
  bool check_magic_code() const { return MAGIC_CODE == magic_code_; }
  static MemoryContext &root();
private:
  int64_t magic_code_;
  int64_t seq_id_;
  __MemoryContext__ *ref_context_;
};

class __MemoryContext__
{
  friend class TreeNode;
  friend class MemoryContext;
  constexpr static int64_t MAGIC_CODE = 0xfddf13244231dfdf;
public:
  static int64_t gen_seq_id()
  {
    static __thread uint32_t local_id = 0;
    return common::get_itid() << 32 | local_id++;
  }
public:
  __MemoryContext__(const bool need_free, const DynamicInfo &di, __MemoryContext__ *parent,
                ContextParam &param, const StaticInfo *static_info)
    : magic_code_(-1),
      seq_id_(-1),
      need_free_(need_free),
      tree_node_(parent != nullptr ? &parent->tree_node_ : nullptr,
                 param.properties_ & ADD_CHILD_THREAD_SAFE),
      di_(di),
      param_(param),
      properties_(param.properties_),
      static_id_(reinterpret_cast<int64_t>(static_info)),
      p_alloc_(nullptr),
      p_arena_alloc_(nullptr),
      p_safe_arena_alloc_(nullptr),
      parallel_alloc_(nullptr),
      freeable_alloc_(nullptr),
      default_allocator_(nullptr)
  {
  #ifdef OB_USE_ASAN
    use_asan_allocator_ = enable_asan_allocator;
  #endif
  }
  int64_t get_static_id() const { return static_id_; }
  const DynamicInfo &get_dynamic_info() const { return di_; }
  const StaticInfo &get_static_info() const { return *reinterpret_cast<StaticInfo*>(static_id_); }
  void *allocf(const int64_t size,
               const ObMemAttr &attr)
  {
    return freeable_alloc_->alloc(size, attr);
  }
  void *allocp(const int64_t size)
  {
    void *ptr = nullptr;
    ptr = arena_alloc_.alloc(size);
    return ptr;
  }
  void free(void *ptr)
  {
    freeable_alloc_->free(ptr);
  }
  void reuse()
  {
    reuse_arena();
  }
  void reuse_arena()
  {
    arena_alloc_.reuse();
  }
  void reset_remain_one_page()
  {
    arena_alloc_.reset_remain_one_page();
  }
  int64_t malloc_hold() const
  {
    return hold() - arena_hold();
  }
  int64_t malloc_used() const
  {
    return freeable_alloc_->used() - arena_hold();
  }
  int64_t arena_hold() const
  {
    return arena_alloc_.total();
  }
  int64_t arena_used() const
  {
    return arena_alloc_.used();
  }
  int64_t hold() const
  {
    return freeable_alloc_->total();
  }
  int64_t used() const
  {
    return malloc_used() + arena_used();
  }

  ObIAllocator &get_malloc_allocator() { return *freeable_alloc_; }
  common::ObArenaAllocator &get_arena_allocator() { return arena_alloc_; }
  common::ObSafeArenaAllocator &get_safe_arena_allocator() { return safe_arena_alloc_; }
  ObIAllocator &get_allocator()
  {
    OB_ASSERT(default_allocator_ != nullptr);
    return *default_allocator_;
  }
  static __MemoryContext__ &root();
  bool check_magic_code() const { return MAGIC_CODE == magic_code_; }
  TO_STRING_KV(KP(this),
               "static_id", static_id_,
               "static_info", *reinterpret_cast<StaticInfo*>(static_id_),
               "dynamic info", di_,
               K(properties_), K(attr_));
private:
  static __MemoryContext__ *node2context(TreeNode *node)
  {
DISABLE_WARNING_GCC_PUSH
DISABLE_WARNING_GCC("-Winvalid-offsetof")
    return reinterpret_cast<__MemoryContext__*>((char*)node - offsetof(__MemoryContext__, tree_node_));
DISABLE_WARNING_GCC_POP
  }
public:
  int init()
  {
    tree_node_.init();
    int ret = common::OB_SUCCESS;
    // change tenant_id
    ObMemAttr inner_attr = param_.attr_;
    auto *ma = ObMallocAllocator::get_instance();
    // tenant_allocator is created synchronously when the tenant is built, and 500 tenant memory is used when there is no such tenant
    auto ta = ma->get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_);
    if (nullptr == ta) {
      inner_attr.tenant_id_ = common::OB_SERVER_TENANT_ID;
    }
    attr_ = inner_attr;
    // init allocator
    const bool thread_safe = param_.properties_ & ALLOC_THREAD_SAFE;
    uint32_t ablock_size = param_.ablock_size_;
    if (ablock_size != lib::INTACT_MIDDLE_AOBJECT_SIZE) {
      ablock_size = lib::INTACT_NORMAL_AOBJECT_SIZE;
    }
#ifdef OB_USE_ASAN
    if (use_asan_allocator_) {
      ret = init_asan_alloc(asan_alloc_, thread_safe);
    } else {
      ret = init_alloc(alloc_, thread_safe, ablock_size);
    }
#else
    ret = init_alloc(alloc_, thread_safe, ablock_size);
#endif
    if (OB_SUCC(ret)) {
      // init arena allocator
      p_arena_alloc_ = new (&arena_alloc_) common::ObArenaAllocator(*p_alloc_, param_.page_size_,
                                                                    true/*enable_sanity*/);
      arena_alloc_.set_attr(attr_);
      p_safe_arena_alloc_ = new (&safe_arena_alloc_) common::ObSafeArenaAllocator(arena_alloc_);
      default_allocator_ = (param_.properties_ & RETURN_MALLOC_DEFAULT) ?
      static_cast<ObIAllocator*>(freeable_alloc_) : static_cast<ObIAllocator*>(&arena_alloc_);
    }
    if (OB_FAIL(ret)) {
      deinit();
    }
    return ret;
  }
  int init_alloc(common::ObAllocator& allocator, const bool thread_safe, uint32_t ablock_size)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(thread_safe)) {
      p_alloc_ = new (&allocator) common::ObAllocator(this, attr_, false/*use_pm*/, ablock_size);
      void *ptr = allocator.alloc(sizeof(common::ObParallelAllocator));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        parallel_alloc_ = new (ptr) common::ObParallelAllocator(allocator, this, attr_, param_.parallel_, ablock_size);
        freeable_alloc_ = parallel_alloc_;
      }
    } else {
      bool use_pm = false;
      if ((param_.properties_ & USE_TL_PAGE_OPTIONAL)
          && ContextTLOptGuard::enable_tl_opt) {
        use_pm = true;
      }
      p_alloc_ = new (&allocator) common::ObAllocator(this, attr_, use_pm, ablock_size);
      freeable_alloc_ = p_alloc_;
    }
    return ret;
  }

  #ifdef OB_USE_ASAN
  int init_asan_alloc(common::ObAsanAllocator& allocator, const bool thread_safe)
  {
    int ret = common::OB_SUCCESS;
    p_alloc_ = new (&allocator) common::ObAsanAllocator;
    if (OB_UNLIKELY(thread_safe)) {
      void *ptr = allocator.alloc(sizeof(common::ObParallelAsanAllocator));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        parallel_alloc_ = new (ptr) common::ObParallelAsanAllocator(allocator, param_.parallel_);
        freeable_alloc_ = parallel_alloc_;
      }
    } else {
      freeable_alloc_ = p_alloc_;
    }
    return ret;
  }
  #endif

  void deinit()
  {
    default_allocator_ = nullptr;
    freeable_alloc_ =  nullptr;
    if (p_safe_arena_alloc_ != nullptr) {
      p_safe_arena_alloc_->~ObSafeArenaAllocator();
      p_safe_arena_alloc_ = nullptr;
    }
    if (p_arena_alloc_ != nullptr) {
      p_arena_alloc_->~ObArenaAllocator();
      p_arena_alloc_ = nullptr;
    }
    if (parallel_alloc_ != nullptr) {
      parallel_alloc_->~ObIAllocator();
      abort_unless(p_alloc_ != nullptr);
      p_alloc_->free(parallel_alloc_);
      parallel_alloc_ = nullptr;
    }
    if (p_alloc_ != nullptr) {
      p_alloc_->~ObIAllocator();
      p_alloc_ = nullptr;
    }
    tree_node_.deinit();
  }
#ifdef OB_USE_ASAN
  static void set_enable_asan_allocator(bool enable)
  {
    enable_asan_allocator = enable;
  }
#endif
  template<typename ... Args>
  int create_context(MemoryContext &context,
                     const DynamicInfo &di,
                     Args && ... args)
  {
    int ret = common::OB_SUCCESS;
    __MemoryContext__ *ref_context = nullptr;

    ObMemAttr attr;
    attr.label_ = "CreateContext";
    void *ptr = allocf(sizeof(__MemoryContext__), attr);
    if (OB_UNLIKELY(nullptr == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ref_context = new (ptr) __MemoryContext__(/*need_free*/true, di, this, args...);
      if (OB_FAIL(ref_context->init())) {
        OB_LOG(WARN, "init failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      if (ref_context != nullptr) {
        ref_context->deinit();
      }
      if (ptr != nullptr) {
        free(ptr);
      }
    } else {
      ref_context->magic_code_ = MAGIC_CODE;
      ref_context->seq_id_ = gen_seq_id();
      context = ref_context;
    }
    return ret;
  }
  template<typename ... Args>
  int create_context(MemoryContext &context,
                     __MemoryContext__ &ref_context,
                     const DynamicInfo &di,
                     Args && ... args)
  {
    int ret = common::OB_SUCCESS;

    new (&ref_context) __MemoryContext__(/*need_free*/false, di, this, args...);
    if (OB_FAIL(ref_context.init())) {
      OB_LOG(WARN, "init failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      ref_context.deinit();
    } else {
      ref_context.magic_code_ = MAGIC_CODE;
      ref_context.seq_id_ = gen_seq_id();
      context = &ref_context;
    }

    return ret;
  }
  static void destory_context(__MemoryContext__ *context)
  {
    abort_unless(context->check_magic_code());
    context->magic_code_ = 0;
    context->seq_id_ = 0;
    TreeNode *child_node = nullptr;
    while ((child_node = context->tree_node_.child_) != nullptr) {
      __MemoryContext__ *child = node2context(child_node);
      destory_context(child);
    }
    const bool need_free = context->need_free_;
    TreeNode *parent_node = context->tree_node_.parent_;
    abort_unless(parent_node != nullptr);
    context->deinit();
    if (need_free) {
      __MemoryContext__ *parent = node2context(parent_node);
      parent->free(context);
    }
  }
  static void destory_context(MemoryContext &context)
  {
    abort_unless(context.check_magic_code());
    auto *ref_context = context.ref_context_;
    if (OB_LIKELY(ref_context != nullptr)) {
      abort_unless(context.seq_id_ == ref_context->seq_id_);
      destory_context(ref_context);
    }
  }
public:
  int64_t magic_code_;
  int64_t seq_id_;
  // Assignment is not in the constructor, record who assigns
  const bool need_free_;
  TreeNode tree_node_;
  DynamicInfo di_;
  ContextParam param_;
  int64_t properties_;
  int64_t static_id_;
  common::ObMemAttr attr_;

  // Delayed member
  union {
    common::ObAllocator alloc_;
#ifdef OB_USE_ASAN
    common::ObAsanAllocator asan_alloc_;
#endif
  };
  union {
    common::ObArenaAllocator arena_alloc_;
  };
  union {
    common::ObSafeArenaAllocator safe_arena_alloc_;
  };
  common::ObIAllocator *p_alloc_;

  common::ObArenaAllocator *p_arena_alloc_;
  common::ObSafeArenaAllocator *p_safe_arena_alloc_;
  common::ObIAllocator *parallel_alloc_;

  // Allocators that require explicit free, p_alloc_ or parallel_alloc_
  common::ObIAllocator *freeable_alloc_;
  // Allocator returned by get_allocator()
  ObIAllocator *default_allocator_;
#ifdef OB_USE_ASAN
private:
  static bool enable_asan_allocator;
  bool use_asan_allocator_;
#endif
};

inline MemoryContext &MemoryContext::operator=(__MemoryContext__ *ref_context)
{
  ref_context_ = ref_context;
  if (OB_LIKELY(ref_context != nullptr)) {
    seq_id_ = ref_context->seq_id_;
  } else {
    seq_id_ = -1;
  }
  magic_code_ = MAGIC_CODE;
  return *this;
}

inline bool operator==(const MemoryContext &__a, nullptr_t)
{ return __a.ref_context() == nullptr; }

inline bool operator==(nullptr_t, const MemoryContext &__b)
{ return nullptr == __b.ref_context(); }

inline bool operator!=(const MemoryContext &__a, nullptr_t)
{ return __a.ref_context() != nullptr; }

inline bool operator!=(nullptr_t, const MemoryContext &__b)
{ return nullptr != __b.ref_context(); }

inline bool operator==(const MemoryContext &__a, const MemoryContext &__b)
{ return __a.ref_context() == __b.ref_context(); }

inline bool operator!=(const MemoryContext &__a, const MemoryContext &__b)
{ return __a.ref_context() != __b.ref_context(); }

class Flow final
{
public:
  Flow(MemoryContext &ref_context)
    : ref_context_(ref_context),
      prev_(nullptr),
      next_(nullptr),
      is_inited_(false)
  {}
  MemoryContext &context() { return ref_context_; }
  int init()
  {
    int ret = common::OB_SUCCESS;
    Flow *&cur = g_flow();
    if (nullptr == cur) {
      cur = this;
    } else {
      abort_unless(cur != this);
      cur->next_ = this;
      this->prev_ = cur;
      cur = this;
    }
    is_inited_ = true;
    return ret;
  }
  void deinit()
  {
    if (is_inited_) {
      Flow *&cur = g_flow();
      abort_unless(cur == this);
      Flow *parent = cur->prev_;
      if (nullptr == parent) {
        cur = nullptr;
      } else {
        parent->next_ = nullptr;
        cur->prev_ = nullptr;
        cur = parent;
      }
    }
  }
  static Flow &current_flow()
  {
    Flow *&cur = g_flow();
    if (OB_ISNULL(cur)) {
      struct FlowBuf {
        char v_[sizeof(Flow)];
      };
      RLOCAL_INLINE(FlowBuf, buf);
      Flow *flow = new ((&buf)->v_) Flow(ROOT_CONTEXT);
      abort_unless(flow != nullptr);
      int ret = flow->init();
      abort_unless(common::OB_SUCCESS == ret);
      cur = flow;
    }
    return *cur;
  }
  static MemoryContext &current_ctx()
  {
    return current_flow().ref_context_;
  }
  Flow &parent()
  {
    abort_unless(prev_ != nullptr);
    return *prev_;
  }
private:
  static Flow *&g_flow()
  {
    RLOCAL_INLINE(Flow*, g_flow);
    return g_flow;
  }
private:
  MemoryContext ref_context_;
  Flow *prev_;
  Flow *next_;
  bool is_inited_;
};

inline void *ctxalf(const int64_t size,
                    const ObMemAttr &attr)
{
  return CURRENT_CONTEXT->allocf(size, attr);
}

inline void *ctxalp(const int64_t size)
{
  return CURRENT_CONTEXT->allocp(size);
}

inline void ctxfree(void *ptr)
{
  CURRENT_CONTEXT->free(ptr);
}

class _SBase
{
public:
  int get_ret() const
  {
    return ret_;
  }
  _SBase()
    : i_(1),
      ret_(common::OB_SUCCESS)
  {}
  ~_SBase()
  {
    if (OB_UNLIKELY(0 == i_)) {
      OB_LOG_RET(WARN, OB_ERROR, "has break statement!!!");
    }
  }
  int i_;
  int ret_;
};

template<ContextSource es> class _S {};

template<>
class _S<ContextSource::WITH> : public _SBase
{
public:
  _S(const bool condition, MemoryContext &context)
    : _SBase(), flow_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(context)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (condition) {
      Flow *tmp_flow = new (buf_) Flow(context);
      if (OB_FAIL(tmp_flow->init())) {
      } else {
        flow_ = tmp_flow;
      }
    }
    ret_ = ret;
  }
  ~_S()
  {
    if (flow_ != nullptr) {
      flow_->deinit();
      flow_->~Flow();
    }
  }
  char buf_[sizeof(Flow)] __attribute__ ((aligned (16)));
  Flow *flow_;
};

template<>
class _S<ContextSource::CREATE> : public _SBase
{
public:
  template<typename ... Args>
  _S(const bool condition, Args && ... args)
    : _SBase(), flow_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(condition)) {
      __MemoryContext__ *tmp_context = reinterpret_cast<__MemoryContext__*>(buf0_);
      if (OB_FAIL(CURRENT_CONTEXT->create_context(context_, *tmp_context, args...))) {
        OB_LOG(WARN, "create context failed", K(ret));
      } else {
        Flow *tmp_flow = new (buf1_) Flow(context_);
        if (OB_FAIL(tmp_flow->init())) {
        } else {
          flow_ = tmp_flow;
        }
      }
    }
    ret_ = ret;
  }
  ~_S()
  {
    if (flow_ != nullptr) {
      flow_->deinit();
      flow_->~Flow();
    }
    if (context_ != nullptr) {
      __MemoryContext__::destory_context(context_);
    }
  }
  char buf0_[sizeof(__MemoryContext__)] __attribute__ ((aligned (16)));
  char buf1_[sizeof(Flow)] __attribute__ ((aligned (16)));
  MemoryContext context_;
  Flow *flow_;
};

} // end of namespace lib
} // end of namespace oceanbase

#endif // CONTEXT_H_
