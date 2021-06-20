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

namespace oceanbase {
namespace lib {
#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)

#define CURRENT_CONTEXT lib::Flow::current_ctx()
#define ROOT_CONTEXT lib::MemoryContext::root()
#define CREATE_CONTEXT(args...) CREATE_CONTEXT_(CONCAT(static_id, __COUNTER__), args)
#define DESTROY_CONTEXT(context) lib::MemoryContext::destory_context(context)
#define WITH_CONTEXT(context) WITH_CONTEXT_P(true, context)
#define WITH_CONTEXT_P(condition, context) CONTEXT_P(condition, lib::ContextSource::WITH, context)
#define CREATE_WITH_TEMP_CONTEXT(...) CREATE_WITH_TEMP_CONTEXT_P(true, __VA_ARGS__)
#define CREATE_WITH_TEMP_CONTEXT_P(condition, args...) \
  CREATE_WITH_TEMP_CONTEXT_P_(CONCAT(static_id, __COUNTER__), condition, args)
// The following are auxiliary macros
#define CREATE_CONTEXT_(static_id, context, args...)                                                         \
  create_context(context, lib::DynamicInfo(), args, ({                                                       \
    const static int static_id = lib::StaticInfos::get_instance().add(__FILENAME__, __LINE__, __FUNCTION__); \
    static_id;                                                                                               \
  }))
#define CONTEXT_P(condition, context_source, ...)                                                \
  for (lib::_S<context_source> _s{condition, __VA_ARGS__}; OB_SUCC(ret) && _s.i_-- > 0; _s.i_--) \
    if (OB_SUCC(_s.get_ret()))
#define CREATE_WITH_TEMP_CONTEXT_P_(static_id, condition, ...)                                             \
  const static int static_id = lib::StaticInfos::get_instance().add(__FILENAME__, __LINE__, __FUNCTION__); \
  CONTEXT_P(condition, lib::ContextSource::CREATE, lib::DynamicInfo(), __VA_ARGS__, static_id)

using lib::ObMemAttr;
using oceanbase::common::default_memattr;
class Flow;
class MemoryContext;
enum class ContextSource {
  WITH,   // The parameter is already the target Context, switch directly
  CREATE  // Created by parameters
};

class ContextTLOptGuard {
  friend class MemoryContext;
  static RLOCAL(bool, enable_tl_opt);

public:
  ContextTLOptGuard(const bool enable_flag) : enable_flag_bak_(enable_tl_opt)
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
enum ContextPropertyEnum {
  ADD_CHILD_THREAD_SAFE = 1 << 1,
  ALLOC_THREAD_SAFE = 1 << 2,
  USE_TL_PAGE_OPTIONAL =
      1 << 3,  // By default, TL optimization is not used, OPTINAL will read the local state of the thread to determine
  // Determine whether the allocator returned by the context get_allocator interface is malloc or arena
  RETURN_MALLOC_DEFAULT = 1 << 4
};

class TreeNode {
public:
  TreeNode(TreeNode* parent, const bool with_lock)
      : parent_(parent), child_(nullptr), prev_(nullptr), next_(nullptr), with_lock_(with_lock), lock_(0)
  {}

  void init()
  {
    if (OB_LIKELY(parent_ != nullptr)) {
      auto* parent = parent_;
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

  void add_child(TreeNode& node)
  {
    if (OB_UNLIKELY(with_lock_)) {
      while (ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    abort_unless(nullptr == node.parent_ && nullptr == node.prev_ && nullptr == node.next_);
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

  void del_child(TreeNode& node)
  {
    if (OB_UNLIKELY(with_lock_)) {
      while (ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    abort_unless(node.parent_ == this);
    node.parent_ = nullptr;
    TreeNode* prev = node.prev_;
    TreeNode* next = node.next_;
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

  TreeNode* parent_;
  TreeNode* child_;
  TreeNode* prev_;
  TreeNode* next_;
  const bool with_lock_;
  int64_t lock_;
};

class ContextParam {
  friend class MemoryContext;

public:
  ContextParam()
      : properties_(DEFAULT_PROPERTIES),
        attr_(),
        page_size_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        ablock_size_(lib::INTACT_NORMAL_AOBJECT_SIZE),
        parallel_(1)
  {}
  ContextParam& set_properties(int64_t properties)
  {
    properties_ = properties;
    return *this;
  }
  template <typename... Args>
  ContextParam& set_mem_attr(Args&&... args)
  {
    attr_ = ObMemAttr(args...);
    return *this;
  }
  ContextParam& set_label(const ObLabel& label)
  {
    attr_.label_ = label;
    return *this;
  }
  ContextParam& set_page_size(int64_t page_size)
  {
    page_size_ = page_size;
    return *this;
  }
  ContextParam& set_ablock_size(uint32_t ablock_size)
  {
    ablock_size_ = ablock_size;
    return *this;
  }
  ContextParam& set_parallel(int parallel)
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

struct StaticInfo {
  const char* filename_;
  int line_;
  const char* function_;
  TO_STRING_KV(K(filename_), K(line_), K(function_));
};

class StaticInfos {
public:
  StaticInfos() : cnt_(0)
  {}
  static StaticInfos& get_instance()
  {
    static StaticInfos one;
    return one;
  }
  int get_cnt() const
  {
    return cnt_;
  }
  int add(const char* filename, const int line, const char* function)
  {
    int pos = ATOMIC_FAA(&cnt_, 1);
    abort_unless(pos < MAX_NUM && nullptr == infos_[pos].filename_);
    infos_[pos] = {filename, line, function};
    const int static_id = pos;
    OB_LOG(INFO, "add info", K(static_id), "info", infos_[static_id]);
    return static_id;
  }
  const StaticInfo& get(const int static_id) const
  {
    return infos_[static_id];
  }

private:
  static const int MAX_NUM = 128;
  StaticInfo infos_[MAX_NUM];
  int cnt_;
};

struct DynamicInfo {
  DynamicInfo()
      : tid_(GETTID()), cid_(CO_IS_ENABLED() ? CO_ID() : 0lu), create_time_(common::ObTimeUtility::fast_current_time())
  {}
  TO_STRING_KV(K(tid_), K(cid_), K(create_time_));
  int64_t tid_;
  int64_t cid_;
  int64_t create_time_;
};

class MemoryContext {
  friend class TreeNode;

public:
  MemoryContext(
      const bool need_free, const DynamicInfo& di, MemoryContext* parent, ContextParam& param, const int static_id)
      : need_free_(need_free),
        tree_node_(parent != nullptr ? &parent->tree_node_ : nullptr, param.properties_ & ADD_CHILD_THREAD_SAFE),
        di_(di),
        param_(param),
        properties_(param.properties_),
        static_id_(static_id),
        p_alloc_(nullptr),
        p_arena_alloc_(nullptr),
        p_safe_arena_alloc_(nullptr),
        parallel_alloc_(nullptr),
        freeable_alloc_(nullptr),
        default_allocator_(nullptr)
  {}
  int get_static_id() const
  {
    return static_id_;
  }
  const DynamicInfo& get_dynamic_info() const
  {
    return di_;
  }
  const StaticInfo& get_static_info() const
  {
    return StaticInfos::get_instance().get(static_id_);
  }
  void* allocf(const int64_t size, const ObMemAttr& attr = default_memattr)
  {
    return freeable_alloc_->alloc(size, attr);
  }
  void* allocp(const int64_t size, const ObMemAttr& attr = default_memattr)
  {
    UNUSEDx(attr);
    void* ptr = nullptr;
    ptr = arena_alloc_.alloc(size);
    return ptr;
  }
  void free(void* ptr)
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
  ObIAllocator& get_malloc_allocator()
  {
    return *freeable_alloc_;
  }
  common::ObArenaAllocator& get_arena_allocator()
  {
    return arena_alloc_;
  }
  common::ObSafeArenaAllocator& get_safe_arena_allocator()
  {
    return safe_arena_alloc_;
  }
  ObIAllocator& get_allocator()
  {
    OB_ASSERT(default_allocator_ != nullptr);
    return *default_allocator_;
  }
  static MemoryContext& root();
  TO_STRING_KV(KP(this), "static_id", static_id_, "static_info", StaticInfos::get_instance().get(static_id_),
      "dynamic info", di_, K(properties_), K(attr_));

private:
  static MemoryContext* node2context(TreeNode* node)
  {
    return reinterpret_cast<MemoryContext*>((char*)node - offsetof(MemoryContext, tree_node_));
  }

public:
  int init()
  {
    tree_node_.init();
    int ret = OB_SUCCESS;
    // change tenant_id
    ObMemAttr inner_attr = param_.attr_;
    auto* ma = ObMallocAllocator::get_instance();
    // tenant_allocator is created synchronously when the tenant is built, and 500 tenant memory is used when there is
    // no such tenant
    ObTenantCtxAllocator* ta =
        ma != nullptr ? ma->get_tenant_ctx_allocator(inner_attr.tenant_id_, inner_attr.ctx_id_) : nullptr;
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
    if (OB_UNLIKELY(thread_safe)) {
      p_alloc_ = new (&alloc_) common::ObAllocator(this, inner_attr, false /*use_pm*/, ablock_size);
      void* ptr = alloc_.alloc(sizeof(common::ObParallelAllocator));
      if (OB_UNLIKELY(nullptr == ptr)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        parallel_alloc_ =
            new (ptr) common::ObParallelAllocator(alloc_, this, inner_attr, param_.parallel_, ablock_size);
        freeable_alloc_ = parallel_alloc_;
      }
    } else {
      bool use_pm = false;
      if ((param_.properties_ & USE_TL_PAGE_OPTIONAL) && ContextTLOptGuard::enable_tl_opt) {
        use_pm = true;
      }
      p_alloc_ = new (&alloc_) common::ObAllocator(this, inner_attr, use_pm, ablock_size);
      freeable_alloc_ = p_alloc_;
    }
    if (OB_SUCC(ret)) {
      // init arena allocator
      p_arena_alloc_ = new (&arena_alloc_) common::ObArenaAllocator(*p_alloc_, param_.page_size_);
      arena_alloc_.set_attr(inner_attr);
      p_safe_arena_alloc_ = new (&safe_arena_alloc_) common::ObSafeArenaAllocator(arena_alloc_);
      default_allocator_ = (param_.properties_ & RETURN_MALLOC_DEFAULT) ? static_cast<ObIAllocator*>(freeable_alloc_)
                                                                        : static_cast<ObIAllocator*>(&arena_alloc_);
    }
    if (OB_FAIL(ret)) {
      deinit();
    }
    return ret;
  }
  void deinit()
  {
    default_allocator_ = nullptr;
    freeable_alloc_ = nullptr;
    if (p_safe_arena_alloc_ != nullptr) {
      p_safe_arena_alloc_->~ObSafeArenaAllocator();
      p_safe_arena_alloc_ = nullptr;
    }
    if (p_arena_alloc_ != nullptr) {
      p_arena_alloc_->~ObArenaAllocator();
      p_arena_alloc_ = nullptr;
    }
    if (parallel_alloc_ != nullptr) {
      parallel_alloc_->~ObParallelAllocator();
      abort_unless(p_alloc_ != nullptr);
      p_alloc_->free(parallel_alloc_);
      parallel_alloc_ = nullptr;
    }
    if (p_alloc_ != nullptr) {
      p_alloc_->~ObAllocator();
      p_alloc_ = nullptr;
    }
    tree_node_.deinit();
  }
  template <typename... Args>
  int create_context(MemoryContext*& context, const DynamicInfo& di, Args&&... args)
  {
    int ret = common::OB_SUCCESS;
    context = nullptr;

    ObMemAttr attr;
    attr.label_ = "CreateContext";
    void* ptr = allocf(sizeof(MemoryContext), attr);
    if (OB_UNLIKELY(nullptr == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      context = new (ptr) MemoryContext(/*need_free*/ true, di, this, args...);
      if (OB_FAIL(context->init())) {
        OB_LOG(WARN, "init failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      if (context != nullptr) {
        context->deinit();
      }
      if (ptr != nullptr) {
        free(ptr);
      }
    }

    return ret;
  }
  template <typename... Args>
  int create_context(MemoryContext& context, const DynamicInfo& di, Args&&... args)
  {
    int ret = common::OB_SUCCESS;

    new (&context) MemoryContext(/*need_free*/ false, di, this, args...);
    if (OB_FAIL(context.init())) {
      OB_LOG(WARN, "init failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      context.deinit();
    }

    return ret;
  }
  static void destory_context(MemoryContext* mem_context)
  {
    if (OB_LIKELY(mem_context != nullptr)) {
      TreeNode* child_node = nullptr;
      while ((child_node = mem_context->tree_node_.child_) != nullptr) {
        MemoryContext* child = node2context(child_node);
        destory_context(child);
      }
      const bool need_free = mem_context->need_free_;
      TreeNode* parent_node = mem_context->tree_node_.parent_;
      abort_unless(parent_node != nullptr);
      mem_context->deinit();
      mem_context->tree_node_.~TreeNode();
      if (need_free) {
        MemoryContext* parent = node2context(parent_node);
        parent->free(mem_context);
      }
    }
  }

public:
  // Assignment is not in the constructor, record who assigns
  const bool need_free_;
  TreeNode tree_node_;
  DynamicInfo di_;
  ContextParam param_;
  int64_t properties_;
  int static_id_;
  common::ObMemAttr attr_;

  // Delayed member
  union {
    common::ObAllocator alloc_;
  };
  union {
    common::ObArenaAllocator arena_alloc_;
  };
  union {
    common::ObSafeArenaAllocator safe_arena_alloc_;
  };
  common::ObAllocator* p_alloc_;
  common::ObArenaAllocator* p_arena_alloc_;
  common::ObSafeArenaAllocator* p_safe_arena_alloc_;

  common::ObParallelAllocator* parallel_alloc_;
  // Allocators that require explicit free, p_alloc_ or parallel_alloc_
  common::ObIAllocator* freeable_alloc_;
  // Allocator returned by get_allocator()
  ObIAllocator* default_allocator_;
};

class Flow final {
public:
  Flow(MemoryContext& ref_context) : ref_context_(ref_context), prev_(nullptr), next_(nullptr), is_inited_(false)
  {}
  MemoryContext& context()
  {
    return ref_context_;
  }
  int init()
  {
    int ret = common::OB_SUCCESS;
    Flow*& cur = g_flow();
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
      Flow*& cur = g_flow();
      abort_unless(cur == this);
      Flow* parent = cur->prev_;
      if (nullptr == parent) {
        cur = nullptr;
      } else {
        parent->next_ = nullptr;
        cur->prev_ = nullptr;
        cur = parent;
      }
    }
  }
  static Flow& current_flow()
  {
    Flow*& cur = g_flow();
    if (OB_UNLIKELY(nullptr == cur)) {
      static CoVar<char[sizeof(Flow)]> buf;
      Flow* flow = new (&buf[0]) Flow(MemoryContext::root());
      abort_unless(flow != nullptr);
      int ret = flow->init();
      abort_unless(common::OB_SUCCESS == ret);
      cur = flow;
    }
    return *cur;
  }
  static MemoryContext& current_ctx()
  {
    return current_flow().ref_context_;
  }
  Flow& parent()
  {
    abort_unless(prev_ != nullptr);
    return *prev_;
  }

private:
  static Flow*& g_flow()
  {
    static CoVar<Flow*> g_flow;
    return g_flow;
  }

private:
  MemoryContext& ref_context_;
  Flow* prev_;
  Flow* next_;
  bool is_inited_;
};

inline void* ctxalf(const int64_t size, const ObMemAttr& attr = default_memattr)
{
  return CURRENT_CONTEXT.allocf(size, attr);
}

inline void* ctxalp(const int64_t size, const ObMemAttr& attr = default_memattr)
{
  return CURRENT_CONTEXT.allocp(size, attr);
}

inline void ctxfree(void* ptr)
{
  CURRENT_CONTEXT.free(ptr);
}

class _SBase {
public:
  int get_ret() const
  {
    return ret_;
  }
  _SBase() : i_(1), ret_(common::OB_SUCCESS)
  {}
  ~_SBase()
  {
    if (OB_UNLIKELY(0 == i_)) {
      OB_LOG(ERROR, "has break statement!!!");
    }
  }
  int i_;
  int ret_;
};

template <ContextSource es>
class _S {};

template <>
class _S<ContextSource::WITH> : public _SBase {
public:
  _S(const bool condition, MemoryContext* context) : _SBase(), flow_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(context)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (condition) {
      Flow* tmp_flow = new (buf_) Flow(*context);
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
  char buf_[sizeof(Flow)] __attribute__((aligned(16)));
  Flow* flow_;
};

template <>
class _S<ContextSource::CREATE> : public _SBase {
public:
  template <typename... Args>
  _S(const bool condition, Args&&... args) : _SBase(), context_(nullptr), flow_(nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(condition)) {
      MemoryContext* tmp_context = reinterpret_cast<MemoryContext*>(buf0_);
      if (OB_FAIL(CURRENT_CONTEXT.create_context(*tmp_context, args...))) {
        OB_LOG(WARN, "create context failed", K(ret));
      } else {
        context_ = tmp_context;
        Flow* tmp_flow = new (buf1_) Flow(*context_);
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
      MemoryContext::destory_context(context_);
    }
  }
  char buf0_[sizeof(MemoryContext)] __attribute__((aligned(16)));
  char buf1_[sizeof(Flow)] __attribute__((aligned(16)));
  MemoryContext* context_;
  Flow* flow_;
};

}  // end of namespace lib
}  // end of namespace oceanbase

#endif  // CONTEXT_H_
