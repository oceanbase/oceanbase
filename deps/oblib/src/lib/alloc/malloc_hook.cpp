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
#include "malloc_hook.h"
#include "deps/oblib/src/lib/hash/ob_hashmap.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include <dlfcn.h>

#define OBMALLOC_ATTR(s) __attribute__((s))
#define OBMALLOC_EXPORT __attribute__((visibility("default")))
#define OBMALLOC_ALLOC_SIZE(s) __attribute__((alloc_size(s)))
#define OBMALLOC_NOTHROW __attribute__((nothrow))
#define LIBC_ALIAS(fn)	__attribute__((alias (#fn), used))

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
static bool g_malloc_hook_inited = false;
typedef void* (*MemsetPtr)(void*, int, size_t);
MemsetPtr memset_ptr = nullptr;
void __attribute__((constructor(0))) init_malloc_hook()
{
  // The aim of calling memset is to initialize certain states in memset,
  // and to avoid nested deadlock of memset after malloc_hook inited.
  static void *(*real_func)(void *, int, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "memset");
  real_func(&memset_ptr, 0, sizeof(memset_ptr));
  memset_ptr = real_func;
  g_malloc_hook_inited = true;
}
uint64_t up_align(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

struct Header
{
  static const uint32_t MAGIC_CODE = 0XA1B2C3D1;
  static const uint32_t SIZE;
  Header(uint32_t size, uint32_t alloc_from)
    : magic_code_(MAGIC_CODE),
      data_size_(size),
      offset_(0),
      alloc_from_(alloc_from)
  {}
  bool check_magic_code() const { return MAGIC_CODE ==  magic_code_; }
  void mark_unused() { magic_code_ &= ~0x1; }
  static Header *ptr2header(void *ptr) { return reinterpret_cast<Header*>((char*)ptr - SIZE); }
  uint32_t magic_code_;
  uint32_t data_size_;
  uint32_t offset_;
  uint32_t alloc_from_;
  char data_[0];
} __attribute__((aligned (16)));

const uint32_t Header::SIZE = offsetof(Header, data_);

static inline void *ob_mmap(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  void *ptr = (void*)syscall(SYS_mmap, addr, length, prot, flags, fd, offset);
  if (OB_UNLIKELY(!UNMAMAGED_MEMORY_STAT.is_disabled()) && OB_LIKELY(MAP_FAILED != ptr)) {
    const int64_t page_size = get_page_size();
    UNMAMAGED_MEMORY_STAT.inc(upper_align(length, page_size));
  }
  return ptr;
}

static inline int ob_munmap(void *addr, size_t length)
{
  if (OB_UNLIKELY(!ObUnmanagedMemoryStat::is_disabled())) {
    const int64_t page_size = get_page_size();
    UNMAMAGED_MEMORY_STAT.dec(upper_align(length, page_size));
  }
  return syscall(SYS_munmap, addr, length);
}

enum AllocMode {
  MMAP,
  OB_MALLOC,
  MALLOC_V3,
};

struct SizeClass
{
  static const int64_t SC_MAGIC_CODE = 0x3A3A2B2BB2B2A3A3;
  SizeClass()
    : MAGIC_CODE_(SC_MAGIC_CODE), alloc_(NULL), partitial_list_(NULL)
  {}
  //尾插头取，保证block积累到更多的local free
  void push_partitial(ABlock *block)
  {
    if (NULL == partitial_list_) {
      block->next_ = block;
      block->prev_ = block;
      partitial_list_ = block;
    } else {
      block->next_ = partitial_list_;
      block->prev_ = partitial_list_->prev_;
      partitial_list_->prev_->next_ = block;
      partitial_list_->prev_ = block;
    }
  }

  void pop_partitial(ABlock *block)
  {
    if (block == block->next_) {
      partitial_list_ = NULL;
    } else {
      block->prev_->next_ = block->next_;
      block->next_->prev_ = block->prev_;
      if (block == partitial_list_) {
        partitial_list_ = block->next_;
      }
    }
  }

  ABlock *pop_partitial()
  {
    if (NULL == partitial_list_) return NULL;
    ABlock *block = partitial_list_;
    pop_partitial(block);
    return block;
  }
  int64_t MAGIC_CODE_;
  AObject *alloc_;
  ABlock *partitial_list_; // use ABlock::next_
  //ABlock *deferred_list_; // use ABlock::next2_
};


template<int _size>
class FixedAllocer
{
public:
  FixedAllocer() : lock_(), head_(NULL), using_cnt_(0), free_cnt_(0) {}
  int64_t using_cnt() { return using_cnt_; }
  int64_t free_cnt() { return free_cnt_; }
  void *alloc()
  {
    void *ret = NULL;
    lock_.lock();
    if (!head_) {
      int64_t alloc_size = max(get_page_size(), _size);
      void *ptr = NULL;
      if (MAP_FAILED == (ptr = mmap(NULL, alloc_size,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      } else {
        int64_t left = alloc_size;
        while (left >= _size) {
          void *obj = (void*)((char*)ptr + left - _size);
          *(void**)obj = head_;
          head_ = obj;
          free_cnt_++;
          left -= _size;
        }
      }
    }
    if (head_) {
      void *next = *(void**)head_;
      ret = head_;
      head_ = next;
      using_cnt_++;
      free_cnt_--;
    }
    lock_.unlock();
    return ret;
  }
  void free(void *ptr)
  {
    lock_.lock();
    using_cnt_--;
    free_cnt_++;
    if (!head_) {
      *(void**)ptr = NULL;
      head_ = ptr;
    } else {
      *(void**)ptr = head_;
      head_ = ptr;
    }
    lock_.unlock();
  }
private:
  ObSimpleLock lock_;
  void *head_;
  int64_t using_cnt_;
  int64_t free_cnt_;
};

class ObjectSetV3 : public ObDLinkBase<ObjectSetV3>
{
public:
  ObjectSetV3()
    : ObDLinkBase<ObjectSetV3>(),
      ta_(NULL), mgr_(NULL), deferred_list_(NULL)
  {}
  static int calc_sc_idx(const int64_t x)
  {
    if (OB_LIKELY(x <= 1024)) {
      return (x-1)>>6;
    } else if (x <= ABLOCK_SIZE) {
      return 8 * sizeof(int64_t) - __builtin_clzll(x - 1) - 11 + 16;
    } else {
      return ObjectSetV2::LARGE_SC_IDX;
    }
  }

  ABlock* new_block(int sc_idx)
  {
    ABlock *block = NULL;
    const int bin_size = ObjectSetV2::BIN_SIZE_MAP(sc_idx);
    const int64_t ablock_size = ObjectSetV2::BLOCK_SIZE_MAP(sc_idx);
    if (OB_NOT_NULL(block = alloc_block(ablock_size))) {
      block->sc_idx_ = sc_idx;
      block->max_cnt_ = ablock_size/bin_size;
      char *data = block->data();
      int32_t cls = bin_size/AOBJECT_CELL_BYTES;
      AObject *freelist = NULL;
      AObject *cur = NULL;
      for (int i = 0; i < block->max_cnt_; i++) {
        cur = new (data + bin_size * i) AObject();
        MEMCPY(cur->label_, "glibc_malloc_v3", AOBJECT_LABEL_SIZE + 1);
        cur->block_ = block;
        cur->nobjs_ = cls;
        cur->obj_offset_ = cls * i;
        cur->next_ = freelist;
        cur->ignore_version_ = true;
        freelist = cur;
      }
      cur->nobjs_ = ablock_size/AOBJECT_CELL_BYTES - cur->obj_offset_;
      block->local_free_.reset(freelist, block->max_cnt_);
      block->remote_free_.reset();
    }
    return block;
  }

  void *slow_alloc(const uint64_t size, uint32_t &alloc_from)
  {
    void *ptr = NULL;
    bool in_hook_bak = in_hook();
    in_hook() = true;
    DEFER(in_hook() = in_hook_bak);
    if (OB_UNLIKELY(!g_malloc_hook_inited || in_hook_bak)) {
      if (MAP_FAILED == (ptr = ob_mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
        ptr = nullptr;
      } else {
        alloc_from = AllocMode::MMAP;
      }
    } else if (OB_UNLIKELY(is_malloc_v2_enabled() ||
        !ObMallocHookAttrGuard::get_tl_use_500())) {
      ObMemAttr attr = ObMallocHookAttrGuard::get_tl_mem_attr();
      if (OB_NOT_NULL(ptr = ob_malloc(size, attr))) {
        alloc_from = AllocMode::OB_MALLOC;
      }
    } else {
      reclaim_deferred_to_local();
      uint64_t all_size = size + AOBJECT_META_SIZE;
      static thread_local ObMallocSampleLimiter sample_limiter;
      const bool sample_allowed = sample_limiter.try_acquire(size);
      if (OB_UNLIKELY(sample_allowed)) {
        all_size += AOBJECT_EXTRA_INFO_SIZE;
      }
      const int sc_idx = calc_sc_idx(all_size);
      // ** 处理非本线程释放的内存，将deferred_list_转移到partitial_list_
      SizeClass &sc = scs_[sc_idx];
      AObject *obj = NULL;
      if (OB_UNLIKELY(sc_idx == ObjectSetV2::LARGE_SC_IDX)) {
        ABlock *block = alloc_block(all_size);
        if (OB_LIKELY(block != NULL)) {
          obj = new (block->data()) AObject();
          obj->block_ = block;
          obj->is_large_ = true;
          obj->ignore_version_ = true;
          MEMCPY(obj->label_, "glibc_malloc_v3", AOBJECT_LABEL_SIZE + 1);
        }
      } else if (sc.alloc_) {
        abort_unless(sample_allowed);
        obj = sc.alloc_;
        sc.alloc_ = sc.alloc_->next_;
      } else {
        ABlock *block = NULL;
        if (OB_NOT_NULL(block = sc.pop_partitial()) ||
            OB_NOT_NULL(block = new_block(sc_idx))) {
          obj = block->local_free_.popall();
          sc.alloc_ = obj->next_;
        }
      }
      if (OB_LIKELY(obj)) {
        obj->in_use_ = true;
        obj->alloc_bytes_ = size;
        //reinterpret_cast<uint64_t&>(obj->data_[size]) = AOBJECT_TAIL_MAGIC_CODE;
        if (OB_UNLIKELY(sample_allowed)) {
          void *addrs[100] = {nullptr};
          backtrace(addrs, ARRAYSIZEOF(addrs));
          MEMCPY(obj->bt(), (char*)addrs, AOBJECT_BACKTRACE_SIZE);
          obj->on_malloc_sample_ = true;
        }
        alloc_from = AllocMode::MALLOC_V3;
        ptr = obj->data_;
      }
    }
    return ptr;
  }

  void *alloc(const uint64_t size, uint32_t &alloc_from)
  {
    SANITY_DISABLE_CHECK_RANGE();
    void *ptr = NULL;
    const int sc_idx = calc_sc_idx(size + AOBJECT_META_SIZE);
    SizeClass &sc = scs_[sc_idx];
    AObject *&alloc = sc.alloc_;
    if (OB_LIKELY(alloc)) {
      //优先从本地的alloc_中获取内存块
      AObject *obj = alloc;
      alloc = obj->next_;
      obj->in_use_ = true;
      obj->alloc_bytes_ = size;
      //reinterpret_cast<uint64_t&>(obj->data_[size]) = AOBJECT_TAIL_MAGIC_CODE;
      alloc_from = MALLOC_V3;
      ptr = obj->data_;
    } else {
      // * 慢路径
      do {
        ptr = slow_alloc(size, alloc_from);
        if (OB_ISNULL(ptr)) {
          ::usleep(10000);  // 10ms
        }
      } while (OB_ISNULL(ptr) && 0 != size);
    }
    SANITY_UNPOISON(ptr, size);
    return ptr;
  }

  void free(void *ptr)
  {
    SANITY_DISABLE_CHECK_RANGE();
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    ABlock *block = obj->block_;
    ObjectSetV3 *os = block->obj_set_v3_;
    obj->in_use_ = false;
    SANITY_POISON(obj->data_, obj->alloc_bytes_);
    // TODO: check AOBJECT_TAIL_MAGIC_CODE
    if (OB_UNLIKELY(obj->is_large_)) {
      os->free_block(block);
    } else if (OB_LIKELY(os == this)) {
      os->local_free(obj, block);
    } else {
      os->remote_free(obj, block);
    }
  }

  void local_free(AObject *obj, ABlock *block)
  {
    const int64_t cnt = block->local_free_.push(obj);
    if (OB_LIKELY(1 != cnt && block->max_cnt_ != cnt)) {
      return;
    }
    SizeClass &sc = scs_[block->sc_idx_];
    if (OB_UNLIKELY(1 == cnt)) {
      sc.push_partitial(block);
    } else if (OB_UNLIKELY(block->max_cnt_ == cnt)) {
      sc.pop_partitial(block);
      free_block(block);
    }
  }

  void remote_free(AObject *obj, ABlock *block)
  {
    if (1 == block->remote_free_.push(obj)) {
      ABlock *old = ATOMIC_LOAD(&deferred_list_);
      do {
        block->next2_ = old;
      } while (!ATOMIC_CMP_AND_EXCHANGE(&deferred_list_, &old, block));
    }
  }

  void reclaim_deferred_to_local()
  {
    ABlock *next = ATOMIC_TAS(&deferred_list_, NULL);
    while (next) {
      ABlock *block = next;
      next = next->next2_;
      if (OB_UNLIKELY(block->local_free_.is_empty())) {
        scs_[block->sc_idx_].push_partitial(block);
      }
      AObject *head = NULL;
      AObject *tail = NULL;
      const int64_t cnt = block->remote_free_.popall(head, tail);
      const int64_t local_free_cnt = block->local_free_.push(head, tail, cnt);
      if (OB_UNLIKELY(block->max_cnt_ == local_free_cnt)) {
        scs_[block->sc_idx_].pop_partitial(block);
        free_block(block);
      }
    }
  }

  ABlock *alloc_block(const uint64_t size)
  {
    const ObMemAttr attr(OB_SERVER_TENANT_ID, "glibc_malloc_v3", ObCtxIds::GLIBC);
    if (OB_UNLIKELY(NULL == mgr_)) {
      ta_ = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ObCtxIds::GLIBC);
      mgr_ = &ta_->get_block_mgr();
    }
    ABlock *block = mgr_->alloc_block(size, attr);
    if (OB_UNLIKELY(NULL == block)) {
      mgr_->sync_wash(INT64_MAX);
      block = mgr_->alloc_block(size, attr);
    }
    if (OB_LIKELY(NULL != block)) {
      block->obj_set_v3_ = this;
      block->ablock_size_ = size;
      SANITY_POISON(block->data(), size);
    }
    return block;
  }

  void free_block(ABlock *block)
  {
    mgr_->free_block(block);
  }
public:
  ObTenantCtxAllocatorGuard ta_;
  IBlockMgr *mgr_;
  ABlock *deferred_list_; // use ABlock::next2_
  SizeClass scs_[ObjectSetV2::NORMAL_SC_CNT + 1];
} CACHE_ALIGNED;


struct ObjectSetList
{
  ObjectSetList()
    : lock_(), list_()
  {}
  void push(ObjectSetV3 *os)
  {
    lock_.lock();
    list_.add_last(os);
    lock_.unlock();
  }
  ObjectSetV3 *pop()
  {
    ObjectSetV3 *ret = NULL;
    lock_.lock();
    ret = list_.remove_first();
    lock_.unlock();
    return ret;
  }
  ObSimpleLock lock_;
  ObDList<ObjectSetV3> list_;
};

class GlibcMalloc
{
public:
  static void destructor(void* ptr)
  {
    if (!ptr) return;
    ObjectSetV3* os = static_cast<ObjectSetV3*>(ptr);
    os->reclaim_deferred_to_local();
    global_os_list.push(os);
  }

  GlibcMalloc()
  {
    static struct KeyInit {
      KeyInit()
      {
          pthread_key_create(&GlibcMalloc::tls_key, GlibcMalloc::destructor);
      }
    } key_init;
    if (tl_os) return;
    const int MAX_RECLAIM_CNT = 4;
    for (int cnt = 0; cnt < MAX_RECLAIM_CNT; ++cnt) {
      ObjectSetV3 *os = NULL;
      os = global_os_list.pop();
      if (os) {
        os->reclaim_deferred_to_local();
        global_os_list.push(os);
      }
    }
    if (NULL == (tl_os = global_os_list.pop())) {
      void *ptr = NULL;
      static FixedAllocer<sizeof(ObjectSetV3)> bin_allocer_;
      do {
        ptr = bin_allocer_.alloc();
        if (NULL == ptr) {
          ::usleep(10000);  // 10ms
        }
      } while (NULL == ptr);
      tl_os = new (ptr) ObjectSetV3();
    }
    pthread_setspecific(tls_key, tl_os);
  }
public:
  static __thread ObjectSetV3 *tl_os;
private:
  static pthread_key_t tls_key;
  static ObjectSetList global_os_list;

};
pthread_key_t GlibcMalloc::tls_key;
ObjectSetList GlibcMalloc::global_os_list;
__thread ObjectSetV3 *GlibcMalloc::tl_os = NULL;

EXTERN_C_BEGIN

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ATTR(malloc) OBMALLOC_ALLOC_SIZE(1)
malloc(size_t size)
{
  static thread_local GlibcMalloc tl_glibc_malloc;
  void *ptr = NULL;
  uint32_t alloc_from = 0;
  void *tmp_ptr = GlibcMalloc::tl_os->alloc(size + Header::SIZE, alloc_from);
  Header *header = new (tmp_ptr) Header ((uint32_t)size, alloc_from);
  return header->data_;
}

OBMALLOC_EXPORT void OBMALLOC_NOTHROW
free(void *ptr)
{
  if (OB_LIKELY(ptr != nullptr)) {
    Header *header = Header::ptr2header(ptr);
    const uint32_t alloc_from = header->alloc_from_;
    void *orig_ptr = (char*)header - header->offset_;
    abort_unless(header->check_magic_code());
    header->mark_unused();
    if (OB_LIKELY(AllocMode::MALLOC_V3 == alloc_from)) {
      GlibcMalloc::tl_os->free(orig_ptr);
    } else if (AllocMode::OB_MALLOC == alloc_from) {
      ob_free(orig_ptr);
    } else if (AllocMode::MMAP == alloc_from) {
      ob_munmap(orig_ptr, header->data_size_ + Header::SIZE + header->offset_);
    }
  }
}

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ALLOC_SIZE(2)
realloc(void *ptr, size_t size)
{
  void *nptr = malloc(size);
  if (nullptr == ptr) return nptr;
  if (nullptr == nptr) return ptr;
  Header *old_header = Header::ptr2header(ptr);
  abort_unless(old_header->check_magic_code());
  memmove(nptr, ptr, MIN(old_header->data_size_, size));
  free(ptr);
  return nptr;
}

OBMALLOC_EXPORT
void OBMALLOC_NOTHROW *
OBMALLOC_ATTR(malloc)
memalign(size_t alignment, size_t size)
{
  static thread_local GlibcMalloc tl_glibc_malloc;
  void *ptr = nullptr;
  // avoid alignment overflow
  // Make sure alignment is power of 2
  {
    size_t a = 8;
    while (a < alignment)
      a <<= 1;
    alignment = a;
  }
  uint32_t alloc_from = 0;
  void *tmp_ptr = GlibcMalloc::tl_os->alloc(alignment + size + Header::SIZE, alloc_from);
  char *start = (char *)tmp_ptr + Header::SIZE;
  char *align_ptr = (char *)up_align(reinterpret_cast<int64_t>(start), alignment);
  char *pheader = align_ptr - Header::SIZE;
  size_t offset = pheader - (char*)tmp_ptr;
  Header *header = new (pheader) Header((uint32_t)size, alloc_from);
  header->offset_ = (uint32_t)offset;
  ptr = header->data_;
  return ptr;
}

void *ob_mmap_hook(void *addr, size_t length, int prot, int flags, int fd, loff_t offset)
{
  return ob_mmap(addr, length, prot, flags, fd, offset);
}

int ob_munmap_hook(void *addr, size_t length)
{
  return ob_munmap(addr, length);
}

__attribute__((visibility("default"))) void *mmap(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) void *mmap64(void *addr, size_t, int, int, int, loff_t) __attribute__((weak,alias("ob_mmap_hook")));
__attribute__((visibility("default"))) int munmap(void *addr, size_t length) __attribute__((weak,alias("ob_munmap_hook")));

OBMALLOC_EXPORT size_t OBMALLOC_NOTHROW
malloc_usable_size(void *ptr)
{
  size_t ret = 0;
  if (OB_LIKELY(nullptr != ptr)) {
    Header *header = Header::ptr2header(ptr);
    abort_unless(header->check_magic_code());
    ret = header->data_size_;
  }
  return ret;
}

void *__libc_malloc(size_t size) LIBC_ALIAS(malloc);
void *__libc_realloc(void* ptr, size_t size) LIBC_ALIAS(realloc);
void __libc_free(void* ptr) LIBC_ALIAS(free);
void *__libc_memalign(size_t align, size_t s) LIBC_ALIAS(memalign);

int pthread_getattr_np(pthread_t thread, pthread_attr_t *attr)
{
  // pthread_getattr_np has lock and will allocate memory
  // add in_hook to avoid deadlock with backtrace get_stackattr
  bool in_hook_bak = in_hook();
  in_hook() = true;
  DEFER(in_hook() = in_hook_bak);
  static int (*real_pthread_getattr_np)(pthread_t thread, pthread_attr_t *attr) =
      (typeof(real_pthread_getattr_np))dlsym(RTLD_NEXT, "pthread_getattr_np");
  return real_pthread_getattr_np(thread, attr);
}

EXTERN_C_END
