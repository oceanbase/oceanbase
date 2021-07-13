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

#ifndef OCEANBASE_COMMON_PAGE_ARENA_H_
#define OCEANBASE_COMMON_PAGE_ARENA_H_

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_utility.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace common {

inline int64_t sys_page_size()
{
  static int64_t sz = sysconf(_SC_PAGE_SIZE);
  return sz;
}

// convenient function for memory alignment
inline size_t get_align_offset(void* p, const int64_t alignment)
{
  size_t size = 0;
  // quick test for power of 2
  size = alignment - (((uint64_t)p) & (alignment - 1));
  return size;
}

struct DefaultPageAllocator : public ObIAllocator {
  DefaultPageAllocator(const lib::ObLabel& label = ObModIds::OB_PAGE_ARENA, uint64_t tenant_id = OB_SERVER_TENANT_ID)
      : label_(label), tenant_id_(tenant_id), ctx_id_(ObCtxIds::DEFAULT_CTX_ID){};
  virtual ~DefaultPageAllocator(){};
  void* alloc(const int64_t sz)
  {
    ObMemAttr malloc_attr;
    malloc_attr.label_ = label_;
    malloc_attr.tenant_id_ = tenant_id_;
    malloc_attr.ctx_id_ = ctx_id_;
    return alloc(sz, malloc_attr);
  }
  void* alloc(const int64_t size, const ObMemAttr& attr)
  {
    return ob_malloc(size, attr);
  }
  void free(void* p)
  {
    ob_free(p);
  }
  void freed(const int64_t sz)
  {
    UNUSED(sz); /* mostly for effcient bulk stat reporting */
  }
  void set_label(const lib::ObLabel& label)
  {
    label_ = label;
  };
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  };
  void set_ctx_id(int64_t ctx_id)
  {
    ctx_id_ = ctx_id;
  }
  lib::ObLabel get_label() const
  {
    return label_;
  };
  void* mod_alloc(const int64_t sz, const lib::ObLabel& label)
  {
    ObMemAttr malloc_attr;
    malloc_attr.label_ = label;
    malloc_attr.tenant_id_ = tenant_id_;
    return ob_malloc(sz, malloc_attr);
  }

private:
  lib::ObLabel label_;
  uint64_t tenant_id_;
  int64_t ctx_id_;
};

struct ModulePageAllocator : public ObIAllocator {
  ModulePageAllocator(const lib::ObLabel& label = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
      int64_t tenant_id = OB_SERVER_TENANT_ID, int64_t ctx_id = 0)
      : allocator_(NULL), label_(label), tenant_id_(tenant_id), ctx_id_(ctx_id)
  {}
  explicit ModulePageAllocator(ObIAllocator& allocator, const lib::ObLabel& label = lib::ObLabel())
      : allocator_(&allocator), label_(label), tenant_id_(OB_SERVER_TENANT_ID), ctx_id_(0)
  {}
  virtual ~ModulePageAllocator()
  {}
  void set_label(const lib::ObLabel& label)
  {
    label_ = label;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  };
  void set_ctx_id(int64_t ctx_id)
  {
    ctx_id_ = ctx_id;
  }
  lib::ObLabel get_label() const
  {
    return label_;
  }
  void* alloc(const int64_t sz)
  {
    ObMemAttr malloc_attr(tenant_id_, label_, ctx_id_);
    return alloc(sz, malloc_attr);
  }
  void* alloc(const int64_t size, const ObMemAttr& attr)
  {
    return (NULL == allocator_) ? ob_malloc(size, attr) : allocator_->alloc(size, attr);
  }
  void free(void* p)
  {
    (NULL == allocator_) ? ob_free(p) : allocator_->free(p);
    p = NULL;
  }
  void freed(const int64_t sz)
  {
    UNUSED(sz); /* mostly for effcient bulk stat reporting */
  }
  void set_allocator(ObIAllocator* allocator)
  {
    allocator_ = allocator;
  }
  ModulePageAllocator& operator=(const ModulePageAllocator& that)
  {
    if (this != &that) {
      allocator_ = that.allocator_;
      label_ = that.label_;
      tenant_id_ = that.tenant_id_;
      ctx_id_ = that.ctx_id_;
    }
    return *this;
  }

protected:
  ObIAllocator* allocator_;
  lib::ObLabel label_;
  uint64_t tenant_id_;
  uint64_t ctx_id_;
};

/**
 * A simple/fast allocator to avoid individual deletes/frees
 * Good for usage patterns that just:
 * load, use and free the entire container repeatedly.
 */
template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
class PageArena {
private:  // types
  typedef PageArena<CharT, PageAllocatorT> Self;

  struct Page {
    static constexpr uint64_t MAGIC = 0x1234abcddbca4321;
    bool check_magic_code()
    {
      return MAGIC == magic_;
    }
    uint64_t magic_;
    Page* next_page_;
    char* alloc_end_;
    const char* page_end_;
#ifdef MEMORY_DIAGNOSIS
    void* alloc_ptr_;
    void* protect_head_;
    void* protect_tail_;
#endif
    char buf_[0];

#ifdef MEMORY_DIAGNOSIS
    // In Memory diagnosis mode, we add two protected sys pages around the used page.
    Page()
        : magic_(MAGIC),
          next_page_(0),
          alloc_end_(),
          page_end_(),
          alloc_ptr_(this),
          protect_head_(NULL),
          protect_tail_(NULL)
    {}
    explicit Page(const char* end, void* alloc_ptr)
        : magic_(MAGIC),
          next_page_(0),
          alloc_end_(),
          page_end_(end),
          alloc_ptr_(alloc_ptr),
          protect_head_(NULL),
          protect_tail_(NULL)
    {
      alloc_end_ = buf_;
      const int64_t spz = sys_page_size();
      if (0 == mprotect((char*)this - spz, spz, PROT_NONE)) {
        protect_head_ = (char*)this - spz;
      }
      void* tail = (void*)(((uint64_t)page_end_ + spz - 1) & (~(spz - 1)));
      if (0 == mprotect(tail, spz, PROT_NONE)) {
        protect_tail_ = tail;
      }
    }

    ~Page()
    {
      const int64_t spz = sys_page_size();
      if (protect_head_) {
        mprotect(protect_head_, spz, PROT_READ | PROT_WRITE | PROT_EXEC);
        protect_head_ = NULL;
      }
      if (protect_tail_) {
        mprotect(protect_tail_, spz, PROT_READ | PROT_WRITE | PROT_EXEC);
        protect_tail_ = NULL;
      }
    }
#else
    Page() : magic_(MAGIC), next_page_(0), alloc_end_(), page_end_()
    {}
    explicit Page(const char* end) : magic_(MAGIC), next_page_(0), alloc_end_(), page_end_(end)
    {
      alloc_end_ = buf_;
    }
#endif

    inline int64_t remain() const
    {
      return page_end_ - alloc_end_;
    }
    inline int64_t used() const
    {
      return alloc_end_ - buf_;
    }
    inline int64_t raw_size() const
    {
      return page_end_ - buf_ + sizeof(Page);
    }
    inline int64_t reuse_size() const
    {
      return page_end_ - buf_;
    }

    inline CharT* alloc(int64_t sz)
    {
      CharT* ret = NULL;
      if (sz <= 0) {
        // alloc size is invalid
      } else if (sz <= remain()) {
        char* start = alloc_end_;
        alloc_end_ += sz;
        ret = (CharT*)start;
      }
      return ret;
    }

    inline CharT* alloc_down(int64_t sz)
    {
      page_end_ -= sz;
      return (CharT*)page_end_;
    }

    inline void reuse()
    {
      alloc_end_ = buf_;
    }
  };

  struct TracerContext {
    TracerContext() : header_(nullptr), cur_page_(), pages_(), used_(), total_()
    {}
    Page* header_;
    Page cur_page_;
    int64_t pages_;
    int64_t used_;
    int64_t total_;
  };

public:
  static const int64_t DEFAULT_PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE - sizeof(Page);  // default 8KB
  static const int64_t DEFAULT_BIG_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;                // default 2M

private:  // data
  Page* cur_page_;
  Page* header_;
  Page* tailer_;
  int64_t page_limit_;  // capacity in bytes of an empty page
  int64_t page_size_;   // page size in number of bytes
  int64_t pages_;       // number of pages allocated
  int64_t used_;        // total number of bytes allocated by users
  int64_t total_;       // total number of bytes occupied by pages
  PageAllocatorT page_allocator_;
  TracerContext* tc_;

private:  // helpers
  Page* insert_head(Page* page)
  {
    if (OB_ISNULL(page)) {
    } else {
      if (NULL != header_) {
        page->next_page_ = header_;
      }
      header_ = page;
    }
    return page;
  }

  Page* insert_tail(Page* page)
  {
    if (NULL != tailer_) {
      tailer_->next_page_ = page;
    }
    tailer_ = page;

    return page;
  }

#ifdef MEMORY_DIAGNOSIS
  Page* alloc_new_page(const int64_t sz)
  {
    Page* page = NULL;
    const int64_t spz = sys_page_size();
    int64_t new_size = (sz + spz - 1) & (~(spz - 1));
    new_size = 3 * spz + new_size;
    void* ptr = page_allocator_.alloc(new_size);

    if (NULL != ptr) {
      void* new_ptr = (void*)((((uint64_t)ptr + spz - 1) & (~(spz - 1))) + spz);
      page = new (new_ptr) Page((char*)new_ptr + sz, ptr);

      total_ += sz;
      ++pages_;
    } else {
      _OB_LOG(WARN, "cannot allocate memory.sz=%ld, pages_=%ld,total_=%ld,new_size=%ld", sz, pages_, total_, new_size);
    }

    return page;
  }

  void free_page(Page* page)
  {
    void* alloc_ptr = page->alloc_ptr_;
    page->~Page();
    page_allocator_.free(alloc_ptr);
  }
#else
  Page* alloc_new_page(const int64_t sz)
  {
    Page* page = NULL;
    void* ptr = page_allocator_.alloc(sz);

    if (NULL != ptr) {
      page = new (ptr) Page((char*)ptr + sz);

      total_ += sz;
      ++pages_;
    } else {
      _OB_LOG(WARN, "cannot allocate memory.sz=%ld, pages_=%ld,total_=%ld", sz, pages_, total_);
    }

    return page;
  }
  void free_page(Page* page)
  {
    page_allocator_.free(page);
  }
#endif

  Page* extend_page(const int64_t sz)
  {
    Page* page = cur_page_;
    if (NULL != page) {
      page = page->next_page_;
      if (NULL != page) {
        page->reuse();
      } else {
        page = alloc_new_page(sz);
        if (NULL == page) {
          _OB_LOG(WARN, "extend_page sz =%ld cannot alloc new page", sz);
        } else {
          insert_tail(page);
        }
      }
    }
    return page;
  }

  inline bool lookup_next_page(const int64_t sz)
  {
    bool ret = false;
    if (NULL != cur_page_ && NULL != cur_page_->next_page_ && cur_page_->next_page_->reuse_size() >= sz) {
      cur_page_->next_page_->reuse();
      cur_page_ = cur_page_->next_page_;
      ret = true;
    }
    return ret;
  }

  inline bool ensure_cur_page()
  {
    if (NULL == cur_page_) {
      header_ = cur_page_ = tailer_ = alloc_new_page(page_size_);
      if (NULL != cur_page_) {
        page_limit_ = cur_page_->remain();
      }
    }

    return (NULL != cur_page_);
  }

  inline bool is_normal_overflow(const int64_t sz)
  {
    return sz <= page_limit_;
  }

  inline bool is_large_page(Page* page)
  {
    return NULL == page ? false : page->raw_size() > page_size_;
  }

  CharT* alloc_big(const int64_t sz)
  {
    CharT* ptr = NULL;
    // big enough object to have their own page
    Page* p = alloc_new_page(sz + sizeof(Page));
    if (NULL != p) {
      insert_head(p);
      ptr = p->alloc(sz);
    }
    return ptr;
  }

  void free_large_pages()
  {
    Page** current = &header_;
    while (NULL != *current) {
      Page* entry = *current;
      abort_unless(entry->check_magic_code());
      if (is_large_page(entry)) {
        *current = entry->next_page_;
        pages_ -= 1;
        total_ -= entry->raw_size();
        free_page(entry);
        entry = NULL;
      } else {
        tailer_ = *current;
        current = &entry->next_page_;
      }
    }
    if (NULL == header_) {
      tailer_ = NULL;
    }
  }

  Self& assign(Self& rhs)
  {
    if (this != &rhs) {
      free();

      header_ = rhs.header_;
      cur_page_ = rhs.cur_page_;
      tailer_ = rhs.tailer_;

      pages_ = rhs.pages_;
      used_ = rhs.used_;
      total_ = rhs.total_;
      page_size_ = rhs.page_size_;
      page_limit_ = rhs.page_limit_;
      page_allocator_ = rhs.page_allocator_;
    }
    return *this;
  }

public:  // API
  /** constructor */
  PageArena(const int64_t page_size = DEFAULT_PAGE_SIZE, const PageAllocatorT& alloc = PageAllocatorT())
      : cur_page_(NULL),
        header_(NULL),
        tailer_(NULL),
        page_limit_(0),
        page_size_(page_size),
        pages_(0),
        used_(0),
        total_(0),
        page_allocator_(alloc),
        tc_(nullptr)
  {
    if (page_size < (int64_t)sizeof(Page)) {
      _OB_LOG(ERROR, "invalid page size(page_size=%ld, page=%ld)", page_size, (int64_t)sizeof(Page));
    }
  }
  virtual ~PageArena()
  {
    free();
  }

  int init(const int64_t page_size, PageAllocatorT& alloc)
  {
    int ret = OB_SUCCESS;
    if (page_size < (int64_t)sizeof(Page)) {
      _OB_LOG(ERROR, "invalid page size(page_size=%ld, page=%ld)", page_size, (int64_t)sizeof(Page));
    } else {
      page_size_ = page_size;
      page_allocator_ = alloc;
    }
    return ret;
  }

  int mprotect_page_arena(int prot)
  {
    int ret = OB_SUCCESS;
    Page* page = NULL;
    Page* curr = header_;
    while (OB_SUCC(ret) && NULL != curr) {
      abort_unless(curr->check_magic_code());
      page = curr;
      curr = curr->next_page_;
      if (OB_FAIL(mprotect_page(page, page_size_, prot, "page_arena"))) {
        LIB_LOG(WARN, "mprotect page failed", K(ret));
      }
    }
    return ret;
  }

  Self& join(Self& rhs)
  {
    if (this != &rhs && rhs.used_ == 0) {
      if (NULL == header_) {
        assign(rhs);
      } else if (NULL != rhs.header_ && NULL != tailer_) {
        tailer_->next_page_ = rhs.header_;
        tailer_ = rhs.tailer_;

        pages_ += rhs.pages_;
        total_ += rhs.total_;
      }
      rhs.reset();
    }
    return *this;
  }

  int64_t page_size() const
  {
    return page_size_;
  }

  void set_label(const lib::ObLabel& label)
  {
    page_allocator_.set_label(label);
  }
  lib::ObLabel get_label() const
  {
    return page_allocator_.get_label();
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    page_allocator_.set_tenant_id(tenant_id);
  }
  void set_ctx_id(int64_t ctx_id)
  {
    page_allocator_.set_ctx_id(ctx_id);
  }
  /** allocate sz bytes */
  CharT* alloc(const int64_t sz)
  {
    ensure_cur_page();

    // common case
    CharT* ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc(sz);
      } else if (is_normal_overflow(sz)) {
        Page* new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc(sz);
      } else {
        ret = alloc_big(sz);
      }

      if (NULL != ret) {
        used_ += sz;
      }
    }
    return ret;
  }
  CharT* alloc(const int64_t sz, const lib::ObMemAttr& attr)
  {
    UNUSED(attr);
    return alloc(sz);
  }

  template <class T>
  T* new_object()
  {
    T* ret = NULL;
    void* tmp = (void*)alloc_aligned(sizeof(T));
    if (NULL == tmp) {
      _OB_LOG(WARN, "fail to alloc mem for T");
    } else {
      ret = new (tmp) T();
    }
    return ret;
  }

  /** allocate sz bytes */
  CharT* alloc_aligned(const int64_t sz, const int64_t alignment = 16)
  {
    ensure_cur_page();

    // common case
    CharT* ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      int64_t align_offset = get_align_offset(cur_page_->alloc_end_, alignment);
      int64_t adjusted_sz = sz + align_offset;

      if (adjusted_sz <= cur_page_->remain()) {
        ret = cur_page_->alloc(adjusted_sz) + align_offset;
        if (NULL != ret) {
          used_ += align_offset;
        }
      } else if (is_normal_overflow(sz)) {
        Page* new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else {
        ret = alloc_big(sz);
      }

      if (NULL != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  /**
   * allocate from the end of the page.
   * - allow better packing/space saving for certain scenarios
   */
  CharT* alloc_down(const int64_t sz)
  {
    ensure_cur_page();

    // common case
    CharT* ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc_down(sz);
      } else if (is_normal_overflow(sz)) {
        Page* new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc_down(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc_down(sz);
      } else {
        ret = alloc_big(sz);
      }

      if (NULL != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  /** realloc for newsz bytes */
  CharT* realloc(CharT* p, const int64_t oldsz, const int64_t newsz)
  {
    CharT* ret = NULL;
    if (OB_ISNULL(cur_page_)) {
    } else {
      ret = p;
      // if we're the last one on the current page with enough space
      if (p + oldsz == cur_page_->alloc_end_ && p + newsz < cur_page_->page_end_) {
        cur_page_->alloc_end_ = (char*)p + newsz;
        ret = p;
      } else {
        ret = alloc(newsz);
        if (NULL != ret) {
          MEMCPY(ret, p, newsz > oldsz ? oldsz : newsz);
        }
      }
    }
    return ret;
  }

  /** duplicate a null terminated string s */
  CharT* dup(const char* s)
  {
    if (NULL == s) {
      return NULL;
    }

    int64_t len = strlen(s) + 1;
    CharT* copy = alloc(len);
    if (NULL != copy) {
      MEMCPY(copy, s, len);
    }
    return copy;
  }

  /** duplicate a buffer of size len */
  CharT* dup(const void* s, const int64_t len)
  {
    CharT* copy = NULL;
    if (NULL != s && len > 0) {
      copy = alloc(len);
      if (NULL != copy) {
        MEMCPY(copy, s, len);
      }
    }

    return copy;
  }

  /** free the whole arena */
  void free()
  {
    Page* page = NULL;

    while (NULL != header_) {
      abort_unless(header_->check_magic_code());
      page = header_;
      header_ = header_->next_page_;
      free_page(page);
      page = NULL;
    }
    page_allocator_.freed(total_);

    cur_page_ = NULL;
    tailer_ = NULL;
    used_ = 0;
    pages_ = 0;
    total_ = 0;
    tc_ = nullptr;
  }

  /** free the arena and remain one normal page */
  void free_remain_one_page()
  {
    Page* page = NULL;
    Page* remain_page = NULL;
    while (NULL != header_) {
      abort_unless(header_->check_magic_code());
      page = header_;
      if (NULL == remain_page && !is_large_page(page)) {
        remain_page = page;
        header_ = header_->next_page_;
      } else {
        header_ = header_->next_page_;
        free_page(page);
      }
      page = NULL;
    }
    header_ = cur_page_ = remain_page;
    if (NULL == cur_page_) {
      page_allocator_.freed(total_);
      total_ = 0;
      pages_ = 0;
    } else {
      cur_page_->next_page_ = NULL;
      page_allocator_.freed(total_ - cur_page_->raw_size());
      cur_page_->reuse();
      total_ = cur_page_->raw_size();
      pages_ = 1;
    }
    tailer_ = cur_page_;
    used_ = 0;
    tc_ = nullptr;
  }
  /**
   * free some of pages. remain memory can be reuse.
   *
   * @param sleep_pages force sleep when pages are freed every time.
   * @param sleep_interval_us sleep interval in microseconds.
   * @param remain_size keep size of memory pages less than %remain_size
   *
   */
  void partial_slow_free(const int64_t sleep_pages, const int64_t sleep_interval_us, const int64_t remain_size = 0)
  {
    Page* page = NULL;

    int64_t current_sleep_pages = 0;

    while (NULL != header_ && (remain_size == 0 || total_ > remain_size)) {
      abort_unless(header_->check_magic_code());
      page = header_;
      header_ = header_->next_page_;

      total_ -= page->raw_size();

      free_page(page);

      ++current_sleep_pages;
      --pages_;

      if (sleep_pages > 0 && current_sleep_pages >= sleep_pages) {
        ::usleep(static_cast<useconds_t>(sleep_interval_us));
        current_sleep_pages = 0;
      }
    }

    // reset allocate start point, important.
    // once slow_free called, all memory allocated before
    // CANNOT use anymore.
    cur_page_ = header_;
    if (NULL == header_) {
      tailer_ = NULL;
    }
    used_ = 0;
  }

  void free(CharT* ptr)
  {
    UNUSED(ptr);
  }

  // Tracer is used to free memory in a arena allocator.  When call
  // set_tracer function, arena would record a snapshot which is used
  // to recover. As soon as responding revert_tracer function is
  // called, any follow up allocates would been freed and arena states
  // would rollback to the states of that snapshot. It's useful when
  // repeat doing something where use arena to allocate memory. Set a
  // tracer by using set_tracer function before each round of repeat and
  // invoke revert_tracer routine to free all allocates within current
  // round.
  bool set_tracer()
  {
    bool bret = true;
    if (header_ != nullptr) {
      if (OB_UNLIKELY(tc_ == nullptr)) {
        auto* pool = this;
        tc_ = OB_NEWx(TracerContext, pool);
        if (tc_ == nullptr) {
          bret = false;
        }
      }
      if (bret) {
        tc_->header_ = header_;
        tc_->cur_page_ = *cur_page_;
        tc_->cur_page_.next_page_ = cur_page_;
        tc_->pages_ = pages_;
        tc_->total_ = total_;
        tc_->used_ = used_;
      }
    }
    return bret;
  }

  bool revert_tracer()
  {
    bool bret = true;
    if (nullptr != tc_) {
      // Free large pages from current header to header of trace pointer.
      // Free normal pages from trace pointer page to current page.
      // Restore current page and statistics information.

      // 1. free large pages
      Page*& header = header_;
      while (header != tc_->header_ && header != nullptr) {
        abort_unless(header_->check_magic_code());
        Page* next_header = header->next_page_;
        free_page(header);
        header = next_header;
      }

      // 2. free normal pages
      abort_unless(tc_->cur_page_.check_magic_code());
      tailer_ = cur_page_ = tc_->cur_page_.next_page_;
      Page*& page = cur_page_->next_page_;
      while (page != nullptr) {
        abort_unless(page->check_magic_code());
        Page* next_page = page->next_page_;
        free_page(page);
        page = next_page;
      }

      // 3. restore statistics
      pages_ = tc_->pages_;
      total_ = tc_->total_;
      used_ = tc_->used_;
    } else {
      // There are two cases for tc_ == nullptr
      // 1. Set_tracer has not been adjusted;
      // 2. I adjusted set_tracer but did not apply for any page at that time
      // The premise of the revert function call is that the user has adjusted set_tracer, so all pages need to be
      // released here
      free();
    }
    return bret;
  }

  void fast_reuse()
  {
    used_ = 0;
    cur_page_ = header_;
    if (NULL != cur_page_) {
      cur_page_->reuse();
    }
    tc_ = nullptr;
  }

  void reuse()
  {
    free_large_pages();
    fast_reuse();
  }

  void dump() const
  {
    Page* page = header_;
    int64_t count = 0;
    while (NULL != page) {
      abort_unless(page->check_magic_code());
      _OB_LOG(INFO,
          "DUMP PAGEARENA page[%ld]:rawsize[%ld],used[%ld],remain[%ld]",
          count++,
          page->raw_size(),
          page->used(),
          page->remain());
      page = page->next_page_;
    }
  }

  /** stats accessors */
  int64_t pages() const
  {
    return pages_;
  }
  int64_t used() const
  {
    return used_;
  }
  int64_t total() const
  {
    return total_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(PageArena);
};

typedef PageArena<> CharArena;
typedef PageArena<unsigned char> ByteArena;
typedef PageArena<char, ModulePageAllocator> ModuleArena;

class ObArenaAllocator final : public ObIAllocator {
public:
  ObArenaAllocator(const lib::ObLabel& label = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
      const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE, int64_t tenant_id = OB_SERVER_TENANT_ID,
      int64_t ctx_id = 0)
      : arena_(page_size, ModulePageAllocator(label, tenant_id, ctx_id)){};
  ObArenaAllocator(ObIAllocator& allocator, const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE)
      : arena_(page_size, ModulePageAllocator(allocator)){};
  virtual ~ObArenaAllocator(){};

public:
  virtual void* alloc(const int64_t sz) override
  {
    return arena_.alloc_aligned(sz);
  }
  void* alloc(const int64_t size, const ObMemAttr& attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  virtual void* alloc_aligned(const int64_t sz, const int64_t align)
  {
    return arena_.alloc_aligned(sz, align);
  }
  virtual void* realloc(void* ptr, const int64_t oldsz, const int64_t newsz) override
  {
    return arena_.realloc(reinterpret_cast<char*>(ptr), oldsz, newsz);
  }
  virtual void free(void* ptr) override
  {
    arena_.free(reinterpret_cast<char*>(ptr));
    ptr = NULL;
  }
  virtual void clear()
  {
    arena_.free();
  }
  int64_t used() const override
  {
    return arena_.used();
  }
  int64_t total() const override
  {
    return arena_.total();
  }
  void reset() override
  {
    arena_.free();
  }
  void reset_remain_one_page()
  {
    arena_.free_remain_one_page();
  }
  void reuse() override
  {
    arena_.reuse();
  }
  virtual void set_label(const lib::ObLabel& label)
  {
    arena_.set_label(label);
  }
  virtual lib::ObLabel get_label() const
  {
    return arena_.get_label();
  }
  virtual void set_tenant_id(uint64_t tenant_id)
  {
    arena_.set_tenant_id(tenant_id);
  }
  bool set_tracer()
  {
    return arena_.set_tracer();
  }
  bool revert_tracer()
  {
    return arena_.revert_tracer();
  }
  void set_ctx_id(int64_t ctx_id)
  {
    arena_.set_ctx_id(ctx_id);
  }
  void set_attr(const ObMemAttr& attr) override
  {
    arena_.set_tenant_id(attr.tenant_id_);
    arena_.set_ctx_id(attr.ctx_id_);
    arena_.set_label(attr.label_);
  }
  ModuleArena& get_arena()
  {
    return arena_;
  }
  int64_t to_string(char* buf, int64_t len) const
  {
    int64_t printed =
        snprintf(buf, len, "pages=%ld, used=%ld, total=%ld", arena_.pages(), arena_.used(), arena_.total());
    return printed < len ? printed : len;
  }
  int mprotect_arena_allocator(int prot)
  {
    return arena_.mprotect_page_arena(prot);
  }

private:
  ModuleArena arena_;
};

class ObSafeArenaAllocator : public ObIAllocator {
public:
  ObSafeArenaAllocator(ObArenaAllocator& arena) : arena_(arena), lock_()
  {}
  virtual ~ObSafeArenaAllocator()
  {}

public:
  void* alloc(const int64_t sz) override
  {
    return alloc(sz, default_memattr);
  }
  void* alloc(const int64_t sz, const ObMemAttr& attr) override
  {
    ObSpinLockGuard guard(lock_);
    return arena_.alloc(sz, attr);
  }
  void free(void* ptr) override
  {
    ObSpinLockGuard guard(lock_);
    arena_.free(ptr);
  }
  void clear()
  {
    ObSpinLockGuard guard(lock_);
    arena_.clear();
  }
  void reuse() override
  {
    ObSpinLockGuard guard(lock_);
    arena_.reuse();
  }
  int64_t total() const override
  {
    return arena_.total();
  }
  int64_t used() const override
  {
    return arena_.used();
  }

private:
  ObArenaAllocator& arena_;
  ObSpinLock lock_;
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // end if OCEANBASE_COMMON_PAGE_ARENA_H_
