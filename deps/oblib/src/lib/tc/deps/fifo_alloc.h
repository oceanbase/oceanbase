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
#include <new>
#define MAGIC 0xdeadbeafdeadbeafULL
class FifoAlloc
{
private:
  enum { PAGE_SIZE = 1<<16 };
  struct Page {
    Page(int sz): MAGIC_(MAGIC), limit_(sz - sizeof(Page)), pos_(0), alloc_cnt_(0), ref_(0) {}
    int ref(int x) {
      int ret = ATOMIC_AAF(&ref_, x);
      if (0 == ret) {
        MAGIC_ = MAGIC - 1;
      }
      return ret;
    }
    void* alloc(int sz) {
      int64_t alloc_sz = sz + sizeof(Page*);
      void* ret = NULL;
      if (pos_ + alloc_sz <= limit_) {
        *(Page**)(base_ + pos_) = this;
        ret = base_ + pos_ + sizeof(Page*);
        pos_ += alloc_sz;
        alloc_cnt_++;
      }
      return ret;
    }
    int retire() { return ref(alloc_cnt_); }
    uint64_t MAGIC_;
    int limit_;
    int pos_;
    int alloc_cnt_;
    int ref_ CACHE_ALIGNED;
    char base_[0] CACHE_ALIGNED;
  };
  struct CacheRef
  {
    CacheRef(): page_(NULL), ref_(0) {}
    Page* page_;
    int ref_;
    Page* free(Page* p) {
      Page* ret = NULL;
      if (page_ != p) {
        if (NULL != page_ && 0 == page_->ref(-ref_)) {
          ret = page_;
        }
        page_ = p;
        ref_ = 1;
      } else {
        ref_++;
      }
      return ret;
    }
  };
  Page* cur_page_;
  int64_t limit_;
  int64_t hold_ CACHE_ALIGNED;
  CacheRef cache_ref_;
public:
  FifoAlloc(): cur_page_(NULL), limit_(INT64_MAX), hold_(0) {}
  void set_limit(int64_t limit) { limit_ = limit; }
  void* alloc(int sz) {
    void* ret = NULL;
    if (NULL != cur_page_ && NULL != (ret = cur_page_->alloc(sz))) {
      return ret;
    }
    if (NULL != cur_page_) {
      retire_page(cur_page_);
    }
    if (NULL != (cur_page_ = alloc_page())) {
      ret = cur_page_->alloc(sz);
    }
    return ret;
  }
  void free(void* p) {
    Page* page = *((Page**)p - 1);
    if(page->MAGIC_ != MAGIC) {
      abort();
    }
    Page* p2free = cache_ref_.free(page);
    if (p2free) {
      free_page(p2free);
    }
    /*
    if (0 == page->ref(-1)) {
      free_page(page);
    }
    */
  }
private:
  Page* alloc_page() {
    Page* p = NULL;
    if (ATOMIC_LOAD(&hold_) > limit_) {
    } else if (ATOMIC_AAF(&hold_, PAGE_SIZE) > limit_) {
      ATOMIC_FAA(&hold_, -PAGE_SIZE);
    } else {
      p = (typeof(p))::malloc(PAGE_SIZE);
      new(p)Page(PAGE_SIZE);
    }
    return p;
  }
  void free_page(Page* p) {
    ATOMIC_FAA(&hold_, -PAGE_SIZE);
    ::free(p);
  }
  void retire_page(Page* p) {
    if(p->MAGIC_ != MAGIC) {
      abort();
    }
    if (0 == p->retire()) {
      free_page(p);
    }
  }
};
