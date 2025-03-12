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

#ifndef ENABLE_SANITY
#else
#define USING_LOG_PREFIX LIB
#include "memory_sanity.h"

int do_madvise(void *addr, size_t length, int advice)
{
  int result = 0;
  do {
    result = ::madvise(addr, length, advice);
  } while (result == -1 && errno == EAGAIN);
  return result;
}

bool is_range_mapped(int64_t start, int64_t end)
{
  FILE *maps_file = fopen("/proc/self/maps", "r");
  if (maps_file == NULL) {
    perror("fopen");
    return false;
  }

  char line[256];
  while (fgets(line, sizeof(line), maps_file)) {
    int64_t addr_start, addr_end;
    if (sscanf(line, "%" PRIxPTR "-%" PRIxPTR, &addr_start, &addr_end) != 2) {
      continue;
    }

    if ((start < addr_end) && (end > addr_start)) {
      fclose(maps_file);
      return true;
    }
  }

  fclose(maps_file);
  return false;
}

class ISegMgr
{
public:
  virtual void* alloc_seg(int64_t size) = 0 ;
  virtual void free_seg(void *ptr) = 0;
};

template<int _size>
class FixedAllocer
{
public:
  FixedAllocer() : head_(NULL), using_cnt_(0), free_cnt_(0) {}
  int64_t using_cnt() { return using_cnt_; }
  int64_t free_cnt() { return free_cnt_; }
  void *alloc()
  {
    void *ret = NULL;
    if (!head_) {
      int64_t alloc_size = max(get_page_size(), _size);
      void *ptr = NULL;
      if (MAP_FAILED == (ptr = mmap(NULL, alloc_size,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
        LOG_WARN_RET(OB_ERR_SYS, "mmap failed", K(errno));
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
    return ret;
  }
  void free(void *ptr)
  {
    using_cnt_--;
    free_cnt_++;
    if (!head_) {
      *(void**)ptr = NULL;
      head_ = ptr;
    } else {
      *(void**)ptr = head_;
      head_ = ptr;
    }
  }
private:
  void *head_;
  int64_t using_cnt_;
  int64_t free_cnt_;
};

class BuddyMgr
{
public:
  struct Bin
  {
    Bin()
      : in_use_(0), is_large_(0), seg_(NULL), offset_(0), nbins_(0),
        phy_prev_(NULL), phy_next_(NULL), prev_(NULL), next_(NULL)  {}
    union {
      uint32_t MAGIC_CODE_;
      struct {
        struct {
          uint8_t in_use_ : 1;
          uint8_t is_large_ : 1;
        };
      };
    };
    void *seg_;
    int64_t offset_;
    int64_t nbins_;
    Bin *phy_prev_;
    Bin *phy_next_;
    Bin *prev_;
    Bin *next_;
  };
  struct FreeList
  {
    Bin *head_;
    int64_t cnt_;
  };
  BuddyMgr(int64_t full_size, int64_t bin_size)
    : full_size_(full_size), bin_size_(bin_size),
      seg_mgr_(NULL), kept_nseg_limit_(0)
  {
    abort_unless(0 == (bin_size & (bin_size - 1)));
    abort_unless(full_size > bin_size);
    abort_unless(0 == full_size % bin_size);
    max_nbins_ = full_size / bin_size;
    max_ind_ = calc_next_ind(max_nbins_);
    memset(freelists_, 0, sizeof(freelists_));
  }
  int64_t get_bin_size()
  { return bin_size_; }
  void set_seg_mgr(ISegMgr *seg_mgr)
  { seg_mgr_ = seg_mgr; }
  void set_kept_nseg_limit(int limit)
  { kept_nseg_limit_ = limit; }
  Bin *alloc(int64_t size)
  {
    Bin *bin = NULL;
    int nbins = align_up2(size, bin_size_) / bin_size_;
    int ind = calc_next_ind(nbins);
    if (ind > max_ind_) {
      void *seg = seg_mgr_->alloc_seg(size);
      if (seg) {
        bin = alloc_bin();
        if (bin) {
          bin->is_large_ = 1;
          bin->seg_ = seg;
        }
      }
    } else {
      Bin *avail_bin = NULL;
      for (int i = ind; i <= max_ind_; i++) {
        avail_bin = freelists_[i].head_;
        if (avail_bin) {
          break;
        }
      }
      if (avail_bin != NULL) {
        take_off_free_bin(avail_bin);
        if (avail_bin->nbins_ > nbins) {
          Bin *next_bin = split_bin(avail_bin, nbins);
          if (next_bin) {
            add_free_bin(next_bin);
          }
        }
        bin = avail_bin;
      }
      if (!bin) {
        void *seg = seg_mgr_->alloc_seg(full_size_);
        if (seg) {
          bin = alloc_bin();
          if (bin) {
            bin->seg_ = seg;
            bin->offset_ = 0;
            bin->nbins_ = max_nbins_;
            Bin *next_bin = split_bin(bin, nbins);
            if (next_bin) {
              add_free_bin(next_bin);
            }
          }
        }
      }
    }
    if (bin) {
      bin->in_use_ = 1;
    }
    return bin;
  }
  void free(Bin *bin)
  {
    bin->in_use_ = 0;
    if (bin->is_large_) {
      void *seg = bin->seg_;
      free_bin(bin);
      seg_mgr_->free_seg(seg);
    } else {
      Bin *phy_prev = bin->phy_prev_;
      Bin *phy_next = bin->phy_next_;
      Bin *head = bin;
      if (phy_prev && !phy_prev->in_use_) {
        take_off_free_bin(phy_prev);
        head = merge_bin(phy_prev, bin);
      }
      if (phy_next && !phy_next->in_use_) {
        take_off_free_bin(phy_next);
        head = merge_bin(head, phy_next);
      }
      if (max_nbins_ == head->nbins_ &&
          freelists_[max_ind_].cnt_ >= kept_nseg_limit_) {
        void *seg = head->seg_;
        free_bin(head);
        seg_mgr_->free_seg(seg);
      } else {
        add_free_bin(head);
      }
    }
  }
private:
  int calc_next_ind(int64_t nbins)
  {
    abort_unless(nbins > 0);
    return (0 == nbins - 1) ? 0 : (64 - __builtin_clzll(nbins - 1));
  }
  int calc_prev_ind(int64_t nbins)
  {
    abort_unless(nbins > 0);
    return 64 - __builtin_clzll(nbins) - 1;
  }
  Bin *alloc_bin()
  {
    Bin *bin = NULL;
    void *ptr = bin_allocer_.alloc();
    if (ptr) {
      bin = new(ptr) Bin();
    }
    return bin;
  }
  void free_bin(Bin *bin)
  {
    bin->~Bin();
    bin_allocer_.free((void*)bin);
  }
  Bin *split_bin(Bin *bin, int nbins)
  {
    Bin *next_bin = bin->nbins_ <= nbins ? NULL : alloc_bin();
    if (next_bin) {
      next_bin->seg_ = bin->seg_;
      next_bin->offset_ = bin->offset_ + nbins;
      next_bin->nbins_ = bin->nbins_ - nbins;
      next_bin->phy_prev_ = bin;
      next_bin->phy_next_ = bin->phy_next_;
      if (bin->phy_next_) {
       bin->phy_next_->phy_prev_ = next_bin;
      }
      bin->phy_next_ = next_bin;
      bin->nbins_ = nbins;
    }
    return next_bin;
  }
  Bin *merge_bin(Bin *bin, Bin *next_bin)
  {
    bin->nbins_ += next_bin->nbins_;
    bin->phy_next_ = next_bin->phy_next_;
    if (next_bin->phy_next_) {
      next_bin->phy_next_->phy_prev_ = bin;
    }
    free_bin(next_bin);
    return bin;
  }
  void add_free_bin(Bin *bin)
  {
    FreeList &freelist = freelists_[calc_prev_ind(bin->nbins_)];
    Bin *&head = freelist.head_;
    if (!head) {
      bin->prev_ = bin->next_ = bin;
    } else {
      bin->prev_ = head->prev_;
      bin->next_ = head;
      head->prev_->next_ = bin;
      head->prev_ = bin;
    }
    head = bin;
    freelist.cnt_++;
  }
  void take_off_free_bin(Bin *bin)
  {
    FreeList &freelist = freelists_[calc_prev_ind(bin->nbins_)];
    Bin *&head = freelist.head_;
    if (bin == bin->next_) {
      head = NULL;
    } else {
      bin->prev_->next_ = bin->next_;
      bin->next_->prev_ = bin->prev_;
      if (head == bin) {
        head = head->next_;
      }
    }
    freelist.cnt_--;
  }
private:
  int64_t full_size_;
  int64_t bin_size_;
  int64_t max_nbins_;
  int max_ind_;
  ISegMgr *seg_mgr_;
  int64_t kept_nseg_limit_;
  FixedAllocer<sizeof(Bin)> bin_allocer_;
  // [1,2), [2,4), [4,8) ...
  FreeList freelists_[64];
};

extern int64_t global_addr;
class VMMgr
{
public:
  VMMgr()
    : buddy_mgr_(4L<<30, 2L<<20)
  {
    pthread_mutex_init(&mutex_, NULL);
    buddy_mgr_.set_seg_mgr(&seg_mgr_);
    buddy_mgr_.set_kept_nseg_limit(16);
  }
  class SegMgr : public ISegMgr
  {
  public:
    virtual void* alloc_seg(int64_t size) override
    {
      void *ptr = (void*)ATOMIC_FAA(&global_addr, size);
      if (!sanity_addr_in_range(ptr, size)) {
        ATOMIC_FAA(&global_addr, -size);
        LOG_WARN_RET(OB_ERR_SYS, "sanity address exhausted", KP(ptr));
        ptr = NULL;
      }
      return ptr;
    }
    virtual void free_seg(void *ptr) override
    {
    }
  };
  void *alloc_addr(int64_t size, int64_t &addr)
  {
    pthread_mutex_lock(&mutex_);
    DEFER(pthread_mutex_unlock(&mutex_));
    BuddyMgr::Bin *bin = buddy_mgr_.alloc(size);
    if (bin) {
      addr = (int64_t)bin->seg_ + bin->offset_ * buddy_mgr_.get_bin_size();
    }
    return (void*)bin;
  }
  void free_addr(void *bin)
  {
    pthread_mutex_lock(&mutex_);
    DEFER(pthread_mutex_unlock(&mutex_));
    buddy_mgr_.free((BuddyMgr::Bin*)bin);
  }
private:
  pthread_mutex_t mutex_;
  SegMgr seg_mgr_;
  BuddyMgr buddy_mgr_;
};

int64_t global_addr = 0;
VMMgr *g_vm_mgr = NULL;

int64_t get_global_addr()
{
  return ATOMIC_LOAD(&global_addr);
}

bool init_sanity()
{
  ObUnmanagedMemoryStat::DisableGuard guard;
  bool succ = false;
  int64_t maxs[] = {0x600000000000, 0x500000000000, 0x400000000000};
  int64_t mins[] = {0, 0, 0};
  int N = sizeof(maxs)/sizeof(maxs[0]);
  for (int i = 0; i < N; i++) {
    int64_t max = maxs[i];
    int64_t min = max>>3;
    int64_t step = 128L<<30;
    do {
      if (!is_range_mapped(min, max) &&
          !is_range_mapped(sanity_to_shadow_size(min), sanity_to_shadow_size(max))) {
        mins[i] = min;
        break;
      }
      min += step;
    } while (min < max);
  }
  int64_t max = 0;
  int64_t min = 0;
  for (int i = 0; i < N; i++) {
    if (mins[i] && maxs[i] - mins[i] > max - min) {
      max = maxs[i];
      min = mins[i];
    }
  }
  void *ptr = (void*)min;
  void *shadow_ptr = sanity_to_shadow(ptr);
  size_t size = max - min;
  size_t shadow_size = sanity_to_shadow_size(size);
  if (0 == min) {
    LOG_WARN_RET(OB_ERR_SYS, "search region failed");
  } else if (MAP_FAILED == mmap(ptr, size,
                                PROT_NONE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "reserve region failed", K(errno));
  } else if (MAP_FAILED == mmap(shadow_ptr, shadow_size,
                                PROT_NONE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "reserve shadow region failed", K(errno));
  } else if (-1 == do_madvise(ptr, size, MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "madvise failed", K(errno));
  } else if (-1 == do_madvise(shadow_ptr, shadow_size, MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "madvise shadow failed", K(errno));
  } else {
    sanity_max_addr = max;
    sanity_min_addr = min;
    global_addr = sanity_min_addr;
    static VMMgr vm_mgr;
    g_vm_mgr = &vm_mgr;
    succ = true;
  }
  return succ;
}

void *sanity_mmap(size_t size)
{
  if (0 == global_addr) return NULL;
  void *ret = NULL;
  int64_t addr;
  void *ref = g_vm_mgr->alloc_addr(size, addr);
  if (!ref) {
  } else {
    void *ptr = (void*)addr;
    void *shadow_ptr = sanity_to_shadow(ptr);
    size_t shadow_size = sanity_to_shadow_size(size);
    if (MAP_FAILED == mmap(ptr, size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0)) {
      LOG_WARN_RET(OB_ERR_SYS, "mmap failed", K(errno));
    } else if (MAP_FAILED == mmap(shadow_ptr, shadow_size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0)) {
      LOG_WARN_RET(OB_ERR_SYS, "mmap shadow failed", K(errno));
    } else {
      *(void**)ptr = ref;
      ret = ptr;
    }
  }
  return ret;
}

void sanity_munmap(void *ptr, size_t size)
{
  void *ref = *(void**)ptr;
  void *shadow_ptr = sanity_to_shadow((void*)ptr);
  size_t shadow_size = sanity_to_shadow_size(size);
  if (MAP_FAILED == mmap(ptr, size,
      PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "mmap failed", K(errno));
  }
  if (MAP_FAILED == mmap(shadow_ptr, shadow_size,
      PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "mmap shadow failed", K(errno));
  }
  if (-1 == do_madvise(ptr, size, MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "madvise failed", K(errno));
  }
  if (-1 == do_madvise(shadow_ptr, shadow_size, MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "madvise shadow failed", K(errno));
  }
  g_vm_mgr->free_addr(ref);
}

struct t_vip {
public:
  typedef char t_func[128];
  t_func func_;
  int64_t min_addr_;
  int64_t max_addr_;
};
static char whitelist[1024]; // store origin str
static t_vip vips[8];

void sanity_set_whitelist(const char *str)
{
  if (0 == strncmp(str, whitelist, sizeof(whitelist))) {
  } else {
    strncpy(whitelist, str, sizeof(whitelist));
    memset(vips, 0, sizeof(vips));
    decltype(whitelist) whitelist_cpy;
    memcpy(whitelist_cpy, whitelist, sizeof(whitelist));
    char *p = whitelist_cpy;
    char *saveptr = NULL;
    int i = 0;
    while ((p = strtok_r(p, ";", &saveptr)) != NULL && i < 8) {
      t_vip *vip = &vips[i++];
      strncpy(vip->func_, p, sizeof(vip->func_));
      vip->min_addr_ = vip->max_addr_ = 0;
      p = saveptr;
    }
  }
}

BacktraceSymbolizeFunc backtrace_symbolize_func = NULL;

void memory_sanity_abort()
{
  if ('\0' == whitelist[0]) {
    abort();
  }
  void *addrs[128];
  int n_addr = ob_backtrace(addrs, sizeof(addrs)/sizeof(addrs[0]));
  addrs_to_offsets(addrs, n_addr);
  void *vip_addr = NULL;
  for (int i = 0; NULL == vip_addr && i < n_addr; i++) {
    for (int j = 0; NULL == vip_addr && j < sizeof(vips)/sizeof(vips[0]); j++) {
      t_vip *vip = &vips[j];
      if ('\0' == vip->func_[0]) {
        break;
      } else if (0 == vip->min_addr_ || 0 == vip->max_addr_) {
        continue;
      } else if ((int64_t)addrs[i] >= vip->min_addr_ && (int64_t)addrs[i] <= vip->max_addr_) {
        vip_addr = addrs[i];
        break;
      }
    }
  }
  if (vip_addr != NULL) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      fprintf(stderr, "[ERROR] sanity check failed, vip_addr: %p, lbt: %s\n",
              vip_addr, oceanbase::common::parray((int64_t*)addrs, n_addr));
    }
  } else {
    char buf[8192];
    int64_t buf_len = sizeof(buf);
    int32_t pos = 0;
    t_vip::t_func vip_func = {'\0'};
    auto check_vip = [&](void *addr, const char *func_name, const char *file_name, uint32_t line)
    {
      int real_len = snprintf(buf + pos, buf_len - pos, "    %-14p %s %s:%u\n", addr, func_name, file_name, line);
      if (real_len < buf_len - pos) {
        pos += real_len;
      }
      for (int i = 0; i < sizeof(vips)/sizeof(vips[0]); i++) {
        t_vip *vip = &vips[i];
        if ('\0' == vip->func_[0]) {
          break;
        } else if (strstr(func_name, vip->func_) != NULL) {
          strncpy(vip_func, func_name, sizeof(vip_func));
          if ((int64_t)addr < vip->min_addr_ || 0 == vip->min_addr_) {
            vip->min_addr_ = (int64_t)addr;
          }
          if ((int64_t)addr > vip->max_addr_) {
            vip->max_addr_ = (int64_t)addr;
          }
          break;
        }
      }
    };
    if (backtrace_symbolize_func != NULL) {
      backtrace_symbolize_func(addrs, n_addr, check_vip);
    }
    while (pos > 0 && '\n' == buf[pos - 1]) pos--;
    fprintf(stderr, "[ERROR] sanity check failed, vip_func: %s, lbt: %s\nsymbolize:\n%.*s\n", vip_func,
            oceanbase::common::parray((int64_t*)addrs, n_addr), pos, buf);
    if ('\0' == vip_func[0]) {
      abort();
    }
  }
}
#endif
