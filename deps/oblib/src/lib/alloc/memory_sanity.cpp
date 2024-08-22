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
#include "lib/alloc/memory_sanity.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

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

int64_t global_addr = 0;

void *sanity_mmap(size_t size)
{
  if (0 == global_addr) return NULL;
  void *ret = NULL;
  void *ptr = (void*)ATOMIC_FAA(&global_addr, size);
  if (!sanity_addr_in_range(ptr, size)) {
    ATOMIC_FAA(&global_addr, -size);
    LOG_WARN_RET(OB_ERR_SYS, "sanity address exhausted", KP(ptr));
  } else {
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
      ret = ptr;
    }
  }
  return ret;
}

void sanity_munmap(void *ptr, size_t size)
{
  void *shadow_ptr = sanity_to_shadow((void*)ptr);
  size_t shadow_size = sanity_to_shadow_size(size);
  int rc = 0;
  if (0 == rc) while (-1 == (rc = ::madvise(ptr, size, MADV_DONTNEED)) && EAGAIN == errno);
  if (0 == rc) while (-1 == (rc = ::madvise(ptr, size, MADV_DONTDUMP)) && EAGAIN == errno);
  if (0 == rc) while (-1 == (rc = ::madvise(shadow_ptr, shadow_size, MADV_DONTNEED)) && EAGAIN == errno);
  if (0 == rc) while (-1 == (rc = ::madvise(shadow_ptr, shadow_size, MADV_DONTDUMP)) && EAGAIN == errno);
  if (rc != 0) {
    LOG_WARN_RET(OB_ERR_SYS, "madvise failed", K(errno));
  }
}

bool init_sanity()
{
  set_ob_mem_mgr_path();
  DEFER(unset_ob_mem_mgr_path(););
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
  if (0 == min) {
    LOG_WARN_RET(OB_ERR_SYS, "search region failed");
  } else if (MAP_FAILED == mmap((void*)min, max - min,
                                PROT_NONE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "reserve region failed", K(errno));
  } else if (MAP_FAILED == mmap(sanity_to_shadow((void*)min), sanity_to_shadow_size(max - min),
                                PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_NORESERVE, -1, 0)) {
    LOG_WARN_RET(OB_ERR_SYS, "reserve shadow region failed", K(errno));
  } else if (-1 == madvise((void*)min, max - min, MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "suggest region nodump failed", K(errno));
  } else if (-1 == madvise(sanity_to_shadow((void*)min), sanity_to_shadow_size(max - min), MADV_DONTDUMP)) {
    LOG_WARN_RET(OB_ERR_SYS, "suggest shadow region nodump failed", K(errno));
  } else {
    sanity_max_addr = max;
    sanity_min_addr = min;
    global_addr = sanity_min_addr;
    succ = true;
  }
  return succ;
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
