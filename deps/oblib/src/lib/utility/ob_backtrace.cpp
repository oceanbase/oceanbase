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

#define USING_LOG_PREFIX LIB

#include "lib/utility/ob_backtrace.h"
#include <stdio.h>
#include <execinfo.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/coro/co_var.h"

namespace oceanbase
{
namespace common
{

int ob_backtrace(void **buffer, int size)
{
  int rv = 0;
  if (OB_LIKELY(g_enable_backtrace)) {
    rv = backtrace(buffer, size);
  }
  return rv;
}

bool read_min_max_addr(int64_t &min_addr, int64_t &max_addr)
{
  bool bret = false;
  FILE *fp = fopen("/proc/self/maps", "r");
  if (!fp) return bret;
  DEFER(fclose(fp));
  char line[512];
  min_addr = INT64_MAX;
  max_addr = -1;
  int64_t addr = (int64_t)__func__;
  while (fgets(line, sizeof(line), fp) != NULL) {
    int64_t start, end, inode, offset, major, minor;
    char perms[8];
    char path[256];
    int n = sscanf(line,
                   "%lx-%lx %4s %lx %lx:%lx %ld %255s",
                   &start, &end, perms,
                   &offset, &major, &minor, &inode, path);
    if (n < 8) {
      continue;
    }
    uint64_t dst_inode = inode;
    if (start <= addr && addr < end) {
      bret = true;
      fseek(fp, 0, SEEK_SET);
      while (fgets(line, sizeof(line), fp) != NULL) {
        int n = sscanf(line,
                       "%lx-%lx %4s %lx %lx:%lx %ld %255s",
                       &start, &end, perms,
                       &offset, &major, &minor, &inode, path);
        if (n < 8) {
          continue;
        }
        if (dst_inode != inode) {
          continue;
        }
        if (start < min_addr) {
          min_addr = start;
        }
        if (end > max_addr) {
          max_addr = end;
        }
      }
      break;
    }
  }
  return bret;
}

bool g_enable_backtrace = true;
struct ProcMapInfo
{
  int64_t code_start_addr_;
  int64_t code_end_addr_;
  bool is_inited_;
};

ProcMapInfo g_proc_map_info{.code_start_addr_ = -1, .code_end_addr_ = -1, .is_inited_ = false};

void init_proc_map_info()
{
  read_min_max_addr(g_proc_map_info.code_start_addr_, g_proc_map_info.code_end_addr_);
  g_proc_map_info.is_inited_ = true;
}

int64_t get_rel_offset(int64_t addr)
{
  int64_t code_start_addr = -1;
  int64_t code_end_addr = -1;
  if (OB_UNLIKELY(!g_proc_map_info.is_inited_)) {
    read_min_max_addr(code_start_addr, code_end_addr);
  } else {
    code_start_addr = g_proc_map_info.code_start_addr_;
    code_end_addr = g_proc_map_info.code_end_addr_;
  }
  if (code_start_addr != -1) {
    if (OB_LIKELY(addr >= code_start_addr && addr < code_end_addr)) {
      addr -= code_start_addr;
    }
  }
  return addr;
}


constexpr int MAX_ADDRS_COUNT = 100;
using PointerBuf = void *[MAX_ADDRS_COUNT];
RLOCAL(PointerBuf, addrs);
RLOCAL(ByteBuf<LBT_BUFFER_LENGTH>, buffer);

char *lbt()
{
  int size = OB_BACKTRACE_M(addrs, MAX_ADDRS_COUNT);
  return parray(*&buffer, LBT_BUFFER_LENGTH, (int64_t *)addrs, size);
}

char *lbt(char *buf, int32_t len)
{
  int size = OB_BACKTRACE_M(addrs, MAX_ADDRS_COUNT);
  return parray(buf, len, (int64_t *)addrs, size);
}

char *parray(int64_t *array, int size)
{
  return parray(buffer, LBT_BUFFER_LENGTH, array, size);
}

char *parray(char *buf, int64_t len, int64_t *array, int size)
{
  //As used in lbt, and lbt used when print error log.
  //Can not print error log this function.
  if (NULL != buf && len > 0 && NULL != array) {
    int64_t pos = 0;
    int64_t count = 0;
    for (int64_t i = 0; i < size; i++) {
      int64_t addr = get_rel_offset(array[i]);
      if (0 == i) {
        count = snprintf(buf + pos, len - pos, "0x%lx", addr);
      } else {
        count = snprintf(buf + pos, len - pos, " 0x%lx", addr);
      }
      if (count >= 0 && pos + count < len) {
        pos += count;
      } else {
        // buf not enough
        break;
      }
    }
    buf[pos] = 0;
  }
  return buf;
}

void addrs_to_offsets(void **buffer, int size)
{
  for (int64_t i = 0; i < size; i++) {
    buffer[i] = (void*)get_rel_offset((int64_t)buffer[i]);
  }
}

EXTERN_C_BEGIN
int ob_backtrace_c(void **buffer, int size)
{
  return OB_BACKTRACE_M(buffer, size);
}
char *parray_c(char *buf, int64_t len, int64_t *array, int size)
{
  return parray(buf, len, array, size);
}
int64_t get_rel_offset_c(int64_t addr)
{
  return get_rel_offset(addr);
}
EXTERN_C_END

} // end namespace common
} // end namespace oceanbase
