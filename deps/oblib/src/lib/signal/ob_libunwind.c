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

#ifdef __x86_64__
#include "lib/signal/ob_libunwind.h"
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#include "util/easy_string.h"

static const int MAX_BT_ADDRESS_CNT = 100;
static int safe_backtrace_(unw_context_t *context, char *buf, int64_t len, int64_t *pos);
static ssize_t get_stack_trace_inplace(unw_context_t *context, unw_cursor_t *cursor,
    uintptr_t *addresses, size_t max_addresses);
static int8_t get_frame_info(unw_cursor_t *cursor, uintptr_t *ip);
extern int64_t safe_parray_c(char *buf, int64_t len, int64_t *array, int size);

int safe_backtrace(char *buf, int64_t len, int64_t *pos)
{
  int ret = 0;
  unw_context_t context;
  if (unw_getcontext(&context) < 0) {
    ret = -1;
  } else {
    ret = safe_backtrace_(&context, buf, len, pos);
  }
  return ret;
}

static int safe_backtrace_(unw_context_t *context, char *buf, int64_t len,
                   int64_t *pos)
{
  int ret = 0;
  unw_cursor_t cursor;
  uintptr_t addrs[MAX_BT_ADDRESS_CNT];
  int n = get_stack_trace_inplace(context, &cursor, addrs, sizeof(addrs)/sizeof(addrs[0]));
  *pos = 0;
  if (n < 0) {
    ret = -1;
  } else {
    *pos += safe_parray_c(buf + *pos, len - *pos, (int64_t*)addrs, n);
  }
  buf[*pos] = '\0';
  return ret;
}

ssize_t get_stack_trace_inplace(unw_context_t *context, unw_cursor_t *cursor,
    uintptr_t *addresses, size_t max_addresses)
{
  if (max_addresses == 0) {
    return 0;
  }
  if (unw_init_local(cursor, context) < 0) {
    return -1;
  }
  if (!get_frame_info(cursor, addresses)) {
    return -1;
  }
  ++addresses;
  size_t count = 1;
  for (; count != max_addresses; ++count, ++addresses) {
    int r = unw_step(cursor);
    if (r < 0) {
      return -1;
    }
    if (r == 0) {
      break;
    }
    if (!get_frame_info(cursor, addresses)) {
      return -1;
    }
  }
  return count;
}

int8_t get_frame_info(unw_cursor_t *cursor, uintptr_t *ip)
{
  unw_word_t uip;
  if (unw_get_reg(cursor, UNW_REG_IP, &uip) < 0) {
    return 0;
  }
  int r = unw_is_signal_frame(cursor);
  if (r < 0) {
    return 0;
  }
  // Use previous instruction in normal (call) frames (because the
  // return address might not be in the same function for noreturn functions)
  // but not in signal frames.
  *ip = uip - (r == 0);
  return 1;
}

#endif
