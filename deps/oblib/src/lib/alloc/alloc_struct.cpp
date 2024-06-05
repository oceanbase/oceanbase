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

#include "lib/alloc/alloc_struct.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/coro/co_var.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_fast_convert.h"

namespace oceanbase
{

using namespace common;

namespace lib
{
thread_local ObMemAttr ObMallocHookAttrGuard::tl_mem_attr(OB_SERVER_TENANT_ID,
                                                          "glibc_malloc",
                                                          ObCtxIds::GLIBC);
static int64_t g_divisive_mem_size[OB_MAX_CPU_NUM];
static thread_local bool g_is_ob_mem_mgr_path = false;

static bool g_memleak_light_backtrace_enabled = false;

uint32_t ObMemVersionNode::global_version = 0;
__thread bool ObMemVersionNode::tl_ignore_node = true;
__thread ObMemVersionNode* ObMemVersionNode::tl_node = NULL;

ObMallocHookAttrGuard::ObMallocHookAttrGuard(const ObMemAttr& attr)
  : old_attr_(tl_mem_attr)
{
  tl_mem_attr = attr;
  tl_mem_attr.ctx_id_ = ObCtxIds::GLIBC;
}

ObMallocHookAttrGuard::~ObMallocHookAttrGuard()
{
  tl_mem_attr = old_attr_;
}

bool ObLabel::operator==(const ObLabel &other) const
{
  bool bret = false;
  if (is_valid() && other.is_valid()) {
    if (str_[0] == other.str_[0]) {
      if (0 == STRCMP(str_, other.str_)) {
        bret = true;
      }
    } 
  } else if (!is_valid() && !other.is_valid()) {
    bret = true;
  }
  return bret;
}

ObLabel::operator const char *() const
{
  return str_;
}

int64_t ObLabel::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)common::logdata_printf(
      buf, buf_len, pos, "%s", (const char*)(*this));
  return pos;
}

int64_t ObMemAttr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)common::logdata_printf(
      buf, buf_len, pos,
      "tenant_id=%ld, label=%s, ctx_id=%ld, prio=%d",
      tenant_id_, (const char *)label_, ctx_id_, prio_);
  return pos;
}

void Label::fmt(char *buf, int64_t buf_len, int64_t &pos, const char *str)
{
  if (OB_UNLIKELY(pos >= buf_len)) {
  } else {
    int64_t len = snprintf(buf + pos, buf_len - pos, "%s", str);
    if (len < buf_len - pos) {
      pos += len;
    } else {
      pos = buf_len;
    }
  }
}

int64_t get_divisive_mem_size()
{
  int64_t total_size = 0;
  for (int64_t i = 0; i < OB_MAX_CPU_NUM; i++) {
    total_size += g_divisive_mem_size[i];
  }
  return total_size;
}

void inc_divisive_mem_size(const int64_t size)
{
  const int64_t idx = ob_gettid() % OB_MAX_CPU_NUM;
  __sync_fetch_and_add(&(g_divisive_mem_size[idx]), size);
}

void dec_divisive_mem_size(const int64_t size)
{
  const int64_t idx = ob_gettid() % OB_MAX_CPU_NUM;
  __sync_fetch_and_add(&(g_divisive_mem_size[idx]), 0 - size);
}

void set_ob_mem_mgr_path()
{
  g_is_ob_mem_mgr_path = true;
}

void unset_ob_mem_mgr_path()
{
  g_is_ob_mem_mgr_path = false;
}

bool is_ob_mem_mgr_path()
{
  return g_is_ob_mem_mgr_path;
}

void enable_memleak_light_backtrace(const bool enable)
{
#if defined(__x86_64__) || defined(__aarch64__)
  g_memleak_light_backtrace_enabled = enable;
#else
  UNUSED(enable);
#endif
}
bool is_memleak_light_backtrace_enabled()
{
  return g_memleak_light_backtrace_enabled;
}
} // end of namespace lib
} // end of namespace oceanbase
