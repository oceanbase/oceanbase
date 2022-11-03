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

#define USING_LOG_PREFIX COMMON
#include "common/ob_common_utility.h"

using namespace oceanbase::lib;

namespace oceanbase
{
namespace common
{
_RLOCAL(char*, g_stackaddr);
_RLOCAL(size_t, g_stacksize);

const char *print_server_role(const ObServerRole server_role)
{
  const char *role_string = NULL;
  switch (server_role) {
    case OB_CHUNKSERVER:
      role_string = "chunkserver";
      break;
    case OB_MERGESERVER:
      role_string = "mergeserver";
      break;
    case OB_ROOTSERVER:
      role_string = "rootserver";
      break;
    case OB_UPDATESERVER:
      role_string = "updateserver";
      break;
    case OB_SERVER:
      role_string = "observer";
      break;
    default:
      role_string = "invalid_role";
  }
  return role_string;
}

static int64_t reserved_stack_size = 64L << 10;

int64_t get_reserved_stack_size()
{
  return reserved_stack_size;
}

void set_reserved_stack_size(int64_t reserved_size)
{
  reserved_stack_size = reserved_size;
}

int check_stack_overflow(bool &is_overflow,
                         int64_t reserved_size/* default equals 'reserved_stack_size' variable*/,
                         int64_t *used_size/*nullptr*/)
{
  int ret = OB_SUCCESS;
  is_overflow = false;
  size_t stack_size = 0;
  char *stack_eof = NULL;
  void *cur_stack = NULL;
  void *stack_start = NULL;
  if (OB_FAIL(get_stackattr(stack_start, stack_size))) {
    LOG_ERROR("get stack attributes fail", K(ret));
    is_overflow = true;
  } else {
    stack_eof = static_cast<char *>(stack_start) + stack_size;
    cur_stack = &stack_start;
    if (OB_UNLIKELY(static_cast<int64_t>(stack_size) < reserved_size)) { //stack size is the whole stack size
      ret = OB_ERR_UNEXPECTED;
      is_overflow = true;
      COMMON_LOG(ERROR, "stack size smaller than reserved_stack_size ",
          K(ret), K(stack_size), K(reserved_size));
    } else if (OB_UNLIKELY(stack_eof < static_cast<char *>(cur_stack))) {
      is_overflow = true;
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "stack incorrect params", K(ret), KP(stack_eof), KP(cur_stack));
    } else {
      int64_t cur_stack_used = stack_eof - (static_cast<char *>(cur_stack));
      if (used_size != nullptr) {
        *used_size = cur_stack_used;
      }
      COMMON_LOG(DEBUG, "stack info ", K(cur_stack_used), K(stack_size), K(reserved_size));
      if (OB_UNLIKELY(cur_stack_used > (static_cast<int64_t>(stack_size) - reserved_size))) {
        is_overflow = true;
        COMMON_LOG(WARN, "stack possible overflow", KP(cur_stack), KP(stack_eof),
            KP(stack_start), K(stack_size), K(reserved_size), K(cur_stack_used));
      }
    }
  }
  return ret;
}

int get_stackattr(void *&stackaddr, size_t &stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(g_stackaddr != nullptr)) {
    stackaddr = g_stackaddr;
    stacksize = g_stacksize;
  } else {
    pthread_attr_t attr;
    if (OB_UNLIKELY(0 != pthread_getattr_np(pthread_self(), &attr))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "cannot get thread params", K(ret));
    } else if (OB_UNLIKELY(0 != pthread_attr_getstack(&attr, &stackaddr, &stacksize))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "cannot get thread statck params", K(ret));
    } else if (OB_UNLIKELY(0 != pthread_attr_destroy(&attr))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "destroy thread attr failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      g_stackaddr = (char*)stackaddr;
      g_stacksize = stacksize;
    }
  }
  return ret;
}

void set_stackattr(void *stackaddr, size_t stacksize)
{
  g_stackaddr = (char*)stackaddr;
  g_stacksize = stacksize;
}

ObFatalErrExtraInfoGuard::ObFatalErrExtraInfoGuard()
{
  last_ = get_val();
  get_val() = this;
}

ObFatalErrExtraInfoGuard::~ObFatalErrExtraInfoGuard()
{
  get_val() = last_;
}

ObFatalErrExtraInfoGuard *&ObFatalErrExtraInfoGuard::get_val()
{
  RLOCAL(ObFatalErrExtraInfoGuard *, value_ptr);
  return value_ptr;
}

const ObFatalErrExtraInfoGuard *ObFatalErrExtraInfoGuard::get_thd_local_val_ptr()
{
  return ObFatalErrExtraInfoGuard::get_val();
}

} // end of namespace common
} // end of namespace oceanbse
