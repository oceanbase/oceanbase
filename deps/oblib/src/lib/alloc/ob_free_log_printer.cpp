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

#include "lib/alloc/ob_free_log_printer.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
using namespace oceanbase::lib;
using namespace oceanbase::common;

ObFreeLogPrinter::ObFreeLogPrinter() : level_(DEFAULT), lock_()
{}

ObFreeLogPrinter& ObFreeLogPrinter::get_instance()
{
  static ObFreeLogPrinter free_log_print;
  return free_log_print;
}

int ObFreeLogPrinter::get_level()
{
  int ret = DEFAULT;
  int reason = g_alloc_failed_ctx().reason_;
  switch (reason) {
  case CTX_HOLD_REACH_LIMIT : {
      ret = CTX;
      break;
    }
  case TENANT_HOLD_REACH_LIMIT: {
      ret = TENANT;
      break;
    }
  case SERVER_HOLD_REACH_LIMIT: {
      ret = SERVER;
      break;
    }
  case PHYSICAL_MEMORY_EXHAUST: {
      ret = SERVER;
      break;
    }
  }
  return ret;
}

void ObFreeLogPrinter::enable_free_log(int64_t tenant_id, int64_t ctx_id, int level)
{
  bool has_lock = false;
  if (DEFAULT == level_ && OB_SUCCESS == lock_.trylock()) {
    DEFER(lock_.unlock());
    last_enable_time_ = ObTimeUtility::current_time();
    tenant_id_ = tenant_id;
    ctx_id_ = ctx_id;
    level_ = level;
    has_lock = true;
  }
  if (has_lock) {
    _OB_LOG(INFO, "start to print free log");
  }
}

void ObFreeLogPrinter::disable_free_log()
{
  bool has_lock = false;
  if (DEFAULT != level_ && OB_SUCCESS == lock_.trylock()) {
    DEFER(lock_.unlock());
    level_ = DEFAULT;
    has_lock = true;
  }
  if (has_lock) {
    _OB_LOG(INFO, "finish to print free log");
  }
}

void ObFreeLogPrinter::print_free_log(int64_t tenant_id, int64_t ctx_id, AObject *obj)
{
  if (DEFAULT != level_ && obj->on_malloc_sample_) {
    if (ObTimeUtility::current_time() - last_enable_time_ > MAX_FREE_LOG_TIME) {
      disable_free_log();
    } else {
      bool allowed = false;
      switch (level_) {
        case CTX: {
          allowed = tenant_id == tenant_id_ && ctx_id == ctx_id_;
          break;
        }
        case TENANT: {
          allowed = tenant_id == tenant_id_;
          break;
        }
        case SERVER: {
          allowed = true;
          break;
        }
      }
      if (allowed) {
        char buf[MAX_BACKTRACE_LENGTH];
        int64_t addrs[AOBJECT_BACKTRACE_COUNT];
        int64_t offset = obj->alloc_bytes_ - AOBJECT_BACKTRACE_SIZE;
        MEMCPY((char*)addrs, &obj->data_[offset], sizeof(addrs));
        IGNORE_RETURN parray(buf, sizeof(buf), addrs, ARRAYSIZEOF(addrs));
        allow_next_syslog();
        _OB_LOG(INFO, "[MEM_FREE_LOG] tenant_id=%ld, ctx_name=%s, mod=%s, alloc_bytes=%u, malloc_bt=%s\n",
                tenant_id, get_global_ctx_info().get_ctx_name(ctx_id),
                obj->label_, obj->alloc_bytes_, buf);
      }
    }
  }
}
