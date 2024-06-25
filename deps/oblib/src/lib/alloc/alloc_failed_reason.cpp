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

#include "lib/alloc/alloc_failed_reason.h"
#include <sys/sysinfo.h>
#include <unistd.h>
#include <stdio.h>
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace lib
{

AllocFailedCtx &g_alloc_failed_ctx()
{
  struct GafcBuf {
    char v_[sizeof(AllocFailedCtx)];
  };
  RLOCAL(GafcBuf, gafc_buf);
  return *reinterpret_cast<AllocFailedCtx*>((&gafc_buf)->v_);
}

void get_process_physical_hold(int64_t &phy_hold)
{
  phy_hold = 0;
  int64_t unused = 0;
  char buffer[1024] = "";
  FILE* file = fopen("/proc/self/status", "r");
  if (NULL != file) {
    while (fscanf(file, " %1023s", buffer) == 1) {
      if (strcmp(buffer, "VmRSS:") == 0) {
        fscanf(file, " %ld", &phy_hold);
      }
      if (strcmp(buffer, "VmHWM:") == 0) {
        fscanf(file, " %ld", &unused);
      }
      if (strcmp(buffer, "VmSize:") == 0) {
        fscanf(file, " %ld", &unused);
      }
      if (strcmp(buffer, "VmPeak:") == 0) {
        fscanf(file, " %ld", &unused);
      }
    }
    fclose(file);
  }
}

char *alloc_failed_msg()
{
  const int len = 256;
  struct MsgBuf {
    char v_[len];
  };
  RLOCAL(MsgBuf, buf);
  char *msg = (&buf)->v_;
  auto &afc = g_alloc_failed_ctx();
  switch (afc.reason_) {
  case UNKNOWN: {
      snprintf(msg, len,
               "unknown(alloc_size: %ld)",
               afc.alloc_size_);
      break;
    }
  case SINGLE_ALLOC_SIZE_OVERFLOW: {
      snprintf(msg, len,
               "single alloc size large than 4G is not allowed(alloc_size: %ld)",
               afc.alloc_size_);
      break;
    }
  case CTX_HOLD_REACH_LIMIT : {
      snprintf(msg, len,
               "ctx memory has reached the upper limit(ctx_name: %s, ctx_hold: %ld, ctx_limit: %ld, alloc_size: %ld)",
               common::get_global_ctx_info().get_ctx_name(afc.ctx_id_), afc.ctx_hold_, afc.ctx_limit_, afc.alloc_size_);
      break;
    }
  case TENANT_HOLD_REACH_LIMIT: {
      snprintf(msg, len,
               "tenant memory has reached the upper limit(tenant_id: %lu, tenant_hold: %ld, tenant_limit: %ld, alloc_size: %ld)",
               afc.tenant_id_, afc.tenant_hold_, afc.tenant_limit_, afc.alloc_size_);
      break;
    }
  case SERVER_HOLD_REACH_LIMIT: {
      snprintf(msg, len,
               "server memory has reached the upper limit(server_hold: %ld, server_limit: %ld, alloc_size: %ld)",
               afc.server_hold_, afc.server_limit_, afc.alloc_size_);
      break;
    }
  case PHYSICAL_MEMORY_EXHAUST: {
      int64_t process_hold = 0;
      int64_t virtual_memory_used = common::get_virtual_memory_used(&process_hold);
      snprintf(msg, len,
               "physical memory exhausted(os_total: %ld, os_available: %ld, virtual_memory_used: %ld, server_hold: %ld, errno: %d, alloc_size: %ld)",
               sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE),
               sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE),
               virtual_memory_used,
               process_hold,
               afc.errno_,
               afc.alloc_size_);
      break;
    }
  case ERRSIM_INJECTION: {
      snprintf(msg, len, "errsim injection");
      break;
    }
  default: {
      snprintf(msg, len, "unknown reason");
      break;
    }
  }
  return msg;
}

} // end of namespace lib
} // end of namespace oceanbase
