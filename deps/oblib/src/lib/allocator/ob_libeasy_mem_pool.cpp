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

#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "rpc/obrpc/ob_rpc_packet.h"

using namespace oceanbase;
using namespace oceanbase::common;

void *common::ob_easy_realloc(void *ptr, size_t size)
{
  void *ret = NULL;
  if (size != 0) {
    ObMemAttr attr;
    // current_pcode() obtains pcode from thread local, rpc_proxy is responsible for setting thread local pcode
    // OB_MOD_DO_NOT_USE_ME is also 0, here conversion of pcode is to not play WARN when realloc
    obrpc::ObRpcPacketCode pcode = obrpc::current_pcode();
    if (0 == pcode) {
      pcode = obrpc::OB_TEST2_PCODE;
    }
    auto &set = obrpc::ObRpcPacketSet::instance();
    attr.label_ = set.name_of_idx(set.idx_of_pcode(pcode));
    attr.ctx_id_ = ObCtxIds::LIBEASY;
    attr.tenant_id_ = OB_SERVER_TENANT_ID;
    attr.prio_ = lib::OB_HIGH_ALLOC;
    {
      TP_SWITCH_GUARD(true);
      ret = ob_realloc(ptr, size, attr);
    }
    if (ret == NULL) {
      _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_realloc failed, ptr:%p, size:%lu", ptr, size);
    }
  } else if (ptr) {
    ob_free(ptr);
  }
  return ret;
}
