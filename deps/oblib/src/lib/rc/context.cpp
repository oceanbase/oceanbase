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
#include "lib/rc/context.h"
#include "lib/lock/ob_mutex.h"
#include "lib/rc/ob_rc.h"
#include "lib/coro/co_var.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace lib {
int64_t TreeNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(parent_));
  J_OBJ_END();
  return pos;
}
int64_t StaticInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(filename_), K(line_), K(function_));
  J_OBJ_END();
  return pos;
}
int64_t DynamicInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(tid_), K(cid_), K(create_time_));
  J_OBJ_END();
  return pos;
}
int64_t MemoryContext::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), "static_id", static_id_, "static_info", StaticInfos::get_instance().get(static_id_),"dynamic info", di_, K(properties_), K(attr_));
  J_OBJ_END();
  return pos;
}
RLOCAL(bool, ContextTLOptGuard::enable_tl_opt);

MemoryContext& MemoryContext::root()
{
  static MemoryContext* root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      ContextParam param;
      param.set_properties(ADD_CHILD_THREAD_SAFE | ALLOC_THREAD_SAFE)
          .set_parallel(4)
          .set_mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_ROOT_CONTEXT, ObCtxIds::DEFAULT_CTX_ID);
      // ObMallocAllocator to design a non-destroy mode
      const static int static_id = StaticInfos::get_instance().add(__FILENAME__, __LINE__, __FUNCTION__);
      MemoryContext* tmp = new (std::nothrow) MemoryContext(false, DynamicInfo(), nullptr, param, static_id);
      abort_unless(tmp != nullptr);
      int ret = tmp->init();
      abort_unless(OB_SUCCESS == ret);
      root = tmp;
    }
  }
  return *root;
}

}  // end of namespace lib
}  // end of namespace oceanbase
