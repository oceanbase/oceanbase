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

#define USING_LOG_PREFIX SHARE
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace share {
using namespace oceanbase::common;

#define INIT_BIND_FUNC_TMP(IDX)                                                     \
  ObTenantBase::init_m##IDX##_func_name ObTenantBase::init_m##IDX##_func = nullptr; \
  ObTenantBase::destory_m##IDX##_func_name ObTenantBase::destory_m##IDX##_func = nullptr;
#define INIT_BIND_FUNC(UNUSED, IDX) INIT_BIND_FUNC_TMP(IDX)
LST_DO2(INIT_BIND_FUNC, (), MTL_MEMBERS);

#define CONSTRUCT_MEMBER(T, IDX) m##IDX##_()
ObTenantBase::ObTenantBase(const int64_t id) : LST_DO2(CONSTRUCT_MEMBER, (, ), MTL_MEMBERS), id_(id), inited_(false)
{}
#undef CONSTRUCT_MEMBER

int ObTenantBase::init()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice error", K(ret));
  }

  if (OB_SYS_TENANT_ID == id_ || OB_DIAG_TENANT_ID == id_ || id_ > OB_MAX_RESERVED_TENANT_ID) {
    // @byDesign: Init will not fail even if the registration function is not set
#define INIT_TMP(IDX)                                                  \
  if (OB_SUCC(ret)) {                                                  \
    if (nullptr == ObTenantBase::init_m##IDX##_func) {                 \
      LOG_INFO("init_m" #IDX "_func hasn't been set", K(ret));         \
    } else if (OB_FAIL(ObTenantBase::init_m##IDX##_func(m##IDX##_))) { \
      LOG_WARN("m" #IDX "_ init failed", K(ret));                      \
    }                                                                  \
  }
#define INIT(UNUSED, IDX) INIT_TMP(IDX)
    LST_DO2(INIT, (), MTL_MEMBERS);
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  return ret;
}

void ObTenantBase::destory()
{
#define DESTORY_TMP(IDX)                                \
  if (ObTenantBase::destory_m##IDX##_func != nullptr) { \
    ObTenantBase::destory_m##IDX##_func(m##IDX##_);     \
  }
#define DESTORY(UNUSED, IDX) DESTORY_TMP(IDX)
  LST_DO2(DESTORY, (), MTL_MEMBERS);
}

}  // end of namespace share
}  // end of namespace oceanbase
