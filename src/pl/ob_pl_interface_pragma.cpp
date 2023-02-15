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

#define USING_LOG_PREFIX PL

#include "pl/ob_pl_interface_pragma.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{

using namespace common;

namespace pl
{

struct ObPLInterface {
  const char* name;
  void* entry;
};

static const ObPLInterface OB_PL_INTERFACE[INTERFACE_END +1] =
{
#define INTERFACE_DEF(type, name, entry) {name, entry},
#include "pl/ob_pl_interface_pragma.h"
#undef INTERFACE_DEF
};

void *ObPLInterfaceService::get_entry(ObString &name) const
{
  int64_t idx = get_type(name);
  void *entry_res = NULL;
  if (idx >= 0 && idx < sizeof(OB_PL_INTERFACE)/sizeof(ObPLInterface)) {
    entry_res = OB_PL_INTERFACE[idx].entry;
  }
  return entry_res;
}

int ObPLInterfaceService::init()
{
  int ret = OB_SUCCESS;
  if (!interface_map_.created() &&
             OB_FAIL(interface_map_.create(hash::cal_next_prime(32),
                                           ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    LOG_WARN("create interface map failed", K(ret));
  } else {
    for (int64_t i = INTERFACE_START; OB_SUCC(ret) && i < INTERFACE_END; ++i) {
      ObPLInterfaceType type = static_cast<ObPLInterfaceType>(i);
      if (OB_FAIL(interface_map_.set_refactored(ObString(OB_PL_INTERFACE[i].name), type))) {
        LOG_WARN("fail insert ps id to hash map", K(i), K(OB_PL_INTERFACE[i].name), K(type),K(ret));
      }
    }
  }
  return ret;
}

ObPLInterfaceType ObPLInterfaceService::get_type(ObString &name) const
{
  ObPLInterfaceType type = INTERFACE_END;
  if (OB_SUCCESS != interface_map_.get_refactored(name, type)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get interface failed", K(name), K(type));
  }
  return type;
}

int ObPLInterfaceImpl::call(sql::ObExecContext &ctx, sql::ParamStore &params, ObObj &result)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    LOG_DEBUG("========>>>>>>>>ryan.ly interface test", K(params.at(i)), K(ret));
  }
  OX (result.set_int32(42));
  return ret;
}


}
}

