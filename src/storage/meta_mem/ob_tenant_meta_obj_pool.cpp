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

#define USING_LOG_PREFIX STORAGE

#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

constexpr const char RPMetaObjLabel::LABEL[];

TryWashTabletFunc::TryWashTabletFunc(ObTenantMetaMemMgr &t3m)
  : t3m_(t3m)
{
}

TryWashTabletFunc::~TryWashTabletFunc()
{
}

int TryWashTabletFunc::operator()()
{
  return t3m_.try_wash_tablet();
}

} // end namespace storage
} // end namespace oceanbase
