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

#include "pl/ob_pl_package.h"
#include "ob_pl_package_guard.h"

namespace oceanbase
{
namespace pl
{
ObPLPackageGuard::~ObPLPackageGuard()
{
  if (map_.created()) {
    FOREACH(it, map_) {
      if (OB_ISNULL(it->second)) {
      } else {
        it->second->~ObCacheObjGuard();
      }
    }
    map_.destroy();
  }
}

int ObPLPackageGuard::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.create(
        common::hash::cal_next_prime(256),
        common::ObModIds::OB_HASH_BUCKET,
        common::ObModIds::OB_HASH_NODE))) {
    LOG_WARN("failed to create package guard map!", K(ret));
  }
  return ret;
}

} // end namespace pl
} // end namespace oceanbase
