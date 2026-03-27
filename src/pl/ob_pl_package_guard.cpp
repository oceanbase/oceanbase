/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PL

#include "ob_pl_package_guard.h"
#include "src/pl/ob_pl_package_state.h"

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
