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

#include "ob_trans_version_mgr.h"
#include "common/ob_clock_generator.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/profile/ob_perf_event.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace transaction
{

void ObTransVersionMgr::destroy()
{
  TRANS_LOG(INFO, "ObTransVersionMgr destroyed");
}

void ObTransVersionMgr::reset()
{
  publish_version_ = 0;
  local_trans_version_ = 0;
}

int ObTransVersionMgr::get_and_update_local_trans_version(int64_t &local_trans_version)
{
  int ret = OB_SUCCESS;

  const int64_t cur_ts = ObClockGenerator::getClock();
  const int64_t tmp_trans_version = ATOMIC_LOAD(&local_trans_version_) + 1;
  const int64_t ret_trans_version = (cur_ts >= tmp_trans_version ? cur_ts : tmp_trans_version);
  if (OB_FAIL(update_local_trans_version_(ret_trans_version))) {
    TRANS_LOG(WARN, "update local transaction version error", KR(ret), K(ret_trans_version));
  } else if (OB_FAIL(update_publish_version_(ret_trans_version))) {
    TRANS_LOG(WARN, "update publish version error", KR(ret), K(ret_trans_version));
  } else {
    local_trans_version = ret_trans_version;
  }

  return ret;
}

int ObTransVersionMgr::get_local_trans_version(int64_t &local_trans_version)
{
  int ret = OB_SUCCESS;
  local_trans_version = ATOMIC_LOAD(&local_trans_version_);
  return ret;
}

int ObTransVersionMgr::update_local_trans_version(const int64_t local_trans_version)
{
  int ret = OB_SUCCESS;

  if (!ObTransVersion::is_valid(local_trans_version)) {
    TRANS_LOG(WARN, "invalid argument", K(local_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(update_local_trans_version_(local_trans_version))) {
    TRANS_LOG(WARN, "update local transaction version error", KR(ret), K(local_trans_version));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransVersionMgr::get_publish_version(int64_t &publish_version)
{
  int ret = OB_SUCCESS;
  publish_version = ATOMIC_LOAD(&publish_version_);
  return ret;
}

int ObTransVersionMgr::update_publish_version(const int64_t publish_version)
{
  int ret = OB_SUCCESS;

  if (!ObTransVersion::is_valid(publish_version)) {
    TRANS_LOG(WARN, "invalid arugment", K(publish_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(update_local_trans_version_(publish_version))) {
    TRANS_LOG(WARN, "update local transaction version error", KR(ret), K(publish_version));
  } else if (OB_FAIL(update_publish_version_(publish_version))) {
    TRANS_LOG(WARN, "update publish transaction version error", KR(ret), K(publish_version));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransVersionMgr::update_publish_version_(const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  int64_t cur_publish_version = ATOMIC_LOAD(&publish_version_);

  while (bool_ret && cur_publish_version < publish_version) {
    if (ATOMIC_BCAS(&publish_version_, cur_publish_version, publish_version)) {
      bool_ret = false;
    } else {
      cur_publish_version = ATOMIC_LOAD(&publish_version_);
    }
  }

  return ret;
}

int ObTransVersionMgr::update_local_trans_version_(const int64_t local_trans_version)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  int64_t tmp_trans_version = ATOMIC_LOAD(&local_trans_version_);

  while (bool_ret && tmp_trans_version < local_trans_version) {
    if (ATOMIC_BCAS(&local_trans_version_, tmp_trans_version, local_trans_version)) {
      bool_ret = false;
    } else {
      tmp_trans_version = ATOMIC_LOAD(&local_trans_version_);
    }
  }

  return ret;
}

ObTransVersionMgr &ObTransVersionMgr::get_instance()
{
  static ObTransVersionMgr trans_version_mgr;
  return trans_version_mgr;
}

} // transaction
} // oceanbase
