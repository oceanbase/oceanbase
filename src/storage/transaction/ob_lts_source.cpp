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

#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "ob_lts_source.h"
#include "ob_trans_version_mgr.h"

#define OB_TRANS_VERSION (ObTransVersionMgr::get_instance())

namespace oceanbase {
using namespace common;

namespace transaction {

int ObLtsSource::update_gts(const int64_t gts, bool& update)
{
  int ret = OB_SUCCESS;
  if (0 >= gts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(gts))) {
    TRANS_LOG(WARN, "update publish version failed", K(ret), K(gts));
  } else {
    update = false;
  }
  return ret;
}

int ObLtsSource::update_local_trans_version(const int64_t version, bool& update)
{
  return update_gts(version, update);
}

int ObLtsSource::get_gts(ObTsCbTask* task, int64_t& gts)
{
  UNUSED(task);
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_TRANS_VERSION.get_and_update_local_trans_version(gts))) {
    TRANS_LOG(WARN, "get local trans version failed", KR(ret));
  }
  return ret;
}

int ObLtsSource::get_gts(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts)
{
  UNUSED(receive_gts_ts);
  int ret = OB_SUCCESS;
  if (!stc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(stc), KP(task));
  } else if (OB_FAIL(OB_TRANS_VERSION.get_and_update_local_trans_version(gts))) {
    TRANS_LOG(WARN, "get local trans version failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLtsSource::get_local_trans_version(ObTsCbTask* task, int64_t& version)
{
  return get_gts(task, version);
}

int ObLtsSource::get_local_trans_version(
    const MonotonicTs stc, ObTsCbTask* task, int64_t& version, MonotonicTs& receive_gts_ts)
{
  return get_gts(stc, task, version, receive_gts_ts);
}

int ObLtsSource::wait_gts_elapse(const int64_t gts, ObTsCbTask* task, bool& need_wait)
{
  int ret = OB_SUCCESS;
  if (0 >= gts || NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts), KP(task));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(gts))) {
    TRANS_LOG(WARN, "update publish version failed", KR(ret), K(gts));
  } else {
    need_wait = false;
  }
  return ret;
}

int ObLtsSource::wait_gts_elapse(const int64_t gts)
{
  int ret = OB_SUCCESS;
  if (0 >= gts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(gts))) {
    TRANS_LOG(WARN, "update publish version failed", KR(ret), K(gts));
  } else {
    // do nothing
  }
  return ret;
}

int ObLtsSource::refresh_gts(const bool need_refresh)
{
  // do nothing
  UNUSED(need_refresh);
  return OB_SUCCESS;
}

int ObLtsSource::update_base_ts(const int64_t base_ts, const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  if (0 > base_ts || 0 > publish_version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(base_ts), K(publish_version));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_local_trans_version(base_ts))) {
    TRANS_LOG(WARN, "update local trans version failed", KR(ret), K(base_ts));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(publish_version))) {
    TRANS_LOG(WARN, "update publish version failed", KR(ret), K(publish_version));
  } else {
    TRANS_LOG(INFO, "update base ts success", K(base_ts), K(publish_version));
  }
  return ret;
}

int ObLtsSource::get_base_ts(int64_t& base_ts, int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_TRANS_VERSION.get_local_trans_version(base_ts))) {
    TRANS_LOG(WARN, "get local trans version failed", KR(ret));
  } else if (OB_FAIL(OB_TRANS_VERSION.get_publish_version(publish_version))) {
  } else {
    // do nothing
  }
  return ret;
}

int ObLtsSource::update_publish_version(const int64_t publish_version)
{
  int ret = OB_SUCCESS;
  if (0 > publish_version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(publish_version));
  } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(publish_version))) {
    TRANS_LOG(WARN, "update publish version failed", KR(ret), K(publish_version));
  } else {
  }
  return ret;
}

int ObLtsSource::get_publish_version(int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_publish_version;
  if (OB_FAIL(OB_TRANS_VERSION.get_publish_version(tmp_publish_version))) {
    TRANS_LOG(WARN, "get publish version failed", KR(ret));
  } else {
    // In the scenario of unlocking in advance,
    // 10us is pushed up here to increase the probability of
    // predecessor dependence and improve the performance of elr
    // However, when the elr scenario is closed,
    // the probability of waiting for the end of the row lock transaction will increase
    publish_version = tmp_publish_version + 10;
    if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(publish_version))) {
      TRANS_LOG(WARN, "update local trans version failed", K(ret), K(publish_version));
    }
    TRANS_LOG(DEBUG, "get publish version from lts", K(publish_version));
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
