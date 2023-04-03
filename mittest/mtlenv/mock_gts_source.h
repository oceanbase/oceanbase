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

#ifndef OCEANBASE_TRANSACTION_OB_LTS_SOURCE_
#define OCEANBASE_TRANSACTION_OB_LTS_SOURCE_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/tx/ob_trans_version_mgr.h"

#define OB_TRANS_VERSION (ObTransVersionMgr::get_instance())

namespace oceanbase
{
namespace transaction
{
class MockObGtsSource
{
public:
  MockObGtsSource() {}
  ~MockObGtsSource() {}
public:
  int update_gts(const int64_t gts, bool &update)
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
  int update_local_trans_version(const int64_t version, bool &update)
  {
    return update_gts(version, update);
  }
  int get_gts(const MonotonicTs stc, ObTsCbTask *task, share::SCN &gts_scn, MonotonicTs &receive_gts_ts)
  {
    UNUSED(receive_gts_ts);
    int ret = OB_SUCCESS;
    int64_t gts = 0;
    if (!stc.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(stc), KP(task));
    } else if (OB_FAIL(OB_TRANS_VERSION.get_and_update_local_trans_version(gts))) {
      TRANS_LOG(WARN, "get local trans version failed", KR(ret));
    } else if (OB_FAIL(gts_scn.convert_for_logservice(gts))) {
      TRANS_LOG(WARN, "failed to convert", K(gts), KR(ret));
    } else {
      // do nothing
    }
    return ret;
  }
  int get_gts(ObTsCbTask *task, share::SCN &gts_scn)
  {
    UNUSED(task);
    int ret = OB_SUCCESS;
    int64_t gts = 0;
    if (OB_FAIL(OB_TRANS_VERSION.get_and_update_local_trans_version(gts))) {
      TRANS_LOG(WARN, "get local trans version failed", KR(ret));
    } else if (OB_FAIL(gts_scn.convert_for_logservice(gts))) {
      TRANS_LOG(WARN, "failed to convert", K(gts), KR(ret));
    }
    return ret;
  }
  /* int get_local_trans_version(const MonotonicTs stc, ObTsCbTask *task, int64_t &version, MonotonicTs &receive_gts_ts)
  {
    return get_gts(stc, task, version, receive_gts_ts);
  }
  int get_local_trans_version(ObTsCbTask *task, int64_t &version)
  {
    return get_gts(task, version);
  } */
  int wait_gts_elapse(const share::SCN &scn, ObTsCbTask *task, bool &need_wait)
  {
    int ret = OB_SUCCESS;
    if (!scn.is_valid() || NULL == task) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(scn), KP(task));
    } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(scn.get_val_for_logservice()))) {
      TRANS_LOG(WARN, "update publish version failed", KR(ret), K(scn));
    } else {
      need_wait = false;
    }
    return ret;
  }

  int wait_gts_elapse(const share::SCN &gts)
  {
    int ret = OB_SUCCESS;
    if (!gts.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(gts));
    } else if (OB_FAIL(OB_TRANS_VERSION.update_publish_version(gts.get_val_for_logservice()))) {
      TRANS_LOG(WARN, "update publish version failed", KR(ret), K(gts));
    } else {
      // do nothing
    }
    return ret;
  }
  int refresh_gts(const bool need_refresh)
  {
    // do nothing
    UNUSED(need_refresh);
    return OB_SUCCESS;
  }
  int update_base_ts(const int64_t base_ts)
  {
    int ret = OB_SUCCESS;
    if (0 > base_ts) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(base_ts));
    } else if (OB_FAIL(OB_TRANS_VERSION.update_local_trans_version(base_ts))) {
      TRANS_LOG(WARN, "update local trans version failed", KR(ret), K(base_ts));
    } else {
      TRANS_LOG(INFO, "update base ts success", K(base_ts));
    }
    return ret;
  }
  int get_base_ts(int64_t &base_ts)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(OB_TRANS_VERSION.get_local_trans_version(base_ts))) {
      TRANS_LOG(WARN, "get local trans version failed", KR(ret));
    } else {
      // do nothing
    }
    return ret;
  }
  bool is_external_consistent() { return false; }
public:
  TO_STRING_KV("ts_source", "Mock_GTS");
};
}
}//end of namespace oceanbase

#endif //OCEANBASE_TRANSACTION_OB_LTS_SOURCE_
