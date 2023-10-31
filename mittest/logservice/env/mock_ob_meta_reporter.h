/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_MITTEST_MOCK_OB_META_REPORTER_
#define OCEANBASE_MITTEST_MOCK_OB_META_REPORTER_


#include "observer/report/ob_i_meta_report.h"


namespace oceanbase
{
using namespace observer;
namespace unittest
{
class MockMetaReporter : public ObIMetaReport
{
public:
  MockMetaReporter() { }
  ~MockMetaReporter() { }
  int submit_ls_update_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id)
  {
    UNUSEDx(tenant_id, ls_id);
    return OB_SUCCESS;
  }
  int submit_tablet_checksums_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id)
  {
    UNUSEDx(tenant_id, ls_id, tablet_id);
    return OB_SUCCESS;
  }
};
}// storage
}// oceanbase
#endif