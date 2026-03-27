/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#include "observer/report/ob_i_meta_report.h"

namespace oceanbase
{
namespace storage
{

class MockObMetaReport : public observer::ObIMetaReport
{
public:
  MOCK_METHOD2(submit_ls_update_task, int(const uint64_t tenant_id,
                                                  const share::ObLSID &ls_id));
};

}
}
