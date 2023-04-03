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

#include "ob_reporter_adapter.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace logservice
{

ObLogReporterAdapter::ObLogReporterAdapter() :
    is_inited_(false),
    rs_reporter_(NULL)
  {}

ObLogReporterAdapter::~ObLogReporterAdapter()
{
  destroy();
}

int ObLogReporterAdapter::init(observer::ObIMetaReport *reporter)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "ObLogReporterAdapter init twice", K(ret));
  } else if (OB_ISNULL(reporter)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(reporter), K(ret));
  } else {
    rs_reporter_ = reporter;
    is_inited_ = true;
    PALF_LOG(INFO, "ObLogReporterAdapter init success", K(ret), KP(rs_reporter_));
  }
  return ret;
}

void ObLogReporterAdapter::destroy()
{
  is_inited_ = false;
  rs_reporter_ = NULL;
}

int ObLogReporterAdapter::report_replica_info(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID id(palf_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "ObLogReporterAdapter is not inited", K(ret));
  } else if (OB_FAIL(rs_reporter_->submit_ls_update_task(tenant_id, id))) {
    PALF_LOG(WARN, "report ls info failed", K(ret), K(tenant_id), K(palf_id));
  } else {
    PALF_LOG(INFO, "submit_ls_update_task success", K(tenant_id), K(palf_id));
    // do nothing.
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase