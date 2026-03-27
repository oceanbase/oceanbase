/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_batch_common.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;

int ObTableBatchCtx::check_legality()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_param_) || OB_ISNULL(credential_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null input", K(ret), KP_(trans_param), KP_(credential));
  } else if (tablet_ids_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ids is empty", K(ret));
  }

  return ret;
}
