/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_global_info.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace libobcdc
{
ObCDCGlobalInfo::ObCDCGlobalInfo() :
  lob_aux_table_schema_info_(),
  min_cluster_version_(0)
{
}

int ObCDCGlobalInfo::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lob_aux_table_schema_info_.init())) {
    LOG_ERROR("table_schema_ init failed", KR(ret));
  } else {}

  return ret;
}

void ObCDCGlobalInfo::reset()
{
  lob_aux_table_schema_info_.reset();
  min_cluster_version_ = 0;
}

} // namespace libobcdc
} // namespace oceanbase
