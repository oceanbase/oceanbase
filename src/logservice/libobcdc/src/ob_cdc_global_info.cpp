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
