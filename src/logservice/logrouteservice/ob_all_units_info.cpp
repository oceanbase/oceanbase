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

#define USING_LOG_PREFIX OBLOG
#include "share/ob_errno.h"                                     // KR
#include "ob_all_units_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace logservice
{
int ObUnitsRecord::init(
    const common::ObAddr &server,
    ObString &zone,
    common::ObZoneType &zone_type,
    ObString &region)
{
  int ret = OB_SUCCESS;
  server_ = server;
  zone_type_ = zone_type;

  if (OB_FAIL(zone_.assign(zone))) {
    LOG_ERROR("zone assign fail", KR(ret), K(zone));
  } else if (OB_FAIL(region_.assign(region))) {
    LOG_ERROR("zone assign fail", KR(ret), K(region));
  } else {}

  return ret;
}

void ObUnitsRecordInfo::reset()
{
  cluster_id_ = 0;
  units_record_array_.reset();
}

int ObUnitsRecordInfo::init(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  cluster_id_ = cluster_id;
  units_record_array_.reset();

  return ret;
}

int ObUnitsRecordInfo::add(ObUnitsRecord &record)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(units_record_array_.push_back(record))) {
    LOG_ERROR("units_record_array_ push_back failed", KR(ret), K(record));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase
