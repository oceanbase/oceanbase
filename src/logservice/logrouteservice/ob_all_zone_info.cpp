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
#include "ob_all_zone_info.h"
#include "share/ob_errno.h"                                     // KR

namespace oceanbase
{
namespace logservice
{
int AllZoneRecord::init(ObString &zone,
    ObString &region)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(zone_.assign(zone))) {
    LOG_ERROR("zone assign fail", KR(ret), K(zone));
  } else if (OB_FAIL(region_.assign(region))) {
    LOG_ERROR("zone assign fail", KR(ret), K(region));
  } else {}

  return ret;
}

int AllZoneTypeRecord::init(ObString &zone,
    common::ObZoneType &zone_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(zone_.assign(zone))) {
    LOG_ERROR("zone assign fail", KR(ret), K(zone));
  } else {
    zone_type_ = zone_type;
  }

  return ret;
}

void ObAllZoneInfo::reset()
{
  cluster_id_ = 0;
  all_zone_array_.reset();
}

int ObAllZoneInfo::init(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  cluster_id_ = cluster_id;
  all_zone_array_.reset();

  return ret;
}

int ObAllZoneInfo::add(AllZoneRecord &record)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(all_zone_array_.push_back(record))) {
    LOG_ERROR("all_zone_array_ push_back failed", KR(ret), K(record));
  }

  return ret;
}

void ObAllZoneTypeInfo::reset()
{
  cluster_id_ = 0;
  all_zone_type_array_.reset();
}

int ObAllZoneTypeInfo::init(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  cluster_id_ = cluster_id;
  all_zone_type_array_.reset();

  return ret;
}

int ObAllZoneTypeInfo::add(AllZoneTypeRecord &record)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(all_zone_type_array_.push_back(record))) {
    LOG_ERROR("all_zone_type_array_ push_back failed", KR(ret), K(record));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

