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

#define USING_LOG_PREFIX SHARE

#include "share/ob_unit_stat_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_server_struct.h" //GCTX

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
namespace share
{
ObUnitStatTableOperator::ObUnitStatTableOperator()
  : inited_(false), check_stop_provider_(NULL)
{
}

ObUnitStatTableOperator::~ObUnitStatTableOperator()
{
}

int ObUnitStatTableOperator::init(share::ObCheckStopProvider &check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    check_stop_provider_ = &check_stop_provider;
    inited_ = true;
  }
  return ret;
}

int ObUnitStatTableOperator::get_unit_stat(uint64_t tenant_id,
                                           uint64_t unit_id,
                                           ObUnitStat &unit_stat) const
{
  UNUSEDx(tenant_id, unit_id, unit_stat);
  // TODO: @wanhong.wwh
  return 0;
}

int ObUnitStatTableOperator::get_unit_stat(uint64_t tenant_id,
                                           share::ObUnitStatMap &unit_stat_map) const
{
  UNUSEDx(tenant_id, unit_stat_map);
  // TODO: @wanhong.wwh
  return 0;
}


}//end namespace share
}//end namespace oceanbase
