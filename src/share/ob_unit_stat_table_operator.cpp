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
#include "share/partition_table/ob_partition_table_iterator.h"
#include "observer/ob_server_struct.h"  //GCTX

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
namespace share {
ObUnitStatTableOperator::ObUnitStatTableOperator()
    : inited_(false), pt_operator_(NULL), schema_service_(NULL), check_stop_provider_(NULL)
{}

ObUnitStatTableOperator::~ObUnitStatTableOperator()
{}

int ObUnitStatTableOperator::init(share::ObPartitionTableOperator& pt_operator,
    share::schema::ObMultiVersionSchemaService& schema_service, share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    check_stop_provider_ = &check_stop_provider;
    inited_ = true;
  }
  return ret;
}

int ObUnitStatTableOperator::get_unit_stat(uint64_t tenant_id, uint64_t unit_id, ObUnitStat& unit_stat) const
{
  int ret = OB_SUCCESS;
  ObTenantPartitionIterator tenant_partition_iter;
  const bool ignore_row_checksum = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == schema_service_ || nullptr == pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service or pt operator is null", K(ret));
  } else if (OB_FAIL(tenant_partition_iter.init(*pt_operator_, *schema_service_, tenant_id, ignore_row_checksum))) {
    LOG_WARN("fail to init tenant partition iter", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else {
    unit_stat.unit_id_ = unit_id;
    ObPartitionInfo info;
    while (OB_SUCC(ret) && OB_SUCC(tenant_partition_iter.next(info))) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else {
        FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCC(ret))
        {
          if (OB_ISNULL(r)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid replica", K(ret), K(info));
          } else if (!r->is_in_service() && !r->is_flag_replica()) {
            // nothing todo
          } else if (r->unit_id_ == unit_id) {
            unit_stat.partition_cnt_++;
            if (ObReplicaTypeCheck::is_replica_with_ssstore(r->replica_type_)) {
              unit_stat.required_size_ += r->required_size_;
            }
            // more info add here if needed
            break;
          }
        }
      }
      info.reuse();
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("iterator partition failed", K(ret));
    }
  }
  return ret;
}

int ObUnitStatTableOperator::get_unit_stat(uint64_t tenant_id, share::ObUnitStatMap& unit_stat_map) const
{
  int ret = OB_SUCCESS;
  ObTenantPartitionIterator tenant_partition_iter;
  const bool ignore_row_checksum = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == schema_service_ || nullptr == pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service or pt operator is null", K(ret));
  } else if (OB_FAIL(tenant_partition_iter.init(*pt_operator_, *schema_service_, tenant_id, ignore_row_checksum))) {
    LOG_WARN("fail to init tenant partition iter", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("balancer stop", K(ret));
  } else {
    ObPartitionInfo info;
    ObUnitStatMap::Item* unit_stat = NULL;
    while (OB_SUCC(ret) && OB_SUCC(tenant_partition_iter.next(info))) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else {
        FOREACH_CNT_X(r, info.get_replicas_v2(), OB_SUCC(ret))
        {
          if (OB_ISNULL(r)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid replica", K(ret), K(info));
          } else if (!r->is_in_service() && !r->is_flag_replica()) {
            // nothing todo
          } else if (OB_FAIL(unit_stat_map.locate(r->unit_id_, unit_stat))) {
            LOG_WARN("fail locate unit", "unit_id", r->unit_id_, K(ret));
          } else {
            unit_stat->v_.partition_cnt_++;
            if (ObReplicaTypeCheck::is_replica_with_ssstore(r->replica_type_)) {
              unit_stat->v_.required_size_ += r->required_size_;
            }
          }
        }
      }
      info.reuse();
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("iterator partition failed", K(ret));
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
