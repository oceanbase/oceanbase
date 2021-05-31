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

#define USING_LOG_PREFIX RS
#include "ob_all_rebalance_unit_stat.h"
#include "share/schema/ob_multi_version_schema_service.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceUnitStat::ObAllRebalanceUnitStat() : cur_unit_idx_(-1)
{}

ObAllRebalanceUnitStat::~ObAllRebalanceUnitStat()
{}

int ObAllRebalanceUnitStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  return impl_.init(
      schema_service, unit_mgr, server_mgr, pt_operator, remote_pt_operator, zone_mgr, task_mgr, check_stop_provider);
}

int ObAllRebalanceUnitStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(impl_.tenant_balance_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(impl_.get_all_tenant())) {
  } else if (OB_FAIL(impl_.get_table_schema(OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID))) {
  } else {
    impl_.cur_tenant_idx_ = -1;
    impl_.cur_zone_idx_ = -1;
    cur_unit_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceUnitStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!impl_.inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(next())) {
    } else if (OB_FAIL(get_row())) {
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllRebalanceUnitStat::next()
{
  int ret = OB_SUCCESS;
  bool loop = false;
  do {
    loop = false;
    ++cur_unit_idx_;  // next unit
    if (impl_.cur_zone_idx_ < 0) {
      impl_.cur_zone_idx_ = 0;  // first zone
    }
    if (impl_.cur_zone_idx_ >= impl_.tenant_balance_stat_.all_zone_unit_.count()) {  // end of zone
      ++impl_.cur_tenant_idx_;                                                       // next tenant
      if (impl_.cur_tenant_idx_ >= impl_.all_tenants_.count()) {
        ret = OB_ITER_END;
      } else {
        if (OB_FAIL(impl_.init_tenant_balance_stat(impl_.all_tenants_.at(impl_.cur_tenant_idx_)))) {
        } else {
          impl_.cur_zone_idx_ = -1;
          cur_unit_idx_ = -1;
          loop = true;
        }
      }
    } else {  // valid zone
      const ZoneUnit& zu = impl_.tenant_balance_stat_.all_zone_unit_.at(impl_.cur_zone_idx_);
      if (cur_unit_idx_ >= zu.all_unit_.count()) {  // end of unit
        impl_.cur_zone_idx_++;                      // next zone
        cur_unit_idx_ = -1;
        loop = true;
      }
    }
  } while (loop);
  return ret;
}

int ObAllRebalanceUnitStat::get_row()
{
  int ret = OB_SUCCESS;
  impl_.columns_.reuse();
  if (OB_FAIL(get_full_row(impl_.table_schema_, impl_.columns_))) {
    LOG_WARN("fail to get full row", K(ret));
  } else if (OB_FAIL(project_row(impl_.columns_, cur_row_))) {
    LOG_WARN("fail to project row", K_(impl_.columns), K(ret));
  }
  return ret;
}

int ObAllRebalanceUnitStat::get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (impl_.cur_zone_idx_ >= impl_.tenant_balance_stat_.all_zone_unit_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch");
  } else if (cur_unit_idx_ >= impl_.tenant_balance_stat_.all_zone_unit_.at(impl_.cur_zone_idx_).all_unit_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch");
  } else {
    const ZoneUnit& zu = impl_.tenant_balance_stat_.all_zone_unit_.at(impl_.cur_zone_idx_);
    const UnitStat* unit = zu.all_unit_.at(cur_unit_idx_);
    // primary key
    ADD_COLUMN(set_int, table, "tenant_id", impl_.tenant_balance_stat_.tenant_id_, columns);
    ADD_COLUMN(set_varchar, table, "zone", zu.zone_.str(), columns);
    ADD_COLUMN(set_int, table, "unit_id", unit->info_.unit_.unit_id_, columns);
    // other columns
    ADD_COLUMN(set_double, table, "load", unit->load_, columns);
    ADD_COLUMN(set_double, table, "cpu_usage_rate", unit->get_cpu_usage_rate(), columns);
    ADD_COLUMN(set_double, table, "disk_usage_rate", unit->get_disk_usage_rate(), columns);
    ADD_COLUMN(set_double, table, "iops_usage_rate", unit->get_iops_usage_rate(), columns);
    ADD_COLUMN(set_double, table, "memory_usage_rate", unit->get_memory_usage_rate(), columns);
  }
  return ret;
}
