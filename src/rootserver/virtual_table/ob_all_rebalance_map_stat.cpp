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
#include "ob_all_rebalance_map_stat.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_balance_group_container.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::rootserver::balancer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllRebalanceMapStat::ObAllRebalanceMapStat()
    : inited_(false),
      schema_service_(NULL),
      tenant_balance_stat_(),
      all_tenants_(),
      tenant_maps_(),
      cur_tenant_idx_(-1),
      cur_map_idx_(-1),
      schema_guard_(),
      table_schema_(NULL),
      columns_()
{}

ObAllRebalanceMapStat::~ObAllRebalanceMapStat()
{}

int ObAllRebalanceMapStat::init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllRebalanceMapStat already inited", K(ret));
  } else if (OB_FAIL(tenant_balance_stat_.init(unit_mgr,
                 server_mgr,
                 pt_operator,
                 remote_pt_operator,
                 zone_mgr,
                 task_mgr,
                 check_stop_provider,
                 unit_mgr.get_sql_proxy()))) {
    LOG_WARN("failed to init tenant_balance_stat", K(ret));
  } else if (OB_FAIL(schema_service.get_schema_guard(schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObAllRebalanceMapStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tenant_balance_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(get_all_tenant())) {
    LOG_WARN("fail to get all maps", K(ret));
  } else if (OB_FAIL(get_table_schema(OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TID))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    cur_tenant_idx_ = -1;
    cur_map_idx_ = -1;
  }
  return ret;
}

int ObAllRebalanceMapStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
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

int ObAllRebalanceMapStat::next()
{
  int ret = OB_SUCCESS;
  bool loop = false;
  do {
    loop = false;
    ++cur_map_idx_;
    if (cur_map_idx_ >= tenant_maps_.count()) {
      ++cur_tenant_idx_;
      if (cur_tenant_idx_ >= all_tenants_.count()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(init_tenant_balance_stat(all_tenants_.at(cur_tenant_idx_)))) {
        LOG_WARN("fail init tenant balance stat", K_(cur_tenant_idx), K_(cur_map_idx), K(ret));
      } else {
        cur_map_idx_ = -1;
        loop = true;
      }
    }
  } while (loop);
  return ret;
}

int ObAllRebalanceMapStat::get_row()
{
  int ret = OB_SUCCESS;
  columns_.reuse();
  if (OB_FAIL(get_full_row(table_schema_, columns_))) {
    LOG_WARN("fail to get full row", K(ret));
  } else if (OB_FAIL(project_row(columns_, cur_row_))) {
    LOG_WARN("fail to project row", K_(columns), K(ret));
  }
  return ret;
}

int ObAllRebalanceMapStat::get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (cur_map_idx_ >= tenant_maps_.count() || cur_map_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected branch", K_(cur_map_idx), K(ret));
  } else {
    const balancer::SquareIdMap& map = *tenant_maps_.at(cur_map_idx_);
    ObArray<const ObPartitionSchema*> entity_schemas;
    int64_t item_cnt = 0;
    if (balancer::SHARD_GROUP_BALANCE_GROUP == map.get_map_type() ||
        balancer::SHARD_PARTITION_BALANCE_GROUP == map.get_map_type()) {
      FOREACH_X(item, map, OB_SUCC(ret))
      {
        int64_t tg_idx = item->all_tg_idx_;
        uint64_t tenant_id = map.get_tenant_id();
        if (tenant_id != tenant_balance_stat_.get_tenant_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant id not match", K(tenant_id), "other", tenant_balance_stat_.get_tenant_id(), K(ret));
        } else if (tg_idx < 0 || tg_idx >= tenant_balance_stat_.all_tg_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tg_idx value", K(tg_idx), "max", tenant_balance_stat_.all_tg_.count(), K(ret));
        } else if (OB_FAIL(StatFinderUtil::get_partition_entity_schemas_by_tg_idx(
                       tenant_balance_stat_, schema_guard_, tenant_id, tg_idx, entity_schemas))) {
          LOG_WARN("fail get tables schemas for tg", K(tenant_id), K(tg_idx));
        } else {
          if (++item_cnt >= map.get_col_size()) {
            break;  // only need first row
          }
        }
      }
    } else {
      FOREACH_X(item, map, OB_SUCC(ret))
      {
        int64_t tg_idx = item->all_tg_idx_;
        uint64_t tenant_id = map.get_tenant_id();
        if (tenant_id != tenant_balance_stat_.get_tenant_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant id not match", K(tenant_id), "other", tenant_balance_stat_.get_tenant_id(), K(ret));
        } else if (tg_idx < 0 || tg_idx >= tenant_balance_stat_.all_tg_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected tg_idx value", K(tg_idx), "max", tenant_balance_stat_.all_tg_.count(), K(ret));
        } else if (OB_FAIL(StatFinderUtil::get_partition_entity_schemas_by_tg_idx(
                       tenant_balance_stat_, schema_guard_, tenant_id, tg_idx, entity_schemas))) {
          LOG_WARN("fail get tables schemas for tg", K(tenant_id), K(tg_idx));
        } else {
          break;  // only need first item
        }
      }
    }

    // max 64K per column
    char* buf = NULL;
    const int64_t buf_size = 64 * 1024;
    int64_t pos = 0;
    if (OB_SUCC(ret)) {
      buf = static_cast<char*>(allocator_.alloc(buf_size));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory fail", K(ret));
      } else {
        FOREACH_X(it, entity_schemas, OB_SUCC(ret))
        {
          const ObPartitionSchema* t = *it;
          if (OB_ISNULL(t)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", KP(t), K(ret));
          } else {
            int64_t write = snprintf(buf + pos, buf_size - pos, "%s ", t->get_entity_name());
            if (write >= buf_size - pos) {
              break;  // overflow, truncate
            } else {
              pos += write;
            }
          }
        }
      }
    }

    ADD_COLUMN(set_int, table, "tenant_id", map.get_tenant_id(), columns);
    ADD_COLUMN(set_int, table, "map_type", map.get_map_type(), columns);
    ADD_COLUMN(set_bool, table, "is_valid", map.is_valid(), columns);
    ADD_COLUMN(set_int, table, "row_size", map.get_row_size(), columns);
    ADD_COLUMN(set_int, table, "col_size", map.get_col_size(), columns);
    ADD_COLUMN(set_varchar, table, "tables", ObString(buf), columns);
  }
  return ret;
}

int ObAllRebalanceMapStat::get_all_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllRebalanceMapStat not init", K(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_tenant_ids(all_tenants_))) {
    LOG_WARN("fail to get_tenant_ids", K(ret));
  }
  return ret;
}

int ObAllRebalanceMapStat::init_tenant_balance_stat(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret));
  } else {
    tenant_balance_stat_.reuse();
    tenant_maps_.reuse();
    TenantSchemaGetter stat_finder(tenant_id);
    ObLeaderBalanceGroupContainer balance_group_container(schema_guard_, stat_finder, allocator_);
    if (OB_FAIL(balance_group_container.init(tenant_id))) {
      LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
    } else if (OB_FAIL(balance_group_container.build())) {
      LOG_WARN("fail to build balance index builder", K(ret));
    } else if (OB_FAIL(tenant_balance_stat_.gather_stat(
                   tenant_id, &schema_guard_, balance_group_container.get_hash_index()))) {
      LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
    } else {
      // do it
      balancer::ITenantStatFinder& stat_finder = tenant_balance_stat_;
      balancer::ObPartitionBalanceGroupContainer container(schema_guard_, stat_finder, allocator_);
      if (OB_FAIL(container.init(tenant_id))) {
        LOG_WARN("fail to init", K(ret));
      } else if (OB_FAIL(container.build())) {
        LOG_WARN("fail to build", K(ret));
      } else if (OB_FAIL(tenant_maps_.assign(container.get_square_id_map_array()))) {
        LOG_WARN("fail to assign tenant maps", K(ret));
      }
    }
  }
  return ret;
}

int ObAllRebalanceMapStat::get_table_schema(uint64_t tid)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, tid);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllRebalanceTenantStat not init", K(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_table_schema(table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", K(table_id), K(ret));
  } else if (NULL == table_schema_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret));
  }
  return ret;
}
