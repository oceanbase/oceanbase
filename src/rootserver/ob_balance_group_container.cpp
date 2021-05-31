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

#define USING_LOG_PREFIX RS_LB

#include "ob_balance_group_container.h"
#include "ob_balance_group_analyzer.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_primary_zone_util.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::rootserver::balancer;

int PartitionSchemaChecker::check_same_primary_zone(
    const common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas,
    share::schema::ObSchemaGetterGuard& schema_guard, bool& same)
{
  int ret = OB_SUCCESS;
  const share::schema::ObPartitionSchema* sample = nullptr;
  if (OB_UNLIKELY(partition_schemas.count()) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (nullptr == (sample = partition_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sample ptr is null", K(ret));
  } else {
    same = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_schemas.count() && same; ++i) {
      const share::schema::ObPartitionSchema* ref = partition_schemas.at(i);
      if (nullptr == ref) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref ptr is null", K(ret));
      } else if (OB_FAIL(check_same_primary_zone(*sample, *ref, schema_guard, same))) {
        LOG_WARN("fail to check same primary zone", K(ret));
      }
    }
  }
  return ret;
}

int PartitionSchemaChecker::check_same_primary_zone(
    const common::ObIArray<const share::schema::ObTableSchema*>& table_schemas,
    share::schema::ObSchemaGetterGuard& schema_guard, bool& same)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema* sample = nullptr;
  if (OB_UNLIKELY(table_schemas.count()) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (nullptr == (sample = table_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sample ptr is null", K(ret));
  } else {
    same = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count() && same; ++i) {
      const share::schema::ObTableSchema* ref = table_schemas.at(i);
      if (nullptr == ref) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref ptr is null", K(ret));
      } else if (OB_FAIL(check_same_primary_zone(*sample, *ref, schema_guard, same))) {
        LOG_WARN("fail to check same primary zone", K(ret));
      }
    }
  }
  return ret;
}

int PartitionSchemaChecker::check_same_primary_zone(
    const ObPartitionSchema& ref, const ObPartitionSchema& other, ObSchemaGetterGuard& schema_guard, bool& same)
{
  int ret = OB_SUCCESS;
  same = true;
  if (ref.get_tenant_id() != other.get_tenant_id()) {
    same = false;
  } else if (OB_FAIL(ObPrimaryZoneUtil::check_primary_zone_equal(schema_guard, ref, other, same))) {
    LOG_WARN("fail to check primary zone equal", K(ret));
  }
  return ret;
}

/*
 * Because the process of adding or subtracting partitions will inevitably lead to changes
 * in the number of partitions in a ShardGroup,
 * it cannot be determined at this time that they do not belong to a ShardGroup.
 * For the range partition, this function ignores the part_num check
 * FIXME:Support non-templated secondary partition
 */
bool PartitionSchemaChecker::is_same_partition_schema(const ObTableSchema& ref, const ObTableSchema& other)
{
  bool same = false;
  if (!rootserver::ObTenantUtils::is_balance_target_schema(other)) {
    /* Ignore irregular tables in system tables */
    same = true;
  } else if (ref.get_part_level() == other.get_part_level()) {
    switch (ref.get_part_level()) {
      case PARTITION_LEVEL_ONE:
        same = (ref.get_part_option().get_part_func_type() == other.get_part_option().get_part_func_type());
        if (same && (ref.is_hash_part() || ref.is_key_part())) {
          // hash / key Partition requires equal number of parts
          same = (ref.get_part_option().get_part_num() == other.get_part_option().get_part_num());
        }
        // The partition column needs to be able to compare
        if (same) {
          same = (is_same_schema_partition_key_info(ref.get_partition_key_info(), other.get_partition_key_info()));
        }
        break;
      case PARTITION_LEVEL_TWO:
        same = (ref.get_part_option().get_part_func_type() == other.get_part_option().get_part_func_type()) &&
               (ref.get_sub_part_option().get_part_func_type() == other.get_sub_part_option().get_part_func_type());
        // hash / key Partition requires equal number of parts
        if (same && (ref.is_hash_part() || ref.is_key_part())) {
          same = (ref.get_part_option().get_part_num() == other.get_part_option().get_part_num());
        }
        // hash / key  sub Partition requires equal number of parts
        if (same && (ref.is_hash_subpart() || ref.is_key_subpart())) {
          same = (ref.get_sub_part_option().get_part_num() == other.get_sub_part_option().get_part_num());
        }
        if (same) {
          same = (is_same_schema_partition_key_info(ref.get_partition_key_info(), other.get_partition_key_info()));
        }
        // subpartition column needs to be able to compare
        if (same) {
          same =
              (is_same_schema_partition_key_info(ref.get_subpartition_key_info(), other.get_subpartition_key_info()));
        }

        break;
      default:
        same = true; /* LEVEL_ZERO */
        break;
    }
  }
  return same;
}

bool PartitionSchemaChecker::is_same_schema_partition_key_info(
    const common::ObRowkeyInfo& left, const common::ObRowkeyInfo& right)
{
  bool bool_ret = true;
  if (left.get_size() != right.get_size()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < left.get_size(); ++i) {
      const ObRowkeyColumn* left_c = left.get_column(i);
      const ObRowkeyColumn* right_c = right.get_column(i);
      if (nullptr == left_c || nullptr == right_c) {
        bool_ret = false;
      } else if (left_c->get_meta_type() != right_c->get_meta_type()) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

bool PartitionSchemaChecker::is_one_level_and_partition_by_range(
    common::ObIArray<const share::schema::ObTableSchema*>& schemas)
{
  bool bret = false;
  if (schemas.count() > 0 && NULL != schemas.at(0)) {
    const share::schema::ObTableSchema& schema = *schemas.at(0);
    bret = PARTITION_LEVEL_ONE == schema.get_part_level() && schema.get_part_option().is_range_part();
  }
  return bret;
}

int ShardGroupValidator::check_table_schemas_compatible(ITenantStatFinder& stat_finder,
    ObSchemaGetterGuard& schema_guard, const ObIArray<const ObTableSchema*>& tables, bool& compatible,
    const bool check_primary_zone)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = stat_finder.get_tenant_id();
  compatible = true;
  if (tables.count() < 2) {
    compatible = false;
  } else {
    // 1. Ensure that the tablegroup id of shard tables is different
    ObHashSet<uint64_t> tmp_tg_set;
    if (OB_FAIL(tmp_tg_set.create(tables.count()))) {
      LOG_WARN("fail create table id set", K(ret));
    }
    ARRAY_FOREACH_X(tables, idx, cnt, OB_SUCC(ret) && compatible)
    {
      uint64_t tablegroup_id = tables.at(idx)->get_tablegroup_id();
      if (OB_INVALID_ID == tablegroup_id) {
      } else if (OB_FAIL(DistributionUtil::check_and_set(tablegroup_id, tmp_tg_set))) {
        compatible = false;
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail mark tablegroup as processed", K(tablegroup_id), K(ret));
        }
      }
    }
    // 2. Ensure that the schema of each group of tablegroups is compatible,
    //    such as the compatibility of the partitioning method and the number of partitions
    const ObTableSchema* first_table_schema = tables.at(0);
    ARRAY_FOREACH_X(tables, idx, cnt, OB_SUCC(ret) && compatible)
    {
      const ObTableSchema* table_schema = tables.at(idx);
      uint64_t tablegroup_id = table_schema->get_tablegroup_id();
      bool same = false;
      if (OB_INVALID_ID != tablegroup_id) {
        ObArray<const ObTableSchema*> tg_tables;
        if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, tg_tables))) {
          LOG_WARN("get_table_schema_by_tg_id fail", K(tenant_id), K(tablegroup_id), K(ret));
        } else {
          ARRAY_FOREACH_X(tg_tables, tb_idx, tb_cnt, OB_SUCC(ret))
          {
            same = false;
            if (nullptr == tg_tables.at(tb_idx)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table schema ptr is null", K(ret));
            } else if (!tg_tables.at(tb_idx)->has_self_partition()) {
              // by pass
            } else if (!PartitionSchemaChecker::is_same_partition_schema(*first_table_schema, *tg_tables.at(tb_idx))) {
              compatible = false;
              LOG_INFO("all tables in sharding group should have compatible schema",
                  "part_num",
                  table_schema->get_all_part_num(),
                  "table_name",
                  table_schema->get_table_name_str(),
                  K(ret));
            } else if (!check_primary_zone) {
              // no need check primary zone
            } else if (OB_FAIL(PartitionSchemaChecker::check_same_primary_zone(
                           *first_table_schema, *tg_tables.at(tb_idx), schema_guard, same))) {
              LOG_WARN("fail to check same primary zone", K(ret));
            } else if (!same) {
              compatible = false;
              LOG_INFO("tables in sharding group have different primary zone",
                  "left_table",
                  first_table_schema->get_table_id(),
                  "right_table",
                  tg_tables.at(tb_idx)->get_table_id());
            }
          }
        }
      } else if (!PartitionSchemaChecker::is_same_partition_schema(*first_table_schema, *table_schema)) {
        compatible = false;
        LOG_INFO("all tables in sharding group should have compatible schema",
            "part_num",
            table_schema->get_all_part_num(),
            "table_name",
            table_schema->get_table_name_str(),
            K(ret));
      } else if (!check_primary_zone) {
        // no need check primary zone
      } else if (OB_FAIL(PartitionSchemaChecker::check_same_primary_zone(
                     *first_table_schema, *table_schema, schema_guard, same))) {
        LOG_WARN("fail to check same primary zone", K(ret));
      } else if (!same) {
        compatible = false;
        LOG_INFO("tables in sharding group have different primary zone",
            "left_table",
            first_table_schema->get_table_id(),
            "right_table",
            table_schema->get_table_id());
      }
    }
  }
  return ret;
}

int ObBalanceGroupContainer::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(processed_tids_.create(PROCESSED_TABLE_MAP_SIZE))) {
    LOG_WARN("fail to create map", K(ret));
  } else if (OB_FAIL(shardgroup_container_builder_.init(tenant_id))) {
    LOG_WARN("fail to init shardgroup container builder", K(ret), K(tenant_id));
  } else if (OB_FAIL(tablegroup_container_builder_.init(tenant_id))) {
    LOG_WARN("fail to init tablegroup container builder", K(ret));
  } else if (OB_FAIL(partition_container_builder_.init(tenant_id))) {
    LOG_WARN("fail to init partition table index builder", K(ret), K(tenant_id));
  } else if (OB_FAIL(non_partition_container_builder_.init(tenant_id))) {
    LOG_WARN("fail to init non partition table index builder", K(ret), K(tenant_id));
  } else {
    inited_ = true;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObBalanceGroupContainer::get_gts_switch(bool& on)
{
  int ret = OB_SUCCESS;
  int64_t cur_ts_type = -1;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_guard_.get_timestamp_service_type(tenant_id_, cur_ts_type))) {
    LOG_WARN("fail to get gts switch", K(ret));
  } else {
    on = transaction::is_ts_type_external_consistent(cur_ts_type);
  }
  return ret;
}

int ObBalanceGroupContainer::build()
{
  int ret = OB_SUCCESS;
  const char** str_arr = ObConfigPartitionBalanceStrategyFuncChecker::balance_strategy;
  bool small_tenant = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObTenantUtils::check_small_tenant(tenant_id_, small_tenant))) {
    LOG_WARN("fail to check small tenant", K(ret), "tenant_id", tenant_id_);
  } else if (small_tenant) {
    // small tenant, no need to build balance group container
  } else {
    if (0 == ObString::make_string(GCONF._partition_balance_strategy)
                 .case_compare(str_arr[ObConfigPartitionBalanceStrategyFuncChecker::AUTO])) {
      if (OB_FAIL(shardgroup_container_builder_.build())) {
        LOG_WARN("fail to build shardgroup container", K(ret));
      } else if (OB_FAIL(tablegroup_container_builder_.build())) {
        LOG_WARN("fail to build tablegroup container", K(ret));
      } else if (OB_FAIL(partition_container_builder_.build())) {
        LOG_WARN("fail to build partition container", K(ret));
      } else if (OB_FAIL(non_partition_container_builder_.build())) {
        LOG_WARN("fail to build non partition table index builder", K(ret));
      }
    } else if (0 == ObString::make_string(GCONF._partition_balance_strategy)
                        .case_compare(str_arr[ObConfigPartitionBalanceStrategyFuncChecker::STANDARD])) {
      if (OB_FAIL(tablegroup_container_builder_.build())) {
        LOG_WARN("fail to build tablegroup container", K(ret));
      } else if (OB_FAIL(partition_container_builder_.build())) {
        LOG_WARN("fail to build partition container", K(ret));
      } else if (OB_FAIL(non_partition_container_builder_.build())) {
        LOG_WARN("fail to build non partition table index builder", K(ret));
      }
    } else if (0 == ObString::make_string(GCONF._partition_balance_strategy)
                        .case_compare(str_arr[ObConfigPartitionBalanceStrategyFuncChecker::DISK_UTILIZATION_ONLY])) {
      // disk only, do not build
    }
  }
  return ret;
}

int IBalanceGroupContainerBuilder::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    inited_ = true;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int TableGroupContainerBuilder::build()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    TableGroupAnalyzer analyzer(schema_guard_, stat_finder_, allocator_, processed_tids_);
    if (OB_FAIL(analyzer.analysis(tenant_id_))) {
      LOG_WARN("fail to analysis tablegroup", K(ret), K(tenant_id_));
    } else {
      const ObPartitionSchema* leader_schema = nullptr;
      while (OB_SUCC(ret) && OB_SUCC(analyzer.next(leader_schema))) {
        if (OB_UNLIKELY(nullptr == leader_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("leader table schema ptr is null", K(ret));
        } else if (OB_FAIL(build_tablegroup_container(leader_schema))) {
          LOG_WARN("fail to build tablegroup container", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int TableGroupContainerBuilder::build_tablegroup_container(const share::schema::ObPartitionSchema* partition_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == partition_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema ptr is null", K(ret));
  } else {
    SinglePtBalanceContainerBuilder inner_builder(TABLE_GROUP_BALANCE_GROUP,
        schema_guard_,
        stat_finder_,
        allocator_,
        balance_group_container_,
        max_used_balance_group_id_);
    if (OB_FAIL(inner_builder.build(*partition_schema))) {
      LOG_WARN("fail to build tablegroup container", K(ret));
    }
  }
  return ret;
}

int PartitionTableContainerBuilder::build()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    PartitionTableAnalyzer analyzer(schema_guard_, stat_finder_, allocator_, processed_tids_);
    if (OB_FAIL(analyzer.analysis(tenant_id_))) {
      LOG_WARN("fail to analysis partition table", K(ret), K(tenant_id_));
    } else {
      const ObTableSchema* table_schema = nullptr;
      while (OB_SUCC(ret) && OB_SUCC(analyzer.next(table_schema))) {
        if (OB_UNLIKELY(nullptr == table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema ptr is null", K(ret));
        } else if (OB_FAIL(build_partition_table_container(table_schema))) {
          LOG_WARN("fail to build tablegroup container", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int PartitionTableContainerBuilder::build_partition_table_container(const share::schema::ObTableSchema* table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema ptr is null", K(ret));
  } else {
    SinglePtBalanceContainerBuilder inner_builder(PARTITION_TABLE_BALANCE_GROUP,
        schema_guard_,
        stat_finder_,
        allocator_,
        balance_group_container_,
        max_used_balance_group_id_);
    if (OB_FAIL(inner_builder.build(*table_schema))) {
      LOG_WARN("fail to build table group container", K(ret));
    }
  }
  return ret;
}

int NonPartitionTableContainerBuilder::build()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    NonPartitionTableAnalyzer analyzer(schema_guard_, stat_finder_, allocator_, processed_tids_);
    if (OB_FAIL(analyzer.analysis(tenant_id_))) {
      LOG_WARN("fail to analysis partition table", K(ret), K(tenant_id_));
    } else {
      common::ObArray<const share::schema::ObPartitionSchema*> partition_schemas;
      while (OB_SUCC(ret) && OB_SUCC(analyzer.next(partition_schemas))) {
        if (partition_schemas.count() < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected array size", K(ret));
        } else if (OB_FAIL(build_non_partition_table_container(partition_schemas))) {
          LOG_WARN("fail to build non partition table container", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int NonPartitionTableContainerBuilder::build_non_partition_table_container(
    const common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas)
{
  int ret = OB_SUCCESS;
  BalanceGroupBoxBuilder balance_group_box_builder(NON_PARTITION_TABLE_BALANCE_GROUP,
      balance_group_container_.get_container_type_array(),
      schema_guard_,
      stat_finder_,
      allocator_);
  const int64_t row_size = 1;  // non partition is single row
  const int64_t col_size = partition_schemas.count();
  common::ObZone integrated_primary_zone;
  bool primary_zone_match = true;
  const share::schema::ObPartitionSchema* sample_schema = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(partition_schemas.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (nullptr == (sample_schema = partition_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sample schema ptr is null", K(ret));
  } else if (OB_FAIL(PartitionSchemaChecker::check_same_primary_zone(
                 partition_schemas, schema_guard_, primary_zone_match))) {
    LOG_WARN("fail to check same primary zone", K(ret));
  } else if (primary_zone_match && OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
                                       schema_guard_, *sample_schema, integrated_primary_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (OB_FAIL(
                 balance_group_box_builder.init(row_size, col_size, !primary_zone_match, integrated_primary_zone))) {
    LOG_WARN("fail to init balance group box builder", K(ret));
  } else {
    const int64_t count = partition_schemas.count();
    bool check_dropped_partition = true;
    ++max_used_balance_group_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_schemas.count(); ++i) {
      const share::schema::ObPartitionSchema* partition_schema = partition_schemas.at(i);
      int64_t all_tg_idx = OB_INVALID_INDEX_INT64;
      int64_t part_num = 0;
      if (OB_UNLIKELY(nullptr == partition_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (OB_FAIL(partition_schema->get_all_partition_num(check_dropped_partition, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(partition_schema));
      } else if (part_num > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema part num greater than 1", K(ret));
      } else if (OB_FAIL(stat_finder_.get_all_tg_idx(
                     partition_schema->get_tablegroup_id(), partition_schema->get_table_id(), all_tg_idx))) {
        LOG_WARN("fail to get all tg idx", K(ret));
      } else {
        ObPartitionKey pkey;
        ObPartitionKeyIter iter(partition_schema->get_table_id(), *partition_schema, check_dropped_partition);
        int64_t part_idx = -1;
        int64_t all_pg_idx = -1;
        if (OB_FAIL(iter.next_partition_key_v2(pkey))) {
          LOG_WARN("fail to get next partition key", K(ret));
        } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                       *partition_schema, check_dropped_partition, pkey.get_partition_id(), part_idx))) {
          LOG_WARN("fail to get partition idx by id", K(ret));
        } else if (OB_FAIL(stat_finder_.get_all_pg_idx(pkey, all_tg_idx, all_pg_idx))) {
          LOG_WARN("fail to get all pg idx", K(ret));
        } else {
          BalanceGroupBoxItem item(pkey,
              i,
              count,
              tenant_id_,
              NON_PARTITION_TABLE_BALANCE_GROUP,
              0 /*row*/,
              i /*col*/,
              all_tg_idx,
              all_pg_idx,
              partition_schema->get_tablegroup_id(),
              partition_schema->get_table_id(),
              part_idx,
              max_used_balance_group_id_);
          if (OB_FAIL(balance_group_box_builder.set_item(item))) {
            LOG_WARN("fail to set item", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              balance_group_container_.collect_balance_group_box(balance_group_box_builder.get_balance_group_box()))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

PartitionDistributePolicy SinglePtBalanceContainerBuilder::get_partition_schema_distribute_policy(
    const share::schema::ObPartitionSchema& partition_schema)
{
  PartitionDistributePolicy policy = DISTRIBUTE_BY_AUTO;
  switch (partition_schema.distribute_by()) {
    case 1:
      policy = DISTRIBUTE_BY_LEVEL_ONE;
      break;
    case 2:
      policy = DISTRIBUTE_BY_LEVEL_TWO;
      break;
    default:
      policy = DISTRIBUTE_BY_LEVEL_ONE;
      break;
  }
  return policy;
}

int SinglePtBalanceContainerBuilder::build_one_level_partition_table_container(
    const share::schema::ObPartitionSchema& partition_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t index_group_id =
      (partition_schema.get_tablegroup_id() == OB_INVALID_ID ? partition_schema.get_table_id()
                                                             : partition_schema.get_tablegroup_id());
  bool check_dropped_partition = true;
  ObPartIteratorV2 main_part_iter(partition_schema, check_dropped_partition);
  const share::schema::ObPartition* partition = NULL;
  ObPartitionKey pkey;
  int64_t all_tg_idx = -1;
  ++max_used_balance_group_id_;
  BalanceGroupBoxBuilder balance_group_box_builder(balance_group_type_,
      balance_group_container_.get_container_type_array(),
      schema_guard_,
      stat_finder_,
      allocator_);
  int64_t part_num = 0;
  common::ObZone integrated_primary_zone;
  if (OB_FAIL(partition_schema.get_all_partition_num(check_dropped_partition, part_num))) {
    LOG_WARN("fail to get all partition num", K(ret), K(partition_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
                 schema_guard_, partition_schema, integrated_primary_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (OB_FAIL(
                 balance_group_box_builder.init(1, part_num, false /*do leader balance*/, integrated_primary_zone))) {
    LOG_WARN("fail to init balance group box builder", K(ret));
  } else if (OB_FAIL(stat_finder_.get_all_tg_idx(
                 partition_schema.get_tablegroup_id(), partition_schema.get_table_id(), all_tg_idx))) {
    LOG_WARN("fail to get all tg idx", K(ret));
  } else {
    int64_t idx = 0;
    while (OB_SUCC(ret) && OB_SUCC(main_part_iter.next(partition))) {
      int64_t part_idx = -1;
      int64_t all_pg_idx = -1;
      pkey.reset();
      if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition ptr is null", K(ret), KP(partition));
      } else if (idx < 0 || idx >= part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_idx value unexpected", K(ret), K(part_idx), K(part_num));
      } else if (OB_FAIL(pkey.init(partition_schema.get_table_id(),
                     partition->get_part_id(),
                     partition_schema.get_partition_cnt()))) {
        LOG_WARN("fail to init partition key", K(ret));
      } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                     partition_schema, check_dropped_partition, pkey.get_partition_id(), part_idx))) {
        LOG_WARN("fail to get partition idx by id", K(ret));
      } else if (OB_FAIL(stat_finder_.get_all_pg_idx(pkey, all_tg_idx, all_pg_idx))) {
        LOG_WARN("fail to get all pg idx", K(ret));
      } else {
        BalanceGroupBoxItem item(pkey,
            idx,
            part_num,
            index_group_id,
            balance_group_type_,
            0,
            idx,
            all_tg_idx,
            all_pg_idx,
            partition_schema.get_tablegroup_id(),
            partition_schema.get_table_id(),
            part_idx,
            max_used_balance_group_id_);
        if (OB_FAIL(balance_group_box_builder.set_item(item))) {
          LOG_WARN("fail to set item", K(ret));
        }
      }
      ++idx;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              balance_group_container_.collect_balance_group_box(balance_group_box_builder.get_balance_group_box()))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

int SinglePtBalanceContainerBuilder::build_two_level_partition_table_container(
    const share::schema::ObPartitionSchema& partition_schema)
{
  int ret = OB_SUCCESS;
  PartitionDistributePolicy policy = get_partition_schema_distribute_policy(partition_schema);
  if (DISTRIBUTE_BY_LEVEL_ONE == policy) {
    if (OB_FAIL(build_two_level_partition_table_container_by_first_level(partition_schema))) {
      LOG_WARN("fail to append two level partition table index map by first level", K(ret));
    }
  } else if (DISTRIBUTE_BY_LEVEL_TWO == policy) {
    if (OB_FAIL(build_two_level_partition_table_container_by_second_level(partition_schema))) {
      LOG_WARN("fail to append two level partition table index map by second level", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distribute policy unexpected", K(ret), K(policy));
  }
  return ret;
}

int SinglePtBalanceContainerBuilder::build_two_level_partition_table_container_by_first_level(
    const share::schema::ObPartitionSchema& partition_schema)
{
  int ret = OB_SUCCESS;
  BalanceGroupBoxBuilder balance_group_box_builder(balance_group_type_,
      balance_group_container_.get_container_type_array(),
      schema_guard_,
      stat_finder_,
      allocator_);
  int64_t all_tg_idx = -1;
  common::ObZone integrated_primary_zone;
  bool check_dropped_partition = true;
  const int64_t level_one_part_num =
      partition_schema.get_first_part_num() + partition_schema.get_dropped_partition_num();
  int64_t level_two_part_num = 0;
  // by_first_level_one must be a templated secondary partition
  if (!partition_schema.is_sub_part_template()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub part not template should not be here", K(ret));
  } else if (OB_FAIL(partition_schema.get_first_individual_sub_part_num(level_two_part_num))) {
    LOG_WARN("fail to get first sub part num", K(ret), K(partition_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
                 schema_guard_, partition_schema, integrated_primary_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (PARTITION_LEVEL_TWO != partition_schema.get_part_level()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part level", K(ret), "part_level", partition_schema.get_part_level());
  } else if (OB_FAIL(balance_group_box_builder.init(
                 level_two_part_num, level_one_part_num, false /*do leader balance*/, integrated_primary_zone))) {
    LOG_WARN("fail to init balance group box builder", K(ret));
  } else if (OB_FAIL(stat_finder_.get_all_tg_idx(
                 partition_schema.get_tablegroup_id(), partition_schema.get_table_id(), all_tg_idx))) {
    LOG_WARN("fail to get all tg idx", K(ret));
  } else {
    ObPartIteratorV2 main_part_iter(partition_schema, check_dropped_partition);
    const share::schema::ObPartition* partition = NULL;
    ObPartitionKey pkey;
    ++max_used_balance_group_id_;
    int64_t level_one_part_idx = 0;
    while (OB_SUCC(ret) && OB_SUCC(main_part_iter.next(partition))) {
      if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret), KP(partition));
      } else if (level_one_part_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected first part idx", K(ret), K(level_one_part_idx));
      } else if (level_one_part_idx >= level_one_part_num) {
        LOG_WARN("partition count larger than defined in schema ignore",
            "partition_entity_id",
            partition_schema.get_table_id());
      } else {
        ObSubPartIteratorV2 sub_part_iter(partition_schema, *partition, check_dropped_partition);
        const share::schema::ObSubPartition* sub_partition = NULL;
        int64_t level_two_part_idx = 0;
        uint64_t index_group_id =
            (partition_schema.get_tablegroup_id() == OB_INVALID_ID ? partition_schema.get_table_id()
                                                                   : partition_schema.get_tablegroup_id());
        while (OB_SUCC(ret) && OB_SUCC(sub_part_iter.next(sub_partition))) {
          int64_t phy_part_id = -1;
          pkey.reset();
          int64_t part_idx = -1;
          int64_t all_pg_idx = -1;
          if (OB_UNLIKELY(NULL == sub_partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub partition ptr is null", K(ret));
          } else if (level_two_part_idx < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected seconde part idx", K(ret), K(level_two_part_idx));
          } else if (level_two_part_idx >= level_two_part_num) {
            LOG_WARN("partition count larger than defined in schema, ignore",
                "partition_entity_id",
                partition_schema.get_table_id());
          } else if (FALSE_IT(phy_part_id = generate_phy_part_id(partition->get_part_id(),
                                  sub_partition->get_sub_part_id(),
                                  partition_schema.get_part_level()))) {
            // never be here
          } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                         partition_schema, check_dropped_partition, phy_part_id, part_idx))) {
            LOG_WARN("fail to get partition idx", K(ret));
          } else if (OB_FAIL(pkey.init(
                         partition_schema.get_table_id(), phy_part_id, partition_schema.get_partition_cnt()))) {
            LOG_WARN("fail to init pkey", K(ret));
          } else if (OB_FAIL(stat_finder_.get_all_pg_idx(pkey, all_tg_idx, all_pg_idx))) {
            LOG_WARN("fail to get all pg idx", K(ret));
          } else {
            BalanceGroupBoxItem item(pkey,
                level_one_part_idx,
                level_one_part_num,
                index_group_id,
                balance_group_type_,
                level_two_part_idx,
                level_one_part_idx,
                all_tg_idx,
                all_pg_idx,
                partition_schema.get_tablegroup_id(),
                partition_schema.get_table_id(),
                part_idx,
                max_used_balance_group_id_ + level_two_part_idx);
            if (OB_FAIL(balance_group_box_builder.set_item(item))) {
              LOG_WARN("fail to set item", K(ret));
            }
          }
          ++index_group_id;
          ++level_two_part_idx;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        ++level_one_part_idx;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      max_used_balance_group_id_ += level_two_part_num;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              balance_group_container_.collect_balance_group_box(balance_group_box_builder.get_balance_group_box()))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

int SinglePtBalanceContainerBuilder::build_two_level_partition_table_container_by_second_level(
    const share::schema::ObPartitionSchema& partition_schema)
{
  int ret = OB_SUCCESS;
  int64_t all_tg_idx = -1;
  common::ObZone integrated_primary_zone;
  bool check_dropped_partition = true;
  const int64_t level_one_part_num =
      partition_schema.get_first_part_num() + partition_schema.get_dropped_partition_num();
  if (OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
          schema_guard_, partition_schema, integrated_primary_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (PARTITION_LEVEL_TWO != partition_schema.get_part_level()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part level", K(ret), "part_level", partition_schema.get_part_level());
  } else if (OB_FAIL(stat_finder_.get_all_tg_idx(
                 partition_schema.get_tablegroup_id(), partition_schema.get_table_id(), all_tg_idx))) {
    LOG_WARN("fail to get all tg idx", K(ret));
  } else {
    ObPartIteratorV2 main_part_iter(partition_schema, check_dropped_partition);
    const share::schema::ObPartition* partition = NULL;
    ObPartitionKey pkey;
    uint64_t index_group_id =
        (partition_schema.get_tablegroup_id() == OB_INVALID_ID ? partition_schema.get_table_id()
                                                               : partition_schema.get_tablegroup_id());
    int64_t level_one_part_idx = 0;
    while (OB_SUCC(ret) && OB_SUCC(main_part_iter.next(partition))) {
      BalanceGroupBoxBuilder balance_group_box_builder(balance_group_type_,
          balance_group_container_.get_container_type_array(),
          schema_guard_,
          stat_finder_,
          allocator_);
      int64_t level_two_part_num = 0;
      if (OB_UNLIKELY(NULL == partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret), KP(partition));
      } else if (level_one_part_idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected first part idx", K(ret), K(level_one_part_idx));
      } else if (level_one_part_idx >= level_one_part_num) {
        LOG_WARN("partition count larger than defined in schema ignore",
            "partition_entity_id",
            partition_schema.get_table_id());
      } else if (FALSE_IT(level_two_part_num =
                              (partition_schema.is_sub_part_template() ? partition_schema.get_def_sub_part_num()
                                                                       : partition->get_sub_part_num()))) {
        // shall never be here
      } else if (OB_FAIL(balance_group_box_builder.init(
                     1 /*one row*/, level_two_part_num, false /*do leader balance*/, integrated_primary_zone))) {
        LOG_WARN("fail to init balance group box builder", K(ret));
      } else {
        int64_t level_two_part_idx = 0;
        ++max_used_balance_group_id_;
        ObSubPartIteratorV2 sub_part_iter(partition_schema, *partition, check_dropped_partition);
        const share::schema::ObSubPartition* sub_partition = NULL;
        ++max_used_balance_group_id_;
        while (OB_SUCC(ret) && OB_SUCC(sub_part_iter.next(sub_partition))) {
          int64_t part_idx = -1;
          int64_t phy_part_id = -1;
          int64_t all_pg_idx = -1;
          pkey.reset();
          if (OB_UNLIKELY(NULL == sub_partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub partition ptr is null", K(ret));
          } else if (level_two_part_idx < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("level_two_part_idx value unexpected", K(ret), K(level_two_part_idx));
          } else if (level_two_part_idx >= level_two_part_num) {
            LOG_WARN("partition count larger than defined in schema ignore",
                "partition_entity_id",
                partition_schema.get_table_id());
          } else if (FALSE_IT(phy_part_id = generate_phy_part_id(partition->get_part_id(),
                                  sub_partition->get_sub_part_id(),
                                  partition_schema.get_part_level()))) {
            // never be here
          } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                         partition_schema, check_dropped_partition, phy_part_id, part_idx))) {
            LOG_WARN("fail to get partition idx",
                K(ret),
                K(phy_part_id),
                "partition",
                *partition,
                "sub_partition",
                *sub_partition);
          } else if (OB_FAIL(pkey.init(
                         partition_schema.get_table_id(), phy_part_id, partition_schema.get_partition_cnt()))) {
            LOG_WARN("fail to init pkey", K(ret));
          } else if (OB_FAIL(stat_finder_.get_all_pg_idx(pkey, all_tg_idx, all_pg_idx))) {
            LOG_WARN("fail to get all pg idx", K(ret));
          } else {
            BalanceGroupBoxItem item(pkey,
                level_two_part_idx,
                level_two_part_num,
                index_group_id,
                balance_group_type_,
                0,
                level_two_part_idx,
                all_tg_idx,
                all_pg_idx,
                partition_schema.get_tablegroup_id(),
                partition_schema.get_table_id(),
                part_idx,
                max_used_balance_group_id_);
            if (OB_FAIL(balance_group_box_builder.set_item(item))) {
              LOG_WARN("fail to set item", K(ret));
            }
          }
          ++level_two_part_idx;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(balance_group_container_.collect_balance_group_box(
                  balance_group_box_builder.get_balance_group_box()))) {
            LOG_WARN("fail to collect balance group box", K(ret));
          }
        }
        ++index_group_id;
        ++level_one_part_idx;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int SinglePtBalanceContainerBuilder::build(const share::schema::ObPartitionSchema& partition_schema)
{
  int ret = OB_SUCCESS;
  int64_t part_num = 0;
  if (OB_FAIL(partition_schema.get_all_partition_num(true, part_num))) {
    LOG_WARN("fail to get all partition num", K(ret), K(partition_schema));
  } else if (part_num <= 1) {
    // by pass, no need to build for single part table
  } else {
    share::schema::ObPartitionLevel part_level = partition_schema.get_part_level();
    if (PARTITION_LEVEL_ZERO == part_level || PARTITION_LEVEL_ONE == part_level) {
      if (OB_FAIL(build_one_level_partition_table_container(partition_schema))) {
        LOG_WARN("fail to append one level partition table index map", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      if (OB_FAIL(build_two_level_partition_table_container(partition_schema))) {
        LOG_WARN("fail to append two level partition table index map", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part level unexpected", K(ret), K(part_level));
    }
  }
  return ret;
}

int ShardGroupContainerBuilder::TableSchemaPartitionCntCmp::sort()
{
  std::sort(shardgroup_schemas_.begin(), shardgroup_schemas_.end(), *this);
  return ret_;
}

bool ShardGroupContainerBuilder::TableSchemaPartitionCntCmp::operator()(
    const share::schema::ObTableSchema* left, const share::schema::ObTableSchema* right)
{
  bool b_ret = false;
  int64_t l_part_cnt = 0;
  int64_t r_part_cnt = 0;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // by pass
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("left or right ptr is null", KP(left), KP(right));
  } else if (OB_SUCCESS != (ret_ = left->get_all_partition_num(true, l_part_cnt))) {
    LOG_WARN("fail to get all partition num", K_(ret), KPC(left));
  } else if (OB_SUCCESS != (ret_ = right->get_all_partition_num(true, r_part_cnt))) {
    LOG_WARN("fail to get all partition num", K_(ret), KPC(right));
  } else {
    if (l_part_cnt < r_part_cnt) {
      b_ret = true;
    } else if (l_part_cnt > r_part_cnt) {
      b_ret = false;
    } else {
      if (left->get_table_id() < right->get_table_id()) {
        b_ret = true;
      } else {
        b_ret = false;
      }
    }
  }
  return b_ret;
}

int ShardGroupContainerBuilder::build()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ShardGroupAnalyzer analyzer(schema_guard_, stat_finder_, allocator_, processed_tids_);
    if (OB_FAIL(analyzer.analysis(tenant_id_))) {
      LOG_WARN("fail to analysis shardgroup", K(ret), K(tenant_id_));
    } else {
      ObArray<const ObTableSchema*> shardgroup_table_schemas;
      while (OB_SUCC(ret) && OB_SUCC(analyzer.next(shardgroup_table_schemas))) {
        if (shardgroup_table_schemas.count() < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected array size", K(ret));
        } else if (OB_FAIL(build_shardgroup_container(shardgroup_table_schemas))) {
          LOG_WARN("fail to build shardgroup container", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ShardGroupContainerBuilder::build_shardgroup_container(
    common::ObArray<const share::schema::ObTableSchema*>& shardgroup_schemas)
{
  int ret = OB_SUCCESS;
  common::ObZone integrated_primary_zone;
  bool primary_zone_match = true;
  const share::schema::ObTableSchema* sample_schema = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (shardgroup_schemas.count() <= 0) {
    // bypass
  } else if (nullptr == (sample_schema = shardgroup_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sample schema ptr is null", K(ret));
  } else if (OB_FAIL(PartitionSchemaChecker::check_same_primary_zone(
                 shardgroup_schemas, schema_guard_, primary_zone_match))) {
    LOG_WARN("fail to check same primary zone", K(ret));
  } else if (primary_zone_match && OB_FAIL(ObPrimaryZoneUtil::get_pg_integrated_primary_zone(
                                       schema_guard_, *sample_schema, integrated_primary_zone))) {
    LOG_WARN("fail to get pg integrated primary zone", K(ret));
  } else if (PartitionSchemaChecker::is_one_level_and_partition_by_range(shardgroup_schemas)) {
    // For the first-level range partition shard group,
    // divide the group according to the partition range,
    // and put a group of partitions with the same partition range into a group
    // t1_001   (    p1, p2, p3         )
    // t1_002   (    p1, p2, p3, p4     )
    // t1_003   (p0, p1,         p4, p5 )
    // ...
    // result  : m0, m1, m2, m3, m4, m5
    // p0, p1, etc. represent a group of partitions with the same range
    if (OB_FAIL(build_one_level_range_shard_partition_container(
            shardgroup_schemas, primary_zone_match, integrated_primary_zone))) {
      LOG_WARN("fail to build one level range shard partition container", K(ret));
    }
  } else {
    if (OB_FAIL(
            build_shardgroup_partition_container(shardgroup_schemas, primary_zone_match, integrated_primary_zone))) {
      LOG_WARN("fail to build shardgroup partition container", K(ret));
    }
  }
  return ret;
}

int ShardGroupContainerBuilder::locate_same_range_array(
    SameRangeMap& same_range_map, const share::schema::ObPartition& partition, SameRangeArray*& same_range_array)
{
  int ret = OB_SUCCESS;
  same_range_array = NULL;
  const ObRowkey& key = partition.get_high_bound_val();
  int tmp_ret = same_range_map.get_refactored(key, same_range_array);
  if (OB_SUCCESS == tmp_ret) {
    if (OB_UNLIKELY(NULL == same_range_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("same range array ptr is null", K(ret));
    } else {
      // good, get same range array
    }
  } else if (OB_HASH_NOT_EXIST == tmp_ret) {
    const int32_t overwrite = 0;
    void* ptr = allocator_.alloc(sizeof(SameRangeArray));
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if (NULL == (same_range_array = new (ptr) SameRangeArray(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to construct same range pkey array", K(ret));
    } else if (OB_FAIL(same_range_map.set_refactored(key, same_range_array, overwrite))) {
      LOG_WARN("fail to insert into hash map", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get item from hash map", K(ret));
  }
  return ret;
}

int ShardGroupContainerBuilder::set_same_range_array(SameRangeMap& same_range_map,
    const share::schema::ObPartition& partition, const int64_t part_idx,
    const share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SameRangeArray* same_range_array = NULL;
    ObPartitionKey part_key;
    const uint64_t table_id = partition.get_table_id();
    const int64_t partition_id = partition.get_part_id();
    const uint64_t tablegroup_id = table_schema.get_tablegroup_id();
    int64_t all_tg_idx = OB_INVALID_INDEX_INT64;
    if (OB_FAIL(locate_same_range_array(same_range_map, partition, same_range_array))) {
      LOG_WARN("fail to locate same range pkey array", K(ret));
    } else if (OB_UNLIKELY(NULL == same_range_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("same range array ptr is null", K(ret));
    } else if (OB_FAIL(part_key.init(table_id, partition_id, table_schema.get_partition_cnt()))) {
      LOG_WARN("fail to init part key", K(ret));
    } else if (OB_FAIL(stat_finder_.get_all_tg_idx(tablegroup_id, table_id, all_tg_idx))) {
      LOG_WARN("fail to get all tg idx", K(ret));
    } else {
      SameRangePartInfo part_info(part_key, tablegroup_id, table_id, all_tg_idx, part_idx);
      if (OB_FAIL(same_range_array->push_back(part_info))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ShardGroupContainerBuilder::build_one_level_range_shard_partition_container(
    common::ObArray<const share::schema::ObTableSchema*>& shardgroup_schemas, const bool primary_zone_match,
    const common::ObZone& integrated_primary_zone)
{
  int ret = OB_SUCCESS;
  SameRangeMap same_range_map;
  const int64_t MAP_SIZE = shardgroup_schemas.count() * 2;
  bool is_map_valid = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (shardgroup_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shardgroup schemas count", K(ret));
  } else if (MAP_SIZE <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size", K(ret), K(MAP_SIZE));
  } else if (OB_FAIL(same_range_map.create(MAP_SIZE, ObModIds::OB_HASH_BUCKET_BALANCER))) {
    LOG_WARN("fail to create map", K(ret));
  } else if (OB_FAIL(ShardGroupValidator::check_table_schemas_compatible(
                 stat_finder_, schema_guard_, shardgroup_schemas, is_map_valid))) {
    LOG_WARN("fail to check table schemas compatible", K(ret));
  } else if (!is_map_valid) {
    // invalid shard group map
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < shardgroup_schemas.count(); ++i) {
      const share::schema::ObTableSchema* table_schema = shardgroup_schemas.at(i);
      if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else {
        bool check_dropped_schema = true;
        ObPartIteratorV2 iter(*table_schema, check_dropped_schema);
        const ObPartition* partition = NULL;
        int64_t part_idx = 0;
        while (OB_SUCC(ret) && OB_SUCC(iter.next(partition))) {
          if (OB_UNLIKELY(NULL == partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition ptr is null", K(ret));
          } else if (OB_FAIL(set_same_range_array(same_range_map, *partition, part_idx, *table_schema))) {
            LOG_WARN("fail to do append partition key", K(ret));
          } else {
            ++part_idx;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    uint64_t base_index_group_id = shardgroup_schemas.at(0)->get_table_id();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_build_one_level_range_partition_container(
                   same_range_map, base_index_group_id, primary_zone_match, integrated_primary_zone))) {
      LOG_WARN("fail to do append", K(ret));
    }
  }
  return ret;
}

int ShardGroupContainerBuilder::do_build_one_level_range_partition_container(SameRangeMap& same_range_map,
    const uint64_t base_index_group_id, const bool primary_zone_match, const common::ObZone& integrated_primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (primary_zone_match && integrated_primary_zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(integrated_primary_zone));
  } else {
    uint64_t index_group_id = base_index_group_id;
    for (SameRangeMap::iterator iter = same_range_map.begin(); OB_SUCC(ret) && iter != same_range_map.end(); ++iter) {
      ++max_used_balance_group_id_;
      // Each loop generates a set balance group,
      // increment this value in advance
      SameRangeArray* array = iter->second;
      BalanceGroupBoxBuilder balance_group_box_builder(SHARD_PARTITION_BALANCE_GROUP,
          balance_group_container_.get_container_type_array(),
          schema_guard_,
          stat_finder_,
          allocator_);
      const int64_t row_size = 1;  // shard partition's square map is single row
      if (OB_UNLIKELY(NULL == array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array ptr is null", K(ret));
      } else if (array->count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array ptr is null", K(ret));
      } else if (OB_FAIL(balance_group_box_builder.init(
                     row_size, array->count(), !primary_zone_match, integrated_primary_zone))) {
        LOG_WARN("fail to init balance group box builder", K(ret));
      } else {
        const int64_t shard_count = array->count();
        for (int64_t i = 0; OB_SUCC(ret) && i < array->count(); ++i) {
          int64_t all_pg_idx = -1;
          const SameRangePartInfo& info = array->at(i);
          if (OB_FAIL(stat_finder_.get_all_pg_idx(info.pkey_, info.all_tg_idx_, all_pg_idx))) {
            LOG_WARN("fail to get all pg idx", K(ret));
          } else {
            BalanceGroupBoxItem item(info.pkey_,
                i,
                shard_count,
                index_group_id,
                SHARD_PARTITION_BALANCE_GROUP,
                0 /*row*/,
                i,
                info.all_tg_idx_,
                all_pg_idx,
                info.tablegroup_id_,
                info.table_id_,
                info.part_idx_,
                max_used_balance_group_id_);
            if (OB_FAIL(balance_group_box_builder.set_item(item))) {
              LOG_WARN("fail to set item", K(ret));
            }
          }
        }
        ++index_group_id;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(balance_group_container_.collect_balance_group_box(
                balance_group_box_builder.get_balance_group_box()))) {
          LOG_WARN("fail to collect balance group box", K(ret));
        }
      }
    }
  }
  return ret;
}

/*       t(1)  t(2)  t(3)  t(4) ..... t(k)  t(k+1)  row part count(shard_count)   primary_pkey
 *       p10   p20   p30   p40  ..... pk0   p(k+1)0              k+1              p(k+1)0
 *       p11   p21   p31   p41  ..... pk1   p(k+1)1              k+1              p(k+1)1
 *       -     -     p32   p42  ..... pk2   p(k+1)2              k-1              p(k+1)2
 *       -     -     p33   p43  ..... pk3   p(k+1)3              k-1              p(k+1)3
 *             .................
 *             .................
 *       -     -     -     -    ..... pkn   p(k+1)n               2               p(k+1)n
 */
int ShardGroupContainerBuilder::build_shardgroup_partition_container(
    common::ObArray<const share::schema::ObTableSchema*>& shardgroup_schemas, const bool primary_zone_match,
    const common::ObZone& integrated_primary_zone)
{
  int ret = OB_SUCCESS;
  bool is_map_valid = false;
  // Sort in ascending order by the number of partitions in the table
  TableSchemaPartitionCntCmp cmp_operator(shardgroup_schemas);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (primary_zone_match && integrated_primary_zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(integrated_primary_zone));
  } else if (OB_FAIL(ShardGroupValidator::check_table_schemas_compatible(
                 stat_finder_, schema_guard_, shardgroup_schemas, is_map_valid))) {
    LOG_WARN("fail to check table schemas compatible", K(ret));
  } else if (!is_map_valid) {
    // invalid map
  } else if (OB_FAIL(cmp_operator.sort())) {
    LOG_WARN("fail to sort shardgroup schemas", K(ret));
  } else if (shardgroup_schemas.count() <= 0 || NULL == shardgroup_schemas.at(shardgroup_schemas.count() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema array count unexpected", K(ret));
  } else {
    const int64_t count = shardgroup_schemas.count();
    ObArray<int64_t> shard_count_array;
    int64_t max_partition_cnt = 0;
    int64_t min_partition_cnt = 0;
    if (OB_FAIL(shardgroup_schemas.at(count - 1)->get_all_partition_num(true, max_partition_cnt))) {
      LOG_WARN("fail to get all partition num", K(ret));
    } else if (OB_FAIL(shardgroup_schemas.at(0)->get_all_partition_num(true, min_partition_cnt))) {
      LOG_WARN("fail to get all partition num", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < max_partition_cnt; ++i) {
      if (OB_FAIL(shard_count_array.push_back(0))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < shardgroup_schemas.count(); ++i) {
      const share::schema::ObTableSchema* table_schema = shardgroup_schemas.at(i);
      int64_t this_part_num = 0;
      if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (OB_FAIL(table_schema->get_all_partition_num(true, this_part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(table_schema));
      } else if (this_part_num <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all part num is unexpected", K(ret));
      } else if (this_part_num > max_partition_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all part num unexpected", K(ret), K(this_part_num), K(max_partition_cnt));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < this_part_num; ++j) {
          ++shard_count_array.at(j);
        }
      }
    }
    /*
     *       |  col0   |  col1  |  col2 |
     * ------+---------+--------+------------
     *  row0 |  t1,p0  | t2,p0  | t3,p0 | ...
     *  row1 |  t1,p1  | t2,p1  | t3,p1 | ...
     *  row2 |  t1,p2  | t2,p2  | t3,p2 | ...
     *  row3 |  -      | t2,p3  |  -    | ...   <=== t2's part_num is one more table than other tables
     *
     */
    BalanceGroupBoxBuilder balance_group_box_builder(SHARD_GROUP_BALANCE_GROUP,
        balance_group_container_.get_container_type_array(),
        schema_guard_,
        stat_finder_,
        allocator_);
    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_FAIL(balance_group_box_builder.init(
                   min_partition_cnt, shardgroup_schemas.count(), !primary_zone_match, integrated_primary_zone))) {
      LOG_WARN("fail to build balance group box builder", K(ret));
    }
    int64_t start_balance_group_id = max_used_balance_group_id_ + 1;
    for (int64_t i = shardgroup_schemas.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const int64_t in_shard_index = shardgroup_schemas.count() - (i + 1);
      const share::schema::ObTableSchema* table_schema = shardgroup_schemas.at(i);
      int64_t all_tg_idx = OB_INVALID_INDEX_INT64;
      if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (OB_FAIL(stat_finder_.get_all_tg_idx(
                     table_schema->get_tablegroup_id(), table_schema->get_table_id(), all_tg_idx))) {
        LOG_WARN("fail to get all tg idx", K(ret));
      } else {
        int64_t part_id = 0;
        bool check_dropped_schema = true;
        ObTablePartitionKeyIter iter(*table_schema, check_dropped_schema);
        const uint64_t table_id = table_schema->get_table_id();
        const uint64_t tablegroup_id = table_schema->get_tablegroup_id();
        uint64_t index_group_id = shardgroup_schemas.at(0)->get_table_id();
        int64_t row = 0;
        bool check_dropped_partition = true;
        while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(part_id))) {
          int64_t part_idx = -1;
          int64_t all_pg_idx = -1;
          ObPartitionKey pkey;
          int64_t shard_count = 0;
          if (OB_FAIL(
                  ObPartMgrUtils::get_partition_idx_by_id(*table_schema, check_dropped_partition, part_id, part_idx))) {
            LOG_WARN("fail to get partition idx by id", K(ret));
          } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= max_partition_cnt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part idx unexpected", K(ret), K(part_idx), K(max_partition_cnt));
          } else if (row >= min_partition_cnt) {
            LOG_WARN("partition count larger than min part count schema", K(ret), K(table_id));
          } else if ((shard_count = shard_count_array.at(part_idx)) <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected shard count", K(ret), K(shard_count));
          } else if (OB_FAIL(pkey.init(table_id, part_id, table_schema->get_partition_cnt()))) {
            LOG_WARN("fail to init pkey", K(ret));
          } else if (OB_FAIL(stat_finder_.get_all_pg_idx(pkey, all_tg_idx, all_pg_idx))) {
            LOG_WARN("fail to get all pg idx", K(ret));
          } else {
            BalanceGroupBoxItem item(pkey,
                in_shard_index,
                shard_count,
                index_group_id,
                SHARD_GROUP_BALANCE_GROUP,
                row,
                in_shard_index,
                all_tg_idx,
                all_pg_idx,
                tablegroup_id,
                table_id,
                part_idx,
                start_balance_group_id + row);
            if (OB_FAIL(balance_group_box_builder.set_item(item))) {
              LOG_WARN("fail to set item", K(ret));
            }
          }
          ++row;
          ++index_group_id;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret)) {
      max_used_balance_group_id_ += max_partition_cnt;
      if (OB_FAIL(
              balance_group_container_.collect_balance_group_box(balance_group_box_builder.get_balance_group_box()))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

int ObSinglePtBalanceContainer::init(const int64_t item_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(hash_index_collection_.init(item_size))) {
    LOG_WARN("fail to create map", K(ret));
  } else if (OB_FAIL(container_type_array_.push_back(HASH_INDEX_TYPE))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObSinglePtBalanceContainer::build()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t dummy_max_used_bg_id_ = -1;
    SinglePtBalanceContainerBuilder inner_builder(
        PARTITION_TABLE_BALANCE_GROUP, schema_guard_, stat_finder_, allocator_, *this, dummy_max_used_bg_id_);
    if (OB_FAIL(inner_builder.build(partition_schema_))) {
      LOG_WARN("fail to build", K(ret));
    }
  }
  return ret;
}

int ObSinglePtBalanceContainer::collect_balance_group_box(
    const common::ObIArray<BalanceGroupBox*>& balance_group_box_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(balance_group_box_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_group_box_array.count(); ++i) {
      const BalanceGroupBox* this_box = balance_group_box_array.at(i);
      if (OB_UNLIKELY(nullptr == this_box)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this box ptr is null", K(ret));
      } else if (HASH_INDEX_TYPE != this_box->get_container_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid box type", K(ret), "container_type", this_box->get_container_type());
      } else if (OB_FAIL(hash_index_collection_.collect_box(this_box))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

int BalanceGroupBoxBuilder::set_item(const BalanceGroupBoxItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_box_array_.count(); ++i) {
      BalanceGroupBox* this_box = output_box_array_.at(i);
      if (OB_UNLIKELY(nullptr == this_box)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nullptr is null", K(ret));
      } else if (OB_FAIL(this_box->set_item(item))) {
        LOG_WARN("fail to set item", K(ret));
      }
    }
  }
  return ret;
}

int BalanceGroupBoxBuilder::init_square_id_map(const int64_t row_size, const int64_t col_size,
    const bool ignore_leader_balance, const common::ObZone& primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(balance_group_type_ <= INVALID_BALANCE_GROUP_TYPE || balance_group_type_ >= MAX_BALANCE_GROUP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(balance_group_type_));
  } else if (OB_UNLIKELY(row_size <= 0 || col_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_size), K(col_size));
  } else {
    switch (balance_group_type_) {
      case SHARD_GROUP_BALANCE_GROUP: {
        ShardGroupIdMap* id_map = nullptr;
        void* ptr = allocator_.alloc(sizeof(ShardGroupIdMap));
        if (OB_UNLIKELY(nullptr == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_UNLIKELY(
                       nullptr == (id_map = new (ptr) ShardGroupIdMap(schema_guard_, stat_finder_, allocator_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to construct shardgroup id map");
        } else if (OB_FAIL(id_map->init(row_size, col_size))) {
          LOG_WARN("fail to init id map", K(ret));
        } else if (OB_FAIL(output_box_array_.push_back(id_map))) {
          LOG_WARN("fail to push back to box array", K(ret));
        } else {
          id_map->set_ignore_leader_balance(ignore_leader_balance);
          id_map->set_primary_zone(primary_zone);
        }
        break;
      }
      case TABLE_GROUP_BALANCE_GROUP: {
        TableGroupIdMap* id_map = nullptr;
        void* ptr = allocator_.alloc(sizeof(TableGroupIdMap));
        if (OB_UNLIKELY(nullptr == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_UNLIKELY(
                       nullptr == (id_map = new (ptr) TableGroupIdMap(schema_guard_, stat_finder_, allocator_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to construct tablegroup id map", K(ret));
        } else if (OB_FAIL(id_map->init(row_size, col_size))) {
          LOG_WARN("fail to init id map", K(ret));
        } else if (OB_FAIL(output_box_array_.push_back(id_map))) {
          LOG_WARN("fail to push back to box array", K(ret));
        } else {
          id_map->set_ignore_leader_balance(ignore_leader_balance);
          id_map->set_primary_zone(primary_zone);
        }
        break;
      }
      case PARTITION_TABLE_BALANCE_GROUP: {
        PartitionTableIdMap* id_map = nullptr;
        void* ptr = allocator_.alloc(sizeof(PartitionTableIdMap));
        if (OB_UNLIKELY(nullptr == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_UNLIKELY(
                       nullptr == (id_map = new (ptr) PartitionTableIdMap(schema_guard_, stat_finder_, allocator_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to construct partition group id map", K(ret));
        } else if (OB_FAIL(id_map->init(row_size, col_size))) {
          LOG_WARN("fail to init id map", K(ret));
        } else if (OB_FAIL(output_box_array_.push_back(id_map))) {
          LOG_WARN("fail to push back to box array", K(ret));
        } else {
          id_map->set_ignore_leader_balance(ignore_leader_balance);
          id_map->set_primary_zone(primary_zone);
        }
        break;
      }
      case SHARD_PARTITION_BALANCE_GROUP: {
        ShardPartitionIdMap* id_map = nullptr;
        void* ptr = allocator_.alloc(sizeof(ShardPartitionIdMap));
        if (OB_UNLIKELY(nullptr == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_UNLIKELY(
                       nullptr == (id_map = new (ptr) ShardPartitionIdMap(schema_guard_, stat_finder_, allocator_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to construct shard partition id map", K(ret));
        } else if (OB_FAIL(id_map->init(row_size, col_size))) {
          LOG_WARN("fail to init id map", K(ret));
        } else if (OB_FAIL(output_box_array_.push_back(id_map))) {
          LOG_WARN("fail to push back to box array", K(ret));
        } else {
          id_map->set_ignore_leader_balance(ignore_leader_balance);
          id_map->set_primary_zone(primary_zone);
        }
        break;
      }
      case NON_PARTITION_TABLE_BALANCE_GROUP: {
        NonPartitionTableIdMap* id_map = nullptr;
        void* ptr = allocator_.alloc(sizeof(NonPartitionTableIdMap));
        if (OB_UNLIKELY(nullptr == ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_UNLIKELY(nullptr ==
                               (id_map = new (ptr) NonPartitionTableIdMap(schema_guard_, stat_finder_, allocator_)))) {
          LOG_WARN("fail to construct non partition table id map", K(ret));
        } else if (OB_FAIL(id_map->init(row_size, col_size))) {
          LOG_WARN("fail to init map", K(ret));
        } else if (OB_FAIL(output_box_array_.push_back(id_map))) {
          LOG_WARN("fail to push back to box array", K(ret));
        } else {
          id_map->set_ignore_leader_balance(ignore_leader_balance);
          id_map->set_primary_zone(primary_zone);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected balance group type", K(ret));
        break;
    }
  }
  return ret;
}

int BalanceGroupBoxBuilder::init_hash_index_map(const int64_t row_size, const int64_t col_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_size <= 0 || col_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t map_size = 2 * row_size * col_size;
    if (OB_FAIL(hash_index_map_.init(map_size))) {
      LOG_WARN("fail to init hash index map", K(ret));
    } else if (OB_FAIL(output_box_array_.push_back(&hash_index_map_))) {
      LOG_WARN("fail to push hash index map to output box array", K(ret));
    }
  }
  return ret;
}

int BalanceGroupBoxBuilder::init(const int64_t row_size, const int64_t col_size, const bool ignore_leader_balance,
    const common::ObZone& primary_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(row_size <= 0 || col_size <= 0 || (!ignore_leader_balance && primary_zone.is_empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_size), K(col_size));
  } else if (OB_UNLIKELY(container_type_array_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("container type array is null", K(ret));
  } else {
    output_box_array_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < container_type_array_.count(); ++i) {
      BalanceGroupContainerType container_type = container_type_array_.at(i);
      switch (container_type) {
        case SQUARE_MAP_TYPE:
          if (OB_FAIL(init_square_id_map(row_size, col_size, ignore_leader_balance, primary_zone))) {
            LOG_WARN("fail to init square id map", K(ret));
          }
          break;
        case HASH_INDEX_TYPE:
          if (OB_FAIL(init_hash_index_map(row_size, col_size))) {
            LOG_WARN("fail to init hash index map", K(ret));
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected container type", K(ret), K(container_type));
          break;
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int ObLeaderBalanceGroupContainer::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t HASH_MAP_SIZE = 100 * 1024;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceGroupContainer::init(tenant_id))) {
    LOG_WARN("fail to init basic class", K(ret));
  } else if (OB_FAIL(hash_index_collection_.init(HASH_MAP_SIZE))) {
    LOG_WARN("fail to init hash index collection", K(ret));
  } else if (OB_FAIL(container_type_array_.push_back(HASH_INDEX_TYPE))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

int ObLeaderBalanceGroupContainer::collect_balance_group_box(
    const common::ObIArray<BalanceGroupBox*>& balance_group_box_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(balance_group_box_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_group_box_array.count(); ++i) {
      const BalanceGroupBox* this_box = balance_group_box_array.at(i);
      if (OB_UNLIKELY(nullptr == this_box)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this box ptr is null", K(ret));
      } else if (HASH_INDEX_TYPE != this_box->get_container_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid box type", K(ret), "container_type", this_box->get_container_type());
      } else if (OB_FAIL(hash_index_collection_.collect_box(this_box))) {
        LOG_WARN("fail to collect balance group box", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionBalanceGroupContainer::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t HASH_MAP_SIZE = 100 * 1024;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceGroupContainer::init(tenant_id))) {
    LOG_WARN("fail to init basic class", K(ret));
  } else if (OB_FAIL(hash_index_collection_.init(HASH_MAP_SIZE))) {
    LOG_WARN("fail to init hash index collection", K(ret));
  } else if (OB_FAIL(square_id_map_collection_.init())) {
    LOG_WARN("fail to init square id map", K(ret));
  } else if (OB_FAIL(container_type_array_.push_back(HASH_INDEX_TYPE))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(container_type_array_.push_back(SQUARE_MAP_TYPE))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

int ObPartitionBalanceGroupContainer::collect_balance_group_box(
    const common::ObIArray<BalanceGroupBox*>& balance_group_box_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(balance_group_box_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_group_box_array.count(); ++i) {
      const BalanceGroupBox* this_box = balance_group_box_array.at(i);
      if (OB_UNLIKELY(nullptr == this_box)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this box ptr is null", K(ret));
      } else if (HASH_INDEX_TYPE == this_box->get_container_type()) {
        if (OB_FAIL(hash_index_collection_.collect_box(this_box))) {
          LOG_WARN("fail to collect box", K(ret));
        }
      } else if (SQUARE_MAP_TYPE == this_box->get_container_type()) {
        if (OB_FAIL(square_id_map_collection_.collect_box(this_box))) {
          LOG_WARN("fail to collect box", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected container type", K(ret), "container_type", this_box->get_container_type());
      }
    }
  }
  return ret;
}

int ObPartitionBalanceGroupContainer::calc_leader_balance_statistic(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(square_id_map_collection_.calc_leader_balance_statistic(zone, hash_index_collection_))) {
    LOG_WARN("fail to calc leader balance statistic", K(ret));
  } else {
  }
  return ret;
}
