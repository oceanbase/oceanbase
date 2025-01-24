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

#define USING_LOG_PREFIX STORAGE
#include "ob_partition_pre_split.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/ob_index_builder_util.h"
#include "src/share/scheduler/ob_partition_auto_split_helper.h"


namespace oceanbase
{
using namespace share;
using namespace rootserver;
using namespace common;
using namespace obrpc;
using namespace pl;

namespace storage
{

ObPartitionPreSplit::ObPartitionPreSplit(rootserver::ObDDLService &ddl_service)
  : ddl_service_(&ddl_service)
{
}

ObPartitionPreSplit::ObPartitionPreSplit()
{
  ddl_service_ = nullptr;
}

void ObPartitionPreSplit::reset()
{
  split_ranges_.reset();
  ddl_service_ = nullptr;
}

void ObPartitionPreSplit::get_split_num(
    const int64_t tablet_size,
    const int64_t split_size,
    int64_t &split_num)
{
  int ret = OB_SUCCESS;
  int64_t tablet_limit_penalty = 0;
  int64_t real_split_size = split_size;
  split_num = 0;
  if (tablet_size < 0 || split_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arugument", K(ret), K(tablet_size), K(split_size));
  } else if (OB_FAIL((ObServerAutoSplitScheduler::check_tablet_creation_limit(MAX_SPLIT_RANGE_NUM/*inc_tablet_cnt*/, 0.8/*safe ratio*/, split_size, real_split_size)))) {
    if (OB_TOO_MANY_PARTITIONS_ERROR == ret) {
      LOG_WARN("too many partitions in the observer, choose to not to do the pre split", K(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check tablet creation limit", K(ret));
    }
  } else if (real_split_size > 0 && tablet_size > real_split_size) {
    split_num = (tablet_size % real_split_size) == 0 ?
          (tablet_size / real_split_size) :
          (tablet_size / real_split_size + 1);
    split_num = split_num < MAX_SPLIT_RANGE_NUM ?
          split_num :
          MAX_SPLIT_RANGE_NUM;
  }
  LOG_DEBUG("[PRE_SPLIT] get tablet split num", K(split_num));
}

/*
  function description:
    1. rebuild none partition index table
    2. create partition/none partition index table
    3. alter main none partition table
*/
int ObPartitionPreSplit::build_new_partition_table_schema(
    const ObTableSchema &ori_table_schema,
    ObTableSchema &inc_partition_schema,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  if (inc_partition_schema.get_partition_num() <= 0) {
    // skip // no new add partitions
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] invalid ddl service", K(ret), KP(ddl_service_));
  } else if (OB_FAIL(ddl_service_->fill_part_name(ori_table_schema, inc_partition_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build split table schema", K(ret), K(inc_partition_schema));
  } else if (OB_FAIL(build_new_table_schema(
      ori_table_schema,
      inc_partition_schema,
      new_table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build new table schema", K(ret), K(inc_partition_schema));
  } else {
    LOG_DEBUG("success to build new partition table schema",
      K(ret), K(inc_partition_schema), K(ori_table_schema), K(new_table_schema));
  }
  return ret;
}

int ObPartitionPreSplit::rebuild_partition_table_schema(
    const ObIArray<ObTabletID> &split_tablet_ids,
    const ObTableSchema &ori_table_schema,
    const ObTableSchema &inc_partition_schema,
    ObTableSchema &all_partition_schema,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (split_tablet_ids.count() > 0) { // has split tablets.
    if (inc_partition_schema.get_partition_num()<= 0) {
      // no new parts, skip.
    } else if (OB_FAIL(generate_all_partition_schema( // gen part idx
        split_tablet_ids,
        ori_table_schema,
        inc_partition_schema,
        all_partition_schema))) {
      LOG_WARN("[PRE_SPLIT] fail to fill full tablet partition schema",
        K(ret), K(ori_table_schema), K(inc_partition_schema));
    } else if (OB_FAIL(build_new_table_schema(
        ori_table_schema,
        all_partition_schema,
        table_schema))) {
      LOG_WARN("[PRE_SPLIT] fail to build new table schema", K(ret), K(all_partition_schema));
    } else {
      LOG_DEBUG("success to rebuild partition table schema",
        K(ret), K(split_tablet_ids), K(inc_partition_schema),
        K(all_partition_schema), K(table_schema));
    }
  }
  return ret;
}

/*
 * get table column ids.
*/
int ObPartitionPreSplit::extract_table_columns_id(
    const ObTableSchema &table_schema,
    ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  ObTableSchema::const_column_iterator col_iter = table_schema.column_begin();
  ObTableSchema::const_column_iterator col_end = table_schema.column_end();
  for (; OB_SUCC(ret) && col_iter != col_end; ++col_iter) {
    const ObColumnSchemaV2 *column_schema = *col_iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PRE_SPLIT] invalid column schema", K(column_schema));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("[PRE_SPLIT] fail to push back data column id.", K(ret), K(column_schema));
    }
  }
  return ret;
}

// not support pre-split global index table in none partition main table.
int ObPartitionPreSplit::get_data_table_part_ids(
    const ObTableSchema &data_table_schema,
    ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  part_ids.reset();
  ObPartition **part_array = data_table_schema.get_part_array();
  ObPartitionLevel part_level = data_table_schema.get_part_level();
  const int64_t partition_num = data_table_schema.get_partition_num();
  if (0 == partition_num || PARTITION_LEVEL_ZERO == part_level) {
    // auto partition none partition table is support, partition num maybe zero
    if (OB_FAIL(part_ids.push_back(data_table_schema.get_table_id()))) {
      LOG_WARN("[PRE_SPLIT] fail to push back table id.", K(ret));
    }
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] invalid part array", K(ret), KP(part_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      ObPartition *part = part_array[i];
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] invalid partition", K(ret), KP(part));
      } else if (OB_FAIL(part_ids.push_back(part->get_part_id()))) {
        LOG_WARN("[PRE_SPLIT] fail to push back part id.", K(ret), K(part));
      }
    }
  }
  return ret;
}

// only for global index
int ObPartitionPreSplit::get_estimated_table_size(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    int64_t &table_size)
{
  int ret = OB_SUCCESS;

  ObSEArray<uint64_t, 4> column_ids;
  ObSEArray<uint64_t, 1> part_sizes;
  ObSEArray<int64_t, 1> part_ids;
  table_size = 0; // reset

  if (OB_FAIL(index_table_schema.get_column_ids(column_ids))) {
    LOG_WARN("[PRE_SPLIT] fail to get mapping column ids.", K(ret));
  } else if (OB_FAIL(get_data_table_part_ids(data_table_schema, part_ids))) {
    LOG_WARN("[PRE_SPLIT] fail to get data table part id.", K(ret), K(data_table_schema));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObDbmsSpace::IndexCostInfo cost_info;
    if (OB_FAIL(cost_info.part_ids_.assign(part_ids))) {
      LOG_WARN("[PRE_SPLIT] fail to assign cost info part id.", K(ret), K(part_ids));
    } else if (OB_FAIL(cost_info.column_ids_.assign(column_ids))) {
      LOG_WARN("[PRE_SPLIT] fail to assign cost info column id.", K(ret), K(column_ids));
    } else if (FALSE_IT(cost_info.tenant_id_ = OB_SYS_TENANT_ID)) {
    } else if (FALSE_IT(cost_info.table_id_ = data_table_schema.get_table_id())) {
    } else if (FALSE_IT(cost_info.table_tenant_id_ = data_table_schema.get_tenant_id())) {
    } else if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PRE_SPLIT] invalid sql proxy.", KP(sql_proxy));
    } else if (OB_FAIL(ObDbmsSpace::estimate_index_table_size(
        sql_proxy,
        &data_table_schema,
        cost_info,
        part_sizes))) {
      LOG_WARN("[PRE_SPLIT] fail to estimate index table size", K(ret), K(cost_info));
    } else if (part_sizes.count() != part_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PRE_SPLIT] return count not match", K(ret), K(part_sizes.count()), K(part_ids.count()));
    } else {
      for (int64_t i = 0; i < part_sizes.count(); ++i) {
        table_size += part_sizes.at(i);
      }
    }
  }
  return ret;
}
/*
 description:
   1. exist main table
   2. exist global table
*/
int ObPartitionPreSplit::get_exist_table_size(
    const ObTableSchema &table_schema,
    ObIArray<TabletIDSize> &tablet_size)
{
  int ret = OB_SUCCESS;
  tablet_size.reset();
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] invalid sql proxy.", KP(sql_proxy));
  } else if (OB_FAIL(ObDbmsSpace::get_each_tablet_size(
      sql_proxy,
      &table_schema,
      tablet_size))) {
    LOG_WARN("[PRE_SPLIT] fail to get each tablet size");
  }
  return ret;
}

int ObPartitionPreSplit::get_global_index_pre_split_schema_if_need(
    const int64_t tenant_id,
    const int64_t session_id,
    const ObString &database_name,
    const ObString &table_name,
    common::ObIArray<ObIndexArg *> &index_arg_list)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *data_table_schema = NULL;
  schema::ObSchemaGetterGuard schema_guard;
  schema_guard.set_session_id(session_id);
  uint64_t current_data_version = 0;
  if (tenant_id == OB_INVALID_TENANT_ID || database_name.empty() || table_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] unexpected database name or table name", K(tenant_id), K(database_name), K(table_name));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, current_data_version))) {
    LOG_WARN("[PRE_SPLIT] failed to get data version", K(ret));
  } else if (current_data_version < DATA_VERSION_4_3_4_0) {
    LOG_INFO("[PRE_SPLIT] current data version less than 4.3.4.0 is not support", K(ret), K(current_data_version));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("[PRE_SPLIT] fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FALSE_IT(schema_guard.set_session_id(session_id))) {
  } else if (schema_guard.get_table_schema(tenant_id, database_name, table_name, false/*is_index*/, data_table_schema)) {
    LOG_WARN("[PRE_SPLIT] fail to get table schema", K(ret), K(tenant_id), K(database_name), K(table_name));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("[PRE_SPLIT] table not exist", K(ret), KP(data_table_schema));
  } else if (!data_table_schema->is_auto_partitioned_table() ||
              data_table_schema->is_mysql_tmp_table()) {
    LOG_INFO("[PRE_SPLIT] not support auto split table type", K(ret), K(data_table_schema));
  } else {
    // alter table add global index
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.count(); ++i) {
      ObIndexArg *index_arg = index_arg_list.at(i);
      if (index_arg->index_action_type_ == ObIndexArg::ADD_INDEX) {
        HEAP_VAR(ObTableSchema, index_schema) {
          ObCreateIndexArg *create_index_arg = static_cast<ObCreateIndexArg *>(index_arg);
          if (OB_FAIL(index_schema.assign(create_index_arg->index_schema_))) {
            LOG_WARN("fail to assign index schema", K(ret));
          } else if (OB_FALSE_IT(index_schema.set_tenant_id(data_table_schema->get_tenant_id()))) {
          } else if (OB_FALSE_IT(index_schema.set_data_table_id(data_table_schema->get_table_id()))) {
          } else if (index_schema.get_column_count() == 0 &&
              OB_FAIL(ObIndexBuilderUtil::set_index_table_columns(*create_index_arg, *data_table_schema, index_schema))) {
            LOG_WARN("[PRE_SPLIT] fail to set index table columns", K(ret));
          } else if (OB_FAIL(check_table_can_do_pre_split(index_schema))) {
            LOG_WARN("[PRE_SPLIT] fail to check table info", K(ret), K(index_schema));
          } else if (OB_FAIL(do_pre_split_global_index(database_name, *data_table_schema, index_schema, index_schema))) {
            LOG_WARN("[PRE_SPLIT] fail to get new index table split range", K(ret), K(tenant_id), K(database_name));
          } else {
            LOG_DEBUG("[PRE_SPLIT] success pre split index schema", K(index_schema));
          }
          if (OB_SUCC(ret) && index_schema.is_partitioned_table()) {
            if (OB_FAIL(create_index_arg->index_schema_.assign(index_schema))) {
              LOG_WARN("[PRE_SPLIT] fail to assign index schema", K(ret), K(index_schema));
            }
          } else if (ret == OB_NOT_SUPPORTED) {
            ret = OB_SUCCESS;
            LOG_INFO("[PRE_SPLIT] not support pre-split schema. do nothing", K(ret), K(index_schema));
          }
        }
      }
    }
  }
  return ret;
}

/*
  1. 在预分裂创建全局索引的场景，外层ddl_type可以不传，其他场景需要传
  2. 在预分裂主表或者已存在的全局索引表时，外层传入的data_table_schema可以是主表和全局索引表的的new_table_schema
*/
int ObPartitionPreSplit::do_table_pre_split_if_need(
    const ObString &db_name,
    const ObDDLType ddl_type,
    const bool is_building_global_index,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &ori_table_schema,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = new_table_schema.get_tenant_id();
  uint64_t current_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, current_data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (current_data_version < DATA_VERSION_4_3_4_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("current data version less than 4.4.0.0 is not support", K(ret), K(current_data_version));
  } else if (!data_table_schema.is_auto_partitioned_table()) {
    // skip // not auto split partition table
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("[PRE_SPLIT] table is not auto part table, no need to pre split", K(ret));
  } else if (OB_UNLIKELY(db_name.empty() || OB_INVALID_ID == tenant_id || ddl_type > ObDDLType::DDL_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument", K(ret), K(db_name), K(tenant_id), K(ddl_type));
  } else if (OB_FAIL(check_table_can_do_pre_split(new_table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to check table info", K(ret), K(new_table_schema));
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_NOT_SUPPORTED) {
      ret = OB_SUCCESS; // some ddl type no need to do pre split, just return success.
      LOG_INFO("not support pre-split schema. do nothing", K(ret), K(new_table_schema));
    }
  } else if (is_building_global_index) {
    // 1. create index global (partition or not partition table)
    // 2. alter table add index global (partition or not partition table)
    ObTableSchema ori_index_table_schema;
    if (OB_FAIL(ori_index_table_schema.assign(new_table_schema))) {
      LOG_WARN("fail to assign schema", K(ret), K(new_table_schema));
    } else if (OB_FAIL(do_pre_split_global_index(db_name, data_table_schema, ori_index_table_schema, new_table_schema))) {
      LOG_WARN("fail to do pre split global index", K(ret),
        K(db_name), K(data_table_schema), K(ori_index_table_schema), K(new_table_schema));
    }
  } else {
    // 1. alter main table
    // 2. rebuild global index partition/ none partition table, ddl_type will be DDL_CREATE_INDEX
    bool need_pre_split = false;
    bool is_global_index = new_table_schema.is_global_local_index_table() || new_table_schema.is_global_index_table();
    if (is_global_index && ddl_type == DDL_CREATE_INDEX && new_table_schema.get_auto_part_size() > 0) {
      need_pre_split = true;
    } else if (OB_FALSE_IT(need_pre_split = is_supported_pre_split_ddl_type(ddl_type))) {
      LOG_WARN("[PRE_SPLIT] fail to check need pre split partition", K(ret), K(ddl_type), K(new_table_schema));
    }
    if (!need_pre_split) {
      // do nothing
      LOG_INFO("[PRE_SPLIT] no need to do pre split type", K(ddl_type), K(new_table_schema));
    } else if (OB_FAIL(do_pre_split_main_table(db_name, ori_table_schema, new_table_schema))) {
      LOG_WARN("fail to do pre split global index", K(ret),
        K(db_name), K(ori_table_schema), K(new_table_schema));
    }
  }
  return ret;
}

/*
  description:
    1. create index global (partition or not partition table)
    2. alter table add index global (partition or not partition table)
*/
int ObPartitionPreSplit::do_pre_split_global_index(
    const ObString &db_name,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &ori_index_schema,
    ObTableSchema &new_index_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema inc_partition_schema;
  ObArray<TabletIDSize> tablets_size_array;
  ObTabletID source_tablet_id(OB_INVALID_ID);
  int64_t physical_size = 0;
  int64_t data_table_physical_size = 0;
  int64_t split_num = 0;
  int64_t split_size = new_index_schema.get_part_option().get_auto_part_size();
  const int64_t tenant_id = new_index_schema.get_tenant_id();
  const bool need_generate_part_name = true;
#ifdef ERRSIM
  split_size = 30;
#endif
  DEBUG_SYNC(START_DDL_PRE_SPLIT_PARTITION);
  if (OB_FAIL(get_estimated_table_size(data_table_schema, new_index_schema, physical_size))) { // estimate global index table size
    LOG_WARN("[PRE_SPLIT] fail to get create table size", K(ret), K(new_index_schema));
  } else if (OB_FAIL(get_exist_table_size(data_table_schema, tablets_size_array))) {  // get data table size
    LOG_WARN("[PRE_SPLIT] fail to get data table size", K(ret), K(data_table_schema));
  }
  if (OB_SUCC(ret)) {
    // calculate data table size.
    for (int64_t i = 0; i < tablets_size_array.count(); ++i) {
      data_table_physical_size += tablets_size_array[i].second;
    }
  }
  LOG_DEBUG("[PRE_SPLIT] size for create global index",
    K(ret), K(split_size), K(data_table_physical_size), K(physical_size), K(tablets_size_array));

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(get_split_num(physical_size, split_size, split_num))) {
  } else if (split_num < 2) {
    // do nothing
    LOG_DEBUG("[PRE_SPLIT] tablet physical size no reach split limited, no need to split",
      K(physical_size), K(split_size), K(split_num), K(tablets_size_array));
  } else if (OB_FAIL(build_global_index_pre_split_ranges( // none partition table, partition key should be rowkey
      tenant_id,
      data_table_physical_size,
      split_num,
      db_name,
      new_index_schema,
      data_table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build pre split ranges",
      K(ret), K(source_tablet_id), K(tenant_id), K(physical_size));
  } else if (OB_FAIL(build_split_tablet_partition_schema(
      tenant_id,
      source_tablet_id,
      need_generate_part_name,
      inc_partition_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build one split tablet part schema", K(ret), K(tenant_id), K(source_tablet_id));
  } else if (OB_FAIL(build_new_table_schema(
      ori_index_schema,
      inc_partition_schema,
      new_index_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build new table schema", K(ret), K(inc_partition_schema));
  }
  return ret;
}
/*
  description:
    1. alter main table
    2. rebuild global index partition/ none partition table, ddl_type will be DDL_CREATE_INDEX
*/
int ObPartitionPreSplit::do_pre_split_main_table(
    const ObString &db_name,
    const ObTableSchema &ori_table_schema,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = new_table_schema.get_tenant_id();
  int64_t split_size = new_table_schema.get_part_option().get_auto_part_size();
#ifdef ERRSIM
  split_size = 200;
#endif
  DEBUG_SYNC(START_DDL_PRE_SPLIT_PARTITION);
  if (OB_FAIL(build_table_pre_split_schema(
      tenant_id,
      split_size,
      db_name,
      ori_table_schema,
      new_table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to build split table schema", K(ret),
      K(tenant_id), K(ori_table_schema), K(new_table_schema));
  }
  LOG_DEBUG("[PRE_SPLIT] finish do pre split", K(ret), K(ori_table_schema), K(new_table_schema));
  return ret;
}

/*
  function description:
    constraint check:
    1. should be range partition and partition level > 0, or table is auto split partition table (auto split partition table / auto split none partition table)
    2. not support interval partition
    3. not support none user table/global index
    4. partition key is prefix of rowkey
*/
int ObPartitionPreSplit::check_table_can_do_pre_split(
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption &part_option = table_schema.get_part_option();
  const bool is_user_table = table_schema.is_user_table();
  const bool is_global_index_table =
    table_schema.is_global_local_index_table() || table_schema.is_global_index_table();

  if (OB_UNLIKELY(table_schema.is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[PRE_SPLIT] not support heap table", K(ret), K(table_schema));
  } else if (OB_UNLIKELY(!is_user_table && !is_global_index_table)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[PRE_SPLIT] not support none user table/global index", K(ret), K(is_user_table), K(is_global_index_table));
  } else if (table_schema.get_part_level() > PARTITION_LEVEL_ONE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[PRE_SPLIT] not support part level > 1 currently", K(ret), K(table_schema.get_part_level()));
  } else if (part_option.is_interval_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[PRE_SPLIT] not support interval partition", K(ret), K(part_option));
  } else if (!part_option.is_range_part()) { // partiton by range/range columns type
    if (is_global_index_table && part_option.is_hash_part()) {
      /* case: create index idx1 on t1(c1), default partition type is hash */
      LOG_INFO("[pre_split] support global index table without partition by", K(part_option));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[PRE_SPLIT] not support none range partition table", K(ret), K(part_option));
    }
  }
  if (OB_SUCC(ret)) {
    // if user/global table is auto split table, partition level may be zero but partition key not null
    // like:
    //    1. alter table xxx partition by range(xxx) size(), partition key should be include in rowkey
    //    2. alter table xxx partition by range() size(), partition key is rowkey by default.
    // For these two scenarios of altering automatic partition properties, the preceding RS has already performed the verification,
    // so it's unnecessary to check the primary key prefix again here.
    // but the following case should be checked, like:
    //    3. alter table t1 partition by range(xxx) (partitions...)
    //    4. alter table t1 add index idx1(xxx) partition by range(xxx) (partitions...)
    // For scenarios involving modifying the main table partition or creating an index table partition,
    // it is necessary to verify whether the partition key is a prefix of the primary key.
    bool is_prefix = true;
    if (table_schema.is_partitioned_table() &&
        OB_FAIL(table_schema.is_partition_key_match_rowkey_prefix(is_prefix))) {
      LOG_WARN("[PRE_SPLIT] fail to check partition key match prefix rowkey", K(ret));
    } else if (!is_prefix) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[PRE_SPLIT] partition key not prefix of rowkey is not support", K(ret));
    }
  }
  LOG_WARN("[PRE_SPLIT] table is partition", K(table_schema.is_partitioned_table()));
  return ret;
}

int ObPartitionPreSplit::generate_tablet_and_part_id(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] invalid ddl service", K(ret), KP(ddl_service_));
  } else if (OB_FAIL(ddl_service_->generate_object_id_for_partition_schema(table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to generate object_id for partition schema", K(ret), K(table_schema));
  } else if (OB_FAIL(ddl_service_->generate_tablet_id(table_schema))) {
    LOG_WARN("[PRE_SPLIT] fail to fetch new table id", K(table_schema), K(ret));
  }
  return ret;
}

int ObPartitionPreSplit::build_table_pre_split_schema(
    const int64_t tenant_id,
    const int64_t split_size,
    const ObString &db_name,
    const ObTableSchema &ori_table_schema,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<TabletIDSize> tablet_size_array;

  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || split_size <= 0 || db_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument.", K(ret), K(tenant_id), K(split_size), K(db_name));
  } else if (OB_FAIL(get_exist_table_size(ori_table_schema, tablet_size_array))) {
    LOG_WARN("[PRE_SPLIT] fail to get exist table size.", K(ret), K(ori_table_schema));
  } else if (tablet_size_array.count() <= 0) {
    // do nothing
  } else {
    HEAP_VAR(ObTableSchema, inc_partition_schema) {
    HEAP_VAR(ObTableSchema, all_partition_schema) {
    ObArray<ObTabletID> split_tablet_ids;
    int64_t split_num = 0;
    bool has_modify_partition_rule = false;

    if (OB_FAIL(check_is_modify_partition_rule(table_schema, ori_table_schema, has_modify_partition_rule))) {
      LOG_WARN("[PRE_SPLIT] fail to check modify partition rule", K(ret));
      // 1. pre split all table partitions
    } else if (has_modify_partition_rule) {
      const bool need_generate_part_name = true;
      int64_t data_table_phycical_size = 0;
      ObTabletID split_source_id(OB_INVALID_ID);
      for (int64_t i = 0; i < tablet_size_array.count(); ++i) {
        data_table_phycical_size += tablet_size_array[i].second;
      }
      ObSplitSampler range_builder;
      ObArray<ObNewRange> tmp_ranges;
      ObArray<ObNewRange> part_columns_range;
      ObArray<ObString> part_columns_name;
      ObRowkey src_l_bound_val;
      ObRowkey src_h_bound_val;
      ObObj obj_l_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj obj_h_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];

      if (FALSE_IT(get_split_num(data_table_phycical_size, split_size, split_num))) {
      } else if (split_num < 2) {
        LOG_DEBUG("[PRE_SPLIT] tablet physical size no reach split limited, no need to split",
            K(data_table_phycical_size), K(split_size), K(split_num));
      } else if (OB_FAIL(get_partition_columns_name(table_schema, part_columns_name))) {
        LOG_WARN("[PRE_SPLIT] fail to get partition columns name", K(ret));
      } else if (OB_FAIL(get_partition_columns_range(table_schema, part_columns_name.count(), part_columns_range))) {
        LOG_WARN("[PRE_SPLIT] fail to get partition ranges", K(ret));
      } else if (OB_FAIL(range_builder.query_ranges(tenant_id, db_name, ori_table_schema,
          part_columns_name,
          part_columns_range,
          split_num,
          data_table_phycical_size,
          allocator_,
          tmp_ranges))) {
        LOG_WARN("[PRE_SPLIT] fail to query ranges", K(ret), K(part_columns_name), K(part_columns_range));
      } else if (OB_FAIL(get_table_partition_bounder(table_schema, part_columns_name.size(),
          src_l_bound_val, src_h_bound_val, obj_l_buf, obj_h_buf))) {
      } else if (OB_FAIL(check_and_get_split_range(
          src_l_bound_val, src_h_bound_val, part_columns_name.size(), tmp_ranges))) {
        LOG_WARN("[PRE_SPLIT] fail to check split range.", K(ret), K(tmp_ranges));
      } else if (OB_FAIL(build_split_tablet_partition_schema(
          tenant_id,
          split_source_id,
          need_generate_part_name,
          inc_partition_schema))) {
        LOG_WARN("[PRE_SPLIT] fail to build one split tablet part schema",  K(ret), K(tenant_id));
      }
      LOG_DEBUG("[PRE_SPLIT] pre split info",
        K(ret), K(data_table_phycical_size), K(split_size), K(split_num), K(tmp_ranges));
    } else {
      const bool need_generate_part_name = false;
      // 2. pre split every single tablet
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_size_array.count(); ++i) {
        const ObTabletID &tablet_id = tablet_size_array[i].first;
        const uint64_t physical_size = tablet_size_array[i].second;
        if (FALSE_IT(get_split_num(physical_size, split_size, split_num))) {
        } else if (split_num < 2) { // do nothing
          LOG_DEBUG("[PRE_SPLIT] tablet physical size no reach split limited, no need to split",
            K(tablet_id), K(physical_size), K(split_num));
        } else if (!tablet_id.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[PRE_SPLIT] invalid tablet id.", K(ret), K(tablet_id));
        } else if (OB_FAIL(build_tablet_pre_split_ranges(
            tenant_id,
            physical_size,
            split_num,
            tablet_id,
            db_name,
            ori_table_schema,
            table_schema))) {
          LOG_WARN("[PRE_SPLIT] fail to build pre split ranges",
            K(ret), K(tablet_id), K(tenant_id), K(physical_size));
        } else if (OB_FAIL(build_split_tablet_partition_schema(
            tenant_id,
            tablet_id,
            need_generate_part_name,
            inc_partition_schema))) {
          LOG_WARN("[PRE_SPLIT] fail to build one split tablet part schema",  K(ret), K(tenant_id), K(tablet_id));
        } else if (split_ranges_.count() > 0 && OB_FAIL(split_tablet_ids.push_back(tablet_id))) {
          LOG_WARN("[PRE_SPLIT] fail to push split tablet id to array", K(ret), K(tablet_id));
        } else {
          LOG_DEBUG("[PRE_SPLIT] pre split info",
            K(ret), K(tablet_id), K(physical_size), K(split_size), K(split_num), K(split_ranges_), K(inc_partition_schema));
        }
      }
    }
    // generate new part id / part idx / tablet id ...
    if (OB_SUCC(ret)) {
      if (!ori_table_schema.is_partitioned_table() || has_modify_partition_rule) {
         // 1. alter main none partition table or rebuild global index none partition table
         // 2. modify partition rule, we have to rebuild all partition schema
        if (OB_FAIL(build_new_partition_table_schema(
            ori_table_schema,
            inc_partition_schema,
            table_schema))) {
          LOG_WARN("[PRE_SPLIT] fail to build new index table schema.", K(ret), K(table_schema));
        }
      } else {
        if (OB_FAIL(rebuild_partition_table_schema(
            // 1. alter main partition table or rebuild global index partition table
            split_tablet_ids,
            ori_table_schema,
            inc_partition_schema,
            all_partition_schema,
            table_schema))) {
          LOG_WARN("[PRE_SPLIT] fail to fill full tablet partition schema",
            K(ret), K(ori_table_schema), K(inc_partition_schema));
        }
      }
    }
    }
    }
  }

  return ret;
}
/*
function description：
  1. merge old part and inc part
  2. generate all partition new sort part idx.
  3. generate all parititon new part name

ATTENTION!!
  due to part_idx is indicated sort order of val of all partitions, like part idx 0 high bound is
  part idx 1 low bound and so on :

  |-- part idx0 --|---part idx 1 --|-- part idx 2 --|
  low0       high0/low1       high1/low2        high2

  so part idx should be keep in order even thought there are have be splited.
  for example, if we have three partitions like partition a/b/c, part idx is 1/2/3，if partition b split into
  two partition like b1 and b2，and b1 range is smaller than b2, than the part idx after pre-split
  should be like : a->1, b1->2, b2->3, c->4
*/
int ObPartitionPreSplit::generate_all_partition_schema(
    const ObIArray<ObTabletID> &split_tablet_ids,
    const ObTableSchema &ori_table_schema,
    const ObPartitionSchema &inc_partition_schema,
    ObPartitionSchema &all_partition_schema)
{
  int ret = OB_SUCCESS;

  ObPartition **ori_part_array = ori_table_schema.get_part_array();
  ObPartition **inc_part_array = inc_partition_schema.get_part_array();
  int64_t ori_part_num = ori_table_schema.get_partition_num();
  int64_t inc_part_num = inc_partition_schema.get_partition_num();
  int64_t split_tablet_num = split_tablet_ids.count();
  int64_t all_part_num = ori_part_num + inc_part_num - split_tablet_num;

  if (OB_ISNULL(ori_part_array) || OB_ISNULL(inc_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] part array is null", K(ret), K(ori_table_schema), K(inc_partition_schema));
  } else {
    int64_t cur_part_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ori_part_num && cur_part_idx < all_part_num; ++i) { // origin
      ObPartition *ori_part = ori_part_array[i];
      if (OB_ISNULL(ori_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] invalid partition.", K(ret), KP(ori_part));
      } else {
        const ObTabletID &source_tablet_id = ori_part->get_tablet_id();
        // check this partition is in spliting
        PartLowBound low_bound_array;
        for (int64_t j = 0; OB_SUCC(ret) && j < inc_part_num; ++j) { // find split part of source_tablet_id
          ObPartition *inc_part = inc_part_array[j];
          if (OB_ISNULL(inc_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[PRE_SPLIT] invalid part", K(ret), KP(inc_part));
          } else if (source_tablet_id == inc_part->get_split_source_tablet_id()) {
            const ObRowkey &low_bound_val = inc_part->get_low_bound_val();
            if (OB_FAIL(low_bound_array.push_back(std::pair<ObRowkey, ObPartition*>(low_bound_val, inc_part)))) {
              LOG_WARN("fail to push part to array", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (low_bound_array.count() > 0) { // ori_part have been split
            lib::ob_sort(low_bound_array.begin(), low_bound_array.end(), PartRangeCmpFunc()); // sort part
            for (int64_t k = 0; OB_SUCC(ret) && k < low_bound_array.count(); ++k) {
              ObPartition *part = low_bound_array.at(k).second; // inc partition
              part->set_part_idx(cur_part_idx);
              if (OB_FAIL(all_partition_schema.add_partition(*part))) {
                LOG_WARN("[PRE_SPLIT] fail to add new partition", K(ret));
              } else {
                cur_part_idx++;
              }
            }
          } else {   // ori_part not been split
            ObPartition tmp_part;
            if (OB_FAIL(tmp_part.assign(*ori_part))) {
              LOG_WARN("[PRE_SPLIT] fail to assign original part", K(ret), K(cur_part_idx));
            } else if (FALSE_IT(tmp_part.set_part_idx(cur_part_idx))){
            } else if (FALSE_IT(tmp_part.set_is_empty_partition_name(false))) { // not generate new name for not split part
            } else if (OB_FAIL(all_partition_schema.add_partition(tmp_part))) {
              LOG_WARN("[PRE_SPLIT] fail to add new partition", K(ret), K(tmp_part), K(cur_part_idx));
            } else {
              cur_part_idx++;
            }
          }
        }
      }
    }
    // verify max part idx
    if (OB_SUCC(ret)) {
      if (cur_part_idx != all_part_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] fail to gen part idx, max part idx not equal to all part num",
          K(ret), K(cur_part_idx), K(all_part_num), K(all_partition_schema));
      } else if (all_part_num != all_partition_schema.get_partition_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] fail to gen part idx, partition num not match",
          K(ret), K(all_part_num), K(all_partition_schema));
      }
    }
    // generate all part name
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ddl_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] invalid ddl service", K(ret), KP(ddl_service_));
      } else if (OB_FAIL(ddl_service_->fill_part_name(ori_table_schema, all_partition_schema))) {
        LOG_WARN("[PRE_SPLIT] fail to build split table schema", K(ret), K(all_partition_schema));
      }
    }
  }

  return ret;
}

int ObPartitionPreSplit::get_and_set_part_column_info(
    ObTableSchema &table_schema,
    char *rowkey_column_buf,
    int64_t &buf_pos,
    int64_t &column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(rowkey_column_buf) || buf_pos != 0 )) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument.", K(ret), K(rowkey_column_buf), K(buf_pos));
  } else {
    column_cnt = 0;
    int64_t part_key_pos = 0;
    const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); i++) {
      const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(i);
      const int64_t column_id = rowkey_column->column_id_;
      ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] col_schema is nullptr", K(ret), K(column_id), K(table_schema));
      } else if (table_schema.is_index_table() && col_schema->get_index_position() <= 0) {
        // skip rowkey column from duplicate main table rowkey.
      } else if (OB_FAIL(table_schema.add_partition_key(col_schema->get_column_name_str()))) {
        LOG_WARN("[PRE_SPLIT] Failed to add partition key", K(ret), K(col_schema->get_column_name_str()));
      } else {
        const ObString &column_name = col_schema->get_column_name_str();
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[PRE_SPLIT] column name null, unexpected", K(ret), K(column_name));
        } else if (0 == i && OB_FAIL(databuff_printf(
            rowkey_column_buf,
            OB_MAX_SQL_LENGTH,
            buf_pos,
            "%.*s",
            static_cast<int>(column_name.length()),
            column_name.ptr()))) {
          LOG_WARN("[PRE_SPLIT] fail to printf rowkey_column_buf", K(ret), K(i), K(column_name));
        } else if (0 != i && OB_FAIL(databuff_printf(
            rowkey_column_buf,
            OB_MAX_SQL_LENGTH,
            buf_pos,
            ", %.*s",
            static_cast<int>(column_name.length()),
            column_name.ptr()))) {
          LOG_WARN("[PRE_SPLIT] fail to printf rowkey_column_buf", K(ret), K(i), K(column_name));
        } else {
          column_cnt++;
        }
      }
    }
  }
  return ret;
}

/*
  预分裂后，因为会分裂为多个分区，new_table_schema 一定要填分区键
  如果分区键是double/float等类型，需要把分区类型改成 range column
*/
int ObPartitionPreSplit::build_new_table_schema(
    const ObTableSchema &ori_table_schema,
    ObPartitionSchema &partition_schema,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  ObPartitionOption &part_option = partition_schema.get_part_option();
  const ObPartitionLevel ori_part_level = new_table_schema.get_part_level();
  const ObPartitionOption &ori_part_option = new_table_schema.get_part_option();
  const int64_t partition_num = partition_schema.get_partition_num();
  // origin table is not partition table. we should build new partition option
  // including main table and global index table.
  const bool need_to_modify_partition = partition_num > 0;
  if (need_to_modify_partition) {
    if (OB_FAIL(part_option.assign(ori_part_option))) {
      LOG_WARN("[PRE_SPLIT] fail to assign part option", K(ret), K(ori_part_option));
    } else if (!ori_table_schema.is_partitioned_table()) {
      if (!part_option.get_part_func_expr_str().empty()) {
        // skip // part func expr is ready
      } else { // extract partition func expr from new table
        ObString part_func_expr_str;
        char *rowkey_column_buf = nullptr;
        int64_t rowkey_str_buf_len = 0;
        int64_t actual_rowkey_column_cnt = 0; // rowkey column cnt expect duplicate main table rowkey
        if (OB_ISNULL(rowkey_column_buf = static_cast<char *>(allocator_.alloc(OB_MAX_SQL_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[PRE_SPLIT] fail to alloc new_part_func_expr", K(ret));
        } else if (OB_FAIL(get_and_set_part_column_info(new_table_schema,
            rowkey_column_buf,
            rowkey_str_buf_len,
            actual_rowkey_column_cnt))) {
          LOG_WARN("[PRE_SPLIT] fail to get rowkey column buf.", K(ret));
        } else if (FALSE_IT(part_func_expr_str.assign_ptr(
          rowkey_column_buf,
          static_cast<int32_t>(rowkey_str_buf_len)))) {
        } else if (OB_FAIL(part_option.set_part_expr(part_func_expr_str))) {
          LOG_WARN("[PRE_SPLIT] set part expr failed", K(ret), K(part_func_expr_str));
        } else {
          ObPartitionFuncType part_type = actual_rowkey_column_cnt > 1 ?
            PARTITION_FUNC_TYPE_RANGE_COLUMNS :
            PARTITION_FUNC_TYPE_RANGE;
          part_option.set_part_func_type(part_type);
        }
      }
      if (OB_SUCC(ret)) {
        partition_schema.set_part_level(PARTITION_LEVEL_ONE);
      }
    } else { // set ori partition level
      partition_schema.set_part_level(ori_part_level);
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(part_option.set_part_num(partition_num))) {
    } else if (OB_FAIL(new_table_schema.assign_partition_schema(partition_schema))) { // cover old
      LOG_WARN("[PRE_SPLIT] fail to assign partition schema", K(ret), K(partition_schema));
    } else if (OB_FAIL(modify_partition_func_type_if_need(new_table_schema))) {
      LOG_WARN("[PRE_SPLIT] fail to modify partition type", K(ret), K(new_table_schema));
    }
    LOG_DEBUG("[PRE_SPLIT] build new schema finish", K(ori_table_schema), K(new_table_schema));
  }
  return ret;
}

/*
  如果分区键包含double等类型，需要把分区类型改成range column
*/
int ObPartitionPreSplit::modify_partition_func_type_if_need(ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  if (!new_table_schema.is_partitioned_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, is should be partition table here", K(ret), K(new_table_schema));
  } else {
    ObArray<uint64_t> part_key_ids;
    const ObPartitionKeyInfo &partition_keys = new_table_schema.get_partition_key_info();
    if (partition_keys.is_valid() && OB_FAIL(partition_keys.get_column_ids(part_key_ids))) {
      LOG_WARN("fail to get column ids from partition keys", K(ret));
    } else {
      const ObColumnSchemaV2 *column_schema = NULL;
      ObPartitionFuncType part_type = part_key_ids.count() > 1 ?
        PARTITION_FUNC_TYPE_RANGE_COLUMNS :
        PARTITION_FUNC_TYPE_RANGE;
      for (int64_t i = 0; OB_SUCC(ret) && part_type != PARTITION_FUNC_TYPE_RANGE_COLUMNS
          && i < part_key_ids.count(); ++i) {
        uint64_t part_key_id = part_key_ids.at(i);
        if (part_key_id >= OB_MIN_SHADOW_COLUMN_ID || part_key_id < OB_APP_MIN_COLUMN_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected part key id", K(ret), K(part_key_id));
        } else if (OB_FALSE_IT(column_schema = new_table_schema.get_column_schema(part_key_id))) {
        } else if (OB_ISNULL(column_schema)) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("column schema not exist", K(ret), K(new_table_schema), K(part_key_id));
        } else {
          ObObjType column_type = column_schema->get_data_type();
          if (ObResolverUtils::is_partition_range_column_type(column_type)) {
            part_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
            new_table_schema.get_part_option().set_part_func_type(part_type);
          }
        }
      }
    }
  }
  return ret;
}

/*
  目前只有创建全局索引表，need_generate_part_name才会为true
  如果是重建全局索引表，need_generate_part_name为false
*/
int ObPartitionPreSplit::build_split_tablet_partition_schema(
    const int64_t tenant_id,
    const ObTabletID &source_tablet_id,
    const bool need_generate_part_name,
    ObPartitionSchema &inc_partition_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument", K(ret), K(tenant_id));
  } else if (split_ranges_.count() <= 0) {
    // skip // empty split range
  } else {
    int64_t partition_count = split_ranges_.count();
    share::schema::ObPartition new_partition;
    int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
    char part_name[OB_MAX_PARTITION_NAME_LENGTH];
    ObString part_name_str;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < partition_count; ++idx) {
      const ObRowkey& low_bound_val = split_ranges_.at(idx).get_start_key();
      const ObRowkey& high_bound_val = split_ranges_.at(idx).get_end_key();
      if (low_bound_val > high_bound_val) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] fail to build split table schema", K(ret), K(low_bound_val), K(high_bound_val));
      } else if (OB_FAIL(new_partition.set_high_bound_val(high_bound_val))) {
        LOG_WARN("[PRE_SPLIT] failed to set high_bound_val", K(ret));
      } else if (OB_FAIL(new_partition.set_low_bound_val(low_bound_val))) {
        LOG_WARN("[PRE_SPLIT] failed to set low_bound_val", K(ret));
      } else {
        new_partition.set_is_empty_partition_name(true); // generate new part name for split part
        new_partition.set_tenant_id(tenant_id);
        new_partition.set_split_source_tablet_id(source_tablet_id);
        new_partition.set_partition_type(PartitionType::PARTITION_TYPE_NORMAL);
        new_partition.set_part_idx(idx); // change later
        if (need_generate_part_name) {
          int64_t pos = 0;
          if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", ++max_part_id))) {
            LOG_WARN("failed to constrate partition name", K(ret), K(max_part_id));
          } else if (OB_FALSE_IT(part_name_str.assign(part_name, static_cast<int32_t>(pos)))) {
          } else if (OB_FAIL(new_partition.set_part_name(part_name_str))) {
            LOG_WARN("failed to set partition name", K(ret), K(part_name_str));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inc_partition_schema.add_partition(new_partition))) {
          LOG_WARN("[PRE_SPLIT] failed to add partition", K(ret), K(tenant_id), K(source_tablet_id));
        }
      }
    }
  }
  return ret;
}
/*
  description:
    1. build index split range
  Attention:
    index_schema should be old index schema, because new index schema
    has not been created in create index case
*/
int ObPartitionPreSplit::build_global_index_pre_split_ranges(
    const int64_t tenant_id,
    const int64_t data_table_phycical_size,
    const int64_t split_num,
    const ObString &db_name,
    const ObTableSchema &index_schema,
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  split_ranges_.reset(); // reset range
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID
      || data_table_phycical_size <= 0
      || split_num <= 0
      || db_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument.",
      K(ret), K(tenant_id), K(data_table_phycical_size), K(split_num), K(db_name));
  } else if (index_schema.get_data_table_id() != table_schema.get_table_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] table id not equal.",
      K(ret), K(index_schema.get_data_table_id()), K(table_schema.get_table_id()));
  } else {
    ObSplitSampler range_builder;
    ObArray<ObNewRange> tmp_ranges;
    ObArray<ObNewRange> part_columns_range;
    ObArray<ObString> part_columns_name;
    ObRowkey src_l_bound_val;
    ObRowkey src_h_bound_val;
    ObObj obj_l_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
    ObObj obj_h_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
    if (OB_FAIL(get_partition_columns_name(index_schema, part_columns_name))) {
      LOG_WARN("[PRE_SPLIT] fail to get partition columns range", K(ret));
    } else if (OB_FAIL(get_partition_columns_range(index_schema, part_columns_name.count(), part_columns_range))) {
      LOG_WARN("[PRE_SPLIT] fail to get partition ranges", K(ret));
    } else if (OB_FAIL(range_builder.query_ranges(tenant_id, db_name, table_schema,
        part_columns_name,
        part_columns_range,
        split_num,
        data_table_phycical_size,
        allocator_,
        tmp_ranges))) {
    } else if (tmp_ranges.count() <= 0) {
      // empty range, do nothing
    } else if (OB_FAIL(get_table_partition_bounder(index_schema, part_columns_name.count(),
        src_l_bound_val, src_h_bound_val, obj_l_buf, obj_h_buf))) {
      LOG_WARN("[PRE_SPLIT] fail ti get table partition bounder", K(ret), K(table_schema));
    } else if (OB_FAIL(check_and_get_split_range(
        src_l_bound_val, src_h_bound_val, part_columns_name.size(), tmp_ranges))) {
      LOG_WARN("[PRE_SPLIT] fail to check split range.", K(ret), K(tmp_ranges));
    }
  }
  return ret;
}

/*
  需要按照table_schema的分区范围，指定每个column的范围（table_schema的分区键可能有多列，
  并且最后一个分区的范围可能是[maxvalue, 100] 或者 [100, maxvalue]这种形式，需要考虑到。
*/
int ObPartitionPreSplit::get_partition_columns_range(
    const ObTableSchema &table_schema,
    const int64_t part_columns_cnt,
    ObIArray<ObNewRange> &part_range)
{
  int ret = OB_SUCCESS;
  ObArray<ObNewRange> orig_part_range;
  if (table_schema.is_partitioned_table()
      && OB_FAIL(get_partition_ranges(table_schema, orig_part_range))) {
    LOG_WARN("fail to get partition ranges", K(ret), K(table_schema));
  } else if (orig_part_range.count() < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition range", K(ret), K(table_schema));
  }
  if (OB_SUCC(ret)) {
    const int64_t ori_part_range_cnt = orig_part_range.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_columns_cnt; ++i) {
      const int64_t part_key_length = 1;
      ObNewRange tmp_range;
      ObRowkey low_key_bound;
      ObRowkey high_key_bound;
      if (ori_part_range_cnt > 0) {
        ObNewRange &last_part_range = orig_part_range.at(ori_part_range_cnt - 1);
        high_key_bound.assign(&last_part_range.end_key_.get_obj_ptr()[i], part_key_length);
      } else {
        high_key_bound.set_max_row();
        high_key_bound.set_length(part_key_length);
      }
      low_key_bound.set_min_row();
      low_key_bound.set_length(part_key_length);
      tmp_range.start_key_ = low_key_bound;
      tmp_range.end_key_ = high_key_bound;
      tmp_range.border_flag_.set_inclusive_start();
      tmp_range.border_flag_.unset_inclusive_end();
      if (OB_FAIL(part_range.push_back(tmp_range))) {
        LOG_WARN("[PRE_SPLIT] fail to push back part column range", K(ret));
      }
    }
  }
  LOG_DEBUG("get partition columns range", K(ret), K(part_range));
  return ret;
}

int ObPartitionPreSplit::get_partition_ranges(
    const ObTableSchema &table_schema,
    ObIArray<ObNewRange> &part_range)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = table_schema.get_part_array();
  const int64_t partition_num = table_schema.get_partition_num();
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] part array is null, not expected", K(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) { // find split part of source_tablet_id
      ObPartition *partition = part_array[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] invalid part", K(ret), KP(partition), K(table_schema));
      } else {
        ObNewRange tmp_range;
        tmp_range.start_key_ = partition->get_low_bound_val();
        tmp_range.end_key_ = partition->get_high_bound_val();
        if (OB_FAIL(part_range.push_back(tmp_range))) {
          LOG_WARN("[PRE_SPLIT] fail to push back part range", K(ret), K(tmp_range), K(table_schema));
        }
      }
    }
  }
  return ret;
}
/*
description
  1. if partition column is double or float, dont not support yet
*/
int ObPartitionPreSplit::get_partition_columns_name(
    const ObTableSchema &table_schema,
    ObIArray<ObString> &part_columns)
{
  int ret = OB_SUCCESS;
  uint64_t rowkey_col_id = OB_INVALID_ID;
  part_columns.reset();

  if (table_schema.get_part_option().get_part_func_expr_str().empty()) {
    const common::ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
    const share::schema::ObColumnSchemaV2 *column_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_col_id))) {
        LOG_WARN("[PRE_SPLIT] failed to column id");
      } else if (rowkey_col_id >= OB_MIN_SHADOW_COLUMN_ID
          || rowkey_col_id < OB_APP_MIN_COLUMN_ID) {
        continue;
      } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(rowkey_col_id))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("[PRE_SPLIT] column schema not exist", K(ret), K(table_schema), K(rowkey_col_id));
      } else if (table_schema.is_index_table() && column_schema->get_index_position() <= 0) {
        // if table is index table, ignore duplicate rowkey from main table rowkey column
      } else if (OB_FAIL(part_columns.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("[PRE_SPLIT] failed to add string", K(ret));
      }
    }
  } else {
    ObString tmp_part_func_expr = table_schema.get_part_option().get_part_func_expr_str();
    ObArray<ObString> part_expr_strs;
    if (OB_FAIL(split_on(tmp_part_func_expr, ',', part_expr_strs))) {
      LOG_WARN("[PRE_SPLIT] fail to split func expr", K(ret), K(tmp_part_func_expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_expr_strs.count(); ++i) {
        if (OB_FAIL(part_columns.push_back(part_expr_strs.at(i).trim()))) {
          LOG_WARN("[PRE_SPLIT] fail to push back part func str.", K(ret), K(part_expr_strs));
        }
      }
    }
  }
  return ret;
}

/*
  获取分区表的特定tablet的边界，需要保证预分裂时原表的分区规则没发生变更的情况下，才需要走这个函数。
*/
int ObPartitionPreSplit::get_partition_table_tablet_bounder(
    const ObTableSchema &table_schema,
    const ObTabletID &source_tablet_id,
    ObRowkey &l_bound_val,
    ObRowkey &h_bound_val)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = table_schema.get_part_array();
  const int64_t partition_num =  table_schema.get_partition_num();
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[PRE_SPLIT] part array is null, not expected", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) { // find split part of source_tablet_id
      ObPartition *partition = part_array[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[PRE_SPLIT] invalid part", K(ret), KP(partition));
      } else if (source_tablet_id == partition->get_tablet_id()) {
        if (partition->get_high_bound_val().is_valid()) {
          h_bound_val = partition->get_high_bound_val();
        }
        const int64_t part_idx = partition->get_part_idx();
        const int64_t subpart_idx = OB_INVALID_ID;
        if (part_idx >= 1) {
          ObBasePartition *last_part = NULL;
          if (OB_FAIL(table_schema.get_part_by_idx(part_idx - 1, subpart_idx, last_part))) {
            LOG_WARN("[PRE_SPLIT] fail to get part by idx", K(ret), K(part_idx));
          } else if (OB_ISNULL(last_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[PRE_SPLIT] fail to get part by idx, null pointer", K(ret), KP(last_part));
          } else if (!last_part->get_high_bound_val().is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[PRE_SPLIT] fail to get part by idx, invalid high bound", K(ret), KPC(last_part));
          } else {
            l_bound_val = last_part->get_high_bound_val();
          }
        }
      }
    }
  }
  return ret;
}

/*
  if ddl is modifing partition rules, we will redefinition partition based on the low and high limited user had set.
  but the finial partition rules may be diff from user setting after pre-split
*/
int ObPartitionPreSplit::check_is_modify_partition_rule(
    const ObTableSchema &new_table_schema,
    const ObTableSchema &old_table_schema,
    bool &has_modify_partition_rule)
{
  int ret = OB_SUCCESS;

  has_modify_partition_rule = false;
  ObArray<ObString> new_part_names;
  ObArray<ObString> old_part_names;
  ObArray<ObNewRange> new_part_ranges;
  ObArray<ObNewRange> old_part_ranges;
  if (!new_table_schema.is_partitioned_table()) {
    // skip, only partition table should be check, new table is not partition table means not modifing partition rule
  } else if (!old_table_schema.is_partitioned_table()) {
    // ori table is auto split none partition table, and new table is auto split partition table,
    // so is must be modified partition rule
    has_modify_partition_rule = true;
  } else if (OB_FAIL(get_partition_columns_name(new_table_schema, new_part_names))) {
    LOG_WARN("[PRE_SPLIT] fail to get partition column name", K(ret));
  } else if (OB_FAIL(get_partition_columns_name(old_table_schema, old_part_names))) {
    LOG_WARN("[PRE_SPLIT] fail to get partition column name", K(ret));
  } else if (OB_FAIL(get_partition_ranges(new_table_schema, new_part_ranges))) {
    LOG_WARN("[PRE_SPLIT] fail to get partition range", K(ret));
  } else if (OB_FAIL(get_partition_ranges(old_table_schema, old_part_ranges))) {
    LOG_WARN("[PRE_SPLIT] fail to get partition range", K(ret));
  } else {
    // check modify partition rule
    if (new_part_names.count() != old_part_names.count()) {
      has_modify_partition_rule = true;
    } else if (new_part_ranges.count() != old_part_ranges.count()) {
      has_modify_partition_rule = true;
    } else {
      for (int64_t i = 0; !has_modify_partition_rule && i < new_part_ranges.count(); ++i) {
        if (new_part_ranges.at(i).start_key_ != old_part_ranges.at(i).start_key_) {
          has_modify_partition_rule = true;
        } else if (new_part_ranges.at(i).end_key_ != old_part_ranges.at(i).end_key_) {
          has_modify_partition_rule = true;
        }
      }
    }
    LOG_DEBUG("[PRE_SPLIT] check partition rule changed",
      K(ret), K(has_modify_partition_rule),
      K(old_part_names), K(new_part_names), K(old_part_ranges), K(new_part_ranges));
  }
  return ret;
}
/*
  function description:
    get tablet split range, ensure every range is in order
  Atention!!!!
    1. split range should be left close right open, like: [1, 10)
    2. as to auto split none partition table, range_builder.query_ranges should sample according to table primary key
    3. 采样时，如果此时new table schema 的分区键比 old table schema的分区键多，那么多出来的分区键的range，使用min_rowkey和max_rowkey填充，采样时会根据新分区键range范围采样过滤。
    4. part_columns 和 part_columns_range 一一对应，采样会根据range过滤，如果不想采样过滤则填min和max
*/
int ObPartitionPreSplit::build_tablet_pre_split_ranges(
    const int64_t tenant_id,
    const int64_t tablet_phycical_size,
    const int64_t split_num,
    const ObTabletID &source_tablet_id,
    const ObString &db_name,
    const ObTableSchema &old_table_schema,
    const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;

  split_ranges_.reset(); // reset range
  ObSplitSampler range_builder;
  ObArray<ObNewRange> tmp_ranges;
  ObArray<ObString> part_columns_name;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || tablet_phycical_size <= 0
      || split_num <= 0
      || !source_tablet_id.is_valid()
      || db_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument.",  K(ret), K(tenant_id),
      K(source_tablet_id), K(split_num), K(db_name), K(tablet_phycical_size));
  } else if (OB_FAIL(range_builder.query_ranges(tenant_id, db_name, old_table_schema,
      source_tablet_id,
      split_num,
      tablet_phycical_size,
      allocator_,
      tmp_ranges))) {
    LOG_WARN("[PRE_SPLIT] fail to get query ranges.", K(ret), K(tenant_id), K(source_tablet_id), K(old_table_schema));
  } else if (tmp_ranges.count() <= 0) {
    // empty range, do nothing
    LOG_DEBUG("[PRE_SPLIT] query ranges result is none, no need to split");
  } else if (OB_FAIL(get_partition_columns_name(new_table_schema, part_columns_name))) {
    LOG_WARN("[PRE_SPLIT] fail to get rowkey column name.", K(ret), K(part_columns_name));
  } else {
    /* if table is partition table, then get bounder. if not, regard bounder as min or max */
    ObRowkey src_l_bound_val;
    ObRowkey src_h_bound_val;
    ObObj obj_l_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
    ObObj obj_h_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
    if (old_table_schema.is_partitioned_table()) {
      if (OB_FAIL(get_partition_table_tablet_bounder(old_table_schema, source_tablet_id,
          src_l_bound_val,
          src_h_bound_val))) {
        LOG_WARN("[PRE_SPLIT] fail to get partition table tablet bounder", K(ret), K(source_tablet_id));
      }
    } else {
      if (OB_FAIL(get_table_partition_bounder(new_table_schema, part_columns_name.count(),
        src_l_bound_val, src_h_bound_val, obj_l_buf, obj_h_buf))) {
        LOG_WARN("[PRE_SPLIT] fail to get partition bounder", K(ret), K(new_table_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_and_get_split_range(
        src_l_bound_val, src_h_bound_val, part_columns_name.count(), tmp_ranges))) {
      LOG_WARN("[PRE_SPLIT] fail to check and get split range.",
        K(ret), K(tmp_ranges), K(src_l_bound_val), K(src_h_bound_val));
    }
  }
  return ret;
}

/*
  获取表的分区上下限，如果表是分区表，则获取min和max，如果不是分区表，则按预分裂的分区键数量构造min和max。
*/
int ObPartitionPreSplit::get_table_partition_bounder(
  const ObTableSchema &table_schema,
  const int64_t part_key_length,
  ObRowkey &src_l_bound_val,
  ObRowkey &src_h_bound_val,
  ObObj *obj_l_buf,
  ObObj *obj_h_buf)
{
  int ret = OB_SUCCESS;
  ObArray<ObNewRange> partition_ranges;
  if (OB_ISNULL(obj_l_buf) || OB_ISNULL(obj_h_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[PRE_SPLIT] invalid argument", K(ret), KP(obj_l_buf), KP(obj_h_buf));
  } else if (table_schema.is_partitioned_table() &&
      OB_FAIL(get_partition_ranges(table_schema, partition_ranges))) {
    LOG_WARN("[PRE_SPLIT] fail to get partition ranges", K(ret), K(table_schema));
  } else if (partition_ranges.count() > 0) {
    src_l_bound_val = partition_ranges[partition_ranges.count()-1].start_key_;
    src_h_bound_val = partition_ranges[partition_ranges.count()-1].end_key_;
  } else {
    for (int64_t i = 0; i < part_key_length; ++i) {
      ObObj tmp_l_buf;
      ObObj tmp_h_buf;
      tmp_l_buf.set_min_value();
      tmp_h_buf.set_max_value();
      obj_l_buf[i] = tmp_l_buf;
      obj_h_buf[i] = tmp_h_buf;
    }
    src_l_bound_val.assign(obj_l_buf, part_key_length);
    src_h_bound_val.assign(obj_h_buf, part_key_length);
  }
  return ret;
}

/*
  1. 需要考虑每个ranges之间除了首尾之外没有交集
  2. 需要确认每个range是否只有一个high bound val，如果这种场景则需要保证tmp_split_ranges的range是有序递增的。
*/
int ObPartitionPreSplit::check_and_get_split_range(
    const ObRowkey &src_l_bound_val,
    const ObRowkey &src_h_bound_val,
    const int64_t part_key_length,
    ObIArray<ObNewRange> &tmp_split_ranges)
{
  int ret = OB_SUCCESS;
  split_ranges_.reset(); // reset range

  for (int64_t idx = 0; OB_SUCC(ret) && idx < tmp_split_ranges.count(); ++idx) {
    ObNewRange &tmp_range = tmp_split_ranges.at(idx);
    const ObRowkey& l_bound_val = tmp_range.get_start_key();
    const ObRowkey& h_bound_val = tmp_range.get_end_key();
    if (l_bound_val > h_bound_val) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PRE_SPLIT] fail to check and get split range, invalid split range",
        K(ret), K(l_bound_val), K(h_bound_val));
    } else if ((l_bound_val.is_valid() && src_l_bound_val.is_valid() && l_bound_val < src_l_bound_val)
            || (h_bound_val.is_valid() && src_h_bound_val.is_valid() && h_bound_val > src_h_bound_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PRE_SPLIT] fail to check and get split range, out of source range.",
        K(ret),
        K(l_bound_val), K(h_bound_val),
        K(src_l_bound_val), K(src_h_bound_val));
    } else {
      // ensure that there is no intersection between the two ranges except the beginning and the end
      for (int64_t next_idx = idx + 1; OB_SUCC(ret) && next_idx < tmp_split_ranges.count(); ++next_idx) {
        const ObNewRange &tmp_next_range = tmp_split_ranges.at(next_idx);
        const ObRowkey& next_l_bound_val = tmp_next_range.get_start_key();
        const ObRowkey& next_h_bound_val = tmp_next_range.get_end_key();
        if (l_bound_val >= next_h_bound_val || h_bound_val <= next_l_bound_val) {
          continue; // is vaild range
        }
        ret = OB_ERR_UNEXPECTED; // invalid range
        LOG_WARN("[PRE_SPLIT] fail to check split range, ranges is illegal", K(ret),
          K(l_bound_val), K(h_bound_val),
          K(next_l_bound_val), K(next_h_bound_val));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(split_ranges_.push_back(tmp_split_ranges.at(idx)))) {
          LOG_WARN("[PRE_SPLIT] fail to push back range to split range array", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end oceanbase
} // end storage
