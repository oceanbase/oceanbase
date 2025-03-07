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
#include "ob_ddl_alter_auto_part_attr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;


ObAlterAutoPartAttrOp::ObAlterAutoPartAttrOp(rootserver::ObDDLService &ddl_service)
  : ddl_service_(&ddl_service)
{
}

int ObAlterAutoPartAttrOp::check_alter_table_partition_attr(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const share::schema::ObTableSchema &orig_table_schema,
    const bool is_oracle_mode,
    share::ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  const uint64_t tablegroup_id = orig_table_schema.get_tablegroup_id();
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  const AlterTableSchema &alter_table_schema =  alter_table_arg.alter_table_schema_;
  const ObPartitionOption &part_option = alter_table_schema.get_part_option();
  ObPartition **alter_part_array = alter_table_schema.get_part_array();
  if (is_long_running_ddl(ddl_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "There are several mutually exclusive DDL in single statement");
  } else if (obrpc::ObAlterTableArg::REPARTITION_TABLE == alter_table_arg.alter_part_type_) {
    if (is_oracle_mode && PARTITION_LEVEL_ZERO != part_level && OB_NOT_NULL(alter_part_array)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "re-partition a patitioned table");
    } else {
      ddl_type = ObDDLType::DDL_ALTER_PARTITION_BY;
    }
  } else {
    ddl_type = ObDDLType::DDL_NORMAL_TYPE;
  }
  if (OB_SUCC(ret)) {
    /* in the case like "alter table partition by range() size('unlimited')
      * the variables like enable_auto_split and auto_split_tablet_size will be reset to false or int64_t in ddl resolver
      * then we will regard it as DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE and deal with in alter_table_in_trans */
    if (part_option.get_part_func_expr_str().empty()) {
      /* alter partition by range() size(xxx) */
      ddl_type = ObDDLType::DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE;
    } else {
      /* alter partition by range(xxx) size(xxx) */
      if (OB_ISNULL(alter_part_array)) {
        ddl_type = ObDDLType::DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE;
      }
    }
  }
  LOG_DEBUG("auto part, after switch ddl type", K(ddl_type));
  return ret;
}

int ObAlterAutoPartAttrOp::alter_table_partition_attr(
    obrpc::ObAlterTableArg &alter_table_arg,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema)
{
  // In the alter table partition by range(xxx) size(xxx) (partitions...) case,
  // it is necessary to modify attributes related to automatic partitioning here.
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ddl service", K(ret), KP(ddl_service_));
  } else if (OB_FAIL(ddl_service_->gen_alter_partition_new_table_schema_offline(
      alter_table_arg, alter_table_schema, orig_table_schema, new_table_schema))) {
    LOG_WARN("fail to gen alter partition new table schema", K(ret));
  } else if (OB_FAIL(new_table_schema.check_validity_for_auto_partition())) {
    LOG_WARN("fail to check enable auto partitioning", KR(ret), K(new_table_schema));
  }
  return ret;
}

/*
description:
 0. For non-partitioned tables with automatic partitioning, the part_func_expr is not null, but part_level=zero.
 1. For partitioned tables, whether automatically partitioned or not, directly use the partition expression, or directly use the partition key.
 2. For non-partitioned tables, there are two scenarios:
    a. For automatically partitioned non-partitioned tables, use the partition expression.
    b. For non-automatically partitioned non-partitioned tables, use the primary key.
*/
int ObAlterAutoPartAttrOp::get_part_key_column_ids(
    const ObTableSchema &table_schema,
    ObIArray<uint64_t> &part_key_ids)
{
  int ret = OB_SUCCESS;
  part_key_ids.reset();
  // origin table partition is auto partition table, and is not partition table
  if (!table_schema.is_partitioned_table() && table_schema.is_auto_partitioned_table()) {
    if (OB_FAIL(table_schema.get_presetting_partition_keys(part_key_ids))) { // take part func or rowkey
      LOG_WARN("fail to get presetting partition keys", K(ret));
    }
  } else { // origin table is partition table or is not auto partition table
    ObString origin_table_part_func_expr = table_schema.get_part_option().get_part_func_expr_str();
    if (origin_table_part_func_expr.empty()) { // get rowkey as part key
      const ObRowkeyInfo &part_keys = table_schema.get_rowkey_info();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_keys.get_size(); ++i) {
        const ObRowkeyColumn *part_key_column = part_keys.get_column(i);
        if (OB_ISNULL(part_key_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the partition key is NULL, ", K(ret), K(part_keys));
        } else if (is_shadow_column(part_key_column->column_id_)) {
        } else if (OB_FAIL(part_key_ids.push_back(part_key_column->column_id_))) {
          LOG_WARN("failed to push back rowkey column id", K(ret));
        }
      }
    } else {
      const ObPartitionKeyInfo &partition_keys = table_schema.get_partition_key_info();
      if (partition_keys.is_valid() && OB_FAIL(partition_keys.get_column_ids(part_key_ids))) {
        LOG_WARN("fail to get column ids from partition keys", K(ret));
      }
    }
  }
  LOG_DEBUG("get partition key columns id", K(ret), K(part_key_ids));
  return ret;
}

int ObAlterAutoPartAttrOp::check_part_key_column_type(
    const ObTableSchema &table_schema,
    const ObPartitionOption &alter_part_option,
    bool &is_valid_part_column)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> part_key_ids;
  ObString alter_part_func_expr = alter_part_option.get_part_func_expr_str();
  is_valid_part_column = true;
  if (!alter_part_option.is_enable_auto_part()
      && !table_schema.is_partitioned_table() && !table_schema.is_auto_partitioned_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected none partition table, not enable auto part to but check part key.",
      K(ret), K(table_schema), K(alter_part_option));
  } else if (alter_part_func_expr.empty()) {
    // if user not define auto part func expr, we will get part ids from old table schema.
    if (OB_FAIL(get_part_key_column_ids(table_schema, part_key_ids))) {
      LOG_WARN("fail to get part key ids", K(ret));
    }
  } else if (OB_FAIL(table_schema.get_partition_keys_by_part_func_expr(alter_part_func_expr, part_key_ids))) {
    LOG_WARN("fail to get partition key", K(ret));
  }
  // check part column type is valid
  if (OB_SUCC(ret)) {
    const ObColumnSchemaV2 *column_schema = NULL;
    ObPartitionFuncType part_type = part_key_ids.count() > 1 ?
      PARTITION_FUNC_TYPE_RANGE_COLUMNS :
      PARTITION_FUNC_TYPE_RANGE;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_part_column && i < part_key_ids.count(); ++i) {
      uint64_t part_key_id = part_key_ids.at(i);
      if (part_key_id >= OB_MIN_SHADOW_COLUMN_ID || part_key_id < OB_APP_MIN_COLUMN_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part key id", K(ret), K(part_key_id));
      } else if (OB_FALSE_IT(column_schema = table_schema.get_column_schema(part_key_id))) {
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("column schema not exist", K(ret), K(table_schema), K(part_key_id));
      } else {
        ObObjType type = column_schema->get_data_type();
        if (ObResolverUtils::is_partition_range_column_type(type)) {
          /* case:
            create table t1(c1 double, c2 int, primary key(c1));
            alter table t1 partition by range();
          */
          part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS;
        }
        is_valid_part_column =
            ObResolverUtils::is_valid_partition_column_type(column_schema->get_data_type(), part_type, false);
      }
    }
  }
  return ret;
}

int ObAlterAutoPartAttrOp::lock_for_modify_auto_part_size(
    const ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t data_table_id = table_schema.get_table_id();
  ObArray<uint64_t> global_index_table_ids;
  ObArray<ObAuxTableMetaInfo> simple_index_infos;
  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const uint64_t index_table_id = simple_index_infos.at(i).table_id_;
    const ObIndexType index_type = simple_index_infos.at(i).index_type_;
    if (!is_index_local_storage(index_type)) {
      if (OB_FAIL(global_index_table_ids.push_back(index_table_id))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLLock::lock_for_modify_auto_part_size_in_trans(tenant_id, data_table_id, global_index_table_ids, trans))) {
    LOG_WARN("failed to lock for modify auto part size", K(ret));
  }
  return ret;
}

int ObAlterAutoPartAttrOp::alter_table_auto_part_attr_if_need(
    const ObAlterTableArg &alter_table_arg,
    const ObDDLType ddl_type,
    ObSchemaGetterGuard &schema_guard,
    ObTableSchema &table_schema,
    rootserver::ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!alter_table_arg.alter_auto_partition_attr_
      || ddl_type != ObDDLType::DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE) {
    /* in alter table <table> partition by range(<range>) (partition...) case,
     * we do not suppose to modify everything about auto split attr here */
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modified auto partition attr, not expected here", K(ret), K(ddl_type),
      K(alter_table_arg.alter_auto_partition_attr_));
  } else if (table_schema.is_table_without_pk() || table_schema.is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modified auto partition attr, unexpected table status", K(ret), K(table_schema));
  } else if (OB_FAIL(lock_for_modify_auto_part_size(table_schema, schema_guard, trans))) {
    LOG_WARN("failed to lock for modify auto part size", K(ret));
  } else {
    const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    const ObPartitionOption &alter_part_option = alter_table_schema.get_part_option();
    const uint64_t tenant_id = table_schema.get_tenant_id();
    if (alter_part_option.is_enable_auto_part()) {
      ObString alter_part_func_expr = alter_part_option.get_part_func_expr_str(); // no ref
      bool is_valid_column_type = true;
      if (OB_FAIL(check_part_key_column_type(table_schema, alter_part_option, is_valid_column_type))) {
        LOG_WARN("fail to check part column type", K(ret), K(alter_part_option));
      } else if (!is_valid_column_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("fail to alter table partition, not support yet", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (alter_part_func_expr.empty()) {
          /* case 1: part func is empty, like: alter table partition by range() size('xxx'); */
          /* if table is none part table, and part func is empty, set partition type "range" */
          /* we will set part func type in table_schema.enable_auto_partition according to old table schema potential part func, do nothing here */
        } else {
          /* case 2: part func is not empty, like : alter table partition by range(c1) size('xxx'); */
          if (OB_FAIL(check_auto_part_table_unique_index(table_schema, alter_part_func_expr, schema_guard))) {
            LOG_WARN("fail to check auto part table unique index.", K(ret), K(table_schema));
          } else if (OB_FAIL(check_and_set_table_auto_part_func(alter_part_option, table_schema))) {
            LOG_WARN("fail to check and set table auto part func.", K(ret), K(table_schema));
          }
        }
      }
      // enable auto split
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_schema.enable_auto_partition(alter_part_option.get_auto_part_size(), alter_part_option.get_part_func_type()))) {
          LOG_WARN("fail to enable auto partition", K(ret), K(alter_part_option));
        } else if (OB_FAIL(table_schema.check_enable_split_partition(true))) { // check origin table is satisfied auto partition conditions before enabled
          LOG_WARN("fail to check validity for auto-partition", K(ret), K(table_schema));
        }
      }
    } else {
      /* case 3: disable auto partition, like: alter table partition by range() size('unlimited') */
      table_schema.forbid_auto_partition();
    }
    if (OB_SUCC(ret)) {
      /* update global index property
       * if main table is enable auto split partition table, global index table should
       * change auto split property too */
      ObArray<uint64_t> modified_index_type_ids;
      if (OB_FAIL(alter_global_indexes_auto_part_attribute_online(  // update index
          alter_part_option, table_schema, schema_guard, ddl_operator, trans, modified_index_type_ids))) {
        LOG_WARN("fail to alter global index auto part property.", K(ret), K(table_schema));
      // for example, enable_auto_partition may change part_func_type if data table is not partitioned,
      // so need to sync aux tables' partition option
      } else if (!table_schema.is_partitioned_table() && OB_FAIL(sync_aux_tables_partition_option(table_schema, schema_guard, ddl_operator, trans, modified_index_type_ids))) {
        LOG_WARN("failed to sync aux tables partition schema", K(ret));
      } else if (OB_FAIL(ddl_operator.update_partition_option(trans, table_schema, alter_table_arg.ddl_stmt_str_))) {  // update main table
        LOG_WARN("fail to update partition option", K(ret), K(table_schema));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + GCONF.rpc_timeout;
      ObArray<ObTabletID> tablet_ids;
      if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
        LOG_WARN("failed to get tablet ids", K(ret));
      } else if (OB_FAIL(ObTabletSplitMdsHelper::modify_auto_part_size(tenant_id, tablet_ids, table_schema.get_auto_part_size(), abs_timeout_us, trans))) {
        LOG_WARN("failed to modify auto part size", K(ret));
      }
    }
  }
  return ret;
}

/*
description:
  1. if origin table is none partition table, should set new partition type according to alter table partition func
  2. if origin table if partition table, should check alter table partition func is equal to origin partition func or not
*/
int ObAlterAutoPartAttrOp::check_and_set_table_auto_part_func(
    const ObPartitionOption &alter_part_option,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.get_part_level() == PARTITION_LEVEL_ZERO) {
    ObString tmp_part_func_expr = alter_part_option.get_part_func_expr_str();
    ObPartitionFuncType part_func_type;
    if (OB_FAIL(extract_potential_partition_func_type(table_schema, tmp_part_func_expr, part_func_type))) {
      LOG_WARN("fail to extract partition func type", K(ret), K(table_schema));
    } else {
      table_schema.get_part_option().set_part_func_type(part_func_type);
      /* origin table is none partition table, set alter table partition func expr directly */
      table_schema.get_part_option().set_part_expr(alter_part_option.get_part_func_expr_str());
    }
  } else if (table_schema.get_part_level() == PARTITION_LEVEL_ONE) {
    ObString tmp_old_part_func_expr = table_schema.get_part_option().get_part_func_expr_str();
    ObString tmp_alter_table_part_func_expr = alter_part_option.get_part_func_expr_str();
    ObArray<ObString> old_expr_strs;
    ObArray<ObString> new_expr_strs;
    // partition by range columns(c1,c2) and partition by range columns(c1,    c2) is the same.
    if (OB_FAIL(split_on(tmp_old_part_func_expr, ',', old_expr_strs))) {
      LOG_WARN("fail to split func expr", K(ret), K(tmp_old_part_func_expr));
    } else if (OB_FAIL(split_on(tmp_alter_table_part_func_expr, ',', new_expr_strs))) {
      LOG_WARN("fail to split func expr", K(ret), K(tmp_alter_table_part_func_expr));
    } else if (old_expr_strs.count() != new_expr_strs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("fail to alter table partition, ori table func expr is diff from alter partition func expr.",
        K(ret), K(old_expr_strs), K(new_expr_strs));
    } else {
      // the columns in origin table part func expr must be in order with primary key (prefixed)
      // so as to the columns order of alter partition func expr. if not the same, alter partition by will
      // cause error in sql resolver.
      ObArenaAllocator allocator;
      for (int64_t i = 0; OB_SUCC(ret) && i < new_expr_strs.count(); ++i) {
        ObString in_new_expr_str = new_expr_strs.at(i).trim();
        ObString in_old_expr_str = old_expr_strs.at(i).trim();
        ObString out_new_expr_str = in_new_expr_str;
        ObString out_old_expr_str = in_old_expr_str;
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(ob_simple_low_to_up(allocator, in_new_expr_str, out_new_expr_str))) {
            LOG_WARN("failed to transfer low to up column name", K(ret));
          } else if (OB_FAIL(ob_simple_low_to_up(allocator, in_old_expr_str, out_old_expr_str))) {
            LOG_WARN("failed to transfer low to up column name", K(ret));
          }
        }
        if (OB_SUCC(ret) && (out_new_expr_str != out_old_expr_str)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("fail to alter table partition, ori table func expr is diff from alter partition func expr",
            K(ret), K(old_expr_strs), K(new_expr_strs));
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter auto table partition level large than one is not support currently!!!",
      K(ret), K(table_schema.get_part_level()));
  }
  return ret;
}

/*
description:
  1. If is enable auto partition and table is global index table, we should change global local index to global,
  2. Specially, if table is a partition table, partition key should satisfy prefix of index rowkey.
*/
int ObAlterAutoPartAttrOp::alter_global_indexes_auto_part_attribute_online(
    const ObPartitionOption &part_option,
    const ObTableSchema &table_schema,
    ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans,
    ObArray<uint64_t> &modified_index_type_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObTableSchema *index_schema = nullptr;
  const int64_t tenant_id = table_schema.get_tenant_id();
  uint64_t index_table_id = OB_INVALID_ID;

  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    index_table_id = simple_index_infos.at(i).table_id_;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
      LOG_WARN("fail to get to_table_schema schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index_schema is null", K(ret));
    } else if ((!index_schema->is_global_index_table() && !index_schema->is_global_local_index_table())
        || index_schema->is_spatial_index()) {
      // skip
    } else {
      // cover global index table auto split property
      ObString ddl_stmt_str("");  // empty ddl_stmt_str
      HEAP_VAR(ObTableSchema, new_index_schema) {
        if (OB_FAIL(new_index_schema.assign(*index_schema))) {
          LOG_WARN("assign index_schema failed", K(ret));
        } else if (OB_FAIL(update_global_auto_split_attr(part_option, new_index_schema))) {
          LOG_WARN("fail to update global auto split attr", K(ret), K(part_option));
        } else if (OB_FAIL(ddl_operator.update_partition_option(trans, new_index_schema, ddl_stmt_str))) {
          LOG_WARN("fail to update partition option.", K(ret), K(new_index_schema));
        }

        if (OB_SUCC(ret)) {
          // Note that global local index tablets also have auto_part_size
          const int64_t abs_timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts() : ObTimeUtility::current_time() + GCONF.rpc_timeout;
          ObArray<ObTabletID> tablet_ids;
          if (OB_FAIL(new_index_schema.get_tablet_ids(tablet_ids))) {
            LOG_WARN("failed to get tablet ids", K(ret));
          } else if (OB_FAIL(ObTabletSplitMdsHelper::modify_auto_part_size(tenant_id, tablet_ids, new_index_schema.get_auto_part_size(), abs_timeout_us, trans))) {
            LOG_WARN("failed to modify auto part size", K(ret));
          }
        }
      } // end heap var
      if (OB_SUCC(ret)) {
        if (part_option.get_auto_part()
            && table_schema.get_part_level() == PARTITION_LEVEL_ZERO
            && index_schema->is_global_local_index_table()) {
          ObIndexType index_type;
          if (OB_FAIL(switch_global_local_index_type(*index_schema, index_type))) {
            LOG_WARN("fail to switch index type", K(ret));
          } else if (OB_FAIL(ddl_operator.update_index_type(
              table_schema, index_table_id, index_type, &ddl_stmt_str, trans))) {
            LOG_WARN("fail to update index type", K(ret), K(index_type));
          } else if (OB_FAIL(modified_index_type_ids.push_back(index_table_id))) {
            LOG_WARN("fail to push back index table id", K(ret), K(index_table_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterAutoPartAttrOp::sync_aux_tables_partition_option(
    const ObTableSchema &data_table_schema,
    ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans,
    ObArray<uint64_t> &modified_index_type_ids)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSEArray<uint64_t, 20> aux_table_ids;

  // 1. gather local aux table schemas, see also ObDDLService::generate_tables_array
  if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get_simple_index_infos failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (OB_FAIL(aux_table_ids.push_back(simple_index_infos.at(i).table_id_))) {
      LOG_WARN("fail to push back index table id", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t mtid = data_table_schema.get_aux_lob_meta_tid();
    const uint64_t ptid = data_table_schema.get_aux_lob_piece_tid();
    if (!((mtid != OB_INVALID_ID && ptid != OB_INVALID_ID) || (mtid == OB_INVALID_ID && ptid == OB_INVALID_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expect meta tid and piece tid both valid or both invalid", KR(ret), K(mtid), K(ptid));
    } else if (OB_INVALID_ID != mtid &&
        OB_FAIL(aux_table_ids.push_back(mtid))) {
      LOG_WARN("fail to push back lob meta tid", KR(ret), K(mtid));
    } else if (OB_INVALID_ID != ptid &&
        OB_FAIL(aux_table_ids.push_back(ptid))) {
      LOG_WARN("fail to push back lob piece tid", KR(ret), K(ptid));
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t mlog_tid = data_table_schema.get_mlog_tid();
    if ((OB_INVALID_ID != mlog_tid)
        && OB_FAIL(aux_table_ids.push_back(mlog_tid))) {
      LOG_WARN("failed to push back materialized view log tid", KR(ret), K(mlog_tid));
    }
  }

  // 2. update inner table
  for (int64_t i = 0; OB_SUCC(ret) && i < aux_table_ids.count(); ++i) {
    const uint64_t aux_table_id = aux_table_ids.at(i);
    const ObTableSchema *aux_table_schema = nullptr;
    const ObString ddl_stmt_str("");
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, aux_table_id, aux_table_schema))) {
      LOG_WARN("fail to get to_table_schema schema", K(ret), K(aux_table_id));
    } else if (OB_ISNULL(aux_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(aux_table_id));
    } else if ((aux_table_schema->is_index_local_storage() && !(common::is_contain(modified_index_type_ids, aux_table_id)))
        || aux_table_schema->is_aux_lob_table()
        || aux_table_schema->is_mlog_table()) {
      HEAP_VAR(ObTableSchema, new_aux_table_schema) {
        if (OB_FAIL(new_aux_table_schema.assign(*aux_table_schema))) {
          LOG_WARN("assign index_schema failed", K(ret));
        } else if (OB_FAIL(new_aux_table_schema.assign_partition_schema(data_table_schema))) {
          LOG_WARN("fail to assign partition schema", K(data_table_schema), KR(ret));
        } else if (OB_FAIL(ddl_operator.update_partition_option(trans, new_aux_table_schema, ddl_stmt_str))) {
          LOG_WARN("fail to update partition option.", K(ret), K(new_aux_table_schema));
        }
      }
    }

  }
  return ret;
}

int ObAlterAutoPartAttrOp::switch_global_local_index_type(
    const ObTableSchema &index_schema,
    ObIndexType& index_type)
{
  int ret = OB_SUCCESS;
  switch (index_schema.get_index_type()) {
    case INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE: {
      index_type = INDEX_TYPE_UNIQUE_GLOBAL;
      break;
    }
    case INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE: {
      index_type = INDEX_TYPE_NORMAL_GLOBAL;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      index_type = INDEX_TYPE_MAX;
      LOG_WARN("not supported index type", K(ret), K(index_type), K(index_schema));
    }
  }
  return ret;
}
/*
description:
  1. in alter table partition by range(xxx) size(xxx) partitions case, should update global index partition attr
*/
int ObAlterAutoPartAttrOp::alter_global_indexes_auto_part_attribute_offline(
    ObAlterTableArg &alter_table_arg,
    ObTableSchema &new_index_schema)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = nullptr;
  AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  const ObPartitionOption &alter_part_option = alter_table_schema.get_part_option();

  if (!alter_table_arg.alter_auto_partition_attr_) {
    // skip.
    // in alter table <table> partition by range(<range>) (partition...) case,
    // we do not suppose to modify everything about auto split table attr here
    LOG_INFO("alter table part attr, auto attr is false, no need to change",
      K(alter_table_arg.alter_auto_partition_attr_));
  } else if (!new_index_schema.is_global_index_table()) {
    // only for global index table (not global local)
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type, here should be modify to global index type",
      K(ret), K(new_index_schema));
  } else if (alter_part_option.get_part_func_expr_str().empty()) {
    // This code path is specific to the 'alter table partition by range(xxx) size(xxx) (partition...)' syntax,
    // indicating a modification of the automatic partitioning rules;
    // therefore, if the 'part func' is empty, it is not allowed.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter partition by case here, partition func expr is empty",
      K(ret), K(alter_part_option));
  } else if (OB_ISNULL(alter_table_schema.get_part_array())) {
    // Similar to the above, if the partitioning rules are empty, it is not permitted.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter partition by case here, partition array is empty",
      K(ret), K(alter_table_schema));
  } else if (OB_FAIL(update_global_auto_split_attr(alter_part_option, new_index_schema))) {
    LOG_WARN("fail to update global auto split attr", K(ret), K(new_index_schema));
  }
  return ret;
}

/*
  description:
    Obtain the potential partition type of a non-partitioned table.
    这里如果预分裂键为double，虽然只有1列，但partition_func_type也要变成PARTITION_FUNC_TYPE_RANGE_COLUMNS
*/
int ObAlterAutoPartAttrOp::extract_potential_partition_func_type(
    const ObTableSchema &table_schema,
    const ObString &part_func_expr,
    ObPartitionFuncType &part_func_type)
{
  int ret = OB_SUCCESS;
  bool is_range_col = false;
  if (part_func_expr.empty()) {
    if (table_schema.is_index_table()) {  // index table
      ObArray<ObString> rowkey_columns;
      if (OB_FAIL(table_schema.extract_actual_index_rowkey_columns_name(rowkey_columns))) {
        LOG_WARN("fail to extract index rowkey column cnt", K(ret));
      } else if (rowkey_columns.count() > 1) {
        part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
      } else if (OB_FAIL(table_schema.is_range_col_part_type(is_range_col))) {
        LOG_WARN("fail to check is first part key range col", K(ret));
      } else if (is_range_col) {
        part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
      } else {
        part_func_type = PARTITION_FUNC_TYPE_RANGE;
      }
    } else if (table_schema.is_user_table()) {    // user table
      const ObRowkeyInfo &part_keys = table_schema.get_rowkey_info();
      if (part_keys.get_size() > 1) {
        part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
      } else if (OB_FAIL(table_schema.is_range_col_part_type(is_range_col))) {
        LOG_WARN("fail to check is first part key range col", K(ret));
      } else if (is_range_col) {
        part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
      } else {
        part_func_type = PARTITION_FUNC_TYPE_RANGE;
      }
    }
  } else { // part_func_expr is not empty
    ObString tmp_part_func_expr = part_func_expr;
    ObArray<ObString> expr_strs;
    if (OB_FAIL(split_on(tmp_part_func_expr, ',', expr_strs))) {
      LOG_WARN("fail to split func expr", K(ret), K(tmp_part_func_expr));
    } else if (expr_strs.count() > 1) { // multi partition column
      part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    } else if (OB_FAIL(table_schema.is_range_col_part_type(is_range_col))) {
      LOG_WARN("fail to check is first part key range col", K(ret));
    } else if (is_range_col) {
      part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    } else {
      part_func_type = PARTITION_FUNC_TYPE_RANGE;
    }
  }
  return ret;
}
/*
description:
The following scenarios exist for modifications to global indexes:
  1. For a global non-partitioned index table, if it is being modified to an auto-partitioned global non-partitioned table,
     it is necessary to change the auto_part and auto_part_size. The part_func_type should be modified to 'range' or 'range columns' based on the actual number of index keys.
  2. For a global partitioned index table, if it is being modified to an auto-partitioned global partitioned table,
     it is necessary to change the auto_part and auto_part_size.
  3. For a global auto-partitioned non-partitioned index table, if it is being modified to a non-auto-partitioned non-partitioned table,
     it is necessary to change the auto_part and auto_part_size, set the part_func_type to default, and modify the part_func_expr to null.
  4. For a global auto-partitioned partitioned index table, if it is being modified to a non-auto-partitioned non-partitioned table,
     it is necessary to change the auto_part and auto_part_size.
*/
int ObAlterAutoPartAttrOp::update_global_auto_split_attr(
    const ObPartitionOption &alter_part_option,
    ObTableSchema &new_index_schema)
{
  int ret = OB_SUCCESS;
  bool enable_auto_split = alter_part_option.get_auto_part();
  ObPartitionOption &new_index_option = new_index_schema.get_part_option();
  ObPartitionFuncType part_func_type;
  if (new_index_schema.get_part_level() == PARTITION_LEVEL_ZERO) {
    if (enable_auto_split) {
      if (new_index_option.get_part_func_expr_str().empty()) {
        ObString empty_part_func_expr;
        if (OB_FAIL(extract_potential_partition_func_type(new_index_schema, empty_part_func_expr, part_func_type))) {
          LOG_WARN("fail to extract partition func type", K(ret), K(new_index_schema));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("none partition table and partition func expr not empty is not expected in global table",
          K(ret), K(new_index_schema));
      }
    }
  } else if (new_index_schema.get_part_level() == PARTITION_LEVEL_ONE) {
    // if user is turning on auto split attr, and index table is partition table,
    // we should check partition key is satisfied prefixed of index rowkey key
    if (enable_auto_split) {
      if (OB_FAIL(new_index_schema.is_partition_key_match_rowkey_prefix(enable_auto_split))) {
        LOG_WARN("fail to check partition key match rowkey prefix", K(ret));
      } else if (!new_index_schema.is_range_part()) {
        // none range part table is not support, here we not set auto split attr
        enable_auto_split = false;
      } else {
        part_func_type = new_index_option.get_part_func_type();
      }
    }
  } else { // not support
    enable_auto_split = false;
  }
  if (OB_SUCC(ret)) {
    if (enable_auto_split) {
      if (OB_FAIL(new_index_schema.enable_auto_partition(alter_part_option.get_auto_part_size(), part_func_type))) {
        LOG_WARN("fail to enable auto partition", K(ret), K(alter_part_option));
      }
    } else {
      new_index_schema.forbid_auto_partition();
    }
  }
  return ret;
}

/*
description:
  check unique local index if main table is none partition table, but open auto split partition property
*/
int ObAlterAutoPartAttrOp::check_auto_part_table_unique_index(
    const ObTableSchema &table_schema,
    ObString &alter_table_part_func_expr,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = table_schema.get_tenant_id();
  const int64_t table_id = table_schema.get_table_id();
  bool has_unique_local_index = false;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || table_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(tenant_id), K(table_id));
  } else if (table_schema.get_part_level() > PARTITION_LEVEL_ZERO) {
    // skip
  } else if (!table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support none user table do auto part attr modification", K(ret));
  } else if (OB_FAIL(schema_guard.check_has_local_unique_index(
      tenant_id, table_id, has_unique_local_index))) {
    LOG_WARN("fail to check has local unique index of main table", K(tenant_id), K(table_id));
  } else if (!has_unique_local_index) {
    // skip
  } else {
    // get index table id
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", KR(ret), K(tenant_id), K(table_id));
    }
    const ObTableSchema *index_schema = nullptr;
    int64_t index_table_id = OB_INVALID_ID;
    ObArray<ObString> rowkey_name_columns;
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      index_table_id = simple_index_infos.at(i).table_id_;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
        LOG_WARN("fail to get to_table_schema schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index_schema is null", K(ret));
      } else if (!index_schema->is_local_unique_index_table()){
        // skip
      } else if (OB_FAIL(index_schema->extract_actual_index_rowkey_columns_name(rowkey_name_columns))) {
        LOG_WARN("fail to  extract actual index rowkey columns", K(ret), K(*index_schema));
      } else {
        // check main table partition key is included in unique local index key.
        ObArray<ObString> alter_part_column_array;
        if (OB_FAIL(split_on(alter_table_part_func_expr, ',', alter_part_column_array))) {
          LOG_WARN("fail to split func expr", K(ret), K(alter_table_part_func_expr));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < alter_part_column_array.count(); ++i) {
            if (!has_exist_in_array(rowkey_name_columns, alter_part_column_array.at(i).trim())) {
              ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
              LOG_WARN("unique local index rowkey key not include all table partition key.",
                K(ret), K(alter_table_part_func_expr), K(rowkey_name_columns));
              LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "UNIQUE INDEX");
            }
          }
        }
      }
    }
  }
  return ret;
}
