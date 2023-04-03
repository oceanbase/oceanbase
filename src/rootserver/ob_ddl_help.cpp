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
#include "rootserver/ob_ddl_help.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_primary_zone_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_rpc_struct.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/ob_resolver_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

namespace rootserver
{
/**
 * alter tablegroup add table_list
 * It can be considered as a batch implementation of alter table set tablegroup.
 *
 * In order to gradually migrate the table to the tablegroup created after 2.0, this method does
 * not support adding table to the tablegroup created before 2.0.
 */
int ObTableGroupHelp::add_tables_to_tablegroup(ObMySQLTransaction &trans,
                                               ObSchemaGetterGuard &schema_guard,
                                               const ObTablegroupSchema &tablegroup_schema,
                                               const ObAlterTablegroupArg &arg)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableItem> &table_items = arg.table_items_;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_UNLIKELY(!trans.is_started()) || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (table_items.count() <= 0) {
    //nothing todo
  } else if (OB_INVALID_ID == tablegroup_id || is_sys_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service is null", K(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    const bool is_index = false;
    const uint64_t tenant_id = arg.tenant_id_;
    ObString tablegroup_name = tablegroup_schema.get_tablegroup_name();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const ObTableItem &table_item = table_items.at(i);
      const ObTableSchema *table_schema = NULL;
      uint64_t database_id = common::OB_INVALID_ID;
      //check database exist
      if (OB_FAIL(schema_guard.get_database_id(
                  tenant_id, table_item.database_name_, database_id))) {
        LOG_WARN("failed to get database schema!", K(database_id),
                 K(tenant_id), K(table_item));
      } else if (OB_FAIL(schema_guard.get_table_schema(
                  tenant_id, database_id, table_item.table_name_, is_index, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(table_item));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(table_item.database_name_), to_cstring(table_item.table_name_));
        LOG_WARN("table not exist!", K(tenant_id), K(database_id), K(table_item), K(ret));
      } else if (is_inner_table(table_schema->get_table_id())) {
        //the tablegroup of sys table must be oceanbase
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("sys table's tablegroup should be oceanbase", KR(ret), K(arg), KPC(table_schema));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set the tablegroup of system table besides oceanbase");
      } else {
        ObTableSchema new_table_schema;
        ObSqlString sql;
        if (OB_FAIL(new_table_schema.assign(*table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          new_table_schema.set_tablegroup_id(tablegroup_id);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_table_alter_tablegroup(schema_guard, *table_schema, new_table_schema))) {
          LOG_WARN("fail to check primary zone and locality", K(ret));
        } else if (OB_FAIL(sql.append_fmt("ALTER TABLEGROUP %.*s ADD TABLE %.*s.%.*s",
                                          tablegroup_name.length(),
                                          tablegroup_name.ptr(),
                                          table_item.database_name_.length(),
                                          table_item.database_name_.ptr(),
                                          table_item.table_name_.length(),
                                          table_item.table_name_.ptr()))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (OB_FAIL(ddl_service_->check_tablegroup_in_single_database(schema_guard, new_table_schema))) {
          LOG_WARN("fail to check tablegroup in single database", K(ret));
        } else {
          ObString sql_str = sql.string();
          if (OB_FAIL(ddl_operator.alter_tablegroup(schema_guard, new_table_schema, trans, &sql_str))) {
            LOG_WARN("ddl operator alter tablegroup failed", K(ret), K(tenant_id), K(tablegroup_id));
          }
        }
      } // no more
    } // end for get all tableschema
  }
  return ret;
}

/**
 * Move table between tablegroups, or moved out of and into tablegroup, check the following conditions:
 * 1. In order to gradually migrate the table to the tablegroup created after 2.0, this method
 *    does not support adding table to the tablegroup created before 2.0.
 */
int ObTableGroupHelp::check_table_alter_tablegroup(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  bool can_alter_tablegroup = !is_sys_tablegroup_id(new_table_schema.get_tablegroup_id());
  ObString src_previous_locality_str;
  ObString dst_previous_locality_str;
  if (!can_alter_tablegroup) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cann't alter table's tablegroup", K(ret),
             "src_tg_id", orig_table_schema.get_tablegroup_id(),
             "dst_tg_id", new_table_schema.get_tablegroup_id());
  } else if (OB_INVALID_ID == new_table_schema.get_tablegroup_id()) {
    // skip
  } else {
    // Handling the source is a tablegroup created after 2.0
    if (OB_FAIL(check_partition_option_for_create_table(schema_guard, new_table_schema))) {
      LOG_WARN("fail to check tablegroup partition", K(ret), K(new_table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    const share::schema::ObTablegroupSchema *orig_tg = nullptr;
    const share::schema::ObTablegroupSchema *new_tg = nullptr;
    const int64_t orig_tg_id = orig_table_schema.get_tablegroup_id();
    const int64_t new_tg_id = new_table_schema.get_tablegroup_id();
    const uint64_t tenant_id = orig_table_schema.get_tenant_id();
    if (OB_INVALID_ID == orig_tg_id) {
      // Did not belong to any tablegroup
    } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, orig_tg_id, orig_tg))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id), K(orig_tg_id));
    } else if (OB_UNLIKELY(nullptr == orig_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", K(ret), K(orig_tg_id));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == new_tg_id) {
      // Did not belong to any tablegroup
    } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, new_tg_id, new_tg))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id), K(new_tg_id));
    } else if (OB_UNLIKELY(nullptr == new_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", K(ret), K(new_tg_id));
    }
    if (OB_SUCC(ret)) {
      if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != new_table_schema.get_duplicate_scope()
          && OB_INVALID_ID != new_table_schema.get_tablegroup_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("duplicate table in tablegroup is not supported", K(ret),
                 "table_id",new_table_schema.get_table_id(),
                 "tablegroup_id", new_table_schema.get_tablegroup_id());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate table in tablegroup");
      }
    }
  }
  return ret;
}

// When creating a table, need to check whether the partition type of tablegroup and table are consistent
int ObTableGroupHelp::check_partition_option_for_create_table(ObSchemaGetterGuard &schema_guard,
                                                              ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  const ObTablegroupSchema *tablegroup = NULL;
  if (OB_INVALID_ID == tablegroup_id || is_sys_tablegroup_id(tablegroup_id)) {
    LOG_INFO("skip to check tablegroup partition", KPC(tablegroup), K(table));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id), KT(tablegroup_id));
  } else if (OB_ISNULL(tablegroup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup schema is null", K(ret), KT(tablegroup_id));
  } else if (table.is_in_splitting() || tablegroup->is_in_splitting()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table or tablegroup is splitting", K(ret), K(table), K(tablegroup));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add table to tablegroup while either object is splitting");
  } else if (OB_FAIL(check_partition_option(*tablegroup, table))) {
    LOG_WARN("fail to check partition option", K(ret), KPC(tablegroup), K(table));
  }
  return ret;
}

/**
 * If partition data distribution methods are same, in the following scenarios,
 * the partition types of table and tablegroup are considered to be the same:
 * 1.For hash partition, the number of partitions must be the same
 * 2.For key partition, the number of partitions must be the same and the column_list_num must match
 * 3.For range/range column or list/list column partition,
 *   the number of partition expressions partitions/partition split point are required to be same
 */
int ObTableGroupHelp::check_partition_option(
    const ObTablegroupSchema &tablegroup,
    ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  const uint64_t table_id = table.get_table_id();
  if (PARTITION_LEVEL_TWO == table.get_part_level()) {
    is_matched = false;
    LOG_WARN("Add composited-partitioned table to tablegroup is not supported yet", K(tablegroup), K(table));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add composited-partitioned table to tablegroup");
  } else if (is_sys_tablegroup_id(tablegroup_id)) {
    is_matched = true;
    LOG_INFO("skip to check tablegroup partition", K(tablegroup), K(table));
  } else if (tablegroup.get_part_level() != table.get_part_level()) {
    LOG_WARN("part level not matched", K(ret), KT(tablegroup_id), KT(table_id),
             "tg_part_level", tablegroup.get_part_level(), "table_part_level", table.get_part_level());
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition level not matched");
  } else if (PARTITION_LEVEL_ZERO == tablegroup.get_part_level()) {
    // Non-partitioned table
    is_matched = true;
    LOG_INFO("tablegroup & table has no partitions, just pass", K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == table.get_part_level()
        || PARTITION_LEVEL_TWO == table.get_part_level()) {
      const bool is_subpart = false;
      if (OB_FAIL(check_partition_option(tablegroup, table, is_subpart, is_matched))) {
        LOG_WARN("level one partition not matched", K(ret), KT(tablegroup_id), KT(table_id));
      } else if (!is_matched) {
        // bypass
      }
    }
    if (OB_SUCC(ret) && is_matched
        && PARTITION_LEVEL_TWO == table.get_part_level()) {
      const bool is_subpart = true;
      if (OB_FAIL(check_partition_option(tablegroup, table, is_subpart, is_matched))) {
        LOG_WARN("level two partition not matched", K(ret), KT(tablegroup_id), KT(table_id));
      } else if (!is_matched) {
        // bypass
      }
    }
  }
  if (OB_SUCC(ret) && !is_matched) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("partition option not match", K(ret));
    // LOG_USER_ERROR will be printed under each 'is_matched=false' conditions
  }
  return ret;
}

// FIXME:Support non-template subpartition
int ObTableGroupHelp::check_partition_option(
    const ObTablegroupSchema &tablegroup,
    ObTableSchema &table,
    bool is_subpart,
    bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  const uint64_t table_id = table.get_table_id();
  bool is_oracle_mode = false;
  bool table_is_oracle_mode = false;
  if (OB_FAIL(tablegroup.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(tablegroup));
  } else if (OB_FAIL(table.check_if_oracle_compat_mode(table_is_oracle_mode))) {
    LOG_WARN("fail to get tenant mode", KR(ret), K(table));
  } else if (is_oracle_mode != table_is_oracle_mode) {
    is_matched = false;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "table mode not match");
  } else {
    if (is_sys_tablegroup_id(tablegroup_id)) {
      is_matched = true;
      LOG_WARN("skip to check tablegroup partition", K(tablegroup), K(table));
    } else {
      const ObPartitionOption &tablegroup_part = !is_subpart ? tablegroup.get_part_option()
                                                            : tablegroup.get_sub_part_option();
      const ObPartitionOption &table_part = !is_subpart ? table.get_part_option()
                                                        : table.get_sub_part_option();
      ObPartitionFuncType tg_part_func_type = tablegroup_part.get_part_func_type();
      ObPartitionFuncType table_part_func_type = table_part.get_part_func_type();
      if (tg_part_func_type != table_part_func_type
          && (!is_key_part(tg_part_func_type) || !is_key_part(table_part_func_type))) {
        //skip
        LOG_WARN("partition func type not matched",
                KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
      } else if (PARTITION_FUNC_TYPE_MAX == tg_part_func_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid part_func_type", K(ret), K(tg_part_func_type), K(table_part_func_type));
      } else if (is_hash_like_part(tg_part_func_type)) {
        is_matched = tablegroup_part.get_part_num() == table_part.get_part_num();
        if (!is_matched) {
          LOG_WARN("partition num not matched",
                  KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "range partition number not match");
        } else if (is_key_part(tg_part_func_type)) {
          // For key partition, needs compared column_list_num
          is_matched = false;
          int64_t tablegroup_expr_num = OB_INVALID_INDEX;
          int64_t table_expr_num = OB_INVALID_INDEX;
          if (!is_subpart) {
            if (OB_FAIL(tablegroup.calc_part_func_expr_num(tablegroup_expr_num))) {
              LOG_WARN("fail to get part_func_expr_num", K(ret), K(tablegroup));
            } else if (OB_FAIL(table.calc_part_func_expr_num(table_expr_num))) {
              LOG_WARN("fail to get part_func_expr_num", K(ret), K(table));
            }
          } else {
            if (OB_FAIL(tablegroup.calc_subpart_func_expr_num(tablegroup_expr_num))) {
              LOG_WARN("fail to get subpart_func_expr_num", K(ret), K(tablegroup));
            } else if (OB_FAIL(table.calc_subpart_func_expr_num(table_expr_num))) {
              LOG_WARN("fail to get subpart_func_expr_num", K(ret), K(table));
            }
          }
          if (OB_FAIL(ret)) {
            // skip
          } else if (OB_INVALID_INDEX == tablegroup_expr_num
                    || OB_INVALID_INDEX == table_expr_num) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr num is invalid", K(ret), K(tablegroup_expr_num), K(table_expr_num));
          } else {
            is_matched = (tablegroup_expr_num == table_expr_num);
          }
        }
      } else if (is_range_part(tg_part_func_type)
                || is_list_part(tg_part_func_type)) {
        const int64_t tg_part_num = tablegroup_part.get_part_num();
        const int64_t table_part_num = table_part.get_part_num();
        if (tg_part_num != table_part_num) {
          LOG_WARN("range partition number not matched", KT(tablegroup_id), KT(table_id),
                  K(tablegroup_part), K(table_part), K(tg_part_num), K(table_part_num));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "range or list partition number not match");
        } else if (!is_subpart) {
          if (OB_ISNULL(tablegroup.get_part_array())
              || OB_ISNULL(table.get_part_array())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition_array is null", K(ret), K(tablegroup), K(table));
          } else {
            is_matched = true;
            for (int i = 0; i < tg_part_num && is_matched && OB_SUCC(ret); i++) {
              is_matched = false;
              ObPartition *tg_part = tablegroup.get_part_array()[i];
              for (int j = 0; j < table_part_num && !is_matched && OB_SUCC(ret); j++) {
                ObPartition *table_part = table.get_part_array()[j];
                if (OB_ISNULL(tg_part) || OB_ISNULL(table_part)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition is null", K(ret), KPC(tg_part), KPC(table_part));
                } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                           is_oracle_mode, *tg_part, *table_part, tg_part_func_type, is_matched))) {
                  LOG_WARN("fail to check partition value", KPC(tg_part), KPC(table_part), K(tg_part_func_type));
                }
              }
            }
          }
          if (!is_matched) {
            LOG_WARN("range partition value not matched", K(ret), KT(tablegroup_id), KT(table_id),
                    "tg_partition_array", ObArrayWrap<ObPartition *>(tablegroup.get_part_array(), tg_part_num),
                    "table_part_array", ObArrayWrap<ObPartition *>(table.get_part_array(), table_part_num));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "range or list partition with not matched collation or value");
          }
        } else {
          if (OB_ISNULL(tablegroup.get_def_subpart_array())
              || OB_ISNULL(table.get_def_subpart_array())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("def_sub_partition_array is null", K(ret), K(tablegroup), K(table));
          } else {
            is_matched = true;
            for (int i = 0; i < tg_part_num && is_matched && OB_SUCC(ret); i++) {
              is_matched = false;
              ObSubPartition *tg_part = tablegroup.get_def_subpart_array()[i];
              for (int j = 0; j < table_part_num && !is_matched && OB_SUCC(ret); j++) {
                ObSubPartition *table_part = table.get_def_subpart_array()[j];
                if (OB_ISNULL(tg_part) || OB_ISNULL(table_part)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition is null", K(ret), KPC(tg_part), KPC(table_part));
                } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                           is_oracle_mode, *tg_part, *table_part, tg_part_func_type, is_matched))) {
                  LOG_WARN("fail to check partition value", KPC(tg_part), KPC(table_part), K(tg_part_func_type));
                }
              }
            }
          }
          if (!is_matched) {
            LOG_WARN("range partition value not matched", K(ret), KT(tablegroup_id), KT(table_id),
                    "tg_partition_array", ObArrayWrap<ObSubPartition *>(tablegroup.get_def_subpart_array(), tg_part_num),
                    "table_part_array", ObArrayWrap<ObSubPartition *>(table.get_def_subpart_array(), table_part_num));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "range or list partition with not matched collation or value");
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid part func type", KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
      }
    }
  }
  return ret;
}

/**
 * Check whether adding or dropping partition is legal, currently only adding or dropping the
 * first-level range/range columns partition is allowed
 * For drop partition:
 * 1.part_name need exist(When drop a partition which has same name partition,
 *                        it is intercepted by resolver)
 * For add partition:
 * 1.part_name needs to not exist
 * 2.Ensure that high_bound_val increases monotonically
 * For modify partition:
 * The partition expressions of all tables are required to exist
 */
int ObTableGroupHelp::check_alter_partition(const ObPartitionSchema *&orig_part_schema,
                                            ObPartitionSchema *&alter_part_schema,
                                            const ObAlterTablegroupArg::ModifiableOptions alter_part_type,
                                            int64_t expr_num,
                                            bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema)
      || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null",
             KP(orig_part_schema), KP(alter_part_schema), K(ret));
  } else if (ObAlterTablegroupArg::MAX_OPTION == alter_part_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in upgrade, can not do partition maintenance", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition maintenance during upgrade");
  } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type
             || ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type
             || ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("split partitions is not supported", KR(ret), K(orig_part_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partitions is");
  } else if (ObAlterTablegroupArg::DROP_PARTITION == alter_part_type) {
    if (OB_FAIL(check_drop_partition(orig_part_schema, alter_part_schema, is_tablegroup))) {
      LOG_WARN("failed to check drop partition", K(ret));
    }
  } else {
    // Except drop partition, may lack for the definition of partition name in Oracle mode
    ObTablegroupSchema *tablegroup_schema = NULL;
    AlterTableSchema *alter_table_schema = NULL;
    if (is_tablegroup) {
      tablegroup_schema = static_cast<ObTablegroupSchema *>(alter_part_schema);
      if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed get tablegroup schema", K(ret), K(tablegroup_schema));
      } else if (tablegroup_schema->is_range_part() || tablegroup_schema->is_list_part()) {
        if (OB_FAIL(ddl_service_->fill_part_name(*orig_part_schema, *tablegroup_schema))) {
          LOG_WARN("failed to fill part name", K(ret));
        }
      }
    } else {
      // For split partition, if the split partition name of the table is not filled,
      // then do not fill the last partition range, only fill the partition name
      alter_table_schema = static_cast<AlterTableSchema *>(alter_part_schema);
      if (OB_ISNULL(alter_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed get alter table schema", K(ret), KP(alter_part_schema));
      } else if (alter_table_schema->is_range_part() || alter_table_schema->is_list_part()) {
        if (OB_FAIL(ddl_service_->fill_part_name(*orig_part_schema, *alter_table_schema))) {
          LOG_WARN("failed to fill part name", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!orig_part_schema->is_range_part()
             && !orig_part_schema->is_list_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can only be used on RANGE/RANGE COLUMNS, list/list columns partitions", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "current partition maintenance operation for non {range/range, list/list} partitions");
  } else if (expr_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr num is invalid", K(ret), K(expr_num));
  } else if (ObAlterTablegroupArg::ADD_PARTITION == alter_part_type) {
    if (OB_FAIL(check_add_partition(orig_part_schema, alter_part_schema, expr_num, is_tablegroup))) {
      LOG_WARN("failed to check add partition", K(ret));
    }
  } else if (ObAlterTablegroupArg::DROP_PARTITION != alter_part_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter partition type", K(ret), K(alter_part_type));
  }
  return ret;
}

int ObTableGroupHelp::check_drop_partition(const share::schema::ObPartitionSchema *&orig_part_schema,
                                           const share::schema::ObPartitionSchema *alter_part_schema,
                                           bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema)
      || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret),
             KP(orig_part_schema), KP(alter_part_schema));
  } else {
    if (alter_part_schema->get_part_option().get_part_num() >=
        orig_part_schema->get_part_option().get_part_num()) {
      ret = OB_ERR_DROP_LAST_PARTITION;
      LOG_WARN("cannot drop all partitions", K(ret),
               "partitions current", orig_part_schema->get_part_option().get_part_num(),
               "partitions to be dropped", alter_part_schema->get_part_option().get_part_num());
      LOG_USER_ERROR(OB_ERR_DROP_LAST_PARTITION);
    } else {
      const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
      ObPartition **part_array = alter_part_schema->get_part_array();
      const int64_t orig_part_num = orig_part_schema->get_part_option().get_part_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        ObPartition *alter_part = part_array[i];
        bool found = false;
        for (int64_t j = 0; j < orig_part_num && OB_SUCC(ret) && !found; j++) {
          const ObPartition *part = orig_part_schema->get_part_array()[j];
          if (OB_ISNULL(part) || OB_ISNULL(alter_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), KP(part), KP(alter_part));
          } else if (is_tablegroup) {
            if (ObCharset::case_insensitive_equal(part->get_part_name(),
                                                  alter_part->get_part_name())) {
              if (OB_FAIL(alter_part->assign(*part))) {
                LOG_WARN("partition assign failed", K(ret), K(part), K(alter_part));
              }
              found = true;
            }
          } else if (OB_FAIL(ObDDLOperator::check_part_equal(
                             orig_part_schema->get_part_option().get_part_func_type(),
                             part, alter_part, found))) {
            LOG_WARN("check_part_equal failed", K(ret));
          } else if (found) {
            if (OB_FAIL(alter_part->assign(*part))) {
              LOG_WARN("partition assign failed", K(ret), K(part), K(alter_part));
            }
          }
        }// end for
        if (OB_SUCC(ret)) {
          if (!found) {
            ret = OB_ERR_DROP_PARTITION_NON_EXISTENT;
            LOG_WARN("partition to be dropped not exist", K(ret), "partition name", part_array[i]->get_part_name());
            LOG_USER_ERROR(OB_ERR_DROP_PARTITION_NON_EXISTENT);
          }
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_partarray_expr_name_valid(const share::schema::ObPartitionSchema *&orig_part_schema,
                                                      const share::schema::ObPartitionSchema *alter_part_schema,
                                                      int64_t expr_num,
                                                      const ObString *split_part_name)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(orig_part_schema)
      || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret),
             KP(orig_part_schema), KP(alter_part_schema));
  } else if (OB_FAIL(orig_part_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(orig_part_schema));
  } else {
    const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
    ObPartition **part_array = alter_part_schema->get_part_array();
    ObPartition **orig_part_array = orig_part_schema->get_part_array();
    const int64_t orig_part_num = orig_part_schema->get_part_option().get_part_num();
    const ObPartitionFuncType part_func_type = orig_part_schema->get_part_option().get_part_func_type();
    if (OB_ISNULL(part_array) || OB_ISNULL(orig_part_array) || orig_part_num < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), KP(part_array), KP(orig_part_array), K(orig_part_num));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      ObPartition *alter_part = part_array[i];
      bool found = false;
      // Check whether the type and number of partition value meet the requirements
      const ObPartition *orig_part = orig_part_array[0];
      if (OB_ISNULL(orig_part) || OB_ISNULL(alter_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), KP(orig_part), KP(alter_part));
      } else if (OB_FAIL(check_part_expr_num_and_value_type(is_oracle_mode,
                                                            alter_part,
                                                            orig_part,
                                                            part_func_type,
                                                            expr_num))) {
        LOG_WARN("fail to check part", K(ret), K(part_func_type), K(expr_num), K(alter_part), K(orig_part));
      }
      // Check partition name for naming conflict
      for (int64_t j = 0; j < orig_part_num && OB_SUCC(ret) && !found; j++) {
        const ObPartition *part = orig_part_array[j];
        if (OB_ISNULL(part) || OB_ISNULL(alter_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), KP(part), KP(alter_part));
        } else if (ObCharset::case_insensitive_equal(part->get_part_name(), alter_part->get_part_name())) {
          if (OB_NOT_NULL(split_part_name)
              && ObCharset::case_insensitive_equal(part->get_part_name(), *split_part_name)) {
            found = false;
          } else {
            found = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (found) {
          ret = OB_ERR_SAME_NAME_PARTITION;
          LOG_WARN("duplicate partition name", K(part_array[i]->get_part_name()), K(ret));
          LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, part_array[i]->get_part_name().length(),
                         part_array[i]->get_part_name().ptr());
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_add_partition(const share::schema::ObPartitionSchema *&orig_part_schema,
                                          share::schema::ObPartitionSchema *&alter_part_schema,
                                          int64_t expr_num, bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  UNUSED(is_tablegroup);
  if (OB_ISNULL(orig_part_schema)
      || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret),
             KP(orig_part_schema), KP(alter_part_schema));
  } else {
    bool is_oracle_mode = false;
    const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(orig_part_schema->get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else if ((is_oracle_mode && orig_part_schema->get_all_part_num() + part_num > OB_MAX_PARTITION_NUM_ORACLE)
         || (!is_oracle_mode && orig_part_schema->get_all_part_num() + part_num > OB_MAX_PARTITION_NUM_MYSQL)) {
      ret = OB_TOO_MANY_PARTITIONS_ERROR;
      LOG_WARN("too partitions", K(ret),
               "partition cnt current", orig_part_schema->get_all_part_num(),
               "partition cnt to be added", part_num);
    } else if (OB_FAIL(check_partarray_expr_name_valid(orig_part_schema, alter_part_schema,
                                                       expr_num))) {
      LOG_WARN("failed to check new partition expr and name", K(ret));
    } else if (orig_part_schema->is_range_part()) {
      const ObRowkey *rowkey_last =
        &orig_part_schema->get_part_array()[orig_part_schema->get_part_option().get_part_num()- 1]->get_high_bound_val();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        const ObRowkey *rowkey_cur =
          &alter_part_schema->get_part_array()[i]->get_high_bound_val();
        if (*rowkey_cur <= *rowkey_last) {
          ret = OB_ERR_RANGE_NOT_INCREASING_ERROR;
          LOG_WARN("range values should increasing", K(ret), K(rowkey_cur), K(rowkey_last));
          const ObString &err_msg = orig_part_schema->get_part_array()[orig_part_schema->get_part_option().get_part_num()- 1]->get_part_name();
          LOG_USER_ERROR(OB_ERR_RANGE_NOT_INCREASING_ERROR, lib::is_oracle_mode() ? err_msg.length() : 0, err_msg.ptr());
        } else {
          rowkey_last = rowkey_cur;
        }
      }
    } else if (orig_part_schema->is_list_part()) {
      if (OB_FAIL(ddl_service_->check_add_list_partition(*orig_part_schema, *alter_part_schema))) {
        LOG_WARN("failed to check add list partition", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alter partition type", K(ret),
               "part type", orig_part_schema->get_part_option().get_part_func_type());
    }
  }
  return ret;
}
//1. check the validity of partition_option
//2. create or drop tablegroup partition
//3. create or drop table partition which is in one tablegroup
int ObTableGroupHelp::modify_partition_option(ObMySQLTransaction &trans,
                                              ObSchemaGetterGuard &schema_guard,
                                              const ObTablegroupSchema &tablegroup_schema,
                                              const ObAlterTablegroupArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  int64_t expr_num = tablegroup_schema.get_part_func_expr_num();
  ObTablegroupSchema &alter_tablegroup_schema = const_cast<ObTablegroupSchema&>(arg.alter_tablegroup_schema_);
  alter_tablegroup_schema.set_tablegroup_id(tablegroup_id);
  alter_tablegroup_schema.set_tenant_id(tenant_id);
  ObAlterTablegroupArg::ModifiableOptions alter_part_type = ObAlterTablegroupArg::MAX_OPTION;
  if (!arg.is_alter_partitions()) {
    // skip
  } else if (is_sys_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("modify sys tablegroup's partition optition is not supported", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::ADD_PARTITION)) {
      alter_tablegroup_schema.get_part_option().set_part_num(alter_tablegroup_schema.get_partition_num());
      alter_part_type = ObAlterTablegroupArg::ADD_PARTITION;
      if (OB_FAIL(modify_add_partition(trans,
                                       schema_guard,
                                       tablegroup_schema,
                                       alter_tablegroup_schema,
                                       new_schema_version,
                                       expr_num,
                                       arg))) {
        LOG_WARN("failed to modify add partition", K(ret));
      }
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::DROP_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::DROP_PARTITION;
      alter_tablegroup_schema.get_part_option().set_part_num(alter_tablegroup_schema.get_partition_num());
      if (OB_FAIL(modify_drop_partition(trans,
                                        schema_guard,
                                        tablegroup_schema,
                                        alter_tablegroup_schema,
                                        new_schema_version,
                                        expr_num,
                                        arg))) {
        LOG_WARN("failed to modify drop partition", K(ret));
      }
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::PARTITIONED_TABLE)) {
      alter_part_type = ObAlterTablegroupArg::PARTITIONED_TABLE;
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::REORGANIZE_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::REORGANIZE_PARTITION;
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::SPLIT_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::SPLIT_PARTITION;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
    }
    if (OB_FAIL(ret)) {
    } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type
               || ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type
               || ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("split partition in tablegroup is not supported", KR(ret), K(tablegroup_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition in tablegroup");
    }
  }
  return ret;
}
//drop partition
//check alter tabletablegroup partition, batch modify table schema, modify tablegroup schema
int ObTableGroupHelp::modify_drop_partition(common::ObMySQLTransaction &trans,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            const share::schema::ObTablegroupSchema &orig_tablegroup_schema,
                                            share::schema::ObTablegroupSchema &inc_tablegroup_schema,
                                            const int64_t new_schema_version,
                                            const int64_t expr_num,
                                            const obrpc::ObAlterTablegroupArg &arg)
{
  int ret = OB_SUCCESS;
  const ObPartitionSchema *orig_schema = &orig_tablegroup_schema;
  ObPartitionSchema *alter_schema = &inc_tablegroup_schema;
  ObTablegroupSchema new_tablegroup_schema;
  bool is_tablegroup = true;
  if (PARTITION_LEVEL_ZERO == orig_schema->get_part_level()) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_WARN("unsupport management on non-partition table", K(ret));
  } else if (!orig_schema->is_list_part() && !orig_schema->is_range_part()) {
    ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
    LOG_WARN("can only be used on RANGE/LIST partitions", K(ret));
  } else if (OB_FAIL(check_alter_partition(orig_schema,
                                           alter_schema,
                                           ObAlterTablegroupArg::DROP_PARTITION,
                                           expr_num,
                                           is_tablegroup))) {
    LOG_WARN("failed to check alter partition", K(ret), K(orig_schema), K(alter_schema));
  } else if (OB_FAIL(batch_modify_table_partitions(trans,
                                                   schema_guard,
                                                   inc_tablegroup_schema,
                                                   orig_tablegroup_schema,
                                                   new_schema_version,
                                                   expr_num,
                                                   ObAlterTablegroupArg::DROP_PARTITION))) {
    LOG_WARN("failed to modify table partition", K(ret), K(orig_schema), K(alter_schema));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or sql_proxy is null", K(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(ddl_operator.drop_tablegroup_partitions(orig_tablegroup_schema,
                                                        inc_tablegroup_schema,
                                                        new_schema_version,
                                                        new_tablegroup_schema,
                                                        trans,
                                                        &(arg.ddl_stmt_str_)))) {
      LOG_WARN("fail to drop tablegroup partition", K(ret), K(orig_schema), K(new_tablegroup_schema));
    }
  }
  return ret;
}
// Add partition

// Modify the table schema, fill table partition name, check table partition name conflicts,
// fill tablegroup partition name, check tablegroup partition, modify tablegroup schema
int ObTableGroupHelp::modify_add_partition(common::ObMySQLTransaction &trans,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            const share::schema::ObTablegroupSchema &orig_tablegroup_schema,
                                            share::schema::ObTablegroupSchema &inc_tablegroup_schema,
                                            const int64_t new_schema_version,
                                            const int64_t expr_num,
                                            const obrpc::ObAlterTablegroupArg &arg)
{
  int ret = OB_SUCCESS;
  const ObPartitionSchema *orig_schema = &orig_tablegroup_schema;
  ObPartitionSchema *alter_schema = &inc_tablegroup_schema;
  ObTablegroupSchema new_tablegroup_schema;
  bool is_tablegroup = true;

  if (PARTITION_LEVEL_ZERO == orig_schema->get_part_level()) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_WARN("unsupport management on non-partition table", K(ret));
  } else if (!orig_schema->is_list_part() && !orig_schema->is_range_part()) {
    ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
    LOG_WARN("can only be used on RANGE/LIST partitions", K(ret));
  } else if (OB_FAIL(check_alter_partition(orig_schema,
                                           alter_schema,
                                           ObAlterTablegroupArg::ADD_PARTITION,
                                           expr_num,
                                           is_tablegroup))) {
    LOG_WARN("failed to check tablegroup schema", K(ret), K(inc_tablegroup_schema));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or sql_proxy is null", K(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(ddl_operator.add_tablegroup_partitions(orig_tablegroup_schema,
                                                       inc_tablegroup_schema,
                                                       new_schema_version,
                                                       new_tablegroup_schema,
                                                       trans,
                                                       &(arg.ddl_stmt_str_)))) {
      LOG_WARN("fail to add tablegroup partition", K(ret), K(orig_tablegroup_schema), K(inc_tablegroup_schema));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_modify_table_partitions(trans,
                                                   schema_guard,
                                                   inc_tablegroup_schema,
                                                   inc_tablegroup_schema,
                                                   new_schema_version,
                                                   expr_num,
                                                   ObAlterTablegroupArg::ADD_PARTITION))) {
    // add_partition does not construct new_tablegroup_schema, use inc instead
    LOG_WARN("failed to modify table schema", K(ret), K(inc_tablegroup_schema));
  }

  return ret;
}

int ObTableGroupHelp::batch_modify_table_partitions(
    ObMySQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    ObTablegroupSchema &inc_tablegroup_schema,
    const ObTablegroupSchema &new_tablegroup_schema,
    const int64_t new_schema_version,
    const int64_t expr_num,
    ObAlterTablegroupArg::ModifiableOptions alter_part_type)
{
  int ret = OB_SUCCESS;
  const int64_t tablegroup_id = new_tablegroup_schema.get_tablegroup_id();
  uint64_t tenant_id = new_tablegroup_schema.get_tenant_id();
  ObArray<const ObTableSchema*> table_schemas;
  if (OB_INVALID_ID == tablegroup_id || is_sys_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablegroup_id", K(ret), K(tablegroup_id));
  } else if (new_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(new_schema_version));
  } else if (ObAlterTablegroupArg::MAX_OPTION == alter_part_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(alter_part_type));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup", K(ret), K(tenant_id), K(tablegroup_id));
  } else {
    bool is_tablegroup = false;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObTableSchema &orig_table_schema = *(table_schemas.at(i));
      if (!orig_table_schema.has_partition()) {
        // No need to deal with tables without partition
        continue;
      }
      AlterTableSchema alter_table_schema;
      bool is_split = false;
      if (ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type
          || ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type
          || ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type) {
        is_split = true;
        // TODO: Need to support merge in the future
      }
      // When the index is not available, partition management is prohibited.
      // Does not need to check whether the partition key meets the requirements of the index key for tablegroup
      if (OB_FAIL(ddl_service_->check_index_valid_for_alter_partition(orig_table_schema,
                                                                      schema_guard,
                                                                      ObAlterTablegroupArg::DROP_PARTITION == alter_part_type,
                                                                      is_split))) {
        LOG_WARN("failed to check index valid", K(ret), "alter_part_type", alter_part_type,
                 K(is_split), K(orig_table_schema));
      } else if (OB_FAIL(alter_table_schema.assign(orig_table_schema))) {
        LOG_WARN("failed to assign from origin table schema", K(ret), K(orig_table_schema));
      } else if (OB_FAIL(alter_table_schema.assign_tablegroup_partition(inc_tablegroup_schema))) {
        LOG_WARN("fail to assign from tablegroup_schema", K(ret), K(orig_table_schema),
            K(inc_tablegroup_schema));
      } else {
        ObPartitionSchema *inc_table = &alter_table_schema;
        const ObPartitionSchema *orig_table = &orig_table_schema;
        if (OB_FAIL(check_alter_partition(orig_table, inc_table, alter_part_type,
                                          expr_num, is_tablegroup))) {
          LOG_WARN("fail to check partition optition", K(ret));
        } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type
                   && 1 == alter_table_schema.get_part_option().get_part_num()) {
          // When partition num is 1, no real split is performed, and have already written the table
          // schema for all tables in the tablegroup, no further processing is required
        } else {
          ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
          if (ObAlterTablegroupArg::ADD_PARTITION == alter_part_type) {
             if (OB_FAIL(add_table_partition_in_tablegroup(orig_table_schema,
                                                           inc_tablegroup_schema,
                                                           new_schema_version,
                                                           alter_table_schema,
                                                           trans))) {
              LOG_WARN("fail to add table partition", K(ret));
            }
          } else if (ObAlterTablegroupArg::DROP_PARTITION == alter_part_type) {
            ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
            if (OB_FAIL(ddl_operator.drop_table_partitions(orig_table_schema,
                                                           alter_table_schema,
                                                           new_schema_version,
                                                           trans))) {
              LOG_WARN("fail to drop table partitions", K(ret));
            }
          } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("split partition in tablegroup is not supported",
                     KR(ret), K(orig_table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition in tablegroup");
          } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type
                     || ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("split partition in tablegroup is not supported",
                     KR(ret), K(orig_table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition in tablegroup");
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_part_expr_num_and_value_type(
    const bool is_oracle_mode,
    const ObPartition *alter_part,
    const ObPartition *orig_part,
    const ObPartitionFuncType part_type,
    int64_t expr_num)
{
  int ret = OB_SUCCESS;
  const bool is_check_value = true;
  lib::CompatModeGuard g(is_oracle_mode ?
                    lib::Worker::CompatMode::ORACLE :
                    lib::Worker::CompatMode::MYSQL);
  if (OB_ISNULL(alter_part) || OB_ISNULL(orig_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter_part is null", K(ret));
  } else if (expr_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr num is invalid", K(ret), K(expr_num));
  } else if (is_range_part(part_type)) {
    const ObRowkey rowkey = alter_part->get_high_bound_val();
    const ObRowkey tg_rowkey = orig_part->get_high_bound_val();
    if (PARTITION_FUNC_TYPE_RANGE == part_type) {
      // For range partition, need to ensure that the number of expressions is 1,
      // and the value is an integer
      if (1 != rowkey.get_obj_cnt()) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret), "expr_num", rowkey.get_obj_cnt());
      } else if (OB_ISNULL(rowkey.get_obj_ptr()) || OB_ISNULL(tg_rowkey.get_obj_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey obj ptr is null", K(ret), KPC(alter_part));
      } else {
        const ObObj &obj = rowkey.get_obj_ptr()[0];
        if (obj.is_max_value()) {
          // just pass
        } else if (!ob_is_integer_type(obj.get_type())) {
          ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
          LOG_WARN("obj type is invalid", K(ret), K(rowkey));
          LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                         alter_part->get_part_name().length(),
                         alter_part->get_part_name().ptr());
        }
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type) {
      // For range column partition, need to ensure that the number of partition expression is
      // consistent with that of the tablegroup
      if (expr_num != rowkey.get_obj_cnt()) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near",
                 K(ret), K(expr_num), "obj_cnt", rowkey.get_obj_cnt());
      } else if (expr_num != tg_rowkey.get_obj_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj cnt not matched", K(ret), K(expr_num), "obj_cnt", tg_rowkey.get_obj_cnt());
      } else if (OB_ISNULL(rowkey.get_obj_ptr()) || OB_ISNULL(tg_rowkey.get_obj_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey obj ptr is null", K(ret), KPC(alter_part), KPC(orig_part));
      } else {
        ObCastCtx cast_ctx(const_cast<ObPartition *>(alter_part)->get_allocator(), NULL,
                           CM_NONE, ObCharset::get_system_collation());
        for (int64_t k = 0; k < rowkey.get_obj_cnt() && OB_SUCC(ret); k++) {
          const ObObj &obj = rowkey.get_obj_ptr()[k];
          ObObj &tg_obj = const_cast<ObObj&>(tg_rowkey.get_obj_ptr()[k]);
          if (obj.is_max_value() ||tg_obj.is_max_value()) {
            // just pass
          } else if (!sql::ObResolverUtils::is_valid_partition_column_type(obj.get_type(), part_type, is_check_value)) {
            ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
            LOG_WARN("obj type is invalid", K(ret), K(obj));
            LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                alter_part->get_part_name().length(),
                alter_part->get_part_name().ptr());
          } else if (obj.get_type() != tg_obj.get_type()) {
            if (!ObPartitionUtils::is_types_equal_for_partition_check(
                is_oracle_mode, obj.get_type(), tg_obj.get_type())) {
              ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
              LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
              LOG_WARN("object type is invalid ", K(ret), KPC(alter_part), KPC(orig_part), K(is_oracle_mode));
            } else if (OB_FAIL(ObObjCaster::to_type(obj.get_type(),
                                                    cast_ctx,
                                                    tg_obj,
                                                    tg_obj))) {
              LOG_WARN("failed to cast object", K(obj), K(ret), K(k));
            }
          }
        }
      }
    }
  } else if (is_list_part(part_type)) {
    const common::ObIArray<common::ObNewRow>* new_rowkey = &(alter_part->get_list_row_values());
    const common::ObIArray<common::ObNewRow>* orig_rowkey = &(orig_part->get_list_row_values());
    if (1 > new_rowkey->count() || 1 > orig_rowkey->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list value is empty", K(ret),
               "orig num", orig_rowkey->count(), "new num", new_rowkey->count());
    } else if (PARTITION_FUNC_TYPE_LIST == part_type) {
      int64_t count = new_rowkey->count();
      for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
        if (1 != new_rowkey->at(index).get_count()) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near",
                   K(ret), K(index), "obj count", new_rowkey->at(index).get_count());
        } else if (new_rowkey->at(index).get_cell(0).is_max_value()) {
          // No need to check default partition
        } else if (!ob_is_integer_type(new_rowkey->at(index).get_cell(0).get_type())) {
          // Currently only support int type for list partition
          ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
          LOG_WARN("obj type is invalid", K(ret), K(index), K(new_rowkey));
          LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                         alter_part->get_part_name().length(),
                         alter_part->get_part_name().ptr());
        }
      }
    } else if (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      if ((1 == new_rowkey->count()
           && 1 == new_rowkey->at(0).get_count()
           && new_rowkey->at(0).get_cell(0).is_max_value())) {
        // There will only be one default partition, and the semantics is different from
        // the maxvalue of the range partition
      } else {
        int64_t count = new_rowkey->count();
        for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
          int64_t obj_count = new_rowkey->at(index).get_count();
          if (obj_count != expr_num) {
            ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
            LOG_WARN("Inconsistency in usage of column lists for partitioning near",
                     K(ret), K(expr_num), K(obj_count));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
            const ObObj &obj = new_rowkey->at(index).get_cell(i);
            if (!sql::ObResolverUtils::is_valid_partition_column_type(obj.get_type(), part_type, is_check_value)) {
              ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
              LOG_WARN("obj type is invalid", K(ret), K(obj));
              LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                             alter_part->get_part_name().length(),
                             alter_part->get_part_name().ptr());
            }
          }
        }// end for
        if (OB_FAIL(ret)) {
        } else if (1 == orig_rowkey->count()
                   && 1 == orig_rowkey->at(0).get_count()
                   && orig_rowkey->at(0).get_cell(0).is_max_value()) {
          //pass nothing to check
        } else {
          // Compare the number of column partition expressions is consistent
          int64_t count = new_rowkey->count();
          for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
            if (new_rowkey->at(index).get_count() != orig_rowkey->at(0).get_count()) {
              ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
              LOG_WARN("Inconsistency in usage of column lists for partitioning near",
                       K(ret), K(index), "obj count", new_rowkey->at(index).get_count(),
                       "orig count", orig_rowkey->at(0).get_count());
            } else {
              int64_t obj_count = new_rowkey->at(index).get_count();
              ObCastCtx cast_ctx(const_cast<ObPartition *>(alter_part)->get_allocator(), NULL, CM_NONE, ObCharset::get_system_collation());
              for (int64_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
                ObObj &obj = const_cast<ObObj&>(new_rowkey->at(index).get_cell(i));
                if (obj.get_type() != orig_rowkey->at(0).get_cell(i).get_type()) {
                  if (!ObPartitionUtils::is_types_equal_for_partition_check(
                      is_oracle_mode, obj.get_type(), orig_rowkey->at(0).get_cell(i).get_type())) {
                    ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
                    LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
                    LOG_WARN("object type is invalid ", K(ret), KPC(alter_part), KPC(orig_part), K(is_oracle_mode));
                  } else if (OB_FAIL(ObObjCaster::to_type(orig_rowkey->at(0).get_cell(i).get_type(),
                                                          cast_ctx,
                                                          obj,
                                                          obj))) {
                    LOG_WARN("failed to cast object", K(orig_rowkey->at(0).get_cell(i)), K(ret));
                  }
                }
              }//end for check obj type
            }
          }// end for check newrow
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support range/range column, list/list columns partition", K(ret), K(part_type));
  }
  return ret;
}
//////////////////////////////
//////////////////////////////

int ObTableGroupHelp::add_table_partition_in_tablegroup(
    const share::schema::ObTableSchema &orig_table_schema,
    const share::schema::ObTablegroupSchema &inc_tablegroup_schema,
    const int64_t schema_version,
    share::schema::AlterTableSchema &alter_table_schema,
    common::ObMySQLTransaction &client)
{
  return OB_NOT_SUPPORTED;
}

} // end namespace rootserver
} // end namespace oceanbase
