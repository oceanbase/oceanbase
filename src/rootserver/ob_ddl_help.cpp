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

#include "lib/ob_errno.h"
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
#include "share/schema/ob_schema_service_sql_impl.h"
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
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (table_items.count() <= 0) {
    //nothing todo
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", KR(ret), K(tablegroup_id));
  } else if (is_sys_tablegroup_id(tablegroup_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not handle with sys tablegroup", KR(ret), K(tablegroup_id));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service is null", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    const bool is_index = false;
    const uint64_t tenant_id = arg.tenant_id_;
    ObString tablegroup_name = tablegroup_schema.get_tablegroup_name();
    // first table is used for tablegroup is empty, but add list's num more than one to compare partition
    const ObTableSchema *first_table_schema = NULL;
    int64_t table_items_count = table_items.count();
    ObArray<uint64_t> table_ids;
    bool duplicate_table = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_count; ++i) {
      duplicate_table = false;
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
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id), K(table_item));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(table_item.database_name_), to_cstring(table_item.table_name_));
        LOG_WARN("table not exist!", KR(ret), K(tenant_id), K(database_id), K(table_item));
      } else if (is_inner_table(table_schema->get_table_id())) {
        //the tablegroup of sys table must be oceanbase
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("sys table's tablegroup should be oceanbase", KR(ret), K(arg), KPC(table_schema));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set the tablegroup of system table besides oceanbase");
      } else {
        if (is_contain(table_ids, table_schema->get_table_id())) {
          duplicate_table = true;
        } else if (OB_FAIL(table_ids.push_back(table_schema->get_table_id()))) {
          LOG_WARN("fail to push back table", KR(ret));
        }
      }
      if (OB_SUCC(ret) && !duplicate_table) {
        ObTableSchema new_table_schema;
        ObSqlString sql;
        if (OB_FAIL(new_table_schema.assign(*table_schema))) {
          LOG_WARN("fail to assign schema", KR(ret));
        } else {
          new_table_schema.set_tablegroup_id(tablegroup_id);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_table_alter_tablegroup(schema_guard, first_table_schema, *table_schema, new_table_schema))) {
          LOG_WARN("fail to check primary zone and locality", KR(ret));
        } else if (OB_FAIL(sql.append_fmt("ALTER TABLEGROUP %.*s ADD TABLE %.*s.%.*s",
                                          tablegroup_name.length(),
                                          tablegroup_name.ptr(),
                                          table_item.database_name_.length(),
                                          table_item.database_name_.ptr(),
                                          table_item.table_name_.length(),
                                          table_item.table_name_.ptr()))) {
          LOG_WARN("failed to append sql", KR(ret));
        } else if (OB_FAIL(ddl_service_->check_tablegroup_in_single_database(schema_guard, new_table_schema))) {
          LOG_WARN("fail to check tablegroup in single database", KR(ret));
        } else {
          ObString sql_str = sql.string();
          if (OB_FAIL(ddl_operator.alter_tablegroup(schema_guard, new_table_schema, trans, &sql_str))) {
            LOG_WARN("ddl operator alter tablegroup failed", KR(ret), K(tenant_id), K(tablegroup_id));
          } else if (table_items_count >= 2 && i == 0) {
            first_table_schema = table_schema;
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
    const share::schema::ObTableSchema *first_table_schema,
    const share::schema::ObTableSchema &orig_table_schema,
    share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  bool can_alter_tablegroup = !is_sys_tablegroup_id(new_table_schema.get_tablegroup_id());
  ObString src_previous_locality_str;
  ObString dst_previous_locality_str;
  if (!can_alter_tablegroup) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cann't alter table's tablegroup", KR(ret),
             "src_tg_id", orig_table_schema.get_tablegroup_id(),
             "dst_tg_id", new_table_schema.get_tablegroup_id());
  } else if (OB_INVALID_ID == new_table_schema.get_tablegroup_id()) {
    // skip
  } else {
    if (OB_FAIL(check_table_partition_in_tablegroup(first_table_schema, new_table_schema, schema_guard))) {
      LOG_WARN("fail to check table partition in tablegroup", KR(ret), K(new_table_schema));
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
      LOG_WARN("fail to get tablegroup schema", KR(ret), K(tenant_id), K(orig_tg_id));
    } else if (OB_UNLIKELY(nullptr == orig_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", KR(ret), K(orig_tg_id));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == new_tg_id) {
      // Did not belong to any tablegroup
    } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, new_tg_id, new_tg))) {
      LOG_WARN("fail to get tablegroup schema", KR(ret), K(tenant_id), K(new_tg_id));
    } else if (OB_UNLIKELY(nullptr == new_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", KR(ret), K(new_tg_id));
    }
    if (OB_SUCC(ret)) {
      if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != new_table_schema.get_duplicate_scope()
          && OB_INVALID_ID != new_table_schema.get_tablegroup_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("duplicate table in tablegroup is not supported", KR(ret),
                 "table_id",new_table_schema.get_table_id(),
                 "tablegroup_id", new_table_schema.get_tablegroup_id());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate table in tablegroup");
      }
    }
  }
  return ret;
}

//need to check whether the partition type of tablegroup and table are consistent
int ObTableGroupHelp::check_table_partition_in_tablegroup(const ObTableSchema *first_table_schema,
                                                          ObTableSchema &table,
                                                          ObSchemaGetterGuard &schema_guard                                                              )
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  const ObTablegroupSchema *tablegroup = NULL;
  if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", KR(ret), K(tablegroup_id));
  } else if (is_sys_tablegroup_id(tablegroup_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not handle with sys tablegroup", KR(ret), K(tablegroup_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup))) {
    LOG_WARN("fail to get tablegroup schema", KR(ret), K(tenant_id), KT(tablegroup_id));
  } else if (OB_ISNULL(tablegroup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup schema is null", KR(ret), KT(tablegroup_id));
  } else if (table.is_in_splitting() || tablegroup->is_in_splitting()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table or tablegroup is splitting", KR(ret), K(table), K(tablegroup));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add table to tablegroup while either object is splitting");
  } else {
    // sort partition info in order, to prevent same value not in order from being misjudged
    if (OB_FAIL(ObSchemaServiceSQLImpl::sort_table_partition_info_v2(table))) {
      LOG_WARN("fail to sort table partition", K(ret));
    } else if (OB_FAIL(check_partition_option(*tablegroup, first_table_schema, table, schema_guard))) {
      LOG_WARN("fail to check partition option", KR(ret), KPC(tablegroup), K(table));
    }
  }
  return ret;
}

#define PRINT_CHECK_PARTITION_ERROR(ERROR_STRING, USER_ERROR) { \
  if (OB_SUCC(ret)) {\
    if (OB_FAIL(ERROR_STRING.append(USER_ERROR))) { \
        LOG_WARN("fail to append user error", KR(ret));\
    } else {\
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, ERROR_STRING.ptr());\
    }\
  }\
}\
//when modify tablegroup's sharding, we need to check all tables' partition info whether fit new sharding
int ObTableGroupHelp::check_all_table_partition_option(const ObTablegroupSchema &tablegroup_schema,
                                                       ObSchemaGetterGuard &schema_guard,
                                                       bool check_subpart,
                                                       bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = true;
  ObSqlString user_error;
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  ObArray<const schema::ObSimpleTableSchemaV2 *> table_schemas;

  if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail get table schemas from tablegroup", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (0 == table_schemas.count()) {
    // do nothing
  } else {
    int64_t table_schemas_count = table_schemas.count();
    const ObSimpleTableSchemaV2* primary_table_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_count && is_matched; i++) {
      if (OB_ISNULL(table_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(tablegroup_schema), K(i));
      } else if (OB_ISNULL(primary_table_schema)) {
        primary_table_schema = table_schemas.at(i);
      } else if (OB_FAIL(ObSimpleTableSchemaV2::compare_partition_option(*table_schemas.at(i), *primary_table_schema,
                                                                        check_subpart, is_matched, &user_error))) {
        LOG_WARN("fail to check partition option", KR(ret), K(*table_schemas.at(i)), K(*primary_table_schema));
      } else if (!is_matched) {
        LOG_WARN("two tables’ part method not consistent, not suit sharding type",
                K(tablegroup_id), K(*table_schemas.at(i)),
                K(*primary_table_schema), K(tablegroup_schema.get_sharding()));
        PRINT_CHECK_PARTITION_ERROR(user_error, ", modify tablegroup sharding attribute");
      }
    }
  }
  return ret;
}

//first table schema is for tablegroup is empty,but add list num more than one
// eg:alter tablegroup tg add test1,test2,test3;
// when tablegroup tg is empty, first table is test1, we need to compare other tables' partition with test1
// when tablegroup tg is not empty, we just ignore first table to use tablegroup's fist table;
int ObTableGroupHelp::check_table_partition_option(const ObTableSchema *table_schema,
                                                   const ObTableSchema *first_table_schema,
                                                   ObSchemaGetterGuard &schema_guard,
                                                   bool check_subpart,
                                                   bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = true;
  ObSqlString user_error;
  const ObSimpleTableSchemaV2 *tmp_table_schema = NULL;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", KR(ret));
  } else if (OB_FAIL(schema_guard.get_primary_table_schema_in_tablegroup(table_schema->get_tenant_id(),
                                                                         table_schema->get_tablegroup_id(),
                                                                         tmp_table_schema))) {
    LOG_WARN("fail get table schemas from tablegroup", KR(ret), K(table_schema->get_tenant_id()), K(table_schema->get_tablegroup_id()));
  } else if (OB_ISNULL(first_table_schema) && OB_ISNULL(tmp_table_schema)) {
    // do nothing
  } else {
    const ObSimpleTableSchemaV2* primary_table_schema = NULL;
    //1.if tablegroup is empty,just compare with add list's first table
    //2.if tablegroup not empty,ignore add list's fist table,just compare with tablegroup's fist table
    if (OB_NOT_NULL(tmp_table_schema)) {
      primary_table_schema = tmp_table_schema;
    } else if (OB_NOT_NULL(first_table_schema)) {
      primary_table_schema = first_table_schema;
    }
    if (OB_FAIL(ObSimpleTableSchemaV2::compare_partition_option(*primary_table_schema, *table_schema,
                                                                check_subpart, is_matched, &user_error))) {
      LOG_WARN("fail to check partition option", KR(ret), K(*primary_table_schema), K(*table_schema));
    } else if (!is_matched) {
      LOG_WARN("two tables’ part method not consistent, not suit sharding type",
            K(table_schema->get_tablegroup_id()), K(*primary_table_schema),
            K(*table_schema), K(check_subpart));
      PRINT_CHECK_PARTITION_ERROR(user_error, ", add table to tablegroup");
    }
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
    const ObTableSchema *first_table_schema,
    const ObTableSchema &table,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  const uint64_t table_id = table.get_table_id();
  if (tablegroup.get_sharding().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup sharding can not be empty", KR(ret));
  } else if (tablegroup.get_sharding() == OB_PARTITION_SHARDING_NONE) {
    is_matched = true;
  } else if (table.is_partitioned_table() && table.is_interval_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("interval part add to tablegroup when sharding is not NONE",
            KR(ret), K(tablegroup_id), K(table_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add interval part table to tablegroup when sharding is not NONE");
  } else if (tablegroup.get_sharding() == OB_PARTITION_SHARDING_PARTITION) {
    // check level one partitions info
    if (OB_FAIL(check_table_partition_option(&table, first_table_schema, schema_guard, false, is_matched))) {
      LOG_WARN("fail to check partition sharding type", KR(ret), K(tablegroup_id), K(table_id), K(is_matched));
    }
  } else if (tablegroup.get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
    //check level one and two partitions info
    if (OB_FAIL(check_table_partition_option(&table, first_table_schema, schema_guard, true, is_matched))) {
      LOG_WARN("fail to check adaptive sharding type", KR(ret), K(tablegroup_id), K(table_id), K(is_matched));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found sharding", KR(ret), K(tablegroup));
  }
  if (OB_SUCC(ret) && !is_matched) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("partition option not match", KR(ret));
  }
  return ret;
}

int ObTableGroupHelp::modify_sharding_type(const ObAlterTablegroupArg &arg,
                                           const ObTablegroupSchema &tablegroup_schema,
                                           common::ObMySQLTransaction &trans,
                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  ObTablegroupSchema new_tablegroup_schema;
  if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::SHARDING)) {
    if (is_sys_tablegroup_id(tablegroup_schema.get_tablegroup_id())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("modify sys tablegroup's sharding type is not allowed", KR(ret), K(tablegroup_schema.get_tablegroup_id()));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify sys tablegroup's sharding type");
    } else if (OB_FAIL(new_tablegroup_schema.assign(tablegroup_schema))) {
      LOG_WARN("fail to assign tablegroup schema", KR(ret), K(tablegroup_schema));
    } else if (OB_FAIL(new_tablegroup_schema.set_sharding(arg.alter_tablegroup_schema_.get_sharding()))) {
      LOG_WARN("fail to set tablegroup name", KR(ret), K(tablegroup_schema));
    } else if (new_tablegroup_schema.get_sharding().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new tablegroup schema's sharding should not be empty", KR(ret));
    } else if (new_tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_NONE) {
      is_matched = true;
    } else if (new_tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_PARTITION) {
      if (OB_FAIL(check_all_table_partition_option(new_tablegroup_schema, schema_guard, false, is_matched))) {
        LOG_WARN("fail to check table sharding partition", KR(ret), K(new_tablegroup_schema));
      }
    } else if (new_tablegroup_schema.get_sharding() == OB_PARTITION_SHARDING_ADAPTIVE) {
      if (OB_FAIL(check_all_table_partition_option(new_tablegroup_schema, schema_guard, true, is_matched))) {
        LOG_WARN("fail to check table sharding adaptive", KR(ret), K(new_tablegroup_schema));
      }
    }
    if (OB_SUCC(ret) && is_matched) {
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      // update sharding type
      if (OB_FAIL(ddl_operator.alter_tablegroup(new_tablegroup_schema, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("fail to alter tablegroup sharding type", KR(ret), K(new_tablegroup_schema));
      }
    } else if (OB_SUCC(ret) && !is_matched) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can not modify sharding type", KR(ret), K(new_tablegroup_schema.get_sharding()));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
