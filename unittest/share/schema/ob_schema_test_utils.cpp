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

#include <gtest/gtest.h>
#include "share/system_variable/ob_system_variable.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
using namespace oceanbase::sql;

#define FILL_TABLEGROUP_SCHEMA(tg, t_id, tg_id, tg_name, comment)    \
{  tg.set_tenant_id(t_id);                                           \
   tg.set_tablegroup_id(combine_id(t_id, tg_id));                    \
   tg.set_tablegroup_name(tg_name);                                  \
   tg.set_comment(comment);                                          \
}

#define FILL_DATABASE_SCHEMA(db, t_id, db_id, db_name, comment)      \
{  db.set_tenant_id(t_id);                                           \
   db.set_database_id(combine_id(t_id, db_id));                      \
   db.set_database_name(db_name);                                    \
   db.set_comment(comment);                                          \
}

#define FILL_SCHEMA_OPERATION(so, sv, tn_id, db_id, tg_id,t_id, ot)  \
{                                                                    \
   so.schema_version_ = sv;                                          \
   so.tenant_id_ = tn_id;                                            \
   so.database_id_ = db_id;                                          \
   so.tablegroup_id_ = tg_id;                                        \
   so.table_id_ = t_id;                                              \
   so.op_type_ = ot;                                                 \
}

#define SCHEMA_ASSERT(left, right, val)                              \
{                                                                    \
  ASSERT_EQ(left.val, right.val);                                    \
}

#define CREATE_USER_TABLE_SCHEMA(ret, table_schema)                  \
{                                                                    \
  ObMySQLTransaction trans;                                          \
  ret = trans.start(&db_initer_.get_sql_proxy());                    \
  ret = multi_schema_service_.get_schema_service()->get_table_sql_service().create_table(table_schema, trans);          \
  const bool commit = true;                                          \
  ret = trans.end(commit);                                           \
}

#define DROP_USER_TABLE_SCHEMA(ret, table_schema)                     \
{                                                                     \
  ObMySQLTransaction trans;                                           \
  ret = trans.start(&db_initer_.get_sql_proxy());                     \
  ASSERT_EQ(OB_SUCCESS, ret);                                         \
  ret = multi_schema_service_.get_schema_service()->get_table_sql_service().drop_table(table_schema, trans);             \
  ASSERT_EQ(OB_SUCCESS, ret);                                         \
  const bool commit = true;                                           \
  ret = trans.end(commit);                                            \
  ASSERT_EQ(OB_SUCCESS, ret);                                         \
}

#define READ_TABLE_SCHEMA_AND_ASSERT_EQUAL(ret, table_schema, result_schema)                 \
{                                                                                            \
  ObMySQLTransaction trans;                                                                  \
  ret = trans.start(&db_initer_.get_sql_proxy());                                            \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                \
                                                                                             \
  ret = multi_schema_service_.get_schema_service()->get_table_schema(table_schema.get_table_id(), &trans, result_schema);\
  ASSERT_EQ(OB_SUCCESS, ret);                                                                \
  ObSchemaTestUtils::expect_table_eq(&table_schema, &result_schema);                         \
}

#define CREATE_TABLEGROUP_SCHEMA(ret, tg_schema)                     \
{                                                                    \
  ObMySQLTransaction trans;                                          \
  ret = trans.start(&db_initer_.get_sql_proxy());                    \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  ret = multi_schema_service_.get_schema_service()->get_tablegroup_sql_service().insert_tablegroup(tg_schema, trans);        \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  const bool commit = true;                                          \
  ret = trans.end(commit);                                           \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
}                                                                    \

#define CREATE_DATABASE_SCHEMA(ret, db_schema)                       \
{                                                                    \
  ObMySQLTransaction trans;                                          \
  ret = trans.start(&db_initer_.get_sql_proxy());                    \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  ret = multi_schema_service_.get_schema_service()->get_database_sql_service().insert_database(db_schema, trans);          \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  const bool commit = true;                                          \
  ret = trans.end(commit);                                           \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
}                                                                    \

#define INSERT_SCHEMA_OPERATION(ret, schema_op)                      \
{                                                                    \
  ObMySQLTransaction trans;                                          \
  ret = trans.start(&db_initer_.get_sql_proxy());                    \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  ret = multi_schema_service_.get_schema_service()->log_operation(schema_op, &trans);            \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
                                                                     \
  const bool commit = true;                                          \
  ret = trans.end(commit);                                           \
  ASSERT_EQ(OB_SUCCESS, ret);                                        \
}                                                                    \

//For test privilege
#define FILL_TENANT_INFO(tenant_info, tenant_id, tenant_name, replica_num,             \
                         zone_list, primary_zone, locked, comment)          \
{                                                                                      \
  tenant_info.set_tenant_id(tenant_id);                                                \
  tenant_info.set_tenant_name(tenant_name);                                            \
  tenant_info.set_zone_list(zone_list);                                                \
  tenant_info.set_primary_zone(primary_zone);                                          \
  tenant_info.set_locked(locked);                                                      \
  tenant_info.set_comment(comment);                                                    \
  tenant_info.set_locality("");\
}

#define FILL_USER_INFO(user, tenant_id, user_id, user_name, passwd, info, is_locked, priv_set) \
{                                                                                              \
  user.set_tenant_id(tenant_id);                                                               \
  user.set_user_id(user_id);                                                                   \
  user.set_user_name(user_name);                                                               \
  user.set_host(OB_DEFAULT_HOST_NAME);                                                    \
  user.set_passwd(passwd);                                                                     \
  user.set_info(info);                                                                         \
  user.set_is_locked(is_locked);                                                               \
  user.set_priv_set(priv_set);                                                                 \
}

#define FILL_DB_PRIV(db_priv, tenant_id, user_id, db_name, priv_set)                           \
{                                                                                              \
  db_priv.set_tenant_id(tenant_id);                                                            \
  db_priv.set_user_id(user_id);                                                                \
  db_priv.set_database_name(db_name);                                                          \
  db_priv.set_priv_set(priv_set);                                                              \
  db_priv.set_sort(get_sort(db_name));                                                         \
}

#define FILL_TABLE_PRIV(table_priv, tenant_id, user_id, db_name, table_name, priv_set)         \
{                                                                                              \
  table_priv.set_tenant_id(tenant_id);                                                         \
  table_priv.set_user_id(user_id);                                                             \
  table_priv.set_database_name(db_name);                                                       \
  table_priv.set_table_name(table_name);                                                       \
  table_priv.set_priv_set(priv_set);                                                           \
}

#define FILL_DB_PRIV_ORG_KEY(db_priv_key, tenant_id, user_id, db_name)                         \
{                                                                                              \
  db_priv_key.tenant_id_ = tenant_id;                                                          \
  db_priv_key.user_id_ = user_id;                                                              \
  db_priv_key.db_ = db_name;                                                                   \
}

#define FILL_TABLE_PRIV_KEY(table_priv, tenant_id, user_id, db_name, table_name)               \
{                                                                                              \
  table_priv.tenant_id_ = tenant_id;                                                           \
  table_priv.user_id_ = user_id;                                                               \
  table_priv.db_ = db_name;                                                                    \
  table_priv.table_ = table_name;                                                              \
}

#define CREATE_TENANT(ret, tenant)                                                             \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ObSysVariableSchema sys_variable_schema;                                                     \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ret = multi_schema_service_.get_schema_service()->get_tenant_sql_service().insert_tenant(tenant, trans);                                        \
  INIT_SYS_VARIABLE(trans, tenant, sys_variable_schema);     \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
}

#define DROP_TENANT(ret, tenant_id)                                                             \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ret = multi_schema_service_.get_schema_service()->get_tenant_sql_service().delete_tenant(tenant_id, trans);                                        \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
}

#define INIT_SYS_VARIABLE(trans, tenant_schema, sys_variable_schema) \
{ \
  if (OB_SYS_TENANT_ID == tenant_schema.get_tenant_id()) { \
    sys_variable_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE); \
  } else { \
    sys_variable_schema.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE); \
  } \
  sys_variable_schema.set_tenant_id(tenant_schema.get_tenant_id()); \
  sys_variable_schema.set_schema_version(tenant_schema.get_schema_version()); \
  ObSysParam sys_params[OB_MAX_SYS_PARAM_NUM]; \
  int64_t var_amount = ObSysVariables::get_amount(); \
  int64_t sys_param_num = 0; \
  ObSysVarSchema sysvar_schema; \
  for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; i++) { \
    ret = sys_params[sys_param_num].init(sys_variable_schema.get_tenant_id(), \
                                         "", \
                                         ObSysVariables::get_name(i).ptr(), \
                                         ObSysVariables::get_type(i), \
                                         ObSysVariables::get_value(i).ptr(), \
                                         ObSysVariables::get_min(i).ptr(), \
                                         ObSysVariables::get_max(i).ptr(), \
                                         ObSysVariables::get_info(i).ptr(), \
                                         ObSysVariables::get_flags(i)); \
    if (OB_FAIL(ret)) { \
      SHARE_SCHEMA_LOG(WARN, "fail to init param, ", K(tenant_schema), K(i), K(ret)); \
      break; \
    } else if (0 == ObSysVariables::get_name(i).compare(OB_SV_LOWER_CASE_TABLE_NAMES)) { \
      snprintf(sys_params[i].value_, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%d", sys_variable_schema.get_name_case_mode()); \
    } \
    if (OB_SUCC(ret)) { \
      sysvar_schema.reset(); \
      sysvar_schema.set_tenant_id(tenant_schema.get_tenant_id()); \
      if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_params[sys_param_num], sysvar_schema))) { \
        SHARE_SCHEMA_LOG(WARN, "convert sys param to sysvar schema failed", K(ret)); \
      } else if (OB_FAIL(sys_variable_schema.add_sysvar_schema(sysvar_schema))) { \
        SHARE_SCHEMA_LOG(WARN, "add sysvar schema to tenant schema failed", K(ret)); \
      } \
    } \
    ++sys_param_num; \
  } \
  if (OB_SUCC(ret)) { \
    ObSchemaOperationType operation_type = OB_DDL_MAX_OP;\
    ret = multi_schema_service_.get_schema_service()-> \
          get_sys_variable_sql_service().replace_sys_variable(sys_variable_schema, trans, operation_type); \
  } \
}

#define CREATE_USER_PRIV(ret, user)                                                            \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  ret = multi_schema_service_.get_schema_service()->get_user_sql_service().create_user(user, trans);                                             \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
}

#define GRANT_DATABASE_PRIV(ret, db_priv_key, priv_set)                                        \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  ret = multi_schema_service_.get_schema_service()->grant_database(db_priv_key, priv_set, trans);                         \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
}

#define GRANT_TABLE_PRIV(ret, table_priv_key, priv_set)                                        \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  ret = multi_schema_service_.get_schema_service()->grant_table(table_priv_key, priv_set, trans);                         \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
}

#define INIT_CORE_VERSION()                                        \
{                                                                                              \
  ObMySQLTransaction trans;                                                                    \
  ret = trans.start(&db_initer_.get_sql_proxy());                                              \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  ret = multi_schema_service_.get_schema_service()->log_core_operation(trans);                         \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
                                                                                               \
  const bool commit = true;                                                                    \
  ret = trans.end(commit);                                                                     \
  ASSERT_EQ(OB_SUCCESS, ret);                                                                  \
}

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

bool operator <(const ObTableSchema &lv, const ObTableSchema &rv)
{
  bool res = lv.get_tenant_id() < rv.get_tenant_id();
  if (lv.get_tenant_id() == rv.get_tenant_id()) {
    res = lv.get_table_id() < rv.get_table_id();
  }
  return res;
};

bool operator <(const ObTablegroupSchema &lv, const ObTablegroupSchema &rv)
{
  bool res = lv.get_tenant_id() < rv.get_tenant_id();
  if (lv.get_tenant_id() == rv.get_tenant_id()) {
    res = lv.get_tablegroup_id() < rv.get_tablegroup_id();
  }
  return res;
};

bool operator <(const ObDatabaseSchema &lv, const ObDatabaseSchema &rv)
{
  bool res = lv.get_tenant_id() < rv.get_tenant_id();
  if (lv.get_tenant_id() == rv.get_tenant_id()) {
    res = lv.get_database_id() < rv.get_database_id();
  }
  return res;
};

class ObSchemaTestUtils
{
public:
  static void assert_view_equal(const ObViewSchema &l_view, const ObViewSchema &r_view);
  static void assert_expire_condition_equal(const  ObString &l_ec, const ObString &r_ec);
  static void assert_tg_equal(const ObTablegroupSchema &l_tg, const ObTablegroupSchema &r_tg);
  static void assert_db_equal(const ObDatabaseSchema &l_db, const ObDatabaseSchema &r_db);
  static void assert_schema_operation_equal(const ObSchemaOperation &l_db, const ObSchemaOperation &r_db);
  static void assert_tn_equal(const ObTenantSchema &l_tn, const ObTenantSchema &r_tn);

  static void check_hash_array(const ObTableSchema *table);
  static void expect_column_eq(const ObColumnSchemaV2 *column, const ObColumnSchemaV2 *new_column);
  static void expect_table_eq(const ObTableSchema *table, const ObTableSchema *new_table);
  static void fill_table_schema(ObTableSchema &table, uint64_t table_id, bool fill_column = false);
  static void fill_user_table_schema(
      ObTableSchema &table_schema,
      uint64_t tenant_id,
      uint64_t table_id,
      uint64_t tablegroup_id,
      uint64_t part_num,
      uint64_t replica_num,
      int64_t schema_version,
      bool fill_column);

  static void table_set_tenant(share::schema::ObTableSchema &table,
      const uint64_t tenant_id);
};

void ObSchemaTestUtils::table_set_tenant(share::schema::ObTableSchema &table,
    const uint64_t tenant_id)
{
  table.set_tenant_id(tenant_id);
  table.set_table_id(OB_INVALID_ID == table.get_table_id() ?
      table.get_table_id() : combine_id(tenant_id, table.get_table_id()));
  table.set_tablegroup_id(OB_INVALID_ID == table.get_tablegroup_id() ?
      table.get_tablegroup_id() : combine_id(tenant_id, table.get_tablegroup_id()));
  table.set_database_id(OB_INVALID_ID == table.get_database_id() ?
      table.get_database_id() : combine_id(tenant_id, table.get_database_id()));
}

void ObSchemaTestUtils::assert_tn_equal(const ObTenantSchema &l_tn, const ObTenantSchema &r_tn)
{
  ASSERT_EQ(l_tn.get_tenant_id(),    r_tn.get_tenant_id());
  ASSERT_STREQ(l_tn.get_tenant_name(),  r_tn.get_tenant_name());
  ASSERT_STREQ(l_tn.get_comment(),      r_tn.get_comment());
}


void ObSchemaTestUtils::assert_expire_condition_equal(const ObString &l_ec,
                                                             const ObString &r_ec)
{
  ASSERT_EQ(l_ec, r_ec);
}

void ObSchemaTestUtils::assert_view_equal(const ObViewSchema &l_view,
                                                 const ObViewSchema &r_view)
{

  ASSERT_STREQ(l_view.get_view_definition(),         r_view.get_view_definition());
  ASSERT_EQ(l_view.get_view_check_option(),          r_view.get_view_check_option());
  ASSERT_EQ(l_view.get_view_is_updatable(),          r_view.get_view_is_updatable());
}

void ObSchemaTestUtils::assert_tg_equal(const ObTablegroupSchema &l_tg,
                                        const ObTablegroupSchema &r_tg)
{
  ASSERT_EQ(l_tg.get_tenant_id(),                    r_tg.get_tenant_id());
  ASSERT_EQ(l_tg.get_tablegroup_id(),                r_tg.get_tablegroup_id());
  ASSERT_STREQ(l_tg.get_tablegroup_name_str(),       r_tg.get_tablegroup_name_str());
  ASSERT_STREQ(l_tg.get_comment(),                   r_tg.get_comment());
}

void ObSchemaTestUtils::assert_db_equal(const ObDatabaseSchema &l_db,
                                                  const ObDatabaseSchema &r_db)
{
  ASSERT_EQ(l_db.get_tenant_id(),                r_db.get_tenant_id());
  ASSERT_EQ(l_db.get_database_id(),              r_db.get_database_id());
  ASSERT_STREQ(l_db.get_database_name(),         r_db.get_database_name());
  ASSERT_STREQ(l_db.get_comment(),               r_db.get_comment());
}

void ObSchemaTestUtils::assert_schema_operation_equal(const ObSchemaOperation &l_so,
                                                                const ObSchemaOperation &r_so){

  ASSERT_EQ(l_so.schema_version_,         r_so.schema_version_);
  ASSERT_EQ(l_so.tablegroup_id_,          r_so.tablegroup_id_);
  ASSERT_EQ(l_so.database_id_,            r_so.database_id_);
  ASSERT_EQ(l_so.table_id_,               r_so.table_id_);
  ASSERT_EQ(l_so.tenant_id_,              r_so.tenant_id_);
  ASSERT_EQ(l_so.op_type_,                r_so.op_type_);
}

void ObSchemaTestUtils::check_hash_array(const ObTableSchema* table)
{
  for (ObTableSchema::const_column_iterator iter = table->column_begin();
      iter != table->column_end(); iter++)
  {
    const ObColumnSchemaV2* column = *iter;
    const ObColumnSchemaV2* new_column = table->get_column_schema(column->get_column_id());
    expect_column_eq(column, new_column);
  }
}

void ObSchemaTestUtils::expect_column_eq(const ObColumnSchemaV2* column, const ObColumnSchemaV2* new_column)
{
  // TODO baihua: add back
  // EXPECT_EQ(column->get_table_id(), new_column->get_table_id());
  EXPECT_EQ(column->get_column_id(), new_column->get_column_id());
  EXPECT_EQ(column->get_rowkey_position(), new_column->get_rowkey_position());
  EXPECT_EQ(column->get_tbl_part_key_pos(), new_column->get_tbl_part_key_pos());
  EXPECT_EQ(column->get_data_length(), new_column->get_data_length());
  EXPECT_EQ(column->get_data_type(), new_column->get_data_type());
  EXPECT_EQ(column->get_orig_default_value(), new_column->get_orig_default_value());
  EXPECT_EQ(column->get_cur_default_value(), new_column->get_cur_default_value());
  EXPECT_EQ(column->is_not_null_rely_column(), new_column->is_not_null_rely_column());
  EXPECT_EQ(column->is_not_null_enable_column(), new_column->is_not_null_enable_column());
  EXPECT_EQ(column->is_not_null_validate_column(), new_column->is_not_null_validate_column());
  EXPECT_EQ(column->get_charset_type(), new_column->get_charset_type());
  EXPECT_EQ(column->get_collation_type(), new_column->get_collation_type());
  EXPECT_EQ(column->get_data_precision(),new_column->get_data_precision());
  EXPECT_EQ(column->get_data_scale(),new_column->get_data_scale());
  //EXPECT_EQ(0 , memcmp(column->get_name(), new_column->get_name(), strlen(column->get_name())));
  //EXPECT_EQ(column->get_name_str(), new_column->get_name_str());
  EXPECT_EQ(0 , memcmp(column->get_comment(), new_column->get_comment(),
                       strlen(column->get_comment())));
}


void ObSchemaTestUtils::expect_table_eq(const ObTableSchema* table, const ObTableSchema* new_table)
{
  EXPECT_EQ(table->is_valid(), new_table->is_valid());

  int ret= OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ret = table->get_simple_index_infos(simple_index_infos);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos_new;
  ret = new_table->get_simple_index_infos(simple_index_infos_new);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(simple_index_infos.count(), simple_index_infos_new.count());
  for (int64_t i = 0; i < simple_index_infos.count(); ++i) {
    EXPECT_EQ(simple_index_infos.at(i), simple_index_infos_new.at(i));
  }
  EXPECT_EQ(table->get_tenant_id(), new_table->get_tenant_id());
  EXPECT_EQ(table->get_database_id(), new_table->get_database_id());
  EXPECT_EQ(table->get_tablegroup_id(), new_table->get_tablegroup_id());
  EXPECT_EQ(table->get_table_id(), new_table->get_table_id());
  EXPECT_EQ(table->get_index_tid_count(), new_table->get_index_tid_count());
  EXPECT_EQ(table->get_max_used_column_id(), new_table->get_max_used_column_id());
  EXPECT_EQ(table->get_rowkey_split_pos(), new_table->get_rowkey_split_pos());
  EXPECT_EQ(table->get_block_size(),new_table->get_block_size());
  EXPECT_EQ(table->is_use_bloomfilter(),new_table->is_use_bloomfilter());
  EXPECT_EQ(table->get_progressive_merge_num(),new_table->get_progressive_merge_num());
  EXPECT_EQ(table->get_load_type(), new_table->get_load_type());
  EXPECT_EQ(table->get_table_type(), new_table->get_table_type());
  EXPECT_EQ(table->get_index_type(), new_table->get_index_type());
  EXPECT_EQ(table->get_def_type(), new_table->get_def_type());
  EXPECT_EQ(table->get_part_level(), new_table->get_part_level());

  EXPECT_EQ(table->get_part_option(), new_table->get_part_option());
  EXPECT_EQ(table->get_sub_part_option(), new_table->get_sub_part_option());

  EXPECT_EQ(table->get_table_name_str(), new_table->get_table_name_str());
  EXPECT_EQ(0, memcmp(table->get_table_name(), new_table->get_table_name(),
        strlen(table->get_table_name())));
  EXPECT_EQ(0, memcmp(table->get_compress_func_name(), new_table->get_compress_func_name(),
        strlen(table->get_compress_func_name())));
  EXPECT_EQ(table->is_compressed(), new_table->is_compressed());
  //EXPECT_EQ(table->get_rowkey_info(), new_table->get_rowkey_info());
  EXPECT_EQ(table->get_charset_type(), new_table->get_charset_type());
  EXPECT_EQ(table->get_collation_type(), new_table->get_collation_type());
  EXPECT_EQ(table->get_data_table_id(), new_table->get_data_table_id());
  EXPECT_EQ(table->get_create_mem_version(), new_table->get_create_mem_version());
  EXPECT_EQ(table->get_index_status(), new_table->get_index_status());
  EXPECT_EQ(table->get_code_version(), new_table->get_code_version());
  EXPECT_EQ(table->get_schema_version(), new_table->get_schema_version());
  EXPECT_EQ(table->get_expire_info() ,new_table->get_expire_info());
  EXPECT_EQ(table->get_column_count(),new_table->get_column_count());
  OB_ASSERT(table->get_column_count() == new_table->get_column_count());
  EXPECT_EQ(table->get_index_tid_count(),new_table->get_index_tid_count());

  for (ObTableSchema::const_column_iterator iter = table->column_begin();
      iter != table->column_end(); iter++)
  {
    const ObColumnSchemaV2 * column = *iter;
    const ObColumnSchemaV2* new_column = new_table->get_column_schema(column->get_column_id());
    EXPECT_TRUE(NULL != new_column);
    ObSchemaTestUtils::expect_column_eq(column, new_column);
  }
}

void ObSchemaTestUtils::fill_table_schema(ObTableSchema &table_schema, uint64_t table_id, bool fill_column)
{
  table_schema.set_database_id(2);
  table_schema.set_tablegroup_id(2);
  table_schema.set_table_id(table_id);
  table_set_tenant(table_schema, 2);
  char buf[10];
  snprintf(buf, 8, "%lu", table_id);
  table_schema.set_table_name(buf);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_RAM);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_def_type(TABLE_DEF_TYPE_USER);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_max_used_column_id(39);
  table_schema.set_progressive_merge_num(3);
  table_schema.set_compress_func_name(ObString::make_string("lalala"));
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_comment("lalala");
  table_schema.set_block_size(900);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table_schema.set_data_table_id(1001);
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_GLOBAL);
  table_schema.set_part_level(PARTITION_LEVEL_TWO);
  table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_part_option().set_part_expr(ObString::make_string("part_func_expr"));
  table_schema.get_part_option().set_part_num(1024);
  table_schema.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_sub_part_option().set_part_expr(ObString::make_string("part_func_expr"));
  table_schema.get_sub_part_option().set_part_num(1024);
  table_schema.set_create_mem_version(3);
  table_schema.set_schema_version(316);

  if (fill_column) {
    ObColumnSchemaV2 column_schema_1;
    column_schema_1.set_column_id(29);
    column_schema_1.set_data_type(ObIntType);
    column_schema_1.set_column_name(ObString::make_string("price"));
    column_schema_1.set_rowkey_position(1);

    ObColumnSchemaV2 column_schema_2;
    column_schema_2.set_column_id(36);
    column_schema_2.set_data_type(ObIntType);
    column_schema_2.set_column_name(ObString::make_string("user_id"));
    column_schema_2.set_rowkey_position(2);

    ObColumnSchemaV2 column_schema_3;
    column_schema_3.set_column_id(37);
    column_schema_3.set_data_type(ObIntType);
    column_schema_3.set_column_name(ObString::make_string("item_type"));
    column_schema_3.set_rowkey_position(3);

    ObColumnSchemaV2 column_schema_4;
    column_schema_4.set_column_id(38);
    column_schema_4.set_data_type(ObIntType);
    column_schema_4.set_column_name(ObString::make_string("item_id"));
    column_schema_4.set_rowkey_position(4);

    int ret = table_schema.add_column(column_schema_1);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_2);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_3);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_4);
    ASSERT_EQ(OB_SUCCESS ,ret);
  }
}


void ObSchemaTestUtils::fill_user_table_schema(
    ObTableSchema &table_schema,
    uint64_t tenant_id,
    uint64_t table_id,
    uint64_t tablegroup_id,
    uint64_t part_num,
    uint64_t replica_num,
    int64_t schema_version,
    bool fill_column)
{
  UNUSED(replica_num);
  char buf[10];
  snprintf(buf, 8, "%lu", table_id);
  table_schema.set_table_name(buf);
  table_schema.set_database_id(2);
  table_schema.set_tablegroup_id(tablegroup_id);
  table_schema.set_data_table_id(table_id);
  table_schema.set_table_id(table_id);
  table_schema.get_part_option().set_part_num(part_num);
  table_set_tenant(table_schema, tenant_id);

  // FIXED
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_def_type(TABLE_DEF_TYPE_USER);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_max_used_column_id(39);
  table_schema.set_progressive_merge_num(3);
  table_schema.set_compress_func_name(ObString::make_string("lalala"));
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_comment("lalala");
  table_schema.set_block_size(900);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_part_level(PARTITION_LEVEL_ONE);
  table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_part_option().set_part_expr(ObString::make_string("part_func_expr"));
  table_schema.set_create_mem_version(3);
  table_schema.set_schema_version(schema_version);

  if (fill_column) {
    ObColumnSchemaV2 column_schema_1;
    column_schema_1.set_column_id(29);
    column_schema_1.set_data_type(ObIntType);
    column_schema_1.set_column_name(ObString::make_string("price"));
    column_schema_1.set_rowkey_position(1);

    ObColumnSchemaV2 column_schema_2;
    column_schema_2.set_column_id(36);
    column_schema_2.set_data_type(ObIntType);
    column_schema_2.set_column_name(ObString::make_string("user_id"));
    column_schema_2.set_rowkey_position(2);

    ObColumnSchemaV2 column_schema_3;
    column_schema_3.set_column_id(37);
    column_schema_3.set_data_type(ObIntType);
    column_schema_3.set_column_name(ObString::make_string("item_type"));
    column_schema_3.set_rowkey_position(3);

    ObColumnSchemaV2 column_schema_4;
    column_schema_4.set_column_id(38);
    column_schema_4.set_data_type(ObIntType);
    column_schema_4.set_column_name(ObString::make_string("item_id"));
    column_schema_4.set_rowkey_position(4);

    int ret = table_schema.add_column(column_schema_1);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_2);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_3);
    ASSERT_EQ(OB_SUCCESS ,ret);
    ret = table_schema.add_column(column_schema_4);
    ASSERT_EQ(OB_SUCCESS ,ret);
  }
}

} // end namespace schema
} // end namespace share
} // end namespace oceanbase
