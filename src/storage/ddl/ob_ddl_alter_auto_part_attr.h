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

#ifndef OCEANBASE_STORAGE_OB_DDL_ALTER_AUTO_PART_ATTR_H_
#define OCEANBASE_STORAGE_OB_DDL_ALTER_AUTO_PART_ATTR_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace storage
{

class ObAlterAutoPartAttrOp final
{
public:
  ObAlterAutoPartAttrOp(rootserver::ObDDLService &ddl_service);
  virtual ~ObAlterAutoPartAttrOp() = default;

  int alter_table_auto_part_attr_if_need(
      const obrpc::ObAlterTableArg &alter_table_arg, 
      const share::ObDDLType ddl_type,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObTableSchema &table_schema,
      rootserver::ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int alter_global_indexes_auto_part_attribute_offline(
      obrpc::ObAlterTableArg &alter_table_arg,
      ObTableSchema &new_index_schema);
  void switch_ddl_type_if_need(
      const obrpc::ObAlterTableArg &alter_table_arg, 
      share::ObDDLType &ddl_type);
  int check_alter_table_partition_attr(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const bool is_oracle_mode,
      share::ObDDLType &ddl_type);
  int alter_table_partition_attr(
      obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema);
private:
  int get_part_key_column_ids(
      const ObTableSchema &table_schema, 
      ObIArray<uint64_t> &part_key_ids);
  int check_part_key_column_type(
      const ObTableSchema &table_schema, 
      const ObPartitionOption &alter_part_option, 
      bool &is_valid_part_column);
  int check_and_set_table_auto_part_func(
      const ObPartitionOption &alter_part_option, 
      ObTableSchema &table_schema);
  int alter_global_indexes_auto_part_attribute_online(
      const ObPartitionOption &part_option,
      const ObTableSchema &table_schema,
      ObSchemaGetterGuard &schema_guard,
      rootserver::ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans, 
      ObArray<uint64_t> &modified_index_type_ids);
  int sync_aux_tables_partition_option(
      const ObTableSchema &table_schema,
      ObSchemaGetterGuard &schema_guard,
      rootserver::ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans, 
      ObArray<uint64_t> &modified_index_type_ids);
  int check_auto_part_table_unique_index(
      const ObTableSchema &table_schema,
      ObString &alter_table_part_func_expr,
      ObSchemaGetterGuard &schema_guard);
  int update_global_auto_split_attr(
      const ObPartitionOption &alter_part_option, 
      ObTableSchema &new_index_schema);
  int extract_potential_partition_func_type(
      const ObTableSchema &table_schema,
      const ObString &part_func_expr,
      ObPartitionFuncType &part_func_type);
  int switch_global_local_index_type(
      const ObTableSchema &index_schema, 
      ObIndexType& index_type);
  int lock_for_modify_auto_part_size(
      const ObTableSchema &table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObMySQLTransaction &trans);
private:
  rootserver::ObDDLService *ddl_service_;
};


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_DDL_ALTER_AUTO_PART_ATTR_H_
