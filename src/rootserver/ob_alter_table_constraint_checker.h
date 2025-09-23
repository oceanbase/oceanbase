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
#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_CONSTRAINT_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_CONSTRAINT_CHECKER_H_
#include "lib/ob_define.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class AlterTableSchema;
}
}
namespace obrpc
{
class ObAlterTableArg;
}

namespace rootserver
{
class ObDDLService;
class ObAlterTableConstraintChecker {
public:

  static int check_can_change_cst_column_name(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version,
      bool &can_modify_column_name_and_constraint);

  static int check_can_add_cst_on_multi_column(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const uint64_t tenant_data_version,
      bool &can_add_cst_on_multi_column);
  static int check_is_change_cst_column_name(
        const share::schema::ObTableSchema &table_schema,
        const share::schema::AlterTableSchema &alter_table_schema,
        bool &change_cst_column_name);
  static int check_alter_table_constraint(
      rootserver::ObDDLService &ddl_service,
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version,
      share::ObDDLType &ddl_type);
  static int need_modify_not_null_constraint_validate(
      rootserver::ObDDLService &ddl_service,
      const obrpc::ObAlterTableArg &alter_table_arg,
      const uint64_t tenant_data_version,
      bool &is_add_not_null_col,
      bool &need_modify);
  static int modify_not_null_constraint_validate(
      const obrpc::ObAlterTableArg &alter_table_arg,
      AlterTableSchema &alter_table_schema);
  static int fill_new_constraint_info(
      const obrpc::ObAlterTableArg::AlterConstraintType op_type,
      const AlterTableSchema &tmp_alter_table_schema,
      const ObTableSchema &new_table_schema,
      AlterTableSchema &alter_table_schema);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_CONSTRAINT_CHECKER_H_
