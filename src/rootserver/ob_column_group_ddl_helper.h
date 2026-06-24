/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_COLUMN_GROUP_DDL_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_COLUMN_GROUP_DDL_HELPER_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace obrpc
{
class ObAlterTableArg;
}
namespace share
{
namespace schema
{
class ObTableSchema;
class AlterTableSchema;
}
}
namespace rootserver
{
class ObDDLOperator;
class ObColumnGroupDDLHelper
{
public:
  static int add_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                              const share::schema::ObTableSchema &ori_table_schema,
                              share::schema::ObTableSchema &new_table_schema);
  static int drop_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                               const share::schema::ObTableSchema &ori_table_schema,
                               share::schema::ObTableSchema &new_table_schema);
  static int alter_column_group(obrpc::ObAlterTableArg &alter_table_arg,
                                const share::schema::ObTableSchema &origin_table_schema,
                                share::schema::ObTableSchema &new_table_schema);
  static int update_column_group_table_inplace(const share::schema::ObTableSchema &origin_table_schema,
                                               const share::schema::ObTableSchema &new_table_schema,
                                               ObDDLOperator &ddl_operator,
                                               common::ObMySQLTransaction &trans);
  static int rebuild_column_groups(share::schema::ObTableSchema &new_table_schema);
  static int check_alter_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                                      const share::schema::ObTableSchema &orig_schema,
                                      share::ObDDLType &ddl_type);
  static int add_column_to_column_group(const share::schema::ObTableSchema &origin_table_schema,
                                        const share::schema::AlterTableSchema &alter_table_schema,
                                        share::schema::ObTableSchema &new_table_schema,
                                        ObDDLOperator &ddl_operator,
                                        common::ObMySQLTransaction &trans);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_COLUMN_GROUP_DDL_HELPER_H_
