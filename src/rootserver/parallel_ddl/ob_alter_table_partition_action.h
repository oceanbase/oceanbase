/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_PARTITION_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_PARTITION_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_helper.h"
#include "rootserver/ob_ddl_service.h" // UpdateGlobalIndexOpType

namespace oceanbase
{
namespace rootserver
{

class ObAlterTablePartitionAction : public ObIAlterTableAction
{
public:
  explicit ObAlterTablePartitionAction(ObAlterTableHelper &helper)
    : ObIAlterTableAction(helper),
      orig_table_schemas_(),
      new_table_schemas_(),
      inc_table_schemas_(),
      del_table_schemas_(),
      upd_table_schemas_(),
      global_index_op_types_() {}
  virtual ~ObAlterTablePartitionAction() {}
  // subclasses must override
  virtual int init() override = 0;
  virtual int calc_schema_version_cnt() override = 0;
  // common implementations for all partition actions
  virtual int register_ddl_object_locks() override;
  virtual int check_legitimacy() override;
  virtual int generate_schemas() override;
  virtual int operate_schemas() override;
  virtual int before_commit() override { return OB_SUCCESS; }
  virtual int clean_on_fail() override { return OB_SUCCESS; }

protected:
  int generate_tables_array_(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int pre_decide_global_index_op_types_();
  int calc_global_index_schema_version_cnt_(int64_t &cnt) const;

protected:
  share::schema::ObTableSchema new_table_schema_;
  common::ObArray<const share::schema::ObTableSchema*> orig_table_schemas_;
  common::ObArray<share::schema::ObTableSchema*> new_table_schemas_;
  common::ObArray<share::schema::AlterTableSchema*> inc_table_schemas_;
  common::ObArray<share::schema::AlterTableSchema*> del_table_schemas_;
  common::ObArray<share::schema::ObTableSchema*> upd_table_schemas_;
  common::ObArray<ObDDLService::UpdateGlobalIndexOpType> global_index_op_types_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablePartitionAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_PARTITION_ACTION_H_
