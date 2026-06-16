/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_COLUMN_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_COLUMN_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_helper.h"

namespace oceanbase
{
namespace rootserver
{
class ObAlterTableColumnAction : public ObIAlterTableAction
{
public:
  explicit ObAlterTableColumnAction(ObAlterTableHelper &helper) : ObIAlterTableAction(helper) {}
  virtual ~ObAlterTableColumnAction() {}
  virtual int init() override;
  virtual int register_ddl_object_locks() override;
  virtual int generate_schemas() override;
  virtual int calc_schema_version_cnt() override;
  virtual int operate_schemas() override;
  virtual int before_commit() override;
  virtual int clean_on_fail() override;
  virtual const char *get_action_name() const override { return "column_action"; }

protected:
  // Map raw ObTableLockService retry ret codes (which can leak through
  // ObDDLLock::lock_for_common_ddl_in_trans) to OB_ERR_PARALLEL_DDL_CONFLICT
  // so the SQL layer can retry. drop column uses a different lock entry
  // (lock_for_online_drop_column_in_trans) that already maps retry codes to
  // OB_EAGAIN internally — drop maps OB_EAGAIN to OB_ERR_PARALLEL_DDL_CONFLICT
  // inline (drop_column_action.cpp:337) and does not call this helper.
  static void map_table_lock_retry_to_parallel_ddl_conflict_(int &ret);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableColumnAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_COLUMN_ACTION_H_
