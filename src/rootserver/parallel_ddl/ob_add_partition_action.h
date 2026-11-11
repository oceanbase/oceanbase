/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ADD_PARTITION_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ADD_PARTITION_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"

namespace oceanbase
{
namespace rootserver
{

class ObAddPartitionAction : public ObAlterTablePartitionAction
{
public:
  explicit ObAddPartitionAction(ObAlterTableHelper &helper) : ObAlterTablePartitionAction(helper) {}
  virtual ~ObAddPartitionAction() {}
  virtual int init() override;
  virtual int check_legitimacy() override;
  virtual int calc_schema_version_cnt() override;
  virtual int before_commit() override;
  virtual const char *get_action_name() const override { return "add_partition"; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAddPartitionAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ADD_PARTITION_ACTION_H_
