/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ADD_SUBPARTITION_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ADD_SUBPARTITION_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"

namespace oceanbase
{
namespace rootserver
{

class ObAddSubpartitionAction : public ObAlterTablePartitionAction
{
public:
  explicit ObAddSubpartitionAction(ObAlterTableHelper &helper) : ObAlterTablePartitionAction(helper) {}
  virtual ~ObAddSubpartitionAction() {}
  virtual int init() override;
  virtual int calc_schema_version_cnt() override;
  virtual int before_commit() override;
  virtual const char *get_action_name() const override { return "add_subpartition"; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAddSubpartitionAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ADD_SUBPARTITION_ACTION_H_
