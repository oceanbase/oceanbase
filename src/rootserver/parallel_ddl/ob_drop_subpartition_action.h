/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_SUBPARTITION_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_DROP_SUBPARTITION_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"

namespace oceanbase
{
namespace rootserver
{

class ObDropSubpartitionAction : public ObAlterTablePartitionAction
{
public:
  explicit ObDropSubpartitionAction(ObAlterTableHelper &helper)
    : ObAlterTablePartitionAction(helper) {}
  virtual ~ObDropSubpartitionAction() {}
  virtual int init() override;
  virtual int calc_schema_version_cnt() override;
  virtual int before_commit() override;
  virtual const char *get_action_name() const override { return "drop_subpartition"; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDropSubpartitionAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_SUBPARTITION_ACTION_H_
