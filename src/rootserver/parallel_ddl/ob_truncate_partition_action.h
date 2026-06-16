/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_TRUNCATE_PARTITION_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_TRUNCATE_PARTITION_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_partition_action.h"

namespace oceanbase
{
namespace rootserver
{

class ObTruncatePartitionAction : public ObAlterTablePartitionAction
{
public:
  explicit ObTruncatePartitionAction(ObAlterTableHelper &helper)
    : ObAlterTablePartitionAction(helper) {}
  virtual ~ObTruncatePartitionAction() {}
  virtual int init() override;
  virtual int calc_schema_version_cnt() override;
  virtual int before_commit() override;
  virtual const char *get_action_name() const override { return "truncate_partition"; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTruncatePartitionAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_TRUNCATE_PARTITION_ACTION_H_
