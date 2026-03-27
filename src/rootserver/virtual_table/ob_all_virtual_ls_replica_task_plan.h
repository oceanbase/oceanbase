/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}
namespace rootserver
{
class ObDRWorker;
class ObLSReplicaTaskDisplayInfo;
class ObAllVirtualLSReplicaTaskPlan : public common::ObVirtualTableProjector
{
public:
  ObAllVirtualLSReplicaTaskPlan();
  virtual ~ObAllVirtualLSReplicaTaskPlan();
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_full_row_(const share::schema::ObTableSchema *table,
                    const ObLSReplicaTaskDisplayInfo &task_stat,
                    common::ObIArray<Column> &columns);
  int try_tenant_disaster_recovery_(const uint64_t tenant_id,
                                    ObDRWorker &task_worker);

private:
  common::ObArenaAllocator arena_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLSReplicaTaskPlan);
};
}//end namespace rootserver
}//end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_H_
