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

  int init(
      share::schema::ObMultiVersionSchemaService &schema_service,
      rootserver::ObDRWorker &task_worker);
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_full_row_(const share::schema::ObTableSchema *table,
                    const ObLSReplicaTaskDisplayInfo &task_stat,
                    common::ObIArray<Column> &columns);
private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  rootserver::ObDRWorker *task_worker_;
  common::ObArenaAllocator arena_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLSReplicaTaskPlan);
};
}//end namespace rootserver
}//end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_H_
