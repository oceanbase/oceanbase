/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_struct.h"


namespace oceanbase
{
namespace observer
{

class ObTableLoadAssignedTaskManager
{
public:
	ObTableLoadAssignedTaskManager();
	~ObTableLoadAssignedTaskManager();
	int init();
	int add_assigned_task(ObDirectLoadResourceApplyArg &arg);
	int delete_assigned_task(ObTableLoadUniqueKey &task_key);
	int get_assigned_tasks(common::ObSArray<ObDirectLoadResourceApplyArg> &assigned_tasks);
private:
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey,
																	ObDirectLoadResourceApplyArg,
                                  common::hash::NoPthreadDefendMode>
		ResourceApplyMap;
	ResourceApplyMap assigned_tasks_map_;
	mutable lib::ObMutex mutex_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase