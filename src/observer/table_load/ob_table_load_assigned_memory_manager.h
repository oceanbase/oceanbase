/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"


namespace oceanbase
{
namespace observer
{

class ObTableLoadAssignedMemoryManager
{
public:
	// Minimum memory required in sort execution mode
	static const int64_t MIN_SORT_MEMORY_PER_TASK = 128LL * 1024LL * 1024LL;  // 128 MB
  ObTableLoadAssignedMemoryManager();
	~ObTableLoadAssignedMemoryManager();
	int init();
	int assign_memory(bool is_sort, int64_t assign_memory);
	int recycle_memory(bool is_sort, int64_t assign_memory);
	int64_t get_avail_memory();
	int refresh_avail_memory(int64_t avail_memory);
	int get_sort_memory(int64_t &sort_memory);
private:
	int64_t avail_sort_memory_;
	int64_t avail_memory_;
	int64_t chunk_count_;
  mutable lib::ObMutex mutex_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase