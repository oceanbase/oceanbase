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

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/ob_table_load_service.h"


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