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

#include "lib/ob_define.h"
#include "lib/task/ob_timer.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadService;

class ObTableLoadResourceManager
{
public:
	static const int64_t MAX_INIT_RETRY_TIMES = 3;
	ObTableLoadResourceManager();
	~ObTableLoadResourceManager();
	int init();
	int start();
	void stop();
	int wait();
	void destroy();
	int resume();
	void pause();
	int apply_resource(ObDirectLoadResourceApplyArg &arg, ObDirectLoadResourceOpRes &res);
	int release_resource(ObDirectLoadResourceReleaseArg &arg);
	int update_resource(ObDirectLoadResourceUpdateArg &arg);
	void check_resource();
	int refresh_and_check(bool first_check = false);
	int release_all_resource();
private:
	class ObResourceCtx
	{
	public:
		ObResourceCtx()
			: thread_remain_(0), memory_remain_(0), thread_total_(0), memory_total_(0)
		{
		}
		ObResourceCtx(int64_t thread_remain, int64_t memory_remain,
									int64_t thread_total, int64_t memory_total)
			: thread_remain_(thread_remain), memory_remain_(memory_remain), thread_total_(thread_total), memory_total_(memory_total)
		{
		}
		TO_STRING_KV(K_(thread_remain), K_(memory_remain), K_(thread_total), K_(memory_total));
	public:
		int64_t thread_remain_;
		int64_t memory_remain_;
		int64_t thread_total_;
		int64_t memory_total_;
	};
	class ObResourceAssigned
	{
	public:
		ObResourceAssigned()
			: miss_counts_(0)
		{
		}
		ObResourceAssigned(const ObDirectLoadResourceApplyArg &arg)
			: apply_arg_(arg), miss_counts_(0)
		{
		}
		ObDirectLoadResourceApplyArg apply_arg_;
		uint64_t miss_counts_;
	};
	class ObRefreshAndCheckTask : public common::ObTimerTask
	{
  public:
    ObRefreshAndCheckTask(ObTableLoadResourceManager &manager)
			: manager_(manager), tenant_id_(common::OB_INVALID_ID), is_inited_(false)
		{
		}
    virtual ~ObRefreshAndCheckTask() = default;
		int init(uint64_t tenant_id);
    void runTimerTask() override;
  public:
		ObTableLoadResourceManager &manager_;
		uint64_t tenant_id_;
	bool is_inited_;
  };
	int gen_update_arg(ObDirectLoadResourceUpdateArg &update_arg);
	int gen_check_res(bool first_check,
										ObDirectLoadResourceUpdateArg &update_arg,
										common::ObArray<ObDirectLoadResourceOpRes> &check_res);
	void check_assigned_task(common::ObArray<ObDirectLoadResourceOpRes> &check_res);
	int init_resource();
private:
  typedef common::hash::ObHashMap<ObAddr, ObResourceCtx, common::hash::NoPthreadDefendMode> ResourceCtxMap;
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObResourceAssigned, common::hash::NoPthreadDefendMode> ResourceAssignedMap;
  ObRefreshAndCheckTask refresh_and_check_task_;
	static const int64_t MAX_MISS_COUNT = 3;
	static const int64_t REFRESH_AND_CHECK_TASK_INTERVAL = 30LL * 1000LL * 1000LL; // 30s
	ResourceCtxMap resource_pool_;
	ResourceAssignedMap assigned_tasks_;
  mutable lib::ObMutex mutex_;
	volatile bool is_stop_;
	bool resource_inited_;
	bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
