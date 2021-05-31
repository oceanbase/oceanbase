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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_granule_pump.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase {
namespace sql {

using namespace oceanbase::share::schema;

#define TSCOp typename ObEngineOpTraits<NEW_ENG>::TSC
#define ModifyOp typename ObEngineOpTraits<NEW_ENG>::TableModify
#define GIOp typename ObEngineOpTraits<NEW_ENG>::GI

int ObGITaskSet::get_task_at_pos(ObGranuleTaskInfo& info, const Pos& pos) const
{
  int ret = OB_SUCCESS;
  if (pos.task_idx_ < 0 || pos.task_idx_ >= ranges_.count() || pos.task_idx_ >= offsets_.count() ||
      pos.partition_idx_ < 0 || pos.partition_idx_ >= partition_offsets_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    info.partition_id_ = partition_keys_.at(pos.partition_idx_).get_partition_id();
    info.ranges_.reset();
    int64_t end = offsets_.at(pos.task_idx_);
    int64_t begin = 0 == pos.task_idx_ ? 0 : offsets_.at(pos.task_idx_ - 1) + 1;
    for (int64_t i = begin; i <= end && OB_SUCC(ret); ++i) {
      if (OB_FAIL(info.ranges_.push_back(ranges_.at(i)))) {
        LOG_WARN("push back ranges failed", K(ret));
      }
    }
  }
  return ret;
}

int ObGITaskSet::get_next_gi_task_pos(Pos& pos)
{
  int ret = OB_SUCCESS;
  if (cur_.task_idx_ == ranges_.count() || cur_.task_idx_ == offsets_.count() ||
      cur_.partition_idx_ == partition_offsets_.count()) {
    ret = OB_ITER_END;
  } else if (cur_.partition_idx_ >= partition_keys_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_.partition_idx_ is out of range",
        K(ret),
        K(cur_.partition_idx_),
        K(partition_offsets_.count()),
        K(partition_keys_.count()));
  } else {
    pos = cur_;
    int64_t end = offsets_.at(cur_.task_idx_);
    if (end == partition_offsets_.at(cur_.partition_idx_)) {
      ++cur_.partition_idx_;
    }
    ++cur_.task_idx_;
  }
  return ret;
}

int ObGITaskSet::get_next_gi_task(ObGranuleTaskInfo& info)
{
  int ret = OB_SUCCESS;
  if (cur_.task_idx_ == ranges_.count() || cur_.task_idx_ == offsets_.count() ||
      cur_.partition_idx_ == partition_offsets_.count()) {
    ret = OB_ITER_END;
  } else if (cur_.partition_idx_ >= partition_keys_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current_partition_idx_ is out of range",
        K(ret),
        K(cur_.partition_idx_),
        K(partition_offsets_.count()),
        K(partition_keys_.count()));
  } else {
    info.partition_id_ = partition_keys_.at(cur_.partition_idx_).get_partition_id();
    info.ranges_.reset();
    info.task_id_ = common::OB_INVALID_INDEX_INT64;
    int64_t end = offsets_.at(cur_.task_idx_);
    int64_t begin = 0 == cur_.task_idx_ ? 0 : offsets_.at(cur_.task_idx_ - 1) + 1;
    for (int64_t i = begin; i <= end && OB_SUCC(ret); ++i) {
      if (OB_FAIL(info.ranges_.push_back(ranges_.at(i)))) {
        LOG_WARN("push back ranges failed", K(ret));
      }
    }
    if (end == partition_offsets_.at(cur_.partition_idx_)) {
      ++cur_.partition_idx_;
    }
    ++cur_.task_idx_;
  }
  return ret;
}

int ObGITaskSet::assign(const ObGITaskSet& other)
{
  int ret = OB_SUCCESS;
  IGNORE_RETURN partition_keys_.reset();
  IGNORE_RETURN ranges_.reset();
  IGNORE_RETURN offsets_.reset();
  IGNORE_RETURN partition_offsets_.reset();
  if (OB_FAIL(partition_keys_.assign(other.partition_keys_))) {
    LOG_WARN("failed to assign partition key", K(ret));
  } else if (OB_FAIL(ranges_.assign(other.ranges_))) {
    LOG_WARN("failed to assign ranges", K(ret));
  } else if (OB_FAIL(offsets_.assign(other.offsets_))) {
    LOG_WARN("failed to assign offsets");
  } else if (OB_FAIL(partition_offsets_.assign(other.partition_offsets_))) {
    LOG_WARN("failed to assign partition offsets", K(ret));
  } else {
    cur_.partition_idx_ = other.cur_.partition_idx_;
    cur_.task_idx_ = other.cur_.task_idx_;
  }
  return ret;
}

int ObGITaskSet::set_pw_affi_partition_order(bool asc)
{
  int ret = OB_SUCCESS;
  if (partition_keys_.count() <= 1) {
    // No sorting is required in two cases:
    // 1. The partition key is empty
    //   Because in the case of affinitize, the task is divided according to the granularity of the partition,
    //   If the value of parallel may be greater than the number of partitions in the table, the task set will appear to
    //   be "empty", If the task set is "empty", skip the `set_pw_affi_partition_order` process
    // 2. The count of the partition key is equal to 1
    //   The count of the partition key is equal to 1, which means there is only one partition, so there is no need to
    //   sort
    // do nothing
  } else {
    if (partition_keys_.count() != offsets_.count() || partition_keys_.count() != partition_offsets_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid pw affi taskset",
          K(ret),
          K(partition_keys_.count()),
          K(ranges_.count()),
          K(offsets_.count()),
          K(partition_offsets_.count()));
    } else if (!((asc && (partition_keys_.at(0).get_partition_id() > partition_keys_.at(1).get_partition_id()))) ||
               (!asc && (partition_keys_.at(0).get_partition_id() < partition_keys_.at(1).get_partition_id()))) {
      // no need to reverse this taskset
    } else if (OB_FAIL(reverse_task())) {
      LOG_WARN("failed to reverset gi task", K(ret));
    } else {
      LOG_TRACE("reverse this pw affinitize partition keys", K(ret), K(partition_keys_));
    }
  }
  return ret;
}

int ObGITaskSet::reverse_task()
{
  // The query range will not be extracted in advance between partition granule tasks,
  //   and each partition will only correspond to a whole range[min,max]
  // Different ranges, so when performing gi task reverse, you need to reverse the partition key, ranges, and offset
  // E.g:
  // partition_keys {p0,p1}
  // ranges {[1,1],[2,2],[4,4],[5,8],[10,15]}
  // partition_offset/offset {1,4}; indicates that the end idx of the range corresponding to the first partition is 1,
  // and the range corresponding to the second partition, the end idx is 4
  // The result of the overall reverse is:
  //   reverse_partition_keys {p1,p0}
  //   reverse_ranges {[4,4],[5,8],[10,15],[1,1],[2,2]}
  //   reverse_partition_offset/offset {2,4}
  int ret = OB_SUCCESS;
  common::ObSEArray<ObPartitionKey, 64> reverse_part_keys;
  common::ObSEArray<common::ObNewRange, 64> reverse_ranges;
  common::ObSEArray<int64_t, 64> reverse_offset;
  if (partition_keys_.count() != offsets_.count() || partition_keys_.count() != partition_offsets_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to reverse task",
        K(ret),
        K(partition_keys_.count()),
        K(offsets_.count()),
        K(partition_offsets_.count()));
  } else {
    // Calculate the partition key, ranges, offset corresponding to the reverse
    int part_cnt = partition_keys_.count();
    for (int64_t i = part_cnt - 1; i >= 0 && OB_SUCC(ret); i--) {
      if (OB_FAIL(reverse_part_keys.push_back(partition_keys_.at(i)))) {
        LOG_WARN("failed to push back partition key", K(ret));
      } else {
        // handle ranges
        int begin_idx = OB_INVALID_INDEX;
        if (i > 0) {
          begin_idx = partition_offsets_.at(i - 1);
          begin_idx++;
        } else {
          begin_idx = 0;
        }
        int end_idx = partition_offsets_.at(i);
        while (begin_idx <= end_idx && OB_SUCC(ret)) {
          if (OB_FAIL(reverse_ranges.push_back(ranges_.at(begin_idx)))) {
            LOG_WARN("failed push back to reverse ranges", K(ret));
          } else {
            begin_idx++;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(reverse_offset.push_back(ranges_.count() - 1))) {
            LOG_WARN("failed push back to reverse offset", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition_keys_.assign(reverse_part_keys))) {
      LOG_WARN("failed to assign partition key", K(ret));
    } else if (OB_FAIL(ranges_.assign(reverse_ranges))) {
      LOG_WARN("failed to assign ranges", K(ret));
    } else if (OB_FAIL(offsets_.assign(reverse_offset))) {
      LOG_WARN("failed to assign offsets", K(ret));
    } else if (OB_FAIL(partition_offsets_.assign(reverse_offset))) {
      LOG_WARN("failed to assign partition offset", K(ret));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////

int ObGranulePump::try_fetch_pwj_tasks(
    ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids, int64_t worker_id)
{
  int ret = OB_SUCCESS;
  /*try get gi task*/
  if (GIT_UNINITIALIZED == splitter_type_) {
    ret = OB_NOT_INIT;
    LOG_WARN("granule pump is not init", K(ret));
  } else if (worker_id < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("thread_id out of range", K(ret), K(worker_id));
  } else {
    switch (splitter_type_) {
      case GIT_FULL_PARTITION_WISE:
        if (OB_FAIL(fetch_pw_granule_from_shared_pool(infos, op_ids))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fetch granule from shared pool failed", K(ret));
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_task(
    const ObGITaskSet*& res_task_set, ObGITaskSet::Pos& pos, int64_t worker_id, uint64_t tsc_op_id)
{
  int ret = OB_SUCCESS;
  /*try get gi task*/
  LOG_DEBUG("fetch granule task from granule pump");
  if (GIT_UNINITIALIZED == splitter_type_) {
    ret = OB_NOT_INIT;
    LOG_WARN("granule pump is not init", K(ret));
  } else if (worker_id < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("thread_id out of range", K(ret), K(worker_id));
  } else {
    switch (splitter_type_) {
      case GIT_PARTIAL_PARTITION_WISE_WITH_AFFINITY:
      case GIT_ACCESS_ALL:
      case GIT_FULL_PARTITION_WISE:
      case GIT_FULL_PARTITION_WISE_WITH_AFFINITY:
        if (OB_FAIL(fetch_granule_by_worker_id(res_task_set, pos, worker_id, tsc_op_id))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fetch granule by worker id failed", K(ret));
          }
        }
        break;
      case GIT_RANDOM:
        if (OB_FAIL(fetch_granule_from_shared_pool(res_task_set, pos, tsc_op_id))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fetch granule from shared pool failed", K(ret));
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_by_worker_id(
    const ObGITaskSet*& res_task_set, ObGITaskSet::Pos& pos, int64_t worker_id, uint64_t tsc_op_id)
{
  int ret = OB_SUCCESS;
  ObGITaskArray* taskset_array = nullptr;
  if (OB_FAIL(find_taskset_by_tsc_id(tsc_op_id, taskset_array))) {
    LOG_WARN("the op_id do not have task set", K(ret), K(tsc_op_id));
  } else if (OB_ISNULL(taskset_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the taskset_array is null", K(ret));
  } else if (taskset_array->count() < worker_id + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the taskset_array size is invalid", K(taskset_array->count()), K(ret));
  } else {
    res_task_set = &taskset_array->at(worker_id);
    ObGITaskSet& taskset = taskset_array->at(worker_id);
    if (OB_FAIL(taskset.get_next_gi_task_pos(pos))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next gi task pos", K(ret));
      }
    } else {
      LOG_TRACE("get GI task",
          K(taskset.partition_keys_),
          K(taskset.ranges_),
          K(taskset.offsets_),
          K(taskset.partition_offsets_),
          K(taskset.cur_.task_idx_),
          K(taskset.cur_.partition_idx_),
          K(ret));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_from_shared_pool(
    const ObGITaskSet*& res_task_set, ObGITaskSet::Pos& pos, uint64_t tsc_op_id)
{
  int ret = OB_SUCCESS;
  /*try lock*/
  if (OB_FAIL(lock_.lock())) {
    LOG_ERROR("lock self fail", K(ret));
  }
  ObGITaskArray* taskset_array = nullptr;
  if (OB_FAIL(ret)) {
    // has been failed. do nothing.
  } else if (OB_FAIL(find_taskset_by_tsc_id(tsc_op_id, taskset_array))) {
    LOG_WARN("the tsc_op_id do not have task set", K(ret), K(tsc_op_id));
  } else if (OB_ISNULL(taskset_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the taskset_array is null", K(ret));
  } else if (OB_FAIL(taskset_array->count() < OB_GRANULE_SHARED_POOL_POS + 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("taskset array count is invalid", K(ret), K(taskset_array->count()));
  } else {
    res_task_set = &taskset_array->at(OB_GRANULE_SHARED_POOL_POS);
    ObGITaskSet& taskset = taskset_array->at(OB_GRANULE_SHARED_POOL_POS);
    if (OB_FAIL(taskset.get_next_gi_task_pos(pos))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next gi task pos", K(ret));
      }
    } else {
      LOG_TRACE("get GI task",
          K(taskset.partition_keys_),
          K(taskset.ranges_),
          K(taskset.offsets_),
          K(taskset.partition_offsets_),
          K(taskset.cur_.task_idx_),
          K(taskset.cur_.partition_idx_),
          K(ret));
    }
  }
  int lock_ret = lock_.unlock();
  if (lock_ret != OB_SUCCESS) {
    LOG_ERROR("unlock self fail", K(lock_ret));
  }
  return ret;
}

template <bool NEW_ENG>
int ObGranulePump::fetch_pw_granule_by_worker_id(
    ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<const TSCOp*>& tscs, int64_t thread_id)
{
  int ret = OB_SUCCESS;
  int64_t end_tsc_count = 0;
  if (GIT_FULL_PARTITION_WISE_WITH_AFFINITY != splitter_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only partition wise join granule pump offer this service", K(splitter_type_), K(ret));
  }
  ARRAY_FOREACH_X(tscs, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = tscs.at(idx);
    ObGITaskArray* taskset_array = nullptr;
    ObGranuleTaskInfo info;
    uint64_t op_id = tsc->get_id();

    if (OB_FAIL(find_taskset_by_tsc_id(op_id, taskset_array))) {
      LOG_WARN("the op_id do not have task set", K(ret), K(op_id));
    } else if (OB_ISNULL(taskset_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the taskset_array is null", K(ret));
    } else if (taskset_array->count() < thread_id + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the taskset_array size is invalid", K(taskset_array->count()), K(ret));
    } else if (OB_FAIL(taskset_array->at(thread_id).get_next_gi_task(info))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get info", K(ret));
      } else {
        ret = OB_SUCCESS;
        end_tsc_count++;
      }
    } else if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("push back task info failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_pw_end(end_tsc_count, tscs.count(), infos.count()))) {
    LOG_WARN("incorrect state", K(ret));
  }
  LOG_TRACE("get a new partition wise join gi tasks", K(infos), K(ret));
  return ret;
}

int ObGranulePump::fetch_pw_granule_from_shared_pool(
    ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids)
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  int ret = OB_SUCCESS;
  // Indicates that the number of ops of the next GI task cannot be retrieved;
  // In theory, end_op_count can only be equal to 0 (indicating that gi tasks have not been consumed yet)
  // or equal to `op_ids.count()` (indicating that all gi tasks have been consumed)
  int64_t end_op_count = 0;
  if (GIT_FULL_PARTITION_WISE != splitter_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only partition wise join granule pump offer this service", K(splitter_type_), K(ret));
  }
  ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret))
  {
    ObGITaskArray* taskset_array = nullptr;
    ObGranuleTaskInfo info;
    uint64_t op_id = op_ids.at(idx);
    if (OB_FAIL(find_taskset_by_tsc_id(op_id, taskset_array))) {
      LOG_WARN("the op_id do not have task set", K(ret), K(op_id));
    } else if (OB_ISNULL(taskset_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the taskset_array is null", K(ret));
    } else if (taskset_array->count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the taskset_array size is invalid", K(taskset_array->count()), K(ret));
    } else if (OB_FAIL(taskset_array->at(0).get_next_gi_task(info))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get info", K(ret));
      } else {
        ret = OB_SUCCESS;
        end_op_count++;
      }
    } else if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("push back task info failed", K(ret));
    }
  }

  // Defensive code: In the case of full partition wise, check whether the GI task
  // corresponding to each op has been consumed at the same time
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_pw_end(end_op_count, op_ids.count(), infos.count()))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("incorrect state", K(ret));
    }
  }
  LOG_TRACE("get a new partition wise join gi tasks", K(infos), K(ret));
  return ret;
}

int ObGranulePump::check_pw_end(int64_t end_op_count, int64_t op_count, int64_t task_count)
{
  int ret = OB_SUCCESS;
  if (end_op_count != 0 && end_op_count != op_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the end op count does not match partition wise join ops count", K(end_op_count), K(op_count), K(ret));
  } else if (end_op_count != 0) {
    ret = OB_ITER_END;
  } else if (end_op_count == 0 && task_count != op_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the infos count does not match partition wise join ops count",
        K(end_op_count),
        K(task_count),
        K(op_count),
        K(ret));
  } else if (end_op_count == 0) {
    /* we get tasks for every tsc */
  }
  return ret;
}

int ObGranulePump::add_new_gi_task(
    ObGranulePumpArgs& args, ObIArray<const ObTableScan*>& scan_ops, const ObTableModify* modify_op)
{
  return add_new_gi_task_inner<false>(args, scan_ops, modify_op);
}

int ObGranulePump::add_new_gi_task(
    ObGranulePumpArgs& args, ObIArray<const ObTableScanSpec*>& scan_ops, const ObTableModifySpec* modify_op)
{
  return add_new_gi_task_inner<true>(args, scan_ops, modify_op);
}

/**
 * This function is special, it may be called multiple times in an SQC, similar to this plan
 *
 *             [Join]
 *               |
 *          ------------
 *          |          |
 *       [Join]        GI
 *          |          |
 *     -----------     TSC3
 *     |         |
 *    EX(pkey)   GI
 *     |         |
 *   ....        TSC2
 * In sqc's setup_op_input process, this interface will be called once when a GI is found.
 *
 */
template <bool NEW_ENG>
int ObGranulePump::add_new_gi_task_inner(
    ObGranulePumpArgs& args, ObIArray<const TSCOp*>& scan_ops, const ModifyOp* modify_op)
{
  int ret = OB_SUCCESS;
  partition_wise_join_ = args.pwj_gi();
  LOG_DEBUG("init granule", K(args));
  int map_size = 0;
  if (OB_NOT_NULL(modify_op)) {
    map_size++;
  }
  map_size += scan_ops.count();
  if (gi_task_array_map_.empty()) {
    if (OB_FAIL(gi_task_array_map_.prepare_allocate(map_size))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (args.access_all()) {
    splitter_type_ = GIT_ACCESS_ALL;
    ObAccessAllGranuleSplitter splitter;
    if (OB_FAIL(splitter.split_granule<NEW_ENG>(args.ctx_,
            scan_ops,
            args.pkey_arrays_,
            args.parallelism_,
            args.tablet_size_,
            args.partition_service_,
            gi_task_array_map_))) {
      LOG_WARN("failed to prepare access all gi task", K(ret));
    }
  } else if (args.pwj_gi() && args.affinitize()) {
    splitter_type_ = GIT_FULL_PARTITION_WISE_WITH_AFFINITY;
    ObPWAffinitizeGranuleSplitter splitter;
    if (OB_FAIL(splitter.partitions_info_.assign(args.partitions_info_))) {
      LOG_WARN("Failed to assign partitions info", K(ret));
    } else if (OB_FAIL(splitter.split_granule<NEW_ENG>(args.ctx_,
                   scan_ops,
                   args.pkey_arrays_,
                   args.parallelism_,
                   args.tablet_size_,
                   args.partition_service_,
                   gi_task_array_map_))) {
      LOG_WARN("failed to prepare affinity gi task", K(ret));
    }
  } else if (args.affinitize()) {
    splitter_type_ = GIT_PARTIAL_PARTITION_WISE_WITH_AFFINITY;
    ObNormalAffinitizeGranuleSplitter splitter;
    if (OB_FAIL(splitter.partitions_info_.assign(args.partitions_info_))) {
      LOG_WARN("Failed to assign partitions info", K(ret));
    } else if (OB_FAIL(splitter.split_granule<NEW_ENG>(args.ctx_,
                   scan_ops,
                   args.pkey_arrays_,
                   args.parallelism_,
                   args.tablet_size_,
                   args.partition_service_,
                   gi_task_array_map_))) {
      LOG_WARN("failed to prepare affinity gi task", K(ret));
    }
  } else if (args.pwj_gi()) {
    splitter_type_ = GIT_FULL_PARTITION_WISE;
    ObPartitionWiseGranuleSplitter splitter;
    if (OB_FAIL(splitter.split_granule<NEW_ENG>(args.ctx_,
            scan_ops,
            modify_op,
            args.pkey_arrays_,
            args.parallelism_,
            args.tablet_size_,
            args.partition_service_,
            gi_task_array_map_))) {
      LOG_WARN("failed to prepare pw gi task", K(ret));
    }
  } else {
    splitter_type_ = GIT_RANDOM;
    ObRandomGranuleSplitter splitter;
    bool partition_granule = args.force_partition_granule();
    if (OB_FAIL(splitter.split_granule<NEW_ENG>(args.ctx_,
            scan_ops,
            args.pkey_arrays_,
            args.parallelism_,
            args.tablet_size_,
            args.partition_service_,
            gi_task_array_map_,
            partition_granule))) {
      LOG_WARN("failed to prepare random gi task", K(ret), K(partition_granule));
    }
  }
  return ret;
}

void ObGranulePump::destroy()
{
  gi_task_array_map_.reset();
}

int64_t ObGranulePump::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(parallelism), K_(tablet_size), K_(partition_wise_join));
  J_OBJ_END();
  return pos;
}

///////////////////////////////////////////////////////////////////////////////////////

int ObGranuleSplitter::split_gi_task(ObExecContext& ctx, const ObQueryRange& tsc_pre_query_range, int64_t table_id,
    const common::ObIArray<common::ObPartitionKey>& pkeys, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, bool partition_granule, ObGITaskSet& task_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObNewRange, 16> ranges;
  if (0 > parallelism) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parallelism is invalid", K(ret), K(parallelism));
  } else if (0 > tablet_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parallelism is invalid", K(ret), K(tablet_size));
  } else if (pkeys.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the task has an empty pkeys", K(ret), K(pkeys));
  } else if (OB_FAIL(get_query_range(ctx, tsc_pre_query_range, ranges, table_id, partition_granule))) {
    LOG_WARN("get query range failed", K(ret));
  } else if (ranges.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the task has an empty range", K(ret), K(ranges));
  } else if (OB_FAIL(ObGranuleUtil::split_block_ranges(ctx.get_allocator(),
                 ranges,
                 pkeys,
                 partition_service,
                 parallelism,
                 tablet_size,
                 partition_granule,
                 task_set.ranges_,
                 task_set.offsets_,
                 task_set.partition_offsets_))) {
    LOG_WARN("failed to get graunle task", K(ret), K(ranges), K(pkeys));
  } else {
    if (task_set.partition_keys_.empty()) {
      if (OB_FAIL(task_set.partition_keys_.reserve(pkeys.count()))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
    }
    FOREACH_CNT_X(it, pkeys, OB_SUCC(ret))
    {
      if (OB_FAIL(task_set.partition_keys_.push_back(*it))) {
        LOG_WARN("add partition key failed", K(ret));
      }
    }
  }
  return ret;
}

int ObGranuleSplitter::get_query_range(ObExecContext& ctx, const ObQueryRange& tsc_pre_query_range,
    ObIArray<ObNewRange>& ranges, int64_t table_id, bool partition_granule)
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray scan_ranges;
  ObGetMethodArray get_method;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical plan ctx", K(ret));
  } else if (partition_granule) {
    // For partition granule, we will prepare query range in table scan.
    LOG_DEBUG("set partition granule to whole range", K(table_id), K(tsc_pre_query_range.get_column_count()));
    ObNewRange whole_range;
    if (0 == tsc_pre_query_range.get_column_count()) {
      whole_range.set_whole_range();
    } else if (OB_FAIL(ObSQLUtils::make_whole_range(
                   ctx.get_allocator(), table_id, tsc_pre_query_range.get_column_count(), whole_range))) {
      LOG_WARN("Failed to make whole range", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ranges.push_back(whole_range))) {
        LOG_WARN("Failed to push back scan range", K(ret));
      }
    }
  } else if (OB_FAIL(ObSQLUtils::extract_pre_query_range(tsc_pre_query_range,
                 ctx.get_allocator(),
                 plan_ctx->get_param_store(),
                 scan_ranges,
                 get_method,
                 ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
    LOG_WARN("failed to get scan ranges", K(ret));
  } else {
    for (int64_t i = 0; i < scan_ranges.count() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(scan_ranges.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the scan range is null", K(ret));
      } else if (OB_FAIL(ranges.push_back(*scan_ranges.at(i)))) {
        LOG_WARN("push back ranges failed", K(ret));
      } else {
        ranges.at(ranges.count() - 1).table_id_ = table_id;
        // do nothing
      }
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObRandomGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()), K(gi_task_array_result.count()));
  }
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = scan_ops.at(idx);
    if (OB_ISNULL(tsc) || scan_ops.count() != pkey_arrays.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret), K(scan_ops.count()), K(pkey_arrays.count()));
    } else {
      uint64_t scan_key_id = tsc->get_scan_key_id();
      uint64_t op_id = tsc->get_id();
      ObGITaskSet total_task_set;
      ObGITaskArray& taskset_array = gi_task_array_result.at(idx).taskset_array_;
      partition_granule = is_virtual_table(scan_key_id) || partition_granule;
      if (OB_FAIL(split_gi_task(ctx,
              tsc->get_query_range(),
              scan_key_id,
              pkey_arrays.at(idx),
              parallelism,
              tablet_size,
              partition_service,
              partition_granule,
              total_task_set))) {
        LOG_WARN("failed to init granule iter pump", K(ret), K(idx), K(pkey_arrays));
      } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
        LOG_WARN("failed to push back task set", K(ret));
      } else {
        gi_task_array_result.at(idx).tsc_op_id_ = op_id;
      }
      LOG_TRACE(
          "random granule split a task_array", K(op_id), K(scan_key_id), K(taskset_array), K(ret), K(scan_ops.count()));
    }
  }
  return ret;
}

int ObAccessAllGranuleSplitter::split_tasks_access_all(
    ObGITaskSet& taskset, int64_t parallelism, ObGITaskArray& taskset_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < parallelism && OB_SUCC(ret); ++i) {
    if (OB_FAIL(taskset_array.at(i).assign(taskset))) {
      LOG_WARN("failed to assign taskset", K(ret));
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObAccessAllGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()), K(gi_task_array_result.count()));
  }
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    ObGITaskArray& taskset_array = gi_task_array_result.at(idx).taskset_array_;
    if (OB_ISNULL(tsc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(taskset_array.prepare_allocate(parallelism))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(ctx,
                   tsc->get_query_range(),
                   scan_key_id,
                   pkey_arrays.at(idx),
                   parallelism,
                   tablet_size,
                   partition_service,
                   partition_granule,
                   total_task_set))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_access_all(total_task_set, parallelism, taskset_array))) {
      LOG_WARN("failed to split ");
    } else {
      gi_task_array_result.at(idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("access all granule split a task_array",
        K(op_id),
        K(tsc->get_location_table_id()),
        K(taskset_array),
        K(ret),
        K(scan_ops.count()));
  }
  return ret;
}

int ObAffinitizeGranuleSplitter::split_tasks_affinity(
    ObExecContext& ctx, ObGITaskSet& taskset, int64_t parallelism, ObGITaskArray& taskset_array)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  ObPxAffinityByRandom affinitize_rule;
  ObSQLSessionInfo* my_session = NULL;
  ObPxPartitionInfo partition_row_info;
  bool check_dropped_partition = false;

  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  }
  ARRAY_FOREACH_X(taskset.partition_keys_, idx, cnt, OB_SUCC(ret))
  {
    const ObPartitionKey& key = taskset.partition_keys_.at(idx);
    int64_t real_partition_idx = -1;
    if (NULL == table_schema || table_schema->get_table_id() != key.get_table_id()) {
      uint64_t table_id = key.get_table_id();
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(my_session->get_effective_tenant_id(), schema_guard))) {
        LOG_WARN("Failed to get schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("Failed to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Table schema is null", K(ret), K(table_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObPartMgrUtils::get_partition_idx_by_id(
                   *table_schema, check_dropped_partition, key.get_partition_id(), real_partition_idx))) {
      LOG_WARN("Failed to get partition_idx", K(ret));
    } else if (OB_FAIL(ObPxAffinityByRandom::get_partition_info(
                   key.get_partition_id(), partitions_info_, partition_row_info))) {
      LOG_WARN("Failed to get partition info", K(ret));
    } else if (OB_FAIL(affinitize_rule.add_partition(key.get_partition_id(),
                   real_partition_idx,
                   parallelism,
                   my_session->get_effective_tenant_id(),
                   partition_row_info))) {
      LOG_WARN("Failed to get affinitize taskid", K(ret));
    }
  }
  affinitize_rule.do_random(!partitions_info_.empty());
  const ObIArray<ObPxAffinityByRandom::PartitionHashValue>& partition_worker_pairs = affinitize_rule.get_result();
  ARRAY_FOREACH(partition_worker_pairs, rt_idx)
  {
    int64_t real_partition_idx = partition_worker_pairs.at(rt_idx).partition_idx_;
    int64_t task_id = partition_worker_pairs.at(rt_idx).worker_id_;
    int64_t partition_id = partition_worker_pairs.at(rt_idx).partition_id_;
    ARRAY_FOREACH(taskset.partition_keys_, idx)
    {
      const ObPartitionKey& key = taskset.partition_keys_.at(idx);
      if (partition_id == key.get_partition_id()) {
        if (task_id >= parallelism) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Task id is invalid", K(ret), K(task_id), K(parallelism));
        } else {
          ObGITaskSet& real_task_set = taskset_array.at(task_id);
          int64_t end = taskset.offsets_.at(idx);
          int64_t begin = 0 == idx ? 0 : taskset.offsets_.at(idx - 1) + 1;
          if (begin > end) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("The begin and end idx of offset is invalid", K(ret));
          }
          for (int64_t i = begin; i <= end && OB_SUCC(ret); ++i) {
            if (OB_FAIL(real_task_set.ranges_.push_back(taskset.ranges_.at(i)))) {
              LOG_WARN("Failed to push back ranges", K(ret));
            }
          }
          // When GI is AFF, TSC must be divided according to the granularity of partition:
          // 1. Each TSC will be divided into multiple partitions
          // 2. Each partition will be composed of multiple ranges
          // 3. partition_offsets_ records the end offset of the ranges corresponding to each partition
          // 4. Offsets_ records the end offset of the ranges corresponding to each task. Since the task division of AFF
          // is partition granular, so The end offset stored in offsets_ is consistent with the content of
          // partition_offsets_
          if (OB_SUCC(ret)) {
            int64_t offset_end = real_task_set.ranges_.count() - 1;
            if (OB_FAIL(real_task_set.offsets_.push_back(offset_end))) {
              LOG_WARN("Push back failed", K(ret));
            } else if (OB_FAIL(real_task_set.partition_offsets_.push_back(offset_end))) {
              LOG_WARN("Push back failed", K(ret));
            } else if (OB_FAIL(real_task_set.partition_keys_.push_back(taskset.partition_keys_.at(idx)))) {
              LOG_WARN("Push back failed", K(ret));
            }
          }
        }
        LOG_TRACE("affinitize granule split a task_array",
            K(real_partition_idx),
            K(task_id),
            K(parallelism),
            K(taskset_array),
            K(ret));
      }
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObNormalAffinitizeGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()), K(gi_task_array_result.count()));
  }
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    ObGITaskArray& taskset_array = gi_task_array_result.at(idx).taskset_array_;
    if (OB_ISNULL(tsc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(gi_task_array_result.at(idx).taskset_array_.prepare_allocate(parallelism))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(ctx,
                   tsc->get_query_range(),
                   scan_key_id,
                   pkey_arrays.at(idx),
                   parallelism,
                   tablet_size,
                   partition_service,
                   partition_granule,
                   total_task_set))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_affinity(ctx, total_task_set, parallelism, taskset_array))) {
      LOG_WARN("failed to split task affinity", K(ret));
    } else {
      gi_task_array_result.at(idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("normal affinitize granule split a task_array",
        K(op_id),
        K(tsc->get_location_table_id()),
        K(taskset_array),
        K(ret),
        K(scan_ops.count()));
  }
  return ret;
}

template <bool NEW_ENG>
int ObPartitionWiseGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  UNUSED(ctx);
  UNUSED(scan_ops);
  UNUSED(pkey_arrays);
  UNUSED(partition_service);
  UNUSED(parallelism);
  UNUSED(tablet_size);
  UNUSED(partition_granule);
  UNUSED(gi_task_array_result);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

template <bool NEW_ENG>
int ObPartitionWiseGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const ModifyOp* modify_op, const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism,
    int64_t tablet_size, storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  int expected_map_size = 0;
  // If GI needs to split INSERT/REPLACE tasks, then pkey_arrays not only contains
  // the partition keys information corresponding to the table_scans table, but also
  // The partition keys information corresponding to the insert/replace table; for example,
  // such a plan:
  // ....
  //     GI
  //      INSERT/REPLACE
  //         JOIN
  //          TSC1
  //          TSC2
  // The first element of `pkey_arrays` corresponds to the partition keys of the INSERT/REPLACE table,
  // and the other elements correspond to the partition keys of the TSC table
  int tsc_begin_idx = 0;
  if (OB_NOT_NULL(modify_op)) {
    expected_map_size++;
    tsc_begin_idx = 1;
  }
  expected_map_size += scan_ops.count();
  if (expected_map_size != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result",
        K(ret),
        K(expected_map_size),
        K(gi_task_array_result.count()),
        K(modify_op != NULL),
        K(scan_ops.count()));
  } else if (pkey_arrays.count() != expected_map_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(ret), K(pkey_arrays.count()), K(expected_map_size));
  } else if (0 >= pkey_arrays.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(ret), K(pkey_arrays.count()));
  }
  if (OB_SUCC(ret)) {
    int pkey_count = pkey_arrays.at(0).count();
    ObPartitionArray part_keys = pkey_arrays.at(0);
    ARRAY_FOREACH(pkey_arrays, idx)
    {
      // Verify that the number of partition keys corresponding to each op is the same
      ObPartitionArray pkey_array = pkey_arrays.at(idx);
      if (pkey_count != pkey_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the pkey count is not equal", K(ret), K(pkey_count), K(pkey_array.count()));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(modify_op)) {
    int index_idx = 0;
    ObGITaskSet total_task_set;
    ObGITaskArray& taskset_array = gi_task_array_result.at(index_idx).taskset_array_;
    int rowkey_cnt = modify_op->primary_key_count();
    LOG_TRACE("handler split dml op task", K(rowkey_cnt), K(modify_op->get_type()));
    if (rowkey_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the primary key count is error for insert op", K(ret));
    } else if (split_insert_gi_task(ctx,
                   modify_op->get_index_tid(),
                   rowkey_cnt,
                   pkey_arrays.at(0),
                   parallelism,
                   tablet_size,
                   partition_service,
                   partition_granule,
                   total_task_set)) {
      LOG_WARN("failed to prepare pw insert gi task", K(ret));
    } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
      LOG_WARN("failed to push back task set", K(ret));
    } else {
      // Obtain the corresponding insert/replace op id
      LOG_TRACE("splite modify gi task successfully", K(modify_op->get_id()));
      gi_task_array_result.at(index_idx).tsc_op_id_ = modify_op->get_id();
    }
  }
  // Process the task division of tsc
  if (OB_SUCC(ret)) {
    ObSEArray<common::ObPartitionArray, 4> tsc_pkey_arrays;
    for (int i = tsc_begin_idx; i < pkey_arrays.count(); i++) {
      if (OB_FAIL(tsc_pkey_arrays.push_back(pkey_arrays.at(i)))) {
        LOG_WARN("failed to push back tsc pkey arrays", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // pass
    } else if (OB_FAIL(split_tsc_gi_task<NEW_ENG>(ctx,
                   scan_ops,
                   tsc_pkey_arrays,
                   parallelism,
                   tablet_size,
                   partition_service,
                   tsc_begin_idx,
                   gi_task_array_result))) {
      LOG_WARN("failed to prepare pw tsc gi task", K(ret));
    }
  }

  return ret;
}

int ObPartitionWiseGranuleSplitter::split_insert_gi_task(ObExecContext& ctx, const uint64_t insert_table_id,
    const int64_t row_key_count, const common::ObIArray<common::ObPartitionKey>& pkeys, int64_t parallelism,
    int64_t tablet_size, storage::ObPartitionService& partition_service, bool partition_granule, ObGITaskSet& task_set)
{
  // At present, the GI corresponding to INSERT must be of full partition wise type,
  // and the granularity of task division must be divided according to partition
  int ret = OB_SUCCESS;
  ObNewRange each_partition_range;
  ObSEArray<ObNewRange, 4> ranges;
  if (0 >= parallelism || pkeys.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected args", K(ret), K(parallelism), K(pkeys.count()));
  } else if (OB_FAIL(ObSQLUtils::make_whole_range(
                 ctx.get_allocator(), insert_table_id, row_key_count, each_partition_range))) {
    LOG_WARN("failed to make whole range", K(ret));
  } else if (OB_FAIL(ranges.push_back(each_partition_range))) {
    LOG_WARN("failed to push partition range to ranges", K(ret));
  } else if (OB_FAIL(ObGranuleUtil::split_block_ranges(ctx.get_allocator(),
                 ranges,
                 pkeys,
                 partition_service,
                 parallelism,
                 tablet_size,
                 partition_granule,  // true
                 task_set.ranges_,
                 task_set.offsets_,
                 task_set.partition_offsets_))) {
    LOG_WARN("failed to get insert graunle task", K(ret), K(each_partition_range), K(pkeys));
  } else {
    // The task division of INSERT must be partition wise, and each rescan of
    // the INSERT operator only needs the partition key corresponding to each task.
    // Task parameters such as `ranges` and `offsets` are not needed
    if (task_set.partition_keys_.empty()) {
      if (OB_FAIL(task_set.partition_keys_.reserve(pkeys.count()))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
    }
    FOREACH_CNT_X(it, pkeys, OB_SUCC(ret))
    {
      if (OB_FAIL(task_set.partition_keys_.push_back(*it))) {
        LOG_WARN("add partition key failed", K(ret));
      }
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObPartitionWiseGranuleSplitter::split_tsc_gi_task(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, int64_t tsc_begin_idx, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    int64_t task_array_idx = idx + tsc_begin_idx;
    ObGITaskArray& taskset_array = gi_task_array_result.at(task_array_idx).taskset_array_;
    if (OB_ISNULL(tsc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(split_gi_task(ctx,
                   tsc->get_query_range(),
                   scan_key_id,
                   pkey_arrays.at(idx),
                   parallelism,
                   tablet_size,
                   partition_service,
                   partition_granule,
                   total_task_set))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
      LOG_WARN("failed to push back task set", K(ret));
    } else {
      gi_task_array_result.at(task_array_idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("partition wise tsc granule split a task_array",
        K(op_id),
        K(tsc->get_location_table_id()),
        K(taskset_array),
        K(ret),
        K(scan_ops.count()));
  }
  return ret;
}

template <bool NEW_ENG>
int ObPWAffinitizeGranuleSplitter::split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
    const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
    storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
    bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = gi_task_array_result.count();
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    GITaskArrayItem empty_task_array_item;
    if (OB_FAIL(gi_task_array_result.push_back(empty_task_array_item))) {
      LOG_WARN("push back new task array failed", K(ret));
    }
  }
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
  {
    const TSCOp* tsc = scan_ops.at(idx);
    const GIOp* gi = static_cast<const GIOp*>(tsc->get_parent());
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    bool asc_gi_task_order = true;
    ObGITaskArray& taskset_array = gi_task_array_result.at(idx + task_idx).taskset_array_;
    if (OB_ISNULL(tsc) || OB_ISNULL(gi)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc/gi ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(asc_gi_task_order = !gi->desc_partition_order())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(taskset_array.prepare_allocate(parallelism))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(ctx,
                   tsc->get_query_range(),
                   scan_key_id,
                   pkey_arrays.at(idx),
                   parallelism,
                   tablet_size,
                   partition_service,
                   partition_granule,
                   total_task_set))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_affinity(ctx, total_task_set, parallelism, taskset_array))) {
      LOG_WARN("failed to split task affinity", K(ret));
    } else if (OB_FAIL(adjust_task_order(asc_gi_task_order, taskset_array))) {
      LOG_WARN("failed to adjust task order", K(ret));
    } else {
      gi_task_array_result.at(idx + task_idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("partition wise with affinitize granule split a task_array",
        K(op_id),
        K(taskset_array),
        K(ret),
        K(scan_ops.count()));
  }
  return ret;
}

int ObPWAffinitizeGranuleSplitter::adjust_task_order(bool asc, ObGITaskArray& taskset_array)
{
  // In same pw affi task group, worker has there own task order,
  // we must adjust task order to get right join result, just see issue/22963231.
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < taskset_array.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(taskset_array.at(i).set_pw_affi_partition_order(asc))) {
      LOG_WARN("failed to set partition order", K(ret));
    }
  }
  return ret;
}

int ObGranulePump::find_taskset_by_tsc_id(uint64_t op_id, ObGITaskArray*& taskset_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < gi_task_array_map_.count() && OB_SUCC(ret); ++i) {
    if (op_id == gi_task_array_map_.at(i).tsc_op_id_) {
      taskset_array = &gi_task_array_map_.at(i).taskset_array_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(taskset_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task don't exist", K(ret), K(op_id));
  }
  return ret;
}

}  // namespace sql

}  // namespace oceanbase
