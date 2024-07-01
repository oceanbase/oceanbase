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
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/ob_engine_op_traits.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::share::schema;

bool ObGranulePumpArgs::need_partition_granule()
{
  return 0 == cur_tablet_idx_ && ObGranuleUtil::force_partition_granule(gi_attri_flag_);
}


//------------------------------end ObGranulePumpArgs

int ObGITaskSet::get_task_at_pos(ObGranuleTaskInfo &info, const int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= gi_task_set_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pos));
  } else {
    int64_t cur_idx = gi_task_set_.at(pos).idx_;
    info.tablet_loc_ = const_cast<ObDASTabletLoc *>(gi_task_set_.at(pos).tablet_loc_);
    info.ranges_.reset();
    info.ss_ranges_.reset();
    for (int64_t i = pos; OB_SUCC(ret) && i < gi_task_set_.count(); i++) {
      if (cur_idx == gi_task_set_.at(i).idx_) {
        if (OB_FAIL(info.ranges_.push_back(gi_task_set_.at(i).range_))) {
          LOG_WARN("push back ranges failed", K(ret));
        } else if (OB_FAIL(info.ss_ranges_.push_back(gi_task_set_.at(i).ss_range_))) {
          LOG_WARN("push back skip scan ranges failed", K(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObGITaskSet::get_task_tablet_id_at_pos(const int64_t &pos, uint64_t &tablet_id) const
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= gi_task_set_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pos));
  } else {
    int64_t cur_idx = gi_task_set_.at(pos).idx_;
    tablet_id = gi_task_set_.at(pos).tablet_loc_->tablet_id_.id();
  }
  return ret;
}

int ObGITaskSet::get_next_gi_task_pos(int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ == gi_task_set_.count()) {
    ret = OB_ITER_END;
  } else if (cur_pos_ > gi_task_set_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_pos_ is out of range", K(ret), K(cur_pos_), K(gi_task_set_.count()));
  } else {
    pos = cur_pos_;
    int64_t cur_idx = gi_task_set_.at(cur_pos_).idx_;
    for (int64_t i = cur_pos_; OB_SUCC(ret) && i < gi_task_set_.count(); i++) {
      if (cur_idx == gi_task_set_.at(i).idx_) {
        if (i == (gi_task_set_.count() - 1)) {
          cur_pos_ = gi_task_set_.count();
        }
      } else {
        cur_pos_ = i;
        break;
      }
    }
  }
  return ret;
}

int ObGITaskSet::get_next_gi_task(ObGranuleTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ == gi_task_set_.count()) {
    ret = OB_ITER_END;
  } else if (cur_pos_ > gi_task_set_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_pos_ is out of range", K(ret), K(cur_pos_), K(gi_task_set_.count()));
  } else {
    int64_t cur_idx = gi_task_set_.at(cur_pos_).idx_;
    info.tablet_loc_ = gi_task_set_.at(cur_pos_).tablet_loc_;
    info.ranges_.reset();
    info.ss_ranges_.reset();
    for (int64_t i = cur_pos_; OB_SUCC(ret) && i < gi_task_set_.count(); i++) {
      if (cur_idx == gi_task_set_.at(i).idx_) {
        if (OB_FAIL(info.ranges_.push_back(gi_task_set_.at(i).range_))) {
          LOG_WARN("push back ranges failed", K(ret));
        } else if (OB_FAIL(info.ss_ranges_.push_back(gi_task_set_.at(i).ss_range_))) {
          LOG_WARN("push back skip scan ranges failed", K(ret));
        }
        if (i == (gi_task_set_.count() - 1)) {
          cur_pos_ = gi_task_set_.count();
        }
      } else {
        cur_pos_ = i;
        break;
      }
    }
  }
  return ret;
}

int ObGITaskSet::assign(const ObGITaskSet &other)
{
  int ret = OB_SUCCESS;
  IGNORE_RETURN gi_task_set_.reset();
  if (OB_FAIL(gi_task_set_.assign(other.gi_task_set_))) {
    LOG_WARN("failed to assign gi_task_set", K(ret));
  } else {
    cur_pos_ = other.cur_pos_;
  }
  return ret;
}

int ObGITaskSet::set_pw_affi_partition_order(bool asc)
{
  int ret = OB_SUCCESS;
  if (gi_task_set_.count() <= 1) {
    // 两种情况下不需要进行排序：
    // 1. partition keys 是empty
    // 增加empty的判断条件，aone：
    // 由于在affinitize情况下，任务是按照partition的粒度划分，
    // 如果parallel的值可能大于表的partition个数，就会出现task set为”空“，
    // 如果task set是”空“就跳过`set_pw_affi_partition_order`过程
    // 2. partition keys的count等于1
    // partition keys的count等于1，也就表示仅有一个partition，所以不需要进行排序

    // do nothing
  } else {
    // first we do a defensive check. if data already sorted as expected, we just skip reverse
    // FIXME YISHEN , need check
    if (!(asc && (gi_task_set_.at(0).tablet_loc_->tablet_id_ > gi_task_set_.at(1).tablet_loc_->tablet_id_))
      || (!asc && (gi_task_set_.at(0).tablet_loc_->tablet_id_ < gi_task_set_.at(1).tablet_loc_->tablet_id_))) {
      // no need to reverse this taskset
    } else {
      common::ObArray<ObGITaskInfo> reverse_task_info;
      if (OB_FAIL(reverse_task_info.reserve(gi_task_set_.count()))) {
        LOG_WARN("fail reserve memory for array", K(ret));
      }
      for (int64_t i = gi_task_set_.count() - 1; OB_SUCC(ret) && i >=0; --i) {
        if (OB_FAIL(reverse_task_info.push_back(gi_task_set_.at(i)))) {
          LOG_WARN("failed to push back task info", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_task_set_.assign(reverse_task_info))) {
          LOG_WARN("failed to assign task info", K(ret));
        }
      }
      LOG_TRACE("reverse this pw affinitize task info", K(ret), K(gi_task_set_));
    }
  }
  return ret;
}

// reverse block order for every partition
int ObGITaskSet::set_block_order(bool desc)
{
  int ret = OB_SUCCESS;
  if (desc && gi_task_set_.count() > 1) {
    common::ObArray<ObGITaskInfo> reverse_task_info;
    if (OB_FAIL(reverse_task_info.reserve(gi_task_set_.count()))) {
      LOG_WARN("fail reserve memory for array", K(ret));
    }
    int64_t lower_inclusive = 0;
    while (lower_inclusive < gi_task_set_.count() && OB_SUCC(ret)) {
      // step1: in order to reverse block inside a partition, look for partition boundary
      int64_t upper_exclusive = lower_inclusive + 1;
      for (; upper_exclusive < gi_task_set_.count() && OB_SUCC(ret); ++upper_exclusive) {
        if (gi_task_set_.at(upper_exclusive).tablet_loc_->tablet_id_.id() !=
            gi_task_set_.at(lower_inclusive).tablet_loc_->tablet_id_.id()) {
          break;
        }
      }
      // step2: reverse gi_task_set_[lower_inclusive, upper_exclusive)
      int64_t pos = upper_exclusive;
      for (;lower_inclusive < upper_exclusive && OB_SUCC(ret); ++lower_inclusive) {
        pos--;
        if (OB_FAIL(reverse_task_info.push_back(gi_task_set_.at(pos)))) {
          LOG_WARN("failed to push back task info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(gi_task_set_.assign(reverse_task_info))) {
        LOG_WARN("failed to assign task info", K(ret));
      }
    }
    LOG_TRACE("reverse block task info", K(ret), K(gi_task_set_));
  }
  return ret;
}

int ObGITaskSet::construct_taskset(ObIArray<ObDASTabletLoc*> &taskset_tablets,
                                   ObIArray<ObNewRange> &taskset_ranges,
                                   ObIArray<ObNewRange> &ss_ranges,
                                   ObIArray<int64_t> &taskset_idxs,
                                   ObGIRandomType random_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(taskset_tablets.count() != taskset_ranges.count() ||
                  taskset_tablets.count() != taskset_idxs.count() ||
                  taskset_tablets.empty() || ss_ranges.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("taskset count err", K(taskset_tablets.count()),
                                  K(taskset_ranges),
                                  K(taskset_idxs),
                                  K(ss_ranges.count()));
  } else if (!(GI_RANDOM_NONE <= random_type && random_type <= GI_RANDOM_RANGE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("random type err", K(random_type));
  } else if (gi_task_set_.empty() && OB_FAIL(gi_task_set_.reserve(taskset_tablets.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    ObNewRange whole_range;
    whole_range.set_whole_range();
    ObNewRange &ss_range = ss_ranges.empty() ? whole_range : ss_ranges.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < taskset_tablets.count(); i++) {
      ObGITaskInfo task_info(taskset_tablets.at(i), taskset_ranges.at(i), ss_range, taskset_idxs.at(i));
      if (random_type != ObGITaskSet::GI_RANDOM_NONE) {
        task_info.hash_value_ = common::murmurhash(&task_info.idx_, sizeof(task_info.idx_), 0);
      }
      if (OB_FAIL(gi_task_set_.push_back(task_info))) {
        LOG_WARN("add partition key failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && random_type != GI_RANDOM_NONE) {
      auto compare_fun = [](const ObGITaskInfo &a, const ObGITaskInfo &b) -> bool { return a.hash_value_ > b.hash_value_; };
      lib::ob_sort(gi_task_set_.begin(), gi_task_set_.end(), compare_fun);
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////

int ObGranulePump::try_fetch_pwj_tasks(ObIArray<ObGranuleTaskInfo> &infos,
                                       const ObIArray<int64_t> &op_ids,
                                       int64_t worker_id)
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
    switch(splitter_type_) {
    case GIT_FULL_PARTITION_WISE :
      if (OB_FAIL(fetch_pw_granule_from_shared_pool(infos, op_ids))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fetch granule from shared pool failed", K(ret));
        }
      }
      break;
    case GIT_PARTITION_WISE_WITH_AFFINITY:
      if (OB_FAIL(fetch_pw_granule_by_worker_id(infos, op_ids, worker_id))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fetch pw granule by worker id failed", K(ret), K(worker_id));
        }
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(ret), K(splitter_type_));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_task(const ObGITaskSet *&res_task_set,
                                      int64_t &pos,
                                      int64_t worker_id,
                                      uint64_t tsc_op_id)
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
    switch(splitter_type_) {
    case GIT_AFFINITY:
    case GIT_ACCESS_ALL:
    case GIT_FULL_PARTITION_WISE:
    case GIT_PARTITION_WISE_WITH_AFFINITY:
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
      LOG_WARN("unexpected type", K(ret), K(splitter_type_));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_by_worker_id(const ObGITaskSet *&res_task_set,
                                              int64_t &pos,
                                              int64_t worker_id,
                                              uint64_t tsc_op_id)
{
  int ret = OB_SUCCESS;
  ObGITaskArray *taskset_array = nullptr;
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
    ObGITaskSet &taskset = taskset_array->at(worker_id);
    if (OB_FAIL(taskset.get_next_gi_task_pos(pos))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next gi task pos", K(ret));
      }
    } else {
      LOG_TRACE("get GI task", K(taskset), K(ret));
    }
  }
  return ret;
}

int ObGranulePump::fetch_granule_from_shared_pool(const ObGITaskSet *&res_task_set,
                                                  int64_t &pos,
                                                  uint64_t tsc_op_id)
{
  int ret = OB_SUCCESS;
  if (no_more_task_from_shared_pool_) {
    // when worker threads count >> shared task count, it performs better
    ret = OB_ITER_END;
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    if (no_more_task_from_shared_pool_) {
      ret = OB_ITER_END;
    }
    ObGITaskArray *taskset_array = nullptr;
    if (OB_FAIL(ret)) {
      //has been failed. do nothing.
    } else if (OB_FAIL(find_taskset_by_tsc_id(tsc_op_id, taskset_array))) {
      LOG_WARN("the tsc_op_id do not have task set", K(ret), K(tsc_op_id));
    } else if (OB_ISNULL(taskset_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the taskset_array is null", K(ret));
    } else if (taskset_array->count() < OB_GRANULE_SHARED_POOL_POS + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("taskset array count is invalid", K(ret), K(taskset_array->count()));
    } else {
      res_task_set = &taskset_array->at(OB_GRANULE_SHARED_POOL_POS);
      ObGITaskSet &taskset = taskset_array->at(OB_GRANULE_SHARED_POOL_POS);
      if (OB_FAIL(taskset.get_next_gi_task_pos(pos))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next gi task pos", K(ret));
        } else {
          no_more_task_from_shared_pool_ = true;
        }
      } else {
        LOG_TRACE("get GI task", K(taskset), K(ret));
      }
    }
  }
  return ret;
}

int ObGranulePump::fetch_pw_granule_by_worker_id(ObIArray<ObGranuleTaskInfo> &infos,
                                                 const ObIArray<int64_t> &op_ids,
                                                 int64_t thread_id)
{
  int ret = OB_SUCCESS;
  int64_t end_tsc_count = 0;
  if (GIT_PARTITION_WISE_WITH_AFFINITY != splitter_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only partition wise join granule pump offer this service", K(splitter_type_), K(ret));
  }
  ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret)) {
    ObGITaskArray *taskset_array = nullptr;
    ObGranuleTaskInfo info;
    uint64_t op_id = op_ids.at(idx);

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
  } else if (OB_FAIL(check_pw_end(end_tsc_count, op_ids.count(), infos.count()))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("incorrect state", K(ret));
    }
  }
  LOG_TRACE("get a new partition wise join gi tasks", K(infos), K(ret));
  return ret;
}

int ObGranulePump::fetch_pw_granule_from_shared_pool(ObIArray<ObGranuleTaskInfo> &infos,
                                                     const ObIArray<int64_t> &op_ids)
{

  int ret = OB_SUCCESS;
  if (no_more_task_from_shared_pool_) {
    ret = OB_ITER_END;
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    // 表示取不到下一个GI task的op的个数；
    // 理论上end_op_count只能等于0（表示gi任务还没有被消费完）或者等于`op_ids.count()`（表示gi任务全部被消费完）
    int64_t end_op_count = 0;
    if (OB_FAIL(fetch_task_ret_)) {
      LOG_WARN("fetch task concurrently already failed", K(ret));
    } else if (no_more_task_from_shared_pool_) {
      ret = OB_ITER_END;
    } else if (GIT_FULL_PARTITION_WISE != splitter_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only partition wise join granule pump offer this service", K(splitter_type_), K(ret));
    }
    ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret)) {
      ObGITaskArray *taskset_array = nullptr;
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

    // 防御性代码：检查full partition wise的情况下，每一个op对应的GI task是否被同时消费完毕
    if (OB_FAIL(ret)) {
      fetch_task_ret_ = ret;
    } else if (OB_FAIL(check_pw_end(end_op_count, op_ids.count(), infos.count()))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("incorrect state", K(ret));
      } else {
        no_more_task_from_shared_pool_ = true;
      }
    }
    LOG_TRACE("get a new partition wise join gi tasks", K(infos), K(ret));
  }
  return ret;
}

int ObGranulePump::check_pw_end(int64_t end_op_count, int64_t op_count, int64_t task_count)
{
  int ret = OB_SUCCESS;
  if (end_op_count !=0 && end_op_count != op_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the end op count does not match partition wise join ops count",
      K(end_op_count), K(op_count), K(ret));
  } else if (end_op_count != 0) {
    ret = OB_ITER_END;
  } else if (end_op_count == 0 && task_count != op_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the infos count does not match partition wise join ops count",
      K(end_op_count), K(task_count), K(op_count), K(ret));
  } else if (end_op_count == 0) {
    /* we get tasks for every tsc */
  }
  return ret;
}

/**
 * 该函数比较特殊，在一个SQC中它可能被调用多次，类似于这样的计划
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
 * 在sqc的setup_op_input流程中，找到一个GI会调用一次这个接口。
 *
 */
int ObGranulePump::add_new_gi_task(ObGranulePumpArgs &args)
{
  int ret = OB_SUCCESS;
  partition_wise_join_ = ObGranuleUtil::pwj_gi(args.gi_attri_flag_);
  ObTableModifySpec *modify_op = args.op_info_.get_modify_op();
  ObIArray<const ObTableScanSpec *> &scan_ops = args.op_info_.get_scan_ops();
  LOG_DEBUG("init granule", K(args));
  // GI目前不仅仅支持TSC的任务划分，还支持INSERT的任务划分，需要同时考虑INSERT与TSC
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

  ObGITaskSet::ObGIRandomType random_type = ObGITaskSet::GI_RANDOM_NONE;
  if (OB_SUCC(ret)) {
    // only GIT_FULL_PARTITION_WISE and GIT_RANDOM are possible now
    bool can_randomize = false;
    if (OB_FAIL(check_can_randomize(args, can_randomize))) {
      LOG_WARN("check can randomize failed", K(ret));
    } else if (can_randomize) {
      random_type = ObGITaskSet::GI_RANDOM_RANGE;
      LOG_TRACE("split random task/range for online ddl and pdml");
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObGranuleUtil::access_all(args.gi_attri_flag_)) {
    splitter_type_ = GIT_ACCESS_ALL;
    ObAccessAllGranuleSplitter splitter;
    if (OB_FAIL(splitter.split_granule(args,
                                       scan_ops,
                                       gi_task_array_map_,
                                       random_type))) {
      LOG_WARN("failed to prepare access all gi task", K(ret));
    }
  } else if (ObGranuleUtil::pwj_gi(args.gi_attri_flag_) &&
             ObGranuleUtil::affinitize(args.gi_attri_flag_)) {
    splitter_type_ = GIT_PARTITION_WISE_WITH_AFFINITY;
    ObPWAffinitizeGranuleSplitter splitter;
    if (OB_FAIL(splitter.partitions_info_.assign(args.partitions_info_))) {
      LOG_WARN("Failed to assign partitions info", K(ret));
    } else if (OB_FAIL(splitter.split_granule(args,
                                              scan_ops,
                                              gi_task_array_map_,
                                              random_type))) {
      LOG_WARN("failed to prepare affinity gi task", K(ret));
    }
  } else if (ObGranuleUtil::affinitize(args.gi_attri_flag_)) {
    splitter_type_ = GIT_AFFINITY;
    ObNormalAffinitizeGranuleSplitter splitter;
    if (OB_FAIL(splitter.partitions_info_.assign(args.partitions_info_))) {
      LOG_WARN("Failed to assign partitions info", K(ret));
    } else if (OB_FAIL(splitter.split_granule(args,
                                              scan_ops,
                                              gi_task_array_map_,
                                              random_type))) {
      LOG_WARN("failed to prepare affinity gi task", K(ret));
    }
  } else if (ObGranuleUtil::pwj_gi(args.gi_attri_flag_)) {
    splitter_type_ = GIT_FULL_PARTITION_WISE;
    ObPartitionWiseGranuleSplitter splitter;
    if (OB_FAIL(splitter.split_granule(args,
                                       scan_ops,
                                       modify_op,
                                       gi_task_array_map_,
                                       random_type))) {
      LOG_WARN("failed to prepare pw gi task", K(ret));
    }
  } else {
    splitter_type_ = GIT_RANDOM;
    ObRandomGranuleSplitter splitter;
    bool partition_granule = args.need_partition_granule();
    // TODO: randomize GI
    // if (!(args.asc_order() || args.desc_order() || ObGITaskSet::GI_RANDOM_NONE != random_type)) {
    //   random_type = ObGITaskSet::GI_RANDOM_TASK;
    // }
    if (OB_FAIL(splitter.split_granule(args,
                                       scan_ops,
                                       gi_task_array_map_,
                                       random_type,
                                       partition_granule))) {
      LOG_WARN("failed to prepare random gi task", K(ret), K(partition_granule));
    }
  }
  return ret;
}

int ObGranulePump::check_can_randomize(ObGranulePumpArgs &args, bool &can_randomize)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = nullptr;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  const ObPhysicalPlan *phy_plan = NULL;
  bool need_start_ddl = false;
  bool need_start_pdml = false;

  if (OB_ISNULL(args.ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, arg ctx must not be nullptr", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(*args.ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, session must not be nullptr", K(ret));
  } else if (my_session->get_ddl_info().is_ddl()) {
    need_start_ddl = true;
  }

  if(OB_SUCC(ret)) {
    if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*args.ctx_)) ||
        OB_ISNULL(phy_plan = phy_plan_ctx->get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some params are NULL", K(ret), K(phy_plan_ctx), K(phy_plan));
    } else if(phy_plan->is_use_pdml()) {
      need_start_pdml = true;
    }
  }

  // Only when in ddl and pdml, can randomize. Specially, can not randomize when sql specifies the order
  can_randomize = (need_start_ddl || need_start_pdml)
                  && (!(ObGranuleUtil::asc_order(args.gi_attri_flag_)
                        || ObGranuleUtil::desc_order(args.gi_attri_flag_)
                        || ObGranuleUtil::is_partition_granule_flag(args.gi_attri_flag_)));
  LOG_DEBUG("scan order is ", K(ObGranuleUtil::asc_order(args.gi_attri_flag_)),
            K(ObGranuleUtil::desc_order(args.gi_attri_flag_)),
            K(ObGranuleUtil::is_partition_granule_flag(args.gi_attri_flag_)), K(can_randomize),
            K(need_start_ddl), K(need_start_pdml));
  return ret;
}

void ObGranulePump::destroy()
{
  gi_task_array_map_.reset();
  pump_args_.reset();
}

void ObGranulePump::reset_task_array()
{
  gi_task_array_map_.reset();
}

int ObGranulePump::get_first_tsc_range_cnt(int64_t &cnt)
{
  int ret = OB_SUCCESS;
  if (gi_task_array_map_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happen, empty task array", K(ret));
  } else {
    ObGITaskArray &taskset_array = gi_task_array_map_.at(0).taskset_array_;
    if (taskset_array.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected taskset array", K(ret));
    } else {
      cnt = taskset_array.at(0).gi_task_set_.count();
    }
  }
  return ret;
}

int64_t ObGranulePump::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(parallelism),
       K_(tablet_size),
       K_(partition_wise_join));
  J_OBJ_END();
  return pos;
}

///////////////////////////////////////////////////////////////////////////////////////

int ObGranuleSplitter::split_gi_task(ObGranulePumpArgs &args,
                                     const ObTableScanSpec *tsc,
                                     int64_t table_id,
                                     int64_t op_id,
                                     const common::ObIArray<ObDASTabletLoc*> &tablets,
                                     bool partition_granule,
                                     ObGITaskSet &task_set,
                                     ObGITaskSet::ObGIRandomType random_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObNewRange, 16> ranges;
  ObSEArray<ObNewRange, 16> ss_ranges;
  DASTabletLocSEArray taskset_tablets;
  ObSEArray<ObNewRange, 16> taskset_ranges;
  ObSEArray<int64_t, 16> taskset_idxs;
  bool range_independent = random_type == ObGITaskSet::GI_RANDOM_RANGE;
  if (0 > args.parallelism_ || OB_ISNULL(tsc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parallelism is invalid", K(ret), K(args.parallelism_), K(tsc));
  } else if (0 > args.tablet_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the parallelism is invalid", K(ret), K(args.tablet_size_));
  } else if (tablets.count() <= 0 || OB_ISNULL(args.ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the task has an empty tablets", K(ret), K(tablets));
  } else if (!args.query_range_by_runtime_filter_.empty()
             && OB_FAIL(ranges.assign(args.query_range_by_runtime_filter_))) {
    LOG_WARN("failed to assign query range", K(ret), K(tablets));
  } else if (args.query_range_by_runtime_filter_.empty()
             && OB_FAIL(get_query_range(*args.ctx_, tsc->get_query_range(), ranges, ss_ranges,
                                        table_id, op_id, partition_granule,
                                        ObGranuleUtil::with_param_down(args.gi_attri_flag_)))) {
    LOG_WARN("get query range failed", K(ret));
  } else if (ranges.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the task has an empty range", K(ret), K(ranges));
  } else {
    bool is_external_table = tsc->tsc_ctdef_.scan_ctdef_.is_external_table_;
    if (is_external_table) {
      ret = ObGranuleUtil::split_granule_for_external_table(args.ctx_->get_allocator(),
                                                            tsc,
                                                            ranges,
                                                            tablets,
                                                            args.external_table_files_,
                                                            args.parallelism_,
                                                            taskset_tablets,
                                                            taskset_ranges,
                                                            taskset_idxs);
    } else {
      ret = ObGranuleUtil::split_block_ranges(*args.ctx_,
                                              args.ctx_->get_allocator(),
                                              tsc,
                                              ranges,
                                              tablets,
                                              args.parallelism_,
                                              args.tablet_size_,
                                              partition_granule,
                                              taskset_tablets,
                                              taskset_ranges,
                                              taskset_idxs,
                                              range_independent);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get granule task", K(ret), K(ranges), K(tablets), K(is_external_table));
    } else if (OB_FAIL(task_set.construct_taskset(taskset_tablets,
                                                  taskset_ranges,
                                                  ss_ranges,
                                                  taskset_idxs,
                                                  random_type))) {
      LOG_WARN("construct taskset failed", K(ret), K(taskset_tablets),
                                                   K(taskset_ranges),
                                                   K(ss_ranges),
                                                   K(taskset_idxs),
                                                   K(random_type));
    }
  }
  return ret;
}

int ObGranuleSplitter::get_query_range(ObExecContext &ctx,
                                       const ObQueryRange &tsc_pre_query_range,
                                       ObIArray<ObNewRange> &ranges,
                                       ObIArray<ObNewRange> &ss_ranges,
                                       int64_t table_id,
                                       int64_t op_id,
                                       bool partition_granule,
                                       bool with_param_down /* = false */)
{
  int ret = OB_SUCCESS;
  ObQueryRangeArray scan_ranges;
  ObQueryRangeArray skip_scan_ranges;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  bool has_extract_query_range = false;
  // 如果tsc有对应的query range，就预先抽取对应的query range
  LOG_DEBUG("set partition granule to whole range", K(table_id), K(op_id),
      K(partition_granule), K(with_param_down),
      K(tsc_pre_query_range.get_column_count()),
      K(tsc_pre_query_range.has_exec_param()));
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get physical plan ctx", K(ret));
  } else if (partition_granule) {
    // For partition granule, we will prepare query range in table scan.
    ObNewRange whole_range;
    // partition granule情况下，尽量提前抽取有效的query range，如果无法抽取有效的query range
    // 就使用whole range
    if (0 == tsc_pre_query_range.get_column_count()) {
      whole_range.set_whole_range();
      has_extract_query_range = !tsc_pre_query_range.has_exec_param();
    } else if (OB_FAIL(ObSQLUtils::make_whole_range(ctx.get_allocator(),
                                                    table_id,
                                                    tsc_pre_query_range.get_column_count(),
                                                    whole_range))) {
      LOG_WARN("Failed to make whole range", K(ret));
    } else if (!tsc_pre_query_range.has_exec_param()) {
      // 没有动态参数才能够进行query range的提前抽取
      LOG_DEBUG("try to get scan range for partition granule");
      if (OB_FAIL(ObSQLUtils::extract_pre_query_range(
                    tsc_pre_query_range,
                    ctx.get_allocator(),
                    ctx,
                    scan_ranges,
                    ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
        LOG_WARN("failed to get scan ranges", K(ret));
      } else if (OB_FAIL(tsc_pre_query_range.get_ss_tablet_ranges(
                                  ctx.get_allocator(),
                                  ctx,
                                  skip_scan_ranges,
                                  ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
        LOG_WARN("failed to final extract index skip query range", K(ret));
      } else {
        has_extract_query_range = true;
      }
    }
    if (OB_SUCC(ret)) {
      // 没有抽取出来query range, 就使用whole range
      if (scan_ranges.empty()) {
        LOG_DEBUG("the scan ranges is invalid, use the whole range", K(scan_ranges));
        if (OB_FAIL(ranges.push_back(whole_range))) {
           LOG_WARN("Failed to push back scan range", K(ret));
        }
      }
    }
  } else {
    if (tsc_pre_query_range.has_exec_param() &&
        with_param_down) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block iterater cannot have exec param", K(ret), K(op_id), K(tsc_pre_query_range));
    } else if (OB_FAIL(ObSQLUtils::extract_pre_query_range(
                      tsc_pre_query_range,
                      ctx.get_allocator(),
                      ctx,
                      scan_ranges,
                      ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
      LOG_WARN("failed to get scan ranges", K(ret));
    } else if (OB_FAIL(tsc_pre_query_range.get_ss_tablet_ranges(
                                  ctx.get_allocator(),
                                  ctx,
                                  skip_scan_ranges,
                                  ObBasicSessionInfo::create_dtc_params(ctx.get_my_session())))) {
        LOG_WARN("failed to final extract index skip query range", K(ret));
    } else {
      has_extract_query_range = true;
      /* Here is an improvement made:
        1. By default, access to each partition is always in sequential ascending order.
        Compared to not sorting, this is an enhancement that is not always 100% necessary
        (some scenarios do not require order), but the logic is acceptable.
        2. If reverse order is required outside, then the reverse sorting should be done outside on its own.
      */
      lib::ob_sort(scan_ranges.begin(), scan_ranges.end(), ObNewRangeCmp());
    }
  }

  LOG_DEBUG("gi get the scan range", K(ret), K(partition_granule), K(has_extract_query_range),
                                     K(scan_ranges), K(skip_scan_ranges));
  if (OB_SUCC(ret)) {
    // index skip scan, ranges from extract_pre_query_range/get_ss_tablet_ranges,
    //  prefix range and postfix range is single range
    ObNewRange *ss_range = NULL;
    ObNewRange whole_range;
    whole_range.set_whole_range();
    if (!skip_scan_ranges.empty() &&
        (OB_ISNULL(skip_scan_ranges.at(0)) ||
         OB_UNLIKELY(1 != skip_scan_ranges.count() || 1 != scan_ranges.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index skip scan range", K(ret), K(scan_ranges), K(skip_scan_ranges));
    } else if (OB_FAIL(ss_ranges.push_back(skip_scan_ranges.empty()
                                           ? whole_range : *skip_scan_ranges.at(0)))) {
      LOG_WARN("push back ranges failed", K(ret));
    } else {
      ss_ranges.at(ss_ranges.count() - 1).table_id_ = table_id;
    }
  }
  for (int64_t i = 0; i < scan_ranges.count() && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(scan_ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the scan range is null", K(ret));
    } else if (OB_FAIL(ranges.push_back(*scan_ranges.at(i)))) {
      LOG_WARN("push back ranges failed", K(ret));
    } else {
      ranges.at(ranges.count() - 1).table_id_ = table_id;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObOperatorKit *kit = ctx.get_operator_kit(op_id);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit), K(table_id), K(op_id));
    } else {
      ObTableScanOpInput *tsc_input = static_cast<ObTableScanOpInput*>(kit->input_);
      tsc_input->set_need_extract_query_range(!has_extract_query_range);
    }
  }
  return ret;
}

int ObRandomGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                           ObIArray<const ObTableScanSpec *> &scan_ops,
                                           GITaskArrayMap &gi_task_array_result,
                                           ObGITaskSet::ObGIRandomType random_type,
                                           bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()),
        K(gi_task_array_result.count()));
  } else if (ObGITaskSet::GI_RANDOM_NONE != random_type &&
             (ObGranuleUtil::asc_order(args.gi_attri_flag_) ||
             (ObGranuleUtil::desc_order(args.gi_attri_flag_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict order config", K(random_type),
        K(ObGranuleUtil::desc_order(args.gi_attri_flag_)));
  }
  const common::ObIArray<DASTabletLocArray> &tablet_arrays = args.tablet_arrays_;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    const ObTableScanSpec *tsc = scan_ops.at(idx);
    if (OB_ISNULL(tsc) || scan_ops.count() != tablet_arrays.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret), K(scan_ops.count()), K(tablet_arrays.count()));
    } else {
      uint64_t scan_key_id = tsc->get_scan_key_id();
      uint64_t op_id = tsc->get_id();
      ObGITaskSet total_task_set;
      ObGITaskArray &taskset_array = gi_task_array_result.at(idx).taskset_array_;
      partition_granule = is_virtual_table(scan_key_id) || partition_granule;
      if (OB_FAIL(split_gi_task(args,
                                tsc,
                                scan_key_id,
                                op_id,
                                tablet_arrays.at(idx),
                                partition_granule,
                                total_task_set,
                                random_type))) {
        LOG_WARN("failed to init granule iter pump", K(ret), K(idx), K(tablet_arrays));
      } else if (OB_FAIL(total_task_set.set_block_order(
            ObGranuleUtil::desc_order(args.gi_attri_flag_)))) {
        LOG_WARN("fail set block order", K(ret));
      } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
        LOG_WARN("failed to push back task set", K(ret));
      } else {
        gi_task_array_result.at(idx).tsc_op_id_ = op_id;
      }
      LOG_TRACE("random granule split a task_array",
        K(op_id), K(scan_key_id), K(taskset_array), K(ret), K(scan_ops.count()));
    }
  }
  return ret;
}

// duplicate all scan ranges to each worker, so that every worker can
// access all data
int ObAccessAllGranuleSplitter::split_tasks_access_all(ObGITaskSet &taskset,
                                                       int64_t parallelism,
                                                       ObGITaskArray &taskset_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < parallelism && OB_SUCC(ret); ++i) {
    if(OB_FAIL(taskset_array.at(i).assign(taskset))) {
      LOG_WARN("failed to assign taskset", K(ret));
    }
  }
  return ret;
}

int ObAccessAllGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                              ObIArray<const ObTableScanSpec *> &scan_ops,
                                              GITaskArrayMap &gi_task_array_result,
                                              ObGITaskSet::ObGIRandomType random_type,
                                              bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()),
        K(gi_task_array_result.count()));
  }
  const common::ObIArray<DASTabletLocArray> &tablet_arrays = args.tablet_arrays_;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    const ObTableScanSpec *tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    ObGITaskArray &taskset_array = gi_task_array_result.at(idx).taskset_array_;
    if (OB_ISNULL(tsc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(taskset_array.prepare_allocate(args.parallelism_))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(args,
                                              tsc,
                                              scan_key_id,
                                              op_id,
                                              tablet_arrays.at(idx),
                                              partition_granule,
                                              total_task_set,
                                              random_type))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_access_all(total_task_set, args.parallelism_, taskset_array))) {
      LOG_WARN("failed to split ");
    } else {
      gi_task_array_result.at(idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("access all granule split a task_array",
      K(op_id), K(tsc->get_loc_ref_table_id()), K(taskset_array), K(ret), K(scan_ops.count()));
  }
  return ret;
}

int ObAffinitizeGranuleSplitter::split_tasks_affinity(ObExecContext &ctx,
                                                      ObGITaskSet &taskset,
                                                      int64_t parallelism,
                                                      ObGITaskArray &taskset_array)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *my_session = NULL;
  ObPxTabletInfo partition_row_info;
  ObTabletIdxMap idx_map;
  bool qc_order_gi_tasks = false;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx)) || OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret), K(my_session), K(ctx.get_sqc_handler()));
  } else {
    qc_order_gi_tasks = ctx.get_sqc_handler()->get_sqc_init_arg().qc_order_gi_tasks_;
  }
  int64_t cur_idx = -1;
  ObPxAffinityByRandom affinitize_rule(qc_order_gi_tasks);
  ARRAY_FOREACH_X(taskset.gi_task_set_, idx, cnt, OB_SUCC(ret)) {
    if (cur_idx != taskset.gi_task_set_.at(idx).idx_) {
      cur_idx = taskset.gi_task_set_.at(idx).idx_; // get all different parition key in Affinitize
      const ObDASTabletLoc &tablet_loc = *taskset.gi_task_set_.at(idx).tablet_loc_;
      int64_t tablet_idx = -1;
      if (NULL == table_schema || table_schema->get_table_id() != tablet_loc.loc_meta_->ref_table_id_) {
        uint64_t table_id = tablet_loc.loc_meta_->ref_table_id_;
        if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                    my_session->get_effective_tenant_id(),
                    schema_guard))) {
          LOG_WARN("Failed to get schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(
                   my_session->get_effective_tenant_id(),
                   table_id, table_schema))) {
          LOG_WARN("Failed to get table schema", K(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("Table schema is null", K(ret), K(table_id));
        } else if (OB_FAIL(ObPXServerAddrUtil::build_tablet_idx_map(table_schema, idx_map))) {
          LOG_WARN("fail to build tablet idx map", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // see issue
        // for virtual table, we can directly mock a tablet id
        // function build_tablet_idx_map will mock a idx map whose key
        // varies from 1 to table_schema->get_all_part_num(), and the value = key + 1
        // so we can directly set tablet_idx = tablet_loc.tablet_id_.id() + 1, the result is same
        if (is_virtual_table(table_schema->get_table_id())) {
          tablet_idx = tablet_loc.tablet_id_.id() + 1;
        } else if (OB_FAIL(idx_map.get_refactored(tablet_loc.tablet_id_.id(), tablet_idx))) {
          ret = OB_HASH_NOT_EXIST == ret ? OB_SCHEMA_ERROR : ret;
          LOG_WARN("fail to get tablet idx", K(ret), K(tablet_loc), KPC(table_schema));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObPxAffinityByRandom::get_tablet_info(tablet_loc.tablet_id_.id(),
                                                               partitions_info_,
                                                               partition_row_info))) {
        LOG_WARN("Failed to get tablet info", K(ret));
      } else if (OB_FAIL(affinitize_rule.add_partition(tablet_loc.tablet_id_.id(),
                                                      tablet_idx,
                                                      parallelism,
                                                      my_session->get_effective_tenant_id(),
                                                      partition_row_info))) {
        LOG_WARN("Failed to get affinitize taskid" , K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(affinitize_rule.do_random(!partitions_info_.empty(),
                                               my_session->get_effective_tenant_id()))) {
    LOG_WARN("failed to do random", K(ret));
  } else {
    const ObIArray<ObPxAffinityByRandom::TabletHashValue> &partition_worker_pairs = affinitize_rule.get_result();
    ARRAY_FOREACH(partition_worker_pairs, rt_idx) {
      int64_t task_id = partition_worker_pairs.at(rt_idx).worker_id_;
      int64_t tablet_id = partition_worker_pairs.at(rt_idx).tablet_id_;
      if (task_id >= parallelism) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Task id is invalid", K(ret), K(task_id), K(parallelism));
      }
      ARRAY_FOREACH(taskset.gi_task_set_, idx) {
        const ObDASTabletLoc &tablet_key = *taskset.gi_task_set_.at(idx).tablet_loc_;
        if (tablet_id == tablet_key.tablet_id_.id()) {
          ObGITaskSet &real_task_set = taskset_array.at(task_id);
          if (OB_FAIL(real_task_set.gi_task_set_.push_back(taskset.gi_task_set_.at(idx)))) {
            LOG_WARN("Failed to push back task info", K(ret));
          }
        }
      }
      LOG_TRACE("affinitize granule split a task_array",
          K(tablet_id), K(task_id), K(parallelism), K(taskset_array), K(ret));
    }
  }
  return ret;
}

int ObNormalAffinitizeGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                                     ObIArray<const ObTableScanSpec *> &scan_ops,
                                                     GITaskArrayMap &gi_task_array_result,
                                                     ObGITaskSet::ObGIRandomType random_type,
                                                     bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  if (scan_ops.count() != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(scan_ops.count()),
        K(gi_task_array_result.count()));
  }
  const common::ObIArray<DASTabletLocArray> &tablet_arrays = args.tablet_arrays_;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    const ObTableScanSpec *tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    ObGITaskArray &taskset_array = gi_task_array_result.at(idx).taskset_array_;
    if (OB_ISNULL(tsc) || OB_ISNULL(args.ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(gi_task_array_result.at(idx).taskset_array_.prepare_allocate(args.parallelism_))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(args,
                                     tsc,
                                     scan_key_id,
                                     op_id,
                                     tablet_arrays.at(idx),
                                     partition_granule,
                                     total_task_set,
                                     random_type))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_affinity(*args.ctx_, total_task_set, args.parallelism_,
        taskset_array))) {
      LOG_WARN("failed to split task affinity", K(ret));
    } else {
      gi_task_array_result.at(idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("normal affinitize granule split a task_array",
      K(op_id), K(tsc->get_loc_ref_table_id()), K(taskset_array), K(ret), K(scan_ops.count()));
  }
  return ret;
}

int ObPartitionWiseGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                                  ObIArray<const ObTableScanSpec *> &scan_ops,
                                                  GITaskArrayMap &gi_task_array_result,
                                                  ObGITaskSet::ObGIRandomType random_type,
                                                  bool partition_granule /* = true */)
{
  // 由于FULL PARTITION WISE的split方法较为特殊（需要对INSERT进行任务切分），而目前的`ObGranuleSplitter`的接口`split_granule`仅仅考虑了TSC的处理，
  // 因此将`ObPartitionWiseGranuleSplitter`的该接口废弃掉
  UNUSED(args);
  UNUSED(scan_ops);
  UNUSED(gi_task_array_result);
  UNUSED(random_type);
  UNUSED(partition_granule);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "split granule method");
  return ret;
}

// FULL PARTITION WISE 独有的split方法，可以处理INSERT/REPLACE的任务切分
int ObPartitionWiseGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                      ObIArray<const ObTableScanSpec *> &scan_ops,
                                      const ObTableModifySpec * modify_op,
                                      GITaskArrayMap &gi_task_array_result,
                                      ObGITaskSet::ObGIRandomType random_type,
                                      bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  int expected_map_size = 0;
  // 如果GI需要切分INSERT/REPLACE任务，那么tablet_arrays中不仅包含了table_scans表对应的partition keys信息，还包含了
  // insert/replace表对应的partition keys信息；例如这样的计划：
  // ....
  //     GI
  //      INSERT/REPLACE
  //         JOIN
  //          TSC1
  //          TSC2
  // `tablet_arrays`的第一个元素对应的是INSERT/REPLACE表的partition keys，其他元素对应的是TSC的表的partition keys
  int tsc_begin_idx = 0;
  const common::ObIArray<DASTabletLocArray> &tablet_arrays = args.tablet_arrays_;
  if (OB_NOT_NULL(modify_op)) {
    expected_map_size++;
    tsc_begin_idx = 1; // 目前最多只有一个INSERT/REPLACE算子
  }
  expected_map_size += scan_ops.count();
  if (expected_map_size != gi_task_array_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scan ops and gi task array result", K(ret), K(expected_map_size),
        K(gi_task_array_result.count()), K(modify_op!=NULL), K(scan_ops.count()));
  } else if (tablet_arrays.count() != expected_map_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(ret), K(tablet_arrays.count()), K(expected_map_size));
  } else if (0 >= tablet_arrays.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(ret), K(tablet_arrays.count()));
  }
  // 校验：校验REPLACE对应的分区与TSC对应的分区是否在逻辑上是相同的
  if (OB_SUCC(ret)) {
    int tablet_count = tablet_arrays.at(0).count();
    DASTabletLocArray tablets = tablet_arrays.at(0);
    ARRAY_FOREACH(tablet_arrays, idx) {
      // 校验每一个op对应的partition key的个数是相同的
      DASTabletLocArray tablet_array = tablet_arrays.at(idx);
      if (tablet_count != tablet_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the tablet count is not equal", K(ret), K(tablet_count), K(tablet_arrays.count()));
      }
    }
  }
  // 处理 insert/replace的任务划分
  if (OB_SUCC(ret) && OB_NOT_NULL(modify_op)) {
    int index_idx = 0;
    ObGITaskSet total_task_set;
    ObGITaskArray &taskset_array = gi_task_array_result.at(index_idx).taskset_array_;
    const ObDMLBaseCtDef *dml_ctdef = nullptr;
    LOG_TRACE("handler split dml op task", K(modify_op->get_type()));
    if (OB_FAIL(modify_op->get_single_dml_ctdef(dml_ctdef))) {
      LOG_WARN("get single table loc id failed", K(ret));
    } else if (OB_FAIL(split_insert_gi_task(args,
                                    dml_ctdef->das_base_ctdef_.index_tid_,
                                    dml_ctdef->das_base_ctdef_.rowkey_cnt_, // insert对应的row key count
                                    tablet_arrays.at(0),
                                    partition_granule,
                                    total_task_set,
                                    random_type))){
      LOG_WARN("failed to prepare pw insert gi task", K(ret));
    } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
      LOG_WARN("failed to push back task set", K(ret));
    } else {
      // 获得对应的insert/replace op id
      LOG_TRACE("split modify gi task successfully", K(modify_op->get_id()));
      gi_task_array_result.at(index_idx).tsc_op_id_ = modify_op->get_id();
    }
  }
  // 处理 tsc的任务划分
  if(OB_SUCC(ret)) {
    ObSEArray<DASTabletLocArray, 4> tsc_tablet_arrays;
    for (int i = tsc_begin_idx; i < tablet_arrays.count(); i++) {
      if (OB_FAIL(tsc_tablet_arrays.push_back(tablet_arrays.at(i)))) {
        LOG_WARN("failed to push back tsc tablet arrays", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // pass
    } else if (OB_FAIL(split_tsc_gi_task(args,
                                    scan_ops,
                                    tsc_tablet_arrays,
                                    tsc_begin_idx,
                                    gi_task_array_result,
                                    partition_granule,
                                    random_type))) {
      LOG_WARN("failed to prepare pw tsc gi task", K(ret));
    }
  }

  return ret;
}

int ObPartitionWiseGranuleSplitter::split_insert_gi_task(ObGranulePumpArgs &args,
                                            const uint64_t insert_table_id,
                                            const int64_t row_key_count,
                                            const common::ObIArray<ObDASTabletLoc*> &tablets,
                                            bool partition_granule,
                                            ObGITaskSet &task_set,
                                            ObGITaskSet::ObGIRandomType random_type)
{
  // 目前INSERT对应的GI一定是full partition wise类型，任务的划分粒度一定是按照partition进行划分
  int ret = OB_SUCCESS;
  // insert的每一个partition对应的区间默认是[min_rowkey,max_rowkey]
  ObNewRange each_partition_range;
  ObSEArray<ObNewRange, 4> ranges;
  ObSEArray<ObNewRange, 1> empty_ss_ranges;
  DASTabletLocSEArray taskset_tablets;
  ObSEArray<ObNewRange, 4> taskset_ranges;
  ObSEArray<int64_t, 4> taskset_idxs;
  bool range_independent = random_type == ObGITaskSet::GI_RANDOM_RANGE;
  if (0 >= args.parallelism_ || tablets.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected args", K(ret), K(args.parallelism_), K(tablets.count()));
  } else if (OB_FAIL(ObSQLUtils::make_whole_range(args.ctx_->get_allocator(),
                                                  insert_table_id,
                                                  row_key_count,
                                                  each_partition_range))) {
    LOG_WARN("failed to make whole range", K(ret));
  } else if (OB_FAIL(ranges.push_back(each_partition_range))) {
    LOG_WARN("failed to push partition range to ranges", K(ret));
  } else if (OB_FAIL(ObGranuleUtil::split_block_ranges(*args.ctx_,
                                                       args.ctx_->get_allocator(),
                                                       NULL,
                                                       ranges,
                                                       tablets,
                                                       args.parallelism_,
                                                       args.tablet_size_,
                                                       partition_granule, // true
                                                       taskset_tablets,
                                                       taskset_ranges,
                                                       taskset_idxs,
                                                       range_independent))) {
    LOG_WARN("failed to get insert granule task", K(ret), K(each_partition_range), K(tablets));
  } else if (OB_FAIL(task_set.construct_taskset(taskset_tablets, taskset_ranges,
                                                empty_ss_ranges, taskset_idxs, random_type))) {
    // INSERT的任务划分一定是 partition wise的，并且INSERT算子每次rescan仅仅需要每一个task对应的partition key，
    // `ranges`,`idx`等任务参数是不需要
    LOG_WARN("construct taskset failed", K(ret), K(taskset_tablets),
                                                 K(taskset_ranges),
                                                 K(taskset_idxs),
                                                 K(random_type));
  }
  return ret;
}

int ObPartitionWiseGranuleSplitter::split_tsc_gi_task(ObGranulePumpArgs &args,
                                                  ObIArray<const ObTableScanSpec *> &scan_ops,
                                                  const common::ObIArray<DASTabletLocArray> &tablet_arrays,
                                                  int64_t tsc_begin_idx,
                                                  GITaskArrayMap &gi_task_array_result,
                                                  bool partition_granule,
                                                  ObGITaskSet::ObGIRandomType random_type)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    const ObTableScanSpec *tsc = scan_ops.at(idx);
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    int64_t task_array_idx = idx + tsc_begin_idx;
    ObGITaskArray &taskset_array = gi_task_array_result.at(task_array_idx).taskset_array_;
    if (OB_ISNULL(tsc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc ptr", K(ret));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(split_gi_task(args,
                                     tsc,
                                     scan_key_id,
                                     op_id,
                                     tablet_arrays.at(idx),
                                     partition_granule,
                                     total_task_set,
                                     random_type))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(taskset_array.push_back(total_task_set))) {
      LOG_WARN("failed to push back task set", K(ret));
    } else {
      gi_task_array_result.at(task_array_idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("partition wise tsc granule split a task_array",
      K(op_id), K(tsc->get_loc_ref_table_id()), K(taskset_array), K(ret), K(scan_ops.count()));
  }
  return ret;
}

int ObPWAffinitizeGranuleSplitter::split_granule(ObGranulePumpArgs &args,
                                                 ObIArray<const ObTableScanSpec *> &scan_ops,
                                                 GITaskArrayMap &gi_task_array_result,
                                                 ObGITaskSet::ObGIRandomType random_type,
                                                 bool partition_granule /* = true */)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = gi_task_array_result.count();
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    GITaskArrayItem empty_task_array_item;
    if (OB_FAIL(gi_task_array_result.push_back(empty_task_array_item))) {
      LOG_WARN("push back new task array failed", K(ret));
    }
  }
  const common::ObIArray<DASTabletLocArray> &tablet_arrays = args.tablet_arrays_;
  ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret)) {
    const ObTableScanSpec *tsc = scan_ops.at(idx);
    const ObGranuleIteratorSpec *gi = static_cast<const ObGranuleIteratorSpec *>(tsc->get_parent());
    const ObOpSpec *gi_spec = tsc->get_parent();
    while (OB_NOT_NULL(gi_spec) && PHY_GRANULE_ITERATOR != gi_spec->get_type()) {
      gi_spec = gi_spec->get_parent();
    }
    ObGITaskSet total_task_set;
    uint64_t op_id = OB_INVALID_ID;
    uint64_t scan_key_id = OB_INVALID_ID;
    bool asc_gi_task_order = true;
    ObGITaskArray &taskset_array = gi_task_array_result.at(idx + task_idx).taskset_array_;
    if (OB_ISNULL(tsc) || OB_ISNULL(gi_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get a null tsc/gi ptr", K(ret), K(tsc), K(gi_spec));
    } else if (FALSE_IT(op_id = tsc->get_id())) {
    } else if (FALSE_IT(asc_gi_task_order = !ObGranuleUtil::desc_order(static_cast<const ObGranuleIteratorSpec *>(gi_spec)->gi_attri_flag_))) {
    } else if (FALSE_IT(scan_key_id = tsc->get_scan_key_id())) {
    } else if (OB_FAIL(taskset_array.prepare_allocate(args.parallelism_))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(split_gi_task(args,
                                     tsc,
                                     scan_key_id,
                                     op_id,
                                     tablet_arrays.at(idx),
                                     partition_granule,
                                     total_task_set,
                                     random_type))) {
      LOG_WARN("failed to init granule iter pump", K(ret));
    } else if (OB_FAIL(split_tasks_affinity(*args.ctx_, total_task_set, args.parallelism_,
        taskset_array))) {
      LOG_WARN("failed to split task affinity", K(ret));
    } else if (OB_FAIL(adjust_task_order(asc_gi_task_order, taskset_array))) {
      LOG_WARN("failed to adjust task order", K(ret));
    } else {
      gi_task_array_result.at(idx + task_idx).tsc_op_id_ = op_id;
    }
    LOG_TRACE("partition wise with affinitize granule split a task_array",
      K(op_id), K(taskset_array), K(ret), K(scan_ops.count()));
  }
  return ret;
}

int ObPWAffinitizeGranuleSplitter::adjust_task_order(bool asc, ObGITaskArray &taskset_array)
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

int ObGranulePump::find_taskset_by_tsc_id(uint64_t op_id, ObGITaskArray *&taskset_array)
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

int ObGranulePump::init_pump_args_inner(ObExecContext *ctx,
    ObIArray<const ObTableScanSpec*> &scan_ops,
    const common::ObIArray<DASTabletLocArray> &tablet_arrays,
    common::ObIArray<ObPxTabletInfo> &partitions_info,
    common::ObIArray<ObExternalFileInfo> &external_table_files,
    const ObTableModifySpec* modify_op,
    int64_t parallelism,
    int64_t tablet_size,
    uint64_t gi_attri_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or partition service is null", K(ret));
  } else {
    ObGranulePumpArgs new_arg;
    if (!(ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_PARTITION_WISE) &&
        ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_AFFINITIZE)) &&
        pump_args_.count() > 0) {
      if (pump_args_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("args is unexpected", K(ret));
      } else {
        if (OB_FAIL(init_arg(pump_args_.at(0), ctx, scan_ops, tablet_arrays, partitions_info,
          external_table_files, modify_op, parallelism, tablet_size, gi_attri_flag))) {
          LOG_WARN("fail to init arg", K(ret));
        } else if (OB_FAIL(add_new_gi_task(pump_args_.at(0)))) {
          LOG_WARN("fail to add new gi task", K(ret));
        }
      }
    } else if (OB_FAIL(init_arg(new_arg, ctx, scan_ops, tablet_arrays, partitions_info,
          external_table_files, modify_op, parallelism, tablet_size, gi_attri_flag))) {
      LOG_WARN("fail to init arg", K(ret));
    } else if (OB_FAIL(pump_args_.push_back(new_arg))) {
      LOG_WARN("fail to push back new arg", K(ret));
    } else if (OB_FAIL(add_new_gi_task(new_arg))) {
      LOG_WARN("fail to add new gi task", K(ret));
    }
  }
  return ret;
}

int ObGranulePump::init_pump_args(ObExecContext *ctx,
    ObIArray<const ObTableScanSpec*> &scan_ops,
    const common::ObIArray<DASTabletLocArray> &tablet_arrays,
    common::ObIArray<ObPxTabletInfo> &partitions_info,
    common::ObIArray<share::ObExternalFileInfo> &external_table_files,
    const ObTableModifySpec* modify_op,
    int64_t parallelism,
    int64_t tablet_size,
    uint64_t gi_attri_flag)
{
  return init_pump_args_inner(ctx, scan_ops, tablet_arrays, partitions_info,
                              external_table_files, modify_op, parallelism,
                              tablet_size, gi_attri_flag);
}

int ObGranulePump::init_arg(
    ObGranulePumpArgs &arg,
    ObExecContext *ctx,
    ObIArray<const ObTableScanSpec*> &scan_ops,
    const common::ObIArray<DASTabletLocArray> &tablet_arrays,
    common::ObIArray<ObPxTabletInfo> &partitions_info,
    const common::ObIArray<ObExternalFileInfo> &external_table_files,
    const ObTableModifySpec* modify_op,
    int64_t parallelism,
    int64_t tablet_size,
    uint64_t gi_attri_flag)
{
  int ret = OB_SUCCESS;
  arg.op_info_.reset();
  arg.tablet_arrays_.reset();
  for (int i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
    OZ(arg.op_info_.push_back_scan_ops(scan_ops.at(i)));
  }
  for (int i = 0; OB_SUCC(ret) && i < tablet_arrays.count(); ++i) {
    OZ(arg.tablet_arrays_.push_back(tablet_arrays.at(i)));
  }
  for (int i = 0; OB_SUCC(ret) && i < partitions_info.count(); ++i) {
    OZ(arg.partitions_info_.push_back(partitions_info.at(i)));
  }

  OZ(arg.external_table_files_.assign(external_table_files));

  if (OB_SUCC(ret)) {
    arg.ctx_ = ctx;
    arg.op_info_.init_modify_op(modify_op);
    arg.parallelism_ = parallelism;
    arg.tablet_size_= tablet_size;
    arg.gi_attri_flag_ = gi_attri_flag;
    if (ObGranuleUtil::partition_filter(gi_attri_flag) &&
        !ObGranuleUtil::is_partition_task_mode(gi_attri_flag)) {
      if (1 != tablet_arrays.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet array cnt is unexpected", K(tablet_arrays.count()));
      } else {
        const DASTabletLocArray &array = tablet_arrays.at(0);
        arg.cur_tablet_idx_ = 0;
        for (int i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
          OZ(arg.run_time_pruning_flags_.push_back(false));
        }
      }
    }
  }
  return ret;
}

int ObGranulePump::regenerate_gi_task()
{
  int ret = common::OB_SUCCESS;
  pump_version_ += 1;
  reset_task_array();
  no_more_task_from_shared_pool_ = false;
  for (int i = 0; i < pump_args_.count() && OB_SUCC(ret); ++i) {
    ObGranulePumpArgs &arg = pump_args_.at(i);
    if (OB_FAIL(add_new_gi_task(arg))) {
      LOG_WARN("failed to add new gi task", K(ret));
    }
  }
  return ret;
}

int ObGranulePump::reset_gi_task()
{
  int ret = common::OB_SUCCESS;
  if (is_taskset_reset_) {
  } else {
    ObLockGuard<ObSpinLock> lock_guard(lock_);
    if (is_taskset_reset_) {
      /*do nothing*/
    } else {
      is_taskset_reset_ = true;
      no_more_task_from_shared_pool_ = false;
      fetch_task_ret_ = OB_SUCCESS;
      for (int64_t i = 0; i < gi_task_array_map_.count() && OB_SUCC(ret); ++i) {
        GITaskArrayItem &item = gi_task_array_map_.at(i);
        for(int64_t j = 0; j < item.taskset_array_.count() && OB_SUCC(ret); ++j) {
          ObGITaskSet &taskset = item.taskset_array_.at(j);
          taskset.cur_pos_ = 0;
        }
      }
    }
  }
  return ret;
}

int ObGranulePumpArgs::assign(const ObGranulePumpArgs &rhs)
{
  int ret = OB_SUCCESS;
  ctx_ = rhs.ctx_;
  cur_tablet_idx_ = rhs.cur_tablet_idx_;
  finish_pruning_tablet_idx_ = rhs.finish_pruning_tablet_idx_;
  sharing_iter_end_ = rhs.sharing_iter_end_;
  pruning_status_ = rhs.pruning_status_;
  pruning_ret_ = rhs.pruning_ret_;
  parallelism_ = rhs.parallelism_;
  tablet_size_ = rhs.tablet_size_;
  gi_attri_flag_ = rhs.gi_attri_flag_;
  lucky_one_ = rhs.lucky_one_;
  extract_finished_ = rhs.extract_finished_;
  if (OB_FAIL(op_info_.assign(rhs.op_info_))) {
    LOG_WARN("Failed to assign op_info", K(ret));
  } else if (OB_FAIL(tablet_arrays_.assign(rhs.tablet_arrays_))) {
    LOG_WARN("Failed to assign tablet_arrays", K(ret));
  } else if (OB_FAIL(run_time_pruning_flags_.assign(rhs.run_time_pruning_flags_))) {
    LOG_WARN("Failed to assign run_time_pruning_flags", K(ret));
  } else if (OB_FAIL(partitions_info_.assign(rhs.partitions_info_))) {
    LOG_WARN("Failed to assign partitions_info", K(ret));
  } else if (OB_FAIL(external_table_files_.assign(rhs.external_table_files_))) {
    LOG_WARN("Failed to assign external_table_files", K(ret));
  } else if (OB_FAIL(query_range_by_runtime_filter_.assign(rhs.query_range_by_runtime_filter_))) {
    LOG_WARN("Failed to assign query_range_by_runtime_filter", K(ret));
  }
  return ret;
}

int ObGranulePumpArgs::ObGranulePumpOpInfo::assign(const ObGranulePumpOpInfo &rhs) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_ops_.assign(rhs.scan_ops_))) {
    LOG_WARN("Failed to assign scan_ops_.", K(ret));
  } else {
    modify_op_ = rhs.modify_op_;
  }
  return ret;
}

}//sql
}//oceanbase
