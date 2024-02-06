/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include "lib/random/ob_random.h"
#include "share/ob_ddl_sim_point.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::common;
using namespace oceanbase::share;


int64_t ObTenantDDLSimContext::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  const int64_t MAX_FIXED_POINT_COUNT = 64;
  ObDDLSimPointID fixed_points[MAX_FIXED_POINT_COUNT];
  int fixed_point_count = 0;
  for (int64_t i = 0; i < MAX_DDL_SIM_POINT_ID; ++i) {
    if (nullptr != fixed_points_ && fixed_points_[i] && i < MAX_FIXED_POINT_COUNT) {
      fixed_points[fixed_point_count++] = static_cast<ObDDLSimPointID>(i);
    }
  }
  J_KV(K(tenant_id_), K(type_), K(seed_), K(trigger_percent_),
      "fixed_points_", ObArrayWrap<ObDDLSimPointID>(fixed_points, fixed_point_count));
  J_OBJ_END();
  return pos;
}

ObDDLSimPointMgr &ObDDLSimPointMgr::get_instance()
{
  static ObDDLSimPointMgr instance;
  return instance;
}

ObDDLSimPointMgr::ObDDLSimPointMgr()
  : is_inited_(false), arena_("ddl_sim_pnt_mgr")
{
  memset(all_points_, 0, sizeof(all_points_));
}

int ObDDLSimPointMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ddl sim point mgr already inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(tenant_map_.create(207, "ddl_sim_tnt_map"))) {
    LOG_WARN("create tenant context map failed");
  } else if (OB_FAIL(task_sim_map_.create(199999, "ddl_sim_pnt_map"))) {
    LOG_WARN("create task sim map failed", K(ret));
  } else {
    // 1. remember sim action size
#define RET_ERR(args...) sizeof(ObDDLSimRetAction<ARGS_NUM(args)>)
#define N_RET_ERR(max_repeat_times, args...) sizeof(ObDDLSimRetAction<ARGS_NUM(args)>)
#define SLEEP_MS(args...) sizeof(ObDDLSimSleepAction)
#define N_SLEEP_MS(args...) sizeof(ObDDLSimSleepAction)
#define DDL_SIM_POINT_DEFINE(type, name, id, desc, action_size) all_points_[id].action_size_ = action_size;
#include "share/ob_ddl_sim_point_define.h"
#undef DDL_SIM_POINT_DEFINE
#undef RET_ERR
#undef N_RET_ERR
#undef SLEEP_MS
#undef N_SLEEP_MS

    // 2. constuct sim point
    void *buf = nullptr;
#define RET_ERR(ret_code, args...) new (buf) ObDDLSimRetAction<1 + ARGS_NUM(args)>(1, {ret_code, ##args})
#define N_RET_ERR(max_repeat_times, ret_code, args...) new (buf) ObDDLSimRetAction<1 + ARGS_NUM(args)>(max_repeat_times, {ret_code, ##args})
#define SLEEP_MS(min_time, max_time...) new (buf) ObDDLSimSleepAction(1, min_time, ##max_time)
#define N_SLEEP_MS(max_repeat_times, min_time, max_time...) new (buf) ObDDLSimSleepAction(max_repeat_times, min_time, ##max_time)
#define DDL_SIM_POINT_DEFINE(type, name, id, desc, action) \
    if (OB_SUCC(ret)) {\
      if (OB_ISNULL(buf = ob_malloc(all_points_[id].action_size_, "ddl_sim_act"))) {\
        ret = OB_ALLOCATE_MEMORY_FAILED;\
        LOG_WARN("allocate memory for ddl sim action failed", K(ret), K(all_points_[id].action_size_));\
      } else {\
        all_points_[id] = ObDDLSimPoint(name, type, #name, desc, action);\
      }\
    }
#include "share/ob_ddl_sim_point_define.h"
#undef DDL_SIM_POINT_DEFINE
#undef RET_ERR
#undef N_RET_ERR
#undef SLEEP_MS
#undef N_SLEEP_MS
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}


class TenantContextUpdater
{
public:
  TenantContextUpdater(const ObTenantDDLSimContext &tenant_context, const ObIArray<int64_t> &fixed_points_array)
    : new_context_(tenant_context), fixed_point_array_(fixed_points_array) {}
  ~TenantContextUpdater() = default;
  int operator() (hash::HashMapPair<uint64_t, ObTenantDDLSimContext> &entry) {
    int ret = OB_SUCCESS;
    if (new_context_.trigger_percent_ > 0 && 0 == entry.second.trigger_percent_) {
      entry.second.seed_ = new_context_.seed_;
      entry.second.trigger_percent_ = new_context_.trigger_percent_;
      entry.second.type_ = new_context_.type_;
    }
    const int64_t point_map_size = sizeof(bool) * MAX_DDL_SIM_POINT_ID;
    if (fixed_point_array_.count() > 0) {
      if (nullptr == entry.second.fixed_points_) {
        void *buf = ObDDLSimPointMgr::get_instance().get_arena_allocator().alloc(point_map_size);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(point_map_size));
        } else {
          bool *tmp_map = new bool[MAX_DDL_SIM_POINT_ID];
          memset(tmp_map, 0, point_map_size);
          entry.second.fixed_points_ = tmp_map;
        }
      }
    }
    if (nullptr != entry.second.fixed_points_) {
      memset(entry.second.fixed_points_, 0, point_map_size);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fixed_point_array_.count(); ++i) {
      const int64_t point_id = fixed_point_array_.at(i);
      entry.second.fixed_points_[point_id] = true;
    }
    LOG_INFO("update tenant param of ddl sim point success", K(new_context_), K(fixed_point_array_), K(entry.second));
    return OB_SUCCESS;
  }
public:
  const ObTenantDDLSimContext &new_context_;
  const ObIArray<int64_t> &fixed_point_array_;
};

int ObDDLSimPointMgr::set_tenant_param(const uint64_t tenant_id, const ObConfigIntListItem &rand_param, const ObConfigIntListItem &fixed_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<int64_t> fixed_point_array;
    ObTenantDDLSimContext tenant_context;
    tenant_context.tenant_id_ = tenant_id;
    // 1. fill random param if need
    if (rand_param.size() >= 2) {
      tenant_context.seed_ = rand_param[0];
      tenant_context.trigger_percent_ = rand_param[1];
      if (rand_param.size() >= 3) {
        tenant_context.type_ = static_cast<ObSimType>(rand_param[2]);
      }
    }
    // 2. try push tenant context into tenant map
    if (OB_FAIL(tenant_map_.set_refactored(tenant_id, tenant_context))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("set tenant context failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    // 3. fill fixed param if need
    for (int i = 0; OB_SUCC(ret) && i < fixed_param.size(); ++i) {
      const int64_t point_id = fixed_param[i];
      if (point_id <= MIN_DDL_SIM_POINT_ID || point_id >= MAX_DDL_SIM_POINT_ID) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(point_id));
        ret = OB_SUCCESS; // ignore invalid point id
      } else if (!all_points_[point_id].is_valid()) {
        // do nothing
      } else if (OB_FAIL(fixed_point_array.push_back(point_id))) {
        LOG_WARN("push back fixed point failed", K(ret), K(point_id), K(i));
      }
    }
    // 4. update tenant context
    if (OB_SUCC(ret)) {
      TenantContextUpdater updater(tenant_context, fixed_point_array);
      if (OB_FAIL(tenant_map_.atomic_refactored(tenant_id, updater))) {
        LOG_WARN("update tenant context failed", K(ret), K(tenant_id), K(tenant_context));
      }
    }
  }
  return ret;
}

int ObDDLSimPointMgr::generate_task_sim_map(const ObTenantDDLSimContext &tenant_context, const int64_t current_task_id, const std::initializer_list<ObDDLSimPointID> &point_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(tenant_context.trigger_percent_ <= 0)) {
    // skip
  } else {
    const uint64_t tenant_id = tenant_context.tenant_id_;
    const int64_t seed = tenant_context.seed_;
    const int64_t trigger_percent = tenant_context.trigger_percent_;
    for (std::initializer_list<ObDDLSimPointID>::iterator it = point_ids.begin(); OB_SUCC(ret) && it != point_ids.end(); ++it) {
      ObDDLSimPointID point_id = *it;
      const ObDDLSimPoint &cur_sim_point = all_points_[point_id];
      if (cur_sim_point.is_valid() && (SIM_TYPE_ALL == tenant_context.type_ || tenant_context.type_ == cur_sim_point.type_)) {
        srand(static_cast<int32_t>((tenant_id + seed) * current_task_id * point_id));
        if (rand() % 100 < trigger_percent) {
          if (OB_FAIL(task_sim_map_.set_refactored(TaskSimPoint(tenant_id, current_task_id, point_id), 0))) {
            if (OB_HASH_EXIST != ret) {
              LOG_WARN("set task sim point into map failed", K(ret), K(tenant_id), K(current_task_id), K(point_id));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }
  return ret;
}

class SimCountUpdater
{
public:
  explicit SimCountUpdater(int64_t step) : step_(step), old_trigger_count_(0) {}
  ~SimCountUpdater() = default;
  int operator() (hash::HashMapPair<ObDDLSimPointMgr::TaskSimPoint, int64_t> &entry) {
    old_trigger_count_ = entry.second;
    entry.second += step_;
    return OB_SUCCESS;
  }
public:
  int64_t step_;
  int64_t old_trigger_count_;
};

int ObDDLSimPointMgr::try_sim(const uint64_t tenant_id, const uint64_t task_id, const std::initializer_list<ObDDLSimPointID> &point_ids)
{
  int ret = OB_SUCCESS;
  ObTenantDDLSimContext tenant_context;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || task_id < 0)) {
    LOG_INFO("invalid argument for ddl errsim, ignore", K(tenant_id), K(task_id));
  } else if (0 == task_id) {
    // task_id maybe not set, skip
  } else if (OB_FAIL(tenant_map_.get_refactored(tenant_id, tenant_context))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get tenant context failed", K(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(generate_task_sim_map(tenant_context, task_id, point_ids))) {
    LOG_WARN("generate task sim map failed", K(ret), K(tenant_context), K(task_id));
  } else {
    for (std::initializer_list<ObDDLSimPointID>::iterator it = point_ids.begin(); OB_SUCC(ret) && it != point_ids.end(); ++it) {
      ObDDLSimPointID point_id = *it;
      bool need_execute = false;
      TaskSimPoint sim_key(tenant_id, task_id, point_id);
      if (point_id <= MIN_DDL_SIM_POINT_ID || point_id > MAX_DDL_SIM_POINT_ID || !all_points_[point_id].is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid point id", K(ret), K(point_id));
      } else if (nullptr != tenant_context.fixed_points_ && tenant_context.fixed_points_[point_id]) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(task_sim_map_.set_refactored(TaskSimPoint(tenant_id, task_id, point_id), 0))) {
          if (OB_HASH_EXIST != tmp_ret) {
            LOG_WARN("set fixed point into task sim map failed", K(tmp_ret), K(tenant_id), K(task_id), K(point_id));
          }
        }
      }
      if (OB_SUCC(ret)) {
        SimCountUpdater inc(1);
        if (OB_FAIL(task_sim_map_.atomic_refactored(sim_key, inc))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("check task need sim ddl point failed", K(ret), K(sim_key));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          need_execute = inc.old_trigger_count_ < all_points_[sim_key.point_id_].action_->max_repeat_times_;
          if (need_execute) {
            ret = all_points_[point_id].action_->execute();
            LOG_INFO("ddl sim point executed", K(ret), K(sim_key));
          } else {
            SimCountUpdater dec(-1);
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(task_sim_map_.atomic_refactored(sim_key, dec))) {
              LOG_WARN("decrease trigger count failed", K(tmp_ret), K(sim_key));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLSimPointMgr::get_sim_point(const int64_t idx, ObDDLSimPoint &sim_point) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(idx < MIN_DDL_SIM_POINT_ID || idx >= MAX_DDL_SIM_POINT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx));
  } else {
    sim_point = all_points_[idx];
  }
  return ret;
}

class SimCountCollector
{
public:
  SimCountCollector(ObIArray<ObDDLSimPointMgr::TaskSimPoint> &task_sim_points, ObIArray<int64_t> &sim_counts)
    : task_sim_points_(task_sim_points), sim_counts_(sim_counts) {}
  ~SimCountCollector() = default;
  int operator() (hash::HashMapPair<ObDDLSimPointMgr::TaskSimPoint, int64_t> &entry) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(task_sim_points_.push_back(entry.first))) {
      LOG_WARN("push back task sim point failed", K(ret), K(entry.first));
    } else if (OB_FAIL(sim_counts_.push_back(entry.second))) {
      LOG_WARN("push back sim count failed", K(ret), K(entry.second));
    }
    return ret;
  }
public:
  ObIArray<ObDDLSimPointMgr::TaskSimPoint> &task_sim_points_;
  ObIArray<int64_t> &sim_counts_;
};

int ObDDLSimPointMgr::get_sim_stat(ObIArray<TaskSimPoint> &task_sim_points, ObIArray<int64_t> &sim_counts)
{
  int ret = OB_SUCCESS;
  task_sim_points.reset();
  sim_counts.reset();
  int64_t entry_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    entry_count = task_sim_map_.size();
  }
  if (OB_SUCC(ret) && entry_count > 0) {
    const int64_t reserve_count = entry_count + 1000L;
    if (OB_FAIL(task_sim_points.reserve(reserve_count))) {
      LOG_WARN("reserve array capacity failed", K(ret), K(entry_count), K(reserve_count));
    } else if (OB_FAIL(sim_counts.reserve(reserve_count))) {
      LOG_WARN("reserve array capacity failed", K(ret), K(entry_count), K(reserve_count));
    }
  }
  if (OB_SUCC(ret) && entry_count > 0) {
    SimCountCollector stat_collector(task_sim_points, sim_counts);
    if (OB_FAIL(task_sim_map_.foreach_refactored(stat_collector))) {
      LOG_WARN("collect ddl sim entry failed", K(ret));
    }
  }
  return ret;
}
