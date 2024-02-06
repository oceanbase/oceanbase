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

#ifndef OCEANBASE_SHARE_OB_DDL_SIM_POINT_H
#define OCEANBASE_SHARE_OB_DDL_SIM_POINT_H

#include<initializer_list>
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/config/ob_config.h"

namespace oceanbase
{
namespace share
{

enum ObSimType
{
  SIM_TYPE_ALL = 0,
  SIM_TYPE_DDL = 1,
  SIM_TYPE_TRANSFER = 2,
};

struct ObDDLSimAction
{
public:
  explicit ObDDLSimAction(const int64_t max_repeat_times) : max_repeat_times_(max_repeat_times) {}
  virtual int execute() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
public:
  int64_t max_repeat_times_;
};

template<int64_t COUNT>
struct ObDDLSimRetAction : public ObDDLSimAction
{
public:
  ObDDLSimRetAction(const int64_t max_repeat_times, const std::initializer_list<int> &ret_codes)
  : ObDDLSimAction(max_repeat_times) {
    std::initializer_list<int>::iterator it = ret_codes.begin();
    for (int64_t i = 0; i < COUNT && it != ret_codes.end(); ++i, ++it) {
      ret_codes_[i] = *it;
    }
  }
  virtual int execute() override {
    int64_t i = ObRandom::rand(0, COUNT - 1);
    return ret_codes_[i];
  }
  VIRTUAL_TO_STRING_KV("repeat_times", max_repeat_times_, "ret_codes", ObArrayWrap<int>(ret_codes_, COUNT));
public:
  int ret_codes_[COUNT];
};

struct ObDDLSimSleepAction : public ObDDLSimAction
{
public:
  ObDDLSimSleepAction(const int64_t max_repeat_times, const int min_sleep_ms, const int max_sleep_ms = 0)
    : ObDDLSimAction(max_repeat_times), min_sleep_ms_(min_sleep_ms), max_sleep_ms_(max(min_sleep_ms, max_sleep_ms)) {}
  virtual int execute() override {
    int64_t sleep_ms = ObRandom::rand(min_sleep_ms_, max_sleep_ms_);
    ob_usleep(static_cast<useconds_t>(sleep_ms * 1000L));
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV("repeat_times", max_repeat_times_, K_(min_sleep_ms), K_(max_sleep_ms));
public:
  int min_sleep_ms_;
  int max_sleep_ms_;
};

enum ObDDLSimPointID // check unique id by compiler
{
  MIN_DDL_SIM_POINT_ID = 0,
#define DDL_SIM_POINT_DEFINE(type, name, id, desc, action) name = id,
#include "share/ob_ddl_sim_point_define.h"
  MAX_DDL_SIM_POINT_ID
};
#undef DDL_SIM_POINT_DEFINE

struct ObDDLSimPoint
{
public:
  ObDDLSimPoint()
    : id_(MIN_DDL_SIM_POINT_ID), type_(SIM_TYPE_ALL), name_(nullptr), desc_(nullptr), action_(nullptr) {}
  ObDDLSimPoint(const ObDDLSimPointID id, const ObSimType type, const char *name, const char *desc, ObDDLSimAction *action)
    : id_(id), type_(type), name_(name), desc_(desc), action_(action) {}
  bool is_valid() const { return id_ > MIN_DDL_SIM_POINT_ID && id_ < MAX_DDL_SIM_POINT_ID && nullptr != name_ && nullptr != desc_ && nullptr != action_; }
  TO_STRING_KV(K(id_), K_(type), K(name_), K(desc_), KP(action_), K(action_size_));

public:
  ObDDLSimPointID id_;
  ObSimType type_;
  const char *name_;
  const char *desc_;
  union {
    ObDDLSimAction *action_;
    int64_t action_size_;
  };
};

struct ObTenantDDLSimContext
{
public:
  ObTenantDDLSimContext() : tenant_id_(0), type_(SIM_TYPE_ALL), seed_(0), trigger_percent_(0), fixed_points_(nullptr) {}
  DECLARE_TO_STRING;
public:
  uint64_t tenant_id_;
  ObSimType type_;
  int64_t seed_;
  int64_t trigger_percent_;
  bool *fixed_points_;
};

class ObDDLSimPointMgr
{
public:
  struct TaskSimPoint
  {
  public:
    TaskSimPoint(const uint64_t tenant_id = 0, const uint64_t task_id = 0, const ObDDLSimPointID point_id = MIN_DDL_SIM_POINT_ID)
      : tenant_id_(tenant_id), task_id_(task_id), point_id_(point_id) {}
    int hash(uint64_t &hash_val) const
    {
      hash_val = 0;
      hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = murmurhash(&task_id_, sizeof(task_id_), hash_val);
      hash_val = murmurhash(&point_id_, sizeof(point_id_), hash_val);
      return OB_SUCCESS;
    }
    bool operator ==(const TaskSimPoint &other) const
    {
      return tenant_id_ == other.tenant_id_ && task_id_ == other.task_id_ && point_id_ == other.point_id_;
    }
    TO_STRING_KV(K(tenant_id_), K(task_id_), K(point_id_));
  public:
    uint64_t tenant_id_;
    uint64_t task_id_;
    ObDDLSimPointID point_id_;
  };
public:
  static ObDDLSimPointMgr &get_instance();
  int init();
  int set_tenant_param(const uint64_t tenant_id, const ObConfigIntListItem &rand_param, const ObConfigIntListItem &fixed_param);
  int try_sim(const uint64_t tenant_id, const uint64_t task_id, const std::initializer_list<ObDDLSimPointID> &point_ids);
  int get_sim_point(const int64_t idx, ObDDLSimPoint &sim_point) const;
  int get_sim_stat(ObIArray<TaskSimPoint> &task_sim_points, ObIArray<int64_t> &sim_counts);
  ObIAllocator &get_arena_allocator() { return arena_; }
  TO_STRING_KV(K(is_inited_), K(task_sim_map_.size()));
private:
  int generate_task_sim_map(const ObTenantDDLSimContext &tenant_context, const int64_t current_task_id, const std::initializer_list<ObDDLSimPointID> &point_ids);
private:
  ObDDLSimPointMgr();
  DISABLE_COPY_ASSIGN(ObDDLSimPointMgr);

private:
  bool is_inited_;
  ObDDLSimPoint all_points_[MAX_DDL_SIM_POINT_ID];
  hash::ObHashMap<TaskSimPoint, int64_t> task_sim_map_;
  hash::ObHashMap<uint64_t, ObTenantDDLSimContext> tenant_map_;
  ObArenaAllocator arena_;
};

#ifdef ERRSIM
#define DDL_SIM(tenant_id, task_id, sim_point, args...) ::oceanbase::share::ObDDLSimPointMgr::get_instance().try_sim(tenant_id, task_id, {sim_point, ##args})
#define DDL_SIM_WHEN(condition, tenant_id, task_id, sim_point, args...) (condition) ? DDL_SIM(tenant_id, task_id, sim_point, args) : OB_SUCCESS
#else
#define DDL_SIM(...) OB_SUCCESS
#define DDL_SIM_WHEN(...) OB_SUCCESS
#endif




} // namespace share
} // namespace oceanbase

#endif//OCEANBASE_SHARE_OB_DDL_SIM_POINT_H
