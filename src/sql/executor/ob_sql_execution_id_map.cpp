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

#include "ob_sql_execution_id_map.h"

namespace oceanbase {
namespace sql {

using namespace common;

struct OuterMapOp {
public:
  OuterMapOp(const FetchMod fetch_mod) : scheduler_(NULL), fetch_mod_(fetch_mod)
  {}

  void operator()(common::hash::HashMapPair<uint64_t, std::pair<ObDistributedScheduler*, common::SpinRWLock*>>& it)
  {
    if (NULL != it.second.first && NULL != it.second.second) {
      if (FM_SHARED == fetch_mod_) {
        if (it.second.second->try_rdlock()) {
          scheduler_ = it.second.first;
        }
      } else if (FM_MUTEX_BLOCK == fetch_mod_) {
        (void)it.second.second->wrlock();
        scheduler_ = it.second.first;
      } else if (FM_MUTEX_NONBLOCK == fetch_mod_) {
        if (it.second.second->try_wrlock()) {
          scheduler_ = it.second.first;
        }
      }
    }
  }

  ObDistributedScheduler* scheduler_;
  FetchMod fetch_mod_;
};

int ObSqlExecutionIDMap::init(int64_t num)
{
  int ret = OB_SUCCESS;
  if (num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(inner_id_map_.init(num))) {
    LOG_WARN("init id map failed", K(ret), K(num));
  } else if (OB_FAIL(
                 outer_id_map_.create(hash::cal_next_prime(num), ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    LOG_WARN("create hash map failed", K(ret), K(num));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObSqlExecutionIDMap::destroy()
{
  inner_id_map_.destroy();
  if (outer_id_map_.created()) {
    int tmp_ret = outer_id_map_.destroy();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("destroy hash map failed", K(tmp_ret));
    }
  }
  inited_ = false;
}

int ObSqlExecutionIDMap::assign(ObDistributedScheduler* value, uint64_t& id)
{
  return inner_id_map_.assign(value, id);
}

int ObSqlExecutionIDMap::assign_external_id(const uint64_t id, ObDistributedScheduler* value)
{
  int ret = OB_SUCCESS;
  SpinRWLock* lock = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_outer_id(id) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id), KP(value));
  } else if (OB_ISNULL(lock = OB_NEW(SpinRWLock, ObModIds::OB_SQL_EXECUTOR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(outer_id_map_.set_refactored(id, std::make_pair(value, lock)))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ENTRY_EXIST;
      }
      LOG_WARN("add id to map failed", K(ret));
      OB_DELETE(SpinRWLock, ObModIds::OB_SQL_EXECUTOR, lock);
    }
  }
  return ret;
}

ObDistributedScheduler* ObSqlExecutionIDMap::fetch(const uint64_t id, const FetchMod mod)
{
  ObDistributedScheduler* scheduler = NULL;
  if (!is_outer_id(id)) {
    scheduler = inner_id_map_.fetch(id, mod);
  } else {
    OuterMapOp op(mod);
    if (OB_SUCCESS == outer_id_map_.read_atomic(id, op)) {
      scheduler = op.scheduler_;
    }
  }
  return scheduler;
}

void ObSqlExecutionIDMap::revert(const uint64_t id, bool erase /* = false */)
{
  if (!is_outer_id(id)) {
    inner_id_map_.revert(id, erase);
  } else {
    auto v = outer_id_map_.get(id);
    if (NULL != v) {
      auto lock = v->second;
      if (erase) {
        outer_id_map_.erase_refactored(id);
      }
      lock->unlock();
      if (erase) {
        OB_DELETE(SpinRWLock, ObModIds::OB_SQL_EXECUTOR, lock);
      }
    }
  }
}

}  // namespace sql
}  // namespace oceanbase
