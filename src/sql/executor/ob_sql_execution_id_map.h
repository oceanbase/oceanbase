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

#ifndef OCEANBASE_EXECUTOR_OB_SQL_EXECUTION_ID_MAP_H_
#define OCEANBASE_EXECUTOR_OB_SQL_EXECUTION_ID_MAP_H_

#include "lib/container/ob_id_map.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace sql {

class ObDistributedScheduler;

class ObSqlExecutionIDMap {
public:
  static const uint64_t OUTER_ID_BASE = UINT64_MAX / 4;
  static const int64_t EXECUTION_ID_TSI_HOLD_NUM = 4096;

  ObSqlExecutionIDMap() : inited_(false)
  {}
  ~ObSqlExecutionIDMap()
  {}

  int init(int64_t num);
  bool is_inited() const
  {
    return inited_;
  }
  void destroy();

  static bool is_outer_id(const uint64_t id)
  {
    return common::OB_INVALID_ID != id && id >= OUTER_ID_BASE;
  }

  int assign(ObDistributedScheduler* value, uint64_t& id);
  int assign_external_id(const uint64_t id, ObDistributedScheduler* value);

  ObDistributedScheduler* fetch(const uint64_t id, const common::FetchMod mod = common::FM_SHARED);
  void revert(const uint64_t id, bool erase = false);

  template <typename CB>
  void traverse(CB& cb) const
  {
    inner_id_map_.traverse(cb);
    FOREACH(iter, outer_id_map_)
    {
      cb(iter->first);
    }
  }

private:
  bool inited_;
  common::ObIDMap<ObDistributedScheduler, uint64_t, EXECUTION_ID_TSI_HOLD_NUM> inner_id_map_;
  common::hash::ObHashMap<uint64_t, std::pair<ObDistributedScheduler*, common::SpinRWLock*> > outer_id_map_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_EXECUTOR_OB_SQL_EXECUTION_ID_MAP_H_
