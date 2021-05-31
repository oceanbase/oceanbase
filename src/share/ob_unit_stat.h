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

#ifndef OCEANBASE_SHARE_OB_UNIT_STAT_H_
#define OCEANBASE_SHARE_OB_UNIT_STAT_H_

#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_refered_map.h"

namespace oceanbase {
namespace share {

struct ObUnitStat {
  ObUnitStat() : unit_id_(common::OB_INVALID_ID), required_size_(0), partition_cnt_(0)
  {}
  ~ObUnitStat()
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != unit_id_;
  }
  void reset()
  {
    unit_id_ = common::OB_INVALID_ID;
    required_size_ = 0;
    partition_cnt_ = 0;
  }
  // ObReferedMap template method
  void set_key(const int64_t unit_id)
  {
    unit_id_ = unit_id;
  }
  const uint64_t& get_key() const
  {
    return unit_id_;
  }

  TO_STRING_KV(K_(unit_id), K_(required_size), K_(partition_cnt));

  uint64_t unit_id_;
  int64_t required_size_;
  int64_t partition_cnt_;
};

typedef common::hash::ObReferedMap<uint64_t, ObUnitStat> ObUnitStatMap;

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_UNIT_STAT_H_
