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

namespace oceanbase
{
namespace share
{

// Unit statistic info
class ObUnitStat
{
public:
  ObUnitStat() : unit_id_(common::OB_INVALID_ID), required_size_(0), is_migrating_(false) {}
  ~ObUnitStat() {}
  int init(uint64_t unit_id, int64_t required_size, bool is_migrating) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_INVALID_ID == unit_id)) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "Invalid unit_id", KR(ret), K(unit_id), K(required_size), K(is_migrating));
    } else if (OB_UNLIKELY(required_size < 0)) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "required_size should not be negative", KR(ret), K(unit_id), K(required_size), K(is_migrating));
    } else {
      set(unit_id, required_size, is_migrating);
    }
    return ret;
  }
  void deep_copy(const ObUnitStat& other) {
    set(other.unit_id_, other.required_size_, other.is_migrating_);
  }
  void reset()
  {
    set(common::OB_INVALID_ID, 0/*required_size*/, false/*is_migrating*/);
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != unit_id_;
  }
  // get methods
  uint64_t get_unit_id() const {
    return unit_id_;
  }
  int64_t get_required_size() const {
    return required_size_;
  }
  bool get_is_migrating() const {
    return is_migrating_;
  }
  // ObReferedMap template method
  void set_key(const int64_t unit_id) { unit_id_ = unit_id; }
  const uint64_t &get_key() const { return unit_id_; }

  TO_STRING_KV(K_(unit_id), K_(required_size), K_(is_migrating));
private:
  void set(uint64_t unit_id, int64_t required_size, bool is_migrating) {
    unit_id_ = unit_id;
    required_size_ = required_size;
    is_migrating_ = is_migrating;
  }
  uint64_t unit_id_;
  int64_t required_size_;
  bool is_migrating_;
};

typedef common::hash::ObReferedMap<uint64_t, ObUnitStat> ObUnitStatMap;

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_UNIT_STAT_H_
