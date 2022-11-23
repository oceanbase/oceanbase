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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_UTILS_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_UTILS_H
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

class ObLCLLabel
{
  OB_UNIS_VERSION(1);
public:
  ObLCLLabel() : id_(INVALID_VALUE), priority_(INVALID_VALUE) {}
  ObLCLLabel(const uint64_t id,
             const ObDetectorPriority &priority);
  ObLCLLabel(const ObLCLLabel &rhs);
  ~ObLCLLabel() = default;
  bool is_valid() const;
  ObLCLLabel &operator=(const ObLCLLabel &rhs);
  bool operator==(const ObLCLLabel &rhs) const;
  bool operator<(const ObLCLLabel &rhs) const;
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_id() const { return id_; }
  const ObDetectorPriority &get_priority() const { return priority_; }
  TO_STRING_KV(K_(addr), K_(id), K_(priority));
private:
  common::ObAddr addr_;
  uint64_t id_;
  ObDetectorPriority priority_;
};

}
}
}
#endif