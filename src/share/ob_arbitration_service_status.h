/*
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

#ifndef OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_STATUS_H_
#define OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_STATUS_H_

#include "lib/utility/ob_unify_serialize.h" // for OB_UNIS_VERSION
#include "lib/string/ob_string.h"           // for ObString

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceStatus
{
  OB_UNIS_VERSION(1);
public:
  enum ArbitrationServiceStatus
  {
    INVALID = -1,
    ENABLING,
    ENABLED,
    DISABLING,
    DISABLED,
    MAX
  };
public:
  ObArbitrationServiceStatus() : status_(INVALID) {}
  explicit ObArbitrationServiceStatus(ArbitrationServiceStatus status) : status_(status) {}

  ObArbitrationServiceStatus &operator=(const ArbitrationServiceStatus status) { status_ = status; return *this; }
  ObArbitrationServiceStatus &operator=(const ObArbitrationServiceStatus &other) { status_ = other.status_; return *this; }
  void reset() { status_ = INVALID; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void assign(const ObArbitrationServiceStatus &other) { status_ = other.status_; }
  bool operator==(const ObArbitrationServiceStatus &other) const { return other.status_ == status_; }
  bool operator!=(const ObArbitrationServiceStatus &other) const { return other.status_ != status_; }
  bool is_valid() const { return INVALID < status_ && MAX > status_; }
  bool is_enabling() const { return ENABLING == status_; }
  bool is_enabled() const { return ENABLED == status_; }
  bool is_enable_like() const { return ENABLING == status_ || ENABLED == status_; }
  bool is_disable_like() const { return DISABLING == status_ || DISABLED == status_; }
  bool is_disabling() const { return DISABLING == status_; }
  bool is_disabled() const { return DISABLED == status_; }
  int parse_from_string(const ObString &status);
  const ArbitrationServiceStatus &get_status() const { return status_; }
  const char* get_status_str() const;
private:
  ArbitrationServiceStatus status_;
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_STATUS_H_
