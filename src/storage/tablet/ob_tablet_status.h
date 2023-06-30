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

#ifndef OCEANBASE_STORAGE_OB_TABLET_STATUS
#define OCEANBASE_STORAGE_OB_TABLET_STATUS

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
class ObTabletStatus
{
public:
  enum Status : uint8_t
  {
    CREATING = 0, // deprecated after 4.1
    NORMAL   = 1,
    DELETING = 2, // deprecated after 4.1
    DELETED  = 3,
    TRANSFER_OUT = 4,
    TRANSFER_IN  = 5,
    TRANSFER_OUT_DELETED = 6,
    MAX,
  };
public:
  ObTabletStatus();
  ~ObTabletStatus() = default;
  explicit ObTabletStatus(const Status &status);
public:
  ObTabletStatus &operator=(const Status &status);
  bool operator==(const Status &status);
  bool operator!=(const Status &status);
  operator Status() const;

  bool is_valid() const;
  static const char *get_str(const ObTabletStatus &status);
  Status &get_status() { return status_; }
  const Status &get_status() const { return status_; }

  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV("val", static_cast<uint8_t>(status_),
               "str", get_str(*this));
private:
  static constexpr const char *status_str_array_[] = {
    "CREATING",
    "NORMAL",
    "DELETING",
    "DELETED",
    "TRANSFER_OUT",
    "TRANSFER_IN",
    "TRANSFER_OUT_DELETED",
    "MAX"
  };

  Status status_;
};

inline ObTabletStatus &ObTabletStatus::operator=(const Status &status)
{
  status_ = status;
  return *this;
}

inline bool ObTabletStatus::operator==(const Status &status)
{
  return status_ == status;
}

inline bool ObTabletStatus::operator!=(const Status &status)
{
  return status_ != status;
}

inline ObTabletStatus::operator Status() const
{
  return status_;
}

inline bool ObTabletStatus::is_valid() const
{
  return status_ < Status::MAX;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_STATUS
