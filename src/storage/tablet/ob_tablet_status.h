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
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
namespace common
{
class ObThreadCond;
}

namespace transaction
{
class ObTransID;
}
namespace share
{
class SCN;
}

namespace storage
{
class ObTablet;
class ObTabletTxMultiSourceDataUnit;

class ObTabletStatus
{
public:
  enum Status : uint8_t
  {
    CREATING = 0,
    NORMAL,
    DELETING,
    DELETED,
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
  Status &get_status() { return status_; }
  const Status &get_status() const { return status_; }

  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;

  static bool is_valid_status(const Status current_status, const Status target_status);

  TO_STRING_KV(K_(status));
private:
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

class ObTabletStatusChecker
{
public:
  ObTabletStatusChecker(ObTablet &tablet);
  ~ObTabletStatusChecker() = default;
  ObTabletStatusChecker(const ObTabletStatusChecker&) = delete;
  ObTabletStatusChecker &operator=(const ObTabletStatusChecker&) = delete;
public:
  int check(const uint64_t time_us);
  int wake_up(
      ObTabletTxMultiSourceDataUnit &tx_data,
      const share::SCN &memtable_scn,
      const bool for_replay,
      const memtable::MemtableRefOp ref_op);
private:
  int do_wait(common::ObThreadCond &cond, const uint64_t time_ms);
  static bool is_final_status(const ObTabletStatus::Status &status);
private:
  ObTablet &tablet_;
};

inline bool ObTabletStatusChecker::is_final_status(const ObTabletStatus::Status &status)
{
  return ObTabletStatus::NORMAL == status || ObTabletStatus::DELETED == status;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_STATUS
