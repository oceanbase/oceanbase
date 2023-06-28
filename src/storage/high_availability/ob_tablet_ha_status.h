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

#ifndef OCEABASE_STORAGE_TABLET_HA_STATUS_
#define OCEABASE_STORAGE_TABLET_HA_STATUS_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{

// status for tablet restore
class ObTabletRestoreStatus final
{
public:
  enum STATUS : uint8_t
  {
    FULL = 0,                   // both minor and major data are complete
    EMPTY = 1,                  // both minor and major data are empty, wait restore
    MINOR_AND_MAJOR_META = 2,   // complete minor data with only major sst meta
    PENDING = 3,                // not sure if it is a valid tablet, need to confirm with backup media
    UNDEFINED = 4,              // invalid tablet, a placeholder
    RESTORE_STATUS_MAX
  };
public:
  ObTabletRestoreStatus() = default;
  ~ObTabletRestoreStatus() = default;
public:
  static bool is_valid(const ObTabletRestoreStatus::STATUS &status);
  static bool is_full(const ObTabletRestoreStatus::STATUS &status) { return STATUS::FULL == status; }
  static bool is_empty(const ObTabletRestoreStatus::STATUS &status) { return STATUS::EMPTY == status; }
  static bool is_minor_and_major_meta(const ObTabletRestoreStatus::STATUS &status) { return STATUS::MINOR_AND_MAJOR_META == status; }
  static bool is_pending(const ObTabletRestoreStatus::STATUS &status) { return STATUS::PENDING == status; }
  static bool is_undefined(const ObTabletRestoreStatus::STATUS &status) { return STATUS::UNDEFINED == status; }
  static int check_can_change_status(
      const ObTabletRestoreStatus::STATUS &cur_status,
      const ObTabletRestoreStatus::STATUS &change_status,
      bool &can_change);
};

class ObTabletDataStatus final
{
public:
  enum STATUS : uint8_t
  {
    COMPLETE = 0,                   // minor and major data are complete
    INCOMPLETE = 1,                 // minor or major data is incomplete
    DATA_STATUS_MAX
  };
public:
  ObTabletDataStatus() = default;
  ~ObTabletDataStatus() = default;

  static bool is_valid(const ObTabletDataStatus::STATUS &status);
  static bool is_complete(const ObTabletDataStatus::STATUS &status) { return STATUS::COMPLETE == status; }
  static bool is_incomplete(const ObTabletDataStatus::STATUS &status) { return STATUS::INCOMPLETE == status; }
  static int check_can_change_status(
      const ObTabletDataStatus::STATUS &cur_status,
      const ObTabletDataStatus::STATUS &change_status,
      bool &can_change);
};

class ObTabletExpectedStatus final
{
public:
  enum STATUS : uint8_t
  {
    NORMAL = 0,
    DELETED = 1,
    EXPECTED_STATUS_MAX
  };
public:
  ObTabletExpectedStatus() = default;
  ~ObTabletExpectedStatus() = default;
  static bool is_valid(const ObTabletExpectedStatus::STATUS &status);
  static bool is_normal(const ObTabletExpectedStatus::STATUS &status) { return STATUS::NORMAL == status; }
  static bool is_deleted(const ObTabletExpectedStatus::STATUS &status) { return STATUS::DELETED == status; }
  static int check_can_change_status(
      const ObTabletExpectedStatus::STATUS &cur_status,
      const ObTabletExpectedStatus::STATUS &change_status,
      bool &can_change);
};

class ObTabletHAStatus final
{
public:
  ObTabletHAStatus();
  ~ObTabletHAStatus() = default;
  bool is_valid() const;
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
  void reset();
  bool is_none() const { return is_data_status_complete() && is_restore_status_full(); }
  bool is_data_status_complete() const { return ObTabletDataStatus::is_complete(data_status_); }
  bool is_restore_status_full() const { return ObTabletRestoreStatus::is_full(restore_status_); }
  bool is_restore_status_pending() const { return ObTabletRestoreStatus::is_pending(restore_status_); }
  bool is_restore_status_undefined() const { return ObTabletRestoreStatus::is_undefined(restore_status_); }
  bool is_restore_status_empty() const { return ObTabletRestoreStatus::is_empty(restore_status_); }
  bool is_restore_status_minor_and_major_meta() const { return ObTabletRestoreStatus::is_minor_and_major_meta(restore_status_); }
  bool is_expected_status_normal() const { return ObTabletExpectedStatus::is_normal(expected_status_); }
  bool is_expected_status_deleted() const { return ObTabletExpectedStatus::is_deleted(expected_status_); }

  int set_restore_status(const ObTabletRestoreStatus::STATUS &restore_status);
  int get_restore_status(ObTabletRestoreStatus::STATUS &restore_status) const;
  int set_data_status(const ObTabletDataStatus::STATUS &data_status);
  int get_data_status(ObTabletDataStatus::STATUS &data_status) const;
  int set_expected_status(const ObTabletExpectedStatus::STATUS &expected_status);
  int get_expected_status(ObTabletExpectedStatus::STATUS &expected_status) const;
  int init_status();
  int init_status_for_ha(const ObTabletHAStatus &ha_status);
  bool is_valid_for_backup() const { return ObTabletDataStatus::is_complete(data_status_) && ObTabletRestoreStatus::is_full(restore_status_); }

  TO_STRING_KV(K_(restore_status), K_(data_status), K_(expected_status), K_(reserved));
public:
  static const uint64_t SF_BIT_RESTORE_STATUS = 8;
  static const uint64_t SF_BIT_DATA_STATUS = 8;
  static const uint64_t SF_BIT_EXPECTED_STATUS = 8;
  static const uint64_t SF_BIT_RESERVED = 40;
private:
  union {
    int64_t ha_status_;
    struct {
      ObTabletRestoreStatus::STATUS restore_status_         : SF_BIT_RESTORE_STATUS;
      ObTabletDataStatus::STATUS data_status_               : SF_BIT_DATA_STATUS;
      ObTabletExpectedStatus::STATUS expected_status_       : SF_BIT_EXPECTED_STATUS;
      uint64_t reserved_                                    : SF_BIT_RESERVED;
    };
  };
};



}
}

#endif
