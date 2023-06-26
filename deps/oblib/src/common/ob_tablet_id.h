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

#ifndef OCEANBASE_COMMON_OB_TABLET_ID_H_
#define OCEANBASE_COMMON_OB_TABLET_ID_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/rowid/ob_urowid.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase
{
namespace common
{

class ObTabletID
{
public:
  static const uint64_t MIN_USER_TABLET_ID = OB_MAX_INNER_TABLE_ID;
  static const uint64_t INVALID_TABLET_ID = 0;
  static const uint64_t MIN_VALID_TABLET_ID = 1;

  static const uint64_t MIN_USER_NORMAL_ROWID_TABLE_TABLET_ID = MIN_USER_TABLET_ID;
  static const uint64_t MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID = ((uint64_t)1 << 37) - 1;
  static const uint64_t MIN_USER_EXTENDED_ROWID_TABLE_TABLET_ID = ((uint64_t)1 << 60);
  static const uint64_t MAX_USER_EXTENDED_ROWID_TABLE_TABLET_ID = ((uint64_t)1 << 61) - 1;

  // for LS inner tablet
  static const uint64_t MIN_LS_INNER_TABLET_ID = OB_MIN_LS_INNER_TABLE_ID;
  static const uint64_t LS_TX_CTX_TABLET_ID    = MIN_LS_INNER_TABLET_ID + 1;
  static const uint64_t LS_TX_DATA_TABLET_ID   = MIN_LS_INNER_TABLET_ID + 2;
  static const uint64_t LS_LOCK_TABLET_ID      = MIN_LS_INNER_TABLET_ID + 3;
  static const uint64_t MAX_LS_INNER_TABLET_ID = OB_MAX_LS_INNER_TABLE_ID;

public:
  explicit ObTabletID(const uint64_t id = INVALID_TABLET_ID) : id_(id) {}
  ObTabletID(const ObTabletID &other) : id_(other.id_) {}
  ~ObTabletID() { reset(); }

public:
  uint64_t id() const { return id_; }
  void reset() { id_ = INVALID_TABLET_ID; }

  // assignment
  ObTabletID &operator=(const uint64_t id) { id_ = id; return *this; }
  ObTabletID &operator=(const ObTabletID &other) { id_ = other.id_; return *this; }

  // tablet attribute interface
  bool is_valid() const { return id_ != INVALID_TABLET_ID; }
  bool is_sys_tablet() const { return (is_valid() && is_sys_table(id_)); }
  bool is_inner_tablet() const { return (is_valid() && is_inner_table(id_)); }
  bool is_reserved_tablet() const { return is_inner_tablet() && !is_sys_tablet(); }
  bool is_ls_inner_tablet() const { return (id_ > MIN_LS_INNER_TABLET_ID && id_ < MAX_LS_INNER_TABLET_ID); }
  bool is_ls_tx_data_tablet() const { return (LS_TX_DATA_TABLET_ID == id_); }
  bool is_ls_tx_ctx_tablet() const { return (LS_TX_CTX_TABLET_ID == id_); }
  bool is_ls_lock_tablet() const { return (LS_LOCK_TABLET_ID == id_); }
  // interface for compaction schedule
  bool is_only_mini_merge_tablet() const
  { // only do mini merge
    return is_ls_tx_ctx_tablet() || is_ls_lock_tablet();
  }
  bool is_mini_and_minor_merge_tablet() const
  { // only do mini merge and minor merge
    return is_ls_tx_data_tablet();
  }
  bool is_special_merge_tablet() const
  {
    return is_mini_and_minor_merge_tablet() || is_only_mini_merge_tablet();
  }

  bool is_valid_with_tenant(const uint64_t tenant_id) const
  {
    // 1. Meta tenant only has inner tablet, no user tablet
    // 2. User tenant and SYS tenant support all valid tablet
    return (is_meta_tenant(tenant_id) && is_inner_tablet())
        || ((is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) && is_valid());
  }

  bool is_user_normal_rowid_table_tablet() const
  {
    return MIN_USER_NORMAL_ROWID_TABLE_TABLET_ID < id_
        && id_ < MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID;
  }

  bool is_user_extended_rowid_table_tablet() const {
    return MIN_USER_EXTENDED_ROWID_TABLE_TABLET_ID < id_
        && id_ < MAX_USER_EXTENDED_ROWID_TABLE_TABLET_ID;
  }

  // compare operator
  bool operator==(const ObTabletID &other) const { return id_ == other.id_; }
  bool operator!=(const ObTabletID &other) const { return id_ != other.id_; }
  bool operator< (const ObTabletID &other) const { return id_ <  other.id_; }
  bool operator<=(const ObTabletID &other) const { return id_ <= other.id_; }
  bool operator> (const ObTabletID &other) const { return id_ >  other.id_; }
  bool operator>=(const ObTabletID &other) const { return id_ >= other.id_; }
  int compare(const ObTabletID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }

  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));
private:
  uint64_t id_;
};

// static constants for ObTabletID
static const ObTabletID LS_TX_CTX_TABLET(ObTabletID::LS_TX_CTX_TABLET_ID);
static const ObTabletID LS_TX_DATA_TABLET(ObTabletID::LS_TX_DATA_TABLET_ID);
static const ObTabletID LS_LOCK_TABLET(ObTabletID::LS_LOCK_TABLET_ID);

static const int64_t OB_DEFAULT_TABLET_ID_COUNT = 3;
typedef ObSEArray<ObTabletID, OB_DEFAULT_TABLET_ID_COUNT> ObTabletIDArray;
typedef ObFixedArray<ObTabletID, ObIAllocator> ObTabletIDFixedArray;

} // end namespace common
} // end namespace oceanbase
#endif /* OCEANBASE_COMMON_OB_TABLET_ID_H_ */
