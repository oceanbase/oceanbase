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

#ifndef OCEANBASE_SHARE_OB_LS_ID
#define OCEANBASE_SHARE_OB_LS_ID
#include <stdint.h>
#include "lib/ob_define.h"                  // is_sys_tenant/is_meta_tenant/is_user_tenant
#include "lib/container/ob_se_array.h"      // ObSEArray
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV

namespace oceanbase
{
namespace share
{
class ObLSID
{
public:
  // constant LS ID
  static const int64_t INVALID_LS_ID = -1;               // INVALID LS
  static const int64_t SYS_LS_ID = 1;                    // SYS LS for every Tenant
  static const int64_t VT_LS_ID = SYS_LS_ID;             // LS for virtual table
  static const int64_t IDS_LS_ID = SYS_LS_ID;            // LS for Trans GTS service
  static const int64_t LOCK_SERVICE_LS_ID = SYS_LS_ID;   // LS for Lock Service
  static const int64_t GAIS_LS_ID = SYS_LS_ID;           // LS for Global AutoInc Service
  static const int64_t DAS_ID_LS_ID = SYS_LS_ID;         // LS for DAS Id service
  static const int64_t WRS_LS_ID = SYS_LS_ID;            // LS for Weak Read Service
  static const int64_t SCHEDULER_LS_ID = INT64_MAX;      // LS for Trans scheduler
  static const int64_t MAJOR_FREEZE_LS_ID = SYS_LS_ID;   // LS for Major Freeze Service

  static const int64_t MIN_USER_LS_ID = 1000;
  static const int64_t MIN_USER_LS_GROUP_ID = 1000;

public:
  ObLSID() : id_(INVALID_LS_ID) {}
  ObLSID(const ObLSID &other) : id_(other.id_) {}
  explicit ObLSID(const int64_t id) : id_(id) {}
  ~ObLSID() { reset(); }

public:
  int64_t id() const { return id_; }
  void reset() { id_ = INVALID_LS_ID; }

  // assignment
  ObLSID &operator=(const int64_t id) { id_ = id; return *this; }
  ObLSID &operator=(const ObLSID &other) { id_ = other.id_; return *this; }

  // LS attribute interface
  bool is_sys_ls() const { return SYS_LS_ID == id_; }
  bool is_user_ls() const { return id_ > MIN_USER_LS_ID && SCHEDULER_LS_ID != id_; }
  bool is_scheduler_ls() const { return SCHEDULER_LS_ID == id_; }
  bool is_valid() const { return INVALID_LS_ID != id_; }
  bool is_valid_with_tenant(const uint64_t tenant_id) const
  {
    // 1. User tenant have SYS LS and User LS
    // 2. SYS tenant and Meta tenant only have SYS LS
    return (is_user_tenant(tenant_id) && (is_sys_ls() || is_user_ls()))
        || ((is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) && is_sys_ls());
  }

  // compare operator
  bool operator == (const ObLSID &other) const { return id_ == other.id_; }
  bool operator >  (const ObLSID &other) const { return id_ > other.id_; }
  bool operator >= (const ObLSID &other) const { return id_ >= other.id_; }
  bool operator != (const ObLSID &other) const { return id_ != other.id_; }
  bool operator <  (const ObLSID &other) const { return id_ < other.id_; }
  bool operator <= (const ObLSID &other) const { return id_ <= other.id_; }
  int compare(const ObLSID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }

  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));

private:
  int64_t id_;
};

static const ObLSID INVALID_LS(ObLSID::INVALID_LS_ID);
static const ObLSID SYS_LS(ObLSID::SYS_LS_ID);
static const ObLSID IDS_LS(ObLSID::IDS_LS_ID);
static const ObLSID GTS_LS(ObLSID::IDS_LS_ID);
static const ObLSID GTI_LS(ObLSID::IDS_LS_ID);
static const ObLSID GAIS_LS(ObLSID::GAIS_LS_ID);
static const ObLSID SCHEDULER_LS(ObLSID::SCHEDULER_LS_ID);
static const ObLSID LOCK_SERVICE_LS(ObLSID::LOCK_SERVICE_LS_ID);
static const ObLSID DAS_ID_LS(ObLSID::DAS_ID_LS_ID);
static const ObLSID MAJOR_FREEZE_LS(ObLSID::MAJOR_FREEZE_LS_ID);
static const ObLSID WRS_LS_ID(ObLSID::WRS_LS_ID);

static const int64_t OB_DEFAULT_LS_COUNT = 3;
typedef common::ObSEArray<share::ObLSID, OB_DEFAULT_LS_COUNT> ObLSArray;
} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_LS_ID
