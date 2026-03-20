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

#ifndef OCEANBASE_SHARE_OB_TENANT_ROLE_H_
#define OCEANBASE_SHARE_OB_TENANT_ROLE_H_

#include "lib/string/ob_string.h" // ObString
#include "lib/utility/ob_print_utils.h"             // TO_STRING_KV
#include "lib/utility/ob_unify_serialize.h"   // serialize
#include "lib/oblog/ob_log_module.h"      // LOG*

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObMySQLResult;
} // namespace sqlclient
} // namespace common
namespace share {
class ObAllTenantInfo;

class ObTenantRole
{
  OB_UNIS_VERSION(1);
public:
  // Tenant Role
  enum Role
  {
    INVALID_TENANT = 0,
    PRIMARY_TENANT = 1,
    STANDBY_TENANT = 2,
    RESTORE_TENANT = 3,
    CLONE_TENANT   = 4,
    MAX_TENANT = 5,
  };
public:
  ObTenantRole() : value_(INVALID_TENANT) {}
  explicit ObTenantRole(const Role value) : value_(value) {}
  explicit ObTenantRole(const ObString &str);
  ~ObTenantRole() { reset(); }

public:
  void reset() { value_ = INVALID_TENANT; }
  bool is_valid() const { return INVALID_TENANT != value_; }
  Role value() const { return value_; }
  const char* to_str() const;

  // assignment
  ObTenantRole &operator=(const Role value) { value_ = value; return *this; }

  // compare operator
  bool operator == (const ObTenantRole &other) const { return value_ == other.value_; }
  bool operator != (const ObTenantRole &other) const { return value_ != other.value_; }

  // ObTenantRole attribute interface
  bool is_invalid() const { return INVALID_TENANT == value_; }
  bool is_primary() const { return PRIMARY_TENANT == value_; }
  bool is_standby() const { return STANDBY_TENANT == value_; }
  bool is_restore() const { return RESTORE_TENANT == value_; }
  bool is_clone() const { return CLONE_TENANT == value_; }

  TO_STRING_KV("tenant_role", to_str(), K_(value));
  DECLARE_TO_YSON_KV;
private:
  Role value_;
};

#define GEN_IS_TENANT_ROLE_DECLARE(TENANT_ROLE_VALUE, TENANT_ROLE) \
  bool is_##TENANT_ROLE##_tenant(const ObTenantRole::Role value);

GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::INVALID_TENANT, invalid)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::PRIMARY_TENANT, primary)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::STANDBY_TENANT, standby)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::RESTORE_TENANT, restore)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::CLONE_TENANT, clone)
#undef GEN_IS_TENANT_ROLE_DECLARE

static const ObTenantRole INVALID_TENANT_ROLE(ObTenantRole::INVALID_TENANT);
static const ObTenantRole PRIMARY_TENANT_ROLE(ObTenantRole::PRIMARY_TENANT);
static const ObTenantRole STANDBY_TENANT_ROLE(ObTenantRole::STANDBY_TENANT);
static const ObTenantRole RESTORE_TENANT_ROLE(ObTenantRole::RESTORE_TENANT);
static const ObTenantRole CLONE_TENANT_ROLE(ObTenantRole::CLONE_TENANT);

class ObProtectionMode
{
  OB_UNIS_VERSION(1);
public:
  enum Mode {
    INVALID_PROTECTION_MODE = 0,
    MAXIMUM_PERFORMANCE_MODE = 1,
    MAXIMUM_AVAILABILITY_MODE = 2,
    MAXIMUM_PROTECTION_MODE = 3,
    PROTECTION_MODE_MAX,
  };
public:
  ObProtectionMode() : mode_(INVALID_PROTECTION_MODE) {}
  explicit ObProtectionMode(const Mode mode) : mode_(mode) {}
  explicit ObProtectionMode(const ObString &str);
  ~ObProtectionMode() { reset(); }

  TO_STRING_KV("protection_mode", to_str(), K_(mode));

public:
  void reset() { mode_ = INVALID_PROTECTION_MODE; }
  bool is_valid() const { return mode_ > INVALID_PROTECTION_MODE && mode_ < PROTECTION_MODE_MAX;}
  Mode value() const { return mode_; }
  const char *to_str() const;

  // assignment
  ObProtectionMode &operator=(const Mode value) { mode_ = value; return *this; }

  // compare operator
  bool operator == (const ObProtectionMode &other) const { return mode_ == other.mode_; }
  bool operator != (const ObProtectionMode &other) const { return mode_ != other.mode_; }

  // ObProtectionMode attribute interface
  bool is_maximum_performance() const { return MAXIMUM_PERFORMANCE_MODE == mode_; }
  bool is_maximum_availability() const { return MAXIMUM_AVAILABILITY_MODE == mode_; }
  bool is_maximum_protection() const { return MAXIMUM_PROTECTION_MODE == mode_; }
  bool is_sync_mode() const { return is_maximum_protection() || is_maximum_availability(); }

private:
  Mode mode_;
};

class ObProtectionLevel
{
  OB_UNIS_VERSION(1);
public:
  enum Level {
    INVALID_PROTECTION_LEVEL = 0,
    MAXIMUM_PERFORMANCE_LEVEL = 1,
    MAXIMUM_AVAILABILITY_LEVEL = 2,
    MAXIMUM_PROTECTION_LEVEL = 3,
    RESYNCHRONIZATION_LEVEL = 4,
    PRE_MAXIMUM_PERFORMANCE_LEVEL = 5,
    PROTECTION_LEVEL_MAX,
  };
public:
ObProtectionLevel() : level_(INVALID_PROTECTION_LEVEL) {}
explicit ObProtectionLevel(const Level level) : level_(level) {}
explicit ObProtectionLevel(const ObString &str);
~ObProtectionLevel() { reset(); }

TO_STRING_KV("protection_level", to_str(), K_(level));

public:
  void reset() { level_ = INVALID_PROTECTION_LEVEL; }
  bool is_valid() const { return level_ > INVALID_PROTECTION_LEVEL && level_ < PROTECTION_LEVEL_MAX;}
  Level value() const { return level_; }
  const char *to_str() const;

  // assignment
  ObProtectionLevel &operator=(const Level value) { level_ = value; return *this; }

  // compare operator
  bool operator == (const ObProtectionLevel &other) const { return level_ == other.level_; }
  bool operator != (const ObProtectionLevel &other) const { return level_ != other.level_; }

  // ObProtectionLevel attribute interface
  bool is_maximum_performance() const { return MAXIMUM_PERFORMANCE_LEVEL == level_; }
  bool is_maximum_availability() const { return MAXIMUM_AVAILABILITY_LEVEL == level_; }
  bool is_maximum_protection() const { return MAXIMUM_PROTECTION_LEVEL == level_; }
  bool is_resynchronization() const { return RESYNCHRONIZATION_LEVEL == level_; }
  bool is_pre_maximum_performance() const { return PRE_MAXIMUM_PERFORMANCE_LEVEL == level_; }

  bool is_sync_level() const { return is_maximum_protection() || is_maximum_availability(); }

private:
  Level level_;
};

class ObProtectionStat
{
  OB_UNIS_VERSION(1);
public:
  ObProtectionStat();
  ~ObProtectionStat() { reset(); }

  TO_STRING_KV(K_(protection_mode), K_(protection_level), K_(switchover_epoch));
  void reset();
  int init(const ObProtectionMode &protection_mode, const ObProtectionLevel &protection_level, const int64_t switchover_epoch);
  int init(common::sqlclient::ObMySQLResult *result);
  int init(const ObAllTenantInfo &tenant_info);
  bool is_valid() const;
  bool is_steady() const;
  bool is_async_to_sync() const;
  bool is_sync_to_async() const;
  void set_switchover_epoch(const int64_t &switchover_epoch) { switchover_epoch_ = switchover_epoch; }
  ObProtectionMode get_protection_mode() const { return protection_mode_; }
  ObProtectionLevel get_protection_level() const { return protection_level_; }
  int64_t get_switchover_epoch() const { return switchover_epoch_; }
  bool operator == (const ObProtectionStat &other) const
  {
    return protection_mode_ == other.protection_mode_
      && protection_level_ == other.protection_level_
      && switchover_epoch_ == other.switchover_epoch_;
  }
  int get_next_protection_stat(ObProtectionStat &next_protection_stat) const;
  // when protection mode not change, it will return the next protection level according to current protection level
  // MA: RE -> MPF -> MA -> PRE-MPF -> RE
  // MPT: MPF -> MPT -> MPT
  // MPF: PRE-MPF -> MPF -> MPF
  static int get_next_protection_level(const ObProtectionMode &current_protection_mode,
    const ObProtectionLevel &current_protection_level,
    const ObProtectionMode &next_protection_mode,
    ObProtectionLevel &next_protection_level);
private:
  static int get_next_protection_level_from_mpt_(
      const ObProtectionLevel &current_protection_level,
      const ObProtectionMode &next_protection_mode,
      ObProtectionLevel &next_protection_level);
  static int get_next_protection_level_from_mpf_(const ObProtectionLevel &current_protection_level,
      const ObProtectionMode &next_protection_mode,
      ObProtectionLevel &next_protection_level);
  static int get_next_protection_level_from_ma_(const ObProtectionLevel &current_protection_level,
      const ObProtectionMode &next_protection_mode,
      ObProtectionLevel &next_protection_level);
private:
  ObProtectionMode protection_mode_;
  ObProtectionLevel protection_level_;
  int64_t switchover_epoch_;
};

static bool is_protection_mode_level_match(
  const ObProtectionMode &protection_mode,
  const ObProtectionLevel &protection_level);

struct ObSetProtectionModeArg
{
  OB_UNIS_VERSION(1);
public:
  ObSetProtectionModeArg() : tenant_id_(OB_INVALID_TENANT_ID),
                             protection_mode_(share::ObProtectionMode::INVALID_PROTECTION_MODE) {}
  ~ObSetProtectionModeArg() {}
  share::ObProtectionMode get_protection_mode() const { return protection_mode_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int init(const uint64_t tenant_id, const share::ObProtectionMode &protection_mode);
  bool is_valid() const;
  int assign(const ObSetProtectionModeArg &other);
  TO_STRING_KV(K_(tenant_id), K_(protection_mode));
private:
  uint64_t tenant_id_;
  share::ObProtectionMode protection_mode_;
};

}  // share
}  // oceanbase

#endif /* OCEANBASE_SHARE_OB_TENANT_ROLE_H_ */
