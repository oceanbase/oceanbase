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

#define USING_LOG_PREFIX SHARE

#include "share/ob_tenant_role.h"
#include "deps/oblib/src/lib/json/ob_yson.h"
#include "share/ob_tenant_info_proxy.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

static const char* TENANT_ROLE_ARRAY[] = 
{
  "INVALID",
  "PRIMARY",
  "STANDBY",
  "RESTORE",
  "CLONE",
};

OB_SERIALIZE_MEMBER(ObTenantRole, value_);
DEFINE_TO_YSON_KV(ObTenantRole,
                  OB_ID(value), value_);

const char* ObTenantRole::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_ROLE_ARRAY) == MAX_TENANT, "array size mismatch");
  const char *type_str = "UNKNOWN";
  if (OB_UNLIKELY(value_ >= ARRAYSIZEOF(TENANT_ROLE_ARRAY)
                  || value_ < INVALID_TENANT)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown tenant role", K_(value));
  } else {
    type_str = TENANT_ROLE_ARRAY[value_];
  }
  return type_str;
}

ObTenantRole::ObTenantRole(const ObString &str)
{
  value_ = INVALID_TENANT;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_ROLE_ARRAY); i++) {
      if (0 == str.case_compare(TENANT_ROLE_ARRAY[i])) {
        value_ = static_cast<ObTenantRole::Role>(i);
        break;
      }
    }
  }

  if (INVALID_TENANT == value_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid tenant role", K_(value), K(str));
  }
}

#define GEN_IS_TENANT_ROLE(TENANT_ROLE_VALUE, TENANT_ROLE) \
  bool is_##TENANT_ROLE##_tenant(const ObTenantRole::Role value) { return TENANT_ROLE_VALUE == value; }

GEN_IS_TENANT_ROLE(ObTenantRole::Role::INVALID_TENANT, invalid)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::PRIMARY_TENANT, primary)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::STANDBY_TENANT, standby)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::RESTORE_TENANT, restore)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::CLONE_TENANT, clone)
#undef GEN_IS_TENANT_ROLE
// The standard view value
static const char *protection_mode_strs[] = { "INVALID PROTECTION MODE",
                                                      "MAXIMUM PERFORMANCE",
                                                      "MAXIMUM AVAILABILITY",
                                                      "MAXIMUM PROTECTION"};
static const char *protection_level_strs[] = { "INVALID PROTECTION LEVEL",
                                                       "MAXIMUM PERFORMANCE",
                                                       "MAXIMUM AVAILABILITY",
                                                       "MAXIMUM PROTECTION",
                                                       "RESYNCHRONIZATION",
                                                       "PRE MAXIMUM PERFORMANCE"};

ObProtectionMode::ObProtectionMode(const ObString &str)
{
  mode_ = INVALID_PROTECTION_MODE;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(protection_mode_strs); i++) {
      if (0 == str.case_compare(protection_mode_strs[i])) {
        mode_ = static_cast<ObProtectionMode::Mode>(i);
        break;
      }
    }
  }

  if (INVALID_PROTECTION_MODE == mode_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid protection mode", K_(mode), K(str));
  }
}

const char* ObProtectionMode::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(protection_mode_strs) == PROTECTION_MODE_MAX, "array size mismatch");
  const char *mode_str = "UNKNOWN";
  if (OB_UNLIKELY(mode_ < INVALID_PROTECTION_MODE) || OB_UNLIKELY(mode_ >= PROTECTION_MODE_MAX)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown protection mode", K_(mode));
  } else {
    mode_str = protection_mode_strs[static_cast<int64_t>(mode_)];
  }
  return mode_str;
}

OB_SERIALIZE_MEMBER(ObProtectionMode, mode_);

ObProtectionLevel::ObProtectionLevel(const ObString &str)
{
  level_ = INVALID_PROTECTION_LEVEL;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(protection_level_strs); i++) {
      if (0 == str.case_compare(protection_level_strs[i])) {
        level_ = static_cast<ObProtectionLevel::Level>(i);
        break;
      }
    }
  }

  if (INVALID_PROTECTION_LEVEL == level_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid protection level", K_(level), K(str));
  }
}

const char* ObProtectionLevel::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(protection_level_strs) == PROTECTION_LEVEL_MAX, "array size mismatch");
  const char *level_str = "UNKNOWN";
  if (OB_UNLIKELY(level_ < INVALID_PROTECTION_LEVEL) || OB_UNLIKELY(level_ >= PROTECTION_LEVEL_MAX)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown protection level", K_(level));
  } else {
    level_str = protection_level_strs[static_cast<int64_t>(level_)];
  }
  return level_str;
}

OB_SERIALIZE_MEMBER(ObProtectionLevel, level_);

bool is_protection_mode_level_match(const ObProtectionMode &protection_mode,
   const ObProtectionLevel &protection_level)
{
  bool bret = true;
  if (protection_mode.is_maximum_performance()) {
    // 最大性能/降级
    bret = protection_level.is_maximum_performance() || protection_level.is_pre_maximum_performance();
  } else if (protection_mode.is_maximum_protection()) {
    // 最大保护/升级
    bret = protection_level.is_maximum_protection() || protection_level.is_maximum_performance();
  } else if (protection_mode.is_maximum_availability()) {
    // 最大可用/最大可用升级/最大可用等同步/最大可用降级
    bret = protection_level.is_maximum_availability() || protection_level.is_maximum_performance()
      || protection_level.is_resynchronization() || protection_level.is_pre_maximum_performance();
  } else {
    bret = false;
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObProtectionStat, protection_mode_, protection_level_, switchover_epoch_);
ObProtectionStat::ObProtectionStat()
  : protection_mode_(ObProtectionMode::INVALID_PROTECTION_MODE),
    protection_level_(ObProtectionLevel::INVALID_PROTECTION_LEVEL),
    switchover_epoch_(ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH) {}

void ObProtectionStat::reset()
{
  protection_mode_ = ObProtectionMode::INVALID_PROTECTION_MODE;
  protection_level_ = ObProtectionLevel::INVALID_PROTECTION_LEVEL;
  switchover_epoch_ = ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH;
}

int ObProtectionStat::init(const ObProtectionMode &protection_mode, const ObProtectionLevel &protection_level, const int64_t switchover_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!protection_mode.is_valid() || !protection_level.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(protection_mode), K(protection_level));
  } else if (OB_UNLIKELY(switchover_epoch < ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(switchover_epoch));
  } else {
    protection_mode_ = protection_mode;
    protection_level_ = protection_level;
    switchover_epoch_ = switchover_epoch;
  }
  return ret;
}

int ObProtectionStat::init(common::sqlclient::ObMySQLResult *result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(result));
  } else {
    ObString protection_mode_str;
    ObString protection_level_str;
    int64_t switchover_epoch = ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH;
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "protection_mode", protection_mode_str,
                false /* skip_null_error */, true /* skip_column_error */, "MAXIMUM PERFORMANCE");
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "protection_level", protection_level_str,
                false /* skip_null_error */, true /* skip_column_error */, "MAXIMUM PERFORMANCE");
    EXTRACT_INT_FIELD_MYSQL(*result, "switchover_epoch", switchover_epoch, int64_t);
    ObProtectionMode protection_mode(protection_mode_str);
    ObProtectionLevel protection_level(protection_level_str);
    if (FAILEDx(init(protection_mode, protection_level, switchover_epoch))) {
      LOG_WARN("failed to init protection stat", KR(ret), K(protection_mode_str), K(protection_level_str), K(switchover_epoch));
    }
  }
  return ret;
}

int ObProtectionStat::init(const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_info));
  } else if (OB_FAIL(init(tenant_info.get_protection_mode(), tenant_info.get_protection_level(),
    tenant_info.get_switchover_epoch()))) {
    LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
  }
  return ret;
}
bool ObProtectionStat::is_valid() const
{
  bool bret = true;
  if (!protection_mode_.is_valid() || !protection_level_.is_valid() ||
      switchover_epoch_ < ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH) {
    bret = false;
  } else if (!is_protection_mode_level_match(protection_mode_, protection_level_)) {
    bret = false;
  }
  return bret;
}

bool ObProtectionStat::is_steady() const
{
  bool bret = false;
  if (protection_mode_.is_maximum_performance()) {
    bret = protection_level_.is_maximum_performance();
  } else if (protection_mode_.is_maximum_protection()) {
    bret = protection_level_.is_maximum_protection();
  } else if (protection_mode_.is_maximum_availability()) {
    bret = protection_level_.is_maximum_availability() || protection_level_.is_resynchronization();
  }
  return bret;
}

bool ObProtectionStat::is_async_to_sync() const
{
  return protection_mode_.is_sync_mode() && protection_level_.is_maximum_performance();
}

bool ObProtectionStat::is_sync_to_async() const
{
  return protection_level_.is_pre_maximum_performance();
}

int ObProtectionStat::get_next_protection_stat(ObProtectionStat &next_protection_stat) const
{
  int ret = OB_SUCCESS;
  ObProtectionLevel next_level;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid protection stat", KR(ret), K(*this));
  } else if (OB_FAIL(get_next_protection_level(protection_mode_, protection_level_,
      protection_mode_, next_level))) {
    LOG_WARN("failed to get next level", KR(ret), K(*this));
  } else if (OB_FAIL(next_protection_stat.init(protection_mode_, next_level, switchover_epoch_))) {
    LOG_WARN("failed to init next protection stat", KR(ret), K(protection_mode_), K(next_level),
      K(switchover_epoch_));
  }
  return ret;
}

// TODO(shouju.zyp for MPT): add tests here
int ObProtectionStat::get_next_protection_level(const ObProtectionMode &current_protection_mode,

  const ObProtectionLevel &current_protection_level,
  const ObProtectionMode &next_protection_mode,
  ObProtectionLevel &next_protection_level)
{
  int ret = OB_SUCCESS;
  if (!current_protection_mode.is_valid() || !current_protection_level.is_valid()
      || !next_protection_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(current_protection_mode), K(current_protection_level), K(next_protection_mode));
  } else if (current_protection_mode.is_maximum_performance()) {
    if (FAILEDx(get_next_protection_level_from_mpf_(current_protection_level, next_protection_mode, next_protection_level))) {
      LOG_WARN("failed to get next protection mode for mpf", KR(ret), K(current_protection_mode), K(current_protection_level), K(next_protection_mode));
    }
  } else if (current_protection_mode.is_maximum_protection()) {
    if (FAILEDx(get_next_protection_level_from_mpt_(current_protection_level, next_protection_mode, next_protection_level))) {
      LOG_WARN("failed to get next protection mode for mpt", KR(ret), K(current_protection_mode), K(current_protection_level), K(next_protection_mode));
    }
  } else if (current_protection_mode.is_maximum_availability()) {
    if (FAILEDx(get_next_protection_level_from_ma_(current_protection_level, next_protection_mode, next_protection_level))) {
      LOG_WARN("failed to get next protection mode for ma", KR(ret), K(current_protection_mode), K(current_protection_level), K(next_protection_mode));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current protection mode", KR(ret), K(current_protection_mode));
  }
  return ret;
}
int ObProtectionStat::get_next_protection_level_from_mpt_(const ObProtectionLevel &current_protection_level,
    const ObProtectionMode &next_protection_mode,
    ObProtectionLevel &next_protection_level)
{
  int ret = OB_SUCCESS;
  if (next_protection_mode.is_maximum_performance()) {
    // MPT/MPF -> MPF/??? : downgrade now
    next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
  } else if (next_protection_mode.is_maximum_protection()) {
    if (current_protection_level.is_maximum_protection()) {
      // MPT/MPT -> MPT/MPT : no change
      next_protection_level = ObProtectionLevel::MAXIMUM_PROTECTION_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MPT/MPF -> MPT/MPT : upgrade now
      next_protection_level = ObProtectionLevel::MAXIMUM_PROTECTION_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode),
        K(current_protection_level));
    }
  } else if (next_protection_mode.is_maximum_availability()) {
    if (current_protection_level.is_maximum_protection()) {
      // MPT/MPT -> MA/MA : change from MPT to MA in MPT mode
      next_protection_level = ObProtectionLevel::MAXIMUM_AVAILABILITY_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MPT/MPF -> MA/MPF : change from MPF to MA in upgrade process
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode),
        K(current_protection_level));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next protection mode", KR(ret), K(next_protection_mode));
  }
  return ret;
}

int ObProtectionStat::get_next_protection_level_from_mpf_(const ObProtectionLevel &current_protection_level,
    const ObProtectionMode &next_protection_mode,
    ObProtectionLevel &next_protection_level)
{
  int ret = OB_SUCCESS;
  if (next_protection_mode.is_maximum_performance()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MPF/pre-MPF -> MPF/MPF : change from pre-MPF to MPF in downgrade process
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MPF/MPF -> MPF/MPF : no change
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else if (next_protection_mode.is_maximum_protection()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MPF/PRE-MPF -> MPT : change to MPT in downgrade process is not allowed
      ret = OB_NEED_WAIT;
      LOG_WARN("change from pre-mpf to mpt operation", KR(ret), K(next_protection_mode),
        K(current_protection_level));
    } else if (current_protection_level.is_maximum_performance()) {
      // MPF/MPF -> MPT/MPF : change from MPF to MPT in upgrade process
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else if (next_protection_mode.is_maximum_availability()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MPF/PRE-MPF -> MA/PRE-MPF : change from MPF to MA in downgrade process
      next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MPF/MPF -> MA/RE : change from MPF to MA
      next_protection_level = ObProtectionLevel::RESYNCHRONIZATION_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next protection mode", KR(ret), K(next_protection_mode));
  }
  return ret;
}

int ObProtectionStat::get_next_protection_level_from_ma_(const ObProtectionLevel &current_protection_level,
    const ObProtectionMode &next_protection_mode,
    ObProtectionLevel &next_protection_level)
{
  int ret = OB_SUCCESS;
  if (next_protection_mode.is_maximum_performance()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MA/PRE-MPF -> MPF/PRE-MPF : change from MA to MPF in downgrade process
      next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MA/MPF -> MPF/PRE_MPF : change from MPF to MA in upgrade process
      next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_maximum_availability()) {
      // MA/MA -> MPF/PRE-MPF : change from MA to MPF, start to downgrade
      next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_resynchronization()) {
      // MA/RE -> MPF/MPF : change from MA to MPF, no need to downgrade
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else if (next_protection_mode.is_maximum_protection()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MA/PRE-MPF -> MPT : change to MPT in downgrade process is not allowed
      ret = OB_NEED_WAIT;
      LOG_WARN("change from pre-mpf to mpt operation", KR(ret), K(next_protection_mode),
        K(current_protection_level));
    } else if (current_protection_level.is_maximum_performance()) {
      // MA/MPF -> MPT/MPF : change from MPF to MPT in upgrade process
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_maximum_availability()) {
      // MA/MA -> MPT/MPT : change from MA to MPT
      next_protection_level = ObProtectionLevel::MAXIMUM_PROTECTION_LEVEL;
    } else if (current_protection_level.is_resynchronization()) {
      // MA/RE -> MPT/MPF : change from MA to MPF, start to upgrade
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else if (next_protection_mode.is_maximum_availability()) {
    if (current_protection_level.is_pre_maximum_performance()) {
      // MA/PRE-MPF -> MA/RE : change from PRE-MPF to RE in downgrade process
      next_protection_level = ObProtectionLevel::RESYNCHRONIZATION_LEVEL;
    } else if (current_protection_level.is_maximum_performance()) {
      // MA/MPF -> MA/MA : change from MPF to MA, in upgrade process
      next_protection_level = ObProtectionLevel::MAXIMUM_AVAILABILITY_LEVEL;
    } else if (current_protection_level.is_maximum_availability()) {
      // MA/MA -> MA/PRE-MPF : start to downgrade
      next_protection_level = ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL;
    } else if (current_protection_level.is_resynchronization()) {
      // MA/RE -> MA/MPF : change from RE to MPF, start to upgrade
      next_protection_level = ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid current protection level", KR(ret), K(next_protection_mode), K(current_protection_level));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next protection mode", KR(ret), K(next_protection_mode));
  }
  return ret;
}
int ObSetProtectionModeArg::init(const uint64_t tenant_id, const share::ObProtectionMode &protection_mode)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id) || !is_valid_tenant_id(tenant_id) || !protection_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(protection_mode));
  } else {
    tenant_id_ = tenant_id;
    protection_mode_ = protection_mode;
  }
  return ret;
}
int ObSetProtectionModeArg::assign(const ObSetProtectionModeArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    protection_mode_ = other.protection_mode_;
  }
  return ret;
}

bool ObSetProtectionModeArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && is_user_tenant(tenant_id_) && protection_mode_.is_valid();
}
}  // share
}  // oceanbase