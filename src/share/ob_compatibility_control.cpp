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
#include "share/ob_compatibility_control.h"
#include "share/ob_cluster_version.h"
#include "share/ob_upgrade_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/file/ob_string_util.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

#define DEF_COMPAT_CONTROL_FEATURE(type, description, args...)                    \
const ObCompatInfo<ARGS_NUM(args)> ObCompatControl::COMPAT_##type = \
  ObCompatInfo<ARGS_NUM(args)>(ObCompatFeatureType::type, #type, description, args);
#include "share/ob_compatibility_control_feature_def.h"
#include "share/ob_compatibility_security_feature_def.h"
#undef DEF_COMPAT_CONTROL_FEATURE

const ObICompatInfo* ObCompatControl::infos_[] =
{
#define DEF_COMPAT_CONTROL_FEATURE(type, args...)                       \
  &COMPAT_##type,
#include "share/ob_compatibility_control_feature_def.h"
  NULL, // COMPAT_FEATURE_END
#include "share/ob_compatibility_security_feature_def.h"
#undef DEF_COMPAT_CONTROL_FEATURE
};

#define DEF_COMPAT_CONTROL_FEATURE(type, description, args...)                    \
static_assert(1 == ARGS_NUM(args) % 2, "num of versions must be odd");
#include "share/ob_compatibility_control_feature_def.h"
#include "share/ob_compatibility_security_feature_def.h"
#undef DEF_COMPAT_CONTROL_FEATURE

bool ObICompatInfo::is_valid_version(uint64_t version) const
{
  bool bret = false;
  const uint64_t *versions = NULL;
  int64_t version_num = 0;
  int64_t i = 0;
  get_versions(versions, version_num);
  if (OB_ISNULL(versions) || OB_UNLIKELY(version_num <= 0)) {
    bret = false;
  }
  for (; !bret && i + 1 < version_num; i += 2) {
    bret = version >= versions[i] && version < versions[i + 1];
  }
  if (!bret) {
    bret = version >= versions[i];
  }
  return bret;
}

int ObICompatInfo::print_version_range(common::ObString &range_str, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const uint64_t *versions = NULL;
  int64_t version_num = 0;
  int64_t i = 0;
  ObSqlString str;
  char buf[OB_CLUSTER_VERSION_LENGTH] = {0};
  int64_t len = 0;
  get_versions(versions, version_num);
  if (OB_ISNULL(versions) || OB_UNLIKELY(version_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (; OB_SUCC(ret) && i + 1 < version_num; i += 2) {
    if (i != 0) {
      if (OB_FAIL(str.append(", "))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(str.append("["))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (len = ObClusterVersion::print_version_str(
        buf, OB_CLUSTER_VERSION_LENGTH, versions[i])))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to print version", KR(ret), K(versions[i]));
    } else if (OB_FAIL(str.append(buf, len))) {
      LOG_WARN("failed to append start version", K(ret), K(versions[i]));
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (len = ObClusterVersion::print_version_str(
        buf, OB_CLUSTER_VERSION_LENGTH, versions[i + 1])))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to print version", KR(ret), K(versions[i]));
    } else if (OB_FAIL(str.append_fmt(", %.*s)", static_cast<int>(len), buf))) {
      LOG_WARN("failed to append end version", K(ret), K(versions[i]));
    }
  }
  if (OB_SUCC(ret)) {
    if (i != 0) {
      if (OB_FAIL(str.append(", "))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (len = ObClusterVersion::print_version_str(
        buf, OB_CLUSTER_VERSION_LENGTH, versions[i])))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to print version", KR(ret), K(versions[i]));
    } else if (OB_FAIL(str.append_fmt("[%.*s, )", static_cast<int>(len), buf))) {
      LOG_WARN("failed to append last version", K(ret), K(versions[i]));
    } else if (OB_FAIL(ob_write_string(allocator, str.string(), range_str))) {
      LOG_WARN("failed to deep copy str", K(ret));
    }
  }
  return ret;
}

int ObCompatControl::get_compat_version(const ObString &str, uint64_t &version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObClusterVersion::get_version(str, version))) {
    LOG_WARN("failed to get version", K(ret), K(str));
  }
  return ret;
}

int ObCompatControl::check_compat_version(const uint64_t tenant_id, const uint64_t compat_version)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t min_data_version = 0;
  if (OB_UNLIKELY(compat_version > GET_MIN_CLUSTER_VERSION())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compat version must less than min cluster version", K(ret), K(compat_version));
  } else if (OB_FAIL(ObUpgradeChecker::get_data_version_by_cluster_version(compat_version,
                                                                           data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    // tenant_id is invalid when create tenant, there is no need to check min data version
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (OB_UNLIKELY(data_version > min_data_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data version", K(ret), K(compat_version), K(tenant_id), K(min_data_version));
  }
  return ret;
}

int ObCompatControl::get_version_str(uint64_t version, ObString &str, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char buf[OB_CLUSTER_VERSION_LENGTH] = {0};
  int64_t len = ObClusterVersion::print_version_str(buf, OB_CLUSTER_VERSION_LENGTH, version);
  if (OB_UNLIKELY(OB_INVALID_INDEX == len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to print version", KR(ret), K(version));
  } else if (OB_FAIL(ob_write_string(allocator, ObString(len, buf), str))) {
    LOG_WARN("failed to write version str", K(ret), K(version));
  }
  return ret;
}

int ObCompatControl::check_feature_enable(const uint64_t compat_version,
                                          const ObCompatFeatureType feature_type,
                                          bool &is_enable)
{
  int ret = OB_SUCCESS;
  int64_t i = feature_type;
  if (OB_UNLIKELY(i < 0 || i >= ObCompatFeatureType::MAX_TYPE) || OB_ISNULL(infos_[i])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected compat feature type", K(ret), K(feature_type));
  } else {
    is_enable = infos_[i]->is_valid_version(compat_version);
  }
  return ret;
}

} // end of namespace share
} // end of namespace oceanbase
