/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/mv/ob_mv_compat_control.h"
#include "share/ob_upgrade_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace share;

#define DEF_MV_COMPAT_FEATURE(type, description, args...)                    \
const ObCompatInfo<ARGS_NUM(args)> ObMVCompatControl::COMPAT_##type = \
  ObCompatInfo<ARGS_NUM(args)>(static_cast<ObCompatFeatureType>(ObMVCompatFeatureType::type), \
                               #type, description, args);
#include "sql/resolver/mv/ob_mv_compat_feature_def.h"
#undef DEF_MV_COMPAT_FEATURE

const ObICompatInfo* ObMVCompatControl::infos_[] =
{
#define DEF_MV_COMPAT_FEATURE(type, args...)                       \
  &COMPAT_##type,
#include "sql/resolver/mv/ob_mv_compat_feature_def.h"
#undef DEF_MV_COMPAT_FEATURE
};

#define DEF_MV_COMPAT_FEATURE(type, description, args...)                    \
static_assert(1 == ARGS_NUM(args) % 2, "num of versions must be odd");
#include "sql/resolver/mv/ob_mv_compat_feature_def.h"
#undef DEF_MV_COMPAT_FEATURE

int ObMVCompatControl::check_feature_enable(const uint64_t compat_version,
                                            const ObMVCompatFeatureType feature_type,
                                            bool &is_enable)
{
  int ret = OB_SUCCESS;
  int64_t i = feature_type;
  if (OB_UNLIKELY(i < 0 || i >= ObMVCompatFeatureType::MV_COMPAT_MAX_TYPE) || OB_ISNULL(infos_[i])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mv compat feature type", K(ret), K(feature_type));
  } else {
    is_enable = infos_[i]->is_valid_version(compat_version);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
