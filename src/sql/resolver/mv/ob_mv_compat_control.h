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

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_COMPAT_CONTROL_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_COMPAT_CONTROL_H_

#include "share/ob_compatibility_control.h"

namespace oceanbase
{
namespace sql
{

enum ObMVCompatFeatureType
{
#define DEF_MV_COMPAT_FEATURE(type, args...) type,
#include "sql/resolver/mv/ob_mv_compat_feature_def.h"
#undef DEF_MV_COMPAT_FEATURE
  MV_COMPAT_MAX_TYPE
};

class ObMVCompatControl
{
public:
  static int check_feature_enable(const uint64_t compat_version,
                                  const ObMVCompatFeatureType feature_type,
                                  bool &is_enable);
private:
#define DEF_MV_COMPAT_FEATURE(type, description, args...)      \
  static const share::ObCompatInfo<ARGS_NUM(args)> COMPAT_##type;
#include "sql/resolver/mv/ob_mv_compat_feature_def.h"
#undef DEF_MV_COMPAT_FEATURE
  static const share::ObICompatInfo* infos_[];
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_MV_OB_MV_COMPAT_CONTROL_H_
