/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_BUILD_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_BUILD_HELPER_H_

#include "share/vector_index/ob_ivf_index_build_helper.h"

namespace oceanbase {
namespace share {

class ObIvfflatIndexBuildHelper: public ObIvfIndexBuildHelper {
public:
  ObIvfflatIndexBuildHelper()
      : ObIvfIndexBuildHelper("Ivfflat") {}
  int init(const int64_t tenant_id, const int64_t lists,
           const ObVectorDistanceType distance_type) override;
};

} // namespace share
} // namespace oceanbase

#endif
