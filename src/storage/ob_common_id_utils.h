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

#ifndef OCEANBASE_STORAGE_OB_COMMON_ID_UTILS_H_
#define OCEANBASE_STORAGE_OB_COMMON_ID_UTILS_H_

#include "share/ob_common_id.h"             // ObCommonID

#include "share/ob_max_id_fetcher.h"     // ObMaxIdType, ObMaxIdFetcher

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace share
{
enum ObMaxIdType;
}
namespace storage
{

// Utils for ObCommonID
class ObCommonIDUtils
{
public:
  // Use ObUniqueIDService to generate Unique ID for ObCommonID in target tenant.
  //
  // NOTE: ID is unique, but not monotonic.
  //
  //
  // @param [in] tenant_id  target tenant id
  // @param [out] id        generated ID
  //
  // @return
  //    - OB_INVALID_ARGUMENT  tenant_id is invalid or not matched with MTL_ID
  static int gen_unique_id(const uint64_t tenant_id, share::ObCommonID &id);

  // Send rpc to the leader of sys LS of target tenant to execute gen_unique_id.
  //
  // Use this one when target tenant doesn't exist on current machine.
  static int gen_unique_id_by_rpc(const uint64_t tenant_id, share::ObCommonID &id);

  // Use ObMaxIdFetcher to generate monotonically increasing ID for ObCommonID in target tenant
  //
  // @param [in] tenant_id    target tenant id
  // @param [in] id_type      id type for ObMaxIdFetcher
  // @param [in] proxy        sql proxy
  // @param [out] id          generated monotonically increasing ID
  static int gen_monotonic_id(const uint64_t tenant_id,
      const share::ObMaxIdType id_type,
      const int32_t group_id,
      common::ObMySQLProxy &proxy,
      share::ObCommonID &id);
};

}
}

#endif /* OCEANBASE_STORAGE_OB_COMMON_ID_UTILS_H_ */
