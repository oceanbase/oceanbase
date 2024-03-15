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

#ifndef _OB_SHARE_OB_I_GLOBAL_AUTO_INCR_SERVICE_H_
#define _OB_SHARE_OB_I_GLOBAL_AUTO_INCR_SERVICE_H_

#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_autoincrement_param.h"

namespace oceanbase
{
namespace share
{


class ObIGlobalAutoIncrementService
{
public:
  ObIGlobalAutoIncrementService() = default;
  ~ObIGlobalAutoIncrementService() = default;

  virtual int get_value(
      const AutoincKey &key,
      const uint64_t offset,
      const uint64_t increment,
      const uint64_t max_value,
      const uint64_t table_auto_increment,
      const uint64_t desired_count,
      const uint64_t cache_size,
      const int64_t &autoinc_version,
      uint64_t &sync_value,
      uint64_t &start_inclusive,
      uint64_t &end_inclusive) = 0;

  virtual int get_sequence_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &sequence_value) = 0;

  virtual int get_auto_increment_values(
      const uint64_t tenant_id,
      const common::ObIArray<AutoincKey> &autoinc_keys,
      const common::ObIArray<int64_t> &autoinc_versions,
      common::hash::ObHashMap<AutoincKey, uint64_t> &inc_values) = 0;

  virtual int local_push_to_global_value(
      const AutoincKey &key,
      const uint64_t max_value,
      const uint64_t insert_value,
      const int64_t &autoinc_version,
      const int64_t cache_size,
      uint64_t &global_sync_value) = 0;

  virtual int local_sync_with_global_value(const AutoincKey &key, const int64_t &autoinc_version, uint64_t &value) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIGlobalAutoIncrementService);
};

}
}
#endif /* _OB_SHARE_OB_I_GLOBAL_AUTO_INCR_SERVICE_H_ */
//// end of header file
