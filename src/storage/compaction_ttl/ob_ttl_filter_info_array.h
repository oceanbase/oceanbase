/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_TTL_FILTER_INFO_ARRAY_H_
#define OB_STORAGE_TTL_FILTER_INFO_ARRAY_H_

#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"

namespace oceanbase
{
namespace storage
{

class ObTTLFilterInfoArray : public ObMDSInfoArray<ObTTLFilterInfo>
{
public:
  static bool compare(const ObTTLFilterInfo *lhs, const ObTTLFilterInfo *rhs);
  int sort_array() override;

  VIRTUAL_TO_STRING_KV(K_(is_inited), "array_cnt", count(), K_(src), K_(mds_info_array));
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TTL_FILTER_INFO_ARRAY_H_