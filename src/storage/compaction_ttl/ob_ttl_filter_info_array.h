/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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