/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_TTL_FILTER_INFO_READER_H_
#define OB_STORAGE_TTL_FILTER_INFO_READER_H_

#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/truncate_info/ob_tablet_truncate_info_reader.h"

namespace oceanbase
{
namespace storage
{

using ObTabletTTLFilterInfoReader = ObTabletMDSInfoReader<ObTTLFilterInfoKey, ObTTLFilterInfo>;

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TTL_FILTER_INFO_READER_H_
