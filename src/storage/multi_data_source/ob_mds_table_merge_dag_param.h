/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM
#define OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM

#include <stdint.h>
#include "storage/compaction/ob_tablet_merge_task.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObTabletMdsMiniMergeDagParam : public compaction::ObTabletMergeDagParam
{
public:
  ObTabletMdsMiniMergeDagParam();
  virtual ~ObTabletMdsMiniMergeDagParam() = default;
public:
  INHERIT_TO_STRING_KV("ObTabletMergeDagParam", compaction::ObTabletMergeDagParam,
                       K_(flush_scn), KTIME_(generate_ts), K_(mds_construct_sequence), K_(mds_construct_sequence));
public:
  share::SCN flush_scn_;
  int64_t generate_ts_;
  int64_t mds_construct_sequence_;
};
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM