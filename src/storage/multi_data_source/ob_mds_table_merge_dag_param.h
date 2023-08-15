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
class ObMdsTableMergeDagParam : public compaction::ObTabletMergeDagParam
{
public:
  ObMdsTableMergeDagParam();
  virtual ~ObMdsTableMergeDagParam() = default;
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