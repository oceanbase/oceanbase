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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_MDS_NODE_FILTER
#define OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_MDS_NODE_FILTER

#include "lib/ob_errno.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/multi_data_source/mds_node.h"

namespace oceanbase
{
namespace storage
{
class ObTabletMediumInfoMdsNodeFilter
{
public:
  int operator()(mds::UserMdsNode<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> &node,
                 bool &need_skip) {
    int ret = common::OB_SUCCESS;
    if (!node.is_committed_()) {
      need_skip = true;
    }
    return ret;
  }
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_MDS_NODE_FILTER
