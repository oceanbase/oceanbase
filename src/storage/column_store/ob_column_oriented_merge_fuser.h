/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_OB_COLUMN_ORIENTED_MERGE_FUSER_H_
#define OB_STORAGE_OB_COLUMN_ORIENTED_MERGE_FUSER_H_

#include "storage/compaction/ob_partition_merge_fuser.h"

namespace oceanbase
{
namespace compaction
{

class ObCOMinorSSTableFuser : public ObIPartitionMergeFuser
{
public:
  ObCOMinorSSTableFuser(common::ObIAllocator &allocator) : ObIPartitionMergeFuser(allocator) {}
  virtual ~ObCOMinorSSTableFuser() {}
  virtual int end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row) override
  { UNUSEDx(nop_pos, result_row); return OB_SUCCESS;}
  virtual int inner_init(const ObMergeParameter &merge_param)
  {
    int ret = OB_SUCCESS;
    int64_t column_cnt = 0;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
    } else if (OB_FAIL(merge_param.get_schema()->get_store_column_count(column_cnt, true/*full_col*/))) {
      STORAGE_LOG(WARN, "failed to get store column count", K(ret), K(merge_param.get_schema()));
    } else {
      column_cnt_ = column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    }
    return ret;
  }
};


} //compaction
} //oceanbase


#endif
