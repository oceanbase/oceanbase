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

#ifndef OB_STORAGE_COMPACTION_REORG_INFO_MINOR_FILTER_H_
#define OB_STORAGE_COMPACTION_REORG_INFO_MINOR_FILTER_H_

#include "storage/compaction/ob_i_compaction_filter.h"
#include "share/scn.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
}
namespace compaction
{

class ObReorgInfoMinorFilter : public ObICompactionFilter
{
public:
  ObReorgInfoMinorFilter()
    : ObICompactionFilter(),
      is_inited_(false),
      filter_val_(),
      filter_col_idx_(0),
      max_filtered_commit_scn_()
  {
    filter_val_.set_max();
    max_filtered_commit_scn_.set_min();
  }
  virtual ~ObReorgInfoMinorFilter() {}
  int init(const share::SCN &filter_val);
  OB_INLINE void reset()
  {
    filter_val_.set_max();
    filter_col_idx_ = 0;
    is_inited_ = false;
  }
  virtual CompactionFilterType get_filter_type() const override { return REORG_INFO_MINOR; }
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;

  INHERIT_TO_STRING_KV("ObMemberTableFilter", ObICompactionFilter,
      K_(filter_val), K_(filter_col_idx), K_(max_filtered_commit_scn));

public:
  share::SCN get_filter_scn() { return filter_val_; }
  share::SCN get_max_filtered_commit_scn() { return max_filtered_commit_scn_; }

private:
  bool is_inited_;
  share::SCN filter_val_;
  int64_t filter_col_idx_;
  share::SCN max_filtered_commit_scn_;
};


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_REORG_INFO_MINOR_FILTER_H_
