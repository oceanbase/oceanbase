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

#ifndef OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
#define OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_

#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_param.h"
#include "share/scn.h"
namespace oceanbase
{
namespace blocksstable
{
  struct ObDatumRow;
}
namespace storage
{
class ObStoreRow;
}

namespace compaction
{

class ObICompactionFilter
{
public:
  ObICompactionFilter(bool is_full_merge = true)
   : is_full_merge_(is_full_merge)
  {
  }

  virtual ~ObICompactionFilter() {}
  OB_INLINE virtual void reset() { is_full_merge_ = false; }

  // for statistics
  enum ObFilterRet
  {
    FILTER_RET_NOT_CHANGE = 0,
    FILTER_RET_REMOVE = 1,
    FILTER_RET_MAX = 2,
  };
  const static char *ObFilterRetStr[];
  const static char *get_filter_ret_str(const int64_t idx);

  struct ObFilterStatistics
  {
    ObFilterStatistics()
    {
      MEMSET(row_cnt_, 0, sizeof(row_cnt_));
    }
    ~ObFilterStatistics() {}
    void add(const ObFilterStatistics &other);
    void inc(ObFilterRet filter_ret);
    void reset();

    int64_t to_string(char *buf, const int64_t buf_len) const;

    int64_t row_cnt_[FILTER_RET_MAX];
  };

  // need be thread safe
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) = 0;

  VIRTUAL_TO_STRING_KV(K_(is_full_merge));

  bool is_full_merge_; // be careful!!!! if full_merge=false, can't filter all keys
};

class ObTransStatusFilter : public ObICompactionFilter
{
public:
  ObTransStatusFilter()
    : ObICompactionFilter(true),
      is_inited_(false),
      filter_val_(),
      filter_col_idx_(0),
      max_filtered_end_scn_()
  {
    filter_val_.set_max();
    max_filtered_end_scn_.set_min();
  }
  ~ObTransStatusFilter() {}
  int init(const share::SCN &filter_val, const int64_t filter_col_idx);
  OB_INLINE virtual void reset() override
  {
    ObICompactionFilter::reset();
    filter_val_.set_max();
    filter_col_idx_ = 0;
    max_filtered_end_scn_.set_min();
    is_inited_ = false;
  }

  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;

  INHERIT_TO_STRING_KV("ObICompactionFilter", ObICompactionFilter, "filter_name", "ObTransStatusFilter", K_(filter_val),
      K_(filter_col_idx), K_(max_filtered_end_scn));

public:
  share::SCN get_max_filtered_end_scn() { return max_filtered_end_scn_; }
  share::SCN get_recycle_scn() { return filter_val_; }

private:
  bool is_inited_;
  share::SCN filter_val_;
  int64_t filter_col_idx_;
  share::SCN max_filtered_end_scn_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
