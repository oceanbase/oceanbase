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

#ifndef OCEANBASE_STORAGE_OB_MDS_COMPACTION_FILTER
#define OCEANBASE_STORAGE_OB_MDS_COMPACTION_FILTER

#include "storage/compaction/ob_i_compaction_filter.h"

namespace oceanbase
{
namespace storage
{
class ObMdsMediumInfoFilter : public compaction::ObICompactionFilter
{
public:
  ObMdsMediumInfoFilter();
  virtual ~ObMdsMediumInfoFilter() = default;
  int init(const int64_t last_major_snapshot);
  virtual void reset() override
  {
    ObICompactionFilter::reset();
    last_major_snapshot_ = 0;
    is_inited_ = false;
  }

  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;
  INHERIT_TO_STRING_KV("ObICompactionFilter", ObICompactionFilter, "filter_name", "ObMdsMediumInfoFilter", K_(is_inited),
      K_(last_major_snapshot));

private:
  bool is_inited_;
  int64_t last_major_snapshot_;
};

class ObCrossLSMdsMinorFilter : public compaction::ObICompactionFilter
{
public:
  ObCrossLSMdsMinorFilter();
  virtual ~ObCrossLSMdsMinorFilter() = default;
public:
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_COMPACTION_FILTER
