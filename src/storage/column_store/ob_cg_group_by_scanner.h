// Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_GROUP_BY_SCANNER_H_
#define OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_GROUP_BY_SCANNER_H_
#include "ob_cg_scanner.h"
#include "storage/access/ob_aggregated_store.h"

namespace oceanbase
{
namespace storage
{

class ObCGGroupByScanner final : public ObCGRowScanner, public ObICGGroupByProcessor
{
public:
  ObCGGroupByScanner();
  virtual ~ObCGGroupByScanner();
  virtual void reuse() override;
  virtual void reset() override;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override final;
  virtual ObCGIterType get_type() override
  { return OB_CG_GROUP_BY_SCANNER; }
  virtual int init_group_by_info() override;
  virtual int decide_group_size(int64_t &group_size) override;
  virtual int decide_can_group_by(const int32_t group_by_col, bool &can_group_by) override;
  virtual int read_distinct(const int32_t group_by_col) override;
  virtual int read_reference(const int32_t group_by_col) override;
  virtual int calc_aggregate(const bool is_group_by_col) override;
  virtual int locate_micro_index(const ObCSRange &range) override;
  INHERIT_TO_STRING_KV("ObCGRowScanner", ObCGRowScanner,
      KPC_(output_exprs), K_(group_by_agg_idxs), KP_(group_by_cell));
private:
  typedef ObSEArray<int32_t, 2>  ObGroupByAggIdxArray;
  int do_group_by_aggregate(const uint64_t count, const bool is_group_by_col, const int64_t ref_offset);
  const sql::ObExprPtrIArray *output_exprs_;
  // aggregate cell indexes for each output(agg) expr
  ObSEArray<ObGroupByAggIdxArray, 2> group_by_agg_idxs_;
  ObGroupByCell *group_by_cell_;
  ObCGIndexPrefetcher index_prefetcher_;
};

}
}
#endif
