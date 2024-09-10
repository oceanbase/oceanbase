// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_AGGREGATED_SCANNER_H_
#define OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_AGGREGATED_SCANNER_H_
#include "ob_cg_scanner.h"

namespace oceanbase
{
namespace storage
{
class ObAggGroupBase;

class ObCGAggregatedScanner : public ObCGRowScanner
{
public:
  ObCGAggregatedScanner();
  virtual ~ObCGAggregatedScanner();
  virtual void reset() override final;
  virtual void reuse() override final;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override final;
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) override final;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override final;
  virtual ObCGIterType get_type() override final
  { return OB_CG_AGGREGATED_SCANNER; }
  INHERIT_TO_STRING_KV("ObCGRowScanner", ObCGRowScanner,
      K_(need_access_data), K_(need_get_row_ids), K_(is_agg_finished), K_(agg_group), K_(cur_processed_row_count));
private:
  virtual int inner_fetch_rows(const int64_t row_cap, const int64_t datum_offset) override final;
  int check_need_access_data(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx);
  bool check_agg_finished();
  ObAggGroupBase *agg_group_;
  int64_t cur_processed_row_count_;
  bool need_access_data_;
  bool need_get_row_ids_;
  bool is_agg_finished_;
};

}
}
#endif
