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
#ifndef OB_STORAGE_COLUMN_STORE_OB_VIRTUAL_CG_SCANNER_H_
#define OB_STORAGE_COLUMN_STORE_OB_VIRTUAL_CG_SCANNER_H_
#include "ob_i_cg_iterator.h"
#include "storage/access/ob_aggregated_store.h"

namespace oceanbase
{
namespace storage
{
// virtual cg scanner which handles SCAN without referenced column, such as
// - RAND() < 2 filter
// - COUNT(*) aggregation
class ObVirtualCGScanner : public ObICGIterator
{
public:
  ObVirtualCGScanner();
  virtual ~ObVirtualCGScanner() { reset(); }
  virtual void reset() override final;
  virtual void reuse() override final;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override final;
  virtual int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override final;
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) override final;
  virtual int apply_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &filter_info,
      const int64_t row_count,
      const ObCGBitmap *parent_bitmap,
      ObCGBitmap &result_bitmap) override final;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override final;
  virtual ObCGIterType get_type() override final
  { return OB_CG_VIRTUAL_SCANNER; }
  TO_STRING_KV(K_(is_inited), K_(is_reverse_scan), KP_(iter_param), KP_(access_ctx),
               K_(current_group_size), KP_(cg_agg_cells));
private:
  int init_agg_cells(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx);
  int64_t is_inited_;
  bool is_reverse_scan_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  int64_t current_group_size_;
  ObCGAggCells *cg_agg_cells_;
};

class ObDefaultCGScanner : public ObICGIterator
{
public:
  ObDefaultCGScanner()
		:	query_range_valid_row_count_(0),
      datum_infos_(),
			default_row_(),
      stmt_allocator_(nullptr),
			is_inited_(false),
      filter_result_(false),
			total_row_count_(0),
			iter_param_(nullptr),
      filter_(nullptr),
			cg_agg_cells_(nullptr)
	{}
  virtual ~ObDefaultCGScanner() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) override;
  virtual int apply_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &filter_info,
      const int64_t row_count,
      const ObCGBitmap *parent_bitmap,
      ObCGBitmap &result_bitmap) override;
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) override;
  virtual int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
  virtual ObCGIterType get_type() override
  { return OB_CG_DEFAULT_SCANNER; }
  TO_STRING_KV(K_(is_inited), K_(total_row_count), K_(default_row), K_(query_range_valid_row_count),
			KPC_(iter_param), K_(datum_infos), K_(default_row), KPC_(cg_agg_cells));

private:
	int init_datum_infos_and_default_row(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx);
  int init_cg_agg_cells(const ObTableIterParam &iter_param, ObTableAccessContext &access_ctx);
  int do_filter(sql::ObPushdownFilterExecutor *filter, const sql::ObBitVector &skip_bit, bool &result);
  int add_lob_header_if_need(
      const share::schema::ObColumnParam &column_param,
      ObIAllocator &allocator,
      blocksstable::ObStorageDatum &datum);
protected:
  uint64_t query_range_valid_row_count_;
  common::ObFixedArray<blocksstable::ObSqlDatumInfo, common::ObIAllocator> datum_infos_;
	blocksstable::ObDatumRow default_row_;
private:
  ObIAllocator* stmt_allocator_;
	bool is_inited_;
  bool filter_result_;
	int64_t total_row_count_;
	const ObTableIterParam *iter_param_;
  sql::ObPushdownFilterExecutor *filter_;
	ObCGAggCells *cg_agg_cells_;
};

class ObDefaultCGGroupByScanner final : public ObDefaultCGScanner, public ObICGGroupByProcessor
{
public:
  ObDefaultCGGroupByScanner();
  virtual ~ObDefaultCGGroupByScanner();
  virtual void reset() override;
  virtual int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) override;
  virtual int init_group_by_info() override;
  virtual ObCGIterType get_type() override
  { return OB_CG_GROUP_BY_DEFAULT_SCANNER; }
  virtual int decide_group_size(int64_t &group_size) override;
  virtual int decide_can_group_by(
      const int32_t group_by_col,
      bool &can_group_by) override;
  virtual int read_distinct(const int32_t group_by_col) override;
  virtual int read_reference(const int32_t group_by_col) override;
  virtual int calc_aggregate(const bool is_group_by_col) override;
  virtual int locate_micro_index(const ObCSRange &range) override
  { return locate(range, nullptr); }
  INHERIT_TO_STRING_KV("ObDefaultCGScanner", ObDefaultCGScanner,
      KPC_(output_exprs), K_(group_by_agg_idxs), KP_(group_by_cell));
private:
  typedef ObSEArray<int32_t, 2>  ObGroupByAggIdxArray;
  const sql::ObExprPtrIArray *output_exprs_;
  ObSEArray<ObGroupByAggIdxArray, 2> group_by_agg_idxs_;
  ObGroupByCell *group_by_cell_;
};

}
}
#endif
