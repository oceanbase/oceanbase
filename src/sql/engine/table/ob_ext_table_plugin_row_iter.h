/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_table_plugin_row_iter.h
/// \brief Generic external-table row iterator for the plugin contract.
///
/// Drives the plugin vtable (`ObExtTablePluginApi`: plan_create / reader_create /
/// reader_open_scan / reader_open_task / reader_next_batch /
/// reader_close_task / reader_close_scan / reader_close) and imports each Arrow
/// batch into OB output vectors via `ObArrowDataLoader` for any plugin-backed
/// format.
///
/// Scan tasks are produced by the optimizer-side plugin pruner and distributed
/// through the existing PX path. At execution time the iterator converts the
/// instantiated storage filter into the contract predicate JSON for
/// `reader_open_scan`; OB still evaluates the same filter as the correctness
/// fallback. If the format's plugin .so is not loaded, init returns
/// OB_NOT_SUPPORTED.

#ifndef OB_EXT_TABLE_PLUGIN_ROW_ITER_H
#define OB_EXT_TABLE_PLUGIN_ROW_ITER_H

#include "lib/container/ob_bitmap.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "plugin/v2/include/ob_external_table_plugin.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // ObExtScanTaskArray (parse target)
#include "plugin/v2/host/ob_ext_host_provider.h"

#include <memory>

namespace arrow
{
class RecordBatch;
}

namespace oceanbase
{
namespace sql
{

class ObArrowDataLoader;
class ObPushdownFilterExecutor;

namespace ext_plugin
{

class ObExtTablePluginRowIterator : public ObExternalTableRowIterator
{
public:
  ObExtTablePluginRowIterator();
  virtual ~ObExtTablePluginRowIterator();

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row(common::ObNewRow *&row) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;

private:
  int open_next_task();
  int fetch_next_batch();
  int import_current_arrow_batch(struct ArrowArray *arrow_array,
                                 struct ArrowSchema *arrow_schema);
  int init_column_mapping_if_need();
  int project_column(ObEvalCtx &eval_ctx, const ObExpr *from, const ObExpr *to,
                     const int64_t read_count);
  int project_output_columns(const int64_t read_count);
  int build_reader_predicate_json();
  // File-column set + projection: iterator-lifetime, built once at init.
  int build_reader_column_set_and_projection();
  int build_projection_json(const common::ObIArray<uint64_t> &column_ids,
                            common::ObString &out_projection_json);
  int init_filter_evaluated_datums(ObPushdownFilterExecutor *curr_filter);
  int ensure_filter_eval_inited_once(ObPushdownFilterExecutor *root_filter);
  int calc_filters(const int64_t count,
                   ObPushdownFilterExecutor *curr_filter,
                   ObPushdownFilterExecutor *parent_filter);
  int reorder_expr_vector(ObExpr *expr,
                          ObEvalCtx &ctx,
                          const common::ObBitmap &bitmap,
                          const int64_t read_count,
                          const int64_t real_count);
  int reorder_output(const common::ObBitmap &bitmap, ObEvalCtx &ctx, int64_t &read_count);
  int reset_column_mapping();
  void close_ext();

  ObExternalIteratorState state_;

  const ObExtTablePluginApi *api_ = nullptr;
  ObExtHostCtx host_ctx_;
  ObExtTableHostApi host_;

  // The three backing objects are allocated once per row iterator. Scan/task
  // callbacks clear scope-local members without reallocating the objects.
  ObExtTableReaderWorkerStateRef reader_worker_state_ = nullptr;
  ObExtTableReaderScanStateRef reader_scan_state_ = nullptr;
  ObExtTableReaderTaskStateRef reader_task_state_ = nullptr;
  // Backing state exists independently of whether its logical scope is active.
  bool scan_active_ = false;
  // True after reader_open_task succeeds, until reader_close_task.
  bool has_open_task_ = false;

  std::shared_ptr<arrow::RecordBatch> cur_record_batch_;
  int64_t cur_batch_row_idx_;
  int64_t cur_batch_row_count_;

  // index is file_column_expr, value is the column index inside the arrow batch
  common::ObArray<int64_t> column_index_map_;
  common::ObArray<ObArrowDataLoader *> column_loaders_;
  ObBitVector *bit_vector_cache_;
  bool filter_eval_inited_;
  bool reader_predicate_built_;
  // True iff the WHOLE pushdown filter tree was converted to the plugin JSON
  // predicate (only white/logic nodes; black/sample/other "black-box" nodes are
  // never emitted). Set in build_reader_predicate_json from the builder's
  // out_fully_converted. When true, get_next_rows TRUSTS the plugin — once
  // reader_open_scan's parse_predicate_json accepts it and SetPredicate succeeds,
  // paimon owns the filtering — and SKIPS calc_filters. When false, a black-box
  // part was never pushed, so OB MUST run calc_filters to evaluate it.
  //
  // SAFETY MODEL (by design, chosen for CPP_PLUGIN_FORMAT): the optimizer's
  // ObLogTableScan::extract_pushdown_filters sets need_dup_filter=false for
  // CPP_PLUGIN_FORMAT, so the pushed predicates are NOT duplicated into
  // spec.filters_. There is therefore NO upper-layer backstop for them — the
  // plugin (SetPredicate) is the sole filterer of the pushed predicates, and
  // calc_filters inside the reader is only the "evaluate the black-box part"
  // path. Correctness is LOAD-BEARING on the plugin actually applying the
  // predicate: reader_open_scan currently returns SUCCESS even if parse_predicate_json
  // skips SetPredicate, so "no error" does NOT imply "filtered". The plugin MUST
  // apply SetPredicate for every JSON it accepts, or return an error so OB can
  // fall back. The only residual risk is the plugin OVER-filtering (dropping
  // qualifying rows), which OB cannot recover — the cost of trusting SetPredicate.
  //
  // If this trust model is ever abandoned (e.g. revert need_dup_filter to true to
  // restore the spec.filters_ backstop), the skip below can stay; the upper pass
  // would then just be redundant. If the plugin stops consuming predicate_json
  // (parse_predicate_json->SetPredicate) while need_dup_filter stays false, this
  // skip becomes unsafe — revert to unconditional calc_filters.
  bool reader_predicate_fully_pushed_;
  common::ObString reader_predicate_json_;
  // {"field_ids":[...]} projection handed to reader_create. Empty => read all.
  common::ObString reader_projection_json_;
  // File-column set backing projection + predicate column mapping.
  common::ObSEArray<uint64_t, 16> reader_file_column_ids_;
  common::ObSEArray<common::ObString, 16> reader_file_column_names_;
  ObArenaAllocator allocator_;
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_TABLE_PLUGIN_ROW_ITER_H
