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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_UTILS_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_UTILS_H_

#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/das/iter/ob_das_lookup_iter.h"
#include "sql/das/iter/ob_das_group_fold_iter.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
namespace sql
{
class ObDASIterUtils
{
public:
  static int create_table_scan_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                         ObTableScanRtDef &tsc_rtdef,
                                         ObEvalCtx &eval_ctx,
                                         ObExecContext &exec_ctx,
                                         ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                         const ObTableScanSpec &spec,
                                         ObDASMergeIter *&scan_iter,
                                         ObDASIter *&iter_tree);

  static int create_local_lookup_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                           ObTableScanRtDef &tsc_rtdef,
                                           ObEvalCtx &eval_ctx,
                                           ObExecContext &exec_ctx,
                                           ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                           const ObTableScanSpec &spec,
                                           ObDASMergeIter *&scan_iter,
                                           ObDASIter *&iter_tree);

  static int create_global_lookup_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                            ObTableScanRtDef &tsc_rtdef,
                                            ObEvalCtx &eval_ctx,
                                            ObExecContext &exec_ctx,
                                            ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                            const ObTableScanSpec &spec,
                                            bool can_retry,
                                            ObDASMergeIter *&scan_iter,
                                            ObDASIter *&iter_tree);

  static int create_group_fold_iter(const ObTableScanCtDef &tsc_ctdef,
                                    ObTableScanRtDef &tsc_rtdef,
                                    ObEvalCtx &eval_ctx,
                                    ObExecContext &exec_ctx,
                                    ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                    const ObTableScanSpec &spec,
                                    ObDASIter *iter_tree,
                                    ObDASGroupFoldIter *&fold_iter);

private:
  static int create_das_merge_iter_help(ObDASIterParam &param,
                                        common::ObIAllocator &alloc,
                                        bool is_global_lookup,
                                        ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                        const ObTableScanSpec &spec,
                                        ObDASMergeIter *&result);

  static int create_das_global_lookup_iter_help(ObDASIterParam &param,
                                                common::ObIAllocator &alloc,
                                                int64_t default_batch_row_count,
                                                ObDASMergeIter *index_table_iter,
                                                ObDASMergeIter *data_table_iter,
                                                bool can_retry,
                                                const ObTableScanCtDef &tsc_ctdef,
                                                ObTableScanRtDef &tsc_rtdef,
                                                ObDASGlobalLookupIter *&result);

  static int create_das_group_fold_iter_help(ObDASIterParam &param,
                                             common::ObIAllocator &alloc,
                                             bool need_check_output_datum,
                                             ObDASIter *iter_tree,
                                             ObDASGroupFoldIter *&result);

  ObDASIterUtils() = delete;
  ~ObDASIterUtils() = delete;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_UTILS_H_ */
