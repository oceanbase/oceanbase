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

#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_global_lookup_iter.h"
#include "sql/das/iter/ob_das_group_fold_iter.h"
#include "sql/das/iter/ob_das_sort_iter.h"
#include "sql/das/iter/ob_das_text_retrieval_iter.h"
#include "sql/das/iter/ob_das_text_retrieval_merge_iter.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASIterUtils
{

public:
  static int create_das_scan_iter_tree(ObDASIterTreeType tree_type,
                                       storage::ObTableScanParam &scan_param,
                                       const ObDASScanCtDef *scan_ctdef,
                                       ObDASScanRtDef *scan_rtdef,
                                       const ObDASScanCtDef *lookup_ctdef,
                                       ObDASScanRtDef *lookup_rtdef,
                                       const ObDASBaseCtDef *attach_ctdef,
                                       ObDASBaseRtDef *attach_rtdef,
                                       const ObDASRelatedTabletID &related_tablet_ids,
                                       transaction::ObTxDesc *trans_desc,
                                       transaction::ObTxReadSnapshot *snapshot,
                                       common::ObIAllocator &alloc,
                                       ObDASIter *&iter_tree);

  static int create_tsc_iter_tree(ObDASIterTreeType tree_type,
                                  const ObTableScanCtDef &tsc_ctdef,
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

  static int set_text_retrieval_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                            const ObDASRelatedTabletID &related_tablet_ids,
                                            const ObLSID &ls_id,
                                            ObDASIter *root_iter);

private:
  static int create_partition_scan_tree(ObTableScanParam &scan_param,
                                        common::ObIAllocator &alloc,
                                        const ObDASScanCtDef *scan_ctdef,
                                        ObDASScanRtDef *scan_rtdef,
                                        ObDASIter *&iter_tree);

  static int create_local_lookup_tree(ObTableScanParam &scan_param,
                                      common::ObIAllocator &alloc,
                                      const ObDASScanCtDef *scan_ctdef,
                                      ObDASScanRtDef *scan_rtdef,
                                      const ObDASScanCtDef *lookup_ctdef,
                                      ObDASScanRtDef *lookup_rtdef,
                                      const ObDASRelatedTabletID &related_tablet_ids,
                                      transaction::ObTxDesc *trans_desc,
                                      transaction::ObTxReadSnapshot *snapshot,
                                      ObDASIter *&iter_tree);

  static int create_domain_lookup_tree(ObTableScanParam &scan_param,
                                       common::ObIAllocator &alloc,
                                       const ObDASBaseCtDef *attach_ctdef,
                                       ObDASBaseRtDef *attach_rtdef,
                                       ObDASIter *&iter_tree);

  static int create_text_retrieval_tree(ObTableScanParam &scan_param,
                                        common::ObIAllocator &alloc,
                                        const ObDASBaseCtDef *attach_ctdef,
                                        ObDASBaseRtDef *attach_rtdef,
                                        const ObDASRelatedTabletID &related_tablet_ids,
                                        transaction::ObTxDesc *trans_desc,
                                        transaction::ObTxReadSnapshot *snapshot,
                                        ObDASIter *&iter_tree);

  static int create_domain_lookup_sub_tree(const ObLSID &ls_id,
                                           common::ObIAllocator &alloc,
                                           const ObDASTableLookupCtDef *table_lookup_ctdef,
                                           ObDASTableLookupRtDef *table_lookup_rtdef,
                                           const ObDASRelatedTabletID &related_tablet_ids,
                                           transaction::ObTxDesc *trans_desc,
                                           transaction::ObTxReadSnapshot *snapshot,
                                           ObDASIter *doc_id_iter,
                                           ObDASIter *&domain_lookup_result);

  static int create_text_retrieval_sub_tree(const ObLSID &ls_id,
                                            common::ObIAllocator &alloc,
                                            const ObDASIRScanCtDef *ir_scan_ctdef,
                                            ObDASIRScanRtDef *ir_scan_rtdef,
                                            const ObDASRelatedTabletID &related_tablet_ids,
                                            transaction::ObTxDesc *trans_desc,
                                            transaction::ObTxReadSnapshot *snapshot,
                                            ObDASIter *&retrieval_result);

  static int create_sort_sub_tree(common::ObIAllocator &alloc,
                                  const ObDASSortCtDef *sort_ctdef,
                                  ObDASSortRtDef *sort_rtdef,
                                  ObDASIter *sort_input,
                                  ObDASIter *&sort_result);

  static int create_table_scan_iter_tree(const ObTableScanCtDef &tsc_ctdef,
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

  static int create_iter_children_array(const int64_t children_cnt,
                                        common::ObIAllocator &alloc,
                                        ObDASIter *iter);

  template<class IterType, class IterParamType>
  static int create_das_iter(common::ObIAllocator &alloc, IterParamType &param, IterType *&result)
  {
    int ret = OB_SUCCESS;
    IterType *iter = nullptr;
    if (OB_ISNULL(iter = OB_NEWx(IterType, &alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new a das iter", K(ret));
    } else if (OB_FAIL(iter->init(param))) {
      LOG_WARN("failed to init das iter", K(param), K(ret));
    }
    if (OB_SUCC(ret)) {
      result = iter;
    } else {
      if (OB_NOT_NULL(iter)) {
        iter->release();
        alloc.free(iter);
        iter = nullptr;
      }
    }
    return ret;
  }

  ObDASIterUtils() = delete;
  ~ObDASIterUtils() = delete;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_UTILS_H_ */
