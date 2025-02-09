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
#include "sql/das/iter/ob_das_spatial_scan_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_global_lookup_iter.h"
#include "sql/das/iter/ob_das_group_fold_iter.h"
#include "sql/das/iter/ob_das_sort_iter.h"
#include "sql/das/iter/ob_das_text_retrieval_iter.h"
#include "sql/das/iter/ob_das_text_retrieval_merge_iter.h"
#include "sql/das/iter/ob_das_index_merge_iter.h"
#include "sql/das/iter/ob_das_func_data_iter.h"
#include "sql/das/iter/ob_das_functional_lookup_iter.h"
#include "sql/das/iter/ob_das_cache_lookup_iter.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/das/iter/ob_das_mvi_lookup_iter.h"
#include "sql/das/iter/ob_das_domain_id_merge_iter.h"

namespace oceanbase
{
namespace sql
{

class ObDASIvfScanIter;
class ObDASHNSWScanIter;
class ObDASIvfScanIterParam;

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
                                            ObDASBaseRtDef *attach_rtdef,
                                            const ObDASRelatedTabletID &related_tablet_ids,
                                            const ObLSID &ls_id,
                                            ObDASIter *root_iter);
  static int set_func_lookup_iter_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                              ObDASBaseRtDef *attach_rtdef,
                                              const ObDASRelatedTabletID &related_tablet_ids,
                                              const ObLSID &ls_id,
                                              ObDASIter *root_iter);

  static int set_index_merge_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                         ObDASBaseRtDef *attach_rtdef,
                                         const ObDASRelatedTabletID &related_tablet_ids,
                                         const ObLSID &ls_id,
                                         ObDASIter *root_iter);
  static int set_vec_lookup_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                        ObDASBaseRtDef *attach_rtdef,
                                        const ObDASRelatedTabletID &related_tablet_ids,
                                        const ObLSID &ls_id,
                                        ObDASIter *root_iter);
  static bool is_vec_ivf_scan(const ObDASBaseCtDef *attach_ctdef, ObDASBaseRtDef *attach_rtdef);

private:
  static int create_das_scan_iter(common::ObIAllocator &alloc,
                                  const ObDASScanCtDef *scan_ctdef,
                                  ObDASScanRtDef *scan_rtdef,
                                  ObDASScanIter *&iter_tree);

  static int create_das_scan_with_merge_iter(storage::ObTableScanParam &scan_param,
                                             common::ObIAllocator &alloc,
                                             const ObDASBaseCtDef *input_ctdef,
                                             ObDASBaseRtDef *input_rtdef,
                                             const ObDASRelatedTabletID &related_tablet_ids,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot *snapshot,
                                             ObDASScanIter *&data_table_tree,
                                             ObDASIter *&iter_tree);
  static int create_partition_scan_tree(ObTableScanParam &scan_param,
                                        common::ObIAllocator &alloc,
                                        const ObDASScanCtDef *scan_ctdef,
                                        ObDASScanRtDef *scan_rtdef,
                                        const ObDASBaseCtDef *attach_ctdef,
                                        ObDASBaseRtDef *attach_rtdef,
                                        const ObDASRelatedTabletID &related_tablet_ids,
                                        transaction::ObTxDesc *trans_desc,
                                        transaction::ObTxReadSnapshot *snapshot,
                                        ObDASIter *&iter_tree);

  static int create_local_lookup_sub_tree(ObTableScanParam &scan_param,
                                          common::ObIAllocator &alloc,
                                          const ObDASBaseCtDef *index_ctdef,
                                          ObDASBaseRtDef *index_rtdef,
                                          const ObDASScanCtDef *lookup_ctdef,
                                          ObDASScanRtDef *lookup_rtdef,
                                          const ObDASBaseCtDef *attach_ctdef,
                                          ObDASBaseRtDef *attach_rtdef,
                                          const ObDASRelatedTabletID &related_tablet_ids,
                                          transaction::ObTxDesc *trans_desc,
                                          transaction::ObTxReadSnapshot *snapshot,
                                          const ObTabletID &lookup_tablet_id,
                                          ObDASIter *index_table_sub_tree,
                                          ObDASIter *&iter_tree,
                                          const int64_t batch_row_count = ObDASLookupIterParam::LOCAL_LOOKUP_ITER_DEFAULT_BATCH_ROW_COUNT);

  static int create_local_lookup_tree(ObTableScanParam &scan_param,
                                      common::ObIAllocator &alloc,
                                      const ObDASScanCtDef *scan_ctdef,
                                      ObDASScanRtDef *scan_rtdef,
                                      const ObDASScanCtDef *lookup_ctdef,
                                      ObDASScanRtDef *lookup_rtdef,
                                      const ObDASBaseCtDef *attach_ctdef,
                                      ObDASBaseRtDef *attach_rtdef,
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

  static int create_function_lookup_tree(ObTableScanParam &scan_param,
                                         common::ObIAllocator &alloc,
                                         const ObDASBaseCtDef *attach_ctdef,
                                         ObDASBaseRtDef *attach_rtdef,
                                         const ObDASRelatedTabletID &related_tablet_ids,
                                         transaction::ObTxDesc *trans_desc,
                                         transaction::ObTxReadSnapshot *snapshot,
                                         ObDASIter *&iter_tree);

  static int create_doc_id_scan_sub_tree(ObTableScanParam &scan_param,
                                         common::ObIAllocator &alloc,
                                         const ObDASDocIdMergeCtDef *merge_ctdef,
                                         ObDASDocIdMergeRtDef *merge_rtdef,
                                         const ObDASRelatedTabletID &related_tablet_ids,
                                         transaction::ObTxDesc *trans_desc,
                                         transaction::ObTxReadSnapshot *snapshot,
                                         ObDASScanIter *&data_table_tree,
                                         ObDASIter *&iter_tree);

  static int create_vid_scan_sub_tree(ObTableScanParam &scan_param,
                                      common::ObIAllocator &alloc,
                                      const ObDASVIdMergeCtDef *merge_ctdef,
                                      ObDASVIdMergeRtDef *merge_rtdef,
                                      const ObDASRelatedTabletID &related_tablet_ids,
                                      transaction::ObTxDesc *trans_desc,
                                      transaction::ObTxReadSnapshot *snapshot,
                                      ObDASScanIter *&data_table_tree,
                                      ObDASIter *&iter_tree);

  static int create_domain_id_scan_sub_tree(ObTableScanParam &scan_param,
                                            common::ObIAllocator &alloc,
                                            const ObDASDomainIdMergeCtDef *merge_ctdef,
                                            ObDASDomainIdMergeRtDef *merge_rtdef,
                                            const ObDASRelatedTabletID &related_tablet_ids,
                                            transaction::ObTxDesc *trans_desc,
                                            transaction::ObTxReadSnapshot *snapshot,
                                            ObDASScanIter *&data_table_tree,
                                            ObDASIter *&iter_tree);

  static int create_domain_lookup_sub_tree(ObTableScanParam &scan_param,
                                           const ObLSID &ls_id,
                                           common::ObIAllocator &alloc,
                                           const ObDASTableLookupCtDef *table_lookup_ctdef,
                                           ObDASTableLookupRtDef *table_lookup_rtdef,
                                           const ObDASRelatedTabletID &related_tablet_ids,
                                           const bool &doc_id_lookup_keep_order,
                                           const bool &main_lookup_keep_order,
                                           transaction::ObTxDesc *trans_desc,
                                           transaction::ObTxReadSnapshot *snapshot,
                                           ObDASIter *doc_id_iter,
                                           ObDASIter *&domain_lookup_result);

  static int create_mvi_lookup_tree(ObTableScanParam &scan_param,
                                    common::ObIAllocator &alloc,
                                    const ObDASBaseCtDef *attach_ctdef,
                                    ObDASBaseRtDef *attach_rtdef,
                                    const ObDASRelatedTabletID &related_tablet_ids,
                                    transaction::ObTxDesc *trans_desc,
                                    transaction::ObTxReadSnapshot *snapshot,
                                    ObDASIter *&iter_tree);

  static int create_gis_lookup_tree(ObTableScanParam &scan_param,
                                    common::ObIAllocator &alloc,
                                    const ObDASBaseCtDef *attach_ctdef,
                                    ObDASBaseRtDef *attach_rtdef,
                                    const ObDASRelatedTabletID &related_tablet_ids,
                                    transaction::ObTxDesc *trans_desc,
                                    transaction::ObTxReadSnapshot *snapshot,
                                    ObDASIter *&iter_tree,
                                    const bool in_vec_pre_filter = false);
  static int create_vec_lookup_tree(ObTableScanParam &scan_param,
                                    common::ObIAllocator &alloc,
                                    const ObDASBaseCtDef *attach_ctdef,
                                    ObDASBaseRtDef *attach_rtdef,
                                    const ObDASRelatedTabletID &related_tablet_ids,
                                    transaction::ObTxDesc *trans_desc,
                                    transaction::ObTxReadSnapshot *snapshot,
                                    ObDASIter *&iter_tree);
  static int create_vec_hnsw_lookup_tree(ObTableScanParam &scan_param,
                                    common::ObIAllocator &alloc,
                                    const ObDASBaseCtDef *attach_ctdef,
                                    ObDASBaseRtDef *attach_rtdef,
                                    const ObDASRelatedTabletID &related_tablet_ids,
                                    transaction::ObTxDesc *trans_desc,
                                    transaction::ObTxReadSnapshot *snapshot,
                                    ObDASIter *&iter_tree);
  static int create_vec_ivf_lookup_tree(ObTableScanParam &scan_param,
                                        common::ObIAllocator &alloc,
                                        const ObDASBaseCtDef *attach_ctdef,
                                        ObDASBaseRtDef *attach_rtdef,
                                        const ObDASRelatedTabletID &related_tablet_ids,
                                        transaction::ObTxDesc *trans_desc,
                                        transaction::ObTxReadSnapshot *snapshot,
                                        ObDASIter *&iter_tree);
  static int create_text_retrieval_sub_tree(const ObLSID &ls_id,
                                            common::ObIAllocator &alloc,
                                            const ObDASIRScanCtDef *ir_scan_ctdef,
                                            ObDASIRScanRtDef *ir_scan_rtdef,
                                            const ObDASFTSTabletID &related_tablet_ids,
                                            const bool is_func_lookup,
                                            transaction::ObTxDesc *trans_desc,
                                            transaction::ObTxReadSnapshot *snapshot,
                                            ObDASIter *&retrieval_result);

  static int create_sort_sub_tree(common::ObIAllocator &alloc,
                                  const ObDASSortCtDef *sort_ctdef,
                                  ObDASSortRtDef *sort_rtdef,
                                  const bool need_rewind,
                                  const bool need_distinct,
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

  static int create_index_merge_iter_tree(ObTableScanParam &scan_param,
                                          common::ObIAllocator &alloc,
                                          const ObDASBaseCtDef *attach_ctdef,
                                          ObDASBaseRtDef *attach_rtdef,
                                          const ObDASRelatedTabletID &related_tablet_ids,
                                          transaction::ObTxDesc *tx_desc,
                                          transaction::ObTxReadSnapshot *snapshot,
                                          ObDASIter *&iter_tree);

  static int create_index_merge_sub_tree(ObTableScanParam &scan_param,
                                         common::ObIAllocator &alloc,
                                         const ObDASBaseCtDef *ctdef,
                                         ObDASBaseRtDef *rtdef,
                                         const ObDASRelatedTabletID &related_tablet_ids,
                                         transaction::ObTxDesc *tx_desc,
                                         transaction::ObTxReadSnapshot *snapshot,
                                         ObDASIter *&iter);
  static int create_functional_lookup_sub_tree(ObTableScanParam &scan_param,
                                               const ObLSID &ls_id,
                                               common::ObIAllocator &alloc,
                                               const ObDASFuncLookupCtDef *table_lookup_ctdef,
                                               ObDASFuncLookupRtDef *table_lookup_rtdef,
                                               const ObDASRelatedTabletID &related_tablet_ids,
                                               const bool &lookup_keep_order,
                                               transaction::ObTxDesc *trans_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObDASIter *&fun_lookup_result);

  static int create_local_lookup_sub_tree(ObTableScanParam &scan_param,
                                          common::ObIAllocator &alloc,
                                          const ObDASBaseCtDef *index_ctdef,
                                          ObDASBaseRtDef *index_rtdef,
                                          const ObDASScanCtDef *lookup_ctdef,
                                          ObDASScanRtDef *lookup_rtdef,
                                          transaction::ObTxDesc *trans_desc,
                                          transaction::ObTxReadSnapshot *snapshot,
                                          ObDASIter *index_iter,
                                          const ObTabletID &lookup_tablet_id,
                                          ObDASLocalLookupIter *&lookup_iter,
                                          const bool lookup_keep_order = true,
                                          const int64_t lookup_batch_size =
                                              ObDASLookupIterParam::LOCAL_LOOKUP_ITER_DEFAULT_BATCH_ROW_COUNT);

  static int create_cache_lookup_sub_tree(ObTableScanParam &scan_param,
                                          common::ObIAllocator &alloc,
                                          const ObDASBaseCtDef *attach_ctdef,
                                          ObDASBaseRtDef *attach_rtdef,
                                          transaction::ObTxDesc *trans_desc,
                                          transaction::ObTxReadSnapshot *snapshot,
                                          ObDASIter *index_iter,
                                          const ObTabletID &lookup_tablet_id,
                                          ObDASCacheLookupIter *&lookup_iter,
                                          const bool lookup_keep_order = true,
                                          const int64_t lookup_batch_size =
                                              ObDASLookupIterParam::LOCAL_LOOKUP_ITER_DEFAULT_BATCH_ROW_COUNT);

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

  static void init_scan_iter_param(ObDASScanIterParam &param,
                                   const ObDASScanCtDef *scan_ctdef,
                                   ObDASBaseRtDef *scan_rtdef);
  static void init_spatial_scan_iter_param(ObDASSpatialScanIterParam &param,
                                           const ObDASScanCtDef *scan_ctdef,
                                           ObDASScanRtDef *scan_rtdef);

  static int create_das_spatial_scan_iter(ObIAllocator &alloc, ObDASSpatialScanIterParam &param, ObDASSpatialScanIter *&result);
  static int create_das_ivf_scan_iter(
    ObVectorIndexAlgorithmType type,
    ObIAllocator &alloc,
    ObDASIvfScanIterParam &param,
    ObDASIvfScanIter *&result);
  ObDASIterUtils() = delete;
  ~ObDASIterUtils() = delete;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_UTILS_H_ */
