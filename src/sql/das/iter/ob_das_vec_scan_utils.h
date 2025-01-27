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

#ifndef OB_DAS_VEC_SCAN_UTILS_H_
#define OB_DAS_VEC_SCAN_UTILS_H_

#include "sql/das/iter/ob_das_iter_define.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/engine/expr/ob_expr_vector.h"

namespace oceanbase
{
namespace sql
{
class ObDasVecScanUtils
{
public:
  static int set_lookup_key(ObRowkey &rowkey, ObTableScanParam &scan_param, uint64_t table_id);
  static int set_lookup_range(const ObNewRange &look_range, ObTableScanParam &scan_param, uint64_t table_id);
  static void release_scan_param(ObTableScanParam &scan_param);
  static void set_whole_range(ObNewRange &scan_range, common::ObTableID table_id);
  static int get_distance_expr_type(ObExpr &expr, ObEvalCtx &ctx, ObExprVectorDistance::ObVecDisType &dis_type);
  static int get_real_search_vec(common::ObArenaAllocator &allocator,
                                 ObDASSortRtDef *sort_rtdef,
                                 ObExpr *origin_vec,
                                 ObString &real_search_vec);
  static int init_limit(const ObDASVecAuxScanCtDef *ir_ctdef,
                        ObDASVecAuxScanRtDef *ir_rtdef,
                        const ObDASSortCtDef *sort_ctdef,
                        ObDASSortRtDef *sort_rtdef,
                        common::ObLimitParam &limit_param);
  static int init_sort(const ObDASVecAuxScanCtDef *ir_ctdef,
                       ObDASVecAuxScanRtDef *ir_rtdef,
                       const ObDASSortCtDef *sort_ctdef,
                       ObDASSortRtDef *sort_rtdef,
                       const common::ObLimitParam &limit_param,
                       ObExpr *&search_vec,
                       ObExpr *&distance_calc);
  static int reuse_iter(const share::ObLSID &ls_id,
                        ObDASScanIter *iter,
                        ObTableScanParam &scan_param,
                        const ObTabletID tablet_id);
  static int init_scan_param(const share::ObLSID &ls_id,
                             const common::ObTabletID &tablet_id,
                             const ObDASScanCtDef *ctdef,
                             ObDASScanRtDef *rtdef,
                             transaction::ObTxDesc *tx_desc,
                             transaction::ObTxReadSnapshot *snapshot,
                             ObTableScanParam &scan_param,
                             bool is_get = true);
  static int init_vec_aux_scan_param(const share::ObLSID &ls_id,
                                      const common::ObTabletID &tablet_id,
                                      const sql::ObDASScanCtDef *ctdef,
                                      sql::ObDASScanRtDef *rtdef,
                                      transaction::ObTxDesc *tx_desc,
                                      transaction::ObTxReadSnapshot *snapshot,
                                      ObTableScanParam &scan_param,
                                      bool is_get = false);
};

}  // namespace sql
}  // namespace oceanbase
#endif
