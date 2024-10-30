/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVFPQ_INDEX_SEARCH_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVFPQ_INDEX_SEARCH_HELPER_H_

#include "share/vector_index/ob_ivf_index_search_helper.h"
#include "share/vector_index/ob_tenant_pq_center_cache.h"

namespace oceanbase
{
namespace share
{

class ObIvfpqIndexSearchHelper: public ObIvfIndexSearchHelper
{
public:
  ObIvfpqIndexSearchHelper()
    : ObIvfIndexSearchHelper("Ivfpq"), pq_centers_(nullptr),
      pq_seg_(OB_DEFAULT_VECTOR_PQ_SEG)
  {}
  virtual ~ObIvfpqIndexSearchHelper()
  {
    destroy();
  }
  virtual void destroy() {
    ObIvfIndexSearchHelper::destroy();
    pq_centers_dis_array_.reset();
  }
  int init(
    const int64_t tenant_id,
    const int64_t index_table_id,
    const int64_t ann_k,
    const int64_t probes,
    const int64_t segment,
    const ObTypeVector &qvector,
    const common::ObIArray<int32_t> &output_projector,
    common::sqlclient::ObISQLConnection *conn,
    sql::ObSQLSessionInfo *session);
  int generate_pq_centers_by_storage(
      const sql::ExprFixedArray *output_exprs,
      sql::ObEvalCtx *eval_ctx) override;
  int get_rows_by_storage(
    const sql::ExprFixedArray *extra_access_exprs,
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx) override; // one by one
  int reset_pq_centers() override;
  int set_partition_name(common::ObTabletID &tablet_id) override;
  int get_pq_centers_by_cache() override;
  bool has_cached_pq_centers() const override { return pq_centers_ != nullptr; }
private:
  int update_pq_center_heap(const int64_t pq_center_idx, const int64_t seg_idx,
                            const ObTypeVector &tmp_vec, const ObVectorDistanceType &distance_type);
  int get_vector_probes(const sql::ObSQLSessionInfo *session, uint64_t &probes) const override;
  int split_vector(ObArray<ObTypeVector*> &vector_list, const ObTypeVector &vector, const int64_t center_idx, const int64_t segment, const int64_t pq_dim);
  ObTablePQCenters *pq_centers_;
  ObArray<ObArray<ObArray<double>>> pq_centers_dis_array_;
  int64_t pq_seg_;          // keep
};
} // share
} // oceanbase
#endif
