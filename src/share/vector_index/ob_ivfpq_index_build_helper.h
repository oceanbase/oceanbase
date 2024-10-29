/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVFPQ_INDEX_BUILD_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVFPQ_INDEX_BUILD_HELPER_H_

#include "share/vector_index/ob_ivf_index_build_helper.h"

namespace oceanbase {
namespace share {
enum ObIvfPQBuildStatus {
  IVF_CENTERS,
  PQ_PREPARE_CENTERS,
  PQ_KMEANS,
  COMPLETE
};

class ObIvfpqIndexBuildHelper: public ObIvfIndexBuildHelper {
public:
  ObIvfpqIndexBuildHelper()
      : ObIvfIndexBuildHelper("Ivfpq"), status_(IVF_CENTERS),
        pq_seg_(OB_DEFAULT_VECTOR_PQ_SEG), pq_dim_(0), pq_samples_(), pq_kmeans_helpers_() {}
  int init(const int64_t tenant_id, const int64_t lists, const int64_t segment,
           const ObVectorDistanceType distance_type);

  void destroy() override;
  void reuse() override;

  int build() override;
  bool is_finish() const override { return COMPLETE == status_; }
  bool skip_insert() const override { return is_finish() && center_helper_.skip_insert(); }

  int construct_batch_insert_second_container_sql(
      common::ObSqlString &second_container_string, const int64_t dest_table_id,
      const int64_t second_container_table_id) override;
  int construct_batch_insert_index_sql(sqlclient::ObMySQLResult &result,
                                       common::ObSqlString &index_string,
                                       int64_t &row_count, int64_t &idx) override;
  int set_partition_name(common::ObTabletID &tablet_id, uint64_t base_table_id) override;
  int set_center_cache(const int64_t table_id) override;
private:
  int pq_prepare_centers();
  int pq_kmeans();
  int split_vector(ObArray<ObTypeVector*> &vector_list, const ObTypeVector &vector, const int64_t nearest_center_idx, const int64_t segment);
  void destroy_pq_kmeans_helpers();
  void destroy_pq_samples();
private:
  ObIvfPQBuildStatus status_;        // reset to IVF_CENTERS
  int64_t pq_seg_;          // keep
  int64_t pq_dim_;          // keep
  ObArray<ObArray<ObTypeVector*>> pq_samples_;  // keep
  ObArray<ObKMeansCenterHelper*> pq_kmeans_helpers_;  // keep
};

} // namespace share
} // namespace oceanbase

#endif
