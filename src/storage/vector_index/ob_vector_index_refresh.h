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

#pragma once

#include "storage/vector_index/ob_vector_refresh_idx_transaction.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
}

namespace storage {

struct ObVectorRefreshIndexCtx {
public:
  ObVectorRefreshIndexCtx()
      : allocator_("VecRefCtx"), tenant_id_(OB_INVALID_TENANT_ID),
        base_tb_id_(OB_INVALID_ID), domain_tb_id_(OB_INVALID_ID),
        index_id_tb_id_(OB_INVALID_ID), trans_(nullptr),
        refresh_method_(share::schema::ObVectorRefreshMethod::MAX) {}
  bool is_valid() const {
    return OB_INVALID_TENANT_ID != tenant_id_ &&
           OB_INVALID_ID != domain_tb_id_ && OB_INVALID_ID != base_tb_id_ &&
           OB_INVALID_ID != index_id_tb_id_ && OB_NOT_NULL(trans_) &&
           share::schema::ObVectorRefreshMethod::MAX != refresh_method_;
  }
  void reuse() {
    trans_ = nullptr;
    allocator_.reuse();
  }
  TO_STRING_KV(K_(tenant_id), K_(base_tb_id), K_(domain_tb_id),
               K_(index_id_tb_id), K_(refresh_method), K_(delta_rate_threshold),
               K_(refresh_threshold));

public:
  ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  uint64_t base_tb_id_;
  uint64_t domain_tb_id_;
  uint64_t index_id_tb_id_;
  ObVectorRefreshIdxTransaction *trans_;
  share::schema::ObVectorRefreshMethod refresh_method_;
  share::schema::ObVectorIndexOrganization idx_organization_;
  share::schema::ObVetcorIndexDistanceMetric idx_distance_metric_;
  ObString idx_parameters_;
  int64_t idx_parallel_creation_;
  share::SCN scn_;

  double delta_rate_threshold_;
  int64_t refresh_threshold_;
};

class ObVectorIndexRefresher {
public:
  ObVectorIndexRefresher();
  ~ObVectorIndexRefresher();
  DISABLE_COPY_ASSIGN(ObVectorIndexRefresher);

  int init(sql::ObExecContext &ctx, ObVectorRefreshIndexCtx &refresh_ctx);
  int refresh();

  TO_STRING_KV(KP_(ctx), KP_(refresh_ctx));

private:
  static int get_current_scn(share::SCN &current_scn);
  static int lock_domain_tb(ObVectorRefreshIdxTransaction &trans,
                               const uint64_t tenant_id,
                               const uint64_t domain_tb_id,
                               const bool try_lock = false);
  int get_table_row_count(const ObString &db_name, const ObString &table_name,
                          const share::SCN &scn, int64_t &row_cnt);
  int get_vector_index_col_names(const ObTableSchema *table_schema,
                                 bool is_collect_col_id,
                                 ObIArray<uint64_t>& col_ids,
                                 ObSqlString &col_names);
  int lock_domain_table_for_refresh();
  int do_refresh();
  int do_rebuild();

private:
  sql::ObExecContext *ctx_;
  ObVectorRefreshIndexCtx *refresh_ctx_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase