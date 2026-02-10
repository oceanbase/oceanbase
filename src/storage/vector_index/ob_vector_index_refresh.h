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
namespace share{
class ObPluginVectorIndexAdaptor;
}
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
        refresh_method_(share::schema::ObVectorRefreshMethod::MAX),
        domain_tablet_id_(), is_tablet_level_(false),
        tmp_repeat_interval_(),
        domain_index_name_(),
        database_id_(OB_INVALID_ID) {}
  bool is_valid() const {
    return OB_INVALID_TENANT_ID != tenant_id_ &&
           OB_INVALID_ID != domain_tb_id_ && OB_INVALID_ID != base_tb_id_ &&
           OB_INVALID_ID != index_id_tb_id_ && OB_NOT_NULL(trans_) &&
           share::schema::ObVectorRefreshMethod::MAX != refresh_method_ &&
           (! is_tablet_level_ || domain_tablet_id_.is_valid());
  }
  void reuse() {
    trans_ = nullptr;
    allocator_.reuse();
  }
  TO_STRING_KV(K_(tenant_id), K_(base_tb_id), K_(domain_tb_id),
               K_(index_id_tb_id), K_(refresh_method), K_(delta_rate_threshold),
               K_(refresh_threshold), K_(idx_parameters),
               K_(domain_tablet_id), K_(is_tablet_level),
               K_(database_id));

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
  ObTabletID domain_tablet_id_;
  bool is_tablet_level_;
  ObString tmp_repeat_interval_;
  ObString domain_index_name_;
  uint64_t database_id_;
};

class ObVectorIndexRefresher {
public:
  static int tablet_fast_refresh(share::ObPluginVectorIndexAdaptor *adaptor, const share::SCN &scn);
  static int trigger_major_freeze(const uint64_t tenant_id, ObIArray<uint64_t> &tablet_ids);
  static int trigger_minor_freeze(const uint64_t tenant_id, ObIArray<uint64_t> &tablet_ids);

public:
  ObVectorIndexRefresher();
  ~ObVectorIndexRefresher();
  DISABLE_COPY_ASSIGN(ObVectorIndexRefresher);

  int init(sql::ObExecContext &ctx, ObVectorRefreshIndexCtx &refresh_ctx);
  int init(ObVectorRefreshIndexCtx &refresh_ctx);
  int refresh();
  int tablet_fast_refresh();
  TO_STRING_KV(KP_(ctx), KP_(refresh_ctx));

private:
  static int get_current_scn(share::SCN &current_scn);
  static int lock_domain_tb(ObVectorRefreshIdxTransaction &trans,
                               const uint64_t tenant_id,
                               const uint64_t domain_tb_id,
                               const bool try_lock = false);
  int get_table_row_count(const ObString &db_name, const ObString &table_name,
                          const share::SCN &scn, int64_t &row_cnt);
  static int get_vector_index_col_names(const ObTableSchema *table_schema,
                                 bool is_collect_col_id,
                                 ObIArray<uint64_t>& col_ids,
                                 ObSqlString &col_names);
  int lock_domain_table_for_refresh();
  int do_refresh();
  int do_rebuild();

  int lock_domain_table_for_tablet_refresh();
  static int get_partition_name(
      const uint64_t tenant_id,
      const int64_t data_table_id, const int64_t index_table_id, const ObTabletID &tablet_id,
      common::ObIAllocator &allocator, ObString &partition_names);

private:
  sql::ObExecContext *ctx_;
  ObVectorRefreshIndexCtx *refresh_ctx_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase