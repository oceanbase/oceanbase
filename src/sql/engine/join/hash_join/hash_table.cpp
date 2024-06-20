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
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/join/hash_join/hash_table.h"

namespace oceanbase
{
namespace sql
{
int GenericProber::equal(JoinTableCtx &ctx,
                         GenericItem *item,
                         const int64_t batch_idx,
                         bool &is_equal) {
  int ret = OB_SUCCESS;
  is_equal = true;
  ObHJStoredRow *build_sr = item->get_stored_row();
  int cmp_ret = 0;
  for (int64_t i = 0; is_equal && i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    ObExpr *probe_key = ctx.probe_keys_->at(i);
    ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
    const char *r_v = NULL;
    ObLength r_len = 0;
    build_sr->get_cell_payload(ctx.build_row_meta_, build_col_idx, r_v, r_len);
    vec->null_first_cmp(*probe_key, batch_idx,
                        build_sr->is_null(build_col_idx),
                        r_v, r_len, cmp_ret);
    is_equal = (cmp_ret == 0);
    LOG_DEBUG("generic probe equal", K(cmp_ret), K(is_equal));
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
