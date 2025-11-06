/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_vec_define.h"

namespace oceanbase
{
namespace sql
{

ObSPIVBlockMaxSpec::ObSPIVBlockMaxSpec(common::ObIAllocator &alloc)
  : col_types_(alloc),
    col_store_idxes_(alloc),
    scan_col_proj_(alloc),
    min_id_idx_(-1),
    max_id_idx_(-1),
    value_idx_(-1) {}

OB_SERIALIZE_MEMBER(ObSPIVBlockMaxSpec,
    col_types_,
    col_store_idxes_,
    scan_col_proj_,
    min_id_idx_,
    max_id_idx_,
    value_idx_);

bool ObSPIVBlockMaxSpec::is_valid() const
{
  return SKIP_INDEX_COUNT == col_types_.count()
      && SKIP_INDEX_COUNT == col_store_idxes_.count()
      && SKIP_INDEX_COUNT == scan_col_proj_.count()
      && MIN_ID_IDX == min_id_idx_
      && MAX_ID_IDX == max_id_idx_
      && VALUE_IDX == value_idx_;
}

OB_SERIALIZE_MEMBER((ObDASVecAuxScanCtDef, ObDASAttachCtDef),
                    inv_scan_vec_id_col_, vec_index_param_, dim_, vec_type_, algorithm_type_,
                    selectivity_, row_count_, access_pk_, can_use_vec_pri_opt_,
                    extra_column_count_, spiv_scan_docid_col_, spiv_scan_value_col_,
                    vector_index_param_, vec_query_param_, adaptive_try_path_,
                    is_multi_value_index_, is_spatial_index_, can_extract_range_, block_max_spec_, relevance_col_cnt_,
                    is_hybrid_, all_filters_can_be_picked_out_, use_rowkey_vid_tbl_);
OB_SERIALIZE_MEMBER(ObDASVecAuxScanRtDef);

} // sql
} // oceanbase
