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

OB_SERIALIZE_MEMBER((ObDASVecAuxScanCtDef, ObDASAttachCtDef),
                    inv_scan_vec_id_col_, vec_index_param_, dim_, vec_type_, 
                    algorithm_type_, selectivity_, row_count_, access_pk_, 
                    can_use_vec_pri_opt_, extra_column_count_, spiv_scan_docid_col_, 
                    spiv_scan_value_col_, vector_index_param_, vec_query_param_,
                    adaptive_try_path_, // FARM COMPAT WHITELIST
                    is_multi_value_index_, // FARM COMPAT WHITELIST
                    is_spatial_index_, // FARM COMPAT WHITELIST
                    can_extract_range_// FARM COMPAT WHITELIST
                    );
OB_SERIALIZE_MEMBER(ObDASVecAuxScanRtDef);

} // sql
} // oceanbase

