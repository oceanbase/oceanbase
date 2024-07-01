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
#include "ob_das_ir_define.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASIRScanCtDef, ObDASAttachCtDef),
                    flags_,
                    search_text_,
                    inv_scan_doc_id_col_,
                    inv_scan_doc_length_col_,
                    match_filter_,
                    relevance_expr_,
                    relevance_proj_col_,
                    estimated_total_doc_cnt_);

OB_SERIALIZE_MEMBER(ObDASIRScanRtDef);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupCtDef, ObDASAttachCtDef),
                    relevance_proj_col_);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupRtDef, ObDASAttachRtDef));

} // sql
} // oceanbase
