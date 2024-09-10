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
                    inv_scan_vec_id_col_, vec_index_param_, dim_);
OB_SERIALIZE_MEMBER(ObDASVecAuxScanRtDef);

} // sql
} // oceanbase
