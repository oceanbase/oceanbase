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

#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObDMLOpRowDesc, part_id_index_);
OB_SERIALIZE_MEMBER(ObPxMultiPartModifyOpInput, task_id_, sqc_id_, dfo_id_);
}
}
//// end of header file
