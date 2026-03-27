/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
