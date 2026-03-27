/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/executor/ob_slice_id.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

OB_SERIALIZE_MEMBER(ObSliceID, ob_task_id_, slice_id_);

}/* ns */
}/* ns oceanbase */
