/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

OB_SERIALIZE_MEMBER(ObTableLoadUniqueKey, table_id_, task_id_);

OB_SERIALIZE_MEMBER(ObTableLoadDDLParam,
                    dest_table_id_,
                    task_id_,
                    schema_version_,
                    snapshot_version_,
                    data_version_,
                    cluster_version_,
                    is_no_logging_);

} // namespace observer
} // namespace oceanbase
