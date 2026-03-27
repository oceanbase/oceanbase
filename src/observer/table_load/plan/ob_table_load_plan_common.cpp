/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_plan_common.h"

namespace oceanbase
{
namespace observer
{
DEFINE_ENUM_FUNC(ObTableLoadTableType::Type, type, OB_TABLE_LOAD_TABLE_TYPE_DEF,
                 ObTableLoadTableType::);

DEFINE_ENUM_FUNC(ObTableLoadDependencyType::Type, type, OB_TABLE_LOAD_DEPENDENCY_TYPE_DEF,
                 ObTableLoadDependencyType::);

DEFINE_ENUM_FUNC(ObTableLoadInputType::Type, type, OB_TABLE_LOAD_INPUT_TYPE_DEF,
                 ObTableLoadInputType::);

DEFINE_ENUM_FUNC(ObTableLoadInputDataType::Type, type, OB_TABLE_LOAD_INPUT_DATA_TYPE_DEF,
                 ObTableLoadInputDataType::);

DEFINE_ENUM_FUNC(ObTableLoadWriteType::Type, type, OB_TABLE_LOAD_WRITE_TYPE_DEF,
                 ObTableLoadWriteType::);

DEFINE_ENUM_FUNC(ObTableLoadOpType::Type, type, OB_TABLE_LOAD_OP_TYPE_DEF, ObTableLoadOpType::);

} // namespace observer
} // namespace oceanbase
