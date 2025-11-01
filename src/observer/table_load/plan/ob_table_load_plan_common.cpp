/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
