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
                    cluster_version_);

} // namespace observer
} // namespace oceanbase
