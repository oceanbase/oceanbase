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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAR_VERSION_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAR_VERSION_H_

#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace sql
{

#define JAR_VERSION_100 (oceanbase::common::cal_version(1, 0, 0, 0))
#define JAR_VERSION_101 (oceanbase::common::cal_version(1, 0, 1, 0))

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAR_VERSION_H_ */
