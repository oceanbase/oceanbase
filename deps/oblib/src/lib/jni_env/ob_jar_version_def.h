/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#define JAR_VERSION_102 (oceanbase::common::cal_version(1, 0, 2, 0))
#define JAR_VERSION_103 (oceanbase::common::cal_version(1, 0, 3, 0))

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAR_VERSION_H_ */
