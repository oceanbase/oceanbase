/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DTL

#include "ob_op_metric.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;


OB_SERIALIZE_MEMBER(ObOpMetric, enable_audit_, id_, type_, first_in_ts_, first_out_ts_, last_in_ts_, last_out_ts_, counter_, exec_time_, eof_);
