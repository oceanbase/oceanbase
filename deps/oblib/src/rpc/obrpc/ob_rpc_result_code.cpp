/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obrpc/ob_rpc_result_code.h"

OB_SERIALIZE_MEMBER(oceanbase::obrpc::ObRpcResultCode, rcode_, msg_, warnings_)
