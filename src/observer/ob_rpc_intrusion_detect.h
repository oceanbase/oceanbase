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

#ifndef OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_
#define OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_

#include "io/easy_io.h"
#include "share/ob_i_server_auth.h"

namespace oceanbase
{
namespace observer
{
int ob_rpc_intrusion_detect_patch(easy_io_handler_pt* ez_handler, share::ObIServerAuth* auth);
}; // end namespace observer
}; // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_ */

