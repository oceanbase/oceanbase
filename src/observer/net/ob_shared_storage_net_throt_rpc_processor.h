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

#ifndef OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_PROCESSOR_H_
#define OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_PROCESSOR_H_

#include "observer/net/ob_shared_storage_net_throt_rpc_proxy.h"
#include "observer/ob_rpc_processor_simple.h"
namespace oceanbase
{
namespace observer
{
OB_DEFINE_PROCESSOR_S(SSNT, OB_SHARED_STORAGE_NET_THROT_REGISTER, ObSharedStorageNetThrotRegisterP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SHARED_STORAGE_NET_THROT_PREDICT, ObSharedStorageNetThrotPredictP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SHARED_STORAGE_NET_THROT_SET, ObSharedStorageNetThrotSetP);

}  // namespace observer
}  // namespace oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_PROCESSOR_H_ */