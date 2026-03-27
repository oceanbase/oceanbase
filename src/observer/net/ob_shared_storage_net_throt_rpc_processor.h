/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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