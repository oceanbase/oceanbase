/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_IPALF_INTERFACE_STRUCTS_
#define OCEANBASE_LOGSERVICE_IPALF_INTERFACE_STRUCTS_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
struct ObAddr;
class ObILogAllocator;
class ObIOManager;
}
namespace share
{
class ObResourceManager;
class ObLocalDevice;
}
namespace palf
{
class PalfMonitorCb;
struct LSKey;
class PalfOptions;
class ILogBlockPool;
class PalfTransportCompressOptions;
}
namespace obrpc
{
class ObBatchRpc;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace ipalf
{
struct PalfEnvCreateParams
{
  const palf::PalfOptions *options_;
  const char *base_dir_;
  const common::ObAddr *self_;
  rpc::frame::ObReqTransport *transport_;
  obrpc::ObBatchRpc *batch_rpc_;
  common::ObILogAllocator *log_alloc_mgr_;
  palf::ILogBlockPool *log_block_pool_;
  palf::PalfMonitorCb *monitor_;
  share::ObLocalDevice *log_local_device_;
  share::ObResourceManager *resource_manager_;
  common::ObIOManager *io_manager_;

  TO_STRING_KV(K(options_),
               K(base_dir_),
               K(self_),
               KP(transport_),
               KP(batch_rpc_),
               KP(log_alloc_mgr_),
               KP(log_block_pool_),
               KP(monitor_),
               KP(log_local_device_),
               KP(resource_manager_),
               KP(io_manager_));
};

struct LibPalfEnvCreateParams
{
  palf::PalfMonitorCb *monitor_;

  TO_STRING_KV(KP(monitor_));
};
} // end namespace ipalf
} // end namespace oceanbase

#endif