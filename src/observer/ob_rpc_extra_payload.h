/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_RPC_EXTRA_PAYLOAD_H_
#define OCEANBASE_OBSERVER_OB_RPC_EXTRA_PAYLOAD_H_

#include "rpc/obrpc/ob_irpc_extra_payload.h"

namespace oceanbase
{
namespace observer
{

class ObRpcExtraPayload : public obrpc::ObIRpcExtraPayload
{
public:
  ObRpcExtraPayload() {}
  virtual ~ObRpcExtraPayload() {}

  virtual int64_t get_serialize_size() const override;
  virtual int serialize(SERIAL_PARAMS) const override;
  virtual int deserialize(DESERIAL_PARAMS) override;

  static ObRpcExtraPayload &extra_payload_instance()
  {
    static ObRpcExtraPayload global_rpc_extra_payload;
    return global_rpc_extra_payload;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcExtraPayload);
};

} // end namespace server
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_RPC_EXTRA_PAYLOAD_H_
