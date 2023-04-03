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
