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

#ifndef OCEANBASE_OBRPC_OB_IRPC_EXTRA_PAYLOAD_H_
#define OCEANBASE_OBRPC_OB_IRPC_EXTRA_PAYLOAD_H_

#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace obrpc
{

class ObIRpcExtraPayload
{
public:
  virtual ~ObIRpcExtraPayload() {}

  virtual int64_t get_serialize_size() const = 0;
  virtual int serialize(SERIAL_PARAMS) const = 0;
  virtual int deserialize(DESERIAL_PARAMS) = 0;

  static inline ObIRpcExtraPayload &instance() { return *instance_pointer(); }
  // not thread safe
  static void set_extra_payload(ObIRpcExtraPayload &extra_payload)
  {
    instance_pointer() = &extra_payload;
  }

private:
  static inline ObIRpcExtraPayload *&instance_pointer();
};

class ObEmptyExtraPayload : public ObIRpcExtraPayload
{
public:
  virtual int64_t get_serialize_size() const override { return 0; }
  virtual int serialize(SERIAL_PARAMS) const override { UNF_UNUSED_SER; return common::OB_SUCCESS; }
  virtual int deserialize(DESERIAL_PARAMS) override { UNF_UNUSED_DES; return common::OB_SUCCESS; }

  static ObEmptyExtraPayload &empty_instance()
  {
    static ObEmptyExtraPayload global_empty_extra_payload;
    return global_empty_extra_payload;
  }
};

inline ObIRpcExtraPayload *&ObIRpcExtraPayload::instance_pointer()
{
  static ObIRpcExtraPayload *global_rpc_extra_payload = &ObEmptyExtraPayload::empty_instance();
  return global_rpc_extra_payload;
}

} // end namespace obrpc
} // end namespace oceanbase

#endif // OCEANBASE_OBRPC_OB_IRPC_EXTRA_PAYLOAD_H_
