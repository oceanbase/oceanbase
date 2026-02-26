/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_CLIENT_TRANSPORT_H
#define _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_CLIENT_TRANSPORT_H

#include "ob_t_sasl_transport.h"
#include "ob_t_sasl.h"

#include <thrift/transport/TTransport.h>
#include <thrift/transport/TVirtualTransport.h>

namespace apache
{
namespace thrift
{
namespace transport
{

/**
 * This transport implements the Simple Authentication and Security Layer (SASL).
 * see: http://www.ietf.org/rfc/rfc2222.txt.  It is based on and depends
 * on the presence of the cyrus-sasl library.  This is the client side.
 */
class TSaslClientTransport : public TSaslTransport
{
public:
  /**
   * Constructs a new TSaslTransport to act as a client.
   * sasl_client: the sasl object implimenting the underlying authentication
   * handshake transport: the transport to read and write data.
   */
  TSaslClientTransport(std::shared_ptr<TSasl> sasl_client,
                       std::shared_ptr<TTransport> transport);
  ~TSaslClientTransport() override;

  // Only impl Mechanism GSSAPI (KRB5)
  static std::shared_ptr<TTransport>
  wrap_client_transports(const oceanbase::common::ObString &service_name,
                         const oceanbase::common::ObString &hostname,
                         std::shared_ptr<TTransport> raw_transport);

protected:
  /* Set up the Sasl server state for a connection. */
  virtual void setup_sasl_negotiation_state() override;

  // Handle any startup messages.
  virtual void handle_sasl_start_message() override;

  /* Reset the Sasl client state. The negotiation will have to start from
   * scratch after this is called.
   */
  virtual void reset_sasl_negotiation_state() override;
};

} // namespace transport
} // namespace thrift
} // namespace apache

#endif /* _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_CLIENT_TRANSPORT_H */
