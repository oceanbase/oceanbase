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
#define USING_LOG_PREFIX SHARE

#include "ob_t_sasl_client_transport.h"

#include <memory>
#include <thrift/transport/TBufferTransports.h>

namespace apache
{
namespace thrift
{
namespace transport
{

TSaslClientTransport::TSaslClientTransport(
    std::shared_ptr<TSasl> sasl_client, std::shared_ptr<TTransport> transport)
    : TSaslTransport(sasl_client, transport)
{}

TSaslClientTransport::~TSaslClientTransport()
{
  reset_sasl_negotiation_state();
}

std::shared_ptr<TTransport> TSaslClientTransport::wrap_client_transports(
    const oceanbase::common::ObString &service_name,
    const oceanbase::common::ObString &hostname,
    std::shared_ptr<TTransport> raw_transport)
{
  TSaslClient::setup_client_with_kerberos();
  std::shared_ptr<TSaslClient> sasl_client =
      std::shared_ptr<TSaslClient>(new TSaslClient(service_name, hostname));
  return std::unique_ptr<TSaslClientTransport>(
      new TSaslClientTransport(sasl_client, raw_transport));
}

void TSaslClientTransport::setup_sasl_negotiation_state()
{
  if (OB_ISNULL(sasl_)) {
    throw SaslClientImplException(
        "Invalid state: setup_sasl_negotiation_state() failed. TSaslClient not created");
  }
  sasl_->setup_sasl_context();
}

void TSaslClientTransport::handle_sasl_start_message()
{
  uint32_t res_length = 0;
  uint8_t dummy = 0;
  uint8_t *initialResponse = &dummy;

  /* Get data to send to the server if the client goes first. */
  if (OB_LIKELY(sasl_->has_initial_response())) {
    initialResponse = sasl_->evaluate_challenge_or_response(nullptr, 0, &res_length);
  }

  /* These two calls comprise a single message in the thrift-sasl protocol. */
  send_sasl_message(TSASL_START,
                    const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(
                        sasl_->get_mechanism_name().ptr())),
                    sasl_->get_mechanism_name().length(), false);
  send_sasl_message(TSASL_OK, initialResponse, res_length);

  transport_->flush();
}

void TSaslClientTransport::reset_sasl_negotiation_state()
{
  if (OB_ISNULL(sasl_)) {
    throw SaslClientImplException(
        "Invalid state: reset_sasl_negotiation_state() failed. TSaslClient not created");
  }
  sasl_->reset_sasl_context();
}

} // namespace transport
} // namespace thrift
} // namespace apache
