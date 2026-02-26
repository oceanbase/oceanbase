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

#include "ob_t_sasl_transport.h"
#include "lib/oblog/ob_log_module.h"
#include <thrift/transport/TBufferTransports.h>

namespace apache
{
namespace thrift
{
namespace transport
{

// For sasl server.
TSaslTransport::TSaslTransport(std::shared_ptr<TTransport> transport)
    : TVirtualTransport(transport->getConfiguration()), transport_(transport),
      sasl_(nullptr), should_wrap_(false), is_client_(false),
      proto_buf_(nullptr), allocator_(OB_SASL_SERVER_TRANS_ALLOCATOR)
{
  mem_buf_ =
      new TMemoryBuffer(DEFAULT_MEM_BUF_SIZE, transport->getConfiguration());
}

// For sasl client.
TSaslTransport::TSaslTransport(std::shared_ptr<TSasl> sasl_client,
                               std::shared_ptr<TTransport> transport)
    : TVirtualTransport(transport->getConfiguration()), transport_(transport),
      sasl_(sasl_client), should_wrap_(false), is_client_(true),
      proto_buf_(nullptr), allocator_(OB_SASL_CLIENT_TRANS_ALLOCATOR)
{
  mem_buf_ = new TMemoryBuffer(transport->getConfiguration());
}

TSaslTransport::~TSaslTransport()
{
  if (OB_NOT_NULL(proto_buf_)) {
    allocator_.free(proto_buf_);
    proto_buf_ = nullptr;
    allocator_.reset();
  }
  delete mem_buf_;
}

/*----------------start of override virtual methods-------------------*/
bool TSaslTransport::isOpen() const { return transport_->isOpen(); }

bool TSaslTransport::peek() { return (transport_->peek()); }

void TSaslTransport::open()
{
  int ret = OB_SUCCESS;
  // Only client should open the underlying transport.
  if (is_client_ && !transport_->isOpen()) {
    transport_->open();
  }

  // Start the SASL negotiation protocol.
  do_sasl_negotiation();
}

void TSaslTransport::close()
{
  // Reset the sasl negotiation state to avoid re-use state sasl connections.
  reset_sasl_negotiation_state();
  transport_->close();
}

uint32_t TSaslTransport::read(uint8_t *buf, uint32_t len)
{
  uint32_t result = 0;
  uint32_t read_bytes = mem_buf_->read(buf, len);

  if (OB_LIKELY(read_bytes > 0)) {
    shrink_buffer();
    result = read_bytes;
  } else {
    // If there's not enough data in cache, read from underlying transport.
    uint32_t data_length = read_length();
    // Fast path
    if (OB_LIKELY(is_client_ && len == data_length && !should_wrap_)) {
      transport_->readAll(buf, len);
      result = len;
    } else {
      uint8_t *tmp_buf = new uint8_t[data_length];
      transport_->readAll(tmp_buf, data_length);
      if (OB_LIKELY(should_wrap_)) {
        tmp_buf = sasl_->unwrap(tmp_buf, 0, data_length, &data_length);
      }

      // We will consume all the data, no need to put it in the memory buffer.
      if (OB_LIKELY(len == data_length)) {
        memcpy(buf, tmp_buf, len);
        delete[] tmp_buf;
        result = len;
      } else {
        mem_buf_->write(tmp_buf, data_length);
        mem_buf_->flush();
        delete[] tmp_buf;

        result = mem_buf_->read(buf, len);
        shrink_buffer();
      }
    }
  }
  return result;
}

void TSaslTransport::write(const uint8_t *buf, uint32_t len) {
  const uint8_t *new_buf;

  if (OB_LIKELY(should_wrap_)) {
    new_buf = sasl_->wrap(const_cast<uint8_t *>(buf), 0, len, &len);
  } else {
    new_buf = buf;
  }
  write_length(len);
  transport_->write(new_buf, len);
}

void TSaslTransport::flush() { transport_->flush(); }

/*----------------end of override virtual methods-------------------*/
/*----------------start of self defined methods-------------------*/
const ObString TSaslTransport::get_mechanism_name() const { return sasl_->get_mechanism_name(); }
/*----------------end of self defined methods-------------------*/
/*----------------start of protected methods-------------------*/
void TSaslTransport::do_sasl_negotiation() {
  int ret = OB_SUCCESS;
  NegotiationStatus status = TSASL_INVALID;
  uint32_t res_length;

  try {
    // Setup Sasl context.
    setup_sasl_negotiation_state();

    // Initiate SASL message.
    handle_sasl_start_message();

    // SASL connection handshake
    while (!sasl_->is_complete()) {
      uint8_t *message = receive_sasl_message(&status, &res_length);
      if (TSASL_COMPLETE == status) {
        if (is_client_) {
          if (!sasl_->is_complete()) {
            // Server sent COMPLETE out of order.
            throw TTransportException("Received COMPLETE but no handshake occurred");
          }
          break; // handshake complete
        }
      } else if (TSASL_OK != status) {
        const char *err_msg = "Expect TSASL_OK, but not";
        throw TTransportException(err_msg);
      }
      uint32_t challenge_length;
      uint8_t *challenge = sasl_->evaluate_challenge_or_response(
          message, res_length, &challenge_length);
      send_sasl_message(sasl_->is_complete() ? TSASL_COMPLETE : TSASL_OK,
                        challenge, challenge_length);
    }
    // If the server isn't complete yet, we need to wait for its response.
    // This will occur with ANONYMOUS auth, for example, where we send an
    // initial response and are immediately complete.
    if (is_client_ && (TSASL_INVALID == status || TSASL_OK == status)) {
      receive_sasl_message(&status, &res_length);
      if (status != TSASL_COMPLETE) {
        const char *err_msg = "Expect TSASL_COMPLETE or TSASL_OK, but not";
        throw TTransportException(err_msg);
      }
    }
  } catch (const TException &e) {
    // If we hit an exception, that means the Sasl negotiation failed. We
    // explicitly reset the negotiation state here since the caller may retry an
    // open() which would start a new connection negotiation.
    const char *err_msg = e.what();
    LOG_TRACE("throw real TException with err message", K(ret), K(status), K(err_msg));
    reset_sasl_negotiation_state();
    throw e;
  }
}

uint8_t *TSaslTransport::receive_sasl_message(NegotiationStatus *status,
                                              uint32_t *length)
{
  uint8_t messageHeader[HEADER_LENGTH];

  // read header
  transport_->readAll(messageHeader, HEADER_LENGTH);

  // get payload status
  *status = (NegotiationStatus)messageHeader[0];
  if ((*status < TSASL_START) || (*status > TSASL_COMPLETE)) {
    throw TTransportException("invalid sasl status");
  } else if (*status == TSASL_BAD || *status == TSASL_ERROR) {
    throw TTransportException("sasl Peer indicated failure: ");
  }

  // get the length
  *length = decodeInt(messageHeader, STATUS_BYTES);

  // get payload
  if (OB_NOT_NULL(proto_buf_)) {
    allocator_.free(proto_buf_);
    proto_buf_ = nullptr;
  }

  // TODO(bitao): if length is 0, means it could not need proto buf.
  if (0 == *length) {
  } else {
    uint8_t *data = static_cast<uint8_t *>(allocator_.alloc(*length));
    if (OB_ISNULL(data)) {
      throw TTransportException("sasl payload alloc buf failed");
    } else {
      proto_buf_ = data;
      memset(proto_buf_, 0, *length);
    }
    transport_->readAll(proto_buf_, *length);
  }
  return proto_buf_;
}

uint32_t TSaslTransport::read_length() {
  uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

  transport_->readAll(lenBuf, PAYLOAD_LENGTH_BYTES);
  int32_t len = decodeInt(lenBuf, 0);
  if (len < 0) {
    throw TTransportException("Frame size has negative value");
  }
  return static_cast<uint32_t>(len);
}

void TSaslTransport::write_length(uint32_t length) {
  uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

  encodeInt(length, lenBuf, 0);
  transport_->write(lenBuf, PAYLOAD_LENGTH_BYTES);
}

void TSaslTransport::send_sasl_message(const NegotiationStatus status,
                                       const uint8_t *payload,
                                       const uint32_t length, bool flush)
{
  uint8_t messageHeader[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];
  uint8_t dummy = 0;
  if (payload == NULL) {
    payload = &dummy;
  }
  messageHeader[0] = (uint8_t)status;
  encodeInt(length, messageHeader, STATUS_BYTES);
  transport_->write(messageHeader, HEADER_LENGTH);
  transport_->write(payload, length);
  if (OB_LIKELY(flush)) {
    transport_->flush();
  }
}

void TSaslTransport::shrink_buffer() {
  // readEnd() returns the number of bytes already read, i.e. the number of
  // 'junk' bytes taking up space at the front of the memory buffer.
  uint32_t read_end = mem_buf_->readEnd();

  // If the size of the junk space at the beginning of the buffer is too large,
  // and there's no data left in the buffer to read (number of bytes read ==
  // number of bytes written), then shrink the buffer back to the default. We
  // don't want to do this on every read that exhausts the buffer, since the
  // layer above often reads in small chunks, which is why we only resize if
  // there's too much junk. The write and read pointers will eventually catch up
  // after every RPC, so we will always take this path eventually once the
  // buffer becomes sufficiently full.
  //
  // readEnd() may reset the write / read pointers (but only once if there's no
  // intervening read or write between calls), so needs to be called a second
  // time to get their current position.
  if (read_end > DEFAULT_MEM_BUF_SIZE &&
      mem_buf_->writeEnd() == mem_buf_->readEnd()) {
    mem_buf_->resetBuffer(DEFAULT_MEM_BUF_SIZE);
  }
}

} // namespace transport
} // namespace thrift
} // namespace apache
