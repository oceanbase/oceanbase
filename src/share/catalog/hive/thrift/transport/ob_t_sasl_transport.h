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

#ifndef _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_TRANSPORT_H
#define _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_TRANSPORT_H

#include <thrift/transport/TTransport.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/transport/TBufferTransports.h>

#include "ob_t_sasl.h"
#include "lib/allocator/page_arena.h"

namespace apache
{
namespace thrift
{
namespace transport
{

using namespace oceanbase::common;

static constexpr char OB_SASL_CLIENT_TRANS_ALLOCATOR[] = "SaslCTAlloc";
static constexpr char OB_SASL_SERVER_TRANS_ALLOCATOR[] = "SaslSTAlloc";

enum NegotiationStatus {
  TSASL_INVALID = -1,
  TSASL_START = 1,
  TSASL_OK = 2,
  TSASL_BAD = 3,
  TSASL_ERROR = 4,
  TSASL_COMPLETE = 5
};

static const int MECHANISM_NAME_BYTES = 1;
static const int STATUS_BYTES = 1;
static const int PAYLOAD_LENGTH_BYTES = 4;
static const int HEADER_LENGTH = STATUS_BYTES + PAYLOAD_LENGTH_BYTES;

// Default size, in bytes, for the memory buffer used to stage reads.
const int32_t DEFAULT_MEM_BUF_SIZE = 32 * 1024;

/**
 * This transport implements the Simple Authentication and Security Layer (SASL).
 * see: http://www.ietf.org/rfc/rfc2222.txt.  It is based on and depends
 * on the presence of the cyrus-sasl library.
 *
 */
class TSaslTransport : public TVirtualTransport<TSaslTransport>
{
 public:
  /**
   * Constructs a new TSaslTransport to act as a server.
   * SetSaslServer must be called later to initialize the SASL endpoint underlying this
   * transport.
   *
   */
  TSaslTransport(std::shared_ptr<TTransport> transport);

  /**
   * Constructs a new TSaslTransport to act as a client.
   *
   */
  TSaslTransport(std::shared_ptr<TSasl> sasl_client,
                 std::shared_ptr<TTransport> transport);
  /**
   * Destroys the TSasl object.
   */
   virtual ~TSaslTransport() override;

public:
  /**
   * Whether this transport is open.
   */
  virtual bool isOpen() const override;

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open.
   */
  virtual bool peek() override;

  /**
   * Opens the transport for communications.
   *
   * @throws TTransportException if opening failed
   */
  virtual void open() override;

  /**
   * Closes the transport.
   */
  virtual void close() override;

   /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @param len  How many bytes to read
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  uint32_t read(uint8_t *buf, uint32_t len);

  /**
   * Writes the string in its entirety to the buffer.
   *
   * Note: You must call flush() to ensure the data is actually written,
   * and available to be read back in the future.  Destroying a TTransport
   * object does not automatically flush pending data--if you destroy a
   * TTransport object with written but unflushed data, that data may be
   * discarded.
   *
   * @param buf  The data to write out
   * @throws TTransportException if an error occurs
   */
  void write(const uint8_t *buf, uint32_t len);

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  virtual void flush();

public:
  /* Self defined TSaslTransport methods. */
  /**
   * Returns the transport underlying this one
   */
  std::shared_ptr<TTransport> get_underlying_transport() {
    return transport_;
  }

  /**
   * Returns the IANA-registered mechanism name from underlying sasl connection.
   *
   * @throws TTransportException if an error occurs
   */
  const ObString get_mechanism_name() const;

protected:
  /**
   * Create the Sasl context for a server/client connection.
   */
  virtual void setup_sasl_negotiation_state() = 0;

  /**
   * Handle any startup messages.
   */
  virtual void handle_sasl_start_message() = 0;

  /**
   * Reset the negotiation state.
   */
  virtual void reset_sasl_negotiation_state() = 0;

protected:

  /* store the big endian format int to given buffer */
  void encodeInt(uint32_t x, uint8_t *buf, uint32_t offset) {
    *(reinterpret_cast<uint32_t *>(buf + offset)) = htonl(x);
  }

  /* load the big endian format int to given buffer */
  uint32_t decodeInt(uint8_t *buf, uint32_t offset) {
    return ntohl(*(reinterpret_cast<uint32_t *>(buf + offset)));
  }

  /**
   * Performs the SASL negotiation.
   */
  void do_sasl_negotiation();

  /**
   * Read a complete Thrift SASL message.
   *
   * @return The SASL status and payload from this message.
   *    Is valid only to till the next call.
   * @throws TTransportException
   *           Thrown if there is a failure reading from the underlying
   *           transport, or if a status code of BAD or ERROR is encountered.
   */
  uint8_t *receive_sasl_message(NegotiationStatus* status , uint32_t* length);

  /**
   * send message with SASL transport headers.
   * status is put before the payload.
   * If flush is false we delay flushing the underlying transport so
   * that the following message will be in the same packet if necessary.
   */
  void send_sasl_message(const NegotiationStatus status, const uint8_t *payload,
                         const uint32_t length, bool flush = true);

  /**
   * Opens the transport for communications.
   *
   * @return bool Whether the transport was successfully opened
   * @throws TTransportException if opening failed
   */
  uint32_t read_length();

  /**
   * Write the given integer as 4 bytes to the underlying transport.
   *
   * @param length
   *          The length prefix of the next SASL message to write.
   * @throws TTransportException
   *           Thrown if writing to the underlying transport fails.
   */
  void write_length(uint32_t length);

  /// If mem_buf_ is filled with bytes that are already read, and has crossed a size
  /// threshold (see implementation for exact value), resize the buffer to a default value.
  void shrink_buffer();

protected:
  /// Underlying transport
  std::shared_ptr<TTransport> transport_;

  /// Sasl implementation class. This is passed in to the transport constructor
  /// initialized for either a client or a server.
  std::shared_ptr<TSasl> sasl_;

  /// IF true we wrap data in encryption.
  bool should_wrap_;

  /// True if this is a client.
  bool is_client_;

  /// Buffer to hold protocol info.
  uint8_t *proto_buf_;
  ObArenaAllocator allocator_;
  /// Buffer for reading and writing.
  TMemoryBuffer *mem_buf_;
};

} // namespace transport
} // namespace thrift
} // apache

#endif /* _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_TRANSPORT_H */
