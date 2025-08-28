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

#ifndef _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_H
#define _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_H

#ifdef _WIN32
#include <sasl.h>
#define MD5_CTX OPENSSL_MD5_CTX
#include <saslplug.h>
#undef MD5_CTX
#include <saslutil.h>
#else /* _WIN32 */
#include <cyrus-sasl/sasl.h>
#define MD5_CTX OPENSSL_MD5_CTX
#include <cyrus-sasl/saslplug.h>
#undef MD5_CTX
#include <cyrus-sasl/saslutil.h>
#endif

#include <thrift/transport/TTransportException.h>

#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_mutex.h"
#include "lib/string/ob_string.h"

namespace apache
{
namespace thrift
{
namespace transport
{

using namespace oceanbase::common;

static constexpr char OB_SASL_CLIENT_ALLOCATOR[] = "SaslCAlloc";
static constexpr char OB_SASL_SERVER_ALLOCATOR[] = "SaslSAlloc";

static const char *const KERBEROS_MECHANISM = "GSSAPI";
static const char *const GENERAL_CALLBACKS_CONTEXT_NAME = "General";
static const char *const KERBEROS_CALLBACKS_CONTEXT_NAME = "KerBeros";

// typedef oceanbase::lib::ObLockGuard<oceanbase::lib::ObMutex> LockGuard;

class SaslContext
{
public:
  SaslContext() = default;
  ~SaslContext() { cleanup(); }

public:
  static SaslContext &instance()
  {
    static SaslContext sasl_ctx;
    return sasl_ctx;
  }

  void mark_inited() {
    inited_ = true;
  }

  bool is_inited() { return inited_; }

  // Initialized or refresh SASL context（thread safe）
  void refresh_kerberos_context() {
    // Clean older resources.
    cleanup();

    // Construct new context.
    setup_general_callbacks();
    setup_kerberos_callbacks();
  }

  const sasl_callback_t *get_general_callbacks();
  const sasl_callback_t *get_kerberos_callbacks();

private:
  void cleanup()
  {
    general_callbacks_.clear();
    kerberos_callbacks_.clear();
  }
  void setup_general_callbacks();
  void setup_kerberos_callbacks();
  static int sasl_log_callbacks(void *context, int level, const char *message);
  static int sasl_user_callbacks(void *context, int, const char **result, unsigned *len);

private:
  std::vector<sasl_callback_t> general_callbacks_;
  std::vector<sasl_callback_t> kerberos_callbacks_;
  bool inited_ = false;
private:
  DISALLOW_COPY_AND_ASSIGN(SaslContext);
};


class SaslException : public TTransportException
{
public:
  SaslException(const char *msg) : TTransportException(msg) {}
};

/**
 * These classes implement the Simple Authentication and Security Layer (SASL)
 * authentication mechanisms.  see: http://www.ietf.org/rfc/rfc2222.txt.
 * They are mostly wrappers for the cyrus-sasl library routines.
 */
class TSasl
{
public:
  /* Determines whether the authentication exchange has completed. */
  bool is_complete() { return auth_complete_; }
  /*
   * Called once per application to free resources.`
   * Note that there is no distinction in the sasl library between being done
   * with servers or done with clients.  Internally the library maintains a
   * which is being used.  A call to SaslDone should only happen after all
   * clients and servers are finished.
   */
   // TODO(bitao): find a place to correct to done sasl.
  static void client_done_sasl()
  {
    sasl_client_done();
  }

  static void server_done_sasl()
  {
    sasl_server_done();
  }

public:
   /*
    * Unwraps a received byte array.
    * Returns a buffer for unwrapped result, and sets
    * 'len' to the buffer's length. The buffer is only valid until the next call,
    * or until the client is closed.
    */
  uint8_t *unwrap(const uint8_t *incoming, const int offset, const uint32_t len,
                  uint32_t *outlen);

  /*
   * Wraps a byte array to be sent.
   * Returns a buffer of wrapped result, and sets
   * 'len' to the buffer's length. The buffer is only valid until the next call,
   * or until the client is closed.
   */
  uint8_t *wrap(const uint8_t *outgoing, int offset, const uint32_t len,
                uint32_t *outlen);

public:
  virtual ~TSasl() { dispose_sasl_context(); }

  /* Setup the SASL negotiation state. */
  virtual void setup_sasl_context() = 0;

  /* Reset the SASL negotiation state. */
  virtual void reset_sasl_context() = 0;

  /* Evaluates the challenge or response data and generates a response. */
  virtual uint8_t *evaluate_challenge_or_response(const uint8_t *challenge,
                                                  const uint32_t len,
                                                  uint32_t *resLen) = 0;
  /* Returns the IANA-registered mechanism name. */
  virtual const ObString get_mechanism_name() const = 0;

  /* Determines whether this mechanism has an optional initial response. */
  virtual bool has_initial_response() { return false; }

protected:
  /* Dispose of the SASL state. It is called once per connection as a part of
   * teardown. */
   void dispose_sasl_context();

protected:
  TSasl(const ObString &service, const ObString &server_FQDN, const sasl_callback_t *callbacks);

protected:
  /* Name of service */
  ObString service_;

  /* FQDN of server in use or the server to connect to */
  ObString server_FQDN_;

  /* Authorization is complete. */
  bool auth_complete_;

  /*
   * Callbacks to provide to the Cyrus-SASL library. Not owned. The user of the
   * class must ensure that the callbacks live as long as the TSasl instance in
   * use.
   */
  const sasl_callback_t *callbacks_;

  /* Sasl Connection. */
  sasl_conn_t *conn_;
private:
  DISALLOW_COPY_AND_ASSIGN(TSasl);
};

class SaslClientImplException : public SaslException
{
public:
  SaslClientImplException(const char *err_msg) : SaslException(err_msg) {}
};

/* Client sasl implementation class. */
class TSaslClient : public TSasl
{
public:
  TSaslClient(const ObString &service, const ObString &server_FQDN,
              const sasl_callback_t *callbacks, const ObString &mechanisms,
              const ObString &authentication_id);
  TSaslClient(const ObString &service, const ObString &server_FQDN);

  virtual ~TSaslClient() override;

  /* Evaluates the challenge data and generates a response. */
  virtual uint8_t *evaluate_challenge_or_response(const uint8_t *challenge,
                                                   const uint32_t len,
                                                   uint32_t *outlen) override;

  /* Returns the IANA-registered mechanism name of this SASL client. */
  virtual const ObString get_mechanism_name() const override;

  /* Setup the SASL client negotiation state. */
  virtual void setup_sasl_context() override;

  /* Reset the SASL client negotiation state. */
  virtual void reset_sasl_context() override;

  /* Determines whether this mechanism has an optional initial response. */
  virtual bool has_initial_response() override;
public:
  static void sasl_init(const sasl_callback_t *callbacks);
  static void setup_client_with_kerberos();
  /* Retrieves the negotiated property */
  const ObString get_negotiate_property(const ObString &prop_name) const;
private:
  /* true if sasl_client_start has been called. */
  bool client_started_;

  /* The chosen mechanism. */
  ObString chosen_mech_;

  /* List of possible mechanisms. */
  ObString mech_list_;
  ObString authorization_id_;
  ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(TSaslClient);
};

class SaslServerImplException : public SaslException
{
public:
  SaslServerImplException(const char *err_msg) : SaslException(err_msg) {}
};

/* Server sasl implementation class. */
class TSaslServer : public TSasl
{
public:
  TSaslServer(const ObString &service, const ObString &server_FQDN,
              const ObString &user_realm, unsigned flags,
              const sasl_callback_t *callbacks);

  virtual ~TSaslServer() override;

  /*
   * This initializes the sasl server library and should be called onece per
   * application. Note that the caller needs to ensure the life time of
   * 'callbacks' and 'appname' is beyond that of this object.
   */
  /* Setup the SASL server negotiation state. */
  virtual void setup_sasl_context() override;

  /* Reset the SASL server negotiation state. */
  virtual void reset_sasl_context() override;

  /* Evaluates the response data and generates a challenge. */
  virtual uint8_t *evaluate_challenge_or_response(const uint8_t *challenge,
                                                  const uint32_t len,
                                                  uint32_t *reslen) override;

  /* Returns the active IANA-registered mechanism name of this SASL server. */
  virtual const ObString get_mechanism_name() const override;

public:
  static void sasl_init(const sasl_callback_t *callbacks, const char *appname);

private:
  /* The domain of the user agent */
  ObString user_realm_;

  /* Flags to pass down to the SASL library */
  unsigned flags_;

  /* true if sasl_server_start has been called. */
  bool server_started_;
  ObArenaAllocator allocator_;
};

} // namespace transport
} // namespace thrift
} // namespace apache

#endif /* _SHARE_CATALOG_HIVE_THRIFT_TRANSPORT_T_SASL_H */
