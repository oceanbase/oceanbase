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

#include "ob_t_sasl.h"

// DEFINE_bool(force_lowercase_usernames, false, "If true, all principals and usernames are"
//     " mapped to lowercase shortnames before being passed to any components (Ranger, "
//     "admission control) for authorization");
// using boost::algorithm::is_any_of;
// using boost::algorithm::join;
// using boost::algorithm::split;
// using boost::algorithm::to_lower;

namespace apache
{
namespace thrift
{
namespace transport
{
/*---------------------start of SaslContext----------------------------*/

const sasl_callback_t *SaslContext::get_general_callbacks()
{
  return general_callbacks_.empty() ? nullptr : general_callbacks_.data();
}

const sasl_callback_t *SaslContext::get_kerberos_callbacks()
{
  return kerberos_callbacks_.empty() ? nullptr : kerberos_callbacks_.data();
}

void SaslContext::setup_general_callbacks()
{
  general_callbacks_.resize(2);

  general_callbacks_[0].id = SASL_CB_LOG;
  general_callbacks_[0].proc = reinterpret_cast<int (*)()>(&sasl_log_callbacks);
  general_callbacks_[0].context = const_cast<void*>(reinterpret_cast<const void*>(GENERAL_CALLBACKS_CONTEXT_NAME));

  general_callbacks_[1].id = SASL_CB_LIST_END;
  general_callbacks_[1].proc = nullptr;
  general_callbacks_[1].context = nullptr;
}

void SaslContext::setup_kerberos_callbacks()
{
  kerberos_callbacks_.resize(3);

  kerberos_callbacks_[0].id = SASL_CB_LOG;
  kerberos_callbacks_[0].proc = reinterpret_cast<int (*)()>(&sasl_log_callbacks);
  kerberos_callbacks_[0].context = const_cast<void*>(reinterpret_cast<const void*>(KERBEROS_CALLBACKS_CONTEXT_NAME));

  kerberos_callbacks_[1].id = SASL_CB_USER;
  kerberos_callbacks_[1].proc = reinterpret_cast<int (*)()>(&sasl_user_callbacks);
  kerberos_callbacks_[1].context = nullptr;

  kerberos_callbacks_[2].id = SASL_CB_LIST_END;
  kerberos_callbacks_[2].proc = nullptr;
  kerberos_callbacks_[2].context = nullptr;
}

int SaslContext::sasl_log_callbacks(void *context, int level, const char *message)
{
  int result = SASL_OK;
  if (OB_ISNULL(message)) {
    result = SASL_BADPARAM;
  } else {
    switch(level) {
      case SASL_LOG_NONE: // "Don't log anything"
      case SASL_LOG_PASS: // "Traces... including passwords" - don't log!
        break;
      case SASL_LOG_ERR:  // "Unusual errors"
      case SASL_LOG_FAIL: // "Authentication failures"
      case SASL_LOG_WARN: // "Non-fatal warnings"
        LOG_INFO("SASL message log_err/log_fail/log_warn", K(message));
        break;
      case SASL_LOG_NOTE: // "More verbose than WARN"
        break;
      case SASL_LOG_DEBUG: // "More verbose than NOTE"
        LOG_TRACE("SASL message log_debug", K(message));
        break;
      case SASL_LOG_TRACE: // "Traces of internal protocols"
      default:
        LOG_TRACE("SASL Message log_trace/default", K(message));
        break;
    }
  }
  return result;
}

int SaslContext::sasl_user_callbacks(void *context, int, const char **result, unsigned *len)
{
  UNUSED(context);
  // Setting the username to the empty string causes the remote end to use the
  // clients Kerberos principal, which is correct.
  *result = "";
  if (OB_NOT_NULL(len)) {
    *len = 0;
  }
  return SASL_OK;
}

/*---------------------end of TSasl----------------------------*/
/*---------------------start of TSasl----------------------------*/
TSasl::TSasl(const ObString &service, const ObString &server_FQDN,
             const sasl_callback_t *callbacks)
    : service_(service), server_FQDN_(server_FQDN), auth_complete_(false),
      callbacks_(callbacks), conn_(nullptr)
{}

uint8_t *TSasl::unwrap(const uint8_t *incoming, const int offset,
                       const uint32_t len, uint32_t *outLen)
{
  uint32_t outputlen;
  uint8_t *output;
  int result;

  result =
      sasl_decode(conn_, reinterpret_cast<const char *>(incoming), len,
                  const_cast<const char **>(reinterpret_cast<char **>(&output)),
                  &outputlen);
  if (OB_UNLIKELY(result != SASL_OK)) {
    throw SaslException(sasl_errdetail(conn_));
  }
  *outLen = outputlen;
  return output;
}

uint8_t *TSasl::wrap(const uint8_t *outgoing, const int offset,
                     const uint32_t len, uint32_t *outLen)
{
  uint32_t outputlen;
  uint8_t *output;
  int result;

  result =
      sasl_encode(conn_, reinterpret_cast<const char *>(outgoing) + offset, len,
                  const_cast<const char **>(reinterpret_cast<char **>(&output)),
                  &outputlen);
  if (OB_UNLIKELY(result != SASL_OK)) {
    throw SaslException(sasl_errdetail(conn_));
  }
  *outLen = outputlen;
  return output;
}

void TSasl::dispose_sasl_context()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(conn_)) {
    sasl_dispose(&conn_);
    conn_ = nullptr;
  }
}
/*---------------------end of TSasl----------------------------*/
/*---------------------start of TSaslClient----------------------------*/
TSaslClient::TSaslClient(const ObString &service, const ObString &server_FQDN,
                         const sasl_callback_t *callbacks, const ObString &mechanisms,
                         const ObString &authentication_id)
    : TSasl(service, server_FQDN,
            callbacks != nullptr
                ? callbacks
                : SaslContext::instance().get_kerberos_callbacks()),
      client_started_(false), mech_list_(mechanisms),
      authorization_id_(authentication_id),
      allocator_(OB_SASL_CLIENT_ALLOCATOR)
{}

TSaslClient::TSaslClient(const ObString &service, const ObString &server_FQDN)
    : TSaslClient(service, server_FQDN, nullptr, KERBEROS_MECHANISM, "")
{}

TSaslClient::~TSaslClient()
{
  client_started_ = false;
  chosen_mech_.reset();
  mech_list_.reset();
  allocator_.reset();
}

void TSaslClient::setup_sasl_context() {
  if (OB_NOT_NULL(conn_)) {
    throw SaslClientImplException("Connection should be null");
  } else {
    int result = sasl_client_new(service_.ptr(), server_FQDN_.ptr(), nullptr,
                                 nullptr, callbacks_, 0, &conn_);
    if (OB_UNLIKELY(SASL_OK != result)) {
      if (OB_NOT_NULL(conn_)) {
        throw SaslClientImplException(sasl_errdetail(conn_));
      } else {
        throw SaslClientImplException(sasl_errstring(result, NULL, NULL));
      }
    }
  }
}

void TSaslClient::reset_sasl_context() {
  client_started_ = false;
  auth_complete_ = false;
  dispose_sasl_context();
}

/* Evaluates the challenge data and generates a response. */
uint8_t *TSaslClient::evaluate_challenge_or_response(const uint8_t *challenge,
                                                     const uint32_t len,
                                                     uint32_t *res_len)
{
  int ret = OB_SUCCESS;

  sasl_interact_t *client_interact = nullptr;
  uint8_t *out = nullptr;
  uint32_t outlen=0;
  uint32_t result;
  char *mech_using;

  if (OB_LIKELY(!client_started_)) {
    result = sasl_client_start(
        conn_, mech_list_.ptr(),
        &client_interact, /* filled in if an interaction is needed */
        const_cast<const char **>(reinterpret_cast<char **>(&out)), /* filled in on success */
        &outlen, /* filled in on success */
        const_cast<const char **>(&mech_using));
    client_started_ = true;
    // TODO(bitao): check this is correct
    if (OB_LIKELY(SASL_OK == result || SASL_CONTINUE == result)) {
      ObString tmp_mech(mech_using);
      LOG_TRACE("get temp chonse mech", K(ret), K(tmp_mech), K(mech_list_), K(chosen_mech_), K(mech_using));
      if (OB_FAIL(ob_write_string(allocator_, tmp_mech, chosen_mech_, true))) {
        throw SaslClientImplException("Failed to set chosen mechnism");
      } else {
        LOG_TRACE("set chosen mechnism", K(ret), K(tmp_mech), K(chosen_mech_));
      }
    }
  } else {
    if (OB_LIKELY(len  > 0)) {
      result = sasl_client_step(
          conn_, /* our context */
          reinterpret_cast<const char *>(
              challenge),   /* the data from the server */
          len,              /* its length */
          &client_interact, /* this should be unallocated and NULL */
          const_cast<const char **>(reinterpret_cast<char **>(&out)), /* filled in on success */
          &outlen);         /* filled in on success */
    } else {
      result = SASL_CONTINUE;
    }
  }

  if (OB_LIKELY(SASL_OK == result)) {
    auth_complete_ = true;
  } else if (OB_UNLIKELY(SASL_CONTINUE != result)) {
    throw SaslClientImplException(sasl_errdetail(conn_));
  }
  *res_len = outlen;
  return out;
}

/* Returns the IANA-registered mechanism name of this SASL client. */
const ObString TSaslClient::get_mechanism_name() const
{
  return chosen_mech_;
}

/* Determines whether this mechanism has an optional initial response. */
bool TSaslClient::has_initial_response()
{
  return true;
}

void TSaslClient::sasl_init(const sasl_callback_t *callbacks) {
  if (OB_LIKELY(SaslContext::instance().is_inited())) {
    LOG_INFO("sasl client already initied, skip to re-init");
  } else {
    int result = sasl_client_init(callbacks);
    if (OB_UNLIKELY(result != SASL_OK)) {
      throw SaslClientImplException(sasl_errstring(result, nullptr, nullptr));
    } else {
      LOG_INFO("sasl client first init");
      SaslContext::instance().mark_inited();
    }
  }
}

void TSaslClient::setup_client_with_kerberos()
{
  SaslContext::instance().refresh_kerberos_context();

  sasl_init(SaslContext::instance().get_general_callbacks());
}

/* Retrieves the negotiated property */
const ObString TSaslClient::get_negotiate_property(const ObString &prop_name) const
{
  return ObString::make_empty_string();
}

/*---------------------end of TSaslClient----------------------------*/
/*---------------------start of TSaslServer----------------------------*/
TSaslServer::TSaslServer(const ObString &service, const ObString &server_FQDN,
                         const ObString &user_realm, unsigned flags,
                         const sasl_callback_t *callbacks)
    : TSasl(service, server_FQDN, callbacks), user_realm_(user_realm),
      flags_(flags), server_started_(false),
      allocator_(OB_SASL_SERVER_ALLOCATOR)
{}

TSaslServer::~TSaslServer()
{
  user_realm_.reset();
  server_started_ = false;
  allocator_.reset();
}

void TSaslServer::setup_sasl_context()
{
  int result = sasl_server_new(
      service_.ptr(),
      server_FQDN_.length() == 0 ? nullptr : server_FQDN_.ptr(),
      user_realm_.length() == 0 ? nullptr : user_realm_.ptr(),
      nullptr,
      nullptr,
      callbacks_,
      flags_,
      &conn_);
  if (OB_UNLIKELY(SASL_OK != result)) {
    if (OB_NOT_NULL(conn_)) {
      throw SaslServerImplException(sasl_errdetail(conn_));
    } else {
      throw SaslServerImplException(sasl_errstring(result, nullptr, nullptr));
    }
  }
}

void TSaslServer::reset_sasl_context() {
  server_started_ = false;
  auth_complete_ = false;
  dispose_sasl_context();
}

uint8_t *TSaslServer::evaluate_challenge_or_response(const uint8_t *response,
                                                     const uint32_t len,
                                                     uint32_t *res_len)
{
  uint32_t result;
  uint8_t *out = nullptr;
  uint32_t outlen = 0;

  if (OB_LIKELY(!server_started_)) {
    result = sasl_server_start(
        conn_, reinterpret_cast<const char *>(response), NULL, 0,
        const_cast<const char **>(reinterpret_cast<char **>(&out)), &outlen);
  } else {
    result = sasl_server_step(conn_, reinterpret_cast<const char*>(response), len,
        const_cast<const char**>(reinterpret_cast<char**>(&out)), &outlen);
  }

  if (OB_LIKELY(SASL_OK == result)) {
    auth_complete_ = true;
  } else {
    throw SaslServerImplException(sasl_errdetail(conn_));
  }
  server_started_ = true;
  *res_len = outlen;
  return out;
}

const ObString TSaslServer::get_mechanism_name() const
{
  int ret = OB_SUCCESS;
  const char *mech_name = nullptr;
  int result = sasl_getprop(conn_, SASL_MECHNAME,
                            reinterpret_cast<const void **>(&mech_name));
  if (OB_UNLIKELY(SASL_OK != result)) {
    throw SaslServerImplException(sasl_errstring(result, nullptr, nullptr));
  }
  return ObString(mech_name);
}

void TSaslServer::sasl_init(const sasl_callback_t *callbacks, const char *appname)
{
  if (OB_LIKELY(SaslContext::instance().is_inited())) {
    LOG_INFO("sasl server already inited, skip to re-init");
  } else {
    int result = sasl_server_init(callbacks, appname);
    if (OB_UNLIKELY(result != SASL_OK)) {
      throw SaslServerImplException(sasl_errstring(result, nullptr, nullptr));
    } else {
      LOG_INFO("sasl server first init");
      SaslContext::instance().mark_inited();
    }
  }
}

/*---------------------end of TSaslServer----------------------------*/

} // namespace transport
} // namespace thrift
} // namespace apache