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

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits.h>
#include <memory>
#include <mutex>
#include <string>
#include <strings.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define USING_LOG_PREFIX TRANS
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/session/ob_sql_session_mgr.h"
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "storage/tx/ob_tx_hotspot_define.h"
#include "../../close_modules/hotspot_group_commit/sql/ob_sql_group_commit_aggregator.h"
#endif

namespace oceanbase
{
namespace unittest
{
using namespace common;

static const std::string &run_prefix()
{
  static const std::string prefix = std::string("test_hotspot_rollback_")
      + std::to_string(ObTimeUtility::current_time()) + "_" + std::to_string(getpid());
  return prefix;
}

#if defined(OB_HOTSPOT_GROUP_COMMIT) && defined(ENABLE_DEBUG_LOG)
namespace
{

constexpr int CLIENT_COUNT = 32;
constexpr int MAX_WAVES = 8;
constexpr int SOCKET_TIMEOUT_SECONDS = 10;
constexpr int WAVE_TIMEOUT_SECONDS = 30;
constexpr int64_t INITIAL_BALANCE = 1000000;
constexpr int64_t REJECT_AMOUNT = INITIAL_BALANCE + 1;

unsigned int packet_errno(const int ret)
{
  return static_cast<uint16_t>(ob_errpkt_errno(ret, false /* MySQL mode */));
}

// No proxy retries, automatic reconnection, or connection-close rollback is allowed to
// hide the result of the SQL group end-trans operation under test.
class SQLClient
{
public:
  SQLClient() : initialized_(false), connected_(false), stmt_(nullptr), socket_(-1) {}
  ~SQLClient()
  {
    const int cancel_fd = socket_.exchange(-1, std::memory_order_acq_rel);
    if (cancel_fd >= 0) {
      (void)close(cancel_fd);
    }
    if (nullptr != stmt_) {
      mysql_stmt_close(stmt_);
    }
    if (initialized_) {
      mysql_close(&mysql_);
    }
  }

  int connect(const std::string &host, const int port, const std::string &user)
  {
    if (nullptr == mysql_init(&mysql_)) {
      return OB_ALLOCATE_MEMORY_FAILED;
    }
    initialized_ = true;
    unsigned int timeout = SOCKET_TIMEOUT_SECONDS;
    my_bool reconnect = 0;
    if (0 != mysql_options(&mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout)
        || 0 != mysql_options(&mysql_, MYSQL_OPT_READ_TIMEOUT, &timeout)
        || 0 != mysql_options(&mysql_, MYSQL_OPT_WRITE_TIMEOUT, &timeout)) {
      return remember_mysql_error();
    }
#ifdef OB_BUILD_TDE_SECURITY
    int64_t ssl_enforce = 1;
    if (0 != mysql_options(&mysql_, MYSQL_OPT_SSL_ENFORCE, &ssl_enforce)) {
      return remember_mysql_error();
    }
#endif
    if (nullptr == mysql_real_connect(&mysql_, host.c_str(), user.c_str(), "", "test",
                                      port, nullptr, 0)) {
      return remember_mysql_error();
    }
    connected_ = true;
    const int cancel_fd = dup(static_cast<int>(mysql_get_socket(&mysql_)));
    if (cancel_fd < 0) {
      error_message_ = "cannot duplicate the connected socket for bounded cancellation";
      return OB_ERR_SYS;
    }
    socket_.store(cancel_fd, std::memory_order_release);
    if (0 != mysql_options(&mysql_, MYSQL_OPT_RECONNECT, &reconnect)) {
      return remember_mysql_error();
    }
    return OB_SUCCESS;
  }

  int execute(const std::string &sql)
  {
    error_message_.clear();
    if (!connected_) {
      error_message_ = "not connected; query=" + sql;
      return OB_NOT_INIT;
    }
    if (0 != mysql_real_query(&mysql_, sql.c_str(), sql.size())) {
      return remember_mysql_error(sql);
    }
    MYSQL_RES *res = mysql_store_result(&mysql_);
    if (nullptr != res) {
      mysql_free_result(res);
    } else if (mysql_field_count(&mysql_) != 0) {
      return remember_mysql_error(sql);
    }
    return OB_SUCCESS;
  }

  int scalar(const std::string &sql, int64_t &value)
  {
    error_message_.clear();
    if (0 != mysql_real_query(&mysql_, sql.c_str(), sql.size())) {
      return remember_mysql_error(sql);
    }
    MYSQL_RES *res = mysql_store_result(&mysql_);
    if (nullptr == res) {
      return remember_mysql_error(sql);
    }
    int ret = OB_ERR_UNEXPECTED;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (mysql_num_fields(res) == 1 && mysql_num_rows(res) == 1
        && nullptr != row && nullptr != row[0]) {
      char *end = nullptr;
      errno = 0;
      value = strtoll(row[0], &end, 10);
      if (errno != ERANGE && nullptr != end && end != row[0] && '\0' == *end) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCCESS != ret) {
      remember_result_error(sql, res, "expected exactly one non-null int64 cell");
    }
    mysql_free_result(res);
    return ret;
  }

  int set_session_int(const char *name, const int64_t expected)
  {
    int ret = execute("SET SESSION " + std::string(name) + "=" + std::to_string(expected));
    int64_t actual = 0;
    if (OB_SUCCESS == ret) {
      ret = scalar("SELECT @@session." + std::string(name), actual);
      if (OB_SUCCESS == ret && actual != expected) {
        error_message_ = "session variable " + std::string(name) + ": expected="
            + std::to_string(expected) + ", actual=" + std::to_string(actual);
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  int parameter_value(const char *name, const uint64_t tenant_id, std::string &value)
  {
    error_message_.clear();
    // SHOW PARAMETERS intentionally hides unchanged underscore parameters, even
    // with LIKE. Read the same real tenant config VT without SHOW's visibility filter.
    const std::string sql = "SELECT name AS parameter_name, value AS parameter_value "
        "FROM oceanbase.__all_virtual_tenant_parameter_stat WHERE tenant_id="
        + std::to_string(tenant_id) + " AND name='" + name + "'";
    if (0 != mysql_real_query(&mysql_, sql.c_str(), sql.size())) {
      return remember_mysql_error(sql);
    }
    MYSQL_RES *res = mysql_store_result(&mysql_);
    if (nullptr == res) {
      return remember_mysql_error(sql);
    }
    int ret = OB_ERR_UNEXPECTED;
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    MYSQL_ROW row = mysql_fetch_row(res);
    if (mysql_num_fields(res) == 2 && mysql_num_rows(res) == 1 && nullptr != fields
        && 0 == strcasecmp(fields[0].name, "parameter_name")
        && 0 == strcasecmp(fields[1].name, "parameter_value")
        && nullptr != row && nullptr != row[0] && nullptr != row[1]
        && 0 == strcmp(row[0], name)) {
      value = row[1];
      ret = OB_SUCCESS;
    } else {
      remember_result_error(sql, res, "expected one exact-name, tenant-scoped parameter row");
    }
    mysql_free_result(res);
    return ret;
  }

  int prepare_update(const std::string &table, int64_t &amount, int64_t &key,
                     const bool group_commit = true)
  {
    const std::string hint = group_commit
        ? "/*+ GROUP_COMMIT(TRUE) TRANS_PARAM('DISABLE_TX_FREE_ROUTE' 'true') */ " : "";
    const std::string sql = "UPDATE " + hint + table
        + " SET value=value-? WHERE id=? AND value>=?";
    stmt_ = mysql_stmt_init(&mysql_);
    if (nullptr == stmt_ || 0 != mysql_stmt_prepare(stmt_, sql.c_str(), sql.size())) {
      return remember_stmt_error();
    }
    if (mysql_stmt_param_count(stmt_) != 3) {
      error_message_ = "prepared update expected 3 parameters, actual="
          + std::to_string(mysql_stmt_param_count(stmt_));
      return OB_ERR_UNEXPECTED;
    }
    std::memset(binds_, 0, sizeof(binds_));
    for (int i = 0; i < 3; ++i) {
      lengths_[i] = sizeof(int64_t);
      nulls_[i] = 0;
      binds_[i].buffer_type = ::MYSQL_TYPE_LONGLONG;
      binds_[i].buffer = 1 == i ? static_cast<void *>(&key) : static_cast<void *>(&amount);
      binds_[i].buffer_length = sizeof(int64_t);
      binds_[i].length = &lengths_[i];
      binds_[i].is_null = &nulls_[i];
#ifdef OB_BUILD_ORACLE_PL
      // Match ObMySQLPreparedParam's contract with the bundled OB client.
      binds_[i].mysql = &mysql_;
#endif
    }
    return OB_SUCCESS;
  }

  unsigned int execute_update(int64_t &affected, std::string &message)
  {
    affected = -1;
    if (!connected_) {
      message = "transport=standard_PS_packet; client_local_fallback=true; not connected";
      return packet_errno(OB_NOT_INIT);
    }
    const auto *methods = mysql_.methods;
    if (nullptr == stmt_ || nullptr == methods || nullptr == methods->db_command
        || nullptr == methods->db_read_query_result || stmt_->param_count != 3
        || stmt_->field_count != 0 || stmt_->stmt_id > 0xffffffffUL
        || mysql_.status != ::MYSQL_STATUS_READY || mysql_field_count(&mysql_) != 0) {
      message = "transport=standard_PS_packet; client_local_fallback=true; "
          "protocol setup requires a prepared three-int64 UPDATE and an idle, zero-field connection; "
          "stmt_id=" + (nullptr == stmt_ ? std::string("null") : std::to_string(stmt_->stmt_id))
          + ", field_count=" + std::to_string(mysql_field_count(&mysql_))
          + ", connection_status=" + std::to_string(static_cast<int>(mysql_.status));
      return packet_errno(OB_ERR_UNEXPECTED);
    }
    // Fixed test-only COM_STMT_EXECUTE payload. The real prepared statement ID
    // selects the server PS cache entry; no SQL text or synthetic group state is sent.
    char payload[41];
    size_t pos = 0;
    auto append_le = [&payload, &pos](uint64_t value, const unsigned int bytes) {
      for (unsigned int i = 0; i < bytes; ++i) {
        payload[pos++] = static_cast<char>(value & 0xff);
        value >>= 8;
      }
    };
    append_le(stmt_->stmt_id, 4);
    append_le(0, 1); // flags: no cursor/array binding
    append_le(1, 4); // standard iteration count; server accepts the no-checksum form
    append_le(0, 1); // one-byte null bitmap: all three values are non-null
    append_le(1, 1); // bind all parameter types on this request
    for (int i = 0; i < 3; ++i) {
      if (nullptr == binds_[i].buffer || binds_[i].buffer_type != ::MYSQL_TYPE_LONGLONG
          || binds_[i].buffer_length != sizeof(int64_t) || binds_[i].is_unsigned || nulls_[i]) {
        message = "transport=standard_PS_packet; client_local_fallback=true; "
            "only three non-null signed int64 parameters are supported";
        return packet_errno(OB_ERR_UNEXPECTED);
      }
      append_le(::MYSQL_TYPE_LONGLONG, 1);
      append_le(0, 1); // signed
    }
    for (int i = 0; i < 3; ++i) {
      int64_t value = 0;
      std::memcpy(&value, binds_[i].buffer, sizeof(value));
      append_le(static_cast<uint64_t>(value), 8);
    }
    if (pos != sizeof(payload)) {
      message = "transport=standard_PS_packet; client_local_fallback=true; invalid payload size";
      return packet_errno(OB_ERR_UNEXPECTED);
    }
    const int bind_rc = mysql_stmt_bind_param(stmt_, binds_);
    int send_rc = -1;
    int read_rc = -1;
    if (bind_rc == 0) {
      // Never call mysql_stmt_execute first and retry: a working v1 would execute twice.
      // The provider preserves this connection's negotiated OB20/TLS framing.
      send_rc = methods->db_command(&mysql_, ::COM_STMT_EXECUTE, payload, sizeof(payload),
                                    1 /* skip_check: read the reply below */, stmt_);
      if (send_rc == 0) {
        read_rc = methods->db_read_query_result(&mysql_);
      }
    }
    const unsigned int stmt_error = mysql_stmt_errno(stmt_);
    const unsigned int conn_error = mysql_errno(&mysql_);
    const bool invalid_response = mysql_field_count(&mysql_) != 0
        || mysql_.status != ::MYSQL_STATUS_READY
        || 0 != (mysql_.server_status & SERVER_MORE_RESULTS_EXIST);
    const bool failed = bind_rc != 0 || send_rc != 0 || read_rc != 0 || invalid_response;
    const bool local_fallback = invalid_response
        || (failed && conn_error == 0 && (bind_rc == 0 || stmt_error == 0));
    message = "transport=standard_PS_packet; command=COM_STMT_EXECUTE; stmt_id="
        + std::to_string(stmt_->stmt_id) + ", payload_bytes=" + std::to_string(pos)
        + ", bind_rc=" + std::to_string(bind_rc) + ", send_rc="
        + (bind_rc == 0 ? std::to_string(send_rc) : std::string("not_called")) + ", read_rc="
        + (send_rc == 0 ? std::to_string(read_rc) : std::string("not_called"))
        + ", field_count=" + std::to_string(mysql_field_count(&mysql_))
        + ", connection_status=" + std::to_string(static_cast<int>(mysql_.status))
        + ", server_status=" + std::to_string(mysql_.server_status)
        + ", invalid_response=" + (invalid_response ? "true" : "false")
        + ", stmt_errno=" + std::to_string(stmt_error)
        + ", stmt_sqlstate=" + mysql_stmt_sqlstate(stmt_)
        + ", stmt_message=" + mysql_stmt_error(stmt_)
        + ", conn_errno=" + std::to_string(conn_error)
        + ", conn_sqlstate=" + mysql_sqlstate(&mysql_)
        + ", conn_message=" + mysql_error(&mysql_)
        + ", client_local_fallback=" + (local_fallback ? "true" : "false");
    if (invalid_response) {
      // A malformed protocol state must never match an expected server terminal
      // error. Preserve the real errno above, but fail this fixture locally.
      return packet_errno(OB_ERR_UNEXPECTED);
    } else if (failed) {
      // The provider parses the real DML ERR packet into the connection, not the
      // otherwise unexecuted MYSQL_STMT result fields. Zero-errno failures stay explicit.
      return conn_error != 0 ? conn_error
          : (bind_rc != 0 && stmt_error != 0 ? stmt_error : packet_errno(OB_ERR_UNEXPECTED));
    }
    affected = static_cast<int64_t>(mysql_affected_rows(&mysql_));
    return 0;
  }

  bool in_transaction() const { return 0 != (mysql_.server_status & SERVER_STATUS_IN_TRANS); }
  unsigned long session_id() const { return mysql_thread_id(const_cast<MYSQL *>(&mysql_)); }
  const std::string &error_message() const { return error_message_; }
  void interrupt()
  {
    // The duplicate remains owned until after join, even if MYSQL closed its
    // original descriptor. A reused numeric FD cannot target an unrelated socket.
    const int fd = socket_.load(std::memory_order_acquire);
    if (fd >= 0) {
      (void)shutdown(fd, SHUT_RDWR);
    }
  }

private:
  int remember_mysql_error(const std::string &query = std::string())
  {
    const unsigned int error = mysql_errno(&mysql_);
    error_message_ = "mysql_errno=" + std::to_string(error) + ", sqlstate="
        + mysql_sqlstate(&mysql_) + ", field_count=" + std::to_string(mysql_field_count(&mysql_))
        + ", message=" + mysql_error(&mysql_);
    if (!query.empty()) {
      error_message_ += "; query=" + query;
    }
    return 0 == error ? OB_ERR_UNEXPECTED : -static_cast<int>(error);
  }
  void remember_result_error(const std::string &query, MYSQL_RES *res, const char *reason)
  {
    error_message_ = std::string(reason) + "; mysql_errno=" + std::to_string(mysql_errno(&mysql_))
        + ", sqlstate=" + mysql_sqlstate(&mysql_) + ", rows="
        + std::to_string(mysql_num_rows(res)) + ", fields="
        + std::to_string(mysql_num_fields(res)) + ", field_names=[";
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    for (unsigned int i = 0; nullptr != fields && i < mysql_num_fields(res) && i < 16; ++i) {
      if (i > 0) {
        error_message_ += ",";
      }
      error_message_ += nullptr == fields[i].name ? "<null>" : fields[i].name;
    }
    error_message_ += "]; query=" + query;
  }
  int remember_stmt_error()
  {
    if (nullptr == stmt_) {
      return remember_mysql_error();
    }
    const unsigned int error = mysql_stmt_errno(stmt_);
    error_message_ = mysql_stmt_error(stmt_);
    return 0 == error ? OB_ERR_UNEXPECTED : -static_cast<int>(error);
  }

  MYSQL mysql_;
  bool initialized_;
  bool connected_;
  MYSQL_STMT *stmt_;
  MYSQL_BIND binds_[3];
  unsigned long lengths_[3];
  my_bool nulls_[3];
  std::atomic<int> socket_;
  std::string error_message_;
};

class ScopedEvent
{
public:
  explicit ScopedEvent(const char *name) : name_(name), item_(nullptr)
  {
    DLIST_FOREACH_NORET(event, EventTable::global_item_list()) {
      if (nullptr != event->item_.name_ && 0 == strcasecmp(event->item_.name_, name)) {
        item_ = &event->item_;
        saved_ = *item_;
        break;
      }
    }
  }
  ~ScopedEvent()
  {
    if (nullptr != item_) {
      EXPECT_EQ(OB_SUCCESS, EventTable::set_event(name_, saved_));
    }
  }
  bool registered() const { return nullptr != item_; }
  int arm(const int error, const int64_t occur)
  {
    if (occur <= 0) {
      return OB_INVALID_ARGUMENT;
    }
    EventItem event;
    event.error_code_ = error;
    event.occur_ = occur;
    event.trigger_freq_ = 0;
    return EventTable::set_event(name_, event);
  }
  int64_t remaining() const { return nullptr == item_ ? -1 : ATOMIC_LOAD(&item_->occur_); }
private:
  const char *name_;
  EventItem *item_;
  EventItem saved_;
};

int wait_for_sql_session_state(sql::ObSQLSessionMgr &manager, const uint32_t session_id,
                               const uint64_t tenant_id, const bool expect_active,
                               int64_t &tx_id, bool &in_transaction)
{
  sql::ObSessionGetterGuard session_guard(manager, session_id);
  sql::ObSQLSessionInfo *session = nullptr;
  int ret = session_guard.get_session(session);
  if (OB_SUCCESS != ret) {
    return ret;
  } else if (nullptr == session) {
    return OB_ERR_UNEXPECTED;
  }
  const int64_t expire = ObTimeUtility::current_monotonic_time() + 5 * 1000 * 1000;
  while (ObTimeUtility::current_monotonic_time() < expire) {
    const int query_ret = session->get_query_lock().trylock();
    if (OB_SUCCESS == query_ret) {
      const int data_ret = session->get_thread_data_lock().trylock();
      if (OB_SUCCESS == data_ret) {
        if (session->get_effective_tenant_id() != tenant_id) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const transaction::ObTxDesc *desc = session->get_tx_desc();
          tx_id = nullptr == desc ? 0 : desc->get_tx_id().get_id();
          in_transaction = session->is_in_transaction();
        }
        (void)session->get_thread_data_lock().unlock();
      }
      // Release both mutexes before reverting the SessionMgr reference or retrying.
      (void)session->get_query_lock().unlock();
      if (OB_SUCCESS != ret) {
        return ret;
      } else if (OB_SUCCESS == data_ret
                 && (expect_active ? (tx_id > 0 && in_transaction)
                                   : (tx_id == 0 && !in_transaction))) {
        return OB_SUCCESS;
      } else if (OB_SUCCESS != data_ret && OB_EAGAIN != data_ret) {
        return data_ret;
      }
    } else if (OB_EAGAIN != query_ret) {
      return query_ret;
    }
    ob_usleep(1000);
  }
  return OB_TIMEOUT;
}

struct RequestResult
{
  RequestResult()
      : side_key_(0), amount_(0), hot_key_(1), setup_ret_(OB_SUCCESS), sql_error_(0),
        affected_(-1), execute_us_(0), tx_before_(0), tx_after_(-1), post_ret_(OB_SUCCESS),
        in_transaction_after_(true), session_id_(0) {}
  int64_t side_key_;
  int64_t amount_;
  int64_t hot_key_;
  int setup_ret_;
  unsigned int sql_error_;
  int64_t affected_;
  int64_t execute_us_;
  int64_t tx_before_;
  int64_t tx_after_;
  int post_ret_;
  bool in_transaction_after_;
  unsigned long session_id_;
  std::string error_;
};

class RequestWave
{
public:
  RequestWave(const std::string &host, const int port, const std::string &user,
              const std::string &table, const int wave, sql::ObSQLSessionMgr &sessions,
              const uint64_t tenant_id)
      : host_(host), port_(port), user_(user), table_(table), sessions_(sessions), tenant_id_(tenant_id),
        ready_(0), finished_(0), go_(false), cancelled_(false)
  {
    clients_.reserve(CLIENT_COUNT);
    results_.resize(CLIENT_COUNT);
    threads_.reserve(CLIENT_COUNT);
    for (int i = 0; i < CLIENT_COUNT; ++i) {
      clients_.emplace_back(new SQLClient());
      results_[i].side_key_ = 100 + wave * CLIENT_COUNT + i;
      // Rotate the first/primary candidate. A mixed physical group is required;
      // serial executions cannot satisfy the event-consumption assertions.
      results_[i].amount_ = 0 == (i + wave) % 3 ? 1 : REJECT_AMOUNT;
    }
  }
  ~RequestWave()
  {
    cancel();
    for (std::thread &thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    // Connections stay open until all assertions/probes in the test have run.
  }

  void start()
  {
    for (int i = 0; i < CLIENT_COUNT; ++i) {
      threads_.emplace_back([this, i]() { run(i); });
    }
  }
  bool wait_ready()
  {
    std::unique_lock<std::mutex> guard(lock_);
    return cv_.wait_for(guard, std::chrono::seconds(WAVE_TIMEOUT_SECONDS),
                        [this]() { return ready_ == CLIENT_COUNT; });
  }
  void release()
  {
    std::lock_guard<std::mutex> guard(lock_);
    go_ = true;
    cv_.notify_all();
  }
  bool wait_finished()
  {
    std::unique_lock<std::mutex> guard(lock_);
    const bool done = cv_.wait_for(guard, std::chrono::seconds(WAVE_TIMEOUT_SECONDS),
                                  [this]() { return finished_ == CLIENT_COUNT; });
    guard.unlock();
    if (!done) {
      cancel();
    }
    return done;
  }
  const std::vector<RequestResult> &results() const { return results_; }

private:
  void cancel()
  {
    bool pending = false;
    {
      std::lock_guard<std::mutex> guard(lock_);
      cancelled_ = true;
      go_ = true;
      pending = finished_ < static_cast<int>(threads_.size());
      cv_.notify_all();
    }
    if (pending) {
      for (const auto &client : clients_) {
        client->interrupt();
      }
    }
  }
  void run(const int index)
  {
    SQLClient &client = *clients_[index];
    RequestResult &result = results_[index];
    const bool thread_initialized = 0 == mysql_thread_init();
    int ret = thread_initialized ? client.connect(host_, port_, user_) : OB_ERR_UNEXPECTED;
    if (OB_SUCCESS == ret) {
      ret = client.set_session_int("ob_query_timeout", 10000000);
    }
    if (OB_SUCCESS == ret) {
      ret = client.set_session_int("ob_trx_timeout", 60000000);
    }
    if (OB_SUCCESS == ret) {
      ret = client.set_session_int("ob_trx_idle_timeout", 60000000);
    }
    if (OB_SUCCESS == ret) {
      ret = client.prepare_update(table_, result.amount_, result.hot_key_);
    }
    if (OB_SUCCESS == ret) {
      ret = client.execute("BEGIN");
    }
    if (OB_SUCCESS == ret) {
      ret = client.execute("INSERT INTO " + table_ + " VALUES("
          + std::to_string(result.side_key_) + ",0)");
    }
    if (OB_SUCCESS == ret) {
      result.session_id_ = client.session_id();
      bool active = false;
      // Keep the last wire command an INSERT OK (zero fields). The provider ERR
      // path does not clear a preceding SELECT's field metadata. Read the real
      // active descriptor without sending SQL or retaining its reference/locks.
      ret = wait_for_sql_session_state(sessions_, static_cast<uint32_t>(result.session_id_),
                                      tenant_id_, true, result.tx_before_, active);
      if (OB_SUCCESS == ret && (result.tx_before_ <= 0 || !active || !client.in_transaction())) {
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCCESS != ret) {
        result.error_ = "protocol setup: pre-existing active transaction snapshot failed; session="
            + std::to_string(result.session_id_) + ", tx=" + std::to_string(result.tx_before_);
      }
    }
    result.setup_ret_ = ret;
    if (OB_SUCCESS != ret && result.error_.empty()) {
      result.error_ = client.error_message();
    }
    bool execute = false;
    {
      std::unique_lock<std::mutex> guard(lock_);
      ++ready_;
      cv_.notify_all();
      cv_.wait(guard, [this]() { return go_; });
      execute = !cancelled_ && OB_SUCCESS == ret;
    }
    if (execute) {
      const int64_t execute_start = ObTimeUtility::current_monotonic_time();
      result.sql_error_ = client.execute_update(result.affected_, result.error_);
      result.execute_us_ = ObTimeUtility::current_monotonic_time() - execute_start;
      // Do not issue another SQL command: even SELECT can trigger automatic
      // end-trans and hide the defect. Observe the real session/descriptor under
      // try-locked SQL locks, without ending or releasing the original transaction.
      result.post_ret_ = wait_for_sql_session_state(sessions_, static_cast<uint32_t>(result.session_id_),
                                                   tenant_id_, false, result.tx_after_,
                                                   result.in_transaction_after_);
    }
    {
      std::lock_guard<std::mutex> guard(lock_);
      ++finished_;
      cv_.notify_all();
    }
    if (thread_initialized) {
      mysql_thread_end();
    }
  }

  const std::string host_;
  const int port_;
  const std::string user_;
  const std::string table_;
  sql::ObSQLSessionMgr &sessions_;
  const uint64_t tenant_id_;
  // Native statements must close before their bound amount/key buffers die.
  std::vector<RequestResult> results_;
  std::vector<std::unique_ptr<SQLClient>> clients_;
  std::vector<std::thread> threads_;
  std::mutex lock_;
  std::condition_variable cv_;
  int ready_;
  int finished_;
  bool go_;
  bool cancelled_;
};

class ScopedGroupCommitConfig
{
public:
  ScopedGroupCommitConfig(SQLClient &admin, SQLClient &observer, const std::string &tenant,
                         const uint64_t tenant_id, const bool saved)
      : admin_(admin), observer_(observer), tenant_(tenant), tenant_id_(tenant_id),
        saved_(saved), changed_(false) {}
  ~ScopedGroupCommitConfig()
  {
    if (changed_) {
      EXPECT_EQ(OB_SUCCESS, set(saved_)) << admin_.error_message();
    }
  }
  int set(const bool value)
  {
    int ret = admin_.execute("ALTER SYSTEM SET _enable_hotspot_group_commit="
        + std::string(value ? "true" : "false") + " TENANT='" + tenant_ + "'");
    if (OB_SUCCESS == ret) {
      changed_ = true;
      bool ready = false;
      std::string readback;
      const int64_t expire = ObTimeUtility::current_monotonic_time() + 10 * 1000 * 1000;
      // ALTER acknowledgement and either local view alone can precede propagation.
      // Wait on both views for enable AND restore so an earlier restore cannot
      // race the next case's enable. No cohort transaction is touched here.
      while (OB_SUCCESS == ret && !ready && ObTimeUtility::current_monotonic_time() < expire) {
        ret = observer_.parameter_value("_enable_hotspot_group_commit", tenant_id_, readback);
        if (OB_SUCCESS == ret) {
          const bool enabled = 0 == strcasecmp(readback.c_str(), "true") || readback == "1";
          const bool disabled = 0 == strcasecmp(readback.c_str(), "false") || readback == "0";
          if (!enabled && !disabled) {
            ret = OB_ERR_UNEXPECTED;
          } else if (enabled == value) {
            MTL_SWITCH(tenant_id_) {
              sql::ObSqlGroupCommitAggregator *aggregator = MTL(sql::ObSqlGroupCommitAggregator *);
              ready = nullptr != aggregator && aggregator->is_group_commit_enabled() == value;
            }
          }
        }
        if (OB_SUCCESS == ret && !ready) {
          ob_usleep(10 * 1000);
        }
      }
      if (OB_SUCCESS == ret && !ready) {
        ret = OB_TIMEOUT;
      }
      if (OB_SUCCESS != ret) {
        LOG_WARN("hotspot SQL test config did not converge", K(ret), K_(tenant_id), K(value),
                 "readback", readback.c_str());
      }
    }
    return ret;
  }
private:
  SQLClient &admin_;
  SQLClient &observer_;
  const std::string tenant_;
  const uint64_t tenant_id_;
  const bool saved_;
  bool changed_;
};
} // namespace

class ObHotspotRollbackSQLTest : public ObSimpleClusterTestBase
{
public:
  ObHotspotRollbackSQLTest() : ObSimpleClusterTestBase(run_prefix()), port_(0) {}

  static void TearDownTestCase()
  {
    // Do not invoke the base wildcard deletion or its early _Exit: preserve this
    // run's evidence and let gtest finish reporting before main terminates threads.
    if (!curr_dir_.empty()) {
      EXPECT_EQ(0, chdir(curr_dir_.c_str())) << "cannot restore gtest's original working directory";
    }
  }

protected:
  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    host_ = get_curr_simple_server().get_local_ip();
    port_ = get_curr_simple_server().get_mysql_port();
    ASSERT_FALSE(host_.empty());
    admin_.reset(new SQLClient());
    ASSERT_EQ(OB_SUCCESS, admin_->connect(host_, port_, "root@sys")) << admin_->error_message();
    if (!tenant_created_) {
      tenant_ = "hotspot_rb_" + std::to_string(getpid());
      ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_.c_str()));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_, tenant_.c_str()));
      tenant_created_ = true;
    }
    observer_.reset(new SQLClient());
    ASSERT_EQ(OB_SUCCESS, observer_->connect(host_, port_, "root@" + tenant_))
        << observer_->error_message();
    std::string saved;
    ASSERT_EQ(OB_SUCCESS, observer_->parameter_value("_enable_hotspot_group_commit", tenant_id_, saved))
        << observer_->error_message();
    ASSERT_TRUE(0 == strcasecmp(saved.c_str(), "true") || saved == "1"
                || 0 == strcasecmp(saved.c_str(), "false") || saved == "0") << saved;
    const bool enabled = 0 == strcasecmp(saved.c_str(), "true") || saved == "1";
    config_.reset(new ScopedGroupCommitConfig(*admin_, *observer_, tenant_, tenant_id_, enabled));
    ASSERT_EQ(OB_SUCCESS, config_->set(true)) << admin_->error_message();
    ASSERT_EQ(OB_SUCCESS, observer_->parameter_value("_enable_hotspot_group_commit", tenant_id_, saved))
        << observer_->error_message();
    ASSERT_TRUE(0 == strcasecmp(saved.c_str(), "true") || saved == "1") << saved;
    table_ = "hotspot_rollback_t6_" + std::to_string(++table_index_);
    ASSERT_EQ(OB_SUCCESS, observer_->execute("CREATE TABLE " + table_
        + " (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")) << observer_->error_message();
    ASSERT_EQ(OB_SUCCESS, observer_->execute("INSERT INTO " + table_ + " VALUES(1,"
        + std::to_string(INITIAL_BALANCE) + ")")) << observer_->error_message();
    {
      SCOPED_TRACE("protocol setup: standard PS UPDATE, no GC hint, no matching row");
      // This independent connection is gone before any cohort BEGIN or event arm.
      // It tests only the real standard-EXECUTE transport, not aggregation coverage.
      int64_t amount = 1;
      int64_t absent_key = -1;
      SQLClient protocol_probe;
      ASSERT_EQ(OB_SUCCESS, protocol_probe.connect(host_, port_, "root@" + tenant_))
          << protocol_probe.error_message();
      // The native connect path may leave field metadata from its internal
      // capability queries. Establish a real zero-field OK on this fresh probe,
      // before prepare/execute and before any original cohort transaction exists.
      ASSERT_EQ(OB_SUCCESS, protocol_probe.execute("SET SESSION autocommit=1"))
          << protocol_probe.error_message();
      ASSERT_EQ(OB_SUCCESS, protocol_probe.prepare_update(table_, amount, absent_key, false))
          << protocol_probe.error_message();
      int64_t affected = -1;
      std::string diagnostic;
      ASSERT_EQ(0, protocol_probe.execute_update(affected, diagnostic)) << diagnostic;
      ASSERT_EQ(0, affected) << diagnostic;
      int64_t tx_after = -1;
      bool in_transaction = true;
      ASSERT_EQ(OB_SUCCESS, wait_for_sql_session_state(get_curr_observer().get_sql_session_mgr(),
          static_cast<uint32_t>(protocol_probe.session_id()), tenant_id_, false,
          tx_after, in_transaction)) << diagnostic;
      ASSERT_EQ(0, tx_after);
      ASSERT_FALSE(in_transaction);
    }
  }

  void run_scenario(const char *event_name, const int injected_error, const int public_error,
                    const bool exhaust_local_budget = false)
  {
    // Only the local-budget case keeps a finite retryable fault armed. The four
    // original outcome cases remain one-shot; no case injects an unlimited event.
    const int64_t injection_count = exhaust_local_budget ? 100000 : 1;
    if (OB_SUCCESS == public_error) {
      // Successful primary rollback needs a primary-only witness. Secondary
      // memtable cleanup has its own EN_HOTSPOT_SECONDARY_ROLLBACK_MEMTABLE_FAIL.
      ASSERT_STREQ("EN_HOTSPOT_PRIMARY_ROLLBACK_MEMTABLE_FAIL", event_name);
    }
    if (exhaust_local_budget) {
      ASSERT_EQ(OB_EAGAIN, injected_error);
      ASSERT_EQ(OB_TIMEOUT, public_error);
    }
    ScopedEvent event(event_name);
    ASSERT_TRUE(event.registered()) << "required production event is not registered: " << event_name;
    bool hit_expected_path = false;
    int64_t expected_balance = INITIAL_BALANCE;
    const int64_t scenario_expire = ObTimeUtility::current_monotonic_time() + 60 * 1000 * 1000;
    for (int wave_index = 0; wave_index < MAX_WAVES && !hit_expected_path
         && ObTimeUtility::current_monotonic_time() < scenario_expire; ++wave_index) {
      SCOPED_TRACE(::testing::Message() << "event=" << event_name << ", wave=" << wave_index);
      RequestWave wave(host_, port_, "root@" + tenant_, table_, wave_index,
                       get_curr_observer().get_sql_session_mgr(), tenant_id_);
      wave.start();
      ASSERT_TRUE(wave.wait_ready()) << "client setup exceeded its finite deadline";
      ObSEArray<int64_t, CLIENT_COUNT> cohort_tx_ids;
      for (const RequestResult &result : wave.results()) {
        ASSERT_EQ(OB_SUCCESS, result.setup_ret_) << result.error_;
        ASSERT_GT(result.tx_before_, 0) << "pre-existing writes are required to avoid HS-001";
        ASSERT_EQ(OB_SUCCESS, cohort_tx_ids.push_back(result.tx_before_));
      }
      // Arm only after tenant/bootstrap/client setup. Inspect counters without
      // calling the event, which would itself consume the next occurrence.
      ASSERT_EQ(OB_SUCCESS, event.arm(injected_error, injection_count));
      const int64_t arm_wall_us = ObClockGenerator::getClock();
      wave.release();
      ASSERT_TRUE(wave.wait_finished()) << "PS/client completion exceeded its finite deadline";
      const int64_t complete_wall_us = ObClockGenerator::getClock();
      const int64_t event_remaining = event.remaining();
      const bool consumed = event_remaining >= 0 && event_remaining < injection_count;
      TRANS_LOG(INFO, "hotspot rollback SQL event consumption", K(event_name), K(wave_index),
                K(event_remaining), K(injected_error), K(injection_count), K(exhaust_local_budget),
                K(arm_wall_us), K(complete_wall_us), K(cohort_tx_ids));
      ASSERT_GE(event_remaining, 0);
      ASSERT_LE(event_remaining, injection_count);
      if (exhaust_local_budget && consumed) {
        ASSERT_GT(event_remaining, 0) << "fault must remain armed through the budget exit";
        ASSERT_GE(injection_count - event_remaining, 2) << "a single probe is not a retry wait";
      }
      int64_t good_success = 0;
      int64_t good_aborted = 0;
      int64_t bad_count = 0;
      for (const RequestResult &result : wave.results()) {
        ASSERT_LT(ObTimeUtility::current_monotonic_time(), scenario_expire)
            << "finite scenario budget expired while observing SQL cleanup";
        SCOPED_TRACE(::testing::Message() << "session=" << result.session_id_
            << ", tx=" << result.tx_before_ << ", side_key=" << result.side_key_
            << ", mysql_error=" << result.sql_error_ << ", execute_us=" << result.execute_us_
            << ", message=" << result.error_);
        ASSERT_EQ(OB_SUCCESS, result.post_ret_)
            << "original SQL session/descriptor did not end before the read-only probe deadline";
        EXPECT_FALSE(result.in_transaction_after_);
        EXPECT_EQ(0, result.tx_after_) << "SQL returned while its original descriptor remained attached";
        const bool good = result.amount_ == 1;
        if (good && result.sql_error_ == 0) {
          ASSERT_EQ(1, result.affected_);
          ++good_success;
        } else if (good) {
          ASSERT_NE(OB_SUCCESS, public_error);
          ASSERT_EQ(packet_errno(public_error), result.sql_error_);
          if (exhaust_local_budget) {
            // Exclude the later read-only session probe from this duration.
            // The injected code is EAGAIN, never TIMEOUT; server work diagnostics
            // must also witness LOCAL/default budget before claiming that coverage.
            ASSERT_GE(result.execute_us_, transaction::ObHotspotRollbackWork::DEFAULT_BUDGET_US);
          }
          ++good_aborted;
        } else {
          ++bad_count;
          // A zero-row request must report rollback even when it is the primary of a committed group.
          ASSERT_TRUE(result.sql_error_ == packet_errno(OB_ROLLBACK_ON_NO_AFFECTED_ROWS)
                      || (OB_SUCCESS != public_error && result.sql_error_ == packet_errno(public_error)));
        }
        int64_t row_count = -1;
        ASSERT_EQ(OB_SUCCESS, observer_->scalar("SELECT COUNT(*) FROM " + table_
            + " WHERE id=" + std::to_string(result.side_key_), row_count))
            << observer_->error_message();
        EXPECT_EQ(good && result.sql_error_ == 0 ? 1 : 0, row_count)
            << "failed/zero-row request committed its earlier side-effect";
      }
      ASSERT_GT(bad_count, 0);
      expected_balance -= good_success;
      int64_t balance = -1;
      ASSERT_EQ(OB_SUCCESS, observer_->scalar("SELECT value FROM " + table_ + " WHERE id=1", balance))
          << observer_->error_message();
      EXPECT_EQ(expected_balance, balance);

      // No original client has been closed or manually rolled back at this point.
      // A separate finite-lock transaction must be able to acquire both the hot row
      // and every rolled-back insert key. Closing clients first would mask leaked locks.
      SQLClient probe;
      ASSERT_EQ(OB_SUCCESS, probe.connect(host_, port_, "root@" + tenant_));
      ASSERT_EQ(OB_SUCCESS, probe.set_session_int("ob_query_timeout", 3000000))
          << probe.error_message();
      ASSERT_EQ(OB_SUCCESS, probe.set_session_int("ob_trx_lock_timeout", 1000000))
          << probe.error_message();
      ASSERT_EQ(OB_SUCCESS, probe.execute("BEGIN"));
      ASSERT_EQ(OB_SUCCESS, probe.execute("UPDATE " + table_ + " SET value=value WHERE id=1"))
          << probe.error_message();
      for (const RequestResult &result : wave.results()) {
        ASSERT_LT(ObTimeUtility::current_monotonic_time(), scenario_expire)
            << "finite scenario budget expired during lock probes";
        if (result.amount_ != 1 || result.sql_error_ != 0) {
          ASSERT_EQ(OB_SUCCESS, probe.execute("INSERT INTO " + table_ + " VALUES("
              + std::to_string(result.side_key_) + ",0)")) << probe.error_message();
        }
      }
      ASSERT_EQ(OB_SUCCESS, probe.execute("ROLLBACK"));
      EXPECT_FALSE(probe.in_transaction());
      TRANS_LOG(INFO, "hotspot rollback SQL wave observed", K(wave_index), K(event_name),
                K(consumed), K(good_success), K(good_aborted), K(expected_balance), K(balance),
                K(public_error));
      if (OB_SUCCESS == public_error) {
        // The checkpoint event is now exclusive to primary rollback. Consuming
        // it cannot be explained by rolling back only bad secondary requests.
        hit_expected_path = consumed && good_success > 0;
      } else {
        // Preparation events may still fire in best-effort secondary cleanup;
        // the primary-only checkpoint event does not. In either case require
        // a valid guarded request to receive the terminal group error and have
        // its prior write rolled back, not just an event-counter decrement.
        hit_expected_path = consumed && good_aborted > 0;
      }
    }
    ASSERT_TRUE(hit_expected_path)
        << "no qualifying primary storage rollback/group-error outcome within " << MAX_WAVES
        << " finite mixed PS waves; hint/concurrency alone is not coverage";
  }

private:
  static bool tenant_created_;
  static uint64_t tenant_id_;
  static std::string tenant_;
  static int table_index_;
  std::string host_;
  int port_;
  std::string table_;
  std::unique_ptr<SQLClient> admin_;
  std::unique_ptr<SQLClient> observer_;
  std::unique_ptr<ScopedGroupCommitConfig> config_;
};

bool ObHotspotRollbackSQLTest::tenant_created_ = false;
uint64_t ObHotspotRollbackSQLTest::tenant_id_ = OB_INVALID_TENANT_ID;
std::string ObHotspotRollbackSQLTest::tenant_;
int ObHotspotRollbackSQLTest::table_index_ = 0;

TEST_F(ObHotspotRollbackSQLTest, normal_partial_rollback_preserves_successful_requests)
{
  // Returning zero still consumes occur (EventItem::get_event_code). This point
  // is primary-checkpoint-only; secondary rollback uses a distinct event. Thus
  // consumption witnesses the real primary path without injecting a failure.
  run_scenario("EN_HOTSPOT_PRIMARY_ROLLBACK_MEMTABLE_FAIL", OB_SUCCESS, OB_SUCCESS);
}

TEST_F(ObHotspotRollbackSQLTest, checkpoint_error_rolls_back_the_sql_group)
{
  run_scenario("EN_HOTSPOT_PRIMARY_ROLLBACK_MEMTABLE_FAIL",
               OB_ERR_UNEXPECTED, OB_TRANS_NEED_ROLLBACK);
}

TEST_F(ObHotspotRollbackSQLTest, raw_iter_end_cannot_commit_the_sql_group)
{
  run_scenario("EN_ROLLBACK_TO_LOG_GET_TX_DATA_FAIL", OB_ITER_END, OB_TRANS_NEED_ROLLBACK);
}

TEST_F(ObHotspotRollbackSQLTest, timeout_result_rolls_back_the_sql_group)
{
  // This exercises timeout propagation/end-group, not a measured local-budget wait.
  run_scenario("EN_ROLLBACK_TO_LOG_GET_TX_DATA_FAIL", OB_TIMEOUT, OB_TIMEOUT);
}

TEST_F(ObHotspotRollbackSQLTest, retryable_prepare_exhausts_shared_local_budget)
{
  // Repeated real pre-out EAGAIN drives the existing shared operation deadline;
  // it does not inject the expected timeout or alter SQL/transaction timeouts.
  run_scenario("EN_ROLLBACK_TO_LOG_GET_TX_DATA_FAIL", OB_EAGAIN, OB_TIMEOUT, true);
}
#else
TEST(ObHotspotRollbackSQLBuild, requires_hotspot_and_debug_fault_registration)
{
  FAIL() << "Build this integration target with OB_HOTSPOT_GROUP_COMMIT and ENABLE_DEBUG_LOG";
}
#endif

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char cwd[PATH_MAX] = {0};
  char log_path[PATH_MAX] = {0};
  if (nullptr == getcwd(cwd, sizeof(cwd))) {
    return 1;
  }
  const int length = snprintf(log_path, sizeof(log_path), "%s/%s.log", cwd,
                              oceanbase::unittest::run_prefix().c_str());
  if (length < 0 || static_cast<size_t>(length) >= sizeof(log_path)) {
    return 1;
  }
  // Do not call init_log_and_gtest(): its legacy wildcard log deletion is not
  // appropriate for a new test. The wall-time/PID prefix also owns its data directory.
  OB_LOGGER.set_file_name(log_path, true, false);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  // Reports (including XML) are complete. The single observer is not gracefully
  // stopped here: the OS reclaims its threads, avoiding singleton-destructor hangs.
  fflush(stdout);
  fflush(stderr);
  _Exit(ret);
}
