// owner: anli.hx
// owner group: DAS

/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <map>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

#include "env/ob_simple_cluster_test_base.h"
#include <mysql.h>

// Dima 2026092900119496982 / OB5PI10029S.
// Capture the DML OK packet before any subsequent SELECT or SHOW WARNINGS.
// Compatibility OFF retains the 44x baseline; ON restores measured 42x ID behavior.
// Stored-data and error checks are independent of the ID compatibility switch.
// Optional MySQL runs report differences; they do not define the OB assertions.
namespace oceanbase
{
namespace unittest
{
namespace last_insert_id
{
using Id = std::uint64_t;

struct Parameter {
  enum class Kind { SIGNED_INTEGER, UNSIGNED_INTEGER, STRING, NULL_VALUE };
  Kind kind;
  Id value = 0;
  std::string text = {};

  static Parameter integer(std::int64_t value)
  {
    return {Kind::SIGNED_INTEGER, static_cast<Id>(value), {}};
  }
  static Parameter unsigned_integer(Id value) { return {Kind::UNSIGNED_INTEGER, value, {}}; }
  static Parameter string(const std::string &value) { return {Kind::STRING, 0, value}; }
  static Parameter null() { return {Kind::NULL_VALUE, 0, {}}; }
};

struct Case {
  std::string name;
  std::vector<std::string> setup;
  std::string statement;
  // A decimal constant, "row:N" for the generated ID where uk = N, or
  // "sql:SELECT ..." for a saved session ID or a mysqltest-derived expectation.
  std::string expected_id;
  std::string expected_session;
  Id affected_rows = 0;
  // A scalar SELECT returning 1 when the stored data is correct.
  std::string data_check;
  bool auto_increment = true;
  unsigned expected_error = 0;
  std::string table_definition = {};
  std::vector<Case> steps = {};
  bool source_table = false;
  bool require_pdml = false;
  bool generated_auto_values = false;
  std::vector<Parameter> parameters = {};
  bool multi_statement = false;
  bool enable_batch = false;
  bool expect_batch = false;
  bool fresh_session = false;
  bool peer_connection = false;
  bool reuse_prepared = false;
  std::vector<std::string> cleanup = {};
  bool force_internal_batch = false;
  int values_split = -1;
  int batch_row_count = 2;
};

struct ProtocolCase {
  Case test;
  bool prepared;
  bool compatible = false;
};

void bind_document_literals(Case &test);
void PrintTo(const ProtocolCase &value, std::ostream *output);
std::vector<ProtocolCase> protocol_cases();
const char *test_env_prefix();

using Rows = std::vector<std::vector<std::string>>;

struct DbError : std::runtime_error {
  unsigned code;
  DbError(unsigned code, const std::string &message)
      : std::runtime_error("MySQL error " + std::to_string(code) + ": " + message), code(code) {}
};

struct Outcome {
  Id insert_id;
  Id affected_rows;
};

struct Observed {
  unsigned error = 0;
  Outcome outcome{0, 0};
  Id session_id = 0;
  Id data_check = 0;
  Rows rows;
  std::vector<Observed> steps = {};
};

// This catalog contains only numeric/NULL literals. Keep setup and assertions
// identical when binding those literals through COM_STMT_EXECUTE.
void bind_document_literals(Case &test)
{
  if (!test.steps.empty()) {
    for (auto &step : test.steps) { bind_document_literals(step); }
    return;
  }
  const std::string sql = test.statement;
  test.statement.clear();
  for (std::size_t i = 0; i < sql.size();) {
    const bool boundary = i == 0 || !(std::isalnum(static_cast<unsigned char>(sql[i - 1])) || sql[i - 1] == '_');
    if (boundary && sql.compare(i, 4, "NULL") == 0) {
      test.statement += "?";
      test.parameters.push_back(Parameter::null());
      i += 4;
    } else if (boundary && std::isdigit(static_cast<unsigned char>(sql[i]))) {
      std::size_t end = i + 1;
      while (end < sql.size() && std::isdigit(static_cast<unsigned char>(sql[end]))) { ++end; }
      test.statement += "?";
      test.parameters.push_back(Parameter::integer(std::stoll(sql.substr(i, end - i))));
      i = end;
    } else {
      test.statement += sql[i++];
    }
  }
}

std::string expand(std::string sql, const std::string &table)
{
  for (const auto &entry : std::vector<std::pair<std::string, std::string>>{
      {"{t}", table}, {"{s}", table + "_source"}, {"{tr}", table + "_before_insert"},
      {"{log}", table + "_log"}, {"{ai}", table + "_after_insert"},
      {"{ad}", table + "_after_delete"},
      {"{proc}", table + "_proc"}, {"{func}", table + "_func"}}) {
    std::size_t pos = 0;
    while ((pos = sql.find(entry.first, pos)) != std::string::npos) {
      sql.replace(pos, entry.first.size(), "`" + entry.second + "`");
      pos += entry.second.size() + 2;
    }
  }
  return sql;
}

Id number(const std::string &value)
{
  if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos) {
    throw std::runtime_error("Expected an unsigned integer, got: " + value);
  }
  std::size_t end = 0;
  Id result = std::stoull(value, &end);
  if (end != value.size()) {
    throw std::runtime_error("Invalid unsigned integer: " + value);
  }
  return result;
}

class Connection {
public:
  Connection(const std::string &host, unsigned port, const std::string &user,
             const std::string &password, const std::string &database, bool multi_statement = false)
  {
    mysql_ = mysql_init(nullptr);
    if (mysql_ == nullptr) {
      throw std::runtime_error("mysql_init failed");
    }
    unsigned timeout = 60;
    unsigned protocol = MYSQL_PROTOCOL_TCP;
    if (mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout) != 0 ||
        mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &timeout) != 0 ||
        mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &timeout) != 0 ||
        mysql_options(mysql_, MYSQL_OPT_PROTOCOL, &protocol) != 0 ||
        mysql_real_connect(mysql_, host.c_str(), user.c_str(), password.c_str(),
                           database.c_str(), port, nullptr,
                           multi_statement ? CLIENT_MULTI_STATEMENTS : 0) == nullptr) {
      const std::string message = mysql_error(mysql_);
      mysql_close(mysql_);
      mysql_ = nullptr;
      throw std::runtime_error("Connection failed: " + message);
    }
  }

  ~Connection()
  {
    if (reused_statement_ != nullptr) { mysql_stmt_close(reused_statement_); }
    mysql_close(mysql_);
  }
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

  Rows query(const std::string &sql)
  {
    if (mysql_real_query(mysql_, sql.data(), sql.size()) != 0) {
      throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
    }
    MYSQL_RES *result = mysql_store_result(mysql_);
    if (result == nullptr) {
      if (mysql_field_count(mysql_) != 0) {
        throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
      }
      return {};
    }
    Rows rows;
    MYSQL_ROW row;
    const unsigned fields = mysql_num_fields(result);
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::vector<std::string> values;
      unsigned long *lengths = mysql_fetch_lengths(result);
      for (unsigned i = 0; i < fields; ++i) {
        values.emplace_back(row[i] == nullptr ? "NULL" : std::string(row[i], lengths[i]));
      }
      rows.push_back(std::move(values));
    }
    const unsigned error = mysql_errno(mysql_);
    const std::string message = mysql_error(mysql_);
    mysql_free_result(result);
    if (error != 0) {
      throw DbError(error, message);
    }
    return rows;
  }

  Id scalar(const std::string &sql)
  {
    Rows rows = query(sql);
    if (rows.size() != 1 || rows.front().size() != 1) {
      throw std::runtime_error("Expected one scalar result: " + sql);
    }
    return number(rows.front().front());
  }

  std::vector<Outcome> execute_multi(const std::string &sql)
  {
    if (mysql_real_query(mysql_, sql.data(), sql.size()) != 0) {
      throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
    }
    std::vector<Outcome> results;
    bool unexpected_result_set = false;
    int status = 0;
    do {
      // Save each OK packet before mysql_next_result replaces the client state.
      results.push_back({mysql_insert_id(mysql_), mysql_affected_rows(mysql_)});
      MYSQL_RES *rows = mysql_store_result(mysql_);
      if (rows != nullptr) {
        unexpected_result_set = true;
        mysql_free_result(rows);
      } else if (mysql_field_count(mysql_) != 0) {
        throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
      }
      status = mysql_next_result(mysql_);
    } while (status == 0);
    if (status > 0) {
      throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
    }
    if (unexpected_result_set) {
      throw std::runtime_error("Expected only DML OK packets in the multi-statement request");
    }
    return results;
  }

  Outcome execute(const std::string &sql, bool prepared, const std::vector<Parameter> &parameters = {},
                  bool reuse_prepared = false)
  {
    if (!prepared) {
      if (!parameters.empty()) {
        throw std::runtime_error("Bound parameters require the PS protocol");
      }
      if (mysql_real_query(mysql_, sql.data(), sql.size()) != 0) {
        throw DbError(mysql_errno(mysql_), mysql_error(mysql_));
      }
      // Capture immediately: a SELECT or SHOW WARNINGS would replace this state.
      return {mysql_insert_id(mysql_), mysql_affected_rows(mysql_)};
    }
    if (reuse_prepared && reused_statement_ != nullptr && reused_sql_ != sql) {
      throw std::runtime_error("A reused prepared statement must retain its SQL");
    }
    const bool already_prepared = reuse_prepared && reused_statement_ != nullptr;
    MYSQL_STMT *statement = already_prepared ? reused_statement_ : mysql_stmt_init(mysql_);
    if (statement == nullptr) {
      throw std::runtime_error("mysql_stmt_init failed");
    }
    const int prepare_status = already_prepared ? 0 : mysql_stmt_prepare(statement, sql.data(), sql.size());
    if (prepare_status != 0) {
      const unsigned code = mysql_errno(mysql_) != 0 ? mysql_errno(mysql_) : mysql_stmt_errno(statement);
      const std::string message = "PS prepare status=" + std::to_string(prepare_status) +
          " connection_error=" + mysql_error(mysql_) + " statement_error=" + mysql_stmt_error(statement);
      mysql_stmt_close(statement);
      throw DbError(code, message);
    }
    if (reuse_prepared) {
      reused_statement_ = statement;
      reused_sql_ = sql;
    }
    // Send COM_STMT_EXECUTE through the SDK transport: the repository's
    // libobclnt 2.2.3 mysql_stmt_execute() returns 1 without sending the request.
    // Match the server's placeholder count before encoding the bound values.
    if (mysql_stmt_param_count(statement) != parameters.size() || mysql_stmt_field_count(statement) != 0) {
      if (!reuse_prepared) { mysql_stmt_close(statement); }
      throw std::runtime_error("Prepared DML parameter count mismatch or unexpected result columns");
    }
    std::vector<char> request(9, 0);
    for (unsigned i = 0; i < 4; ++i) {
      request[i] = static_cast<char>((statement->stmt_id >> (8 * i)) & 0xff);
    }
    request[4] = 0; // CURSOR_TYPE_NO_CURSOR.
    request[5] = 1; // Little-endian iteration count = 1.
    if (!parameters.empty()) {
      const std::size_t bitmap_bytes = (parameters.size() + 7) / 8;
      request.resize(request.size() + bitmap_bytes, 0);
      request.push_back(1); // new_params_bind_flag: include types on this execution.
      for (std::size_t i = 0; i < parameters.size(); ++i) {
        const auto kind = parameters[i].kind;
        if (kind == Parameter::Kind::NULL_VALUE) {
          request[9 + i / 8] = static_cast<char>(static_cast<unsigned char>(request[9 + i / 8]) |
                                               (1u << (i % 8)));
        }
        const auto type = kind == Parameter::Kind::NULL_VALUE ? ::MYSQL_TYPE_NULL :
                          kind == Parameter::Kind::STRING ? ::MYSQL_TYPE_VAR_STRING : ::MYSQL_TYPE_LONGLONG;
        request.push_back(static_cast<char>(type));
        request.push_back(kind == Parameter::Kind::UNSIGNED_INTEGER ? static_cast<char>(0x80) : 0);
      }
      auto append_integer = [&](Id value, unsigned bytes) {
        for (unsigned i = 0; i < bytes; ++i) {
          request.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
        }
      };
      for (const auto &parameter : parameters) {
        if (parameter.kind == Parameter::Kind::STRING) {
          const Id length = parameter.text.size();
          if (length < 251) {
            append_integer(length, 1);
          } else {
            const unsigned bytes = length < 65536 ? 2 : length < 16777216 ? 3 : 8;
            request.push_back(static_cast<char>(bytes == 2 ? 0xfc : bytes == 3 ? 0xfd : 0xfe));
            append_integer(length, bytes);
          }
          request.insert(request.end(), parameter.text.begin(), parameter.text.end());
        } else if (parameter.kind != Parameter::Kind::NULL_VALUE) {
          append_integer(parameter.value, 8);
        }
      }
    }
    if (mysql_->methods->db_command(mysql_, ::COM_STMT_EXECUTE, request.data(), request.size(), 1, statement) != 0 ||
        mysql_->methods->db_read_stmt_result(mysql_) != 0) {
      const unsigned code = mysql_errno(mysql_);
      const std::string message = "COM_STMT_EXECUTE: " + std::string(mysql_error(mysql_));
      if (!reuse_prepared) { mysql_stmt_close(statement); }
      throw DbError(code, message);
    }
    Outcome outcome{mysql_insert_id(mysql_), mysql_affected_rows(mysql_)};
    if (!reuse_prepared) { mysql_stmt_close(statement); }
    return outcome;
  }

private:
  MYSQL *mysql_ = nullptr;
  MYSQL_STMT *reused_statement_ = nullptr;
  std::string reused_sql_;
};

// Optional reference-server comparison. Credentials stay in a local file and
// are never written to the comparison report or test output.
struct MySQLReference {
  bool enabled = false;
  std::string host;
  unsigned port = 3306;
  std::string user;
  std::string password;
  std::string database;
  std::string report;
};

const MySQLReference &mysql_reference()
{
  static const MySQLReference config = [] {
    MySQLReference result;
    const char *path = std::getenv("LAST_INSERT_ID_MYSQL_CONFIG");
    if (path == nullptr || *path == '\0') {
      return result;
    }
    std::ifstream input(path);
    if (!input) {
      throw std::runtime_error("Cannot open MySQL reference configuration");
    }
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty() || line.front() == '#') {
        continue;
      }
      const std::size_t separator = line.find('=');
      if (separator == std::string::npos) {
        throw std::runtime_error("Expected key=value in MySQL reference configuration");
      }
      const std::string key = line.substr(0, separator);
      const std::string value = line.substr(separator + 1);
      if (key == "host") {
        result.host = value;
      } else if (key == "port") {
        const Id port = number(value);
        if (port == 0 || port > 65535) {
          throw std::runtime_error("Invalid MySQL reference port");
        }
        result.port = static_cast<unsigned>(port);
      } else if (key == "user") {
        result.user = value;
      } else if (key == "password") {
        result.password = value;
      } else if (key == "database") {
        result.database = value;
      } else if (key == "report") {
        result.report = value;
      } else {
        throw std::runtime_error("Unknown MySQL reference configuration key: " + key);
      }
    }
    if (result.host.empty() || result.user.empty() || result.database.empty() || result.report.empty()) {
      throw std::runtime_error("MySQL reference requires host, user, database and report");
    }
    result.enabled = true;
    return result;
  }();
  return config;
}

Id expected(Connection &connection, const std::string &value, const std::string &table)
{
  if (value.compare(0, 4, "sql:") == 0) {
    return connection.scalar(expand(value.substr(4), table));
  }
  if (value.compare(0, 4, "row:") == 0) {
    const Id uk = number(value.substr(4));
    return connection.scalar("SELECT id FROM `" + table + "` WHERE uk=" + std::to_string(uk));
  }
  return number(value);
}

Rows read_batch_plan_stat(Connection &connection, const std::string &table)
{
  return connection.query(
      "SELECT SVR_IP,SVR_PORT,PLAN_ID,SQL_ID,IS_BATCHED_MULTI_STMT,"
      "EXECUTIONS,HIT_COUNT,STATEMENT,QUERY_SQL "
      "FROM oceanbase.GV$OB_PLAN_CACHE_PLAN_STAT "
      "WHERE INSTR(STATEMENT,'" + table + "')>0 "
      "AND (UPPER(SUBSTR(QUERY_SQL,1,6))='INSERT' "
      "OR QUERY_SQL LIKE '/*last_insert_id_batch*/%') "
      "ORDER BY SVR_IP,SVR_PORT,PLAN_ID");
}

Rows executed_plan_delta(const Rows &before, const Rows &after)
{
  Rows changed;
  for (const auto &row : after) {
    Id old_executions = 0;
    for (const auto &old : before) {
      if (old.at(0) == row.at(0) && old.at(1) == row.at(1) &&
          old.at(2) == row.at(2)) {
        old_executions = number(old.at(5));
        break;
      }
    }
    const Id executions = number(row.at(5));
    if (executions > old_executions) {
      auto result = row;
      result.push_back(std::to_string(old_executions));
      result.push_back(std::to_string(executions - old_executions));
      changed.push_back(result);
    }
  }
  return changed;
}

std::vector<std::string> run_case(Connection &connection, const Case &test,
                                const std::string &table, bool prepared,
                                Observed *observed = nullptr, bool validate_expected = true,
                                bool initialize_session = true, Rows *batch_plans = nullptr,
                                Connection *peer = nullptr)
{
  if (initialize_session) {
    connection.query("SET SESSION sql_mode='STRICT_ALL_TABLES'");
    connection.query("SET SESSION auto_increment_increment=1");
    connection.query("SET SESSION auto_increment_offset=1");
  }
  for (const auto &sql : test.setup) {
    connection.query(expand(sql, table));
  }
  // The known session ID deliberately differs from the explicit inserted ID.
  if (initialize_session && !test.require_pdml) {
    const Id id = connection.scalar(test.fresh_session ? "SELECT LAST_INSERT_ID()" : "SELECT LAST_INSERT_ID(7)");
    if (id != (test.fresh_session ? 0 : 7)) {
      throw std::runtime_error("Unexpected initial session LAST_INSERT_ID: " + std::to_string(id));
    }
  }

  std::vector<std::string> failures;
  if (test.multi_statement) {
    if (prepared || test.steps.empty()) {
      throw std::runtime_error("Multi-statements require COM_QUERY and at least one statement");
    }
    std::string sql;
    for (const auto &step : test.steps) {
      sql += "/*last_insert_id_batch*/" + expand(step.statement, table) + ";";
    }
    const Rows plans_before = batch_plans == nullptr ? Rows{} : read_batch_plan_stat(connection, table);
    const auto results = connection.execute_multi(sql);
    if (results.size() != test.steps.size()) {
      throw std::runtime_error("Wrong number of multi-statement OK packets: " +
                               std::to_string(results.size()));
    }
    if (batch_plans != nullptr) {
      for (unsigned attempt = 0; attempt < 100; ++attempt) {
        *batch_plans = executed_plan_delta(plans_before, read_batch_plan_stat(connection, table));
        if (!batch_plans->empty()) { break; }
        usleep(10000);
      }
    }
    const Id session = connection.scalar("SELECT LAST_INSERT_ID()");
    const Id data_check = connection.scalar(expand(test.data_check, table));
    const Rows rows = observed == nullptr ? Rows{} :
        connection.query("SELECT id,uk,v FROM `" + table + "` ORDER BY uk");
    auto compare = [&](const std::string &label, Id actual, Id wanted) {
      if (actual != wanted) {
        failures.push_back(label + "=" + std::to_string(actual) + " expected=" + std::to_string(wanted));
      }
    };
    for (std::size_t i = 0; i < results.size(); ++i) {
      const auto &step = test.steps[i];
      if (validate_expected) {
        compare(step.name + ".ok_packet_id", results[i].insert_id, expected(connection, step.expected_id, table));
        compare(step.name + ".affected_rows", results[i].affected_rows, step.affected_rows);
      }
      if (observed != nullptr) {
        Observed value;
        value.outcome = results[i];
        value.session_id = i + 1 == results.size() ? session : 0;
        value.data_check = data_check;
        value.rows = rows;
        observed->steps.push_back(std::move(value));
      }
    }
    if (validate_expected) {
      compare("final_session_id", session, expected(connection, test.expected_session, table));
    }
    compare("stored_data_check", data_check, 1);
    return failures;
  }
  if (!test.steps.empty()) {
    for (const auto &step : test.steps) {
      Observed step_observation;
      try {
        if (step.peer_connection && peer == nullptr) {
          throw std::runtime_error("A second independent connection is required");
        }
        for (const auto &failure : run_case(step.peer_connection ? *peer : connection, step, table, prepared,
            observed != nullptr ? &step_observation : nullptr, validate_expected, false)) {
          failures.push_back(step.name + ": " + failure);
        }
      } catch (const std::exception &error) {
        throw std::runtime_error(step.name + ": " + error.what());
      }
      if (observed != nullptr) {
        observed->steps.push_back(std::move(step_observation));
      }
    }
    return failures;
  }

  if (test.require_pdml && validate_expected) {
    std::string plan;
    for (const auto &row : connection.query("EXPLAIN " + expand(test.statement, table))) {
      for (const auto &cell : row) { plan += cell + "\n"; }
    }
    const auto coordinator = plan.find("PX COORDINATOR");
    const auto insert = plan.find("INSERT");
    if (coordinator == std::string::npos || insert == std::string::npos || coordinator > insert) {
      throw std::runtime_error("INSERT did not use the required PDML plan:\n" + plan);
    }
  }
  auto compare = [&](const std::string &label, Id actual, Id wanted) {
    if (actual != wanted) {
      failures.push_back(label + "=" + std::to_string(actual) + " expected=" + std::to_string(wanted));
    }
  };
  try {
    Outcome result = connection.execute(expand(test.statement, table), prepared, test.parameters, test.reuse_prepared);
    const Id row_count = connection.scalar("SELECT ROW_COUNT()");
    if (validate_expected) { compare("row_count", row_count, test.affected_rows); }
    const Id session = test.require_pdml ? 0 : connection.scalar("SELECT LAST_INSERT_ID()");
    if (observed != nullptr) {
      observed->outcome = result;
      observed->session_id = session;
    }
    if (validate_expected && test.expected_error != 0) {
      failures.push_back("expected SQL error " + std::to_string(test.expected_error) + ", got success");
    } else if (validate_expected) {
      if (!test.require_pdml) {
        compare("ok_packet_id", result.insert_id, expected(connection, test.expected_id, table));
        compare("session_id", session, expected(connection, test.expected_session, table));
      }
      compare("affected_rows", result.affected_rows, test.affected_rows);
    }
  } catch (const DbError &error) {
    if (observed != nullptr) {
      observed->error = error.code;
    }
    if (validate_expected && (test.expected_error == 0 || error.code != test.expected_error)) {
      if (observed == nullptr) {
        throw;
      }
      failures.push_back(error.what());
    }
    // MySQL documents last insert ID after an error as undefined: do not assert it.
  }
  const Id data_check = connection.scalar(expand(test.data_check, table));
  compare("stored_data_check", data_check, 1);
  if (observed != nullptr) {
    observed->data_check = data_check;
    observed->rows = connection.query("SELECT id,uk,v FROM `" + table + "` ORDER BY uk");
  }
  return failures;
}

std::string json_string(const std::string &value)
{
  std::string result = "\"";
  for (const char ch : value) {
    switch (ch) {
      case '\\': result += "\\\\"; break;
      case '"': result += "\\\""; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default: result += ch; break;
    }
  }
  return result + "\"";
}

std::string rows_json(const Rows &rows, bool include_id = true)
{
  std::string result = "[";
  for (std::size_t i = 0; i < rows.size(); ++i) {
    if (i > 0) { result += ","; }
    result += "[";
    for (std::size_t j = include_id ? 0 : 1; j < rows[i].size(); ++j) {
      if (j > (include_id ? 0 : 1)) { result += ","; }
      result += json_string(rows[i][j]);
    }
    result += "]";
  }
  return result + "]";
}

std::string normalized_id(Id value, const std::string &reference, const Rows &rows)
{
  if (reference.compare(0, 4, "row:") == 0) {
    for (const auto &row : rows) {
      if (row.at(1) == reference.substr(4) && number(row.at(0)) == value) {
        return reference;
      }
    }
  }
  return std::to_string(value);
}

std::string observed_json(const Observed &value, const Case &test)
{
  const bool include_id = value.error == 0 && !test.require_pdml;
  const bool include_session = include_id && !test.expected_session.empty();
  std::ostringstream output;
  output << "{\"error\":" << value.error
         << ",\"ok_id\":" << (include_id ? std::to_string(value.outcome.insert_id) : "null")
         << ",\"session_id\":" << (include_session ? std::to_string(value.session_id) : "null")
         << ",\"affected_rows\":" << (value.error == 0 ? std::to_string(value.outcome.affected_rows) : "null")
         << ",\"normalized_ok_id\":" << (include_id ?
             json_string(normalized_id(value.outcome.insert_id, test.expected_id, value.rows)) : "null")
         << ",\"normalized_session_id\":" << (include_session ?
             json_string(normalized_id(value.session_id, test.expected_session, value.rows)) : "null")
         << ",\"data_check\":" << value.data_check << ",\"rows\":" << rows_json(value.rows) << "}";
  return output.str();
}

std::string parameters_json(const std::vector<Parameter> &parameters)
{
  std::string output = "[";
  for (std::size_t i = 0; i < parameters.size(); ++i) {
    if (i != 0) { output += ","; }
    const auto &parameter = parameters[i];
    const char *type = parameter.kind == Parameter::Kind::NULL_VALUE ? "NULL" :
                       parameter.kind == Parameter::Kind::STRING ? "STRING" :
                       parameter.kind == Parameter::Kind::UNSIGNED_INTEGER ? "UNSIGNED_INT64" : "SIGNED_INT64";
    const std::string value = parameter.kind == Parameter::Kind::NULL_VALUE ? "null" :
        parameter.kind == Parameter::Kind::STRING ? json_string(parameter.text) :
        json_string(parameter.kind == Parameter::Kind::UNSIGNED_INTEGER ? std::to_string(parameter.value) :
                    std::to_string(static_cast<std::int64_t>(parameter.value)));
    output += "{\"type\":" + json_string(type) + ",\"value\":" + value + "}";
  }
  return output + "]";
}

std::vector<std::string> report_comparison(const Case &test, bool prepared,
                                          const Observed &ob, const Observed &mysql,
                                          const std::string &mysql_version,
                                          const std::string &mysql_table)
{
  std::vector<std::string> differences;
  auto compare = [&](const std::string &label, const std::string &left, const std::string &right) {
    if (left != right) {
      differences.push_back(label + ": OB=" + left + " MySQL=" + right);
    }
  };
  compare("error", std::to_string(ob.error), std::to_string(mysql.error));
  if (ob.error == 0 && mysql.error == 0) {
    if (!test.require_pdml) {
      compare("ok_packet_id", normalized_id(ob.outcome.insert_id, test.expected_id, ob.rows),
              normalized_id(mysql.outcome.insert_id, test.expected_id, mysql.rows));
      if (!test.expected_session.empty()) {
        compare("session_id", normalized_id(ob.session_id, test.expected_session, ob.rows),
                normalized_id(mysql.session_id, test.expected_session, mysql.rows));
      }
    }
    compare("affected_rows", std::to_string(ob.outcome.affected_rows),
            std::to_string(mysql.outcome.affected_rows));
  }
  compare("stored_data_check", std::to_string(ob.data_check), std::to_string(mysql.data_check));
  // Allocated numbers may differ between engines. Each side's data predicate
  // checks explicit IDs, and normalized_id checks the first generated row.
  // Preserve all raw rows in the report, comparing uk/v for generated cases.
  const bool generated = test.generated_auto_values || test.expected_id.compare(0, 4, "row:") == 0 ||
                         test.expected_session.compare(0, 4, "row:") == 0;
  compare("stored_rows", rows_json(ob.rows, !generated), rows_json(mysql.rows, !generated));
  std::ofstream output(mysql_reference().report, std::ios::app);
  if (!output) {
    throw std::runtime_error("Cannot write MySQL comparison report");
  }
  output << "{\"case\":" << json_string(test.name)
         << ",\"protocol\":" << json_string(prepared ? "PS" : "Text")
         << ",\"sql\":" << json_string(test.statement)
         << ",\"parameter_count\":" << test.parameters.size()
         << ",\"parameters\":" << parameters_json(test.parameters)
         << ",\"mysql_version\":" << json_string(mysql_version)
         << ",\"mysql_table\":" << json_string(mysql_table)
         << ",\"last_insert_id_checked\":" << (test.require_pdml ? "false" : "true")
         << ",\"generated_ids_normalized\":" << (generated ? "true" : "false")
         << ",\"match\":" << (differences.empty() ? "true" : "false")
         << ",\"differences\":[";
  for (std::size_t i = 0; i < differences.size(); ++i) {
    if (i > 0) { output << ","; }
    output << json_string(differences[i]);
  }
  output << "],\"ob\":" << observed_json(ob, test) << ",\"mysql\":"
         << observed_json(mysql, test) << "}\n";
  return differences;
}

std::string create_table_sql(const std::string &table, const Case &test)
{
  if (!test.table_definition.empty()) {
    return expand(test.table_definition, table);
  }
  return "CREATE TABLE `" + table + "` (id BIGINT UNSIGNED NOT NULL " +
         (test.auto_increment ? "AUTO_INCREMENT " : "") +
         "PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
}

std::string create_source_table_sql(const std::string &table)
{
  return expand("CREATE TABLE {s} (id BIGINT, uk BIGINT NOT NULL PRIMARY KEY, "
                "v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB", table);
}

unsigned mysql_table_counter = 0;

void PrintTo(const ProtocolCase &value, std::ostream *output)
{
  *output << (value.prepared ? "PS/" : "Text/") << value.test.name;
}

// OFF: measured 44x baseline. ON: measured 42x compatibility IDs.
// This file owns only the general cases; setup, checks and PS bindings are preserved.
std::vector<ProtocolCase> protocol_cases()
{
  std::vector<ProtocolCase> result;
  for (bool compatible : {false, true}) {
    {
      Case test;
      test.name="insert_explicit_single";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_multi";
      test.statement="INSERT INTO {t} VALUES (300,1,0),(100,2,0),(200,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_descending";
      test.statement="INSERT INTO {t} VALUES (300,1,0),(200,2,0),(100,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=3 THEN id END)=100 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_set";
      test.statement="INSERT INTO {t} SET id=100,uk=1,v=0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_uint64";
      test.statement="INSERT INTO {t} VALUES (4294967296,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=4294967296 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_select_explicit";
      test.statement="INSERT INTO {t} SELECT 100,1,0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_select_explicit_ordered";
      test.statement="INSERT INTO {t} SELECT id,n,0 FROM (SELECT 1 AS n,300 AS id UNION ALL SELECT 2,100 UNION ALL SELECT 3,200) AS s ORDER BY n";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_new";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_primary_conflict";
      test.statement="REPLACE INTO {t} VALUES (100,2,0)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=2 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_unique_conflict";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_multi";
      test.statement="REPLACE INTO {t} VALUES (300,1,0),(100,2,0),(200,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_select_explicit";
      test.statement="REPLACE INTO {t} SELECT 100,1,0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_last_row_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,2,0),(200,1,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=100 AND MAX(id)=500 AND SUM(v)=9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_first_row_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,0)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=200 AND MAX(id)=500 AND SUM(v)=9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_all_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,1,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=500 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_select_no_rows";
      test.statement="INSERT INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="ignore_select_no_rows";
      test.statement="INSERT IGNORE INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_generated_null";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_generated_omitted";
      test.statement="INSERT INTO {t}(uk,v) VALUES (1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_generated_zero";
      test.statement="INSERT INTO {t} VALUES (0,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_generated_multi_first_id";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0),(NULL,2,0),(NULL,3,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=3 AND COUNT(DISTINCT id)=3 AND MIN(id)>0 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_generated_before_explicit";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0),(100,2,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)>0 AND MAX(CASE WHEN uk=2 THEN id END)=100 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_explicit_before_generated";
      test.statement="INSERT INTO {t} VALUES (100,1,0),(NULL,2,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=2 THEN id END)>0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_explicit_generated_explicit";
      test.statement="INSERT INTO {t} VALUES (100,1,0),(NULL,2,0),(300,3,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=3 THEN id END)=300 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_generated_null";
      test.statement="REPLACE INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_generated_unique_conflict";
      test.statement="REPLACE INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(id)<>100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_mixed_explicit_generated_explicit";
      test.statement="REPLACE INTO {t} VALUES (100,1,0),(NULL,2,0),(300,3,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=3 THEN id END)=300 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="ignore_generated_all_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (NULL,1,0)";
      test.expected_id=(compatible ? "101" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_last_insert_id_expr_zero";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(0))";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_last_insert_id_expr";
      test.statement="REPLACE INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_last_insert_id_expr_zero";
      test.statement="REPLACE INTO {t} VALUES (100,1,LAST_INSERT_ID(0))";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_no_auto_column";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_no_auto_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_zero_sql_mode";
      test.statement="INSERT INTO {t} VALUES (0,1,0)";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET SESSION sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_nonzero_sql_mode";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET SESSION sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_explicit_insert";
      test.statement="INSERT INTO {t} VALUES (100,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session=(compatible ? "7" : "row:1");
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_unique_conflict_old_id";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "200" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_changes_id";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE id=200,v=0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=LAST_INSERT_ID(id)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=100 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_last_insert_id_expr_zero";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=LAST_INSERT_ID(0)";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_generated_insert";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_generated_conflict";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "101" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_up_mixed_insert_update";
      test.statement="INSERT INTO {t} VALUES (NULL,2,0),(200,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "200" : "row:2");
      test.expected_session=(compatible ? "7" : "row:2");
      test.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND SUM(v)=10 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_duplicate_error";
      test.statement="INSERT INTO {t} VALUES (100,2,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_multi_duplicate_atomicity";
      test.statement="INSERT INTO {t} VALUES (200,2,0),(100,3,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="first_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="second_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND COUNT(DISTINCT id)=2 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="single_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="first_row_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (1),(3)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="middle_row_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (4),(1),(5)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_errors";
        step.statement="INSERT INTO {t}(uk) VALUES (3)";
        step.expected_id="row:3";
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_generated";
        step.statement="INSERT INTO {t} VALUES (100,100,0)";
        step.expected_id=(compatible ? "row:100" : "0");
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=4 AND MAX(CASE WHEN uk=100 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_after_explicit";
        step.statement="INSERT INTO {t} VALUES (DEFAULT,101,0)";
        step.expected_id="row:101";
        step.expected_session="row:101";
        step.data_check="SELECT COUNT(*)=5 AND MAX(id)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="omitted_after_default";
        step.statement="INSERT INTO {t}(uk) VALUES (102)";
        step.expected_id="row:102";
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=6 AND COUNT(DISTINCT id)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_after_recovery";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.data_check="SELECT COUNT(*)=6 AND COUNT(DISTINCT uk)=6 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_ignore_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="first_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="second_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_first";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1)";
        step.expected_id=(compatible ? "3" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_second";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (2)";
        step.expected_id=(compatible ? "4" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="first_generated_row_ignored";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1),(3)";
        step.expected_id="5";
        step.expected_session="5";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND MAX(CASE WHEN uk=3 THEN id END)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="middle_generated_row_ignored";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (4),(1),(5)";
        step.expected_id="row:4";
        step.expected_session="row:4";
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=15 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_ignore";
        step.statement="INSERT IGNORE INTO {t} VALUES (100,100,0)";
        step.expected_id=(compatible ? "row:100" : "0");
        step.expected_session="row:4";
        step.data_check="SELECT COUNT(*)=6 AND MAX(CASE WHEN uk=100 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_after_explicit";
        step.statement="INSERT IGNORE INTO {t} VALUES (DEFAULT,101,0)";
        step.expected_id="row:101";
        step.expected_session="row:101";
        step.data_check="SELECT COUNT(*)=7 AND MAX(id)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="omitted_after_default";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (102)";
        step.expected_id="row:102";
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=8 AND COUNT(DISTINCT uk)=8 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_after_recovery";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1)";
        step.expected_id=(compatible ? "103" : "0");
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=8 AND COUNT(DISTINCT uk)=8 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_update_delete_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_multi";
        step.statement="INSERT INTO {t}(uk) VALUES (1),(2)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=2 AND COUNT(DISTINCT id)=2 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="update_preserves_session";
        step.statement="UPDATE {t} SET id=100 WHERE uk=1";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"SET @remembered_id=LAST_INSERT_ID()"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="delete_preserves_session";
        step.statement="DELETE FROM {t} WHERE uk=2";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="replace_preserves_session";
        step.statement="REPLACE INTO {t} VALUES (100,1,9)";
        step.expected_id=(compatible ? "row:1" : "0");
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=9 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_update";
        step.statement="INSERT INTO {t}(uk) VALUES (3)";
        step.expected_id="row:3";
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=3 THEN id END)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_manual_rollback_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="insert_in_transaction";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(uk)=1 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"SET autocommit=0", "BEGIN"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="no_rows_after_rollback";
        step.statement="INSERT INTO {t} SELECT 100,2,0 WHERE 1=0";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=0 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        step.setup={"SET @rollback_id=LAST_INSERT_ID()", "ROLLBACK"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_rollback";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=1 AND MIN(uk)=2 AND MIN(id)>@rollback_id FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_commit";
        step.statement="INSERT INTO {t} VALUES (100,3,0)";
        step.expected_id=(compatible ? "row:3" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=3 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"COMMIT", "SET autocommit=1"};
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_last_insert_id_expr_sequence";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      {
        Case step;
        step.name="no_auto_column";
        step.statement="INSERT INTO {t} VALUES (7,1,0)";
        step.expected_id="0";
        step.expected_session="0";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        step.setup={"SELECT LAST_INSERT_ID(0)"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="update_expr_without_matching_row";
        step.statement="UPDATE {t} SET id=1 WHERE 1 IN (SELECT LAST_INSERT_ID(9))";
        step.expected_id="9";
        step.expected_session="9";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=0;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="delete_expr_without_matching_row";
        step.statement="DELETE FROM {t} WHERE 1 IN (SELECT LAST_INSERT_ID(19))";
        step.expected_id="19";
        step.expected_session="19";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=0;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="insert_select_expr";
        step.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),2,0";
        step.expected_id="row:2";
        step.expected_session="19";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=2 THEN id END)=29 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="expr_zero_after_nonzero";
        step.statement="INSERT INTO {t} VALUES (100,3,LAST_INSERT_ID(0))";
        step.expected_id="0";
        step.expected_session="0";
        step.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=3 THEN v END)=0 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_two_unique_conflicts";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9),(100,2,8)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_select_no_rows";
      test.statement="REPLACE INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_middle_duplicate_atomicity";
      test.statement="INSERT INTO {t} VALUES (200,2,0),(100,3,0),(300,4,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_unique_auto_column";
      test.statement="INSERT INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_column";
      test.statement="REPLACE INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_two_conflicts";
      test.statement="REPLACE INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t}(id,uk,v) VALUES (90,1,9),(100,2,8)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_generated";
      test.statement="REPLACE INTO {t}(uk,v) VALUES (1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>100 AND MIN(v)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t}(id,uk,v) VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_pdml_explicit_single";
      test.statement="INSERT /*+ parallel(2) enable_parallel_dml */ INTO {t} SELECT id,uk,v FROM {s}";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, uk BIGINT NOT NULL, v BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(id,uk), UNIQUE KEY(uk)) ENGINE=InnoDB PARTITION BY HASH(uk) PARTITIONS 4";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.require_pdml=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_pdml_generated_single";
      test.statement="INSERT /*+ parallel(2) enable_parallel_dml */ INTO {t} SELECT id,uk,v FROM {s}";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, uk BIGINT NOT NULL, v BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(id,uk), UNIQUE KEY(uk)) ENGINE=InnoDB PARTITION BY HASH(uk) PARTITIONS 4";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.require_pdml=true;
      test.generated_auto_values=true;
      test.setup={"INSERT INTO {s} VALUES (NULL,1,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_bigint";
      test.table_definition="CREATE TABLE {t} (id bigint unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2,2,2),(3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="row:70";
        step.expected_session=(compatible ? "row:101" : "row:70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="row:71";
        step.expected_session=(compatible ? "row:101" : "row:71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="row:72";
        step.expected_session=(compatible ? "row:101" : "row:72");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "83" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_float";
      test.table_definition="CREATE TABLE {t} (id float unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2.2,2,2),(3.3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70.1,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="70";
        step.expected_session=(compatible ? "row:101" : "70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71.2,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="71";
        step.expected_session=(compatible ? "row:101" : "71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72.5,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="73";
        step.expected_session=(compatible ? "row:101" : "73");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "88" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_double";
      test.table_definition="CREATE TABLE {t} (id double unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2.2,2,2),(3.3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70.1,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="70";
        step.expected_session=(compatible ? "row:101" : "70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71.2,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="71";
        step.expected_session=(compatible ? "row:101" : "71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72.5,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="73";
        step.expected_session=(compatible ? "row:101" : "73");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "88" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_last_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,1),(200,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_values_last_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,1),(200,2,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_first_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,1)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_values_first_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,1)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_all_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_values_all_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_all_filtered_descending";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "700" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (900,1,0),(700,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_values_all_filtered_descending";
      test.statement="INSERT IGNORE INTO {t} VALUES (900,1,0),(700,2,0)";
      test.expected_id=(compatible ? "700" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_replace_error";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_replace_conflict_error";
      test.statement="REPLACE INTO {t} VALUES (200,1,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_generated";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(uk)=1 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (NULL,1,1),(200,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_values_generated";
      test.statement="INSERT IGNORE INTO {t} VALUES (NULL,1,1),(200,2,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(uk)=1 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review_check_select_duplicate_last";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=100 AND MAX(id)=500 AND SUM(v)=2 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {t} VALUES (500,2,1)", "INSERT INTO {s} VALUES (100,1,1),(200,2,1)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(0),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="row:2";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=1", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_no_expr";
      test.statement="INSERT INTO {t} SELECT 29,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(29),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(0),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=0", "BEGIN", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_no_expr";
      test.statement="INSERT INTO {t} SELECT 29,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(29),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),false,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_single";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_multi";
      test.statement="INSERT INTO {t} VALUES (300,1,0),(100,2,0),(200,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_descending";
      test.statement="INSERT INTO {t} VALUES (300,1,0),(200,2,0),(100,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=3 THEN id END)=100 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_set";
      test.statement="INSERT INTO {t} SET id=100,uk=1,v=0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_uint64";
      test.statement="INSERT INTO {t} VALUES (4294967296,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=4294967296 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_select_explicit";
      test.statement="INSERT INTO {t} SELECT 100,1,0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_select_explicit_ordered";
      test.statement="INSERT INTO {t} SELECT id,n,0 FROM (SELECT 1 AS n,300 AS id UNION ALL SELECT 2,100 UNION ALL SELECT 3,200) AS s ORDER BY n";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_new";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_primary_conflict";
      test.statement="REPLACE INTO {t} VALUES (100,2,0)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=2 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_unique_conflict";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_multi";
      test.statement="REPLACE INTO {t} VALUES (300,1,0),(100,2,0),(200,3,0)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_select_explicit";
      test.statement="REPLACE INTO {t} SELECT 100,1,0";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_last_row_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,2,0),(200,1,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=100 AND MAX(id)=500 AND SUM(v)=9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_first_row_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,0)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=200 AND MAX(id)=500 AND SUM(v)=9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="ignore_explicit_all_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,1,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=500 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_select_no_rows";
      test.statement="INSERT INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="ignore_select_no_rows";
      test.statement="INSERT IGNORE INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_generated_null";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_generated_omitted";
      test.statement="INSERT INTO {t}(uk,v) VALUES (1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_generated_zero";
      test.statement="INSERT INTO {t} VALUES (0,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_generated_multi_first_id";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0),(NULL,2,0),(NULL,3,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=3 AND COUNT(DISTINCT id)=3 AND MIN(id)>0 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_generated_before_explicit";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0),(100,2,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)>0 AND MAX(CASE WHEN uk=2 THEN id END)=100 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_explicit_before_generated";
      test.statement="INSERT INTO {t} VALUES (100,1,0),(NULL,2,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=2 THEN id END)>0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_mixed_explicit_generated_explicit";
      test.statement="INSERT INTO {t} VALUES (100,1,0),(NULL,2,0),(300,3,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=3 THEN id END)=300 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_generated_null";
      test.statement="REPLACE INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_generated_unique_conflict";
      test.statement="REPLACE INTO {t} VALUES (NULL,1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(id)<>100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_mixed_explicit_generated_explicit";
      test.statement="REPLACE INTO {t} VALUES (100,1,0),(NULL,2,0),(300,3,0)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=3 THEN id END)=300 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="ignore_generated_all_ignored";
      test.statement="INSERT IGNORE INTO {t} VALUES (NULL,1,0)";
      test.expected_id=(compatible ? "101" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_last_insert_id_expr_zero";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(0))";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_last_insert_id_expr";
      test.statement="REPLACE INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="replace_explicit_last_insert_id_expr_zero";
      test.statement="REPLACE INTO {t} VALUES (100,1,LAST_INSERT_ID(0))";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_no_auto_column";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_no_auto_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (100,1,LAST_INSERT_ID(42))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_zero_sql_mode";
      test.statement="INSERT INTO {t} VALUES (0,1,0)";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET SESSION sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_explicit_nonzero_sql_mode";
      test.statement="INSERT INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET SESSION sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_explicit_insert";
      test.statement="INSERT INTO {t} VALUES (100,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session=(compatible ? "7" : "row:1");
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_unique_conflict_old_id";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "200" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_changes_id";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE id=200,v=0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_last_insert_id_expr";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=LAST_INSERT_ID(id)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=100 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_conflict_last_insert_id_expr_zero";
      test.statement="INSERT INTO {t} VALUES (200,1,0) ON DUPLICATE KEY UPDATE v=LAST_INSERT_ID(0)";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_generated_insert";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_generated_conflict";
      test.statement="INSERT INTO {t} VALUES (NULL,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "101" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_up_mixed_insert_update";
      test.statement="INSERT INTO {t} VALUES (NULL,2,0),(200,1,0) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "200" : "row:2");
      test.expected_session=(compatible ? "7" : "row:2");
      test.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND SUM(v)=10 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_duplicate_error";
      test.statement="INSERT INTO {t} VALUES (100,2,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="insert_multi_duplicate_atomicity";
      test.statement="INSERT INTO {t} VALUES (200,2,0),(100,3,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="first_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="second_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND COUNT(DISTINCT id)=2 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="single_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="first_row_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (1),(3)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="middle_row_duplicate";
        step.statement="INSERT INTO {t}(uk) VALUES (4),(1),(5)";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_errors";
        step.statement="INSERT INTO {t}(uk) VALUES (3)";
        step.expected_id="row:3";
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_generated";
        step.statement="INSERT INTO {t} VALUES (100,100,0)";
        step.expected_id=(compatible ? "row:100" : "0");
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=4 AND MAX(CASE WHEN uk=100 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_after_explicit";
        step.statement="INSERT INTO {t} VALUES (DEFAULT,101,0)";
        step.expected_id="row:101";
        step.expected_session="row:101";
        step.data_check="SELECT COUNT(*)=5 AND MAX(id)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="omitted_after_default";
        step.statement="INSERT INTO {t}(uk) VALUES (102)";
        step.expected_id="row:102";
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=6 AND COUNT(DISTINCT id)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_after_recovery";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.data_check="SELECT COUNT(*)=6 AND COUNT(DISTINCT uk)=6 FROM {t}";
        step.affected_rows=0;
        step.expected_error=1062;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_ignore_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="first_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="second_generated";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_first";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1)";
        step.expected_id=(compatible ? "3" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_second";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (2)";
        step.expected_id=(compatible ? "4" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND SUM(uk)=3 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="first_generated_row_ignored";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1),(3)";
        step.expected_id="5";
        step.expected_session="5";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND MAX(CASE WHEN uk=3 THEN id END)=6 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="middle_generated_row_ignored";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (4),(1),(5)";
        step.expected_id="row:4";
        step.expected_session="row:4";
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=15 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_ignore";
        step.statement="INSERT IGNORE INTO {t} VALUES (100,100,0)";
        step.expected_id=(compatible ? "row:100" : "0");
        step.expected_session="row:4";
        step.data_check="SELECT COUNT(*)=6 AND MAX(CASE WHEN uk=100 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_after_explicit";
        step.statement="INSERT IGNORE INTO {t} VALUES (DEFAULT,101,0)";
        step.expected_id="row:101";
        step.expected_session="row:101";
        step.data_check="SELECT COUNT(*)=7 AND MAX(id)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="omitted_after_default";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (102)";
        step.expected_id="row:102";
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=8 AND COUNT(DISTINCT uk)=8 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="all_ignored_after_recovery";
        step.statement="INSERT IGNORE INTO {t}(uk) VALUES (1)";
        step.expected_id=(compatible ? "103" : "0");
        step.expected_session="row:102";
        step.data_check="SELECT COUNT(*)=8 AND COUNT(DISTINCT uk)=8 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_update_delete_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_multi";
        step.statement="INSERT INTO {t}(uk) VALUES (1),(2)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=2 AND COUNT(DISTINCT id)=2 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="update_preserves_session";
        step.statement="UPDATE {t} SET id=100 WHERE uk=1";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=1 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"SET @remembered_id=LAST_INSERT_ID()"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="delete_preserves_session";
        step.statement="DELETE FROM {t} WHERE uk=2";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="replace_preserves_session";
        step.statement="REPLACE INTO {t} VALUES (100,1,9)";
        step.expected_id=(compatible ? "row:1" : "0");
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=9 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_update";
        step.statement="INSERT INTO {t}(uk) VALUES (3)";
        step.expected_id="row:3";
        step.expected_session="row:3";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=3 THEN id END)>100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_manual_rollback_sequence";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="insert_in_transaction";
        step.statement="INSERT INTO {t}(uk) VALUES (1)";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=1 AND MIN(uk)=1 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"SET autocommit=0", "BEGIN"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="no_rows_after_rollback";
        step.statement="INSERT INTO {t} SELECT 100,2,0 WHERE 1=0";
        step.expected_id="0";
        step.expected_session="1";
        step.data_check="SELECT COUNT(*)=0 FROM {t}";
        step.affected_rows=0;
        step.expected_error=0;
        step.setup={"SET @rollback_id=LAST_INSERT_ID()", "ROLLBACK"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_rollback";
        step.statement="INSERT INTO {t}(uk) VALUES (2)";
        step.expected_id="row:2";
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=1 AND MIN(uk)=2 AND MIN(id)>@rollback_id FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_after_commit";
        step.statement="INSERT INTO {t} VALUES (100,3,0)";
        step.expected_id=(compatible ? "row:3" : "0");
        step.expected_session="row:2";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=3 THEN id END)=100 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        step.setup={"COMMIT", "SET autocommit=1"};
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_last_insert_id_expr_sequence";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      {
        Case step;
        step.name="no_auto_column";
        step.statement="INSERT INTO {t} VALUES (7,1,0)";
        step.expected_id="0";
        step.expected_session="0";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        step.setup={"SELECT LAST_INSERT_ID(0)"};
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="update_expr_without_matching_row";
        step.statement="UPDATE {t} SET id=1 WHERE 1 IN (SELECT LAST_INSERT_ID(9))";
        step.expected_id="9";
        step.expected_session="9";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=0;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="delete_expr_without_matching_row";
        step.statement="DELETE FROM {t} WHERE 1 IN (SELECT LAST_INSERT_ID(19))";
        step.expected_id="19";
        step.expected_session="19";
        step.data_check="SELECT COUNT(*)=1 AND MIN(id)=7 FROM {t}";
        step.affected_rows=0;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="insert_select_expr";
        step.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),2,0";
        step.expected_id="row:2";
        step.expected_session="19";
        step.data_check="SELECT COUNT(*)=2 AND MAX(CASE WHEN uk=2 THEN id END)=29 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="expr_zero_after_nonzero";
        step.statement="INSERT INTO {t} VALUES (100,3,LAST_INSERT_ID(0))";
        step.expected_id="0";
        step.expected_session="0";
        step.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=3 THEN v END)=0 FROM {t}";
        step.affected_rows=1;
        step.auto_increment=false;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_two_unique_conflicts";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9),(100,2,8)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_select_no_rows";
      test.statement="REPLACE INTO {t} SELECT 100,1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_middle_duplicate_atomicity";
      test.statement="INSERT INTO {t} VALUES (200,2,0),(100,3,0),(300,4,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_unique_auto_column";
      test.statement="INSERT INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_column";
      test.statement="REPLACE INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_two_conflicts";
      test.statement="REPLACE INTO {t}(id,uk,v) VALUES (100,1,0)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t}(id,uk,v) VALUES (90,1,9),(100,2,8)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_replace_unique_auto_generated";
      test.statement="REPLACE INTO {t}(uk,v) VALUES (1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>100 AND MIN(v)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (uk BIGINT NOT NULL PRIMARY KEY, id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, v BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t}(id,uk,v) VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_pdml_explicit_single";
      test.statement="INSERT /*+ parallel(2) enable_parallel_dml */ INTO {t} SELECT id,uk,v FROM {s}";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, uk BIGINT NOT NULL, v BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(id,uk), UNIQUE KEY(uk)) ENGINE=InnoDB PARTITION BY HASH(uk) PARTITIONS 4";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.require_pdml=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_pdml_generated_single";
      test.statement="INSERT /*+ parallel(2) enable_parallel_dml */ INTO {t} SELECT id,uk,v FROM {s}";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, uk BIGINT NOT NULL, v BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(id,uk), UNIQUE KEY(uk)) ENGINE=InnoDB PARTITION BY HASH(uk) PARTITIONS 4";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.require_pdml=true;
      test.generated_auto_values=true;
      test.setup={"INSERT INTO {s} VALUES (NULL,1,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_bigint";
      test.table_definition="CREATE TABLE {t} (id bigint unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2,2,2),(3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="row:70";
        step.expected_session=(compatible ? "row:101" : "row:70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="row:71";
        step.expected_session=(compatible ? "row:101" : "row:71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="row:72";
        step.expected_session=(compatible ? "row:101" : "row:72");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "83" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_float";
      test.table_definition="CREATE TABLE {t} (id float unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2.2,2,2),(3.3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70.1,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="70";
        step.expected_session=(compatible ? "row:101" : "70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71.2,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="71";
        step.expected_session=(compatible ? "row:101" : "71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72.5,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="73";
        step.expected_session=(compatible ? "row:101" : "73");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "88" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="mysqltest_insert_up_full_sequence_double";
      test.table_definition="CREATE TABLE {t} (id double unsigned NOT NULL AUTO_INCREMENT, uk int, v int, PRIMARY KEY (id,uk), UNIQUE KEY (v))";
      test.affected_rows=0;
      test.expected_error=0;
      {
        Case step;
        step.name="generated_initial";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3) on duplicate key update uk = uk + 100";
        step.expected_id="row:1";
        step.expected_session="row:1";
        step.data_check="SELECT COUNT(*)=3 AND SUM(uk)=6 AND SUM(v)=6 FROM {t}";
        step.affected_rows=3;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_middle_conflict";
        step.statement="insert into {t}(id,uk,v) values(default,21,21),(default,21,21),(default,23,23) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:23" : "row:121");
        step.expected_session=(compatible ? "row:1" : "row:121");
        step.data_check="SELECT COUNT(*)=5 AND SUM(uk)=150 AND SUM(v)=50 FROM {t}";
        step.affected_rows=4;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_leading_conflicts";
        step.statement="insert into {t}(uk,v) values(1,1),(2,2),(3,3),(8,8),(9,9) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:9" : "row:8");
        step.expected_session=(compatible ? "row:101" : "row:8");
        step.data_check="SELECT COUNT(*)=7 AND SUM(uk)=467 AND SUM(v)=67 FROM {t}";
        step.affected_rows=8;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_then_explicit_conflicts";
        step.statement="insert into {t}(id,uk,v) values(default,30,30),(2.2,2,2),(3.3,3,3) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:203" : "row:30");
        step.expected_session=(compatible ? "row:101" : "row:30");
        step.data_check="SELECT COUNT(*)=8 AND SUM(uk)=697 AND SUM(v)=97 FROM {t}";
        step.affected_rows=5;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_70";
        step.statement="insert into {t}(id,uk,v) values(70.1,70,70) on duplicate key update uk = uk + 100";
        step.expected_id="70";
        step.expected_session=(compatible ? "row:101" : "70");
        step.data_check="SELECT COUNT(*)=9 AND SUM(uk)=767 AND SUM(v)=167 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_71";
        step.statement="insert into {t}(id,uk,v) values(71.2,71,71) on duplicate key update uk = uk + 100";
        step.expected_id="71";
        step.expected_session=(compatible ? "row:101" : "71");
        step.data_check="SELECT COUNT(*)=10 AND SUM(uk)=838 AND SUM(v)=238 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="explicit_72";
        step.statement="insert into {t}(id,uk,v) values(72.5,72,72) on duplicate key update uk = uk + 100";
        step.expected_id="73";
        step.expected_session=(compatible ? "row:101" : "73");
        step.data_check="SELECT COUNT(*)=11 AND SUM(uk)=910 AND SUM(v)=310 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_after_explicit";
        step.statement="insert into {t}(id,uk,v) values(default,2,2),(default,3,3),(default,50,50),(default,51,51),(default,52,52) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "row:52" : "row:50");
        step.expected_session=(compatible ? "row:101" : "row:50");
        step.data_check="SELECT COUNT(*)=14 AND SUM(uk)=1263 AND SUM(v)=463 FROM {t}";
        step.affected_rows=7;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_61";
        step.statement="insert into {t}(uk,v) values(61,61) on duplicate key update uk = uk + 100";
        step.expected_id="row:61";
        step.expected_session="row:61";
        step.data_check="SELECT COUNT(*)=15 AND SUM(uk)=1324 AND SUM(v)=524 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id="row:62";
        step.expected_session="row:62";
        step.data_check="SELECT COUNT(*)=16 AND SUM(uk)=1386 AND SUM(v)=586 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id="row:63";
        step.expected_session="row:63";
        step.data_check="SELECT COUNT(*)=17 AND SUM(uk)=1449 AND SUM(v)=649 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id="row:64";
        step.expected_session="row:64";
        step.data_check="SELECT COUNT(*)=18 AND SUM(uk)=1513 AND SUM(v)=713 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="generated_65";
        step.statement="insert into {t}(uk,v) values(65,65) on duplicate key update uk = uk + 100";
        step.expected_id="row:65";
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1578 AND SUM(v)=778 FROM {t}";
        step.affected_rows=1;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_62";
        step.statement="insert into {t}(uk,v) values(62,62) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "84" : "row:162");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1678 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_63";
        step.statement="insert into {t}(uk,v) values(63,63) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "85" : "row:163");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1778 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="duplicate_64";
        step.statement="insert into {t}(uk,v) values(64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "86" : "row:164");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1878 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_64";
        step.statement="insert into {t} values(default,64,64) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "87" : "row:264");
        step.expected_session="row:65";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=1978 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      {
        Case step;
        step.name="default_duplicate_65";
        step.statement="insert into {t} values(default,65,65) on duplicate key update uk = uk + 100";
        step.expected_id=(compatible ? "88" : "row:165");
        step.expected_session="row:165";
        step.data_check="SELECT COUNT(*)=19 AND SUM(uk)=2078 AND SUM(v)=778 FROM {t}";
        step.affected_rows=2;
        step.expected_error=0;
        test.steps.push_back(std::move(step));
      }
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_last_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,1),(200,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_values_last_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,1),(200,2,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_first_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,1)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_values_first_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,1)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_all_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_values_all_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (100,1,0),(200,2,0)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_all_filtered_descending";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "700" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (900,1,0),(700,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_values_all_filtered_descending";
      test.statement="INSERT IGNORE INTO {t} VALUES (900,1,0),(700,2,0)";
      test.expected_id=(compatible ? "700" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_replace_error";
      test.statement="REPLACE INTO {t} VALUES (100,1,0)";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_replace_conflict_error";
      test.statement="REPLACE INTO {t} VALUES (200,1,0)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_generated";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(uk)=1 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (NULL,1,1),(200,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_values_generated";
      test.statement="INSERT IGNORE INTO {t} VALUES (NULL,1,1),(200,2,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(uk)=1 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review_check_select_duplicate_last";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=100 AND MAX(id)=500 AND SUM(v)=2 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {t} VALUES (500,2,1)", "INSERT INTO {s} VALUES (100,1,1),(200,2,1)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(0),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="row:2";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=1", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_no_expr";
      test.statement="INSERT INTO {t} SELECT 29,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_autocommit_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(29),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(0),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0 WHERE 1=0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=0", "BEGIN", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_no_expr";
      test.statement="INSERT INTO {t} SELECT 29,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(29),1,0";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="review8_transaction_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(29),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_signed";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=-9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(-9)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_unsigned";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=9223372036854775908 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::unsigned_integer(9223372036854775908ULL),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_string_id";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::string("100"),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_null";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_zero";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(0),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_omitted";
      test.statement="INSERT INTO {t}(uk,v) VALUES (?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_default";
      test.statement="INSERT INTO {t} VALUES (DEFAULT,?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_explicit_multi";
      test.statement="INSERT INTO {t} VALUES (?,?,?),(?,?,?),(?,?,?)";
      test.expected_id=(compatible ? "row:3" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=3 AND SUM(id)=600 AND MAX(CASE WHEN uk=3 THEN id END)=200 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.parameters={Parameter::integer(300),Parameter::integer(1),Parameter::integer(0),Parameter::integer(100),Parameter::integer(2),Parameter::integer(0),Parameter::integer(200),Parameter::integer(3),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_multi_null_bitmap";
      test.statement="INSERT INTO {t} VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=4 AND COUNT(DISTINCT id)=4 AND MIN(id)>0 FROM {t}";
      test.affected_rows=4;
      test.expected_error=0;
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0),Parameter::null(),Parameter::integer(2),Parameter::integer(0),Parameter::null(),Parameter::integer(3),Parameter::integer(0),Parameter::null(),Parameter::integer(4),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_mixed";
      test.statement="INSERT INTO {t} VALUES (?,?,?),(?,?,?),(?,?,?)";
      test.expected_id="row:2";
      test.expected_session="row:2";
      test.data_check="SELECT COUNT(*)=3 AND MAX(CASE WHEN uk=1 THEN id END)=100 AND MAX(CASE WHEN uk=3 THEN id END)=300 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0),Parameter::null(),Parameter::integer(2),Parameter::integer(0),Parameter::integer(300),Parameter::integer(3),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_select";
      test.statement="INSERT INTO {t} SELECT ?,?,?";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_replace_explicit";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_replace_primary_conflict";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=2 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(100),Parameter::integer(2),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_replace_two_conflicts";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "row:1" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=3;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9),(100,2,8)"};
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_replace_generated";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(id)<>100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_expr";
      test.statement="INSERT INTO {t} VALUES (?,?,LAST_INSERT_ID(?))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(42)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_expr_zero";
      test.statement="INSERT INTO {t} VALUES (?,?,LAST_INSERT_ID(?))";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_replace_expr";
      test.statement="REPLACE INTO {t} VALUES (?,?,LAST_INSERT_ID(?))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (90,1,9)"};
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(42)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_ignore_partial";
      test.statement="INSERT IGNORE INTO {t} VALUES (?,?,?),(?,?,?)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=100 AND MAX(id)=500 AND SUM(v)=9 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      test.parameters={Parameter::integer(100),Parameter::integer(2),Parameter::integer(0),Parameter::integer(200),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_ignore_generated_all";
      test.statement="INSERT IGNORE INTO {t} VALUES (?,?,?)";
      test.expected_id=(compatible ? "101" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_ignore_all";
      test.statement="INSERT IGNORE INTO {t} VALUES (?,?,?),(?,?,?)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=500 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (500,1,9)"};
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0),Parameter::integer(200),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_zero_sql_mode";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET SESSION sql_mode='STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO'"};
      test.parameters={Parameter::integer(0),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_duplicate_error";
      test.statement="INSERT INTO {t} VALUES (?,?,?)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(100),Parameter::integer(2),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_middle_duplicate";
      test.statement="INSERT INTO {t} VALUES (?,?,?),(?,?,?),(?,?,?)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.affected_rows=0;
      test.expected_error=1062;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(200),Parameter::integer(2),Parameter::integer(0),Parameter::integer(100),Parameter::integer(3),Parameter::integer(0),Parameter::integer(300),Parameter::integer(4),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_up_generated";
      test.statement="INSERT INTO {t} VALUES (?,?,?) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)>0 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_up_explicit";
      test.statement="INSERT INTO {t} VALUES (?,?,?) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id="row:1";
      test.expected_session=(compatible ? "7" : "row:1");
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_up_generated_conflict";
      test.statement="INSERT INTO {t} VALUES (?,?,?) ON DUPLICATE KEY UPDATE v=v+1";
      test.expected_id=(compatible ? "101" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::null(),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_up_conflict";
      test.statement="INSERT INTO {t} VALUES (?,?,?) ON DUPLICATE KEY UPDATE v=v+?";
      test.expected_id=(compatible ? "200" : "row:1");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=10 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(200),Parameter::integer(1),Parameter::integer(0),Parameter::integer(1)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_insert_up_expr_zero";
      test.statement="INSERT INTO {t} VALUES (?,?,?) ON DUPLICATE KEY UPDATE v=LAST_INSERT_ID(?)";
      test.expected_id="0";
      test.expected_session="0";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=0 FROM {t}";
      test.affected_rows=2;
      test.expected_error=0;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(200),Parameter::integer(1),Parameter::integer(0),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_no_auto_expr";
      test.statement="INSERT INTO {t} VALUES (?,?,LAST_INSERT_ID(?))";
      test.expected_id="42";
      test.expected_session="42";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=42 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(42)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(0),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),1,0 WHERE 1=?";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id+?),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="row:2";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=1", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      test.parameters={Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(?),1,0";
      test.expected_id="row:1";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_no_expr";
      test.statement="INSERT INTO {t} SELECT ?,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_autocommit_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(?),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=1"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_positive";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_zero";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=0 AND MIN(uk)=1 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(0),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_auto_column";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),?,?";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_empty";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(?),1,0 WHERE 1=?";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.affected_rows=0;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_source";
      test.statement="INSERT INTO {t} SELECT LAST_INSERT_ID(id+?),uk,0 FROM {s} ORDER BY uk";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=2 AND MIN(id)=29 AND MAX(id)=39 AND SUM(uk)=3 FROM {t}";
      test.affected_rows=2;
      test.auto_increment=false;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"SET autocommit=0", "BEGIN", "INSERT INTO {s} VALUES (29,1,0),(39,2,0)"};
      test.parameters={Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_replace";
      test.statement="REPLACE INTO {t} SELECT LAST_INSERT_ID(?),1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_no_expr";
      test.statement="INSERT INTO {t} SELECT ?,1,0";
      test.expected_id="0";
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review8_transaction_values";
      test.statement="INSERT INTO {t} VALUES (LAST_INSERT_ID(?),1,0)";
      test.expected_id="row:1";
      test.expected_session="row:1";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=29 AND MIN(uk)=1 AND MIN(v)=0 FROM {t}";
      test.affected_rows=1;
      test.auto_increment=false;
      test.expected_error=0;
      test.setup={"SET autocommit=0", "BEGIN"};
      test.parameters={Parameter::integer(29)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_values";
      test.statement="INSERT IGNORE INTO {t} VALUES (?,?,?),(?,?,?)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(1),Parameter::integer(200),Parameter::integer(2),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_values_all_filtered";
      test.statement="INSERT IGNORE INTO {t} VALUES (?,?,?),(?,?,?)";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0),Parameter::integer(200),Parameter::integer(2),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_select_last_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} WHERE uk>? ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,1),(200,2,0)"};
      test.parameters={Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_select_first_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} WHERE uk>? ORDER BY uk";
      test.expected_id=(compatible ? "row:2" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=200 AND MIN(v)=1 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=1;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,1)"};
      test.parameters={Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_select_all_filtered";
      test.statement="INSERT IGNORE INTO {t} SELECT id,uk,v FROM {s} WHERE uk>? ORDER BY uk";
      test.expected_id=(compatible ? "200" : "0");
      test.expected_session="7";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=0;
      test.source_table=true;
      test.setup={"INSERT INTO {s} VALUES (100,1,0),(200,2,0)"};
      test.parameters={Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_replace_error";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.data_check="SELECT COUNT(*)=0 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      test.parameters={Parameter::integer(100),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
    {
      Case test;
      test.name="bound_review_check_replace_conflict_error";
      test.statement="REPLACE INTO {t} VALUES (?,?,?)";
      test.data_check="SELECT COUNT(*)=1 AND MIN(id)=100 AND MIN(uk)=1 AND MIN(v)=9 FROM {t}";
      test.table_definition="CREATE TABLE {t} (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, uk BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL CHECK(v>0)) ENGINE=InnoDB";
      test.affected_rows=0;
      test.expected_error=3819;
      test.setup={"INSERT INTO {t} VALUES (100,1,9)"};
      test.parameters={Parameter::integer(200),Parameter::integer(1),Parameter::integer(0)};
      result.push_back({std::move(test),true,compatible});
    }
  }
  return result;
}
const char *test_env_prefix()
{
  return "test_last_insert_id_";
}

} // namespace last_insert_id
} // namespace unittest
} // namespace oceanbase

namespace oceanbase
{
namespace unittest
{
using namespace last_insert_id;

class TestLastInsertId : public ObSimpleClusterTestBase,
                        public ::testing::WithParamInterface<ProtocolCase>
{
public:
  TestLastInsertId() : ObSimpleClusterTestBase(test_env_prefix()) {}

  static void TearDownTestCase()
  {
    if (config_connection_) {
      try {
        config_connection_->query("ALTER SYSTEM SET _enable_last_insert_id_42x_compatibility=" + original_compatibility_);
      } catch (const std::exception &error) {
        ADD_FAILURE() << "restore compatibility parameter: " << error.what();
      }
      config_connection_.reset();
    }
    ObSimpleClusterTestBase::TearDownTestCase();
  }


protected:
  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      tenant_created_ = true;
    }
    try {

      if (!config_connection_) {
        config_connection_.reset(new Connection(get_curr_simple_server().get_local_ip(),
            get_curr_simple_server().get_mysql_port(), "root@tt1", "", "test"));
        original_compatibility_ = config_connection_->query(
            "SELECT DISTINCT LOWER(VALUE) FROM oceanbase.GV$OB_PARAMETERS "
            "WHERE NAME='_enable_last_insert_id_42x_compatibility'").at(0).at(0);
        current_compatibility_ = original_compatibility_ == "true" || original_compatibility_ == "1";
      }
      if (current_compatibility_ != GetParam().compatible) {
        config_connection_->query(std::string("ALTER SYSTEM SET _enable_last_insert_id_42x_compatibility=")
            + (GetParam().compatible ? "true" : "false"));
        bool applied = false;
        for (unsigned retry = 0; retry < 100 && !applied; ++retry) {
          applied = config_connection_->scalar(
              "SELECT MIN(LOWER(VALUE) IN ('true','1')) FROM oceanbase.GV$OB_PARAMETERS "
              "WHERE NAME='_enable_last_insert_id_42x_compatibility'") == GetParam().compatible;
          if (!applied) { usleep(100000); }
        }
        if (!applied) { throw std::runtime_error("Compatibility parameter did not propagate"); }
        // Pooled sessions retain tenant-config caches across reconnects.
        // Wait past the five-second refresh interval before testing the new value.
        sleep(6);
        current_compatibility_ = GetParam().compatible;
      }
      connection_.reset(new Connection(get_curr_simple_server().get_local_ip(),
          get_curr_simple_server().get_mysql_port(), "root@tt1", "", "test", GetParam().test.multi_statement));
      if (GetParam().test.force_internal_batch) {
        original_values_split_opt_ = GCONF._sql_insert_multi_values_split_opt;
        internal_batch_config_changed_ = true;
        // Prevent VALUES splitting from bypassing the operator's internal batches.
        GCONF._sql_insert_multi_values_split_opt = false;
        Connection admin(get_curr_simple_server().get_local_ip(),
            get_curr_simple_server().get_mysql_port(), "root@sys", "", "oceanbase");
        admin.query("ALTER SYSTEM SET_TP tp_no=402,error_code=" + std::to_string(GetParam().test.batch_row_count) + ",frequency=1");
      }
      if (!GetParam().test.force_internal_batch && GetParam().test.values_split != -1) {
        original_values_split_opt_ = GCONF._sql_insert_multi_values_split_opt;
        values_split_changed_ = true;
        GCONF._sql_insert_multi_values_split_opt = GetParam().test.values_split != 0;
      }
      if (GetParam().test.peer_connection) {
        peer_connection_.reset(new Connection(get_curr_simple_server().get_local_ip(),
            get_curr_simple_server().get_mysql_port(), "root@tt1", "", "test"));
      }
      if (GetParam().test.multi_statement) {
        // The mittest framework disables performance events by default.
        // Plan-cache execution deltas require these counters.
        original_perf_event_ = GCONF.enable_perf_event;
        GCONF.enable_perf_event = true;
        batch_perf_changed_ = true;
        batch_config_ = connection_->query("SELECT DISTINCT LOWER(VALUE) FROM oceanbase.GV$OB_PARAMETERS "
            "WHERE NAME='ob_enable_batched_multi_statement'").at(0).at(0);
        batch_config_changed_ = true;
        set_batch_config(GetParam().test.enable_batch);
        // ObCachedTenantConfigInfo refreshes after five seconds, and pooled
        // sessions retain that cache across reconnects. Wait past the interval
        // before testing either setting; reconnecting alone is insufficient.
        sleep(6);
        connection_->query("SET SESSION ob_enable_plan_cache=1");
      }
      connection_->query(create_table_sql(table_, GetParam().test));
      table_created_ = true;
      if (GetParam().test.source_table) {
        connection_->query(create_source_table_sql(table_));
        source_table_created_ = true;
      }
      const auto &reference = oceanbase::unittest::last_insert_id::mysql_reference();
      if (reference.enabled && !GetParam().test.force_internal_batch) {
        mysql_connection_.reset(new Connection(reference.host, reference.port, reference.user,
                                               reference.password, reference.database,
                                               GetParam().test.multi_statement));
        if (GetParam().test.peer_connection) {
          mysql_peer_connection_.reset(new Connection(reference.host, reference.port, reference.user,
                                                      reference.password, reference.database));
        }
        const Rows identity = mysql_connection_->query("SELECT VERSION(),@@version_comment");
        if (identity.size() != 1 || identity.front().size() != 2 ||
            identity.front().at(1).find("MySQL") == std::string::npos) {
          throw std::runtime_error("Reference server is not identified as MySQL");
        }
        mysql_version_ = identity.front().at(0);
        mysql_table_ = "last_insert_id_compare_" + std::to_string(getpid()) +
                       "_" + std::to_string(++mysql_table_counter);
        mysql_connection_->query(create_table_sql(mysql_table_, GetParam().test));
        mysql_table_created_ = true;
        if (GetParam().test.source_table) {
          mysql_connection_->query(create_source_table_sql(mysql_table_));
          mysql_source_table_created_ = true;
        }
      }
    } catch (const std::exception &error) {
      FAIL() << error.what();
    }
  }

  void TearDown() override
  {
    peer_connection_.reset();
    mysql_peer_connection_.reset();
    if (mysql_table_created_) {
      try {
        mysql_connection_->query("ROLLBACK");
        for (const auto &sql : GetParam().test.cleanup) { mysql_connection_->query(expand(sql, mysql_table_)); }
        if (mysql_source_table_created_) {
          mysql_connection_->query(expand("DROP TABLE {s}", mysql_table_));
        }
        mysql_connection_->query("DROP TABLE `" + mysql_table_ + "`");
      } catch (const std::exception &error) {
        ADD_FAILURE() << "MySQL cleanup " << mysql_table_ << ": " << error.what();
      }
    }
    mysql_connection_.reset();
    if (table_created_) {
      try {
        connection_->query("ROLLBACK");
        for (const auto &sql : GetParam().test.cleanup) { connection_->query(expand(sql, table_)); }
        if (source_table_created_) {
          connection_->query(expand("DROP TABLE {s}", table_));
        }
        connection_->query("DROP TABLE `" + table_ + "`");
      } catch (const std::exception &error) {
        ADD_FAILURE() << "cleanup " << table_ << ": " << error.what();
      }
    }
    if (batch_config_changed_) {
      try {
        set_batch_config(batch_config_ == "true" || batch_config_ == "1");
      } catch (const std::exception &error) {
        ADD_FAILURE() << "restore batch configuration: " << error.what();
      }
    }
    if (internal_batch_config_changed_) {
      GCONF._sql_insert_multi_values_split_opt = original_values_split_opt_;
      try {
        Connection admin(get_curr_simple_server().get_local_ip(),
            get_curr_simple_server().get_mysql_port(), "root@sys", "", "oceanbase");
        admin.query("ALTER SYSTEM SET_TP tp_no=402,error_code=0,frequency=0");
      } catch (const std::exception &error) {
        ADD_FAILURE() << "restore internal batch tracepoint: " << error.what();
      }
    }
    if (values_split_changed_) { GCONF._sql_insert_multi_values_split_opt = original_values_split_opt_; }
    if (batch_perf_changed_) {
      GCONF.enable_perf_event = original_perf_event_;
    }
    connection_.reset();
    ObSimpleClusterTestBase::TearDown();
  }

  void set_batch_config(bool enabled)
  {
    connection_->query(std::string("ALTER SYSTEM SET ob_enable_batched_multi_statement=") +
                       (enabled ? "true" : "false"));
    for (unsigned attempt = 0; attempt < 100; ++attempt) {
      const Id actual = connection_->scalar("SELECT LOWER(VALUE) IN ('true','1') "
          "FROM oceanbase.GV$OB_PARAMETERS WHERE NAME='ob_enable_batched_multi_statement'");
      if ((actual != 0) == enabled) { return; }
      usleep(100000);
    }
    throw std::runtime_error("Timed out waiting for the tenant batch configuration");
  }

  static std::unique_ptr<Connection> config_connection_;
  static std::string original_compatibility_;
  static bool current_compatibility_;
  bool values_split_changed_ = false;
  static bool tenant_created_;
  std::unique_ptr<Connection> connection_;
  std::unique_ptr<Connection> peer_connection_;
  std::unique_ptr<Connection> mysql_connection_;
  std::unique_ptr<Connection> mysql_peer_connection_;
  std::string mysql_table_;
  std::string mysql_version_;
  bool mysql_table_created_ = false;
  bool mysql_source_table_created_ = false;
  const std::string table_ = "lid_pc_" + std::to_string(getpid()) + "_" + std::to_string(++mysql_table_counter);
  bool table_created_ = false;
  bool source_table_created_ = false;
  std::string batch_config_;
  bool batch_config_changed_ = false;
  bool original_perf_event_ = false;
  bool batch_perf_changed_ = false;
  bool original_values_split_opt_ = true;
  bool internal_batch_config_changed_ = false;
};

bool TestLastInsertId::tenant_created_ = false;
std::unique_ptr<Connection> TestLastInsertId::config_connection_;
std::string TestLastInsertId::original_compatibility_;
bool TestLastInsertId::current_compatibility_ = false;

TEST_P(TestLastInsertId, ok_packet_and_session)
{
  SCOPED_TRACE(GetParam().test.name + ": " + GetParam().test.statement);
  try {
    Observed ob;
    Rows batch_plans;
    for (const auto &failure : run_case(*connection_, GetParam().test, table_, GetParam().prepared,
                                        mysql_connection_ ? &ob : nullptr, true, true,
                                        GetParam().test.multi_statement ? &batch_plans : nullptr,
                                        peer_connection_.get())) {
      ADD_FAILURE() << failure;
    }
    if (GetParam().test.multi_statement) {
      Id batched = 0;
      Id ordinary = 0;
      for (const auto &plan : batch_plans) {
        if (std::stoull(plan.at(4)) == 1) { ++batched; } else { ++ordinary; }
      }
      EXPECT_FALSE(batch_plans.empty()) << "No executed plan found: batch path cannot be determined";
      EXPECT_EQ(GetParam().test.expect_batch, batched > 0)
          << "Plan cache batch flag does not match the required path";
      if (GetParam().test.expect_batch) {
        EXPECT_EQ(0, ordinary) << "Unexpected ordinary execution alongside batch plan";
      }
    }
    if (mysql_connection_) {
      Observed mysql;
      // These are OB regression expectations, not universal MySQL expectations.
      // Run the optional reference as a diagnostic and retain its raw results;
      // intentional/historical differences must not fail the OB regression suite.
      run_case(*mysql_connection_, GetParam().test, mysql_table_, GetParam().prepared,
               &mysql, false, true, nullptr, mysql_peer_connection_.get());
      const auto &test = GetParam().test;
      if (test.steps.empty()) {
        report_comparison(test, GetParam().prepared, ob, mysql, mysql_version_, mysql_table_);
      } else {
        ASSERT_EQ(test.steps.size(), ob.steps.size());
        ASSERT_EQ(test.steps.size(), mysql.steps.size());
        for (std::size_t i = 0; i < test.steps.size(); ++i) {
          Case step = test.steps[i];
          step.name = test.name + "." + step.name;
          report_comparison(step, GetParam().prepared,
              ob.steps[i], mysql.steps[i], mysql_version_, mysql_table_);
        }
      }
    }
  } catch (const std::exception &error) {
    FAIL() << error.what();
  }
}

INSTANTIATE_TEST_CASE_P(MySQLProtocols, TestLastInsertId,
    ::testing::ValuesIn(protocol_cases()),
    [](const ::testing::TestParamInfo<ProtocolCase> &info) {
      return std::string(info.param.compatible ? "On_" : "Off_") + (info.param.prepared ? "PS_" : "Text_") + info.param.test.name;
    });

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  try {
    const auto &reference = oceanbase::unittest::last_insert_id::mysql_reference();
    if (reference.enabled) {
      std::ofstream report(reference.report, std::ios::trunc);
      if (!report) {
        throw std::runtime_error("Cannot create MySQL comparison report");
      }
    }
  } catch (const std::exception &error) {
    std::cerr << error.what() << std::endl;
    return 2;
  }
  // The framework owns this test's run directory; do not remove pre-existing logs.
  oceanbase::unittest::init_log_and_gtest(argc, argv, false);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
