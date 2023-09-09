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

#ifndef OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_
#define OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_

#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{

namespace sqlclient
{
class ObISQLResultHandler;
class ObMySQLResult;
class ObISQLConnectionPool;
class ObIExecutor;
};

inline bool is_zero_row(const int64_t row_count) { return 0 == row_count; }
inline bool is_single_row(const int64_t row_count) { return 1 == row_count; }
inline bool is_double_row(const int64_t row_count) { return 2 == row_count; }


class ObISQLClient
{
public:
  class ReadResult;

  ObISQLClient() : active_(true) {}
  virtual ~ObISQLClient() {}

  // sql string escape
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) = 0;
  int escape(const char *from, const int64_t from_size,
             char *to, const int64_t to_size);

  // FIXME baihua: replace 'const char *' with 'const ObString &'
  // execute query and return data result
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) = 0;
  virtual int read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql) = 0;
  virtual int read(ReadResult &res, const char *sql) { return this->read(res, OB_SYS_TENANT_ID, sql); }
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) = 0;

  // execute update sql
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) = 0;
  virtual int write(const char *sql, int64_t &affected_rows) { return this->write(OB_SYS_TENANT_ID, sql, affected_rows); }
  virtual int write(const uint64_t tenant_id, const char *sql,  const int32_t group_id, int64_t &affected_rows) = 0;

  // executor execute
  int execute(const uint64_t tenant_id, sqlclient::ObIExecutor &executor)
  {
    UNUSEDx(tenant_id, executor);
    return OB_NOT_SUPPORTED;
  }

  virtual sqlclient::ObISQLConnectionPool *get_pool() = 0;
  virtual sqlclient::ObISQLConnection *get_connection() = 0;

  virtual bool is_oracle_mode() const = 0;

  class ReadResult
  {
  public:
    const static int64_t BUF_SIZE = 24 * 1024;
    friend class ObISQLClient;

    ReadResult();
    virtual ~ReadResult();

    sqlclient::ObMySQLResult *mysql_result();
    // FIXME baihua: remove
    sqlclient::ObMySQLResult *get_result() { return mysql_result(); }
    int close();
    void reset();
    void reuse();
    void set_enable_use_result(bool val) { enable_use_result_ = val; }
    bool is_enable_use_result() { return enable_use_result_; }

    template <typename T, typename... Args>
    int create_handler(T *&res, Args &... args)
    {
      static_assert(sizeof(T) <= sizeof(buf_), "buffer not enough");
      if (NULL != result_handler_) {
        reset();
      }
      res = new (buf_) T(args...);
      result_handler_ = res;
      return common::OB_SUCCESS;
    }
  private:
    sqlclient::ObISQLResultHandler *result_handler_;
    char buf_[BUF_SIZE];
    bool enable_use_result_; // only dblink set will it to true, in order to use mysql_use_result()
  };

  bool is_active() const { return active_; }
  void set_active() { active_ = true; }
  void set_inactive();
protected:
  volatile bool active_;
};

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_
