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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_STATEMENT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_STATEMENT__

#include <mysql.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
struct ObBindParam
{
  ObBindParam() : col_idx_(-1), buffer_type_(enum_field_types::MAX_NO_FIELD_TYPES), buffer_(nullptr),
                  buffer_len_(0), length_(0), is_unsigned_(0), is_null_(0)
  {
  }
  ObBindParam(int64_t col_idx, enum_field_types buffer_type,
              void *buffer, int64_t buffer_len, unsigned long length)
    : col_idx_(col_idx), buffer_type_(buffer_type), buffer_(buffer), buffer_len_(buffer_len),
      length_(length), is_unsigned_(0), is_null_(0)
  {
  }
  ObBindParam(int64_t col_idx, enum_field_types buffer_type,
              void *buffer, int64_t buffer_len, unsigned long length,
              my_bool is_unsigned, my_bool is_null)
    : col_idx_(col_idx), buffer_type_(buffer_type), buffer_(buffer), buffer_len_(buffer_len),
      length_(length), is_unsigned_(is_unsigned), is_null_(is_null)
  {
  }
  int assign(const ObBindParam &other);
  TO_STRING_KV(K_(col_idx),
               K_(buffer_type),
               K_(buffer),
               K_(buffer_len),
               K_(length),
               K_(is_unsigned),
               K_(is_null));

public:
  int64_t col_idx_;
  enum_field_types buffer_type_;
  void *buffer_;
  int64_t buffer_len_;
  unsigned long length_;
  my_bool is_unsigned_;
  my_bool is_null_;
};

class ObBindParamEncode
{
public:
  #define ENCODE_FUNC_ARG_DECL const int64_t col_idx,           \
                               const bool is_output_param,      \
                               const ObTimeZoneInfo &tz_info,   \
                               ObObjParam &param,               \
                               ObBindParam &bind_param,         \
                               ObIAllocator &allocator
  #define DEF_ENCODE_FUNC(name)   \
  static int encode_##name(ENCODE_FUNC_ARG_DECL);
  using EncodeFunc = int (*)(ENCODE_FUNC_ARG_DECL);

  DEF_ENCODE_FUNC(null);
  DEF_ENCODE_FUNC(int);
  DEF_ENCODE_FUNC(uint);
  DEF_ENCODE_FUNC(float);
  DEF_ENCODE_FUNC(ufloat);
  DEF_ENCODE_FUNC(double);
  DEF_ENCODE_FUNC(udouble);
  DEF_ENCODE_FUNC(number);
  DEF_ENCODE_FUNC(unumber);
  DEF_ENCODE_FUNC(datetime);
  DEF_ENCODE_FUNC(date);
  DEF_ENCODE_FUNC(time);
  DEF_ENCODE_FUNC(year);
  DEF_ENCODE_FUNC(string);
  static int encode_not_supported(ENCODE_FUNC_ARG_DECL);

public:
  static const EncodeFunc encode_map_[ObMaxType + 1];
};

class ObBindParamDecode
{
public:
  #define DECODE_FUNC_ARG_DECL const enum_field_types field_type,       \
                               const ObTimeZoneInfo &tz_info,           \
                               const ObBindParam &bind_param,           \
                               ObObjParam &param,                       \
                               ObIAllocator &allocator
  #define DEF_DECODE_FUNC(name)  \
  static int decode_##name(DECODE_FUNC_ARG_DECL);
  using DecodeFunc = int (*)(DECODE_FUNC_ARG_DECL);

  DEF_DECODE_FUNC(null);
  DEF_DECODE_FUNC(int);
  DEF_DECODE_FUNC(uint);
  DEF_DECODE_FUNC(float);
  DEF_DECODE_FUNC(ufloat);
  DEF_DECODE_FUNC(double);
  DEF_DECODE_FUNC(udouble);
  DEF_DECODE_FUNC(number);
  DEF_DECODE_FUNC(unumber);
  DEF_DECODE_FUNC(datetime);
  DEF_DECODE_FUNC(time);
  DEF_DECODE_FUNC(year);
  DEF_DECODE_FUNC(string);
  static int decode_not_supported(DECODE_FUNC_ARG_DECL);

public:
  static const DecodeFunc decode_map_[ObMaxType + 1];
};

class ObMySQLPreparedStatement
{
public:
  ObMySQLPreparedStatement();
  virtual ~ObMySQLPreparedStatement();
  ObIAllocator &get_allocator();
  ObMySQLConnection *get_connection();
  MYSQL_STMT *get_stmt_handler();
  MYSQL *get_conn_handler();
  int close();
  int init(ObMySQLConnection &conn, const char *sql);
  int bind_param(const ObBindParam &param);
  int bind_result(const ObBindParam &param);
  int bind_param_int(const int64_t col_idx, int64_t *out_buf);
  int bind_param_varchar(const int64_t col_idx, char *out_buf, unsigned long res_len);

  int bind_result_int(const int64_t col_idx, int64_t *out_buf);
  int bind_result_varchar(const int64_t col_idx, char *out_buf, const int buf_len, unsigned long *&res_len);
  int64_t get_stmt_param_count() const { return stmt_param_count_; }
  int64_t get_result_column_count() const { return result_column_count_; }
  /*
   * execute a SQL command, such as
   *  - set @@session.ob_query_timeout=10
   *  - commit
   *  - insert into t values (v1,v2),(v3,v4)
   */
  int execute_update();

  /*
   * ! Deprecated
   * use prepare method to read data instead
   * reference ObMySQLPrepareStatement
   */
  ObMySQLPreparedResult *execute_query();

protected:
  int alloc_bind_params(const int64_t size, ObBindParam *&bind_params);
  int get_bind_param_by_idx(const int64_t idx,
                            ObBindParam *&param);
  int get_bind_result_param_by_idx(const int64_t idx,
                                   ObBindParam *&param);
  int get_mysql_type(ObObjType ob_type, obmysql::EMySQLFieldType &mysql_type) const;
  int get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type) const;

protected:
  ObMySQLConnection *conn_;
  ObArenaAllocator arena_allocator_;
  ObIAllocator *alloc_;  // bind to arena_allocator_
  ObMySQLPreparedParam param_;
  ObMySQLPreparedResult result_;
  int64_t stmt_param_count_;
  int64_t result_column_count_;
  MYSQL_STMT *stmt_;
  ObBindParam *bind_params_;
  ObBindParam *result_params_;
};

class ObMySQLProcStatement : public ObMySQLPreparedStatement
{
public:
  ObMySQLProcStatement()
  {
  }
  ~ObMySQLProcStatement()
  {
  }
  int execute_proc(ObIAllocator &allocator,
                   ParamStore &params,
                   const share::schema::ObRoutineInfo &routine_info,
                   const ObTimeZoneInfo *tz_info);

private:
  int bind_proc_param(ObIAllocator &allocator,
                      ParamStore &params,
                      const share::schema::ObRoutineInfo &routine_info,
                      common::ObIArray<int64_t> &basic_out_param,
                      const ObTimeZoneInfo *tz_info);
  int bind_param(const int64_t col_idx,
                 const bool is_output_param,
                 const ObTimeZoneInfo *tz_info,
                 ObObjParam &obj,
                 const share::schema::ObRoutineInfo &routine_info,
                 ObIAllocator &allocator);
  int process_proc_output_params(ObIAllocator &allocator,
                                 ParamStore &params,
                                 const share::schema::ObRoutineInfo &routine_info,
                                 common::ObIArray<int64_t> &basic_out_param,
                                 const ObTimeZoneInfo *tz_info);
  int convert_proc_output_param_result(const ObTimeZoneInfo &tz_info,
                                       const ObBindParam &bind_param,
                                       ObObjParam &param,
                                       const share::schema::ObRoutineInfo &routine_info,
                                       ObIAllocator &allocator);
};
} //namespace sqlclient
}
}
#endif
