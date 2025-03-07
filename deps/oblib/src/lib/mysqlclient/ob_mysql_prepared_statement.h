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
// #include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObRoutineParam;
}
}


namespace pl
{
class ObUserDefinedType;
class ObPLDataType;
class ObPLCollection;
class ObRecordType;
class ObCollectionType;
class ObPLRecord;
class ObPLComposite;
}
namespace common
{
namespace sqlclient
{
class ObMySQLConnection;
struct ObBindParam
{
  ObBindParam() : col_idx_(-1), buffer_type_(enum_field_types::MAX_NO_FIELD_TYPES), buffer_(nullptr),
                  buffer_len_(0), length_(0), is_unsigned_(0), is_null_(0), array_buffer_(nullptr),
                  ele_size_(0), max_array_size_(0), out_valid_array_size_(nullptr), array_is_null_(0)
  {
  }
  ObBindParam(int64_t col_idx, enum_field_types buffer_type,
              void *buffer, int64_t buffer_len, unsigned long length)
    : col_idx_(col_idx), buffer_type_(buffer_type), buffer_(buffer), buffer_len_(buffer_len),
      length_(length), is_unsigned_(0), is_null_(0), array_buffer_(nullptr),
      ele_size_(0), max_array_size_(0), out_valid_array_size_(nullptr), array_is_null_(0)
  {
  }
  ObBindParam(int64_t col_idx, enum_field_types buffer_type,
              void *buffer, int64_t buffer_len, unsigned long length,
              my_bool is_unsigned, my_bool is_null)
    : col_idx_(col_idx), buffer_type_(buffer_type), buffer_(buffer), buffer_len_(buffer_len),
      length_(length), is_unsigned_(is_unsigned), is_null_(is_null), array_buffer_(nullptr),
      ele_size_(0), max_array_size_(0), out_valid_array_size_(nullptr), array_is_null_(0)
  {
  }
  void assign(const ObBindParam &other);
  TO_STRING_KV(K_(col_idx),
               K_(buffer_type),
               K_(buffer),
               K_(buffer_len),
               K_(length),
               K_(is_unsigned),
               K_(is_null),
               K_(array_buffer),
               K_(ele_size),
               K_(max_array_size),
               K_(out_valid_array_size),
               K_(array_is_null));

public:
  int64_t col_idx_;
  enum_field_types buffer_type_;
  void *buffer_;
  int64_t buffer_len_;
  unsigned long length_;
  my_bool is_unsigned_;
  my_bool is_null_;
  void *array_buffer_;
  int64_t ele_size_;
  int64_t max_array_size_;
  uint32_t *out_valid_array_size_;
  int64_t array_is_null_;
};

class ObBindParamEncode
{
public:
  #define ENCODE_FUNC_ARG_DECL const int64_t col_idx,           \
                               const bool is_output_param,      \
                               const ObTimeZoneInfo &tz_info,   \
                               ObObj &param,               \
                               ObBindParam &bind_param,         \
                               ObIAllocator &allocator,          \
                               enum_field_types buffer_type
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
  DEF_ENCODE_FUNC(timestamp);
  DEF_ENCODE_FUNC(date);
  DEF_ENCODE_FUNC(time);
  DEF_ENCODE_FUNC(year);
  DEF_ENCODE_FUNC(string);
  DEF_ENCODE_FUNC(number_float);
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
                               ObObj &param,                       \
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
  DEF_DECODE_FUNC(timestamp);
  DEF_DECODE_FUNC(time);
  DEF_DECODE_FUNC(year);
  DEF_DECODE_FUNC(string);
  DEF_DECODE_FUNC(number_float);
  static int decode_not_supported(DECODE_FUNC_ARG_DECL);

public:
  static const DecodeFunc decode_map_[ObMaxType + 1];
};

class ObMySQLPreparedStatement
{
friend ObBindParamDecode;
public:
  ObMySQLPreparedStatement();
  virtual ~ObMySQLPreparedStatement();
  ObIAllocator *get_allocator();
  void set_allocator(ObIAllocator *alloc);
  ObMySQLConnection *get_connection();
  MYSQL_STMT *get_stmt_handler();
  MYSQL *get_conn_handler();
  virtual int close();
  virtual int init(ObMySQLConnection &conn, const ObString &sql, int64_t param_count);
  int bind_param(ObBindParam &param);
  int bind_result(ObBindParam &param);
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
  static int get_mysql_type(ObObjType ob_type, obmysql::EMySQLFieldType &mysql_type);
  static int get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type);

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
class ObCompositeData
{
public:
  ObCompositeData() : param_position_(OB_INVALID_INDEX),
                      is_out_(false),
                      is_record_(false),
                      is_assoc_array_(true),
                      udt_id_(OB_INVALID_ID),
                      data_array_() {}
  ObCompositeData(int64_t position) : param_position_(position),
                                      is_out_(false),
                                      is_record_(false),
                                      is_assoc_array_(true),
                                      udt_id_(OB_INVALID_ID),
                                      data_array_() {}
  int assign(const ObCompositeData &other)
  {
    param_position_ = other.param_position_;
    is_out_ = other.is_out_;
    is_record_ = other.is_record_;
    is_assoc_array_ = other.is_assoc_array_;
    udt_id_ = other.udt_id_;
    return data_array_.assign(other.data_array_);
  }

  ObCompositeData &operator =(const ObCompositeData &other)
  {
    assign(other);
    return *this;
  }

  inline int64_t get_param_position() { return param_position_; }
  inline bool is_valid() { return param_position_ != OB_INVALID_INDEX && data_array_.count() > 0; }
  inline bool is_record() { return is_record_; }
  inline bool is_out() { return is_out_; }
  inline bool is_assoc_array() { return is_assoc_array_; }
  inline uint64_t get_udt_id() { return udt_id_; }
  inline void set_is_record(bool v) { is_record_ = v; }
  inline void set_is_out(bool v) { is_out_ = v; }
  inline void set_is_assoc_array(bool v) { is_assoc_array_ = v; }
  inline void set_udt_id(uint64_t v) { udt_id_ = v; }
  int add_element(ObBindParam* param) { return data_array_.push_back(param); }
  ObIArray<ObBindParam *> &get_data_array() { return data_array_; }
  TO_STRING_KV(K(param_position_), K(is_out_), K(is_record_), K(is_assoc_array_), K(udt_id_));
private:
  int64_t param_position_;
  bool is_out_;
  bool is_record_;
  bool is_assoc_array_;
  uint64_t udt_id_;
  common::ObSEArray<ObBindParam*, 1> data_array_;
};

public:
  ObMySQLProcStatement() : ObMySQLPreparedStatement()
  {
    in_out_map_.reset();
    proc_ = NULL;
    out_param_start_pos_ = 0;
    out_param_cur_pos_ = 0;
    basic_param_start_pos_ = 0;
    basic_return_value_pos_ = 0;
    com_datas_.reset();
  }
  ~ObMySQLProcStatement()
  {
    in_out_map_.reset();
    proc_ = NULL;
    out_param_start_pos_ = 0;
    out_param_cur_pos_ = 0;
    basic_param_start_pos_ = 0;
    basic_return_value_pos_ = 0;
    com_datas_.reset();
  }
  virtual int init(ObMySQLConnection &conn, const ObString &sql, int64_t param_count);
  virtual int close();
  virtual void free_resouce();
  virtual int close_mysql_stmt();
  int execute_proc(ObIAllocator &allocator,
                   ParamStore &params,
                   const share::schema::ObRoutineInfo &routine_info,
                   const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                   const ObTimeZoneInfo *tz_info,
                   ObObj *result,
                   bool is_sql,
                   int64_t out_param_start_pos,
                   int64_t basic_param_start_pos,
                   int64_t basic_return_value_pos);
  int execute_proc();
  int bind_basic_type_by_pos(uint64_t position,
                             void *param_buffer,
                             int64_t param_size,
                             int32_t datatype,
                             int32_t &indicator,
                             bool is_out_param);
  int bind_array_type_by_pos(uint64_t position,
                             void *array,
                             int32_t *indicators,
                             int64_t ele_size,
                             int32_t ele_datatype,
                             uint64_t array_size,
                             uint32_t *out_valid_array_size);
  inline void set_proc(const char *sql) { proc_ = sql; }
  static bool is_in_param(const share::schema::ObRoutineParam &r_param);
  static bool is_out_param(const share::schema::ObRoutineParam &r_param);
  static int get_udt_by_id(uint64_t user_type_id,
                           const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                           const pl::ObUserDefinedType *&udt);
  static int get_udt_by_name(const ObString &name,
                             const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                             const pl::ObUserDefinedType *&udt);
  static int get_anonymous_param_count(ParamStore &params,
                                       const share::schema::ObRoutineInfo &routine_info,
                                       const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                       bool is_sql,
                                       int64_t &param_cnt,
                                       int64_t &out_param_start_pos,
                                       int64_t &basic_param_start_pos,
                                       int64_t &basic_return_value_pos);
  static int store_string_obj(ObObj &param,
                              ObObjType obj_type,
                              ObIAllocator &allocator,
                              const int64_t length,
                              char *buffer);
private:
  int bind_proc_param(ObIAllocator &allocator,
                      ParamStore &params,
                      const share::schema::ObRoutineInfo &routine_info,
                      const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                      common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                      const ObTimeZoneInfo *tz_info,
                      ObObj *result,
                      bool is_sql);
  int bind_proc_param_with_composite_type(ObIAllocator &allocator,
                                          ParamStore &params,
                                          const share::schema::ObRoutineInfo &routine_info,
                                          const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                          const ObTimeZoneInfo *tz_info,
                                          ObObj *result,
                                          bool is_sql,
                                          common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param);
  int bind_param(const int64_t col_idx,
                 const int64_t param_idx,
                 const bool is_output_param,
                 const ObTimeZoneInfo *tz_info,
                 ObObj &obj,
                 const share::schema::ObRoutineInfo &routine_info,
                 ObIAllocator &allocator);
  int process_proc_output_params(ObIAllocator &allocator,
                                 ParamStore &params,
                                 const share::schema::ObRoutineInfo &routine_info,
                                 const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                 common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                                 const ObTimeZoneInfo *tz_info,
                                 ObObj *result,
                                 bool is_sql);
  int process_composite_out_param(ObIAllocator &allocator,
                                  ParamStore &params,
                                  ObObj *result,
                                  int64_t start_idx_in_result,
                                  const share::schema::ObRoutineInfo &routine_info,
                                  const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                  const ObTimeZoneInfo *tz_info);
#ifdef OB_BUILD_ORACLE_PL
  int process_record_out_param(const pl::ObRecordType *record_type,
                               ObCompositeData &com_data,
                               ObIAllocator &allocator,
                               ObObj &param,
                               const ObTimeZoneInfo *tz_info);
  int process_array_out_param(const pl::ObCollectionType *coll_type,
                              const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                              ObCompositeData &com_data,
                              ObIAllocator &allocator,
                              ObObj &param,
                              const ObTimeZoneInfo *tz_info);
  int process_array_element(int64_t pos,
                            ObIAllocator &allocator,
                            void *buffer,
                            ObObj &param,
                            const ObTimeZoneInfo *tz_info);
#endif
  int convert_proc_output_param_result(int64_t out_param_idx,
                                       const ObTimeZoneInfo &tz_info,
                                       const ObBindParam &bind_param,
                                       ObObj *param,
                                       const share::schema::ObRoutineInfo &routine_info,
                                       ObIAllocator &allocator,
                                       bool is_return_value);
  int execute_stmt_v2_interface();
  int handle_data_truncated(ObIAllocator &allocator);
#ifdef OB_BUILD_ORACLE_PL
  int bind_compsite_type(int64_t &position,
                         ObIAllocator &allocator,
                         ObObj &param,
                         const share::schema::ObRoutineParam *r_param,
                         const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                         const pl::ObUserDefinedType *udt,
                         const ObTimeZoneInfo *tz_info);
  int build_record_meta(ObIAllocator &allocator,
                        ObObj &param,
                        const share::schema::ObRoutineParam *r_param,
                        const pl::ObRecordType *record_type,
                        int64_t &position,
                        const ObTimeZoneInfo *tz_info);
  int build_record_element(int64_t position,
                           bool is_out,
                           int64_t element_idx,
                           ObIAllocator &allocator,
                           ObObj &param,
                           const pl::ObPLDataType &pl_type,
                           const ObTimeZoneInfo *tz_info,
                           ObBindParam *&bind_param);
  int get_current_obj(pl::ObPLComposite *composite,
                      int64_t current_idx,
                      int64_t element_idx,
                      ObObj &current_obj);
  int build_array_meta(ObIAllocator &allocator,
                       ObObj &param,
                       const share::schema::ObRoutineParam *r_param,
                       const pl::ObCollectionType *coll_type,
                       const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                       int64_t &position,
                       const ObTimeZoneInfo *tz_info);
  int build_array_element(int64_t position,
                          bool is_out,
                          int64_t element_idx,
                          ObIAllocator &allocator,
                          ObObj &param,
                          ObObjType obj_type,
                          const ObTimeZoneInfo *tz_info);
  int build_array_buffer(int64_t position,
                         int64_t element_idx,
                         ObIAllocator &allocator,
                         pl::ObPLCollection *coll,
                         ObObjType obj_type,
                         bool is_out,
                         const ObTimeZoneInfo *tz_info);
  int check_assoc_array(pl::ObPLCollection *coll);
  int build_array_isnull(int64_t position,
                         bool is_in,
                         ObObj &param);
  int store_array_element(ObObj &obj,
                          MYSQL_COMPLEX_BIND_HEADER *header,
                          ObIAllocator &allocator,
                          const ObTimeZoneInfo *tz_info);
#endif
  void increase_out_param_cur_pos(int64_t v) {
    out_param_cur_pos_ += v;
  }
  int64_t get_out_param_cur_pos() {
    return out_param_cur_pos_;
  }
  int64_t get_basic_param_start_pos() {
    return basic_param_start_pos_;
  }
  int64_t get_basic_return_value_pos() {
    return basic_return_value_pos_;
  }
  ObIArray<ObCompositeData> &get_com_datas() { return com_datas_; }
private:
  common::ObSEArray<bool, 8> in_out_map_;
  ObString proc_;
  int64_t out_param_start_pos_; // composite type out param_start position
  int64_t out_param_cur_pos_;  // composite type out param current position
  int64_t basic_param_start_pos_;
  int64_t basic_return_value_pos_;
  common::ObSEArray<ObCompositeData, 2> com_datas_;
};
} //namespace sqlclient
}
}
#endif
