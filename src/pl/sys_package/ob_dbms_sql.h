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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_SQL_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_SQL_H_

#include "pl/ob_pl_type.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
}
namespace pl
{

typedef common::ParamStore ParamStore;

class ObDbmsInfo
{
  /*
   * 这个结构抽出来的初衷是，
   * 调研 oracle 表现，dbms_sql 的 param 信息，并不会强绑定在 dbms_cursor 上
   * 即使 dbms_cursor 重新 open , 或者 close，
   * 只要没有通过 parse, bind_value, column_value 等手段修改 param 的信息，
   * param 的值就不会变
   * 
   * 之前放在 dbms_cursor 里， 依赖 cursor.entity 的内存管理，
   * cursor 在 reopen 和 close 的时候都会清理自己的内存，导致 param 的值不预期，
   * 
   * 重构之后，dbmsinfo 有了区别于 cursor 的内存管理方式，避免了这一类问题的出现
   */
public:
  ObDbmsInfo(common::ObIAllocator &alloc)
    : sql_stmt_(),
      stmt_type_(sql::stmt::T_NONE),
      entity_(nullptr),
      ps_sql_(),
      param_names_(alloc),
      into_names_(alloc),
      bind_params_(alloc),
      exec_params_(/*ObWrapperAllocator(&alloc)*/),
      fields_(),
      define_columns_(),
      fetch_rows_() {}
  void reset();
  inline void reuse() {}
  int init();

  inline lib::MemoryContext &get_dbms_entity() { return entity_; }
  inline const lib::MemoryContext get_dbms_entity() const { return entity_; }
  inline common::ObString &get_ps_sql() { return ps_sql_; }
  inline void set_ps_sql(ObString sql) { ps_sql_ = sql; }
  common::ObString &get_sql_stmt() { return sql_stmt_; }
  sql::stmt::StmtType get_stmt_type() const { return stmt_type_; }
  inline void set_stmt_type(sql::stmt::StmtType type) { stmt_type_ = type; }
  ParamStore &get_exec_params() { return exec_params_; }
  common::ColumnsFieldArray &get_field_columns() { return fields_; }
  static int deep_copy_field_columns(
    ObIAllocator& allocator,
    const common::ColumnsFieldIArray* src_fields,
    common::ColumnsFieldArray &dst_fields);
  static int deep_copy_field_columns(
    ObIAllocator& allocator,
    const common::ColumnsFieldArray src_fields,
    common::ColumnsFieldArray &dst_fields);

  int init_params(int64_t param_count);
  int64_t get_param_name_count() const { return param_names_.count(); }
  ObIArray<ObString>& get_param_names() { return param_names_; }

  int add_param_name(ObString &clone_name);
  int set_into_names(int64_t into_cnt);
  ObIArray<ObString> &get_into_names() { return into_names_; }
protected:

  class BindParam
  {
  public:
    BindParam()
      : param_name_(),
        param_value_()
    {}
    BindParam(const common::ObString &param_name,
              const common::ObObjParam &param_value)
      : param_name_(param_name),
        param_value_(param_value)
    {}
    TO_STRING_KV(K(param_name_), K(param_value_));
  public:
    /*
     * 考虑到BIND_ARRAY接口，后面需要做如下修改：
     * 1. param_value_需要扩展为数组;
     * 2. 记录上下界，默认为0、size - 1;
     * 3. 记录当前迭代位置，供expand_next_params接口使用;
     */
    common::ObString param_name_;
    common::ObObjParam param_value_;
  };
  typedef common::ObFixedArray<common::ObString, common::ObIAllocator> ParamNames;
  typedef common::ObFixedArray<common::ObString, common::ObIAllocator> IntoNames;
  typedef common::ObFixedArray<BindParam, common::ObIAllocator> BindParams;

public:
  struct ArrayDesc
  {
    ArrayDesc() :
      id_(OB_INVALID_ID),
      cnt_(OB_INVALID_COUNT),
      lower_bnd_(OB_INVALID_INDEX),
      cur_idx_(OB_INVALID_INDEX),
      type_() {}
    ArrayDesc(uint64_t id, int64_t cnt, int64_t lower_bnd, ObDataType type) :
      id_(id),
      cnt_(cnt),
      lower_bnd_(lower_bnd),
      cur_idx_(lower_bnd_ - 1),
      type_(type) {}
    uint64_t id_;
    int64_t cnt_;
    int64_t lower_bnd_;
    int64_t cur_idx_;
    ObDataType type_;
  };
  const ObObjParam *get_bind_param(const common::ObString &param_name) const;
  int set_bind_param(const ObString &param_name, const ObObjParam&param_value);

  typedef common::hash::ObHashMap<int64_t, ArrayDesc,
                                            common::hash::NoPthreadDefendMode> DefineArrays;
  inline const DefineArrays &get_define_arrays() const { return define_arrays_; }
  inline DefineArrays &get_define_arrays() { return define_arrays_; }

  typedef common::hash::ObHashMap<int64_t, int64_t,
                                          common::hash::NoPthreadDefendMode> DefineColumns;
  inline const DefineColumns &get_define_columns() const { return define_columns_; }

  /*
   * TODO: use hashmap may better?
   */
  typedef common::ObSEArray<ObNewRow, 16> RowBuffer;
  inline const RowBuffer &get_fetch_rows() const { return fetch_rows_; }
  inline RowBuffer &get_fetch_rows() { return fetch_rows_; }

protected:
  common::ObString sql_stmt_;
  sql::stmt::StmtType stmt_type_;
private:
  lib::MemoryContext entity_;
  common::ObString ps_sql_;
  ParamNames  param_names_;
  IntoNames   into_names_;
  BindParams  bind_params_;
  ParamStore exec_params_;
  common::ColumnsFieldArray fields_;
  DefineColumns define_columns_; //key: column pos, value: column size
  DefineArrays define_arrays_;
  RowBuffer fetch_rows_;
};

class ObDbmsCursorInfo : public ObPLCursorInfo, public ObDbmsInfo
{
public:
  ObDbmsCursorInfo(common::ObIAllocator &alloc)
    : ObPLCursorInfo(true),
      ObDbmsInfo(alloc),
      affected_rows_(-1) { }
  virtual ~ObDbmsCursorInfo() { reset(); }
  int parse(const common::ObString &sql_stmt, sql::ObSQLSessionInfo &session);
  virtual int close(sql::ObSQLSessionInfo &session, 
                    bool is_cursor_reuse = false, 
                    bool is_dbms_reuse = false);

public:
  int init();
  void reset();
  void reuse();
  void reset_private();
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }
  int64_t get_affected_rows() const { return affected_rows_; }
  int prepare_entity(sql::ObSQLSessionInfo &session);
  int64_t search_array(const ObString &name, ObIArray<ObString> &array);

private:
  // affected_rows_ 在每次 open 都会被重置
  int64_t affected_rows_;
};

}
}

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_SQL_H_ */
