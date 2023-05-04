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

#ifndef OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_
#define OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_
#include "lib/timezone/ob_time_convert.h"
#include "share/system_variable/ob_system_variable_init.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace sql
{
class ObExecContext;
class ObBasicSessionInfo;
}

namespace share
{
class ObSpecialSysVarValues
{
public:
  // OB_SV_VERSION_COMMENT
  const static int64_t VERSION_COMMENT_MAX_LEN = 256;
  static char version_comment_[VERSION_COMMENT_MAX_LEN];

  // OB_SV_VERSION
  const static int64_t VERSION_MAX_LEN = 256;
  static char version_[VERSION_MAX_LEN];

  // OB_SV_SYSTEM_TIME_ZONE
  const static int64_t SYSTEM_TIME_ZONE_MAX_LEN = 64;
  static char system_time_zone_str_[SYSTEM_TIME_ZONE_MAX_LEN];

  // charset和collation相关
  static const int64_t COLL_INT_STR_MAX_LEN = 64;
  static char default_coll_int_str_[COLL_INT_STR_MAX_LEN];

  //OB_SV_SERVER_UUID:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
  const static int64_t SERVER_UUID_MAX_LEN = 37;
  static char server_uuid_[SERVER_UUID_MAX_LEN];
public:
  ObSpecialSysVarValues();
};

class ObSetVar
{
public:
  enum SetScopeType
  {
    /*
     * most system variables can be set with:
     * 1. GLOBAL scope
     *    with 'SET GLOBAL xxx=xxx' only in mysql mode.
     *    set with global scope will affect all sessions that created in future, and keep
     *    all exist sessions unchanged.
     * 2. SESSION scope
     *    with 'SET [SESSION] xxx=xxx' in mysql mode, SESSION is optional.
     *      or 'ALTER SESSION SET xxx=xxx' in oracle mode,
     *    set with session scope will affect all following operations in current session.
     * 3. NONE scope
     *    with 'SET xxx=xxx' in both mysql and oracle mode.
     *    besides GLOBAL and SESSION scopes, a few variables can be set with NONE scope,
     *    which only affect the NEXT ONE transaction in current session, like tx_isolation.
     *    so I think 'SET_SCOPE_NEXT_TRANS' is better than 'SET_SCOPE_NONE'.
     */
    SET_SCOPE_NEXT_TRANS = 0,
    SET_SCOPE_GLOBAL,
    SET_SCOPE_SESSION,
  };

  ObSetVar(const common::ObString &var_name,
           ObSetVar::SetScopeType set_scope,
           bool is_set_default,
           uint64_t actual_tenant_id,
           common::ObIAllocator &calc_buf,
           common::ObMySQLProxy &sql_proxy)
    : var_name_(var_name),
      set_scope_(set_scope),
      is_set_default_(is_set_default),
      actual_tenant_id_(actual_tenant_id),
      calc_buf_(calc_buf),
      sql_proxy_(sql_proxy)
  {}
  virtual ~ObSetVar() {}

  common::ObString var_name_;
  ObSetVar::SetScopeType set_scope_;
  bool is_set_default_;
  uint64_t actual_tenant_id_;
  common::ObIAllocator &calc_buf_;
  common::ObMySQLProxy &sql_proxy_;

  TO_STRING_KV(K_(var_name), K_(set_scope), K_(is_set_default), K_(actual_tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetVar);
};

class ObSysVarTypeLib
{
public:
  //ObSysVarTypeLib() : count_(0), type_names_(NULL) {}
  ObSysVarTypeLib(const char **type_names) //传入的char*数组的最后一个元素必须为0
    : count_(0),
      type_names_(NULL)
  {
    if (OB_ISNULL(type_names)) {
      SQL_SESSION_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "type names is NULL");
    } else {
      // 这里没法按照编码规范检查下标
      for (count_ = 0; 0 != type_names[count_]; count_++);
      type_names_ = type_names;
    }
  }
  virtual ~ObSysVarTypeLib() {}
  virtual void reset() { count_ = 0; type_names_ = NULL; }

  int64_t count_;
  const char **type_names_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarTypeLib);
};

class ObBasicSysVar
{
  OB_UNIS_VERSION_V(1);
public:
  static const char * EMPTY_STRING;
  typedef int (*OnCheckAndConvertFunc)(sql::ObExecContext &ctx,
                                       const ObSetVar &set_var,
                                       const ObBasicSysVar &sys_var,
                                       const common::ObObj &in_val,
                                       common::ObObj &out_val);
  typedef int (*OnUpdateFunc)(sql::ObExecContext &ctx,
                              const ObSetVar &set_var,
                              const ObBasicSysVar &sys_var,
                              const common::ObObj &val);
  typedef int (*ToStrFunc)(common::ObIAllocator &allocator,
                           const sql::ObBasicSessionInfo &session,
                           const ObBasicSysVar &sys_var,
                           common::ObString &result_str);
  typedef int (*ToObjFunc)(common::ObIAllocator &allocator,
                           const sql::ObBasicSessionInfo &session,
                           const ObBasicSysVar &sys_var,
                           common::ObObj &result_obj);
  typedef common::ObObjType (*GetMetaTypeFunc)();
  //非varchar类型的sys variable才可能为NULL
  static inline bool is_null_value(const common::ObString & value, int64_t flag)
  {
    return (flag & ObSysVarFlag::NULLABLE)
        && (value == common::ObString::make_string(ObBasicSysVar::EMPTY_STRING));
  }
public:
  ObBasicSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                OnUpdateFunc on_update = NULL,
                ToObjFunc to_select_obj = NULL,
                ToStrFunc to_show_str = NULL,
                GetMetaTypeFunc get_meta_type = NULL,
                bool is_enum_type = false)
    : base_version_(common::OB_INVALID_VERSION),
      base_value_(),
      inc_value_(),
      min_val_(),
      max_val_(),
      type_(common::ObUnknownType),
      flags_(ObSysVarFlag::NONE),
      on_check_and_convert_(on_check_and_convert),
      on_update_(on_update),
      to_select_obj_(to_select_obj),
      to_show_str_(to_show_str),
      get_meta_type_(get_meta_type),
      is_enum_type_(is_enum_type)
  {
    clean_value();
  }
  virtual ~ObBasicSysVar() {}

  virtual share::ObSysVarClassType get_type() const = 0;
  virtual const common::ObObj &get_global_default_value() const = 0;

  virtual int init(const common::ObObj &value,
                   const common::ObObj &min_val,
                   const common::ObObj &max_val,
                   common::ObObjType type,
                   int64_t flags);
  virtual void reset();
  virtual void clean_value();
  virtual void clean_base_value();
  virtual void clean_inc_value();
  virtual bool is_base_value_empty() const;
  virtual bool is_inc_value_empty() const;
  virtual int check_and_convert(sql::ObExecContext &ctx,
                                const ObSetVar &set_var,
                                const common::ObObj &in_val,
                                common::ObObj &out_val);
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
  virtual int session_update(sql::ObExecContext &ctx, const ObSetVar &set_var, const common::ObObj &val);
  virtual int update(sql::ObExecContext &ctx, const ObSetVar &set_var, const common::ObObj &val);

  virtual common::ObObjType inner_get_meta_type() const; // select @@XXX的时候meta data里面的类型
  virtual int inner_to_select_obj(common::ObIAllocator &allocator,
                                  const sql::ObBasicSessionInfo &session,
                                  common::ObObj &select_obj) const;
  virtual int inner_to_show_str(common::ObIAllocator &allocator,
                                const sql::ObBasicSessionInfo &session,
                                common::ObString &show_str) const;

  int64_t get_base_version() const;
  common::ObObjType get_meta_type() const;
  const common::ObObj &get_value() const;
  const common::ObObj &get_base_value() const;
  const common::ObObj &get_inc_value() const;
  const common::ObObj &get_min_val() const;
  const common::ObObj &get_max_val() const;
  void set_value(const common::ObObj &value);
  common::ObObjType get_data_type() const;
  void set_data_type(common::ObObjType type);
  void set_flags(int64_t flags);
  int to_select_obj(common::ObIAllocator &allocator,
                    const sql::ObBasicSessionInfo &session,
                    common::ObObj &select_obj) const;
  int to_show_str(common::ObIAllocator &allocator,
                  const sql::ObBasicSessionInfo &session,
                  common::ObString &show_str) const;

  inline bool is_readonly() const { return 0 != (flags_ & ObSysVarFlag::READONLY); }
  inline bool is_session_readonly() const { return 0 != (flags_ & ObSysVarFlag::SESSION_READONLY); }
  inline bool is_invisible() const { return 0 != (flags_ & ObSysVarFlag::INVISIBLE); }
  inline bool is_global_scope() const { return 0 != (flags_ & ObSysVarFlag::GLOBAL_SCOPE); }
  inline bool is_session_scope() const { return 0 != (flags_ & ObSysVarFlag::SESSION_SCOPE); }
  inline bool is_influence_plan() const { return 0 != (flags_ & ObSysVarFlag::INFLUENCE_PLAN); }
  inline bool is_oracle_only() const { return 0 != (flags_ & ObSysVarFlag::ORACLE_ONLY); }
  inline bool is_enum_type() const { return is_enum_type_; }
  inline bool is_mysql_only() const { return 0 != (flags_ & ObSysVarFlag::MYSQL_ONLY); }
  inline bool is_with_upgrade() const { return 0 != (flags_ & ObSysVarFlag::WITH_UPGRADE); }
  inline bool is_need_serialize() const { return 0 != (flags_ & ObSysVarFlag::NEED_SERIALIZE); }
  const common::ObString get_name() const;
  static int get_charset_var_and_val_by_collation(const common::ObString &coll_var_name,
                                           const common::ObString &coll_val,
                                           common::ObString &cs_var_name,
                                           common::ObString &cs_val,
                                           common::ObCollationType &coll_type);
  static int get_collation_var_and_val_by_charset(const common::ObString &cs_var_name,
                                           const common::ObString &cs_val,
                                           common::ObString &coll_var_name,
                                           common::ObString &coll_val,
                                           common::ObCollationType &coll_type);
  DECLARE_TO_STRING;

protected:
  // 目前base_value和inc_value的设置操作有一个简单原则：
  // 1. 有且只有base_value会和min_value/max_value一起设置，并且不需要判断base_value是否有效。
  //    目前只有init接口。
  // 2. 有且只有inc_value会单独设置，并且可能根据min_value/max_value判断inc_value是否有效。
  //    目前只有set_value接口。
  int64_t base_version_;
  common::ObObj base_value_;
  common::ObObj inc_value_;
  common::ObObj min_val_;
  common::ObObj max_val_;
  common::ObObjType type_;
  int64_t flags_;
protected:
  int log_err_wrong_value_for_var(int error_no, const common::ObObj &val) const;
  int check_and_convert_int_tc_value(const common::ObObj &value,
                                     int64_t invalid_value,
                                     int64_t &result_value) const;
  int check_and_convert_uint_tc_value(const common::ObObj &value,
                                      uint64_t invalid_value,
                                      uint64_t &result_value) const;
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
  OnCheckAndConvertFunc on_check_and_convert_;
  OnUpdateFunc on_update_;
  ToObjFunc to_select_obj_;
  ToStrFunc to_show_str_;
  GetMetaTypeFunc get_meta_type_;
  bool is_enum_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBasicSysVar);
};

class ObTypeLibSysVar : public ObBasicSysVar
{
public:
  ObTypeLibSysVar(const char **type_names,
                  OnCheckAndConvertFunc on_check_and_convert = NULL,
                  OnUpdateFunc on_update = NULL,
                  ToObjFunc to_select_obj = NULL,
                  ToStrFunc to_show_str = NULL,
                  GetMetaTypeFunc get_meta_type = NULL,
                  bool is_enum_type = false)
      : ObBasicSysVar(on_check_and_convert,
                      on_update,
                      to_select_obj,
                      to_show_str,
                      get_meta_type,
                      is_enum_type),
        type_lib_(type_names)
  {
  }
  virtual ~ObTypeLibSysVar() {}

  virtual void reset();
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
  virtual int inner_to_show_str(common::ObIAllocator &allocator,
                                const sql::ObBasicSessionInfo &session,
                                common::ObString &show_str) const;
  int find_type(const common::ObString &type, int64_t &type_index) const;
protected:
  ObSysVarTypeLib type_lib_;
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTypeLibSysVar);
};

class ObEnumSysVar : public ObTypeLibSysVar
{
public:
  ObEnumSysVar(const char **type_names,
               OnCheckAndConvertFunc on_check_and_convert = NULL,
               OnUpdateFunc on_update = NULL,
               ToObjFunc to_select_obj = NULL,
               ToStrFunc to_show_str = NULL,
               GetMetaTypeFunc get_meta_type = NULL)
      : ObTypeLibSysVar(type_names,
                        on_check_and_convert,
                        on_update,
                        to_select_obj,
                        to_show_str,
                        get_meta_type,
                        true/* is_enum_type */)
  {
  }
  virtual ~ObEnumSysVar() {}

  virtual int inner_to_select_obj(common::ObIAllocator &allocator,
                                  const sql::ObBasicSessionInfo &session,
                                  common::ObObj &select_obj) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObEnumSysVar);
};

class ObBoolSysVar : public ObTypeLibSysVar
{
public:
  const static char *BOOL_TYPE_NAMES[];
public:
  ObBoolSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
               OnUpdateFunc on_update = NULL,
               ToObjFunc to_select_obj = NULL,
               ToStrFunc to_show_str = NULL,
               GetMetaTypeFunc get_meta_type = NULL)
      : ObTypeLibSysVar(ObBoolSysVar::BOOL_TYPE_NAMES,
                        on_check_and_convert,
                        on_update,
                        to_select_obj,
                        to_show_str,
                        get_meta_type)
  {
  }
  virtual ~ObBoolSysVar() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObBoolSysVar);
};

//////////////////////////////
class ObSetSysVar : public ObTypeLibSysVar
{
  static const int64_t MAX_STR_BUF_LEN = 512;
public:
  ObSetSysVar(const char **type_names,
              OnCheckAndConvertFunc on_check_and_convert = NULL,
              OnUpdateFunc on_update = NULL,
              ToObjFunc to_select_obj = NULL,
              ToStrFunc to_show_str = NULL,
              GetMetaTypeFunc get_meta_type = NULL)
      : ObTypeLibSysVar(type_names,
                        on_check_and_convert,
                        on_update,
                        to_select_obj,
                        to_show_str,
                        get_meta_type)
  {
  }
  virtual ~ObSetSysVar() {}
  int find_set(const common::ObString &value);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetSysVar);
};

//////////////////////////////
class ObSqlModeVar : public ObSetSysVar
{
public:
  const static char* SQL_MODE_NAMES[];
public:
  ObSqlModeVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
               OnUpdateFunc on_update = NULL,
               ToObjFunc to_select_obj = NULL,
               ToStrFunc to_show_str = NULL,
               GetMetaTypeFunc get_meta_type = NULL)
      : ObSetSysVar(ObSqlModeVar::SQL_MODE_NAMES,
                    on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type)
  {
  }
  virtual ~ObSqlModeVar() {}
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlModeVar);
};

//////////////////////////////

class ObSysVarAccessMode : public ObBoolSysVar
{
public:
  ObSysVarAccessMode(OnCheckAndConvertFunc on_check_and_convert = NULL,
                     OnUpdateFunc on_update = NULL,
                     ToObjFunc to_select_obj = NULL,
                     ToStrFunc to_show_str = NULL,
                     GetMetaTypeFunc get_meta_type = NULL)
      : ObBoolSysVar(on_check_and_convert,
                     on_update,
                     to_select_obj,
                     to_show_str,
                     get_meta_type)
  {
  }
  virtual ~ObSysVarAccessMode() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarAccessMode);
};

/////////////////////////////
class ObCharsetSysVar : public ObBasicSysVar
{
public:
  ObCharsetSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                  OnUpdateFunc on_update = NULL,
                  ToObjFunc to_select_obj = NULL,
                  ToStrFunc to_show_str = NULL,
                  GetMetaTypeFunc get_meta_type = NULL)
      : ObBasicSysVar(on_check_and_convert,
                      on_update,
                      to_select_obj,
                      to_show_str,
                      get_meta_type)
  {
  }
  virtual ~ObCharsetSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCharsetSysVar);
};

/////////////////////////////
class ObTinyintSysVar : public ObBasicSysVar
{
public:
  ObTinyintSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                  OnUpdateFunc on_update = NULL,
                  ToObjFunc to_select_obj = NULL,
                  ToStrFunc to_show_str = NULL,
                  GetMetaTypeFunc get_meta_type = NULL)
      : ObBasicSysVar(on_check_and_convert,
                      on_update,
                      to_select_obj,
                      to_show_str,
                      get_meta_type)
  {
  }
  virtual ~ObTinyintSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTinyintSysVar);
};

class ObIntSysVar : public ObBasicSysVar
{
public:
  ObIntSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
              OnUpdateFunc on_update = NULL,
              ToObjFunc to_select_obj = NULL,
              ToStrFunc to_show_str = NULL,
              GetMetaTypeFunc get_meta_type = NULL)
      : ObBasicSysVar(on_check_and_convert,
                      on_update,
                      to_select_obj,
                      to_show_str,
                      get_meta_type)
  {
  }
  virtual ~ObIntSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);

private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
  virtual int do_convert(sql::ObExecContext &ctx,
                         const common::ObObj &in_val,
                         common::ObObj &out_val,
                         bool &is_converted);
private:
  DISALLOW_COPY_AND_ASSIGN(ObIntSysVar);
};

// 严格范围的int，这个类遇到set语句的值超出范围的时候会报错，而不是像ObIntSysVar那样截断
class ObStrictRangeIntSysVar : public ObIntSysVar
{
public:
  ObStrictRangeIntSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                         OnUpdateFunc on_update = NULL,
                         ToObjFunc to_select_obj = NULL,
                         ToStrFunc to_show_str = NULL,
                         GetMetaTypeFunc get_meta_type = NULL)
      : ObIntSysVar(on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type)
  {
  }
  virtual ~ObStrictRangeIntSysVar() {}
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStrictRangeIntSysVar);
};

class ObNumericSysVar : public ObBasicSysVar
{
public:
  ObNumericSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                  OnUpdateFunc on_update = NULL,
                  ToObjFunc to_select_obj = NULL,
                  ToStrFunc to_show_str = NULL,
                  GetMetaTypeFunc get_meta_type = NULL)
    : ObBasicSysVar(on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type)
  {
  }
  virtual ~ObNumericSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
  virtual int do_convert(sql::ObExecContext &ctx,
                         const common::ObObj &in_val,
                         common::ObObj &out_val,
                         bool &is_converted);
private:
  DISALLOW_COPY_AND_ASSIGN(ObNumericSysVar);
};

class ObVarcharSysVar : public ObBasicSysVar
{
public:
  ObVarcharSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                  OnUpdateFunc on_update = NULL,
                  ToObjFunc to_select_obj = NULL,
                  ToStrFunc to_show_str = NULL,
                  GetMetaTypeFunc get_meta_type = NULL)
    : ObBasicSysVar(on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type)
  {
  }
  virtual ~ObVarcharSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObVarcharSysVar);
};

class ObTimeZoneSysVar : public ObBasicSysVar
{
public:
  ObTimeZoneSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                   OnUpdateFunc on_update = NULL,
                   ToObjFunc to_select_obj = NULL,
                   ToStrFunc to_show_str = NULL,
                   GetMetaTypeFunc get_meta_type = NULL)
      : ObBasicSysVar(on_check_and_convert,
                      on_update,
                      to_select_obj,
                      to_show_str,
                      get_meta_type)
  {
  }
  virtual ~ObTimeZoneSysVar() {}
  virtual int check_update_type(const ObSetVar &set_var, const common::ObObj &val);
private:
  virtual int do_check_and_convert(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &in_val,
                                   common::ObObj &out_val);
  int find_pos_time_zone(sql::ObExecContext &ctx, const common::ObString &str_val, const bool is_oracle_compatible);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTimeZoneSysVar);
};

class ObSessionSpecialIntSysVar : public ObIntSysVar
{
public:
  typedef int (*SessionSpecialUpdateFunc)(sql::ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const common::ObObj &val);
public:
  ObSessionSpecialIntSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                         SessionSpecialUpdateFunc session_special_update = NULL,
                         ToObjFunc to_select_obj = NULL,
                         ToStrFunc to_show_str = NULL,
                         GetMetaTypeFunc get_meta_type = NULL)
      : ObIntSysVar(on_check_and_convert,
                    NULL,
                    to_select_obj,
                    to_show_str,
                    get_meta_type),
      session_special_update_(session_special_update)
  {
  }
  virtual ~ObSessionSpecialIntSysVar() {}

  virtual int session_update(sql::ObExecContext &ctx, const ObSetVar &set_var, const common::ObObj &val)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(session_special_update_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_SESSION_LOG(ERROR, "function ptr session_special_update_ is NULL", K(ret));
    } else if (OB_FAIL(session_special_update_(ctx, set_var, val))) {
      SQL_SESSION_LOG(WARN, "fail to call session_special_update_", K(ret));
    }
    return ret;
  }

private:
  SessionSpecialUpdateFunc session_special_update_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionSpecialIntSysVar);
};

class ObSessionSpecialVarcharSysVar : public ObVarcharSysVar
{
public:
  typedef int (*SessionSpecialUpdateFunc)(sql::ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const common::ObObj &val);
public:
  ObSessionSpecialVarcharSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                         OnUpdateFunc on_update = NULL,
                         SessionSpecialUpdateFunc session_special_update = NULL,
                         ToObjFunc to_select_obj = NULL,
                         ToStrFunc to_show_str = NULL,
                         GetMetaTypeFunc get_meta_type = NULL)
      : ObVarcharSysVar(on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type),
      session_special_update_(session_special_update)
  {
  }
  virtual ~ObSessionSpecialVarcharSysVar() {}

  virtual int session_update(sql::ObExecContext &ctx, const ObSetVar &set_var, const common::ObObj &val)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(session_special_update_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_SESSION_LOG(ERROR, "function ptr session_special_update_ is NULL", K(ret));
    } else if (OB_FAIL(session_special_update_(ctx, set_var, val))) {
      SQL_SESSION_LOG(WARN, "fail to call session_special_update_", K(ret));
    }
    return ret;
  }

private:
  SessionSpecialUpdateFunc session_special_update_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionSpecialVarcharSysVar);
};

class ObSessionSpecialBoolSysVar : public ObBoolSysVar
{
public:
  typedef int (*SessionSpecialUpdateFunc)(sql::ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const common::ObObj &val);
public:
  ObSessionSpecialBoolSysVar(OnCheckAndConvertFunc on_check_and_convert = NULL,
                         OnUpdateFunc on_update = NULL,
                         SessionSpecialUpdateFunc session_special_update = NULL,
                         ToObjFunc to_select_obj = NULL,
                         ToStrFunc to_show_str = NULL,
                         GetMetaTypeFunc get_meta_type = NULL)
      : ObBoolSysVar(on_check_and_convert,
                    on_update,
                    to_select_obj,
                    to_show_str,
                    get_meta_type),
      session_special_update_(session_special_update)
  {
  }
  virtual ~ObSessionSpecialBoolSysVar() {}

  virtual int session_update(sql::ObExecContext &ctx, const ObSetVar &set_var, const common::ObObj &val)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(session_special_update_)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_SESSION_LOG(ERROR, "function ptr session_special_update_ is NULL", K(ret));
    } else if (OB_FAIL(session_special_update_(ctx, set_var, val))) {
      SQL_SESSION_LOG(WARN, "fail to call session_special_update_", K(ret));
    }
    return ret;
  }

private:
  SessionSpecialUpdateFunc session_special_update_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionSpecialBoolSysVar);
};

class ObSysVarOnCheckFuncs
{
public:
  ObSysVarOnCheckFuncs() {}
  virtual ~ObSysVarOnCheckFuncs() {}

public:
  static int check_and_convert_timestamp_service(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_max_allowed_packet(sql::ObExecContext &ctx,
                                                  const ObSetVar &set_var,
                                                  const ObBasicSysVar &sys_var,
                                                  const common::ObObj &in_val,
                                                  common::ObObj &out_val);
  static int check_and_convert_net_buffer_length(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_charset(sql::ObExecContext &ctx,
                                       const ObSetVar &set_var,
                                       const ObBasicSysVar &sys_var,
                                       const common::ObObj &in_val,
                                       common::ObObj &out_val);
  static int check_and_convert_charset_not_null(sql::ObExecContext &ctx,
                                                const ObSetVar &set_var,
                                                const ObBasicSysVar &sys_var,
                                                const common::ObObj &in_val,
                                                common::ObObj &out_val);
  static int check_and_convert_collation_not_null(sql::ObExecContext &ctx,
                                                  const ObSetVar &set_var,
                                                  const ObBasicSysVar &sys_var,
                                                  const common::ObObj &in_val,
                                                  common::ObObj &out_val);
  static int check_and_convert_tx_isolation(sql::ObExecContext &ctx,
                                            const ObSetVar &set_var,
                                            const ObBasicSysVar &sys_var,
                                            const common::ObObj &in_val,
                                            common::ObObj &out_val);
  static int check_and_convert_tx_read_only(sql::ObExecContext &ctx,
                                            const ObSetVar &set_var,
                                            const ObBasicSysVar &sys_var,
                                            const common::ObObj &in_val,
                                            common::ObObj &out_val);
  static int check_and_convert_timeout_too_large(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_max_user_connections(sql::ObExecContext &ctx,
                                                    const ObSetVar &set_var,
                                                    const ObBasicSysVar &sys_var,
                                                    const common::ObObj &in_val,
                                                    common::ObObj &out_val);
  static int check_and_convert_sql_mode(sql::ObExecContext &ctx,
                                        const ObSetVar &set_var,
                                        const ObBasicSysVar &sys_var,
                                        const common::ObObj &in_val,
                                        common::ObObj &out_val);
  static int check_and_convert_time_zone(sql::ObExecContext &ctx,
                                        const ObSetVar &set_var,
                                        const ObBasicSysVar &sys_var,
                                        const common::ObObj &in_val,
                                        common::ObObj &out_val);
  static int check_and_convert_max_min_timestamp(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_ob_org_cluster_id(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_plsql_warnings(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val);
  static int check_and_convert_plsql_ccflags(sql::ObExecContext &ctx,
                                             const ObSetVar &set_var,
                                             const ObBasicSysVar &sys_var,
                                             const common::ObObj &in_val,
                                             common::ObObj &out_val);
  static int check_and_convert_sql_throttle_queue_time(sql::ObExecContext &ctx,
                                                       const ObSetVar &set_var,
                                                       const ObBasicSysVar &sys_var,
                                                       const common::ObObj &in_val,
                                                       common::ObObj &out_val);
  static int check_and_convert_nls_currency_too_long(sql::ObExecContext &ctx,
                                                     const ObSetVar &set_var,
                                                     const ObBasicSysVar &sys_var,
                                                     const common::ObObj &in_val,
                                                     common::ObObj &out_val);
  static int check_and_convert_nls_iso_currency_is_valid(sql::ObExecContext &ctx,
                                                         const ObSetVar &set_var,
                                                         const ObBasicSysVar &sys_var,
                                                         const common::ObObj &in_val,
                                                         common::ObObj &out_val);
  static int check_and_convert_nls_length_semantics_is_valid(sql::ObExecContext &ctx,
                                                             const ObSetVar &set_var,
                                                             const ObBasicSysVar &sys_var,
                                                             const common::ObObj &in_val,
                                                             common::ObObj &out_val);
  static int check_update_resource_manager_plan(sql::ObExecContext &ctx,
                                                const ObSetVar &set_var,
                                                const ObBasicSysVar &sys_var,
                                                const common::ObObj &val,
                                                common::ObObj &out_val);
  static int check_log_row_value_option_is_valid(sql::ObExecContext &ctx,
                                                  const ObSetVar &set_var,
                                                  const ObBasicSysVar &sys_var,
                                                  const common::ObObj &in_val,
                                                  common::ObObj &out_val);
  static int check_runtime_filter_type_is_valid(sql::ObExecContext &ctx,
                                                const ObSetVar &set_var,
                                                const ObBasicSysVar &sys_var,
                                                const common::ObObj &in_val,
                                                common::ObObj &out_val);
private:
  static int check_session_readonly(sql::ObExecContext &ctx,
                                    const ObSetVar &set_var,
                                    const ObBasicSysVar &sys_var,
                                    const common::ObObj &in_val,
                                    common::ObObj &out_val);
  static bool can_set_trans_var(ObSetVar::SetScopeType scope, sql::ObBasicSessionInfo &session);
  static int get_string(const common::ObObj &val, common::ObString &str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarOnCheckFuncs);
};

class ObSysVarOnUpdateFuncs
{
public:
  ObSysVarOnUpdateFuncs() {}
  virtual ~ObSysVarOnUpdateFuncs() {}

public:
  static int update_tx_isolation(sql::ObExecContext &ctx,
                                 const ObSetVar &set_var,
                                 const ObBasicSysVar &sys_var,
                                 const common::ObObj &val);
  static int update_tx_read_only_no_scope(sql::ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObBasicSysVar &sys_var,
                                          const common::ObObj &val);
  static int update_sql_mode(sql::ObExecContext &ctx,
                             const ObSetVar &set_var,
                             const ObBasicSysVar &sys_var,
                             const common::ObObj &val);
  static int update_safe_weak_read_snapshot(sql::ObExecContext &ctx,
                             const ObSetVar &set_var,
                             const ObBasicSysVar &sys_var,
                             const common::ObObj &val);
private:
  // start trans helper use by set transaction charactors
  static int start_trans_by_set_trans_char_(
                             sql::ObExecContext &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObSysVarOnUpdateFuncs);
};

class ObSysVarToObjFuncs
{
public:
  ObSysVarToObjFuncs() {}
  virtual ~ObSysVarToObjFuncs() {}
public:
  static int to_obj_charset(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                            const ObBasicSysVar &sys_var, common::ObObj &result_obj);
  static int to_obj_collation(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                              const ObBasicSysVar &sys_var, common::ObObj &result_obj);
  static int to_obj_sql_mode(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                             const ObBasicSysVar &sys_var, common::ObObj &result_obj);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarToObjFuncs);
};

class ObSysVarToStrFuncs
{
public:
  ObSysVarToStrFuncs() {}
  virtual ~ObSysVarToStrFuncs() {}
public:
  static int to_str_charset(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                            const ObBasicSysVar &sys_var, common::ObString &result_str);
  static int to_str_collation(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                              const ObBasicSysVar &sys_var, common::ObString &result_str);
  static int to_str_sql_mode(common::ObIAllocator &allocator, const sql::ObBasicSessionInfo &session,
                             const ObBasicSysVar &sys_var, common::ObString &result_str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarToStrFuncs);
};

class ObSysVarGetMetaTypeFuncs
{
public:
  ObSysVarGetMetaTypeFuncs() {}
  virtual ~ObSysVarGetMetaTypeFuncs() {}
public:
  static common::ObObjType get_meta_type_varchar() { return common::ObVarcharType; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarGetMetaTypeFuncs);
};

class ObSysVarSessionSpecialUpdateFuncs
{
public:
  ObSysVarSessionSpecialUpdateFuncs() {}
  virtual ~ObSysVarSessionSpecialUpdateFuncs() {}
public:
  // @@identiy alias to @@last_insert_id
  static int update_identity(sql::ObExecContext &ctx,
                             const ObSetVar &set_var,
                             const common::ObObj &val);
  static int update_last_insert_id(sql::ObExecContext &ctx,
                                   const ObSetVar &set_var,
                                   const common::ObObj &val);
  // @@tx_isolation alias to @@transaction_isolation
  static int update_tx_isolation(sql::ObExecContext &ctx,
                                 const ObSetVar &set_var,
                                 const common::ObObj &val);
  // @@tx_read_only alias to @@transaction_read_only
  static int update_tx_read_only(sql::ObExecContext &ctx,
                                 const ObSetVar &set_var,
                                 const common::ObObj &val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarSessionSpecialUpdateFuncs);
};

class ObCharsetSysVarPair
{
public:
  static const int64_t SYS_CHARSET_SYS_VAR_PAIR_COUNT = 3;
  static ObCharsetSysVarPair CHARSET_SYS_VAR_PAIRS[SYS_CHARSET_SYS_VAR_PAIR_COUNT];

  ObCharsetSysVarPair(const common::ObString &cs_var_name, const common::ObString &coll_var_name)
      : cs_var_name_(cs_var_name), coll_var_name_(coll_var_name)
  {
  }
  ObCharsetSysVarPair(const char *cs_var_name, const char *coll_var_name)
      : cs_var_name_(common::ObString(cs_var_name)), coll_var_name_(common::ObString(coll_var_name))
  {
  }
  virtual ~ObCharsetSysVarPair() {}

  static int get_charset_var_by_collation_var(const common::ObString &coll_var_name,
                                              common::ObString &cs_var_name);
  static int get_collation_var_by_charset_var(const common::ObString &cs_var_name,
                                              common::ObString &coll_var_name);

private:
  common::ObString cs_var_name_;
  common::ObString coll_var_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCharsetSysVarPair);
};

class ObBinlogRowImage
{
public:
  enum ImageType
  {
    MINIMAL = 0,
    NOBLOB = 1,
    FULL = 2,
  };
  ObBinlogRowImage() {}
  virtual ~ObBinlogRowImage() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObBinlogRowImage);
};

class ObPreProcessSysVars
{
public:
  ObPreProcessSysVars() {}
  virtual ~ObPreProcessSysVars() {}
public:
  static int init_sys_var();

private:
  static int change_initial_value();
private:
  DISALLOW_COPY_AND_ASSIGN(ObPreProcessSysVars);
};

class ObSysVarUtils
{
public:
  static int log_bounds_error_or_warning(sql::ObExecContext &ctx,
                                         const ObSetVar &set_var,
                                         const common::ObObj &in_val);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVarUtils);
};

}
}

#endif //OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_
