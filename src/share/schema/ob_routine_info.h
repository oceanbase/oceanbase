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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_INFO_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_INFO_H_
#include "share/schema/ob_schema_struct.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_fixed_array.h"
#include "pl/ob_pl_type.h"
#include "ob_trigger_info.h"


#define SP_PARAM_NO_FLAG      0
#define SP_PARAM_INTEGER_MASK 0x3C
#define SP_PARAM_MODE_MASK    0x03
#define RET_PARAM_POSITION    0
#define SP_PARAM_TYPE_MASK    0x3C
#define SP_PARAM_ATTR_MASK    0xC0
// record nocopy attribute
#define SP_PARAM_NOCOPY_MASK  0x100
// Is cast required by default value
#define SP_PARAM_DEFAULT_CAST 0x200

// flag_的低2位用来表示参数IN OUT属性
// 当Type是INT的时候,flag_的低3-6位用来表示PL_INTEGER类型
// 当Type是Extend的时候,flag_低3-6位用来表示外部类型的来源
enum ObParamExternType
{
  SP_EXTERN_INVALID = 0,
  SP_EXTERN_UDT,              // user define type
  SP_EXTERN_PKG,              // package type
  SP_EXTERN_PKG_VAR,          // package.var%type
  SP_EXTERN_TAB,              // table%rowtype
  SP_EXTERN_TAB_COL,          // table.col%type
  SP_EXTERN_PKGVAR_OR_TABCOL, // package.var%type or table.col%type
  SP_EXTERN_LOCAL_VAR,        // declare v number; function f return is v%type;
  SP_EXTERN_SYS_REFCURSOR,
  SP_EXTERN_DBLINK,
};

enum ObRoutineParamInOut
{
  SP_PARAM_INVALID = 0,
  SP_PARAM_IN = 1,
  SP_PARAM_OUT = 2,
  SP_PARAM_INOUT = 3,
};

// flag的7-8位用来表示这个参数的额外属性，
enum ObRoutineParamAttr{
  SP_PARAM_SELF = 1, // 这是一个udt类型udf的隐藏的 self 参数
};

enum ObRoutineFlag
{
  SP_FLAG_INVALID         = 1,
  SP_FLAG_NONEDITIONABLE  = 2,
  SP_FLAG_DETERMINISTIC   = 4,
  SP_FLAG_PARALLEL_ENABLE = 8,
  SP_FLAG_INVOKER_RIGHT = 16,
  SP_FLAG_RESULT_CACHE = 32,
  SP_FLAG_ACCESSIBLE_BY = 64,
  SP_FLAG_PIPELINED = 128,
  SP_FLAG_STATIC = 256, // udt static function
  SP_FLAG_UDT_MAP = 512, // UDT map function
  SP_FLAG_UDT_UDF = 1024, // this is udt udf
  SP_FLAG_UDT_FUNC = 2048, // this is udt function, else is procedure
  SP_FLAG_UDT_CONS = 4096, // this is udt constructor
  SP_FLAG_UDT_ORDER = 8192, // UDT order function
  SP_FLAG_AGGREGATE = 16384,
  SP_FLAG_NO_SQL = 32768,
  SP_FLAG_READS_SQL_DATA = 65536,
  SP_FLAG_MODIFIES_SQL_DATA = 131072,
  SP_FLAG_CONTAINS_SQL = 262144,
  SP_FLAG_WPS = SP_FLAG_CONTAINS_SQL * 2,
  SP_FLAG_RPS = SP_FLAG_WPS * 2,
  SP_FLAG_HAS_SEQUENCE = SP_FLAG_RPS * 2,
  SP_FLAG_HAS_OUT_PARAM = SP_FLAG_HAS_SEQUENCE * 2,
  SP_FLAG_EXTERNAL_STATE = SP_FLAG_HAS_OUT_PARAM * 2,
};

namespace oceanbase
{
namespace share
{
namespace schema
{
enum ObRoutineType
{
  INVALID_ROUTINE_TYPE = 0,
  ROUTINE_PROCEDURE_TYPE = 1,
  ROUTINE_FUNCTION_TYPE = 2,
  ROUTINE_PACKAGE_TYPE = 3,
  ROUTINE_UDT_TYPE = 4,
};

class ObIRoutineParam
{
public:
  ObIRoutineParam() {}
  virtual ~ObIRoutineParam() {}
  virtual const common::ObString& get_default_value() const = 0;
  virtual pl::ObPLDataType get_pl_data_type() const = 0;
  virtual const ObString& get_name() const = 0;
  virtual int64_t get_mode() const { return -1; }
  virtual bool is_schema_routine_param() const { return false; }
  virtual bool is_in_param() const { return false; }
  virtual bool is_out_param() const { return false; }
  virtual bool is_inout_param() const { return false; }
  virtual bool is_nocopy_param() const { return false; }
  virtual bool is_ret_param() const { return false; }
  virtual bool is_default_cast() const { return false; }
  virtual bool is_self_param() const { return false; }

  TO_STRING_KV(K(is_schema_routine_param()));
};

class ObIRoutineInfo
{
public:
  virtual int64_t get_param_count() const = 0;
  virtual int find_param_by_name(const common::ObString &name, int64_t &position) const = 0;
  virtual int get_routine_param(int64_t position, ObIRoutineParam *&param) const = 0;
  virtual const ObIRoutineParam* get_ret_info() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual uint64_t get_database_id() const = 0;
  virtual uint64_t get_package_id() const = 0;
  virtual void set_deterministic() = 0;
  virtual bool is_deterministic() const = 0;
  virtual void set_parallel_enable() = 0;
  virtual bool is_parallel_enable() const = 0;
  virtual void set_invoker_right() {}
  virtual bool is_invoker_right() const { return false; }
  virtual void set_result_cache() = 0;
  virtual bool is_result_cache() const = 0;
  virtual void set_accessible_by_clause() = 0;
  virtual bool has_accessible_by_clause() const = 0;
  virtual bool is_udt_static_routine() const = 0;
  virtual bool is_udt_routine() const = 0;
  virtual bool is_udt_cons() const = 0;
  virtual bool is_udt_map() const = 0;
  virtual bool is_udt_order() const = 0;
  virtual void set_pipelined() = 0;
  virtual bool is_pipelined() const = 0;
  virtual void set_no_sql() = 0;
  virtual bool is_no_sql() const = 0;
  virtual void set_reads_sql_data() = 0;
  virtual bool is_reads_sql_data() const = 0;
  virtual void set_modifies_sql_data() = 0;
  virtual bool is_modifies_sql_data() const = 0;
  virtual void set_contains_sql() = 0;
  virtual bool is_contains_sql() const = 0;
  virtual bool is_wps() const = 0;
  virtual bool is_rps() const = 0;
  virtual bool is_has_sequence() const = 0;
  virtual bool is_has_out_param() const = 0;
  virtual bool is_external_state() const = 0;
  virtual void set_wps() = 0;
  virtual void set_rps() = 0;
  virtual void set_has_sequence() = 0;
  virtual void set_has_out_param() = 0;
  virtual void set_external_state() = 0;
  virtual int64_t get_param_start_idx() const { return 0; }
  virtual const common::ObString &get_routine_name() const = 0;
  virtual uint64_t get_dblink_id() const { return OB_INVALID_ID; }

  TO_STRING_EMPTY();
};

class ObRoutineParam: public ObSchema, public ObIRoutineParam
{
  OB_UNIS_VERSION(1);
public:
  ObRoutineParam();
  explicit ObRoutineParam(common::ObIAllocator *allocator);
  ObRoutineParam(const ObRoutineParam &src_schema);
  virtual ~ObRoutineParam();
  ObRoutineParam &operator =(const ObRoutineParam &src_schema);
  bool is_user_field_valid() const;
  bool is_valid() const;
  void reset();
  int64_t get_convert_size() const;
  bool is_same(const ObRoutineParam &other) const;
  //getter
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_routine_id() const { return routine_id_; }
  OB_INLINE int64_t get_sequence() const { return sequence_; }
  OB_INLINE int64_t get_subprogram_id() const { return subprogram_id_; }
  OB_INLINE int64_t get_param_position() const { return param_position_; }
  OB_INLINE int64_t get_param_level() const { return param_level_; }
  OB_INLINE const common::ObString &get_param_name() const { return param_name_; }
  OB_INLINE const common::ObString &get_name() const { return get_param_name(); }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE const common::ObDataType &get_param_type() const { return param_type_; }
  OB_INLINE int64_t get_flag() const { return flag_; }
  OB_INLINE int64_t get_param_mode() const { return flag_ & 0x03; }
  OB_INLINE const common::ObString &get_default_value() const { return default_value_; }
  OB_INLINE uint64_t get_type_owner() const { return type_owner_; }
  OB_INLINE const common::ObString &get_type_name() const { return type_name_; }
  OB_INLINE const common::ObString &get_type_subname() const { return type_subname_; }
  OB_INLINE const common::ObIArray<common::ObString> &get_extended_type_info() const { return extended_type_info_; }

  //setter
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  OB_INLINE void set_sequence(int64_t sequence) { sequence_ = sequence; }
  OB_INLINE void set_subprogram_id(int64_t subprogram_id) { subprogram_id_ = subprogram_id; }
  OB_INLINE void set_param_position(int64_t position) { param_position_ = position; }
  OB_INLINE void set_param_level(int64_t level) { param_level_ = level; }
  OB_INLINE int set_param_name(const common::ObString &param_name) { return deep_copy_str(param_name, param_name_); }
  OB_INLINE void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_param_type(const common::ObDataType &param_type) { param_type_ = param_type; }
  OB_INLINE void set_param_type(const common::ObObjType &type) { param_type_.set_obj_type(type); }
  OB_INLINE void set_param_length(common::ObLength length) { param_type_.set_length(length); }
  OB_INLINE void set_param_precision(common::ObPrecision precision) { param_type_.set_precision(precision); }
  OB_INLINE void set_param_scale(common::ObScale scale) { param_type_.set_scale(scale); }
  OB_INLINE void set_param_zero_fill(bool is_zero_fill) { param_type_.set_zero_fill(is_zero_fill); }
  OB_INLINE void set_param_charset(common::ObCharsetType charset) { param_type_.set_charset_type(charset); }
  OB_INLINE void set_param_coll_type(common::ObCollationType coll_type) { param_type_.set_collation_type(coll_type); }
  OB_INLINE void set_flag(int64_t flag) { flag_ = flag; }
  OB_INLINE void set_type_owner(uint64_t type_owner) { type_owner_ = type_owner; }
  OB_INLINE int set_type_name(const common::ObString &type_name) { return deep_copy_str(type_name, type_name_); }
  OB_INLINE int set_type_subname(const common::ObString &type_subname) { return deep_copy_str(type_subname, type_subname_); }
  OB_INLINE int set_default_value(const common::ObString &default_value) { return deep_copy_str(default_value, default_value_); }

  OB_INLINE bool is_ret_param() const { return RET_PARAM_POSITION == param_position_; }
  OB_INLINE bool is_in_sp_param() const { return SP_PARAM_IN == (flag_ & SP_PARAM_MODE_MASK); }
  OB_INLINE bool is_out_sp_param() const { return SP_PARAM_OUT == (flag_ & SP_PARAM_MODE_MASK); }
  OB_INLINE bool is_inout_sp_param() const { return SP_PARAM_INOUT == (flag_ & SP_PARAM_MODE_MASK); }
  OB_INLINE void set_in_sp_param_flag() { flag_ = (flag_ & ~SP_PARAM_MODE_MASK) | SP_PARAM_IN; }
  OB_INLINE void set_out_sp_param_flag() { flag_ = (flag_ & ~SP_PARAM_MODE_MASK) | SP_PARAM_OUT; }
  OB_INLINE void set_inout_sp_param_flag() { flag_ = (flag_ & ~SP_PARAM_MODE_MASK) | SP_PARAM_INOUT; }
  OB_INLINE int64_t get_mode() const { return flag_ & SP_PARAM_MODE_MASK; }
  // ObExtendType标识该Type不是一个基础类型, 通过flag_来判断最终的类型
  OB_INLINE bool is_complex_type() const { return ObExtendType == param_type_.get_obj_type() && is_udt_type(); }
  OB_INLINE bool is_extern_type() const { return ObExtendType == param_type_.get_obj_type(); }

  int set_extended_type_info(const common::ObIArray<common::ObString> &extended_type_info);
  int serialize_extended_type_info(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_extended_type_info(const char *buf, const int64_t data_len, int64_t &pos);

  OB_INLINE void set_extern_type_flag(ObParamExternType type)
  {
    flag_ &= ~SP_PARAM_INTEGER_MASK;
    flag_ |= type << 2;
  }
  OB_INLINE ObParamExternType get_extern_type_flag() const
  {
    ObParamExternType type = SP_EXTERN_INVALID;
    if (common::ObExtendType == param_type_.get_obj_type()) {
      type = static_cast<ObParamExternType>((flag_ & SP_PARAM_INTEGER_MASK) >> 2);
    }
    return type;
  }
  OB_INLINE void set_udt_type() { set_extern_type_flag(SP_EXTERN_UDT); }
  OB_INLINE void set_pkg_type() { set_extern_type_flag(SP_EXTERN_PKG); }
  OB_INLINE void set_pkg_var_type() { set_extern_type_flag(SP_EXTERN_PKG_VAR); }
  OB_INLINE void set_table_row_type() { set_extern_type_flag(SP_EXTERN_TAB); }
  OB_INLINE void set_table_col_type() { set_extern_type_flag(SP_EXTERN_TAB_COL); }
  OB_INLINE void set_sys_refcursor_type() { set_extern_type_flag(SP_EXTERN_SYS_REFCURSOR); }

  OB_INLINE bool is_udt_type() const { return SP_EXTERN_UDT == get_extern_type_flag(); }
  OB_INLINE bool is_pkg_type() const { return SP_EXTERN_PKG == get_extern_type_flag(); }
  OB_INLINE bool is_pkg_var_type() const { return SP_EXTERN_PKG_VAR == get_extern_type_flag(); }
  OB_INLINE bool is_table_row_type() const { return SP_EXTERN_TAB == get_extern_type_flag(); }
  OB_INLINE bool is_table_col_type() const { return SP_EXTERN_TAB_COL == get_extern_type_flag(); }
  OB_INLINE bool is_sys_refcursor_type() const
  {
    return SP_EXTERN_SYS_REFCURSOR == get_extern_type_flag();
  }
  OB_INLINE bool is_dblink_type() const { return SP_EXTERN_DBLINK == get_extern_type_flag(); }

  OB_INLINE bool is_pl_integer_type() const
  {
    return common::ObInt32Type == param_type_.get_obj_type()
           && (flag_ & SP_PARAM_INTEGER_MASK) != 0;
  }
  OB_INLINE void set_pl_integer_type(const pl::ObPLIntegerType &type)
  {
    flag_ &= ~SP_PARAM_INTEGER_MASK;
    flag_ |= (type << 2);
  }
  OB_INLINE pl::ObPLIntegerType get_pl_integer_type() const
  {
    return static_cast<pl::ObPLIntegerType>((flag_ & SP_PARAM_INTEGER_MASK) >> 2);
  }
  OB_INLINE pl::ObPLDataType get_pl_data_type() const
  {
    pl::ObPLDataType type;
    if (is_pl_integer_type()) {
      type.set_pl_integer_type(get_pl_integer_type(), get_param_type());
      pl::ObPLIntegerType pls_type = type.get_pl_integer_type();
      switch (pls_type) {
        case pl::PL_PLS_INTEGER:
        case pl::PL_BINARY_INTEGER:
        case pl::PL_SIMPLE_INTEGER: {
          type.set_range(-2147483648, 2147483647);
          type.set_not_null(pl::PL_SIMPLE_INTEGER == pls_type);
        }
        break;
        case pl::PL_NATURAL:
        case pl::PL_NATURALN: {
          type.set_range(0, 2147483647);
          type.set_not_null(pl::PL_NATURALN == pls_type);
        }
        break;
        case pl::PL_POSITIVE:
        case pl::PL_POSITIVEN: {
          type.set_range(1, 2147483647);
          type.set_not_null(pl::PL_POSITIVEN == pls_type);
        }
        break;
        case pl::PL_SIGNTYPE: {
          type.set_range(-1, 1);
        }
        break;
        default: // do nothing ...
        break;
      }
    } else if (is_sys_refcursor_type()) {
      type.set_type(pl::PL_REF_CURSOR_TYPE);
      type.set_type_from(pl::PL_TYPE_SYS_REFCURSOR);
    } else {
      type.set_data_type(get_param_type());
    }
    return type;
  }
  OB_INLINE bool is_schema_routine_param() const { return true; }

  virtual bool is_in_param() const { return is_in_sp_param(); }
  virtual bool is_out_param() const { return is_out_sp_param(); }
  virtual bool is_inout_param() const { return is_inout_sp_param(); }

  OB_INLINE void set_nocopy_param()
  {
    flag_ = (flag_ & ~SP_PARAM_NOCOPY_MASK) | SP_PARAM_NOCOPY_MASK;
  }
  OB_INLINE bool is_nocopy_param() const
  {
    return (flag_ & SP_PARAM_NOCOPY_MASK) > 0;
  }
  OB_INLINE void set_default_cast()
  {
    flag_ = (flag_ & ~SP_PARAM_DEFAULT_CAST) | SP_PARAM_DEFAULT_CAST;
  }
  OB_INLINE bool is_default_cast()
  {
    return (flag_ & SP_PARAM_DEFAULT_CAST) > 0;
  }
  OB_INLINE bool is_self_param() const 
  { return SP_PARAM_SELF == (flag_ & SP_PARAM_ATTR_MASK) >> 6; }

  OB_INLINE void set_is_self_param() {
    flag_ &= ~SP_PARAM_ATTR_MASK;
    flag_ = flag_ | SP_PARAM_SELF << 6;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(routine_id),
               K_(sequence),
               K_(subprogram_id),
               K_(param_position),
               K_(param_level),
               K_(param_name),
               K_(schema_version),
               K_(param_type),
               K_(flag),
               K_(default_value),
               K_(type_owner),
               K_(type_name),
               K_(type_subname),
               K_(extended_type_info));
private:
  uint64_t tenant_id_;            //set by sys
  uint64_t routine_id_;           //set by sys
  int64_t sequence_;              //set by sys, current level index
  int64_t subprogram_id_;         //set by sys, 0 indicate standalone routine, others indicate pacakge routine index
  int64_t param_position_;        //set by user, 0 is ret type
  int64_t param_level_;           //set by user, for complex type, current level
  common::ObString param_name_;   //set by user
  int64_t schema_version_;        //set by sys
  common::ObDataType param_type_; //set by user, ObExtendType indicate complex type/%type/%rowtype
  int64_t flag_;                  //set by user
  common::ObString default_value_;//set by user, mysql procedure parameters have no default value
  uint64_t type_owner_;           //set by user, valid if ObExtendType
  common::ObString type_name_;    //set by user, valid if ObExtendType
  common::ObString type_subname_; //set by user, valid if ObExtendType, indicate pacakge name
  common::ObArrayHelper<common::ObString> extended_type_info_; // set by user, for enum/set info
};

class ObRoutineInfo: public ObSchema, public ObIRoutineInfo, public IObErrorInfo
{
  OB_UNIS_VERSION(1);
public:
  ObRoutineInfo();
  explicit ObRoutineInfo(common::ObIAllocator *allocator);
  ObRoutineInfo(const ObRoutineInfo &src_schema);
  virtual ~ObRoutineInfo();
  ObRoutineInfo &operator=(const ObRoutineInfo &src_schema);
  int assign(const ObRoutineInfo &other);
  bool is_user_field_valid() const;
  bool is_valid() const;
  void reset();
  int64_t get_convert_size() const;
  int add_routine_param(const ObRoutineParam &routine_param);
  const common::ObDataType *get_ret_type() const;
  const common::ObIArray<common::ObString> *get_ret_type_info() const;
  int find_param_by_name(const common::ObString& name, int64_t &position) const;
  int get_routine_param(int64_t idx, ObRoutineParam*& param) const;
  int get_routine_param(int64_t idx, ObIRoutineParam*& param) const;
  const ObIRoutineParam* get_ret_info() const;
  // getter
  int64_t get_out_param_count() const;
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_database_id() const { return database_id_; }
  OB_INLINE uint64_t get_package_id() const { return package_id_; }
  OB_INLINE uint64_t get_owner_id() const { return owner_id_; }
  OB_INLINE uint64_t get_routine_id() const { return routine_id_; }
  OB_INLINE uint64_t get_object_id() const { return routine_id_; }
  OB_INLINE ObObjectType get_object_type() const
  { return ObRoutineType::ROUTINE_PROCEDURE_TYPE == get_routine_type() ?
           ObObjectType::PROCEDURE : ObRoutineType::ROUTINE_FUNCTION_TYPE ?
           ObObjectType::FUNCTION : ObObjectType::INVALID; }
  OB_INLINE const common::ObString &get_routine_name() const { return routine_name_; }
  OB_INLINE int64_t get_overload() const { return overload_; }
  OB_INLINE int64_t get_subprogram_id() const { return subprogram_id_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE ObRoutineType get_routine_type() const { return routine_type_; }
  OB_INLINE int64_t get_flag() const { return flag_; }
  OB_INLINE const common::ObString &get_priv_user() const { return priv_user_; }
  OB_INLINE int64_t get_comp_flag() const { return comp_flag_; }
  OB_INLINE const common::ObString &get_exec_env() const { return exec_env_; }
  OB_INLINE const common::ObString &get_routine_body() const { return routine_body_; }
  OB_INLINE const common::ObString &get_comment() const { return comment_; }
  OB_INLINE const common::ObIArray<ObRoutineParam*> &get_routine_params() const { return routine_params_; }
  OB_INLINE common::ObIArray<ObRoutineParam*> &get_routine_params() { return routine_params_; }
  OB_INLINE int64_t get_type_id() const { return type_id_; }
  OB_INLINE TgTimingEvent get_tg_timing_event() const { return tg_timing_event_; }
  OB_INLINE uint64_t get_dblink_id() const { return dblink_id_; }
  OB_INLINE const common::ObString &get_dblink_db_name() const { return dblink_db_name_; }
  OB_INLINE const common::ObString &get_dblink_pkg_name() const { return dblink_pkg_name_; }

  // setter
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  OB_INLINE void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  OB_INLINE void set_owner_id(int64_t owner_id) { owner_id_ = owner_id; }
  OB_INLINE void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  OB_INLINE int set_routine_name(const common::ObString &routine_name) { return deep_copy_str(routine_name, routine_name_); }
  OB_INLINE void set_overload(int64_t overload) { overload_ = overload; }
  OB_INLINE void set_subprogram_id(int64_t subprogram_id) { subprogram_id_ = subprogram_id; }
  OB_INLINE void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_routine_type(ObRoutineType routine_type) { routine_type_ = routine_type; }
  OB_INLINE void set_flag(int64_t flag) { flag_ = flag; }
  OB_INLINE int set_priv_user(const common::ObString &priv_user_name) { return deep_copy_str(priv_user_name, priv_user_); }
  OB_INLINE void set_comp_flag(int64_t comp_flag) { comp_flag_ = comp_flag; }
  OB_INLINE int set_exec_env(const common::ObString &exec_env) { return deep_copy_str(exec_env, exec_env_); }
  OB_INLINE int set_routine_body(const common::ObString &routine_body) { return deep_copy_str(routine_body, routine_body_); }
  OB_INLINE int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  OB_INLINE const common::ObString &get_route_sql() const { return route_sql_; }
  OB_INLINE int set_route_sql(const common::ObString &route_sql) { return deep_copy_str(route_sql, route_sql_); }
  OB_INLINE bool is_procedure() const { return NULL == get_ret_type()?true:false; }
  OB_INLINE bool is_function() const { return !is_procedure(); }
  OB_INLINE int64_t get_param_count() const { return is_procedure()?routine_params_.count():routine_params_.count()-1; }
  OB_INLINE int64_t get_param_start_idx() const { return is_procedure()?0:1; }
  OB_INLINE void set_type_id(int64_t type_id) { type_id_ = type_id; }
  OB_INLINE void set_tg_timing_event(TgTimingEvent tg) { tg_timing_event_ = tg; }
  OB_INLINE void set_dblink_id(uint64_t dblink_id) { dblink_id_ = dblink_id; }
  OB_INLINE int set_dblink_db_name(const common::ObString &db_name) { return deep_copy_str(db_name, dblink_db_name_); }
  OB_INLINE int set_dblink_pkg_name(const common::ObString &pkg_name)
                  { return deep_copy_str(pkg_name, dblink_pkg_name_); }
  OB_INLINE void set_routine_invalid() { flag_ |= SP_FLAG_INVALID; }
  OB_INLINE void set_noneditionable() { flag_ |= SP_FLAG_NONEDITIONABLE; }
  OB_INLINE void set_deterministic() { flag_ |= SP_FLAG_DETERMINISTIC; }
  OB_INLINE void set_parallel_enable() { flag_ |= SP_FLAG_PARALLEL_ENABLE; }
  OB_INLINE void set_invoker_right() { flag_ |= SP_FLAG_INVOKER_RIGHT; }
  OB_INLINE void clear_invoker_right() { flag_ &= (~((uint64_t)SP_FLAG_INVOKER_RIGHT)); }
  OB_INLINE void set_result_cache() { flag_ |= SP_FLAG_RESULT_CACHE; }
  OB_INLINE void set_accessible_by_clause() { flag_ |= SP_FLAG_ACCESSIBLE_BY; }
  OB_INLINE void set_pipelined() { flag_ |= SP_FLAG_PIPELINED; }
  OB_INLINE void set_is_static() { flag_ |= SP_FLAG_STATIC; }
  OB_INLINE void set_is_udt_udf() { flag_ |= SP_FLAG_UDT_UDF; }
  OB_INLINE void set_is_udt_function() { flag_ |= SP_FLAG_UDT_FUNC; }
  OB_INLINE void set_is_udt_cons() { flag_ |= SP_FLAG_UDT_CONS; }
  OB_INLINE void set_is_udt_order() { flag_ |= SP_FLAG_UDT_ORDER; }
  OB_INLINE void set_is_udt_map() { flag_ |= SP_FLAG_UDT_MAP; }
  OB_INLINE void set_is_aggregate() { flag_ |= SP_FLAG_AGGREGATE; }
  OB_INLINE void set_no_sql() { flag_ &= ~SP_FLAG_READS_SQL_DATA; flag_ &= ~SP_FLAG_MODIFIES_SQL_DATA; flag_ |= SP_FLAG_NO_SQL;}
  OB_INLINE void set_reads_sql_data() { flag_ &= ~SP_FLAG_NO_SQL; flag_ &= ~SP_FLAG_MODIFIES_SQL_DATA; flag_ |= SP_FLAG_READS_SQL_DATA;}
  OB_INLINE void set_modifies_sql_data() { flag_ &= ~SP_FLAG_NO_SQL; flag_ &= ~SP_FLAG_READS_SQL_DATA; flag_ |= SP_FLAG_MODIFIES_SQL_DATA;}
  OB_INLINE void set_contains_sql()
  {
    flag_ &= ~SP_FLAG_NO_SQL;
    flag_ &= ~SP_FLAG_READS_SQL_DATA;
    flag_ &= ~SP_FLAG_MODIFIES_SQL_DATA;
    flag_ |= SP_FLAG_CONTAINS_SQL;
  }


  OB_INLINE bool is_wps() const { return SP_FLAG_WPS == (flag_ & SP_FLAG_WPS); }
  OB_INLINE bool is_rps() const { return SP_FLAG_RPS == (flag_ & SP_FLAG_RPS); }
  OB_INLINE bool is_has_sequence() const { return SP_FLAG_HAS_SEQUENCE == (flag_ & SP_FLAG_HAS_SEQUENCE); }
  OB_INLINE bool is_has_out_param() const { return SP_FLAG_HAS_OUT_PARAM == (flag_ & SP_FLAG_HAS_OUT_PARAM); }
  OB_INLINE bool is_external_state() const { return SP_FLAG_EXTERNAL_STATE == (flag_ & SP_FLAG_EXTERNAL_STATE); }

  OB_INLINE void set_wps() { flag_ |= SP_FLAG_WPS;}
  OB_INLINE void set_rps() { flag_ |= SP_FLAG_RPS;}
  OB_INLINE void set_has_sequence() { flag_ |= SP_FLAG_HAS_SEQUENCE;}
  OB_INLINE void set_has_out_param() { flag_ |= SP_FLAG_HAS_OUT_PARAM;}
  OB_INLINE void set_external_state() { flag_ |= SP_FLAG_EXTERNAL_STATE;}

  OB_INLINE bool is_aggregate() const { return SP_FLAG_AGGREGATE == (flag_ & SP_FLAG_AGGREGATE); }

  OB_INLINE bool is_udt_order() const { return SP_FLAG_UDT_ORDER == (flag_ & SP_FLAG_UDT_ORDER); }
  OB_INLINE bool is_udt_map() const { return SP_FLAG_UDT_MAP == (flag_ & SP_FLAG_UDT_MAP); }
  OB_INLINE bool is_udt_cons() const { return SP_FLAG_UDT_CONS == (flag_ & SP_FLAG_UDT_CONS); }
  OB_INLINE bool is_udt_routine() const { return SP_FLAG_UDT_UDF == (flag_ & SP_FLAG_UDT_UDF); }
  OB_INLINE bool is_udt_function() const
  {
     return is_udt_routine() && SP_FLAG_UDT_FUNC == (flag_ & SP_FLAG_UDT_FUNC);
  }
  OB_INLINE bool is_udt_procedure() const {
    return is_udt_routine() && SP_FLAG_UDT_FUNC != (flag_ & SP_FLAG_UDT_FUNC);
  }

  OB_INLINE bool is_routine_invalid() const
  {
    return SP_FLAG_INVALID == (flag_ & SP_FLAG_INVALID);
  }
  OB_INLINE bool is_noneditionable() const
  {
    return SP_FLAG_NONEDITIONABLE == (flag_ & SP_FLAG_NONEDITIONABLE);
  }
  OB_INLINE bool is_deterministic() const
  {
    return SP_FLAG_DETERMINISTIC == (flag_ & SP_FLAG_DETERMINISTIC);
  }
  OB_INLINE bool is_no_sql() const {
    return SP_FLAG_NO_SQL == (flag_ & SP_FLAG_NO_SQL);
  }
  OB_INLINE bool is_reads_sql_data() const {
    return SP_FLAG_READS_SQL_DATA == (flag_ & SP_FLAG_READS_SQL_DATA);
  }
  OB_INLINE bool is_modifies_sql_data() const {
    return SP_FLAG_MODIFIES_SQL_DATA == (flag_ & SP_FLAG_MODIFIES_SQL_DATA);
  }
  OB_INLINE bool is_contains_sql() const {
    return !(is_no_sql()||is_reads_sql_data()||is_modifies_sql_data());
  }
  OB_INLINE bool is_parallel_enable() const
  {
    return SP_FLAG_PARALLEL_ENABLE == (flag_ & SP_FLAG_PARALLEL_ENABLE);
  }
  OB_INLINE bool is_invoker_right() const
  {
    return SP_FLAG_INVOKER_RIGHT == (flag_ & SP_FLAG_INVOKER_RIGHT);
  }
  OB_INLINE bool is_result_cache() const
  {
    return SP_FLAG_RESULT_CACHE == (flag_ & SP_FLAG_RESULT_CACHE);
  }
  OB_INLINE bool has_accessible_by_clause() const
  {
    return SP_FLAG_ACCESSIBLE_BY == (flag_ & SP_FLAG_ACCESSIBLE_BY);
  }
  OB_INLINE bool is_pipelined() const
  {
    return SP_FLAG_PIPELINED == (flag_ & SP_FLAG_PIPELINED);
  }
  virtual bool is_udt_static_routine() const {
    return is_udt_routine() && SP_FLAG_STATIC == (flag_ & SP_FLAG_STATIC);
  }

  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(package_id),
               K_(owner_id),
               K_(routine_id),
               K_(routine_name),
               K_(overload),
               K_(subprogram_id),
               K_(schema_version),
               K_(routine_type),
               K_(flag),
               K_(priv_user),
               K_(comp_flag),
               K_(exec_env),
               K_(routine_body),
               K_(comment),
               K_(route_sql),
               K_(type_id),
               K_(routine_params));
private:
  uint64_t tenant_id_;            //set by user,
  uint64_t database_id_;          //set by sys,
  uint64_t package_id_;           //set by user,OB_INVALID_ID for standalone routine
  uint64_t owner_id_;             //set by user,
  uint64_t routine_id_;           //set by sys,
  common::ObString routine_name_; //set by user,
  int64_t overload_;              //set by user, 0 indicate no overload
  int64_t subprogram_id_;         //set by user, 0 indicate standalone routine, others indicate pacakge routine index
  int64_t schema_version_;        //set by sys
  ObRoutineType routine_type_;    //set by user
  int64_t flag_;                  //set by user
  common::ObString priv_user_;    //set by user
  int64_t comp_flag_;             //set by user
  common::ObString exec_env_;     //set by user
  common::ObString routine_body_; //set by user
  common::ObString comment_;      //set by user
  common::ObString route_sql_;
  int64_t type_id_;               //set by user,NOT OB_INVALID_ID for aggregate routine
  common::ObSEArray<ObRoutineParam *, 64> routine_params_;
  //set by user, for function, idx 0 param is ret type
  TgTimingEvent tg_timing_event_;
  uint64_t dblink_id_;
  common::ObString dblink_db_name_;
  common::ObString dblink_pkg_name_;
};
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_INFO_H_ */

