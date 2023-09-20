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

#ifndef OCEANBASE_SRC_PL_OB_PL_TYPE_H_
#define OCEANBASE_SRC_PL_OB_PL_TYPE_H_

#include "share/ob_define.h"
#include "objit/ob_llvm_helper.h"
#include "objit/common/ob_item_type.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_fast_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/tx/ob_trans_define.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "ob_pl_adt_service.h"
#include "ob_pl_di_adt_service.h"

#define ObCursorType ObIntType
#define ObPtrType ObIntType

#define IS_TYPE_FROM_TYPE_OR_ROWTYPE(type_from)  \
      (PL_TYPE_ATTR_ROWTYPE == type_from) ||     \
      (PL_TYPE_ATTR_TYPE == type_from)

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlExpression;
class ObRawExprFactory;
class ObExecContext;
struct ObSPICursor;
class ObSPIResultSet;
class ObSQLSessionInfo;
}
namespace common
{
class ObTimeZoneInfo;
}

namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObRoutineParam;
class ObIRoutineParam;
class ObIRoutineInfo;
class ObSchemaGetterGuard;
struct ObSchemaObjVersion;
}
}

namespace pl
{
struct ObPLExecCtx;
class ObPLResolver;
class ObPLResolveCtx;
class ObPLCodeGenerator;
class ObPLBlockNS;
class ObPLRoutineParam;
class ObPLUserTypeTable;
class ObUserDefinedType;
class ObPLStmt;
class ObPLDbLinkGuard;

enum ObProcType
{
  INVALID_PROC_TYPE = 0,
  STANDALONE_PROCEDURE,
  STANDALONE_FUNCTION,
  PACKAGE_PROCEDURE, /* A subprogram created inside a package is a packaged subprogram */
  PACKAGE_FUNCTION,
  NESTED_PROCEDURE, /* A subprogram created inside a PL/SQL block is a nested subprogram */
  NESTED_FUNCTION,
  STANDALONE_ANONYMOUS,
  UDT_PROCEDURE,
  UDT_FUNCTION,
};

enum ObPLType
{
  PL_INVALID_TYPE = -1,
  PL_OBJ_TYPE, //pl type scope from object type
  PL_RECORD_TYPE, //pl typoe scope from user defined
  PL_NESTED_TABLE_TYPE,
  PL_ASSOCIATIVE_ARRAY_TYPE,
  PL_VARRAY_TYPE,
  PL_CURSOR_TYPE,
  PL_SUBTYPE,
  // pls_integer, binary_integer, natural, naturaln, positive, positiven, signtype, simple_integer
  PL_INTEGER_TYPE,
  PL_REF_CURSOR_TYPE,
  PL_OPAQUE_TYPE,
};

enum ObPLOpaqueType
{
  PL_INVALID = -1,
  PL_ANY_TYPE = 0,
  PL_ANY_DATA = 1,
  PL_XML_TYPE = 2,
  PL_JSON_TYPE = 3
};

enum ObPLIntegerType
{
  PL_INTEGER_INVALID = 0,
  PL_PLS_INTEGER,
  PL_BINARY_INTEGER,
  PL_NATURAL,
  PL_NATURALN,
  PL_POSITIVE,
  PL_POSITIVEN,
  PL_SIGNTYPE,
  PL_SIMPLE_INTEGER,
  PL_INTEGER_MAX,
};

enum ObPLGenericType
{
  // match with package standard`s type
  PL_GENERIC_INVALID = 0,
  PL_ADT_1,
  PL_RECORD_1,
  PL_TUPLE_1,
  PL_VARRAY_1,
  PL_V2_TABLE_1,
  PL_TABLE_1,
  PL_COLLECTION_1,
  PL_REF_CURSOR_1,

  PL_TYPED_TABLE,
  PL_ADT_WITH_OID,
  PL_SYS_INT_V2TABLE,
  PL_SYS_BULK_ERROR_RECORD,
  PL_SYS_REC_V2TABLE,
  PL_ASSOC_ARRAY_1,

  PL_GENERIC_MAX
};

enum parent_expr_type : int8_t {
  EXPR_UNKNOWN = -1,
  EXPR_PRIOR,
  EXPR_NEXT,
  EXPR_EXISTS,
};

struct ObPLIntegerRange {
public:
  ObPLIntegerRange() : lower_(2147483647), upper_(-2147483648) {}
  ObPLIntegerRange(int64_t range) { range_ = range; }

  inline void reset() { lower_ = 2147483647; upper_ = -2147483648; }
  inline bool valid() { return lower_ <= upper_; }
  inline int32_t get_lower() const { return lower_; }
  inline int32_t get_upper() const { return upper_; }
  inline void set_range(int64_t range) { range_ = range; }
  inline void set_range(int32_t lower, int32_t upper)
  {
    lower_ = lower;
    upper_ = upper;
  }
  union {
    int64_t range_;
    struct {
      int32_t lower_;
      int32_t upper_;
    };
  };
  TO_STRING_KV(K(range_), K(lower_), K(upper_));
};

class ObPLINS;

enum ObPLTypeFrom
{
  PL_TYPE_LOCAL,
  PL_TYPE_PACKAGE,
  PL_TYPE_UDT,
  PL_TYPE_ATTR_ROWTYPE,
  PL_TYPE_ATTR_TYPE,
  PL_TYPE_SYS_REFCURSOR,
  PL_TYPE_DBLINK,
};

enum ObPLTypeSize
{
  PL_TYPE_ROW_SIZE,
  PL_TYPE_INIT_SIZE
};

struct ObPLExternTypeInfo
{
  ObPLExternTypeInfo()
    : flag_(0),
      type_owner_(OB_INVALID_ID),
      type_name_(),
      type_subname_(),
      obj_version_() {}

  void reset()
  {
    flag_ = 0;
    type_owner_ = OB_INVALID_ID;
    type_name_.reset();
    type_subname_.reset();
  }

  int64_t flag_; // 表示当前Type的来源
  uint64_t type_owner_; // TypeOwner
  common::ObString type_name_; // TypeName
  common::ObString type_subname_; // 根据flag确定是PackageName还是TableName
  share::schema::ObSchemaObjVersion obj_version_;
  TO_STRING_KV(K_(flag), K_(type_owner), K_(type_name), K_(type_subname), K_(obj_version));
};

class ObPLDataType
{
public:
  ObPLDataType()
    : type_(PL_INVALID_TYPE),
      type_from_(PL_TYPE_LOCAL),
      type_from_origin_(PL_TYPE_LOCAL),
      obj_type_(),
      user_type_id_(common::OB_INVALID_ID),
      not_null_(false),
      pls_type_(ObPLIntegerType::PL_INTEGER_INVALID),
      type_info_() {}
  ObPLDataType(ObPLType type)
    : type_(type),
      type_from_(PL_TYPE_LOCAL),
      type_from_origin_(PL_TYPE_LOCAL),
      obj_type_(),
      user_type_id_(common::OB_INVALID_ID),
      not_null_(false),
      pls_type_(ObPLIntegerType::PL_INTEGER_INVALID),
      type_info_() {}
  ObPLDataType(common::ObObjType type)
    : type_(PL_OBJ_TYPE),
      type_from_(PL_TYPE_LOCAL),
      type_from_origin_(PL_TYPE_LOCAL),
      obj_type_(),
      user_type_id_(common::OB_INVALID_ID),
      not_null_(false),
      pls_type_(ObPLIntegerType::PL_INTEGER_INVALID),
      type_info_()
  {
    common::ObDataType data_type;
    data_type.set_obj_type(type);
    set_data_type(data_type);
  }
  ObPLDataType(const ObPLDataType &other)
    : type_(other.type_),
      type_from_(other.type_from_),
      type_from_origin_(other.type_from_origin_),
      obj_type_(other.obj_type_),
      user_type_id_(other.user_type_id_),
      not_null_(other.not_null_),
      pls_type_(other.pls_type_)
  {
    type_info_ = other.type_info_;
  }

  virtual ~ObPLDataType() {}
  int deep_copy(common::ObIAllocator &alloc, const ObPLDataType &other);
  void reset()
  {
    type_ = PL_INVALID_TYPE;
    type_from_ = PL_TYPE_LOCAL;
    type_from_origin_ = PL_TYPE_LOCAL;
    obj_type_ = ObDataType();
    user_type_id_ = common::OB_INVALID_ID;
    not_null_ = false;
    pls_type_ = ObPLIntegerType::PL_INTEGER_INVALID;
    type_info_.reset();
  }

  bool operator==(const ObPLDataType &other) const;

  inline ObPLType get_type() const { return type_; }
  inline void set_type(ObPLType type) { type_ = type; }

  inline ObPLTypeFrom get_type_from() const { return type_from_; }
  inline void set_type_from(ObPLTypeFrom type_from) { type_from_ = type_from; }
  inline void set_type_from_orgin(ObPLTypeFrom type_from_origin)
  {
    type_from_origin_ = type_from_origin;
  }

  const common::ObDataType *get_data_type() const { return is_obj_type() ? &obj_type_ : NULL; }
  common::ObDataType *get_data_type() { return is_obj_type() ? &obj_type_ : NULL; }
  const common::ObObjMeta *get_meta_type() const { return  is_obj_type() ? &(obj_type_.meta_) : NULL; }
  void set_data_type(const common::ObDataType &obj_type);

  common::ObObjType get_obj_type() const;
  ObPLIntegerType get_pl_integer_type() const { return pls_type_; }
  inline void set_pl_integer_type(ObPLIntegerType integer_type, const common::ObDataType &obj_type);
  inline void set_sys_refcursor_type();

  uint64_t get_user_type_id() const;
  inline void set_user_type_id(ObPLType type, uint64_t user_type_id);

  inline int64_t get_range() const { return user_type_id_; }
  inline int32_t get_lower() const { return lower_; }
  inline void set_lower(int32_t lower) { lower_ = lower; }
  inline int32_t get_upper() const { return upper_; }
  inline void set_upper(int32_t upper) { upper_ = upper; }
  inline void set_range(int32_t lower, int32_t upper)
  {
    lower_ = lower;
    upper_ = upper;
  }
  inline bool is_default_pls_range() const
  {
    return -2147483648 == lower_ && 2147483647 == upper_;
  }

  inline bool is_not_null() const { return not_null_; }
  inline bool get_not_null() const { return not_null_; }
  inline void set_not_null(bool not_null) { not_null_ = not_null; }

  const common::ObIArray<common::ObString>& get_type_info() const { return type_info_; }
  int set_type_info(const common::ObIArray<common::ObString> &type_info);
  int set_type_info(const common::ObIArray<common::ObString> *type_info);
  int deep_copy_type_info(common::ObIAllocator &allocator,
                          const common::ObIArray<common::ObString>& type_info);
  int get_external_user_type(const ObPLResolveCtx &resolve_ctx,
                          const ObUserDefinedType *&user_type) const;

  inline bool is_valid_type() const { return PL_INVALID_TYPE != type_; }
  inline bool is_obj_type() const { return PL_OBJ_TYPE == type_ || PL_INTEGER_TYPE == type_; }
  inline bool is_record_type() const { return PL_RECORD_TYPE == type_; }
  inline bool is_nested_table_type() const { return PL_NESTED_TABLE_TYPE == type_; }
  inline bool is_associative_array_type() const { return PL_ASSOCIATIVE_ARRAY_TYPE == type_; }
  inline bool is_varray_type() const { return PL_VARRAY_TYPE == type_; }
  inline bool is_cursor_type() const { return PL_CURSOR_TYPE == type_ || PL_REF_CURSOR_TYPE == type_; }
  inline bool is_opaque_type() const { return PL_OPAQUE_TYPE == type_; }
  inline bool is_sys_refcursor_type() const
  {
    return is_ref_cursor_type() && PL_TYPE_SYS_REFCURSOR == type_from_;
  }
  inline bool is_ref_cursor_type() const {
    return PL_REF_CURSOR_TYPE == type_;
  }
  inline bool is_cursor_var() const {
    return PL_CURSOR_TYPE == type_;
  }
  inline bool is_pl_integer_type() const { return PL_INTEGER_TYPE == type_; }
  inline bool is_subtype() const { return PL_SUBTYPE == type_; }

  inline bool is_local_type() const {
    return PL_TYPE_LOCAL == type_from_ ||
      (IS_TYPE_FROM_TYPE_OR_ROWTYPE(type_from_) ? PL_TYPE_LOCAL == type_from_origin_ : false);
  }
  inline bool is_package_type() const
  {
    return PL_TYPE_PACKAGE == type_from_ ||
      (IS_TYPE_FROM_TYPE_OR_ROWTYPE(type_from_) ? PL_TYPE_PACKAGE == type_from_origin_ : false);
  }
  inline bool is_udt_type() const
  {
    return PL_TYPE_UDT == type_from_ ||
      (IS_TYPE_FROM_TYPE_OR_ROWTYPE(type_from_) ? PL_TYPE_UDT == type_from_origin_ : false);
  }
  inline bool is_rowtype_type() const { return PL_TYPE_ATTR_ROWTYPE == type_from_; }
  inline bool is_type_type() const { return PL_TYPE_ATTR_TYPE == type_from_; }

  inline bool is_collection_type() const
  {
    return is_nested_table_type() || is_associative_array_type() || is_varray_type();
  }
  inline bool is_user_type() const
  {
    return is_record_type()
          || is_collection_type()
          || is_cursor_type()
          || is_subtype()
          || is_opaque_type();
  }
  inline bool is_boolean_type() const
  {
    return PL_OBJ_TYPE == type_ && obj_type_.get_meta_type().is_tinyint();
  }
  inline bool is_composite_type() const
  {
    return is_record_type() || is_collection_type() || is_subtype() || is_opaque_type();
  }
  //eg: create or replace type a as object (a number);
  inline bool is_object_type() const {
    return (is_record_type() || is_opaque_type()) && is_udt_type();
  }
  //存储过程内部通过type定义的record
  inline bool is_type_record() const {
    return is_record_type() && !is_udt_type();
  }
  inline bool is_lob_type() const {
    return PL_OBJ_TYPE == type_ && obj_type_.get_meta_type().is_lob_locator();
  }
  inline bool is_lob_storage_type() const {
    return PL_OBJ_TYPE == type_ && obj_type_.get_meta_type().is_lob_storage();
  }
  inline bool is_long_type() const {
    return PL_OBJ_TYPE == type_ && ObVarcharType == obj_type_.get_meta_type().get_type()
          && obj_type_.get_meta_type().is_cs_collation_free();
  }
  inline bool is_real_type() const {
    return PL_OBJ_TYPE == type_ && obj_type_.get_meta_type().is_number_float()
         && CS_TYPE_BINARY == obj_type_.get_meta_type().get_collation_type();
  }
  inline bool is_urowid_type() const {
    return PL_OBJ_TYPE == type_ && obj_type_.get_meta_type().is_urowid();
  }

  inline void set_generic_type(ObPLGenericType generic_type) { generic_type_ = generic_type; }
  inline bool is_generic_type() const
  {
    return !is_obj_type() && generic_type_ != PL_GENERIC_INVALID;
  }
  inline ObPLGenericType get_generic_type() const { return generic_type_; }
  inline bool is_generic_adt_type() const { return PL_ADT_1 == generic_type_; }
  inline bool is_generic_record_type() const { return PL_RECORD_1 == generic_type_; }
  inline bool is_generic_varray_type() const { return PL_VARRAY_1 == generic_type_; }
  inline bool is_generic_v2_table_type() const { return PL_V2_TABLE_1 == generic_type_; }
  inline bool is_generic_table_type() const { return PL_TABLE_1 == generic_type_; }
  inline bool is_generic_collection_type() const { return PL_COLLECTION_1 == generic_type_; }
  inline bool is_generic_ref_cursor_type() const { return PL_REF_CURSOR_1 == generic_type_; }

  /*!
   * ------ new session serialize/deserialize interface -------
   */
  // for session module interface
  int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, common::ObObj &obj, int64_t &size) const;
  int serialize(
    const ObPLResolveCtx &resolve_ctx, common::ObObj &obj, common::ObObj &result) const;
  int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, common::ObObj &result) const;

  // for PL type interface
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;
  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;
  // ------ new session serialize/deserialize interface -------


  //LLVM里存储的类型
  static int get_llvm_type(common::ObObjType obj_type, jit::ObLLVMHelper& helper, ObPLADTService &adt_service, jit::ObLLVMType &type);
  //sql里存储的类型
  static int get_datum_type(common::ObObjType obj_type, jit::ObLLVMHelper& helper, ObPLADTService &adt_service, jit::ObLLVMType &type);

  virtual int generate_assign_with_null(ObPLCodeGenerator &generator,
                                        const ObPLBlockNS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &dest) const;
  virtual int generate_default_value(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     const pl::ObPLStmt *stmt,
                                     jit::ObLLVMValue &value) const;

  virtual int generate_copy(ObPLCodeGenerator &generator,
                            const ObPLBlockNS &ns,
                            jit::ObLLVMValue &allocator,
                            jit::ObLLVMValue &src,
                            jit::ObLLVMValue &dest,
                            bool in_notfound,
                            bool in_warning,
                            uint64_t package_id = OB_INVALID_ID) const;
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int generate_new(ObPLCodeGenerator &generator,
                                       const ObPLINS &ns,
                                       jit::ObLLVMValue &value,
                                       const pl::ObPLStmt *stmt = NULL) const;
  virtual int newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const;
  virtual int get_size(const ObPLINS& ns, ObPLTypeSize type, int64_t &size) const;
  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const;
  virtual int free_data(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const;
  virtual int add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                               const ObPLBlockNS &block_ns,
                                               const common::ObString &package_name,
                                               const common::ObString &param_name,
                                               int64_t mode,
                                               int64_t position, int64_t level, int64_t &sequence,
                                               share::schema::ObRoutineInfo &routine_info) const;
  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;
  int get_field_count(const ObPLINS& ns, int64_t &count) const;

  int serialize(share::schema::ObSchemaGetterGuard &schema_guard, const common::ObTimeZoneInfo *tz_info,
                obmysql::MYSQL_PROTOCOL_TYPE type, char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  int deserialize(share::schema::ObSchemaGetterGuard &schema_guard, common::ObIAllocator &allocator,
                  const common::ObCharsetType charset, const common::ObCollationType cs_type,
                  const common::ObCollationType ncs_type, const common::ObTimeZoneInfo *tz_info,
                  const char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;

  int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  static int get_udt_type_by_name(uint64_t tenant_id,
                                  uint64_t owner_id,
                                  const common::ObString &udt,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  ObPLDataType &pl_type,
                                  share::schema::ObSchemaObjVersion *obj_version);
#ifdef OB_BUILD_ORACLE_PL
  static int get_pkg_type_by_name(uint64_t tenant_id,
                                  uint64_t owner_id,
                                  const common::ObString &pkg,
                                  const common::ObString &type,
                                  common::ObIAllocator &allocator,
                                  sql::ObSQLSessionInfo &session_info,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  common::ObMySQLProxy &sql_proxy,
                                  bool is_pkg_var, // pkg var or pkg type
                                  ObPLDataType &pl_type,
                                  share::schema::ObSchemaObjVersion *obj_version);
#endif
  static int get_table_type_by_name(uint64_t tenant_id,
                                  uint64_t owner_id,
                                  const ObString &table,
                                  const ObString &type,
                                  common::ObIAllocator &allocator,
                                  sql::ObSQLSessionInfo &session_info,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  bool is_rowtype,
                                  ObPLDataType &pl_type,
                                  share::schema::ObSchemaObjVersion *obj_version);
  static int transform_from_iparam(const share::schema::ObRoutineParam *iparam,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  sql::ObSQLSessionInfo &session_info,
                                  common::ObIAllocator &allocator,
                                  common::ObMySQLProxy &sql_proxy,
                                  pl::ObPLDataType &pl_type,
                                  share::schema::ObSchemaObjVersion *obj_version = NULL,
                                  pl::ObPLDbLinkGuard *dblink_guard = NULL);
  static int transform_and_add_routine_param(const pl::ObPLRoutineParam *param,
                                  int64_t position,
                                  int64_t level,
                                  int64_t &sequence,
                                  share::schema::ObRoutineInfo &routine_info);
  static int deep_copy_pl_type(ObIAllocator &allocator, const ObPLDataType &src, ObPLDataType *&dst);

  DECLARE_TO_STRING;

protected:
  ObPLType type_;
  ObPLTypeFrom type_from_;
  ObPLTypeFrom type_from_origin_; /* valid if type_from is PL_TYPE_ATTR_ROWTYPE or PL_TYPE_ATTR_TYPE */
  common::ObDataType obj_type_;
  union {
    uint64_t user_type_id_;
    struct {
      int32_t lower_;
      int32_t upper_;
    };
  };
  bool not_null_;
  union {
    ObPLIntegerType pls_type_;
    ObPLGenericType generic_type_;
  };
  common::ObArray<common::ObString> type_info_;
};

inline void ObPLDataType::set_pl_integer_type(ObPLIntegerType integer_type, const common::ObDataType &obj_type)
{
  set_data_type(obj_type);
  type_ = PL_INTEGER_TYPE;
  pls_type_ = integer_type;
}

inline void ObPLDataType::set_sys_refcursor_type()
{
  type_from_ = PL_TYPE_SYS_REFCURSOR;
  set_user_type_id(PL_REF_CURSOR_TYPE, common::combine_pl_type_id(OB_INVALID_ID, 0));
}

inline void ObPLDataType::set_user_type_id(ObPLType type, uint64_t user_type_id)
{
  type_ = type;
  user_type_id_ = user_type_id;
}

class ObObjAccessIdx
{
public:
  enum AccessType //必须与enum ExternalType的定义保持一致
  {
    IS_INVALID = -1,
    IS_LOCAL = 0,      //本地变量：PL内部定义的变量
    IS_DB_NS = 1,          //外部变量：包变量所属的DB
    IS_PKG_NS = 2,         //外部变量：包变量所属的PKG
    IS_PKG = 3,            //外部变量：包变量
    IS_USER = 4,           //外部变量：用户变量
    IS_SESSION = 5,        //外部变量：SESSION系统变量
    IS_GLOBAL = 6,         //外部变量：GLOBAL系统变量
    IS_TABLE_NS = 7,       //外部变量: 用户表,用于实现 %TYPE, %ROWTYPE
    IS_TABLE_COL = 8,      //外部变量: 用户列,用于实现 %TYPE
    IS_LABEL_NS = 9,       //Label
    IS_SUBPROGRAM_VAR = 10, //Subprogram Var
    IS_EXPR = 11,           //for table type access index
    IS_CONST = 12,          //常量 special case for is_expr
    IS_PROPERTY = 13,       //固有属性，如count
    IS_INTERNAL_PROC = 14,  //Package中的Procedure
    IS_EXTERNAL_PROC = 15, //Standalone的Procedure
    IS_NESTED_PROC = 16,
    IS_TYPE_METHOD = 17,    //自定义类型的方法
    IS_SYSTEM_PROC = 18,    //系统中已经预定义的Procedure(如: RAISE_APPLICATION_ERROR)
    IS_UDT_NS = 19,
    IS_UDF_NS = 20,
    IS_LOCAL_TYPE = 21,     // 本地的自定义类型
    IS_PKG_TYPE = 22,       // 包中的自定义类型
    IS_SELF_ATTRIBUTE = 23, // self attribute for udt
    IS_DBLINK_PKG_NS = 24,  // dblink package
  };

  ObObjAccessIdx()
    : elem_type_(),
      access_type_(IS_INVALID),
      var_name_(),
      var_type_(),
      var_index_(common::OB_INVALID_INDEX),
      routine_info_(NULL),
      get_sysfunc_(NULL) {}
  ObObjAccessIdx(const ObPLDataType &elem_type,
                 AccessType access_type,
                 const common::ObString &var_name,
                 const ObPLDataType &var_type,
                 int64_t value = 0
                 );
  //不实现析构函数，避免LLVM映射麻烦

  int deep_copy(common::ObIAllocator &allocator, sql::ObRawExprFactory &expr_factory, const ObObjAccessIdx &src);
  void reset();
  bool operator==(const ObObjAccessIdx &other) const;

  TO_STRING_KV(K_(access_type),
               K_(var_name),
               K_(var_type),
               K_(var_index),
               K_(elem_type),
               K_(type_method_params),
               KP_(get_sysfunc));
public:
  bool is_invalid() const { return IS_INVALID == access_type_; }
  bool is_local() const { return IS_LOCAL == access_type_; }
  bool is_pkg() const { return IS_PKG == access_type_; }
  bool is_subprogram_var() const { return IS_SUBPROGRAM_VAR == access_type_; }
  bool is_user_var() const { return IS_USER == access_type_; }
  bool is_session_var() const { return IS_SESSION == access_type_ || IS_GLOBAL == access_type_; }
  bool is_ns() const { return IS_DB_NS == access_type_ || IS_PKG_NS == access_type_ || IS_UDT_NS == access_type_; }
  bool is_const() const { return IS_CONST == access_type_; }
  bool is_property() const { return IS_PROPERTY == access_type_; }
  bool is_external() const
  {
    return IS_PKG == access_type_
            || IS_SUBPROGRAM_VAR == access_type_
            || IS_USER == access_type_
            || IS_SESSION == access_type_
            || IS_GLOBAL == access_type_;
  }
  bool is_type_method() const { return IS_TYPE_METHOD == access_type_; }
  bool is_internal_procedure() const { return IS_INTERNAL_PROC == access_type_; }
  bool is_external_procedure() const { return IS_EXTERNAL_PROC == access_type_; }
  bool is_system_procedure() const { return IS_SYSTEM_PROC == access_type_; }
  bool is_nested_procedure() const { return IS_NESTED_PROC == access_type_; }
  bool is_procedure() const { return is_internal_procedure() || is_external_procedure() || is_nested_procedure(); }
  bool is_variable() const { return IS_LOCAL == access_type_ || IS_PKG == access_type_; }
  bool is_table() const { return IS_TABLE_NS == access_type_; }
  bool is_table_col() const { return IS_TABLE_COL == access_type_; }
  bool is_expr() const { return IS_EXPR == access_type_; }
  bool is_label() const { return IS_LABEL_NS == access_type_; }
  bool is_local_type() const { return IS_LOCAL_TYPE == access_type_; }
  bool is_pkg_type() const { return IS_PKG_TYPE == access_type_; }
  bool is_udt_type() const { return IS_UDT_NS == access_type_; }
  bool is_udf_type() const { return IS_UDF_NS == access_type_; }

  static bool is_table(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_table_column(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_local_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_function_return_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_subprogram_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_package_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_local_baisc_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_local_refcursor_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_local_cursor_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_package_cursor_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_subprogram_basic_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_subprogram_cursor_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_subprogram_baisc_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_package_baisc_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_get_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_local_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_pkg_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_udt_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_external_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static const ObPLDataType &get_final_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static int get_package_id(const ObIArray<ObObjAccessIdx> &access_idxs,
                            uint64_t &package_id, uint64_t &var_idx);
  static int get_package_id(const sql::ObRawExpr *expr,
                            uint64_t& package_id, uint64_t *p_var_idx = NULL);
  static bool has_collection_access(const sql::ObRawExpr *expr);
  static int datum_need_copy(const sql::ObRawExpr *into, const sql::ObRawExpr *value, AccessType &alloc_scop);
  static int64_t get_local_variable_idx(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static int64_t get_subprogram_idx(const common::ObIArray<ObObjAccessIdx> &access_idxs);
  static bool is_contain_object_type(const common::ObIArray<ObObjAccessIdx> &access_idxs);

public:
  ObPLDataType elem_type_; //通过本变量访问得到的struct结构类型
  AccessType access_type_;
  common::ObString var_name_;
  ObPLDataType var_type_; //本变量本身的数据结构，大多时候和elem_type_是一样的。当通过变量作为下标访问数据的时候不同。
  union {
    int64_t var_index_; //注意：ObObjAccessIdx在不同的地方，本变量的含义不同：在PL里代表的是该变量在全局符号表里的下标；在ObObjAccessRawExpr里代表的是该变量在var_indexs_和param_exprs里的下标
    const ObPLBlockNS *label_ns_; //当AccessType是LABEL_NS时,这里记录的是Label对应的NameSpace
  };
  union {
    share::schema::ObIRoutineInfo *routine_info_;
    const ObPLBlockNS *var_ns_; //当AccessType是SUBPROGRAM_VAR时,这里记录的是VAR对应的NS
  };
  common::ObSEArray<int64_t, 4> type_method_params_;
  sql::ObRawExpr *get_sysfunc_; //user/session/pkg var or table index expr
};

enum ObPLCursorFlag {
  CURSOR_FLAG_UNDEF = 0,
  REF_BY_REFCURSOR = 1, // this a ref cursor
  SESSION_CURSOR = 2, // this cursor is alloc in session memory
  TRANSFERING_RESOURCE = 4, // this cursor is returned by a udf
  SYNC_CURSOR = 8, // this cursor from package cursor sync, can not used by this server.
  INVALID_CURSOR = 16, // this cursor is convert to a dbms cursor, invalid for dynamic cursor op.
};
class ObPLCursorInfo
{
public:
  ObPLCursorInfo(bool is_explicit = true) :
    id_(OB_INVALID_ID),
    entity_(nullptr),
    is_explicit_(is_explicit),
    current_position_(OB_INVALID_ID),
    bulk_rowcount_(),
    bulk_exceptions_(),
    spi_cursor_(NULL),
    allocator_(NULL),
    cursor_flag_(CURSOR_FLAG_UNDEF),
    ref_count_(0),
    is_scrollable_(false),
    last_execute_time_(0),
    last_stream_cursor_(false)
  {
    reset();
  }
  ObPLCursorInfo(ObIAllocator *allocator) :
    id_(OB_INVALID_ID),
    entity_(nullptr),
    is_explicit_(true),
    current_position_(OB_INVALID_ID),
    bulk_rowcount_(),
    bulk_exceptions_(),
    spi_cursor_(NULL),
    allocator_(allocator),
    cursor_flag_(CURSOR_FLAG_UNDEF),
    ref_count_(0),
    is_scrollable_(false),
    snapshot_(),
    is_need_check_snapshot_(false),
    last_execute_time_(0),
    last_stream_cursor_(false)
  {
    reset();
  }
  virtual ~ObPLCursorInfo() { reset(); };
  struct ObCursorBulkException
  {
    ObCursorBulkException() :
      index_(-1), error_code_(-1) {}
    ObCursorBulkException(int64_t index, int64_t code_) :
      index_(index), error_code_(code_) {}
    TO_STRING_KV(K_(index), K_(error_code));
    int64_t index_;
    int64_t error_code_;
  };

  void reuse()
  {
    // reuse接口不充值id
    if (nullptr != entity_) {
      DESTROY_CONTEXT(entity_);
      entity_ = nullptr;
      spi_cursor_ = nullptr;
    }
//    is_explicit_ = true; //一个CURSOR的is_explicit_属性永远不会改变
    for_update_ = false;
    has_hidden_rowid_ = false;
    is_streaming_ = false;
    isopen_ = false;
    fetched_ = false;
    fetched_with_row_ = false;
    rowcount_ = 0;
    current_position_ = OB_INVALID_ID;
    in_forall_ = false;
    save_exception_ = false;
    forall_rollback_ = false;
    if (is_session_cursor()) {
      cursor_flag_ = SESSION_CURSOR;
    } else {
      cursor_flag_ = CURSOR_FLAG_UNDEF;
    }
    // ref_count_ = 0; // 这个不要清零，因为oracle在close之后，它的ref count还是保留的
    is_scrollable_ = false;
    last_execute_time_ = 0;
    trans_id_.reset();
    bulk_rowcount_.reset();
    bulk_exceptions_.reset();
    current_row_.reset();
    first_row_.reset();
    last_row_.reset();
    is_need_check_snapshot_ = false;
  }

  void reset()
  {
    id_ = OB_INVALID_ID;
    reuse();
  }

  int deep_copy(ObPLCursorInfo &src, common::ObIAllocator *allocator = NULL);

  inline void open(void *spi_cursor = NULL)
  {
    spi_cursor != NULL ? (void)NULL : reset();
    isopen_ = true;
    spi_cursor_ = spi_cursor;
    is_explicit_ = spi_cursor != NULL;
  }
  virtual int close(sql::ObSQLSessionInfo &session, bool is_reuse = false);

  inline void set_id(int64_t id) { id_ = id; }
  inline void set_entity(lib::MemoryContext entity) { entity_ = entity; }
  inline void set_fetched() { fetched_ = true; }
  inline void set_fetched_with_row(bool with_row) { fetched_with_row_ = with_row; }
  inline void set_spi_cursor(void *spi_cursor) { spi_cursor_ = spi_cursor; }
  inline void set_implicit() { is_explicit_ = false; }
  inline void set_for_update() { for_update_ = true; }
  inline void set_hidden_rowid() { has_hidden_rowid_ = true; }
  inline void set_streaming() { is_streaming_ = true; }
  inline void set_scrollable() { is_scrollable_ = true; }
  inline bool is_scrollable() { return is_scrollable_; }
  inline bool get_fetched() const { return fetched_; }
  inline bool get_fetched_with_row() const { return fetched_with_row_; }

  inline void set_in_forall(bool save_exception)
  {
    reset();
    in_forall_ = true;
    save_exception_ = save_exception;
  }
  inline void unset_in_forall()
  {
    // 清除掉in_forall标识, 不清除其他信息, 后续SQL%BULK_ROWCOUNT需要这些信息, 由下一个DML语句到达时清除
    in_forall_ = false;
  }
  inline int64_t get_id() const { return id_; }
  inline lib::MemoryContext &get_cursor_entity() { return entity_; }
  inline const lib::MemoryContext get_cursor_entity() const { return entity_; }
  inline bool get_in_forall() const { return in_forall_; }
  inline bool is_for_update() const { return for_update_; }
  inline bool has_hidden_rowid() const { return has_hidden_rowid_; }
  inline bool is_streaming() const { return is_streaming_; }

  inline bool isopen() const { return isopen_; }
  inline bool is_server_cursor() const { return get_id() != common::OB_INVALID_ID; }
  inline bool is_ps_cursor() const { return !((id_ & (1LL << 31)) != 0); }
  inline int64_t get_rowcount() const { return rowcount_; }
  inline int64_t get_current_position() { return current_position_; }
  inline const ObNewRow &get_current_row() const { return current_row_; }
  inline ObNewRow &get_current_row() { return current_row_; }
  inline const ObNewRow &get_first_row() const { return first_row_; }
  inline ObNewRow &get_first_row() { return first_row_; }
  inline const ObNewRow &get_last_row() const { return last_row_; }
  inline ObNewRow &get_last_row() { return last_row_; }
  inline sql::ObSPIResultSet* get_cursor_handler() const { return reinterpret_cast<sql::ObSPIResultSet*>(spi_cursor_); }
  inline sql::ObSPICursor* get_spi_cursor() const { return reinterpret_cast<sql::ObSPICursor*>(spi_cursor_); }

  inline bool get_isopen() const { return is_explicit_ ? isopen_ : false; }
  inline bool get_save_exception() const { return save_exception_; }

  inline void set_last_execute_time(int64_t last_execute_time) { last_execute_time_ = last_execute_time; }
  inline int64_t get_last_execute_time() const { return last_execute_time_; }

  void set_snapshot(const transaction::ObTxReadSnapshot &snapshot) { snapshot_ = snapshot; }
  transaction::ObTxReadSnapshot &get_snapshot() { return snapshot_; }

  void set_need_check_snapshot(bool is_need_check_snapshot) { is_need_check_snapshot_ = is_need_check_snapshot; }
  bool is_need_check_snapshot() { return is_need_check_snapshot_; }
  int set_and_register_snapshot(const transaction::ObTxReadSnapshot &snapshot);

  int set_current_position(int64_t position);
  int set_rowcount(int64_t rowcount);
  int set_rowid(ObString &rowid);
  int set_bulk_exception(int64_t error);

  int get_found(bool &found, bool &isnull) const ;
  int get_notfound(bool &notfound, bool &isnull) const ;
  int get_rowcount(int64_t &rowcount, bool &isnull) const;
  int get_rowid(ObString &rowid) const;

  int get_bulk_rowcount(int64_t index, int64_t &rowcount) const;
  int get_bulk_exception(int64_t index, bool need_code, int64_t &result) const;
  int64_t get_bulk_exception_count() const { return bulk_exceptions_.count(); }
  int64_t get_bulk_rowcount_count() const { return bulk_rowcount_.count(); }
  inline void reset_bulk_rowcount()
  {
    if (bulk_rowcount_.count() != 0) {
      bulk_rowcount_.reset();
    }
  }
  inline void clear_row_count() { rowcount_ = 0; }
  inline int add_bulk_row_count(int64_t row_count) { return bulk_rowcount_.push_back(row_count); }
  inline int add_bulk_exception(int64_t index, int64_t code) { return bulk_exceptions_.push_back(ObCursorBulkException(index, code)); }

  inline void set_forall_rollback() { forall_rollback_ = true; }
  inline bool is_forall_rollback() const { return forall_rollback_; }

  inline void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
  inline const transaction::ObTransID& get_trans_id() const { return trans_id_; }

  inline ObIAllocator *get_allocator() { return NULL == entity_ ? allocator_ : &entity_->get_arena_allocator(); }

  inline void set_ref_by_refcursor() { set_flag_bit(REF_BY_REFCURSOR); }
  inline bool is_ref_by_refcursor() const { return test_flag_bit(REF_BY_REFCURSOR); }

  inline void set_is_session_cursor() { set_flag_bit(SESSION_CURSOR); }
  inline bool is_session_cursor() const { return test_flag_bit(SESSION_CURSOR); }
  inline void inc_ref_count() { ref_count_ += 1; }
  inline void dec_ref_count() { ref_count_ -= 1; }
  inline int64_t get_ref_count() const { return ref_count_; }
  inline void set_ref_count(int64_t ref_cnt) { ref_count_ = ref_cnt; }
  inline void set_is_returning(bool flag) { flag ? set_flag_bit(TRANSFERING_RESOURCE)
                                                 : clear_flag_bit(TRANSFERING_RESOURCE); }
  inline bool get_is_returning() const { return test_flag_bit(TRANSFERING_RESOURCE); }

  inline void set_flag_bit(const ObPLCursorFlag &flag) { cursor_flag_ =
     static_cast<ObPLCursorFlag>(static_cast<uint64_t>(cursor_flag_) | static_cast<uint64_t>(flag)); }
  inline void clear_flag_bit(const ObPLCursorFlag &flag) { cursor_flag_ =
     static_cast<ObPLCursorFlag>(static_cast<uint64_t>(cursor_flag_) & ~static_cast<uint64_t>(flag)); }
  inline bool test_flag_bit(const ObPLCursorFlag &flag) const { return
     !!(static_cast<uint64_t>(cursor_flag_) & static_cast<uint64_t>(flag)); }

  inline void set_sync_cursor() { set_flag_bit(SYNC_CURSOR); }
  inline bool is_sync_cursor() { return test_flag_bit(SYNC_CURSOR); }

  inline void set_invalid_cursor() { set_flag_bit(INVALID_CURSOR); }
  inline bool is_invalid_cursor() { return test_flag_bit(INVALID_CURSOR); }

  static int prepare_entity(sql::ObSQLSessionInfo &session, 
                            lib::MemoryContext &entity);
  int prepare_spi_result(ObPLExecCtx *ctx, sql::ObSPIResultSet *&spi_result);
  int prepare_spi_cursor(sql::ObSPICursor *&spi_cursor,
                          uint64_t tenant_id,
                          uint64_t mem_limit,
                          bool is_local_for_update = false);

  TO_STRING_KV(K_(id),
               K_(is_explicit),
               K_(for_update),
               K_(has_hidden_rowid),
               K_(is_streaming),
               K_(isopen),
               K_(fetched),
               K_(fetched_with_row),
               K_(rowcount),
               K_(current_position),
               K_(first_row),
               K_(last_row),
               K_(in_forall),
               K_(save_exception),
               K_(forall_rollback),
               K_(trans_id),
               K_(bulk_rowcount),
               K_(bulk_exceptions),
               KP_(spi_cursor),
               K_(cursor_flag),
               K_(ref_count),
               K_(is_scrollable),
               K_(snapshot),
               K_(is_need_check_snapshot),
               K_(last_execute_time));

protected:
  int64_t id_;            // Cursor ID
  lib::MemoryContext entity_;
  bool is_explicit_;      // 是否是显式游标
  bool for_update_;    //是否可更新游标
  bool has_hidden_rowid_;    //是否包含隐藏ROWID属性
  bool is_streaming_;    //是否流式
  bool isopen_;          // 游标是否OPEN, 隐式游标利用该值表示PL中是否有DML执行过
  bool fetched_;          // 显示游标是否执行过fetch
  bool fetched_with_row_; // 显式游标表示上次执行fetch是否获得行
  int64_t rowcount_;      // 当前读取的行数
  int64_t current_position_; //当前行的位置，用于offset
  ObNewRow current_row_;    //当前行
  ObNewRow first_row_;    // 首行
  ObNewRow last_row_;     // 尾行
  bool in_forall_;        // 是否是forall中的隐式游标
  bool save_exception_;   // 在forall中是否需要遇到错误继续执行
  bool forall_rollback_;  // 是否Forall语句会退到了Single Sql mode
  transaction::ObTransID trans_id_; // used to check txn liveness when cursor is a 'for update cursor'
  common::ObArray<int64_t> bulk_rowcount_;
  common::ObArray<ObCursorBulkException> bulk_exceptions_;
  void *spi_cursor_; //Mysql模式读取结果的handler为sql::ObSPICursor(数据缓存)，Oracle模式是ObMySQLProxy::MySQLResult(流式读取)
  ObIAllocator *allocator_;
  ObPLCursorFlag cursor_flag_; // OBPLCURSORFLAG;
  int64_t ref_count_; // a ref cursor may referenced by many ref cursor
  bool is_scrollable_; // 是否是滚动游标
  transaction::ObTxReadSnapshot snapshot_;
  bool is_need_check_snapshot_;
  int64_t last_execute_time_; // 记录上一次cursor操作的时间点
  bool last_stream_cursor_; // cursor复用场景下，记录上一次是否是流式cursor
};

class ObPLGetCursorAttrInfo
{
public:
  ObPLGetCursorAttrInfo() :
    type_(PL_CURSOR_INVALID),
    bulk_rowcount_idx_(0),
    bulk_exceptions_idx_(0),
    bulk_exceptions_need_code_(false),
    is_explicit_(false) {}
  inline void set_type(int64_t type)
  {
    switch (type)
    {
#define SET_TYPE(type)        \
  case T_SP_CURSOR_##type: {  \
    type_ = PL_CURSOR_##type; \
    break;                    \
  }
      SET_TYPE(ISOPEN);
      SET_TYPE(FOUND);
      SET_TYPE(NOTFOUND);
      SET_TYPE(ROWCOUNT);
      SET_TYPE(ROWID);
      SET_TYPE(BULK_ROWCOUNT);
      SET_TYPE(BULK_EXCEPTIONS);
      SET_TYPE(BULK_EXCEPTIONS_COUNT);
      default: {
        type_ = PL_CURSOR_INVALID;
      }
#undef SET_TYPE
    }
  }
  inline bool is_valid() const { return type_ != PL_CURSOR_INVALID; }
  inline void set_is_explicit(bool is_explicit) { is_explicit_ = is_explicit; }
  inline void set_bulk_rowcount_idx(int64_t idx) { bulk_rowcount_idx_ = idx; }
  inline void set_bulk_exceptions_idx(int64_t idx) { bulk_exceptions_idx_ = idx; }
  inline void set_bulk_exceptions_code_or_idx(bool need_code) { bulk_exceptions_need_code_ = need_code; }

  inline bool is_explicit_cursor() const { return is_explicit_; }
  inline bool is_isopen() const { return PL_CURSOR_ISOPEN == type_; }
  inline bool is_found() const { return  PL_CURSOR_FOUND == type_; }
  inline bool is_notfound() const { return PL_CURSOR_NOTFOUND == type_; }
  inline bool is_rowcount() const { return PL_CURSOR_ROWCOUNT == type_; }
  inline bool is_rowid() const { return PL_CURSOR_ROWID == type_; }
  inline bool is_bulk_rowcount() const { return PL_CURSOR_BULK_ROWCOUNT == type_; }
  inline bool is_bulk_exceptions() const { return PL_CURSOR_BULK_EXCEPTIONS == type_; }
  inline bool is_bulk_exceptions_count() const { return PL_CURSOR_BULK_EXCEPTIONS_COUNT == type_; }

  inline int64_t get_type() const { return type_; }
  inline int64_t get_bulk_rowcount_idx() const { return bulk_rowcount_idx_; }
  inline int64_t get_bulk_exceptions_idx() const { return bulk_exceptions_idx_; }
  inline bool need_get_exception_code() const { return bulk_exceptions_need_code_; }
  inline bool need_get_exception_idx() const { return !bulk_exceptions_need_code_; }

  enum Type
  {
    PL_CURSOR_INVALID,
    PL_CURSOR_ISOPEN,
    PL_CURSOR_FOUND,
    PL_CURSOR_NOTFOUND,
    PL_CURSOR_ROWCOUNT,
    PL_CURSOR_ROWID,
    PL_CURSOR_BULK_ROWCOUNT,
    PL_CURSOR_BULK_EXCEPTIONS,
    PL_CURSOR_BULK_EXCEPTIONS_COUNT,
  };

  int64_t type_; // 获取的属性类型
  int64_t bulk_rowcount_idx_;
  int64_t bulk_exceptions_idx_;
  bool bulk_exceptions_need_code_;
  bool is_explicit_; // 是否是显式游标

  TO_STRING_KV(K_(type),
               K_(bulk_rowcount_idx), K_(bulk_exceptions_idx),
               K_(bulk_exceptions_need_code), K_(is_explicit));
};

}
}
#endif /* OCEANBASE_SRC_PL_OB_PL_TYPE_H_ */
