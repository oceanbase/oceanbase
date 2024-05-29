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

#ifndef DEV_SRC_PL_OB_PL_USER_TYPE_H_
#define DEV_SRC_PL_OB_PL_USER_TYPE_H_
#include "pl/ob_pl_type.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_array_index_hash_set.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/rc/ob_tenant_base.h"

#ifdef OB_BUILD_ORACLE_PL
#include "pl/opaque/ob_pl_opaque.h"
#include "pl/opaque/ob_pl_xml.h"
#include "pl/opaque/ob_pl_json_type.h"
#endif

namespace oceanbase
{
namespace sql
{
  class ObRawExprFactory;
  class ObRawExpr;
};
namespace common
{
class ObTimeZoneInfo;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace pl
{
struct ObPLExecCtx;
class ObPLResolveCtx;
class ObPLResolver;
class ObPLStmt;
class ObPLCollAllocator;

class ObUserDefinedType : public ObPLDataType
{
public:
  ObUserDefinedType() : ObPLDataType(), type_name_() {}
  ObUserDefinedType(ObPLType type) : ObPLDataType(type), type_name_() {}
  virtual ~ObUserDefinedType() {}

  int deep_copy(common::ObIAllocator &alloc, const ObUserDefinedType &other);
  void set_type(ObPLType type) { type_ = type; }
  ObPLType get_type() const { return type_; }
  inline void set_name(const common::ObString &type_name) { type_name_ = type_name; }
  inline const common::ObString &get_name() const { return type_name_; }
  void set_user_type_id(uint64_t user_type_id) { user_type_id_ = user_type_id; }
  inline uint64_t get_user_type_id() const { return user_type_id_; }

public:
  virtual int64_t get_member_count() const;
  virtual const ObPLDataType *get_member(int64_t i) const;
  virtual int generate_assign_with_null(
    ObPLCodeGenerator &generator, const ObPLINS &ns,
    jit::ObLLVMValue &allocator, jit::ObLLVMValue &dest) const;
  virtual int generate_default_value(
    ObPLCodeGenerator &generator,const ObPLINS &ns,
    const pl::ObPLStmt *stmt, jit::ObLLVMValue &value) const;
  virtual int generate_copy(ObPLCodeGenerator &generator,
                            const ObPLBlockNS &ns,
                            jit::ObLLVMValue &allocator,
                            jit::ObLLVMValue &src,
                            jit::ObLLVMValue &dest,
                            bool in_notfound,
                            bool in_warning,
                            uint64_t package_id = OB_INVALID_ID) const;
  virtual int generate_construct(ObPLCodeGenerator &generator, const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int generate_new(ObPLCodeGenerator &generator,
                                            const ObPLINS &ns,
                                            jit::ObLLVMValue &value,
                                            const pl::ObPLStmt *s = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                   const ObPLINS *ns,
                   int64_t &ptr) const;

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;
  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               common::ObObj &obj) const;
  virtual int free_data(
    const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const;

  // --------- for session serialize/deserialize interface ---------
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;
  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;

  virtual int add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                               const ObPLBlockNS &block_ns,
                                               const common::ObString &package_name,
                                               const common::ObString &param_name,
                                               int64_t mode, int64_t position,
                                               int64_t level, int64_t &sequence,
                                               share::schema::ObRoutineInfo &routine_info) const;
  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;

  virtual int init_obj(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       common::ObObj &obj,
                       int64_t &init_size) const;
  virtual int serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
                       char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       const common::ObCharsetType charset,
                       const common::ObCollationType cs_type,
                       const common::ObCollationType ncs_type,
                       const common::ObTimeZoneInfo *tz_info,
                       const char *&src,
                       char *dst,
                       const int64_t dst_len,
                       int64_t &dst_pos) const;

  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  static int deep_copy_obj(
    ObIAllocator &allocator, const ObObj &src, ObObj &dst, bool need_new_allocator = true, bool ignore_del_element = false);
  static int destruct_obj(ObObj &src, sql::ObSQLSessionInfo *session = NULL);
  static int alloc_sub_composite(ObObj &dest_element, ObIAllocator &allocator);
  static int alloc_for_second_level_composite(ObObj &src, ObIAllocator &allocator);
  static int serialize_obj(const ObObj &obj, char* buf, const int64_t len, int64_t& pos);
  static int deserialize_obj(ObObj &obj, const char* buf, const int64_t len, int64_t& pos);
  static int64_t get_serialize_obj_size(const ObObj &obj);

  int text_protocol_prefix_info_for_each_item(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const ObPLDataType &type,
                                              char *buf,
                                              const int64_t len,
                                              int64_t &pos) const;
  int text_protocol_suffix_info_for_each_item(const ObPLDataType &type,
                                              char *buf,
                                              const int64_t len,
                                              int64_t &pos,
                                              const bool is_last_item,
                                              const bool is_null) const;
  int text_protocol_base_type_convert(const ObPLDataType &type, char *buf, int64_t &pos, int64_t len) const;
  int base_type_serialize_for_text(ObObj* obj,
                                   const ObTimeZoneInfo *tz_info,
                                   char *dst,
                                   const int64_t dst_len,
                                   int64_t &dst_pos,
                                   bool &has_serialized) const;

  VIRTUAL_TO_STRING_KV(K_(type), K_(user_type_id), K_(type_name));
protected:
  common::ObString type_name_;
};

#ifdef OB_BUILD_ORACLE_PL
//---------- for ObUserDefinedSubType ----------

class ObUserDefinedSubType : public ObUserDefinedType
{
public:
  ObUserDefinedSubType()
    : ObUserDefinedType(PL_SUBTYPE), base_type_() {}
  virtual ~ObUserDefinedSubType() {}

  inline void set_base_type(ObPLDataType &base_type) { base_type_ = base_type; }
  inline const ObPLDataType* get_base_type() const { return &base_type_; }

  int deep_copy(common::ObIAllocator &alloc, const ObUserDefinedSubType &other);

public:
  virtual int64_t get_member_count() const { return 1; }
  virtual const ObPLDataType *get_member(int64_t i) const { return 0 == i ? &base_type_ : NULL; }
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
                                                const pl::ObPLStmt *s = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                   const ObPLINS *ns,
                   int64_t &ptr) const;

  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;
  virtual int serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
                       char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                          common::ObIAllocator &allocator,
                          const common::ObCharsetType charset,
                          const common::ObCollationType cs_type,
                          const common::ObCollationType ncs_type,
                          const common::ObTimeZoneInfo *tz_info,
                          const char *&src,
                          char *dst,
                          const int64_t dst_len,
                          int64_t &dst_pos) const;
  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(not_null),
               K_(lower),
               K_(upper),
               K_(base_type));

private:
  ObPLDataType base_type_;
};
#endif

//---------- for ObRefCursorType ----------

class ObRefCursorType : public ObUserDefinedType
{
public:
  ObRefCursorType()
    : ObUserDefinedType(PL_REF_CURSOR_TYPE),
      return_type_id_(OB_INVALID_ID)
    {}
  virtual ~ObRefCursorType() {}

  inline void set_return_type_id(uint64_t type_id) { return_type_id_ = type_id; }
  inline uint64_t get_return_type_id() const { return return_type_id_; }

  virtual int64_t get_member_count() const { return 0; }
  virtual const ObPLDataType *get_member(int64_t i) const { UNUSED(i); return NULL; }
  virtual int generate_assign_with_null(ObPLCodeGenerator &generator,
                                        ObPLINS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &dest) const
  { UNUSED(generator); UNUSED(ns), UNUSED(allocator); UNUSED(dest); return OB_SUCCESS;}
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              const pl::ObPLStmt *s = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                     const ObPLINS *ns,
                     int64_t &ptr) const;

public:
  int deep_copy(common::ObIAllocator &alloc, const ObRefCursorType &other);

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;

  virtual int init_obj(
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator, ObObj &obj, int64_t &init_size) const;

  virtual int init_session_var(
    const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator,
    sql::ObExecContext &exec_ctx, const sql::ObSqlExpression *default_expr,
    bool default_construct, common::ObObj &obj) const;

  virtual int free_session_var(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &obj_allocator, common::ObObj &obj) const;

  virtual int get_all_depended_user_type(
    const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS &current_ns) const
  {
    UNUSEDx(resolve_ctx, current_ns);
    return OB_SUCCESS;
  }

  static int deep_copy_cursor(
    common::ObIAllocator &allocator, const ObObj &src, ObObj &dest);

  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(return_type_id));

private:
  uint64_t return_type_id_;
};

//---------- for ObRecordType ----------

class ObRecordMember
{
public:
  ObRecordMember() : member_name_(),
                     member_type_(),
                     default_expr_(OB_INVALID_INDEX),
                     default_raw_expr_(NULL) {}

  ObRecordMember(const common::ObString &record_name,
                 const ObPLDataType &data_type,
                 int64_t default_expr,
                 sql::ObRawExpr* default_raw_expr)
    : member_name_(record_name),
      member_type_(data_type),
      default_expr_(default_expr),
      default_raw_expr_(default_raw_expr) {}
  ObRecordMember(const common::ObString &record_name)
    : member_name_(record_name),
      member_type_(),
      default_expr_(OB_INVALID_INDEX),
      default_raw_expr_(NULL) { }
  virtual ~ObRecordMember() {}

  uint64_t hash() const { return common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, member_name_, 0); }
  bool operator ==(const ObRecordMember &other) const
  {
    return common::ObCharset::case_insensitive_equal(member_name_, other.member_name_);
  }
  bool operator !=(const ObRecordMember &other) const { return !(operator ==(other)); }

  inline void set_default(int64_t idx) { default_expr_ = idx; }
  inline int64_t get_default() const { return default_expr_; }

  inline sql::ObRawExpr* get_default_expr() const { return default_raw_expr_; }

  // int deep_copy_default_expr(const ObRecordMember &member, ObIAllocator &allocator,
  //                            sql::ObRawExprFactory &expr_factory, bool deep_copy_expr = false);

  TO_STRING_KV(K_(member_name), K_(member_type), K_(default_expr), KP_(default_raw_expr));

  common::ObString member_name_;
  ObPLDataType member_type_;
  int64_t default_expr_;
  sql::ObRawExpr *default_raw_expr_;
};

class ObPLRecord;
class ObRecordType: public ObUserDefinedType
{
public:
  ObRecordType()
    : ObUserDefinedType(PL_RECORD_TYPE),
      record_members_()
    {}
  ObRecordType(ObPLType type)
    : ObUserDefinedType(type),
      record_members_()
    {}
  virtual ~ObRecordType() {}

  int deep_copy(
    common::ObIAllocator &alloc, const ObRecordType &other, bool shaow_copy = true);

  int add_record_member(
    const common::ObString &record_name, const ObPLDataType &record_type,
    int64_t default_idx = OB_INVALID_INDEX, sql::ObRawExpr *default_raw_expr = NULL);

  int add_record_member(const ObRecordMember &record);

  int extend_record_member(common::ObIAllocator *alloc);

  int64_t get_record_member_count() const { return record_members_.count(); }

  int get_record_member_type(const common::ObString &record_name, ObPLDataType *&record_type);

  int64_t get_record_member_index(const common::ObString &record_name) const;

  const ObPLDataType *get_record_member_type(int64_t index) const;

  const common::ObString *get_record_member_name(int64_t index) const;

  const ObRecordMember *get_record_member(int64_t index) const;

  int is_compatble(const ObRecordType &other, bool &is_comp) const;
  int record_members_init(common::ObIAllocator *alloc, int64_t size);
  void reset_record_member() { record_members_.reset(); }

  static int64_t get_notnull_offset();
  static int64_t get_meta_offset(int64_t count);
  static int64_t get_data_offset(int64_t count);
  static int64_t get_init_size(int64_t count);
public:
  virtual int64_t get_member_count() const { return record_members_.count(); }

  virtual const ObPLDataType *get_member(int64_t i) const { return get_record_member_type(i); }

  virtual int generate_assign_with_null(ObPLCodeGenerator &generator,
                                        const ObPLINS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &dest) const;

  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;

  virtual int generate_default_value(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     const pl::ObPLStmt *stmt,
                                     jit::ObLLVMValue &value) const;
  virtual int generate_new(ObPLCodeGenerator &generator,
                                                const ObPLINS &ns,
                                                jit::ObLLVMValue &value,
                                                const pl::ObPLStmt *s = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                     const ObPLINS *ns,
                     int64_t &ptr) const;

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;

  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const;
  virtual int free_data(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const;

  // --------- for session serialize/deserialize interface ---------
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;
  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;

  virtual int add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                               const ObPLBlockNS &block_ns,
                                               const common::ObString &package_name,
                                               const common::ObString &param_name,
                                               int64_t mode, int64_t position,
                                               int64_t level, int64_t &sequence,
                                               share::schema::ObRoutineInfo &routine_info) const;
  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;
  virtual int init_obj(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       common::ObObj &obj,
                       int64_t &init_size) const;
  virtual int serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
                       char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       const common::ObCharsetType charset,
                       const common::ObCollationType cs_type,
                       const common::ObCollationType ncs_type,
                       const common::ObTimeZoneInfo *tz_info,
                       const char *&src,
                       char *dst,
                       const int64_t dst_len,
                       int64_t &dst_pos) const;
  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  TO_STRING_KV(K_(type),
               K_(type_from),
               K_(user_type_id),
               K_(record_members));
private:
  static const int64_t MAX_RECORD_COUNT = 65536; // Compatible with Oracle
private:
  common::ObFixedArray<ObRecordMember, common::ObIAllocator> record_members_;
};

#ifdef OB_BUILD_ORACLE_PL
//---------- for ObOpaqueType ----------

class ObOpaqueType : public ObUserDefinedType
{
public:
  ObOpaqueType() : ObUserDefinedType(PL_OPAQUE_TYPE) {}
  virtual ~ObOpaqueType() {}

public:
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int64_t get_member_count() const { return 0; }
  virtual const ObPLDataType *get_member(int64_t i) const { UNUSED(i); return NULL; }

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;

  virtual int get_all_depended_user_type(
    const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS &current_ns) const
  {
    UNUSEDx(resolve_ctx, current_ns); return OB_SUCCESS;
  }
  virtual int newx(common::ObIAllocator &allocator,
                   const ObPLINS *ns,
                   int64_t &ptr) const;
  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const;
  virtual int generate_assign_with_null(ObPLCodeGenerator &generator,
                                        const ObPLINS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &dest) const;
};
#endif
//---------- for ObCollectionType ----------

class ObPLCollection;
class ObCollectionType : public ObUserDefinedType
{
public:
  enum PropertyType
  {
    INVALID_PROPERTY = -1,
    COUNT_PROPERTY,
    FIRST_PROPERTY,
    LAST_PROPERTY,
    LIMIT_PROPERTY,
    PRIOR_PROPERTY,
    NEXT_PROPERTY,
    EXISTS_PROPERTY,
  };

public:
  ObCollectionType(ObPLType type)
    : ObUserDefinedType(type),
      element_type_()
    {}
  virtual ~ObCollectionType() {}

  const ObPLDataType &get_element_type() const { return element_type_; }
  void set_element_type(const ObPLDataType &element_type) { element_type_ = element_type; }

  virtual int64_t get_member_count() const { return 1; }
  virtual const ObPLDataType *get_member(int64_t i) const { return 0 == i ? &element_type_ : NULL; }

  int deep_copy(common::ObIAllocator &alloc, const ObCollectionType &other);

  int get_init_size(int64_t &size) const;

public:
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int generate_assign_with_null(ObPLCodeGenerator &generator,
                                        const ObPLINS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &dest) const;
  virtual int generate_new(ObPLCodeGenerator &generator,
                           const ObPLINS &ns,
                           jit::ObLLVMValue &value,
                           const pl::ObPLStmt *s = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                     const ObPLINS *ns,
                     int64_t &ptr) const;

  virtual int get_size(ObPLTypeSize type, int64_t &size) const;

  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const;
  virtual int free_data(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const;

  // --------- for session serialize/deserialize interface ---------
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;
  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;

  virtual int add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                               const ObPLBlockNS &block_ns,
                                               const common::ObString &package_name,
                                               const common::ObString &param_name,
                                               int64_t mode, int64_t position,
                                               int64_t level, int64_t &sequence,
                                               share::schema::ObRoutineInfo &routine_info) const;
  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;
  virtual int init_obj(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       common::ObObj &obj,
                       int64_t &init_size) const;
  virtual int serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
                       char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       const common::ObCharsetType charset,
                       const common::ObCollationType cs_type,
                       const common::ObCollationType ncs_type,
                       const common::ObTimeZoneInfo *tz_info,
                       const char *&src,
                       char *dst,
                       const int64_t dst_len,
                       int64_t &dst_pos) const;
  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(element_type));
protected:
  ObPLDataType element_type_;
};

#ifdef OB_BUILD_ORACLE_PL
class ObNestedTableType: public ObCollectionType
{
public:
  ObNestedTableType()
    : ObCollectionType(PL_NESTED_TABLE_TYPE) {}
  virtual ~ObNestedTableType() {}
public:
  virtual int64_t get_member_count() const { return ObCollectionType::get_member_count(); }
  virtual const ObPLDataType *get_member(int64_t i) const { return ObCollectionType::get_member(i); }
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                       const ObPLINS *ns,
                       int64_t &ptr) const;
  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;
  virtual int free_session_var(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const;
  virtual int free_data(const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const;

  // --------- for session serialize/deserialize interface ---------
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;
  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;

  virtual int add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                               const ObPLBlockNS &block_ns,
                                               const common::ObString &package_name,
                                               const common::ObString &param_name,
                                               int64_t mode, int64_t position,
                                               int64_t level, int64_t &sequence,
                                               share::schema::ObRoutineInfo &routine_info) const;
  virtual int get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObPLBlockNS &current_ns) const;
  virtual int init_obj(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       common::ObObj &obj,
                       int64_t &init_size) const;
  virtual int serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
                       char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const;
  virtual int deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       const common::ObCharsetType charset,
                       const common::ObCollationType cs_type,
                       const common::ObCollationType ncs_type,
                       const common::ObTimeZoneInfo *tz_info,
                       const char *&src,
                       char *dst,
                       const int64_t dst_len,
                       int64_t &dst_pos) const;
  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(element_type),
               K_(not_null));
private:
};

class ObVArrayType: public ObCollectionType
{
public:
  ObVArrayType()
    : ObCollectionType(PL_VARRAY_TYPE),
      capacity_(OB_INVALID_SIZE) {}
  virtual ~ObVArrayType() {}

  int64_t get_capacity() const { return capacity_; }
  void set_capacity(int64_t size) { capacity_ = size; }

  int deep_copy(common::ObIAllocator &alloc, const ObVArrayType &other);

public:
  virtual int64_t get_member_count() const { return ObCollectionType::get_member_count(); }
  virtual const ObPLDataType *get_member(int64_t i) const { return ObCollectionType::get_member(i); }
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                       const ObPLINS *ns,
                       int64_t &ptr) const;

  virtual int init_session_var(
    const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator,
    sql::ObExecContext &exec_ctx, const sql::ObSqlExpression *default_expr,
    bool default_construct, common::ObObj &obj) const;

  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(element_type),
               K_(not_null),
               K_(capacity));
private:
  int64_t capacity_;
};

class ObAssocArrayType : public ObCollectionType
{
public:
  ObAssocArrayType()
    : ObCollectionType(PL_ASSOCIATIVE_ARRAY_TYPE),
      index_type_() {}
  virtual ~ObAssocArrayType() {}

  const ObPLDataType &get_index_type() const { return index_type_; }
  void set_index_type(const ObPLDataType &index_type) { index_type_ = index_type; }

  int deep_copy(common::ObIAllocator &alloc, const ObAssocArrayType &other);

public:
  virtual int64_t get_member_count() const { return ObCollectionType::get_member_count(); }
  virtual const ObPLDataType *get_member(int64_t i) const { return ObCollectionType::get_member(i); }
  virtual int generate_construct(ObPLCodeGenerator &generator,
                                 const ObPLINS &ns,
                                 jit::ObLLVMValue &value,
                                 const pl::ObPLStmt *stmt = NULL) const;
  virtual int newx(common::ObIAllocator &allocator,
                       const ObPLINS *ns,
                       int64_t &ptr) const;
  virtual int init_session_var(const ObPLResolveCtx &resolve_ctx,
                               common::ObIAllocator &obj_allocator,
                               sql::ObExecContext &exec_ctx,
                               const sql::ObSqlExpression *default_expr,
                               bool default_construct,
                               common::ObObj &obj) const;

  // --------- for session serialize/deserialize interface ---------
  virtual int get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const;

  virtual int serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const;

  virtual int deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const;

  virtual int convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const;

  TO_STRING_KV(K_(type),
               K_(user_type_id),
               K_(element_type),
               K_(not_null),
               K_(index_type));
private:
  ObPLDataType index_type_;
};
#endif

class ObPLComposite
{
public:
  ObPLComposite() : type_(PL_INVALID_TYPE), id_(OB_INVALID_ID), is_null_(false) {}
  ObPLComposite(ObPLType type, uint64_t id, bool is_null = false) : type_(type), id_(id), is_null_(is_null)  {}

  inline ObPLType get_type() const { return type_; }
  inline void set_type(ObPLType type) { type_ = type; }
  inline uint64_t get_id() const { return id_; }
  inline void set_id(uint64_t id) { id_ = id; }
  inline bool is_null() const { return is_null_; }
  inline void set_is_null(bool is_null) { is_null_ = is_null; }
  inline void set_null() { is_null_ = true; }
  inline bool is_record() const { return PL_RECORD_TYPE == type_; }
  inline bool is_nested_table() const { return PL_NESTED_TABLE_TYPE == type_; }
  inline bool is_associative_array() const { return PL_ASSOCIATIVE_ARRAY_TYPE == type_; }
  inline bool is_varray() const { return PL_VARRAY_TYPE == type_; }
  inline bool is_cursor() const { return PL_CURSOR_TYPE == type_; }
  inline bool is_collection() const { return is_nested_table() || is_associative_array() || is_varray(); }

  int assign(ObPLComposite *src, ObIAllocator *allocator);
  static int deep_copy(ObPLComposite &src,
                       ObPLComposite *&dest,
                       ObIAllocator &allocator,
                       const ObPLINS *ns,
                       sql::ObSQLSessionInfo *session,
                       bool need_new_allocator,
                       bool ignore_del_element = false);
  static int assign_element(ObObj &src, ObObj &dest, ObIAllocator &allocator);
  static int copy_element(const ObObj &src,
                          ObObj &dest,
                          ObIAllocator &allocator,
                          const ObPLINS *ns = NULL,
                          sql::ObSQLSessionInfo *session = NULL,
                          const ObDataType *dest_type = NULL,
                          bool need_new_allocator = true,
                          bool ignore_del_element = false);
  //NOTICE：不能实现为虚函数！！！
  int64_t get_init_size() const;
  int64_t get_serialize_size() const;
  int serialize(char *buf, int64_t len, int64_t &pos) const;
  int deserialize(const char* buf, const int64_t len, int64_t &pos);
  void print() const;
  static bool obj_is_null(ObObj* obj);

  TO_STRING_KV(K_(type), K_(id), K_(is_null));

protected:
  ObPLType type_;
  uint64_t id_;
  bool is_null_;
};

#define RECORD_META_OFFSET 4
#define IDX_RECORD_TYPE 0
#define IDX_RECORD_ID 1
#define IDX_RECORD_ISNULL 2
#define IDX_RECORD_COUNT 3
class ObPLRecord : public ObPLComposite
{
public:
  ObPLRecord() : ObPLComposite(PL_RECORD_TYPE, OB_INVALID_ID), count_(OB_INVALID_COUNT) {}
  ObPLRecord(uint64_t id, int32_t count) : ObPLComposite(PL_RECORD_TYPE, id), count_(count)
  {
    MEMSET(get_not_null(), 0, get_init_size() - ObRecordType::get_notnull_offset());
    for (int64_t i = 0; i < count_; ++i) {
      new (get_element() + i) ObObj();
    }
  }

  inline int32_t get_count() const { return count_; }
  inline void set_count(int32_t count) { count_ = count; }
  int get_not_null(int64_t i, bool &not_null);
  int get_element_type(int64_t i, ObDataType &type);
  bool *get_not_null()
  {
    return reinterpret_cast<bool*>((int64_t)this + ObRecordType::get_notnull_offset());
  }
  ObDataType *get_element_type()
  {
    return reinterpret_cast<ObDataType*>((int64_t)this + ObRecordType::get_meta_offset(get_count()));
  }
  ObObj *get_element()
  {
    return reinterpret_cast<ObObj*>((int64_t)this + ObRecordType::get_data_offset(get_count()));
  }

  int get_element(int64_t i, ObObj &obj) const;
  int get_element(int64_t i, ObObj *&obj);

  int assign(ObPLRecord *src, ObIAllocator *allocator);
  int deep_copy(ObPLRecord &src, ObIAllocator &allocator,
                const ObPLINS *ns = NULL, sql::ObSQLSessionInfo *session = NULL,
                bool ignore_del_element = false);

  int set_data(const ObIArray<ObObj> &row);
  int64_t get_init_size() const
  {
    return ObRecordType::get_data_offset(count_) + sizeof(ObObj) * count_;
  }
  inline bool is_inited() const { return count_ != OB_INVALID_COUNT; }
  void print() const;

  TO_STRING_KV(K_(type), K_(count), K(id_), K(is_null_));

private:
  int32_t count_; //field count
  //后面紧跟每个FIELD的类型和NOTNULL信息以及每个FIELD的数据，由CG动态生成
};


#define IDX_ELEMDESC_META 0
#define IDX_ELEMDESC_ACCURACY 1
#define IDX_ELEMDESC_CHARSET 2
#define IDX_ELEMDESC_IS_BINARY 3
#define IDX_ELEMDESC_IS_ZERO 4
#define IDX_ELEMDESC_TYPE 5
#define IDX_ELEMDESC_NOTNULL 6
#define IDX_ELEMDESC_FIELD_COUNT 7
class ObElemDesc : public common::ObDataType {
public:
  ObElemDesc() : type_(PL_INVALID_TYPE), not_null_(false), field_cnt_(0) {}
  //不定义析构函数

  inline ObPLType get_pl_type() const { return type_; }
  inline bool is_not_null() const { return not_null_; }
  inline int32_t get_field_count() const { return field_cnt_; }
  inline void set_pl_type(ObPLType type) { type_ = type; }
  inline void set_not_null(bool not_null) { not_null_ = not_null; }
  inline void set_field_count(int32_t cnt) { field_cnt_ = cnt; }
  inline void set_data_type(const ObDataType &type) { MEMCPY(this, &type, sizeof(ObDataType)); }

  inline bool is_obj_type() const { return PL_OBJ_TYPE == type_; }
  inline bool is_record_type() const { return PL_RECORD_TYPE == type_; }
  inline bool is_nested_table_type() const { return PL_NESTED_TABLE_TYPE == type_; }
  inline bool is_associative_array_type() const { return PL_ASSOCIATIVE_ARRAY_TYPE == type_; }
  inline bool is_varray_type() const { return PL_VARRAY_TYPE == type_; }

  inline bool is_opaque_type() const { return PL_OPAQUE_TYPE == type_; }
  inline bool is_collection_type() const
  {
    return is_nested_table_type() || is_associative_array_type() || is_varray_type();
  }
  inline bool is_composite_type() const { return meta_.is_ext(); }

  int64_t get_serialize_size() const;
  int serialize(char *buf, int64_t len, int64_t &pos) const;
  int deserialize(const char* buf, const int64_t len, int64_t &pos);

  TO_STRING_KV(K_(meta), K_(type), K_(not_null), K_(field_cnt));

public:
  ObPLType type_;
  bool not_null_;
  int32_t field_cnt_; //如果是Record描述列数，其他类型为1
};

// 这是为next，prior，exist准备的占位符，不会产生真实的读内存代码
#define IDX_COLLECTION_PLACEHOLD 10

#define IDX_COLLECTION_TYPE 0
#define IDX_COLLECTION_ID 1
#define IDX_COLLECTION_ISNULL 2
#define IDX_COLLECTION_ALLOCATOR 3
#define IDX_COLLECTION_ELEMENT 4
#define IDX_COLLECTION_COUNT 5
#define IDX_COLLECTION_FIRST 6
#define IDX_COLLECTION_LAST 7
#define IDX_COLLECTION_DATA 8
class ObPLCollection : public ObPLComposite
{
public:
  enum IndexRangeType
  {
    INVALID_RANGE_TYPE = -5,
    LARGE_THAN_LAST = -2,
    LESS_THAN_FIRST = -1,
  };

public:
  ObPLCollection(ObPLType type, uint64_t id)
    : ObPLComposite(type, id),
      allocator_(NULL),
      element_(),
      count_(OB_INVALID_COUNT),
      first_(OB_INVALID_INDEX),
      last_(OB_INVALID_INDEX),
      data_(NULL) {}
  common::ObIAllocator *get_coll_allocator();
  inline common::ObIAllocator *get_allocator() { return allocator_; }
  inline void set_allocator(common::ObIAllocator *allocator) { allocator_ = allocator; }
  inline const ObElemDesc &get_element_desc() const { return element_; }
  inline ObElemDesc &get_element_desc() { return element_; }
  inline void set_element_desc(const ObElemDesc &type) { element_ = type; }
  inline void set_element_type(const ObDataType &type) { static_cast<ObDataType&>(element_) = type; }
  inline const ObDataType &get_element_type() const { return element_; }
  inline int64_t get_count() const { return count_; }
  inline void set_count(int64_t count) { count_ = count; }
  inline int64_t get_column_count() const { return element_.field_cnt_; }
  inline void set_column_count(int64_t count) { element_.field_cnt_ = static_cast<int32_t>(count); }
  int64_t get_first();
  inline void set_first(int64_t first) { first_ = first; }
  int64_t get_last();
  inline void set_last(int64_t last) { last_ = last; }
  inline const ObObj *get_data() const { return data_; }
  inline void set_not_null(bool not_null) { element_.not_null_ = not_null; }
  inline bool is_not_null() const { return element_.not_null_; }
  inline void set_element_pl_type(ObPLType type) { element_.type_ = type; }
  inline ObObj *get_data() { return data_; }
  inline void set_data(ObObj* data) { data_ = data; }
  inline bool is_of_composite() { return element_.get_meta_type().is_ext(); }
  inline void set_inited() { count_ = 0; }
  inline bool is_inited() const { return count_ != -1; }
  inline bool is_collection_null() const
  {
    return PL_ASSOCIATIVE_ARRAY_TYPE == type_ ? false : !is_inited();
  }

  bool is_contain_null_val() const;
  int is_elem_deleted(int64_t index, bool &is_del) const;
  int delete_collection_elem(int64_t index);
  int update_first();
  int update_last();
  int update_first_impl();
  int update_last_impl();
  int64_t get_actual_count();
  static uint32_t allocator_offset_bits() { return offsetof(ObPLCollection, allocator_) * 8; }
  static uint32_t type_offset_bits() { return offsetof(ObPLCollection, type_) * 4; }
  static uint32_t element_offset_bits() { return offsetof(ObPLCollection, element_) * 8; }
  static uint32_t count_offset_bits() { return offsetof(ObPLCollection, count_) * 8; }
  static uint32_t first_offset_bits() { return offsetof(ObPLCollection, first_) * 8; }
  static uint32_t last_offset_bits() { return offsetof(ObPLCollection, last_) * 8; }
  static uint32_t data_offset_bits() { return offsetof(ObPLCollection, data_) * 8; }
  void print() const;
  int deep_copy(ObPLCollection *src, common::ObIAllocator *allocator, bool ignore_del_element = false);
  int assign(ObPLCollection *src, ObIAllocator *allocator);
  int64_t get_init_size() const
  {
    return sizeof(ObPLCollection);
  }
  int trim_collection_elem(int64_t trim_number);
  int shrink();
  int add_row(const ObIArray<ObObj> &row, bool deep_copy = false);
  int set_row(const ObIArray<ObObj> &row, int64_t idx, bool deep_copy = false);
  int set_row(const ObObj &row, int64_t idx, bool deep_copy = false);

  /*serialize functions*/
  int get_serialize_size(int64_t &size);
  int serialize(char* buf, const int64_t len, int64_t& pos);
  int deserialize(
    common::ObIAllocator &allocator, const char *buf, const int64_t len, int64_t &pos);

  int first(ObObj &result);
  int last(ObObj &result);
  int next(int64_t idx, ObObj &result);
  int prior(int64_t idx, ObObj &result);
  int exist(int64_t idx, ObObj &result);

  TO_STRING_KV(
    KP_(allocator), K_(type), K_(element), K_(count), K_(first), K_(last), K_(data));

protected:
  common::ObIAllocator *allocator_;
  ObElemDesc element_;
  int64_t count_; // -1: 当前Collection未初始化 其他: 当前Collection中的元素个数
  int64_t first_; //Collection首元素下标，因为用户可以访问该属性，所以从1开始
  int64_t last_; //Collection末元素下标
  ObObj *data_;
};

#ifdef OB_BUILD_ORACLE_PL
class ObPLNestedTable : public ObPLCollection
{
public:
  ObPLNestedTable()  : ObPLCollection(PL_NESTED_TABLE_TYPE, OB_INVALID_ID) { }
  ObPLNestedTable(uint64_t id)  : ObPLCollection(PL_NESTED_TABLE_TYPE, id) { }

  TO_STRING_KV(KP_(allocator), K_(type), K_(count), K_(first), K_(last), K_(data));
private:

};
#endif

#define IDX_ASSOCARRAY_KEY 9
#define IDX_ASSOCARRAY_SORT 10

#ifdef OB_BUILD_ORACLE_PL
class ObPLAssocArray : public ObPLCollection
{
public:
  ObPLAssocArray(uint64_t id)  : ObPLCollection(PL_ASSOCIATIVE_ARRAY_TYPE, id), key_(NULL), sort_(NULL) {}

  inline const common::ObObj *get_key() const { return key_; }
  inline common::ObObj *get_key() { return key_; }
  inline void set_key(common::ObObj* key) { key_ = key; }
  inline const common::ObObj *get_key(int64_t i) const { return i < 0 || i >= count_ ? NULL : &key_[i]; }
  inline common::ObObj *get_key(int64_t i) {  return i < 0 || i >= count_ ? NULL : &key_[i];  }
  inline int set_key(int64_t i, const common::ObObj &key)
  {
    int ret = OB_SUCCESS;
    if (i < 0 || i >= count_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      key_[i] = key;
    }
    return ret;
  }
  inline const int64_t *get_sort() const { return sort_; }
  inline int64_t *get_sort() { return sort_; }
  inline void set_sort(int64_t* sort) { sort_ = sort; }
  inline int64_t get_sort(int64_t i) const { return i < 0 || i >= count_ ? OB_INVALID_INDEX : sort_[i]; }
  inline int set_sort(int64_t i, int64_t sort)
  {
    int ret = OB_SUCCESS;
    if (i < 0 || i >= count_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      sort_[i] = sort;
    }
    return ret;
  }

  int deep_copy(ObPLCollection *src, ObIAllocator *allocator, bool ignore_del_element = false);
  int64_t get_init_size() const
  {
    return sizeof(ObPLAssocArray);
  }
  int update_first();
  int update_last();
  int64_t get_first();
  int64_t get_last();

  /*serialize functions*/
  int get_serialize_size(int64_t &size);
  int serialize(char* buf, const int64_t len, int64_t& pos);
  int deserialize(common::ObIAllocator &allocator,
                                  const char *buf, const int64_t len, int64_t &pos);

  static uint32_t key_offset_bits() { return offsetof(ObPLAssocArray, key_) * 8; }
  static uint32_t map_offset_bits() { return offsetof(ObPLAssocArray, sort_) * 8; }

  int first(ObObj &result);
  int last(ObObj &result);
  int next(int64_t idx, ObObj &result);
  int prior(int64_t idx, ObObj &result);
  int exist(int64_t idx, ObObj &result);

  TO_STRING_KV(KP_(allocator), K_(type), K_(count), K_(first), K_(last), KP_(data), KP_(key), KP_(sort));
private:
  common::ObObj *key_;
  int64_t *sort_; //每一个元素的sort属性存储的是排序在自己后面元素的下标，从0开始
};
#endif

#define IDX_VARRAY_CAPACITY 9
#ifdef OB_BUILD_ORACLE_PL
class ObPLVArray : public ObPLNestedTable
{
public:
  ObPLVArray(uint64_t id) : ObPLNestedTable(id), capacity_(OB_INVALID_SIZE) { set_type(PL_VARRAY_TYPE); }

  inline int64_t get_capacity() const { return capacity_; }
  inline void set_capacity(int64_t size) { capacity_ = size; }

  static uint32_t key_offset_bits() { return offsetof(ObPLVArray, capacity_) * 8; }

  int deep_copy(ObPLCollection *src, ObIAllocator *allocator, bool ignore_del_element = false);
  int64_t get_init_size() const
  {
    return sizeof(ObPLVArray);
  }

  /*serialize functions*/
  int get_serialize_size(int64_t &size);
  int serialize(char* buf, const int64_t len, int64_t& pos);
  int deserialize(common::ObIAllocator &allocator,
                                  const char *buf, const int64_t len, int64_t &pos);

  TO_STRING_KV(KP_(allocator), K_(type), K_(count), K_(first), K_(last), K_(data), K_(capacity));
private:
  int64_t capacity_;
};

#endif

}  // namespace pl
}  // namespace oceanbase
#endif /* DEV_SRC_PL_OB_PL_USER_TYPE_H_ */
