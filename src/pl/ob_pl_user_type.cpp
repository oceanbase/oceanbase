/**
 * 
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

#define USING_LOG_PREFIX PL

#include "observer/mysql/obsm_utils.h"
#include "pl/ob_pl_user_type.h"
#include "common/object/ob_object.h"
#include "common/ob_smart_call.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "pl/ob_pl_code_generator.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "pl/ob_pl_allocator.h"
#include "share/ob_lob_access_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace jit;
using namespace obmysql;
using namespace sql;

namespace pl
{
int64_t ObUserDefinedType::get_member_count() const
{
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

const ObPLDataType *ObUserDefinedType::get_member(int64_t i) const
{
  UNUSEDx(i);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return NULL;
}

int ObUserDefinedType::generate_assign_with_null(
  ObPLCodeGenerator &generator,
  const ObPLBlockNS &ns, jit::ObLLVMValue &allocator, jit::ObLLVMValue &dest) const
{
  UNUSEDx(generator, ns, allocator, dest); return OB_SUCCESS;
}

int ObUserDefinedType::generate_default_value(
  ObPLCodeGenerator &generator,
  const ObPLINS &ns, const pl::ObPLStmt *stmt, jit::ObLLVMValue &value) const
{
  UNUSEDx(generator, ns, stmt, value); return OB_SUCCESS;
}

int ObUserDefinedType::generate_copy(
  ObPLCodeGenerator &generator, const ObPLBlockNS &ns,
  jit::ObLLVMValue &allocator, jit::ObLLVMValue &src, jit::ObLLVMValue &dest,
  bool in_notfound, bool in_warning, uint64_t package_id) const
{
  UNUSEDx(generator, ns, allocator, src, dest, in_notfound, in_warning, package_id);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::get_size(
  const ObPLINS &ns, ObPLTypeSize type, int64_t &size) const
{
  UNUSEDx(ns, type, size);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::init_session_var(
  const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator,
  sql::ObExecContext &exec_ctx, const sql::ObSqlExpression *default_expr, bool default_construct,
  common::ObObj &obj) const
{
  UNUSEDx(resolve_ctx, obj_allocator, exec_ctx, default_expr, default_construct, obj);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::free_session_var(
  const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &obj_allocator, common::ObObj &obj) const
{
  UNUSEDx(resolve_ctx, obj_allocator, obj);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::free_data(
  const ObPLResolveCtx &resolve_ctx, common::ObIAllocator &data_allocator, void *data) const
{
  UNUSEDx(resolve_ctx, data_allocator, data);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  UNUSEDx(resolve_ctx, src, size);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::serialize(
    const ObPLResolveCtx &resolve_ctx,
    char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const
{
  UNUSEDx(resolve_ctx, src, dst, dst_len, dst_pos);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::deserialize(
    const ObPLResolveCtx &resolve_ctx,
    common::ObIAllocator &allocator,
    const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  UNUSEDx(resolve_ctx, allocator, src, src_len, src_pos, dst);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::add_package_routine_schema_param(
  const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS &block_ns,
  const common::ObString &package_name, const common::ObString &param_name,
  int64_t mode, int64_t position, int64_t level, int64_t &sequence,
  share::schema::ObRoutineInfo &routine_info) const
{
  UNUSEDx(
    resolve_ctx, block_ns,package_name,
    param_name, mode, position, level, sequence, routine_info);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::get_all_depended_user_type(
  const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS &current_ns) const
{
  UNUSEDx(resolve_ctx, current_ns);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::init_obj(
  share::schema::ObSchemaGetterGuard &schema_guard, common::ObIAllocator &allocator,
  common::ObObj &obj, int64_t &init_size) const
{
  UNUSEDx(schema_guard, allocator, obj, init_size);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::serialize(
  share::schema::ObSchemaGetterGuard &schema_guard,
  const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
  char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const
{
  UNUSEDx(schema_guard, tz_info, type, src, dst, dst_len, dst_pos);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::deserialize(
  share::schema::ObSchemaGetterGuard &schema_guard, common::ObIAllocator &allocator,
  const common::ObCharsetType charset, const common::ObCollationType cs_type,
  const common::ObCollationType ncs_type, const common::ObTimeZoneInfo *tz_info,
  const char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const
{
  UNUSEDx(
    schema_guard, allocator, charset, cs_type, ncs_type, tz_info, src, dst, dst_len, dst_pos);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  UNUSEDx(ctx, src, dst);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::deep_copy(common::ObIAllocator &alloc, const ObUserDefinedType &other)
{
  int ret = OB_SUCCESS;
  OZ (ObPLDataType::deep_copy(alloc, other));
  OZ (ob_write_string(alloc, other.get_name(), type_name_));
  return ret;
}


int ObUserDefinedType::generate_new(ObPLCodeGenerator &generator,
                                          const ObPLINS &ns,
                                          jit::ObLLVMValue &value, //返回值是一个int64_t，代表extend的值
                                          const pl::ObPLStmt *s) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 3> args;
  ObLLVMValue var_idx, init_value, var_value;
  ObLLVMValue composite_value, extend_ptr;
  ObLLVMValue ret_err;
  ObLLVMValue var_type, type_id;
  ObLLVMType ptr_type;
  ObLLVMType ir_type;
  ObLLVMType ir_pointer_type;
  ObLLVMValue stack;
  int64_t init_size = 0;
  // Step 1: 初始化内存
  OZ (generator.get_helper().stack_save(stack));
  OZ (generator.get_helper().get_llvm_type(ObIntType, ptr_type));
  OZ (generator.get_helper().create_alloca("alloc_composite_addr", ptr_type, extend_ptr));
  OZ (args.push_back(generator.get_vars().at(generator.CTX_IDX)));
  OZ (generator.get_helper().get_int8(type_, var_type));
  OZ (args.push_back(var_type));
  OZ (generator.get_helper().get_int64(user_type_id_, type_id));
  OZ (args.push_back(type_id));
  OZ (generator.get_helper().get_int64(OB_INVALID_INDEX, var_idx));
  OZ (args.push_back(var_idx));
  OZ (ns.get_size(PL_TYPE_INIT_SIZE, *this, init_size));
  OZ (generator.get_helper().get_int32(init_size, init_value));
  OZ (args.push_back(init_value));
  OZ (args.push_back(extend_ptr));
  OZ (generator.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                         generator.get_spi_service().spi_alloc_complex_var_,
                                         args,
                                         ret_err));
  OZ (generator.check_success(ret_err,
                              s->get_stmt_id(),
                              s->get_block()->in_notfound(),
                              s->get_block()->in_warning()));

  // Step 2: 初始化类型内容, 如Collection的rowsize,element type等
  OZ (generator.get_helper().create_load("load_extend_ptr", extend_ptr, value));
  OZ (generator.get_llvm_type(*this, ir_type));
  OZ (ir_type.get_pointer_to(ir_pointer_type));
  OZ (generator.get_helper().create_int_to_ptr(ObString("ptr_to_user_type"), value, ir_pointer_type,
                                             composite_value));
  OZ (generate_construct(generator, ns, composite_value, s));
  OZ (generator.get_helper().stack_restore(stack));
  return ret;
}

int ObUserDefinedType::generate_construct(ObPLCodeGenerator &generator,
                                          const ObPLINS &ns,
                                          jit::ObLLVMValue &value,
                                          const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  UNUSED(ns);
  UNUSED(stmt);
  jit::ObLLVMType ir_type;
  jit::ObLLVMValue const_value;
  OZ (generator.get_llvm_type(*this, ir_type));
  OZ (jit::ObLLVMHelper::get_null_const(ir_type, const_value));
  OZ (generator.get_helper().create_store(const_value, value));
  return ret;
}

int ObUserDefinedType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(allocator, ns, ptr);
  LOG_WARN("Unexpected type to nex", K(ret));
  return ret;
}

int ObUserDefinedType::deep_copy_obj(
  ObIAllocator &allocator, const ObObj &src, ObObj &dst, bool need_new_allocator, bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  CK (src.is_pl_extend());

  if (OB_SUCC(ret)) {
    switch (src.get_meta().get_extend_type()) {
    case PL_CURSOR_TYPE:
    case PL_REF_CURSOR_TYPE: {
      OZ (ObRefCursorType::deep_copy_cursor(allocator, src, dst));
    }
      break;
    case PL_RECORD_TYPE: {
      OZ (ObPLComposite::copy_element(src, dst, allocator, NULL, NULL, NULL,  need_new_allocator, ignore_del_element));
    }
      break;

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to deep copy", K(src), K(ret));
    }
      break;
    }
  }
  return ret;
}

int ObUserDefinedType::destruct_obj(ObObj &src, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;

  if (src.is_pl_extend()) {
    switch (src.get_meta().get_extend_type()) {
    case PL_CURSOR_TYPE: {
      ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo*>(src.get_ext());
      CK (OB_NOT_NULL(cursor));
      CK (OB_NOT_NULL(session));
      OZ (cursor->close(*session));
      OX (cursor->~ObPLCursorInfo());
      OX (src.set_null());
    }
      break;
    case PL_RECORD_TYPE: {
      ObPLRecord *record = reinterpret_cast<ObPLRecord*>(src.get_ext());
      CK  (OB_NOT_NULL(record));
      for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
        OZ (destruct_obj(record->get_element()[i], session));
      }
      OX (src.set_null());
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to destruct", K(src), K(src.get_meta().get_extend_type()), K(ret));
    }
       break;
    }
  } else {
    //do nothing and return
  }
  return ret;
}

int ObUserDefinedType::serialize_obj(const ObObj &obj, char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  CK (obj.is_pl_extend());
  OZ (serialization::encode(buf, len, pos, GET_MIN_CLUSTER_VERSION()));
  OZ (serialization::encode(buf, len, pos, obj.get_meta().get_extend_type()));
  if (OB_SUCC(ret)) {
    switch (obj.get_meta().get_extend_type()) {
    case PL_RECORD_TYPE: {
      //todo:
      ret = OB_NOT_SUPPORTED;
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to serialize", K(obj), K(ret));
    }
      break;
    }
  }
  return ret;
}

int ObUserDefinedType::deserialize_obj(ObObj &obj, const char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = OB_INVALID_VERSION;
  uint8_t pl_type = PL_INVALID_TYPE;
  uint64_t id = OB_INVALID_ID;
  OZ (serialization::decode(buf, len, pos, version));
  OZ (serialization::decode(buf, len, pos, pl_type));
  OZ (serialization::decode(buf, len, pos, id));
  if (OB_SUCC(ret)) {
    switch (pl_type) {
    case PL_RECORD_TYPE: {
      //todo:
      ret = OB_NOT_SUPPORTED;
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to deserialize", K(obj), K(ret));
    }
      break;
    }
  }

  return ret;
}

int64_t ObUserDefinedType::get_serialize_obj_size(const ObObj &obj)
{
  int64_t size = 0;
  int ret = OB_SUCCESS;
  CK (obj.is_pl_extend());
  OX (size += serialization::encoded_length(GET_MIN_CLUSTER_VERSION()));
  OX (size += serialization::encoded_length(obj.get_meta().get_extend_type()));
  if (OB_SUCC(ret)) {
    switch (obj.get_meta().get_extend_type()) {
    case PL_RECORD_TYPE: {
      //todo:
      ret = OB_NOT_SUPPORTED;
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected type to get serialize size", K(obj), K(ret));
    }
      break;
    }
  }
  return size;
}


//---------- for ObRefCursorType ----------

int ObRefCursorType::deep_copy(common::ObIAllocator &alloc, const ObRefCursorType &other)
{
  int ret = OB_SUCCESS;
  OZ (ObUserDefinedType::deep_copy(alloc, other));
  OX (return_type_id_ = other.return_type_id_);
  return ret;
}

int ObRefCursorType::generate_construct(ObPLCodeGenerator &generator,
                                        const ObPLINS &ns,
                                        jit::ObLLVMValue &value,
                                        const pl::ObPLStmt *stmt) const
{
  UNUSEDx(generator, ns, value, stmt);
  return OB_NOT_SUPPORTED;
}

int ObRefCursorType::generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              const pl::ObPLStmt *s) const
{
  UNUSED(generator);
  UNUSED(ns);
  UNUSED(value);
  UNUSED(s);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObRefCursorType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(allocator, ns, ptr);
  return ret;
}

int ObRefCursorType::get_size(const ObPLINS &ns, ObPLTypeSize type, int64_t &size) const
{
  UNUSEDx(ns, type, size);
  size = sizeof(ObPLCursorInfo) + 8;
  return OB_SUCCESS;
}

int ObRefCursorType::init_obj(ObSchemaGetterGuard &schema_guard,
                              ObIAllocator &allocator,
                              ObObj &obj,
                              int64_t &init_size) const
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  init_size = 0;
  if (obj.is_ext()){
    data = reinterpret_cast<char *>(obj.get_ext());
  }
  if (OB_NOT_NULL(data)) {
    MEMSET(data, 0, init_size);
    new(data) ObPLCursorInfo(&allocator);
    obj.set_ext(reinterpret_cast<int64_t>(data));
  } else if (OB_FAIL(get_size(ObPLUDTNS(schema_guard), PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    new(data) ObPLCursorInfo(&allocator);
    obj.set_extend(reinterpret_cast<int64_t>(data), PL_CURSOR_TYPE);
  }
  return ret;
}

int ObRefCursorType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                      ObIAllocator &obj_allocator,
                                      sql::ObExecContext &exec_ctx,
                                      const sql::ObSqlExpression *default_expr,
                                      bool default_construct,
                                      ObObj &obj) const
{
  UNUSEDx(exec_ctx, default_expr, default_construct);
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t init_size = 0;
  if (OB_FAIL(get_size(resolve_ctx, PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(obj_allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    obj.set_extend(reinterpret_cast<int64_t>(data), PL_CURSOR_TYPE);
  }
  return ret;
}

int ObRefCursorType::free_session_var(const ObPLResolveCtx &resolve_ctx,
                                      ObIAllocator &obj_allocator,
                                      ObObj &obj) const
{
  UNUSED(resolve_ctx);
  char *data = NULL;
  data = reinterpret_cast<char *>(obj.get_ext());
  if (!OB_ISNULL(data)) {
    obj_allocator.free(data);
  }
  obj.set_null();
  return OB_SUCCESS;
}

int ObRefCursorType::deep_copy_cursor(common::ObIAllocator &allocator,
                                   const ObObj &src,
                                   ObObj &dest)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *src_cursor = NULL;
  ObPLCursorInfo *dest_cursor = NULL;
  if (0 == dest.get_ext()) {
    OZ (ObSPIService::spi_cursor_alloc(allocator, dest));
  }
  OX (src_cursor = reinterpret_cast<ObPLCursorInfo*>(src.get_ext()));
  OX (dest_cursor = reinterpret_cast<ObPLCursorInfo*>(dest.get_ext()));
  CK (OB_NOT_NULL(src_cursor));
  CK (OB_NOT_NULL(dest_cursor));
  OZ (dest_cursor->deep_copy(*src_cursor, &allocator));
  return ret;
}

//---------- for ObRecordType ----------

// int ObRecordMember::deep_copy_default_expr(const ObRecordMember &member,
//                                            ObIAllocator &allocator,
//                                            ObRawExprFactory &expr_factory,
//                                            bool deep_copy_expr)
// {
//   UNUSED(allocator);
//   int ret = OB_SUCCESS;
//   // first copy the default expr, later will check need deep copy
//   ObRawExpr *expr = member.get_default_expr();
//   if (OB_INVALID_INDEX == member.get_default() || OB_ISNULL(member.get_default_expr())) {
//     // do nothing
//   } else if (deep_copy_expr && ObPLExprCopier::copy_expr(expr_factory,
//                                                          member.get_default_expr(),
//                                                          expr)) {
//     LOG_WARN("copy raw expr failed", K(ret));
//   } else {
//     default_expr_ = 0;
//     default_raw_expr_ = expr;
//   }
//   return ret;
// }

//---------- for ObRecordType ----------

int ObRecordType::record_members_init(common::ObIAllocator *alloc, int64_t size)
{
  int ret = OB_SUCCESS;
  record_members_.set_allocator(alloc);
  if (OB_FAIL(record_members_.init(size))) {
    LOG_WARN("failed to init record_members_ count", K(ret));
  }

  return ret;
}

int ObRecordType::extend_record_member(common::ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(alloc));
  if (OB_SUCC(ret)) {
    if (0 == get_record_member_count()) {
      OZ (record_members_init(alloc, 1));
    } else {
      ObSEArray<ObRecordMember, 1> bak;
      for (int64_t i = 0; OB_SUCC(ret) && i < get_record_member_count(); ++i) {
        OZ (bak.push_back(*(get_record_member(i))));
      }
      OX (reset_record_member());
      OZ (record_members_init(alloc, 1 + bak.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < bak.count(); ++i) {
        OZ (add_record_member(bak.at(i)));
      }
    }
  }
  return ret;
}

int ObRecordType::add_record_member(const ObRecordMember &record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(record_members_.count() >= MAX_RECORD_COUNT)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("record member count is too many", K(record_members_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      if (common::ObCharset::case_insensitive_equal(
        record_members_.at(i).member_name_, record.member_name_)) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("dup record member found", K(ret), K(record.member_name_), K(i));
        break;
      }
    }
    OZ (record_members_.push_back(record));
  }
  return ret;
}

int ObRecordType::add_record_member(const ObString &record_name,
                                    const ObPLDataType &record_type,
                                    int64_t default_idx,
                                    sql::ObRawExpr *default_raw_expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(record_members_.count() >= MAX_RECORD_COUNT)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("record member count is too many", K(record_members_.count()));
  } else if (record_type.get_not_null() && OB_INVALID_INDEX == default_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("record member with not null modifier must hava default value", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      if (common::ObCharset::case_insensitive_equal(
        record_members_.at(i).member_name_, record_name)) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("dup record member found", K(ret), K(record_name), K(i));
        break;
      }
    }
    OZ (record_members_.push_back(ObRecordMember(
      record_name, record_type, default_idx, default_raw_expr)));
  }
  return ret;
}

int ObRecordType::get_record_member_type(const ObString &record_name,
                                         ObPLDataType *&record_type)
{
  int ret = OB_SUCCESS;
  record_type = NULL;
  if (OB_UNLIKELY(record_members_.count() <= 0)
      || OB_UNLIKELY(record_members_.count() > MAX_RECORD_COUNT)) {
    ret = OB_NOT_INIT;
    LOG_WARN("record type is not inited", K(record_members_.count()));
  } else {
    int64_t index = get_record_member_index(record_name);
    if (OB_UNLIKELY(index >= record_members_.count()) || OB_UNLIKELY(index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is invalid", K(index), K(record_members_.count()));
    } else {
      record_type = &(record_members_.at(index).member_type_);
    }
  }
  return ret;
}

int64_t ObRecordType::get_record_member_index(const ObString &record_name) const
{
  int64_t index = OB_INVALID_INDEX;
  for (int64_t i = 0; i < record_members_.count(); ++i) {
    if (common::ObCharset::case_insensitive_equal(
        record_members_.at(i).member_name_, record_name)) {
      index = i;
      break;
    }
  }
  return index;
}

const ObPLDataType *ObRecordType::get_record_member_type(int64_t index) const
{
  const ObPLDataType *type = NULL;
  if (OB_LIKELY(index >= 0) && OB_LIKELY(index < record_members_.count())) {
    type = &record_members_.at(index).member_type_;
  }
  return type;
}

const ObString *ObRecordType::get_record_member_name(int64_t index) const
{
  const ObString *type = NULL;
  if (OB_LIKELY(index >= 0) && OB_LIKELY(index < record_members_.count())) {
    type = &record_members_.at(index).member_name_;
  }
  return type;
}

const ObRecordMember *ObRecordType::get_record_member(int64_t index) const
{
  const ObRecordMember *record_member = NULL;
  if (OB_LIKELY(index >= 0) && OB_LIKELY(index < record_members_.count())) {
    record_member = &record_members_.at(index);
  }
  return record_member;
}

int ObRecordType::is_compatble(const ObRecordType &other, bool &is_comp) const
{
  int ret = OB_SUCCESS;
  is_comp = true;
  if (get_record_member_count() != other.get_record_member_count()) {
    is_comp = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_comp && i < get_record_member_count(); ++i) {
      const ObPLDataType *left = get_record_member_type(i);
      const ObPLDataType *right = other.get_record_member_type(i);
      CK (OB_NOT_NULL(left));
      CK (OB_NOT_NULL(right));
      if (OB_SUCC(ret)) {
        if (left->is_obj_type() && right->is_obj_type()) {
          CK (OB_NOT_NULL(left->get_data_type()));
          CK (OB_NOT_NULL(right->get_data_type()));
          OX (is_comp = cast_supported(left->get_data_type()->get_obj_type(),
                                      left->get_data_type()->get_collation_type(),
                                      right->get_data_type()->get_obj_type(),
                                      right->get_data_type()->get_collation_type()));
        } else if ((!left->is_obj_type() ||
                    (left->get_data_type() != NULL && left->get_data_type()->get_meta_type().is_ext()))
                      &&
                    (!right->is_obj_type() ||
                    (right->get_data_type() != NULL && right->get_data_type()->get_meta_type().is_ext()))) {
          uint64_t left_udt_id = (NULL == left->get_data_type()) ? left->get_user_type_id()
                                                                  : left->get_data_type()->get_udt_id();
          uint64_t right_udt_id = (NULL == right->get_data_type()) ? right->get_user_type_id()
                                                                    : right->get_data_type()->get_udt_id();
          if (left_udt_id != right_udt_id) {
            is_comp = false;
          }
        } else {
          is_comp = false;
        }
      }
    }
  }
  return ret;
}

int64_t ObRecordType::get_notnull_offset()
{
  return sizeof(ObPLRecord);
}

int64_t ObRecordType::get_meta_offset(int64_t count)
{
  return ObRecordType::get_notnull_offset() + 8 * ((count - 1) / 8 + 1); //notnull是bool，需要对齐
}

int64_t ObRecordType::get_data_offset(int64_t count)
{
  return ObRecordType::get_meta_offset(count) + sizeof(ObDataType) * count;
}

int64_t ObRecordType::get_init_size(int64_t count)
{
  return ObRecordType::get_data_offset(count) + sizeof(ObObj) * count;
}


int ObRecordType::deep_copy(
  common::ObIAllocator &alloc, const ObRecordType &other, bool shadow_copy)
{
  int ret = OB_SUCCESS;
  OZ (ObUserDefinedType::deep_copy(alloc, other));
  OZ (record_members_init(&alloc, other.get_record_member_count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < other.get_record_member_count(); i++) {
    const ObRecordMember *record_member = other.get_record_member(i);
    ObString new_member_name;
    OZ (ob_write_string(alloc, record_member->member_name_, new_member_name));
    OZ (add_record_member(new_member_name,
                          record_member->member_type_,
                          record_member->default_expr_,
                          shadow_copy ? record_member->default_raw_expr_ : NULL));
  } 
  return ret;
}

int ObRecordType::generate_assign_with_null(ObPLCodeGenerator &generator,
                                            const ObPLBlockNS &ns,
                                            jit::ObLLVMValue &allocator,
                                            jit::ObLLVMValue &dest) const
{
  /*
   * ORACLE 12.1 Document, Page 196:
   * Assigning the value NULL to a record variable assigns the value NULL to each of its fields.
   */
  int ret = OB_SUCCESS;
  ObLLVMValue isnull_ptr;
  ObLLVMValue dest_elem;
  ObLLVMValue llvm_null_obj;
  ObObj null_obj;
  null_obj.set_null();
  ObLLVMValue null_obj_ptr;
  const ObPLDataType *member_type = NULL;
  OZ (generator.generate_obj(null_obj, null_obj_ptr));
  OZ (generator.get_helper().create_load("load_null_obj", null_obj_ptr, llvm_null_obj));
  for (int64_t i = 0; OB_SUCC(ret) && i < get_record_member_count(); ++i) {
    dest_elem.reset();
    if (OB_FAIL(generator.extract_element_ptr_from_record(dest,
                                                          get_record_member_count(),
                                                          i,
                                                          dest_elem))) {
      LOG_WARN("failed to create gep", K(ret));
    } else if (OB_ISNULL(member_type = get_record_member_type(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get member type", K(ret));
    } else if (member_type->is_composite_type()) {
      ObLLVMValue extend;
      OZ (generator.extract_extend_from_obj(dest_elem, *member_type, extend));
      OZ (member_type->generate_assign_with_null(generator, ns, allocator, extend));
    } else if (OB_FAIL(generator.get_helper().create_store(llvm_null_obj, dest_elem))) {
      LOG_WARN("failed to create store", K(ret));
    } else {
    }
  }
  OZ (generator.extract_isnull_ptr_from_record(dest, isnull_ptr));
  OZ (generator.get_helper().create_istore(TRUE, isnull_ptr));
  return ret;
}

int ObRecordType::generate_construct(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  OZ (SMART_CALL(ObUserDefinedType::generate_construct(generator, ns, value, stmt)));
  OZ (SMART_CALL(generate_default_value(generator, ns, stmt, value)));
  return ret;
}

int ObRecordType::generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              const pl::ObPLStmt *s) const
{
  int ret = OB_NOT_SUPPORTED;
  ret = ObUserDefinedType::generate_new(generator, ns, value, s);
  return ret;
}


int ObRecordType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  ObPLRecord *record = NULL;
  ObObj *member = NULL;
  int64_t init_size = ObRecordType::get_init_size(get_member_count());
  OX (record = reinterpret_cast<ObPLRecord*>(allocator.alloc(init_size)));
  CK (OB_NOT_NULL(record));
  OX (new (record)ObPLRecord(user_type_id_, get_member_count()));
  OX (ptr = reinterpret_cast<int64_t>(record));
  for (int64_t i = 0; OB_SUCC(ret) && i < get_member_count(); ++i) {
    CK (OB_NOT_NULL(get_member(i)));
    OZ (record->get_element(i, member));
    CK (OB_NOT_NULL(member));
    if (get_member(i)->is_obj_type()) {
      OX (new (member) ObObj(ObNullType));
    } else {
      int64_t init_size = OB_INVALID_SIZE;
      int64_t member_ptr = 0;
      OZ (get_member(i)->get_size(*ns, PL_TYPE_INIT_SIZE, init_size));
      OZ (get_member(i)->newx(allocator, ns, member_ptr));
      OX (member->set_extend(member_ptr, get_member(i)->get_type(), init_size));
    }
  }
  return ret;
}

int ObRecordType::generate_default_value(ObPLCodeGenerator &generator,
                                         const ObPLINS &ns,
                                         const ObPLStmt *stmt,
                                         jit::ObLLVMValue &value) const
{
  int ret = OB_SUCCESS;
  ObLLVMValue type_value;
  ObLLVMValue type_ptr;
  ObLLVMValue id_value;
  ObLLVMValue id_ptr;
  ObLLVMValue isnull_value;
  ObLLVMValue isnull_ptr;
  ObLLVMValue count_value;
  ObLLVMValue count_ptr;
  ObLLVMValue notnull_value;
  ObLLVMValue notnull_ptr;
  ObLLVMValue meta_value;
  ObLLVMValue meta_ptr;
  ObDataType meta;
  const ObRecordMember *member = NULL;
  int64_t result_idx = OB_INVALID_INDEX;
  ObLLVMValue result;
  ObLLVMValue obobj_res;
  ObLLVMValue ptr_elem;
  ObObj null_obj;
  ObLLVMValue null_obj_ptr;
  ObLLVMValue null_obj_value;

  //设置composite和count
  OZ (generator.get_helper().get_int32(type_, type_value));
  OZ (generator.extract_type_ptr_from_record(value, type_ptr));
  OZ (generator.get_helper().create_store(type_value, type_ptr));
  OZ (generator.get_helper().get_int64(user_type_id_, id_value));
  OZ (generator.extract_id_ptr_from_record(value, id_ptr));
  OZ (generator.get_helper().create_store(id_value, id_ptr));
  if (is_object_type()) {
    OZ (generator.get_helper().get_int8(TRUE, isnull_value));
  } else {
    OZ (generator.get_helper().get_int8(FALSE, isnull_value));
  }
  OZ (generator.extract_isnull_ptr_from_record(value, isnull_ptr));
  OZ (generator.get_helper().create_store(isnull_value, isnull_ptr));
  OZ (generator.get_helper().get_int32( get_record_member_count(), count_value));
  OZ (generator.extract_count_ptr_from_record(value, count_ptr));
  OZ (generator.get_helper().create_store(count_value, count_ptr));

  //设置meta和数据
  null_obj.set_null();
  OZ (generator.generate_obj(null_obj, null_obj_ptr));
  OZ (generator.get_helper().create_load("load_null_obj", null_obj_ptr, null_obj_value));
  CK (OB_NOT_NULL(stmt));
  for (int64_t i = 0; OB_SUCC(ret) && i < get_record_member_count(); ++i) {
    member = get_record_member(i);
    CK (OB_NOT_NULL(member));

    //设置notnull和meta
    if (OB_SUCC(ret)) {
      meta.reset();
      if (NULL == member->member_type_.get_data_type()) {
        meta.set_obj_type(ObExtendType);
      } else {
        meta = *member->member_type_.get_data_type();
      }
      OZ (generator.get_helper().get_int8(false, notnull_value));
      OZ (generator.extract_notnull_ptr_from_record(value, i, notnull_ptr));
      OZ (generator.get_helper().create_store(notnull_value, notnull_ptr));
      OZ (generator.extract_meta_ptr_from_record(value, get_record_member_count(), i, meta_ptr));
      OZ (generator.store_data_type(meta, meta_ptr));
    }

    //设置数据
    if (OB_SUCC(ret)) {
      obobj_res = null_obj_value;
      if (OB_INVALID_INDEX != member->get_default()) {
        if (OB_NOT_NULL(member->get_default_expr())) {
          OZ (generator.generate_expr(member->get_default(), *stmt, result_idx, result));
        } else {
          OV (is_package_type(), OB_ERR_UNEXPECTED, KPC(this));
          OZ (generator.generate_spi_package_calc(extract_package_id(get_user_type_id()),
                                                  member->get_default(),
                                                  *stmt,
                                                  result));
        }
        OZ (generator.extract_obobj_from_objparam(result, obobj_res));
      }
      if (OB_SUCC(ret)) {
        ptr_elem.reset();
        if (member->member_type_.is_obj_type() || OB_INVALID_INDEX != member->get_default()) {
          //不论基础类型还是复杂类型，如果有default，直接把default值存入即可
          OZ (generator.extract_element_ptr_from_record(value,
                                                        get_record_member_count(),
                                                        i,
                                                        ptr_elem));
          OZ (generator.get_helper().create_store(obobj_res, ptr_elem));
          OZ (generator.generate_check_not_null(*stmt,
                                                member->member_type_.get_not_null(),
                                                result));
        } else { //复杂类型如果没有default，调用generate_construct
          ObLLVMValue extend_value;
          ObLLVMValue type_value;
          ObLLVMValue init_value;
          int64_t init_size = OB_INVALID_SIZE;
          OZ (generator.extract_element_ptr_from_record(value,
                                                    get_record_member_count(),
                                                    i,
                                                    ptr_elem));
          OZ (SMART_CALL(member->member_type_.generate_new(generator, ns, extend_value, stmt)));

          OZ (generator.get_helper().get_int8(member->member_type_.get_type(), type_value));
          OZ (member->member_type_.get_size(ns, PL_TYPE_INIT_SIZE, init_size));
          OZ (generator.get_helper().get_int32(init_size, init_value));
          OZ (generator.generate_set_extend(ptr_elem, type_value, init_value, extend_value));
        }
      }
    }
  }
  return ret;
}

int ObRecordType::get_size(const ObPLINS &ns, ObPLTypeSize type, int64_t &size) const
{
  int ret = OB_SUCCESS;
  size += get_data_offset(get_record_member_count());
  for (int64_t i = 0; OB_SUCC(ret) && i < get_record_member_count(); ++i) {
    const ObPLDataType *elem_type = get_record_member_type(i);
    CK (OB_NOT_NULL(elem_type));
    OZ (elem_type->get_size(ns, type, size));
  }
  return ret;
}

int ObRecordType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                   ObIAllocator &obj_allocator,
                                   sql::ObExecContext &exec_ctx,
                                   const sql::ObSqlExpression *default_expr,
                                   bool default_construct,
                                   ObObj &obj) const
{
  UNUSEDx(exec_ctx, default_expr, default_construct);
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t init_size = 0;
  obj.set_null();
  if (OB_NOT_NULL(default_expr)) {
    ObObj calc_obj;
    OZ (ObSQLUtils::calc_sql_expression_without_row(exec_ctx, *default_expr, calc_obj));
    CK (calc_obj.is_null() || calc_obj.is_pl_extend());
    if (calc_obj.is_pl_extend()) {
      OZ (ObUserDefinedType::deep_copy_obj(obj_allocator, calc_obj, obj));
    }
  }
  if (OB_FAIL(ret) || obj.is_pl_extend()) {
    // do nothing ...
  } else if (OB_FAIL(get_size(resolve_ctx, PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(obj_allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    new (data) ObPLRecord(user_type_id_, record_members_.count());
    obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size);

    ObPLRecord *record = reinterpret_cast<ObPLRecord*>(data);
    ObObj *member = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_member_count(); ++i) {
      CK (OB_NOT_NULL(get_member(i)));
      OZ (record->get_element(i, member));
      CK (OB_NOT_NULL(member));
      if (get_member(i)->is_obj_type()) {
        OX (new (member) ObObj(ObNullType));
      } else {
        int64_t init_size = OB_INVALID_SIZE;
        int64_t member_ptr = 0;
        OZ (get_member(i)->get_size(resolve_ctx, PL_TYPE_INIT_SIZE, init_size));
        OZ (get_member(i)->newx(obj_allocator, &resolve_ctx, member_ptr));
        OX (member->set_extend(member_ptr, get_member(i)->get_type(), init_size));
      }
    }
  }
  return ret;
}

int ObRecordType::free_session_var(const ObPLResolveCtx &resolve_ctx,
                                   ObIAllocator &obj_allocator,
                                   ObObj &obj) const
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  data = reinterpret_cast<char *>(obj.get_ext());
  if (!OB_ISNULL(data)) {
    int64_t data_pos = 0;
    int64_t element_init_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      if (OB_ISNULL(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid elem type", K(i), K(ret));
      } else {
        if (OB_FAIL(type->free_data(resolve_ctx, obj_allocator, static_cast<char *>(data)+data_pos))) {
          LOG_WARN("failed to get element serialize size", K(*this), K(ret));
        } else if (OB_FAIL(type->get_size(resolve_ctx, PL_TYPE_INIT_SIZE, element_init_size))) {
          LOG_WARN("get record element init size failed", K(ret));
        } else {
          data_pos += element_init_size;
        }
      }
    }
    if (OB_SUCC(ret)) {
      obj_allocator.free(data);
    }
  }
  if (OB_SUCC(ret)) {
    obj.set_null();
  }
  return ret;
}

int ObRecordType::free_data(const ObPLResolveCtx &resolve_ctx,
                            common::ObIAllocator &data_allocator,
                            void *data) const
{
  int ret = OB_SUCCESS;
  if (!OB_ISNULL(data)) {
    int64_t data_pos = 0;
    int64_t element_init_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      if (OB_ISNULL(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid elem type", K(i), K(ret));
      } else {
        if (OB_FAIL(type->free_data(resolve_ctx, data_allocator, static_cast<char *>(data)+data_pos))) {
          LOG_WARN("failed to get element serialize size", K(*this), K(ret));
        } else if (OB_FAIL(type->get_size(resolve_ctx, PL_TYPE_INIT_SIZE, element_init_size))) {
          LOG_WARN("get record element init size failed", K(ret));
        } else {
          data_pos += element_init_size;
        }
      }
    }
  }
  return ret;
}

// --------- for session serialize/deserialize interface ---------
int ObRecordType::get_serialize_size(
  const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  int ret = OB_SUCCESS;
  ObPLRecord *record = reinterpret_cast<ObPLRecord *>(src);
  CK (OB_NOT_NULL(record));
  OX (size += record->get_serialize_size());
  OX (size += serialization::encoded_length(record->get_count()));

  char *data = reinterpret_cast<char*>(record->get_element());
  for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
    const ObPLDataType *type = get_record_member_type(i);
    CK (OB_NOT_NULL(type));
    OZ (type->get_serialize_size(resolve_ctx, data, size));
  }
  return ret;
}

int ObRecordType::serialize(
  const ObPLResolveCtx &resolve_ctx,
  char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  ObPLRecord *record = reinterpret_cast<ObPLRecord *>(src);
  CK (OB_NOT_NULL(record));
  OX (record->serialize(dst, dst_len, dst_pos));
  OZ (serialization::encode(dst, dst_len, dst_pos, record->get_count()));

  char *data = reinterpret_cast<char*>(record->get_element());
  for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
    const ObPLDataType *type = get_record_member_type(i);
    CK (OB_NOT_NULL(type));
    OZ (type->serialize(resolve_ctx, data, dst, dst_len, dst_pos));
  }
  return ret;
}

int ObRecordType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  int ret = OB_SUCCESS;
  ObPLRecord *record = reinterpret_cast<ObPLRecord *>(dst);
  CK (OB_NOT_NULL(record));
  int64_t count = OB_INVALID_COUNT;
  // when record be delete , type will be PL_INVALID_TYPE
  OX (record->deserialize(src, src_len, src_pos));
  if (OB_SUCC(ret) && record->get_type() != PL_INVALID_TYPE) {
    OZ (serialization::decode(src, src_len, src_pos, count));
    OX (record->set_count(count));

    dst = reinterpret_cast<char*>(record->get_element());
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      CK (OB_NOT_NULL(type));
      OZ (type->deserialize(resolve_ctx, allocator, src, src_len, src_pos, dst));
    }
  }
  return ret;
}

int ObRecordType::add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                                   const ObPLBlockNS &block_ns,
                                                   const common::ObString &package_name,
                                                   const common::ObString &param_name,
                                                   int64_t mode, int64_t position,
                                                   int64_t level, int64_t &sequence,
                                                   share::schema::ObRoutineInfo &routine_info) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_name, position);
  for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
    const ObRecordMember* record_member = get_record_member(i);
    const ObPLDataType &type = record_member->member_type_;
    OZ (type.add_package_routine_schema_param(
        resolve_ctx, block_ns, package_name, record_member->member_name_,
        mode, i+1, level+1, sequence, routine_info), KPC(this));
  }
  return ret;
}

int ObRecordType::get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                             const ObPLBlockNS &current_ns) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
    const ObRecordMember* record_member = get_record_member(i);
    const ObPLDataType &type = record_member->member_type_;
    if (OB_FAIL(type.get_all_depended_user_type(resolve_ctx, current_ns))) {
       LOG_WARN("failed to add user type", K(*this), K(ret));
    }
  }
  return ret;
}

int ObRecordType::init_obj(ObSchemaGetterGuard &schema_guard,
                           ObIAllocator &allocator,
                           ObObj &obj,
                           int64_t &init_size) const
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  init_size = 0;
  if (OB_FAIL(get_size(ObPLUDTNS(schema_guard), PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    new (data) ObPLRecord(get_user_type_id(), get_record_member_count());
    obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size);
  }
  return ret;
}

int ObRecordType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                            const ObTimeZoneInfo *tz_info,
                            MYSQL_PROTOCOL_TYPE protocl_type,
                            char *&src,
                            char *dst,
                            const int64_t dst_len,
                            int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  int64_t bitmap_bytes = (record_members_.count() + 7 + 2) / 8;
  char* bitmap = NULL;
  ObObj* src_obj = reinterpret_cast<ObObj*>(src);
  ObPLRecord *record = NULL;
  char* new_src = NULL;

  // 计算空值位图位置
  if (dst_len - dst_pos < bitmap_bytes) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow",
             K(ret), K(dst_len), K(dst_pos), K(bitmap_bytes), K(record_members_.count()));
  } else {
    bitmap = dst + dst_pos;
    MEMSET(dst + dst_pos, 0, bitmap_bytes);
    dst_pos += bitmap_bytes;
  }
  CK (OB_NOT_NULL(src_obj));
  if (OB_SUCC(ret) && src_obj->is_ext()) {
    CK (OB_NOT_NULL(record = reinterpret_cast<ObPLRecord*>(src_obj->get_ext())));
    CK (OB_NOT_NULL(new_src = reinterpret_cast<char*>(record->get_element())));

    // 序列化值并更新空值位图
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      ObObj* obj = (reinterpret_cast<ObObj *>(new_src));
      CK (OB_NOT_NULL(type));
      CK (OB_NOT_NULL(obj));
      if (OB_FAIL(ret)) {
      } else if (obj->is_null()) {
        ObMySQLUtil::update_null_bitmap(bitmap, i);
        new_src += sizeof(ObObj);
      } else if (type->is_collection_type()) {
      } else {
        OZ (type->serialize(schema_guard, tz_info, protocl_type, new_src, dst, dst_len, dst_pos),
                            K(i), KPC(this));
      }
      LOG_DEBUG("serialize element finished!", K(ret), K(*this), K(i), K(src), K(dst), K(dst_len), K(dst_pos));
    }
  }
  OX (src += sizeof(ObObj));
  return ret;
}

int ObRecordType::deserialize(ObSchemaGetterGuard &schema_guard,
                              common::ObIAllocator &allocator,
                              const ObCharsetType charset,
                              const ObCollationType cs_type,
                              const ObCollationType ncs_type,
                              const common::ObTimeZoneInfo *tz_info,
                              const char *&src,
                              char *dst,
                              const int64_t dst_len,
                              int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  int64_t init_size = 0;
  ObPLRecord *record = reinterpret_cast<ObPLRecord*>(dst);
  if (OB_ISNULL(record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("record is null", K(ret), KP(dst), KP(record));
  } else if (OB_FAIL(get_size(ObPLUDTNS(schema_guard), PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("failed to get record type init size", K(ret));
  } else if (OB_ISNULL(dst) || (dst_len - dst_pos < init_size)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("data deserialize failed", K(ret), K(dst_len), K(dst_pos), K(init_size));
  } else {
    int64_t bitmap_bytes = ((record_members_.count() + 7) / 8);
    const char* bitmap = src;
    src += bitmap_bytes;
    record->set_count(record_members_.count());
    ObObj null_value;
    ObDataType *data_type = record->get_element_type();
    bool *not_null = record->get_not_null();
    char *new_dst = reinterpret_cast<char*>(record->get_element());
    int64_t new_dst_len = get_member_count() * sizeof(ObObj);
    int64_t new_dst_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      if (OB_ISNULL(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid record element type", K(ret), K(i), K(type), KPC(this));
      } else if (ObSMUtils::update_from_bitmap(null_value, bitmap, i)) {
        ObObj* value = reinterpret_cast<ObObj*>(new_dst + new_dst_pos);
        if (!type->is_obj_type()) {
          const ObUserDefinedType *user_type = NULL;
          ObPLUDTNS ns(schema_guard);
          ObArenaAllocator local_allocator;
          int64_t ptr = 0;
          ObPLComposite *composite = NULL;
          if (OB_FAIL(ns.get_user_type(type->get_user_type_id(), user_type, &local_allocator))) {
            LOG_WARN("failed to get user type", K(ret), KPC(type));
          } else if (OB_ISNULL(user_type)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get element type", K(ret), KPC(type));
          } else if (OB_FAIL(user_type->newx(allocator, &ns, ptr))) {
            LOG_WARN("failed to newx", K(ret), KPC(type));
          } else if (OB_ISNULL(composite = reinterpret_cast<ObPLComposite*>(ptr))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, got null composite value", K(ret));
          } else {
            composite->set_null();
            value->set_extend(ptr, type->get_type());
          }
        } else {
          value->set_null();
          new_dst_pos += sizeof(ObObj);
        }
      } else if (OB_FAIL(type->deserialize(schema_guard, allocator, charset, cs_type, ncs_type,
                                           tz_info, src, new_dst, new_dst_len, new_dst_pos))) {
        LOG_WARN("deserialize record element type failed", K(i), K(*this), K(src), K(dst), K(dst_len), K(dst_pos), K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (NULL == type->get_data_type()) {
        ObDataType data_type_tmp;
        data_type_tmp.set_obj_type(ObExtendType);
        data_type_tmp.set_udt_id(type->get_user_type_id());
        *data_type = data_type_tmp;
      } else {
        *data_type = *(type->get_data_type());
      }
      OX (*not_null = type->get_not_null());
      OX (data_type++);
      OX (not_null++);
      LOG_DEBUG("deserialize record element type finished", K(ret), K(i), K(*this), K(src), K(dst), K(dst_len), K(dst_pos));
    }
    OX (dst_pos += init_size);
  }
  return ret;
}

int ObRecordType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dst));
  for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
    const ObPLDataType *type = get_record_member_type(i);
    CK (OB_NOT_NULL(type));
    OZ (type->convert(ctx, src, dst));
  }
  return ret;
}


//---------- for ObPLCollection ----------

int ObPLComposite::deep_copy(ObPLComposite &src,
                             ObPLComposite *&dest,
                             ObIAllocator &allocator,
                             const ObPLINS *ns,
                             sql::ObSQLSessionInfo *session,
                             bool need_new_allocator,
                             bool ignore_del_element)
{
  int ret = OB_SUCCESS;


  switch (src.get_type()) {
  case PL_RECORD_TYPE: {
    ObPLRecord *composite = NULL;
    if (NULL == dest) {
      dest = reinterpret_cast<ObPLComposite*>(allocator.alloc(src.get_init_size()));
      composite = static_cast<ObPLRecord*>(dest);
      if (OB_ISNULL(composite)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate composite memory failed", K(ret));
      }
      OX (new(composite)ObPLRecord(src.get_id(), static_cast<ObPLRecord&>(src).get_count()));
    } else {
      OX (composite = static_cast<ObPLRecord*>(dest));
    }
    OZ (composite->deep_copy(static_cast<ObPLRecord&>(src), allocator, ns, session, ignore_del_element));
  }
    break;


  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected composite to copy", K(src.get_type()), K(ret));
  }
    break;
  }
  return ret;
}

int ObPLComposite::assign_element(ObObj &src, ObObj &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (src.is_ext()) {
    ObPLComposite *dest_composite = reinterpret_cast<ObPLComposite*>(dest.get_ext());
    ObPLComposite *src_composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
    CK (OB_NOT_NULL(src_composite));
    CK (OB_NOT_NULL(dest_composite));
    OZ (dest_composite->assign(src_composite, &allocator));
  } else {
    OZ (dest.apply(src));
  }
  return ret;
}

int ObPLComposite::copy_element(const ObObj &src,
                                ObObj &dest,
                                ObIAllocator &allocator,
                                const ObPLINS *ns,
                                sql::ObSQLSessionInfo *session,
                                const ObDataType *dest_type,
                                bool need_new_allocator,
                                bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  if (src.is_ext()) {
      ObPLComposite *dest_composite
        = (dest.get_ext() == src.get_ext()) ? NULL : reinterpret_cast<ObPLComposite*>(dest.get_ext());
      ObPLComposite *src_composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
      ObArenaAllocator tmp_allocator;
      CK (OB_NOT_NULL(src_composite));
      OZ (ObPLComposite::deep_copy(*src_composite,
                                   dest_composite,
                                   dest.get_ext() == src.get_ext() ? tmp_allocator : allocator,
                                   ns,
                                   session,
                                   need_new_allocator,
                                   ignore_del_element));
      CK (OB_NOT_NULL(dest_composite));
      if (src.get_ext() == dest.get_ext()) {
        OX (dest.set_extend(reinterpret_cast<int64_t>(src_composite),
                            src.get_meta().get_extend_type(),
                            src.get_val_len()));
        OZ (ObUserDefinedType::destruct_obj(dest, session));
        OZ (ObPLComposite::deep_copy(*dest_composite,
                                   src_composite,
                                   allocator,
                                   ns,
                                   session,
                                   need_new_allocator,
                                   ignore_del_element));
        OX (dest.set_extend(reinterpret_cast<int64_t>(dest_composite),
                            src.get_meta().get_extend_type(),
                            src.get_val_len()));
        OZ (ObUserDefinedType::destruct_obj(dest, session));
        OX (dest_composite = src_composite);
      }
      OX (dest.set_extend(reinterpret_cast<int64_t>(dest_composite),
                          src.get_meta().get_extend_type(),
                          src.get_val_len()));
  } else if (NULL != dest_type && NULL != session && !src.is_null()) {
    ObExprResType result_type;
    ObObjParam result;
    ObObjParam src_tmp;
    CK (OB_NOT_NULL(dest_type));
    OX (result_type.set_meta(dest_type->get_meta_type()));
    OX (result_type.set_accuracy(dest_type->get_accuracy()));
    OX (src_tmp = src);
    OZ (ObSPIService::spi_convert(*session, allocator, src_tmp, result_type, result));
    OZ (deep_copy_obj(allocator, result, dest));
  } else {
    if (src.is_null() && 0 != src.get_unknown()) {
      LOG_INFO("here maybe a bug", K(src), K(&src), K(src.get_unknown()));
    }
    OZ (deep_copy_obj(allocator, src, dest));
  }
  return ret;
}

int ObPLComposite::assign(ObPLComposite *src, ObIAllocator *allocator)
{
  int64_t size = OB_INVALID_SIZE;
  switch (get_type()) {
  case PL_RECORD_TYPE: {
    size = static_cast<ObPLRecord*>(this)->assign(static_cast<ObPLRecord*>(src), allocator);
  }
    break;
  default: {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected composite to get init size", K(get_type()));
  }
  }
  return size;
}

/*
 * 为了ObPLComposite及其继承类和LLVM之间的内存映射，本函数不能实现虚函数
 * */
int64_t ObPLComposite::get_init_size() const
{
  int64_t size = OB_INVALID_SIZE;
  switch (get_type()) {
  case PL_RECORD_TYPE: {
    size = static_cast<const ObPLRecord*>(this)->get_init_size();
  }
    break;


  default: {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected composite to get init size", K(get_type()));
  }
  }
  return size;
}

int64_t ObPLComposite::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length(type_);
  size += serialization::encoded_length(id_);
  size += serialization::encoded_length(is_null_);
  return size;
}

int ObPLComposite::serialize(char *buf, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ (serialization::encode(buf, len, pos, type_));
  OZ (serialization::encode(buf, len, pos, id_));
  OZ (serialization::encode(buf, len, pos, is_null_));
  return ret;
}

int ObPLComposite::deserialize(const char* buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OZ (serialization::decode(buf, len, pos,type_));
  OZ (serialization::decode(buf, len, pos, id_));
  OZ (serialization::decode(buf, len, pos, is_null_));
  return ret;
}

void ObPLComposite::print() const
{
  switch (get_type()) {
    case PL_RECORD_TYPE: {
      static_cast<const ObPLRecord*>(this)->print();
    }
      break;
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected composite to print", K(get_type()));
    }
    }
}

int ObPLRecord::get_not_null(int64_t i, bool &not_null)
{
  int ret = OB_SUCCESS;
  CK (i >= 0 && i < get_count());
  OX (not_null = reinterpret_cast<bool*>((int64_t)this + ObRecordType::get_notnull_offset())[i]);
  return ret;
}

int ObPLRecord::get_element_type(int64_t i, ObDataType &type)
{
  int ret = OB_SUCCESS;
  CK (i >= 0 && i < get_count());
  OX (type = reinterpret_cast<ObDataType*>((int64_t)this
      + ObRecordType::get_meta_offset(get_count()))[i]);
  return ret;
}

int ObPLRecord::get_element(int64_t i, ObObj &obj) const
{
  int ret = OB_SUCCESS;
  CK (i >= 0 && i < get_count());
  OX (obj = reinterpret_cast<const ObObj*>((int64_t)this
      + ObRecordType::get_data_offset(get_count()))[i]);
  return ret;
}

int ObPLRecord::get_element(int64_t i, ObObj *&obj)
{
  int ret = OB_SUCCESS;
  CK (i >= 0 && i < get_count());
  OX (obj = &reinterpret_cast<ObObj*>((int64_t)this
      + ObRecordType::get_data_offset(get_count()))[i]);
  return ret;
}

int ObPLRecord::assign(ObPLRecord *src, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(src));
  if (OB_SUCC(ret)) {
    MEMCPY(this, src, src->get_init_size());
    ObObj src_element;
    ObObj *dest_element = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
      OZ (src->get_element(i, src_element));
      OZ (get_element(i, dest_element));
      OZ (ObPLComposite::assign_element(src_element, *dest_element, *allocator));
    }
  }
  return ret;
}

int ObPLRecord::deep_copy(ObPLRecord &src,
                          ObIAllocator &allocator,
                          const ObPLINS *ns,
                          sql::ObSQLSessionInfo *session,
                          bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  if (get_id() == src.get_id()) {
    MEMCPY(this, &src, ObRecordType::get_data_offset(src.get_count()));
  }

  const ObUserDefinedType *user_type = NULL;
  const ObRecordType *record_type = NULL;
  if (NULL != ns) {
    OZ (ns->get_user_type(get_id(), user_type, NULL));
    OV (OB_NOT_NULL(user_type), OB_ERR_UNEXPECTED, K(get_id()), K(src.get_id()));
    CK (user_type->is_record_type());
    OX (record_type = static_cast<const ObRecordType*>(user_type));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
    ObObj src_element;
    ObObj *dest_element = NULL;
    const ObPLDataType *elem_type = NULL;
    OZ (src.get_element(i, src_element));
    OZ (get_element(i, dest_element));
    if (NULL != record_type) {
      CK (OB_NOT_NULL(elem_type = record_type->get_record_member_type(i)));
    }
    OZ (ObPLComposite::copy_element(src_element,
                                    *dest_element,
                                    allocator,
                                    ns,
                                    session,
                                    NULL == elem_type ? NULL : elem_type->get_data_type(),
                                    true, /*need_new_allocator*/
                                    ignore_del_element));
  }
  return ret;
}

int ObPLRecord::set_data(const ObIArray<ObObj> &row)
{
  int ret = OB_SUCCESS;
  CK (get_count() == row.count());
  if (OB_SUCC(ret)) {
    int64_t current_datum = reinterpret_cast<int64_t>(this)
        + ObRecordType::get_data_offset(get_count());
    for (int64_t i = 0; OB_SUCC(ret) && i < row.count(); ++i) {
      new (reinterpret_cast<ObObj*>(current_datum))ObObj(row.at(i));
      current_datum += sizeof(ObObj);
    }
  }
  return ret;
}

void ObPLRecord::print() const
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObPLRecord Header", K(this), K(*this), K(count_));
  ObObj obj;
  for (int64_t i= 0; i < get_count(); ++i) {
    OZ (get_element(i, obj));
    if (OB_SUCC(ret)) {
      if (obj.is_pl_extend()) {
        ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(obj.get_ext());
        LOG_INFO("ObPLRecord Data", K(i), K(get_count()), K(*composite));
        OX (composite->print());
      } else if (obj.is_varchar_or_char() && obj.get_data_length() > 100) {
        LOG_INFO("ObPLRecord Data", K(i), K(get_count()), K("xxx...xxx"));
      } else {
        LOG_INFO("ObPLRecord Data", K(i), K(get_count()), K(obj));
      }
    }
  }
}

int64_t ObElemDesc::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length(type_);
  size += serialization::encoded_length(not_null_);
  size += serialization::encoded_length(field_cnt_);
  return size;
}

int ObElemDesc::serialize(char *buf, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ (serialization::encode(buf, len, pos, type_));
  OZ (serialization::encode(buf, len, pos, not_null_));
  OZ (serialization::encode(buf, len, pos, field_cnt_));
  return ret;
}

int ObElemDesc::deserialize(const char* buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OZ (serialization::decode(buf, len, pos, type_));
  OZ (serialization::decode(buf, len, pos, not_null_));
  OZ (serialization::decode(buf, len, pos, field_cnt_));
  return ret;
}

/*
 * 我们约定一个原则：
 * 1、所有Collection内部的data域的ObObj数组（包括sort域和key域的内存）的内存都必须由该Collection自己的allocator分配，而不允许是其他任何allocator；
 * 2、如果data域里是基础数据类型，那么内存也应由Collection自己的allocator分配；
 * 3、如果data域是record，那么该record本身的内存同样由Collection自己的allocator分配；record里的基础数据类型的内存同样由Collection自己的allocator分配；
 * 4、如果data域里是子Collection，那么该子Collection数据结构本身由父Collection的allocator分配，子Collection的内存管理递归遵循此约定。
 * */
int ObPLCollection::deep_copy(ObPLCollection *src, ObIAllocator *allocator, bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  /*
   * 本函数会被copy_all_element_with_new_allocator调用，所以如果allocator传入了的话，必须用新的allocator。
   * 除了copy_all_element_with_new_allocator的调用栈之外，其他调用时传入的allocator都应是NULL。
   * 这就要求本函数调用前，必须已经设置好自己的allocator。
   */
  ObObj *new_objs = NULL;
  ObObj *old_objs = NULL;
  ObIAllocator *coll_allocator = NULL == allocator ? allocator_ : allocator;
  CK (OB_NOT_NULL(coll_allocator));
  CK (OB_NOT_NULL(src) && src->is_collection());

  if (OB_SUCC(ret)) {
    void* data = NULL;
    int64_t k = 0;
    if (src->get_count() > 0) {
      data = coll_allocator->alloc(src->get_count() * sizeof(ObObj));
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for collection",
                 K(ret), K(src->get_count()));
      }
      CK (OB_NOT_NULL(new_objs = reinterpret_cast<ObObj*>(data)));
      CK (OB_NOT_NULL(old_objs = reinterpret_cast<ObObj*>(src->get_data())));
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < src->get_count(); ++i) {
        ObObj old_obj = old_objs[i];
        if (old_objs[i].is_invalid_type() && ignore_del_element && !is_associative_array()) {
          // ignore delete element
        } else {
          if (old_objs[i].is_invalid_type() && src->is_of_composite()) {
            old_obj.set_type(ObExtendType);
            CK (old_obj.is_pl_extend());
          }
          OX (new (&new_objs[k])ObObj());
          OZ (ObPLComposite::copy_element(old_obj,
                                          new_objs[k],
                                          *coll_allocator,
                                          NULL, /*ns*/
                                          NULL, /*session*/
                                          NULL, /*dest_type*/
                                          true, /*need_new_allocator*/
                                          ignore_del_element));
          OX (++k);
          if (old_objs[i].is_invalid_type() && src->is_of_composite()) {
            new_objs[i].set_type(ObMaxType);
          }
        }
      }
      // 对于已经copy成功的new obj释放内存
      if (OB_FAIL(ret) && OB_NOT_NULL(data)) {
        for (int64_t j = 0; j <= k && j < src->get_count(); ++j) {
          int tmp = ObUserDefinedType::destruct_obj(new_objs[j]);
          if (OB_SUCCESS != tmp) {
            LOG_WARN("fail torelease memory", K(ret), K(tmp));
          }
          new_objs[j].set_type(ObMaxType);
        }
        if (NULL == allocator) {
          coll_allocator->reset();
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_allocator(coll_allocator);
      set_type(src->get_type());
      set_id(src->get_id());
      set_is_null(src->is_null());
      set_element_desc(src->get_element_desc());
      set_count(src->get_count() > 0 ? k : src->get_count());
      if (src->get_count() > 0) {
        if (0 == k) {
          set_first(OB_INVALID_INDEX);
          set_last(OB_INVALID_INDEX);
        } else if (ignore_del_element && !is_associative_array()) {
          set_first(1);
          set_last(k);
        } else {
          set_first(src->get_first());
          set_last(src->get_last());
        }
      } else {
        set_first(src->get_first());
        set_last(src->get_last());
      }
      set_data(new_objs);
    }
  }
  return ret;
}

int ObPLCollection::assign(ObPLCollection *src, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObObj *new_objs = NULL;
  ObObj *old_objs = NULL;
  ObIAllocator *coll_allocator = NULL == allocator_ ? allocator : allocator_;
  CK (OB_NOT_NULL(coll_allocator));
  CK (OB_NOT_NULL(src) && src->is_collection());
  if (OB_SUCC(ret)) {
    void* data = NULL;
    if (src->get_count() > 0) {
      data = coll_allocator->alloc(src->get_count() * sizeof(ObObj));
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for collection",
                 K(ret), K(src->get_count()));
      }
      CK (OB_NOT_NULL(new_objs = reinterpret_cast<ObObj*>(data)));
      CK (OB_NOT_NULL(old_objs = reinterpret_cast<ObObj*>(src->get_data())));
      for (int64_t i = 0; OB_SUCC(ret) && i < src->get_count(); ++i) {
        new (&new_objs[i])ObObj();
        OZ (ObPLComposite::assign_element(old_objs[i], new_objs[i], *coll_allocator));
      }
    }
    if (OB_SUCC(ret)) {
      set_allocator(coll_allocator);
      set_type(src->get_type());
      set_id(src->get_id());
      set_is_null(src->is_null());
      set_element_desc(src->get_element_desc());
      set_count(src->get_count());
      set_first(src->get_first());
      set_last(src->get_last());
      set_data(new_objs);
    }
  }
  return ret;
}

bool ObPLCollection::is_contain_null_val() const
{
  bool b_ret = false;
  for (int64_t i = 0; !b_ret && i < count_; ++i) {
    ObObj *item =data_ + i;
    b_ret = item->is_null();
  }
  return b_ret;
}

int ObPLCollection::is_elem_deleted(int64_t index, bool &is_del) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > index || index > get_count() - 1)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of range.", K(index), K(get_count()));
  } else if (OB_ISNULL(get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is uninited", K(ret));
  } else {
    ObObj *obj = const_cast<ObObj *>(static_cast<const ObObj *>(get_data()));
    is_del = obj[index].is_invalid_type();
  }

  return ret;
}

int ObPLCollection::delete_collection_elem(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > index || index > get_count() - 1)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("array index out of range.", K(index), K(get_count()));
  } else if (OB_ISNULL(get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is uninited", K(ret));
  } else {
    ObObj *obj = static_cast<ObObj *>(get_data());
    // data的type设置为max表示被delete
    if (index < get_count()) {
      if (OB_FAIL(ObUserDefinedType::destruct_obj(obj[index], NULL))) {
        LOG_WARN("failed to destruct obj", K(ret), K(obj[index]), K(index));
      } else {
        obj[index].set_type(ObMaxType);
      }
    } else {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("type with step large than 1 is oversize", K(index), K(get_count()));
    }
  }

  return ret;
}

int ObPLCollection::trim_collection_elem(int64_t trim_number)
{
  int ret = OB_SUCCESS;

  if (is_associative_array()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("associative array is not support trim operation", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "associative array in trim operation");
  } else {
    if (0 > trim_number || trim_number > get_count()) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      LOG_WARN("trim size is out of range", K(trim_number), K(get_count()));
    } else if (0 == trim_number) {
      // do nothing
    } else {
      count_ -= trim_number;
      last_ = OB_INVALID_INDEX;
      update_last_impl();
      //全部
      if (first_ >= count_) {
        first_ = OB_INVALID_INDEX;
        update_first_impl();
      }
    }
  }

  return ret;
}

int64_t ObPLCollection::get_actual_count()
{
  int64_t count = get_count();
  int64_t cnt = 0;
  ObObj *objs = static_cast<ObObj*>(get_data());
  for (int64_t i = 0; i < count; ++i) {
    if (objs[i].is_invalid_type()) {
      cnt++;
    } else {
      LOG_DEBUG("array out of range.", K(i), K(cnt), K(count));
    }
  }
  return count - cnt;
}

int ObPLCollection::update_first_impl()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl collection is not inited", K(ret));
  } else if (0 > count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is empty", K(count_), K(ret));
  } else {
    #define FIND_FIRST(start, end) \
    do {\
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) { \
      if (OB_FAIL(is_elem_deleted(i, is_deleted))) { \
        LOG_WARN("unexpected first index", K(ret));\
      } else if (!is_deleted) {\
        OX (set_first(i + 1));\
        break;\
      }\
    } }while(0)

    bool is_deleted = false;
    // 当有赋值的时候，first和last都会被置成该值，所以需要从头遍历一遍。
    // 为啥需要这个，是为了优化性能，比如现在first是4，这个时候2的赋值赋值的。
    // 所以需要从头遍历。但是delete的时候，不会做这个操作，所以，只要判断first对应的是否有效又可以了。
    if (OB_INVALID_INDEX == first_) {
      FIND_FIRST(0, count_ - 1);
    } else {
      FIND_FIRST(first_ - 1, count_ - 1);
    }
  }
  return ret;
}

int ObPLCollection::update_last_impl()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl collection is not inited", K(ret));
  } else if (0 > count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is empty", K(count_), K(ret));
  } else {

#define FIND_LAST(start, end)                                     \
  do {                                                            \
    for (int64_t i = start; OB_SUCC(ret) && i >= end; --i) {      \
      if (OB_FAIL(is_elem_deleted(i, is_deleted))) {              \
        LOG_WARN("unexpected last index", K(ret));                \
      } else if (!is_deleted) {                                   \
        OX (set_last(i + 1));                                     \
        break;                                                    \
      }                                                           \
    }                                                             \
  } while(0);

    bool is_deleted = true;
    if (OB_INVALID_INDEX == last_) {
      FIND_LAST(count_ - 1, 0);
    } else {
      FIND_LAST(last_ - 1, 0);
    }

#undef FIND_LAST
  }
  return ret;
}

int64_t ObPLCollection::get_first()
{
  int ret = OB_SUCCESS;
  int64_t first = first_;
  if (OB_FAIL(update_first_impl())) {
    first = OB_INVALID_INDEX;
    LOG_WARN("update collection first failed.", K(ret), K(first), K(first_));
  } else {
    first = first_;
  }
  return first;
}

int64_t ObPLCollection::get_last()
{
  int ret = OB_SUCCESS;
  int64_t last = last_;
  if (OB_FAIL(update_last_impl())) {
    last = OB_INVALID_INDEX;
    LOG_WARN("update collection last failed.", K(ret), K(last), K(last_));
  } else {
    last = last_;
  }
  return last;
}

int ObPLCollection::get_serialize_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  CK (is_inited());
  OX (size += serialization::encoded_length(get_count()));
  OX (size += serialization::encoded_length(get_first()));
  OX (size += serialization::encoded_length(get_last()));
  if (OB_SUCC(ret)) {
      char *data = reinterpret_cast<char *>(get_data());
      for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
        ObObj *obj = reinterpret_cast<ObObj*>(data + sizeof(ObObj) * i);
        OX (size += obj->get_serialize_size());
      }
    }
  return ret;
}

int ObPLCollection::serialize(char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  CK (is_inited());
  OZ (serialization::encode(buf, len, pos, get_count()));
  OZ (serialization::encode(buf, len, pos, get_first()));
  OZ (serialization::encode(buf, len, pos, get_last()));

  if (OB_SUCC(ret)) {
    char *data = reinterpret_cast<char *>(get_data());
    for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
      ObObj *obj = reinterpret_cast<ObObj*>(data + sizeof(ObObj) * i);
      OZ (obj->serialize(buf, len, pos));
    }
  }
  return ret;
}

void ObPLCollection::print() const
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObPLCollection Header", K(this), K(*this));
  for (int64_t i = 0; i < count_; ++i) {
    ObObj &obj = data_[i];
    if (obj.is_pl_extend()) {
      ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(obj.get_ext());
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(*composite));
      OX (composite->print());
    } else if (obj.is_varchar_or_char() && obj.get_data_length() > 100) {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K("xxx...xxx"));
    } else if (obj.is_invalid_type()) {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K("deleted element"), K(obj));
    } else {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(obj));
    }
  }
}

int ObPLCollection::shrink()
{
  int ret = OB_SUCCESS;
  if (dynamic_cast<ObPLAllocator*>(allocator_) != NULL) {
    OZ ((dynamic_cast<ObPLAllocator*>(allocator_))->shrink());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("should never be here");
  }
  return ret;
}

int ObPLCollection::deserialize(common::ObIAllocator &allocator,
                                const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(allocator, buf, len, pos);
  return ret;
}

int ObPLCollection::add_row(const ObIArray<ObObj> &row, bool deep_copy)
{
  UNUSEDx(row, deep_copy);
  int ret = OB_SUCCESS;
  return ret;
}

/*
 * Collection里的数据有多重情况需要分开处理：
 * 1、简单类型，element_不是extend，col_cnt_是1：直接按顺序写进Obj即可
 * 2、复杂类型，element_是extend，col_cnt_是1：此时两种情况：
 *        a、可能是个Record，里面只有一个元素：需要构造Record空间
 *        b、可能是个Collection，直接写进Obj即可
 * 3、复杂类型，element_是extend，col_cnt_大于1：说明是个Record：需要构造Record空间
 * */
int ObPLCollection::set_row(const ObIArray<ObObj> &row, int64_t idx, bool deep_copy)
{
  int ret = OB_SUCCESS;
  CK (!row.empty());
  OV (idx >= 0 && idx < get_count(), OB_ERR_UNEXPECTED, idx, get_count());
  OV (element_.get_field_count() == row.count(), OB_ERR_UNEXPECTED, element_, row);
  if (OB_FAIL(ret)) {
  } else if (deep_copy) {
    //TODO: @ryan.ly
  } else {
    ObObj &data_obj = data_[idx];
    if (element_.is_composite_type()) {
      if (data_obj.is_ext()) { //已经是extend，说明该空间已经分配了内存，直接在内存写即可
        CK (0 != data_obj.get_ext());
        if (OB_SUCC(ret)) {
          ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(data_obj.get_ext());
          if (composite->is_record()) {
            ObPLRecord *record = static_cast<ObPLRecord*>(composite);
            OZ (record->set_data(row));
          } else if (composite->is_collection()) {
            CK (1 == row.count() && row.at(0).is_ext());
            OX (new (&data_obj)ObObj(row.at(0)));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected composite in array", K(*composite), K(ret));
          }
        }
      } else if (data_obj.is_null()) { //还没有分配空间，需要分配
        if (element_.is_record_type()) {
          ObPLRecord *new_record = reinterpret_cast<ObPLRecord*>(
              allocator_->alloc(ObRecordType::get_init_size(element_.get_field_count())));
          if (OB_ISNULL(new_record)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate composite memory failed", K(ret));
          }
          OX (new (new_record)ObPLRecord(element_.get_udt_id(), element_.get_field_count()));
          OX (new_record->set_data(row));
          OX (data_obj.set_extend(reinterpret_cast<int64_t>(new_record),
                                  PL_RECORD_TYPE,
                                  ObRecordType::get_init_size(element_.get_field_count())));
        } else {
          CK (1 == row.count());
          OX (new (&data_obj)ObObj(row.at(0)));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected data in array", K(data_obj), K(element_), K(ret));
      }
    } else {
      CK (1 == row.count());
      OX (new (&data_obj)ObObj(row.at(0)));
    }
  }
  return ret;
}

int ObPLCollection::set_row(const ObObj &row, int64_t idx, bool deep_copy)
{
  int ret = OB_SUCCESS;
  CK (idx >= 0 && idx < get_count());
  if (deep_copy) {
    //TODO: @ryan.ly
  } else {
    ObObj &data_obj = data_[idx];
    if (element_.is_composite_type()) {
      CK (row.is_pl_extend());
      if (data_obj.is_ext()) { //已经是extend，说明该空间已经分配了内存，直接在内存写即可
        CK (0 != data_obj.get_ext());
        if (OB_SUCC(ret)) {
          ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(data_obj.get_ext());
          if (composite->is_record()) {
            CK (PL_RECORD_TYPE == row.get_meta().get_extend_type());
            if (OB_SUCC(ret)) {
              ObPLRecord *src_record = reinterpret_cast<ObPLRecord*>(row.get_ext());
              ObPLRecord *dest_record = static_cast<ObPLRecord*>(composite);
              OZ (dest_record->assign(src_record, get_allocator()));
            }
          } else if (composite->is_collection()) {
            CK (PL_NESTED_TABLE_TYPE == row.get_meta().get_extend_type()
                || PL_ASSOCIATIVE_ARRAY_TYPE == row.get_meta().get_extend_type()
                || PL_VARRAY_TYPE == row.get_meta().get_extend_type());
            ObPLCollection *src_collection = reinterpret_cast<ObPLCollection*>(row.get_ext());
            ObPLCollection *dest_collection = static_cast<ObPLCollection*>(composite);
            OZ (dest_collection->assign(src_collection, get_allocator()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected composite in array", K(*composite), K(ret));
          }
        }
      } else if (data_obj.is_null()) { //还没有分配空间，需要分配
        if (element_.is_record_type()) {
          ObPLRecord *new_record = reinterpret_cast<ObPLRecord*>(
              allocator_->alloc(ObRecordType::get_init_size(element_.get_field_count())));
          CK (OB_NOT_NULL(new_record));
          OX (new (new_record)ObPLRecord(element_.get_udt_id(), element_.get_field_count()));
          CK (PL_RECORD_TYPE == row.get_meta().get_extend_type());
          if (OB_SUCC(ret)) {
            ObPLRecord *src_record = reinterpret_cast<ObPLRecord*>(row.get_ext());
            OZ (new_record->assign(src_record, get_allocator()));
          }
          OX (data_obj.set_ext(reinterpret_cast<int64_t>(new_record)));
        } else {
          CK (PL_NESTED_TABLE_TYPE == row.get_meta().get_extend_type()
              || PL_ASSOCIATIVE_ARRAY_TYPE == row.get_meta().get_extend_type()
              || PL_VARRAY_TYPE == row.get_meta().get_extend_type());
          //TODO: @ryan.ly
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected data in array", K(data_obj), K(element_), K(ret));
      }
    } else { //情况1
      CK (!row.is_pl_extend());
      if (OB_SUCC(ret)) {
        new (&data_obj)ObObj(row);
      }
    }
  }
  return ret;
}

int ObPLCollection::first(ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == get_first()) {
    result.set_null();
  } else {
    result.set_int(get_first());
  }
  return ret;
}

int ObPLCollection::last(ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == get_last()) {
    result.set_null();
  } else {
    result.set_int(get_last());
  }
  return ret;
}

int ObPLCollection::prior(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  bool need_search = true;
  if (OB_INVALID_INDEX == get_first() || idx < get_first()) {
    result.set_null();
    need_search = false;
  } else if (idx > get_last()) {
    OX (idx = get_last());
    OZ (is_elem_deleted((idx - 1), need_search));
    OX (result.set_int(idx));
  }
  if (OB_SUCC(ret) && need_search) {
    bool is_del = false;
    for (idx = (idx - 1); OB_SUCC(ret) && idx >= get_first(); --idx) {
      CK (idx >= 1);
      OZ (is_elem_deleted((idx - 1), is_del));
      if (OB_SUCC(ret) && !is_del) {
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_del || idx < get_first()) {
      result.set_null();
    } else {
      result.set_int(idx);
    }
  }
  return ret;
}

int ObPLCollection::next(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  bool need_search = true;
  if (OB_INVALID_INDEX == get_last() || idx > get_last()) {
    result.set_null();
    need_search = false;
  } else if (idx < get_first()) {
    idx = get_first();
    OZ (is_elem_deleted(idx - 1, need_search));
    OX (result.set_int(idx));
  }
  if (OB_SUCC(ret) && need_search) {
    bool is_del = false;
    idx = (idx > get_count()) ? get_count() : idx;
    for (idx = idx + 1; OB_SUCC(ret) && idx <= get_last(); ++idx) {
      CK (idx >= 1);
      OZ (is_elem_deleted((idx - 1), is_del));
      if (OB_SUCC(ret) && !is_del) {
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_del || idx > get_last()) {
      result.set_null();
    } else {
      result.set_int(idx);
    }
  }
  return ret;
}

int ObPLCollection::exist(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t first = get_first();
  int64_t last = get_last();
  if (OB_INVALID_INDEX == idx || OB_INVALID_INDEX == first || OB_INVALID_INDEX == last) {
    result.set_tinyint(false);
  } else if (idx < first || idx > last) {
    result.set_tinyint(false);
  } else {
    bool is_del =  false;
    OZ (is_elem_deleted(idx - 1, is_del));
    OX (result.set_tinyint(is_del ? false : true));
  }
  return ret;
}


}  // namespace pl
}  // namespace oceanbase
