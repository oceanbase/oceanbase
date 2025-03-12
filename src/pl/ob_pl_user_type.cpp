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

#define USING_LOG_PREFIX PL

#include "ob_pl_user_type.h"
#include "observer/mysql/obsm_utils.h"
#include "pl/ob_pl_code_generator.h"
#include "pl/ob_pl_package.h"
#include "observer/mysql/ob_query_driver.h"

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
  const ObPLINS &ns, jit::ObLLVMValue &allocator, jit::ObLLVMValue &dest) const
{
  UNUSEDx(generator, ns, allocator, dest); return OB_SUCCESS;
}

int ObUserDefinedType::generate_default_value(
  ObPLCodeGenerator &generator,
  const ObPLINS &ns, const pl::ObPLStmt *stmt, jit::ObLLVMValue &value, jit::ObLLVMValue &allocator, bool is_top_level) const
{
  UNUSEDx(generator, ns, stmt, value, allocator); return OB_SUCCESS;
}

int ObUserDefinedType::generate_copy(
  ObPLCodeGenerator &generator, const ObPLBlockNS &ns,
  jit::ObLLVMValue &allocator, jit::ObLLVMValue &src, jit::ObLLVMValue &dest,
  uint64_t location, bool in_notfound, bool in_warning, uint64_t package_id) const
{
  UNUSEDx(generator, ns, allocator, src, dest, in_notfound, in_warning, package_id);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "Call virtual func of ObUserDefinedType! May forgot implement in SubClass", K(this));
  return OB_NOT_SUPPORTED;
}

int ObUserDefinedType::get_size(
  ObPLTypeSize type, int64_t &size) const
{
  UNUSEDx(type, size);
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
  const sql::ObSQLSessionInfo &session,
  const common::ObTimeZoneInfo *tz_info, obmysql::MYSQL_PROTOCOL_TYPE type,
  char *&src, char *dst, const int64_t dst_len, int64_t &dst_pos) const
{
  UNUSEDx(schema_guard, session, tz_info, type, src, dst, dst_len, dst_pos);
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
                                          jit::ObLLVMValue &allocator,
                                          bool is_top_level,
                                          const pl::ObPLStmt *s) const
{
  int ret = OB_SUCCESS;
  ObLLVMValue composite_value;
  ObLLVMType ir_type;
  ObLLVMType ir_pointer_type;

  OZ (generator.get_llvm_type(*this, ir_type));
  OZ (ir_type.get_pointer_to(ir_pointer_type));
  OZ (generator.get_helper().create_int_to_ptr(ObString("ptr_to_user_type"), value, ir_pointer_type,
                                             composite_value));
  OZ (generate_construct(generator, ns, composite_value, allocator, is_top_level, s));
  return ret;
}

int ObUserDefinedType::generate_construct(ObPLCodeGenerator &generator,
                                          const ObPLINS &ns,
                                          jit::ObLLVMValue &value,
                                          jit::ObLLVMValue &allocator,
                                          bool is_top_level,
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
#ifdef OB_BUILD_ORACLE_PL
    //all Composite can call copy_element
    case PL_OPAQUE_TYPE: //fallthrough
    case PL_NESTED_TABLE_TYPE: //fallthrough
    case PL_ASSOCIATIVE_ARRAY_TYPE: //fallthrough
    case PL_VARRAY_TYPE: //fallthrough
#endif
    case PL_RECORD_TYPE: {
      OZ (ObPLComposite::copy_element(src, dst, allocator, NULL, NULL, NULL,  need_new_allocator, ignore_del_element));
    }
      break;

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to deep copy", K(src), K(ret), K(src.get_meta().get_extend_type()));
    }
      break;
    }
  }
  return ret;
}

int ObUserDefinedType::destruct_objparam(ObIAllocator &alloc, ObObj &src, ObSQLSessionInfo *session, bool direct_use_alloc)
{
  int ret = OB_SUCCESS;

  if (src.is_pl_extend()) {
    int8_t extend_type = src.get_meta().get_extend_type();
    if (PL_RECORD_TYPE == extend_type ||
        PL_NESTED_TABLE_TYPE == extend_type ||
        PL_ASSOCIATIVE_ARRAY_TYPE == extend_type ||
        PL_VARRAY_TYPE == extend_type) {
      ObPLAllocator1 *pl_allocator = nullptr;
      ObIAllocator *parent_allocator = nullptr;
      ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
      if (direct_use_alloc) {
        ObIAllocator *allocator = nullptr;
        OV (OB_NOT_NULL(composite), OB_ERR_UNEXPECTED, lbt());
        OX (allocator = composite->get_allocator());
        OZ (SMART_CALL(ObUserDefinedType::destruct_obj(src, session)));
        if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
          alloc.free(allocator);
          composite->set_allocator(nullptr);
        }
        OX (alloc.free(composite));
      } else {
        OV (OB_NOT_NULL(composite), OB_ERR_UNEXPECTED, lbt());
        OV (OB_NOT_NULL(composite->get_allocator()), OB_ERR_UNEXPECTED, lbt());
        OX (pl_allocator = dynamic_cast<ObPLAllocator1 *>(composite->get_allocator()));
        CK (OB_NOT_NULL(pl_allocator));
        CK (OB_NOT_NULL(parent_allocator = pl_allocator->get_parent_allocator()));
        OZ (SMART_CALL(ObUserDefinedType::destruct_obj(src, session)));
        //CK (parent_allocator == &alloc);
        OX (parent_allocator->free(pl_allocator));
        OX (composite->set_allocator(nullptr));
        OX (parent_allocator->free(composite));
      }
#ifdef OB_BUILD_ORACLE_PL
    } else if (PL_OPAQUE_TYPE == extend_type) {
      ObPLOpaque *opaque = reinterpret_cast<ObPLOpaque*>(src.get_ext());
      OZ (SMART_CALL(ObUserDefinedType::destruct_obj(src, session)));
      OX (alloc.free(opaque));
#endif
    } else {
      OZ (SMART_CALL(ObUserDefinedType::destruct_obj(src, session)));
    }
  } else {
    void *ptr = src.get_deep_copy_obj_ptr();
    if (nullptr != ptr) {
      alloc.free(ptr);
    }
  }
  src.set_null();

  return ret;
}

int ObUserDefinedType::reset_composite(ObObj &value, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  CK (value.is_pl_extend());
  if (OB_SUCC(ret)) {
    if (PL_RECORD_TYPE == value.get_meta().get_extend_type()) {
      OZ (ObUserDefinedType::reset_record(value, session));
    } else {
      OZ (ObUserDefinedType::destruct_obj(value, session, true));
    }
  }

  return ret;
}

int ObUserDefinedType::reset_record(ObObj &src, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;

  ObPLRecord *record = reinterpret_cast<ObPLRecord*>(src.get_ext());
  CK (OB_NOT_NULL(record));
  if (OB_SUCC(ret) && OB_NOT_NULL(record->get_allocator())) {
    ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(record->get_allocator());
    CK (OB_NOT_NULL(pl_allocator));
    for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
      ObObj &obj = record->get_element()[i];
      if (obj.is_pl_extend()) {
        int8_t extend_type = obj.get_meta().get_extend_type();
        if (PL_RECORD_TYPE == extend_type) {
          OZ (SMART_CALL(reset_record(obj, session)));
        } else if (PL_NESTED_TABLE_TYPE == extend_type ||
                  PL_ASSOCIATIVE_ARRAY_TYPE == extend_type ||
                  PL_VARRAY_TYPE == extend_type) {
          OZ (SMART_CALL(destruct_obj(obj, session, true)));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ret), K(obj), K(extend_type), KPC(record));
        }
      } else {
        OZ (SMART_CALL(destruct_objparam(*pl_allocator, obj, session, true)));
      }
    }
  }

  return ret;
}

// keep_composite_attr = true, 保留其allocator属性，对于record而言，保留data域
// 否则, 所有内存都清理
int ObUserDefinedType::destruct_obj(ObObj &src, ObSQLSessionInfo *session, bool keep_composite_attr)
{
  int ret = OB_SUCCESS;

  if (src.is_pl_extend() && src.get_ext() != 0) {
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
    case PL_REF_CURSOR_TYPE: {
      // do nothing
    }
      break;
    case PL_RECORD_TYPE: {
      ObPLRecord *record = reinterpret_cast<ObPLRecord*>(src.get_ext());
      CK  (OB_NOT_NULL(record));
      if (OB_SUCC(ret) && OB_NOT_NULL(record->get_allocator())) {
        ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(record->get_allocator());
        CK (OB_NOT_NULL(pl_allocator));
        for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
          ObObj &obj = record->get_element()[i];
          OZ (SMART_CALL(destruct_objparam(*pl_allocator, obj, session, true)));
          new(&obj)ObObj();
        }
      }
      if (OB_SUCC(ret)) {
        common::ObIAllocator *record_allocator = record->get_allocator();
        if (NULL == record_allocator) {
          //只定义过而没有用过的Record的allocator为空，这是正常的，跳过即可
          LOG_DEBUG("Notice: a record declared but not used", K(src), K(ret));
        } else {
          ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(record_allocator);
          if (NULL == pl_allocator) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("here must be a bug!!!", K(record_allocator), K(ret));
          } else if (!pl_allocator->is_inited()) {
            // do nothing
          } else if (!keep_composite_attr) {
            common::ObIAllocator *parent_allocator = pl_allocator->get_parent_allocator();
            CK (OB_NOT_NULL(parent_allocator));
            if (OB_SUCC(ret)) {
              pl_allocator->free(record->get_element());
              //pl_allocator->reset();
              pl_allocator->~ObPLAllocator1();
              //parent_allocator->free(pl_allocator);
              record->set_allocator(nullptr);
              record->set_data(nullptr);
              record->set_count(0);
              //parent_allocator->free(record);
            }
          } else {
            OX (record->set_null());
          }
        }
      }
    }
      break;
#ifdef OB_BUILD_ORACLE_PL
    case PL_NESTED_TABLE_TYPE: //fallthrough
    case PL_ASSOCIATIVE_ARRAY_TYPE: //fallthrough
    case PL_VARRAY_TYPE: {
      ObPLCollection *collection = reinterpret_cast<ObPLCollection*>(src.get_ext());
      CK  (OB_NOT_NULL(collection));
      if (OB_SUCC(ret) && OB_NOT_NULL(collection->get_allocator())) {
        ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(collection->get_allocator());
        CK (OB_NOT_NULL(pl_allocator));
        for (int64_t i = 0; OB_SUCC(ret) && i < collection->get_count(); ++i) {
          CK (OB_NOT_NULL(collection->get_data()));
          if (OB_SUCC(ret)) {
            ObObj &obj = collection->get_data()[i];
            OZ (SMART_CALL(destruct_objparam(*pl_allocator, obj, session, true)));
          }
        }
      }
      if (OB_SUCC(ret)) {
        common::ObIAllocator *collection_allocator = collection->get_allocator();
        if (NULL == collection_allocator) {
          //只定义过而没有用过的Collection的allocator为空，这是正常的，跳过即可
          LOG_DEBUG("Notice: a collection declared but not used", K(src), K(ret));
        } else {
          ObPLAllocator1 *pl_allocator = dynamic_cast<ObPLAllocator1 *>(collection_allocator);
          if (NULL == pl_allocator) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("here must be a bug!!!", K(collection_allocator), K(ret));
          } else if (!pl_allocator->is_inited()) {
            // do nothing
          } else {
            ObPLAssocArray *assoc = NULL;
            common::ObIAllocator *parent_allocator = pl_allocator->get_parent_allocator();
            CK (OB_NOT_NULL(parent_allocator));
            if (OB_SUCC(ret)) {
              if (collection->is_associative_array() &&
                  OB_NOT_NULL(assoc = static_cast<ObPLAssocArray*>(collection))) {
                if (OB_NOT_NULL(assoc->get_key()) &&
                    OB_NOT_NULL(assoc->get_sort())) {
                  for (int64_t i = 0; OB_SUCC(ret) && i < collection->get_count(); ++i) {
                    ObObj *key = assoc->get_key(i);
                    CK (OB_NOT_NULL(key));
                    if (OB_SUCC(ret)) {
                      void * ptr = key->get_deep_copy_obj_ptr();
                      if (nullptr != ptr) {
                        pl_allocator->free(ptr);
                      }
                    }
                  }
                  pl_allocator->free(assoc->get_key());
                  pl_allocator->free(assoc->get_sort());
                }
                assoc->set_key(NULL);
                assoc->set_sort(NULL);
              }
              if (keep_composite_attr) {
                pl_allocator->free(collection->get_data());
                collection->set_data(NULL, 0);
                collection->set_count(-1);
                collection->set_first(OB_INVALID_INDEX);
                collection->set_last(OB_INVALID_INDEX);
              } else {
                pl_allocator->free(collection->get_data());
                pl_allocator->~ObPLAllocator1();
                collection->set_data(NULL, 0);
                collection->set_null();
                collection->set_count(-1);
                collection->set_allocator(nullptr);
              }
            }
          }
        }
      }
    }
      break;
    case PL_OPAQUE_TYPE: {
      ObPLOpaque *opaque = reinterpret_cast<ObPLOpaque*>(src.get_ext());
      CK (OB_NOT_NULL(opaque));
      // json pl object manage
      if (OB_NOT_NULL(opaque) && opaque->is_json_type()) {
        ObPLJsonBaseType* pl_jsontype = static_cast<ObPLJsonBaseType*>(opaque);
        ObPlJsonNode* pl_json_node = pl_jsontype->get_data();
        if (OB_NOT_NULL(pl_json_node) && OB_NOT_NULL(pl_json_node->get_data_node())) {
          pl_jsontype->destroy();
        }
      }
      OX (opaque->~ObPLOpaque());
    }
      break;
#endif
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

int ObUserDefinedType::alloc_sub_composite(ObObj &dest_element, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

#define COPY_SUB_COLLECTION(TYPE) \
  do {  \
    if (OB_ISNULL(dest_composite = reinterpret_cast<ObPLComposite*>(allocator.alloc(old_composite->get_init_size())))) {  \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                \
      LOG_WARN("failed to alloc memory for collection", K(ret));      \
    } else {                                                          \
      TYPE *collection = static_cast<TYPE*>(dest_composite);                    \
      CK (OB_NOT_NULL(collection));                                   \
      LOG_INFO("src is: ", KP(old_composite), KP(dest_composite), K(old_composite->get_init_size()));                                   \
      OX (new(collection)TYPE(old_composite->get_id()));                         \
      OZ (collection->init_allocator(allocator, false));  \
      if (OB_FAIL(ret)) {    \
        allocator.free(dest_composite);     \
      }    \
    }     \
  } while (0)

  if (dest_element.is_ext() && dest_element.get_meta().get_extend_type() != PL_OPAQUE_TYPE) {
    ObPLComposite *old_composite = reinterpret_cast<ObPLComposite*>(dest_element.get_ext());
    ObPLComposite *dest_composite = nullptr;
    CK (OB_NOT_NULL(old_composite));
    if (OB_SUCC(ret)) {
      switch (old_composite->get_type()) {
        case PL_RECORD_TYPE: {
          ObPLRecord *composite = NULL;
          dest_composite = reinterpret_cast<ObPLComposite*>(allocator.alloc(old_composite->get_init_size()));
          composite = static_cast<ObPLRecord*>(dest_composite);
          int64_t record_count = static_cast<ObPLRecord*>(old_composite)->get_count();
          if (OB_ISNULL(composite)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate composite memory failed", K(ret));
          }
          OX (new(composite)ObPLRecord(old_composite->get_id(), record_count));
          OZ (composite->init_data(allocator, false));
          if (OB_FAIL(ret) && OB_NOT_NULL(composite)) {
            allocator.free(composite);
          }
        }
          break;
#ifdef OB_BUILD_ORACLE_PL
        case PL_NESTED_TABLE_TYPE: {
          COPY_SUB_COLLECTION(ObPLNestedTable);
        }
          break;
        case PL_ASSOCIATIVE_ARRAY_TYPE: {
          COPY_SUB_COLLECTION(ObPLAssocArray);
        }
          break;
        case PL_VARRAY_TYPE: {
          COPY_SUB_COLLECTION(ObPLVArray);
        }
          break;
#endif
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected type to destruct", K(dest_element), K(dest_element.get_meta().get_extend_type()), K(ret));
        }
          break;
      }
      OX (dest_element.set_extend(reinterpret_cast<int64_t>(dest_composite),
                                    dest_element.get_meta().get_extend_type(),
                                    dest_element.get_val_len()));
    }
  }
#undef COPY_SUB_COLLECTION
  return ret;
}

int ObUserDefinedType::alloc_for_second_level_composite(ObObj &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (dest.is_pl_extend()) {
    switch (dest.get_meta().get_extend_type()) {
    case PL_RECORD_TYPE: {
      ObPLRecord *record = reinterpret_cast<ObPLRecord*>(dest.get_ext());
      CK  (OB_NOT_NULL(record));
      if (OB_SUCC(ret) && OB_NOT_NULL(record->get_allocator())) {
        for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
          ObObj *dest_element = nullptr;
          OZ (record->get_element(i, dest_element));
          CK (OB_NOT_NULL(dest_element));
          OZ (alloc_sub_composite(*dest_element, *record->get_allocator()));
        }
      }
    }
      break;
#ifdef OB_BUILD_ORACLE_PL
    case PL_NESTED_TABLE_TYPE: //fallthrough
    case PL_ASSOCIATIVE_ARRAY_TYPE: //fallthrough
    case PL_VARRAY_TYPE: {
      ObPLCollection *collection = reinterpret_cast<ObPLCollection*>(dest.get_ext());
      CK  (OB_NOT_NULL(collection));
      if (OB_SUCC(ret) && OB_NOT_NULL(collection->get_allocator())) {
        for (int64_t i = 0; OB_SUCC(ret) && i < collection->get_count(); ++i) {
          OZ (alloc_sub_composite(collection->get_data()[i], *collection->get_allocator()));
        }
      }
    }
      break;
#endif
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected type to destruct", K(dest), K(dest.get_meta().get_extend_type()), K(ret));
    }
       break;
    }
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
#ifdef OB_BUILD_ORACLE_PL
#define SERIALIZE_COLLECTION(type, class) \
    case type: { \
      class *collection = reinterpret_cast<class*>(obj.get_ext()); \
      OZ (collection->serialize(buf, len, pos)); \
    } \
      break;

    SERIALIZE_COLLECTION(PL_NESTED_TABLE_TYPE, ObPLNestedTable)

    SERIALIZE_COLLECTION(PL_ASSOCIATIVE_ARRAY_TYPE, ObPLAssocArray)

    SERIALIZE_COLLECTION(PL_VARRAY_TYPE, ObPLVArray)

#undef SERIALIZE_COLLECTION
#endif
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
#ifdef OB_BUILD_ORACLE_PL
#define DESERIALIZE_COLLECTION(type, class) \
  case type: { \
    if (OB_SUCC(ret)) { \
      class *new_coll = NULL; \
      ObIAllocator &allocator = CURRENT_CONTEXT->get_arena_allocator(); \
      if (OB_ISNULL(new_coll = reinterpret_cast<class *>(allocator.alloc(sizeof(class))))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("failed to allocator memory for collection", K(ret)); \
      } else { \
        new(new_coll) class(id); \
        OX (new_coll->set_allocator(&allocator)); \
        OZ (new_coll->deserialize(allocator, buf, len, pos)); \
        OX (obj.set_extend(reinterpret_cast<int64_t>(new_coll), type)); \
      } \
    } \
  } \
    break;

  DESERIALIZE_COLLECTION(PL_NESTED_TABLE_TYPE, ObPLNestedTable)

  DESERIALIZE_COLLECTION(PL_ASSOCIATIVE_ARRAY_TYPE, ObPLAssocArray)

  DESERIALIZE_COLLECTION(PL_VARRAY_TYPE, ObPLVArray)

#undef DESERIALIZE_COLLECTION
#endif
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
#ifdef OB_BUILD_ORACLE_PL
#define COLLECTION_SERIALIZE_SIZE(type, class) \
    case type: { \
      class *collection = reinterpret_cast<class*>(obj.get_ext()); \
      OZ (collection->get_serialize_size(size)); \
    } \
      break;

    COLLECTION_SERIALIZE_SIZE(PL_NESTED_TABLE_TYPE, ObPLNestedTable)

    COLLECTION_SERIALIZE_SIZE(PL_ASSOCIATIVE_ARRAY_TYPE, ObPLAssocArray)

    COLLECTION_SERIALIZE_SIZE(PL_VARRAY_TYPE, ObPLVArray)

#undef COLLECTION_SERIALIZE_SIZE
#endif
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected type to get serialize size", K(obj), K(ret));
    }
      break;
    }
  }
  return size;
}

int ObUserDefinedType::text_protocol_prefix_info_for_each_item(share::schema::ObSchemaGetterGuard &schema_guard,
                                                               const ObPLDataType &type,
                                                               char *buf,
                                                               const int64_t len,
                                                               int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (type.is_collection_type() || type.is_record_type()) {
    const ObUserDefinedType *user_type = NULL;
    const ObUDTTypeInfo *udt_info = NULL;
    ObArenaAllocator local_allocator;
    const uint64_t tenant_id = get_tenant_id_by_object_id(type.get_user_type_id());
    const_cast<ObPLDataType&>(type).set_charset(get_charset());

    if (!is_udt_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support other type except udt type", K(ret), K(get_type_from()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-schema user defined type deserialize");
    } else if (OB_FAIL(schema_guard.get_udt_info(tenant_id, type.get_user_type_id(), udt_info))) {
      LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(type.get_user_type_id()));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt info is null", K(ret), K(type.get_user_type_id()));
    } else {
      if (len - pos < udt_info->get_type_name().length() + 1) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("buffer length is not enough. ", K(udt_info->get_type_name()), K(udt_info->get_type_name().length()), K(len));
      } else {
        MEMCPY(buf + pos, udt_info->get_type_name().ptr(), udt_info->get_type_name().length());
        pos += udt_info->get_type_name().length();
        MEMCPY(buf + pos, "(", 1);
        pos += 1;
      }
    }
  } else if (NULL != type.get_meta_type() && (type.get_meta_type()->is_string_or_lob_locator_type()
                || type.get_meta_type()->is_oracle_temporal_type()
                || type.get_meta_type()->is_raw())) {
    if (len - pos < 1) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer length is not enough. ", K(type_name_), K(type_name_.length()), K(len));
    } else {
      MEMCPY(buf + pos, "'", 1);
      pos += 1;
    }
  }

  return ret;
}

int ObUserDefinedType::text_protocol_suffix_info_for_each_item(const ObPLDataType &type,
                                                               char *buf,
                                                               const int64_t len,
                                                               int64_t &pos,
                                                               const bool is_last_item,
                                                               const bool is_null) const
{
  int ret = OB_SUCCESS;
  if (type.is_collection_type() || type.is_record_type()) {
    // reset charset
    const_cast<ObPLDataType&>(type).reset_charset();

    if (len - pos < 3) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer length is not enough. ", K(type_name_), K(type_name_.length()), K(len));
    } else if (!is_null) {
      MEMCPY(buf + pos, ")", 1);
      pos += 1;
    }

    if (OB_SUCC(ret) && !is_last_item) {
      MEMCPY(buf + pos, ", ", 2);
      pos += 2;
    }
  } else if (!is_null && NULL != type.get_meta_type() && (type.get_meta_type()->is_string_or_lob_locator_type()
                || type.get_meta_type()->is_oracle_temporal_type()
                || type.get_meta_type()->is_raw())) {
    if (len - pos < 3) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer length is not enough. ", K(type_name_), K(type_name_.length()), K(len));
    } else {
      MEMCPY(buf + pos, "'", 1);
      pos += 1;
      if (!is_last_item) {
        MEMCPY(buf + pos, ", ", 2);
        pos += 2;
      }
    }
  } else if (!is_last_item) {
    if (len - pos < 2) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer length is not enough. ", K(type_name_), K(type_name_.length()), K(len));
    } else {
      MEMCPY(buf + pos, ", ", 2);
      pos += 2;
    }
  }

  return ret;
}

int ObUserDefinedType::text_protocol_base_type_convert(const ObPLDataType &type, char *buf, int64_t &pos, int64_t len) const
{
  int ret = OB_SUCCESS;
  /* 1. base type is use cell string to convert to string type
   *    string type store as [length + value]
   *    we should remove length in buffer
   * 2. nchar/nvarchar need convert string with charset
   */
  uint64_t orign_str_length = 0; // the string length
  uint64_t inc_len = 0; // the length of [string length] in buffer
  const char *start = buf + pos; // all string begin
  uint32_t convert_length = 0;
  if (OB_FAIL(ObMySQLUtil::get_length(start, orign_str_length, inc_len))) {
    LOG_WARN("get length fail.", K(ret));
  } else {
    ObArenaAllocator alloc;
    char* tmp_buf = static_cast<char*>(alloc.alloc(orign_str_length));
    MEMCPY(tmp_buf, buf + pos + inc_len, orign_str_length);
    if (type.is_obj_type() && (OB_NOT_NULL(type.get_data_type()))
          && (type.get_data_type()->get_meta_type().is_string_or_lob_locator_type())) {
      // need do convert first
      if (OB_FAIL(observer::ObQueryDriver::convert_string_charset(ObString(orign_str_length, tmp_buf),
                                                        type.get_data_type()->get_collation_type(),
                                                        get_charset(),
                                                        buf + pos,
                                                        len - pos,
                                                        convert_length))) {
        LOG_WARN("convert string charset failed", K(ret));
      } else {
        pos += convert_length;
      }
    } else {
      // only remove length info in buf when type is not nchar/nvarchar
      MEMCPY(buf + pos, tmp_buf, orign_str_length);
      pos += orign_str_length;
    }
  }
  return ret;
}

int ObUserDefinedType::base_type_serialize_for_text(ObObj* obj,
                                                    const ObTimeZoneInfo *tz_info,
                                                    char *dst,
                                                    const int64_t dst_len,
                                                    int64_t &dst_pos,
                                                    bool &has_serialized) const
{
  int ret = OB_SUCCESS;
  has_serialized = true;
  if (NULL == obj) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj is null.", K(ret));
  } else if (ObOTimestampTC == obj->get_type_class()) {
    OZ (ObMySQLUtil::otimestamp_cell_str2(dst, dst_len, obj->get_otimestamp_value(), TEXT, dst_pos, tz_info,
      ObAccuracy::DML_DEFAULT_ACCURACY[obj->get_type()].get_scale(), obj->get_type()));
  } else if (obj->is_raw() || obj->is_hex_string()) {
    OZ (ObMySQLUtil::store_length(dst, dst_len, obj->get_string_len() * 2 + 1, dst_pos));
    OZ (to_hex_cstr(obj->get_string_ptr(), obj->get_string_len(), dst + dst_pos, dst_len - dst_pos));
    OX (dst_pos += obj->get_string_len() * 2 + 1);
  } else if (obj->is_lob() && NULL != obj->get_string_ptr()) {
    ObString lob_string;
    OZ (obj->get_string(lob_string));
    if (obj->is_blob()) {
      OZ (ObMySQLUtil::store_length(dst, dst_len, lob_string.length() * 2 + 1, dst_pos));
      OZ (to_hex_cstr(lob_string.ptr(), lob_string.length(), dst + dst_pos, dst_len - dst_pos));
      OX (dst_pos += (lob_string.length() * 2 + 1));
    } else {
      CK (obj->is_clob());
      OZ (ObMySQLUtil::store_length(dst, dst_len, lob_string.length(), dst_pos));
      OX (MEMCPY(dst + dst_pos, lob_string.ptr(), lob_string.length()));
      OX (dst_pos += lob_string.length());
    }
  } else {
    has_serialized = false;
  }
  return ret;
}

int ObUserDefinedType::generate_init_composite(ObPLCodeGenerator &generator,
                                                const ObPLINS &ns,
                                                jit::ObLLVMValue &value,
                                                const pl::ObPLStmt *stmt,
                                                jit::ObLLVMValue &allocator,
                                                bool is_record_type,
                                                bool is_top_level)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 3> args;
  ObLLVMValue ret_err;
  ObLLVMValue addr;
  ObLLVMType int_type;
  ObLLVMValue int_value, is_record, is_top;
  OZ (generator.get_helper().get_llvm_type(ObIntType, int_type));
  OZ (generator.get_helper().create_ptr_to_int(ObString("composite_to_int64"),
                                               value,
                                               int_type,
                                               int_value));
  OZ (args.push_back(allocator));
  OZ (args.push_back(int_value));
  OZ (generator.get_helper().get_int8(is_record_type, is_record));
  OZ (args.push_back(is_record));
  OZ (generator.get_helper().get_int8(is_top_level, is_top));
  OZ (args.push_back(is_top));
  OZ (generator.get_helper().create_call(ObString("spi_init_composite"),
                                         generator.get_spi_service().spi_init_composite_,
                                         args,
                                         ret_err));
  OZ (generator.check_success(ret_err,
                              stmt->get_stmt_id(),
                              stmt->get_block()->in_notfound(),
                              stmt->get_block()->in_warning()));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL

//---------- for ObUserDefinedSubType ----------

int ObUserDefinedSubType::deep_copy(common::ObIAllocator &alloc, const ObUserDefinedSubType &other)
{
  int ret = OB_SUCCESS;
  OZ (base_type_.deep_copy(alloc, other.base_type_));
  OZ (ObUserDefinedType::deep_copy(alloc, other));
  return ret;
}

int ObUserDefinedSubType::generate_copy(ObPLCodeGenerator &generator,
                                        const ObPLBlockNS &ns,
                                        jit::ObLLVMValue &allocator,
                                        jit::ObLLVMValue &src,
                                        jit::ObLLVMValue &dest,
                                        uint64_t location,
                                        bool in_notfound,
                                        bool in_warning,
                                        uint64_t package_id) const
{
  int ret = OB_SUCCESS;
  OZ (SMART_CALL(base_type_.generate_copy(
    generator, ns, allocator, src, dest, location, in_notfound, in_warning, package_id)));
  return ret;
}

int ObUserDefinedSubType::generate_construct(ObPLCodeGenerator &generator,
                                             const ObPLINS &ns,
                                             jit::ObLLVMValue &value,
                                             jit::ObLLVMValue &allocator,
                                             bool is_top_level,
                                             const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  OZ (SMART_CALL(base_type_.generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  return ret;
}

int ObUserDefinedSubType::generate_new(ObPLCodeGenerator &generator,
                                                const ObPLINS &ns,
                                                jit::ObLLVMValue &value,
                                                jit::ObLLVMValue &allocator,
                                                bool is_top_level,
                                                const pl::ObPLStmt *s) const
{
  int ret = OB_NOT_SUPPORTED;
  ret = ObUserDefinedType::generate_new(generator, ns, value, allocator, is_top_level, s);
  return ret;
}

int ObUserDefinedSubType::newx(common::ObIAllocator &allocator,
                               const ObPLINS *ns,
                               int64_t &ptr) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(allocator, ns, ptr);
  return ret;
}

int ObUserDefinedSubType::get_size(ObPLTypeSize type, int64_t &size) const
{
  int ret = OB_SUCCESS;
  OZ (base_type_.get_size(type, size));
  return ret;
}

int ObUserDefinedSubType::get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                                     const ObPLBlockNS &current_ns) const
{
  int ret = OB_SUCCESS;
  OZ (base_type_.get_all_depended_user_type(resolve_ctx, current_ns), base_type_);
  return ret;
}

int ObUserDefinedSubType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                                    const sql::ObSQLSessionInfo &session,
                                    const common::ObTimeZoneInfo *tz_info,
                                    obmysql::MYSQL_PROTOCOL_TYPE type,
                                    char *&src,
                                    char *dst,
                                    const int64_t dst_len,
                                    int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  OZ (base_type_.serialize(schema_guard, session, tz_info, type, src, dst, dst_len, dst_pos));
  return ret;
}

int ObUserDefinedSubType::deserialize(share::schema::ObSchemaGetterGuard &schema_guard,
                                      common::ObIAllocator &allocator,
                                      const common::ObCharsetType charset,
                                      const common::ObCollationType cs_type,
                                      const common::ObCollationType ncs_type,
                                      const common::ObTimeZoneInfo *tz_info,
                                      const char *&src,
                                      char *dst,
                                      const int64_t dst_len,
                                      int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  OZ (base_type_.deserialize(
    schema_guard, allocator, charset, cs_type, ncs_type, tz_info, src, dst, dst_len, dst_pos));
  return ret;
}

int ObUserDefinedSubType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  int ret = OB_SUCCESS;
  OZ (base_type_.convert(ctx, src, dst));
  return ret;
}
#endif

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
                                        jit::ObLLVMValue &allocator,
                                        bool is_top_level,
                                        const pl::ObPLStmt *stmt) const
{
  UNUSEDx(generator, ns, value, stmt);
  return OB_NOT_SUPPORTED;
}

int ObRefCursorType::generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              jit::ObLLVMValue &allocator,
                                              bool is_top_level,
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

int ObRefCursorType::get_size(ObPLTypeSize type, int64_t &size) const
{
  UNUSEDx(type, size);
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
  } else if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
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
  if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
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
      if (common::ObCharset::case_compat_mode_equal(
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
      if (common::ObCharset::case_compat_mode_equal(
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
    if (common::ObCharset::case_compat_mode_equal(
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
  return ObRecordType::get_data_offset(count);
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
                                            const ObPLINS &ns,
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
  ObObj null_obj;
  null_obj.set_null();
  const ObPLDataType *member_type = NULL;
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
    } else {
      ObSEArray<jit::ObLLVMValue, 2> args;
      ObLLVMType int_type;
      ObLLVMValue int_value, is_record, member_idx;
      if (OB_FAIL(generator.get_helper().get_llvm_type(ObIntType, int_type))) {
        LOG_WARN("failed to get_llvm_type", K(ret));
      } else if (OB_FAIL(generator.get_helper().create_ptr_to_int(ObString("cast_ptr_to_int64"), dest,
                                                                  int_type, int_value))) {
        LOG_WARN("failed to create ptr to int", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator.get_helper().get_int8(true, is_record))) {
        LOG_WARN("fail to get int8", K(ret));
      } else if (OB_FAIL(args.push_back(is_record))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator.get_helper().get_int32(i, member_idx))) {
        LOG_WARN("fail to get int8", K(ret));
      } else if (OB_FAIL(args.push_back(member_idx))) {
        LOG_WARN("push_back error", K(ret));
      } else {
        jit::ObLLVMValue ret_err;
        if (OB_FAIL(generator.get_helper().create_call(ObString("spi_reset_composite"),
            generator.get_spi_service().spi_reset_composite_, args, ret_err))) {
          LOG_WARN("failed to create call", K(ret));
        } else if (OB_FAIL(generator.check_success(ret_err))) {
          LOG_WARN("failed to check success", K(ret));
        } else if (OB_FAIL(generator.store_obj(null_obj, dest_elem))) {
          LOG_WARN("failed to create store", K(ret));
        }
      }
    }
  }
  OZ (generator.extract_isnull_ptr_from_record(dest, isnull_ptr));
  OZ (generator.get_helper().create_istore(TRUE, isnull_ptr));
  return ret;
}

int ObRecordType::generate_construct(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     jit::ObLLVMValue &allocator,
                                     bool is_top_level,
                                     const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  OZ (SMART_CALL(ObUserDefinedType::generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  OZ (SMART_CALL(generate_default_value(generator, ns, stmt, value, allocator, is_top_level)));
  return ret;
}

int ObRecordType::generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              jit::ObLLVMValue &allocator,
                                              bool is_top_level,
                                              const pl::ObPLStmt *s) const
{
  int ret = OB_NOT_SUPPORTED;
  ret = ObUserDefinedType::generate_new(generator, ns, value, allocator, is_top_level, s);
  return ret;
}


int ObRecordType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  ObPLRecord *record = NULL;
  ObObj *member = NULL;
  int64_t init_size = ObRecordType::get_init_size(get_member_count());
  record = reinterpret_cast<ObPLRecord*>(allocator.alloc(init_size));
  if (OB_ISNULL(record)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc record failed", K(ret));
  }
  OX (new (record)ObPLRecord(user_type_id_, get_member_count()));
  OZ (record->init_data(allocator, false));
  OX (ptr = reinterpret_cast<int64_t>(record));
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_member_count(); ++i) {
      CK (OB_NOT_NULL(get_member(i)));
      OZ (record->get_element(i, member));
      CK (OB_NOT_NULL(member));
      if (get_member(i)->is_obj_type()) {
        OX (new (member) ObObj(ObNullType));
      } else {
        int64_t init_size = OB_INVALID_SIZE;
        int64_t member_ptr = 0;
        OZ (get_member(i)->get_size(PL_TYPE_INIT_SIZE, init_size));
        OZ (get_member(i)->newx(*record->get_allocator(), ns, member_ptr));
        OX (member->set_extend(member_ptr, get_member(i)->get_type(), init_size));
      }
    }
    if (OB_FAIL(ret)) {
      ObObj tmp;
      tmp.set_extend(ptr, this->get_type(), init_size);
      ObUserDefinedType::destruct_objparam(allocator, tmp, nullptr);
      ptr = 0;
    }
  } else if (OB_NOT_NULL(record)) {
    allocator.free(record);
  }
  return ret;
}

int ObRecordType::generate_alloc_complex_addr(ObPLCodeGenerator &generator,
                                              int8_t type,
                                              int64_t user_type_id,
                                              int64_t init_size,
                                              jit::ObLLVMValue &value, //返回值是一个int64_t，代表extend的值
                                              jit::ObLLVMValue &allocator,
                                              const pl::ObPLStmt *s)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 8> args;
  ObLLVMValue var_idx, init_value;
  ObLLVMValue extend_ptr;
  ObLLVMValue ret_err;
  ObLLVMValue var_type, type_id;
  ObPLCGBufferGuard buffer_guard(generator);

  OZ (buffer_guard.get_int_buffer(extend_ptr));
  OZ (args.push_back(generator.get_vars().at(generator.CTX_IDX)));
  OZ (generator.get_helper().get_int8(type, var_type));
  OZ (args.push_back(var_type));
  OZ (generator.get_helper().get_int64(user_type_id, type_id));
  OZ (args.push_back(type_id));
  OZ (generator.get_helper().get_int64(OB_INVALID_INDEX, var_idx));
  OZ (args.push_back(var_idx));
  OZ (generator.get_helper().get_int32(init_size, init_value));
  OZ (args.push_back(init_value));
  OZ (args.push_back(extend_ptr));
  OZ (args.push_back(allocator));
  OZ (generator.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                         generator.get_spi_service().spi_alloc_complex_var_,
                                         args,
                                         ret_err));
  OZ (generator.check_success(ret_err,
                              s->get_stmt_id(),
                              s->get_block()->in_notfound(),
                              s->get_block()->in_warning()));

  OZ (generator.get_helper().create_load("load_extend_ptr", extend_ptr, value));
  return ret;
}

int ObRecordType::generate_default_value(ObPLCodeGenerator &generator,
                                         const ObPLINS &ns,
                                         const ObPLStmt *stmt,
                                         jit::ObLLVMValue &value,
                                         jit::ObLLVMValue &allocator,
                                         bool is_top_level) const
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
  ObLLVMValue obobj_res;
  ObLLVMValue ptr_elem;
  ObObj null_obj;

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
  OZ (ObUserDefinedType::generate_init_composite(generator, ns, value, stmt, allocator, true, is_top_level));
  OZ (generator.generate_debug("generate_default_value", value));
  //设置meta和数据
  null_obj.set_null();
  CK (OB_NOT_NULL(stmt));
  for (int64_t i = 0; OB_SUCC(ret) && i < get_record_member_count(); ++i) {
    ObLLVMValue result;
    ObPLCGBufferGuard buffer_guard(generator);

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

    OZ (buffer_guard.get_objparam_buffer(result));

    //设置数据
    if (OB_SUCC(ret)) {
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
        OZ (generator.extract_element_ptr_from_record(value,
                                                      get_record_member_count(),
                                                      i,
                                                      ptr_elem));
        OZ (generator.generate_debug("generate_extract_value", ptr_elem));
        if (OB_FAIL(ret)) {
        } else if (member->member_type_.is_obj_type() || OB_INVALID_INDEX != member->get_default()) {
          //不论基础类型还是复杂类型，如果有default，直接把default值存入即可
          if (OB_INVALID_INDEX != member->get_default()) {
            ObLLVMValue record_allocator;
            ObLLVMValue src_datum;
            ObLLVMValue dst_datum;
            OZ (generator.extract_allocator_from_record(value, record_allocator));
            OZ (generator.extract_obobj_ptr_from_objparam(result, src_datum));
            OZ (member->member_type_.generate_copy(generator,
                                                   stmt->get_block()->get_namespace(),
                                                   record_allocator,
                                                   src_datum,
                                                   ptr_elem,
                                                   stmt->get_location(),
                                                   stmt->get_block()->in_notfound(),
                                                   stmt->get_block()->in_warning(),
                                                   OB_INVALID_ID));
            OZ (generator.generate_check_not_null(*stmt,
                                                  member->member_type_.get_not_null(),
                                                  result));
          } else {
            OZ (generator.store_obj(null_obj, ptr_elem));
          }
          if (OB_SUCC(ret) && !member->member_type_.is_obj_type()) { // process complex null value
            ObLLVMBasicBlock null_branch;
            ObLLVMBasicBlock final_branch;
            ObLLVMValue p_type_value;
            ObLLVMValue type_value;
            ObLLVMValue is_null;
            ObLLVMValue record_allocator;
            ObLLVMValue extend_value;
            ObLLVMValue init_value;
            ObLLVMValue composite_value;
            ObLLVMType ir_type;
            ObLLVMType ir_pointer_type;
            int64_t init_size = OB_INVALID_SIZE;
            OZ (generator.get_helper().create_block(ObString("null_branch"), generator.get_func(), null_branch));
            OZ (generator.get_helper().create_block(ObString("final_branch"), generator.get_func(), final_branch));
            OZ (generator.extract_type_ptr_from_objparam(result, p_type_value));
            OZ (generator.get_helper().create_load(ObString("load_type"), p_type_value, type_value));
            OZ (generator.get_helper().create_icmp_eq(type_value, ObNullType, is_null));
            OZ (generator.get_helper().create_cond_br(is_null, null_branch, final_branch));
            // null branch
            OZ (generator.set_current(null_branch));
            OZ (generator.extract_allocator_from_record(value, record_allocator));
            OZ (ns.get_size(PL_TYPE_INIT_SIZE, member->member_type_, init_size));
            OZ (generator.get_helper().get_int32(init_size, init_value));
            OZ (generate_alloc_complex_addr(generator,
                                            member->member_type_.get_type(),
                                            member->member_type_.get_user_type_id(),
                                            init_size,
                                            extend_value,
                                            record_allocator,
                                            stmt));
            OZ (generator.get_helper().get_int8(member->member_type_.get_type(), type_value));
            OZ (generator.generate_set_extend(ptr_elem, type_value, init_value, extend_value));
            OZ (SMART_CALL(member->member_type_.generate_new(generator, ns, extend_value, record_allocator, false, stmt)));
            OZ (generator.generate_null(ObIntType, record_allocator));
            OZ (generator.get_llvm_type(member->member_type_, ir_type));
            OZ (ir_type.get_pointer_to(ir_pointer_type));
            OZ (generator.get_helper().create_int_to_ptr(ObString("cast_extend_to_ptr"), extend_value, ir_pointer_type, composite_value));
            OZ (member->member_type_.generate_assign_with_null(generator, ns, record_allocator, composite_value));
            OZ (generator.get_helper().create_br(final_branch));
            // final branch
            OZ (generator.set_current(final_branch));
          }
        } else { //复杂类型如果没有default，调用generate_new
          ObLLVMValue extend_value;
          ObLLVMValue type_value;
          ObLLVMValue init_value;
          ObLLVMValue record_allocator;
          int64_t init_size = OB_INVALID_SIZE;
          int64_t size = OB_INVALID_SIZE;
          OZ (generator.extract_allocator_from_record(value, record_allocator));
          OZ (ns.get_size(PL_TYPE_INIT_SIZE, member->member_type_, init_size));
          OZ (generator.get_helper().get_int32(init_size, init_value));
          OZ (generate_alloc_complex_addr(generator,
                                          member->member_type_.get_type(),
                                          member->member_type_.get_user_type_id(),
                                          init_size,
                                          extend_value,
                                          record_allocator,
                                          stmt));
          OZ (generator.get_helper().get_int8(member->member_type_.get_type(), type_value));
          OZ (generator.generate_set_extend(ptr_elem, type_value, init_value, extend_value));
          OZ (SMART_CALL(member->member_type_.generate_new(generator, ns, extend_value, record_allocator, false, stmt)));
        }
      }
    }
  }
  return ret;
}

int ObRecordType::get_size(ObPLTypeSize type, int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = get_data_offset(get_record_member_count());
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
  ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_INIT_SESSION_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  obj.set_null();
  if (OB_NOT_NULL(default_expr)) {
    ObObj calc_obj;
    OZ (ObSQLUtils::calc_sql_expression_without_row(exec_ctx, *default_expr, calc_obj, &tmp_allocator));
    CK (calc_obj.is_null() || calc_obj.is_pl_extend());
    if (OB_SUCC(ret) && calc_obj.is_pl_extend()) {
      OZ (ObUserDefinedType::deep_copy_obj(obj_allocator, calc_obj, obj));
    }
  }
  if (OB_FAIL(ret) || obj.is_pl_extend()) {
    // do nothing ...
  } else if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(obj_allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    ObPLRecord *record = reinterpret_cast<ObPLRecord*>(data);
    ObObj *member = NULL;
    MEMSET(data, 0, init_size);
    new (data) ObPLRecord(user_type_id_, record_members_.count());
    if (OB_FAIL(record->init_data(obj_allocator, true))) {
      obj_allocator.free(data);
    } else {
      obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size);
      for (int64_t i = 0; OB_SUCC(ret) && i < get_member_count(); ++i) {
        const ObRecordMember* record_member = get_record_member(i);
        const ObPLDataType* member_type = get_record_member_type(i);
        CK (OB_NOT_NULL(get_member(i)));
        OZ (record->get_element(i, member));
        CK (OB_NOT_NULL(member));
        CK (OB_NOT_NULL(record_member));
        CK (OB_NOT_NULL(member_type));
        if (OB_FAIL(ret)) {
        } else if (record_member->get_default() != OB_INVALID_INDEX) {
          uint64_t package_id = extract_package_id(get_user_type_id());
          int64_t expr_idx = record_member->get_default();
          ObObjParam result;
          OV (is_package_type(), OB_ERR_UNEXPECTED, KPC(this));
          OV (package_id != OB_INVALID_ID, OB_ERR_UNEXPECTED, KPC(this));
          OV (expr_idx != OB_INVALID_INDEX, OB_ERR_UNEXPECTED, KPC(this));
          OZ (sql::ObSPIService::spi_calc_package_expr_v1(resolve_ctx, exec_ctx, tmp_allocator, package_id, expr_idx, &result));
          if (OB_FAIL(ret)) {
          } else if (result.is_pl_extend()) {
            ObObj tmp;
            OZ (ObUserDefinedType::deep_copy_obj(*record->get_allocator(), result, tmp, false));
            OX (result = tmp);
            OX (*member = tmp);
          } else if (result.is_null() && !get_member(i)->is_obj_type()) {
            int64_t init_size = OB_INVALID_SIZE;
            int64_t member_ptr = 0;
            OZ (get_member(i)->get_size(PL_TYPE_INIT_SIZE, init_size));
            OZ (get_member(i)->newx(*record->get_allocator(), &resolve_ctx, member_ptr));
            OX (member->set_extend(member_ptr, get_member(i)->get_type(), init_size));
            if (OB_SUCC(ret) && get_member(i)->is_record_type()) {
              ObPLComposite *composite = reinterpret_cast<ObPLComposite *>(member_ptr);
              CK (OB_NOT_NULL(composite));
              OX (composite->set_null());
            }
          } else {
            ObObj tmp;
            OZ (common::deep_copy_obj(*record->get_allocator(), result, tmp));
            OX (result = tmp);
            OX (*member = result);
          }
        } else {
          if (get_member(i)->is_obj_type()) {
            OX (new (member) ObObj(ObNullType));
          } else {
            int64_t init_size = OB_INVALID_SIZE;
            int64_t member_ptr = 0;
            OZ (get_member(i)->get_size(PL_TYPE_INIT_SIZE, init_size));
            OZ (get_member(i)->newx(*record->get_allocator(), &resolve_ctx, member_ptr));
            OX (member->set_extend(member_ptr, get_member(i)->get_type(), init_size));
          }
        }
      }
      if (OB_FAIL(ret)) {
        ObUserDefinedType::destruct_objparam(obj_allocator, obj, &(resolve_ctx.session_info_));
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
  OV (record->get_count() == record_members_.count(), OB_ERR_WRONG_TYPE_FOR_VAR, KPC(record), K(record_members_));
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
  CK (record->get_count() == record_members_.count());
  OX (record->serialize(dst, dst_len, dst_pos));
  OZ (serialization::encode(dst, dst_len, dst_pos, record->get_count()));

  char *data = reinterpret_cast<char*>(record->get_element());
  CK (OB_NOT_NULL(data));
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
  int32_t count = OB_INVALID_COUNT;
  // when record be delete , type will be PL_INVALID_TYPE
  OX (record->deserialize(src, src_len, src_pos));
  if (OB_SUCC(ret) && record->get_type() != PL_INVALID_TYPE) {
    OZ (serialization::decode(src, src_len, src_pos, count));
    CK (count == record_members_.count());
    OX (record->set_count(count));

    dst = reinterpret_cast<char*>(record->get_element());
    CK (OB_NOT_NULL(dst));
    CK (OB_NOT_NULL(record->get_allocator()));
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      CK (OB_NOT_NULL(type));
      if (OB_SUCC(ret) && type->is_obj_type()) {
        ObObj &obj = record->get_element()[i];
        OZ (ObUserDefinedType::destruct_objparam(*record->get_allocator(), obj, nullptr));
      }
      OZ (type->deserialize(resolve_ctx, *record->get_allocator(), src, src_len, src_pos, dst));
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
  if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    ObPLRecord *record = reinterpret_cast<ObPLRecord*>(data);
    MEMSET(data, 0, init_size);
    new (data) ObPLRecord(get_user_type_id(), get_record_member_count());
    OZ (record->init_data(allocator, true));
    if (OB_FAIL(ret)) {
      allocator.free(data);
    } else {
      OX (obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size));
    }
  }
  return ret;
}

int ObRecordType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                            const sql::ObSQLSessionInfo &session,
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
  } else if (BINARY == protocl_type) {
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
      } else if (ObPLComposite::obj_is_null(obj)) {
        if (BINARY == protocl_type) {
          ObMySQLUtil::update_null_bitmap(bitmap, i);
          new_src += sizeof(ObObj);
        } else {
          if (dst_len - dst_pos < 4) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("size overflow", K(ret), K(dst_len), K(dst_pos));
          } else {
            MEMCPY(dst + dst_pos, "NULL", 4);
            dst_pos += 4;
            new_src += sizeof(ObObj);
          }
        }
      } else if (TEXT == protocl_type && OB_FAIL(text_protocol_prefix_info_for_each_item(schema_guard,
                                                                 *type,
                                                                 dst,
                                                                 dst_len - dst_pos,
                                                                 dst_pos))) {
        LOG_WARN("set text protocol prefix info fail.", K(ret), K(get_name()));
      } else if (type->is_collection_type()) {
#ifdef OB_BUILD_ORACLE_PL
        char *coll_src = reinterpret_cast<char*>(obj->get_ext());
        ObPLNestedTable *coll_table = reinterpret_cast<ObPLNestedTable *>(coll_src);
        CK (obj->is_ext());
        CK (OB_NOT_NULL(coll_table));
        CK (OB_NOT_NULL(coll_src));
        if (OB_FAIL(ret)) {
        } else if (BINARY == protocl_type && !coll_table->is_inited()) {
          ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else {
          OZ (type->serialize(schema_guard, session, tz_info, protocl_type, new_src, dst, dst_len, dst_pos));
        }
#endif
      } else {
        int64_t offset_dst_pos = dst_pos;
        bool has_serialized = false;
        if (TEXT == protocl_type && OB_FAIL(base_type_serialize_for_text(obj, tz_info, dst, dst_len, dst_pos, has_serialized))) {
          LOG_WARN("serialize for text fail.", K(ret), K(has_serialized));
        } else if (false == has_serialized) {
          OZ (type->serialize(schema_guard, session, tz_info, protocl_type, new_src, dst, dst_len, dst_pos),
                              K(i), KPC(this));
        }
        if (TEXT == protocl_type && !type->is_record_type()) {
          OZ (text_protocol_base_type_convert(*type, dst, offset_dst_pos, dst_len));
          OX (dst_pos = offset_dst_pos);
        }
      }
      if (TEXT == protocl_type && !obj->is_invalid_type()) {
        OZ (text_protocol_suffix_info_for_each_item(*type,
                                                    dst,
                                                    dst_len - dst_pos,
                                                    dst_pos,
                                                    i < record_members_.count() - 1 ? false : true,
                                                    ObPLComposite::obj_is_null(obj)));
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
  if (OB_ISNULL(record) || OB_ISNULL(record->get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("record is null", K(ret), KP(dst), KP(record));
  } else if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
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
          } else if (OB_FAIL(user_type->newx(*record->get_allocator(), &ns, ptr))) {
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
        }
        OX (new_dst_pos += sizeof(ObObj));
      } else if (OB_FAIL(type->deserialize(schema_guard, *record->get_allocator(), charset, cs_type, ncs_type,
                                           tz_info, src, new_dst, new_dst_len, new_dst_pos))) {
        LOG_WARN("deserialize record element type failed", K(i), K(*this), KP(src), KP(dst), K(dst_len), K(dst_pos), K(ret));
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
      LOG_DEBUG("deserialize record element type finished", K(ret), K(i), K(*this), KP(src), KP(dst), K(dst_len), K(dst_pos));
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
  if (OB_FAIL(ret)) {
  } else if (src->is_null() || src->get_ext() == 0) {
    dst->set_null();
  } else if (dst->is_null() || dst->get_ext() == 0) {
    int64_t ptr = 0;
    OZ (newx(ctx.allocator_, &ctx, ptr));
    OX (dst->set_extend(ptr, get_type(), get_init_size(get_member_count())));
  }
  CK (src->is_pl_extend() && ObPLType::PL_RECORD_TYPE == src->get_meta().get_extend_type());
  if (OB_SUCC(ret)) {
    ObPLComposite *src_composite = reinterpret_cast<ObPLComposite*>(src->get_ext());
    ObPLComposite *dst_composite = reinterpret_cast<ObPLComposite*>(dst->get_ext());
    ObPLRecord* src_record = static_cast<ObPLRecord*>(src_composite);
    ObPLRecord* dst_record = static_cast<ObPLRecord*>(dst_composite);
    CK (OB_NOT_NULL(src_composite) && src_composite->is_record());
    CK (OB_NOT_NULL(dst_composite) && dst_composite->is_record());
    CK (OB_NOT_NULL(src_record));
    CK (OB_NOT_NULL(dst_record));
    CK (OB_NOT_NULL(dst_record->get_allocator()));
    if (OB_SUCC(ret)) {
      ObPLResolveCtx resolve_ctx(*dst_record->get_allocator(),
                                  ctx.session_info_,
                                  ctx.schema_guard_,
                                  ctx.package_guard_,
                                  ctx.sql_proxy_,
                                  false);
      for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
        const ObPLDataType *type = get_record_member_type(i);
        ObObj* src_obj = NULL;
        ObObj *dst_obj = NULL;
        OZ (src_record->get_element(i, src_obj));
        OZ (dst_record->get_element(i, dst_obj));
        CK (OB_NOT_NULL(type));
        OZ (type->convert(resolve_ctx, src_obj, dst_obj));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
//---------- for ObOpaqueType ----------

int ObOpaqueType::get_size(ObPLTypeSize type, int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  if (PL_TYPE_INIT_SIZE == type) {
    ObPLOpaque opaque;
    size += opaque.get_init_size();
  } else {
    OZ (ObUserDefinedType::get_size(type, size));
  }
  return ret;
}

int ObOpaqueType::generate_construct(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     jit::ObLLVMValue &allocator,
                                     bool is_top_level,
                                     const pl::ObPLStmt *stmt) const
{
  UNUSEDx(generator, ns, value, stmt);
  return OB_SUCCESS;
}

int ObOpaqueType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  ObPLOpaque *opaque = NULL;
  ObPLOpaque tmp;
  int64_t init_size = tmp.get_init_size();
  UNUSED(ns);
  OX (opaque = reinterpret_cast<ObPLOpaque *>(allocator.alloc(init_size)));
  CK (OB_NOT_NULL(opaque));
  OX (new (opaque) ObPLOpaque());
  OX (ptr = reinterpret_cast<int64_t>(opaque));
  return ret;
}

int ObOpaqueType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                   ObIAllocator &obj_allocator,
                                   sql::ObExecContext &exec_ctx,
                                   const sql::ObSqlExpression *default_expr,
                                   bool default_construct,
                                   ObObj &obj) const
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t init_size = 0;
  ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_INIT_SESSION_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  UNUSEDx(exec_ctx, default_construct);
  if (OB_NOT_NULL(default_expr)) {
    ObObj calc_obj;
    OZ (ObSQLUtils::calc_sql_expression_without_row(exec_ctx, *default_expr, calc_obj, &tmp_allocator));
    CK (calc_obj.is_null() || calc_obj.is_pl_extend());
    if (OB_SUCC(ret) && calc_obj.is_pl_extend()) {
      OZ (ObUserDefinedType::deep_copy_obj(obj_allocator, calc_obj, obj));
    }
  }
  if (OB_FAIL(ret) || obj.is_pl_extend()) {
    // do nothing ...
  } else if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(obj_allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for opaque type", K(ret), K(init_size));
  } else {
    MEMSET(data, 0, init_size);
    new (data) ObPLOpaque();
    obj.set_extend(reinterpret_cast<int64_t>(data), PL_OPAQUE_TYPE);
  }
  return ret;
}

int ObOpaqueType::free_session_var(const ObPLResolveCtx &resolve_ctx,
                                   ObIAllocator &obj_allocator,
                                   ObObj &obj) const
{
  int ret = OB_SUCCESS;
  char *data = reinterpret_cast<char *>(obj.get_ext());
  UNUSED(resolve_ctx);
  if (OB_NOT_NULL(data)) {
    obj_allocator.free(data);
  }
  obj.set_null();
  return ret;
}

int ObOpaqueType::generate_assign_with_null(ObPLCodeGenerator &generator,
                                            const ObPLINS &ns,
                                            jit::ObLLVMValue &allocator,
                                            jit::ObLLVMValue &dest) const
{
  int ret = OB_SUCCESS;
  jit::ObLLVMType int_type;
  jit::ObLLVMValue ret_err;
  jit::ObLLVMValue dest_addr;
  ObSEArray<ObLLVMValue, 1> args;
  OZ (generator.get_helper().get_llvm_type(ObIntType, int_type));
  OZ (generator.get_helper().create_ptr_to_int(ObString("cast_ptr_to_int64"), dest, int_type, dest_addr));
  OZ (args.push_back(dest_addr));
  OZ (generator.get_helper().create_call(ObString("spi_opaque_assign_null"),
                                         generator.get_spi_service().spi_opaque_assign_null_,
                                         args,
                                         ret_err));
  OZ (generator.check_success(ret_err));
  return ret;
}

//---------- for ObCollectionType ----------

int ObCollectionType::deep_copy(common::ObIAllocator &alloc, const ObCollectionType &other)
{
  int ret = OB_SUCCESS;
  OZ (ObUserDefinedType::deep_copy(alloc, other));
  OZ (element_type_.deep_copy(alloc, other.get_element_type()));
  return ret;
}

int ObCollectionType::generate_construct(ObPLCodeGenerator &generator,
                                         const ObPLINS &ns,
                                         jit::ObLLVMValue &value,
                                         jit::ObLLVMValue &allocator,
                                         bool is_top_level,
                                         const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  ObLLVMValue type_ptr;
  ObLLVMValue id_ptr;
  ObLLVMValue isnull_ptr;
  ObLLVMValue element_type_ptr;
  ObLLVMValue rowsize_ptr;
  ObLLVMValue count_ptr;
  ObLLVMValue first_ptr;
  ObLLVMValue last_ptr;
  ObLLVMValue notnull_ptr;
  ObElemDesc elem_desc;
  OZ (SMART_CALL(ObUserDefinedType::generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  OZ (ObUserDefinedType::generate_init_composite(generator, ns, value, stmt, allocator, false, is_top_level));
  OZ (generator.extract_type_ptr_from_collection(value, type_ptr));
  OZ (generator.get_helper().create_istore(type_, type_ptr));
  OZ (generator.extract_id_ptr_from_collection(value, id_ptr));
  OZ (generator.get_helper().create_istore(user_type_id_, id_ptr));
  OZ (generator.extract_isnull_ptr_from_collection(value, isnull_ptr));
  OZ (generator.get_helper().create_istore(FALSE, isnull_ptr));
  OZ (generator.extract_element_ptr_from_collection(value, element_type_ptr));

  if (NULL == element_type_.get_data_type()) { //复杂类型
    OX (elem_desc.set_obj_type(ObExtendType));
    const ObUserDefinedType *user_type = NULL;
    OZ (ns.get_user_type(element_type_.get_user_type_id(), user_type, NULL));
    CK (OB_NOT_NULL(user_type));
    if (OB_SUCC(ret)) {
      if (user_type->is_record_type()) {
        OX (elem_desc.set_field_count(
            static_cast<const ObRecordType*>(user_type)->get_member_count()));
      } else {
        OX (elem_desc.set_field_count(1));
      }
      OX (elem_desc.set_udt_id(element_type_.get_user_type_id()));
    }
  } else { //基础类型
    OX (elem_desc.set_meta_type(element_type_.get_data_type()->get_meta_type()));
    OX (elem_desc.set_accuracy(element_type_.get_data_type()->get_accuracy()));
    OX (elem_desc.set_field_count(1));
  }
  OX (elem_desc.set_pl_type(element_type_.get_type()));
  OX (elem_desc.set_not_null(element_type_.get_not_null()));
  OZ (generator.store_elem_desc(elem_desc, element_type_ptr));
  OZ (generator.extract_count_ptr_from_collection(value, count_ptr));
  OZ (generator.get_helper().create_istore(is_associative_array_type() ? 0 : OB_INVALID_COUNT, count_ptr));
  OZ (generator.extract_first_ptr_from_collection(value, first_ptr));
  OZ (generator.get_helper().create_istore(OB_INVALID_INDEX, first_ptr));
  OZ (generator.extract_last_ptr_from_collection(value, last_ptr));
  OZ (generator.get_helper().create_istore(OB_INVALID_INDEX, last_ptr));
  return ret;
}

int ObCollectionType::generate_new(ObPLCodeGenerator &generator,
                                              const ObPLINS &ns,
                                              jit::ObLLVMValue &value,
                                              jit::ObLLVMValue &allocator,
                                              bool is_top_level,
                                              const pl::ObPLStmt *s) const
{
  int ret = OB_SUCCESS;
  ret = ObUserDefinedType::generate_new(generator, ns, value, allocator, is_top_level, s);
  return ret;
}


int ObCollectionType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{

#define COLLECTION_NEWX(class) \
  do { \
    if (OB_SUCC(ret)) { \
      class *table = NULL; \
      ObPLAllocator1 *collection_allocator = NULL; \
      OX (table = reinterpret_cast<class*>(allocator.alloc(sizeof(class)))); \
      OX (collection_allocator \
        = reinterpret_cast<ObPLAllocator1*>(allocator.alloc(sizeof(ObPLAllocator1)))); \
      if (OB_ISNULL(table) || OB_ISNULL(collection_allocator)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("failed to alloc memory", K(ret)); \
      } \
      OX (new (table)class(user_type_id_)); \
      OX (collection_allocator = new(collection_allocator)ObPLAllocator1(PL_MOD_IDX::OB_PL_COLLECTION, &allocator)); \
      OZ (collection_allocator->init(&allocator));  \
      OX (table->set_allocator(collection_allocator)); \
      if (OB_SUCC(ret)) { \
        ObElemDesc elem_desc; \
        elem_desc.set_pl_type(element_type_.get_type()); \
        elem_desc.set_not_null(element_type_.get_not_null()); \
        if (OB_ISNULL(element_type_.get_data_type())) { \
          int64_t field_cnt = OB_INVALID_COUNT; \
          elem_desc.set_obj_type(common::ObExtendType); \
          OZ (element_type_.get_field_count(*ns, field_cnt)); \
          OX (elem_desc.set_field_count(field_cnt)); \
          OX (elem_desc.set_udt_id(element_type_.get_user_type_id())); \
        } else { \
          elem_desc.set_data_type(*(element_type_.get_data_type())); \
          elem_desc.set_field_count(1); \
        } \
        OX (table->set_element_desc(elem_desc));       \
      } \
      OX (ptr = reinterpret_cast<int64_t>(table)); \
      if (OB_FAIL(ret)) {    \
        if (OB_NOT_NULL(collection_allocator)) {  \
          allocator.free(collection_allocator);  \
          collection_allocator = nullptr;   \
        }   \
        if (OB_NOT_NULL(table)) {  \
          allocator.free(table);   \
          table = nullptr;  \
        }   \
      }  \
    } \
  } while (0)

  int ret = OB_SUCCESS;
  switch (get_type()) {
  case PL_NESTED_TABLE_TYPE: {
    COLLECTION_NEWX(ObPLNestedTable);
  }
    break;
  case PL_ASSOCIATIVE_ARRAY_TYPE: {
    COLLECTION_NEWX(ObPLAssocArray);
  }
    break;
  case PL_VARRAY_TYPE: {
    COLLECTION_NEWX(ObPLVArray);
  }
    break;
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected composite to copy", K(get_type()), K(ret));
  }
    break;
  }

#undef COLLECTION_NEWX

  //TODO:@ryan.ly
  UNUSED(ns);
  return ret;
}

int ObCollectionType::get_init_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (is_associative_array_type()) {
    size += sizeof(ObPLAssocArray) + 8;
  } else if (is_varray_type()) {
    size += sizeof(ObPLVArray) + 8;
  } else if (is_nested_table_type()) {
    size += sizeof(ObPLNestedTable) + 8;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support collection type in get_size", K(ret), K(type_));
  }
  return ret;
}

int ObCollectionType::get_size(ObPLTypeSize type, int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  if (PL_TYPE_ROW_SIZE == type) {
    OZ (get_element_type().get_size(type, size));
  } else if (PL_TYPE_INIT_SIZE == type) {
    OZ (get_init_size(size));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support pl type size", K(ret), K(type));
  }
  return ret;
}

int ObCollectionType::generate_assign_with_null(ObPLCodeGenerator &generator,
                                                const ObPLINS &ns,
                                                jit::ObLLVMValue &allocator,
                                                jit::ObLLVMValue &dest) const
{
  UNUSED(allocator); UNUSED(ns);
  int ret = OB_SUCCESS;

  ObSEArray<jit::ObLLVMValue, 1> args;
  ObLLVMValue isnull_ptr;
  ObLLVMType int_type;
  ObLLVMValue int_value, is_record, member_idx;

  if (OB_FAIL(generator.get_helper().get_llvm_type(ObIntType, int_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(generator.get_helper().create_ptr_to_int(ObString("cast_ptr_to_int64"), dest,
                                                              int_type, int_value))) {
    LOG_WARN("failed to create ptr to int", K(ret));
  } else if (OB_FAIL(args.push_back(int_value))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(generator.get_helper().get_int8(false, is_record))) {
    LOG_WARN("fail to get int8", K(ret));
  } else if (OB_FAIL(args.push_back(is_record))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(generator.get_helper().get_int32(-1, member_idx))) {
    LOG_WARN("fail to get int8", K(ret));
  } else if (OB_FAIL(args.push_back(member_idx))) {
    LOG_WARN("push_back error", K(ret));
  } else {
    jit::ObLLVMValue ret_err;
    if (OB_FAIL(generator.get_helper().create_call(ObString("spi_reset_composite"),
        generator.get_spi_service().spi_reset_composite_, args, ret_err))) {
      LOG_WARN("failed to create call", K(ret));
    } else if (OB_FAIL(generator.check_success(ret_err))) {
      LOG_WARN("failed to check success", K(ret));
    } else { /*do nothing*/ }
  }
  OZ (generator.extract_isnull_ptr_from_record(dest, isnull_ptr));
  OZ (generator.get_helper().create_istore(TRUE, isnull_ptr));
  return ret;
}

/*
int ObCollectionType::set_row_size(ObPLCodeGenerator &generator, const ObPLINS &ns, ObLLVMValue &collection) const
{
  int ret = OB_SUCCESS;
  int64_t rowsize = 0;
  ObLLVMValue p_rowsize;
  OZ (generator.extract_rowsize_ptr_from_collection(collection, p_rowsize));
  OZ (get_size(PL_TYPE_ROW_SIZE, rowsize));
  OZ (generator.get_helper().create_istore(rowsize, p_rowsize));
  return ret;
}
*/

int ObCollectionType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                       common::ObIAllocator &obj_allocator,
                                       sql::ObExecContext &exec_ctx,
                                       const sql::ObSqlExpression *default_expr,
                                       bool default_construct,
                                       ObObj &obj) const
{
  UNUSEDx(exec_ctx, default_expr);
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_INIT_SESSION_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  char *data = NULL;
  int64_t init_size = 0;
  int64_t row_size = 0;
  obj.set_null();
  if (OB_NOT_NULL(default_expr) && !default_construct) {
    ObObj calc_obj;
    OZ (ObSQLUtils::calc_sql_expression_without_row(exec_ctx, *default_expr, calc_obj, &tmp_allocator));
    CK (calc_obj.is_null() || calc_obj.is_pl_extend());
    if (OB_SUCC(ret) && calc_obj.is_pl_extend()) {
      OZ (ObUserDefinedType::deep_copy_obj(obj_allocator, calc_obj, obj));
    }
  }
  if (OB_FAIL(ret) || obj.is_pl_extend()) {
    // do nothing ...
  } else if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_FAIL(get_size(PL_TYPE_ROW_SIZE, row_size))) {
    LOG_WARN("get row size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(obj_allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    ObPLCollection *coll = NULL;
    if (is_associative_array_type()) {
      coll = new(data) ObPLAssocArray(user_type_id_);
    } else if (is_nested_table_type()) {
      coll = new(data) ObPLNestedTable(user_type_id_);
    } else if (is_varray_type()) {
      coll = new(data) ObPLVArray(user_type_id_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected collection type", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObElemDesc elem_desc;
      elem_desc.set_pl_type(element_type_.get_type());
      elem_desc.set_not_null(element_type_.get_not_null());
      if (OB_ISNULL(element_type_.get_data_type())) {
        int64_t field_cnt = OB_INVALID_COUNT;
        elem_desc.set_obj_type(common::ObExtendType);
        OZ (element_type_.get_field_count(resolve_ctx, field_cnt));
        OX (elem_desc.set_field_count(field_cnt));
        OX (elem_desc.set_udt_id(element_type_.get_user_type_id()));
      } else {
        elem_desc.set_data_type(*(element_type_.get_data_type()));
        elem_desc.set_field_count(1);
      }
      CK (OB_NOT_NULL(coll));
      OX (coll->set_element_desc(elem_desc));
    }
    OZ (coll->init_allocator(obj_allocator, true));
    if (OB_FAIL(ret)) {
      obj_allocator.free(data);
    } else {
      obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size);
      // coll->set_allocator(&obj_allocator);// package variable的初始化使用外层的allocator, 避免内存泄漏
      CK (OB_NOT_NULL(exec_ctx.get_my_session()));
      OZ (ObSPIService::spi_set_collection(exec_ctx.get_my_session()->get_effective_tenant_id(),
                                            &resolve_ctx,
                                            obj_allocator,
                                            *coll,
                                            0,
                                            false));
      OX (default_construct ? coll->set_inited() : void(NULL));
      if (OB_FAIL(ret)) {
        ObUserDefinedType::destruct_objparam(obj_allocator, obj, &(resolve_ctx.session_info_));
      }
    }
  }
  return ret;
}

// --------- for session serialize/deserialize interface ---------
int ObCollectionType::get_serialize_size(
    const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  int ret = OB_SUCCESS;
  ObPLCollection *table = reinterpret_cast<ObPLCollection *>(src);
  char *data = NULL;
  CK (OB_NOT_NULL(table));

  OX (size += static_cast<ObPLComposite*>(table)->get_serialize_size());
  OX (size += table->get_element_desc().get_serialize_size());
  OX (size += serialization::encoded_length(table->get_count()));
  if (is_associative_array_type() && (GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_1 ||
     (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1))) {
    ObPLAssocArray *assoc_table = static_cast<ObPLAssocArray *>(table);
    CK (OB_NOT_NULL(assoc_table));
    OX (size += serialization::encoded_length(assoc_table->get_first()));
    OX (size += serialization::encoded_length(assoc_table->get_last()));
  } else {
    OX (size += serialization::encoded_length(table->get_pure_first()));
    OX (size += serialization::encoded_length(table->get_pure_last()));
  }

  OX (data = reinterpret_cast<char*>(table->get_data()));
  for (int64_t i = 0; OB_SUCC(ret) && i < table->get_count(); ++i) {
    ObObj* obj = reinterpret_cast<ObObj*>(data);
    CK (OB_NOT_NULL(obj));
    if (OB_FAIL(ret)) {
    } else if (element_type_.is_composite_type() && ObMaxType == obj->get_type()) {
      ObPLComposite composite;
      OX (size += composite.get_serialize_size());
      OX (data += sizeof(ObObj));
    } else {
      OZ (element_type_.get_serialize_size(resolve_ctx, data, size));
    }
  }
  return ret;
}

int ObCollectionType::serialize(
  const ObPLResolveCtx &resolve_ctx,
  char *&src, char *dst, int64_t dst_len, int64_t &dst_pos) const
{
#define ENCODE(v) \
  OZ (serialization::encode(dst, dst_len, dst_pos, v));

  int ret = OB_SUCCESS;
  ObPLCollection *table = reinterpret_cast<ObPLCollection *>(src);

  CK (OB_NOT_NULL(table));

  OV (table->get_column_count() > 0, OB_ERR_UNEXPECTED, KPC(table));

  OX (static_cast<ObPLComposite*>(table)->serialize(dst, dst_len, dst_pos));
  OX (table->get_element_desc().serialize(dst, dst_len, dst_pos));
  ENCODE(table->get_count());
  if (is_associative_array_type() && (GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_1 ||
     (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1))) {
    ObPLAssocArray *assoc_table = static_cast<ObPLAssocArray *>(table);
    CK (OB_NOT_NULL(assoc_table));
    ENCODE(assoc_table->get_first());
    ENCODE(assoc_table->get_last());
  } else {
    ENCODE(table->get_pure_first());
    ENCODE(table->get_pure_last());
  }

  if (OB_SUCC(ret)) {
    char *data = reinterpret_cast<char *>(table->get_data());
    for (int64_t i = 0; OB_SUCC(ret) && i < table->get_count(); ++i) {
      ObObj *obj = reinterpret_cast<ObObj*>(data);
      CK (OB_NOT_NULL(obj));
      if (OB_FAIL(ret)) {
      } else if (element_type_.is_composite_type() && ObMaxType == obj->get_type()) {
        // deleted element
        ObPLComposite composite;
        OZ (composite.serialize(dst, dst_len, dst_pos));
        OX (data += sizeof(ObObj));
      } else {
        OZ (element_type_.serialize(resolve_ctx, data, dst, dst_len, dst_pos));
      }
    }
  }
  return ret;

#undef ENCODE
}

int ObCollectionType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char *src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(resolve_ctx, allocator, src, src_len, src_pos, dst);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
#define DECODE(v) \
  OZ (serialization::decode(src, src_len, src_pos, v));

  ObPLCollection *table = reinterpret_cast<ObPLCollection *>(dst);
  int64_t count = 0;
  int64_t first = 0;
  int64_t last = 0;

  CK (OB_NOT_NULL(table));

  OZ (static_cast<ObPLComposite*>(table)->deserialize(src, src_len, src_pos));
  // delete element will deserialize to a invalid composite
  // when see invalid composte, stop table deserialize
  if (OB_SUCC(ret) && table->get_type() != PL_INVALID_TYPE) {
    OZ (table->get_element_desc().deserialize(src, src_len, src_pos));
    DECODE(count);
    DECODE(first);
    DECODE(last);

    UNUSED(allocator);
    CK (OB_NOT_NULL(table->get_allocator()));
    if (OB_FAIL(ret)) {
    } else if (count <= 0) {
      if (table->get_count() > 0) {
        ObObj tmp;
        tmp.set_extend(reinterpret_cast<int64_t>(table), table->get_type());
        OZ (ObUserDefinedType::destruct_obj(tmp, &resolve_ctx.session_info_, true));
      }
      OX (table->set_count(count));
    } else if (is_associative_array_type()) {
      ObPLAssocArray *assoc_table = static_cast<ObPLAssocArray *>(table);
      CK (OB_NOT_NULL(assoc_table));
      CK (OB_NOT_NULL(table->get_allocator()));
      OZ (ObSPIService::spi_extend_assoc_array(
        OB_INVALID_ID, &resolve_ctx, *(table->get_allocator()), *assoc_table, count));
    } else {
      if (table->get_count() > 0) {
        ObObj tmp;
        tmp.set_extend(reinterpret_cast<int64_t>(table), table->get_type());
        ObUserDefinedType::destruct_obj(tmp, &resolve_ctx.session_info_, true);
      }
      OX (table->set_count(0));
      OZ (ObSPIService::spi_set_collection(
        OB_INVALID_ID, &resolve_ctx, *table->get_allocator(), *table, count, true));
    }

    if (OB_SUCC(ret)) {
      char *table_data = reinterpret_cast<char*>(table->get_data());
      // if element type is schema varray type, udt will mark it as nested table, which cause element type is not correct.
      ObPLType elem_type = PL_INVALID_TYPE;
      if (count > 0) {
        ObObj* elem_obj = reinterpret_cast<ObObj*>(table_data);
        if (elem_obj->is_ext()) {
          ObPLComposite* elem_composite = reinterpret_cast<ObPLComposite*>(elem_obj->get_ext());
          CK (OB_NOT_NULL(elem_composite));
          OX (elem_type = elem_composite->get_type());
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObObj* obj = reinterpret_cast<ObObj*>(table_data);
        CK (OB_NOT_NULL(table->get_allocator()));
        OZ (element_type_.deserialize(
          resolve_ctx, *(table->get_allocator()), src, src_len, src_pos, table_data));
        if (OB_SUCC(ret) && obj->is_ext()) {
          ObPLComposite* composite = reinterpret_cast<ObPLComposite*>(obj->get_ext());
          CK (OB_NOT_NULL(composite));
          if (OB_SUCC(ret) && composite->get_type() == PL_INVALID_TYPE) {
            composite->set_type(elem_type);
            composite->set_is_null(!element_type_.get_not_null());
            composite->set_id(element_type_.get_user_type_id());
            OZ (ObUserDefinedType::destruct_objparam(*(table->get_allocator()), *obj, nullptr));
            OX (obj->set_type(ObMaxType));
          }
        }
      }
    }

    OX (table->set_first(first));
    OX (table->set_last(last));
  }
#endif
  return ret;

#undef DECODE
}

int ObCollectionType::add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                                  const ObPLBlockNS &block_ns,
                                                  const common::ObString &package_name,
                                                  const common::ObString &param_name,
                                                  int64_t mode, int64_t position,
                                                  int64_t level, int64_t &sequence,
                                                  share::schema::ObRoutineInfo &routine_info) const
{
  UNUSEDx(param_name, position);
  int ret = OB_SUCCESS;
  ObString empty_param_name;
  if (OB_FAIL(element_type_.add_package_routine_schema_param(resolve_ctx, block_ns, package_name, empty_param_name,
      mode, 1, level+1, sequence, routine_info))) {
     LOG_WARN("failed to add routine schema param", K(*this), K(ret));
  }
  return ret;
}

int ObCollectionType::get_all_depended_user_type(
  const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS &current_ns) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(element_type_.get_all_depended_user_type(resolve_ctx, current_ns))) {
    LOG_WARN("element type get depended user type failed", K(ret));
  }
  return ret;
}

int ObCollectionType::init_obj(ObSchemaGetterGuard &schema_guard,
                               ObIAllocator &allocator,
                               ObObj &obj,
                               int64_t &init_size) const
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  init_size = 0;
  if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get init size failed", K(ret));
  } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory allocate failed", K(ret));
  } else {
    MEMSET(data, 0, init_size);
    if (is_varray_type()) {
      new (data) ObPLVArray(get_user_type_id());
    } else if (is_associative_array_type()) {
      new (data) ObPLAssocArray(get_user_type_id());
    } else {
      new (data) ObPLCollection(get_type(), get_user_type_id());
    }
    ObPLCollection *coll = reinterpret_cast<ObPLCollection *>(data);
    CK (OB_NOT_NULL(coll));
    OZ (coll->init_allocator(allocator, true));
    if (OB_FAIL(ret)) {
      allocator.free(data);
    } else {
      OX (obj.set_extend(reinterpret_cast<int64_t>(data), type_, init_size));
      LOG_DEBUG("success to init obj", K(*this), K(init_size), K(data));
    }
  }
  return ret;
}

int ObCollectionType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                                const sql::ObSQLSessionInfo &session,
                                const ObTimeZoneInfo *tz_info,
                                MYSQL_PROTOCOL_TYPE type,
                                char *&src,
                                char *dst,
                                const int64_t dst_len,
                                int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  ObObj *src_obj = NULL;
  ObPLNestedTable *table = NULL;
  if (OB_ISNULL(src_obj = reinterpret_cast<ObObj*>(src))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src is null", K(ret), KP(src_obj), KPC(this));
  } else if (!src_obj->is_ext()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src obj not pl extend", K(ret), KPC(src_obj), KPC(this));
  } else if (OB_ISNULL(table
      = reinterpret_cast<ObPLNestedTable *>(src_obj->get_ext()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret), KPC(table), KPC(this));
  } else if (!table->is_inited()) {
    // table未初始化应该序列化为null, 空在空值位图中标识, 上层已经处理过空值位图, 这里什么都不做
  } else if (BINARY == type && OB_FAIL(ObMySQLUtil::store_length(dst, dst_len, table->get_actual_count(), dst_pos))) {
    LOG_WARN("failed to stroe_length for table count", K(ret), KPC(this), KPC(table), K(table->get_count()));
  } else {
    char* bitmap = NULL;
    int64_t bitmap_bytes = (table->get_actual_count() + 7 + 2) / 8;
    if (BINARY == type) {
      // 计算空值位图位置
      if ((dst_len - dst_pos) < bitmap_bytes) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size overflow", K(ret), KPC(this), KPC(table), K(dst_len), K(dst_pos), K(bitmap_bytes));
      } else {
        bitmap = dst + dst_pos;
        MEMSET(dst + dst_pos, 0, bitmap_bytes);
        dst_pos += bitmap_bytes;
      }
    } else {
      // do nothing
    }
    // 序列化值并更新空值位图
    for (int64_t i = 0; OB_SUCC(ret) && i < table->get_count(); ++i) {
      char *data = reinterpret_cast<char *>(table->get_data()) + (sizeof(ObObj) * i);
      ObObj* obj = reinterpret_cast<ObObj*>(data);
      CK (OB_NOT_NULL(obj));
      if (OB_FAIL(ret)) {
      } else if (obj->is_invalid_type()) {
        // deleted element, do nothing...
      } else if (ObPLComposite::obj_is_null(obj)) {
        if (BINARY == type) {
          ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else {
          if (dst_len - dst_pos < 4) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("size overflow", K(ret), K(dst_len), K(dst_pos));
          } else {
            MEMCPY(dst + dst_pos, "NULL", 4);
            dst_pos += 4;
          }
        }
      } else if (TEXT == type && OB_FAIL(text_protocol_prefix_info_for_each_item(schema_guard,
                                                                 element_type_,
                                                                 dst,
                                                                 dst_len - dst_pos,
                                                                 dst_pos))) {
        LOG_WARN("set text protocol prefix info fail.", K(ret), K(get_name()));
      } else if (element_type_.is_collection_type()) {
        char *coll_src = reinterpret_cast<char *>(obj->get_ext());
        ObPLNestedTable *coll_table = reinterpret_cast<ObPLNestedTable *>(coll_src);
        OV (obj->is_ext(), OB_ERR_UNEXPECTED, KP(obj), KP(data), K(i));
        CK (OB_NOT_NULL(coll_src));
        CK (OB_NOT_NULL(coll_table));
        if (OB_FAIL(ret)) {
        } else if (BINARY == type && !coll_table->is_inited()) {
          ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else {
          OZ (element_type_.serialize(schema_guard, session, tz_info, type, data, dst, dst_len, dst_pos), KPC(this), K(i));
        }
      } else {
        int64_t offset_dst_pos = dst_pos;
        bool has_serialized = false;
        if (TEXT == type && OB_FAIL(base_type_serialize_for_text(obj, tz_info, dst, dst_len, dst_pos, has_serialized))) {
          LOG_WARN("serialize for text fail.", K(ret), K(has_serialized));
        } else if (false == has_serialized) {
          OZ (element_type_.serialize(schema_guard, session, tz_info, type, data, dst, dst_len, dst_pos), KPC(this), K(i));
        }
        if (TEXT == type && !element_type_.is_record_type()) {
          OZ (text_protocol_base_type_convert(element_type_, dst, offset_dst_pos, dst_len));
          OX (dst_pos = offset_dst_pos);
        }
      }
      if (TEXT == type && !obj->is_invalid_type()) {
        OZ (text_protocol_suffix_info_for_each_item(element_type_,
                                                    dst,
                                                    dst_len - dst_pos,
                                                    dst_pos,
                                                    i < table->get_count() - 1 ? false : true,
                                                    ObPLComposite::obj_is_null(obj)));
      }
    }
    LOG_DEBUG("serialize length", K(ret), KPC(table), KPC(this), K(reinterpret_cast<int64_t>(dst)), K(dst_len), K(dst_pos));
    if (OB_SUCC(ret)) {
      src += sizeof(ObObj);
    }
  }
  return ret;
}

int ObCollectionType::deserialize(ObSchemaGetterGuard &schema_guard,
                                  ObIAllocator &allocator,
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
  int64_t element_init_size = 0;
  int64_t field_cnt = OB_INVALID_COUNT;

  if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
    LOG_WARN("get table type init size failed", K(ret), KPC(this));
  } else if (OB_ISNULL(dst) || (dst_len - dst_pos) < init_size) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("data deserialize failed", K(ret), K(dst), K(init_size), K(dst_len), K(dst_pos));
  } else if (OB_FAIL(element_type_.get_size(PL_TYPE_INIT_SIZE, element_init_size))) {
    LOG_WARN("get element init size failed", K(ret), KPC(this), K(init_size));
  } else if (OB_FAIL(element_type_.get_field_count(ObPLUDTNS(schema_guard), field_cnt))) {
    LOG_WARN("get field count failed", K(ret));
  } else {
    ObPLNestedTable *table = reinterpret_cast<ObPLNestedTable *>(dst + dst_pos);
    ObPLAllocator1 *collection_allocator = NULL;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), KPC(this), K(dst_pos), K(init_size), K(element_init_size));
    } else if (OB_ISNULL(table->get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table allocator is not null", K(ret), KPC(this), KPC(table));
    } else {
      uint64_t max_count = OB_INVALID_SIZE;
      uint64_t tab_count = OB_INVALID_SIZE;
      uint64_t count = OB_INVALID_SIZE;
      char *table_data = NULL;
      collection_allocator = static_cast<ObPLAllocator1 *>(table->get_allocator());
      CK (OB_NOT_NULL(collection_allocator));
      OZ (ObMySQLUtil::get_length(src, count), K(*this), K(*table));
      OX (max_count = count >> 32);
      OX (tab_count = count & 0xffffffff);
      OX (max_count = (0 == max_count) ? tab_count : max_count);
      OX (count = tab_count);
      CK (max_count >= count);
      if (OB_SUCC(ret) && OB_LIKELY(0 != max_count)) {
        int64_t bitmap_bytes = ((count + 7) / 8);
        const char* bitmap = src;
        src += bitmap_bytes;
        ObObj null_value;
        int64_t table_data_len = element_init_size * max_count;
        int64_t table_data_pos = 0;

        if (OB_ISNULL(table_data = static_cast<char *>(collection_allocator->alloc(table_data_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed",
                   K(ret), KPC(this), KPC(table), K(element_init_size), K(count), K(max_count));
        }
        if (OB_SUCC(ret)) {
          // initialize all ObObj
          ObObj *obj = reinterpret_cast<ObObj*>(table_data);
          CK (OB_NOT_NULL(obj));
          for (int64_t i = 0; OB_SUCC(ret) && i < max_count; i++) {
            obj[i].reset();
          }
        }
        int64_t n = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (ObSMUtils::update_from_bitmap(null_value, bitmap, i)) { // null value
            ObObj* value = reinterpret_cast<ObObj*>(table_data + table_data_pos);
            if (element_type_.is_obj_type()) {
              value->set_null();
            } else {
              const ObUserDefinedType *user_type = NULL;
              ObPLUDTNS ns(schema_guard);
              ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
              int64_t ptr = 0;
              ObPLComposite *composite = NULL;
              if (OB_FAIL(ns.get_user_type(element_type_.get_user_type_id(), user_type, &tmp_allocator))) {
                LOG_WARN("failed to get user type", K(ret), K(element_type_));
              } else if (OB_ISNULL(user_type)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get element type", K(ret), K(element_type_));
              } else if (OB_FAIL(user_type->newx(*collection_allocator, &ns, ptr))) {
                LOG_WARN("failed to newx", K(ret), K(element_type_));
              } else if (OB_ISNULL(composite = reinterpret_cast<ObPLComposite*>(ptr))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected error, got null composite value", K(ret));
              } else {
                composite->set_null();
                value->set_extend(ptr, element_type_.get_type());
              }
            }
            OX (table_data_pos += sizeof(ObObj));
          } else {
            if (OB_FAIL(element_type_.deserialize(schema_guard, *collection_allocator, charset, cs_type, ncs_type,
                                                tz_info, src, table_data, table_data_len, table_data_pos))) {
              LOG_WARN("deserialize element failed", K(ret), K(i), K(element_init_size), K(count));
            }
          }
          OX(++n);
          LOG_DEBUG("deserialize element done", K(ret), KPC(this), K(i), K(element_init_size), K(count),
            K(src), K(table_data), K(table_data_len), K(table_data_pos));
        }
        if (OB_FAIL(ret)) {
          table->set_type(PL_NESTED_TABLE_TYPE);
          table->set_allocator(collection_allocator);
          if (OB_NOT_NULL(table_data)) {
            table->set_count(n + 1);
            table->set_first(1);
            table->set_last(n + 1);
            table->set_data(reinterpret_cast<ObObj*>(table_data), n + 1);
            ObObj tmp;
            tmp.set_extend(reinterpret_cast<int64_t>(table), table->get_type());
            ObUserDefinedType::destruct_obj(tmp, nullptr, true);
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObElemDesc elem_desc;
        table->set_type(PL_NESTED_TABLE_TYPE);
        table->set_allocator(collection_allocator);
        table->set_count(max_count);
        table->set_first(1);
        table->set_last(count);
        table->set_not_null(element_type_.get_not_null());
        table->set_data(reinterpret_cast<ObObj*>(table_data), max_count);
        table->set_column_count(field_cnt);
        elem_desc.set_pl_type(element_type_.get_type());
        elem_desc.set_not_null(element_type_.get_not_null());
        if (OB_ISNULL(element_type_.get_data_type())) {
          OX (elem_desc.set_obj_type(common::ObExtendType));
          OX (elem_desc.set_field_count(field_cnt));
          OX (elem_desc.set_udt_id(element_type_.get_user_type_id()));
          OX (table->set_element_desc(elem_desc));
        } else {
          elem_desc.set_data_type(*(element_type_.get_data_type()));
          elem_desc.set_field_count(1);
          table->set_element_desc(elem_desc);
        }
      }
      for (int64_t i = count; OB_SUCC(ret) && i < max_count; ++i) {
        OZ (table->delete_collection_elem(i), K(i), K(max_count), K(count));
      }
    }
  }
  return ret;
}

int ObCollectionType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  int ret = OB_SUCCESS;
  ObPLCollection *src_table = NULL;
  ObPLCollection *dst_table = NULL;
  int64_t element_init_size = 0;
  ObPLAllocator1 *collection_allocator = NULL;
  char *table_data = NULL;

  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dst));
  CK (OB_LIKELY(src->is_ext()));
  CK (OB_LIKELY(dst->is_ext()));
  CK (OB_NOT_NULL(src_table = reinterpret_cast<ObPLCollection *>(src->get_ext())));
  CK (OB_NOT_NULL(dst_table = reinterpret_cast<ObPLCollection *>(dst->get_ext())));
  CK (OB_NOT_NULL(dst_table->get_allocator()));
  OZ (element_type_.get_size(PL_TYPE_INIT_SIZE, element_init_size));
  OX (collection_allocator = dynamic_cast<ObPLAllocator1 *>(dst_table->get_allocator()));
  CK (OB_NOT_NULL(collection_allocator));

  if (OB_SUCC(ret) && src_table->get_count() > 0
    && OB_ISNULL(table_data
      = static_cast<char *>(
          collection_allocator->alloc(element_init_size * src_table->get_count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc table data", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObPLResolveCtx resolve_ctx(*collection_allocator,
                                ctx.session_info_,
                                ctx.schema_guard_,
                                ctx.package_guard_,
                                ctx.sql_proxy_,
                                false);
    for (int64_t i = 0; OB_SUCC(ret) && i < src_table->get_count(); i++) {
      ObObj *src_table_pos = reinterpret_cast<ObObj*>(src_table->get_data()) + i;
      ObObj *dst_table_pos = reinterpret_cast<ObObj*>(table_data) + i;
      if (src_table_pos->is_invalid_type()) {
        OX (dst_table_pos->set_type(ObMaxType));
      } else {
        OX (new (dst_table_pos)ObObj());
        OZ (element_type_.convert(resolve_ctx, src_table_pos, dst_table_pos));
      }
    }
  }
  if (OB_SUCC(ret)) {
    dst_table->set_type(src_table->get_type());
    dst_table->set_allocator(collection_allocator);
    dst_table->set_count(src_table->get_count());
    if (src_table->get_count() > 0) {
      dst_table->set_first(1);
      dst_table->set_last(src_table->get_count());
    } else {
      dst_table->set_first(OB_INVALID_INDEX);
      dst_table->set_last(OB_INVALID_INDEX);
    }
    dst_table->set_data(reinterpret_cast<ObObj*>(table_data), src_table->get_count());

    ObElemDesc elem_desc;
    elem_desc.set_pl_type(element_type_.get_type());
    elem_desc.set_not_null(element_type_.get_not_null());
    if (OB_ISNULL(element_type_.get_data_type())) {
      int64_t field_cnt = OB_INVALID_COUNT;
      elem_desc.set_obj_type(common::ObExtendType);
      elem_desc.set_udt_id(element_type_.get_user_type_id());
      OZ (element_type_.get_field_count(ctx, field_cnt));
      OX (elem_desc.set_field_count(field_cnt));
    } else {
      elem_desc.set_data_type(*(element_type_.get_data_type()));
      elem_desc.set_field_count(1);
    }
    OX (dst_table->set_element_desc(elem_desc));
  }
  return ret;
}

//---------- for ObNestedTableType ----------

int ObNestedTableType::generate_construct(ObPLCodeGenerator &generator,
                                          const ObPLINS &ns,
                                          jit::ObLLVMValue &value,
                                          jit::ObLLVMValue &allocator,
                                          bool is_top_level,
                                          const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  OZ (SMART_CALL(ObCollectionType::generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  return ret;
}

int ObNestedTableType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  OZ (ObCollectionType::newx(allocator, ns, ptr));
  return ret;
}

int ObNestedTableType::init_obj(ObSchemaGetterGuard &schema_guard,
                                ObIAllocator &allocator,
                                ObObj &obj,
                                int64_t &init_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::init_obj(schema_guard, allocator, obj, init_size))) {
    LOG_WARN("failed to init obj", K(ret));
  }
  return ret;
}

int ObNestedTableType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                        common::ObIAllocator &obj_allocator,
                                        sql::ObExecContext &exec_ctx,
                                        const sql::ObSqlExpression *default_expr,
                                        bool default_construct,
                                        ObObj &obj) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::init_session_var(resolve_ctx,
                                                 obj_allocator,
                                                 exec_ctx,
                                                 default_expr,
                                                 default_construct,
                                                 obj))) {
    LOG_WARN("generate copy failed", K(ret));
  }
  return ret;
}

int ObNestedTableType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                                 const sql::ObSQLSessionInfo &session,
                                 const ObTimeZoneInfo *tz_info,
                                 MYSQL_PROTOCOL_TYPE type,
                                 char *&src,
                                 char *dst,
                                 const int64_t dst_len,
                                 int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::serialize(schema_guard,
                                          session,
                                          tz_info,
                                          type,
                                          src,
                                          dst,
                                          dst_len,
                                          dst_pos))) {
    LOG_WARN("failed to serialize ObNestedTableType", K(ret));
  }
  return ret;
}

int ObNestedTableType::deserialize(ObSchemaGetterGuard &schema_guard,
                                   ObIAllocator &allocator,
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
  OZ (ObCollectionType::deserialize(schema_guard,
                                    allocator,
                                    charset,
                                    cs_type,
                                    ncs_type,
                                    tz_info,
                                    src, dst, dst_len, dst_pos), *this);
  return ret;
}

// --------- for session serialize/deserialize interface ---------
int ObNestedTableType::get_serialize_size(
  const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  return ObCollectionType::get_serialize_size(resolve_ctx, src, size);
}

int ObNestedTableType::serialize(
  const ObPLResolveCtx &resolve_ctx,
  char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const
{
  return ObCollectionType::serialize(resolve_ctx, src, dst, dst_len, dst_pos);
}

int ObNestedTableType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  return ObCollectionType::deserialize(resolve_ctx, allocator, src, src_len, src_pos, dst);
}

int ObNestedTableType::add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                                  const ObPLBlockNS &block_ns,
                                                  const common::ObString &package_name,
                                                  const common::ObString &param_name,
                                                  int64_t mode, int64_t position,
                                                  int64_t level, int64_t &sequence,
                                                  share::schema::ObRoutineInfo &routine_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::add_package_routine_schema_param(resolve_ctx,
                                                                 block_ns,
                                                                 package_name,
                                                                 param_name,
                                                                 mode,
                                                                 position,
                                                                 level,
                                                                 sequence,
                                                                 routine_info))) {
    LOG_WARN("generate copy failed", K(ret));
  }
  return ret;
}

int ObNestedTableType::get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                                  const ObPLBlockNS &current_ns) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::get_all_depended_user_type(resolve_ctx, current_ns))) {
    LOG_WARN("generate copy failed", K(ret));
  }
  return ret;
}

//---------- for ObVArrayType ----------

int ObVArrayType::deep_copy(common::ObIAllocator &alloc, const ObVArrayType &other)
{
  int ret = OB_SUCCESS;
  OZ (ObCollectionType::deep_copy(alloc, other));
  OX (capacity_ = other.capacity_);
  return ret;
}

int ObVArrayType::generate_construct(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     jit::ObLLVMValue &allocator,
                                     bool is_top_level,
                                     const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  ObLLVMValue capacity_ptr;
  OZ (SMART_CALL(ObCollectionType::generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  OZ (generator.extract_capacity_ptr_from_varray(value, capacity_ptr));
  OZ (generator.get_helper().create_istore(capacity_, capacity_ptr));
  return ret;
}

int ObVArrayType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  OZ (ObCollectionType::newx(allocator, ns, ptr));
  OX (reinterpret_cast<ObPLVArray*>(ptr)->set_capacity(capacity_));
  return ret;
}

int ObVArrayType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                   common::ObIAllocator &obj_allocator,
                                   sql::ObExecContext &exec_ctx,
                                   const sql::ObSqlExpression *default_expr,
                                   bool default_construct,
                                   ObObj &obj) const
{
  int ret = OB_SUCCESS;
  ObPLVArray *varray_ptr = NULL;
  int64_t data = 0;
  OZ (ObCollectionType::init_session_var(
    resolve_ctx, obj_allocator, exec_ctx, default_expr, default_construct, obj));
  OZ (obj.get_ext(data));
  OX (varray_ptr = reinterpret_cast<ObPLVArray *>(data));
  OX (varray_ptr->set_capacity(capacity_));
  return ret;
}

int ObVArrayType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  UNUSEDx(ctx, src, dst);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "failed to convert to varray type");
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "convert to varray");
  return OB_NOT_SUPPORTED;
}

//---------- for ObAssocArrayType ----------

int ObAssocArrayType::deep_copy(common::ObIAllocator &alloc, const ObAssocArrayType &other)
{
  int ret = OB_SUCCESS;
  OZ (ObCollectionType::deep_copy(alloc, other));
  OZ (index_type_.deep_copy(alloc, other.index_type_));
  return ret;
}

int ObAssocArrayType::generate_construct(ObPLCodeGenerator &generator,
                                         const ObPLINS &ns,
                                         jit::ObLLVMValue &value,
                                         jit::ObLLVMValue &allocator,
                                         bool is_top_level,
                                         const pl::ObPLStmt *stmt) const
{
  //TODO: @ryan.ly
  int ret = OB_SUCCESS;
  ObLLVMValue capacity_ptr;
  OZ (SMART_CALL(ObCollectionType::generate_construct(generator, ns, value, allocator, is_top_level, stmt)));
  return ret;
}

int ObAssocArrayType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  OZ (ObCollectionType::newx(allocator, ns, ptr));
  return ret;
}

int ObAssocArrayType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                       common::ObIAllocator &obj_allocator,
                                       sql::ObExecContext &exec_ctx,
                                       const sql::ObSqlExpression *default_expr,
                                       bool default_construct,
                                       ObObj &obj) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCollectionType::init_session_var(resolve_ctx,
                                                obj_allocator,
                                                exec_ctx,
                                                default_expr,
                                                default_construct,
                                                obj))) {
    LOG_WARN("failed to init associative variable", K(ret));
  }
  return ret;
}

// --------- for session serialize/deserialize interface ---------
int ObAssocArrayType::get_serialize_size(
  const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  int ret = OB_SUCCESS;
  ObPLAssocArray *assoc_table = reinterpret_cast<ObPLAssocArray *>(src);
  char *key = NULL;
  int64_t *sort = NULL;
  int64_t key_sort_cnt = 0; // 紧密数组, key和sort是null
  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  CK (OB_NOT_NULL(assoc_table));
  OZ (ObCollectionType::get_serialize_size(resolve_ctx, src, size));
  OX (key = reinterpret_cast<char *>(assoc_table->get_key()));
  OZ (assoc_table->get_compatible_sort(allocator, sort));
  OX (key_sort_cnt = OB_NOT_NULL(key) ? assoc_table->get_count() : 0);
  OX (size += serialization::encoded_length(key_sort_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
    CK (OB_NOT_NULL(key));
    CK (OB_NOT_NULL(sort));
    OZ (index_type_.get_serialize_size(resolve_ctx, key, size));
    OX (size += serialization::encoded_length(*sort));
    OX (sort++);
  }
  return ret;
}

int ObAssocArrayType::serialize(
  const ObPLResolveCtx &resolve_ctx,
  char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  ObPLAssocArray *assoc_table = reinterpret_cast<ObPLAssocArray *>(src);
  char *key = NULL;
  int64_t *sort = NULL;
  int64_t key_sort_cnt = 0;
  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  CK (OB_NOT_NULL(assoc_table));
  OZ (ObCollectionType::serialize(resolve_ctx, src, dst, dst_len, dst_pos));
  OX (key = reinterpret_cast<char *>(assoc_table->get_key()));
  OZ (assoc_table->get_compatible_sort(allocator, sort));
  OX (key_sort_cnt = OB_NOT_NULL(key) ? assoc_table->get_count() : 0);
  OZ (serialization::encode(dst, dst_len, dst_pos, key_sort_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
    OZ (index_type_.serialize(resolve_ctx, key, dst, dst_len, dst_pos));
    OZ (serialization::encode(dst, dst_len, dst_pos, *sort));
    OX (sort++);
  }
  return ret;
}

int ObAssocArrayType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  int ret = OB_SUCCESS;
  ObPLAssocArray *assoc_table = reinterpret_cast<ObPLAssocArray *>(dst);
  char *key = NULL;
  int64_t *sort = NULL;
  int64_t key_sort_cnt = 0;
  CK (OB_NOT_NULL(assoc_table));
  CK (OB_NOT_NULL(assoc_table->get_allocator()));
  OZ (ObCollectionType::deserialize(resolve_ctx, allocator, src, src_len, src_pos, dst));
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (PL_INVALID_TYPE == (reinterpret_cast<ObPLCollection *>(dst))->get_type()) {
    // element be delete . do not deserialize continue
  } else {
    OZ (serialization::decode(src, src_len, src_pos, key_sort_cnt));
    if (OB_FAIL(ret)) {
    } else if (0 == key_sort_cnt) {
      if (OB_NOT_NULL(assoc_table->get_key()) &&
          OB_NOT_NULL(assoc_table->get_sort())) {
        assoc_table->get_allocator()->free(assoc_table->get_key());
        assoc_table->get_allocator()->free(assoc_table->get_sort());
      }
      assoc_table->set_key(NULL);
      assoc_table->set_sort(NULL);
    } else {
      CK (key_sort_cnt == assoc_table->get_count());
      CK (OB_NOT_NULL(key = reinterpret_cast<char *>(assoc_table->get_key())));
      CK (OB_NOT_NULL(sort = assoc_table->get_sort()));
      for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
        OZ (index_type_.deserialize(
          resolve_ctx, *(assoc_table->get_allocator()), src, src_len, src_pos, key));
        OZ (serialization::decode(src, src_len, src_pos, *sort));
        OX (sort++);
      }
    }
  }
  return ret;
}

int ObAssocArrayType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  UNUSEDx(ctx, src, dst);
  LOG_WARN_RET(OB_NOT_SUPPORTED, "failed to convert to assoc array type");
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "convert to associtive array");
  return OB_NOT_SUPPORTED;
}
#endif

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

#ifdef OB_BUILD_ORACLE_PL
#define COPY_COLLECTION(TYPE) \
  do { \
    if (OB_SUCC(ret)) { \
      TYPE *collection = NULL; \
      bool keep_collectin_attr = true;  \
      if (NULL == dest) { \
        if (OB_ISNULL(dest = reinterpret_cast<ObPLComposite*>(allocator.alloc(src.get_init_size())))) {  \
          ret = OB_ALLOCATE_MEMORY_FAILED;                                \
          LOG_WARN("failed to alloc memory for collection", K(ret));      \
        } else {                                                          \
          TYPE *collection = static_cast<TYPE*>(dest);                    \
          CK (OB_NOT_NULL(collection));                                   \
          LOG_INFO("src is: ", KP(&src), K(src), KP(dest), K(src.get_init_size()));                                   \
          OX (new(collection)TYPE(src.get_id()));                         \
          OZ (collection->init_allocator(allocator, need_new_allocator));  \
          OX (keep_collectin_attr = false);  \
          if (OB_FAIL(ret) && OB_NOT_NULL(dest)) {  \
            allocator.free(dest);   \
          }   \
        }                   \
      } else { \
        CK (OB_NOT_NULL(dest->get_allocator())); \
      } \
      OX (collection = static_cast<TYPE*>(dest)); \
      if (OB_SUCC(ret)) { \
        ObObj destruct_obj; \
        destruct_obj.set_extend(reinterpret_cast<int64_t>(collection), collection->get_type()); \
        OZ (ObUserDefinedType::destruct_obj(destruct_obj, session, true)); \
      } \
      if (OB_FAIL(ret)) {    \
      } else if (OB_FAIL(collection->deep_copy(static_cast<TYPE*>(&src), NULL, ignore_del_element))) { \
        ObObj destruct_obj; \
        int tmp = OB_SUCCESS; \
        destruct_obj.set_extend(reinterpret_cast<int64_t>(collection), collection->get_type()); \
        if (keep_collectin_attr) {  \
          tmp = ObUserDefinedType::destruct_obj(destruct_obj, session, keep_collectin_attr); \
        } else {  \
          tmp = ObUserDefinedType::destruct_objparam(allocator, destruct_obj, session); \
        }  \
        LOG_WARN("fail to deep copy collection, release memory", K(ret), K(tmp)); \
      } \
    } \
  } while(0)
#endif

  switch (src.get_type()) {
  case PL_RECORD_TYPE: {
    ObPLRecord *composite = NULL;
    bool need_free = false;
    if (NULL == dest) {
      dest = reinterpret_cast<ObPLComposite*>(allocator.alloc(src.get_init_size()));
      composite = static_cast<ObPLRecord*>(dest);
      if (OB_ISNULL(composite)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate composite memory failed", K(ret));
      }
      OX (new(composite)ObPLRecord(src.get_id(), static_cast<ObPLRecord&>(src).get_count()));
      OZ (composite->init_data(allocator, need_new_allocator));
      OX (need_free = true);
      if (OB_FAIL(ret) && OB_NOT_NULL(composite)) {
        allocator.free(composite);
      }
    } else {
      OX (composite = static_cast<ObPLRecord*>(dest));
    }
    if (OB_SUCC(ret)) {
      OZ (composite->deep_copy(static_cast<ObPLRecord&>(src), allocator, ns, session, ignore_del_element));
      if (OB_FAIL(ret) && need_free) {
        ObObj destruct_obj;
        int tmp = OB_SUCCESS;
        destruct_obj.set_extend(reinterpret_cast<int64_t>(composite), composite->get_type());
        tmp = ObUserDefinedType::destruct_objparam(allocator, destruct_obj, session);
        LOG_WARN("fail to deep copy record, release memory", K(ret), K(tmp));
      }
    }
  }
    break;

#ifdef OB_BUILD_ORACLE_PL
  case PL_NESTED_TABLE_TYPE: {
    ObPLNestedTable *collection = NULL;
    bool keep_collectin_attr = true;
    if (NULL == dest) {
      if (OB_ISNULL(dest = reinterpret_cast<ObPLComposite*>(allocator.alloc(src.get_init_size())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for collection", K(ret));
      } else {
        ObPLNestedTable *collection = static_cast<ObPLNestedTable*>(dest);
        CK (OB_NOT_NULL(collection));
        LOG_INFO("src is: ", KP(&src), K(src), KP(dest), K(src.get_init_size()));
        OX (new(collection)ObPLNestedTable(src.get_id()));
        OZ (collection->init_allocator(allocator, need_new_allocator));
        OX (keep_collectin_attr = false);
        if (OB_FAIL(ret) && OB_NOT_NULL(dest)) {
          allocator.free(dest);
        }
      }
    } else {
      CK (OB_NOT_NULL(dest->get_allocator()));
    }
    OX (collection = static_cast<ObPLNestedTable*>(dest));
    if (OB_SUCC(ret)) {
      ObObj destruct_obj;
      destruct_obj.set_extend(reinterpret_cast<int64_t>(collection), collection->get_type());
      OZ (ObUserDefinedType::destruct_obj(destruct_obj, session, true));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(collection->deep_copy(static_cast<ObPLNestedTable*>(&src), NULL, ignore_del_element))) {
      ObObj destruct_obj;
      int tmp = OB_SUCCESS;
      destruct_obj.set_extend(reinterpret_cast<int64_t>(collection), collection->get_type());
      if (keep_collectin_attr) {
        tmp = ObUserDefinedType::destruct_obj(destruct_obj, session, keep_collectin_attr);
      } else {
        tmp = ObUserDefinedType::destruct_objparam(allocator, destruct_obj, session);
      }
      LOG_WARN("fail to deep copy collection, release memory", K(ret), K(tmp));
    }
  }
    break;
  case PL_ASSOCIATIVE_ARRAY_TYPE: {
    COPY_COLLECTION(ObPLAssocArray);
  }
    break;
  case PL_VARRAY_TYPE: {
    COPY_COLLECTION(ObPLVArray);
  }
    break;
  case PL_OPAQUE_TYPE: { //forthrough

  }
#endif

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
#ifdef OB_BUILD_ORACLE_PL
    if (PL_OPAQUE_TYPE == src.get_meta().get_extend_type()) {
      ObPLOpaque *dest_composite = reinterpret_cast<ObPLOpaque*>(dest.get_ext());
      ObPLOpaque *src_composite = reinterpret_cast<ObPLOpaque*>(src.get_ext());
      CK (OB_NOT_NULL(src_composite));
      if (OB_SUCC(ret) && src_composite != dest_composite) {
        OZ (ObSPIService::spi_copy_opaque(NULL,
                                          &allocator,
                                          *src_composite,
                                          dest_composite,
                                          OB_INVALID_ID));
      }
      CK (OB_NOT_NULL(dest_composite));
      OX (dest.set_extend(reinterpret_cast<int64_t>(dest_composite),
                          src.get_meta().get_extend_type(),
                          src.get_val_len()));
    } else {
#endif
      ObPLComposite *dest_composite = reinterpret_cast<ObPLComposite*>(dest.get_ext());
      ObPLComposite *src_composite = reinterpret_cast<ObPLComposite*>(src.get_ext());
      if (src_composite != dest_composite) {
        CK (OB_NOT_NULL(src_composite));
        OZ (SMART_CALL(ObPLComposite::deep_copy(*src_composite,
                                    dest_composite,
                                    allocator,
                                    ns,
                                    session,
                                    need_new_allocator,
                                    ignore_del_element)));
        CK (OB_NOT_NULL(dest_composite));
        OX (dest.set_extend(reinterpret_cast<int64_t>(dest_composite),
                            src.get_meta().get_extend_type(),
                            src.get_val_len()));
      }
#ifdef OB_BUILD_ORACLE_PL
    }
#endif
  } else if (NULL != dest_type && NULL != session && !src.is_null()) {
    ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObExprResType result_type;
    ObObjParam result;
    ObObjParam src_tmp;
    CK (OB_NOT_NULL(dest_type));
    OX (result_type.set_meta(dest_type->get_meta_type()));
    OX (result_type.set_accuracy(dest_type->get_accuracy()));
    OX (src_tmp = src);
    OZ (ObSPIService::spi_convert(*session, tmp_allocator, src_tmp, result_type, result));
    OZ (ObUserDefinedType::destruct_objparam(allocator, dest));
    OZ (deep_copy_obj(allocator, result, dest));
  } else {
    if (src.is_null() && 0 != src.get_unknown()) {
      LOG_INFO("here maybe a bug", K(src), K(&src), K(src.get_unknown()));
    }
    OZ (ObUserDefinedType::destruct_objparam(allocator, dest));
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
#ifdef OB_BUILD_ORACLE_PL
  case PL_NESTED_TABLE_TYPE: {
    size = static_cast<ObPLNestedTable*>(this)->assign(static_cast<ObPLNestedTable*>(src),
                                                       allocator);
  }
    break;
  case PL_ASSOCIATIVE_ARRAY_TYPE: {
    size = static_cast<ObPLAssocArray*>(this)->assign(static_cast<ObPLAssocArray*>(src), allocator);
  }
    break;
  case PL_VARRAY_TYPE: {
    size = static_cast<ObPLVArray*>(this)->assign(static_cast<ObPLVArray*>(src), allocator);
  }
    break;
#endif
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

#ifdef OB_BUILD_ORACLE_PL
  case PL_NESTED_TABLE_TYPE: {
    size = static_cast<const ObPLNestedTable*>(this)->get_init_size();
  }
    break;
  case PL_ASSOCIATIVE_ARRAY_TYPE: {
    size = static_cast<const ObPLAssocArray*>(this)->get_init_size();
  }
    break;
  case PL_VARRAY_TYPE: {
    size = static_cast<const ObPLVArray*>(this)->get_init_size();
  }
    break;
#endif

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
#ifdef OB_BUILD_ORACLE_PL
    case PL_NESTED_TABLE_TYPE:
    case PL_ASSOCIATIVE_ARRAY_TYPE:
    case PL_VARRAY_TYPE: {
      static_cast<const ObPLCollection*>(this)->print();
    }
      break;
#endif
    default: {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected composite to print", K(get_type()));
    }
    }
}

bool ObPLComposite::obj_is_null(ObObj* obj) {
  int ret = OB_SUCCESS;
  bool is_null = true;
  if (OB_ISNULL(obj)) {
  } else if (obj->is_null()) {
  } else if (obj->is_ext()) {
    if (0 == obj->get_ext()) {
      is_null = true;
    } else if (PL_RECORD_TYPE == obj->get_meta().get_extend_type()) {
      ObPLRecord *record = reinterpret_cast<ObPLRecord*>(obj->get_ext());
      is_null = (record->is_null() || !record->is_inited()) ? true : false;
    } else if (PL_VARRAY_TYPE == obj->get_meta().get_extend_type()
                || PL_NESTED_TABLE_TYPE == obj->get_meta().get_extend_type()
                || PL_ASSOCIATIVE_ARRAY_TYPE == obj->get_meta().get_extend_type()) {
      ObPLCollection *coll = reinterpret_cast<ObPLCollection*>(obj->get_ext());
      is_null = (coll->is_null() || !coll->is_inited()) ? true : false;
    } else {
      is_null = false;
    }
  } else {
    is_null = false;
  }
  return is_null;
}

int ObPLRecord::init_data(common::ObIAllocator &allocator, bool need_new_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_COUNT == count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must construct obplrecord before init data", K(ret));
  } else if (OB_NOT_NULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot init record data twice", K(ret));
  } else {
    ObPLAllocator1 *pl_allocator = static_cast<ObPLAllocator1*>(allocator.alloc(sizeof(ObPLAllocator1)));
    if (OB_ISNULL(pl_allocator)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for record allocator", K(ret));
    } else {
      pl_allocator = new(pl_allocator)ObPLAllocator1(PL_MOD_IDX::OB_PL_RECORD, &allocator);
      OZ (pl_allocator->init(need_new_allocator ? nullptr : &allocator));
      OX (set_allocator(pl_allocator));
    }
    if (OB_SUCC(ret)) {
      ObObj* data = reinterpret_cast<ObObj*>(get_allocator()->alloc(sizeof(ObObj) * count_));
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for record data", K(ret));
      } else {
        for (int64_t i = 0; i < count_; ++i) {
          new (data + i) ObObj();
        }
        set_data(data);
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(pl_allocator)) {
      pl_allocator->~ObPLAllocator1();
      allocator.free(pl_allocator);
      set_allocator(nullptr);
    }
  }
  return ret;
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
  CK (OB_NOT_NULL(data_));
  OX (obj = data_[i]);
  return ret;
}

int ObPLRecord::get_element(int64_t i, ObObj *&obj)
{
  int ret = OB_SUCCESS;
  CK (i >= 0 && i < get_count());
  CK (OB_NOT_NULL(data_));
  OX (obj = &data_[i]);
  return ret;
}

int ObPLRecord::assign(ObPLRecord *src, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(src));
  if (OB_SUCC(ret)) {
    set_type(src->get_type());
    set_id(src->get_id());
    set_is_null(src->is_null());
    set_count(src->get_count());
    MEMCPY(this->get_not_null(), src->get_not_null(), src->get_init_size() - ObRecordType::get_notnull_offset());
    ObObj src_element;
    ObObj *dest_element = NULL;
    CK (OB_NOT_NULL(get_allocator()));
    for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
      OZ (src->get_element(i, src_element));
      OZ (get_element(i, dest_element));
      OZ (ObPLComposite::assign_element(src_element, *dest_element, *get_allocator()));
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

  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(count_), KPC(data_));
  }
  OV (get_count() == src.get_count(), OB_ERR_WRONG_TYPE_FOR_VAR, K(get_count()), K(src.get_count()));
  CK (OB_NOT_NULL(get_allocator()));
  if (OB_SUCC(ret)) {
    if (get_id() == src.get_id()) {
      set_type(src.get_type());
      set_is_null(src.is_null());
      MEMCPY(this->get_not_null(), src.get_not_null(), src.get_init_size() - ObRecordType::get_notnull_offset());
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
                                      *get_allocator(),
                                      ns,
                                      session,
                                      NULL == elem_type ? NULL : elem_type->get_data_type(),
                                      false, /*need_new_allocator*/
                                      ignore_del_element));
    }
  }
  return ret;
}

int ObPLRecord::set_data(const ObIArray<ObObj> &row)
{
  int ret = OB_SUCCESS;
  CK (get_count() == row.count());
  CK (OB_NOT_NULL(data_));
  CK (OB_NOT_NULL(allocator_));
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count(); ++i) {
    ObObj &cur_obj = data_[i];
    if (row.at(i).is_pl_extend()) {
      OZ (ObUserDefinedType::destruct_objparam(*allocator_, cur_obj, nullptr));
      OZ (ObUserDefinedType::deep_copy_obj(*allocator_, row.at(i), cur_obj));
    } else {
      void * ptr = cur_obj.get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        allocator_->free(ptr);
      }
      OZ (deep_copy_obj(*allocator_, row.at(i), cur_obj));
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

ObIAllocator* ObPLCollection::get_coll_allocator()
{
  return dynamic_cast<ObPLAllocator1 *>(allocator_);
}

int ObPLCollection::init_allocator(common::ObIAllocator &allocator, bool need_new_allocator)
{
  int ret = OB_SUCCESS;

  ObPLAllocator1 *collection_allocator = nullptr;
  CK (OB_ISNULL(get_allocator()));
  collection_allocator = static_cast<ObPLAllocator1*>(allocator.alloc(sizeof(ObPLAllocator1)));
  if (OB_ISNULL(collection_allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("get a invalud obj", K(ret), K(collection_allocator));
  } else {
    collection_allocator = new(collection_allocator)ObPLAllocator1(PL_MOD_IDX::OB_PL_COLLECTION, &allocator);
    OZ (collection_allocator->init(need_new_allocator ? nullptr : &allocator));
    if (OB_SUCC(ret)) {
      set_allocator(collection_allocator);
    } else {
      allocator.free(collection_allocator);
    }
  }

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
  ObIAllocator *coll_allocator = allocator_;
  CK (OB_ISNULL(allocator));
  CK (OB_NOT_NULL(coll_allocator));
  CK (OB_NOT_NULL(src) && src->is_collection());

  if (OB_SUCC(ret)) {
    void* data = NULL;
    int64_t k = 0;
    if (src->get_inner_capacity() > 0) {
      data = coll_allocator->alloc(src->get_inner_capacity() * sizeof(ObObj));
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for collection",
                 K(ret), K(src->get_count()));
      }
      CK (OB_NOT_NULL(new_objs = reinterpret_cast<ObObj*>(data)));
      CK (OB_NOT_NULL(old_objs = reinterpret_cast<ObObj*>(src->get_data())));
      for (int64_t i = 0; OB_SUCC(ret) && i < src->get_inner_capacity(); ++i) {
        new (new_objs + i) ObObj();
      }
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < src->get_count(); ++i) {
        ObObj old_obj = old_objs[i];
        if (old_objs[i].is_invalid_type() && ignore_del_element && !is_associative_array()) {
          // ignore delete element
        } else {
          OX (new (&new_objs[k])ObObj());
          if (old_objs[i].is_invalid_type() && src->is_of_composite()) {
            new_objs[k].set_type(ObMaxType);
          } else {
            OZ (ObPLComposite::copy_element(old_obj,
                                            new_objs[k],
                                            *coll_allocator,
                                            NULL, /*ns*/
                                            NULL, /*session*/
                                            NULL, /*dest_type*/
                                            false, /*need_new_allocator*/
                                            ignore_del_element));
          }
          OX (++k);
        }
      }
      // 对于已经copy成功的new obj释放内存
      if (OB_FAIL(ret) && OB_NOT_NULL(data)) {
        for (int64_t j = 0; j <= k && j < src->get_count(); ++j) {
          if (new_objs[j].is_pl_extend()) {
            int tmp = ObUserDefinedType::destruct_objparam(*coll_allocator, new_objs[j], nullptr);
            if (OB_SUCCESS != tmp) {
              LOG_WARN("fail torelease memory", K(ret), K(tmp));
            }
          } else {
            void * ptr = new_objs[j].get_deep_copy_obj_ptr();
            if (nullptr != ptr) {
              coll_allocator->free(ptr);
            }
          }
          new_objs[j].set_type(ObMaxType);
        }
        coll_allocator->free(data);
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
#ifdef OB_BUILD_ORACLE_PL
        } else if (PL_ASSOCIATIVE_ARRAY_TYPE == src->get_type()) {
          set_first(static_cast<ObPLAssocArray *>(src)->get_pure_first());
          set_last(static_cast<ObPLAssocArray *>(src)->get_pure_last());
#endif
        } else {
          set_first(src->get_first());
          set_last(src->get_last());
        }
#ifdef OB_BUILD_ORACLE_PL
      } else if (PL_ASSOCIATIVE_ARRAY_TYPE == src->get_type()) {
        set_first(static_cast<ObPLAssocArray *>(src)->get_pure_first());
        set_last(static_cast<ObPLAssocArray *>(src)->get_pure_last());
#endif
      } else {
        set_first(src->is_inited() ? src->get_first() : OB_INVALID_INDEX);
        set_last(src->is_inited() ? src->get_last() : OB_INVALID_INDEX);
      }
      set_data(new_objs, src->get_inner_capacity());
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
    if (src->get_inner_capacity() > 0) {
      data = coll_allocator->alloc(src->get_inner_capacity() * sizeof(ObObj));
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
      for (int64_t i = src->get_count(); OB_SUCC(ret) && i < src->get_inner_capacity(); ++i) {
        new (&new_objs[i])ObObj();
      }
    }
    if (OB_SUCC(ret)) {
      set_allocator(coll_allocator);
      set_type(src->get_type());
      set_id(src->get_id());
      set_is_null(src->is_null());
      set_element_desc(src->get_element_desc());
      set_count(src->get_count());
      set_first(src->get_pure_first());
      set_last(src->get_pure_last());
      set_data(new_objs, src->get_inner_capacity());
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
  } else if (OB_ISNULL(get_data()) || OB_ISNULL(get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is uninited", K(ret));
  } else {
    ObObj *obj = static_cast<ObObj *>(get_data());
    // data的type设置为max表示被delete
    if (index < get_count()) {
      if (obj[index].is_pl_extend()) {
        if (OB_FAIL(ObUserDefinedType::destruct_objparam(*get_allocator(), obj[index], NULL))) {
          LOG_WARN("failed to destruct obj", K(ret), K(obj[index]), K(index));
        }
      } else {
        void *ptr = obj[index].get_deep_copy_obj_ptr();
        if (nullptr != ptr) {
          get_allocator()->free(ptr);
        }
      }
      OX (obj[index].set_type(ObMaxType));
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
      ObObj *obj = static_cast<ObObj *>(get_data());
      CK (OB_NOT_NULL(get_allocator()));
      for (int64_t index = count_ - trim_number; OB_SUCC(ret) && index < count_; ++index) {
        if (obj[index].is_pl_extend()) {
          if (OB_FAIL(ObUserDefinedType::destruct_objparam(*get_allocator(), obj[index], NULL))) {
            LOG_WARN("failed to destruct obj", K(ret), K(obj[index]), K(index));
          }
        } else {
          void *ptr = obj[index].get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            get_allocator()->free(ptr);
          }
        }
        OX (obj[index].set_type(ObMaxType));
      }
      if (OB_SUCC(ret)) {
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
    ret = OB_ERR_COLLECION_NULL;
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
    ret = OB_ERR_COLLECION_NULL;
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
  OX (size += serialization::encoded_length(get_pure_first()));
  OX (size += serialization::encoded_length(get_pure_last()));
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
  OZ (serialization::encode(buf, len, pos, get_pure_first()));
  OZ (serialization::encode(buf, len, pos, get_pure_last()));

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
  const int64_t *sort_array = nullptr;
  const ObObj *key_array = nullptr;
  LOG_INFO("ObPLCollection Header", K(this), K(*this));

#ifdef OB_BUILD_ORACLE_PL
  if (is_associative_array()) {
    sort_array = static_cast<const ObPLAssocArray*>(this)->get_sort();
    key_array = static_cast<const ObPLAssocArray*>(this)->get_key();
  }
#endif // OB_BUILD_ORACLE_PL

  for (int64_t i = 0; i < count_; ++i) {
    ObObj &obj = data_[i];
    const ObObj *key = key_array != nullptr ? &(key_array[i]) : nullptr;
    const int64_t sort = sort_array != nullptr ? sort_array[i] : OB_INVALID_INDEX;
    if (obj.is_pl_extend()) {
      ObPLComposite *composite = reinterpret_cast<ObPLComposite*>(obj.get_ext());
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(sort), KPC(key), K(*composite));
      OX (composite->print());
    } else if (obj.is_varchar_or_char() && obj.get_data_length() > 100) {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(sort), KPC(key), K("xxx...xxx"));
    } else if (obj.is_invalid_type()) {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(sort), KPC(key), K("deleted element"), K(obj));
    } else {
      LOG_INFO("ObPLCollection Data", K(i), K(get_count()), K(sort), KPC(key), K(obj));
    }
  }
}


int ObPLCollection::deserialize(common::ObIAllocator &allocator,
                                const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(allocator, buf, len, pos);
#else
  int64_t count = 0;
  int64_t rowsize = 0;
  int64_t first = 0;
  int64_t last = 0;

  OZ (serialization::decode(buf, len, pos, count));
  OZ (serialization::decode(buf, len, pos, rowsize));
  OZ (serialization::decode(buf, len, pos, first));
  OZ (serialization::decode(buf, len, pos, last));
  CK (rowsize > 0);

  UNUSED(allocator);
  CK (OB_NOT_NULL(get_allocator()));
  OX (set_inited());
  OX (set_first(first));
  OX (set_last(last));
  if (OB_FAIL(ret)) {
  } else if (is_associative_array()) {
    ObPLAssocArray *assoc_table = static_cast<ObPLAssocArray *>(this);
    OZ (ObSPIService::spi_extend_assoc_array( //TODO:@ryan.ly myst be bug here!!!
      OB_INVALID_ID, NULL, *get_allocator(), *assoc_table, count));
  } else {
    OZ (ObSPIService::spi_set_collection(
      OB_INVALID_ID, NULL, *get_allocator(), *this, count, true));
  }
  CK (OB_NOT_NULL(get_data()));

  if (OB_SUCC(ret)) {
    char *table_data = reinterpret_cast<char*>(get_data());
    for (int64_t i = 0; OB_SUCC(ret) && i < count * rowsize / sizeof(ObObj); ++i) {
      ObObj src_obj;
      OZ (src_obj.deserialize(buf, len, pos));
      OZ (deep_copy_obj(*get_allocator(), src_obj, reinterpret_cast<ObObj*>(table_data)[i]));
    }
  }
#endif
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
            OZ (ObUserDefinedType::destruct_objparam(*allocator_, data_obj, nullptr));
            OZ (ObUserDefinedType::deep_copy_obj(*allocator_, row.at(0), data_obj));
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
          OZ (new_record->init_data(*allocator_, false));
          if (OB_FAIL(ret)) {
            if (OB_NOT_NULL(new_record)) {
              allocator_->free(new_record);
            }
          } else {
            OX (new_record->set_data(row));
            OX (data_obj.set_extend(reinterpret_cast<int64_t>(new_record),
                                    PL_RECORD_TYPE,
                                    ObRecordType::get_init_size(element_.get_field_count())));
          }
        } else {
          CK (1 == row.count());
          OZ (deep_copy_obj(*allocator_, row.at(0), data_obj));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected data in array", K(data_obj), K(element_), K(ret));
      }
    } else {
      CK (1 == row.count());
      OZ (deep_copy_obj(*allocator_, row.at(0), data_obj));
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
          OZ (new_record->init_data(*allocator_, false));
          CK (PL_RECORD_TYPE == row.get_meta().get_extend_type());
          if (OB_SUCC(ret)) {
            ObPLRecord *src_record = reinterpret_cast<ObPLRecord*>(row.get_ext());
            OZ (new_record->assign(src_record, get_allocator()));
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(new_record)) {
            allocator_->free(new_record);
          } else {
            OX (data_obj.set_ext(reinterpret_cast<int64_t>(new_record)));
          }
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

#ifdef OB_BUILD_ORACLE_PL
//---------- for ObPLAssocArray ----------

int ObPLAssocArray::first(ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t first = get_first();
  if (OB_INVALID_INDEX == first) {
    result.set_null();
  } else if (NULL == get_key()) {
    result.set_int(first);
  } else {
    CK (OB_NOT_NULL(get_key(first - 1)));
    OX (result = *(get_key(first - 1)));
  }
  return ret;
}

int ObPLAssocArray::last(ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t last = get_last();
  if (OB_INVALID_INDEX == last) {
    result.set_null();
  } else if (NULL == get_key()) {
    result.set_int(last);
  } else {
    OV (OB_NOT_NULL(get_key(last - 1)), OB_ERR_UNEXPECTED, KPC(this), K(last_), K(last));
    if (OB_FAIL(ret)) {
      this->print();
    }
    OX (result = *(get_key(last - 1)));
  }
  return ret;
}

int ObPLAssocArray::prior(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_key()) && OB_ISNULL(get_sort())) {
    OZ (ObPLCollection::prior(idx, result));
  } else if (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort())) {
    bool need_search = false;
    result.set_null();
    if (IndexRangeType::LESS_THAN_FIRST == idx) {
      result.set_null();
    } else if (IndexRangeType::LARGE_THAN_LAST == idx) {
      OX (idx = get_last());
      OZ (is_elem_deleted(idx - 1, need_search));
    } else if (idx > 0) {
      if (idx == get_first()) {
        idx = IndexRangeType::LESS_THAN_FIRST;
        result.set_null();
      } else {
        need_search = true;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected assoc array", K(ret), K(idx));
    }
    if (OB_SUCC(ret) && need_search) {
      int64_t search = OB_INVALID_INDEX;
      for (int64_t i = 0; i < get_count(); ++i) {
        if (get_sort()[i] == (idx - 1)) {
          search = i;
          break;
        }
      }
      CK (search != OB_INVALID_INDEX);
      OX (search = (search - 1));
      while (OB_SUCC(ret) && need_search && search >= 0) {
        OX (idx = get_sort()[search] + 1);
        OZ (is_elem_deleted(idx - 1, need_search));
        OX (search = (search - 1));
      }
    }
    if (OB_SUCC(ret) && !need_search && idx >= 1 && idx <= get_count()) {
      CK (OB_NOT_NULL(get_key(idx - 1)));
      OX (result = *(get_key(idx - 1)));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected assoc array", K(ret), K(get_key()), K(get_sort()));
  }
  return ret;
}

int ObPLAssocArray::next(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_key()) && OB_ISNULL(get_sort())) {
    OZ (ObPLCollection::next(idx, result));
  } else if (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort())) {
    bool need_search = false;
    result.set_null();
    if (IndexRangeType::LARGE_THAN_LAST == idx) {
      result.set_null();
    } else if (IndexRangeType::LESS_THAN_FIRST == idx) {
      if (OB_INVALID_INDEX == get_first()) {
        result.set_null();
      } else {
        OX (idx = get_first());
        OZ (is_elem_deleted(idx - 1, need_search));
      }
    } else if (idx > 0) {
      if (idx == get_last()) {
        idx = IndexRangeType::LARGE_THAN_LAST;
        result.set_null();
      } else {
        need_search = true;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected assoc array", K(ret), K(idx));
    }
    CK (idx <= get_count());
    if (OB_SUCC(ret) && need_search) {
      int64_t search = OB_INVALID_INDEX;
      for (int64_t i = 0; i < get_count(); ++i) {
        if (get_sort()[i] == (idx - 1)) {
          search = i;
          break;
        }
      }
      CK (search != OB_INVALID_INDEX && search < get_count());
      int64_t i = 0;
      OX (search = search + 1);
      OX (i++);
      while (OB_SUCC(ret) && need_search && search < get_count()) {
        OX (idx = get_sort(search) + 1);
        OZ (is_elem_deleted(idx - 1, need_search));
        OX (search = search + 1);
        OX (i++);
      }
    }
    if (OB_SUCC(ret) && !need_search && idx > 0 && idx <= get_count()) {
      CK (OB_NOT_NULL(get_key(idx - 1)));
      OX (result = *(get_key(idx - 1)));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected assoc array", K(ret), K(get_key()), K(get_sort()));
  }
  return ret;
}

int ObPLAssocArray::exist(int64_t idx, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_key()) && OB_ISNULL(get_sort())) {
    OZ (ObPLCollection::exist(idx, result));
  } else if (OB_INVALID_INDEX == idx) {
    OX (result.set_tinyint(false));
  } else if (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort())) {
    bool is_del = false;
    OZ (is_elem_deleted(idx - 1, is_del));
    OX (result.set_tinyint(is_del ? false : true));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assoc array is illegal", K(ret));
  }
  return ret;
}

int ObPLAssocArray::deep_copy(ObPLCollection *src, ObIAllocator *allocator, bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  ObObj *key = NULL;
  int64_t *sort = NULL;
  CK (OB_NOT_NULL(src));
  CK (src->is_associative_array());
  OZ (ObPLCollection::deep_copy(src, allocator, ignore_del_element));
  if (OB_SUCC(ret) && src->get_inner_capacity() > 0) {
    ObPLAssocArray *src_aa = static_cast<ObPLAssocArray*>(src);
    CK (OB_NOT_NULL(src_aa));
    if (OB_FAIL(ret)) {
    } else if (NULL != src_aa->get_key() && NULL != src_aa->get_sort()) {
      CK (OB_NOT_NULL(get_allocator()));
      OX (key = static_cast<ObObj*>(get_allocator()->alloc(src->get_inner_capacity() * sizeof(ObObj))));
      OV (OB_NOT_NULL(key), OB_ALLOCATE_MEMORY_FAILED);
      for (int64_t i = 0; OB_SUCC(ret) && i < src->get_count(); ++i) {
        new(key + i)ObObj(ObNullType);
      }
      OX (sort = static_cast<int64_t*>(get_allocator()->alloc(src->get_inner_capacity() * sizeof(int64_t))));
      OV (OB_NOT_NULL(sort), OB_ALLOCATE_MEMORY_FAILED);
      for (int64_t i = 0; OB_SUCC(ret) && i < src->get_count(); ++i) {
        OZ (deep_copy_obj(*get_allocator(), *(src_aa->get_key(i)), key[i]));
        OX (sort[i] = src_aa->get_sort(i));
      }
    } else if (NULL == src_aa->get_key() && NULL == src_aa->get_sort()) {
      //Associative array的优化会出现这种情况，拷贝的时候同样按优化拷
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected associative array", K(*src_aa), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_key(key);
    set_sort(sort);
  } else {
    if (OB_NOT_NULL(key)) {
      for (int64_t i = 0; i < src->get_count(); ++i) {
        ObUserDefinedType::destruct_objparam(*get_allocator(), key[i], nullptr);
      }
      get_allocator()->free(key);
    }
    if (OB_NOT_NULL(sort)) {
      get_allocator()->free(sort);
    }
  }

  return ret;
}

int ObPLAssocArray::update_first()
{
  int ret = OB_SUCCESS;
  CK (is_inited());
  CK (get_count() > 0);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(get_key()) && OB_ISNULL(get_sort())) {
    update_first_impl();
  } else {
    int64_t first = OB_INVALID_INDEX;
    bool is_deleted = false;
    int64_t i = first_ - 1;
    CK (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort()));
    CK (first_ > 0 && first_ <= get_count());
    do {
      OX (first = get_sort()[i]);
      OZ (is_elem_deleted(first, is_deleted));
      if (OB_FAIL(ret)) {
      } else if (is_deleted) {
        i += 1;
        if (i >= get_count()) {
          OX (set_first(OB_INVALID_INDEX));
          break;
        }
      } else {
        first_ = i + 1;
        break;
      }
    } while (i < get_count() && OB_SUCC(ret));
  }
  return ret;
}

int ObPLAssocArray::update_last()
{
  int ret = OB_SUCCESS;
  CK (is_inited());
  CK (get_count() > 0);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(get_key()) && OB_ISNULL(get_sort())) {
    update_last_impl();
  } else {
    int64_t last = OB_INVALID_INDEX;
    bool is_deleted = false;
    int64_t i = last_ - 1;
    CK (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort()));
    CK (last_ > 0 && last_ <= get_count());
    do {
      OX (last = get_sort()[i]);
      OZ (is_elem_deleted(last, is_deleted));
      if (OB_FAIL(ret)) {
      } else if (is_deleted) {
        i -= 1;
        if (i < 0) {
          OX (set_last(OB_INVALID_INDEX));
          break;
        }
      } else {
        last_ = i + 1;
        break;
      }
    } while (i >= 0 && OB_SUCC(ret));
  }
  return ret;
}

int ObPLAssocArray::update_first_last(int64_t new_update)
{
  int ret = OB_SUCCESS;
  CK (is_inited());
  CK (get_count() > 0);
  CK (OB_NOT_NULL(get_key()) && OB_NOT_NULL(get_sort()));
  OX (new_update = new_update + 1);
  if (OB_SUCC(ret)) {
    if ((first_ != OB_INVALID_INDEX && new_update < first_)
        || OB_INVALID_INDEX == first_) {
      set_first(new_update);
    }
    if ((last_ != OB_INVALID_INDEX && new_update > last_)
        || OB_INVALID_INDEX == last_) {
      set_last(new_update);
    }
  }
  return ret;
}

int64_t ObPLAssocArray::get_first()
{
  return (get_sort() != NULL && first_ != OB_INVALID_INDEX && first_ >= 1 && first_ <= get_count()) ? get_sort()[first_ - 1] + 1 : first_;
}

int64_t ObPLAssocArray::get_last()
{
  return (get_sort() != NULL && last_ != OB_INVALID_INDEX && last_ >= 1 && last_ <= get_count()) ? get_sort()[last_ - 1] + 1 : last_;
}

int ObPLAssocArray::get_serialize_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t key_sort_cnt = 0; // 紧密数组, key和sort是null
  ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  int64_t* compatible_sort = NULL;
  OZ (get_compatible_sort(allocator, compatible_sort));
  OZ (ObPLCollection::get_serialize_size(size));
  OX (key_sort_cnt = OB_NOT_NULL(get_key()) ? get_count() : 0);
  OX (size += serialization::encoded_length(key_sort_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
    CK (OB_NOT_NULL(compatible_sort));
    OZ (size += get_key(i)->get_serialize_size());
    OX (size += serialization::encoded_length(compatible_sort[i]));
  }
  return ret;
}

int ObPLAssocArray::serialize(char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t key_sort_cnt = 0;
  OZ (ObPLCollection::serialize(buf, len, pos));
  OX (key_sort_cnt = OB_NOT_NULL(get_key()) ? get_count() : 0);
  OZ (serialization::encode(buf, len, pos, key_sort_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
    OZ (get_key(i)->serialize(buf, len, pos));
    OZ (serialization::encode(buf, len, pos, get_sort(i)));
  }
  return ret;
}

int ObPLAssocArray::deserialize(common::ObIAllocator &allocator,
                                const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t key_sort_cnt = 0;
  OZ (ObPLCollection::deserialize(allocator, buf, len, pos));
  OZ (serialization::decode(buf, len, pos, key_sort_cnt));
  if (OB_FAIL(ret)) {
  } else if (0 == key_sort_cnt) {
    if (OB_NOT_NULL(get_key()) &&
        OB_NOT_NULL(get_sort()) &&
        OB_NOT_NULL(get_allocator())) {
      get_allocator()->free(get_key());
      get_allocator()->free(get_sort());
    }
    set_key(NULL);
    set_sort(NULL);
  } else {
    CK (key_sort_cnt == get_count());
    for (int64_t i = 0; OB_SUCC(ret) && i < key_sort_cnt; ++i) {
      ObObj src_obj;
      OZ (src_obj.deserialize(buf, len, pos));
      OZ (deep_copy_obj(*get_allocator(), src_obj, *get_key(i)));
      OZ (serialization::decode(buf, len, pos, get_sort()[i]));
    }
  }
  return ret;
}

int ObPLAssocArray::compare_key(const ObObj &key1, const ObObj &key2, int &comp_ret)
{
  int ret = OB_SUCCESS;

  ObCollationType cs_type = key1.get_collation_type();
  OV ((key1.is_int32() && key2.is_int32())
      || (key1.is_varchar() && key2.is_varchar()
          && key1.get_collation_type() == key2.get_collation_type()
          && key1.get_collation_type() != ObCollationType::CS_TYPE_INVALID),
    OB_ERR_UNEXPECTED, K(key1), K(key2));

  if (OB_FAIL(ret)) {
  } else if (key1.is_int32()) {
    comp_ret = key1.get_int32() == key2.get_int32() ? 0 : key1.get_int32() > key2.get_int32() ? 1 : -1;
  } else {
    int val = ObCharset::strcmpsp(cs_type,
                                  key1.v_.string_, key1.val_len_,
                                  key2.v_.string_, key2.val_len_,
                                  is_calc_with_end_space(key1.get_type(), key2.get_type(), lib::is_oracle_mode(), cs_type, cs_type));
    comp_ret = val == 0 ? 0 : val > 0 ? 1 : -1;
  }
  return ret;
}

int ObPLAssocArray::search_key(const ObObj &key, int64_t &index, int64_t &search_end, int64_t sort_count)
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  search_end = OB_INVALID_INDEX;
  if (OB_NOT_NULL(this->get_key())) {
    int64_t low = 0;
    int64_t high = sort_count - 1;
    while (low <= high) {
      int64_t mid = (high + low) / 2;
      ObObj& cur_key = this->get_key()[this->get_sort()[mid]];
      int comp_ret = 0;
      OZ (compare_key(key, cur_key, comp_ret), K(mid));
      if (OB_FAIL(ret)) {
        this->print();
        break;
      } else if (-1 == comp_ret) { // less
        high = mid - 1;
        search_end = mid;
      } else if (1 == comp_ret) { // greater
        low = mid + 1;
        search_end = mid + 1;
      } else {
        index = this->get_sort()[mid];
        search_end = mid;
        break;
      }
    }
  } else if (sort_count > 0) {
    int32_t key_int;
    CK (key.is_int32());
    OX (key_int = key.get_int32() - 1);
    if (OB_SUCC(ret) && key_int >= 0 && key_int < sort_count) {
      index = key_int;
      search_end = key_int;
    }
  }
  return ret;
}

int ObPLAssocArray::get_compatible_sort(ObIAllocator &allocator, int64_t *&compatible_sort)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_sort()) && (GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_1 ||
     (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1))) {
    compatible_sort = reinterpret_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * get_count()));
    if (OB_ISNULL(compatible_sort)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for sort array", K(ret), KPC(this));
    }
    OX (MEMSET(compatible_sort, 0, sizeof(int64_t) * get_count()));
    OX (compatible_sort[get_sort()[get_count() - 1]] = OB_INVALID_INDEX);
    for (int64_t i = get_count() - 2; OB_SUCC(ret) && i >= 0; --i) {
      OX (compatible_sort[get_sort()[i]] = get_sort()[i + 1]);
    }
  } else {
    compatible_sort = get_sort();
  }
  return ret;
}

int ObPLAssocArray::rebuild_sort(ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (obj.is_pl_extend()) {
    switch (obj.get_meta().get_extend_type()) {
      case PL_NESTED_TABLE_TYPE:
      case PL_ASSOCIATIVE_ARRAY_TYPE:
      case PL_VARRAY_TYPE: {
        ObPLCollection *coll = reinterpret_cast<ObPLCollection *>(obj.get_ext());
        if (OB_NOT_NULL(coll)) {
          for (int64_t i = 0; i < coll->get_count(); ++i) {
            CK (OB_NOT_NULL(coll->get_data()));
            OZ (SMART_CALL(rebuild_sort(coll->get_data()[i])));
          }
          if (coll->is_associative_array()) {
            ObPLAssocArray *assoc_array = static_cast<ObPLAssocArray *>(coll);
            CK (OB_NOT_NULL(assoc_array));
            OZ (assoc_array->rebuild_sort());
          }
        }
      } break;
      case PL_RECORD_TYPE: {
        ObPLRecord *record = reinterpret_cast<ObPLRecord *>(obj.get_ext());
        if (OB_NOT_NULL(record)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
            ObObj elem;
            OZ (record->get_element(i, elem));
            OZ (SMART_CALL(rebuild_sort(elem)));
          }
        }
      } break;
      default: {
        // do nothing ...
      } break;
    }
  }
  return ret;
}

int ObPLAssocArray::rebuild_sort()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_sort())) {
    CK (OB_NOT_NULL(get_sort()) && OB_NOT_NULL(get_key()));
    OX (MEMSET(get_sort(), 0, sizeof(int64_t) * get_count()));

    for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
      int64_t index = OB_INVALID_INDEX;
      int64_t search_end = OB_INVALID_INDEX;
      OZ (search_key(get_key()[i], index, search_end, i));
      CK (OB_INVALID_INDEX == index);
      if (OB_FAIL(ret)) {
        this->print();
      }
      OZ (insert_sort(get_key()[i], i, search_end, i));
    }
    if (get_count() > 0) {
      OX (first_ = 1);
      OX (last_ = get_count());
      OZ (update_first());
      OZ (update_last());
    }
  }
  return ret;
}

int ObPLAssocArray::insert_sort(
  const ObObj &key, int64_t key_position, int64_t &sort_position, int64_t sort_count)
{
  int ret = OB_SUCCESS;
  int64_t key_index = OB_INVALID_INDEX;
  ObObj key_value;
  if (sort_position == OB_INVALID_INDEX) { // no sort and no key, insert to first position
    CK (0 == sort_count);
    CK (0 == key_position);
    OX (this->get_sort()[0] = key_position);
  } else if (sort_position == sort_count) { // last position
    CK (key_position == sort_count);
    OX (this->get_sort()[sort_position] = key_position);
  } else { // mid position
    int comp_ret = 0;
    OX (key_index = this->get_sort()[sort_position]);
    OX (key_value = this->get_key()[key_index]);
    OZ (compare_key(key, key_value, comp_ret));
    OV (comp_ret == -1, OB_ERR_UNEXPECTED, K(key), KPC(this), K(sort_position));
#ifndef NDEBUG
    if (OB_FAIL(ret)) {
      this->print();
    }
#endif
    // already extend sort memory on extend_assoc_array, so can move memory safety.
    OX (MEMMOVE(this->get_sort() + sort_position + 1, this->get_sort() + sort_position, sizeof(int64_t) * (sort_count - sort_position)));
    OX (this->get_sort()[sort_position] = key_position);
    if (OB_SUCC(ret)) { // first_ and last_ is sort array position, if move sort memory, need to update first_, last_
      if (first_ > sort_position) {
        first_ += 1;
      }
      if (last_ > sort_position) {
        last_ += 1;
      }
    }
  }
  return ret;
}

int ObPLAssocArray::reserve_assoc_key()
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(get_allocator()));
  CK (get_count() != 0);
  CK (OB_ISNULL(get_key()) && OB_ISNULL(get_sort()));

#define RESERVE_ASSOC_ARRAY(TYPE, PROPERTY) \
  do { \
    if (OB_SUCC(ret)) { \
      TYPE *addr = static_cast<TYPE *>(get_allocator()->alloc(sizeof(TYPE) * get_count())); \
      if (OB_ISNULL(addr)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("alloc failed", KPC(this), K(get_count()), K(sizeof(TYPE)), K(ret)); \
      } else { \
        set_##PROPERTY(addr); \
      } \
    } \
  } while(0)

  if (OB_SUCC(ret)) {
    RESERVE_ASSOC_ARRAY(ObObj, key);

    RESERVE_ASSOC_ARRAY(int64_t, sort);

    for (int64_t i = 0; OB_SUCC(ret) && i < get_count(); ++i) {
      OX (get_key(i)->set_int32(i + 1));
      OZ (set_sort(i, i));
    }
    OX (set_first(1));
    OX (set_last(get_count()));

    if (OB_FAIL(ret)) {
      if (nullptr != get_key()) {
        get_allocator()->free(get_key());
      }
      if (nullptr != get_sort()) {
        get_allocator()->free(get_sort());
      }
      set_key(nullptr);
      set_sort(nullptr);
    }
  }

#undef RESERVE_ASSOC_ARRAY
  return ret;
}

//---------- for ObPLVarray ----------

int ObPLVArray::deep_copy(ObPLCollection *src, ObIAllocator *allocator, bool ignore_del_element)
{
  int ret = OB_SUCCESS;
  ObPLVArray *src_va = NULL;
  CK (OB_NOT_NULL(src));
  CK (src->is_varray());
  OZ (ObPLCollection::deep_copy(src, allocator, ignore_del_element));
  CK (OB_NOT_NULL(src_va = static_cast<ObPLVArray*>(src)));
  OX (set_capacity(src_va->get_capacity()));
  return ret;
}

int ObPLVArray::get_serialize_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  OZ (ObPLCollection::get_serialize_size(size));
  OX (size += serialization::encoded_length(get_capacity()));
  return ret;
}

int ObPLVArray::serialize(char* buf, const int64_t len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  OZ (ObPLCollection::serialize(buf, len, pos));
  OZ (serialization::encode(buf, len, pos, get_capacity()));
  return ret;
}

int ObPLVArray::deserialize(common::ObIAllocator &allocator,
                                const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OZ (ObPLCollection::deserialize(allocator, buf, len, pos));
  OZ (serialization::decode(buf, len, pos, capacity_));
  return ret;
}
#endif

}  // namespace pl
}  // namespace oceanbase
