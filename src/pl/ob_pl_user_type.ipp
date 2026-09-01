/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 *
 * Template serialize implementations for schema duck-typing (Guard / CDC dict ctx).
 * Use PL_LOG; do not rely on USING_LOG_PREFIX / LOG_WARN in header-included templates.
 */

#ifndef OCEANBASE_PL_OB_PL_USER_TYPE_IPP_
#define OCEANBASE_PL_OB_PL_USER_TYPE_IPP_

#include "share/schema/ob_udt_info.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{

template <typename SCHEMA_PROVIDER>
int ObUserDefinedType::text_protocol_prefix_info_for_each_item(
    SCHEMA_PROVIDER &schema_provider,
    const uint64_t tenant_id,
    const ObPLDataType &type,
    char *buf,
    const int64_t len,
    int64_t &pos,
    const bool full_format) const
{
  int ret = OB_SUCCESS;
  if (type.is_collection_type() || type.is_record_type()) {
    const ObUDTTypeInfo *udt_info = NULL;
    const uint64_t udt_id = type.get_user_type_id();
    const bool is_inner_udt = is_inner_pl_object_id(udt_id);
    const uint64_t resolved_tenant_id = is_inner_udt ? OB_SYS_TENANT_ID : tenant_id;
    const_cast<ObPLDataType&>(type).set_charset(get_charset());

    if (!is_udt_type()) {
      ret = OB_NOT_SUPPORTED;
      PL_LOG(WARN, "not support other type except udt type", K(ret), K(get_type_from()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-schema user defined type deserialize");
    } else if (OB_FAIL(schema_provider.get_udt_info(resolved_tenant_id, udt_id, udt_info))) {
      PL_LOG(WARN, "failed to get udt info", K(ret), K(resolved_tenant_id), K(udt_id));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "udt info is null", K(ret), K(udt_id));
    } else if (!full_format) {
      if (len - pos < udt_info->get_type_name().length() + 1) {
        ret = OB_SIZE_OVERFLOW;
        PL_LOG(WARN, "buffer length is not enough", K(udt_info->get_type_name()), K(len));
      } else {
        MEMCPY(buf + pos, udt_info->get_type_name().ptr(), udt_info->get_type_name().length());
        pos += udt_info->get_type_name().length();
        MEMCPY(buf + pos, "(", 1);
        pos += 1;
      }
    } else {
      const ObString &type_name = udt_info->get_type_name();
      ObString db_name;
      const share::schema::ObDatabaseSchema *db_schema = NULL;
      if (is_inner_udt) {
        db_name = OB_ORA_SYS_SCHEMA_NAME;
      } else if (OB_FAIL(schema_provider.get_database_schema(udt_info->get_tenant_id(),
                                                            udt_info->get_database_id(),
                                                            db_schema))) {
        PL_LOG(WARN, "get database schema fail", K(ret),
               K(udt_info->get_tenant_id()), K(udt_info->get_database_id()));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "database schema is null", K(ret));
      } else {
        db_name = db_schema->get_database_name_str();
      }
      if (OB_SUCC(ret) && db_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "database name is empty", K(ret));
      }
      if (OB_SUCC(ret)) {
        const int64_t need_len = db_name.length() + type_name.length() + 5;
        if (len - pos < need_len) {
          ret = OB_SIZE_OVERFLOW;
          PL_LOG(WARN, "buffer length is not enough for qualified udt name", K(ret), K(need_len));
        } else {
          buf[pos++] = '"';
          MEMCPY(buf + pos, db_name.ptr(), db_name.length());
          pos += db_name.length();
          buf[pos++] = '"';
          buf[pos++] = '.';
          buf[pos++] = '"';
          MEMCPY(buf + pos, type_name.ptr(), type_name.length());
          pos += type_name.length();
          buf[pos++] = '"';
          buf[pos++] = '(';
        }
      }
    }
  } else if (NULL != type.get_meta_type()
             && (type.get_meta_type()->is_string_or_lob_locator_type()
                 || type.get_meta_type()->is_oracle_temporal_type()
                 || type.get_meta_type()->is_raw())) {
    if (len - pos < 1) {
      ret = OB_SIZE_OVERFLOW;
      PL_LOG(WARN, "buffer length is not enough", K(type_name_), K(len));
    } else {
      MEMCPY(buf + pos, "'", 1);
      pos += 1;
    }
  }
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObRecordType::serialize(
    SCHEMA_PROVIDER &schema_provider,
    const sql::ObSQLSessionInfo &session,
    const ObTimeZoneInfo *tz_info,
    obmysql::MYSQL_PROTOCOL_TYPE protocl_type,
    char *&src,
    char *dst,
    const int64_t dst_len,
    int64_t &dst_pos,
    const bool full_format) const
{
  int ret = OB_SUCCESS;
  int64_t bitmap_bytes = (record_members_.count() + 7 + 2) / 8;
  char *bitmap = NULL;
  ObObj *src_obj = reinterpret_cast<ObObj*>(src);
  ObPLRecord *record = NULL;
  char *new_src = NULL;

  if (dst_len - dst_pos < bitmap_bytes) {
    ret = OB_SIZE_OVERFLOW;
    PL_LOG(WARN, "size overflow", K(ret), K(dst_len), K(dst_pos), K(bitmap_bytes));
  } else if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == protocl_type) {
    bitmap = dst + dst_pos;
    MEMSET(dst + dst_pos, 0, bitmap_bytes);
    dst_pos += bitmap_bytes;
  }
  if (OB_ISNULL(src_obj)) {
    ret = OB_ERR_UNEXPECTED;
    PL_LOG(WARN, "src_obj is null", K(ret));
  } else if (OB_SUCC(ret) && src_obj->is_ext()) {
    record = reinterpret_cast<ObPLRecord*>(src_obj->get_ext());
    if (OB_ISNULL(record)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "record is null", K(ret));
    } else if (OB_ISNULL(new_src = reinterpret_cast<char*>(record->get_element()))) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "record element is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < record_members_.count(); ++i) {
      const ObPLDataType *type = get_record_member_type(i);
      ObObj *obj = reinterpret_cast<ObObj *>(new_src);
      if (OB_ISNULL(type) || OB_ISNULL(obj)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "invalid record member", K(ret), K(i));
      } else if (ObPLComposite::obj_is_null(obj)) {
        if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == protocl_type) {
          obmysql::ObMySQLUtil::update_null_bitmap(bitmap, i);
          new_src += sizeof(ObObj);
        } else if (dst_len - dst_pos < 4) {
          ret = OB_SIZE_OVERFLOW;
          PL_LOG(WARN, "size overflow", K(ret), K(dst_len), K(dst_pos));
        } else {
          MEMCPY(dst + dst_pos, "NULL", 4);
          dst_pos += 4;
          new_src += sizeof(ObObj);
        }
      } else if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == protocl_type
                 && OB_FAIL(text_protocol_prefix_info_for_each_item(schema_provider,
                        session.get_effective_tenant_id(), *type, dst, dst_len - dst_pos, dst_pos,
                        full_format))) {
        PL_LOG(WARN, "set text protocol prefix info fail", K(ret), K(get_name()));
      } else if (type->is_collection_type()) {
#ifdef OB_BUILD_ORACLE_PL
        char *coll_src = reinterpret_cast<char*>(obj->get_ext());
        ObPLCollection *coll_table = reinterpret_cast<ObPLCollection *>(coll_src);
        if (!obj->is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "collection obj is not extend", K(ret), K(i));
        } else if (OB_ISNULL(coll_table) || OB_ISNULL(coll_src)) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "collection src is null", K(ret), K(i));
        } else if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == protocl_type && !coll_table->is_inited()) {
          obmysql::ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else if (OB_FAIL(type->serialize(schema_provider, session, tz_info, protocl_type,
                       new_src, dst, dst_len, dst_pos, full_format))) {
          PL_LOG(WARN, "serialize collection member fail", K(ret), K(i));
        }
#endif
      } else {
        int64_t offset_dst_pos = dst_pos;
        bool has_serialized = false;
        if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == protocl_type
            && OB_FAIL(base_type_serialize_for_text(obj, tz_info, dst, dst_len, dst_pos,
                       has_serialized, session))) {
          PL_LOG(WARN, "serialize for text fail", K(ret), K(has_serialized));
        } else if (!has_serialized) {
          if (OB_FAIL(type->serialize(schema_provider, session, tz_info, protocl_type, new_src,
                         dst, dst_len, dst_pos, full_format))) {
            PL_LOG(WARN, "serialize record member fail", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && obmysql::MYSQL_PROTOCOL_TYPE::TEXT == protocl_type
            && !type->is_record_type()) {
          if (OB_FAIL(text_protocol_base_type_convert(*type, dst, offset_dst_pos, dst_len))) {
            PL_LOG(WARN, "text_protocol_base_type_convert fail", K(ret));
          } else {
            dst_pos = offset_dst_pos;
          }
        }
      }
      if (OB_SUCC(ret) && obmysql::MYSQL_PROTOCOL_TYPE::TEXT == protocl_type
          && !obj->is_invalid_type()) {
        if (OB_FAIL(text_protocol_suffix_info_for_each_item(*type, dst, dst_len - dst_pos, dst_pos,
                       i < record_members_.count() - 1 ? false : true,
                       ObPLComposite::obj_is_null(obj)))) {
          PL_LOG(WARN, "text_protocol_suffix fail", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    src += sizeof(ObObj);
  }
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObCollectionType::serialize(
    SCHEMA_PROVIDER &schema_provider,
    const sql::ObSQLSessionInfo &session,
    const ObTimeZoneInfo *tz_info,
    obmysql::MYSQL_PROTOCOL_TYPE type,
    char *&src,
    char *dst,
    const int64_t dst_len,
    int64_t &dst_pos,
    const bool full_format) const
{
  int ret = OB_SUCCESS;
  ObObj *src_obj = NULL;
  ObPLCollection *table = NULL;
  if (OB_ISNULL(src_obj = reinterpret_cast<ObObj*>(src))) {
    ret = OB_ERR_UNEXPECTED;
    PL_LOG(WARN, "src is null", K(ret), KP(src_obj), KPC(this));
  } else if (!src_obj->is_ext()) {
    ret = OB_ERR_UNEXPECTED;
    PL_LOG(WARN, "src obj not pl extend", K(ret), KPC(src_obj), KPC(this));
  } else if (OB_ISNULL(table = reinterpret_cast<ObPLCollection *>(src_obj->get_ext()))) {
    ret = OB_ERR_UNEXPECTED;
    PL_LOG(WARN, "table is null", K(ret), KPC(table), KPC(this));
  } else if (!table->is_inited()) {
    // null handled by caller
  } else if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == type
             && OB_FAIL(obmysql::ObMySQLUtil::store_length(dst, dst_len, table->get_actual_count(), dst_pos))) {
    PL_LOG(WARN, "failed to store_length for table count", K(ret), KPC(this), KPC(table));
  } else {
    char *bitmap = NULL;
    int64_t bitmap_bytes = (table->get_actual_count() + 7 + 2) / 8;
    if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == type) {
      if ((dst_len - dst_pos) < bitmap_bytes) {
        ret = OB_SIZE_OVERFLOW;
        PL_LOG(WARN, "size overflow", K(ret), KPC(this), K(dst_len), K(dst_pos), K(bitmap_bytes));
      } else {
        bitmap = dst + dst_pos;
        MEMSET(dst + dst_pos, 0, bitmap_bytes);
        dst_pos += bitmap_bytes;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table->get_count(); ++i) {
      char *data = reinterpret_cast<char *>(table->get_data()) + (sizeof(ObObj) * i);
      ObObj *obj = reinterpret_cast<ObObj*>(data);
      if (OB_ISNULL(obj)) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "collection element is null", K(ret), K(i));
      } else if (obj->is_invalid_type()) {
        // deleted element
      } else if (ObPLComposite::obj_is_null(obj)) {
        if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == type) {
          obmysql::ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else if (dst_len - dst_pos < 4) {
          ret = OB_SIZE_OVERFLOW;
          PL_LOG(WARN, "size overflow", K(ret), K(dst_len), K(dst_pos));
        } else {
          MEMCPY(dst + dst_pos, "NULL", 4);
          dst_pos += 4;
        }
      } else if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type
                 && OB_FAIL(text_protocol_prefix_info_for_each_item(schema_provider,
                        session.get_effective_tenant_id(), element_type_, dst, dst_len - dst_pos,
                        dst_pos, full_format))) {
        PL_LOG(WARN, "set text protocol prefix info fail", K(ret), K(get_name()));
      } else if (element_type_.is_collection_type()) {
        char *coll_src = reinterpret_cast<char *>(obj->get_ext());
        ObPLCollection *coll_table = reinterpret_cast<ObPLCollection *>(coll_src);
        if (!obj->is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "nested collection obj is not extend", K(ret), K(i));
        } else if (OB_ISNULL(coll_src) || OB_ISNULL(coll_table)) {
          ret = OB_ERR_UNEXPECTED;
          PL_LOG(WARN, "nested collection src is null", K(ret), K(i));
        } else if (obmysql::MYSQL_PROTOCOL_TYPE::BINARY == type && !coll_table->is_inited()) {
          obmysql::ObMySQLUtil::update_null_bitmap(bitmap, i);
        } else if (OB_FAIL(element_type_.serialize(schema_provider, session, tz_info, type, data,
                       dst, dst_len, dst_pos, full_format))) {
          PL_LOG(WARN, "serialize nested collection fail", K(ret), K(i));
        }
      } else {
        int64_t offset_dst_pos = dst_pos;
        bool has_serialized = false;
        if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type
            && OB_FAIL(base_type_serialize_for_text(obj, tz_info, dst, dst_len, dst_pos,
                       has_serialized, session))) {
          PL_LOG(WARN, "serialize for text fail", K(ret), K(has_serialized));
        } else if (!has_serialized) {
          if (OB_FAIL(element_type_.serialize(schema_provider, session, tz_info, type, data, dst,
                         dst_len, dst_pos, full_format))) {
            PL_LOG(WARN, "serialize collection element fail", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type
            && !element_type_.is_record_type()) {
          if (OB_FAIL(text_protocol_base_type_convert(element_type_, dst, offset_dst_pos, dst_len))) {
            PL_LOG(WARN, "text_protocol_base_type_convert fail", K(ret));
          } else {
            dst_pos = offset_dst_pos;
          }
        }
      }
      if (OB_SUCC(ret) && obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type && !obj->is_invalid_type()) {
        if (OB_FAIL(text_protocol_suffix_info_for_each_item(element_type_, dst,
                       dst_len - dst_pos, dst_pos,
                       i < table->get_count() - 1 ? false : true,
                       ObPLComposite::obj_is_null(obj)))) {
          PL_LOG(WARN, "text_protocol_suffix fail", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      src += sizeof(ObObj);
    }
  }
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObUserDefinedType::serialize(
    SCHEMA_PROVIDER &schema_provider,
    const sql::ObSQLSessionInfo &session,
    const common::ObTimeZoneInfo *tz_info,
    obmysql::MYSQL_PROTOCOL_TYPE type,
    char *&src,
    char *dst,
    const int64_t dst_len,
    int64_t &dst_pos,
    const bool full_format) const
{
  int ret = OB_SUCCESS;
  if (is_record_type()) {
    ret = static_cast<const ObRecordType*>(this)->serialize(
        schema_provider, session, tz_info, type, src, dst, dst_len, dst_pos, full_format);
  } else if (is_collection_type()) {
    ret = static_cast<const ObCollectionType*>(this)->serialize(
        schema_provider, session, tz_info, type, src, dst, dst_len, dst_pos, full_format);
  } else {
    ret = OB_NOT_SUPPORTED;
    PL_LOG(WARN, "unsupported user type for serialize", K(ret), K(type_));
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase

#endif /* OCEANBASE_PL_OB_PL_USER_TYPE_IPP_ */
