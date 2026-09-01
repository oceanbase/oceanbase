/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 *
 * SQL UDT convert orchestration templates (provider duck-typing).
 * PL TEXT serialize uses pl/ob_pl_user_type.ipp templates.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_PROVIDER_IPP_
#define OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_PROVIDER_IPP_

#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_user_type.ipp"
#include "pl/ob_pl_type.ipp"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
template <typename SCHEMA_PROVIDER>
int ObSqlUdtUtils::build_qualified_udt_name(
    SCHEMA_PROVIDER &schema_provider,
    const share::schema::ObUDTTypeInfo &udt_info,
    common::ObIAllocator &allocator,
    common::ObString &qualified_name)
{
  int ret = OB_SUCCESS;
  const share::schema::ObDatabaseSchema *db_schema = NULL;
  const ObString &type_name = udt_info.get_type_name();
  ObString db_name;
  const bool is_inner_udt = is_inner_pl_object_id(udt_info.get_type_id());

  if (is_inner_udt) {
    db_name = OB_ORA_SYS_SCHEMA_NAME;
  } else if (OB_FAIL(schema_provider.get_database_schema(udt_info.get_tenant_id(),
                                                         udt_info.get_database_id(),
                                                         db_schema))) {
    SQL_ENG_LOG(WARN, "get database schema fail", K(ret),
             K(udt_info.get_tenant_id()), K(udt_info.get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "database schema is null", K(ret));
  } else {
    db_name = db_schema->get_database_name_str();
  }
  if (OB_SUCC(ret) && db_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "database name is empty", K(ret));
  }
  if (OB_SUCC(ret)) {
    const int64_t max_len = db_name.length() + type_name.length() + 5;
    char *buf = static_cast<char *>(allocator.alloc(max_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "alloc qualified name buffer fail", K(ret), K(max_len));
    } else {
      int64_t pos = 0;
      buf[pos++] = '"';
      MEMCPY(buf + pos, db_name.ptr(), db_name.length());
      pos += db_name.length();
      buf[pos++] = '"';
      buf[pos++] = '.';
      buf[pos++] = '"';
      MEMCPY(buf + pos, type_name.ptr(), type_name.length());
      pos += type_name.length();
      buf[pos++] = '"';
      qualified_name.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObSqlUdtUtils::serialize_pl_extend_to_string(
    SCHEMA_PROVIDER &schema_provider,
    const sql::ObSQLSessionInfo &session,
    const common::ObTimeZoneInfo *tz_info,
    const pl::ObUserDefinedType *user_type,
    const common::ObString &root_qualified_name,
    common::ObObj &pl_obj,
    common::ObIAllocator &res_allocator,
    common::ObString &res_str)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(schema_provider, session, tz_info, user_type, root_qualified_name, pl_obj, res_allocator, res_str);
  ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "serialize pl obj to text not supported without oracle pl", K(ret));
#else
  static const int64_t INIT_UDT_TEXT_BUF_LEN = 4 * 1024;
  if (OB_ISNULL(user_type)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "user_type is null", K(ret));
  } else {
    ObArenaAllocator tmp_alloc("SqlUDT", OB_MALLOC_NORMAL_BLOCK_SIZE,
                               session.get_effective_tenant_id());
    int64_t dst_len = INIT_UDT_TEXT_BUF_LEN;
    bool done = false;
    const char *final_buf = NULL;
    int64_t final_pos = 0;
    while (OB_SUCC(ret) && !done) {
      char *dst = static_cast<char *>(tmp_alloc.alloc(dst_len));
      if (OB_ISNULL(dst)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "alloc text buffer fail", K(ret), K(dst_len));
      } else {
        int tmp_ret = OB_SUCCESS;
        int64_t dst_pos = 0;
        if (OB_UNLIKELY(dst_len < root_qualified_name.length() + 2)) {
          tmp_ret = OB_SIZE_OVERFLOW;
        } else {
          MEMCPY(dst + dst_pos, root_qualified_name.ptr(), root_qualified_name.length());
          dst_pos += root_qualified_name.length();
          dst[dst_pos++] = '(';
        }
        if (OB_SUCCESS == tmp_ret) {
          ObObj shadow = pl_obj;
          char *src = reinterpret_cast<char *>(&shadow);
          tmp_ret = user_type->serialize(schema_provider, session, tz_info,
              obmysql::MYSQL_PROTOCOL_TYPE::TEXT, src, dst, dst_len, dst_pos, true);
        }
        if (OB_SUCCESS == tmp_ret) {
          if (OB_UNLIKELY(dst_len - dst_pos < 1)) {
            tmp_ret = OB_SIZE_OVERFLOW;
          } else {
            dst[dst_pos++] = ')';
          }
        }
        if (OB_SIZE_OVERFLOW == tmp_ret) {
          if (dst_len >= OB_MAX_LONGTEXT_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            SQL_ENG_LOG(WARN, "udt text buffer exceed limit", K(ret), K(dst_len));
          } else {
            dst_len = std::min(dst_len * 2, OB_MAX_LONGTEXT_LENGTH);
            tmp_alloc.reuse();
          }
        } else if (OB_SUCCESS == tmp_ret) {
          final_buf = dst;
          final_pos = dst_pos;
          done = true;
        } else {
          ret = tmp_ret;
          SQL_ENG_LOG(WARN, "user_type serialize text fail", K(ret), KPC(user_type));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == final_pos) {
        res_str.reset();
      } else {
        char *res_buf = static_cast<char *>(res_allocator.alloc(final_pos));
        if (OB_ISNULL(res_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "alloc res buf fail", K(ret), K(final_pos));
        } else {
          MEMCPY(res_buf, final_buf, final_pos);
          res_str.assign_ptr(res_buf, static_cast<int32_t>(final_pos));
        }
      }
    }
  }
#endif
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObSqlUdtUtils::convert_sql_udt_to_string(
    common::ObIAllocator &res_allocator,
    SCHEMA_PROVIDER &schema_provider,
    const sql::ObSQLSessionInfo &session,
    const common::ObTimeZoneInfo *tz_info,
    const uint64_t udt_id,
    const common::ObObj &sql_udt_obj,
    common::ObString &res_str,
    const bool has_lob_header)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(res_allocator, schema_provider, session, tz_info, udt_id, sql_udt_obj, res_str, has_lob_header);
  ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "convert sql udt to text not supported without oracle pl", K(ret));
#else
  if (OB_UNLIKELY(!(sql_udt_obj.is_user_defined_sql_type()
                    || (sql_udt_obj.is_string_type() && !has_lob_header)))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "input obj is not user defined sql type", K(ret), K(sql_udt_obj));
  } else if (OB_UNLIKELY(OB_INVALID_ID == udt_id)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid udt_id", K(ret), K(udt_id));
  } else {
    ObArenaAllocator tmp_alloc("SqlUDT", OB_MALLOC_NORMAL_BLOCK_SIZE, session.get_effective_tenant_id());
    ObString udt_data = sql_udt_obj.get_string();
    if (has_lob_header && OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &tmp_alloc, ObLongTextType, CS_TYPE_BINARY, true, udt_data))) {
      SQL_ENG_LOG(WARN, "read real udt data fail", K(ret), K(sql_udt_obj));
    } else if (udt_data.empty()) {
      res_str.assign_ptr("NULL", 4);
    } else {
      ObObj pl_obj;
      int64_t pos = 0;
      bool need_destruct = false;
      if (OB_FAIL(pl::ObUserDefinedType::do_deserialize_obj(tmp_alloc, pl_obj, udt_data.ptr(),
                     udt_data.length(), pos, false))) {
        SQL_ENG_LOG(WARN, "deserialize sql udt byte stream to pl extend fail", K(ret), K(udt_data.length()));
      } else {
        need_destruct = true;
        if (pl_obj.is_null() || 0 == pl_obj.get_ext()) {
          res_str.assign_ptr("NULL", 4);
        } else {
          const share::schema::ObUDTTypeInfo *udt_info = NULL;
          const pl::ObUserDefinedType *user_type = NULL;
          const uint64_t udt_tenant_id = is_inner_pl_object_id(udt_id)
                                         ? OB_SYS_TENANT_ID
                                         : session.get_effective_tenant_id();
          if (OB_FAIL(schema_provider.get_udt_info(udt_tenant_id, udt_id, udt_info))) {
            SQL_ENG_LOG(WARN, "get udt info fail", K(ret), K(udt_tenant_id), K(udt_id));
          } else if (OB_ISNULL(udt_info)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "udt info not found", K(ret), K(udt_tenant_id), K(udt_id));
          } else if (OB_FAIL(udt_info->transform_to_pl_type(tmp_alloc,
                             schema_provider, user_type))) {
            SQL_ENG_LOG(WARN, "transform udt info to pl type fail", K(ret), K(udt_id));
          } else if (OB_ISNULL(user_type)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "user_type is null after transform", K(ret), K(udt_id));
          } else {
            ObString root_qualified_name;
            if (OB_FAIL(build_qualified_udt_name(schema_provider, *udt_info,
                            tmp_alloc, root_qualified_name))) {
              SQL_ENG_LOG(WARN, "build qualified udt name fail", K(ret), K(udt_id));
            } else if (OB_FAIL(serialize_pl_extend_to_string(
                           schema_provider, session, tz_info, user_type,
                           root_qualified_name, pl_obj, res_allocator, res_str))) {
              SQL_ENG_LOG(WARN, "serialize pl obj to text fail", K(ret), K(udt_id));
            }
          }
        }
      }
      if (need_destruct) {
        int tmp_ret = pl::ObUserDefinedType::destruct_obj(pl_obj, nullptr);
        if (OB_SUCCESS != tmp_ret) {
          SQL_ENG_LOG(WARN, "destruct pl extend after udt text serialize fail", K(tmp_ret), K(ret));
          if (OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
#endif
  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_OB_EXPR_SQL_UDT_UTILS_PROVIDER_IPP_ */
