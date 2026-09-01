/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 *
 * ObPLDataType::serialize template (schema duck-typing).
 */

#ifndef OCEANBASE_PL_OB_PL_TYPE_IPP_
#define OCEANBASE_PL_OB_PL_TYPE_IPP_

#include "observer/mysql/obsm_utils.h"
#include "observer/mysql/ob_query_driver.h"
#include "share/schema/ob_udt_info.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{

template <typename SCHEMA_PROVIDER>
int ObPLDataType::serialize(
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
  if (is_obj_type()) {
    obmysql::EMySQLFieldType mysql_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED;
    uint16_t flags = 0;
    ObScale num_decimals = 0;
    ObObj obj;
    ObField field;
    if (OB_FAIL(ObSMUtils::get_mysql_type(get_obj_type(), mysql_type, flags, num_decimals))) {
      PL_LOG(WARN, "get mysql type failed", K(ret), K(get_obj_type()));
    } else {
      obj = *(reinterpret_cast<ObObj *>(src));
      src += sizeof(ObObj);
      field.accuracy_ = get_data_type()->get_accuracy();
      field.flags_ = get_data_type()->is_zero_fill() ? ZEROFILL_FLAG : 0;
    }
    if (OB_SUCC(ret) && !obj.is_invalid_type() && !obj.is_null()) {
      if (obj.get_type() != get_obj_type()) {
        ret = OB_ERR_UNEXPECTED;
        PL_LOG(WARN, "data type inconsistent with pl type", K(ret), K(obj), K(*this));
      } else if (obj.is_lob() || obj.is_lob_locator() || obj.is_json() || obj.is_geometry()) {
        ObArenaAllocator local_allocator("PlDataType", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                          session.get_effective_tenant_id());
        if (OB_FAIL(observer::ObQueryDriver::process_lob_locator_results(
                obj, session.is_client_use_lob_locator(),
                session.is_client_support_lob_locatorv2(),
                &local_allocator, &session, NULL))) {
          PL_LOG(WARN, "failed to process lob locator", K(ret));
        } else if (OB_FAIL(ObSMUtils::cell_str(dst, dst_len, obj, type, dst_pos, OB_INVALID_ID,
                       NULL, tz_info, &field, session, NULL))) {
          PL_LOG(WARN, "failed to cell str", K(ret));
        }
      } else if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type && obj.is_interval_ym()) {
        if (OB_FAIL(intervalym_element_cell_str(dst, dst_len, obj.get_interval_ym(), dst_pos,
                       field.accuracy_.get_scale()))) {
          PL_LOG(WARN, "failed to cell intervalym", K(ret));
        }
      } else if (obmysql::MYSQL_PROTOCOL_TYPE::TEXT == type && obj.is_interval_ds()) {
        if (OB_FAIL(intervalds_element_cell_str(dst, dst_len, obj.get_interval_ds(), dst_pos,
                       field.accuracy_.get_scale()))) {
          PL_LOG(WARN, "failed to cell intervalds", K(ret));
        }
      } else if (OB_FAIL(ObSMUtils::cell_str(dst, dst_len, obj, type, dst_pos, OB_INVALID_ID,
                     NULL, tz_info, &field, session, NULL))) {
        PL_LOG(WARN, "failed to cell str", K(ret));
      }
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    const share::schema::ObUDTTypeInfo *udt_info = NULL;
    ObArenaAllocator local_allocator("SerUdtType", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     session.get_effective_tenant_id());
    const uint64_t tenant_id = is_inner_pl_object_id(get_user_type_id())
                               ? OB_SYS_TENANT_ID
                               : session.get_effective_tenant_id();
    if (!is_udt_type()) {
      ret = OB_NOT_SUPPORTED;
      PL_LOG(WARN, "not support other type except udt type", K(ret), K(get_type_from()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-schema user defined type deserialize");
    } else if (OB_FAIL(schema_provider.get_udt_info(tenant_id, get_user_type_id(), udt_info))) {
      PL_LOG(WARN, "failed to get udt info", K(ret), K(tenant_id), K(get_user_type_id()));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "udt info is null", K(ret), K(get_user_type_id()));
    } else if (OB_FAIL(udt_info->transform_to_pl_type(local_allocator, schema_provider, user_type))) {
      PL_LOG(WARN, "failed to transform to pl type", K(ret), KPC(udt_info));
    } else if (OB_ISNULL(user_type)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN, "user type is null", K(ret));
    } else if (OB_FAIL(user_type->serialize(schema_provider, session, tz_info, type, src, dst,
                       dst_len, dst_pos, full_format))) {
      PL_LOG(WARN, "failed to serialize user type", K(ret));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase

#endif /* OCEANBASE_PL_OB_PL_TYPE_IPP_ */
