
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
#define USING_LOG_PREFIX SHARE

#include "ob_create_hidden_tablev2_rpc_struct.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace obrpc
{
bool ObCreateHiddenTableArgV2::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_
          && OB_INVALID_TENANT_ID != exec_tenant_id_
          && OB_INVALID_ID != table_id_
          && OB_INVALID_ID != dest_tenant_id_
          && share::DDL_INVALID != ddl_type_);
}

int ObCreateHiddenTableArgV2::assign(const ObCreateHiddenTableArgV2 &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.assign(other.tz_info_))) {
    LOG_WARN("tz_info assign failed", KR(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(other.tz_info_wrap_))) {
    LOG_WARN("failed to deep_copy tz info wrap", KR(ret), "tz_info_wrap", other.tz_info_wrap_);
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(other.ddl_stmt_str_.ptr(), static_cast<int32_t>(other.ddl_stmt_str_.length())))) {
    // do nothing
  } else if (OB_FAIL(nls_formats_.assign(other.nls_formats_))) {
    LOG_WARN("fail to assign nls formats", KR(ret));
  } else if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("fail to assign tablet_ids", KR(ret));
  } else {
    exec_tenant_id_ = other.exec_tenant_id_;
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    consumer_group_id_ = other.consumer_group_id_;
    dest_tenant_id_ = other.dest_tenant_id_;
    session_id_ = other.session_id_;
    parallelism_ = other.parallelism_;
    ddl_type_ = other.ddl_type_;
    sql_mode_ = other.sql_mode_;
    foreign_key_checks_ = other.foreign_key_checks_;
  }
  return ret;
}

int ObCreateHiddenTableArgV2::assign(const ObCreateHiddenTableArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.assign(other.get_tz_info()))) {
    LOG_WARN("tz_info assign failed", KR(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(other.get_tz_info_wrap()))) {
    LOG_WARN("failed to deep_copy tz info wrap", KR(ret), "tz_info_wrap", other.get_tz_info_wrap());
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(other.get_ddl_stmt_str().ptr(), static_cast<int32_t>(other.get_ddl_stmt_str().length())))) {
    // do nothing
  } else if (OB_FAIL(tablet_ids_.assign(other.get_tablet_ids()))) {
    LOG_WARN("fail to assign tablet_ids", KR(ret));
  } else {
    exec_tenant_id_ = other.get_exec_tenant_id();
    tenant_id_ = other.get_tenant_id();
    table_id_ = other.get_table_id();
    consumer_group_id_ = other.get_consumer_group_id();
    dest_tenant_id_ = other.get_dest_tenant_id();
    session_id_ = other.get_session_id();
    parallelism_ = other.get_parallelism();
    ddl_type_ = other.get_ddl_type();
    sql_mode_ = other.get_sql_mode();
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(nls_formats_.push_back(other.get_nls_formats()[i]))) {
        LOG_WARN("fail to deep copy nls format", KR(ret), K(i), "nls_format", other.get_nls_formats()[i]);
      }
    }
    foreign_key_checks_ = other.get_foreign_key_checks();
  }
  return ret;
}

int ObCreateHiddenTableArgV2::init(const uint64_t tenant_id, const uint64_t dest_tenant_id, uint64_t exec_tenant_id,
                                   const uint64_t table_id, const int64_t consumer_group_id, const uint64_t session_id,
                                   const int64_t parallelism, const share::ObDDLType ddl_type, const ObSQLMode sql_mode,
                                   const ObTimeZoneInfo &tz_info, const common::ObString &local_nls_date,
                                   const common::ObString &local_nls_timestamp, const common::ObString &local_nls_timestamp_tz,
                                   const ObTimeZoneInfoWrap &tz_info_wrap, const ObIArray<ObTabletID> &tablet_ids,
                                   const bool foreign_key_checks)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(tz_info_wrap_.deep_copy(tz_info_wrap))) {
    LOG_WARN("failed to deep copy tz_info_wrap", KR(ret));
  } else if (OB_FAIL(nls_formats_.push_back(local_nls_date))) {
    LOG_WARN("fail to push back local_nls_date", KR(ret));
  } else if (OB_FAIL(nls_formats_.push_back(local_nls_timestamp))) {
    LOG_WARN("fail to push back local_nls_timestamp", KR(ret));
  } else if (OB_FAIL(nls_formats_.push_back(local_nls_timestamp_tz))) {
    LOG_WARN("fail to push back local_nls_timestamp_tz", KR(ret));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign tablet ids", KR(ret), K(tablet_ids));
  } else {
    exec_tenant_id_ = exec_tenant_id;
    tenant_id_ = tenant_id;
    dest_tenant_id_ = dest_tenant_id;
    consumer_group_id_ = consumer_group_id;
    table_id_ = table_id;
    parallelism_ = parallelism;
    ddl_type_ = ddl_type;
    session_id_ = session_id;
    sql_mode_ = sql_mode;
    tz_info_ = tz_info;
    // load data no need to reorder column id
    foreign_key_checks_ = DATA_CURRENT_VERSION >= DATA_VERSION_4_3_5_1 ? (is_oracle_mode() || (is_mysql_mode() && foreign_key_checks)) : true;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateHiddenTableArgV2, ObDDLArg),
                     tenant_id_,
                     table_id_,
                     consumer_group_id_,
                     dest_tenant_id_,
                     session_id_,
                     parallelism_,
                     ddl_type_,
                     sql_mode_,
                     tz_info_,
                     tz_info_wrap_,
                     nls_formats_,
                     tablet_ids_,
                     foreign_key_checks_);

bool ObCreateHiddenTableArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_
          && OB_INVALID_TENANT_ID != exec_tenant_id_
          && OB_INVALID_ID != table_id_
          && OB_INVALID_ID != dest_tenant_id_
          && share::DDL_INVALID != ddl_type_);
}

int ObCreateHiddenTableArg::assign(const ObCreateHiddenTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.assign(arg.tz_info_))) {
    LOG_WARN("tz_info assign failed", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(arg.tz_info_wrap_))) {
    LOG_WARN("failed to deep_copy tz info wrap", K(ret), "tz_info_wrap", arg.tz_info_wrap_);
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(arg.ddl_stmt_str_.ptr(), static_cast<int32_t>(arg.ddl_stmt_str_.length())))) {
    // do nothing
  } else {
    tenant_id_ = arg.tenant_id_;
    table_id_ = arg.table_id_;
    consumer_group_id_ = arg.consumer_group_id_;
    dest_tenant_id_ = arg.dest_tenant_id_;
    session_id_ = arg.session_id_;
    parallelism_ = arg.parallelism_;
    ddl_type_ = arg.ddl_type_;
    sql_mode_ = arg.sql_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < common::ObNLSFormatEnum::NLS_MAX; i++) {
      nls_formats_[i].assign_ptr(arg.nls_formats_[i].ptr(), static_cast<int32_t>(arg.nls_formats_[i].length()));
    }
    OZ (tablet_ids_.assign(arg.tablet_ids_));
    need_reorder_column_id_ = arg.need_reorder_column_id_;
    foreign_key_checks_ = arg.foreign_key_checks_;
  }
  return ret;
}

int ObCreateHiddenTableArg::init(const uint64_t tenant_id, const uint64_t dest_tenant_id, uint64_t exec_tenant_id,
                                 const uint64_t table_id, const int64_t consumer_group_id, const uint64_t session_id,
                                 const int64_t parallelism, const share::ObDDLType ddl_type, const ObSQLMode sql_mode,
                                 const ObTimeZoneInfo &tz_info, const common::ObString &local_nls_date,
                                 const common::ObString &local_nls_timestamp, const common::ObString &local_nls_timestamp_tz,
                                 const ObTimeZoneInfoWrap &tz_info_wrap, const ObIArray<ObTabletID> &tablet_ids,
                                 const bool need_reorder_column_id, const bool foreign_key_checks)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(tz_info_wrap_.deep_copy(tz_info_wrap))) {
    LOG_WARN("failed to deep copy tz_info_wrap", KR(ret));
  } else if (FALSE_IT(nls_formats_[ObNLSFormatEnum::NLS_DATE].assign_ptr(local_nls_date.ptr(), static_cast<int32_t>(local_nls_date.length())))) {
    // do nothing
  } else if (FALSE_IT(nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP].assign_ptr(local_nls_timestamp.ptr(), static_cast<int32_t>(local_nls_timestamp.length())))) {
    // do nothing
  } else if (FALSE_IT(nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ].assign_ptr(local_nls_timestamp_tz.ptr(), static_cast<int32_t>(local_nls_timestamp_tz.length())))) {
    // do nothing
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign tablet ids", KR(ret), K(tablet_ids));
  } else {
    exec_tenant_id_ = exec_tenant_id;
    tenant_id_ = tenant_id;
    dest_tenant_id_ = dest_tenant_id;
    consumer_group_id_ = consumer_group_id;
    table_id_ = table_id;
    parallelism_ = parallelism;
    ddl_type_ = ddl_type;
    session_id_ = session_id;
    sql_mode_ = sql_mode;
    tz_info_ = tz_info;
    // load data no need to reorder column id
    need_reorder_column_id_ = need_reorder_column_id;
    foreign_key_checks_ = DATA_CURRENT_VERSION >= DATA_VERSION_4_3_5_1 ? (is_oracle_mode() || (is_mysql_mode() && foreign_key_checks)) : true;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObCreateHiddenTableArg)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObDDLArg::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize DDLArg", K(ret), K(buf_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                tenant_id_,
                table_id_,
                consumer_group_id_,
                dest_tenant_id_,
                session_id_,
                parallelism_,
                ddl_type_,
                sql_mode_,
                tz_info_,
                tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
        if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize nls_formats_[i]", K(ret), K(nls_formats_[i]));
        }
      }
    }
    if (OB_SUCC(ret)) {
      OB_UNIS_ENCODE(tablet_ids_);
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_ENCODE, need_reorder_column_id_, foreign_key_checks_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObCreateHiddenTableArg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize DDLArg", K(ret), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              table_id_,
              consumer_group_id_,
              dest_tenant_id_,
              session_id_,
              parallelism_,
              ddl_type_,
              sql_mode_,
              tz_info_,
              tz_info_wrap_);
    ObString tmp_string;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize nls_formats_", K(ret), K(i));
      } else if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(tmp_string.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory!", K(ret));
      } else {
        MEMCPY(tmp_ptr[i], tmp_string.ptr(), tmp_string.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], tmp_string.length());
        tmp_string.reset();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        allocator_.free(tmp_ptr[i]);
      }
    }
    if (OB_SUCC(ret)) {
      OB_UNIS_DECODE(tablet_ids_);
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, need_reorder_column_id_, foreign_key_checks_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCreateHiddenTableArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    len += ObDDLArg::get_serialize_size();
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                tenant_id_,
                table_id_,
                consumer_group_id_,
                dest_tenant_id_,
                session_id_,
                parallelism_,
                ddl_type_,
                sql_mode_,
                tz_info_,
                tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        len += nls_formats_[i].get_serialize_size();
      }
    }
    if (OB_SUCC(ret)) {
      OB_UNIS_ADD_LEN(tablet_ids_);
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, need_reorder_column_id_, foreign_key_checks_);
    }
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

}//end namespace obrpc
}//end namespace oceanbase