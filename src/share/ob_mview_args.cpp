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

#include "ob_mview_args.h"

namespace oceanbase
{
using namespace share::schema;
namespace obrpc
{

bool ObMViewCompleteRefreshArg::is_valid() const
{
  bool bret = OB_INVALID_TENANT_ID != exec_tenant_id_ &&
              !based_schema_object_infos_.empty() &&
              OB_INVALID_TENANT_ID != tenant_id_ &&
              OB_INVALID_ID != table_id_;
  for (int64_t i = 0; bret && i < based_schema_object_infos_.count(); ++i) {
    const ObBasedSchemaObjectInfo &based_info = based_schema_object_infos_.at(i);
    bret = (OB_INVALID_TENANT_ID == based_info.schema_tenant_id_ ||
            tenant_id_ == based_info.schema_tenant_id_) &&
           OB_INVALID_ID != based_info.schema_id_ &&
           ObSchemaType::TABLE_SCHEMA == based_info.schema_type_ &&
           OB_INVALID_VERSION != based_info.schema_version_;
  }
  return bret;
}

void ObMViewCompleteRefreshArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  session_id_ = OB_INVALID_ID;
  sql_mode_ = 0;
  last_refresh_scn_.reset();
  tz_info_.reset();
  tz_info_wrap_.reset();
  for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
    nls_formats_[i].reset();
  }
  parent_task_id_ = 0;
  allocator_.reset();
  ObDDLArg::reset();
}

int ObMViewCompleteRefreshArg::assign(const ObMViewCompleteRefreshArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ObDDLArg::assign(other))) {
      LOG_WARN("fail to assign ddl arg", KR(ret));
    } else {
      tenant_id_ = other.tenant_id_;
      table_id_ = other.table_id_;
      session_id_ = other.session_id_;
      sql_mode_ = other.sql_mode_;
      last_refresh_scn_ = other.last_refresh_scn_;
      parent_task_id_ = other.parent_task_id_;
      if (OB_FAIL(tz_info_.assign(other.tz_info_))) {
        LOG_WARN("fail to assign tz info", KR(ret), "tz_info", other.tz_info_);
      } else if (OB_FAIL(tz_info_wrap_.deep_copy(other.tz_info_wrap_))) {
        LOG_WARN("fail to deep copy tz info wrap", KR(ret), "tz_info_wrap", other.tz_info_wrap_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
        if (OB_FAIL(ob_write_string(allocator_, other.nls_formats_[i], nls_formats_[i]))) {
          LOG_WARN("fail to deep copy nls format", KR(ret), K(i), "nls_format", other.nls_formats_[i]);
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObMViewCompleteRefreshArg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    BASE_SER((, ObDDLArg));
    LST_DO_CODE(OB_UNIS_ENCODE,
                tenant_id_,
                table_id_,
                session_id_,
                sql_mode_,
                last_refresh_scn_,
                tz_info_,
                tz_info_wrap_);
    OB_UNIS_ENCODE_ARRAY(nls_formats_, ObNLSFormatEnum::NLS_MAX);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, parent_task_id_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObMViewCompleteRefreshArg)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t nls_formats_count = -1;
  ObString nls_formats[ObNLSFormatEnum::NLS_MAX];
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              table_id_,
              session_id_,
              sql_mode_,
              last_refresh_scn_,
              tz_info_,
              tz_info_wrap_);
  OB_UNIS_DECODE(nls_formats_count);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(ObNLSFormatEnum::NLS_MAX != nls_formats_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nls formats count", KR(ret), K(nls_formats_count));
    }
    OB_UNIS_DECODE_ARRAY(nls_formats, nls_formats_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < nls_formats_count; i++) {
      if (OB_FAIL(ob_write_string(allocator_, nls_formats[i], nls_formats_[i]))) {
        LOG_WARN("fail to deep copy nls format", KR(ret), K(i), K(nls_formats[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, parent_task_id_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMViewCompleteRefreshArg)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    BASE_ADD_LEN((, ObDDLArg));
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                tenant_id_,
                table_id_,
                session_id_,
                sql_mode_,
                last_refresh_scn_,
                tz_info_,
                tz_info_wrap_);
    OB_UNIS_ADD_LEN_ARRAY(nls_formats_, ObNLSFormatEnum::NLS_MAX);
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, parent_task_id_);
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewCompleteRefreshRes,
                    task_id_,
                    trace_id_);

bool ObMViewRefreshInfo::is_valid() const
{
  return OB_INVALID_ID != mview_table_id_ &&
         refresh_scn_.is_valid() &&
         OB_INVALID_TIMESTAMP != start_time_ &&
         (!last_refresh_scn_.is_valid() || last_refresh_scn_ < refresh_scn_);
}

void ObMViewRefreshInfo::reset()
{
  mview_table_id_ = OB_INVALID_ID;
  last_refresh_scn_.reset();
  refresh_scn_.reset();
  start_time_ = OB_INVALID_TIMESTAMP;
  is_mview_complete_refresh_ = false;
}

int ObMViewRefreshInfo::assign(const ObMViewRefreshInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    mview_table_id_ = other.mview_table_id_;
    last_refresh_scn_ = other.last_refresh_scn_;
    refresh_scn_ = other.refresh_scn_;
    start_time_ = other.start_time_;
    is_mview_complete_refresh_ = other.is_mview_complete_refresh_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshInfo,
                    mview_table_id_,
                    last_refresh_scn_,
                    refresh_scn_,
                    start_time_,
                    is_mview_complete_refresh_);

bool ObAlterMViewArg::is_valid() const
{
  bool is_valid = true;
  if (is_alter_refresh_method_ && ObMVRefreshMethod::MAX <= refresh_method_) {
    is_valid = false;
  } else if (is_alter_refresh_dop_ && refresh_dop_ <= 0) {
    is_valid = false;
  } else if (is_alter_refresh_start_ && !start_time_.is_timestamp()) {
    is_valid = false;
  } else if (is_alter_refresh_next_ && next_time_expr_.empty()) {
    is_valid = false;
  }
  return is_valid;
}

void ObAlterMViewArg::reset()
{
  exec_env_.reset();
  is_alter_on_query_computation_ = false;
  enable_on_query_computation_ = false;
  is_alter_query_rewrite_ = false;
  enable_query_rewrite_ = false;
  is_alter_refresh_method_ = false;
  refresh_method_ = ObMVRefreshMethod::MAX;
  is_alter_refresh_dop_ = false;
  refresh_dop_ = 0;
  is_alter_refresh_start_ = false;
  start_time_.reset();
  is_alter_refresh_next_ = false;
  next_time_expr_.reset();
}

int ObAlterMViewArg::assign(const ObAlterMViewArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    exec_env_ = other.exec_env_;
    is_alter_on_query_computation_ = other.is_alter_on_query_computation_;
    enable_on_query_computation_ = other.enable_on_query_computation_;
    is_alter_query_rewrite_ = other.is_alter_query_rewrite_;
    enable_query_rewrite_ = other.enable_query_rewrite_;
    is_alter_refresh_method_ = other.is_alter_refresh_method_;
    refresh_method_ = other.refresh_method_;
    is_alter_refresh_dop_ = other.is_alter_refresh_dop_;
    refresh_dop_ = other.refresh_dop_;
    is_alter_refresh_start_ = other.is_alter_refresh_start_;
    start_time_ = other.start_time_;
    is_alter_refresh_next_ = other.is_alter_refresh_next_;
    next_time_expr_ = other.next_time_expr_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterMViewArg,
                    exec_env_,
                    is_alter_on_query_computation_,
                    enable_on_query_computation_,
                    is_alter_query_rewrite_,
                    enable_query_rewrite_,
                    is_alter_refresh_method_,
                    refresh_method_,
                    is_alter_refresh_dop_,
                    refresh_dop_,
                    is_alter_refresh_start_,
                    start_time_,
                    is_alter_refresh_next_,
                    next_time_expr_);

bool ObAlterMLogArg::is_valid() const
{
  bool is_valid = true;
  if (is_alter_table_dop_ && table_dop_ <= 0) {
    is_valid = false;
  } else if (is_alter_purge_start_ && !start_time_.is_timestamp()) {
    is_valid = false;
  } else if (is_alter_purge_next_ && next_time_expr_.empty()) {
    is_valid = false;
  } else if (is_alter_lob_threshold_ && lob_threshold_ <= 0) {
    is_valid = false;
  }
  return is_valid;
}

void ObAlterMLogArg::reset()
{
  exec_env_.reset();
  is_alter_table_dop_ = false;
  table_dop_ = 0;
  is_alter_purge_start_ = false;
  start_time_.reset();
  is_alter_purge_next_ = false;
  next_time_expr_.reset();
  is_alter_lob_threshold_ = false;
  lob_threshold_ = 0;
}

int ObAlterMLogArg::assign(const ObAlterMLogArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    exec_env_ = other.exec_env_;
    is_alter_table_dop_ = other.is_alter_table_dop_;
    table_dop_ = other.table_dop_;
    is_alter_purge_start_ = other.is_alter_purge_start_;
    start_time_ = other.start_time_;
    is_alter_purge_next_ = other.is_alter_purge_next_;
    next_time_expr_ = other.next_time_expr_;
    is_alter_lob_threshold_ = other.is_alter_lob_threshold_;
    lob_threshold_ = other.lob_threshold_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterMLogArg,
                    exec_env_,
                    is_alter_table_dop_,
                    table_dop_,
                    is_alter_purge_start_,
                    start_time_,
                    is_alter_purge_next_,
                    next_time_expr_,
                    is_alter_lob_threshold_,
                    lob_threshold_);

} // namespace obrpc
} // namespace oceanbase
