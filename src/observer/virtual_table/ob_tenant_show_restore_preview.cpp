// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#include "observer/virtual_table/ob_tenant_show_restore_preview.h"
#include "share/backup/ob_multi_backup_dest_util.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/string/ob_fixed_length_string.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {

ObTenantShowRestorePreview::ObTenantShowRestorePreview()
    : ObVirtualTableIterator(), is_inited_(false), idx_(-1), total_cnt_(0), backup_set_list_(), backup_piece_list_()
{}

ObTenantShowRestorePreview::~ObTenantShowRestorePreview()
{}

int ObTenantShowRestorePreview::init()
{
  int ret = OB_SUCCESS;
  ObObj backup_dest_value;
  ObObj cluster_name_value;
  ObString uri;
  ObString cluster_name_str;
  ObString cluster_id_str;
  uint64_t backup_tenant_id;
  int64_t restore_timestamp;
  ObArray<ObString> uri_list;
  char cluster_name_buf[OB_MAX_CLUSTER_NAME_LENGTH] = "";
  int64_t cluster_id = -1;
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (!session_->user_variable_exists(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR) ||
             !session_->user_variable_exists(OB_RESTORE_PREVIEW_TENANT_ID_SESSION_STR) ||
             !session_->user_variable_exists(OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR) ||
             !session_->user_variable_exists(OB_RESTORE_PREVIEW_BACKUP_CLUSTER_NAME_SESSION_STR) ||
             !session_->user_variable_exists(OB_RESTORE_PREVIEW_BACKUP_CLUSTER_ID_SESSION_STR)) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "no restore preview backup dest specified before", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "show restore preview do not specify backup dest");
  } else if (OB_FAIL(
                 session_->get_user_variable_value(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR, backup_dest_value)) ||
             OB_FAIL(session_->get_user_variable_value(
                 OB_RESTORE_PREVIEW_BACKUP_CLUSTER_NAME_SESSION_STR, cluster_name_value))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(backup_dest_value.get_varchar(uri))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(backup_dest_value));
  } else if (OB_FAIL(cluster_name_value.get_varchar(cluster_name_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(cluster_name_value));
  } else if (OB_FAIL(uri_list.push_back(uri))) {
    SHARE_LOG(WARN, "failed to push back", KR(ret));
  } else if (OB_FAIL(databuff_printf(cluster_name_buf,
                 OB_MAX_CLUSTER_NAME_LENGTH,
                 "%.*s",
                 cluster_name_str.length(),
                 cluster_name_str.ptr()))) {
    SHARE_LOG(WARN, "failed to databuff printf", KR(ret));
  } else if (OB_FAIL(parse_cluster_id_from_session(cluster_id))) {
    SHARE_LOG(WARN, "failed to parse cluster id from session", KR(ret));
  } else if (OB_FAIL(parse_tenant_id_from_session(backup_tenant_id))) {
    SHARE_LOG(WARN, "failed to parse tenant id from session", KR(ret));
  } else if (OB_FAIL(parse_restore_timestamp_from_session(restore_timestamp))) {
    SHARE_LOG(WARN, "failed to parse restore timestamp from session", KR(ret));
  } else if (OB_FAIL(ObMultiBackupDestUtil::get_multi_backup_path_list(true /*is_preview*/,
                 cluster_name_buf,
                 cluster_id,
                 backup_tenant_id,
                 restore_timestamp,
                 uri_list,
                 backup_set_list_,
                 backup_piece_list_))) {
    SHARE_LOG(WARN, "failed to get multi backup path list", KR(ret), K(backup_tenant_id), K(restore_timestamp));
  } else {
    idx_ = 0;
    is_inited_ = true;
    total_cnt_ = backup_set_list_.count() + backup_piece_list_.count();
  }
  return ret;
}

int ObTenantShowRestorePreview::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get next row", KR(ret), K(cur_row_));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

void ObTenantShowRestorePreview::reset()
{
  ObVirtualTableIterator::reset();
  backup_set_list_.reset();
  backup_piece_list_.reset();
}

int ObTenantShowRestorePreview::parse_tenant_id_from_session(uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ObObj tenant_id_value;
  ObString tenant_id_str;
  ObFixedLengthString<MAX_INT64_STR_LENGTH> fixed_string;
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (OB_FAIL(session_->get_user_variable_value(OB_RESTORE_PREVIEW_TENANT_ID_SESSION_STR, tenant_id_value))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(tenant_id_value.get_varchar(tenant_id_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(tenant_id_value));
  } else if (OB_FAIL(fixed_string.assign(tenant_id_str))) {
    SHARE_LOG(WARN, "failed to assign tenant id str", KR(ret), K(tenant_id_str));
  } else if (1 != sscanf(fixed_string.ptr(), "%lu", &tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to get uint64_t from value", KR(ret), K(tenant_id_value));
  } else {
    SHARE_LOG(INFO, "parse tenant id from session success", K(tenant_id));
  }
  return ret;
}

int ObTenantShowRestorePreview::parse_cluster_id_from_session(int64_t& cluster_id)
{
  int ret = OB_SUCCESS;
  cluster_id = -1;
  ObObj cluster_id_value;
  ObString cluster_id_str;
  ObFixedLengthString<MAX_INT64_STR_LENGTH> fixed_string;
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (OB_FAIL(session_->get_user_variable_value(
                 OB_RESTORE_PREVIEW_BACKUP_CLUSTER_ID_SESSION_STR, cluster_id_value))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(cluster_id_value.get_varchar(cluster_id_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(cluster_id_value));
  } else if (OB_FAIL(fixed_string.assign(cluster_id_str))) {
    SHARE_LOG(WARN, "failed to assign tenant id str", KR(ret), K(cluster_id_str));
  } else if (1 != sscanf(fixed_string.ptr(), "%ld", &cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to get int64_t from value", KR(ret), K(cluster_id_value));
  } else {
    SHARE_LOG(INFO, "parse cluster id from session success", K(cluster_id));
  }
  return ret;
}

int ObTenantShowRestorePreview::parse_restore_timestamp_from_session(int64_t& restore_timestamp)
{
  int ret = OB_SUCCESS;
  restore_timestamp = -1;
  ObObj restore_timestamp_value;
  ObString restore_timestamp_str;
  ObFixedLengthString<MAX_INT64_STR_LENGTH> fixed_string;
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (OB_FAIL(session_->get_user_variable_value(
                 OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR, restore_timestamp_value))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(restore_timestamp_value.get_varchar(restore_timestamp_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(restore_timestamp_value));
  } else if (OB_FAIL(fixed_string.assign(restore_timestamp_str))) {
    SHARE_LOG(WARN, "failed to assign tenant id str", KR(ret), K(restore_timestamp_str));
  } else if (1 != sscanf(fixed_string.ptr(), "%ld", &restore_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to get uint64_t from value", KR(ret), K(restore_timestamp_value));
  } else {
    SHARE_LOG(INFO, "parse restore timestamp from session success", K(restore_timestamp));
  }
  return ret;
}

int ObTenantShowRestorePreview::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ == total_cnt_) {
    ret = OB_ITER_END;
    SHARE_LOG(INFO, "iterator end", KR(ret), K(idx_), K(total_cnt_));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case BACKUP_TYPE: {
          BackupType type;
          if (OB_FAIL(get_backup_type(type))) {
            SHARE_LOG(WARN, "failed to get backup id", KR(ret));
          } else {
            if (type == BACKUP_TYPE_PIECE) {
              cur_row_.cells_[i].set_varchar("BACKUP_PIECE");
            } else if (type == BACKUP_TYPE_SET) {
              cur_row_.cells_[i].set_varchar("BACKUP_SET");
            }
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case BACKUP_ID: {
          int64_t id = 0;
          if (OB_FAIL(get_backup_id(id))) {
            SHARE_LOG(WARN, "failed to get backup id", KR(ret));
          } else {
            cur_row_.cells_[i].set_int(id);
          }
          break;
        }
        case COPY_ID: {
          int64_t copy_id = 0;
          if (OB_FAIL(get_copy_id(copy_id))) {
            SHARE_LOG(WARN, "failed to get copy id", KR(ret));
          } else {
            cur_row_.cells_[i].set_int(copy_id);
          }
          break;
        }
        case PREVIEW_PATH: {
          common::ObString str;
          if (OB_FAIL(get_backup_path(str))) {
            SHARE_LOG(WARN, "failed to get backup path", KR(ret));
          } else {
            cur_row_.cells_[i].set_varchar(str);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case FILE_STATUS: {
          common::ObString str;
          if (OB_FAIL(get_file_status(str))) {
            SHARE_LOG(WARN, "failed to get backup path", KR(ret));
          } else {
            cur_row_.cells_[i].set_varchar(str);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++idx_;
    }
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_type(BackupType& type)
{
  int ret = OB_SUCCESS;
  type = BACKUP_TYPE_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    type = BACKUP_TYPE_SET;
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    type = BACKUP_TYPE_PIECE;
  } else {
    type = BACKUP_TYPE_MAX;
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_id(int64_t& backup_id)
{
  int ret = OB_SUCCESS;
  backup_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    backup_id = backup_set_list_.at(idx_).backup_set_id_;
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    backup_id = backup_piece_list_.at(idx_ - backup_set_list_.count()).backup_piece_id_;
  }
  return ret;
}

int ObTenantShowRestorePreview::get_copy_id(int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  copy_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    copy_id = backup_set_list_.at(idx_).copy_id_;
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    copy_id = backup_piece_list_.at(idx_ - backup_set_list_.count()).copy_id_;
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_path(common::ObString& str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    str = backup_set_list_.at(idx_).backup_dest_.str();
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    str = backup_piece_list_.at(idx_ - backup_set_list_.count()).backup_dest_.str();
  }
  return ret;
}

int ObTenantShowRestorePreview::get_file_status(common::ObString& str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    str = ObBackupFileStatus::get_str(backup_set_list_.at(idx_).file_status_);
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    str = ObBackupFileStatus::get_str(backup_piece_list_.at(idx_ - backup_set_list_.count()).file_status_);
  }
  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
