/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_tenant_show_restore_preview.h"
#include "lib/string/ob_fixed_length_string.h"
#include "sql/session/ob_sql_session_info.h"
#include "rootserver/restore/ob_restore_util.h"
#include "share/restore/ob_restore_uri_parser.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace observer
{

ObTenantShowRestorePreview::ObTenantShowRestorePreview()
  : ObVirtualTableIterator(),
    is_inited_(false),
    idx_(-1),
    total_cnt_(0),
    uri_(),
    restore_scn_(),
    only_contain_backup_set_(false),
    backup_set_list_(),
    backup_piece_list_(),
    log_path_list_(),
    allocator_()
{
}

ObTenantShowRestorePreview::~ObTenantShowRestorePreview()
{
}

void ObTenantShowRestorePreview::reset()
{
  ObVirtualTableIterator::reset();
  backup_set_list_.reset();
  backup_piece_list_.reset();
  log_path_list_.reset();
  uri_.reset();
  idx_ = -1;
  total_cnt_ = 0;
}

int ObTenantShowRestorePreview::init()
{
  int ret = OB_SUCCESS;
  ObObj backup_dest_value;
  ObObj passwd;
  ObArenaAllocator allocator;
  ObArray<ObString> tenant_path_array;
  ObString backup_passwd(0, "");
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (!session_->user_variable_exists(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR)
              || !(session_->user_variable_exists(OB_RESTORE_PREVIEW_SCN_SESSION_STR)
                  || session_->user_variable_exists(OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR)) ) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "no ALTER SYSTEM RESTORE PREVIEW statement executed before", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "show restore preview before ALTER SYSTEM RESTORE PREVIEW is");
  } else if (OB_FAIL(session_->get_user_variable_value(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR, backup_dest_value))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(backup_dest_value.get_varchar(uri_))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(backup_dest_value));
  } else if (OB_FAIL(ObPhysicalRestoreUriParser::parse(uri_, allocator, tenant_path_array))) {
    SHARE_LOG(WARN, "fail to parse uri", K(ret));
  } else if (OB_FAIL(rootserver::ObRestoreUtil::check_restore_using_complement_log(tenant_path_array, only_contain_backup_set_))) {
    SHARE_LOG(WARN, "check restore using complement log failed", K(ret), K(tenant_path_array));
  } else if (!session_->user_variable_exists(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR)) {
  } else if (OB_FAIL(session_->get_user_variable_value(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR, passwd))) {
    SHARE_LOG(WARN, "failed to get user passwd", K(ret));
  } else if (OB_FAIL(passwd.get_varchar(backup_passwd))) {
    SHARE_LOG(WARN, "failed to parser passwd", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_restore_scn_from_session_(backup_passwd, tenant_path_array))) {
    SHARE_LOG(WARN, "failed to parse restore timestamp from session", KR(ret));
  } else if (OB_FAIL(rootserver::ObRestoreUtil::get_restore_source(only_contain_backup_set_,
      tenant_path_array, backup_passwd, restore_scn_, backup_set_list_, backup_piece_list_, log_path_list_))) {
    SHARE_LOG(WARN, "failed to get restore source", K(ret), K(tenant_path_array), K(restore_scn_), K(backup_passwd));
  } else {
    idx_ = 0;
    total_cnt_ = backup_set_list_.count() + backup_piece_list_.count();
    is_inited_ = true;
    SHARE_LOG(INFO, "succeed to parse restore preview", K_(restore_scn));
  }
  return ret;
}

int ObTenantShowRestorePreview::parse_restore_scn_from_session_(
    const ObString &backup_passwd, ObIArray<ObString> &tenant_path_array)
{
  int ret = OB_SUCCESS;
  uint64_t restore_scn = 0;
  ObObj restore_scn_obj;
  ObObj restore_timestamp_obj;
  ObString restore_scn_str;
  ObString restore_timestamp_str;
  const share::SCN  src_scn = share::SCN::min_scn();
  ObFixedLengthString<MAX_INT64_STR_LENGTH> fixed_string;
  if (OB_ISNULL(session_)) {
    ret = OB_BAD_NULL_ERROR;
    SHARE_LOG(WARN, "session should not be null", KR(ret));
  } else if (OB_FAIL(session_->get_user_variable_value(
      OB_RESTORE_PREVIEW_SCN_SESSION_STR, restore_scn_obj))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(session_->get_user_variable_value(
      OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR, restore_timestamp_obj))) {
    SHARE_LOG(WARN, "failed to get user variable value", KR(ret));
  } else if (OB_FAIL(restore_scn_obj.get_varchar(restore_scn_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(restore_scn_obj));
  } else if (OB_FAIL(restore_timestamp_obj.get_varchar(restore_timestamp_str))) {
    SHARE_LOG(WARN, "failed to varchar", KR(ret), K(restore_scn_obj));
  } else if (OB_FAIL(fixed_string.assign(restore_scn_str))) {
    SHARE_LOG(WARN, "failed to assign tenant id str", KR(ret), K(restore_scn_str));
  } else if (1 != sscanf(fixed_string.ptr(), "%lu", &restore_scn)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to get uint64_t from value", KR(ret), K(fixed_string));
  } else if (restore_scn != 0) {
    if (OB_FAIL(restore_scn_.convert_for_inner_table_field(restore_scn))) {
      SHARE_LOG(WARN, "failed to convert for inner table field", K(ret), K(restore_scn));
    }
  } else if (OB_FAIL(rootserver::ObRestoreUtil::fill_restore_scn(
      src_scn, restore_timestamp_str, false/*with_restore_scn*/, tenant_path_array, backup_passwd, only_contain_backup_set_, restore_scn_))) {
    SHARE_LOG(WARN, "failed to parse restore scn", K(ret));
  }
  return ret;
}

int ObTenantShowRestorePreview::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_sys_tenant(effective_tenant_id_)) {
    ret = OB_OP_NOT_ALLOW;
    SHARE_LOG(WARN, "show restore preview is sys only", K(ret), K(effective_tenant_id_));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "SHOW RESTORE PREVIEW in user tenant");
  } else if (OB_FAIL(inner_get_next_row_())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get next row", KR(ret), K(cur_row_));
    } else {
      if (session_->user_variable_exists(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR)
          && OB_TMP_FAIL(session_->remove_user_variable(OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR))) {
        ret = tmp_ret;
        SHARE_LOG(WARN, "fail to remove session variable OB_RESTORE_PREVIEW_BACKUP_DEST_SESSION_STR", K(tmp_ret), KPC(session_));
      }

      if (session_->user_variable_exists(OB_RESTORE_PREVIEW_SCN_SESSION_STR)
                && OB_TMP_FAIL(session_->remove_user_variable(OB_RESTORE_PREVIEW_SCN_SESSION_STR))) {
        ret = tmp_ret;
        SHARE_LOG(WARN, "fail to remove session variable OB_RESTORE_PREVIEW_SCN_SESSION_STR", K(tmp_ret), KPC(session_));
      }

      if (session_->user_variable_exists(OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR)
                && OB_TMP_FAIL(session_->remove_user_variable(OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR))) {
        ret = tmp_ret;
        SHARE_LOG(WARN, "fail to remove session variable OB_RESTORE_PREVIEW_TIMESTAMP_SESSION_STR", K(tmp_ret), KPC(session_));
      }

      if (session_->user_variable_exists(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR)
                && OB_TMP_FAIL(session_->remove_user_variable(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR))) {
        ret = tmp_ret;
        SHARE_LOG(WARN, "fail to remove session variable OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR", K(tmp_ret), KPC(session_));
      }
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObTenantShowRestorePreview::inner_get_next_row_()
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
          if (OB_FAIL(get_backup_type_(type))) {
            SHARE_LOG(WARN, "failed to get backup id", KR(ret));
          } else {
            if (type == BACKUP_TYPE_PIECE) {
              cur_row_.cells_[i].set_varchar("BACKUP_PIECE");
            } else if (type == BACKUP_TYPE_SET) {
              cur_row_.cells_[i].set_varchar("BACKUP_SET");
            }
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
          }
          break;
        }
        case BACKUP_ID: {
          int64_t id = 0;
          if (OB_FAIL(get_backup_id_(id))) {
            SHARE_LOG(WARN, "failed to get backup id", KR(ret));
          } else {
            cur_row_.cells_[i].set_int(id);
          }
          break;
        }
        case PREVIEW_PATH: {
          common::ObString str;
          if (OB_FAIL(get_backup_path_(str))) {
            SHARE_LOG(WARN, "failed to get backup path", KR(ret));
          } else {
            cur_row_.cells_[i].set_varchar(str);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
          }
          break;
        }
        case BACKUP_DESC: {
          common::ObString str;
          if (OB_FAIL(get_backup_desc_(str))) {
            SHARE_LOG(WARN, "failed to get backup desc", KR(ret));
          } else {
            cur_row_.cells_[i].set_varchar(str);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                      ObCharset::get_default_charset()));
          }
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      SHARE_LOG(WARN, "curr idx in show restore preview", K(idx_), K(cur_row_));
      ++idx_;
    }
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_id_(int64_t &backup_id)
{
  int ret = OB_SUCCESS;
  backup_id = -1;
  if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    backup_id = backup_set_list_.at(idx_).backup_set_desc_.backup_set_id_;
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    backup_id = backup_piece_list_.at(idx_ - backup_set_list_.count()).piece_id_;
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_type_(BackupType &type)
{
  int ret = OB_SUCCESS;
  type = BACKUP_TYPE_MAX;
  if (idx_ < 0) {
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

int ObTenantShowRestorePreview::get_backup_path_(common::ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    str = backup_set_list_.at(idx_).backup_set_path_.str();
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    str = backup_piece_list_.at(idx_ - backup_set_list_.count()).piece_path_.str();
  }
  return ret;
}

int ObTenantShowRestorePreview::get_backup_desc_(common::ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "idx should be less than 0", KR(ret), K(idx_));
  } else if (idx_ <= backup_set_list_.count() - 1) {
    if (OB_FAIL(backup_set_list_.at(idx_).get_restore_backup_set_brief_info_str(allocator_, str))) {
      SHARE_LOG(WARN, "failed to get restore backup set brief info str", K(ret));
    }
  } else if (idx_ >= backup_set_list_.count() && idx_ <= total_cnt_ - 1) {
    if (OB_FAIL(backup_piece_list_.at(idx_ - backup_set_list_.count()).get_restore_log_piece_brief_info_str(allocator_, str))) {
      SHARE_LOG(WARN, "failed to get restore backup set brief info str", K(ret));
    }
  }
  return ret;
}


} // end namespace observer
} // end namespace oceanbase