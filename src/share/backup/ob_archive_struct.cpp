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
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/hash_func/murmur_hash.h"

using namespace oceanbase;
using namespace share;

/**
 * ------------------------------ObArchiveRoundState---------------------
 */
const char *OB_ARCHIVE_ROUND_STATE_STR[] = {"INVALID", "PREPARE", "BEGINNING", "DOING", "INTERRUPTED", "STOPPING", "STOP", "SUSPENDING", "SUSPEND", "MAX_STATUS"};

bool ObArchiveRoundState::is_valid() const
{
  return Status::INVALID < status_ && Status::MAX_STATUS > status_;
}

const char *ObArchiveRoundState::to_status_str() const
{
  const char *str = OB_ARCHIVE_ROUND_STATE_STR[0];
  if (status_ >= Status::INVALID && status_ <= Status::MAX_STATUS) {
    str = OB_ARCHIVE_ROUND_STATE_STR[static_cast<int64_t>(status_)];
  }
  return str;
}

int ObArchiveRoundState::set_status(const char *status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else {
    int64_t i = 0;
    for (; i < static_cast<int64_t>(Status::MAX_STATUS); i++) {
      if (0 == strcmp(OB_ARCHIVE_ROUND_STATE_STR[i], status)) {
        status_ = static_cast<ObArchiveRoundState::Status>(i);
        break;
      }
    }

    if (i == static_cast<int64_t>(Status::MAX_STATUS)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid status", K(ret), K(status));
    }
  }

  return ret;
}



/**
 * ------------------------------ObTenantArchiveRoundAttr::Key---------------------
 */
bool ObTenantArchiveRoundAttr::Key::is_pkey_valid() const
{
  return is_user_tenant(tenant_id_) && dest_no_ >= 0;
}

int ObTenantArchiveRoundAttr::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_NO, dest_no_))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}


/**
 * ------------------------------ObTenantArchiveRoundAttr---------------------
 */
// Return if both primary key and value are valid.
bool ObTenantArchiveRoundAttr::is_valid() const
{
  return key_.is_pkey_valid() && state_.is_valid() && compatible_.is_valid()
         && OB_START_INCARNATION <= incarnation_
         && 0 < dest_id_
         && 0 < round_id_
         && start_scn_.is_valid()
         && checkpoint_scn_.is_valid()
         && max_scn_.is_valid()
         && 0 <= base_piece_id_
         && base_piece_id_ <= used_piece_id_
         && 0 <= piece_switch_interval_
         && 0 <= frozen_input_bytes_
         && 0 <= frozen_output_bytes_
         && 0 <= active_input_bytes_
         && 0 <= active_output_bytes_
         && 0 <= deleted_input_bytes_
         && 0 <= deleted_output_bytes_;
}

// Parse row from the sql result, the result has full columns.
int ObTenantArchiveRoundAttr::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t compatible = 0;
  int64_t real_length = 0;
  uint64_t start_scn = 0;
  uint64_t checkpoint_scn = 0;
  uint64_t max_scn = 0;
  ObString comment;
  ObString path;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, key_.dest_no_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, dest_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_SCN, start_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_SCN, checkpoint_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MAX_SCN, max_scn, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, compatible, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BASE_PIECE_ID, base_piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_USED_PIECE_ID, used_piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_SWITCH_INTERVAL, piece_switch_interval_, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FROZEN_INPUT_BYTES, frozen_input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FROZEN_OUTPUT_BYTES, frozen_output_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ACTIVE_INPUT_BYTES, active_input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ACTIVE_OUTPUT_BYTES, active_output_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETED_INPUT_BYTES, deleted_input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETED_OUTPUT_BYTES, deleted_output_bytes_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_PATH, path);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_COMMENT, comment);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("failed to set path", K(ret), K(path));
  } else if (OB_FAIL(set_compatible_version(compatible))) {
    LOG_WARN("failed to set compatible", K(ret), K(compatible));
  } else if (OB_FAIL(set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to set status", K(ret), K(comment));
  } else if (OB_FAIL(start_scn_.convert_for_inner_table_field(start_scn))) {
    LOG_WARN("failed to set start scn", K(ret), K(start_scn));
  } else if (OB_FAIL(checkpoint_scn_.convert_for_inner_table_field(checkpoint_scn))) {
    LOG_WARN("failed to set checkpoint scn", K(ret), K(checkpoint_scn));
  } else if (OB_FAIL(max_scn_.convert_for_inner_table_field(max_scn))) {
    LOG_WARN("failed to set max scn", K(ret), K(max_scn));
  }

  return ret;
}

// Fill primary key and value to dml.
int ObTenantArchiveRoundAttr::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ROUND_ID, round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INCARNATION, incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_ID, dest_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, state_.to_status_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_SCN, start_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_CHECKPOINT_SCN, checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_MAX_SCN, max_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMPATIBLE, compatible_.version_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_BASE_PIECE_ID, base_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_USED_PIECE_ID, used_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PIECE_SWITCH_INTERVAL, piece_switch_interval_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FROZEN_INPUT_BYTES, frozen_input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FROZEN_OUTPUT_BYTES, frozen_output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ACTIVE_INPUT_BYTES, active_input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ACTIVE_OUTPUT_BYTES, active_output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DELETED_INPUT_BYTES, deleted_input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DELETED_OUTPUT_BYTES, deleted_output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, path_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

void ObTenantArchiveRoundAttr::set_stop()
{
  frozen_input_bytes_ += active_input_bytes_;
  frozen_output_bytes_ += active_output_bytes_;
  active_input_bytes_ = 0;
  active_output_bytes_ = 0;
  state_.set_stop();
}

int ObTenantArchiveRoundAttr::deep_copy_from(const ObTenantArchiveRoundAttr &other)
{
  int ret = OB_SUCCESS;

  key_ = other.key_;
  incarnation_ = other.incarnation_;
  dest_id_ = other.dest_id_;
  round_id_ = other.round_id_;
  state_ = other.state_;
  start_scn_ = other.start_scn_;
  checkpoint_scn_ = other.checkpoint_scn_;
  max_scn_ = other.max_scn_;
  compatible_ = other.compatible_;

  base_piece_id_ = other.base_piece_id_;
  used_piece_id_ = other.used_piece_id_;
  piece_switch_interval_ = other.piece_switch_interval_;

  frozen_input_bytes_ = other.frozen_input_bytes_;
  frozen_output_bytes_ = other.frozen_output_bytes_;
  active_input_bytes_ = other.active_input_bytes_;
  active_output_bytes_ = other.active_output_bytes_;
  deleted_input_bytes_ = other.deleted_input_bytes_;
  deleted_output_bytes_ = other.deleted_output_bytes_;
  if (OB_FAIL(set_path(other.path_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(other));
  } else if (OB_FAIL(set_comment(other.comment_.ptr()))) {
    LOG_WARN("failed to set comment", K(ret), K(other));
  }

  return ret;
}

int ObTenantArchiveRoundAttr::generate_next_round(const int64_t incarnation,
    const int64_t dest_id, const int64_t piece_switch_interval, const ObBackupPathString &path,
    ObTenantArchiveRoundAttr &next_round) const
{
  int ret = OB_SUCCESS;
  next_round.key_ = key_;
  next_round.incarnation_ = incarnation;
  next_round.dest_id_ = dest_id;
  next_round.round_id_ = round_id_ + 1;
  next_round.state_.set_prepare();

  next_round.base_piece_id_ = used_piece_id_ + 1;
  next_round.used_piece_id_ = used_piece_id_ + 1;
  next_round.piece_switch_interval_ = piece_switch_interval;
  next_round.path_ = path;

  next_round.frozen_input_bytes_ = 0;
  next_round.frozen_input_bytes_ = 0;
  next_round.active_input_bytes_ = 0;
  next_round.active_output_bytes_ = 0;

  return ret;
}

int ObTenantArchiveRoundAttr::generate_first_piece(ObTenantArchivePieceAttr &first_piece) const
{
  int ret = OB_SUCCESS;
  if (!state_.is_beginning()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round state is not BEGINNING", K(ret), K(*this));
  } else if (OB_FAIL(first_piece.set_path(path_))) {
    LOG_WARN("failed to set path", K(ret), K(*this));
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_end_scn(start_scn_, base_piece_id_, piece_switch_interval_, base_piece_id_, first_piece.end_scn_))) {
    LOG_WARN("failed to get end scn", K(ret), K(*this));
  } else {
    first_piece.key_.tenant_id_ = key_.tenant_id_;
    first_piece.key_.dest_id_ = dest_id_;
    first_piece.key_.round_id_ = round_id_;
    first_piece.key_.piece_id_ = base_piece_id_;
    first_piece.incarnation_ = incarnation_;
    first_piece.dest_no_ = key_.dest_no_;
    first_piece.file_count_ = 0;
    first_piece.start_scn_ = start_scn_;
    first_piece.checkpoint_scn_ = start_scn_;
    first_piece.max_scn_ = start_scn_;
    first_piece.compatible_ = compatible_;
    first_piece.input_bytes_ = 0;
    first_piece.output_bytes_ = 0;
    first_piece.set_active();
    first_piece.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_INCOMPLETE;
    first_piece.cp_file_id_ = 0;
    first_piece.cp_file_offset_ = 0;
  }
  return ret;
}

ObTenantArchiveHisRoundAttr ObTenantArchiveRoundAttr::generate_his_round() const
{
  ObTenantArchiveHisRoundAttr his_round;
  his_round.key_.tenant_id_ = key_.tenant_id_;
  his_round.key_.dest_no_ = key_.dest_no_;
  his_round.key_.round_id_ = round_id_;
  his_round.incarnation_ = incarnation_;
  his_round.dest_id_ = dest_id_;
  his_round.start_scn_ = start_scn_;
  his_round.checkpoint_scn_ = checkpoint_scn_;
  his_round.max_scn_ = max_scn_;
  his_round.compatible_ = compatible_;
  his_round.base_piece_id_ = base_piece_id_;
  his_round.used_piece_id_ = used_piece_id_;
  his_round.piece_switch_interval_ = piece_switch_interval_;
  his_round.input_bytes_ = frozen_input_bytes_ + active_input_bytes_;
  his_round.output_bytes_ = frozen_output_bytes_ + active_output_bytes_;
  his_round.deleted_input_bytes_ = deleted_input_bytes_;
  his_round.deleted_output_bytes_ = deleted_output_bytes_;
  his_round.path_ = path_;
  his_round.comment_ = comment_;
  return his_round;
}

int ObTenantArchiveRoundAttr::generate_initial_round(const ObTenantArchiveRoundAttr::Key &key,
    const int64_t incarnation, const int64_t dest_id, const int64_t piece_switch_interval,
    const ObBackupPathString &path, ObTenantArchiveRoundAttr &initial_round)
{
  int ret = OB_SUCCESS;
  initial_round.key_ = key;
  initial_round.incarnation_ = incarnation;
  initial_round.dest_id_ = dest_id;
  initial_round.round_id_ = 1;
  initial_round.state_.set_prepare();
  initial_round.compatible_.version_ = ObArchiveCompatible::get_current_compatible_version();
  initial_round.base_piece_id_ = 1;
  initial_round.used_piece_id_ = 1;
  initial_round.piece_switch_interval_ = piece_switch_interval;
  initial_round.path_ = path;

  return ret;
}


/**
 * ------------------------------ObTenantArchiveHisRoundAttr::Key---------------------
 */
bool ObTenantArchiveHisRoundAttr::Key::is_pkey_valid() const
{
  return is_user_tenant(tenant_id_) && dest_no_ >= 0 && round_id_ > 0;
}

int ObTenantArchiveHisRoundAttr::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_NO, dest_no_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_ROUND_ID, round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}


/**
 * ------------------------------ObTenantArchiveHisRoundAttr---------------------
 */
// Return if both primary key and value are valid.
bool ObTenantArchiveHisRoundAttr::is_valid() const
{
  return key_.is_pkey_valid() && compatible_.is_valid()
         && OB_START_INCARNATION <= incarnation_
         && 0 < dest_id_
         && start_scn_.is_valid()
         && checkpoint_scn_.is_valid()
         && max_scn_.is_valid()
         && 0 <= base_piece_id_
         && base_piece_id_ <= used_piece_id_
         && 0 <= piece_switch_interval_
         && 0 <= input_bytes_
         && 0 <= output_bytes_
         && 0 <= deleted_input_bytes_
         && 0 <= deleted_output_bytes_;
}

// Parse row from the sql result, the result has full columns.
int ObTenantArchiveHisRoundAttr::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t compatible = 0;
  int64_t real_length = 0;
  uint64_t start_scn = 0;
  uint64_t checkpoint_scn = 0;
  uint64_t max_scn = 0;
  ObString path;
  ObString comment;

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, key_.dest_no_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, key_.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, dest_id_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_SCN, start_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_SCN, checkpoint_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MAX_SCN, max_scn, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, compatible, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BASE_PIECE_ID, base_piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_USED_PIECE_ID, used_piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_SWITCH_INTERVAL, piece_switch_interval_, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INPUT_BYTES, input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_OUTPUT_BYTES, output_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETED_INPUT_BYTES, deleted_input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DELETED_OUTPUT_BYTES, deleted_output_bytes_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_PATH, path);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_COMMENT, comment);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("failed to set path", K(ret), K(path));
  } else if (OB_FAIL(set_compatible_version(compatible))) {
    LOG_WARN("failed to set compatible", K(ret), K(compatible));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to set status", K(ret), K(comment));
  } else if (OB_FAIL(start_scn_.convert_for_inner_table_field(start_scn))) {
    LOG_WARN("failed to set start scn", K(ret), K(start_scn));
  } else if (OB_FAIL(checkpoint_scn_.convert_for_inner_table_field(checkpoint_scn))) {
    LOG_WARN("failed to set checkpoint scn", K(ret), K(checkpoint_scn));
  } else if (OB_FAIL(max_scn_.convert_for_inner_table_field(max_scn))) {
    LOG_WARN("failed to set max scn", K(ret), K(max_scn));
  }

  return ret;
}

// Fill primary key and value to dml.
int ObTenantArchiveHisRoundAttr::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key dml", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INCARNATION, incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_ID, dest_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_SCN, start_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_CHECKPOINT_SCN, checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_MAX_SCN, max_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMPATIBLE, compatible_.version_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_BASE_PIECE_ID, base_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_USED_PIECE_ID, used_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PIECE_SWITCH_INTERVAL, piece_switch_interval_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INPUT_BYTES, input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_OUTPUT_BYTES, output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DELETED_INPUT_BYTES, deleted_input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DELETED_OUTPUT_BYTES, deleted_output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, path_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

ObTenantArchiveRoundAttr ObTenantArchiveHisRoundAttr::generate_round() const
{
  ObTenantArchiveRoundAttr round;
  round.key_.tenant_id_ = key_.tenant_id_;
  round.key_.dest_no_ = key_.dest_no_;
  round.round_id_ = key_.round_id_;
  round.incarnation_ = incarnation_;
  round.state_.set_stop();
  round.dest_id_ = dest_id_;
  round.start_scn_ = start_scn_;
  round.checkpoint_scn_ = checkpoint_scn_;
  round.max_scn_ = max_scn_;
  round.compatible_ = compatible_;
  round.base_piece_id_ = base_piece_id_;
  round.used_piece_id_ = used_piece_id_;
  round.piece_switch_interval_ = piece_switch_interval_;
  round.active_input_bytes_ = 0;
  round.active_output_bytes_ = 0;
  round.frozen_input_bytes_ = input_bytes_;
  round.frozen_output_bytes_ = output_bytes_;
  round.deleted_input_bytes_ = deleted_input_bytes_;
  round.deleted_output_bytes_ = deleted_output_bytes_;
  round.path_ = path_;
  round.comment_ = comment_;
  return round;
}

/**
 * ------------------------------ObArchivePieceStatus---------------------
 */
OB_SERIALIZE_MEMBER(ObArchivePieceStatus, status_);

const char *OB_ARCHIVE_PIECE_STATUS_STR[] = {"INACTIVE", "ACTIVE", "FROZEN", "MAX_STATUS"};
bool ObArchivePieceStatus::is_valid() const
{
  return Status::INACTIVE < status_ && Status::MAX_STATUS > status_;
}

const char *ObArchivePieceStatus::to_status_str() const
{
  return OB_ARCHIVE_PIECE_STATUS_STR[static_cast<int64_t>(status_)];
}

int ObArchivePieceStatus::set_status(const char *status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else {
    int64_t i = 0;
    for (; i < static_cast<int64_t>(Status::MAX_STATUS); i++) {
      if (0 == strcmp(OB_ARCHIVE_PIECE_STATUS_STR[i], status)) {
        status_ = static_cast<ObArchivePieceStatus::Status>(i);
        break;
      }
    }

    if (i == static_cast<int64_t>(Status::MAX_STATUS)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid status", K(ret), K(status));
    }
  }

  return ret;
}


/**
 * ------------------------------ObTenantArchivePieceAttr::Key---------------------
 */
OB_SERIALIZE_MEMBER(ObTenantArchivePieceAttr::Key, tenant_id_, dest_id_, round_id_, piece_id_);

bool ObTenantArchivePieceAttr::Key::is_pkey_valid() const
{
  return is_user_tenant(tenant_id_) && dest_id_ > 0 && round_id_ > 0 && piece_id_ > 0;
}

int ObTenantArchivePieceAttr::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_ID, dest_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_ROUND_ID, round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_PIECE_ID, piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}



/**
 * ------------------------------ObTenantArchivePieceAttr---------------------
 */
OB_SERIALIZE_MEMBER(ObTenantArchivePieceAttr, key_, incarnation_, dest_no_,
  file_count_, start_scn_, checkpoint_scn_,
  max_scn_, end_scn_, compatible_, input_bytes_, output_bytes_, status_,
  file_status_, cp_file_id_, cp_file_offset_, path_);

// Return if both primary key and value are valid.
bool ObTenantArchivePieceAttr::is_valid() const
{
  return key_.is_pkey_valid() && compatible_.is_valid()
         && OB_START_INCARNATION <= incarnation_
         && 0 <= dest_no_
         && 0 <= file_count_
         && start_scn_.is_valid()
         && checkpoint_scn_.is_valid()
         && max_scn_.is_valid()
         && end_scn_.is_valid()
         && 0 <= input_bytes_
         && 0 <= output_bytes_
         && status_.is_valid()
         && ObBackupFileStatus::is_valid(file_status_)
         && 0 <= cp_file_id_
         && 0 <= cp_file_offset_
         && !path_.is_empty();
}

// Parse row from the sql result, the result has full columns.
int ObTenantArchivePieceAttr::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t compatible = 0;
  int64_t real_length = 0;
  uint64_t start_scn = 0;
  uint64_t checkpoint_scn = 0;
  uint64_t max_scn = 0;
  uint64_t end_scn = 0;
  char file_status_str[OB_DEFAULT_STATUS_LENTH] = "";
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";
  ObString path;

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, key_.dest_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, key_.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_ID, key_.piece_id_, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, dest_no_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FILE_COUNT, file_count_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_SCN, start_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_SCN, checkpoint_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MAX_SCN, max_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_END_SCN, end_scn, uint64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, compatible, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INPUT_BYTES, input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_OUTPUT_BYTES, output_bytes_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_FILE_STATUS, file_status_str, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_CP_FILE_ID, cp_file_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_CP_FILE_OFFSET, cp_file_offset_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, OB_STR_PATH, path);

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(file_status_ = ObBackupFileStatus::get_status(file_status_str))) {
  } else if (!ObBackupFileStatus::is_valid(file_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file status", K(ret), K(file_status_str));
  } else if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("failed to assign path", K(ret), K(path));
  } else if (OB_FAIL(set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (OB_FAIL(set_compatible_version(compatible))) {
    LOG_WARN("failed to set compatible", K(ret), K(compatible));
  } else if (OB_FAIL(start_scn_.convert_for_inner_table_field(start_scn))) {
    LOG_WARN("failed to set start scn", K(ret), K(start_scn));
  } else if (OB_FAIL(checkpoint_scn_.convert_for_inner_table_field(checkpoint_scn))) {
    LOG_WARN("failed to set checkpoint scn", K(ret), K(checkpoint_scn));
  } else if (OB_FAIL(max_scn_.convert_for_inner_table_field(max_scn))) {
    LOG_WARN("failed to set max scn", K(ret), K(max_scn));
  } else if (OB_FAIL(end_scn_.convert_for_inner_table_field(end_scn))) {
    LOG_WARN("failed to set end scn", K(ret), K(end_scn));
  }

  return ret;
}

// Fill primary key and value to dml.
int ObTenantArchivePieceAttr::fill_dml(ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INCARNATION, incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_DEST_NO, dest_no_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FILE_COUNT, file_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_SCN, start_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_CHECKPOINT_SCN, checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_MAX_SCN, max_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_END_SCN, end_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMPATIBLE, compatible_.version_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INPUT_BYTES, input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_OUTPUT_BYTES, output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status_.to_status_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FILE_STATUS, ObBackupFileStatus::get_str(file_status_)))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_CP_FILE_ID, cp_file_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_CP_FILE_OFFSET, cp_file_offset_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_PATH, path_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObTenantArchivePieceAttr::assign(const ObTenantArchivePieceAttr &other)
{
  int ret = OB_SUCCESS;
  key_ = other.key_;
  incarnation_ = other.incarnation_;
  dest_no_ = other.dest_no_;
  file_count_ = other.file_count_;
  start_scn_ = other.start_scn_;
  checkpoint_scn_ = other.checkpoint_scn_;
  max_scn_ = other.max_scn_;
  end_scn_ = other.end_scn_;
  compatible_ = other.compatible_;

  input_bytes_ = other.input_bytes_;
  output_bytes_ = other.output_bytes_;

  status_ = other.status_;
  file_status_ = other.file_status_;

  cp_file_id_ = other.cp_file_id_;
  cp_file_offset_ = other.cp_file_offset_;
  path_ = other.path_;
  return ret;
}



/**
 * ------------------------------ObLSArchivePersistInfo::Key---------------------
 */
bool ObLSArchivePersistInfo::Key::is_pkey_valid() const
{
  return is_user_tenant(tenant_id_) && dest_id_ > 0 && round_id_ > 0 && piece_id_ > 0 && ls_id_.is_valid();
}

int ObLSArchivePersistInfo::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_ID, dest_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_ROUND_ID, round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_PIECE_ID, piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id_.id()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

void ObLSArchivePersistInfo::Key::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  ls_id_.reset();
}


/*
 *------------------------------ObLSArchivePersistInfo---------------------
 * */
bool ObLSArchivePersistInfo::is_valid() const
{
  return key_.is_pkey_valid()
    && state_.is_valid()
    && OB_START_INCARNATION <= incarnation_
    && start_scn_.is_valid()
    && checkpoint_scn_.is_valid()
    && 0 <= start_lsn_
    && 0 <= lsn_
    && 0 <= input_bytes_
    && 0 <= output_bytes_
    && ((state_.is_interrupted() && -1 <= archive_file_id_ && -1 <= archive_file_offset_)
        || (0 <= archive_file_id_ && 0 <= archive_file_offset_));
}

// Parse row from the sql result, the result has full columns.
int ObLSArchivePersistInfo::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t ls_id = 0;
  int64_t real_length = 0;
  uint64_t start_scn = 0;
  uint64_t checkpoint_scn = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, key_.dest_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, key_.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_ID, key_.piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_ID, ls_id, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, incarnation_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_SCN, start_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MIN_LSN, start_lsn_, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_SCN, checkpoint_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MAX_LSN, lsn_, uint64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ARCHIVE_FILE_ID, archive_file_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ARCHIVE_FILE_OFFSET, archive_file_offset_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INPUT_BYTES, input_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_OUTPUT_BYTES, output_bytes_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_status(status_str))) {
    LOG_WARN("failed to set status", K(ret), K(status_str));
  } else if (OB_FAIL(start_scn_.convert_for_inner_table_field(start_scn))) {
    LOG_WARN("failed to set start scn", K(ret), K(start_scn));
  } else if (OB_FAIL(checkpoint_scn_.convert_for_inner_table_field(checkpoint_scn))) {
    LOG_WARN("failed to set checkpoint scn", K(ret), K(checkpoint_scn));
  } else {
    key_.ls_id_ = ObLSID(ls_id);
  }

  return ret;
}

// Fill primary key and value to dml.
int ObLSArchivePersistInfo::fill_dml(ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INCARNATION, incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  }  else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_SCN, start_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_MIN_LSN, start_lsn_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_CHECKPOINT_SCN, checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_MAX_LSN, lsn_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ARCHIVE_FILE_ID, archive_file_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_ARCHIVE_FILE_OFFSET, archive_file_offset_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_INPUT_BYTES, input_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_OUTPUT_BYTES, output_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, state_.to_status_str()))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObLSArchivePersistInfo::set_stop(const uint64_t tenant_id,
    const int64_t incarnation,
    const int64_t round,
    const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || OB_START_INCARNATION > incarnation
        || OB_START_LOG_ARCHIVE_ROUND_ID > round
        || dest_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(incarnation), K(round), K(dest_id));
  } else {
    key_.tenant_id_ = tenant_id;
    incarnation_ = incarnation;
    key_.round_id_ = round;
    key_.dest_id_ = dest_id;
    state_.set_stop();
  }
  return ret;
}

void ObLSArchivePersistInfo::reset()
{
  key_.reset();
  incarnation_ = OB_START_INCARNATION;
  start_scn_ = SCN::min_scn();
  start_lsn_ = 0;
  checkpoint_scn_.SCN::min_scn();
  lsn_ = 0;
  archive_file_id_ = -1;
  archive_file_offset_ = -1;
  input_bytes_ = 0;
  output_bytes_ = 0;
  state_.set_invalid();
}

void ObLSArchivePersistInfo::refresh_state(const ObArchiveRoundState &state)
{
  if (state.is_stopping() || state.is_stop()) {
    state_.set_stop();
  }
}

int ObLSArchivePersistInfo::build_column_names(ObSqlString &column_names)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_names.assign_fmt("%s", OB_STR_TENANT_ID))) {
    LOG_WARN("failed to assign fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_DEST_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_ROUND_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_PIECE_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_LS_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_INCARNATION))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_START_SCN))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_MIN_LSN))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_CHECKPOINT_SCN))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_MAX_LSN))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_ARCHIVE_FILE_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_ARCHIVE_FILE_OFFSET))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_INPUT_BYTES))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_OUTPUT_BYTES))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(column_names.append_fmt(",%s", OB_STR_STATUS))) {
    LOG_WARN("failed to append fmt", K(ret));
  }
  return ret;
}

/**
 * ------------------------------ObArchiveLSPieceSummary---------------------
 */
ObArchiveLSPieceSummary::ObArchiveLSPieceSummary()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_archiving_ = false;
  is_deleted_ = false;

  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  incarnation_ = OB_START_INCARNATION;
  state_.set_invalid();

  start_scn_ = SCN::min_scn();
  checkpoint_scn_ = SCN::min_scn();
  min_lsn_ = 0;
  max_lsn_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
}

/**
 * ------------------------------ObLSDestRoundSummary---------------------
 */
ObLSDestRoundSummary::ObLSDestRoundSummary()
{
  reset();
}

bool ObLSDestRoundSummary::has_piece() const
{
  return !piece_list_.empty();
}

int64_t ObLSDestRoundSummary::get_piece_idx(const int64_t piece_id) const
{
  int64_t idx = -1;
  for (int64_t i = 0; i < piece_list_.count(); i++) {
    const OnePiece &piece = piece_list_.at(i);
    if (piece.piece_id_ == piece_id) {
      idx = i;
      break;
    }
  }

  return idx;
}

int64_t ObLSDestRoundSummary::min_piece_id() const
{
  int64_t min_piece_id = 0;
  const int64_t count = piece_list_.count();
  if (count > 0) {
    min_piece_id = piece_list_.at(0).piece_id_;
  }

  return min_piece_id;
}

int64_t ObLSDestRoundSummary::max_piece_id() const
{
  int64_t max_piece_id = 0;
  const int64_t count = piece_list_.count();
  if (count > 0) {
    max_piece_id = piece_list_.at(count - 1).piece_id_;
  }

  return max_piece_id;
}

int ObLSDestRoundSummary::add_one_piece(const ObArchiveLSPieceSummary &piece)
{
  int ret = OB_SUCCESS;
  if (!piece.is_archiving_) {
    tenant_id_ = piece.tenant_id_;
    ls_id_ = piece.ls_id_;
    is_deleted_ = piece.is_deleted_;
    round_id_ = piece.round_id_;
    state_.status_ = piece.state_.status_;
    start_scn_ = piece.start_scn_;
    checkpoint_scn_ = piece.checkpoint_scn_;
    if (has_piece()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls has piece", K(ret), K(piece_list_), K(piece));
    }
  } else {
    OnePiece one_piece;
    one_piece.piece_id_ = piece.piece_id_;
    one_piece.start_scn_ = piece.start_scn_;
    one_piece.checkpoint_scn_ = piece.checkpoint_scn_;
    one_piece.min_lsn_ = piece.min_lsn_;
    one_piece.max_lsn_ = piece.max_lsn_;
    one_piece.input_bytes_ = piece.input_bytes_;
    one_piece.output_bytes_ = piece.output_bytes_;

    if (!has_piece()) {
      tenant_id_ = piece.tenant_id_;
      dest_id_ = piece.dest_id_;
      round_id_ = piece.round_id_;
      ls_id_ = piece.ls_id_;
      is_deleted_ = piece.is_deleted_;
      state_.status_ = piece.state_.status_;
      start_scn_ = piece.start_scn_;
      checkpoint_scn_ = piece.checkpoint_scn_;
    } else {
      // piece id must be continous.
      const OnePiece &last_piece = piece_list_.at(piece_list_.count() - 1);
      if (tenant_id_ != piece.tenant_id_
        || dest_id_ != piece.dest_id_
        || round_id_ != piece.round_id_
        || ls_id_ != piece.ls_id_
        || last_piece.piece_id_ + 1 != piece.piece_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece are not continuous.", K(ret), K(piece_list_), K(piece));
      }
    }

    if(OB_FAIL(ret)) {
    } else if (OB_FAIL(piece_list_.push_back(one_piece))) {
      LOG_WARN("failed to push backup piece", K(ret));
    } else {
      checkpoint_scn_ = piece.checkpoint_scn_;
      // May be now round is at piece 1, the returned ls-1001 is DOING at piece 1, but 
      // INTERRUPTED at piece 2. So we need decide the log stream archive status by the 
      // latest piece.
      state_.status_ = piece.state_.status_;
    }
  }

  return ret;
}

void ObLSDestRoundSummary::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = 0;
  round_id_ = 0;
  ls_id_.reset();
  is_deleted_ = false;
  state_.set_invalid();
  start_scn_ = SCN::min_scn();
  checkpoint_scn_.reset();
  piece_list_.reset();
}

bool ObLSDestRoundSummary::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid();
}

int ObLSDestRoundSummary::check_is_last_piece_for_deleted_ls(const int64_t piece_id, bool &last_piece) const
{
  int ret = OB_SUCCESS;
  const int64_t biggest_piece_id = max_piece_id();
  if (!is_deleted_) {
    last_piece = false;
  } else if (piece_id > biggest_piece_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece id", K(ret), K(piece_id), KPC(this));
  } else {
    last_piece = (piece_id == biggest_piece_id);
  }

  return ret;
}

/**
 * ------------------------------ObDestRoundSummary---------------------
 */
int64_t ObDestRoundSummary::ls_count() const
{
  return ls_round_list_.count();
}

bool ObDestRoundSummary::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID
         && 0 <= dest_id_
         && 0 < round_id_;
}

int ObDestRoundSummary::add_ls_dest_round_summary(const ObLSDestRoundSummary &dest_round_summary)
{
  int ret = OB_SUCCESS;
  tenant_id_ = dest_round_summary.tenant_id_;
  dest_id_ = dest_round_summary.dest_id_;
  round_id_ = dest_round_summary.round_id_;
  if (OB_FAIL(ls_round_list_.push_back(dest_round_summary))) {
    LOG_WARN("failed to push back ls dest round summary", K(ret), K(dest_round_summary));
  }
  return ret;
}

/**
 * ------------------------------ObArchiveLSMetaType---------------------
 */
bool ObArchiveLSMetaType::is_valid() const
{
  return type_ > Type::INVALID_TYPE && type_ < Type::MAX_TYPE;
}

int ObArchiveLSMetaType::compare(const ObArchiveLSMetaType &other) const
{
  int ret = 0;
  if (type_ == other.type_) {
    ret = 0;
  } else if (type_ > other.type_) {
    ret = 1;
  } else {
    ret = -1;
  }
  return ret;
}

const char *ObArchiveLSMetaType::get_type_str() const
{
  const char *type_str = nullptr;
  const char *meta_type_strs[] = {
    "invalid",
    "schema_meta",
  };
  STATIC_ASSERT(Type::MAX_TYPE == ARRAYSIZEOF(meta_type_strs), "type count mismatch");
  if (type_ < Type::INVALID_TYPE || type_ >= Type::MAX_TYPE) {
    type_str = "unknow";
  } else {
    type_str = meta_type_strs[type_];
  }
  return type_str;
}

int ObArchiveLSMetaType::get_next_type()
{
  int ret = OB_SUCCESS;
  switch (type_) {
    case Type::INVALID_TYPE: {
      type_ = Type::SCHEMA_META;
      break;
    };
    case Type::SCHEMA_META: {
      ret = OB_ITER_END;
      break;
    };
    case Type::MAX_TYPE: {
      ret = OB_ITER_END;
      break;
    };
  }
  return ret;
}