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

#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/restore/ob_storage.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/utility.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_archive_checkpoint_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace share;

/**
 * ------------------------------ObRoundStartDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObRoundStartDesc, dest_id_, round_id_, start_scn_, base_piece_id_, piece_switch_interval_);

ObRoundStartDesc::ObRoundStartDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_ROUND_START_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  start_scn_ = SCN::min_scn();
  base_piece_id_ = 0;
  piece_switch_interval_ = 0;
}

bool ObRoundStartDesc::is_valid() const
{
  return 0 <= dest_id_ && 0 < round_id_ && start_scn_.is_valid() && 0 < base_piece_id_
         && 0 < piece_switch_interval_;
}


/**
 * ------------------------------ObRoundEndDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObRoundEndDesc, dest_id_, round_id_, start_scn_, checkpoint_scn_, base_piece_id_, piece_switch_interval_);

ObRoundEndDesc::ObRoundEndDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_ROUND_END_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  start_scn_ = SCN::min_scn();
  checkpoint_scn_ = SCN::min_scn();
  base_piece_id_ = 0;
  piece_switch_interval_ = 0;
}

bool ObRoundEndDesc::is_valid() const
{
  return 0 <= dest_id_ && 0 < round_id_ && start_scn_.is_valid() && start_scn_ <= checkpoint_scn_ && 0 < base_piece_id_
         && 0 < piece_switch_interval_;
}

int ObRoundEndDesc::assign(const ObRoundStartDesc &round_start)
{
  int ret = OB_SUCCESS;
  dest_id_ = round_start.dest_id_;
  round_id_ = round_start.round_id_;
  start_scn_ = round_start.start_scn_;
  base_piece_id_ = round_start.base_piece_id_;
  piece_switch_interval_ = round_start.piece_switch_interval_;
  checkpoint_scn_ = SCN::min_scn();
  return ret;
}

bool ObRoundEndDesc::operator < (const ObRoundEndDesc &other) const
{
  return round_id_ < other.round_id_;
}


/**
 * ------------------------------ObPieceStartDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObPieceStartDesc, dest_id_, round_id_, piece_id_, start_scn_);

ObPieceStartDesc::ObPieceStartDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_PIECE_START_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  start_scn_ = SCN::min_scn();
}

bool ObPieceStartDesc::is_valid() const
{
  return 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_ && start_scn_.is_valid();
}


/**
 * ------------------------------ObPieceEndDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObPieceEndDesc, dest_id_, round_id_, piece_id_, end_scn_);

ObPieceEndDesc::ObPieceEndDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_PIECE_END_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  end_scn_ = SCN::min_scn();
}

bool ObPieceEndDesc::is_valid() const
{
  return 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_ && end_scn_.is_valid();
}


/**
 * ------------------------------ObTenantArchivePieceInfosDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObTenantArchivePieceInfosDesc, tenant_id_, dest_id_, round_id_, piece_id_, incarnation_, dest_no_, compatible_, 
  start_scn_, end_scn_, path_, his_frozen_pieces_);

ObTenantArchivePieceInfosDesc::ObTenantArchivePieceInfosDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_TENANT_ARCHIVE_PIECE_INFOS, FILE_VERSION)
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  incarnation_ = 0;
  dest_no_ = -1;
  start_scn_ = SCN::min_scn();
  end_scn_ = SCN::min_scn();
}

bool ObTenantArchivePieceInfosDesc::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID && 0 < dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_ && 0 <= dest_no_ && start_scn_.is_valid() && end_scn_.is_valid();
}




/**
 * ------------------------------ObSinglePieceDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObSinglePieceDesc, piece_);

ObSinglePieceDesc::ObSinglePieceDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_SINGLE_PIECE_INFO, FILE_VERSION)
{
}

bool ObSinglePieceDesc::is_valid() const
{
  return piece_.is_valid();
}


/**
 * ------------------------------ObPieceCheckpointDesc---------------------
 */
ObPieceCheckpointDesc::ObPieceCheckpointDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_ARCHIVE_SINGLE_PIECE_INFO, FILE_VERSION)
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  incarnation_ = 0;
  start_scn_ = SCN::min_scn();
  checkpoint_scn_ = SCN::min_scn();
  max_scn_ = SCN::min_scn();
  end_scn_ = SCN::min_scn();
  MEMSET(reserved_, 0, sizeof(reserved_));
}

int ObPieceCheckpointDesc::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  // custom serialization, the format is as following:
  // | VER | LEN | Member1 | Member2 | ... |
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size();
  if (buf_len - pos < len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf length not enough", K(ret), K(buf_len), K(pos), K(len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, UNIS_VERSION))) {
    LOG_WARN("failed to encode version", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, len))) {
    LOG_WARN("failed to encode len", K(ret));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize_", K(ret));
  } 

  return ret;
}

int ObPieceCheckpointDesc::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t ver = 0;
  int64_t len = 0;
  int64_t tmp_pos = pos;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &ver))) {
    LOG_WARN("failed to decode version", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &len))) {
    LOG_WARN("failed to decode len", K(ret));
  } else if (ver != UNIS_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object version mismatch", K(ret), K(ver));
  } else if (data_len < len + pos) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("buf length not enough", K(ret), K(len), K(pos), K(data_len));
  } else if (OB_FALSE_IT(pos = tmp_pos)) {
  } else if (OB_FAIL(deserialize_(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize_", K(ret));
  } 

  return ret;
}

int64_t ObPieceCheckpointDesc::get_serialize_size() const
{
  return sizeof(int64_t)/* VER */ + sizeof(int64_t)/* LEN */ + get_serialize_size_();
}

int ObPieceCheckpointDesc::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t compatible = static_cast<int64_t>(compatible_.version_);
  int64_t tenant_id = static_cast<int64_t>(tenant_id_);
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, tenant_id))) {
    LOG_WARN("failed to encode tenant_id", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, dest_id_))) {
    LOG_WARN("failed to encode dest_id", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, round_id_))) {
    LOG_WARN("failed to encode round_id", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, piece_id_))) {
    LOG_WARN("failed to encode piece_id", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, incarnation_))) {
    LOG_WARN("failed to encode incarnation", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, compatible))) {
    LOG_WARN("failed to encode compatible", K(ret));
  } else if (OB_FAIL(start_scn_.fixed_serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize start scn", K(ret));
  } else if (OB_FAIL(checkpoint_scn_.fixed_serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize checkpoint scn", K(ret));
  } else if (OB_FAIL(max_scn_.fixed_serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize max scn", K(ret));
  } else if (OB_FAIL(end_scn_.fixed_serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize end scn", K(ret));
  } else {
    constexpr int64_t reserverd_len = sizeof(reserved_) / sizeof(int64_t);
    for (int64_t i = 0; OB_SUCC(ret) && i < reserverd_len; i++) {
      if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, reserved_[i]))) {
        LOG_WARN("failed to encode reserved", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObPieceCheckpointDesc::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t compatible = 0;
  int64_t tenant_id = 0;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tenant_id))) {
    LOG_WARN("failed to decode tenant_id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &dest_id_))) {
    LOG_WARN("failed to decode dest_id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &round_id_))) {
    LOG_WARN("failed to decode round_id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &piece_id_))) {
    LOG_WARN("failed to decode piece_id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &incarnation_))) {
    LOG_WARN("failed to decode incarnation", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &compatible))) {
    LOG_WARN("failed to decode compatible", K(ret));
  } else if (OB_FAIL(start_scn_.fixed_deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize start scn", K(ret));
  } else if (OB_FAIL(checkpoint_scn_.fixed_deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize checkpoint scn", K(ret));
  } else if (OB_FAIL(max_scn_.fixed_deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize max scn", K(ret));
  } else if (OB_FAIL(end_scn_.fixed_deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize end scn", K(ret));
  } else if (OB_FAIL(compatible_.set_version(compatible))) {
    LOG_WARN("failed to set compatible", K(ret), K(compatible));
  } else {
    tenant_id_ = static_cast<uint64_t>(tenant_id);
    constexpr int64_t reserverd_len = sizeof(reserved_) / sizeof(int64_t);
    for (int64_t i = 0; OB_SUCC(ret) && i < reserverd_len; i++) {
      if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &reserved_[i]))) {
        LOG_WARN("failed to decode reserved", K(ret), K(i));
      }
    }
  }

  return ret;
}

int64_t ObPieceCheckpointDesc::get_serialize_size_() const
{
  return sizeof(tenant_id_) + sizeof(dest_id_) + sizeof(round_id_) + sizeof(piece_id_)
       + sizeof(incarnation_) + sizeof(compatible_.version_)
       + start_scn_.get_fixed_serialize_size() + checkpoint_scn_.get_fixed_serialize_size()
       + max_scn_.get_fixed_serialize_size() + end_scn_.get_fixed_serialize_size() + sizeof(reserved_);
}

bool ObPieceCheckpointDesc::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID && 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_;
}


/**
 * ------------------------------ObPieceInnerPlaceholderDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObPieceInnerPlaceholderDesc, dest_id_, round_id_, piece_id_, start_scn_, checkpoint_scn_);

ObPieceInnerPlaceholderDesc::ObPieceInnerPlaceholderDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_PIECE_INNER_PLACEHOLDER_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  start_scn_ = SCN::min_scn();
  checkpoint_scn_ = SCN::min_scn();
}

bool ObPieceInnerPlaceholderDesc::is_valid() const
{
  return 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_ && start_scn_.is_valid() && start_scn_ <= checkpoint_scn_;
}


/**
 * ------------------------------ObSingleLSInfoDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObSingleLSInfoDesc::OneFile, file_id_, size_bytes_);

OB_SERIALIZE_MEMBER(ObSingleLSInfoDesc, dest_id_, round_id_, piece_id_, ls_id_, start_scn_, checkpoint_scn_, min_lsn_, max_lsn_, filelist_, deleted_);
ObSingleLSInfoDesc::ObSingleLSInfoDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_PIECE_SINGLE_LS_FILE_LIST_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
  start_scn_ = SCN::min_scn();
  checkpoint_scn_ = SCN::min_scn();
  min_lsn_ = 0;
  max_lsn_ = 0;
  deleted_ = false;
}

bool ObSingleLSInfoDesc::is_valid() const
{
  return 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_ && ls_id_.is_valid();
}


/**
 * ------------------------------ObPieceInfoDesc---------------------
 */
OB_SERIALIZE_MEMBER(ObPieceInfoDesc, dest_id_, round_id_, piece_id_, filelist_);

ObPieceInfoDesc::ObPieceInfoDesc()
    : ObExternArchiveDesc(ObBackupFileType::BACKUP_PIECE_FILE_LIST_INFO, FILE_VERSION)
{
  dest_id_ = 0;
  round_id_ = 0;
  piece_id_ = 0;
}

bool ObPieceInfoDesc::is_valid() const
{
  return 0 <= dest_id_ && OB_START_LOG_ARCHIVE_ROUND_ID < round_id_
         && 0 < piece_id_;
}


/**
 * ------------------------------ObArchiveStore---------------------
 */
ObArchiveStore::ObArchiveStore()
  : ObBackupStore()
{}

void ObArchiveStore::reset()
{
  ObBackupStore::reset();
}

// oss://archive/rounds/round_d[dest_id]r[round_id]_start
int ObArchiveStore::is_round_start_file_exist(const int64_t dest_id, const int64_t round_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_start_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round start file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check round start file exist.", K(ret), K(full_path), K(dest));
  }

  return ret;
}

int ObArchiveStore::read_round_start(const int64_t dest_id, const int64_t round_id, ObRoundStartDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_start_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round start file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_round_start(const int64_t dest_id, const int64_t round_id, const ObRoundStartDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_start_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round start file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

// oss://archive/rounds/round_d[dest_id]r[round_id]_end.obarc
int ObArchiveStore::is_round_end_file_exist(const int64_t dest_id, const int64_t round_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_end_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round end file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check round end file exist.", K(ret), K(full_path), K(dest));
  }

  return ret;
}

int ObArchiveStore::read_round_end(const int64_t dest_id, const int64_t round_id, ObRoundEndDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_end_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round end file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_round_end(const int64_t dest_id, const int64_t round_id, const ObRoundEndDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_end_file_path(dest, dest_id, round_id, full_path))) {
    LOG_WARN("failed to get round start file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::get_round_id(const int64_t dest_id, const SCN &scn, int64_t &round_id)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath round_prefix;
  ObLocateRoundFilter round_op;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &backup_dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_rounds_dir_path(backup_dest, round_prefix))) {
    LOG_WARN("failed to get round file prefix", K(ret), K(backup_dest), K(dest_id));
  } else if (OB_FAIL(round_op.init(this, scn))) {
    LOG_WARN("failed to init round op", K(ret), K(scn));
  } else if (OB_FAIL(util.list_files(round_prefix.get_ptr(), storage_info, round_op))) {
    LOG_WARN("list files failed", K(ret), K(round_prefix), K(backup_dest));
  } else {
    ObArray<int64_t> &result = round_op.result();
    lib::ob_sort(result.begin(), result.end());
    if (result.count() > 0) {
      round_id = result.at(0);
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no round match exist", K(ret), K(dest_id), K(scn));
    }
  }
  return ret;
}

int ObArchiveStore::get_round_range(const int64_t dest_id, int64_t &min_round_id, int64_t &max_round_id)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> roundid_array;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_all_round_ids(dest_id, roundid_array))) {
    LOG_WARN("failed to get all round ids", K(ret));
  } else if (roundid_array.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no round exist", K(ret), K(dest_id));
  } else {
    min_round_id = roundid_array.at(0);
    max_round_id = roundid_array.at(roundid_array.count() - 1);
  }

  return ret;
}

int ObArchiveStore::get_all_round_ids(const int64_t dest_id, ObIArray<int64_t> &roundid_array)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath round_prefix;
  ObRoundRangeFilter round_op;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &backup_dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_rounds_dir_path(backup_dest, round_prefix))) {
    LOG_WARN("failed to get round file prefix", K(ret), K(backup_dest), K(dest_id));
  } else if (OB_FAIL(round_op.init(this, dest_id))) {
    LOG_WARN("failed to init round op", K(ret));
  } else if (OB_FAIL(util.list_files(round_prefix.get_ptr(), storage_info, round_op))) {
    LOG_WARN("failed to list files", K(ret), K(round_prefix), K(backup_dest));
  } else {
    ObArray<int64_t> &result = round_op.result();
    lib::ob_sort(result.begin(), result.end());
    if (OB_FAIL(roundid_array.assign(result))) {
      LOG_WARN("failed to assign round id array", K(ret));
    }
  }
  return ret;
}

int ObArchiveStore::get_all_rounds(const int64_t dest_id, ObIArray<ObRoundEndDesc> &rounds)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath round_prefix;
  ObRoundFilter round_op;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &backup_dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_rounds_dir_path(backup_dest, round_prefix))) {
    LOG_WARN("failed to get round file prefix", K(ret), K(backup_dest), K(dest_id));
  } else if (OB_FAIL(round_op.init(this))) {
    LOG_WARN("failed to init round op", K(ret));
  } else if (OB_FAIL(util.list_files(round_prefix.get_ptr(), storage_info, round_op))) {
    LOG_WARN("list files failed", K(ret), K(round_prefix), K(backup_dest));
  } else {
    ObArray<ObRoundEndDesc> &result = round_op.result();
    lib::ob_sort(result.begin(), result.end());
    if (OB_FAIL(rounds.assign(result))) {
      LOG_WARN("failed to assign rounds array", K(ret));
    }
  }
  return ret;
}

// oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_start_20220601T120000.obarc
int ObArchiveStore::is_piece_start_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_start_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece start file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece start file exist.", K(ret), K(full_path), K(dest));
  }

  return ret;
}

int ObArchiveStore::read_piece_start(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, ObPieceStartDesc &desc) const
{
  int ret = OB_SUCCESS;
  const ObBackupDest &dest = get_backup_dest();
  ObBackupPath full_path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_start_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece start file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_piece_start(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, const ObPieceStartDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_start_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece start file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

// oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_end_20220601T120000.obarc
int ObArchiveStore::is_piece_end_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_end_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece end file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece end file exist.", K(ret), K(full_path), K(dest));
  }

  return ret;
}

int ObArchiveStore::read_piece_end(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, ObPieceEndDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_end_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece end file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  }
  return ret;
}

int ObArchiveStore::write_piece_end(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const SCN &create_scn, const ObPieceEndDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_end_file_path(dest, dest_id, round_id, piece_id, create_scn, full_path))) {
    LOG_WARN("failed to get piece end file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id), K(create_scn));
  }
  return ret;
}

int ObArchiveStore::get_piece_range(const int64_t dest_id, const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath piece_prefix;
  ObPieceRangeFilter piece_op;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &backup_dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(backup_dest, piece_prefix))) {
    LOG_WARN("failed to get piece file prefix", K(ret), K(dest_id), K(round_id));
  } else if (OB_FAIL(piece_op.init(this, dest_id, round_id))) {
    LOG_WARN("failed to int ObPieceRangeFilter", K(ret));
  } else if  (OB_FAIL(util.list_files(piece_prefix.get_ptr(), storage_info, piece_op))) {
    LOG_WARN("failed to list files", K(ret), K(piece_prefix), K(backup_dest));
  } else {
    ObArray<int64_t> &result = piece_op.result();
    lib::ob_sort(result.begin(), result.end());
    if (result.count() > 0) {
      min_piece_id = result.at(0);
      max_piece_id = result.at(result.count() - 1);
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no piece exist", K(ret), K(dest_id));
    }
  }
  return ret;
}

// oss://archive/d[dest_id]r[round_id]p[piece_id]/single_piece_info.obarc
int ObArchiveStore::is_single_piece_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_single_piece_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece end file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece end file exist.", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(full_path), K(storage_info));
  } 

  return ret;
}

int ObArchiveStore::read_single_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObSinglePieceDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_single_piece_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get single piece file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id));
  }
  return ret;
}

int ObArchiveStore::write_single_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObSinglePieceDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_single_piece_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get single piece file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id));
  }
  return ret;
}

int ObArchiveStore::read_single_piece(ObSinglePieceDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
   if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_single_piece_file_info_path(dest, full_path))) {
    LOG_WARN("fail to get single piece file path", K(ret), K(dest));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("fail to read single file", K(ret), K(full_path));
  }
  return ret;
}

// oss://archive/d[dest_id]r[round_id]p[piece_id]/checkpoint/checkpoint_info.[file_id].obarc
int ObArchiveStore::is_piece_checkpoint_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_file_path(dest, dest_id, round_id, piece_id, file_id, full_path))) {
    LOG_WARN("failed to get piece checkpoint file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(file_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece checkpoint file exist.", K(ret), K(full_path), K(dest));
  }

  return ret;
}

int ObArchiveStore::read_piece_checkpoint(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, ObPieceCheckpointDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath dir_path;
  ObBackupPath meta_full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else {
    ObArchiveCheckpointMgr mgr;
    uint64_t max_checkpoint_scn = 0;
    if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_file_path(dest, dest_id, round_id, piece_id, 0, meta_full_path))) {
      LOG_WARN("failed to get checkpoint meta file path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id));
    } else if (OB_FAIL(read_single_file(meta_full_path.get_ptr(), desc))) {
      LOG_WARN("failed to read mate file", K(ret), K(meta_full_path));
    } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(dest, dest_id, round_id, piece_id, dir_path))) {
      LOG_WARN("failed to get piece checkpoint dir path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
    } else if (OB_FAIL(mgr.init(dir_path, OB_STR_CHECKPOINT_FILE_NAME, ObBackupFileSuffix::ARCHIVE, get_storage_info()))) {
      LOG_WARN("failed to init ObArchiveCheckPointMgr", K(ret), K(dir_path));
    } else if (OB_FAIL(mgr.read(max_checkpoint_scn))) {
      LOG_WARN("failed to read checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (0 == max_checkpoint_scn) {
      //do nothing
    } else if (OB_FAIL(desc.checkpoint_scn_.convert_for_inner_table_field(max_checkpoint_scn))) {
      LOG_WARN("failed to set checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (OB_FAIL(desc.max_scn_.convert_for_inner_table_field(max_checkpoint_scn))) {
      LOG_WARN("failed to set max scn", K(ret), K(max_checkpoint_scn));
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("succeed to read checkpoint desc.", K(desc));
    }
  }
  return ret;
}

int ObArchiveStore::write_piece_checkpoint(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const int64_t file_id, const ObPieceCheckpointDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  ObBackupPath dir_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else {
    ObArchiveCheckpointMgr mgr;
    bool meta_is_exist = false;
    if (OB_FAIL(is_piece_checkpoint_file_exist(dest_id, round_id, piece_id, 0, meta_is_exist))) {
      LOG_WARN("failed to check checkpoint meta file exist.", K(ret));
    } else if (!meta_is_exist) {
      if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_file_path(dest, dest_id, round_id, piece_id, 0, full_path))) {
        LOG_WARN("failed to get piece checkpoint meta file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
      } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
        LOG_WARN("failed to write checkpoint index file", K(ret), K(desc), K(full_path));
      }
    }
    if (OB_FAIL(ret)) {
      //do noting
    } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(dest, dest_id, round_id, piece_id, dir_path))) {
      LOG_WARN("failed to get piece checkpoint dir path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
    } else if (OB_FAIL(mgr.init(dir_path, OB_STR_CHECKPOINT_FILE_NAME, ObBackupFileSuffix::ARCHIVE, get_storage_info()))) {
      LOG_WARN("failed to init ObArchiveCheckPointMgr", K(ret), K(dir_path));
    } else if (OB_FAIL(mgr.write(desc.checkpoint_scn_.get_val_for_inner_table_field()))) {
      LOG_WARN("failed to write checkpoint info", K(ret), K(desc));
    }
  }
  return ret;
}

int ObArchiveStore::read_piece_checkpoint(ObPieceCheckpointDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath dir_path;
  ObBackupPath meta_full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
  LOG_WARN("ObArchiveStore not init", K(ret));
  } else {
    ObArchiveCheckpointMgr mgr;
    uint64_t max_checkpoint_scn = 0;
    if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_file_path(dest, 0, meta_full_path))) {
      LOG_WARN("failed to get checkpoint meta file path", K(ret), K(dest));
    } else if (OB_FAIL(read_single_file(meta_full_path.get_ptr(), desc))) {
      LOG_WARN("failed to read mate file", K(ret), K(meta_full_path));
    } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(dest, dir_path))) {
      LOG_WARN("failed to get piece checkpoint dir path", K(ret), K(dest));
    } else if (OB_FAIL(mgr.init(dir_path, OB_STR_CHECKPOINT_FILE_NAME, ObBackupFileSuffix::ARCHIVE, get_storage_info()))) {
      LOG_WARN("failed to init ObArchiveCheckPointMgr", K(ret), K(dir_path));
    } else if (OB_FAIL(mgr.read(max_checkpoint_scn))) {
      LOG_WARN("failed to read checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (0 == max_checkpoint_scn) {
      //do nothing, archive is not started yet
    } else if (OB_FAIL(desc.checkpoint_scn_.convert_for_inner_table_field(max_checkpoint_scn))) {
      LOG_WARN("failed to set checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (OB_FAIL(desc.max_scn_.convert_for_inner_table_field(max_checkpoint_scn))) {
      LOG_WARN("failed to set max scn", K(ret), K(max_checkpoint_scn));
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("succeed to read checkpoint desc.", K(desc));
    }
  }
  return ret;
}

// oss://archive/d[dest_id]r[round_id]p[piece_id]/piece_d[dest_id]r[round_id]p[piece_id]_20220601T120000_20220602T120000.obarc
int ObArchiveStore::is_piece_inner_placeholder_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn,
    const SCN &end_scn, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_inner_placeholder_file_path(dest, dest_id, round_id, piece_id, start_scn, end_scn, full_path))) {
    LOG_WARN("failed to get piece inner placeholder file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece inner placeholder file exist.", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn), K(full_path), K(storage_info));
  } 

  return ret;
}

int ObArchiveStore::read_piece_inner_placeholder(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn, const SCN &end_scn, ObPieceInnerPlaceholderDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_inner_placeholder_file_path(dest, dest_id, round_id, piece_id, start_scn, end_scn, full_path))) {
    LOG_WARN("failed to get piece inner placeholder file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  }
  return ret;
}

int ObArchiveStore::write_piece_inner_placeholder(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const SCN &start_scn, const SCN &end_scn, const ObPieceInnerPlaceholderDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_inner_placeholder_file_path(dest, dest_id, round_id, piece_id, start_scn, end_scn, full_path))) {
    LOG_WARN("failed to get piece inner placeholder file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  }
  return ret;
}

// oss://archive/d[dest_id]r[round_id]p[piece_id]/[ls_id]/file_info.obarc
int ObArchiveStore::is_single_ls_info_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id,
    const ObLSID &ls_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_file_info_path(dest, dest_id, round_id, piece_id, ls_id,full_path))) {
    LOG_WARN("failed to get single ls info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check single ls info file exist.", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id), K(full_path), K(storage_info));
  } 

  return ret;
}

int ObArchiveStore::read_single_ls_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObSingleLSInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_file_info_path(dest, dest_id, round_id, piece_id, ls_id, full_path))) {
    LOG_WARN("failed to get single ls info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_single_ls_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, const ObSingleLSInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_file_info_path(dest, dest_id, round_id, piece_id, ls_id, full_path))) {
    LOG_WARN("failed to get single ls info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

// oss://archive/d[dest_id]r[round_id]p[piece_id]/file_info.obarc
int ObArchiveStore::is_piece_info_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_info_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece info file exist.", K(ret), K(full_path), K(storage_info));
  }

  return ret;
}

int ObArchiveStore::read_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObPieceInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_info_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObPieceInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_info_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write single file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::is_tenant_archive_piece_infos_file_exist(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece extend info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece extend info file exist.", K(ret), K(full_path), K(storage_info));
  }

  return ret;
}

int ObArchiveStore::read_tenant_archive_piece_infos(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, ObTenantArchivePieceInfosDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece extend info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read piece extend info file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::write_tenant_archive_piece_infos(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObTenantArchivePieceInfosDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest, dest_id, round_id, piece_id, full_path))) {
    LOG_WARN("failed to get piece extend info file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(write_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to write piece extend info file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::read_tenant_archive_piece_infos(ObTenantArchivePieceInfosDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest, full_path))) {
    LOG_WARN("failed to get piece extend info file path", K(ret), K(dest));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read piece extend info file", K(ret), K(full_path));
  }
  return ret;
}

int ObArchiveStore::is_tenant_archive_piece_infos_file_exist(bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest, full_path))) {
    LOG_WARN("failed to get piece extend info file path", K(ret), K(dest));
  } else if (OB_FAIL(util.is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check piece extend info file exist.", K(ret), K(full_path), K(storage_info));
  }
  return ret;
}

int ObArchiveStore::is_archive_log_file_exist(const int64_t dest_id, const int64_t round_id,
  const int64_t piece_id, const ObLSID &ls_id, const int64_t file_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath full_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(dest, dest_id, round_id, piece_id, ls_id, file_id, full_path))) {
    LOG_WARN("failed to get archive log file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id), K(file_id));
  } else if (OB_FAIL(util.adaptively_is_exist(full_path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check archive log file exist.", K(ret), K(full_path), K(storage_info));
  }

  return ret;
}


int ObArchiveStore::get_all_piece_keys(ObIArray<ObPieceKey> &keys)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath piece_prefix;
  ObPieceFilter piece_op;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &backup_dest = get_backup_dest();

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(backup_dest, piece_prefix))) {
    LOG_WARN("failed to get piece file prefix", K(ret), K(backup_dest));
  } else if (OB_FAIL(piece_op.init(this))) {
    LOG_WARN("failed to init piece op", K(ret));
  } else if (OB_FAIL(util.list_files(piece_prefix.get_ptr(), storage_info, piece_op))) {
    LOG_WARN("failed to list files", K(ret), K(piece_prefix), K(backup_dest));
  } else {
    ObArray<ObPieceKey> &result = piece_op.result();
    lib::ob_sort(result.begin(), result.end());
    if (OB_FAIL(keys.assign(result))) {
      LOG_WARN("failed to assign piece key array", K(ret));
    }
  }
  return ret;
}

int ObArchiveStore::get_single_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, 
    bool &is_empty_piece, ObSinglePieceDesc &single_piece)
{
  int ret = OB_SUCCESS;
  is_empty_piece = false;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(read_single_piece(dest_id, round_id, piece_id, single_piece))) {
    // not a frozen piece, build single piece info with extend and checkpoint info.
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ObPieceCheckpointDesc checkpoint_desc;
      ObTenantArchivePieceInfosDesc extend_desc;
      ret = OB_SUCCESS;
      if (OB_FAIL(read_piece_checkpoint(dest_id, round_id, piece_id, 0, checkpoint_desc))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_empty_piece = true;
        } else {
          LOG_WARN("failed to read piece checkpoint info", K(ret), K(dest_id), K(round_id), K(piece_id));
        }
      } else if (OB_FAIL(read_tenant_archive_piece_infos(dest_id, round_id, piece_id, extend_desc))) {
        LOG_WARN("failed to read piece extend info", K(ret), K(dest_id), K(round_id), K(piece_id));
      } else {
        // merge static piece attr with dynamic attr.
        single_piece.piece_.key_.tenant_id_ = extend_desc.tenant_id_;
        single_piece.piece_.key_.dest_id_ = extend_desc.dest_id_;
        single_piece.piece_.key_.round_id_ = extend_desc.round_id_;
        single_piece.piece_.key_.piece_id_ = extend_desc.piece_id_;
        single_piece.piece_.incarnation_ = extend_desc.incarnation_;
        single_piece.piece_.dest_no_ = extend_desc.dest_no_;
        single_piece.piece_.start_scn_ = extend_desc.start_scn_;
        single_piece.piece_.checkpoint_scn_ = checkpoint_desc.checkpoint_scn_;
        single_piece.piece_.max_scn_ = checkpoint_desc.max_scn_;
        single_piece.piece_.end_scn_ = extend_desc.end_scn_;
        single_piece.piece_.compatible_ = extend_desc.compatible_;
        single_piece.piece_.status_.set_active();
        single_piece.piece_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
        single_piece.piece_.path_ = extend_desc.path_;
      }
    } else {
       LOG_WARN("failed to get single piece info", K(ret), K(dest_id), K(round_id), K(piece_id));
    }
  }

  return ret;
}

int ObArchiveStore::get_whole_piece_info(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, 
    bool &is_empty_piece, ObExternPieceWholeInfo &whole_piece_info)
{
  int ret = OB_SUCCESS;
  ObTenantArchivePieceInfosDesc extend_desc;
  ObSinglePieceDesc single_piece_desc;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_single_piece_info(dest_id, round_id, piece_id, is_empty_piece, single_piece_desc))) {
    LOG_WARN("failed to get whole piece info", K(ret), K(dest_id), K(round_id), K(piece_id));
  } else if (is_empty_piece) {
  } else if (OB_FAIL(read_tenant_archive_piece_infos(dest_id, round_id, piece_id, extend_desc))) {
    LOG_WARN("failed to read piece extend info", K(ret), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(whole_piece_info.current_piece_.assign(single_piece_desc.piece_))) {
    LOG_WARN("failed to assign piece", K(ret));
  } else if (OB_FAIL(whole_piece_info.his_frozen_pieces_.assign(extend_desc.his_frozen_pieces_))) {
    LOG_WARN("failed to assign history frozen pieces", K(ret));
  }
  return ret;
}

int ObArchiveStore::get_single_piece_info(bool &is_empty_piece, ObSinglePieceDesc &single_piece)
{
  int ret = OB_SUCCESS;
  is_empty_piece = false;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(read_single_piece(single_piece))) {
    // not a frozen piece, build single piece info with extend and checkpoint info.
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ObPieceCheckpointDesc checkpoint_desc;
      ObTenantArchivePieceInfosDesc extend_desc;
      ret = OB_SUCCESS;
      if (OB_FAIL(read_piece_checkpoint(checkpoint_desc))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_empty_piece = true;
        } else {
          LOG_WARN("failed to read piece checkpoint info", K(ret));
        }
      } else if (OB_FAIL(read_tenant_archive_piece_infos(extend_desc))) {
        LOG_WARN("failed to read piece extend info", K(ret));
      } else {
        // merge static piece attr with dynamic attr.
        single_piece.piece_.key_.tenant_id_ = extend_desc.tenant_id_;
        single_piece.piece_.key_.dest_id_ = extend_desc.dest_id_;
        single_piece.piece_.key_.round_id_ = extend_desc.round_id_;
        single_piece.piece_.key_.piece_id_ = extend_desc.piece_id_;
        single_piece.piece_.incarnation_ = extend_desc.incarnation_;
        single_piece.piece_.dest_no_ = extend_desc.dest_no_;
        single_piece.piece_.start_scn_ = extend_desc.start_scn_;
        single_piece.piece_.checkpoint_scn_ = checkpoint_desc.checkpoint_scn_;
        single_piece.piece_.max_scn_ = checkpoint_desc.max_scn_;
        single_piece.piece_.end_scn_ = extend_desc.end_scn_;
        single_piece.piece_.compatible_ = extend_desc.compatible_;
        single_piece.piece_.status_.set_active();
        single_piece.piece_.file_status_ = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
        single_piece.piece_.path_ = extend_desc.path_;
      }
    } else {
       LOG_WARN("failed to get single piece info", K(ret));
    }
  }

  return ret;
}

int ObArchiveStore::get_whole_piece_info(bool &is_empty_piece, ObExternPieceWholeInfo &whole_piece_info)
{
  int ret = OB_SUCCESS;
  ObTenantArchivePieceInfosDesc extend_desc;
  ObSinglePieceDesc single_piece_desc;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_single_piece_info(is_empty_piece, single_piece_desc))) {
    LOG_WARN("failed to get whole piece info", K(ret));
  } else if (is_empty_piece) {
  } else if (OB_FAIL(read_tenant_archive_piece_infos(extend_desc))) {
    LOG_WARN("failed to read piece extend info", K(ret));
  } else if (OB_FAIL(whole_piece_info.current_piece_.assign(single_piece_desc.piece_))) {
    LOG_WARN("failed to assign piece", K(ret));
  } else if (OB_FAIL(whole_piece_info.his_frozen_pieces_.assign(extend_desc.his_frozen_pieces_))) {
    LOG_WARN("failed to assign history frozen pieces", K(ret));
  }
  return ret;
}


// Get pieces needed in the specific interval indicated by 'start_scn' and 'end_scn'.
int ObArchiveStore::get_piece_paths_in_range(const SCN &start_scn, const SCN &end_scn, ObIArray<share::ObRestoreLogPieceBriefInfo> &pieces)
{
  int ret = OB_SUCCESS;
  ObArray<ObPieceKey> piece_keys;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (start_scn >= end_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_scn), K(end_scn));
  } else if (OB_FAIL(get_all_piece_keys(piece_keys))) {
    LOG_WARN("failed to get all piece keys", K(ret));
  }

  // The input parameter 'start_scn' may be 0.
  const ObBackupDest &dest = get_backup_dest();
  bool is_empty_piece = true;
  ObExternPieceWholeInfo piece_whole_info;
  for (int64_t i = piece_keys.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    const ObPieceKey &key = piece_keys.at(i);
    if (OB_FAIL(get_whole_piece_info(key.dest_id_, key.round_id_, key.piece_id_, is_empty_piece, piece_whole_info))) {
      LOG_WARN("failed to get whole piece info", K(ret), K(key));
    } else if (!is_empty_piece) {
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_empty_piece) {
    // no piece exist
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no piece is found", K(ret), K(start_scn), K(end_scn));
    LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "No enough log for restore");
  } else if (OB_FAIL(piece_whole_info.his_frozen_pieces_.push_back(piece_whole_info.current_piece_))) {
     LOG_WARN("failed to push backup piece", K(ret));
  } else {
    const int64_t dest_id = piece_keys.at(0).dest_id_;
    int64_t last_piece_idx = -1;
    int64_t i = 0;
    int64_t pieces_cnt = piece_whole_info.his_frozen_pieces_.count();
    while (OB_SUCC(ret) && i < pieces_cnt) {
      const ObTenantArchivePieceAttr &cur = piece_whole_info.his_frozen_pieces_.at(i);
      ObBackupPath piece_path;
      if (cur.key_.dest_id_ != dest_id) {
        // Filter pieces archived at other path.
        ++i;
        continue;
      }

      if (cur.file_status_ != ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE) {
        // Filter unavailable piece
        ++i;
        continue;
      }

      if (cur.end_scn_ <= start_scn) {
        ++i;
        continue;
      }
      ObRestoreLogPieceBriefInfo piece_brief_info;
      if (cur.start_scn_ >= end_scn) {
        // this piece may be required for restore, consider the following case.
        // Piece#1 : <2022-06-01 06:00:00, 2022-06-02 05:00:00, 2022-06-02 06:00:00>
        // Piece#2 : <2022-06-02 06:00:00, 2022-06-03 05:00:00, 2022-06-03 06:00:00>
        // Piece#3 : <2022-06-03 06:00:00, 2022-06-03 10:00:00, 2022-06-04 06:00:00>
        // If 'end_scn' is indicated to ' 2022-06-03 05:30:00', Piece#3 is required.
        if (!pieces.empty()) {
          const ObTenantArchivePieceAttr &prev = piece_whole_info.his_frozen_pieces_.at(last_piece_idx);
          // If pieces are not enough, and current piece is continous with previous one.
          if (prev.end_scn_ == cur.start_scn_ && prev.checkpoint_scn_ < end_scn) {
            if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dest, cur.key_.dest_id_, cur.key_.round_id_, cur.key_.piece_id_, piece_path))) {
              LOG_WARN("failed to get piece path", K(ret), K(dest), K(cur));
            } else if (OB_FAIL(piece_brief_info.piece_path_.assign(piece_path.get_obstr()))) {
              LOG_WARN("failed to assign piece path", K(ret));
            } else if (OB_FALSE_IT(piece_brief_info.piece_id_ = cur.key_.piece_id_)) {
            } else if (OB_FALSE_IT(piece_brief_info.start_scn_ = cur.start_scn_)) {
            } else if (OB_FALSE_IT(piece_brief_info.checkpoint_scn_ = cur.checkpoint_scn_)) {
            } else if (OB_FAIL(pieces.push_back(piece_brief_info))) {
              LOG_WARN("fail to push back path", K(ret), K(piece_brief_info));
            } else {
              last_piece_idx = i;
            }
          }
        }
        break;
      }
      if (pieces.empty()) {
        // this piece may be used to restore.
        if (cur.start_scn_ <= start_scn) {
          if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dest, cur.key_.dest_id_, cur.key_.round_id_, cur.key_.piece_id_, piece_path))) {
            LOG_WARN("failed to get piece path", K(ret), K(dest), K(cur));
          } else if (OB_FAIL(piece_brief_info.piece_path_.assign(piece_path.get_obstr()))) {
            LOG_WARN("failed to assign piece path", K(ret));
          } else if (OB_FALSE_IT(piece_brief_info.piece_id_ = cur.key_.piece_id_)) {
          } else if (OB_FALSE_IT(piece_brief_info.start_scn_ = cur.start_scn_)) {
          } else if (OB_FALSE_IT(piece_brief_info.checkpoint_scn_ = cur.checkpoint_scn_)) {
          } else if (OB_FAIL(pieces.push_back(piece_brief_info))) {
            LOG_WARN("fail to push back path", K(ret), K(piece_brief_info));
          } else {
            last_piece_idx = i;
            ++i;
          }
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("first piece start_scn is bigger than start_scn", K(ret), K(cur), K(start_scn), K(end_scn));
          LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "No enough log for restore");
        }
      } else {
        const ObTenantArchivePieceAttr &prev = piece_whole_info.his_frozen_pieces_.at(last_piece_idx);
        if (prev.end_scn_ != cur.start_scn_) {
          // The <start_scn, checkpoint_scn, end_scn> of pieces are as following:
          // Piece#1 : <2022-06-01 00:00:00, 2022-06-01 06:00:00, 2022-06-02 00:00:00>
          // Piece#2 : <2022-06-01 08:00:00, 2022-06-02 07:59:00, 2022-06-02 08:00:00>
          // Piece#3 : <2022-06-02 08:00:00, 2022-06-03 06:00:00, 2022-06-03 08:00:00>

          // And the input [start_scn, end_scn] pair is [2022-06-01 12:00:00, 2022-06-03 04:00:00].

          //  Previously, Piece#1 is required, and pushed into 'pieces'. However, when i = 1,
          //  we find that Piece#2 is not continous with Piece#1, and Piece#1 is not required actually.
          //  Then Piece#1 is abandoned, and recompute the required pieces.
          pieces.reset();
          last_piece_idx = -1;
          // Do not do ++i, recompute if current piece can be used to restore.
          LOG_INFO("pieces are not continous", K(prev), K(cur), K(start_scn), K(end_scn));
        } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dest, cur.key_.dest_id_, cur.key_.round_id_, cur.key_.piece_id_, piece_path))) {
          LOG_WARN("failed to get piece path", K(ret), K(dest), K(cur));
        } else if (OB_FAIL(piece_brief_info.piece_path_.assign(piece_path.get_obstr()))) {
          LOG_WARN("failed to assign piece path", K(ret));
        } else if (OB_FALSE_IT(piece_brief_info.piece_id_ = cur.key_.piece_id_)) {
        } else if (OB_FALSE_IT(piece_brief_info.start_scn_ = cur.start_scn_)) {
        } else if (OB_FALSE_IT(piece_brief_info.checkpoint_scn_ = cur.checkpoint_scn_)) {
        } else if (OB_FAIL(pieces.push_back(piece_brief_info))) {
          LOG_WARN("fail to push back path", K(ret), K(piece_path));
        } else {
          last_piece_idx = i;
          ++i;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (-1 == last_piece_idx) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no enough log for restore", K(ret), K(last_piece_idx), K(end_scn), K(piece_keys));
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "No enough log for restore");
    } else {
      const ObTenantArchivePieceAttr &last_piece = piece_whole_info.his_frozen_pieces_.at(last_piece_idx);
      if (last_piece.checkpoint_scn_ < end_scn) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no enough log for restore", K(ret), K(last_piece), K(end_scn));
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "No enough log for restore");
      }
    }
  }

  if (OB_FAIL(ret)) {
    pieces.reset();
  } else {
    LOG_INFO("find pieces", K(ret), K(start_scn), K(end_scn), K(pieces));
  }
  
  return ret;
}

int ObArchiveStore::get_file_range_in_piece(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, int64_t &min_file_id, int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObSingleLSInfoDesc::OneFile> filelist;
  if (OB_FAIL(get_file_list_in_piece(
              dest_id,
              round_id,
              piece_id,
              ls_id,
              filelist))) {
    LOG_WARN("failed to get file list", K(ret), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else if (filelist.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("file not exist", K(ret), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else {
    lib::ob_sort(filelist.begin(), filelist.end());
    min_file_id = filelist.at(0).file_id_;
    max_file_id = filelist.at(filelist.count() - 1).file_id_;
  }

  return ret;
}

int ObArchiveStore::get_file_list_in_piece(const int64_t dest_id, const int64_t round_id,
    const int64_t piece_id, const ObLSID &ls_id, ObIArray<ObSingleLSInfoDesc::OneFile> &filelist) const
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath piece_path;
  const ObBackupStorageInfo *storage_info = get_storage_info();
  const ObBackupDest &dest = get_backup_dest();
  ObLSFileListOp op;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(dest, dest_id, round_id, piece_id, ls_id, piece_path))) {
    LOG_WARN("get piece ls dir path failed", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(op.init(this, &filelist))) {
    LOG_WARN("ObLSFileListOp init failed", K(ret));
  } else if (OB_FAIL(util.adaptively_list_files(piece_path.get_ptr(), storage_info, op))) {
    LOG_WARN("list files failed", K(ret), K(piece_path), K(dest));
  }
  return ret;
}

int ObArchiveStore::get_max_checkpoint_scn(const int64_t dest_id, int64_t &round_id, 
    int64_t &piece_id, SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  int64_t min_round_id = 0;
  int64_t max_round_id = 0;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_round_range(dest_id, min_round_id, max_round_id))) {
    LOG_WARN("failed to get round range", K(ret), K(dest_id));
  } else if (OB_FALSE_IT(round_id = max_round_id)) {
  } else if (OB_FAIL(get_round_max_checkpoint_scn(dest_id, round_id, piece_id, max_checkpoint_scn))) {
    LOG_WARN("failed to get round max checkpoint scn", K(ret), K(dest_id));
  }

  return ret;
}

int ObArchiveStore::get_round_max_checkpoint_scn(const int64_t dest_id, const int64_t round_id, 
    int64_t &piece_id, SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  int64_t min_piece_id = 0;
  int64_t max_piece_id = 0;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_piece_range(dest_id, round_id, min_piece_id, max_piece_id))) {
    LOG_WARN("failed to get piece range", K(ret), K(dest_id), K(round_id));
  } else {
    piece_id = max_piece_id;
    // filter empty piece
    while (OB_SUCC(ret)) {
      if (piece_id < min_piece_id) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no valid piece exist", K(ret), K(dest_id), K(round_id), K(min_piece_id), K(max_piece_id), K(piece_id), K(max_checkpoint_scn));
      } else if (OB_FAIL(get_piece_max_checkpoint_scn(dest_id, round_id, piece_id, max_checkpoint_scn))) {
        LOG_WARN("failed to get piece max checkpoint scn", K(ret), K(dest_id), K(round_id), K(min_piece_id), K(max_checkpoint_scn));
      } else if (max_checkpoint_scn.is_min()) {
        // empty piece
        LOG_INFO("ignore empty piece", K(dest_id), K(round_id), K(min_piece_id), K(max_piece_id), K(piece_id));
        --piece_id;
      } else {
        LOG_INFO("get max checkpoint scn", K(dest_id), K(round_id), K(min_piece_id), K(max_piece_id), K(piece_id), K(max_checkpoint_scn));
        break;
      }
    }
  }

  return ret;
}

int ObArchiveStore::get_piece_max_checkpoint_scn(const int64_t dest_id, const int64_t round_id, 
    const int64_t piece_id, SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  bool is_empty_piece = false;
  ObSinglePieceDesc single_piece_desc;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(get_single_piece_info(dest_id, round_id, piece_id, is_empty_piece, single_piece_desc))) {
    LOG_WARN("failed to get single piece info", K(ret), K(dest_id), K(round_id), K(piece_id));
  } else if (is_empty_piece) {
    max_checkpoint_scn = SCN::min_scn();
  } else {
    max_checkpoint_scn = single_piece_desc.piece_.checkpoint_scn_;
  }

  return ret;
}

int ObArchiveStore::read_single_ls_info(const ObLSID &ls_id, ObSingleLSInfoDesc &desc) const
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  const ObBackupDest &dest = get_backup_dest();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchiveStore not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_file_info_path(dest, ls_id, full_path))) {
    LOG_WARN("fail to get ls file info path", K(ret), K(dest), K(ls_id));
  } else if (OB_FAIL(read_single_file(full_path.get_ptr(), desc))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

static int parse_piece_file_(ObString &dir_name, int64_t &dest_id, int64_t &round_id, int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  if (3 != sscanf(dir_name.ptr(), "piece_d%ldr%ldp%ld", &dest_id, &round_id, &piece_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid piece placeholder name", K(dir_name));
  } else {
    LOG_INFO("succeed to parse dest and round and piece id", K(dest_id), K(round_id), K(piece_id));
  }
  return ret;
}


static int is_piece_start_file_name_(ObString &file_name, bool &is_piece_start)
{
  // parse file name like 'piece_d[dest_id]r[round_id]p[piece_id]_start_20220601T120000.obarc'
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  is_piece_start = false;
  const int32_t len = static_cast<int32_t>(file_name.length() - strlen(OB_ARCHIVE_SUFFIX));
  if (file_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty file name", K(ret), K(file_name));
  } else if (len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file name without a unified suffix", K(ret), K(file_name.length()), K(len));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", len, file_name.ptr()))) {
    LOG_WARN("fail to save tmp file name", K(ret), K(file_name));
  } else {
    const char *PREFIX = "piece";
    const char *SUFFIX = "start";

    char *token = tmp_str;
    char *saveptr = nullptr;
    char *p_end = nullptr;

    int64_t dest_id = 0;
    int64_t round_id = 0;
    int64_t piece_id = 0;

    int i = 0;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, "_", &saveptr);
      if (nullptr == token) {
        break;
      } else if (0 == i && 0 != STRCASECMP(token, PREFIX)) {
        // must be start with 'piece'
        is_piece_start = false;
        break;
      } else if (1 == i && 3 != sscanf(token, "d%ldr%ldp%ld", &dest_id, &round_id, &piece_id)) {
        is_piece_start = false;
        break;
      } else if (2 == i) {
        // must be end with 'start'
        if (0 == STRCASECMP(token, SUFFIX)) {
          is_piece_start = true;
        } else {
          is_piece_start = false;
        }
        break;
      }

      ++i;
    }
  }

  return ret;
}


ObArchiveStore::ObPieceRangeFilter::ObPieceRangeFilter()
  : is_inited_(false), store_(nullptr), dest_id_(0), round_id_(0), pieces_()
{}

int ObArchiveStore::ObPieceRangeFilter::init(ObArchiveStore *store, const int64_t dest_id, const int64_t round_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPieceRangeFilter init twice.", K(ret));
  } else if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid store", K(ret), K(store));
  } else if (OB_UNLIKELY(dest_id <= 0 || round_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest or round id", K(ret), K(dest_id), K(round_id));
  } else {
    store_ = store;
    dest_id_ = dest_id;
    round_id_ = round_id;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObPieceRangeFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString file_name(entry->d_name);
  bool is_piece_start = false;
  int64_t dest_id = 0;
  int64_t round_id = 0;
  int64_t piece_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPieceRangeFilter not init", K(ret));
  } else if (pieces_.count() >= OB_MAX_BACKUP_PIECE_NUM) { // list upper limit //TODO(zeyong) add new error code
  } else if (OB_FAIL(is_piece_start_file_name_(file_name, is_piece_start))) {
    LOG_WARN("failed to check piece start file name", K(ret), K(file_name));
  } else if (! is_piece_start) {
    LOG_INFO("skip not piece start file", K(ret), K(file_name));
  } else if (OB_FAIL(parse_piece_file_(file_name, dest_id, round_id, piece_id))) {
    LOG_WARN("failed to parse dir name", K(ret), K(file_name));
  } else if (dest_id != dest_id_ || round_id != round_id_) {
    LOG_WARN("dest or round id not match", K(file_name), K(dest_id_), K(round_id_));
  } else if (OB_FAIL(pieces_.push_back(piece_id))) {
    LOG_WARN("push back failed", K(ret), K(piece_id));
  }
  return ret;
}

static int is_round_start_file_name_(ObString &file_name, bool &is_round_start)
{
  int ret = OB_SUCCESS;
  const char *PREFIX = "round_";
  const char *SUFFIX = "_start";
  int64_t suffix_len = strlen(SUFFIX) + strlen(OB_ARCHIVE_SUFFIX);
  int64_t p = file_name.length() - suffix_len;
  ObString tmp;
  if (p <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file name without a unified suffix", K(ret), K(p));
  } else if (!file_name.prefix_match(PREFIX)) {
    is_round_start = false;
  } else if (file_name.length() < suffix_len) {
    is_round_start = false;
  } else if (OB_FALSE_IT(tmp.assign(file_name.ptr() + p, static_cast<int32_t>(strlen(SUFFIX))))) {
  } else if (!tmp.prefix_match(SUFFIX)) {
    is_round_start = false;
  } else {
    is_round_start = true;
  }
  return ret;
}

static int parse_round_file_(ObString &dir_name, int64_t &dest_id, int64_t &round_id)
{
  int ret = OB_SUCCESS;
  if (2 != sscanf(dir_name.ptr(), "round_d%ldr%ld", &dest_id, &round_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round placeholder name", K(dir_name));
  } else {
    LOG_INFO("succeed to parse dest and round id", K(dest_id), K(round_id));
  }
  return ret;
}


ObArchiveStore::ObRoundFilter::ObRoundFilter()
  : is_inited_(false), store_(nullptr), rounds_()
{}

int ObArchiveStore::ObRoundFilter::init(ObArchiveStore *store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRoundFilter init twice", K(ret));
  } else if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid store", K(ret), K(store));
  } else {
    store_ = store;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObRoundFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString dir_name(entry->d_name);
  bool is_round_start = false;
  int64_t dest_id = 0;
  int64_t round_id = 0;
  ObRoundStartDesc start_desc;
  ObRoundEndDesc end_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoundFilter not init", K(ret));
  } else if (OB_FAIL(is_round_start_file_name_(dir_name, is_round_start))) {
    LOG_WARN("failed to check round start file name", K(ret), K(dir_name));
  } else if (!is_round_start) {
    LOG_INFO("skip not round start file", K(dir_name));
  } else if (OB_FAIL(parse_round_file_(dir_name, dest_id, round_id))) {
    LOG_WARN("failed to parse dir name", K(ret), K(dir_name));
  } else if (OB_FAIL(store_->read_round_end(dest_id, round_id, end_desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(store_->read_round_start(dest_id, round_id, start_desc))) {
        LOG_WARN("failed to read round start file", K(ret), K(dest_id), K(round_id));
      } else if (OB_UNLIKELY(! start_desc.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("round start file is invalid", K(ret), K(dest_id), K(round_id), K(start_desc));
      } else if (OB_FAIL(end_desc.assign(start_desc))) {
        LOG_WARN("failed to assign start desc", K(ret), K(dest_id), K(round_id), K(start_desc));
      }
    } else {
      LOG_WARN("failed to read round end file", K(ret), K(dest_id), K(round_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rounds_.push_back(end_desc))) {
    LOG_WARN("failed to push backup round end desc", K(ret));
  }

  return ret;
}

ObArchiveStore::ObPieceFilter::ObPieceFilter()
  : is_inited_(false), store_(nullptr), piece_keys_()
{}

int ObArchiveStore::ObPieceFilter::init(ObArchiveStore *store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPieceFilter init twice", K(ret));
  } else if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid store", K(ret), K(store));
  } else {
    store_ = store;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObPieceFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString file_name(entry->d_name);
  bool is_piece_start = false;
  ObPieceKey key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPieceFilter not init", K(ret));
  } else if (OB_FAIL(is_piece_start_file_name_(file_name, is_piece_start))) {
    LOG_WARN("failed to check piece start file name", K(ret), K(file_name));
  } else if (! is_piece_start) {
    LOG_INFO("skip not piece start file", K(file_name));
  } else if (OB_FAIL(parse_piece_file_(file_name, key.dest_id_, key.round_id_, key.piece_id_))) {
    LOG_WARN("failed to parse piece key", K(ret), K(file_name));
  } else if (OB_FAIL(piece_keys_.push_back(key))) {
    LOG_WARN("push back failed", K(ret), K(key));
  }
  return ret;
}


ObArchiveStore::ObLocateRoundFilter::ObLocateRoundFilter()
  : is_inited_(false), store_(nullptr), scn_(), rounds_()
{}

int ObArchiveStore::ObLocateRoundFilter::init(ObArchiveStore *store, const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLocateRoundFilter init twice", K(ret));
  } else if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid store", K(ret), K(store));
  } else if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scn", K(ret), K(scn));
  } else {
    store_ = store;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObLocateRoundFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString dir_name(entry->d_name);
  bool is_round_start = false;
  int64_t dest_id = 0;
  int64_t round_id = 0;
  ObRoundStartDesc start_desc;
  ObRoundEndDesc end_desc;
  bool end_file_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLocateRoundFilter not init", K(ret));
  } else if (OB_FAIL(is_round_start_file_name_(dir_name, is_round_start))) {
    LOG_WARN("failed to check round start file name", K(ret), K(dir_name));
  } else if (! is_round_start) {
    LOG_WARN("skip not round start file", K(ret), K(dir_name));
  } else if (OB_FAIL(parse_round_file_(dir_name, dest_id, round_id))) {
    LOG_WARN("failed to parse dir name", K(ret), K(dir_name));
  } else if (OB_FAIL(store_->read_round_start(dest_id, round_id, start_desc))) {
    LOG_WARN("round start file not exist", K(ret), K(dest_id), K(round_id));
  } else if (OB_UNLIKELY(! start_desc.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("round start file is invalid", K(ret), K(dest_id), K(round_id), K(start_desc));
  } else if (OB_FAIL(store_->is_round_end_file_exist(dest_id, round_id, end_file_exist))) {
    LOG_WARN("failed to check round end file exist", K(ret), K(dest_id), K(round_id));
  } else if (! end_file_exist) {
    LOG_INFO("active round", K(dest_id), K(round_id));
  } else if (OB_FAIL(store_->read_round_end(dest_id, round_id, end_desc))) {
    LOG_WARN("failed to read round end file", K(ret), K(dest_id), K(round_id));
  }

  if (OB_FAIL(ret) || ! is_round_start) {
  } else if (end_file_exist && end_desc.checkpoint_scn_ <= scn_) {
    LOG_INFO("scn bigger than round max checkpoint_scn, skip it", K(dest_id), K(round_id), K(start_desc), K(end_file_exist), K(end_desc));
  } else if (OB_FAIL(rounds_.push_back(round_id))) {
    LOG_WARN("push back failed", K(ret), K(round_id), K(start_desc), K(end_desc), K_(scn));
  } else {
    LOG_INFO("round may be match, add round succ", K(dest_id), K(round_id), K_(scn), K(start_desc), K(end_desc));
  }

  return ret;
}

ObArchiveStore::ObRoundRangeFilter::ObRoundRangeFilter()
  : is_inited_(false), store_(nullptr), dest_id_(0), rounds_()
{}

int ObArchiveStore::ObRoundRangeFilter::init(ObArchiveStore *store, const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRoundRangeFilter init twice.", K(ret));
  } else if (OB_ISNULL(store) || dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(store), K(dest_id));
  } else {
    store_ = store;
    dest_id_ = dest_id;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObRoundRangeFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString file_name(entry->d_name);
  bool is_round_start = false;
  int64_t dest_id = 0;
  int64_t round_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoundRangeFilter not init", K(ret));
  } else if (OB_FAIL(is_round_start_file_name_(file_name, is_round_start))) {
    LOG_WARN("failed to check round start file name", K(ret), K(file_name));
  } else if (! is_round_start) {
    LOG_WARN("skip not round start file", K(ret), K(file_name));
  } else if (OB_FAIL(parse_round_file_(file_name, dest_id, round_id))) {
    LOG_WARN("failed to parse dir name", K(ret), K(file_name));
  } else if (dest_id != dest_id_) {
    LOG_WARN("dest id not match", K(dest_id_), K(file_name));
  } else if (OB_FAIL(rounds_.push_back(round_id))) {
    LOG_WARN("push back failed", K(ret), K(round_id));
  }
  return ret;
}

ObArchiveStore::ObLSFileListOp::ObLSFileListOp()
  : is_inited_(false), store_(nullptr), filelist_(nullptr)
{}

int ObArchiveStore::ObLSFileListOp::init(const ObArchiveStore *store, ObIArray<ObSingleLSInfoDesc::OneFile> *filelist)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSFileListOp init twice", K(ret));
  } else if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid store", K(ret), KPC(store));
  } else if (OB_ISNULL(filelist)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid filelist", K(ret), KPC(filelist));
  } else {
    store_ = store;
    filelist_ = filelist;
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveStore::ObLSFileListOp::func(const dirent *entry)
{
  // The format of archive file name is like '100.obarc'.
  int ret = OB_SUCCESS;
  ObSingleLSInfoDesc::OneFile one_file;
  char *endptr = NULL;
  const char *filename = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSFileListOp not init", K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", K(ret));
  } else if (OB_FALSE_IT(filename = entry->d_name)) {
  } else if (STRLEN(filename) <= STRLEN(OB_ARCHIVE_SUFFIX)) {
    LOG_WARN("ignore invalid file name", KCSTRING(filename));
  } else if (OB_FAIL(ob_strtoll(filename, endptr, one_file.file_id_))) {
    LOG_WARN("ignore invalid file name", K(ret), KCSTRING(filename));
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(endptr) || 0 != STRCMP(endptr, OB_ARCHIVE_SUFFIX)) {
    LOG_WARN("ignore invalid file name", KCSTRING(filename));
  } else if (OB_FALSE_IT(one_file.size_bytes_ = get_size())) {
  } else if (OB_FAIL(filelist_->push_back(one_file))) {
    LOG_WARN("push back failed", K(ret), K(one_file));
  } else {
    LOG_INFO("find one archive file", KCSTRING(filename), "bytes", one_file.size_bytes_);
  }
  return ret;
}
