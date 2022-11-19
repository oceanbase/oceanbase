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
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/ob_define.h"

using namespace oceanbase;
using namespace common;
using namespace share;

/**
 * ------------------------------ObArchiveStore---------------------
 */
const char * const ObArchivePathUtil::ROUNDS_DIR_PATH = "rounds";
const char * const ObArchivePathUtil::PIECES_DIR_PATH = "pieces";


int ObArchivePathUtil::build_restore_prefix(const char *base,
    const share::ObLSID &id,
    share::ObBackupPath &prefix)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefix.init(base))) {
    ARCHIVE_LOG(WARN, "init prefix failed", K(ret), K(base));
  } else if (OB_FAIL(prefix.join_ls(id))) {
    ARCHIVE_LOG(WARN, "join ls failed", K(ret), K(id));
  } else {
    ARCHIVE_LOG(INFO, "build restore prefix succ", K(prefix));
  }
  return ret;
}

int ObArchivePathUtil::build_restore_path(const char *base,
    const share::ObLSID &id,
    const int64_t file_id,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_FAIL(path.init(base))) {
    ARCHIVE_LOG(WARN, "init prefix failed", K(ret), K(base));
  } else if (OB_FAIL(path.join_ls(id))) {
    ARCHIVE_LOG(WARN, "join ls failed", K(ret), K(id));
  } else if (OB_FAIL(path.join(file_id))) {
    ARCHIVE_LOG(WARN, "join file_id failed", K(ret), K(id), K(file_id));
  } else {
    ARCHIVE_LOG(INFO, "build restore prefix succ", K(path));
  }
  return ret;
}

// oss://archive/rounds
int ObArchivePathUtil::get_rounds_dir_path(const ObBackupDest &dest, ObBackupPath &rounds_path)
{
  int ret = OB_SUCCESS;
  rounds_path.reset();
  if (OB_FAIL(rounds_path.init(dest.get_root_path()))) {
    LOG_WARN("failed to assign dest path", K(ret), K(dest));
  } else if (OB_FAIL(rounds_path.join(ROUNDS_DIR_PATH))) {
    LOG_WARN("failed to join path", K(ret), K(rounds_path));
  }
  return ret;
}

// oss://archive/rounds/round_d[dest_id]r[round_id]_start
int ObArchivePathUtil::get_round_start_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(get_rounds_dir_path(dest, path))) {
    LOG_WARN("failed to get rounds dir path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "round_d%ldr%ld_start", dest_id, round_id))) {
    LOG_WARN("failed to assign round start file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join path", K(ret), K(path), K(file_name)); 
  }
  return ret;
}

// oss://archive/rounds/round_d[dest_id]r[round_id]_end
int ObArchivePathUtil::get_round_end_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(get_rounds_dir_path(dest, path))) {
    LOG_WARN("failed to get rounds dir path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "round_d%ldr%ld_end", dest_id, round_id))) {
    LOG_WARN("failed to assign round end file path", K(ret), K(dest), K(dest_id), K(round_id));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join file name", K(ret), K(file_name), K(path));
  }
  return ret;
}

// oss://archive/pieces
int ObArchivePathUtil::get_pieces_dir_path(const ObBackupDest &dest, ObBackupPath &pieces_path)
{
  int ret = OB_SUCCESS;
  pieces_path.reset();
  if (OB_FAIL(pieces_path.init(dest.get_root_path()))) {
    LOG_WARN("failed to assign dest path", K(ret), K(dest));
  } else if (OB_FAIL(pieces_path.join(PIECES_DIR_PATH))) {
    LOG_WARN("failed to join path", K(ret), K(pieces_path));
  }
  return ret;
}

// // oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_start_20220601T120000
int ObArchivePathUtil::get_piece_start_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const uint64_t start_scn, 
    ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  char buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  if (OB_FAIL(backup_scn_to_time_tag(start_scn, buff, sizeof(buff), pos))) {
    LOG_WARN("failed to format time tag", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn));
  } else if (OB_FAIL(get_pieces_dir_path(dest, path))) {
    LOG_WARN("failed to get pieces dir path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "piece_d%ldr%ldp%ld_start_%s", dest_id, round_id, piece_id, buff))) {
    LOG_WARN("failed to assign piece start file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(buff));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join file name", K(ret), K(path), K(file_name)); 
  }
  return ret;
}

// oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_end_20220601T120000
int ObArchivePathUtil::get_piece_end_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const uint64_t end_scn, 
    ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  char buff[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  if (OB_FAIL(backup_scn_to_time_tag(end_scn, buff, sizeof(buff), pos))) {
    LOG_WARN("failed to convert scn to string", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(end_scn));
  } else if (OB_FAIL(get_pieces_dir_path(dest, path))) {
    LOG_WARN("failed to get pieces dir path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(end_scn));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "piece_d%ldr%ldp%ld_end_%s", dest_id, round_id, piece_id, buff))) {
    LOG_WARN("failed to assign piece end file path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(end_scn), K(buff));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join file name", K(ret), K(path), K(file_name)); 
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]
int ObArchivePathUtil::get_piece_dir_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  char dir_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(path.init(dest.get_root_path()))) {
    LOG_WARN("failed to assign dest path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(databuff_printf(dir_name, sizeof(dir_name), "piece_d%ldr%ldp%ld", dest_id, round_id, piece_id))) {
    LOG_WARN("failed to assign piece dir path", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(path.join(dir_name))) {
    LOG_WARN("failed to join dir name", K(ret), K(path), K(dir_name)); 
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/single_piece_info
int ObArchivePathUtil::get_single_piece_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id));
  } else if (OB_FAIL(path.join("single_piece_info"))) {
    LOG_WARN("failed to join single piece info", K(ret), K(path));
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/checkpoint
int ObArchivePathUtil::get_piece_checkpoint_dir_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id));
  } else if (OB_FAIL(path.join("checkpoint"))) { 
    LOG_WARN("failed to join checkpoint dir path", K(ret), K(path));
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/checkpoint/checkpoint_info_[file_id]
int ObArchivePathUtil::get_piece_checkpoint_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(get_piece_checkpoint_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(file_id));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "checkpoint_info_%ld", file_id))) {
    LOG_WARN("failed to assign checkpoint info path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(file_id));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join file name", K(ret), K(path), K(file_name)); 
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/piece_d[dest_id]r[round_id]p[piece_id]_20220601T120000_20220602T120000
int ObArchivePathUtil::get_piece_inner_placeholder_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const uint64_t start_scn, const uint64_t end_scn, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buff1[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  char buff2[OB_BACKUP_MAX_TIME_STR_LEN] = { 0 };
  char file_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(backup_scn_to_time_tag(start_scn, buff1, sizeof(buff1), pos))) {
    LOG_WARN("failed to format time tag", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(backup_scn_to_time_tag(end_scn, buff2, sizeof(buff2), pos))) {
    LOG_WARN("failed to format time tag", K(ret), K(dest), K(dest_id), K(round_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "piece_d%ldr%ldp%ld_%s_%s", dest_id, round_id, piece_id, buff1, buff2))) {
    LOG_WARN("failed to assign piece inner placeholder file path", K(ret), K(dest), K(dest_id), 
      K(round_id), K(piece_id), K(start_scn), K(end_scn), K(buff1), K(buff2));
  } else if (OB_FAIL(path.join(file_name))) {
    LOG_WARN("failed to join file name", K(ret), K(path), K(file_name)); 
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[ls_id]
int ObArchivePathUtil::get_piece_ls_dir_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char dir_name[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(databuff_printf(dir_name, sizeof(dir_name), "logstream_%ld", ls_id.id()))) {
    LOG_WARN("failed to assign ls dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(path.join(dir_name))) {
    LOG_WARN("failed to join dir name", K(ret), K(path), K(dir_name)); 
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[ls_id]/ls_file_info
int ObArchivePathUtil::get_single_ls_info_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_piece_ls_dir_path(dest, dest_id, round_id, piece_id, ls_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(ls_id));
  } else if (OB_FAIL(path.join("ls_file_info"))) {
    LOG_WARN("failed to join ls file info ", K(ret), K(path));
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/piece_file_info
int ObArchivePathUtil::get_piece_info_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id));
  } else if (OB_FAIL(path.join("piece_file_info"))) {
    LOG_WARN("failed to join piece info file", K(ret), K(path));
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/tenant_archive_piece_infos
int ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_piece_dir_path(dest, dest_id, round_id, piece_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id));
  } else if (OB_FAIL(path.join("tenant_archive_piece_infos"))) {
    LOG_WARN("failed to join tenant_archive_piece_infos file", K(ret), K(path));
  }
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[%ld]/[file_id]
int ObArchivePathUtil::get_ls_archive_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const share::ObLSID &ls_id, const int64_t file_id, ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_piece_ls_dir_path(dest, dest_id, round_id, piece_id, ls_id, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(round_id), K(dest_id), K(piece_id), K(ls_id), K(file_id));
  } else if (OB_FAIL(path.join(file_id))) {
    LOG_WARN("failed to join file id", K(ret), K(path), K(file_id));
  }
  return ret;
}
