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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_PATH_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_PATH_H_

#include <time.h>
#include "ob_archive_struct.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase
{
namespace share
{

class ObArchivePathUtil
{
public:
  // oss://archive/[dest_id][round_id][piece_id]/
  static int build_restore_prefix(const char *base,
      const share::ObLSID &ls_id,
      share::ObBackupPath &prefix);
  // oss://archive/[dest_id][round_id][piece_id]/
  static int build_restore_path(const char *base,
      const share::ObLSID &ls_id,
      const int64_t file_id,
      share::ObBackupPath &path);

  // oss://archive/rounds
  static int get_rounds_dir_path(const ObBackupDest &dest, ObBackupPath &rounds_path);

  // oss://archive/rounds/round_d[dest_id]r[round_id]_start.obarc
  static int get_round_start_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, ObBackupPath &path);

  // oss://archive/rounds/round_d[dest_id]r[round_id]_end.obarc
  static int get_round_end_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, ObBackupPath &path);

  // oss://archive/pieces
  static int get_pieces_dir_path(const ObBackupDest &dest, ObBackupPath &pieces_path);

  // oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_start_20220601T120000.obarc
  static int get_piece_start_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, const SCN &start_scn,
      ObBackupPath &path);

  // oss://archive/pieces/piece_d[dest_id]r[round_id]p[piece_id]_end_20220601T120000.obarc
  static int get_piece_end_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, const SCN &end_scn,
      ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]
  static int get_piece_dir_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/single_piece_info.obarc
  static int get_single_piece_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, ObBackupPath &path);
  // oss://[user_specified_path]/single_piece_info.obarc
  static int get_single_piece_file_info_path(const ObBackupDest &dest, ObBackupPath &path);
  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/checkpoint
  static int get_piece_checkpoint_dir_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, ObBackupPath &path);

  // oss://archive/checkpoint, or oss://[user_specified_path]/checkpoint
  static int get_piece_checkpoint_dir_path(const ObBackupDest &dest, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/checkpoint/checkpoint_info.[file_id].obarc
  static int get_piece_checkpoint_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, const int64_t file_id, ObBackupPath &path);

  // oss://[user_specified_path]/checkpoint/checkpoint_info.[file_id].obarc
  static int get_piece_checkpoint_file_path(const ObBackupDest &dest, const int64_t file_id, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/piece_d[dest_id]r[round_id]p[piece_id]_20220601T120000_20220602T120000.obarc
  static int get_piece_inner_placeholder_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, const SCN &start_scn,
      const SCN &end_scn, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[ls_id]/log/
  static int get_piece_ls_log_dir_path(const ObBackupDest &dest, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[ls_id]/file_info.obarc
  static int get_ls_file_info_path(const ObBackupDest &dest, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObBackupPath &path);

  // oss://[user_specified_path]/logstream_[ls_id]/file_info.obarc
  static int get_ls_file_info_path(const ObBackupDest &dest, const ObLSID &ls_id, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/file_info.obarc
  static int get_piece_info_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, ObBackupPath &path);
  
  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/tenant_archive_piece_infos.obarc
  static int get_tenant_archive_piece_infos_file_path(const ObBackupDest &dest, const int64_t dest_id, 
      const int64_t round_id, const int64_t piece_id, ObBackupPath &path);

  static int get_tenant_archive_piece_infos_file_path(const ObBackupDest &dest, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[%ld]/log/[file_id].obarc
  static int get_ls_archive_file_path(const ObBackupDest &dest, const int64_t dest_id, 
    const int64_t round_id, const int64_t piece_id, const share::ObLSID &ls_id, const int64_t file_id, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[%ld]/"meta_type"/
  static int get_ls_meta_record_prefix(const ObBackupDest &dest, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const share::ObLSID &ls_id,
      const ObArchiveLSMetaType &meta_type, ObBackupPath &path);

  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[%ld]/"meta_type"/[file_id]
  static int get_ls_meta_record_path(const ObBackupDest &dest, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const share::ObLSID &ls_id,
      const ObArchiveLSMetaType &meta_type, const int64_t file_id, ObBackupPath &path);

private:
  // oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/logstream_[ls_id]/
  static int get_piece_ls_dir_path_(const ObBackupDest &dest, const int64_t dest_id,
      const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id, ObBackupPath &path);

private:

  static const char * const ROUNDS_DIR_PATH;
  static const char * const PIECES_DIR_PATH;

  DISALLOW_COPY_AND_ASSIGN(ObArchivePathUtil);
};

}
}

#endif
