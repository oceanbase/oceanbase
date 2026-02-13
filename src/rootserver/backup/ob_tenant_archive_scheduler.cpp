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

#define USING_LOG_PREFIX ARCHIVE
#include "rootserver/backup/ob_tenant_archive_scheduler.h"
#include "storage/tx/ob_ts_mgr.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/ls/ob_ls_operator.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "logservice/archiveservice/ob_archive_file_utils.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

using namespace oceanbase;
using namespace rootserver;
using namespace common;
using namespace share;
using namespace backup;

static int record_piece_start(const ObTenantArchiveRoundAttr &old_round_info, const ObTenantArchivePieceAttr &piece_info, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!(piece_info.key_.piece_id_ > old_round_info.used_piece_id_
    || (piece_info.key_.piece_id_ == old_round_info.base_piece_id_ 
        && old_round_info.start_scn_ == old_round_info.checkpoint_scn_))) {
  } else {
    // persist piece start placeholder
    bool is_exist = false;
    ObPieceStartDesc piece_start_desc;
    piece_start_desc.dest_id_ = piece_info.key_.dest_id_;
    piece_start_desc.round_id_ = piece_info.key_.round_id_;
    piece_start_desc.piece_id_ = piece_info.key_.piece_id_;
    piece_start_desc.start_scn_ = piece_info.start_scn_;
    if (OB_FAIL(store.is_piece_start_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.start_scn_, is_exist))) {
      LOG_WARN("failed to check piece start info file exist", K(ret), K(old_round_info), K(piece_info), K(piece_start_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_piece_start(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.start_scn_, piece_start_desc))) {
      LOG_WARN("failed to write piece start info file", K(ret), K(old_round_info), K(piece_info), K(piece_start_desc));
    }
  }
  return ret;
}

static int record_piece_extend_info(
    common::ObISQLClient &sql_proxy,
    const ObTenantArchiveRoundAttr &old_round_info,
    const share::ObDestRoundCheckpointer::Result &result, 
    const ObTenantArchivePieceAttr &piece_info,
    const ObArchiveStore &store)
{

  int ret = OB_SUCCESS;
  ObTenantArchivePieceInfosDesc piece_extend_desc;
  ObArchivePersistHelper archive_table_op;
  int64_t max_available_piece_id = 0;
  // Record when inte piece start.
  if (ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE != piece_info.file_status_ && !piece_info.status_.is_frozen()) {
    // Do not persist to piece with file status 'INCOMPLETE' for ACTIVE piece. 
  } else if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(old_round_info.start_scn_, old_round_info.base_piece_id_, 
    old_round_info.piece_switch_interval_, old_round_info.checkpoint_scn_, max_available_piece_id))) {
    LOG_WARN("failed to calc max available piece id", K(ret), K(old_round_info));
  } else if (!(piece_info.key_.piece_id_ > max_available_piece_id 
    || (piece_info.key_.piece_id_ == old_round_info.base_piece_id_ 
       && old_round_info.start_scn_ == old_round_info.checkpoint_scn_))) {
    // Only newly created pieces(include first piece) need do persist.
  } else if (OB_FAIL(archive_table_op.init(piece_info.key_.tenant_id_))) {
    LOG_WARN("failed to init archive table operator", K(ret), K(piece_info));
  } else if (OB_FAIL(archive_table_op.get_frozen_pieces(sql_proxy, 
    piece_info.key_.dest_id_, piece_info.key_.piece_id_, piece_extend_desc.his_frozen_pieces_))) {
    LOG_WARN("failed to get frozen pieces", K(ret), K(piece_info));
  } else {
    // Maybe some frozen pieces are newly created, and have not been persisted.
    for (int64_t i = 0; OB_SUCC(ret) && i < result.piece_list_.count(); i++) {
      const ObDestRoundCheckpointer::GeneratedPiece &gen_piece = result.piece_list_.at(i);
      if (gen_piece.piece_info_.key_.piece_id_ >= piece_info.key_.piece_id_) {
        break;
      }

      if (!gen_piece.piece_info_.status_.is_frozen()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece is not frozen", K(ret), K(result), K(i));
        break;
      }

      if (OB_FAIL(piece_extend_desc.his_frozen_pieces_.push_back(gen_piece.piece_info_))) {
        LOG_WARN("failed to get push backup piece", K(ret), K(result), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      bool is_exist = false;
      piece_extend_desc.tenant_id_ = piece_info.key_.tenant_id_;
      piece_extend_desc.dest_id_ = piece_info.key_.dest_id_;
      piece_extend_desc.round_id_ = piece_info.key_.round_id_;
      piece_extend_desc.piece_id_ = piece_info.key_.piece_id_;
      piece_extend_desc.incarnation_ = piece_info.incarnation_;
      piece_extend_desc.dest_no_ = piece_info.dest_no_;
      piece_extend_desc.compatible_ = piece_info.compatible_;
      piece_extend_desc.start_scn_ = piece_info.start_scn_;
      piece_extend_desc.end_scn_ = piece_info.end_scn_;
      piece_extend_desc.path_ = piece_info.path_;
      if (OB_FAIL(store.is_tenant_archive_piece_infos_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, is_exist))) {
        LOG_WARN("failed to check piece extend info file exist", K(ret), K(piece_info), K(piece_extend_desc));
      } else if (is_exist) {
      } else if (OB_FAIL(store.write_tenant_archive_piece_infos(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_extend_desc))) {
        LOG_WARN("failed to write piece extend info file", K(ret), K(piece_info), K(piece_extend_desc));
      }
    }
  }

  return ret;
}

static int record_piece_checkpoint(const ObTenantArchiveRoundAttr &old_round_info, const ObTenantArchivePieceAttr &piece_info, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!(piece_info.status_.is_active() 
    && ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE == piece_info.file_status_)) {
  } else {
    // persist piece checkpoint
    ObPieceCheckpointDesc checkpoint_desc;
    checkpoint_desc.tenant_id_ = piece_info.key_.tenant_id_;
    checkpoint_desc.dest_id_ = piece_info.key_.dest_id_;
    checkpoint_desc.round_id_ = piece_info.key_.round_id_;
    checkpoint_desc.piece_id_ = piece_info.key_.piece_id_;
    checkpoint_desc.incarnation_ = piece_info.incarnation_;
    checkpoint_desc.compatible_ = piece_info.compatible_;
    checkpoint_desc.start_scn_ = piece_info.start_scn_;
    checkpoint_desc.checkpoint_scn_ = piece_info.checkpoint_scn_;
    checkpoint_desc.max_scn_ = piece_info.max_scn_;
    checkpoint_desc.end_scn_ = piece_info.end_scn_;
    if (OB_FAIL(store.write_piece_checkpoint(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, 0, old_round_info.checkpoint_scn_, checkpoint_desc))) {
      LOG_WARN("failed to write piece checkpoint info file", K(ret), K(piece_info), K(checkpoint_desc));
    }
  }
  return ret;
}

static int add_ls_meta_recorder_file_file_list(
    const ObSingleLSInfoDesc &single_ls_desc,
    const ObArchiveStore &store,
    const ObBackupPath &shema_meta_dir,
    const ObArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (!single_ls_desc.is_valid() || !store.is_init()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_ls_desc), K(store));
  } else {
    bool is_exist = false;
    if (OB_FAIL(store.is_file_list_file_exist(shema_meta_dir, ObBackupFileSuffix::ARCHIVE, is_exist))) {
      LOG_WARN("failed to check file list file exist", KR(ret), K(shema_meta_dir));
    } else if (is_exist) {
      //do nothing
    } else {
      ObBackupPath file_path;
      const ObBackupDest &dest = store.get_backup_dest();
      ObBackupFileListInfo file_list_info;
      const ObBackupStorageInfo *storage_info = store.get_storage_info();
      const int64_t dest_id = storage_info->get_dest_id();
      ObBackupPath file_list_path;
      share::ObArchiveLSMetaType meta_type(share::ObArchiveLSMetaType::Type::SCHEMA_META);
      for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
        const int64_t file_id = array.at(i);
        file_path.reset();
        if (OB_FAIL(share::ObArchivePathUtil::get_ls_meta_record_path(dest, single_ls_desc.dest_id_,
                                                  single_ls_desc.round_id_, single_ls_desc.piece_id_,
                                                  single_ls_desc.ls_id_, meta_type, file_id, file_path))) {
          LOG_WARN("failed to get ls meta record path", KR(ret), K(dest), K(single_ls_desc), K(file_id));
        } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(file_path,
                                                                                      *storage_info, file_list_info))) {
          LOG_WARN("failed to push file info", KR(ret), K(file_path), KP(storage_info));
        }
      }
      if (OB_FAIL(ret) || file_list_info.is_empty()) {
      } else if (OB_FAIL(ObBackupFileListWriterUtil::write_file_list_to_path(storage_info, ObBackupFileSuffix::ARCHIVE,
                                                                            shema_meta_dir, dest_id, file_list_info))) {
        LOG_WARN("failed to write file list", KR(ret), K(shema_meta_dir), K(file_list_info), KP(storage_info), K(dest_id));
      }
    }
  }
  return ret;
}

static int add_ls_meta_recorder_to_file_list(
  const ObSingleLSInfoDesc &single_ls_desc,
  const ObArchiveStore &store,
  ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  if (!single_ls_desc.is_valid() || !store.is_init()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_ls_desc), K(store));
  } else {
    ObBackupPath shema_meta_prefix;
    const ObBackupDest &dest = store.get_backup_dest();
    ObArray<int64_t> array;
    const ObArchiveLSMetaType meta_type(ObArchiveLSMetaType::Type::SCHEMA_META);
    if (OB_FAIL(ObArchivePathUtil::get_ls_meta_record_prefix(dest, single_ls_desc.dest_id_,
                                                  single_ls_desc.round_id_, single_ls_desc.piece_id_,
                                                  single_ls_desc.ls_id_, meta_type, shema_meta_prefix))) {
      LOG_WARN("failed to get ls meta record prefix", KR(ret), K(single_ls_desc));
    } else if (OB_FAIL(archive::ObArchiveFileUtils::list_files(shema_meta_prefix.get_obstr(),
                            store.get_storage_info(), array))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to list ls meta record files", KR(ret), K(shema_meta_prefix));
      }
    } else if (OB_FAIL(add_ls_meta_recorder_file_file_list(single_ls_desc, store, shema_meta_prefix, array))) {
      LOG_WARN("failed to add ls meta recorder file file list", KR(ret), K(single_ls_desc), K(shema_meta_prefix), K(array));
    } else if (OB_FAIL(file_list_info.push_dir_info(shema_meta_prefix))) {
      LOG_WARN("failed to push dir name", KR(ret), K(shema_meta_prefix));
    }
  }
  return ret;
}

static int record_piece_ls_file_list(
    const ObSingleLSInfoDesc &single_ls_desc,
    const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!single_ls_desc.is_valid() || !store.is_init()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_ls_desc), K(store));
  } else {
    const ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
    ObBackupPath dir_path;
    ObBackupPath ls_log_dir_path;
    const ObBackupDest &dest = store.get_backup_dest();
    ObBackupFileListInfo file_list_info;
    const int64_t file_count = single_ls_desc.filelist_.count();
    const ObBackupStorageInfo *storage_info = store.get_storage_info();
    const int64_t dest_id = storage_info->get_dest_id();
    const ObLSID &ls_id = single_ls_desc.ls_id_;
    bool is_exist = false;
    if (OB_FAIL(ObArchivePathUtil::get_piece_ls_dir_path(dest, single_ls_desc.dest_id_, single_ls_desc.round_id_,
                                                            single_ls_desc.piece_id_, ls_id, dir_path))) {
      LOG_WARN("failed to get piece ls dir path", KR(ret), K(dest), K(single_ls_desc));
    } else if (OB_FAIL(store.is_file_list_file_exist(dir_path, suffix, is_exist))) {
      LOG_WARN("failed to check file list file exist", KR(ret), K(dir_path));
    } else if (is_exist) {
    } else if (0 != file_count && OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(dest, single_ls_desc.dest_id_,
                                                  single_ls_desc.round_id_, single_ls_desc.piece_id_,
                                                  single_ls_desc.ls_id_, ls_log_dir_path))) {
      LOG_WARN("failed to get ls log dir path", KR(ret), K(dest), K(single_ls_desc));
    } else if (0 != file_count && OB_FAIL(file_list_info.push_dir_info(ls_log_dir_path))) {
      LOG_WARN("failed to push ls log dir to list", KR(ret), K(ls_log_dir_path));
    } else if (ls_id.is_sys_ls() && OB_FAIL(add_ls_meta_recorder_to_file_list(single_ls_desc, store, file_list_info))) {
      LOG_WARN("failed to add ls meta recorder to file list", KR(ret), K(single_ls_desc));
    } else {
      ObBackupPath file_info_path;
      if (OB_FAIL(ObArchivePathUtil::get_ls_file_info_path(dest, single_ls_desc.dest_id_,
                                                  single_ls_desc.round_id_, single_ls_desc.piece_id_,
                                                  single_ls_desc.ls_id_, file_info_path))) {
        LOG_WARN("failed to get ls file info path", KR(ret), K(dest), K(single_ls_desc));
      } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(file_info_path,
                                                                                    single_ls_desc, file_list_info))) {
        LOG_WARN("failed to add file to list", KR(ret), K(file_info_path), K(single_ls_desc), K(file_list_info));
      } else if (OB_FAIL(ObBackupFileListWriterUtil::write_file_list_to_path(storage_info, suffix,
                                                                            dir_path, dest_id, file_list_info))) {
        LOG_WARN("failed to write file list", KR(ret), K(dir_path), K(file_list_info));
      }
    }
  }
  return ret;
}

static int record_piece_ls_log_file_list(
    const ObSingleLSInfoDesc &single_ls_desc, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!single_ls_desc.is_valid() || !store.is_init()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_ls_desc), K(store));
  } else {
    bool is_exist = false;
    ObBackupPath ls_log_dir_path;
    const ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
    const ObBackupDest &dest = store.get_backup_dest();
    const ObBackupStorageInfo *storage_info = store.get_storage_info();
    const int64_t dest_id = storage_info->get_dest_id();
    ObBackupFileListInfo file_list_info;
    ObBackupFileInfo file_info;
    ObBackupPath file_path;
    ObBackupPathString file_name;
    ObBackupBlockFileItemWriter writer;
    ObBackupBlockFileAddr entry_block_addr;
    int64_t total_item_count = 0;
    int64_t total_block_count = 0;
    const int64_t max_file_size = DEFAULT_BACKUP_DATA_FILE_SIZE;
    const ObBackupBlockFileDataType data_type = ObBackupBlockFileDataType::FILE_PATH_INFO;
    const int64_t file_count = single_ls_desc.filelist_.count();
    if (OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(dest, single_ls_desc.dest_id_,
                                                  single_ls_desc.round_id_, single_ls_desc.piece_id_,
                                                  single_ls_desc.ls_id_, ls_log_dir_path))) {
      LOG_WARN("failed to get ls log dir path", KR(ret), K(dest), K(single_ls_desc));
    } else if (OB_FAIL(store.is_file_list_file_exist(ls_log_dir_path, suffix, is_exist))) {
      LOG_WARN("failed to check file list file exist", KR(ret), K(ls_log_dir_path));
    } else if (is_exist || 0 == file_count) {
    } else if (OB_FAIL(writer.init(storage_info, data_type, suffix, ls_log_dir_path, dest_id, max_file_size))) {
      LOG_WARN("failed to init writer", KR(ret), K(storage_info), K(suffix), K(ls_log_dir_path), K(dest_id), K(max_file_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_count; i++) {
        const ObSingleLSInfoDesc::OneFile &file = single_ls_desc.filelist_.at(i);
        file_path.reset();
        file_info.reset();
        if (OB_FAIL(share::ObArchivePathUtil::get_ls_archive_file_path(dest, single_ls_desc.dest_id_,
            single_ls_desc.round_id_, single_ls_desc.piece_id_, single_ls_desc.ls_id_, file.file_id_, file_path))) {
          LOG_WARN("failed to get ls archive file path", KR(ret), K(dest), K(single_ls_desc), K(file));
        } else if (OB_FAIL(file_info.set_file_info(file_path, file.size_bytes_))) {
          LOG_WARN("failed to set file info", KR(ret), K(file_path), K(file.size_bytes_));
        } else if (OB_FAIL(writer.write_item(file_info))) {
          LOG_WARN("failed to write item", KR(ret), K(file_info));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(writer.close(entry_block_addr, total_item_count, total_block_count))) {
        LOG_WARN("failed to close writer", KR(ret));
      }
    }
  }
  return ret;
}

static int record_piece_info(const ObDestRoundCheckpointer::GeneratedPiece &piece,
                                  const ObArchiveStore &store, ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  const ObTenantArchivePieceAttr &piece_info = piece.piece_info_;
  if (!piece_info.status_.is_frozen()) {
  } else {
    bool is_exist = false;
    ObBackupPath piece_file_info_path;
    const ObBackupDest &dest = store.get_backup_dest();
    // persist piece ls info & piece info
    ObPieceInfoDesc piece_info_desc;
    for (int64_t i = 0; OB_SUCC(ret) && i < piece.ls_piece_list_.count(); i++) {
      const ObDestRoundCheckpointer::GeneratedLSPiece &ls_piece = piece.ls_piece_list_.at(i);
      ObSingleLSInfoDesc single_ls_desc;
      single_ls_desc.dest_id_ = piece_info.key_.dest_id_;
      single_ls_desc.round_id_ = piece_info.key_.round_id_;
      single_ls_desc.piece_id_ = piece_info.key_.piece_id_;
      single_ls_desc.ls_id_ = ls_piece.ls_id_;
      single_ls_desc.start_scn_ = ls_piece.start_scn_;
      single_ls_desc.checkpoint_scn_ = ls_piece.checkpoint_scn_;
      single_ls_desc.min_lsn_ = ls_piece.min_lsn_;
      single_ls_desc.max_lsn_ = ls_piece.max_lsn_;
      single_ls_desc.deleted_ = ls_piece.is_ls_deleted_;
      if (OB_FAIL(ret)) {
      } else if (ls_piece.max_lsn_ > ls_piece.min_lsn_
        && OB_FAIL(store.get_file_list_in_piece(single_ls_desc.dest_id_, 
        single_ls_desc.round_id_, single_ls_desc.piece_id_, single_ls_desc.ls_id_,
        single_ls_desc.filelist_))) {
        LOG_WARN("failed to get archive file list", K(ret), K(single_ls_desc));
      } else if (OB_FALSE_IT(lib::ob_sort(single_ls_desc.filelist_.begin(), single_ls_desc.filelist_.end()))) {
      } else if (OB_FAIL(piece_info_desc.filelist_.push_back(single_ls_desc))) {
        LOG_WARN("failed to push backup single_ls_desc", K(ret), K(single_ls_desc), K(piece_info_desc));
      } else if (OB_FAIL(store.is_single_ls_info_file_exist(single_ls_desc.dest_id_,
                              single_ls_desc.round_id_, single_ls_desc.piece_id_, single_ls_desc.ls_id_, is_exist))) {
        LOG_WARN("failed to check single ls info file exist", K(ret), K(piece), K(single_ls_desc));
      } else if (is_exist) {
      } else {
        const int64_t file_count = single_ls_desc.filelist_.count();
        if (file_count > 0) {
          const int64_t max_file_id = single_ls_desc.filelist_.at(file_count-1).file_id_;
          if (OB_FAIL(store.seal_file(single_ls_desc.dest_id_,
                single_ls_desc.round_id_, single_ls_desc.piece_id_, single_ls_desc.ls_id_, max_file_id))) {
            LOG_WARN("failed to seal last file", K(ret), K(single_ls_desc), K(max_file_id));
          }
        }
        if (FAILEDx(store.write_single_ls_info(single_ls_desc.dest_id_, single_ls_desc.round_id_,
          single_ls_desc.piece_id_, single_ls_desc.ls_id_, single_ls_desc))) {
          LOG_WARN("failed to write single ls info file", K(ret), K(piece), K(single_ls_desc));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(record_piece_ls_log_file_list(single_ls_desc, store))) {
        LOG_WARN("failed to record piece ls file list", KR(ret), K(piece), K(single_ls_desc));
      } else if (OB_FAIL(record_piece_ls_file_list(single_ls_desc, store))) {
        LOG_WARN("failed to record piece ls file list", KR(ret), K(piece), K(single_ls_desc));
      }
    }

    piece_info_desc.dest_id_ = piece_info.key_.dest_id_;
    piece_info_desc.round_id_ = piece_info.key_.round_id_;
    piece_info_desc.piece_id_ = piece_info.key_.piece_id_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_info_file_path(dest, piece_info.key_.dest_id_,
                          piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_file_info_path))) {
      LOG_WARN("failed to get piece file info path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(piece_file_info_path,
                                                                                  piece_info_desc, file_list_info))) {
      LOG_WARN("failed to add file to list", K(ret), K(piece_file_info_path), K(piece_info_desc), K(file_list_info));
    } else if (OB_FAIL(store.is_piece_info_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                                          piece_info.key_.piece_id_, is_exist))) {
      LOG_WARN("failed to check piece info file exist", K(ret), K(piece), K(piece_info_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_piece_info(piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                                          piece_info.key_.piece_id_, piece_info_desc))) {
      LOG_WARN("failed to write piece info file", K(ret), K(piece), K(piece_info_desc));
    }
  }
  return ret;
}

static int record_piece_inner_placeholder(const ObTenantArchivePieceAttr &piece_info,
                                          const ObArchiveStore &store, ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  if (!piece_info.status_.is_frozen()) {
  } else {
    bool is_exist = false;
    ObBackupPath inner_placeholder_path;
    const ObBackupDest &dest = store.get_backup_dest();
    ObPieceInnerPlaceholderDesc inner_placeholder_desc;
    inner_placeholder_desc.dest_id_ = piece_info.key_.dest_id_;
    inner_placeholder_desc.round_id_ = piece_info.key_.round_id_;
    inner_placeholder_desc.piece_id_ = piece_info.key_.piece_id_;
    inner_placeholder_desc.start_scn_ = piece_info.start_scn_;
    inner_placeholder_desc.checkpoint_scn_ = piece_info.checkpoint_scn_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_inner_placeholder_file_path(dest, piece_info.key_.dest_id_,
                                        piece_info.key_.round_id_, piece_info.key_.piece_id_,
                                        piece_info.start_scn_, piece_info.checkpoint_scn_, inner_placeholder_path))) {
      LOG_WARN("failed to get piece inner placeholder file path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(inner_placeholder_path,
                                                                            inner_placeholder_desc, file_list_info))) {
      LOG_WARN("failed to add file to list", K(ret), K(inner_placeholder_path),
                    K(inner_placeholder_desc), K(file_list_info));
    } else if (OB_FAIL(store.is_piece_inner_placeholder_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.start_scn_, piece_info.checkpoint_scn_, is_exist))) {
      LOG_WARN("failed to check piece inner placeholder file exist", K(ret), K(piece_info), K(inner_placeholder_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_piece_inner_placeholder(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.start_scn_, piece_info.checkpoint_scn_, inner_placeholder_desc))) {
      LOG_WARN("failed to write piece inner placeholder info file", K(ret), K(piece_info), K(inner_placeholder_desc));
    }
  }
  return ret;
}

static int record_single_piece_file_list(
    const ObDestRoundCheckpointer::GeneratedPiece &piece,
    const ObTenantArchivePieceAttr &piece_info,
    const ObArchiveStore &store,
    ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  if (!piece_info.status_.is_frozen()) {
  } else {
    ObBackupPath checkpoint_dir_path;
    ObBackupPath piece_path;
    const ObBackupDest &dest = store.get_backup_dest();
    const ObBackupStorageInfo *storage_info = store.get_storage_info();
    const int64_t dest_id = storage_info->get_dest_id();
    ObBackupPath tenant_piece_infos_path;
    bool is_exist = false;
    const ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
    if (OB_FAIL(share::ObArchivePathUtil::get_piece_dir_path(dest, piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                                                piece_info.key_.piece_id_, piece_path))) {
      LOG_WARN("failed to get piece dir path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(store.is_file_list_file_exist(piece_path, suffix, is_exist))) {
      LOG_WARN("failed to check file list file exist", K(ret), K(piece_path));
    } else if (is_exist) {
    } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_checkpoint_dir_path(dest,
        piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, checkpoint_dir_path))) {
      LOG_WARN("failed to get piece checkpoint dir path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(store.is_file_list_file_exist(checkpoint_dir_path, suffix, is_exist))) {
      LOG_WARN("failed to check file list file exist", K(ret), K(checkpoint_dir_path));
    } else if (is_exist && OB_FAIL(file_list_info.push_dir_info(checkpoint_dir_path))) {
      LOG_WARN("failed to push checkpoint dir info", K(ret), K(checkpoint_dir_path));
    } else if (OB_FAIL(share::ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(dest,
                                      piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                      piece_info.key_.piece_id_, tenant_piece_infos_path))) {
      LOG_WARN("failed to get tenant archive piece infos file path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(tenant_piece_infos_path,
                                                                                  *storage_info, file_list_info))) {
      LOG_WARN("failed to add file to list", K(ret), K(file_list_info), K(tenant_piece_infos_path));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < piece.ls_piece_list_.count(); i++) {
        const ObDestRoundCheckpointer::GeneratedLSPiece &ls_piece = piece.ls_piece_list_.at(i);
        ObBackupPath ls_dir_path;
        if (OB_FAIL(share::ObArchivePathUtil::get_piece_ls_dir_path(dest,
            piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_,
            ls_piece.ls_id_, ls_dir_path))) {
          LOG_WARN("failed to get piece ls dir path", K(ret), K(dest), K(piece_info), K(ls_piece.ls_id_));
        } else if (OB_FAIL(file_list_info.push_dir_info(ls_dir_path))) {
          LOG_WARN("failed to push ls dir info", K(ret), K(ls_dir_path));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBackupFileListWriterUtil::write_file_list_to_path(storage_info, suffix,
                                                                            piece_path, dest_id, file_list_info))) {
        LOG_WARN("failed to write file list", K(ret), K(piece_path), K(file_list_info));
      }
    }
  }
  return ret;
}

static int record_checkpoint_dir_file_list(
  const ObTenantArchivePieceAttr &piece_info,
  const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!piece_info.status_.is_frozen()) {
  } else {
    const ObBackupDest &dest = store.get_backup_dest();
    const ObBackupStorageInfo *storage_info = store.get_storage_info();
    const int64_t dest_id = storage_info->get_dest_id();
    const int64_t min_file_id = 0;
    uint64_t checkpoint_scn = 0;
    ObBackupPath checkpoint_file_path;
    ObBackupPath checkpoint_dir_path;
    ObBackupFileListInfo file_list_info;
    ObPieceCheckpointDesc checkpoint_desc;
    const ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
    bool is_exist = false;
    if (OB_FAIL(share::ObArchivePathUtil::get_piece_checkpoint_dir_path(dest,
        piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, checkpoint_dir_path))) {
      LOG_WARN("failed to get piece checkpoint dir path", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(store.is_file_list_file_exist(checkpoint_dir_path, suffix, is_exist))) {
      LOG_WARN("failed to check file list file exist", K(ret), K(checkpoint_dir_path));
    } else if (is_exist) {
    } else if (OB_FAIL(checkpoint_file_path.init(checkpoint_dir_path.get_ptr()))) {
      LOG_WARN("failed to init checkpoint file path", K(ret));
    } else if (OB_FAIL(checkpoint_file_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME,
                                                                          min_file_id, ObBackupFileSuffix::ARCHIVE))) {
      LOG_WARN("failed to get piece checkpoint file path for min file id", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(store.is_piece_checkpoint_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                                            piece_info.key_.piece_id_, min_file_id, is_exist))) {
      LOG_WARN("failed to check checkpoint file exist", K(ret), K(piece_info));
    } else if (!is_exist) {
    // Some test environments set the PIECE_SWITCH_INTERVAL time to a shorter time,
    // causing the piece status to become frozen without executing checkpoint.
    // In this case, the checkpoint file does not exist.
    // The checkpoint file is only dependent on the piece when it is active.
    // This function only executes when the piece is frozen,
    // so the absence of the checkpoint file does not affect the validity of the piece.
    } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(checkpoint_file_path,
                                                                          *storage_info, file_list_info))) {
      LOG_WARN("failed to add file to list", K(ret), K(file_list_info), K(checkpoint_file_path));
    } else if (FALSE_IT(checkpoint_file_path.reset())) {
    } else if (OB_FAIL(store.read_piece_checkpoint(piece_info.key_.dest_id_, piece_info.key_.round_id_,
                                                            piece_info.key_.piece_id_, min_file_id, checkpoint_desc))) {
      LOG_WARN("failed to read piece checkpoint", K(ret), K(piece_info));
    } else if (FALSE_IT(checkpoint_scn = checkpoint_desc.checkpoint_scn_.get_val_for_inner_table_field())) {
    } else if (OB_FAIL(checkpoint_file_path.init(checkpoint_dir_path.get_ptr()))) {
      LOG_WARN("failed to init max checkpoint file path", K(ret), K(checkpoint_dir_path), K(piece_info));
    } else if (OB_FAIL(checkpoint_file_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME, checkpoint_scn,
                                                                            ObBackupFileSuffix::ARCHIVE))) {
      LOG_WARN("failed to get piece checkpoint file path for max file id", K(ret), K(dest), K(piece_info));
    } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(checkpoint_file_path, *storage_info,
                                                                            file_list_info))) {
      LOG_WARN("failed to add file to list", K(ret), K(file_list_info), K(checkpoint_file_path));
    } else if (OB_FAIL(ObBackupFileListWriterUtil::write_file_list_to_path(storage_info, suffix, checkpoint_dir_path,
                                                                              dest_id, file_list_info))) {
      LOG_WARN("failed to write file list", K(ret), K(checkpoint_dir_path), K(file_list_info));
    }
  }
  return ret;
}

static int record_single_piece_info(const ObTenantArchivePieceAttr &piece_info,
                                    const ObArchiveStore &store, ObBackupFileListInfo &file_list_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_exist = false;
  ObBackupPath single_piece_info_path;
  const ObBackupDest &dest = store.get_backup_dest();
  ObSinglePieceDesc single_piece_desc;
  if (!piece_info.status_.is_frozen()) {
  } else if (OB_FAIL(share::ObArchivePathUtil::get_single_piece_file_path(dest, piece_info.key_.dest_id_,
                                    piece_info.key_.round_id_, piece_info.key_.piece_id_, single_piece_info_path))) {
    LOG_WARN("failed to get single piece info path", K(ret), K(dest), K(piece_info));
  } else if (OB_FAIL(single_piece_desc.piece_.assign(piece_info))) {
    LOG_WARN("failed to assign piece", K(ret));
  } else if (OB_FAIL(ObBackupFileListWriterUtil::add_file_to_file_list_info(single_piece_info_path, single_piece_desc,
                                                                                file_list_info))) {
    LOG_WARN("failed to add file to list", K(ret), K(single_piece_desc), K(single_piece_info_path));
  } else if (OB_FAIL(store.is_single_piece_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, is_exist))) {
    LOG_WARN("failed to check single piece info file exist", K(ret), K(piece_info), K(single_piece_desc));
  } else if (is_exist) {
  } else if (OB_FAIL(store.write_single_piece(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, single_piece_desc))) {
    LOG_WARN("failed to write single piece info file", K(ret), K(piece_info), K(single_piece_desc));
  }
  return ret;
}

static int record_piece_end(const ObTenantArchivePieceAttr &piece_info, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!piece_info.status_.is_frozen()) {
  } else {
    bool is_exist = false;
    ObPieceEndDesc piece_end_desc;
    piece_end_desc.dest_id_ = piece_info.key_.dest_id_;
    piece_end_desc.round_id_ = piece_info.key_.round_id_;
    piece_end_desc.piece_id_ = piece_info.key_.piece_id_;
    piece_end_desc.end_scn_ = piece_info.checkpoint_scn_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(store.is_piece_end_file_exist(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.checkpoint_scn_, is_exist))) {
      LOG_WARN("failed to check piece end file exist", K(ret), K(piece_info), K(piece_end_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_piece_end(piece_info.key_.dest_id_, piece_info.key_.round_id_, piece_info.key_.piece_id_, piece_info.checkpoint_scn_, piece_end_desc))) {
      LOG_WARN("failed to write piece end file", K(ret), K(piece_info), K(piece_end_desc));
    }
  }
  return ret;
}

static int record_round_start(const share::ObTenantArchiveRoundAttr &round_info, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!round_info.state_.is_beginning()) {
  } else {
    // persist round start placeholder
    bool is_exist = false;
    ObRoundStartDesc round_start_desc;
    round_start_desc.dest_id_ = round_info.dest_id_;
    round_start_desc.round_id_ = round_info.round_id_;
    round_start_desc.start_scn_ = round_info.start_scn_;
    round_start_desc.base_piece_id_ = round_info.base_piece_id_;
    round_start_desc.piece_switch_interval_ = round_info.piece_switch_interval_;
    if (OB_FAIL(store.is_round_start_file_exist(round_info.dest_id_, round_info.round_id_, is_exist))) {
      LOG_WARN("failed to check rounds start file exist", K(ret), K(round_info), K(round_start_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_round_start(round_info.dest_id_, round_info.round_id_, round_start_desc))) {
      LOG_WARN("failed to write round start file", K(ret), K(round_info), K(round_start_desc));
    }
  }

  return ret;
}

static int record_round_end(const share::ObTenantArchiveRoundAttr &round_info, const ObArchiveStore &store)
{
  int ret = OB_SUCCESS;
  if (!round_info.state_.is_stop()) {
  } else {
    // persist round end placeholder
    bool is_exist = false;
    ObRoundEndDesc round_end_desc;
    round_end_desc.dest_id_ = round_info.dest_id_;
    round_end_desc.round_id_ = round_info.round_id_;
    round_end_desc.start_scn_ = round_info.start_scn_;
    round_end_desc.checkpoint_scn_ = round_info.checkpoint_scn_;
    round_end_desc.base_piece_id_ = round_info.base_piece_id_;
    round_end_desc.piece_switch_interval_ = round_info.piece_switch_interval_;
    if (OB_FAIL(store.is_round_end_file_exist(round_info.dest_id_, round_info.round_id_, is_exist))) {
      LOG_WARN("failed to check rounds end file exist", K(ret), K(round_info), K(round_end_desc));
    } else if (is_exist) {
    } else if (OB_FAIL(store.write_round_end(round_info.dest_id_, round_info.round_id_, round_end_desc))) {
      LOG_WARN("failed to write round end file", K(ret), K(round_info), K(round_end_desc));
    }
  }

  return ret;
}

static int piece_generated_cb(
    common::ObISQLClient *sql_proxy, 
    const ObTenantArchiveRoundAttr &old_round_info, 
    const share::ObDestRoundCheckpointer::Result &result,
    const ObDestRoundCheckpointer::GeneratedPiece &piece)
{
  int ret = OB_SUCCESS;
  const ObTenantArchivePieceAttr &piece_info = piece.piece_info_;
  ObArchiveStore store;
  ObBackupDest dest;
  ObBackupFileListInfo file_list_info;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, piece_info.key_.tenant_id_, piece_info.path_, dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(old_round_info), K(piece)); 
  } else if (OB_FAIL(store.init(dest))) {
    LOG_WARN("failed to init archive store", K(ret), K(old_round_info), K(dest));
  } else if (OB_FAIL(record_piece_start(old_round_info, piece_info, store))) {
    LOG_WARN("failed to record piece start", K(ret), K(old_round_info), K(piece));
  } else if (OB_FAIL(record_piece_extend_info(*sql_proxy, old_round_info, result, piece.piece_info_, store))) {
    LOG_WARN("failed to record piece extend info", K(ret), K(old_round_info), K(piece));
  } else if (OB_FAIL(record_piece_checkpoint(old_round_info, piece_info, store))) {
    LOG_WARN("failed to record piece checkpoint", K(ret), K(old_round_info), K(piece));
  } else if (OB_FAIL(record_piece_info(piece, store, file_list_info))) {
    LOG_WARN("failed to record piece info", K(ret), K(old_round_info), K(piece), K(file_list_info));
  } else if (OB_FAIL(record_piece_inner_placeholder(piece_info, store, file_list_info))) {
    LOG_WARN("failed to record piece inner placeholder", K(ret), K(old_round_info), K(piece), K(file_list_info));
  } else if (OB_FAIL(record_single_piece_info(piece_info, store, file_list_info))) {
    LOG_WARN("failed to record single piece info", K(ret), K(old_round_info), K(piece));
  } else if (OB_FAIL(record_checkpoint_dir_file_list(piece_info, store))) {
    LOG_WARN("failed to record checkpoint dir file list", K(ret), K(piece_info), K(old_round_info), K(file_list_info));
  } else if (OB_FAIL(record_single_piece_file_list(piece, piece_info, store, file_list_info))) {
    LOG_WARN("failed to record piece file list", K(ret), K(old_round_info), K(piece), K(file_list_info));
  } else if (OB_FAIL(record_piece_end(piece_info, store))) {
    LOG_WARN("failed to record piece end", K(ret), K(old_round_info), K(piece));
  }
  
  return ret;
}

static int round_checkpoint_cb(
    common::ObISQLClient *sql_proxy, 
    const ObTenantArchiveRoundAttr &old_round_info, 
    const ObTenantArchiveRoundAttr &new_round_info)
{
  int ret = OB_SUCCESS;
  // record round start
  ObArchiveStore store;
  ObBackupDest dest;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, new_round_info.key_.tenant_id_, new_round_info.path_, dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(old_round_info), K(new_round_info)); 
  } else if (OB_FAIL(store.init(dest))) {
    LOG_WARN("failed to init archive store", K(ret), K(old_round_info), K(dest));
  } else if (OB_FAIL(record_round_start(old_round_info, store))) {
    LOG_WARN("failed to record round start", K(ret), K(old_round_info), K(new_round_info));
  } else if (OB_FAIL(record_round_end(new_round_info, store))) {
    LOG_WARN("failed to record round start", K(ret), K(old_round_info), K(new_round_info));
  } 

  return ret;
}


/**
 * ------------------------------ObArchiveHandler---------------------
 */
ObArchiveHandler::ObArchiveHandler()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), rpc_proxy_(nullptr),
    sql_proxy_(nullptr), schema_service_(nullptr), round_handler_(),
    archive_table_op_()
{

}

int ObArchiveHandler::init(
    const uint64_t tenant_id,
    share::schema::ObMultiVersionSchemaService *schema_service,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("archive scheduler init twice", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service is null", K(ret), KP(schema_service));
  } else if (OB_FAIL(archive_table_op_.init(tenant_id))) {
    LOG_WARN("failed to init archive table operator", K(ret), K(tenant_id));
  } else if (OB_FAIL(round_handler_.init(tenant_id, OB_START_INCARNATION, sql_proxy))) {
    LOG_WARN("failed to init archive round", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    schema_service_ = schema_service;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }

  return ret;
}

int ObArchiveHandler::open_archive_mode()
{
  int ret = OB_SUCCESS;
  ObArchiveMode archive_mode;
  bool can = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if(OB_FAIL(check_can_do_archive(can))) {
    LOG_WARN("failed to check can do archive", K(ret));
  } else if (!can) {
    ret = OB_CANNOT_START_LOG_ARCHIVE_BACKUP;
    LOG_WARN("tenant can not do archive", K(ret), K_(tenant_id));
  } else if (OB_FAIL(archive_table_op_.get_archive_mode(*sql_proxy_, archive_mode))) {
    LOG_WARN("failed to get archive mode", K(ret), K_(tenant_id));
  } else if (!archive_mode.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive mode not valid", K(ret), K_(tenant_id), K(archive_mode));
  } else if (archive_mode.is_archivelog()) {
    ret = OB_ALREADY_IN_ARCHIVE_MODE;
    LOG_USER_ERROR(OB_ALREADY_IN_ARCHIVE_MODE);
    LOG_WARN("already in archive mode", K(ret), K_(tenant_id));
  } else if (OB_FAIL(archive_table_op_.open_archive_mode(*sql_proxy_))) {
    LOG_WARN("failed to open archive mode", K(ret), K_(tenant_id));
  } else {
    LOG_INFO("open archive mode", K_(tenant_id));
  }

  ROOTSERVICE_EVENT_ADD("log_archive", "open_archive_mode", "tenant_id", tenant_id_,
    "result", ret);

  return ret;
}

int ObArchiveHandler::close_archive_mode()
{
  int ret = OB_SUCCESS;
  ObArchiveMode archive_mode;
  bool has_tenant_snapshot = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if (OB_FAIL(archive_table_op_.get_archive_mode(*sql_proxy_, archive_mode))) {
    LOG_WARN("failed to get archive mode", K(ret), K_(tenant_id));
  } else if (!archive_mode.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive mode not valid", K(ret), K_(tenant_id), K(archive_mode));
  } else if (archive_mode.is_noarchivelog()) {
    ret = OB_ALREADY_IN_NOARCHIVE_MODE;
    LOG_USER_ERROR(OB_ALREADY_IN_NOARCHIVE_MODE);
    LOG_WARN("already in noarchive mode", K(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_has_snapshot(*sql_proxy_,
                                        tenant_id_, has_tenant_snapshot))) {
    LOG_WARN("failed to check whether tenant has snapshot", K(ret));
  } else if (has_tenant_snapshot) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not close log archive while tenant snapshots exist", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant snapshots exist, close log archive");
  } else if (OB_FAIL(archive_table_op_.close_archive_mode(*sql_proxy_))) {
    LOG_WARN("failed to close archive mode", K(ret), K_(tenant_id));
  } else {
    LOG_INFO("close archive mode", K_(tenant_id));
  }

  ROOTSERVICE_EVENT_ADD("log_archive", "close_archive_mode", "tenant_id", tenant_id_,
    "result", ret);

  return ret;
}

int ObArchiveHandler::check_archive_dest_validity_(const int64_t dest_no)
{ 
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  bool need_lock = false;
  ObBackupPathString dest_str;
  ObBackupDestMgr dest_mgr;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG;

  if (OB_FAIL(helper.init(tenant_id_))) {
    LOG_WARN("fail to init archive helper", K(ret), K(tenant_id_));
  } else if (OB_FAIL(helper.get_archive_dest(*sql_proxy_, need_lock, dest_no, dest_str))) {
    LOG_WARN("fail to get archive path", K(ret), K(tenant_id_));
  } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, dest_str,  *sql_proxy_))) {
    LOG_WARN("fail to init dest manager", K(ret), K(tenant_id_), K(dest_str));
  } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/, true/*need_check_permission*/))) {
    LOG_WARN("fail to check archive dest validity", K(ret), K(tenant_id_), K(dest_str));
  }

  return ret;
}

int ObArchiveHandler::check_can_do_archive(bool &can) const
{
  // Tenant which is restoring/dropping/dropped can not do archive.
  int ret = OB_SUCCESS;
  bool is_dropped;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id_, is_dropped))) {
    LOG_WARN("failed to check if tenant has been dropped", K(ret), K_(tenant_id));
  } else if (is_dropped) {
    can = false;
    LOG_WARN("tenant is dropped", K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    can = false;
    LOG_WARN("tenant schema is null, tenant may has been dropped", K(ret), K_(tenant_id));
  } else if (tenant_schema->is_normal()) {
    can = true;
  } else if (tenant_schema->is_creating()) {
    can = false;
    LOG_WARN("tenant is creating", K_(tenant_id));
  } else if (tenant_schema->is_restore()) {
    can = false;
    LOG_WARN("tenant is doing restore", K_(tenant_id));
  } else if (tenant_schema->is_dropping()) {
    can = false;
    LOG_WARN("tenant is dropping", K_(tenant_id));
  } else if (tenant_schema->is_in_recyclebin()) {
    can = false;
    LOG_WARN("tenant is in recyclebin", K_(tenant_id));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown tenant status", K_(tenant_id), K(tenant_schema));
  }
  return ret;
}

int ObArchiveHandler::enable_archive(const int64_t dest_no)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool can;
  bool create;
  ObTenantArchiveRoundAttr new_round_attr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if(OB_FAIL(check_can_do_archive(can))) {
    LOG_WARN("failed to check can do archive", K(ret));
  } else if (!can) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant can not do archive", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_archive_dest_validity_(dest_no))) {
    LOG_WARN("fail to check archive dest valivity", K(ret), K_(tenant_id), K(dest_no)); 
  } else if (OB_FAIL(round_handler_.enable_archive(dest_no, new_round_attr))) {
    LOG_WARN("failed to enable archive", K(ret), K_(tenant_id), K(dest_no));
  } else {
    if (new_round_attr.state_.status_ == ObArchiveRoundState::Status::BEGINNING) {
      if (OB_TMP_FAIL(notify_(new_round_attr))) {
        LOG_WARN("notify failed", K(tmp_ret), K(new_round_attr));
      }
    }
    LOG_INFO("enable archive", K(dest_no), K(new_round_attr));
  }

  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("log_archive", "enable_archive", "tenant_id", tenant_id_,
      "dest_no", dest_no, "round_id", new_round_attr.round_id_,
      "status", new_round_attr.state_.status_,
      "path", new_round_attr.path_.ptr());
  }

  return ret;
}

int ObArchiveHandler::disable_archive(const int64_t dest_no)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr new_round_attr;
  bool has_tenant_snapshot = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_has_snapshot(*sql_proxy_,
                                        tenant_id_, has_tenant_snapshot))) {
    LOG_WARN("failed to check whether tenant has snapshot", K(ret));
  } else if (has_tenant_snapshot) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not disable archive while tenant snapshots exist", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant snapshots exist, disable log archive");
  } else if (OB_FAIL(round_handler_.disable_archive(dest_no, new_round_attr))) {
    LOG_WARN("failed to disable archive", K(ret), K_(tenant_id), K(dest_no));
  } else {
    if (OB_TMP_FAIL(notify_(new_round_attr))) {
      LOG_WARN("notify failed", K(tmp_ret), K(new_round_attr));
    }
    LOG_INFO("disable archive", K(dest_no), K(new_round_attr));
  }

  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("log_archive", "disable_archive", "tenant_id", tenant_id_,
      "dest_no", dest_no, "round_id", new_round_attr.round_id_,
      "status", new_round_attr.state_.status_,
      "path", new_round_attr.path_.ptr());
  }

  return ret;
}

int ObArchiveHandler::defer_archive(const int64_t dest_no)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr new_round_attr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if (OB_FAIL(round_handler_.defer_archive(dest_no, new_round_attr))) {
    LOG_WARN("failed to defer archive", K(ret), K_(tenant_id), K(dest_no));
  } else {
    if (OB_TMP_FAIL(notify_(new_round_attr))) {
      LOG_WARN("notify failed", K(tmp_ret), K(new_round_attr));
    }
    LOG_INFO("defer archive", K(dest_no), K(new_round_attr));
  }

  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("log_archive", "defer_archive", "tenant_id", tenant_id_,
      "dest_no", dest_no, "round_id", new_round_attr.round_id_,
      "status", new_round_attr.state_.status_,
      "path", new_round_attr.path_.ptr());
  }

  return ret;
}

int ObArchiveHandler::checkpoint()
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantArchiveRoundAttr> rounds;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive scheduler not init", K(ret));
  } else if (OB_FAIL(archive_table_op_.get_all_active_rounds(*sql_proxy_, rounds))) {
    LOG_WARN("failed to get all rounds", K(ret));
  } else {
    // checkpoint each dest round.
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < rounds.count(); i++) {
      ObTenantArchiveRoundAttr &dest_round = rounds.at(i);
      if (OB_SUCCESS != (tmp_ret = checkpoint_(dest_round))) {
        LOG_WARN("failed to checkpoint dest round", K(ret), K(dest_round));
      }
    }
  }

  return ret;
}

int ObArchiveHandler::checkpoint_(ObTenantArchiveRoundAttr &round_info)
{
  int ret = OB_SUCCESS;

  LOG_INFO("schedule archive", K(round_info));

  switch (round_info.state_.status_) {
    case ObArchiveRoundState::Status::STOP:
    case ObArchiveRoundState::Status::INTERRUPTED:
    case ObArchiveRoundState::Status::SUSPEND:
      break;
    case ObArchiveRoundState::Status::PREPARE: {
      DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_PREPARE);
      if (OB_FAIL(start_archive_(round_info))) {
        LOG_WARN("failed to prepare archive", K(ret), K(round_info));
      }
      break;
    }
    case ObArchiveRoundState::Status::BEGINNING: {
      DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_BEGINNING);
      if (OB_FAIL(do_checkpoint_(round_info))) {
        LOG_WARN("failed to checkpoint", K(ret), K(round_info));
      }
      break;
    }
    case ObArchiveRoundState::Status::DOING: {
      DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_DOING);
      if (OB_FAIL(do_checkpoint_(round_info))) {
        LOG_WARN("failed to checkpoint", K(ret), K(round_info));
      }
      break;
    }
    case ObArchiveRoundState::Status::SUSPENDING: {
      DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_SUSPENDING);
      if (OB_FAIL(do_checkpoint_(round_info))) {
        LOG_WARN("failed to checkpoint", K(ret), K(round_info));
      }
      break;
    }
    case ObArchiveRoundState::Status::STOPPING: {
      DEBUG_SYNC(BEFROE_LOG_ARCHIVE_SCHEDULE_STOPPING);
      if (OB_FAIL(do_checkpoint_(round_info))) {
        LOG_WARN("failed to checkpoint", K(ret), K(round_info));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      LOG_ERROR("unknown archive status", K(ret), K(round_info));
    }
  }

  return ret;
}

int ObArchiveHandler::start_archive_(ObTenantArchiveRoundAttr &round_attr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // Handle archive in PREPARE state, following work will be done.
  // 1. Check available to backup media.
  // 2. If round has been switched, previous round end and current round start information need to be persisted to backup media.
  // 3. Update archive state to BEGINNING.
  // 4. Notify logstreams under the tenant to start archive.

  // TODO: 故障注入
  ObTenantArchiveRoundAttr new_round;
  ObArchiveStore store;
  ObBackupDest dest;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(round_handler_.start_archive(round_attr, new_round))) {
    LOG_WARN("failed to prepare beginning dest round", K(ret), K(round_attr));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, new_round.key_.tenant_id_, new_round.path_, dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(new_round), K(new_round)); 
  } else if (OB_FAIL(store.init(dest))) {
    LOG_WARN("failed to init archive store", K(ret), K(round_attr), K(dest));
  } else if (OB_SUCCESS != (tmp_ret = record_round_start(new_round, store))) {
    LOG_WARN("failed to open archive", K(ret), K(round_attr), K(new_round));
  } else if (OB_SUCCESS != (tmp_ret = notify_(new_round))) {
    LOG_WARN("notify failed", K(tmp_ret), K(new_round));
  }

  LOG_INFO("beginning archive", K(ret), K(new_round));
  return ret;
}

int ObArchiveHandler::do_checkpoint_(share::ObTenantArchiveRoundAttr &round_info)
{
  int ret = OB_SUCCESS;
  int64_t since_piece_id = 0;
  ObDestRoundSummary summary;
  ObDestRoundCheckpointer checkpointer;
  SCN max_checkpoint_scn = SCN::min_scn();
  bool can = false;
  bool allow_force_stop = false;
  if (OB_FAIL(ObTenantArchiveMgr::decide_piece_id(round_info.start_scn_, round_info.base_piece_id_, round_info.piece_switch_interval_, round_info.checkpoint_scn_, since_piece_id))) {
    LOG_WARN("failed to calc since piece id", K(ret), K(round_info));
  } else if (OB_FAIL(archive_table_op_.get_dest_round_summary(*sql_proxy_, round_info.dest_id_, round_info.round_id_, since_piece_id, summary))) {
    LOG_WARN("failed to get dest round summary.", K(ret), K(round_info), K(since_piece_id));
  } else if (OB_FAIL(get_max_checkpoint_scn_(tenant_id_, max_checkpoint_scn))) {
    LOG_WARN("failed to get limit scn.", K(ret));
  } else if (OB_FAIL(check_can_do_archive(can))) {
    LOG_WARN("failed to check can do archive", K(ret));
  } else if (!can) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant can not do archive", K(ret), K_(tenant_id));
  } else if (OB_FALSE_IT(DEBUG_SYNC(BEFROE_LOG_ARCHIVE_DO_CHECKPOINT))) {
  } else if (OB_FAIL(checkpointer.init(&round_handler_, piece_generated_cb, round_checkpoint_cb, max_checkpoint_scn))) {
    LOG_WARN("failed to init checkpointer", K(ret), K(round_info));
  } else if (round_info.state_.is_stopping() && OB_FAIL(check_allow_force_stop_(round_info, allow_force_stop))) {
    LOG_WARN("failed to check allow force stop", K(ret), K(round_info));
  } else if (allow_force_stop && OB_FALSE_IT(checkpointer.set_allow_force_stop())) {
  } else if (OB_FAIL(checkpointer.checkpoint(round_info, summary))) {
    LOG_WARN("failed to do checkpoint.", K(ret), K(round_info), K(summary));
  }

  return ret;
}


int ObArchiveHandler::notify_(const ObTenantArchiveRoundAttr &round)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  UNUSED(round);
  share::ObLSAttrArray ls_array;
  share::ObLSAttrOperator ls_operator(tenant_id_, sql_proxy_);
  hash::ObHashSet<ObAddr> notify_addr_set;
  share::ObLocationService *location_service = GCTX.location_service_;
  const bool force_renew = true;
  common::ObAddr leader_addr;
  obrpc::ObNotifyArchiveArg arg;
  arg.tenant_id_ = tenant_id_;

  if (OB_FAIL(ls_operator.get_all_ls_by_order(ls_array))) {
    LOG_WARN("failed to get all ls info", K(ret), K(tenant_id_));
  } else if (OB_FAIL(notify_addr_set.create(ls_array.count()))) {
    LOG_WARN("failed to create notify addr set", K(ret));
  } else {
    ARRAY_FOREACH_N(ls_array, i, cnt) {
      const ObLSAttr &ls_attr = ls_array.at(i);
      if(OB_FAIL(location_service->get_leader(GCONF.cluster_id, tenant_id_, ls_attr.get_ls_id(), force_renew, leader_addr))) {
        LOG_WARN("failed to get leader addr", K(ret), KP(location_service), "ls_id", ls_attr.get_ls_id());
      } else if(OB_FAIL(notify_addr_set.set_refactored(leader_addr))) {
        LOG_WARN("failed to set server_addr in notify_addr_set", K(ret), "ls_id", ls_attr.get_ls_id(), K(leader_addr));
      }
    }
    LOG_INFO("leader_addr_set to be notified archive:", K(notify_addr_set));
    for (hash::ObHashSet<ObAddr>::const_iterator it = notify_addr_set.begin(); it != notify_addr_set.end(); it++) {
      if (OB_TMP_FAIL(rpc_proxy_->to(it->first).by(tenant_id_).notify_archive(arg))) {
        LOG_WARN("failed to notify ls leader archive", K(tmp_ret), K(arg));
      } else {
        LOG_INFO("succeed to notify ls leader archive", K(arg), K(it->first));
      }
    }
  }

  return ret;
}

int ObArchiveHandler::get_max_checkpoint_scn_(const uint64_t tenant_id, SCN &max_checkpoint_scn) const
{
  // For standby tenant, archive progress is limited only by the max replayable scn for each log stream.
  // That will leads some log of type of create log stream is archived before been replayed. In this case,
  // we should limit tenant archive progress not more than the GTS.
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupUtils::get_backup_scn(tenant_id_, max_checkpoint_scn))) {
    LOG_WARN("failed to get max checkpoint scn.", K(ret), K_(tenant_id));
  }
  return ret;
}


int ObArchiveHandler::check_allow_force_stop_(const ObTenantArchiveRoundAttr &round, bool &allow_force_stop) const
{
  int ret = OB_SUCCESS;
  int64_t stopping_ts = 0;
  const int64_t current_ts = ObTimeUtility::current_time();
#ifdef ERRSIM
  const int64_t force_stop_threshold = GCONF.errsim_allow_force_archive_threshold;;
#else
  const int64_t force_stop_threshold = ALLOW_FORCE_STOP_THRESHOLD;
#endif
  allow_force_stop = false;
  if (OB_FAIL(archive_table_op_.get_round_stopping_ts(*sql_proxy_, round.key_.dest_no_, stopping_ts))) {
    LOG_WARN("failed to get round stopping ts.", K(ret), K(round));
  } else {
    allow_force_stop = force_stop_threshold <= (current_ts - stopping_ts);
  }
  return ret;
}