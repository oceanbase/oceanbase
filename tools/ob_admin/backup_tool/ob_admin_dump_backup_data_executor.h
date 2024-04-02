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

#ifndef OB_ADMIN_DUMP_BACKUP_DATA_EXECUTOR_H_
#define OB_ADMIN_DUMP_BACKUP_DATA_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/backup/ob_archive_store.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
namespace oceanbase {
namespace tools {

class ObAdminDumpBackupDataExecutor;

class ObAdminDumpBackupDataUtil {
public:
  static int read_archive_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str, 
      share::ObIBackupSerializeProvider &serializer);
  static int read_backup_file_header(const common::ObString &backup_path, const common::ObString &storage_info_str,
      backup::ObBackupFileHeader &file_header);
  static int read_data_file_trailer(const common::ObString &backup_path, const common::ObString &storage_info_str,
      backup::ObBackupDataFileTrailer &file_trailer);
  static int read_index_file_trailer(const common::ObString &backup_path, const common::ObString &storage_info_str,
      backup::ObBackupMultiLevelIndexTrailer &index_trailer);
  static int read_tablet_metas_file_trailer(const common::ObString &backup_path, const common::ObString &storage_info_str,
      backup::ObTabletInfoTrailer &tablet_meta_trailer);
  static int pread_file(const common::ObString &backup_path, const common::ObString &storage_info_str, const int64_t offset,
      const int64_t read_size, char *buf);
  static int get_backup_file_length(
      const common::ObString &backup_path, const common::ObString &storage_info_str, int64_t &file_length);
  static int get_common_header(
      blocksstable::ObBufferReader &buffer_reader, const share::ObBackupCommonHeader *&common_header);
  template <class IndexType>
  static int parse_from_index_blocks(blocksstable::ObBufferReader &buffer_reader,
      const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
      const std::function<int(const common::ObIArray<IndexType> &)> &print_func2);
  template <class IndexType>
  static int parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
      const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
      const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
      const std::function<int(const common::ObIArray<IndexType> &)> &print_func3);
  template <class IndexType, class IndexIndexType>
  static int parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
      const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
      const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
      const std::function<int(const common::ObIArray<IndexType> &)> &print_func3,
      const std::function<int(const common::ObIArray<IndexIndexType> &)> &print_func4);
  template <class IndexType>
  static int parse_from_index_blocks(
      blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list);
  template <typename BackupSmallFileType> 
  static int read_backup_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str, 
      BackupSmallFileType &file_info);
};

class ObAdminDumpBackupDataExecutor : public ObAdminExecutor {
public:
  ObAdminDumpBackupDataExecutor();
  virtual ~ObAdminDumpBackupDataExecutor();
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd_(int argc, char *argv[]);
  int check_file_exist_(const char *data_path, const char *storage_info_str);
  int check_dir_exist_(const char *data_path, const char *storage_info_str, bool &is_exist);
  int get_backup_file_path_();
  int get_backup_file_type_();
  int do_execute_();
  int print_usage_();
  int check_tenant_backup_path_(const char *data_path, const char *storage_info_str, bool &is_exist);
  int check_tenant_backup_path_type_(const char *data_path, const char *storage_info_str, share::ObBackupDestType::TYPE &type);
  int dump_tenant_backup_path_();
  int dump_tenant_archive_path_();
  int do_check_exist_();
  int build_rounds_info_(
      const share::ObPieceKey &first_piece, 
      const common::ObIArray<share::ObTenantArchivePieceAttr> &pieces,
      common::ObIArray<share::ObTenantArchiveRoundAttr> &rounds);

private:
  int print_backup_data_file_();
  int print_macro_range_index_file();
  int print_meta_index_file();

private:
  int print_backup_file_header_();
  int print_macro_block_();
  int print_tablet_meta_();
  int print_sstable_metas_();
  int print_macro_block_index_list_();
  int print_meta_index_list_();
  int print_data_file_trailer_();
  int print_index_file_trailer_();
  int print_macro_range_index_index_list_();
  int print_meta_index_index_list_();
  int print_ls_attr_info_();
  int print_tablet_to_ls_info_();
  int print_deleted_tablet_info_();
  int print_tenant_locality_info_();
  int print_tenant_diagnose_info_();
  int print_backup_set_info_();
  int print_place_holder_info_();
  int print_archive_round_start_file_();
  int print_archive_round_end_file_();
  int print_archive_piece_start_file_();
  int print_archive_piece_end_file_();
  int print_archive_single_piece_file_();
  int print_archive_piece_inner_placeholder_file_();
  int print_archive_single_ls_info_file_();
  int print_archive_piece_list_info_file_();
  int print_tenant_archive_piece_infos_file_();
  int print_ls_tablet_meta_tablets_();
  int print_backup_format_file_();
  int print_tenant_backup_set_infos_();
  int print_backup_ls_meta_infos_file_();
  int print_tablet_tx_data_file_();
private:
  int inner_print_macro_block_(const int64_t offset, const int64_t length, const int64_t idx = -1);
  int inner_print_tablet_meta_(const int64_t offset, const int64_t length);
  int inner_print_sstable_metas_(const int64_t offset, const int64_t length);
  int inner_print_backup_macro_block_id_mapping_metas_(const int64_t offset, const int64_t length);
  int inner_print_macro_block_index_list_(const int64_t offset, const int64_t length);
  int inner_print_meta_index_list_(const int64_t offset, const int64_t length);
  int inner_print_macro_range_index_list_(const int64_t offset, const int64_t length);
  int inner_print_macro_range_index_index_list_(const int64_t offset, const int64_t length);
  int inner_print_meta_index_index_list_(const int64_t offset, const int64_t length);
  int inner_print_common_header_(const common::ObString &backup_path, const common::ObString &storage_info);
  int inner_print_common_header_(const char *data_path, const char *storage_info_str);

private:
  int dump_tablet_trailer_(const backup::ObTabletInfoTrailer &tablet_meta_trailer);
  int dump_backup_file_header_(const backup::ObBackupFileHeader &file_header);
  int dump_common_header_(const share::ObBackupCommonHeader &common_header);
  int dump_data_file_trailer_(const backup::ObBackupDataFileTrailer &trailer);
  int dump_index_file_trailer_(const backup::ObBackupMultiLevelIndexTrailer &trailer);
  int dump_multi_level_index_header_(const backup::ObBackupMultiLevelIndexHeader &header);
  int dump_macro_block_index_(const backup::ObBackupMacroBlockIndex &index);
  int dump_macro_block_index_list_(const common::ObIArray<backup::ObBackupMacroBlockIndex> &index_list);
  int dump_macro_range_index_(const backup::ObBackupMacroRangeIndex &index);
  int dump_macro_range_index_list_(const common::ObIArray<backup::ObBackupMacroRangeIndex> &index_list);
  int dump_macro_range_index_index_(const backup::ObBackupMacroRangeIndexIndex &index);
  int dump_macro_range_index_index_list_(const common::ObIArray<backup::ObBackupMacroRangeIndexIndex> &index_list);
  int dump_meta_index_(const backup::ObBackupMetaIndex &index);
  int dump_meta_index_list_(const common::ObIArray<backup::ObBackupMetaIndex> &index_list);
  int dump_meta_index_index_(const backup::ObBackupMetaIndexIndex &index_index);
  int dump_meta_index_index_list_(const common::ObIArray<backup::ObBackupMetaIndexIndex> &index_list);
  int dump_backup_tablet_meta_(const backup::ObBackupTabletMeta &tablet_meta);
  int dump_backup_sstable_meta_(const backup::ObBackupSSTableMeta &sstable_meta);
  int dump_backup_macro_block_id_mapping_meta_(const backup::ObBackupMacroBlockIDMappingsMeta &mapping_meta);
  int dump_ls_attr_info_(const share::ObLSAttr &ls_attr);
  int dump_tablet_to_ls_info_(const storage::ObBackupDataTabletToLSInfo &tablet_to_ls_info);
  int dump_tenant_locality_info_(const storage::ObExternTenantLocalityInfoDesc &locality_info);
  int dump_tenant_diagnose_info_(const storage::ObExternTenantDiagnoseInfoDesc &diagnose_info);
  int dump_backup_set_info(const share::ObBackupSetFileDesc &backup_set_info);
  int dump_tenant_backup_set_infos_(const ObIArray<oceanbase::share::ObBackupSetFileDesc> &backup_set_infos);
  int dump_backup_ls_meta_infos_file_(const storage::ObBackupLSMetaInfosDesc &ls_meta_infos);
  int dump_archive_round_start_file_(const share::ObRoundStartDesc &round_start_file);
  int dump_archive_round_end_file_(const share::ObRoundEndDesc round_end_file);
  int dump_archive_piece_start_file_(const share::ObPieceStartDesc &piece_start_file);
  int dump_archive_piece_end_file_(const share::ObPieceEndDesc &piece_end_file);
  int dump_archive_single_piece_file_(const share::ObSinglePieceDesc &piece_single_file);
  int dump_one_piece_(const share::ObTenantArchivePieceAttr &piece);
  int dump_archive_piece_inner_placeholder_file_(const share::ObPieceInnerPlaceholderDesc &piece_inner_placeholder);
  int dump_archive_single_ls_info_file_(const share::ObSingleLSInfoDesc &single_ls_info_file);
  int dump_archive_piece_list_info_file_(const share::ObPieceInfoDesc &piece_info_file);
  int dump_tenant_archive_piece_infos_file_(const share::ObTenantArchivePieceInfosDesc &piece_infos_file);
  int dump_backup_format_file_(const share::ObBackupFormatDesc &format_file);
  int dump_check_exist_result_(const char *data_path, const char *storage_info_str, const bool is_exist);
  int print_tablet_autoinc_seq_(const share::ObTabletAutoincSeq &autoinc_seq);
  
private:
  int get_tenant_backup_set_infos_path_(const share::ObBackupSetDesc &backup_set_dir_name, 
      share::ObBackupPath &target_path);
  int get_backup_set_placeholder_dir_path(share::ObBackupPath &path);
  int filter_backup_set_(const storage::ObTenantBackupSetInfosDesc &tenant_backup_set_infos,
      const ObSArray<share::ObBackupSetDesc> &placeholder_infos,
      ObIArray<share::ObBackupSetFileDesc> &target_backup_set);
  int read_locality_info_file(const char *tenant_backup_path,
      share::ObBackupSetDesc latest_backup_set_desc,
      storage::ObExternTenantLocalityInfoDesc &locality_info);
private:
  char backup_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  int64_t offset_;
  int64_t length_;
  int64_t file_type_;
  bool is_quiet_;
  bool check_exist_;
  common::ObArenaAllocator allocator_;
};

}  // namespace tools
}  // namespace oceanbase

#endif
