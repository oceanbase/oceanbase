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

#ifndef OB_ADMIN_DUMPBACKUP_EXECUTOR_H_
#define OB_ADMIN_DUMPBACKUP_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"
#include "lib/container/ob_array.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "share/backup/ob_tenant_name_mgr.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_i_table.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_multi_tenant.h"

#include "storage/ob_i_table.h"
#include "storage/backup/ob_partition_backup_struct.h"

namespace oceanbase
{
namespace tools
{

typedef common::hash::ObHashMap<storage::ObITable::TableKey,
    common::ObArray<storage::ObBackupTableMacroIndex> *> TableMacroIndexMap;

enum ObBackupMacroIndexVersion
{
  BACKUP_MACRO_INDEX_VERSION_1, // before 2.2.77
  BACKUP_MACRO_INDEX_VERSION_2, // after 3.1
};

class ObAdminDumpBackupDataReaderUtil
{
public:
  static int get_data_type(
      const char *data_path,
      const char *storage_info,
      ObBackupFileType &data_type);
  static int get_backup_data_common_header(
      const char *data_path,
      const char *storage_info,
      share::ObBackupCommonHeader &common_header);
  static int get_backup_data(
      const char *data_path,
      const char *storage_info,
      char *&buf,
      int64_t &file_length,
      common::ObIAllocator &allocator);
  static int get_meta_index(
      blocksstable::ObBufferReader &buffer_reader,
      share::ObBackupCommonHeader &common_header,
      common::ObIArray<ObBackupMetaIndex> &meta_index_array);
  static int get_backup_data(
      const char *data_path,
      const char *storage_info,
      char *&buf,
      const int64_t offset,
      const int64_t data_length,
      common::ObIAllocator &allocator);
  static int get_meta_header(
      const char *buf,
      const int64_t read_size,
      share::ObBackupMetaHeader &common_header);
  static int get_macro_block_index(
      blocksstable::ObBufferReader &buffer_reader,
      share::ObBackupCommonHeader &common_header,
      common::ObIArray<ObBackupMacroIndex> &macro_index_array);
  static int get_macro_block_index_v2(
      blocksstable::ObBufferReader &buffer_reader,
      common::ObIAllocator &allocator,
      TableMacroIndexMap &map);
private:
  static int add_sstable_index(
      const storage::ObITable::TableKey &table_key,
      const common::ObIArray<storage::ObBackupTableMacroIndex> &index_list,
      common::ObIAllocator &allocator,
      TableMacroIndexMap &index_map);
  static int get_table_key_ptr(
      common::ObIAllocator &allocator,
      common::ObArray<storage::ObITable::TableKey *> &table_key_ptrs,
      const storage::ObITable::TableKey &table_key,
      const storage::ObITable::TableKey *&table_key_ptr);
};


class ObAdminDumpBackupDataExecutor : public ObAdminExecutor
{
public:
  ObAdminDumpBackupDataExecutor();
  virtual ~ObAdminDumpBackupDataExecutor();
  virtual int execute(int argc, char *argv[]);
private:
  int parse_cmd(int argc, char *argv[]);
  void print_common_header();
  void print_common_header(const share::ObBackupCommonHeader &common_header);
  void print_meta_header(const share::ObBackupMetaHeader &meta_header);
  void print_backup_info();
  void print_backup_tenant_info();
  void print_clog_backup_info();
  void print_backup_set_info();
  void print_backup_piece_info();
  void print_backup_single_piece_info();
  void print_backup_set_file_info();
  void print_pg_list();
  void print_meta_index();
  int print_meta_index_(
      const share::ObBackupCommonHeader &common_header,
      const common::ObIArray<ObBackupMetaIndex> &meta_index_array);
  void print_meta_data();
  void print_partition_group_meta(
      const char *buf,
      const int64_t size);
  void print_partition_meta(
      const char *buf,
      const int64_t size);
  void print_sstable_meta(
      const char *buf,
      const int64_t size);
  void print_table_keys(
      const char *buf,
      const int64_t size);
  void print_partition_group_meta_info(
      const char *buf,
      const int64_t size);
  void print_macro_data_index();
  int get_macro_data_index_version(int64_t &version);
  void print_macro_data_index_v1(
      const char *buf,
      const int64_t size);
  int print_macro_data_index_v1_(
      const share::ObBackupCommonHeader &common_header,
      const common::ObIArray<ObBackupMacroIndex> &macro_index_array);
  void print_macro_data_index_v2(
      const char *buf,
      const int64_t size);
  int print_macro_data_index_v2_(
      const TableMacroIndexMap &index_map);
  void print_macro_block();
  void print_tenant_name_info();
  void print_tenant_name_info_meta(const ObTenantNameSimpleMgr::ObTenantNameMeta &meta);
  void print_tenant_name_info(const int64_t idx,
      const ObTenantNameSimpleMgr::ObTenantNameInfo &info);
  void print_usage();
  void print_macro_meta();
  void print_super_block();
  int dump_macro_block(const int64_t macro_id);
  void dump_sstable();
  void dump_sstable_meta();
  int open_store_file();
  void print_archive_block_meta();
  void print_archive_index_file();
  int check_exist(const char *data_path, const char *storage_info);
private:
  static const int64_t MAX_BACKUP_ID_STR_LENGTH = 64;
private:
  char data_path_[common::OB_MAX_URI_LENGTH];
  char input_data_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  share::ObBackupCommonHeader common_header_;
  common::ObArenaAllocator allocator_;
  bool is_quiet_;
  int64_t offset_;
  int64_t data_length_;
  bool check_exist_;
};


} //namespace tools
} //namespace oceanbase

#endif /* OB_ADMIN_DUMPBACKUP_EXECUTOR_H_ */
