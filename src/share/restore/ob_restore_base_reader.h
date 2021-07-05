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

#ifndef SRC_SHARE_RESTORE_OB_RESTORE_BASE_READER_H_
#define SRC_SHARE_RESTORE_OB_RESTORE_BASE_READER_H_

#include "lib/container/ob_iarray.h"
#include "lib/restore/ob_storage_path.h"
#include "ob_restore_args.h"
#include "lib/hash/ob_hashset.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {

namespace share {
namespace restore {

class ObRestoreBaseReader {
public:
  ObRestoreBaseReader(ObRestoreArgs& args);
  virtual ~ObRestoreBaseReader() = default;
  int init(const common::ObString& oss_uri);
  int get_create_unit_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_pool_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_tenant_stmt(common::ObString& stmt);
  int get_create_tablegroup_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_foreign_key_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_database_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_data_table_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_user_stmts(common::ObIArray<common::ObString>& stmts);
  int get_create_index_table_stmts(
      common::ObIArray<common::ObString>& stmts, common::hash::ObHashSet<uint64_t>& dropped_index_ids);
  int get_create_synonym_stmts(common::ObIArray<common::ObString>& stmts);
  /* Commands that can be executed directly without any modification */
  int get_direct_executable_stmts(const char* direct_executable_definitions, common::ObIArray<common::ObString>& stmts);
  int get_recycle_objects(common::ObIArray<schema::ObRecycleObject>& objects);
  int get_create_all_timezone_stmts(common::ObIArray<common::ObString>& stmts);

private:
  int get_one_object_from_oss(
      const char* last_name, const bool allow_not_exist, common::ObIArray<common::ObString>& stmts);
  int get_create_table_stmt(
      const common::ObString& table_id_str, const bool is_index, common::ObString& stmt, uint64_t& table_id);
  int get_create_stmts_for_solo_backup_object(
      const char* object_ids_list_str, const char* object_definition_str, common::ObIArray<common::ObString>& stmts);

  int read_one_file(const common::ObStoragePath& path, common::ObIAllocator& allocator, char*& buf, int64_t& read_size);

private:
  // oss path: "oss://runiu1/ob1.XX/3/1001"
  // file path: "file:///mnt/test_nfs_runiu/ob1.XX/3/1001"
  common::ObStoragePath common_path_;
  ObRestoreArgs& args_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

}  // namespace restore
}  // namespace share
}  // namespace oceanbase

#endif /* SRC_SHARE_RESTORE_OB_RESTORE_BASE_READER_H_ */
