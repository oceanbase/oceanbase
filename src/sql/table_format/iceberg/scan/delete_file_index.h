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

#ifndef DELETE_FILE_INDEX_H
#define DELETE_FILE_INDEX_H

#include "sql/table_format/iceberg/spec/manifest.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

// todo 基于统计信息的过滤还没做
// DeleteFilexIndex 里面不持有任何内存，需要自己管理生命周期
class DeleteFileIndex
{
public:
  int init(const ObIArray<const ManifestEntry *> &manifest_entries);
  int match_delete_files(const ManifestEntry &data_file,
                         ObIArray<const ManifestEntry *> &delete_files);
  int match_delete_files(const ManifestEntry &data_file,
                         ObIArray<const ManifestEntry *> &pos_deletes,
                         ObIArray<const ManifestEntry *> &eq_deletes,
                         ObIArray<const ManifestEntry *> &dv) const;
  static void sort_delete_files(ObArray<const ManifestEntry *> &delete_files);
  static int find_eq_deletes(const ManifestEntry &manifest_entry,
                             const ObArray<const ManifestEntry *> &eq_delete_files,
                             ObIArray<const ManifestEntry *> &result);
  static int find_pos_deletes(const ManifestEntry &manifest_entry,
                              const ObArray<const ManifestEntry *> &pos_delete_files,
                              ObIArray<const ManifestEntry *> &result);

private:
  int add_deletion_vector_(const ManifestEntry *manifest_entry);
  int add_pos_delete_(const ManifestEntry *manifest_entry);
  int add_eq_delete_(const ManifestEntry *manifest_entry);
  int sort_all_delete_files_();
  int find_global_deletes_(const ManifestEntry &manifest_entry, ObIArray<const ManifestEntry *> &result) const;
  int find_eq_partition_deletes_(const ManifestEntry &manifest_entry,
                                 ObIArray<const ManifestEntry *> &result) const;
  int find_deletion_vectors_(const ManifestEntry &manifest_entry,
                             ObIArray<const ManifestEntry *> &result) const;
  int find_pos_path_deletes_(const ManifestEntry &manifest_entry,
                             ObIArray<const ManifestEntry *> &result) const;
  int find_pos_partition_deletes_(const ManifestEntry &manifest_entry,
                                  ObIArray<const ManifestEntry *> &result) const;

  ObArray<const ManifestEntry *> global_deletes_;
  hash::ObHashMap<PartitionKey, ObArray<const ManifestEntry *> *> eq_deletes_by_partition_;
  hash::ObHashMap<PartitionKey, ObArray<const ManifestEntry *> *> pos_deletes_by_partition_;
  hash::ObHashMap<ObString, ObArray<const ManifestEntry *> *> pos_deletes_by_path_;
  hash::ObHashMap<ObString, const ManifestEntry *> dv_by_path_;
  ObArenaAllocator allocator_;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // DELETE_FILE_INDEX_H
