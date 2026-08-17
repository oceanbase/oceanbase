/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

// DeleteFilexIndex 里面不持有任何内存，需要自己管理生命周期
class DeleteFileIndex
{
public:
  ~DeleteFileIndex();
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
  static const int64_t EXPECTED_DELETE_FILES_PER_KEY = 4;
  static const int64_t DELETE_FILE_LIST_BLOCK_SIZE
      = EXPECTED_DELETE_FILES_PER_KEY * sizeof(const ManifestEntry *);
  static const int32_t POSITION_DELETE_FILE_PATH_FIELD_ID = 2147483546;

  static int alloc_delete_file_list_(ObArenaAllocator &allocator,
                                     ObArray<const ManifestEntry *> *&delete_files);
  static const ObString *find_string_bound_(const ObIArray<std::pair<int32_t, ObString>> &bounds,
                                            int32_t field_id);
  static bool may_contain_data_file_path_(const ManifestEntry &delete_file,
                                          const ObString &data_file_path);

  int add_deletion_vector_(const ManifestEntry *manifest_entry);
  int add_pos_delete_(const ManifestEntry *manifest_entry);
  int add_eq_delete_(const ManifestEntry *manifest_entry);
  int sort_all_delete_files_();
  int find_global_deletes_(const ManifestEntry &manifest_entry,
                           ObIArray<const ManifestEntry *> &result) const;
  int find_eq_partition_deletes_(const ManifestEntry &manifest_entry,
                                 ObIArray<const ManifestEntry *> &result) const;
  int find_deletion_vectors_(const ManifestEntry &manifest_entry,
                             ObIArray<const ManifestEntry *> &result) const;
  int find_pos_path_deletes_(const ManifestEntry &manifest_entry,
                             ObIArray<const ManifestEntry *> &result) const;
  int find_pos_partition_deletes_(const ManifestEntry &manifest_entry,
                                  ObIArray<const ManifestEntry *> &result) const;

  ObArray<const ManifestEntry *> global_deletes_;
  // DeleteFileIndex is local to one pruning flow, so map operations are serial.
  hash::ObHashMap<PartitionKey, ObArray<const ManifestEntry *> *, hash::NoPthreadDefendMode>
      eq_deletes_by_partition_;
  hash::ObHashMap<PartitionKey, ObArray<const ManifestEntry *> *, hash::NoPthreadDefendMode>
      pos_deletes_by_partition_;
  hash::ObHashMap<ObString, ObArray<const ManifestEntry *> *, hash::NoPthreadDefendMode>
      pos_deletes_by_path_;
  hash::ObHashMap<ObString, const ManifestEntry *, hash::NoPthreadDefendMode> dv_by_path_;
  ObArenaAllocator allocator_;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // DELETE_FILE_INDEX_H
