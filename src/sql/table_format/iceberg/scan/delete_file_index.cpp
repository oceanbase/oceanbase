/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/scan/delete_file_index.h"

#include "lib/container/ob_se_array.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

DeleteFileIndex::~DeleteFileIndex()
{
  if (eq_deletes_by_partition_.created()) {
    eq_deletes_by_partition_.destroy();
  }
  if (pos_deletes_by_partition_.created()) {
    pos_deletes_by_partition_.destroy();
  }
  if (pos_deletes_by_path_.created()) {
    pos_deletes_by_path_.destroy();
  }
  if (dv_by_path_.created()) {
    dv_by_path_.destroy();
  }
}

int DeleteFileIndex::alloc_delete_file_list_(ObArenaAllocator &allocator,
                                             ObArray<const ManifestEntry *> *&delete_files)
{
  int ret = OB_SUCCESS;
  delete_files = OB_NEWx(ObArray<const ManifestEntry *>,
                         &allocator,
                         DELETE_FILE_LIST_BLOCK_SIZE,
                         ModulePageAllocator(allocator));
  if (OB_ISNULL(delete_files)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate delete file list", K(ret));
  }
  return ret;
}

const ObString *DeleteFileIndex::find_string_bound_(
    const ObIArray<std::pair<int32_t, ObString>> &bounds,
    const int32_t field_id)
{
  const ObString *bound = nullptr;
  for (int64_t i = 0; OB_ISNULL(bound) && i < bounds.count(); ++i) {
    if (bounds.at(i).first == field_id && !bounds.at(i).second.empty()) {
      bound = &bounds.at(i).second;
    }
  }
  return bound;
}

bool DeleteFileIndex::may_contain_data_file_path_(const ManifestEntry &delete_file,
                                                  const ObString &data_file_path)
{
  bool may_contain = true;
  const ObString *lower_bound
      = find_string_bound_(delete_file.data_file.lower_bounds, POSITION_DELETE_FILE_PATH_FIELD_ID);
  const ObString *upper_bound
      = find_string_bound_(delete_file.data_file.upper_bounds, POSITION_DELETE_FILE_PATH_FIELD_ID);
  // Iceberg bounds are inclusive. Truncated string bounds remain conservative:
  // a truncated lower bound is <= all values and an upper bound is >= all values.
  if (OB_NOT_NULL(lower_bound) && data_file_path.compare(*lower_bound) < 0) {
    may_contain = false;
  } else if (OB_NOT_NULL(upper_bound) && data_file_path.compare(*upper_bound) > 0) {
    may_contain = false;
  }
  return may_contain;
}

int DeleteFileIndex::init(const ObIArray<const ManifestEntry *> &manifest_entries)
{
  int ret = OB_SUCCESS;
  global_deletes_.set_block_size(DELETE_FILE_LIST_BLOCK_SIZE);
  int64_t eq_partition_count = 0;
  int64_t pos_partition_count = 0;
  int64_t pos_path_count = 0;
  int64_t dv_path_count = 0;
  int64_t global_eq_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_entries.count(); ++i) {
    const ManifestEntry *entry = manifest_entries.at(i);
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("manifest entry is null", K(ret), K(i));
    } else if (entry->is_deletion_vector_file()) {
      ++dv_path_count;
    } else if (entry->is_position_delete_file()) {
      if (entry->data_file.referenced_data_file.has_value()) {
        ++pos_path_count;
      } else {
        ++pos_partition_count;
      }
    } else if (entry->is_equality_delete_file()) {
      if (entry->partition_spec.is_unpartitioned()) {
        ++global_eq_count;
      } else {
        ++eq_partition_count;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (eq_partition_count > 0
             && OB_FAIL(eq_deletes_by_partition_.create(eq_partition_count, "DeleteFileIndex"))) {
    LOG_WARN("init map failed", K(ret));
  } else if (pos_partition_count > 0
             && OB_FAIL(pos_deletes_by_partition_.create(pos_partition_count, "DeleteFileIndex"))) {
    LOG_WARN("init map failed", K(ret));
  } else if (pos_path_count > 0
             && OB_FAIL(pos_deletes_by_path_.create(pos_path_count, "DeleteFileIndex"))) {
    LOG_WARN("init map failed", K(ret));
  } else if (dv_path_count > 0 && OB_FAIL(dv_by_path_.create(dv_path_count, "DeleteFileIndex"))) {
    LOG_WARN("init map failed", K(ret));
  } else if (global_eq_count > 0 && OB_FAIL(global_deletes_.reserve(global_eq_count))) {
    LOG_WARN("failed to reserve global delete files", K(ret), K(global_eq_count));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_entries.count(); ++i) {
    const ManifestEntry *entry = manifest_entries.at(i);
    if (entry->is_position_delete_file()) {
      if (entry->is_deletion_vector_file()) {
        if (OB_FAIL(add_deletion_vector_(entry))) {
          LOG_WARN("add dv failed", K(ret));
        }
      } else {
        if (OB_FAIL(add_pos_delete_(entry))) {
          LOG_WARN("add pos delete failed", K(ret));
        }
      }
    } else if (entry->is_equality_delete_file()) {
      if (OB_FAIL(add_eq_delete_(entry))) {
        LOG_WARN("add eq delete failed", K(ret));
      }
    }
  }

  OZ(sort_all_delete_files_());
  return ret;
}

int DeleteFileIndex::match_delete_files(const ManifestEntry &data_file,
                                        ObIArray<const ManifestEntry *> &delete_files)
{
  int ret = OB_SUCCESS;
  delete_files.reset();
  ObSEArray<const ManifestEntry *, EXPECTED_DELETE_FILES_PER_KEY> pos_deletes;
  ObSEArray<const ManifestEntry *, EXPECTED_DELETE_FILES_PER_KEY> eq_deletes;
  ObSEArray<const ManifestEntry *, EXPECTED_DELETE_FILES_PER_KEY> dv;
  if (OB_FAIL(match_delete_files(data_file, pos_deletes, eq_deletes, dv))) {
    LOG_WARN("match_delete_files failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pos_deletes.count(); ++i) {
      OZ(delete_files.push_back(pos_deletes.at(i)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < eq_deletes.count(); ++i) {
      OZ(delete_files.push_back(eq_deletes.at(i)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dv.count(); ++i) {
      OZ(delete_files.push_back(dv.at(i)));
    }
  }
  return ret;
}

int DeleteFileIndex::match_delete_files(const ManifestEntry &data_file,
                                        ObIArray<const ManifestEntry *> &pos_deletes,
                                        ObIArray<const ManifestEntry *> &eq_deletes,
                                        ObIArray<const ManifestEntry *> &dv) const
{
  int ret = OB_SUCCESS;
  pos_deletes.reset();
  eq_deletes.reset();
  dv.reset();
  if (!data_file.is_data_file()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input manifest entry must be data file", K(ret));
  } else if (OB_FAIL(find_global_deletes_(data_file, eq_deletes))) {
    LOG_WARN("find global eq deletes failed", K(ret));
  } else if (OB_FAIL(find_eq_partition_deletes_(data_file, eq_deletes))) {
    LOG_WARN("find eq partition deletes failed", K(ret));
  } else if (OB_FAIL(find_deletion_vectors_(data_file, dv))) {
    LOG_WARN("find deletion vectors failed", K(ret));
  } else if (dv.empty()) {
    // we only search pos deletes when there is no dv matched
    if (OB_FAIL(find_pos_path_deletes_(data_file, pos_deletes))) {
      LOG_WARN("find pos path deletes failed", K(ret));
    } else if (OB_FAIL(find_pos_partition_deletes_(data_file, pos_deletes))) {
      LOG_WARN("find pos partition deletes failed", K(ret));
    }
  }
  return ret;
}

int DeleteFileIndex::add_deletion_vector_(const ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  if (!manifest_entry->data_file.referenced_data_file.has_value()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dv must have referenced data file", K(ret));
  } else {
    OZ(dv_by_path_.set_refactored(manifest_entry->data_file.referenced_data_file.value(),
                                  manifest_entry));
  }
  return ret;
}

int DeleteFileIndex::add_pos_delete_(const ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  ObArray<const ManifestEntry *> *delete_files = NULL;

  if (manifest_entry->data_file.referenced_data_file.has_value()) {
    const ObString &referenced_data_file = manifest_entry->data_file.referenced_data_file.value();
    ret = pos_deletes_by_path_.get_refactored(referenced_data_file, delete_files);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(alloc_delete_file_list_(allocator_, delete_files))) {
        LOG_WARN("failed to allocate path delete file list", K(ret));
      } else if (OB_FAIL(pos_deletes_by_path_.set_refactored(referenced_data_file, delete_files))) {
        LOG_WARN("failed to add path delete file list", K(ret));
      }
    }
  } else {
    PartitionKey partition_key;
    if (OB_FAIL(partition_key.init_from_manifest_entry(*manifest_entry))) {
      LOG_WARN("init partition key failed", K(ret));
    } else {
      ret = pos_deletes_by_partition_.get_refactored(partition_key, delete_files);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(alloc_delete_file_list_(allocator_, delete_files))) {
          LOG_WARN("failed to allocate partition delete file list", K(ret));
        } else if (OB_FAIL(pos_deletes_by_partition_.set_refactored(partition_key, delete_files))) {
          LOG_WARN("failed to add partition delete file list", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(delete_files)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete file list is null", K(ret));
    } else if (OB_FAIL(delete_files->push_back(manifest_entry))) {
      LOG_WARN("failed to add position delete file", K(ret));
    }
  }
  return ret;
}

int DeleteFileIndex::add_eq_delete_(const ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  const PartitionSpec &partition_spec = manifest_entry->partition_spec;
  ObArray<const ManifestEntry *> *delete_files = NULL;
  if (partition_spec.is_unpartitioned()) {
    delete_files = &global_deletes_;
  } else {
    PartitionKey partition_key;
    if (OB_FAIL(partition_key.init_from_manifest_entry(*manifest_entry))) {
      LOG_WARN("init partition key failed", K(ret));
    } else {
      ret = eq_deletes_by_partition_.get_refactored(partition_key, delete_files);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(alloc_delete_file_list_(allocator_, delete_files))) {
          LOG_WARN("failed to allocate equality delete file list", K(ret));
        } else if (OB_FAIL(eq_deletes_by_partition_.set_refactored(partition_key, delete_files))) {
          LOG_WARN("failed to add equality delete file list", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(delete_files)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete file list is null", K(ret));
    } else if (OB_FAIL(delete_files->push_back(manifest_entry))) {
      LOG_WARN("failed to add equality delete file", K(ret));
    }
  }
  return ret;
}

int DeleteFileIndex::sort_all_delete_files_()
{
  struct SortByPartitionKey
  {
    int operator()(
        const hash::HashMapPair<PartitionKey, ObArray<const ManifestEntry *> *> &kv) const
    {
      DeleteFileIndex::sort_delete_files(*kv.second);
      return OB_SUCCESS;
    }
  };

  struct SortByPath
  {
    int operator()(const hash::HashMapPair<ObString, ObArray<const ManifestEntry *> *> &kv) const
    {
      DeleteFileIndex::sort_delete_files(*kv.second);
      return OB_SUCCESS;
    }
  };

  int ret = OB_SUCCESS;
  DeleteFileIndex::sort_delete_files(global_deletes_);
  SortByPartitionKey sort_by_partition_key;
  SortByPath sort_by_path;
  if (eq_deletes_by_partition_.created()) {
    OZ(eq_deletes_by_partition_.foreach_refactored(sort_by_partition_key));
  }
  if (OB_SUCC(ret) && pos_deletes_by_partition_.created()) {
    OZ(pos_deletes_by_partition_.foreach_refactored(sort_by_partition_key));
  }
  if (OB_SUCC(ret) && pos_deletes_by_path_.created()) {
    OZ(pos_deletes_by_path_.foreach_refactored(sort_by_path));
  }
  return ret;
}

int DeleteFileIndex::find_global_deletes_(const ManifestEntry &data_file,
                                          ObIArray<const ManifestEntry *> &result) const
{
  // data files' seq < delete files' seq
  int ret = OB_SUCCESS;
  if (OB_FAIL(DeleteFileIndex::find_eq_deletes(data_file, global_deletes_, result))) {
    LOG_WARN("find global eq deletes failed", K(ret));
  }
  return ret;
}

int DeleteFileIndex::find_eq_partition_deletes_(const ManifestEntry &data_file,
                                                ObIArray<const ManifestEntry *> &result) const
{
  // data files' partition key = delete files' partition key
  // AND
  // data files' seq < delete files' seq
  int ret = OB_SUCCESS;
  PartitionKey partition_key;
  ObArray<const ManifestEntry *> *delete_files = NULL;
  if (eq_deletes_by_partition_.created()) {
    if (OB_FAIL(partition_key.init_from_manifest_entry(data_file))) {
      LOG_WARN("init partition key failed", K(ret));
    } else {
      ret = eq_deletes_by_partition_.get_refactored(partition_key, delete_files);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        delete_files = NULL;
      }
    }
  }

  if (OB_SUCC(ret) && delete_files != NULL) {
    if (OB_FAIL(DeleteFileIndex::find_eq_deletes(data_file, *delete_files, result))) {
      LOG_WARN("find partition eq deletes failed", K(ret));
    }
  }

  return ret;
}

int DeleteFileIndex::find_deletion_vectors_(const ManifestEntry &data_file,
                                            ObIArray<const ManifestEntry *> &result) const
{
  int ret = OB_SUCCESS;
  const ManifestEntry *delete_file = NULL;
  if (dv_by_path_.created()) {
    ret = dv_by_path_.get_refactored(data_file.data_file.file_path, delete_file);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      delete_file = NULL;
    }
  }
  if (OB_SUCC(ret) && delete_file != NULL) {
    if (delete_file->sequence_number < data_file.sequence_number) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dv's seq must >= data file's seq", K(ret));
    } else {
      OZ(result.push_back(delete_file));
    }
  }
  return ret;
}

int DeleteFileIndex::find_pos_path_deletes_(const ManifestEntry &data_file,
                                            ObIArray<const ManifestEntry *> &result) const
{
  int ret = OB_SUCCESS;
  ObArray<const ManifestEntry *> *delete_files = NULL;
  if (pos_deletes_by_path_.created()) {
    ret = pos_deletes_by_path_.get_refactored(data_file.data_file.file_path, delete_files);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      delete_files = NULL;
    }
  }

  if (OB_SUCC(ret) && delete_files != NULL) {
    if (OB_FAIL(DeleteFileIndex::find_pos_deletes(data_file, *delete_files, result))) {
      LOG_WARN("find pos deletes failed", K(ret));
    }
  }

  return ret;
}

int DeleteFileIndex::find_pos_partition_deletes_(const ManifestEntry &data_file,
                                                 ObIArray<const ManifestEntry *> &result) const
{
  int ret = OB_SUCCESS;
  PartitionKey partition_key;
  ObArray<const ManifestEntry *> *delete_files = NULL;
  if (pos_deletes_by_partition_.created()) {
    if (OB_FAIL(partition_key.init_from_manifest_entry(data_file))) {
      LOG_WARN("init partition key failed", K(ret));
    } else {
      ret = pos_deletes_by_partition_.get_refactored(partition_key, delete_files);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        delete_files = NULL;
      }
    }
  }

  if (OB_SUCC(ret) && delete_files != NULL) {
    if (OB_FAIL(DeleteFileIndex::find_pos_deletes(data_file, *delete_files, result))) {
      LOG_WARN("find pos deletes failed", K(ret));
    }
  }
  return ret;
}

void DeleteFileIndex::sort_delete_files(ObArray<const ManifestEntry *> &delete_files)
{
  struct ManifestEntryComparator
  {
    bool operator()(const ManifestEntry *&l, const ManifestEntry *&r) const
    {
      return l->sequence_number < r->sequence_number;
    }
  };
  lib::ob_sort(delete_files.begin(), delete_files.end(), ManifestEntryComparator());
}

int DeleteFileIndex::find_eq_deletes(const ManifestEntry &manifest_entry,
                                     const ObArray<const ManifestEntry *> &eq_delete_files,
                                     ObIArray<const ManifestEntry *> &result)
{
  struct SeqComparator
  {
    bool operator()(int64_t seq, const ManifestEntry *entry) const
    {
      return seq < entry->sequence_number;
    }
  };

  int ret = OB_SUCCESS;
  ObArray<const ManifestEntry *>::const_iterator iter
      = std::upper_bound(eq_delete_files.begin(),
                         eq_delete_files.end(),
                         manifest_entry.sequence_number,
                         SeqComparator());
  while (OB_SUCC(ret) && iter != eq_delete_files.end()) {
    if (OB_UNLIKELY(manifest_entry.sequence_number >= (*iter)->sequence_number)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected", K(ret));
    } else {
      OZ(result.push_back(*iter));
    }
    iter++;
  }
  return ret;
}

int DeleteFileIndex::find_pos_deletes(const ManifestEntry &manifest_entry,
                                      const ObArray<const ManifestEntry *> &pos_delete_files,
                                      ObIArray<const ManifestEntry *> &result)
{
  struct SeqComparator
  {
    bool operator()(const ManifestEntry *entry, int64_t seq) const
    {
      return entry->sequence_number < seq;
    }
  };

  int ret = OB_SUCCESS;
  ObArray<const ManifestEntry *>::const_iterator iter
      = std::lower_bound(pos_delete_files.begin(),
                         pos_delete_files.end(),
                         manifest_entry.sequence_number,
                         SeqComparator());
  while (OB_SUCC(ret) && iter != pos_delete_files.end()) {
    if (OB_UNLIKELY(manifest_entry.sequence_number > (*iter)->sequence_number)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected", K(ret));
    } else if (may_contain_data_file_path_(**iter, manifest_entry.data_file.file_path)) {
      OZ(result.push_back(*iter));
    }
    iter++;
  }
  return ret;
}

} // namespace iceberg

} // namespace sql

}; // namespace oceanbase
