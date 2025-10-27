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

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/spec/snapshot.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/scan/delete_file_index.h"
#include "sql/table_format/iceberg/scan/task.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"

#include <avro/DataFile.hh>
#include <avro/Stream.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

Snapshot::Snapshot(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      v1_manifests(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      summary(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      cached_manifest_file_(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int Snapshot::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    std::optional<int64_t> tmp_sequence_number;
    if (OB_FAIL(
            ObCatalogJsonUtils::get_primitive(json_object, SEQUENCE_NUMBER, tmp_sequence_number))) {
      LOG_WARN("fail to get sequence-number", K(ret));
    } else if (tmp_sequence_number.has_value()) {
      sequence_number = tmp_sequence_number.value();
    } else {
      sequence_number = INITIAL_SEQUENCE_NUMBER;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SNAPSHOT_ID, snapshot_id))) {
      LOG_WARN("fail to get snapshot-id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                  PARENT_SNAPSHOT_ID,
                                                  parent_snapshot_id))) {
      LOG_WARN("fail to get parent-snapshot-id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, TIMESTAMP_MS, timestamp_ms))) {
      LOG_WARN("fail to get timestamp-ms", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObJsonNode *json_summary = json_object.get_value(SUMMARY);
    if (NULL != json_summary) {
      if (ObJsonNodeType::J_OBJECT != json_summary->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid summary value", K(ret));
      } else if (OB_FAIL(ObCatalogJsonUtils::convert_json_object_to_map(
                     allocator_,
                     *down_cast<const ObJsonObject *>(json_summary),
                     summary))) {
        LOG_WARN("failed to parse summary", K(ret));
      }
    } else {
      summary.reset();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SCHEMA_ID, schema_id))) {
      LOG_WARN("fail to get schema-id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObJsonNode *manifest_list_value = json_object.get_value(MANIFEST_LIST);
    if (NULL != manifest_list_value) {
      if (ObJsonNodeType::J_STRING != manifest_list_value->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid manifest list value", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator_,
                                         ObString(manifest_list_value->get_data_length(),
                                                  manifest_list_value->get_data()),
                                         manifest_list))) {
        LOG_WARN("fail to deep copy manifest-list", K(ret));
      }
    } else {
      // v1 有种情况是其没有 manifest_list，直接在 snapshot 里面存了 manifest 的数组
      ret = OB_SUCCESS;
      if (OB_FAIL(ObCatalogJsonUtils::get_string_array(allocator_,
                                                       json_object,
                                                       MANIFESTS,
                                                       v1_manifests))) {
        LOG_WARN("fail to get v1-manifests", K(ret));
      }
    }
  }
  return ret;
}

int Snapshot::assign(const Snapshot &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    snapshot_id = other.snapshot_id;
    parent_snapshot_id = other.parent_snapshot_id;
    sequence_number = other.sequence_number;
    timestamp_ms = other.timestamp_ms;
    schema_id = other.schema_id;
    OZ(ob_write_string(allocator_, other.manifest_list, manifest_list));
    for (int64_t i = 0; OB_SUCC(ret) && i < other.v1_manifests.size(); i++) {
      ObString tmp;
      OZ(ob_write_string(allocator_, other.v1_manifests[i], tmp));
      OZ(v1_manifests.push_back(tmp));
    }
    OZ(ObIcebergUtils::deep_copy_map_string(allocator_, other.summary, summary));
  }
  return ret;
}

int64_t Snapshot::get_convert_size() const
{
  // todo
  return 0;
}

int Snapshot::get_manifest_files(const ObString &access_info,
                                 ObIArray<ManifestFile *> &manifest_files)
{
  int ret = OB_SUCCESS;
  if (cached_manifest_file_.empty()) {
    if (OB_FAIL(get_manifest_files_(access_info, cached_manifest_file_))) {
      LOG_WARN("failed to load manifest file into cache", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(cached_manifest_file_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ManifestFile is empty", K(ret));
    } else if (OB_FAIL(manifest_files.reserve(cached_manifest_file_.size()))) {
      LOG_WARN("failed to reserve manifest file", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cached_manifest_file_.size(); i++) {
        OZ(manifest_files.push_back(cached_manifest_file_.at(i)));
      }
    }
  }
  return ret;
}

int Snapshot::get_manifest_files_(const ObString &access_info,
                                 ObIArray<ManifestFile *> &manifest_files) const
{
  int ret = OB_SUCCESS;
  if (!v1_manifests.empty()) {
    // handle manifest v1
    for (int i = 0; OB_SUCC(ret) && i < v1_manifests.size(); ++i) {
      ManifestFile *manifest_file = OB_NEWx(ManifestFile, &allocator_, allocator_);
      if (OB_ISNULL(manifest_file)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate manifest file", K(ret));
      } else {
        manifest_file->manifest_length = 0;
        manifest_file->partition_spec_id = 0;
        manifest_file->content = ManifestContent::DATA;
        manifest_file->sequence_number = 0L;
        manifest_file->min_sequence_number = 0L;
        manifest_file->added_snapshot_id = snapshot_id;
        OZ(ob_write_string(allocator_, v1_manifests.at(i), manifest_file->manifest_path));
        OZ(manifest_files.push_back(manifest_file));
      }
    }
  } else {
    ObArenaAllocator tmp_allocator;
    char *buf = NULL;
    int64_t read_size = 0;
    std::unique_ptr<avro::InputStream> input_stream;
    if (OB_FAIL(ObIcebergFileIOUtils::read(tmp_allocator,
                                           manifest_list,
                                           access_info,
                                           buf,
                                           read_size))) {
      LOG_WARN("fail to read manifest-list", K(ret));
    } else if (OB_FALSE_IT(input_stream = avro::memoryInputStream(reinterpret_cast<uint8_t *>(buf),
                                                                  read_size))) {
    } else {
      try {
        avro::DataFileReader<avro::GenericDatum> avro_reader(std::move(input_stream));
        avro::GenericDatum generic_datum(avro_reader.dataSchema());
        while (OB_SUCC(ret) && avro_reader.read(generic_datum)) {
          ManifestFile *manifest_file = OB_NEWx(ManifestFile, &allocator_, allocator_);
          if (OB_ISNULL(manifest_file)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate manifest file", K(ret));
          } else if (avro::Type::AVRO_RECORD != generic_datum.type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid manifest file", K(ret), K(generic_datum.type()));
          } else if (OB_FAIL(manifest_file->init_from_avro(
                         generic_datum.value<avro::GenericRecord>()))) {
            LOG_WARN("failed to init manifest file", K(ret));
          } else if (OB_FAIL(manifest_files.push_back(manifest_file))) {
            LOG_WARN("failed to add manifest file", K(ret));
          }
        }
      } catch (std::exception &e) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to read manifest file", K(ret), K(e.what()));
      }
    }
  }

  return ret;
}

// 不会使用的代码，先留着，可能后面可以用于写 UT
// int Snapshot::plan_files(ObIAllocator &allocator,
//                          const ObString &access_info,
//                          ObIArray<FileScanTask *> &file_scan_tasks) const
// {
//   int ret = OB_SUCCESS;
//   ObArray<const ManifestEntry *> data_files;
//   ObArray<const ManifestEntry *> delete_files;
//   ObArray<ManifestFile *> manifest_files;
//   if (OB_FAIL(get_manifest_files(allocator, access_info, manifest_files))) {
//     LOG_WARN("failed to get manifest files", K(ret));
//   } else {
//     for (int64_t i = 0; OB_SUCC(ret) && i < manifest_files.size(); ++i) {
//       const ManifestFile *manifest_file = manifest_files[i];
//       LOG_INFO("get manifest file",
//                K(manifest_file->manifest_path),
//                K(manifest_file->manifest_length));
//
//       ObArray<ManifestEntry *> manifest_entries;
//       if (OB_FAIL(manifest_file->get_manifest_entries(allocator, access_info, manifest_entries))) {
//         LOG_WARN("failed to get manifest entries", K(ret), K(manifest_file->manifest_path));
//       } else {
//         for (int j = 0; OB_SUCC(ret) && j < manifest_entries.size(); ++j) {
//           ManifestEntry *manifest_entry = manifest_entries[j];
//           if (DataFileContent::DATA == manifest_entry->data_file.content) {
//             OZ(data_files.push_back(manifest_entry));
//           } else {
//             OZ(delete_files.push_back(manifest_entry));
//           }
//         }
//       }
//     }
//   }
//
//   if (OB_SUCC(ret)) {
//     DeleteFileIndex delete_file_index;
//     if (OB_FAIL(delete_file_index.init(delete_files))) {
//       LOG_WARN("failed to init delete", K(ret));
//     }
//
//     for (int i = 0; OB_SUCC(ret) && i < data_files.count(); i++) {
//       const ManifestEntry *data_file_entry = data_files[i];
//       FileScanTask *file_scan_task = OB_NEWx(FileScanTask, &allocator, allocator);
//       if (OB_ISNULL(file_scan_task)) {
//         ret = OB_ALLOCATE_MEMORY_FAILED;
//         LOG_WARN("failed to allocate file_scan_task", K(ret));
//       } else {
//         file_scan_task->start = 0; // todo
//         file_scan_task->length = data_file_entry->data_file.file_size_in_bytes;
//         file_scan_task->data_file_content = data_file_entry->data_file.content;
//         file_scan_task->data_file_format = data_file_entry->data_file.file_format;
//         OZ(ob_write_string(allocator,
//                            data_file_entry->data_file.file_path,
//                            file_scan_task->data_file_path));
//         OZ(file_scan_tasks.push_back(file_scan_task));
//         OZ(delete_file_index.match_delete_files(*data_file_entry,
//                                                 file_scan_task->pos_delete_files,
//                                                 file_scan_task->eq_delete_files,
//                                                 file_scan_task->dv_files));
//       }
//     }
//   }
//
//   if (OB_SUCC(ret)) {
//     FOREACH_CNT_X(file_scan_task, file_scan_tasks, OB_SUCC(ret))
//     {
//       LOG_INFO("file scan task: ",
//                K((*file_scan_task)->data_file_path),
//                K((*file_scan_task)->pos_delete_files),
//                K((*file_scan_task)->eq_delete_files),
//                K((*file_scan_task)->dv_files));
//     }
//   }
//   return ret;
// }

} // namespace iceberg
} // namespace sql
} // namespace oceanbase
