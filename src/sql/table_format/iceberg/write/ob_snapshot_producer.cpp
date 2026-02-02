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

#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "share/ob_define.h"
#include "share/ob_server_struct.h"
#include "sql/table_format/iceberg/spec/snapshot.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/write/ob_snapshot_producer.h"
#include "sql/table_format/iceberg/write/ob_manifest_writer.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/catalog/hive/ob_hms_catalog.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/catalog/ob_catalog_meta_getter.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"


#include <avro/Compiler.hh>
#include <avro/GenericDatum.hh>
#include <avro/ValidSchema.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

ObSnapshotProducer::~ObSnapshotProducer()
{
  if (catalog_meta_getter_ != nullptr) {
    catalog_meta_getter_->~ObCatalogMetaGetter();
    catalog_meta_getter_ = nullptr;
  }
  for (int64_t i = 0; i < manifest_files_.size(); i++) {
    manifest_files_.at(i)->~ManifestFile();
  }
}

int ObSnapshotProducer::init()
{
  int ret = OB_SUCCESS;
  int64_t high = 0;
  int64_t low = 0;
  unsigned char uuid_bin[UuidCommon::BYTE_LENGTH] = {0};
  share::schema::ObSchemaGetterGuard schema_guard;

  // gen commit_uuid and snapshot_id
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObExprUuid::gen_server_uuid(commit_uuid_, UuidCommon::LENGTH_UUID))) {
    LOG_WARN("failed to gen server uuid", K(ret));
  } else if (OB_FAIL(ObExprUuid::gen_uuid_bin(uuid_bin))) {
    LOG_WARN("failed to gen uuid bin", K(ret));
  } else {
    for (int i = 0; i < 8; ++i) {
      high = (high << 8) | uuid_bin[i];
    }
    for (int i = 8; i < 16; ++i) {
      low = (low << 8) | uuid_bin[i];
    }
    snapshot_id_ = (high ^ low) & INT64_MAX;
  }

  // get catalog
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get schema service failed", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(iceberg_table_metadata_.tenant_id_,
                                                                     schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret));
    } else if (OB_ISNULL(catalog_meta_getter_ = OB_NEWx(share::ObCatalogMetaGetter,
                                                        &allocator_,
                                                        schema_guard,
                                                        allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for catalog_meta_getter", K(ret));
    } else if (catalog_meta_getter_->get_catalog(iceberg_table_metadata_.tenant_id_,
                                                 iceberg_table_metadata_.catalog_id_,
                                                 catalog_)) {
      LOG_WARN("failed to get catalog", K(ret));
    } else if (OB_ISNULL(catalog_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("catalog is nullptr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObSnapshotProducer::produce_manifest_files(const Snapshot* parent_snapshot)
{
  int ret = OB_SUCCESS;
  // todo: hard code partition spec id now
  OZ(generate_new_manifest_file(0));
  if (OB_SUCC(ret) && parent_snapshot != nullptr) {
    ObArray<ManifestFile *> old_manifest_files;
    Snapshot* tmp_snapshot = const_cast<Snapshot*>(parent_snapshot);
    OZ(tmp_snapshot->get_manifest_files(iceberg_table_metadata_.access_info_,
                                        old_manifest_files));
    for (int64_t i = 0; OB_SUCC(ret) && i < old_manifest_files.count(); ++i) {
      OZ(manifest_files_.push_back(old_manifest_files.at(i)));
    }
  }
  return ret;
}

int ObSnapshotProducer::generate_new_manifest_file(int32_t partition_spec_id)
{
  int ret = OB_SUCCESS;
  ManifestFile* manifest_file = OB_NEWx(ManifestFile, &allocator_, allocator_);
  ObSqlString manifest_path;
  if (OB_ISNULL(manifest_file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for manifest_file", K(ret));
  } else {
    manifest_file->content = ManifestContent::DATA;
    manifest_file->partition_spec_id = partition_spec_id;
    manifest_file->sequence_number = base_table_metadata_->get_next_sequence_number();
    manifest_file->min_sequence_number = manifest_file->sequence_number;
    manifest_file->added_snapshot_id = snapshot_id_;
    manifest_file->added_files_count = data_files_.count();
    manifest_file->existing_files_count = 0;
    manifest_file->existing_rows_count = 0;
    manifest_file->deleted_files_count = 0;
    manifest_file->deleted_rows_count = 0;

    OZ(manifest_path.append(base_table_metadata_->location));
    OZ(manifest_path.append("/metadata/"));
    OZ(manifest_path.append(commit_uuid_, UuidCommon::LENGTH_UUID));
    OZ(manifest_path.append("-m0.avro")); //todo: hard code now
    OZ(ob_write_string(allocator_, manifest_path.ptr(), manifest_file->manifest_path, true));

    int added_rows_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < data_files_.count(); ++i) {
      CK(OB_NOT_NULL(data_files_.at(i)));
      OX(added_rows_count += data_files_.at(i)->record_count);
    }
    manifest_file->added_rows_count = added_rows_count;
    OZ(write_manifest_file(*manifest_file));

    OZ(manifest_files_.push_back(manifest_file));
  }
  return ret;
}

int ObSnapshotProducer::write_manifest_file(ManifestFile& manifest_file)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObManifestFileWriter* manifest_writer = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_files_.count(); ++i) {
    ManifestEntry manifest_entry(tmp_allocator);
    manifest_entry.data_file.shallow_copy(*data_files_.at(i));
    manifest_entry.status = ManifestEntryStatus::ADDED;
    manifest_entry.snapshot_id = snapshot_id_;
    // spark iceberg use inherit
    manifest_entry.sequence_number = manifest_file.sequence_number;
    manifest_entry.file_sequence_number = manifest_file.sequence_number;

    if (i == 0) {
      OX(manifest_writer = OB_NEWx(ObManifestFileWriter, &tmp_allocator,
                                   manifest_file.manifest_path,
                                   iceberg_table_metadata_.access_info_,
                                   base_table_metadata_->format_version,
                                   base_table_metadata_->current_schema_id,
                                   base_table_metadata_->current_schema_str));
      CK(OB_NOT_NULL(manifest_writer));
      OZ(manifest_writer->open_file());
    }
    OZ(manifest_writer->write_manifest_entry(manifest_entry));
  }
  OZ(manifest_writer->close());
  OX(manifest_file.manifest_length = manifest_writer->file_size());
  OX(manifest_writer->~ObManifestFileWriter());
  return ret;
}

int ObSnapshotProducer::produce_snapshot(Snapshot*& new_snapshot)
{
  int ret = OB_SUCCESS;
  const Snapshot* current_snapshot = nullptr;
  ObSqlString manifest_list_path;
  ObArenaAllocator tmp_allocator;
  ObManifestListWriter* manifest_writer = nullptr;
  new_snapshot = OB_NEWx(Snapshot, &allocator_, allocator_);
  if (OB_ISNULL(new_snapshot)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for new snapshot", K(ret));
  }

  if (base_table_metadata_->current_snapshot_id != -1) {
    OZ(base_table_metadata_->get_current_snapshot(current_snapshot));
    CK(OB_NOT_NULL(current_snapshot));
    OX(new_snapshot->parent_snapshot_id = current_snapshot->snapshot_id);
  }

  if (OB_SUCC(ret)) {
    new_snapshot->sequence_number = base_table_metadata_->get_next_sequence_number();
    new_snapshot->timestamp_ms = ObTimeUtility::current_time_ms();
    new_snapshot->schema_id = base_table_metadata_->current_schema_id;
    new_snapshot->snapshot_id = snapshot_id_;
    // todo: add more info to the new snapshot summary
    OZ(new_snapshot->summary.init(1));
    OZ(ObIcebergUtils::set_string_map(allocator_, "operation", "append", new_snapshot->summary));

    OZ(manifest_list_path.append(base_table_metadata_->location));
    OZ(manifest_list_path.append("/metadata/snap-"));
    OZ(manifest_list_path.append(std::to_string(snapshot_id_).c_str()));
    OZ(manifest_list_path.append("-1-")); //todo: hard code now
    OZ(manifest_list_path.append(commit_uuid_, UuidCommon::LENGTH_UUID));
    OZ(manifest_list_path.append(".avro"));
    OZ(ob_write_string(allocator_, manifest_list_path.ptr(), new_snapshot->manifest_list, true));
  }

  OZ(produce_manifest_files(current_snapshot));
  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_files_.count(); ++i) {
    CK(OB_NOT_NULL(manifest_files_.at(i)));
    if (i == 0) {
      OX(manifest_writer = OB_NEWx(ObManifestListWriter, &tmp_allocator,
                                   new_snapshot->manifest_list,
                                   iceberg_table_metadata_.access_info_,
                                   base_table_metadata_->format_version,
                                   base_table_metadata_->current_schema_id,
                                   new_snapshot->parent_snapshot_id,
                                   new_snapshot->snapshot_id,
                                   new_snapshot->sequence_number));
      CK(OB_NOT_NULL(manifest_writer));
      OZ(manifest_writer->open_file());
    }
    OZ(manifest_writer->write_manifest_file(*manifest_files_.at(i)));
  }
  OZ(manifest_writer->close());
  OX(manifest_writer->~ObManifestListWriter());
  return ret;
}

int ObSnapshotProducer::produce_metadata(TableMetadata*& new_metadata, Snapshot* new_snapshot)
{
  int ret = OB_SUCCESS;
  new_metadata = OB_NEWx(TableMetadata, &allocator_, allocator_);
  if (OB_ISNULL(new_metadata)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for new metadata", K(ret));
  } else if (OB_FAIL(new_metadata->assign(*base_table_metadata_, 1 /*extra_snapshot_capacity*/))) {
    LOG_WARN("failed to assign base table metadata", K(ret));
  } else if (OB_FAIL(new_metadata->add_snapshot(new_snapshot))) {
    LOG_WARN("failed to add snapshot", K(ret));
  }
  return ret;
}

int ObSnapshotProducer::filesystem_catalog_commit(const TableMetadata* new_metadata)
{
  int ret = OB_SUCCESS;
  ObSqlString metadata_path;
  ObSqlString version_path;
  int64_t version_number = -1;
  CK(OB_NOT_NULL(new_metadata));
  OZ(iceberg_table_metadata_.get_current_version(allocator_, new_metadata->location, version_number));
  OZ(metadata_path.append(new_metadata->location));
  OZ(metadata_path.append("/metadata/v"));
  OZ(metadata_path.append(std::to_string(version_number + 1).c_str()));
  OZ(metadata_path.append(".metadata.json")); //todo: use codec in properties to set file extension
  OZ(version_path.append(new_metadata->location));
  OZ(version_path.append("/metadata/version-hint.text"));
  ObString metadata_path_str = metadata_path.string();
  ObString version_path_str = version_path.string();
  ObMetadataWriter* metadata_writer = OB_NEWx(ObMetadataWriter, &allocator_,
                                              metadata_path_str, iceberg_table_metadata_.access_info_);
  ObVersionHintWriter* version_writer = OB_NEWx(ObVersionHintWriter, &allocator_,
                                                version_path_str, iceberg_table_metadata_.access_info_);

  if (OB_ISNULL(metadata_writer) || OB_ISNULL(version_writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for writer", K(ret));
  } else {
    OZ(metadata_writer->open_file());
    OZ(metadata_writer->write_metadata(*new_metadata, allocator_));
    OZ(metadata_writer->close());
    OX(metadata_writer->~ObMetadataWriter());

    OZ(version_writer->open_file());
    OZ(version_writer->overwrite_version(version_number + 1));
    OZ(version_writer->close());
    OX(version_writer->~ObVersionHintWriter());
  }
  return ret;
}

int ObSnapshotProducer::hms_catalog_commit(const TableMetadata* new_metadata)
{
  int ret = OB_SUCCESS;
  share::ApacheHive::Table table;
  share::ObHMSCatalog* hms_catalog = nullptr;
  ObSqlString metadata_path;
  char commit_uuid[UuidCommon::LENGTH_UUID] = {0};
  int new_version = 1; // todo: parse new version from base_metadata_location
  CK(OB_NOT_NULL(new_metadata));
  OZ(metadata_path.append(new_metadata->location));
  OZ(metadata_path.append("/metadata/")); //todo: hms catalog can use "write.metadata.path" in properties to relocate metadata
  OZ(ObExprUuid::gen_server_uuid(commit_uuid, UuidCommon::LENGTH_UUID));
  OZ(metadata_path.append_fmt("%05d-%.*s", new_version, UuidCommon::LENGTH_UUID, commit_uuid));
  OZ(metadata_path.append(".metadata.json")); //todo: use "write.metadata.compression-codec" in properties to set file extension
  ObString metadata_path_str = metadata_path.string();
  ObMetadataWriter* metadata_writer = OB_NEWx(ObMetadataWriter, &allocator_,
                                              metadata_path_str, iceberg_table_metadata_.access_info_);
  if (OB_ISNULL(metadata_writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for writer", K(ret));
  } else {
    OZ(metadata_writer->open_file());
    OZ(metadata_writer->write_metadata(*new_metadata, allocator_));
    OZ(metadata_writer->close());
    OX(metadata_writer->~ObMetadataWriter());
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(hms_catalog = static_cast<share::ObHMSCatalog*>(catalog_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hms catalog is nullptr", K(ret));
    } else if (OB_FAIL(hms_catalog->fetch_latest_table(iceberg_table_metadata_.namespace_name_,
                                                       iceberg_table_metadata_.table_name_,
                                                       iceberg_table_metadata_.case_mode_,
                                                       table))) {
      LOG_WARN("failed to fetch latest table", K(ret), K(iceberg_table_metadata_.namespace_name_),
               K(iceberg_table_metadata_.table_name_));
    } else if (OB_UNLIKELY(table.parameters.find(share::ObHMSCatalog::TABLE_TYPE)
                           == table.parameters.end())
               || OB_UNLIKELY(table.parameters[share::ObHMSCatalog::TABLE_TYPE]
                              != "ICEBERG")
               || OB_UNLIKELY(table.parameters.find(share::ObHMSCatalog::ICEBERG_METADATA_LOCATION)
                              == table.parameters.end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table parameter", K(ret));
    } else if (table.parameters[share::ObHMSCatalog::ICEBERG_METADATA_LOCATION]
               != std::string(base_table_metadata_->metadata_file_location.ptr(),
                              base_table_metadata_->metadata_file_location.length())) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_USER_ERROR(OB_ERROR_DURING_COMMIT, "base metadata location is not same as the current table metadata location");
      LOG_WARN("base metadata location is not same as the current table metadata location",
               K(base_table_metadata_->metadata_file_location),
               K(table.parameters[share::ObHMSCatalog::ICEBERG_METADATA_LOCATION].c_str()));
    }
  }
  if (OB_SUCC(ret)) {
    table.parameters[share::ObHMSCatalog::TABLE_TYPE] = "ICEBERG";
    table.parameters[share::ObHMSCatalog::ICEBERG_METADATA_LOCATION] =
                                  std::string(metadata_path_str.ptr(), metadata_path_str.length());
    OZ(hms_catalog->alter_table_with_lock(iceberg_table_metadata_.namespace_name_,
                                          iceberg_table_metadata_.table_name_,
                                          iceberg_table_metadata_.case_mode_,
                                          table));
  }
  return ret;
}

int ObSnapshotProducer::refresh()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  share::ObILakeTableMetadata *lake_table_metadata = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("get schema service failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(iceberg_table_metadata_.tenant_id_,
                                                                   schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else {
    share::ObCachedCatalogMetaGetter catalog_meta_getter{schema_guard, tmp_allocator};
    if (OB_FAIL(catalog_meta_getter.fetch_lake_table_metadata(allocator_,
                                                              iceberg_table_metadata_.tenant_id_,
                                                              iceberg_table_metadata_.catalog_id_,
                                                              iceberg_table_metadata_.database_id_,
                                                              iceberg_table_metadata_.table_id_,
                                                              iceberg_table_metadata_.namespace_name_,
                                                              iceberg_table_metadata_.table_name_,
                                                              iceberg_table_metadata_.case_mode_,
                                                              lake_table_metadata))) {
      LOG_WARN("failed to fetch_lake_table_metadata", K(ret));
    } else if (OB_ISNULL(lake_table_metadata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null lake_table_metadata", K(ret));
    } else if (lake_table_metadata->get_format_type() != share::ObLakeTableFormat::ICEBERG) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format type", K(ret), "format_type", lake_table_metadata->get_format_type());
    } else {
      base_table_metadata_ = &(static_cast<ObIcebergTableMetadata*>(lake_table_metadata)->table_metadata_);
    }
  }
  return ret;
}

int ObSnapshotProducer::catalog_commit(const TableMetadata* new_metadata)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(catalog_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (catalog_->get_catalog_type() == share::ObCatalogProperties::CatalogType::FILESYSTEM_TYPE) {
    ret = filesystem_catalog_commit(new_metadata);
  } else if (catalog_->get_catalog_type() == share::ObCatalogProperties::CatalogType::HMS_TYPE) {
    ret = hms_catalog_commit(new_metadata);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported catalog type", K(ret), K(catalog_->get_catalog_type()));
  }
  return ret;
}

int ObSnapshotProducer::commit()
{
  int ret = OB_SUCCESS;
  Snapshot *new_snapshot = nullptr;
  TableMetadata* new_metadata = nullptr;
  try {
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(refresh())) {
      LOG_WARN("failed to refresh", K(ret));
    } else if (OB_FAIL(produce_snapshot(new_snapshot))) {
      LOG_WARN("failed to produce snapshot", K(ret));
    } else if (OB_FAIL(produce_metadata(new_metadata, new_snapshot))) {
      LOG_WARN("failed to produce metadata", K(ret));
    } else if (OB_FAIL(catalog_commit(new_metadata))) {
      LOG_WARN("failed to write metadata", K(ret));
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when committing", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERROR_DURING_COMMIT, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when committing", K(ret));
    }
  }
  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase