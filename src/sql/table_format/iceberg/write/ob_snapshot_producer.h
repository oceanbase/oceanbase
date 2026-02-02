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

#ifndef SNAPSHOT_PRODUCER_H
#define SNAPSHOT_PRODUCER_H

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"

namespace oceanbase
{

namespace common
{
class ObString;
}

namespace share
{
class ObCatalogMetaGetter;
}

namespace sql
{

namespace iceberg
{

class ObSnapshotProducer
{
public:
  ObSnapshotProducer(const ObIcebergTableMetadata& iceberg_table_metadata,
                     ObIAllocator& allocator,
                     const ObArray<DataFile*>& data_files)
    : iceberg_table_metadata_(iceberg_table_metadata),
      base_table_metadata_(&iceberg_table_metadata.table_metadata_),
      allocator_(allocator),
      manifest_files_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      data_files_(data_files) {}
  ~ObSnapshotProducer();
  int init();
  int commit();

private:
  int generate_new_manifest_file(int32_t partition_spec_id);
  int write_manifest_file(ManifestFile& manifest_file);
  int produce_snapshot(Snapshot*& new_snapshot);
  int produce_manifest_files(const Snapshot* parent_snapshot);
  int produce_metadata(TableMetadata*& new_metadata, Snapshot* new_snapshot);
  int catalog_commit(const TableMetadata* new_metadata);
  int filesystem_catalog_commit(const TableMetadata* new_metadata);
  int hms_catalog_commit(const TableMetadata* new_metadata);
  int refresh();

  const ObIcebergTableMetadata& iceberg_table_metadata_;
  const TableMetadata* base_table_metadata_;
  ObIAllocator& allocator_;
  share::ObCatalogMetaGetter* catalog_meta_getter_ = nullptr;
  share::ObIExternalCatalog* catalog_ = nullptr;
  int64_t snapshot_id_ = 0;
  char commit_uuid_[UuidCommon::LENGTH_UUID] = {0};
  bool is_inited_ = false;
  ObArray<ManifestFile*> manifest_files_;
  const ObArray<DataFile*>& data_files_;
  DISALLOW_COPY_AND_ASSIGN(ObSnapshotProducer);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // SNAPSHOT_PRODUCER_H