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

#define USING_LOG_PREFIX SQL_OPT

#include "ob_lake_table_partition_info.h"

#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_iceberg_table_schema.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/engine/basic/ob_consistent_hashing_load_balancer.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "sql/optimizer/file_prune/ob_hive_file_pruner.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::iceberg;

namespace sql
{

int ObLakeTablePartitionInfo::assign(const ObTablePartitionInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObLakeTablePartitionInfo &info = static_cast<const ObLakeTablePartitionInfo&>(other);
    if (OB_FAIL(ObTablePartitionInfo::assign(other))) {
      LOG_WARN("failed to assign table partition info");
    } else {
      file_pruner_ = NULL;
      if (info.file_pruner_->type_ == PrunnerType::ICEBERG) {
        file_pruner_ = OB_NEWx(ObIcebergFilePrunner, &allocator_, allocator_);
      } else if (info.file_pruner_->type_ == PrunnerType::HIVE) {
        file_pruner_ = OB_NEWx(ObHiveFilePruner, &allocator_, allocator_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pruner type");
      }

      if (OB_FAIL(ret)) {
      } else if (file_pruner_ == NULL) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for file_pruner");
      } else if (OB_FAIL(file_pruner_->assign(*info.file_pruner_))) {
        LOG_WARN("failed to assign file prunner");
      } else {
        is_hash_aggregate_ = info.is_hash_aggregate_;
        hash_count_ = info.hash_count_;
        first_bucket_partition_value_offset_ = info.first_bucket_partition_value_offset_;
      }
    }
  }

  return ret;
}

int get_manifest_entries(const ObString &access_info,
                         ObIArray<iceberg::ManifestFile*> &manifest_files,
                         ObIArray<iceberg::ManifestEntry*> &manifest_entries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < manifest_files.count(); ++i) {
    if (OB_ISNULL(manifest_files.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else if (OB_FAIL(manifest_files.at(i)->get_manifest_entries(access_info, manifest_entries))) {
      LOG_WARN("failed to get manifest entries");
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::prune_file_and_select_location(ObSqlSchemaGuard &sql_schema_guard,
                                                             const ObDMLStmt &stmt,
                                                             ObExecContext *exec_ctx,
                                                             const uint64_t table_id,
                                                             const uint64_t ref_table_id,
                                                             const ObIArray<ObRawExpr*> &filter_exprs)
{
  int ret = OB_SUCCESS;
  ObILakeTableMetadata *lake_table_metadata = nullptr;
  if (OB_FAIL(sql_schema_guard.get_lake_table_metadata(ref_table_id, lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ref_table_id));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lake table metadata", KP(lake_table_metadata));
  } else if (share::ObLakeTableFormat::ICEBERG == lake_table_metadata->get_format_type()) {
    ObIcebergTableMetadata *iceberg_table_metadata
        = down_cast<ObIcebergTableMetadata *>(lake_table_metadata);
    const ObString &access_info = iceberg_table_metadata->access_info_;
    ObSEArray<iceberg::ManifestFile*, 16> all_manifest_files;
    ObSEArray<iceberg::ManifestFile*, 16> manifest_files;
    ObSEArray<iceberg::ManifestEntry*, 16> manifest_entries;
    hash::ObHashMap<ObLakeTablePartKey, uint64_t> part_key_map;
    iceberg::Snapshot *current_snapshot = NULL;
    if (OB_FAIL(iceberg_table_metadata->table_metadata_.get_current_snapshot(current_snapshot))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        // do nothing
        // 空表
        current_snapshot = NULL;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get current snapshot", K(ret));
      }
    } else if (OB_ISNULL(current_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null snapshot");
    }

    ObIcebergFilePrunner *iceberg_file_pruner = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(iceberg_file_pruner = OB_NEWx(ObIcebergFilePrunner, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObIcebergFilePrunner", K(ret));
    } else if (OB_FAIL(iceberg_file_pruner->init(&sql_schema_guard,
                                                 stmt,
                                                 exec_ctx,
                                                 table_id,
                                                 ref_table_id,
                                                 iceberg_table_metadata->table_metadata_.partition_specs,
                                                 filter_exprs))) {
      LOG_WARN("failed to init table location", K(ret));
    } else if (NULL == current_snapshot) {
      // do nothing
      // 空表
    } else if (OB_FAIL(current_snapshot->get_manifest_files(access_info, all_manifest_files))) {
      LOG_WARN("failed to get manifest files");
    } else if (all_manifest_files.empty()) {
      // do nothing
    } else if (OB_FAIL(iceberg_file_pruner->prune_manifest_files(all_manifest_files, manifest_files))) {
      LOG_WARN("failed to prune manifest files");
    // 解析出的 ManifestEntry 裁剪之后还要用来获取统计信息，因此使用类的成员 allocator 生成。
    } else if (manifest_files.empty()) {
      // do nothing
    } else if (OB_FAIL(get_manifest_entries(access_info,
                                            manifest_files,
                                            manifest_entries))) {
      LOG_WARN("failed to get manifest entries");
    } else if (manifest_entries.empty()) {
      // do nothing
    } else if (OB_FAIL(check_iceberg_use_hash_part(
                   iceberg_table_metadata->table_metadata_.partition_specs,
                   first_bucket_partition_value_offset_))) {
      LOG_WARN("failed to check iceberg use hash part");
    } else if (OB_FAIL(iceberg_file_pruner->prune_data_files(*exec_ctx,
                                                             manifest_entries,
                                                             is_hash_aggregate(),
                                                             part_key_map,
                                                             iceberg_file_descs_))) {
      LOG_WARN("failed to prune data files");
    }

    // 这里不能接上面的 else if, 否则 manifest_files 为空的情况会出 bug
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(select_location_for_iceberg(exec_ctx, part_key_map, iceberg_file_descs_))) {
      LOG_WARN("failed to select location for iceberg");
    } else {
      candi_table_loc_.set_table_location_key(iceberg_file_pruner->get_table_id(), iceberg_file_pruner->get_ref_table_id());
      candi_table_loc_.set_is_lake_table(true);
      file_pruner_ = iceberg_file_pruner;
    }
    if (part_key_map.created()) {
      int tmp_ret = part_key_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destroy part key map", K(tmp_ret));
      }
    }
  } else if (share::ObLakeTableFormat::HIVE == lake_table_metadata->get_format_type()) {
    ObHiveFilePruner *hive_file_pruner = NULL;
    ObArray<ObHiveFileDesc> hive_files(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_));
    if (OB_ISNULL(hive_file_pruner = OB_NEWx(ObHiveFilePruner, &allocator_, allocator_))) {
      LOG_WARN("failed to allocate memory for ObHiveFilePrunner", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(hive_file_pruner->init(sql_schema_guard,
                                               stmt,
                                               exec_ctx,
                                               table_id,
                                               ref_table_id,
                                               filter_exprs))) {
      LOG_WARN("failed to init hive file prunner", K(ret));
    } else if (OB_FAIL(hive_file_pruner->prunner_files(*exec_ctx, hive_files))) {
      LOG_WARN("failed to init hive table location", K(ret));
    } else if (OB_FAIL(select_location_for_hive(exec_ctx, hive_files))) {
      LOG_WARN("failed to select location for hive");
    } else {
      candi_table_loc_.set_table_location_key(hive_file_pruner->get_table_id(),
                                              hive_file_pruner->get_ref_table_id());
      candi_table_loc_.set_is_lake_table(true);
      file_pruner_ = hive_file_pruner;
    }

  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get unsupported lake format");
  }
  return ret;
}

int ObLakeTablePartitionInfo::check_iceberg_use_hash_part(const ObIArray<iceberg::PartitionSpec*> &partition_specs,
                                                          int64_t &offset)
{
  int ret = OB_SUCCESS;
  iceberg::PartitionSpec* part_spec = nullptr;
  int64_t hash_count = 0;
  offset = -1;
  if (partition_specs.count() != 1) {
    // do nothing
  } else if (OB_ISNULL(part_spec = partition_specs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null partition spec");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (-1 == offset) && i < part_spec->fields.count(); ++i) {
      if (OB_ISNULL(part_spec->fields.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null field");
      } else if (part_spec->fields.at(i)->transform.transform_type == iceberg::TransformType::Bucket) {
        hash_count = part_spec->fields.at(i)->transform.param.value();
        offset = i;
      }
    }
    if (OB_SUCC(ret) && offset > -1) {
      if (OB_UNLIKELY(hash_count == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected hash count");
      } else {
        set_is_hash_aggregate(true);
        set_hash_count(hash_count);
      }
    }
  }
  return ret;
}


int ObLakeTablePartitionInfo::select_location_for_iceberg(ObExecContext *exec_ctx,
                                                          hash::ObHashMap<ObLakeTablePartKey, uint64_t> &part_key_map,
                                                          ObIArray<ObIcebergFileDesc*> &file_descs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> all_servers;
  ObIArray<ObCandiTabletLoc> &candi_tablet_locs = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  candi_tablet_locs.reset();
  ObDefaultLoadBalancer load_balancer;
  ObAddr addr;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_UNLIKELY(file_descs.empty())) {
    ObCandiTabletLoc* tablet_loc = nullptr;
    uint64_t part_id = 0;
    if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
    } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, GCTX.self_addr(), part_id))) {
      LOG_WARN("failed to init tablet loc by addr");
    // } else if (OB_FAIL(add_table_file(*tablet_loc, file_desc))) {
    //   LOG_WARN("failed to add table file");
    }
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(exec_ctx->get_my_session()->get_effective_tenant_id(),
                                                                all_servers))) {
    LOG_WARN("fail to get external table location");
  } else if (OB_FAIL(load_balancer.add_server_list(all_servers))) {
    LOG_WARN("failed to add server list");
  } else if (is_hash_aggregate()) {
    /* 将分区按照bucket分区定义划分
     * 考虑到存在partition by (c1, bucket(c2), 4) 的场景，需要把bucket_idx相同，但是c1值不同的分区居合道一起，
     * 因此使用了一个 hash map 记录是否已经为 bucket_idx 生成过 tablet loc，使用一个 hash map 记录每个iceberg
     * part idx 映射的 tablet loc。
    */
    // iceberg part idx -> tablet loc idx
    hash::ObHashMap<int64_t, int64_t> part_idx_map;
    // bucket idx -> tablet loc idx
    hash::ObHashMap<int64_t, int64_t> bucket_idx_map;
    if (OB_FAIL(part_idx_map.create(part_key_map.size(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create part idx map");
    } else if (OB_FAIL(bucket_idx_map.create(all_servers.count(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create bucket idx map");
    }
    hash::ObHashMap<ObLakeTablePartKey, uint64_t>::const_iterator iter = part_key_map.begin();
    for (; OB_SUCC(ret) && iter != part_key_map.end(); ++iter) {
      int32_t bucket_idx = -1;
      int64_t tablet_loc_idx = -1;
      if (OB_FAIL(get_bucket_idx(iter->first, first_bucket_partition_value_offset_, bucket_idx))) {
        LOG_WARN("failed to get hash part idx");
      } else if (OB_FAIL(bucket_idx_map.get_refactored(bucket_idx, tablet_loc_idx))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          tablet_loc_idx = candi_tablet_locs.count();
          ObCandiTabletLoc* tablet_loc = candi_tablet_locs.alloc_place_holder();
          if (OB_ISNULL(tablet_loc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
          } else if (OB_FAIL(bucket_idx_map.set_refactored(bucket_idx, tablet_loc_idx))) {
            LOG_WARN("failed to set bucket idx map");
          } else if (OB_FAIL(load_balancer.select_server(bucket_idx, addr))) {
            LOG_WARN("failed to select server");
          } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, static_cast<uint64_t>(bucket_idx)))) {
            LOG_WARN("failed to init tablet loc by addr");
          }
        } else {
          LOG_WARN("failed to get tablet loc");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_idx_map.set_refactored(iter->second, tablet_loc_idx))) {
        LOG_WARN("failed to set part idx map");
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      int64_t tablet_loc_idx = -1;
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      if (OB_ISNULL(file_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null file desc");
      } else if (OB_FAIL(part_idx_map.get_refactored(file_desc->part_idx_, tablet_loc_idx))) {
        LOG_WARN("failed to get tablet loc");
      } else if (OB_UNLIKELY(tablet_loc_idx < 0 || tablet_loc_idx >= candi_tablet_locs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(tablet_loc_idx));
      } else if (OB_FAIL(add_table_file(candi_tablet_locs.at(tablet_loc_idx), file_desc))) {
        LOG_WARN("failed to add table file");
      }
    }

    if (part_idx_map.created()) {
      int tmp_ret = part_idx_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory part idx map", K(tmp_ret));
      }
    }
    if (bucket_idx_map.created()) {
      int tmp_ret = bucket_idx_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory bucket idx map", K(tmp_ret));
      }
    }
  } else {
    uint64_t last_part_id = 0;
    hash::ObHashMap<ObAddr, int64_t> tablet_loc_map;
    if (OB_FAIL(tablet_loc_map.create(all_servers.count(), "TabeltLocMap", "LakeTableLoc"))) {
      LOG_WARN("failed to create tablet loc map");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      int64_t idx = -1;
      if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null file desc");
      } else if (OB_FAIL(load_balancer.select_server(file_desc->entry_->data_file.file_path, addr))) {
        LOG_WARN("failed to select server");
      } else if (OB_FAIL(tablet_loc_map.get_refactored(addr, idx))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
          idx = candi_tablet_locs.count();
          ObCandiTabletLoc* tablet_loc = candi_tablet_locs.alloc_place_holder();
          if (OB_ISNULL(tablet_loc)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
          } else if (OB_FAIL(tablet_loc_map.set_refactored(addr, idx))) {
            LOG_WARN("failed to set tablet loc map");
          } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, ++last_part_id))) {
            LOG_WARN("failed to init tablet loc by addr");
          }
        } else {
          LOG_WARN("failed to get tablet loc");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(idx < 0 || idx >= candi_tablet_locs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected idx", K(idx));
      } else if (OB_FAIL(add_table_file(candi_tablet_locs.at(idx), file_desc))) {
        LOG_WARN("failed to add table file");
      }
    }
    if (tablet_loc_map.created()) {
      int tmp_ret = tablet_loc_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destroy tablet loc map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::select_location_for_hive(ObExecContext *exec_ctx,
                                                       ObIArray<ObHiveFileDesc> &file_descs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> all_servers;
  ObIArray<ObCandiTabletLoc> &candi_tablet_locs
      = candi_table_loc_.get_phy_part_loc_info_list_for_update();
  candi_tablet_locs.reset();
  ObDefaultLoadBalancer load_balancer;
  ObAddr addr;
  if (OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(
                 exec_ctx->get_my_session()->get_effective_tenant_id(),
                 all_servers))) {
    LOG_WARN("fail to get external table location");
  } else if (OB_FAIL(load_balancer.add_server_list(all_servers))) {
    LOG_WARN("failed to add server list");
  } else {
    uint64_t last_part_id = 0;
    hash::ObHashMap<ObAddr, int64_t> tablet_loc_map;
    if (OB_FAIL(tablet_loc_map.create(all_servers.count(),
                                      "TabletHiveMap",
                                      "HiveTableLoc"))) {
      LOG_WARN("failed to create tablet loc map");
    } else if (file_descs.empty()) {
      ObCandiTabletLoc* tablet_loc = nullptr;
      uint64_t part_id = 0;
      if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to alloc place holder for ObCandiTabletLoc");
      } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, GCTX.self_addr(), part_id))) {
        LOG_WARN("failed to init tablet loc by addr");
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
        int64_t idx = -1;
        ObCandiTabletLoc *tablet_loc = nullptr;
        if (OB_FAIL(load_balancer.select_server(file_descs.at(i).file_path_, addr))) {
          LOG_WARN("failed to select server");
        } else if (OB_FAIL(tablet_loc_map.get_refactored(addr, idx))) {
          if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
            ret = OB_SUCCESS;
            idx = candi_tablet_locs.count();
            if (OB_ISNULL(tablet_loc = candi_tablet_locs.alloc_place_holder())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to alloc place holder for ObCandiTabletLoc", K(ret));
            } else if (OB_FAIL(tablet_loc_map.set_refactored(addr, idx))) {
              LOG_WARN("failed to set tablet loc map", K(ret));
            } else if (OB_FAIL(init_tablet_loc_by_addr(*tablet_loc, addr, ++last_part_id))) {
              LOG_WARN("failed to init tablet loc by addr", K(ret));
            }
          } else {
            LOG_WARN("failed to get tablet loc");
          }
        }
        if (OB_FAIL(ret)) {
        } else if  (OB_UNLIKELY(idx < 0 || idx >= candi_tablet_locs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected idx", K(idx));
        } else if (OB_FAIL(add_table_file_for_hive(candi_tablet_locs.at(idx), file_descs.at(i)))) {
          LOG_WARN("failed to add table file");
        }
      }
    }
    if (tablet_loc_map.created()) {
      int tmp_ret = tablet_loc_map.destroy();
      if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to destory tablet loc map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::get_bucket_idx(const ObLakeTablePartKey &part_key,
                                             const int64_t offset,
                                             int32_t &bucket_idx)
{
  int ret = OB_SUCCESS;
  if (part_key.part_values_.at(offset).is_null()) {
    bucket_idx = hash_count_;
  } else if (!part_key.part_values_.at(offset).is_int32()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash part value is not int", K(part_key.part_values_.at(offset)));
  } else {
    bucket_idx = part_key.part_values_.at(offset).get_int32();
  }
  return ret;
}

int ObLakeTablePartitionInfo::init_tablet_loc_by_addr(ObCandiTabletLoc &tablet_loc,
                                                      const ObAddr &addr,
                                                      const uint64_t part_id)
{
  int ret = OB_SUCCESS;
  ObLSLocation location;
  if (OB_FAIL(ObDASLocationRouter::get_external_table_ls_location(location, &addr))) {
    LOG_WARN("failed to get external table location");
  } else {
    ObObjectID first_level_part_id = OB_INVALID_ID;
    ObTabletID mock_tablet_id = ObTabletID(part_id);
    if (OB_FAIL(tablet_loc.set_part_loc_with_only_readable_replica(part_id,
                                                                   first_level_part_id,
                                                                   mock_tablet_id,
                                                                   location,
                                                                   ObRoutePolicyType::READONLY_ZONE_FIRST))) {
      LOG_WARN("failed to set partition location with only readable replica", K(location));
    } else {
      tablet_loc.set_selected_replica_idx(0);
    }
    LOG_TRACE("set partition location with only readable replica", K(location), K(tablet_loc));
  }
  return ret;
}

int ObLakeTablePartitionInfo::add_table_file(ObCandiTabletLoc &tablet_loc,
                                             ObIcebergFileDesc *file_desc)
{
  int ret = OB_SUCCESS;
  ObIArray<ObIOptLakeTableFile*>& files = tablet_loc.get_opt_lake_table_files_for_update();
  ObIOptLakeTableFile *file = nullptr;
  if (OB_FAIL(ObIOptLakeTableFile::create_opt_lake_table_file_by_type(allocator_, LakeFileType::ICEBERG, file))) {
    LOG_WARN("failed to create opt lake table file by type");
  } else if (OB_ISNULL(file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate place holder for ObIOptLakeTableFile");
  } else if (OB_FAIL(files.push_back(file))) {
    LOG_WARN("failed to push back opt lake table file");
  } else if (OB_ISNULL(file_desc) || OB_ISNULL(file_desc->entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null file desc");
  } else {
    ObOptIcebergFile *iceberg_file = static_cast<ObOptIcebergFile*>(file);
    iceberg_file->file_url_ = file_desc->entry_->data_file.file_path;
    iceberg_file->file_size_ = file_desc->entry_->data_file.file_size_in_bytes;
    iceberg_file->modification_time_ = file_desc->entry_->snapshot_id;
    iceberg_file->file_format_ = file_desc->entry_->data_file.file_format;
    iceberg_file->record_count_ = file_desc->entry_->data_file.record_count;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_desc->delete_files_.size(); i++) {
      const iceberg::ManifestEntry *delete_entry = file_desc->delete_files_.at(i);
      ObLakeDeleteFile *delete_file = NULL;
      if (OB_ISNULL(delete_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete_entry is null", K(ret));
      } else if (OB_ISNULL(delete_file = OB_NEWx(ObLakeDeleteFile, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory");
      } else {
        delete_file->file_url_ = delete_entry->data_file.file_path;
        delete_file->file_size_ = delete_entry->data_file.file_size_in_bytes;
        delete_file->modification_time_ = delete_entry->snapshot_id;
        delete_file->file_format_ = delete_entry->data_file.file_format;
        if (delete_entry->is_equality_delete_file() || delete_entry->is_deletion_vector_file()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support equality delete file or deletion vector file");
        } else if (delete_entry->is_position_delete_file()) {
          delete_file->type_ = ObLakeDeleteFileType::POSITION_DELETE;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid delete file entry");
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(iceberg_file->delete_files_.push_back(delete_file))) {
          LOG_WARN("failed to push back delete file");
        }
      }
    }
  }
  return ret;
}

int ObLakeTablePartitionInfo::add_table_file_for_hive(ObCandiTabletLoc &tablet_loc,
                                                      ObHiveFileDesc &file_desc)
{
  int ret = OB_SUCCESS;
  ObIArray<ObIOptLakeTableFile*>& files = tablet_loc.get_opt_lake_table_files_for_update();
  ObIOptLakeTableFile *file = nullptr;
  if (OB_FAIL(ObIOptLakeTableFile::create_opt_lake_table_file_by_type(allocator_, LakeFileType::HIVE, file))) {
    LOG_WARN("failed to create opt lake table file by type");
  } else if (OB_ISNULL(file)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate place holder for ObIOptLakeTableFile");
  } else if (OB_FAIL(files.push_back(file))) {
    LOG_WARN("failed to push back opt lake table file");
  } else {
    ObOptHiveFile *hive_file = static_cast<ObOptHiveFile*>(file);
    hive_file->file_url_ = file_desc.file_path_;
    hive_file->part_id_ = file_desc.part_id_;
    hive_file->file_size_ = file_desc.file_size_;
    hive_file->modification_time_ = file_desc.modify_ts_;
  }
  return ret;
}

int ObLakeTablePartitionInfo::get_partition_values(ObIArray<ObString> &partition_values) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_pruner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file pruner is null");
  } else if (OB_FAIL(partition_values.assign(file_pruner_->partition_values_))) {
    LOG_WARN("failed to assign partition values", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
