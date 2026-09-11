/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_odps_file_pruner.h"

#include "share/catalog/odps/ob_odps_catalog.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/external_table/ob_odps_table_utils.h"
#include "share/location_cache/ob_location_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/rewrite/ob_query_range_define.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

// ---------------------------------------------------------------------------
// The split helpers of the QC-stage ODPS assignment. They only ever serve
// assign_odps_file_to_sqcs_, and write into per-server-slot ObOdpsSlotFiles.
// The load-aware balancer is calc_assigned_odps_files_to_sqcs_optimized below.
// ---------------------------------------------------------------------------
namespace oceanbase
{
namespace sql
{
namespace
{

// The ObPxSqcMeta overload's twin: append one file to a server slot.
int add_odps_file_to_addr(common::ObIArray<ObOdpsSlotFiles> &sqcs,
                          int64_t addr_idx,
                          ObOptOdpsFile *file)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null odps file", K(ret));
  } else {
    ObOdpsSlotFiles &sqc = sqcs.at(addr_idx);
    OZ(sqc.get_files().push_back(file));
  }
  return ret;
}

int clone_odps_file_for_slot(common::ObIAllocator &allocator,
                             const ObOptOdpsFile &src,
                             ObOptOdpsFile *&dst)
{
  int ret = OB_SUCCESS;
  dst = NULL;
  ObOptOdpsFile *tmp = OB_NEWx(ObOptOdpsFile, &allocator, allocator);
  if (OB_ISNULL(tmp)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate odps scan unit", K(ret));
  } else if (OB_FAIL(tmp->assign(src))) {
    LOG_WARN("failed to assign odps scan unit", K(ret));
  } else {
    dst = tmp;
  }
  return ret;
}

// assigned_idx is sorted by file index. Count each partition once even when
// it has several scan units; row counts follow the sampled partition order.
static int64_t estimate_tunnel_row_count(
    const ObIArray<int64_t> &file_sizes,
    const ObIArray<ObODPSFilePruner::FileInfoWithIdx> &assigned_idx,
    const ObIArray<int64_t> &partition_row_counts)
{
  long double known_rows = 0;
  long double known_size = 0;
  long double unknown_size = 0;
  int64_t sample_idx = 0;
  int64_t partition_count = 0;
  for (int64_t i = 0; i < assigned_idx.count(); ++i) {
    const ObODPSFilePruner::FileInfoWithIdx &meta = assigned_idx.at(i);
    if (i > 0 && meta.file_idx_ == assigned_idx.at(i - 1).file_idx_) {
      continue;
    }
    if (meta.file_idx_ < 0 || meta.file_idx_ >= file_sizes.count()
        || file_sizes.at(meta.file_idx_) < 0 || file_sizes.at(meta.file_idx_) == INT64_MAX) {
      return -1;
    }
    ++partition_count;
    if (meta.should_split_) {
      if (sample_idx >= partition_row_counts.count()
          || partition_row_counts.at(sample_idx) < 0
          || partition_row_counts.at(sample_idx) == INT64_MAX) {
        return -1;
      }
      known_rows += partition_row_counts.at(sample_idx++);
      known_size += file_sizes.at(meta.file_idx_);
    } else {
      unknown_size += file_sizes.at(meta.file_idx_);
    }
  }
  if (partition_count != file_sizes.count() || sample_idx == 0
      || sample_idx != partition_row_counts.count()
      || (unknown_size > 0 && (known_size <= 0 || known_rows <= 0))) {
    return -1;
  }
  // Use floating point before division so sparse partitions do not round to
  // zero. A zero-row sample cannot establish that other partitions are empty.
  const long double row_count = known_rows
      + (unknown_size > 0 ? known_rows / known_size * unknown_size : 0);
  return row_count < static_cast<long double>(INT64_MAX)
      ? static_cast<int64_t>(ceil(row_count)) : -1;
}

static int split_qc_for_odps_to_sqcs_by_line_tunnel_partition(common::ObIAllocator &allocator, ObExecContext &exec_ctx,
    const ObString &properties, common::ObIArray<ObOptOdpsFile *> &files, common::ObIArray<ObOdpsSlotFiles> &sqcs,
    int parallel, bool one_partition_per_thread, int64_t &estimated_row_count)
{
  int ret = OB_SUCCESS;
  estimated_row_count = -1;
  int64_t sqc_count = sqcs.count();
  if (one_partition_per_thread || parallel == 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      ObOptOdpsFile *unit = NULL;
      if (OB_ISNULL(files.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null odps file", K(ret), K(i));
      } else if (OB_FAIL(clone_odps_file_for_slot(allocator, *files.at(i), unit))) {
        LOG_WARN("failed to clone odps file", K(ret), K(i));
      } else {
        unit->file_size_ = INT64_MAX;
        unit->row_start_ = 0;
        unit->row_count_ = INT64_MAX;
        OZ(add_odps_file_to_addr(sqcs, i % sqc_count, unit));
      }
    }
  } else {
    common::ObSEArray<int64_t, 20> file_sizes;
    common::ObSEArray<ObODPSFilePruner::FileInfoWithIdx, 20> assigned_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      if (OB_ISNULL(files.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null odps file", K(ret), K(i));
      } else if (OB_FAIL(file_sizes.push_back(files.at(i)->file_size_))) {
        LOG_WARN("failed to push back file size", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObODPSFilePruner::calc_assigned_odps_files_to_sqcs_optimized(file_sizes, assigned_idx, sqc_count))) {
      LOG_WARN("Failed to use optimized file assignment, fallback to original strategy", K(ret));
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
        ObOptOdpsFile *unit = NULL;
        if (OB_FAIL(clone_odps_file_for_slot(allocator, *files.at(i), unit))) {
          LOG_WARN("failed to clone odps file", K(ret), K(i));
        } else {
          unit->file_size_ = INT64_MAX;
          unit->row_start_ = 0;
          unit->row_count_ = INT64_MAX;
          OZ(add_odps_file_to_addr(sqcs, i % sqc_count, unit));
        }
      }
    } else {
      struct SortByFileIdx {
        bool operator()(const ObODPSFilePruner::FileInfoWithIdx &l, const ObODPSFilePruner::FileInfoWithIdx &r) const
        {
          return l.file_idx_ < r.file_idx_ || (l.file_idx_ == r.file_idx_ && l.end_permyriad_ < r.end_permyriad_);
        }
      };
      lib::ob_sort(assigned_idx.begin(), assigned_idx.end(), SortByFileIdx());
      int64_t cur_row_count = 0;
      int64_t start_row_idx = 0;
      int64_t end_row_idx = -1;

      ObSEArray<ObString, 20> partition_strs;
      for (int64_t i = 0; OB_SUCC(ret) && i < assigned_idx.count(); ++i) {
        const ObODPSFilePruner::FileInfoWithIdx &file_info_with_idx = assigned_idx.at(i);
        if (file_info_with_idx.should_split_) {
          if (i == 0 || file_info_with_idx.file_idx_ != assigned_idx.at(i - 1).file_idx_) {
            cur_row_count = 0;
            end_row_idx = -1;
            if (OB_UNLIKELY(file_info_with_idx.file_idx_ < 0 || file_info_with_idx.file_idx_ >= files.count())
                || OB_ISNULL(files.at(file_info_with_idx.file_idx_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected odps file idx", K(ret), K(file_info_with_idx.file_idx_));
            } else {
              OZ(partition_strs.push_back(files.at(file_info_with_idx.file_idx_)->file_url_));
            }
          }
        }
      }
      ObSEArray<ObString, 20> partition_name_ret;
      ObSEArray<int64_t, 20> partition_row_counts_ret;
      ObSEArray<ObString, 20> session_ids_ret;
      ObSQLSessionInfo *session = exec_ctx.get_my_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      }
      if (OB_SUCC(ret)) {
        OZ(ObOdpsTableUtils::fetch_odps_all_partitions_info(*session, properties, partition_strs, sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT
          , sqc_count, MTL_ID(), allocator, partition_name_ret, partition_row_counts_ret, session_ids_ret));
      }
      if (OB_SUCC(ret)) {
        estimated_row_count = estimate_tunnel_row_count(file_sizes, assigned_idx,
                                                       partition_row_counts_ret);
        LOG_TRACE("estimate ODPS Tunnel row count from split partitions", K(estimated_row_count));
      }
      int indx_of_partition = 0;
      ObString session_id;

      for (int64_t i = 0; OB_SUCC(ret) && i < assigned_idx.count(); ++i) {
        const ObODPSFilePruner::FileInfoWithIdx &meta = assigned_idx.at(i);
        if (OB_UNLIKELY(meta.file_idx_ < 0 || meta.file_idx_ >= files.count())
            || OB_ISNULL(files.at(meta.file_idx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected odps file idx", K(ret), K(meta.file_idx_));
        } else if (meta.should_split_) {
          if (i == 0 || meta.file_idx_ != assigned_idx.at(i - 1).file_idx_) {
            cur_row_count = 0;
            end_row_idx = -1;
            session_id = session_ids_ret.at(indx_of_partition);
            cur_row_count = partition_row_counts_ret.at(indx_of_partition);
            indx_of_partition++;
          }
          if (OB_SUCC(ret)) {
            int64_t start_permyriad_ = meta.start_permyriad_;
            int64_t end_permyriad_ = meta.end_permyriad_;
            start_row_idx = end_row_idx + 1;
            end_row_idx = cur_row_count * (end_permyriad_ / 10000.00) - 1;
            if (end_row_idx < start_row_idx) {
              LOG_WARN("end_row_idx < start_row_idx", K(end_row_idx), K(start_row_idx));
            } else {
              ObOptOdpsFile *unit = NULL;
              if (OB_FAIL(clone_odps_file_for_slot(allocator, *files.at(meta.file_idx_), unit))) {
                LOG_WARN("failed to clone odps file", K(ret));
              } else if (OB_FAIL(ob_write_string(allocator, session_id, unit->session_id_))) {
                LOG_WARN("failed to copy odps session id", K(ret));
              } else {
                unit->row_start_ = start_row_idx;
                unit->row_count_ = end_row_idx - start_row_idx + 1;
                unit->file_size_ = files.at(meta.file_idx_)->file_size_ * (end_permyriad_ - start_permyriad_) / 10000.00;
                OZ(add_odps_file_to_addr(sqcs, meta.sqc_idx_, unit));
              }
            }
          }
        } else {
          ObOptOdpsFile *unit = NULL;
          if (OB_FAIL(clone_odps_file_for_slot(allocator, *files.at(meta.file_idx_), unit))) {
            LOG_WARN("failed to clone odps file", K(ret));
          } else {
            unit->file_size_ = INT64_MAX;
            unit->row_start_ = 0;
            unit->row_count_ = INT64_MAX;
            OZ(add_odps_file_to_addr(sqcs, meta.sqc_idx_, unit));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(files.at(meta.file_idx_))) {
          LOG_INFO("px task info: odps file to machine ", K(i),
              K(files.at(meta.file_idx_)->file_url_),
              K(session_id),
              K(meta.start_permyriad_),
              K(meta.end_permyriad_),
              K(meta.should_split_),
              K(meta.process_size_),
              K(meta.sqc_idx_));
        }
      }
    }
  }
  return ret;
}

static int split_qc_for_odps_to_sqcs_storage_api_byte(common::ObIAllocator &allocator,
    int64_t split_task_count,
    const ObString& session_str, const ObString &new_file_urls, common::ObIArray<ObOdpsSlotFiles> &sqcs)
{
  int ret = OB_SUCCESS;
  int64_t sqc_count = sqcs.count();
  if (split_task_count == 0) {
    LOG_INFO("no task for reader", K(lbt()));
  } else {
    int64_t sqc_idx = 0;
    for (int i = 0; OB_SUCC(ret) && i < split_task_count; i += 1) {
      ObOptOdpsFile *unit = OB_NEWx(ObOptOdpsFile, &allocator, allocator);
      if (OB_ISNULL(unit)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate odps scan unit", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, new_file_urls, unit->file_url_))) {
        LOG_WARN("failed to copy odps file url", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, session_str, unit->session_id_))) {
        LOG_WARN("failed to copy odps session id", K(ret));
      } else {
        unit->part_id_ = 0;  // urls contain multi parts
        unit->row_start_ = 0;
        unit->row_count_ = 0;
        unit->first_split_idx_ = i;
        unit->last_split_idx_ = i + 1;
        if (OB_FAIL(sqcs.at(sqc_idx).get_files().push_back(unit))) {
          LOG_WARN("failed to push back task info", K(ret));
        } else {
          sqc_idx = (sqc_idx + 1) % sqc_count;
        }
      }
    }
  }

  return ret;
}

static int split_qc_for_odps_to_sqcs_storage_api_row(common::ObIAllocator &allocator,
    int64_t table_total_row_count,
    const ObString& session_str, const ObString &new_file_urls, common::ObIArray<ObOdpsSlotFiles> &sqcs, int parallel)
{
  int ret = OB_SUCCESS;
  int64_t sqc_count = sqcs.count();
  if (table_total_row_count == 0) {
    // do nothing
  } else {
    int32_t task_count = parallel;
    int64_t step = (table_total_row_count + task_count - 1) / task_count;
    if (step < 1000) {
      step = 1000;
    }
    int64_t sqc_idx = 0;
    for (int64_t start = 0; OB_SUCC(ret) && start < table_total_row_count; start += step) {
      if (start + step > table_total_row_count) {
        step = table_total_row_count - start;
      }
      ObOptOdpsFile *unit = OB_NEWx(ObOptOdpsFile, &allocator, allocator);
      if (OB_ISNULL(unit)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate odps scan unit", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, new_file_urls, unit->file_url_))) {
        LOG_WARN("failed to copy odps file url", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, session_str, unit->session_id_))) {
        LOG_WARN("failed to copy odps session id", K(ret));
      } else {
        unit->part_id_ = 0;
        unit->row_start_ = start;
        unit->row_count_ = step;
        if (OB_FAIL(sqcs.at(sqc_idx).get_files().push_back(unit))) {
          LOG_WARN("failed to push back task info", K(ret), K(start), K(step));
        } else {
          sqc_idx = (sqc_idx + 1) % sqc_count;
        }
      }
    }
  }

  return ret;
}

} // anonymous namespace
} // namespace sql
} // namespace oceanbase


ObODPSFilePruner::ObODPSFilePruner(common::ObIAllocator &allocator)
    : ObILakeTableFilePruner(allocator), sql_schema_guard_(NULL), odps_part_bounds_(allocator_),
      part_column_ids_(allocator_), api_mode_(ObODPSGeneralFormat::ApiMode::TUNNEL_API),
      format_str_(), nonpart_col_idxs_(), part_col_idxs_(), pred_col_infos_(), clause_part_ids_(),
      estimated_row_count_(-1)
{
}

void ObODPSFilePruner::reset()
{
  ObILakeTableFilePruner::reset();
  sql_schema_guard_ = NULL;
  odps_part_bounds_.reset();
  part_column_ids_.reset();
  api_mode_ = ObODPSGeneralFormat::ApiMode::TUNNEL_API;
  format_str_.reset();
  nonpart_col_idxs_.reset();
  part_col_idxs_.reset();
  pred_col_infos_.reset();
  clause_part_ids_.reset();
  estimated_row_count_ = -1;
}

int ObODPSFilePruner::clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const
{
  int ret = OB_SUCCESS;
  pruner = nullptr;
  ObODPSFilePruner *tmp = nullptr;
  if (OB_ISNULL(tmp = OB_NEWx(ObODPSFilePruner, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObODPSFilePruner");
  } else if (OB_FAIL(tmp->assign(*this))) {
    LOG_WARN("failed to assign odps file pruner");
  } else {
    pruner = tmp;
  }
  return ret;
}

int ObODPSFilePruner::assign(const ObILakeTableFilePruner &o)
{
  int ret = OB_SUCCESS;
  if (this != &o) {
    reset();
    const ObODPSFilePruner &other = static_cast<const ObODPSFilePruner &>(o);
    if (OB_FAIL(ObILakeTableFilePruner::assign(o))) {
      LOG_WARN("assign loc meta failed", K(ret), K(other.loc_meta_));
    } else if (OB_FAIL(odps_part_bounds_.assign(other.odps_part_bounds_))) {
      LOG_WARN("failed to assign odps part bounds");
    } else if (OB_FAIL(part_column_ids_.assign(other.part_column_ids_))) {
      LOG_WARN("failed to assign part column ids");
    } else if (OB_FAIL(nonpart_col_idxs_.assign(other.nonpart_col_idxs_))) {
      LOG_WARN("failed to assign nonpart col idxs");
    } else if (OB_FAIL(part_col_idxs_.assign(other.part_col_idxs_))) {
      LOG_WARN("failed to assign part col idxs");
    } else if (OB_FAIL(pred_col_infos_.assign(other.pred_col_infos_))) {
      LOG_WARN("failed to assign pred col infos");
    } else if (OB_FAIL(clause_part_ids_.assign(other.clause_part_ids_))) {
      LOG_WARN("failed to assign clause part ids");
    } else if (!other.format_str_.empty()
               && OB_FAIL(ob_write_string(allocator_, other.format_str_, format_str_))) {
      LOG_WARN("failed to copy odps format str", K(ret));
    } else {
      sql_schema_guard_ = other.sql_schema_guard_;
      api_mode_ = other.api_mode_;
      estimated_row_count_ = other.estimated_row_count_;
    }
  }
  if (OB_FAIL(ret)) {
    inited_ = false;
  }
  return ret;
}

int ObODPSFilePruner::init(ObSqlSchemaGuard &sql_schema_guard,
                           const ObDMLStmt &stmt,
                           ObExecContext *exec_ctx,
                           const uint64_t table_id,
                           const uint64_t ref_table_id,
                           const ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  sql_schema_guard_ = &sql_schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("odps file pruner init twice", K(ret));
  } else if (OB_FAIL(sql_schema_guard.get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(ref_table_id));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(table_schema), KP(exec_ctx));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    const int64_t part_key_nums = table_schema->get_partition_key_column_num();
    if (part_key_nums > 0) {
      is_partitioned_ = true;
      if (OB_FAIL(part_column_ids_.init(part_key_nums))) {
        LOG_WARN("failed to init part column ids", K(ret));
      } else if (OB_FAIL(table_schema->get_partition_key_info().get_column_ids(part_column_ids_))) {
        LOG_WARN("failed to get part column ids", K(ret));
      }
    } else {
      is_partitioned_ = false;
    }

    // The api mode decides how the optimizer-time split fetch turns the
    // selected partitions into scan units (TUNNEL: per partition / row
    // sub-range, STORAGE: per split-index or row range of one download
    // session).
    if (OB_SUCC(ret)) {
      bool is_odps_external_table = false;
      const ObString format_str = table_schema->get_external_file_format().empty()
                                      ? table_schema->get_external_properties()
                                      : table_schema->get_external_file_format();
      if (OB_FAIL(ObSQLUtils::get_odps_api_mode(format_str, is_odps_external_table, api_mode_))) {
        LOG_WARN("failed to get odps api mode", K(ret));
      } else if (OB_UNLIKELY(!is_odps_external_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not an odps external table", K(ret), K(ref_table_id));
      } else if (OB_FAIL(ob_write_string(allocator_, format_str, format_str_))) {
        LOG_WARN("failed to copy odps format str", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(collect_projected_column_idxs_(stmt))) {
      LOG_WARN("failed to collect projected odps column idxs", K(ret));
    } else if (OB_FAIL(collect_clause_part_ids_(stmt))) {
      LOG_WARN("failed to collect clause part ids", K(ret));
    } else if (OB_FAIL(generate_column_meta_info(stmt))) {
      LOG_WARN("failed to generate column meta info", K(ret));
    } else if (OB_FAIL(generate_partition_bound(stmt, exec_ctx, table_schema, filter_exprs,
                                                odps_part_bounds_))) {
      LOG_WARN("failed to generate partition bound", K(ret));
    } else if (need_all_) {
      // no filter (or non-partitioned): every partition is selected
    } else if (OB_FAIL(ObLakeTablePushDownFilter::generate_pd_filter_spec(allocator_,
                                                                         *exec_ctx,
                                                                         &stmt,
                                                                         filter_exprs,
                                                                         file_filter_spec_))) {
      LOG_WARN("failed to generate pd filter spec", K(ret));
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int ObODPSFilePruner::create_odps_file_(const ObString &partition_spec,
                                        const int64_t part_id,
                                        ObOptOdpsFile *&file)
{
  int ret = OB_SUCCESS;
  file = NULL;
  ObOptOdpsFile *tmp = OB_NEWx(ObOptOdpsFile, &allocator_, allocator_);
  if (OB_ISNULL(tmp)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObOptOdpsFile", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, partition_spec, tmp->file_url_))) {
    LOG_WARN("failed to copy odps partition spec", K(ret));
  } else {
    tmp->part_id_ = part_id;
    tmp->file_size_ = OB_INVALID_SIZE; // unknown until plan_files fetches it
    file = tmp;
  }
  return ret;
}

int ObODPSFilePruner::fill_partition_values_(const ObTableSchema &table_schema,
                                             const ObIArray<const ObPartition *> &parts)
{
  int ret = OB_SUCCESS;
  // Partition values are built with the stats path's constructor
  // (ObOdpsCatalogUtils::get_partition_odps_str_from_table_schema):
  // comma separated col='val' pairs — NOT the hive style c=v/c=v.
  ObSEArray<ObString, 4> partition_column_names;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_key_column_num(); ++i) {
    ObString *partition_column_name = NULL;
    if (OB_ISNULL(partition_column_name = partition_column_names.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for partition column name", K(ret));
    } else if (OB_FAIL(table_schema.get_part_key_column_name(i, *partition_column_name))) {
      LOG_WARN("failed to get partition column name", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(partition_values_.prepare_allocate(parts.count()))) {
    LOG_WARN("failed to init partition values", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
    if (OB_ISNULL(parts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition", K(ret), K(i));
    } else if (OB_FAIL(ObOdpsCatalogUtils::construct_partition_values(allocator_,
                                                                      partition_column_names,
                                                                      parts.at(i)->get_list_row_values(),
                                                                      partition_values_.at(i)))) {
      LOG_WARN("failed to construct partition values", K(ret), K(i));
    }
  }
  return ret;
}

// Recursively collect the 0-based odps column indexes of the pseudo columns
// (external$tablecol[i] / metadata$partition_list_col[i]) hidden inside the
// (stored generated) column expressions — the optimizer-time twin of
// ObLogTableScan::extract_file_column_exprs_recursively, consistent with the
// runtime ObExpr::extra_ - 1.
int ObODPSFilePruner::collect_pseudo_col_idx_recursively_(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()
             || T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type()) {
    const ObPseudoColumnRawExpr *pseudo_col_expr = static_cast<const ObPseudoColumnRawExpr *>(expr);
    const int64_t column_idx = pseudo_col_expr->get_column_idx() - 1;
    ObSEArray<int64_t, 8> &idxs = T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()
                                      ? nonpart_col_idxs_
                                      : part_col_idxs_;
    bool found = false;
    for (int64_t i = 0; !found && i < idxs.count(); ++i) {
      found = (idxs.at(i) == column_idx);
    }
    if (!found && OB_FAIL(idxs.push_back(column_idx))) {
      LOG_WARN("failed to push back odps column idx", K(ret), K(column_idx));
    }
  } else if (expr->is_column_ref_expr()
             && OB_NOT_NULL(static_cast<const ObColumnRefRawExpr *>(expr)->get_dependant_expr())) {
    if (OB_FAIL(collect_pseudo_col_idx_recursively_(
            static_cast<const ObColumnRefRawExpr *>(expr)->get_dependant_expr()))) {
      LOG_WARN("failed to collect pseudo col idx from dependant expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(collect_pseudo_col_idx_recursively_(expr->get_param_expr(i)))) {
        LOG_WARN("failed to collect pseudo col idx from param expr", K(ret), K(i));
      }
    }
  }
  return ret;
}

namespace
{
// Same walk as ObODPSFilePruner::collect_pseudo_col_idx_recursively_, but keeps
// the owning OB schema column id so the optimizer-time predicate printer can
// resolve the white-filter column ids (the re-derived pushdown filter tree
// carries OB column ids, not odps column indexes).
int collect_pred_col_info_recursive_(const ObRawExpr *expr,
                                     const uint64_t column_id,
                                     ObIArray<ObOdpsPredColumnInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()
             || T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type()) {
    const ObPseudoColumnRawExpr *pseudo_col_expr = static_cast<const ObPseudoColumnRawExpr *>(expr);
    bool found = false;
    for (int64_t i = 0; !found && i < infos.count(); ++i) {
      found = (infos.at(i).column_id_ == column_id);
    }
    if (!found && OB_FAIL(infos.push_back(
            ObOdpsPredColumnInfo(column_id, expr->get_expr_type(), pseudo_col_expr->get_column_idx())))) {
      LOG_WARN("failed to push back pred column info", K(ret), K(column_id));
    }
  } else if (expr->is_column_ref_expr()
             && OB_NOT_NULL(static_cast<const ObColumnRefRawExpr *>(expr)->get_dependant_expr())) {
    if (OB_FAIL(collect_pred_col_info_recursive_(
            static_cast<const ObColumnRefRawExpr *>(expr)->get_dependant_expr(), column_id, infos))) {
      LOG_WARN("failed to collect pred column info from dependant expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(collect_pred_col_info_recursive_(expr->get_param_expr(i), column_id, infos))) {
        LOG_WARN("failed to collect pred column info from param expr", K(ret), K(i));
      }
    }
  }
  return ret;
}
} // anonymous namespace

int ObODPSFilePruner::collect_projected_column_idxs_(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> column_items;
  nonpart_col_idxs_.reset();
  part_col_idxs_.reset();
  pred_col_infos_.reset();
  if (OB_FAIL(stmt.get_column_items(loc_meta_.table_loc_id_, column_items))) {
    LOG_WARN("failed to get column items", K(ret), K(loc_meta_.table_loc_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
    const ColumnItem &item = column_items.at(i);
    if (OB_ISNULL(item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column expr", K(ret), K(i));
    } else if (OB_FAIL(collect_pseudo_col_idx_recursively_(item.expr_))) {
      LOG_WARN("failed to collect pseudo col idx", K(ret), K(i));
    } else if (OB_FAIL(collect_pred_col_info_recursive_(item.expr_, item.column_id_, pred_col_infos_))) {
      LOG_WARN("failed to collect pred column info", K(ret), K(i));
    }
  }
  return ret;
}

int ObODPSFilePruner::collect_clause_part_ids_(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  clause_part_ids_.reset();
  // An explicit PARTITION(p0, ...) clause is resolved by ObPartGetter into
  // TableItem::part_ids_ (schema partition ids of the materialized LIST
  // partitions); prune on it here.
  const TableItem *table_item = stmt.get_table_item_by_id(loc_meta_.table_loc_id_);
  if (OB_NOT_NULL(table_item) && !table_item->part_ids_.empty()
      && OB_FAIL(clause_part_ids_.assign(table_item->part_ids_))) {
    LOG_WARN("failed to assign clause part ids", K(ret));
  }
  return ret;
}

int ObODPSFilePruner::get_server_count_(ObExecContext &exec_ctx, int64_t &server_count)
{
  int ret = OB_SUCCESS;
  server_count = 1; // single-node fallback
  ObSEArray<ObAddr, 16> all_servers;
  if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(
                 exec_ctx.get_my_session()->get_effective_tenant_id(), all_servers))) {
    LOG_WARN("fail to get external table location", K(ret));
  } else if (all_servers.count() > 0) {
    server_count = all_servers.count();
  }
  return ret;
}

/// Select the surviving first-level ODPS partitions (PARTITION_LEVEL_ONE only)
/// and emit one ObOptOdpsFile per partition (possibly none — plan_files emits
/// the dummy unit when nothing real survives).
///
/// Selects the partitions whose list rows pass the per partition key column
/// query ranges (ObPreRangeGraph) and the pushdown filter
/// (ObLakePartRowPushDownFilter as a min=max=point skipping index). The pruner
/// claims no range exprs (base get_part_id_and_range_exprs default), so the
/// partition predicates stay in the row scan and are re-evaluated per row.
int ObODPSFilePruner::prune_partitions(ObExecContext &exec_ctx, ObIArray<ObOptOdpsFile *> &files)
{
  int ret = OB_SUCCESS;
  files.reset();
  const ObTableSchema *table_schema = NULL;
  ObSEArray<const ObPartition *, 16> selected_parts;
  if (OB_ISNULL(sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql schema guard is null", K(ret));
  } else if (OB_FAIL(sql_schema_guard_->get_table_schema(loc_meta_.ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(loc_meta_.ref_table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table schema", K(ret), K(loc_meta_.ref_table_id_));
  } else if (OB_UNLIKELY(share::schema::PARTITION_LEVEL_TWO == table_schema->get_part_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("odps table only supports one-level partition", K(ret),
             K(table_schema->get_part_level()), K(loc_meta_.ref_table_id_));
  } else if (!is_partitioned_) {
    // Non-partitioned ODPS table: one implicit partition with an empty spec.
    ObOptOdpsFile *file = NULL;
    if (OB_FAIL(partition_values_.prepare_allocate(1))) {
      LOG_WARN("failed to init partition values", K(ret));
    } else if (FALSE_IT(partition_values_.at(0).reset())) {
    } else if (OB_FAIL(create_odps_file_(ObString(), 0, file))) {
      LOG_WARN("failed to create odps file for non-partitioned table", K(ret));
    } else if (OB_FAIL(files.push_back(file))) {
      LOG_WARN("failed to push back odps file", K(ret));
    } else {
      all_partitions_selected_ = true;
    }
  } else {
    bool pd_filter_ready = false;
    ObLakePartRowPushDownFilter file_filter(exec_ctx, file_filter_spec_, &part_column_ids_);
    if (!need_all_ && OB_NOT_NULL(file_filter_spec_.pd_expr_spec_)) {
      if (OB_FAIL(file_filter.init(column_ids_, column_metas_))) {
        LOG_WARN("failed to init skip filter executor", K(ret));
      } else {
        pd_filter_ready = true;
      }
    }
    int64_t materialized_part_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_num(); ++i) {
      const ObPartition *part = table_schema->get_part_array()[i];
      bool in_bound = false;
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i));
      } else if (part->get_list_row_values().empty()) {
        // A mocked placeholder partition (added when the ODPS table is
        // partitioned but no partition is materialized yet) carries no list
        // row — there is nothing to read, so it is simply skipped.
      } else if (0 == part->get_part_name().case_compare("P_DEFAULT")) {
        // The schema-level DEFAULT catch-all partition backs no materialized
        // ODPS partition and has no valid spec — skip it.
      } else {
        ++materialized_part_count;
        bool in_clause = true;
        if (!clause_part_ids_.empty()) {
          in_clause = has_exist_in_array(clause_part_ids_,
                                         static_cast<ObObjectID>(part->get_part_id()));
        }
        if (!in_clause) {
          // filtered out by the explicit PARTITION(p0, ...) clause
        } else if (need_all_) {
          in_bound = true;
        } else {
          const ObIArray<ObNewRow> &list_rows = part->get_list_row_values();
          if (OB_UNLIKELY(list_rows.count() != 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("external table partition list value num should be one",
                     K(ret), K(list_rows.count()), K(part->get_part_name()));
          } else if (OB_UNLIKELY(list_rows.at(0).get_count() != odps_part_bounds_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part_row size should equal with partition bounds",
                     K(list_rows.at(0)), K(odps_part_bounds_.count()));
          } else if (check_one_row_part_column(list_rows.at(0), odps_part_bounds_)) {
            bool is_filtered = false;
            ObNewRow ob_part_row = list_rows.at(0);
            if (pd_filter_ready && OB_FAIL(file_filter.filter(ob_part_row, is_filtered))) {
              LOG_WARN("failed to check file filter range", K(ret));
            } else if (!is_filtered) {
              in_bound = true;
            }
          }
        }
      }
      if (OB_SUCC(ret) && in_bound) {
        if (OB_FAIL(selected_parts.push_back(part))) {
          LOG_WARN("failed to push back selected partition", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !selected_parts.empty()
        && OB_FAIL(fill_partition_values_(*table_schema, selected_parts))) {
      LOG_WARN("failed to fill partition values", K(ret));
    }
    // When every partition is pruned away, partition_values_ stays empty and
    // stats estimation falls back to the full-table row count.
    if (OB_SUCC(ret)) {
      all_partitions_selected_ = selected_parts.count() == materialized_part_count;
    }
    // Emit one unit per selected partition. The spec must be the
    // driver-reported string (see resolve_partition_specs_): the OB partition
    // column name may differ from the ODPS partition key (e.g. PAR1 vs dt),
    // and a name-rebuilt spec is rejected by the ODPS tunnel API.
    ObSEArray<common::ObString, 16> part_specs;
    // name-rebuilt values from fill_partition_values_: mocked-table fallback
    // (a mocked schema's column names mirror the ODPS-side names).
    ObSEArray<common::ObString, 16> name_rebuild;
    if (OB_FAIL(ret) || selected_parts.empty()) {
      // nothing selected: partition_values_ stays empty as well
    } else if (OB_FAIL(name_rebuild.assign(partition_values_))) {
      LOG_WARN("failed to capture name-rebuilt partition values", K(ret));
    } else if (OB_FAIL(resolve_partition_specs_(exec_ctx,
                                                 selected_parts, part_specs))) {
      LOG_WARN("failed to resolve partition specs", K(ret));
    } else if (OB_UNLIKELY(part_specs.count() != selected_parts.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition specs and selected parts are not aligned",
               K(ret), K(part_specs.count()), K(selected_parts.count()));
    } else {
      // Partitions without a spec are skipped. Collect first, then
      // prepare_allocate into the fixed array member (ObFixedArray cannot
      // push_back after reset()).
      common::ObSEArray<common::ObString, 16> final_values;
      for (int64_t i = 0; OB_SUCC(ret) && i < selected_parts.count(); ++i) {
        common::ObString spec = part_specs.at(i);
        if (spec.empty()
            && common::is_external_object_id(loc_meta_.ref_table_id_)
            && OB_LIKELY(i < name_rebuild.count())) {
          // mocked-table fallback when external_location is missing
          spec = name_rebuild.at(i);
        }
        if (spec.empty()) {
          continue;
        }
        ObOptOdpsFile *file = NULL;
        if (OB_FAIL(create_odps_file_(spec, selected_parts.at(i)->get_part_id(), file))) {
          LOG_WARN("failed to create odps file", K(ret), K(i));
        } else if (OB_FAIL(files.push_back(file))) {
          LOG_WARN("failed to push back odps file", K(ret));
        } else if (OB_FAIL(final_values.push_back(spec))) {
          LOG_WARN("failed to push back partition value", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(partition_values_.prepare_allocate(final_values.count()))) {
        LOG_WARN("failed to prepare allocate partition values", K(ret), K(final_values.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < final_values.count(); ++i) {
        partition_values_.at(i) = final_values.at(i);
      }
    }
  }
  return ret;
}

int ObODPSFilePruner::resolve_partition_specs_(
    ObExecContext &exec_ctx,
    const common::ObIArray<const share::schema::ObPartition *> &parts,
    common::ObIArray<common::ObString> &part_specs)
{
  int ret = OB_SUCCESS;
  part_specs.reset();
  if (common::is_external_object_id(loc_meta_.ref_table_id_)) {
    // catalog (mocked) table: the resolver materialized the driver-reported
    // spec into each mocked partition's external_location at resolve time.
    for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
      const share::schema::ObPartition *part = parts.at(i);
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret), K(i));
      } else if (OB_FAIL(part_specs.push_back(part->get_external_location()))) {
        LOG_WARN("failed to push back partition spec", K(ret), K(i));
      }
    }
  } else {
    // CREATE EXTERNAL TABLE: the driver-reported specs are mirrored in
    // __all_external_table_file (file_url_), read through the file-manager
    // KV cache. The OB partition column names may differ from the ODPS
    // partition keys, so the mirror is the authoritative spec source here.
    common::ObSEArray<int64_t, 16> part_ids;
    common::ObSEArray<share::ObExternalFileInfo, 16> mirror_files;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    if (OB_ISNULL(exec_ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
      if (OB_ISNULL(parts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret), K(i));
      } else if (OB_FAIL(part_ids.push_back(parts.at(i)->get_part_id()))) {
        LOG_WARN("failed to push back part id", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(share::ObExternalTableFileManager::get_instance()
                           .get_external_files_by_part_ids(tenant_id,
                                                           loc_meta_.ref_table_id_,
                                                           part_ids,
                                                           false /* not on local disk */,
                                                           allocator_,
                                                           mirror_files,
                                                           exec_ctx,
                                                           nullptr /* range filter */))) {
      LOG_WARN("failed to get external files by part ids", K(ret),
               K(tenant_id), K(loc_meta_.ref_table_id_), K(part_ids));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
        const int64_t part_id = parts.at(i)->get_part_id();
        common::ObString spec;
        for (int64_t j = 0; OB_SUCC(ret) && j < mirror_files.count(); ++j) {
          if (mirror_files.at(j).part_id_ == part_id) {
            // deep copy into the pruner allocator (the mirror entries are
            // already copied there by the file manager, this is belt & braces)
            if (OB_FAIL(ob_write_string(allocator_, mirror_files.at(j).file_url_, spec))) {
              LOG_WARN("failed to copy partition spec", K(ret), K(part_id));
            }
            break;
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(part_specs.push_back(spec))) {
          LOG_WARN("failed to push back partition spec", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

/*
 * 优化文件分配策略说明：
 *
 * 1. 文件分块策略：
 *    - 小于50MB的文件：不分块，避免额外的文件打开成本
 *    - 大于50MB的文件：根据成本效益分析决定是否分块
 *    - 分块大小：100MB，平衡并行度和文件打开成本
 *
 * 2. 负载均衡策略：
 *    - 基于总处理时间（文件处理时间 + 文件打开成本）进行分配
 *    - 使用最小堆优先选择负载最轻的SQC
 *    - 大文件优先分配，确保负载均衡
 *
 * 3. 成本效益分析：
 *    - 文件打开成本：2秒/文件
 *    - 处理速度：10MB/s
 *    - 分块阈值：当分块成本 < 处理时间/2 时分块
 *
 * 4. 失败原因：
 *   我们设置的每个线程能处理avg_size_per_sqc太小时会发现分不下来
 * 使用示例：
 *   ObArray<int64_t> assigned_idx;
 *   int ret = ObODPSFilePruner::calc_assigned_odps_files_to_sqcs_optimized(
 *     files, assigned_idx, parallel);
 *   if (OB_SUCC(ret)) {
 *     // 根据assigned_idx将文件分配给对应的SQC
 *     for (int64_t i = 0; i < files.count(); i++) {
 *       int64_t sqc_idx = assigned_idx.at(i);
 *       sqcs.at(sqc_idx)->get_access_external_table_files().push_back(files.at(i));
 *     }
 *   }
 */

int ObODPSFilePruner::calc_assigned_odps_files_to_sqcs_optimized(
    const ObIArray<int64_t> &file_sizes,
    common::ObSEArray<FileInfoWithIdx, 20> &sorted_files,
    int64_t parallel)
{
  int ret = OB_SUCCESS;
  // 常量定义：文件打开成本2秒，处理速度10MB/s
  const int64_t FILE_OPEN_COST_MS = 2000;  // 2秒
  const int64_t PROCESSING_SPEED_MBPS = 10; // 10MB/s
  const int64_t MIN_FILE_SIZE_FOR_SPLIT = 60 * 1024 * 1024; // 50MB，小于此值不分块


  // 第一步：计算总文件大小和平均每个SQC应分配的大小
  int64_t total_file_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_sizes.count(); i++) {
    if (file_sizes.at(i) != INT64_MAX) {
      total_file_size += file_sizes.at(i);
    } else {
      ret = OB_ERROR_OUT_OF_RANGE;
    }
  }
  // check total_file_size overflow
  if (OB_SUCC(ret) && total_file_size < 0) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("total_file_size < 0", K(ret), K(total_file_size));
  }

  // 第二步：分析文件，决定是否需要分块
  OZ(sorted_files.reserve(file_sizes.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < file_sizes.count(); i++) {
    FileInfoWithIdx file_info;
    file_info.file_size_ = file_sizes.at(i);
    file_info.file_idx_ = i;
    file_info.remain_file_size_ = file_sizes.at(i);
    file_info.end_permyriad_ = 0;
    // 判断文件是否需要分块
    if (file_info.file_size_ < MIN_FILE_SIZE_FOR_SPLIT) {
      file_info.should_split_ = false;
      LOG_DEBUG("odps trace file_info not split", K(file_info.file_idx_), K(file_info.file_size_), K(file_info.should_split_));
    } else {
      // 计算分块的成本效益
      int64_t split_cost = FILE_OPEN_COST_MS; // 分块需要额外打开成本
      int64_t split_benefit = (file_info.file_size_ * 1000) / (PROCESSING_SPEED_MBPS * 1024 * 1024);
      // 如果分块成本小于处理时间的一半，则分块
      file_info.should_split_ = (split_cost < split_benefit / 2);
      LOG_DEBUG("odps trace file_info split", K(file_info.file_idx_), K(file_info.file_size_), K(file_info.should_split_));
    }
    // cout << file_info.file_idx_ << " should_split_: " << file_info.should_split_ << endl;
    OZ(sorted_files.push_back(file_info));
  }
  // 第三步：按文件大小升序排序（小文件优先分配）
  struct SortByFileSize {
    bool operator()(const FileInfoWithIdx &l, const FileInfoWithIdx &r) const {
      return l.file_size_ < r.file_size_;
    }
  };
  OX(lib::ob_sort(sorted_files.begin(), sorted_files.end(), SortByFileSize()));

  int64_t last_k_files_count = parallel - 1; // parallel - 1 QZ
  if (parallel * 100 < sorted_files.count()) {
    last_k_files_count = 0;
  }

  // 在测试场景中，通过环境变量控制是否使用方差分割
  // const char* variance_split_disable = getenv("OB_VARIANCE_SPLIT_DISABLE");
  // if (variance_split_disable != nullptr && strcmp(variance_split_disable, "1") == 0) {
  //   last_k_files_count = 0;
  // }
  for (int64_t i = 0; OB_SUCC(ret) && i < sorted_files.count(); i++) {
    if (i < sorted_files.count() - last_k_files_count) {
      sorted_files.at(i).should_split_ = false;
    }
  }

  // 第四步：初始化SQC集合，设置目标大小
  ObSEArray<SqcFileSet, 8> sqc_sets;
  OZ(sqc_sets.prepare_allocate(parallel));
  for (int64_t i = 0; OB_SUCC(ret) && i < parallel; i++) {
    sqc_sets[i].sqc_idx_ = i;
    sqc_sets[i].total_file_size_ = 0;
    sqc_sets[i].total_file_count_ = 0;
    sqc_sets[i].total_processing_time_ms_ = 0;
  }
  // 第五步：不分块的文件分配
  int64_t should_split_file_count = 0;
  for (int64_t i = sorted_files.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    FileInfoWithIdx &file_split_meta = sorted_files[i];
    if (!file_split_meta.should_split_) {
      int64_t best_sqc_idx = -1;
      int64_t min_load = INT64_MAX;
      for (int64_t j = 0; j < parallel; j++) {
        int64_t current_load = sqc_sets[j].total_processing_time_ms_;
        if (current_load < min_load) {
          min_load = current_load;
          best_sqc_idx = j;
        }
      }
      if (OB_UNLIKELY(best_sqc_idx < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to find suitable SQC for file", K(file_split_meta.file_idx_));
      } else {
        // 小文件不分块，直接分配
        sqc_sets[best_sqc_idx].add_file(file_split_meta.file_size_, 1);
        file_split_meta.sqc_idx_ = best_sqc_idx;
        file_split_meta.start_permyriad_ = 0;
        file_split_meta.end_permyriad_ = 10000;
        file_split_meta.should_split_ = false;
        file_split_meta.process_size_ += file_split_meta.file_size_;
        LOG_INFO("Small ODPS File assigned", K(file_split_meta.file_idx_), K(best_sqc_idx),
                 K(file_split_meta.file_size_), K(file_split_meta.should_split_));
      }
    } else {
      should_split_file_count++;
    }
  }
  // 第六步：分块分配
  // 计算平均每个SQC应分配的大小，向上取整
  int64_t avg_size_per_sqc = 0;
  int64_t init_file_count = 0;
  int64_t threshold = 0;
  int64_t strict_avg_size_per_sqc = 0;
  int64_t loose_avg_size_per_sqc = 0;
  OX(strict_avg_size_per_sqc = total_file_size / parallel);
  OX(loose_avg_size_per_sqc =
         ceil(((total_file_size + parallel +
                +should_split_file_count * PROCESSING_SPEED_MBPS * 1024 * 1024 *
                    1.5) *
               1.00 / parallel)));
  OX(avg_size_per_sqc = max((int64_t)ceil(strict_avg_size_per_sqc * 1.02),
                            (int64_t)(loose_avg_size_per_sqc)));
  OX(init_file_count = sorted_files.count());
  OX(threshold = init_file_count + (should_split_file_count + 1) * parallel * 2);
  int64_t i = 0;
  for (i = 0; OB_SUCC(ret) && i < sorted_files.count() && i < threshold; i++) {
    FileInfoWithIdx &file_split_meta = sorted_files.at(i);
    if (file_split_meta.should_split_) {
      // 跳过已经分配的文件
      if (file_split_meta.sqc_idx_ >= 0) {
        continue;
      }
      // 寻找当前最适合的SQC（优先选择未达到目标大小的，然后选择负载最轻的）
      int64_t best_sqc_idx = -1;
      int64_t max_file_count_load = 0;
      int64_t min_load = INT64_MAX;

      // 第一优先级：选择未达到目标大小的SQC中负载最轻的
      for (int64_t j = 0; OB_SUCC(ret) && j < parallel; j++) {
        int64_t current_load = sqc_sets[j].total_processing_time_ms_;
        if (current_load < 0) {
          ret = OB_ERR_UNEXPECTED;
        }
        if (OB_SUCC(ret)) {
          if (current_load < min_load) {
            min_load = current_load;
            best_sqc_idx = j;
          }
          if (sqc_sets[j].total_file_count_ > max_file_count_load) {
            max_file_count_load = sqc_sets[j].total_file_count_;
          }
        }
      }
      LOG_DEBUG("odps trace best_sqc_idx", K(best_sqc_idx), K(min_load), K(max_file_count_load));

      if (OB_UNLIKELY(best_sqc_idx < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to find suitable SQC for file",
                 K(file_split_meta.file_idx_));
      } else {
        // 分配文件到选中的SQC
        // 大文件分块处理 - 分配大小为目标大小减去现有大小
        // 计算应该分配的大小：目标大小 - 当前SQC已分配的大小
        int64_t cur_sqc_avail_process_size = avg_size_per_sqc - sqc_sets[best_sqc_idx].total_file_size_;
        int add_round = 0;
        if (cur_sqc_avail_process_size <= 20 * 1024 * 1024) {
          cur_sqc_avail_process_size = 20 * 1024 * 1024;
        }
        while (cur_sqc_avail_process_size <= 0 && avg_size_per_sqc < strict_avg_size_per_sqc * 2 && add_round < 5) {
          avg_size_per_sqc = max(ceil(avg_size_per_sqc * 1.1), ceil(avg_size_per_sqc + max_file_count_load * PROCESSING_SPEED_MBPS * 1024 * 1024 * 1.25));
          cur_sqc_avail_process_size = avg_size_per_sqc - sqc_sets[best_sqc_idx].total_file_size_;
          ++add_round;
          LOG_DEBUG("odps trace cur_sqc_avail_process_size", K(avg_size_per_sqc));
        }

        LOG_DEBUG("odps trace cur_sqc_avail_process_size", K(cur_sqc_avail_process_size), K(add_round),
                  K(avg_size_per_sqc), K(strict_avg_size_per_sqc), K(best_sqc_idx));
        if (cur_sqc_avail_process_size <= 0 && add_round >= 100) {
          OZ(sqc_sets[best_sqc_idx].add_file(file_split_meta.remain_file_size_, 1));
          if (OB_SUCC(ret)) {
            file_split_meta.sqc_idx_ = best_sqc_idx;
            file_split_meta.remain_file_size_ = 0;
            file_split_meta.end_permyriad_ = 10000;
            file_split_meta.process_size_ += file_split_meta.remain_file_size_;
          }
          if (OB_SUCC(ret) && file_split_meta.start_permyriad_ == 0) {
            file_split_meta.should_split_ = false;
          }
          LOG_WARN("Large file file_size_ <= 0 fully assigned", K(file_split_meta.file_idx_),
                   K(best_sqc_idx),
                   K(file_split_meta.should_split_),
                   K(file_split_meta.start_permyriad_),
                   K(file_split_meta.end_permyriad_),
                   K(file_split_meta.process_size_),
                   K(file_split_meta.remain_file_size_),
                   K(file_split_meta.file_size_),
                   K(cur_sqc_avail_process_size), K(parallel));
        } else {
          // 文件分到cur_sqc_avail_process_size
          int64_t cur_sqc_process_size = std::min(cur_sqc_avail_process_size, file_split_meta.remain_file_size_);
          int64_t file_after_cut_size = file_split_meta.remain_file_size_ - cur_sqc_process_size;
          int64_t remain_low_limit_size = file_split_meta.remain_file_size_ * 0.2;
          // cur_sqc_avail_process_size 过小的时候，分割文件 小于1.1倍剩余文件不额外分回来
          if (cur_sqc_avail_process_size < file_split_meta.remain_file_size_ && file_after_cut_size > remain_low_limit_size) {
            int permyriad = ceil((cur_sqc_process_size * 1.00 / file_split_meta.file_size_) * 10000.0);  // 万分比
            // 能放多少放多少，剩余部分重新加入队列，分配给其他SQC
            OX(cur_sqc_process_size = file_split_meta.file_size_ * permyriad / 10000);
            OX(file_after_cut_size = file_split_meta.remain_file_size_ - cur_sqc_process_size);
            OZ(sqc_sets[best_sqc_idx].add_file(cur_sqc_process_size, 1));


            if (OB_SUCC(ret)) {
              sorted_files.at(i).end_permyriad_ = sorted_files.at(i).start_permyriad_ + permyriad;
              sorted_files.at(i).sqc_idx_ = best_sqc_idx;
              sorted_files.at(i).process_size_ += cur_sqc_process_size;
            }

            if (sorted_files.at(i).end_permyriad_ < 10000) {
              // 将剩余部分重新加入队列，分配给其他SQC,
              OZ(sorted_files.push_back(FileInfoWithIdx(file_split_meta.file_size_,
                  file_split_meta.file_idx_, // files idx
                  -1,  // sqc_idx -1 means to be assigned
                  file_after_cut_size, // remain_file_size
                  true, // should_split
                  sorted_files.at(i).end_permyriad_,  // start precent this file end percent is next file start percent
                  10000))); // end percent this file 100
            }
            LOG_INFO("Large ODPS file split assigned",
                      K(file_split_meta.file_idx_),
                      K(file_split_meta.start_permyriad_),
                      K(file_split_meta.end_permyriad_),
                      K(best_sqc_idx),
                      K(file_split_meta.process_size_),
                      K(file_split_meta.remain_file_size_),
                      K(file_after_cut_size),
                      K(cur_sqc_process_size),
                      K(file_split_meta.file_size_),
                      K(parallel));

          } else {
            // 文件可以完全分配给当前SQC
            int64_t processing_size = file_split_meta.remain_file_size_;
            OZ(sqc_sets[best_sqc_idx].add_file(file_split_meta.remain_file_size_, 1));
            if (OB_SUCC(ret)) {
              file_split_meta.sqc_idx_ = best_sqc_idx;
              file_split_meta.remain_file_size_ = 0;
              file_split_meta.end_permyriad_ = 10000;
              file_split_meta.process_size_ +=
                  processing_size;
            }
            if (OB_SUCC(ret) && file_split_meta.start_permyriad_ == 0) {
              file_split_meta.should_split_ = false;
            }
            LOG_INFO("Large ODPS file fully assigned", K(file_split_meta.file_idx_),
                     K(file_split_meta.start_permyriad_),
                     K(file_split_meta.end_permyriad_), K(best_sqc_idx),
                     K(processing_size), K(file_split_meta.remain_file_size_),
                     K(file_split_meta.file_size_), K(parallel));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && i == threshold) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to assign all files", K(ret), K(i), K(threshold));
  }

  // 第六步：输出分配结果统计
  if (OB_SUCC(ret)) {
    LOG_TRACE("File assignment completed", K(file_sizes.count()), K(parallel));
    for (int64_t i = 0; i < parallel; i++) {
      int64_t target_size = avg_size_per_sqc;
      LOG_TRACE("SQC assignment result", K(i), K(sqc_sets[i].total_file_size_),
                K(sqc_sets[i].total_file_count_), K(target_size),
                K(sqc_sets[i].total_processing_time_ms_));
    }
  }
  return ret;
}

/// Optimizer-time assignment of ODPS scan units to per-server slots. Inputs
/// come from the pruner (format_str_, the projected column indexes,
/// file_filter_spec_); the output lands in per-server-slot file lists that
/// ObLakeTablePartitionInfo::select_location_for_odps turns into candidate
/// tablet locations. plan_files emits a single dummy unit when no real unit
/// was produced at all.
///
///   fetch: TUNNEL collects per-partition sizes (SQL CALC_ODPS_SIZE path);
///          STORAGE opens a download session over the whole partition list —
///          with the optimizer-time re-derived pushdown predicate when it can
///          be printed (see print_optimizer_pushdown_predicate) — and fetches
///          the split count / total row count. BYTE uses a temporary ROW
///          session for the row count while retaining its BYTE session for
///          split assignment and execution;
///   split: TUNNEL carves big partitions by the load-aware heuristic, STORAGE
///          byte/row spreads the split-index / row-range units round-robin.
int ObODPSFilePruner::assign_odps_file_to_sqcs_(ObExecContext &exec_ctx,
                                                ObIArray<ObOptOdpsFile *> &files,
                                                int64_t parallel,
                                                ObIArray<ObOdpsSlotFiles> &slot_lists)
{
  int ret = OB_SUCCESS;
  estimated_row_count_ = -1;
  bool one_partition_per_thread = false;
  int64_t split_task_count = 0;
  int64_t table_total_row_count = 0;
  ObString session_str;
  ObString part_str;
  bool use_odps_jni_connector = true;
  int8_t odps_data_transfer_mode = 0;
  if (OB_FAIL(ObSQLUtils::parse_odps_jni_params_from_format_str(
          format_str_, use_odps_jni_connector, odps_data_transfer_mode))) {
    LOG_WARN("failed to parse odps jni params from format str", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (!use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      if (api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("storage api is not supported", K(ret));
      } else if (OB_FAIL(fetch_odps_all_partitions_info_for_task_assign_(
                     exec_ctx, files, one_partition_per_thread))) {
        LOG_WARN("failed to fetch row count", K(ret));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (api_mode_ == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (OB_FAIL(fetch_odps_all_partitions_info_for_task_assign_(
                exec_ctx, files, one_partition_per_thread))) {
          LOG_WARN("failed to fetch row count", K(ret));
        }
      } else {
        ObSqlString part_spec_str;
        int64_t part_count = files.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
          if (OB_ISNULL(files.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null odps file", K(ret), K(i));
          } else if (0 == files.at(i)->file_url_.compare(ObExternalTableUtils::dummy_file_name())) {
            // skip dummy
          } else if (OB_FAIL(part_spec_str.append(files.at(i)->file_url_))){
            LOG_WARN("failed to append file url", K(ret), K(files.at(i)->file_url_));
          } else if (i < part_count - 1 && OB_FAIL(part_spec_str.append("#"))) {
            LOG_WARN("failed to append comma", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(allocator_, part_spec_str.string(), part_str, true))) {
          LOG_WARN("failed to write string", K(ret), K(part_spec_str));
        } else if (api_mode_ == sql::ObODPSGeneralFormat::ApiMode::BYTE) {
          // the session is opened with the optimizer-time re-derived pushdown
          // predicate (null when there is no filter); a predicate that cannot
          // be printed at this stage is skipped — the session is then opened
          // without it and correctness is upheld by the OB-side TSC filter
          if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_split_by_byte(
                  exec_ctx, nonpart_col_idxs_, part_col_idxs_, part_str, format_str_,
                  need_all_ ? nullptr : &file_filter_spec_, pred_col_infos_,
                  parallel, session_str, split_task_count, table_total_row_count, allocator_))) {
            LOG_WARN("failed to get task count ", K(ret));
          } else {
            estimated_row_count_ = table_total_row_count;
          }
        } else {
          if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_split_by_row(
                  exec_ctx, nonpart_col_idxs_, part_col_idxs_, part_str, format_str_,
                  need_all_ ? nullptr : &file_filter_spec_, pred_col_infos_,
                  parallel, session_str, table_total_row_count, allocator_))) {
            LOG_WARN("failed to get total row count ", K(ret));
          } else {
            estimated_row_count_ = table_total_row_count;
          }
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS JNI connector is not enabled", K(ret));
#endif
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!use_odps_jni_connector || api_mode_ == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
    if (OB_FAIL(split_qc_for_odps_to_sqcs_by_line_tunnel_partition(
            allocator_, exec_ctx, format_str_, files, slot_lists, parallel,
            one_partition_per_thread, estimated_row_count_))) {
      LOG_WARN("failed to split odps to sqc process", K(ret));
    }
  } else if (api_mode_ == sql::ObODPSGeneralFormat::ApiMode::BYTE) {
    if (OB_FAIL(split_qc_for_odps_to_sqcs_storage_api_byte(
            allocator_, split_task_count, session_str, part_str, slot_lists))) {
      LOG_WARN("failed to split odps to sqc process in byte mode", K(ret));
    }
  } else {
    if (OB_FAIL(split_qc_for_odps_to_sqcs_storage_api_row(
            allocator_, table_total_row_count, session_str, part_str, slot_lists, parallel))) {
      LOG_WARN("failed to split odps to sqc process in row mode", K(ret));
    }
  }
  return ret;
}

/// Per-partition size collection through the SQL CALC_ODPS_SIZE path (shared
/// with the statistics path via the exec-ctx level partition-size cache).
/// `_max_partition_count_to_collect_statistic == 0` (or too many uncollected
/// partitions) falls back to the original assignment: one whole partition per
/// unit, round-robin. Sizes are fetched against the pruner's own format_str_
/// and ref_table_id.
int ObODPSFilePruner::fetch_odps_all_partitions_info_for_task_assign_(
    ObExecContext &exec_ctx,
    ObIArray<ObOptOdpsFile *> &files,
    bool &one_partition_per_thread)
{
  int ret = OB_SUCCESS;
  int64_t total_file_count = files.count();
  int64_t collected_file_count = 0;
  int64_t uncollected_file_count = 0;
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t max_parttition_count_to_collect_statistic = 10;
  if (OB_LIKELY(tenant_config.is_valid())) {
    max_parttition_count_to_collect_statistic = tenant_config->_max_partition_count_to_collect_statistic;
  }
  for (int i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    if (OB_ISNULL(files.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null odps file", K(ret), K(i));
    } else if (files.at(i)->file_size_ >= 0) { // 内表收集行数, catalog表收集file_size
      ++collected_file_count;
    } else {
      ++uncollected_file_count;
    }
  }

  // 0: keep the original assignment (one whole partition per unit,
  // round-robin across slots) even when sizes are already known.
  if (0 == max_parttition_count_to_collect_statistic) {
    one_partition_per_thread = true;
  } else if (collected_file_count == total_file_count) {
    // do nothing
  } else if (uncollected_file_count > max_parttition_count_to_collect_statistic) {
    one_partition_per_thread = true;
  } else {
    common::hash::ObHashMap<ObOdpsPartitionKey, int64_t>& partition_str_to_file_size = exec_ctx.get_odps_partition_str_to_file_size();
    if (OB_SUCC(ret)) {
      ObSQLSessionInfo *session = NULL;
      if (OB_ISNULL(session = exec_ctx.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      }
      OZ(ObOdpsTableUtils::fetch_odps_all_partitions_size(*session, format_str_, files,
                                 tenant_id, loc_meta_.ref_table_id_, exec_ctx.get_allocator(), partition_str_to_file_size));

      for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
        if (OB_ISNULL(files.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null odps file", K(ret), K(i));
        } else if (0 == files.at(i)->file_url_.compare(ObExternalTableUtils::dummy_file_name())) {
          // do nothing
        } else {
          int64_t file_size = 0;
          OZ(partition_str_to_file_size.get_refactored(ObOdpsPartitionKey(loc_meta_.ref_table_id_, files.at(i)->file_url_), file_size));
          OX(files.at(i)->file_size_ = file_size);
        }

      }
      if (OB_FAIL(ret)) {
        // sizes are a best-effort input of the assignment heuristic: give up
        // gracefully and assign one whole partition per unit instead.
        ret = OB_SUCCESS;
        one_partition_per_thread = true;
      }
    }
  }
  return ret;
}

int ObODPSFilePruner::plan_files(ObExecContext &exec_ctx,
                                   const ObDMLStmt &stmt,
                                   ObIArray<ObOptOdpsFile *> &files,
                                   ObIArray<int64_t> &slot_idxs,
                                   const int64_t estimated_parallel)
{
  int ret = OB_SUCCESS;
  estimated_row_count_ = -1;
  int64_t server_count = 1;
  ObSEArray<ObOdpsSlotFiles, 4> slot_lists;
  ObSEArray<ObOptOdpsFile *, 16> units;
  ObSEArray<int64_t, 16> unit_slots;
  UNUSED(stmt);
  if (OB_FAIL(get_server_count_(exec_ctx, server_count))) {
    LOG_WARN("failed to get server count", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (files.empty()) {
    // every partition was pruned away (or none is materialized): skip all the
    // ODPS API calls; the dummy unit is appended below
    // An empty selected partition set is known to contain zero rows in all modes.
    estimated_row_count_ = 0;
  } else {
    // The dop is not decided yet at this stage; the estimate only affects the
    // split granularity (the workers pull granules dynamically). Caller
    // (ObJoinOrder) already resolved table/global hint and AUTO DOP via
    // get_explicit_dop_for_path; fall back to alive server count.
    int64_t parallel = server_count;
    if (estimated_parallel > parallel) {
      parallel = estimated_parallel;
    }
    if (OB_FAIL(slot_lists.prepare_allocate(server_count))) {
      LOG_WARN("failed to prepare slot file lists", K(ret));
    } else if (OB_FAIL(assign_odps_file_to_sqcs_(exec_ctx, files, parallel, slot_lists))) {
      LOG_WARN("failed to assign odps file to server slots", K(ret));
    }
    for (int64_t s = 0; OB_SUCC(ret) && s < slot_lists.count(); ++s) {
      const ObIArray<ObOptOdpsFile *> &slot_files = slot_lists.at(s).get_files();
      for (int64_t i = 0; OB_SUCC(ret) && i < slot_files.count(); ++i) {
        if (OB_FAIL(units.push_back(slot_files.at(i)))) {
          LOG_WARN("failed to push back odps scan unit", K(ret), K(s), K(i));
        } else if (OB_FAIL(unit_slots.push_back(s))) {
          LOG_WARN("failed to push back odps scan unit slot", K(ret), K(s), K(i));
        }
      }
    }
  }
  // keep the "lake file map always has an entry" invariant even when the
  // assignment produced zero units (all partitions pruned / 0-row table): the
  // ODPS iterators end a dummy file immediately (0 rows).
  if (OB_SUCC(ret) && units.empty()) {
    ObOptOdpsFile *file = NULL;
    if (OB_FAIL(create_odps_file_(ObString::make_string(ObExternalTableUtils::dummy_file_name()),
                                  0, file))) {
      LOG_WARN("failed to create dummy odps file", K(ret));
    } else if (OB_FAIL(units.push_back(file))) {
      LOG_WARN("failed to push back dummy odps file", K(ret));
    } else if (OB_FAIL(unit_slots.push_back(0))) {
      LOG_WARN("failed to push back dummy odps file slot", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    files.reset();
    if (OB_FAIL(files.assign(units))) {
      LOG_WARN("failed to assign odps scan units", K(ret));
    } else if (OB_FAIL(slot_idxs.assign(unit_slots))) {
      LOG_WARN("failed to assign odps scan unit slots", K(ret));
    }
  }
  return ret;
}
