/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/file_prune/ob_ext_file_pruner.h"

#include "sql/resolver/dml/ob_hint.h"  // ObOptParamHint::EXT_TABLE_OPTIONS
#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"
#include "plugin/v2/external_table/ob_ext_table_metadata.h"
#include "plugin/v2/host/ob_ext_host_provider.h"
#include "plugin/v2/host/ob_ext_malloc_guard.h"
#include "plugin/v2/external_table/ob_ext_plugin_util.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // build_options_json, parse_scan_tasks_json
#include "share/rc/ob_tenant_base.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"

#include <string>

namespace oceanbase
{
namespace sql
{
using namespace ext_plugin;

int ObPluginSplitDesc::assign(ObIAllocator &allocator, const ObPluginSplitDesc &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ob_write_string(allocator, other.task_json_, task_json_))) {
      LOG_WARN("failed to copy plugin task json", K(ret));
    } else {
      record_count_ = other.record_count_;
    }
  }
  return ret;
}

void ObPluginSplitDesc::reset()
{
  task_json_.reset();
  record_count_ = 0;
}

ObExtFilePruner::ObExtFilePruner(ObIAllocator &allocator)
    : ObILakeTableFilePruner(allocator), table_uri_(), access_info_(),
      plugin_format_(share::ObLakeTableFormat::INVALID), ext_metadata_(nullptr), filter_exprs_(),
      exec_ctx_(nullptr)
{
}

int ObExtFilePruner::clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const
{
  int ret = OB_SUCCESS;
  pruner = nullptr;
  ObExtFilePruner *tmp = nullptr;
  if (OB_ISNULL(tmp = OB_NEWx(ObExtFilePruner, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObExtFilePruner");
  } else if (OB_FAIL(tmp->assign(*this))) {
    LOG_WARN("failed to assign ext file pruner");
  } else {
    pruner = tmp;
  }
  return ret;
}

int ObExtFilePruner::assign(const ObILakeTableFilePruner &o)
{
  int ret = OB_SUCCESS;
  const ObExtFilePruner &other = static_cast<const ObExtFilePruner &>(o);
  if (OB_FAIL(ObILakeTableFilePruner::assign(other))) {
    LOG_WARN("failed to assign ObILakeTableFilePruner", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, other.table_uri_, table_uri_))) {
    LOG_WARN("failed to deep copy table uri", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, other.access_info_, access_info_))) {
    LOG_WARN("failed to deep copy access info", K(ret));
  } else {
    plugin_format_ = other.plugin_format_;
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator_, other.ext_options_hint_,
                                     ext_options_hint_, true))) {
    LOG_WARN("failed to deep copy ext_options hint", K(ret));
  } else {
    ext_metadata_ = other.ext_metadata_;
    exec_ctx_ = other.exec_ctx_;
  }
  // filter_exprs_ are optimizer-stage pointers, not deep-copied in assign
  return ret;
}

int ObExtFilePruner::init(const ObDMLStmt &stmt,
                          ObExecContext *exec_ctx,
                          const uint64_t table_id,
                          const uint64_t ref_table_id,
                          const ext_plugin::ObExtTableMetadata *ext_metadata,
                          const ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ext file pruner already inited", K(ret));
  } else {
    loc_meta_.table_loc_id_ = table_id;
    loc_meta_.ref_table_id_ = ref_table_id;
    loc_meta_.route_policy_ = READONLY_ZONE_FIRST;
    loc_meta_.is_external_table_ = true;
    loc_meta_.is_lake_table_ = true;
    loc_meta_.is_external_files_on_disk_ = false;
    if (OB_ISNULL(ext_metadata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ext table metadata is null", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ext_metadata->get_table_location(),
                                       table_uri_, true))) {
      LOG_WARN("failed to deep copy table uri", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ext_metadata->get_access_info(),
                                       access_info_, true))) {
      LOG_WARN("failed to deep copy access info", K(ret));
    } else if (OB_FAIL(generate_column_meta_info(stmt))) {
      LOG_WARN("failed to generate column meta info", K(ret));
    } else if (OB_FAIL(filter_exprs_.assign(filter_exprs))) {
      LOG_WARN("failed to store filter exprs", K(ret));
    } else {
      plugin_format_ = ext_metadata->get_format_type();
      ext_metadata_ = ext_metadata;
      exec_ctx_ = exec_ctx;
      inited_ = true;
      // Pull the per-query EXT_TABLE_OPTIONS opt_param hint (an opaque JSON
      // string) and deep-copy it; it is later passed verbatim into the plugin's
      // options_json under OB_EXT_K_EXT_OPTIONS. OB never inspects the contents.
      // A missing/invalid hint simply leaves ext_options_hint_ empty (no tuning).
      if (OB_NOT_NULL(stmt.get_query_ctx())) {
        ObObj opt_val;
        int64_t opt_idx = common::OB_INVALID_INDEX;
        if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_opt_param(
                ObOptParamHint::EXT_TABLE_OPTIONS, opt_val, &opt_idx))) {
          LOG_WARN("failed to get ext_table_options hint", K(ret));
          ret = OB_SUCCESS;  // non-fatal: scan without per-query tuning
          ext_options_hint_.reset();
        } else if (opt_idx != common::OB_INVALID_INDEX && opt_val.is_varchar()) {
          if (OB_FAIL(ob_write_string(allocator_, opt_val.get_varchar(),
                                      ext_options_hint_, true))) {
            LOG_WARN("failed to deep copy ext_table_options hint", K(ret));
            ret = OB_SUCCESS;  // non-fatal
            ext_options_hint_.reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObExtFilePruner::prune_ext_splits(ObExecContext &exec_ctx,
                                      ObIArray<ObPluginSplitDesc *> &splits,
                                      ObExtTableDispatchMode &dispatch_mode)
{
  int ret = OB_SUCCESS;
  dispatch_mode = ObExtTableDispatchMode::ROUND_ROBIN;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ext file pruner not inited", K(ret));
  } else {
    const share::ObPluginSlot slot = share::lake_plugin_slot_of(plugin_format_);
    const ObExtTablePluginApi *api =
        share::ObExtFormatRegistry::get_instance().get_plugin_by_slot(slot);
    if (OB_ISNULL(api) || OB_ISNULL(api->plan_create) || OB_ISNULL(api->tasks_destroy)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "external table plugin not loaded");
      LOG_WARN("ext plugin not loaded for format", K(ret), K(plugin_format_), K(slot));
    } else {
      const char *fmt = (OB_NOT_NULL(api->format_name)) ? api->format_name() : nullptr;
      ObExtMallocGuard guard(fmt);  // tag plugin mallocs from plan_create

      ObExtHostCtx host_ctx;
      if (OB_FAIL(host_ctx.default_fs.set_path(table_uri_))) {
        LOG_WARN("failed to set fs path", K(ret));
      } else if (OB_FAIL(host_ctx.default_fs.set_access_info(access_info_))) {
        LOG_WARN("failed to set fs access info", K(ret));
      }
      host_ctx.default_fs.set_cache_options(ObExternalFileCacheOptions(true /*enable_page_cache*/,
                                                                       true /*enable_disk_cache*/));
      host_ctx.default_fs.set_tenant_id(MTL_ID());
      host_ctx.select_arrow_pool(true);
      host_ctx.pool->set_attr(get_ext_mem_attr(fmt));
      host_ctx.executor = nullptr;  // planning runs inline
      ObExtTableHostApi host;
      build_ext_host_api(host, &host_ctx);

      // options: location + access info + optional catalog_context (T0 opaque
      // blob from load_schema) + optional ext_options hint. catalog_context and
      // ext_options are embedded as raw JSON objects (no quoting/escaping).
      // Arena copies are NUL-terminated and valid for the whole stage.
      const ObString &loc_str = host_ctx.default_fs.path();
      const ObString &acc_str = host_ctx.default_fs.access_info();
      const ObString &catalog_ctx = ext_metadata_->get_catalog_context_json();
      const bool has_catalog_ctx = !catalog_ctx.empty();
      const bool has_ext_options = !ext_options_hint_.empty();
      const char *keys[4] = {"location", "access_info", nullptr, nullptr};
      const char *vals[4] = {loc_str.ptr(), acc_str.ptr(), nullptr, nullptr};
      const char *raw_keys[2] = {nullptr, nullptr};
      int32_t opt_count = 2;
      int32_t raw_count = 0;
      if (has_catalog_ctx) {
        keys[opt_count] = OB_EXT_K_CATALOG_CONTEXT;
        vals[opt_count] = catalog_ctx.ptr();
        raw_keys[raw_count++] = OB_EXT_K_CATALOG_CONTEXT;
        ++opt_count;
      }
      if (has_ext_options) {
        keys[opt_count] = OB_EXT_K_EXT_OPTIONS;
        vals[opt_count] = ext_options_hint_.ptr();
        raw_keys[raw_count++] = OB_EXT_K_EXT_OPTIONS;
        ++opt_count;
      }
      ObString options_json;
      ObArenaAllocator opt_alloc("ExtPrunerOpt");
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(share::build_options_json(opt_alloc, keys, vals, opt_count,
                                                   options_json,
                                                   raw_keys, raw_count))) {
        LOG_WARN("failed to build options json", K(ret));
      } else {
        // Build the single predicate-tree JSON from the optimizer filter exprs.
        // OB does NOT split partition vs residual: the plugin's SDK splits the
        // one Predicate internally (paimon: CreatePickedFieldFilter picks
        // partition-key conjuncts for pruning, ExcludePredicateWithFields yields
        // the residual row predicate) — mirroring the deleted native paimon
        // path that only called SetPredicate. The plan_create
        // partition_filter_json argument is therefore passed NULL (kept in the
        // ABI; not bumped). predicate_json may be empty (NULL -> no pushdown).
        // See the protocol comment in ob_external_table_plugin.h for the grammar.
        ObString predicate_json;
        const common::ObIArray<uint64_t> &part_col_ids = ext_metadata_->get_partition_col_ids();
        if (OB_FAIL(ext_predicate::build_predicate_json_from_raw_expr(
                allocator_, exec_ctx_, filter_exprs_, part_col_ids, predicate_json))) {
          LOG_WARN("failed to build predicate json, scanning without pushdown",
                   K(ret), K(part_col_ids.count()));
          ret = OB_SUCCESS;  // degrade gracefully: no pushdown, OB filters rows
          predicate_json.reset();
        }
        const char *predicate_cstr =
            predicate_json.empty() ? nullptr : predicate_json.ptr();
        char *tasks_json = nullptr;
        int32_t tasks_len = 0;
        // desired_task_count is a PX parallelism hint the plugin may ignore.
        // The plugin returns an OB errno verbatim and logs its own diagnostic
        // (with plugin-side source location) via host->log before returning —
        // grep "[ExtPlugin]" in observer.log for the stack.
        int rc = api->plan_create(loc_str.ptr(), options_json.ptr(),
                                  /*partition_filter_json*/ nullptr, predicate_cstr,
                                  /*limit*/ -1, /*desired_task_count*/ 1,
                                  &host, &tasks_json, &tasks_len);
        if (rc != OB_SUCCESS || OB_ISNULL(tasks_json) || tasks_len <= 0) {
          ret = (rc != OB_SUCCESS) ? rc : OB_ERR_UNEXPECTED;
          LOG_WARN("plugin plan_create failed", K(ret));
        } else {
          share::ObExtScanTaskArray scan_tasks = {};
          ObArenaAllocator task_alloc("ExtPrunerTask");
          if (OB_FAIL(share::parse_scan_tasks_json(task_alloc, tasks_json, tasks_len,
                                                   scan_tasks))) {
            LOG_WARN("failed to parse scan tasks json", K(ret));
          } else {
            LOG_INFO("ext pruner plan_create result",
                     K(predicate_json),
                     "predicate_pushed", !predicate_json.empty(),
                     "scan_task_count", scan_tasks.count);
            for (int32_t i = 0; OB_SUCC(ret) && i < scan_tasks.count; ++i) {
              const share::ObExtScanTask &t = scan_tasks.data[i];
              ObPluginSplitDesc *desc = OB_NEWx(ObPluginSplitDesc, &allocator_, allocator_);
              if (OB_ISNULL(desc)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc ObPluginSplitDesc", K(ret), K(i));
              } else if (OB_FAIL(ob_write_string(allocator_,
                                                  ObString(t.size, t.data),
                                                  desc->task_json_))) {
                LOG_WARN("failed to deep copy task json", K(ret), K(i));
              } else {
                desc->record_count_ = (t.row_count > 0) ? t.row_count : 0;
                if (OB_FAIL(splits.push_back(desc))) {
                  LOG_WARN("failed to push back split desc", K(ret), K(i));
                }
              }
            }
          }
        }
        // Release the plugin's output buffer via the plugin's tasks_destroy (the
        // plugin owns the release — it may be static/own-alloc/host-alloc).
        ob_ext_tasks_destroy(api, tasks_json, tasks_len, &host);
      }
    }
  }
  return ret;
}


} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
