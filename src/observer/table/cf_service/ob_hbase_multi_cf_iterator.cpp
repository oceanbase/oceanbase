/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SERVER
#include "ob_hbase_multi_cf_iterator.h"
#include "share/table/ob_table_util.h"
#include "observer/table/part_calc/ob_table_part_clip.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace table
{
ObHbaseMultiCFIterator::ObHbaseMultiCFIterator(const ObHbaseQuery &hbase_query, ObTableExecCtx &exec_ctx) 
    : ObHbaseQueryResultIterator(hbase_query, exec_ctx),
      hbase_query_(hbase_query),
      exec_ctx_(exec_ctx),
      compare_(nullptr),
      merge_iter_(nullptr)
{}

int ObHbaseMultiCFIterator::init()
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = hbase_query_.get_query();

  ObQueryFlag::ScanOrder scan_order = query.get_scan_order();
  if (OB_FAIL(init_cf_iters())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to init cf iterators", K(ret));
    }
  } else if (OB_ISNULL(merge_iter_ = OB_NEWx(ResultMergeIterator, &allocator_, query))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create merge_iter_", K(ret));
  } else if (OB_FAIL(merge_iter_->assign_inner_result_iters(cf_iters_))) {
    LOG_WARN("fail to assign results", K(ret));
  } else if (scan_order == ObQueryFlag::Reverse && OB_ISNULL(compare_ = OB_NEWx(ObTableHbaseRowKeyReverseCompare, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (scan_order == ObQueryFlag::Forward && OB_ISNULL(compare_ = OB_NEWx(ObTableHbaseRowKeyDefaultCompare, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (OB_FAIL(merge_iter_->init(compare_))) {
    LOG_WARN("fail to build merge_iter_", K(ret));
  } else {}

  if (OB_FAIL(ret) && OB_NOT_NULL(merge_iter_)) {
    merge_iter_->~ResultMergeIterator();
    allocator_.free(merge_iter_);
    merge_iter_ = nullptr;
  }
  return ret;
}

int ObHbaseMultiCFIterator::init_cf_queries(ObTableExecCtx &exec_ctx, const ObHbaseQuery &hbase_query)
{
  int ret = OB_SUCCESS;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas;
  table_schemas.set_attr(ObMemAttr(MTL_ID(), "CFIterSchemas"));
  const ObTableQuery &query = hbase_query.get_query();
  const uint64_t first_table_id = hbase_query.get_table_id();
  ObTableApiCredential &credential = exec_ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
  const ObSimpleTableSchemaV2 *first_table_schema = nullptr;
  uint64_t tablegroup_id = OB_INVALID_ID;
  uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, first_table_id, first_table_schema))) {
    LOG_WARN("fail to get simple table schema", K(ret), K(tenant_id), K(first_table_id));
  } else if (OB_ISNULL(first_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(tenant_id), K(first_table_id));
  } else if (FALSE_IT(tablegroup_id = first_table_schema->get_tablegroup_id())) {
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup", K(ret), K(tenant_id), K(tablegroup_id));
  } else if (table_schemas.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is empty", K(ret));
  } else {
    if (query.get_scan_order() == ObQueryFlag::Reverse) {
      lib::ob_sort(table_schemas.begin(), table_schemas.end(), schema_cmp_func_for_reverse);
    } else {
      lib::ob_sort(table_schemas.begin(), table_schemas.end(), schema_cmp_func);

    }
  }

  ObSEArray<std::pair<int64_t, int64_t>, 1> part_subpart_ids; 
  part_subpart_ids.set_attr(ObMemAttr(MTL_ID(), "HbasePartids"));
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard &schema_guard = exec_ctx.get_schema_guard();
    int64_t part_idx = OB_INVALID_INDEX;
    int64_t subpart_idx = OB_INVALID_INDEX;
    const ObIArray<common::ObTabletID> &frist_tablet_ids = query.get_tablet_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < frist_tablet_ids.count(); i++) {
      if (OB_FAIL(ObTableUtils::get_part_idx_by_tablet_id(schema_guard,
                                                          first_table_id,
                                                          frist_tablet_ids.at(i),
                                                          part_idx, 
                                                          subpart_idx))) {
        LOG_WARN("fail to get part idx", K(ret), K(hbase_query.get_table_id()), K(hbase_query.get_tablet_id()));
      } else if (OB_FAIL(part_subpart_ids.push_back(std::make_pair(part_idx, subpart_idx)))) {
        LOG_WARN("fail to add part subpart idx", K(ret));
      }
    }
  }


  if (OB_SUCC(ret)) {
    const ObIArray<ObString>& columns = query.htable_filter().get_columns();
    ObArray<std::pair<ObString, bool>> family_addfamily_flag_pairs;
    ObArray<ObString> real_columns;
    family_addfamily_flag_pairs.prepare_allocate_and_keep_count(8);
    real_columns.prepare_allocate_and_keep_count(8);
    const char *first_table_name = first_table_schema->get_table_name();
    const char *end = strchr(first_table_name, '$');
    ObString tablegroup_name;
    if (end == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table name without $", K(ret));
    } else {
      tablegroup_name.assign_ptr(first_table_name, end - first_table_name);
    }

    if (OB_SUCC(ret) && OB_FAIL(ObHTableUtils::process_columns(columns, family_addfamily_flag_pairs, real_columns))) {
      LOG_WARN("fail to process columns", K(ret));
    }

    bool is_empty_family = family_addfamily_flag_pairs.empty();
    for (int i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i); 
      bool is_found = true;
      ObHbaseCFQuery *cf_query = nullptr;
      std::pair<ObString, bool> is_add_family;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schemas is NULL", K(ret));
      } else if (!is_empty_family) {
        if (OB_FAIL(ObHTableUtils::check_family_existence_with_base_name(table_schema->get_table_name_str(),
                                                                         tablegroup_name,
                                                                         ObTableEntityType::ET_HKV,
                                                                         family_addfamily_flag_pairs,
                                                                         is_add_family,
                                                                         is_found))) {
          LOG_WARN("fail to check family exist", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_found) {
        if (OB_ISNULL(cf_query = OB_NEWx(ObHbaseCFQuery, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate hbase cf query", K(ret));
        } else if (OB_FAIL(query.deep_copy(allocator_, cf_query->get_query()))) {
          LOG_WARN("fail to deep copy query", K(ret));
        } else if (!is_empty_family &&
            OB_FAIL(ObHTableUtils::update_query_columns(cf_query->get_query(),
                                                        family_addfamily_flag_pairs,
                                                        real_columns,
                                                        is_add_family))) {
          LOG_WARN("fail to update query columns", K(ret), K(family_addfamily_flag_pairs), K(real_columns));
        } else if (OB_FAIL(update_tablet_ids_by_part_ids(part_subpart_ids, *table_schema,
                                                         const_cast<ObTableQuery &>(cf_query->get_query())))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to update tablet id by part ids", K(ret));
          }
        } else {
          cf_query->set_table_id(table_schema->get_table_id());
          // set correct table name
          cf_query->set_table_name(table_schema->get_table_name());
          if (OB_FAIL(cf_queries_.push_back(cf_query))) {
            LOG_WARN("fail to add cf query", K(ret));
          }
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(cf_query)) {
          cf_query->~ObHbaseCFQuery();
          allocator_.free(cf_query);
          cf_query = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObHbaseMultiCFIterator::init_cf_iters()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_cf_queries(exec_ctx_, hbase_query_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to init cf queries", K(ret), K(hbase_query_));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cf_queries_.count(); i++) {
    ObHbaseCFIterator *cf_iter = nullptr;
    const ObHbaseCFQuery *cf_query = cf_queries_.at(i);
    if (OB_ISNULL(cf_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null cf query", K(ret));
    } else if (OB_ISNULL(cf_iter = OB_NEWx(ObHbaseCFIterator, &allocator_, cf_query->get_hbase_query(), exec_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate cf iter", K(ret));
    } else if (OB_FAIL(cf_iter->init())) {
      LOG_WARN("fail to init cf iter", K(ret));
    } else if (OB_FAIL(cf_iters_.push_back(cf_iter))) {
      LOG_WARN("fail to add cf iter", K(ret));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(cf_iter)) {
      cf_iter->close();
      cf_iter->~ObHbaseCFIterator();
      allocator_.free(cf_iter);
      cf_iter = nullptr;
    }
  }
  LOG_DEBUG("finish init cf iters", K(ret), K(cf_queries_), K(cf_iters_));
  return ret;
}

int ObHbaseMultiCFIterator::get_next_result(ObTableQueryResult &hbase_wide_rows)
{
  return inner_get_next_result(hbase_wide_rows);
}

int ObHbaseMultiCFIterator::get_next_result(ObTableQueryIterableResult &hbase_wide_rows)
{
  return inner_get_next_result(hbase_wide_rows);
}

int ObHbaseMultiCFIterator::get_next_result(ObTableQueryIterableResult *&hbase_wide_rows)
{
  return inner_get_next_result(hbase_wide_rows);
}

bool ObHbaseMultiCFIterator::has_more_result() const
{
  bool bret = false;
  if (OB_NOT_NULL(merge_iter_)) {
    bret = merge_iter_->has_more_result();
  }
  return bret;
}

int ObHbaseMultiCFIterator::update_tablet_ids_by_part_ids(ObIArray<std::pair<int64_t, int64_t>> &part_subpart_ids,
                                                          const ObSimpleTableSchemaV2 &table_schema,
                                                          ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTabletID> &tablet_ids = query.get_tablet_ids();
  tablet_ids.reset();
  ObTabletID real_tablet_id;
  ObObjectID tmp_object_id = OB_INVALID_ID;
  ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_subpart_ids.count(); i++) {
    uint64_t part_idx = part_subpart_ids.at(i).first;
    uint64_t subpart_idx = part_subpart_ids.at(i).second;
  if (part_idx == OB_INVALID_INDEX) {
      if (part_subpart_ids.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must be one part id for non-partitioned table", K(ret), K(part_subpart_ids));
      } else {
        real_tablet_id = table_schema.get_tablet_id();
      }
    } else {
      if (OB_FAIL(table_schema.get_part_id_and_tablet_id_by_idx(part_idx,
                                                                subpart_idx,
                                                                tmp_object_id,
                                                                tmp_first_level_part_id,
                                                                real_tablet_id))) {
        LOG_WARN("fail to get_part_id_and_tablet_id_by_idx", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_ids.push_back(real_tablet_id))) {
      LOG_WARN("fail to add real tablet id", K(ret));
    } else if (query.is_hot_only()) {
      ObSEArray<ObTabletID, 32> src_tablet_ids;
      if (OB_FAIL(src_tablet_ids.assign(tablet_ids))) {
        LOG_WARN("fail to assign tablet_ids", K(ret), K(tablet_ids));
      } else {
        tablet_ids.reset();
        if (OB_FAIL(ObTablePartClipper::clip(table_schema,
                                             ObTablePartClipType::HOT_ONLY,
                                             src_tablet_ids,
                                             tablet_ids))) {
          LOG_WARN("fail to clip partition", K(ret), K(src_tablet_ids));
        } else if (tablet_ids.empty()) {
          ret = OB_ITER_END;
          LOG_DEBUG("all partitions are clipped", K(query));
        }
      }

    }
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase