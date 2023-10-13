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

#include "lib/worker.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_udf_mgr.h"
#include "share/schema/ob_schema_mgr.h"
#include "rootserver/ob_locality_util.h"

#include "pl/ob_pl_stmt.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

#define EXTRACT_PRIV_FROM_MYSQL_RESULT(result, column_name, obj, priv_type) \
  if (OB_SUCC(ret)) { \
    int64_t int_value = 0;  \
    if (OB_FAIL((result).get_int(#column_name, int_value))) {   \
      SHARE_SCHEMA_LOG(WARN, "Fail to get privilege in row", "priv_name", #priv_type, K(ret)); \
    } else {   \
      1 == int_value ? (obj).set_priv(OB_##priv_type) : (void) 0;      \
    }  \
  }

#define EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, column_name, obj, priv_type, skip_column_error) \
  if (OB_SUCC(ret)) { \
    int64_t priv_col_value = 0;  \
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, #column_name, priv_col_value, int64_t, false, skip_column_error, 0) \
    if (OB_SUCC(ret)) {   \
      1 == priv_col_value ? (obj).set_priv(OB_##priv_type) : (void) 0;      \
    }  \
  }

#define EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE_NULL_AND_IGNORE_COLUMN_ERROR(result, column_name, obj, priv_type) \
  if (OB_SUCC(ret)) { \
    int64_t priv_col_value = 0;  \
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, #column_name, priv_col_value, int64_t, true, true, 0) \
    if (OB_SUCC(ret)) {   \
      1 == priv_col_value ? (obj).set_priv(OB_##priv_type) : (void) 0;      \
    }  \
  }

#define EXTRACT_PRIV_FIELD_FROM_MYSQL_RESULT(result, column_name, priv_set, priv_type) \
  if (OB_SUCC(ret)) { \
    int64_t int_value = 0;  \
    if (OB_FAIL((result).get_int(#column_name, int_value))) {   \
      SHARE_SCHEMA_LOG(WARN, "Fail to get privilege in row", "priv_name", #priv_type, K(ret)); \
    } else if (1 == int_value) { \
      priv_set |= OB_##priv_type;  \
    }  \
  }

/*********************************************************************
 *
 * for full schemas
 *
 *********************************************************************/

template<typename T, typename TABLE_SCHEMA>
int ObSchemaRetrieveUtils::retrieve_table_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObIAllocator &allocator,
    ObIArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_table_id = common::OB_INVALID_ID;
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  TABLE_SCHEMA table_schema(&tmp_allocator);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    table_schema.reset();
    tmp_allocator.reuse();
    bool is_deleted = false;
    TABLE_SCHEMA *allocated_table_schema = NULL;
    if (OB_FAIL(fill_table_schema(tenant_id, check_deleted, result, table_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill table schema", KR(ret), K(check_deleted));
    } else if (table_schema.get_table_id() == prev_table_id) {
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO,"table is is_deleted, don't add to table_schema_array",
                       "table_id", table_schema.get_table_id(),
                       "table_name", table_schema.get_table_name(),
                       "schema_version", table_schema.get_schema_version());
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, table_schema, allocated_table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "alloc_table_schema failed", KR(ret));
    } else if (OB_FAIL(table_schema_array.push_back(allocated_table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", KR(ret));

      // free table schema allocated
      allocator.free(allocated_table_schema);
      allocated_table_schema = NULL;
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve table schema", KR(ret), K(table_schema), K(is_deleted));
    }
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "retrieve table schema failed", KR(ret),
                       "table_id", table_schema.get_table_id(),
                       "schema_version", table_schema.get_schema_version(),
                       K(prev_table_id), K(is_deleted));
    }
    prev_table_id = table_schema.get_table_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all table schema. iter quit. ", KR(ret), K(tenant_id));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve table schema array", K(tenant_id));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  uint64_t last_table_id = OB_INVALID_ID;
  uint64_t last_schema_id = common::OB_INVALID_ID;
  // store current_schema and last_schema
  bool is_last_deleted = false;
  SCHEMA *last_schema = NULL;
  ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA> helper(table_schema_array);
  while (OB_SUCC(ret) && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    SCHEMA &current = helper.get_current();
    current.reset();
    if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, result, current, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill schema failed", K(ret));
    } else if (current.get_table_id() == last_table_id
               && helper.get_curr_schema_id() == last_schema_id) {
      //the same with last schema, continue;
      ret = common::OB_SUCCESS;
    } else {
      if (NULL == last_schema || is_last_deleted) {
        //LAST schema IS INVALID, IGNORE
      } else if (OB_FAIL(helper.add(*last_schema))) { //add last schema
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
    //save current column to last, rotate
    last_schema = &current;
    is_last_deleted = is_deleted;
    last_table_id = current.get_table_id();
    last_schema_id = helper.get_curr_schema_id();
    helper.rotate(); //rotate
  }
  if (OB_ITER_END != ret) {
    SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    //add last schema
    if (NULL != last_schema && !is_last_deleted) {
      if (OB_FAIL(helper.add(*last_schema))) {
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_column_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<ObTableSchema *> &table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObColumnSchemaV2>(
                                          tenant_id,
                                          check_deleted,
                                          result,
                                          table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve column schema failed", K(ret));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_constraint(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<ObTableSchema *> &table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObConstraint>(
                                          tenant_id,
                                          check_deleted,
                                          result,
                                          table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve constraint schema failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_part_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<TABLE_SCHEMA, ObPartition>(
                                          tenant_id,
                                          check_deleted,
                                          result,
                                          table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve part info failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_def_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = true;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
                                          tenant_id,
                                          check_deleted,
                                          is_subpart_template,
                                          result,
                                          table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info faield", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = false;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
                                          tenant_id,
                                          check_deleted,
                                          is_subpart_template,
                                          result,
                                          table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info faield", K(ret));
  }
  return ret;
}


template<typename T>
int ObSchemaRetrieveUtils::retrieve_table_schema(
    const uint64_t tenant_id,
    const bool check_deleted, T &result,
    ObIAllocator &allocator,
    ObTableSchema *&table_schema)
{
  int ret = common::OB_SUCCESS;

  table_schema = NULL;
  SHARE_SCHEMA_LOG(DEBUG, "retrieve table schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) { //no record
      ret = common::OB_ERR_SCHEMA_HISTORY_EMPTY;
      SHARE_SCHEMA_LOG(WARN, "schema history is empty", KR(ret));
    } else {
      SHARE_SCHEMA_LOG(WARN, "get table schema failed, iter quit", K(ret));
    }
  } else {
    bool is_deleted = false;
    ObTableSchema tmp_table_schema;
    if (OB_FAIL(fill_table_schema(tenant_id, check_deleted, result, tmp_table_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill table schema. ", K(check_deleted), K(ret));
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_table_schema, table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "alloc_table_schema failed", K(ret));
    } else {
      //check if this is only one
      if (OB_ITER_END != (ret = result.next())) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "should be one row only", K(ret));
      } else {
        ret = common::OB_SUCCESS;
        SHARE_SCHEMA_LOG(INFO, "retrieve table schema succeed", K(*table_schema), K(is_deleted));
      }
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_tablegroup_schema(
    const uint64_t tenant_id,
    T &result,
    ObIAllocator &allocator,
    ObTablegroupSchema *&tablegroup_schema)
{
  int ret = common::OB_SUCCESS;

  tablegroup_schema = NULL;
  SHARE_SCHEMA_LOG(DEBUG, "retrieve tablegroup schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) { //no record
      ret = common::OB_ERR_SCHEMA_HISTORY_EMPTY;
      SHARE_SCHEMA_LOG(WARN, "schema history is empty", KR(ret));
    } else {
      SHARE_SCHEMA_LOG(WARN, "get tablegroup schema failed, iter quit", K(ret));
    }
  } else {
    bool is_deleted = false;
    ObTablegroupSchema tmp_tablegroup_schema;
    if (OB_FAIL(fill_tablegroup_schema(tenant_id, result, tmp_tablegroup_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill tablegroup schema. ", K(ret));
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_tablegroup_schema, tablegroup_schema))) {
      SHARE_SCHEMA_LOG(WARN, "alloc_tablegroup_schema failed", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve tablegroup schema succeed", K(*tablegroup_schema), K(is_deleted));
    }
  }
  return ret;
}

template<typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::get_schema_id(const ObColumnSchemaV2 &p)
{
  return p.get_column_id();
}

template<typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::get_schema_id(const ObConstraint &p)
{
  return p.get_constraint_id();
}

template<typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::get_schema_id(const ObPartition &p)
{
  return p.get_part_id();
}

template<typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::add_schema(TABLE_SCHEMA &table_schema, ObColumnSchemaV2 &p)
{
  return ObSchemaUtils::add_column_to_table_schema(p, table_schema);
}

template<typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::add_schema(TABLE_SCHEMA &table_schema, ObConstraint &p)
{
  return table_schema.add_constraint(p);
}

template<typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::add_schema(TABLE_SCHEMA &table_schema, ObPartition &p)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && table_schema.is_interval_part()) {
    const ObRowkey &transition_point = table_schema.get_transition_point();
    const ObRowkey &high_bound_val = p.get_high_bound_val();
    if (high_bound_val > transition_point) {
      const ObRowkey &interval_range = table_schema.get_interval_range();
      bool is_oracle_mode = false;
      if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to check oracle mode", KR(ret), K(table_schema));
      } else if (OB_FAIL(ObPartitionUtils::set_low_bound_val_by_interval_range_by_innersql(
          is_oracle_mode, p, interval_range))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set_low_bound_val_by_interval_range", K(interval_range),
            K(is_oracle_mode), K(p), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(table_schema.add_partition(p))) {
    SHARE_SCHEMA_LOG(WARN, "fail to add_partition", K(table_schema), K(p));
  }
  return ret;
}

template<typename TABLE_SCHEMA>
template<typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::fill_current(const uint64_t tenant_id,
                                                                             const bool check_deleted,
                                                                             T &result, ObColumnSchemaV2 &p,
                                                                             bool &is_deleted)
{
  return ObSchemaRetrieveUtils::fill_column_schema(tenant_id, check_deleted,
                                                   result, p, is_deleted);
}

template<typename TABLE_SCHEMA>
template<typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::fill_current(const uint64_t tenant_id,
                                                                         const bool check_deleted,
                                                                         T &result, ObConstraint &p,
                                                                         bool &is_deleted)
{
  return ObSchemaRetrieveUtils::fill_constraint(tenant_id, check_deleted,
                                                result, p, is_deleted);
}

template<typename TABLE_SCHEMA>
template<typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::fill_current(const uint64_t tenant_id,
                                                                        const bool check_deleted,
                                                                        T &result, ObPartition &p,
                                                                        bool &is_deleted)
{
  return ObSchemaRetrieveUtils::fill_part_info(tenant_id, check_deleted,
                                               result, p, is_deleted);
}

template<typename TABLE_SCHEMA>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::get_table(
    const uint64_t table_id,
    TABLE_SCHEMA *&table)
{
  int ret = OB_SUCCESS;
  table = NULL;
  if (SINGLE_TABLE == mode_) {
    if (OB_ISNULL(table_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "table should not be null", K(ret), K(mode_));
    } else {
      table = table_;
    }
  } else if (MULTIPLE_TABLE == mode_) {
    if (OB_ISNULL(table_array_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "table array should not be null", K(ret), K(mode_));
    } else if (OB_ISNULL(tmp_table_) || table_id != tmp_table_->get_table_id()) {
      tmp_table_ = ObSchemaRetrieveUtils::find_table_schema(table_id, *table_array_);
    }
    table = tmp_table_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid mode", K(ret), K(mode_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_ISNULL(table) || table->get_table_id() != table_id)) {
    // may occur when upgrade system tables
    ret = common::OB_ENTRY_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "cannot find table", K(ret), K(table_id), KPC(table));
  }
  return ret;
}

template<typename TABLE_SCHEMA>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::add(ObSubPartition &p)
{
  int ret = OB_SUCCESS;
  TABLE_SCHEMA *table = NULL;
  if (OB_FAIL(get_table(p.get_table_id(), table))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get table failed", K(ret), K(mode_));
  } else if (!is_subpart_template_) {
    if (OB_ISNULL(partition_)
        || partition_->get_table_id() != p.get_table_id()
        || partition_->get_part_id() != p.get_part_id()) {
      // fill partition
      const ObPartition *tmp_partition = NULL;
      if (OB_FAIL(table->get_partition_by_part_id(
                  p.get_part_id(), CHECK_PARTITION_MODE_ALL, tmp_partition))) {
        SHARE_SCHEMA_LOG(WARN, "fail to find partition", K(ret), KPC(table), K(p));
      } else if (OB_ISNULL(tmp_partition)) {
        ret = OB_PARTITION_NOT_EXIST;
        SHARE_SCHEMA_LOG(WARN, "partition not exist", KR(ret), K(p));
      } else {
        partition_ = const_cast<ObPartition *>(tmp_partition);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(partition_)
               || partition_->get_table_id() != p.get_table_id()
               || partition_->get_part_id() != p.get_part_id()) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition not match", K(ret), K(p), KPC_(partition));
    } else if (OB_FAIL(partition_->add_partition(p))) {
      SHARE_SCHEMA_LOG(WARN, "add schema failed", K(ret));
    }
  } else {
    if (OB_FAIL(table->add_def_subpartition(p))) {
      SHARE_SCHEMA_LOG(WARN, "add def subpart schema failed", K(ret), K(p));
    }
  }
  return ret;
}

template<typename TABLE_SCHEMA>
int64_t ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::get_curr_schema_id()
{
  return schemas_[index_].get_sub_part_id();
}

template<typename TABLE_SCHEMA>
template<typename T>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::fill_current(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObSubPartition &p,
    bool &is_deleted)
{
  int ret = OB_SUCCESS;
  if (is_subpart_template_) {
    if (OB_FAIL(ObSchemaRetrieveUtils::fill_def_subpart_info(
                tenant_id, check_deleted, result, p, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to retrieve def sub part info", K(ret));
    }
  } else {
    if (OB_FAIL(ObSchemaRetrieveUtils::fill_subpart_info(
                tenant_id, check_deleted, result, p, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to retrieve sub part info", K(ret));
    }
  }
  return ret;
}
template<typename TABLE_SCHEMA, typename SCHEMA>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::get_table(
    const uint64_t table_id,
    TABLE_SCHEMA *&table)
{
  int ret = OB_SUCCESS;
  table = NULL;
  if (SINGLE_TABLE == mode_) {
    if (OB_ISNULL(table_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "table should not be null", K(ret), K(mode_));
    } else {
      table = table_;
    }
  } else if (MULTIPLE_TABLE == mode_) {
    if (OB_ISNULL(table_array_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "table array should not be null", K(ret), K(mode_));
    } else if (OB_ISNULL(tmp_table_) || table_id != tmp_table_->get_table_id()) {
      tmp_table_ = ObSchemaRetrieveUtils::find_table_schema(table_id, *table_array_);
    }
    table = tmp_table_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid mode", K(ret), K(mode_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_ISNULL(table) || table->get_table_id() != table_id)) {
    // may occur when upgrade system tables
    ret = common::OB_ENTRY_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "cannot find table", K(ret), K(table_id), KPC(table));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename SCHEMA>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::add(SCHEMA &p)
{
  int ret = OB_SUCCESS;
  TABLE_SCHEMA *table = NULL;
  if (OB_FAIL(get_table(p.get_table_id(), table))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get table failed", K(ret), K(mode_));
  } else if (OB_FAIL((ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::add_schema(
                      *table, p)))) {
    SHARE_SCHEMA_LOG(WARN, "add schema failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename SCHEMA>
int64_t ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::get_curr_schema_id()
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::get_schema_id(schemas_[index_]);
}

template<typename TABLE_SCHEMA, typename SCHEMA>
template<typename T>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::fill_current(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result, SCHEMA &p,
    bool &is_deleted)
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::fill_current(tenant_id, check_deleted, result, p, is_deleted);
}

template<typename TABLE_SCHEMA, typename SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_schema(const uint64_t tenant_id,
                                           const bool check_deleted,
                                           T &result,
                                           TABLE_SCHEMA *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "table schema is NULL", K(ret), K(table_schema));
  } else {
    const uint64_t table_id = table_schema->get_table_id();
    uint64_t last_schema_id = common::OB_INVALID_ID;
    // store current_schema and last_schema
    bool is_last_deleted = false;
    SCHEMA *last_schema = NULL;
    ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA> helper(*table_schema);
    while (OB_SUCC(ret) && common::OB_SUCCESS == (ret = result.next())) {
      bool is_deleted = false;
      SCHEMA &current = helper.get_current();
      current.reset();
      if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, result, current, is_deleted))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
      } else if (table_id != current.get_table_id()) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "table_id is not equal", K(ret), K(table_id), K(current));
      } else if (helper.get_curr_schema_id() == last_schema_id) {
        //the same with last schema, continue;
        ret = common::OB_SUCCESS;
      } else {
        if (NULL == last_schema || is_last_deleted) {
          //LAST schema IS INVALID, IGNORE
        } else if (OB_FAIL(helper.add(*last_schema))) { //add last schema
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
      //save current column to last, rotate
      last_schema = &current;
      is_last_deleted = is_deleted;
      last_schema_id = helper.get_curr_schema_id();
      helper.rotate(); //rotate
    }
    if (OB_ITER_END != ret) {
      SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      //add last partition
      if (NULL != last_schema && !is_last_deleted) {
        if (OB_FAIL(helper.add(*last_schema))) {
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
    }
    // Generate columns' dependency after all column schemas are loaded,
    // so that generated column can be defined in any order.
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cascaded_generated_column(*table_schema))) {
        SHARE_SCHEMA_LOG(WARN, "cascaded_generated_column failed", K(ret), KPC(table_schema));
      }
    }
  }
  return ret;
}

template<typename TABLE_SCHEMA>
int ObSchemaRetrieveUtils::cascaded_generated_column(TABLE_SCHEMA &table_schema)
{
  int ret = OB_SUCCESS;
  UNUSED(table_schema);
  return ret;
}

template<>
inline int ObSchemaRetrieveUtils::cascaded_generated_column<ObTableSchema>(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
    ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(column)) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "column schema is null", K(ret), K(table_schema));
    } else if (OB_FAIL(ObSchemaUtils::cascaded_generated_column(table_schema, *column, true))) {
      SHARE_SCHEMA_LOG(WARN, "cascaded_generated_column failed",
                        K(ret), K(table_schema), K(column));
    }
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_part_info(const uint64_t tenant_id,
                                              const bool check_deleted,
                                              T &result,
                                              TABLE_SCHEMA *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<TABLE_SCHEMA, ObPartition, T>(tenant_id,
                                                             check_deleted,
                                                             result,
                                                             table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve part info failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_def_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    TABLE_SCHEMA *&table_schema)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = true;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(tenant_id,
                                                        check_deleted,
                                                        is_subpart_template,
                                                        result,
                                                        table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    TABLE_SCHEMA *&table_schema)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = false;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(tenant_id,
                                                        check_deleted,
                                                        is_subpart_template,
                                                        result,
                                                        table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info failed", K(ret));
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    const bool is_subpart_template,
    T &result,
    ObArray<TABLE_SCHEMA *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_last_deleted = false;
  ObSubPartition *last_schema = NULL;
  ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA> helper(table_schema_array,
                                                     is_subpart_template);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    bool is_deleted = false;
    ObSubPartition &current = helper.get_current();
    current.reset();
    if (OB_FAIL(helper.fill_current(tenant_id, check_deleted,
                                    result, current, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
    } else if (OB_ISNULL(last_schema)
               || current.key_match(*last_schema)) {
      // continue
    } else if (!is_last_deleted && OB_FAIL(helper.add(*last_schema))) { //add last schema
      SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
    }
    last_schema = &current;
    is_last_deleted = is_deleted;
    helper.rotate(); //rotate
  }
  if (OB_ITER_END != ret) {
    SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    //add last partition
    if (OB_NOT_NULL(last_schema) && !is_last_deleted) {
      if (OB_FAIL(helper.add(*last_schema))) {
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    const bool is_subpart_template,
    T &result,
    TABLE_SCHEMA *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "table schema is NULL", K(ret), K(table_schema));
  } else {
    bool is_last_deleted = false;
    ObSubPartition *last_schema = NULL;
    ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA> helper(*table_schema,
                                                       is_subpart_template);
    while (OB_SUCC(ret) && OB_SUCC(result.next())) {
      bool is_deleted = false;
      ObSubPartition &current = helper.get_current();
      current.reset();
      if (OB_FAIL(helper.fill_current(tenant_id, check_deleted,
                                      result, current, is_deleted))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
      } else if (OB_ISNULL(last_schema)
                 || current.key_match(*last_schema)) {
        // continue
      } else if (!is_last_deleted && OB_FAIL(helper.add(*last_schema))) { //add last schema
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
      last_schema = &current;
      is_last_deleted = is_deleted;
      helper.rotate(); //rotate
    }
    if (OB_ITER_END != ret) {
      SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      //add last partition
      if (OB_NOT_NULL(last_schema) && !is_last_deleted) {
        if (OB_FAIL(helper.add(*last_schema))) {
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_column_schema(const uint64_t tenant_id,
                                                  const bool check_deleted,
                                                  T &result,
                                                  ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObColumnSchemaV2>(tenant_id,
                                                                check_deleted,
                                                                result,
                                                                table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve column schema failed", K(ret));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_constraint(const uint64_t tenant_id,
                                               const bool check_deleted,
                                               T &result,
                                               ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObConstraint>(tenant_id,
                                                            check_deleted,
                                                            result,
                                                            table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve constraint schema failed", K(ret));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_constraint_column_info(const uint64_t tenant_id,
                                                           T &result,
                                                           ObConstraint *&cst)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_column_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  common::ObSEArray<uint64_t, common::SEARRAY_INIT_NUM> column_ids;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_constraint_column_info(tenant_id, result, column_id, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill constraint column info", K(ret));
    } else if (prev_column_id == column_id) {
      // skip
    } else if (is_deleted) {
      // skip
    } else if (OB_FAIL(column_ids.push_back(column_id))) {
      SHARE_SCHEMA_LOG(WARN, "push back to column_ids failed", K(ret), K(column_id));
    }
    prev_column_id = column_id;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get constraint column info. iter quit.", K(ret));
  } else {
    SHARE_SCHEMA_LOG(DEBUG, "retrieve constraint column info");
    ret = common::OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cst->assign_column_ids(column_ids))) {
      SHARE_SCHEMA_LOG(WARN, "fail to assign_column_ids", K(column_ids));
    }
  }
  return ret;
}

// when tenant_id is sys, recycle_objs's tenant_id is invalid, can not use it
template<typename T>
int ObSchemaRetrieveUtils::retrieve_recycle_object(
    const uint64_t tenant_id,
    T &result,
    ObIArray<ObRecycleObject> &recycle_objs)
{
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObRecycleObject recycle_obj(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    recycle_obj.reset();
    allocator.reuse();
    if (OB_FAIL(fill_recycle_object(tenant_id, result, recycle_obj))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill recycle object. ", K(ret));
    } else if (OB_FAIL(recycle_objs.push_back(recycle_obj))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all recycle objects. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}


/*
 * fill functions for full schemas
 * NOTICE: When we retrieve column, use should marco like EXTRACT_**_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE.
 * Here, "**" means column type, such as BOOL, INT, UINT, VARCHAR.
 */

template<typename T>
int ObSchemaRetrieveUtils::fill_tenant_schema(
    T &result,
    ObTenantSchema &tenant_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  tenant_schema.reset();
  is_deleted = false;
  bool skip_null_error = false;
  ObString info;
  ObString locality;
  ObString locality_default_value("");
  ObString previous_locality;
  ObString previous_locality_default_value("");
  int64_t default_tablegroup_id_default_value = OB_INVALID_INDEX;
  ObCompatibilityMode default_compat_mode = ObCompatibilityMode::MYSQL_MODE;
  int64_t default_drop_tenant_time = OB_INVALID_VERSION;
  int64_t default_in_recyclebin = 0;
  ObString tenant_status_str("");
  ObString default_tenant_status_str("NORMAL");
  ObString arbitration_service_status_str("");
  ObString default_arbitration_service_status_str("DISABLED");
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, tenant_id, tenant_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tenant_name, tenant_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "info", info);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "locality", locality, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, locality_default_value);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "previous_locality", previous_locality, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, previous_locality_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, locked, tenant_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tenant_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, tenant_schema, common::ObCollationType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, default_tablegroup_id,
        tenant_schema, uint64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_,
        default_tablegroup_id_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, compatibility_mode,
        tenant_schema, common::ObCompatibilityMode, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_,
        default_compat_mode);
    tenant_schema.set_charset_type(ObCharset::charset_type_by_coll(tenant_schema.get_collation_type()));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, drop_tenant_time,
        tenant_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_drop_tenant_time);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "status", tenant_status_str,
        skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_tenant_status_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, in_recyclebin,
        tenant_schema, int64_t, skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_, default_in_recyclebin);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "arbitration_service_status", arbitration_service_status_str,
        skip_null_error, true/*ignore_column_retrieve_error*/, default_arbitration_service_status_str);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_comment(info))) {
        SHARE_SCHEMA_LOG(WARN, "set_comment failed", K(ret));
      } else if (OB_FAIL(tenant_schema.set_locality(locality))) {
        SHARE_SCHEMA_LOG(WARN, "set locality failed", K(ret));
      } else if (OB_FAIL(fill_replica_options(result, tenant_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fail to fill replica options", K(ret));
      } else if (OB_FAIL(fill_schema_zone_region_replica_num_array(tenant_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema zone replica dist failed", K(ret));
      } else if (OB_FAIL(tenant_schema.set_previous_locality(previous_locality))) {
        SHARE_SCHEMA_LOG(WARN, "set previous locality failed", K(ret));
      } else {} // good
    }
    if (OB_SUCC(ret)) {
      ObTenantStatus status = TENANT_STATUS_MAX;
      if (OB_FAIL(get_tenant_status(tenant_status_str, status))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get tenant status", K(ret), K(tenant_status_str));
      } else {
        tenant_schema.set_status(status);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_arbitration_service_status_from_string(arbitration_service_status_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set arb status from string", K(ret), K(arbitration_service_status_str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(TRACE, "retrieve tenant schema", K(tenant_schema), K(is_deleted), KR(ret));
  } else {
    SHARE_SCHEMA_LOG(WARN, "retrieve tenant schema failed",
                    "tenant_id", tenant_schema.get_tenant_id(),
                    "schema_version", tenant_schema.get_schema_version(),
                    K(is_deleted), KR(ret));
  }
  return ret;
}

template<typename SCHEMA>
int ObSchemaRetrieveUtils::fill_schema_zone_region_replica_num_array(
    SCHEMA &schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  rootserver::ObLocalityDistribution locality_dist;
  ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
  if (OB_FAIL(locality_dist.init())) {
    SHARE_SCHEMA_LOG(WARN, "fail to init locality dist", K(ret));
  } else if (OB_FAIL(schema.get_zone_list(zone_list))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get zone list", K(ret));
  } else if (OB_FAIL(locality_dist.parse_locality(
          schema.get_locality_str(), zone_list))) {
    SHARE_SCHEMA_LOG(WARN, "fail to parse locality", K(ret));
  } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_attr_array))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get zone region replica num array", K(ret));
  } else if (OB_FAIL(schema.set_zone_replica_attr_array(zone_replica_attr_array))) {
    SHARE_SCHEMA_LOG(WARN, "fail to set zone replica num array", K(ret));
  } else if (schema.get_locality_str().length() > 0) {
    // no need to fill locality
  } else {
    // For compatibility, fill format locality string if schema's locality is an empty string.
    char locality_str[MAX_LOCALITY_LENGTH + 1];
    int64_t pos = 0;
    if (OB_FAIL(locality_dist.output_normalized_locality(
            locality_str, MAX_LOCALITY_LENGTH, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to normalized locality", K(ret));
    } else if (OB_FAIL(schema.set_locality(locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set normalized locality back to schema", K(ret));
    } else {} // no more to do
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_temp_table_schema(const uint64_t tenant_id, T &result, ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  UNUSED(tenant_id);
  ObString create_host;
  ObString default_create_host("");
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "create_host", create_host,
                      true, ObSchemaService::g_ignore_column_retrieve_error_, default_create_host);
  if (0 >= create_host.length()) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get unexpected create_host. ", K(ret), K(create_host));
  } else {
    table_schema.set_create_host(create_host);
  }
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(INFO, "Get create_host ", K(create_host), K(table_schema));
  } else {
    SHARE_SCHEMA_LOG(WARN, "Get create_host failed", KR(ret),
                    "table_id", table_schema.get_table_id(),
                    "schema_version", table_schema.get_schema_version());
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_table_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObTableSchema &table_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  table_schema.reset();
  is_deleted = false;
  ObString expire_info;
  ObString expire_expr_hex;
  ObString part_func_expr;
  ObString sub_part_func_expr;
  table_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, table_schema, tenant_id);
  if (check_deleted) {
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  }
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, tablespace_id,
        table_schema, tenant_id, true, ObSchemaService::g_ignore_column_retrieve_error_, common::OB_INVALID_ID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_type, table_schema, ObTableType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_type, table_schema, ObIndexType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_using_type, table_schema, ObIndexUsingType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, load_type, table_schema, ObTableLoadType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, def_type, table_schema, ObTableDefType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_column_num, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_column_num, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, autoinc_column_id, table_schema, uint64_t);
    uint64_t auto_increment_default = 1;
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, auto_increment, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, auto_increment_default);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, read_only, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_split_pos, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, max_used_column_id, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_level, table_schema, ObPartitionLevel);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, block_size, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, progressive_merge_num, table_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_schema);

    if (OB_SUCCESS == ret && table_schema.get_block_size() <= 0) {
      SHARE_SCHEMA_LOG(WARN,  "set tablet sstable block size to default value:",
          "read", table_schema.get_block_size(), "table_id", table_schema.get_table_id());
      table_schema.set_block_size(OB_DEFAULT_SSTABLE_BLOCK_SIZE);
    }
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, compress_func_name, table_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "expire_condition", expire_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, is_use_bloomfilter, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, table_schema, common::ObCollationType);
    table_schema.set_charset_type(ObCharset::charset_type_by_coll(table_schema.get_collation_type()));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, data_table_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_status, table_schema, ObIndexStatus);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_status, table_schema, ObPartitionStatus, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_schema_version, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, comment, table_schema);
    ObString parser_name_default_val;
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, parser_name, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, parser_name_default_val);
    int64_t tablet_size_default = OB_DEFAULT_TABLET_SIZE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, tablet_size, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, tablet_size_default);
    int64_t pctfree_default = OB_DEFAULT_PCTFREE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, pctfree, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, pctfree_default);
    //view schema
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, view_definition, table_schema.get_view_schema());
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, view_check_option, table_schema.get_view_schema(), ViewCheckOption);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, view_is_updatable, table_schema.get_view_schema(), bool);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, index_attributes_set, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, session_id, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    ObString pk_comment("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, pk_comment, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, pk_comment);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sess_active_time, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    /*
     * Here comes a compatibility problem:
     * row_store_type's default value is defined as flat_row_store when cluster is upgraded from ver 1.4.x
     * and is defined as encoding_row_store when cluster is upgraded from ver 2.x.
     * (
     *
     * Because we don't actually record row_store_type by DML when cluster is in upgradation,
     * row_store_type's value is different according to different hardcoded row_store_type's default value
     * if row_store_type's value is null.
     *
     * After schema split in ver 2.2.x(or __all_table's column modification in ver 2.2.60),
     * column's row_store_type's value may be changed because hardcoded row_store_type's default value has been changed.
     * If tables are created in ver 1.4.x, it means we modify such tables' row_store_type to encoding_row_store
     * after we upgrade cluster to ver 2.2.x, which is contrary to the user's initial expectation.
     */
    ObString row_store_type("encoding_row_store");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, row_store_type, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, row_store_type);
    ObString store_format("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, store_format, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, store_format);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, progressive_merge_round, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, storage_format_version, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, table_mode, table_schema, int32_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.set_expire_info(expire_info))) {
        SHARE_SCHEMA_LOG(WARN, "set expire info failed", K(ret));
      }
    }

    //part_expr
    int64_t part_num = 0;
    ObPartitionOption &partition_option = table_schema.get_part_option();
    enum ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_HASH;
    EXTRACT_INT_FIELD_MYSQL(result, "part_func_type", part_func_type, ObPartitionFuncType);
    partition_option.set_part_func_type(part_func_type);
    EXTRACT_INT_FIELD_MYSQL(result, "part_num", part_num, int64_t);
    partition_option.set_part_num(part_num);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_func_expr", part_func_expr);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_option.set_part_expr(part_func_expr))) {
        SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
      }
    }
    //sub_part_expr
    ObPartitionOption &sub_part_option = table_schema.get_sub_part_option();
    EXTRACT_INT_FIELD_MYSQL(result, "sub_part_func_type", part_func_type, ObPartitionFuncType);
    sub_part_option.set_part_func_type(part_func_type);
    EXTRACT_INT_FIELD_MYSQL(result, "sub_part_num", part_num, int64_t);
    sub_part_option.set_part_num(part_num);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_func_expr", sub_part_func_expr);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sub_part_option.set_part_expr(sub_part_func_expr))) {
        SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
      }
    }

    const ObDuplicateScope duplicate_scope_default = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, duplicate_scope, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, duplicate_scope_default);
    //encrypt
    ObString encryption_default("");
    ObString encryption;
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        result, "encryption", encryption, true,
        ObSchemaService::g_ignore_column_retrieve_error_, encryption_default);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.set_encryption_str(encryption))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set encryption str", K(ret), K(encryption));
      }
    }
    bool ignore_column_error = false;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, sub_part_template_flags, table_schema, int64_t, true /* skip null error*/,
      ignore_column_error, true);
    if (OB_SUCC(ret) && !table_schema.has_sub_part_template_def()) {
      table_schema.get_sub_part_option().set_part_num(0);
      table_schema.set_def_sub_part_num(0);
    }

    int64_t default_table_dop = OB_DEFAULT_TABLE_DOP;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, dop, table_schema, int64_t, true, ignore_column_error, default_table_dop);

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
                result, character_set_client,
                table_schema.get_view_schema(),
                ObCharsetType, true /* skip null error*/,
                ignore_column_error,
                CHARSET_INVALID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
                result, collation_connection,
                table_schema.get_view_schema(),
                ObCollationType, true /* skip null error*/,
                ignore_column_error,
                CS_TYPE_INVALID);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, auto_part, partition_option, true, ignore_column_error, false);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, auto_part_size, partition_option, int64_t, true, ignore_column_error, -1);

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, association_table_id,
    table_schema, tenant_id, true, ignore_column_error, common::OB_INVALID_ID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, define_user_id,
    table_schema, tenant_id, true, ignore_column_error, common::OB_INVALID_ID);

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, max_dependency_version,
    table_schema, int64_t, true, ignore_column_error, common::OB_INVALID_VERSION);

    if (OB_SUCC(ret) && table_schema.is_interval_part()) {
      ObString btransition_point;
      ObString binterval_range;
      ObString tmp_str("");
      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        result, "b_transition_point", btransition_point, true, ignore_column_error, tmp_str);

      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        result, "b_interval_range", binterval_range, true, ignore_column_error, tmp_str);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_schema.set_transition_point_with_hex_str(btransition_point))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set transition point to partition", K(ret));
      } else if (OB_FAIL(table_schema.set_interval_range_with_hex_str(binterval_range))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set interval range to partition", K(ret));
      }
    }

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, tablet_id, table_schema, uint64_t,
        true, ignore_column_error, ObTabletID::INVALID_TABLET_ID);
    ignore_column_error = true;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, table_flags, table_schema, uint64_t,
        true, ignore_column_error, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, object_status, table_schema, int64_t, true, ignore_column_error, static_cast<int64_t> (ObObjectStatus::VALID));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, truncate_version, table_schema, int64_t, true, ignore_column_error, common::OB_INVALID_VERSION);

    ObString external_file_location;
    ObString external_file_location_access_info;
    ObString external_file_format;
    ObString external_file_pattern;
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, external_file_location, table_schema, true/*skip null*/, true/*ignore column error*/, external_file_location);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, external_file_location_access_info, table_schema, true/*skip null*/, true/*ignore column error*/, external_file_location_access_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, external_file_format, table_schema, true/*skip null*/, true/*ignore column error*/, external_file_format);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, external_file_pattern, table_schema, true/*skip null*/, true/*ignore column error*/, external_file_pattern);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, ttl_definition, table_schema, true, ignore_column_error, "");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, kv_attributes, table_schema, true, ignore_column_error, "");

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, name_generated_type, table_schema, ObNameGeneratedType, true/*skip null*/, true/*ignore column error*/, GENERATED_TYPE_UNKNOWN);
  }
  if (OB_SUCC(ret) && OB_FAIL(fill_sys_table_lob_tid(table_schema))) {
    SHARE_SCHEMA_LOG(WARN, "fail to fill lob table id for inner table", K(ret), K(table_schema.get_table_id()));
  }
  return ret;
}

template<typename T>
int fill_column_schema_default_value(T &result,
                                     ObColumnSchemaV2 &column,
                                     common::ColumnType default_type,
                                     const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  bool is_oracle_mode = false;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
          column.get_tenant_id(), column.get_table_id(), is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "failed to get oracle mode", K(ret));
  } else if (is_oracle_mode) {
    compat_mode = lib::Worker::CompatMode::ORACLE;
  }
  lib::CompatModeGuard guard(compat_mode);
  EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, orig_default_value, default_type,
                                    column,false, false, tenant_id);
  EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, cur_default_value, default_type,
                                    column, true, false, tenant_id);
  EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, orig_default_value_v2, default_type,
                                    column, false, true, tenant_id);
  EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, cur_default_value_v2, default_type,
                                    column, true, true, tenant_id);
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_column_schema(
    const uint64_t tenant_id, const bool check_deleted, T &result,
    ObColumnSchemaV2 &column, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  column.reset();
  is_deleted = false;

  column.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, column, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, column, uint64_t);
  if (check_deleted) {
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  }
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_position, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_position, column, int64_t);
    int64_t tbl_part_key_pos = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "partition_key_position", tbl_part_key_pos, int64_t);
    column.set_tbl_part_key_pos(tbl_part_key_pos);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_type, column, common::ColumnType);
    if (ob_is_accuracy_length_valid_tc(column.get_data_type())) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_length, column, int32_t);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_precision, column, int16_t);
    } else {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_precision, column, int16_t);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_scale, column, int16_t);
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, zero_fill, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, nullable, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, autoincrement, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, is_hidden, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, on_update_current_timestamp, column, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, column, common::ObCollationType);
    column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, order_in_rowkey, column, ObOrderType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, schema_version, column, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, column);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, comment, column);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_flags, column, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, prev_column_id, column, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, UINT64_MAX);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, srs_id, column, uint64_t, true, true, OB_DEFAULT_COLUMN_SRS_ID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, udt_set_id, column, int64_t, true, true, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sub_data_type, column, int64_t, true, true, 0);

    common::ColumnType default_type = column.get_data_type();
    if (column.is_generated_column() || column.is_identity_column()) {
      default_type = ObVarcharType;
    } else if (column.is_xmltype()) {
      default_type = ObLongTextType;
    }

    if (OB_SUCC(ret)) {
      ret = fill_column_schema_default_value<T>(result, column, default_type, tenant_id);
    }

    if (OB_SUCC(ret) && column.is_identity_column()) {
      // orig_default_value is used to store sequence_id when column is identity column.
      uint64_t sequence_id = OB_INVALID_ID;
      if (OB_FAIL(ObSchemaUtils::str_to_uint(column.get_orig_default_value().get_string(), sequence_id))) {
        SHARE_SCHEMA_LOG(WARN, "get sequence id fail", K(ret), K(column.get_cur_default_value().get_string()),
                                                       K(column.get_orig_default_value().get_string()));
      } else {
        column.set_sequence_id(sequence_id);
      }
    }

    if (OB_SUCC(ret) && column.is_enum_or_set()) {
      ObString extend_type_info;
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "extended_type_info", extend_type_info);
      int64_t pos = 0;
      if (extend_type_info.empty()) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "extend_type_info is empty", K(ret));
      } else if (OB_FAIL(column.deserialize_extended_type_info(extend_type_info.ptr(), extend_type_info.length(), pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize_extended_type_info", K(ret));
      } else {}
    }

  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_constraint(
    const uint64_t tenant_id, const bool check_deleted, T &result,
    ObConstraint &constraint, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  constraint.reset();
  is_deleted = false;

  constraint.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, constraint, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, constraint_id, constraint, uint64_t);
  if (check_deleted) {
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  }
  if (!is_deleted) {
    const bool default_rely_flag = false;
    const bool default_enable_flag = true;
    const ObCstFkValidateFlag default_validate_flag = CST_FK_VALIDATED;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, constraint_type, constraint, ObConstraintType, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, rely_flag, constraint, true, ObSchemaService::g_ignore_column_retrieve_error_, default_rely_flag);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, enable_flag, constraint, true, ObSchemaService::g_ignore_column_retrieve_error_, default_enable_flag);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, validate_flag, constraint, ObCstFkValidateFlag, true, ObSchemaService::g_ignore_column_retrieve_error_, default_validate_flag);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, schema_version, constraint, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, constraint_name, constraint);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, check_expr, constraint);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, name_generated_type, constraint, ObNameGeneratedType, true/*skip null*/, true/*ignore column error*/, GENERATED_TYPE_UNKNOWN);
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_constraint_column_info(
    const uint64_t tenant_id, T &result, uint64_t &column_id, bool &is_deleted)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  column_id = 0;
  is_deleted = false;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "column_id", column_id, uint64_t);
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_database_schema(
    const uint64_t tenant_id,
    T &result,
    ObDatabaseSchema &db_schema,
    bool &is_deleted)
{
  db_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  db_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, db_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, database_name, db_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, comment, db_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, db_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, db_schema, common::ObCollationType);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, read_only, db_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, default_tablegroup_id, db_schema, tenant_id);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, in_recyclebin, db_schema);
    db_schema.set_charset_type(ObCharset::charset_type_by_coll(db_schema.get_collation_type()));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_tablegroup_schema(
    const uint64_t tenant_id,
    T &result,
    ObTablegroupSchema &tg_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  tg_schema.reset();
  is_deleted  = false;
  ObString sharding_default(OB_PARTITION_SHARDING_ADAPTIVE);
  tg_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, tg_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tablegroup_name, tg_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comment, tg_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tg_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_status, tg_schema, ObPartitionStatus, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_schema_version, tg_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);

    const bool skip_null_error = true;
    const enum ObPartitionLevel default_part_level = PARTITION_LEVEL_ZERO;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, part_level, tg_schema, ObPartitionLevel, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_part_level);

    const int64_t default_part_num = 0;
    enum ObPartitionFuncType default_part_func_type = PARTITION_FUNC_TYPE_HASH;
    const int64_t default_func_expr_num = 0;
    int64_t def_sub_part_num = 0;

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, part_num, tg_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_part_num);
    ObPartitionOption &partition_option = tg_schema.get_part_option();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, part_func_type, partition_option, ObPartitionFuncType, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_part_func_type);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, part_func_expr_num, tg_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_func_expr_num);

    ObPartitionOption &sub_part_option = tg_schema.get_sub_part_option();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sub_part_func_type, sub_part_option, ObPartitionFuncType, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_part_func_type);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sub_part_func_expr_num, tg_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_func_expr_num);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "sub_part_num", def_sub_part_num, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_part_num);
    tg_schema.set_def_sub_part_num(def_sub_part_num);
    sub_part_option.set_part_num(def_sub_part_num);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, sub_part_template_flags, tg_schema, int64_t, true /* skip null error*/,
      ObSchemaService::g_ignore_column_retrieve_error_, true);
    if (OB_SUCC(ret) && !tg_schema.has_sub_part_template_def()) {
      tg_schema.get_sub_part_option().set_part_num(0);
      tg_schema.set_def_sub_part_num(0);
    }
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sharding, tg_schema, true, true, sharding_default);
  }
  SQL_LOG(DEBUG, "fill tablegroup schema", K(ret), K(tg_schema));
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_user_schema(
    const uint64_t tenant_id,
    T &result,
    ObUserInfo &user_info,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  user_info.reset();
  is_deleted = false;
  user_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, user_id, user_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, user_name, user_info);
    ObString default_host_value(OB_DEFAULT_HOST_NAME);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, host, user_info, false, ObSchemaService::g_ignore_column_retrieve_error_, default_host_value);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, passwd, user_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, info, user_info);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_alter, user_info, PRIV_ALTER);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create, user_info, PRIV_CREATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create_user, user_info, PRIV_CREATE_USER);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_delete, user_info, PRIV_DELETE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_drop, user_info, PRIV_DROP);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_grant_option, user_info, PRIV_GRANT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_insert, user_info, PRIV_INSERT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_update, user_info, PRIV_UPDATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_select, user_info, PRIV_SELECT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_index, user_info, PRIV_INDEX);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create_view, user_info, PRIV_CREATE_VIEW);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_show_view, user_info, PRIV_SHOW_VIEW);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_show_db, user_info, PRIV_SHOW_DB);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_super, user_info, PRIV_SUPER);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_process, user_info, PRIV_PROCESS);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_file, user_info, PRIV_FILE, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_alter_tenant, user_info, PRIV_ALTER_TENANT, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_alter_system, user_info, PRIV_ALTER_SYSTEM, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_create_resource_pool, user_info, PRIV_CREATE_RESOURCE_POOL, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_create_resource_unit, user_info, PRIV_CREATE_RESOURCE_UNIT, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_repl_slave, user_info, PRIV_REPL_SLAVE, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, priv_repl_client, user_info, PRIV_REPL_CLIENT, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, is_locked, user_info, bool);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, user_info, int64_t);

    const ObSSLType default_ssl_type = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
                                        ssl_type,
                                        user_info,
                                        ObSSLType,
                                        false,
                                        ObSchemaService::g_ignore_column_retrieve_error_,
                                        default_ssl_type);
    ObString default_ssl_specified("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, ssl_cipher, user_info, false, ObSchemaService::g_ignore_column_retrieve_error_, default_ssl_specified);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, x509_issuer, user_info, false, ObSchemaService::g_ignore_column_retrieve_error_, default_ssl_specified);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, x509_subject, user_info, false, ObSchemaService::g_ignore_column_retrieve_error_, default_ssl_specified);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        type, user_info, int32_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, profile_id,
        user_info, tenant_id, false, ObSchemaService::g_ignore_column_retrieve_error_, common::OB_INVALID_ID);
    int64_t default_last_changed = -1;
    int64_t password_last_changed = -1;
    common::ObTimeZoneInfoWrap tz_info_wrap;
    ret = GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(
          result.get_timestamp, "password_last_changed", password_last_changed,
          default_last_changed, tz_info_wrap.get_time_zone_info());
    user_info.set_password_last_changed(password_last_changed);

    if (OB_SUCC(ret)) {
       int64_t int_value = 0;
       if (OB_SUCCESS == (ret = result.get_int("priv_create_synonym", int_value))) {
       } else if (OB_ERR_NULL_VALUE == ret) {
         ret = OB_SUCCESS;
       } else if (OB_ERR_COLUMN_NOT_FOUND == ret
         && ObSchemaService::g_ignore_column_retrieve_error_) {
         int_value = 1;//default value
         ret = OB_SUCCESS;
       }
       if (OB_SUCC(ret)) {
         if (int_value) {
            user_info.set_priv(OB_PRIV_CREATE_SYNONYM);
         }
         SQL_LOG(DEBUG, "succ to fill user schema", K(user_info));
       } else {
         SQL_LOG(WARN, "fail to retrieve priv_create_synonym", K(ret));
       }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, max_connections, user_info, uint64_t, true,
        ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, max_user_connections, user_info, uint64_t, true,
        ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE_NULL_AND_IGNORE_COLUMN_ERROR(result, priv_drop_database_link, user_info, PRIV_DROP_DATABASE_LINK);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE_NULL_AND_IGNORE_COLUMN_ERROR(result, priv_create_database_link, user_info, PRIV_CREATE_DATABASE_LINK);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_role_grantee_map_schema(
    const uint64_t tenant_id,
    T &result,
    const bool is_fetch_role,
    ObArray<ObUserInfo> &user_array)
{
  int ret = common::OB_SUCCESS;
  // <key_id, value_id>:
  // if is_fetch_role
  //   true : <grantee_id, role_id>
  //   false: <role_id, grantee_id>
  uint64_t prev_key_id = common::OB_INVALID_ID;
  uint64_t prev_value_id = common::OB_INVALID_ID;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    uint64_t grantee_id = common::OB_INVALID_ID;
    uint64_t role_id = common::OB_INVALID_ID;
    bool is_deleted = false;
    uint64_t admin_option = 0;
    uint64_t disable_flag = 0;
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "grantee_id", grantee_id, tenant_id);
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "role_id", role_id, tenant_id);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
       result, "admin_option", admin_option, uint64_t, true /* skip null error*/,
       ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
       result, "disable_flag", disable_flag, uint64_t, true /* skip null error*/,
       ObSchemaService::g_ignore_column_retrieve_error_, 0);

    ObUserInfo *user_info = NULL;
    if (OB_FAIL(ret)) {
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "grantee / role is deleted", K(grantee_id), K(role_id));
    } else if (prev_key_id == (is_fetch_role ? grantee_id : role_id)) {
      if (prev_value_id == (is_fetch_role ? role_id : grantee_id)) {
        ret = common::OB_SUCCESS;
      } else if (OB_FAIL(ObSchemaRetrieveUtils::find_user_info(is_fetch_role ? grantee_id : role_id,
            user_array, user_info))) {
        SHARE_SCHEMA_LOG(WARN, "failed to find user info", K(ret), K(grantee_id), K(role_id));
      } else if (NULL == user_info) {
        // skip
        // e.g: user_array may only contain role.
        //      When we find user info by grantee_id, it's valid that user_info is null.
        continue;
        SHARE_SCHEMA_LOG(INFO, "user info is null", K(ret), K(is_fetch_role), K(grantee_id), K(role_id));
      } else if (is_fetch_role && (OB_FAIL(user_info->add_role_id(role_id,
                                                                  admin_option,
                                                                  disable_flag)))) {
        SHARE_SCHEMA_LOG(WARN, "failed to add role id", K(ret), K(role_id));
      } else if (!is_fetch_role && (OB_FAIL(user_info->add_grantee_id(grantee_id)))) {
        SHARE_SCHEMA_LOG(WARN, "failed to add grantee_id id", K(ret), K(grantee_id));
      }
      // reset prev_value_id only
      prev_value_id = (is_fetch_role ? role_id : grantee_id);
    } else {
      if (OB_FAIL(ObSchemaRetrieveUtils::find_user_info(is_fetch_role ? grantee_id : role_id, user_array, user_info))) {
        SHARE_SCHEMA_LOG(WARN, "failed to find user info", K(ret), K(grantee_id), K(role_id));
      } else if (NULL == user_info) {
        // skip
        // e.g: user_array may only contain role.
        //      When we find user info by grantee_id, it's valid that user_info is null.
        continue;
        SHARE_SCHEMA_LOG(INFO, "user info is null", K(ret), K(is_fetch_role), K(grantee_id), K(role_id));
      } else if (is_fetch_role && OB_FAIL(user_info->add_role_id(role_id,
                                                                 admin_option,
                                                                 disable_flag))) {
        SHARE_SCHEMA_LOG(WARN, "failed to add role id", K(ret), K(grantee_id), K(role_id));
      } else if (!is_fetch_role && OB_FAIL(user_info->add_grantee_id(grantee_id))) {
        SHARE_SCHEMA_LOG(WARN, "failed to add role id", K(ret), K(grantee_id), K(role_id));
      }
    }
    // iterate next <key_id, value_id>
    prev_key_id = is_fetch_role ? grantee_id : role_id;
    prev_value_id = is_fetch_role ? role_id : grantee_id;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get role grantee map. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

int ObSchemaRetrieveUtils::find_user_info(
    const uint64_t user_id,
    ObArray<ObUserInfo> &user_array,
    ObUserInfo *&user_info)
{
  int ret = OB_SUCCESS;
  typename ObArray<ObUserInfo>::iterator iter = user_array.end();
  iter = std::lower_bound(user_array.begin(),
      user_array.end(),
      user_id,
      compare_user_id<ObUserInfo>);
  if (iter != user_array.end()) {
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "fail to get user info. ", K(ret));
    } else if ((iter)->get_user_id() == user_id) {
      user_info = iter;
    }
  }
  return ret;
}

template<typename T>
bool ObSchemaRetrieveUtils::compare_user_id(
    const T &user_info,
    const uint64_t user_id)
{
  bool cmp = false;
  cmp = user_info.get_user_id() > user_id;
  return cmp;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_db_priv_schema(
    const uint64_t tenant_id, T &result, ObDBPriv &db_priv, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  db_priv.reset();
  is_deleted = false;

  db_priv.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, user_id, db_priv, tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, database_name, db_priv);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_alter, db_priv, PRIV_ALTER);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create, db_priv, PRIV_CREATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_delete, db_priv, PRIV_DELETE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_drop, db_priv, PRIV_DROP);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_grant_option, db_priv, PRIV_GRANT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_insert, db_priv, PRIV_INSERT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_update, db_priv, PRIV_UPDATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_select, db_priv, PRIV_SELECT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_index, db_priv, PRIV_INDEX);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create_view, db_priv, PRIV_CREATE_VIEW);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_show_view, db_priv, PRIV_SHOW_VIEW);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, db_priv, int64_t);
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_sys_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObSysPriv &sys_priv,
    bool &is_deleted,
    ObRawPriv &raw_p_id,
    uint64_t &option)
{
  int ret = common::OB_SUCCESS;
  sys_priv.reset();

  sys_priv.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, grantee_id, sys_priv, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "priv_id", raw_p_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "priv_option", option, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, sys_priv, int64_t);

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_table_priv_schema(
    const uint64_t tenant_id, T &result, ObTablePriv &table_priv, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  table_priv.reset();
  is_deleted = false;

  table_priv.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, user_id, table_priv, tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, database_name, table_priv);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_priv);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_alter, table_priv, PRIV_ALTER);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create, table_priv, PRIV_CREATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_delete, table_priv, PRIV_DELETE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_drop, table_priv, PRIV_DROP);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_grant_option, table_priv, PRIV_GRANT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_insert, table_priv, PRIV_INSERT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_update, table_priv, PRIV_UPDATE);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_select, table_priv, PRIV_SELECT);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_index, table_priv, PRIV_INDEX);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_create_view, table_priv, PRIV_CREATE_VIEW);
    EXTRACT_PRIV_FROM_MYSQL_RESULT(result, priv_show_view, table_priv, PRIV_SHOW_VIEW);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, table_priv, int64_t);
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_obj_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObObjPriv &obj_priv,
    bool &is_deleted,
    ObRawObjPriv &raw_p_id,
    uint64_t &option)
{
  int ret = common::OB_SUCCESS;
  obj_priv.reset();
  is_deleted = false;

  obj_priv.set_tenant_id(tenant_id);

  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, obj_id, obj_priv, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, objtype, obj_priv, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, col_id, obj_priv, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, grantor_id, obj_priv, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, grantee_id, obj_priv, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "priv_id", raw_p_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "priv_option", option, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, obj_priv, int64_t);

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_outline_schema(
    const uint64_t tenant_id,
    T &result,
    ObOutlineInfo &outline_info,
    bool &is_deleted)
{
  outline_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  outline_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, outline_id, outline_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, outline_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, signature, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, sql_id, outline_info, true, ObSchemaService::g_ignore_column_retrieve_error_, ObString::make_string(""));
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, outline_content, outline_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, outline_info, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, sql_text, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, owner, outline_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, used, outline_info, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, version, outline_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, compatible, outline_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, enabled, outline_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, format, outline_info, ObHintFormat);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, outline_params, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, outline_target, outline_info);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_synonym_schema(
    const uint64_t tenant_id,
    T &result,
    ObSynonymInfo &synonym_info,
    bool &is_deleted)
{
  synonym_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  synonym_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, synonym_id, synonym_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, synonym_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, synonym_info, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, synonym_name, synonym_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, object_name, synonym_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, object_database_id, synonym_info, tenant_id);
    bool ignore_column_error = true;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, status, synonym_info, int64_t, true, ignore_column_error, ObObjectStatus::VALID);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udf_schema(
    const uint64_t tenant_id,
    T &result,
    ObUDF &udf_info,
    bool &is_deleted)
{
  udf_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  udf_info.set_tenant_id(tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, udf_info);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, ret, udf_info, int);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, udf_id, udf_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, dl, udf_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, udf_info, int);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, udf_info, uint64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udf_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleUDFSchema &udf_schema,
    bool &is_deleted)
{
  udf_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  udf_schema.set_tenant_id(tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, udf_schema);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, udf_id, udf_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, ret, udf_schema, int);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, dl, udf_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, udf_schema, int);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, udf_schema, uint64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_dblink_schema(
    const uint64_t tenant_id,
    T &result,
    ObDbLinkSchema &dblink_schema,
    bool &is_deleted)
{
  dblink_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  bool is_oracle_mode = false;
  ObString default_val("");
  dblink_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, dblink_id, dblink_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, dblink_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, dblink_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, dblink_name, dblink_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, cluster_name, dblink_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tenant_name, dblink_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, user_name, dblink_schema);
    // TOTO(jiuren): decrypt
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, password, dblink_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, host_ip, dblink_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, host_port, dblink_schema, int32_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, driver_proto, dblink_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, flag, dblink_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, conn_string, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, service_name, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, authusr, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, authpwd, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, passwordx, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, authpwdx, dblink_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    bool skip_column_error = true;
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
                                                            encrypted_password,
                                                            dblink_schema,
                                                            true,
                                                            skip_column_error,
                                                            default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_host_ip, dblink_schema, true, skip_column_error, default_val);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_host_port, dblink_schema, int32_t, true, skip_column_error, 0);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_cluster_name, dblink_schema, true, skip_column_error, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_tenant_name, dblink_schema, true, skip_column_error, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_user_name, dblink_schema, true, skip_column_error, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, reverse_password, dblink_schema, true, skip_column_error, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, database_name, dblink_schema, true, skip_column_error, default_val);
    if (OB_FAIL(ret)) {
    } else if (!ObSchemaService::g_liboblog_mode_ && OB_FAIL(dblink_schema.do_decrypt_password())) {
      LOG_WARN("failed to decrypt password", K(ret));
    } else if (!ObSchemaService::g_liboblog_mode_ && OB_FAIL(dblink_schema.do_decrypt_reverse_password())) {
      LOG_WARN("failed to decrypt reverse_password", K(ret));
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                tenant_id, is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(tenant_id), K(ret));
    } else if (!is_oracle_mode && dblink_schema.get_database_name().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql dblink database_name is empty", K(ret));
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_link_table_schema(
    const uint64_t tenant_id,
    T &result,
    ObTableSchema &table_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  table_schema.reset();
  is_deleted = false;

  table_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL_WITH_TENAND_ID(result, table_id, table_schema, tenant_id);
  if (!is_deleted) {
    EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(result, schema_version, table_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_schema);
    EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, collation_type, table_schema, common::ObCollationType);
    table_schema.set_charset_type(ObCharset::charset_type_by_coll(table_schema.get_collation_type()));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_link_column_schema(
    const uint64_t tenant_id,
    T &result,
    ObColumnSchemaV2 &column_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  column_schema.reset();
  is_deleted = false;

  column_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL_WITH_TENAND_ID(result, table_id, column_schema, tenant_id);
  EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, column_id, column_schema, uint64_t);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, rowkey_position, column_schema, int64_t);
    EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, data_type, column_schema, common::ColumnType);
    if (ob_is_accuracy_length_valid_tc(column_schema.get_data_type())) {
      EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, data_length, column_schema, int32_t);
      EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, data_precision, column_schema, int16_t);
    } else {
      EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, data_precision, column_schema, int16_t);
      EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, data_scale, column_schema, int16_t);
    }
    EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, nullable, column_schema, int64_t);
    EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, collation_type, column_schema, common::ObCollationType);
    column_schema.set_charset_type(ObCharset::charset_type_by_coll(column_schema.get_collation_type()));
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, column_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_routine_schema(
    const uint64_t tenant_id, T &result, ObRoutineInfo &routine_info, bool &is_deleted)
{
  routine_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  routine_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, routine_id, routine_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, routine_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, routine_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, routine_name, routine_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, overload, routine_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, subprogram_id, routine_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, routine_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, routine_type, routine_info, ObRoutineType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, routine_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, routine_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, priv_user, routine_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, comp_flag, routine_info, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, exec_env, routine_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, routine_body, routine_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comment, routine_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, route_sql, routine_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type_id, routine_info, uint64_t);
    if (OB_SUCC(ret)
        && pl::get_tenant_id_by_object_id(routine_info.get_type_id()) != OB_SYS_TENANT_ID) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, type_id, routine_info, tenant_id);
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_routine_param_schema(
    const uint64_t tenant_id, T &result, ObRoutineParam &schema, bool &is_deleted)
{
  schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  ObString default_val("");
  schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, routine_id, schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sequence, schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, subprogram_id, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_position, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_level, schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, param_name, schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_type, schema, common::ObObjType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_length, schema, common::ObLength);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_precision, schema, common::ObPrecision);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_scale, schema, common::ObScale);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_zero_fill, schema, bool);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_charset, schema, common::ObCharsetType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, param_coll_type, schema, common::ObCollationType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, default_value, schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type_owner, schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, type_name, schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, type_subname, schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_val);
    if (OB_SUCC(ret) && ob_is_enum_or_set_type(schema.get_param_type().get_obj_type())) {
      ObString extended_type_info;
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "extended_type_info", extended_type_info);
      int64_t pos = 0;
      if (extended_type_info.empty()) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "extended_type_info is empty", K(ret));
      } else if (OB_FAIL(schema.deserialize_extended_type_info(extended_type_info.ptr(), extended_type_info.length(), pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize_extended_type_info", K(ret));
      } else {}
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_package_schema(
    const uint64_t tenant_id, T &result, ObPackageInfo &package_info, bool &is_deleted)
{
  package_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  package_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, package_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, package_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, package_name, package_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, package_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, package_info, ObPackageType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, package_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, package_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, comp_flag, package_info, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, exec_env, package_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, source, package_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comment, package_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, route_sql, package_info);

  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_trigger_schema(
    const uint64_t tenant_id, T &result, ObTriggerInfo &trigger_info, bool &is_deleted)
{
  trigger_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  ObString default_value;
  int64_t order_type_defualt_value = 0;
  int64_t action_order_default_value = 1;
  uint64_t analyze_flag_default_value = 0;
  trigger_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, trigger_id, trigger_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, trigger_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, trigger_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, trigger_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, base_object_id, trigger_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, base_object_type, trigger_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, trigger_type, trigger_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, trigger_events, trigger_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, timing_points, trigger_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, trigger_flags, trigger_info, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, trigger_name, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, update_columns, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, ref_old_name, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, ref_new_name, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, ref_parent_name, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, when_condition, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, trigger_body, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, package_spec_source, trigger_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, package_body_source, trigger_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, package_flag, trigger_info, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, package_comp_flag, trigger_info, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, package_exec_env, trigger_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sql_mode, trigger_info, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, trigger_priv_user, trigger_info,
      true, false, default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, order_type, trigger_info, int64_t,
      false, true, order_type_defualt_value);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, ref_trg_db_name, trigger_info,
      true, true, default_value);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, ref_trg_name, trigger_info,
      true, true, default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, action_order, trigger_info, int64_t,
      false, true, action_order_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, analyze_flag, trigger_info, uint64_t,
      true, true, analyze_flag_default_value);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udt_schema(const uint64_t tenant_id, T &result, ObUDTTypeInfo &udt_info, bool &is_deleted)
{
  udt_info.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  udt_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, type_id, udt_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, udt_info, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, typecode, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, properties, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, attributes, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, methods, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, hiddenmethods, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, supertypes, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, subtypes, udt_info, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, externname, udt_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, helperclassname, udt_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, local_attrs, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, local_methods, udt_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, supertypeid, udt_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, type_name, udt_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, udt_info, tenant_id);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udt_attr_schema(const uint64_t tenant_id, T &result, ObUDTTypeAttr &schema, bool &is_deleted)
{
  schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, type_id, schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, attribute, schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type_attr_id, schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, properties, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_id, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_form, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, length, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, number_precision, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, scale, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, zero_fill, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, coll_type, schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, externname, schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, xflags, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, setter, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, getter, schema, uint64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udt_coll_schema(const uint64_t tenant_id, T &result, ObUDTCollectionType &schema, bool &is_deleted)
{
  schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, coll_type_id, schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, elem_type_id, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, elem_schema_version, schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, properties, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_id, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_form, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, length, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, number_precision, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, scale, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, zero_fill, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, coll_type, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, upper_bound, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, schema, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, coll_name, schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udt_object_schema(const uint64_t tenant_id, T &result,
                                                  ObUDTObjectType &schema, bool &is_deleted)
{
  schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, object_type_id, schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, schema, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, properties, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_id, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, charset_form, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, length, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, number_precision, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, scale, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, zero_fill, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, coll_type, schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comp_flag, schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, object_name, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, exec_env, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, source, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comment, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, route_sql, schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_sequence_schema(
    const uint64_t tenant_id,
    T &result,
    ObSequenceSchema &sequence_schema,
    bool &is_deleted)
{
  sequence_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  sequence_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, sequence_id, sequence_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, sequence_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, sequence_name, sequence_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, sequence_schema, tenant_id);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, min_value, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, max_value, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, increment_by, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, start_with, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, cache_size, sequence_schema);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, cycle_flag, sequence_schema);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, order_flag, sequence_schema);
    bool ignore_column_error = false;
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, is_system_generated, sequence_schema, true, ignore_column_error, false);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_audit_schema(
    const uint64_t tenant_id,
    T &result,
    ObSAuditSchema &audit_schema,
    bool &is_deleted)
{
  audit_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  audit_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, audit_id, audit_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, audit_type, audit_schema, ObSAuditType);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, owner_id, audit_schema, tenant_id);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, operation_type, audit_schema, ObSAuditOperationType);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, in_success, audit_schema, uint64_t);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, in_failure, audit_schema, uint64_t);
  }
  SHARE_SCHEMA_LOG(DEBUG, "finish fill audit schema", K(is_deleted), KR(ret), K(tenant_id),
                   K(audit_schema));
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_recycle_object(
    const uint64_t tenant_id,
    T &result,
    ObRecycleObject &recycle_obj)
{
  int ret = common::OB_SUCCESS;
  recycle_obj.reset();
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, recycle_obj, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, recycle_obj, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, recycle_obj, tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, object_name, recycle_obj);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, original_name, recycle_obj);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, recycle_obj, ObRecycleObject::RecycleObjType);
  if (OB_SUCC(ret)) {
    recycle_obj.set_tenant_id(tenant_id);
  }
  return ret;
}


template<typename T>
int ObSchemaRetrieveUtils::fill_label_se_policy_schema(
    const uint64_t tenant_id,
    T &result,
    ObLabelSePolicySchema &label_se_policy_schema,
    bool &is_deleted)
{

  label_se_policy_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;

  label_se_policy_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_policy_id, label_se_policy_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, label_se_policy_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_name, label_se_policy_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, label_se_policy_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, default_options, label_se_policy_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, label_se_policy_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_label_se_component_schema(
    const uint64_t tenant_id,
    T &result,
    ObLabelSeComponentSchema &label_se_component_schema,
    bool &is_deleted)
{

  label_se_component_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;

  label_se_component_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_policy_id, label_se_component_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_component_id, label_se_component_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, comp_type, label_se_component_schema, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, label_se_component_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, comp_num, label_se_component_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, short_name, label_se_component_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, long_name, label_se_component_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, parent_name, label_se_component_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_label_se_label_schema(
    const uint64_t tenant_id,
    T &result,
    ObLabelSeLabelSchema &label_se_label_schema,
    bool &is_deleted)
{

  label_se_label_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;

  label_se_label_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_policy_id, label_se_label_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_label_id, label_se_label_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, label_se_label_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, label_tag, label_se_label_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, label, label_se_label_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flag, label_se_label_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_label_se_user_level_schema(
    const uint64_t tenant_id,
    T &result,
    ObLabelSeUserLevelSchema &label_se_user_level_schema,
    bool &is_deleted)
{

  label_se_user_level_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;

  label_se_user_level_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, user_id, label_se_user_level_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_user_level_id, label_se_user_level_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, label_se_policy_id, label_se_user_level_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, label_se_user_level_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, maximum_level, label_se_user_level_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, minimum_level, label_se_user_level_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, default_level, label_se_user_level_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, row_level, label_se_user_level_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_schema_operation(
    const uint64_t tenant_id,
    T &result, ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
    ObSchemaOperation &schema_operation)
{
  int ret = common::OB_SUCCESS;
  schema_operation.reset();
  ObSchemaOperationType operation_type = OB_INVALID_DDL_OP;
  EXTRACT_INT_FIELD_MYSQL(result, "operation_type", operation_type, ObSchemaOperationType);
  if (is_tenant_operation(operation_type)) {
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", schema_operation.tenant_id_, uint64_t);
  } else {
    schema_operation.tenant_id_ = tenant_id;
  }
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", schema_operation.table_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "database_id", schema_operation.database_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "tablegroup_id", schema_operation.tablegroup_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "user_id", schema_operation.user_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "schema_version", schema_operation.schema_version_, uint64_t);
  ObString str_val;
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "database_name", str_val);
  if (OB_SUCC(ret)) {
    schema_operations.write_string(str_val, &schema_operation.database_name_);
  }
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "table_name", str_val);
  if (OB_SUCC(ret)) {
    schema_operations.write_string(str_val, &schema_operation.table_name_);
  }

  if (OB_SUCC(ret)) {
    schema_operation.op_type_ = operation_type;
    SHARE_SCHEMA_LOG(INFO, "fill schema operation", K(schema_operation));
  } else {
    SHARE_SCHEMA_LOG(WARN, "fail to fill schema operation", KR(ret));
  }
  return ret;
}


/***********************************************************************
 *
 * for simple schemas
 *
 ***********************************************************************/

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_tenant_schema(
    ObISQLClient &client,
    T &result,
    ObIArray<S> &tenant_schema_array)
{
  UNUSED(client);
  int ret = common::OB_SUCCESS;

  uint64_t prev_tenant_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S tenant_schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    tenant_schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_tenant_schema(result, tenant_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill tenant schema", K(ret));
    } else if (tenant_schema.get_tenant_id() == prev_tenant_id) {
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "tenant is is_deleted, don't add",
               "tenant_id", tenant_schema.get_tenant_id());
    } else if (OB_FAIL(tenant_schema_array.push_back(tenant_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve tenant schema", K(tenant_schema), K(is_deleted));
    }
    prev_tenant_id = tenant_schema.get_tenant_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all tenant schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_system_variable_obj(
    const uint64_t tenant_id,
    T &result,
    ObIAllocator &allocator,
    ObObj &out_var_obj)
{
  UNUSED(tenant_id);
  int ret = common::OB_SUCCESS;
  int64_t vtype = 0;
  bool is_deleted = false;
  char *value_buf = NULL;
  ObString result_value;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "data_type", vtype, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "value", result_value);
  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN,"fail to extract data", K(ret));
  } else if (is_deleted) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else if (!result_value.empty() && OB_ISNULL(value_buf = static_cast<char*>(allocator.alloc(result_value.length())))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(WARN,"fail to alloc char array", K(ret));
  } else {
    if (!result_value.empty()) {
      MEMCPY(value_buf, result_value.ptr(), result_value.length());
    }
    ObString svalue(result_value.length(), value_buf);
    ObObj var_value;
    var_value.set_varchar(svalue);
    ObObjType var_type = static_cast<ObObjType>(vtype);
    //checked with qianfu,there is no timestamp data in __all_sys_variables, so just pass timezone
    //info as NULL
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
    ObObj casted_val;
    const ObObj *res_val = NULL;
    if (OB_FAIL(ObObjCaster::to_type(var_type, cast_ctx, var_value, casted_val, res_val))) {
      _SHARE_SCHEMA_LOG(WARN,"failed to cast object, ret=%d cell=%s from_type=%s to_type=%s",
                       ret, to_cstring(var_value), ob_obj_type_str(var_value.get_type()), ob_obj_type_str(var_type));
    } else if (OB_ISNULL(res_val)) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN,"casted success, but res_val is NULL", K(ret), K(var_value), K(var_type));
    } else {
      out_var_obj = *res_val;
      if (ob_is_string_type(out_var_obj.get_type())) {
        out_var_obj.set_collation_level(CS_LEVEL_SYSCONST);
        if (CS_TYPE_INVALID == out_var_obj.get_collation_type()) {
          out_var_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
      }
    }
  }
  return ret;
}
template<typename T, typename SCHEMA>
int ObSchemaRetrieveUtils::retrieve_system_variable(const uint64_t tenant_id, T &result, SCHEMA &sys_variable_schema)
{
  UNUSED(tenant_id);
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  ObSysVarSchema sysvar_schema(&allocator);
  ObString prev_sys_name;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    bool is_deleted = false;
    sysvar_schema.reset();
    allocator.reuse();
    if (OB_FAIL(fill_sysvar_schema(tenant_id, result, sysvar_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill sysvar schema", K(ret));
    } else if (ObCharset::case_insensitive_equal(prev_sys_name, sysvar_schema.get_name())) {
      //do nothing
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "sysvar is is_deleted, don't add", K(sysvar_schema));
    } else if (OB_FAIL(sys_variable_schema.add_sysvar_schema(sysvar_schema))) {
      if (common::OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
        ret = common::OB_SUCCESS;
        SHARE_SCHEMA_LOG(INFO, "sysvar maybe come from diff version, ingore it", K(sysvar_schema));
      } else {
        SHARE_SCHEMA_LOG(WARN, "add sysvar schema failed", K(ret), K(sysvar_schema));
      }
    } else if (FALSE_IT(tmp_allocator.reuse())) {
    } else if (OB_FAIL(ob_write_string(tmp_allocator, sysvar_schema.get_name(), prev_sys_name))) {
      SHARE_SCHEMA_LOG(WARN, "write sysvar name failed", K(ret), K(sysvar_schema));
    } else {
      SHARE_SCHEMA_LOG(INFO, "fetch system variable schema finish", K(sysvar_schema));
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all sysvar schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_sysvar_schema(const uint64_t tenant_id, T &result, ObSysVarSchema &schema, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  schema.reset();
  is_deleted = false;

  schema.set_tenant_id(tenant_id);
  EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, schema);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, schema_version, schema, int64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_type, schema, ObObjType);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, value, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, min_val, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, max_val, schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, flags, schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, info, schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, zone, schema)
  }
  return ret;
}

/**************************************************************************************
 *
 * for simple schemas
 *
 *************************************************************************************/

#define RETRIEVE_SCHEMA_FUNC_DEFINE(SCHEMA)     \
  template<typename T, typename S>                                       \
  int ObSchemaRetrieveUtils::retrieve_##SCHEMA##_schema( \
      const uint64_t tenant_id, \
      T &result, \
      ObIArray<S> &schema_array)                          \
  {                                                                 \
    int ret = common::OB_SUCCESS;                                           \
    uint64_t prev_id = common::OB_INVALID_ID;                               \
    ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES); \
    S schema(&allocator);                                             \
    int64_t count = 0; \
    while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {  \
      schema.reset();                                                   \
      allocator.reuse();                                                \
      bool is_deleted = false;                                          \
      count++; \
      if (OB_FAIL(fill_##SCHEMA##_schema(tenant_id, result, schema, is_deleted))) { \
        SHARE_SCHEMA_LOG(WARN, "fail to fill "#SCHEMA" schema ", K(ret));              \
      } else if (schema.get_##SCHEMA##_id() == prev_id) {                \
        SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_##SCHEMA##_id(), "version", schema.get_schema_version());                     \
      } else if (is_deleted) {                                           \
        SHARE_SCHEMA_LOG(INFO, #SCHEMA" is is_deleted, don't add",                     \
                 #SCHEMA"_id", schema.get_##SCHEMA##_id());              \
      } else if (OB_FAIL(schema_array.push_back(schema))) {              \
        SHARE_SCHEMA_LOG(WARN, "failed to push back", K(schema), K(ret));              \
      } else {                                                           \
        SHARE_SCHEMA_LOG(INFO, "retrieve "#SCHEMA" schema succeed", K(schema));        \
      }                                                                  \
      prev_id = schema.get_##SCHEMA##_id();                              \
    }                                                                    \
    if (ret != common::OB_ITER_END) {                                                \
      SHARE_SCHEMA_LOG(WARN, "fail to get all "#SCHEMA" schema. iter quit. ", K(ret));     \
    } else {                                                                 \
      ret = common::OB_SUCCESS;                                                      \
      SHARE_SCHEMA_LOG(INFO, "retrieve all "#SCHEMA" schemas succeed", K(schema_array));       \
    }                                                                        \
    return ret;                                                              \
  }

RETRIEVE_SCHEMA_FUNC_DEFINE(user);
RETRIEVE_SCHEMA_FUNC_DEFINE(database);
RETRIEVE_SCHEMA_FUNC_DEFINE(tablegroup);
RETRIEVE_SCHEMA_FUNC_DEFINE(outline);
RETRIEVE_SCHEMA_FUNC_DEFINE(package);
RETRIEVE_SCHEMA_FUNC_DEFINE(trigger);
RETRIEVE_SCHEMA_FUNC_DEFINE(sequence);
RETRIEVE_SCHEMA_FUNC_DEFINE(keystore);
RETRIEVE_SCHEMA_FUNC_DEFINE(tablespace);
RETRIEVE_SCHEMA_FUNC_DEFINE(audit);
RETRIEVE_SCHEMA_FUNC_DEFINE(context);
RETRIEVE_SCHEMA_FUNC_DEFINE(mock_fk_parent_table);

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_routine_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &routine_infos)
{
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S routine_info(&allocator);
  uint64_t pre_routine_id = OB_INVALID_ID;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    routine_info.reset();
    allocator.reuse();
    if (OB_FAIL(fill_routine_schema(tenant_id, result, routine_info, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill routine info ", K(ret));
    } else if (routine_info.get_routine_id() == pre_routine_id) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "routine info is deleted", K(routine_info));
    } else if (OB_FAIL(routine_infos.push_back(routine_info))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve routine schema succeed", K(routine_info));
    }
    if (OB_SUCC(ret)) {
      pre_routine_id = routine_info.get_routine_id();
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all routine info. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve routine infos succeed", K(tenant_id));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_trigger_list(
    const uint64_t tenant_id,
    T &result,
    ObIArray<uint64_t> &trigger_list)
{
  int ret = common::OB_SUCCESS;
  uint64_t pre_trigger_id = common::OB_INVALID_ID;
  uint64_t trigger_id = common::OB_INVALID_ID;
  bool is_deleted = false;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_trigger_id(tenant_id, result, trigger_id, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill trigger id failed", K(ret));
    } else if (pre_trigger_id == trigger_id) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "trigger is deleted", K(trigger_id));
    } else if (OB_FAIL(trigger_list.push_back(trigger_id))) {
      SHARE_SCHEMA_LOG(WARN, "add trigger id failed", K(pre_trigger_id), K(trigger_id), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "retrieve trigger id succeed", K(trigger_id));
    }
    pre_trigger_id = trigger_id;
    trigger_id = common::OB_INVALID_ID;
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_routine_param_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<ObRoutineInfo> &routine_infos)
{
  int ret = common::OB_SUCCESS;
  uint64_t pre_routine_id = OB_INVALID_ID;
  int64_t pre_sequence = OB_INVALID_ID;
  bool is_deleted = false;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObRoutineParam cur_param(&allocator);
  ObRoutineParamSetter routine_param_setter(routine_infos);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    cur_param.reset();
    allocator.reuse();
    if (OB_FAIL(fill_routine_param_schema(tenant_id, result, cur_param, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill routine param schema failed", K(ret));
    } else if (cur_param.get_routine_id() == pre_routine_id
               && cur_param.get_sequence() == pre_sequence) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "routine param info is deleted", K(cur_param));
    } else if (OB_FAIL(routine_param_setter.add_routine_param(cur_param))) {
      SHARE_SCHEMA_LOG(WARN, "add routine param failed", K(pre_routine_id), K(pre_sequence), K(cur_param), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "add routine param success", K(cur_param));
    }
    if (OB_SUCC(ret)) {
      pre_routine_id = cur_param.get_routine_id();
      pre_sequence = cur_param.get_sequence();
    }
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_udt_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &udt_infos)
{
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S udt_info(&allocator);
  int64_t pre_type_id = OB_INVALID_ID;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    udt_info.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_udt_schema(tenant_id, result, udt_info, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill udt info ", K(ret));
    } else if (udt_info.get_type_id() == pre_type_id) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "udt info is deleted", K(udt_info));
    } else if (OB_FAIL(udt_infos.push_back(udt_info))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve udt schema success", K(udt_info));
    }
    if (OB_SUCC(ret)) {
      pre_type_id = udt_info.get_type_id();
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all udt info. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve udt infos succeed", K(tenant_id));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_udt_attr_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<ObUDTTypeInfo> &udt_infos)
{
  int ret = common::OB_SUCCESS;
  ObUDTTypeAttr cur_attr;
  ObUDTTypeAttr last_attr;
  bool is_deleted = false;
  ObUDTAttrSetter udt_attr_setter(udt_infos);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_udt_attr_schema(tenant_id, result, cur_attr, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill udt attr schema failed", K(ret));
    } else if (last_attr.get_type_id() == cur_attr.get_type_id()
               && last_attr.get_attribute() == cur_attr.get_attribute()) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "udt param info is deleted", K(cur_attr));
    } else if (OB_FAIL(udt_attr_setter.add_type_attr(cur_attr))) {
      SHARE_SCHEMA_LOG(WARN, "add udt param failed", K(last_attr), K(cur_attr), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "add udt param success", K(cur_attr));
    }
    OX (last_attr = cur_attr);
    OX (ret = last_attr.get_err_ret());
    OX (cur_attr.reset());
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_udt_coll_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<ObUDTTypeInfo> &udt_infos)
{
  int ret = common::OB_SUCCESS;
  ObUDTCollectionType cur_coll;
  ObUDTCollectionType last_coll;
  bool is_deleted = false;
  ObUDTAttrSetter udt_coll_setter(udt_infos);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_udt_coll_schema(tenant_id, result, cur_coll, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill udt coll schema failed", K(ret));
    } else if (last_coll.get_coll_type_id() == cur_coll.get_coll_type_id()) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "udt attr info is deleted", K(cur_coll));
    } else if (OB_FAIL(udt_coll_setter.add_type_coll(cur_coll))) {
      SHARE_SCHEMA_LOG(WARN, "add udt param failed", K(last_coll), K(cur_coll), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "add udt param success", K(cur_coll));
    }
    OX (last_coll = cur_coll);
    OX (ret = last_coll.get_err_ret());
    OX (cur_coll.reset());
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}


template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_udt_object_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &udt_infos)
{
  int ret = common::OB_SUCCESS;
  ObUDTObjectType cur_obj;
  ObUDTObjectType last_obj;
  bool is_deleted = false;
  ObUDTAttrSetter udt_obj_setter(udt_infos);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_udt_object_schema(tenant_id, result, cur_obj, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill udt object type schema failed", K(ret));
    } else if (!cur_obj.is_valid()
              || (last_obj.get_type() == cur_obj.get_type()
              && last_obj.get_object_type_id() == cur_obj.get_object_type_id())) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "udt object type info is deleted", K(cur_obj));
    } else if (OB_FAIL(udt_obj_setter.add_type_object(cur_obj))) {
      SHARE_SCHEMA_LOG(WARN, "add udt param failed", K(last_obj), K(cur_obj), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "add udt param success", K(cur_obj));
    }
    OX (last_obj = cur_obj);
    OX (ret = last_obj.get_err_ret());
    OX (cur_obj.reset());
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

RETRIEVE_SCHEMA_FUNC_DEFINE(synonym);

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_udf_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
    int ret = common::OB_SUCCESS;
    common::ObString udf_name;
    ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
    ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
    S schema(&allocator);
    while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
      bool is_deleted = false;
      schema.reset();
      allocator.reuse();
      if (OB_FAIL(fill_udf_schema(tenant_id, result, schema, is_deleted))) {
        SHARE_SCHEMA_LOG(WARN, "fail to fill udf schema", K(ret));
      } else if (schema.get_udf_name_str() == udf_name) {
        SHARE_SCHEMA_LOG(DEBUG, "debug ignore", K(schema.get_udf_name_str()), "version", schema.get_schema_version());
      } else if (is_deleted) {
        SHARE_SCHEMA_LOG(INFO, "udf is is_deleted, don't add", K(schema.get_udf_name_str()));
      } else if (OB_FAIL(schema_array.push_back(schema))) {
        SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
      } else {
        SHARE_SCHEMA_LOG(INFO, "retrieve udf schema succeed", K(schema));
      }
      tmp_allocator.reuse();
      if (FAILEDx(ob_write_string(tmp_allocator, schema.get_udf_name_str(), udf_name))) {
        SHARE_SCHEMA_LOG(WARN, "write udf_name failed", KR(ret), K(schema));
      }
    }
    if (ret != common::OB_ITER_END) {
      SHARE_SCHEMA_LOG(WARN, "fail to get all udf schema. iter quit. ", K(ret));
    } else {
      ret = common::OB_SUCCESS;
      SHARE_SCHEMA_LOG(INFO, "retrieve udf schemas succeed", K(tenant_id));
    }
    return ret;
}

RETRIEVE_SCHEMA_FUNC_DEFINE(dblink);

template<typename AT, typename TST>
int ObSchemaRetrieveUtils::retrieve_link_table_schema(
    const uint64_t tenant_id,
    AT &result,
    ObIAllocator &allocator,
    TST *&table_schema)
{
  int ret = common::OB_SUCCESS;
  bool is_deleted = false;
  TST tmp_table_schema;

  table_schema = NULL;
  SHARE_SCHEMA_LOG(DEBUG, "retrieve link table schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) { //no record
      ret = common::OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "no row", K(ret));
    } else {
      SHARE_SCHEMA_LOG(WARN, "get link table schema failed, iter quit", K(ret));
    }
  } else if (OB_FAIL(fill_link_table_schema(tenant_id, result, tmp_table_schema, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "fail to fill link table schema. ", K(is_deleted), K(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_table_schema, table_schema))) {
    SHARE_SCHEMA_LOG(WARN, "alloc link table schema failed", K(ret));
  } else {
    //check if this is only one
    if (OB_ITER_END != (ret = result.next())) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "should be one row only", K(ret));
    } else {
      ret = common::OB_SUCCESS;
      SHARE_SCHEMA_LOG(INFO, "retrieve link table schema succeed", K(*table_schema), K(is_deleted));
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_link_column_schema(
    const uint64_t tenant_id,
    T &result,
    const common::sqlclient::DblinkDriverProto driver_type,
    ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  bool is_deleted = false;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObColumnSchemaV2 column_schema(&allocator);

  SHARE_SCHEMA_LOG(DEBUG, "retrieve link column schema");
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    column_schema.reset();
    allocator.reuse();
    if (OB_FAIL(fill_link_column_schema(tenant_id, result, column_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill link column schema. ", K(is_deleted), K(ret));
    } else if (FALSE_IT(column_schema.set_table_id(table_schema.get_table_id()))) {
    } else if (FALSE_IT(column_schema.set_column_id(column_schema.get_column_id()
                      + (common::sqlclient::DBLINK_DRV_OCI == driver_type ? OB_END_RESERVED_COLUMN_ID_NUM : 0)))) {
    // } else if (OB_FAIL(table_schema.add_column(column_schema))) {
    //   SHARE_SCHEMA_LOG(WARN, "fail to add link column schema. ", K(column_schema), K(ret));
    } else {
      // deal with oracle number
      // create table (a int, b number, c number(38));
      // oracle (precision, scale is (null, 0), (null, null), (38, 0),
      // ob (precision, scale is (38, 0), (null, null), (38, 0))
      // we have to set this same to the result_type to be same
      if (common::sqlclient::DBLINK_DRV_OCI == driver_type && ob_is_oracle_numeric_type(column_schema.get_data_type())) {
        if (-1 == column_schema.get_data_precision() && 0 == column_schema.get_data_scale()) {
          column_schema.set_data_precision(38);
        }
      }
      if (OB_FAIL(table_schema.add_column(column_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fail to add link column schema. ", K(column_schema), K(ret));
      }
    }
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve link column schema succeed", K(table_schema));
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_db_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &db_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  S db_priv(&allocator);
  ObOriginalDBKey pre_priv;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    db_priv.reset();
    allocator.reuse();
    if (OB_FAIL(fill_db_priv_schema(tenant_id, result, db_priv, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill database privileges", K(ret));
    } else if (db_priv.get_original_key() == pre_priv) {
      // ignore it
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "db_priv is is_deleted", K(db_priv));
    } else if (OB_FAIL(db_priv_array.push_back(db_priv))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to push back", K(ret));
    }
    if (OB_SUCC(ret)) {
      tmp_allocator.reuse();
      if (OB_FAIL(pre_priv.deep_copy(db_priv.get_original_key(), tmp_allocator))) {
        SHARE_SCHEMA_LOG(WARN, "alloc_table_schema failed", KR(ret));
      }
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get database privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_obj_priv_schema_inner(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &obj_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObRawObjPriv priv_id = OBJ_PRIV_ID_NONE;
  ObRawObjPriv prev_priv_id = OBJ_PRIV_ID_MAX;
  S prev_priv;
  S obj_priv;
  uint64_t option;
  ObPackedObjPriv packed_obj_privs;
  bool is_deleted;

  /* gather obj priv by key <obj_id, obj_type, col_id, grantor_id, grantee_id, priv_id> */
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    if (OB_FAIL(fill_obj_priv_schema(tenant_id, result, obj_priv, is_deleted, priv_id, option))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill obj privileges", K(ret));
    } else if ((prev_priv.get_sort_key() == obj_priv.get_sort_key()
               && prev_priv_id == priv_id)) {
      // jump over same priv operation before , eg: revoke or add grant option
      ret = common::OB_SUCCESS;
    } else {
      // it's the first row
      SHARE_SCHEMA_LOG(TRACE, "get priv operation", K(priv_id), K(is_deleted));
      if (is_deleted) {
        //jump over revoke operation
        SHARE_SCHEMA_LOG(TRACE, "revoke priv", K(obj_priv));
      } else {
        /* set priv info and push back sys_priv */
        if (OB_SUCC(ret)) {
          packed_obj_privs = 0;
          if (OB_FAIL(ObPrivPacker::pack_raw_obj_priv(option, priv_id, packed_obj_privs))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to pack raw obj priv", K(ret));
          } else {
            obj_priv.set_obj_privs(packed_obj_privs);
            if (OB_FAIL(obj_priv_array.push_back(obj_priv))) {
              SHARE_SCHEMA_LOG(WARN, "Fail to push back", K(ret));
            }
          }
        }
      }
      OX (prev_priv_id = priv_id);
      OX (prev_priv = obj_priv);
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get obj privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_sys_priv_schema_inner(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &sys_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObRawPriv priv_id;
  ObRawPriv prev_priv_id = PRIV_ID_MAX;
  ObSysPrivKey prekey;
  uint64_t option;
  ObPackedPrivArray packed_priv_array;
  bool is_deleted;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S sys_priv(&allocator);
  /* gather sys priv by key <grantee_id, priv_id> */
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    sys_priv.reset();
    allocator.reuse();
    if (OB_FAIL(fill_sys_priv_schema(tenant_id, result, sys_priv, is_deleted, priv_id, option))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill system privileges", K(ret));
    } else if ((sys_priv.get_key() == prekey && prev_priv_id == priv_id)) {
      // jump over same priv operation before
      ret = common::OB_SUCCESS;
    } else {
      // it's the first row
      SHARE_SCHEMA_LOG(TRACE, "get priv operation", K(priv_id), K(is_deleted));
      if (is_deleted) {
        //jump over revoke operation
        SHARE_SCHEMA_LOG(TRACE, "revoke priv", K(sys_priv));
        /*sys_priv.set_revoke();
        if (OB_FAIL(sys_priv_array.push_back(sys_priv))) {
          SHARE_SCHEMA_LOG(WARN, "Fail to push back", K(ret));
        }*/
      } else {
        /* set priv info and push back sys_priv */
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObPrivPacker::init_packed_array(packed_priv_array))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to init packed array", K(ret));
          } else if (OB_FAIL(ObPrivPacker::pack_raw_priv(option, priv_id, packed_priv_array))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to pack raw priv", K(ret));
          } else if (OB_FAIL(sys_priv.set_priv_array(packed_priv_array))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to set priv array", K(ret));
          } else if (OB_FAIL(sys_priv_array.push_back(sys_priv))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to push back", K(ret));
          }
        }
      }
      OX (prev_priv_id = priv_id);
      OX (prekey = sys_priv.get_key());
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get system privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template<typename S>
int ObSchemaRetrieveUtils::push_prev_array_if_has(
    ObIArray<S> &sys_priv_array,
    S &sys_priv,
    ObPackedPrivArray &packed_grant_privs)
{
  int ret = common::OB_SUCCESS;
  if (packed_grant_privs.count() > 0) {
    if (OB_FAIL(sys_priv.set_priv_array(packed_grant_privs))) {
      SHARE_SCHEMA_LOG(WARN, "set priv array", K(packed_grant_privs), K(ret));
    } else if (OB_FAIL(sys_priv_array.push_back(sys_priv))) {
      SHARE_SCHEMA_LOG(WARN, "push back failed", K(sys_priv), K(ret));
    }
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_sys_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &sys_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObSEArray<S, 3> tmp_priv_array;
  S prev_priv;
  S it_priv;
  ObPackedPrivArray packed_grant_privs;

  if (OB_FAIL(retrieve_sys_priv_schema_inner(tenant_id, result, tmp_priv_array))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve_sys_priv_schema_inner failed", K(ret));
  } else {
    ARRAY_FOREACH(tmp_priv_array, i) {
      it_priv = tmp_priv_array.at(i);
      if (prev_priv.get_key() == it_priv.get_key()) {
        if (OB_FAIL(ObPrivPacker::merge_two_packed_array(packed_grant_privs,
                                                        it_priv.get_priv_array()))) {
          SHARE_SCHEMA_LOG(WARN, "merg two packed array failed", K(ret));
        }
      } else {
        /* push back previous group */
        if (OB_FAIL(push_prev_array_if_has(sys_priv_array,
                                           prev_priv,
                                           packed_grant_privs))) {
          SHARE_SCHEMA_LOG(WARN, "push prev array if has failed", K(ret));
        } else {
          /* initialize new sys priv group */
          packed_grant_privs = it_priv.get_priv_array();
          prev_priv = it_priv;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    /* push back the last group */
    if (OB_FAIL(push_prev_array_if_has(sys_priv_array,
                                       prev_priv,
                                       packed_grant_privs))) {
      SHARE_SCHEMA_LOG(WARN, "push prev array if has failed", K(ret));
    }
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_table_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &table_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObArenaAllocator tmp_allocator(ObModIds::OB_TEMP_VARIABLES);
  S table_priv(&allocator);
  ObTablePrivSortKey pre_table_sort_key;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    table_priv.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_table_priv_schema(tenant_id, result, table_priv, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill table_priv", K(ret));
    } else if (table_priv.get_sort_key() == pre_table_sort_key) {
      // ignore it
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "table_priv is is_deleted", K(table_priv));
    } else if (OB_FAIL(table_priv_array.push_back(table_priv))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to push back", K(ret));
    }
    if (OB_SUCC(ret)) {
      tmp_allocator.reuse();
      if (OB_FAIL(pre_table_sort_key.deep_copy(table_priv.get_sort_key(), tmp_allocator))) {
        SHARE_SCHEMA_LOG(WARN, "alloc_table_schema failed", KR(ret));
      }
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get table privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_obj_priv_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &obj_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObSEArray<S, 3> tmp_obj_priv_array;
  S prev_obj_priv;
  S it_obj_priv;
  ObPackedObjPriv packed_obj_privs = 0;

  // {
  //   ObMySQLProxy::MySQLResult, res;
  //   sqlclient::ObMySQLResult *result = NULL;
  //   uint64_t  t_id;
  //   uint64_t  u_id;

  //   while (OB_SUCC(ret) && OB_SUCC(result->next())) {
  //     EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "tenant_id", t_id, uint64_t);
  //     EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "user_id", u_id, uint64_t);
  //     EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "user_name", user_name);
  //     LOG_WARN("ZLM retrieve result ", K(grantee_id), K(t_id), K(u_id), K(user_name));
  //   }
  // }

  if (OB_FAIL(retrieve_obj_priv_schema_inner(tenant_id, result, tmp_obj_priv_array))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve_obj_priv_schema_inner failed", K(ret));
  } else {
    ARRAY_FOREACH(tmp_obj_priv_array, i) {
      it_obj_priv = tmp_obj_priv_array.at(i);
      if (prev_obj_priv.get_sort_key() == it_obj_priv.get_sort_key()) {
        packed_obj_privs |= it_obj_priv.get_obj_privs();
      } else {
        /* push back previous group */
        if (OB_FAIL(push_prev_obj_privs_if_has(obj_priv_array,
                                               prev_obj_priv,
                                               packed_obj_privs))) {
          SHARE_SCHEMA_LOG(WARN, "push prev obj privs if has failed", K(ret));
        } else {
          /* initialize new sys priv group */
          packed_obj_privs = it_obj_priv.get_obj_privs();
          prev_obj_priv = it_obj_priv;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    /* push back the last group */
    if (OB_FAIL(push_prev_obj_privs_if_has(obj_priv_array,
                                           prev_obj_priv,
                                           packed_obj_privs))) {
      SHARE_SCHEMA_LOG(WARN, "push prev obj privs if has failed", K(ret));
    }
  }
  return ret;
}

template<typename S>
int ObSchemaRetrieveUtils::push_prev_obj_privs_if_has(
    ObIArray<S> &obj_priv_array,
    S &obj_priv,
    ObPackedObjPriv &packed_obj_privs)
{
  int ret = common::OB_SUCCESS;
  if (packed_obj_privs != 0) {
    obj_priv.set_obj_privs(packed_obj_privs);
    if (OB_FAIL(obj_priv_array.push_back(obj_priv))) {
      SHARE_SCHEMA_LOG(WARN, "push back failed", K(obj_priv), K(ret));
    }
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_label_se_policy_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_label_se_policy_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill label_se_policy schema ", K(ret));
    } else if (schema.get_label_se_policy_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_label_se_policy_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "label_se_policy is is_deleted, don't add",
               "label_se_policy_id", schema.get_label_se_policy_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve label_se_policy schema succeed", K(schema));
    }
    prev_id = schema.get_label_se_policy_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all label_se_policy schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve label_se_policy schemas succeed", K(tenant_id));
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_label_se_component_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_label_se_component_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill label_se_component schema ", K(ret));
    } else if (schema.get_label_se_component_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_label_se_component_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "label_se_component is is_deleted, don't add",
               "label_se_component_id", schema.get_label_se_component_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve label_se_component schema succeed", K(schema));
    }
    prev_id = schema.get_label_se_component_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all label_se_component schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve label_se_component schemas succeed", K(tenant_id));
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_label_se_label_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_label_se_label_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill label_se_label schema ", K(ret));
    } else if (schema.get_label_se_label_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_label_se_label_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "label_se_label is is_deleted, don't add",
               "label_se_label_id", schema.get_label_se_label_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve label_se_label schema succeed", K(schema));
    }
    prev_id = schema.get_label_se_label_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all label_se_label schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve label_se_label schemas succeed", K(tenant_id));
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_label_se_user_level_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_label_se_user_level_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill label_se_user_level schema ", K(ret));
    } else if (schema.get_label_se_user_level_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_label_se_user_level_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "label_se_user_level is is_deleted, don't add",
               "label_se_user_level_id", schema.get_label_se_user_level_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve label_se_user_level schema succeed", K(schema));
    }
    prev_id = schema.get_label_se_user_level_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all label_se_user_level schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve label_se_user_level schemas succeed", K(tenant_id));
  }
  return ret;
}

//for simple schemas

template<typename T>
int ObSchemaRetrieveUtils::fill_user_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleUserSchema &user_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  user_schema.reset();
  is_deleted = false;
  user_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, user_id, user_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, user_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, user_name, user_schema);
    ObString default_host_value(OB_DEFAULT_HOST_NAME);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, host, user_schema, false, ObSchemaService::g_ignore_column_retrieve_error_, default_host_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        type, user_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_tenant_schema(T &result,
                                              ObSimpleTenantSchema &tenant_schema,
                                              bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  tenant_schema.reset();
  is_deleted = false;

  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, tenant_id, tenant_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    bool skip_null_error = false;
    ObString primary_zone_str;
    ObString locality_str;
    ObString previous_locality_str;
    int64_t default_gmt_modified = 0;
    int64_t gmt_modified = 0;
    ObCompatibilityMode default_compat_mode = ObCompatibilityMode::MYSQL_MODE;
    int64_t default_drop_tenant_time = OB_INVALID_TIMESTAMP;
    ObString tenant_status_str("");
    ObString arbitration_service_status_str("");
    ObString default_tenant_status_str("NORMAL");
    ObString default_arbitration_service_status_str("DISABLED");
    int64_t default_in_recyclebin = 0;
    common::ObTimeZoneInfoWrap tz_info_wrap;
    ret = GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(
          result.get_timestamp, "gmt_modified", gmt_modified,
          default_gmt_modified, tz_info_wrap.get_time_zone_info());
    tenant_schema.set_gmt_modified(gmt_modified);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tenant_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tenant_name, tenant_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "primary_zone", primary_zone_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "locality", locality_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "previous_locality", previous_locality_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, compatibility_mode,
        tenant_schema, common::ObCompatibilityMode, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_,
        default_compat_mode);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, drop_tenant_time,
        tenant_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_,
        default_drop_tenant_time);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "status", tenant_status_str,
        skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_tenant_status_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, in_recyclebin,
        tenant_schema, int64_t, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_,
        default_in_recyclebin);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "arbitration_service_status", arbitration_service_status_str,
        skip_null_error, true/*ignore_column_retrieve_error*/, default_arbitration_service_status_str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_schema.set_primary_zone(primary_zone_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set primary zone", K(ret), K(primary_zone_str));
    } else if (OB_FAIL(tenant_schema.set_locality(locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set locality", K(ret), K(locality_str));
    } else if (OB_FAIL(tenant_schema.set_previous_locality(previous_locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set previous_locality", K(ret), K(previous_locality_str));
    }
    if (OB_SUCC(ret)) {
      ObTenantStatus status = TENANT_STATUS_MAX;
      if (OB_FAIL(get_tenant_status(tenant_status_str, status))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get tenant status", K(ret), K(tenant_status_str));
      } else {
        tenant_schema.set_status(status);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_arbitration_service_status_from_string(arbitration_service_status_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set arb status from string", K(ret), K(arbitration_service_status_str));
      }
    }
  }
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(TRACE, "retrieve tenant schema", K(tenant_schema), K(is_deleted), KR(ret));
  } else {
    SHARE_SCHEMA_LOG(WARN, "retrieve tenant schema failed",
                    "tenant_id", tenant_schema.get_tenant_id(),
                    "schema_version", tenant_schema.get_schema_version(),
                    K(is_deleted), KR(ret));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_trigger_id(const uint64_t tenant_id, T &result,
                                           uint64_t &trigger_id, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "trigger_id", trigger_id, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_table_schema(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObSimpleTableSchemaV2 &table_schema,
    bool &is_deleted)
{
  UNUSED(check_deleted);
  int ret = common::OB_SUCCESS;
  bool ignore_column_error = false;
  table_schema.reset();
  is_deleted = false;
  table_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, table_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_type, table_schema, ObTableType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, data_table_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_num, table_schema, int64_t);
    int64_t def_sub_part_num = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "sub_part_num", def_sub_part_num, int64_t);
    table_schema.set_def_sub_part_num(def_sub_part_num);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_level, table_schema, ObPartitionLevel);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_status, table_schema, ObIndexStatus);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_status, table_schema, ObPartitionStatus, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_schema_version, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_type, table_schema, ObIndexType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, session_id, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, table_mode, table_schema, int32_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, tablespace_id,
        table_schema, tenant_id, true, ObSchemaService::g_ignore_column_retrieve_error_, common::OB_INVALID_ID);
    ObPartitionOption &partition_option = table_schema.get_part_option();
    if (OB_SUCC(ret)) {
      ObString part_func_expr;
      ObString sub_part_func_expr;
      //part_expr
      int64_t part_num = 0;
      enum ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_HASH;
      EXTRACT_INT_FIELD_MYSQL(result, "part_func_type", part_func_type, ObPartitionFuncType);
      partition_option.set_part_func_type(part_func_type);
      EXTRACT_INT_FIELD_MYSQL(result, "part_num", part_num, int64_t);
      partition_option.set_part_num(part_num);
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_func_expr", part_func_expr);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_option.set_part_expr(part_func_expr))) {
          SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
        }
      }
      //sub_part_expr
      ObPartitionOption &sub_part_option = table_schema.get_sub_part_option();
      EXTRACT_INT_FIELD_MYSQL(result, "sub_part_func_type", part_func_type, ObPartitionFuncType);
      sub_part_option.set_part_func_type(part_func_type);
      EXTRACT_INT_FIELD_MYSQL(result, "sub_part_num", part_num, int64_t);
      sub_part_option.set_part_num(part_num);
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_func_expr", sub_part_func_expr);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sub_part_option.set_part_expr(sub_part_func_expr))) {
          SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_schema);
      const ObDuplicateScope duplicate_scope_default = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, duplicate_scope, table_schema, int64_t, true /* skip null error*/,
          ObSchemaService::g_ignore_column_retrieve_error_, duplicate_scope_default);
      ObString encryption_default("");
      ObString encryption;
      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
          result, "encryption", encryption, true,
          ObSchemaService::g_ignore_column_retrieve_error_, encryption_default);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_schema.set_encryption_str(encryption))) {
          SHARE_SCHEMA_LOG(WARN, "fail to set encryption str", K(ret), K(encryption));
        }
      }
      ignore_column_error = false;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, sub_part_template_flags, table_schema, int64_t, true /* skip null error*/,
        ignore_column_error, true);
      if (OB_SUCC(ret) && !table_schema.has_sub_part_template_def()) {
        table_schema.get_sub_part_option().set_part_num(0);
        table_schema.set_def_sub_part_num(0);
      }
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, auto_part, partition_option, true, ignore_column_error, false);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, auto_part_size, partition_option, int64_t, true, ignore_column_error, -1);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, association_table_id,
      table_schema, tenant_id, true, ignore_column_error, common::OB_INVALID_ID);

      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, max_dependency_version,
      table_schema, int64_t, true, ignore_column_error, common::OB_INVALID_VERSION);
    }

    if (OB_SUCC(ret) && table_schema.is_interval_part()) {
      ObString btransition_point;
      ObString binterval_range;
      ObString tmp_str("");
      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        result, "b_transition_point", btransition_point, true, ignore_column_error, tmp_str);

      EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        result, "b_interval_range", binterval_range, true, ignore_column_error, tmp_str);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_schema.set_transition_point_with_hex_str(btransition_point))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set transition point to partition", K(ret));
      } else if (OB_FAIL(table_schema.set_interval_range_with_hex_str(binterval_range))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set interval range to partition", K(ret));
      }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, tablet_id, table_schema, uint64_t, true, ignore_column_error, 0);
    ignore_column_error = true;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, object_status, table_schema, int64_t, true, ignore_column_error, ObObjectStatus::VALID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, truncate_version, table_schema, int64_t, true, ignore_column_error, common::OB_INVALID_VERSION);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_database_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleDatabaseSchema &database_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  database_schema.reset();
  is_deleted = false;

  database_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, database_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, database_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, default_tablegroup_id, database_schema, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, database_name, database_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_tablegroup_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleTablegroupSchema &tablegroup_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  tablegroup_schema.reset();
  is_deleted = false;
  tablegroup_schema.set_tenant_id(tenant_id);
  ObString sharding_default(OB_PARTITION_SHARDING_ADAPTIVE);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, tablegroup_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tablegroup_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tablegroup_name, tablegroup_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_status, tablegroup_schema, ObPartitionStatus, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, partition_schema_version, tablegroup_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sharding, tablegroup_schema, true, true, sharding_default);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_outline_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleOutlineSchema &outline_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  outline_schema.reset();
  is_deleted = false;

  outline_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, outline_id, outline_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, outline_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, outline_schema, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, outline_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, signature, outline_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, sql_id, outline_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, ObString::make_string(""));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_routine_schema(const uint64_t tenant_id, T &result,
    ObSimpleRoutineSchema &routine_schema, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  routine_schema.reset();
  is_deleted = false;
  routine_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, routine_id, routine_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, routine_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, routine_schema, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, routine_name, routine_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, overload, routine_schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, routine_type, routine_schema, ObRoutineType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, routine_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_udt_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleUDTSchema &udt_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  udt_schema.reset();
  is_deleted = false;
  udt_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, type_id, udt_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, udt_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, udt_schema, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, type_name, udt_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, typecode, udt_schema, ObUDTTypeCode);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, udt_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_package_schema(const uint64_t tenant_id, T &result,
    ObSimplePackageSchema &package_schema, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  package_schema.reset();
  is_deleted = false;
  package_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, package_id, package_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, package_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, package_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, package_name, package_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, type, package_schema, ObPackageType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, comp_flag, package_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_trigger_schema(const uint64_t tenant_id, T &result,
    ObSimpleTriggerSchema &trigger_schema, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  trigger_schema.reset();
  is_deleted = false;
  trigger_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, trigger_id, trigger_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, trigger_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, trigger_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, trigger_name, trigger_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_synonym_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleSynonymSchema &synonym_schema,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  synonym_schema.reset();
  is_deleted = false;

  synonym_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, synonym_id, synonym_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, synonym_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, synonym_schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, synonym_name, synonym_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, object_name, synonym_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, object_database_id, synonym_schema, tenant_id);
    bool ignore_column_error = true;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, status, synonym_schema, int64_t, true, ignore_column_error, ObObjectStatus::VALID);
  }
  return ret;
}

template<typename T>
T *ObSchemaRetrieveUtils::find_table_schema(
    const uint64_t table_id,
    ObArray<T *> &table_schema_array)
{
  T *table_schema = NULL;
  typename ObArray<T *>::iterator table_iter = table_schema_array.end();
  table_iter = std::lower_bound(table_schema_array.begin(),
      table_schema_array.end(),
      table_id,
      compare_table_id<T>);
  if (table_iter != table_schema_array.end()) {
    if (OB_ISNULL(*table_iter)) {
    } else if ((*table_iter)->get_table_id() == table_id) {
      table_schema = (*table_iter);
    }
  }
  return table_schema;
}

template<typename T, typename SCHEMA>
int ObSchemaRetrieveUtils::fill_replica_options(T &result, SCHEMA &schema)
{
  int ret = common::OB_SUCCESS;
  ObString zone_list_str;
  ObString primary_zone_str;
  ObArray<ObString> zones;

  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "primary_zone", primary_zone_str);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "zone_list", zone_list_str);
  if (OB_FAIL(schema.str2string_array(to_cstring(zone_list_str), zones))) {
    SHARE_SCHEMA_LOG(WARN, "str2string_array failed", K(zone_list_str), K(ret));
  } else {
    if (OB_FAIL(schema.set_zone_list(zones))) {
      SHARE_SCHEMA_LOG(WARN, "set_zone_list failed", K(ret));
    } else if (OB_FAIL(schema.set_primary_zone(primary_zone_str))) {
      SHARE_SCHEMA_LOG(WARN, "set_primary_zone failed", K(ret));
    } else if (!ObPrimaryZoneUtil::no_need_to_check_primary_zone(schema.get_primary_zone())) {
      ObPrimaryZoneUtil primary_zone_util(schema.get_primary_zone());
      if (OB_FAIL(primary_zone_util.init())) {
        SHARE_SCHEMA_LOG(WARN, "fail to init primary zone util", K(ret));
      } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
        SHARE_SCHEMA_LOG(WARN, "fail to check and parse primary zone", K(ret));
      } else if (OB_FAIL(schema.set_primary_zone_array(primary_zone_util.get_zone_array()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set primary zone array", K(ret));
      } else {} // set primary zone array success
    } else {} // empty primary zone, no need to check and parse
  }
  return ret;
}

template<typename T>
bool ObSchemaRetrieveUtils::compare_table_id(
    const T *table_schema,
    const uint64_t table_id)
{
  bool cmp = false;
  if (OB_ISNULL(table_schema)) {
    SHARE_SCHEMA_LOG_RET(WARN, OB_ERR_UNEXPECTED, "table schema is NULL");
  } else {
    //order by table id desc, used in sort function, the tenant_id is desc too
    cmp = table_schema->get_table_id() > table_id;
  }
  return cmp;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_part_info(
    const uint64_t tenant_id, const bool check_deleted, T &result,
    ObPartition &partition, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = false;
  bool is_subpart_template = false;
  if (OB_FAIL(fill_base_part_info(tenant_id, check_deleted, is_subpart_def,
                                  is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  } else if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, part_name, partition);
    const int64_t default_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, part_idx, partition, int64_t,
        true, ObSchemaService::g_ignore_column_retrieve_error_, default_part_idx);

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_num, partition, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, partition_type, partition, PartitionType, true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        share::schema::PartitionType::PARTITION_TYPE_NORMAL);
  }
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(TRACE, "retrieve partition info", KR(ret), K(is_deleted), K(partition));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_def_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObSubPartition &partition, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = true;
  bool is_subpart_template = true;
  if (OB_FAIL(fill_base_part_info(tenant_id, check_deleted, is_subpart_def,
                                  is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  } else if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_id, partition, int64_t);
    const int64_t default_sub_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sub_part_idx, partition, int64_t,
        true, ObSchemaService::g_ignore_column_retrieve_error_, default_sub_part_idx);
    ObString sub_part_name;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_name", sub_part_name);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.set_part_name(sub_part_name))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set part name", K(ret));
      }
    }
  } else { }//do nothing
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(TRACE, "retrieve def subpartition info", KR(ret), K(is_deleted), K(partition));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_subpart_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    T &result,
    ObSubPartition &partition, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = true;
  bool is_subpart_template = false;
  if (OB_FAIL(fill_base_part_info(tenant_id, check_deleted, is_subpart_def,
                                  is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  }
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_id, partition, int64_t);
  if (OB_SUCC(ret) && !is_deleted) {
    const int64_t default_sub_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, sub_part_idx, partition, int64_t,
        true, ObSchemaService::g_ignore_column_retrieve_error_, default_sub_part_idx);
    ObString sub_part_name;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_name", sub_part_name);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.set_part_name(sub_part_name))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set part name", K(ret));
      }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, partition_type, partition, PartitionType, true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        share::schema::PartitionType::PARTITION_TYPE_NORMAL);
  } else { }//do nothing
  if (OB_SUCC(ret)) {
    SHARE_SCHEMA_LOG(TRACE, "retrieve subpartition info", KR(ret), K(is_deleted), K(partition));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_base_part_info(
    const uint64_t tenant_id,
    const bool check_deleted,
    const bool is_subpart_def,
    const bool is_subpart_template,
    T &result,
    ObBasePartition &partition,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  is_deleted = false;

  partition.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, partition, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, tablespace_id,
      partition, tenant_id, true, ObSchemaService::g_ignore_column_retrieve_error_, common::OB_INVALID_ID);
  if (OB_FAIL(ret)) {
  } else if (!is_subpart_def && is_subpart_template) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid arg", K(ret), K(is_subpart_def), K(is_subpart_template));
  } else if (is_subpart_def && is_subpart_template) {
    //for subpart template, we set it's part_id explicitly
    partition.set_part_id(ObSubPartition::TEMPLATE_PART_ID);
  } else {
    // part level one or part level two (non-template)
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_id, partition, int64_t);
  }

  if (OB_SUCC(ret) && check_deleted) {
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  }
  if (OB_SUCC(ret) && !is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, partition, int64_t);
    ObString bhigh_bound_val;
    ObString blist_val;

    if (OB_SUCC(ret)) {
      ret = result.get_varchar("b_high_bound_val", bhigh_bound_val);
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
        //do nothing
      } else if (OB_ERR_COLUMN_NOT_FOUND == ret && ObSchemaService::g_ignore_column_retrieve_error_) {
        // ignore error that column is not exist when schema service runs in liboblog
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        SQL_LOG(WARN, "fail to get varchar. ", K(ret));
      } else if (OB_FAIL(partition.set_high_bound_val_with_hex_str(bhigh_bound_val))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set high bound val to partition", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ret = result.get_varchar("b_list_val", blist_val);
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
        //do nothing
      } else if (OB_ERR_COLUMN_NOT_FOUND == ret && ObSchemaService::g_ignore_column_retrieve_error_) {
        // ignore error that column is not exist when schema service runs in liboblog
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        SQL_LOG(WARN, "fail to get varchar column 'b_list_val' of base_part_info.", K(ret));
      } else {
        // bugfix: issue/48579037
        // In 4.x, tablegroup_id/table_id is in the same scope, so we can distinguish table and tablegroup based on object_id.
        bool is_oracle_mode = false;
        const uint64_t table_id = partition.get_table_id();
        if (is_sys_tablegroup_id(table_id)) {
          is_oracle_mode = false;
        } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                   tenant_id, table_id, is_oracle_mode))) {
          LOG_WARN("fail to check oracle mode", KR(ret), K(tenant_id), K(table_id));
        }
        lib::CompatModeGuard guard(is_oracle_mode ?
                                   lib::Worker::CompatMode::ORACLE :
                                   lib::Worker::CompatMode::MYSQL);
        if (FAILEDx(partition.set_list_vector_values_with_hex_str(blist_val))) {
          SHARE_SCHEMA_LOG(WARN, "Failed to set list val to partition", K(ret));
        }
      }
    }
    bool ignore_column_error = ObSchemaService::g_ignore_column_retrieve_error_;
    if (OB_SUCC(ret) && !(is_subpart_def && is_subpart_template)) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, tablet_id, partition, uint64_t, true, ignore_column_error, 0);
    }
  } else { }//do nothing
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_aux_tables(
    const uint64_t tenant_id,
    T &result,
    ObIArray<ObAuxTableMetaInfo> &aux_tables)
{
  int ret = common::OB_SUCCESS;

  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    uint64_t table_id = OB_INVALID_ID;
    ObTableType table_type = MAX_TABLE_TYPE;
    ObIndexType index_type = INDEX_TYPE_MAX;

    EXTRACT_INT_FIELD_MYSQL(result, "table_type", table_type, ObTableType);

    if (USER_INDEX == table_type
        || AUX_VERTIAL_PARTITION_TABLE == table_type
        || AUX_LOB_META == table_type
        || AUX_LOB_PIECE == table_type) {

      EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "table_id", table_id, tenant_id);
      EXTRACT_INT_FIELD_MYSQL(result, "index_type", index_type, ObIndexType);

      ObAuxTableMetaInfo aux_table_meta(table_id, table_type, index_type);
      if (FAILEDx(aux_tables.push_back(aux_table_meta))) {
        SHARE_SCHEMA_LOG(WARN, "fail to push back aux table", KR(ret), K(aux_table_meta));
      }

      SHARE_SCHEMA_LOG(TRACE, "dump aux table", K(aux_table_meta), K(table_type), K(index_type));
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get aux table. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(TRACE, "retrieve aux table finish", K(tenant_id), K(aux_tables));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_schema_version(
    T &result, VersionHisVal &version_his_val)
{
  int ret = common::OB_SUCCESS;
  int &row_idx = version_his_val.valid_cnt_;
  row_idx = 0;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    if (row_idx >= MAX_CACHED_VERSION_CNT) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "row count is more than", LITERAL_K(MAX_CACHED_VERSION_CNT), K(ret));
    } else {
      if (0 == row_idx) {
        EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", version_his_val.is_deleted_, bool);
        EXTRACT_INT_FIELD_MYSQL(result, "min_version", version_his_val.min_version_, int64_t);
      }
      EXTRACT_INT_FIELD_MYSQL(result, "schema_version", version_his_val.versions_[row_idx++], int64_t);
    }
  }

  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all schema version. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve schema version", K(version_his_val));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_mock_fk_parent_table_column_info(const uint64_t tenant_id,
    T &result, uint64_t &parent_column_id, ObString &parent_column_name, bool &is_deleted)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  parent_column_id = OB_INVALID_ID;
  parent_column_name.reset();
  is_deleted = false;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "parent_column_id", parent_column_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "parent_column_name", parent_column_name);
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_mock_fk_parent_table_schema_column(
    const uint64_t tenant_id, T &result, ObMockFKParentTableSchema &mock_fk_parent_table)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_parent_column_id = OB_INVALID_ID;
  uint64_t parent_column_id = OB_INVALID_ID;
  ObString parent_column_name;
  mock_fk_parent_table.reset_column_array();
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_mock_fk_parent_table_column_info(
                tenant_id, result, parent_column_id, parent_column_name, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill mock_fk_parent_table_column_info", K(ret));
    } else if (prev_parent_column_id == parent_column_id) {
      // skip
    } else if (is_deleted) {
      // skip
    } else if (OB_FAIL(mock_fk_parent_table.add_column_info_to_column_array(
                       std::pair<uint64_t, common::ObString>(parent_column_id, parent_column_name)))) {
      SHARE_SCHEMA_LOG(WARN, "fail to push back child_column_id", K(ret));
    }
    prev_parent_column_id = parent_column_id;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get column info. iter quit.", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve mock_fk_parent_table_schema_column", K(mock_fk_parent_table));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_foreign_key_info(
    const uint64_t tenant_id,
    T &result,
    TABLE_SCHEMA &table_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObForeignKeyInfo foreign_key_info(&allocator);
  uint64_t prev_foreign_key_id = common::OB_INVALID_ID;
  bool is_deleted = false;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    foreign_key_info.reset();
    allocator.reuse();
    if (OB_FAIL(fill_foreign_key_info(tenant_id, table_schema.get_table_id(), result, foreign_key_info, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill foreign key info", K(ret));
    } else if (foreign_key_info.foreign_key_id_ == prev_foreign_key_id) {
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO,"foreign key is is_deleted",
                       "table_id", table_schema.get_table_id(),
                       "foreign_key_id", foreign_key_info.foreign_key_id_);
    } else if (OB_FAIL(table_schema.add_foreign_key_info(foreign_key_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add foreign key info", K(ret), K(foreign_key_info));
    }
    prev_foreign_key_id = foreign_key_info.foreign_key_id_;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get foreign key schema. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(TRACE, "retrieve foreign key schema", K(table_schema));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_foreign_key_column_info(
    const uint64_t tenant_id,
    T &result,
    ObForeignKeyInfo &foreign_key_info)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  int64_t prev_child_column_id = 0; // OB_INVALID_ID;
  int64_t prev_parent_column_id = 0;
  int64_t child_column_id = 0;
  int64_t parent_column_id = 0;
  foreign_key_info.child_column_ids_.reset();
  foreign_key_info.parent_column_ids_.reset();
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_foreign_key_column_info(tenant_id, result, child_column_id, parent_column_id, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill foreign key info", K(ret));
    } else if (prev_child_column_id == child_column_id && prev_parent_column_id == parent_column_id) {
      // skip
    } else if (is_deleted) {
      // skip
    } else if (OB_FAIL(foreign_key_info.child_column_ids_.push_back(child_column_id))) {
      SHARE_SCHEMA_LOG(WARN, "fail to push back child_column_id", K(ret));
    } else if (OB_FAIL(foreign_key_info.parent_column_ids_.push_back(parent_column_id))) {
      SHARE_SCHEMA_LOG(WARN, "fail to push back parent_column_id", K(ret));
    }
    prev_child_column_id = child_column_id;
    prev_parent_column_id = parent_column_id;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get foreing key. iter quit.", K(ret));
  } else {
    SHARE_SCHEMA_LOG(TRACE, "retrieve foreign key", K(foreign_key_info));
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_foreign_key_info(
    const uint64_t tenant_id,
    uint64_t table_id,
    T &result,
    ObForeignKeyInfo &foreign_key_info,
    bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  foreign_key_info.table_id_ = table_id;
  is_deleted = false;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, foreign_key_id, foreign_key_info, tenant_id);
  if (OB_SUCC(ret)) {
    if (is_deleted) {
      // do nothing since fk is deleted
    } else {
      const bool default_rely_flag = false;
      const bool default_enable_flag = true;
      const bool default_is_parent_table_mock = false;
      const ObCstFkValidateFlag default_validate_flag = CST_FK_VALIDATED;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, child_table_id, foreign_key_info, tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, parent_table_id, foreign_key_info, tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, update_action, foreign_key_info, ObReferenceAction);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, delete_action, foreign_key_info, ObReferenceAction);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, foreign_key_name, foreign_key_info);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, rely_flag, foreign_key_info, true, ObSchemaService::g_ignore_column_retrieve_error_, default_rely_flag);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, enable_flag, foreign_key_info, true, ObSchemaService::g_ignore_column_retrieve_error_, default_enable_flag);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, validate_flag, foreign_key_info, ObCstFkValidateFlag, true, ObSchemaService::g_ignore_column_retrieve_error_, default_validate_flag);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, ref_cst_type, foreign_key_info, ObConstraintType, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, ref_cst_id, foreign_key_info, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, -1);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, is_parent_table_mock, foreign_key_info, true, ObSchemaService::g_ignore_column_retrieve_error_, default_is_parent_table_mock);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, name_generated_type, foreign_key_info, ObNameGeneratedType, true/*skip null*/, true/*ignore column error*/, GENERATED_TYPE_UNKNOWN);
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_foreign_key_column_info(
    const uint64_t tenant_id,
    T &result,
    int64_t &child_column_id,
    int64_t &parent_column_id,
    bool &is_deleted)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  child_column_id = 0;
  parent_column_id = 0;
  is_deleted = false;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result.get_int("child_column_id", child_column_id))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get child_column_id in row", K(ret));
    } else if (OB_FAIL(result.get_int("parent_column_id", parent_column_id))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get parent_column_id in row", K(ret));
    }
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_simple_foreign_key_info(
    const uint64_t tenant_id,
    T &result,
    ObArray<ObSimpleTableSchemaV2 *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_foreign_key_id = common::OB_INVALID_ID;
  uint64_t fk_id = common::OB_INVALID_ID;
  uint64_t table_id = common::OB_INVALID_ID;
  ObString fk_name;
  ObSimpleTableSchemaV2 *table_schema_ptr = nullptr;

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    is_deleted = false;
    table_schema_ptr = nullptr;
    fk_id = common::OB_INVALID_ID;
    fk_name.reset();
    if (OB_FAIL(get_foreign_key_id_and_name(tenant_id, result, is_deleted, fk_id, fk_name, table_id))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get foreign key id and name", K(ret));
    } else if (fk_id == prev_foreign_key_id) {
      ret = OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO,"foreign key is deleted", K(table_id), K(fk_id));
    } else if (table_id == common::OB_INVALID_ID || OB_ISNULL(table_schema_ptr = ObSchemaRetrieveUtils::find_table_schema(table_id, table_schema_array))) {
      SHARE_SCHEMA_LOG(WARN,"fail to find table schema by table id", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema_ptr->add_simple_foreign_key_info(
                                          table_schema_ptr->get_tenant_id(),
                                          table_schema_ptr->get_database_id(),
                                          table_schema_ptr->get_table_id(),
                                          fk_id, fk_name))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add simple foreign key info", K(ret), K(fk_id), K(fk_name));
    }
    prev_foreign_key_id = fk_id;
  }
  if (ret != OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get simple foreign key info. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve simple foreign key info", K(tenant_id));
    ret = OB_SUCCESS;
  }

  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_simple_encrypt_info(
    const uint64_t tenant_id,
    T &result,
    ObArray<S *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_tablespace_id = common::OB_INVALID_ID;
  uint64_t master_key_id = common::OB_INVALID_ID;
  uint64_t tablespace_id = common::OB_INVALID_ID;
  ObString encrypt_key;

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    is_deleted = false;
    master_key_id = common::OB_INVALID_ID;
    tablespace_id = common::OB_INVALID_ID;
    encrypt_key.reset();
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "tablespace_id", tablespace_id, tenant_id);
    EXTRACT_UINT_FIELD_MYSQL(result, "master_key_id", master_key_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "encrypt_key", encrypt_key);
    if (OB_FAIL(ret)) {
     /*nothing */
    } else if (tablespace_id == prev_tablespace_id) {
      continue;
    } else if (!is_deleted) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_array.count(); ++i) {
        if (tablespace_id == table_schema_array.at(i)->get_tablespace_id()) {
          table_schema_array.at(i)->set_master_key_id(master_key_id);
          table_schema_array.at(i)->set_encrypt_key(encrypt_key);
        }
      }
      prev_tablespace_id = tablespace_id;
    }
  }
  if (ret != OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get simple encrypt info. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve simple encrypt info", K(master_key_id), K(encrypt_key));
    ret = OB_SUCCESS;
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::get_foreign_key_id_and_name(const uint64_t tenant_id,
                                                       T &result,
                                                       bool &is_deleted,
                                                       uint64_t &fk_id,
                                                       ObString &fk_name,
                                                       uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  is_deleted = false;

  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "fail to extract is_deleted", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "foreign_key_id", fk_id, tenant_id);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "fail to extract foreign key id for simple foreign key info", K(ret));
    } else {
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "foreign_key_name", fk_name);
      if (OB_FAIL(ret)) {
        SHARE_SCHEMA_LOG(WARN, "fail to extract foreign key name for simple foreign key info", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "child_table_id", table_id, tenant_id);
        if (OB_FAIL(ret)) {
          SHARE_SCHEMA_LOG(WARN, "fail to extract child table id for simple foreign key info", K(ret));
        }
      }
    }
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_simple_constraint_info(const uint64_t tenant_id, T &result, ObArray<ObSimpleTableSchemaV2 *> &table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_table_id = common::OB_INVALID_ID;
  uint64_t prev_constraint_id = common::OB_INVALID_ID;
  uint64_t cst_id = common::OB_INVALID_ID;
  uint64_t table_id = common::OB_INVALID_ID;
  ObString cst_name;
  ObConstraintType cst_type = CONSTRAINT_TYPE_INVALID;
  ObSimpleTableSchemaV2 *table_schema_ptr = nullptr;

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    is_deleted = false;
    table_schema_ptr = nullptr;
    cst_id = common::OB_INVALID_ID;
    cst_name.reset();
    cst_type = CONSTRAINT_TYPE_INVALID;
    if (OB_FAIL(get_constraint_id_and_name(tenant_id, result, is_deleted, cst_id, cst_name, table_id, cst_type))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get constraint id and name", K(ret));
    } else if (table_id == prev_table_id && cst_id == prev_constraint_id) {
      ret = OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO,"constraint is deleted", K(table_id), K(cst_id));
    } else if (table_id == common::OB_INVALID_ID || OB_ISNULL(table_schema_ptr = ObSchemaRetrieveUtils::find_table_schema(table_id, table_schema_array))) {
      SHARE_SCHEMA_LOG(WARN,"fail to find table schema by table id", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema_ptr->add_simple_constraint_info(table_schema_ptr->get_tenant_id(),
                                                                    table_schema_ptr->get_database_id(),
                                                                    table_schema_ptr->get_table_id(),
                                                                    cst_id,
                                                                    cst_name))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add simple constraint info", K(ret), K(table_schema_ptr->get_tenant_id()),
                                                                   K(table_schema_ptr->get_database_id()),
                                                                   K(table_schema_ptr->get_table_id()),
                                                                   K(cst_id),
                                                                   K(cst_name));
    }
    prev_table_id = table_id;
    prev_constraint_id = cst_id;
  }
  if (ret != OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get simple constraint info. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve simple constraint info", K(tenant_id));
    ret = OB_SUCCESS;
  }

  return ret;
}


template<typename T>
int ObSchemaRetrieveUtils::get_constraint_id_and_name(const uint64_t tenant_id,
                                                      T &result,
                                                      bool &is_deleted,
                                                      uint64_t &cst_id,
                                                      ObString &cst_name,
                                                      uint64_t &table_id,
                                                      ObConstraintType &cst_type)
{
  int ret = OB_SUCCESS;
  is_deleted = false;

  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "constraint_id", cst_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "table_id", table_id, tenant_id);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "constraint_name", cst_name);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "constraint_type", cst_type, ObConstraintType);
  }

  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::retrieve_drop_tenant_infos(
    T &result,
    ObIArray<ObDropTenantInfo> &drop_tenant_infos)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_tenant_id = common::OB_INVALID_ID;
  ObDropTenantInfo drop_tenant_info;
  drop_tenant_infos.reset();
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    drop_tenant_info.reset();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, tenant_id, drop_tenant_info, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, drop_tenant_info, int64_t);
    if (OB_FAIL(ret)) {
    } else if (drop_tenant_info.get_tenant_id() == prev_tenant_id) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "duplicate drop tenant info", K(ret), K(drop_tenant_info), K(prev_tenant_id));
    } else if (OB_FAIL(drop_tenant_infos.push_back(drop_tenant_info))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret), K(drop_tenant_info));
    } else {
      prev_tenant_id = drop_tenant_info.get_tenant_id();
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all drop tenant infos. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_keystore_schema(
    const uint64_t tenant_id,
    T &result,
    ObKeystoreSchema &keystore_schema,
    bool &is_deleted)
{
  keystore_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  keystore_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, keystore_id, keystore_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, status, keystore_schema, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, master_key_id, keystore_schema, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, keystore_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, keystore_name, keystore_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, password, keystore_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, master_key, keystore_schema);
    ObString empty_str("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, encrypted_key, keystore_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, empty_str);
  }
  return ret;
}
template<typename T>
int ObSchemaRetrieveUtils::fill_tablespace_schema(
    const uint64_t tenant_id,
    T &result,
    ObTablespaceSchema &tablespace_schema,
    bool &is_deleted)
{
  tablespace_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  tablespace_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablespace_id, tablespace_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tablespace_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tablespace_name, tablespace_schema);
    bool ignore_column_error = false;
    ObString empty_str("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, encryption_name, tablespace_schema, true, ignore_column_error, empty_str);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, encrypt_key, tablespace_schema, true, ignore_column_error, empty_str);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, master_key_id,
        tablespace_schema, uint64_t, true, ignore_column_error, common::OB_INVALID_ID);
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_profile_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_profile_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill profile schema ", K(ret));
    } else if (schema.get_profile_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_profile_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "profile is is_deleted, don't add",
               "profile_id", schema.get_profile_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve profile schema succeed", K(schema));
    }
    prev_id = schema.get_profile_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all profile schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve profile schemas succeed", K(tenant_id));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_profile_schema(
    const uint64_t tenant_id,
    T &result,
    ObProfileSchema &profile_schema,
    bool &is_deleted)
{
  profile_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  profile_schema.set_tenant_id(tenant_id);
  ObString default_password_verify_function("");
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, profile_id, profile_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, profile_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, profile_name, profile_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, failed_login_attempts, profile_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, password_lock_time, profile_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
      result, password_verify_function, profile_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, default_password_verify_function);
    bool ignore_column_error = false;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, password_life_time, profile_schema,
        int64_t, false, ignore_column_error, INT64_MAX);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, password_grace_time, profile_schema,
        int64_t, false, ignore_column_error, INT64_MAX);
  }
  return ret;
}

template<typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_directory_schema(
    const uint64_t tenant_id,
    T &result,
    ObIArray<S> &schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  S schema(&allocator);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    schema.reset();
    allocator.reuse();
    bool is_deleted = false;
    if (OB_FAIL(fill_directory_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill directory schema ", K(ret));
    } else if (schema.get_directory_id() == prev_id) {
      SHARE_SCHEMA_LOG(DEBUG, "hualong debug ignore", "id", schema.get_directory_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "directory is is_deleted, don't add",
               "directory_id", schema.get_directory_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve directory schema succeed", K(schema));
    }
    prev_id = schema.get_directory_id();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all directory schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve directory schemas succeed", K(tenant_id));
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_directory_schema(
    const uint64_t tenant_id,
    T &result,
    ObDirectorySchema &directory_schema,
    bool &is_deleted)
{
  directory_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  directory_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, directory_id, directory_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, directory_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, directory_name, directory_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, directory_path, directory_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_context_schema(
    const uint64_t tenant_id,
    T &result,
    ObContextSchema &context_schema,
    bool &is_deleted)
{
  context_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  context_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, context_id, context_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, context_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, namespace, context_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_VALUE_MYSQL(result, database_name, schema_name, context_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_VALUE_MYSQL(result, package, trusted_package, context_schema);
    EXTRACT_INT_FIELD_TO_CLASS_VALUE_MYSQL(result, type, context_type, context_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_VALUE_MYSQL(result, tracking, is_tracking, context_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, origin_con_id, context_schema, int64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_mock_fk_parent_table_schema(
    const uint64_t tenant_id,
    T &result,
    ObSimpleMockFKParentTableSchema &mock_fk_parent_table_schema,
    bool &is_deleted)
{
  mock_fk_parent_table_schema.reset();
  is_deleted  = false;
  int ret = common::OB_SUCCESS;
  mock_fk_parent_table_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, mock_fk_parent_table_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, mock_fk_parent_table_id, mock_fk_parent_table_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, mock_fk_parent_table_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, mock_fk_parent_table_name, mock_fk_parent_table_schema);
  }
  return ret;
}

int ObSchemaRetrieveUtils::fill_sys_table_lob_tid(ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  const int64_t table_id = table.get_table_id();
  uint64_t lob_meta_table_id = OB_INVALID_ID;
  uint64_t lob_piece_table_id = OB_INVALID_ID;
  if (is_system_table(table_id)) {
    if (OB_ALL_CORE_TABLE_TID == table_id) {
      // do nothing
    } else if (!get_sys_table_lob_aux_table_id(table_id, lob_meta_table_id, lob_piece_table_id)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("get lob aux table id failed.", K(ret), K(table_id));
    } else {
      table.set_aux_lob_meta_tid(lob_meta_table_id);
      table.set_aux_lob_piece_tid(lob_piece_table_id);
    }
  }
  return ret;
}

RETRIEVE_SCHEMA_FUNC_DEFINE(rls_policy);
RETRIEVE_SCHEMA_FUNC_DEFINE(rls_group);
RETRIEVE_SCHEMA_FUNC_DEFINE(rls_context);

template<typename T>
int ObSchemaRetrieveUtils::retrieve_rls_column_schema(
    const uint64_t tenant_id,
    T &result,
    ObArray<ObRlsPolicySchema *> &rls_policy_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_policy_id = common::OB_INVALID_ID;
  uint64_t prev_column_id = common::OB_INVALID_ID;
  ObRlsPolicySchema *rls_policy_schema_ptr = NULL;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObRlsSecColumnSchema rls_column_schema(&allocator);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    is_deleted = false;
    rls_column_schema.reset();
    allocator.reuse();
    if (OB_FAIL(fill_rls_column_schema(tenant_id, result, rls_column_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill rls_column schema", K(ret));
    } else if (rls_column_schema.get_rls_policy_id() == prev_policy_id
               && rls_column_schema.get_column_id() == prev_column_id) {
      SHARE_SCHEMA_LOG(DEBUG, "ignore", "policy_id", rls_column_schema.get_rls_policy_id(),
                        "column_id", rls_column_schema.get_column_id(),
                        "version", rls_column_schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "rls_column is is_deleted, don't add",
                       "rls_policy_id", rls_column_schema.get_rls_policy_id(),
                       "column_id", rls_column_schema.get_column_id());
    } else if (OB_FAIL(find_rls_policy_schema(rls_column_schema.get_rls_policy_id(),
        rls_policy_array, rls_policy_schema_ptr))) {
      SHARE_SCHEMA_LOG(WARN, "failed to find rls_policy schema", K(tenant_id), "rls_policy_id",
                             rls_column_schema.get_rls_policy_id(), K(ret));
    } else if (OB_ISNULL(rls_policy_schema_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else if (OB_FAIL(rls_policy_schema_ptr->add_sec_column(rls_column_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "retrieve rls_column schema succeed", K(rls_column_schema));
    }
    prev_policy_id = rls_column_schema.get_rls_policy_id();
    prev_column_id = rls_column_schema.get_column_id();
  }
  if (ret != OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get rls column schema. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(TRACE, "retrieve rls column schema", K(tenant_id));
    ret = OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::find_rls_policy_schema(
    const uint64_t rls_policy_id,
    ObArray<T *> rls_policy_schema_array,
    T *&rls_policy_schema)
{
  int ret = OB_SUCCESS;
  typename ObArray<T *>::iterator iter = rls_policy_schema_array.end();
  iter = std::lower_bound(rls_policy_schema_array.begin(),
      rls_policy_schema_array.end(),
      rls_policy_id,
      compare_rls_policy_id<T>);
  if (iter != rls_policy_schema_array.end()) {
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "fail to get rls policy schema", K(ret));
    } else if ((*iter)->get_rls_policy_id() == rls_policy_id) {
      rls_policy_schema = (*iter);
    }
  }
  return ret;
}

template<typename T>
bool ObSchemaRetrieveUtils::compare_rls_policy_id(
    const T *rls_policy_schema,
    const uint64_t rls_policy_id)
{
  bool cmp = false;
  if (OB_ISNULL(rls_policy_schema)) {
    SHARE_SCHEMA_LOG_RET(WARN, OB_ERR_UNEXPECTED, "rls_policy schema is NULL");
  } else {
    cmp = rls_policy_schema->get_rls_policy_id() > rls_policy_id;
  }
  return cmp;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_rls_policy_schema(
    const uint64_t tenant_id,
    T &result,
    ObRlsPolicySchema &rls_policy_schema,
    bool &is_deleted)
{
  rls_policy_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  rls_policy_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rls_policy_id, rls_policy_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, rls_policy_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_name, rls_policy_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_id, rls_policy_schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rls_group_id, rls_policy_schema, uint64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, stmt_type, rls_policy_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, check_opt, rls_policy_schema, bool);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, enable_flag, rls_policy_schema, bool);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_function_schema, rls_policy_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_package_name, rls_policy_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_function_name, rls_policy_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_rls_group_schema(
    const uint64_t tenant_id,
    T &result,
    ObRlsGroupSchema &rls_group_schema,
    bool &is_deleted)
{
  rls_group_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  rls_group_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rls_group_id, rls_group_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, rls_group_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, policy_group_name, rls_group_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_id, rls_group_schema, uint64_t);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_rls_context_schema(
    const uint64_t tenant_id,
    T &result,
    ObRlsContextSchema &rls_context_schema,
    bool &is_deleted)
{
  rls_context_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  rls_context_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rls_context_id, rls_context_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, rls_context_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_id, rls_context_schema, uint64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, context_name, rls_context_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, attribute, rls_context_schema);
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_rls_column_schema(
    const uint64_t tenant_id,
    T &result,
    ObRlsSecColumnSchema &rls_column_schema,
    bool &is_deleted)
{
  rls_column_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  rls_column_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rls_policy_id, rls_column_schema, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, rls_column_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, rls_column_schema, int64_t);
  }
  return ret;
}


template<typename T>
int ObSchemaRetrieveUtils::retrieve_object_list(
    const uint64_t tenant_id,
    T &result,
    ObIArray<uint64_t> &object_list)
{
  int ret = common::OB_SUCCESS;
  uint64_t pre_object_id = common::OB_INVALID_ID;
  uint64_t object_id = common::OB_INVALID_ID;
  bool is_deleted = false;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    if (OB_FAIL(fill_object_id(tenant_id, result, object_id, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill object id failed", K(ret));
    } else if (pre_object_id == object_id) {
      // ignore
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "object is deleted", K(object_id));
    } else if (OB_FAIL(object_list.push_back(object_id))) {
      SHARE_SCHEMA_LOG(WARN, "add object id failed", K(pre_object_id), K(object_id), K(ret));
    } else {
      SHARE_SCHEMA_LOG(TRACE, "retrieve object id succeed", K(object_id));
    }
    pre_object_id = object_id;
    object_id = common::OB_INVALID_ID;
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template<typename T>
int ObSchemaRetrieveUtils::fill_object_id(const uint64_t tenant_id, T &result,
                                          uint64_t &object_id, bool &is_deleted)
{
  int ret = common::OB_SUCCESS;
  EXTRACT_INT_FIELD_MYSQL(result, "object_id", object_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_table_latest_schema_versions(
    T &result,
    ObIArray<ObTableLatestSchemaVersion> &table_schema_versions)
{
  int ret = common::OB_SUCCESS;
  ObTableLatestSchemaVersion table_schema_version;

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    table_schema_version.reset();
    uint64_t table_id = common::OB_INVALID_ID;
    int64_t schema_version = common::OB_INVALID_VERSION;
    bool is_deleted = false;

    EXTRACT_INT_FIELD_MYSQL(result, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", schema_version, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);

    if (FAILEDx(table_schema_version.init(table_id, schema_version, is_deleted))) {
      LOG_WARN("init failed", KR(ret), K(table_id), K(schema_version), K(is_deleted));
    } else if (OB_FAIL(table_schema_versions.push_back(table_schema_version))) {
      LOG_WARN("push back failed", KR(ret), K(table_schema_version), K(table_schema_versions));
    }
  }
  if (ret == common::OB_ITER_END) {
    ret = common::OB_SUCCESS;
  } else {
    SHARE_SCHEMA_LOG(WARN, "fail to get all table latest schema version", KR(ret));
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
