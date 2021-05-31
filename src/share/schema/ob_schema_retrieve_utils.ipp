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
#include "rootserver/ob_locality_util.h"

namespace oceanbase {
namespace share {
namespace schema {

#define EXTRACT_PRIV_FROM_MYSQL_RESULT(result, column_name, obj, priv_type)                    \
  if (OB_SUCC(ret)) {                                                                          \
    int64_t int_value = 0;                                                                     \
    if (OB_FAIL((result).get_int(#column_name, int_value))) {                                  \
      SHARE_SCHEMA_LOG(WARN, "Fail to get privilege in row", "priv_name", #priv_type, K(ret)); \
    } else {                                                                                   \
      1 == int_value ? (obj).set_priv(OB_##priv_type) : (void)0;                               \
    }                                                                                          \
  }

#define EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result, column_name, obj, priv_type, skip_column_error) \
  if (OB_SUCC(ret)) {                                                                                 \
    int64_t priv_col_value = 0;                                                                       \
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(                                                       \
        result, #column_name, priv_col_value, int64_t, false, skip_column_error, 0)                   \
    if (OB_SUCC(ret)) {                                                                               \
      1 == priv_col_value ? (obj).set_priv(OB_##priv_type) : (void)0;                                 \
    }                                                                                                 \
  }

#define EXTRACT_PRIV_FIELD_FROM_MYSQL_RESULT(result, column_name, priv_set, priv_type)         \
  if (OB_SUCC(ret)) {                                                                          \
    int64_t int_value = 0;                                                                     \
    if (OB_FAIL((result).get_int(#column_name, int_value))) {                                  \
      SHARE_SCHEMA_LOG(WARN, "Fail to get privilege in row", "priv_name", #priv_type, K(ret)); \
    } else if (1 == int_value) {                                                               \
      priv_set |= OB_##priv_type;                                                              \
    }                                                                                          \
  }

/*********************************************************************
 *
 * for full schemas
 *
 *********************************************************************/

template <typename T>
int ObSchemaRetrieveUtils::retrieve_table_schema(const uint64_t tenant_id, const bool check_deleted, T& result,
    ObIAllocator& allocator, ObIArray<ObTableSchema*>& table_schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_table_id = common::OB_INVALID_ID;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    ObTableSchema* allocated_table_schema = NULL;
    ObTableSchema table_schema;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_table_schema(tenant_id, check_deleted, result, table_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill table schema. ", K(check_deleted), K(ret));
    } else if (table_schema.get_table_id() == prev_table_id) {
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO,
          "table is is_deleted, don't add to table_schema_array",
          "table_id",
          table_schema.get_table_id(),
          "table_name",
          table_schema.get_table_name(),
          "schema_version",
          table_schema.get_schema_version());
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, table_schema, allocated_table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "alloc_table_schema failed", K(ret));
    } else if (OB_FAIL(table_schema_array.push_back(allocated_table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));

      // free table schema allocated
      allocator.free(allocated_table_schema);
      allocated_table_schema = NULL;
    }
    SHARE_SCHEMA_LOG(INFO, "retrieve table schema", K(table_schema), K(is_deleted), K(ret));
    prev_table_id = table_schema.get_table_id();
    table_schema.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all table schema. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve table schema");
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  uint64_t last_table_id = OB_INVALID_ID;
  uint64_t last_schema_id = common::OB_INVALID_ID;
  // store current_schema and last_schema
  bool is_last_deleted = false;
  SCHEMA* last_schema = NULL;
  ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA> helper(table_schema_array);
  while (OB_SUCC(ret) && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    SCHEMA& current = helper.get_current();
    current.reset();
    if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, result, current, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill schema failed", K(ret));
    } else if (current.get_table_id() == last_table_id && helper.get_curr_schema_id() == last_schema_id) {
      // the same with last schema, continue;
      ret = common::OB_SUCCESS;
    } else {
      if (NULL == last_schema || is_last_deleted) {
        // LAST schema IS INVALID, IGNORE
      } else if (OB_FAIL(helper.add(*last_schema))) {  // add last schema
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
    // save current column to last, rotate
    last_schema = &current;
    is_last_deleted = is_deleted;
    last_table_id = current.get_table_id();
    last_schema_id = helper.get_curr_schema_id();
    helper.rotate();  // rotate
  }
  if (OB_ITER_END != ret) {
    SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    // add last schema
    if (NULL != last_schema && !is_last_deleted) {
      if (OB_FAIL(helper.add(*last_schema))) {
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_column_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<ObTableSchema*>& table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(
          (retrieve_schema<ObTableSchema, ObColumnSchemaV2>(tenant_id, check_deleted, result, table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve column schema failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_constraint(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<ObTableSchema*>& table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObConstraint>(tenant_id, check_deleted, result, table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve constraint schema failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_part_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<TABLE_SCHEMA, ObPartition>(tenant_id, check_deleted, result, table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve part info failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_def_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = true;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
          tenant_id, check_deleted, is_subpart_template, result, table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info faield", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = false;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
          tenant_id, check_deleted, is_subpart_template, result, table_schema_array)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info faield", K(ret));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_table_schema(const uint64_t tenant_id, const bool check_deleted, T& result,
    ObIAllocator& allocator, ObTableSchema*& table_schema)
{
  int ret = common::OB_SUCCESS;

  table_schema = NULL;
  SHARE_SCHEMA_LOG(INFO, "retrieve table schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) {  // no record
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "no row", K(ret));
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
      // check if this is only one
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

template <typename T>
int ObSchemaRetrieveUtils::retrieve_tablegroup_schema(
    const uint64_t tenant_id, T& result, ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema)
{
  int ret = common::OB_SUCCESS;

  tablegroup_schema = NULL;
  SHARE_SCHEMA_LOG(INFO, "retrieve tablegroup schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) {  // no record
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "no row", K(ret));
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

template <typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::get_schema_id(const ObColumnSchemaV2& p)
{
  return p.get_column_id();
}

template <typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::get_schema_id(const ObConstraint& p)
{
  return p.get_constraint_id();
}

template <typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::get_schema_id(const ObPartition& p)
{
  return p.get_part_id();
}

template <typename TABLE_SCHEMA>
int64_t ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::get_schema_id(const ObSubPartition& p)
{
  return p.get_sub_part_id();
}

template <typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::add_schema(
    TABLE_SCHEMA& table_schema, ObColumnSchemaV2& p)
{
  return ObSchemaUtils::add_column_to_table_schema(p, table_schema);
}

template <typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::add_schema(TABLE_SCHEMA& table_schema, ObConstraint& p)
{
  return table_schema.add_constraint(p);
}

template <typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::add_schema(TABLE_SCHEMA& table_schema, ObPartition& p)
{
  if (PARTITION_LEVEL_TWO == table_schema.get_part_level() && table_schema.is_sub_part_template()) {
    // We once support add/drop subpartition by ob_admin in ver 1.4.7x,
    // but sub_part_num is incorrect while we add/drop subpartition by such method.
    // To avoid unexpected error, we ensure sub_part_num is same with def_sub_part_num
    // in templete secondary partition table.
    p.set_sub_part_num(table_schema.get_def_sub_part_num());
  }
  return table_schema.add_partition(p);
}

template <typename TABLE_SCHEMA>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::add_schema(TABLE_SCHEMA& table_schema, ObSubPartition& p)
{
  return table_schema.add_def_subpartition(p);
}

template <typename TABLE_SCHEMA>
template <typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObColumnSchemaV2>::fill_current(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObColumnSchemaV2& p, bool& is_deleted)
{
  return ObSchemaRetrieveUtils::fill_column_schema(tenant_id, check_deleted, result, p, is_deleted);
}

template <typename TABLE_SCHEMA>
template <typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObConstraint>::fill_current(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObConstraint& p, bool& is_deleted)
{
  return ObSchemaRetrieveUtils::fill_constraint(tenant_id, check_deleted, result, p, is_deleted);
}

template <typename TABLE_SCHEMA>
template <typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObPartition>::fill_current(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObPartition& p, bool& is_deleted)
{
  return ObSchemaRetrieveUtils::fill_part_info(tenant_id, check_deleted, result, p, is_deleted);
}

template <typename TABLE_SCHEMA>
template <typename T>
int ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::fill_current(const uint64_t tenant_id,
    const bool check_deleted, const bool is_subpart_template, T& result, ObSubPartition& p, bool& is_deleted)
{
  int ret = OB_SUCCESS;
  if (is_subpart_template) {
    if (OB_FAIL(ObSchemaRetrieveUtils::fill_def_subpart_info(tenant_id, check_deleted, result, p, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to retrieve def sub part info", K(ret));
    }
  } else {
    if (OB_FAIL(ObSchemaRetrieveUtils::fill_subpart_info(tenant_id, check_deleted, result, p, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to retrieve sub part info", K(ret));
    }
  }
  return ret;
}

template <typename TABLE_SCHEMA>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::get_table(const uint64_t table_id, TABLE_SCHEMA*& table)
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
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(ERROR, "cannot find table", K(ret), K(table_id), KPC(table));
  }
  return ret;
}

template <typename TABLE_SCHEMA>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::add(ObSubPartition& p)
{
  int ret = OB_SUCCESS;
  TABLE_SCHEMA* table = NULL;
  if (!is_subpart_def_ && !is_subpart_template_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid arg", K(ret), K_(is_subpart_def), K_(is_subpart_template));
  } else if (OB_FAIL(get_table(p.get_table_id(), table))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get table failed", K(ret), K(mode_));
  } else if (!is_subpart_template_) {
    if (OB_ISNULL(partition_) || partition_->get_table_id() != p.get_table_id() ||
        partition_->get_part_id() != p.get_part_id()) {
      // fill partition
      const ObPartition* tmp_partition = NULL;
      if (OB_FAIL(table->find_partition_by_part_id(p.get_part_id(), true /*check_dropped_partition*/, tmp_partition))) {
        SHARE_SCHEMA_LOG(WARN, "fail to find partition", K(ret), KPC(table), K(p));
      } else {
        partition_ = const_cast<ObPartition*>(tmp_partition);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(partition_) || partition_->get_table_id() != p.get_table_id() ||
               partition_->get_part_id() != p.get_part_id()) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition not match", K(ret), K(p), KPC_(partition));
    } else if (OB_FAIL(partition_->add_partition(p))) {
      SHARE_SCHEMA_LOG(WARN, "add schema failed", K(ret));
    }
  } else {
    if (OB_FAIL((ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::add_schema(*table, p)))) {
      SHARE_SCHEMA_LOG(WARN, "add schema failed", K(ret));
    }
  }
  return ret;
}

template <typename TABLE_SCHEMA>
int64_t ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::get_curr_schema_id()
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::get_schema_id(schemas_[index_]);
}

template <typename TABLE_SCHEMA>
template <typename T>
int ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA>::fill_current(const uint64_t tenant_id, const bool check_deleted,
    const bool is_subpart_template, T& result, ObSubPartition& p, bool& is_deleted)
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, ObSubPartition>::fill_current(
      tenant_id, check_deleted, is_subpart_template, result, p, is_deleted);
}
template <typename TABLE_SCHEMA, typename SCHEMA>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::get_table(const uint64_t table_id, TABLE_SCHEMA*& table)
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
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(ERROR, "cannot find table", K(ret), K(table_id), KPC(table));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename SCHEMA>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::add(SCHEMA& p)
{
  int ret = OB_SUCCESS;
  TABLE_SCHEMA* table = NULL;
  if (OB_FAIL(get_table(p.get_table_id(), table))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get table failed", K(ret), K(mode_));
  } else if (OB_FAIL((ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::add_schema(*table, p)))) {
    SHARE_SCHEMA_LOG(WARN, "add schema failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename SCHEMA>
int64_t ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::get_curr_schema_id()
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::get_schema_id(schemas_[index_]);
}

template <typename TABLE_SCHEMA, typename SCHEMA>
template <typename T>
int ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA>::fill_current(
    const uint64_t tenant_id, const bool check_deleted, T& result, SCHEMA& p, bool& is_deleted)
{
  return ObSchemaRetrieveHelperBase<TABLE_SCHEMA, SCHEMA>::fill_current(
      tenant_id, check_deleted, result, p, is_deleted);
}

template <typename TABLE_SCHEMA, typename SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema)
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
    SCHEMA* last_schema = NULL;
    ObSchemaRetrieveHelper<TABLE_SCHEMA, SCHEMA> helper(*table_schema);
    while (OB_SUCC(ret) && common::OB_SUCCESS == (ret = result.next())) {
      bool is_deleted = false;
      SCHEMA& current = helper.get_current();
      current.reset();
      if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, result, current, is_deleted))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
      } else if (table_id != current.get_table_id()) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "table_id is not equal", K(ret), K(table_id), K(current));
      } else if (helper.get_curr_schema_id() == last_schema_id) {
        // the same with last schema, continue;
        ret = common::OB_SUCCESS;
      } else {
        if (NULL == last_schema || is_last_deleted) {
          // LAST schema IS INVALID, IGNORE
        } else if (OB_FAIL(helper.add(*last_schema))) {  // add last schema
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
      // save current column to last, rotate
      last_schema = &current;
      is_last_deleted = is_deleted;
      last_schema_id = helper.get_curr_schema_id();
      helper.rotate();  // rotate
    }
    if (OB_ITER_END != ret) {
      SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      // add last partition
      if (NULL != last_schema && !is_last_deleted) {
        if (OB_FAIL(helper.add(*last_schema))) {
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_part_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<TABLE_SCHEMA, ObPartition, T>(tenant_id, check_deleted, result, table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve part info failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_def_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = true;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
          tenant_id, check_deleted, is_subpart_template, result, table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, TABLE_SCHEMA*& table_schema)
{
  int ret = OB_SUCCESS;
  bool is_subpart_template = false;
  if (OB_FAIL((retrieve_subpart_schema<TABLE_SCHEMA, T>(
          tenant_id, check_deleted, is_subpart_template, result, table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve subpart info failed", K(ret));
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_schema(const uint64_t tenant_id, const bool check_deleted,
    const bool is_subpart_template, T& result, ObArray<TABLE_SCHEMA*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_subpart_def = true;
  bool is_last_deleted = false;
  ObSubPartition* last_schema = NULL;
  ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA> helper(table_schema_array, is_subpart_def, is_subpart_template);
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    bool is_deleted = false;
    ObSubPartition& current = helper.get_current();
    current.reset();
    if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, is_subpart_template, result, current, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
    } else if (OB_ISNULL(last_schema) || current.key_match(*last_schema)) {
      // continue
    } else if (!is_last_deleted && OB_FAIL(helper.add(*last_schema))) {  // add last schema
      SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
    }
    last_schema = &current;
    is_last_deleted = is_deleted;
    helper.rotate();  // rotate
  }
  if (OB_ITER_END != ret) {
    SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    // add last partition
    if (OB_NOT_NULL(last_schema) && !is_last_deleted) {
      if (OB_FAIL(helper.add(*last_schema))) {
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
    }
  }
  return ret;
}

template <typename TABLE_SCHEMA, typename T>
int ObSchemaRetrieveUtils::retrieve_subpart_schema(const uint64_t tenant_id, const bool check_deleted,
    const bool is_subpart_template, T& result, TABLE_SCHEMA*& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "table schema is NULL", K(ret), K(table_schema));
  } else {
    bool is_subpart_def = true;
    bool is_last_deleted = false;
    ObSubPartition* last_schema = NULL;
    ObSubPartSchemaRetrieveHelper<TABLE_SCHEMA> helper(*table_schema, is_subpart_def, is_subpart_template);
    while (OB_SUCC(ret) && OB_SUCC(result.next())) {
      bool is_deleted = false;
      ObSubPartition& current = helper.get_current();
      current.reset();
      if (OB_FAIL(helper.fill_current(tenant_id, check_deleted, is_subpart_template, result, current, is_deleted))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema fail", K(ret));
      } else if (OB_ISNULL(last_schema) || current.key_match(*last_schema)) {
        // continue
      } else if (!is_last_deleted && OB_FAIL(helper.add(*last_schema))) {  // add last schema
        SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
      }
      last_schema = &current;
      is_last_deleted = is_deleted;
      helper.rotate();  // rotate
    }
    if (OB_ITER_END != ret) {
      SHARE_SCHEMA_LOG(WARN, "fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      // add last partition
      if (OB_NOT_NULL(last_schema) && !is_last_deleted) {
        if (OB_FAIL(helper.add(*last_schema))) {
          SHARE_SCHEMA_LOG(WARN, "add last schema failed", K(*last_schema), K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_column_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObColumnSchemaV2>(tenant_id, check_deleted, result, table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve column schema failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_constraint(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((retrieve_schema<ObTableSchema, ObConstraint>(tenant_id, check_deleted, result, table_schema)))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve constraint schema failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_constraint_column_info(const uint64_t tenant_id, T& result, ObConstraint*& cst)
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
    SHARE_SCHEMA_LOG(INFO, "retrieve constraint column info");
    ret = common::OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cst->assign_column_ids(column_ids))) {
      SHARE_SCHEMA_LOG(WARN, "fail to assign_column_ids", K(column_ids));
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_recycle_object(
    const uint64_t tenant_id, T& result, ObIArray<ObRecycleObject>& recycle_objs)
{
  int ret = common::OB_SUCCESS;
  ObRecycleObject recycle_obj;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    if (OB_FAIL(fill_recycle_object(tenant_id, result, recycle_obj))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill recycle object. ", K(ret));
    } else if (OB_FAIL(recycle_objs.push_back(recycle_obj))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    }
    recycle_obj.reset();
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

template <typename T>
int ObSchemaRetrieveUtils::fill_tenant_schema(T& result, ObTenantSchema& tenant_schema, bool& is_deleted)
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
  int64_t storage_format_version_default_value = 0;
  int64_t storage_format_work_version_default_value = 0;
  int64_t default_tablegroup_id_default_value = OB_INVALID_INDEX;
  ObCompatibilityMode default_compat_mode = ObCompatibilityMode::MYSQL_MODE;
  int64_t default_drop_tenant_time = OB_INVALID_VERSION;
  int64_t default_in_recyclebin = 0;
  ObString tenant_status_str("");
  ObString default_tenant_status_str("TENANT_STATUS_NORMAL");
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, tenant_id, tenant_schema, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tenant_name, tenant_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "info", info);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "locality",
        locality,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        locality_default_value);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "previous_locality",
        previous_locality,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        previous_locality_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, locked, tenant_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tenant_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, tenant_schema, common::ObCollationType);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, read_only, tenant_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rewrite_merge_version, tenant_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        storage_format_version,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        storage_format_version_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        storage_format_work_version,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        storage_format_work_version_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        default_tablegroup_id,
        tenant_schema,
        uint64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_tablegroup_id_default_value);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        compatibility_mode,
        tenant_schema,
        common::ObCompatibilityMode,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_compat_mode);
    tenant_schema.set_charset_type(ObCharset::charset_type_by_coll(tenant_schema.get_collation_type()));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_tenant_time,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_drop_tenant_time);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "status",
        tenant_status_str,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_tenant_status_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        in_recyclebin,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_in_recyclebin);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_comment(info))) {
        SHARE_SCHEMA_LOG(WARN, "set_comment failed", K(ret));
      } else if (OB_FAIL(tenant_schema.set_locality(locality))) {
        SHARE_SCHEMA_LOG(WARN, "set locality failed", K(ret));
      } else if (OB_FAIL(fill_replica_options(result, tenant_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fail to fill replica options", K(ret));
      } else if (OB_FAIL(fill_tenant_zone_region_replica_num_array(tenant_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema zone replica dist failed", K(ret));
      } else if (OB_FAIL(tenant_schema.set_previous_locality(previous_locality))) {
        SHARE_SCHEMA_LOG(WARN, "set previous locality failed", K(ret));
      } else {
      }  // good
    }
    if (OB_SUCC(ret)) {
      ObTenantStatus status = TENANT_STATUS_MAX;
      if (OB_FAIL(get_tenant_status(tenant_status_str, status))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get tenant status", K(ret), K(tenant_status_str));
      } else {
        tenant_schema.set_status(status);
      }
    }
  }
  SHARE_SCHEMA_LOG(INFO, "retrieve tenant schema", K(tenant_schema), K(is_deleted), K(ret));
  return ret;
}

/*
 * format tenant's locality info
 * If locality is an empty string, it means each zone should has one Full(replica type) replica.
 */
int ObSchemaRetrieveUtils::fill_tenant_zone_region_replica_num_array(share::schema::ObTenantSchema& tenant_schema)
{
  return fill_schema_zone_region_replica_num_array(tenant_schema);
}

/*
 * format table's locality info
 * If locality is an empty string, it means each zone should has one Full(replica type) replica.
 */
template <typename SCHEMA>
int ObSchemaRetrieveUtils::fill_table_zone_region_replica_num_array(SCHEMA& table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.has_partition() && !table_schema.get_locality_str().empty()) {
    if (OB_FAIL(fill_schema_zone_region_replica_num_array(table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill table zone region replica num array", K(ret));
    } else {
    }  // no more to do
  } else {
  }  // no more to do
  return ret;
}

template <typename SCHEMA>
int ObSchemaRetrieveUtils::fill_schema_zone_region_replica_num_array(SCHEMA& schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  rootserver::ObLocalityDistribution locality_dist;
  ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
  if (OB_FAIL(locality_dist.init())) {
    SHARE_SCHEMA_LOG(WARN, "fail to init locality dist", K(ret));
  } else if (OB_FAIL(schema.get_zone_list(zone_list))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get zone list", K(ret));
  } else if (OB_FAIL(locality_dist.parse_locality(schema.get_locality_str(), zone_list))) {
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
    if (OB_FAIL(locality_dist.output_normalized_locality(locality_str, MAX_LOCALITY_LENGTH, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to normalized locality", K(ret));
    } else if (OB_FAIL(schema.set_locality(locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set normalized locality back to schema", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_temp_table_schema(const uint64_t tenant_id, T& result, ObTableSchema& table_schema)
{
  int ret = common::OB_SUCCESS;
  UNUSED(tenant_id);
  ObString create_host;
  ObString default_create_host("");
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "create_host", create_host, true, ObSchemaService::g_ignore_column_retrieve_error_, default_create_host);
  if (0 >= create_host.length()) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "get unexpected create_host. ", K(ret), K(create_host));
  } else {
    table_schema.set_create_host(create_host);
  }
  SHARE_SCHEMA_LOG(INFO, "Get create_host ", K(create_host), K(table_schema));
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_table_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObTableSchema& table_schema, bool& is_deleted)
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, table_type, table_schema, ObTableType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_type, table_schema, ObIndexType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_using_type, table_schema, ObIndexUsingType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, load_type, table_schema, ObTableLoadType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, def_type, table_schema, ObTableDefType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_column_num, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_column_num, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, replica_num, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, autoinc_column_id, table_schema, uint64_t);
    uint64_t auto_increment_default = 1;
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        auto_increment,
        table_schema,
        uint64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        auto_increment_default);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, read_only, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_split_pos, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, max_used_column_id, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_level, table_schema, ObPartitionLevel);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, create_mem_version, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, block_size, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, progressive_merge_num, table_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_schema);

    if (OB_SUCCESS == ret && table_schema.get_block_size() <= 0) {
      SHARE_SCHEMA_LOG(WARN,
          "set tablet sstable block size to default value:",
          "read",
          table_schema.get_block_size(),
          "table_id",
          table_schema.get_table_id());
      table_schema.set_block_size(OB_DEFAULT_SSTABLE_BLOCK_SIZE);
    }
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, compress_func_name, table_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "expire_condition", expire_info);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, is_use_bloomfilter, table_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, table_schema, common::ObCollationType);
    table_schema.set_charset_type(ObCharset::charset_type_by_coll(table_schema.get_collation_type()));
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, data_table_id, table_schema, tenant_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_status, table_schema, ObIndexStatus);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_status,
        table_schema,
        ObPartitionStatus,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_schema_version,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, comment, table_schema);
    ObString locality_default_val("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, locality, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, locality_default_val);
    ObString previous_locality_default_val("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        previous_locality,
        table_schema,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        previous_locality_default_val);
    ObString parser_name_default_val;
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        parser_name,
        table_schema,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        parser_name_default_val);
    int64_t tablet_size_default = OB_DEFAULT_TABLET_SIZE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        tablet_size,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        tablet_size_default);
    int64_t pctfree_default = OB_DEFAULT_PCTFREE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        pctfree,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        pctfree_default);
    // view schema
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, view_definition, table_schema.get_view_schema());
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(
        result, view_check_option, table_schema.get_view_schema(), ViewCheckOption);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, view_is_updatable, table_schema.get_view_schema(), bool);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        index_attributes_set,
        table_schema,
        uint64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, session_id, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    ObString pk_comment("");
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, pk_comment, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, pk_comment);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, sess_active_time, table_schema, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    /*
     * Here comes a compatibility problem:
     * row_store_type's default value is defined as flat_row_store when cluster is upgraded from ver 1.4.x
     * and is defined as encoding_row_store when cluster is upgraded from ver 2.x.
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        progressive_merge_round,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        storage_format_version,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, table_mode, table_schema, int32_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.set_expire_info(expire_info))) {
        SHARE_SCHEMA_LOG(WARN, "set expire info failed", K(ret));
      }
    }

    // part_expr
    int64_t part_num = 0;
    ObPartitionOption& partition_option = table_schema.get_part_option();
    enum ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_HASH;
    EXTRACT_INT_FIELD_MYSQL(result, "part_func_type", part_func_type, ObPartitionFuncType);
    partition_option.set_part_func_type(part_func_type);
    EXTRACT_INT_FIELD_MYSQL(result, "part_num", part_num, int64_t);
    partition_option.set_part_num(part_num);
    int64_t max_used_part_id_default = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        max_used_part_id,
        partition_option,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        max_used_part_id_default);
    int64_t partition_cnt_within_partition_table_default = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_cnt_within_partition_table,
        partition_option,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        partition_cnt_within_partition_table_default);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_func_expr", part_func_expr);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_option.set_part_expr(part_func_expr))) {
        SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
      }
    }
    // sub_part_expr
    ObPartitionOption& sub_part_option = table_schema.get_sub_part_option();
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

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_replica_options(result, table_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fill replica options failed", K(ret));
      } else if (OB_FAIL(fill_table_zone_region_replica_num_array(table_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fill schema zone region replica dist failed", K(ret));
      } else {
      }  // good
    }
    int64_t max_used_constraint_id_default = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        max_used_constraint_id,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        max_used_constraint_id_default);
    const ObDuplicateScope duplicate_scope_default = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        duplicate_scope,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        duplicate_scope_default);
    const bool default_binding = false;
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        binding,
        table_schema,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_binding);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        table_schema,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);
    /*
     * __all_table_v2_history is added in ver 2.2.60. To avoid compatibility problems,
     * __all_table/__all_table_history/__all_table_v2/__all_table_v2_history should add columns in upgrade post stage
     * from ver 2.2.60. Here, we should ignore error because column is not exist when cluster is still in upgradation.
     */
    /* ver 2.2.60 */
    bool ignore_column_error =
        ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, is_sub_part_template, table_schema, bool, true /* skip null error*/, ignore_column_error, true);
    if (OB_SUCC(ret) && !table_schema.is_sub_part_template()) {
      table_schema.get_sub_part_option().set_part_num(0);
      table_schema.set_def_sub_part_num(0);
    }

    // table dop: table dop supported from 2270
    int64_t default_table_dop = OB_DEFAULT_TABLE_DOP;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, dop, table_schema, int64_t, true, ignore_column_error, default_table_dop);

    ignore_column_error =
        ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2271;
    // character_set_client and collation_connection of view schema, supported from 227
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        character_set_client,
        table_schema.get_view_schema(),
        ObCharsetType,
        true /* skip null error*/,
        ignore_column_error,
        CHARSET_INVALID);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        collation_connection,
        table_schema.get_view_schema(),
        ObCollationType,
        true /* skip null error*/,
        ignore_column_error,
        CS_TYPE_INVALID);

    /* ver 3.1 */
    ignore_column_error =
        ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100;
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, auto_part, partition_option, true, ignore_column_error, false);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, auto_part_size, partition_option, int64_t, true, ignore_column_error, -1);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_column_schema(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObColumnSchemaV2& column, bool& is_deleted)
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, column_flags, column, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, prev_column_id, column, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, UINT64_MAX);

    common::ColumnType default_type = column.is_generated_column() ? ObVarcharType : column.get_data_type();
    EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, orig_default_value, default_type, column, false, false, tenant_id);
    EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, cur_default_value, default_type, column, true, false, tenant_id);
    EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, orig_default_value_v2, default_type, column, false, true, tenant_id);
    EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, cur_default_value_v2, default_type, column, true, true, tenant_id);

    if (OB_SUCC(ret) && column.is_enum_or_set()) {
      ObString extend_type_info;
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "extended_type_info", extend_type_info);
      int64_t pos = 0;
      if (extend_type_info.empty()) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "extend_type_info is empty", K(ret));
      } else if (OB_FAIL(
                     column.deserialize_extended_type_info(extend_type_info.ptr(), extend_type_info.length(), pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize_extended_type_info", K(ret));
      } else {
      }
    }
  }

  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_constraint(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObConstraint& constraint, bool& is_deleted)
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
    const bool default_validate_flag = true;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        constraint_type,
        constraint,
        ObConstraintType,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, rely_flag, constraint, true, ObSchemaService::g_ignore_column_retrieve_error_, default_rely_flag);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, enable_flag, constraint, true, ObSchemaService::g_ignore_column_retrieve_error_, default_enable_flag);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        validate_flag,
        constraint,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_validate_flag);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, schema_version, constraint, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, constraint_name, constraint);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, check_expr, constraint);
  }

  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_constraint_column_info(
    const uint64_t tenant_id, T& result, uint64_t& column_id, bool& is_deleted)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  column_id = 0;
  is_deleted = false;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "column_id", column_id, uint64_t);
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_database_schema(
    const uint64_t tenant_id, T& result, ObDatabaseSchema& db_schema, bool& is_deleted)
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_replica_options(result, db_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fill replica options failed", K(ret));
      }
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        db_schema,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_tablegroup_schema(
    const uint64_t tenant_id, T& result, ObTablegroupSchema& tg_schema, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  tg_schema.reset();
  is_deleted = false;
  tg_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, tg_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    ObString primary_zone_str;
    ObString locality_str;
    ObString previous_locality_str;
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tablegroup_name, tg_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, comment, tg_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "primary_zone", primary_zone_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "locality", locality_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "previous_locality", previous_locality_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tg_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_status,
        tg_schema,
        ObPartitionStatus,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_schema_version,
        tg_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tg_schema.set_primary_zone(primary_zone_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set primary zone", K(ret), K(primary_zone_str));
      } else if (OB_FAIL(tg_schema.set_locality(locality_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set locality", K(ret), K(locality_str));
      } else if (OB_FAIL(tg_schema.set_previous_locality(previous_locality_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set previous_locality", K(ret), K(previous_locality_str));
      } else if (OB_FAIL(tg_schema.fill_additional_options())) {
        SHARE_SCHEMA_LOG(WARN, "fail to normalize options", K(ret));
      }
    }

    const bool skip_null_error = true;
    const enum ObPartitionLevel default_part_level = PARTITION_LEVEL_ZERO;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        part_level,
        tg_schema,
        ObPartitionLevel,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_part_level);

    const int64_t default_part_num = 0;
    enum ObPartitionFuncType default_part_func_type = PARTITION_FUNC_TYPE_HASH;
    const int64_t default_func_expr_num = 0;
    const bool default_binding = false;
    int64_t max_used_part_id_default = -1;
    int64_t def_sub_part_num = 0;

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        part_num,
        tg_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_part_num);
    ObPartitionOption& partition_option = tg_schema.get_part_option();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        part_func_type,
        partition_option,
        ObPartitionFuncType,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_part_func_type);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        part_func_expr_num,
        tg_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_func_expr_num);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        max_used_part_id,
        partition_option,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        max_used_part_id_default);

    ObPartitionOption& sub_part_option = tg_schema.get_sub_part_option();
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sub_part_func_type,
        sub_part_option,
        ObPartitionFuncType,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_part_func_type);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sub_part_func_expr_num,
        tg_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_func_expr_num);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "sub_part_num",
        def_sub_part_num,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_part_num);
    tg_schema.set_def_sub_part_num(def_sub_part_num);
    sub_part_option.set_part_num(def_sub_part_num);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, binding, tg_schema, skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_binding);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        tg_schema,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        is_sub_part_template,
        tg_schema,
        bool,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        true);
    if (OB_SUCC(ret) && !tg_schema.is_sub_part_template()) {
      tg_schema.get_sub_part_option().set_part_num(0);
      tg_schema.set_def_sub_part_num(0);
    }
  }
  SQL_LOG(DEBUG, "fill tablegroup schema", K(ret), K(tg_schema));
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_user_schema(
    const uint64_t tenant_id, T& result, ObUserInfo& user_info, bool& is_deleted)
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
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(
        result, priv_file, user_info, PRIV_FILE, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(
        result, priv_alter_tenant, user_info, PRIV_ALTER_TENANT, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(
        result, priv_alter_system, user_info, PRIV_ALTER_SYSTEM, ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result,
        priv_create_resource_pool,
        user_info,
        PRIV_CREATE_RESOURCE_POOL,
        ObSchemaService::g_ignore_column_retrieve_error_);
    EXTRACT_PRIV_FROM_MYSQL_RESULT_IGNORE(result,
        priv_create_resource_unit,
        user_info,
        PRIV_CREATE_RESOURCE_UNIT,
        ObSchemaService::g_ignore_column_retrieve_error_);
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
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        x509_subject,
        user_info,
        false,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_ssl_specified);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, type, user_info, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result,
        profile_id,
        user_info,
        tenant_id,
        false,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_ID);
    int64_t default_last_changed = -1;
    int64_t password_last_changed = -1;
    common::ObTimeZoneInfoWrap tz_info_wrap;
    GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(result.get_timestamp,
        "password_last_changed",
        password_last_changed,
        default_last_changed,
        tz_info_wrap.get_time_zone_info());
    user_info.set_password_last_changed(password_last_changed);

    if (OB_SUCC(ret)) {
      int64_t int_value = 0;
      if (OB_SUCCESS == (ret = result.get_int("priv_create_synonym", int_value))) {
      } else if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
      } else if (OB_ERR_COLUMN_NOT_FOUND == ret && ObSchemaService::g_ignore_column_retrieve_error_) {
        int_value = 1;  // default value
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
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_role_grantee_map_schema(
    const uint64_t tenant_id, T& result, const bool is_fetch_role, ObArray<ObUserInfo>& user_array)
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
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "admin_option",
        admin_option,
        uint64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "disable_flag",
        disable_flag,
        uint64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);

    ObUserInfo* user_info = NULL;
    if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "grantee / role is deleted", K(grantee_id), K(role_id));
    } else if (prev_key_id == (is_fetch_role ? grantee_id : role_id)) {
      if (prev_value_id == (is_fetch_role ? role_id : grantee_id)) {
        ret = common::OB_SUCCESS;
      } else if (OB_FAIL(ObSchemaRetrieveUtils::find_user_info(
                     is_fetch_role ? grantee_id : role_id, user_array, user_info))) {
        SHARE_SCHEMA_LOG(WARN, "failed to find user info", K(ret), K(grantee_id), K(role_id));
      } else if (NULL == user_info) {
        // skip
        // e.g: user_array may only contain role.
        //      When we find user info by grantee_id, it's valid that user_info is null.
        continue;
        SHARE_SCHEMA_LOG(INFO, "user info is null", K(ret), K(is_fetch_role), K(grantee_id), K(role_id));
      } else if (is_fetch_role && (OB_FAIL(user_info->add_role_id(role_id, admin_option, disable_flag)))) {
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
      } else if (is_fetch_role && OB_FAIL(user_info->add_role_id(role_id, admin_option, disable_flag))) {
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
    const uint64_t user_id, ObArray<ObUserInfo>& user_array, ObUserInfo*& user_info)
{
  int ret = OB_SUCCESS;
  typename ObArray<ObUserInfo>::iterator iter = user_array.end();
  iter = std::lower_bound(user_array.begin(), user_array.end(), user_id, compare_user_id<ObUserInfo>);
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

template <typename T>
bool ObSchemaRetrieveUtils::compare_user_id(const T& user_info, const uint64_t user_id)
{
  bool cmp = false;
  cmp = user_info.get_user_id() > user_id;
  return cmp;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_db_priv_schema(const uint64_t tenant_id, T& result, ObDBPriv& db_priv, bool& is_deleted)
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

template <typename T>
int ObSchemaRetrieveUtils::fill_sys_priv_schema(
    const uint64_t tenant_id, T& result, ObSysPriv& sys_priv, bool& is_deleted, ObRawPriv& raw_p_id, uint64_t& option)
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

template <typename T>
int ObSchemaRetrieveUtils::fill_table_priv_schema(
    const uint64_t tenant_id, T& result, ObTablePriv& table_priv, bool& is_deleted)
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

template <typename T>
int ObSchemaRetrieveUtils::fill_obj_priv_schema(const uint64_t tenant_id, T& result, ObObjPriv& obj_priv,
    bool& is_deleted, ObRawObjPriv& raw_p_id, uint64_t& option)
{
  int ret = common::OB_SUCCESS;
  uint64_t table_id;
  uint64_t objtype;
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
template <typename T>
int ObSchemaRetrieveUtils::fill_outline_schema(
    const uint64_t tenant_id, T& result, ObOutlineInfo& outline_info, bool& is_deleted)
{
  outline_info.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
  outline_info.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, outline_id, outline_info, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, database_id, outline_info, tenant_id);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, name, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, signature, outline_info);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sql_id,
        outline_info,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        ObString::make_string(""));
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

template <typename T>
int ObSchemaRetrieveUtils::fill_synonym_schema(
    const uint64_t tenant_id, T& result, ObSynonymInfo& synonym_info, bool& is_deleted)
{
  synonym_info.reset();
  is_deleted = false;
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
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_udf_schema(const uint64_t tenant_id, T& result, ObUDF& udf_info, bool& is_deleted)
{
  udf_info.reset();
  is_deleted = false;
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

template <typename T>
int ObSchemaRetrieveUtils::fill_udf_schema(
    const uint64_t tenant_id, T& result, ObSimpleUDFSchema& udf_schema, bool& is_deleted)
{
  udf_schema.reset();
  is_deleted = false;
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

template <typename T>
int ObSchemaRetrieveUtils::fill_dblink_schema(
    const uint64_t tenant_id, T& result, ObDbLinkSchema& dblink_schema, bool& is_deleted)
{
  dblink_schema.reset();
  is_deleted = false;
  int ret = common::OB_SUCCESS;
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
    // TOTO: decrypt
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, password, dblink_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, host_ip, dblink_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, host_port, dblink_schema, int32_t);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_link_table_schema(
    const uint64_t tenant_id, T& result, ObTableSchema& table_schema, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  table_schema.reset();
  is_deleted = false;

  table_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, table_schema, tenant_id);
  if (!is_deleted) {
    EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(result, schema_version, table_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, table_name, table_schema);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, table_schema, common::ObCollationType);
    table_schema.set_charset_type(ObCharset::charset_type_by_coll(table_schema.get_collation_type()));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_link_column_schema(
    const uint64_t tenant_id, T& result, ObColumnSchemaV2& column_schema, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  column_schema.reset();
  is_deleted = false;

  column_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, column_schema, tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, column_schema, uint64_t);
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, rowkey_position, column_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_type, column_schema, common::ColumnType);
    if (ob_is_accuracy_length_valid_tc(column_schema.get_data_type())) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_length, column_schema, int32_t);
    } else {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_precision, column_schema, int16_t);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_scale, column_schema, int16_t);
    }
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, nullable, column_schema, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, collation_type, column_schema, common::ObCollationType);
    column_schema.set_charset_type(ObCharset::charset_type_by_coll(column_schema.get_collation_type()));
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, column_schema);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_sequence_schema(
    const uint64_t tenant_id, T& result, ObSequenceSchema& sequence_schema, bool& is_deleted)
{
  sequence_schema.reset();
  is_deleted = false;
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
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, start_with, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, increment_by, sequence_schema);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, cycle_flag, sequence_schema);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, order_flag, sequence_schema);
    EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, cache_size, sequence_schema);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_recycle_object(const uint64_t tenant_id, T& result, ObRecycleObject& recycle_obj)
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
    if (tenant_id == OB_SYS_TENANT_ID) {
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, tenant_id, recycle_obj, uint64_t);
    } else {
      recycle_obj.set_tenant_id(tenant_id);
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_schema_operation(const uint64_t tenant_id, T& result,
    ObSchemaService::SchemaOperationSetWithAlloc& schema_operations, ObSchemaOperation& schema_operation)
{
  int ret = common::OB_SUCCESS;
  schema_operation.reset();
  int64_t operation_type = 0;
  if (OB_SYS_TENANT_ID != tenant_id && OB_INVALID_TENANT_ID != tenant_id) {
    schema_operation.tenant_id_ = tenant_id;
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "table_id", schema_operation.table_id_, tenant_id);
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "database_id", schema_operation.database_id_, tenant_id);
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "tablegroup_id", schema_operation.tablegroup_id_, tenant_id);
    EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "user_id", schema_operation.user_id_, tenant_id);
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", schema_operation.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "table_id", schema_operation.table_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "database_id", schema_operation.database_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "tablegroup_id", schema_operation.tablegroup_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "user_id", schema_operation.user_id_, uint64_t);
  }
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
  EXTRACT_INT_FIELD_MYSQL(result, "operation_type", operation_type, int64_t);

  if (OB_SUCC(ret)) {
    schema_operation.op_type_ = static_cast<ObSchemaOperationType>(operation_type);
    schema_operation.outline_id_ = schema_operation.table_id_;  // table_id_ reused to store outline_id
    schema_operation.synonym_id_ = schema_operation.table_id_;  // table_id_ reused to store synonym_id
    schema_operation.udf_name_ =
        schema_operation.table_name_;  // table_name_ reused to store user defined function' name
    schema_operation.sequence_id_ = schema_operation.table_id_;      // table_id_ reused to store synonym_id
    schema_operation.sequence_name_ = schema_operation.table_name_;  // table_name_ reused to store sequence name
    schema_operation.profile_id_ = schema_operation.table_id_;       // table_id_ reused to store profile_id_
    schema_operation.grantee_id_ = schema_operation.user_id_;        // table_id_ reused to store grantee_id_
    schema_operation.grantor_id_ = schema_operation.database_id_;    // database_id_ reused to store grantor_id_

    SHARE_SCHEMA_LOG(DEBUG, "fill schema operation", K(schema_operation));
    schema_operation.dblink_id_ = schema_operation.table_id_;  // table_id_ reused to store dblink_id
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

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_tenant_schema(ObISQLClient& client, T& result, ObIArray<S>& tenant_schema_array)
{
  UNUSED(client);
  int ret = common::OB_SUCCESS;

  uint64_t prev_tenant_id = common::OB_INVALID_ID;
  S tenant_schema;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    if (OB_FAIL(fill_tenant_schema(result, tenant_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill tenant schema", K(ret));
    } else if (tenant_schema.get_tenant_id() == prev_tenant_id) {
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "tenant is is_deleted, don't add", "tenant_id", tenant_schema.get_tenant_id());
    } else if (OB_FAIL(tenant_schema_array.push_back(tenant_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    }
    prev_tenant_id = tenant_schema.get_tenant_id();
    tenant_schema.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all tenant schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_system_variable_obj(
    const uint64_t tenant_id, T& result, ObIAllocator& allocator, ObObj& out_var_obj)
{
  UNUSED(tenant_id);
  int ret = common::OB_SUCCESS;
  int64_t vtype = 0;
  bool is_deleted = false;
  char* value_buf = NULL;
  ObString result_value;
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "data_type", vtype, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "value", result_value);
  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "fail to extract data", K(ret));
  } else if (is_deleted) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else if (OB_UNLIKELY(result_value.length() > common::OB_MAX_SYS_VAR_VAL_LENGTH)) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_SCHEMA_LOG(WARN,
        "set sysvar value is overflow",
        "max length",
        OB_MAX_SYS_VAR_VAL_LENGTH,
        "value length",
        result_value.length());
  } else if (!result_value.empty() &&
             OB_ISNULL(value_buf = static_cast<char*>(allocator.alloc(result_value.length())))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(WARN, "fail to alloc char array", K(ret));
  } else {
    if (!result_value.empty()) {
      MEMCPY(value_buf, result_value.ptr(), result_value.length());
    }
    ObString svalue(result_value.length(), value_buf);
    ObObj var_value;
    var_value.set_varchar(svalue);
    ObObjType var_type = static_cast<ObObjType>(vtype);
    // checked with ,there is no timestamp data in __all_sys_variables, so just pass timezone
    // info as NULL
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
    ObObj casted_val;
    const ObObj* res_val = NULL;
    if (OB_FAIL(ObObjCaster::to_type(var_type, cast_ctx, var_value, casted_val, res_val))) {
      _SHARE_SCHEMA_LOG(WARN,
          "failed to cast object, ret=%d cell=%s from_type=%s to_type=%s",
          ret,
          to_cstring(var_value),
          ob_obj_type_str(var_value.get_type()),
          ob_obj_type_str(var_type));
    } else if (OB_ISNULL(res_val)) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "casted success, but res_val is NULL", K(ret), K(var_value), K(var_type));
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
template <typename T, typename SCHEMA>
int ObSchemaRetrieveUtils::retrieve_system_variable(const uint64_t tenant_id, T& result, SCHEMA& sys_variable_schema)
{
  UNUSED(tenant_id);
  int ret = common::OB_SUCCESS;
  ObSysVarSchema sysvar_schema;
  ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
  ObString prev_sys_name;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    bool is_deleted = false;
    sysvar_schema.reset();
    if (OB_FAIL(fill_sysvar_schema(tenant_id, result, sysvar_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill sysvar schema", K(ret));
    } else if (ObCharset::case_insensitive_equal(prev_sys_name, sysvar_schema.get_name())) {
      // do nothing
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "sysvar is is_deleted, don't add", K(sysvar_schema));
    } else if (OB_FAIL(sys_variable_schema.add_sysvar_schema(sysvar_schema))) {
      if (common::OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
        ret = common::OB_SUCCESS;
        SHARE_SCHEMA_LOG(INFO, "sysvar maybe come from diff version, ingore it", K(sysvar_schema));
      } else {
        SHARE_SCHEMA_LOG(WARN, "add sysvar schema failed", K(ret), K(sysvar_schema));
      }
    } else if (OB_FAIL(ob_write_string(allocator, sysvar_schema.get_name(), prev_sys_name))) {
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

template <typename T>
int ObSchemaRetrieveUtils::fill_sysvar_schema(
    const uint64_t tenant_id, T& result, ObSysVarSchema& schema, bool& is_deleted)
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

#define RETRIEVE_SCHEMA_FUNC_DEFINE(SCHEMA)                                                                           \
  template <typename T, typename S>                                                                                   \
  int ObSchemaRetrieveUtils::retrieve_##SCHEMA##_schema(                                                              \
      const uint64_t tenant_id, T& result, ObIArray<S>& schema_array)                                                 \
  {                                                                                                                   \
    int ret = common::OB_SUCCESS;                                                                                     \
    uint64_t prev_id = common::OB_INVALID_ID;                                                                         \
    S schema;                                                                                                         \
    int64_t count = 0;                                                                                                \
    while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {                                        \
      bool is_deleted = false;                                                                                        \
      count++;                                                                                                        \
      if (OB_FAIL(fill_##SCHEMA##_schema(tenant_id, result, schema, is_deleted))) {                                   \
        SHARE_SCHEMA_LOG(WARN, "fail to fill " #SCHEMA " schema ", K(ret));                                           \
      } else if (schema.get_##SCHEMA##_id() == prev_id) {                                                             \
        SHARE_SCHEMA_LOG(                                                                                             \
            DEBUG, "hualong debug ignore", "id", schema.get_##SCHEMA##_id(), "version", schema.get_schema_version()); \
      } else if (is_deleted) {                                                                                        \
        SHARE_SCHEMA_LOG(INFO, #SCHEMA " is is_deleted, don't add", #SCHEMA "_id", schema.get_##SCHEMA##_id());       \
      } else if (OB_FAIL(schema_array.push_back(schema))) {                                                           \
        SHARE_SCHEMA_LOG(WARN, "failed to push back", K(schema), K(ret));                                             \
      } else {                                                                                                        \
        SHARE_SCHEMA_LOG(INFO, "retrieve " #SCHEMA " schema succeed", K(schema));                                     \
      }                                                                                                               \
      prev_id = schema.get_##SCHEMA##_id();                                                                           \
      schema.reset();                                                                                                 \
    }                                                                                                                 \
    if (ret != common::OB_ITER_END) {                                                                                 \
      SHARE_SCHEMA_LOG(WARN, "fail to get all " #SCHEMA " schema. iter quit. ", K(ret));                              \
    } else {                                                                                                          \
      ret = common::OB_SUCCESS;                                                                                       \
      SHARE_SCHEMA_LOG(INFO, "retrieve all " #SCHEMA " schemas succeed", K(schema_array));                            \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }

RETRIEVE_SCHEMA_FUNC_DEFINE(user);
RETRIEVE_SCHEMA_FUNC_DEFINE(database);
RETRIEVE_SCHEMA_FUNC_DEFINE(tablegroup);
RETRIEVE_SCHEMA_FUNC_DEFINE(table);
RETRIEVE_SCHEMA_FUNC_DEFINE(outline);
RETRIEVE_SCHEMA_FUNC_DEFINE(sequence);

RETRIEVE_SCHEMA_FUNC_DEFINE(synonym);

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_udf_schema(const uint64_t tenant_id, T& result, ObIArray<S>& schema_array)
{
  int ret = common::OB_SUCCESS;
  common::ObString udf_name;
  S schema;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
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
    udf_name = schema.get_udf_name_str();
    schema.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all udf schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve udf schemas succeed");
  }
  return ret;
}

RETRIEVE_SCHEMA_FUNC_DEFINE(dblink);

template <typename AT, typename TST>
int ObSchemaRetrieveUtils::retrieve_link_table_schema(
    const uint64_t tenant_id, AT& result, ObIAllocator& allocator, TST*& table_schema)
{
  int ret = common::OB_SUCCESS;
  bool is_deleted = false;
  TST tmp_table_schema;

  table_schema = NULL;
  SHARE_SCHEMA_LOG(INFO, "retrieve link table schema");
  if (OB_FAIL(result.next())) {
    if (ret == common::OB_ITER_END) {  // no record
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "no row", K(ret));
    } else {
      SHARE_SCHEMA_LOG(WARN, "get link table schema failed, iter quit", K(ret));
    }
  } else if (OB_FAIL(fill_link_table_schema(tenant_id, result, tmp_table_schema, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "fail to fill link table schema. ", K(is_deleted), K(ret));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_table_schema, table_schema))) {
    SHARE_SCHEMA_LOG(WARN, "alloc link table schema failed", K(ret));
  } else {
    // check if this is only one
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

template <typename T>
int ObSchemaRetrieveUtils::retrieve_link_column_schema(const uint64_t tenant_id, T& result, ObTableSchema& table_schema)
{
  int ret = common::OB_SUCCESS;
  bool is_deleted = false;
  ObColumnSchemaV2 column_schema;

  SHARE_SCHEMA_LOG(INFO, "retrieve link column schema");
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    column_schema.reset();
    if (OB_FAIL(fill_link_column_schema(tenant_id, result, column_schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill link column schema. ", K(is_deleted), K(ret));
    } else if (FALSE_IT(column_schema.set_table_id(table_schema.get_table_id()))) {
    } else if (OB_FAIL(table_schema.add_column(column_schema))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add link column schema. ", K(column_schema), K(ret));
    }
  }
  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve link column schema succeed");
  }
  return ret;
}

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_db_priv_schema(const uint64_t tenant_id, T& result, ObIArray<S>& db_priv_array)
{
  int ret = common::OB_SUCCESS;
  S db_priv;
  S prev_priv;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    if (OB_FAIL(fill_db_priv_schema(tenant_id, result, db_priv, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill database privileges", K(ret));
    } else if (prev_priv.get_original_key() == db_priv.get_original_key()) {
      // ignore it
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "db_priv is is_deleted", K(db_priv));
    } else if (OB_FAIL(db_priv_array.push_back(db_priv))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to push back", K(ret));
    }
    prev_priv = db_priv;
    db_priv.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get database privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_obj_priv_schema_inner(
    const uint64_t tenant_id, T& result, ObIArray<S>& obj_priv_array)
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
    } else if ((prev_priv.get_sort_key() == obj_priv.get_sort_key() && prev_priv_id == priv_id)) {
      // jump over same priv operation before , eg: revoke or add grant option
      ret = common::OB_SUCCESS;
    } else {
      // it's the first row
      SHARE_SCHEMA_LOG(TRACE, "get priv operation", K(priv_id), K(is_deleted));
      if (is_deleted) {
        // jump over revoke operation
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
      OX(prev_priv_id = priv_id);
      OX(prev_priv = obj_priv);
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get obj privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_sys_priv_schema_inner(
    const uint64_t tenant_id, T& result, ObIArray<S>& sys_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObRawPriv priv_id;
  ObRawPriv prev_priv_id = PRIV_ID_MAX;
  S prev_priv;
  S sys_priv;
  uint64_t option;
  ObPackedPrivArray packed_priv_array;
  bool is_deleted;

  /* gather sys priv by key <grantee_id, priv_id> */
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    if (OB_FAIL(fill_sys_priv_schema(tenant_id, result, sys_priv, is_deleted, priv_id, option))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill system privileges", K(ret));
    } else if ((prev_priv.get_key() == sys_priv.get_key() && prev_priv_id == priv_id)) {
      // jump over same priv operation before
      ret = common::OB_SUCCESS;
    } else {
      // it's the first row
      SHARE_SCHEMA_LOG(TRACE, "get priv operation", K(priv_id), K(is_deleted));
      if (is_deleted) {
        // jump over revoke operation
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
      OX(prev_priv_id = priv_id);
      OX(prev_priv = sys_priv);
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get system privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template <typename S>
int ObSchemaRetrieveUtils::push_prev_array_if_has(
    ObIArray<S>& sys_priv_array, S& sys_priv, ObPackedPrivArray& packed_grant_privs)
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

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_sys_priv_schema(const uint64_t tenant_id, T& result, ObIArray<S>& sys_priv_array)
{
  int ret = common::OB_SUCCESS;
  ObSEArray<S, 3> tmp_priv_array;
  S prev_priv;
  S it_priv;
  uint64_t option;
  bool is_first_group = true;
  ObPackedPrivArray packed_grant_privs;
  bool only_revoke = false;

  if (OB_FAIL(retrieve_sys_priv_schema_inner(tenant_id, result, tmp_priv_array))) {
    SHARE_SCHEMA_LOG(WARN, "retrieve_sys_priv_schema_inner failed", K(ret));
  } else {
    ARRAY_FOREACH(tmp_priv_array, i)
    {
      it_priv = tmp_priv_array.at(i);
      if (prev_priv.get_key() == it_priv.get_key()) {
        if (OB_FAIL(ObPrivPacker::merge_two_packed_array(packed_grant_privs, it_priv.get_priv_array()))) {
          SHARE_SCHEMA_LOG(WARN, "merg two packed array failed", K(ret));
        }
      } else {
        /* push back previous group */
        if (OB_FAIL(push_prev_array_if_has(sys_priv_array, prev_priv, packed_grant_privs))) {
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
    if (OB_FAIL(push_prev_array_if_has(sys_priv_array, prev_priv, packed_grant_privs))) {
      SHARE_SCHEMA_LOG(WARN, "push prev array if has failed", K(ret));
    }
  }
  return ret;
}

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_table_priv_schema(
    const uint64_t tenant_id, T& result, ObIArray<S>& table_priv_array)
{
  int ret = common::OB_SUCCESS;
  S table_priv;
  S prev_table_priv;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    if (OB_FAIL(fill_table_priv_schema(tenant_id, result, table_priv, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to fill table_priv", K(ret));
    } else if (table_priv.get_sort_key() == prev_table_priv.get_sort_key()) {
      // ignore it
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(TRACE, "table_priv is is_deleted", K(table_priv));
    } else if (OB_FAIL(table_priv_array.push_back(table_priv))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to push back", K(ret));
    }
    prev_table_priv = table_priv;
    table_priv.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "Fail to get table privileges. iter quit", K(ret));
  } else {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_obj_priv_schema(const uint64_t tenant_id, T& result, ObIArray<S>& obj_priv_array)
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
    ARRAY_FOREACH(tmp_obj_priv_array, i)
    {
      it_obj_priv = tmp_obj_priv_array.at(i);
      if (prev_obj_priv.get_sort_key() == it_obj_priv.get_sort_key()) {
        packed_obj_privs |= it_obj_priv.get_obj_privs();
      } else {
        /* push back previous group */
        if (OB_FAIL(push_prev_obj_privs_if_has(obj_priv_array, prev_obj_priv, packed_obj_privs))) {
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
    if (OB_FAIL(push_prev_obj_privs_if_has(obj_priv_array, prev_obj_priv, packed_obj_privs))) {
      SHARE_SCHEMA_LOG(WARN, "push prev obj privs if has failed", K(ret));
    }
  }
  return ret;
}

template <typename S>
int ObSchemaRetrieveUtils::push_prev_obj_privs_if_has(
    ObIArray<S>& obj_priv_array, S& obj_priv, ObPackedObjPriv& packed_obj_privs)
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

// for simple schemas

template <typename T>
int ObSchemaRetrieveUtils::fill_user_schema(
    const uint64_t tenant_id, T& result, ObSimpleUserSchema& user_schema, bool& is_deleted)
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, type, user_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_tenant_schema(T& result, ObSimpleTenantSchema& tenant_schema, bool& is_deleted)
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
    ObString default_tenant_status_str("TENANT_STATUS_NORMAL");
    int64_t default_in_recyclebin = 0;
    common::ObTimeZoneInfoWrap tz_info_wrap;
    GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(
        result.get_timestamp, "gmt_modified", gmt_modified, default_gmt_modified, tz_info_wrap.get_time_zone_info());
    tenant_schema.set_gmt_modified(gmt_modified);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tenant_schema, int64_t);
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, read_only, tenant_schema);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tenant_name, tenant_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "primary_zone", primary_zone_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "locality", locality_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "previous_locality", previous_locality_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        compatibility_mode,
        tenant_schema,
        common::ObCompatibilityMode,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_compat_mode);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_tenant_time,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_drop_tenant_time);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "status",
        tenant_status_str,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_tenant_status_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        in_recyclebin,
        tenant_schema,
        int64_t,
        skip_null_error,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_in_recyclebin);
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
  }
  SHARE_SCHEMA_LOG(INFO, "retrieve tenant schema", K(tenant_schema), K(is_deleted), K(ret));
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_table_schema(
    const uint64_t tenant_id, T& result, ObSimpleTableSchemaV2& table_schema, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_status,
        table_schema,
        ObPartitionStatus,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_schema_version,
        table_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, index_type, table_schema, ObIndexType);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, session_id, table_schema, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, table_mode, table_schema, int32_t, true, ObSchemaService::g_ignore_column_retrieve_error_, 0);
    ObPartitionOption& partition_option = table_schema.get_part_option();
    if (OB_SUCC(ret)) {
      ObString part_func_expr;
      ObString sub_part_func_expr;
      // part_expr
      int64_t part_num = 0;
      enum ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_HASH;
      EXTRACT_INT_FIELD_MYSQL(result, "part_func_type", part_func_type, ObPartitionFuncType);
      partition_option.set_part_func_type(part_func_type);
      EXTRACT_INT_FIELD_MYSQL(result, "part_num", part_num, int64_t);
      partition_option.set_part_num(part_num);
      int64_t partition_cnt_within_partition_table_default = -1;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          partition_cnt_within_partition_table,
          partition_option,
          int64_t,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          partition_cnt_within_partition_table_default);
      int64_t max_used_part_id_default = -1;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          max_used_part_id,
          partition_option,
          int64_t,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          max_used_part_id_default);
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_func_expr", part_func_expr);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_option.set_part_expr(part_func_expr))) {
          SHARE_SCHEMA_LOG(WARN, "set part expr failed", K(ret));
        }
      }
      // sub_part_expr
      ObPartitionOption& sub_part_option = table_schema.get_sub_part_option();
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
      ObString locality_default_val("");
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, locality, table_schema, true, ObSchemaService::g_ignore_column_retrieve_error_, locality_default_val);
      ObString previous_locality_default_val("");
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          previous_locality,
          table_schema,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          previous_locality_default_val);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(fill_replica_options(result, table_schema))) {
          SHARE_SCHEMA_LOG(WARN, "fill replica options failed", K(ret));
        } else if (OB_FAIL(fill_table_zone_region_replica_num_array(table_schema))) {
          SHARE_SCHEMA_LOG(WARN, "fill schema zone region replica dist failed", K(ret));
        } else {
        }  // good
      }
      const ObDuplicateScope duplicate_scope_default = ObDuplicateScope::DUPLICATE_SCOPE_NONE;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          duplicate_scope,
          table_schema,
          int64_t,
          true /* skip null error*/,
          ObSchemaService::g_ignore_column_retrieve_error_,
          duplicate_scope_default);
      const bool default_binding = false;
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          binding,
          table_schema,
          true /* skip null error*/,
          ObSchemaService::g_ignore_column_retrieve_error_,
          default_binding);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          drop_schema_version,
          table_schema,
          int64_t,
          true /* skip null error*/,
          ObSchemaService::g_ignore_column_retrieve_error_,
          common::OB_INVALID_VERSION);
      /*
       * __all_table_v2_history is added in ver 2.2.60. To avoid compatibility problems,
       * __all_table/__all_table_history/__all_table_v2/__all_table_v2_history should add columns in upgrade post stage
       * from ver 2.2.60. Here, we should ignore error because column is not exist when cluster is still in upgradation.
       */
      /* ver 2.2.60 */
      bool ignore_column_error =
          ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, is_sub_part_template, table_schema, bool, true /* skip null error*/, ignore_column_error, true);
      if (OB_SUCC(ret) && !table_schema.is_sub_part_template()) {
        table_schema.get_sub_part_option().set_part_num(0);
        table_schema.set_def_sub_part_num(0);
      }
      /* ver 3.1 */
      ignore_column_error =
          ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100;
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, auto_part, partition_option, true, ignore_column_error, false);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, auto_part_size, partition_option, int64_t, true, ignore_column_error, -1);
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_database_schema(
    const uint64_t tenant_id, T& result, ObSimpleDatabaseSchema& database_schema, bool& is_deleted)
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
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        database_schema,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);
    if (OB_SUCC(ret)) {
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, database_name, database_schema);
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_tablegroup_schema(
    const uint64_t tenant_id, T& result, ObSimpleTablegroupSchema& tablegroup_schema, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  tablegroup_schema.reset();
  is_deleted = false;
  tablegroup_schema.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, tablegroup_id, tablegroup_schema, tenant_id);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  if (!is_deleted) {
    ObString primary_zone_str;
    ObString locality_str;
    ObString previous_locality_str;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, tablegroup_schema, int64_t);
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, tablegroup_name, tablegroup_schema);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "primary_zone", primary_zone_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "locality", locality_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "previous_locality", previous_locality_str);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_status,
        tablegroup_schema,
        ObPartitionStatus,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        partition_schema_version,
        tablegroup_schema,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        0);
    const bool default_binding = false;
    EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        binding,
        tablegroup_schema,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_binding);
    if (OB_FAIL(tablegroup_schema.set_primary_zone(primary_zone_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set primary zone", K(ret), K(primary_zone_str));
    } else if (OB_FAIL(tablegroup_schema.set_locality(locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set locality", K(ret), K(locality_str));
    } else if (OB_FAIL(tablegroup_schema.set_previous_locality(previous_locality_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to set previous_locality", K(ret), K(previous_locality_str));
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_outline_schema(
    const uint64_t tenant_id, T& result, ObSimpleOutlineSchema& outline_schema, bool& is_deleted)
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
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sql_id,
        outline_schema,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        ObString::make_string(""));
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_synonym_schema(
    const uint64_t tenant_id, T& result, ObSimpleSynonymSchema& synonym_schema, bool& is_deleted)
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
  }
  return ret;
}

template <typename T>
T* ObSchemaRetrieveUtils::find_table_schema(const uint64_t table_id, ObArray<T*>& table_schema_array)
{
  T* table_schema = NULL;
  typename ObArray<T*>::iterator table_iter = table_schema_array.end();
  table_iter = std::lower_bound(table_schema_array.begin(), table_schema_array.end(), table_id, compare_table_id<T>);
  if (table_iter != table_schema_array.end()) {
    if (OB_ISNULL(*table_iter)) {
    } else if ((*table_iter)->get_table_id() == table_id) {
      table_schema = (*table_iter);
    }
  }
  return table_schema;
}

template <typename T, typename SCHEMA>
int ObSchemaRetrieveUtils::fill_replica_options(T& result, SCHEMA& schema)
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
      } else {
      }  // set primary zone array success
    } else {
    }  // empty primary zone, no need to check and parse
  }
  return ret;
}

template <typename T>
bool ObSchemaRetrieveUtils::compare_table_id(const T* table_schema, const uint64_t table_id)
{
  bool cmp = false;
  if (OB_ISNULL(table_schema)) {
    SHARE_SCHEMA_LOG(WARN, "table schema is NULL");
  } else {
    // order by table id desc, used in sort function, the tenant_id is desc too
    cmp = table_schema->get_table_id() > table_id;
  }
  return cmp;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_part_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObPartition& partition, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = false;
  bool is_subpart_template = true;
  if (OB_FAIL(fill_base_part_info(
          tenant_id, check_deleted, is_subpart_def, is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  } else if (!is_deleted) {
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, part_name, partition);
    const int64_t default_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, part_idx, partition, int64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, default_part_idx);
    const int64_t default_mapping_pg_part_id = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        mapping_pg_part_id,
        partition,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_mapping_pg_part_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        partition,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);

    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_num, partition, int64_t);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        max_used_sub_part_id,
        partition,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_ID);
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_def_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObSubPartition& partition, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = true;
  bool is_subpart_template = true;
  if (OB_FAIL(fill_base_part_info(
          tenant_id, check_deleted, is_subpart_def, is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  } else if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_id, partition, int64_t);
    const int64_t default_sub_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sub_part_idx,
        partition,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_sub_part_idx);
    ObString sub_part_name;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_name", sub_part_name);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.set_part_name(sub_part_name))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set part name", K(ret));
      }
    }
    const int64_t default_mapping_pg_sub_part_id = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        mapping_pg_sub_part_id,
        partition,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_mapping_pg_sub_part_id);
  } else {
  }  // do nothing
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_subpart_info(
    const uint64_t tenant_id, const bool check_deleted, T& result, ObSubPartition& partition, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  bool is_subpart_def = true;
  bool is_subpart_template = false;
  if (OB_FAIL(fill_base_part_info(
          tenant_id, check_deleted, is_subpart_def, is_subpart_template, result, partition, is_deleted))) {
    SHARE_SCHEMA_LOG(WARN, "Failed to fill base part info", K(ret));
  }
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, sub_part_id, partition, int64_t);
  if (OB_SUCC(ret) && !is_deleted) {
    const int64_t default_sub_part_idx = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        sub_part_idx,
        partition,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_sub_part_idx);
    ObString sub_part_name;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "sub_part_name", sub_part_name);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.set_part_name(sub_part_name))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set part name", K(ret));
      }
    }
    const int64_t default_mapping_pg_sub_part_id = -1;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        mapping_pg_sub_part_id,
        partition,
        int64_t,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_mapping_pg_sub_part_id);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        drop_schema_version,
        partition,
        int64_t,
        true /* skip null error*/,
        ObSchemaService::g_ignore_column_retrieve_error_,
        common::OB_INVALID_VERSION);
  } else {
  }  // do nothing
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_base_part_info(const uint64_t tenant_id, const bool check_deleted,
    const bool is_subpart_def, const bool is_subpart_template, T& result, ObBasePartition& partition, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  is_deleted = false;

  partition.set_tenant_id(tenant_id);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, table_id, partition, tenant_id);
  if (OB_FAIL(ret)) {
  } else if (!is_subpart_def && !is_subpart_template) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid arg", K(ret), K(is_subpart_def), K(is_subpart_template));
  } else if (!is_subpart_def) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_id, partition, int64_t);
    ObString resource_partition_str;
    ObString default_value("");
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
        "source_partition_id",
        resource_partition_str,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_value);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.set_source_part_id(resource_partition_str))) {
        SHARE_SCHEMA_LOG(WARN, "fail to set resource partition id", K(ret), K(resource_partition_str));
      }
    }
  } else if (!is_subpart_template) {
    // for subpart non-template
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, part_id, partition, int64_t);
  } else {
    // for subpart template, we set it's part_id explicitly
    partition.set_part_id(ObSubPartition::TEMPLATE_PART_ID);
  }

  // part_idx

  if (check_deleted) {
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  }
  if (!is_deleted) {
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, schema_version, partition, int64_t);
    ObString bhigh_bound_val;
    ObString blist_val;

    if (OB_SUCC(ret)) {
      ret = result.get_varchar("b_high_bound_val", bhigh_bound_val);
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
        // do nothing
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
        // do nothing
      } else if (OB_ERR_COLUMN_NOT_FOUND == ret && ObSchemaService::g_ignore_column_retrieve_error_) {
        // ignore error that column is not exist when schema service runs in liboblog
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        SQL_LOG(WARN, "fail to get varchar column 'b_list_val' of base_part_info.", K(ret));
      } else if (OB_FAIL(partition.set_list_vector_values_with_hex_str(blist_val))) {
        SHARE_SCHEMA_LOG(WARN, "Failed to set list val to partition", K(ret));
      }
    }
  } else {
  }  // do nothing
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_aux_tables(
    const uint64_t tenant_id, T& result, ObIArray<ObAuxTableMetaInfo>& aux_tables)
{
  int ret = common::OB_SUCCESS;

  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    uint64_t table_id = OB_INVALID_ID;
    ObTableType table_type = MAX_TABLE_TYPE;
    int64_t drop_schema_version = OB_INVALID_VERSION;
    EXTRACT_INT_FIELD_MYSQL(result, "table_type", table_type, ObTableType);
    if (USER_INDEX == table_type || AUX_VERTIAL_PARTITION_TABLE == table_type) {
      EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, "table_id", table_id, tenant_id);
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result,
          "drop_schema_version",
          drop_schema_version,
          int64_t,
          true /* skip null error*/,
          ObSchemaService::g_ignore_column_retrieve_error_,
          common::OB_INVALID_VERSION);
      ObAuxTableMetaInfo aux_table_meta(table_id, table_type, drop_schema_version);
      SHARE_SCHEMA_LOG(DEBUG, "dump aux table", K(aux_table_meta), K(table_type), K(drop_schema_version));
      ret = aux_tables.push_back(aux_table_meta);
    }
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get aux table. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve aux table finish");
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_schema_version(T& result, VersionHisVal& version_his_val)
{
  int ret = common::OB_SUCCESS;
  int& row_idx = version_his_val.valid_cnt_;
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
    SHARE_SCHEMA_LOG(INFO, "retrieve schema version");
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_foreign_key_info(const uint64_t tenant_id, T& result, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObForeignKeyInfo foreign_key_info;
  uint64_t prev_foreign_key_id = common::OB_INVALID_ID;
  bool is_deleted = false;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    foreign_key_info.reset();
    if (OB_FAIL(fill_foreign_key_info(tenant_id, table_schema.get_table_id(), result, foreign_key_info, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill foreign key info", K(ret));
    } else if (foreign_key_info.foreign_key_id_ == prev_foreign_key_id) {
      ret = common::OB_SUCCESS;
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "foreign key is is_deleted");
    } else if (OB_FAIL(table_schema.add_foreign_key_info(foreign_key_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add foreign key info", K(ret), K(foreign_key_info));
    }
    prev_foreign_key_id = foreign_key_info.foreign_key_id_;
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get foreign key schema. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve foreign key schema");
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::retrieve_foreign_key_column_info(
    const uint64_t tenant_id, T& result, ObForeignKeyInfo& foreign_key_info)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  int64_t prev_child_column_id = 0;  // OB_INVALID_ID;
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
    SHARE_SCHEMA_LOG(INFO, "retrieve foreign key");
    ret = common::OB_SUCCESS;
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_foreign_key_info(
    const uint64_t tenant_id, uint64_t table_id, T& result, ObForeignKeyInfo& foreign_key_info, bool& is_deleted)
{
  int ret = common::OB_SUCCESS;
  table_id = combine_id(tenant_id, table_id);
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
      const bool default_validate_flag = true;
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, child_table_id, foreign_key_info, tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, parent_table_id, foreign_key_info, tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, update_action, foreign_key_info, ObReferenceAction);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, delete_action, foreign_key_info, ObReferenceAction);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, foreign_key_name, foreign_key_info);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          rely_flag,
          foreign_key_info,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          default_rely_flag);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          enable_flag,
          foreign_key_info,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          default_enable_flag);
      EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          validate_flag,
          foreign_key_info,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          default_validate_flag);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
          ref_cst_type,
          foreign_key_info,
          ObConstraintType,
          true,
          ObSchemaService::g_ignore_column_retrieve_error_,
          0);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
          result, ref_cst_id, foreign_key_info, uint64_t, true, ObSchemaService::g_ignore_column_retrieve_error_, -1);
    }
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_foreign_key_column_info(
    const uint64_t tenant_id, T& result, int64_t& child_column_id, int64_t& parent_column_id, bool& is_deleted)
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

template <typename T>
int ObSchemaRetrieveUtils::retrieve_simple_foreign_key_info(
    const uint64_t tenant_id, T& result, ObArray<ObSimpleTableSchemaV2*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_foreign_key_id = common::OB_INVALID_ID;
  uint64_t fk_id = common::OB_INVALID_ID;
  uint64_t table_id = common::OB_INVALID_ID;
  ObString fk_name;
  ObSimpleTableSchemaV2* table_schema_ptr = nullptr;

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
      SHARE_SCHEMA_LOG(INFO, "foreign key is deleted");
    } else if (table_id == common::OB_INVALID_ID ||
               OB_ISNULL(table_schema_ptr = ObSchemaRetrieveUtils::find_table_schema(table_id, table_schema_array))) {
      SHARE_SCHEMA_LOG(WARN, "fail to find table schema by table id", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema_ptr->add_simple_foreign_key_info(table_schema_ptr->get_tenant_id(),
                   table_schema_ptr->get_database_id(),
                   table_schema_ptr->get_table_id(),
                   fk_id,
                   fk_name))) {
      SHARE_SCHEMA_LOG(WARN, "fail to add simple foreign key info", K(ret), K(fk_id), K(fk_name));
    }
    prev_foreign_key_id = fk_id;
  }
  if (ret != OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get simple foreign key info. iter quit. ", K(ret));
  } else {
    SHARE_SCHEMA_LOG(INFO, "retrieve simple foreign key info");
    ret = OB_SUCCESS;
  }

  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::get_foreign_key_id_and_name(
    const uint64_t tenant_id, T& result, bool& is_deleted, uint64_t& fk_id, ObString& fk_name, uint64_t& table_id)
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

template <typename T>
int ObSchemaRetrieveUtils::retrieve_simple_constraint_info(
    const uint64_t tenant_id, T& result, ObArray<ObSimpleTableSchemaV2*>& table_schema_array)
{
  int ret = OB_SUCCESS;
  bool is_deleted = false;
  uint64_t prev_table_id = common::OB_INVALID_ID;
  uint64_t prev_constraint_id = common::OB_INVALID_ID;
  uint64_t cst_id = common::OB_INVALID_ID;
  uint64_t table_id = common::OB_INVALID_ID;
  ObString cst_name;
  ObConstraintType cst_type = CONSTRAINT_TYPE_INVALID;
  ObSimpleTableSchemaV2* table_schema_ptr = nullptr;

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
      SHARE_SCHEMA_LOG(INFO, "constraint is deleted");
    } else if (table_id == common::OB_INVALID_ID ||
               OB_ISNULL(table_schema_ptr = ObSchemaRetrieveUtils::find_table_schema(table_id, table_schema_array))) {
      SHARE_SCHEMA_LOG(WARN, "fail to find table schema by table id", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema_ptr->add_simple_constraint_info(table_schema_ptr->get_tenant_id(),
                   table_schema_ptr->get_database_id(),
                   table_schema_ptr->get_table_id(),
                   cst_id,
                   cst_name))) {
      SHARE_SCHEMA_LOG(WARN,
          "fail to add simple constraint info",
          K(ret),
          K(table_schema_ptr->get_tenant_id()),
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
    SHARE_SCHEMA_LOG(INFO, "retrieve simple constraint info");
    ret = OB_SUCCESS;
  }

  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::get_constraint_id_and_name(const uint64_t tenant_id, T& result, bool& is_deleted,
    uint64_t& cst_id, ObString& cst_name, uint64_t& table_id, ObConstraintType& cst_type)
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

template <typename T>
int ObSchemaRetrieveUtils::retrieve_drop_tenant_infos(T& result, ObIArray<ObDropTenantInfo>& drop_tenant_infos)
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

template <typename T, typename S>
int ObSchemaRetrieveUtils::retrieve_profile_schema(const uint64_t tenant_id, T& result, ObIArray<S>& schema_array)
{
  int ret = common::OB_SUCCESS;
  uint64_t prev_id = common::OB_INVALID_ID;
  S schema;
  while (OB_SUCCESS == ret && common::OB_SUCCESS == (ret = result.next())) {
    bool is_deleted = false;
    if (OB_FAIL(fill_profile_schema(tenant_id, result, schema, is_deleted))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fill profile schema ", K(ret));
    } else if (schema.get_profile_id() == prev_id) {
      SHARE_SCHEMA_LOG(
          DEBUG, "hualong debug ignore", "id", schema.get_profile_id(), "version", schema.get_schema_version());
    } else if (is_deleted) {
      SHARE_SCHEMA_LOG(INFO, "profile is is_deleted, don't add", "profile_id", schema.get_profile_id());
    } else if (OB_FAIL(schema_array.push_back(schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to push back", K(ret));
    } else {
      SHARE_SCHEMA_LOG(INFO, "retrieve profile schema succeed", K(schema));
    }
    prev_id = schema.get_profile_id();
    schema.reset();
  }
  if (ret != common::OB_ITER_END) {
    SHARE_SCHEMA_LOG(WARN, "fail to get all profile schema. iter quit. ", K(ret));
  } else {
    ret = common::OB_SUCCESS;
    SHARE_SCHEMA_LOG(INFO, "retrieve profile schemas succeed");
  }
  return ret;
}

template <typename T>
int ObSchemaRetrieveUtils::fill_profile_schema(
    const uint64_t tenant_id, T& result, ObProfileSchema& profile_schema, bool& is_deleted)
{
  profile_schema.reset();
  is_deleted = false;
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
    EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result,
        password_verify_function,
        profile_schema,
        true,
        ObSchemaService::g_ignore_column_retrieve_error_,
        default_password_verify_function);
    // __all_tenant_profile is a new table in 2230, adding column actions should be in post stage.
    // Ignore column error according to cluster version
    bool ignore_column_error =
        ObSchemaService::g_ignore_column_retrieve_error_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276;
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, password_life_time, profile_schema, int64_t, false, ignore_column_error, INT64_MAX);
    EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        result, password_grace_time, profile_schema, int64_t, false, ignore_column_error, INT64_MAX);
  }
  return ret;
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
