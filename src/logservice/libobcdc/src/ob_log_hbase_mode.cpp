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
 *
 * OBCDC HBase Util
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_hbase_mode.h"                       // ObLogHbaseUtil

#include "share/schema/ob_table_schema.h"            // ObTableSchema, ObColumnIterByPrevNextID
#include "share/schema/ob_column_schema.h"           // ObColumnSchemaV2

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace libobcdc
{

ObLogHbaseUtil::ObLogHbaseUtil() :
    inited_(false),
    table_id_set_(),
    column_id_map_()
{}

ObLogHbaseUtil::~ObLogHbaseUtil()
{
  destroy();
}

int ObLogHbaseUtil::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(table_id_set_.create(DEFAULT_TABLE_SET_SIZE))) {
    LOG_ERROR("table_id_set_ create fail", KR(ret));
  } else if (OB_FAIL(column_id_map_.init(ObModIds::OB_LOG_HBASE_COLUMN_ID_MAP))) {
    LOG_ERROR("init column_id_map_ fail", KR(ret));
  } else {
    LOG_INFO("hbase_util init succ");
    inited_ = true;
  }

  return ret;
}

void ObLogHbaseUtil::destroy()
{
  inited_ = false;

  table_id_set_.destroy();
  column_id_map_.destroy();
}

int ObLogHbaseUtil::add_hbase_table_id(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  bool is_hbase_mode_table = false;
  const uint64_t table_id = table_schema.get_table_id();
  const char *table_name = table_schema.get_table_name();

  if (OB_FAIL(filter_hbase_mode_table_(table_schema, is_hbase_mode_table))) {
    LOG_ERROR("filter_hbase_mode_table_ fail", KR(ret), K(table_id), K(table_name), K(is_hbase_mode_table));
  } else if (! is_hbase_mode_table) {
    LOG_INFO("[IS_NOT_HBASE_TABLE]", K(table_name), K(table_id), K(is_hbase_mode_table));
  } else if (OB_FAIL(table_id_set_.set_refactored(table_id))) {
    LOG_ERROR("add_table_id into table_id_set_ fail", KR(ret), K(table_name), K(table_id));
  } else {
    LOG_INFO("[HBASE] add_table_id into table_id_set_ succ", K(table_name), K(table_id));
  }

  return ret;
}

int ObLogHbaseUtil::filter_hbase_mode_table_(const ObTableSchema &table_schema,
    bool &is_hbase_mode_table)
{
  int ret = OB_SUCCESS;

  is_hbase_mode_table = false;
  // Marks the presence or absence of a specified column
  int column_flag[HBASE_TABLE_COLUMN_COUNT];
  // Mark column T as bigint or not
  bool is_T_column_bigint_type = false;
  // Record T-column id
  uint64_t column_id = OB_INVALID_ID;
  memset(column_flag, '\0', sizeof(column_flag));
  ObColumnIterByPrevNextID pre_next_id_iter(table_schema);

  while (OB_SUCCESS == ret) {
    const ObColumnSchemaV2 *column_schema = NULL;

    if (OB_FAIL(pre_next_id_iter.next(column_schema))) {
      if (OB_ITER_END != ret) {
        LOG_ERROR("pre_next_id_iter next fail", KR(ret), KPC(column_schema));
      }
    } else if (OB_ISNULL(column_schema)) {
      LOG_ERROR("column_schema is null", KPC(column_schema));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const char *column_name = column_schema->get_column_name();

      if (0 == strcmp(column_name, K_COLUMN)) {
        column_flag[0]++;
      } else if (0 == strcmp(column_name, Q_COLUMN)) {
        column_flag[1]++;
      } else if (0 == strcmp(column_name, T_COLUMN)) {
        column_flag[2]++;

        if (ObIntType == column_schema->get_data_type()) {
          is_T_column_bigint_type = true;
          column_id = column_schema->get_column_id();
        }
      } else if (0 == strcmp(column_name, V_COLUMN)) {
        column_flag[3]++;
      }
    }
  } // while

  // Iterate through all columns
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  int64_t hbase_table_column_cnt = 0;
  // check contains four columns K, Q, T, V
  for (int64_t idx=0; idx < HBASE_TABLE_COLUMN_COUNT && OB_SUCC(ret); ++idx) {
    if (1 == column_flag[idx]) {
      ++hbase_table_column_cnt;
    }
  }

  if (OB_SUCC(ret)) {
    if ((HBASE_TABLE_COLUMN_COUNT == hbase_table_column_cnt)
        && is_T_column_bigint_type) {
      is_hbase_mode_table = true;

      TableID table_key(table_schema.get_table_id());
      if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
        LOG_ERROR("column_id is not valid", K(column_id));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(column_id_map_.insert(table_key, column_id))) {
        LOG_ERROR("column_id_map_ insert fail", KR(ret), K(table_key), K(column_id));
      } else {
        // succ
      }
    } else {
      is_hbase_mode_table = false;
    }
  }

  LOG_INFO("[HBASE] table info", "table_id", table_schema.get_table_id(),
      "table_name", table_schema.get_table_name(),
      K(hbase_table_column_cnt),
      K(column_id), K(is_T_column_bigint_type),
      K(is_hbase_mode_table));

  return ret;
}

int ObLogHbaseUtil::judge_hbase_T_column(const uint64_t table_id,
    const uint64_t column_id,
    bool &chosen)
{
  int ret = OB_SUCCESS;
  chosen = false;

  if (OB_FAIL(table_id_set_.exist_refactored(table_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;

      // Table exists to determine if it is a T column
      TableID table_key(table_id);
      uint64_t T_column_id = OB_INVALID_ID;

      if (OB_FAIL(column_id_map_.get(table_key, T_column_id))) {
        LOG_ERROR("get column_id from map fail", KR(ret), K(table_key), K(T_column_id));
      } else if (OB_UNLIKELY(OB_INVALID_ID == T_column_id)) {
        LOG_ERROR("T_column_id is not valid", K(T_column_id));
        ret = OB_ERR_UNEXPECTED;
      } else if (column_id == T_column_id) {
        chosen = true;
      } else {
        chosen = false;
      }
    } else if (OB_HASH_NOT_EXIST == ret) {
      chosen = false;
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("table_id_set_ exist_refactored fail", KR(ret), K(table_id));
    }
  }

  return ret;
}

int ObLogHbaseUtil::is_hbase_table(const uint64_t table_id,
    bool &chosen)
{
  int ret = OB_SUCCESS;
  chosen = false;

  if (OB_FAIL(table_id_set_.exist_refactored(table_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      chosen = true;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      chosen = false;
    } else {
      LOG_ERROR("table_id_set_ exist_refactored fail", KR(ret), K(table_id));
    }
  }

  return ret;
}

}
}
