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

#define USING_LOG_PREFIX SHARE

#include "share/ob_core_table_proxy.h"

#include <algorithm>
#include "lib/container/ob_array_helper.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "inner_table/ob_inner_table_schema.h"
#include "share/ob_debug_sync.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/ob_tenant_id_schema_version.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;

namespace share
{

int ObCoreTableProxy::Row::init(const int64_t row_id,
    ObCoreTableProxy &kv_proxy, const common::ObIArray<Cell> &cells)
{
  // allow init twice
  reset();
  int ret = OB_SUCCESS;
  if (row_id < 0 || !kv_proxy.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id), K(kv_proxy));
  } else  {
    row_id_ = row_id;
    kv_proxy_ = &kv_proxy;

    if (OB_FAIL(extend_cell_array(cells.count()))) {
      LOG_WARN("extend cell array failed", K(ret));
    } else {
      cell_cnt_ = 0;
      FOREACH_CNT(c, cells) {
        cells_[cell_cnt_++] = *c;
      }
      if (cell_cnt_ > 0) {
        std::sort(cells_, cells_ + cell_cnt_);
      }
      inited_ = true;
    }
  }
  return ret;
}

int ObCoreTableProxy::Row::get_int(const char *name, int64_t &value) const
{
  int ret = OB_SUCCESS;
  Cell *cell = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(name));
  } else if (OB_FAIL(get_cell(name, cell))) {
    // return NULL value for not found
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      LOG_WARN("find cell failed", K(ret), K(name));
    }
  } else if (NULL == cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL cell", K(ret), K(name));
  } else if (NULL == cell->value_.ptr()) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    char *endptr = NULL;
    value = strtoll(cell->value_.ptr(), &endptr, 0);
    if (cell->value_.empty() || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("cell not int value", K(ret), "cell", *cell);
    }
  }
  return ret;
}

int ObCoreTableProxy::Row::get_uint(const char *name, uint64_t &value) const
{
  int ret = OB_SUCCESS;
  Cell *cell = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(name));
  } else if (OB_FAIL(get_cell(name, cell))) {
    // return NULL value for not found
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      LOG_WARN("find cell failed", K(ret), K(name));
    }
  } else if (NULL == cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL cell", K(ret), K(name));
  } else if (NULL == cell->value_.ptr()) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    char *endptr = NULL;
    value = strtoull(cell->value_.ptr(), &endptr, 0);
    if (cell->value_.empty() || '\0' != *endptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("cell not int value", K(ret), "cell", *cell);
    }
  }
  return ret;
}

int ObCoreTableProxy::Row::get_varchar(const char *name, ObString &value) const
{
  int ret = OB_SUCCESS;
  Cell *cell = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(name));
  } else if (OB_FAIL(get_cell(name, cell))) {
    // return NULL value for not found
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      LOG_WARN("find cell failed", K(ret), K(name));
    }
  } else if (NULL == cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL cell", K(ret), K(name));
  } else if (NULL == cell->value_.ptr()) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    value  = cell->value_;
  }
  return ret;
}

int ObCoreTableProxy::Row::get_timestamp(const char *name, const common::ObTimeZoneInfo *tz_info, int64_t &value) const
{
  int ret = OB_SUCCESS;
  Cell *cell = NULL;
  const char *special_column = "gmt_modified";
  ObTimeConvertCtx cvrt_ctx(tz_info, true);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(name));
  } else if (strncmp(special_column, name, strlen(name))) {
    // gmt_modified is special treatment, get modify_time_us of __all_core_table
    const char *change_to_column = "modify_time_us";
    if (OB_FAIL(get_int(change_to_column, value))) {
      LOG_WARN("get gmt_modified fail", K(ret), K(name), K(value));
    }
  } else if (OB_FAIL(get_cell(name, cell))) {
    // return NULL value for not found
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      LOG_WARN("find cell failed", K(ret), K(name));
    }
  } else if (NULL == cell) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL cell", K(ret), K(name));
  } else if (NULL == cell->value_.ptr()) {
    ret = OB_ERR_NULL_VALUE;
  } else if (OB_FAIL(ObTimeConverter::str_to_datetime(cell->value_, cvrt_ctx, value, NULL))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timestamp format", K(ret), "datetime_str", cell->value_, K(value));
  }
  return ret;
}

int ObCoreTableProxy::Row::get_bool(const char *name, bool &value) const
{
  int ret = OB_SUCCESS;
  int64_t int_val = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(name));
  } else if (OB_FAIL(get_int(name, int_val))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get_int failed", K(name), K(ret));
    }
  } else {
    value = int_val;
  }
  return ret;
}

int ObCoreTableProxy::Row::get_cell(const char *name, ObCoreTableProxy::Cell *&cell) const
{
  cell = NULL;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name) {
    LOG_WARN("invalid argument", K(ret), K(name));
  } else if (OB_FAIL(get_cell(ObString::make_string(name), cell))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get cell failed", K(ret), K(name));
    }
  }
  return ret;
}

int ObCoreTableProxy::Row::get_cell(const ObString &name, ObCoreTableProxy::Cell *&cell) const
{
  cell = NULL;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == name.ptr()) {
    LOG_WARN("invalid argument", K(ret), K(name));
  } else if (cell_cnt_ <= 0 || NULL == cells_) {
    ret = OB_ENTRY_NOT_EXIST;
    // do nothing for empty row
  } else {
    Cell find_cell;
    find_cell.name_ = name;

    cell = std::lower_bound(cells_, cells_ + cell_cnt_, find_cell);
    if (cell == (cells_ + cell_cnt_) || cell->name_ != find_cell.name_) {
      ret = OB_ENTRY_NOT_EXIST;
      cell = NULL;
    }
  }

  return ret;
}

int ObCoreTableProxy::Row::extend_cell_array(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  // called in init() func, do not check inited_ flag.
  if (cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cnt));
  } else {
    if (cnt > cell_cnt_) {
      Cell *new_cells = static_cast<Cell *>(kv_proxy_->allocator_.alloc(sizeof(Cell) * cnt));
      if (NULL == new_cells) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc cell array failed", K(ret), "size", sizeof(Cell) * cnt);
      } else {
        for (int64_t i = 0; i < cnt; ++i) {
          if (i < cell_cnt_) {
            new (&new_cells[i]) Cell(cells_[i]);
            cells_[i].~Cell();
          } else {
            new (&new_cells[i]) Cell();
          }
        }
        if (NULL != cells_) {
          kv_proxy_->allocator_.free(cells_);
        }
        cells_ = new_cells;
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::Row::update_cell(const Cell &cell)
{
  int ret = OB_SUCCESS;
  Cell new_cell;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!cell.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cell));
  } else if (OB_FAIL(kv_proxy_->store_cell(cell, new_cell))) {
    LOG_WARN("store cell failed", K(ret));
  } else {
    Cell *c = NULL;
    if (OB_FAIL(get_cell(cell.name_.ptr(), c))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get cell failed", K(ret));
      } else { // not found
        LOG_WARN("update cell not found in row, we ignore this and continue",
            K(cell), K_(row_id), "table_name", kv_proxy_->table_name_);
        if (OB_FAIL(extend_cell_array(cell_cnt_ + 1))) {
          LOG_WARN("extend cell array failed", K(ret), K_(cell_cnt));
        } else {
          c = std::lower_bound(cells_, cells_ + cell_cnt_, new_cell);
          for (Cell *p = cells_ + cell_cnt_; p > c; --p) {
            *p = *(p - 1);
          }
          ++cell_cnt_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      *c = new_cell;
    }
  }
  return ret;
}

DEF_TO_STRING(ObCoreTableProxy::Row)
{
  int64_t pos = 0;
  J_OBJ_START();
  BUF_PRINTF("\"row_id:%ld, \"cells\":", row_id_);
  J_ARRAY_START();
  for (int64_t i = 0; i < cell_cnt_; ++i) {
    BUF_PRINTO(cells_[i]);
    J_COMMA();
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

ObCoreTableProxy::ObCoreTableProxy(
  const char *table_name,
  ObISQLClient &sql_client,
  const uint64_t tenant_id)
  : table_name_(table_name), sql_client_(&sql_client), load_for_update_(false),
    cur_idx_(-1), all_row_(), allocator_(ObModIds::OB_CORE_TABLE_PROXY), tenant_id_(tenant_id)
{
}

ObCoreTableProxy::~ObCoreTableProxy()
{
}

int ObCoreTableProxy::load()
{
  const bool for_update = false;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (OB_FAIL(load(for_update))) {
    LOG_WARN("load failed", K(ret), K(for_update));
  }
  return ret;
}

int ObCoreTableProxy::load_for_update()
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (OB_FAIL(load(for_update))) {
    LOG_WARN("load failed", K(ret), K(for_update));
  }
  return ret;
}

int ObCoreTableProxy::load_gmt_create(const char *column_name, int64_t &gmt_create_value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;

    if (OB_ISNULL(column_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(column_name));
    } else if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) FROM %s "
        "WHERE table_name = '%s' and column_name = '%s' ",
        OB_ALL_CORE_TABLE_TNAME, table_name_, column_name))) {
      LOG_WARN("assign sql failed", KR(ret), K(column_name), K(sql));
    } else if (OB_FAIL(sql_client_->read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        LOG_WARN("query empty result, fail to load gmt_create column", KR(ret), K(table_name_),
            K(column_name), K(sql));
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("next_result failed", KR(ret), K(table_name_), K(column_name), K(sql));
      }
    } else if (OB_FAIL(result->get_int(0L, gmt_create_value))) {
      LOG_WARN("get column fail", KR(ret), K(sql));
    }

  }
  return ret;
}

int ObCoreTableProxy::load(const bool for_update)
{
  all_row_.reset();
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KPC(this));
    } else if (OB_FAIL(sql.assign_fmt("SELECT row_id, column_name, column_value "
        "FROM %s WHERE table_name = '%s' ORDER BY row_id, column_name%s",
        OB_ALL_CORE_TABLE_TNAME, table_name_, for_update ? " FOR UPDATE" : ""))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret), K(sql));
    } else {
      int64_t row_id = OB_INVALID_INDEX;
      ObArray<Cell> cells;
      while (OB_SUCCESS == ret && (OB_SUCC(result->next()))) {

        int64_t cur_row_id = OB_INVALID_INDEX;
        Cell cell;
        int64_t idx = 0;
        ret = GET_COL_IGNORE_NULL(result->get_int, idx++, cur_row_id);
        ret = GET_COL_IGNORE_NULL(result->get_varchar, idx++, cell.name_);
        ret = GET_COL_IGNORE_NULL(result->get_varchar, idx++, cell.value_);
        if (OB_FAIL(ret)) {
        } else {
          if (OB_INVALID_INDEX == cur_row_id || !cell.is_valid()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid row_id or name", K(ret), K(cur_row_id), K(cell));
          }
        }

        if (OB_SUCC(ret)) {
          if (cur_row_id != row_id && OB_INVALID_INDEX != row_id) {
            if (OB_FAIL(add_row(row_id, cells))) {
              LOG_WARN("add row failed", K(ret), K(row_id), K(cells));
            } else {
              cells.reuse();
            }
          }
        }
        if (OB_SUCC(ret)) {
          row_id = cur_row_id;
          Cell stored_cell;
          if (OB_FAIL(store_cell(cell, stored_cell))) {
            LOG_WARN("store cell failed", K(ret), K(cell));
          } else if (OB_FAIL(cells.push_back(stored_cell))) {
            LOG_WARN("add cell failed", K(ret));
          }
        }
      }
      if (OB_SUCCESS != ret && OB_ITER_END != ret) {
          LOG_WARN("get result failed", K(ret), K(lbt()));
      } else {
        ret = OB_SUCCESS;
        if (OB_INVALID_INDEX != row_id) {
          if (OB_FAIL(add_row(row_id, cells))) {
            LOG_WARN("add row failed", K(ret), K(row_id), K(cells));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(seek_to_head())) {
        LOG_WARN("seek to head failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      load_for_update_ = for_update;
    }
  }
  return ret;
}

int ObCoreTableProxy::seek_to_head()
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else {
    cur_idx_ = -1;
  }
  return ret;
}

int ObCoreTableProxy::add_row(const int64_t row_id, const ObIArray<Cell> &cells)
{
  int ret = OB_SUCCESS;
  Row row;
  if (!is_valid() || row_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(row_id));
  } else if (OB_FAIL(row.init(row_id, *this, cells))) {
    LOG_WARN("init row failed", K(ret), K(row_id), K(cells));
  } else if (OB_FAIL(all_row_.push_back(row))) {
    LOG_WARN("add row failed", K(ret));
  }
  return ret;
}

int ObCoreTableProxy::next()
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (cur_idx_ < -1 || cur_idx_ >= all_row_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current index", K(ret), K_(cur_idx));
  } else {
    if (cur_idx_ + 1 >= all_row_.count()) {
      ret = OB_ITER_END;
    } else {
      ++cur_idx_;
    }
  }
  return ret;
}

int ObCoreTableProxy::get_cur_row(const Row *&row) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (cur_idx_ < 0 || cur_idx_ >= all_row_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current index, may be next() not invoked",
        K(ret), K_(cur_idx));
  } else {
    row = &all_row_.at(cur_idx_);
  }
  return ret;
}

int ObCoreTableProxy::get_int(const char *name, int64_t &value) const
{
  int ret = OB_SUCCESS;
  const Row *row = NULL;
  if (!is_valid() || NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(name));
  } else if (OB_FAIL(get_cur_row(row))) {
    LOG_WARN("get current row failed", K(ret));
  } else if (NULL == row) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL row", K(ret));
  } else if (OB_FAIL(row->get_int(name, value))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get int value failed", K(ret), K(name), "row", *row);
    }
  }
  return ret;
}

int ObCoreTableProxy::get_uint(const char *name, uint64_t &value) const
{
  int ret = OB_SUCCESS;
  const Row *row = NULL;
  if (!is_valid() || NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(name));
  } else if (OB_FAIL(get_cur_row(row))) {
    LOG_WARN("get current row failed", K(ret));
  } else if (NULL == row) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL row", K(ret));
  } else if (OB_FAIL(row->get_uint(name, value))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get int value failed", K(ret), K(name), "row", *row);
    }
  }
  return ret;
}

int ObCoreTableProxy::get_varchar(const char *name, common::ObString &value) const
{
  int ret = OB_SUCCESS;
  const Row *row = NULL;
  if (!is_valid() || NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(name));
  } else if (OB_FAIL(get_cur_row(row))) {
    LOG_WARN("get current row failed", K(ret));
  } else if (NULL == row) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL row", K(ret));
  } else if (OB_FAIL(row->get_varchar(name, value))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get varchar value failed", K(ret), K(name), "row", *row);
    }
  }

  return ret;
}

int ObCoreTableProxy::get_timestamp(const char *name, const common::ObTimeZoneInfo *tz_info, int64_t &value) const
{
  int ret = OB_SUCCESS;
  const Row *row = NULL;
  if (!is_valid() || NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(name));
  } else if (OB_FAIL(get_cur_row(row))) {
    LOG_WARN("get current row failed", K(ret));
  } else if (NULL == row) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL row", K(ret));
  } else if (OB_FAIL(row->get_timestamp(name, tz_info, value))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get timestamp value failed", K(ret), K(name), "row", *row);
    }
  }
  return ret;
}

//if the row not exist, execute insert;
//otherwise update the row while the new is large than the old value of the row.
int ObCoreTableProxy::incremental_update(const ObIArray<UpdateCell> &cells,
                                         const bool insert,
                                         int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             KPC(this), "cell count", cells.count());
  } else if (!load_for_update_) {
    ret = OB_ERR_SYS;
    LOG_WARN("table not loaded for update", K(ret));
  } else {
    affected_rows = 0;
    int64_t match_rows = 0;
    FOREACH_X(row, all_row_, OB_SUCCESS == ret) {
      bool match = false;
      if (OB_FAIL(check_row_match(*row, cells, match))) {
        LOG_WARN("check row match failed", K(ret), "row", *row, K(cells));
      } else {
        if (match) {
          match_rows++;
          if (OB_FAIL(execute_incremental_update_sql(*row, cells, affected_rows))) {
            LOG_WARN("execute update sql failed", K(ret));
          } else if (OB_FAIL(update_row_struct(cells, *row))) {
            LOG_WARN("update row failed", K(ret), "row", *row);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == match_rows && insert) {
        Row insert_row;
        int64_t new_row_id = 0;
        if (OB_FAIL(generate_row_id(new_row_id))) {
          LOG_WARN("generate row id failed", K(ret));
        } else {
          ObArray<Cell> empty_cells;
          if (OB_FAIL(insert_row.init(new_row_id, *this, empty_cells))) {
            LOG_WARN("init row failed", K(ret), K(new_row_id));
          }
        }
        ObArray<Cell> new_cells;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(execute_update_sql(insert_row, cells, affected_rows))) {
          LOG_WARN("execute update sql failed", K(ret));
        } else {
          FOREACH_CNT_X(c, cells, OB_SUCCESS == ret) {
            if (OB_FAIL(new_cells.push_back(c->cell_))) {
              LOG_WARN("add cell failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          std::sort(new_cells.begin(), new_cells.end());
          if (OB_FAIL(add_row(new_row_id, new_cells))) {
            LOG_WARN("add row failed", K(ret));
          }
        }
      }
    }
  }
  return ret;

}

int ObCoreTableProxy::update(const ObIArray<UpdateCell> &cells,
    const bool insert, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
        KPC(this), "cell count", cells.count());
  } else if (!load_for_update_) {
    ret = OB_ERR_SYS;
    LOG_WARN("table not loaded for update", K(ret));
  } else {
    affected_rows = 0;
    int64_t match_rows = 0;
    FOREACH_X(row, all_row_, OB_SUCCESS == ret) {
      bool match = false;
      if (OB_FAIL(check_row_match(*row, cells, match))) {
        LOG_WARN("check row match failed", K(ret), "row", *row, K(cells));
      } else {
        if (match) {
          match_rows++;
          if (OB_FAIL(execute_update_sql(*row, cells, affected_rows))) {
            LOG_WARN("execute update sql failed", K(ret));
          } else if (OB_FAIL(update_row_struct(cells, *row))) {
            LOG_WARN("update row failed", K(ret), "row", *row);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == match_rows && insert) {
        Row insert_row;
        int64_t new_row_id = 0;
        if (OB_FAIL(generate_row_id(new_row_id))) {
          LOG_WARN("generate row id failed", K(ret));
        } else {
          ObArray<Cell> empty_cells;
          if (OB_FAIL(insert_row.init(new_row_id, *this, empty_cells))) {
            LOG_WARN("init row failed", K(ret), K(new_row_id));
          }
        }
        ObArray<Cell> new_cells;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(execute_update_sql(insert_row, cells, affected_rows))) {
          LOG_WARN("execute update sql failed", K(ret));
        } else {
          FOREACH_CNT_X(c, cells, OB_SUCCESS == ret) {
            if (OB_FAIL(new_cells.push_back(c->cell_))) {
              LOG_WARN("add cell failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          std::sort(new_cells.begin(), new_cells.end());
          if (OB_FAIL(add_row(new_row_id, new_cells))) {
            LOG_WARN("add row failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::update_row(const ObIArray<UpdateCell> &cells, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  const bool insert = false;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
        KPC(this), "cell count", cells.count());
  } else if (OB_FAIL(update(cells, insert, affected_rows))) {
    LOG_WARN("update cells failed", K(ret), K(cells), K(insert));
  }

  return ret;
}

int ObCoreTableProxy::replace_row(const ObIArray<UpdateCell> &cells, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  const bool insert = true;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
        KPC(this), "cell count", cells.count());
  } else if (OB_FAIL(update(cells, insert, affected_rows))) {
    LOG_WARN("update cells failed", K(ret), K(cells), K(insert));
  }

  return ret;
}

int ObCoreTableProxy::incremental_update_row(const ObIArray<UpdateCell> &cells, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  const bool insert = false;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
        KPC(this), "cell count", cells.count());
  } else if (OB_FAIL(incremental_update(cells, insert, affected_rows))) {
    LOG_WARN("update cells failed", K(ret), K(cells), K(insert));
  }

  return ret;
}

int ObCoreTableProxy::incremental_replace_row(const ObIArray<UpdateCell> &cells, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  const bool insert = true;
  if (!is_valid() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
        KPC(this), "cell count", cells.count());
  } else if (OB_FAIL(incremental_update(cells, insert, affected_rows))) {
    LOG_WARN("update cells failed", K(ret), K(cells), K(insert));
  }

  return ret;
}



int ObCoreTableProxy::delete_row(const ObIArray<UpdateCell> &cells, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  // cells may be empty (delete all row)
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (!load_for_update_) {
    ret = OB_ERR_SYS;
    LOG_WARN("table not loaded for update", K(ret));
  } else {
    affected_rows = 0;
    FOREACH_CNT_X(uc, cells, OB_SUCCESS == ret) {
      if (!uc->is_filter_cell_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("cell not filter cell", K(ret), "cell", *uc);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_row_.count();) {
    bool match = false;
    if (OB_FAIL(check_row_match(all_row_.at(i), cells, match))) {
      LOG_WARN("check row match failed", K(ret), "row", all_row_.at(i), K(cells));
    } else {
      if (!match) {
        i++;
        continue;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(execute_delete_sql(all_row_.at(i).get_row_id()))) {
        LOG_WARN("delete row failed", K(ret), GETK(all_row_.at(i), row_id));
      } else {
        affected_rows++;
        if (OB_FAIL(all_row_.remove(i))) {
          LOG_WARN("remove row failed", K(ret), K(i));
        } else {
          if (cur_idx_ > i) {
            --cur_idx_;
          }
        }
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::execute_delete_sql(const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || OB_INVALID_INDEX == row_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(row_id));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE table_name = '%s' AND row_id = %ld",
        OB_ALL_CORE_TABLE_TNAME, table_name_, row_id))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else {
      LOG_TRACE("execute sql", KR(ret), K_(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObCoreTableProxy::store_string(const common::ObString &src, common::ObString &dest)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else {
    if (NULL == src.ptr()) { // null value
      dest = src;
    } else {
      char *buf = static_cast<char *>(allocator_.alloc(src.length() + 1));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("all memory for store string failed", K(ret), "length", src.length() + 1);
      } else {
        MEMCPY(buf, src.ptr(), src.length());
        buf[src.length()] = '\0';
        dest = ObString(0, src.length(), buf);
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::store_cell(const Cell &src, Cell &dest)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(src));
  } else if (OB_FAIL(store_string(src.name_, dest.name_))) {
    LOG_WARN("store cell name failed", K(ret), K(src));
  } else if (OB_FAIL(store_string(src.value_, dest.value_))) {
    LOG_WARN("store cell value failed", K(ret), K(src));
  } else {
    dest.is_hex_value_ = src.is_hex_value_;
  }
  return ret;
}

int ObCoreTableProxy::check_row_match(
    const Row &row, const ObIArray<UpdateCell> &cells, bool &match)
{

  int ret = OB_SUCCESS;
  match = true;
  if (!row.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row));
  }
  FOREACH_CNT_X(uc, cells, OB_SUCCESS == ret && match) {
    // row match, if no filter cell exist.
    if (!uc->is_filter_cell_) {
      continue;
    }
    Cell *c = NULL;
    if (OB_FAIL(row.get_cell(uc->cell_.name_, c))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        match = false;
      } else {
        LOG_WARN("get cell failed", K(ret), K(row), "name", uc->cell_.name_);
      }
    } else if (NULL == c) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL cell", K(ret));
    } else {
      if (c->value_ != uc->cell_.value_) {
        match = false;
      }
    }
  }

  return ret;
}

int ObCoreTableProxy::execute_incremental_update_sql(const Row &row, const ObIArray<UpdateCell> &cells,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows_bak = affected_rows;
  if (!is_valid() || !row.is_inited() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(row), K(cells));
  } else {
    ObSqlString insert_sql;
    ObSqlString update_sql;
    FOREACH_CNT_X(uc, cells, OB_SUCCESS == ret) {
      Cell *c = NULL;
      bool is_insert = false;
      if (OB_FAIL(row.get_cell(uc->cell_.name_, c))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get cell failed", K(ret), K(row), "name", uc->cell_.name_);
        } else {
          ret = OB_SUCCESS;
          is_insert = true;
        }
      } else if (NULL == c) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL cell", K(ret));
      } else {
        int64_t old_value = atoll(c->value_.ptr());
        int64_t new_value = atoll(uc->cell_.value_.ptr());
        if (old_value >= new_value
            && OB_INVALID_SCHEMA_VERSION != new_value) {
          LOG_INFO("value is less than, just continue", KPC(c), KPC(uc),
                   K(old_value), K(new_value));
          continue;
        }
      }
      ObSqlString &value_sql = is_insert ? insert_sql : update_sql;
      if (OB_FAIL(ret)) {
        //skip
      } else if (NULL == uc->cell_.value_.ptr()) {
        if (OB_FAIL(value_sql.append_fmt("%s ('%s', %ld, '%.*s', NULL)",
                                         value_sql.empty() ? "" : ",", table_name_, row.get_row_id(),
                                         uc->cell_.name_.length(), uc->cell_.name_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      } else {
        if (OB_FAIL(value_sql.append_fmt("%s ('%s', %ld, '%.*s', ",
                                         value_sql.empty() ? "" : ",", table_name_, row.get_row_id(),
                                         uc->cell_.name_.length(), uc->cell_.name_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        } else if (OB_FAIL(value_sql.append_fmt(uc->cell_.is_hex_value_ ? "%.*s)" : "'%.*s')",
                                                uc->cell_.value_.length(), uc->cell_.value_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
    }

    DEBUG_SYNC(BEFORE_UPDATE_CORE_TABLE);
    int64_t affected = 0;
    ObSqlString sql;

    //batch insert
    //can use insert ... on duplicate update
    //the empty of __all_core_table and __all_root_table take into count,
    //in the case of concurrent reporting, the row_id is 0, if it use batch update,
    //the information will be overlapped.
    //batch_execute deem update_replica complete with not retry.
    if (OB_FAIL(ret)) {
      //skip
    } else if (insert_sql.empty()) {
      //skip
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (table_name, row_id, column_name, column_value) VALUES %s ",
                                      OB_ALL_CORE_TABLE_TNAME, insert_sql.ptr()))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else {
      LOG_TRACE("execute sql", KR(ret), K_(tenant_id), K(sql), K(affected));
    }

    //batch update
    if (OB_FAIL(ret)) {
      //skip
    } else if (update_sql.empty()) {
      //skip
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (table_name, row_id, column_name, column_value) VALUES %s "
                                      "ON DUPLICATE KEY UPDATE column_value = if ((cast(column_value as signed) > values(column_value)) "
                                      "and (values(column_value) != %ld), "
                                      "column_value, values(column_value))",
                                      OB_ALL_CORE_TABLE_TNAME, update_sql.ptr(), OB_INVALID_SCHEMA_VERSION))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else {
      LOG_TRACE("execute sql", KR(ret), K_(tenant_id), K(sql), K(affected));
      if (is_zero_row(affected)) {
        LOG_WARN("core table update do nothing", K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      if (!insert_sql.empty() || !update_sql.empty()) {
        affected_rows = affected_rows_bak + 1;
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::execute_update_sql(const Row &row, const ObIArray<UpdateCell> &cells,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows_bak = affected_rows;
  if (!is_valid() || !row.is_inited() || cells.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this), K(row), K(cells));
  } else {
    ObSqlString insert_sql;
    ObSqlString update_sql;
    FOREACH_CNT_X(uc, cells, OB_SUCCESS == ret) {
      Cell *c = NULL;
      bool is_insert = false;
      if (OB_FAIL(row.get_cell(uc->cell_.name_, c))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get cell failed", K(ret), K(row), "name", uc->cell_.name_);
        } else {
          ret = OB_SUCCESS;
          is_insert = true;
        }
      } else if (NULL == c) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL cell", K(ret));
      } else if ((OB_ISNULL(c->value_.ptr()) && OB_NOT_NULL(uc->cell_.value_.ptr()))
                 || (OB_NOT_NULL(c->value_.ptr()) && OB_ISNULL(uc->cell_.value_.ptr()))) {
        // NULL == ObString.ptr() means NULL, which is different with empty string(data_length is 0, but ptr is not null)
      } else if (c->value_ == uc->cell_.value_) {
        LOG_INFO("value is same, just continue", KPC(c), KPC(uc));
        continue;
      }
      ObSqlString &value_sql = is_insert ? insert_sql : update_sql;
      if (OB_FAIL(ret)) {
        //skip
      } else if (NULL == uc->cell_.value_.ptr()) {
        if (OB_FAIL(value_sql.append_fmt("%s ('%s', %ld, '%.*s', NULL)",
                                         value_sql.empty() ? "" : ",", table_name_, row.get_row_id(),
                                         uc->cell_.name_.length(), uc->cell_.name_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      } else {
        if (OB_FAIL(value_sql.append_fmt("%s ('%s', %ld, '%.*s', ",
                                         value_sql.empty() ? "" : ",", table_name_, row.get_row_id(),
                                         uc->cell_.name_.length(), uc->cell_.name_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        } else if (OB_FAIL(value_sql.append_fmt(uc->cell_.is_hex_value_ ? "%.*s)" : "'%.*s')",
                                                uc->cell_.value_.length(), uc->cell_.value_.ptr()))) {
          LOG_WARN("fail to append fmt", K(ret));
        }
      }
    }

    int64_t affected = 0;
    ObSqlString sql;

    //batch insert
    //can use insert ... on duplicate update
    //the empty of __all_core_table and __all_root_table take into count,
    //in the case of concurrent reporting, the row_id is 0, if it use batch update,
    //the information will be overlapped.
    //batch_execute deem update_replica complete with not retry.
    if (OB_FAIL(ret)) {
      //skip
    } else if (insert_sql.empty()) {
      //skip
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (table_name, row_id, column_name, column_value) VALUES %s ",
                                      OB_ALL_CORE_TABLE_TNAME, insert_sql.ptr()))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else {
      LOG_TRACE("execute sql", KR(ret), K_(tenant_id), K(sql), K(affected));
    }

    //batch update
    if (OB_FAIL(ret)) {
      //skip
    } else if (update_sql.empty()) {
      //skip
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (table_name, row_id, column_name, column_value) VALUES %s "
                                      "ON DUPLICATE KEY UPDATE column_value = values(column_value)",
                                      OB_ALL_CORE_TABLE_TNAME, update_sql.ptr()))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected))) {
      LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else {
      LOG_TRACE("execute sql", KR(ret), K_(tenant_id), K(sql), K(affected));
    }

    if (OB_SUCC(ret)) {
      if (!insert_sql.empty() || !update_sql.empty()) {
        affected_rows = affected_rows_bak + 1;
      }
    }
  }
  return ret;
}

int ObCoreTableProxy::update_row_struct(const common::ObIArray<UpdateCell> &cells, Row &row)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || cells.count() <= 0 || !row.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cells), K(row), "cell count", cells.count());
  }
  FOREACH_CNT_X(uc, cells, OB_SUCCESS == ret) {
    if (OB_FAIL(row.update_cell(uc->cell_))) {
      LOG_WARN("update cell failed", K(ret), "update_cell", *uc);
    }
  }
  return ret;
}

int ObCoreTableProxy::generate_row_id(int64_t &row_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else {
    int64_t max_row_id = 0;
    FOREACH(r, all_row_) {
      if (max_row_id < r->get_row_id()) {
        max_row_id = r->get_row_id();
      }
    }
    row_id = max_row_id + 1;
  }
  return ret;
}

int ObCoreTableProxy::supplement_cell(const UpdateCell &cell)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (!load_for_update_) {
    ret = OB_ERR_SYS;
    LOG_WARN("table not loaded for update", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<UpdateCell> cells;
    ret = cells.push_back(cell);
    FOREACH_X(row, all_row_, OB_SUCCESS == ret) {
      if (OB_FAIL(execute_update_sql(*row, cells, affected_rows))) {
        LOG_WARN("execute update sql failed", K(ret));
      } else if (OB_FAIL(update_row_struct(cells, *row))) {
        LOG_WARN("update row failed", K(ret), "row", *row);
      }
    }
    if (OB_SUCC(ret)) {
      if (all_row_.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows),
                 K(all_row_.count()), K(all_row_));
      }
    }
  }

  return ret;
}

} // end namespace share
} // end namespace oceanbase
