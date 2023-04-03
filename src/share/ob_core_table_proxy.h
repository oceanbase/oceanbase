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

#ifndef OCEANBASE_SHARE_OB_CORE_TABLE_PROXY_H_
#define OCEANBASE_SHARE_OB_CORE_TABLE_PROXY_H_

#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObTimeZoneInfo;
}

namespace share
{

// Core kv table operate proxy.
//
// Example:
//   for read:
//     ObCoreTableProxy core_kv("__all_table", sql_proxy);
//     if (OB_FAIL(core_kv.load())) {
//       LOG_WARN(...);
//     } else {
//       while (OB_SUCCESS == ret && (OB_ITER_END != (ret = core_kv.next()))) {
//         core_kv.get_cur_row().get_int("table_id", table_id);
//         core_kv.get_cur_row().get_varchar("table_name", table_name);
//         ...
//       }
//       if (OB_ITER_END == ret) {
//         ret = OB_SUCCESS;
//       } else if (OB_FAIL(ret)) {
//         LOG_WARN(...);
//       }
//     }
//
//   for update:
//     ObCoreTableProxy core_kv("__all_root_table", sql_proxy);
//     if (OB_FAIL(core_kv.load_for_update())) {
//       LOG_WARN(...);
//     } else {
//       ...
//       read
//       ...
//
//       ObDMLSqlSplicer dml_executor(NAKED_VALUE_MODE); // no extra quote for string value
//       // treate pk columns as filter columns
//       ObArray<ObCoreTableProxy::UpdateCell> cells;
//       if (OB_FAIL(dml_executor.add_pk_column("table_id", 2))
//           || OB_FAIL(dml_executor.add_pk_column("partition_id", 0))
//           || OB_FAIL(dml_executor.add_pk_column("server", "ip:port"))
//           || OB_FAIL(dml_executor.add_column("role", 0))
//           || OB_FAIL(dml_executor.add_column("data_version", 2))
//           ....
//           || OB_FAIL(dml_executor.add_column("data_size", 1024))) {
//         // log ..
//       } else if (dml_executor.splice_core_cells(cells)) {
//         // log ...
//       }
//       ....
//       ret = core_kv.update_row(cells, affected_rows); // or replace_row()/delete_row()
//     }
//
class ObCoreTableProxy
{
public:
  struct Cell
  {
    Cell() : is_hex_value_(false) {}
    // name_.ptr() and value_.ptr() is terminated with '\0'
    common::ObString name_;
    common::ObString value_;
    bool is_hex_value_;

    bool operator <(const Cell o) const { return name_ < o.name_; }

    bool is_valid() const { return !name_.empty(); }
    TO_STRING_KV(K_(name), K_(value), K_(is_hex_value));
  };

  struct UpdateCell
  {
    bool is_filter_cell_;
    Cell cell_;

    UpdateCell() : is_filter_cell_(false), cell_() {}
    ~UpdateCell() {}

    bool is_valid() const { return cell_.is_valid(); }
    TO_STRING_KV(K_(is_filter_cell), K_(cell));
  };

  class Row {
  public:
    Row() : inited_(false), row_id_(common::OB_INVALID_INDEX),
            cells_(NULL), cell_cnt_(0), kv_proxy_(NULL)
    {}
    virtual ~Row() = default;

    // set sorted cells, and cells are stored in ObCoreTableProxy
    int init(const int64_t row_id, ObCoreTableProxy &kv_proxy,
        const common::ObIArray<Cell> &cells);
    bool is_inited() const { return inited_; }
    void reset() { *this = Row(); }

    // return OB_ENTRY_NOT_EXIST for not found
    int get_cell(const char *name, Cell *&cell) const;
    int get_cell(const common::ObString &name, Cell *&cell) const;

    // return OB_ERR_NULL_VALUE if got null
    virtual int get_int(const char *name, int64_t &value) const;
    virtual int get_uint(const char *name, uint64_t &value) const;
    virtual int get_varchar(const char *name, common::ObString &value) const;
    virtual int get_bool(const char *name, bool &value) const;
    virtual int get_timestamp(const char *name, const common::ObTimeZoneInfo *tz_info, int64_t &value) const;

    int64_t get_row_id() const { return row_id_; }
    void set_row_id(const int64_t row_id) { row_id_ = row_id; }

    int64_t get_cell_cnt() const { return cell_cnt_; }

    int update_cell(const Cell &cell);

    DECLARE_TO_STRING;
  private:
    int extend_cell_array(const int64_t cnt);

  private:
    bool inited_;
    int64_t row_id_;
    // cells are sorted by name
    Cell *cells_;
    int64_t cell_cnt_;
    ObCoreTableProxy *kv_proxy_;
  };

  ObCoreTableProxy(const char *table_name,
                   common::ObISQLClient &sql_client,
                   const uint64_t tenant_id);
  virtual ~ObCoreTableProxy();

  bool is_valid() const { return NULL != table_name_ && NULL != sql_client_; }

  void set_sql_client(common::ObISQLClient &sql_client) { sql_client_ = &sql_client; }

  // select %table_name_'s all rows
  int load();
  // select %table_name_'s all rows with FOR UPDATE suffix
  int load_for_update();

  // read gmt_create of one row in __all_core_table
  //
  // @note table_name_ and column_name decide together
  //
  // @retval OB_SUCCESS         success
  // @retval OB_ENTRY_NOT_EXIST the row is not exist
  // @retval other error code   fail
  int load_gmt_create(const char *column_name, int64_t &gmt_create_value);

  int next();

  int seek_to_head();

  int get_cur_row(const Row *&row) const;

  // return OB_ERR_NULL_VALUE if got null
  virtual int get_int(const char *name, int64_t &value) const;
  virtual int get_uint(const char *name, uint64_t &value) const;
  virtual int get_varchar(const char *name, common::ObString &value) const;
  virtual int get_timestamp(const char *name, const common::ObTimeZoneInfo *tz_info, int64_t &value) const;

  int64_t row_count() const { return all_row_.count(); }
  const common::ObIArray<Row> &get_all_row() const { return all_row_; }

  int store_cell(const Cell &src, Cell &dest);

  // update logic rows.
  // return logic affected row count (not rows in kv table)
  // if no rows exist after filter, no update will happen
  // %cells are stored in ObCoreTableProxy
  int update_row(const common::ObIArray<UpdateCell> &cells, int64_t &affected_rows);
  // update logic rows or insert one row. (OceanBase 0.5 replace semantic)
  // return logic affected row count (not rows in kv table)
  // if no rows exist, insert this row
  // %cells are stored in ObCoreTableProxy
  int replace_row(const common::ObIArray<UpdateCell> &cells, int64_t &affected_rows);
  //if the row not exist, execute insert; otherwise update the row while the new is large than the old value of the row.
  //it is used for resolve some value can not backoff.
  //in the RS switch, the new value will be overwrite while old query may execute on new leader.
  int incremental_replace_row(const common::ObIArray<UpdateCell> &cells, int64_t &affected_rows);
  //update while old value is larger then new value, otherwise do nothing.
  int incremental_update_row(const common::ObIArray<UpdateCell> &cells, int64_t &affected_rows);
  // delete logic rows
  // return logic affected row count (not rows in kv table)
  // %cells should all be filter cells
  int delete_row(const common::ObIArray<UpdateCell> &cells, int64_t &affected_rows);
  int supplement_cell(const UpdateCell &cell);

  TO_STRING_KV(K_(table_name), K_(load_for_update), K_(cur_idx), K_(all_row));
private:
  int store_string(const common::ObString &src, common::ObString &dest);

  int load(const bool for_update);
  int add_row(const int64_t row_id, const common::ObIArray<Cell> &cells);

  int update(const common::ObIArray<UpdateCell> &cells,
      const bool insert, int64_t &affected_rows);
  int incremental_update(const common::ObIArray<UpdateCell> &cells,
                         const bool insert, int64_t &affected_rows);

  static int check_row_match(
      const Row &row, const common::ObIArray<UpdateCell> &cells, bool &match);

  // update cell or insert new cell
  int execute_update_sql(const Row &row, const common::ObIArray<UpdateCell> &cells,
                         int64_t &affected_rows);
  int execute_incremental_update_sql(const Row &row, const common::ObIArray<UpdateCell> &cells,
                                     int64_t &affected_rows);

  int execute_delete_sql(const int64_t row_id);

  // update %row
  int update_row_struct(const common::ObIArray<UpdateCell> &cells, Row &row);

  // choose the minimal non exist row_id (started with 0)
  int generate_row_id(int64_t &row_id) const;

private:
  const char *table_name_;
  common::ObISQLClient *sql_client_;

  bool load_for_update_;
  int64_t cur_idx_;
  common::ObArray<Row> all_row_;
  common::ObArenaAllocator allocator_;
  uint64_t tenant_id_;

  DISALLOW_COPY_AND_ASSIGN(ObCoreTableProxy);
};

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_CORE_TABLE_PROXY_H_
