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

#define USING_LOG_PREFIX LOGMNR

#include "lib/utility/ob_fast_convert.h"
#include "ob_log_miner_record.h"
#include "ob_log_miner_br.h"
#include "ob_log_binlog_record.h"
#include "ob_log_miner_logger.h"

#define APPEND_STMT(stmt, args...) \
  do { \
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(stmt.append(args))) { \
        LOG_ERROR("failed to append args to stmt", K(stmt));\
      } \
    } \
  } while (0)

// only used in ObLogMinerRecord
#define APPEND_ESCAPE_CHAR(stmt) \
  do { \
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(build_escape_char_(stmt))) { \
        LOG_ERROR("failed to append escape char to stmt", K(stmt));\
      } \
    } \
  } while (0)

namespace oceanbase
{
namespace oblogminer
{
const char *ObLogMinerRecord::ORACLE_ESCAPE_CHAR = "\"";
const char *ObLogMinerRecord::MYSQL_ESCAPE_CHAR = "`";
const char *ObLogMinerRecord::ORA_GEO_PREFIX = "SRID=";
const char *ObLogMinerRecord::JSON_EQUAL = "JSON_EQUAL";
const char *ObLogMinerRecord::LOB_COMPARE = "0=DBMS_LOB.COMPARE";
const char *ObLogMinerRecord::ST_EQUALS = "ST_Equals";

ObLogMinerRecord::ObLogMinerRecord():
    ObLogMinerRecyclableTask(TaskType::LOGMINER_RECORD),
    is_inited_(false),
    is_filtered_(false),
    alloc_(nullptr),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    tenant_id_(OB_INVALID_TENANT_ID),
    orig_cluster_id_(OB_INVALID_CLUSTER_ID),
    tenant_name_(),
    database_name_(),
    table_name_(),
    trans_id_(),
    primary_keys_(),
    unique_keys_(),
    row_unique_id_(),
    record_type_(EUNKNOWN),
    commit_scn_(),
    redo_stmt_(),
    undo_stmt_()
{
}

ObLogMinerRecord::ObLogMinerRecord(ObIAllocator *alloc):
    ObLogMinerRecord()
{
  set_allocator(alloc);
}

ObLogMinerRecord::~ObLogMinerRecord()
{
  destroy();
}

int ObLogMinerRecord::init(ObLogMinerBR &logminer_br)
{
  int ret = OB_SUCCESS;
  ICDCRecord *cdc_record = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogMinerRecord has already been initialized", K(is_inited_));
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("allocator is null before init, unexpected", K(alloc_));
  } else if (OB_ISNULL(cdc_record = logminer_br.get_br())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get a null cdc record in logminer br", K(cdc_record));
  } else {
    is_inited_ = true;
    is_filtered_ = false;
    record_type_ = logminer_br.get_record_type();
    compat_mode_ = logminer_br.get_compat_mode();
    tenant_id_ = logminer_br.get_tenant_id();
    commit_scn_ = logminer_br.get_commit_scn();
    trans_id_ = logminer_br.get_trans_id();
    orig_cluster_id_ = cdc_record->getThreadId();
    if (is_dml_record() || is_ddl_record()) {
      if (OB_FAIL(fill_data_record_fields_(*cdc_record))) {
        LOG_ERROR("fill data record fields failed", K(cdc_record),
            K(record_type_), K(tenant_id_), K(trans_id_), K(commit_scn_));
      }
    }
  }
  return ret;
}

void ObLogMinerRecord::set_allocator(ObIAllocator *alloc)
{
  alloc_ = alloc;
  redo_stmt_.set_allocator(alloc_);
  undo_stmt_.set_allocator(alloc_);
}

void ObLogMinerRecord::destroy()
{
  reset();
}

void ObLogMinerRecord::reset()
{
  is_filtered_ = false;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  orig_cluster_id_ = OB_INVALID_CLUSTER_ID;
  tenant_name_.reset();
  database_name_.reset();
  table_name_.reset();
  trans_id_.reset();
  record_type_ = EUNKNOWN;
  commit_scn_.reset();

  redo_stmt_.reset();
  undo_stmt_.reset();
  free_row_unique_id_();
  free_keys_(primary_keys_);
  free_keys_(unique_keys_);
  is_inited_ = false;
}

bool ObLogMinerRecord::is_dml_record() const
{
  bool bret = false;
  switch (record_type_) {
    case EINSERT:
    case EUPDATE:
    case EDELETE: {
      bret = true;
      break;
    }
    default: {
      bret = false;
      break;
    }
  }
  return bret;
}

bool ObLogMinerRecord::is_ddl_record() const
{
  return EDDL == record_type_;
}

void ObLogMinerRecord::copy_base_info(const ObLogMinerRecord &other)
{
  is_inited_ = other.is_inited_;
  is_filtered_ = other.is_filtered_;
  compat_mode_ = other.compat_mode_;
  tenant_id_ = other.tenant_id_;
  orig_cluster_id_ = other.orig_cluster_id_;
  tenant_name_ = other.tenant_name_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  trans_id_ = other.trans_id_;
  record_type_ = other.record_type_;
  commit_scn_ = other.commit_scn_;
}

int ObLogMinerRecord::build_stmts(ObLogMinerBR &br)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else {
    ICDCRecord *cdc_rec = br.get_br();

    if (OB_ISNULL(cdc_rec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get a null cdc record when building redo", K(cdc_rec), KPC(this));
    } else if (is_ddl_record()) {
      if (OB_FAIL(build_ddl_stmt_(*cdc_rec))) {
        LOG_ERROR("build ddl stmt failed", KPC(this));
      }
    } else if (is_dml_record()) {
      if (OB_FAIL(build_dml_stmt_(*cdc_rec))) {
        LOG_ERROR("build dml stmt failed", KPC(this));
      }
    }
    if (OB_SUCC(ret) && 0 == redo_stmt_.length()) {
      APPEND_STMT(redo_stmt_, "/* NO SQL_REDO GENERATED */");
    }
    if (OB_SUCC(ret) && 0 == undo_stmt_.length()) {
      APPEND_STMT(undo_stmt_, "/* NO SQL_UNDO GENERATED */");
    }
  }
  return ret;
}

void ObLogMinerRecord::free_row_unique_id_()
{
  if (nullptr != row_unique_id_.ptr()) {
    alloc_->free(row_unique_id_.ptr());
    row_unique_id_.reset();
  }
}

void ObLogMinerRecord::free_keys_(KeyArray &arr)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(arr, idx) {
    ObString &item = arr.at(idx);
    alloc_->free(item.ptr());
    item.reset();
  }
  arr.reset();
}

int ObLogMinerRecord::fill_data_record_fields_(ICDCRecord &record)
{
  int ret = OB_SUCCESS;
  if (! is_dml_record() && ! is_ddl_record()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected record type when filling record fileds", K(record_type_));
  } else if (OB_FAIL(fill_tenant_db_name_(record.dbname()))) {
    LOG_ERROR("fill tenant_name and db_name failed", "tenant_db_name", record.dbname());
  } else if (is_dml_record()) {
    ITableMeta *tbl_meta = record.getTableMeta();

    if (OB_ISNULL(tbl_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null table meta from cdc_record", K(tbl_meta),
        K(record_type_), K(trans_id_));
    } else if (OB_FAIL(table_name_.assign(tbl_meta->getName()))) {
      LOG_ERROR("failed to fill table name", "tbl_name", tbl_meta->getName());
    } else if (OB_FAIL(fill_primary_keys_(*tbl_meta))) {
      LOG_ERROR("failed to fill primary keys", "tbl_name", tbl_meta->getName());
    } else if (OB_FAIL(fill_unique_keys_(*tbl_meta))) {
      LOG_ERROR("failed to fill unique keys", "tbl_name", tbl_meta->getName());
    }
  }
  return ret;
}

int ObLogMinerRecord::fill_row_unique_id_(ICDCRecord &cdc_record)
{
  int ret = OB_SUCCESS;
  uint filter_rv_count = 0;
  BinlogRecordImpl &br_impl = static_cast<BinlogRecordImpl&>(cdc_record);
  const binlogBuf *filter_rvs = br_impl.filterValues(filter_rv_count);

  if (nullptr != filter_rvs && filter_rv_count > 2) {
    const binlogBuf &unique_id = filter_rvs[1];
    char *row_unique_id_buf = static_cast<char *>(alloc_->alloc(unique_id.buf_used_size + 1));
    if (OB_ISNULL(row_unique_id_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for row_unique_id_buf failed", K(row_unique_id_buf));
    } else {
      MEMCPY(row_unique_id_buf, unique_id.buf, unique_id.buf_used_size);
      row_unique_id_buf[unique_id.buf_used_size+1] = '\0';
    }
  }

  return ret;
}

int ObLogMinerRecord::fill_tenant_db_name_(const char *tenant_db_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_db_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get a null tenant_db_name", K(tenant_db_name), K(record_type_));
  } else {
    // expect tenant_db_name has the format like "tenant_name.db_name" for all dml record
    const char tenant_db_sep = '.';
    const int64_t tenant_db_len = STRLEN(tenant_db_name);
    const char *dot_pos = STRCHR(tenant_db_name, tenant_db_sep);
    if (OB_ISNULL(dot_pos)) {
      if (is_dml_record()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant_db_name for dml record doesn't contain dot, unexpected", K(tenant_db_name));
      } else if (is_ddl_record()) {
        if (OB_FAIL(tenant_name_.assign(tenant_db_name))) {
          LOG_ERROR("assign tenant name for ddl_record failed", K(tenant_db_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid record type when filling tenant_db_name", K(tenant_db_name), K(record_type_));
      }
    } else {
      const int64_t tenant_name_len = dot_pos - tenant_db_name;
      ObString tenant_name(tenant_name_len, tenant_db_name);
      ObString db_name(dot_pos + 1);
      if (OB_FAIL(tenant_name_.assign(tenant_name))) {
        LOG_ERROR("assign tenant_name failed", K(tenant_name), K(tenant_db_name));
      } else if (OB_FAIL(database_name_.assign(db_name))) {
        LOG_ERROR("assign database_name failed", K(db_name), K(tenant_db_name));
      }
    }
  }
  return ret;
}

int ObLogMinerRecord::copy_string_to_array_(const ObString &str,
    KeyArray &array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("logminer record has not been initialized", K(is_inited_));
  } else {
    const int64_t str_len = str.length();
    ObString tmp_str;
    char *buf = static_cast<char*>(alloc_->alloc(str_len + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate buf for obstring failed", K(buf), K(str_len), K(str));
    } else {
      MEMCPY(buf, str.ptr(), str_len);
      buf[str_len] = '\0';
      tmp_str.assign(buf, str_len);
      if (OB_FAIL(array.push_back(tmp_str))) {
        LOG_ERROR("failed to push token to keyarray", K(str), K(tmp_str), K(array));
      }
    }
  }

  return ret;
}

int ObLogMinerRecord::fill_keys_(const char *key_cstr, KeyArray &key_arr)
{
  int ret = OB_SUCCESS;
  const char *curr_ptr = key_cstr, *next_ptr = nullptr;
  while (OB_SUCC(ret) && nullptr != (next_ptr = strchr(curr_ptr, ','))) {
    ObString key(next_ptr - curr_ptr, curr_ptr);
    if (OB_FAIL(copy_string_to_array_(key, key_arr))) {
      LOG_ERROR("failed to copy key to primary_keys", K(key), K(key_arr));
    } else {
      // skip delimiter ','
      curr_ptr = next_ptr + 1;
    }
  }

  if (OB_SUCC(ret)) {
    // curr_ptr must end with '\0'
    ObString last_key(curr_ptr);
    if (OB_FAIL(copy_string_to_array_(last_key, key_arr))) {
      LOG_ERROR("failed to copy last key to primary_keys_", K(last_key), K(key_arr));
    }
  }
  return ret;
}

int ObLogMinerRecord::fill_primary_keys_(ITableMeta &tbl_meta)
{
  int ret = OB_SUCCESS;
  const char *pks_cstr = tbl_meta.getPKs();
  if (OB_ISNULL(pks_cstr)) {
    LOG_TRACE("get null pk_cstr", "table", tbl_meta.getName());
  } else {
    const int64_t pks_len = STRLEN(pks_cstr);
    const int64_t column_cnt = tbl_meta.getColCount();
    if (0 == pks_len) {
      LOG_TRACE("no pk for tbl", K(pks_cstr), K(column_cnt), "tbl_name", tbl_meta.getName());
    } else if (OB_FAIL(fill_keys_(pks_cstr, primary_keys_))) {
      LOG_ERROR("failed to fill primary keys", K(pks_cstr), "tbl_name", tbl_meta.getName());
    } else {
      LOG_TRACE("finish fill primary keys", K(pks_cstr), K(primary_keys_));
    }
  }
  return ret;
}

int ObLogMinerRecord::fill_unique_keys_(ITableMeta &tbl_meta)
{
  int ret = OB_SUCCESS;
  const char *uks_cstr = tbl_meta.getUKs();
  if (OB_ISNULL(uks_cstr)) {
    LOG_TRACE("get null uk_cstr", "table", tbl_meta.getName());
  } else {
    const int64_t uks_len = STRLEN(uks_cstr);
    const int64_t column_cnt = tbl_meta.getColCount();
    if (0 == uks_len) {
      LOG_TRACE("no uk for tbl", K(uks_cstr), K(column_cnt), "tbl_name", tbl_meta.getName());
    } else if (OB_FAIL(fill_keys_(uks_cstr, unique_keys_))) {
      LOG_ERROR("failed to fill unique keys", K(uks_cstr), "tbl_name", tbl_meta.getName());
    } else {
      LOG_TRACE("finish fill unique keys", K(uks_cstr), K(unique_keys_));
    }
  }
  return ret;
}

int ObLogMinerRecord::build_ddl_stmt_(ICDCRecord &cdc_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else {
    unsigned int new_col_cnt = 0;
    binlogBuf *brbuf = cdc_rec.newCols(new_col_cnt);
    if (new_col_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new_col_cnt for ddl stmt less than zero", K(new_col_cnt), KPC(this));
    } else {
      // only need ddl_stmt_str and expect it is placed in the first slot
      redo_stmt_.append(brbuf[0].buf, brbuf[0].buf_used_size);
    }
  }
  return ret;
}

int ObLogMinerRecord::build_dml_stmt_(ICDCRecord &cdc_rec)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else {
    unsigned int new_col_cnt = 0, old_col_cnt = 0;
    binlogBuf *new_cols = cdc_rec.newCols(new_col_cnt);
    binlogBuf *old_cols = cdc_rec.oldCols(old_col_cnt);
    ITableMeta *tbl_meta = cdc_rec.getTableMeta();
    // When updating or deleting records with lob type,
    // the null value of the lob type column may be incorrect
    // due to the limitations of obcdc.
	  bool has_lob_null = false;
	  // xmltype and sdo_geometry type don't support compare operation.
    bool has_unsupport_type_compare = false;
    if (OB_SUCC(ret)) {
      switch(record_type_) {
        // Insert records with lob type is accurate. obcdc will output all value of lob type.
        case EINSERT: {
          if (OB_FAIL(build_insert_stmt_(redo_stmt_, new_cols, new_col_cnt, tbl_meta))) {
            LOG_ERROR("build insert redo stmt failed", KPC(this));
          } else {
            if (OB_FAIL(build_delete_stmt_(undo_stmt_, new_cols, new_col_cnt,
                tbl_meta, has_lob_null, has_unsupport_type_compare))) {
              LOG_ERROR("build insert undo stmt failed", KPC(this));
            } else {
              // ignore has_lob_null
              if (has_unsupport_type_compare) {
                APPEND_STMT(undo_stmt_, "/* POTENTIALLY INACCURATE */");
              }
            }
          }
          break;
        }

        // Update records with lob type maybe inaccurate,
        // if NULL value appears in the pre/post mirror, the NULL may be incorrect.
        case EUPDATE: {
          if (OB_FAIL(build_update_stmt_(redo_stmt_, new_cols, new_col_cnt, old_cols,
              old_col_cnt, tbl_meta,  has_lob_null, has_unsupport_type_compare))) {
            LOG_ERROR("build update redo stmt failed", KPC(this));
          } else {
            if (has_lob_null || has_unsupport_type_compare) {
              APPEND_STMT(redo_stmt_, "/* POTENTIALLY INACCURATE */");
              has_lob_null = false;
              has_unsupport_type_compare = false;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(build_update_stmt_(undo_stmt_, old_cols, old_col_cnt, new_cols,
                new_col_cnt, tbl_meta, has_lob_null, has_unsupport_type_compare))) {
              LOG_ERROR("build update undo stmt failed", KPC(this));
            } else {
              if (has_lob_null || has_unsupport_type_compare) {
                APPEND_STMT(undo_stmt_, "/* POTENTIALLY INACCURATE */");
              }
            }
          }
          break;
        }

        // Delete records with lob type maybe inaccurate,
        // if NULL value appears in the pre mirror, the NULL may be incorrect.
        case EDELETE: {
          if (OB_FAIL(build_delete_stmt_(redo_stmt_, old_cols, old_col_cnt,
              tbl_meta, has_lob_null, has_unsupport_type_compare))) {
            LOG_ERROR("build delete redo stmt failed", KPC(this));
          } else {
            if (has_lob_null || has_unsupport_type_compare) {
              APPEND_STMT(redo_stmt_, "/* POTENTIALLY INACCURATE */");
              has_lob_null = false;
              has_unsupport_type_compare = false;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(build_insert_stmt_(undo_stmt_, old_cols,
                old_col_cnt, tbl_meta, has_lob_null))) {
              LOG_ERROR("build delete undo stmt failed", KPC(this));
            } else {
              if (has_lob_null) {
                APPEND_STMT(undo_stmt_, "/* POTENTIALLY INACCURATE */");
              }
            }
          }
          break;
        }

        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid dml record type", KPC(this));
          break;
        }
      }
    }
  }
  return ret;
}

int ObLogMinerRecord::build_insert_stmt_(ObStringBuffer &stmt,
    binlogBuf *new_cols,
    const unsigned int new_col_cnt,
    ITableMeta *tbl_meta)
{
  int ret = OB_SUCCESS;
  // ignore has_lob_null
  bool has_lob_null = false;
  if (OB_FAIL(build_insert_stmt_(stmt, new_cols, new_col_cnt, tbl_meta, has_lob_null))) {
    LOG_ERROR("build insert stmt failed", KPC(this));
  }
  return ret;
}
int ObLogMinerRecord::build_insert_stmt_(ObStringBuffer &stmt,
    binlogBuf *new_cols,
    const unsigned int new_col_cnt,
    ITableMeta *tbl_meta,
    bool &has_lob_null)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else {
    APPEND_STMT(stmt, "INSERT INTO ");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, database_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, ".");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, table_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, " (");
    // append all col name
    for (int i = 0; i < new_col_cnt && OB_SUCC(ret); i++) {
      IColMeta *col_meta = tbl_meta->getCol(i);
      if (OB_ISNULL(col_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get null col_meta", "table_name", tbl_meta->getName(), K(i));
      }
      if (0 != i) {
        APPEND_STMT(stmt, ", ");
      }
      APPEND_ESCAPE_CHAR(stmt);
      APPEND_STMT(stmt, col_meta->getName());
      APPEND_ESCAPE_CHAR(stmt);
    }
    APPEND_STMT(stmt, ") VALUES (");
    // append all col value

    for (int i = 0; i < new_col_cnt && OB_SUCC(ret); i++) {
      IColMeta *col_meta = tbl_meta->getCol(i);
      if (OB_ISNULL(col_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get null col_meta", "table_name", tbl_meta->getName(), K(i));
      }
      if (0 != i) {
        APPEND_STMT(stmt, ", ");
      }
      if (OB_SUCC(ret) && OB_FAIL(build_column_value_(stmt, col_meta, new_cols[i]))) {
        LOG_ERROR("failed to build column_value", "table_name", tbl_meta->getName(),
            "col_idx", i);
      }
      if (OB_SUCC(ret)) {
        if (is_lob_type_(col_meta) && nullptr == new_cols[i].buf) {
          has_lob_null = true;
        }
      }
    }

    APPEND_STMT(stmt, ");");
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("build insert stmt", "insert_stmt", stmt.ptr());
  }
  return ret;
}

int ObLogMinerRecord::build_update_stmt_(ObStringBuffer &stmt,
    binlogBuf *new_cols,
    const unsigned int new_col_cnt,
    binlogBuf *old_cols,
    const unsigned int old_col_cnt,
    ITableMeta *tbl_meta,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else if (new_col_cnt != old_col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new_col_cnt is not equal to old_col_cnt, unexpected", K(new_col_cnt), K(old_col_cnt));
  } else {
    APPEND_STMT(stmt, "UPDATE ");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, database_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, ".");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, table_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, " SET ");
    for (int i = 0; OB_SUCC(ret) && i < new_col_cnt; i++) {
      IColMeta *col_meta = tbl_meta->getCol(i);
      if (OB_ISNULL(col_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get null col_meta", "table_name", tbl_meta->getName(), K(i));
      }
      if (0 != i) {
        APPEND_STMT(stmt, ", ");
      }
      APPEND_ESCAPE_CHAR(stmt);
      APPEND_STMT(stmt, col_meta->getName());
      APPEND_ESCAPE_CHAR(stmt);
      APPEND_STMT(stmt, "=");
      if (OB_SUCC(ret) && OB_FAIL(build_column_value_(stmt, col_meta, new_cols[i]))) {
        LOG_ERROR("failed to build column_value", "table_name", tbl_meta->getName(),
            "col_idx", i);
      }
      if (OB_SUCC(ret)) {
        if (is_lob_type_(col_meta) && nullptr == new_cols[i].buf) {
          has_lob_null = true;
        }
      }
    }
    APPEND_STMT(stmt, " WHERE ");

    if (OB_SUCC(ret) && OB_FAIL(build_where_conds_(stmt, old_cols, old_col_cnt,
        tbl_meta, has_lob_null, has_unsupport_type_compare))) {
      LOG_ERROR("build where conds failed",);
    }
    if (lib::Worker::CompatMode::MYSQL == compat_mode_) {
      APPEND_STMT(stmt, " LIMIT 1");
    } else if (lib::Worker::CompatMode::ORACLE == compat_mode_) {
      APPEND_STMT(stmt, " AND ROWNUM=1");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get invalid compat mode when build stmt", K(compat_mode_));
    }

    APPEND_STMT(stmt, ";");

    if (OB_SUCC(ret)) {
      LOG_TRACE("build update stmt", "update_stmt", stmt.ptr());
    }
  }
  return ret;
}

int ObLogMinerRecord::build_delete_stmt_(ObStringBuffer &stmt,
    binlogBuf *old_cols,
    const unsigned int old_col_cnt,
    ITableMeta *tbl_meta,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("record hasn't been initialized", KPC(this));
  } else {
    APPEND_STMT(stmt, "DELETE FROM ");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, database_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, ".");
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, table_name_.ptr());
    APPEND_ESCAPE_CHAR(stmt);
    APPEND_STMT(stmt, " WHERE ");

    if (OB_SUCC(ret) && OB_FAIL(build_where_conds_(stmt, old_cols, old_col_cnt,
        tbl_meta, has_lob_null, has_unsupport_type_compare))) {
      LOG_ERROR("build where conds failed",);
    }
    if (lib::Worker::CompatMode::MYSQL == compat_mode_) {
      APPEND_STMT(stmt, " LIMIT 1");
    } else if (lib::Worker::CompatMode::ORACLE == compat_mode_) {
      APPEND_STMT(stmt, " and ROWNUM=1");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get invalid compat mode when build stmt", K(compat_mode_));
    }

    APPEND_STMT(stmt, ";");
    if (OB_SUCC(ret)) {
      LOG_TRACE("build delete stmt", "delete_stmt", stmt.ptr());
    }
  }
  return ret;
}

int ObLogMinerRecord::build_column_value_(ObStringBuffer &stmt,
    IColMeta *col_meta,
    binlogBuf &col_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(col_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null col_meta when building column_value", K(col_meta));
  } else if (OB_ISNULL(col_data.buf)) {
    APPEND_STMT(stmt, "NULL");
  } else if (is_number_type_(col_meta)) {
    APPEND_STMT(stmt, col_data.buf, col_data.buf_used_size);
  } else if (is_binary_type_(col_meta)) {
    if (lib::Worker::CompatMode::MYSQL == compat_mode_) {
      APPEND_STMT(stmt, "UNHEX('");
      if (OB_FAIL(build_hex_val_(stmt, col_data))) {
        LOG_ERROR("failed to build hex_val for col", "col_name", col_meta->getName());
      }
      APPEND_STMT(stmt, "')");
    } else if (lib::Worker::CompatMode::ORACLE == compat_mode_) {
      APPEND_STMT(stmt, "HEXTORAW('");
      if (OB_FAIL(build_hex_val_(stmt, col_data))) {
        LOG_ERROR("failed to build hex_val for col", "col_name", col_meta->getName());
      }
      APPEND_STMT(stmt, "')");
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("get invalid compat mode, not supported", KPC(this));
    }
  } else if (is_string_type_(col_meta)) {
    ObArenaAllocator tmp_alloc;
    ObString target_str;
    ObString src_str(col_data.buf_used_size, col_data.buf);
    ObString srid;
    const char *src_encoding_str = col_meta->getEncoding();
    const ObCharsetType src_cs_type = ObCharset::charset_type(src_encoding_str);
    const ObCollationType src_coll_type = ObCharset::get_default_collation(src_cs_type);
    const ObCollationType dst_coll_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    const char *curr_ptr = nullptr, *next_ptr = nullptr;
    int64_t buf_size = 0;
    int64_t data_len = 0;
    ObStringBuffer str_val(alloc_);
    bool is_atoi_valid = false;

    if (src_coll_type == dst_coll_type) {
      target_str.assign_ptr(src_str.ptr(), src_str.length());
      curr_ptr = target_str.ptr();
      buf_size = target_str.length();
    } else {
      if (OB_FAIL(ObCharset::charset_convert(tmp_alloc, src_str, src_coll_type,
          dst_coll_type, target_str))) {
        LOG_ERROR("failed to convert src_str to target_str", K(src_coll_type), K(src_encoding_str),
            K(src_cs_type), K(src_coll_type), K(dst_coll_type), "src_len", src_str.length());
      } else {
        curr_ptr = target_str.ptr();
        buf_size = target_str.length();
        LOG_TRACE("charset_convert finished", K(src_str), K(src_encoding_str), K(src_cs_type),
            K(src_coll_type), K(dst_coll_type), K(target_str));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_geo_type_(col_meta) && lib::Worker::CompatMode::ORACLE == compat_mode_) {
        if (!target_str.prefix_match(ORA_GEO_PREFIX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("convert oracle geometry column value failed", K(target_str));
        } else {
          curr_ptr += strlen(ORA_GEO_PREFIX);
          buf_size -= strlen(ORA_GEO_PREFIX);
          next_ptr = static_cast<const char*>(memchr(curr_ptr, ';', buf_size));
          if (nullptr == next_ptr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("convert oracle geometry column value failed", K(target_str));
          } else {
            data_len = next_ptr - curr_ptr;
            srid.assign_ptr(curr_ptr, data_len);
            next_ptr++;
            curr_ptr = next_ptr;
            buf_size -= (data_len + 1); // ignore ";"
          }
        }
        APPEND_STMT(stmt, "SDO_GEOMETRY(");
      } else if (is_geo_type_(col_meta) && lib::Worker::CompatMode::MYSQL == compat_mode_) {
        APPEND_STMT(stmt, "ST_GeomFromText(");
      } else if (is_bit_type_(col_meta)) {
        APPEND_STMT(stmt, "b");
      }
    }

    APPEND_STMT(stmt, "\'");
    while (OB_SUCC(ret) && (nullptr != (next_ptr =
        static_cast<const char*>(memchr(curr_ptr, '\'', buf_size))))) {
      // next_ptr point to the of '\''
      next_ptr++;
      data_len = next_ptr - curr_ptr;
      APPEND_STMT(str_val, curr_ptr, data_len);
      APPEND_STMT(str_val, "'");
      curr_ptr = next_ptr;
      buf_size -= data_len;
    }
    APPEND_STMT(str_val, curr_ptr, buf_size);
    if (OB_SUCC(ret) && is_bit_type_(col_meta)) {
      uint64_t bit_val = common::ObFastAtoi<uint64_t>::atoi(str_val.ptr(), str_val.ptr()
          + str_val.length(), is_atoi_valid);
      if (!is_atoi_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("str convert to uint64 failed failed", K(str_val));
      } else {
        str_val.reset();
        if (OB_FAIL(uint_to_bit(bit_val, str_val))) {
          LOG_ERROR("convert uint64 to bit failed", K(bit_val));
        }
      }
    }
    APPEND_STMT(stmt, str_val.ptr());
    APPEND_STMT(stmt, "\'");
    if (OB_SUCC(ret)) {
      if (is_geo_type_(col_meta) && lib::Worker::CompatMode::ORACLE == compat_mode_) {
        APPEND_STMT(stmt, ", ");
        APPEND_STMT(stmt, srid.ptr(), srid.length());
        APPEND_STMT(stmt, ")");
      } else if (is_geo_type_(col_meta) && lib::Worker::CompatMode::MYSQL == compat_mode_) {
        APPEND_STMT(stmt, ")");
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("get not supported column type", "col_name", col_meta->getName(),
        "col_type", col_meta->getType());
  }

  return ret;
}

int ObLogMinerRecord::build_where_conds_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_cnt,
		ITableMeta *tbl_meta,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  if (!unique_keys_.empty()) {
    if (OB_FAIL(build_key_conds_(stmt, cols, col_cnt, tbl_meta,
        unique_keys_, has_lob_null, has_unsupport_type_compare))) {
      LOG_ERROR("build unique keys failed", K(stmt), K(unique_keys_));
    }
  } else if (!primary_keys_.empty()) {
    if (OB_FAIL(build_key_conds_(stmt, cols, col_cnt, tbl_meta,
        primary_keys_, has_lob_null, has_unsupport_type_compare))) {
      LOG_ERROR("build primary keys failed", K(stmt), K(primary_keys_));
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < col_cnt; i++) {
      IColMeta *col_meta = tbl_meta->getCol(i);
      if (0 != i) {
        APPEND_STMT(stmt, " AND ");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(build_cond_(stmt, cols, i, tbl_meta, col_meta, has_lob_null, has_unsupport_type_compare))) {
          LOG_ERROR("build cond failed", "table_name", tbl_meta->getName());
        }
      }
    }
  }
  return ret;
}

int ObLogMinerRecord::build_key_conds_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_cnt,
		ITableMeta *tbl_meta,
		const KeyArray &key,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < key.count(); i++) {
    int64_t col_idx = tbl_meta->getColIndex(key.at(i).ptr());
    if (0 > col_idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get col index failed", K(key));
    } else {
      IColMeta *col_meta = tbl_meta->getCol(col_idx);
      if (0 != i) {
        APPEND_STMT(stmt, " AND ");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(build_cond_(stmt, cols, col_idx, tbl_meta, col_meta,
            has_lob_null, has_unsupport_type_compare))) {
          LOG_ERROR("build cond failed", "table_name", tbl_meta->getName());
        }
      }
    }
  }
  return ret;
}

int ObLogMinerRecord::build_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null col_meta", "table_name", tbl_meta->getName(), K(col_idx));
  } else {
    if (is_lob_type_(col_meta) && nullptr != cols[col_idx].buf) {
      // build lob type compare condition, excluding null value condition
      if (OB_FAIL(build_lob_cond_(stmt, cols, col_idx, tbl_meta,
          col_meta, has_lob_null, has_unsupport_type_compare))) {
        LOG_ERROR("build lob condition failed", "table_name", tbl_meta->getName());
      }
    } else {
      if (OB_FAIL(build_normal_cond_(stmt, cols, col_idx, tbl_meta, col_meta))) {
        LOG_ERROR("build normal condition failed", "table_name", tbl_meta->getName());
      }
    }
    if (OB_SUCC(ret)) {
      if (is_lob_type_(col_meta) && nullptr == cols[col_idx].buf) {
        has_lob_null = true;
      }
    }
  }
  return ret;
}

int ObLogMinerRecord::build_lob_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta,
    bool &has_lob_null,
		bool &has_unsupport_type_compare)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_lob_type_(col_meta) || nullptr == cols[col_idx].buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("column type or value is unexpected", KP(col_meta), K(col_idx));
  } else {
    int type = col_meta->getType();
    if (lib::Worker::CompatMode::ORACLE == compat_mode_) {
      if (obmysql::MYSQL_TYPE_JSON == type) {
        if (OB_FAIL(build_func_cond_(stmt, cols, col_idx, tbl_meta, col_meta, JSON_EQUAL))) {
          LOG_ERROR("build func cond failed", "table_name", tbl_meta->getName(), K(JSON_EQUAL));
        }
      } else if (drcmsg_field_types::DRCMSG_TYPE_ORA_XML == type ||
          obmysql::MYSQL_TYPE_GEOMETRY == type) {
        if (OB_FAIL(build_normal_cond_(stmt, cols, col_idx, tbl_meta, col_meta))) {
            LOG_ERROR("build xml condition failed", "table_name", tbl_meta->getName(),
          "col_idx", col_idx);
        } else {
          // oracle xml and geometry type don't support compare operation.
          has_unsupport_type_compare = true;
        }
      } else {
        if (OB_FAIL(build_func_cond_(stmt, cols, col_idx, tbl_meta, col_meta, LOB_COMPARE))) {
          LOG_ERROR("build func cond failed", "table_name", tbl_meta->getName(), K(LOB_COMPARE));
        }
      }
    } else if (lib::Worker::CompatMode::MYSQL == compat_mode_) {
      if (obmysql::MYSQL_TYPE_JSON == type) {
        APPEND_ESCAPE_CHAR(stmt);
        APPEND_STMT(stmt, col_meta->getName());
        APPEND_ESCAPE_CHAR(stmt);
        APPEND_STMT(stmt, "=cast(");
        if (OB_SUCC(ret) && OB_FAIL(build_column_value_(stmt, col_meta, cols[col_idx]))) {
          LOG_ERROR("failed to build column_value", "table_name", tbl_meta->getName(),
              "col_idx", col_idx);
        }
        APPEND_STMT(stmt, "as json)");
      } else if (obmysql::MYSQL_TYPE_GEOMETRY == type) {
        if (OB_FAIL(build_func_cond_(stmt, cols, col_idx, tbl_meta, col_meta, ST_EQUALS))) {
          LOG_ERROR("build func cond failed", "table_name", tbl_meta->getName(), K(ST_EQUALS));
        }
      } else {
        if (OB_FAIL(build_normal_cond_(stmt, cols, col_idx, tbl_meta, col_meta))) {
            LOG_ERROR("build xml condition failed", "table_name", tbl_meta->getName(),
          "col_idx", col_idx);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compat mode is invalid", K(compat_mode_));
    }
  }
  return ret;
}

int ObLogMinerRecord::build_func_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
    ITableMeta *tbl_meta,
		IColMeta *col_meta,
    const char *func_name)
{
  int ret = OB_SUCCESS;
  APPEND_STMT(stmt, func_name);
  APPEND_STMT(stmt, "(");
  APPEND_ESCAPE_CHAR(stmt);
  APPEND_STMT(stmt, col_meta->getName());
  APPEND_ESCAPE_CHAR(stmt);
  APPEND_STMT(stmt, ", ");
  if (OB_SUCC(ret) && OB_FAIL(build_column_value_(stmt, col_meta, cols[col_idx]))) {
    LOG_ERROR("failed to build column_value", "table_name", tbl_meta->getName(),
        "col_idx", col_idx);
  }
  APPEND_STMT(stmt, ")");
  return ret;
}

int ObLogMinerRecord::build_normal_cond_(ObStringBuffer &stmt,
		binlogBuf *cols,
		const unsigned int col_idx,
		ITableMeta *tbl_meta,
		IColMeta *col_meta)
{
  int ret = OB_SUCCESS;
  APPEND_ESCAPE_CHAR(stmt);
  APPEND_STMT(stmt, col_meta->getName());
  APPEND_ESCAPE_CHAR(stmt);
  if (OB_SUCC(ret)) {
    if (nullptr == cols[col_idx].buf) {
      APPEND_STMT(stmt, " IS ");
    } else {
      APPEND_STMT(stmt, "=");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(build_column_value_(stmt, col_meta, cols[col_idx]))) {
    LOG_ERROR("failed to build column_value", "table_name", tbl_meta->getName(),
        "col_idx", col_idx);
  }
  return ret;
}

int ObLogMinerRecord::build_hex_val_(ObStringBuffer &stmt,
    binlogBuf &col_data)
{
  int ret = OB_SUCCESS;
  char byte_buf[3] = {0};
  if (OB_FAIL(stmt.reserve(stmt.length() + col_data.buf_used_size * 2))) {
    LOG_ERROR("stmt reserve failed", K(stmt), K(col_data.buf_used_size));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_data.buf_used_size; i++) {
    // there may be a sign for signed char, 2 byte is not enough, use unsigned char instead.
    unsigned char curr_byte = col_data.buf[i];
    if (OB_FAIL(databuff_printf(byte_buf, sizeof(byte_buf), "%02X", curr_byte))) {
      LOG_ERROR("failed to fill byte_buf with byte", "byte", curr_byte);
    } else {
      APPEND_STMT(stmt, byte_buf);
    }
  }

  return ret;
}

int ObLogMinerRecord::build_escape_char_(ObStringBuffer &stmt)
{
  int ret = OB_SUCCESS;
  if (is_oracle_compat_mode()) {
    if (OB_FAIL(stmt.append(ORACLE_ESCAPE_CHAR))) {
      LOG_ERROR("append stmt failed");
    }
  } else if (is_mysql_compat_mode()) {
    if (OB_FAIL(stmt.append(MYSQL_ESCAPE_CHAR))) {
      LOG_ERROR("append stmt failed");
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unsupported compact mode", K(compat_mode_));
  }
  return ret;
}


bool ObLogMinerRecord::is_string_type_(IColMeta *col_meta) const
{
  int type = col_meta->getType();
  bool bret = true;

  switch(type) {
    case obmysql::MYSQL_TYPE_TIMESTAMP:
    case obmysql::MYSQL_TYPE_DATE:
    case obmysql::MYSQL_TYPE_TIME:
    case obmysql::MYSQL_TYPE_DATETIME:
    case obmysql::MYSQL_TYPE_YEAR:
    case obmysql::MYSQL_TYPE_NEWDATE:
    case obmysql::MYSQL_TYPE_BIT:
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_NANO:
    case obmysql::MYSQL_TYPE_OB_NVARCHAR2:
    case obmysql::MYSQL_TYPE_OB_NCHAR:
    case obmysql::MYSQL_TYPE_JSON:
    case obmysql::MYSQL_TYPE_ENUM:
    case obmysql::MYSQL_TYPE_SET:
    case obmysql::MYSQL_TYPE_ORA_CLOB:
    case drcmsg_field_types::DRCMSG_TYPE_ORA_XML:
    case obmysql::MYSQL_TYPE_OB_UROWID:
    case obmysql::MYSQL_TYPE_OB_INTERVAL_YM:
    case obmysql::MYSQL_TYPE_OB_INTERVAL_DS:
    case obmysql::MYSQL_TYPE_GEOMETRY:
    case obmysql::MYSQL_TYPE_OB_RAW:
      bret = true;
      break;
    case obmysql::MYSQL_TYPE_VARCHAR:
    case obmysql::MYSQL_TYPE_TINY_BLOB:
    case obmysql::MYSQL_TYPE_MEDIUM_BLOB:
    case obmysql::MYSQL_TYPE_LONG_BLOB:
    case obmysql::MYSQL_TYPE_BLOB:
    case obmysql::MYSQL_TYPE_VAR_STRING:
    case obmysql::MYSQL_TYPE_STRING:
      if (OB_ISNULL(col_meta->getEncoding())) {
        bret = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED ,"get null col encoding for string type", K(type))
      } else if (0 == strcmp(col_meta->getEncoding(), "binary")) {
        bret = false;
      } else {
        bret = true;
      }
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

bool ObLogMinerRecord::is_binary_type_(IColMeta *col_meta) const
{
  obmysql::EMySQLFieldType type = static_cast<obmysql::EMySQLFieldType>(col_meta->getType());
  bool bret = false;
  switch(type) {
    case obmysql::MYSQL_TYPE_ORA_BLOB:
      bret = true;
      break;
    case obmysql::MYSQL_TYPE_VARCHAR:
    case obmysql::MYSQL_TYPE_TINY_BLOB:
    case obmysql::MYSQL_TYPE_MEDIUM_BLOB:
    case obmysql::MYSQL_TYPE_LONG_BLOB:
    case obmysql::MYSQL_TYPE_BLOB:
    case obmysql::MYSQL_TYPE_VAR_STRING:
    case obmysql::MYSQL_TYPE_STRING:
      if (OB_ISNULL(col_meta->getEncoding())) {
        bret = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "get null col encoding for string type", K(type))
      } else if (0 == strcmp(col_meta->getEncoding(), "binary")) {
        bret = true;
      } else {
        bret = false;
      }
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

bool ObLogMinerRecord::is_number_type_(IColMeta *col_meta) const
{
  int type = col_meta->getType();
  bool bret = false;
  switch(type) {
    case obmysql::MYSQL_TYPE_DECIMAL:
    case obmysql::MYSQL_TYPE_TINY:
    case obmysql::MYSQL_TYPE_SHORT:
    case obmysql::MYSQL_TYPE_LONG:
    case obmysql::MYSQL_TYPE_FLOAT:
    case obmysql::MYSQL_TYPE_DOUBLE:
    case obmysql::MYSQL_TYPE_LONGLONG:
    case obmysql::MYSQL_TYPE_INT24:
    case obmysql::MYSQL_TYPE_COMPLEX:
    case obmysql::MYSQL_TYPE_OB_NUMBER_FLOAT:
    case obmysql::MYSQL_TYPE_NEWDECIMAL:
    case drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_FLOAT:
    case drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_DOUBLE:
      bret = true;
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

bool ObLogMinerRecord::is_lob_type_(IColMeta *col_meta) const
{
  int type = col_meta->getType();
  bool bret = false;
  switch(type) {
    case obmysql::MYSQL_TYPE_TINY_BLOB:
    case obmysql::MYSQL_TYPE_MEDIUM_BLOB:
    case obmysql::MYSQL_TYPE_LONG_BLOB:
    case obmysql::MYSQL_TYPE_BLOB:
    case obmysql::MYSQL_TYPE_ORA_BLOB:
    case obmysql::MYSQL_TYPE_ORA_CLOB:
    case obmysql::MYSQL_TYPE_JSON:
    case obmysql::MYSQL_TYPE_GEOMETRY:
    case drcmsg_field_types::DRCMSG_TYPE_ORA_XML:
      bret = true;
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

bool ObLogMinerRecord::is_geo_type_(IColMeta *col_meta) const
{
  obmysql::EMySQLFieldType type = static_cast<obmysql::EMySQLFieldType>(col_meta->getType());
  bool bret = false;
  if (obmysql::MYSQL_TYPE_GEOMETRY == type) {
    bret = true;
  }
  return bret;
}

bool ObLogMinerRecord::is_bit_type_(IColMeta *col_meta) const
{
  obmysql::EMySQLFieldType type = static_cast<obmysql::EMySQLFieldType>(col_meta->getType());
  bool bret = false;
  if (obmysql::MYSQL_TYPE_BIT == type) {
    bret = true;
  }
  return bret;
}


}
}

#undef APPEND_STMT
#undef APPEND_ESCAPE_CHAR