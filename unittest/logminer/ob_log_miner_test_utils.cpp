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

#include "ob_log_binlog_record.h"
#include "ob_log_miner_br.h"
#include "ob_log_miner_test_utils.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

ObLogMinerBR *v_build_logminer_br(binlogBuf *new_bufs,
              binlogBuf *old_bufs,
              RecordType type,
              lib::Worker::CompatMode compat_mode,
              const char *db_name,
              const char *table_name,
              const char *encoding,
              const int arg_count, va_list *ap)
{
  ObLogMinerBR *br = new ObLogMinerBR;
  EXPECT_NE(nullptr, br);
  libobcdc::ObLogBR *cdc_br = new libobcdc::ObLogBR;
  cdc_br->init_data(EINSERT, 0, 1, 0, ObString(), ObString(), ObString(),
      100, 100);
  ICDCRecord *rec = DRCMessageFactory::createBinlogRecord();
  rec->setUserData(cdc_br);
  EXPECT_NE(nullptr, rec);
  ITableMeta *tab_meta = DRCMessageFactory::createTableMeta();
  EXPECT_NE(nullptr, tab_meta);
  IDBMeta *db_meta = DRCMessageFactory::createDBMeta();
  EXPECT_NE(nullptr, db_meta);
  rec->setNewColumn(new_bufs, arg_count/3);
  rec->setOldColumn(old_bufs, arg_count/3);
  rec->setRecordType(type);
  rec->setDbname(db_name);
  rec->setTbname(table_name);
  db_meta->setName(db_name);
  tab_meta->setPKs("");
  tab_meta->setPkinfo("");
  tab_meta->setUKs("");
  tab_meta->setUkinfo("");
  tab_meta->setName(table_name);
  tab_meta->setDBMeta(db_meta);
  IColMeta *col_meta = nullptr;
  const char *next = nullptr;
  int64_t arg_idx = 0;
  for (int i = 0; i < arg_count; i++) {
    const char *col_arg = nullptr;
    int len = 0;
    int data_type = 0;
    if (3 > i%4) {
      col_arg = va_arg(*ap, const char *);
      len = col_arg == nullptr ? 0 : strlen(col_arg);
    } else {
      data_type = va_arg(*ap, int);
    }
    if (0 == i%4) { // column name
      col_meta = DRCMessageFactory::createColMeta();
      EXPECT_NE(col_meta, nullptr);
      col_meta->setName(col_arg);
      col_meta->setEncoding(encoding);
      tab_meta->append(col_arg, col_meta);
    } else if (1 == i%4 && EDELETE != type) { // new value, EDELETE hasn't new value
      rec->putNew(col_arg, len);
    } else if (2 == i%4 && EINSERT != type) { // old value, EINSERT hasn't old value
      rec->putOld(col_arg, len);
    } else if (3 == i%4) { // column data type
      col_meta->setType(data_type);
    }
  }
  rec->setTableMeta(tab_meta);
  br->init(nullptr, rec, compat_mode, 0, 1, 0);
  return br;
}

ObLogMinerBR *build_logminer_br(binlogBuf *new_bufs,
              binlogBuf *old_bufs,
              RecordType type,
              lib::Worker::CompatMode compat_mode,
              const char *db_name,
              const char *table_name,
              const char *encoding,
              const int arg_count, ...)
{
  va_list ap;
  ObLogMinerBR *br = nullptr;
  va_start(ap, arg_count);
  br = v_build_logminer_br(new_bufs, old_bufs, type,
      compat_mode, db_name, table_name, encoding, arg_count, &ap);
  va_end(ap);
  return br;
}

ObLogMinerBR *build_logminer_br(binlogBuf *new_bufs,
              binlogBuf *old_bufs,
              RecordType type,
              lib::Worker::CompatMode compat_mode,
              const char *db_name,
              const char *table_name,
              const int arg_count, ...)
{
  va_list ap;
  ObLogMinerBR *br = nullptr;
  va_start(ap, arg_count);
  br = v_build_logminer_br(new_bufs, old_bufs, type,
      compat_mode, db_name, table_name, "utf8mb4", arg_count, &ap);
  va_end(ap);
  return br;
}

void destroy_miner_br(ObLogMinerBR *&br) {
  ICDCRecord *cdc_br = br->get_br();
  IDBMeta *db_meta = cdc_br->getDBMeta();
  ITableMeta *tbl_meta = cdc_br->getTableMeta();
  delete static_cast<libobcdc::ObLogBR*>(cdc_br->getUserData());
  DRCMessageFactory::destroy(db_meta);
  DRCMessageFactory::destroy(tbl_meta);
  DRCMessageFactory::destroy(cdc_br);
  delete br;
  br = nullptr;
}

ObLogMinerRecord *build_logminer_record(ObIAllocator &alloc,
                  lib::Worker::CompatMode	compat_mode,
                  uint64_t tenant_id,
                  int64_t orig_cluster_id,
                  const char *tenant_name,
                  const char *database_name,
                  const char *tbl_name,
                  int64_t trans_id,
                  const char* const * pks,
                  const int64_t pk_cnt,
                  const char* const * uks,
                  const int64_t uk_cnt,
                  const char* row_unique_id,
                  RecordType record_type,
                  int64_t commit_ts_us,
                  const char * redo_stmt,
                  const char * undo_stmt)
{
  void *ptr = ob_malloc(sizeof(ObLogMinerRecord), "testminer_rec");
  ObLogMinerRecord *rec = new (ptr) (ObLogMinerRecord);
  rec->set_allocator(&alloc);

  rec->compat_mode_ = compat_mode;
  rec->tenant_id_ = tenant_id;
  rec->orig_cluster_id_ = orig_cluster_id;
  rec->tenant_name_.assign(tenant_name);
  rec->database_name_.assign(database_name);
  rec->table_name_.assign(tbl_name);
  rec->trans_id_ = trans_id;
  for (int i = 0; i < pk_cnt; i++) {
    rec->primary_keys_.push_back(pks[i]);
  }
  for (int i = 0; i < uk_cnt; i++) {
    rec->unique_keys_.push_back(uks[i]);
  }
  rec->row_unique_id_ = row_unique_id;
  rec->record_type_ = record_type;
  rec->commit_scn_.convert_from_ts(commit_ts_us);
  rec->redo_stmt_.append(redo_stmt);
  rec->undo_stmt_.append(undo_stmt);

  return rec;
}

void destroy_miner_record(ObLogMinerRecord *&rec)
{
  rec->redo_stmt_.reset();
  rec->undo_stmt_.reset();
  ob_free(rec);
}
}
}