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
 * Binlog Record
 */

#ifndef OCEANBASE_LIBOBCDC_BINLOG_RECORD_
#define OCEANBASE_LIBOBCDC_BINLOG_RECORD_

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/BR.h>                               // IBinlogRecord
#include <drcmsg/DRCMessageFactory.h>                // createBinlogRecord
#endif
#include "lib/queue/ob_link.h"                       // ObLink

#include "share/ob_define.h"
#include "lib/oblog/ob_log_module.h"                 // OBLOG_LOG
#include "lib/string/ob_string.h"                    // ObString

#include "ob_log_resource_recycle_task.h"            // ObLogResourceRecycleTask

namespace oceanbase
{
namespace libobcdc
{

class ObLogBR : public ObLogResourceRecycleTask, public common::ObLink
{
public:
  static const uint64_t MIN_DRC_CLUSTER_ID = 4294901760; // 0xffff0000
  static const uint64_t MAX_DRC_CLUSTER_ID = 4294967295; // 0xffffffff

public:
  ObLogBR();
  virtual ~ObLogBR();

public:
  static int put_old(IBinlogRecord *br, const bool is_changed);

public:
  void reset();

  // init INSERT/UPDATE/DELETE/DDL/BEGIN/COMMIT/HEARTBEAT Binlog Record
  // unique id of BinlogRecord: Pkey + LogId + LogOffset + row_index
  // 1. DML statement
  //    set unique id by puFilterRuleVal
  // 2. DDL statement
  //    set tenant id and schema version
  int init_data(const RecordType type,
      const uint64_t cluster_id,
      const int64_t tenant_id,
      const uint64_t row_index,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const common::ObString &unique_id,
      const int64_t schema_version,
      const int64_t commit_version,
      const int64_t part_trans_task_count = 0,
      const common::ObString *major_version_str = NULL);

  IBinlogRecord *get_data() { return data_; }
  int get_record_type(int &record_type);

  void set_next(ObLogBR *next) {next_br_ = next;};
  ObLogBR *get_next() {return next_br_;};

  void set_is_valid(const bool is_valid) { valid_ = is_valid; }
  bool is_valid() const { return valid_ && NULL != data_; }

  int set_table_meta(ITableMeta *table_meta);
  int set_db_meta(IDBMeta *db_meta);

  inline int64_t get_commit_version() const { return commit_version_; }

  inline void *get_host() { return host_; }
  void set_host(void *host) { host_ = host; }

  inline void *get_stmt_task() { return stmt_task_; }
  void set_stmt_task(void *stmt_task) { stmt_task_ = stmt_task; }

  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_schema_version() const { return schema_version_; }
  uint64_t get_row_index() const { return row_index_; }
  int64_t get_part_trans_task_count() const { return part_trans_task_count_; }

  // Aone:
  // for put operation of HBASE: store data type as update, new value use full-column mode, old value is empty
  // special treatment for libobcdc:
  // TODO:observer add new dml operation type to represend PUT operation
  int setInsertRecordTypeForHBasePut(const RecordType type);

 public:
  TO_STRING_KV(K_(valid),
      K_(commit_version));

private:
  int verify_part_trans_task_count_(const RecordType type,
    const int64_t part_trans_task_count);

  // ob defined cluster id:  0x00000001~0xfffeffff. [1, 4294901759]
  // drc defined cluster id: 0xffff0000-0xffffffff, [4294901760~4294967295]
  // libobcdc valid cluster_id: [0x00000001~0xffffffff]
  bool is_valid_cluster_id_(const uint64_t cluster_id) const;

private:
  IBinlogRecord *data_;               ///< real BinlogRecord
  void          *host_;               ///< record corresponsding ObLogEntryTask
  void          *stmt_task_;          // StmtTask
  ObLogBR       *next_br_;
  bool          valid_;               ///< statement is valid or not

  uint64_t      tenant_id_;
  // corresponding schema version for DDL
  // use schema version allocated by Sequencer for DML
  // 0 for HEARTBEAT
  int64_t       schema_version_;

  int64_t       commit_version_;      ///< transaction commit version in micro seconds

  // DML/DDL record row_index(start from 0 in participants, not global)
  uint64_t      row_index_;

  // Number of tasks in the transaction partition, i.e. number of participants in the transaction
  // 1. DDL
  // 2. DML begin/commit binglog record will carry this info
  int64_t       part_trans_task_count_;

protected:
  /*
   * DRCMessageFactory
   *  static IBinlogRecord* createBinlogRecord(
   *      const std::string &type = DFT_BR, bool creating = true);
   *
   *  @param creating is to differentiate two kinds of usage, if creating is
   *         true, it means the created binlog record has not been
   *         serilized, all in-memory functions can be called. Otherwise
   *         if creating is false, only after-serialized function could
   *         be called
   */
  void construct_data_(const bool creating_binlog_record);

private:
  void destruct_data_();
};

class ObLogUnserilizedBR : public ObLogBR
{
public:
  ObLogUnserilizedBR();
  virtual ~ObLogUnserilizedBR();

private:
  // Build unserialized IBinlogRecord, in-memory operations
  void construct_unserilized_data_();
};

class ObLogSerilizedBR : public ObLogBR
{
public:
  ObLogSerilizedBR();
  virtual ~ObLogSerilizedBR();

private:
  // Build serialized IBinlogRecord, parse based on persistent data
  void construct_serilized_data_();
};

} // end namespace libobcdc
} // end namespace oceanbase

#endif // end OCEANBASE_LIBOBCDC_BINLOG_RECORD_
