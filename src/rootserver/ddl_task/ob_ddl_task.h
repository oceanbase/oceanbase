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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_TASK_H_

#include "lib/container/ob_array.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/trace/ob_trace.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_ddl_common.h"
#include "share/longops_mgr/ob_ddl_longops.h"
#include "rootserver/ddl_task/ob_ddl_single_replica_executor.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;

struct ObDDLTaskRecord;
struct ObDDLTaskKey final
{
public:
  ObDDLTaskKey();
  ObDDLTaskKey(const uint64_t tenant_id, const int64_t object_id, const int64_t schema_version);
  ~ObDDLTaskKey() = default;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ObDDLTaskKey &other) const;
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != object_id_ && schema_version_ > 0;}
  int assign(const ObDDLTaskKey &other);
  TO_STRING_KV(K_(tenant_id), K_(object_id), K_(schema_version));
public:
  uint64_t tenant_id_;
  int64_t object_id_;
  int64_t schema_version_;
};

struct ObDDLTaskID final
{
public:
  ObDDLTaskID();
  ObDDLTaskID(const uint64_t tenant_id, const int64_t task_id);
  ~ObDDLTaskID() = default;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ObDDLTaskID &other) const;
  bool operator!=(const ObDDLTaskID &other) const;
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && task_id_ > 0; }
  int assign(const ObDDLTaskID &other);
  TO_STRING_KV(K_(tenant_id), K_(task_id));
public:
  uint64_t tenant_id_;
  int64_t task_id_;
};

struct ObDDLTaskRecord final
{
public:
  ObDDLTaskRecord() { reset(); }
  ~ObDDLTaskRecord() {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(task_id), K_(parent_task_id), K_(ddl_type), K_(trace_id), K_(task_status), K_(tenant_id), K_(object_id),
      K_(schema_version), K_(target_object_id), K_(snapshot_version), K_(message), K_(task_version), K_(ret_code), K_(execution_id),
      K_(ddl_need_retry_at_executor));
public:
  static const int64_t MAX_MESSAGE_LENGTH = 4096;
  typedef common::ObFixedLengthString<MAX_MESSAGE_LENGTH> TaskMessage;
public:
  uint64_t gmt_create_;
  int64_t task_id_;
  int64_t parent_task_id_;
  share::ObDDLType ddl_type_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t task_status_;
  uint64_t tenant_id_;
  uint64_t object_id_;
  uint64_t schema_version_;
  uint64_t target_object_id_;
  int64_t snapshot_version_;
  ObString message_;
  int64_t task_version_;
  int64_t ret_code_;
  int64_t execution_id_;
  ObString ddl_stmt_str_;
  bool ddl_need_retry_at_executor_;
};

struct ObDDLTaskInfo final
{
public:
  ObDDLTaskInfo() : row_scanned_(0), row_inserted_(0) {}
  ~ObDDLTaskInfo() {}
  TO_STRING_KV(K_(row_scanned), K_(row_inserted), K_(ls_id), K_(ls_leader_addr), K_(partition_ids));
public:
  int64_t row_scanned_;
  int64_t row_inserted_;
  share::ObLSID ls_id_;
  common::ObAddr ls_leader_addr_;
  ObArray<ObTabletID> partition_ids_;
};

struct ObFTSDDLChildTaskInfo final
{
public:
  ObFTSDDLChildTaskInfo() : index_name_(), table_id_(OB_INVALID_ID), task_id_(0) {}
  ObFTSDDLChildTaskInfo(
      common::ObString &index_name,
      const uint64_t table_id,
      const int64_t task_id)
    : index_name_(index_name),
      table_id_(table_id),
      task_id_(task_id)
  {}
  ~ObFTSDDLChildTaskInfo() = default;
  bool is_valid() const { return OB_INVALID_ID != table_id_ && !index_name_.empty(); }
  int deep_copy_from_other(const ObFTSDDLChildTaskInfo &other, common::ObIAllocator &allocator);
  TO_STRING_KV(K_(table_id), K_(task_id), K_(index_name));
  OB_UNIS_VERSION(1);
public:
  common::ObString index_name_;
  uint64_t table_id_;
  // The following fields are not persisted to the `__all_ddl_task_status` system table.
  int64_t task_id_;
};

struct ObDDLTaskSerializeField final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_version), K_(parallelism), K_(data_format_version), K_(consumer_group_id),
               K_(is_abort), K_(sub_task_trace_id), K_(is_unique_index), K_(is_global_index) ,K_(is_pre_split));
  ObDDLTaskSerializeField() : task_version_(0), parallelism_(0), data_format_version_(0),
                              consumer_group_id_(0), is_abort_(false), sub_task_trace_id_(0),
                              is_unique_index_(false), is_global_index_(false), is_pre_split_(false) {}
  ObDDLTaskSerializeField(const int64_t task_version,
                          const int64_t parallelism,
                          const uint64_t data_format_version,
                          const int64_t consumer_group_id,
                          const bool is_abort,
                          const int32_t sub_task_trace_id,
                          const bool is_unique_index = false,
                          const bool is_global_index = false,
                          const bool is_pre_split = false);
  ~ObDDLTaskSerializeField() = default;
  void reset();
public:
  int64_t task_version_;
  int64_t parallelism_;
  uint64_t data_format_version_;
  int64_t consumer_group_id_;
  bool is_abort_;
  int32_t sub_task_trace_id_;
  bool is_unique_index_;
  bool is_global_index_;
  bool is_pre_split_;
};

struct ObCreateDDLTaskParam final
{
public:
  ObCreateDDLTaskParam();
  ObCreateDDLTaskParam(const uint64_t tenant_id,
                       const share::ObDDLType &type,
                       const ObTableSchema *src_table_schema,
                       const ObTableSchema *dest_table_schema,
                       const int64_t object_id,
                       const int64_t schema_version,
                       const int64_t parallelism,
                       const int64_t consumer_group_id,
                       ObIAllocator *allocator,
                       const obrpc::ObDDLArg *ddl_arg = nullptr,
                       const int64_t parent_task_id = 0,
                       const int64_t task_id = 0,
                       const bool ddl_need_retry_at_executor = false);
  ~ObCreateDDLTaskParam() = default;
  bool is_valid() const { return OB_INVALID_ID != tenant_id_ && type_ > share::DDL_INVALID
                                 && type_ < share::DDL_MAX && nullptr != allocator_; }
  TO_STRING_KV(K_(tenant_id), K_(object_id), K_(schema_version), K_(parallelism), K_(consumer_group_id), K_(parent_task_id), K_(task_id),
               K_(type), KPC_(src_table_schema), KPC_(dest_table_schema), KPC_(ddl_arg), K_(tenant_data_version),
               K_(sub_task_trace_id), KPC_(aux_rowkey_doc_schema), KPC_(aux_doc_rowkey_schema), KPC_(aux_doc_word_schema),
               K_(ddl_need_retry_at_executor), K_(is_pre_split));
public:
  int32_t sub_task_trace_id_;
  uint64_t tenant_id_;
  int64_t object_id_;
  int64_t schema_version_;
  int64_t parallelism_;
  int64_t consumer_group_id_;
  int64_t parent_task_id_;
  int64_t task_id_;
  share::ObDDLType type_;
  const ObTableSchema *src_table_schema_;
  const ObTableSchema *dest_table_schema_;
  const obrpc::ObDDLArg *ddl_arg_;
  common::ObIAllocator *allocator_;
  const ObTableSchema *aux_rowkey_doc_schema_;
  const ObTableSchema *aux_doc_rowkey_schema_;
  const ObTableSchema *aux_doc_word_schema_;
  uint64_t tenant_data_version_;
  bool ddl_need_retry_at_executor_;
  bool is_pre_split_;
};

class ObDDLTaskRecordOperator final
{
public:
  static int update_task_status(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t task_status);

  static int update_snapshot_version(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t snapshot_version);

  static int update_ret_code(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t ret_code);

  static int update_execution_id(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t execution_id);

  static int update_message(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const ObString &message);

  static int update_status_and_message(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t task_status,
      ObString &message);

  static int update_ret_code_and_message(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int ret_code,
      ObString &message);

  static int delete_record(
      common::ObMySQLProxy &proxy,
      const uint64_t tenant_id,
      const int64_t task_id);

  static int select_for_update(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t task_id,
      int64_t &task_status,
      int64_t &execution_id,
      int64_t &ret_code);

  static int get_ddl_task_record(
      const uint64_t tenant_id,
      const int64_t task_id,
      common::ObMySQLProxy &proxy,
      common::ObIAllocator &allocator,
      ObDDLTaskRecord &record);
  static int get_all_ddl_task_record(
      common::ObMySQLProxy &proxy,
      common::ObIAllocator &allocator,
      common::ObIArray<ObDDLTaskRecord> &records);

  static int check_task_id_exist(
      common::ObMySQLProxy &proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      bool &exist);

  static int check_is_adding_constraint(
     common::ObMySQLProxy *proxy,
     common::ObIAllocator &allocator,
     const uint64_t tenant_id,
     const uint64_t table_id,
     bool &is_building);

  // To check if any long-time running DDL exists.
  static int check_has_long_running_ddl(
     common::ObMySQLProxy *proxy,
     const uint64_t tenant_id,
     const uint64_t table_id,
     const share::ObCheckExistedDDLMode check_mode,
     bool &has_long_running_ddl);

  static int check_has_conflict_ddl(
      common::ObMySQLProxy *proxy,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t task_id,
      const share::ObDDLType ddl_type,
      bool &has_conflict_ddl);

  static int check_has_index_or_mlog_task(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      bool &has_index_task);

  static int get_create_index_or_mlog_task_cnt(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    int64_t &task_cnt);

  static int insert_record(
      common::ObISQLClient &proxy,
      ObDDLTaskRecord &record);

  static int to_hex_str(const ObString &src, ObSqlString &dst);

  static int kill_task_inner_sql(
      common::ObMySQLProxy &proxy,
      const common::ObCurTraceId::TraceId &trace_id,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t snapshot_version,
      const ObIArray<common::ObAddr> &sql_exec_addrs);

  //query the internal table __all_virtual_session_info to obtain the executing tasks sql meeting specified mode.
  static int get_running_tasks_inner_sql(
      common::ObMySQLProxy &proxy,
      const common::ObCurTraceId::TraceId &trace_id,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t snapshot_version,
      const common::ObAddr &sql_exec_addr,
      common::ObIAllocator &allocator,
      common::ObIArray<ObString> &records);

private:
  static int fill_task_record(
      const uint64_t tenant_id,
      const common::sqlclient::ObMySQLResult *result_row,
      common::ObIAllocator &allocator,
      ObDDLTaskRecord &task_record);

  static int64_t get_record_id(share::ObDDLType ddl_type, int64_t origin_id);
  static int kill_inner_sql(
      common::ObMySQLProxy &proxy,
      const uint64_t tenant_id,
      const uint64_t session_id);

  static int get_task_record(
      const uint64_t tenant_id,
      const ObSqlString &sql_string,
      common::ObMySQLProxy &proxy,
      common::ObIAllocator &allocator,
      common::ObIArray<ObDDLTaskRecord> &records);
};

class ObDDLWaitTransEndCtx
{
public:
  enum WaitTransType
  {
    MIN_WAIT_TYPE = 0,
    WAIT_SCHEMA_TRANS,
    WAIT_SSTABLE_TRANS,
    MAX_WAIT_TYPE
  };
public:
  ObDDLWaitTransEndCtx();
  ~ObDDLWaitTransEndCtx();
  int init(
      const uint64_t tenant_id,
      const int64_t ddl_task_id,
      const uint64_t table_id,
      const WaitTransType wait_trans_type,
      const int64_t wait_version);
  void reset();
  bool is_inited() const { return is_inited_; }
  int try_wait(bool &is_trans_end, int64_t &snapshot_version, const bool need_wait_trans_end = true);
  transaction::ObTransID get_pending_tx_id() const { return pending_tx_id_; }
  TO_STRING_KV(K(is_inited_), K_(tenant_id), K(table_id_), K(is_trans_end_), K(wait_type_),
      K(wait_version_), K_(pending_tx_id), K(tablet_ids_.count()), K(snapshot_array_.count()), K(ddl_task_id_));

public:
  /**
   * To calculate the final snapshot version used for writing macro block.
   * @param [in] tenant_id
   * @param [in] trans_end_snapshot: usually the snapshot version obtained after wait trans end.
   * @param [out] snapshot: used for data scan, row trans version for ddl.
  */
  static int calc_snapshot_with_gts(
      const uint64_t tenant_id,
      const int64_t ddl_task_id,
      const int64_t trans_end_snapshot,
      int64_t &snapshot);
private:
  static bool is_wait_trans_type_valid(const WaitTransType wait_trans_type);
  int get_snapshot_check_list(
      common::ObIArray<common::ObTabletID> &need_check_tablets,
      ObIArray<int64_t> &tablet_pos_indexes);
  int get_snapshot(int64_t &snapshot_version);

  // check if all transactions before a schema version have ended
  int check_schema_trans_end(
      const int64_t schema_version,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<int> &ret_array,
      common::ObIArray<int64_t> &snapshot_array,
      const uint64_t tenant_id,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      share::ObLocationService *location_service,
      const bool need_wait_trans_end);

  // check if all transactions before a timestamp have ended
   int check_sstable_trans_end(
      const uint64_t tenant_id,
      const int64_t sstable_exist_ts,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      share::ObLocationService *location_service,
      common::ObIArray<int> &ret_array,
      common::ObIArray<int64_t> &snapshot_array);
private:
  static const int64_t INDEX_SNAPSHOT_VERSION_DIFF = 100 * 1000 * 1000; // 100ms
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  bool is_trans_end_;
  WaitTransType wait_type_;
  int64_t wait_version_;
  transaction::ObTransID pending_tx_id_;
  common::ObArray<common::ObTabletID> tablet_ids_;
  common::ObArray<int64_t> snapshot_array_;
  int64_t ddl_task_id_;
};

class ObDDLTask;

struct ObDDLTracing final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLTracing() = delete;
  explicit ObDDLTracing(const ObDDLTask *ddl_task)
    : trace_ctx_(), task_span_id_(), status_span_id_(), parent_task_span_id_(),
      task_start_ts_(0), status_start_ts_(0), parent_task_span_(nullptr),
      task_span_(nullptr), status_span_(nullptr), task_(ddl_task),
      is_status_span_begin_(false), is_status_span_end_(false), is_task_span_flushed_(false)
  {}
  bool is_valid() const
  {
    return task_span_id_.low_ != 0 && task_span_id_.high_ != 0 &&
           status_span_id_.low_ != 0 && status_span_id_.high_ != 0 &&
           parent_task_span_id_.low_ != 0 && parent_task_span_id_.high_ != 0 &&
           task_start_ts_ != 0 && status_start_ts_ != 0;
  }
  void open();
  void open_for_recovery();
  void restore_span_hierarchy();
  void release_span_hierarchy();
  void end_status_span();
  void close();

private:
  void init_span_id(trace::ObSpanCtx *span);
  void init_task_span();
  void init_status_span();
  trace::ObSpanCtx* begin_task_span();
  void end_task_span();
  trace::ObSpanCtx* begin_status_span(const share::ObDDLTaskStatus status);
  trace::ObSpanCtx* restore_parent_task_span();
  trace::ObSpanCtx* restore_task_span();
  trace::ObSpanCtx* restore_status_span();
  void record_trace_ctx();
  void record_parent_task_span(trace::ObSpanCtx *span);
  void record_task_span(trace::ObSpanCtx *span);
  void record_status_span(trace::ObSpanCtx *span);

private:
  // members that will be serialized to ddl task record
  trace::FltTransCtx trace_ctx_;
  trace::UUID task_span_id_;      // build index task, drop index task etc
  trace::UUID status_span_id_;    // status: prepare, succ etc
  trace::UUID parent_task_span_id_;
  int64_t task_start_ts_;
  int64_t status_start_ts_;
  // members that will not be serialized
  trace::ObSpanCtx *parent_task_span_;
  trace::ObSpanCtx *task_span_;
  trace::ObSpanCtx *status_span_;
  const ObDDLTask *task_;
  bool is_status_span_begin_;
  bool is_status_span_end_;
  bool is_task_span_flushed_;
};

struct ObDDLTaskStatInfo final
{
public:
  ObDDLTaskStatInfo();
  ~ObDDLTaskStatInfo() = default;
  int init(const char *&ddl_type_str, const uint64_t table_id);
  TO_STRING_KV(K_(start_time), K_(finish_time), K_(time_remaining), K_(percentage),
               K_(op_name), K_(target), K_(message));
public:
  int64_t start_time_;
  int64_t finish_time_;
  int64_t time_remaining_;
  int64_t percentage_;
  char op_name_[common::MAX_LONG_OPS_NAME_LENGTH];
  char target_[common::MAX_LONG_OPS_TARGET_LENGTH];
  char message_[common::MAX_LONG_OPS_MESSAGE_LENGTH];
};

class ObDDLTask : public common::ObDLinkBase<ObDDLTask>
{
public:
  explicit ObDDLTask(const share::ObDDLType task_type)
    : lock_(), ddl_tracing_(this), is_inited_(false), need_retry_(true), is_running_(false), is_abort_(false),
      task_type_(task_type), trace_id_(), sub_task_trace_id_(0), tenant_id_(0), dst_tenant_id_(0), object_id_(0), schema_version_(0), dst_schema_version_(0),
      target_object_id_(0), task_status_(share::ObDDLTaskStatus::PREPARE), snapshot_version_(0), ret_code_(OB_SUCCESS), task_id_(0),
      parent_task_id_(0), parent_task_key_(), task_version_(0), parallelism_(0),
      allocator_(lib::ObLabel("DdlTask")), compat_mode_(lib::Worker::CompatMode::INVALID), err_code_occurence_cnt_(0),
      longops_stat_(nullptr), gmt_create_(0), stat_info_(), delay_schedule_time_(0), next_schedule_ts_(0),
      execution_id_(-1), start_time_(0), data_format_version_(0), is_pre_split_(false)
  {}
  virtual ~ObDDLTask() {}
  virtual int process() = 0;
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) { return common::OB_NOT_SUPPORTED; }
  virtual bool is_valid() const { return is_inited_; }
  typedef common::ObCurTraceId::TraceId TraceId;
  virtual const TraceId &get_trace_id() const { return trace_id_; }
  virtual int set_trace_id(const TraceId &trace_id) { return trace_id_.set(trace_id.get()); }
  virtual bool need_retry() const { return need_retry_; };
  share::ObDDLType get_task_type() const { return task_type_; }
  void set_not_running() { ATOMIC_SET(&is_running_, false); }
  void set_task_status(const share::ObDDLTaskStatus new_status) {task_status_ = new_status; }
  void set_is_abort(const bool is_abort) { is_abort_ = is_abort; }
  bool get_is_abort() { return is_abort_; }
  void set_consumer_group_id(const int64_t group_id) { consumer_group_id_ = group_id; }
  void set_sub_task_trace_id(const int32_t sub_task_trace_id) { sub_task_trace_id_ = sub_task_trace_id; }
  void add_event_info(const ObString &ddl_event_stmt);
  void add_event_info(const share::ObDDLTaskStatus status, const uint64_t tenant_id);
  bool try_set_running() { return !ATOMIC_CAS(&is_running_, false, true); }
  uint64_t get_tenant_id() const { return dst_tenant_id_; }
  uint64_t get_object_id() const { return object_id_; }
  int64_t get_schema_version() const { return dst_schema_version_; }
  uint64_t get_target_object_id() const { return target_object_id_; }
  int64_t get_task_status() const { return task_status_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  int get_ddl_type_str(const int64_t ddl_type, const char *&ddl_type_str);
  int64_t get_ret_code() const { return ret_code_; }
  int64_t get_task_id() const { return task_id_; }
  ObDDLTaskID get_ddl_task_id() const { return ObDDLTaskID(dst_tenant_id_, task_id_); }
  ObDDLTaskKey get_task_key() const { return ObDDLTaskKey(dst_tenant_id_, target_object_id_, dst_schema_version_); }
  int64_t get_parent_task_id() const { return parent_task_id_; }
  int64_t get_task_version() const { return task_version_; }
  int64_t get_parallelism() const { return parallelism_; }
  uint64_t get_gmt_create() const { return gmt_create_; }
  void set_gmt_create(uint64_t gmt_create) { gmt_create_ = gmt_create; }
  static int deep_copy_table_arg(common::ObIAllocator &allocator,
                                 const obrpc::ObDDLArg &source_arg,
                                 obrpc::ObDDLArg &dest_arg);
  void set_longops_stat(share::ObDDLLongopsStat *longops_stat) { longops_stat_ = longops_stat; }
  share::ObDDLLongopsStat *get_longops_stat() const { return longops_stat_; }
  uint64_t get_data_format_version() const { return data_format_version_; }
  static int fetch_new_task_id(ObMySQLProxy &sql_proxy, const uint64_t tenant_id, int64_t &new_task_id);
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const;
  virtual int deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos);
  virtual int64_t get_serialize_param_size() const;
  const ObString &get_ddl_stmt_str() const { return ddl_stmt_str_; }
  int set_ddl_stmt_str(const ObString &ddl_stmt_str);
  int convert_to_record(ObDDLTaskRecord &task_record, common::ObIAllocator &allocator);
  int switch_status(const share::ObDDLTaskStatus new_status, const bool enable_flt, const int ret_code);
  int refresh_status();
  int refresh_schema_version();
  int remove_task_record();
  int report_error_code(const ObString &forward_user_message, const int64_t affected_rows = 0);
  int check_ddl_task_is_cancel(const TraceId &trace_id, bool &is_cancel);
  int wait_trans_end(
      ObDDLWaitTransEndCtx &wait_trans_ctx,
      const share::ObDDLTaskStatus next_task_status);
  lib::Worker::CompatMode get_compat_mode() { return compat_mode_; }
  int batch_release_snapshot(
      const int64_t snapshot_version, 
      const common::ObIArray<common::ObTabletID> &tablet_ids);
  int set_sql_exec_addr(const common::ObAddr &addr);
  int remove_sql_exec_addr(const common::ObAddr &addr);
  void set_sys_task_id(const TraceId &sys_task_id) { sys_task_id_ = sys_task_id; }
  const TraceId &get_sys_task_id() const { return sys_task_id_; }
  virtual int collect_longops_stat(share::ObLongopsValue &value);

  void calc_next_schedule_ts(const int ret_code, const int64_t total_task_cnt);
  void disable_schedule() { next_schedule_ts_ = INT64_MAX; }
  void enable_schedule() { next_schedule_ts_ = 0; }
  bool need_schedule() { return next_schedule_ts_ <= ObTimeUtility::current_time(); }
  bool is_replica_build_need_retry(const int ret_code);
  int64_t get_execution_id() const;
  static int push_execution_id(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType task_type,
      const bool ddl_can_retry,
      const int64_t data_format_version,
      int64_t &new_execution_id);
  void check_ddl_task_execute_too_long();
  static bool check_is_load_data(share::ObDDLType task_type);
  virtual bool support_longops_monitoring() const { return false; }
  int cleanup();
  virtual int cleanup_impl() = 0;
  virtual void flt_set_task_span_tag() const = 0;
  virtual void flt_set_status_span_tag() const = 0;
  int update_task_record_status_and_msg(common::ObISQLClient &proxy, const share::ObDDLTaskStatus real_new_status);

  #ifdef ERRSIM
  int check_errsim_error();
  #endif
  VIRTUAL_TO_STRING_KV(
      K(is_inited_), K(need_retry_), K(is_abort_), K(task_type_), K(trace_id_), K(sub_task_trace_id_),
      K(tenant_id_), K(dst_tenant_id_), K(object_id_), K(schema_version_),
      K(target_object_id_), K(task_status_), K(snapshot_version_),
      K_(ret_code), K_(task_id), K_(parent_task_id), K_(parent_task_key),
      K_(task_version), K_(parallelism), K_(ddl_stmt_str), K_(compat_mode),
      K_(sys_task_id), K_(err_code_occurence_cnt), K_(stat_info),
      K_(next_schedule_ts), K_(delay_schedule_time), K(execution_id_), K(sql_exec_addrs_), K_(data_format_version), K(consumer_group_id_),
      K_(dst_tenant_id), K_(dst_schema_version), K_(is_pre_split));
  static const int64_t MAX_ERR_TOLERANCE_CNT = 3L; // Max torlerance count for error code.
  static const int64_t DEFAULT_TASK_IDLE_TIME_US = 10L * 1000L; // 10ms
protected:
  int gather_redefinition_stats(const uint64_t tenant_id,
                                const int64_t task_id,
                                ObMySQLProxy &sql_proxy,
                                int64_t &row_scanned,
                                int64_t &row_sorted,
                                int64_t &row_inserted_cg,
                                int64_t &row_inserted_file);
  int gather_scanned_rows(
      const uint64_t tenant_id,
      const int64_t task_id,
      ObMySQLProxy &sql_proxy,
      int64_t &row_scanned);
  int gather_sorted_rows(
      const uint64_t tenant_id,
      const int64_t task_id,
      ObMySQLProxy &sql_proxy,
      int64_t &row_sorted);
  int gather_inserted_rows(
      const uint64_t tenant_id,
      const int64_t task_id,
      ObMySQLProxy &sql_proxy,
      int64_t &row_inserted_cg,
      int64_t &row_inserted_file);
  int copy_longops_stat(share::ObLongopsValue &value);
  virtual bool is_error_need_retry(const int ret_code)
  {
    return task_can_retry() && (!share::ObIDDLTask::in_ddl_retry_black_list(ret_code) && (share::ObIDDLTask::in_ddl_retry_white_list(ret_code)
             || MAX_ERR_TOLERANCE_CNT > ++err_code_occurence_cnt_));
  }
  int init_ddl_task_monitor_info(const uint64_t target_table_id);
  virtual bool task_can_retry() const { return true; }
protected:
  static const int64_t TASK_EXECUTE_TIME_THRESHOLD = 3 * 24 * 60 * 60 * 1000000L; // 3 days
  common::TCRWLock lock_;
  ObDDLTracing ddl_tracing_;
  bool is_inited_;
  bool need_retry_;
  bool is_running_;
  bool is_abort_;
  share::ObDDLType task_type_;
  TraceId trace_id_;
  int32_t sub_task_trace_id_;
  uint64_t tenant_id_;
  uint64_t dst_tenant_id_;
  uint64_t object_id_;
  uint64_t schema_version_;
  uint64_t dst_schema_version_;
  uint64_t target_object_id_;
  share::ObDDLTaskStatus task_status_;
  int64_t snapshot_version_;
  int64_t ret_code_;
  int64_t task_id_;
  int64_t parent_task_id_;
  ObDDLTaskKey parent_task_key_;
  int64_t task_version_;
  int64_t parallelism_;
  ObString ddl_stmt_str_;
  common::ObArenaAllocator allocator_;
  lib::Worker::CompatMode compat_mode_;
  TraceId sys_task_id_;
  int64_t err_code_occurence_cnt_; // occurence count for all error return codes not in white list.
  share::ObDDLLongopsStat *longops_stat_;
  uint64_t gmt_create_;
  ObDDLTaskStatInfo stat_info_;
  int64_t delay_schedule_time_;
  int64_t next_schedule_ts_;
  int64_t execution_id_; // guarded by lock_
  ObArray<common::ObAddr> sql_exec_addrs_;
  int64_t start_time_;
  uint64_t data_format_version_;
  int64_t consumer_group_id_;
  bool is_pre_split_;
};

enum ColChecksumStat
{
  CCS_INVALID = 0,
  CCS_NOT_MASTER,
  CCS_SUCCEED,
  CCS_FAILED,
};

struct PartitionColChecksumStat
{
  PartitionColChecksumStat()
    : tablet_id_(),
      col_checksum_stat_(CCS_INVALID),
      snapshot_(-1),
      execution_id_(-1),
      ret_code_(common::OB_SUCCESS),
      retry_cnt_(0),
      table_id_(common::OB_INVALID_ID)
  {}
  void reset() {
    tablet_id_.reset();
    col_checksum_stat_ = CCS_INVALID;
    snapshot_ = -1;
    execution_id_ = -1;
    ret_code_ = common::OB_SUCCESS;
    retry_cnt_ = 0;
    table_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const { return tablet_id_.is_valid() && execution_id_ >= 0 && common::OB_INVALID_ID != table_id_; }
  TO_STRING_KV(K_(tablet_id),
               K_(col_checksum_stat),
               K_(snapshot),
               K_(execution_id),
               K_(ret_code),
               K_(retry_cnt),
               K_(table_id));
  ObTabletID tablet_id_; // may be data table, local index or global index
  ColChecksumStat col_checksum_stat_;
  int64_t snapshot_;
  int64_t execution_id_;
  int ret_code_;
  int retry_cnt_;
  int64_t table_id_;
};

class ObDDLWaitColumnChecksumCtx final
{
public:
  ObDDLWaitColumnChecksumCtx();
  ~ObDDLWaitColumnChecksumCtx();
  int init(
      const int64_t task_id,
      const uint64_t tenant_id,
      const uint64_t source_table_id,
      const uint64_t target_table_id,
      const int64_t schema_version,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int64_t timeout_us);
  void reset();
  bool is_inited() const { return is_inited_; }
  int try_wait(bool &is_column_checksum_ready);
  int update_status(const common::ObTabletID &tablet_id, const int ret_code);
  TO_STRING_KV(K(is_inited_), K(source_table_id_), K(target_table_id_),
      K(schema_version_), K(snapshot_version_), K(execution_id_), K(timeout_us_),
      K(last_drive_ts_), K(stat_array_), K_(tenant_id));

private:
  int send_calc_rpc(int64_t &send_succ_count);
  int refresh_zombie_task();

private:
  bool is_inited_;
  uint64_t source_table_id_;
  uint64_t target_table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t timeout_us_;
  int64_t last_drive_ts_;
  common::ObArray<PartitionColChecksumStat> stat_array_;
  int64_t task_id_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_DDL_TASK_H_
