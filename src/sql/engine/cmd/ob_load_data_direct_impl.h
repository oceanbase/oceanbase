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
#pragma once

#include "lib/allocator/page_arena.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "observer/table_load/ob_table_load_task.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_row_array.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "common/storage/ob_io_device.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_instance.h"
#include "storage/direct_load/ob_direct_load_struct.h"
#include "sql/engine/cmd/ob_load_data_storage_info.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;
class ObTableLoadTableCtx;
class ObTableLoadExecCtx;
class ObTableLoadCoordinator;
class ObITableLoadTaskScheduler;
class ObTableLoadInstance;
class ObTableLoadBackupTable;
} // namespace observer
namespace sql
{
/**
 * LOAD DATA接入direct load路径
 *
 * - 输入行必须包含表的所有列数据, 除了堆表的隐藏主键列
 * - 不支持SET子句
 * - 不支持表达式
 */
class ObLoadDataDirectImpl : public ObLoadDataBase
{
  static const int64_t MAX_DATA_MEM_USAGE_LIMIT = 64;

public:
  ObLoadDataDirectImpl();
  virtual ~ObLoadDataDirectImpl();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;

private:
  class Logger;

  struct DataAccessParam
  {
  public:
    DataAccessParam();
    bool is_valid() const;
    TO_STRING_KV(K_(file_location), K_(file_column_num), K_(file_cs_type));
  public:
    ObLoadFileLocation file_location_;
    ObLoadDataStorageInfo access_info_;
    int64_t file_column_num_; // number of column in file
    ObDataInFileStruct file_format_;
    common::ObCollationType file_cs_type_;
  };

  struct LoadExecuteParam
  {
  public:
    LoadExecuteParam();
    bool is_valid() const;
    TO_STRING_KV(K_(tenant_id),
                 K_(database_id),
                 K_(table_id),
                 K_(combined_name),
                 K_(parallel),
                 K_(thread_count),
                 K_(batch_row_count),
                 K_(data_mem_usage_limit),
                 K_(need_sort),
                 K_(online_opt_stat_gather),
                 K_(max_error_rows),
                 K_(ignore_row_num),
                 K_(dup_action),
                 "method", storage::ObDirectLoadMethod::get_type_string(method_),
                 "insert_mode", storage::ObDirectLoadInsertMode::get_type_string(insert_mode_),
                 K_(data_access_param),
                 K_(column_ids));
  public:
    uint64_t tenant_id_;
    uint64_t database_id_;
    uint64_t table_id_;
    common::ObString database_name_;
    common::ObString table_name_;
    common::ObString combined_name_; // database name + table name
    int64_t parallel_;
    int64_t thread_count_; // number of concurrent threads
    int64_t batch_row_count_;
    int64_t data_mem_usage_limit_; // limit = data_mem_usage_limit * MAX_BUFFER_SIZE
    bool need_sort_;
    bool online_opt_stat_gather_;
    int64_t max_error_rows_; // max allowed error rows
    int64_t ignore_row_num_; // number of rows to ignore per file
    sql::ObLoadDupActionType dup_action_;
    storage::ObDirectLoadMethod::Type method_;
    storage::ObDirectLoadInsertMode::Type insert_mode_;
    DataAccessParam data_access_param_;
    ObArray<uint64_t> column_ids_;
  };

  struct LoadExecuteContext
  {
  public:
    LoadExecuteContext();
    bool is_valid() const;
    TO_STRING_KV(K_(exec_ctx), KP_(allocator), KP_(direct_loader), KP_(job_stat), KP_(logger));
  public:
    observer::ObTableLoadExecCtx exec_ctx_;
    common::ObIAllocator *allocator_;
    observer::ObTableLoadInstance *direct_loader_;
    sql::ObLoadDataStat *job_stat_;
    Logger *logger_;
  };

private:
  class Logger
  {
    static const char *log_file_column_names;
    static const char *log_file_row_fmt;
  public:
    Logger();
    ~Logger();
    int init(const common::ObString &load_info, int64_t max_error_rows);
    int log_error_line(const common::ObString &file_name, int64_t line_no, int err_code);
    int64_t inc_error_count() { return ATOMIC_AAF(&err_cnt_, 1); }
  private:
    int create_log_file(const common::ObString &load_info);
    static int generate_log_file_name(char *buf, int64_t size, common::ObString &file_name);
  private:
    lib::ObMutex mutex_;
    ObFileAppender file_appender_;
    bool is_oracle_mode_;
    char *buf_;
    bool is_create_log_succ_;
    int64_t err_cnt_;
    int64_t max_error_rows_;
    bool is_inited_;
    DISALLOW_COPY_AND_ASSIGN(Logger);
  };

private:
  struct DataDesc
  {
  public:
    DataDesc() : file_idx_(0), start_(0), end_(-1) {}
    ~DataDesc() {}
    TO_STRING_KV(K_(file_idx), K_(filename), K_(start), K_(end));
  public:
    int64_t file_idx_;
    ObString filename_;
    int64_t start_;
    int64_t end_;
  };

  class DataDescIterator
  {
  public:
    DataDescIterator();
    ~DataDescIterator();
    int64_t count() const { return data_descs_.count(); }
    int copy(const ObLoadFileIterator &file_iter);
    int copy(const DataDescIterator &desc_iter);
    int add_data_desc(const DataDesc &data_desc);
    int get_next_data_desc(DataDesc &data_desc, int64_t &pos);
    TO_STRING_KV(K_(data_descs), K_(pos));
  private:
    common::ObArray<DataDesc> data_descs_;
    int64_t pos_;
  };

  struct DataBuffer
  {
  public:
    DataBuffer();
    ~DataBuffer();
    void reuse();
    void reset();
    int init(int64_t capacity = ObLoadFileBuffer::MAX_BUFFER_SIZE);
    bool is_valid() const;
    int64_t get_data_length() const;
    int64_t get_remain_length() const;
    bool empty() const;
    char *data() const;
    void advance(int64_t length);
    void update_data_length(int64_t length);
    int squash();
    void swap(DataBuffer &other);
    TO_STRING_KV(KPC_(file_buffer), K_(pos));
  public:
    ObLoadFileBuffer *file_buffer_;
    int64_t pos_; // left pos
    bool is_end_file_;
  private:
    DISALLOW_COPY_AND_ASSIGN(DataBuffer);
  };

  // Read the buffer and align it by row.
  class DataReader
  {
  public:
    DataReader();
    ~DataReader();
    int init(const DataAccessParam &data_access_param, LoadExecuteContext &execute_ctx,
             const DataDesc &data_desc, bool read_raw = false);
    int get_next_buffer(ObLoadFileBuffer &file_buffer, int64_t &line_count,
                        int64_t limit = INT64_MAX);
    int get_next_raw_buffer(DataBuffer &data_buffer);
    bool has_incomplate_data() const { return data_trimer_.has_incomplate_data(); }
    bool is_end_file() const;
    ObCSVGeneralParser &get_csv_parser() { return csv_parser_; }

  private:
    int read_buffer(ObLoadFileBuffer &file_buffer);

  private:
    ObArenaAllocator allocator_;
    LoadExecuteContext *execute_ctx_;
    ObCSVGeneralParser csv_parser_; // 用来计算完整行
    ObLoadFileDataTrimer data_trimer_; // 缓存不完整行的数据
    ObFileReader *file_reader_;
    int64_t end_offset_; // use -1 in stream file such as load data local
    bool read_raw_;
    bool is_iter_end_;
    bool is_inited_;
    DISALLOW_COPY_AND_ASSIGN(DataReader);
  };

  // Parse the data in the buffer into a string array by column.
  class DataParser
  {
  public:
    DataParser();
    ~DataParser();
    int init(const DataAccessParam &data_access_param, Logger &logger);
    int parse(const common::ObString &file_name, int64_t start_line_no, DataBuffer &data_buffer);
    int get_next_row(common::ObNewRow &row);
    int64_t get_parsed_row_count() { return pos_; }
  private:
    int log_error_line(int err_ret, int64_t err_line_no);
  private:
    ObCSVGeneralParser csv_parser_;
    DataBuffer escape_buffer_;
    DataBuffer *data_buffer_;
    // 以下参数是为了打错误日志
    common::ObString file_name_;
    int64_t start_line_no_;
    int64_t pos_;
    Logger *logger_;
    bool is_inited_;
    DISALLOW_COPY_AND_ASSIGN(DataParser);
  };

  class SimpleDataSplitUtils
  {
  public:
    static bool is_simple_format(const ObDataInFileStruct &file_format,
                                 common::ObCollationType file_cs_type);
    static int split(const DataAccessParam &data_access_param, const DataDesc &data_desc,
                     int64_t count, DataDescIterator &data_desc_iter);
  };

  struct TaskResult
  {
    TaskResult()
      : ret_(OB_SUCCESS),
        created_ts_(0),
        start_process_ts_(0),
        finished_ts_(0),
        proccessed_row_count_(0),
        parsed_row_count_(0),
        parsed_bytes_(0)
    {
    }
    void reset()
    {
      ret_ = OB_SUCCESS;
      created_ts_ = 0;
      start_process_ts_ = 0;
      finished_ts_ = 0;
      proccessed_row_count_ = 0;
      parsed_row_count_ = 0;
      parsed_bytes_ = 0;
    }
    int ret_;
    int64_t created_ts_;
    int64_t start_process_ts_;
    int64_t finished_ts_;
    int64_t proccessed_row_count_;
    int64_t parsed_row_count_;
    int64_t parsed_bytes_;
    TO_STRING_KV(K_(ret), K_(created_ts), K_(start_process_ts), K_(finished_ts),
                 K_(proccessed_row_count), K_(parsed_row_count), K_(parsed_bytes));
  };

  struct TaskHandle
  {
  public:
    TaskHandle()
      : task_id_(common::OB_INVALID_ID), worker_idx_(-1), session_id_(0), start_line_no_(0)
    {
    }
    table::ObTableLoadSequenceNo get_next_seq_no () { return sequence_no_ ++ ; }
  public:
    int64_t task_id_;
    DataBuffer data_buffer_;
    int64_t worker_idx_; // parse thread idx
    int32_t session_id_; // table load session id
    DataDesc data_desc_;
    int64_t start_line_no_; // 从1开始
    table::ObTableLoadSequenceNo sequence_no_;
    TaskResult result_;
    TO_STRING_KV(K_(task_id), K_(data_buffer), K_(worker_idx), K_(session_id), K_(data_desc),
                 K_(start_line_no), K_(result));
  private:
    DISALLOW_COPY_AND_ASSIGN(TaskHandle);
  };

  class FileLoadExecutor
  {
  public:
    FileLoadExecutor();
    virtual ~FileLoadExecutor();
    virtual int init(const LoadExecuteParam &execute_param, LoadExecuteContext &execute_ctx,
                     const DataDescIterator &data_desc_iter) = 0;
    int execute();
    int alloc_task(observer::ObTableLoadTask *&task);
    void free_task(observer::ObTableLoadTask *task);
    void task_finished(TaskHandle *handle);
    int process_task_handle(TaskHandle *handle, int64_t &line_count);
    int64_t get_total_line_count() const {return total_line_count_; }
  protected:
    virtual int prepare_execute() = 0;
    virtual int get_next_task_handle(TaskHandle *&handle) = 0;
    virtual int fill_task(TaskHandle *handle, observer::ObTableLoadTask *task) = 0;
  protected:
    int inner_init(const LoadExecuteParam &execute_param, LoadExecuteContext &execute_ctx,
                   int64_t worker_count, int64_t handle_count);
    int init_worker_ctx_array();
    int fetch_task_handle(TaskHandle *&handle);
    int handle_task_result(int64_t task_id, TaskResult &result);
    int handle_all_task_result();
    void wait_all_task_finished();
  protected:
    struct WorkerContext
    {
    public:
      DataParser data_parser_;
      table::ObTableLoadArray<ObObj> objs_;
    };
  protected:
    ObArenaAllocator allocator_;
    const LoadExecuteParam *execute_param_;
    LoadExecuteContext *execute_ctx_;
    observer::ObTableLoadObjectAllocator<observer::ObTableLoadTask> task_allocator_;
    observer::ObITableLoadTaskScheduler *task_scheduler_;
    int64_t worker_count_; // <= thread_count_
    WorkerContext *worker_ctx_array_;
    // task ctrl
    ObParallelTaskController task_controller_;
    ObConcurrentFixedCircularArray<TaskHandle *> handle_reserve_queue_;
    common::ObArray<TaskHandle *> handle_resource_; // 用于释放资源
    int64_t total_line_count_;
    // trans
    observer::ObTableLoadInstance::TransCtx trans_ctx_;
    bool is_inited_;
  private:
    DISALLOW_COPY_AND_ASSIGN(FileLoadExecutor);
  };

  class FileLoadTaskCallback;

private:
  /**
   * Large File
   */

  class LargeFileLoadExecutor : public FileLoadExecutor
  {
  public:
    LargeFileLoadExecutor();
    virtual ~LargeFileLoadExecutor();
    int init(const LoadExecuteParam &execute_param, LoadExecuteContext &execute_ctx,
             const DataDescIterator &data_desc_iter) override;
  protected:
    int prepare_execute() override;
    int get_next_task_handle(TaskHandle *&handle) override;
    int fill_task(TaskHandle *handle, observer::ObTableLoadTask *task) override;
  private:
    int64_t get_worker_idx();
    int skip_ignore_rows();
  private:
    DataDesc data_desc_;
    DataBuffer expr_buffer_;
    DataReader data_reader_;
    int64_t next_worker_idx_;
    int64_t next_chunk_id_;
    int64_t total_line_no_;
    DISALLOW_COPY_AND_ASSIGN(LargeFileLoadExecutor);
  };

  class LargeFileLoadTaskProcessor;

private:
  /**
   * Multi Files
   */

  class MultiFilesLoadExecutor : public FileLoadExecutor
  {
  public:
    MultiFilesLoadExecutor() = default;
    virtual ~MultiFilesLoadExecutor() = default;
    int init(const LoadExecuteParam &execute_param, LoadExecuteContext &execute_ctx,
             const DataDescIterator &data_desc_iter) override;
  protected:
    int prepare_execute() override;
    int get_next_task_handle(TaskHandle *&handle) override;
    int fill_task(TaskHandle *handle, observer::ObTableLoadTask *task) override;
  private:
    DataDescIterator data_desc_iter_;
    DISALLOW_COPY_AND_ASSIGN(MultiFilesLoadExecutor);
  };

  class MultiFilesLoadTaskProcessor;

private:
  /**
   * BackupLoadExecutor
   */
  class BackupLoadExecutor
  {
    const int64_t MIN_TASK_PER_WORKER = 4;
  public:
    BackupLoadExecutor();
    ~BackupLoadExecutor();
    int init(const LoadExecuteParam &execute_param, LoadExecuteContext &execute_ctx,
             const ObString &path);
    int execute();
    int get_next_partition_task(int64_t &partition_idx, int64_t &subpart_count,
                                int64_t &subpart_idx);
    int process_partition(int32_t session_id, int64_t partition_idx, int64_t subpart_count = 1,
                          int64_t subpart_idx = 0);
    void task_finished(observer::ObTableLoadTask *task, int ret_code);
    int64_t get_total_line_count() const { return total_line_count_; }
    int check_status();
  private:
    int check_support_direct_load();
  private:
    ObArenaAllocator allocator_;
    const LoadExecuteParam *execute_param_;
    LoadExecuteContext *execute_ctx_;
    observer::ObTableLoadBackupTable *backup_table_;
    observer::ObTableLoadObjectAllocator<observer::ObTableLoadTask> task_allocator_;
    observer::ObITableLoadTaskScheduler *task_scheduler_;
    ObParallelTaskController task_controller_;
    int64_t worker_count_;
    int64_t partition_count_;
    int64_t subpart_count_;
    lib::ObMutex mutex_;
    int64_t next_partition_idx_;
    int64_t next_subpart_idx_;
    int64_t total_line_count_;
    int task_error_code_;
    bool is_inited_;
  };

  class BackupLoadTaskProcessor;
  class BackupLoadTaskCallback;

private:
  int init_file_iter();
  // init execute param
  int init_execute_param();
  // init execute context
  int init_logger();
  int init_execute_context();

private:
  ObExecContext *ctx_;
  ObLoadDataStmt *load_stmt_;
  ObPhysicalPlan plan_;
  LoadExecuteParam execute_param_;
  LoadExecuteContext execute_ctx_;
  observer::ObTableLoadInstance direct_loader_;
  Logger logger_;
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataDirectImpl);
};

} // namespace sql
} // namespace oceanbase
