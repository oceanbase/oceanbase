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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "lib/thread/thread_mgr_interface.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase
{
namespace storage
{
  class ObValueRowIterator;
}
namespace share
{
typedef common::ObCurTraceId::TraceId TraceId;
const static int64_t VEC_ASYNC_TASK_DEFAULT_ERR_CODE = -1;
class ObPluginVectorIndexMgr;

enum ObVecIndexAsyncTaskTriggerType
{
  OB_VEC_TRIGGER_AUTO = 0,
  OB_VEC_TRIGGER_MANUAL = 1,
  OB_VEC_TRIGGER_INVALID
};

enum ObVecIndexAsyncTaskStatus
{
  OB_VECTOR_ASYNC_TASK_PREPARE = 0,
  OB_VECTOR_ASYNC_TASK_RUNNING = 1,
  OB_VECTOR_ASYNC_TASK_PENDING = 2, // reserved
  OB_VECTOR_ASYNC_TASK_FINISH = 3,
  OB_VECTOR_ASYNC_TASK_INVALID
};

enum ObVecIndexAsyncTaskType
{
  OB_VECTOR_ASYNC_INDEX_BUILT = 0,
  OB_VECTOR_ASYNC_INDEX_OPTINAL = 1,
  OB_VECTOR_ASYNC_TASK_TYPE_INVALID
};

struct ObVecIndexTaskStatus
{
  int64_t gmt_create_;
  int64_t gmt_modified_;

  uint64_t tenant_id_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
  int64_t task_id_;
  int64_t trigger_type_;
  int64_t task_type_;
  int64_t status_;
  SCN target_scn_;
  int64_t ret_code_;
  // ObString trace_id_str_;
  TraceId trace_id_;

  ObVecIndexTaskStatus() :  gmt_create_(0),
                            gmt_modified_(0),
                            tenant_id_(OB_INVALID_ID),
                            table_id_(OB_INVALID_ID),
                            tablet_id_(OB_INVALID_ID),
                            task_id_(-1),
                            trigger_type_(ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_INVALID),
                            task_type_(ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID),
                            status_(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_INVALID),
                            target_scn_(),
                            ret_code_(VEC_ASYNC_TASK_DEFAULT_ERR_CODE),
                            trace_id_() {}

  TO_STRING_KV(K_(gmt_create), K_(gmt_modified), K_(tenant_id), K_(table_id),
                K_(tablet_id), K_(task_type), K_(trigger_type), K_(task_id),
                K_(status), K_(target_scn), K_(trace_id), K_(ret_code));
};

struct ObVecIndexTaskKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t task_id_;
  explicit ObVecIndexTaskKey(uint64_t tenant_id, uint64_t table_id,
      uint64_t tablet_id, int64_t task_id) :
    tenant_id_(tenant_id),
    table_id_(table_id),
    tablet_id_(tablet_id),
    task_id_(task_id) {}
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(tablet_id), K_(task_id));
};

struct ObVecIndexTaskStatusField
{
  ObString field_name_;
  enum
  {
    INT_TYPE = 0,
    UINT_TYPE,
    STRING_TYPE,
  } type_;
  union data
  {
    int64_t int_;
    uint64_t uint_;
    ObString str_;
    data() : str_() {}
  } data_;
  ObVecIndexTaskStatusField()
      : field_name_(),
        type_(INT_TYPE),
        data_() {}
  TO_STRING_KV(K_(field_name), K_(type));
};

typedef common::ObArray<ObVecIndexTaskStatusField> ObVecIndexFieldArray;
typedef common::ObArray<ObVecIndexTaskStatus> ObVecIndexTaskStatusArray;

// vector index async task ctx
struct ObVecIndexAsyncTaskCtx
{
public:
  ObVecIndexAsyncTaskCtx()
      : tenant_id_(OB_INVALID_TENANT_ID),
        retry_time_(0),
        ls_(nullptr),
        task_status_(),
        sys_task_id_(),
        in_thread_pool_(false)
  {}
  ~ObVecIndexAsyncTaskCtx() {}

  TO_STRING_KV(K_(tenant_id), KP_(ls), K_(task_status), K_(sys_task_id), K_(in_thread_pool));

  uint64_t tenant_id_;
  uint64_t retry_time_;
  storage::ObLS *ls_;
  ObVecIndexTaskStatus task_status_;
  TraceId sys_task_id_;
  bool in_thread_pool_;
  common::ObSpinLock lock_; // lock for update task_status_
};

typedef common::hash::ObHashMap<common::ObTabletID, ObVecIndexAsyncTaskCtx *> VecIndexAsyncTaskMap;

class ObVecIndexAsyncTaskOption
{
public:
  ObVecIndexAsyncTaskOption(uint64_t tenant_id) : allocator_(ObMemAttr(tenant_id, "VecIdxATaskCtx"))
  {
  }

  ~ObVecIndexAsyncTaskOption();

  int init(const int64_t capacity, const int64_t tenant_id, ObLSID &ls_id);
  void destroy();
  int add_task_ctx(ObTabletID &tablet_id, ObVecIndexAsyncTaskCtx *task, bool &inc_new_task);
  int del_task_ctx(ObTabletID &tablet_id);
  VecIndexAsyncTaskMap &get_async_task_map() { return task_ctx_map_; }
  ObIAllocator *get_allocator() { return &allocator_; }

private:
  VecIndexAsyncTaskMap task_ctx_map_;
  ObArenaAllocator allocator_;
};

// QUEUE_THREAD
class ObVecIndexAsyncTaskHandler : public lib::TGTaskHandler
{
public:
  ObVecIndexAsyncTaskHandler();
  virtual ~ObVecIndexAsyncTaskHandler();
  int init(ObIAllocator *allocator);
  int start();
  void stop();
  void wait();
  void destroy();
  int push_task(const uint64_t tenant_id, const ObLSID &ls_id, ObVecIndexAsyncTaskCtx *ctx);
  int get_tg_id() { return tg_id_; }

  void inc_async_task_ref() { ATOMIC_INC(&async_task_ref_cnt_); }
  void dec_async_task_ref() { ATOMIC_DEC(&async_task_ref_cnt_); }
  int64_t get_async_task_ref() const { return ATOMIC_LOAD(&async_task_ref_cnt_); }

  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

public:
  static const int64_t MIN_THREAD_COUNT = 1;
  static const int64_t MAX_THREAD_COUNT = 12;
  common::ObSpinLock lock_; // lock for init

private:
  static const int64_t INVALID_TG_ID = -1;
  bool is_inited_;
  int tg_id_;
  volatile int64_t async_task_ref_cnt_;
  ObIAllocator *allocator_;
};

class ObPluginVectorIndexAdaptor;
class ObVecIndexAsyncTask
{
public:
  ObVecIndexAsyncTask() : is_inited_(false),
                          task_type_(ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_TASK_TYPE_INVALID),
                          tenant_id_(OB_INVALID_TENANT_ID),
                          ls_id_(ObLSID::INVALID_LS_ID),
                          ctx_(nullptr),
                          vec_idx_mgr_(nullptr),
                          allocator_(ObMemAttr(MTL_ID(), "VecIdxASyTask"))
  {
  }
  virtual ~ObVecIndexAsyncTask() {}
  int init(const uint64_t tenant_id, const ObLSID &ls_id, const int task_type, ObVecIndexAsyncTaskCtx *ctx);
  int get_task_type() { return task_type_; }
  ObVecIndexAsyncTaskCtx *get_task_ctx() { return ctx_; }
  int do_work();

  TO_STRING_KV(K_(is_inited), K_(task_type), K_(tenant_id), K_(ctx));

private:
  int build_inc_index(ObPluginVectorIndexAdaptor &adaptor);
  int optimize_vector_index( ObPluginVectorIndexAdaptor &adaptor);
  int refresh_snapshot_index_data(ObPluginVectorIndexAdaptor &adaptor);
  int get_old_snapshot_data(
      ObPluginVectorIndexAdaptor &adaptor,
      transaction::ObTxDesc *tx_desc,
      const int64_t snapshot_column_count,
      common::ObCollationType cs_type,
      int64_t vector_key_col_idx,
      int64_t vector_data_col_idx,
      int64_t vector_vid_col_idx,
      int64_t vector_col_idx,
      ObSEArray<int64_t, 4> &extra_column_idxs,
      storage::ObTableScanIterator *table_scan_iter,
      storage::ObValueRowIterator &delete_row_iter);
  int delete_incr_table_data(ObPluginVectorIndexAdaptor &adaptor, storage::ObDMLBaseParam &dml_param, transaction::ObTxDesc *tx_desc);

private:
  bool is_inited_;
  int task_type_; // 0. built; 1. opt
  uint64_t tenant_id_;
  ObLSID ls_id_;
  ObVecIndexAsyncTaskCtx *ctx_;
  ObPluginVectorIndexMgr *vec_idx_mgr_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexAsyncTask);
};

class ObVecIndexAsyncTaskUtil final
{
  static const int64_t DEFAULT_VEC_INSERT_BATCH_SIZE = 10;

public:
  static int read_vec_tasks(
      const uint64_t tenant_id,
      const char* tname,
      const bool for_update /*false*/,
      const ObVecIndexFieldArray& filters,
      storage::ObLS *ls, /* null means get all tenant task */
      common::ObISQLClient& proxy,
      ObVecIndexTaskStatusArray& result_arr,
      common::ObIAllocator *allocator /*NULL*/);
  static int delete_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskKey &key,
      int64_t &affect_rows);
  static int update_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskKey &key,
      ObVecIndexFieldArray &update_fields);
  static int insert_vec_tasks(
      uint64_t tenant_id,
      const char *tname,
      const int64_t batch_size,
      common::ObISQLClient &proxy,
      ObVecIndexTaskStatusArray &task);
  static int batch_insert_vec_task(
      uint64_t tenant_id,
      const char *tname,
      common::ObISQLClient &proxy,
      ObVecIndexTaskStatusArray &task);
  static int clear_history_expire_task_record(
      const uint64_t tenant_id,
      const int64_t batch_size,
      common::ObMySQLTransaction &proxy,
      int64_t &clear_rows);
  static int move_task_to_history_table(
      const uint64_t tenant_id,
      const int64_t batch_size,
      common::ObMySQLTransaction &proxy,
      int64_t &move_rows);
  static int resume_task_from_inner_table(
      const int64_t tenant_id,
      const char *tname,
      const bool for_update /*false*/,
      const ObVecIndexFieldArray &filters,
      storage::ObLS *ls,
      common::ObISQLClient &proxy,
      ObVecIndexAsyncTaskOption &async_task_opt);
  static int get_table_id_from_adapter(
      ObPluginVectorIndexAdaptor *adapter,
      const ObTabletID &tablet_id,
      int64_t &table_id);
  static int construct_task_key(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t tablet_id,
      const int64_t task_id,
      ObVecIndexFieldArray& task_key);

  static bool check_can_do_work();

  static int fetch_new_task_id(const uint64_t tenant_id, int64_t &new_task_id);
  static int add_sys_task(ObVecIndexAsyncTaskCtx *task);
  static int remove_sys_task(ObVecIndexAsyncTaskCtx *task);
  static int fetch_new_trace_id(const uint64_t basic_num, ObIAllocator *allocator, TraceId &new_trace_id);
  static int in_active_time(const uint64_t tenant_id, bool& is_active_time);

private:
  static int construct_read_task_sql(
      const uint64_t tenant_id,
      const char *tname,
      const bool for_update /* select for update*/,
      const bool is_read_tenant_async_task,
      const ObVecIndexFieldArray &filters,
      common::ObISQLClient &proxy,
      ObSqlString &sql);
  static int extract_one_task_sql_result(
      sqlclient::ObMySQLResult *result,
      ObVecIndexTaskStatus &task);
};

}
}

#endif // OCEANBASE_SHARE_VECTOR_INDEX_ASYNC_TASK_UTIL_H_
