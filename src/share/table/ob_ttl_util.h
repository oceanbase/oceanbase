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

#ifndef OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_UTIL_
#define OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_UTIL_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "share/table/redis/ob_redis_common.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "observer/table/utils/ob_htable_utils.h"

namespace oceanbase
{
namespace sql
{
class ObSchemaChecker;
}
namespace common
{

#define OB_TTL_RESPONSE_MASK (1 << 5)
#define OB_TTL_STATUS_MASK  (OB_TTL_RESPONSE_MASK - 1)

#define SET_TASK_PURE_STATUS(status, state) ((status) = ((state) & OB_TTL_STATUS_MASK) + ((status & OB_TTL_RESPONSE_MASK)))
#define SET_TASK_RESPONSE(status, state) ((status) |= (((state) & 1) << 5))
#define SET_TASK_STATUS(status, pure_status, is_responsed) { SET_TASK_PURE_STATUS(status, pure_status), SET_TASK_RESPONSE(status, is_responsed); }

#define EVAL_TASK_RESPONSE(status) (((status) & OB_TTL_RESPONSE_MASK) >> 5)
#define EVAL_TASK_PURE_STATUS(status) (static_cast<ObTTLTaskStatus>((status) & OB_TTL_STATUS_MASK))

class ObTTLTaskConstant
{
public:
  static const ObString TTL_SCAN_INDEX_DEFAULT_VALUE;
};

enum TRIGGER_TYPE
{
  PERIODIC_TRIGGER = 0,
  USER_TRIGGER = 1,
};

const char *ob_trigger_type_to_string(const TRIGGER_TYPE trigger_type);

enum ObTTLTaskType
{
  OB_TTL_TRIGGER, // todo:weiyouchao.wyc merge with rpc arg define
  OB_TTL_SUSPEND,
  OB_TTL_RESUME,
  OB_TTL_CANCEL,
  OB_TTL_MOVE,
  OB_TTL_INVALID,
  OB_LOB_CHECK_TRIGGER = 10,
  OB_LOB_CHECK_SUSPEND,
  OB_LOB_CHECK_RESUME,
  OB_LOB_CHECK_CANCEL,
  OB_LOB_CHECK_INVALID,
  OB_LOB_REPAIR_TRIGGER = 20, // FARM COMPAT WHITELIST
  OB_LOB_REPAIR_SUSPEND,      // FARM COMPAT WHITELIST
  OB_LOB_REPAIR_RESUME,       // FARM COMPAT WHITELIST
  OB_LOB_REPAIR_CANCEL,       // FARM COMPAT WHITELIST
  OB_LOB_REPAIR_INVALID,      // FARM COMPAT WHITELIST
};

const char *ob_ttl_task_type_to_string(const ObTTLTaskType ttl_task_type);

enum ObTTLTaskStatus
{
  // for obsever
  OB_TTL_TASK_PREPARE = 0,  //inner state
  OB_TTL_TASK_RUNNING = 1,
  OB_TTL_TASK_PENDING = 2,
  OB_TTL_TASK_CANCEL  = 3,
  OB_TTL_TASK_FINISH  = 4,  //inner state
  OB_TTL_TASK_MOVING  = 5, // deprecated
  OB_TTL_TASK_SKIP    = 6,
  OB_TTL_TASK_FAILED  = 7,
  // for rs
  OB_RS_TTL_TASK_CREATE = 15,
  OB_RS_TTL_TASK_SUSPEND = 16,
  OB_RS_TTL_TASK_CANCEL = 17,
  OB_RS_TTL_TASK_MOVE = 18,

  OB_TTL_TASK_INVALID
};

const char *ob_ttl_task_status_to_string(const ObTTLTaskStatus ttl_task_status);

enum ObTTLType // FARM COMPAT WHITELIST
{
  NORMAL = 0,
  HBASE_ROWKEY = 1,
  LOB_CHECK = 2,
  LOB_REPAIR = 3, // FARM COMPAT WHITELIST
  COMPACTION_TTL = 4,
};

const char *ob_ttl_type_to_string(const ObTTLType ttl_type);

typedef struct ObTTLStatus {
  int64_t gmt_create_;
  int64_t gmt_modified_;

  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  int64_t task_id_;

  int64_t task_start_time_;
  int64_t task_update_time_;
  int64_t trigger_type_;
  int64_t status_;

  uint64_t ttl_del_cnt_;
  uint64_t max_version_del_cnt_;
  uint64_t scan_cnt_;
  ObString row_key_;
  ObString ret_code_;
  ObTTLType task_type_;
  ObString scan_index_;
  uint64_t ls_id_;
  static const ObString DEFAULT_RET_CODE;
  ObTTLStatus()
  : gmt_create_(0),
    gmt_modified_(0),
    tenant_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    tablet_id_(OB_INVALID_ID),
    task_id_(0),
    task_start_time_(0),
    task_update_time_(0),
    trigger_type_(static_cast<int>(PERIODIC_TRIGGER)),
    status_(OB_TTL_TASK_INVALID),
    ttl_del_cnt_(0),
    max_version_del_cnt_(0),
    scan_cnt_(0),
    row_key_(),
    ret_code_(DEFAULT_RET_CODE),
    task_type_(ObTTLType::NORMAL),
    scan_index_(ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE),
    ls_id_(OB_INVALID_ID) {}

 TO_STRING_KV(K_(gmt_create),
              K_(gmt_modified),
              K_(tenant_id),
              K_(table_id),
              K_(tablet_id),
              K_(task_id),
              K_(task_start_time),
              K_(task_update_time),
              "trigger_type", ob_trigger_type_to_string(static_cast<TRIGGER_TYPE>(trigger_type_)),
              "status", ob_ttl_task_status_to_string(static_cast<ObTTLTaskStatus>(status_)),
              K_(ttl_del_cnt),
              K_(max_version_del_cnt),
              K_(scan_cnt),
              K_(row_key),
              K_(ret_code),
              "task_type", ob_ttl_type_to_string(task_type_),
              K_(scan_index),
              K_(ls_id));
} ObTTLStatus;

typedef common::ObArray<ObTTLStatus> ObTTLStatusArray;

typedef struct ObTTLStatusKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  uint64_t task_id_;
  int64_t task_type_;
  explicit ObTTLStatusKey(const uint64_t tenant_id,
                          const uint64_t table_id,
                          const uint64_t tablet_id,
                          const uint64_t task_id,
                          const int64_t task_type)
  : tenant_id_(tenant_id),
    table_id_(table_id),
    tablet_id_(tablet_id),
    task_id_(task_id),
    task_type_(task_type)
  {}

  TO_STRING_KV(K_(tenant_id),
              K_(table_id),
              K_(tablet_id),
              K_(task_id),
              K_(task_type));
} ObTTLStatusKey;

typedef struct ObTTLStatusField {
  ObString field_name_;
  enum {
    INT_TYPE = 0,
    UINT_TYPE,
    STRING_TYPE,
  } type_;

  union data {
    int64_t int_;
    uint64_t uint_;
    ObString str_;
    data () : str_() {}
  } data_;
  ObTTLStatusField()
    : field_name_(),
      type_(INT_TYPE),
      data_() {}
  TO_STRING_KV(K_(field_name), K_(type));
} ObTTLStatusField;


typedef common::ObArray<ObTTLStatusField> ObTTLStatusFieldArray;

typedef struct ObTTLDayTime {
  int32_t hour_;
  int32_t min_;
  int32_t sec_;
  ObTTLDayTime()
    : hour_(0), min_(0), sec_(0) {}
  bool is_valid() {
    return ((hour_ >= 0 && hour_ <= 24) &&
           (min_ >= 0 && min_ <= 60) &&
           (sec_ >= 0 && sec_ <= 60));
  }
  TO_STRING_KV(K_(hour), K_(min), K_(sec));
} ObTTLDayTime;

struct ObTTLDutyDuration
{
  ObTTLDutyDuration()
    : begin_(), end_(), not_set_(true) {}

  bool is_valid() {
    return ((begin_.is_valid() && end_.is_valid()) || not_set_);
  }
  TO_STRING_KV(K_(begin), K_(end));

  ObTTLDayTime begin_, end_;
  bool not_set_;
};

class ObTTLTime {
public:
  static int64_t current_time();

  static bool is_same_day(int64_t ttl_time1,
                          int64_t ttl_time2);
};

struct ObSimpleTTLInfo
{
public:
  uint64_t tenant_id_;
  TRIGGER_TYPE trigger_type_;
  ObString table_with_tablet_;
  ObSimpleTTLInfo()
    : tenant_id_(OB_INVALID_TENANT_ID),
      trigger_type_(TRIGGER_TYPE::USER_TRIGGER),
      table_with_tablet_()
  {}

  ObSimpleTTLInfo(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      trigger_type_(TRIGGER_TYPE::USER_TRIGGER),
      table_with_tablet_()
  {}

  bool is_valid() const { return (OB_INVALID_TENANT_ID != tenant_id_); }
  TO_STRING_KV(K_(tenant_id), K_(trigger_type), K_(table_with_tablet));
  OB_UNIS_VERSION(1);
};

struct ObTTLParam
{
public:
  ObTTLParam()
    : ttl_info_array_(), ttl_all_(false), transport_(nullptr), trigger_type_(TRIGGER_TYPE::USER_TRIGGER), table_with_tablet_()
  {}

  void reset()
  {
    ttl_info_array_.reset();
    ttl_all_ = false;
    transport_ = nullptr;
    trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;
    table_with_tablet_.reset();
  }

  bool is_valid() const
  {
    return (nullptr != transport_);
  }

  int add_ttl_info(const uint64_t tenant_id);

  TO_STRING_KV(K_(ttl_info_array), K_(ttl_all), KP_(transport), K_(trigger_type), K_(table_with_tablet));

  common::ObArray<ObSimpleTTLInfo> ttl_info_array_;
  bool ttl_all_;
  rpc::frame::ObReqTransport *transport_;
  obrpc::ObTTLRequestArg::TTLRequestType type_;
  TRIGGER_TYPE trigger_type_; // 表示定时器触发还是手动触发
  ObString table_with_tablet_;
};

class ObKVAttr
{
public:
  enum ObTTLTableType {
    HBASE,
    REDIS,
    INVALID,
    TABLE
  };
  explicit ObKVAttr()
    : type_(ObTTLTableType::INVALID),
      ttl_(0),
      max_version_(0),
      is_disable_(false),
      is_redis_ttl_(false),
      redis_model_(table::ObRedisDataModel::MODEL_MAX),
      created_by_admin_(false),
      ttl_scan_index_(ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE)
  {
    MEMSET(ttl_scan_index_buf_, 0, sizeof(ttl_scan_index_buf_));
  }
  bool is_ttl_table() const;
  OB_INLINE bool is_max_versions_valid() const
  {
    return type_ == ObTTLTableType::HBASE && max_version_ > 0;
  }
  OB_INLINE bool is_hbase_ttl_valid() const
  {
    return type_ == ObTTLTableType::HBASE && ttl_ > 0;
  }
  OB_INLINE bool is_empty() const { return type_ == ObTTLTableType::INVALID; }
  OB_INLINE bool is_created_by_admin() const { return type_ == ObTTLTableType::HBASE && created_by_admin_; }
  OB_INLINE bool is_kv_hbase_table() const { return type_ == ObTTLTableType::HBASE; }
  int deep_copy_ttl_scan_index(const ObString &ttl_scan_index);
  TO_STRING_KV(K_(type), K_(ttl), K_(max_version), K_(is_disable), K_(is_redis_ttl), K_(redis_model), K_(created_by_admin),
              K_(ttl_scan_index));

public:
  ObTTLTableType type_;

  // for hbase
  int32_t  ttl_;
  int32_t  max_version_;
  bool     is_disable_;
  // for redis
  bool is_redis_ttl_;
  table::ObRedisDataModel redis_model_;
  bool created_by_admin_;
  // for ttl scan index
  char ttl_scan_index_buf_[OB_MAX_OBJECT_NAME_LENGTH];
  ObString ttl_scan_index_;
};

class ObTTLUtil
{
private:
  static const char* HBASE_KV_ATTR_FORMAT_STR;
public:
  static int parse(const char* str, ObTTLDutyDuration& duration);
  static bool current_in_duration(ObTTLDutyDuration& duration);

  static int transform_tenant_state(const common::ObTTLTaskStatus& tenant_status, common::ObTTLTaskStatus& status);
  static int check_tenant_state(uint64_t tenant_id,
                                uint64_t table_id,
                                common::ObISQLClient& proxy,
                                const ObTTLTaskStatus local_state,
                                const int64_t local_task_id,
                                common::ObTTLType ttl_type,
                                bool &tenant_state_changed);
  static int insert_ttl_task(uint64_t tenant_id,
                             const char* tname,
                             common::ObISQLClient& proxy,
                             ObTTLStatus& task);

  static int replace_ttl_task(uint64_t tenant_id,
                             const char* tname,
                             common::ObISQLClient& proxy,
                             ObTTLStatus& task);

  static int update_ttl_task(uint64_t tenant_id,
                             const char* tname,
                             common::ObISQLClient& proxy,
                             ObTTLStatusKey& key,
                             ObTTLStatusFieldArray& update_fields);

  static int update_ttl_task_all_fields(uint64_t tenant_id,
                                        const char* tname,
                                        common::ObISQLClient& proxy,
                                        const ObTTLStatus& update_task);

  static int delete_ttl_task(uint64_t tenant_id,
                             const char* tname,
                             common::ObISQLClient& proxy,
                             ObTTLStatusKey& key,
                             int64_t &affect_rows);

  static int read_ttl_tasks(uint64_t tenant_id,
                            const char* tname,
                            common::ObISQLClient& proxy,
                            ObTTLStatusFieldArray& filters,
                            ObTTLStatusArray& result_arr,
                            bool for_update = false,
                            common::ObIAllocator *allocator = NULL);

  static int read_tenant_ttl_task(const uint64_t tenant_id,
                                  const uint64_t table_id,
                                  const ObTTLType& task_type,
                                  common::ObISQLClient& proxy,
                                  ObTTLStatus &ttl_record,
                                  const bool for_update = false,
                                  common::ObIAllocator *allocator = NULL);

  static int move_task_to_history_table(uint64_t tenant_id, uint64_t task_id,
                                        const ObTTLType& task_type,
                                        common::ObMySQLTransaction& proxy,
                                        int64_t batch_size, int64_t &move_rows);
  static int move_tenant_task_to_history_table(const ObTTLStatusKey &key,
                                               common::ObMySQLTransaction& proxy);

  static bool check_can_do_work();
  static bool check_can_process_tenant_tasks(uint64_t tenant_id);
  static bool is_support_scan_index_version(uint64_t data_version);

  static int parse_kv_attributes(const ObString &kv_attributes, ObKVAttr &kv_attr);
  static int parse_kv_attributes(uint64_t tenant_id, const ObString &kv_attributes, ObKVAttr &kv_attr);
  static int format_kv_attributes_to_json_str(ObIAllocator &allocator, const ObKVAttr &kv_attr, ObString &json_str);
  static int dispatch_ttl_cmd(const ObTTLParam &param);
  static int get_ttl_info(const ObTTLParam &param, ObIArray<ObSimpleTTLInfo> &ttl_info_array);

  static int check_is_normal_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table, const bool check_recyclebin = true);
  static int check_is_deleting_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table, const bool check_recyclebin = true);
  static int check_is_rowkey_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table);
  static int get_tenant_table_ids(const uint64_t tenant_id, common::ObIArray<uint64_t> &table_id_array);
  static int check_task_status_from_sys_table(uint64_t tenant_id, common::ObISQLClient& proxy,
                                              const uint64_t& task_id, const uint64_t& table_id,
                                              ObTabletID& tablet_id, bool &is_exists, bool &is_end_state);
  static inline bool is_ttl_task_status_end_state(ObTTLTaskStatus status) {
    return status == ObTTLTaskStatus::OB_TTL_TASK_CANCEL || status == ObTTLTaskStatus::OB_TTL_TASK_FINISH;
  }
  static bool is_enable_ttl(uint64_t tenant_id);
  static bool is_enable_hbase_rowkey_ttl(uint64_t tenant_id);
  static const char *get_ttl_tenant_status_cstr(const ObTTLTaskStatus &status);
  static int get_hbase_ttl_columns(const share::schema::ObTableSchema &table_schema, ObIArray<ObString> &ttl_columns);
  static int get_ttl_columns(const ObString &ttl_definition, ObIArray<ObString> &ttl_columns);
  static bool is_ttl_column(const ObString &orig_column_name, const ObIArray<ObString> &ttl_columns);
  static bool is_index_name_match(uint64_t tenant_id, ObNameCaseMode name_case_mode, const ObString &scan_index, const ObString &index_name);
  static bool is_column_name_match(const ObString &col_name, const ObString &other_col_name);
  static int check_kv_attributes(ObKVAttr &attr,
                                 const share::schema::ObTableSchema &table_schema,
                                 const share::schema::ObTableSchema *index_schema,
                                 ObPartitionLevel part_level,
                                 bool by_admin = false);
  static int is_ttl_suport_scan_index(ObIndexType index_type);
  static bool is_default_scan_index(const ObString &scan_index);
  static int check_kv_attributes(const share::schema::ObTableSchema &table_schema,
                                 const common::ObIArray<obrpc::ObCreateIndexArg> &index_arg_list,
                                 bool is_htable);
  static int check_kv_attributes(ObKVAttr &kv_attr,
                                 const share::schema::ObTableSchema &table_schema,
                                 const sql::ObSchemaChecker *schema_checker,
                                 obrpc::ObDDLArg *ddl_arg);
  static int check_ttl_scan_index_name(const ObString &scan_index, const ObString &index_name, bool &is_valid);
  static int check_htable_ddl_supported(const share::schema::ObTableSchema &table_schema,
                                        bool by_admin,
                                        obrpc::ObHTableDDLType ddl_type = obrpc::ObHTableDDLType::INVALID,
                                        const ObString &table_name = ObString());
  static int check_htable_ddl_supported(share::schema::ObSchemaGetterGuard &schema_guard,
                                        const uint64_t tenant_id,
                                        const common::ObIArray<share::schema::ObDependencyInfo> &dep_infos);
  static bool is_ttl_cmd(int32_t cmd_code) { return cmd_code >= 0 && cmd_code < obrpc::ObTTLRequestArg::TTL_INVALID_TYPE; }
  static int check_ttl_scan_index_valid(const share::schema::ObTableSchema &table_schema,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        ObKVAttr::ObTTLTableType ttl_type,
                                        const ObString &scan_index);
  static int check_index_exists(share::schema::ObSchemaGetterGuard &schema_guard,
                                const uint64_t tenant_id,
                                const uint64_t table_id,
                                const ObString &index_name,
                                const share::schema::ObTableSchema *&index_schema);
  static int check_index_columns(const share::schema::ObTableSchema &index_schema,
                                 const ObIArray<ObString> &ttl_columns,
                                 bool is_hbase = false);
  static int check_index_columns(const obrpc::ObCreateIndexArg &index_arg,
                                 const ObIArray<ObString> &ttl_columns,
                                 bool is_hbase = false);
  const static uint64_t TTL_TENNAT_TASK_TABLET_ID = -1;
  const static uint64_t TTL_TENNAT_TASK_TABLE_ID = -1;
  const static uint64_t TTL_ROWKEY_TASK_TABLET_ID = -2;
  const static uint64_t TTL_ROWKEY_TASK_TABLE_ID = -2;
  const static uint64_t TTL_THREAD_MAX_SCORE = 100;
  const static uint64_t COMPACTION_TTL_TABLET_ID = 0;
private:
  static int check_is_htable_ttl_(const ObTableSchema &table_schema, bool allow_timeseries_table, bool &is_ttl_table);
  static int check_htable_ddl_supported_(const ObKVAttr &attr, bool by_admin);
  static int check_kv_attributes_common_(ObKVAttr &attr,
                                         share::schema::ObPartitionLevel part_level,
                                         bool by_admin,
                                         table::ObHbaseModeType mode_type);
  static int check_ttl_scan_index_(ObKVAttr &attr,
                                   const share::schema::ObTableSchema &table_schema,
                                   table::ObHbaseModeType mode_type,
                                   const share::schema::ObTableSchema &index_schema);
  static int check_ttl_scan_index_(ObKVAttr &attr,
                                   const share::schema::ObTableSchema &table_schema,
                                   table::ObHbaseModeType mode_type,
                                   const obrpc::ObCreateIndexArg &index_arg);
private:
  static bool extract_val(const char* ptr, uint64_t len, int& val);
  static bool valid_digit(const char* ptr, uint64_t len);
  static int parse_ttl_daytime(ObString& in, ObTTLDayTime& daytime);
  static int dispatch_one_tenant_ttl(obrpc::ObTTLRequestArg::TTLRequestType type,
                                     const rpc::frame::ObReqTransport &transport,
                                     const ObSimpleTTLInfo &ttl_info);
  static int get_all_user_tenant_ttl(common::ObIArray<ObSimpleTTLInfo> &ttl_info_array);
  static int parse_kv_attributes_table(json::Value *ast);
  static int parse_kv_attributes_hbase(json::Value *ast, ObKVAttr &kv_attr);
  static int parse_kv_attributes_redis(json::Value *ast, ObKVAttr &kv_attr);
  static int parse_kv_attributes_ttl_scan_index(json::Value *ast, ObKVAttr &kv_attr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLUtil);
};

enum class ObTableTTLTimeUnit
{
  INVALID,
  SECOND,
  MINUTE,
  HOUR,
  DAY,
  MONTH,
  YEAR
};

class ObTableTTLExpr
{
public:
  ObTableTTLExpr(): column_name_(), interval_(), time_unit_(ObTableTTLTimeUnit::INVALID), nsecond_(0), nmonth_(0), is_negative_(false) {}
  ~ObTableTTLExpr() {}
  const ObString &get_ttl_column() const { return column_name_; }
  TO_STRING_KV(K_(column_name), K_(interval), K_(time_unit));
public:
  ObString column_name_;
  int64_t interval_;
  ObTableTTLTimeUnit time_unit_;
  int64_t nsecond_;
  int64_t nmonth_;
  bool is_negative_;
};

class ObSimpleTableTTLChecker
{
public:
  ObSimpleTableTTLChecker()
  : ttl_definition_(),
    row_cell_ids_(),
    ttl_type_(share::ObTTLDefinition::INVALID)
  {
    ttl_definition_.set_attr(ObMemAttr(MTL_ID(), "TTLCheckerDef"));
    row_cell_ids_.set_attr(ObMemAttr(MTL_ID(), "TTLCheckerCells"));
  }
  virtual ~ObSimpleTableTTLChecker() {}
  // init ttl checker with table schema, if in_full_column_order is true, the checked row
  // should be in full column schema order, or you shoud set ttl cell ids explicitly.
  virtual int init(const share::schema::ObTableSchema &table_schema, const ObString &ttl_definition, const bool in_full_column_order = true)
  {
    bool unused_has_datetime_col = false;
    return inner_init(table_schema, ttl_definition, in_full_column_order, unused_has_datetime_col);
  }
  int init(const ObString &ttl_definition)
  {
    return parse_ttl_definition(ttl_definition);
  }
  int get_ttl_filter_us(int64_t &ttl_filter_us);
  const common::ObIArray<ObTableTTLExpr> &get_ttl_definition() const { return ttl_definition_; }
  virtual void reset();
protected:
  int parse_ttl_definition(const ObString &ttl_definition);
  int inner_init(
    const share::schema::ObTableSchema &table_schema,
    const ObString &ttl_definition,
    const bool in_full_column_order,
    bool &has_datetime_col);
  int get_ttl_expr_ts(const ObTableTTLExpr &ttl_expr, const int64_t input_ts, int64_t &expire_ts);
protected:
  common::ObSEArray<ObTableTTLExpr, 8> ttl_definition_;
  common::ObSEArray<int64_t, 8> row_cell_ids_; // cell idx scaned row for each ttl expr
  share::ObTTLDefinition::ObTTLType ttl_type_;
};

class ObTableTTLChecker final : public ObSimpleTableTTLChecker
{
public:
  ObTableTTLChecker()
  : ObSimpleTableTTLChecker(),
    tz_info_wrap_()
  {}
  virtual ~ObTableTTLChecker() {}
  virtual int init(const share::schema::ObTableSchema &table_schema, const ObString &ttl_definition, const bool in_full_column_order = true) override;
  int check_row_expired(const common::ObNewRow &row, bool &is_expired);
  common::ObIArray<int64_t> &get_row_cell_ids() { return row_cell_ids_; }
  virtual void reset() override;
private:
  ObTimeZoneInfoWrap tz_info_wrap_;
};

class ObTTLDeleteRateThrottler
{
public:
  struct ThrottleInfo {
    ThrottleInfo()
      : ttl_del_max_ops_(0),
        ttl_thread_num_(0),
        max_rows_per_second_(0),
        expected_interval_us_(0),
        table_id_(0),
        tablet_id_(),
        start_time_(0),
        cur_time_(0),
        total_del_row_count_(0)
    {}
    void reset() {
      ttl_del_max_ops_ = 0;
      ttl_thread_num_ = 0;
      max_rows_per_second_ = 0;
      expected_interval_us_ = 0;
      table_id_ = 0;
      tablet_id_.reset();
      start_time_ = 0;
      cur_time_ = 0;
      total_del_row_count_ = 0;
    }
    TO_STRING_KV(K_(ttl_del_max_ops), K_(ttl_thread_num), K_(max_rows_per_second), K_(expected_interval_us),
        K_(table_id), K_(tablet_id), K_(start_time), K_(cur_time), K_(total_del_row_count));
    int64_t ttl_del_max_ops_;
    int64_t ttl_thread_num_;
    int64_t max_rows_per_second_;
    int64_t expected_interval_us_;
    uint64_t table_id_;
    common::ObTabletID tablet_id_;
    int64_t start_time_;
    int64_t cur_time_;
    int64_t total_del_row_count_;
  };
public:
  ObTTLDeleteRateThrottler();
  ~ObTTLDeleteRateThrottler();
  int init(const table::ObTTLTaskInfo &info);
  int check_and_try_throttle(int64_t trans_timeout_ts);
  int check_and_try_throttle();
  void print_throttle_info();
  void reset();
  OB_INLINE void record_trans_start_time() { trans_start_time_ = ObTimeUtility::current_time(); }
  TO_STRING_KV(K_(is_inited),
               K_(max_rows_per_second),
               K_(smooth_factor),
               K_(trans_start_time),
               K_(prev_delete_time),
               K_(cur_del_row_count),
               K_(expected_interval_us));
private:
  int64_t calculate_wait_time();
  bool need_commit_trans_early(int64_t trans_timeout_ts);
private:
  static const double DEFAULT_SMOOTH_FACTOR;
  static const double DEFAULT_TRANS_TIMEOUT_RATIO;
private:
  bool is_inited_;
  int64_t max_rows_per_second_;
  double smooth_factor_;
  int64_t trans_start_time_;
  int64_t prev_delete_time_;
  int64_t cur_del_row_count_;
  int64_t expected_interval_us_;
  ThrottleInfo throttle_info_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_UTIL_ */
