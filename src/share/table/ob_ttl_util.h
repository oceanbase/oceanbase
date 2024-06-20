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

namespace oceanbase
{
namespace common
{

#define OB_TTL_RESPONSE_MASK (1 << 5)
#define OB_TTL_STATUS_MASK  (OB_TTL_RESPONSE_MASK - 1)

#define SET_TASK_PURE_STATUS(status, state) ((status) = ((state) & OB_TTL_STATUS_MASK) + ((status & OB_TTL_RESPONSE_MASK)))
#define SET_TASK_RESPONSE(status, state) ((status) |= (((state) & 1) << 5))
#define SET_TASK_STATUS(status, pure_status, is_responsed) { SET_TASK_PURE_STATUS(status, pure_status), SET_TASK_RESPONSE(status, is_responsed); }

#define EVAL_TASK_RESPONSE(status) (((status) & OB_TTL_RESPONSE_MASK) >> 5)
#define EVAL_TASK_PURE_STATUS(status) (static_cast<ObTTLTaskStatus>((status) & OB_TTL_STATUS_MASK))


enum TRIGGER_TYPE
{
  PERIODIC_TRIGGER = 0,
  USER_TRIGGER = 1,
};

enum ObTTLTaskType
{
  OB_TTL_TRIGGER, // todo:weiyouchao.wyc merge with rpc arg define
  OB_TTL_SUSPEND,
  OB_TTL_RESUME,
  OB_TTL_CANCEL,
  OB_TTL_MOVE,
  OB_TTL_INVALID
};

enum ObTTLTaskStatus
{
  // for obsever
  OB_TTL_TASK_PREPARE = 0,  //inner state
  OB_TTL_TASK_RUNNING = 1,
  OB_TTL_TASK_PENDING = 2,
  OB_TTL_TASK_CANCEL  = 3,
  OB_TTL_TASK_FINISH  = 4,  //inner state
  OB_TTL_TASK_MOVING  = 5, // deprecated
  // for rs
  OB_RS_TTL_TASK_CREATE = 15,
  OB_RS_TTL_TASK_SUSPEND = 16,
  OB_RS_TTL_TASK_CANCEL = 17,
  OB_RS_TTL_TASK_MOVE = 18,

  OB_TTL_TASK_INVALID
};

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
    ret_code_("OB_SUCCESS") {}

 TO_STRING_KV(K_(gmt_create),
              K_(gmt_modified),
              K_(tenant_id),
              K_(table_id),
              K_(tablet_id),
              K_(task_id),
              K_(task_start_time),
              K_(task_update_time),
              K_(trigger_type),
              K_(status),
              K_(ttl_del_cnt),
              K_(max_version_del_cnt),
              K_(scan_cnt),
              K_(row_key),
              K_(ret_code));
} ObTTLStatus;

typedef common::ObArray<ObTTLStatus> ObTTLStatusArray;

typedef struct ObTTLStatusKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t tablet_id_;
  uint64_t task_id_;
  uint64_t partition_cnt_;
  ObTTLStatusKey()
    : tenant_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      tablet_id_(OB_INVALID_ID),
      task_id_(0),
      partition_cnt_(OB_INVALID_ID)
    {}

  ObTTLStatusKey(uint64_t tenant_id,
                uint64_t table_id,
                uint64_t tablet_id,
                uint64_t task_id)
  : tenant_id_(tenant_id),
    table_id_(table_id),
    tablet_id_(tablet_id),
    task_id_(task_id),
    partition_cnt_(OB_INVALID_ID)
  {}

  TO_STRING_KV(K_(tenant_id),
              K_(table_id),
              K_(tablet_id),
              K_(task_id));
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

  ObSimpleTTLInfo()
    : tenant_id_(OB_INVALID_TENANT_ID)
  {}

  ObSimpleTTLInfo(const uint64_t tenant_id)
    : tenant_id_(tenant_id)
  {}

  bool is_valid() const { return (OB_INVALID_TENANT_ID != tenant_id_); }
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION(1);
};

struct ObTTLParam
{
public:
  ObTTLParam()
    : ttl_info_array_(), ttl_all_(false), transport_(nullptr)
  {}

  void reset()
  {
    ttl_info_array_.reset();
    ttl_all_ = false;
    transport_ = nullptr;
  }

  bool is_valid() const
  {
    return (nullptr != transport_);
  }

  int add_ttl_info(const uint64_t tenant_id);

  TO_STRING_KV(K_(ttl_info_array), K_(ttl_all), KP_(transport));

  common::ObArray<ObSimpleTTLInfo> ttl_info_array_;
  bool ttl_all_;
  rpc::frame::ObReqTransport *transport_;
  obrpc::ObTTLRequestArg::TTLRequestType type_;
};

class ObTTLUtil
{
public:
  static int parse(const char* str, ObTTLDutyDuration& duration);
  static bool current_in_duration(ObTTLDutyDuration& duration);

  static int transform_tenant_state(const common::ObTTLTaskStatus& tenant_status, common::ObTTLTaskStatus& status);
  static int check_tenant_state(uint64_t tenant_id,
                                common::ObISQLClient& proxy,
                                const ObTTLTaskStatus local_state,
                                const int64_t local_task_id,
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
                                        ObTTLStatus& update_task);

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

  static int read_tenant_ttl_task(uint64_t tenant_id,
                                  common::ObISQLClient& proxy,
                                  ObTTLStatus &ttl_record,
                                  const bool for_update = false,
                                  common::ObIAllocator *allocator = NULL);

  static int move_task_to_history_table(uint64_t tenant_id, uint64_t task_id,
                                        common::ObMySQLTransaction& proxy,
                                        int64_t batch_size, int64_t &move_rows);

  static int move_tenant_task_to_history_table(uint64_t tenant_id, uint64_t task_id,
                                               common::ObMySQLTransaction& proxy);


  static bool check_can_do_work();
  static bool check_can_process_tenant_tasks(uint64_t tenant_id);

  static int parse_kv_attributes(const ObString &kv_attributes, int32_t &max_versions, int32_t &time_to_live);

  static int dispatch_ttl_cmd(const ObTTLParam &param);
  static int get_ttl_info(const ObTTLParam &param, ObIArray<ObSimpleTTLInfo> &ttl_info_array);

  static int check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table);
  static int get_tenant_table_ids(const uint64_t tenant_id, common::ObIArray<uint64_t> &table_id_array);
  static int check_task_status_from_sys_table(uint64_t tenant_id, common::ObISQLClient& proxy,
                                              const uint64_t& task_id, const uint64_t& table_id,
                                              ObTabletID& tablet_id, bool &is_exists, bool &is_end_state);
  static inline bool is_ttl_task_status_end_state(ObTTLTaskStatus status) {
    return status == ObTTLTaskStatus::OB_TTL_TASK_CANCEL || status == ObTTLTaskStatus::OB_TTL_TASK_FINISH;
  }
  static bool is_enable_ttl(uint64_t tenant_id);
  static const char *get_ttl_tenant_status_cstr(const ObTTLTaskStatus &status);

  static int get_ttl_columns(const ObString &ttl_definition, ObIArray<ObString> &ttl_columns);
  static bool is_ttl_column(const ObString &orig_column_name, const ObIArray<ObString> &ttl_columns);

  const static uint64_t TTL_TENNAT_TASK_TABLET_ID = -1;
  const static uint64_t TTL_TENNAT_TASK_TABLE_ID = -1;
private:
  static bool extract_val(const char* ptr, uint64_t len, int& val);
  static bool valid_digit(const char* ptr, uint64_t len);
  static int parse_ttl_daytime(ObString& in, ObTTLDayTime& daytime);
  static int dispatch_one_tenant_ttl(obrpc::ObTTLRequestArg::TTLRequestType type,
                                     const rpc::frame::ObReqTransport &transport,
                                     const ObSimpleTTLInfo &ttl_info);
  static int get_all_user_tenant_ttl(common::ObIArray<ObSimpleTTLInfo> &ttl_info_array);
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

class ObTableTTLChecker
{
public:
  ObTableTTLChecker()
  : ttl_definition_(),
    row_cell_ids_(),
    tenant_id_(common::OB_INVALID_TENANT_ID)
  {
    ttl_definition_.set_attr(ObMemAttr(MTL_ID(), "TTLCheckerDef"));
    row_cell_ids_.set_attr(ObMemAttr(MTL_ID(), "TTLCheckerCells"));
  }
  ~ObTableTTLChecker() {}
  // init ttl checker with table schema, if in_full_column_order is true, the checked row
  // should be in full column schema order, or you shoud set ttl cell ids explicitly.
  int init(const share::schema::ObTableSchema &table_schema, bool in_full_column_order = true);
  int check_row_expired(const common::ObNewRow &row, bool &is_expired);
  const common::ObIArray<ObTableTTLExpr> &get_ttl_definition() const { return ttl_definition_; }
  common::ObIArray<int64_t> &get_row_cell_ids() { return row_cell_ids_; }
private:
  common::ObSEArray<ObTableTTLExpr, 8> ttl_definition_;
  common::ObSEArray<int64_t, 8> row_cell_ids_; // cell idx scaned row for each ttl expr
  int64_t tenant_id_;
  ObTimeZoneInfoWrap tz_info_wrap_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_UTIL_ */