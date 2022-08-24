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

#ifndef OCEANBASE_ROOTSERVER_OB_TTL_UTIL_H_
#define OCEANBASE_ROOTSERVER_OB_TTL_UTIL_H_

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

enum ObTTLTaskType {
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
  OB_TTL_TASK_MOVING  = 5,
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
  uint64_t partition_id_;
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
    partition_id_(OB_INVALID_ID),
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
              K_(partition_id),
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

typedef struct ObTTLStatusKey {
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t partition_id_;
  uint64_t task_id_;
  uint64_t partition_cnt_;
  ObTTLStatusKey() 
    : tenant_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      partition_id_(OB_INVALID_ID),
      task_id_(0),
      partition_cnt_(OB_INVALID_ID)
    {}
  
  ObTTLStatusKey(uint64_t tenant_id,
                uint64_t table_id,
                uint64_t partition_id,
                uint64_t task_id) 
  : tenant_id_(tenant_id),
    table_id_(table_id),
    partition_id_(partition_id),
    task_id_(task_id),
    partition_cnt_(OB_INVALID_ID)
  {}

  TO_STRING_KV(K_(tenant_id),
              K_(table_id),
              K_(partition_id),
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

typedef struct ObTTLDutyDuration {
  ObTTLDayTime begin_, end_;

  ObTTLDutyDuration()
    : begin_(), end_() {}

  bool is_valid() {
    return (begin_.is_valid() && end_.is_valid());
  }
  TO_STRING_KV(K_(begin), K_(end));
} ObTTLDutyDuration;

class ObTTLTime {
public:
  static int64_t current_time();
                          
  static bool is_same_day(int64_t ttl_time1,
                          int64_t ttl_time2);
};

class ObTTLUtil {
public:
  static int parse(const char* str, ObTTLDutyDuration& duration);
  static bool current_in_duration(ObTTLDutyDuration& duration);

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

  static int update_ttl_task_all_fields(uint64_t tenant_id,
                                        const char* tname,
                                        common::ObMySQLProxy& proxy, 
                                        ObTTLStatusKey& key,
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

  static int remove_all_task_to_history_table(uint64_t tenant_id, uint64_t task_id, common::ObISQLClient& proxy);

  static bool check_can_do_work();
  static bool check_can_process_tenant_tasks(uint64_t tenant_id);

private:
  static bool extract_val(const char* ptr, uint64_t len, int& val);
  static bool valid_digit(const char* ptr, uint64_t len);
  static int parse_ttl_daytime(ObString& in, ObTTLDayTime& daytime);
  DISALLOW_COPY_AND_ASSIGN(ObTTLUtil);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_TTL_UTIL_H_ */