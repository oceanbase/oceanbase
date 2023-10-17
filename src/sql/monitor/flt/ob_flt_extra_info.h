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

#ifdef FLT_EXTRA_INFO_DEF
// here to add driver's id
//FLT_TYPE_DRV_LOG
//logs from driver
FLT_EXTRA_INFO_DEF(FLT_DRV_LOG, 1, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_DRIVER_END, 1000, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
// here to add proxy's id
FLT_EXTRA_INFO_DEF(FLT_PROXY_END, 2000, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
//APP_INFO
FLT_EXTRA_INFO_DEF(FLT_CLIENT_IDENTIFIER, 2001, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_MODULE, 2002, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_ACTION, 2003, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_CLIENT_INFO, 2004, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)

// QUERY_INFO
FLT_EXTRA_INFO_DEF(FLT_QUERY_START_TIMESTAMP, 2010, EMySQLFieldType::MYSQL_TYPE_LONGLONG)
FLT_EXTRA_INFO_DEF(FLT_QUERY_END_TIMESTAMP, 2011, EMySQLFieldType::MYSQL_TYPE_LONGLONG)

// CONTROL_INFO
FLT_EXTRA_INFO_DEF(FLT_LEVEL, 2020, EMySQLFieldType::MYSQL_TYPE_TINY)
FLT_EXTRA_INFO_DEF(FLT_SAMPLE_PERCENTAGE, 2021, EMySQLFieldType::MYSQL_TYPE_DOUBLE)
FLT_EXTRA_INFO_DEF(FLT_RECORD_POLICY, 2022, EMySQLFieldType::MYSQL_TYPE_TINY)
FLT_EXTRA_INFO_DEF(FLT_PRINT_SAMPLE_PCT, 2023, EMySQLFieldType::MYSQL_TYPE_DOUBLE)
FLT_EXTRA_INFO_DEF(FLT_SLOW_QUERY_THRES, 2024, EMySQLFieldType::MYSQL_TYPE_LONGLONG)
FLT_EXTRA_INFO_DEF(FLT_SHOW_TRACE_ENABLE, 2025, EMySQLFieldType::MYSQL_TYPE_TINY)

// SPAN_INFO
FLT_EXTRA_INFO_DEF(FLT_TRACE_ENABLE, 2030, EMySQLFieldType::MYSQL_TYPE_TINY)
FLT_EXTRA_INFO_DEF(FLT_FORCE_PRINT, 2031, EMySQLFieldType::MYSQL_TYPE_TINY)
FLT_EXTRA_INFO_DEF(FLT_TRACE_ID, 2032, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_REF_TYPE, 2033, EMySQLFieldType::MYSQL_TYPE_TINY)
FLT_EXTRA_INFO_DEF(FLT_SPAN_ID, 2034, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)

// SHOW_TRACE_SPAN
FLT_EXTRA_INFO_DEF(FLT_DRV_SHOW_TRACE_SPAN,2050,EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
FLT_EXTRA_INFO_DEF(FLT_PROXY_SHOW_TRACE_SPAN,2051,EMySQLFieldType::MYSQL_TYPE_VAR_STRING)

FLT_EXTRA_INFO_DEF(FLT_EXTRA_INFO_END, 65535, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
#endif /* FLT_EXTRA_INFO_DEF */

#ifndef __OB_FLT_EXTRA_INFO_H__
#define __OB_FLT_EXTRA_INFO_H__
#include "rpc/obmysql/ob_mysql_global.h"
#include "lib/utility/ob_proto_trans_util.h"
#include "common/object/ob_object.h"
namespace oceanbase {
namespace sql {

using namespace obmysql;

enum FullLinkTraceExtraInfoId
{
  #define FLT_EXTRA_INFO_DEF(extra_id, id, type) extra_id = id,
  #include "sql/monitor/flt/ob_flt_extra_info.h"
  #undef FLT_EXTRA_INFO_DEF
};

enum FullLinkTraceExtraInfoType
{
  // for driver private
  FLT_TYPE_DRV_LOG = 1,
  FLT_TYPE_DRV_END = 1000,

  // for proxy private
  FLT_TYPE_PROXY_END = 2000,

  // for public
  FLT_TYPE_APP_INFO = 2001,
  FLT_TYPE_QUERY_INFO = 2002,
  FLT_TYPE_CONTROL_INFO = 2003,
  FLT_TYPE_SPAN_INFO = 2004,
  FLT_TYPE_SHOW_TRACE_SPAN = 2005,
  FLT_EXTRA_TYPE_END = 65535
};


class FullLinkTraceExtraInfoSet
{
public:
  FullLinkTraceExtraInfoSet()
  {
  #define FLT_EXTRA_INFO_DEF(extra_id, id, type) set_flt_extra_info(extra_id, type);
  #include "sql/monitor/flt/ob_flt_extra_info.h"
  #undef FLT_EXTRA_INFO_DEF
  };

  EMySQLFieldType get_type(FullLinkTraceExtraInfoId id);
private:
  void set_flt_extra_info(FullLinkTraceExtraInfoId id, EMySQLFieldType type)
  {
    types[id] = type;
  }
private:
  EMySQLFieldType types[FLT_EXTRA_INFO_END];
  static FullLinkTraceExtraInfoSet FLT_EXTRA_INFO_SET;

  static EMySQLFieldType get_flt_extra_info_type(FullLinkTraceExtraInfoId id)
  {
    return FLT_EXTRA_INFO_SET.get_type(id);
  }
};



class FLTExtraInfo
{
public:
  FullLinkTraceExtraInfoType type_;
  FLTExtraInfo() : type_(FLT_EXTRA_TYPE_END) {}
  ~FLTExtraInfo() {}
  virtual int serialize(char *buf, const int64_t len, int64_t &pos) = 0;
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                                const int64_t v_len, const char *buf,
                                const int64_t len, int64_t &pos) = 0;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  virtual int get_serialize_size() = 0;
  static int resolve_type_and_len(const char *buf, const int64_t len,
                            int64_t &pos, FullLinkTraceExtraInfoType &extra_type,
                            int32_t &v_len);
  bool is_app_info() { return type_ == FLT_TYPE_APP_INFO; }
  bool is_query_info() { return type_ == FLT_TYPE_QUERY_INFO; }
  bool is_control_info() { return type_ == FLT_TYPE_CONTROL_INFO; }
  bool is_span_info() { return type_ == FLT_TYPE_SPAN_INFO; }
  static const int16_t FLT_TYPE_LEN = 2;
  static const int16_t FLT_LENGTH_LEN = 4;
  static const int16_t FLT_HEADER_LEN = FLT_TYPE_LEN + FLT_LENGTH_LEN;
};



class FLTControlInfo : public FLTExtraInfo
{
  public:
  enum RecordPolicy {
    RP_ALL = 1,
    RP_ONLY_SLOW_QUERY = 2,
    RP_SAMPLE_AND_SLOW_QUERY = 3,
    MAX_RECORD_POLICY = 4
  };
  // control info
  int8_t level_;
  double sample_pct_;
  RecordPolicy rp_;
  double print_sample_pct_;
  int64_t slow_query_thres_;
  bool show_trace_enable_;
  bool support_show_trace_;

  FLTControlInfo() : level_(-1),
                     sample_pct_(-1),
                     rp_(MAX_RECORD_POLICY),
                     print_sample_pct_(-1),
                     slow_query_thres_(-1),
                     show_trace_enable_(false),
                     support_show_trace_(false) {
     type_ = FLT_TYPE_CONTROL_INFO;
  }
  ~FLTControlInfo() {}

  bool is_valid() {
    return level_ > 0 && sample_pct_>0 && sample_pct_<=1 && rp_>0 && rp_<MAX_RECORD_POLICY;
  }
  bool is_valid() const {
    return level_ > 0 && sample_pct_>0 && sample_pct_<=1 && rp_>0 && rp_<MAX_RECORD_POLICY;
  }
  bool is_valid_sys_config() {
    return print_sample_pct_ > 0 && print_sample_pct_ <= 1 && slow_query_thres_ > 0;
  }
  bool is_equal(const FLTControlInfo &other) {
    return level_ == other.level_ &&
           sample_pct_ == other.sample_pct_ &&
           rp_ == other.rp_ &&
           print_sample_pct_ == other.print_sample_pct_ &&
           slow_query_thres_ == other.slow_query_thres_ &&
           show_trace_enable_ == other.show_trace_enable_;
  }
  void reset() {
    level_ = -1;
    sample_pct_ = -1;
    rp_ = MAX_RECORD_POLICY;
    print_sample_pct_ = -1;
    slow_query_thres_ = -1;
    show_trace_enable_ = false;
  }

  inline bool operator==(const FLTControlInfo &other) const {
    return level_ == other.level_ && 
           sample_pct_ == other.sample_pct_ &&
           rp_ == other.rp_;
  }
  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(level), K_(sample_pct), K_(rp), K_(print_sample_pct), K_(slow_query_thres), K_(show_trace_enable), K_(support_show_trace));
};

class FLTSpanInfo : public FLTExtraInfo
{
  public:
  enum RefType {
    SYNC,
    ASYNC,
    MAX_REF_TYPE
  };
  bool trace_enable_;
  bool force_print_;
  ObString trace_id_;
  // 0-sync, 1-async
  RefType ref_type_;
  ObString span_id_;

  FLTSpanInfo() : trace_enable_(false),
                  force_print_(false),
                  trace_id_(),
                  ref_type_(MAX_REF_TYPE),
                  span_id_() {
     type_ = FLT_TYPE_SPAN_INFO;
  }
  ~FLTSpanInfo() {}

  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(trace_enable), K_(force_print), K_(trace_id), K_(ref_type), K_(span_id));
};

class FLTDrvSpan : public FLTExtraInfo
{
  public:
  ObString span_info_;

  FLTDrvSpan() : span_info_() {
     type_ = FLT_TYPE_DRV_LOG;
  }
  ~FLTDrvSpan() {}

  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(span_info));
};

class FLTAppInfo : public FLTExtraInfo
{
  public:
  ObString trace_client_identifier_;
  ObString trace_module_;
  ObString trace_action_;
  ObString trace_client_info_;

  FLTAppInfo() : trace_client_identifier_(),
                 trace_module_(),
                 trace_action_(),
                 trace_client_info_() {
     type_ = FLT_TYPE_APP_INFO;
  }
  ~FLTAppInfo() {}

  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(trace_client_identifier), K_(trace_module),
               K_(trace_action), K_(trace_client_info));
};

class FLTQueryInfo : public FLTExtraInfo
{
  public:
  int64_t query_start_time_;
  int64_t query_end_time_;

  FLTQueryInfo() : query_start_time_(0),
                 query_end_time_(0) {
     type_ = FLT_TYPE_QUERY_INFO;
  }
  ~FLTQueryInfo() {}

  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(query_start_time), K_(query_end_time));
};

class FLTShowTrace : public FLTExtraInfo
{
  public:
  ObString show_trace_span_;
  ObString show_trace_drv_span_;

  FLTShowTrace() : show_trace_span_(),
                   show_trace_drv_span_() {
    type_ = FLT_TYPE_SHOW_TRACE_SPAN;
  }
  ~FLTShowTrace() {}

  // flt_id, len, value ...
  int serialize(char *buf, const int64_t len, int64_t &pos);
  int deserialize_field(FullLinkTraceExtraInfoId extra_id,
                        const int64_t v_len, const char *buf,
                        const int64_t len, int64_t &pos);
  int get_serialize_size();
  TO_STRING_KV(K_(show_trace_span), K_(show_trace_drv_span));
};
}; // end of namespace lib
}; // end of namespace oceanbase

#endif /*__OB_FLT_EXTRA_INFO_H__*/
