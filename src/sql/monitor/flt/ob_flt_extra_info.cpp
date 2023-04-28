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

#define USING_LOG_PREFIX LIB_UTIL
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "sql/monitor/flt/ob_flt_extra_info.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

// usage:
// ...
// if (OB_FAIL(FLTExtraInfo::resolve_type_and_len(buf, len, pos, extra_type, v_len))) {
//  // error
// } else if (extra_type == FLT_CONTROL_INFO) {
//    FLTControlInfo extra_info;
//    extra_info.deserialize(buf, v_len, pos);
// }
int FLTExtraInfo::resolve_type_and_len(const char *buf, const int64_t len,
                                        int64_t &pos, FullLinkTraceExtraInfoType &extra_type,
                                        int32_t &v_len)
{
  int ret = OB_SUCCESS;
  int16_t extra_type_val;
  if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, len, pos, extra_type_val, v_len))) {
    OB_LOG(WARN,"failed to get extra info type", KP(buf), K(len), K(extra_type_val), K(v_len));
  } else {
    extra_type = FullLinkTraceExtraInfoType(extra_type_val);
  }
  return ret;
}
// extra_info.deserisalize(buf, v_len, pos);
int FLTExtraInfo::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t buf_end = pos + len;
  while (OB_SUCC(ret) && pos < len) {
    int32_t val_len = 0;
    int16_t extra_id;
    OB_LOG(TRACE, "deserialize flt extra info", KPHEX(buf+pos, len-pos));
    if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, buf_end, pos, extra_id, val_len))) {
      OB_LOG(WARN,"failed to get extra_info", K(ret), KP(buf));
    } else if (OB_FAIL(deserialize_field(static_cast<FullLinkTraceExtraInfoId>(extra_id),
                                                            val_len, buf, buf_end, pos))) {
      OB_LOG(WARN,"failed to resolve value", K(ret), KP(buf), K(buf_end), K(pos), K(val_len));
    } else {
      // do nothing
    }
  }
  return ret;
}
// usage:
//  FLTControlInfo* control_info;
//  ....
//  control_info->serialize(buf, len, pos);
//
// flt_id, len, value ...
int FLTControlInfo::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // resrver for type and len
  int64_t org_pos = pos;
  if (pos + FLT_HEADER_LEN > len) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN,"buffer size overflow", K(ret), K(pos), K(len));
  } else {
    MEMSET(buf+pos, 0x00, len-pos);
    pos += FLT_HEADER_LEN;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos, level_, FLT_LEVEL))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_LEVEL), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_double(buf, len, pos,
                                      sample_pct_, FLT_SAMPLE_PERCENTAGE))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_SAMPLE_PERCENTAGE), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos, rp_, FLT_RECORD_POLICY))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_RECORD_POLICY), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_double(buf, len, pos,
                                      print_sample_pct_, FLT_PRINT_SAMPLE_PCT))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_SAMPLE_PERCENTAGE), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_int8(buf, len, pos,
                                      slow_query_thres_, FLT_SLOW_QUERY_THRES))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_RECORD_POLICY), K(buf));
  } else if (support_show_trace_ && OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos,
                                      show_trace_enable_, FLT_SHOW_TRACE_ENABLE))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_SHOW_TRACE_ENABLE), K(buf));
  } else {
    // fill type and len in the head
    int32_t total_len = pos - org_pos - FLT_HEADER_LEN;
    if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, len, org_pos, type_, total_len))) {
      OB_LOG(WARN,"failed to store extra info type", K(type_), K(buf));
    } else {
      // do nothing
    }
  }
  return ret;
}
int FLTControlInfo::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // this message is sent by server, and server will not receive this
  switch(extra_id) {
    case FLT_LEVEL: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        level_ = v;
      }
      break;
    }
    case FLT_SAMPLE_PERCENTAGE: {
      double v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_double(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        sample_pct_ = v;
      }
      break;
    }
    case FLT_RECORD_POLICY: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        rp_ = static_cast<RecordPolicy>(v);
      }
      break;
    }
    case FLT_PRINT_SAMPLE_PCT: {
      double v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_double(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        print_sample_pct_ = v;
      }
      break;
    }
    case FLT_SLOW_QUERY_THRES: {
      int64_t v =0;
      if (OB_FAIL(ObProtoTransUtil::get_int8(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        slow_query_thres_ = v;
      }
      break;
    }
    case FLT_SHOW_TRACE_ENABLE: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        show_trace_enable_ = static_cast<bool>(v);
      }
      break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}
int FLTControlInfo::get_serialize_size()
{
  return FLT_HEADER_LEN + FLT_HEADER_LEN + sizeof(level_) +
         FLT_HEADER_LEN + sizeof(sample_pct_) +
         FLT_HEADER_LEN + sizeof(int8_t) +
         FLT_HEADER_LEN + sizeof(print_sample_pct_) +
         FLT_HEADER_LEN + sizeof(slow_query_thres_) +
         (support_show_trace_ ? (FLT_HEADER_LEN + sizeof(show_trace_enable_)) : 0);
}
int FLTSpanInfo::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // resrver for type and len
  int64_t org_pos = pos;
  if (pos + FLT_HEADER_LEN > len) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN,"buffer size overflow", K(ret), K(pos), K(len));
  } else {
    MEMSET(buf+pos, 0x00, len-pos);
    pos += FLT_HEADER_LEN;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos,
                                      trace_enable_, FLT_TRACE_ENABLE))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_TRACE_ENABLE), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos,
                                      force_print_, FLT_FORCE_PRINT))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_FORCE_PRINT), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_str(buf, len, pos,
                        trace_id_.ptr(), trace_id_.length(), FLT_TRACE_ID))) {
    OB_LOG(WARN, "failed to store extra info id", K(FLT_TRACE_ID), K(trace_id_), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, len, pos,
                                      ref_type_, FLT_REF_TYPE))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_REF_TYPE), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_str(buf, len, pos,
                        span_id_.ptr(), span_id_.length(), FLT_SPAN_ID))) {
    OB_LOG(WARN, "failed to store extra info id", K(FLT_SPAN_ID), K(span_id_), K(buf));
  } else {
    // fill type and len in the head
    int32_t total_len = pos - org_pos - FLT_HEADER_LEN;
    if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, len, org_pos, type_, total_len))) {
      OB_LOG(WARN,"failed to store extra info type", K(type_), K(buf));
    } else {
      // do nothing
    }
  }
  return ret;
}
int FLTSpanInfo::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  switch(extra_id) {
    case FLT_TRACE_ENABLE: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        trace_enable_ = static_cast<bool>(v);
      }
      break;
    }
    case FLT_FORCE_PRINT: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        force_print_ = static_cast<bool>(v);
      }
      break;
    }
    case FLT_TRACE_ID: {
      char* ptr = NULL;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        trace_id_.assign(ptr, v_len);
      }
      break;
    }
    case FLT_REF_TYPE: {
      int8_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int1(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        ref_type_ = static_cast<RefType>(v);
      }
      break;
    }
    case FLT_SPAN_ID: {
      char* ptr = NULL;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        span_id_.assign(ptr, v_len);
      }
      break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}
int FLTSpanInfo::get_serialize_size()
{
  return FLT_HEADER_LEN + FLT_HEADER_LEN + sizeof(trace_enable_) + FLT_HEADER_LEN + sizeof(force_print_) +
          FLT_HEADER_LEN + trace_id_.length() + FLT_HEADER_LEN + sizeof(int8_t) + FLT_HEADER_LEN + span_id_.length();
}
int FLTDrvSpan::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // this message is written by driver, server will do nothing
  return ret;
}
int FLTDrvSpan::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  switch(extra_id) {
    case FLT_DRV_LOG: {
      char* ptr;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        span_info_.assign(ptr, len);
      }
      break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}
int FLTDrvSpan::get_serialize_size()
{
  // this message is written by driver, server will do nothing
  return 0;
}
int FLTAppInfo::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // this message is written by driver, server will do nothing
  return ret;
}
int FLTAppInfo::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  switch(extra_id) {
    case FLT_CLIENT_IDENTIFIER: {
      char* ptr;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        trace_client_identifier_.assign(ptr, v_len);
      }
      break;
    }
    case FLT_MODULE: {
      char* ptr;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        trace_module_.assign(ptr, v_len);
      }
      break;
    }
    case FLT_ACTION: {
      char* ptr;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        trace_action_.assign(ptr, v_len);
      }
      break;
    }
    case FLT_CLIENT_INFO: {
      char* ptr;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        trace_client_info_.assign(ptr, v_len);
      }
      break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}
int FLTAppInfo::get_serialize_size()
{
  // this message is written by driver, server will do nothing
  return 0;
}

int FLTQueryInfo::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // resrver for type and len
  int64_t org_pos = pos;
  if (pos + FLT_HEADER_LEN > len) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN,"buffer size overflow", K(ret), K(pos), K(len));
  } else {
    MEMSET(buf+pos, 0x00, len-pos);
    pos += FLT_HEADER_LEN;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObProtoTransUtil::store_int8(buf, len, pos,
                                      query_start_time_, FLT_QUERY_START_TIMESTAMP))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_QUERY_START_TIMESTAMP), K(buf));
  } else if (OB_FAIL(ObProtoTransUtil::store_int8(buf, len, pos,
                                      query_end_time_, FLT_QUERY_END_TIMESTAMP))) {
    OB_LOG(WARN,"failed to store extra info id", K(FLT_QUERY_END_TIMESTAMP), K(buf));
  } else {
    // fill type and len in the head
    int32_t total_len = pos - org_pos - FLT_HEADER_LEN;
    if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, len, org_pos, type_, total_len))) {
      OB_LOG(WARN,"failed to store extra info type", K(type_), K(buf));
    } else {
      // do nothing
    }
  }
  return ret;
}

int FLTQueryInfo::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // this message is written by server, server will do nothing

  switch(extra_id) {
    case FLT_QUERY_START_TIMESTAMP: {
     int64_t v = 0;
     if (OB_FAIL(ObProtoTransUtil::get_int8(buf, len, pos, v_len, v))) {
       OB_LOG(WARN,"failed to resolve flt level", K(ret));
     } else {
       query_start_time_ = v;
     }
     break;
    }
    case FLT_QUERY_END_TIMESTAMP: {
     int64_t v = 0;
     if (OB_FAIL(ObProtoTransUtil::get_int8(buf, len, pos, v_len, v))) {
       OB_LOG(WARN,"failed to resolve flt level", K(ret));
     } else {
       query_end_time_ = v;
     }
     break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}

int FLTQueryInfo::get_serialize_size()
{
  return FLT_HEADER_LEN + FLT_HEADER_LEN + sizeof(int64_t) + FLT_HEADER_LEN + sizeof(int64_t);
}

int FLTShowTrace::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // this message is written by proxy, server will do nothing
  return ret;
}

int FLTShowTrace::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  switch(extra_id) {
    case FLT_PROXY_SHOW_TRACE_SPAN: {
      char* ptr = NULL;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        show_trace_span_.assign(ptr, v_len);
      }
      break;
    }
    case FLT_DRV_SHOW_TRACE_SPAN: {
      char* ptr = NULL;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve flt level", K(ret));
      } else {
        // do nothing
        show_trace_drv_span_.assign(ptr, v_len);
      }
      break;
    }
    default: {
      // skip
      break;
    }
  }
  return ret;
}

int FLTShowTrace::get_serialize_size()
{
  // this message is written by proxy, server will do nothing
  return 0;
}
