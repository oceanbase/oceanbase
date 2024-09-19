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

#ifdef OBP20_EXTRA_INFO_DEF
// here to add driver's id
OBP20_EXTRA_INFO_DEF(OBP20_DRIVER_END, 1, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
OBP20_EXTRA_INFO_DEF(OBP20_DRIVER_MAX_TYPE, 1000, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)

// here to add proxy's id
OBP20_EXTRA_INFO_DEF(FEEDBACK_PROXY_INFO, 1001, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
OBP20_EXTRA_INFO_DEF(OBP20_PROXY_END, 1002, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
OBP20_EXTRA_INFO_DEF(OBP20_PROXY_MAX_TYPE, 2000, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)

// here to add server's id
OBP20_EXTRA_INFO_DEF(TRACE_INFO, 2001, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
OBP20_EXTRA_INFO_DEF(SESS_INFO, 2002, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
OBP20_EXTRA_INFO_DEF(FULL_TRC, 2003, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
OBP20_EXTRA_INFO_DEF(SESS_INFO_VERI, 2004, EMySQLFieldType::MYSQL_TYPE_VAR_STRING)
OBP20_EXTRA_INFO_DEF(OBP20_SVR_END, 2005, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
OBP20_EXTRA_INFO_DEF(OBP20_SVR_MAX_TYPE, 65535, EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED)
#endif /* OBP20_EXTRA_INFO_DEF */


#ifndef __OBP20_EXTRA_INFO_H__
#define __OBP20_EXTRA_INFO_H__
#include "rpc/obmysql/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/utility/ob_proto_trans_util.h"
#include "common/object/ob_object.h"
#include "deps/oblib/src/lib/string/ob_string_holder.h"

namespace oceanbase {
namespace obmysql {

enum ExtraInfoKeyType {
    #define OBP20_EXTRA_INFO_DEF(extra_id, id, type) extra_id=id,
    #include "obp20_extra_info.h"
    #undef OBP20_EXTRA_INFO_DEF
};

class Obp20Encoder {
  public:
  static const int64_t TYPE_KEY_PLACEHOLDER_LENGTH = 2;
  static const int64_t TYPE_VALUE_LEGNTH_PLACEHOLDER_LENGTH = 4;
  ExtraInfoKeyType type_;
  bool is_serial_; // indicate this encoder has accomplish serialization
  Obp20Encoder() : type_(OBP20_SVR_END), is_serial_(false) {}
  ~Obp20Encoder() {}
  virtual int serialize(char *buf, int64_t len, int64_t &pos) = 0;
  virtual int get_serialize_size() = 0;
  virtual bool has_value() = 0;
  virtual void reset() = 0;
  TO_STRING_KV(K_(type), K_(is_serial));
};

class Obp20Decoder {
  public:
  ExtraInfoKeyType type_;
  Obp20Decoder() : type_(OBP20_SVR_END) {}
  ~Obp20Decoder() {}
  virtual int deserialize(const char *buf, int64_t len, int64_t &pos, Ob20ExtraInfo &extra_info) = 0;
  TO_STRING_KV(K_(type));
};

class Obp20FeedbackProxyInfoEncoder : public Obp20Encoder{
public:
  ObString feedback_proxy_info_;
  Obp20FeedbackProxyInfoEncoder() : feedback_proxy_info_() {
    type_ = FEEDBACK_PROXY_INFO;
  }
  ~Obp20FeedbackProxyInfoEncoder() { reset(); }
  int serialize(char *buf, int64_t len, int64_t &pos);
  int get_serialize_size();
  bool has_value() { return !feedback_proxy_info_.empty(); }
  void reset() { feedback_proxy_info_.reset(); }
};

// proxy -> server verify sess info required: addr, sess_id, proxy_sess_id.
// no need encoder.
class Obp20SessInfoVeriDecoder : public Obp20Decoder{
  public:
  ExtraInfoKeyType type_;
  Obp20SessInfoVeriDecoder() : type_(SESS_INFO_VERI) {}
  ~Obp20SessInfoVeriDecoder() {}
  int deserialize(const char *buf, int64_t len, int64_t &pos, Ob20ExtraInfo &extra_info);
};

class Obp20TraceInfoEncoder : public Obp20Encoder {
  public:
  ObString trace_info_;
  Obp20TraceInfoEncoder() : trace_info_(){
    type_ = TRACE_INFO;
  }
  ~Obp20TraceInfoEncoder() {}
  int serialize(char *buf, int64_t len, int64_t &pos);
  int get_serialize_size();
  bool has_value() { return !trace_info_.empty(); }
  void reset() { trace_info_.reset();  }
};

class Obp20TaceInfoDecoder : public Obp20Decoder{
  public:
  ExtraInfoKeyType type_;
  Obp20TaceInfoDecoder() : type_(TRACE_INFO) {}
  ~Obp20TaceInfoDecoder() {}
  int deserialize(const char *buf, int64_t len, int64_t &pos, Ob20ExtraInfo &extra_info);
};

class Obp20SessInfoEncoder  : public Obp20Encoder {
  public:
  ObString sess_info_;
  Obp20SessInfoEncoder() : sess_info_() {
    type_ = SESS_INFO;
  }
  ~Obp20SessInfoEncoder() {}
  int serialize(char *buf, int64_t len, int64_t &pos);
  int get_serialize_size();
  bool has_value() { return !sess_info_.empty(); }
  void reset() { sess_info_.reset();  }
};

class Obp20SessInfoDecoder : public Obp20Decoder{
  public:
  ExtraInfoKeyType type_;
  Obp20SessInfoDecoder() : type_(SESS_INFO) {}
  ~Obp20SessInfoDecoder() {}
  int deserialize(const char *buf, int64_t len, int64_t &pos, Ob20ExtraInfo &extra_info);
};

class Obp20FullTrcEncoder  : public Obp20Encoder {
  public:
  ObString full_trc_;
  Obp20FullTrcEncoder() : full_trc_() {
    type_ = FULL_TRC;
  }
  ~Obp20FullTrcEncoder() {}
  int serialize(char *buf, int64_t len, int64_t &pos);
  int get_serialize_size();
  bool has_value() { return !full_trc_.empty(); }
  void reset() { full_trc_.reset();  }
};

class Obp20FullTrcDecoder : public Obp20Decoder{
  public:
  ExtraInfoKeyType type_;
  Obp20FullTrcDecoder() : type_(FULL_TRC) {}
  ~Obp20FullTrcDecoder() {}
  int deserialize(const char *buf, int64_t len, int64_t &pos, Ob20ExtraInfo &extra_info);
};
}; // end of namespace lib
}; // end of namespace oceanbase

#endif /* __OBP20_EXTRA_INFO_H__ */
