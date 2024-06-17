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

#ifndef OCEANBASE_SQL_OB_SESS_INFO_VERI_H_
#define OCEANBASE_SQL_OB_SESS_INFO_VERI_H_

#include "share/ob_define.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "lib/string/ob_string.h"
#include "lib/atomic/ob_atomic.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"


namespace oceanbase
{

namespace obrpc
{
class ObSrvRpcProxy;
}
namespace share {
  class ObBasicSysVar;
  enum ObSysVarClassType;
}

namespace sql
{


// proxy -> server sess info verification.
enum SessionInfoVerificationId
{
  SESS_INFO_VERI_ADDR = 1,
  SESS_INFO_VERI_SESS_ID = 2,
  SESS_INFO_VERI_PROXY_SESS_ID = 3,
  SESS_INFO_VERI_MAX_TYPE
};

class SessionInfoVerifacation {
  OB_UNIS_VERSION(1);
public:
  SessionInfoVerifacation() : addr_(), sess_id_(0), proxy_sess_id_(0) {}
  ~SessionInfoVerifacation() {}
  void reset() {
    addr_.reset();
    sess_id_ = 0;
      proxy_sess_id_ = 0;
  }
  int set_verify_info_sess_id(const uint32_t sess_id);
  int set_verify_info_proxy_sess_id(const uint64_t proxy_sess_id);
  int set_verify_info_addr(const ObAddr addr);
  uint32_t get_verify_info_sess_id() const { return sess_id_; }
  uint64_t get_verify_info_proxy_sess_id() const { return proxy_sess_id_; }
  const common::ObAddr& get_verify_info_addr() const
  {
    return addr_;
  }
  common::ObAddr& get_verify_info_addr()
  {
    return addr_;
  }
  TO_STRING_KV(K_(addr), K_(sess_id), K_(proxy_sess_id));
  ObAddr addr_;
  uint32_t sess_id_;
  uint64_t proxy_sess_id_;
};

class ObSessInfoVerify {


public:
  static int sync_sess_info_veri(sql::ObSQLSessionInfo &sess,
                          const common::ObString &sess_info_veri,
                          SessionInfoVerifacation &sess_info_verification);
  static int verify_session_info(sql::ObSQLSessionInfo &sess,
                          SessionInfoVerifacation &sess_info_verification);
  static int fetch_verify_session_info(sql::ObSQLSessionInfo &sess, common::ObString &result,
                                      common::ObIAllocator &allocator);
  static int compare_verify_session_info(sql::ObSQLSessionInfo &sess,
              common::ObString &result1, common::ObString &result2);
  static void veri_err_injection(sql::ObSQLSessionInfo &sess);
  static int deserialize_sess_info_veri_id(sql::ObSQLSessionInfo &sess,
                SessionInfoVerificationId extra_id, const int64_t v_len,
                      const char *buf, const int64_t len, int64_t &pos,
                      SessionInfoVerifacation &sess_info_verification);
  static int sql_port_to_rpc_port(sql::ObSQLSessionInfo &sess,
                      SessionInfoVerifacation &sess_info_verification);
  static int create_tmp_sys_var(sql::ObSQLSessionInfo &sess,
        share::ObSysVarClassType sys_var_id, share::ObBasicSysVar *&sys_var,
        common::ObIAllocator &allocator);
  static int sess_veri_control(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo *&session);
};

class GetAnotherSessID
{
public:
  GetAnotherSessID()
      : sess_id_(0), proxy_sess_id_(0) {}
  virtual ~GetAnotherSessID() {}
  bool operator()(SessionInfoKey key, ObSQLSessionInfo *sess_info);
  void set_sess_id(uint32_t sess_id ) {sess_id_ = sess_id;}
  void set_proxy_sess_id(uint64_t proxy_sess_id ) {proxy_sess_id_ = proxy_sess_id;}
  uint32_t get_sess_id() { return sess_id_; }
  uint64_t get_proxy_sess_id() { return proxy_sess_id_; }
private:
  uint32_t sess_id_;
  uint64_t proxy_sess_id_;
};

} // namespace sql
} // namespace oceanbase

#endif
