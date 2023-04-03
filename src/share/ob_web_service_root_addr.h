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

#ifndef OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_
#define OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_

#include "share/ob_root_addr_agent.h"
#include "share/ob_cluster_role.h"        // ObClusterRole
#include "share/ob_cluster_sync_status.h"
#include "lib/json/ob_json.h"
#include "lib/ob_define.h"

#define JSON_RES_CODE "Code"
#define JSON_RES_DATA "Data"
#define JSON_OB_REGION "ObRegion"
#define JSON_OB_REGION_ID "ObRegionId"
#define JSON_OB_CLUSTER "ObCluster"
#define JSON_OB_CLUSTER_ID "ObClusterId"
#define JSON_RS_LIST "RsList"
#define JSON_ADDRESS "address"
#define JSON_ROLE "role"
#define JSON_SQL_PORT "sql_port"
#define JSON_READONLY_RS_LIST "ReadonlyRsList"
#define JSON_PARAM_START "?"
#define JSON_VERSION "version"
#define JSON_PARAM_AND "&"
#define JSON_TYPE "Type"
#define JSON_PRIMARY "PRIMARY"
#define JSON_STANDBY "STANDBY"
#define JSON_TIMESTAMP "timestamp"

namespace oceanbase
{
namespace common
{
class ObSqlString;
class ObString;
class ObIAllocator;
}
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace share
{

struct ObRedoTransportOption
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t DEFAULT_NET_TIMEOUT = 30 * 1000 * 1000; //30s
  static const int64_t DEFAULT_REOPEN = 300 * 1000 * 1000; //300s
  static const int64_t DEFAULT_MAX_FAILURE = 0;//0 always retry
  enum RedoOptionProfile
  {
    INVALID_TYPE = -1,
    SYNC = 0,
    ASYNC,
    NET_TIMEOUT,
    REOPEN,
    MAX_FAILURE,
  };
  int64_t net_timeout_;
  int64_t reopen_;
  int64_t max_failure_;
  bool is_sync_;
  ObRedoTransportOption() : net_timeout_(DEFAULT_NET_TIMEOUT),
                          reopen_(DEFAULT_REOPEN),
                          max_failure_(DEFAULT_MAX_FAILURE),
                          is_sync_(false) {}
  void reset()
  {
    net_timeout_ = DEFAULT_NET_TIMEOUT;
    reopen_ = DEFAULT_REOPEN;
    max_failure_ = DEFAULT_MAX_FAILURE;
    is_sync_ = false;
  }
  ObRedoTransportOption &operator =(const ObRedoTransportOption &other);
  int assign(const ObRedoTransportOption &other);
  bool operator !=(const ObRedoTransportOption &other) const;
  bool operator ==(const ObRedoTransportOption &other) const;
  int append_redo_transport_options_change(const common::ObString &redo_transport_options_str);
  int get_redo_transport_options_str(common::ObSqlString &str) const;
  bool get_is_sync() const { return is_sync_; }
  TO_STRING_KV(K_(net_timeout), K_(reopen), K_(max_failure), K_(is_sync));
private:
  RedoOptionProfile str_to_redo_transport_options(const char *str);
};

struct ObClusterRsAddr
{
  OB_UNIS_VERSION(1);
public:
  ObClusterRsAddr()
    : cluster_id_(common::OB_INVALID_ID),
      addr_()
  {}
  virtual ~ObClusterRsAddr() {}
  void reset();
  bool is_valid() const;
  int assign(const ObClusterRsAddr &other);
  TO_STRING_KV(K_(cluster_id), K_(addr));
public:
  int64_t cluster_id_;
  common::ObAddr addr_;
};

struct ObClusterAddr
{
  OB_UNIS_VERSION(1);
public:
  int64_t cluster_id_;
  common::ObClusterRole cluster_role_;
  common::ObClusterStatus cluster_status_;
  int64_t timestamp_;
  common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH> cluster_name_;
  ObAddrList addr_list_;
  ObAddrList readonly_addr_list_;
  //unique internal label of cluster
  int64_t cluster_idx_;
  int64_t current_scn_;//the current flashback point of cluster
  share::ObRedoTransportOption redo_transport_options_;
  common::ObProtectionLevel protection_level_;

  share::ObClusterSyncStatus sync_status_;
  int64_t last_hb_ts_;
  ObClusterAddr() : cluster_id_(common::OB_INVALID_ID), cluster_role_(common::INVALID_CLUSTER_ROLE),
    cluster_status_(common::INVALID_CLUSTER_STATUS), timestamp_(0),
    cluster_name_(), addr_list_(), readonly_addr_list_(),
    cluster_idx_(common::OB_INVALID_INDEX),
    current_scn_(common::OB_INVALID_VERSION),
    redo_transport_options_(),
    protection_level_(common::INVALID_PROTECTION_LEVEL),
    sync_status_(NOT_AVAILABLE), last_hb_ts_(common::OB_INVALID_TIMESTAMP) {}
  void reset();
  int assign(const ObClusterAddr &other);
  bool is_valid() const;
  int get_addr_list_str(common::ObString &addr_list_str);
  int append_redo_transport_options_change(common::ObString &redo_transport_options,
                                           common::ObIAllocator &alloc) const;
  int construct_rootservice_list(common::ObString &rootservice_list,
                                 common::ObIAllocator &alloc) const;

  bool is_added() const
  { return common::INVALID_CLUSTER_STATUS != cluster_status_
           && common::REGISTERED != cluster_status_
           && common::MAX_CLUSTER_STATUS != cluster_status_; }
  bool operator==(const ObClusterAddr &other) const;
  TO_STRING_KV(K_(cluster_id), K_(cluster_role), K_(cluster_status),
               K_(timestamp), K_(cluster_name),
               K_(addr_list), K_(readonly_addr_list), K_(cluster_idx),
               K_(current_scn), K_(redo_transport_options), K_(protection_level),
               K_(sync_status), K_(last_hb_ts));
};

typedef common::ObIArray<ObClusterAddr> ObClusterIAddrList;
typedef common::ObSArray<ObClusterAddr> ObClusterAddrList;
// store and fetch root server address list via REST style web service.
class ObWebServiceRootAddr : public ObRootAddrAgent
{
public:
  static const int64_t MAX_RECV_CONTENT_LEN = 512 * 1024; // 512KB

  ObWebServiceRootAddr() {}
  virtual ~ObWebServiceRootAddr() {}

  virtual int store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                    const bool force, const common::ObClusterRole cluster_role,
                    const int64_t timestamp);
  virtual int fetch(ObIAddrList &add_list,
                    ObIAddrList &readonly_addr_list);
  /// get RS_LIST from URL
  ///
  /// @param [in] appanme              cluster appanme
  /// @param [in] url                  target URL
  /// @param [in] timeout_ms           timeout : ms
  /// @param [out] rs_list             address list of RS
  /// @param [out] readonly_rs_list    address list of readonly RS
  ///
  /// @retval OB_SUCCESS       success
  /// @retval other error code fail
  static int fetch_rs_list_from_url(
      const char *appname,
      const char *url,
      const int64_t timeout_ms,
      ObIAddrList &rs_list,
      ObIAddrList &readonly_rs_list,
      common::ObClusterRole &cluster_role);

  static int get_all_cluster_info(common::ObServerConfig *config,
                                  ObClusterIAddrList &cluster_list);
  static int to_json(
      const ObIAddrList &addr_list,
      const ObIAddrList &readonly_addr_list,
      const char *appname,
      const int64_t cluster_id,
      const common::ObClusterRole cluster_role,
      const int64_t timestamp,
      common::ObSqlString &json);
  static int from_json(
      const char *json_str,
      const char *appname,
      ObClusterAddr &cluster);
  static int parse_data(const json::Value *data,
                        ObClusterAddr &cluster);
  static int get_addr_list(json::Value *&rs_list, ObIAddrList &addr_list);

private:
  /// store rs_list to URL
  ///
  /// @param rs_list           RS address list
  /// @param readonly_rs_list  readonly RS address list
  /// @param appanme           cluster appanme
  /// @param url               target URL
  /// @param timeout_ms        timeout: ms
  ///
  /// @retval OB_SUCCESS success
  /// @retval other error code fail
  static int store_rs_list_on_url(
      const ObIAddrList &rs_list,
      const ObIAddrList &readonly_rs_list,
      const char *appname,
      const int64_t cluster_id,
      const common::ObClusterRole cluster_role,
      const char *url,
      const int64_t timeout_ms,
      const int64_t timestamp);
  static int add_to_list(common::ObString &addr_str, const int64_t sql_port,
                         common::ObString &role, ObIAddrList &addr_list);

  // call web service, if post_data not NULL use POST method, else use GET method.
  static int call_service(const char *post_data,
      common::ObSqlString &content,
      const char *config_url,
      const int64_t timeout_ms,
      const bool is_delete = false);

  // curl write date interface
  static int64_t curl_write_data(void *ptr, int64_t size, int64_t nmemb, void *stream);
  static int build_new_config_url(char* buf, const int64_t buf_len,
                                  const char* config_url, const int64_t cluster_id);
  //static int get_all_cluster_info(const char *json_str,
  //                                ObClusterIAddrList &cluster_list);
  static int check_store_result(const common::ObSqlString &content);

private:
  DISALLOW_COPY_AND_ASSIGN(ObWebServiceRootAddr);
};

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_
