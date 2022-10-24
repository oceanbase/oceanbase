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

#define USING_LOG_PREFIX SHARE

#include "share/ob_web_service_root_addr.h"
#include <curl/curl.h>
#include "lib/json/ob_json.h"
#include "lib/string/ob_sql_string.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_inner_config_root_addr.h"

namespace oceanbase
{
using namespace common;

namespace share
{
void ObClusterRsAddr::reset()
{
  cluster_id_ = OB_INVALID_ID;
  addr_.reset();
}

bool ObClusterRsAddr::is_valid() const
{
  return cluster_id_ > 0 && addr_.is_valid();
}

int ObClusterRsAddr::assign(const ObClusterRsAddr &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    cluster_id_ = other.cluster_id_;
    addr_ = other.addr_;
  }
  return ret;
}

void ObClusterAddr::reset()
{
  cluster_id_ = common::OB_INVALID_ID;
  cluster_role_ = INVALID_CLUSTER_ROLE;
  cluster_status_ = INVALID_CLUSTER_STATUS;
  timestamp_ = 0;
  cluster_name_.reset();
  addr_list_.reuse();
  readonly_addr_list_.reuse();
  cluster_idx_ = common::OB_INVALID_INDEX;
  current_scn_ = OB_INVALID_VERSION;
  redo_transport_options_.reset();
  protection_level_ = common::MAXIMUM_PERFORMANCE_LEVEL;
  sync_status_ = NOT_AVAILABLE;
  last_hb_ts_ = common::OB_INVALID_TIMESTAMP;
}

bool ObClusterAddr::is_valid() const
{
  bool bret = true;
  if (OB_INVALID_ID == cluster_id_
      || INVALID_CLUSTER_ROLE == cluster_role_
      || 0 == cluster_name_.size()) {
    bret = false;
  }
  return bret;
}
int ObClusterAddr::assign(const ObClusterAddr &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(addr_list_.assign(other.addr_list_))) {
    LOG_WARN("failed to assign addr list", K(ret), K(other));
  } else if (OB_FAIL(readonly_addr_list_.assign(other.readonly_addr_list_))) {
    LOG_WARN("failed to assign addr list", K(ret), K(other));
  } else if (OB_FAIL(cluster_name_.assign(other.cluster_name_))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  } else {
    cluster_id_ = other.cluster_id_;
    cluster_idx_ = other.cluster_idx_;
    cluster_role_ = other.cluster_role_;
    cluster_status_ = other.cluster_status_;
    current_scn_ = other.current_scn_;
    redo_transport_options_ = other.redo_transport_options_;
    protection_level_ = other.protection_level_;
    sync_status_ = other.sync_status_;
    last_hb_ts_ = other.last_hb_ts_;
    timestamp_ = other.timestamp_;
  }
  return ret;
}

bool ObClusterAddr::operator==(const ObClusterAddr &other) const
{
  bool b_ret = false;
  if (this == &other) {
    b_ret = true;
  } else if (addr_list_.count() != other.addr_list_.count()
             || readonly_addr_list_.count() != other.readonly_addr_list_.count()
             || cluster_id_ != other.cluster_id_
             || cluster_idx_ != other.cluster_idx_
             || cluster_role_ != other.cluster_role_
             || cluster_status_ != other.cluster_status_
             || cluster_name_ != other.cluster_name_
             || current_scn_ != other.current_scn_
             || timestamp_ != other.timestamp_
             || redo_transport_options_ != other.redo_transport_options_
             || protection_level_ != other.protection_level_) {
    b_ret = false;
  } else {
    b_ret = true;
    for (int64_t i = 0; b_ret && i < other.addr_list_.count(); ++i) {
      if (addr_list_.at(i) == other.addr_list_.at(i)) {
        continue;
      } else {
        b_ret = false;
      }
    }
    for (int64_t i = 0; b_ret && i < other.readonly_addr_list_.count(); ++i) {
      if (readonly_addr_list_.at(i) == other.readonly_addr_list_.at(i)) {
        continue;
      } else {
        b_ret = false;
      }
    }
  }
  if (!b_ret) {
    LOG_INFO("not equal", K(b_ret), "this", *this, K(other));
  }
  return b_ret;
}
int ObClusterAddr::append_redo_transport_options_change(
    common::ObString &redo_transport_options,
    common::ObIAllocator &alloc) const
{
  int ret = OB_SUCCESS;
  ObSqlString str;

  if (OB_FAIL(redo_transport_options_.get_redo_transport_options_str(str))) {
    LOG_WARN("fail to redo_transport_options", KR(ret), K(redo_transport_options_));
  } else if (OB_FAIL(ob_write_string(alloc, str.string(), redo_transport_options))) {
    LOG_WARN("failed to write stirng", KR(ret), K(str));
  }
  return ret;
}

int ObClusterAddr::construct_rootservice_list(
    common::ObString &rootservice_list,
    common::ObIAllocator &alloc) const
{
  int ret = OB_SUCCESS;
  ObSqlString str;

  if (OB_FAIL(ObInnerConfigRootAddr::format_rootservice_list(addr_list_, str))) {
    LOG_WARN("fail to format rootservice list", KR(ret), K(addr_list_));
  } else if (str.length() <= 0) {
    rootservice_list.reset();
    LOG_INFO("cluster rootservice list is empty", K(addr_list_), K(str));
  } else if (OB_FAIL(ob_write_string(alloc, str.string(), rootservice_list))) {
    LOG_WARN("failed to write stirng", KR(ret), K(str));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObClusterAddr, cluster_id_, cluster_role_, addr_list_,
                    readonly_addr_list_, timestamp_, cluster_name_, cluster_idx_,
                    cluster_status_, current_scn_, redo_transport_options_, protection_level_,
                    sync_status_, last_hb_ts_);

int ObWebServiceRootAddr::build_new_config_url(char* buf, const int64_t buf_len,
                                               const char* config_url, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_ISNULL(config_url) || INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(config_url), K(cluster_id));
  } else {
    int64_t pos = 0;
    //compatible with obtest and configserver
    //it can remove after upgrade obtest
    ObString tmp_url(strlen(config_url), config_url);
    if (OB_ISNULL(tmp_url.find('?'))) {
      BUF_PRINTF("%s?%s=2&%s=%ld", config_url, JSON_VERSION, JSON_OB_CLUSTER_ID, cluster_id);
    } else {
      BUF_PRINTF("%s&%s=2&%s=%ld", config_url, JSON_VERSION, JSON_OB_CLUSTER_ID, cluster_id);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to print buf", K(ret), K(config_url));
    }
  }
  return ret;
}

int ObWebServiceRootAddr::store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                                const bool force, const common::ObClusterRole cluster_role,
                                const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  UNUSED(force); //always update web config
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (addr_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "addr count", addr_list.count());
  } else {
    const char *url = config_->obconfig_url.str();
    const char *appname = config_->cluster.str();
    const int64_t cluster_id = config_->cluster_id;
    //default 4s
    const int64_t timeout_ms = config_->rpc_timeout / 1000 + 2 * 1000;
    const int64_t max_config_length = OB_MAX_CONFIG_URL_LENGTH;
    char new_config_url[max_config_length] = {'\0'};
    if (NULL == url || NULL == appname || cluster_id <= 0 || timeout_ms <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("NULL url or NULL appname or invalid timeout",
          K(ret), KP(url), KP(appname), K(cluster_id), K(timeout_ms));
    } else if (OB_FAIL(build_new_config_url(new_config_url, max_config_length, url, cluster_id))) {
      LOG_WARN("fail to build new config url", K(ret), K(cluster_id), K(url));
    } else if (OB_FAIL(store_rs_list_on_url(addr_list, readonly_addr_list, appname, cluster_id,
                                            cluster_role, new_config_url, timeout_ms,
                                            timestamp))) {
      LOG_WARN("store_rs_list_on_url fail",
          K(ret), K(addr_list), K(readonly_addr_list), K(appname),
          K(new_config_url), K(timeout_ms),
          K(timestamp));
    } else {
      LOG_INFO("store_rs_list_on_url success", K(ret), K(addr_list), K(cluster_role),
               K(readonly_addr_list), K(appname), K(new_config_url), K(timeout_ms));
    }
  }

  return ret;
}


int ObWebServiceRootAddr::fetch(
    ObIAddrList &addr_list,
    ObIAddrList &readonly_addr_list)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(addr_list, readonly_addr_list);
  LOG_WARN("observer can not fetch rs list from configserver", KR(ret));
  return ret;
}

int ObWebServiceRootAddr::store_rs_list_on_url(
    const ObIAddrList &rs_list,
    const ObIAddrList &readonly_rs_list,
    const char *appname,
    const int64_t cluster_id,
    const common::ObClusterRole cluster_role,
    const char *url,
    const int64_t timeout_ms,
    const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  ObSqlString json;
  ObSqlString content;
  if (rs_list.count() <= 0 || NULL == url || NULL == appname
      || cluster_id <= 0 || timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret),
        "addr_count", rs_list.count(), KP(url), KP(appname), K(cluster_id), K(timeout_ms));
  } else if (OB_FAIL(to_json(rs_list, readonly_rs_list, appname,
                             cluster_id, cluster_role, timestamp, json))) {
    LOG_WARN("convert addr list to json format failed", K(ret), K(rs_list), K(readonly_rs_list), K(appname));
  } else if (OB_FAIL(call_service(json.ptr(), content, url, timeout_ms))) {
    LOG_WARN("call web service failed", K(ret), K(json), K(url), K(timeout_ms));
  } else if (OB_FAIL(check_store_result(content))) {
    LOG_WARN("fail to check result", K(ret), K(content), K(url));
  }
  return ret;
}

int ObWebServiceRootAddr::check_store_result(const ObSqlString &content)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;
  json::Value *root = NULL;
  if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_ISNULL(content.ptr()) || content.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid content", K(ret), K(content));
  } else if (OB_FAIL(parser.parse(content.ptr(), strlen(content.ptr()), root))) {
    LOG_WARN("parse json failed", K(ret), K(content));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(content), "root", *root);
  } else {
    bool find_code = false;
    DLIST_FOREACH(it, root->get_object()) {
      if (it->name_.case_compare(JSON_RES_CODE) == 0) {
        find_code = true;
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_NUMBER != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected code type", K(ret), "type", it->value_->get_type());
        } else {
          const int64_t code = it->value_->get_number();
          if (2018 == code) {
            ret = OB_OBCONFIG_CLUSTER_NOT_EXIST;
            LOG_WARN("cluster not exist", K(ret), K(code));
          } else if (200 != code) {
            ret = OB_OBCONFIG_RETURN_ERROR;
            LOG_WARN("return code not success", K(ret), K(code));
          }
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !find_code) {
      ret = OB_OBCONFIG_RETURN_ERROR;
      LOG_WARN("fail to find response code", K(ret));
    }
  }
  return ret;
}

int ObWebServiceRootAddr::fetch_rs_list_from_url(
    const char *appname,
    const char *url,
    const int64_t timeout_ms,
    ObIAddrList &rs_list,
    ObIAddrList &readonly_rs_list,
    ObClusterRole &cluster_role)
{
  int ret = OB_SUCCESS;
  ObSqlString json;
  ObClusterAddr cluster;

  // appname can be NULL to ignore appname check
  if (NULL == url || timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(url), K(timeout_ms));
  } else if (OB_FAIL(call_service(NULL, json, url, timeout_ms))) {
    LOG_WARN("call web service failed", K(ret), K(url), K(timeout_ms));
  } else if (json.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("web service returned empty result", K(ret));
  } else if (OB_FAIL(from_json(json.ptr(), appname, cluster))) {
    LOG_WARN("convert json to rootserver address list failed",
        K(ret), K(json), K(appname));
  } else if (OB_FAIL(rs_list.assign(cluster.addr_list_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else if (OB_FAIL(readonly_rs_list.assign(cluster.readonly_addr_list_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else {
    cluster_role = cluster.cluster_role_;
  }
  return ret;
}

// json format:
//
//    { "rs_list" :
//        [ {
//            "address" : "ip:port",
//            "role" : "leader",
//            "sql_port" : 3306
//          },
//          {
//            "address" : "ip:port",
//            "role" : "follower",
//            "sql_port" : 3306
//          },
//          {
//            "address" : "ip:port",
//            "role" : "follower",
//            "sql_port" : 3306
//          }
//        ],
//      "readonly_rs_list" :
//        [ {
//            "address" : "ip:port",
//            "role" : "follower",
//            "sql_port" : 3306
//          },
//          {
//            "address" : "ip:port",
//            "role" : "follower",
//            "sql_port" : 3306
//          },
//          {
//            "address" : "ip:port",
//            "role" : "follower",
//            "sql_port" : 3306
//          }
//        ]
//    }
//
int ObWebServiceRootAddr::parse_data(const json::Value *data,
                                     ObClusterAddr &cluster)
{
  int ret = OB_SUCCESS;
  json::Value *rs_list = NULL;
  json::Value *readonly_rs_list = NULL;
  cluster.reset();
  if (NULL == data) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no data filed found", K(ret));
  } else if (json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data filed not object", K(ret), "data", *data);
  } else {
    DLIST_FOREACH(it, data->get_object()) {
      if (0 == it->name_.case_compare(JSON_RS_LIST)) {
        rs_list = it->value_;
      } else if (0 == it->name_.case_compare(JSON_READONLY_RS_LIST)) {
        readonly_rs_list = it->value_;
      } else if (0 == it->name_.case_compare(JSON_TYPE)) {
        if (OB_ISNULL(it->value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_STRING != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cluster type", K(ret), "type", it->value_->get_type());
        } else if (it->value_->get_string().case_compare(JSON_PRIMARY) == 0) {
          cluster.cluster_role_ = PRIMARY_CLUSTER;
        } else if (it->value_->get_string().case_compare(JSON_STANDBY) == 0) {
          cluster.cluster_role_ = STANDBY_CLUSTER;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid cluster type", K(ret), "type", it->value_->get_string());
        }
      } else if (it->name_.case_compare(JSON_OB_REGION_ID) == 0
                 || it->name_.case_compare(JSON_OB_CLUSTER_ID) == 0) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_NUMBER != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cluster_id type", K(ret), "type", it->value_->get_type());
        } else {
          cluster.cluster_id_ = it->value_->get_number();
        }
      } else if (it->name_.case_compare(JSON_TIMESTAMP) == 0) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_NUMBER != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cluster_id type", K(ret), "type", it->value_->get_type());
        } else {
          cluster.timestamp_ = it->value_->get_number();
        }
      } else if (0 == it->name_.case_compare(JSON_OB_REGION)
                 || 0 == it->name_.case_compare(JSON_OB_CLUSTER)) {
        if (NULL == it->value_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL value pointer", K(ret));
        } else if (json::JT_STRING != it->value_->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected obregion type", K(ret), "type", it->value_->get_type());
        } else if (OB_FAIL(cluster.cluster_name_.assign(it->value_->get_string()))) {
          LOG_WARN("fail to assign string", KR(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL == rs_list) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no rs list found in json", K(ret));
    } else if (OB_FAIL(get_addr_list(rs_list, cluster.addr_list_))) {
      LOG_WARN("fail to parse rs_list", K(ret), "rs_list", *rs_list);
    } else if (NULL == readonly_rs_list) {
      // readonly_rs_list is allowed to be null
      LOG_WARN("no readonly rs list found in json", K(ret));
    } else if (OB_FAIL(get_addr_list(readonly_rs_list, cluster.readonly_addr_list_))) {
      LOG_WARN("fail to parse readonly_rs_list", K(ret), "readonly_rs_list", *readonly_rs_list);
    }
  }
  return ret;
}

int ObWebServiceRootAddr::from_json(
    const char *json_str,
    const char *appname,
    ObClusterAddr &cluster)
{
  int ret = OB_SUCCESS;
  cluster.reset();
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;
  json::Value *root = NULL;
  // appname can be NULL to ignore appname check
  if (NULL == json_str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(json_str));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str, strlen(json_str), root))) {
    LOG_WARN("parse json failed", K(ret), K(json_str));
  } else if (NULL == root) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else {
    json::Value *data = NULL;
    //  check return code and get data filed
    if (json::JT_OBJECT != root->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error json format", K(ret), K(json_str), "root", *root);
    } else {
      DLIST_FOREACH(it, root->get_object()) {
        if (it->name_.case_compare(JSON_RES_DATA) == 0) {
          data = it->value_;
        }
        if (it->name_.case_compare(JSON_RES_CODE) == 0) {
          if (NULL == it->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_NUMBER != it->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected code type", K(ret), "type", it->value_->get_type());
          } else {
            const int64_t code = it->value_->get_number();
            if (200 != code) {
              ret = OB_OBCONFIG_RETURN_ERROR;
              LOG_WARN("return code not success", K(ret), K(code));
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(data)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no data filed found", K(ret));
    } else if (OB_FAIL(parse_data(data, cluster))) {
      LOG_WARN("fail to parse data", KR(ret));
    } else if (NULL != appname && cluster.cluster_name_.str().case_compare(appname) != 0) {
      ret = OB_OBCONFIG_APPNAME_MISMATCH;
      LOG_ERROR("obconfig appname mismatch",
                K(ret), K(appname), "obregion", cluster.cluster_name_);
    }

  }
  return ret;
}

int ObWebServiceRootAddr::get_addr_list(json::Value *&rs_list, ObIAddrList &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reuse();
  if (OB_ISNULL(rs_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_list is null", K(ret));
  } else if (json::JT_ARRAY != rs_list->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list is not array", K(ret), "type", rs_list->get_type());
  } else {
    ObString addr_str;
    ObString role_str;
    // traverse rs_list array
    DLIST_FOREACH(it, rs_list->get_array()) {
      if (json::JT_OBJECT != it->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not object in array", K(ret), "type", it->get_type());
        break;
      }
      addr_str.reset();
      role_str.reset();
      int64_t sql_port = OB_INVALID_INDEX;
      DLIST_FOREACH(p, it->get_object()) {
        if (p->name_.case_compare(JSON_ADDRESS) == 0) {
          if (NULL == p->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_STRING != p->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected address type", K(ret), "type", p->value_->get_type());
          } else {
            addr_str = p->value_->get_string();
          }
        } else if (p->name_.case_compare(JSON_ROLE) == 0) {
          if (NULL == p->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_STRING != p->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected role type", K(ret), "type", p->value_->get_type());
          } else {
            role_str = p->value_->get_string();
          }
        } else if (p->name_.case_compare(JSON_SQL_PORT) == 0) {
          if (NULL == p->value_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL value pointer", K(ret));
          } else if (json::JT_NUMBER != p->value_->get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected sql_port type", K(ret), "type", p->value_->get_type());
          } else {
            sql_port = p->value_->get_number();
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (addr_str.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no address found in rs object", K(ret));
      } else if (OB_FAIL(add_to_list(addr_str, sql_port, role_str, addr_list))) {
        LOG_WARN("add address to list failed", K(ret),
            K(addr_str), K(sql_port), K(role_str));
      }
    }
  }
  return ret;
}

int ObWebServiceRootAddr::add_to_list(common::ObString &addr_str, const int64_t sql_port,
                                      common::ObString &role_str, ObIAddrList &addr_list)
{
  int ret = OB_SUCCESS;
  if (addr_str.empty() || sql_port <= 0 || role_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr_str), K(sql_port), K(role_str));
  } else {
    ObRootAddr rs;
    ObAddr addr;
    ObRole role;
    if (OB_FAIL(addr.parse_from_string(addr_str))) {
      LOG_WARN("convert string to address failed", K(ret), K(addr_str));
    } else {
      if (role_str.case_compare("LEADER") == 0) {
        role = LEADER;
      } else if (role_str.case_compare("FOLLOWER") != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid role string", K(ret), K(role));
      } else {
        role = FOLLOWER;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(rs.init(addr, role, sql_port))) {
          LOG_WARN("failed to simple init", KR(ret), K(addr), K(role), K(sql_port));
        } else if (OB_FAIL(addr_list.push_back(rs))) {
          LOG_WARN("add to array failed", K(ret), K(rs));
        }
      }
    }
  }
  return ret;
}

int ObWebServiceRootAddr::to_json(
    const ObIAddrList &addr_list,
    const ObIAddrList &readonly_addr_list,
    const char *appname,
    const int64_t cluster_id,
    const common::ObClusterRole cluster_role,
    const int64_t timestamp,
    common::ObSqlString &json)
{
  int ret = OB_SUCCESS;
  const char* type_str = (PRIMARY_CLUSTER == cluster_role) ? JSON_PRIMARY : JSON_STANDBY;
  // TODO support get RS list
  if (OB_ISNULL(appname)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "addr count", addr_list.count(), KP(appname));
  } else if (OB_FAIL(json.append_fmt("{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":%ld,\"%s\":%ld,\"%s\":\"%s\",\"%s\":%ld,\"%s\":[",
                                        JSON_OB_REGION, appname,
                                        JSON_OB_CLUSTER, appname,
                                        JSON_OB_REGION_ID, cluster_id,
                                        JSON_OB_CLUSTER_ID, cluster_id,
                                        JSON_TYPE, type_str, JSON_TIMESTAMP, timestamp, JSON_RS_LIST))) {
    LOG_WARN("assign string failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
      if (i > 0) {
        if (OB_FAIL(json.append(","))) {
          LOG_WARN("append string failed", K(ret));
        }
      }
      char ip_buf[OB_IP_STR_BUFF] = "";
      ObRole role = addr_list.at(i).get_role();
      if (OB_FAIL(ret)) {
      } else if (!addr_list.at(i).get_server().ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert ip to string failed", K(ret), "server", addr_list.at(i).get_server());
      } else if (!is_strong_leader(role) && !is_follower(role)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid role type", K(ret), K(role));
      } else if (OB_FAIL(json.append_fmt("{\"%s\":\"%s:%d\",\"%s\":\"%s\",\"%s\":%ld}",

          JSON_ADDRESS, ip_buf, addr_list.at(i).get_server().get_port(),
          JSON_ROLE, is_strong_leader(role) ? "LEADER" : "FOLLOWER",
          JSON_SQL_PORT, addr_list.at(i).get_sql_port()))) {
        LOG_WARN("append string failed", K(ret));
      }
    }

    // generate readonly_rs_list
    if (OB_SUCC(ret)) {
      if (OB_FAIL(json.append_fmt("],\"%s\":[", JSON_READONLY_RS_LIST))) {
        LOG_WARN("append string failed", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < readonly_addr_list.count(); ++i) {
      if (i > 0) {
        if (OB_FAIL(json.append(","))) {
          LOG_WARN("append string failed", K(ret));
        }
      }
      char ip_buf[OB_IP_STR_BUFF] = "";
      if (OB_FAIL(ret)) {
      } else if (!readonly_addr_list.at(i).get_server().ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert ip to string failed", K(ret), "server", readonly_addr_list.at(i).get_server());
      } else if (OB_FAIL(json.append_fmt("{\"%s\":\"%s:%d\",\"%s\":\"%s\",\"%s\":%ld}",
          JSON_ADDRESS, ip_buf, readonly_addr_list.at(i).get_server().get_port(),
          JSON_ROLE, "FOLLOWER",
          JSON_SQL_PORT, readonly_addr_list.at(i).get_sql_port()))) {
        LOG_WARN("append string failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(json.append_fmt("]}"))) {
        LOG_WARN("append string failed", K(ret));
      } else {
        LOG_INFO("to json success", K(json));
      }
    }
  }
  return ret;
}

int ObWebServiceRootAddr::call_service(const char *post_data,
    common::ObSqlString &content,
    const char *config_url,
    const int64_t timeout_ms,
    const bool is_delete)
{
  int ret = OB_SUCCESS;
  const int64_t begin = ObTimeUtility::current_time();
  CURL *curl = curl_easy_init();
  // post_data can be NULL for http get.
  if (NULL == config_url || timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(config_url), K(timeout_ms));
  } else if (NULL == curl) {
    ret = OB_CURL_ERROR;
    LOG_WARN("init curl failed", K(ret));
  } else {
    // set options
    CURLcode cc = CURLE_OK;
    struct curl_slist *list = NULL;
    if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_URL, config_url))) {
      LOG_WARN("set url failed", K(cc), "url", config_url);
    } else {
      if (NULL != post_data) {
        if (NULL == (list = curl_slist_append(list, "Content-Type: application/json"))) {
          cc = CURLE_OUT_OF_MEMORY;
          LOG_WARN("append list failed", K(cc));
        } else if (CURLE_OK != (cc =  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list))) {
          LOG_WARN("set http header failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(
            curl, CURLOPT_POSTFIELDSIZE, strlen(post_data)))) {
          LOG_WARN("set post data size failed", K(cc), "size", strlen(post_data));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data))) {
          LOG_WARN("set post data failed", K(cc), K(post_data));
        } else if (is_delete
                   && CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE"))) {
          LOG_WARN("fail to set option", KR(ret));
        } else {
          LOG_INFO("post data success", K(post_data));
        }
      }
    }

    const int64_t no_signal = 1;
    const int64_t no_delay = 1;
    const int64_t max_redirect = 3; // set max redirect
    const int64_t follow_location = 1; // for http redirect 301 302
    if (CURLE_OK != cc) {
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, no_signal))) {
      LOG_WARN("set no signal failed", K(cc), K(no_signal));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms))) {
      LOG_WARN("set timeout failed", K(cc), K(timeout_ms));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms))) {
      LOG_WARN("set connect timeout failed", K(cc), K(timeout_ms));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, no_delay))) {
      LOG_WARN("set no delay failed", K(cc), K(no_delay));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, max_redirect))) {
      LOG_WARN("set max redirect failed", K(cc), K(max_redirect));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, follow_location))) {
      LOG_WARN("set follow location failed", K(cc), K(follow_location));
    } else if (CURLE_OK != (cc = curl_easy_setopt(
        curl, CURLOPT_WRITEFUNCTION, &curl_write_data))) {
      LOG_WARN("set write function failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(
        curl, CURLOPT_WRITEDATA, &content))) {
      LOG_WARN("set write data failed", K(cc));
    }

    // send && recv
    int64_t http_code = 0;
    if (CURLE_OK != cc) {
    } else if (CURLE_OK != (cc = curl_easy_perform(curl))) {
      LOG_WARN("curl easy perform failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_getinfo(
        curl, CURLINFO_RESPONSE_CODE, &http_code))) {
      LOG_WARN("curl getinfo failed", K(cc));
    } else {
      // http status code 2xx means success
      if (http_code / 100 != 2) {
        ret = OB_CURL_ERROR;
        LOG_WARN("unexpected http status code", K(ret), K(http_code), K(content));
      }
    }

    if (OB_SUCC(ret)) {
      if (CURLE_OK != cc) {
        ret = OB_CURL_ERROR;
        LOG_WARN("curl error", K(ret), "curl_error_code", cc,
            "curl_error_message", curl_easy_strerror(cc));
      }
    }
    if (NULL != list) {
      curl_slist_free_all(list);
      list = NULL;
    }
  }
  if (NULL != curl) {
    curl_easy_cleanup(curl);
    curl = NULL;
  }
  const int64_t consume_time = ObTimeUtility::current_time() - begin;
  LOG_TRACE("call service", K(ret), K(consume_time),
      "method", NULL == post_data ? "GET" : "POST");
  return ret;
}

int64_t ObWebServiceRootAddr::curl_write_data(
    void *ptr, int64_t size, int64_t nmemb, void *stream)
{
  int ret = OB_SUCCESS;
  // This function may be called with zero bytes data if the transferred file is empty.
  if (NULL == stream || size < 0 || nmemb < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stream), K(size), K(nmemb));
  } else {
    ObSqlString *content = static_cast<ObSqlString *>(stream);
    if (size * nmemb > 0) {
      if (NULL == ptr) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive data but pointer is NULL", K(ret), KP(ptr), K(size), K(nmemb));
      } else if (size * nmemb + content->length() > MAX_RECV_CONTENT_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("unexpected long content", K(ret),
            "new_byte", size * nmemb,
            "received_byte", content->length(),
            LITERAL_K(MAX_RECV_CONTENT_LEN));
      } else if (OB_FAIL(content->append(static_cast<const char *>(ptr), size * nmemb))) {
        LOG_WARN("append data failed", K(ret));
      }
    }
  }

  return OB_SUCCESS == ret ? size * nmemb : 0;
}


//{
//    "Message": "successful",
//    "Success": true,
//    "Code": 200,
//    "Data": [{
//        "ObRegion": "ob2.rongxuan.lc",
//        "ObRegionId": 2,
//        "RsList": [{
//            "address": "10.101.67.165:16825",
//            "role": "LEADER",
//            "sql_port": 16860
//        }, {
//            "address": "10.218.78.76:16827",
//            "role": "FOLLOWER",
//            "sql_port": 16862
//        }],
//        "ReadonlyRsList": []
//    }]
//}

int ObWebServiceRootAddr::get_all_cluster_info(common::ObServerConfig *config,
                                               ObClusterIAddrList &cluster_list)
{
  int ret = OB_SUCCESS;
  UNUSED(config);
  cluster_list.reset();
  //if (OB_ISNULL(config)) {
  //  ret = OB_INVALID_ARGUMENT;
  //  LOG_WARN("invalid argument", K(ret), K(config));
  //} else {
  //  const char *url = config->obconfig_url.str();
  //  const char *appname = config->cluster.str();
  //  int64_t timeout_ms = config->rpc_timeout / 1000;
  //  const int64_t buf_len = OB_MAX_CONFIG_URL_LENGTH;
  //  char buf[buf_len] = {'\0'};
  //  if (NULL == url || NULL == appname || timeout_ms <= 0) {
  //    ret = OB_ERR_UNEXPECTED;
  //    LOG_ERROR("NULL url or NULL appname or invalid timeout",
  //              K(ret), KP(url), KP(appname), K(timeout_ms));
  //  } else {
  //    int64_t pos = 0;
  //    ObString tmp_url(strlen(url), url);
  //    if (OB_ISNULL(tmp_url.find('?'))) {
  //      BUF_PRINTF("%s?%s=2", url, JSON_VERSION);
  //    } else {
  //      BUF_PRINTF("%s&%s=2", url, JSON_VERSION);
  //    }
  //  }
  //  ObSqlString json;
  //  if (OB_FAIL(ret)) {
  //  } else if (OB_FAIL(call_service(NULL, json, buf, timeout_ms))) {
  //    LOG_WARN("fail to call service", K(ret), K(buf));
  //  } else if (json.empty()) {
  //    ret = OB_ERR_UNEXPECTED;
  //    LOG_WARN("web service returned empty result", K(ret));
  //  } else if (OB_FAIL(get_all_cluster_info(json.ptr(), cluster_list))) {
  //    LOG_WARN("fail to get all cluster addr", K(ret), K(json), K(buf));
  //  } else {
  //    LOG_INFO("get all cluster addr success", K(cluster_list));
  //  }
  //}
  return ret;
}

//int ObWebServiceRootAddr::get_all_cluster_info(const char *json_str,
//                                               ObClusterIAddrList &cluster_list)
//{
//  int ret = OB_SUCCESS;
//  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
//  json::Parser parser;
//  json::Value *root = NULL;
//  if (NULL == json_str) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("invalid argument", K(ret), KP(json_str));
//  } else if (OB_FAIL(parser.init(&allocator))) {
//    LOG_WARN("json parser init failed", K(ret));
//  } else if (OB_FAIL(parser.parse(json_str, strlen(json_str), root))) {
//    LOG_WARN("parse json failed", K(ret), K(json_str));
//  } else if (NULL == root) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("no root value", K(ret));
//  } else {
//    cluster_list.reuse();
//    json::Value *data = NULL;
//    //  check return code and get data filed
//    if (json::JT_OBJECT != root->get_type()) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("error json format", K(ret), K(json_str), "root", *root);
//    } else {
//      DLIST_FOREACH(it, root->get_object()) {
//        if (it->name_.case_compare(JSON_RES_DATA) == 0) {
//          data = it->value_;
//        }
//        if (it->name_.case_compare(JSON_RES_CODE) == 0) {
//          if (NULL == it->value_) {
//            ret = OB_ERR_UNEXPECTED;
//            LOG_WARN("NULL value pointer", K(ret));
//          } else if (json::JT_NUMBER != it->value_->get_type()) {
//            ret = OB_ERR_UNEXPECTED;
//            LOG_WARN("unexpected code type", K(ret), "type", it->value_->get_type());
//          } else {
//            const int64_t code = it->value_->get_number();
//            if (200 != code) {
//              ret = OB_OBCONFIG_RETURN_ERROR;
//              LOG_WARN("return code not success", K(ret), K(code));
//            }
//          }
//        }
//      }
//    }
//
//    // check appname and get data
//    if (OB_SUCC(ret)) {
//      if (OB_ISNULL(data)) {
//        ret = OB_ERR_UNEXPECTED;
//        LOG_WARN("no data filed found", K(ret));
//      } else if (json::JT_ARRAY != data->get_type()) {
//        ret = OB_INVALD_WEB_SERVICE_CONTENT;
//        LOG_WARN("data filed not array", K(ret), "data", *data);
//      } else {
//        ObClusterAddr cluster_addr;
//        DLIST_FOREACH(it, data->get_array()) {
//          if (json::JT_OBJECT != it->get_type()) {
//            ret = OB_ERR_UNEXPECTED;
//            LOG_WARN("not object in array", K(ret), "type", it->get_type());
//            break;
//          }
//          cluster_addr.reset();
//          DLIST_FOREACH(p, it->get_object()) {
//            if (p->name_.case_compare(JSON_OB_CLUSTER) == 0) {
//              if (OB_ISNULL(p->value_)) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("NULL value pointer", K(ret));
//              } else if (json::JT_STRING != p->value_->get_type()) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("unexpected address type", K(ret), "type", p->value_->get_type());
//              } else {
//                cluster_addr.cluster_name_ = p->value_->get_string();
//              }
//            } else if (p->name_.case_compare(JSON_TIMESTAMP) == 0) {
//              if (NULL == p->value_) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("NULL value pointer", K(ret));
//              } else if (json::JT_NUMBER != p->value_->get_type()) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("unexpected cluster_id type", K(ret), "type", p->value_->get_type());
//              } else {
//                cluster_addr.timestamp_ = p->value_->get_number();
//              }
//            } else if (p->name_.case_compare(JSON_OB_CLUSTER_ID) == 0) {
//              if (NULL == p->value_) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("NULL value pointer", K(ret));
//              } else if (json::JT_NUMBER != p->value_->get_type()) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("unexpected cluster_id type", K(ret), "type", p->value_->get_type());
//              } else {
//                cluster_addr.cluster_id_ = p->value_->get_number();
//              }
//            } else if (p->name_.case_compare(JSON_TYPE) == 0) {
//              if (OB_ISNULL(p->value_)) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("NULL value pointer", K(ret));
//              } else if (json::JT_STRING != p->value_->get_type()) {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("unexpected cluster type", K(ret), "type", p->value_->get_type());
//              } else if (p->value_->get_string().case_compare(JSON_PRIMARY) == 0) {
//                cluster_addr.cluster_role_ = PRIMARY_CLUSTER;
//              } else if (p->value_->get_string().case_compare(JSON_STANDBY) == 0) {
//                cluster_addr.cluster_role_ = STANDBY_CLUSTER;
//              } else {
//                ret = OB_ERR_UNEXPECTED;
//                LOG_WARN("invalid cluster type", K(ret), "type", p->value_->get_string());
//              }
//            } else if (p->name_.case_compare(JSON_RS_LIST) == 0) {
//              if (OB_FAIL(get_addr_list(p->value_, cluster_addr.addr_list_))) {
//                LOG_WARN("fail to get addr list", K(ret), K(p));
//              }
//            } else if (p->name_.case_compare(JSON_READONLY_RS_LIST) == 0) {
//              if (OB_FAIL(get_addr_list(p->value_, cluster_addr.readonly_addr_list_))) {
//                LOG_WARN("fail to get addr list", K(ret), K(p));
//              }
//            }
//          }
//          if (OB_FAIL(ret)) {
//            //nothing todo
//          } else if (OB_FAIL(cluster_list.push_back(cluster_addr))) {
//            LOG_WARN("fail to push back cluster addr", K(ret), K(cluster_addr));
//          } else {
//            LOG_INFO("push back cluster addr success", K(cluster_addr));
//          }
//        } //end DLIST_FOREACH(it, data->get_array(
//      }
//    }
//  }
//  return ret;
//}


static const char *redo_transport_options_strs[] = {"SYNC", "ASYNC", "NET_TIMEOUT", "REOPEN", "MAX_FAILURE"};


ObRedoTransportOption::RedoOptionProfile ObRedoTransportOption::str_to_redo_transport_options(const char *str)
{
  RedoOptionProfile redo_transport_options = RedoOptionProfile::INVALID_TYPE;
  if (OB_ISNULL(str)) {
    //nothing
  } else {
    ObString option(str);
    for (int64_t i = 0; i < ARRAYSIZEOF(redo_transport_options_strs); i++) {
      if (0 == option.case_compare(redo_transport_options_strs[i])) {
        redo_transport_options = static_cast<RedoOptionProfile>(i);
        break;
      }
    }
  }
  return redo_transport_options;
}
OB_SERIALIZE_MEMBER(ObRedoTransportOption, net_timeout_, reopen_, max_failure_, is_sync_);

int ObRedoTransportOption::append_redo_transport_options_change(
    const ObString &redo_transport_options_str) {
  int ret = OB_SUCCESS;
  //incremental update
  if (redo_transport_options_str.empty()) {
  } else {
    SMART_VAR(char[OB_MAX_CONFIG_VALUE_LEN], format_str) {
      if (OB_FAIL(ObConfigLogArchiveOptionsItem::format_option_str(
              to_cstring(redo_transport_options_str),
              redo_transport_options_str.length(),
              format_str,
              OB_MAX_CONFIG_VALUE_LEN))) {
        LOG_WARN("failed to format option str", KR(ret));
      } else {
        char *saveptr = NULL;
        char *key_name = STRTOK_R(format_str, " ", &saveptr);
        while(OB_NOT_NULL(key_name) && OB_SUCC(ret)) {
          char *s = NULL;
          char *value_str = NULL;
          RedoOptionProfile redo_transport_options = str_to_redo_transport_options(key_name);
          if (RedoOptionProfile::INVALID_TYPE == redo_transport_options) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to construct redo option", KR(ret),
                K(redo_transport_options_str), K(redo_transport_options));
          } else if (RedoOptionProfile::SYNC == redo_transport_options) {
            is_sync_ = true;
          } else if (RedoOptionProfile::ASYNC == redo_transport_options) {
            is_sync_ = false;
          } else if (OB_ISNULL(s = STRTOK_R(NULL, " ", &saveptr))) {
            ret = OB_ERR_PARAM_INVALID;
            LOG_WARN("failed to get value", KR(ret), K(saveptr));
          } else if (0 != ObString::make_string("=").case_compare(s)) {
            ret = OB_ERR_PARAM_INVALID;
            LOG_WARN("expect \"=\"", KR(ret), K(s), K(saveptr));
          } else if (OB_ISNULL(value_str = STRTOK_R(NULL, " ", &saveptr))) {
            ret = OB_ERR_PARAM_INVALID;
            LOG_WARN("failed to get value", KR(ret), K(saveptr));
          } else {
            int64_t value = 0;
            if (OB_FAIL(ob_atoll(value_str, value))) {
              LOG_WARN("failed to get number", KR(ret), K(value_str));
            } else if (OB_UNLIKELY(value < 0)) {
              ret = OB_ERR_PARAM_INVALID;
              LOG_WARN("value should not be negative number", KR(ret), K(value_str));
            } else if (RedoOptionProfile::NET_TIMEOUT == redo_transport_options) {
              net_timeout_ = value;
            } else if (RedoOptionProfile::REOPEN == redo_transport_options) {
              reopen_ = value;
            } else if (RedoOptionProfile::MAX_FAILURE == redo_transport_options) {
              max_failure_ = value;
            } else {
              ret = OB_ERR_PARAM_INVALID;
              LOG_WARN("unknown redo option", KR(ret), K(redo_transport_options));
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            key_name = STRTOK_R(NULL, " ", &saveptr);
          }
        }
      }
    }
  }
  return ret;
}

int ObRedoTransportOption::assign(const ObRedoTransportOption &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    net_timeout_ = other.net_timeout_;
    reopen_ = other.reopen_;
    max_failure_ = other.max_failure_;
    is_sync_ = other.is_sync_;
  }
  return ret;
}

ObRedoTransportOption& ObRedoTransportOption::operator=(const ObRedoTransportOption &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("fail to assign", KR(ret), K(other));
    }
  }
  return *this;
}

bool ObRedoTransportOption::operator!=(const ObRedoTransportOption &other) const
{
  return !(*this == other);
}
bool ObRedoTransportOption::operator==(const ObRedoTransportOption &other) const
{
  return net_timeout_ == other.net_timeout_
         && reopen_ == other.reopen_
         && max_failure_ == other.max_failure_
         && is_sync_ == other.is_sync_;
}

int ObRedoTransportOption::get_redo_transport_options_str(ObSqlString &str) const
{
  int ret = OB_SUCCESS;
  if (is_sync_) {
    if (OB_FAIL(str.assign("SYNC "))) {
      LOG_WARN("failed to assign str", KR(ret));
    }
  } else if (OB_FAIL(str.assign("ASYNC "))) {
    LOG_WARN("failed to assign str", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(str.append_fmt("NET_TIMEOUT = %ld",
                                    net_timeout_))) {
    LOG_WARN("failed to assign str", KR(ret), "this", *this);
  }
  return ret;
}
} // end namespace share
} // end oceanbase
