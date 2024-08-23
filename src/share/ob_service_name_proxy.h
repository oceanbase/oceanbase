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

#ifndef OCEANBASE_SHARE_OB_SERVICE_NAME_PROXY_H_
#define OCEANBASE_SHARE_OB_SERVICE_NAME_PROXY_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class ObServiceNameID final
{
    OB_UNIS_VERSION(1);
public:
  static const uint64_t INVALID_SERVICE_NAME_ID = 0;
  static bool is_valid_service_name_id(const uint64_t id) { return INVALID_SERVICE_NAME_ID != id && OB_INVALID_ID != id; }

  explicit ObServiceNameID(const uint64_t id = INVALID_SERVICE_NAME_ID) : id_(id) {}
  ObServiceNameID(const ObServiceNameID &other) : id_(other.id_) {}
  ~ObServiceNameID() { reset(); }

  uint64_t id() const { return id_; }
  void reset() { id_ = INVALID_SERVICE_NAME_ID; }
  // assignment
  ObServiceNameID &operator=(const uint64_t id) { id_ = id; return *this; }
  ObServiceNameID &operator=(const ObServiceNameID &other) { id_ = other.id_; return *this; }
  bool operator==(const ObServiceNameID &other) const { return id_ == other.id_; }
  bool operator!=(const ObServiceNameID &other) const { return id_ != other.id_; }

  bool is_valid() const { return is_valid_service_name_id(id_); }
  TO_STRING_KV(K_(id));
private:
  uint64_t id_;
};
class ObServiceNameString final
{
  OB_UNIS_VERSION(1);
public:
  ObServiceNameString() : str_() {}
  ~ObServiceNameString() {}
  int init(const ObString &str);
  bool equal_to(const ObServiceNameString &service_name_string) const;
  bool is_valid() const { return OB_SUCCESS == check_service_name(str_.str()); }
  int assign(const ObServiceNameString &other);
  bool is_empty() const { return str_.is_empty(); }
  void reset() { return str_.reset(); }
  static int check_service_name(const ObString &service_name_str);
  const char *ptr() const { return str_.ptr(); }
  TO_STRING_KV(K_(str));
private:
  ObFixedLengthString<OB_SERVICE_NAME_LENGTH + 1> str_;
};
struct ObServiceNameArg
{
public:
  enum ObServiceOp {
    INVALID_SERVICE_OP = 0,
    CREATE_SERVICE,
    DELETE_SERVICE,
    START_SERVICE,
    STOP_SERVICE,
    MAX_SERVICE_OP
  };
  static const char *service_op_to_str(const ObServiceOp &service_op);
  ObServiceNameArg()
    : op_(INVALID_SERVICE_OP),
      target_tenant_id_(OB_INVALID_TENANT_ID),
      service_name_str_() {};
  ~ObServiceNameArg() {};
  int init(const ObServiceOp op, const uint64_t target_tenant_id, const ObString &service_name_str);
  bool is_valid() const;
  static bool is_valid_service_op(ObServiceOp op);
  int assign(const ObServiceNameArg &other);
  void reset();
  bool is_create_service() const {return CREATE_SERVICE == op_; }
  bool is_delete_service() const {return DELETE_SERVICE == op_; }
  bool is_start_service() const {return START_SERVICE == op_; }
  bool is_stop_service() const {return STOP_SERVICE == op_; }
  const share::ObServiceNameString& get_service_name_str() const { return service_name_str_; }
  uint64_t get_target_tenant_id() const { return target_tenant_id_; }
  const ObServiceOp &get_service_op() const { return op_; }
  TO_STRING_KV(K_(op), "service_op_to_str", service_op_to_str(op_),
      K_(target_tenant_id), K_(service_name_str));
private:
  ObServiceOp op_;
  uint64_t target_tenant_id_;
  share::ObServiceNameString service_name_str_;
};
struct ObServiceName
{
  OB_UNIS_VERSION(1);
public:
  enum ObServiceStatus
  {
    INVALID_SERVICE_STATUS = 0,
    STARTED,
    STOPPING,
    STOPPED,
    MAX_SERVICE_STATUS
  };
  static const char *service_status_to_str(const ObServiceStatus &service_status);
  static ObServiceStatus str_to_service_status(const ObString &service_status_str);
  static bool is_valid_service_status(const ObServiceStatus &service_status);

  ObServiceName()
      : tenant_id_(OB_INVALID_TENANT_ID),
        service_name_id_(),
        service_name_str_(),
        service_status_(INVALID_SERVICE_STATUS) {}
  ~ObServiceName() {}
  int init(
      const uint64_t tenant_id,
      const uint64_t service_name_id,
      const ObString &service_name_str,
      const ObString &service_status);
  bool is_valid() const;
  int assign(const ObServiceName &other);
  void reset();
  bool is_started() const { return STARTED == service_status_; }
  bool is_stopping() const { return STOPPING == service_status_; }
  bool is_stopped() const {return STOPPED == service_status_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObServiceNameID &get_service_name_id() const { return service_name_id_; }
  const ObServiceNameString &get_service_name_str() const { return service_name_str_; }
  const ObServiceStatus &get_service_status() const { return service_status_; }
  TO_STRING_KV(K_(tenant_id), "service_name_id", service_name_id_.id(), K_(service_name_str), K_(service_status),
      "service_status_str", service_status_to_str(service_status_));
private:
  uint64_t tenant_id_;
  ObServiceNameID service_name_id_;
  ObServiceNameString service_name_str_;
  ObServiceStatus service_status_;
};

class ObServiceNameProxy
{
public:
  static constexpr int64_t SERVICE_NAME_MAX_NUM = 1;
  static int check_is_service_name_enabled(const uint64_t tenant_id);
  static int select_all_service_names_with_epoch(
    const int64_t tenant_id,
    int64_t &epoch,
    ObIArray<ObServiceName> &all_service_names);
  static int select_service_name(
    common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObServiceNameString &service_name_str,
      ObServiceName &service_name);
  static int insert_service_name(
      const uint64_t tenant_id,
      const ObServiceNameString &service_name_str,
      int64_t &epoch,
      ObArray<ObServiceName> &all_service_names);
  static int update_service_status(
      const ObServiceName &service_name,
      const ObServiceName::ObServiceStatus &new_status,
      int64_t &epoch,
      ObArray<ObServiceName> &all_service_names);
  static int delete_service_name(const ObServiceName &service_name);
  static int get_tenant_service_name_num(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      int64_t &service_name_num);

  ObServiceNameProxy() {}
  virtual ~ObServiceNameProxy() {}
private:
  static int select_all_service_names_(
      common::ObISQLClient &sql_proxy,
      const int64_t tenant_id,
      ObIArray<ObServiceName> &all_service_names);
  static int select_service_name_sql_helper_(
      common::ObISQLClient &sql_proxy,
      const int64_t tenant_id,
      const bool extract_epoch,
      ObSqlString &sql,
      int64_t &epoch,
      ObIArray<ObServiceName> &all_service_names);
  static int trans_start_and_precheck_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      int64_t &epoch);
  static void write_and_end_trans_(
      int &ret,
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const ObSqlString &sql,
      ObArray<ObServiceName> &all_service_names);
  static int build_service_name_(
      const common::sqlclient::ObMySQLResult &res,
      ObServiceName &service_name);
  static int get_tenant_service_name_num_(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      int64_t &service_name_num);
  DISALLOW_COPY_AND_ASSIGN(ObServiceNameProxy);
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_SERVICE_NAME_PROXY_H_