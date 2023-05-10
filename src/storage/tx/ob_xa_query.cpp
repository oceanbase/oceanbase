// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "storage/tx/ob_xa_query.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace sql;
using namespace pl;
using namespace common;
using namespace common::sqlclient;

namespace transaction
{
int ObXAQueryObImpl::init(ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(conn));
  } else {
    conn_ = conn;
    is_inited_ = true;
  }
  return ret;
}

void ObXAQueryObImpl::reset()
{
  conn_ = NULL;
  is_inited_ = false;
}

// select xa_start from dual
#define RM_XA_START_SQL "\
  select dbms_xa.xa_start( \
      DBMS_XA_XID(%ld, \
                  UTL_RAW.cast_to_raw('%.*s'), \
                  UTL_RAW.cast_to_raw('%.*s')), \
      DBMS_XA.TMNOFLAGS) as result \
  from dual"

// NOTE that the input parameter flags is not used
int ObXAQueryObImpl::xa_start(const ObXATransID &xid, const int64_t flags)
{
  UNUSED(flags);
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_start", K(ret));
  }
  return ret;
}

// select xa_end from dual
#define RM_XA_END_SQL "\
  select dbms_xa.xa_end( \
      DBMS_XA_XID(%ld, \
                  UTL_RAW.cast_to_raw('%.*s'), \
                  UTL_RAW.cast_to_raw('%.*s')), \
      DBMS_XA.TMSUCCESS) as result \
  from dual"

int ObXAQueryObImpl::xa_end(const ObXATransID &xid, const int64_t flags)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_end", K(ret));
  }
  return ret;
}

// select xa_prepare from dual
#define RM_XA_PREPARE_SQL "\
  select dbms_xa.xa_prepare( \
      DBMS_XA_XID(%ld, \
                  UTL_RAW.cast_to_raw('%.*s'), \
                  UTL_RAW.cast_to_raw('%.*s'))) \
      as result \
  from dual"

int ObXAQueryObImpl::xa_prepare(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_prepare", K(ret));
  }
  return ret;
}

// select xa_commit from dual
#define RM_XA_COMMIT_SQL "\
  select dbms_xa.xa_commit_with_flags( \
      DBMS_XA_XID(%ld, \
                  UTL_RAW.cast_to_raw('%.*s'), \
                  UTL_RAW.cast_to_raw('%.*s')), \
      %ld) as result \
  from dual"

int ObXAQueryObImpl::xa_commit(const ObXATransID &xid, const int64_t flags)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_commit", K(ret));
  }
  return ret;
}

// select xa_rollback from dual
#define RM_XA_ROLLBACK_SQL "\
  select dbms_xa.xa_rollback( \
      DBMS_XA_XID(%ld, \
                  UTL_RAW.cast_to_raw('%.*s'), \
                  UTL_RAW.cast_to_raw('%.*s'))) \
      as result \
  from dual"

int ObXAQueryObImpl::xa_rollback(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_rollback", K(ret));
  }
  return ret;
}

int ObXAQueryObImpl::execute_query_(const ObSqlString &sql, int &xa_result)
{
  int ret = OB_SUCCESS;
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to execute_query_", K(ret));
  return ret;
}

int ObXAQueryOraImpl::init(ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret));
  } else if (NULL == conn) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(conn));
  } else {
    conn_ = conn;
    is_inited_ = true;
  }
  return ret;
}

#define OCI_DEFAULT            0x00000000
#define OCI_TRANS_NEW          0x00000001
#define OCI_TRANS_JOIN         0x00000002
#define OCI_TRANS_RESUME       0x00000004
#define OCI_TRANS_READONLY     0x00000100
#define OCI_TRANS_READWRITE    0x00000200
#define OCI_TRANS_SERIALIZABLE 0x00000400
#define OCI_TRANS_LOOSE        0x00010000
#define OCI_TRANS_TIGHT        0x00020000
#define OCI_TRANS_TWOPHASE     0x01000000

int ObXAQueryOraImpl::xa_start(const ObXATransID &xid, const int64_t flags)
{
  int ret = OB_SUCCESS;
  uint32_t oci_flag = OCI_DEFAULT;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()
      || !ObXAFlag::is_valid(flags, ObXAReqType::XA_START)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(flags));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else if (OB_FAIL(convert_flag_(flags, ObXAReqType::XA_START, oci_flag))) {
    TRANS_LOG(WARN, "fail to convert xa flag to oci flag", K(ret), K(xid), K(flags), K(oci_flag));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_start", K(ret));
  }
  return ret;
}

int ObXAQueryOraImpl::xa_end(const ObXATransID &xid, const int64_t flags)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()
      || !ObXAFlag::is_valid(flags, ObXAReqType::XA_END)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_end", K(ret));
  }
  return ret;
}


int ObXAQueryOraImpl::xa_prepare(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_prepare", K(ret));
  }
  return ret;
}

int ObXAQueryOraImpl::xa_commit(const ObXATransID &xid, const int64_t flags)
{
  int ret = OB_SUCCESS;
  uint32_t oci_flag = OCI_DEFAULT;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()
      || !ObXAFlag::is_valid(flags, ObXAReqType::XA_COMMIT)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else if (OB_FAIL(convert_flag_(flags, ObXAReqType::XA_COMMIT, oci_flag))) {
    TRANS_LOG(WARN, "fail to convert xa flag to oci flag", K(ret), K(xid), K(flags), K(oci_flag));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_commit", K(ret));
  }
  return ret;
}

int ObXAQueryOraImpl::xa_rollback(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (NULL == conn_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected connection", K(ret), K(xid));
  } else {
  ret = OB_NOT_SUPPORTED;
  TRANS_LOG(WARN, "fail to xa_rollback", K(ret));
  }
  return ret;
}

// NOTE that only support
// xa start, TMNOFLAGS, TMSERIALIZABLE
// xa end, TMSUCCESS
// xa commit, TMNOFLAGS
int ObXAQueryOraImpl::convert_flag_(const int64_t xa_flag,
                                    const int64_t xa_req_type,
                                    uint32_t &oci_flag)
{
  int ret = OB_SUCCESS;
  switch (xa_flag) {
    case ObXAFlag::TMNOFLAGS: {
      if (ObXAReqType::XA_START == xa_req_type) {
        oci_flag = OCI_TRANS_NEW;
      } else if (ObXAReqType::XA_COMMIT == xa_req_type) {
        oci_flag = OCI_TRANS_TWOPHASE;
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case ObXAFlag::TMSUCCESS: {
      if (ObXAReqType::XA_END == xa_req_type) {
        oci_flag = OCI_DEFAULT;
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case ObXAFlag::TMSERIALIZABLE: {
      if (ObXAReqType::XA_START == xa_req_type) {
        oci_flag = OCI_TRANS_SERIALIZABLE;
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

void ObXAQueryOraImpl::reset()
{
  conn_ = NULL;
  is_inited_ = false;
}


} // end namespace of transaction
} // end nemespace of oceanbase
