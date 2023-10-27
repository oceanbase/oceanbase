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

#define USING_LOG_PREFIX SERVER
#include "ob_table_connection_mgr.h"
#include "rpc/ob_rpc_request_operator.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

void ObTableConnection::update_last_active_time(int64_t last_active_time)
{
  this->last_active_time_ = last_active_time;
}

void ObTableConnection::update_all_ids(int64_t tenant_id, int64_t database_id, int64_t user_id)
{
  this->tenant_id_ = tenant_id;
  this->database_id_ = database_id;
  this->user_id_ = user_id;
}

int ObTableConnection::init(const common::ObAddr &addr, int64_t tenant_id, int64_t database_id, int64_t user_id)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table connection stat argument", K(ret), K(addr), K(tenant_id), K(database_id), K(user_id));
  } else {
    client_addr_ = addr;
    tenant_id_ = tenant_id;
    database_id_ = database_id;
    user_id_ = user_id;
    first_active_time_ = last_active_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int64_t ObTableConnectionMgr::once_ = 0;
ObTableConnectionMgr *ObTableConnectionMgr::instance_ = NULL;

ObTableConnectionMgr::ObTableConnectionMgr()
    : connection_map_()
{};

ObTableConnectionMgr &ObTableConnectionMgr::get_instance()
{
  ObTableConnectionMgr *instance = NULL;
  while (OB_UNLIKELY(once_ < 2)) {
    if (ATOMIC_BCAS(&once_, 0, 1)) {
      instance = OB_NEW(ObTableConnectionMgr, ObModIds::TABLE_PROC);
      if (OB_LIKELY(OB_NOT_NULL(instance))) {
        if (common::OB_SUCCESS != instance->init()) {
          LOG_WARN_RET(OB_ERROR, "fail to init ObTableConnectionMgr instance");
          OB_DELETE(ObTableConnectionMgr, ObModIds::TABLE_PROC, instance);
          instance = NULL;
          ATOMIC_BCAS(&once_, 1, 0);
        } else {
          instance_ = instance;
          (void)ATOMIC_BCAS(&once_, 1, 2);
        }
      } else {
        (void)ATOMIC_BCAS(&once_, 1, 0);
      }
    }
  }
  return *(ObTableConnectionMgr *)instance_;
}

int ObTableConnectionMgr::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr bucket_attr(OB_SERVER_TENANT_ID, "TableConnBucket");
  ObMemAttr node_attr(OB_SERVER_TENANT_ID, "TableConnNode");
  if (OB_FAIL(connection_map_.create(CONN_INFO_MAP_BUCKET_SIZE, bucket_attr, node_attr))) {
    LOG_WARN("fail to create table connection map", K(ret));
  } else { /* do nothing */ }
  return ret;
}

// update active time if connection exists
// or insert new connnection into connection mgr if not exists
int ObTableConnectionMgr::update_table_connection(const common::ObAddr &client_addr, int64_t tenant_id, int64_t database_id, int64_t user_id)
{
  int ret = OB_SUCCESS;
  if (!client_addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid client address", K(ret), K(client_addr));
  } else {
    ObTableConnUpdater updater(tenant_id, database_id, user_id);
    ObTableConnection conn;
    if (OB_FAIL(conn.init(client_addr, tenant_id, database_id, user_id))) {
      LOG_WARN("fail to init connection", K(ret), K(client_addr), K(tenant_id), K(database_id), K(user_id));
    } else if (OB_FAIL(connection_map_.set_or_update(client_addr, conn, updater))) {
      LOG_WARN("fail to set or update connection", K(ret), K(conn));
    } else {/* do nothing */}
  }
  LOG_DEBUG("update table connection", K(ret), K(client_addr), K(tenant_id), K(database_id),
            K(user_id), K(connection_map_.size()));
  return ret;
}

int ObTableConnectionMgr::update_table_connection(const rpc::ObRequest *req, int64_t tenant_id, int64_t database_id, int64_t user_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null request", K(ret));
  } else {
    const ObAddr &client_addr = RPC_REQ_OP.get_peer(req);
    if (OB_FAIL(update_table_connection(client_addr, tenant_id, database_id, user_id))) {
      LOG_WARN("fail to update table connection", K(ret));
    } else {/* do nothing */}
  }
  return ret;
}

void ObTableConnectionMgr::on_conn_close(easy_connection_t *c)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  if (OB_ISNULL(c)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy connection is null", K(ret));
  } else {
    easy_addr_t &ez = c->addr;
    if (!ez2ob_addr(addr, ez)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to convert easy_addr to ob_addr", K(ret));
    } else if (OB_FAIL(connection_map_.erase_refactored(addr))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_ERROR("fail to remove table connection stat", K(ret), K(addr));
      }
    } else {
      LOG_INFO("table connection on close", K(ret), K(c), K(addr), K(connection_map_.size()));
    }
  }
}

void ObTableConnUpdater::operator() (common::hash::HashMapPair<common::ObAddr, ObTableConnection> &entry)
{
  entry.second.update_last_active_time(ObTimeUtility::current_time());
  entry.second.update_all_ids(tenant_id_, database_id_, user_id_);
}
