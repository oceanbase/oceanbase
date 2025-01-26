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
#include "lib/allocator/ob_sql_mem_leak_checker.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::lib;

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
    first_active_time_ = last_active_time_ = ObTimeUtility::fast_current_time();
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
  DISABLE_SQL_MEMLEAK_GUARD;
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
  } else {
    const ObMemAttr attr(OB_SERVER_TENANT_ID, "TbleConnMgr");
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      LOG_WARN("fail to init allocator", K(ret));
    }
  }
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
    ObTableConnection *conn = nullptr;
    if (OB_FAIL(connection_map_.get_refactored(client_addr, conn))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get connection", K(ret), K(client_addr));
      } else { // not eixst, create
        conn = static_cast<ObTableConnection*>(allocator_.alloc(sizeof(ObTableConnection)));
        if (OB_ISNULL(conn)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObTableConnection)));
        } else if (OB_FAIL(conn->init(client_addr, tenant_id, database_id, user_id))) {
          LOG_WARN("fail to init connection", K(ret), K(client_addr), K(tenant_id), K(database_id), K(user_id));
        } else if (OB_FAIL(connection_map_.set_refactored(client_addr, conn))) {
          if (OB_HASH_EXIST != ret) {
            LOG_WARN("fail to set_refactored", K(ret), K(client_addr));
          } else { // already set by other thread
            allocator_.free(conn);
            conn = nullptr;
          }
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(conn)) {
          allocator_.free(conn);
          conn = nullptr;
        }
      }
    } else { // already exist, update it
      conn->set_last_active_time(ObTimeUtility::fast_current_time());
      conn->update_all_ids(tenant_id, database_id, user_id);
    }
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
  } else if (!common::is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tenant id", K(ret), K(tenant_id));
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
    ObTableConnection *conn = nullptr;
    easy_addr_t &ez = c->addr;
    if (!ez2ob_addr(addr, ez)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to convert easy_addr to ob_addr", K(ret));
    } else if (OB_FAIL(connection_map_.erase_refactored(addr, &conn))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_ERROR("fail to remove table connection stat", K(ret), K(addr));
      }
    } else {
      if (OB_NOT_NULL(conn)) {
        allocator_.free(conn);
      }
      LOG_INFO("table connection on close", K(ret), K(c), K(addr), K(connection_map_.size()));
    }
  }
}

