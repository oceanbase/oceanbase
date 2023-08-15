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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_utils.h"
#include "common/object/ob_object.h"
#include "observer/ob_server.h"
#include "observer/table/ob_table_rpc_processor.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace table;
using namespace observer;
using namespace share::schema;

int ObTableLoadUtils::check_user_access(const common::ObString &credential_str,
                                        const observer::ObGlobalContext &gctx,
                                        table::ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSchemaGetterGuard schema_guard;
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(gctx.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(serialization::decode(credential_str.ptr(), credential_str.length(), pos, credential))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos));
  } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), "tenant_id", credential.tenant_id_);
  } else if (OB_FAIL(schema_guard.get_user_info(credential.tenant_id_, credential.user_id_, user_info))) {
    LOG_WARN("fail to get user info", K(ret), K(credential));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(ret), K(credential));
  } else {
    const uint64_t user_token = user_info->get_passwd_str().hash();
    uint64_t hash_val = 0;
    credential.hash(hash_val, user_token);
    uint64_t my_cluster_id = GCONF.cluster_id;
    if (hash_val != credential.hash_val_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("invalid credential", K(ret), K(credential), K(hash_val));
    } else if (my_cluster_id != credential.cluster_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("invalid credential cluster id", K(ret), K(credential), K(my_cluster_id));
    } else if (user_info->get_is_locked()) { // check whether user is locked.
      ret = OB_ERR_USER_IS_LOCKED;
      LOG_WARN("user is locked", K(ret), K(credential));
    } else {
      LOG_DEBUG("user can access", K(credential));
    }
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObString &src, ObString &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator, src, dest))) {
    LOG_WARN("fail to deep copy str", KR(ret));
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObObj &src, ObObj &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!src.need_deep_copy()) {
    dest = src;
  } else {
    const int64_t size = src.get_deep_copy_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret));
    } else {
      if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
        LOG_WARN("fail to deep copy obj", KR(ret), K(src));
      }
      if (OB_FAIL(ret)) {
        allocator.free(buf);
      }
    }
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObNewRow &src, ObNewRow &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t size = src.get_deep_copy_size();
  char *buf = nullptr;
  int64_t pos = 0;

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
      LOG_WARN("fail to deep copy obj", K(ret), K(src));
    }
    if (OB_FAIL(ret)) {
      allocator.free(buf);
    }
  }

  return ret;
}

int ObTableLoadUtils::deep_copy(const ObStoreRowkey &src, ObStoreRowkey &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.deep_copy(dest, allocator))) {
    LOG_WARN("fail to deep copy store rowkey", KR(ret), K(src));
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObStoreRange &src, ObStoreRange &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.deep_copy(allocator, dest))) {
    LOG_WARN("fail to deep copy store range", KR(ret), K(src));
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObDatumRow &src, ObDatumRow &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dest.deep_copy(src, allocator))) {
    LOG_WARN("fail to deep copy datum row", K(ret), K(src));
  }

  return ret;
}

int ObTableLoadUtils::deep_copy(const ObStorageDatum &src, ObStorageDatum &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest.deep_copy(src, allocator))) {
    LOG_WARN("fail to deep copy datum", KR(ret), K(src));
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObDatumRowkey &src, ObDatumRowkey &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.deep_copy(dest, allocator))) {
    LOG_WARN("fail to deep copy datum rowkey", KR(ret), K(src));
  } else if (OB_FAIL(deep_copy(src.store_rowkey_, dest.store_rowkey_, allocator))) {
    LOG_WARN("fail to deep copy store rowkey", KR(ret), K(src));
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const ObDatumRange &src, ObDatumRange &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy(src.start_key_, dest.start_key_, allocator))) {
    LOG_WARN("fail to deep copy rowkey", KR(ret), K(src));
  } else if (OB_FAIL(deep_copy(src.end_key_, dest.end_key_, allocator))) {
    LOG_WARN("fail to deep copy rowkey", KR(ret), K(src));
  } else {
    dest.group_idx_ = src.group_idx_;
    dest.border_flag_ = src.border_flag_;
  }
  return ret;
}

int ObTableLoadUtils::deep_copy(const sql::ObSQLSessionInfo &src, sql::ObSQLSessionInfo &dest, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_size = src.get_serialize_size();
  int64_t data_len = 0;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", KR(ret), K(buf_size));
  } else if (OB_FAIL(src.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize session info failed", KR(ret));
  } else {
    data_len = pos;
    pos = 0;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dest.deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize session info failed", KR(ret));
    }
  }
  return ret;
}

bool ObTableLoadUtils::is_local_addr(const ObAddr &addr)
{
  return (ObServer::get_instance().get_self() == addr);
}

int ObTableLoadUtils::create_session_info(sql::ObSQLSessionInfo *&session_info, sql::ObFreeSessionCtx &free_session_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("alloc session id failed", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
              tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session_info))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session_info = nullptr;
    LOG_WARN("create session failed", KR(ret), K(sid));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  return ret;
}

void ObTableLoadUtils::free_session_info(sql::ObSQLSessionInfo *session_info, const sql::ObFreeSessionCtx &free_session_ctx)
{
  int ret = OB_SUCCESS;
  if (session_info == nullptr || free_session_ctx.sessid_ == sql::ObSQLSessionInfo::INVALID_SESSID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(session_info), K(free_session_ctx));
  } else {
    session_info->set_session_sleep();
    GCTX.session_mgr_->revert_session(session_info);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session_info = nullptr;
  }
}

int ObTableLoadUtils::generate_credential(uint64_t tenant_id, uint64_t user_id,
                                          uint64_t database_id, int64_t expire_ts,
                                          uint64_t user_token, ObIAllocator &allocator,
                                          ObString &credential_str)
{
  int ret = OB_SUCCESS;
  table::ObTableApiCredential credential;
  credential.cluster_id_ = GCONF.cluster_id;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  credential.database_id_ = database_id;
  credential.expire_ts_ = expire_ts;
  credential.hash(credential.hash_val_, user_token);
  char *credential_buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(credential_buf = static_cast<char *>(allocator.alloc(CREDENTIAL_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (OB_FAIL(serialization::encode(credential_buf, CREDENTIAL_BUF_SIZE, pos, credential))) {
    LOG_WARN("failed to serialize credential", KR(ret), K(pos));
  } else {
    credential_str.assign_ptr(credential_buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObTableLoadUtils::generate_credential(uint64_t tenant_id, uint64_t user_id,
                                          uint64_t database_id, int64_t expire_ts,
                                          uint64_t user_token, char *buf, int64_t size,
                                          ObString &credential_str)
{
  int ret = OB_SUCCESS;
  table::ObTableApiCredential credential;
  credential.cluster_id_ = GCONF.cluster_id;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  credential.database_id_ = database_id;
  credential.expire_ts_ = expire_ts;
  credential.hash(credential.hash_val_, user_token);
  int64_t pos = 0;
  if (OB_FAIL(serialization::encode(buf, size, pos, credential))) {
    LOG_WARN("failed to serialize credential", KR(ret), K(pos));
  } else {
    credential_str.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
