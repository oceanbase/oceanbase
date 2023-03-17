// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
    uint64_t hash_val = credential.hash(user_token);
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

bool ObTableLoadUtils::is_local_addr(const ObAddr &addr)
{
  return (ObServer::get_instance().get_self() == addr);
}

int ObTableLoadUtils::init_session_info(uint64_t user_id, ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session_info.init(0, 0, nullptr, nullptr, ObTimeUtility::current_time(), MTL_ID()))) {
    LOG_WARN("fail to init session info", KR(ret));
  }
  OZ(session_info.load_default_sys_variable(false, false)); //加载默认的session参数
  OZ(session_info.load_default_configs_in_pc());
  OX(session_info.set_priv_user_id(user_id));
  return ret;
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
  credential.hash_val_ = credential.hash(user_token);
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
  credential.hash_val_ = credential.hash(user_token);
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
