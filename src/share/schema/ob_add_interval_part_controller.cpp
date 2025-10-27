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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_add_interval_part_controller.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

ObIntervalTableCacheKey::ObIntervalTableCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    table_id_(OB_INVALID_ID)
{
}

ObIntervalTableCacheKey::ObIntervalTableCacheKey(const uint64_t tenant_id,
                                                 const uint64_t table_id)
  : tenant_id_(tenant_id),
    table_id_(table_id)
{
}

uint64_t ObIntervalTableCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

bool ObIntervalTableCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObIntervalTableCacheKey &other_key = reinterpret_cast<const ObIntervalTableCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_
         && table_id_ == other_key.table_id_;
}

uint64_t ObIntervalTableCacheKey::hash() const
{
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
  hash_code = murmurhash(&table_id_, sizeof(table_id_), hash_code);
  return hash_code;
}

int64_t ObIntervalTableCacheKey::size() const
{
  return sizeof(*this);
}

int ObIntervalTableCacheKey::deep_copy(char *buf,
                                      const int64_t buf_len,
                                      ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  ObIntervalTableCacheKey *pkey = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf_len), K(size()));
  } else {
    pkey = new (buf) ObIntervalTableCacheKey();
    *pkey = *this;
    key = pkey;
  }
  return ret;
}


ObAddIntervalPartitionController::ObAddIntervalPartitionController() : inited_(false)
{
}

ObAddIntervalPartitionController::~ObAddIntervalPartitionController()
{
}

int ObAddIntervalPartitionController::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  }
  return ret;
}

int ObAddIntervalPartitionController::mtl_init(ObAddIntervalPartitionController* &controller)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(controller)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_interval_partition_controller is null", KR(ret));
  } else {
    ret = controller->init();
  }
  return ret;
}

void ObAddIntervalPartitionController::destroy()
{
  inited_ = false;
}

int ObAddIntervalPartitionController::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(constructing_keys_.create(CONSTRUCTING_KEY_SET_SIZE))) {
    LOG_WARN("failed to create constructing keys set", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < COND_SLOT_NUM; i++) {
      if (OB_FAIL(cond_slots_[i].init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
        LOG_WARN("init cond fail", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int ObAddIntervalPartitionController::add_interval_partition_lock(
    AlterTableSchema &alter_table_schema,
    bool &need_clean_up)
{
  int ret = OB_SUCCESS;
  need_clean_up = false;
  ObIntervalTableCacheKey lock_key = ObIntervalTableCacheKey(alter_table_schema.get_tenant_id(),
                                                             alter_table_schema.get_table_id());
  int64_t lock_slot = lock_key.hash() % COND_SLOT_NUM;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  int64_t start_time = ObTimeUtility::current_time();
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  while (OB_SUCC(ret)) {
    bool is_set = false;
    if (OB_FAIL(check_and_set_key_(lock_key, is_set))) {
      LOG_WARN("fail to check and set key in constructing keys", KR(ret));
    } else if (is_set) {
      need_clean_up = true;
      break;
    } else {
      int tmp_ret = OB_SUCCESS;
      ObThreadCondGuard cond_guard(cond_slots_[lock_slot]);
      if (OB_TMP_FAIL(cond_slots_[lock_slot].wait(WAIT_TIMEOUT_MS))) {
        if (OB_TIMEOUT != tmp_ret) {
          LOG_WARN("cond failed to wait", KR(tmp_ret));
        }
      }
    }
    DEBUG_SYNC(BEFORE_CHECK_INTERVAL_PARTITION_EXIST);
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTableSchemaV2 *table_schema = NULL;
    if (FAILEDx(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                alter_table_schema.get_tenant_id(), schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(alter_table_schema.get_tenant_id()));
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(alter_table_schema.get_tenant_id(),
                       alter_table_schema.get_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(alter_table_schema.get_tenant_id()), K(alter_table_schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_INTERVAL_PARTITION_ERROR;
      LOG_WARN("table_schema is null", KR(ret), K(alter_table_schema));
    } else if (OB_FAIL(share::schema::filter_out_duplicate_interval_part(*table_schema, alter_table_schema))) {
      LOG_WARN("fail to check interval partition existence", KR(ret), K(alter_table_schema), K(*table_schema));
    }

    if (ObTimeUtility::current_time() - start_time > 5 * GCONF.rpc_timeout) {
      break;
    }
  }
  return ret;
}

int ObAddIntervalPartitionController::check_and_set_key_(const ObIntervalTableCacheKey &lock_key, bool &is_set)
{
  int ret = OB_SUCCESS;
  is_set = false;
  ObSpinLockGuard lock_guard(interval_partition_lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t tmp_ret = constructing_keys_.exist_refactored(lock_key);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(constructing_keys_.set_refactored_1(lock_key, true/*overwrite_key*/))) {
        LOG_WARN("set refactored failed", KR(ret), K(lock_key));
      } else {
        is_set = true;
      }
    }
  }
  return ret;
}

int ObAddIntervalPartitionController::cleanup_interval_partition_lock(const ObIntervalTableCacheKey &lock_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t lock_slot = lock_key.hash() % COND_SLOT_NUM;
    {
      ObSpinLockGuard lock_guard(interval_partition_lock_);
      if (OB_FAIL(constructing_keys_.erase_refactored(lock_key))) {
        LOG_WARN("fail to erase key", KR(ret), K(lock_key));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(cond_slots_[lock_slot].broadcast())) {
      LOG_WARN("cond fail to broadcast", KR(tmp_ret));
    }
  }
  return ret;
}

ObAddIntervalPartitionGuard::~ObAddIntervalPartitionGuard()
{
  if (need_cleanup_) {
    ObAddIntervalPartitionController *controller = nullptr;
    if (OB_ISNULL(controller = MTL(ObAddIntervalPartitionController *))) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add_interval_partition_controller is null", KR(ret));
    } else {
      (void)controller->cleanup_interval_partition_lock(lock_key_);
    }
  }
}

int ObAddIntervalPartitionGuard::lock(AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
  bool need_clean_up = false;
  ObAddIntervalPartitionController *controller = nullptr;
  if (OB_ISNULL(controller = MTL(ObAddIntervalPartitionController *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add_interval_partition_controller is null", KR(ret));
  } else if (OB_FAIL(controller->add_interval_partition_lock(alter_table_schema,
                                                             need_clean_up))) {
    LOG_WARN("fail to add interval partition lock", KR(ret));
  } else {
    need_cleanup_ = need_clean_up;
  }
  return ret;
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase