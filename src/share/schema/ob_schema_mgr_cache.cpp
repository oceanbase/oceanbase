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

#include "ob_schema_mgr_cache.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/config/ob_server_config.h"
#include "common/ob_clock_generator.h"
#include "lib/oblog/ob_log.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace common;
void ObSchemaSlot::reset() {
  tenant_id_ = OB_INVALID_TENANT_ID;
  slot_id_ = OB_INVALID_INDEX;
  schema_version_ = OB_INVALID_VERSION;
  schema_count_ = OB_INVALID_COUNT;
  mod_ref_infos_.reset();
  ref_cnt_ = OB_INVALID_COUNT;
  allocator_idx_ = OB_INVALID_INDEX;
}

void ObSchemaSlot::init(const uint64_t &tenant_id, const int64_t &slot_id,
                        const int64_t &schema_version, const int64_t &schema_count,
                        const int64_t &ref_cnt, const common::ObString &str, const int64_t &allocator_idx) {
  tenant_id_ = tenant_id;
  slot_id_ = slot_id;
  schema_version_ = schema_version;
  schema_count_ = schema_count;
  ref_cnt_ = ref_cnt;
  mod_ref_infos_ = str;
  allocator_idx_ = allocator_idx;
}

namespace share
{
namespace schema
{

ObSchemaMgrHandle::ObSchemaMgrHandle()
  : schema_mgr_item_(NULL), ref_timestamp_(0), mod_(ObSchemaMgrItem::MOD_STACK)
{
}

ObSchemaMgrHandle::ObSchemaMgrHandle(const ObSchemaMgrItem::Mod mod)
  : schema_mgr_item_(NULL), ref_timestamp_(0), mod_(mod)
{
}

ObSchemaMgrHandle::~ObSchemaMgrHandle()
{
  reset();
}

ObSchemaMgrHandle::ObSchemaMgrHandle(const ObSchemaMgrHandle &other)
  : schema_mgr_item_(NULL)
{
  *this = other;
}

ObSchemaMgrHandle &ObSchemaMgrHandle::operator =(const ObSchemaMgrHandle &other)
{
  if (this != &other) {
    reset();
    schema_mgr_item_ = other.schema_mgr_item_;
    mod_ = other.mod_;
    if (NULL != schema_mgr_item_) {
      (void)ATOMIC_FAA(&schema_mgr_item_->ref_cnt_, 1);
      (void)ATOMIC_FAA(&schema_mgr_item_->mod_ref_cnt_[mod_], 1);
    }
  }
  return *this;
}

void ObSchemaMgrHandle::reset()
{
  revert();
  schema_mgr_item_ = NULL;
  ref_timestamp_ = 0;
  // mod_ should not be reset
}

bool ObSchemaMgrHandle::is_valid()
{
  int64_t ref_cnt = 0;
  if (NULL != schema_mgr_item_) {
    ref_cnt = ATOMIC_LOAD(&schema_mgr_item_->ref_cnt_);
  }
  return NULL != schema_mgr_item_ && ref_cnt > 0 && mod_ >= 0 && mod_ < ObSchemaMgrItem::MOD_MAX;
}

void ObSchemaMgrHandle::dump() const
{
  LOG_INFO("schema mgr item ptr", K(schema_mgr_item_), K(ref_timestamp_), K_(mod));
}

inline void ObSchemaMgrHandle::revert()
{
  if (NULL != schema_mgr_item_) {
    if (OB_NOT_NULL(schema_mgr_item_)
        && OB_NOT_NULL(schema_mgr_item_->schema_mgr_)
        && ref_timestamp_ > 0
        && ObClockGenerator::getClock() - ref_timestamp_ >= REF_TIME_THRESHOLD) {
      ObSchemaMgr *&schema_mgr = schema_mgr_item_->schema_mgr_;
      LOG_WARN_RET(OB_SUCCESS, "long time to hold one guard", K(schema_mgr),
               "tenant_id", schema_mgr->get_tenant_id(),
               "version", schema_mgr->get_schema_version(),
               "cur_timestamp", ObTimeUtility::current_time(),
               K_(ref_timestamp), K(lbt()));
    }
    (void)ATOMIC_FAA(&schema_mgr_item_->ref_cnt_, -1);
    (void)ATOMIC_FAA(&schema_mgr_item_->mod_ref_cnt_[mod_], -1);
  }
}

ObSchemaMgrCache::ObSchemaMgrCache()
    : lock_(common::ObLatchIds::SCHEMA_MGR_CACHE_LOCK),
      schema_mgr_items_(NULL),
      max_cached_num_(0),
      last_get_schema_idx_(0),
      cur_cached_num_(0),
      mode_(REFRESH),
      latest_schema_idx_(0)
{
}

ObSchemaMgrCache::~ObSchemaMgrCache()
{
  // TODO: release
}

int ObSchemaMgrCache::init(int64_t init_cached_num, Mode mode)
{
  int ret = OB_SUCCESS;

  if (init_cached_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(init_cached_num));
  } else {
    max_cached_num_ = init_cached_num;
    mode_ = mode;
    auto attr = SET_USE_500("SchemaMgrCache", ObCtxIds::SCHEMA_SERVICE);
    void *ptr = ob_malloc(sizeof(ObSchemaMgrItem[MAX_SCHEMA_SLOT_NUM]), attr);
    if (NULL == ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc mem failed", K(ret));
    } else {
      schema_mgr_items_ = (ObSchemaMgrItem*)ptr;
      for (int64_t i = 0; i < MAX_SCHEMA_SLOT_NUM; ++i) {
        new (&schema_mgr_items_[i])ObSchemaMgrItem();
        ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
        schema_mgr_item.schema_mgr_ = NULL;
        schema_mgr_item.ref_cnt_ = 0;
        MEMSET(schema_mgr_item.mod_ref_cnt_, 0, ObSchemaMgrItem::MOD_MAX);
      }
    }
  }

  return ret;
}

inline bool ObSchemaMgrCache::check_inner_stat() const
{
  bool ret = true;
  if (OB_ISNULL(schema_mgr_items_)
      || max_cached_num_ <= 0) {
    ret = false;
    LOG_WARN("inner stat error",
             K(schema_mgr_items_),
             K(max_cached_num_));
  }
  return ret;
}

int ObSchemaMgrCache::check_schema_mgr_exist(const int64_t schema_version, bool &is_exist)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  ObSchemaMgrHandle handle;
  is_exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(schema_version));
  } else if (OB_FAIL(get(schema_version, schema_mgr, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get schema_mgr", K(ret), K(schema_version));
    }
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", K(ret), K(schema_version));
  } else {
    is_exist = true;
  }
  return ret;
}

int ObSchemaMgrCache::get(const int64_t schema_version,
                          const ObSchemaMgr *&schema_mgr,
                          ObSchemaMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  schema_mgr = NULL;
  handle.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(schema_version));
  } else {
    ObSchemaMgrItem *dst_item = NULL;
    bool is_stop = false;
    int64_t next_idx = last_get_schema_idx_; // not thread safe
    TCRLockGuard guard(lock_);
    ObSchemaMgr *latest_schema_mgr = schema_mgr_items_[latest_schema_idx_].schema_mgr_;
    if (OB_NOT_NULL(latest_schema_mgr) && latest_schema_mgr->get_schema_version() == schema_version) {
      dst_item = &schema_mgr_items_[latest_schema_idx_];
      is_stop = true;
      last_get_schema_idx_ = latest_schema_idx_;
    }
    for (int64_t i = 0; i < max_cached_num_ && next_idx < max_cached_num_ && !is_stop; ++i) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[next_idx];
      ObSchemaMgr *tmp_schema_mgr = schema_mgr_item.schema_mgr_;
      if (NULL == tmp_schema_mgr) {
        // do-nothing
      } else if (tmp_schema_mgr->get_schema_version() != schema_version) {
        // do-nothing
      } else {
        dst_item = &schema_mgr_item;
        is_stop = true;
        last_get_schema_idx_ = next_idx; // not thread safe, but it's ok
      }
      if (++next_idx >= max_cached_num_) {
        next_idx = 0;
      }
    }
    if (NULL == dst_item) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      (void)ATOMIC_FAA(&dst_item->ref_cnt_, 1);
      (void)ATOMIC_FAA(&dst_item->mod_ref_cnt_[handle.mod_], 1);
      schema_mgr = dst_item->schema_mgr_;
      handle.schema_mgr_item_ = dst_item;
      handle.ref_timestamp_ = ObClockGenerator::getClock();
    }
  }

  return ret;
}

int ObSchemaMgrCache::get_nearest(const int64_t schema_version,
                          const ObSchemaMgr *&schema_mgr,
                          ObSchemaMgrHandle &handle)
{
  int ret = OB_ENTRY_NOT_EXIST;
  schema_mgr = NULL;
  handle.reset();

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(schema_version));
  } else {
    ObSchemaMgrItem *dst_item = NULL;
    int64_t nearest_pos = -1;
    int64_t min_version_diff = INT64_MAX;
    int64_t tmp_version_diff = 0;
    TCRLockGuard guard(lock_);
    for (int64_t i = 0; i < max_cached_num_; ++i) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      ObSchemaMgr *tmp_schema_mgr = schema_mgr_item.schema_mgr_;
      if (NULL == tmp_schema_mgr) {
        // do-nothing
      } else {
        tmp_version_diff = llabs(tmp_schema_mgr->get_schema_version() - schema_version);
        if (tmp_version_diff < min_version_diff) {
          nearest_pos = i;
          min_version_diff = tmp_version_diff;
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret)) {
      dst_item = &schema_mgr_items_[nearest_pos];
      (void)ATOMIC_FAA(&dst_item->ref_cnt_, 1);
      (void)ATOMIC_FAA(&dst_item->mod_ref_cnt_[handle.mod_], 1);
      schema_mgr = dst_item->schema_mgr_;
      handle.schema_mgr_item_ = dst_item;
      handle.ref_timestamp_ = ObTimeUtility::current_time();
    }
  }

  return ret;
}

// Return the least referenced schema_version; if there is no reference, return the current latest schema version
int ObSchemaMgrCache::get_recycle_schema_version(int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    TCRLockGuard guard(lock_);
    for (int64_t i = 0; i < max_cached_num_; i++) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      ObSchemaMgr *schema_mgr = schema_mgr_item.schema_mgr_;
      if (OB_ISNULL(schema_mgr)) {
        // do-nothing
      } else if (ATOMIC_LOAD(&schema_mgr_item.ref_cnt_) > 0
                 && (OB_INVALID_VERSION == schema_version
                     || schema_mgr->get_schema_version() < schema_version)) {
        schema_version = schema_mgr->get_schema_version();
      }
    }
    if (OB_INVALID_VERSION == schema_version) {
      // No reference version, take the largest schema_version that has been constructed
      ObSchemaMgr *latest_schema_mgr = schema_mgr_items_[latest_schema_idx_].schema_mgr_;
      if (OB_NOT_NULL(latest_schema_mgr)) {
        schema_version = latest_schema_mgr->get_schema_version();
      }
    }
  }
  return ret;
}

static const char* ref_info_type_strs[] = {
  "STACK",
  "VTABLE_SCAN_PARAM",
  "INNER_SQL_RESULT",
  "LOAD_DATA_IMPL",
  "PX_TASK_PROCESSS",
  "REMOTE_EXE",
  "CACHED_GUARD",
  "UNIQ_CHECK",
  "SSTABLE_SPLIT_CTX",
  "RELATIVE_TABLE",
  "VIRTUAL_TABLE",
  "DAS_CTX",
  "SCHEMA_RECORDER",
  "SPI_RESULT_SET",
  "PL_PREPARE_RESULT",
  "PARTITION_BALANCE",
  "RS_MAJOR_CHECK"
};

int ObSchemaMgrCache::get_ref_info_type_str_(const int64_t &index, const char *&type_str) {
  STATIC_ASSERT(ARRAYSIZEOF(ref_info_type_strs) == (int64_t)ObSchemaMgrItem::Mod::MOD_MAX,
                "type string array size mismatch with enum Mod count");
  int ret = OB_SUCCESS;
  int type_str_len = ARRAYSIZEOF(ref_info_type_strs);
  if (index >= type_str_len) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("index is out of range", KR(ret), K(type_str_len));
  } else {
    type_str = ref_info_type_strs[index];
  }
  return ret;
}

int ObSchemaMgrCache::build_ref_mod_infos_(const int64_t *mod_ref,
                                          char *&buff, const int64_t &buf_len,
                                          common::ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  MEMSET(buff, 0, buf_len);

  if (OB_ISNULL(mod_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mod ref is NULL", KR(ret));
  }
  int64_t pos = 0;
  const char *type_str = NULL;

  for (int64_t i = 0; i < ARRAYSIZEOF(ref_info_type_strs) && OB_SUCC(ret); ++i) {
    if (mod_ref[i] > 0) {
      if (OB_FAIL(get_ref_info_type_str_(i, type_str))) {
        LOG_WARN("fail to get ref info type str", KR(ret));
      } else if (OB_FAIL(databuff_printf(buff, buf_len, pos, "%s%s:%ld", (0 != pos ? "," : ""), type_str, mod_ref[i]))) {
        LOG_WARN("fail to fail to databuff printf tmp_buff", KR(ret), K(type_str), K(mod_ref[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int32_t str_len = static_cast<int32_t>(pos);
    if (0 != str_len) {
      str.assign(buff, str_len);
    }
  }
  return ret;
}

int ObSchemaMgrCache::get_slot_info(common::ObIAllocator &allocator, common::ObIArray<ObSchemaSlot> &schema_slot_infos)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    int64_t *mod_ref = NULL;
    ObSchemaMgr *schema_mgr = NULL;
    int64_t cached_slot_num = OB_INVALID_COUNT;
    int64_t slot_id = OB_INVALID_INDEX;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t schema_version = OB_INVALID_VERSION;
    int64_t schema_count = OB_INVALID_COUNT;
    int64_t ref_cnt = OB_INVALID_COUNT;
    int64_t allocator_idx = OB_INVALID_INDEX;
    ObSchemaSlot schema_slot;
    ObString tmp_str;
    ObString ref_infos;
    // alloc 4096 in advance to organize ref_infos and reuse it
    int64_t buf_len = OB_MAX_SCHEMA_REF_INFO;
    char *tmp_buff = static_cast<char*>(allocator.alloc(buf_len));
    if (OB_ISNULL(tmp_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc tmp_buff faild", KR(ret));
    } else {
      TCRLockGuard guard(lock_);
      cached_slot_num = max_cached_num_;
      for (int64_t i = 0; i < cached_slot_num && OB_SUCC(ret); ++i) {
        schema_mgr = schema_mgr_items_[i].schema_mgr_;
        if (OB_NOT_NULL(schema_mgr)) {
          ref_infos.reset();
          tmp_str.reset();
          slot_id = i;
          tenant_id = schema_mgr->get_tenant_id();
          schema_version = schema_mgr->get_schema_version();
          allocator_idx = schema_mgr->get_allocator_idx();
          ref_cnt = schema_mgr_items_[i].ref_cnt_;
          mod_ref = schema_mgr_items_[i].mod_ref_cnt_;
          if (OB_FAIL(schema_mgr->get_schema_count(schema_count))) {
            LOG_WARN("fail to get schema count", KR(ret), K(tenant_id), K(schema_version));
          } else if (0 == ref_cnt) {
            //do nothing
          } else if (OB_ISNULL(mod_ref)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant slot ref_cnt size > 0 but mod ref array is NULL", KR(ret), K(tenant_id),
                    K(slot_id), K(schema_version));
          } else if (OB_FAIL(build_ref_mod_infos_(mod_ref, tmp_buff, buf_len, tmp_str))) {
            LOG_WARN("fail to build mode_ref_cnt to string", KR(ret), K(tenant_id), K(schema_version));
          //deep copy string
          } else if (OB_FAIL(ob_write_string(allocator, tmp_str, ref_infos))) {
            LOG_WARN("set mod_ref_infos string faild", K(tmp_str));
          }
          if (OB_SUCC(ret)) {
            schema_slot.init(tenant_id, slot_id, schema_version,
                              schema_count, ref_cnt, ref_infos, allocator_idx);
            if (OB_FAIL(schema_slot_infos.push_back(schema_slot))) {
              LOG_WARN("push back to schema_slot_infos failed", KR(ret), K(tenant_id), K(schema_version));
            }
          }
        }//OB_NOT_NULL(schema_mgr)
      }//for
      if (OB_NOT_NULL(tmp_buff)) {
        allocator.free(tmp_buff);
      }
    }

  }
  return ret;
}

// in:
//   schema_mgr : mgr that will put
//   eli_schema_mgr : Eliminate the mgr
//   handle : Return the put in mgr
int ObSchemaMgrCache::put(ObSchemaMgr *schema_mgr,
                          ObSchemaMgr *&eli_schema_mgr,
                          ObSchemaMgrHandle *handle/*=NULL*/)
{
  int ret = OB_SUCCESS;
  eli_schema_mgr = NULL;
  if (NULL != handle) {
    handle->reset();
  }

  LOG_INFO("put schema mgr",
           "schema version", NULL != schema_mgr ?
           schema_mgr->get_schema_version() : OB_INVALID_VERSION);
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_ISNULL(schema_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(schema_mgr));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == schema_mgr->get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), "tenant_id", schema_mgr->get_tenant_id());
  } else {
    ObSchemaMgrItem *dst_item = NULL;
    bool is_stop = false;
    const uint64_t tenant_id = schema_mgr->get_tenant_id();
    int64_t max_schema_slot_num = max_cached_num_;
    if (!ObSchemaService::g_liboblog_mode_) {
      omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
      if (tenant_config.is_valid()) {
        max_schema_slot_num = tenant_config->_max_schema_slot_num;
      }
    }
    TCWLockGuard guard(lock_);
    // 1. In order to avoid the repeated adjustment of the configuration item _max_schema_slot_num that may cause problems
    //  that may be caused by the invisible version in the history, max_cached_num_ can only be increased during
    //  the operation of the observer. The memory release frequency of the schema mgr is controlled by _max_schema_slot_num.
    //  The user can reduce the _max_schema_slot_num to speed up the release of the schema mgr memory.
    // 2. Because liboblog and agentserver cannot perceive ob configuration items, they still use startup
    //  settings to control the number of schema slots.
    // 3. The fallback mode has fewer usage scenarios in the OB and has nothing to do with the number of concurrent users,
    //  and the schema_mgr memory management strategy is different from the schema refresh scenario.
    //  In order to reduce unnecessary memory usage, a fixed number of 16 slots is also used.
    if (!ObSchemaService::g_liboblog_mode_ && FALLBACK != mode_) {
      max_cached_num_ = max(max_cached_num_, max_schema_slot_num);
    }
    int64_t target_pos = -1;
    for (int64_t i = 0; i < max_cached_num_ && !is_stop; ++i) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      ObSchemaMgr *tmp_schema_mgr = schema_mgr_item.schema_mgr_;
      if (NULL == tmp_schema_mgr) {
        dst_item = &schema_mgr_item;
        target_pos = i;
        is_stop = true;
      } else if (ATOMIC_LOAD(&schema_mgr_item.ref_cnt_) > 0) {
        // do-nothing
      } else {
        if (NULL == dst_item ||
            tmp_schema_mgr->get_schema_version() < dst_item->schema_mgr_->get_schema_version()) {
          dst_item = &schema_mgr_item;
          target_pos = i;
        }
      }
    }
    if (NULL == dst_item) {
      ret = OB_EAGAIN;
      LOG_WARN("need retry", K(ret));
      for (int64_t i = 0; i < max_cached_num_; ++i) {
        const ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
        const ObSchemaMgr *schema_mgr = schema_mgr_item.schema_mgr_;
        if (OB_NOT_NULL(schema_mgr)) {
          uint64_t tenant_id = schema_mgr->get_tenant_id();
          uint64_t schema_version = schema_mgr->get_schema_version();
          LOG_INFO("schema_mgr_item", "i", i, K(ret), K(tenant_id),
                   K(schema_version), K(schema_mgr),
                   "ref_cnt", schema_mgr_item.ref_cnt_,
                   "mod_ref_cnt", ObArrayWrap<int64_t>(schema_mgr_item.mod_ref_cnt_,
                                                       ObSchemaMgrItem::MOD_MAX));
        }
      }
    } else {
      eli_schema_mgr = dst_item->schema_mgr_;
      schema_mgr->set_timestamp_in_slot(ObClockGenerator::getClock());
      dst_item->schema_mgr_ = schema_mgr;
      uint64_t tenant_id = schema_mgr->get_tenant_id();
      int64_t dst_timestamp = schema_mgr->get_timestamp_in_slot();
      int64_t dst_schema_version = schema_mgr->get_schema_version();
      LOG_INFO("dst schema mgr item ptr", K(tenant_id), K(dst_item),
               K(dst_timestamp), K(dst_schema_version), K(target_pos));
      (void)ATOMIC_STORE(&dst_item->ref_cnt_, 0);
      for (int64_t i = 0; i < ObSchemaMgrItem::MOD_MAX; i++) {
        (void)ATOMIC_STORE(&dst_item->mod_ref_cnt_[i], 0);
      }
      if (NULL != handle) {
        (void)ATOMIC_FAA(&dst_item->ref_cnt_, 1);
        (void)ATOMIC_FAA(&dst_item->mod_ref_cnt_[handle->mod_], 1);
        handle->schema_mgr_item_ = dst_item;
      }
      if (OB_NOT_NULL(eli_schema_mgr)) {
        cur_cached_num_++;
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = try_update_latest_schema_idx())) {
          LOG_WARN("fail to update latest schema idx", K(tmp_ret));
        }
      }
    }
  }

  return ret;
}

int ObSchemaMgrCache::try_gc_tenant_schema_mgr(ObSchemaMgr *&eli_schema_mgr)
{
  int ret = OB_SUCCESS;
  eli_schema_mgr = NULL;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    bool is_stop = false;
    // max_cached_num_ only increases without decreasing, you can only look at max_cached_num_ when releasing,
    // instead of iterating MAX_SCHEMA_SLOT_NUM slots
    for (int64_t i = 0; i < max_cached_num_ && !is_stop; ++i) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      ObSchemaMgr *tmp_schema_mgr = schema_mgr_item.schema_mgr_;
      if (NULL == tmp_schema_mgr) {
        // do-nothing
      } else if (ATOMIC_LOAD(&schema_mgr_item.ref_cnt_) > 0) {
        // do-nothing
      } else {
        eli_schema_mgr = tmp_schema_mgr;
        schema_mgr_item.schema_mgr_ = NULL;
        (void)ATOMIC_STORE(&schema_mgr_item.ref_cnt_, 0);
        for (int64_t i = 0; i < ObSchemaMgrItem::MOD_MAX; i++) {
          (void)ATOMIC_STORE(&schema_mgr_item.mod_ref_cnt_[i], 0);
        }
        is_stop = true;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(eli_schema_mgr)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_update_latest_schema_idx())) {
        LOG_WARN("fail to update latest schema idx", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSchemaMgrCache::try_eliminate_schema_mgr(ObSchemaMgr *&eli_schema_mgr)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(eli_schema_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("eli_schema_mgr is null", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    bool found = false;
    // max_cached_num_ only increases without decreasing, you can only look at max_cached_num_ when releasing,
    // instead of iterating MAX_SCHEMA_SLOT_NUM slots
    for (int64_t i = 0; i < max_cached_num_ && !found; ++i) {
      ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      ObSchemaMgr *tmp_schema_mgr = schema_mgr_item.schema_mgr_;
      if (NULL == tmp_schema_mgr) {
        // do-nothing
      } else if (eli_schema_mgr != tmp_schema_mgr) {
      } else if (ATOMIC_LOAD(&schema_mgr_item.ref_cnt_) > 0) {
        ret = OB_EAGAIN;
        uint64_t tenant_id = tmp_schema_mgr->get_tenant_id();
        int64_t ref_cnt = ATOMIC_LOAD(&schema_mgr_item.ref_cnt_);
        int64_t timestamp = tmp_schema_mgr->get_timestamp_in_slot();
        int64_t schema_version = tmp_schema_mgr->get_schema_version();
        LOG_WARN("schema mgr is in use, try eliminate later", KR(ret), K(tenant_id),
                 K(ref_cnt), K(schema_version), K(timestamp));
      } else {
        eli_schema_mgr = tmp_schema_mgr;
        schema_mgr_item.schema_mgr_ = NULL;
        (void)ATOMIC_STORE(&schema_mgr_item.ref_cnt_, 0);
        for (int64_t i = 0; i < ObSchemaMgrItem::MOD_MAX; i++) {
          (void)ATOMIC_STORE(&schema_mgr_item.mod_ref_cnt_[i], 0);
        }
        found = true;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(eli_schema_mgr)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_update_latest_schema_idx())) {
        LOG_WARN("fail to update latest schema idx", K(tmp_ret));
      }
    }
  }
  return ret;
}

void ObSchemaMgrCache::dump() const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    TCRLockGuard guard(lock_);
    int64_t total_count = 0;
    int64_t total_size = 0;
    for (int64_t i = 0; i < max_cached_num_; ++i) {
      const ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      const ObSchemaMgr *schema_mgr = schema_mgr_item.schema_mgr_;
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t schema_version = OB_INVALID_VERSION;
      int64_t timestamp_in_slot = 0;
      int64_t schema_count = 0;
      int64_t schema_size = 0;
      if (OB_NOT_NULL(schema_mgr)) {
        int tmp_ret = OB_SUCCESS;
        tmp_ret = schema_mgr->get_schema_count(schema_count);
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        tmp_ret = schema_mgr->get_schema_size(schema_size);
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        tenant_id = schema_mgr->get_tenant_id();
        schema_version = schema_mgr->get_schema_version();
        timestamp_in_slot = schema_mgr->get_timestamp_in_slot();
        total_count += schema_count;
        total_size += schema_size;
        FLOG_INFO("[SCHEMA_STATISTICS] dump schema_mgr_item", "i", i, K(ret),
                  K(tenant_id), K(schema_version), K(schema_count),
                  K(schema_size), K(timestamp_in_slot),
                  "ref_cnt", schema_mgr_item.ref_cnt_,
                  "mod_ref_cnt", ObArrayWrap<int64_t>(schema_mgr_item.mod_ref_cnt_,
                                                     ObSchemaMgrItem::MOD_MAX));
      }
    }
    FLOG_INFO("[SCHEMA_STATISTICS] dump schema_mgr_cache", K(ret), K(total_count), K(total_size));
  }
}

// need process in wlock
int ObSchemaMgrCache::try_update_latest_schema_idx()
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; i < max_cached_num_; ++i) {
      const ObSchemaMgrItem &schema_mgr_item = schema_mgr_items_[i];
      const ObSchemaMgr *schema_mgr = schema_mgr_item.schema_mgr_;
      if (OB_ISNULL(schema_mgr)) {
        // skip
      } else if (OB_INVALID_INDEX == idx) {
        idx = i;
      } else if (OB_NOT_NULL(schema_mgr_items_[idx].schema_mgr_)) {
        const ObSchemaMgr *last_schema_mgr = schema_mgr_items_[idx].schema_mgr_;
        if (last_schema_mgr->get_schema_version() < schema_mgr->get_schema_version()) {
          idx = i;
        }
      }
    }
    if (OB_INVALID_INDEX != idx) {
      latest_schema_idx_ = idx;
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
