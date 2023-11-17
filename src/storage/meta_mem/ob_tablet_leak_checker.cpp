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

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/meta_mem/ob_tablet_leak_checker.h"

namespace oceanbase
{
namespace storage
{

ObTabletHandleIndexMap::TabletHandleFootprint::TabletHandleFootprint()
{
  MEMSET(footprint_, 0, FOOTPRINT_LEN);
}

ObTabletHandleIndexMap::TabletHandleFootprint::TabletHandleFootprint(
    const char *file, const int line, const char *func)
{
  int ret = OB_SUCCESS;
  MEMSET(footprint_, 0, FOOTPRINT_LEN);
  const int nwrite = snprintf(footprint_, FOOTPRINT_LEN, "%d, %s, %s", line, func, file);
  if (OB_UNLIKELY(nwrite < 0 || nwrite > FOOTPRINT_LEN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to write footprint info of TabletHandleFootprint", K(nwrite),
             KCSTRING(file), K(line), KCSTRING(func));
  }
}

int ObTabletHandleIndexMap::TabletHandleFootprint::assign(const TabletHandleFootprint &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const char * dest = STRNCPY(footprint_, other.footprint_, FOOTPRINT_LEN);
    if (OB_UNLIKELY(dest != footprint_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to assign footprint", K(ret), KP(dest), KP(footprint_));
    }
  }
  return ret;
}

ObTabletHandleIndexMap::ObTabletHandleIndexMap() : max_index_(0), is_inited_(false)
{
}

int ObTabletHandleIndexMap::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tb_map_.destroy())) {
    LOG_WARN("failed to destroy tb map", K(ret));
  }
  rw_lock_.destroy();
  is_inited_ = false;
  return ret;
}

int ObTabletHandleIndexMap::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletHandleIndexMap init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(tb_map_.create(REF_ARRAY_SIZE, "T3MTBMap"))) {
    LOG_WARN("failed to init tb map", K(ret));
  } else if (OB_FAIL(rw_lock_.init(lib::ObMemAttr(OB_SERVER_TENANT_ID, "T3MQSyncLock")))) {
    LOG_WARN("failed to init rw lock", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret) || OB_UNLIKELY(!is_inited_)) {
    // init failed, reset
    reset();
  }

  return ret;
}

int ObTabletHandleIndexMap::register_handle(const char *file, const int line,
                                            const char *func, int32_t &index)
{
  int ret = OB_SUCCESS;
  index = ObTabletHandleIndexMap::LEAK_CHECKER_INITIAL_INDEX;
  const int64_t cpu_id = icpu_id();
  TabletHandleFootprint thf(file, line, func);

  // Try to read existed index
  int32_t val = -1;
  ret = tb_map_.get_refactored(thf, val);
  if (ret != OB_SUCCESS && ret != OB_HASH_NOT_EXIST) {
    LOG_WARN("fail to get from tb_map_", K(ret), K(thf.footprint_), K(val));
  }

  // No existed index, fetch write lock and set index
  if (ret == OB_HASH_NOT_EXIST) {
    ObQSyncLockWriteGuard guard(rw_lock_);
    val = ATOMIC_LOAD(&max_index_);
    if (OB_FAIL(tb_map_.set_refactored(thf, val, 0 /* is_overwrite */))) {
      if (ret == OB_HASH_EXIST) { // try again
        if (OB_UNLIKELY(OB_FAIL(tb_map_.get_refactored(thf, val)))) {
          LOG_WARN("fail to re-get from tb_map_", K(ret), K(thf.footprint_), K(val));
        } else {
          // Other thread has registered TabletHandle, no need to set, do nothing
        }
      } else {
        LOG_WARN("fail to set refactored", K(ret), K(thf.footprint_), K(val));
      }
    } else {
      ATOMIC_INC(&max_index_);
    }
  }

  // Return index value with reference
  if (OB_SUCC(ret)) {
    // `tb_ref_bucket_` is divided into REF_BUCKET_SIZE buckets.
    // `cpu_id` determine the bucket, `val_ptr` determine the slot.
    index = val + (REF_ARRAY_SIZE * (murmurhash(&cpu_id, sizeof(cpu_id), 0) % REF_BUCKET_SIZE));
  } else {
    LOG_WARN("fail to assign to index", K(ret), K(val));
  }

  return ret;
}

int ObTabletHandleIndexMap::foreach(PrintToLogTraversal &op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tb_map_.foreach_refactored(op))) {
    LOG_WARN("failed to foreach in tb map", K(ret));
  }
  return ret;
}

int ObTabletHandleIndexMap::PrintToLogTraversal::operator() (
    common::hash::HashMapTypes<ObTabletHandleIndexMap::TabletHandleFootprint,
                               int32_t>::pair_type &pair)
{
  if (temporal_ref_map_[pair.second] != 0) {
    int index = pair.second;
    int ref_cnt = temporal_ref_map_[pair.second];
    char code_pos[512];
    MEMSET(code_pos, 0, sizeof(code_pos));
    int begin_at = 0;
    int len = STRLEN(pair.first.footprint_);
    for (begin_at = len - 1;
         begin_at >= 0 && pair.first.footprint_[begin_at] != '/'; --begin_at) {
    }
    int cur = 0;
    for (int i = begin_at + 1; i >= 0 && i < len; ++i) {
      code_pos[cur++] = pair.first.footprint_[i];
    }
    code_pos[cur++] = ':';
    for (int i = 0; i < len && pair.first.footprint_[i] != ','; ++i) {
      code_pos[cur++] = pair.first.footprint_[i];
    }
    LOG_INFO("There is a un-released ObTabletHandle", K(code_pos), K(ref_cnt),
             K(index), K(pair.first.footprint_));
  }
  return OB_SUCCESS;
}

ObTabletLeakChecker::ObTabletLeakChecker()
{
  MEMSET(tb_ref_bucket_, 0, sizeof(tb_ref_bucket_));
}

int ObTabletLeakChecker::inc_ref(const int32_t index)
{
  int ret = OB_SUCCESS;

  if (index == ObTabletHandleIndexMap::LEAK_CHECKER_INITIAL_INDEX) {
    // do nothing
  } else if (OB_LIKELY(index < ObTabletHandleIndexMap::REF_ARRAY_SIZE * ObTabletHandleIndexMap::REF_BUCKET_SIZE &&
                       index >= 0)) {
    if (ATOMIC_LOAD(&(tb_ref_bucket_[index])) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("error ref when inc", K(ret), K(ATOMIC_LOAD(&(tb_ref_bucket_[index]))), K(index));
    } else {
      ATOMIC_INC(&(tb_ref_bucket_[index]));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("unexpected index value", K(ret), K(index));
  }

  return ret;
}

int ObTabletLeakChecker::dec_ref(const int32_t index)
{
  int ret = OB_SUCCESS;

  if (index == ObTabletHandleIndexMap::LEAK_CHECKER_INITIAL_INDEX) {
    // do nothing
  } else if (OB_LIKELY(index < ObTabletHandleIndexMap::REF_ARRAY_SIZE * ObTabletHandleIndexMap::REF_BUCKET_SIZE &&
                       index >= 0)) {
    const int new_ref_cnt = ATOMIC_SAF(&(tb_ref_bucket_[index]), 1);
    if (new_ref_cnt < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected ref count", K(ret), K(new_ref_cnt), K(index));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("unexpected index value", K(ret), K(index));
  }

  return ret;
}

void ObTabletLeakChecker::dump_pinned_tablet_info()
{
  int ret = OB_SUCCESS;
  ObTabletHandleIndexMap::PrintToLogTraversal callback;
  // Print tb ref map to log
  for (int i = 0; i < ObTabletHandleIndexMap::REF_ARRAY_SIZE; ++i) {
    int ref_cnt = 0;
    for (int j = 0; j < ObTabletHandleIndexMap::REF_BUCKET_SIZE; ++j) {
      ref_cnt += ATOMIC_LOAD(&tb_ref_bucket_[i + j * ObTabletHandleIndexMap::REF_ARRAY_SIZE]);
    }
    callback.set_data(i, ref_cnt);
  }
  if (OB_FAIL(ObTabletHandleIndexMap::get_instance()->foreach(callback))) {
    LOG_WARN("fail to foreach on tb_map_", K(ret));
  } else {
    LOG_INFO("dump pinned tablet info finished");
  }
}

}  // namespace storage
}  // namespace oceanbase