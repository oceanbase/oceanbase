 /* Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_kvcache_pointer_swizzle.h"
#include "ob_kvcache_inst_map.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/blocksstable/ob_micro_block_cache.h"

namespace oceanbase
{
namespace common
{
ObPointerSwizzleNode::ObPointerSwizzleNode()
  : node_version_({0}),
    mb_handle_(nullptr),
    value_(nullptr),
    seq_num_(0)
{}

ObPointerSwizzleNode::~ObPointerSwizzleNode()
{}

void ObPointerSwizzleNode::operator=(const ObPointerSwizzleNode &ps_node)
{
  mb_handle_ = ps_node.mb_handle_;
  value_ = ps_node.value_;
  seq_num_ = ps_node.seq_num_;
}

int ObPointerSwizzleNode::swizzle(const blocksstable::ObMicroBlockBufferHandle &handle)
{
  int ret = OB_SUCCESS;
  ObKVMemBlockHandle *mb_handle = handle.get_mb_handle();
  const blocksstable::ObMicroBlockCacheValue *value = handle.get_micro_block();
  if (nullptr == value || !value->get_block_data().is_normal_block()) {
    // We only focus on INDEX_BLOCK and DATA_BLOCK
  } else if (nullptr == mb_handle || LFU != mb_handle->policy_) {
    // Skip the action of moving memory blocks to prevent invalidating the node memory.
  } else if (nullptr != value_) {
    // The attempt to check if value_ is not nullptr (a dirty read) indicates that
    // swizzling has occurred, then skip it at this time.
  } else {
    ObPointerSwizzleGuard guard(node_version_);
    if (guard.is_multi_thd_safe() && nullptr == mb_handle_ && nullptr == value_) {
      set(mb_handle, value);
      COMMON_LOG(DEBUG, "swizzle successfully", KPC(this));
    }
  }

  return ret;
}

int ObPointerSwizzleNode::access_mem_ptr(blocksstable::ObMicroBlockBufferHandle &handle)
{
  int ret = OB_SUCCESS;
  ObPointerSwizzleNode tmp_ps_node;
  if (nullptr == value_) {
    // The attempt to check if value_ is nullptr (a dirty read) might indicate that
    // swizzling has never occurred or that it has already been unswizzled.
    ret = OB_READ_NOTHING;
  } else if (!load_node(tmp_ps_node)) {
    // The multithreaded safe loading of the node failed
    ret = OB_READ_NOTHING;
  } else if (!add_handle_ref(tmp_ps_node.mb_handle_, tmp_ps_node.seq_num_)) {
    // The memory for value_ corresponding to the node has been released;
    // the node is reset to improve efficiency, whether the node is reset or not is
    // independent of the release of memory for value_.
    unswizzle();
    ret = OB_READ_NOTHING;
  } else {
    ++tmp_ps_node.mb_handle_->recent_get_cnt_;
    ATOMIC_AAF(&tmp_ps_node.mb_handle_->get_cnt_, 1);
    tmp_ps_node.mb_handle_->inst_->status_.total_hit_cnt_.inc();
    handle.set_mb_handle(tmp_ps_node.mb_handle_);
    handle.set_micro_block(reinterpret_cast<const blocksstable::ObMicroBlockCacheValue*>(tmp_ps_node.value_));
    COMMON_LOG(DEBUG, "access the memory successfully which the swizzling pointer points to", KPC(this));
  }
  return ret;
}

void ObPointerSwizzleNode::unswizzle()
{
  ObPointerSwizzleGuard guard(node_version_);
  if (guard.is_multi_thd_safe() && (nullptr != mb_handle_ || nullptr != value_)) {
    reset();
  }
}

bool ObPointerSwizzleNode::load_node(ObPointerSwizzleNode &tmp_ps_node)
{
  bool bret = false;
  ObNodeVersion observed_version(ATOMIC_LOAD(&node_version_.value_));
  if (1 == observed_version.write_flag_) {
    // There are some threads that are writing to the current node
  } else if (FALSE_IT(tmp_ps_node = *this)) {
  } else if (ATOMIC_LOAD(&node_version_.value_) != observed_version.value_) {
    // Check if there are threads that write to the current node
  } else if (nullptr == tmp_ps_node.value_ || nullptr == tmp_ps_node.mb_handle_) {
    // Make sure that the value_ is valid
  } else {
    // There is no need to assign a value to status at this point,
    // as subsequent processes will overwrite it
    bret = true;
  }
  return bret;
}

void ObPointerSwizzleNode::reset()
{
  mb_handle_ = nullptr;
  value_ = nullptr;
  seq_num_ = 0;
}

void ObPointerSwizzleNode::set(ObKVMemBlockHandle *mb_handle, const ObIKVCacheValue *value)
{
  mb_handle_ = mb_handle;
  value_ = value;
  seq_num_ = mb_handle->get_seq_num();
}

bool ObPointerSwizzleNode::add_handle_ref(ObKVMemBlockHandle *mb_handle, const uint32_t seq_num)
{
  bool bret = false;
  if (NULL != mb_handle) {
    bret = (OB_SUCCESS == mb_handle->handle_ref_.check_seq_num_and_inc_ref_cnt(seq_num));
  }
  return bret;
}

ObPointerSwizzleNode::ObPointerSwizzleGuard::ObPointerSwizzleGuard(ObNodeVersion &cur_version)
  : cur_version_(cur_version),
    is_multi_thd_safe_(false)
{
  ObNodeVersion tmp_version(ATOMIC_LOAD(&cur_version_.value_));
  ObNodeVersion cmp_version(tmp_version.version_value_, 0);
  ObNodeVersion new_version(tmp_version.version_value_ + 1, 1);
  // Check the version number while also checking if the top bit is zero.
  if (ATOMIC_BCAS(&cur_version_.value_, cmp_version.value_, new_version.value_)) {
    is_multi_thd_safe_ = true;
  }
}

ObPointerSwizzleNode::ObPointerSwizzleGuard::~ObPointerSwizzleGuard()
{
  if (is_multi_thd_safe_) {
    // This location can be safely read because writes from other threads have been blocked.
    ObNodeVersion targ_version(cur_version_.version_value_, 0);
    ATOMIC_STORE(&cur_version_.value_, targ_version.value_);
  }
}

}//end namespace common
}//end namespace oceanbase