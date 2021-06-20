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

#ifndef OB_MACRO_BLOCK_META_MGR_H_
#define OB_MACRO_BLOCK_META_MGR_H_
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/lock/ob_latch.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/task/ob_timer.h"
#include "slog/ob_base_storage_logger.h"
#include "ob_block_sstable_struct.h"

namespace oceanbase {
namespace blocksstable {
enum ObRedoLogSubType {
  CHANGE_MACRO_BLOCK_META = 1,
};

struct ObMacroBlockMetaCtrl {
  ObMacroBlockMetaCtrl() : meta_(NULL), ref_cnt_(0)
  {}
  void reset()
  {
    meta_ = NULL;
    ref_cnt_ = 0;
  }
  const ObMacroBlockMeta* meta_;
  int64_t ref_cnt_;
};

struct ObMacroBlockMetaNode {
  ObMacroBlockMetaNode() : macro_id_(), meta_ctrl_(NULL), next_(NULL)
  {}
  void reset()
  {
    macro_id_.reset();
    meta_ctrl_ = NULL;
    next_ = NULL;
  }
  MacroBlockId macro_id_;
  ObMacroBlockMetaCtrl* meta_ctrl_;
  ObMacroBlockMetaNode* next_;
};

struct ObMajorMacroBlockKey {
  ObMajorMacroBlockKey()
  {
    reset();
  }
  bool is_valid() const
  {
    return table_id_ > 0 && partition_id_ >= 0 && data_version_ > 0 && data_seq_ >= 0;
  }
  uint64_t hash() const;
  void reset();
  bool operator==(const ObMajorMacroBlockKey& key) const;
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(data_version), K_(data_seq));

  uint64_t table_id_;
  int64_t partition_id_;
  int64_t data_version_;
  int64_t data_seq_;
};

class ObMacroBlockMetaMgr;
class ObMacroBlockMetaHandle {
public:
  ObMacroBlockMetaHandle();
  virtual ~ObMacroBlockMetaHandle();
  ObMacroBlockMetaHandle(const ObMacroBlockMetaHandle& other);
  ObMacroBlockMetaHandle& operator=(const ObMacroBlockMetaHandle& other);
  void reset();
  OB_INLINE const ObMacroBlockMeta* get_meta()
  {
    return NULL == meta_ctrl_ ? NULL : meta_ctrl_->meta_;
  }
  TO_STRING_KV(KP_(meta_ctrl));

private:
  friend class ObMacroBlockMetaMgr;
  ObMacroBlockMetaCtrl* meta_ctrl_;
};

struct ObMacroBlockMetaLogEntry : public ObIBaseStorageLogEntry {
  int64_t data_file_id_;
  int64_t disk_no_;
  int64_t block_index_;
  ObMacroBlockMeta& meta_ptr_;
  static const int64_t MACRO_BLOCK_META_LOG_VERSION = 1;
  ObMacroBlockMetaLogEntry(int64_t id, int64_t dn, int64_t bi, ObMacroBlockMeta& meta)
      : data_file_id_(id), disk_no_(dn), block_index_(bi), meta_ptr_(meta)
  {}
  bool is_valid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  OB_UNIS_VERSION_V(MACRO_BLOCK_META_LOG_VERSION);
};

class ObMacroBlockMetaMgrGCTask : public common::ObTimerTask {
public:
  ObMacroBlockMetaMgrGCTask();
  virtual ~ObMacroBlockMetaMgrGCTask();
  virtual void runTimerTask();
};

class ObMacroBlockMetaMgr : public ObIRedoModule {
public:
  typedef common::hash::ObHashMap<ObMajorMacroBlockKey, MacroBlockId, common::hash::ReadWriteDefendMode> MajorKeyMap;
  static ObMacroBlockMetaMgr& get_instance();
  int init(const int64_t max_block_cnt);
  void destroy();
  int get_old_meta(const MacroBlockId macro_id, ObMacroBlockMetaHandle& meta_handle);
  // replay the redo log.
  virtual int replay(const ObRedoModuleReplayParam& param) override;
  // parse the redo log to stream
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;
  virtual int enable_write_log() override;

private:
  friend class ObMacroBlockMetaHandle;
  friend class ObMacroBlockMetaMgrGCTask;
  friend class ObMacroMetaBlockReader;
  ObMacroBlockMetaMgr();
  virtual ~ObMacroBlockMetaMgr();
  int init_buckets(const int64_t buckets_cnt);
  int init_node_queue(const int64_t node_cnt);
  int init_ctrl_queue(const int64_t ctrl_cnt);
  void gc();
  void inc_ref(ObMacroBlockMetaCtrl& meta_ctrl);
  void dec_ref(ObMacroBlockMetaCtrl& meta_ctrl);
  int set_meta(const MacroBlockId macro_id, const ObMacroBlockMeta& meta);
  int erase_meta(const MacroBlockId macro_id);
  static const int64_t GC_DELAY_US = 3600LL * 1000LL * 1000LL;  // 1 hour
  int64_t max_block_cnt_;
  uint64_t buckets_cnt_;
  ObMacroBlockMetaNode** meta_buckets_;
  common::ObBucketLock buckets_lock_;
  common::ObArenaAllocator allocator_;
  common::ObLfFIFOAllocator meta_allocator_;
  common::ObFixedQueue<ObMacroBlockMetaNode> node_queue_;
  common::ObFixedQueue<ObMacroBlockMetaCtrl> ctrl_queue_;
  ObMacroBlockMetaMgrGCTask gc_task_;
  bool is_inited_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MACRO_BLOCK_META_MGR_H_ */
