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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_SHARED_MACRO_BLOCK_MANAGER
#define OCEANBASE_BLOCKSSTABLE_OB_SHARED_MACRO_BLOCK_MANAGER

#include "storage/blocksstable/ob_macro_block_handle.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "lib/task/ob_timer.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace blocksstable
{
struct ObMacroBlocksWriteCtx;
class ObSSTableIndexBuilder;
class ObIndexBlockRebuilder;
class ObSSTableSecMetaIterator;
class ObSSTableMergeRes;
struct ObSSTableBasicMeta;
struct ObBlockInfo
{
public:
  ObBlockInfo()
    : nested_size_(OB_DEFAULT_MACRO_BLOCK_SIZE), nested_offset_(0), macro_id_()
  {
  }
  ~ObBlockInfo();
  void reset();
  bool is_valid() const;
  bool is_small_sstable() const;
  TO_STRING_KV(K_(nested_size), K_(nested_offset), K_(macro_id));
public:
  int64_t nested_size_;
  int64_t nested_offset_;
  MacroBlockId macro_id_;
};

// set it as a member variable of t3m in 4.1
class ObSharedMacroBlockMgr final
{
public:
  ObSharedMacroBlockMgr();
  ~ObSharedMacroBlockMgr();
  void destroy();
  int init();
  int start();
  void stop();
  void wait();
  int64_t get_shared_block_cnt();
  void get_cur_shared_block(MacroBlockId &macro_id);
  int write_block(const char* buf, const int64_t size, ObBlockInfo &block_info, ObMacroBlocksWriteCtx &write_ctx);
  int add_block(const MacroBlockId &block_id, const int64_t block_size);
  int free_block(const MacroBlockId &block_id, const int64_t block_size);

  TO_STRING_KV(K_(macro_handle), K_(offset), K_(header_size));

  static int mtl_init(ObSharedMacroBlockMgr* &shared_block_mgr);

private:
  class ObBlockDefragmentationTask : public common::ObTimerTask
  {
  public:
    explicit ObBlockDefragmentationTask(ObSharedMacroBlockMgr &shared_mgr)
        : shared_mgr_(shared_mgr)
    {
      disable_timeout_check();
    }
    virtual ~ObBlockDefragmentationTask() = default;
    virtual void runTimerTask() override;

  private:
    ObSharedMacroBlockMgr &shared_mgr_;
  };

  struct GetSmallBlockOp
  {
  public:
    GetSmallBlockOp(ObIArray<MacroBlockId> &block_ids, ObIArray<MacroBlockId> &unused_block_ids)
        : block_ids(block_ids), unused_block_ids(unused_block_ids), execution_ret_(OB_SUCCESS) {}
    bool operator()(const MacroBlockId &id, const int32_t used_size)
    {
      int ret = OB_SUCCESS;
      bool bool_ret = true;
      bool is_free = false;
      if (OB_UNLIKELY(MAX_RECYCLABLE_BLOCK_CNT == block_ids.count())) {
        bool_ret = false;
        execution_ret_ = OB_ITER_END;
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.check_macro_block_free(id, is_free))) {
        STORAGE_LOG(WARN, "fail to check macro block free", K(ret), K(id));
      } else if (is_free && unused_block_ids.count() < MAX_RECYCLABLE_BLOCK_CNT) {
        if (OB_FAIL(unused_block_ids.push_back(id))) {
          STORAGE_LOG(WARN, "fail to push unused block id", K(ret), K(id));
        } else {
          bool_ret = true;
        }
      } else if (used_size > 0 && used_size < RECYCLABLE_BLOCK_SIZE) {
        if (OB_FAIL(block_ids.push_back(id))) {
          STORAGE_LOG(WARN, "fail to get small block", K(ret), K(id));
        } else {
          bool_ret = true;
        }
      }
      if (OB_FAIL(ret)) {
        bool_ret = false;
      }
      return bool_ret;
    }
    int64_t get_execution_ret() { return execution_ret_; }
  private:
    ObIArray<MacroBlockId> &block_ids;
    ObIArray<MacroBlockId> &unused_block_ids;
    int64_t execution_ret_; // if the number of recyclable blocks reaches 1000, set it to OB_ITER_END
  };

private:
  int defragment();
  int get_recyclable_blocks(common::ObIAllocator &allocator, ObIArray<MacroBlockId> &block_ids);
  int update_tablet(
      const ObTabletHandle &tablet_handle,
      const ObIArray<MacroBlockId> &macro_ids,
      int64_t &rewrite_cnt,
      ObSSTableIndexBuilder &sstable_index_builder,
      ObIndexBlockRebuilder &index_block_rebuilder);
  int rebuild_sstable(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObSSTable &old_sstable,
      const uint64_t data_version,
      ObSSTableIndexBuilder &sstable_index_builder,
      ObIndexBlockRebuilder &index_block_rebuilder,
      ObSSTable &new_sstable);
  int prepare_data_desc(
      const ObTablet &tablet,
      const ObSSTableBasicMeta &basic_meta,
      const ObMergeType &merge_type,
      const int64_t snapshot_version,
      const int64_t cluster_version,
      ObDataStoreDesc &data_desc) const;
  int alloc_for_tools(
      common::ObIAllocator &allocator,
      ObSSTableIndexBuilder *&sstable_index_builder,
      ObIndexBlockRebuilder *&index_block_rebuilder);
  int read_sstable_block(
      const ObSSTable &sstable,
      ObMacroBlockHandle &block_handle);
  int create_new_sstable(
      common::ObArenaAllocator &allocator,
      const ObSSTableMergeRes &res,
      const ObSSTable &old_table,
      const ObBlockInfo &block_info,
      ObSSTable &new_sstable) const;
  int parse_merge_type(const ObSSTable &sstable, ObMergeType &merge_type) const;
  int try_switch_macro_block();
  int check_write_complete(const MacroBlockId &macro_id, const int64_t macro_size);
  int do_write_block(const ObMacroBlockWriteInfo &write_info, ObBlockInfo &block_info);
  DISALLOW_COPY_AND_ASSIGN(ObSharedMacroBlockMgr);

private:
  const static int64_t DEFRAGMENT_DELAY_US = 30 * 1000 * 1000; // 30s
  const static int64_t SMALL_SSTABLE_STHRESHOLD_SIZE = 1L << 20; // 1MB
  const static int64_t RECYCLABLE_BLOCK_SIZE = (2L << 20) * 3 / 10; // 2MB * 30%
  const static int64_t MAX_RECYCLABLE_BLOCK_CNT = 1000; // return at most 1k blocks once to avoid timeout
  const static int64_t FAILURE_COUNT_INTERVAL = 10;

private:
  ObMacroBlockHandle macro_handle_;
  int64_t offset_;
  char *common_header_buf_;
  int64_t header_size_;
  lib::ObMutex mutex_;
  lib::ObMutex blocks_mutex_; // protect block_used_size_
  ObLinearHashMap<MacroBlockId, int32_t> block_used_size_;
  ObBlockDefragmentationTask defragmentation_task_;
  int tg_id_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif
