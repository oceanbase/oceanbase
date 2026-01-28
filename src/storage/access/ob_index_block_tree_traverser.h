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

#ifndef OCEANBASE_STORAGE_ACCESS_OB_INDEX_BLOCK_TREE_TRAVERSER_H
#define OCEANBASE_STORAGE_ACCESS_OB_INDEX_BLOCK_TREE_TRAVERSER_H

#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_micro_block_handle_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObPartitionEst;
class ObTablet;

/**
 * @brief Interface for multi-range estimate context during index block tree traversal
 */
class ObIIndexTreeTraverserContext
{
public:
  /**
   * @brief Operation types that can be performed during the traversal of an index block tree.
   */
  enum ObTraverserOperationType : uint8_t
  {
    NOTHING,          ///< No operation is needed
    GOTO_NEXT_LEVEL,  ///< Continue traversal by going to next level
  };

  ObIIndexTreeTraverserContext(const bool is_never_estimate = false,
                               const bool consider_multi_version = false)
      : is_never_estimate_(is_never_estimate), consider_multi_version_(consider_multi_version)
  {
  }

  virtual ~ObIIndexTreeTraverserContext() = default;

  virtual bool is_valid() const = 0;

  virtual bool is_ended() const = 0;

  virtual const ObDatumRange &get_curr_range() const = 0;

  OB_INLINE bool is_never_estimate() const { return is_never_estimate_; }

  OB_INLINE bool consider_multi_version() const { return consider_multi_version_; }

  /**
   * @brief Switch to next range. This method is called by tree traverser.
   */
  virtual int next_range() = 0;

  /**
   * @brief Callback function when a leaf node is during traversal
   */
  virtual int on_leaf_node(const ObMicroIndexInfo &index_row, const ObPartitionEst &est) = 0;

  /**
   * @brief Callback function when a inner node is during traversal
   */
  virtual int on_inner_node(const ObMicroIndexInfo &index_row,
                            const bool is_coverd_by_range,
                            ObTraverserOperationType &opertion) = 0;

  /**
   * @brief Callback function when we cann't goto next level because of performance
   */
  virtual int on_node_estimate(const ObMicroIndexInfo &index_row, const bool is_coverd_by_range) = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  bool is_never_estimate_;
  bool consider_multi_version_;
};

/**
 * @brief Traverser for index block tree
 */
class ObIndexBlockTreeTraverser
{
public:
  // If the row count of data micro block is larger than below limit,
  // we should open this micro block to get more precise estimate result
  static constexpr int64_t OPEN_DATA_MICRO_BLOCK_ROW_LIMIT = 65536;

  ObIndexBlockTreeTraverser()
      : tablet_(nullptr), path_caches_(allocator_), is_inited_(false)
  {
  }

  int init(ObSSTable &sstable, const ObITableReadInfo &index_read_info, ObTablet *tablet = nullptr, const ObLabel &label = "ObPartSpli");

  void reuse();

  int traverse(ObIIndexTreeTraverserContext &context);

  OB_INLINE bool is_inited() const { return is_inited_; }

  OB_INLINE const ObSSTable *get_sstable() const { return sstable_; }

  OB_INLINE ObTablet *get_tablet() const { return tablet_; }

  OB_INLINE const ObITableReadInfo *get_read_info() const { return read_info_; }

  OB_INLINE ObIAllocator &get_allocator() { return allocator_; }

  TO_STRING_KV(KPC_(read_info), KPC_(context), KPC_(sstable));

private:
  class TreeNodeContext
  {
  public:
    int init(const ObMicroBlockData &block_data,
             const ObMicroIndexInfo *micro_index_info,
             ObIndexBlockTreeTraverser &traverser);

    int open(const ObMicroBlockData &block_data,
             const ObMicroIndexInfo *micro_index_info,
             const ObIndexBlockTreeTraverser &traverser);

    void reuse();

    int locate_range(const ObDatumRange &range, int64_t &index_row_count);

    int get_next_index_row(ObMicroIndexInfo &idx_block_row);
  private:
    ObIndexBlockRowScanner scanner_;
  };

  struct PathInfo
  {
    PathInfo() : path_count_(0), left_most_(false), right_most_(false), in_middle_(false) {}
    PathInfo(const int64_t path_count, const int64_t path_idx, const PathInfo &father_path_info)
        : path_count_(path_count), left_most_(path_idx == 0 && (!father_path_info.valid() || father_path_info.left_most_)),
          right_most_(path_idx == path_count - 1 && (!father_path_info.valid() || father_path_info.right_most_)),
          in_middle_(!left_most_ && !right_most_)
    {
    }

    OB_INLINE bool valid() const { return path_count_ != 0; }
    OB_INLINE void reset() { *this = PathInfo(); }

    TO_STRING_KV(K_(path_count), K_(left_most), K_(right_most), K_(in_middle));

    int64_t path_count_;
    bool left_most_;
    bool right_most_;
    bool in_middle_;
  };

  class PathNodeCaches
  {
  public:
    constexpr static int64_t DEFAULT_BUFFER_CACHE_LEVEL = 8;

    PathNodeCaches(ObFIFOAllocator &allocator) : allocator_(allocator), is_inited_(false) {}

    ~PathNodeCaches()
    {
      for (int64_t i = DEFAULT_BUFFER_CACHE_LEVEL; i < caches_.count(); i++) {
        caches_[i]->~CacheNode();
        allocator_.free(caches_[i]);
      }
    }

    struct CacheNode
    {
      CacheNode() : is_inited_(false) {}

      void reuse()
      {
        context_.reuse();
      }

      TO_STRING_KV(K_(key), K_(micro_handle), K_(micro_data), K_(is_inited));

      ObMicroBlockCacheKey key_;
      ObMicroBlockDataHandle micro_handle_;
      ObMicroBlockData micro_data_;
      ObMacroBlockReader macro_block_reader_;
      TreeNodeContext context_;
      bool is_inited_;
    };

    int init();

    int load_root(const ObMicroBlockData &block_data, ObIndexBlockTreeTraverser &traverser);

    int find(const int64_t level,
            const ObMicroIndexInfo *micro_index_info,
            bool &is_in_cache);

    int get(const int64_t level,
            const ObMicroIndexInfo *micro_index_info,
            ObIndexBlockTreeTraverser &traverser,
            CacheNode *&cache_node,
            bool &is_in_cache);

    int prefetch(const ObMicroIndexInfo &micro_index_info, ObMicroBlockDataHandle &micro_handle);

    TO_STRING_KV(K_(caches), K_(is_inited));

  private:
    OB_INLINE int ensure_safe_access(const int64_t expect_access_idx)
    {
      return expect_access_idx < caches_.count() ? OB_SUCCESS
                                                 : build_and_push_new_cache(expect_access_idx);
    }

    int build_and_push_new_cache(const int64_t expect_access_idx);

    common::ObFIFOAllocator &allocator_;
    CacheNode buffer_caches_[DEFAULT_BUFFER_CACHE_LEVEL];
    ObSEArray<CacheNode *, DEFAULT_BUFFER_CACHE_LEVEL> caches_;
    bool is_inited_;
  };

  int inner_node_traverse(const ObMicroIndexInfo *micro_index_info,
                          const PathInfo &path_info,
                          const int64_t level);

  int leaf_node_traverse(const ObMicroIndexInfo &micro_index_info,
                         const PathInfo &path_info,
                         const int64_t level);

  int goto_next_level_node(const ObMicroIndexInfo &micro_index_info,
                           const bool is_coverd_by_range,
                           const PathInfo &path_info,
                           const int64_t level);

  int handle_overflow_ranges();

  int is_range_cover_node_end_key(const ObDatumRange &range,
                                  const ObMicroIndexInfo &micro_index_info,
                                  bool &is_over);

  ObFIFOAllocator allocator_;
  ObSSTable *sstable_;
  const ObITableReadInfo *read_info_;
  ObIIndexTreeTraverserContext *context_;
  ObTablet *tablet_;

  PathNodeCaches path_caches_;

  // statistics
  int64_t visited_node_count_;
  int64_t visited_macro_node_count_;

  bool is_inited_;
};

}
}
#endif /* OCEANBASE_STORAGE_ACCESS_OB_INDEX_BLOCK_TREE_TRAVERSER_H */
