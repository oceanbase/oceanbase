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

#ifndef OB_PARTITION_MERGE_UTIL_H_
#define OB_PARTITION_MERGE_UTIL_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fast_array.h"
#include "lib/lock/ob_mutex.h"
#include "share/schema/ob_table_schema.h"
#include "share/stat/ob_column_stat.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/ob_i_store.h"
#include "storage/ob_row_fuse.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_multiple_scan_merge.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/compaction/ob_partition_merge_builder.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "storage/ob_storage_struct.h"
#include "ob_partition_merge.h"

namespace oceanbase {
namespace storage {
struct ObSSTableMergeInfo;
class ObPartitionStorage;
struct ObSSTableMergeCtx;
class ObSSTable;
}  // namespace storage
namespace memtable {
class ObIMemtableCtxFactory;
}

namespace share {
namespace schema {
class ObColDesc;
}
}  // namespace share

namespace compaction {

struct ObMergeIndexInfo {
public:
  ObMergeIndexInfo() : index_id_(OB_INVALID_ID), table_exist_(false)
  {}
  ObMergeIndexInfo(const uint64_t index_id, const bool table_exist) : index_id_(index_id), table_exist_(table_exist)
  {}
  TO_STRING_KV(K_(index_id), K_(table_exist));
  uint64_t index_id_;
  bool table_exist_;
};

struct ObBuildIndexParam {
public:
  static const int64_t DEFAULT_INDEX_SORT_MEMORY_LIMIT = 128L * 1024L * 1024L;
  ObBuildIndexParam()
      : table_schema_(NULL),
        index_schema_(NULL),
        dep_table_schema_(NULL),
        schema_version_(0),
        schema_cnt_(0),
        version_(),
        concurrent_cnt_(0),
        row_store_type_(common::FLAT_ROW_STORE),
        snapshot_version_(storage::BUILD_INDEX_READ_SNAPSHOT_VERSION),
        report_(NULL),
        allocator_(common::ObModIds::OB_CS_BUILD_INDEX),
        checksum_method_(0)
  {}
  virtual ~ObBuildIndexParam()
  {}
  bool is_valid() const
  {
    return NULL != table_schema_ && NULL != index_schema_ && 0 != schema_cnt_ && 0 != concurrent_cnt_ &&
           NULL != report_;
  }
  void reset()
  {
    table_schema_ = NULL;
    index_schema_ = NULL;
    dep_table_schema_ = NULL;
    schema_version_ = 0;
    schema_cnt_ = 0;
    version_.reset();
    concurrent_cnt_ = 0;
    row_store_type_ = common::FLAT_ROW_STORE;
    snapshot_version_ = storage::BUILD_INDEX_READ_SNAPSHOT_VERSION;
    report_ = NULL;
    allocator_.reset();
    checksum_method_ = 0;
  }
  TO_STRING_KV(KP_(table_schema), KP_(index_schema), KP_(dep_table_schema), K_(schema_version), K_(schema_cnt),
      K_(version), K_(concurrent_cnt), K_(row_store_type), K_(snapshot_version), KP_(report), K_(checksum_method));

public:
  const share::schema::ObTableSchema* table_schema_;
  const share::schema::ObTableSchema* index_schema_;
  const share::schema::ObTableSchema* dep_table_schema_;
  int64_t schema_version_;
  int64_t schema_cnt_;
  ObVersion version_;
  int64_t concurrent_cnt_;
  common::ObRowStoreType row_store_type_;
  int64_t snapshot_version_;
  storage::ObIPartitionReport* report_;
  common::ObArenaAllocator allocator_;
  int64_t checksum_method_;
  ObSEArray<common::ObExtStoreRange, 32> local_sort_ranges_;
};

struct ObBuildIndexContext {
public:
  typedef storage::ObExternalSort<storage::ObStoreRow, storage::ObStoreRowComparer> ExternalSort;
  ObBuildIndexContext()
      : is_report_succ_(false),
        update_sstore_snapshot_version_(0),
        is_unique_checking_complete_(false),
        build_index_ret_(common::OB_SUCCESS),
        need_build_(true),
        allocator_(ObModIds::OB_SSTABLE_CREATE_INDEX),
        main_table_checksum_(NULL),
        column_cnt_(0),
        row_cnt_(0),
        lock_(),
        is_update_sstore_complete_(false),
        output_sstable_(NULL),
        index_macro_cnt_(0)
  {}
  virtual ~ObBuildIndexContext()
  {
    destroy();
  }
  int add_main_table_checksum(const int64_t* main_table_checksum, const int64_t row_count, const int64_t column_cnt);
  int check_column_checksum(const int64_t* index_table_checksum, const int64_t index_row_cnt, const int64_t column_cnt);
  int preallocate_local_sorters(const int64_t concurrent_cnt);
  void add_index_macro_cnt(const int64_t index_macro_cnt);
  void destroy();
  TO_STRING_KV(K_(is_report_succ), K_(update_sstore_snapshot_version), K_(is_unique_checking_complete),
      K_(build_index_ret), K_(need_build), KP_(main_table_checksum), K_(column_cnt), K_(row_cnt),
      K_(is_update_sstore_complete), K_(index_macro_cnt));
  bool is_report_succ_;
  int64_t update_sstore_snapshot_version_;
  bool is_unique_checking_complete_;
  int build_index_ret_;
  bool need_build_;
  common::ObArenaAllocator allocator_;
  int64_t* main_table_checksum_;
  int64_t column_cnt_;
  int64_t row_cnt_;
  ObSpinLock lock_;
  ObArray<ExternalSort*> sorters_;
  bool is_update_sstore_complete_;
  storage::ObTableHandle output_sstable_handle_;
  storage::ObSSTable* output_sstable_;
  int64_t index_macro_cnt_;
};

class ObMacroRowIterator {
public:
  struct Param {
    Param();
    bool is_valid() const;
    TO_STRING_KV(KP_(memctx_factory), KP_(schema), KP_(column_ids), KP_(table), K_(is_base_iter), K_(is_last_iter),
        K_(is_full_merge), K_(merge_level), K_(row_store_type), KP_(range), KP_(multi_version_row_info), K_(merge_type),
        K_(log_ts_range), K_(merge_log_ts), K_(is_iter_overflow_to_complement), K_(is_sstable_cut));

    memtable::ObIMemtableCtxFactory* memctx_factory_;
    const share::schema::ObTableSchema* schema_;
    const common::ObIArray<share::schema::ObColDesc>* column_ids_;
    storage::ObITable* table_;
    bool is_base_iter_;
    bool is_last_iter_;
    bool is_full_merge_;
    storage::ObMergeLevel merge_level_;
    common::ObRowStoreType row_store_type_;
    ObExtStoreRange* range_;
    const storage::ObMultiVersionRowInfo* multi_version_row_info_;
    storage::ObMergeType merge_type_;
    ObVersionRange version_range_;
    common::ObLogTsRange log_ts_range_;
    int64_t merge_log_ts_;
    bool is_iter_overflow_to_complement_;
    bool is_sstable_cut_;
  };
  ObMacroRowIterator();
  virtual ~ObMacroRowIterator();
  int init(const Param& param, const ObPartitionKey& pg_key);
  void destroy();
  virtual int next();
  /**
   * compare with other ObMacroRowIterator
   * @param other
   * @param cmp_ret: -2 represent cannot compare and this ObMacroRowIterator is a range iterator
   *                                     -1 represent this < other
   *                                     0 represent this == other
   *                                     1 represent this > other
   *                                     2 represent cannot compare and other ObMacroRowIterator is a range iterator
   * @return OB_SUCCESS or other error code
   */
  int compare(const ObMacroRowIterator& other, int64_t& cmp_ret);

  int open_curr_range();
  virtual int open_curr_macro_block();
  int open_curr_micro_block();

  int exist(const storage::ObStoreRow* row, bool& is_exist);
  inline const storage::ObStoreRow* get_curr_row() const
  {
    return curr_row_;
  }
  OB_INLINE common::ObStoreRange& get_current_range()
  {
    return curr_range_;
  }
  inline const storage::ObMacroBlockDesc& get_curr_macro_block()
  {
    return curr_block_desc_;
  }
  inline const blocksstable::ObMicroBlock& get_curr_micro_block()
  {
    return *curr_micro_block_;
  }
  inline bool macro_block_opened() const;
  inline bool micro_block_opened() const
  {
    return micro_block_opened_;
  }
  inline storage::ObMergeLevel get_merge_level() const
  {
    return curr_merge_level_;
  }
  const common::ObExtStoreRange& get_merge_range() const
  {
    return merge_range_;
  }
  inline bool is_end() const
  {
    return is_iter_end_;
  }
  inline storage::ObITable* get_table()
  {
    return table_;
  }
  inline bool is_base_iter() const
  {
    return is_base_iter_;
  }
  inline bool is_last_iter() const
  {
    return is_last_iter_;
  }
  inline bool is_sstable_iter() const
  {
    return is_sstable_iter_;
  }
  virtual int multi_version_compare(const ObMacroRowIterator& other, int64_t& cmp_ret);
  bool is_multi_version_compacted_row() const
  {
    return curr_row_->row_type_flag_.is_compacted_multi_version_row();
  }
  int compact_multi_version_sparse_row(const ObMacroRowIterator& other);
  int compact_multi_version_row(const ObMacroRowIterator& other, memtable::ObNopBitMap& bitmap);
  int64_t get_multi_version_rowkey_cnt() const
  {
    return multi_version_row_info_->multi_version_rowkey_column_cnt_;
  }
  int64_t get_trans_version_index() const
  {
    return multi_version_row_info_->trans_version_index_;
  }
  int64_t get_rowkey_column_cnt() const
  {
    return (NULL == multi_version_row_info_) ? rowkey_column_cnt_
                                             : multi_version_row_info_->multi_version_rowkey_column_cnt_;
  }
  int64_t get_row_column_cnt() const
  {
    return (NULL == multi_version_row_info_) ? column_ids_->count() : multi_version_row_info_->column_cnt_;
  }
  int64_t get_multi_version_start() const
  {
    return context_.trans_version_range_.multi_version_start_;
  }
  int get_current_trans_version(int64_t& trans_version);
  int64_t get_iter_row_count() const
  {
    return iter_row_count_;
  }
  int64_t get_magic_row_count() const
  {
    return magic_row_count_;
  }
  int64_t get_reuse_micro_block_count() const
  {
    return reuse_micro_block_count_;
  }
  void reset_first_multi_version_row_flag();
  void set_curr_row_first_dml(storage::ObRowDml first_dml)
  {
    const_cast<storage::ObStoreRow*>(curr_row_)->set_first_dml(first_dml);
  }
  OB_INLINE void inc_purged_count()
  {
    ++purged_count_;
  }
  OB_INLINE int64_t get_purged_count() const
  {
    return purged_count_;
  }
  bool need_rewrite_current_macro_block() const;
  bool need_rewrite_dirty_macro_block() const
  {
    return need_rewrite_dirty_macro_block_;
  }
  bool is_trans_state_table_valid() const;

  TO_STRING_KV(K_(table_id), K_(rowkey_column_cnt), KP_(column_ids), K_(schema_version), KP_(table), KP_(curr_row),
      KP_(ctx_factory), K_(is_iter_end), K_(use_block), K_(is_base_iter), K_(is_sstable_iter), K_(is_inited),
      K_(curr_block_desc), K_(macro_block_opened), K_(micro_block_opened), K_(curr_range), K_(macro_range),
      K_(merge_range), K_(advice_merge_level), K_(curr_merge_level), K_(micro_block_count), K_(reuse_micro_block_count),
      K_(iter_row_count), K_(magic_row_count), K_(purged_count), K_(need_rewrite_dirty_macro_block));

public:
  // represent cannot compare with other ObMacroRowIterator and this ObMacroRowIterator is a range iterator
  static const int64_t CANNOT_COMPARE_LEFT_IS_RANGE = -2;
  // represent cannot compare with other ObMacroRowIterator and other ObMacroRowIterator is a range iterator
  static const int64_t CANNOT_COMPARE_RIGHT_IS_RANGE = 2;
  static const int64_t CANNOT_COMPARE_BOTH_ARE_RANGE = 3;

protected:
  int64_t compare(const common::ObStoreRowkey& rowkey, const common::ObStoreRange& range);
  void get_border_key(const common::ObStoreRowkey& border_key, const bool is_start_key, common::ObStoreRowkey& rowkey);

protected:
  int next_range();
  void choose_merge_level();
  int init_macro_iter(const Param& param, const ObPartitionKey& pg_key);
  int init_row_iter(const Param& param);
  int open_curr_macro_block_();
  virtual int inner_init()
  {
    return OB_SUCCESS;
  }
  int init_iter_mode(const Param& param, storage::ObTableIterParam::ObIterTransNodeMode& iter_mode);

  uint64_t table_id_;
  int64_t rowkey_column_cnt_;
  const common::ObIArray<share::schema::ObColDesc>* column_ids_;
  int64_t schema_version_;
  storage::ObITable* table_;
  storage::ObMacroBlockIterator macro_block_iter_;
  storage::ObIStoreRowIterator* row_iter_;
  const storage::ObStoreRow* curr_row_;
  memtable::ObIMemtableCtxFactory* ctx_factory_;
  storage::ObStoreCtx ctx_;
  storage::ObMacroBlockDesc curr_block_desc_;
  storage::ObTableAccessParam param_;
  storage::ObTableAccessContext context_;
  blocksstable::ObBlockCacheWorkingSet block_cache_ws_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator stmt_allocator_;
  bool is_iter_end_;
  bool use_block_;
  bool is_base_iter_;
  bool is_last_iter_;
  bool is_sstable_iter_;
  bool is_inited_;
  ObExtStoreRange merge_range_;
  ObExtStoreRange macro_range_;

  // micro block level related
  common::ObStoreRange curr_range_;
  ObMicroBlockIterator micro_block_iter_;
  blocksstable::ObIMicroBlockRowScanner* micro_scanner_;
  blocksstable::ObMicroBlockRowScanner micro_row_scanner_;
  blocksstable::ObMultiVersionMicroBlockRowScanner multi_version_micro_row_scanner_;
  const blocksstable::ObMicroBlock* curr_micro_block_;
  storage::ObMergeLevel advice_merge_level_;
  storage::ObMergeLevel curr_merge_level_;
  bool macro_block_opened_;
  bool micro_block_opened_;
  common::ObRowStoreType row_store_type_;
  int64_t micro_block_count_;
  int64_t reuse_micro_block_count_;
  int64_t iter_row_count_;
  int64_t magic_row_count_;
  int64_t purged_count_;
  // multi version related
  const storage::ObMultiVersionRowInfo* multi_version_row_info_;
  common::ObObj cells_[common::OB_MAX_ROWKEY_COLUMN_NUMBER + 2];
  blocksstable::ObMacroBlockReader macro_reader_;
  bool need_rewrite_dirty_macro_block_;
  blocksstable::ObStorageFileHandle file_handle_;
};

class ObMinorMergeMacroRowIterator : public ObMacroRowIterator {
public:
  ObMinorMergeMacroRowIterator();
  virtual ~ObMinorMergeMacroRowIterator();
  virtual int next() override;
  virtual int open_curr_macro_block() override;
  virtual int multi_version_compare(const ObMacroRowIterator& other, int64_t& cmp_ret) override;
  void reset();

protected:
  int compact_row(const storage::ObStoreRow& former, const int64_t row_compact_info_index, storage::ObStoreRow& dest);
  int next_row_with_open_macro_block();
  virtual int inner_init() override;

private:
  int make_first_row_compacted();
  int compact_last_row();

private:
  enum RowCompactInfoIndex {
    RNPI_FIRST_ROW = 0,
    RNPI_LAST_ROW = 1,
    RNPI_MAX,
  };

protected:
  common::ObArenaAllocator obj_copy_allocator_;
  storage::ObObjDeepCopy obj_copy_;

private:
  storage::ObNopPos* nop_pos_[RNPI_MAX];
  ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>* bit_set_[RNPI_MAX];

  blocksstable::ObRowQueue row_queue_;
  bool check_first_row_compacted_;
};

struct ObRowFuseInfo {
  ObRowFuseInfo()
      : default_row_(NULL),
        split_old_row_(NULL),
        old_row_(NULL),
        tmp_row_(NULL),
        row_changed_(false),
        column_changed_(NULL),
        column_count_(0),
        rowkey_col_num_(0),
        nop_pos_()
  {}

  // TODO: add reset();

  storage::ObStoreRow* default_row_;
  storage::ObStoreRow* split_old_row_;
  storage::ObStoreRow* old_row_;
  storage::ObStoreRow* tmp_row_;
  bool row_changed_;
  bool* column_changed_;
  int64_t column_count_;
  int64_t rowkey_col_num_;
  storage::ObNopPos nop_pos_;
  common::ObBitSet<> col_id_set_;
};

// filter rows with from_base_ flag
template <typename T>
class ObDeltaSSStoreRowIterator : public T {
public:
  virtual int get_next_row(storage::ObStoreRow*& row) override
  {
    int ret = common::OB_SUCCESS;
    while ((common::OB_SUCCESS == (ret = T::get_next_row(row))) && NULL != row && row->from_base_) {
      // do nothing
    }
    return ret;
  }
};

// TODO(): remove static
class ObPartitionMergeUtil {
public:
  ObPartitionMergeUtil();
  virtual ~ObPartitionMergeUtil();

  static int merge_partition(memtable::ObIMemtableCtxFactory* memctx_factory, storage::ObSSTableMergeCtx& ctx,
      ObIStoreRowProcessor& processor, const int64_t idx, const bool iter_complement = false);

  static void stop_merge();

  static void resume_merge();

  static bool could_merge_start();

private:
  static bool need_open(int64_t cmp_ret);
  static bool need_open_left(int64_t cmp_ret);
  static bool need_open_right(int64_t cmp_ret);
  static int purge_minimum_iters(common::ObIArray<ObMacroRowIterator*>& minimum_iters, ObMacroRowIterator* base_iter);

  static int rewrite_macro_block(ObIPartitionMergeFuser::MERGE_ITER_ARRAY& minimum_iters,
      const storage::ObMergeLevel& merge_level, ObIPartitionMergeFuser* partition_fuser,
      ObIStoreRowProcessor& processor);

  static int fuse_row(const storage::ObSSTableMergeCtx& ctx,
      const common::ObIArray<ObMacroRowIterator*>& macro_row_iters, ObRowFuseInfo& row_fuse_info,
      common::ObIArray<share::schema::ObColDesc>& column_ids,
      const common::ObIArray<ObISqlExpression*>& generated_exprs, ObExprCtx& expr_ctx,
      ObCompactRowType::ObCompactRowTypeEnum& compact_type, common::ObIAllocator& allocator, const bool has_lob_column,
      const common::ObIArray<int32_t>* out_cols_project, const bool need_check_curr_row_last,
      storage::ObRowDml& cur_first_dml);

  static int get_old_row(ObRowFuseInfo& row_fuse_info, const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters);

  static int fuse_old_row(ObRowFuseInfo& row_fuse_info, ObMacroRowIterator* row_iter, storage::ObStoreRow* row);

  static int init_row_fuse_info(const int64_t column_cnt, const storage::ObSSTableMergeCtx& ctx,
      ObArenaAllocator& allocator, ObRowFuseInfo& row_fuse_info,
      ObArray<oceanbase::share::schema::ObColDesc, ObIAllocator&>& column_ids);

  static int init_partition_fuser(const storage::ObMergeParameter& merge_param, ObArenaAllocator& allocator,
      ObIPartitionMergeFuser*& partition_fuser);
  // compute the count of macro block need rewrite
  // only for major merge
  static int get_macro_block_count_to_rewrite_by_version(const storage::ObSSTableMergeCtx& ctx,
      const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
      ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt);
  static int get_macro_block_count_to_rewrite_by_round(const storage::ObSSTableMergeCtx& ctx,
      const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
      ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt);
  static int get_macro_block_count_to_rewrite(const storage::ObSSTableMergeCtx& ctx,
      const ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters, storage::ObSSTable* first_sstable,
      ObExtStoreRange& merge_range, int64_t& need_rewrite_block_cnt);

  static int init_macro_row_iter(storage::ObSSTableMergeCtx& ctx,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, memtable::ObIMemtableCtxFactory* memctx_factory,
      ObIAllocator& allocator, storage::ObITable* table, const blocksstable::ObDataStoreDesc* data_store_desc,
      ObExtStoreRange& merge_range, storage::ObMergeParameter& merge_param, const bool is_base_iter,
      const bool is_last_iter, ObMacroRowIterator*& macro_row_iter);

  static const int64_t INNER_ARRAY_BLOCK_SIZE = 2048;
  static bool do_merge_status;
};

class ObTableDumper {
public:
  static void print_error_info(
      int err_no, storage::ObSSTableMergeCtx& ctx, ObIPartitionMergeFuser::MERGE_ITER_ARRAY& macro_row_iters);
  static int generate_dump_table_name(const char* dir_name, const storage::ObITable* table, char* file_name);
  static int judge_disk_free_space(const char* dir_name, storage::ObITable* table);
  static int check_disk_free_space(const char* dir_name);

  static constexpr const double DUMP_TABLE_DISK_FREE_PERCENTAGE = 0.2;
  static constexpr const double MEMTABLE_DUMP_SIZE_PERCENTAGE = 0.2;
  static const int64_t ROW_COUNT_CHECK_INTERVAL = 10000;
  static int64_t free_space;
  static lib::ObMutex lock;

private:
  static bool need_dump_table(int err_no);
};

} /* namespace compaction */
} /* namespace oceanbase */

#endif /* OB_PARTITION_MERGE_UTIL_H_ */
