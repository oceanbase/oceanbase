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

#ifndef OCEANBASE_STORAGE_OB_TABLET_PERSIST_COMMON_H_
#define OCEANBASE_STORAGE_OB_TABLET_PERSIST_COMMON_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_op_handle.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_file.h"
#endif

namespace oceanbase
{
namespace storage
{
class ObTablet;
struct ObSSTableMetaPersistCtx;
class ObMultiTimeStats;

/// @brief: All shared meta blocks of transfer out tablet.
/// To prevent block leaks, the transfer in SS mode needs to
/// MERGE the src tablet's macro info into the dest tablet.
class ObSSTransferSrcTabletBlockInfo final
{
public:
  ObSSTransferSrcTabletBlockInfo()
    : is_inited_(false),
      src_tablet_meta_block_set_()
  {
  }
  ~ObSSTransferSrcTabletBlockInfo();
  OB_INLINE bool is_inited() const { return is_inited_; }
  int init(const ObTablet &src_tablet);
  int merge_to(/* out */ ObBlockInfoSet &out_block_info_set) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferSrcTabletBlockInfo);
  /// @brief: add block if @param block_info is shared meta block
  int add_block_info_if_need_(const ObTabletBlockInfo &block_info);

private:
  bool is_inited_;
  ObBlockInfoSet::TabletMacroSet src_tablet_meta_block_set_;
};

struct ObTabletPersisterParam final
{
public:
  // private tablet meta persistence
  ObTabletPersisterParam(
      const int64_t data_version,
      const share::ObLSID ls_id,
      const int64_t ls_epoch,
      const ObTabletID tablet_id,
      const int32_t private_transfer_epoch,
      const int64_t meta_version);

  // shared_major tablet meta persistence
  ObTabletPersisterParam(
      const uint64_t data_version,
      const ObTabletID tablet_id,
      const int32_t private_transfer_epoch,
      const int64_t snapshot_version,
      const int64_t start_macro_seq,
      blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback = nullptr,
      blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback = nullptr);
  #ifdef OB_BUILD_SHARED_STORAGE
  // inc_shared tablet meta persistence
  ObTabletPersisterParam(
      const uint64_t data_version,
      const share::ObLSID ls_id,
      const ObTabletID tablet_id,
      const ObMetaUpdateReason update_reason,
      const int64_t sstable_op_id,
      const int64_t start_macro_seq,
      ObAtomicOpHandle<ObAtomicTabletMetaOp> *handle,
      ObAtomicTabletMetaFile *file,
      blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback,
      blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback,
      const int64_t reorganization_scn,
      const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info);
  #endif

  ObTabletPersisterParam() = delete;

  bool is_valid() const;
  // only used to write major sstable's meta in shared storage mode
  bool is_major_shared_object() const
  {
    return snapshot_version_ > 0;
  }
  bool is_inc_shared_object() const
  {
    #ifdef OB_BUILD_SHARED_STORAGE
      return (op_handle_ != nullptr) && (file_ != nullptr);
    #else
      return false;
    #endif
  }
  bool for_ss_persist() const
  {
    return is_major_shared_object() || is_inc_shared_object();
  }
  int get_op_id(int64_t &op_id) const;


  TO_STRING_KV(K_(data_version), K_(ls_id), K_(ls_epoch), K_(tablet_id), K_(private_transfer_epoch),
   K_(snapshot_version), K_(start_macro_seq), KP_(ddl_redo_callback), KP_(ddl_finish_callback)
   #ifdef OB_BUILD_SHARED_STORAGE
   , KPC_(op_handle), KPC_(file), K_(update_reason), K_(sstable_op_id), K_(reorganization_scn), K_(meta_version),
   KP_(src_tablet_block_info)
   #endif
   );

  uint64_t data_version_;
  share::ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int32_t private_transfer_epoch_;
  int64_t snapshot_version_;
  int64_t start_macro_seq_;
  blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback_;
  blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback_;
  #ifdef OB_BUILD_SHARED_STORAGE
  ObAtomicOpHandle<ObAtomicTabletMetaOp> *op_handle_;
  ObAtomicTabletMetaFile *file_;
  ObMetaUpdateReason update_reason_;
  int64_t sstable_op_id_;
  int64_t reorganization_scn_;
  int64_t meta_version_;
  const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info_;
  #endif
  DISALLOW_COPY_AND_ASSIGN(ObTabletPersisterParam);
};

class ObTabletTransformArg final
{
public:
  ObTabletTransformArg();
  ~ObTabletTransformArg();
  ObTabletTransformArg(const ObTabletTransformArg &) = delete;
  ObTabletTransformArg &operator=(const ObTabletTransformArg &) = delete;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(rowkey_read_info_ptr),
               K_(tablet_meta),
               K_(table_store_addr),
               K_(storage_schema_addr),
               K_(tablet_macro_info_addr),
               KP_(tablet_macro_info_ptr),
               K_(ddl_kv_count),
               K_(memtable_count),
               KP_(new_table_store_ptr),
               K_(table_store_cache));
public:
  const ObRowkeyReadInfo *rowkey_read_info_ptr_;
  const ObTabletMacroInfo *tablet_macro_info_ptr_;
  ObTabletMeta tablet_meta_;
  ObMetaDiskAddr table_store_addr_;
  ObMetaDiskAddr storage_schema_addr_;
  ObMetaDiskAddr tablet_macro_info_addr_;
  ObDDLKV **ddl_kvs_;
  int64_t ddl_kv_count_;
  ObIMemtable *memtables_[MAX_MEMSTORE_CNT];
  int64_t memtable_count_;
  ObTabletTableStore *new_table_store_ptr_;
  ObTableStoreCache table_store_cache_;
  // If you want to add new member, make sure all member is assigned in 2 convert function.
  // ObTabletPersister::convert_tablet_to_mem_arg
  // ObTabletPersister::convert_tablet_to_disk_arg
};

class ObSSTablePersistWrapper final
{
public:
  ObSSTablePersistWrapper(
      const uint64_t data_version,
      const blocksstable::ObSSTable *sstable)
    : data_version_(data_version),
      sstable_(sstable)
  {}
  ~ObSSTablePersistWrapper() { reset(); }
  void reset() { sstable_ = nullptr; }
  bool is_valid() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(data_version), KPC_(sstable));
private:
  const uint64_t data_version_;
  const blocksstable::ObSSTable *sstable_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTablePersistWrapper);
};

class ObITabletMetaModifier
{
public:
  ObITabletMetaModifier() = default;
  virtual ~ObITabletMetaModifier() = default;
  virtual int modify_tablet_meta(ObTabletMeta &meta) = 0;
};

struct ObSharedBlockIndex final
{
public:
  ObSharedBlockIndex()
    : shared_macro_id_(), nested_offset_(0)
  {
  }
  ObSharedBlockIndex(
      const blocksstable::MacroBlockId &shared_macro_id,
      const int64_t nested_offset)
    : shared_macro_id_(shared_macro_id),
      nested_offset_(nested_offset)
  {
  }
  ~ObSharedBlockIndex()
  {
    reset();
  }
  void reset()
  {
    shared_macro_id_.reset();
    nested_offset_ = 0;
  }
  int hash(uint64_t &hash_val) const;
  bool operator ==(const ObSharedBlockIndex &other) const;

  TO_STRING_KV(K_(shared_macro_id), K_(nested_offset));
public:
  blocksstable::MacroBlockId shared_macro_id_;
  int64_t nested_offset_;
};

struct ObTabletBlockInfoSetBuilder
{
public:
  using SharedMacroMap = common::hash::ObHashMap<ObSharedBlockIndex,
                                                 int64_t,
                                                 common::hash::NoPthreadDefendMode,
                                                 common::hash::hash_func<ObSharedBlockIndex>,
                                                 common::hash::equal_to<ObSharedBlockIndex>,
                                                 common::hash::SimpleAllocer<common::hash::HashMapTypes<ObSharedBlockIndex, int64_t>::AllocType>,
                                                 common::hash::NormalPointer,
                                                 oceanbase::common::ObMalloc,
                                                 2>;
  using SharedMacroIter = SharedMacroMap::iterator;
  using ConstSharedMacroIter = SharedMacroMap::const_iterator;

  static int convert_macro_info_map(
      const SharedMacroMap &shared_macro_map,
      ObBlockInfoSet::TabletMacroMap &aggregated_info_map);
  static int copy_sstable_macro_info(
      const blocksstable::ObSSTable &sstable,
      SharedMacroMap &shared_macro_map,
      ObBlockInfoSet &block_info_set);
  static int copy_shared_macro_info(
      const blocksstable::ObSSTableMacroInfo &macro_info,
      SharedMacroMap &shared_macro_map,
      ObBlockInfoSet::TabletMacroSet &meta_id_set,
      ObBlockInfoSet::TabletMacroSet &backup_id_set);

  static int copy_data_macro_ids(
      const blocksstable::ObSSTableMacroInfo &macro_info,
      ObBlockInfoSet &block_info_set);
private:
  static int do_copy_ids_(
      blocksstable::ObMacroIdIterator &iter,
      ObBlockInfoSet::TabletMacroSet &data_id_set,
      ObBlockInfoSet::TabletMacroSet &meta_id_set,
      ObBlockInfoSet::TabletMacroSet &backup_id_set);
};

class ObMultiTimeStats
{
public:
  class TimeStats
  {
  public:
    explicit TimeStats(const char *owner);
    void click(const char *step_name);
    ~TimeStats() {}
    int64_t to_string(char *buf, const int64_t buf_len) const;
    int set_extra_info(const char *fmt, ...);
    int64_t get_total_time() const { return last_ts_ - start_ts_; }

  private:
    static const int64_t MAX_CLICK_COUNT = 16;
    static const int64_t MAX_EXTRA_INFO_LENGTH = 128;
    const char *owner_;
    int64_t start_ts_;
    int64_t last_ts_;
    const char *click_str_[MAX_CLICK_COUNT];
    int32_t click_[MAX_CLICK_COUNT];
    int32_t click_count_;
    char extra_info_[MAX_EXTRA_INFO_LENGTH];
    bool has_extra_info_;

    DISALLOW_COPY_AND_ASSIGN(TimeStats);
  };

  ObMultiTimeStats(
    ObArenaAllocator *allocator,
    const int64_t auto_print_threshold = -1);
  ~ObMultiTimeStats();
  int acquire_stats(const char *owner, TimeStats *&stats);
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  static const int64_t MAX_STATS_CNT = 16;
  ObArenaAllocator *allocator_;
  TimeStats *stats_;
  int32_t stats_count_;
  int64_t auto_print_threshold_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiTimeStats);
};

class ObSSTableMetaPersistHelper final
{
public:
  static const int64_t SSTABLE_MAX_SERIALIZE_SIZE = 1966080L;  // 1.875MB

public:
  struct IWriteOperator
  {
  public:
    IWriteOperator() = default;
    virtual ~IWriteOperator() { reset(); }
    virtual bool is_valid() const = 0;
    virtual int do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos);
    virtual int fill_write_info(
        const ObTabletPersisterParam &param,
        const ObSSTable &sstable,
        common::ObIAllocator &allocator,
        /* out */ ObSharedObjectWriteInfo &write_info) const;
    /// @brief: clean the result of last write operation.
    virtual void reset() { written_addrs_.reuse(); }
    const common::ObIArray<ObMetaDiskAddr> &get_written_addrs() const
    {
      return written_addrs_;
    }
    common::ObIArray<ObMetaDiskAddr> &get_written_addrs()
    {
      return written_addrs_;
    }
    VIRTUAL_TO_STRING_KV("", "");

  protected:
    common::ObSArray<ObMetaDiskAddr> written_addrs_;
  };

public:
  ObSSTableMetaPersistHelper(
    const ObTabletPersisterParam &persister_param,
    ObSSTableMetaPersistCtx &ctx,
    const int64_t large_co_sstable_threshold,
    int64_t &start_macro_seq)
    : is_inited_(false),
      large_co_sstable_threshold_(large_co_sstable_threshold),
      data_version_(persister_param.data_version_),
      start_macro_seq_(start_macro_seq),
      persister_param_(persister_param),
      ctx_(ctx),
      sst_write_op_(nullptr)
  {
  }
  ~ObSSTableMetaPersistHelper() = default;
  int register_write_op(ObSSTableMetaPersistHelper::IWriteOperator *sst_write_op);
  /// @param new_table_store_allocator: alloc new_table_store's memory
  int do_persist_all_sstables(
      const ObTabletTableStore &old_table_store,
      common::ObArenaAllocator &new_table_store_allocator,
      /*out*/ ObMultiTimeStats &multi_stats,
      /*out*/ ObTabletTableStore &new_table_store,
      /*out*/ int64_t &sst_meta_size_aligned);
  /// @brief: only be called by fully direct load
  int do_persist_for_full_direct_load(
      ObSSTable *sstable,
      ObCOSSTableV2 *&out_co_sstable);
  TO_STRING_KV(K_(is_inited),
               K_(large_co_sstable_threshold),
               K_(data_version),
               K_(start_macro_seq),
               K_(persister_param),
               K_(ctx),
               KP_(sst_write_op));

private:
  bool is_ready_for_persist_() const;
  // to avoid redundant check, all methods below assume that all parameters are valid.
  /// @brief: inner method for sstable persist
  /// @param skip_normal_sst: true if skip normal sstable(sstable is not a large co sstable), only when processing fully direct load.
  /// @param[out] out_co_sstable: return nullptr if @param sstable is not a large co sstable.
  int inner_do_persist_single_sst_(
      ObSSTable &sstable,
      const bool skip_normal_sst,
      ObCOSSTableV2 *&out_co_sstable);
  int persist_sstable_linked_block_if_need_(ObSSTable &sstable);
  int persist_large_co_sstable_(
      ObCOSSTableV2 &co_sstable,
      ObCOSSTableV2 *&out_co_sstable);
  int persist_normal_sstable_(ObSSTable &sstable);
  int record_cg_sstables_macro_(const ObCOSSTableV2 &co_sstable);
  int fill_sstable_write_info_and_record_(
      const ObSSTable &sstable,
      const bool check_has_padding_meta_cache,
      /*out*/ ObIArray<ObSharedObjectWriteInfo> &write_infos);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMetaPersistHelper);

private:
  bool is_inited_;
  const int64_t large_co_sstable_threshold_;
  const int64_t data_version_;
  int64_t &start_macro_seq_;
  const ObTabletPersisterParam &persister_param_;
  ObSSTableMetaPersistCtx &ctx_;
  ObSSTableMetaPersistHelper::IWriteOperator *sst_write_op_;
};

struct ObSSTableMetaPersistCtx
{
public:
  /// @param allocator: @c write_reqs_'s memory will be allocated by @param allocator.
  /// NOTE: Please make sure that the life cycle of @c write_reqs_ is WITHIN the scope
  /// of @param allocator.
  ObSSTableMetaPersistCtx(
    ObBlockInfoSet &block_info_set,
    ObIArray<ObSharedObjectsWriteCtx> &sstable_meta_write_ctxs,
    common::ObArenaAllocator &allocator)
    : is_inited_(false),
      total_tablet_meta_size_(0),
      cg_sstable_cnt_(0),
      large_co_sstable_cnt_(0),
      small_co_sstable_cnt_(0),
      normal_sstable_cnt_(0),
      allocator_(allocator),
      block_info_set_(block_info_set),
      sstable_meta_write_ctxs_(sstable_meta_write_ctxs)
  {
  }
  ~ObSSTableMetaPersistCtx() = default;
  int init(
      const int64_t ctx_id,
      const int64_t map_bucket_cnt);
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited),
               K_(total_tablet_meta_size),
               K_(cg_sstable_cnt),
               K_(large_co_sstable_cnt),
               K_(small_co_sstable_cnt),
               K_(normal_sstable_cnt),
               K_(tables),
               K_(write_infos),
               K_(sstable_meta_write_ctxs));

public:
  bool is_inited_;
  int64_t total_tablet_meta_size_;
  int64_t cg_sstable_cnt_;
  int64_t large_co_sstable_cnt_;
  int64_t small_co_sstable_cnt_;
  int64_t normal_sstable_cnt_;
  ObTabletBlockInfoSetBuilder::SharedMacroMap shared_macro_map_;
  /// @brief: sstables that exclude cg sstable(used for building table store)
  common::ObSArray<ObITable *> tables_;
  common::ObSEArray<ObSharedObjectWriteInfo, 16> write_infos_;
  common::ObArenaAllocator &allocator_;
  ObBlockInfoSet &block_info_set_;
  ObIArray<ObSharedObjectsWriteCtx> &sstable_meta_write_ctxs_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMetaPersistCtx);
};


struct ObTabletPersistCommon
{
public:
  class SSTablePersistWrapper final
  {
  public:
    SSTablePersistWrapper(
        const uint64_t data_version,
        const blocksstable::ObSSTable *sstable)
      : data_version_(data_version),
        sstable_(sstable)
    {
    }
    ~SSTablePersistWrapper() { reset(); }
    OB_INLINE void reset() { sstable_ = nullptr; }
    OB_INLINE bool is_valid() const
    {
      return nullptr != sstable_
             && sstable_->is_sstable()
             && sstable_->is_valid();
    }
    OB_INLINE int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(!is_valid())) {
        ret = OB_ERR_UNDEFINED;
        STORAGE_LOG(WARN, "wrapper is unexpected not valid", K(ret));
      } else if (OB_FAIL(sstable_->serialize_full_table(data_version_, buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "failed to serialize full sstable", K(ret), KPC(sstable_));
      }
      return ret;
    }
    OB_INLINE int64_t get_serialize_size() const
    {
      int64_t len = 0;
      if (OB_UNLIKELY(!is_valid())) {
        // do nothing
      } else {
        len = sstable_->get_full_serialize_size(data_version_);
      }
      return len;
    }
    TO_STRING_KV(K_(data_version), KPC_(sstable));
  private:
    const uint64_t data_version_;
    const blocksstable::ObSSTable *sstable_;
    DISALLOW_COPY_AND_ASSIGN(SSTablePersistWrapper);
  };

  template<class MemberType>
  class PersistWithDataVersionWrapper final
  {
public:
    PersistWithDataVersionWrapper(
        const uint64_t data_version,
        const MemberType *member)
      : data_version_(data_version),
        member_(member)
    {
    }
    ~PersistWithDataVersionWrapper() { reset(); }
    OB_INLINE void reset() { member_ = nullptr; }
    OB_INLINE bool is_valid() const { return nullptr != member_; }
    OB_INLINE int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(!is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "wrapper is unexpected not valid", K(ret));
      } else if (OB_FAIL(member_->serialize(data_version_, buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "failed to serialize member", K(ret), KP(buf), K(buf_len), K(pos));
      }
      return ret;
    }
    OB_INLINE int64_t get_serialize_size() const
    {
      int64_t len = 0;
      if (OB_UNLIKELY(!is_valid())) {
        // do nothing
      } else {
        len = member_->get_serialize_size(data_version_);
      }
      return len;
    }
    TO_STRING_KV(K_(data_version));
  private:
    const uint64_t data_version_;
    const MemberType *member_;
    DISALLOW_COPY_AND_ASSIGN(PersistWithDataVersionWrapper);
  };

public:
  template<class MemberType>
  /// @brief: serialize tablet member into buffer
  /// @param member: tablet member to serialize. need provide to_string(), get_serialize_size()
  static int fill_write_info(
      const ObTabletPersisterParam &persister_param,
      const MemberType *member,
      ObIAllocator &allocator,
      /*out*/ ObSharedObjectWriteInfo &write_info);
  static int build_async_write_start_opt(
      const ObTabletPersisterParam &param,
      const int64_t cur_macro_seq,
      blocksstable::ObStorageObjectOpt &start_opt);
  static int sync_cur_macro_seq_from_opt(
      const ObTabletPersisterParam &param,
      const blocksstable::ObStorageObjectOpt &curr_opt,
      /*out*/ int64_t &cur_macro_seq);
  static int wait_write_info_callback(const common::ObIArray<ObSharedObjectWriteInfo> &write_infos);
  static int sync_write_ctx_to_total_ctx_if_failed(
      common::ObIArray<ObSharedObjectsWriteCtx> &write_ctxs,
      common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs);
  static int calc_and_set_tablet_space_usage(
      const ObBlockInfoSet &block_info_set,
      /*out*/ common::ObIArray<MacroBlockId> &shared_meta_id_arr,
      /*out*/ ObTabletHandle &new_tablet_hdl,
      /*out*/ ObTabletSpaceUsage &space_usage);
  static int calc_tablet_space_usage(
      const bool is_empty_shell,
      const ObBlockInfoSet &block_info_set,
      const ObTabletTableStore &table_store,
      /*out*/ ObTabletSpaceUsage &space_usage);
  static int get_tablet_persist_size(
      const uint64_t data_version,
      const ObTabletMacroInfo *macro_info,
      const ObTablet *tablet,
      /*out*/ int64_t &size);
  static int fill_tablet_into_buf(
      const uint64_t data_version,
      const ObTabletMacroInfo *macro_info,
      const ObTablet *tablet,
      const int64_t size,
      /*out*/ char *buf,
      /*out*/ int64_t &pos);
  static int fill_tablet_write_info(
      const ObTabletPersisterParam &param,
      common::ObArenaAllocator &allocator,
      const ObTabletMacroInfo &macro_info,
      const ObTablet *tablet,
      ObSharedObjectWriteInfo &write_info);

  static int calc_sstable_occupy_size_by_table_store(
      const ObTabletTableStore &table_store,
      /*out*/ int64_t &all_sstable_occupy_size,
      /*out*/ int64_t &ss_public_sstable_occupy_size,
      /*out*/ int64_t &pure_backup_sstable_occupy_size);
  static int make_tablet_macro_info(
    const ObTabletPersisterParam &param,
    const int64_t macro_seq,
    common::ObArenaAllocator &allocator,
    /* out */ ObBlockInfoSet &block_info_set,
    /* out */ ObLinkedMacroBlockItemWriter &linked_writer,
    /* out */ ObTabletMacroInfo &macro_info);
};

template<class MemberType>
int ObTabletPersistCommon::fill_write_info(
      const ObTabletPersisterParam &persister_param,
      const MemberType *member,
      ObIAllocator &allocator,
      /*out*/ ObSharedObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  write_info.reset();
  char *buf = nullptr;
  int64_t buf_size = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(nullptr == member
                  || !persister_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg", K(ret), KP(member), K(persister_param));
  } else if (FALSE_IT(buf_size = member->get_serialize_size())) {
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "serialize size should not be less than(or equal to) 0.", K(ret), K(member), KPC(member));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to allocate buffer", K(ret), K(buf_size), KP(buf));
  } else if (OB_FAIL(member->serialize(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "failed to serialize member", K(ret), KP(buf), K(buf_size), K(pos), KPC(member));
  } else {
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    if (!persister_param.is_inc_shared_object()) {
      write_info.ls_epoch_ = persister_param.ls_epoch_;
    } else {
      write_info.write_callback_ = persister_param.ddl_redo_callback_;
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_TABLET_PERSIST_COMMON_H_