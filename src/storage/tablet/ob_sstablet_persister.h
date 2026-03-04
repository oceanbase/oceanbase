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

#ifndef OCEANBASE_STORAGE_OB_SSTABLET_PERSISTER_H_
#define OCEANBASE_STORAGE_OB_SSTABLET_PERSISTER_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet_persist_common.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/atomic_protocol/ob_atomic_op_handle.h"
#endif


namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObBlockInfoSet;
#ifdef OB_BUILD_SHARED_STORAGE
class ObSSTabletPersister;
class ObAtomicTabletMetaFile;
class ObAtomicTabletMetaOp;
#endif
class ObSSTabletPersister final
{
public:
  static int persist_tablet(
      const ObTabletPersisterParam &param,
      const ObTablet &tablet);

  TO_STRING_KV(K_(write_strategy),
               K_(param),
               K_(write_buffer),
               K_(linked_block_write_ctxs),
               K_(cur_macro_seq),
               "preallocated_seqs_start", preallocated_seq_range_.first,
               "preallocated_seqs_end", preallocated_seq_range_.second,
               K_(multi_stats));

private:
  /// @brief: SSTablet write strategy
  /// 1. WRITE_BATCH: write reqs will be cached through InnerWriteBuffer, and calling InnerWriteBuffer::sync_batch
  /// will flush all cached reqs in a batch.
  /// 2. WRITE_AGGREGATED: writes will be performed using ObSharedObjectReaderWriter, each InnerWriteBuffer::append
  /// operation immediately executes the write req.
  enum class WriteStrategy:uint8_t
  {
    BATCH = 0,
    AGGREGATED = 1,
    MAX = 2
  };

private:
  static const int64_t WRITE_BUFFER_SIZE_LIMIT = 32LL << 20; // 32MiB

  /// @brief: if the object size exceed the limit, it will be written
  /// to OSS; otherwise, it will be written to table
  static const int64_t OBJECT_LIMIT = 1LL << 20; // 1MiB

private:

  static int decide_write_strategy_(/* out */ WriteStrategy &strategy);

private:
  template<class MemberType>
  class ObSSTabletMetaMemberWrapper final
  {
  public:
    ObSSTabletMetaMemberWrapper(
      const WriteStrategy write_strategy,
      const MemberType *member)
      : write_strategy_(write_strategy),
        member_(member)
    {
    }
    ~ObSSTabletMetaMemberWrapper() { reset(); }
    OB_INLINE void reset() { member_ = nullptr; }
    OB_INLINE bool is_valid() const { return write_strategy_ < WriteStrategy::MAX && nullptr != member_; }
    OB_INLINE int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(!is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "wrapper is unexpected not valid", K(ret));
      } else if (OB_FAIL(member_->serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "failed to serialize member", K(ret), KP(buf), K(buf_len),
          K(pos));
      }
      return ret;
    }

    /// NOTE: The serialization size alignment depends on the target storage:
    ///  1. For member written to the meta table(size <= @c OBJECT_LIMIT), 4K alignment
    ///     is NOT required.
    ///  2. For those written to OSS (size > @c OBJECT_LIMIT), 4K alignment is enforced
    ///     due to low-level IO requirements.
    OB_INLINE int64_t get_serialize_size() const
    {
      int64_t serialize_size = 0;
      if (OB_UNLIKELY(!is_valid())) {
        STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected invalid member wrapper", KPC(this));
      } else {
        serialize_size = member_->get_serialize_size();
        if (WriteStrategy::BATCH == write_strategy_
            && !ObSSTabletPersister::write_to_meta_table_(serialize_size)) {
          // write to OSS, aligned with 4KiB
          serialize_size = upper_align(serialize_size, DIO_READ_ALIGN_SIZE);
        }
      }
      return serialize_size;
    }

    TO_STRING_KV(K_(write_strategy), KP_(member));

  private:
    const ObSSTabletPersister::WriteStrategy write_strategy_;
    const MemberType *member_;
  };

  struct InnerWriteSummary final
  {
  public:
    explicit InnerWriteSummary(const lib::ObMemAttr &mem_attr)
      : write_ops_for_table_(0),
        write_ops_for_oss_(0),
        bytes_in_table_(0),
        bytes_in_oss_(0)
    {
      written_objects_.set_attr(mem_attr);
    }
    ~InnerWriteSummary() = default;
    int record(
      const ObSharedObjectWriteInfo &write_info,
      const ObMetaDiskAddr &addr);
    int64_t total_write_ops() const { return write_ops_for_table_ + write_ops_for_oss_; }
    int64_t total_write_bytes() const { return bytes_in_table_ + bytes_in_oss_; }

    TO_STRING_KV(K_(write_ops_for_table),
                 K_(write_ops_for_oss),
                 K_(bytes_in_table),
                 K_(bytes_in_oss),
                 "written_blocks_cnt",
                 written_objects_.count());

  private:
    DISALLOW_COPY_AND_ASSIGN(InnerWriteSummary);

  public:
    int64_t write_ops_for_table_;
    int64_t write_ops_for_oss_;
    int64_t bytes_in_table_;
    int64_t bytes_in_oss_;
    ObSEArray<MacroBlockId, 16> written_objects_;
  };

  class InnerWriteBuffer final
  {
  private:
    using WriteStrategy = ObSSTabletPersister::WriteStrategy;

  public:
    InnerWriteBuffer(
      const ObTabletPersisterParam &param,
      const lib::ObMemAttr &mem_attr,
      const int64_t buffer_size_limit,
      common::ObArenaAllocator &allocator)
      : is_inited_(false),
        write_strategy_(WriteStrategy::MAX),
        param_(param),
        buffer_size_limit_(buffer_size_limit),
        buf_size_in_bytes_(0),
        allocator_(allocator),
        write_summary_(mem_attr)
    {
      pending_write_infos_.set_attr(mem_attr);
      pending_object_opts_.set_attr(mem_attr);
      block_ids_.set_attr(mem_attr);
    }
    ~InnerWriteBuffer();
    int init(const WriteStrategy write_strategy);
    OB_INLINE common::ObArenaAllocator &get_allocator() { return allocator_; }
    OB_INLINE WriteStrategy write_strategy() const { return write_strategy_; }
    OB_INLINE int64_t pending_write_cnt() const
    {
      return pending_write_infos_.count();
    }
    const InnerWriteSummary &write_summary() const { return write_summary_; }
    int append_batch(
      const ObSharedObjectWriteInfo &write_info,
      const int64_t macro_seq,
      /* out */ ObMetaDiskAddr &addr);
    int append(
      const ObIArray<ObSharedObjectWriteInfo> &write_infos,
      /* out */ int64_t &macro_seq,
      /* out */ ObIArray<ObMetaDiskAddr> &addrs);
    // For non-user tenants, the write operation is already completed
    // upon returning from append() call, so there is no need to manually
    // call this method.
    int sync_batch();
    TO_STRING_KV(K_(write_strategy),
                 K_(buffer_size_limit),
                 K_(buf_size_in_bytes),
                 K(pending_write_infos_.count()),
                 K(pending_object_opts_.count()),
                 K(block_ids_.count()));

  private:
    int append_batch_(
      const ObIArray<ObSharedObjectWriteInfo> &write_infos,
      /* out */ int64_t &macro_seq,
      /* out */ ObIArray<ObMetaDiskAddr> &addrs);
    int append_and_write_aggregated_(
      const ObIArray<ObSharedObjectWriteInfo> &write_infos,
      /* out */ int64_t &macro_seq,
      /* out */ ObIArray<ObMetaDiskAddr> &addrs);

  private:
    /// @brief: Generate @param[out] ObStorageObjectOpt by provided write info,
    /// which is needed by write interface of @c ObObjectManager.
    /// Determine where to write(OSS or table).
    static int get_storage_opt_by_write_info_(
      const ObTabletPersisterParam &param,
      const int64_t macro_seq,
      const ObSharedObjectWriteInfo &write_info,
      /*out*/ ObStorageObjectOpt &object_opt);
    static int convert_write_info_(
      const ObSharedObjectWriteInfo &write_info,
      /*out*/ ObStorageObjectWriteInfo &storage_write_info);

  private:
    bool is_valid_() const
    {
      return is_inited_ && write_strategy_ < WriteStrategy::MAX
             && pending_write_infos_.count() == pending_object_opts_.count()
             && block_ids_.count() == pending_object_opts_.count();
    }
    int inner_sync_batch_();
    int reuse_buffer_();
    int trigger_callback_if_need_(const common::ObIArray<MacroBlockId> &written_blocks);
    DISALLOW_COPY_AND_ASSIGN(InnerWriteBuffer);

  private:
    bool is_inited_;
    WriteStrategy write_strategy_;
    const ObTabletPersisterParam &param_;
    const int64_t buffer_size_limit_;
    int64_t buf_size_in_bytes_;
    common::ObArenaAllocator &allocator_;
    /// mem allocated by @c allocator
    ObSEArray<ObSharedObjectWriteInfo, 16> pending_write_infos_;
    ObSEArray<ObStorageObjectOpt, 16> pending_object_opts_;
    ObSEArray<MacroBlockId, 16> block_ids_; // for consistency check
    InnerWriteSummary write_summary_;
  };

  struct SSTableMetaWriteOp final : public ObSSTableMetaPersistHelper::IWriteOperator
  {
  public:
    SSTableMetaWriteOp(
      const ObTabletPersisterParam &persist_param,
      int64_t &macro_seq,
      ObBlockInfoSet &block_info_set,
      InnerWriteBuffer &write_buffer)
      : persist_param_(persist_param),
        macro_seq_(macro_seq),
        block_info_set_(block_info_set),
        write_buffer_(write_buffer)
    {
    }
    int do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos) override;
    int fill_write_info(
        const ObTabletPersisterParam &param,
        const ObSSTable &sstable,
        common::ObIAllocator &allocator,
        /* out */ ObSharedObjectWriteInfo &write_info) const override;
    bool is_valid() const override;
    INHERIT_TO_STRING_KV(
      "IWriteOperator",
      IWriteOperator,
      K_(persist_param),
      K_(macro_seq),
      K_(write_buffer));

  protected:
    DISALLOW_COPY_AND_ASSIGN(SSTableMetaWriteOp);

  protected:
    const ObTabletPersisterParam &persist_param_;
    int64_t &macro_seq_;
    ObBlockInfoSet &block_info_set_;
    InnerWriteBuffer &write_buffer_;
  };

  struct SubMetaWriteResult final
  {
  public:
    SubMetaWriteResult(const bool is_empty_shell)
      : is_empty_shell_(is_empty_shell)
    {
      if (is_empty_shell) {
        table_store_addr_.set_none_addr();
        storage_schema_addr_.set_none_addr();
      }
    }
    ~SubMetaWriteResult() = default;
    OB_INLINE bool is_valid() const
    {
      return (!is_empty_shell_
             && table_store_addr_.is_valid()
             && storage_schema_addr_.is_valid()
             && new_table_store_.is_valid()) // judgement case 1
             || (is_empty_shell_
             && table_store_addr_.is_none()
             && storage_schema_addr_.is_none()); // judgement case 2
    }

    int get_table_store_meta_info(
        const bool is_ls_tx_data_tablet,
        /*out*/ ObSSTabletTableStoreMetaInfo &table_store_meta_info) const;

    TO_STRING_KV(K_(is_empty_shell),
                 K_(table_store_addr),
                 K_(storage_schema_addr),
                 K_(new_table_store));

  private:
    DISALLOW_COPY_AND_ASSIGN(SubMetaWriteResult);

  public:
    const bool is_empty_shell_;
    ObMetaDiskAddr table_store_addr_;
    ObMetaDiskAddr storage_schema_addr_;
    ObTabletTableStore new_table_store_;
  };

private:
  static bool write_to_meta_table_(const int64_t meta_serialize_size);
  template<typename MemberType>
  static int fill_write_info_(
    const WriteStrategy write_strategy,
    const ObTabletPersisterParam &param,
    const MemberType *member,
    common::ObIAllocator &allocator,
    /* out */ ObSharedObjectWriteInfo &write_info);

private:
  ObSSTabletPersister(
      const ObTabletPersisterParam &param,
      const int64_t mem_ctx_id);
  ~ObSSTabletPersister() = default;
  int init_();
  bool is_ready_for_persist_() const;
  int inner_persist_tablet_(const ObTablet &tablet);

private:
  int inner_persist_sub_tablet_meta_batch_(
      const ObTablet &tablet,
      ObMultiTimeStats::TimeStats &time_stats,
      /*out*/ SubMetaWriteResult &sub_meta_write_res);
  int inner_persist_sub_tablet_meta_aggregated_(
      const ObTablet &tablet,
      ObMultiTimeStats::TimeStats &time_stats,
      /*out*/ SubMetaWriteResult &sub_meta_write_res);
  int inner_persist_aggregated_meta_(
      const ObTablet &tablet,
      const ObTabletMacroInfo &macro_info,
      const SubMetaWriteResult &sub_meta_write_res,
      ObMultiTimeStats::TimeStats &time_stats);

private:
  // sub tablet meta relatived methods
  int fetch_table_store_and_write_info_(
      const ObTablet &tablet,
      /*out*/ ObSharedObjectWriteInfo &out_write_info,
      /*out*/ ObTabletTableStore &new_table_store);
  int fetch_storage_schema_and_write_info_(
      const ObTablet &tablet,
      /*out*/ ObSharedObjectWriteInfo &out_write_info);
  int fetch_and_persist_tablet_macro_info_(
      /*out*/ ObLinkedMacroBlockItemWriter &linked_writer,
      /*out*/ ObTabletMacroInfo &macro_info);

private:
  static int check_tablet_macro_info_(const ObTabletMacroInfo &macro_info);
  static int check_tablet_space_usage_(
    const InnerWriteSummary &summary,
    const ObTabletSpaceUsage &tablet_space_usage);
  static int check_macro_seqs_for_write_batch_(
    const int64_t start_macro_seq,
    const int64_t end_macro_seq,
    const InnerWriteSummary &write_summary,
    const SubMetaWriteResult &sub_meta_write_res,
    const ObIArray<ObSharedObjectsWriteCtx> &linked_block_write_ctxs);
  // sanity check after sub meta persistence complete
  int sanity_check_(const SubMetaWriteResult &sub_meta_write_res);

private:
  void preallocate_macro_seqs_(const int64_t n);
  int get_preallocated_macro_seq_(/* out */ int64_t &macro_seq);

private:
  static int init_new_tablet_for_persist_(
      const ObTablet &old_tablet,
      common::ObArenaAllocator &allocator,
      const SubMetaWriteResult &sub_meta_write_res,
      const ObTabletSpaceUsage &new_space_usage,
      /*out*/ ObTablet &new_tablet);

  DISALLOW_COPY_AND_ASSIGN(ObSSTabletPersister);

private:
  bool is_inited_;
  WriteStrategy write_strategy_;
  const ObTabletPersisterParam &param_;
  ObArenaAllocator allocator_;
  /// @brief: link blocks of sstable's macro info
  /// reclaiming these blocks is needed if some errs happened
  /// during tablet persisting.
  common::ObSArray<ObSharedObjectsWriteCtx> linked_block_write_ctxs_;
  ObBlockInfoSet block_info_set_;
  ObTabletSpaceUsage space_usage_;
  int64_t cur_macro_seq_; // cur_macro_seq >= preallocated_seq_range_.second
  std::pair<int64_t, int64_t> preallocated_seq_range_; // [first, second)
  InnerWriteBuffer write_buffer_;
  ObMultiTimeStats multi_stats_;
};

template<typename MemberType>
int ObSSTabletPersister::fill_write_info_(
    const WriteStrategy write_strategy,
    const ObTabletPersisterParam &param,
    const MemberType *member,
    common::ObIAllocator &allocator,
    /* out */ ObSharedObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  write_info.reset();

  if (OB_UNLIKELY(write_strategy >= WriteStrategy::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid write strategy", K(ret), K(write_strategy));
  } else if (OB_UNLIKELY(!param.is_valid()
                  || !param.for_ss_persist())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid persister param", K(ret), K(param));
  } else if (OB_ISNULL(member)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null member", K(ret), KP(member));
  } else {
    ObSSTabletMetaMemberWrapper<MemberType> wrapper(write_strategy, member);
    if (OB_FAIL(ObTabletPersistCommon::fill_write_info(param,
                                                       &wrapper,
                                                       allocator,
                                                       write_info))) {
      STORAGE_LOG(WARN, "failed to fill write info", K(ret));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_SSTABLET_PERSISTER_H_