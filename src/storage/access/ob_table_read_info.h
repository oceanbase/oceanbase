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

#ifndef OB_STORAGE_ACCESS_TABLE_READ_INFO_H_
#define OB_STORAGE_ACCESS_TABLE_READ_INFO_H_

#include "storage/meta_mem/ob_fixed_meta_obj_array.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ob_storage_schema.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnParam;
class ObColDesc;
class ObColExtend;
}
}
using namespace share::schema;
namespace storage {
class ObStorageSchema;
class ObCGReadInfo;
class ObTenantCGReadInfoMgr;

typedef ObFixedMetaObjArray<ObColumnParam *> Columns;
typedef ObFixedMetaObjArray<int32_t> ColumnsIndex;
typedef ObFixedMetaObjArray<int32_t> CGIndex;
typedef ObFixedMetaObjArray<ObColDesc> ColDescArray;
typedef ObFixedMetaObjArray<ObColExtend> ColExtendArray;
/*
  ObITableReadInfo: basic interface
  --ObReadInfoStruct: basic struct, implement get* func
  ----ObTableReadInfo:  for access, may not get all column
  ----ObRowkeyReadInfo: for compaction, get all column | for mid-index, get rowkey
  --ObCGReadInfo: for cg sstable
    ObReadInfoStruct ptr which point to common cg read info in ObTenantCGReadInfoMgr
    ObColumnParam array ptr record padding column info

  ObTenantCGReadInfoMgr: record common cg_read_info & provide alloc string_cg_read_info with padding

  row_store SSTable
    use ObRowkeyReadInfo on tablet to read data & mid-index
  col_store SSTable
    use ObTenantCGReadInfoMgr::index_read_info to read mid-index
    get ObCGReadInfo from ObTenantCGReadInfoMgr to read data
*/
struct ObColumnIndexArray
{
public:
  ObColumnIndexArray(const bool rowkey_mode = false, const bool for_memtable = false);
  ~ObColumnIndexArray() { reset(); }
  void reset()
  {
    schema_rowkey_cnt_ = 0;
    column_cnt_ = 0;
    array_.reset();
  }
  bool is_valid() const
  {
    return (rowkey_mode_ && column_cnt_ > schema_rowkey_cnt_ && schema_rowkey_cnt_ > 0)
      || (!rowkey_mode_ && array_.count() > 0);
  }
  int init(
    const int64_t count,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator);
  int32_t at(int64_t idx) const
  {
    return (*at_func_)(schema_rowkey_cnt_, column_cnt_, idx, array_);
  }

  OB_INLINE int64_t count() const
  {
    return (*count_func_)(column_cnt_, array_);
  }

  int64_t get_deep_copy_size() const;
  int deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObColumnIndexArray &dst_array) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, common::ObIAllocator &allocator);
  int64_t get_serialize_size() const;
  DECLARE_TO_STRING;
private:
  typedef int64_t (*COUNT_FUNC)(uint32_t, const ObFixedMetaObjArray<int32_t> &);
  typedef int32_t (*AT_FUNC)(uint32_t, uint32_t, int64_t, const ObFixedMetaObjArray<int32_t> &);
public:
  const static uint8_t COLUMN_INDEX_ARRAY_VERSION = 1;
  uint8_t version_;
  bool rowkey_mode_;
  bool for_memtable_;
  uint8_t reserved_;
  uint32_t schema_rowkey_cnt_;
  uint32_t column_cnt_;
  ObFixedMetaObjArray<int32_t> array_;
private:
  COUNT_FUNC count_func_;
  AT_FUNC at_func_;
};
class ObITableReadInfo
{
public:
  ObITableReadInfo() = default;
  virtual ~ObITableReadInfo() = default;
  virtual bool is_oracle_mode() const = 0;
  virtual int64_t get_schema_column_count() const = 0;
  virtual int64_t get_seq_read_column_count() const = 0;
  virtual int64_t get_request_count() const = 0;
  virtual int64_t get_schema_rowkey_count() const = 0;
  virtual int64_t get_rowkey_count() const = 0;
  virtual int64_t get_group_idx_col_index() const = 0;
  virtual int64_t get_max_col_index() const = 0;
  virtual int64_t get_trans_col_index() const = 0;
  virtual const common::ObIArray<ObColDesc> &get_columns_desc() const = 0;
  virtual const ObColumnIndexArray &get_columns_index() const = 0;
  virtual const ObColumnIndexArray &get_memtable_columns_index() const = 0;
  virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const = 0;
  virtual const common::ObIArray<ObColumnParam *> *get_columns() const = 0;
  virtual const common::ObIArray<ObColExtend> *get_columns_extend() const = 0;
  virtual const common::ObIArray<int32_t> *get_cg_idxs() const = 0;
  virtual bool is_access_rowkey_only() const = 0;
  virtual bool has_all_column_group() const = 0;
  virtual bool is_valid() const = 0;
  virtual void reset() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObReadInfoStruct : public ObITableReadInfo
{
public:
  ObReadInfoStruct(const bool rowkey_mode = false)
    : ObITableReadInfo(),
      is_inited_(false),
      is_oracle_mode_(false),
      allocator_(nullptr),
      schema_column_count_(0),
      compat_version_(READ_INFO_VERSION_V2),
      reserved_(0),
      schema_rowkey_cnt_(0),
      rowkey_cnt_(0),
      cols_desc_(),
      cols_index_(rowkey_mode, false/*for_memtable*/),
      memtable_cols_index_(rowkey_mode, true/*for_memtable*/),
      datum_utils_()
  {}
  virtual ~ObReadInfoStruct() { reset(); }
  virtual bool is_valid() const override
  {
    return is_inited_
        && schema_rowkey_cnt_ <= cols_desc_.count()
        && 0 < cols_desc_.count()
        && 0 < cols_index_.count()
        && schema_rowkey_cnt_ <= schema_column_count_
        && datum_utils_.is_valid();
  }
  virtual void reset() override;

  OB_INLINE virtual bool is_oracle_mode() const override { return is_oracle_mode_; }
  OB_INLINE virtual int64_t get_schema_column_count() const override { return schema_column_count_; }
  OB_INLINE virtual int64_t get_schema_rowkey_count() const override { return schema_rowkey_cnt_; }
  OB_INLINE virtual int64_t get_rowkey_count() const override { return rowkey_cnt_; }
  OB_INLINE virtual const common::ObIArray<ObColDesc> &get_columns_desc() const override
  { return cols_desc_; }
  OB_INLINE virtual int64_t get_request_count() const override
  { return cols_desc_.count(); }
  OB_INLINE virtual const ObColumnIndexArray &get_columns_index() const override
  { return cols_index_; }
  OB_INLINE virtual const ObColumnIndexArray &get_memtable_columns_index() const override
  { return memtable_cols_index_; }
  OB_INLINE virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const override { return datum_utils_; }
  OB_INLINE virtual int64_t get_group_idx_col_index() const override
  {
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_max_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise max col index");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise trans col index");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_seq_read_column_count() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise seq read column count");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual const common::ObIArray<ObColumnParam *> *get_columns() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise columns array");
    return nullptr;
  }
  OB_INLINE virtual const common::ObIArray<int32_t> *get_cg_idxs() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise cg index array");
    return nullptr;
  }
  OB_INLINE virtual bool is_access_rowkey_only() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise rowkey info");
    return false;
  }
  OB_INLINE virtual const common::ObIArray<ObColExtend> *get_columns_extend() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise columns extend array");
    return nullptr;
  }
  virtual bool has_all_column_group() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise all column group");
    return false;
  }
  DECLARE_TO_STRING;
  int generate_for_column_store(ObIAllocator &allocator,
                                const ObColDesc &desc,
                                const bool is_oracle_mode);
  void init_basic_info(const int64_t schema_column_count,
                       const int64_t schema_rowkey_cnt,
                       const bool is_oracle_mode,
                       const bool is_cg_sstable);
  int prepare_arrays(common::ObIAllocator &allocator,
                     const common::ObIArray<ObColDesc> &cols_desc,
                     const int64_t col_cnt);
  int init_compat_version();
protected:
  const int64_t READ_INFO_VERSION_V0 = 0;
  const int64_t READ_INFO_VERSION_V1 = 1;
  const int64_t READ_INFO_VERSION_V2 = 2;
  bool is_inited_;
  bool is_oracle_mode_;
  ObIAllocator *allocator_;
  // distinguish schema changed by schema column count
  union {
    uint64_t info_;
    struct {
      uint32_t schema_column_count_;
      uint16_t compat_version_;
      uint16_t reserved_;
    };
  };
  int64_t schema_rowkey_cnt_;
  int64_t rowkey_cnt_;
  ColDescArray cols_desc_;
  ObColumnIndexArray cols_index_; // col index in sstable
  ObColumnIndexArray memtable_cols_index_; // there is no multi verison rowkey col in memtable
  blocksstable::ObStorageDatumUtils datum_utils_;
};

class ObTableReadInfo : public ObReadInfoStruct
{
friend class ObTenantCGReadInfoMgr;
public:
  ObTableReadInfo();
  virtual ~ObTableReadInfo();
  virtual void reset() override;
  /*
   * schema_rowkey_cnt: schema row key count
   * cols_desc: access col descs
   * storage_cols_index: access column store index in storage file row
   * cols_param: access column params
   */
  virtual int init(
      common::ObIAllocator &allocator,
      const int64_t schema_column_count,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &cols_desc,
      const common::ObIArray<int32_t> *storage_cols_index,
      const common::ObIArray<ObColumnParam *> *cols_param = nullptr,
      const common::ObIArray<int32_t> *cg_idxs = nullptr,
      const common::ObIArray<ObColExtend> *cols_extend = nullptr,
      const bool has_all_column_group = true,
      const bool is_cg_sstable = false);
  virtual OB_INLINE bool is_valid() const override
  {
    return ObReadInfoStruct::is_valid()
        && cols_desc_.count() == cols_index_.count()
        && schema_rowkey_cnt_ <= seq_read_column_count_
        && seq_read_column_count_ <= cols_desc_.count();
  }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  { return trans_col_index_; }
  OB_INLINE int64_t get_group_idx_col_index() const
  { return group_idx_col_index_; }
  OB_INLINE int64_t get_seq_read_column_count() const
  { return seq_read_column_count_; }
  virtual const common::ObIArray<ObColumnParam *> *get_columns() const
  { return &cols_param_; }
  OB_INLINE bool need_access_cg() const
  { return cg_idxs_.count() > 0; }
  OB_INLINE const common::ObIArray<int32_t> *get_cg_idxs() const override
  { return cg_idxs_.count() > 0 ? &cg_idxs_ : nullptr; }
  OB_INLINE bool is_access_rowkey_only() const override
  { return max_col_index_ < rowkey_cnt_; }

  OB_INLINE virtual const common::ObIArray<ObColExtend> *get_columns_extend() const override
  {
    return &cols_extend_;
  }
  // this func only called in query
  OB_INLINE virtual int64_t get_request_count() const override
  { return cols_desc_.count(); }
  OB_INLINE virtual int64_t get_max_col_index() const override { return max_col_index_; }
  virtual bool has_all_column_group() const override
  { return has_all_column_group_; }
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
  int64_t get_serialize_size() const;
  DECLARE_TO_STRING;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReadInfo);
  int init_datum_utils(common::ObIAllocator &allocator, const bool is_cg_sstable);

private:
  // distinguish schema changed by schema column count
  int64_t trans_col_index_;
  int64_t group_idx_col_index_;
  // the count of common prefix between request columns and store columns
  int64_t seq_read_column_count_;
  int64_t max_col_index_;
  Columns cols_param_;
  CGIndex cg_idxs_;
  ColExtendArray cols_extend_;
  bool has_all_column_group_;
};

class ObRowkeyReadInfo final : public ObReadInfoStruct
{
public:
  ObRowkeyReadInfo();
  virtual ~ObRowkeyReadInfo() {}

  int init(
      common::ObIAllocator &allocator,
      const int64_t schema_column_count,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &rowkey_col_descs,
      const bool is_cg_sstable = false,
      const bool use_default_compat_version = false);
  OB_INLINE virtual int64_t get_seq_read_column_count() const override
  { return get_request_count(); }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  { return schema_rowkey_cnt_; }
  virtual int64_t get_request_count() const override;
  OB_INLINE bool is_access_rowkey_only() const override
  { return false; }
  int deep_copy(char *buf, const int64_t buf_len, ObRowkeyReadInfo *&value) const;
  int64_t get_deep_copy_size() const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
  int64_t get_serialize_size() const;
  DISALLOW_COPY_AND_ASSIGN(ObRowkeyReadInfo);
};

class ObCGReadInfo : public ObITableReadInfo
{
public:
  const static int64_t CG_COL_CNT = 1;
  const static int64_t CG_ROWKEY_COL_CNT = 0;
  const static int64_t LOCAL_MAX_CG_READ_INFO_CNT = 256;
  const static uint64_t CG_READ_INFO_MEMORY_BASE = 32_GB;
  const static int16_t MIX_READ_INFO_LOCAL_CACHE = 1;
public:
  ObCGReadInfo();
  virtual ~ObCGReadInfo();
  OB_INLINE virtual bool is_valid() const override { return nullptr != cg_basic_info_ && cg_basic_info_->is_valid(); }
  OB_INLINE virtual void reset() override
  { // alloc info should be release in ObTenantCGReadInfoMgr, just clear ptr here
    cg_basic_info_ = nullptr;
    cols_param_ = nullptr;
    cols_extend_.reset();
  }
  OB_INLINE static bool is_cg_sstable(const int64_t schema_rowkey_cnt, const int64_t schema_column_count)
  {
    return ObCGReadInfo::CG_COL_CNT == schema_rowkey_cnt && ObCGReadInfo::CG_ROWKEY_COL_CNT == schema_column_count;
  }
  static uint64_t get_local_max_cg_cnt()
  {
    double local_max_cg_read_info_cnt = 0.0;
    double tenant_memory_limit = static_cast<double>(lib::get_tenant_memory_limit(MTL_ID()));
    local_max_cg_read_info_cnt = (tenant_memory_limit / CG_READ_INFO_MEMORY_BASE) * LOCAL_MAX_CG_READ_INFO_CNT;
    return MAX(static_cast<uint64_t>(local_max_cg_read_info_cnt), LOCAL_MAX_CG_READ_INFO_CNT);
  }
  OB_INLINE virtual bool is_oracle_mode() const override { return cg_basic_info_->is_oracle_mode(); }
  OB_INLINE virtual int64_t get_schema_column_count() const override { return CG_COL_CNT; }
  OB_INLINE virtual int64_t get_schema_rowkey_count() const override { return CG_ROWKEY_COL_CNT; }
  OB_INLINE virtual int64_t get_rowkey_count() const override { return CG_ROWKEY_COL_CNT; }
  OB_INLINE virtual const common::ObIArray<ObColDesc> &get_columns_desc() const override
  { return cg_basic_info_->get_columns_desc(); }
  OB_INLINE virtual int64_t get_request_count() const override { return CG_COL_CNT; }
  OB_INLINE virtual const ObColumnIndexArray &get_columns_index() const override
  { return cg_basic_info_->get_columns_index(); }
  OB_INLINE virtual const ObColumnIndexArray &get_memtable_columns_index() const override
  { return cg_basic_info_->get_memtable_columns_index(); }
  OB_INLINE virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const override
  {
    return cg_basic_info_->get_datum_utils();
  }
  OB_INLINE virtual const common::ObIArray<ObColumnParam *> *get_columns() const override
  {
    return cols_param_;
  }
  OB_INLINE virtual const common::ObIArray<ObColExtend> *get_columns_extend() const override
  {
    return &cols_extend_;
  }
  virtual int64_t get_group_idx_col_index() const
  {
    return OB_INVALID_INDEX;
  }
  virtual int64_t get_max_col_index() const
  {
    OB_ASSERT_MSG(false, "ObCGReadInfo dose not promise max col index");
    return OB_INVALID_INDEX;
  }
  virtual int64_t get_trans_col_index() const
  {
    OB_ASSERT_MSG(false, "ObCGReadInfo dose not promise trans col index");
    return OB_INVALID_INDEX;
  }
  virtual int64_t get_seq_read_column_count() const
  {
    return CG_COL_CNT;
  }
  virtual const common::ObIArray<int32_t> *get_cg_idxs() const override
  {
    OB_ASSERT_MSG(false, "ObCGReadInfo dose not promise cg index");
    return nullptr;
  }
  virtual bool is_access_rowkey_only() const override
  {
    OB_ASSERT_MSG(false, "ObCGReadInfo dose not promise rowkey info");
    return false;
  }
  virtual bool has_all_column_group() const override
  {
    OB_ASSERT_MSG(false, "ObCGReadInfo dose not promise all column group");
    return false;
  }
  TO_STRING_KV(K_(need_release), KPC(cg_basic_info_), KPC(cols_param_), K_(cols_extend));
protected:
  friend class ObTenantCGReadInfoMgr;
  bool need_release_;
  ObReadInfoStruct *cg_basic_info_; // for normal cg, only use basic info
  Columns *cols_param_; // for string cg, need different padding col param
  ColExtendArray cols_extend_;
};

class ObCGRowkeyReadInfo : public ObITableReadInfo
{
public:
  ObCGRowkeyReadInfo(const ObRowkeyReadInfo &rowkey_read_info)
    : rowkey_read_info_(rowkey_read_info)
  {}
  virtual ~ObCGRowkeyReadInfo() = default;
  virtual bool is_oracle_mode() const override { return rowkey_read_info_.is_oracle_mode(); }
  virtual int64_t get_schema_column_count() const override { return rowkey_read_info_.get_schema_rowkey_count(); }
  virtual int64_t get_seq_read_column_count() const override
  {
    return get_request_count();
  }
  virtual int64_t get_request_count() const override { return rowkey_read_info_.get_rowkey_count(); }
  virtual int64_t get_schema_rowkey_count() const override { return rowkey_read_info_.get_schema_rowkey_count(); }
  virtual int64_t get_rowkey_count() const override { return rowkey_read_info_.get_rowkey_count(); }
  virtual int64_t get_group_idx_col_index() const override
  {
    return OB_INVALID_INDEX;
  }
  virtual int64_t get_max_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObCGRowkeyReadInfo dose not promise max col index");
    return OB_INVALID_INDEX;
  }
  virtual int64_t get_trans_col_index() const override { return rowkey_read_info_.get_trans_col_index(); }
  virtual const common::ObIArray<ObColDesc> &get_columns_desc() const override
  {
    return rowkey_read_info_.get_columns_desc();
  }
  virtual const ObColumnIndexArray &get_columns_index() const override
  {
    return rowkey_read_info_.get_columns_index();
  }
  virtual const ObColumnIndexArray &get_memtable_columns_index() const override
  {
    return rowkey_read_info_.get_memtable_columns_index();
  }
  virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const override
  {
    return rowkey_read_info_.get_datum_utils();
  }
  virtual const common::ObIArray<ObColumnParam *> *get_columns() const override
  {
    return rowkey_read_info_.get_columns();
  }
  OB_INLINE virtual const common::ObIArray<int32_t> *get_cg_idxs() const override
  {
    OB_ASSERT_MSG(false, "ObCGRowkeyReadInfo dose not promise cg index array");
    return nullptr;
  }
  OB_INLINE virtual bool is_access_rowkey_only() const override
  { return true; }
  virtual const common::ObIArray<ObColExtend> *get_columns_extend() const override
  {
    return rowkey_read_info_.get_columns_extend();
  }
  virtual bool has_all_column_group() const override
  {
    OB_ASSERT_MSG(false, "ObCGRowkeyReadInfo dose not promise all column group");
    return false;
  }
  virtual bool is_valid() const override { return rowkey_read_info_.is_valid(); }
  virtual void reset() { OB_ASSERT_MSG(false, "ObCGRowkeyReadInfo dose not allow reset"); }
  TO_STRING_KV(K_(rowkey_read_info))
private:
  const ObRowkeyReadInfo &rowkey_read_info_;
  DISALLOW_COPY_AND_ASSIGN(ObCGRowkeyReadInfo);
};

struct ObCGReadInfoHandle final
{
public:
  ObCGReadInfoHandle()
    : cg_read_info_(nullptr)
  {}
  ~ObCGReadInfoHandle();
  void reset();
  bool is_valid() const { return nullptr != cg_read_info_; }
  void set_read_info(ObCGReadInfo *cg_read_info) { cg_read_info_ = cg_read_info; }
  const ObCGReadInfo *get_read_info() const { return cg_read_info_; }
private:
  ObCGReadInfo *cg_read_info_;

  DISALLOW_COPY_AND_ASSIGN(ObCGReadInfoHandle);
};

class ObTenantCGReadInfoMgr final
{
public:
  ObTenantCGReadInfoMgr();
  ~ObTenantCGReadInfoMgr();
  int init();
  void destroy();
  int get_index_read_info(const ObITableReadInfo *&index_read_info);
  int get_cg_read_info(const ObColDesc &col_desc, ObColumnParam *col_param,
                       const ObTabletID &tablet_id, ObCGReadInfoHandle &cg_read_info_handle);
  int release_cg_read_info(ObCGReadInfo *&cg_read_info);
  static int mtl_init(ObTenantCGReadInfoMgr *&read_info_mgr);
  static int construct_index_read_info(common::ObIAllocator &allocator, ObRowkeyReadInfo &index_read_info);
  static int construct_cg_read_info(
      common::ObIAllocator &allocator,
      const bool is_oracle_mode,
      const ObColDesc &col_desc,
      ObColumnParam *cols_param,
      ObTableReadInfo &cg_read_info);
  int gc_cg_info_array();
private:
  int construct_normal_cg_read_infos();
  int alloc_spec_cg_read_info(const ObColDesc &col_desc, const ObColumnParam *col_param, ObCGReadInfoHandle &cg_read_info_handle);
  void free_cg_info(ObCGReadInfo *&cg_read_info, const bool free_ptr_flag);
  static void set_col_desc(const ObObjType type, ObColDesc &col_desc);
  static bool skip_type(const ObObjType type);
  static bool not_in_normal_cg_array(const ObObjType type)
  {
    return skip_type(type) || is_lob_storage(type) || ob_is_string_type(type) || ob_is_decimal_int(type);
  }
  const static int64_t DEFAULT_CG_READ_INFO_ARRAY_SIZE = 256;
  const static int64_t PRINT_LOG_INVERVAL = 3 * 60 * 1000L * 1000L; // 3 mins
  void inner_print_log();
private:
  common::ObFIFOAllocator allocator_;
  ObRowkeyReadInfo index_read_info_;
  ObFixedArray<ObCGReadInfo *, ObIAllocator> normal_cg_read_infos_;
  ObSEArray<ObCGReadInfo *, DEFAULT_CG_READ_INFO_ARRAY_SIZE> release_cg_read_info_array_;
  lib::ObMutex lock_;
  int64_t hold_cg_read_info_cnt_; // wait dec_cnt to 0 before destroy
  int64_t in_progress_cnt_; // wait dec_cnt to 0 before destroy
  void *alloc_buf_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantCGReadInfoMgr);
};

}
}
#endif //OB_STORAGE_ACCESS_TABLE_READ_INFO_H_
