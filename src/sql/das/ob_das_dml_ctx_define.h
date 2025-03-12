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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_DML_CTX_DEFINE_H_
#define DEV_SRC_SQL_DAS_OB_DAS_DML_CTX_DEFINE_H_
#include "common/row/ob_row_iterator.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_define.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "sql/engine/ob_operator.h"
#include "sql/resolver/dml/ob_hint.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
namespace oceanbase
{
namespace storage
{
class ObDMLBaseParam;
}
namespace sql
{
typedef common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> ObjMetaFixedArray;
typedef common::ObFixedArray<common::ObAccuracy, common::ObIAllocator> AccuracyFixedArray;
static const int64_t SAPTIAL_INDEX_DEFAULT_ROW_COUNT = 32; // 一个wkb生成的cellid数量（设定值）
static const int64_t SAPTIAL_INDEX_DEFAULT_COL_COUNT = 2;
typedef common::ObSEArray<blocksstable::ObDatumRow*, SAPTIAL_INDEX_DEFAULT_ROW_COUNT> ObDomainIndexRow;

class ObDomainDMLIterator;
class ObFTDocWordInfo;
struct ObDASDMLBaseRtDef;
//das dml base compile info definition
struct ObDASDMLBaseCtDef : ObDASBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  INHERIT_TO_STRING_KV("ObDASBaseDef", ObDASBaseCtDef,
                       K_(table_id),
                       K_(index_tid),
                       K_(schema_version),
                       K_(rowkey_cnt),
                       K_(spk_cnt),
                       K_(column_ids),
                       K_(column_types),
                       K_(column_accuracys),
                       K_(old_row_projector),
                       K_(new_row_projector),
                       K_(is_total_quantity_log),
                       K_(is_ignore),
                       K_(is_batch_stmt),
                       K_(is_insert_up),
                       K_(is_table_api),
                       K_(is_main_table_in_fts_ddl),
                       K_(tz_info),
                       K_(table_param),
                       K_(encrypt_meta));
  uint64_t table_id_;
  uint64_t index_tid_;
  int64_t schema_version_;
  int64_t rowkey_cnt_;
  int64_t spk_cnt_;
  UIntFixedArray column_ids_; //write row to storage column info
  ObjMetaFixedArray column_types_;
  AccuracyFixedArray column_accuracys_;
  IntFixedArray old_row_projector_;
  IntFixedArray new_row_projector_;
  common::ObTimeZoneInfo tz_info_;
  share::schema::ObTableDMLParam table_param_;
  common::ObFixedArray<transaction::ObEncryptMetaCache, common::ObIAllocator> encrypt_meta_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_total_quantity_log_           : 1;
      uint64_t is_ignore_                       : 1;
      uint64_t is_batch_stmt_                   : 1;
      uint64_t is_insert_up_                    : 1;
      uint64_t is_table_api_                    : 1;
      uint64_t is_access_mlog_as_master_table_  : 1;
      uint64_t is_access_vidx_as_master_table_  : 1; // FARM COMPAT WHITELIST for 4_2_1_release compatibility
      uint64_t is_update_partition_key_         : 1; // FARM COMPAT WHITELIST for 4_2_1_release compatibility
      uint64_t is_update_uk_                    : 1;
      uint64_t is_update_pk_with_dop_           : 1; // update primary_table PK
      uint64_t is_main_table_in_fts_ddl_        : 1; // main table is in fts ddl for mode of unstable ftparser.
      uint64_t is_update_pk_                    : 1;
      uint64_t reserved_                        : 49; //add new flag before reserved_
      uint64_t compat_version_                  : 4; //prohibited to insert new flags between compat_version_ and reserved_
    };
  };
protected:
  ObDASDMLBaseCtDef(common::ObIAllocator &alloc, ObDASOpType op_type)
    : ObDASBaseCtDef(op_type),
      table_id_(common::OB_INVALID_ID),
      index_tid_(common::OB_INVALID_ID),
      schema_version_(-1),
      rowkey_cnt_(0),
      spk_cnt_(0),
      column_ids_(alloc),
      column_types_(alloc),
      column_accuracys_(alloc),
      old_row_projector_(alloc),
      new_row_projector_(alloc),
      tz_info_(),
      table_param_(alloc),
      encrypt_meta_(alloc),
      flags_(0)
  {
    compat_version_ = 1; //notify observer to use new flags after 4.2.5.2
  }
};

typedef common::ObFixedArray<ObDASDMLBaseCtDef*, common::ObIAllocator> DASDMLCtDefArray;
typedef common::ObArrayWrap<ObDASDMLBaseRtDef*> DASDMLRtDefArray;
struct ObDASDMLBaseRtDef : ObDASBaseRtDef
{
  OB_UNIS_VERSION(1);
public:
  INHERIT_TO_STRING_KV("ObDASBaseRtDef", ObDASBaseRtDef,
                       K_(timeout_ts),
                       K_(sql_mode),
                       K_(prelock),
                       K_(tenant_schema_version),
                       K_(is_for_foreign_key_check),
                       K_(affected_rows),
                       K_(is_immediate_row_conflict_check));
  int64_t timeout_ts_;
  ObSQLMode sql_mode_;
  bool prelock_;
  int64_t tenant_schema_version_;
  bool is_for_foreign_key_check_;
  int64_t affected_rows_;
  const DASDMLCtDefArray *related_ctdefs_;
  DASDMLRtDefArray *related_rtdefs_;
  bool is_immediate_row_conflict_check_;
protected:
  ObDASDMLBaseRtDef(ObDASOpType op_type)
    : ObDASBaseRtDef(op_type),
      timeout_ts_(-1),
      sql_mode_(DEFAULT_OCEANBASE_MODE),
      prelock_(false),
      tenant_schema_version_(0),
      is_for_foreign_key_check_(false),
      affected_rows_(0),
      related_ctdefs_(nullptr),
      related_rtdefs_(nullptr),
      is_immediate_row_conflict_check_(true)
  { }
};

struct ObDASInsCtDef : ObDASDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASInsCtDef(common::ObIAllocator &alloc)
    : ObDASDMLBaseCtDef(alloc, DAS_OP_TABLE_INSERT),
      table_rowkey_cids_(alloc),
      table_rowkey_types_(alloc)
  { }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASDMLBaseCtDef,
                         K_(table_rowkey_cids),
                         K_(table_rowkey_types));

  UIntFixedArray table_rowkey_cids_; //主表主键column_ids
  ObjMetaFixedArray table_rowkey_types_;
};
typedef DASDMLCtDefArray DASInsCtDefArray;

struct ObDASInsRtDef : ObDASDMLBaseRtDef
{
  OB_UNIS_VERSION(1);

public:
  ObDASInsRtDef()
    : ObDASDMLBaseRtDef(DAS_OP_TABLE_INSERT),
      need_fetch_conflict_(false),
      is_duplicated_(false),
      direct_insert_task_id_(0),
      use_put_(false),
      ddl_task_id_(0)
  { }

  INHERIT_TO_STRING_KV("ObDASBaseRtDef", ObDASDMLBaseRtDef,
                       K_(need_fetch_conflict),
                       K_(is_duplicated),
                       K_(direct_insert_task_id),
                       K_(use_put),
                       K_(ddl_task_id));

  // used to check whether need to fetch_duplicate_key, will set in table_replace_op
  bool need_fetch_conflict_;
  // used to check whether duplicate_key error occurred, will be set in das_insert_op
  // not need to serialize
  bool is_duplicated_;
  // used in direct-insert mode
  int64_t direct_insert_task_id_;
  // use put, only use in obkv for overlay writting.
  bool use_put_;
  int64_t ddl_task_id_;
};
typedef DASDMLRtDefArray DASInsRtDefArray;

struct ObDASUpdCtDef : ObDASDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASUpdCtDef(common::ObIAllocator &alloc)
    : ObDASDMLBaseCtDef(alloc, DAS_OP_TABLE_UPDATE),
      updated_column_ids_(alloc)
  { }
  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASDMLBaseCtDef,
                       K_(updated_column_ids));
  UIntFixedArray updated_column_ids_;
};
typedef DASDMLCtDefArray DASUpdCtDefArray;

struct ObDASUpdRtDef : ObDASDMLBaseRtDef
{
  ObDASUpdRtDef()
    : ObDASDMLBaseRtDef(DAS_OP_TABLE_UPDATE)
  { }
};
typedef DASDMLRtDefArray DASUpdRtDefArray;

struct ObDASDelCtDef : ObDASDMLBaseCtDef
{
  ObDASDelCtDef(common::ObIAllocator &alloc)
    : ObDASDMLBaseCtDef(alloc, DAS_OP_TABLE_DELETE)
  { }
};
typedef DASDMLCtDefArray DASDelCtDefArray;

struct ObDASDelRtDef : ObDASDMLBaseRtDef
{
  ObDASDelRtDef()
    : ObDASDMLBaseRtDef(DAS_OP_TABLE_DELETE)
  { }
};
typedef DASDMLRtDefArray DASDelRtDefArray;

struct ObDASLockCtDef : ObDASDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASLockCtDef(common::ObIAllocator &alloc)
    : ObDASDMLBaseCtDef(alloc, DAS_OP_TABLE_LOCK),
      lock_flag_(storage::LF_NONE)
  { }

  INHERIT_TO_STRING_KV("ObDASDMLBaseCtDef", ObDASDMLBaseCtDef,
                       K_(lock_flag));
  storage::ObLockFlag lock_flag_;
};

struct ObDASLockRtDef : ObDASDMLBaseRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASLockRtDef()
    : ObDASDMLBaseRtDef(DAS_OP_TABLE_LOCK),
      for_upd_wait_time_(-1)
  { }

  int64_t for_upd_wait_time_;
};

class ObDASWriteBuffer
{
public:
  typedef ObChunkDatumStore::StoredRow DmlRow;
  typedef ObChunkDatumStore::ShadowStoredRow DatumShadowStoredRow;

  class DmlShadowRow : public ObChunkDatumStore::ShadowStoredRow
  {
    template <int N, typename DMLIterator>
    friend class ObDASIndexDMLAdaptor;
  public:
    DmlShadowRow()
      : DatumShadowStoredRow(),
        column_types_(nullptr),
        reserved_buffer_(nullptr),
        total_reserved_size_(0),
        reserve_datum_buf_(false),
        strip_lob_locator_(false)
    { }
    virtual ~DmlShadowRow() { }
    int init(common::ObIAllocator &allocator, int64_t datum_cnt, bool strip_lob_locator);
    //prepare a stored row to shadow copy ObObj from ObNewRow
    int init(common::ObIAllocator &allocator,
             const common::ObIArray<common::ObObjMeta> &col_types,
             bool strip_lob_locator);
    virtual int shadow_copy(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx) override;
    int shadow_copy(const blocksstable::ObDatumRow &row);
    // reset && release referenced memory
    virtual void reset()
    {
      DatumShadowStoredRow::reset();
      column_types_ = nullptr;
      reserved_buffer_ = nullptr;
      total_reserved_size_ = 0;
      reserve_datum_buf_ = false;
      strip_lob_locator_ = false;
    }
    // reset && NOT release referenced memory
    virtual void reuse()
    {
      DatumShadowStoredRow::reuse();
      reset_datum_ptr();
    }

    void reset_datum_ptr()
    {
      if (nullptr != store_row_ && reserve_datum_buf_ &&
          reserved_buffer_ != nullptr && column_types_ != nullptr) {
        int64_t reserved_offset = 0;
        for (int64_t i = 0; i < column_types_->count(); ++i) {
          ObObjDatumMapType obj_datum_map = ObDatum::get_obj_datum_map_type(
              column_types_->at(i).get_type());
          if (OB_LIKELY(OBJ_DATUM_NULL != obj_datum_map)) {
            store_row_->cells()[i].ptr_ = reserved_buffer_ + reserved_offset;
            reserved_offset += ObDatum::get_reserved_size(obj_datum_map);
          }
        }
      }
    }
  private:
    const common::ObIArray<ObObjMeta> *column_types_;
    char *reserved_buffer_;
    int64_t total_reserved_size_;
    bool reserve_datum_buf_;
    bool strip_lob_locator_;
  };

  class Iterator
  {
  public:
    friend class ObDASWriteBuffer;
    Iterator() :
      cur_row_(nullptr),
      datum_iter_(nullptr)
    { }
    ~Iterator() { }
    int get_next_row(const ObChunkDatumStore::StoredRow *&sr);
    int get_next_row_skip_const(ObEvalCtx &ctx, const common::ObIArray<ObExpr*> &exprs);
  private:
    DmlRow *cur_row_;
    ObChunkDatumStore::Iterator *datum_iter_;
  };

  class NewRowIterator
  {
  public:
    friend class ObDASWriteBuffer;
    NewRowIterator() :
      cur_row_(nullptr),
      datum_iter_(nullptr),
      col_types_(nullptr),
      cur_new_row_(nullptr)
    { }
    ~NewRowIterator() { }

    int get_next_row(blocksstable::ObDatumRow *&row);
    inline bool is_inited() { return nullptr != col_types_; }
  private:
    DmlRow *cur_row_;
    ObChunkDatumStore::Iterator *datum_iter_;
    const common::ObIArray<ObObjMeta> *col_types_;
    blocksstable::ObDatumRow *cur_new_row_;
  };

  OB_UNIS_VERSION(1);
  friend class Iterator;
  friend class NewRowIterator;

private:
  typedef struct {
    DmlRow *next_;
  } LinkNode;
  typedef struct {
    LinkNode header_;
    LinkNode tailer_;
    int64_t size_;
    int64_t mem_used_;
  } BufferList;

public:
  static const int32_t ROW_HEAD_SIZE = sizeof(DmlRow);

  ObDASWriteBuffer();
  ~ObDASWriteBuffer();

  int init(common::ObIAllocator &das_alloc,
           uint32_t row_extend_size = 0,
           uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           const char *label = "DasWriteBuffer",
           int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID);
  OB_INLINE bool is_inited() const { return das_alloc_ != nullptr; }
  inline int64_t get_mem_used() const
  {
    return buffer_list_.mem_used_ + (datum_store_ != nullptr ? datum_store_->get_mem_used() : 0);
  }
  inline int64_t get_row_cnt() const
  {
    return buffer_list_.size_ + (datum_store_ != nullptr ? datum_store_->get_row_cnt() : 0);
  }
  inline int64_t get_column_cnt() const
  {
    return buffer_list_.header_.next_ != nullptr ? buffer_list_.header_.next_->cnt_ : 0;
  }
  inline uint64_t get_tenant_id() const { return mem_attr_.tenant_id_; }
  int add_row(const common::ObIArray<ObExpr*> &exprs,
              ObEvalCtx *ctx,
              DmlRow *&stored_row,
              bool strip_lob_locator);
  int add_row(const DmlShadowRow &sr, DmlRow **stored_row = nullptr);

  int try_add_row(const common::ObIArray<ObExpr*> &exprs,
                  ObEvalCtx *ctx,
                  const int64_t memory_limit,
                  DmlRow *&stored_row,
                  bool &row_added,
                  bool strip_lob_locator);
  int try_add_row(const DmlShadowRow &sr, const int64_t memory_limit, bool &row_added, DmlRow **stored_row = nullptr);
  int begin(Iterator &it);
  int begin(NewRowIterator &it, const common::ObIArray<common::ObObjMeta> &col_types);

  int dump_data(const ObDASDMLBaseCtDef &das_base_ctdef) const;
  uint32_t get_row_extend_size() { return row_extend_size_; }

  TO_STRING_KV(K_(mem_attr),
               "buffer_memory", buffer_list_.mem_used_,
               "buffer_size", buffer_list_.size_,
               KPC_(datum_store));
private:
  int create_link_buffer(int64_t row_size, DmlRow *&row_buffer);
  int create_datum_store();
  int add_row_to_dlist(const common::ObIArray<ObExpr*> &exprs,
                       ObEvalCtx *ctx,
                       int64_t row_size,
                       bool &row_added);
  int add_row_to_dlist(const ObChunkDatumStore::ShadowStoredRow &sr, bool &row_added, DmlRow **stored_row);
  int add_row_to_store(const common::ObIArray<ObExpr*> &exprs,
                       ObEvalCtx *ctx,
                       const int64_t memory_limit,
                       bool &row_added);
  int add_row_to_store(const ObChunkDatumStore::ShadowStoredRow &sr,
                       const int64_t memory_limit,
                       bool &row_added,
                       ObChunkDatumStore::StoredRow **stored_sr);
  int add_row_to_store(const ObChunkDatumStore::ShadowStoredRow &sr,
                       ObChunkDatumStore::StoredRow **stored_sr);
  static DmlRow *get_next_dml_row(DmlRow *cur_row);
  int serialize_buffer_list(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_buffer_list_serialize_size() const;
  int deserialize_buffer_list(const char *buf, const int64_t data_len, int64_t &pos);
  int get_stored_row_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t &size);
  int init_dml_shadow_row(int64_t column_cnt, bool strip_lob_locator);
public:
  const static uint32_t DAS_ROW_DEFAULT_EXTEND_SIZE = 16;
  const static uint32_t DAS_ROW_TRANS_STRING_SIZE = 128;
  const static uint32_t DAS_WITH_TRANS_INFO_EXTEND_SIZE =
      DAS_ROW_DEFAULT_EXTEND_SIZE + sizeof(int32_t) + DAS_ROW_TRANS_STRING_SIZE;
private:
  static const int64_t DAS_WRITE_ROW_LIST_LEN = 128;
private:
  common::ObIAllocator *das_alloc_;
  common::ObMemAttr mem_attr_;
  BufferList buffer_list_;
  ObChunkDatumStore *datum_store_;
  DmlShadowRow *dml_shadow_row_;
  uint32_t row_extend_size_;
};

class ObDASDMLIterator : public blocksstable::ObDatumRowIterator
{
public:
  static const int64_t DEFAULT_BATCH_SIZE = 256;
public:
  ObDASDMLIterator(const ObDASDMLBaseCtDef *das_ctdef,
                   ObDASWriteBuffer &write_buffer,
                   common::ObIAllocator &alloc)
    : write_buffer_(write_buffer),
      das_ctdef_(das_ctdef),
      row_projector_(nullptr),
      allocator_(alloc),
      cur_datum_row_(nullptr),
      cur_datum_rows_(nullptr),
      main_ctdef_(das_ctdef),
      domain_iter_(nullptr),
      ft_doc_word_info_(nullptr)
  {
    set_ctdef(das_ctdef);
    batch_size_ = MIN(write_buffer_.get_row_cnt(), DEFAULT_BATCH_SIZE);
  }
  virtual ~ObDASDMLIterator();
  virtual int get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  virtual int get_next_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count);
  ObDASWriteBuffer &get_write_buffer() { return write_buffer_; }
  virtual void reset() override { }
  int rewind(const ObDASDMLBaseCtDef *das_ctdef, const ObFTDocWordInfo *ft_doc_word_info);

private:
  void set_ctdef(const ObDASDMLBaseCtDef *das_ctdef);
  int get_next_domain_index_row(blocksstable::ObDatumRow *&row);
  int get_next_domain_index_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count);
private:
  ObDASWriteBuffer &write_buffer_;
  const ObDASDMLBaseCtDef *das_ctdef_;
  const IntFixedArray *row_projector_;
  ObDASWriteBuffer::Iterator write_iter_;
  common::ObIAllocator &allocator_;
  blocksstable::ObDatumRow *cur_datum_row_;
  blocksstable::ObDatumRow *cur_datum_rows_;
  const ObDASDMLBaseCtDef *main_ctdef_;
  ObDomainDMLIterator *domain_iter_;
  const ObFTDocWordInfo *ft_doc_word_info_;
  int64_t batch_size_;
};

class ObDASMLogDMLIterator : public blocksstable::ObDatumRowIterator
{
public:
  // support get next datum row
  ObDASMLogDMLIterator(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const storage::ObDMLBaseParam &dml_param,
      ObDatumRowIterator *iter,
      ObDASOpType op_type)
    : ls_id_(ls_id),
      tablet_id_(tablet_id),
      dml_param_(dml_param),
      row_iter_(iter),
      op_type_(op_type),
      is_old_row_(false)
  {
    if ((DAS_OP_TABLE_UPDATE == op_type_)
        || (DAS_OP_TABLE_INSERT == op_type_)) {
      is_old_row_ = true;
    }
  }
  virtual ~ObDASMLogDMLIterator() {}
  virtual int get_next_row(blocksstable::ObDatumRow *&datum_row) override;

private:
  const share::ObLSID &ls_id_;
  const ObTabletID &tablet_id_;
  const storage::ObDMLBaseParam &dml_param_;
  ObDatumRowIterator *row_iter_;
  ObDASOpType op_type_;
  bool is_old_row_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_DML_CTX_DEFINE_H_ */
