/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ORC_TABLE_ROW_ITER_H
#define OB_ORC_TABLE_ROW_ITER_H

#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include <orc/OrcFile.hh>
#include <orc/MemoryPool.hh>
#include <orc/Writer.hh>
#include <orc/Reader.hh>
#include <orc/Int128.hh>
#include <orc/Common.hh>

namespace oceanbase {
namespace sql {
  class ObOrcMemPool : public orc::MemoryPool {
    public:
      void init(uint64_t tenant_id) {
        mem_attr_ = ObMemAttr(tenant_id, "OrcMemPool");
      }

      virtual char* malloc(uint64_t size) override {
        int ret = OB_SUCCESS;
        void *buf = ob_malloc_align(64, size, mem_attr_);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(size), K(lbt()));
          throw std::bad_alloc();
        }
        return (char*)buf;
      }

      virtual void free(char* p) override {
        if (OB_ISNULL(p)) {
          throw std::bad_exception();
        }
        ob_free_align(p);
      }

    private:
      common::ObMemAttr mem_attr_;
  };

  class ObOrcFileAccess : public orc::InputStream {
    public:
      ObOrcFileAccess(ObExternalDataAccessDriver &file_reader, const char* file_name, int64_t len)
      : file_reader_(file_reader), file_name_(file_name), total_length_(len), fileName_(file_name) {
      }

      uint64_t getLength() const  {
        return total_length_;
      }

      uint64_t getNaturalReadSize() const  {
        return 128 * 1024;
      }

      void read(void* buf,
                uint64_t length,
                uint64_t offset) override  {
        int64_t bytesRead = 0;
        int ret = file_reader_.pread(buf, length, offset, bytesRead);
        if (ret != OB_SUCCESS) {
          throw std::bad_exception();
        }
        LOG_TRACE("read file access", K(file_name_), K(bytesRead));
      }

      const std::string& getName() const override {
        return fileName_;
      }

    private:
      ObExternalDataAccessDriver &file_reader_;
      const char* file_name_;
      uint64_t total_length_;
      std::string fileName_;
  };

  struct StripeInformation {
    /// \brief Offset of the stripe from the start of the file, in bytes
    int64_t offset;
    /// \brief Length of the stripe, in bytes
    int64_t length;
    /// \brief Number of rows in the stripe
    int64_t num_rows;
    /// \brief Index of the first row of the stripe
    int64_t first_row_id;

    TO_STRING_KV(K(offset), K(length), K(num_rows), K(first_row_id));
  };

  class ObOrcTableRowIterator : public ObExternalTableRowIterator {
  public:
    struct StateValues {
      StateValues() :
        file_idx_(0),
        part_id_(0),
        cur_file_id_(0),
        cur_file_url_(),
        cur_stripe_idx_(0),
        end_stripe_idx_(-1),
        cur_stripe_read_row_count_(0),
        cur_stripe_row_count_(0),
        batch_size_(128),
        part_list_val_() {}
      void reuse() {
        file_idx_ = 0;
        part_id_ = 0;
        cur_file_id_ = 0;
        cur_stripe_idx_ = 0;
        end_stripe_idx_ = -1;
        cur_stripe_read_row_count_ = 0;
        cur_stripe_row_count_ = 0;
        cur_file_url_.reset();
        part_list_val_.reset();
      }
      int64_t file_idx_;
      int64_t part_id_;
      int64_t cur_file_id_;
      ObString cur_file_url_;
      int64_t cur_stripe_idx_;
      int64_t end_stripe_idx_;
      int64_t cur_stripe_read_row_count_;
      int64_t cur_stripe_row_count_;
      int64_t batch_size_;
      ObNewRow part_list_val_;
    };
  public:
  ObOrcTableRowIterator() : file_column_exprs_(allocator_), file_meta_column_exprs_(allocator_), bit_vector_cache_(NULL) {}
  virtual ~ObOrcTableRowIterator() {

  }

  int init(const storage::ObTableScanParam *scan_param) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }

  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;

  virtual void reset() override;
private:
  // load vec data from orc file to expr mem
  struct DataLoader {
    DataLoader(ObEvalCtx &eval_ctx,
               ObExpr *file_col_expr,
               std::unique_ptr<orc::ColumnVectorBatch> &batch,
               const int64_t batch_size,
               const ObIArray<int> &idxs,
               int64_t &row_count):
      eval_ctx_(eval_ctx),
      file_col_expr_(file_col_expr),
      batch_(batch),
      batch_size_(batch_size),
      idxs_(idxs),
      row_count_(row_count)
    {}
    typedef int (DataLoader::*LOAD_FUNC)();
    static LOAD_FUNC select_load_function(const ObDatumMeta &datum_type,
                                          const orc::Type &type);
    int load_data_for_col(LOAD_FUNC &func);
    int load_string_col();
    int load_int32_vec();
    int load_int64_vec();
    int load_timestamp_vec();
    int load_date_to_time_or_stamp();
    int load_float();
    int load_double();
    int load_dec128_vec();
    int load_dec64_vec();

    bool is_orc_read_utc();
    bool is_ob_type_store_utc(const ObDatumMeta &meta);

    int64_t calc_tz_adjust_us();
    ObEvalCtx &eval_ctx_;
    ObExpr *file_col_expr_;
    std::unique_ptr<orc::ColumnVectorBatch> &batch_;
    const int64_t batch_size_;
    const ObIArray<int> &idxs_;
    int64_t &row_count_;
  };
  private:
    int next_file();
    int next_stripe();
    int build_type_name_id_map(const orc::Type* type, ObIArray<ObString> &col_names);
    int to_dot_column_path(ObIArray<ObString> &col_names, ObString &path);
    int get_data_column_batch_idxs(const orc::Type *type, const int col_id, ObIArray<int> &idxs);
  private:

    StateValues state_;
    lib::ObMemAttr mem_attr_;
    ObArenaAllocator allocator_;
    ObOrcMemPool orc_alloc_;
    std::unique_ptr<orc::Reader> reader_;
    std::unique_ptr<orc::RowReader> row_reader_;
    common::ObArrayWrap<StripeInformation> stripes_;
    ObExternalDataAccessDriver data_access_driver_;
    common::ObArrayWrap<int> column_indexs_; //for getting statistics, may useless now.
    ExprFixedArray file_column_exprs_; //column value from parquet file
    ExprFixedArray file_meta_column_exprs_; //column value from file meta
    common::ObArrayWrap<DataLoader::LOAD_FUNC> load_funcs_;
    ObSqlString url_;
    ObBitVector *bit_vector_cache_;
    common::ObArrayWrap<char *> file_url_ptrs_; //for file url expr
    common::ObArrayWrap<ObLength> file_url_lens_; //for file url expr
    hash::ObHashMap<int64_t, const orc::Type*> id_to_type_;
    hash::ObHashMap<ObString, int64_t> name_to_id_;

};

}
}

#endif