/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ICEBERG_DELETE_BITMAP_BUILDER_H
#define OB_ICEBERG_DELETE_BITMAP_BUILDER_H

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "lib/string/ob_string.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_file_prebuffer.h"

#include <orc/Reader.hh>
#include <parquet/api/reader.h>

namespace oceanbase
{
namespace sql
{
class ObIcebergDeleteBitmapBuilder
{
  static uint32_t decode_big_endian_uint32(const char *buf);
  static int get_orc_iceberg_field_id(const orc::Type &type, int64_t &field_id);
  static int find_orc_position_delete_fields(const orc::Type &root_type,
                                             bool require_file_path,
                                             int64_t &file_path_idx,
                                             int64_t &pos_idx);
  static int find_parquet_position_delete_columns(const parquet::FileMetaData &file_meta,
                                                  int &file_path_column_idx,
                                                  int &pos_column_idx);
  static void prune_parquet_position_delete_row_group(
      const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
      int file_path_column_idx,
      const common::ObString &data_file_path,
      bool &skip_row_group,
      bool &range_search_finished);
  static void prune_orc_position_delete_stripe(const orc::Reader &delete_reader,
                                               int64_t stripe_idx,
                                               int64_t file_path_column_id,
                                               const common::ObString &data_file_path,
                                               bool &skip_stripe,
                                               bool &range_search_finished);
  static int match_position_delete_file_path(const common::ObString &data_file_path,
                                             const char *read_file_path,
                                             int64_t read_file_path_length,
                                             bool &is_match,
                                             bool &range_search_finished);
  static int add_delete_positions(const int64_t *positions,
                                  int64_t position_count,
                                  int64_t data_file_record_count,
                                  const common::ObString &data_file_path,
                                  common::ObRoaringBitmap *delete_bitmap);

public:
  class IDeleteFileReader
  {
  public:
    virtual ~IDeleteFileReader() = default;

    virtual int init() = 0;
    virtual void reset() = 0;
    virtual int open_delete_file(const ObLakeDeleteFile &delete_file_path,
                                 ObExternalFileAccess &file_access_driver,
                                 ObFilePreBuffer &file_prebuffer,
                                 const storage::ObTableScanParam *scan_param,
                                 ObExternalTableAccessOptions *options)
        = 0;

    virtual int read_delete_file(const ObString &data_file_path,
                                 const storage::ObTableScanParam *scan_param,
                                 int64_t data_file_record_count,
                                 ObRoaringBitmap *delete_bitmap)
        = 0;
  };

  class OrcDeleteFileReader : public IDeleteFileReader
  {
  public:
    OrcDeleteFileReader()
        : delete_reader_(nullptr), delete_row_reader_(nullptr), delete_data_batch_(nullptr),
          file_path_batch_idx_(-1), pos_batch_idx_(-1), file_path_column_id_(-1),
          is_file_scoped_(false)
    {
    }

    int init() override;
    void reset() override;
    int open_delete_file(const ObLakeDeleteFile &delete_file,
                         ObExternalFileAccess &file_access_driver,
                         ObFilePreBuffer &file_prebuffer,
                         const storage::ObTableScanParam *scan_param,
                         ObExternalTableAccessOptions *options) override;

    int read_delete_file(const ObString &data_file_path,
                         const storage::ObTableScanParam *scan_param,
                         int64_t data_file_record_count,
                         ObRoaringBitmap *delete_bitmap) override;

  private:
    int add_path_scoped_positions(const orc::LongVectorBatch &position_batch,
                                  const orc::StringVectorBatch &file_path_batch,
                                  int64_t row_count,
                                  const common::ObString &data_file_path,
                                  int64_t data_file_record_count,
                                  common::ObRoaringBitmap *delete_bitmap,
                                  bool &range_search_finished) const;
    int process_position_batch(const common::ObString &data_file_path,
                               int64_t data_file_record_count,
                               common::ObRoaringBitmap *delete_bitmap,
                               bool &range_search_finished) const;

    std::unique_ptr<orc::Reader> delete_reader_;
    std::unique_ptr<orc::RowReader> delete_row_reader_;
    std::unique_ptr<orc::ColumnVectorBatch> delete_data_batch_;
    ObOrcMemPool orc_alloc_;
    int64_t file_path_batch_idx_;
    int64_t pos_batch_idx_;
    int64_t file_path_column_id_;
    bool is_file_scoped_;
  };

  class ParquetDeleteFileReader : public IDeleteFileReader
  {
  public:
    ParquetDeleteFileReader()
        : delete_file_reader_(nullptr), read_props_(&arrow_alloc_), file_path_column_idx_(-1),
          pos_column_idx_(-1), is_file_scoped_(false)
    {
    }

    int init() override;
    void reset() override;
    int open_delete_file(const ObLakeDeleteFile &delete_file,
                         ObExternalFileAccess &file_access_driver,
                         ObFilePreBuffer &file_prebuffer,
                         const storage::ObTableScanParam *scan_param,
                         ObExternalTableAccessOptions *options) override;

    int read_delete_file(const ObString &data_file_path,
                         const storage::ObTableScanParam *scan_param,
                         int64_t data_file_record_count,
                         ObRoaringBitmap *delete_bitmap) override;

  private:
    int find_matching_row_range(const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
                                const common::ObString &data_file_path,
                                int row_group_idx,
                                int64_t batch_size,
                                common::ObArrayWrap<parquet::ByteArray> &file_path_values,
                                int64_t &begin_file_row,
                                int64_t &end_file_row,
                                bool &range_search_finished) const;
    int read_and_add_position_range(
        const std::shared_ptr<parquet::RowGroupReader> &row_group_reader,
        const common::ObString &data_file_path,
        int row_group_idx,
        int64_t batch_size,
        int64_t begin_file_row,
        int64_t end_file_row,
        int64_t data_file_record_count,
        common::ObArrayWrap<int64_t> &position_values,
        common::ObRoaringBitmap *delete_bitmap) const;

    std::unique_ptr<parquet::ParquetFileReader> delete_file_reader_;
    ObArrowMemPool arrow_alloc_;
    parquet::ReaderProperties read_props_;
    int file_path_column_idx_;
    int pos_column_idx_;
    bool is_file_scoped_;
  };

  class PuffinDeleteFileReader : public IDeleteFileReader
  {
  public:
    PuffinDeleteFileReader()
        : file_access_driver_(nullptr), file_prebuffer_(nullptr), file_content_offset_(0),
          file_content_size_in_bytes_(0)
    {
    }

    int init() override
    {
      return OB_SUCCESS;
    };
    void reset() override;
    int open_delete_file(const ObLakeDeleteFile &delete_file,
                         ObExternalFileAccess &file_access_driver,
                         ObFilePreBuffer &file_prebuffer,
                         const storage::ObTableScanParam *scan_param,
                         ObExternalTableAccessOptions *options) override;

    int read_delete_file(const ObString &data_file_path,
                         const storage::ObTableScanParam *scan_param,
                         int64_t data_file_record_count,
                         ObRoaringBitmap *delete_bitmap) override;

  private:
    ObExternalFileAccess *file_access_driver_;
    ObFilePreBuffer *file_prebuffer_;
    int64_t file_content_offset_;
    int64_t file_content_size_in_bytes_;
  };

public:
  ObIcebergDeleteBitmapBuilder()
      : scan_param_(nullptr), options_(nullptr), delete_file_prebuffer_(delete_file_access_driver_),
        orc_reader_(), parquet_reader_(), puffin_reader_()
  {
  }

  ~ObIcebergDeleteBitmapBuilder();

  int init(const storage::ObTableScanParam *scan_param, ObExternalTableAccessOptions *options);

  int register_metrics(ObLakeTableReaderProfile &reader_profile);

  ObLakeTableIcebergDeleteMetrics &get_metrics()
  {
    return delete_metrics_;
  }

  int build_delete_bitmap(const ObString &data_file_path,
                          const int64_t task_idx,
                          ObRoaringBitmap *delete_bitmap);

private:
  int get_delete_file_reader(const ObLakeDeleteFile &delete_file, IDeleteFileReader *&reader);

  int process_single_delete_file(const ObLakeDeleteFile &delete_file,
                                 const ObString &data_file_path,
                                 int64_t data_file_record_count,
                                 IDeleteFileReader *reader,
                                 ObRoaringBitmap *delete_bitmap);

  int reset_delete_file_state();
  int pre_buffer_delete_file(const ObLakeDeleteFile &delete_file);

private:
  const storage::ObTableScanParam *scan_param_;
  ObExternalTableAccessOptions *options_;
  // 文件访问相关
  ObExternalFileAccess delete_file_access_driver_;
  ObFilePreBuffer delete_file_prebuffer_;

  ObLakeTableIcebergDeleteMetrics delete_metrics_;

  OrcDeleteFileReader orc_reader_;
  ParquetDeleteFileReader parquet_reader_;
  PuffinDeleteFileReader puffin_reader_;
};

} // namespace sql
} // namespace oceanbase

#endif // OB_ICEBERG_DELETE_BITMAP_BUILDER_H
