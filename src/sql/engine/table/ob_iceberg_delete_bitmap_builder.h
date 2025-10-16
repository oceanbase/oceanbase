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

#ifndef OB_ICEBERG_DELETE_BITMAP_BUILDER_H
#define OB_ICEBERG_DELETE_BITMAP_BUILDER_H

#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/table/ob_file_prebuffer.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include <orc/Reader.hh>
#include <parquet/api/reader.h>
#include "sql/engine/basic/ob_arrow_basic.h"

namespace oceanbase
{
namespace sql
{
class ObIcebergDeleteBitmapBuilder
{
public:
  class IDeleteFileReader
  {
  public:
    virtual ~IDeleteFileReader() = default;

    virtual int init() = 0;
    virtual int open_delete_file(const ObLakeDeleteFile &delete_file_path,
                                ObExternalFileAccess &file_access_driver,
                                ObFilePreBuffer &file_prebuffer,
                                const storage::ObTableScanParam *scan_param,
                                ObExternalTableAccessOptions *options) = 0;

    virtual int read_delete_file(const ObString &data_file_path,
                                const storage::ObTableScanParam *scan_param,
                                ObRoaringBitmap *delete_bitmap) = 0;
  };

  class OrcDeleteFileReader : public IDeleteFileReader
  {
  public:
    OrcDeleteFileReader()
      : delete_reader_(nullptr),
        delete_row_reader_(nullptr),
        delete_data_batch_(nullptr)
    {}

    int init() override;
    int open_delete_file(const ObLakeDeleteFile &delete_file,
                        ObExternalFileAccess &file_access_driver,
                        ObFilePreBuffer &file_prebuffer,
                        const storage::ObTableScanParam *scan_param,
                        ObExternalTableAccessOptions *options) override;

    int read_delete_file(const ObString &data_file_path,
                        const storage::ObTableScanParam *scan_param,
                        ObRoaringBitmap *delete_bitmap) override;

  private:
    std::unique_ptr<orc::Reader> delete_reader_;
    std::unique_ptr<orc::RowReader> delete_row_reader_;
    std::unique_ptr<orc::ColumnVectorBatch> delete_data_batch_;
    orc::RowReaderOptions row_reader_options_;
    ObOrcMemPool orc_alloc_;
  };

  class ParquetDeleteFileReader : public IDeleteFileReader
  {
  public:
    ParquetDeleteFileReader() :
      delete_file_reader_(nullptr),
      read_props_(&arrow_alloc_)
    {}

    int init() override;
    int open_delete_file(const ObLakeDeleteFile &delete_file,
                        ObExternalFileAccess &file_access_driver,
                        ObFilePreBuffer &file_prebuffer,
                        const storage::ObTableScanParam *scan_param,
                        ObExternalTableAccessOptions *options) override;

    int read_delete_file(const ObString &data_file_path,
                        const storage::ObTableScanParam *scan_param,
                        ObRoaringBitmap *delete_bitmap) override;

  private:
    std::unique_ptr<parquet::ParquetFileReader> delete_file_reader_;
    ObArrowMemPool arrow_alloc_;
    parquet::ReaderProperties read_props_;
  };

public:
  ObIcebergDeleteBitmapBuilder()
      : scan_param_(nullptr),
        options_(nullptr),
        delete_file_prebuffer_(delete_file_access_driver_),
        orc_reader_(),
        parquet_reader_()
  {}

  ~ObIcebergDeleteBitmapBuilder();

  int init(const storage::ObTableScanParam *scan_param,
          ObExternalTableAccessOptions *options);

  int build_delete_bitmap(const ObString &data_file_path, const int64_t task_idx,
                          ObRoaringBitmap *delete_bitmap);

private:
  int get_delete_file_reader(const iceberg::DataFileFormat file_format, IDeleteFileReader *&reader);


  int process_single_delete_file(const ObLakeDeleteFile &delete_file,
                                const ObString &data_file_path,
                                IDeleteFileReader *reader,
                                ObRoaringBitmap *delete_bitmap);

  int pre_buffer(int64_t file_size);

private:
  const storage::ObTableScanParam *scan_param_;
  ObExternalTableAccessOptions* options_;
  // 文件访问相关
  ObExternalFileAccess delete_file_access_driver_;
  ObFilePreBuffer delete_file_prebuffer_;

  OrcDeleteFileReader orc_reader_;
  ParquetDeleteFileReader parquet_reader_;
};

} // namespace sql
} // namespace oceanbase

#endif // OB_ICEBERG_DELETE_BITMAP_BUILDER_H
