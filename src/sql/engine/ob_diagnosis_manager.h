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

#ifndef OCEANBASE_SQL_ENGINE_OB_DIAGNOSIS_MANAGER_
#define OCEANBASE_SQL_ENGINE_OB_DIAGNOSIS_MANAGER_

#include "sql/engine/basic/ob_select_into_basic.h"
namespace oceanbase
{
namespace common
{
class ObWarningBuffer;
}
namespace sql
{
class ObCsvFileWriter;
struct ObDiagnosisInfo;

struct ObDiagnosisFileWriter
{
public:
  ObDiagnosisFileWriter() :
    write_offset_(0), file_name_(NULL), file_id_(0), file_max_size_(0),
    compress_type_(CsvCompressType::NONE), data_writer_(NULL),
    file_location_(IntoFileLocation::SERVER_DISK), has_lob_(false), use_shared_buf_(false)
  {
  }

  virtual ~ObDiagnosisFileWriter();
  int create_data_writer(const ObString& file_name);
  bool should_split();
  int split_file();

  bool has_compress_;
  int64_t write_offset_;
  ObString file_name_;
  int64_t file_id_;
  share::ObBackupStorageInfo access_info_;
  int64_t file_max_size_;
  ObCSVGeneralFormat::ObCSVCompression compress_type_;
  ObCsvFileWriter* data_writer_;
  IntoFileLocation file_location_;
  bool has_lob_;
  bool use_shared_buf_;
  ObIAllocator *allocator_;
};

struct ObDiagnosisManager
{
  ObDiagnosisManager()
    :
      idxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      rets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      col_names_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      data_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      missing_col_idxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      cur_file_url_(NULL),
      cur_line_number_(0),
      is_header_written_(false),
      is_file_writer_inited_(false),
      log_file_writer_(nullptr),
      bad_file_writer_(nullptr)
  {
    ObMemAttr attr(MTL_ID(), "DiagnosisMgr");
    allocator_.set_attr(attr);
  }
  virtual ~ObDiagnosisManager();
  void set_cur_file_url(ObString file_url) { cur_file_url_ = file_url; }
  ObString get_cur_file_url() { return cur_file_url_; }
  void set_cur_line_number(int64_t line_number) { cur_line_number_  = line_number; }
  int64_t get_cur_line_number() { return cur_line_number_; }
  int do_diagnosis(ObBitVector &skip,
                  const ObDiagnosisInfo& diagnosis_info,
                  int64_t sqc_id,
                  int64_t task_id,
                  common::ObIAllocator &allocator);
  int calc_first_file_path(ObString &path,
                          int64_t sqc_id,
                          int64_t task_id,
                          bool is_log,
                          ObDiagnosisFileWriter &file_writer);
  int init_file_name(const ObString& file_path,
                    int64_t sqc_id,
                    int64_t task_id,
                    bool is_log,
                    ObDiagnosisFileWriter &file_writer);
  int gen_csv_line_data(int64_t err_ret,
                        int64_t idx,
                        ObString file_name,
                        ObString err_message,
                        ObString& result,
                        common::ObIAllocator &allocator);
  static int calc_next_file_path(ObString& file_name,
                                int64_t& file_id,
                                ObCSVGeneralFormat::ObCSVCompression compress_type,
                                common::ObIAllocator &allocator);
  int add_warning_info(int err_ret, int line_idx);
  int close();
  int init_file_writer(common::ObIAllocator &allocator, const ObString& file_name, int64_t sqc_id,
                      int64_t task_id, bool is_log_file, ObDiagnosisFileWriter *&file_writer);
  int handle_warning(int64_t err_ret, int64_t idx, ObString col_name,
                    ObString log_file, ObWarningBuffer *buffer, int64_t limit_cnt);
  void reuse();

  common::ObArray<int64_t> idxs_;
  common::ObArray<int64_t> rets_;
  common::ObArray<ObString> col_names_;
  common::ObArray<ObString> data_;
  common::ObArray<int64_t> missing_col_idxs_;
  ObString cur_file_url_;
  int64_t cur_line_number_;
  ObArenaAllocator allocator_;
  bool is_header_written_;
  bool is_file_writer_inited_;
  ObDiagnosisFileWriter* log_file_writer_;
  ObDiagnosisFileWriter* bad_file_writer_;
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_OB_DIAGNOSIS_MANAGER_ */
