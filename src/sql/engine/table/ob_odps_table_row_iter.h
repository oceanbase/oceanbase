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

#ifndef __SQL_OB_ODPS_TABLE_ROW_ITER_H__
#define __SQL_OB_ODPS_TABLE_ROW_ITER_H__
#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_tunnel.h>
#include <odps/odps_api.h>
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace sql {

class ObODPSTableRowIterator : public ObExternalTableRowIterator {
public:
  static const int64_t READER_HASH_MAP_BUCKET_NUM = 1 << 7;
  static const int64_t ODPS_BLOCK_DOWNLOAD_SIZE = 1 << 18;
public:
  struct StateValues {
    StateValues() :
      task_idx_(-1),
      part_id_(0),
      start_(0),
      step_(0),
      count_(0),
      is_from_gi_pump_(false),
      download_handle_(NULL),
      record_reader_handle_(NULL) {}
    int reuse();
    TO_STRING_KV(K(task_idx_),
                 K(part_id_),
                 K(start_),
                 K(step_),
                 K(count_),
                 K(is_from_gi_pump_),
                 K(ObString(part_spec_.c_str())),
                 K(ObString(download_id_.c_str())));
    int64_t task_idx_;
    int64_t part_id_;
    int64_t start_;
    int64_t step_;
    int64_t count_;
    bool is_from_gi_pump_;
    apsara::odps::sdk::IDownloadPtr download_handle_;
    apsara::odps::sdk::IRecordReaderPtr record_reader_handle_;
    std::string part_spec_;
    std::string download_id_;
    ObNewRow part_list_val_;
  };
  struct OdpsPartition {
    OdpsPartition() :
      name_(""),
      download_handle_(NULL),
      download_id_(""),
      record_count_(-1)
    {
    }
    OdpsPartition(const std::string &name) :
      name_(name),
      download_handle_(NULL),
      download_id_(""),
      record_count_(-1)
    {
    }
    OdpsPartition(const std::string &name,
                  apsara::odps::sdk::IDownloadPtr download_handle,
                  const std::string download_id,
                  int64_t &record_count) :
      name_(name),
      download_handle_(download_handle),
      download_id_(download_id),
      record_count_(record_count)
    {
    }
    ~OdpsPartition() {
      reset();
    }
    int reset();
    TO_STRING_KV(K(ObString(name_.c_str())), K(record_count_));
    std::string name_;
    apsara::odps::sdk::IDownloadPtr download_handle_;
    std::string download_id_;
    int64_t record_count_;
  };

  struct OdpsColumn {
    OdpsColumn() {}
    OdpsColumn(std::string name, apsara::odps::sdk::ODPSColumnTypeInfo type_info) :
      name_(name),
      type_info_(type_info)
    {
    }
    std::string name_;
    apsara::odps::sdk::ODPSColumnTypeInfo type_info_;
    TO_STRING_KV(K(ObString(name_.c_str())), K(type_info_.mType), K(type_info_.mPrecision), K(type_info_.mScale), K(type_info_.mSpecifiedLength));
  };
public:
  ObODPSTableRowIterator() :
    odps_format_(),
    account_(),
    conf_(),
    tunnel_(),
    odps_(NULL),
    table_handle_(NULL),
    state_(),
    is_part_table_(false),
    total_count_(0),
    bit_vector_cache_(NULL),
    record_(NULL),
    records_(NULL),
    batch_size_(-1),
    get_next_task_(false)
  {
    mem_attr_ = ObMemAttr(MTL_ID(), "odpsrowiter");
    malloc_alloc_.set_attr(mem_attr_);
  }
  virtual ~ObODPSTableRowIterator() {
    if (NULL != bit_vector_cache_) {
      malloc_alloc_.free(bit_vector_cache_);
    }
    for (int64_t i = 0; i < batch_size_; ++i) {
      records_[i].reset();
    }
    if (NULL != records_) {
      malloc_alloc_.free(records_);
    }
    record_.reset();
    records_ = NULL;
    batch_size_ = -1;
    get_next_task_ = false;
    reset();
  }
  virtual int init(const storage::ObTableScanParam *scan_param) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }
  virtual void reset() override;
  int init_tunnel(const sql::ObODPSGeneralFormat &odps_format);
  int create_downloader(ObString &part_spec, apsara::odps::sdk::IDownloadPtr &downloader);
  int pull_partition_info();
  inline ObIArray<OdpsPartition>& get_partition_info() { return partition_list_; }
  inline bool is_part_table() { return is_part_table_; }
  static int check_type_static(const apsara::odps::sdk::ODPSColumnType odps_type,
                                const int32_t odps_type_length,
                                const int32_t odps_type_precision,
                                const int32_t odps_type_scale,
                                const ObObjType ob_type,
                                const int32_t ob_type_length,
                                const int32_t ob_type_precision,
                                const int32_t ob_type_scale);
private:
  int inner_get_next_row();
  int prepare_expr();
  int pull_column();
  int next_task();
  int print_type_map_user_info(apsara::odps::sdk::ODPSColumnTypeInfo odps_type_info,
                                const ObExpr *ob_type_expr);
  int check_type_static(apsara::odps::sdk::ODPSColumnTypeInfo odps_type_info,
                        const ObExpr *ob_type_expr);
  inline bool text_type_length_is_valid_at_runtime(ObObjType type, int64_t odps_data_length) {
    bool is_valid = false;
    if (ObTinyTextType == type && odps_data_length < OB_MAX_TINYTEXT_LENGTH) {
      is_valid = true;
    } else if (ObTextType == type && odps_data_length < OB_MAX_TEXT_LENGTH) {
      is_valid = true;
    } else if (ObMediumTextType == type && odps_data_length < OB_MAX_MEDIUMTEXT_LENGTH) {
      is_valid = true;
    } else if (ObLongTextType == type && odps_data_length < OB_MAX_LONGTEXT_LENGTH) {
      is_valid = true;
    }
    return is_valid;
  }
private:
  ObODPSGeneralFormat odps_format_;
  apsara::odps::sdk::Account account_;
  apsara::odps::sdk::Configuration conf_;
  apsara::odps::sdk::OdpsTunnel tunnel_;
  apsara::odps::sdk::IODPSPtr odps_;
  apsara::odps::sdk::IODPSTablePtr table_handle_;
  ObSEArray<OdpsPartition, 8> partition_list_;
  ObSEArray<OdpsColumn, 8> column_list_;
  ObSEArray<int64_t, 8> target_column_id_list_;
  StateValues state_;
  bool is_part_table_;
  int64_t total_count_;
  ObBitVector *bit_vector_cache_;
  apsara::odps::sdk::ODPSTableRecordPtr record_;
  apsara::odps::sdk::ODPSTableRecordPtr *records_;
  int64_t batch_size_; // -1 means not inited, 0 means call get_next_row(), > 0 means call get_next_rows()
  bool get_next_task_; // only used for get next task and recall inner_get_next_row() when curren task was iter end.
  common::ObMalloc malloc_alloc_;
  common::ObArenaAllocator arena_alloc_;
  common::ObMemAttr mem_attr_;
};

class ObOdpsPartitionDownloaderMgr
{
public:
  struct OdpsPartitionDownloader {
    OdpsPartitionDownloader() :
      odps_driver_(),
      odps_partition_downloader_(NULL)
    {}
    ~OdpsPartitionDownloader() {
      reset();
    }
    int reset();
    ObODPSTableRowIterator odps_driver_;
    apsara::odps::sdk::IDownloadPtr odps_partition_downloader_;
  };
  class DeleteDownloaderFunc
  {
  public:
    DeleteDownloaderFunc() {}
    virtual ~DeleteDownloaderFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, int64_t> &kv);
  };
  struct OdpsUploader {
    OdpsUploader() : upload_(NULL), record_writer_(NULL) {}
    ~OdpsUploader() {
      upload_.reset();
      record_writer_.reset();
    }
    apsara::odps::sdk::IUploadPtr upload_;
    apsara::odps::sdk::IRecordWriterPtr record_writer_;
  };
  ObOdpsPartitionDownloaderMgr() : inited_(false), is_download_(true), ref_(0), need_commit_(true) {}
  int init_downloader(common::ObArray<share::ObExternalFileInfo> &external_table_files,
                      const ObString &properties);
  int init_uploader(const ObString &properties,
                    const ObString &external_partition,
                    bool is_overwrite,
                    int64_t parallel);
  static int create_upload_session(const sql::ObODPSGeneralFormat &odps_format,
                                   const ObString &external_partition,
                                   bool is_overwrite,
                                   apsara::odps::sdk::IUploadPtr &upload);
  int get_odps_downloader(int64_t part_id, apsara::odps::sdk::IDownloadPtr &downloader);
  int get_odps_uploader(int64_t block_id,
                        apsara::odps::sdk::IUploadPtr &upload,
                        apsara::odps::sdk::IRecordWriterPtr &record_writer);
  int commit_upload();
  int reset();
  OB_INLINE bool is_download_mgr_inited() { return inited_ && is_download_; }
  inline int64_t inc_ref()
  {
    return ATOMIC_FAA(&ref_, 1);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_, 1);
  }
  inline void set_fail()
  {
    ATOMIC_STORE(&need_commit_, false);
  }
private:
  bool inited_;
  bool is_download_;
  common::hash::ObHashMap<int64_t, int64_t> odps_mgr_map_;
  common::ObArenaAllocator arena_alloc_;
  int64_t ref_;
  bool need_commit_;
};

} // sql
} // oceanbase

#endif
#endif // __SQL_OB_ODPS_TABLE_ROW_ITER_H__