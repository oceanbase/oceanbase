#ifdef OB_BUILD_CPP_ODPS
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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_table_row_iter.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "src/share/external_table/ob_external_table_utils.h"
#include "src/sql/engine/expr/ob_datum_cast.h"

namespace oceanbase {
namespace sql {

int ObODPSTableRowIterator::OdpsPartition::reset()
{
  int ret = OB_SUCCESS;
  record_count_ = -1;
  name_.clear();
  return ret;
}

int ObODPSTableRowIterator::init_tunnel(const sql::ObODPSGeneralFormat &odps_format, bool need_decrypt)
{
  int ret = OB_SUCCESS;
  try {
    if (OB_FAIL(odps_format_.deep_copy(odps_format))) {
      LOG_WARN("failed to deep copy odps format", K(ret));
    } else if (need_decrypt && OB_FAIL(odps_format_.decrypt())) {
      LOG_WARN("failed to decrypt odps format", K(ret));
    } else {
      LOG_TRACE("init tunnel format", K(ret));
      if (0 == odps_format_.access_type_.case_compare("aliyun") ||
          odps_format_.access_type_.empty()) {
        account_ = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_ALIYUN),
                                              std::string(odps_format_.access_id_.ptr(), odps_format_.access_id_.length()),
                                              std::string(odps_format_.access_key_.ptr(), odps_format_.access_key_.length()));
      } else if (0 == odps_format_.access_type_.case_compare("sts")) {
        account_ = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_STS),
                                              std::string(odps_format_.sts_token_.ptr(), odps_format_.sts_token_.length()));
      } else if (0 == odps_format_.access_type_.case_compare("token")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported access type", K(ret), K(odps_format_.access_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: token");
      } else if (0 == odps_format_.access_type_.case_compare("domain")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported access type", K(ret), K(odps_format_.access_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: domain");
      } else if (0 == odps_format_.access_type_.case_compare("taobao")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported access type", K(ret), K(odps_format_.access_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type: taobao");
      } else if (0 == odps_format_.access_type_.case_compare("app")) {
        account_ = apsara::odps::sdk::Account(std::string(apsara::odps::sdk::ACCOUNT_APPLICATION),
                                              std::string(odps_format_.access_id_.ptr(), odps_format_.access_id_.length()),
                                              std::string(odps_format_.access_key_.ptr(), odps_format_.access_key_.length()));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS access type");
      }
      conf_.SetAccount(account_);
      conf_.SetEndpoint(std::string(odps_format_.endpoint_.ptr(), odps_format_.endpoint_.length()));
      if (!odps_format_.tunnel_endpoint_.empty()) {
        LOG_TRACE("set tunnel endpoint", K(ret), K(odps_format_.tunnel_endpoint_));
        conf_.SetTunnelEndpoint(std::string(odps_format_.tunnel_endpoint_.ptr(), odps_format_.tunnel_endpoint_.length()));
      }
      conf_.SetUserAgent("OB_ACCESS_ODPS");
      conf_.SetTunnelQuotaName(std::string(odps_format_.quota_.ptr(), odps_format_.quota_.length()));
      if (0 == odps_format_.compression_code_.case_compare("zlib")) {
        conf_.SetCompressOption(apsara::odps::sdk::CompressOption::ZLIB_COMPRESS);
      } else if (0 == odps_format_.compression_code_.case_compare("zstd")) {
        conf_.SetCompressOption(apsara::odps::sdk::CompressOption::ZSTD_COMPRESS);
      } else if (0 == odps_format_.compression_code_.case_compare("lz4")) {
        conf_.SetCompressOption(apsara::odps::sdk::CompressOption::LZ4_COMPRESS);
      } else if (0 == odps_format_.compression_code_.case_compare("odps_lz4")) {
        conf_.SetCompressOption(apsara::odps::sdk::CompressOption::ODPS_LZ4_COMPRESS);
      } else {
        conf_.SetCompressOption(apsara::odps::sdk::CompressOption::NO_COMPRESS);
      }
      tunnel_.Init(conf_); // do not need try catch
      if (OB_ISNULL((odps_ = apsara::odps::sdk::IODPS::Create(conf_, // do not need try catch
                                                              std::string(odps_format_.project_.ptr(),
                                                              odps_format_.project_.length()))).get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else if (OB_ISNULL((odps_->GetTables()).get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else if (OB_ISNULL((table_handle_ = odps_->GetTables()->Get(std::string(odps_format_.project_.ptr(), odps_format_.project_.length()), // do not need try catch
                                                          std::string(odps_format_.schema_.ptr(), odps_format_.schema_.length()),
                                                          std::string(odps_format_.table_.ptr(), odps_format_.table_.length()))).get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      }
    }
  } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), KP(this));
    }
  }

  return ret;
}

int ObODPSTableRowIterator::create_downloader(const ObString &part_spec, apsara::odps::sdk::IDownloadPtr &downloader)
{
  int ret = OB_SUCCESS;
  try {
    apsara::odps::sdk::IDownloadPtr download_handle = NULL;
    downloader = NULL;
    std::string project(odps_format_.project_.ptr(), odps_format_.project_.length());
    std::string table(odps_format_.table_.ptr(), odps_format_.table_.length());
    std::string std_part_spec(part_spec.ptr(), part_spec.length());
    std::string download_id("");
    std::string schema(odps_format_.schema_.ptr(), odps_format_.schema_.length());
    download_handle = tunnel_.CreateDownload(project,
                                             table,
                                             std_part_spec,
                                             download_id,
                                             schema);
    if (OB_ISNULL(download_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else {
      downloader = download_handle;
    }
  } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call CreateDownload", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call CreateDownload", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call CreateDownload", K(ret), KP(this));
    }
  }
  return ret;
}

int ObODPSTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
    LOG_WARN("failed to call ObExternalTableRowIterator::init", K(ret));
  } else {
    if (OB_FAIL(init_tunnel(scan_param_->external_file_format_.odps_format_))) {
      LOG_WARN("failed to init odps tunnel", K(ret));
    } else if (OB_FAIL(pull_column())) {
      LOG_WARN("failed to pull column info", K(ret));
    } else if (OB_FAIL(prepare_expr())) {
      LOG_WARN("failed to prepare expr", K(ret));
    }
  }
  return ret;
}

int ObODPSTableRowIterator::next_task()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  int64_t task_idx = state_.task_idx_;
  LOG_TRACE("going to get new task", K(ret), K(batch_size_), K(state_), K(total_count_), K(task_idx), K(scan_param_->key_ranges_.count()));
  int64_t start = 0;
  int64_t step = 0;
  if (++task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("odps table iter end", K(total_count_), K(state_), K(task_idx), K(ret));
  } else {
    ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
    ObPxSqcHandler *sqc = ctx.exec_ctx_.get_sqc_handler();// if sqc is not NULL, odps read is in px plan
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                              ObExternalTableUtils::LINE_NUMBER,
                                                              start,
                                                              step))) {
      LOG_WARN("failed to resolve range in external table", K(ret));
    } else {
      try {
        const ObString &part_spec = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
        int64_t part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
        if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
          ret = OB_ITER_END;
          LOG_WARN("empty file", K(ret));
        } else {
          if (OB_ISNULL(sqc) || !sqc->get_sqc_ctx().gi_pump_.is_odps_downloader_inited()) {
            std::string project(odps_format_.project_.ptr(), odps_format_.project_.length());
            std::string table(odps_format_.table_.ptr(), odps_format_.table_.length());
            std::string std_part_spec(part_spec.ptr(), part_spec.length());
            std::string download_id("");
            std::string schema(odps_format_.schema_.ptr(), odps_format_.schema_.length());
            state_.download_handle_ = tunnel_.CreateDownload(project,
                                                             table,
                                                             std_part_spec,
                                                             download_id,
                                                             schema);
            state_.is_from_gi_pump_ = false;
            LOG_TRACE("succ to create downloader handle without GI", K(ret), K(part_id), KP(sqc), K(state_.is_from_gi_pump_));
          } else {
            ObOdpsPartitionDownloaderMgr::OdpsMgrMap& odps_map = sqc->get_sqc_ctx().gi_pump_.get_odps_map();
            state_.is_from_gi_pump_ = true;
            if (OB_FAIL(sqc->get_sqc_ctx().gi_pump_.get_odps_downloader(part_id, state_.download_handle_))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader *temp_downloader = NULL;
                ObIAllocator &allocator = sqc->get_sqc_ctx().gi_pump_.get_odps_mgr().get_allocator();
                if (sqc->get_sqc_ctx().gi_pump_.get_pump_args().empty()) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected empty gi pump args", K(ret));
                } else if (OB_ISNULL(temp_downloader = static_cast<ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader *>(
                              allocator.alloc(sizeof(ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader))))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader)));
                } else if (FALSE_IT(new(temp_downloader)ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader())) {
                } else if (OB_FAIL(temp_downloader->tunnel_ready_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
                  LOG_WARN("failed to init tunnel condition variable", K(ret));
                } else if (OB_FAIL(odps_map.set_refactored(part_id, reinterpret_cast<int64_t>(temp_downloader), 0/*flag*/, 0/*broadcast*/, 0/*overwrite_key*/))) {
                  if (OB_HASH_EXIST == ret) {
                    ret = OB_SUCCESS;
                    if (OB_FAIL(sqc->get_sqc_ctx().gi_pump_.get_odps_downloader(part_id, state_.download_handle_))) {
                      LOG_WARN("failed to get from odps_map", K(part_id), K(ret));
                    } else {
                      LOG_TRACE("succ to get downloader handle from GI", K(ret), K(part_id), K(state_.is_from_gi_pump_));
                    }
                  } else {
                    LOG_WARN("fail to set refactored", K(ret));
                  }
                  // other thread has create downloader, free current downloader
                  allocator.free(temp_downloader);
                  temp_downloader = NULL;
                } else {
                  if (OB_FAIL(temp_downloader->odps_driver_.init_tunnel(odps_format_, false))) {
                    LOG_WARN("failed to init tunnel", K(ret), K(part_id));
                  } else if (OB_FAIL(temp_downloader->odps_driver_.create_downloader(part_spec, temp_downloader->odps_partition_downloader_))) {
                    LOG_WARN("failed create odps partition downloader", K(ret), K(part_id));
                  }
                  temp_downloader->tunnel_ready_cond_.lock();
                  if (OB_FAIL(ret)) {
                    temp_downloader->downloader_init_status_ = -1; // -1 is temp_downloader failed to initialize temp_downloader
                  } else {
                    state_.download_handle_ = temp_downloader->odps_partition_downloader_;
                    temp_downloader->downloader_init_status_ = 1; // 1 is temp_downloader was initialized successfully
                    LOG_TRACE("succ to create downloader handle and set it to GI", K(ret), K(part_id), K(state_.is_from_gi_pump_), KP(temp_downloader));
                  }
                  temp_downloader->tunnel_ready_cond_.broadcast(); // wake other threads that temp_downloader has finished initializing. The initialization result may be failure or success.
                  temp_downloader->tunnel_ready_cond_.unlock();
                }
                if (OB_FAIL(ret) && OB_NOT_NULL(temp_downloader)) {
                  allocator.free(temp_downloader);
                  temp_downloader = NULL;
                }
              } else {
                LOG_WARN("failed to get from odps_map", K(part_id), K(ret));
              }
            } else {
              LOG_TRACE("succ to get downloader handle from GI", K(ret), K(part_id), K(state_.is_from_gi_pump_));
            }
          }
        }
        if (OB_FAIL(ret)) {
          //do nothing
        } else if (OB_ISNULL(state_.download_handle_.get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcepted null ptr", K(ret), KP(sqc), K(state_.is_from_gi_pump_));
        } else if (column_names_.size() &&
                   OB_ISNULL((state_.record_reader_handle_ = state_.download_handle_->OpenReader(start,
                                                                                         step,
                                                                                         column_names_,
                                                                                         true)).get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcepted null ptr", K(ret));
        } else if (OB_FAIL(calc_file_partition_list_value(part_id, arena_alloc_, state_.part_list_val_))) {
          LOG_WARN("failed to calc parttion list value", K(part_id), K(ret));
        } else {
          int64_t real_time_partition_row_count = state_.download_handle_->GetRecordCount();
          if (start >= real_time_partition_row_count) {
            start = real_time_partition_row_count;
            step = 0;
            LOG_TRACE("start is overflow", K(ret), K(part_id), K(state_.start_), K(task_idx), K(real_time_partition_row_count));
          } else if (INT64_MAX == step || start + step > real_time_partition_row_count) {
            step = real_time_partition_row_count - start;
            LOG_TRACE("refine odps step", K(real_time_partition_row_count), K(step), K(start), K(part_id));
          }
          if (OB_SUCC(ret)) {
            state_.task_idx_ = task_idx;
            state_.part_id_ = part_id;
            state_.start_ = start;
            state_.step_ = step;
            state_.count_ = 0;
            state_.download_id_ = state_.download_handle_->GetDownloadId();
            // what if error occur after this line, how to close state_.record_reader_handle_?
            LOG_TRACE("succ to get a new task", K(ret),
                                                K(batch_size_),
                                                K(state_),
                                                K(real_time_partition_row_count),
                                                K(column_names_.size()),
                                                K(state_.part_list_val_),
                                                K(total_count_),
                                                K(task_idx),
                                                K(scan_param_->key_ranges_.count()),
                                                K(NULL == state_.record_reader_handle_.get()));
          }
          if (OB_SUCC(ret) && -1 == batch_size_ && column_names_.size()) { // exec once only
            batch_size_ = eval_ctx.max_batch_size_;
            if (0 == batch_size_) {
              // even state_.record_reader_handle_ was destroyed, record_/records_ is still valid.
              // see class RecordReader : public IRecordReader to check it.
              if (OB_ISNULL((record_ = state_.record_reader_handle_->CreateBufferRecord()).get())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null ptr", K(ret));
              } else {
                LOG_TRACE("odps record_ inited", K(ret), K(batch_size_));
              }
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
                if (OB_ISNULL((records_[i] = state_.record_reader_handle_->CreateBufferRecord()).get())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected null ptr", K(ret), K(i));
                }
              }
              LOG_TRACE("odps records_ inited", K(ret), K(batch_size_));
            }
          }
        }
      } catch (apsara::odps::sdk::OdpsException& ex) {
        if (OB_SUCC(ret)) {
          ret = OB_ODPS_ERROR;
          LOG_WARN("odps exception occured when calling odps api", K(ret), K(ex.what()));
          LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
        }
      } catch (const std::exception &ex) {
        if (OB_SUCC(ret)) {
          ret = OB_ODPS_ERROR;
          LOG_WARN("odps exception occured when calling odps api", K(ret), K(ex.what()));
          LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
        }
      } catch (...) {
        if (OB_SUCC(ret)) {
          ret = OB_ODPS_ERROR;
          LOG_WARN("odps exception occured when calling odps api", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObODPSTableRowIterator::print_type_map_user_info(apsara::odps::sdk::ODPSColumnTypeInfo odps_type_info,
                                                      const ObExpr *ob_type_expr)
{
  int ret = OB_SUCCESS;
  try {
    std::string odps_type_str = odps_type_info.ToTypeString();//need try catch
    const char* odps_type_cstr = odps_type_str.c_str();
    const char* ob_type_cstr = "";
    if (OB_NOT_NULL(ob_type_expr)) {
      ObArrayWrap<char> buf;
      ObArray<ObString> extend_info;
      int64_t pos = 0;
      ob_type_cstr = ob_obj_type_str(ob_type_expr->datum_meta_.type_);
      if (OB_SUCCESS == buf.allocate_array(arena_alloc_, 128)) { // 128 is enough to hold user info str
        ob_sql_type_str(buf.get_data(), buf.count(), pos,
                        ob_type_expr->datum_meta_.type_,
                        ob_type_expr->max_length_,
                        ob_type_expr->datum_meta_.precision_,
                        ob_type_expr->datum_meta_.scale_,
                        ob_type_expr->datum_meta_.cs_type_,
                        extend_info);
        if (pos < buf.count()) {
          buf.at(pos++) = '\0';
          ob_type_cstr = buf.get_data();
        }
      }
    }
    LOG_USER_ERROR(OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH, odps_type_cstr, ob_type_cstr);
  } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call ToTypeString", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call ToTypeString", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call ToTypeString", K(ret), KP(this));
    }
  }
  return ret;
}

int ObODPSTableRowIterator::check_type_static(apsara::odps::sdk::ODPSColumnTypeInfo odps_type_info,
                                              const ObExpr *ob_type_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_type_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    const apsara::odps::sdk::ODPSColumnType odps_type = odps_type_info.mType;
    const int32_t odps_type_length = odps_type_info.mSpecifiedLength;
    const int32_t odps_type_precision = odps_type_info.mPrecision;
    const int32_t odps_type_scale = odps_type_info.mScale;
    const ObObjType ob_type = ob_type_expr->obj_meta_.get_type();
    const int32_t ob_type_length = ob_type_expr->max_length_;
    const int32_t ob_type_precision = ob_type_expr->datum_meta_.precision_;
    const int32_t ob_type_scale = ob_type_expr->datum_meta_.scale_;
    switch(odps_type)
    {
      case apsara::odps::sdk::ODPS_TINYINT:
      case apsara::odps::sdk::ODPS_BOOLEAN:
      {
        if (ObTinyIntType == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_SMALLINT:
      {
        if (ObSmallIntType == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_INTEGER:
      {
        if (ObInt32Type == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_BIGINT:
      {
        if (ObIntType == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_FLOAT:
      {
        if (ObFloatType == ob_type && ob_type_length == 12 && ob_type_scale == -1) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_DOUBLE:
      {
        if (ObDoubleType == ob_type && ob_type_length == 23 && ob_type_scale == -1) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_DECIMAL:
      {
        if (ObDecimalIntType == ob_type ||
            ObNumberType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_precision != odps_type_precision || // in ObExpr, max_length_ is decimal type's precision
              ob_type_scale != odps_type_scale) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_CHAR:
      {
        if (ObCharType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_length != odps_type_length) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_VARCHAR:
      {
        if (ObVarcharType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_length != odps_type_length) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_STRING:
      case apsara::odps::sdk::ODPS_BINARY://check length at runtime
      {
        if (ObVarcharType == ob_type ||
            ObTinyTextType == ob_type ||
            ObTextType == ob_type ||
            ObLongTextType == ob_type ||
            ObMediumTextType == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_TIMESTAMP:
      {
        if (ObTimestampType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_scale < 6) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
      {
        if (ObDateTimeType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_scale < 6) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_DATE:
      {
        if (ObDateType == ob_type || ObMySQLDateType == ob_type) {
          // odps_type to ob_type is valid
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      case apsara::odps::sdk::ODPS_DATETIME:
      {
        if (ObDateTimeType == ob_type || ObMySQLDateTimeType == ob_type) {
          // odps_type to ob_type is valid
          if (ob_type_scale < 3) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                              K(ob_type_length),
                                                              K(ob_type_precision),
                                                              K(ob_type_scale),
                                                              K(odps_type_length),
                                                              K(odps_type_precision),
                                                              K(odps_type_scale));
            print_type_map_user_info(odps_type_info, ob_type_expr);
          }
        } else {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
          print_type_map_user_info(odps_type_info, ob_type_expr);
        }
        break;
      }
      default:
      {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported odps type", K(ret));
      }
    }
  }
  return ret;
}

int ObODPSTableRowIterator::prepare_expr()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->ext_file_column_exprs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", KP(scan_param_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->ext_file_column_exprs_->count(); ++i) {
      const ObExpr *cur_expr = scan_param_->ext_file_column_exprs_->at(i); // do no check is NULL or not
      int target_idx = cur_expr->extra_ - 1;
      if (OB_UNLIKELY(cur_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL &&
                      (target_idx < 0 || target_idx >= column_list_.count()))) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("unexcepted target_idx", K(ret), K(target_idx), K(column_list_.count()));
        LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR, "wrong column index point to odps, please check the index of external$tablecol[index] and metadata$partition_list_col[index]");
      } else if (OB_FAIL(target_column_id_list_.push_back(target_idx))) {
        LOG_WARN("failed to keep target_idx", K(ret));
      } else if (cur_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL &&
                 OB_FAIL(check_type_static(column_list_.at(target_idx).type_info_, cur_expr))) {
        LOG_WARN("odps type map ob type not support", K(ret), K(target_idx));
      }
    }
    if (OB_SUCC(ret)) {
      try {
        const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
        for (int64_t column_idx = 0; column_idx < target_column_id_list_.count(); ++column_idx) {
          if (file_column_exprs.at(column_idx)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
            column_names_.emplace_back(column_list_.at(target_column_id_list_.at(column_idx)).name_);
          }
        }
      } catch (const std::exception &ex) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error occur when calling vector emplace_back", K(ret), K(ex.what()));
        }
      } catch (...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error occur when calling vector emplace_back", K(ret));
        }
      }
    }
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    void *vec_mem = NULL;
    void *records_mem = NULL;
    malloc_alloc_.set_attr(lib::ObMemAttr(scan_param_->tenant_id_, "ODPSRowIter"));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 > eval_ctx.max_batch_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max_batch_size_", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      if (0 == eval_ctx.max_batch_size_) {
        // do nothing
      } else if (OB_ISNULL(vec_mem = malloc_alloc_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
      } else if (OB_ISNULL(records_mem = malloc_alloc_.alloc(eval_ctx.max_batch_size_ * sizeof(apsara::odps::sdk::ODPSTableRecordPtr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
      } else {
        bit_vector_cache_ = to_bit_vector(vec_mem);
        bit_vector_cache_->reset(eval_ctx.max_batch_size_);
        records_ = static_cast<apsara::odps::sdk::ODPSTableRecordPtr *>(records_mem);
        for (int64_t i = 0; i < eval_ctx.max_batch_size_; ++i) {
          new (&records_[i]) apsara::odps::sdk::ODPSTableRecordPtr;
        }
      }
    }
  }
  return ret;
}

int ObODPSTableRowIterator::pull_partition_info()
{
  int ret = OB_SUCCESS;
  partition_list_.reset();
  std::vector<std::string> part_specs;
  try {
    LOG_TRACE("get partition names start", K(ret));
    if (OB_ISNULL(table_handle_.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else {
      table_handle_->GetPartitionNames(part_specs);
      is_part_table_ = true;
    }
    LOG_TRACE("get partition names end", K(ret), K(is_part_table_));
  } catch (apsara::odps::sdk::OdpsException& ex) {
    std::string ex_msg = ex.what();
    if (std::string::npos != ex_msg.find("ODPS-0110031")) { // ODPS-0110031 means table is not a partitional table
      is_part_table_ = false;
    } else if (OB_FAIL(ret)) {
      //do nothing
    } else {
      ret = OB_ODPS_ERROR;
      LOG_WARN("failed to call GetPartitionNames method in ODPS sdk", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("failed to call GetPartitionNames method in ODPS sdk", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling GetPartitionNames method", K(ret));
    }
  }
  try {
    if (is_part_table_) {
      for (std::vector<std::string>::iterator part_spec = part_specs.begin(); OB_SUCC(ret) && part_spec != part_specs.end(); part_spec++) {
        int64_t record_count = -1;
        //apsara::odps::sdk::IODPSPartitionPtr partition = table_handle_->GetPartition(*part_spec);
        //record_count = partition->GetPartitionSize();
        if (OB_FAIL(partition_list_.push_back(OdpsPartition(*part_spec, record_count)))){
          LOG_WARN("failed to push back partition_list_", K(ret));
        }
      }
    } else {
      int64_t record_count = -1;
      //record_count = table_handle_->GetSize();
      if (OB_FAIL(partition_list_.push_back(OdpsPartition("", record_count)))){
        LOG_WARN("failed to push back partition_list_", K(ret));
      }
    }
  } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), K(ex.what()), KP(this));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), K(ex.what()), KP(this));
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when call external driver api", K(ret), KP(this));
    }
  }
  return ret;
}

int ObODPSTableRowIterator::pull_column() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_handle_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else {
    try {
      apsara::odps::sdk::IODPSTableSchemaPtr schema_handle = table_handle_->GetSchema();
      if (OB_ISNULL(schema_handle.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else {
        for (uint32_t i = 0; OB_SUCC(ret) && i < schema_handle->GetColumnCount(); i++) {
          if (OB_FAIL(column_list_.push_back(OdpsColumn(schema_handle->GetTableColumn(i).GetName(),
                                                        schema_handle->GetTableColumn(i).GetTypeInfo())))) {
            LOG_WARN("failed to push back column_list_", K(ret));
          }
        }
      }
    } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("failed to call GetSchema method in ODPS sdk", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception &ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("failed to call GetSchema method in ODPS sdk", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps exception occured when calling GetSchema method", K(ret));
      }
    }
  }
  return ret;
}

int ObODPSTableRowIterator::fill_partition_list_data(ObExpr &expr, int64_t returned_row_cnt) {
  int ret = OB_SUCCESS;
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  ObDatum *datums = expr.locate_batch_datums(ctx);
  ObObjType type = expr.obj_meta_.get_type();
  if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, returned_row_cnt))) {
    LOG_WARN("failed to init expr vector", K(ret), K(expr));
  } else if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
      int64_t loc_idx = expr.extra_ - 1;
      if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= state_.part_list_val_.get_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted loc_idx", K(ret), K(loc_idx), K(state_.part_list_val_.get_count()), KP(&state_.part_list_val_));
      } else if (state_.part_list_val_.get_cell(loc_idx).is_null()) {
        datums[row_idx].set_null();
      } else {
        CK (OB_NOT_NULL(datums[row_idx].ptr_));
        OZ (datums[row_idx].from_obj(state_.part_list_val_.get_cell(loc_idx)));
      }
    }
  } else {
    //do nothing
  }
  return ret;
}

void ObODPSTableRowIterator::reset()
{
  state_.reuse(); // reset state_ to initial values for rescan
}

int ObODPSTableRowIterator::StateValues::reuse()
{
  int ret = OB_SUCCESS;
  try {
    if (-1 == task_idx_) {
      // do nothing
    } else {
      if (OB_NOT_NULL(record_reader_handle_.get())) {
        record_reader_handle_->Close();
        record_reader_handle_.reset();
      }
      if (OB_ISNULL(download_handle_.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret), K(lbt()));
      } else {
        if (!is_from_gi_pump_) {
          download_handle_->Complete();
        }
        download_handle_.reset();
      }
    }
  } catch (const apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method, ignore it", K(ret), K(ex.what()));
      //LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method, ignore it", K(ret), K(ex.what()));
      //LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method, ignore it", K(ret));
    }
  }
  task_idx_ = -1;
  part_id_ = 0;
  start_ = 0;
  step_ = 0;
  count_ = 0;
  download_id_.clear();
  part_list_val_.reset();
  is_from_gi_pump_ = false;
  return ret;
}

int ObODPSTableRowIterator::retry_read_task()
{
  int ret = OB_SUCCESS;
  try {
    LOG_TRACE("before retry read task", K(ret), K(state_), K(total_count_));
    if (OB_NOT_NULL(state_.record_reader_handle_.get())) {
      state_.record_reader_handle_->Close();
      state_.record_reader_handle_.reset();
    }
    if (column_names_.size() &&
        OB_ISNULL((state_.record_reader_handle_ = state_.download_handle_->OpenReader(state_.start_ + state_.count_,
                                                                                      state_.step_ - state_.count_,
                                                                                      column_names_,
                                                                                      true)).get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else {
      state_.download_id_ = state_.download_handle_->GetDownloadId();
      LOG_TRACE("retry odps task success", K(ret), K(state_), K(total_count_));
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling odps api", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling odps api", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling odps api", K(ret));
    }
  }
  return ret;
}

int ObODPSTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObMallocHookAttrGuard guard(mem_attr_);
  int64_t returned_row_cnt = 0;
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  if (0 == column_names_.size()) {
    if (OB_SUCC(ret) &&
        state_.count_ >= state_.step_ &&
        OB_FAIL(next_task())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      } else if (0 != count){
        ret = OB_SUCCESS;
      }
    } else {
      count = std::min(capacity, state_.step_ - state_.count_);
      total_count_ += count;
      state_.count_ += count;
      for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < target_column_id_list_.count(); ++column_idx) {
        ObExpr &expr = *file_column_exprs.at(column_idx);
        if (OB_FAIL(fill_partition_list_data(expr, count))) {
          LOG_WARN("failed to fill partition list data", K(ret), K(file_column_exprs.count()));
        }
      }
    }
  } else if (state_.count_ >= state_.step_ && OB_FAIL(next_task())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next task failed", K(ret));
    } else {
      LOG_TRACE("get next task end", K(ret), K(state_));
    }
    count = 0;
  } else {
    int64_t returned_row_cnt = 0;
    try {
      while(returned_row_cnt < capacity && OB_SUCC(ret)) {
        if (!(state_.record_reader_handle_->Read(*records_[returned_row_cnt]))) {
          break;
        } else {
          ++state_.count_;
          ++total_count_;
          ++returned_row_cnt;
        }
      }
    } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
      if (OB_SUCC(ret)) {
        std::string ex_msg = ex.what();
        if (std::string::npos != ex_msg.find("EOF")) { //EOF
          LOG_TRACE("odps eof", K(ret), K(total_count_), K(returned_row_cnt), K(ex.what()));
          if (0 == returned_row_cnt && (INT64_MAX == state_.step_ || state_.count_ == state_.step_)) {
            state_.step_ = state_.count_; // goto get next task
            count = 0;
          } else if (0 == returned_row_cnt) {
            LOG_TRACE("unexpected returned_row_cnt, going to retry read task", K(total_count_), K(returned_row_cnt), K(state_), K(ret));
            if (OB_FAIL(THIS_WORKER.check_status())) {
              LOG_WARN("failed to check status", K(ret));
            } else if (OB_FAIL(retry_read_task())) {
              LOG_WARN("failed to retry read task", K(ret), K(state_));
            }
          }
        } else {
          LOG_TRACE("unexpected read error exception, going to retry read task", K(OB_ODPS_ERROR), K(total_count_), K(returned_row_cnt), K(state_), K(ret), K(ex.what()));
          if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret));
          } else if (OB_FAIL(retry_read_task())) {
            LOG_WARN("failed to retry read task", K(ret), K(state_));
          }
        }
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps exception occured when calling Read method", K(ret), K(total_count_), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps exception occured when calling Read method", K(ret), K(total_count_));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 == returned_row_cnt && (INT64_MAX == state_.step_ || state_.count_ == state_.step_)) {
      state_.step_ = state_.count_; // goto get next task
      count = 0;
    } else if (0 == returned_row_cnt) {
      // do nothing
      LOG_TRACE("expected result: already retried reading task successfully", K(total_count_), K(returned_row_cnt), K(state_), K(ret));
      count = 0;
    } else {
      int64_t data_idx = 0;
      for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < target_column_id_list_.count(); ++column_idx) {
        uint32_t target_idx = target_column_id_list_.at(column_idx);
        ObExpr &expr = *file_column_exprs.at(column_idx);
        ObObjType type = expr.obj_meta_.get_type();
        ObDatum *datums = expr.locate_batch_datums(ctx);
        if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
          if (OB_FAIL(fill_partition_list_data(expr, returned_row_cnt))) {
            LOG_WARN("failed to fill partition list data", K(ret));
          }
        } else if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, returned_row_cnt))) {
          LOG_WARN("failed to init expr vector", K(ret), K(expr));
        } else {
          apsara::odps::sdk::ODPSColumnType odps_type = column_list_.at(target_idx).type_info_.mType;
          target_idx = data_idx++;
          try {
            switch(odps_type)
            {
              case apsara::odps::sdk::ODPS_BOOLEAN:
              {
                if ((ObTinyIntType == type ||
                    ObSmallIntType == type ||
                    ObMediumIntType == type ||
                    ObInt32Type == type ||
                    ObIntType == type) && !is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const bool* v = records_[row_idx]->GetBoolValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      datums[row_idx].set_int(*v);
                    }
                  }
                } else if (ObNumberType == type && is_oracle_mode()) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    const bool* v = records_[row_idx]->GetBoolValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t in_val = *v;
                      ObNumStackOnceAlloc tmp_alloc;
                      number::ObNumber nmb;
                      OZ(ObOdpsDataTypeCastUtil::common_int_number_wrap(expr, in_val, tmp_alloc, nmb), in_val);
                      if (OB_FAIL(ret)) {
                        LOG_WARN("failed to cast int to number", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else if (ObDecimalIntType == type && is_oracle_mode()) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    const bool* v = records_[row_idx]->GetBoolValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t in_val = *v;
                      ObDecimalInt *decint = nullptr;
                      int32_t int_bytes = 0;
                      ObDecimalIntBuilder tmp_alloc;
                      ObScale out_scale = expr.datum_meta_.scale_;
                      ObScale in_scale = 0;
                      ObPrecision out_prec = expr.datum_meta_.precision_;
                      ObPrecision in_prec =
                        ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                          .get_precision();
                      const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
                      if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
                        in_prec = MAX_PRECISION_DECIMAL_INT_64;
                      }
                      if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
                        LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx), K(column_idx));
                      } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
                        ObDecimalIntBuilder res_val;
                        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                                        out_prec, expr.extra_, res_val))) {
                          LOG_WARN("scale decimal int failed", K(ret), K(row_idx), K(column_idx));
                        } else {
                          datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                        }
                      } else {
                        datums[row_idx].set_decimal_int(decint, int_bytes);
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_TINYINT:
              case apsara::odps::sdk::ODPS_SMALLINT:
              case apsara::odps::sdk::ODPS_INTEGER:
              case apsara::odps::sdk::ODPS_BIGINT:
              {
                if ((ObTinyIntType == type ||
                    ObSmallIntType == type ||
                    ObMediumIntType == type ||
                    ObInt32Type == type ||
                    ObIntType == type) && !is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const int64_t* v = records_[row_idx]->GetIntValue(target_idx, odps_type);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      datums[row_idx].set_int(*v);
                    }
                  }
                } else if (ObNumberType == type && is_oracle_mode()) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    const int64_t* v = records_[row_idx]->GetIntValue(target_idx, odps_type);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t in_val = *v;
                      ObNumStackOnceAlloc tmp_alloc;
                      number::ObNumber nmb;
                      OZ(ObOdpsDataTypeCastUtil::common_int_number_wrap(expr, in_val, tmp_alloc, nmb), in_val);
                      if (OB_FAIL(ret)) {
                        LOG_WARN("failed to cast int to number", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else if (ObDecimalIntType == type && is_oracle_mode()) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    const int64_t* v = records_[row_idx]->GetIntValue(target_idx, odps_type);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t in_val = *v;
                      ObDecimalInt *decint = nullptr;
                      int32_t int_bytes = 0;
                      ObDecimalIntBuilder tmp_alloc;
                      ObScale out_scale = expr.datum_meta_.scale_;
                      ObScale in_scale = 0;
                      ObPrecision out_prec = expr.datum_meta_.precision_;
                      ObPrecision in_prec =
                        ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                          .get_precision();
                      const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
                      if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
                        in_prec = MAX_PRECISION_DECIMAL_INT_64;
                      }
                      if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
                        LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx), K(column_idx));
                      } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
                        ObDecimalIntBuilder res_val;
                        if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                                        out_prec, expr.extra_, res_val))) {
                          LOG_WARN("scale decimal int failed", K(ret), K(row_idx), K(column_idx));
                        } else {
                          datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                        }
                      } else {
                        datums[row_idx].set_decimal_int(decint, int_bytes);
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_FLOAT:
              {
                if (ObFloatType == type) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const float* v = records_[row_idx]->GetFloatValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      datums[row_idx].set_float(*v);
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_DOUBLE:
              {
                if (ObDoubleType == type) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const double* v = records_[row_idx]->GetDoubleValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      datums[row_idx].set_double(*v);
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_DECIMAL:
              {
                if (ObDecimalIntType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetDecimalValue(target_idx, len);
                    if (v == NULL || len == 0) {
                      datums[row_idx].set_null();
                    } else {
                      ObString in_str(len, v);
                      ObDecimalIntBuilder res_val;
                      if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_decimalint_wrap(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(),
                                                          res_val))) {
                        LOG_WARN("cast string to decimal int failed", K(ret), K(row_idx), K(column_idx));
                      } else {
                        datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                      }
                    }
                  }
                } else if (ObNumberType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetDecimalValue(target_idx, len);
                    if (v == NULL || len == 0) {
                      datums[row_idx].set_null();
                    } else {
                      ObString in_str(len, v);
                      number::ObNumber nmb;
                      ObNumStackOnceAlloc tmp_alloc;
                      if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(expr, in_str, tmp_alloc, nmb))) {
                        LOG_WARN("cast string to number failed", K(ret), K(row_idx), K(column_idx));
                      } else {
                        datums[row_idx].set_number(nmb);
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_CHAR:
              {
                if (ObCharType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetStringValue(target_idx, len, apsara::odps::sdk::ODPS_CHAR);
                    if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                      datums[row_idx].set_null();
                    } else {
                      ObObjType in_type = ObCharType;
                      ObObjType out_type = ObCharType;
                      ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
                      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                      ObString in_str(len, v);
                      bool has_set_res = false;
                      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                      if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                        datums[row_idx].set_string(in_str);
                      } else if (OB_FAIL(oceanbase::sql::ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                              out_cs_type, in_str, ctx, datums[row_idx], has_set_res))) {
                        LOG_WARN("cast string to string failed", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_VARCHAR:
              {
                if (ObVarcharType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetStringValue(target_idx, len, apsara::odps::sdk::ODPS_VARCHAR);
                    if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                      datums[row_idx].set_null();
                    } else {
                      ObObjType in_type = ObVarcharType;
                      ObObjType out_type = ObVarcharType;
                      ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
                      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                      ObString in_str(len, v);
                      bool has_set_res = false;
                      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                      if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                        datums[row_idx].set_string(in_str);
                      } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                              out_cs_type, in_str, ctx, datums[row_idx], has_set_res))) {
                        LOG_WARN("cast string to string failed", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_STRING:
              case apsara::odps::sdk::ODPS_BINARY:
              {
                if (ObVarcharType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetStringValue(target_idx, len, odps_type);
                    if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                      datums[row_idx].set_null();
                    } else if (len > expr.max_length_) {
                      ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                      LOG_WARN("unexpected data length", K(ret),
                                                       K(len),
                                                       K(expr.max_length_),
                                                       K(target_column_id_list_),
                                                       K(column_idx),
                                                       K(type),
                                                       K(target_idx));
                      print_type_map_user_info(column_list_.at(target_column_id_list_.at(column_idx)).type_info_, &expr);
                    } else {
                      ObObjType in_type = ObVarcharType;
                      ObObjType out_type = ObVarcharType;
                      ObCollationType in_cs_type = apsara::odps::sdk::ODPS_STRING == odps_type ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
                      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                      ObString in_str(len, v);
                      bool has_set_res = false;
                      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                      if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                        datums[row_idx].set_string(in_str);
                      } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                              out_cs_type, in_str, ctx, datums[row_idx], has_set_res))) {
                        LOG_WARN("cast string to string failed", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else if (ObTinyTextType == type ||
                          ObTextType == type ||
                          ObLongTextType == type ||
                          ObMediumTextType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetStringValue(target_idx, len, odps_type);
                    if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                      datums[row_idx].set_null();
                    } else if (!text_type_length_is_valid_at_runtime(type, len)) {
                      ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                      LOG_WARN("unexpected data length", K(ret),
                                                       K(len),
                                                       K(expr.max_length_),
                                                       K(target_column_id_list_),
                                                       K(column_idx),
                                                       K(type),
                                                       K(target_idx));
                      print_type_map_user_info(column_list_.at(target_column_id_list_.at(column_idx)).type_info_, &expr);
                    } else {
                      ObString in_str(len, v);
                      ObObjType in_type = ObVarcharType;
                      ObCollationType in_cs_type = apsara::odps::sdk::ODPS_STRING == odps_type ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
                      if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(expr, in_str, ctx, NULL, datums[row_idx], in_type, in_cs_type))) {
                        LOG_WARN("cast string to text failed", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else if (ObRawType == type) {
                  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
                  batch_info_guard.set_batch_idx(0);
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    batch_info_guard.set_batch_idx(row_idx);
                    uint32_t len;
                    const char* v = records_[row_idx]->GetStringValue(target_idx, len, odps_type);
                    if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                      datums[row_idx].set_null();
                    } else {
                      ObString in_str(len, v);
                      bool has_set_res = false;
                      if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, datums[row_idx], has_set_res))) {
                        LOG_WARN("cast string to raw failed", K(ret), K(row_idx), K(column_idx));
                      }
                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_TIMESTAMP:
              {
                if (ObTimestampType == type && !is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const apsara::odps::sdk::TimeStamp* v = records_[row_idx]->GetTimestampValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t datetime = v->GetSecond() * USECS_PER_SEC + (v->GetNano() + 500) / 1000; // suppose odps's timezone is same to oceanbase
                      datums[row_idx].set_datetime(datetime);
                    }
                  }
                } else if (false && ObTimestampLTZType == type && is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const apsara::odps::sdk::TimeStamp* v = records_[row_idx]->GetTimestampValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {

                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
              {
                if (ObDateTimeType == type && !is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const apsara::odps::sdk::TimeStamp* v = records_[row_idx]->GetTimestampNTZValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t datetime = v->GetSecond() * USECS_PER_SEC + (v->GetNano() + 500) / 1000;
                      datums[row_idx].set_datetime(datetime);
                    }
                  }
                } else if (false && ObTimestampNanoType == type && is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const apsara::odps::sdk::TimeStamp* v = records_[row_idx]->GetTimestampNTZValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {

                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_DATE:
              {
                if (ObDateType == type && !is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const int64_t* v = records_[row_idx]->GetDateValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int32_t date = *v;
                      datums[row_idx].set_date(date);
                    }
                  }
                } else if (false && (ObDateTimeType == type || ObMySQLDateTimeType == type)
                           && is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const int64_t* v = records_[row_idx]->GetDateValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {

                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_DATETIME:
              {
                if (ObDateTimeType == type && !is_oracle_mode()) {
                  int32_t tmp_offset = 0;
                  int64_t res_offset = 0;
                  if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
                    LOG_WARN("failed to get timezone offset", K(ret));
                  } else {
                    res_offset = SEC_TO_USEC(tmp_offset);
                  }
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const int64_t* v = records_[row_idx]->GetDatetimeValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {
                      int64_t datetime = *v * 1000 + res_offset;
                      datums[row_idx].set_datetime(datetime);
                    }
                  }
                } else if (false && ObTimestampNanoType == type && is_oracle_mode()) {
                  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                    const int64_t* v = records_[row_idx]->GetDatetimeValue(target_idx);
                    if (v == NULL) {
                      datums[row_idx].set_null();
                    } else {

                    }
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
                }
                break;
              }
              case apsara::odps::sdk::ODPS_JSON:
              {
                for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < returned_row_cnt; ++row_idx) {
                  uint32_t len;
                  const char* v = records_[row_idx]->GetJsonValue(target_idx, len);
                  if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                    datums[row_idx].set_null();
                  } else {
                    datums[row_idx].set_string(v, len);
                  }
                }
                break;
              }
              default:
              {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("odps not support type", K(ret));
              }
            }
          } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
            if (OB_SUCC(ret)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("odps exception occured when calling OpenReader method", K(ret), K(ex.what()));
              LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
            }
          } catch (const std::exception& ex) {
            if (OB_SUCC(ret)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("odps exception occured when calling OpenReader method", K(ret), K(ex.what()));
              LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
            }
          } catch (...) {
            if (OB_SUCC(ret)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("odps exception occured when calling OpenReader method", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        count = returned_row_cnt;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
      file_column_exprs.at(i)->set_evaluated_flag(ctx);
    }
    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
      OZ (column_convert_expr->eval_batch(ctx, *bit_vector_cache_, count));
      if (OB_SUCC(ret)) {
        MEMCPY(column_expr->locate_batch_datums(ctx),
              column_convert_expr->locate_batch_datums(ctx), sizeof(ObDatum) * count);
        column_expr->set_evaluated_flag(ctx);
      }
      OZ(column_expr->init_vector(ctx, VEC_UNIFORM, count));
    }
  }
  OZ(calc_exprs_for_rowid(count));
  return ret;
}

int ObODPSTableRowIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  if (state_.count_ >= state_.step_ && OB_FAIL(next_task())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next task failed", K(ret));
    }
  } else {
    bool need_retry = false;
    do {
      if (OB_FAIL(inner_get_next_row(need_retry))) {
        LOG_WARN("failed to get next row inner", K(ret));
      }
    } while (OB_SUCC(ret) && OB_SUCC(THIS_WORKER.check_status()) && need_retry);
  }
  while(OB_SUCC(ret) && get_next_task_) { // used to get next task which has data need to fetch
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      bool need_retry = false;
      do {
        if (OB_FAIL(inner_get_next_row(need_retry))) {
          LOG_WARN("failed to get next row inner", K(ret));
        }
      } while (OB_SUCC(ret) && OB_SUCC(THIS_WORKER.check_status()) && need_retry);
    }
  }
  // 这里不能隐含假设：返回OB_ITER_END的时候不返回数据
  OZ(calc_exprs_for_rowid(1));
  return ret;
}


int ObODPSTableRowIterator::calc_exprs_for_rowid(const int64_t read_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_id_expr_->get_vector(eval_ctx));
      // file_id_ is mapping to state_.part_id_ in odps.
      vec->set_int(i, state_.part_id_);
    }
    file_id_expr_->set_evaluated_flag(eval_ctx);
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    OZ (line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(line_number_expr_->get_vector(eval_ctx));
      vec->set_int(i, state_.cur_line_number_ + i);
    }
    line_number_expr_->set_evaluated_flag(eval_ctx);
  }
  state_.cur_line_number_ += read_count;
  return ret;
}

int ObODPSTableRowIterator::inner_get_next_row(bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObMallocHookAttrGuard guard(mem_attr_);
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  get_next_task_ = false;
  need_retry = false;
  try {
    if (OB_ISNULL(state_.record_reader_handle_.get()) || !file_column_exprs.count()) {
      if (INT64_MAX == state_.step_ || state_.count_ == state_.step_) {
        get_next_task_ = true; // goto get next task
        state_.step_ = state_.count_;
      } else {
        ++state_.count_;
        ++total_count_;
      }
    } else {
      if (!(state_.record_reader_handle_->Read(*record_))) {
        if (INT64_MAX == state_.step_ || state_.count_ == state_.step_) {
          get_next_task_ = true; // goto get next task
          state_.step_ = state_.count_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected end", K(total_count_), K(state_), K(ret));
        }
      } else {
        ++state_.count_;
        ++total_count_;
      }
    }
  } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      std::string ex_msg = ex.what();
      if (std::string::npos != ex_msg.find("EOF")) { // EOF
        LOG_TRACE("odps eof", K(ret), K(total_count_), K(state_), K(ex.what()));
        if (INT64_MAX == state_.step_ || state_.count_ == state_.step_) {
          get_next_task_ = true; // goto get next task
          state_.step_ = state_.count_;
        } else {
          LOG_TRACE("unexpected end, going to retry read task", K(total_count_), K(state_), K(ret));
          if (OB_FAIL(retry_read_task())) {
            LOG_WARN("failed to retry read task", K(ret), K(state_));
          } else {
            need_retry = true;
          }
        }
      } else {
        LOG_WARN("odps exception occured when calling Read or Close method, going to retry read task", K(OB_ODPS_ERROR), K(ret), K(total_count_), K(ex.what()));
        if (OB_FAIL(retry_read_task())) {
          LOG_WARN("failed to retry read task", K(ret), K(state_));
        } else {
          need_retry = true;
        }
      }
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Read or Close method", K(ret), K(total_count_), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Read or Close method", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (get_next_task_) {
    // do nothing
  } else {
    int64_t data_idx = 0;
    for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < target_column_id_list_.count(); ++column_idx) {
      uint32_t target_idx = target_column_id_list_.at(column_idx);
      ObExpr &expr = *file_column_exprs.at(column_idx); // do not check null ptr

      ObObjType type = expr.obj_meta_.get_type();
      ObDatum &datum = expr.locate_datum_for_write(ctx);
      if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
        int64_t loc_idx = file_column_exprs.at(column_idx)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= state_.part_list_val_.get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcepted loc_idx", K(ret), K(loc_idx), K(state_.part_list_val_.get_count()), KP(&state_.part_list_val_));
        } else if (state_.part_list_val_.get_cell(loc_idx).is_null()) {
          datum.set_null();
        } else {
          CK (OB_NOT_NULL(datum.ptr_));
          OZ (datum.from_obj(state_.part_list_val_.get_cell(loc_idx)));
        }
      } else {
        apsara::odps::sdk::ODPSColumnType odps_type = column_list_.at(target_idx).type_info_.mType;
        target_idx = data_idx++;
        try {
          switch(odps_type)
          {
            case apsara::odps::sdk::ODPS_BOOLEAN:
            {
              if ((ObTinyIntType == type ||
                  ObSmallIntType == type ||
                  ObMediumIntType == type ||
                  ObInt32Type == type ||
                  ObIntType == type) && !is_oracle_mode()) {
                const bool* v = record_->GetBoolValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  datum.set_int(*v);
                }
              } else if (ObNumberType == type && is_oracle_mode()) {
                const bool* v = record_->GetBoolValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int64_t in_val = *v;
                  ObNumStackOnceAlloc tmp_alloc;
                  number::ObNumber nmb;
                  OZ(ObOdpsDataTypeCastUtil::common_int_number_wrap(expr, in_val, tmp_alloc, nmb), in_val);
                  if (OB_FAIL(ret)) {
                    LOG_WARN("failed to cast int to number", K(ret), K(column_idx));
                  }
                }
              } else if (ObDecimalIntType == type && is_oracle_mode()) {
                const bool* v = record_->GetBoolValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int64_t in_val = *v;
                  ObDecimalInt *decint = nullptr;
                  int32_t int_bytes = 0;
                  ObDecimalIntBuilder tmp_alloc;
                  ObScale out_scale = expr.datum_meta_.scale_;
                  ObScale in_scale = 0;
                  ObPrecision out_prec = expr.datum_meta_.precision_;
                  ObPrecision in_prec =
                    ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                      .get_precision();
                  const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
                  if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
                    in_prec = MAX_PRECISION_DECIMAL_INT_64;
                  }
                  if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
                    LOG_WARN("from_integer failed", K(ret), K(in_val), K(column_idx));
                  } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
                    ObDecimalIntBuilder res_val;
                    if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                                    out_prec, expr.extra_, res_val))) {
                      LOG_WARN("scale decimal int failed", K(ret), K(column_idx));
                    } else {
                      datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                    }
                  } else {
                    datum.set_decimal_int(decint, int_bytes);
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_TINYINT:
            case apsara::odps::sdk::ODPS_SMALLINT:
            case apsara::odps::sdk::ODPS_INTEGER:
            case apsara::odps::sdk::ODPS_BIGINT:
            {
              if ((ObTinyIntType == type ||
                  ObSmallIntType == type ||
                  ObMediumIntType == type ||
                  ObInt32Type == type ||
                  ObIntType == type) && !is_oracle_mode()) {
                const int64_t* v = record_->GetIntValue(target_idx, odps_type);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  datum.set_int(*v);
                }
              } else if (ObNumberType == type && is_oracle_mode()) {
                const int64_t* v = record_->GetIntValue(target_idx, odps_type);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int64_t in_val = *v;
                  ObNumStackOnceAlloc tmp_alloc;
                  number::ObNumber nmb;
                  OZ(ObOdpsDataTypeCastUtil::common_int_number_wrap(expr, in_val, tmp_alloc, nmb), in_val);
                  if (OB_FAIL(ret)) {
                    LOG_WARN("failed to cast int to number", K(ret), K(column_idx));
                  }
                }
              } else if (ObDecimalIntType == type && is_oracle_mode()) {
                const int64_t* v = record_->GetIntValue(target_idx, odps_type);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int64_t in_val = *v;
                  ObDecimalInt *decint = nullptr;
                  int32_t int_bytes = 0;
                  ObDecimalIntBuilder tmp_alloc;
                  ObScale out_scale = expr.datum_meta_.scale_;
                  ObScale in_scale = 0;
                  ObPrecision out_prec = expr.datum_meta_.precision_;
                  ObPrecision in_prec =
                    ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                      .get_precision();
                  const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
                  if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
                    in_prec = MAX_PRECISION_DECIMAL_INT_64;
                  }
                  if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
                    LOG_WARN("from_integer failed", K(ret), K(in_val), K(column_idx));
                  } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
                    ObDecimalIntBuilder res_val;
                    if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale,
                                                                    out_prec, expr.extra_, res_val))) {
                      LOG_WARN("scale decimal int failed", K(ret), K(column_idx));
                    } else {
                      datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                    }
                  } else {
                    datum.set_decimal_int(decint, int_bytes);
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_FLOAT:
            {
              if (ObFloatType == type) {
                const float* v = record_->GetFloatValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  datum.set_float(*v);
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_DOUBLE:
            {
              if (ObDoubleType == type) {
                const double* v = record_->GetDoubleValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  datum.set_double(*v);
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_DECIMAL:
            {
              if (ObDecimalIntType == type) {
                  uint32_t len;
                  const char* v = record_->GetDecimalValue(target_idx, len);
                  if (v == NULL || len == 0) {
                    datum.set_null();
                  } else {
                    ObString in_str(len, v);
                    ObDecimalIntBuilder res_val;
                    if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_decimalint_wrap(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(), res_val))) {
                      LOG_WARN("cast string to decimal int failed", K(ret), K(column_idx));
                    } else {
                      datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
                    }
                  }
              } else if (ObNumberType == type) {
                uint32_t len;
                const char* v = record_->GetDecimalValue(target_idx, len);
                if (v == NULL || len == 0) {
                  datum.set_null();
                } else {
                  ObString in_str(len, v);
                  number::ObNumber nmb;
                  ObNumStackOnceAlloc tmp_alloc;
                  if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(expr, in_str, tmp_alloc, nmb))) {
                    LOG_WARN("cast string to number failed", K(ret), K(column_idx));
                  } else {
                    datum.set_number(nmb);
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_CHAR:
            {
              if (ObCharType == type) {
                uint32_t len;
                const char* v = record_->GetStringValue(target_idx, len, apsara::odps::sdk::ODPS_CHAR);
                if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                  datum.set_null();
                } else {
                  ObObjType in_type = ObCharType;
                  ObObjType out_type = ObCharType;
                  ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
                  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                  ObString in_str(len, v);
                  bool has_set_res = false;
                  ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                  if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                    datum.set_string(in_str);
                  } else if (OB_FAIL(oceanbase::sql::ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                          out_cs_type, in_str, ctx, datum, has_set_res))) {
                    LOG_WARN("cast string to string failed", K(ret),  K(column_idx));
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_VARCHAR:
            {
              if (ObVarcharType == type) {
                uint32_t len;
                const char* v = record_->GetStringValue(target_idx, len, apsara::odps::sdk::ODPS_VARCHAR);
                if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                  datum.set_null();
                } else {
                  ObObjType in_type = ObVarcharType;
                  ObObjType out_type = ObVarcharType;
                  ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
                  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                  ObString in_str(len, v);
                  bool has_set_res = false;
                  ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                  if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                    datum.set_string(in_str);
                  } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                          out_cs_type, in_str, ctx, datum, has_set_res))) {
                    LOG_WARN("cast string to string failed", K(ret), K(column_idx));
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_STRING:
            case apsara::odps::sdk::ODPS_BINARY:
            {
              if (ObVarcharType == type) {
                uint32_t len;
                const char* v = record_->GetStringValue(target_idx, len, odps_type);
                if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                  datum.set_null();
                } else if (len > expr.max_length_) {
                  ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                  LOG_WARN("unexpected data length", K(ret),
                                                      K(len),
                                                      K(expr.max_length_),
                                                      K(target_column_id_list_),
                                                      K(column_idx),
                                                      K(type),
                                                      K(target_idx));
                  print_type_map_user_info(column_list_.at(target_column_id_list_.at(column_idx)).type_info_, &expr);
                } else {
                  ObObjType in_type = ObVarcharType;
                  ObObjType out_type = ObVarcharType;
                  ObCollationType in_cs_type = apsara::odps::sdk::ODPS_STRING == odps_type ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
                  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
                  ObString in_str(len, v);
                  bool has_set_res = false;
                  ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
                  if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                    datum.set_string(in_str);
                  } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_string_wrap(expr, in_type, in_cs_type, out_type,
                                          out_cs_type, in_str, ctx, datum, has_set_res))) {
                    LOG_WARN("cast string to string failed", K(ret), K(column_idx));
                  }
                }
              } else if (ObTinyTextType == type ||
                        ObTextType == type ||
                        ObLongTextType == type ||
                        ObMediumTextType == type) {
                uint32_t len;
                const char* v = record_->GetStringValue(target_idx, len, odps_type);
                if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                  datum.set_null();
                } else if (!text_type_length_is_valid_at_runtime(type, len)) {
                  ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                  LOG_WARN("unexpected data length", K(ret),
                                                      K(len),
                                                      K(expr.max_length_),
                                                      K(target_column_id_list_),
                                                      K(column_idx),
                                                      K(type),
                                                      K(target_idx));
                  print_type_map_user_info(column_list_.at(target_column_id_list_.at(column_idx)).type_info_, &expr);
                } else {
                  ObString in_str(len, v);
                  ObObjType in_type = ObVarcharType; // lcqlog todo ObHexStringType ?
                  ObCollationType in_cs_type = apsara::odps::sdk::ODPS_STRING == odps_type ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
                  if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(expr, in_str, ctx, NULL, datum, in_type, in_cs_type))) {
                    LOG_WARN("cast string to text failed", K(ret), K(column_idx));
                  }
                }
              } else if (ObRawType == type) {
                uint32_t len;
                const char* v = record_->GetStringValue(target_idx, len, odps_type);
                if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                  datum.set_null();
                } else {
                  ObString in_str(len, v);
                  bool has_set_res = false;
                  if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, in_str, ctx, datum, has_set_res))) {
                    LOG_WARN("cast string to raw failed", K(ret), K(column_idx));
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_TIMESTAMP:
            {
              if (ObTimestampType == type && !is_oracle_mode()) {
                  const apsara::odps::sdk::TimeStamp* v = record_->GetTimestampValue(target_idx);
                  if (v == NULL) {
                    datum.set_null();
                  } else {
                    int64_t datetime = v->GetSecond() * USECS_PER_SEC + (v->GetNano() + 500) / 1000; // suppose odps's timezone is same to oceanbase
                    datum.set_datetime(datetime);
                  }
              } else if (false && ObTimestampLTZType == type && is_oracle_mode()) {
                const apsara::odps::sdk::TimeStamp* v = record_->GetTimestampValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {

                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
            {
              if (ObDateTimeType == type && !is_oracle_mode()) {
                const apsara::odps::sdk::TimeStamp* v = record_->GetTimestampNTZValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int64_t datetime = v->GetSecond() * USECS_PER_SEC + (v->GetNano() + 500) / 1000;
                  datum.set_datetime(datetime);
                }
              } else if (false && ObTimestampNanoType == type && is_oracle_mode()) {
                const apsara::odps::sdk::TimeStamp* v = record_->GetTimestampNTZValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {

                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_DATE:
            {
              if (ObDateType == type && !is_oracle_mode()) {
                const int64_t* v = record_->GetDateValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {
                  int32_t date = *v;
                  datum.set_date(date);
                }
              } else if (false && (ObDateTimeType == type || ObMySQLDateTimeType == type) && is_oracle_mode()) {
                const int64_t* v = record_->GetDateValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {

                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_DATETIME:
            {
              if (ObDateTimeType == type && !is_oracle_mode()) {
                const int64_t* v = record_->GetDatetimeValue(target_idx);
                int32_t tmp_offset = 0;
                int64_t res_offset = 0;
                if (v == NULL) {
                  datum.set_null();
                } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
                  LOG_WARN("failed to get timezone offset", K(ret));
                } else {
                  res_offset = SEC_TO_USEC(tmp_offset);
                  int64_t datetime = *v * 1000 + res_offset;
                  datum.set_datetime(datetime);
                }
              } else if (false && ObTimestampNanoType == type && is_oracle_mode()) {
                const int64_t* v = record_->GetDatetimeValue(target_idx);
                if (v == NULL) {
                  datum.set_null();
                } else {

                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
              }
              break;
            }
            case apsara::odps::sdk::ODPS_JSON:
            {
              uint32_t len;
              const char* v = record_->GetJsonValue(target_idx, len);
              if (v == NULL || (0 == len && lib::is_oracle_mode())) {
                datum.set_null();
              } else {
                datum.set_string(v, len);
              }
              break;
            }
            default:
            {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("odps not support type", K(ret));
            }
          }
        } catch (apsara::odps::sdk::OdpsTunnelException& ex) {
          if (OB_SUCC(ret)) {
            ret = OB_ODPS_ERROR;
            LOG_WARN("odps exception occured when calling OpenReader method", K(ret), K(ex.what()));
            LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
          }
        } catch (const std::exception& ex) {
          if (OB_SUCC(ret)) {
            ret = OB_ODPS_ERROR;
            LOG_WARN("odps exception occured when calling OpenReader method", K(ret), K(ex.what()));
            LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
          }
        } catch (...) {
          if (OB_SUCC(ret)) {
            ret = OB_ODPS_ERROR;
            LOG_WARN("odps exception occured when calling OpenReader method", K(ret));
          }
        }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
      file_column_exprs.at(i)->set_evaluated_flag(ctx);
    }
    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
      ObDatum *convert_datum = NULL;
      OZ (column_convert_expr->eval(ctx, convert_datum));
      if (OB_SUCC(ret)) {
        column_expr->locate_datum_for_write(ctx) = *convert_datum;
        column_expr->set_evaluated_flag(ctx);
      }
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::init_downloader(int64_t bucket_size) {
  int ret = OB_SUCCESS;
  const char *ODPS_TABLE_READER = "OdpsTableReader";
  const int64_t ITER_ALLOC_TOTAL_LIMIT = 1024 * 1024 * 1024;
  if (inited_){
    // do nothing
  } else if (!odps_mgr_map_.created() && OB_FAIL(odps_mgr_map_.create(bucket_size, "OdpsTable","OdpsTableReader"))) {
    LOG_WARN("failed to init odps_mgr_map_", K(ret), K(bucket_size));
  } else if (OB_FAIL(fifo_alloc_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         ODPS_TABLE_READER,
                                         MTL_ID(),
                                         ITER_ALLOC_TOTAL_LIMIT))) {
    LOG_WARN("failed to init fifo_alloc_", K(ret));
  } else {
    inited_ = true;
    is_download_ = true;
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::fetch_row_count(uint64_t tenant_id,
                                                  const ObIArray<ObExternalFileInfo> &external_table_files,
                                                  const ObString &properties,
                                                  bool &use_partition_gi)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  ObODPSTableRowIterator odps_driver;
  common::ObArenaAllocator arena_alloc;
  use_partition_gi = false;
  int64_t uncollect_statistics_part_cnt = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t max_parttition_count_to_collect_statistic = 5;
  if (OB_LIKELY(tenant_config.is_valid())) {
    max_parttition_count_to_collect_statistic = tenant_config->_max_partition_count_to_collect_statistic;
  }
  for (int64_t i = 0; i < external_table_files.count(); ++i) {
    if (external_table_files.at(i).file_size_ < 0 && ++uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
      break;
    }
  }
  if (uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
    use_partition_gi = true;
  } else if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_tunnel(external_odps_format.odps_format_))) {
    LOG_WARN("failed to init tunnel", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
      const share::ObExternalFileInfo &odps_partition = external_table_files.at(i);
      apsara::odps::sdk::IDownloadPtr odps_partition_downloader = NULL;
      if (0 != odps_partition.file_id_) {
        if (INT64_MAX == odps_partition.file_id_ && 0 == odps_partition.file_url_.compare("#######DUMMY_FILE#######")) {
          // do nothing
          *(const_cast<int64_t*>(&odps_partition.file_size_)) = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected file id", K(ret), K(i), K(odps_partition.file_id_), K(odps_partition.part_id_));
        }
      } else if (odps_partition.file_size_ >= 0) {
        // do nothing
      } else if (OB_FAIL(odps_driver.create_downloader(odps_partition.file_url_,
                                                       odps_partition_downloader))) {
        LOG_WARN("failed create odps partition downloader", K(ret), K(i), K(odps_partition.part_id_), K(odps_partition.file_url_));
      } else {
        *(const_cast<int64_t*>(&odps_partition.file_size_)) = odps_partition_downloader->GetRecordCount();
        odps_partition_downloader->Complete();
      }
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::fetch_row_count(const ObString part_spec,
                                                  const ObString &properties,
                                                  int64_t &row_count)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  ObODPSTableRowIterator odps_driver;
  common::ObArenaAllocator arena_alloc;
  row_count = 0;
  if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_tunnel(external_odps_format.odps_format_))) {
    LOG_WARN("failed to init tunnel", K(ret));
  } else {
    apsara::odps::sdk::IDownloadPtr odps_partition_downloader = NULL;
    if (OB_FAIL(odps_driver.create_downloader(part_spec, odps_partition_downloader))) {
      LOG_WARN("failed create odps partition downloader", K(ret), K(part_spec));
    } else {
      row_count = odps_partition_downloader->GetRecordCount();
      odps_partition_downloader->Complete();
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::create_upload_session(const sql::ObODPSGeneralFormat &odps_format,
                                                        const ObString &external_partition,
                                                        bool is_overwrite,
                                                        apsara::odps::sdk::IUploadPtr &upload)
{
  int ret = OB_SUCCESS;
  apsara::odps::sdk::Configuration conf;
  apsara::odps::sdk::OdpsTunnel tunnel;
  const char* account_type = "";
  try {
    conf.SetEndpoint(std::string(odps_format.endpoint_.ptr(), odps_format.endpoint_.length()));
    conf.SetUserAgent("OB_ACCESS_ODPS");
    conf.SetTunnelQuotaName(std::string(odps_format.quota_.ptr(), odps_format.quota_.length()));
    if (0 == odps_format.compression_code_.case_compare("zlib")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZLIB_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("zstd")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ZSTD_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("lz4")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::LZ4_COMPRESS);
    } else if (0 == odps_format.compression_code_.case_compare("odps_lz4")) {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::ODPS_LZ4_COMPRESS);
    } else {
      conf.SetCompressOption(apsara::odps::sdk::CompressOption::NO_COMPRESS);
    }
    if (0 == odps_format.access_type_.case_compare("aliyun") ||
        odps_format.access_type_.empty()) {
      account_type = apsara::odps::sdk::ACCOUNT_ALIYUN;
    } else if (0 == odps_format.access_type_.case_compare("sts")) {
      account_type = apsara::odps::sdk::ACCOUNT_STS;
    } else if (0 == odps_format.access_type_.case_compare("app")) {
      account_type = apsara::odps::sdk::ACCOUNT_APPLICATION;
    } else if (0 == odps_format.access_type_.case_compare("token")
               || 0 == odps_format.access_type_.case_compare("domain")
               || 0 == odps_format.access_type_.case_compare("taobao")) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this ODPS access type");
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported access type", K(ret), K(odps_format.access_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this ODPS access type");
    }
    if (OB_SUCC(ret)) {
      apsara::odps::sdk::Account account(std::string(account_type),
                                       std::string(odps_format.access_id_.ptr(), odps_format.access_id_.length()),
                                       std::string(odps_format.access_key_.ptr(), odps_format.access_key_.length()));
      conf.SetAccount(account);
      tunnel.Init(conf);
      if (OB_UNLIKELY(!(upload = tunnel.CreateUpload(
                              std::string(odps_format.project_.ptr(), odps_format.project_.length()),
                              std::string(odps_format.table_.ptr(), odps_format.table_.length()),
                              std::string(external_partition.ptr(), external_partition.length()),
                              "",
                              is_overwrite,
                              std::string(odps_format.schema_.ptr(), odps_format.schema_.length()))))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when create odps upload session", K(ret));
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::init_uploader(const ObString &properties,
                                                const ObString &external_partition,
                                                bool is_overwrite,
                                                int64_t parallel)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_properties;
  apsara::odps::sdk::IUploadPtr upload;
  apsara::odps::sdk::IRecordWriterPtr record_writer;
  void *ptr;
  OdpsUploader *uploader;
  if (inited_) {
    // do nothing
  } else if (properties.empty()) {
    // do nothing
  } else if (OB_FAIL(external_properties.load_from_string(properties, arena_alloc_))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (sql::ObExternalFileFormat::ODPS_FORMAT != external_properties.format_type_) {
    // do nothing
  } else if (!odps_mgr_map_.created() &&
             OB_FAIL(odps_mgr_map_.create(parallel, "IntoOdps"))) {
    LOG_WARN("create hash table failed", K(ret), K(parallel));
  } else if (OB_FAIL(external_properties.odps_format_.decrypt())) {
    LOG_WARN("failed to decrypt odps format", K(ret));
  } else {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
    try {
      if (OB_FAIL(create_upload_session(external_properties.odps_format_,
                                        external_partition,
                                        is_overwrite,
                                        upload))) {
        LOG_WARN("failed to create upload session", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < parallel; i++) {
          if (OB_UNLIKELY(!(record_writer = upload->OpenWriter(i, true)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexcepted null ptr", K(ret));
          } else if (OB_ISNULL(ptr = arena_alloc_.alloc(sizeof(OdpsUploader)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate uploader", K(ret), K(sizeof(OdpsUploader)));
          } else {
            uploader = new(ptr) OdpsUploader();
            uploader->record_writer_ = record_writer;
            uploader->upload_ = upload;
          }
          if (OB_SUCC(ret)
              && OB_FAIL(odps_mgr_map_.set_refactored(i, reinterpret_cast<int64_t>(uploader)))) {
            LOG_WARN("failed to set refactored", K(ret), K(i));
          }
        }
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
    is_download_ = false;
    ATOMIC_STORE(&ref_, parallel);
    LOG_TRACE("succ to init odps uploader", K(ret), K(ref_));
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::get_odps_downloader(int64_t part_id, apsara::odps::sdk::IDownloadPtr &downloader)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  OdpsPartitionDownloader *odps_downloader = NULL;
  if (OB_FAIL(odps_mgr_map_.get_refactored(part_id, value))) {
    LOG_WARN("failed to get downloader", K(ret), K(part_id));
  } else if (OB_ISNULL(odps_downloader = reinterpret_cast<OdpsPartitionDownloader *>(value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(ret), K(part_id), K(value));
  } else { // wait tunnel to be ready
    int64_t wait_times = 0;
    //timeout is 60s, the initialization of the downloader typically takes under 5 seconds,
    // and 60 seconds is sufficient for that.
    int64_t timeout_ts = ObTimeUtility::current_time() + 60000000;
    while (0 == odps_downloader->downloader_init_status_ && OB_SUCC(THIS_WORKER.check_status())) { // wait odps_downloader to finish initialization
      odps_downloader->tunnel_ready_cond_.lock();
      if (0 == odps_downloader->downloader_init_status_) {
        odps_downloader->tunnel_ready_cond_.wait_us(1 * 1000 * 1000); // wait 1s every loop
      }
      odps_downloader->tunnel_ready_cond_.unlock();
      if (0 == wait_times % 100) {
        LOG_TRACE("waiting odps_downloader to initialize", K(wait_times), K(part_id), KP(odps_downloader), K(ret));
      }
      int64_t time_now_ts = ObTimeUtility::current_time();
      if (timeout_ts < time_now_ts) {
        ret = OB_TIMEOUT;
        LOG_WARN("waiting downloader initializing timeout", K(ret), K(part_id), K(time_now_ts), K(timeout_ts));
        break;
      }
      ++wait_times;
    }
    if (OB_SUCC(ret) && odps_downloader->downloader_init_status_ < 0) { // odps_downloader failed to initialize
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("odps_downloader was failed to initialize", K(ret), K(part_id), K(value), KP(odps_downloader));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(odps_downloader->odps_partition_downloader_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(ret), K(part_id), K(value));
  } else {
    downloader = odps_downloader->odps_partition_downloader_;
    LOG_TRACE("succ to get odps_downloader from map", K(ret), K(part_id));
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::get_odps_uploader(int64_t task_id,
                                                    apsara::odps::sdk::IUploadPtr &upload,
                                                    apsara::odps::sdk::IRecordWriterPtr &record_writer)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  OdpsUploader *uploader = NULL;
  if (OB_FAIL(odps_mgr_map_.get_refactored(task_id, value))) {
    LOG_WARN("failed to get uploader", K(ret), K(task_id));
  } else if (OB_ISNULL(uploader = reinterpret_cast<OdpsUploader *>(value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(ret), K(value));
  } else if (OB_UNLIKELY(!uploader->upload_ || !uploader->record_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null ptr", K(ret));
  } else {
    upload = uploader->upload_;
    record_writer = uploader->record_writer_;
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::commit_upload()
{
  int ret = OB_SUCCESS;
  std::vector<uint32_t> blocks;
  uint32_t block_id = 0;
  uint32_t task_count = static_cast<uint32_t>(odps_mgr_map_.size());
  OdpsUploader *uploader = NULL;
  try {
    for (OdpsMgrMap::iterator iter = odps_mgr_map_.begin();
         OB_SUCC(ret) && iter != odps_mgr_map_.end(); iter++) {
      if (OB_ISNULL(uploader = reinterpret_cast<OdpsUploader *>(iter->second))
          || OB_UNLIKELY(!uploader->record_writer_ || !uploader->upload_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else {
        uploader->record_writer_->Close();
        blocks.push_back(block_id);
        block_id++;
        // 所有线程都成功才commit
        if (block_id == task_count && true == ATOMIC_LOAD(&need_commit_)) {
          uploader->upload_->Commit(blocks);
        }
        uploader->~OdpsUploader();
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when commit", K(ret));
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::reset()
{
  int ret = OB_SUCCESS;
  DeleteDownloaderFunc delete_func(&fifo_alloc_);
  if (!inited_) {
    // do nothing
  } else if (is_download_ && OB_FAIL(odps_mgr_map_.foreach_refactored(delete_func))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    if (is_download_) {
      fifo_alloc_.destroy();
    }
    odps_mgr_map_.destroy();
  }
  if (OB_SUCC(ret) && delete_func.err_occurred()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error occurred in DeleteDownloaderFunc", K(ret), K(delete_func.get_err()));
  }
  inited_ = false;
  is_download_ = true;
  return ret;
}

int ObOdpsPartitionDownloaderMgr::DeleteDownloaderFunc::operator()(common::hash::HashMapPair<int64_t, int64_t> &kv)
{
  int ret = OB_SUCCESS;
  int64_t part_id = kv.first;
  int64_t value = kv.second;
  OdpsPartitionDownloader *downloader = reinterpret_cast<OdpsPartitionDownloader *>(value);
  if (OB_ISNULL(downloader)) {
    // ignore ret
    err_ = (err_ == ErrType::SUCCESS) ? ErrType::DOWNLOADER_IS_NULL : err_;
    LOG_WARN("unexpected error in DeleteDownloaderFunc", K(ret), K(value), K(part_id), K(err_));// ret is still OB_SUCCESS
  } else {
    downloader->reset();
    if (OB_ISNULL(downloader_alloc_)) {
      err_ = (err_ == ErrType::SUCCESS) ? ErrType::ALLOC_IS_NULL : err_;
      LOG_WARN("unexpected error in DeleteDownloaderFunc", K(ret), K(value), K(part_id), K(err_));// ret is still OB_SUCCESS
    } else {
      downloader_alloc_->free(downloader);
    }
  }
  return ret;
}

int ObOdpsPartitionDownloaderMgr::OdpsPartitionDownloader::reset()
{
  int ret = OB_SUCCESS;
  downloader_init_status_ = 0; // reset to uninitialized status
  try {
    if (OB_ISNULL(odps_partition_downloader_.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else {
      odps_partition_downloader_->Complete();
      odps_partition_downloader_.reset();
    }
  } catch (const apsara::odps::sdk::OdpsTunnelException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occured when calling Complete method", K(ret));
    }
  }
  return ret;
}

} // sql
} // oceanbase
#endif