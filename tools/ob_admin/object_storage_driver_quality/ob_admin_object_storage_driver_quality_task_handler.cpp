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

#include "ob_admin_object_storage_driver_quality.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{
//=========================== OSDQTaskHandler ==================================
OSDQTaskHandler::OSDQTaskHandler()
  : prob_of_writing_old_data_(DEFAULT_PROB_OF_WRITING_OLD_DATA),
    prob_of_parallel_(DEFAULT_PROB_OF_PARALLEL),
    is_inited_(false),
    is_stopped_(true),
    tg_id_(-1),
    thread_cnt_(DEFAULT_THREAD_CNT),
    base_uri_(nullptr),
    metric_(nullptr),
    file_set_(nullptr),
    storage_info_(nullptr),
    adapter_()
{}

OSDQTaskHandler::~OSDQTaskHandler()
{}

int OSDQTaskHandler::init(
    const char *base_uri,
    OSDQMetric *metric,
    OSDQFileSet *file_set,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "task handler init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(base_uri) || OB_ISNULL(metric) || OB_ISNULL(storage_info)
      || OB_ISNULL(file_set) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(metric), KPC(storage_info), K(storage_info->is_valid()), K(file_set));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::COMMON_QUEUE_THREAD, tg_id_))) {
    OB_LOG(WARN, "create task handler thread failed", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SET_HANDLER(tg_id_, *this))) {
    OB_LOG(WARN, "failed set hanlder", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_cnt_))) {
    OB_LOG(WARN, "failed set thread cnt", KR(ret), K(tg_id_), K(thread_cnt_));
  } else {
    TG_MGR.tgs_[tg_id_]->set_queue_size(DEFAULT_QUEUE_SIZE);
    base_uri_ = base_uri;
    metric_ = metric;
    file_set_ = file_set;
    storage_info_ = storage_info;
    for (int i = 0; i < MAX_OPERATE_TYPE; i++) {
      op_type_weights_[i] = 1;
    }
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int OSDQTaskHandler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    OB_LOG(WARN, "start thread pool faield", KR(ret));
  } else {
    is_stopped_ = false;
    op_type_random_boundaries_[0] = op_type_weights_[0];
    for (int i = 1; i < MAX_OPERATE_TYPE; i++) {
      op_type_random_boundaries_[i] = op_type_random_boundaries_[i - 1] + op_type_weights_[i];
    }
    OB_LOG(INFO, "start OSDQTaskHandler success");
    OSDQLogEntry::print_log("TASK HANDLER", "task handler has started");
  }
  return ret;
}

void OSDQTaskHandler::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    is_stopped_ = true;
    TG_STOP(tg_id_);
    OB_LOG(INFO, "stop OSDQTaskHandler");
  }
}

int OSDQTaskHandler::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    TG_WAIT(tg_id_);
    OB_LOG(INFO, "wait OSDQTaskHandler success", KR(ret), K(tg_id_));
  }
  return ret;
}

int OSDQTaskHandler::set_thread_cnt(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_cnt))) {
    OB_LOG(WARN, "failed set thread cnt", KR(ret), K(tg_id_), K(thread_cnt));
  } else {
    thread_cnt_ = thread_cnt;
  }
  return ret;
}

void OSDQTaskHandler::destroy()
{
  if (OB_LIKELY(tg_id_ >= 0)) {
    stop();
    wait();
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_inited_ = false;
  is_stopped_ = true;
  metric_ = nullptr;
  file_set_ = nullptr;
  storage_info_ = nullptr;
  OSDQLogEntry::print_log("TASK HANDLER", "task handler has destroy");
}

void OSDQTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  OSDQTask *task_p = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(task));
  } else if (FALSE_IT(task_p = reinterpret_cast<OSDQTask *>(task))) {
  } else if (OB_UNLIKELY(!task_p->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(task_p));
  } else {
    task_p->start_time_stamp_us_ = ObTimeUtility::current_time();
    if (task_p->op_type_ == WRITE_SINGLE_FILE
        || task_p->op_type_ == MULTIPART_WRITE
        || task_p->op_type_ == APPEND_WRITE) {
      if (OB_FAIL(handle_write_task(task_p))) {
        OB_LOG(WARN, "failed handle write task", KR(ret), KPC(task_p));
      }
    } else if (task_p->op_type_ == READ_SINGLE_FILE) {
      if (OB_FAIL(handle_read_single_task(task_p))) {
        OB_LOG(WARN, "failed handle read single task", KR(ret), KPC(task_p));
      }
    } else if (task_p->op_type_ == DEL_FILE) {
      if (OB_FAIL(handle_del_task(task_p))) {
        OB_LOG(WARN, "failed handle del task", KR(ret), KPC(task_p));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid task op type", KR(ret), K(task_p->op_type_));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(task_p)) {
        const double cost_time = (ObTimeUtility::current_time() - task_p->start_time_stamp_us_) / 1000000;
        std::ostringstream oss;
        oss << " error code: " << ret  << " " << common::ob_error_name(ret) \
            << " cost_time:" << std::setprecision(3) << cost_time << "s";
        OSDQLogEntry::print_log("TASK FAILED", oss.str(), RED_COLOR_PREFIX);
      }
    }
  }

  if (OB_NOT_NULL(task_p)) {
    if (OB_FAIL(metric_->sub_queued_entry())) {
      OB_LOG(WARN, "failed sub queued entry", KR(ret), KPC(task_p));
    }
    free_task(task_p);
    task = nullptr;
  }
  push_req_result_(ret);
}

int OSDQTaskHandler::handle_write_single_task_helper_(const OSDQTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adapter_.write_single_file(task->uri_, storage_info_,
          task->buf_, task->buf_len_,
          ObStorageIdMod::get_default_id_mod()))) {
    OB_LOG(WARN, "failed write single file", KR(ret), KPC(task), KPC(storage_info_));
  }
  return ret;
}

int OSDQTaskHandler::handle_multipart_write_task_helper_(const OSDQTask *task)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  ObIODevice *device_handle = nullptr;
  ObIOFd fd;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;

  // split the buf at 8MB
  const int64_t PART_SIZE = 8 * 1024 * 1024;
  const int64_t PART_COUNTS = (task->buf_len_ + PART_SIZE - 1) / PART_SIZE;

  if (OB_FAIL(adapter_.open_with_access_type(device_handle, fd, storage_info_, task->uri_,
          access_type, ObStorageIdMod::get_default_id_mod()))) {
    OB_LOG(WARN, "failed to open device with access type", KR(ret), KPC(task));
  }
  std::vector<ObIOHandle> io_handles(PART_COUNTS);
  for (int i = 0; i < PART_COUNTS && OB_SUCC(ret); i++) {
    const int64_t START_POS = i * PART_SIZE;
    const int64_t PART_LENGTH = min((i + 1) * PART_SIZE, task->buf_len_) - START_POS;
    if (FAILEDx(adapter_.async_upload_data(*device_handle, fd, task->buf_ + START_POS, 0/*offset*/,
          PART_LENGTH, io_handles[i]))) {
      OB_LOG(WARN, "failed to start async upload task!", KR(ret), KPC(task));
    }
  }
  for (int i = 0; i < PART_COUNTS && OB_SUCC(ret); i++) {
    if (FAILEDx(io_handles[i].wait())) {
      OB_LOG(WARN, "failed to wait async upload data finish", KR(ret), KPC(task));
    }
  }

  if (FAILEDx(adapter_.complete(*device_handle, fd))) {
    OB_LOG(WARN, "failed to complete", KR(ret), KPC(task));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(adapter_.close_device_and_fd(device_handle, fd))) {
    ret = COVER_SUCC(tmp_ret);
    OB_LOG(WARN, "failed to close file and release device", KR(ret), KPC(task));
  }
  return ret;
}

int OSDQTaskHandler::handle_append_write_task_helper_(const OSDQTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = task->buf_len_;
  const int64_t write_cnt = 4;
  const int64_t write_size_once = buf_len / write_cnt;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
  int64_t write_size = 0;
  for (int i = 0; i < write_cnt && OB_SUCC(ret); i++) {
    const int64_t offset = write_size_once * i;
    const bool is_can_seal = (i + 1 == write_cnt);
    const int64_t expected_write_size = is_can_seal ? (buf_len - offset) : write_size_once;
    if (FAILEDx(adapter_.pwrite(task->uri_, storage_info_, task->buf_ + offset,
            offset, expected_write_size, access_type,
            write_size, is_can_seal, ObStorageIdMod::get_default_id_mod()))) {
      OB_LOG(WARN, "failed exec pwrite", KR(ret), K(i), K(offset), KPC(task));
    } else if (OB_UNLIKELY(write_size != expected_write_size)) {
      OB_LOG(WARN, "pwrite operation write size not as expected", KR(ret), KPC(task), K(write_size), K(expected_write_size));
    }
  }

  return ret;
}

bool OSDQTaskHandler::check_parallel_write_result_(
    const OSDQOpType op_type1,
    const int ret1,
    const OSDQOpType op_type2,
    const int ret2)
{
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  if ((op_type1 == WRITE_SINGLE_FILE || op_type1 == MULTIPART_WRITE)
      && (op_type2 == WRITE_SINGLE_FILE || op_type2 == MULTIPART_WRITE)) {
    // in this case, no operation is append write
    if (OB_UNLIKELY(ret1 != OB_SUCCESS || ret2 != OB_SUCCESS)) {
      bool_ret = false;
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "parallel write should success when two type is write_single_file or multipart write", KR(ret),
          K(op_type1), K(ret1), K(op_type2), K(ret2));
    }
  } else {
    // in this case, at least one operation is append write
    if (storage_info_->get_type() == ObStorageType::OB_STORAGE_S3) {
      if (OB_UNLIKELY(ret1 != OB_SUCCESS || ret2 != OB_SUCCESS)) {
        bool_ret = false;
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "parallel append write should succeed when storage type is s3", KR(ret),
          K(op_type1), K(ret1), K(op_type2), K(ret2));
      }
    } else if (storage_info_->get_type() == ObStorageType::OB_STORAGE_OSS) {
      // if the storage type is oss, the append write may fail if the append operation is performed
      // after the single or multi-part write operation.
      // but if two parallel operation are both append write operation, they should succeed.
      if (op_type1 == APPEND_WRITE && op_type2 == APPEND_WRITE) {
        if (OB_UNLIKELY(ret1 != OB_SUCCESS || ret2 != OB_SUCCESS)) {
          bool_ret = false;
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "the parallel append write operation should succeed", KR(ret),
              K(op_type1), K(ret1), K(op_type2), K(ret2));
        }
      } else {
        if (OB_UNLIKELY((op_type1 != APPEND_WRITE && ret1 != OB_SUCCESS) || (op_type2 != APPEND_WRITE && ret2 != OB_SUCCESS))) {
          bool_ret = false;
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "the write single operation or multi-part write should success",
              KR(ret), KPC(storage_info_), K(op_type1), K(ret1), K(op_type2), K(ret2));
        }
      }
    } else {
      bool_ret = false;
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "error storage type", KR(ret), KPC(storage_info_));
    }
  }
  return bool_ret;
}

void OSDQTaskHandler::push_req_result_(const int ret)
{
  lib::ObMutexGuard guard(mutex_);
  req_results_.push_back(ret);
}

void OSDQTaskHandler::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  OSDQTask *task_p = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(task));
  } else if (FALSE_IT(task_p = reinterpret_cast<OSDQTask *>(task))) {
  } else if (OB_FAIL(metric_->sub_queued_entry())) {
    OB_LOG(WARN, "failed sub queued entry in metric", KR(ret));
  } else {
    free_task(task_p);
    task = nullptr;
  }
}

int OSDQTaskHandler::push_task(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_stopped_)) {
    OB_LOG(WARN, "task handler is stopped", KR(ret), K(is_stopped_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(task));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    OB_LOG(WARN, "failed push task in task handler", KR(ret), KPC(task));
  } else if (OB_FAIL(metric_->add_queued_entry())){
    OB_LOG(WARN, "failed add queued entry in metric", KR(ret));
  }
  return ret;
}

int OSDQTaskHandler::set_operator_weight(const OSDQOpType op_type, const int64_t op_type_weight)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_stopped_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "set op type weight is disable when task handler is running", KR(ret), K(is_stopped_));
  } else if (OB_UNLIKELY(op_type == MAX_OPERATE_TYPE || op_type_weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(op_type), K(op_type_weight));
  } else {
    op_type_weights_[op_type] = op_type_weight;
  }
  return ret;
}

int OSDQTaskHandler::set_prob_of_writing_old_data(const int64_t prob)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prob < 0 || prob >= 100)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(prob));
  } else {
    prob_of_writing_old_data_ = prob;
  }
  return ret;
}

int OSDQTaskHandler::set_prob_of_parallel(const int64_t prob)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prob < 0 || prob >= 100)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(prob));
  } else {
    prob_of_parallel_ = prob;
  }
  return ret;
}

int OSDQTaskHandler::gen_task(OSDQTask *&task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task = static_cast<OSDQTask *>(malloc(sizeof(OSDQTask))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed allocate memory for OSDQTask", KR(ret));
  } else if (FALSE_IT(task = new(task)OSDQTask())) {
  } else {
    int64_t weight_sum = op_type_random_boundaries_[MAX_OPERATE_TYPE - 1];
    if (file_set_->size() == 0) {
      weight_sum = op_type_random_boundaries_[APPEND_WRITE];
    }
    const int64_t rnd = ObRandom::rand(1, weight_sum);
    if (rnd <= op_type_random_boundaries_[WRITE_SINGLE_FILE]) {
      ret = gen_write_task(task, WRITE_SINGLE_FILE);
    } else if (rnd <= op_type_random_boundaries_[MULTIPART_WRITE]) {
      ret = gen_write_task(task, MULTIPART_WRITE);
    } else if (rnd <= op_type_random_boundaries_[APPEND_WRITE]) {
      ret = gen_write_task(task, APPEND_WRITE);
    } else if (rnd <= op_type_random_boundaries_[READ_SINGLE_FILE]) {
      ret = gen_read_single_task(task);
    } else if (rnd <= op_type_random_boundaries_[DEL_FILE]) {
      ret = gen_del_task(task);
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "wrong rnd", KR(ret));
    }
    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed gen task", KR(ret), KPC(task));
    }
  }

  if (OB_FAIL(ret)) {
    push_req_result_(ret);
    free_task(task);
  }
  return ret;
}

int OSDQTaskHandler::gen_write_task(OSDQTask *task, const OSDQOpType op_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(op_type != WRITE_SINGLE_FILE
             && op_type != MULTIPART_WRITE && op_type != APPEND_WRITE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(task), K(op_type));
  } else {
    // random choosing whether to write a new file or an old one
    // 20% chance of writing old files
    bool is_write_new = true;
    int rnd = ObRandom::rand(0, 100);
    if (rnd <= prob_of_writing_old_data_ && file_set_->size() != 0) {
      is_write_new = false;
    }
    int64_t object_id = 0;
    // when op_type is append_write, only new files can be written.
    if (is_write_new || op_type == APPEND_WRITE) {
      object_id = OSDQIDGenerator::get_instance().get_next_id();
      char object_name[MAX_OBJECT_NAME_LENGTH] = { 0 };
      if (FAILEDx(construct_object_name(object_id, object_name, MAX_OBJECT_NAME_LENGTH))) {
        OB_LOG(WARN, "failed construct object name", KR(ret), K(object_id));
      } else if (OB_ISNULL(task->uri_ = static_cast<char *>(malloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed allocate memory for file path", KR(ret), K(OB_MAX_URI_LENGTH));
      } else if (OB_FAIL(construct_file_path(base_uri_, object_name, task->uri_, OB_MAX_URI_LENGTH))) {
        OB_LOG(WARN, "failed construct file path", KR(ret), K(base_uri_), K(object_name));
      }
    } else {
      if (FAILEDx(file_set_->fetch_and_delete_file(object_id, task->uri_))) {
        OB_LOG(WARN, "failed to fetch file from file set", KR(ret), K(object_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(task->buf_len_ = get_random_content_length(op_type))) {
    } else if (FALSE_IT(task->buf_ = static_cast<char *>(malloc(task->buf_len_)))) {
    } else if (task->buf_len_ > 0) {
      if (OB_ISNULL(task->buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed allocate memory for task buf", KR(ret), K(task->buf_len_));
      } else if (OB_FAIL(generate_content_by_object_id(task->buf_, task->buf_len_, object_id))) {
        OB_LOG(WARN, "failed to generate content for task", KR(ret), K(task->buf_len_), K(object_id));
      }
    }

    if (OB_SUCC(ret)) {
      task->op_type_ = op_type;
      task->object_id_ = object_id;
      // random choosing whether to parallel or not
      rnd = ObRandom::rand(1, 100);
      if (rnd <= prob_of_parallel_) {
        task->parallel_ = true;
        static const OSDQOpType parallel_op_types[] = {
          WRITE_SINGLE_FILE,
          MULTIPART_WRITE,
          APPEND_WRITE
        };
        int parallel_op_type_nums = sizeof(parallel_op_types) / sizeof(OSDQOpType);
        // if buf_len_ is less than 4, the append task cannot be executed
        if (task->buf_len_ < 4) {
          parallel_op_type_nums--;
        }
        rnd = ObRandom::rand(0, parallel_op_type_nums - 1);
        task->parallel_op_type_ = static_cast<OSDQOpType>(rnd);
      }
    }
  }
  return ret;
}

int OSDQTaskHandler::gen_read_single_task(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(task));
  } else {
    task->op_type_ = READ_SINGLE_FILE;
    if (OB_FAIL(file_set_->fetch_and_delete_file(task->object_id_, task->uri_))) {
      OB_LOG(WARN, "failed fetch and delete file from file set", KR(ret));
    } else {
      int rnd = ObRandom::rand(1, 100);
      if (rnd <= prob_of_parallel_) {
        task->parallel_ = true;
        task->parallel_op_type_ = task->op_type_;
      }
    }
  }
  return ret;
}

int OSDQTaskHandler::gen_del_task(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task hanlder not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(task));
  } else {
    task->op_type_ = DEL_FILE;
    if (OB_FAIL(file_set_->fetch_and_delete_file(task->object_id_, task->uri_))) {
      OB_LOG(WARN, "failed fetch and delete file from file set", KR(ret));
    }
  }
  return ret;
}

std::packaged_task<int()> OSDQTaskHandler::get_packaged_task_(OSDQOpType op_type, const OSDQTask *task)
{
  if (op_type == WRITE_SINGLE_FILE) {
    return std::packaged_task<int()>(std::bind(&OSDQTaskHandler::handle_write_single_task_helper_, this, task));
  } else if (op_type == MULTIPART_WRITE) {
    return std::packaged_task<int()>(std::bind(&OSDQTaskHandler::handle_multipart_write_task_helper_, this, task));
  } else {
    // the previous parameter checks ensure that this must be append write operation
    return std::packaged_task<int()>(std::bind(&OSDQTaskHandler::handle_append_write_task_helper_, this, task));
  }
}

int OSDQTaskHandler::handle_write_task(const OSDQTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(task));
  } else {
    if (!task->parallel_) {
      // not parallel write
      const int64_t op_start_time_us = ObTimeUtility::current_time();
      if (task->op_type_ == WRITE_SINGLE_FILE
          && OB_FAIL(handle_write_single_task_helper_(task))) {
        OB_LOG(WARN, "failed handle write single task", KR(ret), KPC(task));
      } else if (task->op_type_ == MULTIPART_WRITE
          && OB_FAIL(handle_multipart_write_task_helper_(task))) {
        OB_LOG(WARN, "failed handle multipart write task", KR(ret), KPC(task));
      } else if (task->op_type_ == APPEND_WRITE
          && OB_FAIL(handle_append_write_task_helper_(task))) {
        OB_LOG(WARN, "failed handle append write task", KR(ret), KPC(task));
      }

      if (FAILEDx(metric_->add_latency_metric(op_start_time_us,
              task->op_type_, task->buf_len_))) {
        OB_LOG(WARN, "failed add latency metric", KR(ret), K(op_start_time_us), KPC(task));
      } else if (OB_FAIL(file_set_->add_file(task->object_id_, task->uri_))) {
        OB_LOG(WARN, "failed add object id and file path in file set", KR(ret), KPC(task));
      }
    } else {
      //parallel write
      std::packaged_task<int()> task1 = get_packaged_task_(task->op_type_, task);
      std::packaged_task<int()> task2 = get_packaged_task_(task->parallel_op_type_, task);

      std::future<int> future1 = task1.get_future();
      std::future<int> future2 = task2.get_future();
      std::thread th1(std::move(task1));
      std::thread th2(std::move(task2));

      th1.join();
      th2.join();
      int ret1 = future1.get();
      int ret2 = future2.get();

      if (OB_UNLIKELY(!check_parallel_write_result_(task->op_type_, ret1, task->parallel_op_type_, ret2))) {
       OSDQLogEntry log;
       log.init("PARALLEL WRITE FAILED", RED_COLOR_PREFIX);
       log.log_entry(std::string(OSDQ_OP_TYPE_NAMES[task->op_type_])
           + ": " + std::to_string(ret1) + " " + std::string(ob_error_name(ret1)));
       log.log_entry(std::string(OSDQ_OP_TYPE_NAMES[task->parallel_op_type_])
           + ": " + std::to_string(ret2) + " " + std::string(ob_error_name(ret2)));
       log.print();
      } else {
        OB_LOG(INFO, "parallel write succees info", KPC(task), K(ret1), K(ret2));
      }
    }
  }
  return ret;
}

int OSDQTaskHandler::handle_read_single_task(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t op_start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(task));
  } else {
    if (!task->parallel_) {
      if (OB_FAIL(handle_read_single_task_helper_(task))) {
        OB_LOG(WARN, "failed handle read single task", KR(ret));
      }  else if (OB_FAIL(metric_->add_latency_metric(op_start_time_us,
              task->op_type_, task->buf_len_))) {
        OB_LOG(WARN, "failed add latency metric", KR(ret), K(op_start_time_us), KPC(task));
      } else if (OB_FAIL(file_set_->add_file(task->object_id_, task->uri_))) {
        OB_LOG(WARN, "failed add object id and file path in file set", KR(ret), KPC(task));
      }
    } else {
      std::packaged_task<int(OSDQTask *task)> helper1(std::bind(&OSDQTaskHandler::handle_read_single_task_helper_, this, std::placeholders::_1));
      std::packaged_task<int(OSDQTask *task)> helper2(std::bind(&OSDQTaskHandler::handle_read_single_task_helper_, this, std::placeholders::_1));
      std::future<int> future_obj1 = helper1.get_future();
      std::future<int> future_obj2 = helper2.get_future();

      OSDQTask *task2 = nullptr;
      if (OB_ISNULL(task2 = static_cast<OSDQTask *>(malloc(sizeof(OSDQTask))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed allocate memory for OSDQTask", KR(ret));
      } else if (FALSE_IT(task2 = new(task2)OSDQTask())) {
      } else if (OB_ISNULL(task2->uri_ = static_cast<char *>(malloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed allocate memory for OSDQTask", KR(ret));
      } else {
        memcpy(task2->uri_, task->uri_, OB_MAX_URI_LENGTH);
        task2->op_type_ = READ_SINGLE_FILE;
        task2->object_id_ = task->object_id_;
      }

      if (OB_FAIL(ret)) {
      } else {
        std::thread th1(std::move(helper1), task);
        std::thread th2(std::move(helper2), task2);

        th1.join();
        th2.join();
        int ret1 = future_obj1.get();
        int ret2 = future_obj2.get();
        if (OB_UNLIKELY(ret1 != OB_SUCCESS || ret2 != OB_SUCCESS)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "failed parallel read single task", KR(ret), KR(ret1), KR(ret2));
        }
      }

      if (OB_NOT_NULL(task2)) {
        free_task(task2);
      }
    }
  }
  return ret;
}

int OSDQTaskHandler::handle_read_single_task_helper_(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (OB_FAIL(adapter_.adaptively_get_file_length(task->uri_, storage_info_, task->buf_len_))) {
    OB_LOG(WARN, "failed get file length", KR(ret), KPC(task), KPC(storage_info_));
  } else if (OB_ISNULL(task->buf_ = static_cast<char *>(malloc(task->buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed allocate memory for task buf", KR(ret), KPC(task));
  } else if (OB_FAIL(adapter_.adaptively_read_single_file(task->uri_, storage_info_, task->buf_,
          task->buf_len_, read_size, ObStorageIdMod::get_default_id_mod()))) {
    OB_LOG(WARN, "failed read single file", KR(ret), KPC(task), KPC(storage_info_));
  }
  return ret;
}

int OSDQTaskHandler::handle_del_task(OSDQTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t op_start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "task handler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(task));
  } else if (OB_FAIL(adapter_.adaptively_del_file(task->uri_, storage_info_))) {
    OB_LOG(WARN, "failed del file", KR(ret), KPC(task));
  } else if (OB_FAIL(metric_->add_latency_metric(op_start_time_us, task->op_type_, 0))) {
    OB_LOG(WARN, "failed add latency metric", KR(ret), K(op_start_time_us), KPC(task));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(file_set_->add_file(task->object_id_, task->uri_))) {
      OB_LOG(WARN, "failed add object id and file path in file set", KR(ret), KPC(task));
    }
  }
  return ret;
}

} // namespace tools
} // namespace oceanbase
