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

#include "lib/restore/ob_storage.h"
#include "ob_storage_s3_base.h"

namespace oceanbase
{
namespace common
{
using namespace Aws::S3;
using namespace Aws::Client;
using namespace Aws::Utils;

/*--------------------------------ObS3Logger--------------------------------*/
Logging::LogLevel ObS3Logger::GetLogLevel(void) const
{
  Logging::LogLevel log_level = Logging::LogLevel::Info;
  int32_t ob_log_level = OB_LOGGER.get_log_level();
  switch (ob_log_level) {
    case OB_LOG_LEVEL_INFO:
      break;
    case OB_LOG_LEVEL_ERROR:
      log_level = Logging::LogLevel::Error;
      break;
    case OB_LOG_LEVEL_WARN:
      log_level = Logging::LogLevel::Warn;
      break;
    case OB_LOG_LEVEL_TRACE:
      log_level = Logging::LogLevel::Debug;
      break;
    case OB_LOG_LEVEL_DEBUG:
      log_level = Logging::LogLevel::Trace;
      break;
    default:
      break;
  }
  return log_level;
}

void ObS3Logger::Log(Logging::LogLevel logLevel, const char* tag, const char* formatStr, ...)
{
  int ret = OB_SUCCESS;

  const int64_t buf_len = 4096;
  char arg_buf[buf_len] = {0};
  va_list args;
  va_start(args, formatStr);
  int psize = vsnprintf(arg_buf, buf_len - 1, formatStr, args);
  va_end(args);

  if (psize > 0) {
    const char *new_format = "[S3] module=%s, %s";
    switch (logLevel) {
      case Logging::LogLevel::Fatal:
      case Logging::LogLevel::Error:
      case Logging::LogLevel::Warn:
        ret = OB_S3_ERROR;
        _OB_LOG(WARN, new_format, tag, arg_buf);
        break;
      case Logging::LogLevel::Info:
        _OB_LOG(INFO, new_format, tag, arg_buf);
        break;
      // NOTICE: the s3 Debug and Trace level keeps the reverse order with them in ob.
      case Logging::LogLevel::Trace:
        _OB_LOG(DEBUG, new_format, tag, arg_buf);
        break;
      case Logging::LogLevel::Debug:
        _OB_LOG(TRACE, new_format, tag, arg_buf);
        break;
      default:
        _OB_LOG(WARN, new_format, tag, arg_buf);
        break;
    }
  }
}

void ObS3Logger::LogStream(Logging::LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream)
{
  Log(logLevel, tag, "msg=%s", messageStream.str().c_str());
}

/*--------------------------------ObS3Client--------------------------------*/
ObS3Client::ObS3Client()
    : lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      is_inited_(false),
      stopped_(false),
      ref_cnt_(0),
      last_modified_ts_(0),
      client_(nullptr)
{
}

ObS3Client::~ObS3Client()
{
  destroy();
}

int ObS3Client::init_s3_client_configuration_(const ObS3Account &account,
                                              S3ClientConfiguration &config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "S3 account not valid", K(ret));
  } else {
    if (OB_NOT_NULL(account.region_) && account.region_[0] != '\0') {
      config.region = account.region_;
    }
    config.scheme = Aws::Http::Scheme::HTTP; // if change to HTTPS, be careful about checksum logic.
    config.verifySSL = true;
    config.connectTimeoutMs = S3_CONNECT_TIMEOUT_MS;
    config.requestTimeoutMs = S3_REQUEST_TIMEOUT_MS;
    config.maxConnections = MAX_S3_CONNECTIONS_PER_CLIENT;
    config.payloadSigningPolicy = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
    config.endpointOverride = account.endpoint_;
    config.executor = nullptr;

    // Default maxRetries is 10
    std::shared_ptr<Aws::Client::DefaultRetryStrategy> retryStrategy =
        Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(S3_SDK, 1/*maxRetries*/);
    config.retryStrategy = retryStrategy;
  }
  return ret;
}

int ObS3Client::init(const ObS3Account &account)
{
  int ret = OB_SUCCESS;
  void *client_buf = nullptr;
  // Disables IMDS to prevent auto-region detection during construction.
  ClientConfigurationInitValues init_values;
  init_values.shouldDisableIMDS = true;
  S3ClientConfiguration config(init_values);
  // Re-enables IMDS access for subsequent operations if needed
  config.disableIMDS = false;
  Aws::Auth::AWSCredentials credentials(account.access_id_, account.secret_key_);
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObS3Client init twice", K(ret));
  } else if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "S3 account not valid", K(ret));
  } else if (OB_FAIL(init_s3_client_configuration_(account, config))) {
    OB_LOG(WARN, "failed to init s3 client config", K(ret), K(account));
  } else if (OB_ISNULL(client_buf = ob_malloc(sizeof(Aws::S3::S3Client), OB_STORAGE_S3_ALLOCATOR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to alloc buf for aws s3 client", K(ret));
  } else {
    try {
      client_ = new(client_buf) Aws::S3::S3Client(credentials,
          Aws::MakeShared<Endpoint::S3EndpointProvider>(S3_SDK), config);
    } catch (const std::exception &e) {
      ret = OB_S3_ERROR;
      OB_LOG(WARN, "caught exception when initing ObS3Client",
          K(ret), K(e.what()), KP(this), K(*this));
    } catch (...) {
      ret = OB_S3_ERROR;
      OB_LOG(WARN, "caught unknown exception when initing ObS3Client", K(ret), KP(this), K(*this));
    }

    if (OB_SUCC(ret)) {
      last_modified_ts_ = ObTimeUtility::current_time();
      is_inited_ = true;
    } else {
      ob_free(client_buf);
    }
  }
  return ret;
}

void ObS3Client::destroy()
{
  SpinWLockGuard guard(lock_);
  is_inited_ = false;
  stopped_ = false;
  ref_cnt_ = 0;
  last_modified_ts_ = 0;
  if (OB_NOT_NULL(client_)) {
    client_->~S3Client();
    ob_free(client_);
    client_ = NULL;
  }
}

bool ObS3Client::is_stopped() const
{
  SpinRLockGuard guard(lock_);
  return stopped_;
}

bool ObS3Client::try_stop(const int64_t timeout)
{
  bool is_stopped = true;
  if (OB_SUCCESS == lock_.wrlock(timeout)) {
    if (is_inited_) {
      const int64_t cur_time = ObTimeUtility::current_time();
      if (ref_cnt_ <= 0 && cur_time - last_modified_ts_ >= MAX_S3_CLIENT_IDLE_DURATION) {
        stopped_ = true;
      } else {
        is_stopped = false;
      }
    }
    lock_.unlock();
  } else {
    is_stopped = false;
  }
  return is_stopped;
}

void ObS3Client::stop()
{
  SpinWLockGuard guard(lock_);
  stopped_ = true;
}

void ObS3Client::increase()
{
  SpinWLockGuard guard(lock_);
  ref_cnt_++;
  last_modified_ts_ = ObTimeUtility::current_time();
}

void ObS3Client::release()
{
  SpinWLockGuard guard(lock_);
  ref_cnt_--;
  last_modified_ts_ = ObTimeUtility::current_time();
}

template<typename RequestType, typename OutcomeType>
int ObS3Client::do_s3_operation_(S3OperationFunc<RequestType, OutcomeType> s3_op_func,
                                 const RequestType &request, OutcomeType &outcome)
{
  int ret = OB_SUCCESS;
  {
    SpinRLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "ObS3Client not init", K(ret));
    } else if (stopped_) {
      ret = OB_IN_STOP_STATE;
      OB_LOG(WARN, "ObS3Client has been stopped", K(ret));
    } else if (OB_ISNULL(client_)) {
      ret = OB_S3_ERROR;
      OB_LOG(WARN, "client is NULL in ObS3Client", K(ret), KP(client_));
    }
  }

  if (OB_SUCC(ret)) {
    outcome = (client_->*s3_op_func)(request);
    last_modified_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObS3Client::head_object(const Aws::S3::Model::HeadObjectRequest &request,
                            Aws::S3::Model::HeadObjectOutcome &outcome)
{
  S3OperationFunc<Model::HeadObjectRequest, Model::HeadObjectOutcome>
      s3_op_func = &S3Client::HeadObject;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::put_object(const Model::PutObjectRequest &request,
                           Model::PutObjectOutcome &outcome)
{
  S3OperationFunc<Model::PutObjectRequest, Model::PutObjectOutcome>
      s3_op_func = &S3Client::PutObject;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::get_object(const Model::GetObjectRequest &request,
                           Model::GetObjectOutcome &outcome)
{
  S3OperationFunc<Model::GetObjectRequest, Model::GetObjectOutcome>
      s3_op_func = &S3Client::GetObject;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::delete_object(const Model::DeleteObjectRequest &request,
                              Model::DeleteObjectOutcome &outcome)
{
  S3OperationFunc<Model::DeleteObjectRequest, Model::DeleteObjectOutcome>
      s3_op_func = &S3Client::DeleteObject;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::put_object_tagging(const Model::PutObjectTaggingRequest &request,
                                   Model::PutObjectTaggingOutcome &outcome)
{
  S3OperationFunc<Model::PutObjectTaggingRequest, Model::PutObjectTaggingOutcome>
      s3_op_func = &S3Client::PutObjectTagging;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::list_objects_v2(const Model::ListObjectsV2Request &request,
                                Model::ListObjectsV2Outcome &outcome)
{
  S3OperationFunc<Model::ListObjectsV2Request, Model::ListObjectsV2Outcome>
      s3_op_func = &S3Client::ListObjectsV2;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::list_objects(const Aws::S3::Model::ListObjectsRequest &request,
                             Model::ListObjectsOutcome &outcome)
{
  S3OperationFunc<Model::ListObjectsRequest, Model::ListObjectsOutcome>
      s3_op_func = &S3Client::ListObjects;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::get_object_tagging(const Model::GetObjectTaggingRequest &request,
                                   Model::GetObjectTaggingOutcome &outcome)
{
  S3OperationFunc<Model::GetObjectTaggingRequest, Model::GetObjectTaggingOutcome>
      s3_op_func = &S3Client::GetObjectTagging;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::create_multipart_upload(const Model::CreateMultipartUploadRequest &request,
                                        Model::CreateMultipartUploadOutcome &outcome)
{
  S3OperationFunc<Model::CreateMultipartUploadRequest, Model::CreateMultipartUploadOutcome>
      s3_op_func = &S3Client::CreateMultipartUpload;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::list_parts(const Model::ListPartsRequest &request,
                           Model::ListPartsOutcome &outcome)
{
  S3OperationFunc<Model::ListPartsRequest, Model::ListPartsOutcome>
      s3_op_func = &S3Client::ListParts;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::complete_multipart_upload(const Model::CompleteMultipartUploadRequest &request,
                                          Model::CompleteMultipartUploadOutcome &outcome)
{
  S3OperationFunc<Model::CompleteMultipartUploadRequest, Model::CompleteMultipartUploadOutcome>
      s3_op_func = &S3Client::CompleteMultipartUpload;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::abort_multipart_upload(const Model::AbortMultipartUploadRequest &request,
                                       Model::AbortMultipartUploadOutcome &outcome)
{
  S3OperationFunc<Model::AbortMultipartUploadRequest, Model::AbortMultipartUploadOutcome>
      s3_op_func = &S3Client::AbortMultipartUpload;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::upload_part(const Model::UploadPartRequest &request,
                            Model::UploadPartOutcome &outcome)
{
  S3OperationFunc<Model::UploadPartRequest, Model::UploadPartOutcome>
      s3_op_func = &S3Client::UploadPart;
  return do_s3_operation_(s3_op_func, request, outcome);
}

int ObS3Client::list_multipart_uploads(const Model::ListMultipartUploadsRequest &request,
                                       Model::ListMultipartUploadsOutcome &outcome)
{
  S3OperationFunc<Model::ListMultipartUploadsRequest, Model::ListMultipartUploadsOutcome>
      s3_op_func = &S3Client::ListMultipartUploads;
  return do_s3_operation_(s3_op_func, request, outcome);
}

/*--------------------------------GLOBAL--------------------------------*/
int init_s3_env()
{
  return ObS3Env::get_instance().init();
}

void fin_s3_env()
{
  ObS3Env::get_instance().stop();

  // wait doing io finish before destroy s3 env.
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t timeout = ObExternalIOCounter::FLYING_IO_WAIT_TIMEOUT;
  int64_t flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  while(0 < flying_io_cnt) {
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > timeout) {
      int ret = OB_TIMEOUT;
      OB_LOG(WARN, "force fin_s3_env", K(ret), K(flying_io_cnt));
      break;
    }
    usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }

  ObS3Env::get_instance().destroy();
}

std::shared_ptr<Aws::Crt::Io::ClientBootstrap> s3_clientBootstrap_create_fn()
{
  return nullptr;
};

std::shared_ptr<Aws::Utils::Logging::LogSystemInterface> s3_logger_create_fn()
{
  return std::make_shared<ObS3Logger>();
}

ObS3Env::ObS3Env()
    : lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      is_inited_(false), s3_mem_manger_(),
      aws_options_(), s3_client_map_()
{
  aws_options_.memoryManagementOptions.memoryManager = &s3_mem_manger_;
  aws_options_.ioOptions.clientBootstrap_create_fn = s3_clientBootstrap_create_fn;
  ObS3Logger s3_logger;
  aws_options_.loggingOptions.logLevel = s3_logger.GetLogLevel();
  aws_options_.loggingOptions.logger_create_fn = s3_logger_create_fn;
}

ObS3Env &ObS3Env::get_instance()
{
  static ObS3Env s3_env_instance;
  return s3_env_instance;
}

int ObS3Env::init()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "S3 env init twice", K(ret));
  } else if (OB_FAIL(s3_client_map_.create(MAX_S3_CLIENT_NUM, OB_STORAGE_S3_ALLOCATOR))) {
    OB_LOG(WARN, "failed to create s3 client map", K(ret));
  } else {
    Aws::InitAPI(aws_options_);
    is_inited_ = true;
    // TO make sure Aws::ShutdownAPI is called before OPENSSL_cleanup
    // fin_s3_env is called when the singleton ObDeviceManager is destructed.
    // Its destructor is called after the cleanup of the openssl environment.
    // So, when executing fin_s3_env, openssl has already been cleaned up.
    // However, fin_s3_env invokes Aws::ShutDownAPI, which requires calling functions from openssl.
    // This can lead to deadlocks or segmentation faults.
    // To avoid this, we register fin_s3_env in advance,
    // ensuring that its invocation occurs before the cleanup of openssl
    atexit(fin_s3_env);
  }
  return ret;
}

void ObS3Env::destroy()
{
  SpinWLockGuard guard(lock_);
  if (OB_LIKELY(is_inited_)) {
    hash::ObHashMap<int64_t, ObS3Client *>::iterator iter = s3_client_map_.begin();
    while (iter != s3_client_map_.end()) {
      if (OB_NOT_NULL(iter->second)) {
        // force destroy s3 client
        ObS3Client *s3_client = iter->second;
        s3_client->~ObS3Client();
        ob_free(iter->second);
      }
      iter++;
    }
    s3_client_map_.destroy();
    Aws::ShutdownAPI(aws_options_);
    is_inited_ = false;
  }
}

int ObS3Env::get_or_create_s3_client(const ObS3Account &account, ObS3Client *&client)
{
  int ret = OB_SUCCESS;
  const int64_t key = account.hash();
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ob s3 env not init", K(ret));
  } else if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "S3 account not valid", K(ret));
  } else if (OB_FAIL(s3_client_map_.get_refactored(key, client))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      void *client_buf = NULL;
      if (s3_client_map_.size() > MAX_S3_CLIENT_MAP_THRESHOLD && OB_FAIL(clean_s3_client_map_())) {
        OB_LOG(WARN, "failed to clean s3 client map", K(ret), K(s3_client_map_.size()));
      } else if (OB_ISNULL(client_buf = ob_malloc(sizeof(ObS3Client), OB_STORAGE_S3_ALLOCATOR))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to alloc buf for ob s3 client", K(ret));
      } else {
        client = new(client_buf) ObS3Client();
        if (OB_FAIL(client->init(account))) {
          client->~ObS3Client();
          ob_free(client_buf);
          client = nullptr;
          OB_LOG(WARN, "failed to init ObS3Client", K(ret), K(account));
        } else if (OB_FAIL(s3_client_map_.set_refactored(key, client))) {
          client->~ObS3Client();
          ob_free(client_buf);
          client = nullptr;
          OB_LOG(WARN, "failed to insert into s3 client map", K(ret), K(account));
        } else {
          OB_LOG(DEBUG, "succeed create new s3 client", K(account), K(s3_client_map_.size()));
        }
      }
    } else {
      OB_LOG(WARN, "failed to get s3 client from map", K(ret), K(account));
    }
  } else if (OB_UNLIKELY(client->is_stopped())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "an stopped client remained in s3 client map", K(ret), KP(client));
  }

  if (OB_SUCC(ret)) {
    client->increase();
  }
  return ret;
}

int ObS3Env::clean_s3_client_map_()
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, ObS3Client *>::iterator iter = s3_client_map_.begin();
  ObArray<int64_t> s3_clients_to_clean;
  while (OB_SUCC(ret) && iter != s3_client_map_.end()) {
    if (OB_NOT_NULL(iter->second)) {
      ObS3Client *s3_client = iter->second;
      if (s3_client->try_stop()) {
        if (OB_FAIL(s3_clients_to_clean.push_back(iter->first))) {
          OB_LOG(WARN, "failed to push back into s3_clients_to_clean",
              K(ret), K(iter->first), KP(s3_client));
        } else {
          s3_client->~ObS3Client();
          ob_free(iter->second);
          iter->second = NULL;
        }
      }
    } else if (OB_FAIL(s3_clients_to_clean.push_back(iter->first))) {
      OB_LOG(WARN, "failed to push back into s3_clients_to_clean",
              K(ret), K(iter->first), KP(iter->second));
    }
    iter++;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < s3_clients_to_clean.count(); i++) {
    if (OB_FAIL(s3_client_map_.erase_refactored(s3_clients_to_clean[i]))) {
      OB_LOG(WARN, "failed to clean s3 client map", K(ret));
    }
  }

  return ret;
}

void ObS3Env::stop()
{
  SpinRLockGuard guard(lock_);
  hash::ObHashMap<int64_t, ObS3Client *>::iterator iter = s3_client_map_.begin();
  while (iter != s3_client_map_.end()) {
    if (OB_NOT_NULL(iter->second)) {
      ObS3Client *s3_client = iter->second;
      s3_client->stop();
    }
    iter++;
  }
}

const int S3_BAD_REQUEST = 400;
const int S3_ITEM_NOT_EXIST = 404;

static void convert_http_error(const Aws::S3::S3Error &s3_err, int &ob_errcode)
{
  const int http_code = static_cast<int>(s3_err.GetResponseCode());
  const Aws::String &exception = s3_err.GetExceptionName();
  const Aws::String &err_msg = s3_err.GetMessage();

  switch (http_code) {
    case S3_BAD_REQUEST: {
      if (exception == "InvalidRequest" && err_msg.find("x-amz-checksum") != std::string::npos) {
        ob_errcode = OB_CHECKSUM_ERROR;
      } else if (err_msg.find("region") != std::string::npos
                 && err_msg.find("is wrong; expecting") != std::string::npos) {
        ob_errcode = OB_S3_REGION_MISMATCH;
      } else {
        ob_errcode = OB_S3_ERROR;
      }
      break;
    }
    case S3_ITEM_NOT_EXIST: {
      if (exception == "NoSuchTagSet") {
        // When using the getObjectTagging function to access an OBS object that does not have tags,
        // a NoSuchTagSet error will be returned.
        ob_errcode = OB_ITEM_NOT_SETTED;
      } else {
        ob_errcode = OB_BACKUP_FILE_NOT_EXIST;
      }
      break;
    }
    default: {
      if (err_msg.find("curlCode: 28") != std::string::npos) {
        ob_errcode = OB_TIMEOUT;
      } else {
        ob_errcode = OB_S3_ERROR;
      }
      break;
    }
  }
}

static void convert_io_error(const Aws::S3::S3Error &s3_err, int &ob_errcode)
{
  switch (s3_err.GetErrorType()) {
    case Aws::S3::S3Errors::NO_SUCH_KEY: {
      ob_errcode = OB_BACKUP_FILE_NOT_EXIST;
      break;
    }
    case Aws::S3::S3Errors::RESOURCE_NOT_FOUND: {
      ob_errcode = OB_BACKUP_FILE_NOT_EXIST;
      break;
    }
    case Aws::S3::S3Errors::SLOW_DOWN: {
      ob_errcode = OB_IO_LIMIT;
      break;
    }
    case Aws::S3::S3Errors::ACCESS_DENIED: {
      ob_errcode = OB_BACKUP_PERMISSION_DENIED;
      break;
    }
    case Aws::S3::S3Errors::NO_SUCH_BUCKET: {
      ob_errcode = OB_INVALID_OBJECT_STORAGE_ENDPOINT;
      break;
    }
    default: {
      convert_http_error(s3_err, ob_errcode);
      break;
    }
  }
}

template<typename OutcomeType>
static void log_s3_status(OutcomeType &outcome, const int ob_errcode)
{
  const char *request_id = outcome.GetResult().GetRequestId().c_str();
  const int code = static_cast<int>(outcome.GetError().GetResponseCode());
  const char *exception = outcome.GetError().GetExceptionName().c_str();
  const char *err_msg = outcome.GetError().GetMessage().c_str();
  if (OB_CHECKSUM_ERROR == ob_errcode) {
    OB_LOG_RET(ERROR, ob_errcode, "S3 info", K(request_id), K(code), K(exception), K(err_msg));
  } else {
    OB_LOG_RET(WARN, ob_errcode, "S3 info", K(request_id), K(code), K(exception), K(err_msg));
  }
}

template<typename OutcomeType>
static void handle_s3_outcome(OutcomeType &outcome, int &ob_errcode)
{
  const Aws::S3::S3Error &s3_err = outcome.GetError();
  convert_io_error(s3_err, ob_errcode);
  log_s3_status(outcome, ob_errcode);
}

/*--------------------------------Checksum Util--------------------------------*/
template<typename RequestType>
static int set_request_checkusum_algorithm(RequestType &request,
                                           const ObStorageChecksumType checksum_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_s3_supported_checksum(checksum_type))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "that checksum algorithm is not supported for s3", K(ret), K(checksum_type));
  } else {
    if (checksum_type == ObStorageChecksumType::OB_CRC32_ALGO) {
      request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32);
    } else {
      // defaut md5
    }
  }
  return ret;
}

static int set_completed_part_checksum(Aws::S3::Model::CompletedPart &completed_part,
                                       const Aws::S3::Model::Part &part,
                                       const ObStorageChecksumType checksum_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_s3_supported_checksum(checksum_type))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "that checksum algorithm is not supported for s3", K(ret), K(checksum_type));
  } else {
    if (checksum_type == ObStorageChecksumType::OB_CRC32_ALGO) {
      completed_part.SetChecksumCRC32(part.GetChecksumCRC32());
    } else {
      // default md5
    }
  }
  return ret;
}

// The check_xxx functions are used to verify that the checksum of the returned data
// when reading a complete object matches the checksum value carried in the response header.
// As it's unclear what checksum algorithm was specifically used during data upload,
// the check_xxx functions should not be used in isolation. Instead, use validate_response_checksum.
// Therefore, the check_xxx functions do not validate the input arguments for their effectiveness;
// validate_response_checksum is responsible for performing a unified validation.
static int check_crc32(const char *buf, const int64_t size, Aws::S3::Model::GetObjectResult &result)
{
  int ret = OB_SUCCESS;
  const Aws::String &response_checksum = result.GetChecksumCRC32();
  if (!response_checksum.empty()) {
    Aws::String buf_str(buf, size);
    Aws::Utils::ByteBuffer checksum_buf = Aws::Utils::HashingUtils::CalculateCRC32(buf_str);
    Aws::String returned_data_checksum = Aws::Utils::HashingUtils::Base64Encode(checksum_buf);

    if (OB_UNLIKELY(returned_data_checksum != response_checksum)) {
      ret = OB_CHECKSUM_ERROR;
      OB_LOG(ERROR, "crc32 mismatch",
          K(ret), K(size), K(returned_data_checksum.c_str()), K(response_checksum.c_str()));
    }
  }
  return ret;
}

static int check_crc32c(const char *buf, const int64_t size, Aws::S3::Model::GetObjectResult &result)
{
  int ret = OB_SUCCESS;
  const Aws::String &response_checksum = result.GetChecksumCRC32C();
  if (!response_checksum.empty()) {
    Aws::String buf_str(buf, size);
    Aws::Utils::ByteBuffer checksum_buf = Aws::Utils::HashingUtils::CalculateCRC32C(buf_str);
    Aws::String returned_data_checksum = Aws::Utils::HashingUtils::Base64Encode(checksum_buf);

    if (OB_UNLIKELY(returned_data_checksum != response_checksum)) {
      ret = OB_CHECKSUM_ERROR;
      OB_LOG(ERROR, "crc32c mismatch",
          K(ret), K(size), K(returned_data_checksum.c_str()), K(response_checksum.c_str()));
    }
  }
  return ret;
}

static int check_sha1(const char *buf, const int64_t size, Aws::S3::Model::GetObjectResult &result)
{
  int ret = OB_SUCCESS;
  const Aws::String &response_checksum = result.GetChecksumSHA1();
  if (!response_checksum.empty()) {
    Aws::String buf_str(buf, size);
    Aws::Utils::ByteBuffer checksum_buf = Aws::Utils::HashingUtils::CalculateSHA1(buf_str);
    Aws::String returned_data_checksum = Aws::Utils::HashingUtils::Base64Encode(checksum_buf);

    if (OB_UNLIKELY(returned_data_checksum != response_checksum)) {
      ret = OB_CHECKSUM_ERROR;
      OB_LOG(ERROR, "sha1 mismatch",
          K(ret), K(size), K(returned_data_checksum.c_str()), K(response_checksum.c_str()));
    }
  }
  return ret;
}

static int check_sha256(const char *buf, const int64_t size, Aws::S3::Model::GetObjectResult &result)
{
  int ret = OB_SUCCESS;
  const Aws::String &response_checksum = result.GetChecksumSHA256();
  if (!response_checksum.empty()) {
    Aws::String buf_str(buf, size);
    Aws::Utils::ByteBuffer checksum_buf = Aws::Utils::HashingUtils::CalculateSHA256(buf_str);
    Aws::String returned_data_checksum = Aws::Utils::HashingUtils::Base64Encode(checksum_buf);

    if (OB_UNLIKELY(returned_data_checksum != response_checksum)) {
      ret = OB_CHECKSUM_ERROR;
      OB_LOG(ERROR, "sha256 mismatch",
          K(ret), K(size), K(returned_data_checksum.c_str()), K(response_checksum.c_str()));
    }
  }
  return ret;
}

static int validate_response_checksum(
    const char *buf, const int64_t size, Aws::S3::Model::GetObjectResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(check_crc32(buf, size, result))) {
    OB_LOG(WARN, "failed to check crc32", K(ret));
  } else if (OB_FAIL(check_crc32c(buf, size, result))) {
    OB_LOG(WARN, "failed to check crc32c", K(ret));
  } else if (OB_FAIL(check_sha1(buf, size, result))) {
    OB_LOG(WARN, "failed to check sha1", K(ret));
  } else if (OB_FAIL(check_sha1(buf, size, result))) {
    OB_LOG(WARN, "failed to check sha256", K(ret));
  }
  return ret;
}

/*--------------------------------ObS3Account--------------------------------*/
ObS3Account::ObS3Account()
{
  reset();
}

ObS3Account::~ObS3Account()
{
  if (is_valid_) {
    reset();
  }
}

void ObS3Account::reset()
{
  is_valid_ = false;
  delete_mode_ = ObIStorageUtil::DELETE;
  MEMSET(region_, 0, sizeof(region_));
  MEMSET(endpoint_, 0, sizeof(endpoint_));
  MEMSET(access_id_, 0, sizeof(access_id_));
  MEMSET(secret_key_, 0, sizeof(secret_key_));
}

int64_t ObS3Account::hash() const
{
  int64_t hash_value = 0;
  hash_value = murmurhash(region_, static_cast<int32_t>(strlen(region_)), hash_value);
  hash_value = murmurhash(endpoint_, static_cast<int32_t>(strlen(endpoint_)), hash_value);
  hash_value = murmurhash(access_id_, static_cast<int32_t>(strlen(access_id_)), hash_value);
  hash_value = murmurhash(secret_key_, static_cast<int32_t>(strlen(secret_key_)), hash_value);
  return hash_value;
}

int ObS3Account::parse_from(const char *storage_info_str, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info_str) || size >= OB_MAX_BACKUP_STORAGE_INFO_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to init s3 account, invalid argument", K(ret), KP(storage_info_str), K(size));
  } else {
    char tmp[OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
    char *token = NULL;
    char *saved_ptr = NULL;

    uint8_t bitmap = 0;
    MEMCPY(tmp, storage_info_str, size);
    tmp[size] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (OB_ISNULL(token)) {
        break;
      } else if (0 == strncmp(REGION, token, strlen(REGION))) {
        if (OB_FAIL(set_field(token + strlen(REGION), region_, sizeof(region_)))) {
          OB_LOG(WARN, "failed to set s3 region", K(ret), KCSTRING(token));
        }
      } else if (0 == strncmp(HOST, token, strlen(HOST))) {
        if (OB_FAIL(set_field(token + strlen(HOST), endpoint_, sizeof(endpoint_)))) {
          OB_LOG(WARN, "failed to set s3 endpoint", K(ret), KCSTRING(token));
        } else {
          bitmap |= 1;
        }
      } else if (0 == strncmp(ACCESS_ID, token, strlen(ACCESS_ID))) {
        if (OB_FAIL(set_field(token + strlen(ACCESS_ID), access_id_, sizeof(access_id_)))) {
          OB_LOG(WARN, "failed to set s3 access id", K(ret), KCSTRING(token));
        } else {
          bitmap |= (1 << 1);
        }
      } else if (0 == strncmp(ACCESS_KEY, token, strlen(ACCESS_KEY))) {
        if (OB_FAIL(set_field(token + strlen(ACCESS_KEY), secret_key_, sizeof(secret_key_)))) {
          OB_LOG(WARN, "failed to set s3 secret key", K(ret), KP(token));
        } else {
          bitmap |= (1 << 2);
        }
      } else if (0 == strncmp(DELETE_MODE, token, strlen(DELETE_MODE))) {
        if (0 == strcmp(token + strlen(DELETE_MODE), "delete")) {
          delete_mode_ = ObIStorageUtil::DELETE;
        } else if (0 == strcmp(token + strlen(DELETE_MODE), "tagging")) {
          delete_mode_ = ObIStorageUtil::TAGGING;
        } else {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "delete mode is invalid", K(ret), KCSTRING(token));
        }
      } else {
        OB_LOG(DEBUG, "unknown s3 info", K(*token));
      }
    }

    if (OB_SUCC(ret) && bitmap != 0x07) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to init s3 account", K(ret), KCSTRING(region_),
          KCSTRING(endpoint_), KCSTRING(access_id_), K(bitmap));
    }
  }

  if (OB_SUCC(ret)) {
    is_valid_ = true;
    OB_LOG(DEBUG, "succeed to init s3 account",
        KCSTRING(region_), KCSTRING(endpoint_), KCSTRING(access_id_));
  } else {
    reset();
  }
  return ret;
}

int ObS3Account::set_field(const char *value, char *field, const uint32_t field_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value) || OB_ISNULL(field)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invliad arguments", K(ret), KP(value), KP(field));
  } else {
    const int64_t value_len = strlen(value);
    if (value_len >= field_length) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "value is too long", K(ret), KP(value), K(value_len), K(field_length));
    } else {
      MEMCPY(field, value, value_len);
      field[value_len] = '\0';
    }
  }
  return ret;
}

/*--------------------------------ObStorageS3Base--------------------------------*/
ObStorageS3Base::ObStorageS3Base()
    : allocator_(OB_STORAGE_S3_ALLOCATOR),
      s3_client_(NULL),
      bucket_(),
      object_(),
      checksum_type_(ObStorageChecksumType::OB_MD5_ALGO),
      is_inited_(false),
      s3_account_()
{
}

ObStorageS3Base::~ObStorageS3Base()
{
  reset();
}

void ObStorageS3Base::reset()
{
  is_inited_ = false;
  s3_account_.reset();
  allocator_.clear();
  checksum_type_ = ObStorageChecksumType::OB_MD5_ALGO;
  if (OB_NOT_NULL(s3_client_)) {
    s3_client_->release();
    s3_client_ = NULL;
  }
}

int ObStorageS3Base::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char info_str[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "s3 base alreagy inited", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to init s3 base, invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(build_bucket_and_object_name(allocator_, uri, bucket_, object_))) {
    OB_LOG(WARN, "failed to parse uri", K(ret), K(uri));
  } else if (OB_FAIL(storage_info->get_storage_info_str(info_str, sizeof(info_str)))) {
    OB_LOG(WARN, "failed to get storage info str", K(ret), KPC(storage_info));
  } else if (OB_FAIL(s3_account_.parse_from(info_str, strlen(info_str)))) {
    OB_LOG(WARN, "failed to build s3 account", K(ret));
  } else if (OB_FAIL(ObS3Env::get_instance().get_or_create_s3_client(s3_account_, s3_client_))) {
    OB_LOG(WARN, "faied to get s3 client", K(ret));
  } else {
    checksum_type_ = storage_info->get_checksum_type();
    if (OB_UNLIKELY(!is_s3_supported_checksum(checksum_type_))) {
      ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
      OB_LOG(WARN, "that checksum algorithm is not supported for s3", K(ret), K_(checksum_type));
    } else {
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}


// can only be used to get the metadata of normal objects
int ObStorageS3Base::get_s3_file_meta_(S3ObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  meta.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 base not inited", K(ret));
  } else {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());
    Aws::S3::Model::HeadObjectOutcome outcome;
    if (OB_FAIL(s3_client_->head_object(request, outcome))) {
      OB_LOG(WARN, "failed to head s3 object", K(ret));
    } else if (!outcome.IsSuccess()) {
      convert_io_error(outcome.GetError(), ret);
      if (OB_BACKUP_FILE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        log_s3_status(outcome, ret);
        OB_LOG(WARN, "failed to head s3 object", K(ret), K_(bucket), K_(object));
      }
    } else {
      meta.is_exist_ = true;
      meta.length_ = outcome.GetResult().GetContentLength();
    }
  }
  return ret;
}

int ObStorageS3Base::do_list_(const int64_t max_list_num, const char *delimiter,
    const Aws::String &next_marker, Aws::S3::Model::ListObjectsOutcome &outcome)
{
  int ret = OB_SUCCESS;
  char dir_path_buf[OB_MAX_URI_LENGTH] = {0};

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 base not inited", K(ret));
  } else if (OB_UNLIKELY(max_list_num <= 0 || max_list_num > OB_STORAGE_LIST_MAX_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K_(bucket), K_(object), K(max_list_num));
  } else if (OB_UNLIKELY(!is_end_with_slash(object_.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not a valid dir path", K(ret), K_(object), K_(bucket));
  } else {
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket_.ptr()).WithPrefix(object_.ptr()).WithMaxKeys(max_list_num);
    if (NULL != delimiter && strlen(delimiter) > 0) {
      request.SetDelimiter(delimiter);
    }
    if (!next_marker.empty()) {
      request.SetMarker(next_marker);
    }

    if (OB_FAIL(s3_client_->list_objects(request, outcome))) {
      OB_LOG(WARN, "failed to list s3 objects", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      OB_LOG(WARN, "failed to list s3 objects", K(ret),
          K_(bucket), K_(object), K(max_list_num), K(delimiter));
    }
  }
  return ret;
}

/*--------------------------------ObStorageS3Writer--------------------------------*/
ObStorageS3Writer::ObStorageS3Writer()
    : ObStorageS3Base(),
      is_opened_(false),
      file_length_(-1)
{
}

ObStorageS3Writer::~ObStorageS3Writer()
{
  if (is_opened_) {
    close();
  }
}

int ObStorageS3Writer::open_(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageS3Base::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in s3 base", K(ret), K(uri));
  } else {
    is_opened_ = true;
    file_length_ = 0;
  }
  return ret;
}

static int init_put_object_request(const char *bucket, const char *object,
    const char *buf, const int64_t size, const ObStorageChecksumType checksum_type,
    Aws::S3::Model::PutObjectRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bucket) || OB_ISNULL(object) || OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(bucket), KP(object), KP(buf), K(size));
  } else if (OB_FAIL(set_request_checkusum_algorithm(request, checksum_type))) {
    OB_LOG(WARN, "fail to set checksum algorithm", K(ret), K(checksum_type));
  } else {
    request.WithBucket(bucket).WithKey(object);
    std::shared_ptr<Aws::IOStream> data_stream =
        Aws::MakeShared<Aws::StringStream>(S3_SDK);
    data_stream->write(buf, size);
    data_stream->flush();
    request.SetBody(data_stream);
    request.SetContentLength(static_cast<long>(request.GetBody()->tellp()));
  }
  return ret;
}

int ObStorageS3Writer::write_(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  Aws::S3::Model::PutObjectRequest request;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 writer not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(write_obj_(object_.ptr(), buf, size))) {
    OB_LOG(WARN, "fail to put s3 object", K(ret), K_(bucket), K_(object));
  } else {
    file_length_ = size;
  }
  return ret;
}

int ObStorageS3Writer::write_obj_(const char *obj_name, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  Aws::S3::Model::PutObjectRequest request;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 writer not opened", K(ret));
  } else if (OB_ISNULL(obj_name) || OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(obj_name), KP(buf), K(size));
  } else if (OB_FAIL(init_put_object_request(bucket_.ptr(), obj_name, buf,
                                             size, checksum_type_, request))) {
    OB_LOG(WARN, "failed to init put obejct request", K(ret),
        K_(bucket), K(obj_name), KP(buf), K(size), K_(checksum_type));
  } else {
    Aws::S3::Model::PutObjectOutcome outcome;
    if (OB_FAIL(s3_client_->put_object(request, outcome))) {
      OB_LOG(WARN, "failed to put s3 object", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      OB_LOG(WARN, "failed to write object into s3",
          K(ret), K_(bucket), K(obj_name), KP(buf), K(size));
    }
  }
  return ret;
}

int ObStorageS3Writer::pwrite_(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  UNUSED(offset);
  return ret;
}

int ObStorageS3Writer::close_()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  file_length_ = -1;
  reset();
  return ret;
}

/*--------------------------------ObStorageS3Reader--------------------------------*/
ObStorageS3Reader::ObStorageS3Reader()
    : ObStorageS3Base(),
      is_opened_(false),
      has_meta_(false),
      file_length_(-1)
{
}

ObStorageS3Reader::~ObStorageS3Reader()
{
}

void ObStorageS3Reader::reset()
{
  ObStorageS3Base::reset();
  is_opened_ = false;
  has_meta_ = false;
  file_length_ = -1;
}

int ObStorageS3Reader::open_(const ObString &uri,
                             ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  S3ObjectMeta meta;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 reader already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageS3Base::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in s3 base", K(ret), K(uri));
  } else {
    if (head_meta) {
      if (OB_FAIL(get_s3_file_meta(meta))) {
        OB_LOG(WARN, "failed to get s3 object meta", K(ret), K(uri));
      } else if (!meta.is_exist_) {
        ret = OB_BACKUP_FILE_NOT_EXIST;
        OB_LOG(WARN, "backup file is not exist", K(ret), K(uri), K_(bucket), K_(object));
      } else {
        file_length_ = meta.length_;
        has_meta_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObStorageS3Reader::pread_(char *buf,
    const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 reader not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_size), K(offset));
  } else {
    int64_t get_data_size = buf_size;
    if (has_meta_) {
      if (file_length_ < offset) {
        ret = OB_FILE_LENGTH_INVALID;
        OB_LOG(WARN, "offset is larger than file length",
            K(ret), K(offset), K_(file_length), K_(bucket), K_(object));
      } else {
        get_data_size = MIN(buf_size, file_length_ - offset);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (get_data_size == 0) {
      read_size = 0;
    } else {
      Aws::S3::Model::GetObjectRequest request;
      Aws::S3::Model::GetObjectOutcome outcome;
      char range_read[64] = { 0 };
      request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());

      if (OB_FAIL(databuff_printf(range_read, sizeof(range_read),
                                  "bytes=%ld-%ld", offset, offset + get_data_size - 1))) {
        OB_LOG(WARN, "fail to set range to read", K(ret),
            K_(bucket), K_(object), K(offset), K(buf_size), K_(has_meta), K_(file_length));
      } else if (FALSE_IT(request.SetRange(range_read))) {
      } else if (OB_FAIL(s3_client_->get_object(request, outcome))) {
        OB_LOG(WARN, "failed to get s3 object", K(ret), K(range_read));
      } else if (!outcome.IsSuccess()) {
        handle_s3_outcome(outcome, ret);
        OB_LOG(WARN, "failed to read object from s3",
            K(ret), K_(bucket), K_(object), K(range_read));
      } else {
        read_size = outcome.GetResult().GetContentLength();
        outcome.GetResult().GetBody().read(buf, read_size);

        // read size <= get_data_size <= buf_size
        if (read_size == file_length_) {
          if (OB_FAIL(validate_response_checksum(buf, read_size, outcome.GetResult()))) {
            OB_LOG(WARN, "fail to validate_response_checksum",
                K(ret), K(read_size), K(get_data_size), K(buf_size));
          }
        }

      }
    }
  }
  return ret;
}

int ObStorageS3Reader::close_()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  reset();
  return ret;
}

/*--------------------------------ObStorageS3Util--------------------------------*/
ObStorageS3Util::ObStorageS3Util() : is_opened_(false), storage_info_(NULL)
{
}

ObStorageS3Util::~ObStorageS3Util()
{
}

int ObStorageS3Util::open(ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util already open, cannot open again", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is NULL", K(ret), KP(storage_info));
  } else {
    is_opened_ = true;
    storage_info_ = storage_info;
  }
  return ret;
}

void ObStorageS3Util::close()
{
  is_opened_ = false;
  storage_info_ = NULL;
}

int ObStorageS3Util::is_exist_(const ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else {
    exist = obj_meta.is_exist_;
  }
  return ret;
}

int ObStorageS3Util::get_file_length_(const ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else if (!obj_meta.is_exist_) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "backup file is not exist", K(ret), K(uri));
  } else {
    file_length = obj_meta.length_;
  }
  return ret;
}

int ObStorageS3Util::head_object_meta(const ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  S3ObjectMeta meta;
  ObStorageS3Base s3_base;
  obj_meta.reset();
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 base", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.get_s3_file_meta(meta))) {
    OB_LOG(WARN, "failed to get s3 file meta", K(ret), K(uri));
  } else {
    obj_meta.is_exist_ = meta.is_exist_;
    if (obj_meta.is_exist_) {
      obj_meta.length_ = meta.length_;
    }
  }
  return ret;
}

int ObStorageS3Util::delete_object_(ObStorageS3Base &s3_base)
{
  int ret = OB_SUCCESS;
  Aws::S3::Model::DeleteObjectRequest request;
  request.WithBucket(s3_base.bucket_.ptr()).WithKey(s3_base.object_.ptr());
  Aws::S3::Model::DeleteObjectOutcome outcome;
  if (OB_FAIL(s3_base.s3_client_->delete_object(request, outcome))) {
    OB_LOG(WARN, "failed to delete s3 object", K(ret));
  } else if (!outcome.IsSuccess()) {
    handle_s3_outcome(outcome, ret);
    OB_LOG(WARN, "failed to delete s3 object",
        K(ret), K(s3_base.bucket_), K(s3_base.object_));
  } else {
    OB_LOG(DEBUG, "delete s3 object succeed", K(s3_base.bucket_), K(s3_base.object_));
  }
  return ret;
}

int ObStorageS3Util::tagging_object_(ObStorageS3Base &s3_base)
{
  int ret = OB_SUCCESS;
  Aws::S3::Model::PutObjectTaggingRequest request;
  request.WithBucket(s3_base.bucket_.ptr()).WithKey(s3_base.object_.ptr());
  Aws::S3::Model::Tag tag;
  tag.WithKey("delete_mode").WithValue("tagging");
  Aws::S3::Model::Tagging tagging;
  tagging.AddTagSet(tag);
  request.SetTagging(tagging);
  Aws::S3::Model::PutObjectTaggingOutcome outcome;
  if (OB_FAIL(s3_base.s3_client_->put_object_tagging(request, outcome))) {
    OB_LOG(WARN, "failed to put s3 object tagging", K(ret));
  } else if (!outcome.IsSuccess()) {
    handle_s3_outcome(outcome, ret);
    OB_LOG(WARN, "failed to tagging s3 object",
        K(ret), K(s3_base.bucket_), K(s3_base.object_));
  } else {
    OB_LOG(DEBUG, "tagging s3 object succeed", K(s3_base.bucket_), K(s3_base.object_));
  }
  return ret;
}

int ObStorageS3Util::del_file_(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 base", K(ret), K(uri));
  } else {
    const int64_t delete_mode = s3_base.s3_account_.delete_mode_;
    if (ObIStorageUtil::DELETE == delete_mode) {
      if (OB_FAIL(delete_object_(s3_base))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          // Uniform handling of 'object not found' scenarios across different object storage services:
          // GCS returns the OB_BACKUP_FILE_NOT_EXIST error when an object does not exist,
          // whereas other object storage services may not report an error.
          // Therefore, to maintain consistency,
          // no error code is returned when attempting to delete a non-existent object
          ret = OB_SUCCESS;
        } else {
          OB_LOG(WARN, "failed to delete s3 object", K(ret), K(uri));
        }
      }
    } else if (ObIStorageUtil::TAGGING == delete_mode) {
      if (OB_FAIL(tagging_object_(s3_base))) {
        OB_LOG(WARN, "failed to tag s3 object", K(ret), K(uri));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "s3 delete mode invalid", K(ret), K(uri), K(delete_mode));
    }
  }
  return ret;
}

int ObStorageS3Util::write_single_file_(const ObString &uri, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObStorageS3Writer s3_writer;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty() || size < 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), KP(buf), K(size));
  } else if (OB_FAIL(s3_writer.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 writer", K(ret), K(uri));
  } else if (OB_FAIL(s3_writer.write(buf, size))) {
    OB_LOG(WARN, "failed to write into s3", K(ret), K(uri), KP(buf), K(size));
  } else if (OB_FAIL(s3_writer.close())) {
    OB_LOG(WARN, "failed to close s3 writer", K(ret), K(uri), KP(buf), K(size));
  }
  return ret;
}

int ObStorageS3Util::mkdir_(const ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to create dir in s3", K(uri));
  UNUSED(uri);
  return ret;
}


int ObStorageS3Util::list_files_(const ObString &uri, ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  ObExternalIOCounterGuard io_guard;
  const char *full_dir_path = NULL;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open s3 base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = s3_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    const int64_t full_dir_path_len = strlen(full_dir_path); // actual dir path len
    Aws::S3::Model::ListObjectsOutcome outcome;
    Aws::String next_marker;
    do {
      if (OB_FAIL(s3_base.do_list_(OB_STORAGE_LIST_MAX_NUM, NULL/*delimiter*/,
                                   next_marker, outcome))) {
        OB_LOG(WARN, "fail to list s3 objects", K(ret), K(uri));
      } else {
        const char *request_id = outcome.GetResult().GetRequestId().c_str();
        const Aws::Vector<Aws::S3::Model::Object> &contents = outcome.GetResult().GetContents();
        for (int64_t i = 0; OB_SUCC(ret) && i < contents.size(); i++) {
          const Aws::S3::Model::Object &obj = contents[i];
          const char *obj_path = obj.GetKey().c_str();
          const int64_t obj_path_len = obj.GetKey().size();

          // For example, we can use oss console to create a 'read dir', like aaa/bbb/ccc/.
          // When list 'aaa/bbb/ccc/' this dir, we will get it self, that means we will get
          // a object whose name length is same with its parent dir length.
          if (OB_UNLIKELY(false == ObString(obj_path).prefix_match(full_dir_path))) {
            ret = OB_S3_ERROR;
            OB_LOG(WARN, "returned object prefix not match",
                K(ret), K(request_id), K(obj_path), K(full_dir_path), K(uri));
          } else if (OB_UNLIKELY(obj_path_len == full_dir_path_len)) {
            // skip
            OB_LOG(INFO, "exist object path length is same with dir path length",
                 K(request_id), K(obj_path), K(full_dir_path), K(full_dir_path_len));
          } else if (OB_FAIL(handle_listed_object(op, obj_path + full_dir_path_len,
                                                  obj_path_len - full_dir_path_len,
                                                  obj.GetSize()))) {
            OB_LOG(WARN, "fail to handle listed s3 object", K(ret),  K(request_id),
                K(obj_path), K(obj_path_len), K(full_dir_path), K(full_dir_path_len), K(uri));
          }
        } // end for
        if (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated()) {
          // We cannot set next_marker to outcome.GetResult().GetNextMarker() directly
          // because GetNextMarker() might return empty data.
          // Below is the description of next marker from the S3 official documentation:
          // This element is returned only if you have the delimiter request parameter specified.
          // If the response does not include the NextMarker element and it is truncated,
          // you can use the value of the last Key element in the response
          // as the marker parameter in the subsequent request to get the next set of object keys.
          if (contents.size() > 0) {
            next_marker = contents[contents.size() - 1].GetKey();
          } else {
            // if result is truncated, contents should not be empty
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "listed s3 objects are empty", K(ret), K(request_id), K(contents.size()));
          }
        }
      }
    } while (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated());
  }
  return ret;
}

int ObStorageS3Util::list_files2_(
    const ObString &uri,
    ObStorageListCtxBase &ctx_base)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  ObExternalIOCounterGuard io_guard;
  const char *full_dir_path = NULL;
  ObStorageListObjectsCtx &list_ctx = static_cast<ObStorageListObjectsCtx &>(ctx_base);

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty() || !list_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), K(list_ctx));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open s3 base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = s3_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    const int64_t full_dir_path_len = strlen(full_dir_path); // actual dir path len
    Aws::S3::Model::ListObjectsOutcome outcome;
    Aws::String next_marker;
    if (list_ctx.next_token_ != NULL && list_ctx.next_token_[0] != '\0') {
      next_marker.assign(list_ctx.next_token_);
    }
    const int64_t max_list_num = MIN(OB_STORAGE_LIST_MAX_NUM, list_ctx.max_list_num_);

    if (OB_FAIL(s3_base.do_list_(max_list_num, NULL/*delimiter*/,
                                 next_marker, outcome))) {
      OB_LOG(WARN, "fail to list s3 objects", K(ret), K(uri), K(max_list_num));
    } else {
      const char *request_id = outcome.GetResult().GetRequestId().c_str();
      const Aws::Vector<Aws::S3::Model::Object> &contents = outcome.GetResult().GetContents();
      if (outcome.GetResult().GetIsTruncated()) {
        if (contents.size() > 0) {
          next_marker = contents[contents.size() - 1].GetKey();
        } else {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "listed s3 objects are empty", K(ret), K(request_id), K(contents.size()));
        }
      } else {
        next_marker = "";
      }

      if (OB_FAIL(ret)) {
      } else if (contents.size() > list_ctx.max_list_num_) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "can't hold all contents", K(ret), K(request_id), K(list_ctx), K(contents.size()));
      } else if (OB_FAIL(list_ctx.set_next_token(outcome.GetResult().GetIsTruncated(),
                                                 next_marker.c_str(),
                                                 next_marker.length()))) {
        OB_LOG(WARN, "fail to set next token when listing s3 objects", K(ret), K(request_id));
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < contents.size()); i++) {
        const Aws::S3::Model::Object &obj = contents[i];
        const int64_t obj_size = obj.GetSize();
        const char *cur_obj_path = obj.GetKey().c_str(); // object full path
        const int64_t cur_obj_path_len = obj.GetKey().size();
        OB_LOG(DEBUG, "s3 list files content", K(cur_obj_path), K(cur_obj_path_len));

        if (OB_UNLIKELY(false == ObString(cur_obj_path).prefix_match(full_dir_path))) {
          ret = OB_S3_ERROR;
          OB_LOG(WARN, "returned object prefix not match",
              K(ret), K(request_id), K(cur_obj_path), K(full_dir_path), K(uri));
        } else if (OB_UNLIKELY(cur_obj_path_len == full_dir_path_len)) {
          // skip
          OB_LOG(INFO, "exist object path length is same with dir path length",
               K(request_id), K(cur_obj_path), K(full_dir_path), K(full_dir_path_len));
        } else if (OB_FAIL(list_ctx.handle_object(cur_obj_path, cur_obj_path_len, obj_size))) {
          OB_LOG(WARN, "fail to add listed s3 obejct meta into ctx",
              K(ret), K(request_id), K(cur_obj_path), K(cur_obj_path_len), K(obj_size));
        }
      } // end for
    }
  }
  return ret;
}

int ObStorageS3Util::del_dir_(const ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to del dir in s3", K(uri));
  UNUSED(uri);
  return ret;
}

int ObStorageS3Util::list_directories_(const ObString &uri, ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  const char *delimiter = "/";
  const char *full_dir_path = NULL;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open s3 base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = s3_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    const int64_t full_dir_path_len = strlen(full_dir_path); // actual dir path len
    Aws::S3::Model::ListObjectsOutcome outcome;
    Aws::String next_marker;
    do {
      if (OB_FAIL(s3_base.do_list_(OB_STORAGE_LIST_MAX_NUM, delimiter,
                                   next_marker, outcome))) {
        OB_LOG(WARN, "fail to list s3 objects", K(ret), K(uri), K(delimiter));
      } else {
        const char *request_id = outcome.GetResult().GetRequestId().c_str();
        const Aws::Vector<Model::CommonPrefix> &common_prefixes = outcome.GetResult().GetCommonPrefixes();
        for (int64_t i = 0; OB_SUCC(ret) && i < common_prefixes.size(); i++) {
          const Aws::S3::Model::CommonPrefix &tmp_common_prefix = common_prefixes[i];
          // For example,
          //       dir1
          //         --file1
          //         --dir11
          //            --file11
          // if we list directories in 'dir1', then full_dir_path == 'dir1/'
          // and listed_dir_full_path == 'dir1/dir11/', which represents the full directory path of 'dir11'
          const char *listed_dir_full_path = tmp_common_prefix.GetPrefix().c_str();
          const int64_t listed_dir_full_path_len = tmp_common_prefix.GetPrefix().size();
          OB_LOG(DEBUG, "s3 list directories", K(i), K(listed_dir_full_path));

          if (OB_UNLIKELY(false == ObString(listed_dir_full_path).prefix_match(full_dir_path))) {
            ret = OB_S3_ERROR;
            OB_LOG(WARN, "returned object prefix not match",
                K(ret), K(request_id), K(listed_dir_full_path), K(full_dir_path), K(uri));
          } else if (OB_UNLIKELY(!is_end_with_slash(listed_dir_full_path))) {
            ret = OB_S3_ERROR;
            OB_LOG(WARN, "the data has no directory",
                K(ret), K(request_id), K(full_dir_path), K(listed_dir_full_path), K(uri));
          } else if (OB_FAIL(handle_listed_directory(op,
              listed_dir_full_path + full_dir_path_len,
              listed_dir_full_path_len - 1 - full_dir_path_len))) {
            OB_LOG(WARN, "fail to handle s3 directory name", K(ret),
                K(request_id), K(listed_dir_full_path), K(full_dir_path), K(full_dir_path_len));
          }
        } // end for
        if (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated()) {
          next_marker = outcome.GetResult().GetNextMarker();
          if (next_marker.empty()) {
            ret = OB_S3_ERROR;
            OB_LOG(WARN, "when listing s3 directories, next marker should not be empty if result is truncated",
                K(ret), K(request_id), K(outcome.GetResult().GetIsTruncated()), K(next_marker.c_str()));
          }
        }
      }
    } while (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated());
  }
  return ret;
}

int ObStorageS3Util::is_tagging_(const ObString &uri, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  ObExternalIOCounterGuard io_guard;
  is_tagging = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 base", K(ret), K(uri));
  } else {
    Aws::S3::Model::GetObjectTaggingRequest request;
    request.WithBucket(s3_base.bucket_.ptr()).WithKey(s3_base.object_.ptr());
    Aws::S3::Model::GetObjectTaggingOutcome outcome;
    if (OB_FAIL(s3_base.s3_client_->get_object_tagging(request, outcome))) {
      OB_LOG(WARN, "failed to get s3 object tagging", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      // When using the getObjectTagging function to access an OBS object that does not have tags,
      // a NoSuchTagSet error will be returned.
      // The handle_s3_outcome function will translate the NoSuchTagSet error returned by S3
      // into the OB_ITEM_NOT_SETTED internal error
      if (OB_ITEM_NOT_SETTED == ret) {
        ret = OB_SUCCESS;
        is_tagging = false;
      } else {
        OB_LOG(WARN, "failed to get s3 object tagging",
            K(ret), K(uri), K(s3_base.bucket_), K(s3_base.object_));
      }
    } else {
      for (const Aws::S3::Model::Tag &tag : outcome.GetResult().GetTagSet()) {
        if (tag.GetKey() == "delete_mode" && tag.GetValue() == "tagging") {
          is_tagging = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObStorageS3Util::del_unmerged_parts_(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageS3Base s3_base;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(s3_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 base", K(ret), K(uri), KPC_(storage_info));
  } else {
    Aws::S3::Model::ListMultipartUploadsRequest request;
    Aws::S3::Model::ListMultipartUploadsOutcome outcome;
    request.WithBucket(s3_base.bucket_.ptr()).WithPrefix(s3_base.object_.ptr());
    request.SetMaxUploads(OB_STORAGE_LIST_MAX_NUM);
    Aws::S3::Model::AbortMultipartUploadRequest abort_request;
    Aws::S3::Model::AbortMultipartUploadOutcome abort_outcome;
    abort_request.WithBucket(s3_base.bucket_.ptr());
    do {
      if (OB_FAIL(s3_base.s3_client_->list_multipart_uploads(request, outcome))) {
        OB_LOG(WARN, "failed to list s3 multipart uploads", K(ret));
      } else if (!outcome.IsSuccess()) {
        handle_s3_outcome(outcome, ret);
        OB_LOG(WARN, "failed to list s3 multipart uploads",
            K(ret), K(uri), K(s3_base.bucket_), K(s3_base.object_));
      } else {
        const char *list_request_id = outcome.GetResult().GetRequestId().c_str();
        const Aws::Vector<Model::MultipartUpload> &uploads = outcome.GetResult().GetUploads();
        for (int64_t i = 0; OB_SUCC(ret) && i < uploads.size(); i++) {
          const Aws::String &obj = uploads[i].GetKey();
          const Aws::String &upload_id = uploads[i].GetUploadId();
          abort_request.WithKey(obj).WithUploadId(upload_id);
          OB_LOG(DEBUG, "list s3 multipat upload",
              K(ret), K(s3_base.bucket_), K(obj.c_str()), K(upload_id.c_str()));

          if (OB_FAIL(s3_base.s3_client_->abort_multipart_upload(abort_request, abort_outcome))) {
            OB_LOG(WARN, "failed to abort s3 multipart upload", K(ret),
                K(list_request_id), K(s3_base.bucket_), K(obj.c_str()), K(upload_id.c_str()));
          } else if (!abort_outcome.IsSuccess()) {
            handle_s3_outcome(abort_outcome, ret);
            OB_LOG(WARN, "failed to abort s3 multipart upload", K(ret),
                K(list_request_id), K(s3_base.bucket_), K(obj.c_str()), K(upload_id.c_str()));
          } else {
            OB_LOG(INFO, "succeed to abort s3 multipart upload",
                K(s3_base.bucket_), K(obj.c_str()), K(upload_id.c_str()));
          }
        }
        request.SetKeyMarker(outcome.GetResult().GetNextKeyMarker());
        request.SetUploadIdMarker(outcome.GetResult().GetNextUploadIdMarker());
      }
    } while (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated());
  }
  return ret;
}

/*--------------------------------ObStorageS3AppendWriter--------------------------------*/
ObStorageS3AppendWriter::ObStorageS3AppendWriter()
    : ObStorageS3Writer(),
      storage_info_(NULL)
{
}

ObStorageS3AppendWriter::~ObStorageS3AppendWriter()
{
}

int ObStorageS3AppendWriter::open_(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 append writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageS3Writer::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in s3 base", K(ret), K(uri));
  } else {
    is_opened_ = true;
    storage_info_ = storage_info;
  }
  return ret;
}

int ObStorageS3AppendWriter::write_(const char *buf, const int64_t size)
{
  return pwrite(buf, size, 0);
}

int ObStorageS3AppendWriter::pwrite_(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  char fragment_name[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  Aws::S3::Model::PutObjectRequest request;
  ObExternalIOCounterGuard io_guard;
  if(OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 append writer cannot write before it is not opened", K(ret));
  } else if(OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size), K(offset));
  } else {
    // write the format file when writing the first fragment because the appender may open multiple times
    if (offset == 0) {
      if (OB_FAIL(construct_fragment_full_name(object_, OB_S3_APPENDABLE_FORMAT_META,
                                               fragment_name, sizeof(fragment_name)))) {
        OB_LOG(WARN, "failed to construct s3 mock append object foramt name",
            K(ret), K_(bucket), K_(object));
      } else if (OB_FAIL(write_obj_(fragment_name, OB_S3_APPENDABLE_FORMAT_CONTENT_V1,
                                    strlen(OB_S3_APPENDABLE_FORMAT_CONTENT_V1)))) {
        OB_LOG(WARN, "fail to write s3 mock append object format file", K(ret), K(fragment_name));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_fragment_full_name(object_, offset, offset + size,
                                                    fragment_name,  sizeof(fragment_name)))) {
      // fragment_name: /xxx/xxx/appendable_obj_name/start-end,
      // the data range covered by this file is from start to end == [start, end), include start not include end
      // start == offset, end == offset + size
      // fragment length == size
      OB_LOG(WARN, "failed to set fragment name for s3 append writer",
          K(ret), K_(bucket), K_(object), K(buf), K(size), K(offset));
    } else if (OB_FAIL(write_obj_(fragment_name, buf, size))) {
      OB_LOG(WARN, "fail to append a fragment into s3",
          K(ret), K_(bucket), K_(object), K(fragment_name), K(size));
    }
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "succeed to append a fragment into s3",
        K_(bucket), K_(object), K(fragment_name), K(size), K(offset));
  }
  return ret;
}

int ObStorageS3AppendWriter::close_()
{
  int ret = OB_SUCCESS;

  is_opened_ = false;
  storage_info_ = NULL;
  reset();
  return ret;
}

int64_t ObStorageS3AppendWriter::get_length() const
{
  int ret = OB_NOT_SUPPORTED;
  OB_LOG(WARN, "s3 appender do not support get length now", K(ret), K_(bucket), K_(object));
  return -1;
}

/*--------------------------------ObStorageS3MultiPartWriter--------------------------------*/
ObStorageS3MultiPartWriter::ObStorageS3MultiPartWriter()
    : ObStorageS3Base(),
      is_opened_(false),
      base_buf_(NULL), base_buf_pos_(-1),
      upload_id_(NULL),
      partnum_(0),
      file_length_(-1)
{}

ObStorageS3MultiPartWriter::~ObStorageS3MultiPartWriter()
{}

void ObStorageS3MultiPartWriter::reset()
{
  is_opened_ = false;
  base_buf_ = NULL;
  base_buf_pos_ = -1;
  upload_id_ = NULL;
  partnum_ = 0;
  file_length_ = -1;
  ObStorageS3Base::reset();
}

int ObStorageS3MultiPartWriter::open_(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_S3_ERROR;
    OB_LOG(WARN, "s3 multipart writer already opened, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageS3Base::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in s3 base", K(ret), K(uri));
  } else {
    Aws::S3::Model::CreateMultipartUploadRequest request;
    request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());
    Aws::S3::Model::CreateMultipartUploadOutcome outcome;

    if (OB_FAIL(set_request_checkusum_algorithm(request, checksum_type_))) {
      OB_LOG(WARN, "fail to set checksum algorithm", K(ret), K_(checksum_type));
    } else if (OB_FAIL(s3_client_->create_multipart_upload(request, outcome))) {
      OB_LOG(WARN, "failed to create s3 multipart upload", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      OB_LOG(WARN, "failed to create multipart upload for s3",
          K(ret), K_(bucket), K_(object));
    } else {
      const Aws::String &upload_id = outcome.GetResult().GetUploadId();
      if (OB_ISNULL(upload_id_ = static_cast<char *>(allocator_.alloc(upload_id.size() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to alloc buf for s3 multipartupload upload id",
            K(ret), K(upload_id.size()));
      } else if (OB_ISNULL(base_buf_ =
          static_cast<char *>(allocator_.alloc(S3_MULTIPART_UPLOAD_BUFFER_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to alloc buf for s3 multipartupload buffer",
            K(ret), K(S3_MULTIPART_UPLOAD_BUFFER_SIZE));
      } else {
        STRNCPY(upload_id_, upload_id.c_str(), upload_id.size());
        upload_id_[upload_id.size()] = '\0';
        base_buf_pos_ = 0;
        file_length_ = 0;
        is_opened_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageS3MultiPartWriter::write_(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t fill_size = 0;
  int64_t buf_pos = 0;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 multipart writer not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
  }

  while (OB_SUCC(ret) && buf_pos != size) {
    fill_size = MIN(S3_MULTIPART_UPLOAD_BUFFER_SIZE - base_buf_pos_, size - buf_pos);
    memcpy(base_buf_ + base_buf_pos_, buf + buf_pos, fill_size);
    base_buf_pos_ += fill_size;
    buf_pos += fill_size;
    if (base_buf_pos_ == S3_MULTIPART_UPLOAD_BUFFER_SIZE) {
      if (OB_FAIL(write_single_part_())) {
        OB_LOG(WARN, "failed to write single s3 part", K(ret), K_(bucket), K_(object));
      } else {
        base_buf_pos_ = 0;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    file_length_ += size;
  }
  return ret;
}

int ObStorageS3MultiPartWriter::pwrite_(const char *buf, const int64_t size, const int64_t offset)
{
  UNUSED(offset);
  return write(buf, size);
}

static bool is_obs_destination(const Aws::S3::Model::CompleteMultipartUploadOutcome &outcome)
{
  Aws::String key_str = Aws::Utils::StringUtils::ToLower("server");
  const Aws::Http::HeaderValueCollection &headers = outcome.GetError().GetResponseHeaders();
  Aws::Http::HeaderValueCollection::const_iterator iter = headers.find(key_str);
  return (iter != headers.end() && iter->second == "OBS");
}

static bool is_gcs_destination(const Aws::S3::Model::CompleteMultipartUploadOutcome &outcome)
{
  // The x-guploader-uploadid header is a unique identifier provided in Cloud Storage responses.
  return outcome.GetError().ResponseHeaderExists("x-guploader-uploadid");
}

int ObStorageS3MultiPartWriter::complete_()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 multipart writer cannot compelete before it is opened", K(ret));
  } else if (base_buf_pos_ > 0) {
    if (OB_FAIL(write_single_part_())) {
      OB_LOG(WARN, "failed to upload last part into s3", K(ret), K_(base_buf_pos));
    } else {
      base_buf_pos_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    // list parts
    Aws::S3::Model::CompletedMultipartUpload completed_multipart_upload;
    int64_t part_num = 0;
    Aws::S3::Model::ListPartsRequest request;
    request.WithBucket(bucket_.ptr()).WithKey(object_.ptr()).WithUploadId(upload_id_);
    request.SetMaxParts(OB_STORAGE_LIST_MAX_NUM);
    Aws::S3::Model::ListPartsOutcome outcome;
    do {
      if (OB_FAIL(s3_client_->list_parts(request, outcome))) {
        OB_LOG(WARN, "failed to list s3 multipart upload parts", K(ret));
      } else if (!outcome.IsSuccess()) {
        handle_s3_outcome(outcome, ret);
        OB_LOG(WARN, "failed to list s3 uploaded parts",
            K(ret), K_(bucket), K_(object));
      } else {
        const Aws::Vector<Aws::S3::Model::Part> &parts = outcome.GetResult().GetParts();
        part_num += parts.size();
        for (int64_t i = 0; OB_SUCC(ret) && i < parts.size(); i++) {
          Aws::S3::Model::CompletedPart tmp_part;
          if (OB_FAIL(set_completed_part_checksum(tmp_part, parts[i], checksum_type_))) {
            OB_LOG(WARN, "fail to set completed part checksum", K(ret), K_(checksum_type));
          } else {
            tmp_part.WithPartNumber(parts[i].GetPartNumber()).WithETag(parts[i].GetETag());
            completed_multipart_upload.AddParts(std::move(tmp_part));
          }
        }
      }
      request.SetPartNumberMarker(outcome.GetResult().GetNextPartNumberMarker());
    } while (OB_SUCC(ret) && outcome.GetResult().GetIsTruncated());


    // complete upload
    Aws::S3::Model::CompleteMultipartUploadRequest complete_multipart_upload_request;
    complete_multipart_upload_request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());
    complete_multipart_upload_request.WithUploadId(upload_id_);
    complete_multipart_upload_request.WithMultipartUpload(completed_multipart_upload);

    Aws::S3::Model::CompleteMultipartUploadOutcome complete_multipart_upload_outcome;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(part_num == 0)) {
      // If 'complete' without uploading any data, S3 will return the error
      // 'InvalidRequest，You must specify at least one part'
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "no parts have been uploaded!", K(ret), K(part_num), K_(upload_id));
    } else if (OB_FAIL(s3_client_->complete_multipart_upload(complete_multipart_upload_request,
                                                             complete_multipart_upload_outcome))) {
      OB_LOG(WARN, "failed to complete s3 multipart upload", K(ret));
    } else if (!complete_multipart_upload_outcome.IsSuccess()) {
      handle_s3_outcome(complete_multipart_upload_outcome, ret);

      // Due to the following reasons:
      //   1. OBS/GCS is currently accessed through the S3 SDK without any dedicated OBS/GCS prefix or type,
      //      and using the host to determine if the endpoint is OBS/GCS is not secure.
      //   2. The S3 SDK can only acquire the response header to distinguish the server type
      //      when a request fails.
      //   3. Furthermore, when the S3 SDK is used to access OBS/GCS with the checksum type set to crc32,
      //      the OBS/GCS server will only return an error in the complete_multipart_upload interface.
      // Therefore, it is only possible to determine at this point
      // whether the configured checksum type is supported by the destination.
      // If the checksum algorithm is crc32 and the destination type is OBS/GCS,
      // for the caller at a higher level,
      // the checksum algorithm should first be set to a type supported by OBS/GCS.
      // Hence, the error code is directly translated to OB_CHECKSUM_TYPE_NOT_SUPPORTED.
      if (ObStorageChecksumType::OB_CRC32_ALGO == checksum_type_
          && (is_obs_destination(complete_multipart_upload_outcome)
              || is_gcs_destination(complete_multipart_upload_outcome))) {
        ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
        OB_LOG(WARN, "the checksum type is not supported for OBS/GCS", K(ret), K_(checksum_type));
      }

      OB_LOG(WARN, "failed to complete multipart upload for s3",
          K(ret), K_(bucket), K_(object));
    }

  }
  return ret;
}

int ObStorageS3MultiPartWriter::close_()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  reset();
  return ret;
}

int ObStorageS3MultiPartWriter::abort_()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 multipart writer not opened", K(ret));
  } else {
    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());
    request.WithUploadId(upload_id_);
    Aws::S3::Model::AbortMultipartUploadOutcome outcome;
    if (OB_FAIL(s3_client_->abort_multipart_upload(request, outcome))) {
      OB_LOG(WARN, "failed to abort s3 multipart upload", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      OB_LOG(WARN, "failed to abort s3 multipart upload",
          K(ret), K_(bucket), K_(object), K_(partnum), K_(upload_id));
    }
  }
  return ret;
}

int ObStorageS3MultiPartWriter::write_single_part_()
{
  // TODO @fangdan: compress data
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ++partnum_; // partnum is between 1 and 10000
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "s3 multipart writer not opened", K(ret));
  } else if (partnum_ > MAX_S3_PART_NUM) {
    ret = OB_OUT_OF_ELEMENT;
    OB_LOG(WARN, "out of s3 part num effective range", K(ret), K_(partnum), K(MAX_S3_PART_NUM));
  } else {
    Aws::S3::Model::UploadPartRequest request;
    request.WithBucket(bucket_.ptr()).WithKey(object_.ptr());
    request.WithPartNumber(partnum_).WithUploadId(upload_id_);
    std::shared_ptr<Aws::IOStream> data_stream =
        Aws::MakeShared<Aws::StringStream>(S3_SDK);
    data_stream->write(base_buf_, base_buf_pos_);
    data_stream->flush();
    request.SetBody(data_stream);

    Aws::S3::Model::UploadPartOutcome outcome;
    if (OB_FAIL(set_request_checkusum_algorithm(request, checksum_type_))) {
      OB_LOG(WARN, "fail to set checksum algorithm", K(ret), K_(checksum_type));
    } else if (OB_FAIL(s3_client_->upload_part(request, outcome))) {
      OB_LOG(WARN, "failed to upload s3 multipart", K(ret));
    } else if (!outcome.IsSuccess()) {
      handle_s3_outcome(outcome, ret);
      OB_LOG(WARN, "failed to upload part into s3", K(ret), K_(bucket), K_(object), K_(partnum));
    } else {
      OB_LOG(DEBUG, "succed upload a part into s3", K_(partnum), K_(bucket), K_(object));
    }
  }
  return ret;
}

} // common
} // oceanbase