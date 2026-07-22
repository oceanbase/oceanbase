/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_MANAGER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_MANAGER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "share/ai_service/ob_batch_file_jsonl_writer.h"
#include "share/ai_service/ob_batch_file_jsonl_iterator.h"

namespace oceanbase
{
namespace share
{

// Batch file status for OpenAI Batch API
enum ObAiBatchFileStatus
{
  OB_AI_BATCH_FILE_STATUS_INVALID = 0,
  OB_AI_BATCH_FILE_STATUS_UPLOADING = 1,
  OB_AI_BATCH_FILE_STATUS_UPLOADED = 2,
  OB_AI_BATCH_FILE_STATUS_SUBMITTING = 3,
  OB_AI_BATCH_FILE_STATUS_QUEUED = 4,
  OB_AI_BATCH_FILE_STATUS_IN_PROGRESS = 5,
  OB_AI_BATCH_FILE_STATUS_FINALIZING = 6,
  OB_AI_BATCH_FILE_STATUS_COMPLETED = 7,
  OB_AI_BATCH_FILE_STATUS_FAILED = 8,
  OB_AI_BATCH_FILE_STATUS_EXPIRED = 9,
  OB_AI_BATCH_FILE_STATUS_CANCELLED = 10,
  OB_AI_BATCH_FILE_STATUS_MAX
};

// Upload file result
struct ObAiFileUploadResult
{
public:
  ObAiFileUploadResult() { reset(); }
  ~ObAiFileUploadResult() = default;

  void reset()
  {
    file_id_.reset();
    status_.reset();
    file_name_.reset();
    purpose_.reset();
    error_detail_.reset();
  }

  bool is_success() const { return !file_id_.empty(); }

  TO_STRING_KV(K_(file_id), K_(status));

public:
  common::ObString file_id_;      // File ID from provider
  common::ObString status_;       // File status
  common::ObString file_name_;    // Original file name
  common::ObString purpose_;      // File purpose (e.g., "batch")
  common::ObString error_detail_; // JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiFileUploadResult);
};

// Batch submission result
struct ObAiBatchSubmitResult
{
public:
  ObAiBatchSubmitResult() { reset(); }
  ~ObAiBatchSubmitResult() = default;

  void reset()
  {
    batch_id_.reset();
    status_ = OB_AI_BATCH_FILE_STATUS_INVALID;
    input_file_id_.reset();
    output_file_id_.reset();
    error_file_id_.reset();
    created_at_ = 0;
    completed_at_ = 0;
    expired_at_ = 0;
    request_counts_total_ = 0;
    request_counts_completed_ = 0;
    request_counts_failed_ = 0;
    error_detail_.reset();
  }

  bool is_success() const { return error_detail_.empty() && !batch_id_.empty(); }
  bool is_terminal() const;
  bool is_completed() const { return OB_AI_BATCH_FILE_STATUS_COMPLETED == status_; }
  bool is_failed() const { return OB_AI_BATCH_FILE_STATUS_FAILED == status_; }

  TO_STRING_KV(K_(batch_id), K_(status), K_(error_detail), K_(request_counts_total),
               K_(request_counts_completed), K_(request_counts_failed));

public:
  common::ObString batch_id_;             // Batch ID from provider
  ObAiBatchFileStatus status_;            // Batch status
  common::ObString input_file_id_;        // Input file ID
  common::ObString output_file_id_;       // Output file ID (results)
  common::ObString error_file_id_;        // Error file ID
  int64_t created_at_;                    // Creation timestamp
  int64_t completed_at_;                  // Completion timestamp
  int64_t expired_at_;                    // Expiration timestamp
  int64_t request_counts_total_;          // Total requests in batch
  int64_t request_counts_completed_;      // Completed requests
  int64_t request_counts_failed_;         // Failed requests
  common::ObString error_detail_;         // JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}

private:
  DISALLOW_COPY_AND_ASSIGN(ObAiBatchSubmitResult);
};

// Batch file line result (from JSONL parsing)
struct ObAiBatchLineResult
{
public:
  ObAiBatchLineResult() { reset(); }
  ~ObAiBatchLineResult() = default;

  void reset()
  {
    custom_id_.reset();
    response_status_ = 0;
    response_body_.reset();
    error_detail_.reset();
  }

  bool is_success() const { return 200 == response_status_; }

  TO_STRING_KV(K_(custom_id), K_(response_status));

public:
  common::ObString custom_id_;      // Custom ID from request
  int64_t response_status_;         // HTTP response status code
  common::ObString response_body_;  // Response body (JSON)
  common::ObString error_detail_;   // JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}
};

/**
 * @brief Batch file manager for handling OpenAI Batch API operations
 *
 * This class provides functionality for:
 * 1. Uploading files to AI providers (PUT /v1/files)
 * 2. Submitting batch jobs (POST /v1/batches)
 * 3. Polling batch status (GET /v1/batches/{batch_id})
 * 4. Downloading result files (GET /v1/files/{file_id}/content)
 * 5. Parsing JSONL result files
 */
class ObAiBatchFileManager
{
public:
  // Constants
  static const int64_t DEFAULT_HTTP_TIMEOUT_US = 60 * 1000 * 1000;   // 60 seconds
  static const int64_t DOWNLOAD_HTTP_TIMEOUT_US = 600 * 1000 * 1000; // 600 seconds for large file downloads
static const int64_t DEFAULT_POLL_INTERVAL_US = 60 * 1000 * 1000;  // 60 seconds

  ObAiBatchFileManager();
  ~ObAiBatchFileManager();

  /**
   * @brief Initialize the manager
   * @param allocator Memory allocator
   * @param base_url Base URL for API
   * @param api_key API key for authentication
   * @return OB_SUCCESS on success
   */
  int init(common::ObIAllocator &allocator,
           const common::ObString &base_url,
           const common::ObString &api_key);

  /**
   * @brief Reset manager state
   */
  void reset();

  /**
   * @brief Upload from TmpFileManager fd using streaming curl_mime_data_cb
   * @param fd TmpFileManager file descriptor
   * @param file_size Total file size
   * @param tenant_id Tenant ID for TmpFileManager access
   * @param file_name File name for the upload
   * @param purpose File purpose (e.g., "batch")
   * @param result Output upload result
   * @return OB_SUCCESS on success
   */
  int upload_from_tmpfile(int64_t fd,
                          int64_t file_size,
                          uint64_t tenant_id,
                          const common::ObString &file_name,
                          const common::ObString &purpose,
                          ObAiFileUploadResult &result);

  /**
   * @brief Download result file to TmpFileManager via streaming curl callback
   * @param file_id Provider file ID to download
   * @param writer Pre-initialized ObBatchFileJsonlWriter (receives downloaded data)
   * @param segment Output segment metadata after download completes
   * @return OB_SUCCESS on success
   */
  int download_to_tmpfile(const common::ObString &file_id,
                          ObBatchFileJsonlWriter &writer,
                          ObBatchFileDataSegment &segment);

  /**
   * @brief Submit a batch job
   * @param input_file_id File ID from upload
   * @param endpoint Endpoint URL (e.g., "/v1/embeddings")
   * @param completion_window Completion window (e.g., "24h")
   * @param result Output submission result
   * @return OB_SUCCESS on success
   */
  int submit_batch(const common::ObString &input_file_id,
                   const common::ObString &endpoint,
                   const common::ObString &completion_window,
                   ObAiBatchSubmitResult &result);

  /**
   * @brief Poll batch status
   * @param batch_id Batch ID to poll
   * @param result Output status result
   * @return OB_SUCCESS on success
   */
  int poll_batch_status(const common::ObString &batch_id,
                        ObAiBatchSubmitResult &result);

  /**
   * @brief Parse JSONL result from buffer (static method)
   * @param allocator Memory allocator for parsing
   * @param buffer JSONL content buffer
   * @param buffer_size Buffer size
   * @param results Output array of results
   * @return OB_SUCCESS on success
   */
  static int parse_jsonl_results_from_buffer(common::ObIAllocator &allocator,
                                              const char *buffer,
                                              int64_t buffer_size,
                                              common::ObIArray<ObAiBatchLineResult> &results);

  /**
   * @brief Cancel a batch job
   * @param batch_id Batch ID to cancel
   * @return OB_SUCCESS on success
   */
  int cancel_batch(const common::ObString &batch_id);

  /**
   * @brief Delete a remote provider file
   * @param file_id Provider file ID to delete
   * @return OB_SUCCESS on success
   */
  int delete_file(const common::ObString &file_id);

  bool is_inited() const { return is_inited_; }
  int64_t get_last_http_status_code() const { return http_status_code_; }

  /// Map HTTP status code to OceanBase internal error code.
  /// - 0        → OB_TIMEOUT (network timeout)
  /// - 400      → OB_INVALID_ARGUMENT
  /// - 401/403  → OB_ERR_NO_PRIVILEGE (auth failure)
  /// - 404      → OB_ENTRY_NOT_EXIST
  /// - 408      → OB_TIMEOUT
  /// - 413      → OB_SIZE_OVERFLOW
  /// - 429      → OB_EAGAIN (rate limit, retryable)
  /// - 5xx      → OB_RPC_POST_ERROR (server error, retryable)
  /// - 4xx else → OB_INVALID_ARGUMENT (client error, not retryable)
  static int map_http_status_to_error_code(int64_t http_status);

  TO_STRING_KV(K_(is_inited), K_(base_url));

private:
  // HTTP operations using curl
  int execute_http_request_(const char *url,
                           const char *method,
                           const struct curl_slist *headers,
                           const char *body,
                           int64_t body_len);
  int execute_authenticated_request_(const char *url,
                                     const char *method,
                                     const char *body,
                                     int64_t body_len,
                                     int64_t expected_http_code,
                                     const char *request_name,
                                     bool add_json_content_type = false);
  int init_authenticated_curl_(const char *endpoint,
                               int64_t timeout_us,
                               char *url_buf,
                               int64_t url_buf_len,
                               struct curl_slist *&headers,
                               CURL *&curl);

  // Response handling
  int parse_upload_response_(const char *response, int64_t response_len,
                            ObAiFileUploadResult &result);
  int parse_batch_response_(const char *response, int64_t response_len,
                           ObAiBatchSubmitResult &result);
  static int parse_jsonl_line_(common::ObIAllocator &allocator,
                               common::ObIAllocator &json_alloc,
                               const char *line, int64_t line_len,
                               ObAiBatchLineResult &result);
  static int parse_json_object_(const common::ObString &json_str,
                                common::ObIAllocator &alloc,
                                common::ObJsonObject *&out_obj);

  // Helper methods
  int build_url_(const char *endpoint, char *buf, int64_t buf_len);
  int build_auth_headers_(struct curl_slist *&headers);
  void setup_curl_common_(CURL *curl, int64_t timeout_us);
  static bool is_success_http_code_(int64_t actual, int64_t expected);

  // Build error_detail JSON string from allocator. Returns OB_SUCCESS or error code.
  // Caller decides whether to pollute its own ret: use tmp_ret in error-recovery paths
  // where the primary error must be preserved, or OB_FAIL where ret is still OB_SUCCESS.
  static int build_error_detail_(common::ObIAllocator &allocator,
                                 int ob_error_code,
                                 int64_t http_code,
                                 const common::ObString &message,
                                 common::ObString &error_detail);

  static size_t curl_write_callback_(void *contents, size_t size, size_t nmemb, void *userp);
  static size_t curl_mime_read_callback_(char *buffer, size_t size, size_t nitems, void *arg);
  static size_t curl_tmpfile_write_callback_(void *contents, size_t size, size_t nmemb, void *userp);

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;

  // Configuration
  common::ObString base_url_;
  common::ObString api_key_;

  // HTTP state
  char *response_buffer_;
  int64_t response_buffer_size_;
  int64_t response_buffer_capacity_;
  int64_t http_status_code_;

  common::ObArenaAllocator local_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObAiBatchFileManager);
};

/**
 * @brief Utility functions for batch file operations
 */
class ObAiBatchFileManagerUtils
{
public:
  // Convert string to batch status enum
  static ObAiBatchFileStatus str_to_batch_status(const common::ObString &status_str);

  // Convert batch status enum to string
  static const char* batch_status_to_str(ObAiBatchFileStatus status);

  // Calculate poll interval based on batch status
  static int64_t calculate_poll_interval(ObAiBatchFileStatus status);

private:
  ObAiBatchFileManagerUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObAiBatchFileManagerUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_BATCH_FILE_MANAGER_H_