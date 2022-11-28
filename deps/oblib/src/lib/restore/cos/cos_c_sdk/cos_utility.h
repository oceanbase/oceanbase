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

#ifndef LIBCOS_UTILITY_H
#define LIBCOS_UTILITY_H

#include "cos_string.h"
#include "cos_transport.h"
#include "cos_status.h"
#include "cos_define.h"
#include "cos_resumable.h"

COS_CPP_START

#define init_sts_token_header() do { \
        if (options->config->sts_token.data != NULL) {\
            apr_table_set(headers, COS_STS_SECURITY_TOKEN, options->config->sts_token.data);\
        }\
    } while(0)

/**
  * @brief  check hostname ends with specific cos domain suffix.
**/
int is_cos_domain(const cos_string_t *str);

/**
  * @brief  check hostname is ip.
**/
int is_valid_ip(const char *str);

/**
  * @brief  get cos acl str according cos_acl
  * @param[in]  cos_acl the cos bucket acl
  * @return cos acl str
**/
const char *get_cos_acl_str(cos_acl_e cos_acl);

/**
  * @brief  create cos config including host, port, access_key_id, access_key_secret, is_cos_domain
**/
cos_config_t *cos_config_create(cos_pool_t *p);

/**
  * @brief evaluate config to curl
**/
void cos_config_resolve(cos_pool_t *pool, cos_config_t *config, cos_http_controller_t *ctl);

/**
  * @brief  create cos request options
  * @return cos request options
**/
cos_request_options_t *cos_request_options_create(cos_pool_t *p);

/**
  * @brief  init cos request
**/
void cos_init_request(const cos_request_options_t *options, http_method_e method,
        cos_http_request_t **req, cos_table_t *params, cos_table_t *headers, cos_http_response_t **resp);

/**
  * @brief  init cos service request
**/
void cos_init_service_request(const cos_request_options_t *options, http_method_e method,
        cos_http_request_t **req, cos_table_t *params, cos_table_t *headers, const int all_region, cos_http_response_t **resp);

/**
  * @brief  init cos bucket request
**/
void cos_init_bucket_request(const cos_request_options_t *options, const cos_string_t *bucket,
        http_method_e method, cos_http_request_t **req, cos_table_t *params, cos_table_t *headers,
        cos_http_response_t **resp);

/**
  * @brief  init cos object request
**/
void cos_init_object_request(const cos_request_options_t *options, const cos_string_t *bucket,
        const cos_string_t *object, http_method_e method, cos_http_request_t **req,
        cos_table_t *params, cos_table_t *headers, cos_progress_callback cb, uint64_t initcrc,
        cos_http_response_t **resp);

/**
  * @brief  init cos live channel request
**/
void cos_init_live_channel_request(const cos_request_options_t *options,
    const cos_string_t *bucket, const cos_string_t *live_channel,
    http_method_e method, cos_http_request_t **req, cos_table_t *params,
    cos_table_t *headers, cos_http_response_t **resp);

/**
  * @brief  init cos request with signed_url
**/
void cos_init_signed_url_request(const cos_request_options_t *options, const cos_string_t *signed_url,
        http_method_e method, cos_http_request_t **req,
        cos_table_t *params, cos_table_t *headers, cos_http_response_t **resp);

/**
  * @brief  cos send request
**/
cos_status_t *cos_send_request(cos_http_controller_t *ctl, cos_http_request_t *req,
        cos_http_response_t *resp);

/**
  * @brief process cos request including sign request, send request, get response
**/
cos_status_t *cos_process_request(const cos_request_options_t *options,
        cos_http_request_t *req, cos_http_response_t *resp);

/**
  * @brief process cos request with signed_url including send request, get response
**/
cos_status_t *cos_process_signed_request(const cos_request_options_t *options,
        cos_http_request_t *req, cos_http_response_t *resp);

/**
  * @brief  get object uri using third-level domain if hostname is cos domain, otherwise second-level domain
**/
void cos_get_object_uri(const cos_request_options_t *options,
                        const cos_string_t *bucket,
                        const cos_string_t *object,
                        cos_http_request_t *req);

/**
  * @brief   bucket uri using third-level domain if hostname is cos domain, otherwise second-level domain
**/
void cos_get_bucket_uri(const cos_request_options_t *options,
                        const cos_string_t *bucket,
                        cos_http_request_t *req);

/**
  * @brief  service uri
**/
void cos_get_service_uri(const cos_request_options_t *options,
                         const int all_region,
                         cos_http_request_t *req);

/**
  * @brief  get rtmp uri using third-level domain if hostname is cos domain, otherwise second-level domain
**/
void cos_get_rtmp_uri(const cos_request_options_t *options,
                      const cos_string_t *bucket,
                      const cos_string_t *live_channel_id,
                      cos_http_request_t *req);

/**
  * @brief  write body content into cos request body from buffer
**/
void cos_write_request_body_from_buffer(cos_list_t *buffer, cos_http_request_t *req);

/**
  * @brief   write body content into cos request body from file
**/
int cos_write_request_body_from_file(cos_pool_t *p, const cos_string_t *filename, cos_http_request_t *req);

/**
  * @brief   write body content into cos request body from multipart upload file
**/
int cos_write_request_body_from_upload_file(cos_pool_t *p, cos_upload_file_t *upload_file, cos_http_request_t *req);

/**
  * @brief  read body content from cos response body to buffer
**/
void cos_fill_read_response_body(cos_http_response_t *resp, cos_list_t *buffer);

/**
  * @brief  read body content from cos response body to file
**/
int cos_init_read_response_body_to_file(cos_pool_t *p, const cos_string_t *filename, cos_http_response_t *resp);

/**
  * @brief  read response header if headers is not null
**/
void cos_fill_read_response_header(cos_http_response_t *resp, cos_table_t **headers);

/**
  * @brief  create cos api result content
  * @return cos api result content
**/
void *cos_create_api_result_content(cos_pool_t *p, size_t size);
cos_acl_grantee_content_t *cos_create_acl_list_content(cos_pool_t *p);
cos_get_service_content_t *cos_create_get_service_content(cos_pool_t *p);
cos_list_object_content_t *cos_create_list_object_content(cos_pool_t *p);
cos_list_object_common_prefix_t *cos_create_list_object_common_prefix(cos_pool_t *p);
cos_list_part_content_t *cos_create_list_part_content(cos_pool_t *p);
cos_list_multipart_upload_content_t *cos_create_list_multipart_upload_content(cos_pool_t *p);
cos_complete_part_content_t *cos_create_complete_part_content(cos_pool_t *p);

/**
 *  @brief create cos api get service parameters
 *  @return cos api get service parameters
**/
cos_get_service_params_t *cos_create_get_service_params(cos_pool_t *p);

/**
  * @brief  create cos api list parameters
  * @return cos api list parameters
**/
cos_list_object_params_t *cos_create_list_object_params(cos_pool_t *p);
cos_list_upload_part_params_t *cos_create_list_upload_part_params(cos_pool_t *p);
cos_list_multipart_upload_params_t *cos_create_list_multipart_upload_params(cos_pool_t *p);
cos_list_live_channel_params_t *cos_create_list_live_channel_params(cos_pool_t *p);
cos_acl_params_t *cos_create_acl_params(cos_pool_t *p);
cos_copy_object_params_t *cos_create_copy_object_params(cos_pool_t *p);

/**
  * @brief  create upload part copy params
  * @return upload part copy params struct for upload part copy
**/
cos_upload_part_copy_params_t *cos_create_upload_part_copy_params(cos_pool_t *p);

/**
  * @brief  create upload file struct for range multipart upload
  * @return upload file struct for range multipart upload
**/
cos_upload_file_t *cos_create_upload_file(cos_pool_t *p);

/**
  * @brief  get content-type for HTTP_POST request
  * @return content-type for HTTP_POST request
**/
void cos_set_multipart_content_type(cos_table_t *headers);

/**
  * @brief  create lifecycle rule content
  * @return lifecycle rule content
**/
cos_lifecycle_rule_content_t *cos_create_lifecycle_rule_content(cos_pool_t *p);

/**
  * @brief  create cors rule content
  * @return cors rule content
**/
cos_cors_rule_content_t *cos_create_cors_rule_content(cos_pool_t *p);

/**
  * @brief  create versioning content
  * @return bucket versioning content
**/
cos_versioning_content_t *cos_create_versioning_content(cos_pool_t *p);

/**
  * @brief  create replication rule content
  * @return replication rule content
**/
cos_replication_rule_content_t *cos_create_replication_rule_content(cos_pool_t *p);

/**
  * @brief  create replication param
  * @return replication param
**/
cos_replication_params_t *cos_create_replication_params(cos_pool_t *p);

/**
 *  @brief  create website rule content
 */
cos_website_rule_content_t *cos_create_website_rule_content(cos_pool_t *P);

/**
 *  @brief  create website params
 */
cos_website_params_t *cos_create_website_params(cos_pool_t *p);

/**
 *  @brief  create domain params
 */
cos_domain_params_t *cos_create_domain_params(cos_pool_t *p);

// @brief  create logging params
cos_logging_params_t *cos_create_logging_params(cos_pool_t *p);

// @brief  create inventory params
cos_list_inventory_params_t *cos_create_list_inventory_params(cos_pool_t *p);
cos_inventory_params_t *cos_create_inventory_params(cos_pool_t *p);
cos_inventory_optional_t *cos_create_inventory_optional(cos_pool_t *p);

// @brief  create tagging params
cos_tagging_params_t *cos_create_tagging_params(cos_pool_t *p);
cos_tagging_tag_t *cos_create_tagging_tag(cos_pool_t *p);

// @brief  create intelligenttiering params
cos_intelligenttiering_params_t *cos_create_intelligenttiering_params(cos_pool_t *p);

cos_object_restore_params_t *cos_create_object_restore_params(cos_pool_t *p);

/**
  * @brief  create cos object content for delete objects
  * @return cos object content
**/
cos_object_key_t *cos_create_cos_object_key(cos_pool_t *p);

/**
  * @brief  create cos live channel publish url content for delete objects
  * @return cos live channel publish url content
**/
cos_live_channel_publish_url_t *cos_create_live_channel_publish_url(cos_pool_t *p);

/**
  * @brief  create cos live channel play url content for delete objects
  * @return cos live channel play url content
**/
cos_live_channel_play_url_t *cos_create_live_channel_play_url(cos_pool_t *p);

/**
  * @brief  create cos list live channel content for delete objects
  * @return cos list live channel content
**/
cos_live_channel_content_t *cos_create_list_live_channel_content(cos_pool_t *p);

/**
  * @brief  create cos live recored content for delete objects
  * @return cos live record content
**/
cos_live_record_content_t *cos_create_live_record_content(cos_pool_t *p);

/**
  * @brief  create live channel configuration content
  * @return live channel configuration content
**/
cos_live_channel_configuration_t *cos_create_live_channel_configuration_content(cos_pool_t *p);

/**
  * @brief  create cos checkpoint content
  * @return cos checkpoint content
**/
cos_checkpoint_t *cos_create_checkpoint_content(cos_pool_t *p);

/**
  * @brief  create cos resumable clt params content
  * @return cos checkpoint content
**/
cos_resumable_clt_params_t *cos_create_resumable_clt_params_content(cos_pool_t *p, int64_t part_size, int32_t thread_num,
                                                                    int enable_checkpoint, const char *checkpoint_path);

/**
  * @brief  get part size for multipart upload
**/
void cos_get_part_size(int64_t filesize, int64_t *part_size);

/**
  * @brief  compare function for part sort
**/
int part_sort_cmp(const void *a, const void *b);

/**
  * @brief  set content type for object according to objectname
  * @return cos content type
**/
char *get_content_type(const char *name);
char *get_content_type_by_suffix(const char *suffix);

/**
  * @brief  set content type for object according to  filename
**/
void set_content_type(const char* filename, const char* key, cos_table_t *headers);

cos_table_t* cos_table_create_if_null(const cos_request_options_t *options,
                                      cos_table_t *table, int table_size);

int is_enable_crc(const cos_request_options_t *options);

int is_enable_md5(const cos_request_options_t *options);

int has_crc_in_response(const cos_http_response_t *resp);

int has_range_or_process_in_request(const cos_http_request_t *req) ;

/**
 * @brief check crc consistent between client and server
**/
int cos_check_crc_consistent(uint64_t crc, const apr_table_t *resp_headers, cos_status_t *s);

int cos_check_len_consistent(cos_list_t *buffer, const apr_table_t *resp_headers, cos_status_t *s);

int cos_get_temporary_file_name(cos_pool_t *p, const cos_string_t *filename, cos_string_t *temp_file_name);

int cos_temp_file_rename(cos_status_t *s, const char *from_path, const char *to_path, apr_pool_t *pool);

int cos_init_read_response_body_to_file_part(cos_pool_t *p,
                                        cos_upload_file_t *download_file,
                                        cos_http_response_t *resp);

/**
 * @brief add Content-MD5 header, md5 calculated from buffer
**/
int cos_add_content_md5_from_buffer(const cos_request_options_t *options,
                                    cos_list_t *buffer,
                                    cos_table_t *headers);

/**
 * @brief add Content-MD5 header, md5 calculated from file
**/
int cos_add_content_md5_from_file(const cos_request_options_t *options,
                                  const cos_string_t *filename,
                                  cos_table_t *headers);

/**
 * @brief add Content-MD5 header, md5 calculated from file range
**/
int cos_add_content_md5_from_file_range(const cos_request_options_t *options,
                                  cos_upload_file_t *upload_file,
                                  cos_table_t *headers);

/**
 * @brief set flag of adding Content-MD5 header
 * @param[in] enable    COS_TRUE: sdk will add Content-MD5 automatically; COS_FALSE:sdk does not add Content-MD5
**/
void cos_set_content_md5_enable(cos_http_controller_t *ctl, int enable);

/**
 * @brief set route address param in request options
 * @param[in] host_ip, string of route ip with '\0' ending, ip-port will not be applied if host_ip is NULL
 * @param[in] host_port, the route port, ip-port will not be applied if host_port is a none-positive integer
**/
void cos_set_request_route(cos_http_controller_t *ctl, char *host_ip, int host_port);

COS_CPP_END

#endif
